using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Proto.Signal;
using Nbn.Runtime.WorkerNode;
using Nbn.Shared;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });
Console.SetError(new StreamWriter(Console.OpenStandardError()) { AutoFlush = true });

var options = WorkerNodeOptions.FromArgs(args);

var system = new ActorSystem();
var remoteConfig = BuildRemoteConfig(options);
system.WithRemote(remoteConfig);
await system.Remote().StartAsync();

var advertisedHost = remoteConfig.AdvertisedHost ?? remoteConfig.Host;
var advertisedPort = remoteConfig.AdvertisedPort ?? remoteConfig.Port;
var nodeAddress = $"{advertisedHost}:{advertisedPort}";
var workerNodeId = options.WorkerNodeId ?? NodeIdentity.DeriveNodeId(nodeAddress);
if (workerNodeId == Guid.Empty)
{
    throw new InvalidOperationException("WorkerNode could not derive a stable worker node id.");
}

var workerPid = system.Root.SpawnNamed(
    Props.FromProducer(() => new WorkerNodeActor(workerNodeId, nodeAddress)),
    options.RootActorName);

var settingsReporter = SettingsMonitorReporter.Start(
    system,
    options.SettingsHost,
    options.SettingsPort,
    options.SettingsName,
    nodeAddress,
    options.LogicalName,
    options.RootActorName);

ServiceEndpointDiscoveryClient? discoveryClient = null;
using var discoveryLoopCancellation = new CancellationTokenSource();
Task discoveryLoopTask = Task.CompletedTask;
try
{
    discoveryClient = ServiceEndpointDiscoveryClient.Create(
        system,
        options.SettingsHost,
        options.SettingsPort,
        options.SettingsName);

    if (discoveryClient is null)
    {
        Console.WriteLine("[WARN] WorkerNode endpoint discovery is disabled because SettingsMonitor coordinates were not configured.");
        WorkerNodeTelemetry.RecordDiscoveryEndpointObserved(
            workerNodeId,
            target: "discovery",
            outcome: "disabled",
            failureReason: "settings_unconfigured");
    }
    else
    {
        discoveryClient.EndpointObserved += observation =>
            system.Root.Send(workerPid, new WorkerNodeActor.EndpointStateObserved(observation));
        discoveryLoopTask = RunDiscoveryBootstrapLoopAsync(
            system,
            workerPid,
            workerNodeId,
            discoveryClient,
            discoveryLoopCancellation.Token);
    }
}
catch (Exception ex)
{
    Console.WriteLine($"[WARN] WorkerNode endpoint discovery setup failed: {ex.GetBaseException().Message}");
    WorkerNodeTelemetry.RecordDiscoveryEndpointObserved(
        workerNodeId,
        target: "discovery",
        outcome: "bootstrap_failed",
        failureReason: ToDiscoveryFailureReason(ex));
}

var startupState = await system.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
    workerPid,
    new WorkerNodeActor.GetWorkerNodeSnapshot());

Console.WriteLine("NBN WorkerNode online.");
Console.WriteLine($"Bind: {remoteConfig.Host}:{remoteConfig.Port}");
Console.WriteLine($"Advertised: {advertisedHost}:{advertisedPort}");
Console.WriteLine($"WorkerNodeId: {startupState.WorkerNodeId}");
Console.WriteLine($"RootActor: {PidLabel(workerPid)}");
Console.WriteLine($"Discovered HiveMind: {FormatEndpoint(startupState.HiveMindEndpoint)}");
Console.WriteLine($"Discovered IO: {FormatEndpoint(startupState.IoGatewayEndpoint)}");
Console.WriteLine("Press Ctrl+C to shut down.");

var shutdown = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    shutdown.TrySetResult();
};

AppDomain.CurrentDomain.ProcessExit += (_, _) => shutdown.TrySetResult();

await shutdown.Task;

discoveryLoopCancellation.Cancel();
try
{
    await discoveryLoopTask.ConfigureAwait(false);
}
catch (OperationCanceledException) when (discoveryLoopCancellation.IsCancellationRequested)
{
}

if (discoveryClient is not null)
{
    await discoveryClient.DisposeAsync();
}

if (settingsReporter is not null)
{
    await settingsReporter.DisposeAsync();
}

await system.Remote().ShutdownAsync(true);
await system.ShutdownAsync();

static RemoteConfig BuildRemoteConfig(WorkerNodeOptions options)
{
    var bindHost = options.BindHost;
    RemoteConfig config;

    if (IsAllInterfaces(bindHost))
    {
        var advertisedHost = options.AdvertisedHost ?? bindHost;
        config = RemoteConfig.BindToAllInterfaces(advertisedHost, options.Port);
    }
    else if (IsLocalhost(bindHost))
    {
        config = RemoteConfig.BindToLocalhost(options.Port);
    }
    else
    {
        config = RemoteConfig.BindTo(bindHost, options.Port);
    }

    if (!string.IsNullOrWhiteSpace(options.AdvertisedHost))
    {
        config = config.WithAdvertisedHost(options.AdvertisedHost);
    }

    if (options.AdvertisedPort.HasValue)
    {
        config = config.WithAdvertisedPort(options.AdvertisedPort);
    }

    return config.WithProtoMessages(
        NbnCommonReflection.Descriptor,
        NbnControlReflection.Descriptor,
        NbnSettingsReflection.Descriptor,
        NbnIoReflection.Descriptor,
        NbnSignalsReflection.Descriptor);
}

static bool IsLocalhost(string host)
    => host.Equals("localhost", StringComparison.OrdinalIgnoreCase)
       || host.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
       || host.Equals("::1", StringComparison.OrdinalIgnoreCase);

static bool IsAllInterfaces(string host)
    => host.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase)
       || host.Equals("::", StringComparison.OrdinalIgnoreCase)
       || host.Equals("*", StringComparison.OrdinalIgnoreCase);

static string PidLabel(PID pid)
    => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

static string FormatEndpoint(ServiceEndpointRegistration? registration)
{
    if (!registration.HasValue)
    {
        return "(unresolved)";
    }

    var value = registration.Value;
    return $"{value.Key}={value.Endpoint.ToSettingValue()}";
}

static async Task RunDiscoveryBootstrapLoopAsync(
    ActorSystem system,
    PID workerPid,
    Guid workerNodeId,
    ServiceEndpointDiscoveryClient discoveryClient,
    CancellationToken cancellationToken)
{
    string[] watchedKeys = [ServiceEndpointSettings.HiveMindKey, ServiceEndpointSettings.IoGatewayKey];
    var refreshInterval = TimeSpan.FromSeconds(15);
    var attempt = 0;
    var bootstrapped = false;

    while (!cancellationToken.IsCancellationRequested)
    {
        try
        {
            var known = await discoveryClient.ResolveKnownAsync(cancellationToken).ConfigureAwait(false);
            system.Root.Send(workerPid, new WorkerNodeActor.DiscoverySnapshotApplied(known));

            await discoveryClient.SubscribeAsync(watchedKeys, cancellationToken).ConfigureAwait(false);

            WorkerNodeTelemetry.RecordDiscoveryEndpointObserved(
                workerNodeId,
                target: "discovery",
                outcome: bootstrapped ? "refresh_succeeded" : "bootstrap_succeeded",
                failureReason: "none");

            bootstrapped = true;
            attempt = 0;

            await Task.Delay(refreshInterval, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            break;
        }
        catch (Exception ex)
        {
            attempt++;
            var failureReason = ToDiscoveryFailureReason(ex);
            var retryDelay = ComputeDiscoveryRetryDelay(attempt);
            WorkerNodeTelemetry.RecordDiscoveryEndpointObserved(
                workerNodeId,
                target: "discovery",
                outcome: "retry_scheduled",
                failureReason: failureReason);
            Console.WriteLine(
                $"[WARN] WorkerNode discovery transition target=discovery outcome=retry_scheduled failure_reason={failureReason} attempt={attempt} retry_ms={(long)retryDelay.TotalMilliseconds}: {ex.GetBaseException().Message}");

            await Task.Delay(retryDelay, cancellationToken).ConfigureAwait(false);
        }
    }

    WorkerNodeTelemetry.RecordDiscoveryEndpointObserved(
        workerNodeId,
        target: "discovery",
        outcome: "stopped",
        failureReason: "shutdown");
}

static TimeSpan ComputeDiscoveryRetryDelay(int attempt)
{
    var clampedAttempt = Math.Clamp(attempt, 1, 6);
    var exponent = clampedAttempt - 1;
    var delayMs = 500d * Math.Pow(2d, exponent);
    return TimeSpan.FromMilliseconds(Math.Min(delayMs, 30_000d));
}

static string ToDiscoveryFailureReason(Exception exception)
{
    var root = exception.GetBaseException();
    return root switch
    {
        TimeoutException => "settings_timeout",
        OperationCanceledException => "operation_canceled",
        _ => "settings_request_failed"
    };
}
