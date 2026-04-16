using Nbn.Runtime.WorkerNode;
using Nbn.Shared;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });
Console.SetError(new StreamWriter(Console.OpenStandardError()) { AutoFlush = true });

WorkerNodeOptions options;
try
{
    options = WorkerNodeOptions.FromArgs(args);
}
catch (ArgumentException ex)
{
    Console.Error.WriteLine($"[ERROR] {ex.Message}");
    WorkerNodeOptions.PrintHelp();
    Environment.ExitCode = 1;
    return;
}

var system = new ActorSystem();
var remoteConfig = WorkerNodeRemote.BuildConfig(options);
system.WithRemote(remoteConfig);
await system.Remote().StartAsync();

var advertisedHost = remoteConfig.AdvertisedHost ?? remoteConfig.Host;
var advertisedPort = remoteConfig.AdvertisedPort ?? remoteConfig.Port;
var nodeAddress = $"{advertisedHost}:{advertisedPort}";

var capabilityProvider = new WorkerNodeCapabilityProvider(
    options.ResourceAvailability);
var workerRoots = new List<WorkerProcessRoot>();
var settingsReporters = new List<SettingsMonitorReporter>();
for (var workerIndex = 0; workerIndex < options.WorkerCount; workerIndex++)
{
    var rootActorName = options.ResolveRootActorName(workerIndex);
    var workerNodeId = options.ResolveWorkerNodeId(nodeAddress, workerIndex);
    if (workerNodeId == Guid.Empty)
    {
        throw new InvalidOperationException(
            $"WorkerNode could not derive a stable worker node id for root actor '{rootActorName}'.");
    }

    var workerPid = system.Root.SpawnNamed(
        Props.FromProducer(() => new WorkerNodeActor(
            workerNodeId,
            nodeAddress,
            enabledRoles: options.ServiceRoles,
            capabilityProfileChanged: capabilityProvider.MarkDirty,
            capabilitySnapshotProvider: capabilityProvider.GetCapabilities,
            resourceAvailability: options.ResourceAvailability,
            observabilityDefaultHost: options.SettingsHost)),
        rootActorName);

    workerRoots.Add(new WorkerProcessRoot(workerNodeId, rootActorName, workerPid));

    var settingsReporter = SettingsMonitorReporter.Start(
        system,
        options.SettingsHost,
        options.SettingsPort,
        options.SettingsName,
        nodeAddress,
        options.LogicalName,
        rootActorName,
        capabilitiesProvider: capabilityProvider.GetCapabilities,
        nodeId: workerNodeId);
    if (settingsReporter is not null)
    {
        settingsReporters.Add(settingsReporter);
    }
}

var publishedWorkerEndpoint = await ServiceEndpointDiscoveryClient.TryPublishAsync(
    system,
    options.SettingsHost,
    options.SettingsPort,
    options.SettingsName,
    ServiceEndpointSettings.WorkerNodeKey,
    new ServiceEndpoint(nodeAddress, workerRoots[0].RootActorName));
if (!publishedWorkerEndpoint)
{
    Console.WriteLine($"[WARN] Failed to publish endpoint setting '{ServiceEndpointSettings.WorkerNodeKey}'.");
}

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
        RecordDiscoveryEndpointObserved(
            workerRoots,
            target: "discovery",
            outcome: "disabled",
            failureReason: "settings_unconfigured");
    }
    else
    {
        discoveryClient.EndpointObserved += observation =>
            SendToWorkerRoots(system, workerRoots, new WorkerNodeActor.EndpointStateObserved(observation));
        discoveryLoopTask = RunDiscoveryBootstrapLoopAsync(
            system,
            workerRoots,
            discoveryClient,
            discoveryLoopCancellation.Token);
    }
}
catch (Exception ex)
{
    Console.WriteLine($"[WARN] WorkerNode endpoint discovery setup failed: {ex.GetBaseException().Message}");
    RecordDiscoveryEndpointObserved(
        workerRoots,
        target: "discovery",
        outcome: "bootstrap_failed",
        failureReason: ToDiscoveryFailureReason(ex));
}

Console.WriteLine("NBN WorkerNode online.");
Console.WriteLine($"Bind: {remoteConfig.Host}:{remoteConfig.Port}");
Console.WriteLine($"Advertised: {advertisedHost}:{advertisedPort}");
Console.WriteLine($"WorkerRoots: {workerRoots.Count}");
foreach (var workerRoot in workerRoots)
{
    var startupState = await system.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
        workerRoot.Pid,
        new WorkerNodeActor.GetWorkerNodeSnapshot());

    Console.WriteLine($"WorkerNodeId: {startupState.WorkerNodeId}");
    Console.WriteLine($"RootActor: {PidLabel(workerRoot.Pid)}");
    Console.WriteLine($"ServiceRoles: {WorkerServiceRoles.ToOptionValue(startupState.EnabledRoles)}");
    Console.WriteLine($"ResourceAvailability: {startupState.ResourceAvailability.ToDisplayString()}");
    Console.WriteLine($"Discovered HiveMind: {FormatEndpoint(startupState.HiveMindEndpoint)}");
    Console.WriteLine($"Discovered IO: {FormatEndpoint(startupState.IoGatewayEndpoint)}");
}
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

foreach (var settingsReporter in settingsReporters)
{
    await settingsReporter.DisposeAsync();
}

await system.Remote().ShutdownAsync(true);
await system.ShutdownAsync();

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
    IReadOnlyList<WorkerProcessRoot> workerRoots,
    ServiceEndpointDiscoveryClient discoveryClient,
    CancellationToken cancellationToken)
{
    string[] watchedKeys =
    [
        ServiceEndpointSettings.HiveMindKey,
        ServiceEndpointSettings.IoGatewayKey,
        ServiceEndpointSettings.ReproductionManagerKey,
        ServiceEndpointSettings.WorkerNodeKey,
        ServiceEndpointSettings.ObservabilityKey
    ];
    var refreshInterval = TimeSpan.FromSeconds(15);
    var attempt = 0;
    var bootstrapped = false;

    while (!cancellationToken.IsCancellationRequested)
    {
        try
        {
            var known = await discoveryClient.ResolveKnownAsync(cancellationToken).ConfigureAwait(false);
            SendToWorkerRoots(system, workerRoots, new WorkerNodeActor.DiscoverySnapshotApplied(known));

            await discoveryClient.SubscribeAsync(watchedKeys, cancellationToken).ConfigureAwait(false);

            RecordDiscoveryEndpointObserved(
                workerRoots,
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
            RecordDiscoveryEndpointObserved(
                workerRoots,
                target: "discovery",
                outcome: "retry_scheduled",
                failureReason: failureReason);
            Console.WriteLine(
                $"[WARN] WorkerNode discovery transition target=discovery outcome=retry_scheduled failure_reason={failureReason} attempt={attempt} retry_ms={(long)retryDelay.TotalMilliseconds}: {ex.GetBaseException().Message}");

            await Task.Delay(retryDelay, cancellationToken).ConfigureAwait(false);
        }
    }

    RecordDiscoveryEndpointObserved(
        workerRoots,
        target: "discovery",
        outcome: "stopped",
        failureReason: "shutdown");
}

static void SendToWorkerRoots(ActorSystem system, IReadOnlyList<WorkerProcessRoot> workerRoots, object message)
{
    foreach (var workerRoot in workerRoots)
    {
        system.Root.Send(workerRoot.Pid, message);
    }
}

static void RecordDiscoveryEndpointObserved(
    IReadOnlyList<WorkerProcessRoot> workerRoots,
    string target,
    string outcome,
    string failureReason)
{
    foreach (var workerRoot in workerRoots)
    {
        WorkerNodeTelemetry.RecordDiscoveryEndpointObserved(
            workerRoot.WorkerNodeId,
            target,
            outcome,
            failureReason);
    }
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

internal readonly record struct WorkerProcessRoot(Guid WorkerNodeId, string RootActorName, PID Pid);
