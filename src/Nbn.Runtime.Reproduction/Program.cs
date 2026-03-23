using Nbn.Runtime.Reproduction;
using Nbn.Shared;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

var options = ReproductionOptions.FromArgs(args);

var system = new ActorSystem();
var remoteConfig = ReproductionRemote.BuildConfig(options);
system.WithRemote(remoteConfig);
await system.Remote().StartAsync();

var ioGatewayPid = BuildIoPid(options);
var managerPid = system.Root.SpawnNamed(
    Props.FromProducer(() => new ReproductionManagerActor(ioGatewayPid)),
    options.ManagerName);

var advertisedHost = remoteConfig.AdvertisedHost ?? remoteConfig.Host;
var advertisedPort = remoteConfig.AdvertisedPort ?? remoteConfig.Port;
var nodeAddress = $"{advertisedHost}:{advertisedPort}";
var endpointSet = NetworkAddressDefaults.BuildEndpointSet(remoteConfig.Host, advertisedHost, advertisedPort, options.ManagerName);
var settingsReporter = SettingsMonitorReporter.Start(
    system,
    options.SettingsHost,
    options.SettingsPort,
    options.SettingsName,
    nodeAddress,
    options.ServiceName,
    options.ManagerName);

var publishedReproEndpoint = await ServiceEndpointDiscoveryClient.TryPublishSetAsync(
    system,
    options.SettingsHost,
    options.SettingsPort,
    options.SettingsName,
    ServiceEndpointSettings.ReproductionManagerKey,
    endpointSet);
if (!publishedReproEndpoint)
{
    Console.WriteLine($"[WARN] Failed to publish endpoint setting '{ServiceEndpointSettings.ReproductionManagerKey}'.");
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
        Console.WriteLine("[WARN] Reproduction endpoint discovery is disabled because SettingsMonitor coordinates were not configured.");
    }
    else
    {
        discoveryClient.EndpointObserved += observation =>
            system.Root.Send(managerPid, new ReproductionManagerActor.EndpointStateObserved(observation));
        discoveryLoopTask = RunDiscoveryBootstrapLoopAsync(
            system,
            managerPid,
            discoveryClient,
            discoveryLoopCancellation.Token);
    }
}
catch (Exception ex)
{
    Console.WriteLine($"[WARN] Reproduction endpoint discovery setup failed: {ex.GetBaseException().Message}");
}

Console.WriteLine("NBN Reproduction node online.");
Console.WriteLine($"Bind: {remoteConfig.Host}:{remoteConfig.Port}");
Console.WriteLine($"Advertised: {remoteConfig.AdvertisedHost ?? remoteConfig.Host}:{remoteConfig.AdvertisedPort ?? remoteConfig.Port}");
Console.WriteLine($"Manager: {PidLabel(managerPid)}");
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

static string PidLabel(PID pid)
    => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

static PID? BuildIoPid(ReproductionOptions options)
{
    if (string.IsNullOrWhiteSpace(options.IoAddress) || string.IsNullOrWhiteSpace(options.IoName))
    {
        return null;
    }

    return new PID(options.IoAddress, options.IoName);
}

static async Task RunDiscoveryBootstrapLoopAsync(
    ActorSystem system,
    PID managerPid,
    ServiceEndpointDiscoveryClient discoveryClient,
    CancellationToken cancellationToken)
{
    string[] watchedKeys = [ServiceEndpointSettings.IoGatewayKey];
    var refreshInterval = TimeSpan.FromSeconds(15);
    var attempt = 0;

    while (!cancellationToken.IsCancellationRequested)
    {
        try
        {
            var known = await discoveryClient.ResolveKnownAsync(cancellationToken).ConfigureAwait(false);
            system.Root.Send(managerPid, new ReproductionManagerActor.DiscoverySnapshotApplied(known));
            await discoveryClient.SubscribeAsync(watchedKeys, cancellationToken).ConfigureAwait(false);
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
            var retryDelay = ComputeDiscoveryRetryDelay(attempt);
            var failureReason = ToDiscoveryFailureReason(ex);
            Console.WriteLine(
                $"[WARN] Reproduction discovery transition target=discovery outcome=retry_scheduled failure_reason={failureReason} attempt={attempt} retry_ms={(long)retryDelay.TotalMilliseconds}: {ex.GetBaseException().Message}");
            await Task.Delay(retryDelay, cancellationToken).ConfigureAwait(false);
        }
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
