using Nbn.Runtime.IO;
using Nbn.Shared;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

var options = IoOptions.FromArgs(args);

var system = new ActorSystem();
var remoteConfig = IoRemote.BuildConfig(options);
system.WithRemote(remoteConfig);
await system.Remote().StartAsync();

var advertisedHost = remoteConfig.AdvertisedHost ?? remoteConfig.Host;
var advertisedPort = remoteConfig.AdvertisedPort ?? remoteConfig.Port;
var endpointSet = NetworkAddressDefaults.BuildEndpointSet(remoteConfig.Host, advertisedHost, advertisedPort, options.GatewayName);

var gatewayPid = system.Root.SpawnNamed(
    Props.FromProducer(() => new IoGatewayActor(options, localEndpointCandidates: endpointSet.Candidates)),
    options.GatewayName);
var nodeAddress = $"{advertisedHost}:{advertisedPort}";
var settingsReporter = SettingsMonitorReporter.Start(
    system,
    options.SettingsHost,
    options.SettingsPort,
    options.SettingsName,
    nodeAddress,
    options.ServerName,
    options.GatewayName,
    nodeEndpointSet: endpointSet);

var publishedIoEndpoint = await ServiceEndpointDiscoveryClient.TryPublishSetAsync(
    system,
    options.SettingsHost,
    options.SettingsPort,
    options.SettingsName,
    ServiceEndpointSettings.IoGatewayKey,
    endpointSet);
if (!publishedIoEndpoint)
{
    Console.WriteLine($"[WARN] Failed to publish endpoint setting '{ServiceEndpointSettings.IoGatewayKey}'.");
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
        options.SettingsName,
        endpointSet.Candidates);

    if (discoveryClient is null)
    {
        Console.WriteLine("[WARN] IO endpoint discovery is disabled because SettingsMonitor coordinates were not configured.");
    }
    else
    {
        discoveryClient.EndpointObserved += observation =>
            system.Root.Send(gatewayPid, new IoGatewayActor.EndpointStateObserved(observation));
        discoveryLoopTask = RunDiscoveryBootstrapLoopAsync(
            system,
            gatewayPid,
            discoveryClient,
            discoveryLoopCancellation.Token);
    }
}
catch (Exception ex)
{
    Console.WriteLine($"[WARN] IO endpoint discovery setup failed: {ex.GetBaseException().Message}");
}

Console.WriteLine("NBN IO Gateway online.");
Console.WriteLine($"Bind: {remoteConfig.Host}:{remoteConfig.Port}");
Console.WriteLine($"Advertised: {remoteConfig.AdvertisedHost ?? remoteConfig.Host}:{remoteConfig.AdvertisedPort ?? remoteConfig.Port}");
Console.WriteLine($"Gateway: {PidLabel(gatewayPid)}");
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

static async Task RunDiscoveryBootstrapLoopAsync(
    ActorSystem system,
    PID gatewayPid,
    ServiceEndpointDiscoveryClient discoveryClient,
    CancellationToken cancellationToken)
{
    string[] watchedKeys =
    [
        ServiceEndpointSettings.HiveMindKey,
        ServiceEndpointSettings.ReproductionManagerKey,
        ServiceEndpointSettings.SpeciationManagerKey
    ];
    var refreshInterval = TimeSpan.FromSeconds(15);
    var attempt = 0;

    while (!cancellationToken.IsCancellationRequested)
    {
        try
        {
            var known = await discoveryClient.ResolveKnownAsync(cancellationToken).ConfigureAwait(false);
            system.Root.Send(gatewayPid, new IoGatewayActor.DiscoverySnapshotApplied(known));
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
                $"[WARN] IO discovery transition target=discovery outcome=retry_scheduled failure_reason={failureReason} attempt={attempt} retry_ms={(long)retryDelay.TotalMilliseconds}: {ex.GetBaseException().Message}");
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
