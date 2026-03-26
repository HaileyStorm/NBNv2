using Nbn.Runtime.Observability;
using Nbn.Shared;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

var options = ObservabilityOptions.FromArgs(args);

using var telemetry = ObservabilityTelemetry.Start(options);

var system = new ActorSystem();
var remoteConfig = ObservabilityRemote.BuildConfig(options);
system.WithRemote(remoteConfig);
await system.Remote().StartAsync();

PID? debugPid = null;
PID? vizPid = null;

if (options.EnableDebugHub)
{
    debugPid = system.Root.SpawnNamed(
        Props.FromProducer(() => new DebugHubActor()),
        ObservabilityNames.DebugHub);
}

if (options.EnableVizHub)
{
    vizPid = system.Root.SpawnNamed(
        Props.FromProducer(() => new VizHubActor()),
        ObservabilityNames.VizHub);
}

var advertisedHost = remoteConfig.AdvertisedHost ?? remoteConfig.Host;
var advertisedPort = remoteConfig.AdvertisedPort ?? remoteConfig.Port;
var nodeAddress = $"{advertisedHost}:{advertisedPort}";
var rootActorName = options.EnableDebugHub
    ? ObservabilityNames.DebugHub
    : options.EnableVizHub ? ObservabilityNames.VizHub : "Observability";
var settingsReporter = SettingsMonitorReporter.Start(
    system,
    options.SettingsHost,
    options.SettingsPort,
    options.SettingsName,
    nodeAddress,
    options.ServiceName,
    rootActorName);

var publishedObsEndpoint = await ServiceEndpointDiscoveryClient.TryPublishAsync(
    system,
    options.SettingsHost,
    options.SettingsPort,
    options.SettingsName,
    ServiceEndpointSettings.ObservabilityKey,
    new ServiceEndpoint(nodeAddress, rootActorName));
if (!publishedObsEndpoint)
{
    Console.WriteLine($"[WARN] Failed to publish endpoint setting '{ServiceEndpointSettings.ObservabilityKey}'.");
}

Console.WriteLine("NBN Observability node online.");
Console.WriteLine($"Bind: {remoteConfig.Host}:{remoteConfig.Port}");
Console.WriteLine($"Advertised: {remoteConfig.AdvertisedHost ?? remoteConfig.Host}:{remoteConfig.AdvertisedPort ?? remoteConfig.Port}");
Console.WriteLine($"DebugHub: {(debugPid is null ? "disabled" : PidLabel(debugPid))}");
Console.WriteLine($"VizHub: {(vizPid is null ? "disabled" : PidLabel(vizPid))}");
Console.WriteLine("Press Ctrl+C to shut down.");

var shutdown = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    shutdown.TrySetResult();
};

AppDomain.CurrentDomain.ProcessExit += (_, _) => shutdown.TrySetResult();

await shutdown.Task;

if (settingsReporter is not null)
{
    await settingsReporter.DisposeAsync();
}

await system.Remote().ShutdownAsync(true);
await system.ShutdownAsync();

static string PidLabel(PID pid)
    => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";
