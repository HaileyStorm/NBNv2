using Nbn.Runtime.Observability;
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

await system.Remote().ShutdownAsync(true);
await system.ShutdownAsync();

static string PidLabel(PID pid)
    => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";
