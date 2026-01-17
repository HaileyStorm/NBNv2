using Nbn.Runtime.IO;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

var options = IoOptions.FromArgs(args);

var system = new ActorSystem();
var remoteConfig = IoRemote.BuildConfig(options);
system.WithRemote(remoteConfig);
await system.Remote().StartAsync();

var gatewayPid = system.Root.SpawnNamed(
    Props.FromProducer(() => new IoGatewayActor(options)),
    options.GatewayName);

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

await system.Remote().ShutdownAsync(true);
await system.ShutdownAsync();

static string PidLabel(PID pid)
    => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";
