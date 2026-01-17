using Nbn.Runtime.HiveMind;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

var options = HiveMindOptions.FromArgs(args);

var system = new ActorSystem();
var remoteConfig = HiveMindRemote.BuildConfig(options);
system.WithRemote(remoteConfig);
await system.Remote().StartAsync();

var hiveMindPid = system.Root.SpawnNamed(
    Props.FromProducer(() => new HiveMindActor(options)),
    HiveMindNames.HiveMind);

Console.WriteLine("NBN HiveMind node online.");
Console.WriteLine($"Bind: {remoteConfig.Host}:{remoteConfig.Port}");
Console.WriteLine($"Advertised: {remoteConfig.AdvertisedHost ?? remoteConfig.Host}:{remoteConfig.AdvertisedPort ?? remoteConfig.Port}");
Console.WriteLine($"HiveMind: {PidLabel(hiveMindPid)}");
Console.WriteLine($"Target tick: {options.TargetTickHz:0.###} Hz (min {options.MinTickHz:0.###} Hz)");
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
