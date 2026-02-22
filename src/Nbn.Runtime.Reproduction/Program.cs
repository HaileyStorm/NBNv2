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

var managerPid = system.Root.SpawnNamed(
    Props.FromProducer(() => new ReproductionManagerActor()),
    options.ManagerName);

var advertisedHost = remoteConfig.AdvertisedHost ?? remoteConfig.Host;
var advertisedPort = remoteConfig.AdvertisedPort ?? remoteConfig.Port;
var nodeAddress = $"{advertisedHost}:{advertisedPort}";
var settingsReporter = SettingsMonitorReporter.Start(
    system,
    options.SettingsHost,
    options.SettingsPort,
    options.SettingsName,
    nodeAddress,
    options.ServiceName,
    options.ManagerName);

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

if (settingsReporter is not null)
{
    await settingsReporter.DisposeAsync();
}

await system.Remote().ShutdownAsync(true);
await system.ShutdownAsync();

static string PidLabel(PID pid)
    => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";
