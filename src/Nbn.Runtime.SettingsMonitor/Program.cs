using Nbn.Runtime.SettingsMonitor;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });
Console.SetError(new StreamWriter(Console.OpenStandardError()) { AutoFlush = true });

var options = SettingsMonitorOptions.FromArgs(args);
var store = new SettingsMonitorStore(options.DatabasePath);
await store.InitializeAsync();
await store.EnsureDefaultSettingsAsync();

var system = new ActorSystem();
var remoteConfig = SettingsMonitorRemote.BuildConfig(options);
system.WithRemote(remoteConfig);
await system.Remote().StartAsync();

var monitorPid = system.Root.SpawnNamed(
    Props.FromProducer(() => new SettingsMonitorActor(store)),
    SettingsMonitorNames.SettingsMonitor);

Console.WriteLine("NBN SettingsMonitor node online.");
Console.WriteLine($"DB: {Path.GetFullPath(options.DatabasePath)}");
Console.WriteLine($"Bind: {remoteConfig.Host}:{remoteConfig.Port}");
Console.WriteLine($"Advertised: {remoteConfig.AdvertisedHost ?? remoteConfig.Host}:{remoteConfig.AdvertisedPort ?? remoteConfig.Port}");
Console.WriteLine($"SettingsMonitor: {PidLabel(monitorPid)}");
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
