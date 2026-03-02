using Nbn.Runtime.Speciation;
using Nbn.Shared;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

var options = SpeciationOptions.FromArgs(args);
var runtimeConfig = options.ToRuntimeConfig();
var store = new SpeciationStore(options.DatabasePath);

var system = new ActorSystem();
var remoteConfig = SpeciationRemote.BuildConfig(options);
system.WithRemote(remoteConfig);
await system.Remote().StartAsync();

var settingsPid = BuildSettingsPid(options);
var managerPid = system.Root.SpawnNamed(
    Props.FromProducer(() => new SpeciationManagerActor(store, runtimeConfig, settingsPid)),
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

var publishedSpeciationEndpoint = await ServiceEndpointDiscoveryClient.TryPublishAsync(
    system,
    options.SettingsHost,
    options.SettingsPort,
    options.SettingsName,
    ServiceEndpointSettings.SpeciationManagerKey,
    new ServiceEndpoint(nodeAddress, options.ManagerName));
if (!publishedSpeciationEndpoint)
{
    Console.WriteLine($"[WARN] Failed to publish endpoint setting '{ServiceEndpointSettings.SpeciationManagerKey}'.");
}

Console.WriteLine("NBN Speciation node online.");
Console.WriteLine($"DB: {Path.GetFullPath(options.DatabasePath)}");
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

static PID? BuildSettingsPid(SpeciationOptions options)
{
    if (string.IsNullOrWhiteSpace(options.SettingsHost)
        || options.SettingsPort <= 0
        || string.IsNullOrWhiteSpace(options.SettingsName))
    {
        return null;
    }

    return new PID($"{options.SettingsHost}:{options.SettingsPort}", options.SettingsName);
}

static string PidLabel(PID pid)
    => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";
