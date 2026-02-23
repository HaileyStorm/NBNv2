using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Proto.Signal;
using Nbn.Runtime.WorkerNode;
using Nbn.Shared;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });
Console.SetError(new StreamWriter(Console.OpenStandardError()) { AutoFlush = true });

var options = WorkerNodeOptions.FromArgs(args);

var system = new ActorSystem();
var remoteConfig = BuildRemoteConfig(options);
system.WithRemote(remoteConfig);
await system.Remote().StartAsync();

var advertisedHost = remoteConfig.AdvertisedHost ?? remoteConfig.Host;
var advertisedPort = remoteConfig.AdvertisedPort ?? remoteConfig.Port;
var nodeAddress = $"{advertisedHost}:{advertisedPort}";
var workerNodeId = options.WorkerNodeId ?? NodeIdentity.DeriveNodeId(nodeAddress);
if (workerNodeId == Guid.Empty)
{
    throw new InvalidOperationException("WorkerNode could not derive a stable worker node id.");
}

var workerPid = system.Root.SpawnNamed(
    Props.FromProducer(() => new WorkerNodeActor(workerNodeId, nodeAddress)),
    options.RootActorName);

var settingsReporter = SettingsMonitorReporter.Start(
    system,
    options.SettingsHost,
    options.SettingsPort,
    options.SettingsName,
    nodeAddress,
    options.LogicalName,
    options.RootActorName);

ServiceEndpointDiscoveryClient? discoveryClient = null;
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
    }
    else
    {
        discoveryClient.EndpointChanged += registration =>
            system.Root.Send(workerPid, new WorkerNodeActor.EndpointRegistrationObserved(registration));

        var known = await discoveryClient.ResolveKnownAsync();
        system.Root.Send(workerPid, new WorkerNodeActor.DiscoverySnapshotApplied(known));
        await discoveryClient.SubscribeAsync([ServiceEndpointSettings.HiveMindKey, ServiceEndpointSettings.IoGatewayKey]);
    }
}
catch (Exception ex)
{
    Console.WriteLine($"[WARN] WorkerNode endpoint discovery setup failed: {ex.GetBaseException().Message}");
}

var startupState = await system.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
    workerPid,
    new WorkerNodeActor.GetWorkerNodeSnapshot());

Console.WriteLine("NBN WorkerNode online.");
Console.WriteLine($"Bind: {remoteConfig.Host}:{remoteConfig.Port}");
Console.WriteLine($"Advertised: {advertisedHost}:{advertisedPort}");
Console.WriteLine($"WorkerNodeId: {startupState.WorkerNodeId}");
Console.WriteLine($"RootActor: {PidLabel(workerPid)}");
Console.WriteLine($"Discovered HiveMind: {FormatEndpoint(startupState.HiveMindEndpoint)}");
Console.WriteLine($"Discovered IO: {FormatEndpoint(startupState.IoGatewayEndpoint)}");
Console.WriteLine("Press Ctrl+C to shut down.");

var shutdown = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    shutdown.TrySetResult();
};

AppDomain.CurrentDomain.ProcessExit += (_, _) => shutdown.TrySetResult();

await shutdown.Task;

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

static RemoteConfig BuildRemoteConfig(WorkerNodeOptions options)
{
    var bindHost = options.BindHost;
    RemoteConfig config;

    if (IsAllInterfaces(bindHost))
    {
        var advertisedHost = options.AdvertisedHost ?? bindHost;
        config = RemoteConfig.BindToAllInterfaces(advertisedHost, options.Port);
    }
    else if (IsLocalhost(bindHost))
    {
        config = RemoteConfig.BindToLocalhost(options.Port);
    }
    else
    {
        config = RemoteConfig.BindTo(bindHost, options.Port);
    }

    if (!string.IsNullOrWhiteSpace(options.AdvertisedHost))
    {
        config = config.WithAdvertisedHost(options.AdvertisedHost);
    }

    if (options.AdvertisedPort.HasValue)
    {
        config = config.WithAdvertisedPort(options.AdvertisedPort);
    }

    return config.WithProtoMessages(
        NbnCommonReflection.Descriptor,
        NbnControlReflection.Descriptor,
        NbnSettingsReflection.Descriptor,
        NbnIoReflection.Descriptor,
        NbnSignalsReflection.Descriptor);
}

static bool IsLocalhost(string host)
    => host.Equals("localhost", StringComparison.OrdinalIgnoreCase)
       || host.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
       || host.Equals("::1", StringComparison.OrdinalIgnoreCase);

static bool IsAllInterfaces(string host)
    => host.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase)
       || host.Equals("::", StringComparison.OrdinalIgnoreCase)
       || host.Equals("*", StringComparison.OrdinalIgnoreCase);

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
