using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Proto.Signal;
using Nbn.Proto.Viz;
using Nbn.Runtime.Brain;
using Nbn.Runtime.BrainHost;
using Nbn.Shared;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });
Console.SetError(new StreamWriter(Console.OpenStandardError()) { AutoFlush = true });

if (HasFlag(args, "--help") || HasFlag(args, "-h"))
{
    PrintHelp();
    return;
}

var bindHost = GetArg(args, "--bind-host") ?? GetArg(args, "--bind") ?? "127.0.0.1";
var port = GetIntArg(args, "--port") ?? 12011;
var advertisedHost = GetArg(args, "--advertise-host") ?? GetArg(args, "--advertise");
var advertisedPort = GetIntArg(args, "--advertise-port");
var brainId = GetGuidArg(args, "--brain-id") ?? Guid.NewGuid();
var routerId = GetArg(args, "--router-id") ?? "demo-router";
var brainRootId = GetArg(args, "--brain-root-id") ?? "BrainRoot";
var hiveAddress = GetArg(args, "--hivemind-address");
var hiveId = GetArg(args, "--hivemind-id") ?? GetArg(args, "--hivemind-name");
var ioAddress = GetArg(args, "--io-address");
var ioId = GetArg(args, "--io-id") ?? GetArg(args, "--io-gateway");
var settingsHost = GetArg(args, "--settings-host") ?? Environment.GetEnvironmentVariable("NBN_SETTINGS_HOST") ?? "127.0.0.1";
var settingsPort = GetIntArg(args, "--settings-port") ?? GetEnvInt("NBN_SETTINGS_PORT") ?? 12010;
var settingsName = GetArg(args, "--settings-name") ?? Environment.GetEnvironmentVariable("NBN_SETTINGS_NAME") ?? "SettingsMonitor";
var inputWidth = GetIntArg(args, "--input-width") ?? 1;
var outputWidth = GetIntArg(args, "--output-width") ?? 1;
var enableOtel = GetEnvBool("NBN_BRAIN_OTEL_ENABLED") ?? false;
var enableOtelMetrics = GetEnvBool("NBN_BRAIN_OTEL_METRICS_ENABLED");
var enableOtelConsole = GetEnvBool("NBN_BRAIN_OTEL_CONSOLE") ?? false;
var otlpEndpoint = GetEnv("NBN_BRAIN_OTEL_ENDPOINT") ?? GetEnv("OTEL_EXPORTER_OTLP_ENDPOINT");
var otelServiceName = GetEnv("NBN_BRAIN_OTEL_SERVICE_NAME") ?? GetEnv("OTEL_SERVICE_NAME") ?? "nbn.brainhost";

if (HasFlag(args, "--enable-otel") || HasFlag(args, "--otel"))
{
    enableOtel = true;
}

if (HasFlag(args, "--disable-otel") || HasFlag(args, "--no-otel"))
{
    enableOtel = false;
}

if (HasFlag(args, "--otel-metrics"))
{
    enableOtelMetrics = true;
}

if (HasFlag(args, "--otel-console"))
{
    enableOtelConsole = true;
}

var otlpArg = GetArg(args, "--otel-endpoint");
if (!string.IsNullOrWhiteSpace(otlpArg))
{
    otlpEndpoint = otlpArg;
}

var serviceArg = GetArg(args, "--otel-service-name");
if (!string.IsNullOrWhiteSpace(serviceArg))
{
    otelServiceName = serviceArg;
}

if (enableOtelMetrics == true)
{
    enableOtel = true;
}

enableOtelMetrics ??= enableOtel;

PID? hivePid = null;
if (!string.IsNullOrWhiteSpace(hiveAddress) && !string.IsNullOrWhiteSpace(hiveId))
{
    hivePid = new PID(hiveAddress, hiveId);
}

using var telemetry = BrainTelemetrySession.Start(enableOtel, enableOtelMetrics.Value, enableOtelConsole, otlpEndpoint, otelServiceName);

var system = new ActorSystem();
var remoteConfig = BuildRemoteConfig(bindHost, port, advertisedHost, advertisedPort);
system.WithRemote(remoteConfig);
await system.Remote().StartAsync();

var routerPid = system.Root.SpawnNamed(
    Props.FromProducer(() => new BrainSignalRouterActor(brainId)),
    routerId);

var brainRootPid = system.Root.SpawnNamed(
    Props.FromProducer(() => new BrainRootActor(brainId, hivePid, autoSpawnSignalRouter: false)),
    brainRootId);

system.Root.Send(brainRootPid, new SetSignalRouter(routerPid));

if (!string.IsNullOrWhiteSpace(ioAddress) && !string.IsNullOrWhiteSpace(ioId))
{
    var ioPid = new PID(ioAddress, ioId);
    system.Root.Send(ioPid, new Nbn.Proto.Io.RegisterBrain
    {
        BrainId = brainId.ToProtoUuid(),
        InputWidth = (uint)Math.Max(0, inputWidth),
        OutputWidth = (uint)Math.Max(0, outputWidth)
    });
}

var nodeAddress = $"{remoteConfig.AdvertisedHost ?? remoteConfig.Host}:{remoteConfig.AdvertisedPort ?? remoteConfig.Port}";
var settingsReporter = SettingsMonitorReporter.Start(
    system,
    settingsHost,
    settingsPort,
    settingsName,
    nodeAddress,
    "brain-host",
    brainRootId);

Console.WriteLine("NBN BrainHost online.");
Console.WriteLine($"Bind: {remoteConfig.Host}:{remoteConfig.Port}");
Console.WriteLine($"Advertised: {remoteConfig.AdvertisedHost ?? remoteConfig.Host}:{remoteConfig.AdvertisedPort ?? remoteConfig.Port}");
Console.WriteLine($"BrainId: {brainId}");
Console.WriteLine($"BrainRoot: {PidLabel(brainRootPid)}");
Console.WriteLine($"Router: {PidLabel(routerPid)}");
Console.WriteLine($"HiveMind: {(hivePid is null ? "(none)" : PidLabel(hivePid))}");
Console.WriteLine($"IO Gateway: {(string.IsNullOrWhiteSpace(ioAddress) ? "(none)" : $"{ioAddress}/{ioId}")}");
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

static RemoteConfig BuildRemoteConfig(string bindHost, int port, string? advertisedHost, int? advertisedPort)
{
    RemoteConfig config;
    if (IsAllInterfaces(bindHost))
    {
        var advertiseHost = advertisedHost ?? bindHost;
        config = RemoteConfig.BindToAllInterfaces(advertiseHost, port);
    }
    else if (IsLocalhost(bindHost))
    {
        config = RemoteConfig.BindToLocalhost(port);
    }
    else
    {
        config = RemoteConfig.BindTo(bindHost, port);
    }

    if (!string.IsNullOrWhiteSpace(advertisedHost))
    {
        config = config.WithAdvertisedHost(advertisedHost);
    }

    if (advertisedPort.HasValue)
    {
        config = config.WithAdvertisedPort(advertisedPort);
    }

    config = config.WithProtoMessages(
        NbnCommonReflection.Descriptor,
        NbnControlReflection.Descriptor,
        NbnDebugReflection.Descriptor,
        NbnIoReflection.Descriptor,
        NbnSettingsReflection.Descriptor,
        NbnSignalsReflection.Descriptor,
        NbnVizReflection.Descriptor);

    return config;
}

static bool IsLocalhost(string host)
    => host.Equals("localhost", StringComparison.OrdinalIgnoreCase)
       || host.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
       || host.Equals("::1", StringComparison.OrdinalIgnoreCase);

static bool IsAllInterfaces(string host)
    => host.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase)
       || host.Equals("::", StringComparison.OrdinalIgnoreCase)
       || host.Equals("*", StringComparison.OrdinalIgnoreCase);

static string? GetArg(string[] args, string name)
{
    for (var i = 0; i < args.Length; i++)
    {
        var arg = args[i];
        if (arg.Equals(name, StringComparison.OrdinalIgnoreCase))
        {
            if (i + 1 < args.Length)
            {
                return args[i + 1];
            }
        }

        if (arg.StartsWith(name + "=", StringComparison.OrdinalIgnoreCase))
        {
            return arg.Substring(name.Length + 1);
        }
    }

    return null;
}

static bool HasFlag(string[] args, string name)
{
    foreach (var arg in args)
    {
        if (arg.Equals(name, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (arg.StartsWith(name + "=", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }
    }

    return false;
}

static int? GetIntArg(string[] args, string name)
{
    var value = GetArg(args, name);
    return int.TryParse(value, out var parsed) ? parsed : null;
}

static int? GetEnvInt(string key)
{
    var value = GetEnv(key);
    return int.TryParse(value, out var parsed) ? parsed : null;
}

static string? GetEnv(string key)
    => Environment.GetEnvironmentVariable(key);

static bool? GetEnvBool(string key)
{
    var value = GetEnv(key);
    if (string.IsNullOrWhiteSpace(value))
    {
        return null;
    }

    if (bool.TryParse(value, out var parsed))
    {
        return parsed;
    }

    return value.Equals("1", StringComparison.OrdinalIgnoreCase)
        || value.Equals("yes", StringComparison.OrdinalIgnoreCase)
        || value.Equals("y", StringComparison.OrdinalIgnoreCase);
}

static Guid? GetGuidArg(string[] args, string name)
{
    var value = GetArg(args, name);
    return Guid.TryParse(value, out var parsed) ? parsed : null;
}

static string PidLabel(PID pid)
    => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

static void PrintHelp()
{
    Console.WriteLine("NBN BrainHost usage:");
    Console.WriteLine("  --bind-host <host> --port <port> --brain-id <guid>");
    Console.WriteLine("  --hivemind-address <host:port> --hivemind-id <name>");
    Console.WriteLine("  [--router-id <name>] [--brain-root-id <name>]");
    Console.WriteLine("  [--io-address <host:port>] [--io-id <name>] [--input-width <n>] [--output-width <n>]");
    Console.WriteLine("  [--enable-otel|--disable-otel] [--otel-metrics] [--otel-console]");
    Console.WriteLine("  [--otel-endpoint <uri>] [--otel-service-name <name>]");
    Console.WriteLine("  [--settings-host <host>] [--settings-port <port>] [--settings-name <name>]");
    Console.WriteLine("  [--advertise-host <host>] [--advertise-port <port>]");
}
