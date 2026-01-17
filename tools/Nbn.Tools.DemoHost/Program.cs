using JsonSerializer = System.Text.Json.JsonSerializer;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Signal;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.Brain;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });
Console.SetError(new StreamWriter(Console.OpenStandardError()) { AutoFlush = true });

if (args.Length == 0)
{
    PrintHelp();
    return;
}

var command = args[0].ToLowerInvariant();
var remaining = args.Skip(1).ToArray();

switch (command)
{
    case "init-artifacts":
        await InitArtifactsAsync(remaining);
        break;
    case "run-brain":
        await RunBrainAsync(remaining);
        break;
    default:
        PrintHelp();
        break;
}

static async Task InitArtifactsAsync(string[] args)
{
    var artifactRoot = GetArg(args, "--artifact-root") ?? Path.Combine(Environment.CurrentDirectory, "demo-artifacts");
    var jsonOnly = HasFlag(args, "--json");

    Directory.CreateDirectory(artifactRoot);

    var nbnBytes = BuildMinimalNbn();
    var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
    var manifest = await store.StoreAsync(new MemoryStream(nbnBytes), "application/x-nbn");

    var payload = new
    {
        nbn_sha256 = manifest.ArtifactId.ToHex(),
        nbn_size = manifest.ByteLength,
        artifact_root = Path.GetFullPath(artifactRoot)
    };

    var json = JsonSerializer.Serialize(payload);
    Console.WriteLine(json);

    if (!jsonOnly)
    {
        Console.WriteLine($"NBN bytes: {manifest.ByteLength}");
    }
}

static async Task RunBrainAsync(string[] args)
{
    var bindHost = GetArg(args, "--bind-host") ?? "127.0.0.1";
    var port = GetIntArg(args, "--port") ?? 12010;
    var advertisedHost = GetArg(args, "--advertise-host");
    var advertisedPort = GetIntArg(args, "--advertise-port");
    var brainId = GetGuidArg(args, "--brain-id") ?? Guid.NewGuid();
    var routerId = GetArg(args, "--router-id") ?? "demo-router";
    var brainRootId = GetArg(args, "--brain-root-id") ?? "BrainRoot";
    var hiveAddress = GetArg(args, "--hivemind-address");
    var hiveId = GetArg(args, "--hivemind-id");

    PID? hivePid = null;
    if (!string.IsNullOrWhiteSpace(hiveAddress) && !string.IsNullOrWhiteSpace(hiveId))
    {
        hivePid = new PID(hiveAddress, hiveId);
    }

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

    Console.WriteLine("NBN Demo BrainHost online.");
    Console.WriteLine($"Bind: {remoteConfig.Host}:{remoteConfig.Port}");
    Console.WriteLine($"Advertised: {remoteConfig.AdvertisedHost ?? remoteConfig.Host}:{remoteConfig.AdvertisedPort ?? remoteConfig.Port}");
    Console.WriteLine($"BrainId: {brainId}");
    Console.WriteLine($"BrainRoot: {PidLabel(brainRootPid)}");
    Console.WriteLine($"Router: {PidLabel(routerPid)}");
    Console.WriteLine($"HiveMind: {(hivePid is null ? "(none)" : PidLabel(hivePid))}");
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
}

static byte[] BuildMinimalNbn()
{
    var stride = 1024u;
    var sections = new List<NbnRegionSection>();
    var directory = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
    ulong offset = NbnBinary.NbnHeaderBytes;

    offset = AddRegionSection(0, 1, stride, ref directory, sections, offset);
    offset = AddRegionSection(1, 1, stride, ref directory, sections, offset);
    offset = AddRegionSection(NbnConstants.OutputRegionId, 1, stride, ref directory, sections, offset);

    var header = new NbnHeaderV2(
        "NBN2",
        2,
        1,
        10,
        brainSeed: 1,
        axonStride: stride,
        flags: 0,
        quantization: QuantizationSchemas.DefaultNbn,
        regions: directory);

    return NbnBinary.WriteNbn(header, sections);
}

static ulong AddRegionSection(
    int regionId,
    uint neuronSpan,
    uint stride,
    ref NbnRegionDirectoryEntry[] directory,
    List<NbnRegionSection> sections,
    ulong offset)
{
    var totalAxons = 0UL;
    var checkpointCount = (uint)((neuronSpan + stride - 1) / stride + 1);
    var checkpoints = new ulong[checkpointCount];
    checkpoints[0] = 0;
    checkpoints[checkpointCount - 1] = totalAxons;

    var neurons = new NeuronRecord[neuronSpan];
    for (var i = 0; i < neurons.Length; i++)
    {
        neurons[i] = new NeuronRecord(
            axonCount: 0,
            paramBCode: 0,
            paramACode: 0,
            activationThresholdCode: 0,
            preActivationThresholdCode: 0,
            resetFunctionId: 0,
            activationFunctionId: 1,
            accumulationFunctionId: 0,
            exists: true);
    }

    var axons = Array.Empty<AxonRecord>();
    var section = new NbnRegionSection(
        (byte)regionId,
        neuronSpan,
        totalAxons,
        stride,
        checkpointCount,
        checkpoints,
        neurons,
        axons);

    directory[regionId] = new NbnRegionDirectoryEntry(neuronSpan, totalAxons, offset, 0);
    sections.Add(section);
    return offset + (ulong)section.ByteLength;
}

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
        NbnSignalsReflection.Descriptor);

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

static Guid? GetGuidArg(string[] args, string name)
{
    var value = GetArg(args, name);
    return Guid.TryParse(value, out var parsed) ? parsed : null;
}

static string PidLabel(PID pid)
    => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

static void PrintHelp()
{
    Console.WriteLine("NBN DemoHost usage:");
    Console.WriteLine("  init-artifacts --artifact-root <path> [--json]");
    Console.WriteLine("  run-brain --bind-host <host> --port <port> --brain-id <guid>");
    Console.WriteLine("            --hivemind-address <host:port> --hivemind-id <name>");
    Console.WriteLine("            [--router-id <name>] [--brain-root-id <name>]");
}
