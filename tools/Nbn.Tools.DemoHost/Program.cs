using JsonSerializer = System.Text.Json.JsonSerializer;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Proto.Settings;
using Nbn.Proto.Viz;
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
    case "io-scenario":
        await RunIoScenarioAsync(remaining);
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
    var ioAddress = GetArg(args, "--io-address");
    var ioId = GetArg(args, "--io-id") ?? GetArg(args, "--io-gateway");
    var settingsHost = GetArg(args, "--settings-host") ?? Environment.GetEnvironmentVariable("NBN_SETTINGS_HOST") ?? "127.0.0.1";
    var settingsPort = GetIntArg(args, "--settings-port") ?? GetEnvInt("NBN_SETTINGS_PORT") ?? 12010;
    var settingsName = GetArg(args, "--settings-name") ?? Environment.GetEnvironmentVariable("NBN_SETTINGS_NAME") ?? "SettingsMonitor";

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

    if (!string.IsNullOrWhiteSpace(ioAddress) && !string.IsNullOrWhiteSpace(ioId))
    {
        var ioPid = new PID(ioAddress, ioId);
        system.Root.Send(ioPid, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1
        });
    }

    var nodeAddress = $"{remoteConfig.AdvertisedHost ?? remoteConfig.Host}:{remoteConfig.AdvertisedPort ?? remoteConfig.Port}";
    var settingsReporter = SettingsMonitorReporter.Start(
        system,
        settingsHost,
        settingsPort,
        settingsName,
        nodeAddress,
        "demo-brainhost",
        brainRootId);

    Console.WriteLine("NBN Demo BrainHost online.");
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
}

static async Task RunIoScenarioAsync(string[] args)
{
    var bindHost = GetArg(args, "--bind-host") ?? "127.0.0.1";
    var port = GetIntArg(args, "--port") ?? 12070;
    var advertisedHost = GetArg(args, "--advertise-host");
    var advertisedPort = GetIntArg(args, "--advertise-port");
    var ioAddress = GetArg(args, "--io-address") ?? throw new InvalidOperationException("--io-address is required.");
    var ioId = GetArg(args, "--io-id") ?? "io-gateway";
    var brainId = GetGuidArg(args, "--brain-id") ?? throw new InvalidOperationException("--brain-id is required.");
    var credit = GetLongArg(args, "--credit") ?? 500;
    var rate = GetLongArg(args, "--rate") ?? 0;
    var costEnabled = GetBoolArg(args, "--cost-enabled") ?? true;
    var energyEnabled = GetBoolArg(args, "--energy-enabled") ?? true;
    var plasticityEnabled = GetBoolArg(args, "--plasticity-enabled") ?? true;
    var plasticityRate = GetFloatArg(args, "--plasticity-rate") ?? 0.05f;
    var probabilistic = GetBoolArg(args, "--probabilistic") ?? true;
    var timeoutSeconds = GetIntArg(args, "--timeout-seconds") ?? 10;
    var jsonOnly = HasFlag(args, "--json");
    var timeout = TimeSpan.FromSeconds(Math.Max(1, timeoutSeconds));

    var system = new ActorSystem();
    var remoteConfig = BuildRemoteConfig(bindHost, port, advertisedHost, advertisedPort);
    system.WithRemote(remoteConfig);
    await system.Remote().StartAsync();

    try
    {
        var ioPid = new PID(ioAddress, ioId);
        var protoBrainId = brainId.ToProtoUuid();

        var creditAck = await system.Root.RequestAsync<IoCommandAck>(
            ioPid,
            new EnergyCredit
            {
                BrainId = protoBrainId,
                Amount = credit
            },
            timeout);

        var rateAck = await system.Root.RequestAsync<IoCommandAck>(
            ioPid,
            new EnergyRate
            {
                BrainId = protoBrainId,
                UnitsPerSecond = rate
            },
            timeout);

        var costEnergyAck = await system.Root.RequestAsync<IoCommandAck>(
            ioPid,
            new SetCostEnergyEnabled
            {
                BrainId = protoBrainId,
                CostEnabled = costEnabled,
                EnergyEnabled = energyEnabled
            },
            timeout);

        var plasticityAck = await system.Root.RequestAsync<IoCommandAck>(
            ioPid,
            new SetPlasticityEnabled
            {
                BrainId = protoBrainId,
                PlasticityEnabled = plasticityEnabled,
                PlasticityRate = plasticityRate,
                ProbabilisticUpdates = probabilistic
            },
            timeout);

        var brainInfo = await system.Root.RequestAsync<BrainInfo>(
            ioPid,
            new BrainInfoRequest
            {
                BrainId = protoBrainId
            },
            timeout);

        var payload = new
        {
            brain_id = brainId.ToString("D"),
            io_address = ioAddress,
            io_id = ioId,
            credit_ack = ToAckPayload(creditAck),
            rate_ack = ToAckPayload(rateAck),
            cost_energy_ack = ToAckPayload(costEnergyAck),
            plasticity_ack = ToAckPayload(plasticityAck),
            brain_info = new
            {
                cost_enabled = brainInfo.CostEnabled,
                energy_enabled = brainInfo.EnergyEnabled,
                energy_remaining = brainInfo.EnergyRemaining,
                energy_rate_units_per_second = brainInfo.EnergyRateUnitsPerSecond,
                plasticity_enabled = brainInfo.PlasticityEnabled,
                plasticity_rate = brainInfo.PlasticityRate,
                plasticity_probabilistic_updates = brainInfo.PlasticityProbabilisticUpdates,
                last_tick_cost = brainInfo.LastTickCost
            }
        };

        var json = JsonSerializer.Serialize(payload);
        Console.WriteLine(json);

        if (!jsonOnly)
        {
            Console.WriteLine($"Scenario brain: {brainId:D}");
            Console.WriteLine($"Credit ack: success={creditAck.Success} message={creditAck.Message}");
            Console.WriteLine($"Rate ack: success={rateAck.Success} message={rateAck.Message}");
            Console.WriteLine($"Cost/Energy ack: success={costEnergyAck.Success} message={costEnergyAck.Message}");
            Console.WriteLine($"Plasticity ack: success={plasticityAck.Success} message={plasticityAck.Message}");
            Console.WriteLine(
                $"BrainInfo: cost={brainInfo.CostEnabled} energy={brainInfo.EnergyEnabled} remaining={brainInfo.EnergyRemaining} " +
                $"rate={brainInfo.EnergyRateUnitsPerSecond}/s plasticity={brainInfo.PlasticityEnabled} " +
                $"mode={(brainInfo.PlasticityProbabilisticUpdates ? "probabilistic" : "absolute")} plasticityRate={brainInfo.PlasticityRate:0.######}");
        }
    }
    finally
    {
        await system.Remote().ShutdownAsync(true);
        await system.ShutdownAsync();
    }
}

static object ToAckPayload(IoCommandAck ack)
{
    return new
    {
        command = ack.Command,
        success = ack.Success,
        message = ack.Message,
        has_energy_state = ack.HasEnergyState,
        energy_state = ack.HasEnergyState && ack.EnergyState is not null
            ? new
            {
                cost_enabled = ack.EnergyState.CostEnabled,
                energy_enabled = ack.EnergyState.EnergyEnabled,
                energy_remaining = ack.EnergyState.EnergyRemaining,
                energy_rate_units_per_second = ack.EnergyState.EnergyRateUnitsPerSecond,
                plasticity_enabled = ack.EnergyState.PlasticityEnabled,
                plasticity_rate = ack.EnergyState.PlasticityRate,
                plasticity_probabilistic_updates = ack.EnergyState.PlasticityProbabilisticUpdates,
                last_tick_cost = ack.EnergyState.LastTickCost
            }
            : null
    };
}

static byte[] BuildMinimalNbn()
{
    var stride = 1024u;
    var sections = new List<NbnRegionSection>();
    var directory = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
    ulong offset = NbnBinary.NbnHeaderBytes;

    var inputAxons = new[]
    {
        new AxonRecord(strengthCode: 31, targetNeuronId: 0, targetRegionId: 1)
    };

    offset = AddRegionSection(
        0,
        1,
        stride,
        ref directory,
        sections,
        offset,
        neuronFactory: _ => new NeuronRecord(
            axonCount: 1,
            paramBCode: 0,
            paramACode: 0,
            activationThresholdCode: 0,
            preActivationThresholdCode: 0,
            resetFunctionId: 0,
            activationFunctionId: 1,
            accumulationFunctionId: 0,
            exists: true),
        axons: inputAxons);

    var demoAxons = new[]
    {
        new AxonRecord(strengthCode: 31, targetNeuronId: 0, targetRegionId: 1),
        new AxonRecord(strengthCode: 31, targetNeuronId: 0, targetRegionId: NbnConstants.OutputRegionId)
    };

    offset = AddRegionSection(
        1,
        1,
        stride,
        ref directory,
        sections,
        offset,
        neuronFactory: _ => new NeuronRecord(
            axonCount: 2,
            paramBCode: 0,
            paramACode: 40,
            activationThresholdCode: 0,
            preActivationThresholdCode: 0,
            resetFunctionId: 0,
            activationFunctionId: 17,
            accumulationFunctionId: 0,
            exists: true),
        axons: demoAxons);

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
    ulong offset,
    Func<int, NeuronRecord>? neuronFactory = null,
    AxonRecord[]? axons = null)
{
    var neurons = new NeuronRecord[neuronSpan];
    for (var i = 0; i < neurons.Length; i++)
    {
        neurons[i] = neuronFactory?.Invoke(i) ?? new NeuronRecord(
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

    ulong totalAxons = 0;
    for (var i = 0; i < neurons.Length; i++)
    {
        totalAxons += neurons[i].AxonCount;
    }

    axons ??= Array.Empty<AxonRecord>();
    if ((ulong)axons.Length != totalAxons)
    {
        throw new InvalidOperationException($"Region {regionId} axon count mismatch. Expected {totalAxons}, got {axons.Length}.");
    }

    var checkpointCount = (uint)((neuronSpan + stride - 1) / stride + 1);
    var checkpoints = new ulong[checkpointCount];
    var checkpointIndex = 1;
    var running = 0UL;
    uint nextBoundary = stride;
    for (var i = 0; i < neurons.Length; i++)
    {
        running += neurons[i].AxonCount;
        if ((uint)(i + 1) == nextBoundary && checkpointIndex < checkpointCount)
        {
            checkpoints[checkpointIndex++] = running;
            nextBoundary += stride;
        }
    }

    checkpoints[0] = 0;
    checkpoints[checkpointCount - 1] = running;
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

static long? GetLongArg(string[] args, string name)
{
    var value = GetArg(args, name);
    return long.TryParse(value, out var parsed) ? parsed : null;
}

static float? GetFloatArg(string[] args, string name)
{
    var value = GetArg(args, name);
    return float.TryParse(value, out var parsed) ? parsed : null;
}

static bool? GetBoolArg(string[] args, string name)
{
    var value = GetArg(args, name);
    if (string.IsNullOrWhiteSpace(value))
    {
        return null;
    }

    if (bool.TryParse(value, out var parsed))
    {
        return parsed;
    }

    if (value == "1")
    {
        return true;
    }

    if (value == "0")
    {
        return false;
    }

    return null;
}

static int? GetEnvInt(string key)
{
    var value = Environment.GetEnvironmentVariable(key);
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
    Console.WriteLine("  io-scenario --io-address <host:port> --io-id <name> --brain-id <guid>");
    Console.WriteLine("             [--credit <int64>] [--rate <int64>] [--cost-enabled <bool>] [--energy-enabled <bool>]");
    Console.WriteLine("             [--plasticity-enabled <bool>] [--plasticity-rate <float>] [--probabilistic <bool>] [--json]");
    Console.WriteLine("  run-brain --bind-host <host> --port <port> --brain-id <guid>");
    Console.WriteLine("            --hivemind-address <host:port> --hivemind-id <name>");
    Console.WriteLine("            [--router-id <name>] [--brain-root-id <name>]");
    Console.WriteLine("            [--io-address <host:port>] [--io-id <name>]");
}
