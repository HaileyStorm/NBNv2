using JsonSerializer = System.Text.Json.JsonSerializer;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Repro = Nbn.Proto.Repro;
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
    case "spawn-brain":
        await RunSpawnBrainAsync(remaining);
        break;
    case "io-scenario":
        await RunIoScenarioAsync(remaining);
        break;
    case "repro-scenario":
        await RunReproScenarioAsync(remaining);
        break;
    case "repro-suite":
        await RunReproSuiteAsync(remaining);
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
        system.Root.Send(ioPid, new Nbn.Proto.Io.RegisterBrain
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

static async Task RunSpawnBrainAsync(string[] args)
{
    var bindHost = GetArg(args, "--bind-host") ?? "127.0.0.1";
    var port = GetIntArg(args, "--port") ?? 12073;
    var advertisedHost = GetArg(args, "--advertise-host");
    var advertisedPort = GetIntArg(args, "--advertise-port");
    var ioAddress = GetArg(args, "--io-address") ?? throw new InvalidOperationException("--io-address is required.");
    var ioId = GetArg(args, "--io-id") ?? "io-gateway";
    var nbnSha = GetArg(args, "--nbn-sha256") ?? GetArg(args, "--brain-def-sha256") ?? throw new InvalidOperationException("--nbn-sha256 is required.");
    var nbnSize = GetULongArg(args, "--nbn-size") ?? GetULongArg(args, "--brain-def-size") ?? throw new InvalidOperationException("--nbn-size is required.");
    var storeUri = GetArg(args, "--store-uri") ?? GetArg(args, "--artifact-root");
    var mediaType = GetArg(args, "--media-type") ?? "application/x-nbn";
    var timeoutSeconds = GetIntArg(args, "--timeout-seconds") ?? 70;
    var waitSeconds = Math.Max(0, GetIntArg(args, "--wait-seconds") ?? 20);
    var jsonOnly = HasFlag(args, "--json");
    var timeout = TimeSpan.FromSeconds(Math.Max(1, timeoutSeconds));
    var brainDef = nbnSha.ToArtifactRef(nbnSize, mediaType, storeUri);

    SpawnBrainViaIOAck? response = null;
    SpawnBrainAck? ack = null;
    Guid brainId = Guid.Empty;
    bool registrationObserved = false;
    string? registrationStatus = null;

    var system = new ActorSystem();
    var remoteConfig = BuildRemoteConfig(bindHost, port, advertisedHost, advertisedPort);
    system.WithRemote(remoteConfig);
    await system.Remote().StartAsync();

    try
    {
        var ioPid = new PID(ioAddress, ioId);

        try
        {
            response = await system.Root.RequestAsync<SpawnBrainViaIOAck>(
                ioPid,
                new SpawnBrainViaIO
                {
                    Request = new SpawnBrain
                    {
                        BrainDef = brainDef
                    }
                },
                timeout);
        }
        catch (Exception ex)
        {
            ack = new SpawnBrainAck
            {
                FailureReasonCode = "spawn_request_failed",
                FailureMessage = $"Spawn request failed: {ex.GetBaseException().Message}"
            };
        }

        ack ??= response?.Ack ?? new SpawnBrainAck
        {
            FailureReasonCode = "spawn_empty_response",
            FailureMessage = "Spawn request failed: IO returned an empty acknowledgment."
        };

        if (ack.BrainId.TryToGuid(out var parsedBrainId))
        {
            brainId = parsedBrainId;
        }

        if (brainId == Guid.Empty)
        {
            registrationStatus = "spawn_failed";
        }
        else if (waitSeconds == 0)
        {
            registrationStatus = "wait_skipped";
        }
        else
        {
            var deadline = DateTime.UtcNow.AddSeconds(waitSeconds);
            while (DateTime.UtcNow <= deadline)
            {
                BrainInfo? info = null;
                try
                {
                    info = await system.Root.RequestAsync<BrainInfo>(
                        ioPid,
                        new BrainInfoRequest
                        {
                            BrainId = brainId.ToProtoUuid()
                        },
                        timeout);
                }
                catch
                {
                    // Keep waiting until deadline; transient startup races are expected.
                }

                if (info is not null && (info.InputWidth > 0 || info.OutputWidth > 0))
                {
                    registrationObserved = true;
                    break;
                }

                await Task.Delay(250).ConfigureAwait(false);
            }

            registrationStatus = registrationObserved ? "registered" : "registration_timeout";
        }

        var brainIdText = brainId == Guid.Empty ? string.Empty : brainId.ToString("D");
        var payload = new
        {
            io_address = ioAddress,
            io_id = ioId,
            wait_seconds = waitSeconds,
            brain_def = ToArtifactPayload(brainDef),
            spawn_ack = new
            {
                brain_id = brainIdText,
                failure_reason_code = ack.FailureReasonCode ?? string.Empty,
                failure_message = ack.FailureMessage ?? string.Empty
            },
            failure_reason_code = response?.FailureReasonCode ?? ack.FailureReasonCode ?? string.Empty,
            failure_message = response?.FailureMessage ?? ack.FailureMessage ?? string.Empty,
            registration_observed = registrationObserved,
            registration_status = registrationStatus ?? string.Empty
        };

        var json = JsonSerializer.Serialize(payload);
        Console.WriteLine(json);

        if (!jsonOnly)
        {
            Console.WriteLine($"Spawn requested via {ioAddress}/{ioId}");
            Console.WriteLine($"BrainId: {(string.IsNullOrWhiteSpace(brainIdText) ? "(empty)" : brainIdText)}");
            Console.WriteLine($"Failure: {(string.IsNullOrWhiteSpace(payload.failure_reason_code) ? "(none)" : payload.failure_reason_code)}");
            Console.WriteLine($"Registration: {payload.registration_status}");
        }
    }
    finally
    {
        await system.Remote().ShutdownAsync(true);
        await system.ShutdownAsync();
    }
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

static async Task RunReproScenarioAsync(string[] args)
{
    var bindHost = GetArg(args, "--bind-host") ?? "127.0.0.1";
    var port = GetIntArg(args, "--port") ?? 12071;
    var advertisedHost = GetArg(args, "--advertise-host");
    var advertisedPort = GetIntArg(args, "--advertise-port");
    var ioAddress = GetArg(args, "--io-address") ?? throw new InvalidOperationException("--io-address is required.");
    var ioId = GetArg(args, "--io-id") ?? "io-gateway";
    var parentASha = GetArg(args, "--parent-a-sha256") ?? GetArg(args, "--parent-sha256") ?? throw new InvalidOperationException("--parent-a-sha256 is required.");
    var parentBSha = GetArg(args, "--parent-b-sha256") ?? parentASha;
    var parentASize = GetULongArg(args, "--parent-a-size") ?? GetULongArg(args, "--parent-size") ?? throw new InvalidOperationException("--parent-a-size is required.");
    var parentBSize = GetULongArg(args, "--parent-b-size") ?? parentASize;
    var storeUri = GetArg(args, "--store-uri") ?? GetArg(args, "--artifact-root");
    var seed = GetULongArg(args, "--seed") ?? 12345UL;
    var timeoutSeconds = GetIntArg(args, "--timeout-seconds") ?? 10;
    var jsonOnly = HasFlag(args, "--json");
    var timeout = TimeSpan.FromSeconds(Math.Max(1, timeoutSeconds));

    var spawnPolicy = ParseSpawnPolicy(GetArg(args, "--spawn-policy") ?? "never");
    var strengthSource = ParseStrengthSource(GetArg(args, "--strength-source"));

    var parentARef = parentASha.ToArtifactRef(parentASize, "application/x-nbn", storeUri);
    var parentBRef = parentBSha.ToArtifactRef(parentBSize, "application/x-nbn", storeUri);

    var request = new Repro.ReproduceByArtifactsRequest
    {
        ParentADef = parentARef,
        ParentBDef = parentBRef,
        StrengthSource = strengthSource,
        Config = new Repro.ReproduceConfig
        {
            SpawnChild = spawnPolicy
        },
        Seed = seed
    };

    var system = new ActorSystem();
    var remoteConfig = BuildRemoteConfig(bindHost, port, advertisedHost, advertisedPort);
    system.WithRemote(remoteConfig);
    await system.Remote().StartAsync();

    try
    {
        var ioPid = new PID(ioAddress, ioId);
        var response = await system.Root.RequestAsync<ReproduceResult>(
            ioPid,
            new ReproduceByArtifacts { Request = request },
            timeout);

        var result = response?.Result;
        var report = result?.Report;
        var summary = result?.Summary;
        var childBrainIdText = string.Empty;
        if (result?.ChildBrainId is not null
            && result.ChildBrainId.TryToGuid(out var childBrainId)
            && childBrainId != Guid.Empty)
        {
            childBrainIdText = childBrainId.ToString("D");
        }

        var payload = new
        {
            io_address = ioAddress,
            io_id = ioId,
            seed,
            strength_source = strengthSource.ToString(),
            spawn_policy = spawnPolicy.ToString(),
            parent_a = ToArtifactPayload(parentARef),
            parent_b = ToArtifactPayload(parentBRef),
            result = result is null
                ? null
                : new
                {
                    compatible = report?.Compatible ?? false,
                    abort_reason = report?.AbortReason ?? string.Empty,
                    similarity_score = report?.SimilarityScore ?? 0f,
                    region_span_score = report?.RegionSpanScore ?? 0f,
                    function_score = report?.FunctionScore ?? 0f,
                    connectivity_score = report?.ConnectivityScore ?? 0f,
                    regions_present_a = report?.RegionsPresentA ?? 0u,
                    regions_present_b = report?.RegionsPresentB ?? 0u,
                    regions_present_child = report?.RegionsPresentChild ?? 0u,
                    summary = new
                    {
                        neurons_added = summary?.NeuronsAdded ?? 0u,
                        neurons_removed = summary?.NeuronsRemoved ?? 0u,
                        axons_added = summary?.AxonsAdded ?? 0u,
                        axons_removed = summary?.AxonsRemoved ?? 0u,
                        axons_rerouted = summary?.AxonsRerouted ?? 0u,
                        functions_mutated = summary?.FunctionsMutated ?? 0u,
                        strength_codes_changed = summary?.StrengthCodesChanged ?? 0u
                    },
                    child_def = ToArtifactPayload(result.ChildDef),
                    spawned = result.Spawned,
                    child_brain_id = childBrainIdText
                }
        };

        var json = JsonSerializer.Serialize(payload);
        Console.WriteLine(json);

        if (!jsonOnly)
        {
            Console.WriteLine($"Repro request sent to {ioAddress}/{ioId}");
            Console.WriteLine($"Compatible: {report?.Compatible ?? false}");
            Console.WriteLine($"Abort reason: {(string.IsNullOrWhiteSpace(report?.AbortReason) ? "(none)" : report!.AbortReason)}");
            Console.WriteLine($"Spawned: {result?.Spawned ?? false}");
            Console.WriteLine($"Child brain: {(string.IsNullOrWhiteSpace(childBrainIdText) ? "(none)" : childBrainIdText)}");
        }
    }
    finally
    {
        await system.Remote().ShutdownAsync(true);
        await system.ShutdownAsync();
    }
}

static async Task RunReproSuiteAsync(string[] args)
{
    var bindHost = GetArg(args, "--bind-host") ?? "127.0.0.1";
    var port = GetIntArg(args, "--port") ?? 12072;
    var advertisedHost = GetArg(args, "--advertise-host");
    var advertisedPort = GetIntArg(args, "--advertise-port");
    var ioAddress = GetArg(args, "--io-address") ?? throw new InvalidOperationException("--io-address is required.");
    var ioId = GetArg(args, "--io-id") ?? "io-gateway";
    var parentASha = GetArg(args, "--parent-a-sha256") ?? GetArg(args, "--parent-sha256") ?? throw new InvalidOperationException("--parent-a-sha256 is required.");
    var parentASize = GetULongArg(args, "--parent-a-size") ?? GetULongArg(args, "--parent-size") ?? throw new InvalidOperationException("--parent-a-size is required.");
    var storeUri = GetArg(args, "--store-uri") ?? GetArg(args, "--artifact-root");
    var seed = GetULongArg(args, "--seed") ?? 12345UL;
    var timeoutSeconds = GetIntArg(args, "--timeout-seconds") ?? 10;
    var jsonOnly = HasFlag(args, "--json");
    var failOnCaseFailure = HasFlag(args, "--fail-on-case-failure");
    var timeout = TimeSpan.FromSeconds(Math.Max(1, timeoutSeconds));

    var baseParent = parentASha.ToArtifactRef(parentASize, "application/x-nbn", storeUri);
    var missingParent = new string('0', 64).ToArtifactRef(parentASize, "application/x-nbn", storeUri);
    var parentBInvalidMedia = parentASha.ToArtifactRef(parentASize, "application/x-nbs", storeUri);

    var variantRoot = ResolveArtifactRootForWrite(storeUri);
    Directory.CreateDirectory(variantRoot);
    var variantStoreUri = string.IsNullOrWhiteSpace(storeUri) ? variantRoot : storeUri;
    var variantStore = new LocalArtifactStore(new ArtifactStoreOptions(variantRoot));
    var variantBytes = BuildRegionSpanMismatchNbn();
    var variantManifest = await variantStore.StoreAsync(new MemoryStream(variantBytes, writable: false), "application/x-nbn");
    var variantParent = variantManifest.ArtifactId.ToHex().ToArtifactRef((ulong)Math.Max(0, variantManifest.ByteLength), "application/x-nbn", variantStoreUri);

    var caseRows = new List<object>();
    var passedCases = 0;
    var ioPid = new PID(ioAddress, ioId);

    var system = new ActorSystem();
    var remoteConfig = BuildRemoteConfig(bindHost, port, advertisedHost, advertisedPort);
    system.WithRemote(remoteConfig);
    await system.Remote().StartAsync();

    try
    {
        await RunCaseAsync(
            "compatible_spawn_never",
            "Base parents match and spawn policy is never.",
            new
            {
                compatible = true,
                abort_reason = "",
                child_def_present = true,
                spawned = false
            },
            CreateArtifactsRequest(
                baseParent,
                baseParent,
                Repro.StrengthSource.StrengthBaseOnly,
                Repro.SpawnChildPolicy.SpawnChildNever,
                seed,
                null),
            validate: (actual, failures) =>
            {
                ExpectResult(actual, failures);
                ExpectEqual(actual.AbortReason, string.Empty, "abort_reason", failures);
                ExpectTrue(actual.Compatible, "compatible", failures);
                ExpectTrue(actual.ChildDefPresent, "child_def_present", failures);
                ExpectFalse(actual.Spawned, "spawned", failures);
            });

        await RunCaseAsync(
            "missing_parent_b_def",
            "Parent B definition omitted.",
            new
            {
                compatible = false,
                abort_reason = "repro_missing_parent_b_def"
            },
            new Repro.ReproduceByArtifactsRequest
            {
                ParentADef = baseParent,
                ParentBDef = null,
                StrengthSource = Repro.StrengthSource.StrengthBaseOnly,
                Config = new Repro.ReproduceConfig { SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever },
                Seed = seed + 1
            },
            validate: (actual, failures) =>
            {
                ExpectResult(actual, failures);
                ExpectEqual(actual.AbortReason, "repro_missing_parent_b_def", "abort_reason", failures);
                ExpectFalse(actual.Compatible, "compatible", failures);
            });

        await RunCaseAsync(
            "parent_b_media_type_invalid",
            "Parent B media type is not application/x-nbn.",
            new
            {
                compatible = false,
                abort_reason = "repro_parent_b_media_type_invalid"
            },
            CreateArtifactsRequest(
                baseParent,
                parentBInvalidMedia,
                Repro.StrengthSource.StrengthBaseOnly,
                Repro.SpawnChildPolicy.SpawnChildNever,
                seed + 2,
                null),
            validate: (actual, failures) =>
            {
                ExpectResult(actual, failures);
                ExpectEqual(actual.AbortReason, "repro_parent_b_media_type_invalid", "abort_reason", failures);
                ExpectFalse(actual.Compatible, "compatible", failures);
            });

        await RunCaseAsync(
            "parent_a_artifact_not_found",
            "Parent A hash not present in artifact store.",
            new
            {
                compatible = false,
                abort_reason = "repro_parent_a_artifact_not_found"
            },
            CreateArtifactsRequest(
                missingParent,
                baseParent,
                Repro.StrengthSource.StrengthBaseOnly,
                Repro.SpawnChildPolicy.SpawnChildNever,
                seed + 3,
                null),
            validate: (actual, failures) =>
            {
                ExpectResult(actual, failures);
                ExpectEqual(actual.AbortReason, "repro_parent_a_artifact_not_found", "abort_reason", failures);
                ExpectFalse(actual.Compatible, "compatible", failures);
            });

        await RunCaseAsync(
            "region_span_mismatch",
            "Parent B has mismatched region span with zero span tolerance.",
            new
            {
                compatible = false,
                abort_reason = "repro_region_span_mismatch"
            },
            CreateArtifactsRequest(
                baseParent,
                variantParent,
                Repro.StrengthSource.StrengthBaseOnly,
                Repro.SpawnChildPolicy.SpawnChildNever,
                seed + 4,
                new Repro.ReproduceConfig
                {
                    SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever,
                    MaxRegionSpanDiffRatio = 0f
                }),
            validate: (actual, failures) =>
            {
                ExpectResult(actual, failures);
                ExpectEqual(actual.AbortReason, "repro_region_span_mismatch", "abort_reason", failures);
                ExpectFalse(actual.Compatible, "compatible", failures);
            });

        await RunCaseAsync(
            "strength_live_without_state",
            "Strength source live codes with no parent state refs should fall back cleanly.",
            new
            {
                compatible = true,
                abort_reason = "",
                child_def_present = true
            },
            CreateArtifactsRequest(
                baseParent,
                baseParent,
                Repro.StrengthSource.StrengthLiveCodes,
                Repro.SpawnChildPolicy.SpawnChildNever,
                seed + 5,
                null),
            validate: (actual, failures) =>
            {
                ExpectResult(actual, failures);
                ExpectEqual(actual.AbortReason, string.Empty, "abort_reason", failures);
                ExpectTrue(actual.Compatible, "compatible", failures);
                ExpectTrue(actual.ChildDefPresent, "child_def_present", failures);
            });

        await RunCaseAsync(
            "spawn_always_attempt",
            "Spawn policy always: either child spawns or a spawn-specific abort reason is returned.",
            new
            {
                child_def_present = true,
                spawned_or_spawn_abort_or_timeout = new[]
                {
                    "spawned",
                    "repro_spawn_unavailable",
                    "repro_child_artifact_missing",
                    "repro_spawn_failed",
                    "repro_spawn_request_failed",
                    "request_timeout"
                }
            },
            CreateArtifactsRequest(
                baseParent,
                baseParent,
                Repro.StrengthSource.StrengthBaseOnly,
                Repro.SpawnChildPolicy.SpawnChildAlways,
                seed + 6,
                null),
            validate: (actual, failures) =>
            {
                if (!actual.ResultPresent)
                {
                    if (!IsExpectedRequestTimeout(actual.ExceptionMessage))
                    {
                        ExpectResult(actual, failures);
                    }
                    return;
                }

                ExpectTrue(actual.ChildDefPresent, "child_def_present", failures);
                if (actual.Spawned)
                {
                    if (string.IsNullOrWhiteSpace(actual.ChildBrainId))
                    {
                        failures.Add("child_brain_id expected when spawned=true");
                    }
                }
                else
                {
                    var allowed = new HashSet<string>(StringComparer.Ordinal)
                    {
                        "repro_spawn_unavailable",
                        "repro_child_artifact_missing",
                        "repro_spawn_failed",
                        "repro_spawn_request_failed"
                    };
                    if (!allowed.Contains(actual.AbortReason))
                    {
                        failures.Add($"abort_reason expected spawn failure code, got '{actual.AbortReason}'.");
                    }
                }
            });

        var totalCases = caseRows.Count;
        var failedCases = totalCases - passedCases;
        var payload = new
        {
            suite = "nbn-repro-suite",
            io_address = ioAddress,
            io_id = ioId,
            seed,
            variant_parent = ToArtifactPayload(variantParent),
            total_cases = totalCases,
            passed_cases = passedCases,
            failed_cases = failedCases,
            all_passed = failedCases == 0,
            cases = caseRows
        };

        var json = JsonSerializer.Serialize(payload);
        Console.WriteLine(json);

        if (!jsonOnly)
        {
            Console.WriteLine($"Repro suite: {passedCases}/{totalCases} passed.");
        }

        if (failOnCaseFailure && failedCases > 0)
        {
            Environment.ExitCode = 2;
        }
    }
    finally
    {
        await system.Remote().ShutdownAsync(true);
        await system.ShutdownAsync();
    }

    return;

    async Task RunCaseAsync(
        string caseName,
        string description,
        object expected,
        Repro.ReproduceByArtifactsRequest request,
        Action<ReproObservation, List<string>> validate)
    {
        var started = DateTimeOffset.UtcNow;
        Repro.ReproduceResult? result = null;
        Exception? error = null;
        try
        {
            result = await RequestReproduceByArtifactsAsync(system, ioPid, request, timeout);
        }
        catch (Exception ex)
        {
            error = ex;
        }

        var elapsedMs = Math.Max(0L, (long)(DateTimeOffset.UtcNow - started).TotalMilliseconds);
        var observation = Observe(result, error);
        var failures = new List<string>();
        validate(observation, failures);
        var passed = failures.Count == 0;
        if (passed)
        {
            passedCases++;
        }

        caseRows.Add(new
        {
            name = caseName,
            description,
            passed,
            duration_ms = elapsedMs,
            expected,
            actual = new
            {
                result_present = observation.ResultPresent,
                exception = observation.ExceptionMessage,
                compatible = observation.Compatible,
                abort_reason = observation.AbortReason,
                child_def_present = observation.ChildDefPresent,
                spawned = observation.Spawned,
                child_brain_id = observation.ChildBrainId
            },
            failures
        });
    }
}

static ReproObservation Observe(Repro.ReproduceResult? result, Exception? error)
{
    var report = result?.Report;
    return new ReproObservation(
        result is not null,
        report?.Compatible ?? false,
        NormalizeAbortReason(report),
        HasValidArtifactRef(result?.ChildDef),
        result?.Spawned ?? false,
        ExtractChildBrainId(result),
        error?.Message ?? string.Empty);
}

static async Task<Repro.ReproduceResult?> RequestReproduceByArtifactsAsync(
    ActorSystem system,
    PID ioPid,
    Repro.ReproduceByArtifactsRequest request,
    TimeSpan timeout)
{
    var response = await system.Root.RequestAsync<ReproduceResult>(
        ioPid,
        new ReproduceByArtifacts { Request = request },
        timeout);
    return response?.Result;
}

static Repro.ReproduceByArtifactsRequest CreateArtifactsRequest(
    ArtifactRef parentA,
    ArtifactRef? parentB,
    Repro.StrengthSource strengthSource,
    Repro.SpawnChildPolicy spawnChildPolicy,
    ulong seed,
    Repro.ReproduceConfig? config)
{
    config ??= new Repro.ReproduceConfig();
    config.SpawnChild = spawnChildPolicy;
    return new Repro.ReproduceByArtifactsRequest
    {
        ParentADef = parentA,
        ParentBDef = parentB,
        StrengthSource = strengthSource,
        Config = config,
        Seed = seed
    };
}

static string NormalizeAbortReason(Repro.SimilarityReport? report)
{
    var reason = report?.AbortReason;
    return string.IsNullOrWhiteSpace(reason) ? string.Empty : reason.Trim();
}

static string ExtractChildBrainId(Repro.ReproduceResult? result)
{
    if (result?.ChildBrainId is not null
        && result.ChildBrainId.TryToGuid(out var id)
        && id != Guid.Empty)
    {
        return id.ToString("D");
    }

    return string.Empty;
}

static bool HasValidArtifactRef(ArtifactRef? reference)
{
    if (reference is null || !reference.TryToSha256Bytes(out var bytes))
    {
        return false;
    }

    return bytes.Length == 32;
}

static void ExpectResult(ReproObservation actual, ICollection<string> failures)
{
    if (!actual.ResultPresent)
    {
        failures.Add(string.IsNullOrWhiteSpace(actual.ExceptionMessage)
            ? "result missing"
            : $"result missing: {actual.ExceptionMessage}");
    }
}

static void ExpectEqual(string actual, string expected, string field, ICollection<string> failures)
{
    if (!string.Equals(actual, expected, StringComparison.Ordinal))
    {
        failures.Add($"{field} expected '{expected}', got '{actual}'.");
    }
}

static void ExpectTrue(bool actual, string field, ICollection<string> failures)
{
    if (!actual)
    {
        failures.Add($"{field} expected true, got false.");
    }
}

static void ExpectFalse(bool actual, string field, ICollection<string> failures)
{
    if (actual)
    {
        failures.Add($"{field} expected false, got true.");
    }
}

static bool IsExpectedRequestTimeout(string? exceptionMessage)
{
    if (string.IsNullOrWhiteSpace(exceptionMessage))
    {
        return false;
    }

    return exceptionMessage.Contains("within the expected time", StringComparison.OrdinalIgnoreCase)
        || exceptionMessage.Contains("timed out", StringComparison.OrdinalIgnoreCase)
        || exceptionMessage.Contains("timeout", StringComparison.OrdinalIgnoreCase);
}

static string ResolveArtifactRootForWrite(string? storeUri)
{
    if (!string.IsNullOrWhiteSpace(storeUri))
    {
        if (Uri.TryCreate(storeUri, UriKind.Absolute, out var uri) && uri.IsFile)
        {
            return uri.LocalPath;
        }

        if (!storeUri.Contains("://", StringComparison.Ordinal))
        {
            return storeUri;
        }
    }

    var envRoot = Environment.GetEnvironmentVariable("NBN_ARTIFACT_ROOT");
    if (!string.IsNullOrWhiteSpace(envRoot))
    {
        return envRoot;
    }

    return Path.Combine(Environment.CurrentDirectory, "artifacts");
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

static byte[] BuildRegionSpanMismatchNbn()
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

    var regionOneAxons = new[]
    {
        new AxonRecord(strengthCode: 31, targetNeuronId: 0, targetRegionId: NbnConstants.OutputRegionId)
    };

    offset = AddRegionSection(
        1,
        2,
        stride,
        ref directory,
        sections,
        offset,
        neuronFactory: neuronId => new NeuronRecord(
            axonCount: neuronId == 0 ? (ushort)1 : (ushort)0,
            paramBCode: 0,
            paramACode: 0,
            activationThresholdCode: 0,
            preActivationThresholdCode: 0,
            resetFunctionId: 0,
            activationFunctionId: 1,
            accumulationFunctionId: 0,
            exists: true),
        axons: regionOneAxons);

    AddRegionSection(NbnConstants.OutputRegionId, 1, stride, ref directory, sections, offset);

    var header = new NbnHeaderV2(
        "NBN2",
        2,
        1,
        10,
        brainSeed: 2,
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

static ulong? GetULongArg(string[] args, string name)
{
    var value = GetArg(args, name);
    return ulong.TryParse(value, out var parsed) ? parsed : null;
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

static Repro.StrengthSource ParseStrengthSource(string? raw)
{
    if (string.IsNullOrWhiteSpace(raw))
    {
        return Repro.StrengthSource.StrengthBaseOnly;
    }

    return raw.Trim().ToLowerInvariant() switch
    {
        "live" => Repro.StrengthSource.StrengthLiveCodes,
        "live_codes" => Repro.StrengthSource.StrengthLiveCodes,
        "strength_live_codes" => Repro.StrengthSource.StrengthLiveCodes,
        _ => Repro.StrengthSource.StrengthBaseOnly
    };
}

static Repro.SpawnChildPolicy ParseSpawnPolicy(string raw)
{
    return raw.Trim().ToLowerInvariant() switch
    {
        "never" => Repro.SpawnChildPolicy.SpawnChildNever,
        "spawn_child_never" => Repro.SpawnChildPolicy.SpawnChildNever,
        "always" => Repro.SpawnChildPolicy.SpawnChildAlways,
        "spawn_child_always" => Repro.SpawnChildPolicy.SpawnChildAlways,
        _ => Repro.SpawnChildPolicy.SpawnChildDefaultOn
    };
}

static object? ToArtifactPayload(ArtifactRef? reference)
{
    if (reference is null || !reference.TryToSha256Bytes(out var bytes))
    {
        return null;
    }

    return new
    {
        sha256 = Convert.ToHexString(bytes).ToLowerInvariant(),
        media_type = reference.MediaType,
        size_bytes = reference.SizeBytes,
        store_uri = reference.StoreUri
    };
}

static string PidLabel(PID pid)
    => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

static void PrintHelp()
{
    Console.WriteLine("NBN DemoHost usage:");
    Console.WriteLine("  init-artifacts --artifact-root <path> [--json]");
    Console.WriteLine("  spawn-brain --io-address <host:port> [--io-id <name>] --nbn-sha256 <hex> --nbn-size <bytes>");
    Console.WriteLine("              [--store-uri <path|file://uri>] [--media-type <mime>] [--timeout-seconds <int>] [--wait-seconds <int>] [--json]");
    Console.WriteLine("  io-scenario --io-address <host:port> --io-id <name> --brain-id <guid>");
    Console.WriteLine("             [--credit <int64>] [--rate <int64>] [--cost-enabled <bool>] [--energy-enabled <bool>]");
    Console.WriteLine("             [--plasticity-enabled <bool>] [--plasticity-rate <float>] [--probabilistic <bool>] [--json]");
    Console.WriteLine("  repro-scenario --io-address <host:port> [--io-id <name>] --parent-a-sha256 <hex> --parent-a-size <bytes>");
    Console.WriteLine("                [--parent-b-sha256 <hex>] [--parent-b-size <bytes>] [--store-uri <path|file://uri>] [--seed <uint64>]");
    Console.WriteLine("                [--spawn-policy default|never|always] [--strength-source base|live] [--json]");
    Console.WriteLine("  repro-suite --io-address <host:port> [--io-id <name>] --parent-a-sha256 <hex> --parent-a-size <bytes>");
    Console.WriteLine("              [--store-uri <path|file://uri>] [--seed <uint64>] [--fail-on-case-failure] [--json]");
    Console.WriteLine("  run-brain --bind-host <host> --port <port> --brain-id <guid>");
    Console.WriteLine("            --hivemind-address <host:port> --hivemind-id <name>");
    Console.WriteLine("            [--router-id <name>] [--brain-root-id <name>]");
    Console.WriteLine("            [--io-address <host:port>] [--io-id <name>]");
}

readonly record struct ReproObservation(
    bool ResultPresent,
    bool Compatible,
    string AbortReason,
    bool ChildDefPresent,
    bool Spawned,
    string ChildBrainId,
    string ExceptionMessage);
