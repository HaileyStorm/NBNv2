using System.Text.Json;
using System.Globalization;
using Nbn.Proto;
using Nbn.Shared;
using Nbn.Tools.EvolutionSim;
using Repro = Nbn.Proto.Repro;

Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });
Console.SetError(new StreamWriter(Console.OpenStandardError()) { AutoFlush = true });

if (args.Length == 0 || IsHelpToken(args[0]))
{
    PrintHelp();
    return;
}

var command = args[0].Trim().ToLowerInvariant();
var remaining = args.Skip(1).ToArray();

switch (command)
{
    case "run":
        await RunAsync(remaining);
        break;
    default:
        PrintHelp();
        break;
}

static async Task RunAsync(string[] args)
{
    try
    {
        var ioAddress = GetArg(args, "--io-address")
                        ?? throw new InvalidOperationException("--io-address is required.");
        var ioId = GetArg(args, "--io-id") ?? "io-gateway";
        var settingsAddress = GetArg(args, "--settings-address");
        var settingsName = GetArg(args, "--settings-name") ?? "SettingsMonitor";
        var bindHost = GetArg(args, "--bind-host") ?? "127.0.0.1";
        var port = GetIntArg(args, "--port") ?? 12074;
        var advertisedHost = GetArg(args, "--advertise-host");
        var advertisedPort = GetIntArg(args, "--advertise-port");
        var seed = GetULongArg(args, "--seed") ?? 12345UL;
        var intervalMs = Math.Max(0, GetIntArg(args, "--interval-ms") ?? 1000);
        var statusSeconds = Math.Max(1, GetIntArg(args, "--status-seconds") ?? 5);
        var requestTimeoutSeconds = Math.Max(1, GetIntArg(args, "--timeout-seconds") ?? 10);
        var maxIterations = Math.Max(0, GetIntArg(args, "--max-iterations") ?? 0);
        var maxPoolSize = Math.Max(2, GetIntArg(args, "--max-parent-pool") ?? 512);
        var commitToSpeciation = GetBoolArg(args, "--commit-to-speciation") ?? true;
        var spawnChildren = GetBoolArg(args, "--spawn-children") ?? false;
        var strengthSource = ParseStrengthSource(GetArg(args, "--strength-source"));
        var minRuns = (uint)Math.Max(1, GetIntArg(args, "--min-runs") ?? 1);
        var maxRuns = (uint)Math.Max((int)minRuns, GetIntArg(args, "--max-runs") ?? 6);
        var gamma = GetDoubleArg(args, "--run-gamma") ?? 1d;
        var jsonOnly = HasFlag(args, "--json");

        if (maxRuns > 64)
        {
            throw new InvalidOperationException("--max-runs must be <= 64.");
        }

        var parentResolution = ResolveParentPool(args);
        if (parentResolution.Parents.Count < 2)
        {
            throw new InvalidOperationException("At least two parent references are required.");
        }
        ValidateParentPool(parentResolution);

        var options = new EvolutionSimulationOptions
        {
            IoAddress = ioAddress,
            IoId = ioId,
            SettingsAddress = settingsAddress,
            SettingsName = settingsName,
            BindHost = bindHost,
            Port = port,
            AdvertiseHost = advertisedHost,
            AdvertisePort = advertisedPort,
            Seed = seed,
            Interval = TimeSpan.FromMilliseconds(intervalMs),
            MaxIterations = maxIterations,
            MaxParentPoolSize = maxPoolSize,
            RequestTimeout = TimeSpan.FromSeconds(requestTimeoutSeconds),
            CommitToSpeciation = commitToSpeciation,
            SpawnChildren = spawnChildren,
            ParentMode = parentResolution.Mode,
            StrengthSource = strengthSource,
            RunPolicy = new InverseCompatibilityRunPolicy(minRuns, maxRuns, gamma)
        };

        await using var runtimeClient = await EvolutionRuntimeClient.StartAsync(options).ConfigureAwait(false);
        var session = new EvolutionSimulationSession(options, parentResolution.Parents, runtimeClient);
        var controller = new EvolutionSimulationController(session);
        if (!controller.Start())
        {
            throw new InvalidOperationException("Failed to start evolution simulation session.");
        }

        using var stopSignal = new CancellationTokenSource();
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            stopSignal.Cancel();
        };

        var statusInterval = TimeSpan.FromSeconds(statusSeconds);
        while (true)
        {
            var runTask = controller.CurrentSessionTask;
            if (runTask is null)
            {
                break;
            }

            if (stopSignal.IsCancellationRequested)
            {
                await controller.StopAsync().ConfigureAwait(false);
                break;
            }

            var completed = await Task.WhenAny(
                    runTask,
                    Task.Delay(statusInterval, stopSignal.Token))
                .ConfigureAwait(false);

            if (completed == runTask)
            {
                await runTask.ConfigureAwait(false);
                break;
            }

            EmitStatus(controller.GetStatus(), jsonOnly, isFinal: false);
        }

        EmitStatus(controller.GetStatus(), jsonOnly, isFinal: true);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"evolution-sim failed: {ex.GetBaseException().Message}");
        Environment.ExitCode = 1;
    }
}

static (EvolutionParentMode Mode, IReadOnlyList<EvolutionParentRef> Parents) ResolveParentPool(string[] args)
{
    var brainParents = ResolveBrainParentPool(args);
    var artifactParents = ResolveArtifactParentPool(args);

    if (brainParents.Count > 0 && artifactParents.Count > 0)
    {
        throw new InvalidOperationException("Do not mix artifact parents and brain_id parents in the same run.");
    }

    if (brainParents.Count > 0)
    {
        return (
            EvolutionParentMode.BrainIds,
            brainParents.Select(EvolutionParentRef.FromBrainId).ToArray());
    }

    return (
        EvolutionParentMode.ArtifactRefs,
        artifactParents.Select(EvolutionParentRef.FromArtifactRef).ToArray());
}

static IReadOnlyList<Guid> ResolveBrainParentPool(string[] args)
{
    var pool = new List<Guid>();
    foreach (var rawBrainId in GetArgs(args, "--parent-brain"))
    {
        pool.Add(ParseParentBrainId(rawBrainId, "--parent-brain"));
    }

    var parentsFile = GetArg(args, "--parents-brain-file");
    if (!string.IsNullOrWhiteSpace(parentsFile))
    {
        pool.AddRange(LoadBrainParentsFile(parentsFile));
    }

    return pool;
}

static IReadOnlyList<ArtifactRef> ResolveArtifactParentPool(string[] args)
{
    var pool = new List<ArtifactRef>();
    var defaultStoreUri = GetArg(args, "--store-uri");
    if (string.IsNullOrWhiteSpace(defaultStoreUri))
    {
        defaultStoreUri = Environment.GetEnvironmentVariable("NBN_ARTIFACT_ROOT");
    }

    foreach (var parentSpec in GetArgs(args, "--parent"))
    {
        pool.Add(ParseParentSpec(parentSpec, defaultStoreUri));
    }

    var parentsFile = GetArg(args, "--parents-file");
    if (!string.IsNullOrWhiteSpace(parentsFile))
    {
        pool.AddRange(LoadParentsFile(parentsFile, defaultStoreUri));
    }

    if (pool.Count > 0)
    {
        return pool;
    }

    var parentASha = GetArg(args, "--parent-a-sha256");
    var parentASize = GetULongArg(args, "--parent-a-size");
    if (string.IsNullOrWhiteSpace(parentASha) || !parentASize.HasValue)
    {
        return pool;
    }

    var parentAMediaType = GetArg(args, "--parent-a-media-type") ?? "application/x-nbn";
    var parentAStoreUri = GetArg(args, "--parent-a-store-uri") ?? defaultStoreUri;
    var parentA = parentASha.ToArtifactRef(parentASize.Value, parentAMediaType, parentAStoreUri);
    pool.Add(parentA);

    var parentBSha = GetArg(args, "--parent-b-sha256") ?? parentASha;
    var parentBSize = GetULongArg(args, "--parent-b-size") ?? parentASize.Value;
    var parentBMediaType = GetArg(args, "--parent-b-media-type") ?? parentAMediaType;
    var parentBStoreUri = GetArg(args, "--parent-b-store-uri") ?? parentAStoreUri;
    var parentB = parentBSha.ToArtifactRef(parentBSize, parentBMediaType, parentBStoreUri);
    pool.Add(parentB);

    return pool;
}

static IReadOnlyList<ArtifactRef> LoadParentsFile(string path, string? defaultStoreUri)
{
    if (!File.Exists(path))
    {
        throw new InvalidOperationException($"--parents-file not found: {path}");
    }

    var entries = new List<ArtifactRef>();
    foreach (var line in File.ReadLines(path))
    {
        var trimmed = line.Trim();
        if (trimmed.Length == 0 || trimmed.StartsWith('#'))
        {
            continue;
        }

        entries.Add(ParseParentSpec(trimmed, defaultStoreUri));
    }

    return entries;
}

static IReadOnlyList<Guid> LoadBrainParentsFile(string path)
{
    if (!File.Exists(path))
    {
        throw new InvalidOperationException($"--parents-brain-file not found: {path}");
    }

    var entries = new List<Guid>();
    foreach (var line in File.ReadLines(path))
    {
        var trimmed = line.Trim();
        if (trimmed.Length == 0 || trimmed.StartsWith('#'))
        {
            continue;
        }

        entries.Add(ParseParentBrainId(trimmed, "--parents-brain-file"));
    }

    return entries;
}

static Guid ParseParentBrainId(string raw, string sourceLabel)
{
    if (!Guid.TryParse(raw.Trim(), out var brainId) || brainId == Guid.Empty)
    {
        throw new InvalidOperationException($"Invalid brain_id '{raw}' from {sourceLabel}.");
    }

    return brainId;
}

static ArtifactRef ParseParentSpec(string raw, string? defaultStoreUri)
{
    var tokens = raw.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    if (tokens.Length < 2 || tokens.Length > 4)
    {
        throw new InvalidOperationException(
            $"Invalid parent spec '{raw}'. Expected 'sha256,size[,store_uri][,media_type]'.");
    }

    if (!ulong.TryParse(tokens[1], out var sizeBytes))
    {
        throw new InvalidOperationException($"Invalid parent size in '{raw}'.");
    }

    var storeUri = tokens.Length >= 3 ? tokens[2] : defaultStoreUri;
    var mediaType = tokens.Length >= 4 ? tokens[3] : "application/x-nbn";
    return tokens[0].ToArtifactRef(sizeBytes, mediaType, storeUri);
}

static void ValidateParentPool((EvolutionParentMode Mode, IReadOnlyList<EvolutionParentRef> Parents) resolution)
{
    for (var i = 0; i < resolution.Parents.Count; i++)
    {
        var parent = resolution.Parents[i];
        if (resolution.Mode == EvolutionParentMode.BrainIds)
        {
            if (parent.BrainId is not Guid brainId || brainId == Guid.Empty)
            {
                throw new InvalidOperationException($"Parent brain_id at index {i} is invalid.");
            }

            continue;
        }

        if (parent.ArtifactRef is null || !parent.ArtifactRef.TryToSha256Hex(out _))
        {
            throw new InvalidOperationException($"Parent artifact at index {i} is missing a valid sha256.");
        }

        if (string.IsNullOrWhiteSpace(parent.ArtifactRef.StoreUri))
        {
            throw new InvalidOperationException(
                $"Parent reference at index {i} is missing store_uri. Provide --store-uri, set NBN_ARTIFACT_ROOT, or include store_uri in --parent entries.");
        }
    }
}

static void EmitStatus(EvolutionSimulationStatus status, bool jsonOnly, bool isFinal)
{
    var failure = string.IsNullOrWhiteSpace(status.LastFailure) ? "(none)" : status.LastFailure;
    if (jsonOnly)
    {
        var payload = new
        {
            type = "evolution_sim_status",
            final = isFinal,
            session_id = status.SessionId,
            running = status.Running,
            iterations = status.Iterations,
            parent_pool_size = status.ParentPoolSize,
            compatibility_checks = status.CompatibilityChecks,
            compatible_pairs = status.CompatiblePairs,
            reproduction_calls = status.ReproductionCalls,
            reproduction_failures = status.ReproductionFailures,
            reproduction_runs_observed = status.ReproductionRunsObserved,
            reproduction_runs_with_mutations = status.ReproductionRunsWithMutations,
            reproduction_mutation_events = status.ReproductionMutationEvents,
            similarity_samples = status.SimilaritySamples,
            min_similarity_observed = status.SimilaritySamples == 0 ? (float?)null : status.MinSimilarityObserved,
            max_similarity_observed = status.SimilaritySamples == 0 ? (float?)null : status.MaxSimilarityObserved,
            children_added_to_pool = status.ChildrenAddedToPool,
            speciation_commit_attempts = status.SpeciationCommitAttempts,
            speciation_commit_successes = status.SpeciationCommitSuccesses,
            last_failure = status.LastFailure,
            last_seed = status.LastSeed
        };
        Console.WriteLine(JsonSerializer.Serialize(payload));
        return;
    }

    Console.WriteLine(
        $"session={status.SessionId} running={status.Running} final={isFinal} iter={status.Iterations} " +
        $"pool={status.ParentPoolSize} compat={status.CompatiblePairs}/{status.CompatibilityChecks} " +
        $"repro_fail={status.ReproductionFailures} runs={status.ReproductionRunsObserved} " +
        $"runs_mutated={status.ReproductionRunsWithMutations} mutation_events={status.ReproductionMutationEvents} " +
        $"sim_min={(status.SimilaritySamples == 0 ? "n/a" : status.MinSimilarityObserved.ToString("0.###", CultureInfo.InvariantCulture))} " +
        $"sim_max={(status.SimilaritySamples == 0 ? "n/a" : status.MaxSimilarityObserved.ToString("0.###", CultureInfo.InvariantCulture))} " +
        $"children={status.ChildrenAddedToPool} " +
        $"speciation={status.SpeciationCommitSuccesses}/{status.SpeciationCommitAttempts} " +
        $"last_seed={status.LastSeed} last_failure={failure}");
}

static bool IsHelpToken(string value)
{
    var normalized = value.Trim().ToLowerInvariant();
    return normalized is "help" or "--help" or "-h";
}

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

static List<string> GetArgs(string[] args, string name)
{
    var values = new List<string>();
    for (var i = 0; i < args.Length; i++)
    {
        var arg = args[i];
        if (arg.Equals(name, StringComparison.OrdinalIgnoreCase))
        {
            if (i + 1 < args.Length)
            {
                values.Add(args[i + 1]);
                i++;
            }

            continue;
        }

        if (arg.StartsWith(name + "=", StringComparison.OrdinalIgnoreCase))
        {
            values.Add(arg.Substring(name.Length + 1));
        }
    }

    return values;
}

static bool HasFlag(string[] args, string name)
{
    foreach (var arg in args)
    {
        if (arg.Equals(name, StringComparison.OrdinalIgnoreCase))
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

static ulong? GetULongArg(string[] args, string name)
{
    var value = GetArg(args, name);
    return ulong.TryParse(value, out var parsed) ? parsed : null;
}

static double? GetDoubleArg(string[] args, string name)
{
    var value = GetArg(args, name);
    return double.TryParse(value, out var parsed) ? parsed : null;
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

static void PrintHelp()
{
    Console.WriteLine("NBN EvolutionSim usage:");
    Console.WriteLine("  run --io-address <host:port> [--io-id <name>] [--bind-host <host>] [--port <int>]");
    Console.WriteLine("      [--settings-address <host:port>] [--settings-name <name>]");
    Console.WriteLine("      [--advertise-host <host>] [--advertise-port <int>] [--seed <uint64>]");
    Console.WriteLine("      [--interval-ms <int>] [--status-seconds <int>] [--timeout-seconds <int>]");
    Console.WriteLine("      [--max-iterations <int>] [--max-parent-pool <int>]");
    Console.WriteLine("      [--min-runs <int>] [--max-runs <int>] [--run-gamma <double>]");
    Console.WriteLine("      [--strength-source base|live] [--commit-to-speciation <bool>] [--spawn-children <bool>]");
    Console.WriteLine("      [--store-uri <path|file://uri>] [--parent <sha256,size[,store_uri][,media_type]> ...]");
    Console.WriteLine("      store-uri fallback: --store-uri, then NBN_ARTIFACT_ROOT env var.");
    Console.WriteLine("      [--parents-file <path>] [--parent-brain <uuid> ...] [--parents-brain-file <path>] [--json]");
    Console.WriteLine("      parent mode is selected by input: artifact parents (default) or brain_id parents (mutually exclusive).");
    Console.WriteLine("  fallback parent flags (if --parent/--parents-file omitted):");
    Console.WriteLine("      --parent-a-sha256 <hex> --parent-a-size <bytes>");
    Console.WriteLine("      [--parent-b-sha256 <hex>] [--parent-b-size <bytes>]");
    Console.WriteLine("      [--parent-a-store-uri <uri>] [--parent-b-store-uri <uri>]");
}
