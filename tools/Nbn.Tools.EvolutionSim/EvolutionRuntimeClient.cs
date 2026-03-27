using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Shared;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;
using System.Globalization;
using System.Text.Json.Nodes;
using Repro = Nbn.Proto.Repro;
using ProtoSpec = Nbn.Proto.Speciation;

namespace Nbn.Tools.EvolutionSim;

/// <summary>
/// Owns simulator remoting and translates session requests into runtime IO/reproduction/speciation calls.
/// </summary>
public sealed class EvolutionRuntimeClient : IEvolutionSimulationClient, IAsyncDisposable
{
    private readonly ActorSystem _system;
    private readonly PID _ioPid;
    private readonly TimeSpan _requestTimeout;
    private readonly Repro.ReproduceConfig _reproduceConfigTemplate;
    // _artifactResolutionGate protects the live-brain-to-artifact cache used by mixed parent requests.
    private readonly object _artifactResolutionGate = new();
    private readonly Dictionary<Guid, ArtifactRef> _brainArtifactCache;
    private bool _disposed;
    private readonly record struct CompatibilityDispatchResult(
        bool Success,
        AssessCompatibilityResult? Response,
        string FailureReason);
    private readonly record struct ReproductionDispatchResult(
        bool Success,
        ReproduceResult? Response,
        string FailureReason);

    private EvolutionRuntimeClient(
        ActorSystem system,
        PID ioPid,
        TimeSpan requestTimeout,
        Repro.ReproduceConfig reproduceConfigTemplate)
    {
        _system = system;
        _ioPid = ioPid;
        _requestTimeout = requestTimeout;
        _reproduceConfigTemplate = reproduceConfigTemplate ?? throw new ArgumentNullException(nameof(reproduceConfigTemplate));
        _brainArtifactCache = new Dictionary<Guid, ArtifactRef>();
    }

    /// <summary>
    /// Starts the local remoting client used by EvolutionSim requests.
    /// </summary>
    public static async Task<EvolutionRuntimeClient> StartAsync(EvolutionSimulationOptions options)
    {
        if (options is null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        var system = new ActorSystem();
        var remoteConfig = BuildRemoteConfig(
            options.BindHost,
            options.Port,
            options.AdvertiseHost,
            options.AdvertisePort);
        system.WithRemote(remoteConfig);
        await system.Remote().StartAsync().ConfigureAwait(false);
        var configTemplate = await ResolveReproduceConfigTemplateAsync(system, options).ConfigureAwait(false);
        return new EvolutionRuntimeClient(
            system,
            new PID(options.IoAddress, options.IoId),
            options.RequestTimeout,
            configTemplate);
    }

    /// <summary>
    /// Assesses parent compatibility through the runtime, resolving mixed parent modes to artifact requests when needed.
    /// </summary>
    public async Task<CompatibilityAssessment> AssessCompatibilityAsync(
        EvolutionParentRef parentA,
        EvolutionParentRef parentB,
        ulong seed,
        Repro.StrengthSource strengthSource,
        CancellationToken cancellationToken)
    {
        try
        {
            var dispatch = await DispatchCompatibilityAsync(
                parentA,
                parentB,
                seed,
                strengthSource,
                cancellationToken).ConfigureAwait(false);
            if (!dispatch.Success)
            {
                return new CompatibilityAssessment(
                    Success: false,
                    Compatible: false,
                    SimilarityScore: 0f,
                    AbortReason: dispatch.FailureReason);
            }

            var response = dispatch.Response;
            var result = response?.Result;
            var report = result?.Report;
            return new CompatibilityAssessment(
                Success: result is not null,
                Compatible: report?.Compatible ?? false,
                SimilarityScore: report?.SimilarityScore ?? 0f,
                AbortReason: NormalizeReason(report?.AbortReason));
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new CompatibilityAssessment(
                Success: false,
                Compatible: false,
                SimilarityScore: 0f,
                AbortReason: $"assess_request_failed:{ex.GetBaseException().Message}");
        }
    }

    /// <summary>
    /// Executes reproduction through the runtime, keeping artifact and brain-id request routing consistent with session parent mode.
    /// </summary>
    public async Task<ReproductionOutcome> ReproduceAsync(
        EvolutionParentRef parentA,
        EvolutionParentRef parentB,
        ulong seed,
        uint runCount,
        bool spawnChildren,
        Repro.StrengthSource strengthSource,
        CancellationToken cancellationToken)
    {
        try
        {
            var dispatch = await DispatchReproductionAsync(
                parentA,
                parentB,
                seed,
                runCount,
                spawnChildren,
                strengthSource,
                cancellationToken).ConfigureAwait(false);
            if (!dispatch.Success)
            {
                return new ReproductionOutcome(
                    Success: false,
                    Compatible: false,
                    AbortReason: dispatch.FailureReason,
                    ChildDefinitions: Array.Empty<ArtifactRef>(),
                    CommitCandidates: Array.Empty<SpeciationCommitCandidate>(),
                    Diagnostics: default);
            }

            var response = dispatch.Response;
            var result = response?.Result;
            var report = result?.Report;
            var reproductionData = ExtractReproductionData(result);
            return new ReproductionOutcome(
                Success: result is not null,
                Compatible: report?.Compatible ?? false,
                AbortReason: NormalizeReason(report?.AbortReason),
                ChildDefinitions: reproductionData.ChildDefinitions,
                CommitCandidates: reproductionData.CommitCandidates,
                Diagnostics: reproductionData.Diagnostics);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new ReproductionOutcome(
                Success: false,
                Compatible: false,
                AbortReason: $"repro_request_failed:{ex.GetBaseException().Message}",
                ChildDefinitions: Array.Empty<ArtifactRef>(),
                CommitCandidates: Array.Empty<SpeciationCommitCandidate>(),
                Diagnostics: default);
        }
    }

    /// <summary>
    /// Commits a reproduction/speciation candidate through the runtime speciation service.
    /// </summary>
    public async Task<SpeciationCommitOutcome> CommitSpeciationAsync(
        SpeciationCommitCandidate candidate,
        EvolutionParentRef parentA,
        EvolutionParentRef parentB,
        CancellationToken cancellationToken)
    {
        if (!TryBuildCandidateRef(candidate, out var candidateRef))
        {
            return new SpeciationCommitOutcome(
                Success: false,
                FailureDetail: "speciation_candidate_missing",
                ExpectedNoOp: false);
        }

        var request = new ProtoSpec.SpeciationAssignRequest
        {
            ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
            Candidate = candidateRef,
            DecisionReason = "evolution_sim_commit",
            DecisionMetadataJson = BuildSpeciationDecisionMetadataJson(candidate)
        };
        if (!TryBuildParentRef(parentA, out var parentRefA)
            || !TryBuildParentRef(parentB, out var parentRefB))
        {
            return new SpeciationCommitOutcome(
                Success: false,
                FailureDetail: "speciation_parent_ref_missing",
                ExpectedNoOp: false);
        }
        request.Parents.Add(parentRefA);
        request.Parents.Add(parentRefB);

        try
        {
            var response = await _system.Root.RequestAsync<SpeciationAssignResult>(
                    _ioPid,
                    new SpeciationAssign { Request = request },
                    _requestTimeout)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            var decision = response?.Response?.Decision;
            var success = decision?.Success == true
                          && decision.FailureReason == ProtoSpec.SpeciationFailureReason.SpeciationFailureNone;

            if (success)
            {
                return new SpeciationCommitOutcome(
                    Success: true,
                    FailureDetail: string.Empty,
                    ExpectedNoOp: false,
                    SpeciesId: decision?.SpeciesId ?? string.Empty,
                    SourceSpeciesId: ExtractSourceSpeciesId(decision?.DecisionMetadataJson),
                    SourceSpeciesSimilarityScore: ExtractSourceSpeciesSimilarityScore(decision?.DecisionMetadataJson));
            }

            if (decision is null)
            {
                return new SpeciationCommitOutcome(
                    Success: false,
                    FailureDetail: "speciation_empty_response",
                    ExpectedNoOp: false);
            }

            var reason = decision.FailureReason.ToString();
            var detail = NormalizeReason(decision.FailureDetail);
            return new SpeciationCommitOutcome(
                Success: false,
                FailureDetail: string.IsNullOrWhiteSpace(detail) ? reason : $"{reason}:{detail}",
                ExpectedNoOp: false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new SpeciationCommitOutcome(
                Success: false,
                FailureDetail: $"speciation_commit_request_failed:{ex.GetBaseException().Message}",
                ExpectedNoOp: false);
        }
    }

    /// <summary>
    /// Shuts down the local remoting client and actor system.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        await _system.Remote().ShutdownAsync(graceful: true).ConfigureAwait(false);
        await _system.ShutdownAsync().ConfigureAwait(false);
    }

    private async Task<CompatibilityDispatchResult> DispatchCompatibilityAsync(
        EvolutionParentRef parentA,
        EvolutionParentRef parentB,
        ulong seed,
        Repro.StrengthSource strengthSource,
        CancellationToken cancellationToken)
    {
        if (parentA.IsArtifactRef && parentB.IsArtifactRef)
        {
            return new CompatibilityDispatchResult(
                Success: true,
                Response: await AssessCompatibilityByArtifactsAsync(
                        parentA.ArtifactRef!,
                        parentB.ArtifactRef!,
                        seed,
                        strengthSource,
                        cancellationToken)
                    .ConfigureAwait(false),
                FailureReason: string.Empty);
        }

        if (parentA.IsBrainId && parentB.IsBrainId)
        {
            return new CompatibilityDispatchResult(
                Success: true,
                Response: await AssessCompatibilityByBrainIdsAsync(
                        parentA,
                        parentB,
                        seed,
                        strengthSource,
                        cancellationToken)
                    .ConfigureAwait(false),
                FailureReason: string.Empty);
        }

        var resolution = await ResolveArtifactParentsAsync(
            parentA,
            parentB,
            cancellationToken).ConfigureAwait(false);
        if (!resolution.Success)
        {
            return new CompatibilityDispatchResult(
                Success: false,
                Response: null,
                FailureReason: "assess_parent_artifact_unavailable");
        }

        return new CompatibilityDispatchResult(
            Success: true,
            Response: await AssessCompatibilityByArtifactsAsync(
                    resolution.ParentA!,
                    resolution.ParentB!,
                    seed,
                    strengthSource,
                    cancellationToken)
                .ConfigureAwait(false),
            FailureReason: string.Empty);
    }

    private async Task<ReproductionDispatchResult> DispatchReproductionAsync(
        EvolutionParentRef parentA,
        EvolutionParentRef parentB,
        ulong seed,
        uint runCount,
        bool spawnChildren,
        Repro.StrengthSource strengthSource,
        CancellationToken cancellationToken)
    {
        if (parentA.IsArtifactRef && parentB.IsArtifactRef)
        {
            return new ReproductionDispatchResult(
                Success: true,
                Response: await ReproduceByArtifactsAsync(
                        parentA.ArtifactRef!,
                        parentB.ArtifactRef!,
                        seed,
                        runCount,
                        spawnChildren,
                        strengthSource,
                        cancellationToken)
                    .ConfigureAwait(false),
                FailureReason: string.Empty);
        }

        if (parentA.IsBrainId && parentB.IsBrainId)
        {
            return new ReproductionDispatchResult(
                Success: true,
                Response: await ReproduceByBrainIdsAsync(
                        parentA,
                        parentB,
                        seed,
                        runCount,
                        spawnChildren,
                        strengthSource,
                        cancellationToken)
                    .ConfigureAwait(false),
                FailureReason: string.Empty);
        }

        var resolution = await ResolveArtifactParentsAsync(
            parentA,
            parentB,
            cancellationToken).ConfigureAwait(false);
        if (!resolution.Success)
        {
            return new ReproductionDispatchResult(
                Success: false,
                Response: null,
                FailureReason: "repro_parent_artifact_unavailable");
        }

        return new ReproductionDispatchResult(
            Success: true,
            Response: await ReproduceByArtifactsAsync(
                    resolution.ParentA!,
                    resolution.ParentB!,
                    seed,
                    runCount,
                    spawnChildren,
                    strengthSource,
                    cancellationToken)
                .ConfigureAwait(false),
            FailureReason: string.Empty);
    }

    private async Task<AssessCompatibilityResult?> AssessCompatibilityByArtifactsAsync(
        ArtifactRef parentA,
        ArtifactRef parentB,
        ulong seed,
        Repro.StrengthSource strengthSource,
        CancellationToken cancellationToken)
    {
        var request = new Repro.AssessCompatibilityByArtifactsRequest
        {
            ParentADef = parentA,
            ParentBDef = parentB,
            StrengthSource = strengthSource,
            Seed = seed,
            RunCount = 1,
            Config = BuildRequestConfig(Repro.SpawnChildPolicy.SpawnChildNever)
        };
        return await _system.Root.RequestAsync<AssessCompatibilityResult>(
                _ioPid,
                new AssessCompatibilityByArtifacts { Request = request },
                _requestTimeout)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<AssessCompatibilityResult?> AssessCompatibilityByBrainIdsAsync(
        EvolutionParentRef parentA,
        EvolutionParentRef parentB,
        ulong seed,
        Repro.StrengthSource strengthSource,
        CancellationToken cancellationToken)
    {
        var request = new Repro.AssessCompatibilityByBrainIdsRequest
        {
            ParentA = parentA.BrainId!.Value.ToProtoUuid(),
            ParentB = parentB.BrainId!.Value.ToProtoUuid(),
            StrengthSource = strengthSource,
            Seed = seed,
            RunCount = 1,
            Config = BuildRequestConfig(Repro.SpawnChildPolicy.SpawnChildNever)
        };
        return await _system.Root.RequestAsync<AssessCompatibilityResult>(
                _ioPid,
                new AssessCompatibilityByBrainIds { Request = request },
                _requestTimeout)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<ReproduceResult?> ReproduceByArtifactsAsync(
        ArtifactRef parentA,
        ArtifactRef parentB,
        ulong seed,
        uint runCount,
        bool spawnChildren,
        Repro.StrengthSource strengthSource,
        CancellationToken cancellationToken)
    {
        var request = new Repro.ReproduceByArtifactsRequest
        {
            ParentADef = parentA,
            ParentBDef = parentB,
            StrengthSource = strengthSource,
            Seed = seed,
            RunCount = runCount,
            Config = BuildRequestConfig(
                spawnChildren
                    ? Repro.SpawnChildPolicy.SpawnChildDefaultOn
                    : Repro.SpawnChildPolicy.SpawnChildNever)
        };
        return await _system.Root.RequestAsync<ReproduceResult>(
                _ioPid,
                new ReproduceByArtifacts { Request = request },
                _requestTimeout)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<ReproduceResult?> ReproduceByBrainIdsAsync(
        EvolutionParentRef parentA,
        EvolutionParentRef parentB,
        ulong seed,
        uint runCount,
        bool spawnChildren,
        Repro.StrengthSource strengthSource,
        CancellationToken cancellationToken)
    {
        var request = new Repro.ReproduceByBrainIdsRequest
        {
            ParentA = parentA.BrainId!.Value.ToProtoUuid(),
            ParentB = parentB.BrainId!.Value.ToProtoUuid(),
            StrengthSource = strengthSource,
            Seed = seed,
            RunCount = runCount,
            Config = BuildRequestConfig(
                spawnChildren
                    ? Repro.SpawnChildPolicy.SpawnChildDefaultOn
                    : Repro.SpawnChildPolicy.SpawnChildNever)
        };
        return await _system.Root.RequestAsync<ReproduceResult>(
                _ioPid,
                new ReproduceByBrainIds { Request = request },
                _requestTimeout)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<(bool Success, ArtifactRef? ParentA, ArtifactRef? ParentB)> ResolveArtifactParentsAsync(
        EvolutionParentRef parentA,
        EvolutionParentRef parentB,
        CancellationToken cancellationToken)
    {
        var resolvedParentA = await ResolveArtifactParentAsync(parentA, cancellationToken).ConfigureAwait(false);
        var resolvedParentB = await ResolveArtifactParentAsync(parentB, cancellationToken).ConfigureAwait(false);
        return HasUsableArtifactRef(resolvedParentA) && HasUsableArtifactRef(resolvedParentB)
            ? (true, resolvedParentA, resolvedParentB)
            : (false, null, null);
    }

    private async Task<ArtifactRef?> ResolveArtifactParentAsync(
        EvolutionParentRef parent,
        CancellationToken cancellationToken)
    {
        if (HasUsableArtifactRef(parent.ArtifactRef))
        {
            return parent.ArtifactRef;
        }

        if (parent.BrainId is not Guid brainId || brainId == Guid.Empty)
        {
            return null;
        }

        lock (_artifactResolutionGate)
        {
            if (_brainArtifactCache.TryGetValue(brainId, out var cachedArtifact)
                && HasUsableArtifactRef(cachedArtifact))
            {
                return cachedArtifact;
            }
        }

        ArtifactRef? resolvedArtifact = null;
        try
        {
            var info = await _system.Root.RequestAsync<BrainInfo>(
                    _ioPid,
                    new BrainInfoRequest { BrainId = brainId.ToProtoUuid() },
                    _requestTimeout)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            if (HasUsableArtifactRef(info?.BaseDefinition))
            {
                resolvedArtifact = info!.BaseDefinition;
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            // fall through to export fallback
        }

        if (!HasUsableArtifactRef(resolvedArtifact))
        {
            try
            {
                var ready = await _system.Root.RequestAsync<BrainDefinitionReady>(
                        _ioPid,
                        new ExportBrainDefinition
                        {
                            BrainId = brainId.ToProtoUuid(),
                            RebaseOverlays = false
                        },
                        _requestTimeout)
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
                if (HasUsableArtifactRef(ready?.BrainDef))
                {
                    resolvedArtifact = ready!.BrainDef;
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch
            {
                return null;
            }
        }

        if (!HasUsableArtifactRef(resolvedArtifact))
        {
            return null;
        }

        lock (_artifactResolutionGate)
        {
            _brainArtifactCache[brainId] = resolvedArtifact!;
        }

        return resolvedArtifact;
    }

    private Repro.ReproduceConfig BuildRequestConfig(Repro.SpawnChildPolicy spawnPolicy)
    {
        var config = _reproduceConfigTemplate.Clone();
        config.SpawnChild = spawnPolicy;
        return config;
    }

    private static async Task<Repro.ReproduceConfig> ResolveReproduceConfigTemplateAsync(
        ActorSystem system,
        EvolutionSimulationOptions options)
    {
        var defaultConfig = ReproductionSettings.CreateDefaultConfig();
        if (string.IsNullOrWhiteSpace(options.SettingsAddress))
        {
            return defaultConfig;
        }

        var settingsName = string.IsNullOrWhiteSpace(options.SettingsName)
            ? "SettingsMonitor"
            : options.SettingsName.Trim();
        var settingsPid = new PID(options.SettingsAddress.Trim(), settingsName);
        var map = await TryLoadSettingsSnapshotAsync(system, settingsPid, options.RequestTimeout).ConfigureAwait(false);
        if (map is null || map.Count == 0)
        {
            return defaultConfig;
        }

        return ReproductionSettings.CreateConfigFromSettings(map);
    }

    private static async Task<IReadOnlyDictionary<string, string?>?> TryLoadSettingsSnapshotAsync(
        ActorSystem system,
        PID settingsPid,
        TimeSpan timeout)
    {
        try
        {
            var response = await system.Root.RequestAsync<SettingListResponse>(
                    settingsPid,
                    new SettingListRequest(),
                    timeout)
                .ConfigureAwait(false);
            if (response is null || response.Settings.Count == 0)
            {
                return null;
            }

            var map = new Dictionary<string, string?>(StringComparer.Ordinal);
            foreach (var setting in response.Settings)
            {
                if (string.IsNullOrWhiteSpace(setting.Key))
                {
                    continue;
                }

                map[setting.Key.Trim()] = setting.Value;
            }

            return map;
        }
        catch
        {
            return null;
        }
    }

    private static (
        IReadOnlyList<ArtifactRef> ChildDefinitions,
        IReadOnlyList<SpeciationCommitCandidate> CommitCandidates,
        ReproductionDiagnostics Diagnostics) ExtractReproductionData(Repro.ReproduceResult? result)
    {
        if (result is null)
        {
            return (Array.Empty<ArtifactRef>(), Array.Empty<SpeciationCommitCandidate>(), default);
        }

        var children = new List<ArtifactRef>();
        var seenKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var commitCandidates = new List<SpeciationCommitCandidate>();
        var seenCommitCandidates = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var run in result.Runs)
        {
            AddArtifactIfValid(run.ChildDef, children, seenKeys);
            AddCommitCandidateIfValid(run.ChildBrainId, run.ChildDef, run.Report, commitCandidates, seenCommitCandidates);
        }

        AddArtifactIfValid(result.ChildDef, children, seenKeys);
        AddCommitCandidateIfValid(result.ChildBrainId, result.ChildDef, result.Report, commitCandidates, seenCommitCandidates);
        return (children, commitCandidates, BuildReproductionDiagnostics(result));
    }

    private static ReproductionDiagnostics BuildReproductionDiagnostics(Repro.ReproduceResult result)
    {
        if (result.Runs.Count == 0)
        {
            var topLevelSimilarity = TryNormalizeScore(result.Report?.SimilarityScore);
            var topLevelMutations = CountMutationEvents(result.Summary);
            return new ReproductionDiagnostics(
                RunCount: 1,
                RunsWithMutations: topLevelMutations > 0 ? 1UL : 0UL,
                MutationEvents: topLevelMutations,
                SimilaritySamples: topLevelSimilarity.HasValue ? 1UL : 0UL,
                MinSimilarity: topLevelSimilarity ?? 0f,
                MaxSimilarity: topLevelSimilarity ?? 0f);
        }

        ulong runCount = 0;
        ulong runsWithMutations = 0;
        ulong mutationEvents = 0;
        ulong similaritySamples = 0;
        var minSimilarity = 0f;
        var maxSimilarity = 0f;

        foreach (var run in result.Runs)
        {
            runCount++;

            var runMutationEvents = CountMutationEvents(run.Summary);
            mutationEvents += runMutationEvents;
            if (runMutationEvents > 0)
            {
                runsWithMutations++;
            }

            var runSimilarity = TryNormalizeScore(run.Report?.SimilarityScore);
            if (!runSimilarity.HasValue)
            {
                continue;
            }

            if (similaritySamples == 0)
            {
                minSimilarity = runSimilarity.Value;
                maxSimilarity = runSimilarity.Value;
            }
            else
            {
                minSimilarity = Math.Min(minSimilarity, runSimilarity.Value);
                maxSimilarity = Math.Max(maxSimilarity, runSimilarity.Value);
            }

            similaritySamples++;
        }

        return new ReproductionDiagnostics(
            RunCount: runCount,
            RunsWithMutations: runsWithMutations,
            MutationEvents: mutationEvents,
            SimilaritySamples: similaritySamples,
            MinSimilarity: similaritySamples == 0 ? 0f : minSimilarity,
            MaxSimilarity: similaritySamples == 0 ? 0f : maxSimilarity);
    }

    private static ulong CountMutationEvents(Repro.MutationSummary? summary)
    {
        if (summary is null)
        {
            return 0;
        }

        return (ulong)summary.NeuronsAdded
               + summary.NeuronsRemoved
               + summary.AxonsAdded
               + summary.AxonsRemoved
               + summary.AxonsRerouted
               + summary.FunctionsMutated
               + summary.StrengthCodesChanged;
    }

    private static void AddArtifactIfValid(ArtifactRef? reference, ICollection<ArtifactRef> children, ISet<string> seenKeys)
    {
        if (!HasUsableArtifactRef(reference))
        {
            return;
        }

        var artifact = reference!;
        if (!artifact.TryToSha256Hex(out var sha))
        {
            return;
        }

        var key = $"{sha}|{artifact.SizeBytes}|{artifact.MediaType}|{artifact.StoreUri}";
        if (!seenKeys.Add(key))
        {
            return;
        }

        children.Add(artifact);
    }

    private static void AddCommitCandidateIfValid(
        Uuid? childBrainId,
        ArtifactRef? childDefinition,
        Repro.SimilarityReport? report,
        ICollection<SpeciationCommitCandidate> candidates,
        ISet<string> seenKeys)
    {
        Guid? parsedBrainId = null;
        if (childBrainId is not null && childBrainId.TryToGuid(out var guid) && guid != Guid.Empty)
        {
            parsedBrainId = guid;
        }

        ArtifactRef? definition = null;
        if (HasUsableArtifactRef(childDefinition))
        {
            definition = childDefinition;
        }

        if (parsedBrainId is null && definition is null)
        {
            return;
        }

        string key;
        if (parsedBrainId.HasValue)
        {
            key = $"brain:{parsedBrainId.Value:D}";
        }
        else if (definition is not null && definition.TryToSha256Hex(out var sha))
        {
            key = $"artifact:{sha}|{definition.SizeBytes}|{definition.MediaType}|{definition.StoreUri}";
        }
        else
        {
            key = string.Empty;
        }
        if (string.IsNullOrWhiteSpace(key) || !seenKeys.Add(key))
        {
            return;
        }

        candidates.Add(new SpeciationCommitCandidate(
            ChildBrainId: parsedBrainId,
            ChildDefinition: definition,
            SimilarityScore: TryNormalizeScore(report?.LineageSimilarityScore)
                             ?? TryNormalizeScore(report?.SimilarityScore),
            LineageSimilarityScore: TryNormalizeScore(report?.LineageSimilarityScore),
            LineageParentASimilarityScore: TryNormalizeScore(report?.LineageParentASimilarityScore),
            LineageParentBSimilarityScore: TryNormalizeScore(report?.LineageParentBSimilarityScore),
            FunctionScore: TryNormalizeScore(report?.FunctionScore),
            ConnectivityScore: TryNormalizeScore(report?.ConnectivityScore),
            RegionSpanScore: TryNormalizeScore(report?.RegionSpanScore)));
    }

    private static string BuildSpeciationDecisionMetadataJson(SpeciationCommitCandidate candidate)
    {
        var metadata = new JsonObject
        {
            ["source"] = "evolution_sim"
        };

        var report = new JsonObject();
        AddScore(report, "similarity_score", candidate.SimilarityScore);
        AddScore(report, "function_score", candidate.FunctionScore);
        AddScore(report, "connectivity_score", candidate.ConnectivityScore);
        AddScore(report, "region_span_score", candidate.RegionSpanScore);
        if (report.Count > 0)
        {
            metadata["report"] = report;
        }

        var lineage = new JsonObject();
        AddScore(lineage, "lineage_similarity_score", candidate.LineageSimilarityScore ?? candidate.SimilarityScore);
        AddScore(lineage, "parent_a_similarity_score", candidate.LineageParentASimilarityScore);
        AddScore(lineage, "parent_b_similarity_score", candidate.LineageParentBSimilarityScore);
        if (candidate.ChildBrainId is Guid childBrainId
            && childBrainId != Guid.Empty
            && HasUsableArtifactRef(candidate.ChildDefinition))
        {
            lineage["candidate_brain_base_artifact_ref"] = BuildStoredArtifactRefNode(candidate.ChildDefinition!);
        }
        else if (HasUsableArtifactRef(candidate.ChildDefinition))
        {
            lineage["candidate_artifact_ref"] = BuildStoredArtifactRefNode(candidate.ChildDefinition!);
        }

        if (lineage.Count > 0)
        {
            metadata["lineage"] = lineage;
        }

        return metadata.ToJsonString();
    }

    private static JsonObject BuildStoredArtifactRefNode(ArtifactRef artifactRef)
    {
        var node = new JsonObject
        {
            ["size_bytes"] = artifactRef.SizeBytes
        };

        if (artifactRef.TryToSha256Hex(out var sha256Hex))
        {
            node["sha256_hex"] = sha256Hex;
        }

        if (!string.IsNullOrWhiteSpace(artifactRef.MediaType))
        {
            node["media_type"] = artifactRef.MediaType;
        }

        if (!string.IsNullOrWhiteSpace(artifactRef.StoreUri))
        {
            node["store_uri"] = artifactRef.StoreUri;
        }

        return node;
    }

    private static string ExtractSourceSpeciesId(string? decisionMetadataJson)
    {
        if (string.IsNullOrWhiteSpace(decisionMetadataJson))
        {
            return string.Empty;
        }

        try
        {
            var root = JsonNode.Parse(decisionMetadataJson);
            var lineage = root?["lineage"];
            return NormalizeReason(
                lineage?["source_species_id"]?.GetValue<string?>()
                ?? lineage?["sourceSpeciesId"]?.GetValue<string?>()
                ?? lineage?["dominant_species_id"]?.GetValue<string?>()
                ?? lineage?["dominantSpeciesId"]?.GetValue<string?>());
        }
        catch
        {
            return string.Empty;
        }
    }

    private static float? ExtractSourceSpeciesSimilarityScore(string? decisionMetadataJson)
    {
        if (string.IsNullOrWhiteSpace(decisionMetadataJson))
        {
            return null;
        }

        try
        {
            var root = JsonNode.Parse(decisionMetadataJson);
            var lineage = root?["lineage"];
            return FindScore(
                       lineage,
                       "source_species_similarity_score",
                       "sourceSpeciesSimilarityScore")
                   ?? FindScore(
                       lineage,
                       "dominant_species_similarity_score",
                       "dominantSpeciesSimilarityScore")
                   ?? FindScore(
                       lineage,
                       "lineage_similarity_score",
                       "lineageSimilarityScore")
                   ?? FindScore(
                       lineage,
                       "similarity_score",
                       "similarityScore")
                   ?? FindScore(
                       root?["report"],
                       "similarity_score",
                       "similarityScore")
                   ?? FindScore(
                       root,
                       "source_species_similarity_score",
                       "sourceSpeciesSimilarityScore")
                   ?? FindScore(
                       root,
                       "dominant_species_similarity_score",
                       "dominantSpeciesSimilarityScore")
                   ?? FindScore(
                       root,
                       "lineage_similarity_score",
                       "lineageSimilarityScore")
                   ?? FindScore(
                       root,
                       "similarity_score",
                       "similarityScore");
        }
        catch
        {
            return null;
        }
    }

    private static float? TryNormalizeScore(float? value)
    {
        if (!value.HasValue || float.IsNaN(value.Value) || float.IsInfinity(value.Value))
        {
            return null;
        }

        return Math.Clamp(value.Value, 0f, 1f);
    }

    private static void AddScore(JsonObject target, string key, float? value)
    {
        if (value.HasValue && !float.IsNaN(value.Value) && !float.IsInfinity(value.Value))
        {
            target[key] = Math.Clamp(value.Value, 0f, 1f);
        }
    }

    private static float? FindScore(JsonNode? node, params string[] aliases)
    {
        if (node is not JsonObject obj)
        {
            return null;
        }

        foreach (var alias in aliases)
        {
            if (!obj.TryGetPropertyValue(alias, out var valueNode))
            {
                continue;
            }

            var numericValue = TryReadNumericValue(valueNode);
            if (numericValue.HasValue)
            {
                return TryNormalizeScore(numericValue.Value);
            }
        }

        return null;
    }

    private static float? TryReadNumericValue(JsonNode? node)
    {
        if (node is not JsonValue valueNode)
        {
            return null;
        }

        if (valueNode.TryGetValue<float>(out var asFloat))
        {
            return asFloat;
        }

        if (valueNode.TryGetValue<double>(out var asDouble))
        {
            return (float)asDouble;
        }

        if (valueNode.TryGetValue<decimal>(out var asDecimal))
        {
            return (float)asDecimal;
        }

        if (valueNode.TryGetValue<int>(out var asInt))
        {
            return asInt;
        }

        if (valueNode.TryGetValue<long>(out var asLong))
        {
            return asLong;
        }

        if (valueNode.TryGetValue<string>(out var asString)
            && float.TryParse(asString, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        return null;
    }

    private static bool TryBuildCandidateRef(
        SpeciationCommitCandidate candidate,
        out ProtoSpec.SpeciationCandidateRef candidateRef)
    {
        candidateRef = new ProtoSpec.SpeciationCandidateRef();
        if (candidate.ChildBrainId is Guid childBrainId && childBrainId != Guid.Empty)
        {
            candidateRef.BrainId = childBrainId.ToProtoUuid();
            return true;
        }

        if (HasUsableArtifactRef(candidate.ChildDefinition))
        {
            candidateRef.ArtifactRef = candidate.ChildDefinition;
            return true;
        }

        return false;
    }

    private static bool TryBuildParentRef(
        EvolutionParentRef parent,
        out ProtoSpec.SpeciationParentRef parentRef)
    {
        parentRef = new ProtoSpec.SpeciationParentRef();
        if (parent.BrainId is Guid brainId && brainId != Guid.Empty)
        {
            parentRef.BrainId = brainId.ToProtoUuid();
            return true;
        }

        if (HasUsableArtifactRef(parent.ArtifactRef))
        {
            parentRef.ArtifactRef = parent.ArtifactRef;
            return true;
        }

        return false;
    }

    private static bool HasUsableArtifactRef(ArtifactRef? artifactRef)
    {
        return artifactRef is not null
               && artifactRef.TryToSha256Hex(out _)
               && !string.IsNullOrWhiteSpace(artifactRef.StoreUri);
    }

    private static string NormalizeReason(string? reason)
    {
        return string.IsNullOrWhiteSpace(reason) ? string.Empty : reason.Trim();
    }

    private static RemoteConfig BuildRemoteConfig(
        string bindHost,
        int port,
        string? advertisedHost,
        int? advertisedPort)
    {
        RemoteConfig config;
        if (IsAllInterfaces(bindHost))
        {
            var advertiseHost = NetworkAddressDefaults.ResolveAdvertisedHost(bindHost, advertisedHost);
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
            config = config.WithAdvertisedPort(advertisedPort.Value);
        }

        return config.WithProtoMessages(
            NbnCommonReflection.Descriptor,
            NbnIoReflection.Descriptor,
            NbnSettingsReflection.Descriptor,
            Repro.NbnReproReflection.Descriptor,
            ProtoSpec.NbnSpeciationReflection.Descriptor);
    }

    private static bool IsLocalhost(string host)
        => NetworkAddressDefaults.IsLoopbackHost(host);

    private static bool IsAllInterfaces(string host)
        => NetworkAddressDefaults.IsAllInterfaces(host);
}
