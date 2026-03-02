using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Shared;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;
using Repro = Nbn.Proto.Repro;
using ProtoSpec = Nbn.Proto.Speciation;

namespace Nbn.Tools.EvolutionSim;

public sealed class EvolutionRuntimeClient : IEvolutionSimulationClient, IAsyncDisposable
{
    private readonly ActorSystem _system;
    private readonly PID _ioPid;
    private readonly TimeSpan _requestTimeout;
    private bool _disposed;

    private EvolutionRuntimeClient(ActorSystem system, PID ioPid, TimeSpan requestTimeout)
    {
        _system = system;
        _ioPid = ioPid;
        _requestTimeout = requestTimeout;
    }

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
        return new EvolutionRuntimeClient(system, new PID(options.IoAddress, options.IoId), options.RequestTimeout);
    }

    public async Task<CompatibilityAssessment> AssessCompatibilityAsync(
        EvolutionParentRef parentA,
        EvolutionParentRef parentB,
        ulong seed,
        Repro.StrengthSource strengthSource,
        CancellationToken cancellationToken)
    {
        try
        {
            AssessCompatibilityResult? response;
            if (parentA.IsArtifactRef && parentB.IsArtifactRef)
            {
                var request = new Repro.AssessCompatibilityByArtifactsRequest
                {
                    ParentADef = parentA.ArtifactRef!,
                    ParentBDef = parentB.ArtifactRef!,
                    StrengthSource = strengthSource,
                    Seed = seed,
                    RunCount = 1,
                    Config = new Repro.ReproduceConfig
                    {
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
                    }
                };
                response = await _system.Root.RequestAsync<AssessCompatibilityResult>(
                        _ioPid,
                        new AssessCompatibilityByArtifacts { Request = request },
                        _requestTimeout)
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
            else if (parentA.IsBrainId && parentB.IsBrainId)
            {
                var request = new Repro.AssessCompatibilityByBrainIdsRequest
                {
                    ParentA = parentA.BrainId!.Value.ToProtoUuid(),
                    ParentB = parentB.BrainId!.Value.ToProtoUuid(),
                    StrengthSource = strengthSource,
                    Seed = seed,
                    RunCount = 1,
                    Config = new Repro.ReproduceConfig
                    {
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
                    }
                };
                response = await _system.Root.RequestAsync<AssessCompatibilityResult>(
                        _ioPid,
                        new AssessCompatibilityByBrainIds { Request = request },
                        _requestTimeout)
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                return new CompatibilityAssessment(
                    Success: false,
                    Compatible: false,
                    SimilarityScore: 0f,
                    AbortReason: "assess_parent_mode_mismatch");
            }

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
            ReproduceResult? response;
            if (parentA.IsArtifactRef && parentB.IsArtifactRef)
            {
                var request = new Repro.ReproduceByArtifactsRequest
                {
                    ParentADef = parentA.ArtifactRef!,
                    ParentBDef = parentB.ArtifactRef!,
                    StrengthSource = strengthSource,
                    Seed = seed,
                    RunCount = runCount,
                    Config = new Repro.ReproduceConfig
                    {
                        SpawnChild = spawnChildren
                            ? Repro.SpawnChildPolicy.SpawnChildDefaultOn
                            : Repro.SpawnChildPolicy.SpawnChildNever
                    }
                };
                response = await _system.Root.RequestAsync<ReproduceResult>(
                        _ioPid,
                        new ReproduceByArtifacts { Request = request },
                        _requestTimeout)
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
            else if (parentA.IsBrainId && parentB.IsBrainId)
            {
                var request = new Repro.ReproduceByBrainIdsRequest
                {
                    ParentA = parentA.BrainId!.Value.ToProtoUuid(),
                    ParentB = parentB.BrainId!.Value.ToProtoUuid(),
                    StrengthSource = strengthSource,
                    Seed = seed,
                    RunCount = runCount,
                    Config = new Repro.ReproduceConfig
                    {
                        SpawnChild = spawnChildren
                            ? Repro.SpawnChildPolicy.SpawnChildDefaultOn
                            : Repro.SpawnChildPolicy.SpawnChildNever
                    }
                };
                response = await _system.Root.RequestAsync<ReproduceResult>(
                        _ioPid,
                        new ReproduceByBrainIds { Request = request },
                        _requestTimeout)
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                return new ReproductionOutcome(
                    Success: false,
                    Compatible: false,
                    AbortReason: "repro_parent_mode_mismatch",
                    ChildDefinitions: Array.Empty<ArtifactRef>(),
                    CommitCandidates: Array.Empty<SpeciationCommitCandidate>());
            }

            var result = response?.Result;
            var report = result?.Report;
            var reproductionData = ExtractReproductionData(result);
            return new ReproductionOutcome(
                Success: result is not null,
                Compatible: report?.Compatible ?? false,
                AbortReason: NormalizeReason(report?.AbortReason),
                ChildDefinitions: reproductionData.ChildDefinitions,
                CommitCandidates: reproductionData.CommitCandidates);
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
                CommitCandidates: Array.Empty<SpeciationCommitCandidate>());
        }
    }

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
            Candidate = candidateRef
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
                    ExpectedNoOp: false);
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

    private static (IReadOnlyList<ArtifactRef> ChildDefinitions, IReadOnlyList<SpeciationCommitCandidate> CommitCandidates) ExtractReproductionData(Repro.ReproduceResult? result)
    {
        if (result is null)
        {
            return (Array.Empty<ArtifactRef>(), Array.Empty<SpeciationCommitCandidate>());
        }

        var children = new List<ArtifactRef>();
        var seenKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var commitCandidates = new List<SpeciationCommitCandidate>();
        var seenCommitCandidates = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var run in result.Runs)
        {
            AddArtifactIfValid(run.ChildDef, children, seenKeys);
            AddCommitCandidateIfValid(run.ChildBrainId, run.ChildDef, commitCandidates, seenCommitCandidates);
        }

        AddArtifactIfValid(result.ChildDef, children, seenKeys);
        AddCommitCandidateIfValid(result.ChildBrainId, result.ChildDef, commitCandidates, seenCommitCandidates);
        return (children, commitCandidates);
    }

    private static void AddArtifactIfValid(ArtifactRef? reference, ICollection<ArtifactRef> children, ISet<string> seenKeys)
    {
        if (reference is null || !reference.TryToSha256Hex(out var sha))
        {
            return;
        }

        var key = $"{sha}|{reference.SizeBytes}|{reference.MediaType}|{reference.StoreUri}";
        if (!seenKeys.Add(key))
        {
            return;
        }

        children.Add(reference);
    }

    private static void AddCommitCandidateIfValid(
        Uuid? childBrainId,
        ArtifactRef? childDefinition,
        ICollection<SpeciationCommitCandidate> candidates,
        ISet<string> seenKeys)
    {
        Guid? parsedBrainId = null;
        if (childBrainId is not null && childBrainId.TryToGuid(out var guid) && guid != Guid.Empty)
        {
            parsedBrainId = guid;
        }

        ArtifactRef? definition = null;
        if (childDefinition is not null && childDefinition.TryToSha256Hex(out _))
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

        candidates.Add(new SpeciationCommitCandidate(parsedBrainId, definition));
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

        if (candidate.ChildDefinition is not null && candidate.ChildDefinition.TryToSha256Hex(out _))
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

        if (parent.ArtifactRef is { } artifactRef
            && (artifactRef.TryToSha256Hex(out _) || !string.IsNullOrWhiteSpace(artifactRef.StoreUri)))
        {
            parentRef.ArtifactRef = artifactRef;
            return true;
        }

        return false;
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
            config = config.WithAdvertisedPort(advertisedPort.Value);
        }

        return config.WithProtoMessages(
            NbnCommonReflection.Descriptor,
            NbnIoReflection.Descriptor,
            Repro.NbnReproReflection.Descriptor,
            ProtoSpec.NbnSpeciationReflection.Descriptor);
    }

    private static bool IsLocalhost(string host)
    {
        return host.Equals("localhost", StringComparison.OrdinalIgnoreCase)
               || host.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
               || host.Equals("::1", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsAllInterfaces(string host)
    {
        return host.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase)
               || host.Equals("::", StringComparison.OrdinalIgnoreCase)
               || host.Equals("*", StringComparison.OrdinalIgnoreCase);
    }
}
