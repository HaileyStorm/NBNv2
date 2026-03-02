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
            Config = new Repro.ReproduceConfig
            {
                SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
            }
        };

        try
        {
            var response = await _system.Root.RequestAsync<AssessCompatibilityResult>(
                    _ioPid,
                    new AssessCompatibilityByArtifacts { Request = request },
                    _requestTimeout)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
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
            Config = new Repro.ReproduceConfig
            {
                SpawnChild = spawnChildren
                    ? Repro.SpawnChildPolicy.SpawnChildDefaultOn
                    : Repro.SpawnChildPolicy.SpawnChildNever
            }
        };

        try
        {
            var response = await _system.Root.RequestAsync<ReproduceResult>(
                    _ioPid,
                    new ReproduceByArtifacts { Request = request },
                    _requestTimeout)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            var result = response?.Result;
            var report = result?.Report;
            var childDefinitions = ExtractChildDefinitions(result);
            return new ReproductionOutcome(
                Success: result is not null,
                Compatible: report?.Compatible ?? false,
                AbortReason: NormalizeReason(report?.AbortReason),
                ChildDefinitions: childDefinitions);
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
                ChildDefinitions: Array.Empty<ArtifactRef>());
        }
    }

    public async Task<SpeciationCommitOutcome> CommitSpeciationAsync(
        ArtifactRef childDefinition,
        ArtifactRef parentA,
        ArtifactRef parentB,
        CancellationToken cancellationToken)
    {
        var request = new ProtoSpec.SpeciationAssignRequest
        {
            ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
            Candidate = new ProtoSpec.SpeciationCandidateRef
            {
                ArtifactRef = childDefinition
            }
        };
        request.Parents.Add(new ProtoSpec.SpeciationParentRef { ArtifactRef = parentA });
        request.Parents.Add(new ProtoSpec.SpeciationParentRef { ArtifactRef = parentB });

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
                return new SpeciationCommitOutcome(true, string.Empty);
            }

            if (decision is null)
            {
                return new SpeciationCommitOutcome(false, "speciation_empty_response");
            }

            var reason = decision.FailureReason.ToString();
            var detail = NormalizeReason(decision.FailureDetail);
            return new SpeciationCommitOutcome(
                Success: false,
                FailureDetail: string.IsNullOrWhiteSpace(detail) ? reason : $"{reason}:{detail}");
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new SpeciationCommitOutcome(
                Success: false,
                FailureDetail: $"speciation_commit_request_failed:{ex.GetBaseException().Message}");
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

    private static IReadOnlyList<ArtifactRef> ExtractChildDefinitions(Repro.ReproduceResult? result)
    {
        if (result is null)
        {
            return Array.Empty<ArtifactRef>();
        }

        var children = new List<ArtifactRef>();
        var seenKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var run in result.Runs)
        {
            AddArtifactIfValid(run.ChildDef, children, seenKeys);
        }

        AddArtifactIfValid(result.ChildDef, children, seenKeys);
        return children;
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
