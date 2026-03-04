using Nbn.Proto;
using Repro = Nbn.Proto.Repro;

namespace Nbn.Tools.EvolutionSim;

public sealed record EvolutionSimulationOptions
{
    public required string IoAddress { get; init; }
    public string IoId { get; init; } = "io-gateway";
    public string BindHost { get; init; } = "127.0.0.1";
    public int Port { get; init; } = 12074;
    public string? AdvertiseHost { get; init; }
    public int? AdvertisePort { get; init; }
    public ulong Seed { get; init; } = 1;
    public TimeSpan Interval { get; init; } = TimeSpan.FromSeconds(1);
    public int MaxIterations { get; init; } // 0 = unbounded
    public int MaxParentPoolSize { get; init; } = 512;
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public bool CommitToSpeciation { get; init; } = true;
    public bool SpawnChildren { get; init; }
    public EvolutionParentMode ParentMode { get; init; } = EvolutionParentMode.ArtifactRefs;
    public Repro.StrengthSource StrengthSource { get; init; } = Repro.StrengthSource.StrengthBaseOnly;
    public InverseCompatibilityRunPolicy RunPolicy { get; init; } = InverseCompatibilityRunPolicy.Default;
}

public enum EvolutionParentMode
{
    ArtifactRefs = 0,
    BrainIds = 1
}

public readonly record struct EvolutionParentRef
{
    public Guid? BrainId { get; }
    public ArtifactRef? ArtifactRef { get; }

    public bool IsBrainId => BrainId.HasValue;
    public bool IsArtifactRef => ArtifactRef is not null;

    private EvolutionParentRef(Guid? brainId, ArtifactRef? artifactRef)
    {
        BrainId = brainId;
        ArtifactRef = artifactRef;
    }

    public static EvolutionParentRef FromBrainId(Guid brainId)
    {
        if (brainId == Guid.Empty)
        {
            throw new ArgumentOutOfRangeException(nameof(brainId), "Parent brain_id must be non-empty.");
        }

        return new EvolutionParentRef(brainId, null);
    }

    public static EvolutionParentRef FromArtifactRef(ArtifactRef artifactRef)
    {
        return artifactRef is null
            ? throw new ArgumentNullException(nameof(artifactRef))
            : new EvolutionParentRef(null, artifactRef);
    }
}

public readonly record struct InverseCompatibilityRunPolicy(uint MinRuns, uint MaxRuns, double Gamma)
{
    public static InverseCompatibilityRunPolicy Default => new(1, 6, 1d);

    public uint ResolveRunCount(float similarityScore)
    {
        var minRuns = MinRuns == 0 ? 1u : MinRuns;
        var maxRuns = MaxRuns < minRuns ? minRuns : MaxRuns;

        var normalizedSimilarity = Math.Clamp(similarityScore, 0f, 1f);
        var inverseCompatibility = 1d - normalizedSimilarity;
        var gamma = Gamma <= 0d ? 0.0001d : Gamma;
        var scaled = minRuns + Math.Round(
            Math.Pow(inverseCompatibility, gamma) * (maxRuns - minRuns),
            MidpointRounding.AwayFromZero);
        var clamped = Math.Clamp(scaled, minRuns, (double)maxRuns);
        return (uint)clamped;
    }
}

public readonly record struct EvolutionSimulationStatus(
    string SessionId,
    bool Running,
    ulong Iterations,
    int ParentPoolSize,
    ulong CompatibilityChecks,
    ulong CompatiblePairs,
    ulong ReproductionCalls,
    ulong ReproductionFailures,
    ulong ChildrenAddedToPool,
    ulong SpeciationCommitAttempts,
    ulong SpeciationCommitSuccesses,
    string LastFailure,
    ulong LastSeed);

public readonly record struct CompatibilityAssessment(
    bool Success,
    bool Compatible,
    float SimilarityScore,
    string AbortReason);

public sealed record ReproductionOutcome(
    bool Success,
    bool Compatible,
    string AbortReason,
    IReadOnlyList<ArtifactRef> ChildDefinitions,
    IReadOnlyList<SpeciationCommitCandidate> CommitCandidates);

public readonly record struct SpeciationCommitCandidate(
    Guid? ChildBrainId,
    ArtifactRef? ChildDefinition,
    float? SimilarityScore = null,
    float? FunctionScore = null,
    float? ConnectivityScore = null,
    float? RegionSpanScore = null);

public readonly record struct SpeciationCommitOutcome(
    bool Success,
    string FailureDetail,
    bool ExpectedNoOp);

public interface IEvolutionSimulationClient
{
    Task<CompatibilityAssessment> AssessCompatibilityAsync(
        EvolutionParentRef parentA,
        EvolutionParentRef parentB,
        ulong seed,
        Repro.StrengthSource strengthSource,
        CancellationToken cancellationToken);

    Task<ReproductionOutcome> ReproduceAsync(
        EvolutionParentRef parentA,
        EvolutionParentRef parentB,
        ulong seed,
        uint runCount,
        bool spawnChildren,
        Repro.StrengthSource strengthSource,
        CancellationToken cancellationToken);

    Task<SpeciationCommitOutcome> CommitSpeciationAsync(
        SpeciationCommitCandidate candidate,
        EvolutionParentRef parentA,
        EvolutionParentRef parentB,
        CancellationToken cancellationToken);
}

internal sealed class DeterministicRandom
{
    private ulong _state;

    public DeterministicRandom(ulong seed)
    {
        _state = seed == 0 ? 0x9E3779B97F4A7C15UL : seed;
    }

    public ulong NextUInt64()
    {
        _state += 0x9E3779B97F4A7C15UL;
        var value = _state;
        value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9UL;
        value = (value ^ (value >> 27)) * 0x94D049BB133111EBUL;
        return value ^ (value >> 31);
    }

    public int NextInt(int exclusiveMax)
    {
        if (exclusiveMax <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(exclusiveMax));
        }

        return (int)(NextUInt64() % (ulong)exclusiveMax);
    }
}
