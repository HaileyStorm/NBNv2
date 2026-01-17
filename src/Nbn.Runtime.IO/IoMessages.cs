using Nbn.Proto;

namespace Nbn.Runtime.IO;

public sealed record RegisterBrain(
    Guid BrainId,
    uint InputWidth,
    uint OutputWidth,
    ArtifactRef? BaseDefinition = null,
    ArtifactRef? LastSnapshot = null,
    BrainEnergyState? EnergyState = null);

public sealed record UnregisterBrain(Guid BrainId, string? Reason = null);

public sealed record ApplyTickCost(Guid BrainId, ulong TickId, long TickCost);

public sealed record UpdateBrainSnapshot(Guid BrainId, ArtifactRef Snapshot);
