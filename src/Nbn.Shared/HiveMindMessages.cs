using Nbn.Shared.Addressing;
using Proto;

namespace Nbn.Shared.HiveMind;

public sealed record StartTickLoop;
public sealed record StopTickLoop;

public sealed record RegisterBrain(Guid BrainId, PID? BrainRootPid = null, PID? SignalRouterPid = null);
public sealed record UpdateBrainSignalRouter(Guid BrainId, PID SignalRouterPid);
public sealed record UnregisterBrain(Guid BrainId);

public sealed record RegisterShard(Guid BrainId, int RegionId, int ShardIndex, PID ShardPid);
public sealed record UnregisterShard(Guid BrainId, int RegionId, int ShardIndex);

public sealed record PauseBrainRequest(Guid BrainId, string? Reason);
public sealed record ResumeBrainRequest(Guid BrainId);

public sealed record GetHiveMindStatus;

public sealed record HiveMindStatus(
    ulong LastCompletedTickId,
    bool TickLoopEnabled,
    float TargetTickHz,
    int PendingCompute,
    int PendingDeliver,
    bool RescheduleInProgress,
    int RegisteredBrains,
    int RegisteredShards);
