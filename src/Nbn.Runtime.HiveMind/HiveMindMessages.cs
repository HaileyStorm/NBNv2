using Nbn.Shared.Addressing;
using Proto;

namespace Nbn.Runtime.HiveMind;

public sealed record StartTickLoop;
public sealed record StopTickLoop;

public sealed record RegisterBrain(Guid BrainId, PID? SignalRouterPid);
public sealed record UpdateBrainSignalRouter(Guid BrainId, PID SignalRouterPid);
public sealed record UnregisterBrain(Guid BrainId);

public sealed record RegisterShard(Guid BrainId, int RegionId, ShardId32 ShardId, PID ShardPid);
public sealed record UnregisterShard(Guid BrainId, ShardId32 ShardId);

public sealed record PauseBrainRequest(Guid BrainId, string? Reason);
public sealed record ResumeBrainRequest(Guid BrainId);
