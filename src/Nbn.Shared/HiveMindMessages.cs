using Nbn.Shared.Addressing;
using Proto;

namespace Nbn.Shared.HiveMind;

/// <summary>
/// Starts the global HiveMind tick loop.
/// </summary>
public sealed record StartTickLoop;

/// <summary>
/// Stops the global HiveMind tick loop.
/// </summary>
public sealed record StopTickLoop;

/// <summary>
/// Registers a brain and its root actors with the HiveMind.
/// </summary>
public sealed record RegisterBrain(Guid BrainId, PID? BrainRootPid = null, PID? SignalRouterPid = null);

/// <summary>
/// Updates the signal router PID for a registered brain.
/// </summary>
public sealed record UpdateBrainSignalRouter(Guid BrainId, PID SignalRouterPid);

/// <summary>
/// Unregisters a brain from the HiveMind routing table.
/// </summary>
public sealed record UnregisterBrain(Guid BrainId);

/// <summary>
/// Registers a region shard for a brain.
/// </summary>
public sealed record RegisterShard(
    Guid BrainId,
    int RegionId,
    int ShardIndex,
    PID ShardPid,
    ulong PlacementEpoch = 0,
    string AssignmentId = "");

/// <summary>
/// Unregisters a region shard for a brain.
/// </summary>
public sealed record UnregisterShard(Guid BrainId, int RegionId, int ShardIndex, ulong PlacementEpoch = 0, string AssignmentId = "");

/// <summary>
/// Requests that a running brain pause execution.
/// </summary>
public sealed record PauseBrainRequest(Guid BrainId, string? Reason);

/// <summary>
/// Requests that a paused brain resume execution.
/// </summary>
public sealed record ResumeBrainRequest(Guid BrainId);

/// <summary>
/// Requests that HiveMind queue a whole-brain runtime-state reset and execute it at the next safe per-brain tick barrier.
/// </summary>
public sealed record RequestBrainRuntimeReset(Guid BrainId, bool ResetBuffer, bool ResetAccumulator);

/// <summary>
/// Applies a barrier-coordinated whole-brain runtime-state reset through the IO gateway runtime surface.
/// </summary>
public sealed record ApplyBrainRuntimeResetAtBarrier(
    Guid BrainId,
    bool ResetBuffer,
    bool ResetAccumulator,
    ulong MinimumAcceptedTickId);

/// <summary>
/// Requests the current HiveMind status snapshot.
/// </summary>
public sealed record GetHiveMindStatus;

/// <summary>
/// Requests the current routing information for a brain.
/// </summary>
public sealed record GetBrainRouting(Guid BrainId);

/// <summary>
/// Summarizes the current HiveMind control-plane state.
/// </summary>
public sealed record HiveMindStatus(
    ulong LastCompletedTickId,
    bool TickLoopEnabled,
    float TargetTickHz,
    int PendingCompute,
    int PendingDeliver,
    bool RescheduleInProgress,
    int RegisteredBrains,
    int RegisteredShards);

/// <summary>
/// Describes the currently registered routing actors and shard counts for a brain.
/// </summary>
public sealed record BrainRoutingInfo(
    Guid BrainId,
    PID? BrainRootPid,
    PID? SignalRouterPid,
    int ShardCount,
    int RoutingCount);
