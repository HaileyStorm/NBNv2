using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;

namespace Nbn.Runtime.RegionHost;

/// <summary>
/// Supplies the immutable wiring and startup settings for a single <see cref="RegionShardActor"/>.
/// </summary>
public sealed record RegionShardActorConfig(
    Guid BrainId,
    ShardId32 ShardId,
    PID? Router,
    PID? OutputSink,
    PID? TickSink,
    RegionShardRoutingTable Routing,
    PID? VizHub = null,
    PID? DebugHub = null,
    bool VizEnabled = false,
    bool DebugEnabled = false,
    ProtoSeverity DebugMinSeverity = ProtoSeverity.SevDebug,
    RegionShardComputeBackendPreference ComputeBackendPreference = RegionShardComputeBackendPreference.Auto);

/// <summary>
/// Updates the router, output, and tick sink endpoints used by an active shard actor.
/// </summary>
public sealed record RegionShardUpdateEndpoints(PID? Router, PID? OutputSink, PID? TickSink);

/// <summary>
/// Replaces the routing table used for outbound shard contributions.
/// </summary>
public sealed record RegionShardUpdateRouting(RegionShardRoutingTable Routing);

/// <summary>
/// Requests the most recent backend execution metadata from a shard actor.
/// </summary>
public sealed record GetRegionShardBackendExecutionInfo;
