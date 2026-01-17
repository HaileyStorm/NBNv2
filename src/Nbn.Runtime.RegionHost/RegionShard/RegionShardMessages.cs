using Nbn.Shared.Addressing;
using Proto;

namespace Nbn.Runtime.RegionHost;

public sealed record RegionShardActorConfig(
    Guid BrainId,
    ShardId32 ShardId,
    PID? Router,
    PID? OutputSink,
    PID? TickSink,
    RegionShardRoutingTable Routing);

public sealed record RegionShardUpdateEndpoints(PID? Router, PID? OutputSink, PID? TickSink);

public sealed record RegionShardUpdateRouting(RegionShardRoutingTable Routing);
