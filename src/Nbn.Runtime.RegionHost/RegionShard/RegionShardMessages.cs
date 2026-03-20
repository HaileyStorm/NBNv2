using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;

namespace Nbn.Runtime.RegionHost;

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

public sealed record RegionShardUpdateEndpoints(PID? Router, PID? OutputSink, PID? TickSink);

public sealed record RegionShardUpdateRouting(RegionShardRoutingTable Routing);

public sealed record GetRegionShardBackendExecutionInfo;
