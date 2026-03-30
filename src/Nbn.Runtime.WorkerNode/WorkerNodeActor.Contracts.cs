using Nbn.Proto;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Proto;
using SharedShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Runtime.WorkerNode;

public sealed partial class WorkerNodeActor
{
    /// <summary>
    /// Applies a full endpoint snapshot published by discovery.
    /// </summary>
    public sealed record DiscoverySnapshotApplied(IReadOnlyDictionary<string, ServiceEndpointRegistration> Registrations);

    /// <summary>
    /// Applies an incremental discovery observation.
    /// </summary>
    public sealed record EndpointStateObserved(ServiceEndpointObservation Observation);

    /// <summary>
    /// Applies a concrete endpoint registration outside the discovery snapshot flow.
    /// </summary>
    public sealed record EndpointRegistrationObserved(ServiceEndpointRegistration Registration);

    /// <summary>
    /// Requests the current worker-node snapshot.
    /// </summary>
    public sealed record GetWorkerNodeSnapshot;

    /// <summary>
    /// Requests the hosted runtime snapshot for a single brain.
    /// </summary>
    public sealed record GetHostedBrainSnapshot(Guid BrainId);

    /// <summary>
    /// Requests backend execution information for a hosted region shard.
    /// </summary>
    public sealed record GetHostedRegionShardBackendExecutionInfo(Guid BrainId, int RegionId, int ShardIndex);

    /// <summary>
    /// Describes the worker's current endpoint and hosting state.
    /// </summary>
    public sealed record WorkerNodeSnapshot(
        Guid WorkerNodeId,
        string WorkerAddress,
        ServiceEndpointRegistration? HiveMindEndpoint,
        ServiceEndpointRegistration? IoGatewayEndpoint,
        WorkerServiceRole EnabledRoles,
        int TrackedAssignmentCount,
        WorkerResourceAvailability ResourceAvailability);

    /// <summary>
    /// Describes the runtime actors hosted for a brain on this worker.
    /// </summary>
    public sealed record HostedBrainSnapshot(
        Guid BrainId,
        PID? BrainRootPid,
        PID? SignalRouterPid,
        PID? InputCoordinatorPid,
        PID? OutputCoordinatorPid,
        PID? HiddenShardPid,
        int RegionShardCount);

    private HostedBrainSnapshot BuildHostedBrainSnapshot(Guid brainId)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return new HostedBrainSnapshot(brainId, null, null, null, null, null, 0);
        }

        return new HostedBrainSnapshot(
            brain.BrainId,
            brain.BrainRootPid,
            brain.SignalRouterPid,
            brain.InputCoordinatorPid,
            brain.OutputCoordinatorPid,
            brain.RegionShards.FirstOrDefault(static entry => entry.Key.RegionId == 1 && entry.Key.ShardIndex == 0).Value.Pid,
            brain.RegionShards.Count);
    }

    private void BeginHandleGetHostedRegionShardBackendExecutionInfo(
        IContext context,
        GetHostedRegionShardBackendExecutionInfo request)
    {
        var replyTarget = context.Sender;
        context.ReenterAfter(
            BuildHostedRegionShardBackendExecutionInfoAsync(context, request),
            completed =>
            {
                var response = completed.IsCompletedSuccessfully
                    ? completed.Result
                    : new RegionShardBackendExecutionInfo(
                        RegionShardComputeBackendPreference.Auto,
                        BackendName: "unavailable",
                        UsedGpu: false,
                        FallbackReason: $"backend_execution_query_failed:{completed.Exception?.GetBaseException().Message ?? "unknown_error"}",
                        HasExecuted: false);
                ReplyToTarget(context, replyTarget, response);
                return Task.CompletedTask;
            });
    }

    private async Task<RegionShardBackendExecutionInfo> BuildHostedRegionShardBackendExecutionInfoAsync(
        IContext context,
        GetHostedRegionShardBackendExecutionInfo request)
    {
        if (!_brains.TryGetValue(request.BrainId, out var brain)
            || !SharedShardId32.TryFrom(request.RegionId, request.ShardIndex, out var shardId)
            || !brain.RegionShards.TryGetValue(shardId, out var hostedShard))
        {
            return new RegionShardBackendExecutionInfo(
                RegionShardComputeBackendPreference.Auto,
                BackendName: "unavailable",
                UsedGpu: false,
                FallbackReason: "hosted_region_shard_not_found",
                HasExecuted: false);
        }

        try
        {
            return await context.RequestAsync<RegionShardBackendExecutionInfo>(
                    hostedShard.Pid,
                    new GetRegionShardBackendExecutionInfo(),
                    TimeSpan.FromSeconds(2))
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new RegionShardBackendExecutionInfo(
                RegionShardComputeBackendPreference.Auto,
                BackendName: "unavailable",
                UsedGpu: false,
                FallbackReason: $"backend_execution_query_failed:{ex.GetBaseException().Message}",
                HasExecuted: false);
        }
    }
}
