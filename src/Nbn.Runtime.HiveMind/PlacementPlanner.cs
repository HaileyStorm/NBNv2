using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Quantization;
using Nbn.Shared.Sharding;
using ProtoControl = Nbn.Proto.Control;
using SharedShardPlanMode = Nbn.Shared.Sharding.ShardPlanMode;

namespace Nbn.Runtime.HiveMind;

public static class PlacementPlanner
{
    public readonly record struct WorkerCandidate(
        Guid NodeId,
        string WorkerAddress,
        string WorkerRootActorName,
        bool IsAlive,
        bool IsReady,
        bool IsFresh,
        uint CpuCores,
        long RamFreeBytes,
        long StorageFreeBytes,
        bool HasGpu,
        long VramFreeBytes,
        float CpuScore,
        float GpuScore);

    public readonly record struct RegionSpan(int RegionId, int NeuronSpan);

    public readonly record struct PlannerInputs(
        Guid BrainId,
        ulong PlacementEpoch,
        string RequestId,
        long RequestedMs,
        long PlannedMs,
        ulong WorkerSnapshotMs,
        int ShardStride,
        ProtoControl.ShardPlan? RequestedShardPlan,
        IReadOnlyList<RegionSpan> Regions);

    public sealed class PlacementPlanningResult
    {
        public PlacementPlanningResult(
            ulong placementEpoch,
            string requestId,
            long requestedMs,
            long plannedMs,
            ulong workerSnapshotMs,
            IReadOnlyList<WorkerCandidate> eligibleWorkers,
            IReadOnlyList<ProtoControl.PlacementAssignment> assignments,
            IReadOnlyList<string> plannerWarnings)
        {
            PlacementEpoch = placementEpoch;
            RequestId = requestId;
            RequestedMs = requestedMs;
            PlannedMs = plannedMs;
            WorkerSnapshotMs = workerSnapshotMs;
            EligibleWorkers = eligibleWorkers.ToArray();
            Assignments = assignments.Select(CloneAssignment).ToArray();
            PlannerWarnings = plannerWarnings.ToArray();
        }

        public ulong PlacementEpoch { get; }
        public string RequestId { get; }
        public long RequestedMs { get; }
        public long PlannedMs { get; }
        public ulong WorkerSnapshotMs { get; }
        public IReadOnlyList<WorkerCandidate> EligibleWorkers { get; }
        public IReadOnlyList<ProtoControl.PlacementAssignment> Assignments { get; }
        public IReadOnlyList<string> PlannerWarnings { get; }

        public PlacementPlanningResult Clone()
            => new(
                PlacementEpoch,
                RequestId,
                RequestedMs,
                PlannedMs,
                WorkerSnapshotMs,
                EligibleWorkers,
                Assignments,
                PlannerWarnings);
    }

    public static bool TryBuildPlan(
        PlannerInputs inputs,
        IEnumerable<WorkerCandidate> workers,
        out PlacementPlanningResult plan,
        out ProtoControl.PlacementFailureReason failureReason,
        out string failureMessage)
    {
        var normalizedRequestId = string.IsNullOrWhiteSpace(inputs.RequestId)
            ? $"{inputs.BrainId:N}:{inputs.PlacementEpoch}"
            : inputs.RequestId.Trim();
        var eligibleWorkers = workers
            .Where(static worker => worker.IsAlive && worker.IsReady && worker.IsFresh && !string.IsNullOrWhiteSpace(worker.WorkerRootActorName))
            .OrderBy(static worker => worker.WorkerAddress ?? string.Empty, StringComparer.Ordinal)
            .ThenBy(static worker => worker.NodeId)
            .ToArray();

        if (eligibleWorkers.Length == 0)
        {
            plan = new PlacementPlanningResult(
                inputs.PlacementEpoch,
                normalizedRequestId,
                inputs.RequestedMs,
                inputs.PlannedMs,
                inputs.WorkerSnapshotMs,
                Array.Empty<WorkerCandidate>(),
                Array.Empty<ProtoControl.PlacementAssignment>(),
                Array.Empty<string>());
            failureReason = ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable;
            failureMessage = "No eligible workers are available for placement.";
            return false;
        }

        if (inputs.Regions is null || inputs.Regions.Count == 0)
        {
            plan = new PlacementPlanningResult(
                inputs.PlacementEpoch,
                normalizedRequestId,
                inputs.RequestedMs,
                inputs.PlannedMs,
                inputs.WorkerSnapshotMs,
                eligibleWorkers,
                Array.Empty<ProtoControl.PlacementAssignment>(),
                Array.Empty<string>());
            failureReason = ProtoControl.PlacementFailureReason.PlacementFailureInternalError;
            failureMessage = "Placement planning failed because no region spans were available.";
            return false;
        }

        var stride = Math.Max(1, inputs.ShardStride);
        var shardPlan = BuildShardPlan(inputs.Regions, stride, inputs.RequestedShardPlan);
        var warnings = shardPlan.Warnings.ToArray();
        var assignments = BuildAssignments(
            inputs.BrainId,
            inputs.PlacementEpoch,
            normalizedRequestId,
            stride,
            eligibleWorkers,
            shardPlan);

        plan = new PlacementPlanningResult(
            inputs.PlacementEpoch,
            normalizedRequestId,
            inputs.RequestedMs,
            inputs.PlannedMs,
            inputs.WorkerSnapshotMs,
            eligibleWorkers,
            assignments,
            warnings);
        failureReason = ProtoControl.PlacementFailureReason.PlacementFailureNone;
        failureMessage = string.Empty;
        return true;
    }

    private static IReadOnlyList<ProtoControl.PlacementAssignment> BuildAssignments(
        Guid brainId,
        ulong placementEpoch,
        string requestId,
        int stride,
        IReadOnlyList<WorkerCandidate> eligibleWorkers,
        ShardPlanResult shardPlan)
    {
        var assignments = new List<ProtoControl.PlacementAssignment>();
        var controlWorker = SelectControlWorker(eligibleWorkers);

        AddControlAssignment(assignments, brainId, placementEpoch, requestId, controlWorker, ProtoControl.PlacementAssignmentTarget.PlacementTargetBrainRoot);
        AddControlAssignment(assignments, brainId, placementEpoch, requestId, controlWorker, ProtoControl.PlacementAssignmentTarget.PlacementTargetSignalRouter);
        AddControlAssignment(assignments, brainId, placementEpoch, requestId, controlWorker, ProtoControl.PlacementAssignmentTarget.PlacementTargetInputCoordinator);
        AddControlAssignment(assignments, brainId, placementEpoch, requestId, controlWorker, ProtoControl.PlacementAssignmentTarget.PlacementTargetOutputCoordinator);

        var computeCursor = 0;
        foreach (var region in shardPlan.Regions.OrderBy(static entry => entry.Key))
        {
            foreach (var shard in region.Value.OrderBy(static span => span.ShardIndex))
            {
                var worker = SelectShardWorker(shard, stride, eligibleWorkers, controlWorker, ref computeCursor);
                assignments.Add(BuildShardAssignment(brainId, placementEpoch, requestId, worker, shard));
            }
        }

        return assignments;
    }

    private static ShardPlanResult BuildShardPlan(
        IReadOnlyList<RegionSpan> regions,
        int stride,
        ProtoControl.ShardPlan? requestedShardPlan)
    {
        var regionDirectory = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        foreach (var region in regions)
        {
            if (region.RegionId < NbnConstants.RegionMinId || region.RegionId > NbnConstants.RegionMaxId || region.NeuronSpan <= 0)
            {
                continue;
            }

            regionDirectory[region.RegionId] = new NbnRegionDirectoryEntry((uint)region.NeuronSpan, 0, 0, 0);
        }

        var header = new NbnHeaderV2(
            magic: "NBN2",
            version: 2,
            endianness: 1,
            headerBytesPow2: 10,
            brainSeed: 0,
            axonStride: (uint)stride,
            flags: 0,
            quantization: QuantizationSchemas.DefaultNbn,
            regions: regionDirectory);

        var mode = requestedShardPlan is null
            ? SharedShardPlanMode.SingleShardPerRegion
            : (SharedShardPlanMode)Math.Clamp((int)requestedShardPlan.Mode, 0, 2);
        var fixedShardCount = requestedShardPlan is not null && requestedShardPlan.ShardCount > 0
            ? (int)requestedShardPlan.ShardCount
            : (int?)null;
        var maxNeuronsPerShard = requestedShardPlan is not null && requestedShardPlan.MaxNeuronsPerShard > 0
            ? (int)requestedShardPlan.MaxNeuronsPerShard
            : (int?)null;

        return ShardPlanner.BuildPlan(header, mode, fixedShardCount, maxNeuronsPerShard);
    }

    private static WorkerCandidate SelectControlWorker(IReadOnlyList<WorkerCandidate> eligibleWorkers)
        => eligibleWorkers
            .OrderByDescending(static worker => worker.CpuScore)
            .ThenByDescending(static worker => worker.CpuCores)
            .ThenByDescending(static worker => worker.GpuScore)
            .ThenBy(static worker => worker.WorkerAddress ?? string.Empty, StringComparer.Ordinal)
            .ThenBy(static worker => worker.NodeId)
            .First();

    private static WorkerCandidate SelectShardWorker(
        ShardPlanSpan shard,
        int stride,
        IReadOnlyList<WorkerCandidate> eligibleWorkers,
        WorkerCandidate controlWorker,
        ref int computeCursor)
    {
        if (shard.RegionId == NbnConstants.InputRegionId || shard.RegionId == NbnConstants.OutputRegionId)
        {
            return controlWorker;
        }

        var preferGpu = shard.NeuronCount >= Math.Max(4096, stride * 2) && eligibleWorkers.Any(static worker => worker.HasGpu);
        var ordered = preferGpu
            ? eligibleWorkers
                .OrderByDescending(static worker => worker.HasGpu)
                .ThenByDescending(static worker => worker.GpuScore)
                .ThenByDescending(static worker => worker.CpuScore)
                .ThenBy(static worker => worker.WorkerAddress ?? string.Empty, StringComparer.Ordinal)
                .ThenBy(static worker => worker.NodeId)
                .ToArray()
            : eligibleWorkers
                .OrderByDescending(static worker => worker.CpuScore)
                .ThenByDescending(static worker => worker.CpuCores)
                .ThenByDescending(static worker => worker.HasGpu)
                .ThenByDescending(static worker => worker.GpuScore)
                .ThenBy(static worker => worker.WorkerAddress ?? string.Empty, StringComparer.Ordinal)
                .ThenBy(static worker => worker.NodeId)
                .ToArray();

        var selected = ordered[computeCursor % ordered.Length];
        computeCursor++;
        return selected;
    }

    private static void AddControlAssignment(
        List<ProtoControl.PlacementAssignment> assignments,
        Guid brainId,
        ulong placementEpoch,
        string requestId,
        WorkerCandidate worker,
        ProtoControl.PlacementAssignmentTarget target)
    {
        assignments.Add(new ProtoControl.PlacementAssignment
        {
            AssignmentId = BuildControlAssignmentId(requestId, placementEpoch, target),
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = placementEpoch,
            Target = target,
            WorkerNodeId = worker.NodeId.ToProtoUuid(),
            ActorName = BuildControlActorName(brainId, target)
        });
    }

    private static ProtoControl.PlacementAssignment BuildShardAssignment(
        Guid brainId,
        ulong placementEpoch,
        string requestId,
        WorkerCandidate worker,
        ShardPlanSpan shard)
        => new()
        {
            AssignmentId = BuildShardAssignmentId(requestId, placementEpoch, shard.RegionId, shard.ShardIndex),
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = placementEpoch,
            Target = ProtoControl.PlacementAssignmentTarget.PlacementTargetRegionShard,
            WorkerNodeId = worker.NodeId.ToProtoUuid(),
            RegionId = (uint)shard.RegionId,
            ShardIndex = (uint)shard.ShardIndex,
            NeuronStart = (uint)shard.NeuronStart,
            NeuronCount = (uint)shard.NeuronCount,
            ActorName = BuildShardActorName(brainId, shard.RegionId, shard.ShardIndex)
        };

    private static string BuildControlAssignmentId(
        string requestId,
        ulong placementEpoch,
        ProtoControl.PlacementAssignmentTarget target)
        => $"{requestId}:{placementEpoch}:{ControlTargetToken(target)}";

    private static string BuildShardAssignmentId(string requestId, ulong placementEpoch, int regionId, int shardIndex)
        => $"{requestId}:{placementEpoch}:region-{regionId}-shard-{shardIndex}";

    private static string BuildControlActorName(Guid brainId, ProtoControl.PlacementAssignmentTarget target)
    {
        var brainToken = brainId.ToString("N");
        return target switch
        {
            ProtoControl.PlacementAssignmentTarget.PlacementTargetBrainRoot => $"brain-{brainToken}-root",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetSignalRouter => $"brain-{brainToken}-router",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetInputCoordinator => $"brain-{brainToken}-input",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetOutputCoordinator => $"brain-{brainToken}-output",
            _ => $"brain-{brainToken}-target-{(int)target}"
        };
    }

    private static string BuildShardActorName(Guid brainId, int regionId, int shardIndex)
        => $"brain-{brainId:N}-r{regionId}-s{shardIndex}";

    private static string ControlTargetToken(ProtoControl.PlacementAssignmentTarget target)
        => target switch
        {
            ProtoControl.PlacementAssignmentTarget.PlacementTargetBrainRoot => "brain-root",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetSignalRouter => "signal-router",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetInputCoordinator => "input-coordinator",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetOutputCoordinator => "output-coordinator",
            _ => $"target-{(int)target}"
        };

    private static ProtoControl.PlacementAssignment CloneAssignment(ProtoControl.PlacementAssignment assignment)
        => new()
        {
            AssignmentId = assignment.AssignmentId,
            BrainId = assignment.BrainId?.Clone(),
            PlacementEpoch = assignment.PlacementEpoch,
            Target = assignment.Target,
            WorkerNodeId = assignment.WorkerNodeId?.Clone(),
            RegionId = assignment.RegionId,
            ShardIndex = assignment.ShardIndex,
            NeuronStart = assignment.NeuronStart,
            NeuronCount = assignment.NeuronCount,
            ActorName = assignment.ActorName
        };
}
