using Nbn.Shared;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public static class PlacementPlanner
{
    private static readonly ProtoControl.PlacementAssignmentTarget[] AssignmentTargets =
    {
        ProtoControl.PlacementAssignmentTarget.PlacementTargetBrainRoot,
        ProtoControl.PlacementAssignmentTarget.PlacementTargetSignalRouter,
        ProtoControl.PlacementAssignmentTarget.PlacementTargetInputCoordinator,
        ProtoControl.PlacementAssignmentTarget.PlacementTargetOutputCoordinator
    };

    public readonly record struct WorkerCandidate(
        Guid NodeId,
        string WorkerAddress,
        string WorkerRootActorName,
        bool IsAlive,
        bool IsReady,
        bool IsFresh);

    public sealed class PlacementPlanningResult
    {
        public PlacementPlanningResult(
            ulong placementEpoch,
            string requestId,
            long requestedMs,
            long plannedMs,
            ulong workerSnapshotMs,
            IReadOnlyList<WorkerCandidate> eligibleWorkers,
            IReadOnlyList<ProtoControl.PlacementAssignment> assignments)
        {
            PlacementEpoch = placementEpoch;
            RequestId = requestId;
            RequestedMs = requestedMs;
            PlannedMs = plannedMs;
            WorkerSnapshotMs = workerSnapshotMs;
            EligibleWorkers = eligibleWorkers.ToArray();
            Assignments = assignments.Select(CloneAssignment).ToArray();
        }

        public ulong PlacementEpoch { get; }
        public string RequestId { get; }
        public long RequestedMs { get; }
        public long PlannedMs { get; }
        public ulong WorkerSnapshotMs { get; }
        public IReadOnlyList<WorkerCandidate> EligibleWorkers { get; }
        public IReadOnlyList<ProtoControl.PlacementAssignment> Assignments { get; }

        public PlacementPlanningResult Clone()
            => new(
                PlacementEpoch,
                RequestId,
                RequestedMs,
                PlannedMs,
                WorkerSnapshotMs,
                EligibleWorkers,
                Assignments);
    }

    public static bool TryBuildPlan(
        Guid brainId,
        ulong placementEpoch,
        string requestId,
        long requestedMs,
        long plannedMs,
        ulong workerSnapshotMs,
        IEnumerable<WorkerCandidate> workers,
        out PlacementPlanningResult plan,
        out ProtoControl.PlacementFailureReason failureReason,
        out string failureMessage)
    {
        var normalizedRequestId = string.IsNullOrWhiteSpace(requestId)
            ? $"{brainId:N}:{placementEpoch}"
            : requestId.Trim();
        var eligibleWorkers = workers
            .Where(static worker => worker.IsAlive && worker.IsReady && worker.IsFresh)
            .OrderBy(static worker => worker.WorkerAddress ?? string.Empty, StringComparer.Ordinal)
            .ThenBy(static worker => worker.NodeId)
            .ToArray();

        if (eligibleWorkers.Length == 0)
        {
            plan = new PlacementPlanningResult(
                placementEpoch,
                normalizedRequestId,
                requestedMs,
                plannedMs,
                workerSnapshotMs,
                Array.Empty<WorkerCandidate>(),
                Array.Empty<ProtoControl.PlacementAssignment>());
            failureReason = ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable;
            failureMessage = "No eligible workers are available for placement.";
            return false;
        }

        var assignments = new List<ProtoControl.PlacementAssignment>(AssignmentTargets.Length);
        for (var i = 0; i < AssignmentTargets.Length; i++)
        {
            var target = AssignmentTargets[i];
            var worker = eligibleWorkers[i % eligibleWorkers.Length];
            assignments.Add(new ProtoControl.PlacementAssignment
            {
                AssignmentId = BuildAssignmentId(normalizedRequestId, placementEpoch, target),
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = placementEpoch,
                Target = target,
                WorkerNodeId = worker.NodeId.ToProtoUuid(),
                ActorName = BuildActorName(brainId, target)
            });
        }

        plan = new PlacementPlanningResult(
            placementEpoch,
            normalizedRequestId,
            requestedMs,
            plannedMs,
            workerSnapshotMs,
            eligibleWorkers,
            assignments);
        failureReason = ProtoControl.PlacementFailureReason.PlacementFailureNone;
        failureMessage = string.Empty;
        return true;
    }

    private static string BuildAssignmentId(
        string requestId,
        ulong placementEpoch,
        ProtoControl.PlacementAssignmentTarget target)
        => $"{requestId}:{placementEpoch}:{TargetToken(target)}";

    private static string BuildActorName(Guid brainId, ProtoControl.PlacementAssignmentTarget target)
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

    private static string TargetToken(ProtoControl.PlacementAssignmentTarget target)
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
