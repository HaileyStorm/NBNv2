using Nbn.Shared;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private void HandleDispatchPlacementPlan(IContext context, DispatchPlacementPlan message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var brain))
        {
            return;
        }

        if (brain.PlacementEpoch == 0
            || brain.PlacementExecution is null
            || brain.PlacementExecution.PlacementEpoch != message.PlacementEpoch
            || brain.PlacementExecution.Completed)
        {
            return;
        }

        if (brain.PlacementExecution.Assignments.Count == 0)
        {
            FailPlacementExecution(
                context,
                brain,
                ProtoControl.PlacementFailureReason.PlacementFailureInternalError,
                ProtoControl.PlacementReconcileState.PlacementReconcileFailed);
            return;
        }

        UpdatePlacementLifecycle(
            brain,
            ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigning,
            ProtoControl.PlacementFailureReason.PlacementFailureNone);

        foreach (var assignment in brain.PlacementExecution.Assignments.Values.OrderBy(static entry => entry.Assignment.AssignmentId, StringComparer.Ordinal))
        {
            DispatchPlacementAssignment(context, brain, assignment, 1);
        }
    }

    private void HandleRetryPlacementAssignment(IContext context, RetryPlacementAssignment message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var brain)
            || brain.PlacementExecution is null
            || brain.PlacementExecution.PlacementEpoch != message.PlacementEpoch
            || brain.PlacementExecution.Completed)
        {
            return;
        }

        if (!brain.PlacementExecution.Assignments.TryGetValue(message.AssignmentId, out var assignment)
            || assignment.Ready
            || assignment.Failed)
        {
            return;
        }

        DispatchPlacementAssignment(context, brain, assignment, message.Attempt);
    }

    private void HandlePlacementAssignmentTimeout(IContext context, PlacementAssignmentTimeout message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var brain)
            || brain.PlacementExecution is null
            || brain.PlacementExecution.PlacementEpoch != message.PlacementEpoch
            || brain.PlacementExecution.Completed)
        {
            return;
        }

        if (!brain.PlacementExecution.Assignments.TryGetValue(message.AssignmentId, out var assignment))
        {
            return;
        }

        if (!assignment.AwaitingAck || assignment.Attempt != message.Attempt || assignment.Ready || assignment.Failed)
        {
            return;
        }

        var target = ToPlacementTargetLabel(assignment.Assignment.Target);
        var assignmentWorkerId = TryGetGuid(assignment.Assignment.WorkerNodeId, out var parsedWorkerId)
            ? parsedWorkerId
            : (Guid?)null;
        assignment.AwaitingAck = false;
        if (CanRetryAssignment(assignment))
        {
            var nextAttempt = assignment.Attempt + 1;
            HiveMindTelemetry.RecordPlacementAssignmentTimeout(
                brain.BrainId,
                brain.PlacementEpoch,
                target,
                assignment.Attempt,
                willRetry: true,
                assignmentWorkerId);
            HiveMindTelemetry.RecordPlacementAssignmentRetry(
                brain.BrainId,
                brain.PlacementEpoch,
                target,
                nextAttempt,
                "timeout",
                assignmentWorkerId);
            EmitDebug(
                context,
                ProtoSeverity.SevWarn,
                "placement.assignment.retry",
                $"Placement assignment {assignment.Assignment.AssignmentId} for brain {brain.BrainId} timed out at attempt={assignment.Attempt}; scheduling retry attempt={nextAttempt}.");
            ScheduleSelf(
                context,
                TimeSpan.FromMilliseconds(Math.Max(0, _options.PlacementAssignmentRetryBackoffMs)),
                new RetryPlacementAssignment(brain.BrainId, brain.PlacementEpoch, assignment.Assignment.AssignmentId, nextAttempt));
            return;
        }

        HiveMindTelemetry.RecordPlacementAssignmentTimeout(
            brain.BrainId,
            brain.PlacementEpoch,
            target,
            assignment.Attempt,
            willRetry: false,
            assignmentWorkerId);
        EmitDebug(
            context,
            ProtoSeverity.SevWarn,
            "placement.assignment.timeout",
            $"Placement assignment {assignment.Assignment.AssignmentId} for brain {brain.BrainId} timed out at attempt={assignment.Attempt} with no retries remaining.");
        FailPlacementExecution(
            context,
            brain,
            ProtoControl.PlacementFailureReason.PlacementFailureAssignmentTimeout,
            ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
            "spawn_assignment_timeout",
            "Spawn failed: placement assignment acknowledgements timed out and retry budget was exhausted.");
    }

    private void HandlePlacementReconcileTimeout(IContext context, PlacementReconcileTimeout message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var brain)
            || brain.PlacementExecution is null
            || brain.PlacementExecution.PlacementEpoch != message.PlacementEpoch
            || !brain.PlacementExecution.ReconcileRequested
            || brain.PlacementExecution.Completed)
        {
            return;
        }

        if (brain.PlacementExecution.PendingReconcileWorkers.Count == 0)
        {
            return;
        }

        HiveMindTelemetry.RecordPlacementReconcileTimeout(
            brain.BrainId,
            brain.PlacementEpoch,
            brain.PlacementExecution.PendingReconcileWorkers.Count);
        HiveMindTelemetry.RecordPlacementReconcileFailed(
            brain.BrainId,
            brain.PlacementEpoch,
            "reconcile_timeout");
        FailPlacementExecution(
            context,
            brain,
            ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch,
            ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
            "spawn_reconcile_timeout",
            "Spawn failed: placement reconcile timed out before all workers reported.");
        EmitDebug(
            context,
            ProtoSeverity.SevWarn,
            "placement.reconcile.timeout",
            $"Placement reconcile timed out for brain {brain.BrainId} epoch={brain.PlacementEpoch} pendingWorkers={brain.PlacementExecution.PendingReconcileWorkers.Count}.");
    }

    private void HandleTrackedPlacementReconcileReport(IContext context, BrainState brain, ProtoControl.PlacementReconcileReport message)
    {
        var execution = brain.PlacementExecution;
        if (execution is null)
        {
            return;
        }

        var reconcileTarget = ResolveReconcileTargetLabel(message);
        if (!TryResolveReconcileWorkerNodeId(context.Sender, execution, message, out var workerId, out var attributionReason))
        {
            var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
            var telemetryWorkerId = workerId != Guid.Empty
                ? workerId
                : TryResolveObservedWorkerNodeId(message, out var observedWorkerId)
                    ? observedWorkerId
                    : (Guid?)null;
            HiveMindTelemetry.RecordPlacementReconcileIgnored(
                brain.BrainId,
                message.PlacementEpoch,
                attributionReason,
                telemetryWorkerId,
                reconcileTarget);
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.reconcile.ignored",
                $"Ignored reconcile report for brain {brain.BrainId}; reason={attributionReason} sender={senderLabel} epoch={message.PlacementEpoch}.");
            if (IsReconcileAttributionMismatchReason(attributionReason))
            {
                EmitDebug(
                    context,
                    ProtoSeverity.SevWarn,
                    "placement.reconcile.response_mismatch",
                    $"Reconcile attribution mismatch for brain {brain.BrainId}; reason={attributionReason} sender={senderLabel} epoch={message.PlacementEpoch}.");
            }
            return;
        }

        execution.PendingReconcileWorkers.Remove(workerId);

        switch (message.ReconcileState)
        {
            case ProtoControl.PlacementReconcileState.PlacementReconcileFailed:
                var reconcileFailureReason = message.FailureReason == ProtoControl.PlacementFailureReason.PlacementFailureNone
                    ? ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch
                    : message.FailureReason;
                HiveMindTelemetry.RecordPlacementReconcileFailed(
                    brain.BrainId,
                    brain.PlacementEpoch,
                    ToFailureReasonLabel(reconcileFailureReason),
                    workerId,
                    reconcileTarget);
                EmitDebug(
                    context,
                    ProtoSeverity.SevWarn,
                    "placement.reconcile.failed",
                    $"Placement reconcile failed for brain {brain.BrainId} epoch={brain.PlacementEpoch} reason={ToFailureReasonLabel(reconcileFailureReason)}.");
                FailPlacementExecution(
                    context,
                    brain,
                    reconcileFailureReason,
                    ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
                    ToSpawnFailureReasonCode(reconcileFailureReason),
                    BuildSpawnFailureMessage(reconcileFailureReason, message.Message));
                return;
            case ProtoControl.PlacementReconcileState.PlacementReconcileRequiresAction:
                execution.RequiresReconcileAction = true;
                break;
            case ProtoControl.PlacementReconcileState.PlacementReconcileMatched:
                break;
        }

        foreach (var observed in message.Assignments)
        {
            if (!string.IsNullOrWhiteSpace(observed.AssignmentId))
            {
                execution.ObservedAssignments[observed.AssignmentId] = observed.Clone();
            }
        }

        if (execution.PendingReconcileWorkers.Count > 0)
        {
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleReconciling,
                ProtoControl.PlacementFailureReason.PlacementFailureNone);
            return;
        }

        if (!TryValidateReconcileMatches(execution, out var mismatch))
        {
            HiveMindTelemetry.RecordPlacementReconcileFailed(
                brain.BrainId,
                brain.PlacementEpoch,
                ToFailureReasonLabel(ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch),
                workerId,
                reconcileTarget);
            FailPlacementExecution(
                context,
                brain,
                ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch,
                ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
                "spawn_reconcile_mismatch",
                $"Spawn failed: placement reconcile mismatch ({mismatch}).");
            EmitDebug(
                context,
                ProtoSeverity.SevWarn,
                "placement.reconcile.mismatch",
                $"Placement reconcile mismatch for brain {brain.BrainId}: {mismatch}");
            return;
        }

        execution.Completed = true;
        brain.PlacementReconcileState = execution.RequiresReconcileAction
            ? ProtoControl.PlacementReconcileState.PlacementReconcileRequiresAction
            : ProtoControl.PlacementReconcileState.PlacementReconcileMatched;

        if (execution.RequiresReconcileAction)
        {
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleReconciling,
                ProtoControl.PlacementFailureReason.PlacementFailureNone);
            return;
        }

        ApplyObservedControlAssignments(context, brain, execution);
        HiveMindTelemetry.RecordPlacementReconcileMatched(
            brain.BrainId,
            brain.PlacementEpoch,
            execution.ObservedAssignments.Count,
            workerId,
            reconcileTarget);
        EmitDebug(
            context,
            ProtoSeverity.SevInfo,
            "placement.reconcile.matched",
            $"Placement reconcile matched for brain {brain.BrainId} epoch={brain.PlacementEpoch} assignments={execution.ObservedAssignments.Count}.");

        UpdatePlacementLifecycle(
            brain,
            brain.Shards.Count > 0
                ? ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning
                : ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned,
            ProtoControl.PlacementFailureReason.PlacementFailureNone);
        TryCompletePendingSpawn(context, brain);
    }

    private void DispatchPlacementUnassignments(
        IContext context,
        BrainState brain,
        PlacementExecutionState? execution,
        string reason)
    {
        if (execution is null || execution.Assignments.Count == 0)
        {
            return;
        }

        foreach (var trackedAssignment in execution.Assignments.Values.OrderBy(static entry => entry.Assignment.AssignmentId, StringComparer.Ordinal))
        {
            var assignment = trackedAssignment.Assignment;
            if (!TryGetGuid(assignment.WorkerNodeId, out var workerNodeId)
                || !execution.WorkerTargets.TryGetValue(workerNodeId, out var workerPid))
            {
                continue;
            }

            try
            {
                context.Request(
                    workerPid,
                    new ProtoControl.PlacementUnassignmentRequest
                    {
                        Assignment = assignment.Clone()
                    });
                EmitDebug(
                    context,
                    ProtoSeverity.SevDebug,
                    "placement.unassignment.dispatch",
                    $"Placement unassignment {assignment.AssignmentId} for brain {brain.BrainId} dispatched reason={reason} target={ToPlacementTargetLabel(assignment.Target)}.");
            }
            catch (Exception ex)
            {
                EmitDebug(
                    context,
                    ProtoSeverity.SevWarn,
                    "placement.unassignment.dispatch_failed",
                    $"Placement unassignment {assignment.AssignmentId} for brain {brain.BrainId} dispatch failed reason={reason}: {ex.GetBaseException().Message}");
                LogError($"Failed to dispatch placement unassignment {assignment.AssignmentId} for brain {brain.BrainId}: {ex.Message}");
            }
        }
    }

    private void DispatchPlacementAssignment(
        IContext context,
        BrainState brain,
        PlacementAssignmentExecutionState assignment,
        int attempt)
    {
        if (brain.PlacementExecution is null)
        {
            return;
        }

        var target = ToPlacementTargetLabel(assignment.Assignment.Target);
        var hasWorkerNodeId = TryGetGuid(assignment.Assignment.WorkerNodeId, out var workerNodeId);
        var telemetryWorkerNodeId = hasWorkerNodeId ? workerNodeId : (Guid?)null;
        if (!hasWorkerNodeId
            || !brain.PlacementExecution.WorkerTargets.TryGetValue(workerNodeId, out var workerPid))
        {
            assignment.Failed = true;
            HiveMindTelemetry.RecordPlacementAssignmentDispatchFailed(
                brain.BrainId,
                brain.PlacementEpoch,
                target,
                Math.Max(1, attempt),
                ToFailureReasonLabel(ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable),
                telemetryWorkerNodeId);
            EmitDebug(
                context,
                ProtoSeverity.SevWarn,
                "placement.assignment.dispatch_failed",
                $"Placement assignment {assignment.Assignment.AssignmentId} for brain {brain.BrainId} could not resolve worker target={target}.");
            FailPlacementExecution(
                context,
                brain,
                ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable,
                ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
                "spawn_worker_unavailable",
                "Spawn failed: worker target was unavailable while dispatching placement assignments.");
            return;
        }

        assignment.Attempt = Math.Max(1, attempt);
        assignment.AwaitingAck = true;
        assignment.LastDispatchMs = NowMs();
        HiveMindTelemetry.RecordPlacementAssignmentDispatch(
            brain.BrainId,
            brain.PlacementEpoch,
            target,
            assignment.Attempt,
            telemetryWorkerNodeId);
        EmitDebug(
            context,
            ProtoSeverity.SevDebug,
            "placement.assignment.dispatch",
            $"Placement assignment {assignment.Assignment.AssignmentId} for brain {brain.BrainId} dispatched target={target} attempt={assignment.Attempt}.");

        try
        {
            context.Request(
                workerPid,
                new ProtoControl.PlacementAssignmentRequest
                {
                    Assignment = assignment.Assignment.Clone()
                });
        }
        catch (Exception ex)
        {
            assignment.AwaitingAck = false;
            assignment.Failed = true;
            HiveMindTelemetry.RecordPlacementAssignmentDispatchFailed(
                brain.BrainId,
                brain.PlacementEpoch,
                target,
                assignment.Attempt,
                ToFailureReasonLabel(ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable),
                telemetryWorkerNodeId);
            EmitDebug(
                context,
                ProtoSeverity.SevWarn,
                "placement.assignment.dispatch_failed",
                $"Placement assignment {assignment.Assignment.AssignmentId} for brain {brain.BrainId} dispatch failed target={target}: {ex.GetBaseException().Message}");
            FailPlacementExecution(
                context,
                brain,
                ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable,
                ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
                "spawn_worker_unavailable",
                "Spawn failed: placement assignment dispatch threw while contacting a worker.");
            LogError($"Failed to dispatch placement assignment {assignment.Assignment.AssignmentId} for brain {brain.BrainId}: {ex.Message}");
            return;
        }

        ScheduleSelf(
            context,
            TimeSpan.FromMilliseconds(Math.Max(100, _options.PlacementAssignmentTimeoutMs)),
            new PlacementAssignmentTimeout(
                brain.BrainId,
                brain.PlacementEpoch,
                assignment.Assignment.AssignmentId,
                assignment.Attempt));
    }

    private void MaybeStartReconcile(IContext context, BrainState brain)
    {
        var execution = brain.PlacementExecution;
        if (execution is null || execution.Completed || execution.ReconcileRequested)
        {
            return;
        }

        if (execution.Assignments.Values.Any(static assignment => !assignment.Ready))
        {
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigning,
                ProtoControl.PlacementFailureReason.PlacementFailureNone);
            return;
        }

        execution.ReconcileRequested = true;
        execution.PendingReconcileWorkers.Clear();
        execution.ObservedAssignments.Clear();
        execution.RequiresReconcileAction = false;
        foreach (var assignment in execution.Assignments.Values)
        {
            if (TryGetGuid(assignment.Assignment.WorkerNodeId, out var workerNodeId))
            {
                execution.PendingReconcileWorkers.Add(workerNodeId);
            }
        }

        UpdatePlacementLifecycle(
            brain,
            ProtoControl.PlacementLifecycleState.PlacementLifecycleReconciling,
            ProtoControl.PlacementFailureReason.PlacementFailureNone);
        EmitDebug(
            context,
            ProtoSeverity.SevInfo,
            "placement.reconcile.requested",
            $"Placement reconcile requested for brain {brain.BrainId} epoch={brain.PlacementEpoch} workers={execution.PendingReconcileWorkers.Count}.");

        foreach (var workerNodeId in execution.PendingReconcileWorkers.ToArray())
        {
            if (!execution.WorkerTargets.TryGetValue(workerNodeId, out var workerPid))
            {
                continue;
            }

            try
            {
                context.Request(
                    workerPid,
                    new ProtoControl.PlacementReconcileRequest
                    {
                        BrainId = brain.BrainId.ToProtoUuid(),
                        PlacementEpoch = brain.PlacementEpoch
                    });
            }
            catch (Exception ex)
            {
                HiveMindTelemetry.RecordPlacementReconcileFailed(
                    brain.BrainId,
                    brain.PlacementEpoch,
                    ToFailureReasonLabel(ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable),
                    workerNodeId);
                EmitDebug(
                    context,
                    ProtoSeverity.SevWarn,
                    "placement.reconcile.failed",
                    $"Placement reconcile dispatch failed for brain {brain.BrainId} worker={workerNodeId}: {ex.GetBaseException().Message}");
                FailPlacementExecution(
                    context,
                    brain,
                    ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable,
                    ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
                    "spawn_worker_unavailable",
                    "Spawn failed: placement reconcile dispatch could not reach a worker.");
                LogError($"Failed to dispatch reconcile request for brain {brain.BrainId}: {ex.Message}");
                return;
            }
        }

        if (execution.PendingReconcileWorkers.Count == 0)
        {
            execution.Completed = true;
            brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileMatched;
            HiveMindTelemetry.RecordPlacementReconcileMatched(
                brain.BrainId,
                brain.PlacementEpoch,
                observedAssignments: 0);
            EmitDebug(
                context,
                ProtoSeverity.SevInfo,
                "placement.reconcile.matched",
                $"Placement reconcile matched for brain {brain.BrainId} epoch={brain.PlacementEpoch} assignments=0.");
            UpdatePlacementLifecycle(
                brain,
                brain.Shards.Count > 0
                    ? ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning
                    : ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned,
                ProtoControl.PlacementFailureReason.PlacementFailureNone);
            TryCompletePendingSpawn(context, brain);
            return;
        }

        ScheduleSelf(
            context,
            TimeSpan.FromMilliseconds(Math.Max(100, _options.PlacementReconcileTimeoutMs)),
            new PlacementReconcileTimeout(brain.BrainId, brain.PlacementEpoch));
    }

    private static bool TryResolveReconcileWorkerNodeId(
        PID? sender,
        PlacementExecutionState execution,
        ProtoControl.PlacementReconcileReport message,
        out Guid workerId,
        out string reason)
    {
        if (sender is null)
        {
            workerId = Guid.Empty;
            reason = "sender_missing";
            return false;
        }

        var senderWorkerId = Guid.Empty;
        var senderWorkerMatches = 0;
        foreach (var target in execution.WorkerTargets)
        {
            if (execution.PendingReconcileWorkers.Contains(target.Key)
                && SenderMatchesPid(sender, target.Value))
            {
                senderWorkerId = target.Key;
                senderWorkerMatches++;
                if (senderWorkerMatches > 1)
                {
                    workerId = Guid.Empty;
                    reason = "sender_worker_ambiguous";
                    return false;
                }
            }
        }

        if (senderWorkerMatches == 0)
        {
            workerId = Guid.Empty;
            reason = "sender_not_pending_worker";
            return false;
        }

        foreach (var observed in message.Assignments)
        {
            if (observed.WorkerNodeId is null)
            {
                continue;
            }

            if (!TryGetGuid(observed.WorkerNodeId, out var observedWorkerId))
            {
                workerId = senderWorkerId;
                reason = "payload_worker_invalid";
                return false;
            }

            if (observedWorkerId != senderWorkerId)
            {
                workerId = senderWorkerId;
                reason = "payload_worker_mismatch";
                return false;
            }
        }

        workerId = senderWorkerId;
        reason = string.Empty;
        return true;
    }

    private static string ResolveReconcileTargetLabel(ProtoControl.PlacementReconcileReport message)
    {
        if (message.Assignments.Count == 0)
        {
            return "reconcile";
        }

        return ToPlacementTargetLabel(message.Assignments[0].Target);
    }

    private static bool TryResolveObservedWorkerNodeId(ProtoControl.PlacementReconcileReport message, out Guid workerNodeId)
    {
        foreach (var observed in message.Assignments)
        {
            if (observed.WorkerNodeId is not null && TryGetGuid(observed.WorkerNodeId, out workerNodeId))
            {
                return true;
            }
        }

        workerNodeId = Guid.Empty;
        return false;
    }

    private static bool IsReconcileAttributionMismatchReason(string reason)
    {
        if (string.IsNullOrWhiteSpace(reason))
        {
            return false;
        }

        return reason.StartsWith("sender_", StringComparison.Ordinal)
               || reason.StartsWith("payload_", StringComparison.Ordinal);
    }

    private static bool TryValidateReconcileMatches(PlacementExecutionState execution, out string mismatch)
    {
        foreach (var assignment in execution.Assignments.Values)
        {
            var assignmentId = assignment.Assignment.AssignmentId;
            if (!execution.ObservedAssignments.TryGetValue(assignmentId, out var observed))
            {
                mismatch = $"missing assignment '{assignmentId}'";
                return false;
            }

            if (observed.Target != assignment.Assignment.Target)
            {
                mismatch = $"target mismatch for '{assignmentId}'";
                return false;
            }

            if (!TryGetGuid(observed.WorkerNodeId, out var observedWorker)
                || !TryGetGuid(assignment.Assignment.WorkerNodeId, out var plannedWorker)
                || observedWorker != plannedWorker)
            {
                mismatch = $"worker mismatch for '{assignmentId}'";
                return false;
            }

            if (observed.RegionId != assignment.Assignment.RegionId || observed.ShardIndex != assignment.Assignment.ShardIndex)
            {
                mismatch = $"shard mismatch for '{assignmentId}'";
                return false;
            }
        }

        mismatch = string.Empty;
        return true;
    }

    private bool TryCreatePlacementExecution(
        IContext context,
        BrainState brain,
        PlacementPlanner.PlacementPlanningResult plan,
        out string failureMessage)
    {
        var workerTargets = new Dictionary<Guid, PID>();
        foreach (var worker in plan.EligibleWorkers)
        {
            if (string.IsNullOrWhiteSpace(worker.WorkerRootActorName))
            {
                failureMessage = $"Worker {worker.NodeId} has no root actor name for placement orchestration.";
                return false;
            }

            var workerPid = string.IsNullOrWhiteSpace(worker.WorkerAddress)
                ? new PID(string.Empty, worker.WorkerRootActorName)
                : new PID(worker.WorkerAddress, worker.WorkerRootActorName);

            workerTargets[worker.NodeId] = ResolveSendTargetPid(context, workerPid);
        }

        var execution = new PlacementExecutionState(brain.PlacementEpoch, workerTargets);
        foreach (var assignment in plan.Assignments)
        {
            if (string.IsNullOrWhiteSpace(assignment.AssignmentId))
            {
                continue;
            }

            execution.Assignments[assignment.AssignmentId] = new PlacementAssignmentExecutionState(assignment.Clone());
        }

        if (execution.Assignments.Count == 0)
        {
            failureMessage = "Placement plan produced no assignments.";
            return false;
        }

        brain.PlacementExecution = execution;
        failureMessage = string.Empty;
        return true;
    }

    private static void UpdateBrainIoWidthsFromPlannedAssignments(
        BrainState brain,
        PlacementPlanner.PlacementPlanningResult plannedPlacement)
    {
        foreach (var assignment in plannedPlacement.Assignments)
        {
            if (assignment.Target != ProtoControl.PlacementAssignmentTarget.PlacementTargetRegionShard
                || assignment.NeuronCount == 0)
            {
                continue;
            }

            var span64 = (long)assignment.NeuronStart + assignment.NeuronCount;
            if (span64 <= 0)
            {
                continue;
            }

            var span = span64 > int.MaxValue ? int.MaxValue : (int)span64;
            if (assignment.RegionId == NbnConstants.InputRegionId && span > brain.InputWidth)
            {
                brain.InputWidth = span;
            }

            if (assignment.RegionId == NbnConstants.OutputRegionId && span > brain.OutputWidth)
            {
                brain.OutputWidth = span;
            }
        }
    }

    private bool CanRetryAssignment(PlacementAssignmentExecutionState assignment)
        => assignment.Attempt <= _options.PlacementAssignmentMaxRetries;

    private void FailPlacementExecution(
        IContext context,
        BrainState brain,
        ProtoControl.PlacementFailureReason failureReason,
        ProtoControl.PlacementReconcileState reconcileState,
        string? spawnFailureReasonCode = null,
        string? spawnFailureMessage = null)
    {
        brain.PlacementReconcileState = reconcileState;
        SetSpawnFailureDetails(
            brain,
            spawnFailureReasonCode ?? ToSpawnFailureReasonCode(failureReason),
            string.IsNullOrWhiteSpace(spawnFailureMessage)
                ? BuildSpawnFailureMessage(failureReason, detail: null)
                : spawnFailureMessage);
        if (brain.PlacementExecution is not null)
        {
            brain.PlacementExecution.Completed = true;
            foreach (var assignment in brain.PlacementExecution.Assignments.Values)
            {
                assignment.AwaitingAck = false;
                if (!assignment.Ready)
                {
                    assignment.Failed = true;
                }
            }
        }

        UpdatePlacementLifecycle(
            brain,
            ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
            failureReason);
        TryCompletePendingSpawn(context, brain);
    }

    private void HandleSpawnCompletionTimeout(IContext context, SpawnCompletionTimeout message)
    {
        if (!_pendingSpawns.TryGetValue(message.BrainId, out var pending)
            || pending.PlacementEpoch != message.PlacementEpoch)
        {
            return;
        }

        _pendingSpawns.Remove(message.BrainId);
        pending.SetFailure(
            reasonCode: "spawn_timeout",
            failureMessage: "Spawn timed out while waiting for placement completion.");
        pending.Completion.TrySetResult(false);
        if (_brains.ContainsKey(message.BrainId))
        {
            UnregisterBrain(context, message.BrainId, reason: "spawn_timeout");
        }
    }

    private void TryCompletePendingSpawn(IContext context, BrainState brain)
    {
        TryCompletePendingReschedule(context, brain);

        if (!_pendingSpawns.TryGetValue(brain.BrainId, out var pending))
        {
            return;
        }

        if (pending.PlacementEpoch != brain.PlacementEpoch)
        {
            if (pending.PlacementEpoch < brain.PlacementEpoch)
            {
                _pendingSpawns.Remove(brain.BrainId);
                pending.SetFailure(
                    reasonCode: "spawn_failed",
                    failureMessage: "Spawn failed: placement epoch changed before completion.");
                pending.Completion.TrySetResult(false);
            }

            return;
        }

        var execution = brain.PlacementExecution;
        if (execution is not null
            && execution.PlacementEpoch == pending.PlacementEpoch
            && !execution.Completed)
        {
            return;
        }

        if (brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned
            || brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning)
        {
            _pendingSpawns.Remove(brain.BrainId);
            pending.Completion.TrySetResult(true);
            return;
        }

        if (brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed
            || brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleTerminated)
        {
            _pendingSpawns.Remove(brain.BrainId);
            pending.SetFailure(
                reasonCode: string.IsNullOrWhiteSpace(brain.SpawnFailureReasonCode)
                    ? ToSpawnFailureReasonCode(brain.PlacementFailureReason)
                    : brain.SpawnFailureReasonCode,
                failureMessage: string.IsNullOrWhiteSpace(brain.SpawnFailureMessage)
                    ? BuildSpawnFailureMessage(brain.PlacementFailureReason, detail: null)
                    : brain.SpawnFailureMessage);
            pending.Completion.TrySetResult(false);
            UnregisterBrain(context, brain.BrainId, reason: "spawn_failed");
        }
    }

    private int ComputeSpawnCompletionTimeoutMs()
    {
        var attempts = Math.Max(1, _options.PlacementAssignmentMaxRetries + 1);
        var assignmentWindow = (long)Math.Max(100, _options.PlacementAssignmentTimeoutMs) * attempts;
        var retryWindow = (long)Math.Max(0, _options.PlacementAssignmentRetryBackoffMs) * Math.Max(0, attempts - 1);
        var reconcileWindow = Math.Max(100, _options.PlacementReconcileTimeoutMs);
        var timeoutMs = assignmentWindow + retryWindow + reconcileWindow + 250L;
        return (int)Math.Min(int.MaxValue, Math.Max(500L, timeoutMs));
    }
}
