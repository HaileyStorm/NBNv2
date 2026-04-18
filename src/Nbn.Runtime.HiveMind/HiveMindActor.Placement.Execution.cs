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

        foreach (var workerAssignments in brain.PlacementExecution.Assignments.Values
                     .OrderBy(static entry => entry.Assignment.AssignmentId, StringComparer.Ordinal)
                     .GroupBy(entry => TryGetGuid(entry.Assignment.WorkerNodeId, out var workerNodeId) ? workerNodeId : Guid.Empty))
        {
            if (workerAssignments.Key == Guid.Empty)
            {
                foreach (var assignment in workerAssignments)
                {
                    DispatchPlacementAssignment(context, brain, assignment, 1, Guid.Empty);
                }

                continue;
            }

            EnqueuePlacementAssignments(
                context,
                brain,
                workerAssignments.Key,
                workerAssignments.Select(static assignment => new QueuedPlacementDispatchAttempt(assignment.Assignment.AssignmentId, 1)));
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

        if (!TryGetGuid(assignment.Assignment.WorkerNodeId, out var workerNodeId))
        {
            DispatchPlacementAssignment(context, brain, assignment, message.Attempt, Guid.Empty);
            return;
        }

        if (TryRetryActiveWorkerPlacementAssignment(context, brain, workerNodeId, assignment, message.Attempt))
        {
            return;
        }

        EnqueuePlacementAssignments(
            context,
            brain,
            workerNodeId,
            new[] { new QueuedPlacementDispatchAttempt(assignment.Assignment.AssignmentId, message.Attempt) });
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

        var awaitingDispatchAck = assignment.AwaitingAck && assignment.Attempt == message.Attempt;
        var acceptedButNotReady = !assignment.AwaitingAck
                                  && assignment.Accepted
                                  && !assignment.Ready
                                  && !assignment.Failed
                                  && assignment.Attempt == message.Attempt;
        if ((!awaitingDispatchAck && !acceptedButNotReady) || assignment.Ready || assignment.Failed)
        {
            return;
        }

        var target = ToPlacementTargetLabel(assignment.Assignment.Target);
        var assignmentWorkerId = TryGetGuid(assignment.Assignment.WorkerNodeId, out var parsedWorkerId)
            ? parsedWorkerId
            : (Guid?)null;
        assignment.AwaitingAck = false;
        if (acceptedButNotReady)
        {
            assignment.Failed = true;
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
                "placement.assignment.accepted_timeout",
                $"Placement assignment {assignment.Assignment.AssignmentId} for brain {brain.BrainId} timed out after accepted state at attempt={assignment.Attempt} before reaching ready.");
            FailPlacementExecution(
                context,
                brain,
                ProtoControl.PlacementFailureReason.PlacementFailureAssignmentTimeout,
                ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
                "spawn_assignment_timeout",
                "Spawn failed: an accepted placement assignment did not become ready before the timeout window elapsed.");
            return;
        }

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

        if (assignmentWorkerId.HasValue)
        {
            ReleaseWorkerPlacementDispatch(context, assignmentWorkerId.Value, brain.BrainId, brain.PlacementEpoch, assignment.Assignment.AssignmentId);
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
                var mismatchMessage =
                    $"Reconcile attribution mismatch for brain {brain.BrainId}; reason={attributionReason} sender={senderLabel} epoch={message.PlacementEpoch}.";
                EmitDebug(
                    context,
                    ProtoSeverity.SevWarn,
                    "placement.reconcile.response_mismatch",
                    mismatchMessage);
                LogError(mismatchMessage);
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

    private void EnqueuePlacementAssignments(
        IContext context,
        BrainState brain,
        Guid workerNodeId,
        IEnumerable<QueuedPlacementDispatchAttempt> assignments)
    {
        if (brain.PlacementExecution is null)
        {
            return;
        }

        var dispatchAttempts = assignments
            .Where(static attempt => !string.IsNullOrWhiteSpace(attempt.AssignmentId))
            .ToArray();
        if (dispatchAttempts.Length == 0)
        {
            return;
        }

        var queue = GetOrCreateWorkerPlacementDispatch(workerNodeId);
        queue.Pending.Enqueue(new QueuedPlacementDispatchBatch(
            brain.BrainId,
            brain.PlacementEpoch,
            workerNodeId,
            dispatchAttempts));
        TryDrainWorkerPlacementDispatch(context, workerNodeId);
    }

    private void DispatchPlacementAssignment(
        IContext context,
        BrainState brain,
        PlacementAssignmentExecutionState assignment,
        int attempt,
        Guid workerNodeId,
        int serialWindowMultiplier = 1)
    {
        if (brain.PlacementExecution is null)
        {
            return;
        }

        var target = ToPlacementTargetLabel(assignment.Assignment.Target);
        var hasWorkerNodeId = workerNodeId != Guid.Empty || TryGetGuid(assignment.Assignment.WorkerNodeId, out workerNodeId);
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
            var replyPid = PidLabel(ResolveSendTargetPid(context, context.Self));
            context.Request(
                workerPid,
                new ProtoControl.PlacementAssignmentRequest
                {
                    Assignment = assignment.Assignment.Clone(),
                    ReplyPid = replyPid
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

        var assignmentTimeoutMs = (long)ComputePlacementAssignmentAttemptTimeoutMs() * Math.Max(1, serialWindowMultiplier);
        ScheduleSelf(
            context,
            TimeSpan.FromMilliseconds(Math.Min(int.MaxValue, assignmentTimeoutMs)),
            new PlacementAssignmentTimeout(
                brain.BrainId,
                brain.PlacementEpoch,
                assignment.Assignment.AssignmentId,
                assignment.Attempt));
    }

    private WorkerPlacementDispatchState GetOrCreateWorkerPlacementDispatch(Guid workerNodeId)
    {
        if (!_workerPlacementDispatches.TryGetValue(workerNodeId, out var queue))
        {
            queue = new WorkerPlacementDispatchState();
            _workerPlacementDispatches[workerNodeId] = queue;
        }

        return queue;
    }

    private void TryDrainWorkerPlacementDispatch(IContext context, Guid workerNodeId)
    {
        if (!_workerPlacementDispatches.TryGetValue(workerNodeId, out var queue))
        {
            return;
        }

        if (queue.Active is { } activeDispatch)
        {
            if (TryDispatchNextActiveWorkerPlacementAssignment(context, workerNodeId, activeDispatch))
            {
                return;
            }

            if (activeDispatch.InFlightAssignmentIds.Count > 0)
            {
                return;
            }

            queue.Active = null;
        }

        while (queue.Pending.Count > 0)
        {
            var pending = queue.Pending.Dequeue();
            if (!_brains.TryGetValue(pending.BrainId, out var brain)
                || brain.PlacementExecution is null
                || brain.PlacementExecution.PlacementEpoch != pending.PlacementEpoch
                || brain.PlacementExecution.Completed)
            {
                continue;
            }

            var dispatchBatch = new List<(QueuedPlacementDispatchAttempt Attempt, PlacementAssignmentExecutionState Assignment)>(pending.Assignments.Count);
            foreach (var attempt in pending.Assignments)
            {
                if (!brain.PlacementExecution.Assignments.TryGetValue(attempt.AssignmentId, out var assignment)
                    || assignment.Ready
                    || assignment.Failed
                    || assignment.AwaitingAck)
                {
                    continue;
                }

                dispatchBatch.Add((attempt, assignment));
            }

            if (dispatchBatch.Count == 0)
            {
                continue;
            }

            var newActiveDispatch = new ActiveWorkerPlacementDispatch(
                pending.BrainId,
                pending.PlacementEpoch,
                workerNodeId,
                dispatchBatch.Select(static item => item.Attempt));
            queue.Active = newActiveDispatch;

            TryDispatchNextActiveWorkerPlacementAssignment(context, workerNodeId, newActiveDispatch);
            return;
        }

        if (queue.Pending.Count == 0 && queue.Active is null)
        {
            _workerPlacementDispatches.Remove(workerNodeId);
        }
    }

    private void ReleaseWorkerPlacementDispatch(
        IContext context,
        Guid workerNodeId,
        Guid brainId,
        ulong placementEpoch,
        string assignmentId)
    {
        if (!_workerPlacementDispatches.TryGetValue(workerNodeId, out var queue))
        {
            return;
        }

        if (queue.Active is { } active
            && active.BrainId == brainId
            && active.PlacementEpoch == placementEpoch)
        {
            active.InFlightAssignmentIds.Remove(assignmentId);
            if (active.RemainingAssignmentCount == 0)
            {
                queue.Active = null;
            }
        }

        TryDrainWorkerPlacementDispatch(context, workerNodeId);
    }

    private bool TryDispatchNextActiveWorkerPlacementAssignment(
        IContext context,
        Guid workerNodeId,
        ActiveWorkerPlacementDispatch activeDispatch)
    {
        if (activeDispatch.InFlightAssignmentIds.Count > 0)
        {
            return true;
        }

        while (activeDispatch.PendingAssignments.Count > 0)
        {
            var attempt = activeDispatch.PendingAssignments.Dequeue();
            if (!_brains.TryGetValue(activeDispatch.BrainId, out var brain)
                || brain.PlacementExecution is null
                || brain.PlacementExecution.PlacementEpoch != activeDispatch.PlacementEpoch
                || brain.PlacementExecution.Completed)
            {
                return false;
            }

            if (!brain.PlacementExecution.Assignments.TryGetValue(attempt.AssignmentId, out var assignment)
                || assignment.Ready
                || assignment.Failed
                || assignment.AwaitingAck)
            {
                continue;
            }

            activeDispatch.InFlightAssignmentIds.Add(attempt.AssignmentId);
            DispatchPlacementAssignment(
                context,
                brain,
                assignment,
                attempt.Attempt,
                workerNodeId,
                serialWindowMultiplier: 1);
            if (!_workerPlacementDispatches.TryGetValue(workerNodeId, out var currentQueue)
                || !ReferenceEquals(currentQueue.Active, activeDispatch))
            {
                return false;
            }

            return true;
        }

        return false;
    }

    private bool TryRetryActiveWorkerPlacementAssignment(
        IContext context,
        BrainState brain,
        Guid workerNodeId,
        PlacementAssignmentExecutionState assignment,
        int attempt)
    {
        if (!_workerPlacementDispatches.TryGetValue(workerNodeId, out var queue)
            || queue.Active is not { } active
            || active.BrainId != brain.BrainId
            || active.PlacementEpoch != brain.PlacementEpoch
            || !active.InFlightAssignmentIds.Contains(assignment.Assignment.AssignmentId))
        {
            return false;
        }

        DispatchPlacementAssignment(
            context,
            brain,
            assignment,
            attempt,
            workerNodeId,
            serialWindowMultiplier: 1);
        return true;
    }

    private void RemoveQueuedPlacementDispatches(IContext context, Guid brainId, ulong placementEpoch)
    {
        foreach (var (workerNodeId, queue) in _workerPlacementDispatches.ToArray())
        {
            if (queue.Pending.Count > 0)
            {
                var retained = new Queue<QueuedPlacementDispatchBatch>(queue.Pending.Count);
                while (queue.Pending.Count > 0)
                {
                    var pending = queue.Pending.Dequeue();
                    if (pending.BrainId == brainId && pending.PlacementEpoch == placementEpoch)
                    {
                        continue;
                    }

                    retained.Enqueue(pending);
                }

                while (retained.Count > 0)
                {
                    queue.Pending.Enqueue(retained.Dequeue());
                }
            }

            if (queue.Active is { } active
                && active.BrainId == brainId
                && active.PlacementEpoch == placementEpoch)
            {
                queue.Active = null;
            }

            if (queue.Pending.Count == 0 && queue.Active is null)
            {
                _workerPlacementDispatches.Remove(workerNodeId);
            }
            else if (queue.Active is null)
            {
                TryDrainWorkerPlacementDispatch(context, workerNodeId);
            }
        }
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
            return TryResolveReconcileWorkerNodeIdFromPayload(execution, message, out workerId, out reason);
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

    private static bool TryResolveReconcileWorkerNodeIdFromPayload(
        PlacementExecutionState execution,
        ProtoControl.PlacementReconcileReport message,
        out Guid workerId,
        out string reason)
    {
        var payloadWorkerId = Guid.Empty;
        var payloadWorkerCount = 0;
        foreach (var observed in message.Assignments)
        {
            if (observed.WorkerNodeId is null)
            {
                continue;
            }

            if (!TryGetGuid(observed.WorkerNodeId, out var observedWorkerId))
            {
                workerId = Guid.Empty;
                reason = "payload_worker_invalid";
                return false;
            }

            if (payloadWorkerCount == 0)
            {
                payloadWorkerId = observedWorkerId;
                payloadWorkerCount = 1;
                continue;
            }

            if (observedWorkerId != payloadWorkerId)
            {
                workerId = Guid.Empty;
                reason = "payload_worker_ambiguous";
                return false;
            }
        }

        if (payloadWorkerCount == 1 && execution.PendingReconcileWorkers.Contains(payloadWorkerId))
        {
            workerId = payloadWorkerId;
            reason = "sender_missing_payload_worker";
            return true;
        }

        if (payloadWorkerCount == 0 && execution.PendingReconcileWorkers.Count == 1)
        {
            workerId = execution.PendingReconcileWorkers.First();
            reason = "sender_missing_single_pending_worker";
            return true;
        }

        workerId = Guid.Empty;
        reason = payloadWorkerCount > 1 ? "payload_worker_ambiguous" : "sender_missing";
        return false;
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

        RemoveQueuedPlacementDispatches(context, brain.BrainId, brain.PlacementEpoch);

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
        RemoveQueuedPlacementDispatches(context, message.BrainId, message.PlacementEpoch);
        RememberCompletedSpawn(new CompletedSpawnState(
            message.BrainId,
            pending.PlacementEpoch,
            AcceptedForPlacement: true,
            PlacementReady: false,
            ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
            ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
            pending.FailureReasonCode,
            pending.FailureMessage));
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
            RememberCompletedSpawn(new CompletedSpawnState(
                brain.BrainId,
                pending.PlacementEpoch,
                AcceptedForPlacement: true,
                PlacementReady: true,
                brain.PlacementLifecycleState,
                brain.PlacementReconcileState,
                FailureReasonCode: string.Empty,
                FailureMessage: string.Empty));
            pending.Completion.TrySetResult(true);
            return;
        }

        if (brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed
            || brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleTerminated)
        {
            _pendingSpawns.Remove(brain.BrainId);
            var reasonCode = string.IsNullOrWhiteSpace(brain.SpawnFailureReasonCode)
                ? ToSpawnFailureReasonCode(brain.PlacementFailureReason)
                : brain.SpawnFailureReasonCode;
            var failureMessage = string.IsNullOrWhiteSpace(brain.SpawnFailureMessage)
                ? BuildSpawnFailureMessage(brain.PlacementFailureReason, detail: null)
                : brain.SpawnFailureMessage;
            pending.SetFailure(reasonCode, failureMessage);
            RememberCompletedSpawn(new CompletedSpawnState(
                brain.BrainId,
                pending.PlacementEpoch,
                AcceptedForPlacement: true,
                PlacementReady: false,
                brain.PlacementLifecycleState,
                brain.PlacementReconcileState,
                reasonCode,
                failureMessage));
            pending.Completion.TrySetResult(false);
            UnregisterBrain(context, brain.BrainId, reason: "spawn_failed");
        }
    }

    private int ComputeSpawnCompletionTimeoutMs(BrainState? brain = null)
    {
        var assignmentWindow = (long)ComputePerAssignmentPlacementWindowMs() * EstimateSerialPlacementWindowAssignments(brain);
        var reconcileWindow = Math.Max(100, _options.PlacementReconcileTimeoutMs);
        var timeoutMs = assignmentWindow + reconcileWindow + 250L;
        return (int)Math.Min(int.MaxValue, Math.Max(500L, timeoutMs));
    }

    private async Task<PendingSpawnAwaitResult> AwaitPendingSpawnAsync(PendingSpawnState pending, int timeoutMs)
    {
        if (timeoutMs <= 0)
        {
            var completed = await pending.Completion.Task.ConfigureAwait(false);
            return new PendingSpawnAwaitResult(completed, TimedOut: false);
        }

        while (true)
        {
            var observedProgressVersion = pending.ProgressVersion;
            try
            {
                var completed = await pending.Completion.Task
                    .WaitAsync(TimeSpan.FromMilliseconds(timeoutMs))
                    .ConfigureAwait(false);
                return new PendingSpawnAwaitResult(completed, TimedOut: false);
            }
            catch (TimeoutException)
            {
                if (pending.Completion.Task.IsCompleted)
                {
                    continue;
                }

                if (pending.ProgressVersion != observedProgressVersion)
                {
                    continue;
                }

                return new PendingSpawnAwaitResult(Completed: false, TimedOut: true);
            }
        }
    }

    private int NormalizeAwaitSpawnTimeoutMs(ulong requestedTimeoutMs, PendingSpawnState? pending = null)
    {
        if (requestedTimeoutMs == 0)
        {
            return pending?.DefaultWaitTimeoutMs ?? ComputeSpawnCompletionTimeoutMs();
        }

        return (int)Math.Min(int.MaxValue, Math.Max(50UL, requestedTimeoutMs));
    }

    private void NotePendingSpawnProgress(Guid brainId, ulong placementEpoch)
    {
        if (_pendingSpawns.TryGetValue(brainId, out var pending)
            && pending.PlacementEpoch == placementEpoch)
        {
            pending.NoteProgress();
        }
    }

    private int ComputePerAssignmentPlacementWindowMs()
    {
        var attempts = Math.Max(1, _options.PlacementAssignmentMaxRetries + 1);
        var assignmentTimeoutWindow = (long)ComputePlacementAssignmentAttemptTimeoutMs() * attempts;
        var retryWindow = (long)Math.Max(0, _options.PlacementAssignmentRetryBackoffMs) * Math.Max(0, attempts - 1);
        return (int)Math.Min(int.MaxValue, Math.Max(100L, assignmentTimeoutWindow + retryWindow));
    }

    private int ComputePlacementAssignmentAttemptTimeoutMs()
        => Math.Max(100, _options.PlacementAssignmentTimeoutMs);

    private int EstimateSerialPlacementWindowAssignments(BrainState? brain)
    {
        if (brain is null)
        {
            return 1;
        }

        var execution = brain.PlacementExecution;
        if (execution is null || execution.Assignments.Count == 0)
        {
            return 1;
        }

        var brainId = brain.BrainId;
        var placementEpoch = brain.PlacementEpoch;

        var workerAssignmentCounts = new Dictionary<Guid, int>();
        var fallbackAssignments = 0;
        foreach (var assignment in execution.Assignments.Values)
        {
            if (TryGetGuid(assignment.Assignment.WorkerNodeId, out var workerNodeId))
            {
                workerAssignmentCounts[workerNodeId] = workerAssignmentCounts.TryGetValue(workerNodeId, out var existingCount)
                    ? existingCount + 1
                    : 1;
            }
            else
            {
                fallbackAssignments++;
            }
        }

        var maxSerialAssignments = Math.Max(1, fallbackAssignments);
        foreach (var (workerNodeId, ownAssignmentCount) in workerAssignmentCounts)
        {
            var queuedAheadAssignments = 0;
            if (_workerPlacementDispatches.TryGetValue(workerNodeId, out var dispatchState)
                && dispatchState is not null)
            {
                queuedAheadAssignments = dispatchState.Pending
                    .Where(pending => pending.BrainId != brainId || pending.PlacementEpoch != placementEpoch)
                    .Sum(static pending => pending.Assignments.Count);
                if (dispatchState.Active is { } active
                    && (active.BrainId != brainId || active.PlacementEpoch != placementEpoch))
                {
                    queuedAheadAssignments += active.RemainingAssignmentCount;
                }
            }

            maxSerialAssignments = Math.Max(maxSerialAssignments, queuedAheadAssignments + ownAssignmentCount);
        }

        return Math.Max(
            Math.Max(1, maxSerialAssignments),
            execution.Assignments.Count);
    }
}
