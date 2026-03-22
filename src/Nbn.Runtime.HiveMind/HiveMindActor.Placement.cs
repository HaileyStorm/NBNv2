using System.Globalization;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private void HandleSpawnBrain(IContext context, ProtoControl.SpawnBrain message)
    {
        Guid brainId = Guid.Empty;
        try
        {
            if (message.BrainDef is null
                || !HasArtifactRef(message.BrainDef)
                || !string.Equals(message.BrainDef.MediaType, "application/x-nbn", StringComparison.OrdinalIgnoreCase))
            {
                context.Respond(BuildSpawnFailureAck(
                    reasonCode: "spawn_invalid_request",
                    failureMessage: "Spawn request rejected: brain definition must be a valid application/x-nbn artifact reference."));
                return;
            }

            do
            {
                brainId = Guid.NewGuid();
            } while (_brains.ContainsKey(brainId) || _pendingSpawns.ContainsKey(brainId));

            var placementAck = ProcessPlacementRequest(
                context,
                new ProtoControl.RequestPlacement
                {
                    BrainId = brainId.ToProtoUuid(),
                    BaseDef = message.BrainDef.Clone(),
                    RequestId = $"spawn:{brainId:N}",
                    RequestedMs = (ulong)NowMs()
                });

            if (!placementAck.Accepted || !_brains.TryGetValue(brainId, out var brain))
            {
                if (_brains.ContainsKey(brainId))
                {
                    UnregisterBrain(context, brainId, reason: "spawn_failed");
                }

                var reasonCode = ToSpawnFailureReasonCode(placementAck.FailureReason);
                var failureMessage = !string.IsNullOrWhiteSpace(placementAck.Message)
                    ? placementAck.Message
                    : BuildSpawnFailureMessage(
                        placementAck.FailureReason,
                        detail: null,
                        fallbackReasonCode: reasonCode);
                context.Respond(BuildSpawnFailureAck(reasonCode, failureMessage));
                return;
            }

            if (message.HasPausePriority)
            {
                brain.PausePriority = message.PausePriority;
            }

            var pending = new PendingSpawnState(brain.BrainId, brain.PlacementEpoch);
            _pendingSpawns[brain.BrainId] = pending;

            ScheduleSelf(
                context,
                TimeSpan.FromMilliseconds(ComputeSpawnCompletionTimeoutMs()),
                new SpawnCompletionTimeout(brain.BrainId, brain.PlacementEpoch));

            context.ReenterAfter(
                pending.Completion.Task,
                task =>
                {
                    var completed = task.IsCompletedSuccessfully && task.Result;
                    if (completed)
                    {
                        context.Respond(new ProtoControl.SpawnBrainAck
                        {
                            BrainId = brain.BrainId.ToProtoUuid()
                        });
                        return Task.CompletedTask;
                    }

                    context.Respond(BuildSpawnFailureAck(
                        reasonCode: pending.FailureReasonCode,
                        failureMessage: pending.FailureMessage));
                    return Task.CompletedTask;
                });
        }
        catch (Exception ex)
        {
            if (brainId != Guid.Empty)
            {
                if (_pendingSpawns.Remove(brainId, out var pending))
                {
                    pending.SetFailure(
                        reasonCode: "spawn_internal_error",
                        failureMessage: $"Spawn failed: internal error while preparing placement ({ex.GetBaseException().Message}).");
                    pending.Completion.TrySetResult(false);
                }

                if (_brains.ContainsKey(brainId))
                {
                    try
                    {
                        UnregisterBrain(context, brainId, reason: "spawn_internal_error");
                    }
                    catch (Exception cleanupEx)
                    {
                        LogError($"Spawn cleanup failed for brain {brainId}: {cleanupEx.GetBaseException().Message}");
                        _brains.Remove(brainId);
                    }
                }
            }

            LogError($"Spawn failed while preparing brain {brainId}: {ex}");
            context.Respond(BuildSpawnFailureAck(
                reasonCode: "spawn_internal_error",
                failureMessage: $"Spawn failed: internal error while preparing placement ({ex.GetBaseException().Message})."));
        }
    }

    private void HandleRequestPlacement(IContext context, ProtoControl.RequestPlacement message)
        => context.Respond(ProcessPlacementRequest(context, message));

    private ProtoControl.PlacementAck ProcessPlacementRequest(IContext context, ProtoControl.RequestPlacement message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            HiveMindTelemetry.RecordPlacementRequestRejected(
                brainId: null,
                placementEpoch: 0,
                failureReason: "invalid_brain_id");
            return new ProtoControl.PlacementAck
            {
                Accepted = false,
                Message = "Invalid brain id.",
                PlacementEpoch = 0,
                LifecycleState = ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
                FailureReason = ProtoControl.PlacementFailureReason.PlacementFailureInvalidBrain,
                AcceptedMs = (ulong)NowMs()
            };
        }

        if (!_brains.TryGetValue(brainId, out var brain))
        {
            brain = new BrainState(brainId)
            {
                SpawnedMs = NowMs()
            };
            _brains[brainId] = brain;
        }

        var previousExecution = brain.PlacementExecution;
        var previousPlacementState = CapturePlacementState(brain);

        var nowMs = NowMs();
        brain.PlacementEpoch = brain.PlacementEpoch >= ulong.MaxValue ? 1UL : brain.PlacementEpoch + 1UL;
        brain.PlacementRequestedMs = nowMs;
        brain.PlacementRequestId = string.IsNullOrWhiteSpace(message.RequestId)
            ? $"{brainId:N}:{brain.PlacementEpoch}"
            : message.RequestId;
        brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileUnknown;
        brain.SpawnFailureReasonCode = string.Empty;
        brain.SpawnFailureMessage = string.Empty;

        if (message.BaseDef is not null && message.BaseDef.Sha256 is not null && message.BaseDef.Sha256.Value.Length == 32)
        {
            brain.BaseDefinition = message.BaseDef;
        }

        if (message.LastSnapshot is not null && message.LastSnapshot.Sha256 is not null && message.LastSnapshot.Sha256.Value.Length == 32)
        {
            brain.LastSnapshot = message.LastSnapshot;
            if (TryReadPlacementSnapshotHeader(brain.LastSnapshot, out var snapshotHeader)
                && snapshotHeader.HomeostasisConfig is not null
                && IsValidSnapshotHomeostasisConfig(snapshotHeader.HomeostasisConfig))
            {
                ApplySnapshotHomeostasisConfig(brain, snapshotHeader.HomeostasisConfig);
            }
        }

        if (message.InputWidth > 0)
        {
            brain.InputWidth = Math.Max(brain.InputWidth, (int)message.InputWidth);
        }

        if (message.OutputWidth > 0)
        {
            brain.OutputWidth = Math.Max(brain.OutputWidth, (int)message.OutputWidth);
        }

        brain.RequestedShardPlan = message.ShardPlan is null ? null : message.ShardPlan.Clone();

        if (!TryBuildPlacementPlan(brain, nowMs, out var plannedPlacement, out var failureReason, out var failureMessage))
        {
            if (previousExecution is not null)
            {
                RestorePlacementState(brain, previousPlacementState);
            }
            else
            {
                brain.PlannedPlacement = null;
                brain.PlacementExecution = null;
                RestorePlacementHomeostasis(brain, previousPlacementState);
            }
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
                failureReason);
            brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileFailed;
            SetSpawnFailureDetails(
                brain,
                ToSpawnFailureReasonCode(failureReason),
                BuildSpawnFailureMessage(failureReason, failureMessage));

            RegisterBrainWithIo(context, brain, force: true);

            var failedPlanLabel = message.ShardPlan is null ? "none" : message.ShardPlan.Mode.ToString();
            Log(
                $"Placement request rejected for brain {brainId} epoch={brain.PlacementEpoch} request={brain.PlacementRequestId} plan={failedPlanLabel} reason={failureReason}: {failureMessage}");
            HiveMindTelemetry.RecordPlacementRequestRejected(
                brainId,
                brain.PlacementEpoch,
                ToFailureReasonLabel(failureReason));
            EmitDebug(
                context,
                ProtoSeverity.SevWarn,
                "placement.request.rejected",
                $"Placement request rejected for brain {brainId} epoch={brain.PlacementEpoch} reason={ToFailureReasonLabel(failureReason)}.");

            return new ProtoControl.PlacementAck
            {
                Accepted = false,
                Message = failureMessage,
                PlacementEpoch = brain.PlacementEpoch,
                LifecycleState = brain.PlacementLifecycleState,
                FailureReason = brain.PlacementFailureReason,
                AcceptedMs = (ulong)brain.PlacementUpdatedMs,
                RequestId = brain.PlacementRequestId
            };
        }

        if (!TryCreatePlacementExecution(context, brain, plannedPlacement, out var executionFailure))
        {
            if (previousExecution is not null)
            {
                RestorePlacementState(brain, previousPlacementState);
            }
            else
            {
                brain.PlannedPlacement = null;
                brain.PlacementExecution = null;
                RestorePlacementHomeostasis(brain, previousPlacementState);
            }
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
                ProtoControl.PlacementFailureReason.PlacementFailureInternalError);
            brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileFailed;
            SetSpawnFailureDetails(
                brain,
                ToSpawnFailureReasonCode(ProtoControl.PlacementFailureReason.PlacementFailureInternalError),
                BuildSpawnFailureMessage(
                    ProtoControl.PlacementFailureReason.PlacementFailureInternalError,
                    executionFailure));
            HiveMindTelemetry.RecordPlacementRequestRejected(
                brainId,
                brain.PlacementEpoch,
                ToFailureReasonLabel(ProtoControl.PlacementFailureReason.PlacementFailureInternalError));
            EmitDebug(
                context,
                ProtoSeverity.SevWarn,
                "placement.request.rejected",
                $"Placement request rejected for brain {brainId} epoch={brain.PlacementEpoch} reason={ToFailureReasonLabel(ProtoControl.PlacementFailureReason.PlacementFailureInternalError)}.");

            return new ProtoControl.PlacementAck
            {
                Accepted = false,
                Message = executionFailure,
                PlacementEpoch = brain.PlacementEpoch,
                LifecycleState = brain.PlacementLifecycleState,
                FailureReason = brain.PlacementFailureReason,
                AcceptedMs = (ulong)brain.PlacementUpdatedMs,
                RequestId = brain.PlacementRequestId
            };
        }

        brain.PlannedPlacement = plannedPlacement.Clone();
        UpdateBrainIoWidthsFromPlannedAssignments(brain, plannedPlacement);
        if (previousExecution is not null)
        {
            DispatchPlacementUnassignments(context, brain, previousExecution, reason: "placement_replaced");
        }
        UpdatePlacementLifecycle(
            brain,
            ProtoControl.PlacementLifecycleState.PlacementLifecycleRequested,
            ProtoControl.PlacementFailureReason.PlacementFailureNone);
        brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileUnknown;

        RegisterBrainWithIo(context, brain, force: true);

        var planLabel = message.ShardPlan is null ? "none" : message.ShardPlan.Mode.ToString();
        var plannerWarnings = plannedPlacement.PlannerWarnings.Count == 0
            ? "none"
            : string.Join("|", plannedPlacement.PlannerWarnings);
        Log(
            $"Placement requested for brain {brainId} epoch={brain.PlacementEpoch} request={brain.PlacementRequestId} plan={planLabel} input={message.InputWidth} output={message.OutputWidth} assignments={plannedPlacement.Assignments.Count} workers={plannedPlacement.EligibleWorkers.Count} warnings={plannerWarnings}");
        HiveMindTelemetry.RecordPlacementRequestAccepted(
            brainId,
            brain.PlacementEpoch,
            plannedPlacement.Assignments.Count,
            plannedPlacement.EligibleWorkers.Count);
        EmitDebug(
            context,
            ProtoSeverity.SevInfo,
            "placement.requested",
            $"Placement requested for brain {brainId} epoch={brain.PlacementEpoch} assignments={plannedPlacement.Assignments.Count} workers={plannedPlacement.EligibleWorkers.Count}.");

        ScheduleSelf(context, TimeSpan.Zero, new DispatchPlacementPlan(brainId, brain.PlacementEpoch));

        return new ProtoControl.PlacementAck
        {
            Accepted = true,
            Message = "Placement request accepted.",
            PlacementEpoch = brain.PlacementEpoch,
            LifecycleState = brain.PlacementLifecycleState,
            FailureReason = brain.PlacementFailureReason,
            AcceptedMs = (ulong)brain.PlacementUpdatedMs,
            RequestId = brain.PlacementRequestId
        };
    }

    private void HandlePlacementUnassignmentAck(IContext context, ProtoControl.PlacementUnassignmentAck message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        var result = message.Accepted ? "accepted" : "rejected";
        var assignmentId = string.IsNullOrWhiteSpace(message.AssignmentId) ? "<missing>" : message.AssignmentId;
        var failure = ToFailureReasonLabel(message.FailureReason);
        EmitDebug(
            context,
            ProtoSeverity.SevDebug,
            "placement.unassignment.ack",
            $"Placement unassignment ack for brain {brainId} assignment={assignmentId} epoch={message.PlacementEpoch} result={result} failure={failure}.");
    }

    private void HandlePlacementAssignmentAck(IContext context, ProtoControl.PlacementAssignmentAck message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId) || !_brains.TryGetValue(brainId, out var brain))
        {
            return;
        }

        var execution = brain.PlacementExecution;

        if (brain.PlacementEpoch == 0 || message.PlacementEpoch != brain.PlacementEpoch)
        {
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.assignment_ack.ignored",
                $"Ignored assignment ack for brain {brainId}; epoch={message.PlacementEpoch} current={brain.PlacementEpoch}.");
            return;
        }

        if (execution is not null
            && execution.PlacementEpoch == brain.PlacementEpoch
            && execution.Completed)
        {
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.assignment_ack.ignored",
                $"Ignored assignment ack for brain {brainId}; placement execution already completed for epoch={message.PlacementEpoch}.");
            return;
        }

        if (execution is not null && execution.PlacementEpoch == brain.PlacementEpoch)
        {
            if (string.IsNullOrWhiteSpace(message.AssignmentId)
                || !execution.Assignments.TryGetValue(message.AssignmentId, out var trackedAssignment))
            {
                var assignmentId = string.IsNullOrWhiteSpace(message.AssignmentId)
                    ? "<missing>"
                    : message.AssignmentId;
                EmitDebug(
                    context,
                    ProtoSeverity.SevDebug,
                    "placement.assignment_ack.ignored",
                    $"Ignored assignment ack for brain {brainId}; assignment={assignmentId} is not tracked for epoch={message.PlacementEpoch}.");
                return;
            }

            if (trackedAssignment.Ready || trackedAssignment.Failed || !trackedAssignment.AwaitingAck)
            {
                EmitDebug(
                    context,
                    ProtoSeverity.SevDebug,
                    "placement.assignment_ack.ignored",
                    $"Ignored assignment ack for brain {brainId}; assignment={trackedAssignment.Assignment.AssignmentId} is not awaiting ack for epoch={message.PlacementEpoch}.");
                return;
            }

            if (!TryGetGuid(trackedAssignment.Assignment.WorkerNodeId, out var plannedWorkerId))
            {
                EmitDebug(
                    context,
                    ProtoSeverity.SevDebug,
                    "placement.assignment_ack.ignored",
                    $"Ignored assignment ack for brain {brainId}; assignment={trackedAssignment.Assignment.AssignmentId} reason=planned_worker_invalid.");
                return;
            }

            if (!execution.WorkerTargets.TryGetValue(plannedWorkerId, out var plannedWorkerPid))
            {
                EmitDebug(
                    context,
                    ProtoSeverity.SevDebug,
                    "placement.assignment_ack.ignored",
                    $"Ignored assignment ack for brain {brainId}; assignment={trackedAssignment.Assignment.AssignmentId} reason=planned_worker_unresolved plannedWorker={plannedWorkerId:D}.");
                return;
            }

            if (!SenderMatchesPid(context.Sender, plannedWorkerPid))
            {
                var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
                EmitDebug(
                    context,
                    ProtoSeverity.SevDebug,
                    "placement.assignment_ack.ignored",
                    $"Ignored assignment ack for brain {brainId}; assignment={trackedAssignment.Assignment.AssignmentId} reason=sender_worker_mismatch sender={senderLabel} plannedWorker={plannedWorkerId:D} plannedPid={PidLabel(plannedWorkerPid)}.");
                return;
            }

            trackedAssignment.AwaitingAck = false;
            trackedAssignment.AcceptedMs = NowMs();
            var target = ToPlacementTargetLabel(trackedAssignment.Assignment.Target);
            var ackFailureReason = message.FailureReason == ProtoControl.PlacementFailureReason.PlacementFailureNone
                ? (!message.Accepted || message.State == ProtoControl.PlacementAssignmentState.PlacementAssignmentFailed
                    ? ToFailureReasonLabel(ProtoControl.PlacementFailureReason.PlacementFailureAssignmentRejected)
                    : "none")
                : ToFailureReasonLabel(message.FailureReason);
            var ackLatencyMs = trackedAssignment.LastDispatchMs > 0
                ? trackedAssignment.AcceptedMs - trackedAssignment.LastDispatchMs
                : 0;
            HiveMindTelemetry.RecordPlacementAssignmentAck(
                brain.BrainId,
                brain.PlacementEpoch,
                target,
                ToAssignmentStateLabel(message.State),
                message.Accepted,
                message.Retryable,
                ackLatencyMs > 0 ? ackLatencyMs : 0,
                plannedWorkerId,
                ackFailureReason);

            if (!message.Accepted || message.State == ProtoControl.PlacementAssignmentState.PlacementAssignmentFailed)
            {
                var failureReason = message.FailureReason == ProtoControl.PlacementFailureReason.PlacementFailureNone
                    ? ProtoControl.PlacementFailureReason.PlacementFailureAssignmentRejected
                    : message.FailureReason;

                if (message.Retryable && CanRetryAssignment(trackedAssignment))
                {
                    var backoffMs = message.RetryAfterMs > 0
                        ? (int)Math.Min(int.MaxValue, message.RetryAfterMs)
                        : _options.PlacementAssignmentRetryBackoffMs;
                    var nextAttempt = trackedAssignment.Attempt + 1;
                    ScheduleSelf(
                        context,
                        TimeSpan.FromMilliseconds(Math.Max(0, backoffMs)),
                        new RetryPlacementAssignment(brain.BrainId, brain.PlacementEpoch, trackedAssignment.Assignment.AssignmentId, nextAttempt));
                    HiveMindTelemetry.RecordPlacementAssignmentRetry(
                        brain.BrainId,
                        brain.PlacementEpoch,
                        target,
                        nextAttempt,
                        "ack_retryable",
                        plannedWorkerId);
                    EmitDebug(
                        context,
                        ProtoSeverity.SevInfo,
                        "placement.assignment.retry",
                        $"Placement assignment {trackedAssignment.Assignment.AssignmentId} for brain {brain.BrainId} scheduled retry attempt={nextAttempt} target={target} reason=ack_retryable.");
                    UpdatePlacementLifecycle(
                        brain,
                        ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigning,
                        ProtoControl.PlacementFailureReason.PlacementFailureNone);
                    return;
                }

                trackedAssignment.Failed = true;
                EmitDebug(
                    context,
                    ProtoSeverity.SevWarn,
                    "placement.assignment.failed",
                    $"Placement assignment {trackedAssignment.Assignment.AssignmentId} for brain {brain.BrainId} failed target={target} reason={ToFailureReasonLabel(failureReason)}.");
                FailPlacementExecution(
                    context,
                    brain,
                    failureReason,
                    ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
                    ToSpawnFailureReasonCode(failureReason),
                    BuildSpawnFailureMessage(failureReason, message.Message));
                return;
            }

            switch (message.State)
            {
                case ProtoControl.PlacementAssignmentState.PlacementAssignmentPending:
                case ProtoControl.PlacementAssignmentState.PlacementAssignmentAccepted:
                    UpdatePlacementLifecycle(
                        brain,
                        ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigning,
                        ProtoControl.PlacementFailureReason.PlacementFailureNone);
                    trackedAssignment.Accepted = true;
                    break;
                case ProtoControl.PlacementAssignmentState.PlacementAssignmentReady:
                    trackedAssignment.Accepted = true;
                    trackedAssignment.Ready = true;
                    trackedAssignment.ReadyMs = NowMs();
                    if (trackedAssignment.LastDispatchMs > 0)
                    {
                        HiveMindTelemetry.RecordPlacementAssignmentReadyLatency(
                            brain.BrainId,
                            brain.PlacementEpoch,
                            target,
                            trackedAssignment.ReadyMs - trackedAssignment.LastDispatchMs,
                            plannedWorkerId);
                    }
                    UpdatePlacementLifecycle(
                        brain,
                        ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigning,
                        ProtoControl.PlacementFailureReason.PlacementFailureNone);
                    MaybeStartReconcile(context, brain);
                    break;
                case ProtoControl.PlacementAssignmentState.PlacementAssignmentDraining:
                    brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileRequiresAction;
                    UpdatePlacementLifecycle(
                        brain,
                        ProtoControl.PlacementLifecycleState.PlacementLifecycleReconciling,
                        ProtoControl.PlacementFailureReason.PlacementFailureNone);
                    break;
            }

            return;
        }

        HandlePlacementAssignmentAckLegacy(context, brain, message);
    }

    private void HandlePlacementAssignmentAckLegacy(IContext context, BrainState brain, ProtoControl.PlacementAssignmentAck message)
    {
        if (!message.Accepted || message.State == ProtoControl.PlacementAssignmentState.PlacementAssignmentFailed)
        {
            var failureReason = message.FailureReason == ProtoControl.PlacementFailureReason.PlacementFailureNone
                ? ProtoControl.PlacementFailureReason.PlacementFailureAssignmentRejected
                : message.FailureReason;
            UpdatePlacementLifecycle(brain, ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed, failureReason);
            brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileFailed;
            TryCompletePendingSpawn(context, brain);
            return;
        }

        switch (message.State)
        {
            case ProtoControl.PlacementAssignmentState.PlacementAssignmentPending:
            case ProtoControl.PlacementAssignmentState.PlacementAssignmentAccepted:
                UpdatePlacementLifecycle(
                    brain,
                    ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigning,
                    ProtoControl.PlacementFailureReason.PlacementFailureNone);
                break;
            case ProtoControl.PlacementAssignmentState.PlacementAssignmentReady:
                UpdatePlacementLifecycle(
                    brain,
                    ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned,
                    ProtoControl.PlacementFailureReason.PlacementFailureNone);
                break;
            case ProtoControl.PlacementAssignmentState.PlacementAssignmentDraining:
                brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileRequiresAction;
                UpdatePlacementLifecycle(
                    brain,
                    ProtoControl.PlacementLifecycleState.PlacementLifecycleReconciling,
                    ProtoControl.PlacementFailureReason.PlacementFailureNone);
                break;
            default:
                break;
        }

        TryCompletePendingSpawn(context, brain);
    }

    private void HandlePlacementReconcileReport(IContext context, ProtoControl.PlacementReconcileReport message)
    {
        var reconcileTarget = ResolveReconcileTargetLabel(message);
        var observedWorkerId = TryResolveObservedWorkerNodeId(message, out var parsedWorkerId)
            ? parsedWorkerId
            : (Guid?)null;

        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.reconcile.ignored",
                $"Ignored reconcile report with invalid brain id; epoch={message.PlacementEpoch}.");
            return;
        }

        if (!_brains.TryGetValue(brainId, out var brain))
        {
            HiveMindTelemetry.RecordPlacementReconcileIgnored(
                brainId,
                message.PlacementEpoch,
                "brain_not_tracked",
                observedWorkerId,
                reconcileTarget);
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.reconcile.ignored",
                $"Ignored reconcile report for brain {brainId}; reason=brain_not_tracked epoch={message.PlacementEpoch}.");
            return;
        }

        var execution = brain.PlacementExecution;

        if (brain.PlacementEpoch == 0 || message.PlacementEpoch != brain.PlacementEpoch)
        {
            HiveMindTelemetry.RecordPlacementReconcileIgnored(
                brain.BrainId,
                message.PlacementEpoch,
                "placement_epoch_mismatch",
                observedWorkerId,
                reconcileTarget);
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.reconcile.ignored",
                $"Ignored reconcile report for brain {brainId}; epoch={message.PlacementEpoch} current={brain.PlacementEpoch}.");
            return;
        }

        if (execution is not null
            && execution.PlacementEpoch == brain.PlacementEpoch
            && execution.Completed)
        {
            HiveMindTelemetry.RecordPlacementReconcileIgnored(
                brain.BrainId,
                message.PlacementEpoch,
                "execution_completed",
                observedWorkerId,
                reconcileTarget);
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.reconcile.ignored",
                $"Ignored reconcile report for brain {brainId}; placement execution already completed for epoch={message.PlacementEpoch}.");
            return;
        }

        if (execution is not null && execution.PlacementEpoch == brain.PlacementEpoch)
        {
            if (!execution.ReconcileRequested)
            {
                HiveMindTelemetry.RecordPlacementReconcileIgnored(
                    brain.BrainId,
                    message.PlacementEpoch,
                    "reconcile_not_requested",
                    observedWorkerId,
                    reconcileTarget);
                EmitDebug(
                    context,
                    ProtoSeverity.SevDebug,
                    "placement.reconcile.ignored",
                    $"Ignored reconcile report for brain {brainId}; reconcile has not started for epoch={message.PlacementEpoch}.");
                return;
            }

            HandleTrackedPlacementReconcileReport(context, brain, message);
            return;
        }

        HandlePlacementReconcileReportLegacy(context, brain, message);
    }

    private void HandlePlacementReconcileReportLegacy(IContext context, BrainState brain, ProtoControl.PlacementReconcileReport message)
    {
        brain.PlacementReconcileState = message.ReconcileState;
        switch (message.ReconcileState)
        {
            case ProtoControl.PlacementReconcileState.PlacementReconcileMatched:
                UpdatePlacementLifecycle(
                    brain,
                    brain.Shards.Count > 0
                        ? ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning
                        : ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned,
                    ProtoControl.PlacementFailureReason.PlacementFailureNone);
                break;
            case ProtoControl.PlacementReconcileState.PlacementReconcileRequiresAction:
                UpdatePlacementLifecycle(
                    brain,
                    ProtoControl.PlacementLifecycleState.PlacementLifecycleReconciling,
                    ProtoControl.PlacementFailureReason.PlacementFailureNone);
                break;
            case ProtoControl.PlacementReconcileState.PlacementReconcileFailed:
                UpdatePlacementLifecycle(
                    brain,
                    ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
                    message.FailureReason == ProtoControl.PlacementFailureReason.PlacementFailureNone
                        ? ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch
                        : message.FailureReason);
                break;
        }

        TryCompletePendingSpawn(context, brain);
    }

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

    private void ApplyObservedControlAssignments(IContext context, BrainState brain, PlacementExecutionState execution)
    {
        var routingUpdated = false;
        var ioUpdated = false;
        if (TryGetObservedControlPid(execution, ProtoControl.PlacementAssignmentTarget.PlacementTargetBrainRoot, out var observedRoot)
            && !PidEquals(brain.BrainRootPid, observedRoot))
        {
            brain.BrainRootPid = NormalizePid(context, observedRoot) ?? observedRoot;
            routingUpdated = true;
        }

        if (TryGetObservedControlPid(execution, ProtoControl.PlacementAssignmentTarget.PlacementTargetSignalRouter, out var observedRouter)
            && !PidEquals(brain.SignalRouterPid, observedRouter))
        {
            brain.SignalRouterPid = NormalizePid(context, observedRouter) ?? observedRouter;
            routingUpdated = true;
        }

        if (TryGetObservedControlPid(execution, ProtoControl.PlacementAssignmentTarget.PlacementTargetInputCoordinator, out var observedInputCoordinator)
            && !PidEquals(brain.InputCoordinatorPid, observedInputCoordinator))
        {
            brain.InputCoordinatorPid = NormalizePid(context, observedInputCoordinator) ?? observedInputCoordinator;
            ioUpdated = true;
        }

        if (TryGetObservedControlPid(execution, ProtoControl.PlacementAssignmentTarget.PlacementTargetOutputCoordinator, out var observedOutputCoordinator)
            && !PidEquals(brain.OutputCoordinatorPid, observedOutputCoordinator))
        {
            brain.OutputCoordinatorPid = NormalizePid(context, observedOutputCoordinator) ?? observedOutputCoordinator;
            ioUpdated = true;
        }

        if (brain.SignalRouterPid is not null && string.IsNullOrWhiteSpace(brain.SignalRouterPid.Address))
        {
            var fallbackAddress = brain.BrainRootPid?.Address;
            if (!string.IsNullOrWhiteSpace(fallbackAddress))
            {
                brain.SignalRouterPid = new PID(fallbackAddress, brain.SignalRouterPid.Id);
                routingUpdated = true;
            }
        }

        if (brain.BrainRootPid is not null && string.IsNullOrWhiteSpace(brain.BrainRootPid.Address))
        {
            var fallbackAddress = brain.SignalRouterPid?.Address;
            if (!string.IsNullOrWhiteSpace(fallbackAddress))
            {
                brain.BrainRootPid = new PID(fallbackAddress, brain.BrainRootPid.Id);
                routingUpdated = true;
            }
        }

        var coordinatorFallbackAddress = brain.SignalRouterPid?.Address;
        if (string.IsNullOrWhiteSpace(coordinatorFallbackAddress))
        {
            coordinatorFallbackAddress = brain.BrainRootPid?.Address;
        }

        if (brain.InputCoordinatorPid is not null
            && string.IsNullOrWhiteSpace(brain.InputCoordinatorPid.Address)
            && !string.IsNullOrWhiteSpace(coordinatorFallbackAddress))
        {
            brain.InputCoordinatorPid = new PID(coordinatorFallbackAddress, brain.InputCoordinatorPid.Id);
            ioUpdated = true;
        }

        if (brain.OutputCoordinatorPid is not null
            && string.IsNullOrWhiteSpace(brain.OutputCoordinatorPid.Address)
            && !string.IsNullOrWhiteSpace(coordinatorFallbackAddress))
        {
            brain.OutputCoordinatorPid = new PID(coordinatorFallbackAddress, brain.OutputCoordinatorPid.Id);
            ioUpdated = true;
        }

        if (!routingUpdated && !ioUpdated)
        {
            return;
        }

        if (routingUpdated)
        {
            UpdateRoutingTable(context, brain);
            ReportBrainRegistration(context, brain);
        }

        if (ioUpdated)
        {
            RegisterBrainWithIo(context, brain, force: true);
        }
    }

    private static bool TryGetObservedControlPid(
        PlacementExecutionState execution,
        ProtoControl.PlacementAssignmentTarget target,
        out PID pid)
    {
        foreach (var observed in execution.ObservedAssignments.Values.OrderBy(static value => value.AssignmentId, StringComparer.Ordinal))
        {
            if (observed.Target != target || !TryParsePid(observed.ActorPid, out var observedPid))
            {
                continue;
            }

            pid = observedPid;
            return true;
        }

        pid = new PID();
        return false;
    }

    private static bool PidEquals(PID? left, PID right)
        => left is not null
           && string.Equals(left.Address ?? string.Empty, right.Address ?? string.Empty, StringComparison.OrdinalIgnoreCase)
           && string.Equals(left.Id ?? string.Empty, right.Id ?? string.Empty, StringComparison.Ordinal);

    private static bool SenderMatchesPid(PID? sender, PID expected)
    {
        if (sender is null)
        {
            return false;
        }

        return expected.Equals(sender)
               || PidEquals(sender, expected)
               || PidHasEquivalentEndpoint(sender, expected);
    }

    private static bool PidHasEquivalentEndpoint(PID sender, PID expected)
    {
        if (!string.Equals(sender.Id ?? string.Empty, expected.Id ?? string.Empty, StringComparison.Ordinal))
        {
            return false;
        }

        if (!TryParseEndpoint(sender.Address, out var senderHost, out var senderPort)
            || !TryParseEndpoint(expected.Address, out var expectedHost, out var expectedPort))
        {
            return false;
        }

        if (senderPort != expectedPort)
        {
            return false;
        }

        if (string.Equals(senderHost, expectedHost, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var senderClass = ClassifyEndpointHost(senderHost);
        var expectedClass = ClassifyEndpointHost(expectedHost);
        if (senderClass == EndpointHostClass.Loopback && expectedClass == EndpointHostClass.Loopback)
        {
            return true;
        }

        if ((senderClass == EndpointHostClass.Wildcard && expectedClass == EndpointHostClass.Loopback)
            || (senderClass == EndpointHostClass.Loopback && expectedClass == EndpointHostClass.Wildcard))
        {
            return true;
        }

        return HostsResolveToSameAddress(senderHost, expectedHost);
    }

    private static bool TryParseEndpoint(string? address, out string host, out int port)
    {
        host = string.Empty;
        port = 0;
        if (string.IsNullOrWhiteSpace(address))
        {
            return false;
        }

        var trimmed = address.Trim();
        if (trimmed.Length == 0)
        {
            return false;
        }

        if (trimmed[0] == '[')
        {
            var closingBracket = trimmed.IndexOf(']');
            if (closingBracket <= 1 || closingBracket >= trimmed.Length - 1 || trimmed[closingBracket + 1] != ':')
            {
                return false;
            }

            var bracketHost = trimmed[1..closingBracket];
            var bracketPort = trimmed[(closingBracket + 2)..];
            if (!int.TryParse(bracketPort, NumberStyles.None, CultureInfo.InvariantCulture, out port) || port <= 0)
            {
                return false;
            }

            host = bracketHost;
            return !string.IsNullOrWhiteSpace(host);
        }

        var separator = trimmed.LastIndexOf(':');
        if (separator <= 0 || separator >= trimmed.Length - 1)
        {
            return false;
        }

        var hostToken = trimmed[..separator];
        var portToken = trimmed[(separator + 1)..];
        if (!int.TryParse(portToken, NumberStyles.None, CultureInfo.InvariantCulture, out port) || port <= 0)
        {
            return false;
        }

        host = hostToken;
        return !string.IsNullOrWhiteSpace(host);
    }

    private static EndpointHostClass ClassifyEndpointHost(string host)
    {
        var normalized = host.Trim();
        if (normalized.StartsWith("[", StringComparison.Ordinal) && normalized.EndsWith("]", StringComparison.Ordinal))
        {
            normalized = normalized[1..^1];
        }

        if (normalized.Equals("localhost", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("::1", StringComparison.OrdinalIgnoreCase))
        {
            return EndpointHostClass.Loopback;
        }

        if (normalized.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("::", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("*", StringComparison.Ordinal)
            || normalized.Equals("+", StringComparison.Ordinal))
        {
            return EndpointHostClass.Wildcard;
        }

        return EndpointHostClass.Other;
    }

    private static string NormalizeEndpointAddress(string? address)
    {
        if (string.IsNullOrWhiteSpace(address))
        {
            return string.Empty;
        }

        if (TryParseEndpoint(address, out var host, out var port))
        {
            return $"{host.Trim().ToLowerInvariant()}:{port.ToString(CultureInfo.InvariantCulture)}";
        }

        return address.Trim().ToLowerInvariant();
    }

    private static bool HostsResolveToSameAddress(string senderHost, string expectedHost)
    {
        var senderAddresses = ResolveEndpointHostAddresses(senderHost);
        if (senderAddresses.Count == 0)
        {
            return false;
        }

        var expectedAddresses = ResolveEndpointHostAddresses(expectedHost);
        if (expectedAddresses.Count == 0)
        {
            return false;
        }

        foreach (var senderAddress in senderAddresses)
        {
            if (expectedAddresses.Contains(senderAddress, StringComparer.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static IReadOnlyList<string> ResolveEndpointHostAddresses(string host)
    {
        var normalized = host.Trim();
        if (normalized.StartsWith("[", StringComparison.Ordinal) && normalized.EndsWith("]", StringComparison.Ordinal))
        {
            normalized = normalized[1..^1];
        }

        if (normalized.Length == 0)
        {
            return Array.Empty<string>();
        }

        if (IPAddress.TryParse(normalized, out var parsed))
        {
            return [NormalizeComparableAddress(parsed)];
        }

        try
        {
            return Dns.GetHostAddresses(normalized)
                .Select(NormalizeComparableAddress)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
        catch
        {
            return Array.Empty<string>();
        }
    }

    private static string NormalizeComparableAddress(IPAddress address)
    {
        if (address.IsIPv4MappedToIPv6)
        {
            address = address.MapToIPv4();
        }

        return address.ToString();
    }

    private static bool IsLikelyLocalSubscriberPid(ActorSystem system, PID? pid)
    {
        if (pid is null)
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(pid.Address))
        {
            return true;
        }

        if (string.IsNullOrWhiteSpace(system.Address))
        {
            return false;
        }

        if (string.Equals(pid.Address, system.Address, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var probeSender = new PID(pid.Address, "probe");
        var probeExpected = new PID(system.Address, "probe");
        return PidHasEquivalentEndpoint(probeSender, probeExpected);
    }

    private static bool TryLookupProcessInRegistry(ActorSystem system, PID pid, out object? process)
    {
        process = null;
        if (ProcessRegistryProperty is null || ProcessRegistryLookupMethod is null)
        {
            return false;
        }

        try
        {
            var registry = ProcessRegistryProperty.GetValue(system);
            if (registry is null)
            {
                return false;
            }

            process = ProcessRegistryLookupMethod.Invoke(registry, new object?[] { pid });
            return true;
        }
        catch
        {
            process = null;
            return false;
        }
    }

    private static MethodInfo? ResolveProcessRegistryLookupMethod()
    {
        var registryType = ProcessRegistryProperty?.PropertyType;
        if (registryType is null)
        {
            return null;
        }

        return registryType
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .FirstOrDefault(static method =>
            {
                var parameters = method.GetParameters();
                return parameters.Length == 1
                       && parameters[0].ParameterType == typeof(PID)
                       && method.ReturnType != typeof(void)
                       && method.ReturnType != typeof(bool)
                       && !method.ReturnType.IsByRef;
            });
    }

    private static string ToPlacementTargetLabel(ProtoControl.PlacementAssignmentTarget target)
        => target switch
        {
            ProtoControl.PlacementAssignmentTarget.PlacementTargetBrainRoot => "brain_root",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetSignalRouter => "signal_router",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetInputCoordinator => "input_coordinator",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetOutputCoordinator => "output_coordinator",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetRegionShard => "region_shard",
            _ => "unknown"
        };

    private static string ToAssignmentStateLabel(ProtoControl.PlacementAssignmentState state)
        => state switch
        {
            ProtoControl.PlacementAssignmentState.PlacementAssignmentPending => "pending",
            ProtoControl.PlacementAssignmentState.PlacementAssignmentAccepted => "accepted",
            ProtoControl.PlacementAssignmentState.PlacementAssignmentReady => "ready",
            ProtoControl.PlacementAssignmentState.PlacementAssignmentDraining => "draining",
            ProtoControl.PlacementAssignmentState.PlacementAssignmentFailed => "failed",
            _ => "unknown"
        };

    private static string ToFailureReasonLabel(ProtoControl.PlacementFailureReason reason)
        => reason switch
        {
            ProtoControl.PlacementFailureReason.PlacementFailureNone => "none",
            ProtoControl.PlacementFailureReason.PlacementFailureInvalidBrain => "invalid_brain",
            ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable => "worker_unavailable",
            ProtoControl.PlacementFailureReason.PlacementFailureAssignmentRejected => "assignment_rejected",
            ProtoControl.PlacementFailureReason.PlacementFailureAssignmentTimeout => "assignment_timeout",
            ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch => "reconcile_mismatch",
            ProtoControl.PlacementFailureReason.PlacementFailureInternalError => "internal_error",
            _ => "unknown"
        };

    private static string ToSpawnFailureReasonCode(ProtoControl.PlacementFailureReason reason)
        => reason switch
        {
            ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable => "spawn_worker_unavailable",
            ProtoControl.PlacementFailureReason.PlacementFailureAssignmentRejected => "spawn_assignment_rejected",
            ProtoControl.PlacementFailureReason.PlacementFailureAssignmentTimeout => "spawn_assignment_timeout",
            ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch => "spawn_reconcile_mismatch",
            ProtoControl.PlacementFailureReason.PlacementFailureInternalError => "spawn_internal_error",
            ProtoControl.PlacementFailureReason.PlacementFailureInvalidBrain => "spawn_invalid_request",
            _ => "spawn_failed"
        };

    private static string BuildSpawnFailureMessage(
        ProtoControl.PlacementFailureReason reason,
        string? detail,
        string? fallbackReasonCode = null)
    {
        if (!string.IsNullOrWhiteSpace(detail))
        {
            return detail.Trim();
        }

        var reasonCode = string.IsNullOrWhiteSpace(fallbackReasonCode)
            ? ToSpawnFailureReasonCode(reason)
            : fallbackReasonCode.Trim();

        return reasonCode switch
        {
            "spawn_invalid_request" => "Spawn request rejected: invalid brain definition request.",
            "spawn_timeout" => "Spawn timed out while waiting for placement completion.",
            "spawn_worker_unavailable" => "Spawn failed: no eligible worker was available for the placement plan.",
            "spawn_assignment_rejected" => "Spawn failed: a worker rejected one or more placement assignments.",
            "spawn_assignment_timeout" => "Spawn failed: placement assignment acknowledgements timed out and retry budget was exhausted.",
            "spawn_reconcile_timeout" => "Spawn failed: placement reconcile timed out before workers reported final assignments.",
            "spawn_reconcile_mismatch" => "Spawn failed: reconcile results did not match planned assignments.",
            "spawn_internal_error" => "Spawn failed: an internal placement error occurred.",
            _ => "Spawn failed before placement completed."
        };
    }

    private static void SetSpawnFailureDetails(BrainState brain, string reasonCode, string failureMessage)
    {
        var normalizedReasonCode = string.IsNullOrWhiteSpace(reasonCode)
            ? "spawn_failed"
            : reasonCode.Trim();
        var normalizedFailureMessage = string.IsNullOrWhiteSpace(failureMessage)
            ? BuildSpawnFailureMessage(
                ProtoControl.PlacementFailureReason.PlacementFailureNone,
                detail: null,
                fallbackReasonCode: normalizedReasonCode)
            : failureMessage.Trim();
        brain.SpawnFailureReasonCode = normalizedReasonCode;
        brain.SpawnFailureMessage = normalizedFailureMessage;
    }

    private static ProtoControl.SpawnBrainAck BuildSpawnFailureAck(string? reasonCode, string? failureMessage)
    {
        var normalizedReasonCode = string.IsNullOrWhiteSpace(reasonCode)
            ? "spawn_failed"
            : reasonCode.Trim();
        var normalizedFailureMessage = string.IsNullOrWhiteSpace(failureMessage)
            ? BuildSpawnFailureMessage(
                ProtoControl.PlacementFailureReason.PlacementFailureNone,
                detail: null,
                fallbackReasonCode: normalizedReasonCode)
            : failureMessage.Trim();
        return new ProtoControl.SpawnBrainAck
        {
            BrainId = Guid.Empty.ToProtoUuid(),
            FailureReasonCode = normalizedReasonCode,
            FailureMessage = normalizedFailureMessage
        };
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

    private ProtoControl.PlacementLifecycleInfo BuildPlacementLifecycleInfo(Guid brainId)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return new ProtoControl.PlacementLifecycleInfo
            {
                BrainId = brainId.ToProtoUuid(),
                LifecycleState = ProtoControl.PlacementLifecycleState.PlacementLifecycleUnknown,
                FailureReason = ProtoControl.PlacementFailureReason.PlacementFailureNone,
                ReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileUnknown
            };
        }

        var info = new ProtoControl.PlacementLifecycleInfo
        {
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = brain.PlacementEpoch,
            LifecycleState = brain.PlacementLifecycleState,
            FailureReason = brain.PlacementFailureReason,
            ReconcileState = brain.PlacementReconcileState,
            RequestedMs = brain.PlacementRequestedMs > 0 ? (ulong)brain.PlacementRequestedMs : 0,
            UpdatedMs = brain.PlacementUpdatedMs > 0 ? (ulong)brain.PlacementUpdatedMs : 0,
            RequestId = brain.PlacementRequestId,
            RegisteredShards = (uint)brain.Shards.Count
        };

        if (brain.RequestedShardPlan is not null)
        {
            info.ShardPlan = brain.RequestedShardPlan.Clone();
        }

        return info;
    }

    private bool TryBuildPlacementPlan(
        BrainState brain,
        long nowMs,
        out PlacementPlanner.PlacementPlanningResult plan,
        out ProtoControl.PlacementFailureReason failureReason,
        out string failureMessage)
    {
        RefreshWorkerCatalogFreshness(nowMs);

        if (!TryResolvePlacementRegions(brain, out var regions, out var shardStride, out var regionWarning, out var regionFailure))
        {
            plan = new PlacementPlanner.PlacementPlanningResult(
                brain.PlacementEpoch,
                brain.PlacementRequestId,
                brain.PlacementRequestedMs,
                nowMs,
                _workerCatalogSnapshotMs > 0 ? (ulong)_workerCatalogSnapshotMs : (ulong)nowMs,
                Array.Empty<PlacementPlanner.WorkerCandidate>(),
                Array.Empty<ProtoControl.PlacementAssignment>(),
                regionFailure is null ? Array.Empty<string>() : new[] { regionFailure });
            failureReason = ProtoControl.PlacementFailureReason.PlacementFailureInternalError;
            failureMessage = regionFailure ?? "Unable to derive placement regions from request metadata.";
            return false;
        }

        var snapshotMs = _workerCatalogSnapshotMs > 0 ? (ulong)_workerCatalogSnapshotMs : (ulong)nowMs;
        var currentWorkerNodeIds = GetCurrentPlacementWorkerNodeIds(brain);
        var hostedBrainCounts = BuildHostedBrainCounts(brain.BrainId);
        var workers = _workerCatalog.Values
            .Where(entry => IsPlacementWorkerCandidate(entry.LogicalName, entry.WorkerRootActorName))
            .Select(entry => new PlacementPlanner.WorkerCandidate(
                entry.NodeId,
                entry.WorkerAddress,
                entry.WorkerRootActorName,
                entry.IsAlive,
                entry.IsReady,
                entry.IsFresh,
                entry.CpuCores,
                entry.RamFreeBytes,
                entry.RamTotalBytes,
                entry.StorageFreeBytes,
                entry.StorageTotalBytes,
                entry.HasGpu,
                entry.VramFreeBytes,
                entry.VramTotalBytes,
                entry.CpuScore,
                entry.GpuScore,
                entry.CpuLimitPercent,
                entry.RamLimitPercent,
                entry.StorageLimitPercent,
                entry.GpuComputeLimitPercent,
                entry.GpuVramLimitPercent,
                entry.ProcessCpuLoadPercent,
                entry.ProcessRamUsedBytes,
                _workerPressureLimitTolerancePercent,
                entry.AveragePeerLatencyMs,
                (uint)Math.Max(0, entry.PeerLatencySampleCount),
                hostedBrainCounts.TryGetValue(entry.NodeId, out var hostedBrainCount) ? hostedBrainCount : 0))
            .ToArray();

        if (!workers.Any(static worker =>
                worker.IsAlive
                && worker.IsReady
                && worker.IsFresh
                && !string.IsNullOrWhiteSpace(worker.WorkerRootActorName)))
        {
            plan = new PlacementPlanner.PlacementPlanningResult(
                brain.PlacementEpoch,
                brain.PlacementRequestId,
                brain.PlacementRequestedMs,
                nowMs,
                snapshotMs,
                Array.Empty<PlacementPlanner.WorkerCandidate>(),
                Array.Empty<ProtoControl.PlacementAssignment>(),
                regionWarning is null ? Array.Empty<string>() : new[] { regionWarning });
            failureReason = ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable;
            failureMessage = "No eligible workers are available for placement.";
            return false;
        }

        var plannerInputs = new PlacementPlanner.PlannerInputs(
            brain.BrainId,
            brain.PlacementEpoch,
            brain.PlacementRequestId,
            brain.PlacementRequestedMs,
            nowMs,
            snapshotMs,
            shardStride,
            brain.RequestedShardPlan,
            regions,
            currentWorkerNodeIds,
            RegionShardComputeBackendPreferenceResolver.Resolve());
        var planned = PlacementPlanner.TryBuildPlan(
            plannerInputs,
            workers,
            out plan,
            out failureReason,
            out failureMessage);
        if (!planned)
        {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(regionWarning))
        {
            var warnings = plan.PlannerWarnings.Concat(new[] { regionWarning }).ToArray();
            plan = new PlacementPlanner.PlacementPlanningResult(
                plan.PlacementEpoch,
                plan.RequestId,
                plan.RequestedMs,
                plan.PlannedMs,
                plan.WorkerSnapshotMs,
                plan.EligibleWorkers,
                plan.Assignments,
                warnings);
        }

        return true;
    }

    private IReadOnlyList<Guid> GetCurrentPlacementWorkerNodeIds(BrainState brain)
    {
        if (brain.PlacementExecution is null)
        {
            return Array.Empty<Guid>();
        }

        var workerIds = new HashSet<Guid>();
        foreach (var assignment in brain.PlacementExecution.Assignments.Values)
        {
            if (TryGetGuid(assignment.Assignment.WorkerNodeId, out var workerNodeId))
            {
                workerIds.Add(workerNodeId);
            }
        }

        return workerIds.OrderBy(static id => id).ToArray();
    }

    private Dictionary<Guid, int> BuildHostedBrainCounts(Guid excludedBrainId)
    {
        var counts = new Dictionary<Guid, int>();
        foreach (var brain in _brains.Values)
        {
            if (brain.BrainId == excludedBrainId)
            {
                continue;
            }

            foreach (var workerNodeId in GetCurrentPlacementWorkerNodeIds(brain))
            {
                counts[workerNodeId] = counts.TryGetValue(workerNodeId, out var current)
                    ? current + 1
                    : 1;
            }
        }

        return counts;
    }

    private static PlacementStateSnapshot CapturePlacementState(BrainState brain)
        => new(
            brain.PlacementEpoch,
            brain.PlacementRequestedMs,
            brain.PlacementUpdatedMs,
            brain.PlacementRequestId,
            brain.RequestedShardPlan?.Clone(),
            brain.PlannedPlacement?.Clone(),
            brain.PlacementExecution,
            brain.PlacementLifecycleState,
            brain.PlacementFailureReason,
            brain.PlacementReconcileState,
            brain.SpawnFailureReasonCode,
            brain.SpawnFailureMessage,
            brain.HomeostasisEnabled,
            brain.HomeostasisTargetMode,
            brain.HomeostasisUpdateMode,
            brain.HomeostasisBaseProbability,
            brain.HomeostasisMinStepCodes,
            brain.HomeostasisEnergyCouplingEnabled,
            brain.HomeostasisEnergyTargetScale,
            brain.HomeostasisEnergyProbabilityScale);

    private static void RestorePlacementState(BrainState brain, PlacementStateSnapshot snapshot)
    {
        brain.PlacementEpoch = snapshot.PlacementEpoch;
        brain.PlacementRequestedMs = snapshot.PlacementRequestedMs;
        brain.PlacementUpdatedMs = snapshot.PlacementUpdatedMs;
        brain.PlacementRequestId = snapshot.PlacementRequestId;
        brain.RequestedShardPlan = snapshot.RequestedShardPlan?.Clone();
        brain.PlannedPlacement = snapshot.PlannedPlacement?.Clone();
        brain.PlacementExecution = snapshot.PlacementExecution;
        brain.PlacementLifecycleState = snapshot.PlacementLifecycleState;
        brain.PlacementFailureReason = snapshot.PlacementFailureReason;
        brain.PlacementReconcileState = snapshot.PlacementReconcileState;
        brain.SpawnFailureReasonCode = snapshot.SpawnFailureReasonCode;
        brain.SpawnFailureMessage = snapshot.SpawnFailureMessage;
        RestorePlacementHomeostasis(brain, snapshot);
    }

    private static void RestorePlacementHomeostasis(BrainState brain, PlacementStateSnapshot snapshot)
    {
        brain.HomeostasisEnabled = snapshot.HomeostasisEnabled;
        brain.HomeostasisTargetMode = snapshot.HomeostasisTargetMode;
        brain.HomeostasisUpdateMode = snapshot.HomeostasisUpdateMode;
        brain.HomeostasisBaseProbability = snapshot.HomeostasisBaseProbability;
        brain.HomeostasisMinStepCodes = snapshot.HomeostasisMinStepCodes;
        brain.HomeostasisEnergyCouplingEnabled = snapshot.HomeostasisEnergyCouplingEnabled;
        brain.HomeostasisEnergyTargetScale = snapshot.HomeostasisEnergyTargetScale;
        brain.HomeostasisEnergyProbabilityScale = snapshot.HomeostasisEnergyProbabilityScale;
    }

    private bool TryResolvePlacementRegions(
        BrainState brain,
        out IReadOnlyList<PlacementPlanner.RegionSpan> regions,
        out int shardStride,
        out string? warningMessage,
        out string? failureMessage)
    {
        if (HasArtifactRef(brain.BaseDefinition)
            && TryReadPlacementHeader(brain.BaseDefinition!, out var header))
        {
            var fromHeader = new List<PlacementPlanner.RegionSpan>();
            for (var regionId = 0; regionId < header.Regions.Length; regionId++)
            {
                var neuronSpan = (int)header.Regions[regionId].NeuronSpan;
                if (neuronSpan <= 0)
                {
                    continue;
                }

                fromHeader.Add(new PlacementPlanner.RegionSpan(regionId, neuronSpan));
            }

            if (fromHeader.Count > 0)
            {
                regions = fromHeader;
                shardStride = (int)Math.Max(1u, header.AxonStride);
                warningMessage = null;
                failureMessage = null;
                return true;
            }
        }

        var fallback = new List<PlacementPlanner.RegionSpan>();
        if (brain.InputWidth > 0)
        {
            fallback.Add(new PlacementPlanner.RegionSpan(NbnConstants.InputRegionId, brain.InputWidth));
        }

        if (brain.OutputWidth > 0)
        {
            fallback.Add(new PlacementPlanner.RegionSpan(NbnConstants.OutputRegionId, brain.OutputWidth));
        }

        if (fallback.Count == 0)
        {
            regions = Array.Empty<PlacementPlanner.RegionSpan>();
            shardStride = NbnConstants.DefaultAxonStride;
            warningMessage = null;
            failureMessage = "Placement planning requires either resolvable base definition metadata or non-zero input/output widths.";
            return false;
        }

        regions = fallback;
        shardStride = NbnConstants.DefaultAxonStride;
        warningMessage = "Placement planner used fallback IO-only regions because base definition metadata was unavailable.";
        failureMessage = null;
        return true;
    }

    private static bool TryReadPlacementHeader(Nbn.Proto.ArtifactRef baseDefinition, out NbnHeaderV2 header)
    {
        header = default!;
        if (!baseDefinition.TryToSha256Bytes(out var baseHashBytes))
        {
            return false;
        }

        try
        {
            var store = CreateArtifactStoreResolver(baseDefinition.StoreUri).Resolve(baseDefinition.StoreUri);
            var stream = store.TryOpenArtifactAsync(new Sha256Hash(baseHashBytes))
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
            if (stream is null)
            {
                return false;
            }

            using (stream)
            {
                var headerBytes = new byte[NbnBinary.NbnHeaderBytes];
                var offset = 0;
                while (offset < headerBytes.Length)
                {
                    var read = stream.Read(headerBytes, offset, headerBytes.Length - offset);
                    if (read <= 0)
                    {
                        return false;
                    }

                    offset += read;
                }

                header = NbnBinary.ReadNbnHeader(headerBytes);
                return true;
            }
        }
        catch
        {
            return false;
        }
    }

    private static bool TryReadPlacementSnapshotHeader(Nbn.Proto.ArtifactRef snapshot, out NbsHeaderV2 header)
    {
        header = default!;
        if (!snapshot.TryToSha256Bytes(out var snapshotHashBytes))
        {
            return false;
        }

        try
        {
            var store = CreateArtifactStoreResolver(snapshot.StoreUri).Resolve(snapshot.StoreUri);
            var stream = store.TryOpenArtifactAsync(new Sha256Hash(snapshotHashBytes))
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
            if (stream is null)
            {
                return false;
            }

            using (stream)
            {
                var headerBytes = new byte[NbnBinary.NbsHeaderBytes];
                var offset = 0;
                while (offset < headerBytes.Length)
                {
                    var read = stream.Read(headerBytes, offset, headerBytes.Length - offset);
                    if (read <= 0)
                    {
                        return false;
                    }

                    offset += read;
                }

                header = NbnBinary.ReadNbsHeader(headerBytes);
                return true;
            }
        }
        catch
        {
            return false;
        }
    }

    private static void ApplySnapshotHomeostasisConfig(BrainState brain, NbsHomeostasisConfig config)
    {
        brain.HomeostasisEnabled = config.Enabled;
        brain.HomeostasisTargetMode = config.TargetMode;
        brain.HomeostasisUpdateMode = config.UpdateMode;
        brain.HomeostasisBaseProbability = config.BaseProbability;
        brain.HomeostasisMinStepCodes = config.MinStepCodes;
        brain.HomeostasisEnergyCouplingEnabled = config.EnergyCouplingEnabled;
        brain.HomeostasisEnergyTargetScale = config.EnergyTargetScale;
        brain.HomeostasisEnergyProbabilityScale = config.EnergyProbabilityScale;
    }

    private static bool IsValidSnapshotHomeostasisConfig(NbsHomeostasisConfig config)
    {
        return IsSupportedHomeostasisTargetMode(config.TargetMode)
               && config.UpdateMode == ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep
               && float.IsFinite(config.BaseProbability)
               && config.BaseProbability >= 0f
               && config.BaseProbability <= 1f
               && config.MinStepCodes != 0
               && IsFiniteInRange(config.EnergyTargetScale, 0f, 4f)
               && IsFiniteInRange(config.EnergyProbabilityScale, 0f, 4f);
    }

    private void UpdatePlacementLifecycle(
        BrainState brain,
        ProtoControl.PlacementLifecycleState state,
        ProtoControl.PlacementFailureReason failureReason)
    {
        brain.PlacementLifecycleState = state;
        brain.PlacementFailureReason = failureReason;
        brain.PlacementUpdatedMs = NowMs();
    }
}
