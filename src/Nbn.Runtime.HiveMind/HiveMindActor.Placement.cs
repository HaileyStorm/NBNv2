using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private bool HandlePlacementMessage(IContext context)
    {
        switch (context.Message)
        {
            case ProtoControl.SpawnBrain message:
                HandleSpawnBrain(context, message);
                return true;
            case ProtoControl.AwaitSpawnPlacement message:
                HandleAwaitSpawnPlacement(context, message);
                return true;
            case ProtoControl.RequestPlacement message:
                HandleRequestPlacement(context, message);
                return true;
            case ProtoControl.GetPlacementLifecycle message:
                if (message.BrainId is not null && message.BrainId.TryToGuid(out var placementBrainId))
                {
                    context.Respond(BuildPlacementLifecycleInfo(placementBrainId));
                }
                else
                {
                    context.Respond(new ProtoControl.PlacementLifecycleInfo());
                }

                return true;
            case ProtoControl.PlacementWorkerInventoryRequest:
                context.Respond(BuildPlacementWorkerInventory());
                return true;
            case ProtoControl.PlacementAssignmentAck message:
                HandlePlacementAssignmentAck(context, message);
                return true;
            case ProtoControl.PlacementUnassignmentAck message:
                HandlePlacementUnassignmentAck(context, message);
                return true;
            case ProtoControl.PlacementReconcileReport message:
                HandlePlacementReconcileReport(context, message);
                return true;
            case DispatchPlacementPlan message:
                HandleDispatchPlacementPlan(context, message);
                return true;
            case RetryPlacementAssignment message:
                HandleRetryPlacementAssignment(context, message);
                return true;
            case PlacementAssignmentTimeout message:
                HandlePlacementAssignmentTimeout(context, message);
                return true;
            case PlacementReconcileTimeout message:
                HandlePlacementReconcileTimeout(context, message);
                return true;
            case SpawnCompletionTimeout message:
                HandleSpawnCompletionTimeout(context, message);
                return true;
            default:
                return false;
        }
    }

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
                    InputWidth = message.InputWidth,
                    OutputWidth = message.OutputWidth,
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

            var spawnCompletionTimeoutMs = ComputeSpawnCompletionTimeoutMs(brain);
            var pending = new PendingSpawnState(brain.BrainId, brain.PlacementEpoch, spawnCompletionTimeoutMs);
            _pendingSpawns[brain.BrainId] = pending;

            ScheduleSelf(
                context,
                TimeSpan.FromMilliseconds(spawnCompletionTimeoutMs),
                new SpawnCompletionTimeout(brain.BrainId, brain.PlacementEpoch));

            context.Respond(BuildSpawnQueuedAck(brain));
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

    private void HandleAwaitSpawnPlacement(IContext context, ProtoControl.AwaitSpawnPlacement message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            context.Respond(BuildSpawnFailureAck(
                reasonCode: "spawn_invalid_request",
                failureMessage: "Spawn wait rejected: brain_id is required."));
            return;
        }

        if (_pendingSpawns.TryGetValue(brainId, out var pending))
        {
            var timeoutMs = NormalizeAwaitSpawnTimeoutMs(message.TimeoutMs, pending);
            context.ReenterAfter(
                AwaitPendingSpawnAsync(pending, timeoutMs),
                task =>
                {
                    if (task.IsCompletedSuccessfully)
                    {
                        context.Respond(BuildAwaitedSpawnAck(brainId, pending, task.Result));
                    }
                    else
                    {
                        var detail = task.Exception?.GetBaseException().Message ?? "unknown_error";
                        context.Respond(BuildSpawnFailureAck(
                            brainId,
                            pending.PlacementEpoch,
                            acceptedForPlacement: true,
                            lifecycleState: ProtoControl.PlacementLifecycleState.PlacementLifecycleUnknown,
                            reconcileState: ProtoControl.PlacementReconcileState.PlacementReconcileUnknown,
                            reasonCode: "spawn_internal_error",
                            failureMessage: $"Spawn wait failed: internal error while awaiting placement ({detail})."));
                    }

                    return Task.CompletedTask;
                });
            return;
        }

        context.Respond(BuildCurrentSpawnAck(brainId));
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
            brain = new BrainState(brainId, _outputVectorSource)
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
                var ignoredMessage =
                    $"Ignored assignment ack for brain {brainId}; assignment={trackedAssignment.Assignment.AssignmentId} reason=sender_worker_mismatch sender={senderLabel} plannedWorker={plannedWorkerId:D} plannedPid={PidLabel(plannedWorkerPid)} target={ToPlacementTargetLabel(trackedAssignment.Assignment.Target)} attempt={trackedAssignment.Attempt}.";
                EmitDebug(
                    context,
                    ProtoSeverity.SevWarn,
                    "placement.assignment_ack.ignored",
                    ignoredMessage);
                LogError(ignoredMessage);
                return;
            }

            var ackObservedMs = NowMs();
            var target = ToPlacementTargetLabel(trackedAssignment.Assignment.Target);
            var plannedWorkerNodeId = plannedWorkerId;
            var ackFailureReason = message.FailureReason == ProtoControl.PlacementFailureReason.PlacementFailureNone
                ? (!message.Accepted || message.State == ProtoControl.PlacementAssignmentState.PlacementAssignmentFailed
                    ? ToFailureReasonLabel(ProtoControl.PlacementFailureReason.PlacementFailureAssignmentRejected)
                    : "none")
                : ToFailureReasonLabel(message.FailureReason);
            var ackLatencyMs = trackedAssignment.LastDispatchMs > 0
                ? ackObservedMs - trackedAssignment.LastDispatchMs
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
                trackedAssignment.AwaitingAck = false;
                trackedAssignment.AcceptedMs = ackObservedMs;
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

                ReleaseWorkerPlacementDispatch(
                    context,
                    plannedWorkerNodeId,
                    brain.BrainId,
                    brain.PlacementEpoch,
                    trackedAssignment.Assignment.AssignmentId);

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
                    if (trackedAssignment.AcceptedMs == 0)
                    {
                        trackedAssignment.AcceptedMs = ackObservedMs;
                    }

                    UpdatePlacementLifecycle(
                        brain,
                        ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigning,
                        ProtoControl.PlacementFailureReason.PlacementFailureNone);
                    trackedAssignment.Accepted = true;
                    break;
                case ProtoControl.PlacementAssignmentState.PlacementAssignmentReady:
                    trackedAssignment.AwaitingAck = false;
                    if (trackedAssignment.AcceptedMs == 0)
                    {
                        trackedAssignment.AcceptedMs = ackObservedMs;
                    }

                    trackedAssignment.Accepted = true;
                    trackedAssignment.Ready = true;
                    trackedAssignment.ReadyMs = ackObservedMs;
                    ReleaseWorkerPlacementDispatch(
                        context,
                        plannedWorkerNodeId,
                        brain.BrainId,
                        brain.PlacementEpoch,
                        trackedAssignment.Assignment.AssignmentId);
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
            if (brain.PlacementExecution is not null)
            {
                brain.PlacementExecution.Completed = true;
            }
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
                if (brain.PlacementExecution is not null)
                {
                    brain.PlacementExecution.Completed = true;
                }
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
                if (brain.PlacementExecution is not null)
                {
                    brain.PlacementExecution.Completed = true;
                }
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
}
