using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoIo = Nbn.Proto.Io;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private void RequestBrainRecovery(IContext context, Guid brainId, string trigger, string? detail = null)
    {
        if (!_brains.TryGetValue(brainId, out var brain) || brain.RecoveryInProgress)
        {
            return;
        }

        var normalizedTrigger = string.IsNullOrWhiteSpace(trigger) ? "recovery" : trigger.Trim();
        brain.RecoveryInProgress = true;
        brain.RecoveryReason = normalizedTrigger;
        brain.RecoveryStartedMs = NowMs();
        brain.RecoveryPlacementEpoch = 0;
        brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileRequiresAction;
        UpdatePlacementLifecycle(
            brain,
            ProtoControl.PlacementLifecycleState.PlacementLifecycleReconciling,
            ProtoControl.PlacementFailureReason.PlacementFailureNone);

        var notes = string.IsNullOrWhiteSpace(detail) ? normalizedTrigger : detail!.Trim();
        ReportBrainState(context, brain.BrainId, "Recovering", notes);
        HiveMindTelemetry.RecordRecoveryRequested(brain.BrainId, normalizedTrigger);
        EmitDebug(
            context,
            ProtoSeverity.SevWarn,
            "brain.recovering",
            $"Brain {brain.BrainId} entered recovery. trigger={normalizedTrigger} detail={notes}");

        if (!HasArtifactRef(brain.BaseDefinition))
        {
            FailBrainRecovery(
                context,
                brain,
                failureReason: "missing_base_definition",
                detail: "Recovery failed: base definition artifact is unavailable.");
            return;
        }

        RequestReschedule(context, $"recovery:{normalizedTrigger}", new[] { brainId });
    }

    private void CompleteBrainRecovery(IContext context, BrainState brain)
    {
        if (!brain.RecoveryInProgress)
        {
            return;
        }

        brain.RecoveryInProgress = false;
        brain.RecoveryReason = string.Empty;
        brain.RecoveryPlacementEpoch = 0;
        brain.RecoveryStartedMs = 0;

        HiveMindTelemetry.RecordRecoveryCompleted(brain.BrainId, brain.PlacementEpoch);
        var state = brain.Paused ? "Paused" : "Active";
        ReportBrainState(context, brain.BrainId, state, "recovery_complete");
        EmitDebug(
            context,
            ProtoSeverity.SevInfo,
            "brain.recovered",
            $"Brain {brain.BrainId} recovered at placement epoch={brain.PlacementEpoch}.");
    }

    private void FailBrainRecovery(IContext context, BrainState brain, string failureReason, string? detail = null)
    {
        if (!brain.RecoveryInProgress)
        {
            return;
        }

        var normalizedReason = string.IsNullOrWhiteSpace(failureReason) ? "recovery_failed" : failureReason.Trim();
        var failureMessage = string.IsNullOrWhiteSpace(detail) ? normalizedReason : detail!.Trim();
        brain.RecoveryInProgress = false;
        brain.RecoveryReason = string.Empty;
        brain.RecoveryPlacementEpoch = 0;
        brain.RecoveryStartedMs = 0;

        if (brain.PlacementFailureReason == ProtoControl.PlacementFailureReason.PlacementFailureNone)
        {
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
                ProtoControl.PlacementFailureReason.PlacementFailureInternalError);
        }

        SetSpawnFailureDetails(brain, normalizedReason, failureMessage);
        HiveMindTelemetry.RecordRecoveryFailed(brain.BrainId, normalizedReason);
        ReportBrainState(context, brain.BrainId, "Dead", failureMessage);
        EmitDebug(
            context,
            ProtoSeverity.SevWarn,
            "brain.recovery.failed",
            $"Brain {brain.BrainId} recovery failed. reason={normalizedReason} detail={failureMessage}");
        KillBrain(context, brain.BrainId, $"recovery_failed:{normalizedReason}");
    }

    private void RequestReschedule(IContext context, string reason)
        => RequestReschedule(context, reason, brainIds: null);

    private void RequestReschedule(IContext context, string reason, IReadOnlyCollection<Guid>? brainIds)
    {
        if (_rescheduleInProgress)
        {
            QueueReschedule(reason, brainIds);
            return;
        }

        var now = DateTime.UtcNow;
        var blockedByTickWindow = _lastRescheduleTick > 0
            && (_lastCompletedTickId - _lastRescheduleTick) < (ulong)_options.RescheduleMinTicks;
        var blockedByMinuteWindow = _lastRescheduleAt != default
            && now - _lastRescheduleAt < TimeSpan.FromMinutes(_options.RescheduleMinMinutes);
        if (blockedByTickWindow || blockedByMinuteWindow)
        {
            QueueReschedule(reason, brainIds);
            ScheduleQueuedRescheduleRetry(context, now, immediate: false);
            return;
        }

        _rescheduleQueued = false;
        _queuedRescheduleReason = null;
        _queuedRescheduleRetryScheduled = false;
        ClearQueuedRescheduleScope();
        SetActiveRescheduleScope(brainIds);
        _rescheduleInProgress = true;
        _lastRescheduleAt = now;
        _lastRescheduleTick = _lastCompletedTickId;

        ScheduleSelf(context, TimeSpan.FromMilliseconds(_options.RescheduleQuietMs), new RescheduleNow(reason));
    }

    private void QueueReschedule(string reason, IReadOnlyCollection<Guid>? brainIds)
    {
        _rescheduleQueued = true;
        _queuedRescheduleReason ??= reason;
        MergeQueuedRescheduleScope(brainIds);
    }

    private void SetActiveRescheduleScope(IReadOnlyCollection<Guid>? brainIds)
    {
        _activeRescheduleBrains.Clear();
        if (brainIds is null || brainIds.Count == 0)
        {
            _activeRescheduleAllBrains = true;
            return;
        }

        _activeRescheduleAllBrains = false;
        foreach (var brainId in brainIds)
        {
            _activeRescheduleBrains.Add(brainId);
        }
    }

    private void ClearActiveRescheduleScope()
    {
        _activeRescheduleAllBrains = true;
        _activeRescheduleBrains.Clear();
    }

    private void MergeQueuedRescheduleScope(IReadOnlyCollection<Guid>? brainIds)
    {
        if (brainIds is null || brainIds.Count == 0)
        {
            _queuedRescheduleAllBrains = true;
            _queuedRescheduleBrains.Clear();
            return;
        }

        if (_queuedRescheduleAllBrains)
        {
            return;
        }

        foreach (var brainId in brainIds)
        {
            _queuedRescheduleBrains.Add(brainId);
        }
    }

    private IReadOnlyCollection<Guid>? CaptureQueuedRescheduleScope()
    {
        if (_queuedRescheduleAllBrains || _queuedRescheduleBrains.Count == 0)
        {
            return null;
        }

        return _queuedRescheduleBrains.ToArray();
    }

    private void ClearQueuedRescheduleScope()
    {
        _queuedRescheduleAllBrains = false;
        _queuedRescheduleBrains.Clear();
    }

    private void BeginReschedule(IContext context, RescheduleNow message)
    {
        if (!_rescheduleInProgress || _pendingReschedule is not null)
        {
            return;
        }

        if (_phase != TickPhase.Idle || _tick is not null)
        {
            ScheduleSelf(context, TimeSpan.FromMilliseconds(Math.Max(50, _options.RescheduleQuietMs)), message);
            return;
        }

        if (_brains.Values.Any(static brain => brain.PlacementExecution is not null && !brain.PlacementExecution.Completed))
        {
            ScheduleSelf(context, TimeSpan.FromMilliseconds(Math.Max(50, _options.RescheduleQuietMs)), message);
            return;
        }

        Log($"Reschedule started: {message.Reason}");

        var peerLatencyTask = EnsurePeerLatencyRefreshTask(context, force: true);
        if (peerLatencyTask is not null)
        {
            context.ReenterAfter(
                peerLatencyTask,
                completed =>
                {
                    ApplyPeerLatencyRefreshResult(peerLatencyTask, completed);
                    if (!_rescheduleInProgress || _pendingReschedule is not null)
                    {
                        return;
                    }

                    BeginRescheduleWithFreshLatency(context, message.Reason);
                });
            return;
        }

        BeginRescheduleWithFreshLatency(context, message.Reason);
    }

    private void CompleteReschedule(IContext context, RescheduleCompleted message)
    {
        _pendingReschedule = null;
        _rescheduleInProgress = false;
        ClearActiveRescheduleScope();
        Log($"Reschedule completed: {message.Reason} (success={message.Success})");

        if (_rescheduleQueued)
        {
            ScheduleQueuedRescheduleRetry(context, DateTime.UtcNow, immediate: true);
            return;
        }

        if (_tickLoopEnabled && _phase == TickPhase.Idle)
        {
            ScheduleNextTick(context, TimeSpan.Zero);
        }
    }

    private void HandleRetryQueuedReschedule(IContext context)
    {
        _queuedRescheduleRetryScheduled = false;
        if (!_rescheduleQueued || _rescheduleInProgress)
        {
            return;
        }

        var queuedReason = _queuedRescheduleReason ?? "queued";
        var queuedBrainIds = CaptureQueuedRescheduleScope();
        RequestReschedule(context, queuedReason, queuedBrainIds);
    }

    private void ScheduleQueuedRescheduleRetry(IContext context, DateTime now, bool immediate)
    {
        if (!_rescheduleQueued || _rescheduleInProgress || _queuedRescheduleRetryScheduled)
        {
            return;
        }

        var retryDelay = immediate ? TimeSpan.Zero : ComputeQueuedRescheduleRetryDelay(now);
        _queuedRescheduleRetryScheduled = true;
        ScheduleSelf(context, retryDelay, new RetryQueuedReschedule());
    }

    private TimeSpan ComputeQueuedRescheduleRetryDelay(DateTime now)
    {
        var retryDelay = TimeSpan.FromMilliseconds(Math.Max(50, _options.RescheduleQuietMs));
        if (_lastRescheduleAt == default || _options.RescheduleMinMinutes <= 0)
        {
            return retryDelay;
        }

        var remaining = TimeSpan.FromMinutes(_options.RescheduleMinMinutes) - (now - _lastRescheduleAt);
        if (remaining <= TimeSpan.Zero)
        {
            return retryDelay;
        }

        return remaining > retryDelay ? remaining : retryDelay;
    }

    private void BeginRescheduleWithFreshLatency(IContext context, string reason)
    {
        var buildResult = BuildRescheduleBrainCandidates();
        if (buildResult.Candidates.Count == 0 && buildResult.Failures.Count == 0)
        {
            ScheduleSelf(context, TimeSpan.Zero, new RescheduleCompleted(reason, true));
            return;
        }

        var prepareTask = PrepareReschedulePlacementRequestsAsync(context.System, _ioPid, buildResult.Candidates, buildResult.Failures);
        context.ReenterAfter(
            prepareTask,
            completed => HandlePreparedRescheduleRequests(context, reason, completed));
    }

    private RescheduleCandidateBuildResult BuildRescheduleBrainCandidates()
    {
        var nowMs = NowMs();
        var eligibleWorkerCount = BuildPeerLatencyProbeTargets(nowMs).Count;
        var candidates = new List<RescheduleBrainCandidate>();
        var failures = new Dictionary<Guid, string>();
        var requireScopedInclusion = !_activeRescheduleAllBrains && _activeRescheduleBrains.Count > 0;

        foreach (var brain in _brains.Values.OrderBy(static value => value.BrainId))
        {
            if (requireScopedInclusion && !_activeRescheduleBrains.Contains(brain.BrainId))
            {
                continue;
            }

            var isRecovery = brain.RecoveryInProgress;
            var mustProduceCandidate = requireScopedInclusion || isRecovery;

            if (brain.PlacementEpoch == 0)
            {
                if (mustProduceCandidate)
                {
                    failures[brain.BrainId] = "brain_not_placed";
                }

                continue;
            }

            if (!HasArtifactRef(brain.BaseDefinition))
            {
                if (mustProduceCandidate)
                {
                    failures[brain.BrainId] = "missing_base_definition";
                }

                continue;
            }

            if (!TryResolvePlacementRegions(brain, out var regions, out _, out _, out var failureMessage))
            {
                if (mustProduceCandidate)
                {
                    failures[brain.BrainId] = string.IsNullOrWhiteSpace(failureMessage)
                        ? "placement_regions_unavailable"
                        : failureMessage;
                }

                continue;
            }

            if (!isRecovery && brain.Shards.Count == 0)
            {
                if (mustProduceCandidate)
                {
                    failures[brain.BrainId] = "no_registered_shards";
                }

                continue;
            }

            candidates.Add(new RescheduleBrainCandidate(
                brain.BrainId,
                brain.PlacementEpoch,
                brain.BaseDefinition!.Clone(),
                brain.LastSnapshot?.Clone(),
                brain.InputWidth,
                brain.OutputWidth,
                BuildRescheduleShardPlan(brain, regions, eligibleWorkerCount),
                brain.CostEnergyEnabled,
                brain.PlasticityEnabled,
                brain.HomeostasisEnabled,
                brain.HomeostasisTargetMode,
                brain.HomeostasisUpdateMode,
                brain.HomeostasisBaseProbability,
                brain.HomeostasisMinStepCodes,
                brain.HomeostasisEnergyCouplingEnabled,
                brain.HomeostasisEnergyTargetScale,
                brain.HomeostasisEnergyProbabilityScale,
                _lastCompletedTickId,
                new Dictionary<ShardId32, PID>(brain.Shards),
                isRecovery));
        }

        if (requireScopedInclusion)
        {
            foreach (var scopedBrainId in _activeRescheduleBrains)
            {
                if (_brains.ContainsKey(scopedBrainId) || failures.ContainsKey(scopedBrainId))
                {
                    continue;
                }

                failures[scopedBrainId] = "brain_not_tracked";
            }
        }

        return new RescheduleCandidateBuildResult(candidates, failures);
    }

    private static ProtoControl.ShardPlan? BuildRescheduleShardPlan(
        BrainState brain,
        IReadOnlyList<PlacementPlanner.RegionSpan> regions,
        int eligibleWorkerCount)
    {
        if (brain.RequestedShardPlan is not null)
        {
            return brain.RequestedShardPlan.Clone();
        }

        if (eligibleWorkerCount <= 1)
        {
            return null;
        }

        var totalComputeNeurons = regions
            .Where(static region => region.RegionId != NbnConstants.InputRegionId && region.RegionId != NbnConstants.OutputRegionId)
            .Sum(static region => Math.Max(1, region.NeuronSpan));
        if (totalComputeNeurons <= 0)
        {
            return null;
        }

        var desiredWorkers = Math.Clamp(
            (int)Math.Ceiling(totalComputeNeurons / 8192d),
            1,
            eligibleWorkerCount);
        if (desiredWorkers <= 1)
        {
            return null;
        }

        return new ProtoControl.ShardPlan
        {
            Mode = (ProtoControl.ShardPlanMode)1,
            ShardCount = (uint)desiredWorkers
        };
    }

    private static async Task<ReschedulePreparationResult> PrepareReschedulePlacementRequestsAsync(
        ActorSystem system,
        PID? ioPid,
        IReadOnlyList<RescheduleBrainCandidate> candidates,
        IReadOnlyDictionary<Guid, string>? initialFailures = null)
    {
        var requests = new List<ReschedulePlacementRequest>(candidates.Count);
        var failures = initialFailures is null
            ? new Dictionary<Guid, string>()
            : new Dictionary<Guid, string>(initialFailures);

        foreach (var candidate in candidates)
        {
            try
            {
                Nbn.Proto.ArtifactRef? snapshot;
                if (candidate.IsRecovery)
                {
                    snapshot = candidate.LastSnapshot?.Clone();
                }
                else
                {
                    var energyRemaining = await TryReadBrainEnergyRemainingAsync(system, ioPid, candidate.BrainId).ConfigureAwait(false);
                    var storeRootPath = ResolveArtifactRoot(candidate.BaseDefinition.StoreUri);
                    var storeUri = string.IsNullOrWhiteSpace(candidate.BaseDefinition.StoreUri)
                        ? storeRootPath
                        : candidate.BaseDefinition.StoreUri;
                    snapshot = await BuildAndStoreSnapshotAsync(
                            system,
                            new SnapshotBuildRequest(
                                candidate.BrainId,
                                candidate.BaseDefinition,
                                candidate.CurrentTickId,
                                energyRemaining,
                                candidate.CostEnergyEnabled,
                                candidate.CostEnergyEnabled,
                                candidate.PlasticityEnabled,
                                candidate.HomeostasisEnabled,
                                candidate.HomeostasisTargetMode,
                                candidate.HomeostasisUpdateMode,
                                candidate.HomeostasisBaseProbability,
                                candidate.HomeostasisMinStepCodes,
                                candidate.HomeostasisEnergyCouplingEnabled,
                                candidate.HomeostasisEnergyTargetScale,
                                candidate.HomeostasisEnergyProbabilityScale,
                                new Dictionary<ShardId32, PID>(candidate.Shards),
                                storeRootPath,
                                storeUri))
                        .ConfigureAwait(false);
                }

                requests.Add(new ReschedulePlacementRequest(
                    candidate.BrainId,
                    candidate.CurrentPlacementEpoch,
                    new ProtoControl.RequestPlacement
                    {
                        BrainId = candidate.BrainId.ToProtoUuid(),
                        BaseDef = candidate.BaseDefinition.Clone(),
                        LastSnapshot = snapshot?.Clone(),
                        ShardPlan = candidate.ShardPlan?.Clone(),
                        InputWidth = (uint)Math.Max(0, candidate.InputWidth),
                        OutputWidth = (uint)Math.Max(0, candidate.OutputWidth),
                        RequestId = candidate.IsRecovery
                            ? $"recovery:{candidate.BrainId:N}:{candidate.CurrentTickId}"
                            : $"reschedule:{candidate.BrainId:N}:{candidate.CurrentTickId}",
                        RequestedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        IsRecovery = candidate.IsRecovery
                    }));
            }
            catch (Exception ex)
            {
                failures[candidate.BrainId] = ex.GetBaseException().Message;
            }
        }

        return new ReschedulePreparationResult(requests, failures);
    }

    private static async Task<long> TryReadBrainEnergyRemainingAsync(ActorSystem system, PID? ioPid, Guid brainId)
    {
        if (ioPid is null)
        {
            return 0L;
        }

        try
        {
            var info = await system.Root.RequestAsync<ProtoIo.BrainInfo>(
                    ioPid,
                    new ProtoIo.BrainInfoRequest
                    {
                        BrainId = brainId.ToProtoUuid()
                    },
                    TimeSpan.FromMilliseconds(500))
                .ConfigureAwait(false);
            return info?.EnergyRemaining ?? 0L;
        }
        catch
        {
            return 0L;
        }
    }

    private void HandlePreparedRescheduleRequests(
        IContext context,
        string reason,
        Task<ReschedulePreparationResult> completed)
    {
        if (!_rescheduleInProgress || _pendingReschedule is not null)
        {
            return;
        }

        if (!completed.IsCompletedSuccessfully)
        {
            ScheduleSelf(context, TimeSpan.Zero, new RescheduleCompleted(reason, false));
            return;
        }

        var prepared = completed.Result;
        _pendingReschedule = new PendingRescheduleState(reason);
        foreach (var failure in prepared.Failures)
        {
            _pendingReschedule.Failures[failure.Key] = failure.Value;
            if (_brains.TryGetValue(failure.Key, out var failedBrain) && failedBrain.RecoveryInProgress)
            {
                FailBrainRecovery(
                    context,
                    failedBrain,
                    failureReason: "recovery_prepare_failed",
                    detail: failure.Value);
            }
            else
            {
                PauseBrainAfterRescheduleFailure(context, failure.Key, failure.Value);
            }
        }

        foreach (var request in prepared.Requests)
        {
            var ack = ProcessPlacementRequest(context, request.Request);
            if (!_brains.TryGetValue(request.BrainId, out var brain))
            {
                _pendingReschedule.Failures[request.BrainId] = "brain_not_tracked";
                continue;
            }

            if (!ack.Accepted)
            {
                _pendingReschedule.Failures[request.BrainId] = string.IsNullOrWhiteSpace(ack.Message)
                    ? "placement_request_rejected"
                    : ack.Message;
                if (brain.RecoveryInProgress)
                {
                    FailBrainRecovery(
                        context,
                        brain,
                        failureReason: "placement_request_rejected",
                        detail: ack.Message);
                }
                else
                {
                    PauseBrainAfterRescheduleFailure(context, brain.BrainId, _pendingReschedule.Failures[request.BrainId]);
                }

                continue;
            }

            if (brain.RecoveryInProgress)
            {
                brain.RecoveryPlacementEpoch = ack.PlacementEpoch;
            }

            _pendingReschedule.PendingBrains[request.BrainId] = ack.PlacementEpoch;
            TryCompletePendingReschedule(context, brain);
        }

        if (_pendingReschedule.PendingBrains.Count == 0)
        {
            ScheduleSelf(
                context,
                TimeSpan.Zero,
                new RescheduleCompleted(reason, _pendingReschedule.Failures.Count == 0));
        }
    }

    private void TryCompletePendingReschedule(IContext context, BrainState brain)
    {
        if (_pendingReschedule is null
            || !_pendingReschedule.PendingBrains.TryGetValue(brain.BrainId, out var expectedEpoch))
        {
            return;
        }

        var completed = false;
        var succeeded = false;
        string? failureReason = null;
        if (brain.PlacementEpoch != expectedEpoch)
        {
            _pendingReschedule.PendingBrains.Remove(brain.BrainId);
            failureReason = "placement_epoch_changed";
            _pendingReschedule.Failures[brain.BrainId] = failureReason;
            completed = true;
        }
        else if (brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed
                 || brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleTerminated)
        {
            _pendingReschedule.PendingBrains.Remove(brain.BrainId);
            failureReason = ToFailureReasonLabel(brain.PlacementFailureReason);
            _pendingReschedule.Failures[brain.BrainId] = failureReason;
            completed = true;
        }
        else if (brain.PlacementExecution is not null
                 && brain.PlacementExecution.PlacementEpoch == expectedEpoch
                 && !brain.PlacementExecution.Completed)
        {
            return;
        }
        else if (brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned
                 || brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning)
        {
            if (brain.PlacementExecution is not null
                && brain.PlacementExecution.PlacementEpoch == expectedEpoch
                && !AreObservedShardAssignmentsRegistered(brain, brain.PlacementExecution))
            {
                return;
            }

            _pendingReschedule.PendingBrains.Remove(brain.BrainId);
            completed = true;
            succeeded = true;
        }
        else
        {
            return;
        }

        if (completed && brain.RecoveryInProgress && (brain.RecoveryPlacementEpoch == 0 || brain.RecoveryPlacementEpoch == expectedEpoch))
        {
            if (succeeded)
            {
                CompleteBrainRecovery(context, brain);
            }
            else
            {
                FailBrainRecovery(context, brain, failureReason ?? "recovery_failed");
            }
        }
        else if (completed && !succeeded)
        {
            if (!string.Equals(failureReason, "placement_epoch_changed", StringComparison.Ordinal))
            {
                PauseBrainAfterRescheduleFailure(context, brain.BrainId, failureReason ?? "reschedule_failed");
            }
        }

        if (_pendingReschedule.PendingBrains.Count == 0)
        {
            ScheduleSelf(
                context,
                TimeSpan.Zero,
                new RescheduleCompleted(
                    _pendingReschedule.Reason,
                    _pendingReschedule.Failures.Count == 0));
        }
    }

    private void PauseBrainAfterRescheduleFailure(IContext context, Guid brainId, string? failureReason)
    {
        if (!_brains.TryGetValue(brainId, out var brain) || brain.RecoveryInProgress)
        {
            return;
        }

        var normalizedReason = string.IsNullOrWhiteSpace(failureReason)
            ? "reschedule_failed"
            : failureReason.Trim();
        if (brain.PlacementFailureReason == ProtoControl.PlacementFailureReason.PlacementFailureNone)
        {
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
                ProtoControl.PlacementFailureReason.PlacementFailureInternalError);
        }
        else
        {
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
                brain.PlacementFailureReason);
        }

        brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileFailed;
        SetSpawnFailureDetails(
            brain,
            "reschedule_failed",
            $"Reschedule failed: {normalizedReason}");
        PauseBrain(context, brainId, $"reschedule_failed:{normalizedReason}");
    }

    private static bool AreObservedShardAssignmentsRegistered(BrainState brain, PlacementExecutionState execution)
    {
        foreach (var observed in execution.ObservedAssignments.Values)
        {
            if (observed.Target != ProtoControl.PlacementAssignmentTarget.PlacementTargetRegionShard)
            {
                continue;
            }

            if (!ShardId32.TryFrom((int)observed.RegionId, (int)observed.ShardIndex, out var shardId)
                || !brain.Shards.ContainsKey(shardId)
                || !brain.ShardRegistrationEpochs.TryGetValue(shardId, out var registrationEpoch)
                || registrationEpoch != execution.PlacementEpoch)
            {
                return false;
            }
        }

        return true;
    }
}
