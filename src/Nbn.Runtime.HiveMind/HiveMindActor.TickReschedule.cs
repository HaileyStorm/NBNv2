using System.Text;
using Nbn.Proto.Viz;
using Nbn.Runtime.IO;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoIo = Nbn.Proto.Io;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private void PauseBrain(IContext context, Guid brainId, string? reason)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return;
        }

        brain.Paused = true;
        brain.PausedReason = reason;

        if (_phase == TickPhase.Compute)
        {
            RemovePendingComputeForBrain(brainId);
            MaybeCompleteCompute(context);
        }

        if (_phase == TickPhase.Deliver && RemovePendingDeliver(brainId))
        {
            MaybeCompleteDeliver(context);
        }

        ReportBrainState(context, brainId, "Paused", reason);
        EmitVizEvent(context, VizEventType.VizBrainPaused, brainId: brainId);
        EmitDebug(context, ProtoSeverity.SevInfo, "brain.paused", $"Brain {brainId} paused. reason={reason ?? "none"}");
    }

    private void ResumeBrain(IContext context, Guid brainId)
    {
        if (_brains.TryGetValue(brainId, out var brain))
        {
            brain.Paused = false;
            brain.PausedReason = null;
            ReportBrainState(context, brainId, "Active", null);
            EmitVizEvent(context, VizEventType.VizBrainActive, brainId: brainId);
            EmitDebug(context, ProtoSeverity.SevInfo, "brain.resumed", $"Brain {brainId} resumed.");
        }
    }

    private void StartTick(IContext context)
    {
        _tick = new TickState(_lastCompletedTickId + 1, DateTime.UtcNow);
        EmitTickVisualizationEvents(context, _tick.TickId);
        _phase = TickPhase.Compute;
        ClearPendingCompute();
        ClearPendingDeliver();

        _tick.ComputeStartedUtc = _tick.StartedUtc;

        foreach (var brain in _brains.Values)
        {
            if (!CanDispatchTickToBrain(brain))
            {
                continue;
            }

            if (brain.RoutingSnapshot.Count == 0)
            {
                LogError($"Routing snapshot missing for brain {brain.BrainId} with {brain.Shards.Count} shard(s).");
            }

            var computeTarget = brain.BrainRootPid ?? brain.SignalRouterPid;
            if (computeTarget is null)
            {
                LogError($"TickCompute skipped: missing BrainRoot/SignalRouter PID for brain {brain.BrainId}.");
                continue;
            }

            foreach (var (shardId, senderPid) in brain.Shards)
            {
                var key = new ShardKey(brain.BrainId, shardId);
                _pendingCompute.Add(key);
                _pendingComputeSenders[key] = senderPid;
            }

            if (LogTickBarrier)
            {
                Log(
                    $"TickCompute dispatch tick={_tick.TickId} brain={brain.BrainId} target={PidLabel(computeTarget)} shards={brain.Shards.Count} pendingCompute={_pendingCompute.Count}");
            }

            context.Send(
                computeTarget,
                new ProtoControl.TickCompute
                {
                    TickId = _tick.TickId,
                    TargetTickHz = _backpressure.TargetTickHz
                });
        }

        _tick.ExpectedComputeCount = _pendingCompute.Count;

        if (_pendingCompute.Count == 0)
        {
            CompleteComputePhase(context);
            return;
        }

        SchedulePhaseTimeout(context, TickPhase.Compute, _tick.TickId, _options.ComputeTimeoutMs);
    }

    private void EmitTickVisualizationEvents(IContext context, ulong tickId)
    {
        if (!ShouldEmitVizTick(tickId))
        {
            return;
        }

        _lastVizTickEmittedTickId = tickId;
        EmitVizEvent(context, VizEventType.VizTick, tickId: tickId);
    }

    private bool ShouldEmitVizTick(ulong tickId)
    {
        if (tickId == 0)
        {
            return false;
        }

        var stride = ComputeVizStride(_backpressure.TargetTickHz, _vizTickMinIntervalMs);
        if (stride <= 1)
        {
            return true;
        }

        if (_lastVizTickEmittedTickId == 0)
        {
            return true;
        }

        return tickId - _lastVizTickEmittedTickId >= stride;
    }

    private void HandleTickComputeDone(IContext context, ProtoControl.TickComputeDone message)
    {
        if (_tick is null)
        {
            if (message.TickId <= _lastCompletedTickId)
            {
                HiveMindTelemetry.RecordLateComputeAfterCompletion();
            }
            return;
        }

        if (message.TickId != _tick.TickId || _phase != TickPhase.Compute)
        {
            if (message.TickId <= _tick.TickId)
            {
                _tick.LateComputeCount++;
            }
            return;
        }

        if (!message.BrainId.TryToGuid(out var brainId) || message.ShardId is null)
        {
            EmitTickComputeDoneIgnored(context, message, "invalid_payload");
            return;
        }

        var shardId = message.ShardId.ToShardId32();
        var key = new ShardKey(brainId, shardId);

        if (LogTickBarrier)
        {
            var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
            Log($"TickComputeDone recv tick={message.TickId} brain={brainId} shard={shardId} sender={senderLabel}");
        }

        if (!_pendingComputeSenders.TryGetValue(key, out var expectedSender))
        {
            EmitTickComputeDoneIgnored(context, message, "untracked_payload");
            return;
        }

        var senderMatchesExpected = SenderMatchesPid(context.Sender, expectedSender);
        var senderMatchesTrustedController = !senderMatchesExpected
            && _brains.TryGetValue(brainId, out var brain)
            && IsTrustedControllerSender(context.Sender, brain);
        if (!senderMatchesExpected && !senderMatchesTrustedController)
        {
            EmitTickComputeDoneIgnored(context, message, "sender_mismatch", expectedSender);
            return;
        }

        if (!RemovePendingCompute(key))
        {
            EmitTickComputeDoneIgnored(context, message, "untracked_payload", expectedSender);
            return;
        }

        if (LogVizDiagnostics)
        {
            var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
            Log(
                $"VizDiag TickComputeDone accepted tick={message.TickId} brain={brainId} shard={shardId} sender={senderLabel} expectedSender={PidLabel(expectedSender)} pendingAfter={_pendingCompute.Count}");
        }

        if (message.TickCostTotal != 0)
        {
            var updated = message.TickCostTotal;
            if (_tick.BrainTickCosts.TryGetValue(brainId, out var existing))
            {
                updated += existing;
            }

            _tick.BrainTickCosts[brainId] = updated;
        }

        _tick.CompletedComputeCount++;
        MaybeCompleteCompute(context);
    }

    private void HandleTickDeliverDone(IContext context, ProtoControl.TickDeliverDone message)
    {
        if (_tick is null)
        {
            if (message.TickId <= _lastCompletedTickId)
            {
                HiveMindTelemetry.RecordLateDeliverAfterCompletion();
            }
            return;
        }

        if (message.TickId != _tick.TickId || _phase != TickPhase.Deliver)
        {
            if (message.TickId <= _tick.TickId)
            {
                _tick.LateDeliverCount++;
            }
            return;
        }

        if (!message.BrainId.TryToGuid(out var brainId))
        {
            EmitTickDeliverDoneIgnored(context, message, "invalid_payload");
            return;
        }

        if (LogTickBarrier)
        {
            var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
            Log($"TickDeliverDone recv tick={message.TickId} brain={brainId} sender={senderLabel}");
        }

        if (!_pendingDeliverSenders.TryGetValue(brainId, out var expectedSender))
        {
            EmitTickDeliverDoneIgnored(context, message, "untracked_payload");
            return;
        }

        var senderMatchesExpected = SenderMatchesPid(context.Sender, expectedSender);
        var senderMatchesTrustedController = !senderMatchesExpected
            && _brains.TryGetValue(brainId, out var brain)
            && IsTrustedControllerSender(context.Sender, brain);
        if (!senderMatchesExpected && !senderMatchesTrustedController)
        {
            EmitTickDeliverDoneIgnored(context, message, "sender_mismatch", expectedSender);
            return;
        }

        if (!RemovePendingDeliver(brainId))
        {
            EmitTickDeliverDoneIgnored(context, message, "untracked_payload", expectedSender);
            return;
        }

        if (LogVizDiagnostics)
        {
            var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
            Log(
                $"VizDiag TickDeliverDone accepted tick={message.TickId} brain={brainId} sender={senderLabel} expectedSender={PidLabel(expectedSender)} pendingAfter={_pendingDeliver.Count}");
        }

        _tick.CompletedDeliverCount++;
        ReportBrainTick(context, brainId, message.TickId);
        MaybeCompleteDeliver(context);
    }

    private void HandleTickPhaseTimeout(IContext context, TickPhaseTimeout message)
    {
        if (_tick is null || message.TickId != _tick.TickId || _phase != message.Phase)
        {
            return;
        }

        switch (message.Phase)
        {
            case TickPhase.Compute:
                _tick.ComputeTimedOut = true;
                if (_pendingCompute.Count > 0)
                {
                    LogError($"TickCompute timeout: tick {_tick.TickId} pending={_pendingCompute.Count}");
                    EmitDebug(
                        context,
                        ProtoSeverity.SevError,
                        "tick.compute.timeout",
                        $"Tick {_tick.TickId} compute timeout pending={_pendingCompute.Count}");
                    if (LogVizDiagnostics)
                    {
                        LogError($"TickCompute timeout detail: {DescribePendingCompute()}");
                    }
                }
                ClearPendingCompute();
                CompleteComputePhase(context);
                break;
            case TickPhase.Deliver:
                _tick.DeliverTimedOut = true;
                if (_pendingDeliver.Count > 0)
                {
                    var pendingBrains = string.Join(",", _pendingDeliver);
                    LogError($"TickDeliver timeout: tick {_tick.TickId} pendingBrains={pendingBrains}");
                    EmitDebug(
                        context,
                        ProtoSeverity.SevError,
                        "tick.deliver.timeout",
                        $"Tick {_tick.TickId} deliver timeout pendingBrains={pendingBrains}");
                    if (LogVizDiagnostics)
                    {
                        LogError($"TickDeliver timeout detail: {DescribePendingDeliver()}");
                    }
                }
                ClearPendingDeliver();
                CompleteTick(context);
                break;
        }
    }

    private void MaybeCompleteCompute(IContext context)
    {
        if (_pendingCompute.Count == 0)
        {
            CompleteComputePhase(context);
        }
    }

    private void CompleteComputePhase(IContext context)
    {
        if (_tick is null || _phase != TickPhase.Compute)
        {
            return;
        }

        _tick.ComputeCompletedUtc = DateTime.UtcNow;
        _phase = TickPhase.Deliver;
        _tick.DeliverStartedUtc = DateTime.UtcNow;

        foreach (var brain in _brains.Values)
        {
            if (!CanDispatchTickToBrain(brain))
            {
                continue;
            }

            if (brain.RoutingSnapshot.Count == 0)
            {
                LogError($"Routing snapshot missing for brain {brain.BrainId} with {brain.Shards.Count} shard(s).");
            }

            var deliverTarget = brain.BrainRootPid ?? brain.SignalRouterPid;
            if (deliverTarget is null)
            {
                LogError($"TickDeliver skipped: missing BrainRoot/SignalRouter PID for brain {brain.BrainId}.");
                continue;
            }
            _pendingDeliver.Add(brain.BrainId);
            _pendingDeliverSenders[brain.BrainId] = deliverTarget;
            context.Request(deliverTarget, new ProtoControl.TickDeliver { TickId = _tick.TickId });
        }

        _tick.ExpectedDeliverCount = _pendingDeliver.Count;

        if (_pendingDeliver.Count == 0)
        {
            CompleteTick(context);
            return;
        }

        SchedulePhaseTimeout(context, TickPhase.Deliver, _tick.TickId, _options.DeliverTimeoutMs);
    }

    private void MaybeCompleteDeliver(IContext context)
    {
        if (_pendingDeliver.Count == 0)
        {
            CompleteTick(context);
        }
    }

    private void CompleteTick(IContext context)
    {
        if (_tick is null)
        {
            _phase = TickPhase.Idle;
            return;
        }

        _tick.DeliverCompletedUtc = DateTime.UtcNow;
        _phase = TickPhase.Idle;

        var outcome = new TickOutcome(
            _tick.TickId,
            SafeDuration(_tick.ComputeStartedUtc, _tick.ComputeCompletedUtc),
            SafeDuration(_tick.DeliverStartedUtc, _tick.DeliverCompletedUtc),
            _tick.ComputeTimedOut,
            _tick.DeliverTimedOut,
            _tick.LateComputeCount,
            _tick.LateDeliverCount,
            _tick.ExpectedComputeCount,
            _tick.CompletedComputeCount,
            _tick.ExpectedDeliverCount,
            _tick.CompletedDeliverCount);

        var elapsed = DateTime.UtcNow - _tick.StartedUtc;
        var completedTickId = _tick.TickId;
        var tickCosts = _tick.BrainTickCosts;
        _tick = null;
        _lastCompletedTickId = completedTickId;

        HiveMindTelemetry.RecordTickOutcome(outcome, _backpressure.TargetTickHz);
        ApplyTickCosts(context, completedTickId, tickCosts);

        var decision = _backpressure.Evaluate(outcome);

        if (decision.RequestReschedule)
        {
            RequestReschedule(context, decision.Reason);
            HiveMindTelemetry.RecordReschedule(decision.Reason);
        }

        if (decision.RequestPause)
        {
            var nextTickDelay = ComputeTickDelay(elapsed, decision.TargetTickHz);
            if (TryApplyBackpressurePause(context, decision.Reason, nextTickDelay))
            {
                return;
            }
        }

        ScheduleNextTick(context, ComputeTickDelay(elapsed, decision.TargetTickHz));
    }

    private void ApplyTickCosts(IContext context, ulong tickId, Dictionary<Guid, long> costs)
    {
        if (_ioPid is null || costs.Count == 0)
        {
            return;
        }

        var ioPid = ResolveSendTargetPid(context, _ioPid);
        foreach (var entry in costs)
        {
            var cost = entry.Value;
            if (cost == 0)
            {
                continue;
            }

            if (_brains.TryGetValue(entry.Key, out var brain))
            {
                brain.LastTickCost = cost;
            }

            HiveMindTelemetry.RecordBrainTickCost(entry.Key, cost);
            context.Send(ioPid, new ApplyTickCost(entry.Key, tickId, cost));
        }
    }

    private void ScheduleNextTick(IContext context, TimeSpan delay)
    {
        if (!_tickLoopEnabled || _rescheduleInProgress)
        {
            return;
        }

        ScheduleSelf(context, delay, new TickStart());
    }

    private static bool CanDispatchTickToBrain(BrainState brain)
    {
        // Placement reconciliation metadata can lag behind already-hosted shards.
        // Tick dispatch should follow runnable state (paused + shard availability).
        return !brain.Paused && brain.Shards.Count > 0;
    }

    private void SchedulePhaseTimeout(IContext context, TickPhase phase, ulong tickId, int timeoutMs)
    {
        if (timeoutMs <= 0)
        {
            return;
        }

        ScheduleSelf(context, TimeSpan.FromMilliseconds(timeoutMs), new TickPhaseTimeout(tickId, phase));
    }

    private bool RemovePendingCompute(ShardKey key)
    {
        _pendingComputeSenders.Remove(key);
        return _pendingCompute.Remove(key);
    }

    private bool RemovePendingDeliver(Guid brainId)
    {
        _pendingDeliverSenders.Remove(brainId);
        return _pendingDeliver.Remove(brainId);
    }

    private void ClearPendingCompute()
    {
        _pendingCompute.Clear();
        _pendingComputeSenders.Clear();
    }

    private void ClearPendingDeliver()
    {
        _pendingDeliver.Clear();
        _pendingDeliverSenders.Clear();
    }

    private void RemovePendingComputeForBrain(Guid brainId)
    {
        if (_pendingCompute.Count == 0)
        {
            return;
        }

        var removeKeys = new List<ShardKey>();
        foreach (var key in _pendingCompute)
        {
            if (key.BrainId == brainId)
            {
                removeKeys.Add(key);
            }
        }

        if (_tick is null)
        {
            foreach (var key in removeKeys)
            {
                RemovePendingCompute(key);
            }

            return;
        }

        foreach (var key in removeKeys)
        {
            if (RemovePendingCompute(key))
            {
                _tick.ExpectedComputeCount = Math.Max(_tick.CompletedComputeCount, _tick.ExpectedComputeCount - 1);
            }
        }
    }

    private bool TryApplyBackpressurePause(IContext context, string reason, TimeSpan nextTickDelay)
    {
        var candidates = BuildBackpressurePauseCandidates();
        if (candidates.Count == 0)
        {
            EmitDebug(context, ProtoSeverity.SevWarn, "backpressure.pause.skipped", $"Backpressure pause skipped: no eligible brains. reason={reason}");
            return false;
        }

        if (_options.BackpressurePauseStrategy == BackpressurePauseStrategy.LowestEnergy)
        {
            var ioPid = _ioPid is null ? null : ResolveSendTargetPid(context, _ioPid);
            var resolveTask = ResolveBackpressurePauseCandidatesAsync(context.System, ioPid, candidates);
            context.ReenterAfter(
                resolveTask,
                task =>
                {
                    var resolvedCandidates = task.IsCompletedSuccessfully ? task.Result : candidates;
                    ApplyBackpressurePauseSelection(context, resolvedCandidates, reason);
                    ScheduleNextTick(context, nextTickDelay);
                    return Task.CompletedTask;
                });
            return true;
        }

        ApplyBackpressurePauseSelection(context, candidates, reason);
        return false;
    }

    private List<BackpressurePauseCandidate> BuildBackpressurePauseCandidates()
    {
        var candidates = new List<BackpressurePauseCandidate>();
        foreach (var brain in _brains.Values)
        {
            if (!CanDispatchTickToBrain(brain))
            {
                continue;
            }

            candidates.Add(new BackpressurePauseCandidate(brain.BrainId, brain.SpawnedMs, brain.PausePriority, 0L));
        }

        return candidates;
    }

    private void ApplyBackpressurePauseSelection(
        IContext context,
        IReadOnlyList<BackpressurePauseCandidate> candidates,
        string reason)
    {
        foreach (var candidate in OrderBackpressurePauseCandidates(candidates))
        {
            if (!_brains.TryGetValue(candidate.BrainId, out var brain) || !CanDispatchTickToBrain(brain))
            {
                continue;
            }

            PauseBrain(
                context,
                candidate.BrainId,
                $"{reason}; strategy={FormatBackpressurePauseStrategy(_options.BackpressurePauseStrategy)}");
            HiveMindTelemetry.RecordPause(reason);
            return;
        }

        EmitDebug(
            context,
            ProtoSeverity.SevWarn,
            "backpressure.pause.skipped",
            $"Backpressure pause skipped: no matching brain remained eligible. reason={reason}");
    }

    private IEnumerable<BackpressurePauseCandidate> OrderBackpressurePauseCandidates(IReadOnlyList<BackpressurePauseCandidate> candidates)
    {
        return _options.BackpressurePauseStrategy switch
        {
            BackpressurePauseStrategy.NewestFirst => candidates
                .OrderByDescending(static candidate => candidate.SpawnedMs)
                .ThenBy(static candidate => candidate.BrainId),
            BackpressurePauseStrategy.LowestEnergy => candidates
                .OrderBy(static candidate => candidate.EnergyRemaining)
                .ThenBy(static candidate => candidate.SpawnedMs)
                .ThenBy(static candidate => candidate.BrainId),
            BackpressurePauseStrategy.LowestPriority => candidates
                .OrderBy(static candidate => candidate.PausePriority)
                .ThenBy(static candidate => candidate.SpawnedMs)
                .ThenBy(static candidate => candidate.BrainId),
            BackpressurePauseStrategy.ExternalOrder => OrderBackpressurePauseCandidatesByExternalOrder(candidates),
            _ => candidates
                .OrderBy(static candidate => candidate.SpawnedMs)
                .ThenBy(static candidate => candidate.BrainId)
        };
    }

    private IEnumerable<BackpressurePauseCandidate> OrderBackpressurePauseCandidatesByExternalOrder(IReadOnlyList<BackpressurePauseCandidate> candidates)
    {
        if (_options.BackpressurePauseExternalOrder is not { Length: > 0 } externalOrder)
        {
            return Array.Empty<BackpressurePauseCandidate>();
        }

        var byId = candidates.ToDictionary(static candidate => candidate.BrainId);
        var ordered = new List<BackpressurePauseCandidate>(Math.Min(externalOrder.Length, candidates.Count));
        foreach (var brainId in externalOrder)
        {
            if (byId.TryGetValue(brainId, out var candidate))
            {
                ordered.Add(candidate);
            }
        }

        return ordered;
    }

    private static async Task<List<BackpressurePauseCandidate>> ResolveBackpressurePauseCandidatesAsync(
        ActorSystem system,
        PID? ioPid,
        IReadOnlyList<BackpressurePauseCandidate> candidates)
    {
        if (candidates.Count == 0 || ioPid is null)
        {
            return new List<BackpressurePauseCandidate>(candidates);
        }

        var resolved = await Task.WhenAll(
                candidates.Select(
                    async candidate => candidate with
                    {
                        EnergyRemaining = await TryReadBrainEnergyRemainingAsync(system, ioPid, candidate.BrainId).ConfigureAwait(false)
                    }))
            .ConfigureAwait(false);
        return resolved.ToList();
    }

    private static string FormatBackpressurePauseStrategy(BackpressurePauseStrategy strategy)
    {
        return strategy switch
        {
            BackpressurePauseStrategy.NewestFirst => "newest-first",
            BackpressurePauseStrategy.LowestEnergy => "lowest-energy",
            BackpressurePauseStrategy.LowestPriority => "lowest-priority",
            BackpressurePauseStrategy.ExternalOrder => "external-order",
            _ => "oldest-first"
        };
    }

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

    private static TimeSpan SafeDuration(DateTime start, DateTime end)
    {
        if (start == default || end == default || end < start)
        {
            return TimeSpan.Zero;
        }

        return end - start;
    }

    private static TimeSpan ComputeTickDelay(TimeSpan elapsed, float targetTickHz)
    {
        if (targetTickHz <= 0)
        {
            return TimeSpan.Zero;
        }

        var period = TimeSpan.FromSeconds(1d / targetTickHz);
        var delay = period - elapsed;
        return delay <= TimeSpan.Zero ? TimeSpan.Zero : delay;
    }

    private void EmitTickComputeDoneIgnored(
        IContext context,
        ProtoControl.TickComputeDone message,
        string reason,
        PID? expectedSender = null)
    {
        var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
        var expectedLabel = expectedSender is null ? string.Empty : $" expectedSender={PidLabel(expectedSender)}";
        var brainLabel = message.BrainId is null || !message.BrainId.TryToGuid(out var brainId)
            ? "<invalid>"
            : brainId.ToString("D");
        var shardLabel = message.ShardId is null
            ? "<missing>"
            : message.ShardId.ToShardId32().ToString();
        EmitDebug(
            context,
            ProtoSeverity.SevDebug,
            "tick.compute_done.ignored",
            $"Ignored TickComputeDone. reason={reason} tick={message.TickId} brain={brainLabel} shard={shardLabel} sender={senderLabel}{expectedLabel}.");
        if (LogTickBarrier || LogVizDiagnostics)
        {
            Log($"TickComputeDone ignored reason={reason} tick={message.TickId} brain={brainLabel} shard={shardLabel} sender={senderLabel}{expectedLabel}");
        }
    }

    private void EmitTickDeliverDoneIgnored(
        IContext context,
        ProtoControl.TickDeliverDone message,
        string reason,
        PID? expectedSender = null)
    {
        var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
        var expectedLabel = expectedSender is null ? string.Empty : $" expectedSender={PidLabel(expectedSender)}";
        var brainLabel = message.BrainId is null || !message.BrainId.TryToGuid(out var brainId)
            ? "<invalid>"
            : brainId.ToString("D");
        EmitDebug(
            context,
            ProtoSeverity.SevDebug,
            "tick.deliver_done.ignored",
            $"Ignored TickDeliverDone. reason={reason} tick={message.TickId} brain={brainLabel} sender={senderLabel}{expectedLabel}.");
        if (LogTickBarrier || LogVizDiagnostics)
        {
            Log($"TickDeliverDone ignored reason={reason} tick={message.TickId} brain={brainLabel} sender={senderLabel}{expectedLabel}");
        }
    }

    private string DescribePendingCompute(int maxItems = 10)
    {
        if (_pendingCompute.Count == 0)
        {
            return "none";
        }

        var sb = new StringBuilder();
        var index = 0;
        foreach (var key in _pendingCompute)
        {
            if (index > 0)
            {
                sb.Append("; ");
            }

            var senderLabel = _pendingComputeSenders.TryGetValue(key, out var sender)
                ? PidLabel(sender)
                : "<missing>";
            sb.Append($"brain={key.BrainId:D} shard={key.ShardId} sender={senderLabel}");
            index++;
            if (index >= maxItems)
            {
                break;
            }
        }

        if (_pendingCompute.Count > index)
        {
            sb.Append($"; +{_pendingCompute.Count - index} more");
        }

        return sb.ToString();
    }

    private string DescribePendingDeliver(int maxItems = 10)
    {
        if (_pendingDeliver.Count == 0)
        {
            return "none";
        }

        var sb = new StringBuilder();
        var index = 0;
        foreach (var brainId in _pendingDeliver)
        {
            if (index > 0)
            {
                sb.Append("; ");
            }

            var senderLabel = _pendingDeliverSenders.TryGetValue(brainId, out var sender)
                ? PidLabel(sender)
                : "<missing>";
            sb.Append($"brain={brainId:D} sender={senderLabel}");
            index++;
            if (index >= maxItems)
            {
                break;
            }
        }

        if (_pendingDeliver.Count > index)
        {
            sb.Append($"; +{_pendingDeliver.Count - index} more");
        }

        return sb.ToString();
    }
}
