using Nbn.Proto.Viz;
using Nbn.Runtime.IO;
using Nbn.Shared;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private bool HandleTickAndRescheduleMessage(IContext context)
    {
        switch (context.Message)
        {
            case ProtoControl.TickComputeDone message:
                HandleTickComputeDone(context, message);
                return true;
            case ProtoControl.TickDeliverDone message:
                HandleTickDeliverDone(context, message);
                return true;
            case TickPhaseTimeout message:
                HandleTickPhaseTimeout(context, message);
                return true;
            case RescheduleNow message:
                BeginReschedule(context, message);
                return true;
            case RescheduleCompleted message:
                CompleteReschedule(context, message);
                return true;
            case RetryQueuedReschedule:
                HandleRetryQueuedReschedule(context);
                return true;
            default:
                return false;
        }
    }

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

        if (LogVizDiagnostics)
        {
            var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
            Log(
                $"VizDiag TickDeliverDone accepted tick={message.TickId} brain={brainId} sender={senderLabel} expectedSender={PidLabel(expectedSender)} pendingAfter={_pendingDeliver.Count}");
        }

        if (!_brains.TryGetValue(brainId, out var brainState))
        {
            EmitTickDeliverDoneIgnored(context, message, "brain_not_registered", expectedSender);
            return;
        }

        if (brainState.PendingRuntimeReset is not null)
        {
            _pendingDeliverSenders.Remove(brainId);
            StartPendingRuntimeReset(context, brainState);
            return;
        }

        if (!RemovePendingDeliver(brainId))
        {
            EmitTickDeliverDoneIgnored(context, message, "untracked_payload", expectedSender);
            return;
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
                FailPendingRuntimeResetsForBrains(_pendingDeliverSenders.Keys.ToArray(), "deliver_timeout");
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

        if (_pendingBarrierResets.Count == 0)
        {
            ScheduleNextTick(context, ComputeTickDelay(elapsed, decision.TargetTickHz));
        }
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

    private void FailPendingRuntimeResetsForBrains(IEnumerable<Guid> brainIds, string reason)
    {
        foreach (var brainId in brainIds)
        {
            if (!_brains.TryGetValue(brainId, out var brain))
            {
                continue;
            }

            if (brain.PendingRuntimeReset is null)
            {
                continue;
            }

            brain.PendingRuntimeReset.Completion.TrySetResult(
                BuildRuntimeResetAck(brain.BrainId, success: false, reason));
            brain.PendingRuntimeReset = null;
            _pendingBarrierResets.Remove(brain.BrainId);
        }
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
}
