using System.Diagnostics;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Proto.Viz;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;

namespace Nbn.Runtime.RegionHost;

public sealed partial class RegionShardActor
{
    private void HandleSignalBatch(IContext context, SignalBatch batch)
    {
        if (batch is null)
        {
            return;
        }

        if (!IsBatchForShard(batch, out var rejectReason))
        {
            RegionHostTelemetry.RecordSignalBatchRejected();
            EmitDebug(context, ProtoSeverity.SevWarn, "signal.rejected", $"Rejected SignalBatch tick={batch.TickId} reason={rejectReason}");
            if (LogDelivery)
            {
                Console.WriteLine($"[RegionShard] SignalBatch rejected. reason={rejectReason} tick={batch.TickId}");
            }
            SendSignalBatchAck(context, batch, preferBatchAddressing: true);
            return;
        }

        var isLateBatch = _hasComputed && batch.TickId < _lastComputeTickId;
        if (isLateBatch)
        {
            RegionHostTelemetry.RecordSignalBatchLate();
            EmitDebug(context, ProtoSeverity.SevWarn, "signal.late", $"Late SignalBatch tick={batch.TickId} lastCompute={_lastComputeTickId}");
            if (LogDelivery)
            {
                Console.WriteLine($"[RegionShard] SignalBatch late. tick={batch.TickId} lastCompute={_lastComputeTickId}");
            }
        }
        else
        {
            foreach (var contrib in batch.Contribs)
            {
                _state.ApplyContribution(contrib.TargetNeuronId, contrib.Value);
            }
        }

        SendSignalBatchAck(context, batch, preferBatchAddressing: false);
    }

    private void SendSignalBatchAck(IContext context, SignalBatch batch, bool preferBatchAddressing)
    {
        var ackRegionId = preferBatchAddressing ? batch.RegionId : (uint)_state.RegionId;
        var ackShardId = preferBatchAddressing && batch.ShardId is not null
            ? batch.ShardId.ToShardId32()
            : _shardId;
        var ack = new SignalBatchAck
        {
            BrainId = _brainId.ToProtoUuid(),
            RegionId = ackRegionId,
            ShardId = ackShardId.ToProtoShardId32(),
            TickId = batch.TickId
        };

        var target = context.Sender;
        if (target is null || string.IsNullOrWhiteSpace(target.Address))
        {
            if (LogDelivery)
            {
                var senderLabel = target is null ? "(null)" : $"{target.Id}";
                var routerLabel = _router is null ? "(null)" : $"{_router.Address}/{_router.Id}";
                Console.WriteLine($"[RegionShard] SignalBatch ack fallback to router. tick={batch.TickId} sender={senderLabel} router={routerLabel}");
            }

            target = _router;
        }

        if (target is null)
        {
            if (LogDelivery)
            {
                Console.WriteLine($"[RegionShard] SignalBatch ack dropped. tick={batch.TickId} reason=no_target");
            }

            return;
        }

        context.Request(target, ack);
    }

    private void HandleTickCompute(IContext context, TickCompute tick)
    {
        if (_recentComputeDone.TryGetValue(tick.TickId, out var cachedDone))
        {
            RegionHostTelemetry.RecordComputeDuplicate();
            EmitDebug(context, ProtoSeverity.SevDebug, "tick.duplicate", $"Duplicate TickCompute {tick.TickId}");
            if (LogDelivery)
            {
                Console.WriteLine($"[RegionShard] TickCompute duplicate. tick={tick.TickId}");
            }

            SendComputeDone(context, cachedDone);
            return;
        }

        if (_hasComputed && tick.TickId < _lastComputeTickId)
        {
            RegionHostTelemetry.RecordComputeOutOfOrder();
            EmitDebug(context, ProtoSeverity.SevWarn, "tick.out_of_order", $"Out-of-order TickCompute tick={tick.TickId} lastCompute={_lastComputeTickId}");
            if (LogDelivery)
            {
                Console.WriteLine($"[RegionShard] TickCompute out-of-order. tick={tick.TickId} lastCompute={_lastComputeTickId}");
            }
            return;
        }

        if (_hasComputed && tick.TickId > _lastComputeTickId + 1)
        {
            RegionHostTelemetry.RecordComputeJump();
            EmitDebug(context, ProtoSeverity.SevWarn, "tick.jump", $"TickCompute jump tick={tick.TickId} lastCompute={_lastComputeTickId}");
            if (LogDelivery)
            {
                Console.WriteLine($"[RegionShard] TickCompute jump. tick={tick.TickId} lastCompute={_lastComputeTickId}");
            }
        }

        var stopwatch = Stopwatch.StartNew();
        var collectVisualization = ShouldCollectVisualizationForTick(tick.TickId, tick.TargetTickHz);
        var visualizationScope = collectVisualization
            ? new RegionShardVisualizationComputeScope(true, _vizFocusRegionId)
            : RegionShardVisualizationComputeScope.Disabled;
        var result = _computeBackend.Compute(
            tick.TickId,
            _brainId,
            _shardId,
            _routing,
            visualizationScope,
            _plasticityEnabled,
            _plasticityRate,
            _plasticityProbabilisticUpdates,
            _plasticityDelta,
            _plasticityRebaseThreshold,
            _plasticityRebaseThresholdPct,
            new RegionShardPlasticityEnergyCostConfig(
                _plasticityEnergyCostModulationEnabled,
                _plasticityEnergyCostReferenceTickCost,
                _plasticityEnergyCostResponseStrength,
                _plasticityEnergyCostMinScale,
                _plasticityEnergyCostMaxScale),
            new RegionShardHomeostasisConfig(
                _homeostasisEnabled,
                _homeostasisTargetMode,
                _homeostasisUpdateMode,
                _homeostasisBaseProbability,
                _homeostasisMinStepCodes,
                _homeostasisEnergyCouplingEnabled,
                _homeostasisEnergyTargetScale,
                _homeostasisEnergyProbabilityScale),
            _costEnergyEnabled,
            _remoteCostEnabled,
            _remoteCostPerBatch,
            _remoteCostPerContribution,
            _costTierAMultiplier,
            _costTierBMultiplier,
            _costTierCMultiplier,
            _outputVectorSource,
            _lastTickCostTotal);
        stopwatch.Stop();

        if (LogViz || LogVizDiagnostics)
        {
            var focusLabel = _vizFocusRegionId.HasValue ? _vizFocusRegionId.Value.ToString() : "all";
            var hubLabel = _vizHub is null
                ? "(null)"
                : (string.IsNullOrWhiteSpace(_vizHub.Address) ? _vizHub.Id : $"{_vizHub.Address}/{_vizHub.Id}");
            var vizStride = ComputeVizStride(tick.TargetTickHz, _vizStreamMinIntervalMs);
            Console.WriteLine(
                $"[RegionShard] Viz compute tick={tick.TickId} shard={_shardId} enabled={_vizEnabled} focus={focusLabel} streamMinIntervalMs={_vizStreamMinIntervalMs} stride={vizStride} collect={collectVisualization} hub={hubLabel} axonEvents={result.AxonVizEvents.Count} bufferEvents={result.BufferNeuronEvents.Count} firedEvents={result.FiredNeuronEvents.Count}.");
        }

        if (result.PlasticityStrengthCodeChanges > 0)
        {
            RegionHostTelemetry.RecordPlasticityStrengthCodeChanges(result.PlasticityStrengthCodeChanges, _state.RegionId, _shardId.ShardIndex);
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "plasticity.mutation",
                $"TickCompute {tick.TickId} mutated {result.PlasticityStrengthCodeChanges} strength code(s) on shard {_shardId.Value}.");
        }

        PublishOutboxBatches(context, tick.TickId, result);
        PublishVisualizationEvents(context, tick.TickId, result);
        PublishOutputEvents(context, tick.TickId, result);

        var done = CreateTickComputeDone(tick, stopwatch, result);
        if (LogOutput && _state.IsOutputRegion)
        {
            var sinkLabel = _outputSink is null
                ? "(null)"
                : (string.IsNullOrWhiteSpace(_outputSink.Address) ? _outputSink.Id : $"{_outputSink.Address}/{_outputSink.Id}");
            Console.WriteLine(
                $"[RegionShard] Output compute tick={tick.TickId} shard={_shardId} sink={sinkLabel} vectorCount={result.OutputVector.Count} singleCount={result.OutputEvents.Count}");
        }

        _hasComputed = true;
        _lastComputeTickId = tick.TickId;
        _lastTickCostTotal = result.Cost.Total;
        CacheComputeDone(done);
        SendComputeDone(context, done);
    }

    private void PublishOutboxBatches(IContext context, ulong tickId, RegionShardComputeResult result)
    {
        var outboxTarget = _router ?? context.Sender;
        if (outboxTarget is null)
        {
            return;
        }

        var brainProto = _brainId.ToProtoUuid();
        foreach (var (destShard, contribs) in result.Outbox)
        {
            if (contribs.Count == 0)
            {
                continue;
            }

            var batch = new OutboxBatch
            {
                BrainId = brainProto,
                TickId = tickId,
                DestRegionId = (uint)destShard.RegionId,
                DestShardId = destShard.ToProtoShardId32()
            };
            batch.Contribs.AddRange(contribs);
            context.Send(outboxTarget, batch);
        }
    }

    private void PublishVisualizationEvents(IContext context, ulong tickId, RegionShardComputeResult result)
    {
        foreach (var axonViz in result.AxonVizEvents)
        {
            EmitVizEvent(
                context,
                VizEventType.VizAxonSent,
                tickId,
                axonViz.AverageSignedValue,
                source: new Address32(axonViz.SourceAddress),
                target: new Address32(axonViz.TargetAddress),
                strength: axonViz.AverageSignedStrength);
        }

        foreach (var buffer in result.BufferNeuronEvents)
        {
            EmitVizEvent(
                context,
                VizEventType.VizNeuronBuffer,
                buffer.TickId,
                buffer.Buffer,
                source: new Address32(buffer.SourceAddress));
        }

        foreach (var fired in result.FiredNeuronEvents)
        {
            EmitVizEvent(
                context,
                VizEventType.VizNeuronFired,
                fired.TickId,
                fired.Potential,
                source: new Address32(fired.SourceAddress));
        }
    }

    private void PublishOutputEvents(IContext context, ulong tickId, RegionShardComputeResult result)
    {
        if (_outputSink is null)
        {
            return;
        }

        if (result.OutputEvents.Count > 0)
        {
            foreach (var output in result.OutputEvents)
            {
                context.Send(_outputSink, output);
            }
        }

        if (result.OutputVector.Count > 0)
        {
            context.Send(
                _outputSink,
                new OutputVectorSegment
                {
                    BrainId = _brainId.ToProtoUuid(),
                    TickId = tickId,
                    OutputIndexStart = (uint)_state.NeuronStart,
                    Values = { result.OutputVector }
                });
        }
    }

    private TickComputeDone CreateTickComputeDone(TickCompute tick, Stopwatch stopwatch, RegionShardComputeResult result)
    {
        return new TickComputeDone
        {
            TickId = tick.TickId,
            BrainId = _brainId.ToProtoUuid(),
            RegionId = (uint)_state.RegionId,
            ShardId = _shardId.ToProtoShardId32(),
            ComputeMs = (ulong)Math.Round(stopwatch.Elapsed.TotalMilliseconds),
            TickCostTotal = result.Cost.Total,
            CostAccum = result.Cost.Accum,
            CostActivation = result.Cost.Activation,
            CostReset = result.Cost.Reset,
            CostDistance = result.Cost.Distance,
            CostRemote = result.Cost.Remote,
            FiredCount = result.FiredCount,
            OutBatches = (uint)result.Outbox.Count,
            OutContribs = result.OutContribs
        };
    }

    private void HandleRuntimeNeuronPulse(RuntimeNeuronPulse message)
    {
        if (!MatchesRegionMessage(message.BrainId, message.TargetRegionId))
        {
            return;
        }

        _state.TryApplyRuntimePulse(message.TargetNeuronId, message.Value);
    }

    private void HandleRuntimeNeuronStateWrite(RuntimeNeuronStateWrite message)
    {
        if (!MatchesRegionMessage(message.BrainId, message.TargetRegionId))
        {
            return;
        }

        _state.TrySetRuntimeNeuronState(
            message.TargetNeuronId,
            message.SetBuffer,
            message.BufferValue,
            message.SetAccumulator,
            message.AccumulatorValue);
    }

    private void CacheComputeDone(TickComputeDone done)
    {
        _recentComputeDone[done.TickId] = done;
        while (_recentComputeDone.Count > RecentComputeCacheSize)
        {
            var oldest = ulong.MaxValue;
            foreach (var key in _recentComputeDone.Keys)
            {
                if (key < oldest)
                {
                    oldest = key;
                }
            }

            if (oldest == ulong.MaxValue)
            {
                break;
            }

            _recentComputeDone.Remove(oldest);
        }
    }

    private void SendComputeDone(IContext context, TickComputeDone done)
    {
        var doneTarget = _tickSink ?? context.Sender ?? _router;
        if (doneTarget is not null)
        {
            context.Request(doneTarget, done);
        }

        if (LogVizDiagnostics)
        {
            var senderLabel = context.Sender is null
                ? "<missing>"
                : (string.IsNullOrWhiteSpace(context.Sender.Address) ? context.Sender.Id : $"{context.Sender.Address}/{context.Sender.Id}");
            var targetLabel = doneTarget is null
                ? "(null)"
                : (string.IsNullOrWhiteSpace(doneTarget.Address) ? doneTarget.Id : $"{doneTarget.Address}/{doneTarget.Id}");
            Console.WriteLine(
                $"[RegionShard] TickComputeDone sent tick={done.TickId} shard={_shardId} sender={senderLabel} target={targetLabel} vizEnabled={_vizEnabled} fired={done.FiredCount} outBatches={done.OutBatches}.");
        }
    }

    private bool IsBatchForShard(SignalBatch batch, out string reason)
    {
        reason = "unknown";

        if (batch.BrainId is null || !batch.BrainId.TryToGuid(out var batchBrain) || batchBrain != _brainId)
        {
            reason = "brain";
            return false;
        }

        if (batch.RegionId != (uint)_state.RegionId)
        {
            reason = "region";
            return false;
        }

        if (batch.ShardId is not null)
        {
            var shardId = batch.ShardId.ToShardId32();
            if (!shardId.Equals(_shardId))
            {
                reason = "shard";
                return false;
            }
        }

        reason = "ok";
        return true;
    }
}
