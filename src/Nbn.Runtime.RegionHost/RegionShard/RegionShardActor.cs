using System.Diagnostics;
using Google.Protobuf;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Proto.Viz;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Nbn.Shared.Quantization;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;

namespace Nbn.Runtime.RegionHost;

public sealed class RegionShardActor : IActor
{
    private static readonly bool LogDelivery = IsEnvTrue("NBN_REGIONHOST_LOG_DELIVERY");
    private const int RecentComputeCacheSize = 2;
    private readonly RegionShardState _state;
    private readonly RegionShardCpuBackend _cpu;
    private readonly Guid _brainId;
    private readonly ShardId32 _shardId;
    private readonly Dictionary<ulong, TickComputeDone> _recentComputeDone = new();
    private RegionShardRoutingTable _routing;
    private PID? _router;
    private PID? _outputSink;
    private PID? _tickSink;
    private PID? _vizHub;
    private PID? _debugHub;
    private bool _debugEnabled;
    private ProtoSeverity _debugMinSeverity;
    private bool _vizEnabled;
    private uint? _vizFocusRegionId;
    private bool _costEnabled;
    private bool _energyEnabled;
    private bool _plasticityEnabled;
    private float _plasticityRate;
    private bool _plasticityProbabilisticUpdates;
    private bool _hasComputed;
    private ulong _lastComputeTickId;
    private ulong _vizSequence;

    public RegionShardActor(RegionShardState state, RegionShardActorConfig config)
    {
        _state = state ?? throw new ArgumentNullException(nameof(state));
        _cpu = new RegionShardCpuBackend(_state);
        _brainId = config.BrainId;
        _shardId = config.ShardId;
        _router = config.Router;
        _outputSink = config.OutputSink;
        _tickSink = config.TickSink;
        _vizHub = config.VizHub;
        _debugHub = config.DebugHub;
        _debugEnabled = config.DebugEnabled;
        _debugMinSeverity = config.DebugMinSeverity;
        _vizEnabled = config.VizEnabled;
        _vizFocusRegionId = null;
        _routing = config.Routing ?? RegionShardRoutingTable.CreateSingleShard(_state.RegionId, _state.NeuronCount);
    }

    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
                EmitVizEvent(context, VizEventType.VizShardSpawned, tickId: 0, value: 0f);
                EmitDebug(context, ProtoSeverity.SevInfo, "shard.started", $"Shard {_shardId} for brain {_brainId} started.");
                break;
            case RegionShardUpdateEndpoints endpoints:
                _router = endpoints.Router;
                _outputSink = endpoints.OutputSink;
                _tickSink = endpoints.TickSink;
                break;
            case RegionShardUpdateRouting routing:
                _routing = routing.Routing;
                break;
            case SignalBatch batch:
                HandleSignalBatch(context, batch);
                break;
            case TickCompute tick:
                HandleTickCompute(context, tick);
                break;
            case RegisterShard registerShard:
                ForwardRegisterShard(context, registerShard);
                break;
            case UnregisterShard unregisterShard:
                ForwardUnregisterShard(context, unregisterShard);
                break;
            case RuntimeNeuronPulse pulse:
                HandleRuntimeNeuronPulse(pulse);
                break;
            case RuntimeNeuronStateWrite stateWrite:
                HandleRuntimeNeuronStateWrite(stateWrite);
                break;
            case UpdateShardOutputSink message:
                HandleUpdateOutputSink(message);
                break;
            case UpdateShardVisualization message:
                HandleUpdateVisualization(message);
                break;
            case UpdateShardRuntimeConfig message:
                HandleUpdateRuntimeConfig(message);
                break;
            case CaptureShardSnapshot message:
                HandleCaptureShardSnapshot(context, message);
                break;
        }

        return Task.CompletedTask;
    }

    private void ForwardRegisterShard(IContext context, RegisterShard message)
    {
        if (_tickSink is not null)
        {
            context.Request(_tickSink, message);
        }
    }

    private void ForwardUnregisterShard(IContext context, UnregisterShard message)
    {
        if (_tickSink is not null)
        {
            context.Request(_tickSink, message);
        }
    }

    private void HandleUpdateOutputSink(UpdateShardOutputSink message)
    {
        if (message.BrainId is null || !message.BrainId.TryToGuid(out var guid) || guid != _brainId)
        {
            return;
        }

        if (message.RegionId != (uint)_state.RegionId || message.ShardIndex != (uint)_shardId.ShardIndex)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(message.OutputPid))
        {
            _outputSink = null;
            return;
        }

        if (TryParsePid(message.OutputPid, out var pid))
        {
            _outputSink = pid;
        }
    }

    private void HandleUpdateVisualization(UpdateShardVisualization message)
    {
        if (message.BrainId is null || !message.BrainId.TryToGuid(out var guid) || guid != _brainId)
        {
            return;
        }

        if (message.RegionId != (uint)_state.RegionId || message.ShardIndex != (uint)_shardId.ShardIndex)
        {
            return;
        }

        _vizEnabled = message.Enabled;
        _vizFocusRegionId = message.Enabled && message.HasFocusRegion
            ? message.FocusRegionId
            : null;
    }

    private void HandleUpdateRuntimeConfig(UpdateShardRuntimeConfig message)
    {
        if (message.BrainId is null || !message.BrainId.TryToGuid(out var guid) || guid != _brainId)
        {
            return;
        }

        if (message.RegionId != (uint)_state.RegionId || message.ShardIndex != (uint)_shardId.ShardIndex)
        {
            return;
        }

        _costEnabled = message.CostEnabled;
        _energyEnabled = message.EnergyEnabled;
        _plasticityEnabled = message.PlasticityEnabled;
        _plasticityRate = message.PlasticityRate;
        _plasticityProbabilisticUpdates = message.ProbabilisticUpdates;
        _debugEnabled = message.DebugEnabled;
        _debugMinSeverity = message.DebugMinSeverity;
    }

    private void HandleCaptureShardSnapshot(IContext context, CaptureShardSnapshot message)
    {
        var response = new CaptureShardSnapshotAck
        {
            BrainId = _brainId.ToProtoUuid(),
            RegionId = (uint)_state.RegionId,
            ShardIndex = (uint)_shardId.ShardIndex,
            NeuronStart = (uint)_state.NeuronStart,
            NeuronCount = (uint)_state.NeuronCount,
            Success = false
        };

        if (message.BrainId is null || !message.BrainId.TryToGuid(out var brainId) || brainId != _brainId)
        {
            response.Error = "brain_id_mismatch";
            context.Respond(response);
            return;
        }

        if (message.RegionId != (uint)_state.RegionId || message.ShardIndex != (uint)_shardId.ShardIndex)
        {
            response.Error = "shard_id_mismatch";
            context.Respond(response);
            return;
        }

        var enabledBytes = new byte[(_state.NeuronCount + 7) / 8];
        for (var i = 0; i < _state.NeuronCount; i++)
        {
            var buffer = _state.Buffer[i];
            if (!float.IsFinite(buffer))
            {
                buffer = 0f;
            }

            var code = QuantizationSchemas.DefaultBuffer.Encode(buffer, bits: 16);
            response.BufferCodes.Add(code);

            if (_state.Enabled[i])
            {
                enabledBytes[i / 8] |= (byte)(1 << (i % 8));
            }
        }

        response.EnabledBitset = ByteString.CopyFrom(enabledBytes);

        for (var i = 0; i < _state.Axons.Count; i++)
        {
            var runtimeCode = _state.Axons.RuntimeStrengthCodes[i];
            if (!_state.Axons.HasRuntimeOverlay[i] || runtimeCode == _state.Axons.BaseStrengthCodes[i])
            {
                continue;
            }

            response.Overlays.Add(new SnapshotOverlayRecord
            {
                FromAddress = _state.Axons.FromAddress32[i],
                ToAddress = _state.Axons.ToAddress32[i],
                StrengthCode = runtimeCode
            });
        }

        response.Success = true;
        context.Respond(response);
    }

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

        var ack = new SignalBatchAck
        {
            BrainId = _brainId.ToProtoUuid(),
            RegionId = (uint)_state.RegionId,
            ShardId = _shardId.ToProtoShardId32(),
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
        var vizScope = _vizEnabled
            ? new RegionShardVisualizationComputeScope(true, _vizFocusRegionId)
            : RegionShardVisualizationComputeScope.Disabled;
        var result = _cpu.Compute(
            tick.TickId,
            _brainId,
            _shardId,
            _routing,
            vizScope,
            _plasticityEnabled,
            _plasticityRate,
            _plasticityProbabilisticUpdates);
        stopwatch.Stop();

        if (result.PlasticityStrengthCodeChanges > 0)
        {
            RegionHostTelemetry.RecordPlasticityStrengthCodeChanges(result.PlasticityStrengthCodeChanges, _state.RegionId, _shardId.ShardIndex);
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "plasticity.mutation",
                $"TickCompute {tick.TickId} mutated {result.PlasticityStrengthCodeChanges} strength code(s) on shard {_shardId.Value}.");
        }

        var outboxTarget = _router ?? context.Sender;
        if (outboxTarget is not null)
        {
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
                    TickId = tick.TickId,
                    DestRegionId = (uint)destShard.RegionId,
                    DestShardId = destShard.ToProtoShardId32()
                };
                batch.Contribs.AddRange(contribs);
                context.Send(outboxTarget, batch);
            }
        }

        foreach (var axonViz in result.AxonVizEvents)
        {
            EmitVizEvent(
                context,
                VizEventType.VizAxonSent,
                tick.TickId,
                axonViz.AverageSignedValue,
                source: new Address32(axonViz.SourceAddress),
                target: new Address32(axonViz.TargetAddress),
                strength: axonViz.AverageSignedStrength);
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

        if (_outputSink is not null)
        {
            if (result.OutputEvents.Count > 0)
            {
                foreach (var output in result.OutputEvents)
                {
                    context.Send(_outputSink, output);
                }
            }

            if (result.OutputVector.Count > 0)
            {
                var vector = new OutputVectorEvent
                {
                    BrainId = _brainId.ToProtoUuid(),
                    TickId = tick.TickId
                };
                vector.Values.Add(result.OutputVector);
                context.Send(_outputSink, vector);
            }
        }

        var done = new TickComputeDone
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

        _hasComputed = true;
        _lastComputeTickId = tick.TickId;
        CacheComputeDone(done);
        SendComputeDone(context, done);
    }

    private void HandleRuntimeNeuronPulse(RuntimeNeuronPulse message)
    {
        if (message.BrainId is null
            || !message.BrainId.TryToGuid(out var guid)
            || guid != _brainId
            || message.TargetRegionId != (uint)_state.RegionId)
        {
            return;
        }

        _state.TryApplyRuntimePulse(message.TargetNeuronId, message.Value);
    }

    private void HandleRuntimeNeuronStateWrite(RuntimeNeuronStateWrite message)
    {
        if (message.BrainId is null
            || !message.BrainId.TryToGuid(out var guid)
            || guid != _brainId
            || message.TargetRegionId != (uint)_state.RegionId)
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

    private void EmitVizEvent(
        IContext context,
        VizEventType type,
        ulong tickId,
        float value,
        Address32? source = null,
        Address32? target = null,
        float strength = 0f)
    {
        if (!_vizEnabled
            || _vizHub is null
            || !ObservabilityTargets.CanSend(context, _vizHub)
            || !TouchesFocusRegion(source, target))
        {
            return;
        }

        var evt = new VisualizationEvent
        {
            EventId = $"region-{++_vizSequence}",
            TimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Type = type,
            BrainId = _brainId.ToProtoUuid(),
            TickId = tickId,
            RegionId = (uint)_state.RegionId,
            ShardId = _shardId.ToProtoShardId32(),
            Value = value,
            Strength = strength
        };

        if (source.HasValue)
        {
            evt.Source = source.Value.ToProtoAddress32();
        }

        if (target.HasValue)
        {
            evt.Target = target.Value.ToProtoAddress32();
        }

        context.Send(_vizHub, evt);
    }

    private bool TouchesFocusRegion(Address32? source, Address32? target)
    {
        if (!_vizFocusRegionId.HasValue)
        {
            return true;
        }

        var focusRegionId = _vizFocusRegionId.Value;
        if ((uint)_state.RegionId == focusRegionId)
        {
            return true;
        }

        if (source.HasValue && (uint)source.Value.RegionId == focusRegionId)
        {
            return true;
        }

        if (target.HasValue && (uint)target.Value.RegionId == focusRegionId)
        {
            return true;
        }

        return false;
    }

    private void EmitDebug(IContext context, ProtoSeverity severity, string category, string message)
    {
        if (!_debugEnabled || severity < _debugMinSeverity)
        {
            return;
        }

        if (_debugHub is null || !ObservabilityTargets.CanSend(context, _debugHub))
        {
            return;
        }

        context.Send(_debugHub, new DebugOutbound
        {
            Severity = severity,
            Context = $"region.{category}",
            Summary = category,
            Message = message,
            SenderActor = string.IsNullOrWhiteSpace(context.Self.Address) ? context.Self.Id : $"{context.Self.Address}/{context.Self.Id}",
            SenderNode = context.System.Address ?? string.Empty,
            TimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        });
    }

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Equals("1", StringComparison.OrdinalIgnoreCase)
               || value.Equals("true", StringComparison.OrdinalIgnoreCase)
               || value.Equals("yes", StringComparison.OrdinalIgnoreCase)
               || value.Equals("y", StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryParsePid(string? value, out PID pid)
    {
        pid = new PID();
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        var slashIndex = trimmed.IndexOf('/');
        if (slashIndex <= 0)
        {
            pid.Id = trimmed;
            return true;
        }

        var address = trimmed[..slashIndex];
        var id = trimmed[(slashIndex + 1)..];
        if (string.IsNullOrWhiteSpace(id))
        {
            return false;
        }

        pid.Address = address;
        pid.Id = id;
        return true;
    }
}
