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
            case UpdateShardOutputSink message:
                HandleUpdateOutputSink(message);
                break;
        }

        return Task.CompletedTask;
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

        if (_hasComputed && batch.TickId < _lastComputeTickId)
        {
            RegionHostTelemetry.RecordSignalBatchLate();
            EmitDebug(context, ProtoSeverity.SevWarn, "signal.late", $"Late SignalBatch tick={batch.TickId} lastCompute={_lastComputeTickId}");
            if (LogDelivery)
            {
                Console.WriteLine($"[RegionShard] SignalBatch late. tick={batch.TickId} lastCompute={_lastComputeTickId}");
            }
        }

        foreach (var contrib in batch.Contribs)
        {
            _state.ApplyContribution(contrib.TargetNeuronId, contrib.Value);
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

        context.Send(target, ack);
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
        var result = _cpu.Compute(tick.TickId, _brainId, _shardId, _routing);
        stopwatch.Stop();

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

        if (result.FiredCount > 0)
        {
            EmitVizEvent(
                context,
                VizEventType.VizNeuronFired,
                tick.TickId,
                result.FiredCount);
        }

        if (result.OutContribs > 0)
        {
            EmitVizEvent(
                context,
                VizEventType.VizAxonSent,
                tick.TickId,
                result.OutContribs);
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
            context.Send(doneTarget, done);
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

    private void EmitVizEvent(IContext context, VizEventType type, ulong tickId, float value)
    {
        if (_vizHub is null || !ObservabilityTargets.CanSend(context, _vizHub))
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
            Value = value
        };

        context.Send(_vizHub, evt);
    }

    private void EmitDebug(IContext context, ProtoSeverity severity, string category, string message)
    {
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
