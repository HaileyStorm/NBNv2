using System.Diagnostics;
using Nbn.Proto.Control;
using Nbn.Proto.Signal;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;

namespace Nbn.Runtime.RegionHost;

public sealed class RegionShardActor : IActor
{
    private readonly RegionShardState _state;
    private readonly RegionShardCpuBackend _cpu;
    private readonly Guid _brainId;
    private readonly ShardId32 _shardId;
    private RegionShardRoutingTable _routing;
    private PID? _router;
    private PID? _outputSink;
    private PID? _tickSink;

    public RegionShardActor(RegionShardState state, RegionShardActorConfig config)
    {
        _state = state ?? throw new ArgumentNullException(nameof(state));
        _cpu = new RegionShardCpuBackend(_state);
        _brainId = config.BrainId;
        _shardId = config.ShardId;
        _router = config.Router;
        _outputSink = config.OutputSink;
        _tickSink = config.TickSink;
        _routing = config.Routing ?? RegionShardRoutingTable.CreateSingleShard(_state.RegionId, _state.NeuronCount);
    }

    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
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
        }

        return Task.CompletedTask;
    }

    private void HandleSignalBatch(IContext context, SignalBatch batch)
    {
        if (batch is null)
        {
            return;
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

        var target = context.Sender ?? _router;
        if (target is not null)
        {
            context.Send(target, ack);
        }
    }

    private void HandleTickCompute(IContext context, TickCompute tick)
    {
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

        if (_outputSink is not null && result.OutputEvents.Count > 0)
        {
            foreach (var output in result.OutputEvents)
            {
                context.Send(_outputSink, output);
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

        var doneTarget = _tickSink ?? context.Sender ?? _router;
        if (doneTarget is not null)
        {
            context.Send(doneTarget, done);
        }
    }
}
