using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Signal;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.Quantization;
using Proto;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Tests.RegionHost;

public class RegionShardDebugEmissionTests
{
    [Fact]
    public async Task EmitDebug_WhenDisabled_DropsEvents()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var brainId = Guid.NewGuid();
        var shardId = ShardId32.From(regionId: 5, shardIndex: 0);
        var state = CreateState(regionId: 5, neuronStart: 0, neuronCount: 2);

        var actor = root.Spawn(Props.FromProducer(() => new RegionShardActor(
            state,
            new RegionShardActorConfig(
                BrainId: brainId,
                ShardId: shardId,
                Router: null,
                OutputSink: null,
                TickSink: null,
                Routing: RegionShardRoutingTable.CreateSingleShard(state.RegionId, state.NeuronCount),
                DebugHub: debugProbePid,
                DebugEnabled: false,
                DebugMinSeverity: Severity.SevTrace))));

        root.Send(actor, new TickCompute { TickId = 41 });
        root.Send(actor, new TickCompute { TickId = 41 });
        root.Send(actor, new SignalBatch
        {
            BrainId = Guid.NewGuid().ToProtoUuid(),
            RegionId = (uint)state.RegionId,
            ShardId = shardId.ToProtoShardId32(),
            TickId = 42
        });

        await Task.Delay(150);
        var snapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.Equal(0, snapshot.Count("shard.started"));
        Assert.Equal(0, snapshot.Count("tick.duplicate"));
        Assert.Equal(0, snapshot.Count("signal.rejected"));

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task EmitDebug_Respects_MinSeverity()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var brainId = Guid.NewGuid();
        var shardId = ShardId32.From(regionId: 5, shardIndex: 0);
        var state = CreateState(regionId: 5, neuronStart: 0, neuronCount: 2);

        var actor = root.Spawn(Props.FromProducer(() => new RegionShardActor(
            state,
            new RegionShardActorConfig(
                BrainId: brainId,
                ShardId: shardId,
                Router: null,
                OutputSink: null,
                TickSink: null,
                Routing: RegionShardRoutingTable.CreateSingleShard(state.RegionId, state.NeuronCount),
                DebugHub: debugProbePid,
                DebugEnabled: true,
                DebugMinSeverity: Severity.SevWarn))));

        root.Send(actor, new TickCompute { TickId = 11 });
        root.Send(actor, new TickCompute { TickId = 11 });
        root.Send(actor, new SignalBatch
        {
            BrainId = Guid.NewGuid().ToProtoUuid(),
            RegionId = (uint)state.RegionId,
            ShardId = shardId.ToProtoShardId32(),
            TickId = 12
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var snapshot = await WaitForDebugCountAsync(root, debugProbePid, "signal.rejected", minCount: 1, cts.Token);
        Assert.Equal(0, snapshot.Count("tick.duplicate"));
        Assert.True(snapshot.Count("signal.rejected") >= 1);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task HandleSignalBatch_WhenLate_DropsContributions_AndStillAcks()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var ackProbePid = root.Spawn(Props.FromProducer(static () => new SignalAckProbeActor()));
        var brainId = Guid.NewGuid();
        var shardId = ShardId32.From(regionId: 5, shardIndex: 0);
        var state = CreateState(regionId: 5, neuronStart: 0, neuronCount: 2);

        var actor = root.Spawn(Props.FromProducer(() => new RegionShardActor(
            state,
            new RegionShardActorConfig(
                BrainId: brainId,
                ShardId: shardId,
                Router: ackProbePid,
                OutputSink: null,
                TickSink: null,
                Routing: RegionShardRoutingTable.CreateSingleShard(state.RegionId, state.NeuronCount),
                DebugHub: debugProbePid,
                DebugEnabled: true,
                DebugMinSeverity: Severity.SevWarn))));

        await root.RequestAsync<TickComputeDone>(actor, new TickCompute { TickId = 11 });

        root.Send(actor, new SignalBatch
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)state.RegionId,
            ShardId = shardId.ToProtoShardId32(),
            TickId = 10,
            Contribs =
            {
                new Contribution
                {
                    TargetNeuronId = 0,
                    Value = 0.75f
                }
            }
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var debugSnapshot = await WaitForDebugCountAsync(root, debugProbePid, "signal.late", minCount: 1, cts.Token);
        var ackSnapshot = await WaitForAckCountAsync(root, ackProbePid, minCount: 1, cts.Token);
        await root.RequestAsync<TickComputeDone>(actor, new TickCompute { TickId = 12 });

        Assert.True(debugSnapshot.Count("signal.late") >= 1);
        Assert.Single(ackSnapshot.Acks);
        Assert.Equal((ulong)10, ackSnapshot.Acks[0].TickId);
        Assert.InRange(state.Buffer[0], -1e-6f, 1e-6f);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task HandleSignalBatch_WhenRepeatedLate_RemainsIdempotent_AndAcksEachBatch()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var ackProbePid = root.Spawn(Props.FromProducer(static () => new SignalAckProbeActor()));
        var brainId = Guid.NewGuid();
        var shardId = ShardId32.From(regionId: 5, shardIndex: 0);
        var state = CreateState(regionId: 5, neuronStart: 0, neuronCount: 2);

        var actor = root.Spawn(Props.FromProducer(() => new RegionShardActor(
            state,
            new RegionShardActorConfig(
                BrainId: brainId,
                ShardId: shardId,
                Router: ackProbePid,
                OutputSink: null,
                TickSink: null,
                Routing: RegionShardRoutingTable.CreateSingleShard(state.RegionId, state.NeuronCount),
                DebugHub: debugProbePid,
                DebugEnabled: true,
                DebugMinSeverity: Severity.SevWarn))));

        await root.RequestAsync<TickComputeDone>(actor, new TickCompute { TickId = 21 });

        var lateBatch = new SignalBatch
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)state.RegionId,
            ShardId = shardId.ToProtoShardId32(),
            TickId = 20,
            Contribs =
            {
                new Contribution
                {
                    TargetNeuronId = 0,
                    Value = 0.9f
                }
            }
        };

        root.Send(actor, lateBatch);
        root.Send(actor, lateBatch);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var debugSnapshot = await WaitForDebugCountAsync(root, debugProbePid, "signal.late", minCount: 2, cts.Token);
        var ackSnapshot = await WaitForAckCountAsync(root, ackProbePid, minCount: 2, cts.Token);
        await root.RequestAsync<TickComputeDone>(actor, new TickCompute { TickId = 22 });

        Assert.True(debugSnapshot.Count("signal.late") >= 2);
        Assert.Equal(2, ackSnapshot.Acks.Count);
        Assert.All(ackSnapshot.Acks, ack => Assert.Equal((ulong)20, ack.TickId));
        Assert.InRange(state.Buffer[0], -1e-6f, 1e-6f);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task HandleSignalBatch_WhenLateThenCurrentTick_OnlyCurrentContributionIsApplied()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var ackProbePid = root.Spawn(Props.FromProducer(static () => new SignalAckProbeActor()));
        var brainId = Guid.NewGuid();
        var shardId = ShardId32.From(regionId: 5, shardIndex: 0);
        var state = CreateState(regionId: 5, neuronStart: 0, neuronCount: 2);

        var actor = root.Spawn(Props.FromProducer(() => new RegionShardActor(
            state,
            new RegionShardActorConfig(
                BrainId: brainId,
                ShardId: shardId,
                Router: ackProbePid,
                OutputSink: null,
                TickSink: null,
                Routing: RegionShardRoutingTable.CreateSingleShard(state.RegionId, state.NeuronCount),
                DebugHub: debugProbePid,
                DebugEnabled: true,
                DebugMinSeverity: Severity.SevWarn))));

        await root.RequestAsync<TickComputeDone>(actor, new TickCompute { TickId = 31 });

        root.Send(actor, new SignalBatch
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)state.RegionId,
            ShardId = shardId.ToProtoShardId32(),
            TickId = 30,
            Contribs =
            {
                new Contribution
                {
                    TargetNeuronId = 0,
                    Value = 0.25f
                }
            }
        });

        root.Send(actor, new SignalBatch
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)state.RegionId,
            ShardId = shardId.ToProtoShardId32(),
            TickId = 31,
            Contribs =
            {
                new Contribution
                {
                    TargetNeuronId = 0,
                    Value = 0.75f
                }
            }
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var debugSnapshot = await WaitForDebugCountAsync(root, debugProbePid, "signal.late", minCount: 1, cts.Token);
        var ackSnapshot = await WaitForAckCountAsync(root, ackProbePid, minCount: 2, cts.Token);
        await root.RequestAsync<TickComputeDone>(actor, new TickCompute { TickId = 32 });

        Assert.True(debugSnapshot.Count("signal.late") >= 1);
        Assert.Equal(2, ackSnapshot.Acks.Count);
        Assert.Contains(ackSnapshot.Acks, ack => ack.TickId == (ulong)30);
        Assert.Contains(ackSnapshot.Acks, ack => ack.TickId == (ulong)31);
        Assert.InRange(state.Buffer[0], 0.749f, 0.751f);

        await system.ShutdownAsync();
    }

    private static async Task<DebugProbeSnapshot> WaitForDebugCountAsync(
        IRootContext root,
        PID debugProbePid,
        string summary,
        int minCount,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var snapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
            if (snapshot.Count(summary) >= minCount)
            {
                return snapshot;
            }

            await Task.Delay(25, cancellationToken);
        }

        throw new TimeoutException($"Timed out waiting for summary '{summary}' to reach {minCount}.");
    }

    private static async Task<SignalAckProbeSnapshot> WaitForAckCountAsync(
        IRootContext root,
        PID ackProbePid,
        int minCount,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var snapshot = await root.RequestAsync<SignalAckProbeSnapshot>(ackProbePid, new GetSignalAckProbeSnapshot());
            if (snapshot.Acks.Count >= minCount)
            {
                return snapshot;
            }

            await Task.Delay(25, cancellationToken);
        }

        throw new TimeoutException($"Timed out waiting for SignalBatchAck count to reach {minCount}.");
    }

    private static RegionShardState CreateState(int regionId, int neuronStart, int neuronCount)
    {
        var regionSpans = new int[Nbn.Shared.NbnConstants.RegionCount];
        regionSpans[regionId] = neuronCount;
        regionSpans[Nbn.Shared.NbnConstants.OutputRegionId] = 1;

        return new RegionShardState(
            regionId: regionId,
            neuronStart: neuronStart,
            neuronCount: neuronCount,
            brainSeed: 0x0102030405060708UL,
            strengthQuantization: QuantizationSchemas.DefaultNbn.Strength,
            regionSpans: regionSpans,
            buffer: new float[neuronCount],
            enabled: Enumerable.Repeat(true, neuronCount).ToArray(),
            exists: Enumerable.Repeat(true, neuronCount).ToArray(),
            accumulationFunctions: Enumerable.Repeat((byte)AccumulationFunction.AccumSum, neuronCount).ToArray(),
            activationFunctions: Enumerable.Repeat((byte)ActivationFunction.ActNone, neuronCount).ToArray(),
            resetFunctions: Enumerable.Repeat((byte)ResetFunction.ResetHold, neuronCount).ToArray(),
            paramA: new float[neuronCount],
            paramB: new float[neuronCount],
            preActivationThreshold: Enumerable.Repeat(-1f, neuronCount).ToArray(),
            activationThreshold: Enumerable.Repeat(1f, neuronCount).ToArray(),
            axonCounts: new ushort[neuronCount],
            axonStartOffsets: new int[neuronCount],
            axons: new RegionShardAxons(
                targetRegionIds: Array.Empty<byte>(),
                targetNeuronIds: Array.Empty<int>(),
                strengths: Array.Empty<float>(),
                baseStrengthCodes: Array.Empty<byte>(),
                runtimeStrengthCodes: Array.Empty<byte>(),
                hasRuntimeOverlay: Array.Empty<bool>(),
                fromAddress32: Array.Empty<uint>(),
                toAddress32: Array.Empty<uint>()));
    }

    private sealed record GetDebugProbeSnapshot;
    private sealed record GetSignalAckProbeSnapshot;

    private sealed record DebugProbeSnapshot(IReadOnlyDictionary<string, int> Counts)
    {
        public int Count(string summary)
            => Counts.TryGetValue(summary, out var count) ? count : 0;
    }

    private sealed record SignalAckProbeSnapshot(IReadOnlyList<SignalBatchAck> Acks);

    private sealed class DebugProbeActor : IActor
    {
        private readonly Dictionary<string, int> _counts = new(StringComparer.Ordinal);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case DebugOutbound outbound:
                    var summary = outbound.Summary ?? string.Empty;
                    if (summary.Length > 0)
                    {
                        _counts[summary] = _counts.TryGetValue(summary, out var count) ? count + 1 : 1;
                    }
                    break;
                case GetDebugProbeSnapshot:
                    context.Respond(new DebugProbeSnapshot(new Dictionary<string, int>(_counts, StringComparer.Ordinal)));
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class SignalAckProbeActor : IActor
    {
        private readonly List<SignalBatchAck> _acks = new();

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case SignalBatchAck ack:
                    _acks.Add(ack);
                    break;
                case GetSignalAckProbeSnapshot:
                    context.Respond(new SignalAckProbeSnapshot(_acks.ToArray()));
                    break;
            }

            return Task.CompletedTask;
        }
    }
}
