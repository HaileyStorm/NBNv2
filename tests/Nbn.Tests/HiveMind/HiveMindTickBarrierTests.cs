using System.Diagnostics;
using Nbn.Proto.Control;
using Nbn.Proto.Signal;
using Nbn.Runtime.Brain;
using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Nbn.Shared.HiveMind;
using Proto;
using Xunit;

namespace Nbn.Tests.HiveMind;

public class HiveMindTickBarrierTests
{
    [Fact]
    public async Task TickBarrier_Completes_When_Shards_And_Router_Ack()
    {
        var system = new ActorSystem();
        var options = new HiveMindOptions(
            BindHost: "127.0.0.1",
            Port: 0,
            AdvertisedHost: null,
            AdvertisedPort: null,
            TargetTickHz: 50f,
            MinTickHz: 10f,
            ComputeTimeoutMs: 500,
            DeliverTimeoutMs: 500,
            BackpressureDecay: 0.9f,
            BackpressureRecovery: 1.1f,
            TimeoutRescheduleThreshold: 3,
            TimeoutPauseThreshold: 6,
            RescheduleMinTicks: 10,
            RescheduleMinMinutes: 1,
            RescheduleQuietMs: 50,
            RescheduleSimulatedMs: 50,
            AutoStart: false);

        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, hiveMind)));
        var router = await WaitForSignalRouter(root, brainRoot, TimeSpan.FromSeconds(2));

        await WaitForStatus(root, hiveMind, status => status.RegisteredBrains == 1, TimeSpan.FromSeconds(2));

        var signalTcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var sink = root.Spawn(Props.FromProducer(() => new SignalSinkActor(signalTcs, expected: 2)));

        var shardA = ShardId32.From(1, 0);
        var shardB = ShardId32.From(1, 1);
        var shardPidA = root.Spawn(Props.FromProducer(() => new TestShardActor(brainId, shardA, router, sink, shardB)));
        var shardPidB = root.Spawn(Props.FromProducer(() => new TestShardActor(brainId, shardB, router, sink, shardA)));

        root.Send(hiveMind, new RegisterShard(brainId, shardA.RegionId, shardA, shardPidA));
        root.Send(hiveMind, new RegisterShard(brainId, shardB.RegionId, shardB, shardPidB));

        await WaitForRoutingTable(root, router, table => table.Count == 2, TimeSpan.FromSeconds(2));

        root.Send(hiveMind, new StartTickLoop());

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await signalTcs.Task.WaitAsync(timeoutCts.Token);
        var status = await WaitForStatus(
            root,
            hiveMind,
            s => s.LastCompletedTickId >= 1 && s.PendingCompute == 0 && s.PendingDeliver == 0,
            TimeSpan.FromSeconds(2));

        root.Send(hiveMind, new StopTickLoop());

        Assert.True(status.LastCompletedTickId >= 1);

        await system.ShutdownAsync();
    }

    private static async Task<PID> WaitForSignalRouter(IRootContext root, PID brainRoot, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            var response = await root.RequestAsync<SignalRouterResponse>(brainRoot, new GetSignalRouter());
            if (response.SignalRouter is not null)
            {
                return response.SignalRouter;
            }

            await Task.Delay(20);
        }

        throw new TimeoutException("Signal router did not become available.");
    }

    private static async Task WaitForRoutingTable(
        IRootContext root,
        PID router,
        Func<RoutingTableSnapshot, bool> predicate,
        TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            var snapshot = await root.RequestAsync<RoutingTableSnapshot>(router, new GetRoutingTable());
            if (predicate(snapshot))
            {
                return;
            }

            await Task.Delay(20);
        }

        throw new TimeoutException("Routing table did not reach expected state.");
    }

    private static async Task<HiveMindStatus> WaitForStatus(
        IRootContext root,
        PID hiveMind,
        Func<HiveMindStatus, bool> predicate,
        TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            var status = await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus());
            if (predicate(status))
            {
                return status;
            }

            await Task.Delay(20);
        }

        throw new TimeoutException("HiveMind status did not reach expected state.");
    }

    private sealed record TestSignalReceived;

    private sealed class SignalSinkActor : IActor
    {
        private readonly TaskCompletionSource<int> _tcs;
        private readonly int _expected;
        private int _count;

        public SignalSinkActor(TaskCompletionSource<int> tcs, int expected)
        {
            _tcs = tcs;
            _expected = expected;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is TestSignalReceived)
            {
                if (Interlocked.Increment(ref _count) >= _expected)
                {
                    _tcs.TrySetResult(_count);
                }
            }

            return Task.CompletedTask;
        }
    }

    private sealed class TestShardActor : IActor
    {
        private readonly Guid _brainId;
        private readonly ShardId32 _shardId;
        private readonly uint _regionId;
        private readonly PID _router;
        private readonly PID _signalSink;
        private readonly ShardId32 _destShard;

        public TestShardActor(Guid brainId, ShardId32 shardId, PID router, PID signalSink, ShardId32 destShard)
        {
            _brainId = brainId;
            _shardId = shardId;
            _regionId = (uint)shardId.RegionId;
            _router = router;
            _signalSink = signalSink;
            _destShard = destShard;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case TickCompute tick:
                    HandleTickCompute(context, tick);
                    break;
                case SignalBatch batch:
                    HandleSignalBatch(context, batch);
                    break;
            }

            return Task.CompletedTask;
        }

        private void HandleTickCompute(IContext context, TickCompute tick)
        {
            var outbox = new OutboxBatch
            {
                BrainId = _brainId.ToProtoUuid(),
                TickId = tick.TickId,
                DestRegionId = (uint)_destShard.RegionId,
                DestShardId = _destShard.ToProtoShardId32()
            };
            outbox.Contribs.Add(new Contribution { TargetNeuronId = 0, Value = 1f });
            context.Send(_router, outbox);

            var done = new TickComputeDone
            {
                TickId = tick.TickId,
                BrainId = _brainId.ToProtoUuid(),
                RegionId = _regionId,
                ShardId = _shardId.ToProtoShardId32(),
                ComputeMs = 1,
                TickCostTotal = 0,
                CostAccum = 0,
                CostActivation = 0,
                CostReset = 0,
                CostDistance = 0,
                CostRemote = 0,
                FiredCount = 0,
                OutBatches = 1,
                OutContribs = 1
            };

            var target = context.Sender ?? _router;
            if (target is not null)
            {
                context.Send(target, done);
            }
        }

        private void HandleSignalBatch(IContext context, SignalBatch batch)
        {
            var ack = new SignalBatchAck
            {
                BrainId = _brainId.ToProtoUuid(),
                RegionId = _regionId,
                ShardId = _shardId.ToProtoShardId32(),
                TickId = batch.TickId
            };

            var target = context.Sender ?? _router;
            if (target is not null)
            {
                context.Send(target, ack);
            }

            context.Send(_signalSink, new TestSignalReceived());
        }
    }
}
