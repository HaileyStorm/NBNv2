using System.Diagnostics;
using Nbn.Proto.Control;
using Nbn.Proto.Signal;
using Nbn.Runtime.Brain;
using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Nbn.Shared.HiveMind;
using ProtoControl = Nbn.Proto.Control;
using Proto;
using Xunit;

namespace Nbn.Tests.HiveMind;

public class HiveMindTickBarrierTests
{
    [Fact]
    public async Task TickBarrier_Completes_When_Shards_And_Router_Ack()
    {
        var system = new ActorSystem();
        var options = CreateOptions();

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
        var shardPidA = root.Spawn(Props.FromProducer(() => new TestShardActor(brainId, shardA, router, sink, shardB, hiveMind)));
        var shardPidB = root.Spawn(Props.FromProducer(() => new TestShardActor(brainId, shardB, router, sink, shardA, hiveMind)));

        root.Send(hiveMind, new Nbn.Proto.Control.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardA.RegionId,
            ShardIndex = (uint)shardA.ShardIndex,
            ShardPid = PidLabel(shardPidA),
            NeuronStart = 0,
            NeuronCount = 1
        });
        root.Send(hiveMind, new Nbn.Proto.Control.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardB.RegionId,
            ShardIndex = (uint)shardB.ShardIndex,
            ShardPid = PidLabel(shardPidB),
            NeuronStart = 0,
            NeuronCount = 1
        });

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

    [Fact]
    public async Task TickBarrier_Completes_With_External_Router()
    {
        var system = new ActorSystem();
        var options = CreateOptions();

        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var brainId = Guid.NewGuid();
        var router = root.Spawn(Props.FromProducer(() => new BrainSignalRouterActor(brainId)));
        var brainRoot = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, hiveMind, autoSpawnSignalRouter: false)));
        root.Send(brainRoot, new SetSignalRouter(router));

        await WaitForSignalRouter(root, brainRoot, TimeSpan.FromSeconds(2));
        root.Send(hiveMind, new Nbn.Proto.Control.RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot),
            SignalRouterPid = PidLabel(router)
        });
        await WaitForStatus(root, hiveMind, status => status.RegisteredBrains == 1, TimeSpan.FromSeconds(2));

        var signalTcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var sink = root.Spawn(Props.FromProducer(() => new SignalSinkActor(signalTcs, expected: 2)));

        var shardA = ShardId32.From(1, 0);
        var shardB = ShardId32.From(1, 1);
        var shardPidA = root.Spawn(Props.FromProducer(() => new TestShardActor(brainId, shardA, router, sink, shardB, hiveMind)));
        var shardPidB = root.Spawn(Props.FromProducer(() => new TestShardActor(brainId, shardB, router, sink, shardA, hiveMind)));

        root.Send(hiveMind, new Nbn.Proto.Control.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardA.RegionId,
            ShardIndex = (uint)shardA.ShardIndex,
            ShardPid = PidLabel(shardPidA),
            NeuronStart = 0,
            NeuronCount = 1
        });
        root.Send(hiveMind, new Nbn.Proto.Control.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardB.RegionId,
            ShardIndex = (uint)shardB.ShardIndex,
            ShardPid = PidLabel(shardPidB),
            NeuronStart = 0,
            NeuronCount = 1
        });

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

    [Fact]
    public async Task TickBarrier_ComputeTimeout_Triggers_Backpressure()
    {
        var system = new ActorSystem();
        var options = CreateOptions(
            targetTickHz: 20f,
            minTickHz: 5f,
            computeTimeoutMs: 100,
            deliverTimeoutMs: 100,
            backpressureDecay: 0.5f);

        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, hiveMind)));
        var router = await WaitForSignalRouter(root, brainRoot, TimeSpan.FromSeconds(2));

        await WaitForStatus(root, hiveMind, status => status.RegisteredBrains == 1, TimeSpan.FromSeconds(2));

        var shardId = ShardId32.From(1, 0);
        var shardPid = root.Spawn(Props.FromProducer(() => new SilentComputeShardActor(brainId, shardId, router)));
        root.Send(hiveMind, new Nbn.Proto.Control.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        });

        await WaitForRoutingTable(root, router, table => table.Count == 1, TimeSpan.FromSeconds(2));

        root.Send(hiveMind, new StartTickLoop());

        var status = await WaitForStatus(
            root,
            hiveMind,
            s => s.LastCompletedTickId >= 1 && s.TargetTickHz < options.TargetTickHz,
            TimeSpan.FromSeconds(2));

        root.Send(hiveMind, new StopTickLoop());

        Assert.True(status.TargetTickHz < options.TargetTickHz);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RegisterBrain_Normalizes_Router_Address_From_BrainRoot()
    {
        var system = new ActorSystem();
        var options = CreateOptions();

        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var brainId = Guid.NewGuid();
        var brainRootPid = new PID("127.0.0.1:12001", "BrainRoot");
        var routerPid = new PID(string.Empty, "Router");

        root.Send(hiveMind, new Nbn.Proto.Control.RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRootPid),
            SignalRouterPid = PidLabel(routerPid)
        });

        var info = await WaitForRoutingInfo(root, hiveMind, brainId, TimeSpan.FromSeconds(2));
        Assert.False(string.IsNullOrWhiteSpace(info.SignalRouterPid));
        Assert.StartsWith(brainRootPid.Address + "/", info.SignalRouterPid);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task TickBarrier_DeliverTimeout_Triggers_Backpressure()
    {
        var system = new ActorSystem();
        var options = CreateOptions(
            targetTickHz: 20f,
            minTickHz: 5f,
            computeTimeoutMs: 200,
            deliverTimeoutMs: 100,
            backpressureDecay: 0.5f);

        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, hiveMind)));
        var router = await WaitForSignalRouter(root, brainRoot, TimeSpan.FromSeconds(2));

        await WaitForStatus(root, hiveMind, status => status.RegisteredBrains == 1, TimeSpan.FromSeconds(2));

        var shardId = ShardId32.From(1, 0);
        var shardPid = root.Spawn(Props.FromProducer(() => new NoAckShardActor(brainId, shardId, router)));
        root.Send(hiveMind, new Nbn.Proto.Control.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        });

        await WaitForRoutingTable(root, router, table => table.Count == 1, TimeSpan.FromSeconds(2));

        root.Send(hiveMind, new StartTickLoop());

        var status = await WaitForStatus(
            root,
            hiveMind,
            s => s.LastCompletedTickId >= 1 && s.TargetTickHz < options.TargetTickHz,
            TimeSpan.FromSeconds(2));

        root.Send(hiveMind, new StopTickLoop());

        Assert.True(status.TargetTickHz < options.TargetTickHz);

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

    private static async Task<ProtoControl.HiveMindStatus> WaitForStatus(
        IRootContext root,
        PID hiveMind,
        Func<ProtoControl.HiveMindStatus, bool> predicate,
        TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        ProtoControl.HiveMindStatus? lastStatus = null;
        while (sw.Elapsed < timeout)
        {
            var status = await root.RequestAsync<ProtoControl.HiveMindStatus>(
                hiveMind,
                new ProtoControl.GetHiveMindStatus());
            lastStatus = status;
            if (predicate(status))
            {
                return status;
            }

            await Task.Delay(20);
        }

        throw new TimeoutException($"HiveMind status did not reach expected state. Last: brains={lastStatus?.RegisteredBrains}, shards={lastStatus?.RegisteredShards}, pendingCompute={lastStatus?.PendingCompute}, pendingDeliver={lastStatus?.PendingDeliver}.");
    }

    private static async Task<ProtoControl.BrainRoutingInfo> WaitForRoutingInfo(
        IRootContext root,
        PID hiveMind,
        Guid brainId,
        TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        ProtoControl.BrainRoutingInfo? lastInfo = null;
        while (sw.Elapsed < timeout)
        {
            var info = await root.RequestAsync<ProtoControl.BrainRoutingInfo>(
                hiveMind,
                new ProtoControl.GetBrainRouting { BrainId = brainId.ToProtoUuid() });
            lastInfo = info;
            if (!string.IsNullOrWhiteSpace(info.BrainRootPid) || !string.IsNullOrWhiteSpace(info.SignalRouterPid))
            {
                return info;
            }

            await Task.Delay(20);
        }

        throw new TimeoutException($"HiveMind routing info did not become available. Last: root={lastInfo?.BrainRootPid}, router={lastInfo?.SignalRouterPid}.");
    }

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

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
        private readonly PID? _tickSink;

        public TestShardActor(Guid brainId, ShardId32 shardId, PID router, PID signalSink, ShardId32 destShard, PID? tickSink = null)
        {
            _brainId = brainId;
            _shardId = shardId;
            _regionId = (uint)shardId.RegionId;
            _router = router;
            _signalSink = signalSink;
            _destShard = destShard;
            _tickSink = tickSink;
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

            var target = _tickSink ?? context.Sender ?? _router;
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

    private sealed class SilentComputeShardActor : IActor
    {
        private readonly Guid _brainId;
        private readonly ShardId32 _shardId;
        private readonly uint _regionId;
        private readonly PID _router;

        public SilentComputeShardActor(Guid brainId, ShardId32 shardId, PID router)
        {
            _brainId = brainId;
            _shardId = shardId;
            _regionId = (uint)shardId.RegionId;
            _router = router;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is SignalBatch batch)
            {
                var ack = new SignalBatchAck
                {
                    BrainId = _brainId.ToProtoUuid(),
                    RegionId = _regionId,
                    ShardId = _shardId.ToProtoShardId32(),
                    TickId = batch.TickId
                };

                context.Send(context.Sender ?? _router, ack);
            }

            return Task.CompletedTask;
        }
    }

    private sealed class NoAckShardActor : IActor
    {
        private readonly Guid _brainId;
        private readonly ShardId32 _shardId;
        private readonly uint _regionId;
        private readonly PID _router;

        public NoAckShardActor(Guid brainId, ShardId32 shardId, PID router)
        {
            _brainId = brainId;
            _shardId = shardId;
            _regionId = (uint)shardId.RegionId;
            _router = router;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is TickCompute tick)
            {
                var outbox = new OutboxBatch
                {
                    BrainId = _brainId.ToProtoUuid(),
                    TickId = tick.TickId,
                    DestRegionId = _regionId,
                    DestShardId = _shardId.ToProtoShardId32()
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

                context.Send(context.Sender ?? _router, done);
            }

            return Task.CompletedTask;
        }
    }

    private static HiveMindOptions CreateOptions(
        float targetTickHz = 50f,
        float minTickHz = 10f,
        int computeTimeoutMs = 500,
        int deliverTimeoutMs = 500,
        float backpressureDecay = 0.9f,
        float backpressureRecovery = 1.1f,
        int lateBackpressureThreshold = 2)
        => new(
            BindHost: "127.0.0.1",
            Port: 0,
            AdvertisedHost: null,
            AdvertisedPort: null,
            TargetTickHz: targetTickHz,
            MinTickHz: minTickHz,
            ComputeTimeoutMs: computeTimeoutMs,
            DeliverTimeoutMs: deliverTimeoutMs,
            BackpressureDecay: backpressureDecay,
            BackpressureRecovery: backpressureRecovery,
            LateBackpressureThreshold: lateBackpressureThreshold,
            TimeoutRescheduleThreshold: 3,
            TimeoutPauseThreshold: 6,
            RescheduleMinTicks: 10,
            RescheduleMinMinutes: 1,
            RescheduleQuietMs: 50,
            RescheduleSimulatedMs: 50,
            AutoStart: false,
            EnableOpenTelemetry: false,
            EnableOtelMetrics: false,
            EnableOtelTraces: false,
            EnableOtelConsoleExporter: false,
            OtlpEndpoint: null,
            ServiceName: "nbn.hivemind.tests",
            SettingsDbPath: null,
            SettingsHost: null,
            SettingsPort: 0,
            SettingsName: "SettingsMonitor",
            IoAddress: null,
            IoName: null);
}
