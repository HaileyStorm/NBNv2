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
    public async Task TickBarrier_Completes_When_ControllerAndShardUseRequestSenders()
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

        await root.RequestAsync<SendMessageAck>(shardPidA, new SendMessage(hiveMind, new Nbn.Proto.Control.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardA.RegionId,
            ShardIndex = (uint)shardA.ShardIndex,
            ShardPid = PidLabel(shardPidA),
            NeuronStart = 0,
            NeuronCount = 1
        }));
        await root.RequestAsync<SendMessageAck>(shardPidB, new SendMessage(hiveMind, new Nbn.Proto.Control.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardB.RegionId,
            ShardIndex = (uint)shardB.ShardIndex,
            ShardPid = PidLabel(shardPidB),
            NeuronStart = 0,
            NeuronCount = 1
        }));

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
        Assert.Equal(options.TargetTickHz, status.TargetTickHz, 3);
        Assert.False(status.RescheduleInProgress);

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
        await WaitForStatus(root, hiveMind, status => status.RegisteredBrains == 1, TimeSpan.FromSeconds(2));

        var signalTcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var sink = root.Spawn(Props.FromProducer(() => new SignalSinkActor(signalTcs, expected: 2)));

        var shardA = ShardId32.From(1, 0);
        var shardB = ShardId32.From(1, 1);
        var shardPidA = root.Spawn(Props.FromProducer(() => new TestShardActor(brainId, shardA, router, sink, shardB, hiveMind)));
        var shardPidB = root.Spawn(Props.FromProducer(() => new TestShardActor(brainId, shardB, router, sink, shardA, hiveMind)));

        await root.RequestAsync<SendMessageAck>(shardPidA, new SendMessage(hiveMind, new Nbn.Proto.Control.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardA.RegionId,
            ShardIndex = (uint)shardA.ShardIndex,
            ShardPid = PidLabel(shardPidA),
            NeuronStart = 0,
            NeuronCount = 1
        }));
        await root.RequestAsync<SendMessageAck>(shardPidB, new SendMessage(hiveMind, new Nbn.Proto.Control.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardB.RegionId,
            ShardIndex = (uint)shardB.ShardIndex,
            ShardPid = PidLabel(shardPidB),
            NeuronStart = 0,
            NeuronCount = 1
        }));

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
        await root.RequestAsync<SendMessageAck>(shardPid, new SendMessage(hiveMind, new Nbn.Proto.Control.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

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
        var bootstrapSender = root.SpawnNamed(Props.FromProducer(() => new ManualSenderActor()), "BrainRootBootstrap");
        var brainRootPid = new PID(string.Empty, bootstrapSender.Id);
        var routerPid = new PID(string.Empty, "Router");
        await root.RequestAsync<SendMessageAck>(bootstrapSender, new SendMessage(hiveMind, new Nbn.Proto.Control.RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRootPid),
            SignalRouterPid = PidLabel(routerPid)
        }));

        var info = await WaitForRoutingInfo(root, hiveMind, brainId, TimeSpan.FromSeconds(2));
        Assert.False(string.IsNullOrWhiteSpace(info.SignalRouterPid));
        Assert.EndsWith(routerPid.Id, info.SignalRouterPid, StringComparison.Ordinal);

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
        var shardPid = root.Spawn(Props.FromProducer(() => new NoAckShardActor(brainId, shardId, router, hiveMind)));
        await root.RequestAsync<SendMessageAck>(shardPid, new SendMessage(hiveMind, new Nbn.Proto.Control.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

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
    public async Task TickBarrier_ComputeDone_ForeignSenderWithValidPayload_IsIgnored()
    {
        var system = new ActorSystem();
        var options = CreateOptions(computeTimeoutMs: 2000, deliverTimeoutMs: 2000);

        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, hiveMind)));
        var router = await WaitForSignalRouter(root, brainRoot, TimeSpan.FromSeconds(2));
        await WaitForStatus(root, hiveMind, status => status.RegisteredBrains == 1, TimeSpan.FromSeconds(2));

        var shardId = ShardId32.From(1, 0);
        var shardSender = root.Spawn(Props.FromProducer(() => new ManualSenderActor()));
        await root.RequestAsync<SendMessageAck>(shardSender, new SendMessage(hiveMind, new ProtoControl.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardSender),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        await WaitForRoutingTable(root, router, table => table.Count == 1, TimeSpan.FromSeconds(2));

        root.Send(hiveMind, new StartTickLoop());
        await WaitForStatus(
            root,
            hiveMind,
            s => s.LastCompletedTickId == 0 && s.PendingCompute == 1 && s.PendingDeliver == 0,
            TimeSpan.FromSeconds(2));

        var foreignSender = root.Spawn(Props.FromProducer(() => new ManualSenderActor()));
        var forgedDone = new TickComputeDone
        {
            TickId = 1,
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardId = shardId.ToProtoShardId32(),
            ComputeMs = 1
        };

        await root.RequestAsync<SendMessageAck>(foreignSender, new SendMessage(hiveMind, forgedDone));
        await Task.Delay(100);
        await AssertBarrierStillWaiting(
            root,
            hiveMind,
            options,
            expectedPendingCompute: 1,
            expectedPendingDeliver: 0);

        root.Send(hiveMind, new StopTickLoop());
        await system.ShutdownAsync();
    }

    [Fact]
    public async Task TickBarrier_ComputeDone_SenderlessRegisterShardPoisoning_And_ForgedDone_IsIgnored()
    {
        var system = new ActorSystem();
        var options = CreateOptions(computeTimeoutMs: 2000, deliverTimeoutMs: 2000);

        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, hiveMind)));
        var router = await WaitForSignalRouter(root, brainRoot, TimeSpan.FromSeconds(2));
        await WaitForStatus(root, hiveMind, status => status.RegisteredBrains == 1, TimeSpan.FromSeconds(2));

        var shardId = ShardId32.From(1, 0);
        var shardSender = root.Spawn(Props.FromProducer(() => new ManualSenderActor()));
        await root.RequestAsync<SendMessageAck>(shardSender, new SendMessage(hiveMind, new ProtoControl.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardSender),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        await WaitForRoutingTable(root, router, table => table.Count == 1, TimeSpan.FromSeconds(2));

        root.Send(hiveMind, new StartTickLoop());
        await WaitForStatus(
            root,
            hiveMind,
            s => s.LastCompletedTickId == 0 && s.PendingCompute == 1 && s.PendingDeliver == 0,
            TimeSpan.FromSeconds(2));

        var forgedSender = root.Spawn(Props.FromProducer(() => new ManualSenderActor()));
        root.Send(hiveMind, new ProtoControl.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(forgedSender),
            NeuronStart = 0,
            NeuronCount = 1
        });
        var forgedDone = new TickComputeDone
        {
            TickId = 1,
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardId = shardId.ToProtoShardId32(),
            ComputeMs = 1
        };

        await root.RequestAsync<SendMessageAck>(forgedSender, new SendMessage(hiveMind, forgedDone));
        await Task.Delay(100);
        await AssertBarrierStillWaiting(
            root,
            hiveMind,
            options,
            expectedPendingCompute: 1,
            expectedPendingDeliver: 0);

        root.Send(hiveMind, new StopTickLoop());
        await system.ShutdownAsync();
    }

    [Fact]
    public async Task TickBarrier_ComputeDone_RealShardSenderWithMismatchedPayload_IsIgnored()
    {
        var system = new ActorSystem();
        var options = CreateOptions(computeTimeoutMs: 2000, deliverTimeoutMs: 2000);

        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, hiveMind)));
        var router = await WaitForSignalRouter(root, brainRoot, TimeSpan.FromSeconds(2));
        await WaitForStatus(root, hiveMind, status => status.RegisteredBrains == 1, TimeSpan.FromSeconds(2));

        var shardId = ShardId32.From(1, 0);
        var shardSender = root.Spawn(Props.FromProducer(() => new ManualSenderActor()));
        await root.RequestAsync<SendMessageAck>(shardSender, new SendMessage(hiveMind, new ProtoControl.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardSender),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        await WaitForRoutingTable(root, router, table => table.Count == 1, TimeSpan.FromSeconds(2));

        root.Send(hiveMind, new StartTickLoop());
        await WaitForStatus(
            root,
            hiveMind,
            s => s.LastCompletedTickId == 0 && s.PendingCompute == 1 && s.PendingDeliver == 0,
            TimeSpan.FromSeconds(2));

        var forgedDone = new TickComputeDone
        {
            TickId = 1,
            BrainId = Guid.NewGuid().ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardId = shardId.ToProtoShardId32(),
            ComputeMs = 1
        };

        await root.RequestAsync<SendMessageAck>(shardSender, new SendMessage(hiveMind, forgedDone));
        await Task.Delay(100);
        await AssertBarrierStillWaiting(
            root,
            hiveMind,
            options,
            expectedPendingCompute: 1,
            expectedPendingDeliver: 0);

        root.Send(hiveMind, new StopTickLoop());
        await system.ShutdownAsync();
    }

    [Fact]
    public async Task TickBarrier_DeliverDone_ForeignSenderWithValidPayload_IsIgnored()
    {
        var system = new ActorSystem();
        var options = CreateOptions(computeTimeoutMs: 2000, deliverTimeoutMs: 2000);

        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, hiveMind)));
        var router = await WaitForSignalRouter(root, brainRoot, TimeSpan.FromSeconds(2));
        await WaitForStatus(root, hiveMind, status => status.RegisteredBrains == 1, TimeSpan.FromSeconds(2));

        var shardId = ShardId32.From(1, 0);
        var shardPid = root.Spawn(Props.FromProducer(() => new NoAckShardActor(brainId, shardId, router, hiveMind)));
        await root.RequestAsync<SendMessageAck>(shardPid, new SendMessage(hiveMind, new ProtoControl.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        await WaitForRoutingTable(root, router, table => table.Count == 1, TimeSpan.FromSeconds(2));

        root.Send(hiveMind, new StartTickLoop());
        await WaitForStatus(
            root,
            hiveMind,
            s => s.LastCompletedTickId == 0 && s.PendingCompute == 0 && s.PendingDeliver == 1,
            TimeSpan.FromSeconds(2));

        var foreignSender = root.Spawn(Props.FromProducer(() => new ManualSenderActor()));
        var forgedDone = new TickDeliverDone
        {
            TickId = 1,
            BrainId = brainId.ToProtoUuid(),
            DeliverMs = 1,
            DeliveredBatches = 0,
            DeliveredContribs = 0
        };

        await root.RequestAsync<SendMessageAck>(foreignSender, new SendMessage(hiveMind, forgedDone));
        await Task.Delay(100);
        await AssertBarrierStillWaiting(
            root,
            hiveMind,
            options,
            expectedPendingCompute: 0,
            expectedPendingDeliver: 1);

        root.Send(hiveMind, new StopTickLoop());
        await system.ShutdownAsync();
    }

    [Fact]
    public async Task TickBarrier_DeliverDone_RealControllerSenderWithMismatchedPayload_IsIgnored()
    {
        var system = new ActorSystem();
        var options = CreateOptions(computeTimeoutMs: 2000, deliverTimeoutMs: 2000);

        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, hiveMind)));
        var router = await WaitForSignalRouter(root, brainRoot, TimeSpan.FromSeconds(2));
        await WaitForStatus(root, hiveMind, status => status.RegisteredBrains == 1, TimeSpan.FromSeconds(2));

        var shardId = ShardId32.From(1, 0);
        var shardPid = root.Spawn(Props.FromProducer(() => new NoAckShardActor(brainId, shardId, router, hiveMind)));
        await root.RequestAsync<SendMessageAck>(shardPid, new SendMessage(hiveMind, new ProtoControl.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        await WaitForRoutingTable(root, router, table => table.Count == 1, TimeSpan.FromSeconds(2));

        root.Send(hiveMind, new StartTickLoop());
        await WaitForStatus(
            root,
            hiveMind,
            s => s.LastCompletedTickId == 0 && s.PendingCompute == 0 && s.PendingDeliver == 1,
            TimeSpan.FromSeconds(2));

        root.Send(brainRoot, new TickDeliverDone
        {
            TickId = 1,
            BrainId = Guid.NewGuid().ToProtoUuid(),
            DeliverMs = 1,
            DeliveredBatches = 0,
            DeliveredContribs = 0
        });

        await Task.Delay(100);
        await AssertBarrierStillWaiting(
            root,
            hiveMind,
            options,
            expectedPendingCompute: 0,
            expectedPendingDeliver: 1);

        root.Send(hiveMind, new StopTickLoop());
        await system.ShutdownAsync();
    }

    [Fact]
    public async Task TickBarrier_ForgedRouterSignalAck_DoesNotCompleteDeliverBarrier()
    {
        var system = new ActorSystem();
        var options = CreateOptions(computeTimeoutMs: 2000, deliverTimeoutMs: 2000);

        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, hiveMind)));
        var router = await WaitForSignalRouter(root, brainRoot, TimeSpan.FromSeconds(2));
        await WaitForStatus(root, hiveMind, status => status.RegisteredBrains == 1, TimeSpan.FromSeconds(2));

        var shardId = ShardId32.From(1, 0);
        var shardPid = root.Spawn(Props.FromProducer(() => new NoAckShardActor(brainId, shardId, router, hiveMind)));
        await root.RequestAsync<SendMessageAck>(shardPid, new SendMessage(hiveMind, new ProtoControl.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        await WaitForRoutingTable(root, router, table => table.Count == 1, TimeSpan.FromSeconds(2));

        root.Send(hiveMind, new StartTickLoop());
        await WaitForStatus(
            root,
            hiveMind,
            s => s.LastCompletedTickId == 0 && s.PendingCompute == 0 && s.PendingDeliver == 1,
            TimeSpan.FromSeconds(2));

        var foreign = root.Spawn(Props.FromProducer(() => new ManualSenderActor()));
        var forgedAck = new SignalBatchAck
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardId = shardId.ToProtoShardId32(),
            TickId = 1
        };

        await root.RequestAsync<SendMessageAck>(foreign, new SendMessage(router, forgedAck));
        await Task.Delay(100);
        await AssertBarrierStillWaiting(
            root,
            hiveMind,
            options,
            expectedPendingCompute: 0,
            expectedPendingDeliver: 1);

        root.Send(hiveMind, new StopTickLoop());
        await system.ShutdownAsync();
    }

    [Fact]
    public async Task TickBarrier_ComputeDone_SenderlessRegisterShardOverwrite_And_ForgedDone_IsIgnored()
    {
        var system = new ActorSystem();
        var options = CreateOptions(computeTimeoutMs: 2000, deliverTimeoutMs: 2000);

        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, hiveMind)));
        var router = await WaitForSignalRouter(root, brainRoot, TimeSpan.FromSeconds(2));
        await WaitForStatus(root, hiveMind, status => status.RegisteredBrains == 1, TimeSpan.FromSeconds(2));

        var shardId = ShardId32.From(1, 0);
        var shardSender = root.Spawn(Props.FromProducer(() => new ManualSenderActor()));
        await root.RequestAsync<SendMessageAck>(shardSender, new SendMessage(hiveMind, new ProtoControl.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardSender),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        await WaitForRoutingTable(root, router, table => table.Count == 1, TimeSpan.FromSeconds(2));

        root.Send(hiveMind, new StartTickLoop());
        await WaitForStatus(
            root,
            hiveMind,
            s => s.LastCompletedTickId == 0 && s.PendingCompute == 1 && s.PendingDeliver == 0,
            TimeSpan.FromSeconds(2));

        var foreignSender = root.Spawn(Props.FromProducer(() => new ManualSenderActor()));
        root.Send(hiveMind, new ProtoControl.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(foreignSender),
            NeuronStart = 0,
            NeuronCount = 1
        });

        await root.RequestAsync<SendMessageAck>(foreignSender, new SendMessage(hiveMind, new TickComputeDone
        {
            TickId = 1,
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardId = shardId.ToProtoShardId32(),
            ComputeMs = 1
        }));

        await Task.Delay(100);
        await AssertBarrierStillWaiting(
            root,
            hiveMind,
            options,
            expectedPendingCompute: 1,
            expectedPendingDeliver: 0);

        root.Send(hiveMind, new StopTickLoop());
        await system.ShutdownAsync();
    }

    [Fact]
    public async Task TickBarrier_DeliverDone_SenderlessControllerPoisoning_And_ForgedDone_IsIgnored()
    {
        var system = new ActorSystem();
        var options = CreateOptions(computeTimeoutMs: 2000, deliverTimeoutMs: 2000);

        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, hiveMind)));
        var router = await WaitForSignalRouter(root, brainRoot, TimeSpan.FromSeconds(2));
        await WaitForStatus(root, hiveMind, status => status.RegisteredBrains == 1, TimeSpan.FromSeconds(2));

        var shardId = ShardId32.From(1, 0);
        var shardSender = root.Spawn(Props.FromProducer(() => new ManualSenderActor()));
        await root.RequestAsync<SendMessageAck>(shardSender, new SendMessage(hiveMind, new ProtoControl.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardSender),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        await WaitForRoutingTable(root, router, table => table.Count == 1, TimeSpan.FromSeconds(2));

        root.Send(hiveMind, new StartTickLoop());
        await WaitForStatus(
            root,
            hiveMind,
            s => s.LastCompletedTickId == 0 && s.PendingCompute == 1 && s.PendingDeliver == 0,
            TimeSpan.FromSeconds(2));

        var foreignSender = root.Spawn(Props.FromProducer(() => new ManualSenderActor()));
        root.Send(hiveMind, new ProtoControl.RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(foreignSender),
            SignalRouterPid = PidLabel(foreignSender)
        });
        root.Send(hiveMind, new ProtoControl.UpdateBrainSignalRouter
        {
            BrainId = brainId.ToProtoUuid(),
            SignalRouterPid = PidLabel(foreignSender)
        });

        await root.RequestAsync<SendMessageAck>(shardSender, new SendMessage(router, new OutboxBatch
        {
            BrainId = brainId.ToProtoUuid(),
            TickId = 1,
            DestRegionId = (uint)shardId.RegionId,
            DestShardId = shardId.ToProtoShardId32(),
            Contribs = { new Contribution { TargetNeuronId = 0, Value = 1f } }
        }));

        await root.RequestAsync<SendMessageAck>(shardSender, new SendMessage(hiveMind, new TickComputeDone
        {
            TickId = 1,
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardId = shardId.ToProtoShardId32(),
            ComputeMs = 1
        }));

        await WaitForStatus(
            root,
            hiveMind,
            s => s.LastCompletedTickId == 0 && s.PendingCompute == 0 && s.PendingDeliver == 1,
            TimeSpan.FromSeconds(2));

        await root.RequestAsync<SendMessageAck>(foreignSender, new SendMessage(hiveMind, new TickDeliverDone
        {
            TickId = 1,
            BrainId = brainId.ToProtoUuid(),
            DeliverMs = 1,
            DeliveredBatches = 0,
            DeliveredContribs = 0
        }));

        await Task.Delay(100);
        await AssertBarrierStillWaiting(
            root,
            hiveMind,
            options,
            expectedPendingCompute: 0,
            expectedPendingDeliver: 1);

        root.Send(hiveMind, new StopTickLoop());
        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ControlPlane_Legitimate_ControllerRouterUpdate_IsAccepted()
    {
        var system = new ActorSystem();
        var options = CreateOptions();

        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, hiveMind, autoSpawnSignalRouter: false)));

        _ = await WaitForRoutingInfo(root, hiveMind, brainId, TimeSpan.FromSeconds(2));

        var externalRouter = root.Spawn(Props.FromProducer(() => new BrainSignalRouterActor(brainId)));
        root.Send(brainRoot, new SetSignalRouter(externalRouter));

        var updatedInfo = await WaitForRoutingInfo(
            root,
            hiveMind,
            brainId,
            info => !string.IsNullOrWhiteSpace(info.BrainRootPid)
                    && !string.IsNullOrWhiteSpace(info.SignalRouterPid)
                    && info.SignalRouterPid.EndsWith(externalRouter.Id, StringComparison.Ordinal),
            TimeSpan.FromSeconds(2));

        Assert.False(string.IsNullOrWhiteSpace(updatedInfo.BrainRootPid));
        Assert.False(string.IsNullOrWhiteSpace(updatedInfo.SignalRouterPid));
        Assert.EndsWith(externalRouter.Id, updatedInfo.SignalRouterPid, StringComparison.Ordinal);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task TickRateOverride_Request_UpdatesStatus_And_Clears()
    {
        var system = new ActorSystem();
        var options = CreateOptions(targetTickHz: 30f, minTickHz: 5f);
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var setAck = await root.RequestAsync<ProtoControl.SetTickRateOverrideAck>(
            hiveMind,
            new ProtoControl.SetTickRateOverride { TargetTickHz = 12.5f });

        Assert.True(setAck.Accepted);
        Assert.True(setAck.HasOverride);
        Assert.Equal(12.5f, setAck.OverrideTickHz, 3);
        Assert.Equal(12.5f, setAck.TargetTickHz, 3);

        var overriddenStatus = await root.RequestAsync<ProtoControl.HiveMindStatus>(
            hiveMind,
            new ProtoControl.GetHiveMindStatus());
        Assert.True(overriddenStatus.HasTickRateOverride);
        Assert.Equal(12.5f, overriddenStatus.TickRateOverrideHz, 3);
        Assert.Equal(12.5f, overriddenStatus.TargetTickHz, 3);

        var clearAck = await root.RequestAsync<ProtoControl.SetTickRateOverrideAck>(
            hiveMind,
            new ProtoControl.SetTickRateOverride { ClearOverride = true });

        Assert.True(clearAck.Accepted);
        Assert.False(clearAck.HasOverride);
        Assert.True(clearAck.TargetTickHz > 0f);
        Assert.True(clearAck.TargetTickHz <= options.TargetTickHz);

        var clearedStatus = await root.RequestAsync<ProtoControl.HiveMindStatus>(
            hiveMind,
            new ProtoControl.GetHiveMindStatus());
        Assert.False(clearedStatus.HasTickRateOverride);
        Assert.Equal(0f, clearedStatus.TickRateOverrideHz, 3);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task TickRateOverride_Request_Rejects_NonPositiveValues()
    {
        var system = new ActorSystem();
        var options = CreateOptions(targetTickHz: 30f, minTickHz: 5f);
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(options)));

        var ack = await root.RequestAsync<ProtoControl.SetTickRateOverrideAck>(
            hiveMind,
            new ProtoControl.SetTickRateOverride { TargetTickHz = 0f });

        Assert.False(ack.Accepted);
        Assert.False(ack.HasOverride);
        Assert.Contains("greater than zero", ack.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(options.TargetTickHz, ack.TargetTickHz, 3);

        var status = await root.RequestAsync<ProtoControl.HiveMindStatus>(
            hiveMind,
            new ProtoControl.GetHiveMindStatus());
        Assert.False(status.HasTickRateOverride);
        Assert.Equal(options.TargetTickHz, status.TargetTickHz, 3);

        await system.ShutdownAsync();
    }

    private static async Task AssertBarrierStillWaiting(
        IRootContext root,
        PID hiveMind,
        HiveMindOptions options,
        uint expectedPendingCompute,
        uint expectedPendingDeliver)
    {
        var status = await root.RequestAsync<ProtoControl.HiveMindStatus>(
            hiveMind,
            new ProtoControl.GetHiveMindStatus());

        Assert.Equal((ulong)0, status.LastCompletedTickId);
        Assert.Equal(expectedPendingCompute, status.PendingCompute);
        Assert.Equal(expectedPendingDeliver, status.PendingDeliver);
        Assert.Equal(options.TargetTickHz, status.TargetTickHz, 3);
        Assert.False(status.RescheduleInProgress);
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
        => await WaitForRoutingInfo(
            root,
            hiveMind,
            brainId,
            info => !string.IsNullOrWhiteSpace(info.BrainRootPid) || !string.IsNullOrWhiteSpace(info.SignalRouterPid),
            timeout);

    private static async Task<ProtoControl.BrainRoutingInfo> WaitForRoutingInfo(
        IRootContext root,
        PID hiveMind,
        Guid brainId,
        Func<ProtoControl.BrainRoutingInfo, bool> predicate,
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
            if (predicate(info))
            {
                return info;
            }

            await Task.Delay(20);
        }

        throw new TimeoutException($"HiveMind routing info did not become available. Last: root={lastInfo?.BrainRootPid}, router={lastInfo?.SignalRouterPid}.");
    }

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private sealed record SendMessage(PID Target, object Message);
    private sealed record SendMessageAck;
    private sealed record TestSignalReceived;

    private sealed class ManualSenderActor : IActor
    {
        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is SendMessage send)
            {
                context.Request(send.Target, send.Message);
                context.Respond(new SendMessageAck());
            }

            return Task.CompletedTask;
        }
    }

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
                case SendMessage send:
                    context.Request(send.Target, send.Message);
                    context.Respond(new SendMessageAck());
                    break;
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
                context.Request(target, done);
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
                context.Request(target, ack);
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
            if (context.Message is SendMessage send)
            {
                context.Request(send.Target, send.Message);
                context.Respond(new SendMessageAck());
            }
            else if (context.Message is SignalBatch batch)
            {
                var ack = new SignalBatchAck
                {
                    BrainId = _brainId.ToProtoUuid(),
                    RegionId = _regionId,
                    ShardId = _shardId.ToProtoShardId32(),
                    TickId = batch.TickId
                };

                context.Request(context.Sender ?? _router, ack);
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
        private readonly PID? _tickSink;

        public NoAckShardActor(Guid brainId, ShardId32 shardId, PID router, PID? tickSink = null)
        {
            _brainId = brainId;
            _shardId = shardId;
            _regionId = (uint)shardId.RegionId;
            _router = router;
            _tickSink = tickSink;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is SendMessage send)
            {
                context.Request(send.Target, send.Message);
                context.Respond(new SendMessageAck());
            }
            else if (context.Message is TickCompute tick)
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

                var target = _tickSink ?? context.Sender ?? _router;
                if (target is not null)
                {
                    context.Request(target, done);
                }
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
