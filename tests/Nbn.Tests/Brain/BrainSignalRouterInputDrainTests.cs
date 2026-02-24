using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Runtime.Brain;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;
using Xunit;

namespace Nbn.Tests.Brain;

public class BrainSignalRouterInputDrainTests
{
    [Fact]
    public async Task TickDeliver_Drains_Inputs_From_Registered_IoGateway()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var drainTcs = new TaskCompletionSource<InputDrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ioPid = root.Spawn(Props.FromProducer(() => new IoDrainActor(brainId, drainTcs)));

        var inputShardId = ShardId32.From(NbnConstants.InputRegionId, 0);
        var batchTcs = new TaskCompletionSource<SignalBatch>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardPid = root.Spawn(Props.FromProducer(() => new InputShardActor(brainId, inputShardId, batchTcs)));

        var router = root.Spawn(Props.FromProducer(() => new BrainSignalRouterActor(brainId)));
        root.Send(router, new SetRoutingTable(new RoutingTableSnapshot(new[]
        {
            new ShardRoute(inputShardId.Value, shardPid)
        })));

        root.Send(router, new RegisterIoGateway
        {
            BrainId = brainId.ToProtoUuid(),
            IoGatewayPid = PidLabel(ioPid)
        });

        var deliverTask = root.RequestAsync<TickDeliverDone>(router, new TickDeliver { TickId = 1 });

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var drain = await drainTcs.Task.WaitAsync(timeoutCts.Token);
        Assert.Equal((ulong)1, drain.TickId);
        Assert.Single(drain.Contribs);

        var batch = await batchTcs.Task.WaitAsync(timeoutCts.Token);
        Assert.Equal((uint)NbnConstants.InputRegionId, batch.RegionId);
        Assert.Equal((ulong)1, batch.TickId);
        Assert.Single(batch.Contribs);

        var deliverDone = await deliverTask.WaitAsync(timeoutCts.Token);
        Assert.Equal((ulong)1, deliverDone.TickId);
        Assert.True(deliverDone.DeliveredBatches > 0);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RuntimeNeuronCommands_AreForwarded_To_All_TargetRegionShards()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();
        const int targetRegionId = 9;

        var targetShardA = ShardId32.From(targetRegionId, 0);
        var targetShardB = ShardId32.From(targetRegionId, 1);

        var pulseATcs = new TaskCompletionSource<RuntimeNeuronPulse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pulseBTcs = new TaskCompletionSource<RuntimeNeuronPulse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var stateATcs = new TaskCompletionSource<RuntimeNeuronStateWrite>(TaskCreationOptions.RunContinuationsAsynchronously);
        var stateBTcs = new TaskCompletionSource<RuntimeNeuronStateWrite>(TaskCreationOptions.RunContinuationsAsynchronously);

        var shardA = root.Spawn(Props.FromProducer(() => new RuntimeShardProbeActor(brainId, pulseATcs, stateATcs)));
        var shardB = root.Spawn(Props.FromProducer(() => new RuntimeShardProbeActor(brainId, pulseBTcs, stateBTcs)));

        var router = root.Spawn(Props.FromProducer(() => new BrainSignalRouterActor(brainId)));
        root.Send(router, new SetRoutingTable(new RoutingTableSnapshot(new[]
        {
            new ShardRoute(targetShardA.Value, shardA),
            new ShardRoute(targetShardB.Value, shardB)
        })));

        root.Send(router, new RuntimeNeuronPulse
        {
            BrainId = brainId.ToProtoUuid(),
            TargetRegionId = targetRegionId,
            TargetNeuronId = 17,
            Value = 0.75f
        });

        root.Send(router, new RuntimeNeuronStateWrite
        {
            BrainId = brainId.ToProtoUuid(),
            TargetRegionId = targetRegionId,
            TargetNeuronId = 17,
            SetBuffer = true,
            BufferValue = -0.5f,
            SetAccumulator = true,
            AccumulatorValue = 1.25f
        });

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var pulseA = await pulseATcs.Task.WaitAsync(timeoutCts.Token);
        var pulseB = await pulseBTcs.Task.WaitAsync(timeoutCts.Token);
        var stateA = await stateATcs.Task.WaitAsync(timeoutCts.Token);
        var stateB = await stateBTcs.Task.WaitAsync(timeoutCts.Token);

        Assert.Equal((uint)targetRegionId, pulseA.TargetRegionId);
        Assert.Equal((uint)targetRegionId, pulseB.TargetRegionId);
        Assert.Equal((uint)17, pulseA.TargetNeuronId);
        Assert.Equal((uint)17, pulseB.TargetNeuronId);
        Assert.Equal(0.75f, pulseA.Value);
        Assert.Equal(0.75f, pulseB.Value);

        Assert.Equal((uint)targetRegionId, stateA.TargetRegionId);
        Assert.Equal((uint)targetRegionId, stateB.TargetRegionId);
        Assert.True(stateA.SetBuffer);
        Assert.True(stateA.SetAccumulator);
        Assert.Equal(-0.5f, stateA.BufferValue);
        Assert.Equal(1.25f, stateA.AccumulatorValue);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task TickDeliver_ForeignSenderForgedAck_IsIgnored()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();
        var shardId = ShardId32.From(1, 0);

        var batchTcs = new TaskCompletionSource<SignalBatch>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shard = root.Spawn(Props.FromProducer(() => new ControlledAckShardActor(brainId, shardId, batchTcs)));
        var router = root.Spawn(Props.FromProducer(() => new BrainSignalRouterActor(brainId)));
        root.Send(router, new SetRoutingTable(new RoutingTableSnapshot(new[]
        {
            new ShardRoute(shardId.Value, shard)
        })));

        root.Send(router, CreateOutboxBatch(brainId, 1, shardId, targetNeuronId: 0, value: 1f));

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var deliverTask = root.RequestAsync<TickDeliverDone>(router, new TickDeliver { TickId = 1 });
        await batchTcs.Task.WaitAsync(timeoutCts.Token);

        var foreign = root.Spawn(Props.FromProducer(() => new ManualSenderActor()));
        await root.RequestAsync<SendMessageAck>(
            foreign,
            new SendMessage(router, CreateAck(brainId, shardId, 1)));

        await AssertTaskStillPending(deliverTask, TimeSpan.FromMilliseconds(150));

        await root.RequestAsync<EmitAckAck>(
            shard,
            new EmitAck(router, CreateAck(brainId, shardId, 1)));

        var deliverDone = await deliverTask.WaitAsync(timeoutCts.Token);
        Assert.Equal((ulong)1, deliverDone.TickId);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task TickDeliver_SameIdDifferentAddressSenderAck_IsIgnored()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();
        var shardId = ShardId32.From(1, 0);
        var senderId = $"AckSender{Guid.NewGuid():N}";
        var expectedRemotePid = new PID("127.0.0.1:12041", senderId);

        var router = root.Spawn(Props.FromProducer(() => new BrainSignalRouterActor(brainId)));
        root.Send(router, new SetRoutingTable(new RoutingTableSnapshot(new[]
        {
            new ShardRoute(shardId.Value, expectedRemotePid)
        })));

        root.Send(router, CreateOutboxBatch(brainId, 1, shardId, targetNeuronId: 0, value: 1f));

        var deliverTask = root.RequestAsync<TickDeliverDone>(router, new TickDeliver { TickId = 1 });
        var forgedSender = root.SpawnNamed(Props.FromProducer(() => new ManualSenderActor()), senderId);
        await root.RequestAsync<SendMessageAck>(
            forgedSender,
            new SendMessage(router, CreateAck(brainId, shardId, 1)));

        await AssertTaskStillPending(deliverTask, TimeSpan.FromMilliseconds(150));

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task TickDeliver_ExpectedSenderWithMismatchedPayload_IsIgnored()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();
        var shardId = ShardId32.From(1, 0);

        var batchTcs = new TaskCompletionSource<SignalBatch>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shard = root.Spawn(Props.FromProducer(() => new ControlledAckShardActor(brainId, shardId, batchTcs)));
        var router = root.Spawn(Props.FromProducer(() => new BrainSignalRouterActor(brainId)));
        root.Send(router, new SetRoutingTable(new RoutingTableSnapshot(new[]
        {
            new ShardRoute(shardId.Value, shard)
        })));

        root.Send(router, CreateOutboxBatch(brainId, 1, shardId, targetNeuronId: 0, value: 1f));

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var deliverTask = root.RequestAsync<TickDeliverDone>(router, new TickDeliver { TickId = 1 });
        await batchTcs.Task.WaitAsync(timeoutCts.Token);

        await root.RequestAsync<EmitAckAck>(
            shard,
            new EmitAck(router, CreateAck(Guid.NewGuid(), shardId, 1)));

        await AssertTaskStillPending(deliverTask, TimeSpan.FromMilliseconds(150));

        await root.RequestAsync<EmitAckAck>(
            shard,
            new EmitAck(router, CreateAck(brainId, shardId, 1)));

        var deliverDone = await deliverTask.WaitAsync(timeoutCts.Token);
        Assert.Equal((ulong)1, deliverDone.TickId);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task TickDeliver_DuplicateAckFromSameShard_DoesNotDoubleCount()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();
        var shardAId = ShardId32.From(1, 0);
        var shardBId = ShardId32.From(1, 1);

        var batchATcs = new TaskCompletionSource<SignalBatch>(TaskCreationOptions.RunContinuationsAsynchronously);
        var batchBTcs = new TaskCompletionSource<SignalBatch>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardA = root.Spawn(Props.FromProducer(() => new ControlledAckShardActor(brainId, shardAId, batchATcs)));
        var shardB = root.Spawn(Props.FromProducer(() => new ControlledAckShardActor(brainId, shardBId, batchBTcs)));
        var router = root.Spawn(Props.FromProducer(() => new BrainSignalRouterActor(brainId)));
        root.Send(router, new SetRoutingTable(new RoutingTableSnapshot(new[]
        {
            new ShardRoute(shardAId.Value, shardA),
            new ShardRoute(shardBId.Value, shardB)
        })));

        root.Send(router, CreateOutboxBatch(brainId, 1, shardAId, targetNeuronId: 0, value: 1f));
        root.Send(router, CreateOutboxBatch(brainId, 1, shardBId, targetNeuronId: 0, value: 1f));

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var deliverTask = root.RequestAsync<TickDeliverDone>(router, new TickDeliver { TickId = 1 });
        await batchATcs.Task.WaitAsync(timeoutCts.Token);
        await batchBTcs.Task.WaitAsync(timeoutCts.Token);

        var shardAAck = CreateAck(brainId, shardAId, 1);
        await root.RequestAsync<EmitAckAck>(shardA, new EmitAck(router, shardAAck));
        await root.RequestAsync<EmitAckAck>(shardA, new EmitAck(router, shardAAck));

        await AssertTaskStillPending(deliverTask, TimeSpan.FromMilliseconds(150));

        await root.RequestAsync<EmitAckAck>(
            shardB,
            new EmitAck(router, CreateAck(brainId, shardBId, 1)));

        var deliverDone = await deliverTask.WaitAsync(timeoutCts.Token);
        Assert.Equal((ulong)1, deliverDone.TickId);
        Assert.Equal((uint)2, deliverDone.DeliveredBatches);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task TickDeliver_Completes_WhenExpectedShardSendersAck()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();
        var shardAId = ShardId32.From(1, 0);
        var shardBId = ShardId32.From(1, 1);

        var batchATcs = new TaskCompletionSource<SignalBatch>(TaskCreationOptions.RunContinuationsAsynchronously);
        var batchBTcs = new TaskCompletionSource<SignalBatch>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardA = root.Spawn(Props.FromProducer(() => new ControlledAckShardActor(brainId, shardAId, batchATcs)));
        var shardB = root.Spawn(Props.FromProducer(() => new ControlledAckShardActor(brainId, shardBId, batchBTcs)));
        var router = root.Spawn(Props.FromProducer(() => new BrainSignalRouterActor(brainId)));
        root.Send(router, new SetRoutingTable(new RoutingTableSnapshot(new[]
        {
            new ShardRoute(shardAId.Value, shardA),
            new ShardRoute(shardBId.Value, shardB)
        })));

        root.Send(router, CreateOutboxBatch(brainId, 1, shardAId, targetNeuronId: 0, value: 1f));
        root.Send(router, CreateOutboxBatch(brainId, 1, shardBId, targetNeuronId: 0, value: 1f));

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var deliverTask = root.RequestAsync<TickDeliverDone>(router, new TickDeliver { TickId = 1 });
        await batchATcs.Task.WaitAsync(timeoutCts.Token);
        await batchBTcs.Task.WaitAsync(timeoutCts.Token);

        await root.RequestAsync<EmitAckAck>(
            shardA,
            new EmitAck(router, CreateAck(brainId, shardAId, 1)));
        await root.RequestAsync<EmitAckAck>(
            shardB,
            new EmitAck(router, CreateAck(brainId, shardBId, 1)));

        var deliverDone = await deliverTask.WaitAsync(timeoutCts.Token);
        Assert.Equal((ulong)1, deliverDone.TickId);
        Assert.Equal((uint)2, deliverDone.DeliveredBatches);

        await system.ShutdownAsync();
    }

    private static OutboxBatch CreateOutboxBatch(Guid brainId, ulong tickId, ShardId32 destination, uint targetNeuronId, float value)
    {
        var outbox = new OutboxBatch
        {
            BrainId = brainId.ToProtoUuid(),
            TickId = tickId,
            DestRegionId = (uint)destination.RegionId,
            DestShardId = destination.ToProtoShardId32()
        };
        outbox.Contribs.Add(new Contribution
        {
            TargetNeuronId = targetNeuronId,
            Value = value
        });
        return outbox;
    }

    private static SignalBatchAck CreateAck(Guid brainId, ShardId32 shardId, ulong tickId)
        => new()
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardId = shardId.ToProtoShardId32(),
            TickId = tickId
        };

    private static async Task AssertTaskStillPending(Task task, TimeSpan timeout)
    {
        var completed = await Task.WhenAny(task, Task.Delay(timeout));
        Assert.NotSame(task, completed);
    }

    private sealed class IoDrainActor : IActor
    {
        private readonly Guid _brainId;
        private readonly TaskCompletionSource<InputDrain> _tcs;

        public IoDrainActor(Guid brainId, TaskCompletionSource<InputDrain> tcs)
        {
            _brainId = brainId;
            _tcs = tcs;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is DrainInputs request && Matches(request.BrainId))
            {
                var drain = new InputDrain
                {
                    BrainId = _brainId.ToProtoUuid(),
                    TickId = request.TickId
                };
                drain.Contribs.Add(new Contribution
                {
                    TargetNeuronId = 0,
                    Value = 1f
                });
                _tcs.TrySetResult(drain);
                context.Respond(drain);
            }

            return Task.CompletedTask;
        }

        private bool Matches(Nbn.Proto.Uuid? brainId)
            => brainId is not null && brainId.TryToGuid(out var guid) && guid == _brainId;
    }

    private sealed class InputShardActor : IActor
    {
        private readonly Guid _brainId;
        private readonly ShardId32 _shardId;
        private readonly TaskCompletionSource<SignalBatch> _tcs;

        public InputShardActor(Guid brainId, ShardId32 shardId, TaskCompletionSource<SignalBatch> tcs)
        {
            _brainId = brainId;
            _shardId = shardId;
            _tcs = tcs;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is SignalBatch batch && Matches(batch.BrainId))
            {
                _tcs.TrySetResult(batch);
                var ack = new SignalBatchAck
                {
                    BrainId = _brainId.ToProtoUuid(),
                    RegionId = (uint)_shardId.RegionId,
                    ShardId = _shardId.ToProtoShardId32(),
                    TickId = batch.TickId
                };
                if (context.Sender is not null)
                {
                    context.Request(context.Sender, ack);
                }
            }

            return Task.CompletedTask;
        }

        private bool Matches(Nbn.Proto.Uuid? brainId)
            => brainId is not null && brainId.TryToGuid(out var guid) && guid == _brainId;
    }

    private sealed record SendMessage(PID Target, object Message);
    private sealed record SendMessageAck;
    private sealed record EmitAck(PID Target, SignalBatchAck Ack);
    private sealed record EmitAckAck;

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

    private sealed class ControlledAckShardActor : IActor
    {
        private readonly Guid _brainId;
        private readonly ShardId32 _shardId;
        private readonly TaskCompletionSource<SignalBatch> _batchTcs;

        public ControlledAckShardActor(Guid brainId, ShardId32 shardId, TaskCompletionSource<SignalBatch> batchTcs)
        {
            _brainId = brainId;
            _shardId = shardId;
            _batchTcs = batchTcs;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case SignalBatch batch when Matches(batch.BrainId):
                    _batchTcs.TrySetResult(batch);
                    break;
                case EmitAck emitAck:
                    context.Request(emitAck.Target, emitAck.Ack);
                    context.Respond(new EmitAckAck());
                    break;
            }

            return Task.CompletedTask;
        }

        private bool Matches(Nbn.Proto.Uuid? brainId)
            => brainId is not null && brainId.TryToGuid(out var guid) && guid == _brainId;
    }

    private sealed class RuntimeShardProbeActor : IActor
    {
        private readonly Guid _brainId;
        private readonly TaskCompletionSource<RuntimeNeuronPulse> _pulseTcs;
        private readonly TaskCompletionSource<RuntimeNeuronStateWrite> _stateWriteTcs;

        public RuntimeShardProbeActor(
            Guid brainId,
            TaskCompletionSource<RuntimeNeuronPulse> pulseTcs,
            TaskCompletionSource<RuntimeNeuronStateWrite> stateWriteTcs)
        {
            _brainId = brainId;
            _pulseTcs = pulseTcs;
            _stateWriteTcs = stateWriteTcs;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case RuntimeNeuronPulse pulse when Matches(pulse.BrainId):
                    _pulseTcs.TrySetResult(pulse);
                    break;
                case RuntimeNeuronStateWrite stateWrite when Matches(stateWrite.BrainId):
                    _stateWriteTcs.TrySetResult(stateWrite);
                    break;
            }

            return Task.CompletedTask;
        }

        private bool Matches(Nbn.Proto.Uuid? brainId)
            => brainId is not null && brainId.TryToGuid(out var guid) && guid == _brainId;
    }

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";
}
