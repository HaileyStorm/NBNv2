using System.Diagnostics;
using Nbn.Proto.Control;
using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;
using ProtoIo = Nbn.Proto.Io;
using Xunit;

namespace Nbn.Tests.HiveMind;

public class HiveMindOutputSinkTests
{
    [Fact]
    public async Task RegisterOutputSink_Updates_Output_Shard()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        root.Send(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        });

        var outputSink = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        var tcs = new TaskCompletionSource<UpdateShardOutputSink>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardId = ShardId32.From(NbnConstants.OutputRegionId, 0);
        var shardPid = root.Spawn(Props.FromProducer(() => new OutputSinkProbe(shardId, tcs)));

        root.Send(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        });

        root.Send(hiveMind, new RegisterOutputSink
        {
            BrainId = brainId.ToProtoUuid(),
            OutputPid = PidLabel(outputSink)
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var update = await tcs.Task.WaitAsync(cts.Token);

        Assert.Equal((uint)shardId.RegionId, update.RegionId);
        Assert.Equal((uint)shardId.ShardIndex, update.ShardIndex);
        Assert.Equal(PidLabel(outputSink), update.OutputPid);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task OutputShard_Registers_After_OutputSink()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        root.Send(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        });

        var outputSink = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        root.Send(hiveMind, new RegisterOutputSink
        {
            BrainId = brainId.ToProtoUuid(),
            OutputPid = PidLabel(outputSink)
        });

        var tcs = new TaskCompletionSource<UpdateShardOutputSink>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardId = ShardId32.From(NbnConstants.OutputRegionId, 0);
        var shardPid = root.Spawn(Props.FromProducer(() => new OutputSinkProbe(shardId, tcs)));

        root.Send(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var update = await tcs.Task.WaitAsync(cts.Token);

        Assert.Equal((uint)shardId.RegionId, update.RegionId);
        Assert.Equal((uint)shardId.ShardIndex, update.ShardIndex);
        Assert.Equal(PidLabel(outputSink), update.OutputPid);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task SetBrainVisualization_Updates_Shards_With_FocusScope()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        root.Send(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        });

        var firstUpdate = new TaskCompletionSource<UpdateShardVisualization>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondUpdate = new TaskCompletionSource<UpdateShardVisualization>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardId = ShardId32.From(13, 0);
        var shardPid = root.Spawn(Props.FromProducer(() => new VisualizationProbe(shardId, firstUpdate, secondUpdate)));

        root.Send(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var initial = await firstUpdate.Task.WaitAsync(cts.Token);
        Assert.False(initial.Enabled);
        Assert.False(initial.HasFocusRegion);

        root.Send(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = true,
            HasFocusRegion = true,
            FocusRegionId = 13
        });

        var focused = await secondUpdate.Task.WaitAsync(cts.Token);
        Assert.True(focused.Enabled);
        Assert.True(focused.HasFocusRegion);
        Assert.Equal<uint>(13, focused.FocusRegionId);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RuntimeConfig_Updates_ActiveShard_And_NewShard()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        root.Send(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        });

        var shardA = ShardId32.From(13, 0);
        var initialA = new TaskCompletionSource<UpdateShardRuntimeConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        var configuredA = new TaskCompletionSource<UpdateShardRuntimeConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardAPid = root.Spawn(Props.FromProducer(() => new RuntimeConfigProbe(
            shardA,
            initialA,
            _ => true,
            configuredA,
            update => update.CostEnabled
                      && update.EnergyEnabled
                      && update.PlasticityEnabled
                      && Math.Abs(update.PlasticityRate - 0.2f) < 0.000001f
                      && !update.ProbabilisticUpdates)));

        root.Send(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardA.RegionId,
            ShardIndex = (uint)shardA.ShardIndex,
            ShardPid = PidLabel(shardAPid),
            NeuronStart = 0,
            NeuronCount = 1
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var initialUpdate = await initialA.Task.WaitAsync(cts.Token);
        Assert.False(initialUpdate.CostEnabled);
        Assert.False(initialUpdate.EnergyEnabled);
        Assert.False(initialUpdate.PlasticityEnabled);

        root.Send(hiveMind, new SetBrainCostEnergy
        {
            BrainId = brainId.ToProtoUuid(),
            CostEnabled = true,
            EnergyEnabled = true
        });

        root.Send(hiveMind, new SetBrainPlasticity
        {
            BrainId = brainId.ToProtoUuid(),
            PlasticityEnabled = true,
            PlasticityRate = 0.2f,
            ProbabilisticUpdates = false
        });

        var configuredUpdate = await configuredA.Task.WaitAsync(cts.Token);
        Assert.True(configuredUpdate.CostEnabled);
        Assert.True(configuredUpdate.EnergyEnabled);
        Assert.True(configuredUpdate.PlasticityEnabled);
        Assert.Equal(0.2f, configuredUpdate.PlasticityRate);
        Assert.False(configuredUpdate.ProbabilisticUpdates);

        var shardB = ShardId32.From(14, 0);
        var configuredB = new TaskCompletionSource<UpdateShardRuntimeConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardBPid = root.Spawn(Props.FromProducer(() => new RuntimeConfigProbe(
            shardB,
            configuredB,
            update => update.CostEnabled
                      && update.EnergyEnabled
                      && update.PlasticityEnabled
                      && Math.Abs(update.PlasticityRate - 0.2f) < 0.000001f
                      && !update.ProbabilisticUpdates)));

        root.Send(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardB.RegionId,
            ShardIndex = (uint)shardB.ShardIndex,
            ShardPid = PidLabel(shardBPid),
            NeuronStart = 0,
            NeuronCount = 1
        });

        var newShardUpdate = await configuredB.Task.WaitAsync(cts.Token);
        Assert.True(newShardUpdate.CostEnabled);
        Assert.True(newShardUpdate.EnergyEnabled);
        Assert.True(newShardUpdate.PlasticityEnabled);
        Assert.Equal(0.2f, newShardUpdate.PlasticityRate);
        Assert.False(newShardUpdate.ProbabilisticUpdates);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RegisterBrainWithIo_Emits_RuntimeConfig_State()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var registerTcs = new TaskCompletionSource<ProtoIo.RegisterBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ioProbe = root.SpawnNamed(
            Props.FromProducer(() => new IoRegisterProbe(registerTcs, message =>
                message.HasRuntimeConfig
                && message.CostEnabled
                && message.EnergyEnabled
                && message.PlasticityEnabled
                && Math.Abs(message.PlasticityRate - 0.4f) < 0.000001f
                && !message.PlasticityProbabilisticUpdates)),
            "io-probe");

        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions(), ioPid: ioProbe)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        root.Send(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        });

        var inputShard = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        root.Send(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)NbnConstants.InputRegionId,
            ShardIndex = 0,
            ShardPid = PidLabel(inputShard),
            NeuronStart = 0,
            NeuronCount = 1
        });

        var outputShard = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        root.Send(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)NbnConstants.OutputRegionId,
            ShardIndex = 0,
            ShardPid = PidLabel(outputShard),
            NeuronStart = 0,
            NeuronCount = 1
        });

        root.Send(hiveMind, new SetBrainCostEnergy
        {
            BrainId = brainId.ToProtoUuid(),
            CostEnabled = true,
            EnergyEnabled = true
        });

        root.Send(hiveMind, new SetBrainPlasticity
        {
            BrainId = brainId.ToProtoUuid(),
            PlasticityEnabled = true,
            PlasticityRate = 0.4f,
            ProbabilisticUpdates = false
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var register = await registerTcs.Task.WaitAsync(cts.Token);
        Assert.True(register.HasRuntimeConfig);
        Assert.True(register.CostEnabled);
        Assert.True(register.EnergyEnabled);
        Assert.True(register.PlasticityEnabled);
        Assert.Equal(0.4f, register.PlasticityRate);
        Assert.False(register.PlasticityProbabilisticUpdates);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task KillBrain_Forwards_Termination_To_Io_And_Unregisters()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var terminatedTcs = new TaskCompletionSource<BrainTerminated>(TaskCreationOptions.RunContinuationsAsynchronously);
        var unregisterTcs = new TaskCompletionSource<ProtoIo.UnregisterBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ioProbe = root.SpawnNamed(
            Props.FromProducer(() => new IoTerminationProbe(terminatedTcs, unregisterTcs)),
            "io-kill-probe");
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions(), ioPid: ioProbe)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        root.Send(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        });

        root.Send(hiveMind, new KillBrain
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "energy_exhausted"
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var terminated = await terminatedTcs.Task.WaitAsync(cts.Token);
        Assert.True(terminated.BrainId.TryToGuid(out var terminatedBrainId));
        Assert.Equal(brainId, terminatedBrainId);
        Assert.Equal("energy_exhausted", terminated.Reason);
        Assert.Equal(0, terminated.LastTickCost);

        await Task.Delay(100);
        Assert.False(unregisterTcs.Task.IsCompleted);

        var status = await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus());
        Assert.Equal((uint)0, status.RegisteredBrains);

        await system.ShutdownAsync();
    }

    private static HiveMindOptions CreateOptions()
        => new(
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
            LateBackpressureThreshold: 2,
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

    private sealed class OutputSinkProbe : IActor
    {
        private readonly ShardId32 _shardId;
        private readonly TaskCompletionSource<UpdateShardOutputSink> _tcs;

        public OutputSinkProbe(ShardId32 shardId, TaskCompletionSource<UpdateShardOutputSink> tcs)
        {
            _shardId = shardId;
            _tcs = tcs;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is UpdateShardOutputSink update)
            {
                if (update.RegionId == (uint)_shardId.RegionId && update.ShardIndex == (uint)_shardId.ShardIndex)
                {
                    _tcs.TrySetResult(update);
                }
            }

            return Task.CompletedTask;
        }
    }

    private sealed class VisualizationProbe : IActor
    {
        private readonly ShardId32 _shardId;
        private readonly TaskCompletionSource<UpdateShardVisualization> _first;
        private readonly TaskCompletionSource<UpdateShardVisualization> _second;
        private int _seen;

        public VisualizationProbe(
            ShardId32 shardId,
            TaskCompletionSource<UpdateShardVisualization> first,
            TaskCompletionSource<UpdateShardVisualization> second)
        {
            _shardId = shardId;
            _first = first;
            _second = second;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is not UpdateShardVisualization update)
            {
                return Task.CompletedTask;
            }

            if (update.RegionId != (uint)_shardId.RegionId || update.ShardIndex != (uint)_shardId.ShardIndex)
            {
                return Task.CompletedTask;
            }

            _seen++;
            if (_seen == 1)
            {
                _first.TrySetResult(update);
            }
            else
            {
                _second.TrySetResult(update);
            }

            return Task.CompletedTask;
        }
    }

    private sealed class EmptyActor : IActor
    {
        public Task ReceiveAsync(IContext context) => Task.CompletedTask;
    }

    private sealed class RuntimeConfigProbe : IActor
    {
        private readonly ShardId32 _shardId;
        private readonly TaskCompletionSource<UpdateShardRuntimeConfig> _first;
        private readonly Func<UpdateShardRuntimeConfig, bool> _firstPredicate;
        private readonly TaskCompletionSource<UpdateShardRuntimeConfig>? _second;
        private readonly Func<UpdateShardRuntimeConfig, bool>? _secondPredicate;

        public RuntimeConfigProbe(
            ShardId32 shardId,
            TaskCompletionSource<UpdateShardRuntimeConfig> first,
            Func<UpdateShardRuntimeConfig, bool> firstPredicate,
            TaskCompletionSource<UpdateShardRuntimeConfig>? second = null,
            Func<UpdateShardRuntimeConfig, bool>? secondPredicate = null)
        {
            _shardId = shardId;
            _first = first;
            _firstPredicate = firstPredicate;
            _second = second;
            _secondPredicate = secondPredicate;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is not UpdateShardRuntimeConfig update)
            {
                return Task.CompletedTask;
            }

            if (update.RegionId != (uint)_shardId.RegionId || update.ShardIndex != (uint)_shardId.ShardIndex)
            {
                return Task.CompletedTask;
            }

            if (!_first.Task.IsCompleted && _firstPredicate(update))
            {
                _first.TrySetResult(update);
            }

            if (_second is not null && !_second.Task.IsCompleted && _secondPredicate is not null && _secondPredicate(update))
            {
                _second.TrySetResult(update);
            }

            return Task.CompletedTask;
        }
    }

    private sealed class IoRegisterProbe : IActor
    {
        private readonly TaskCompletionSource<ProtoIo.RegisterBrain> _tcs;
        private readonly Func<ProtoIo.RegisterBrain, bool> _predicate;

        public IoRegisterProbe(TaskCompletionSource<ProtoIo.RegisterBrain> tcs, Func<ProtoIo.RegisterBrain, bool> predicate)
        {
            _tcs = tcs;
            _predicate = predicate;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is ProtoIo.RegisterBrain register && _predicate(register))
            {
                _tcs.TrySetResult(register);
            }

            return Task.CompletedTask;
        }
    }

    private sealed class IoTerminationProbe : IActor
    {
        private readonly TaskCompletionSource<BrainTerminated> _terminated;
        private readonly TaskCompletionSource<ProtoIo.UnregisterBrain> _unregister;

        public IoTerminationProbe(
            TaskCompletionSource<BrainTerminated> terminated,
            TaskCompletionSource<ProtoIo.UnregisterBrain> unregister)
        {
            _terminated = terminated;
            _unregister = unregister;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case BrainTerminated terminated:
                    _terminated.TrySetResult(terminated);
                    break;
                case ProtoIo.UnregisterBrain unregister:
                    _unregister.TrySetResult(unregister);
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";
}
