using System.Diagnostics;
using Nbn.Proto.Control;
using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;
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

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";
}
