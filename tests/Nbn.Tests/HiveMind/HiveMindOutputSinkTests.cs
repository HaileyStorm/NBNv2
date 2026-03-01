using System.Diagnostics;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Nbn.Tests.TestSupport;
using Proto;
using ProtoIo = Nbn.Proto.Io;
using ProtoSettings = Nbn.Proto.Settings;
using Xunit;

namespace Nbn.Tests.HiveMind;

[Collection("HiveMindSerial")]
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
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var outputSink = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        var tcs = new TaskCompletionSource<UpdateShardOutputSink>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardId = ShardId32.From(NbnConstants.OutputRegionId, 0);
        var shardPid = root.Spawn(Props.FromProducer(() => new OutputSinkProbe(shardId, tcs)));

        await root.RequestAsync<SendMessageAck>(shardPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterOutputSink
        {
            BrainId = brainId.ToProtoUuid(),
            OutputPid = PidLabel(outputSink)
        }));

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
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var outputSink = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterOutputSink
        {
            BrainId = brainId.ToProtoUuid(),
            OutputPid = PidLabel(outputSink)
        }));

        var tcs = new TaskCompletionSource<UpdateShardOutputSink>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardId = ShardId32.From(NbnConstants.OutputRegionId, 0);
        var shardPid = root.Spawn(Props.FromProducer(() => new OutputSinkProbe(shardId, tcs)));

        await root.RequestAsync<SendMessageAck>(shardPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var update = await tcs.Task.WaitAsync(cts.Token);

        Assert.Equal((uint)shardId.RegionId, update.RegionId);
        Assert.Equal((uint)shardId.ShardIndex, update.ShardIndex);
        Assert.Equal(PidLabel(outputSink), update.OutputPid);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RegisterOutputSink_Senderless_Overwrite_Is_Ignored()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var rejectedMetricName = "nbn.hivemind.control.output_sink.rejected";
        var senderMissingReason = "sender_missing";
        var rescheduleBefore = metrics.SumLong("nbn.hivemind.reschedule.requested");
        var pauseBefore = metrics.SumLong("nbn.hivemind.pause.requested");
        var computeTimeoutBefore = metrics.SumLong("nbn.hivemind.tick.compute.timeouts");
        var deliverTimeoutBefore = metrics.SumLong("nbn.hivemind.tick.deliver.timeouts");
        var rejectedBefore = metrics.SumLong(rejectedMetricName, "reason", senderMissingReason);

        var system = new ActorSystem();
        var root = system.Root;
        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions(), debugHubPid: debugProbePid)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var initialSink = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        var shardA = ShardId32.From(NbnConstants.OutputRegionId, 0);
        var shardAUpdateTcs = new TaskCompletionSource<UpdateShardOutputSink>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardAPid = root.Spawn(Props.FromProducer(() => new OutputSinkProbe(shardA, shardAUpdateTcs)));
        await root.RequestAsync<SendMessageAck>(shardAPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardA.RegionId,
            ShardIndex = (uint)shardA.ShardIndex,
            ShardPid = PidLabel(shardAPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterOutputSink
        {
            BrainId = brainId.ToProtoUuid(),
            OutputPid = PidLabel(initialSink)
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var initialUpdate = await shardAUpdateTcs.Task.WaitAsync(cts.Token);
        Assert.Equal(PidLabel(initialSink), initialUpdate.OutputPid);

        var debugBefore = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var ignoredBefore = debugBefore.Count("control.register_output_sink.ignored");

        var poisonedSink = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        root.Send(hiveMind, new RegisterOutputSink
        {
            BrainId = brainId.ToProtoUuid(),
            OutputPid = PidLabel(poisonedSink)
        });

        var shardB = ShardId32.From(NbnConstants.OutputRegionId, 1);
        var shardBUpdateTcs = new TaskCompletionSource<UpdateShardOutputSink>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardBPid = root.Spawn(Props.FromProducer(() => new OutputSinkProbe(shardB, shardBUpdateTcs)));
        await root.RequestAsync<SendMessageAck>(shardBPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardB.RegionId,
            ShardIndex = (uint)shardB.ShardIndex,
            ShardPid = PidLabel(shardBPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        var shardBUpdate = await shardBUpdateTcs.Task.WaitAsync(cts.Token);
        Assert.Equal(PidLabel(initialSink), shardBUpdate.OutputPid);

        var debugAfter = await WaitForDebugCountAsync(
            root,
            debugProbePid,
            "control.register_output_sink.ignored",
            ignoredBefore + 1,
            timeoutMs: 2_000);
        Assert.Equal(ignoredBefore + 1, debugAfter.Count("control.register_output_sink.ignored"));
        Assert.Equal(rejectedBefore + 1, metrics.SumLong(rejectedMetricName, "reason", senderMissingReason));

        await AssertNoTickOrRescheduleSideEffects(
            root,
            hiveMind,
            metrics,
            rescheduleBefore,
            pauseBefore,
            computeTimeoutBefore,
            deliverTimeoutBefore);
        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RegisterOutputSink_ForeignSender_Overwrite_Is_Ignored()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var rejectedMetricName = "nbn.hivemind.control.output_sink.rejected";
        var unauthorizedReason = "sender_not_authorized";
        var rescheduleBefore = metrics.SumLong("nbn.hivemind.reschedule.requested");
        var pauseBefore = metrics.SumLong("nbn.hivemind.pause.requested");
        var computeTimeoutBefore = metrics.SumLong("nbn.hivemind.tick.compute.timeouts");
        var deliverTimeoutBefore = metrics.SumLong("nbn.hivemind.tick.deliver.timeouts");
        var rejectedBefore = metrics.SumLong(rejectedMetricName, "reason", unauthorizedReason);

        var system = new ActorSystem();
        var root = system.Root;
        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions(), debugHubPid: debugProbePid)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var initialSink = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        var shardA = ShardId32.From(NbnConstants.OutputRegionId, 0);
        var shardAUpdateTcs = new TaskCompletionSource<UpdateShardOutputSink>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardAPid = root.Spawn(Props.FromProducer(() => new OutputSinkProbe(shardA, shardAUpdateTcs)));
        await root.RequestAsync<SendMessageAck>(shardAPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardA.RegionId,
            ShardIndex = (uint)shardA.ShardIndex,
            ShardPid = PidLabel(shardAPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterOutputSink
        {
            BrainId = brainId.ToProtoUuid(),
            OutputPid = PidLabel(initialSink)
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var initialUpdate = await shardAUpdateTcs.Task.WaitAsync(cts.Token);
        Assert.Equal(PidLabel(initialSink), initialUpdate.OutputPid);

        var debugBefore = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var ignoredBefore = debugBefore.Count("control.register_output_sink.ignored");

        var poisonedSink = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        var foreignSender = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(foreignSender, new SendMessage(hiveMind, new RegisterOutputSink
        {
            BrainId = brainId.ToProtoUuid(),
            OutputPid = PidLabel(poisonedSink)
        }));

        var shardB = ShardId32.From(NbnConstants.OutputRegionId, 1);
        var shardBUpdateTcs = new TaskCompletionSource<UpdateShardOutputSink>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardBPid = root.Spawn(Props.FromProducer(() => new OutputSinkProbe(shardB, shardBUpdateTcs)));
        await root.RequestAsync<SendMessageAck>(shardBPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardB.RegionId,
            ShardIndex = (uint)shardB.ShardIndex,
            ShardPid = PidLabel(shardBPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        var shardBUpdate = await shardBUpdateTcs.Task.WaitAsync(cts.Token);
        Assert.Equal(PidLabel(initialSink), shardBUpdate.OutputPid);

        var debugAfter = await WaitForDebugCountAsync(
            root,
            debugProbePid,
            "control.register_output_sink.ignored",
            ignoredBefore + 1,
            timeoutMs: 2_000);
        Assert.Equal(ignoredBefore + 1, debugAfter.Count("control.register_output_sink.ignored"));
        Assert.Equal(rejectedBefore + 1, metrics.SumLong(rejectedMetricName, "reason", unauthorizedReason));

        await AssertNoTickOrRescheduleSideEffects(
            root,
            hiveMind,
            metrics,
            rescheduleBefore,
            pauseBefore,
            computeTimeoutBefore,
            deliverTimeoutBefore);
        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ControlPlaneMutations_Senderless_AllowVisualization_AndIgnoreOthers()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        const string senderMissingReason = "sender_missing";
        const string visualizationRejectedMetric = "nbn.hivemind.control.set_brain_visualization.rejected";
        const string costEnergyRejectedMetric = "nbn.hivemind.control.set_brain_cost_energy.rejected";
        const string plasticityRejectedMetric = "nbn.hivemind.control.set_brain_plasticity.rejected";
        const string pauseRejectedMetric = "nbn.hivemind.control.pause_brain.rejected";
        const string resumeRejectedMetric = "nbn.hivemind.control.resume_brain.rejected";
        const string killRejectedMetric = "nbn.hivemind.control.kill_brain.rejected";

        var rescheduleBefore = metrics.SumLong("nbn.hivemind.reschedule.requested");
        var pauseBefore = metrics.SumLong("nbn.hivemind.pause.requested");
        var computeTimeoutBefore = metrics.SumLong("nbn.hivemind.tick.compute.timeouts");
        var deliverTimeoutBefore = metrics.SumLong("nbn.hivemind.tick.deliver.timeouts");
        var visualizationRejectedBefore = metrics.SumLong(visualizationRejectedMetric, "reason", senderMissingReason);
        var costEnergyRejectedBefore = metrics.SumLong(costEnergyRejectedMetric, "reason", senderMissingReason);
        var plasticityRejectedBefore = metrics.SumLong(plasticityRejectedMetric, "reason", senderMissingReason);
        var pauseRejectedBefore = metrics.SumLong(pauseRejectedMetric, "reason", senderMissingReason);
        var resumeRejectedBefore = metrics.SumLong(resumeRejectedMetric, "reason", senderMissingReason);
        var killRejectedBefore = metrics.SumLong(killRejectedMetric, "reason", senderMissingReason);

        var system = new ActorSystem();
        var root = system.Root;
        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var terminatedTcs = new TaskCompletionSource<BrainTerminated>(TaskCreationOptions.RunContinuationsAsynchronously);
        var unregisterTcs = new TaskCompletionSource<ProtoIo.UnregisterBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ioProbe = root.Spawn(Props.FromProducer(() => new IoTerminationProbe(terminatedTcs, unregisterTcs)));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions(), ioPid: ioProbe, debugHubPid: debugProbePid)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        var visualizationShardA = ShardId32.From(13, 0);
        var initialVisualization = new TaskCompletionSource<UpdateShardVisualization>(TaskCreationOptions.RunContinuationsAsynchronously);
        var initialVisualizationExtra = new TaskCompletionSource<UpdateShardVisualization>(TaskCreationOptions.RunContinuationsAsynchronously);
        var visualizationShardAPid = root.Spawn(Props.FromProducer(() => new VisualizationProbe(
            visualizationShardA,
            initialVisualization,
            initialVisualizationExtra)));
        await root.RequestAsync<SendMessageAck>(visualizationShardAPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)visualizationShardA.RegionId,
            ShardIndex = (uint)visualizationShardA.ShardIndex,
            ShardPid = PidLabel(visualizationShardAPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));
        var initialVisualizationUpdate = await initialVisualization.Task.WaitAsync(cts.Token);
        Assert.False(initialVisualizationUpdate.Enabled);
        Assert.False(initialVisualizationUpdate.HasFocusRegion);

        var runtimeShardA = ShardId32.From(14, 0);
        var initialRuntime = new TaskCompletionSource<UpdateShardRuntimeConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        var runtimeShardAPid = root.Spawn(Props.FromProducer(() => new RuntimeConfigProbe(runtimeShardA, initialRuntime, _ => true)));
        await root.RequestAsync<SendMessageAck>(runtimeShardAPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)runtimeShardA.RegionId,
            ShardIndex = (uint)runtimeShardA.ShardIndex,
            ShardPid = PidLabel(runtimeShardAPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));
        var initialRuntimeUpdate = await initialRuntime.Task.WaitAsync(cts.Token);
        Assert.False(initialRuntimeUpdate.CostEnabled);
        Assert.False(initialRuntimeUpdate.EnergyEnabled);
        Assert.True(initialRuntimeUpdate.PlasticityEnabled);
        Assert.Equal(0.001f, initialRuntimeUpdate.PlasticityRate);
        Assert.True(initialRuntimeUpdate.ProbabilisticUpdates);
        Assert.Equal(0.001f, initialRuntimeUpdate.PlasticityDelta);
        Assert.True(initialRuntimeUpdate.HomeostasisEnabled);
        Assert.Equal(HomeostasisTargetMode.HomeostasisTargetZero, initialRuntimeUpdate.HomeostasisTargetMode);
        Assert.Equal(HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep, initialRuntimeUpdate.HomeostasisUpdateMode);

        var debugBefore = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var costEnergyIgnoredBefore = debugBefore.Count("control.set_brain_cost_energy.ignored");
        var plasticityIgnoredBefore = debugBefore.Count("control.set_brain_plasticity.ignored");
        var pauseIgnoredBefore = debugBefore.Count("control.pause_brain.ignored");
        var resumeIgnoredBefore = debugBefore.Count("control.resume_brain.ignored");
        var killIgnoredBefore = debugBefore.Count("control.kill_brain.ignored");
        var brainPausedBefore = debugBefore.Count("brain.paused");
        var brainResumedBefore = debugBefore.Count("brain.resumed");

        root.Send(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = true,
            HasFocusRegion = true,
            FocusRegionId = 13
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
            PlasticityRate = 0.3f,
            ProbabilisticUpdates = true
        });
        root.Send(hiveMind, new PauseBrain
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "senderless_poison"
        });
        root.Send(hiveMind, new ResumeBrain
        {
            BrainId = brainId.ToProtoUuid()
        });
        root.Send(hiveMind, new KillBrain
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "senderless_poison"
        });

        var visualizationUpdate = await initialVisualizationExtra.Task.WaitAsync(cts.Token);
        Assert.True(visualizationUpdate.Enabled);
        Assert.True(visualizationUpdate.HasFocusRegion);
        Assert.Equal((uint)13, visualizationUpdate.FocusRegionId);

        await WaitForDebugCountAsync(root, debugProbePid, "control.set_brain_cost_energy.ignored", costEnergyIgnoredBefore + 1, timeoutMs: 2_000);
        await WaitForDebugCountAsync(root, debugProbePid, "control.set_brain_plasticity.ignored", plasticityIgnoredBefore + 1, timeoutMs: 2_000);
        await WaitForDebugCountAsync(root, debugProbePid, "control.pause_brain.ignored", pauseIgnoredBefore + 1, timeoutMs: 2_000);
        await WaitForDebugCountAsync(root, debugProbePid, "control.resume_brain.ignored", resumeIgnoredBefore + 1, timeoutMs: 2_000);
        await WaitForDebugCountAsync(root, debugProbePid, "control.kill_brain.ignored", killIgnoredBefore + 1, timeoutMs: 2_000);

        var debugAfter = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.Equal(brainPausedBefore, debugAfter.Count("brain.paused"));
        Assert.Equal(brainResumedBefore, debugAfter.Count("brain.resumed"));

        Assert.Equal(visualizationRejectedBefore, metrics.SumLong(visualizationRejectedMetric, "reason", senderMissingReason));
        Assert.Equal(costEnergyRejectedBefore + 1, metrics.SumLong(costEnergyRejectedMetric, "reason", senderMissingReason));
        Assert.Equal(plasticityRejectedBefore + 1, metrics.SumLong(plasticityRejectedMetric, "reason", senderMissingReason));
        Assert.Equal(pauseRejectedBefore + 1, metrics.SumLong(pauseRejectedMetric, "reason", senderMissingReason));
        Assert.Equal(resumeRejectedBefore + 1, metrics.SumLong(resumeRejectedMetric, "reason", senderMissingReason));
        Assert.Equal(killRejectedBefore + 1, metrics.SumLong(killRejectedMetric, "reason", senderMissingReason));

        var visualizationShardB = ShardId32.From(13, 1);
        var rejectedVisualization = new TaskCompletionSource<UpdateShardVisualization>(TaskCreationOptions.RunContinuationsAsynchronously);
        var rejectedVisualizationExtra = new TaskCompletionSource<UpdateShardVisualization>(TaskCreationOptions.RunContinuationsAsynchronously);
        var visualizationShardBPid = root.Spawn(Props.FromProducer(() => new VisualizationProbe(
            visualizationShardB,
            rejectedVisualization,
            rejectedVisualizationExtra)));
        await root.RequestAsync<SendMessageAck>(visualizationShardBPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)visualizationShardB.RegionId,
            ShardIndex = (uint)visualizationShardB.ShardIndex,
            ShardPid = PidLabel(visualizationShardBPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));
        var rejectedVisualizationUpdate = await rejectedVisualization.Task.WaitAsync(cts.Token);
        Assert.True(rejectedVisualizationUpdate.Enabled);
        Assert.True(rejectedVisualizationUpdate.HasFocusRegion);
        Assert.Equal((uint)13, rejectedVisualizationUpdate.FocusRegionId);

        var runtimeShardB = ShardId32.From(14, 1);
        var rejectedRuntime = new TaskCompletionSource<UpdateShardRuntimeConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        var runtimeShardBPid = root.Spawn(Props.FromProducer(() => new RuntimeConfigProbe(runtimeShardB, rejectedRuntime, _ => true)));
        await root.RequestAsync<SendMessageAck>(runtimeShardBPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)runtimeShardB.RegionId,
            ShardIndex = (uint)runtimeShardB.ShardIndex,
            ShardPid = PidLabel(runtimeShardBPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));
        var rejectedRuntimeUpdate = await rejectedRuntime.Task.WaitAsync(cts.Token);
        Assert.False(rejectedRuntimeUpdate.CostEnabled);
        Assert.False(rejectedRuntimeUpdate.EnergyEnabled);
        Assert.True(rejectedRuntimeUpdate.PlasticityEnabled);
        Assert.Equal(0.001f, rejectedRuntimeUpdate.PlasticityRate);
        Assert.True(rejectedRuntimeUpdate.ProbabilisticUpdates);
        Assert.Equal(0.001f, rejectedRuntimeUpdate.PlasticityDelta);

        await Task.Delay(100);
        Assert.False(terminatedTcs.Task.IsCompleted);
        Assert.False(unregisterTcs.Task.IsCompleted);

        var status = await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus());
        Assert.Equal((uint)1, status.RegisteredBrains);

        await AssertNoTickOrRescheduleSideEffects(
            root,
            hiveMind,
            metrics,
            rescheduleBefore,
            pauseBefore,
            computeTimeoutBefore,
            deliverTimeoutBefore);
        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ControlPlaneMutations_ForeignSender_Are_Ignored()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        const string unauthorizedReason = "sender_not_authorized";
        const string visualizationRejectedMetric = "nbn.hivemind.control.set_brain_visualization.rejected";
        const string costEnergyRejectedMetric = "nbn.hivemind.control.set_brain_cost_energy.rejected";
        const string plasticityRejectedMetric = "nbn.hivemind.control.set_brain_plasticity.rejected";
        const string pauseRejectedMetric = "nbn.hivemind.control.pause_brain.rejected";
        const string resumeRejectedMetric = "nbn.hivemind.control.resume_brain.rejected";
        const string killRejectedMetric = "nbn.hivemind.control.kill_brain.rejected";

        var rescheduleBefore = metrics.SumLong("nbn.hivemind.reschedule.requested");
        var pauseBefore = metrics.SumLong("nbn.hivemind.pause.requested");
        var computeTimeoutBefore = metrics.SumLong("nbn.hivemind.tick.compute.timeouts");
        var deliverTimeoutBefore = metrics.SumLong("nbn.hivemind.tick.deliver.timeouts");
        var visualizationRejectedBefore = metrics.SumLong(visualizationRejectedMetric, "reason", unauthorizedReason);
        var costEnergyRejectedBefore = metrics.SumLong(costEnergyRejectedMetric, "reason", unauthorizedReason);
        var plasticityRejectedBefore = metrics.SumLong(plasticityRejectedMetric, "reason", unauthorizedReason);
        var pauseRejectedBefore = metrics.SumLong(pauseRejectedMetric, "reason", unauthorizedReason);
        var resumeRejectedBefore = metrics.SumLong(resumeRejectedMetric, "reason", unauthorizedReason);
        var killRejectedBefore = metrics.SumLong(killRejectedMetric, "reason", unauthorizedReason);

        var system = new ActorSystem();
        var root = system.Root;
        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var terminatedTcs = new TaskCompletionSource<BrainTerminated>(TaskCreationOptions.RunContinuationsAsynchronously);
        var unregisterTcs = new TaskCompletionSource<ProtoIo.UnregisterBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ioProbe = root.Spawn(Props.FromProducer(() => new IoTerminationProbe(terminatedTcs, unregisterTcs)));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions(), ioPid: ioProbe, debugHubPid: debugProbePid)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        var foreignSender = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        var visualizationShardA = ShardId32.From(13, 0);
        var initialVisualization = new TaskCompletionSource<UpdateShardVisualization>(TaskCreationOptions.RunContinuationsAsynchronously);
        var initialVisualizationExtra = new TaskCompletionSource<UpdateShardVisualization>(TaskCreationOptions.RunContinuationsAsynchronously);
        var visualizationShardAPid = root.Spawn(Props.FromProducer(() => new VisualizationProbe(
            visualizationShardA,
            initialVisualization,
            initialVisualizationExtra)));
        await root.RequestAsync<SendMessageAck>(visualizationShardAPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)visualizationShardA.RegionId,
            ShardIndex = (uint)visualizationShardA.ShardIndex,
            ShardPid = PidLabel(visualizationShardAPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));
        var initialVisualizationUpdate = await initialVisualization.Task.WaitAsync(cts.Token);
        Assert.False(initialVisualizationUpdate.Enabled);
        Assert.False(initialVisualizationUpdate.HasFocusRegion);

        var runtimeShardA = ShardId32.From(14, 0);
        var initialRuntime = new TaskCompletionSource<UpdateShardRuntimeConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        var runtimeShardAPid = root.Spawn(Props.FromProducer(() => new RuntimeConfigProbe(runtimeShardA, initialRuntime, _ => true)));
        await root.RequestAsync<SendMessageAck>(runtimeShardAPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)runtimeShardA.RegionId,
            ShardIndex = (uint)runtimeShardA.ShardIndex,
            ShardPid = PidLabel(runtimeShardAPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));
        var initialRuntimeUpdate = await initialRuntime.Task.WaitAsync(cts.Token);
        Assert.False(initialRuntimeUpdate.CostEnabled);
        Assert.False(initialRuntimeUpdate.EnergyEnabled);
        Assert.True(initialRuntimeUpdate.PlasticityEnabled);
        Assert.Equal(0.001f, initialRuntimeUpdate.PlasticityRate);
        Assert.True(initialRuntimeUpdate.ProbabilisticUpdates);
        Assert.Equal(0.001f, initialRuntimeUpdate.PlasticityDelta);
        Assert.True(initialRuntimeUpdate.HomeostasisEnabled);
        Assert.Equal(HomeostasisTargetMode.HomeostasisTargetZero, initialRuntimeUpdate.HomeostasisTargetMode);
        Assert.Equal(HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep, initialRuntimeUpdate.HomeostasisUpdateMode);

        var debugBefore = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var visualizationIgnoredBefore = debugBefore.Count("control.set_brain_visualization.ignored");
        var costEnergyIgnoredBefore = debugBefore.Count("control.set_brain_cost_energy.ignored");
        var plasticityIgnoredBefore = debugBefore.Count("control.set_brain_plasticity.ignored");
        var pauseIgnoredBefore = debugBefore.Count("control.pause_brain.ignored");
        var resumeIgnoredBefore = debugBefore.Count("control.resume_brain.ignored");
        var killIgnoredBefore = debugBefore.Count("control.kill_brain.ignored");
        var brainPausedBefore = debugBefore.Count("brain.paused");
        var brainResumedBefore = debugBefore.Count("brain.resumed");

        await root.RequestAsync<SendMessageAck>(foreignSender, new SendMessage(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = true,
            HasFocusRegion = true,
            FocusRegionId = 13
        }));
        await root.RequestAsync<SendMessageAck>(foreignSender, new SendMessage(hiveMind, new SetBrainCostEnergy
        {
            BrainId = brainId.ToProtoUuid(),
            CostEnabled = true,
            EnergyEnabled = true
        }));
        await root.RequestAsync<SendMessageAck>(foreignSender, new SendMessage(hiveMind, new SetBrainPlasticity
        {
            BrainId = brainId.ToProtoUuid(),
            PlasticityEnabled = true,
            PlasticityRate = 0.3f,
            ProbabilisticUpdates = true
        }));
        await root.RequestAsync<SendMessageAck>(foreignSender, new SendMessage(hiveMind, new PauseBrain
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "foreign_poison"
        }));
        await root.RequestAsync<SendMessageAck>(foreignSender, new SendMessage(hiveMind, new ResumeBrain
        {
            BrainId = brainId.ToProtoUuid()
        }));
        await root.RequestAsync<SendMessageAck>(foreignSender, new SendMessage(hiveMind, new KillBrain
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "foreign_poison"
        }));

        await WaitForDebugCountAsync(root, debugProbePid, "control.set_brain_visualization.ignored", visualizationIgnoredBefore + 1, timeoutMs: 2_000);
        await WaitForDebugCountAsync(root, debugProbePid, "control.set_brain_cost_energy.ignored", costEnergyIgnoredBefore + 1, timeoutMs: 2_000);
        await WaitForDebugCountAsync(root, debugProbePid, "control.set_brain_plasticity.ignored", plasticityIgnoredBefore + 1, timeoutMs: 2_000);
        await WaitForDebugCountAsync(root, debugProbePid, "control.pause_brain.ignored", pauseIgnoredBefore + 1, timeoutMs: 2_000);
        await WaitForDebugCountAsync(root, debugProbePid, "control.resume_brain.ignored", resumeIgnoredBefore + 1, timeoutMs: 2_000);
        await WaitForDebugCountAsync(root, debugProbePid, "control.kill_brain.ignored", killIgnoredBefore + 1, timeoutMs: 2_000);

        var debugAfter = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.Equal(brainPausedBefore, debugAfter.Count("brain.paused"));
        Assert.Equal(brainResumedBefore, debugAfter.Count("brain.resumed"));

        Assert.Equal(visualizationRejectedBefore + 1, metrics.SumLong(visualizationRejectedMetric, "reason", unauthorizedReason));
        Assert.Equal(costEnergyRejectedBefore + 1, metrics.SumLong(costEnergyRejectedMetric, "reason", unauthorizedReason));
        Assert.Equal(plasticityRejectedBefore + 1, metrics.SumLong(plasticityRejectedMetric, "reason", unauthorizedReason));
        Assert.Equal(pauseRejectedBefore + 1, metrics.SumLong(pauseRejectedMetric, "reason", unauthorizedReason));
        Assert.Equal(resumeRejectedBefore + 1, metrics.SumLong(resumeRejectedMetric, "reason", unauthorizedReason));
        Assert.Equal(killRejectedBefore + 1, metrics.SumLong(killRejectedMetric, "reason", unauthorizedReason));

        var visualizationShardB = ShardId32.From(13, 1);
        var rejectedVisualization = new TaskCompletionSource<UpdateShardVisualization>(TaskCreationOptions.RunContinuationsAsynchronously);
        var rejectedVisualizationExtra = new TaskCompletionSource<UpdateShardVisualization>(TaskCreationOptions.RunContinuationsAsynchronously);
        var visualizationShardBPid = root.Spawn(Props.FromProducer(() => new VisualizationProbe(
            visualizationShardB,
            rejectedVisualization,
            rejectedVisualizationExtra)));
        await root.RequestAsync<SendMessageAck>(visualizationShardBPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)visualizationShardB.RegionId,
            ShardIndex = (uint)visualizationShardB.ShardIndex,
            ShardPid = PidLabel(visualizationShardBPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));
        var rejectedVisualizationUpdate = await rejectedVisualization.Task.WaitAsync(cts.Token);
        Assert.False(rejectedVisualizationUpdate.Enabled);
        Assert.False(rejectedVisualizationUpdate.HasFocusRegion);

        var runtimeShardB = ShardId32.From(14, 1);
        var rejectedRuntime = new TaskCompletionSource<UpdateShardRuntimeConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        var runtimeShardBPid = root.Spawn(Props.FromProducer(() => new RuntimeConfigProbe(runtimeShardB, rejectedRuntime, _ => true)));
        await root.RequestAsync<SendMessageAck>(runtimeShardBPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)runtimeShardB.RegionId,
            ShardIndex = (uint)runtimeShardB.ShardIndex,
            ShardPid = PidLabel(runtimeShardBPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));
        var rejectedRuntimeUpdate = await rejectedRuntime.Task.WaitAsync(cts.Token);
        Assert.False(rejectedRuntimeUpdate.CostEnabled);
        Assert.False(rejectedRuntimeUpdate.EnergyEnabled);
        Assert.True(rejectedRuntimeUpdate.PlasticityEnabled);
        Assert.Equal(0.001f, rejectedRuntimeUpdate.PlasticityRate);
        Assert.True(rejectedRuntimeUpdate.ProbabilisticUpdates);
        Assert.Equal(0.001f, rejectedRuntimeUpdate.PlasticityDelta);
        Assert.True(rejectedRuntimeUpdate.HomeostasisEnabled);

        await Task.Delay(100);
        Assert.False(terminatedTcs.Task.IsCompleted);
        Assert.False(unregisterTcs.Task.IsCompleted);

        var status = await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus());
        Assert.Equal((uint)1, status.RegisteredBrains);

        await AssertNoTickOrRescheduleSideEffects(
            root,
            hiveMind,
            metrics,
            rescheduleBefore,
            pauseBefore,
            computeTimeoutBefore,
            deliverTimeoutBefore);
        await system.ShutdownAsync();
    }

    [Fact]
    public async Task PauseResume_TrustedSender_Still_Works()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions(), debugHubPid: debugProbePid)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var debugBefore = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var pausedBefore = debugBefore.Count("brain.paused");
        var resumedBefore = debugBefore.Count("brain.resumed");

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new PauseBrain
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "trusted_pause"
        }));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new ResumeBrain
        {
            BrainId = brainId.ToProtoUuid()
        }));

        var pausedAfter = await WaitForDebugCountAsync(
            root,
            debugProbePid,
            "brain.paused",
            pausedBefore + 1,
            timeoutMs: 2_000);
        Assert.Equal(pausedBefore + 1, pausedAfter.Count("brain.paused"));

        var resumedAfter = await WaitForDebugCountAsync(
            root,
            debugProbePid,
            "brain.resumed",
            resumedBefore + 1,
            timeoutMs: 2_000);
        Assert.Equal(resumedBefore + 1, resumedAfter.Count("brain.resumed"));

        var status = await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus());
        Assert.Equal((uint)1, status.RegisteredBrains);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task DebugStreamDisabled_Suppresses_HiveMind_Emission()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(),
            debugHubPid: debugProbePid,
            debugStreamEnabled: false)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new PauseBrain
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "debug_disabled"
        }));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new ResumeBrain
        {
            BrainId = brainId.ToProtoUuid()
        }));

        await Task.Delay(150);
        var snapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.Equal(0, snapshot.Count("brain.spawned"));
        Assert.Equal(0, snapshot.Count("brain.paused"));
        Assert.Equal(0, snapshot.Count("brain.resumed"));

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
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var firstUpdate = new TaskCompletionSource<UpdateShardVisualization>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondUpdate = new TaskCompletionSource<UpdateShardVisualization>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardId = ShardId32.From(13, 0);
        var shardPid = root.Spawn(Props.FromProducer(() => new VisualizationProbe(shardId, firstUpdate, secondUpdate)));

        await root.RequestAsync<SendMessageAck>(shardPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var initial = await firstUpdate.Task.WaitAsync(cts.Token);
        Assert.False(initial.Enabled);
        Assert.False(initial.HasFocusRegion);

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = true,
            HasFocusRegion = true,
            FocusRegionId = 13
        }));

        var focused = await secondUpdate.Task.WaitAsync(cts.Token);
        Assert.True(focused.Enabled);
        Assert.True(focused.HasFocusRegion);
        Assert.Equal<uint>(13, focused.FocusRegionId);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task SetBrainVisualization_EnabledState_IsPeriodicallyResynced_ToShards()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));
        var subscriber = root.Spawn(Props.FromProducer(() => new EmptyActor()));

        var shardId = ShardId32.From(13, 0);
        var visualizationProbePid = root.Spawn(Props.FromProducer(() => new VisualizationHistoryProbe(shardId)));
        await root.RequestAsync<SendMessageAck>(visualizationProbePid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(visualizationProbePid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        var initialUpdates = await WaitForVisualizationUpdateCountAsync(root, visualizationProbePid, minCount: 1, timeoutMs: 2_000);
        Assert.False(initialUpdates[0].Enabled);
        Assert.False(initialUpdates[0].HasFocusRegion);

        root.Send(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = true,
            HasFocusRegion = true,
            FocusRegionId = 13,
            SubscriberActor = PidLabel(subscriber)
        });

        var enabledUpdates = await WaitForVisualizationUpdateCountAsync(root, visualizationProbePid, minCount: 2, timeoutMs: 2_000);
        Assert.True(enabledUpdates[1].Enabled);
        Assert.True(enabledUpdates[1].HasFocusRegion);
        Assert.Equal<uint>(13, enabledUpdates[1].FocusRegionId);

        var resyncedUpdates = await WaitForVisualizationUpdateCountAsync(root, visualizationProbePid, minCount: 3, timeoutMs: 4_000);
        Assert.True(resyncedUpdates[2].Enabled);
        Assert.True(resyncedUpdates[2].HasFocusRegion);
        Assert.Equal<uint>(13, resyncedUpdates[2].FocusRegionId);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task SetBrainVisualization_MultipleSubscribers_DisablingOne_KeepsVisualizationEnabled()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var subscriberA = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        var subscriberB = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        var subscriberAKey = PidLabel(subscriberA);
        var subscriberBKey = PidLabel(subscriberB);

        var shardId = ShardId32.From(13, 0);
        var visualizationProbePid = root.Spawn(Props.FromProducer(() => new VisualizationHistoryProbe(shardId)));
        await root.RequestAsync<SendMessageAck>(visualizationProbePid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(visualizationProbePid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var initialUpdates = await WaitForVisualizationUpdateCountAsync(root, visualizationProbePid, minCount: 1, timeoutMs: 2_000);
        Assert.False(initialUpdates[0].Enabled);
        Assert.False(initialUpdates[0].HasFocusRegion);

        root.Send(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = true,
            HasFocusRegion = true,
            FocusRegionId = 13,
            SubscriberActor = subscriberAKey
        });
        root.Send(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = true,
            HasFocusRegion = true,
            FocusRegionId = 13,
            SubscriberActor = subscriberBKey
        });
        root.Send(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = false,
            SubscriberActor = subscriberAKey
        });

        var enabledWhileSecondSubscriberRemains = await WaitForVisualizationUpdateCountAsync(root, visualizationProbePid, minCount: 2, timeoutMs: 2_000);
        Assert.True(enabledWhileSecondSubscriberRemains[1].Enabled);
        Assert.True(enabledWhileSecondSubscriberRemains[1].HasFocusRegion);
        Assert.Equal<uint>(13, enabledWhileSecondSubscriberRemains[1].FocusRegionId);

        root.Send(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = false,
            SubscriberActor = subscriberBKey
        });

        var disabledAfterAllSubscribersRemoved = await WaitForVisualizationUpdateCountAsync(root, visualizationProbePid, minCount: 3, timeoutMs: 2_000);
        Assert.False(disabledAfterAllSubscribersRemoved[2].Enabled);
        Assert.False(disabledAfterAllSubscribersRemoved[2].HasFocusRegion);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task SetBrainVisualization_ConflictingFocusedSubscribers_FallsBack_ToFullBrain()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var subscriberA = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        var subscriberB = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        var subscriberAKey = PidLabel(subscriberA);
        var subscriberBKey = PidLabel(subscriberB);

        var shardId = ShardId32.From(13, 0);
        var visualizationProbePid = root.Spawn(Props.FromProducer(() => new VisualizationHistoryProbe(shardId)));
        await root.RequestAsync<SendMessageAck>(visualizationProbePid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(visualizationProbePid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        var initialUpdates = await WaitForVisualizationUpdateCountAsync(root, visualizationProbePid, minCount: 1, timeoutMs: 2_000);
        Assert.False(initialUpdates[0].Enabled);
        Assert.False(initialUpdates[0].HasFocusRegion);

        root.Send(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = true,
            HasFocusRegion = true,
            FocusRegionId = 13,
            SubscriberActor = subscriberAKey
        });
        var focusedOnA = await WaitForVisualizationUpdateCountAsync(root, visualizationProbePid, minCount: 2, timeoutMs: 2_000);
        Assert.True(focusedOnA[1].Enabled);
        Assert.True(focusedOnA[1].HasFocusRegion);
        Assert.Equal<uint>(13, focusedOnA[1].FocusRegionId);

        root.Send(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = true,
            HasFocusRegion = true,
            FocusRegionId = 14,
            SubscriberActor = subscriberBKey
        });

        var conflictState = await WaitForVisualizationUpdateCountAsync(root, visualizationProbePid, minCount: 3, timeoutMs: 2_000);
        Assert.True(conflictState[2].Enabled);
        Assert.False(conflictState[2].HasFocusRegion);

        root.Send(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = false,
            SubscriberActor = subscriberBKey
        });

        var focusedAfterConflictRemoved = await WaitForVisualizationUpdateCountAsync(root, visualizationProbePid, minCount: 4, timeoutMs: 2_000);
        Assert.True(focusedAfterConflictRemoved[3].Enabled);
        Assert.True(focusedAfterConflictRemoved[3].HasFocusRegion);
        Assert.Equal<uint>(13, focusedAfterConflictRemoved[3].FocusRegionId);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task SetBrainVisualization_TerminatedSubscriberMessage_RemovesOnlyTargetedSubscriber()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        const string subscriberAKey = "viz-client-a";
        const string subscriberBKey = "viz-client-b";

        var shardId = ShardId32.From(13, 0);
        var visualizationProbePid = root.Spawn(Props.FromProducer(() => new VisualizationHistoryProbe(shardId)));
        await root.RequestAsync<SendMessageAck>(visualizationProbePid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(visualizationProbePid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        var initialUpdates = await WaitForVisualizationUpdateCountAsync(root, visualizationProbePid, minCount: 1, timeoutMs: 2_000);
        Assert.False(initialUpdates[0].Enabled);
        Assert.False(initialUpdates[0].HasFocusRegion);

        root.Send(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = true,
            HasFocusRegion = true,
            FocusRegionId = 13,
            SubscriberActor = subscriberAKey
        });
        root.Send(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = true,
            HasFocusRegion = true,
            FocusRegionId = 13,
            SubscriberActor = subscriberBKey
        });

        var enabledWithBothSubscribers = await WaitForVisualizationUpdateCountAsync(root, visualizationProbePid, minCount: 2, timeoutMs: 2_000);
        Assert.True(enabledWithBothSubscribers[1].Enabled);
        Assert.True(enabledWithBothSubscribers[1].HasFocusRegion);
        Assert.Equal<uint>(13, enabledWithBothSubscribers[1].FocusRegionId);

        root.Send(hiveMind, new Terminated { Who = new PID(string.Empty, subscriberAKey) });

        var afterFirstTermination = await root.RequestAsync<VisualizationProbeSnapshot>(visualizationProbePid, new GetVisualizationProbeSnapshot());
        Assert.Equal(2, afterFirstTermination.Updates.Count);
        Assert.True(afterFirstTermination.Updates[1].Enabled);
        Assert.True(afterFirstTermination.Updates[1].HasFocusRegion);
        Assert.Equal<uint>(13, afterFirstTermination.Updates[1].FocusRegionId);

        root.Send(hiveMind, new Terminated { Who = new PID(string.Empty, subscriberBKey) });
        var disabledAfterSecondTermination = await WaitForVisualizationUpdateCountAsync(root, visualizationProbePid, minCount: 3, timeoutMs: 2_000);
        Assert.False(disabledAfterSecondTermination[2].Enabled);
        Assert.False(disabledAfterSecondTermination[2].HasFocusRegion);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task SetBrainVisualization_StoppedLocalSubscriber_IsEventuallyCleanedUp()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var subscriberA = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        var subscriberB = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        var subscriberAKey = PidLabel(subscriberA);
        var subscriberBKey = PidLabel(subscriberB);

        var shardId = ShardId32.From(13, 0);
        var visualizationProbePid = root.Spawn(Props.FromProducer(() => new VisualizationHistoryProbe(shardId)));
        await root.RequestAsync<SendMessageAck>(visualizationProbePid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(visualizationProbePid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        var initialUpdates = await WaitForVisualizationUpdateCountAsync(root, visualizationProbePid, minCount: 1, timeoutMs: 2_000);
        Assert.False(initialUpdates[0].Enabled);
        Assert.False(initialUpdates[0].HasFocusRegion);

        root.Send(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = true,
            HasFocusRegion = true,
            FocusRegionId = 13,
            SubscriberActor = subscriberAKey
        });
        root.Send(hiveMind, new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = true,
            HasFocusRegion = true,
            FocusRegionId = 13,
            SubscriberActor = subscriberBKey
        });

        var enabledWithBothSubscribers = await WaitForVisualizationUpdateCountAsync(root, visualizationProbePid, minCount: 2, timeoutMs: 2_000);
        Assert.True(enabledWithBothSubscribers[1].Enabled);
        Assert.True(enabledWithBothSubscribers[1].HasFocusRegion);
        Assert.Equal<uint>(13, enabledWithBothSubscribers[1].FocusRegionId);

        root.Stop(subscriberA);
        await Task.Delay(500);
        var afterFirstStop = await root.RequestAsync<VisualizationProbeSnapshot>(visualizationProbePid, new GetVisualizationProbeSnapshot());
        Assert.True(afterFirstStop.Updates.Count >= 2);
        Assert.True(afterFirstStop.Updates[^1].Enabled);

        root.Stop(subscriberB);
        var disabledAfterSecondStop = await WaitForVisualizationUpdateCountAsync(
            root,
            visualizationProbePid,
            minCount: afterFirstStop.Updates.Count + 1,
            timeoutMs: 6_000);
        Assert.False(disabledAfterSecondStop[^1].Enabled);
        Assert.False(disabledAfterSecondStop[^1].HasFocusRegion);

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
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

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
                      && !update.ProbabilisticUpdates
                      && Math.Abs(update.PlasticityDelta - 0.05f) < 0.000001f
                      && update.PlasticityRebaseThreshold == 3
                      && Math.Abs(update.PlasticityRebaseThresholdPct - 0.5f) < 0.000001f
                      && update.HomeostasisEnabled
                      && update.HomeostasisTargetMode == HomeostasisTargetMode.HomeostasisTargetZero
                      && update.HomeostasisUpdateMode == HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep
                      && Math.Abs(update.HomeostasisBaseProbability - 0.2f) < 0.000001f
                      && update.HomeostasisMinStepCodes == 2
                      && update.HomeostasisEnergyCouplingEnabled
                      && Math.Abs(update.HomeostasisEnergyTargetScale - 0.7f) < 0.000001f
                      && Math.Abs(update.HomeostasisEnergyProbabilityScale - 1.3f) < 0.000001f
                      && update.DebugEnabled
                      && update.DebugMinSeverity == Nbn.Proto.Severity.SevDebug)));

        await root.RequestAsync<SendMessageAck>(shardAPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardA.RegionId,
            ShardIndex = (uint)shardA.ShardIndex,
            ShardPid = PidLabel(shardAPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var initialUpdate = await initialA.Task.WaitAsync(cts.Token);
        Assert.False(initialUpdate.CostEnabled);
        Assert.False(initialUpdate.EnergyEnabled);
        Assert.True(initialUpdate.PlasticityEnabled);
        Assert.Equal(0.001f, initialUpdate.PlasticityRate);
        Assert.True(initialUpdate.ProbabilisticUpdates);
        Assert.Equal(0.001f, initialUpdate.PlasticityDelta);
        Assert.Equal((uint)0, initialUpdate.PlasticityRebaseThreshold);
        Assert.Equal(0f, initialUpdate.PlasticityRebaseThresholdPct);
        Assert.True(initialUpdate.HomeostasisEnabled);
        Assert.Equal(HomeostasisTargetMode.HomeostasisTargetZero, initialUpdate.HomeostasisTargetMode);
        Assert.Equal(HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep, initialUpdate.HomeostasisUpdateMode);
        Assert.Equal(0.01f, initialUpdate.HomeostasisBaseProbability);
        Assert.Equal((uint)1, initialUpdate.HomeostasisMinStepCodes);
        Assert.False(initialUpdate.HomeostasisEnergyCouplingEnabled);
        Assert.True(initialUpdate.DebugEnabled);
        Assert.Equal(Nbn.Proto.Severity.SevDebug, initialUpdate.DebugMinSeverity);

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new SetBrainCostEnergy
        {
            BrainId = brainId.ToProtoUuid(),
            CostEnabled = true,
            EnergyEnabled = true
        }));

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new SetBrainPlasticity
        {
            BrainId = brainId.ToProtoUuid(),
            PlasticityEnabled = true,
            PlasticityRate = 0.2f,
            ProbabilisticUpdates = false,
            PlasticityDelta = 0.05f,
            PlasticityRebaseThreshold = 3,
            PlasticityRebaseThresholdPct = 0.5f
        }));

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new SetBrainHomeostasis
        {
            BrainId = brainId.ToProtoUuid(),
            HomeostasisEnabled = true,
            HomeostasisTargetMode = HomeostasisTargetMode.HomeostasisTargetZero,
            HomeostasisUpdateMode = HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
            HomeostasisBaseProbability = 0.2f,
            HomeostasisMinStepCodes = 2,
            HomeostasisEnergyCouplingEnabled = true,
            HomeostasisEnergyTargetScale = 0.7f,
            HomeostasisEnergyProbabilityScale = 1.3f
        }));

        var configuredUpdate = await configuredA.Task.WaitAsync(cts.Token);
        Assert.True(configuredUpdate.CostEnabled);
        Assert.True(configuredUpdate.EnergyEnabled);
        Assert.True(configuredUpdate.PlasticityEnabled);
        Assert.Equal(0.2f, configuredUpdate.PlasticityRate);
        Assert.False(configuredUpdate.ProbabilisticUpdates);
        Assert.Equal(0.05f, configuredUpdate.PlasticityDelta);
        Assert.Equal((uint)3, configuredUpdate.PlasticityRebaseThreshold);
        Assert.Equal(0.5f, configuredUpdate.PlasticityRebaseThresholdPct);
        Assert.True(configuredUpdate.HomeostasisEnabled);
        Assert.Equal(HomeostasisTargetMode.HomeostasisTargetZero, configuredUpdate.HomeostasisTargetMode);
        Assert.Equal(HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep, configuredUpdate.HomeostasisUpdateMode);
        Assert.Equal(0.2f, configuredUpdate.HomeostasisBaseProbability);
        Assert.Equal((uint)2, configuredUpdate.HomeostasisMinStepCodes);
        Assert.True(configuredUpdate.HomeostasisEnergyCouplingEnabled);
        Assert.Equal(0.7f, configuredUpdate.HomeostasisEnergyTargetScale);
        Assert.Equal(1.3f, configuredUpdate.HomeostasisEnergyProbabilityScale);
        Assert.True(configuredUpdate.DebugEnabled);
        Assert.Equal(Nbn.Proto.Severity.SevDebug, configuredUpdate.DebugMinSeverity);

        var shardB = ShardId32.From(14, 0);
        var configuredB = new TaskCompletionSource<UpdateShardRuntimeConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardBPid = root.Spawn(Props.FromProducer(() => new RuntimeConfigProbe(
            shardB,
            configuredB,
            update => update.CostEnabled
                      && update.EnergyEnabled
                      && update.PlasticityEnabled
                      && Math.Abs(update.PlasticityRate - 0.2f) < 0.000001f
                      && !update.ProbabilisticUpdates
                      && Math.Abs(update.PlasticityDelta - 0.05f) < 0.000001f
                      && update.PlasticityRebaseThreshold == 3
                      && Math.Abs(update.PlasticityRebaseThresholdPct - 0.5f) < 0.000001f
                      && update.HomeostasisEnabled
                      && update.HomeostasisTargetMode == HomeostasisTargetMode.HomeostasisTargetZero
                      && update.HomeostasisUpdateMode == HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep
                      && Math.Abs(update.HomeostasisBaseProbability - 0.2f) < 0.000001f
                      && update.HomeostasisMinStepCodes == 2
                      && update.HomeostasisEnergyCouplingEnabled
                      && Math.Abs(update.HomeostasisEnergyTargetScale - 0.7f) < 0.000001f
                      && Math.Abs(update.HomeostasisEnergyProbabilityScale - 1.3f) < 0.000001f
                      && update.DebugEnabled
                      && update.DebugMinSeverity == Nbn.Proto.Severity.SevDebug)));

        await root.RequestAsync<SendMessageAck>(shardBPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardB.RegionId,
            ShardIndex = (uint)shardB.ShardIndex,
            ShardPid = PidLabel(shardBPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        var newShardUpdate = await configuredB.Task.WaitAsync(cts.Token);
        Assert.True(newShardUpdate.CostEnabled);
        Assert.True(newShardUpdate.EnergyEnabled);
        Assert.True(newShardUpdate.PlasticityEnabled);
        Assert.Equal(0.2f, newShardUpdate.PlasticityRate);
        Assert.False(newShardUpdate.ProbabilisticUpdates);
        Assert.Equal(0.05f, newShardUpdate.PlasticityDelta);
        Assert.Equal((uint)3, newShardUpdate.PlasticityRebaseThreshold);
        Assert.Equal(0.5f, newShardUpdate.PlasticityRebaseThresholdPct);
        Assert.True(newShardUpdate.HomeostasisEnabled);
        Assert.Equal(HomeostasisTargetMode.HomeostasisTargetZero, newShardUpdate.HomeostasisTargetMode);
        Assert.Equal(HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep, newShardUpdate.HomeostasisUpdateMode);
        Assert.Equal(0.2f, newShardUpdate.HomeostasisBaseProbability);
        Assert.Equal((uint)2, newShardUpdate.HomeostasisMinStepCodes);
        Assert.True(newShardUpdate.HomeostasisEnergyCouplingEnabled);
        Assert.Equal(0.7f, newShardUpdate.HomeostasisEnergyTargetScale);
        Assert.Equal(1.3f, newShardUpdate.HomeostasisEnergyProbabilityScale);
        Assert.True(newShardUpdate.DebugEnabled);
        Assert.Equal(Nbn.Proto.Severity.SevDebug, newShardUpdate.DebugMinSeverity);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RuntimeConfig_Propagates_DebugStream_Settings()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(),
            debugStreamEnabled: false,
            debugMinSeverity: Nbn.Proto.Severity.SevWarn)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var shard = ShardId32.From(13, 0);
        var runtimeUpdate = new TaskCompletionSource<UpdateShardRuntimeConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardPid = root.Spawn(Props.FromProducer(() => new RuntimeConfigProbe(
            shard,
            runtimeUpdate,
            update => !update.DebugEnabled && update.DebugMinSeverity == Nbn.Proto.Severity.SevWarn)));

        await root.RequestAsync<SendMessageAck>(shardPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shard.RegionId,
            ShardIndex = (uint)shard.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var update = await runtimeUpdate.Task.WaitAsync(cts.Token);
        Assert.False(update.DebugEnabled);
        Assert.Equal(Nbn.Proto.Severity.SevWarn, update.DebugMinSeverity);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task SystemCostEnergySetting_Disables_RuntimeConfig_For_Enabled_Brain()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var shard = ShardId32.From(12, 0);
        var enabledRuntime = new TaskCompletionSource<UpdateShardRuntimeConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        var disabledRuntime = new TaskCompletionSource<UpdateShardRuntimeConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardPid = root.Spawn(Props.FromProducer(() => new RuntimeConfigProbe(
            shard,
            enabledRuntime,
            update => update.CostEnabled && update.EnergyEnabled,
            disabledRuntime,
            update => !update.CostEnabled && !update.EnergyEnabled)));

        await root.RequestAsync<SendMessageAck>(shardPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shard.RegionId,
            ShardIndex = (uint)shard.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new SetBrainCostEnergy
        {
            BrainId = brainId.ToProtoUuid(),
            CostEnabled = true,
            EnergyEnabled = true
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var enabled = await enabledRuntime.Task.WaitAsync(cts.Token);
        Assert.True(enabled.CostEnabled);
        Assert.True(enabled.EnergyEnabled);

        root.Send(hiveMind, new ProtoSettings.SettingChanged
        {
            Key = CostEnergySettingsKeys.SystemEnabledKey,
            Value = "false",
            UpdatedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        });

        var disabled = await disabledRuntime.Task.WaitAsync(cts.Token);
        Assert.False(disabled.CostEnabled);
        Assert.False(disabled.EnergyEnabled);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task SystemCostEnergySetting_Toggles_RegisterBrainWithIo_EffectiveCostEnergy()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var enabledRegister = new TaskCompletionSource<ProtoIo.RegisterBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var disabledRegister = new TaskCompletionSource<ProtoIo.RegisterBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ioProbe = root.Spawn(Props.FromFunc(context =>
        {
            if (context.Message is not ProtoIo.RegisterBrain register || !register.HasRuntimeConfig)
            {
                return Task.CompletedTask;
            }

            if (register.CostEnabled && register.EnergyEnabled)
            {
                enabledRegister.TrySetResult(register);
            }
            else if (!register.CostEnabled && !register.EnergyEnabled)
            {
                disabledRegister.TrySetResult(register);
            }

            return Task.CompletedTask;
        }));

        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions(), ioPid: ioProbe)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var inputShard = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(inputShard, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)NbnConstants.InputRegionId,
            ShardIndex = 0,
            ShardPid = PidLabel(inputShard),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        var outputShard = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(outputShard, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)NbnConstants.OutputRegionId,
            ShardIndex = 0,
            ShardPid = PidLabel(outputShard),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new SetBrainCostEnergy
        {
            BrainId = brainId.ToProtoUuid(),
            CostEnabled = true,
            EnergyEnabled = true
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var enabled = await enabledRegister.Task.WaitAsync(cts.Token);
        Assert.True(enabled.CostEnabled);
        Assert.True(enabled.EnergyEnabled);

        root.Send(hiveMind, new ProtoSettings.SettingChanged
        {
            Key = CostEnergySettingsKeys.SystemEnabledKey,
            Value = "false",
            UpdatedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        });

        var disabled = await disabledRegister.Task.WaitAsync(cts.Token);
        Assert.False(disabled.CostEnabled);
        Assert.False(disabled.EnergyEnabled);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task SystemPlasticitySetting_Disables_RuntimeConfig_For_Unsuppressed_Brain()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var shard = ShardId32.From(13, 0);
        var initialRuntime = new TaskCompletionSource<UpdateShardRuntimeConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        var disabledRuntime = new TaskCompletionSource<UpdateShardRuntimeConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardPid = root.Spawn(Props.FromProducer(() => new RuntimeConfigProbe(
            shard,
            initialRuntime,
            update => update.PlasticityEnabled,
            disabledRuntime,
            update => !update.PlasticityEnabled)));

        await root.RequestAsync<SendMessageAck>(shardPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shard.RegionId,
            ShardIndex = (uint)shard.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var initial = await initialRuntime.Task.WaitAsync(cts.Token);
        Assert.True(initial.PlasticityEnabled);
        Assert.Equal(0.001f, initial.PlasticityRate);
        Assert.True(initial.ProbabilisticUpdates);

        root.Send(hiveMind, new ProtoSettings.SettingChanged
        {
            Key = PlasticitySettingsKeys.SystemEnabledKey,
            Value = "false",
            UpdatedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        });

        var disabled = await disabledRuntime.Task.WaitAsync(cts.Token);
        Assert.False(disabled.PlasticityEnabled);
        Assert.Equal(0.001f, disabled.PlasticityRate);
        Assert.True(disabled.ProbabilisticUpdates);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task BrainPlasticitySuppression_Persists_When_SystemPlasticity_Reenabled()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new SetBrainPlasticity
        {
            BrainId = brainId.ToProtoUuid(),
            PlasticityEnabled = false,
            PlasticityRate = 0.001f,
            ProbabilisticUpdates = true,
            PlasticityDelta = 0.001f
        }));

        root.Send(hiveMind, new ProtoSettings.SettingChanged
        {
            Key = PlasticitySettingsKeys.SystemEnabledKey,
            Value = "false",
            UpdatedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        });
        root.Send(hiveMind, new ProtoSettings.SettingChanged
        {
            Key = PlasticitySettingsKeys.SystemEnabledKey,
            Value = "true",
            UpdatedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        });

        var shard = ShardId32.From(14, 0);
        var suppressedRuntime = new TaskCompletionSource<UpdateShardRuntimeConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        var unsuppressedRuntime = new TaskCompletionSource<UpdateShardRuntimeConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shardPid = root.Spawn(Props.FromProducer(() => new RuntimeConfigProbe(
            shard,
            suppressedRuntime,
            update => !update.PlasticityEnabled,
            unsuppressedRuntime,
            update => update.PlasticityEnabled)));

        await root.RequestAsync<SendMessageAck>(shardPid, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shard.RegionId,
            ShardIndex = (uint)shard.ShardIndex,
            ShardPid = PidLabel(shardPid),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var suppressed = await suppressedRuntime.Task.WaitAsync(cts.Token);
        Assert.False(suppressed.PlasticityEnabled);

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new SetBrainPlasticity
        {
            BrainId = brainId.ToProtoUuid(),
            PlasticityEnabled = true,
            PlasticityRate = 0.001f,
            ProbabilisticUpdates = true,
            PlasticityDelta = 0.001f
        }));

        var unsuppressed = await unsuppressedRuntime.Task.WaitAsync(cts.Token);
        Assert.True(unsuppressed.PlasticityEnabled);
        Assert.Equal(0.001f, unsuppressed.PlasticityRate);
        Assert.True(unsuppressed.ProbabilisticUpdates);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task SystemPlasticitySetting_Toggles_RegisterBrainWithIo_EffectivePlasticity()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var initialEnabledRegister = new TaskCompletionSource<ProtoIo.RegisterBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var disabledRegister = new TaskCompletionSource<ProtoIo.RegisterBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var reenabledRegister = new TaskCompletionSource<ProtoIo.RegisterBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ioProbe = root.Spawn(Props.FromFunc(context =>
        {
            if (context.Message is not ProtoIo.RegisterBrain register || !register.HasRuntimeConfig)
            {
                return Task.CompletedTask;
            }

            if (register.PlasticityEnabled)
            {
                if (!initialEnabledRegister.Task.IsCompleted)
                {
                    initialEnabledRegister.TrySetResult(register);
                }
                else if (disabledRegister.Task.IsCompleted && !reenabledRegister.Task.IsCompleted)
                {
                    reenabledRegister.TrySetResult(register);
                }
            }
            else if (!disabledRegister.Task.IsCompleted)
            {
                disabledRegister.TrySetResult(register);
            }

            return Task.CompletedTask;
        }));

        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions(), ioPid: ioProbe)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var inputShard = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(inputShard, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)NbnConstants.InputRegionId,
            ShardIndex = 0,
            ShardPid = PidLabel(inputShard),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        var outputShard = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(outputShard, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)NbnConstants.OutputRegionId,
            ShardIndex = 0,
            ShardPid = PidLabel(outputShard),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var initialEnabled = await initialEnabledRegister.Task.WaitAsync(cts.Token);
        Assert.True(initialEnabled.PlasticityEnabled);
        Assert.Equal(0.001f, initialEnabled.PlasticityRate);
        Assert.True(initialEnabled.PlasticityProbabilisticUpdates);
        Assert.Equal(0.001f, initialEnabled.PlasticityDelta);

        root.Send(hiveMind, new ProtoSettings.SettingChanged
        {
            Key = PlasticitySettingsKeys.SystemEnabledKey,
            Value = "false",
            UpdatedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        });

        var disabled = await disabledRegister.Task.WaitAsync(cts.Token);
        Assert.False(disabled.PlasticityEnabled);
        Assert.Equal(0.001f, disabled.PlasticityRate);
        Assert.True(disabled.PlasticityProbabilisticUpdates);
        Assert.Equal(0.001f, disabled.PlasticityDelta);

        root.Send(hiveMind, new ProtoSettings.SettingChanged
        {
            Key = PlasticitySettingsKeys.SystemEnabledKey,
            Value = "true",
            UpdatedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        });

        var reenabled = await reenabledRegister.Task.WaitAsync(cts.Token);
        Assert.True(reenabled.PlasticityEnabled);
        Assert.Equal(0.001f, reenabled.PlasticityRate);
        Assert.True(reenabled.PlasticityProbabilisticUpdates);
        Assert.Equal(0.001f, reenabled.PlasticityDelta);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RegisterBrainWithIo_Emits_Default_Plasticity_RuntimeConfig()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var registerTcs = new TaskCompletionSource<ProtoIo.RegisterBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ioProbe = root.SpawnNamed(
            Props.FromProducer(() => new IoRegisterProbe(registerTcs, message =>
                message.HasRuntimeConfig
                && message.PlasticityEnabled
                && Math.Abs(message.PlasticityRate - 0.001f) < 0.000001f
                && message.PlasticityProbabilisticUpdates
                && Math.Abs(message.PlasticityDelta - 0.001f) < 0.000001f)),
            "io-default-plasticity-probe");

        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions(), ioPid: ioProbe)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var inputShard = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(inputShard, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)NbnConstants.InputRegionId,
            ShardIndex = 0,
            ShardPid = PidLabel(inputShard),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        var outputShard = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(outputShard, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)NbnConstants.OutputRegionId,
            ShardIndex = 0,
            ShardPid = PidLabel(outputShard),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var register = await registerTcs.Task.WaitAsync(cts.Token);
        Assert.True(register.HasRuntimeConfig);
        Assert.True(register.PlasticityEnabled);
        Assert.Equal(0.001f, register.PlasticityRate);
        Assert.True(register.PlasticityProbabilisticUpdates);
        Assert.Equal(0.001f, register.PlasticityDelta);

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
                && !message.PlasticityProbabilisticUpdates
                && Math.Abs(message.PlasticityDelta - 0.08f) < 0.000001f
                && message.PlasticityRebaseThreshold == 7
                && Math.Abs(message.PlasticityRebaseThresholdPct - 0.35f) < 0.000001f
                && message.HomeostasisEnabled
                && message.HomeostasisTargetMode == HomeostasisTargetMode.HomeostasisTargetZero
                && message.HomeostasisUpdateMode == HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep
                && Math.Abs(message.HomeostasisBaseProbability - 0.19f) < 0.000001f
                && message.HomeostasisMinStepCodes == 3
                && message.HomeostasisEnergyCouplingEnabled
                && Math.Abs(message.HomeostasisEnergyTargetScale - 0.8f) < 0.000001f
                && Math.Abs(message.HomeostasisEnergyProbabilityScale - 1.2f) < 0.000001f)),
            "io-probe");

        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions(), ioPid: ioProbe)));

        var brainId = Guid.NewGuid();
        var brainRoot = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        var inputShard = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(inputShard, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)NbnConstants.InputRegionId,
            ShardIndex = 0,
            ShardPid = PidLabel(inputShard),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        var outputShard = root.Spawn(Props.FromProducer(() => new EmptyActor()));
        await root.RequestAsync<SendMessageAck>(outputShard, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)NbnConstants.OutputRegionId,
            ShardIndex = 0,
            ShardPid = PidLabel(outputShard),
            NeuronStart = 0,
            NeuronCount = 1
        }));

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new SetBrainCostEnergy
        {
            BrainId = brainId.ToProtoUuid(),
            CostEnabled = true,
            EnergyEnabled = true
        }));

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new SetBrainPlasticity
        {
            BrainId = brainId.ToProtoUuid(),
            PlasticityEnabled = true,
            PlasticityRate = 0.4f,
            ProbabilisticUpdates = false,
            PlasticityDelta = 0.08f,
            PlasticityRebaseThreshold = 7,
            PlasticityRebaseThresholdPct = 0.35f
        }));

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new SetBrainHomeostasis
        {
            BrainId = brainId.ToProtoUuid(),
            HomeostasisEnabled = true,
            HomeostasisTargetMode = HomeostasisTargetMode.HomeostasisTargetZero,
            HomeostasisUpdateMode = HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
            HomeostasisBaseProbability = 0.19f,
            HomeostasisMinStepCodes = 3,
            HomeostasisEnergyCouplingEnabled = true,
            HomeostasisEnergyTargetScale = 0.8f,
            HomeostasisEnergyProbabilityScale = 1.2f
        }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var register = await registerTcs.Task.WaitAsync(cts.Token);
        Assert.True(register.HasRuntimeConfig);
        Assert.True(register.CostEnabled);
        Assert.True(register.EnergyEnabled);
        Assert.True(register.PlasticityEnabled);
        Assert.Equal(0.4f, register.PlasticityRate);
        Assert.False(register.PlasticityProbabilisticUpdates);
        Assert.Equal(0.08f, register.PlasticityDelta);
        Assert.Equal((uint)7, register.PlasticityRebaseThreshold);
        Assert.Equal(0.35f, register.PlasticityRebaseThresholdPct);
        Assert.True(register.HomeostasisEnabled);
        Assert.Equal(HomeostasisTargetMode.HomeostasisTargetZero, register.HomeostasisTargetMode);
        Assert.Equal(HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep, register.HomeostasisUpdateMode);
        Assert.Equal(0.19f, register.HomeostasisBaseProbability);
        Assert.Equal((uint)3, register.HomeostasisMinStepCodes);
        Assert.True(register.HomeostasisEnergyCouplingEnabled);
        Assert.Equal(0.8f, register.HomeostasisEnergyTargetScale);
        Assert.Equal(1.2f, register.HomeostasisEnergyProbabilityScale);

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
        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(brainRoot)
        }));

        await root.RequestAsync<SendMessageAck>(brainRoot, new SendMessage(hiveMind, new KillBrain
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "energy_exhausted"
        }));

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

    private static async Task AssertNoTickOrRescheduleSideEffects(
        IRootContext root,
        PID hiveMind,
        MeterCollector metrics,
        long expectedReschedule,
        long expectedPause,
        long expectedComputeTimeouts,
        long expectedDeliverTimeouts)
    {
        var status = await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus());
        Assert.False(status.RescheduleInProgress);
        Assert.Equal((uint)0, status.PendingCompute);
        Assert.Equal((uint)0, status.PendingDeliver);

        Assert.Equal(expectedReschedule, metrics.SumLong("nbn.hivemind.reschedule.requested"));
        Assert.Equal(expectedPause, metrics.SumLong("nbn.hivemind.pause.requested"));
        Assert.Equal(expectedComputeTimeouts, metrics.SumLong("nbn.hivemind.tick.compute.timeouts"));
        Assert.Equal(expectedDeliverTimeouts, metrics.SumLong("nbn.hivemind.tick.deliver.timeouts"));
    }

    private static async Task<DebugProbeSnapshot> WaitForDebugCountAsync(
        IRootContext root,
        PID debugProbePid,
        string summary,
        int minCount,
        int timeoutMs)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            var snapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
            if (snapshot.Count(summary) >= minCount)
            {
                return snapshot;
            }

            await Task.Delay(25);
        }

        var finalSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        throw new TimeoutException(
            $"Timed out waiting for debug summary '{summary}' to reach {minCount}. Current={finalSnapshot.Count(summary)}");
    }

    private static async Task<IReadOnlyList<UpdateShardVisualization>> WaitForVisualizationUpdateCountAsync(
        IRootContext root,
        PID probePid,
        int minCount,
        int timeoutMs)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            var snapshot = await root.RequestAsync<VisualizationProbeSnapshot>(probePid, new GetVisualizationProbeSnapshot());
            if (snapshot.Updates.Count >= minCount)
            {
                return snapshot.Updates;
            }

            await Task.Delay(25);
        }

        var finalSnapshot = await root.RequestAsync<VisualizationProbeSnapshot>(probePid, new GetVisualizationProbeSnapshot());
        throw new TimeoutException(
            $"Timed out waiting for visualization updates to reach {minCount}. Current={finalSnapshot.Updates.Count}");
    }

    private sealed record GetDebugProbeSnapshot;
    private sealed record GetVisualizationProbeSnapshot;

    private sealed record DebugProbeSnapshot(IReadOnlyDictionary<string, int> Counts)
    {
        public int Count(string summary)
            => Counts.TryGetValue(summary, out var count) ? count : 0;
    }

    private sealed record VisualizationProbeSnapshot(IReadOnlyList<UpdateShardVisualization> Updates);

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

    private sealed record SendMessage(PID Target, object Message);
    private sealed record SendMessageAck;

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
            if (context.Message is SendMessage send)
            {
                context.Request(send.Target, send.Message);
                context.Respond(new SendMessageAck());
            }
            else if (context.Message is UpdateShardOutputSink update)
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
            if (context.Message is SendMessage send)
            {
                context.Request(send.Target, send.Message);
                context.Respond(new SendMessageAck());
                return Task.CompletedTask;
            }

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

    private sealed class VisualizationHistoryProbe : IActor
    {
        private readonly ShardId32 _shardId;
        private readonly List<UpdateShardVisualization> _updates = new();

        public VisualizationHistoryProbe(ShardId32 shardId)
        {
            _shardId = shardId;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case SendMessage send:
                    context.Request(send.Target, send.Message);
                    context.Respond(new SendMessageAck());
                    break;
                case UpdateShardVisualization update:
                    if (update.RegionId == (uint)_shardId.RegionId && update.ShardIndex == (uint)_shardId.ShardIndex)
                    {
                        _updates.Add(update);
                    }

                    break;
                case GetVisualizationProbeSnapshot:
                    context.Respond(new VisualizationProbeSnapshot(_updates.ToArray()));
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class EmptyActor : IActor
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
            if (context.Message is SendMessage send)
            {
                context.Request(send.Target, send.Message);
                context.Respond(new SendMessageAck());
                return Task.CompletedTask;
            }

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
