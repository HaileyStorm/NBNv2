using System.Diagnostics;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Nbn.Tests.TestSupport;
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
    public async Task ControlPlaneMutations_Senderless_Are_Ignored()
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
        Assert.False(initialRuntimeUpdate.PlasticityEnabled);

        var debugBefore = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var visualizationIgnoredBefore = debugBefore.Count("control.set_brain_visualization.ignored");
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

        await WaitForDebugCountAsync(root, debugProbePid, "control.set_brain_visualization.ignored", visualizationIgnoredBefore + 1, timeoutMs: 2_000);
        await WaitForDebugCountAsync(root, debugProbePid, "control.set_brain_cost_energy.ignored", costEnergyIgnoredBefore + 1, timeoutMs: 2_000);
        await WaitForDebugCountAsync(root, debugProbePid, "control.set_brain_plasticity.ignored", plasticityIgnoredBefore + 1, timeoutMs: 2_000);
        await WaitForDebugCountAsync(root, debugProbePid, "control.pause_brain.ignored", pauseIgnoredBefore + 1, timeoutMs: 2_000);
        await WaitForDebugCountAsync(root, debugProbePid, "control.resume_brain.ignored", resumeIgnoredBefore + 1, timeoutMs: 2_000);
        await WaitForDebugCountAsync(root, debugProbePid, "control.kill_brain.ignored", killIgnoredBefore + 1, timeoutMs: 2_000);

        var debugAfter = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.Equal(brainPausedBefore, debugAfter.Count("brain.paused"));
        Assert.Equal(brainResumedBefore, debugAfter.Count("brain.resumed"));

        Assert.Equal(visualizationRejectedBefore + 1, metrics.SumLong(visualizationRejectedMetric, "reason", senderMissingReason));
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
        Assert.False(rejectedRuntimeUpdate.PlasticityEnabled);

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
        Assert.False(initialRuntimeUpdate.PlasticityEnabled);

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
        Assert.False(rejectedRuntimeUpdate.PlasticityEnabled);

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
                      && !update.ProbabilisticUpdates)));

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
        Assert.False(initialUpdate.PlasticityEnabled);

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
            ProbabilisticUpdates = false
        }));

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
            ProbabilisticUpdates = false
        }));

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

    private sealed record GetDebugProbeSnapshot;

    private sealed record DebugProbeSnapshot(IReadOnlyDictionary<string, int> Counts)
    {
        public int Count(string summary)
            => Counts.TryGetValue(summary, out var count) ? count : 0;
    }

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
