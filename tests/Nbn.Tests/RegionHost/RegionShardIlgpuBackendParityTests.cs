using System.Diagnostics;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Signal;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.Quantization;
using Proto;
using SharedAddress32 = Nbn.Shared.Addressing.Address32;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Tests.RegionHost;

public sealed class RegionShardIlgpuBackendParityTests
{
    [Theory]
    [InlineData(OutputVectorSource.Potential)]
    [InlineData(OutputVectorSource.Buffer)]
    public void IlgpuBackend_MatchesCpu_ForSupportedShardParity(OutputVectorSource outputVectorSource)
    {
        if (!HasCompatibleGpu())
        {
            return;
        }

        var cpuState = CreateSupportedOutputState();
        var gpuState = CloneState(cpuState);
        var routing = CreateRouting();
        var brainId = Guid.Parse("448D9A76-8A7A-42BB-A950-9E8A4C6DDE74");
        var shardId = ShardId32.From(cpuState.RegionId, shardIndex: 0);
        var homeostasis = new RegionShardHomeostasisConfig(
            Enabled: true,
            TargetMode: HomeostasisTargetMode.HomeostasisTargetZero,
            UpdateMode: HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
            BaseProbability: 1f,
            MinStepCodes: 2,
            EnergyCouplingEnabled: true,
            EnergyTargetScale: 0.75f,
            EnergyProbabilityScale: 0.5f);
        var cpu = new RegionShardCpuBackend(cpuState);
        using var gpu = new RegionShardComputeBackendDispatcher(gpuState, RegionShardComputeBackendPreference.Gpu);

        for (var tick = 1UL; tick <= 3UL; tick++)
        {
            ApplyTickInputs(cpuState, tick);
            ApplyTickInputs(gpuState, tick);

            var expected = cpu.Compute(
                tick,
                brainId,
                shardId,
                routing,
                visualization: RegionShardVisualizationComputeScope.Disabled,
                plasticityEnabled: false,
                plasticityRate: 0f,
                probabilisticPlasticityUpdates: false,
                plasticityDelta: 0f,
                homeostasisConfig: homeostasis,
                costEnergyEnabled: true,
                remoteCostEnabled: true,
                remoteCostPerBatch: 7,
                remoteCostPerContribution: 3,
                costTierAMultiplier: 1.0f,
                costTierBMultiplier: 1.2f,
                costTierCMultiplier: 1.5f,
                outputVectorSource: outputVectorSource);

            var actual = gpu.Compute(
                tick,
                brainId,
                shardId,
                routing,
                visualization: RegionShardVisualizationComputeScope.Disabled,
                plasticityEnabled: false,
                plasticityRate: 0f,
                probabilisticPlasticityUpdates: false,
                plasticityDelta: 0f,
                homeostasisConfig: homeostasis,
                costEnergyEnabled: true,
                remoteCostEnabled: true,
                remoteCostPerBatch: 7,
                remoteCostPerContribution: 3,
                costTierAMultiplier: 1.0f,
                costTierBMultiplier: 1.2f,
                costTierCMultiplier: 1.5f,
                outputVectorSource: outputVectorSource);

            Assert.True(gpu.LastExecution.UsedGpu, gpu.LastExecution.FallbackReason);
            AssertEquivalentResults(expected, actual);
            AssertEquivalentState(cpuState, gpuState);
        }
    }

    [Fact]
    public async Task RegionShardActor_CaptureSnapshot_MatchesCpuAfterGpuCompute()
    {
        if (!HasCompatibleGpu())
        {
            return;
        }

        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();
        var cpuState = CreateSupportedOutputState();
        var gpuState = CloneState(cpuState);
        var shardId = ShardId32.From(cpuState.RegionId, shardIndex: 0);
        var routing = CreateRouting();
        var blackhole = root.Spawn(Props.FromProducer(static () => new IgnoreActor()));

        var cpuActor = root.Spawn(Props.FromProducer(() => new RegionShardActor(
            cpuState,
            new RegionShardActorConfig(
                BrainId: brainId,
                ShardId: shardId,
                Router: blackhole,
                OutputSink: null,
                TickSink: null,
                Routing: routing,
                ComputeBackendPreference: RegionShardComputeBackendPreference.Cpu))));

        var gpuActor = root.Spawn(Props.FromProducer(() => new RegionShardActor(
            gpuState,
            new RegionShardActorConfig(
                BrainId: brainId,
                ShardId: shardId,
                Router: blackhole,
                OutputSink: null,
                TickSink: null,
                Routing: routing,
                ComputeBackendPreference: RegionShardComputeBackendPreference.Gpu))));

        ApplyTickInputs(cpuState, tickId: 1);
        ApplyTickInputs(gpuState, tickId: 1);

        _ = await root.RequestAsync<TickComputeDone>(cpuActor, new TickCompute { TickId = 1, TargetTickHz = 30 });
        _ = await root.RequestAsync<TickComputeDone>(gpuActor, new TickCompute { TickId = 1, TargetTickHz = 30 });

        var snapshotRequest = new CaptureShardSnapshot
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)cpuState.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            TickId = 1
        };

        var cpuSnapshot = await root.RequestAsync<CaptureShardSnapshotAck>(cpuActor, snapshotRequest);
        var gpuSnapshot = await root.RequestAsync<CaptureShardSnapshotAck>(gpuActor, snapshotRequest);

        Assert.True(cpuSnapshot.Success);
        Assert.True(gpuSnapshot.Success);
        Assert.Equal(cpuSnapshot.RegionId, gpuSnapshot.RegionId);
        Assert.Equal(cpuSnapshot.ShardIndex, gpuSnapshot.ShardIndex);
        Assert.Equal(cpuSnapshot.NeuronStart, gpuSnapshot.NeuronStart);
        Assert.Equal(cpuSnapshot.NeuronCount, gpuSnapshot.NeuronCount);
        Assert.Equal(cpuSnapshot.BufferCodes, gpuSnapshot.BufferCodes);
        Assert.Equal(cpuSnapshot.EnabledBitset.ToByteArray(), gpuSnapshot.EnabledBitset.ToByteArray());
        Assert.Equal(cpuSnapshot.Overlays.Count, gpuSnapshot.Overlays.Count);

        for (var i = 0; i < cpuSnapshot.Overlays.Count; i++)
        {
            Assert.Equal(cpuSnapshot.Overlays[i].FromAddress, gpuSnapshot.Overlays[i].FromAddress);
            Assert.Equal(cpuSnapshot.Overlays[i].ToAddress, gpuSnapshot.Overlays[i].ToAddress);
            Assert.Equal(cpuSnapshot.Overlays[i].StrengthCode, gpuSnapshot.Overlays[i].StrengthCode);
        }

        await system.ShutdownAsync();
    }

    [Fact]
    public void IlgpuBackend_OutrunsCpu_OnHighLoad()
    {
        if (!HasCompatibleGpu())
        {
            return;
        }

        const int neuronCount = 262_144;
        const int fanOut = 2;
        var cpuState = CreateHighLoadState(neuronCount, fanOut);
        var gpuState = CloneState(cpuState);
        var routing = CreateHighLoadRouting(neuronCount);
        var brainId = Guid.Parse("0DBA3CA0-46F3-4FD9-8F89-10D9E3F1B0F0");
        var shardId = ShardId32.From(cpuState.RegionId, shardIndex: 0);
        var homeostasis = new RegionShardHomeostasisConfig(
            Enabled: true,
            TargetMode: HomeostasisTargetMode.HomeostasisTargetZero,
            UpdateMode: HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
            BaseProbability: 0.10f,
            MinStepCodes: 1,
            EnergyCouplingEnabled: false,
            EnergyTargetScale: 1f,
            EnergyProbabilityScale: 1f);
        var cpu = new RegionShardCpuBackend(cpuState);
        using var gpu = new RegionShardComputeBackendDispatcher(gpuState, RegionShardComputeBackendPreference.Gpu);

        for (var warmupTick = 1UL; warmupTick <= 2UL; warmupTick++)
        {
            _ = cpu.Compute(warmupTick, brainId, shardId, routing, visualization: RegionShardVisualizationComputeScope.Disabled, homeostasisConfig: homeostasis);
            _ = gpu.Compute(warmupTick, brainId, shardId, routing, visualization: RegionShardVisualizationComputeScope.Disabled, homeostasisConfig: homeostasis);
        }

        var cpuStopwatch = Stopwatch.StartNew();
        for (var tick = 3UL; tick <= 8UL; tick++)
        {
            _ = cpu.Compute(tick, brainId, shardId, routing, visualization: RegionShardVisualizationComputeScope.Disabled, homeostasisConfig: homeostasis);
        }
        cpuStopwatch.Stop();

        var gpuStopwatch = Stopwatch.StartNew();
        for (var tick = 3UL; tick <= 8UL; tick++)
        {
            _ = gpu.Compute(tick, brainId, shardId, routing, visualization: RegionShardVisualizationComputeScope.Disabled, homeostasisConfig: homeostasis);
        }
        gpuStopwatch.Stop();

        Console.WriteLine(
            $"[RegionShardIlgpuPerf] neurons={neuronCount} fanOut={fanOut} cpu_ms={cpuStopwatch.Elapsed.TotalMilliseconds:0.###} gpu_ms={gpuStopwatch.Elapsed.TotalMilliseconds:0.###}");

        Assert.True(gpu.LastExecution.UsedGpu, gpu.LastExecution.FallbackReason);
        Assert.True(
            gpuStopwatch.Elapsed < cpuStopwatch.Elapsed,
            $"Expected GPU backend to outrun CPU on high load. cpu={cpuStopwatch.Elapsed.TotalMilliseconds:0.###}ms gpu={gpuStopwatch.Elapsed.TotalMilliseconds:0.###}ms");
    }

    private static bool HasCompatibleGpu()
    {
        var availability = RegionShardGpuRuntime.ProbeAvailability();
        return availability.IsBackendAvailable;
    }

    private static void AssertEquivalentResults(RegionShardComputeResult expected, RegionShardComputeResult actual)
    {
        Assert.Equal(expected.FiredCount, actual.FiredCount);
        Assert.Equal(expected.OutContribs, actual.OutContribs);
        Assert.Equal(expected.PlasticityStrengthCodeChanges, actual.PlasticityStrengthCodeChanges);
        Assert.Equal(expected.Cost.Total, actual.Cost.Total);
        Assert.Equal(expected.Cost.Accum, actual.Cost.Accum);
        Assert.Equal(expected.Cost.Activation, actual.Cost.Activation);
        Assert.Equal(expected.Cost.Reset, actual.Cost.Reset);
        Assert.Equal(expected.Cost.Distance, actual.Cost.Distance);
        Assert.Equal(expected.Cost.Remote, actual.Cost.Remote);

        Assert.Equal(expected.OutputEvents.Count, actual.OutputEvents.Count);
        for (var i = 0; i < expected.OutputEvents.Count; i++)
        {
            Assert.Equal(expected.OutputEvents[i].OutputIndex, actual.OutputEvents[i].OutputIndex);
            Assert.Equal(expected.OutputEvents[i].TickId, actual.OutputEvents[i].TickId);
            Assert.Equal(expected.OutputEvents[i].Value, actual.OutputEvents[i].Value, 6);
        }

        Assert.Equal(expected.OutputVector.Count, actual.OutputVector.Count);
        for (var i = 0; i < expected.OutputVector.Count; i++)
        {
            Assert.Equal(expected.OutputVector[i], actual.OutputVector[i], 6);
        }

        AssertOutboxEqual(expected.Outbox, actual.Outbox);
    }

    private static void AssertOutboxEqual(
        Dictionary<ShardId32, List<Contribution>> expected,
        Dictionary<ShardId32, List<Contribution>> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        foreach (var (shardId, expectedContributions) in expected.OrderBy(static entry => entry.Key.Value))
        {
            Assert.True(actual.TryGetValue(shardId, out var actualContributions), $"Missing outbox shard {shardId}.");
            Assert.Equal(expectedContributions.Count, actualContributions!.Count);
            for (var i = 0; i < expectedContributions.Count; i++)
            {
                Assert.Equal(expectedContributions[i].TargetNeuronId, actualContributions[i].TargetNeuronId);
                Assert.Equal(expectedContributions[i].Value, actualContributions[i].Value, 6);
            }
        }
    }

    private static void AssertEquivalentState(RegionShardState expected, RegionShardState actual)
    {
        Assert.Equal(expected.Buffer.Length, actual.Buffer.Length);
        for (var i = 0; i < expected.Buffer.Length; i++)
        {
            Assert.Equal(expected.Buffer[i], actual.Buffer[i], 6);
        }
    }

    private static void ApplyTickInputs(RegionShardState state, ulong tickId)
    {
        state.ApplyContribution(0, 0.10f + (0.02f * tickId));
        state.ApplyContribution(1, 0.50f);
        state.ApplyContribution(1, 0.80f);
        state.ApplyContribution(2, 0.15f);
        state.ApplyContribution(2, 0.70f);
        state.ApplyContribution(3, 0.90f);
        state.ApplyContribution(4, 0.05f * tickId);
    }

    private static RegionShardState CreateSupportedOutputState()
    {
        const int regionId = NbnConstants.OutputRegionId;
        const int destinationRegionId = 12;
        const int neuronCount = 5;

        var regionSpans = new int[NbnConstants.RegionCount];
        regionSpans[regionId] = neuronCount;
        regionSpans[destinationRegionId] = 4;

        var axonSpecs = new (int SourceNeuronId, int TargetRegionId, int TargetNeuronId, float Strength)[]
        {
            (0, destinationRegionId, 0, 0.50f),
            (0, destinationRegionId, 1, -0.25f),
            (1, destinationRegionId, 2, 0.75f),
            (2, destinationRegionId, 3, 0.50f),
            (4, destinationRegionId, 0, -0.40f)
        };

        var targetRegionIds = new byte[axonSpecs.Length];
        var targetNeuronIds = new int[axonSpecs.Length];
        var strengths = new float[axonSpecs.Length];
        var baseStrengthCodes = new byte[axonSpecs.Length];
        var runtimeStrengthCodes = new byte[axonSpecs.Length];
        var hasRuntimeOverlay = new bool[axonSpecs.Length];
        var fromAddress32 = new uint[axonSpecs.Length];
        var toAddress32 = new uint[axonSpecs.Length];
        var axonCounts = new ushort[neuronCount];
        var axonStartOffsets = new[] { 0, 2, 3, 4, 4 };

        for (var i = 0; i < axonSpecs.Length; i++)
        {
            var axon = axonSpecs[i];
            targetRegionIds[i] = (byte)axon.TargetRegionId;
            targetNeuronIds[i] = axon.TargetNeuronId;
            strengths[i] = axon.Strength;
            var strengthCode = (byte)QuantizationSchemas.DefaultNbn.Strength.Encode(axon.Strength, bits: 5);
            baseStrengthCodes[i] = strengthCode;
            runtimeStrengthCodes[i] = strengthCode;
            hasRuntimeOverlay[i] = false;
            fromAddress32[i] = SharedAddress32.From(regionId, axon.SourceNeuronId).Value;
            toAddress32[i] = SharedAddress32.From(axon.TargetRegionId, axon.TargetNeuronId).Value;
            axonCounts[axon.SourceNeuronId]++;
        }

        return new RegionShardState(
            regionId: regionId,
            neuronStart: 0,
            neuronCount: neuronCount,
            brainSeed: 0xA102030405060708UL,
            strengthQuantization: QuantizationSchemas.DefaultNbn.Strength,
            regionSpans: regionSpans,
            buffer: new[] { 0.60f, 0.40f, 0.85f, -0.20f, 0.30f },
            enabled: new[] { true, true, true, true, true },
            exists: new[] { true, true, true, true, true },
            accumulationFunctions: new[] { (byte)AccumulationFunction.AccumSum, (byte)AccumulationFunction.AccumProduct, (byte)AccumulationFunction.AccumMax, (byte)AccumulationFunction.AccumNone, (byte)AccumulationFunction.AccumSum },
            activationFunctions: new[] { (byte)ActivationFunction.ActIdentity, (byte)ActivationFunction.ActRelu, (byte)ActivationFunction.ActPrelu, (byte)ActivationFunction.ActAdd, (byte)ActivationFunction.ActLin },
            resetFunctions: new[] { (byte)ResetFunction.ResetHold, (byte)ResetFunction.ResetClamp1, (byte)ResetFunction.ResetPotential, (byte)ResetFunction.ResetDivideAxonCt, (byte)ResetFunction.ResetZero },
            paramA: new[] { 0f, 0f, 0.35f, 0.20f, 1.25f },
            paramB: new[] { 0f, 0f, 0f, 0f, 0.10f },
            preActivationThreshold: new[] { 0.05f, 0.10f, 0.20f, -0.50f, 0.00f },
            activationThreshold: new[] { 0.15f, 0.10f, 0.25f, 0.40f, 0.05f },
            axonCounts: axonCounts,
            axonStartOffsets: axonStartOffsets,
            axons: new RegionShardAxons(
                targetRegionIds: targetRegionIds,
                targetNeuronIds: targetNeuronIds,
                strengths: strengths,
                baseStrengthCodes: baseStrengthCodes,
                runtimeStrengthCodes: runtimeStrengthCodes,
                hasRuntimeOverlay: hasRuntimeOverlay,
                fromAddress32: fromAddress32,
                toAddress32: toAddress32));
    }

    private static RegionShardRoutingTable CreateRouting()
    {
        return new RegionShardRoutingTable(
            new Dictionary<int, ShardSpan[]>
            {
                [NbnConstants.OutputRegionId] = new[] { new ShardSpan(0, 5, ShardId32.From(NbnConstants.OutputRegionId, 0)) },
                [12] = new[] { new ShardSpan(0, 4, ShardId32.From(12, 0)) }
            });
    }

    private static RegionShardState CreateHighLoadState(int neuronCount, int fanOut)
    {
        const int regionId = 8;
        const int destinationRegionId = 9;
        var regionSpans = new int[NbnConstants.RegionCount];
        regionSpans[regionId] = neuronCount;
        regionSpans[destinationRegionId] = neuronCount;

        var activationCycle = new[]
        {
            (byte)ActivationFunction.ActIdentity,
            (byte)ActivationFunction.ActRelu,
            (byte)ActivationFunction.ActPrelu,
            (byte)ActivationFunction.ActAdd,
            (byte)ActivationFunction.ActLin,
            (byte)ActivationFunction.ActClamp,
            (byte)ActivationFunction.ActMult,
            (byte)ActivationFunction.ActNrelu,
            (byte)ActivationFunction.ActPclamp
        };
        var resetCycle = new[]
        {
            (byte)ResetFunction.ResetHold,
            (byte)ResetFunction.ResetClamp1,
            (byte)ResetFunction.ResetPotential,
            (byte)ResetFunction.ResetDivideAxonCt,
            (byte)ResetFunction.ResetZero
        };

        var buffer = new float[neuronCount];
        var enabled = new bool[neuronCount];
        var exists = new bool[neuronCount];
        var accumulationFunctions = new byte[neuronCount];
        var activationFunctions = new byte[neuronCount];
        var resetFunctions = new byte[neuronCount];
        var paramA = new float[neuronCount];
        var paramB = new float[neuronCount];
        var preActivationThreshold = new float[neuronCount];
        var activationThreshold = new float[neuronCount];
        var axonCounts = new ushort[neuronCount];
        var axonStartOffsets = new int[neuronCount];

        var axonTotal = neuronCount * fanOut;
        var targetRegionIds = new byte[axonTotal];
        var targetNeuronIds = new int[axonTotal];
        var strengths = new float[axonTotal];
        var baseStrengthCodes = new byte[axonTotal];
        var runtimeStrengthCodes = new byte[axonTotal];
        var hasRuntimeOverlay = new bool[axonTotal];
        var fromAddress32 = new uint[axonTotal];
        var toAddress32 = new uint[axonTotal];

        for (var neuron = 0; neuron < neuronCount; neuron++)
        {
            enabled[neuron] = true;
            exists[neuron] = true;
            buffer[neuron] = 0.72f + ((neuron % 11) * 0.005f);
            accumulationFunctions[neuron] = (byte)AccumulationFunction.AccumSum;
            activationFunctions[neuron] = activationCycle[neuron % activationCycle.Length];
            resetFunctions[neuron] = resetCycle[neuron % resetCycle.Length];
            paramA[neuron] = 0.25f + ((neuron % 5) * 0.1f);
            paramB[neuron] = -0.10f + ((neuron % 7) * 0.05f);
            preActivationThreshold[neuron] = 0.05f;
            activationThreshold[neuron] = 0.10f;
            axonCounts[neuron] = (ushort)fanOut;
            axonStartOffsets[neuron] = neuron * fanOut;

            for (var offset = 0; offset < fanOut; offset++)
            {
                var axonIndex = (neuron * fanOut) + offset;
                var targetNeuronId = (neuron + (offset * 97)) % neuronCount;
                var strength = 0.20f + (0.05f * ((neuron + offset) % 4));

                targetRegionIds[axonIndex] = (byte)destinationRegionId;
                targetNeuronIds[axonIndex] = targetNeuronId;
                strengths[axonIndex] = strength;

                var strengthCode = (byte)QuantizationSchemas.DefaultNbn.Strength.Encode(strength, bits: 5);
                baseStrengthCodes[axonIndex] = strengthCode;
                runtimeStrengthCodes[axonIndex] = strengthCode;
                hasRuntimeOverlay[axonIndex] = false;
                fromAddress32[axonIndex] = SharedAddress32.From(regionId, neuron).Value;
                toAddress32[axonIndex] = SharedAddress32.From(destinationRegionId, targetNeuronId).Value;
            }
        }

        return new RegionShardState(
            regionId: regionId,
            neuronStart: 0,
            neuronCount: neuronCount,
            brainSeed: 0xCCBBAA9988776655UL,
            strengthQuantization: QuantizationSchemas.DefaultNbn.Strength,
            regionSpans: regionSpans,
            buffer: buffer,
            enabled: enabled,
            exists: exists,
            accumulationFunctions: accumulationFunctions,
            activationFunctions: activationFunctions,
            resetFunctions: resetFunctions,
            paramA: paramA,
            paramB: paramB,
            preActivationThreshold: preActivationThreshold,
            activationThreshold: activationThreshold,
            axonCounts: axonCounts,
            axonStartOffsets: axonStartOffsets,
            axons: new RegionShardAxons(
                targetRegionIds: targetRegionIds,
                targetNeuronIds: targetNeuronIds,
                strengths: strengths,
                baseStrengthCodes: baseStrengthCodes,
                runtimeStrengthCodes: runtimeStrengthCodes,
                hasRuntimeOverlay: hasRuntimeOverlay,
                fromAddress32: fromAddress32,
                toAddress32: toAddress32));
    }

    private static RegionShardRoutingTable CreateHighLoadRouting(int neuronCount)
    {
        return new RegionShardRoutingTable(
            new Dictionary<int, ShardSpan[]>
            {
                [8] = new[] { new ShardSpan(0, neuronCount, ShardId32.From(8, 0)) },
                [9] = new[] { new ShardSpan(0, neuronCount, ShardId32.From(9, 0)) }
            });
    }

    private static RegionShardState CloneState(RegionShardState source)
    {
        return new RegionShardState(
            regionId: source.RegionId,
            neuronStart: source.NeuronStart,
            neuronCount: source.NeuronCount,
            brainSeed: source.BrainSeed,
            strengthQuantization: source.StrengthQuantization,
            regionSpans: (int[])source.RegionSpans.Clone(),
            buffer: (float[])source.Buffer.Clone(),
            enabled: (bool[])source.Enabled.Clone(),
            exists: (bool[])source.Exists.Clone(),
            accumulationFunctions: (byte[])source.AccumulationFunctions.Clone(),
            activationFunctions: (byte[])source.ActivationFunctions.Clone(),
            resetFunctions: (byte[])source.ResetFunctions.Clone(),
            paramA: (float[])source.ParamA.Clone(),
            paramB: (float[])source.ParamB.Clone(),
            preActivationThreshold: (float[])source.PreActivationThreshold.Clone(),
            activationThreshold: (float[])source.ActivationThreshold.Clone(),
            axonCounts: (ushort[])source.AxonCounts.Clone(),
            axonStartOffsets: (int[])source.AxonStartOffsets.Clone(),
            axons: new RegionShardAxons(
                targetRegionIds: (byte[])source.Axons.TargetRegionIds.Clone(),
                targetNeuronIds: (int[])source.Axons.TargetNeuronIds.Clone(),
                strengths: (float[])source.Axons.Strengths.Clone(),
                baseStrengthCodes: (byte[])source.Axons.BaseStrengthCodes.Clone(),
                runtimeStrengthCodes: (byte[])source.Axons.RuntimeStrengthCodes.Clone(),
                hasRuntimeOverlay: (bool[])source.Axons.HasRuntimeOverlay.Clone(),
                fromAddress32: (uint[])source.Axons.FromAddress32.Clone(),
                toAddress32: (uint[])source.Axons.ToAddress32.Clone()));
    }

    private sealed class IgnoreActor : IActor
    {
        public Task ReceiveAsync(IContext context) => Task.CompletedTask;
    }
}
