using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.Quantization;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;
using SharedAddress32 = Nbn.Shared.Addressing.Address32;

namespace Nbn.Tests.RegionHost;

public class RegionShardCpuBackendHomeostasisTests
{
    [Fact]
    public void Compute_Homeostasis_UpdatesBufferBeforePreActivationGate()
    {
        var state = CreateSingleNeuronState(buffer: 0.5f, preActivationThreshold: 0.9f);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(state.RegionId, state.NeuronCount, destRegionId: 9, destCount: 1);

        var result = backend.Compute(
            tickId: 10,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            homeostasisConfig: new RegionShardHomeostasisConfig(
                Enabled: true,
                TargetMode: HomeostasisTargetMode.HomeostasisTargetZero,
                UpdateMode: HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
                BaseProbability: 1f,
                MinStepCodes: 1,
                EnergyCouplingEnabled: false,
                EnergyTargetScale: 1f,
                EnergyProbabilityScale: 1f));

        Assert.Empty(result.Outbox);
        Assert.Equal((uint)0, result.FiredCount);
        Assert.True(state.Buffer[0] < 0.5f);
    }

    [Fact]
    public void Compute_Homeostasis_ProbabilisticQuantizedStep_MovesByAtLeastOneCode()
    {
        var state = CreateSingleNeuronState(buffer: 0.72f, preActivationThreshold: 1f);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(state.RegionId, state.NeuronCount, destRegionId: 9, destCount: 1);
        var quantization = QuantizationSchemas.DefaultBuffer;
        var currentCode = quantization.Encode(0.72f, bits: 16);
        var targetCode = quantization.Encode(0f, bits: 16);

        _ = backend.Compute(
            tickId: 11,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            homeostasisConfig: new RegionShardHomeostasisConfig(
                Enabled: true,
                TargetMode: HomeostasisTargetMode.HomeostasisTargetZero,
                UpdateMode: HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
                BaseProbability: 1f,
                MinStepCodes: 3,
                EnergyCouplingEnabled: false,
                EnergyTargetScale: 1f,
                EnergyProbabilityScale: 1f));

        var nextCode = quantization.Encode(state.Buffer[0], bits: 16);
        var movedCodes = Math.Abs(nextCode - currentCode);
        var expectedMove = Math.Min(3, Math.Abs(targetCode - currentCode));
        Assert.True(movedCodes >= 1);
        Assert.Equal(expectedMove, movedCodes);
    }

    [Fact]
    public void Compute_Homeostasis_ProbabilisticGate_IsDeterministicForFixedSeed()
    {
        var first = CreateSingleNeuronState(buffer: 0.63f, preActivationThreshold: 1f, brainSeed: 0x0706050403020100UL);
        var second = CreateSingleNeuronState(buffer: 0.63f, preActivationThreshold: 1f, brainSeed: 0x0706050403020100UL);
        var backendA = new RegionShardCpuBackend(first);
        var backendB = new RegionShardCpuBackend(second);
        var routing = CreateRouting(first.RegionId, first.NeuronCount, destRegionId: 9, destCount: 1);

        _ = backendA.Compute(
            tickId: 123,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(first.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            homeostasisConfig: new RegionShardHomeostasisConfig(
                Enabled: true,
                TargetMode: HomeostasisTargetMode.HomeostasisTargetZero,
                UpdateMode: HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
                BaseProbability: 0.35f,
                MinStepCodes: 2,
                EnergyCouplingEnabled: false,
                EnergyTargetScale: 1f,
                EnergyProbabilityScale: 1f));

        _ = backendB.Compute(
            tickId: 123,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(second.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            homeostasisConfig: new RegionShardHomeostasisConfig(
                Enabled: true,
                TargetMode: HomeostasisTargetMode.HomeostasisTargetZero,
                UpdateMode: HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
                BaseProbability: 0.35f,
                MinStepCodes: 2,
                EnergyCouplingEnabled: false,
                EnergyTargetScale: 1f,
                EnergyProbabilityScale: 1f));

        Assert.Equal(first.Buffer[0], second.Buffer[0], precision: 6);
    }

    [Fact]
    public void Compute_Homeostasis_EnergyCoupling_ModulatesProbability_WhenEnabled()
    {
        const float baseProbability = 0.2f;
        var probeState = CreateSingleNeuronState(buffer: 0.6f, preActivationThreshold: 1f, brainSeed: 0x0102030405060708UL);
        var gatedTick = FindTickWithSampleBetween(probeState, minInclusive: 0.2f, maxExclusive: 0.4f);

        var uncoupledState = CreateSingleNeuronState(buffer: 0.6f, preActivationThreshold: 1f, brainSeed: probeState.BrainSeed);
        var coupledState = CreateSingleNeuronState(buffer: 0.6f, preActivationThreshold: 1f, brainSeed: probeState.BrainSeed);
        var routing = CreateRouting(uncoupledState.RegionId, uncoupledState.NeuronCount, destRegionId: 9, destCount: 1);
        var quantization = QuantizationSchemas.DefaultBuffer;
        var originalCode = quantization.Encode(0.6f, bits: 16);

        var uncoupledBackend = new RegionShardCpuBackend(uncoupledState);
        _ = uncoupledBackend.Compute(
            tickId: gatedTick,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(uncoupledState.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            homeostasisConfig: new RegionShardHomeostasisConfig(
                Enabled: true,
                TargetMode: HomeostasisTargetMode.HomeostasisTargetZero,
                UpdateMode: HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
                BaseProbability: baseProbability,
                MinStepCodes: 2,
                EnergyCouplingEnabled: false,
                EnergyTargetScale: 1f,
                EnergyProbabilityScale: 1f),
            costEnergyEnabled: true);

        var coupledBackend = new RegionShardCpuBackend(coupledState);
        _ = coupledBackend.Compute(
            tickId: gatedTick,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(coupledState.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            homeostasisConfig: new RegionShardHomeostasisConfig(
                Enabled: true,
                TargetMode: HomeostasisTargetMode.HomeostasisTargetZero,
                UpdateMode: HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
                BaseProbability: baseProbability,
                MinStepCodes: 2,
                EnergyCouplingEnabled: true,
                EnergyTargetScale: 1f,
                EnergyProbabilityScale: 2f),
            costEnergyEnabled: true);

        var uncoupledCode = quantization.Encode(uncoupledState.Buffer[0], bits: 16);
        var coupledCode = quantization.Encode(coupledState.Buffer[0], bits: 16);
        Assert.Equal(originalCode, uncoupledCode);
        Assert.NotEqual(originalCode, coupledCode);
    }

    private static RegionShardState CreateSingleNeuronState(
        float strength = 0.5f,
        float buffer = 0.9f,
        float preActivationThreshold = 0.1f,
        ulong brainSeed = 0x0102030405060708UL)
    {
        const int sourceRegionId = 8;
        const int destRegionId = 9;
        var regionSpans = new int[NbnConstants.RegionCount];
        regionSpans[sourceRegionId] = 1;
        regionSpans[destRegionId] = 1;

        var baseCode = (byte)QuantizationSchemas.DefaultNbn.Strength.Encode(strength, bits: 5);

        return new RegionShardState(
            regionId: sourceRegionId,
            neuronStart: 0,
            neuronCount: 1,
            brainSeed: brainSeed,
            strengthQuantization: QuantizationSchemas.DefaultNbn.Strength,
            regionSpans: regionSpans,
            buffer: new[] { buffer },
            enabled: new[] { true },
            exists: new[] { true },
            accumulationFunctions: new[] { (byte)AccumulationFunction.AccumSum },
            activationFunctions: new[] { (byte)ActivationFunction.ActIdentity },
            resetFunctions: new[] { (byte)ResetFunction.ResetHold },
            paramA: new[] { 0f },
            paramB: new[] { 0f },
            preActivationThreshold: new[] { preActivationThreshold },
            activationThreshold: new[] { 0.2f },
            axonCounts: new ushort[] { 1 },
            axonStartOffsets: new[] { 0 },
            axons: new RegionShardAxons(
                targetRegionIds: new[] { (byte)destRegionId },
                targetNeuronIds: new[] { 0 },
                strengths: new[] { strength },
                baseStrengthCodes: new[] { baseCode },
                runtimeStrengthCodes: new[] { baseCode },
                hasRuntimeOverlay: new[] { false },
                fromAddress32: new[] { SharedAddress32.From(sourceRegionId, 0).Value },
                toAddress32: new[] { SharedAddress32.From(destRegionId, 0).Value }));
    }

    private static RegionShardRoutingTable CreateRouting(int sourceRegionId, int sourceCount, int destRegionId, int destCount)
    {
        return new RegionShardRoutingTable(
            new Dictionary<int, ShardSpan[]>
            {
                [sourceRegionId] = new[] { new ShardSpan(0, sourceCount, ShardId32.From(sourceRegionId, 0)) },
                [destRegionId] = new[] { new ShardSpan(0, destCount, ShardId32.From(destRegionId, 0)) },
            });
    }

    private static ulong FindTickWithSampleBetween(RegionShardState state, float minInclusive, float maxExclusive)
    {
        var address = SharedAddress32.From(state.RegionId, state.NeuronStart).Value;
        for (ulong tickId = 1; tickId < 100_000; tickId++)
        {
            var sample = UnitIntervalFromSeed(RegionShardDeterministicRngInput.MixToU64(state.BrainSeed, tickId, address, address));
            if (sample >= minInclusive && sample < maxExclusive)
            {
                return tickId;
            }
        }

        throw new InvalidOperationException("Unable to find deterministic RNG sample in requested range.");
    }

    private static float UnitIntervalFromSeed(ulong seed)
    {
        const double scale = 1d / (1UL << 53);
        var bits = seed >> 11;
        return (float)(bits * scale);
    }
}
