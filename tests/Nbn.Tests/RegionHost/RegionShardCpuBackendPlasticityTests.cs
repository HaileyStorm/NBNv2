using Nbn.Proto;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.Quantization;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;
using SharedAddress32 = Nbn.Shared.Addressing.Address32;

namespace Nbn.Tests.RegionHost;

public class RegionShardCpuBackendPlasticityTests
{
    [Fact]
    public void Compute_WithPlasticityEnabled_UpdatesRuntimeStrengthAfterCurrentTickEmission()
    {
        var state = CreateSingleNeuronState(strength: 0.5f);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(state.RegionId, state.NeuronCount, destRegionId: 9, destCount: 1);

        var result = backend.Compute(
            tickId: 15,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            plasticityEnabled: true,
            plasticityRate: 0.25f,
            probabilisticPlasticityUpdates: false);

        var contribution = Assert.Single(Assert.Single(result.Outbox).Value);
        Assert.Equal(0.45f, contribution.Value, precision: 6);
        Assert.True(result.PlasticityStrengthCodeChanges > 0);

        Assert.Equal(0.725f, state.Axons.Strengths[0], precision: 6);
        var expectedRuntimeCode = (byte)QuantizationSchemas.DefaultNbn.Strength.Encode(state.Axons.Strengths[0], bits: 5);
        Assert.Equal(expectedRuntimeCode, state.Axons.RuntimeStrengthCodes[0]);
        Assert.Equal(expectedRuntimeCode != state.Axons.BaseStrengthCodes[0], state.Axons.HasRuntimeOverlay[0]);
    }

    [Fact]
    public void Compute_WithPlasticitySignMismatch_ReducesMagnitudeWithoutFlippingSign()
    {
        var state = CreateSingleNeuronState(strength: -0.6f);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(state.RegionId, state.NeuronCount, destRegionId: 9, destCount: 1);

        _ = backend.Compute(
            tickId: 20,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            plasticityEnabled: true,
            plasticityRate: 0.5f,
            probabilisticPlasticityUpdates: false);

        Assert.Equal(-0.15f, state.Axons.Strengths[0], precision: 6);
    }

    [Fact]
    public void Compute_WithProbabilisticPlasticity_CanDeterministicallySkipUpdate()
    {
        const float plasticityRate = 0.2f;
        const float activationScale = 0.9f;
        var probability = Math.Clamp(plasticityRate * activationScale, 0f, 1f);

        var probe = CreateSingleNeuronState(strength: 0.5f);
        var gatedTick = FindTickWithSampleAtLeast(probe, probability);

        var probabilisticState = CreateSingleNeuronState(strength: 0.5f);
        var deterministicState = CreateSingleNeuronState(strength: 0.5f);
        var routing = CreateRouting(probabilisticState.RegionId, probabilisticState.NeuronCount, destRegionId: 9, destCount: 1);

        var probabilisticBackend = new RegionShardCpuBackend(probabilisticState);
        var probabilisticResult = probabilisticBackend.Compute(
            tickId: gatedTick,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(probabilisticState.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            plasticityEnabled: true,
            plasticityRate: plasticityRate,
            probabilisticPlasticityUpdates: true);

        var deterministicBackend = new RegionShardCpuBackend(deterministicState);
        var deterministicResult = deterministicBackend.Compute(
            tickId: gatedTick,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(deterministicState.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            plasticityEnabled: true,
            plasticityRate: plasticityRate,
            probabilisticPlasticityUpdates: false);

        Assert.Equal(0.5f, probabilisticState.Axons.Strengths[0], precision: 6);
        Assert.True(deterministicState.Axons.Strengths[0] > 0.5f);
        Assert.Equal((uint)0, probabilisticResult.PlasticityStrengthCodeChanges);
        Assert.True(deterministicResult.PlasticityStrengthCodeChanges > 0);
    }

    [Fact]
    public void Compute_WithInvalidStrength_NormalizesBeforeEmissionAndMetadataUpdate()
    {
        var baseCode = (byte)QuantizationSchemas.DefaultNbn.Strength.Encode(0f, bits: 5);
        var state = CreateSingleNeuronState(
            strength: float.NaN,
            baseStrengthCode: baseCode,
            runtimeStrengthCode: baseCode);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(state.RegionId, state.NeuronCount, destRegionId: 9, destCount: 1);

        var result = backend.Compute(
            tickId: 25,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled);

        var contribution = Assert.Single(Assert.Single(result.Outbox).Value);
        Assert.Equal(0f, contribution.Value, precision: 6);
        Assert.Equal((uint)0, result.PlasticityStrengthCodeChanges);
        Assert.Equal(0f, state.Axons.Strengths[0], precision: 6);
        Assert.Equal(baseCode, state.Axons.RuntimeStrengthCodes[0]);
        Assert.False(state.Axons.HasRuntimeOverlay[0]);
    }

    private static RegionShardState CreateSingleNeuronState(float strength, byte? baseStrengthCode = null, byte? runtimeStrengthCode = null)
    {
        const int sourceRegionId = 8;
        const int destRegionId = 9;
        var regionSpans = new int[NbnConstants.RegionCount];
        regionSpans[sourceRegionId] = 1;
        regionSpans[destRegionId] = 1;

        var baseCode = baseStrengthCode ?? (byte)QuantizationSchemas.DefaultNbn.Strength.Encode(strength, bits: 5);
        var runtimeCode = runtimeStrengthCode ?? baseCode;

        return new RegionShardState(
            regionId: sourceRegionId,
            neuronStart: 0,
            neuronCount: 1,
            brainSeed: 0x0102030405060708UL,
            strengthQuantization: QuantizationSchemas.DefaultNbn.Strength,
            regionSpans: regionSpans,
            buffer: new[] { 0.9f },
            enabled: new[] { true },
            exists: new[] { true },
            accumulationFunctions: new[] { (byte)AccumulationFunction.AccumSum },
            activationFunctions: new[] { (byte)ActivationFunction.ActIdentity },
            resetFunctions: new[] { (byte)ResetFunction.ResetHold },
            paramA: new[] { 0f },
            paramB: new[] { 0f },
            preActivationThreshold: new[] { 0.1f },
            activationThreshold: new[] { 0.2f },
            axonCounts: new ushort[] { 1 },
            axonStartOffsets: new[] { 0 },
            axons: new RegionShardAxons(
                targetRegionIds: new[] { (byte)destRegionId },
                targetNeuronIds: new[] { 0 },
                strengths: new[] { strength },
                baseStrengthCodes: new[] { baseCode },
                runtimeStrengthCodes: new[] { runtimeCode },
                hasRuntimeOverlay: new[] { runtimeCode != baseCode },
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

    private static ulong FindTickWithSampleAtLeast(RegionShardState state, float threshold)
    {
        for (ulong tickId = 1; tickId < 100_000; tickId++)
        {
            var sample = UnitIntervalFromSeed(state.GetDeterministicRngInput(tickId, axonIndex: 0).ToSeed());
            if (sample >= threshold)
            {
                return tickId;
            }
        }

        throw new InvalidOperationException($"Unable to find deterministic RNG sample >= {threshold}.");
    }

    private static float UnitIntervalFromSeed(ulong seed)
    {
        const double scale = 1d / (1UL << 53);
        var bits = seed >> 11;
        return (float)(bits * scale);
    }
}
