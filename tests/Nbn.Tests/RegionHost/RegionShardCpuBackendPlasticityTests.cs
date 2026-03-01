using Nbn.Proto;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.Quantization;
using Nbn.Proto.Signal;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;
using SharedAddress32 = Nbn.Shared.Addressing.Address32;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Tests.RegionHost;

public class RegionShardCpuBackendPlasticityTests
{
    private static readonly RegionShardHomeostasisConfig DisabledHomeostasis = new(
        Enabled: false,
        TargetMode: ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero,
        UpdateMode: ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
        BaseProbability: 0f,
        MinStepCodes: 1,
        EnergyCouplingEnabled: false,
        EnergyTargetScale: 1f,
        EnergyProbabilityScale: 1f);

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
            probabilisticPlasticityUpdates: false,
            plasticityDelta: 0.25f);

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
            probabilisticPlasticityUpdates: false,
            plasticityDelta: 0.5f);

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
            probabilisticPlasticityUpdates: true,
            plasticityDelta: plasticityRate);

        var deterministicBackend = new RegionShardCpuBackend(deterministicState);
        var deterministicResult = deterministicBackend.Compute(
            tickId: gatedTick,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(deterministicState.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            plasticityEnabled: true,
            plasticityRate: plasticityRate,
            probabilisticPlasticityUpdates: false,
            plasticityDelta: plasticityRate);

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

    [Fact]
    public void Compute_UsesPlasticityDelta_ForMagnitude_WhenProbabilisticDisabled()
    {
        var state = CreateSingleNeuronState(strength: 0.5f);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(state.RegionId, state.NeuronCount, destRegionId: 9, destCount: 1);

        _ = backend.Compute(
            tickId: 31,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            plasticityEnabled: true,
            plasticityRate: 0.01f,
            probabilisticPlasticityUpdates: false,
            plasticityDelta: 0.3f);

        Assert.Equal(0.77f, state.Axons.Strengths[0], precision: 6);
    }

    [Fact]
    public void Compute_WithProbabilisticPlasticity_UsesRateForGate_AndDeltaForStep()
    {
        const float plasticityRate = 0.1f;
        const float plasticityDelta = 0.3f;
        const float activationScale = 0.9f;
        var probability = Math.Clamp(plasticityRate * activationScale, 0f, 1f);

        var probe = CreateSingleNeuronState(strength: 0.5f);
        var gatedTick = FindTickWithSampleLessThan(probe, probability);

        var state = CreateSingleNeuronState(strength: 0.5f);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(state.RegionId, state.NeuronCount, destRegionId: 9, destCount: 1);
        _ = backend.Compute(
            tickId: gatedTick,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            plasticityEnabled: true,
            plasticityRate: plasticityRate,
            probabilisticPlasticityUpdates: true,
            plasticityDelta: plasticityDelta);

        Assert.Equal(0.77f, state.Axons.Strengths[0], precision: 6);
    }

    [Fact]
    public void Compute_WithPlasticityRebaseThresholdCount_TriggersOverlayReset()
    {
        var state = CreateSingleNeuronState(strength: 0.5f);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(state.RegionId, state.NeuronCount, destRegionId: 9, destCount: 1);
        var originalBaseCode = state.Axons.BaseStrengthCodes[0];

        _ = backend.Compute(
            tickId: 41,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            plasticityEnabled: true,
            plasticityRate: 0.25f,
            probabilisticPlasticityUpdates: false,
            plasticityDelta: 0.25f,
            plasticityRebaseThreshold: 1);

        Assert.NotEqual(originalBaseCode, state.Axons.BaseStrengthCodes[0]);
        Assert.Equal(state.Axons.BaseStrengthCodes[0], state.Axons.RuntimeStrengthCodes[0]);
        Assert.False(state.Axons.HasRuntimeOverlay[0]);
    }

    [Fact]
    public void Compute_WithPlasticityRebaseThresholdCount_BelowThreshold_PreservesOverlay()
    {
        var state = CreateSingleNeuronState(strength: 0.5f);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(state.RegionId, state.NeuronCount, destRegionId: 9, destCount: 1);
        var originalBaseCode = state.Axons.BaseStrengthCodes[0];

        _ = backend.Compute(
            tickId: 43,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            plasticityEnabled: true,
            plasticityRate: 0.25f,
            probabilisticPlasticityUpdates: false,
            plasticityDelta: 0.25f,
            plasticityRebaseThreshold: 2);

        Assert.Equal(originalBaseCode, state.Axons.BaseStrengthCodes[0]);
        Assert.NotEqual(state.Axons.BaseStrengthCodes[0], state.Axons.RuntimeStrengthCodes[0]);
        Assert.True(state.Axons.HasRuntimeOverlay[0]);
    }

    [Fact]
    public void Compute_WithPlasticityRebaseThresholdPct_TriggersOverlayReset()
    {
        var state = CreateSingleNeuronState(strength: 0.5f);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(state.RegionId, state.NeuronCount, destRegionId: 9, destCount: 1);
        var originalBaseCode = state.Axons.BaseStrengthCodes[0];

        _ = backend.Compute(
            tickId: 45,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            plasticityEnabled: true,
            plasticityRate: 0.25f,
            probabilisticPlasticityUpdates: false,
            plasticityDelta: 0.25f,
            plasticityRebaseThresholdPct: 0.5f);

        Assert.NotEqual(originalBaseCode, state.Axons.BaseStrengthCodes[0]);
        Assert.Equal(state.Axons.BaseStrengthCodes[0], state.Axons.RuntimeStrengthCodes[0]);
        Assert.False(state.Axons.HasRuntimeOverlay[0]);
    }

    [Fact]
    public void Compute_WithZeroStrength_AcquiresPotentialDirection()
    {
        var zeroCode = (byte)QuantizationSchemas.DefaultNbn.Strength.Encode(0f, bits: 5);
        var state = CreateSingleNeuronState(strength: 0f, baseStrengthCode: zeroCode, runtimeStrengthCode: zeroCode);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(state.RegionId, state.NeuronCount, destRegionId: 9, destCount: 1);

        _ = backend.Compute(
            tickId: 51,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            plasticityEnabled: true,
            plasticityRate: 0.3f,
            probabilisticPlasticityUpdates: false,
            plasticityDelta: 0.2f,
            homeostasisConfig: DisabledHomeostasis);

        Assert.True(state.Axons.Strengths[0] > 0f);
        Assert.NotEqual(zeroCode, state.Axons.RuntimeStrengthCodes[0]);
    }

    [Fact]
    public void Compute_WithLocalPredictiveNudge_AmplifiesAlignedTargetState()
    {
        var state = CreateTwoNeuronCadenceState(localPredictiveTarget: true, targetBuffer: 0.8f, initialStrength: 0.5f);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateCadenceRouting();

        state.ApplyContribution(targetNeuronId: 0, value: 0.9f);
        _ = backend.Compute(
            tickId: 61,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            plasticityEnabled: true,
            plasticityRate: 0.25f,
            probabilisticPlasticityUpdates: false,
            plasticityDelta: 0.25f,
            homeostasisConfig: DisabledHomeostasis);

        Assert.True(state.Axons.Strengths[0] > 0.725f);
    }

    [Fact]
    public void Compute_WithLocalPredictiveNudge_DampensOpposedTargetState()
    {
        var state = CreateTwoNeuronCadenceState(localPredictiveTarget: true, targetBuffer: -0.8f, initialStrength: 0.5f);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateCadenceRouting();

        state.ApplyContribution(targetNeuronId: 0, value: 0.9f);
        _ = backend.Compute(
            tickId: 63,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            plasticityEnabled: true,
            plasticityRate: 0.25f,
            probabilisticPlasticityUpdates: false,
            plasticityDelta: 0.25f,
            homeostasisConfig: DisabledHomeostasis);

        Assert.InRange(state.Axons.Strengths[0], 0.5f, 0.725f);
    }

    [Theory]
    [MemberData(nameof(CadencePatterns))]
    public void Compute_WithApproximateNudges_ImprovesInputDependency_AndShortMemoryAcrossCadences(string _, float[] cadence)
    {
        var control = RunCadenceSimulation(cadence, localPredictiveTarget: false);
        var nudged = RunCadenceSimulation(cadence, localPredictiveTarget: true);

        Assert.True(
            nudged.ActiveAverageOutput > control.ActiveAverageOutput + 0.005f,
            $"Expected active output improvement for cadence. control={control.ActiveAverageOutput:F6} nudged={nudged.ActiveAverageOutput:F6}");
        if (control.GapReentrySampleCount > 0 && nudged.GapReentrySampleCount > 0)
        {
            Assert.True(
                nudged.GapReentryAverageOutput > control.GapReentryAverageOutput + 0.005f,
                $"Expected gap-reentry memory improvement for cadence. control={control.GapReentryAverageOutput:F6} nudged={nudged.GapReentryAverageOutput:F6}");
        }
        Assert.True(float.IsFinite(nudged.FinalStrength));
        Assert.InRange(
            nudged.FinalStrength,
            QuantizationSchemas.DefaultNbn.Strength.Min,
            QuantizationSchemas.DefaultNbn.Strength.Max);
    }

    [Theory]
    [MemberData(nameof(CadencePatterns))]
    public void Compute_WithApproximateNudges_IsDeterministicAcrossReplayCadences(string _, float[] cadence)
    {
        var runA = RunCadenceSimulation(cadence, localPredictiveTarget: true);
        var runB = RunCadenceSimulation(cadence, localPredictiveTarget: true);

        Assert.Equal(runA.FinalStrength, runB.FinalStrength, precision: 6);
        Assert.Equal(runA.RuntimeCode, runB.RuntimeCode);
        Assert.Equal(runA.HasOverlay, runB.HasOverlay);
        Assert.Equal(runA.OutputMagnitudes.Count, runB.OutputMagnitudes.Count);
        for (var i = 0; i < runA.OutputMagnitudes.Count; i++)
        {
            Assert.Equal(runA.OutputMagnitudes[i], runB.OutputMagnitudes[i], precision: 6);
        }
    }

    [Fact]
    public void Compute_WithApproximateNudges_ContinuationAfterStateClone_MatchesContinuousRun()
    {
        var cadence = RepeatCadence(new float[] { 1f, 0f, 1f, 0f, 0f, 1f, 1f, 0f }, 4);
        var split = cadence.Length / 2;

        var baselineState = CreateTwoNeuronCadenceState(localPredictiveTarget: true, targetBuffer: 0.35f, initialStrength: 0.2f);
        var baselineBackend = new RegionShardCpuBackend(baselineState);
        var routing = CreateCadenceRouting();

        var firstHalf = cadence[..split];
        var secondHalf = cadence[split..];
        _ = ExecuteCadenceTicks(
            baselineState,
            baselineBackend,
            routing,
            firstHalf,
            startingTickId: 1,
            plasticityEnabled: true,
            outputMagnitudes: null,
            deliverAfterLastTick: false);

        var clonedState = CloneState(baselineState);
        var resumedBackend = new RegionShardCpuBackend(clonedState);

        var baselineOutputs = new List<float>();
        var resumedOutputs = new List<float>();
        _ = ExecuteCadenceTicks(
            baselineState,
            baselineBackend,
            routing,
            secondHalf,
            startingTickId: (ulong)split + 1,
            plasticityEnabled: true,
            outputMagnitudes: baselineOutputs);
        _ = ExecuteCadenceTicks(
            clonedState,
            resumedBackend,
            routing,
            secondHalf,
            startingTickId: (ulong)split + 1,
            plasticityEnabled: true,
            outputMagnitudes: resumedOutputs);

        Assert.Equal(baselineState.Axons.Strengths[0], clonedState.Axons.Strengths[0], precision: 6);
        Assert.Equal(baselineState.Axons.RuntimeStrengthCodes[0], clonedState.Axons.RuntimeStrengthCodes[0]);
        Assert.Equal(baselineState.Axons.HasRuntimeOverlay[0], clonedState.Axons.HasRuntimeOverlay[0]);
        Assert.Equal(baselineOutputs.Count, resumedOutputs.Count);
        for (var i = 0; i < baselineOutputs.Count; i++)
        {
            Assert.Equal(baselineOutputs[i], resumedOutputs[i], precision: 6);
        }
    }

    public static IEnumerable<object[]> CadencePatterns()
    {
        yield return new object[] { "regular", RepeatCadence(new float[] { 1f }, 48) };
        yield return new object[] { "periodic", RepeatCadence(new float[] { 1f, 0f, 0f }, 16) };
        yield return new object[] { "bursty", RepeatCadence(new float[] { 0f, 0f, 1f, 1f, 1f, 0f, 0f, 0f }, 8) };
        yield return new object[] { "irregular", RepeatCadence(new float[] { 1f, 0f, 1f, 0f, 0f, 1f, 0f, 0f, 0f, 1f, 1f, 0f }, 6) };
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

    private static RegionShardState CreateTwoNeuronCadenceState(bool localPredictiveTarget, float targetBuffer, float initialStrength)
    {
        const int sourceRegionId = 8;
        const int remoteRegionId = 9;
        var destinationRegionId = localPredictiveTarget ? sourceRegionId : remoteRegionId;
        var destinationNeuronId = localPredictiveTarget ? 1 : 0;

        var regionSpans = new int[NbnConstants.RegionCount];
        regionSpans[sourceRegionId] = 2;
        regionSpans[remoteRegionId] = 1;

        var baseCode = (byte)QuantizationSchemas.DefaultNbn.Strength.Encode(initialStrength, bits: 5);
        return new RegionShardState(
            regionId: sourceRegionId,
            neuronStart: 0,
            neuronCount: 2,
            brainSeed: 0x8877665544332211UL,
            strengthQuantization: QuantizationSchemas.DefaultNbn.Strength,
            regionSpans: regionSpans,
            buffer: new[] { 0f, targetBuffer },
            enabled: new[] { true, true },
            exists: new[] { true, true },
            accumulationFunctions: new[] { (byte)AccumulationFunction.AccumSum, (byte)AccumulationFunction.AccumSum },
            activationFunctions: new[] { (byte)ActivationFunction.ActIdentity, (byte)ActivationFunction.ActIdentity },
            resetFunctions: new[] { (byte)ResetFunction.ResetHalf, (byte)ResetFunction.ResetHold },
            paramA: new[] { 0f, 0f },
            paramB: new[] { 0f, 0f },
            preActivationThreshold: new[] { 0.05f, 10f },
            activationThreshold: new[] { 0.1f, 0.9f },
            axonCounts: new ushort[] { 1, 0 },
            axonStartOffsets: new[] { 0, 1 },
            axons: new RegionShardAxons(
                targetRegionIds: new[] { (byte)destinationRegionId },
                targetNeuronIds: new[] { destinationNeuronId },
                strengths: new[] { initialStrength },
                baseStrengthCodes: new[] { baseCode },
                runtimeStrengthCodes: new[] { baseCode },
                hasRuntimeOverlay: new[] { false },
                fromAddress32: new[] { SharedAddress32.From(sourceRegionId, 0).Value },
                toAddress32: new[] { SharedAddress32.From(destinationRegionId, destinationNeuronId).Value }));
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

    private static RegionShardRoutingTable CreateCadenceRouting()
    {
        const int sourceRegionId = 8;
        const int remoteRegionId = 9;
        return new RegionShardRoutingTable(
            new Dictionary<int, ShardSpan[]>
            {
                [sourceRegionId] = new[] { new ShardSpan(0, 2, ShardId32.From(sourceRegionId, 0)) },
                [remoteRegionId] = new[] { new ShardSpan(0, 1, ShardId32.From(remoteRegionId, 0)) },
            });
    }

    private static CadenceSimulationResult RunCadenceSimulation(float[] cadencePattern, bool localPredictiveTarget)
    {
        var state = CreateTwoNeuronCadenceState(localPredictiveTarget, targetBuffer: 0.35f, initialStrength: 0.2f);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateCadenceRouting();
        var outputs = new List<float>(cadencePattern.Length);
        var activeOutputs = new List<float>();
        var gapReentryOutputs = new List<float>();

        _ = ExecuteCadenceTicks(
            state,
            backend,
            routing,
            cadencePattern,
            startingTickId: 1,
            plasticityEnabled: true,
            outputMagnitudes: outputs);

        for (var i = 0; i < cadencePattern.Length; i++)
        {
            if (cadencePattern[i] > 0f)
            {
                activeOutputs.Add(outputs[i]);
            }

            if (i > 0 && cadencePattern[i] > 0f && cadencePattern[i - 1] == 0f)
            {
                gapReentryOutputs.Add(outputs[i]);
            }
        }

        return new CadenceSimulationResult(
            FinalStrength: state.Axons.Strengths[0],
            RuntimeCode: state.Axons.RuntimeStrengthCodes[0],
            HasOverlay: state.Axons.HasRuntimeOverlay[0],
            ActiveAverageOutput: Average(activeOutputs),
            GapReentryAverageOutput: Average(gapReentryOutputs),
            GapReentrySampleCount: gapReentryOutputs.Count,
            OutputMagnitudes: outputs);
    }

    private static uint ExecuteCadenceTicks(
        RegionShardState state,
        RegionShardCpuBackend backend,
        RegionShardRoutingTable routing,
        float[] cadencePattern,
        ulong startingTickId,
        bool plasticityEnabled,
        List<float>? outputMagnitudes,
        bool deliverAfterLastTick = true)
    {
        uint totalStrengthCodeChanges = 0;
        for (var i = 0; i < cadencePattern.Length; i++)
        {
            var input = cadencePattern[i];
            if (input > 0f)
            {
                state.ApplyContribution(targetNeuronId: 0, value: input);
            }

            var tickId = startingTickId + (ulong)i;
            var result = backend.Compute(
                tickId: tickId,
                brainId: Guid.Parse("90d85c72-8a72-41ed-8be0-2dc85c63d5bb"),
                shardId: ShardId32.From(state.RegionId, 0),
                routing: routing,
                visualization: RegionShardVisualizationComputeScope.Disabled,
                plasticityEnabled: plasticityEnabled,
                plasticityRate: 0.2f,
                probabilisticPlasticityUpdates: false,
                plasticityDelta: 0.14f,
                homeostasisConfig: DisabledHomeostasis);

            totalStrengthCodeChanges += result.PlasticityStrengthCodeChanges;
            if (outputMagnitudes is not null)
            {
                outputMagnitudes.Add(SumMagnitude(result.Outbox));
            }

            var shouldDeliver = deliverAfterLastTick || i < cadencePattern.Length - 1;
            if (!shouldDeliver)
            {
                continue;
            }

            foreach (var (destinationShard, contributions) in result.Outbox)
            {
                if (destinationShard.RegionId != state.RegionId)
                {
                    continue;
                }

                foreach (var contribution in contributions)
                {
                    state.ApplyContribution(contribution.TargetNeuronId, contribution.Value);
                }
            }
        }

        return totalStrengthCodeChanges;
    }

    private static float[] RepeatCadence(float[] cadence, int repetitions)
    {
        var result = new float[cadence.Length * repetitions];
        for (var repeat = 0; repeat < repetitions; repeat++)
        {
            Array.Copy(cadence, 0, result, repeat * cadence.Length, cadence.Length);
        }

        return result;
    }

    private static float Average(List<float> values)
    {
        if (values.Count == 0)
        {
            return 0f;
        }

        var sum = 0f;
        foreach (var value in values)
        {
            sum += value;
        }

        return sum / values.Count;
    }

    private static float SumMagnitude(Dictionary<ShardId32, List<Contribution>> outbox)
    {
        var total = 0f;
        foreach (var contributions in outbox.Values)
        {
            foreach (var contribution in contributions)
            {
                total += MathF.Abs(contribution.Value);
            }
        }

        return total;
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

    private static ulong FindTickWithSampleLessThan(RegionShardState state, float threshold)
    {
        for (ulong tickId = 1; tickId < 100_000; tickId++)
        {
            var sample = UnitIntervalFromSeed(state.GetDeterministicRngInput(tickId, axonIndex: 0).ToSeed());
            if (sample < threshold)
            {
                return tickId;
            }
        }

        throw new InvalidOperationException($"Unable to find deterministic RNG sample < {threshold}.");
    }

    private static float UnitIntervalFromSeed(ulong seed)
    {
        const double scale = 1d / (1UL << 53);
        var bits = seed >> 11;
        return (float)(bits * scale);
    }

    private readonly record struct CadenceSimulationResult(
        float FinalStrength,
        byte RuntimeCode,
        bool HasOverlay,
        float ActiveAverageOutput,
        float GapReentryAverageOutput,
        int GapReentrySampleCount,
        IReadOnlyList<float> OutputMagnitudes);
}
