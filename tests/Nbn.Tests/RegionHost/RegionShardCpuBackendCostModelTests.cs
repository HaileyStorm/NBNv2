using Nbn.Proto;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.Quantization;
using ProtoControl = Nbn.Proto.Control;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;
using SharedAddress32 = Nbn.Shared.Addressing.Address32;

namespace Nbn.Tests.RegionHost;

public class RegionShardCpuBackendCostModelTests
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
    public void Compute_WhenCostEnergyDisabled_ReportsZeroCostComponents()
    {
        var disabledState = CreateSingleNeuronState();
        var enabledState = CreateSingleNeuronState();
        var routing = CreateRouting(disabledState.RegionId, disabledState.NeuronCount);
        var shardId = ShardId32.From(disabledState.RegionId, 0);

        var disabledBackend = new RegionShardCpuBackend(disabledState);
        var disabledResult = disabledBackend.Compute(
            tickId: 1,
            brainId: Guid.NewGuid(),
            shardId: shardId,
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            homeostasisConfig: DisabledHomeostasis,
            costEnergyEnabled: false,
            remoteCostEnabled: true,
            remoteCostPerBatch: 5,
            remoteCostPerContribution: 2);

        var enabledBackend = new RegionShardCpuBackend(enabledState);
        var enabledResult = enabledBackend.Compute(
            tickId: 1,
            brainId: Guid.NewGuid(),
            shardId: shardId,
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            homeostasisConfig: DisabledHomeostasis,
            costEnergyEnabled: true,
            remoteCostEnabled: true,
            remoteCostPerBatch: 5,
            remoteCostPerContribution: 2);

        Assert.Equal(0L, disabledResult.Cost.Total);
        Assert.Equal(0L, disabledResult.Cost.Accum);
        Assert.Equal(0L, disabledResult.Cost.Activation);
        Assert.Equal(0L, disabledResult.Cost.Reset);
        Assert.Equal(0L, disabledResult.Cost.Distance);
        Assert.Equal(0L, disabledResult.Cost.Remote);

        Assert.True(enabledResult.Cost.Total > 0);
        Assert.True(enabledResult.Cost.Accum > 0);
        Assert.True(enabledResult.Cost.Activation > 0);
        Assert.True(enabledResult.Cost.Reset > 0);
    }

    [Fact]
    public void Compute_WithTierMultipliers_WeightsTierCFunctionsHigherThanTierA()
    {
        var baselineState = CreateSingleNeuronState(
            activationFunction: ActivationFunction.ActIdentity,
            resetFunction: ResetFunction.ResetHold);
        var expensiveState = CreateSingleNeuronState(
            activationFunction: ActivationFunction.ActQuad,
            resetFunction: ResetFunction.ResetInverse);
        var routing = CreateRouting(baselineState.RegionId, baselineState.NeuronCount);
        var shardId = ShardId32.From(baselineState.RegionId, 0);

        var baselineBackend = new RegionShardCpuBackend(baselineState);
        var baseline = baselineBackend.Compute(
            tickId: 5,
            brainId: Guid.NewGuid(),
            shardId: shardId,
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            homeostasisConfig: DisabledHomeostasis,
            costEnergyEnabled: true,
            costTierAMultiplier: 1f,
            costTierBMultiplier: 1f,
            costTierCMultiplier: 2f);

        var expensiveBackend = new RegionShardCpuBackend(expensiveState);
        var expensive = expensiveBackend.Compute(
            tickId: 5,
            brainId: Guid.NewGuid(),
            shardId: shardId,
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            homeostasisConfig: DisabledHomeostasis,
            costEnergyEnabled: true,
            costTierAMultiplier: 1f,
            costTierBMultiplier: 1f,
            costTierCMultiplier: 2f);

        Assert.True(expensive.Cost.Activation > baseline.Cost.Activation);
        Assert.True(expensive.Cost.Reset > baseline.Cost.Reset);
        Assert.True(expensive.Cost.Total > baseline.Cost.Total);
    }

    [Fact]
    public void Compute_WithRemoteCostEnabled_ChargesOnlyRemoteBatchAndContributionCounts()
    {
        var axons = new[]
        {
            new AxonSpec(TargetRegionId: 8, TargetNeuronId: 0, Strength: 0.5f),
            new AxonSpec(TargetRegionId: 9, TargetNeuronId: 0, Strength: 0.5f),
            new AxonSpec(TargetRegionId: 9, TargetNeuronId: 1, Strength: 0.5f)
        };
        var remoteEnabledState = CreateSingleNeuronState(axons: axons);
        var remoteDisabledState = CreateSingleNeuronState(axons: axons);
        var sourceShard = ShardId32.From(remoteEnabledState.RegionId, 0);
        var remoteShard = ShardId32.From(9, 0);
        var routing = CreateRouting(
            remoteEnabledState.RegionId,
            remoteEnabledState.NeuronCount,
            (RegionId: 9, Count: 2, ShardId: remoteShard));

        var remoteEnabledBackend = new RegionShardCpuBackend(remoteEnabledState);
        var remoteEnabled = remoteEnabledBackend.Compute(
            tickId: 7,
            brainId: Guid.NewGuid(),
            shardId: sourceShard,
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            homeostasisConfig: DisabledHomeostasis,
            costEnergyEnabled: true,
            remoteCostEnabled: true,
            remoteCostPerBatch: 5,
            remoteCostPerContribution: 2);

        var remoteDisabledBackend = new RegionShardCpuBackend(remoteDisabledState);
        var remoteDisabled = remoteDisabledBackend.Compute(
            tickId: 7,
            brainId: Guid.NewGuid(),
            shardId: sourceShard,
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled,
            homeostasisConfig: DisabledHomeostasis,
            costEnergyEnabled: true,
            remoteCostEnabled: false,
            remoteCostPerBatch: 5,
            remoteCostPerContribution: 2);

        Assert.True(remoteEnabled.Outbox.ContainsKey(sourceShard));
        Assert.True(remoteEnabled.Outbox.ContainsKey(remoteShard));
        Assert.Equal(2, remoteEnabled.Outbox[remoteShard].Count);
        Assert.Equal(9L, remoteEnabled.Cost.Remote);
        Assert.Equal(0L, remoteDisabled.Cost.Remote);
        Assert.Equal(remoteEnabled.Cost.Total - 9L, remoteDisabled.Cost.Total);
    }

    private readonly record struct AxonSpec(int TargetRegionId, int TargetNeuronId, float Strength);

    private static RegionShardState CreateSingleNeuronState(
        float buffer = 0.9f,
        float preActivationThreshold = 0.1f,
        float activationThreshold = 0.2f,
        AccumulationFunction accumulationFunction = AccumulationFunction.AccumSum,
        ActivationFunction activationFunction = ActivationFunction.ActIdentity,
        ResetFunction resetFunction = ResetFunction.ResetHold,
        AxonSpec[]? axons = null,
        ulong brainSeed = 0x0102030405060708UL)
    {
        const int sourceRegionId = 8;
        axons ??= Array.Empty<AxonSpec>();

        var regionSpans = new int[NbnConstants.RegionCount];
        regionSpans[sourceRegionId] = 1;

        var targetRegionIds = new byte[axons.Length];
        var targetNeuronIds = new int[axons.Length];
        var strengths = new float[axons.Length];
        var baseStrengthCodes = new byte[axons.Length];
        var runtimeStrengthCodes = new byte[axons.Length];
        var hasRuntimeOverlay = new bool[axons.Length];
        var fromAddress32 = new uint[axons.Length];
        var toAddress32 = new uint[axons.Length];

        for (var i = 0; i < axons.Length; i++)
        {
            var axon = axons[i];
            targetRegionIds[i] = (byte)axon.TargetRegionId;
            targetNeuronIds[i] = axon.TargetNeuronId;
            strengths[i] = axon.Strength;

            var strengthCode = (byte)QuantizationSchemas.DefaultNbn.Strength.Encode(axon.Strength, bits: 5);
            baseStrengthCodes[i] = strengthCode;
            runtimeStrengthCodes[i] = strengthCode;
            hasRuntimeOverlay[i] = false;

            fromAddress32[i] = SharedAddress32.From(sourceRegionId, 0).Value;
            toAddress32[i] = SharedAddress32.From(axon.TargetRegionId, axon.TargetNeuronId).Value;
            regionSpans[axon.TargetRegionId] = Math.Max(regionSpans[axon.TargetRegionId], axon.TargetNeuronId + 1);
        }

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
            accumulationFunctions: new[] { (byte)accumulationFunction },
            activationFunctions: new[] { (byte)activationFunction },
            resetFunctions: new[] { (byte)resetFunction },
            paramA: new[] { 0f },
            paramB: new[] { 0f },
            preActivationThreshold: new[] { preActivationThreshold },
            activationThreshold: new[] { activationThreshold },
            axonCounts: new ushort[] { (ushort)axons.Length },
            axonStartOffsets: new[] { 0 },
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

    private static RegionShardRoutingTable CreateRouting(
        int sourceRegionId,
        int sourceCount,
        params (int RegionId, int Count, ShardId32 ShardId)[] additionalSpans)
    {
        var shardMap = new Dictionary<int, ShardSpan[]>
        {
            [sourceRegionId] = new[] { new ShardSpan(0, sourceCount, ShardId32.From(sourceRegionId, 0)) }
        };

        foreach (var span in additionalSpans)
        {
            if (shardMap.ContainsKey(span.RegionId))
            {
                continue;
            }

            shardMap[span.RegionId] = new[] { new ShardSpan(0, span.Count, span.ShardId) };
        }

        return new RegionShardRoutingTable(shardMap);
    }
}
