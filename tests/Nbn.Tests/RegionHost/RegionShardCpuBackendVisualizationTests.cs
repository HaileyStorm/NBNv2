using Nbn.Proto;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.Quantization;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;
using SharedAddress32 = Nbn.Shared.Addressing.Address32;

namespace Nbn.Tests.RegionHost;

public class RegionShardCpuBackendVisualizationTests
{
    [Fact]
    public void Compute_WithVisualizationDisabled_SkipsVizAggregationOutputs()
    {
        var state = CreateSingleNeuronState(sourceRegionId: 8, destRegionId: 9);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(state.RegionId, state.NeuronCount, 9, 1);

        var result = backend.Compute(
            tickId: 15,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled);

        Assert.Equal(1u, result.FiredCount);
        Assert.Equal(1u, result.OutContribs);
        Assert.NotEmpty(result.Outbox);
        Assert.Empty(result.AxonVizEvents);
        Assert.Empty(result.BufferNeuronEvents);
        Assert.Empty(result.FiredNeuronEvents);
    }

    [Fact]
    public void Compute_WithActAddBaselineDriver_FiresFromZeroBuffer()
    {
        const int sourceRegionId = 8;
        const int destRegionId = 9;
        var state = CreateActAddBaselineState(sourceRegionId, destRegionId);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(sourceRegionId, sourceCount: 1, destRegionId, destCount: 1);

        var result = backend.Compute(
            tickId: 16,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(sourceRegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.Disabled);

        Assert.Equal(1u, result.FiredCount);
        Assert.Equal(1u, result.OutContribs);
        Assert.NotEmpty(result.Outbox);
    }

    [Fact]
    public void Compute_WithFocusScopeOutsideRoute_SkipsVizOutputs()
    {
        var state = CreateSingleNeuronState(sourceRegionId: 8, destRegionId: 9);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(state.RegionId, state.NeuronCount, 9, 1);

        var result = backend.Compute(
            tickId: 18,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: new RegionShardVisualizationComputeScope(Enabled: true, FocusRegionId: 7));

        Assert.Empty(result.AxonVizEvents);
        Assert.Empty(result.BufferNeuronEvents);
        Assert.Empty(result.FiredNeuronEvents);
    }

    [Fact]
    public void Compute_WithFocusScopeOnDestination_EmitsOnlyRouteViz()
    {
        const int sourceRegionId = 8;
        const int destRegionId = 9;
        var state = CreateSingleNeuronState(sourceRegionId, destRegionId);
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(state.RegionId, state.NeuronCount, destRegionId, 1);

        var result = backend.Compute(
            tickId: 22,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(state.RegionId, 0),
            routing: routing,
            visualization: new RegionShardVisualizationComputeScope(Enabled: true, FocusRegionId: destRegionId));

        Assert.NotEmpty(result.Outbox);
        var route = Assert.Single(result.AxonVizEvents);
        Assert.Empty(result.BufferNeuronEvents);
        Assert.Empty(result.FiredNeuronEvents);
        Assert.Equal((uint)destRegionId, route.TargetAddress >> NbnConstants.AddressNeuronBits);
    }

    [Fact]
    public void Compute_WithFocusScopeOnSource_EmitsBufferVizEvenWhenNeuronDoesNotFire()
    {
        const int sourceRegionId = 8;
        const int destRegionId = 9;
        var state = CreateSingleNeuronState(sourceRegionId, destRegionId);
        state.Buffer[0] = 0.5f;
        state.PreActivationThreshold[0] = 0.9f;
        state.ActivationThreshold[0] = 0.9f;
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(sourceRegionId, sourceCount: 1, destRegionId, destCount: 1);

        var result = backend.Compute(
            tickId: 22,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(sourceRegionId, 0),
            routing: routing,
            visualization: new RegionShardVisualizationComputeScope(Enabled: true, FocusRegionId: sourceRegionId));

        var buffer = Assert.Single(result.BufferNeuronEvents);
        Assert.Equal(22UL, buffer.TickId);
        Assert.Equal((uint)sourceRegionId, buffer.SourceAddress >> NbnConstants.AddressNeuronBits);
        Assert.Equal(0.5f, buffer.Buffer);
        Assert.Empty(result.FiredNeuronEvents);
    }

    [Fact]
    public void Compute_WithFocusScopeOnSource_EmitsBufferVizOnFirstTick_AndWhenValueChanges()
    {
        const int sourceRegionId = 8;
        const int destRegionId = 9;
        var state = CreateSingleNeuronState(sourceRegionId, destRegionId);
        state.Buffer[0] = 0f;
        state.PreActivationThreshold[0] = 0.5f;
        state.ActivationThreshold[0] = 0.5f;
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(sourceRegionId, sourceCount: 1, destRegionId, destCount: 1);

        var first = backend.Compute(
            tickId: 30,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(sourceRegionId, 0),
            routing: routing,
            visualization: new RegionShardVisualizationComputeScope(Enabled: true, FocusRegionId: sourceRegionId));
        var second = backend.Compute(
            tickId: 31,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(sourceRegionId, 0),
            routing: routing,
            visualization: new RegionShardVisualizationComputeScope(Enabled: true, FocusRegionId: sourceRegionId));
        state.Buffer[0] = 0.25f;
        var third = backend.Compute(
            tickId: 32,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(sourceRegionId, 0),
            routing: routing,
            visualization: new RegionShardVisualizationComputeScope(Enabled: true, FocusRegionId: sourceRegionId));

        var firstBuffer = Assert.Single(first.BufferNeuronEvents);
        var thirdBuffer = Assert.Single(third.BufferNeuronEvents);
        Assert.Equal(0f, firstBuffer.Buffer);
        Assert.Empty(second.BufferNeuronEvents);
        Assert.Equal(0.25f, thirdBuffer.Buffer);
        Assert.Equal(30UL, firstBuffer.TickId);
        Assert.Equal(32UL, thirdBuffer.TickId);
        Assert.Empty(first.FiredNeuronEvents);
        Assert.Empty(second.FiredNeuronEvents);
        Assert.Empty(third.FiredNeuronEvents);
    }

    [Fact]
    public void Compute_WithVisualizationEnabledAll_EmitsBufferVizOnFirstTickOnly_WhenValueUnchanged()
    {
        const int sourceRegionId = 8;
        const int destRegionId = 9;
        var state = CreateSingleNeuronState(sourceRegionId, destRegionId);
        state.Buffer[0] = 0f;
        state.PreActivationThreshold[0] = 0.5f;
        state.ActivationThreshold[0] = 0.5f;
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(sourceRegionId, sourceCount: 1, destRegionId, destCount: 1);

        var first = backend.Compute(
            tickId: 40,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(sourceRegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.EnabledAll);
        var second = backend.Compute(
            tickId: 41,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(sourceRegionId, 0),
            routing: routing,
            visualization: RegionShardVisualizationComputeScope.EnabledAll);

        var firstBuffer = Assert.Single(first.BufferNeuronEvents);
        Assert.Equal(0f, firstBuffer.Buffer);
        Assert.Equal(40UL, firstBuffer.TickId);
        Assert.Empty(second.BufferNeuronEvents);
        Assert.Empty(first.FiredNeuronEvents);
        Assert.Empty(second.FiredNeuronEvents);
    }

    [Fact]
    public void Compute_WhenFocusScopeIsReenabled_EmitsBaselineBufferAgain()
    {
        const int sourceRegionId = 8;
        const int destRegionId = 9;
        var state = CreateSingleNeuronState(sourceRegionId, destRegionId);
        state.Buffer[0] = 0.125f;
        state.PreActivationThreshold[0] = 0.5f;
        state.ActivationThreshold[0] = 0.5f;
        var backend = new RegionShardCpuBackend(state);
        var routing = CreateRouting(sourceRegionId, sourceCount: 1, destRegionId, destCount: 1);

        var focused = backend.Compute(
            tickId: 50,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(sourceRegionId, 0),
            routing: routing,
            visualization: new RegionShardVisualizationComputeScope(Enabled: true, FocusRegionId: sourceRegionId));
        var unfocused = backend.Compute(
            tickId: 51,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(sourceRegionId, 0),
            routing: routing,
            visualization: new RegionShardVisualizationComputeScope(Enabled: true, FocusRegionId: 7));
        var refocused = backend.Compute(
            tickId: 52,
            brainId: Guid.NewGuid(),
            shardId: ShardId32.From(sourceRegionId, 0),
            routing: routing,
            visualization: new RegionShardVisualizationComputeScope(Enabled: true, FocusRegionId: sourceRegionId));

        var first = Assert.Single(focused.BufferNeuronEvents);
        var second = Assert.Single(refocused.BufferNeuronEvents);
        Assert.Equal(0.125f, first.Buffer);
        Assert.Equal(0.125f, second.Buffer);
        Assert.Equal(50UL, first.TickId);
        Assert.Equal(52UL, second.TickId);
        Assert.Empty(unfocused.BufferNeuronEvents);
    }

    private static RegionShardState CreateSingleNeuronState(int sourceRegionId, int destRegionId)
    {
        var regionSpans = new int[NbnConstants.RegionCount];
        regionSpans[sourceRegionId] = 1;
        regionSpans[destRegionId] = 1;

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
                strengths: new[] { 0.5f },
                baseStrengthCodes: new byte[] { 16 },
                runtimeStrengthCodes: new byte[] { 16 },
                hasRuntimeOverlay: new[] { false },
                fromAddress32: new[] { SharedAddress32.From(sourceRegionId, 0).Value },
                toAddress32: new[] { SharedAddress32.From(destRegionId, 0).Value }));
    }

    private static RegionShardState CreateActAddBaselineState(int sourceRegionId, int destRegionId)
    {
        var regionSpans = new int[NbnConstants.RegionCount];
        regionSpans[sourceRegionId] = 1;
        regionSpans[destRegionId] = 1;

        return new RegionShardState(
            regionId: sourceRegionId,
            neuronStart: 0,
            neuronCount: 1,
            brainSeed: 0x1122334455667788UL,
            strengthQuantization: QuantizationSchemas.DefaultNbn.Strength,
            regionSpans: regionSpans,
            buffer: new[] { 0f },
            enabled: new[] { true },
            exists: new[] { true },
            accumulationFunctions: new[] { (byte)AccumulationFunction.AccumSum },
            activationFunctions: new[] { (byte)ActivationFunction.ActAdd },
            resetFunctions: new[] { (byte)ResetFunction.ResetHold },
            paramA: new[] { 1f },
            paramB: new[] { 0f },
            preActivationThreshold: new[] { -1f },
            activationThreshold: new[] { 0.1f },
            axonCounts: new ushort[] { 1 },
            axonStartOffsets: new[] { 0 },
            axons: new RegionShardAxons(
                targetRegionIds: new[] { (byte)destRegionId },
                targetNeuronIds: new[] { 0 },
                strengths: new[] { 0.5f },
                baseStrengthCodes: new byte[] { 16 },
                runtimeStrengthCodes: new byte[] { 16 },
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
}
