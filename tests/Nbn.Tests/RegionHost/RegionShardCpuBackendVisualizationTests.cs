using Nbn.Proto;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
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
        Assert.Empty(result.FiredNeuronEvents);
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
        Assert.Empty(result.FiredNeuronEvents);
        Assert.Equal((uint)destRegionId, route.TargetAddress >> NbnConstants.AddressNeuronBits);
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
