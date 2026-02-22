using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.Quantization;
using Proto;
using SharedAddress32 = Nbn.Shared.Addressing.Address32;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Tests.RegionHost;

public class RegionShardSnapshotCaptureTests
{
    [Fact]
    public async Task CaptureShardSnapshot_Returns_BufferEnabledAndOverlayDeltas()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();
        var shardId = ShardId32.From(regionId: 5, shardIndex: 2);
        var state = CreateState(regionId: 5, neuronStart: 10);

        var actor = root.Spawn(Props.FromProducer(() => new RegionShardActor(
            state,
            new RegionShardActorConfig(
                BrainId: brainId,
                ShardId: shardId,
                Router: null,
                OutputSink: null,
                TickSink: null,
                Routing: RegionShardRoutingTable.CreateSingleShard(state.RegionId, state.NeuronCount)))));

        var ack = await root.RequestAsync<CaptureShardSnapshotAck>(
            actor,
            new CaptureShardSnapshot
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = (uint)state.RegionId,
                ShardIndex = (uint)shardId.ShardIndex,
                TickId = 99
            });

        Assert.True(ack.Success);
        Assert.Equal((uint)state.RegionId, ack.RegionId);
        Assert.Equal((uint)shardId.ShardIndex, ack.ShardIndex);
        Assert.Equal((uint)state.NeuronStart, ack.NeuronStart);
        Assert.Equal((uint)state.NeuronCount, ack.NeuronCount);

        Assert.Equal(state.NeuronCount, ack.BufferCodes.Count);
        Assert.Equal(QuantizationSchemas.DefaultBuffer.Encode(0.25f, 16), ack.BufferCodes[0]);
        Assert.Equal(QuantizationSchemas.DefaultBuffer.Encode(-0.5f, 16), ack.BufferCodes[1]);
        Assert.Equal(QuantizationSchemas.DefaultBuffer.Encode(0f, 16), ack.BufferCodes[2]);

        Assert.Equal(new byte[] { 0b0000_0101 }, ack.EnabledBitset.ToByteArray());

        var overlay = Assert.Single(ack.Overlays);
        Assert.Equal(SharedAddress32.From(state.RegionId, state.NeuronStart).Value, overlay.FromAddress);
        Assert.Equal(SharedAddress32.From(31, 0).Value, overlay.ToAddress);
        Assert.Equal((uint)19, overlay.StrengthCode);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task CaptureShardSnapshot_WithBrainMismatch_Returns_Error()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var state = CreateState(regionId: 5, neuronStart: 10);
        var shardId = ShardId32.From(regionId: 5, shardIndex: 2);
        var actor = root.Spawn(Props.FromProducer(() => new RegionShardActor(
            state,
            new RegionShardActorConfig(
                BrainId: Guid.NewGuid(),
                ShardId: shardId,
                Router: null,
                OutputSink: null,
                TickSink: null,
                Routing: RegionShardRoutingTable.CreateSingleShard(state.RegionId, state.NeuronCount)))));

        var ack = await root.RequestAsync<CaptureShardSnapshotAck>(
            actor,
            new CaptureShardSnapshot
            {
                BrainId = Guid.NewGuid().ToProtoUuid(),
                RegionId = (uint)state.RegionId,
                ShardIndex = (uint)shardId.ShardIndex,
                TickId = 17
            });

        Assert.False(ack.Success);
        Assert.Equal("brain_id_mismatch", ack.Error);

        await system.ShutdownAsync();
    }

    private static RegionShardState CreateState(int regionId, int neuronStart)
    {
        var regionSpans = new int[Nbn.Shared.NbnConstants.RegionCount];
        regionSpans[regionId] = 3;
        regionSpans[31] = 1;

        return new RegionShardState(
            regionId: regionId,
            neuronStart: neuronStart,
            neuronCount: 3,
            brainSeed: 0x0908070605040302UL,
            strengthQuantization: QuantizationSchemas.DefaultNbn.Strength,
            regionSpans: regionSpans,
            buffer: new[] { 0.25f, -0.5f, float.NaN },
            enabled: new[] { true, false, true },
            exists: new[] { true, true, true },
            accumulationFunctions: new[] { (byte)AccumulationFunction.AccumSum, (byte)AccumulationFunction.AccumSum, (byte)AccumulationFunction.AccumSum },
            activationFunctions: new[] { (byte)ActivationFunction.ActIdentity, (byte)ActivationFunction.ActIdentity, (byte)ActivationFunction.ActIdentity },
            resetFunctions: new[] { (byte)ResetFunction.ResetHold, (byte)ResetFunction.ResetHold, (byte)ResetFunction.ResetHold },
            paramA: new[] { 0f, 0f, 0f },
            paramB: new[] { 0f, 0f, 0f },
            preActivationThreshold: new[] { -1f, -1f, -1f },
            activationThreshold: new[] { 0.1f, 0.1f, 0.1f },
            axonCounts: new ushort[] { 2, 0, 0 },
            axonStartOffsets: new[] { 0, 2, 2 },
            axons: new RegionShardAxons(
                targetRegionIds: new byte[] { 31, 31 },
                targetNeuronIds: new[] { 0, 0 },
                strengths: new[] { QuantizationSchemas.DefaultNbn.Strength.Decode(19, 5), QuantizationSchemas.DefaultNbn.Strength.Decode(12, 5) },
                baseStrengthCodes: new byte[] { 10, 12 },
                runtimeStrengthCodes: new byte[] { 19, 12 },
                hasRuntimeOverlay: new[] { true, false },
                fromAddress32: new[] { SharedAddress32.From(regionId, neuronStart).Value, SharedAddress32.From(regionId, neuronStart).Value },
                toAddress32: new[] { SharedAddress32.From(31, 0).Value, SharedAddress32.From(31, 0).Value }));
    }
}
