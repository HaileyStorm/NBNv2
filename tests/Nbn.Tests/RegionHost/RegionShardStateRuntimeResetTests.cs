using System.Reflection;
using Nbn.Proto;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.Quantization;
using SharedAddress32 = Nbn.Shared.Addressing.Address32;

namespace Nbn.Tests.RegionHost;

public sealed class RegionShardStateRuntimeResetTests
{
    [Fact]
    public void ResetRuntimeState_ClearsBufferAndAccumulator_WhenRequested()
    {
        var state = CreateState();
        Assert.True(state.TrySetRuntimeNeuronState(targetNeuronId: 11, setBuffer: true, bufferValue: 0.75f, setAccumulator: true, accumulatorValue: -0.25f));

        state.ResetRuntimeState(resetBuffer: true, resetAccumulator: true);

        Assert.All(state.Buffer, value => Assert.Equal(0f, value));
        Assert.All(ReadInbox(state), value => Assert.Equal(0f, value));
        Assert.All(ReadInboxHasInput(state), value => Assert.False(value));
    }

    [Fact]
    public void ResetRuntimeState_CanLeaveBufferUntouched_WhenOnlyAccumulatorResetRequested()
    {
        var state = CreateState();
        Assert.True(state.TrySetRuntimeNeuronState(targetNeuronId: 10, setBuffer: true, bufferValue: 0.5f, setAccumulator: true, accumulatorValue: 1.5f));

        state.ResetRuntimeState(resetBuffer: false, resetAccumulator: true);

        Assert.Contains(0.5f, state.Buffer);
        Assert.All(ReadInbox(state), value => Assert.Equal(0f, value));
        Assert.All(ReadInboxHasInput(state), value => Assert.False(value));
    }

    private static RegionShardState CreateState()
    {
        const int regionId = 8;
        const int neuronStart = 10;
        var regionSpans = new int[NbnConstants.RegionCount];
        regionSpans[regionId] = 2;
        regionSpans[31] = 1;

        return new RegionShardState(
            regionId: regionId,
            neuronStart: neuronStart,
            neuronCount: 2,
            brainSeed: 0x0102030405060708UL,
            strengthQuantization: QuantizationSchemas.DefaultNbn.Strength,
            regionSpans: regionSpans,
            buffer: new[] { 0.1f, -0.2f },
            enabled: new[] { true, true },
            exists: new[] { true, true },
            accumulationFunctions: new[] { (byte)AccumulationFunction.AccumSum, (byte)AccumulationFunction.AccumSum },
            activationFunctions: new[] { (byte)ActivationFunction.ActIdentity, (byte)ActivationFunction.ActIdentity },
            resetFunctions: new[] { (byte)ResetFunction.ResetHold, (byte)ResetFunction.ResetHold },
            paramA: new[] { 0f, 0f },
            paramB: new[] { 0f, 0f },
            preActivationThreshold: new[] { -1f, -1f },
            activationThreshold: new[] { 0.1f, 0.1f },
            axonCounts: new ushort[] { 1, 0 },
            axonStartOffsets: new[] { 0, 1 },
            axons: new RegionShardAxons(
                targetRegionIds: new byte[] { 31 },
                targetNeuronIds: new[] { 0 },
                strengths: new[] { QuantizationSchemas.DefaultNbn.Strength.Decode(16, 5) },
                baseStrengthCodes: new byte[] { 16 },
                runtimeStrengthCodes: new byte[] { 16 },
                hasRuntimeOverlay: new[] { false },
                fromAddress32: new[] { SharedAddress32.From(regionId, neuronStart).Value },
                toAddress32: new[] { SharedAddress32.From(31, 0).Value }));
    }

    private static float[] ReadInbox(RegionShardState state)
        => (float[])(typeof(RegionShardState)
            .GetField("_inbox", BindingFlags.Instance | BindingFlags.NonPublic)
            ?.GetValue(state)
            ?? throw new InvalidOperationException("RegionShardState._inbox was not found."));

    private static bool[] ReadInboxHasInput(RegionShardState state)
        => (bool[])(typeof(RegionShardState)
            .GetField("_inboxHasInput", BindingFlags.Instance | BindingFlags.NonPublic)
            ?.GetValue(state)
            ?? throw new InvalidOperationException("RegionShardState._inboxHasInput was not found."));
}
