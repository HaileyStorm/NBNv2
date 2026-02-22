using Nbn.Proto;
using Nbn.Shared;
using Nbn.Shared.Quantization;

namespace Nbn.Runtime.RegionHost;

public sealed class RegionShardState
{
    public RegionShardState(
        int regionId,
        int neuronStart,
        int neuronCount,
        ulong brainSeed,
        QuantizationMap strengthQuantization,
        int[] regionSpans,
        float[] buffer,
        bool[] enabled,
        bool[] exists,
        byte[] accumulationFunctions,
        byte[] activationFunctions,
        byte[] resetFunctions,
        float[] paramA,
        float[] paramB,
        float[] preActivationThreshold,
        float[] activationThreshold,
        ushort[] axonCounts,
        int[] axonStartOffsets,
        RegionShardAxons axons)
    {
        RegionId = regionId;
        NeuronStart = neuronStart;
        NeuronCount = neuronCount;
        BrainSeed = brainSeed;
        StrengthQuantization = strengthQuantization;
        RegionSpans = regionSpans ?? throw new ArgumentNullException(nameof(regionSpans));
        Buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
        Enabled = enabled ?? throw new ArgumentNullException(nameof(enabled));
        Exists = exists ?? throw new ArgumentNullException(nameof(exists));
        AccumulationFunctions = accumulationFunctions ?? throw new ArgumentNullException(nameof(accumulationFunctions));
        ActivationFunctions = activationFunctions ?? throw new ArgumentNullException(nameof(activationFunctions));
        ResetFunctions = resetFunctions ?? throw new ArgumentNullException(nameof(resetFunctions));
        ParamA = paramA ?? throw new ArgumentNullException(nameof(paramA));
        ParamB = paramB ?? throw new ArgumentNullException(nameof(paramB));
        PreActivationThreshold = preActivationThreshold ?? throw new ArgumentNullException(nameof(preActivationThreshold));
        ActivationThreshold = activationThreshold ?? throw new ArgumentNullException(nameof(activationThreshold));
        AxonCounts = axonCounts ?? throw new ArgumentNullException(nameof(axonCounts));
        AxonStartOffsets = axonStartOffsets ?? throw new ArgumentNullException(nameof(axonStartOffsets));
        Axons = axons ?? throw new ArgumentNullException(nameof(axons));

        if (RegionSpans.Length != NbnConstants.RegionCount)
        {
            throw new ArgumentException("Region spans must include 32 entries.", nameof(regionSpans));
        }

        if (Buffer.Length != neuronCount
            || Enabled.Length != neuronCount
            || Exists.Length != neuronCount
            || AccumulationFunctions.Length != neuronCount
            || ActivationFunctions.Length != neuronCount
            || ResetFunctions.Length != neuronCount
            || ParamA.Length != neuronCount
            || ParamB.Length != neuronCount
            || PreActivationThreshold.Length != neuronCount
            || ActivationThreshold.Length != neuronCount
            || AxonCounts.Length != neuronCount
            || AxonStartOffsets.Length != neuronCount)
        {
            throw new ArgumentException("Neuron arrays must match neuron count.");
        }

        var expectedAxonCount = 0;
        for (var i = 0; i < AxonCounts.Length; i++)
        {
            expectedAxonCount += AxonCounts[i];
        }

        if (Axons.Count != expectedAxonCount)
        {
            throw new ArgumentException("Axon arrays must match summed axon counts.", nameof(axons));
        }

        _inbox = new float[neuronCount];
        _inboxHasInput = new bool[neuronCount];
    }

    public int RegionId { get; }
    public int NeuronStart { get; }
    public int NeuronCount { get; }
    public ulong BrainSeed { get; }
    public QuantizationMap StrengthQuantization { get; }
    public int[] RegionSpans { get; }

    public float[] Buffer { get; }
    public bool[] Enabled { get; }
    public bool[] Exists { get; }

    public byte[] AccumulationFunctions { get; }
    public byte[] ActivationFunctions { get; }
    public byte[] ResetFunctions { get; }

    public float[] ParamA { get; }
    public float[] ParamB { get; }
    public float[] PreActivationThreshold { get; }
    public float[] ActivationThreshold { get; }

    public ushort[] AxonCounts { get; }
    public int[] AxonStartOffsets { get; }
    public RegionShardAxons Axons { get; }

    internal float[] Inbox => _inbox;
    internal bool[] InboxHasInput => _inboxHasInput;

    public bool IsOutputRegion => RegionId == NbnConstants.OutputRegionId;

    private readonly float[] _inbox;
    private readonly bool[] _inboxHasInput;

    public void ApplyContribution(uint targetNeuronId, float value)
    {
        if (!TryGetLocalNeuronIndex(targetNeuronId, out var localIndex))
        {
            return;
        }

        var accum = (AccumulationFunction)AccumulationFunctions[localIndex];
        switch (accum)
        {
            case AccumulationFunction.AccumSum:
                _inbox[localIndex] += value;
                break;
            case AccumulationFunction.AccumProduct:
                if (!_inboxHasInput[localIndex])
                {
                    _inbox[localIndex] = value;
                    _inboxHasInput[localIndex] = true;
                }
                else
                {
                    _inbox[localIndex] *= value;
                }
                break;
            case AccumulationFunction.AccumMax:
                if (!_inboxHasInput[localIndex])
                {
                    _inbox[localIndex] = value;
                    _inboxHasInput[localIndex] = true;
                }
                else if (value > _inbox[localIndex])
                {
                    _inbox[localIndex] = value;
                }
                break;
            case AccumulationFunction.AccumNone:
                break;
            default:
                _inbox[localIndex] += value;
                break;
        }
    }

    public bool TryApplyRuntimePulse(uint targetNeuronId, float value)
    {
        if (!float.IsFinite(value))
        {
            return false;
        }

        if (!TryGetLocalNeuronIndex(targetNeuronId, out _))
        {
            return false;
        }

        ApplyContribution(targetNeuronId, value);
        return true;
    }

    public bool TrySetRuntimeNeuronState(
        uint targetNeuronId,
        bool setBuffer,
        float bufferValue,
        bool setAccumulator,
        float accumulatorValue)
    {
        if (!setBuffer && !setAccumulator)
        {
            return false;
        }

        if (setBuffer && !float.IsFinite(bufferValue))
        {
            return false;
        }

        if (setAccumulator && !float.IsFinite(accumulatorValue))
        {
            return false;
        }

        if (!TryGetLocalNeuronIndex(targetNeuronId, out var localIndex))
        {
            return false;
        }

        if (setBuffer)
        {
            Buffer[localIndex] = bufferValue;
        }

        if (setAccumulator)
        {
            _inbox[localIndex] = accumulatorValue;
            _inboxHasInput[localIndex] = true;
        }

        return true;
    }

    private bool TryGetLocalNeuronIndex(uint targetNeuronId, out int localIndex)
    {
        localIndex = (int)targetNeuronId - NeuronStart;
        return localIndex >= 0 && localIndex < NeuronCount;
    }

    public RegionShardDeterministicRngInput GetDeterministicRngInput(ulong tickId, int axonIndex)
    {
        if ((uint)axonIndex >= (uint)Axons.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(axonIndex), axonIndex, "Axon index is out of range.");
        }

        return new RegionShardDeterministicRngInput(
            BrainSeed,
            tickId,
            Axons.FromAddress32[axonIndex],
            Axons.ToAddress32[axonIndex]);
    }
}

public sealed class RegionShardAxons
{
    public RegionShardAxons(
        byte[] targetRegionIds,
        int[] targetNeuronIds,
        float[] strengths,
        byte[] baseStrengthCodes,
        byte[] runtimeStrengthCodes,
        bool[] hasRuntimeOverlay,
        uint[] fromAddress32,
        uint[] toAddress32)
    {
        TargetRegionIds = targetRegionIds ?? throw new ArgumentNullException(nameof(targetRegionIds));
        TargetNeuronIds = targetNeuronIds ?? throw new ArgumentNullException(nameof(targetNeuronIds));
        Strengths = strengths ?? throw new ArgumentNullException(nameof(strengths));
        BaseStrengthCodes = baseStrengthCodes ?? throw new ArgumentNullException(nameof(baseStrengthCodes));
        RuntimeStrengthCodes = runtimeStrengthCodes ?? throw new ArgumentNullException(nameof(runtimeStrengthCodes));
        HasRuntimeOverlay = hasRuntimeOverlay ?? throw new ArgumentNullException(nameof(hasRuntimeOverlay));
        FromAddress32 = fromAddress32 ?? throw new ArgumentNullException(nameof(fromAddress32));
        ToAddress32 = toAddress32 ?? throw new ArgumentNullException(nameof(toAddress32));

        var count = TargetRegionIds.Length;
        if (TargetNeuronIds.Length != count
            || Strengths.Length != count
            || BaseStrengthCodes.Length != count
            || RuntimeStrengthCodes.Length != count
            || HasRuntimeOverlay.Length != count
            || FromAddress32.Length != count
            || ToAddress32.Length != count)
        {
            throw new ArgumentException("Axon arrays must be the same length.");
        }
    }

    public byte[] TargetRegionIds { get; }
    public int[] TargetNeuronIds { get; }
    public float[] Strengths { get; }
    public byte[] BaseStrengthCodes { get; }
    public byte[] RuntimeStrengthCodes { get; }
    public bool[] HasRuntimeOverlay { get; }
    public uint[] FromAddress32 { get; }
    public uint[] ToAddress32 { get; }

    public int Count => Strengths.Length;
}

public readonly record struct RegionShardDeterministicRngInput(
    ulong BrainSeed,
    ulong TickId,
    uint FromAddress32,
    uint ToAddress32)
{
    public ulong ToSeed()
    {
        return MixToU64(BrainSeed, TickId, FromAddress32, ToAddress32);
    }

    public static ulong MixToU64(ulong brainSeed, ulong tickId, uint fromAddress32, uint toAddress32)
    {
        var mixed = brainSeed;
        mixed = SplitMixStep(mixed ^ tickId);
        mixed = SplitMixStep(mixed ^ fromAddress32);
        mixed = SplitMixStep(mixed ^ toAddress32);
        return SplitMixStep(mixed);
    }

    private static ulong SplitMixStep(ulong value)
    {
        value ^= value >> 30;
        value *= 0xbf58476d1ce4e5b9UL;
        value ^= value >> 27;
        value *= 0x94d049bb133111ebUL;
        value ^= value >> 31;
        return value;
    }
}
