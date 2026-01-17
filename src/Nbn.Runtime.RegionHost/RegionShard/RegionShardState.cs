using Nbn.Shared;
using Nbn.Proto;

namespace Nbn.Runtime.RegionHost;

public sealed class RegionShardState
{
    public RegionShardState(
        int regionId,
        int neuronStart,
        int neuronCount,
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

        _inbox = new float[neuronCount];
        _inboxHasInput = new bool[neuronCount];
    }

    public int RegionId { get; }
    public int NeuronStart { get; }
    public int NeuronCount { get; }
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
        var localIndex = (int)targetNeuronId - NeuronStart;
        if (localIndex < 0 || localIndex >= NeuronCount)
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
}

public sealed class RegionShardAxons
{
    public RegionShardAxons(byte[] targetRegionIds, int[] targetNeuronIds, float[] strengths)
    {
        TargetRegionIds = targetRegionIds ?? throw new ArgumentNullException(nameof(targetRegionIds));
        TargetNeuronIds = targetNeuronIds ?? throw new ArgumentNullException(nameof(targetNeuronIds));
        Strengths = strengths ?? throw new ArgumentNullException(nameof(strengths));

        if (TargetRegionIds.Length != TargetNeuronIds.Length || TargetRegionIds.Length != Strengths.Length)
        {
            throw new ArgumentException("Axon arrays must be the same length.");
        }
    }

    public byte[] TargetRegionIds { get; }
    public int[] TargetNeuronIds { get; }
    public float[] Strengths { get; }

    public int Count => Strengths.Length;
}
