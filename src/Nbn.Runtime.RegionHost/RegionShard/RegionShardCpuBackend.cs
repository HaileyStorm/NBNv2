using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Shared;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Runtime.RegionHost;

public sealed class RegionShardCpuBackend
{
    private readonly RegionShardState _state;
    private readonly RegionShardCostConfig _costConfig;

    public RegionShardCpuBackend(RegionShardState state, RegionShardCostConfig? costConfig = null)
    {
        _state = state ?? throw new ArgumentNullException(nameof(state));
        _costConfig = costConfig ?? RegionShardCostConfig.Default;
    }

    public RegionShardComputeResult Compute(
        ulong tickId,
        Guid brainId,
        ShardId32 shardId,
        RegionShardRoutingTable routing,
        RegionShardVisualizationComputeScope? visualization = null)
    {
        routing ??= RegionShardRoutingTable.CreateSingleShard(_state.RegionId, _state.NeuronCount);
        var vizScope = visualization ?? RegionShardVisualizationComputeScope.EnabledAll;
        var focusedRegionId = vizScope.FocusRegionId;
        var collectNeuronViz = vizScope.Enabled
                               && (!focusedRegionId.HasValue || focusedRegionId.Value == (uint)_state.RegionId);
        var collectAxonViz = vizScope.Enabled;

        var outbox = new Dictionary<ShardId32, List<Contribution>>();
        Dictionary<AxonVizRouteKey, AxonVizAccumulator>? axonVizRoutes = collectAxonViz
            ? new Dictionary<AxonVizRouteKey, AxonVizAccumulator>()
            : null;
        List<RegionShardNeuronVizEvent>? firedNeuronViz = collectNeuronViz
            ? new List<RegionShardNeuronVizEvent>()
            : null;
        List<OutputEvent>? outputs = _state.IsOutputRegion ? new List<OutputEvent>() : null;
        float[]? outputVector = _state.IsOutputRegion ? new float[_state.NeuronCount] : null;
        var brainProto = brainId.ToProtoUuid();

        var regionDistanceCache = new int?[NbnConstants.RegionCount];
        var sourceRegionZ = RegionZ(_state.RegionId);

        long costAccum = 0;
        long costActivation = 0;
        long costReset = 0;
        long costDistance = 0;
        const long costRemote = 0;

        uint firedCount = 0;
        uint outContribs = 0;

        for (var i = 0; i < _state.NeuronCount; i++)
        {
            MergeInbox(i);
            costAccum++;

            if (!_state.Exists[i])
            {
                continue;
            }

            if (!_state.Enabled[i])
            {
                continue;
            }

            var buffer = _state.Buffer[i];
            if (!float.IsFinite(buffer))
            {
                buffer = 0f;
                _state.Buffer[i] = 0f;
            }
            if (buffer <= _state.PreActivationThreshold[i])
            {
                continue;
            }

            costActivation++;
            var potential = Activate((ActivationFunction)_state.ActivationFunctions[i], buffer, _state.ParamA[i], _state.ParamB[i]);
            if (!float.IsFinite(potential))
            {
                potential = 0f;
            }

            var reset = Reset((ResetFunction)_state.ResetFunctions[i], buffer, potential, _state.ActivationThreshold[i], _state.AxonCounts[i]);
            if (!float.IsFinite(reset))
            {
                reset = 0f;
            }

            _state.Buffer[i] = reset;
            costReset++;

            if (outputVector is not null)
            {
                outputVector[i] = potential;
            }

            if (MathF.Abs(potential) <= _state.ActivationThreshold[i])
            {
                continue;
            }

            firedCount++;
            var sourceNeuronId = _state.NeuronStart + i;
            var sourceAddress = ComposeAddress(_state.RegionId, sourceNeuronId);
            firedNeuronViz?.Add(new RegionShardNeuronVizEvent(sourceAddress, tickId, potential));

            if (outputs is not null)
            {
                outputs.Add(new OutputEvent
                {
                    BrainId = brainProto,
                    OutputIndex = (uint)(_state.NeuronStart + i),
                    Value = potential,
                    TickId = tickId
                });
            }

            var axonCount = _state.AxonCounts[i];
            if (axonCount == 0)
            {
                continue;
            }

            var axonStart = _state.AxonStartOffsets[i];
            for (var a = 0; a < axonCount; a++)
            {
                var index = axonStart + a;
                var destRegion = _state.Axons.TargetRegionIds[index];
                var destNeuron = _state.Axons.TargetNeuronIds[index];
                var strength = _state.Axons.Strengths[index];
                var value = potential * strength;

                if (!routing.TryGetShard(destRegion, destNeuron, out var destShard))
                {
                    destShard = ShardId32.From(destRegion, 0);
                }

                if (!outbox.TryGetValue(destShard, out var list))
                {
                    list = new List<Contribution>();
                    outbox[destShard] = list;
                }

                list.Add(new Contribution
                {
                    TargetNeuronId = (uint)destNeuron,
                    Value = value
                });
                outContribs++;

                var shouldCollectRouteViz = axonVizRoutes is not null
                    && (!focusedRegionId.HasValue
                        || focusedRegionId.Value == (uint)_state.RegionId
                        || focusedRegionId.Value == destRegion);
                if (shouldCollectRouteViz)
                {
                    var targetAddress = ComposeAddress(destRegion, destNeuron);
                    var routeKey = new AxonVizRouteKey(sourceAddress, targetAddress);
                    if (!axonVizRoutes!.TryGetValue(routeKey, out var routeAggregate))
                    {
                        routeAggregate = AxonVizAccumulator.Empty;
                    }

                    axonVizRoutes[routeKey] = routeAggregate.Merge(tickId, value, strength);
                }

                var distanceUnits = ComputeDistanceUnits(sourceNeuronId, destRegion, destNeuron, sourceRegionZ, regionDistanceCache);
                costDistance += _costConfig.AxonBaseCost + (_costConfig.AxonUnitCost * distanceUnits);
            }
        }

        var tickCostTotal = costAccum + costActivation + costReset + costDistance + costRemote;
        var costSummary = new RegionShardCostSummary(tickCostTotal, costAccum, costActivation, costReset, costDistance, costRemote);

        IReadOnlyList<OutputEvent> outputList = outputs ?? (IReadOnlyList<OutputEvent>)Array.Empty<OutputEvent>();
        IReadOnlyList<float> outputVectorList = outputVector ?? Array.Empty<float>();
        IReadOnlyList<RegionShardAxonVizEvent> axonVizEvents;
        if (axonVizRoutes is null || axonVizRoutes.Count == 0)
        {
            axonVizEvents = Array.Empty<RegionShardAxonVizEvent>();
        }
        else
        {
            var events = new List<RegionShardAxonVizEvent>(axonVizRoutes.Count);
            foreach (var (routeKey, routeAggregate) in axonVizRoutes)
            {
                events.Add(new RegionShardAxonVizEvent(
                    routeKey.SourceAddress,
                    routeKey.TargetAddress,
                    routeAggregate.EventCount,
                    routeAggregate.LastTick,
                    routeAggregate.AverageSignedValue,
                    routeAggregate.AverageMagnitude,
                    routeAggregate.AverageSignedStrength,
                    routeAggregate.AverageStrength));
            }

            axonVizEvents = events;
        }

        IReadOnlyList<RegionShardNeuronVizEvent> neuronVizEvents = firedNeuronViz is null
            ? Array.Empty<RegionShardNeuronVizEvent>()
            : firedNeuronViz;

        return new RegionShardComputeResult(
            outbox,
            outputList,
            outputVectorList,
            firedCount,
            outContribs,
            costSummary,
            axonVizEvents,
            neuronVizEvents);
    }

    private void MergeInbox(int index)
    {
        var accum = (AccumulationFunction)_state.AccumulationFunctions[index];
        switch (accum)
        {
            case AccumulationFunction.AccumSum:
                _state.Buffer[index] += _state.Inbox[index];
                break;
            case AccumulationFunction.AccumProduct:
                if (_state.InboxHasInput[index])
                {
                    _state.Buffer[index] *= _state.Inbox[index];
                }
                break;
            case AccumulationFunction.AccumMax:
                if (_state.InboxHasInput[index])
                {
                    _state.Buffer[index] = MathF.Max(_state.Buffer[index], _state.Inbox[index]);
                }
                break;
            case AccumulationFunction.AccumNone:
                break;
            default:
                _state.Buffer[index] += _state.Inbox[index];
                break;
        }

        _state.Inbox[index] = 0f;
        _state.InboxHasInput[index] = false;
    }

    private static float Activate(ActivationFunction function, float buffer, float paramA, float paramB)
    {
        return function switch
        {
            ActivationFunction.ActNone => 0f,
            ActivationFunction.ActIdentity => buffer,
            ActivationFunction.ActStepUp => buffer <= 0f ? 0f : 1f,
            ActivationFunction.ActStepMid => buffer < 0f ? -1f : buffer == 0f ? 0f : 1f,
            ActivationFunction.ActStepDown => buffer < 0f ? -1f : 0f,
            ActivationFunction.ActAbs => MathF.Abs(buffer),
            ActivationFunction.ActClamp => Math.Clamp(buffer, -1f, 1f),
            ActivationFunction.ActRelu => MathF.Max(0f, buffer),
            ActivationFunction.ActNrelu => MathF.Min(buffer, 0f),
            ActivationFunction.ActSin => MathF.Sin(buffer),
            ActivationFunction.ActTan => Math.Clamp(MathF.Tan(buffer), -1f, 1f),
            ActivationFunction.ActTanh => MathF.Tanh(buffer),
            ActivationFunction.ActElu => buffer > 0f ? buffer : paramA * (MathF.Exp(buffer) - 1f),
            ActivationFunction.ActExp => MathF.Exp(buffer),
            ActivationFunction.ActPrelu => buffer >= 0f ? buffer : paramA * buffer,
            ActivationFunction.ActLog => buffer == 0f ? 0f : MathF.Log(buffer),
            ActivationFunction.ActMult => buffer * paramA,
            ActivationFunction.ActAdd => buffer + paramA,
            ActivationFunction.ActSig => 1f / (1f + MathF.Exp(-buffer)),
            ActivationFunction.ActSilu => buffer / (1f + MathF.Exp(-buffer)),
            ActivationFunction.ActPclamp => paramB <= paramA ? 0f : Math.Clamp(buffer, paramA, paramB),
            ActivationFunction.ActModl => paramA == 0f ? 0f : buffer % paramA,
            ActivationFunction.ActModr => buffer == 0f ? 0f : paramA % buffer,
            ActivationFunction.ActSoftp => MathF.Log(1f + MathF.Exp(buffer)),
            ActivationFunction.ActSelu => paramB * (buffer >= 0f ? buffer : paramA * (MathF.Exp(buffer) - 1f)),
            ActivationFunction.ActLin => paramA * buffer + paramB,
            ActivationFunction.ActLogb => paramA == 0f ? 0f : MathF.Log(buffer, paramA),
            ActivationFunction.ActPow => MathF.Pow(buffer, paramA),
            ActivationFunction.ActGauss => MathF.Exp(buffer * buffer),
            ActivationFunction.ActQuad => paramA * (buffer * buffer) + paramB * buffer,
            _ => 0f
        };
    }

    private static float Reset(ResetFunction function, float buffer, float potential, float threshold, int outDegree)
    {
        return function switch
        {
            ResetFunction.ResetZero => 0f,
            ResetFunction.ResetHold => Clamp(buffer, threshold),
            ResetFunction.ResetClampPotential => Clamp(buffer, MathF.Abs(potential)),
            ResetFunction.ResetClamp1 => Math.Clamp(buffer, -1f, 1f),
            ResetFunction.ResetPotentialClampBuffer => Clamp(potential, MathF.Abs(buffer)),
            ResetFunction.ResetNegPotentialClampBuffer => Clamp(-potential, MathF.Abs(buffer)),
            ResetFunction.ResetHundredthsPotentialClampBuffer => Clamp(0.01f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetTenthPotentialClampBuffer => Clamp(0.1f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetHalfPotentialClampBuffer => Clamp(0.5f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetDoublePotentialClampBuffer => Clamp(2f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetFivexPotentialClampBuffer => Clamp(5f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetNegHundredthsPotentialClampBuffer => Clamp(-0.01f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetNegTenthPotentialClampBuffer => Clamp(-0.1f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetNegHalfPotentialClampBuffer => Clamp(-0.5f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetNegDoublePotentialClampBuffer => Clamp(-2f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetNegFivexPotentialClampBuffer => Clamp(-5f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetInversePotentialClampBuffer => Clamp(SafeInverse(potential), MathF.Abs(buffer)),

            ResetFunction.ResetPotentialClamp1 => Math.Clamp(potential, -1f, 1f),
            ResetFunction.ResetNegPotentialClamp1 => Math.Clamp(-potential, -1f, 1f),
            ResetFunction.ResetHundredthsPotentialClamp1 => Math.Clamp(0.01f * potential, -1f, 1f),
            ResetFunction.ResetTenthPotentialClamp1 => Math.Clamp(0.1f * potential, -1f, 1f),
            ResetFunction.ResetHalfPotentialClamp1 => Math.Clamp(0.5f * potential, -1f, 1f),
            ResetFunction.ResetDoublePotentialClamp1 => Math.Clamp(2f * potential, -1f, 1f),
            ResetFunction.ResetFivexPotentialClamp1 => Math.Clamp(5f * potential, -1f, 1f),
            ResetFunction.ResetNegHundredthsPotentialClamp1 => Math.Clamp(-0.01f * potential, -1f, 1f),
            ResetFunction.ResetNegTenthPotentialClamp1 => Math.Clamp(-0.1f * potential, -1f, 1f),
            ResetFunction.ResetNegHalfPotentialClamp1 => Math.Clamp(-0.5f * potential, -1f, 1f),
            ResetFunction.ResetNegDoublePotentialClamp1 => Math.Clamp(-2f * potential, -1f, 1f),
            ResetFunction.ResetNegFivexPotentialClamp1 => Math.Clamp(-5f * potential, -1f, 1f),
            ResetFunction.ResetInversePotentialClamp1 => Math.Clamp(SafeInverse(potential), -1f, 1f),

            ResetFunction.ResetPotential => Clamp(potential, threshold),
            ResetFunction.ResetNegPotential => Clamp(-potential, threshold),
            ResetFunction.ResetHundredthsPotential => Clamp(0.01f * potential, threshold),
            ResetFunction.ResetTenthPotential => Clamp(0.1f * potential, threshold),
            ResetFunction.ResetHalfPotential => Clamp(0.5f * potential, threshold),
            ResetFunction.ResetDoublePotential => Clamp(2f * potential, threshold),
            ResetFunction.ResetFivexPotential => Clamp(5f * potential, threshold),
            ResetFunction.ResetNegHundredthsPotential => Clamp(-0.01f * potential, threshold),
            ResetFunction.ResetNegTenthPotential => Clamp(-0.1f * potential, threshold),
            ResetFunction.ResetNegHalfPotential => Clamp(-0.5f * potential, threshold),
            ResetFunction.ResetNegDoublePotential => Clamp(-2f * potential, threshold),
            ResetFunction.ResetNegFivexPotential => Clamp(-5f * potential, threshold),
            ResetFunction.ResetInversePotential => Clamp(SafeInverse(potential), threshold),

            ResetFunction.ResetHalf => Clamp(0.5f * buffer, threshold),
            ResetFunction.ResetTenth => Clamp(0.1f * buffer, threshold),
            ResetFunction.ResetHundredth => Clamp(0.01f * buffer, threshold),
            ResetFunction.ResetNegative => Clamp(-buffer, threshold),
            ResetFunction.ResetNegHalf => Clamp(-0.5f * buffer, threshold),
            ResetFunction.ResetNegTenth => Clamp(-0.1f * buffer, threshold),
            ResetFunction.ResetNegHundredth => Clamp(-0.01f * buffer, threshold),

            ResetFunction.ResetDoubleClamp1 => Math.Clamp(2f * buffer, -1f, 1f),
            ResetFunction.ResetFivexClamp1 => Math.Clamp(5f * buffer, -1f, 1f),
            ResetFunction.ResetNegDoubleClamp1 => Math.Clamp(-2f * buffer, -1f, 1f),
            ResetFunction.ResetNegFivexClamp1 => Math.Clamp(-5f * buffer, -1f, 1f),

            ResetFunction.ResetDouble => Clamp(2f * buffer, threshold),
            ResetFunction.ResetFivex => Clamp(5f * buffer, threshold),
            ResetFunction.ResetNegDouble => Clamp(-2f * buffer, threshold),
            ResetFunction.ResetNegFivex => Clamp(-5f * buffer, threshold),

            ResetFunction.ResetDivideAxonCt => Clamp(buffer / Math.Max(1, outDegree), threshold),
            ResetFunction.ResetInverseClamp1 => Math.Clamp(-SafeInverse(buffer), -1f, 1f),
            ResetFunction.ResetInverse => Clamp(-SafeInverse(buffer), threshold),
            _ => buffer
        };
    }

    private static float Clamp(float value, float limit)
    {
        return Math.Clamp(value, -limit, limit);
    }

    private static float SafeInverse(float value)
    {
        return value == 0f ? 0f : 1f / value;
    }

    private long ComputeDistanceUnits(int sourceNeuronId, byte destRegionId, int destNeuronId, int sourceRegionZ, int?[] regionDistanceCache)
    {
        var destRegion = (int)destRegionId;
        var regionDist = regionDistanceCache[destRegion] ??= ComputeRegionDistance(sourceRegionZ, destRegion);
        var span = destRegion >= 0 && destRegion < _state.RegionSpans.Length ? _state.RegionSpans[destRegion] : 0;

        var d = Math.Abs(sourceNeuronId - destNeuronId);
        var wrap = span > 0 && d < span ? Math.Min(d, span - d) : d;
        var neuronUnits = _costConfig.NeuronDistShift > 0 ? wrap >> _costConfig.NeuronDistShift : wrap;

        return (_costConfig.RegionWeight * regionDist) + neuronUnits;
    }

    private int ComputeRegionDistance(int sourceRegionZ, int destRegionId)
    {
        if (destRegionId == _state.RegionId)
        {
            return 0;
        }

        var destZ = RegionZ(destRegionId);
        if (destZ == sourceRegionZ)
        {
            return _costConfig.RegionIntrasliceUnit;
        }

        return _costConfig.RegionAxialUnit * Math.Abs(destZ - sourceRegionZ);
    }

    private static int RegionZ(int regionId)
    {
        if (regionId == 0)
        {
            return -3;
        }

        if (regionId <= 3)
        {
            return -2;
        }

        if (regionId <= 8)
        {
            return -1;
        }

        if (regionId <= 22)
        {
            return 0;
        }

        if (regionId <= 27)
        {
            return 1;
        }

        if (regionId <= 30)
        {
            return 2;
        }

        return 3;
    }

    private static uint ComposeAddress(int regionId, int neuronId)
        => ((uint)regionId << NbnConstants.AddressNeuronBits) | ((uint)neuronId & NbnConstants.AddressNeuronMask);

    private readonly record struct AxonVizRouteKey(uint SourceAddress, uint TargetAddress);

    private readonly record struct AxonVizAccumulator(
        int EventCount,
        ulong LastTick,
        float AverageSignedValue,
        float AverageMagnitude,
        float AverageSignedStrength,
        float AverageStrength)
    {
        public static AxonVizAccumulator Empty { get; } = new(0, 0, 0f, 0f, 0f, 0f);

        public AxonVizAccumulator Merge(ulong tickId, float signedValue, float signedStrength)
        {
            var nextCount = EventCount + 1;
            var weight = 1f / nextCount;
            var inverseWeight = 1f - weight;
            return new AxonVizAccumulator(
                nextCount,
                Math.Max(LastTick, tickId),
                (AverageSignedValue * inverseWeight) + (signedValue * weight),
                (AverageMagnitude * inverseWeight) + (Math.Abs(signedValue) * weight),
                (AverageSignedStrength * inverseWeight) + (signedStrength * weight),
                (AverageStrength * inverseWeight) + (Math.Abs(signedStrength) * weight));
        }
    }
}

public sealed class RegionShardCostConfig
{
    public static RegionShardCostConfig Default { get; } = new();

    public long AxonBaseCost { get; init; } = 1;
    public long AxonUnitCost { get; init; } = 1;
    public int RegionWeight { get; init; } = 1;
    public int RegionIntrasliceUnit { get; init; } = 3;
    public int RegionAxialUnit { get; init; } = 5;
    public int NeuronDistShift { get; init; } = 10;
}

public readonly record struct RegionShardCostSummary(
    long Total,
    long Accum,
    long Activation,
    long Reset,
    long Distance,
    long Remote);

public readonly record struct RegionShardAxonVizEvent(
    uint SourceAddress,
    uint TargetAddress,
    int EventCount,
    ulong LastTick,
    float AverageSignedValue,
    float AverageMagnitude,
    float AverageSignedStrength,
    float AverageStrength);

public readonly record struct RegionShardNeuronVizEvent(
    uint SourceAddress,
    ulong TickId,
    float Potential);

public readonly record struct RegionShardVisualizationComputeScope(
    bool Enabled,
    uint? FocusRegionId)
{
    public static RegionShardVisualizationComputeScope EnabledAll { get; } = new(true, null);
    public static RegionShardVisualizationComputeScope Disabled { get; } = new(false, null);
}

public sealed record RegionShardComputeResult(
    Dictionary<ShardId32, List<Contribution>> Outbox,
    IReadOnlyList<OutputEvent> OutputEvents,
    IReadOnlyList<float> OutputVector,
    uint FiredCount,
    uint OutContribs,
    RegionShardCostSummary Cost,
    IReadOnlyList<RegionShardAxonVizEvent> AxonVizEvents,
    IReadOnlyList<RegionShardNeuronVizEvent> FiredNeuronEvents);
