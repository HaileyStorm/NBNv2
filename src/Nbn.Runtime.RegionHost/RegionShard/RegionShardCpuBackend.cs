using System.Globalization;
using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Shared;
using Nbn.Shared.Quantization;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.RegionHost;

public sealed class RegionShardCpuBackend
{
    private const float BufferVizEpsilon = 1e-6f;
    private const float PlasticityPredictiveNudgeGain = 0.35f;
    private const float PlasticitySourceMemoryGain = 0.15f;
    private const float PlasticitySaturationGain = 0.25f;
    private const float PlasticityMinStabilizationScale = 0.35f;
    private const float PlasticityMinNudgeScale = 0.4f;
    private const float PlasticityMaxNudgeScale = 1.6f;
    private static readonly bool LogActivityDiagnostics = IsEnvTrue("NBN_REGIONSHARD_ACTIVITY_DIAGNOSTICS_ENABLED");
    private static readonly ulong ActivityDiagnosticsPeriod = ResolveUnsignedEnv("NBN_REGIONSHARD_ACTIVITY_DIAGNOSTICS_PERIOD", 32UL);
    private static readonly int ActivityDiagnosticsSampleCount = (int)Math.Clamp(ResolveUnsignedEnv("NBN_REGIONSHARD_ACTIVITY_DIAGNOSTICS_SAMPLES", 3UL), 1UL, 16UL);
    private static readonly double[] AccumulationBaseCosts = { 1.0, 1.2, 1.0, 0.1 };
    private static readonly CostTier[] AccumulationTiers =
    {
        CostTier.A,
        CostTier.A,
        CostTier.A,
        CostTier.A
    };
    private static readonly double[] ActivationBaseCosts = BuildActivationBaseCosts();
    private static readonly CostTier[] ActivationTiers = BuildActivationTiers();
    private static readonly double[] ResetBaseCosts = BuildResetBaseCosts();
    private static readonly CostTier[] ResetTiers = BuildResetTiers();

    private readonly RegionShardState _state;
    private readonly RegionShardCostConfig _costConfig;
    private readonly float[] _lastEmittedBufferSamples;
    private readonly bool[] _hasLastEmittedBufferSamples;
    private bool _bufferVizTrackingArmed;

    private enum CostTier
    {
        A = 0,
        B = 1,
        C = 2
    }

    public RegionShardCpuBackend(RegionShardState state, RegionShardCostConfig? costConfig = null)
    {
        _state = state ?? throw new ArgumentNullException(nameof(state));
        _costConfig = costConfig ?? RegionShardCostConfig.Default;
        _lastEmittedBufferSamples = new float[_state.NeuronCount];
        _hasLastEmittedBufferSamples = new bool[_state.NeuronCount];
        _bufferVizTrackingArmed = false;
    }

    public RegionShardComputeResult Compute(
        ulong tickId,
        Guid brainId,
        ShardId32 shardId,
        RegionShardRoutingTable routing,
        RegionShardVisualizationComputeScope? visualization = null,
        bool plasticityEnabled = false,
        float plasticityRate = 0f,
        bool probabilisticPlasticityUpdates = false,
        float plasticityDelta = 0f,
        uint plasticityRebaseThreshold = 0,
        float plasticityRebaseThresholdPct = 0f,
        RegionShardHomeostasisConfig? homeostasisConfig = null,
        bool costEnergyEnabled = false,
        bool? remoteCostEnabled = null,
        long? remoteCostPerBatch = null,
        long? remoteCostPerContribution = null,
        float? costTierAMultiplier = null,
        float? costTierBMultiplier = null,
        float? costTierCMultiplier = null)
    {
        routing ??= RegionShardRoutingTable.CreateSingleShard(_state.RegionId, _state.NeuronCount);
        var homeostasis = homeostasisConfig ?? RegionShardHomeostasisConfig.Default;
        var effectiveRemoteCostEnabled = remoteCostEnabled ?? _costConfig.RemoteCostEnabled;
        var effectiveRemoteCostPerBatch = Math.Max(0L, remoteCostPerBatch ?? _costConfig.RemoteCostPerBatch);
        var effectiveRemoteCostPerContribution = Math.Max(0L, remoteCostPerContribution ?? _costConfig.RemoteCostPerContribution);
        var effectiveTierAMultiplier = NormalizeTierMultiplier(costTierAMultiplier ?? _costConfig.TierAMultiplier);
        var effectiveTierBMultiplier = NormalizeTierMultiplier(costTierBMultiplier ?? _costConfig.TierBMultiplier);
        var effectiveTierCMultiplier = NormalizeTierMultiplier(costTierCMultiplier ?? _costConfig.TierCMultiplier);
        double[]? accumCostLookup = null;
        double[]? activationCostLookup = null;
        double[]? resetCostLookup = null;
        if (costEnergyEnabled)
        {
            accumCostLookup = BuildWeightedLookup(AccumulationBaseCosts, AccumulationTiers, effectiveTierAMultiplier, effectiveTierBMultiplier, effectiveTierCMultiplier);
            activationCostLookup = BuildWeightedLookup(ActivationBaseCosts, ActivationTiers, effectiveTierAMultiplier, effectiveTierBMultiplier, effectiveTierCMultiplier);
            resetCostLookup = BuildWeightedLookup(ResetBaseCosts, ResetTiers, effectiveTierAMultiplier, effectiveTierBMultiplier, effectiveTierCMultiplier);
        }

        var vizScope = visualization ?? RegionShardVisualizationComputeScope.EnabledAll;
        var focusedRegionId = vizScope.FocusRegionId;
        var collectNeuronViz = vizScope.Enabled
                               && (!focusedRegionId.HasValue || focusedRegionId.Value == (uint)_state.RegionId);
        var collectAxonViz = vizScope.Enabled;
        if (!collectNeuronViz)
        {
            _bufferVizTrackingArmed = false;
        }
        else if (!_bufferVizTrackingArmed)
        {
            Array.Clear(_hasLastEmittedBufferSamples, 0, _hasLastEmittedBufferSamples.Length);
            _bufferVizTrackingArmed = true;
        }

        var outbox = new Dictionary<ShardId32, List<Contribution>>();
        Dictionary<AxonVizRouteKey, AxonVizAccumulator>? axonVizRoutes = collectAxonViz
            ? new Dictionary<AxonVizRouteKey, AxonVizAccumulator>()
            : null;
        List<RegionShardNeuronVizEvent>? firedNeuronViz = collectNeuronViz
            ? new List<RegionShardNeuronVizEvent>()
            : null;
        List<RegionShardNeuronBufferVizEvent>? bufferNeuronViz = collectNeuronViz
            ? new List<RegionShardNeuronBufferVizEvent>()
            : null;
        List<OutputEvent>? outputs = _state.IsOutputRegion ? new List<OutputEvent>() : null;
        float[]? outputVector = _state.IsOutputRegion ? new float[_state.NeuronCount] : null;
        var brainProto = brainId.ToProtoUuid();

        var regionDistanceCache = new int?[NbnConstants.RegionCount];
        var sourceRegionZ = RegionZ(_state.RegionId);

        double costAccum = 0d;
        double costActivation = 0d;
        double costReset = 0d;
        long costDistance = 0;
        long costRemote = 0;
        uint plasticityStrengthCodeChanges = 0;

        uint firedCount = 0;
        uint outContribs = 0;

        for (var i = 0; i < _state.NeuronCount; i++)
        {
            MergeInbox(i);
            if (costEnergyEnabled)
            {
                costAccum += ResolveFunctionCost(accumCostLookup!, _state.AccumulationFunctions[i]);
            }

            if (!_state.Exists[i])
            {
                continue;
            }

            if (!_state.Enabled[i])
            {
                continue;
            }

            var buffer = NormalizeBuffer(i);
            ApplyHomeostasis(tickId, i, homeostasis, costEnergyEnabled);
            buffer = NormalizeBuffer(i);

            var sourceNeuronId = _state.NeuronStart + i;
            var sourceAddress = ComposeAddress(_state.RegionId, sourceNeuronId);
            if (bufferNeuronViz is not null)
            {
                var hasSample = _hasLastEmittedBufferSamples[i];
                var previous = _lastEmittedBufferSamples[i];
                var changed = !hasSample || MathF.Abs(buffer - previous) > BufferVizEpsilon;
                if (changed)
                {
                    bufferNeuronViz.Add(new RegionShardNeuronBufferVizEvent(sourceAddress, tickId, buffer));
                    _lastEmittedBufferSamples[i] = buffer;
                    _hasLastEmittedBufferSamples[i] = true;
                }
            }

            if (buffer <= _state.PreActivationThreshold[i])
            {
                continue;
            }

            if (costEnergyEnabled)
            {
                costActivation += ResolveFunctionCost(activationCostLookup!, _state.ActivationFunctions[i]);
            }
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
            if (costEnergyEnabled)
            {
                costReset += ResolveFunctionCost(resetCostLookup!, _state.ResetFunctions[i]);
            }

            if (outputVector is not null)
            {
                outputVector[i] = potential;
            }

            if (MathF.Abs(potential) <= _state.ActivationThreshold[i])
            {
                continue;
            }

            firedCount++;
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
                var strength = NormalizeAxonStrength(index);
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
                if (costEnergyEnabled)
                {
                    costDistance += _costConfig.AxonBaseCost + (_costConfig.AxonUnitCost * distanceUnits);
                }
                if (ApplyPlasticity(
                        tickId,
                        potential,
                        buffer,
                        index,
                        plasticityEnabled,
                        plasticityRate,
                        probabilisticPlasticityUpdates,
                        plasticityDelta))
                {
                    plasticityStrengthCodeChanges++;
                }
            }
        }

        var changedCodeCount = CountChangedStrengthCodes();
        if (ShouldAutoRebasePlasticity(
                plasticityEnabled,
                _state.Axons.Count,
                changedCodeCount,
                plasticityRebaseThreshold,
                plasticityRebaseThresholdPct))
        {
            ApplyPlasticityAutoRebase();
        }

        var costAccumUnits = costEnergyEnabled ? RoundToCostUnits(costAccum) : 0L;
        var costActivationUnits = costEnergyEnabled ? RoundToCostUnits(costActivation) : 0L;
        var costResetUnits = costEnergyEnabled ? RoundToCostUnits(costReset) : 0L;
        if (costEnergyEnabled
            && effectiveRemoteCostEnabled
            && (effectiveRemoteCostPerBatch != 0 || effectiveRemoteCostPerContribution != 0))
        {
            long remoteBatchCount = 0;
            long remoteContributionCount = 0;
            foreach (var (destinationShard, contribs) in outbox)
            {
                if (destinationShard.Equals(shardId))
                {
                    continue;
                }

                remoteBatchCount++;
                remoteContributionCount += contribs.Count;
            }

            costRemote = checked((remoteBatchCount * effectiveRemoteCostPerBatch) + (remoteContributionCount * effectiveRemoteCostPerContribution));
        }

        var tickCostTotal = checked(costAccumUnits + costActivationUnits + costResetUnits + costDistance + costRemote);
        var costSummary = new RegionShardCostSummary(tickCostTotal, costAccumUnits, costActivationUnits, costResetUnits, costDistance, costRemote);

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
        IReadOnlyList<RegionShardNeuronBufferVizEvent> bufferVizEvents = bufferNeuronViz is null
            ? Array.Empty<RegionShardNeuronBufferVizEvent>()
            : bufferNeuronViz;

        if (LogActivityDiagnostics
            && firedCount == 0
            && (ActivityDiagnosticsPeriod == 0 || tickId % ActivityDiagnosticsPeriod == 0))
        {
            EmitActivityDiagnostics(tickId, brainId, shardId);
        }

        return new RegionShardComputeResult(
            outbox,
            outputList,
            outputVectorList,
            firedCount,
            outContribs,
            plasticityStrengthCodeChanges,
            costSummary,
            axonVizEvents,
            bufferVizEvents,
            neuronVizEvents);
    }

    private static double[] BuildWeightedLookup(
        double[] baseCosts,
        CostTier[] tiers,
        float tierAMultiplier,
        float tierBMultiplier,
        float tierCMultiplier)
    {
        var weighted = new double[baseCosts.Length];
        for (var i = 0; i < baseCosts.Length; i++)
        {
            weighted[i] = baseCosts[i] * ResolveTierMultiplier(tiers[i], tierAMultiplier, tierBMultiplier, tierCMultiplier);
        }

        return weighted;
    }

    private static double ResolveFunctionCost(double[] weightedLookup, byte functionId)
    {
        var index = (int)functionId;
        if ((uint)index >= (uint)weightedLookup.Length)
        {
            return weightedLookup[0];
        }

        return weightedLookup[index];
    }

    private static long RoundToCostUnits(double value)
    {
        return (long)Math.Round(value, MidpointRounding.AwayFromZero);
    }

    private static float NormalizeTierMultiplier(float value)
    {
        return float.IsFinite(value) && value > 0f
            ? value
            : 1f;
    }

    private static float ResolveTierMultiplier(CostTier tier, float tierAMultiplier, float tierBMultiplier, float tierCMultiplier)
    {
        return tier switch
        {
            CostTier.B => tierBMultiplier,
            CostTier.C => tierCMultiplier,
            _ => tierAMultiplier
        };
    }

    private static double[] BuildActivationBaseCosts()
    {
        var costs = new double[64];
        for (var i = 0; i < costs.Length; i++)
        {
            costs[i] = 1.0;
        }

        costs[(int)ActivationFunction.ActNone] = 0.0;
        costs[(int)ActivationFunction.ActIdentity] = 1.0;
        costs[(int)ActivationFunction.ActStepUp] = 1.0;
        costs[(int)ActivationFunction.ActStepMid] = 1.0;
        costs[(int)ActivationFunction.ActStepDown] = 1.0;
        costs[(int)ActivationFunction.ActAbs] = 1.1;
        costs[(int)ActivationFunction.ActClamp] = 1.1;
        costs[(int)ActivationFunction.ActRelu] = 1.1;
        costs[(int)ActivationFunction.ActNrelu] = 1.1;
        costs[(int)ActivationFunction.ActSin] = 1.4;
        costs[(int)ActivationFunction.ActTan] = 1.6;
        costs[(int)ActivationFunction.ActTanh] = 1.6;
        costs[(int)ActivationFunction.ActElu] = 1.8;
        costs[(int)ActivationFunction.ActExp] = 1.8;
        costs[(int)ActivationFunction.ActPrelu] = 1.4;
        costs[(int)ActivationFunction.ActLog] = 1.9;
        costs[(int)ActivationFunction.ActMult] = 1.2;
        costs[(int)ActivationFunction.ActAdd] = 1.2;
        costs[(int)ActivationFunction.ActSig] = 2.0;
        costs[(int)ActivationFunction.ActSilu] = 2.0;
        costs[(int)ActivationFunction.ActPclamp] = 1.3;
        costs[(int)ActivationFunction.ActModl] = 2.6;
        costs[(int)ActivationFunction.ActModr] = 2.6;
        costs[(int)ActivationFunction.ActSoftp] = 2.8;
        costs[(int)ActivationFunction.ActSelu] = 2.8;
        costs[(int)ActivationFunction.ActLin] = 1.4;
        costs[(int)ActivationFunction.ActLogb] = 3.0;
        costs[(int)ActivationFunction.ActPow] = 3.5;
        costs[(int)ActivationFunction.ActGauss] = 5.0;
        costs[(int)ActivationFunction.ActQuad] = 6.0;
        return costs;
    }

    private static CostTier[] BuildActivationTiers()
    {
        var tiers = new CostTier[64];
        for (var i = 0; i < tiers.Length; i++)
        {
            tiers[i] = CostTier.A;
        }

        tiers[(int)ActivationFunction.ActSin] = CostTier.B;
        tiers[(int)ActivationFunction.ActTan] = CostTier.B;
        tiers[(int)ActivationFunction.ActTanh] = CostTier.B;
        tiers[(int)ActivationFunction.ActElu] = CostTier.B;
        tiers[(int)ActivationFunction.ActExp] = CostTier.B;
        tiers[(int)ActivationFunction.ActPrelu] = CostTier.B;
        tiers[(int)ActivationFunction.ActLog] = CostTier.B;
        tiers[(int)ActivationFunction.ActSig] = CostTier.B;
        tiers[(int)ActivationFunction.ActSilu] = CostTier.B;
        tiers[(int)ActivationFunction.ActModl] = CostTier.C;
        tiers[(int)ActivationFunction.ActModr] = CostTier.C;
        tiers[(int)ActivationFunction.ActSoftp] = CostTier.C;
        tiers[(int)ActivationFunction.ActSelu] = CostTier.C;
        tiers[(int)ActivationFunction.ActLogb] = CostTier.C;
        tiers[(int)ActivationFunction.ActPow] = CostTier.C;
        tiers[(int)ActivationFunction.ActGauss] = CostTier.C;
        tiers[(int)ActivationFunction.ActQuad] = CostTier.C;
        return tiers;
    }

    private static double[] BuildResetBaseCosts()
    {
        var costs = new double[64];
        for (var i = 0; i < costs.Length; i++)
        {
            costs[i] = 1.0;
        }

        costs[(int)ResetFunction.ResetZero] = 0.2;
        costs[(int)ResetFunction.ResetDoublePotentialClampBuffer] = 1.2;
        costs[(int)ResetFunction.ResetFivexPotentialClampBuffer] = 1.3;
        costs[(int)ResetFunction.ResetNegDoublePotentialClampBuffer] = 1.2;
        costs[(int)ResetFunction.ResetNegFivexPotentialClampBuffer] = 1.3;
        costs[(int)ResetFunction.ResetInversePotentialClampBuffer] = 1.8;
        costs[(int)ResetFunction.ResetDoublePotentialClamp1] = 1.2;
        costs[(int)ResetFunction.ResetFivexPotentialClamp1] = 1.3;
        costs[(int)ResetFunction.ResetNegDoublePotentialClamp1] = 1.2;
        costs[(int)ResetFunction.ResetNegFivexPotentialClamp1] = 1.3;
        costs[(int)ResetFunction.ResetInversePotentialClamp1] = 1.8;
        costs[(int)ResetFunction.ResetDoublePotential] = 1.2;
        costs[(int)ResetFunction.ResetFivexPotential] = 1.3;
        costs[(int)ResetFunction.ResetNegDoublePotential] = 1.2;
        costs[(int)ResetFunction.ResetNegFivexPotential] = 1.3;
        costs[(int)ResetFunction.ResetInversePotential] = 1.8;
        costs[(int)ResetFunction.ResetDoubleClamp1] = 1.2;
        costs[(int)ResetFunction.ResetFivexClamp1] = 1.3;
        costs[(int)ResetFunction.ResetNegDoubleClamp1] = 1.2;
        costs[(int)ResetFunction.ResetNegFivexClamp1] = 1.3;
        costs[(int)ResetFunction.ResetDouble] = 1.2;
        costs[(int)ResetFunction.ResetFivex] = 1.3;
        costs[(int)ResetFunction.ResetNegDouble] = 1.2;
        costs[(int)ResetFunction.ResetNegFivex] = 1.3;
        costs[(int)ResetFunction.ResetDivideAxonCt] = 1.1;
        costs[(int)ResetFunction.ResetInverseClamp1] = 1.8;
        costs[(int)ResetFunction.ResetInverse] = 1.8;
        return costs;
    }

    private static CostTier[] BuildResetTiers()
    {
        var tiers = new CostTier[64];
        for (var i = 0; i < tiers.Length; i++)
        {
            tiers[i] = CostTier.A;
        }

        SetResetTier(tiers, CostTier.B,
            ResetFunction.ResetDoublePotentialClampBuffer,
            ResetFunction.ResetFivexPotentialClampBuffer,
            ResetFunction.ResetNegDoublePotentialClampBuffer,
            ResetFunction.ResetNegFivexPotentialClampBuffer,
            ResetFunction.ResetDoublePotentialClamp1,
            ResetFunction.ResetFivexPotentialClamp1,
            ResetFunction.ResetNegDoublePotentialClamp1,
            ResetFunction.ResetNegFivexPotentialClamp1,
            ResetFunction.ResetDoublePotential,
            ResetFunction.ResetFivexPotential,
            ResetFunction.ResetNegDoublePotential,
            ResetFunction.ResetNegFivexPotential,
            ResetFunction.ResetDoubleClamp1,
            ResetFunction.ResetFivexClamp1,
            ResetFunction.ResetNegDoubleClamp1,
            ResetFunction.ResetNegFivexClamp1,
            ResetFunction.ResetDouble,
            ResetFunction.ResetFivex,
            ResetFunction.ResetNegDouble,
            ResetFunction.ResetNegFivex);
        SetResetTier(tiers, CostTier.C,
            ResetFunction.ResetInversePotentialClampBuffer,
            ResetFunction.ResetInversePotentialClamp1,
            ResetFunction.ResetInversePotential,
            ResetFunction.ResetInverseClamp1,
            ResetFunction.ResetInverse);

        return tiers;
    }

    private static void SetResetTier(CostTier[] tiers, CostTier tier, params ResetFunction[] functions)
    {
        foreach (var function in functions)
        {
            tiers[(int)function] = tier;
        }
    }

    private bool ApplyHomeostasis(
        ulong tickId,
        int neuronIndex,
        RegionShardHomeostasisConfig config,
        bool costEnergyEnabled)
    {
        if (!config.Enabled)
        {
            return false;
        }

        if (config.UpdateMode != ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep)
        {
            return false;
        }

        var probability = ClampFinite(config.BaseProbability, 0f, 1f, fallback: 0f);
        if (probability <= 0f)
        {
            return false;
        }

        if (config.EnergyCouplingEnabled && costEnergyEnabled)
        {
            var probabilityScale = ClampFinite(config.EnergyProbabilityScale, 0f, 4f, fallback: 1f);
            probability = Math.Clamp(probability * probabilityScale, 0f, 1f);
        }

        if (probability <= 0f)
        {
            return false;
        }

        var neuronId = _state.NeuronStart + neuronIndex;
        var address = ComposeAddress(_state.RegionId, neuronId);
        var seed = RegionShardDeterministicRngInput.MixToU64(_state.BrainSeed, tickId, address, address);
        if (UnitIntervalFromSeed(seed) >= probability)
        {
            return false;
        }

        var target = ResolveHomeostasisTarget(config.TargetMode);
        if (config.EnergyCouplingEnabled && costEnergyEnabled)
        {
            var targetScale = ClampFinite(config.EnergyTargetScale, 0f, 4f, fallback: 1f);
            target *= targetScale;
        }

        var current = NormalizeBuffer(neuronIndex);
        var quantization = QuantizationSchemas.DefaultBuffer;
        var currentCode = quantization.Encode(current, bits: 16);
        var targetCode = quantization.Encode(target, bits: 16);
        if (currentCode == targetCode)
        {
            return false;
        }

        var maxStep = QuantizationMap.MaxCode(bits: 16);
        var requestedStep = config.MinStepCodes == 0 ? 1 : (int)Math.Min(config.MinStepCodes, (uint)maxStep);
        var stepCodes = Math.Clamp(requestedStep, 1, maxStep);
        var nextCode = currentCode;
        if (targetCode > currentCode)
        {
            nextCode = Math.Min(currentCode + stepCodes, targetCode);
        }
        else
        {
            nextCode = Math.Max(currentCode - stepCodes, targetCode);
        }

        if (nextCode == currentCode)
        {
            return false;
        }

        _state.Buffer[neuronIndex] = quantization.Decode(nextCode, bits: 16);
        return true;
    }

    private static float ResolveHomeostasisTarget(ProtoControl.HomeostasisTargetMode mode)
    {
        return mode switch
        {
            ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero => 0f,
            ProtoControl.HomeostasisTargetMode.HomeostasisTargetFixed => 0f,
            _ => 0f
        };
    }

    private float NormalizeBuffer(int index)
    {
        var buffer = _state.Buffer[index];
        if (!float.IsFinite(buffer))
        {
            buffer = 0f;
            _state.Buffer[index] = 0f;
        }

        return buffer;
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

    private float NormalizeAxonStrength(int axonIndex)
    {
        var strength = _state.Axons.Strengths[axonIndex];
        var normalized = ClampStrengthValue(strength);
        if (normalized != strength)
        {
            _state.Axons.Strengths[axonIndex] = normalized;
            UpdateRuntimeStrengthMetadata(axonIndex, normalized);
        }

        return normalized;
    }

    private bool ApplyPlasticity(
        ulong tickId,
        float potential,
        float sourceBuffer,
        int axonIndex,
        bool plasticityEnabled,
        float plasticityRate,
        bool probabilisticPlasticityUpdates,
        float plasticityDelta)
    {
        if (!plasticityEnabled
            || !float.IsFinite(plasticityRate)
            || plasticityRate < 0f
            || !float.IsFinite(plasticityDelta)
            || plasticityDelta <= 0f)
        {
            return false;
        }

        var normalizedPotential = Math.Clamp(potential, -1f, 1f);
        var activationScale = MathF.Abs(normalizedPotential);
        if (activationScale <= 0f)
        {
            return false;
        }

        var effectiveRate = MathF.Max(plasticityRate, 0f);
        var effectiveDelta = MathF.Max(plasticityDelta, 0f);
        if (probabilisticPlasticityUpdates)
        {
            var probability = Math.Clamp(effectiveRate * activationScale, 0f, 1f);
            if (probability <= 0f)
            {
                return false;
            }

            var seed = _state.GetDeterministicRngInput(tickId, axonIndex).ToSeed();
            if (UnitIntervalFromSeed(seed) >= probability)
            {
                return false;
            }
        }

        var previousRuntimeCode = _state.Axons.RuntimeStrengthCodes[axonIndex];
        var currentStrength = NormalizeAxonStrength(axonIndex);
        var currentMagnitude = MathF.Abs(currentStrength);
        var currentSign = MathF.Sign(currentStrength);
        var potentialSign = MathF.Sign(normalizedPotential);
        if (potentialSign == 0f)
        {
            return false;
        }

        var delta = effectiveDelta * activationScale;
        var nudgeScale = ComputeApproximatePlasticityNudgeScale(axonIndex, sourceBuffer, normalizedPotential, currentStrength);
        delta *= nudgeScale;
        if (delta <= 0f)
        {
            return false;
        }

        var aligned = currentSign == 0f || potentialSign == currentSign;
        var nextMagnitude = aligned
            ? currentMagnitude + delta
            : currentMagnitude - delta;
        if (!float.IsFinite(nextMagnitude) || nextMagnitude < 0f)
        {
            nextMagnitude = 0f;
        }

        var directionSign = currentSign == 0f ? potentialSign : currentSign;
        var updatedStrength = directionSign * nextMagnitude;
        updatedStrength = ClampStrengthValue(updatedStrength);

        _state.Axons.Strengths[axonIndex] = updatedStrength;
        UpdateRuntimeStrengthMetadata(axonIndex, updatedStrength);
        return _state.Axons.RuntimeStrengthCodes[axonIndex] != previousRuntimeCode;
    }

    private float ComputeApproximatePlasticityNudgeScale(
        int axonIndex,
        float sourceBuffer,
        float normalizedPotential,
        float currentStrength)
    {
        if (!TryGetLocalTargetBuffer(axonIndex, out var targetBuffer))
        {
            return 1f;
        }

        var predictiveAlignment = Math.Clamp(normalizedPotential * targetBuffer, -1f, 1f);
        var predictiveScale = 1f + (predictiveAlignment * PlasticityPredictiveNudgeGain);
        var sourceMemory = Math.Clamp(MathF.Abs(sourceBuffer), 0f, 1f);
        var memoryScale = 1f + (sourceMemory * PlasticitySourceMemoryGain);
        var stabilizationScale = 1f - (MathF.Abs(currentStrength) * PlasticitySaturationGain);
        stabilizationScale = Math.Clamp(stabilizationScale, PlasticityMinStabilizationScale, 1f);

        var combined = predictiveScale * memoryScale * stabilizationScale;
        return Math.Clamp(combined, PlasticityMinNudgeScale, PlasticityMaxNudgeScale);
    }

    private bool TryGetLocalTargetBuffer(int axonIndex, out float normalizedTargetBuffer)
    {
        normalizedTargetBuffer = 0f;

        var targetRegionId = _state.Axons.TargetRegionIds[axonIndex];
        if (targetRegionId != _state.RegionId)
        {
            return false;
        }

        var targetNeuronId = _state.Axons.TargetNeuronIds[axonIndex];
        if (!TryGetLocalNeuronIndex(targetNeuronId, out var targetLocalIndex))
        {
            return false;
        }

        if (!_state.Exists[targetLocalIndex] || !_state.Enabled[targetLocalIndex])
        {
            return false;
        }

        normalizedTargetBuffer = Math.Clamp(NormalizeBuffer(targetLocalIndex), -1f, 1f);
        return true;
    }

    private bool TryGetLocalNeuronIndex(int neuronId, out int localIndex)
    {
        localIndex = neuronId - _state.NeuronStart;
        return (uint)localIndex < (uint)_state.NeuronCount;
    }

    private void UpdateRuntimeStrengthMetadata(int axonIndex, float strength)
    {
        var runtimeCode = (byte)_state.StrengthQuantization.Encode(strength, bits: 5);
        _state.Axons.RuntimeStrengthCodes[axonIndex] = runtimeCode;
        _state.Axons.HasRuntimeOverlay[axonIndex] = runtimeCode != _state.Axons.BaseStrengthCodes[axonIndex];
    }

    private uint CountChangedStrengthCodes()
    {
        uint changed = 0;
        for (var i = 0; i < _state.Axons.Count; i++)
        {
            if (_state.Axons.RuntimeStrengthCodes[i] != _state.Axons.BaseStrengthCodes[i])
            {
                changed++;
            }
        }

        return changed;
    }

    private static bool ShouldAutoRebasePlasticity(
        bool plasticityEnabled,
        int totalAxons,
        uint changedCodeCount,
        uint rebaseThreshold,
        float rebaseThresholdPct)
    {
        if (!plasticityEnabled || totalAxons <= 0)
        {
            return false;
        }

        var countTrigger = rebaseThreshold > 0 && changedCodeCount >= rebaseThreshold;
        var pctTrigger = false;
        if (float.IsFinite(rebaseThresholdPct) && rebaseThresholdPct > 0f)
        {
            var fraction = Math.Clamp(rebaseThresholdPct, 0f, 1f);
            pctTrigger = changedCodeCount / (float)totalAxons >= fraction;
        }

        return countTrigger || pctTrigger;
    }

    private void ApplyPlasticityAutoRebase()
    {
        for (var i = 0; i < _state.Axons.Count; i++)
        {
            _state.Axons.BaseStrengthCodes[i] = _state.Axons.RuntimeStrengthCodes[i];
            _state.Axons.HasRuntimeOverlay[i] = false;
        }
    }

    private float ClampStrengthValue(float value)
    {
        if (!float.IsFinite(value))
        {
            value = 0f;
        }

        var min = MathF.Min(_state.StrengthQuantization.Min, _state.StrengthQuantization.Max);
        var max = MathF.Max(_state.StrengthQuantization.Min, _state.StrengthQuantization.Max);
        return Math.Clamp(value, min, max);
    }

    private static float UnitIntervalFromSeed(ulong seed)
    {
        const double scale = 1d / (1UL << 53);
        var bits = seed >> 11;
        return (float)(bits * scale);
    }

    private static float ClampFinite(float value, float min, float max, float fallback)
    {
        if (!float.IsFinite(value))
        {
            return fallback;
        }

        return Math.Clamp(value, min, max);
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

    private void EmitActivityDiagnostics(ulong tickId, Guid brainId, ShardId32 shardId)
    {
        var samples = new List<string>(ActivityDiagnosticsSampleCount);
        for (var i = 0; i < _state.NeuronCount && samples.Count < ActivityDiagnosticsSampleCount; i++)
        {
            if (!_state.Exists[i] || !_state.Enabled[i])
            {
                continue;
            }

            var buffer = _state.Buffer[i];
            if (!float.IsFinite(buffer))
            {
                buffer = 0f;
            }

            var preThreshold = _state.PreActivationThreshold[i];
            var activationThreshold = _state.ActivationThreshold[i];
            var activation = (ActivationFunction)_state.ActivationFunctions[i];
            var potential = Activate(activation, buffer, _state.ParamA[i], _state.ParamB[i]);
            if (!float.IsFinite(potential))
            {
                potential = 0f;
            }

            var passesBufferGate = buffer > preThreshold;
            var passesFireGate = MathF.Abs(potential) > activationThreshold;
            var neuronId = _state.NeuronStart + i;

            samples.Add(
                $"n={neuronId} act={(int)activation} buf={FormatScalar(buffer)} pre={FormatScalar(preThreshold)} thr={FormatScalar(activationThreshold)} pa={FormatScalar(_state.ParamA[i])} pb={FormatScalar(_state.ParamB[i])} gate={passesBufferGate} fire={passesFireGate} pot={FormatScalar(potential)}");
        }

        var sampleSummary = samples.Count == 0
            ? "no-enabled-neurons"
            : string.Join(" | ", samples);

        Console.WriteLine(
            $"[RegionShardCpuDiag] tick={tickId} brain={brainId} shard={shardId} fired=0 sampleCount={samples.Count} {sampleSummary}");
    }

    private static string FormatScalar(float value)
        => value.ToString("0.######", CultureInfo.InvariantCulture);

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return !string.IsNullOrWhiteSpace(value)
               && (string.Equals(value, "1", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "yes", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "on", StringComparison.OrdinalIgnoreCase));
    }

    private static ulong ResolveUnsignedEnv(string key, ulong fallback)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return ulong.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
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
    public bool RemoteCostEnabled { get; init; }
    public long RemoteCostPerBatch { get; init; }
    public long RemoteCostPerContribution { get; init; }
    public float TierAMultiplier { get; init; } = 1f;
    public float TierBMultiplier { get; init; } = 1f;
    public float TierCMultiplier { get; init; } = 1f;
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

public readonly record struct RegionShardNeuronBufferVizEvent(
    uint SourceAddress,
    ulong TickId,
    float Buffer);

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
    uint PlasticityStrengthCodeChanges,
    RegionShardCostSummary Cost,
    IReadOnlyList<RegionShardAxonVizEvent> AxonVizEvents,
    IReadOnlyList<RegionShardNeuronBufferVizEvent> BufferNeuronEvents,
    IReadOnlyList<RegionShardNeuronVizEvent> FiredNeuronEvents);

public readonly record struct RegionShardHomeostasisConfig(
    bool Enabled,
    ProtoControl.HomeostasisTargetMode TargetMode,
    ProtoControl.HomeostasisUpdateMode UpdateMode,
    float BaseProbability,
    uint MinStepCodes,
    bool EnergyCouplingEnabled,
    float EnergyTargetScale,
    float EnergyProbabilityScale)
{
    public static RegionShardHomeostasisConfig Default { get; } = new(
        Enabled: true,
        TargetMode: ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero,
        UpdateMode: ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
        BaseProbability: 0.01f,
        MinStepCodes: 1,
        EnergyCouplingEnabled: false,
        EnergyTargetScale: 1f,
        EnergyProbabilityScale: 1f);
}
