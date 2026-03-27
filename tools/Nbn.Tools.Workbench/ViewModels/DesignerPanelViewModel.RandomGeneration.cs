using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Media;
using Avalonia.Platform.Storage;
using Avalonia.Threading;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
using Nbn.Shared.Sharding;
using Nbn.Shared.Validation;
using Nbn.Tools.Workbench.Services;
using ProtoControl = Nbn.Proto.Control;
using ProtoShardPlanMode = Nbn.Proto.Control.ShardPlanMode;
using SharedShardPlanMode = Nbn.Shared.Sharding.ShardPlanMode;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class DesignerPanelViewModel
{
    private void NewRandomBrain()
    {
        ClearResetConfirmation();
        if (!RandomOptions.TryBuildOptions(GenerateSeed, out var options, out var error))
        {
            Status = error ?? "Random brain options invalid.";
            return;
        }
        var seed = options.Seed;
        var brainId = Guid.NewGuid();
        var brain = new DesignerBrainViewModel("Random Brain", brainId, seed, NbnConstants.DefaultAxonStride);

        var rng = new Random(unchecked((int)seed));
        var selectedRegions = SelectRegions(rng, options);

        var activationPickers = BuildRegionFunctionPickers(
            rng,
            options.ActivationMode,
            options.ActivationFixedId,
            ActivationFunctionIds,
            ActivationFunctionWeights,
            options.ActivationMode == RandomFunctionSelectionMode.Fixed
                ? GetAllowedActivationFunctionIdsForRegion
                : GetRandomAllowedActivationFunctionIdsForRegion,
            GetDefaultActivationFunctionIdForRegion);
        Func<int, IReadOnlyList<int>> resetAllowedSelector = options.ResetMode == RandomFunctionSelectionMode.Fixed
            ? GetAllowedResetFunctionIdsForRegion
            : GetRandomAllowedResetFunctionIdsForRegion;
        var resetPickers = BuildRegionFunctionPickers(
            rng,
            options.ResetMode,
            options.ResetFixedId,
            ResetFunctionIds,
            ResetFunctionWeights,
            resetAllowedSelector,
            GetDefaultResetFunctionIdForRegion);
        var accumulationIds = options.AccumulationMode == RandomFunctionSelectionMode.Fixed
            ? (IReadOnlyList<int>)AccumulationFunctionIds
            : RandomAccumulationFunctionIds;
        var accumulationWeights = options.AccumulationMode == RandomFunctionSelectionMode.Fixed
            ? AccumulationFunctionWeights
            : RandomAccumulationFunctionWeights;
        var accumulationPicker = CreateFunctionPicker(rng, options.AccumulationMode, options.AccumulationFixedId, accumulationIds, accumulationWeights);
        var preActivationPicker = CreateCenteredCodePicker(rng, options.ThresholdMode, options.PreActivationMin, options.PreActivationMin, options.PreActivationMax);
        var activationThresholdPicker = CreateLowBiasedCodePicker(rng, options.ThresholdMode, options.ActivationThresholdMin, options.ActivationThresholdMin, options.ActivationThresholdMax);
        var paramAPicker = CreateCenteredCodePicker(rng, options.ParamMode, options.ParamAMin, options.ParamAMin, options.ParamAMax);
        var paramBPicker = CreateCenteredCodePicker(rng, options.ParamMode, options.ParamBMin, options.ParamBMin, options.ParamBMax);

        for (var i = 0; i < NbnConstants.RegionCount; i++)
        {
            var region = new DesignerRegionViewModel(i);
            var activationPicker = activationPickers[i];
            var resetPicker = resetPickers[i];
            var targetCount = 0;
            if (i == NbnConstants.InputRegionId || i == NbnConstants.OutputRegionId)
            {
                targetCount = i == NbnConstants.InputRegionId
                    ? PickRangeCount(rng, options.InputNeuronMin, options.InputNeuronMax)
                    : PickRangeCount(rng, options.OutputNeuronMin, options.OutputNeuronMax);
            }
            else if (selectedRegions.Contains(i))
            {
                targetCount = PickCount(rng, options.NeuronCountMode, options.NeuronsPerRegion, options.NeuronCountMin, options.NeuronCountMax);
            }

            for (var n = 0; n < targetCount; n++)
            {
                var neuron = CreateDefaultNeuron(region, n);
                var activationId = activationPicker();
                neuron.ActivationFunctionId = activationId;
                neuron.ResetFunctionId = resetPicker();
                neuron.AccumulationFunctionId = accumulationPicker();
                neuron.PreActivationThresholdCode = preActivationPicker();
                neuron.ActivationThresholdCode = activationThresholdPicker();
                neuron.ParamACode = UsesParamA(activationId) ? paramAPicker() : 0;
                neuron.ParamBCode = UsesParamB(activationId) ? paramBPicker() : 0;
                ApplyRandomNeuronActivityGuardrails(neuron, options);
                region.Neurons.Add(neuron);
            }

            region.UpdateCounts();
            brain.Regions.Add(region);
        }

        var regionsWithNeurons = brain.Regions.Where(r => r.NeuronCount > 0).ToList();
        foreach (var region in regionsWithNeurons)
        {
            var validTargets = BuildValidTargetsForRegion(region, regionsWithNeurons, options);
            if (validTargets.Count == 0)
            {
                continue;
            }

            var targetWeights = BuildTargetWeights(region.RegionId, validTargets, options.TargetBiasMode);
            foreach (var neuron in region.Neurons)
            {
                if (!neuron.Exists)
                {
                    continue;
                }

                var axonMin = options.AxonCountMin;
                var axonMax = options.AxonCountMax;
                var biasPower = AxonCountLowBiasPower;
                if (region.RegionId == NbnConstants.OutputRegionId)
                {
                    axonMin = 0;
                    axonMax = Math.Min(axonMax, 10);
                    if (axonMax < axonMin)
                    {
                        axonMin = axonMax;
                    }

                    biasPower = OutputAxonCountLowBiasPower;
                }

                var targetAxons = PickBiasedCount(rng, options.AxonCountMode, options.AxonsPerNeuron, axonMin, axonMax, biasPower);
                var maxTargets = MaxDistinctTargets(region.RegionId, validTargets, options.AllowSelfLoops);
                if (maxTargets == 0)
                {
                    continue;
                }

                if (targetAxons > maxTargets)
                {
                    targetAxons = maxTargets;
                }

                if (targetAxons == 0)
                {
                    continue;
                }

                var seen = new HashSet<(int regionId, int neuronId)>();
                var attempts = 0;
                var maxAttempts = Math.Max(targetAxons * 6, maxTargets * 3);
                while (seen.Count < targetAxons && attempts < maxAttempts)
                {
                    attempts++;
                    var targetRegion = validTargets[PickWeightedIndex(rng, targetWeights)];
                    if (targetRegion.NeuronCount == 0)
                    {
                        continue;
                    }

                    var targetNeuronId = PickTargetNeuronId(rng, neuron.NeuronId, region.RegionId, targetRegion, options.TargetBiasMode);
                    if (!options.AllowSelfLoops
                        && targetRegion.RegionId == region.RegionId
                        && targetNeuronId == neuron.NeuronId)
                    {
                        continue;
                    }

                    if (!seen.Add((targetRegion.RegionId, targetNeuronId)))
                    {
                        continue;
                    }

                    var strength = PickStrengthCode(rng, options.StrengthDistribution, options.StrengthMinCode, options.StrengthMaxCode);
                    neuron.Axons.Add(new DesignerAxonViewModel(targetRegion.RegionId, targetNeuronId, strength));
                }

                neuron.UpdateAxonCount();
            }

            region.UpdateCounts();
        }

        EnsureNeuronOutboundCoverage(rng, brain, options);
        EnsureRegionInboundConnectivity(rng, brain, options);
        EnsureOutputInbound(rng, brain, options);
        EnsureInputOutputInfluencePath(rng, brain, options);
        EnsureOutputEndpointRecurrence(rng, brain, options);
        if (options.SeedBaselineActivityPath)
        {
            EnsureBaselineActivityPath(rng, brain);
        }
        var normalizedFunctions = NormalizeBrainFunctionConstraints(brain);
        brain.UpdateTotals();

        ApplyLoadedBrainDocument(
            brain,
            loadedLabel: brain.Name,
            status: normalizedFunctions == 0
                ? "Random brain created."
                : $"Random brain created. Normalized {normalizedFunctions} neuron function setting(s) for IO constraints.",
            isDirty: true);
    }
    private static void ApplyRandomNeuronActivityGuardrails(DesignerNeuronViewModel neuron, RandomBrainGenerationOptions options)
    {
        if (neuron.RegionId == NbnConstants.InputRegionId)
        {
            var normalizedActivation = false;
            if (options.ActivationMode != RandomFunctionSelectionMode.Fixed)
            {
                neuron.ActivationFunctionId = DefaultInputActivationFunctionId;
                normalizedActivation = true;
            }

            if (options.ResetMode != RandomFunctionSelectionMode.Fixed)
            {
                neuron.ResetFunctionId = DefaultInputResetFunctionId;
            }

            if (options.AccumulationMode != RandomFunctionSelectionMode.Fixed)
            {
                neuron.AccumulationFunctionId = AccumulationFunctionSumId;
            }

            if (options.ThresholdMode != RandomRangeMode.Fixed)
            {
                neuron.PreActivationThresholdCode = 0;
                neuron.ActivationThresholdCode = 0;
            }

            if (normalizedActivation || options.ParamMode != RandomRangeMode.Fixed)
            {
                neuron.ParamACode = 0;
                neuron.ParamBCode = 0;
            }

            return;
        }

        if (options.ActivationMode != RandomFunctionSelectionMode.Fixed)
        {
            var randomAllowed = GetRandomAllowedActivationFunctionIdsForRegion(neuron.RegionId);
            if (!randomAllowed.Contains(neuron.ActivationFunctionId))
            {
                neuron.ActivationFunctionId = GetDefaultActivationFunctionIdForRegion(neuron.RegionId);
            }
        }

        if (neuron.RegionId == NbnConstants.OutputRegionId)
        {
            if (options.ResetMode != RandomFunctionSelectionMode.Fixed)
            {
                neuron.ResetFunctionId = DefaultInputResetFunctionId;
            }

            if (options.AccumulationMode != RandomFunctionSelectionMode.Fixed)
            {
                neuron.AccumulationFunctionId = AccumulationFunctionSumId;
            }

            if (options.ThresholdMode != RandomRangeMode.Fixed)
            {
                neuron.PreActivationThresholdCode = Math.Min(neuron.PreActivationThresholdCode, MaxRandomOutputPreActivationThresholdCode);
                neuron.ActivationThresholdCode = Math.Min(neuron.ActivationThresholdCode, MaxRandomOutputActivationThresholdCode);
            }
        }
        else if (options.ResetMode != RandomFunctionSelectionMode.Fixed
                 && !RandomInternalResetFunctionIds.Contains(neuron.ResetFunctionId))
        {
            neuron.ResetFunctionId = GetDefaultResetFunctionIdForRegion(neuron.RegionId);
        }

        if (options.AccumulationMode != RandomFunctionSelectionMode.Fixed
            && neuron.AccumulationFunctionId == AccumulationFunctionNoneId)
        {
            neuron.AccumulationFunctionId = AccumulationFunctionSumId;
        }

        if (options.ThresholdMode != RandomRangeMode.Fixed)
        {
            neuron.PreActivationThresholdCode = Math.Min(neuron.PreActivationThresholdCode, MaxRandomPreActivationThresholdCode);
            neuron.ActivationThresholdCode = Math.Min(neuron.ActivationThresholdCode, MaxRandomActivationThresholdCode);
        }

        if (options.ParamMode != RandomRangeMode.Fixed)
        {
            neuron.ParamACode = ActivationUsesParamA(neuron.ActivationFunctionId)
                ? Math.Clamp(neuron.ParamACode, MinRandomParamCode, MaxRandomParamCode)
                : 0;
            neuron.ParamBCode = ActivationUsesParamB(neuron.ActivationFunctionId)
                ? Math.Clamp(neuron.ParamBCode, MinRandomParamCode, MaxRandomParamCode)
                : 0;
        }
    }

    private static IReadOnlyList<int> GetAllowedActivationFunctionIdsForRegion(int regionId)
    {
        if (regionId == NbnConstants.InputRegionId)
        {
            return InputAllowedActivationFunctionIds;
        }

        if (regionId == NbnConstants.OutputRegionId)
        {
            return OutputAllowedActivationFunctionIds;
        }

        return ActivationFunctionIds;
    }

    private static IReadOnlyList<int> GetRandomAllowedActivationFunctionIdsForRegion(int regionId)
    {
        if (regionId == NbnConstants.InputRegionId)
        {
            return InputAllowedActivationFunctionIds;
        }

        if (regionId == NbnConstants.OutputRegionId)
        {
            return RandomOutputActivationFunctionIds;
        }

        return RandomInternalActivationFunctionIds;
    }

    private static IReadOnlyList<int> GetAllowedResetFunctionIdsForRegion(int regionId)
    {
        if (regionId == NbnConstants.InputRegionId)
        {
            return InputAllowedResetFunctionIds;
        }

        return ResetFunctionIds;
    }

    private static IReadOnlyList<int> GetRandomAllowedResetFunctionIdsForRegion(int regionId)
    {
        if (regionId == NbnConstants.InputRegionId)
        {
            return InputAllowedResetFunctionIds;
        }

        if (regionId == NbnConstants.OutputRegionId)
        {
            return RandomOutputResetFunctionIds;
        }

        return RandomInternalResetFunctionIds;
    }

    private static int GetDefaultActivationFunctionIdForRegion(int regionId)
    {
        if (regionId == NbnConstants.InputRegionId)
        {
            return DefaultInputActivationFunctionId;
        }

        if (regionId == NbnConstants.OutputRegionId)
        {
            return DefaultOutputActivationFunctionId;
        }

        return DefaultActivationFunctionId;
    }

    private static int GetDefaultResetFunctionIdForRegion(int regionId)
    {
        if (regionId == NbnConstants.InputRegionId)
        {
            return DefaultInputResetFunctionId;
        }

        return 0;
    }

    private static bool IsActivationFunctionAllowedForRegion(int regionId, int functionId)
        => IsKnownActivationFunctionId(functionId)
           && GetAllowedActivationFunctionIdsForRegion(regionId).Contains(functionId);

    private static bool IsResetFunctionAllowedForRegion(int regionId, int functionId)
        => IsKnownResetFunctionId(functionId)
           && GetAllowedResetFunctionIdsForRegion(regionId).Contains(functionId);

    private static bool IsKnownActivationFunctionId(int functionId)
        => functionId >= 0 && functionId <= MaxKnownActivationFunctionId;

    private static bool IsKnownResetFunctionId(int functionId)
        => functionId >= 0 && functionId <= MaxKnownResetFunctionId;

    private static ulong GenerateSeed()
    {
        Span<byte> buffer = stackalloc byte[8];
        RandomNumberGenerator.Fill(buffer);
        return BinaryPrimitives.ReadUInt64LittleEndian(buffer);
    }

    private static HashSet<int> SelectRegions(Random rng, RandomBrainGenerationOptions options)
    {
        if (options.RegionSelectionMode == RandomRegionSelectionMode.ExplicitList)
        {
            return options.ExplicitRegions.ToHashSet();
        }

        var total = NbnConstants.RegionCount - 2;
        if (options.RegionCount <= 0 || total <= 0)
        {
            return new HashSet<int>();
        }

        if (options.RegionSelectionMode == RandomRegionSelectionMode.Contiguous)
        {
            var startMax = total - options.RegionCount + 1;
            var start = rng.Next(1, Math.Max(2, startMax + 1));
            var list = new HashSet<int>();
            for (var id = start; id < start + options.RegionCount; id++)
            {
                list.Add(id);
            }

            return list;
        }

        var available = Enumerable.Range(1, total).ToList();
        var selected = new HashSet<int>();
        var picks = Math.Min(options.RegionCount, available.Count);
        for (var i = 0; i < picks; i++)
        {
            var index = rng.Next(available.Count);
            selected.Add(available[index]);
            available.RemoveAt(index);
        }

        return selected;
    }

    private static Func<int>[] BuildRegionFunctionPickers(
        Random rng,
        RandomFunctionSelectionMode mode,
        int fixedId,
        IReadOnlyList<int> ids,
        IReadOnlyList<double> weights,
        Func<int, IReadOnlyList<int>> allowedIdSelector,
        Func<int, int> defaultSelector)
    {
        var pickers = new Func<int>[NbnConstants.RegionCount];
        for (var regionId = 0; regionId < NbnConstants.RegionCount; regionId++)
        {
            var allowedIds = allowedIdSelector(regionId);
            if (allowedIds.Count == 0)
            {
                allowedIds = ids;
            }

            var regionFixedId = ResolveRegionFixedFunctionId(fixedId, allowedIds, defaultSelector(regionId));
            var regionWeights = BuildSubsetWeights(ids, weights, allowedIds);
            pickers[regionId] = CreateFunctionPicker(rng, mode, regionFixedId, allowedIds, regionWeights);
        }

        return pickers;
    }

    private static double[] BuildSubsetWeights(IReadOnlyList<int> allIds, IReadOnlyList<double> allWeights, IReadOnlyList<int> subsetIds)
    {
        var weightById = new Dictionary<int, double>(allIds.Count);
        var count = Math.Min(allIds.Count, allWeights.Count);
        for (var i = 0; i < count; i++)
        {
            weightById[allIds[i]] = allWeights[i];
        }

        var subsetWeights = new double[subsetIds.Count];
        for (var i = 0; i < subsetIds.Count; i++)
        {
            subsetWeights[i] = weightById.TryGetValue(subsetIds[i], out var weight)
                ? weight
                : 1.0;
        }

        return subsetWeights;
    }

    private static int ResolveRegionFixedFunctionId(int fixedId, IReadOnlyList<int> allowedIds, int fallbackId)
    {
        if (allowedIds.Contains(fixedId))
        {
            return fixedId;
        }

        if (allowedIds.Contains(fallbackId))
        {
            return fallbackId;
        }

        return allowedIds.Count > 0 ? allowedIds[0] : fixedId;
    }

    private static Func<int> CreateFunctionPicker(Random rng, RandomFunctionSelectionMode mode, int fixedId, IReadOnlyList<int> ids, IReadOnlyList<double> weights)
    {
        return mode switch
        {
            RandomFunctionSelectionMode.Fixed => () => fixedId,
            RandomFunctionSelectionMode.Random => () => ids[rng.Next(ids.Count)],
            RandomFunctionSelectionMode.Weighted => () => ids[PickWeightedIndex(rng, weights)],
            _ => () => fixedId
        };
    }

    private static Func<int> CreateCodePicker(Random rng, RandomRangeMode mode, int fixedValue, int min, int max)
    {
        if (mode == RandomRangeMode.Fixed || min == max)
        {
            return () => fixedValue;
        }

        return () => rng.Next(min, max + 1);
    }

    private static Func<int> CreateCenteredCodePicker(Random rng, RandomRangeMode mode, int fixedValue, int min, int max)
    {
        if (mode == RandomRangeMode.Fixed || min == max)
        {
            return () => fixedValue;
        }

        return () => PickCenteredInt(rng, min, max);
    }

    private static Func<int> CreateLowBiasedCodePicker(Random rng, RandomRangeMode mode, int fixedValue, int min, int max)
    {
        if (mode == RandomRangeMode.Fixed || min == max)
        {
            return () => fixedValue;
        }

        return () => PickLowBiasedInt(rng, min, max, 1.8);
    }

    private static int PickCount(Random rng, RandomCountMode mode, int fixedValue, int min, int max)
    {
        if (mode == RandomCountMode.Fixed || min == max)
        {
            return fixedValue;
        }

        return rng.Next(min, max + 1);
    }

    private static int PickRangeCount(Random rng, int min, int max)
    {
        if (min == max)
        {
            return min;
        }

        if (max < min)
        {
            (min, max) = (max, min);
        }

        return rng.Next(min, max + 1);
    }

    private static int PickBiasedCount(Random rng, RandomCountMode mode, int fixedValue, int min, int max, double biasPower)
    {
        if (mode == RandomCountMode.Fixed || min == max)
        {
            return fixedValue;
        }

        if (max < min)
        {
            (min, max) = (max, min);
        }

        var range = max - min;
        var sample = Math.Pow(rng.NextDouble(), biasPower);
        var value = min + (int)Math.Round(sample * range);
        if (value < min)
        {
            return min;
        }

        if (value > max)
        {
            return max;
        }

        return value;
    }


    private static int PickTargetNeuronId(Random rng, int sourceNeuronId, int sourceRegionId, DesignerRegionViewModel targetRegion, RandomTargetBiasMode bias)
    {
        if (targetRegion.NeuronCount <= 1)
        {
            return 0;
        }

        if (bias == RandomTargetBiasMode.DistanceWeighted && targetRegion.RegionId == sourceRegionId)
        {
            var maxDistance = targetRegion.NeuronCount / 2;
            if (maxDistance == 0)
            {
                return 0;
            }

            var distance = (int)Math.Round(Math.Pow(rng.NextDouble(), 2) * maxDistance);
            var direction = rng.Next(2) == 0 ? -1 : 1;
            var candidate = sourceNeuronId + (direction * distance);
            var wrapped = candidate % targetRegion.NeuronCount;
            if (wrapped < 0)
            {
                wrapped += targetRegion.NeuronCount;
            }

            return wrapped;
        }

        return rng.Next(targetRegion.NeuronCount);
    }

    private static int PickCenteredInt(Random rng, int min, int max)
    {
        if (min == max)
        {
            return min;
        }

        if (max < min)
        {
            (min, max) = (max, min);
        }

        var sample = (rng.NextDouble() + rng.NextDouble() + rng.NextDouble()) / 3.0;
        var range = max - min;
        var value = min + (int)Math.Round(sample * range);
        if (value < min)
        {
            return min;
        }

        if (value > max)
        {
            return max;
        }

        return value;
    }

    private static int PickLowBiasedInt(Random rng, int min, int max, double biasPower)
    {
        if (min == max)
        {
            return min;
        }

        if (max < min)
        {
            (min, max) = (max, min);
        }

        var range = max - min;
        var sample = Math.Pow(rng.NextDouble(), biasPower);
        var value = min + (int)Math.Round(sample * range);
        if (value < min)
        {
            return min;
        }

        if (value > max)
        {
            return max;
        }

        return value;
    }

    private static int PickStrengthCode(Random rng, RandomStrengthDistribution distribution, int min, int max)
    {
        if (min == max)
        {
            return min;
        }

        var range = max - min;
        return distribution switch
        {
            RandomStrengthDistribution.Centered => min + (int)Math.Round(((rng.NextDouble() + rng.NextDouble()) * 0.5) * range),
            RandomStrengthDistribution.Normal => PickNormal(rng, min, max),
            _ => rng.Next(min, max + 1)
        };
    }

    private static int PickNormal(Random rng, int min, int max)
    {
        if (min == max)
        {
            return min;
        }

        var mean = (min + max) / 2.0;
        var stdDev = Math.Max(0.5, (max - min) / 6.0);
        var u1 = Math.Max(double.Epsilon, rng.NextDouble());
        var u2 = rng.NextDouble();
        var standard = Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2.0 * Math.PI * u2);
        var sample = mean + (standard * stdDev);
        var rounded = (int)Math.Round(sample);
        if (rounded < min)
        {
            return min;
        }

        if (rounded > max)
        {
            return max;
        }

        return rounded;
    }

    private static double[] BuildTargetWeights(int sourceRegionId, IReadOnlyList<DesignerRegionViewModel> targets, RandomTargetBiasMode bias)
    {
        var weights = new double[targets.Count];
        var sourceZ = RegionZ(sourceRegionId);
        for (var i = 0; i < targets.Count; i++)
        {
            var target = targets[i];
            var weight = bias switch
            {
                RandomTargetBiasMode.RegionWeighted => Math.Max(1, target.NeuronCount),
                RandomTargetBiasMode.DistanceWeighted => 1.0 / (1.0 + ComputeRegionDistance(sourceRegionId, target.RegionId)),
                _ => 1.0
            };

            if (target.RegionId == sourceRegionId)
            {
                weight *= IntraRegionWeight;
            }
            else
            {
                weight *= InterRegionWeight;
            }

            var targetZ = RegionZ(target.RegionId);
            if (targetZ > sourceZ)
            {
                weight *= ForwardRegionWeight;
            }
            else if (targetZ < sourceZ)
            {
                weight *= BackwardRegionWeight;
            }

            if (target.RegionId == NbnConstants.OutputRegionId)
            {
                weight *= OutputRegionWeight;
            }

            weights[i] = weight;
        }

        return weights;
    }

    private static int ComputeRegionDistance(int sourceRegionId, int destRegionId)
    {
        if (sourceRegionId == destRegionId)
        {
            return 0;
        }

        var sourceZ = RegionZ(sourceRegionId);
        var destZ = RegionZ(destRegionId);
        if (sourceZ == destZ)
        {
            return RegionIntrasliceUnit;
        }

        return RegionAxialUnit * Math.Abs(destZ - sourceZ);
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

    private static int MaxDistinctTargets(int sourceRegionId, IReadOnlyList<DesignerRegionViewModel> targets, bool allowSelfLoops)
    {
        var maxTargets = 0;
        foreach (var target in targets)
        {
            if (target.NeuronCount == 0)
            {
                continue;
            }

            if (!allowSelfLoops && target.RegionId == sourceRegionId)
            {
                maxTargets += Math.Max(0, target.NeuronCount - 1);
            }
            else
            {
                maxTargets += target.NeuronCount;
            }
        }

        return maxTargets;
    }

    private static List<DesignerRegionViewModel> BuildValidTargetsForRegion(
        DesignerRegionViewModel sourceRegion,
        IReadOnlyList<DesignerRegionViewModel> regionsWithNeurons,
        RandomBrainGenerationOptions options)
    {
        return regionsWithNeurons
            .Where(target => target.RegionId != NbnConstants.InputRegionId
                             && !(sourceRegion.RegionId == NbnConstants.OutputRegionId && target.RegionId == NbnConstants.OutputRegionId))
            .Where(target => options.AllowInterRegion || target.RegionId == sourceRegion.RegionId)
            .Where(target => options.AllowIntraRegion || target.RegionId != sourceRegion.RegionId)
            .ToList();
    }

    private static bool TryAddRandomAxonFromNeuron(
        Random rng,
        DesignerNeuronViewModel sourceNeuron,
        DesignerRegionViewModel sourceRegion,
        IReadOnlyList<DesignerRegionViewModel> validTargets,
        IReadOnlyList<double> targetWeights,
        RandomBrainGenerationOptions options,
        int maxAttempts = 64)
    {
        if (!sourceNeuron.Exists || sourceNeuron.Axons.Count >= NbnConstants.MaxAxonsPerNeuron || validTargets.Count == 0)
        {
            return false;
        }

        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            var targetRegion = validTargets[PickWeightedIndex(rng, targetWeights)];
            if (targetRegion.NeuronCount == 0 || targetRegion.RegionId == NbnConstants.InputRegionId)
            {
                continue;
            }

            if (sourceRegion.RegionId == NbnConstants.OutputRegionId && targetRegion.RegionId == NbnConstants.OutputRegionId)
            {
                continue;
            }

            var targetNeuronId = PickTargetNeuronId(rng, sourceNeuron.NeuronId, sourceRegion.RegionId, targetRegion, options.TargetBiasMode);
            if (!options.AllowSelfLoops
                && targetRegion.RegionId == sourceRegion.RegionId
                && targetNeuronId == sourceNeuron.NeuronId)
            {
                continue;
            }

            if (sourceNeuron.Axons.Any(axon => axon.TargetRegionId == targetRegion.RegionId && axon.TargetNeuronId == targetNeuronId))
            {
                continue;
            }

            var strength = PickStrengthCode(rng, options.StrengthDistribution, options.StrengthMinCode, options.StrengthMaxCode);
            sourceNeuron.Axons.Add(new DesignerAxonViewModel(targetRegion.RegionId, targetNeuronId, strength));
            sourceNeuron.UpdateAxonCount();
            return true;
        }

        return false;
    }

    private static void EnsureNeuronOutboundCoverage(Random rng, DesignerBrainViewModel brain, RandomBrainGenerationOptions options)
    {
        if (options.AxonCountMax <= 0)
        {
            return;
        }

        var regionsWithNeurons = brain.Regions.Where(region => region.NeuronCount > 0).ToList();
        foreach (var sourceRegion in regionsWithNeurons)
        {
            if (sourceRegion.RegionId == NbnConstants.OutputRegionId)
            {
                continue;
            }

            var validTargets = BuildValidTargetsForRegion(sourceRegion, regionsWithNeurons, options);
            if (validTargets.Count == 0)
            {
                continue;
            }

            var targetWeights = BuildTargetWeights(sourceRegion.RegionId, validTargets, options.TargetBiasMode);
            var changed = false;
            foreach (var neuron in sourceRegion.Neurons)
            {
                if (!neuron.Exists || neuron.Axons.Count > 0)
                {
                    continue;
                }

                if (TryAddRandomAxonFromNeuron(rng, neuron, sourceRegion, validTargets, targetWeights, options))
                {
                    changed = true;
                }
            }

            if (changed)
            {
                sourceRegion.UpdateCounts();
            }
        }
    }

    private static void EnsureRegionInboundConnectivity(Random rng, DesignerBrainViewModel brain, RandomBrainGenerationOptions options)
    {
        if (!options.AllowInterRegion || options.AxonCountMax <= 0)
        {
            return;
        }

        var regionsWithNeurons = brain.Regions.Where(region => region.NeuronCount > 0).ToList();
        foreach (var targetRegion in regionsWithNeurons)
        {
            if (targetRegion.RegionId == NbnConstants.InputRegionId)
            {
                continue;
            }

            var hasInbound = regionsWithNeurons
                .Where(source => source.RegionId != targetRegion.RegionId)
                .SelectMany(source => source.Neurons.Where(neuron => neuron.Exists))
                .Any(neuron => neuron.Axons.Any(axon => axon.TargetRegionId == targetRegion.RegionId));

            if (hasInbound)
            {
                continue;
            }

            var sourceRegions = regionsWithNeurons
                .Where(source => source.RegionId != targetRegion.RegionId && source.NeuronCount > 0)
                .ToList();

            if (sourceRegions.Count == 0)
            {
                continue;
            }

            for (var attempt = 0; attempt < 128; attempt++)
            {
                var sourceRegion = sourceRegions[rng.Next(sourceRegions.Count)];
                var sourceNeuron = sourceRegion.Neurons[rng.Next(sourceRegion.NeuronCount)];
                var singleTarget = new[] { targetRegion };
                var singleWeight = new[] { 1.0 };
                if (TryAddRandomAxonFromNeuron(rng, sourceNeuron, sourceRegion, singleTarget, singleWeight, options, maxAttempts: 24))
                {
                    sourceRegion.UpdateCounts();
                    break;
                }
            }
        }
    }

    private static void EnsureOutputInbound(Random rng, DesignerBrainViewModel brain, RandomBrainGenerationOptions options)
    {
        if (!options.AllowInterRegion || options.AxonCountMax <= 0)
        {
            return;
        }

        var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
        if (outputRegion.NeuronCount == 0)
        {
            return;
        }

        var hasInbound = brain.Regions
            .Where(region => region.RegionId != NbnConstants.OutputRegionId)
            .SelectMany(region => region.Neurons)
            .Any(neuron => neuron.Axons.Any(axon => axon.TargetRegionId == NbnConstants.OutputRegionId));

        if (hasInbound)
        {
            return;
        }

        var sourceRegions = brain.Regions
            .Where(region => region.RegionId != NbnConstants.OutputRegionId && region.NeuronCount > 0)
            .ToList();

        if (sourceRegions.Count == 0)
        {
            return;
        }

        for (var attempt = 0; attempt < 64; attempt++)
        {
            var sourceRegion = sourceRegions[rng.Next(sourceRegions.Count)];
            if (sourceRegion.NeuronCount == 0)
            {
                continue;
            }

            var sourceNeuron = sourceRegion.Neurons[rng.Next(sourceRegion.NeuronCount)];
            if (!sourceNeuron.Exists)
            {
                continue;
            }

            if (sourceNeuron.Axons.Count >= NbnConstants.MaxAxonsPerNeuron)
            {
                continue;
            }

            var targetNeuronId = rng.Next(outputRegion.NeuronCount);
            if (sourceNeuron.Axons.Any(axon => axon.TargetRegionId == NbnConstants.OutputRegionId
                                               && axon.TargetNeuronId == targetNeuronId))
            {
                continue;
            }

            var strength = PickStrengthCode(rng, options.StrengthDistribution, options.StrengthMinCode, options.StrengthMaxCode);
            sourceNeuron.Axons.Add(new DesignerAxonViewModel(NbnConstants.OutputRegionId, targetNeuronId, strength));
            sourceNeuron.UpdateAxonCount();
            sourceRegion.UpdateCounts();
            return;
        }
    }

    private bool HasSpawnServiceReadiness()
    {
        return _connections.HasSpawnServiceReadiness();
    }

    private static void EnsureInputOutputInfluencePath(Random rng, DesignerBrainViewModel brain, RandomBrainGenerationOptions options)
    {
        if (!options.AllowInterRegion || options.AxonCountMax <= 0)
        {
            return;
        }

        var inputRegion = brain.Regions[NbnConstants.InputRegionId];
        var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
        if (inputRegion.NeuronCount == 0 || outputRegion.NeuronCount == 0)
        {
            return;
        }

        if (HasInputToOutputPath(brain))
        {
            return;
        }

        var sourceCandidates = inputRegion.Neurons
            .Where(neuron => neuron.Exists && neuron.Axons.Count < NbnConstants.MaxAxonsPerNeuron)
            .ToList();
        if (sourceCandidates.Count == 0)
        {
            return;
        }

        var targetCandidates = outputRegion.Neurons.Where(neuron => neuron.Exists).ToList();
        if (targetCandidates.Count == 0)
        {
            return;
        }

        var relayCandidates = brain.Regions
            .Where(region => region.RegionId != NbnConstants.InputRegionId
                             && region.RegionId != NbnConstants.OutputRegionId
                             && region.NeuronCount > 0)
            .SelectMany(region => region.Neurons
                .Where(neuron => neuron.Exists)
                .Select(neuron => (region, neuron)))
            .ToList();

        for (var attempt = 0; attempt < 64; attempt++)
        {
            var sourceNeuron = sourceCandidates[rng.Next(sourceCandidates.Count)];
            var outputNeuron = targetCandidates[rng.Next(targetCandidates.Count)];

            if (relayCandidates.Count > 0)
            {
                var relay = relayCandidates[rng.Next(relayCandidates.Count)];
                var hasInputRelay = TryAddGuidedAxon(
                    rng,
                    sourceNeuron,
                    inputRegion.RegionId,
                    relay.region.RegionId,
                    relay.neuron.NeuronId,
                    options,
                    GuidedPathStrengthMinCode,
                    GuidedPathStrengthMaxCode);
                var hasRelayOutput = TryAddGuidedAxon(
                    rng,
                    relay.neuron,
                    relay.region.RegionId,
                    outputRegion.RegionId,
                    outputNeuron.NeuronId,
                    options,
                    GuidedPathStrengthMinCode,
                    GuidedPathStrengthMaxCode);
                if (hasInputRelay && hasRelayOutput)
                {
                    inputRegion.UpdateCounts();
                    relay.region.UpdateCounts();
                    return;
                }
            }

            if (TryAddGuidedAxon(
                    rng,
                    sourceNeuron,
                    inputRegion.RegionId,
                    outputRegion.RegionId,
                    outputNeuron.NeuronId,
                    options,
                    GuidedPathStrengthMinCode,
                    GuidedPathStrengthMaxCode))
            {
                inputRegion.UpdateCounts();
                return;
            }
        }
    }

    private static void EnsureOutputEndpointRecurrence(Random rng, DesignerBrainViewModel brain, RandomBrainGenerationOptions options)
    {
        if (!options.AllowInterRegion || options.AxonCountMax <= 0)
        {
            return;
        }

        var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
        if (outputRegion.NeuronCount == 0)
        {
            return;
        }

        var outputNeurons = outputRegion.Neurons.Where(neuron => neuron.Exists).ToList();
        if (outputNeurons.Count == 0)
        {
            return;
        }

        var endpointTargets = CollectInputEndpointTargets(brain);
        if (endpointTargets.Count == 0 && TrySeedInputEndpoint(rng, brain, options))
        {
            endpointTargets = CollectInputEndpointTargets(brain);
        }

        if (endpointTargets.Count == 0)
        {
            return;
        }

        var endpointKeys = endpointTargets
            .Select(target => (target.region.RegionId, target.neuron.NeuronId))
            .ToHashSet();
        var usedTargets = new HashSet<(int regionId, int neuronId)>();
        var recurrenceProbability = options.SeedBaselineActivityPath
            ? OutputEndpointRecurrenceProbability
            : OutputEndpointRecurrenceProbabilityWithoutBaseline;

        for (var i = outputNeurons.Count - 1; i > 0; i--)
        {
            var j = rng.Next(i + 1);
            (outputNeurons[i], outputNeurons[j]) = (outputNeurons[j], outputNeurons[i]);
        }

        foreach (var outputNeuron in outputNeurons)
        {
            var existingBridge = outputNeuron.Axons
                .Select(axon => (axon.TargetRegionId, axon.TargetNeuronId))
                .FirstOrDefault(endpointKeys.Contains);
            if (existingBridge != default)
            {
                usedTargets.Add(existingBridge);
                continue;
            }

            if (outputNeuron.Axons.Count >= NbnConstants.MaxAxonsPerNeuron
                || rng.NextDouble() > recurrenceProbability)
            {
                continue;
            }

            var candidates = endpointTargets
                .Where(target => !outputNeuron.Axons.Any(axon => axon.TargetRegionId == target.region.RegionId
                                                                 && axon.TargetNeuronId == target.neuron.NeuronId))
                .ToList();
            if (candidates.Count == 0)
            {
                continue;
            }

            var uniqueCandidates = candidates
                .Where(target => !usedTargets.Contains((target.region.RegionId, target.neuron.NeuronId)))
                .ToList();
            var selectedPool = uniqueCandidates.Count > 0 ? uniqueCandidates : candidates;
            var selected = selectedPool[rng.Next(selectedPool.Count)];
            if (!TryAddGuidedAxon(
                    rng,
                    outputNeuron,
                    outputRegion.RegionId,
                    selected.region.RegionId,
                    selected.neuron.NeuronId,
                    options,
                    RecurrentBridgeStrengthMinCode,
                    RecurrentBridgeStrengthMaxCode))
            {
                continue;
            }

            usedTargets.Add((selected.region.RegionId, selected.neuron.NeuronId));
        }

        outputRegion.UpdateCounts();
    }

    private static bool TrySeedInputEndpoint(Random rng, DesignerBrainViewModel brain, RandomBrainGenerationOptions options)
    {
        if (!options.AllowInterRegion)
        {
            return false;
        }

        var inputRegion = brain.Regions[NbnConstants.InputRegionId];
        if (inputRegion.NeuronCount == 0)
        {
            return false;
        }

        var inputSources = inputRegion.Neurons
            .Where(neuron => neuron.Exists && neuron.Axons.Count < NbnConstants.MaxAxonsPerNeuron)
            .ToList();
        if (inputSources.Count == 0)
        {
            return false;
        }

        var internalTargets = brain.Regions
            .Where(region => region.RegionId != NbnConstants.InputRegionId
                             && region.RegionId != NbnConstants.OutputRegionId
                             && region.NeuronCount > 0)
            .SelectMany(region => region.Neurons
                .Where(neuron => neuron.Exists)
                .Select(neuron => (region, neuron)))
            .ToList();
        if (internalTargets.Count == 0)
        {
            return false;
        }

        for (var attempt = 0; attempt < 64; attempt++)
        {
            var sourceNeuron = inputSources[rng.Next(inputSources.Count)];
            var target = internalTargets[rng.Next(internalTargets.Count)];
            if (!TryAddGuidedAxon(
                    rng,
                    sourceNeuron,
                    inputRegion.RegionId,
                    target.region.RegionId,
                    target.neuron.NeuronId,
                    options,
                    GuidedPathStrengthMinCode,
                    GuidedPathStrengthMaxCode))
            {
                continue;
            }

            inputRegion.UpdateCounts();
            target.region.UpdateCounts();
            return true;
        }

        return false;
    }

    private static List<(DesignerRegionViewModel region, DesignerNeuronViewModel neuron)> CollectInputEndpointTargets(DesignerBrainViewModel brain)
    {
        var inputRegion = brain.Regions[NbnConstants.InputRegionId];
        if (inputRegion.NeuronCount == 0)
        {
            return new List<(DesignerRegionViewModel region, DesignerNeuronViewModel neuron)>();
        }

        var seen = new HashSet<(int regionId, int neuronId)>();
        var targets = new List<(DesignerRegionViewModel region, DesignerNeuronViewModel neuron)>();
        foreach (var inputNeuron in inputRegion.Neurons.Where(neuron => neuron.Exists))
        {
            foreach (var axon in inputNeuron.Axons)
            {
                if (axon.TargetRegionId == NbnConstants.InputRegionId || axon.TargetRegionId == NbnConstants.OutputRegionId)
                {
                    continue;
                }

                if (axon.TargetRegionId < 0 || axon.TargetRegionId >= brain.Regions.Count)
                {
                    continue;
                }

                var targetRegion = brain.Regions[axon.TargetRegionId];
                if (axon.TargetNeuronId < 0 || axon.TargetNeuronId >= targetRegion.NeuronCount)
                {
                    continue;
                }

                var targetNeuron = targetRegion.Neurons[axon.TargetNeuronId];
                if (!targetNeuron.Exists)
                {
                    continue;
                }

                if (!seen.Add((targetRegion.RegionId, targetNeuron.NeuronId)))
                {
                    continue;
                }

                targets.Add((targetRegion, targetNeuron));
            }
        }

        return targets;
    }

    private static bool TryAddGuidedAxon(
        Random rng,
        DesignerNeuronViewModel sourceNeuron,
        int sourceRegionId,
        int targetRegionId,
        int targetNeuronId,
        RandomBrainGenerationOptions options,
        int preferredMinStrengthCode,
        int preferredMaxStrengthCode)
    {
        if (!sourceNeuron.Exists
            || targetRegionId == NbnConstants.InputRegionId
            || (sourceRegionId == NbnConstants.OutputRegionId && targetRegionId == NbnConstants.OutputRegionId)
            || (!options.AllowInterRegion && sourceRegionId != targetRegionId)
            || (!options.AllowIntraRegion && sourceRegionId == targetRegionId)
            || (!options.AllowSelfLoops && sourceRegionId == targetRegionId && sourceNeuron.NeuronId == targetNeuronId))
        {
            return false;
        }

        if (sourceNeuron.Axons.Any(axon => axon.TargetRegionId == targetRegionId && axon.TargetNeuronId == targetNeuronId))
        {
            return true;
        }

        if (sourceNeuron.Axons.Count >= NbnConstants.MaxAxonsPerNeuron)
        {
            return false;
        }

        var strengthCode = PickGuidedStrengthCode(rng, options, preferredMinStrengthCode, preferredMaxStrengthCode);
        sourceNeuron.Axons.Add(new DesignerAxonViewModel(targetRegionId, targetNeuronId, strengthCode));
        sourceNeuron.UpdateAxonCount();
        return true;
    }

    private static int PickGuidedStrengthCode(Random rng, RandomBrainGenerationOptions options, int preferredMinCode, int preferredMaxCode)
    {
        var min = Math.Clamp(options.StrengthMinCode, 0, 31);
        var max = Math.Clamp(options.StrengthMaxCode, 0, 31);
        if (max < min)
        {
            (min, max) = (max, min);
        }

        var guidedMin = Math.Clamp(preferredMinCode, min, max);
        var guidedMax = Math.Clamp(preferredMaxCode, guidedMin, max);
        return PickLowBiasedInt(rng, guidedMin, guidedMax, 1.35);
    }

    private static bool HasInputToOutputPath(DesignerBrainViewModel brain)
    {
        var existing = new HashSet<(int regionId, int neuronId)>();
        var adjacency = new Dictionary<(int regionId, int neuronId), List<(int regionId, int neuronId)>>();
        foreach (var region in brain.Regions)
        {
            foreach (var neuron in region.Neurons.Where(neuron => neuron.Exists))
            {
                existing.Add((region.RegionId, neuron.NeuronId));
            }
        }

        foreach (var region in brain.Regions)
        {
            foreach (var neuron in region.Neurons.Where(neuron => neuron.Exists))
            {
                var source = (region.RegionId, neuron.NeuronId);
                foreach (var axon in neuron.Axons)
                {
                    var target = (axon.TargetRegionId, axon.TargetNeuronId);
                    if (!existing.Contains(target))
                    {
                        continue;
                    }

                    if (!adjacency.TryGetValue(source, out var targets))
                    {
                        targets = new List<(int regionId, int neuronId)>();
                        adjacency[source] = targets;
                    }

                    targets.Add(target);
                }
            }
        }

        var queue = new Queue<(int regionId, int neuronId)>(existing.Where(node => node.regionId == NbnConstants.InputRegionId));
        var visited = new HashSet<(int regionId, int neuronId)>();
        while (queue.Count > 0)
        {
            var node = queue.Dequeue();
            if (!visited.Add(node))
            {
                continue;
            }

            if (node.regionId == NbnConstants.OutputRegionId)
            {
                return true;
            }

            if (!adjacency.TryGetValue(node, out var next))
            {
                continue;
            }

            foreach (var target in next)
            {
                if (!visited.Contains(target))
                {
                    queue.Enqueue(target);
                }
            }
        }

        return false;
    }

    private static void EnsureBaselineActivityPath(Random rng, DesignerBrainViewModel brain)
    {
        var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
        if (outputRegion.NeuronCount == 0)
        {
            return;
        }

        var outputNeuron = outputRegion.Neurons.FirstOrDefault(neuron => neuron.Exists) ?? outputRegion.Neurons[0];
        outputNeuron.Exists = true;
        outputNeuron.ActivationFunctionId = DefaultInputActivationFunctionId;
        outputNeuron.ResetFunctionId = DefaultInputResetFunctionId;
        outputNeuron.AccumulationFunctionId = 0;
        outputNeuron.PreActivationThresholdCode = 0;
        outputNeuron.ActivationThresholdCode = 0;
        outputNeuron.ParamACode = 0;
        outputNeuron.ParamBCode = 0;

        var candidateRegions = brain.Regions
            .Where(region => region.RegionId != NbnConstants.InputRegionId
                             && region.RegionId != NbnConstants.OutputRegionId
                             && region.NeuronCount > 0)
            .ToList();

        if (candidateRegions.Count == 0)
        {
            var fallbackRegion = brain.Regions.FirstOrDefault(region =>
                region.RegionId != NbnConstants.InputRegionId
                && region.RegionId != NbnConstants.OutputRegionId);
            if (fallbackRegion is null)
            {
                return;
            }

            if (fallbackRegion.NeuronCount == 0)
            {
                fallbackRegion.Neurons.Add(CreateDefaultNeuron(fallbackRegion, 0));
                fallbackRegion.UpdateCounts();
            }

            candidateRegions.Add(fallbackRegion);
        }

        var sourceRegion = candidateRegions[rng.Next(candidateRegions.Count)];
        var sourceNeuron = sourceRegion.Neurons.FirstOrDefault(neuron => neuron.Exists) ?? sourceRegion.Neurons[0];
        sourceNeuron.Exists = true;
        sourceNeuron.ActivationFunctionId = ActivityDriverActivationFunctionId;
        sourceNeuron.ResetFunctionId = ActivityDriverResetFunctionId;
        sourceNeuron.AccumulationFunctionId = 0;
        sourceNeuron.PreActivationThresholdCode = 0;
        sourceNeuron.ActivationThresholdCode = 0;
        sourceNeuron.ParamACode = ActivityDriverParamACode;
        sourceNeuron.ParamBCode = 0;

        var hasOutputEdge = sourceNeuron.Axons.Any(axon => axon.TargetRegionId == NbnConstants.OutputRegionId
                                                           && axon.TargetNeuronId == outputNeuron.NeuronId);
        if (!hasOutputEdge && sourceNeuron.Axons.Count < NbnConstants.MaxAxonsPerNeuron)
        {
            sourceNeuron.Axons.Add(new DesignerAxonViewModel(
                NbnConstants.OutputRegionId,
                outputNeuron.NeuronId,
                ActivityDriverStrengthCode));
        }

        var outputAxon = sourceNeuron.Axons.FirstOrDefault(axon => axon.TargetRegionId == NbnConstants.OutputRegionId
                                                                    && axon.TargetNeuronId == outputNeuron.NeuronId);
        if (outputAxon is not null)
        {
            outputAxon.StrengthCode = ActivityDriverStrengthCode;
        }

        sourceNeuron.UpdateAxonCount();
        sourceRegion.UpdateCounts();
        outputRegion.UpdateCounts();
    }

    private static int PickWeightedIndex(Random rng, IReadOnlyList<double> weights)
    {
        var total = 0.0;
        for (var i = 0; i < weights.Count; i++)
        {
            total += Math.Max(0.0, weights[i]);
        }

        if (total <= 0)
        {
            return rng.Next(weights.Count);
        }

        var roll = rng.NextDouble() * total;
        var cumulative = 0.0;
        for (var i = 0; i < weights.Count; i++)
        {
            cumulative += Math.Max(0.0, weights[i]);
            if (roll <= cumulative)
            {
                return i;
            }
        }

        return weights.Count - 1;
    }

    private static double[] BuildActivationFunctionWeights()
    {
        var costs = new[]
        {
            0.0, // ACT_NONE
            1.0, // ACT_IDENTITY
            1.0, // ACT_STEP_UP
            1.0, // ACT_STEP_MID
            1.0, // ACT_STEP_DOWN
            1.1, // ACT_ABS
            1.1, // ACT_CLAMP
            1.1, // ACT_RELU
            1.1, // ACT_NRELU
            1.4, // ACT_SIN
            1.6, // ACT_TAN
            1.6, // ACT_TANH
            1.8, // ACT_ELU
            1.8, // ACT_EXP
            1.4, // ACT_PRELU
            1.9, // ACT_LOG
            1.2, // ACT_MULT
            1.2, // ACT_ADD
            2.0, // ACT_SIG
            2.0, // ACT_SILU
            1.3, // ACT_PCLAMP
            2.6, // ACT_MODL
            2.6, // ACT_MODR
            2.8, // ACT_SOFTP
            2.8, // ACT_SELU
            1.4, // ACT_LIN
            3.0, // ACT_LOGB
            3.5, // ACT_POW
            5.0, // ACT_GAUSS
            6.0  // ACT_QUAD
        };

        var weights = BuildInverseCostWeights(costs, minWeight: 0.1, maxWeight: 2.4);
        weights[1] = 0.16;  // ACT_IDENTITY
        weights[2] = 0.10;  // ACT_STEP_UP
        weights[3] = 0.10;  // ACT_STEP_MID
        weights[4] = 0.08;  // ACT_STEP_DOWN
        weights[6] = 0.34;  // ACT_CLAMP
        weights[9] = 0.14;  // ACT_SIN
        weights[11] = 0.38; // ACT_TANH
        weights[18] = 0.28; // ACT_SIG
        weights[20] = 0.20; // ACT_PCLAMP
        weights[7] = 0.06;  // ACT_RELU
        weights[8] = 0.06;  // ACT_NRELU
        weights[13] = 0.04; // ACT_EXP
        weights[28] = 0.01; // ACT_GAUSS
        weights[29] = 0.01; // ACT_QUAD
        return weights;
    }

    private static double[] BuildResetFunctionWeights()
    {
        var costs = new[]
        {
            0.2, // RESET_ZERO
            1.0, // RESET_HOLD
            1.0, // RESET_CLAMP_POTENTIAL
            1.0, // RESET_CLAMP1
            1.0, // RESET_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_NEG_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_TENTH_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_HALF_POTENTIAL_CLAMP_BUFFER
            1.2, // RESET_DOUBLE_POTENTIAL_CLAMP_BUFFER
            1.3, // RESET_FIVEX_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_NEG_TENTH_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_NEG_HALF_POTENTIAL_CLAMP_BUFFER
            1.2, // RESET_NEG_DOUBLE_POTENTIAL_CLAMP_BUFFER
            1.3, // RESET_NEG_FIVEX_POTENTIAL_CLAMP_BUFFER
            1.8, // RESET_INVERSE_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_POTENTIAL_CLAMP1
            1.0, // RESET_NEG_POTENTIAL_CLAMP1
            1.0, // RESET_HUNDREDTHS_POTENTIAL_CLAMP1
            1.0, // RESET_TENTH_POTENTIAL_CLAMP1
            1.0, // RESET_HALF_POTENTIAL_CLAMP1
            1.2, // RESET_DOUBLE_POTENTIAL_CLAMP1
            1.3, // RESET_FIVEX_POTENTIAL_CLAMP1
            1.0, // RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP1
            1.0, // RESET_NEG_TENTH_POTENTIAL_CLAMP1
            1.0, // RESET_NEG_HALF_POTENTIAL_CLAMP1
            1.2, // RESET_NEG_DOUBLE_POTENTIAL_CLAMP1
            1.3, // RESET_NEG_FIVEX_POTENTIAL_CLAMP1
            1.8, // RESET_INVERSE_POTENTIAL_CLAMP1
            1.0, // RESET_POTENTIAL
            1.0, // RESET_NEG_POTENTIAL
            1.0, // RESET_HUNDREDTHS_POTENTIAL
            1.0, // RESET_TENTH_POTENTIAL
            1.0, // RESET_HALF_POTENTIAL
            1.2, // RESET_DOUBLE_POTENTIAL
            1.3, // RESET_FIVEX_POTENTIAL
            1.0, // RESET_NEG_HUNDREDTHS_POTENTIAL
            1.0, // RESET_NEG_TENTH_POTENTIAL
            1.0, // RESET_NEG_HALF_POTENTIAL
            1.2, // RESET_NEG_DOUBLE_POTENTIAL
            1.3, // RESET_NEG_FIVEX_POTENTIAL
            1.8, // RESET_INVERSE_POTENTIAL
            1.0, // RESET_HALF
            1.0, // RESET_TENTH
            1.0, // RESET_HUNDREDTH
            1.0, // RESET_NEGATIVE
            1.0, // RESET_NEG_HALF
            1.0, // RESET_NEG_TENTH
            1.0, // RESET_NEG_HUNDREDTH
            1.2, // RESET_DOUBLE_CLAMP1
            1.3, // RESET_FIVEX_CLAMP1
            1.2, // RESET_NEG_DOUBLE_CLAMP1
            1.3, // RESET_NEG_FIVEX_CLAMP1
            1.2, // RESET_DOUBLE
            1.3, // RESET_FIVEX
            1.2, // RESET_NEG_DOUBLE
            1.3, // RESET_NEG_FIVEX
            1.1, // RESET_DIVIDE_AXON_CT
            1.8, // RESET_INVERSE_CLAMP1
            1.8  // RESET_INVERSE
        };

        var weights = BuildInverseCostWeights(costs, minWeight: 0.1, maxWeight: 2.4);
        weights[ResetHoldFunctionId] = 0.35;
        weights[ResetHalfFunctionId] = 0.30;
        weights[ResetTenthFunctionId] = 0.20;
        weights[ResetHundredthFunctionId] = 0.10;
        weights[DefaultInputResetFunctionId] = 0.05;
        return weights;
    }

    private static double[] BuildAccumulationFunctionWeights()
    {
        var costs = new[] { 1.0, 1.2, 1.0, 0.1 };
        return BuildInverseCostWeights(costs, minWeight: 0.2, maxWeight: 3.0);
    }

    private static double[] BuildInverseCostWeights(IReadOnlyList<double> costs, double minWeight, double maxWeight)
    {
        var weights = new double[costs.Count];
        for (var i = 0; i < costs.Count; i++)
        {
            var cost = costs[i];
            var weight = cost <= 0.0 ? minWeight : 1.0 / cost;
            if (weight < minWeight)
            {
                weight = minWeight;
            }
            else if (weight > maxWeight)
            {
                weight = maxWeight;
            }

            weights[i] = weight;
        }

        return weights;
    }
}
