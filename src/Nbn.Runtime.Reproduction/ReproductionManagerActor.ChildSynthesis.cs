using Nbn.Proto;
using Nbn.Proto.Repro;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
using Nbn.Shared.Validation;

namespace Nbn.Runtime.Reproduction;

public sealed partial class ReproductionManagerActor
{
    private async Task<ChildBuildResult> BuildAndStoreChildDefinitionAsync(
        ParsedParent parentA,
        ParsedParent parentB,
        ArtifactRef parentARef,
        ArtifactRef parentBRef,
        ReproduceConfig? config,
        ulong seed,
        IReadOnlyList<ManualIoNeuronEdit> manualIoNeuronAdds,
        IReadOnlyList<ManualIoNeuronEdit> manualIoNeuronRemoves)
    {
        if (!TryBuildChildSections(
                parentA,
                parentB,
                config,
                seed,
                manualIoNeuronAdds,
                manualIoNeuronRemoves,
                out var childSections,
                out var summary,
                out var abortReason))
        {
            return new ChildBuildResult(null, null, abortReason ?? "repro_manual_io_neuron_ops_invalid");
        }

        var childHeader = BuildChildHeader(parentA.Header, parentB.Header, childSections, seed);
        var validation = NbnBinaryValidator.ValidateNbn(childHeader, childSections);
        if (!validation.IsValid)
        {
            return new ChildBuildResult(null, null, "repro_child_validation_failed");
        }

        var lineageSimilarity = ComputeLineageSimilarityScores(
            parentA,
            parentB,
            childHeader,
            childSections,
            ResolveSpanTolerance(config),
            seed);

        var bytes = NbnBinary.WriteNbn(childHeader, childSections);
        var storeRoot = ResolveChildStoreRoot(parentARef.StoreUri, parentBRef.StoreUri);
        var storeUri = ResolveChildStoreUri(parentARef.StoreUri, parentBRef.StoreUri, storeRoot);
        IArtifactStore store;
        try
        {
            store = new ArtifactStoreResolver(new ArtifactStoreResolverOptions(storeRoot)).Resolve(storeUri);
        }
        catch
        {
            return new ChildBuildResult(null, null, "repro_child_artifact_store_unavailable");
        }

        await using var stream = new MemoryStream(bytes, writable: false);
        var manifest = await store.StoreAsync(stream, NbnMediaType).ConfigureAwait(false);

        var childRef = manifest.ArtifactId.Bytes.ToArray().ToArtifactRef(
            (ulong)manifest.ByteLength,
            NbnMediaType,
            storeUri);
        return new ChildBuildResult(
            childRef,
            summary,
            null,
            CountPresentRegions(childHeader),
            lineageSimilarity.LineageSimilarityScore,
            lineageSimilarity.ParentASimilarityScore,
            lineageSimilarity.ParentBSimilarityScore);
    }

    private static bool TryBuildChildSections(
        ParsedParent parentA,
        ParsedParent parentB,
        ReproduceConfig? config,
        ulong seed,
        IReadOnlyList<ManualIoNeuronEdit> manualIoNeuronAdds,
        IReadOnlyList<ManualIoNeuronEdit> manualIoNeuronRemoves,
        out List<NbnRegionSection> childSections,
        out MutationSummary summary,
        out string? abortReason)
    {
        abortReason = null;
        var sectionsA = BuildSectionMap(parentA.Regions);
        var sectionsB = BuildSectionMap(parentB.Regions);
        var state = seed == 0 ? DefaultSpotCheckSeed : seed;
        var budgets = CreateMutationBudgets(sectionsA, sectionsB, config?.Limits);
        var mutableRegions = BuildBaseChildRegions(
            sectionsA,
            sectionsB,
            parentA.Header.Quantization,
            config,
            ref state,
            budgets);
        ApplyStructuralMutations(mutableRegions, config, ref state, budgets);
        if (!TryApplyManualIoNeuronOperations(
                mutableRegions,
                config,
                manualIoNeuronAdds,
                manualIoNeuronRemoves,
                budgets,
                out abortReason))
        {
            childSections = new List<NbnRegionSection>();
            summary = new MutationSummary();
            return false;
        }

        childSections = BuildSectionsFromMutableRegions(mutableRegions, parentA.Header.AxonStride, budgets);

        summary = new MutationSummary
        {
            NeuronsAdded = budgets.NeuronsAdded,
            NeuronsRemoved = budgets.NeuronsRemoved,
            AxonsAdded = budgets.AxonsAdded,
            AxonsRemoved = budgets.AxonsRemoved,
            AxonsRerouted = budgets.AxonsRerouted,
            FunctionsMutated = budgets.FunctionsMutated,
            StrengthCodesChanged = budgets.StrengthCodesChanged
        };

        return true;
    }

    private static Dictionary<int, MutableRegion> BuildBaseChildRegions(
        IReadOnlyDictionary<int, NbnRegionSection> sectionsA,
        IReadOnlyDictionary<int, NbnRegionSection> sectionsB,
        NbnQuantizationSchema quantization,
        ReproduceConfig? config,
        ref ulong state,
        MutationBudgets budgets)
    {
        var chooseAProbability = ResolveSelectionProbability(
            config?.ProbChooseParentA ?? 0f,
            config?.ProbChooseParentB ?? 0f);
        var regions = new Dictionary<int, MutableRegion>(sectionsA.Count);

        for (var regionId = 0; regionId < NbnConstants.RegionCount; regionId++)
        {
            if (!sectionsA.TryGetValue(regionId, out var sectionA) || !sectionsB.TryGetValue(regionId, out var sectionB))
            {
                continue;
            }

            var startsA = BuildAxonStarts(sectionA);
            var startsB = BuildAxonStarts(sectionB);
            var neuronCount = Math.Min(sectionA.NeuronRecords.Length, sectionB.NeuronRecords.Length);
            var mutableNeurons = new List<MutableNeuron>(neuronCount);

            for (var neuronId = 0; neuronId < neuronCount; neuronId++)
            {
                var neuronA = sectionA.NeuronRecords[neuronId];
                var neuronB = sectionB.NeuronRecords[neuronId];
                var chooseA = ChooseParentA(chooseAProbability, ref state);
                var sourceAxons = chooseA
                    ? ReadNeuronAxons(sectionA, startsA, neuronId)
                    : ReadNeuronAxons(sectionB, startsB, neuronId);

                var template = BuildNeuronTemplate(neuronA, neuronB, chooseA, config, ref state);
                if (NeuronFunctionsDiffer(neuronA, template))
                {
                    budgets.FunctionsMutated++;
                }

                if (config is not null && config.StrengthTransformEnabled && sourceAxons.Count > 0)
                {
                    var axonsA = ReadNeuronAxons(sectionA, startsA, neuronId);
                    var axonsB = ReadNeuronAxons(sectionB, startsB, neuronId);
                    ApplyStrengthTransforms(
                        sourceAxons,
                        axonsA,
                        axonsB,
                        chooseA,
                        config,
                        quantization.Strength,
                        ref state,
                        budgets);
                }

                mutableNeurons.Add(new MutableNeuron(template, template.Exists, sourceAxons));
            }

            regions[regionId] = new MutableRegion(regionId, mutableNeurons);
        }

        return regions;
    }

    private static List<AxonRecord> ReadNeuronAxons(NbnRegionSection section, IReadOnlyList<int> starts, int neuronId)
    {
        var neuron = section.NeuronRecords[neuronId];
        var start = starts[neuronId];
        var axons = new List<AxonRecord>(neuron.AxonCount);
        for (var offset = 0; offset < neuron.AxonCount; offset++)
        {
            axons.Add(section.AxonRecords[start + offset]);
        }

        return axons;
    }

    private static NeuronRecord BuildNeuronTemplate(
        NeuronRecord neuronA,
        NeuronRecord neuronB,
        bool chooseAByFallback,
        ReproduceConfig? config,
        ref ulong state)
    {
        var exists = chooseAByFallback ? neuronA.Exists : neuronB.Exists;
        var paramBCode = SelectValueCode(neuronA.ParamBCode, neuronB.ParamBCode, chooseAByFallback, config, bits: 6, ref state);
        var paramACode = SelectValueCode(neuronA.ParamACode, neuronB.ParamACode, chooseAByFallback, config, bits: 6, ref state);
        var activationThresholdCode = SelectValueCode(
            neuronA.ActivationThresholdCode,
            neuronB.ActivationThresholdCode,
            chooseAByFallback,
            config,
            bits: 6,
            ref state);
        var preActivationThresholdCode = SelectValueCode(
            neuronA.PreActivationThresholdCode,
            neuronB.PreActivationThresholdCode,
            chooseAByFallback,
            config,
            bits: 6,
            ref state);

        var activationFunctionId = SelectFunctionCode(
            neuronA.ActivationFunctionId,
            neuronB.ActivationFunctionId,
            chooseAByFallback,
            config,
            bits: 6,
            ref state,
            PreferredActivationFunctionIds,
            PreferredActivationMutationBias);
        var resetFunctionId = SelectFunctionCode(
            neuronA.ResetFunctionId,
            neuronB.ResetFunctionId,
            chooseAByFallback,
            config,
            bits: 6,
            ref state,
            PreferredResetFunctionIds,
            PreferredResetMutationBias);
        var accumulationFunctionId = SelectFunctionCode(
            neuronA.AccumulationFunctionId,
            neuronB.AccumulationFunctionId,
            chooseAByFallback,
            config,
            bits: 2,
            ref state,
            PreferredAccumulationFunctionIds,
            PreferredAccumulationMutationBias);

        return new NeuronRecord(
            axonCount: 0,
            paramBCode: paramBCode,
            paramACode: paramACode,
            activationThresholdCode: activationThresholdCode,
            preActivationThresholdCode: preActivationThresholdCode,
            resetFunctionId: resetFunctionId,
            activationFunctionId: activationFunctionId,
            accumulationFunctionId: accumulationFunctionId,
            exists: exists);
    }

    private static byte SelectValueCode(
        byte codeA,
        byte codeB,
        bool chooseAByFallback,
        ReproduceConfig? config,
        int bits,
        ref ulong state)
    {
        var maxCode = QuantizationMap.MaxCode(bits);
        var mode = SelectValueMode(config, chooseAByFallback, ref state);
        var selected = mode switch
        {
            ValueSelectionMode.ParentA => codeA,
            ValueSelectionMode.ParentB => codeB,
            ValueSelectionMode.Average => (byte)Math.Clamp((int)MathF.Round((codeA + codeB) * 0.5f), 0, maxCode),
            ValueSelectionMode.Mutate => (byte)MutateCode(chooseAByFallback ? codeA : codeB, bits, ref state),
            _ => chooseAByFallback ? codeA : codeB
        };

        return (byte)Math.Clamp(selected, 0, maxCode);
    }

    private static ValueSelectionMode SelectValueMode(ReproduceConfig? config, bool chooseAByFallback, ref ulong state)
    {
        var chooseAWeight = ClampProbability(config?.ProbChooseParentA ?? 0f);
        var chooseBWeight = ClampProbability(config?.ProbChooseParentB ?? 0f);
        var averageWeight = ClampProbability(config?.ProbAverage ?? 0f);
        var mutateWeight = ClampProbability(config?.ProbMutate ?? 0f);
        var total = chooseAWeight + chooseBWeight + averageWeight + mutateWeight;
        if (total <= 0f)
        {
            return chooseAByFallback ? ValueSelectionMode.ParentA : ValueSelectionMode.ParentB;
        }

        var draw = NextUnitFloat(ref state) * total;
        if (draw < chooseAWeight)
        {
            return ValueSelectionMode.ParentA;
        }

        draw -= chooseAWeight;
        if (draw < chooseBWeight)
        {
            return ValueSelectionMode.ParentB;
        }

        draw -= chooseBWeight;
        if (draw < averageWeight)
        {
            return ValueSelectionMode.Average;
        }

        return ValueSelectionMode.Mutate;
    }

    private static byte SelectFunctionCode(
        byte functionA,
        byte functionB,
        bool chooseAByFallback,
        ReproduceConfig? config,
        int bits,
        ref ulong state,
        IReadOnlyList<byte>? preferredCodes = null,
        float preferredBias = 0f)
    {
        var maxCode = QuantizationMap.MaxCode(bits);
        var chooseAProbability = ClampProbability(config?.ProbChooseFuncA ?? 0f);
        var mutateProbability = ClampProbability(config?.ProbMutateFunc ?? 0f);
        var selected = chooseAProbability <= 0f && mutateProbability <= 0f
            ? chooseAByFallback ? functionA : functionB
            : (ChooseParentA(chooseAProbability, ref state) ? functionA : functionB);

        if (ShouldMutate(mutateProbability, ref state))
        {
            selected = (byte)MutateFunctionCode(selected, bits, preferredCodes, preferredBias, ref state);
        }

        return (byte)Math.Clamp(selected, 0, maxCode);
    }

    private static int MutateFunctionCode(
        int code,
        int bits,
        IReadOnlyList<byte>? preferredCodes,
        float preferredBias,
        ref ulong state)
    {
        var maxCode = QuantizationMap.MaxCode(bits);
        if (preferredCodes is not null
            && preferredCodes.Count > 0
            && NextUnitFloat(ref state) < ClampProbability(preferredBias))
        {
            var preferred = preferredCodes[(int)(NextRandom(ref state) % (ulong)preferredCodes.Count)];
            return Math.Clamp((int)preferred, 0, maxCode);
        }

        return MutateCode(code, bits, ref state);
    }

    private static int MutateCode(int code, int bits, ref ulong state)
    {
        var maxCode = QuantizationMap.MaxCode(bits);
        if (maxCode <= 0)
        {
            return 0;
        }

        var direction = (NextRandom(ref state) & 1UL) == 0UL ? -1 : 1;
        var magnitude = maxCode >= 2 ? 1 + (int)(NextRandom(ref state) % 2UL) : 1;
        var candidate = code + (direction * magnitude);
        if (candidate < 0 || candidate > maxCode)
        {
            candidate = code - (direction * magnitude);
        }

        candidate = Math.Clamp(candidate, 0, maxCode);
        if (candidate == code)
        {
            candidate = code >= maxCode ? code - 1 : code + 1;
        }

        return Math.Clamp(candidate, 0, maxCode);
    }

    private static void ApplyStrengthTransforms(
        List<AxonRecord> selectedAxons,
        IReadOnlyList<AxonRecord> axonsA,
        IReadOnlyList<AxonRecord> axonsB,
        bool selectedFromA,
        ReproduceConfig config,
        QuantizationMap strengthMap,
        ref ulong state,
        MutationBudgets budgets)
    {
        var strengthsA = BuildStrengthMap(axonsA);
        var strengthsB = BuildStrengthMap(axonsB);
        for (var i = 0; i < selectedAxons.Count; i++)
        {
            var current = selectedAxons[i];
            var key = BuildTargetKey(current.TargetRegionId, current.TargetNeuronId);
            var hasA = strengthsA.TryGetValue(key, out var strengthA);
            var hasB = strengthsB.TryGetValue(key, out var strengthB);
            var transformed = ResolveTransformedStrengthCode(
                current.StrengthCode,
                selectedFromA,
                hasA,
                strengthA,
                hasB,
                strengthB,
                config,
                strengthMap,
                ref state);
            if (transformed == current.StrengthCode)
            {
                continue;
            }

            selectedAxons[i] = new AxonRecord(transformed, current.TargetNeuronId, current.TargetRegionId);
            budgets.RecordStrengthCodeChanged();
        }
    }

    private static Dictionary<uint, byte> BuildStrengthMap(IReadOnlyList<AxonRecord> axons)
    {
        var strengths = new Dictionary<uint, byte>(axons.Count);
        for (var i = 0; i < axons.Count; i++)
        {
            var axon = axons[i];
            strengths[BuildTargetKey(axon.TargetRegionId, axon.TargetNeuronId)] = axon.StrengthCode;
        }

        return strengths;
    }

    private static byte ResolveTransformedStrengthCode(
        byte selectedStrengthCode,
        bool selectedFromA,
        bool hasA,
        byte strengthA,
        bool hasB,
        byte strengthB,
        ReproduceConfig config,
        QuantizationMap strengthMap,
        ref ulong state)
    {
        var chooseAWeight = ClampProbability(config.ProbStrengthChooseA);
        var chooseBWeight = ClampProbability(config.ProbStrengthChooseB);
        var averageWeight = ClampProbability(config.ProbStrengthAverage);
        var weightedAverageWeight = ClampProbability(config.ProbStrengthWeightedAverage);
        var mutateWeight = ClampProbability(config.ProbStrengthMutate);
        var total = chooseAWeight + chooseBWeight + averageWeight + weightedAverageWeight + mutateWeight;
        if (total <= 0f)
        {
            return selectedStrengthCode;
        }

        var draw = NextUnitFloat(ref state) * total;
        if (draw < chooseAWeight)
        {
            return hasA ? strengthA : selectedStrengthCode;
        }

        draw -= chooseAWeight;
        if (draw < chooseBWeight)
        {
            return hasB ? strengthB : selectedStrengthCode;
        }

        draw -= chooseBWeight;
        if (draw < averageWeight)
        {
            if (!hasA || !hasB)
            {
                return selectedStrengthCode;
            }

            var avg = (strengthMap.Decode(strengthA, bits: 5) + strengthMap.Decode(strengthB, bits: 5)) * 0.5f;
            return EncodeStrengthCode(avg, strengthMap);
        }

        draw -= averageWeight;
        if (draw < weightedAverageWeight)
        {
            if (!hasA || !hasB)
            {
                return selectedStrengthCode;
            }

            var weightA = Math.Max(config.StrengthWeightA, 0f);
            var weightB = Math.Max(config.StrengthWeightB, 0f);
            if (weightA <= 0f && weightB <= 0f)
            {
                weightA = 1f;
                weightB = 1f;
            }

            var denominator = weightA + weightB;
            var valueA = strengthMap.Decode(strengthA, bits: 5);
            var valueB = strengthMap.Decode(strengthB, bits: 5);
            var blended = ((valueA * weightA) + (valueB * weightB)) / denominator;
            return EncodeStrengthCode(blended, strengthMap);
        }

        var mutateBase = selectedFromA
            ? hasA ? strengthA : selectedStrengthCode
            : hasB ? strengthB : selectedStrengthCode;
        return MutateStrengthCode(mutateBase, strengthMap, ref state);
    }

    private static byte MutateStrengthCode(byte baseCode, QuantizationMap strengthMap, ref ulong state)
    {
        var baseValue = strengthMap.Decode(baseCode, bits: 5);
        var range = MathF.Max(MathF.Abs(strengthMap.Max - strengthMap.Min), 0.0001f);
        var jitter = ((NextUnitFloat(ref state) * 2f) - 1f) * (range * 0.05f);
        var mutatedValue = Math.Clamp(baseValue + jitter, strengthMap.Min, strengthMap.Max);
        return EncodeStrengthCode(mutatedValue, strengthMap);
    }

    private static byte EncodeStrengthCode(float value, QuantizationMap strengthMap)
        => (byte)Math.Clamp(strengthMap.Encode(value, bits: 5), 0, 31);

    private static MutationBudgets CreateMutationBudgets(
        IReadOnlyDictionary<int, NbnRegionSection> sectionsA,
        IReadOnlyDictionary<int, NbnRegionSection> sectionsB,
        ReproduceLimits? limits)
    {
        var averageNeuronCount = (CountExistingNeurons(sectionsA) + CountExistingNeurons(sectionsB)) / 2;
        var averageAxonCount = (CountTotalAxons(sectionsA) + CountTotalAxons(sectionsB)) / 2;

        return new MutationBudgets(
            ResolveMutationLimit(limits?.MaxNeuronsAddedAbs ?? 0, limits?.MaxNeuronsAddedPct ?? 0f, averageNeuronCount),
            ResolveMutationLimit(limits?.MaxNeuronsRemovedAbs ?? 0, limits?.MaxNeuronsRemovedPct ?? 0f, averageNeuronCount),
            ResolveMutationLimit(limits?.MaxAxonsAddedAbs ?? 0, limits?.MaxAxonsAddedPct ?? 0f, averageAxonCount),
            ResolveMutationLimit(limits?.MaxAxonsRemovedAbs ?? 0, limits?.MaxAxonsRemovedPct ?? 0f, averageAxonCount),
            ResolveRegionLimit(limits?.MaxRegionsAddedAbs ?? 0),
            ResolveRegionLimit(limits?.MaxRegionsRemovedAbs ?? 0));
    }

    private static int CountExistingNeurons(IReadOnlyDictionary<int, NbnRegionSection> sections)
    {
        var count = 0;
        foreach (var section in sections.Values)
        {
            for (var i = 0; i < section.NeuronRecords.Length; i++)
            {
                if (section.NeuronRecords[i].Exists)
                {
                    count++;
                }
            }
        }

        return count;
    }

    private static int CountTotalAxons(IReadOnlyDictionary<int, NbnRegionSection> sections)
    {
        var total = 0;
        foreach (var section in sections.Values)
        {
            for (var i = 0; i < section.NeuronRecords.Length; i++)
            {
                if (section.NeuronRecords[i].Exists)
                {
                    total += section.NeuronRecords[i].AxonCount;
                }
            }
        }

        return total;
    }

    private static int ResolveMutationLimit(uint absLimit, float pctLimit, int baselineCount)
    {
        var absValue = absLimit > 0 ? (int)Math.Min(absLimit, int.MaxValue) : int.MaxValue;
        var pctValue = pctLimit > 0f
            ? (int)Math.Floor(Math.Max(0f, pctLimit) * Math.Max(baselineCount, 0))
            : int.MaxValue;
        return Math.Min(absValue, pctValue);
    }

    private static int ResolveRegionLimit(uint absLimit)
    {
        if (absLimit > 0)
        {
            return (int)Math.Min(absLimit, int.MaxValue);
        }

        return 0;
    }

    private static NbnHeaderV2 BuildChildHeader(
        NbnHeaderV2 parentA,
        NbnHeaderV2 parentB,
        IReadOnlyList<NbnRegionSection> sections,
        ulong seed)
    {
        var sortedSections = sections.OrderBy(static section => section.RegionId).ToList();
        var directory = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        ulong offset = NbnBinary.NbnHeaderBytes;

        for (var i = 0; i < sortedSections.Count; i++)
        {
            var section = sortedSections[i];
            directory[section.RegionId] = new NbnRegionDirectoryEntry(
                section.NeuronSpan,
                section.TotalAxons,
                offset,
                0);
            offset += (ulong)section.ByteLength;
        }

        return new NbnHeaderV2(
            magic: "NBN2",
            version: 2,
            endianness: 1,
            headerBytesPow2: 10,
            brainSeed: MixBrainSeed(parentA.BrainSeed, parentB.BrainSeed, seed),
            axonStride: parentA.AxonStride,
            flags: parentA.Flags,
            quantization: parentA.Quantization,
            regions: directory);
    }

    private static ulong MixBrainSeed(ulong parentASeed, ulong parentBSeed, ulong requestSeed)
    {
        var state = parentASeed ^ (parentBSeed << 1) ^ (requestSeed << 17) ^ 0xD1B54A32D192ED03UL;
        return NextRandom(ref state);
    }

    private static string ResolveChildStoreRoot(string? parentAStoreUri, string? parentBStoreUri)
    {
        if (!string.IsNullOrWhiteSpace(parentAStoreUri))
        {
            return ResolveArtifactRoot(parentAStoreUri);
        }

        if (!string.IsNullOrWhiteSpace(parentBStoreUri))
        {
            return ResolveArtifactRoot(parentBStoreUri);
        }

        return ResolveArtifactRoot(null);
    }

    private static string ResolveChildStoreUri(string? parentAStoreUri, string? parentBStoreUri, string resolvedRoot)
    {
        if (!string.IsNullOrWhiteSpace(parentAStoreUri))
        {
            return parentAStoreUri;
        }

        if (!string.IsNullOrWhiteSpace(parentBStoreUri))
        {
            return parentBStoreUri;
        }

        return resolvedRoot;
    }
}
