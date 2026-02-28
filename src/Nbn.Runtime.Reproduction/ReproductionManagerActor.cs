using Nbn.Proto;
using ProtoControl = Nbn.Proto.Control;
using ProtoIo = Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
using Nbn.Shared.Validation;
using Proto;

namespace Nbn.Runtime.Reproduction;

public sealed class ReproductionManagerActor : IActor
{
    private static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(15);
    private const string NbnMediaType = "application/x-nbn";
    private const string NbsMediaType = "application/x-nbs";
    private const int SpotCheckSampleCount = 32;
    private const float MinRequiredSpotOverlap = 0.35f;
    private const float MaxRequiredSpotOverlap = 0.95f;
    private const ulong DefaultSpotCheckSeed = 0x9E3779B97F4A7C15UL;
    private readonly PID? _ioGatewayPid;

    public ReproductionManagerActor(PID? ioGatewayPid = null)
    {
        _ioGatewayPid = ioGatewayPid;
    }

    public async Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case ReproduceByBrainIdsRequest message:
                context.Respond(await HandleReproduceByBrainIdsAsync(context, message).ConfigureAwait(false));
                break;
            case ReproduceByArtifactsRequest message:
                context.Respond(await HandleReproduceByArtifactsAsync(context, message).ConfigureAwait(false));
                break;
        }
    }

    private async Task<ReproduceResult> HandleReproduceByBrainIdsAsync(IContext context, ReproduceByBrainIdsRequest request)
    {
        if (request.ParentA is null || request.ParentB is null)
        {
            return CreateAbortResult("repro_missing_parent_brain_ids");
        }

        if (!request.ParentA.TryToGuid(out var parentAId) || parentAId == Guid.Empty)
        {
            return CreateAbortResult("repro_parent_a_brain_id_invalid");
        }

        if (!request.ParentB.TryToGuid(out var parentBId) || parentBId == Guid.Empty)
        {
            return CreateAbortResult("repro_parent_b_brain_id_invalid");
        }

        if (_ioGatewayPid is null)
        {
            return CreateAbortResult("repro_parent_resolution_unavailable");
        }

        var parentAResolution = await ResolveParentArtifactAsync(context, request.ParentA, "a").ConfigureAwait(false);
        if (parentAResolution.AbortReason is not null)
        {
            return CreateAbortResult(parentAResolution.AbortReason);
        }

        var parentBResolution = await ResolveParentArtifactAsync(context, request.ParentB, "b").ConfigureAwait(false);
        if (parentBResolution.AbortReason is not null)
        {
            return CreateAbortResult(parentBResolution.AbortReason);
        }

        return await HandleReproduceByArtifactsAsync(
                context,
                new ReproduceByArtifactsRequest
                {
                    ParentADef = parentAResolution.ParentDef!,
                    ParentBDef = parentBResolution.ParentDef!,
                    ParentAState = parentAResolution.ParentState,
                    ParentBState = parentBResolution.ParentState,
                    StrengthSource = request.StrengthSource,
                    Config = request.Config,
                    Seed = request.Seed
                })
            .ConfigureAwait(false);
    }

    private async Task<ResolvedParentArtifact> ResolveParentArtifactAsync(IContext context, Uuid brainId, string label)
    {
        var prefix = label == "a" ? "repro_parent_a" : "repro_parent_b";

        try
        {
            var info = await context
                .RequestAsync<ProtoIo.BrainInfo>(
                    _ioGatewayPid!,
                    new ProtoIo.BrainInfoRequest { BrainId = brainId },
                    DefaultRequestTimeout)
                .ConfigureAwait(false);

            if (info is null)
            {
                return new ResolvedParentArtifact(null, null, $"{prefix}_lookup_failed");
            }

            if (!HasArtifactRef(info.BaseDefinition))
            {
                return info.InputWidth == 0 && info.OutputWidth == 0
                    ? new ResolvedParentArtifact(null, null, $"{prefix}_brain_not_found")
                    : new ResolvedParentArtifact(null, null, $"{prefix}_base_def_missing");
            }

            return new ResolvedParentArtifact(
                info.BaseDefinition,
                HasArtifactRef(info.LastSnapshot) ? info.LastSnapshot : null,
                null);
        }
        catch
        {
            return new ResolvedParentArtifact(null, null, $"{prefix}_lookup_failed");
        }
    }

    private async Task<ReproduceResult> HandleReproduceByArtifactsAsync(IContext context, ReproduceByArtifactsRequest request)
    {
        try
        {
            if (request.ParentADef is null)
            {
                return CreateAbortResult("repro_missing_parent_a_def");
            }

            if (request.ParentBDef is null)
            {
                return CreateAbortResult("repro_missing_parent_b_def");
            }

            var parentA = await TryLoadParentAsync(request.ParentADef, "a").ConfigureAwait(false);
            if (parentA.AbortReason is not null)
            {
                return CreateAbortResult(parentA.AbortReason);
            }

            var parentB = await TryLoadParentAsync(request.ParentBDef, "b").ConfigureAwait(false);
            if (parentB.AbortReason is not null)
            {
                return CreateAbortResult(parentB.AbortReason);
            }

            var transformParentA = await ResolveTransformParentAsync(
                    parentA.Parsed!,
                    request.ParentADef,
                    request.ParentAState,
                    request.StrengthSource,
                    "a")
                .ConfigureAwait(false);

            var transformParentB = await ResolveTransformParentAsync(
                    parentB.Parsed!,
                    request.ParentBDef,
                    request.ParentBState,
                    request.StrengthSource,
                    "b")
                .ConfigureAwait(false);

            return await EvaluateSimilarityGatesAndBuildChildAsync(
                    context,
                    parentA.Parsed!,
                    parentB.Parsed!,
                    transformParentA.Parsed,
                    transformParentB.Parsed,
                    request.ParentADef,
                    request.ParentBDef,
                    request.Config,
                    request.Seed)
                .ConfigureAwait(false);
        }
        catch
        {
            return CreateAbortResult("repro_internal_error");
        }
    }

    private async Task<ReproduceResult> EvaluateSimilarityGatesAndBuildChildAsync(
        IContext context,
        ParsedParent gateParentA,
        ParsedParent gateParentB,
        ParsedParent transformParentA,
        ParsedParent transformParentB,
        ArtifactRef parentARef,
        ArtifactRef parentBRef,
        ReproduceConfig? config,
        ulong seed)
    {
        var presentA = CountPresentRegions(gateParentA.Header);
        var presentB = CountPresentRegions(gateParentB.Header);

        if (!AreFormatContractsCompatible(gateParentA.Header, gateParentB.Header))
        {
            return CreateAbortResult("repro_format_incompatible", regionsPresentA: presentA, regionsPresentB: presentB);
        }

        if (!HaveMatchingRegionPresence(gateParentA.Header, gateParentB.Header))
        {
            return CreateAbortResult("repro_region_presence_mismatch", regionsPresentA: presentA, regionsPresentB: presentB);
        }

        var sectionMapA = BuildSectionMap(gateParentA.Regions);
        var sectionMapB = BuildSectionMap(gateParentB.Regions);
        var spanTolerance = ResolveSpanTolerance(config);
        var spanScore = ComputeRegionSpanScore(gateParentA.Header, gateParentB.Header, spanTolerance, out var spanMismatch);
        if (spanMismatch)
        {
            return CreateAbortResult(
                "repro_region_span_mismatch",
                regionSpanScore: spanScore,
                similarityScore: spanScore,
                regionsPresentA: presentA,
                regionsPresentB: presentB);
        }

        var functionDistance = ComputeFunctionDistance(sectionMapA, sectionMapB);
        var functionScore = 1f - functionDistance;
        var maxFunctionDistance = ResolveDistanceThreshold(config?.MaxFunctionHistDistance ?? 0f);
        if (functionDistance > maxFunctionDistance)
        {
            return CreateAbortResult(
                "repro_function_hist_mismatch",
                regionSpanScore: spanScore,
                functionScore: functionScore,
                similarityScore: ComputeSimilarityScore(spanScore, functionScore, 0f, 0f),
                regionsPresentA: presentA,
                regionsPresentB: presentB);
        }

        var connectivityDistance = ComputeConnectivityDistance(sectionMapA, sectionMapB);
        var connectivityScore = 1f - connectivityDistance;
        var maxConnectivityDistance = ResolveDistanceThreshold(config?.MaxConnectivityHistDistance ?? 0f);
        if (connectivityDistance > maxConnectivityDistance)
        {
            return CreateAbortResult(
                "repro_connectivity_hist_mismatch",
                regionSpanScore: spanScore,
                functionScore: functionScore,
                connectivityScore: connectivityScore,
                similarityScore: ComputeSimilarityScore(spanScore, functionScore, connectivityScore, 0f),
                regionsPresentA: presentA,
                regionsPresentB: presentB);
        }

        var spotCheckOverlap = ComputeSpotCheckOverlap(sectionMapA, sectionMapB, seed);
        var requiredSpotCheckOverlap = ResolveRequiredSpotOverlap(maxConnectivityDistance);
        var similarityScore = ComputeSimilarityScore(spanScore, functionScore, connectivityScore, spotCheckOverlap);
        if (spotCheckOverlap < requiredSpotCheckOverlap)
        {
            return CreateAbortResult(
                "repro_spot_check_overlap_mismatch",
                regionSpanScore: spanScore,
                functionScore: functionScore,
                connectivityScore: connectivityScore,
                similarityScore: similarityScore,
                regionsPresentA: presentA,
                regionsPresentB: presentB);
        }

        var childBuild = await BuildAndStoreChildDefinitionAsync(
                transformParentA,
                transformParentB,
                parentARef,
                parentBRef,
                config,
                seed)
            .ConfigureAwait(false);

        if (childBuild.AbortReason is not null)
        {
            return CreateAbortResult(
                childBuild.AbortReason,
                regionSpanScore: spanScore,
                functionScore: functionScore,
                connectivityScore: connectivityScore,
                similarityScore: similarityScore,
                regionsPresentA: presentA,
                regionsPresentB: presentB);
        }

        var result = CreateSuccessResult(
            childBuild.ChildDef!,
            childBuild.Summary ?? new MutationSummary(),
            spanScore,
            functionScore,
            connectivityScore,
            similarityScore,
            presentA,
            presentB,
            childBuild.RegionsPresentChild);
        return await ApplySpawnPolicyAsync(context, result, config).ConfigureAwait(false);
    }

    private static async Task<ChildBuildResult> BuildAndStoreChildDefinitionAsync(
        ParsedParent parentA,
        ParsedParent parentB,
        ArtifactRef parentARef,
        ArtifactRef parentBRef,
        ReproduceConfig? config,
        ulong seed)
    {
        var childSections = BuildChildSections(parentA, parentB, config, seed, out var summary);
        var childHeader = BuildChildHeader(parentA.Header, parentB.Header, childSections, seed);
        var validation = NbnBinaryValidator.ValidateNbn(childHeader, childSections);
        if (!validation.IsValid)
        {
            return new ChildBuildResult(null, null, "repro_child_validation_failed");
        }

        var bytes = NbnBinary.WriteNbn(childHeader, childSections);
        var storeRoot = ResolveChildStoreRoot(parentARef.StoreUri, parentBRef.StoreUri);
        var storeUri = ResolveChildStoreUri(parentARef.StoreUri, parentBRef.StoreUri, storeRoot);
        var store = new LocalArtifactStore(new ArtifactStoreOptions(storeRoot));
        await using var stream = new MemoryStream(bytes, writable: false);
        var manifest = await store.StoreAsync(stream, NbnMediaType).ConfigureAwait(false);

        var childRef = manifest.ArtifactId.Bytes.ToArray().ToArtifactRef(
            (ulong)manifest.ByteLength,
            NbnMediaType,
            storeUri);
        return new ChildBuildResult(childRef, summary, null, CountPresentRegions(childHeader));
    }

    private static List<NbnRegionSection> BuildChildSections(
        ParsedParent parentA,
        ParsedParent parentB,
        ReproduceConfig? config,
        ulong seed,
        out MutationSummary summary)
    {
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
        var childSections = BuildSectionsFromMutableRegions(mutableRegions, parentA.Header.AxonStride, budgets);

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

        return childSections;
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
            ref state);
        var resetFunctionId = SelectFunctionCode(
            neuronA.ResetFunctionId,
            neuronB.ResetFunctionId,
            chooseAByFallback,
            config,
            bits: 6,
            ref state);
        var accumulationFunctionId = SelectFunctionCode(
            neuronA.AccumulationFunctionId,
            neuronB.AccumulationFunctionId,
            chooseAByFallback,
            config,
            bits: 2,
            ref state);

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
        ref ulong state)
    {
        var maxCode = QuantizationMap.MaxCode(bits);
        var chooseAProbability = ClampProbability(config?.ProbChooseFuncA ?? 0f);
        var mutateProbability = ClampProbability(config?.ProbMutateFunc ?? 0f);
        var selected = chooseAProbability <= 0f && mutateProbability <= 0f
            ? chooseAByFallback ? functionA : functionB
            : (ChooseParentA(chooseAProbability, ref state) ? functionA : functionB);

        if (ShouldMutate(mutateProbability, ref state))
        {
            selected = (byte)MutateCode(selected, bits, ref state);
        }

        return (byte)Math.Clamp(selected, 0, maxCode);
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

    private static int ResolveAbsoluteLimit(uint absLimit)
    {
        if (absLimit > 0)
        {
            return (int)Math.Min(absLimit, int.MaxValue);
        }

        return int.MaxValue;
    }

    private static int ResolveRegionLimit(uint absLimit)
    {
        if (absLimit > 0)
        {
            return (int)Math.Min(absLimit, int.MaxValue);
        }

        return 0;
    }

    private static void ApplyStructuralMutations(
        Dictionary<int, MutableRegion> regions,
        ReproduceConfig? config,
        ref ulong state,
        MutationBudgets budgets)
    {
        if (config is null)
        {
            return;
        }

        var addNeuronToEmptyProbability = ClampProbability(config.ProbAddNeuronToEmptyRegion);
        var removeLastNeuronProbability = ClampProbability(config.ProbRemoveLastNeuronFromRegion);
        var disableNeuronProbability = ClampProbability(config.ProbDisableNeuron);
        var reactivateNeuronProbability = ClampProbability(config.ProbReactivateNeuron);
        var addAxonProbability = ClampProbability(config.ProbAddAxon);
        var removeAxonProbability = ClampProbability(config.ProbRemoveAxon);
        var rerouteAxonProbability = ClampProbability(config.ProbRerouteAxon);
        var rerouteInboundOnDeleteProbability = ClampProbability(config.ProbRerouteInboundAxonOnDelete);
        var inboundRerouteMaxRingDistance = config.InboundRerouteMaxRingDistance > 0
            ? (int)Math.Min(config.InboundRerouteMaxRingDistance, int.MaxValue)
            : 0;

        for (var regionId = 1; regionId < NbnConstants.OutputRegionId; regionId++)
        {
            if (regions.ContainsKey(regionId))
            {
                continue;
            }

            if (!budgets.CanAddNeuron || !budgets.CanAddRegion || !ShouldMutate(addNeuronToEmptyProbability, ref state))
            {
                continue;
            }

            var defaultNeuron = new NeuronRecord(
                axonCount: 0,
                paramBCode: 0,
                paramACode: 0,
                activationThresholdCode: 0,
                preActivationThresholdCode: 0,
                resetFunctionId: 0,
                activationFunctionId: 1,
                accumulationFunctionId: 0,
                exists: true);
            regions[regionId] = new MutableRegion(
                regionId,
                new List<MutableNeuron> { new(defaultNeuron, true, new List<AxonRecord>()) });
            budgets.ConsumeNeuronAdded();
            budgets.ConsumeRegionAdded();
        }

        foreach (var regionId in regions.Keys.OrderBy(static id => id).ToArray())
        {
            if (regionId <= NbnConstants.InputRegionId || regionId >= NbnConstants.OutputRegionId)
            {
                continue;
            }

            if (!regions.TryGetValue(regionId, out var region))
            {
                continue;
            }

            if (CountExistingNeurons(region) != 1 || !budgets.CanRemoveNeuron || !budgets.CanRemoveRegion)
            {
                continue;
            }

            var existingNeuronId = FindSingleExistingNeuron(region);
            if (existingNeuronId < 0)
            {
                continue;
            }

            if (!ShouldMutate(removeLastNeuronProbability, ref state))
            {
                continue;
            }

            if (!HandleNeuronDeletionSideEffects(
                    regions,
                    regionId,
                    existingNeuronId,
                    rerouteInboundOnDeleteProbability,
                    inboundRerouteMaxRingDistance,
                    ref state,
                    budgets))
            {
                continue;
            }

            budgets.ConsumeNeuronRemoved();
            budgets.ConsumeRegionRemoved();
            regions.Remove(regionId);
        }

        foreach (var regionId in regions.Keys.OrderBy(static id => id).ToArray())
        {
            if (regionId <= NbnConstants.InputRegionId || regionId >= NbnConstants.OutputRegionId)
            {
                continue;
            }

            if (!regions.TryGetValue(regionId, out var region))
            {
                continue;
            }

            var existingCount = CountExistingNeurons(region);
            for (var neuronId = 0; neuronId < region.Neurons.Count; neuronId++)
            {
                var neuron = region.Neurons[neuronId];
                if (neuron.Exists)
                {
                    if (!budgets.CanRemoveNeuron || !ShouldMutate(disableNeuronProbability, ref state))
                    {
                        continue;
                    }

                    if (existingCount == 1 && !budgets.CanRemoveRegion)
                    {
                        continue;
                    }

                    if (!HandleNeuronDeletionSideEffects(
                            regions,
                            regionId,
                            neuronId,
                            rerouteInboundOnDeleteProbability,
                            inboundRerouteMaxRingDistance,
                            ref state,
                            budgets))
                    {
                        continue;
                    }

                    if (existingCount == 1)
                    {
                        if (!budgets.CanRemoveRegion)
                        {
                            continue;
                        }

                        budgets.ConsumeNeuronRemoved();
                        budgets.ConsumeRegionRemoved();
                        regions.Remove(regionId);
                        break;
                    }

                    neuron.Exists = false;
                    neuron.Axons.Clear();
                    budgets.ConsumeNeuronRemoved();
                    existingCount--;
                    continue;
                }

                if (!budgets.CanAddNeuron || !ShouldMutate(reactivateNeuronProbability, ref state))
                {
                    continue;
                }

                neuron.Exists = true;
                budgets.ConsumeNeuronAdded();
                existingCount++;
            }
        }

        var addedConnections = new HashSet<ulong>();
        foreach (var regionId in regions.Keys.OrderBy(static id => id))
        {
            if (!regions.TryGetValue(regionId, out var region))
            {
                continue;
            }

            for (var neuronId = 0; neuronId < region.Neurons.Count; neuronId++)
            {
                var neuron = region.Neurons[neuronId];
                if (!neuron.Exists)
                {
                    neuron.Axons.Clear();
                    continue;
                }

                var existingTargets = new HashSet<uint>();
                for (var i = 0; i < neuron.Axons.Count; i++)
                {
                    existingTargets.Add(BuildTargetKey(neuron.Axons[i].TargetRegionId, neuron.Axons[i].TargetNeuronId));
                }

                for (var axonIndex = neuron.Axons.Count - 1; axonIndex >= 0; axonIndex--)
                {
                    if (!budgets.CanRemoveAxon || !ShouldMutate(removeAxonProbability, ref state))
                    {
                        continue;
                    }

                    var axon = neuron.Axons[axonIndex];
                    neuron.Axons.RemoveAt(axonIndex);
                    existingTargets.Remove(BuildTargetKey(axon.TargetRegionId, axon.TargetNeuronId));
                    budgets.ConsumeAxonRemoved();
                }

                for (var axonIndex = 0; axonIndex < neuron.Axons.Count; axonIndex++)
                {
                    if (!ShouldMutate(rerouteAxonProbability, ref state))
                    {
                        continue;
                    }

                    var current = neuron.Axons[axonIndex];
                    var currentTarget = BuildTargetKey(current.TargetRegionId, current.TargetNeuronId);
                    if (!TrySelectTarget(
                            regions,
                            regionId,
                            existingTargets,
                            currentTarget,
                            ref state,
                            out var newTarget))
                    {
                        continue;
                    }

                    neuron.Axons[axonIndex] = new AxonRecord(current.StrengthCode, newTarget.NeuronId, (byte)newTarget.RegionId);
                    existingTargets.Remove(currentTarget);
                    existingTargets.Add(BuildTargetKey(newTarget.RegionId, newTarget.NeuronId));
                    budgets.AxonsRerouted++;
                }

                if (budgets.CanAddAxon
                    && neuron.Axons.Count < NbnConstants.MaxAxonsPerNeuron
                    && ShouldMutate(addAxonProbability, ref state)
                    && TrySelectTarget(regions, regionId, existingTargets, null, ref state, out var addedTarget))
                {
                    var strengthCode = (byte)(NextRandom(ref state) & 0x1F);
                    neuron.Axons.Add(new AxonRecord(strengthCode, addedTarget.NeuronId, (byte)addedTarget.RegionId));
                    existingTargets.Add(BuildTargetKey(addedTarget.RegionId, addedTarget.NeuronId));
                    budgets.ConsumeAxonAdded();
                    addedConnections.Add(BuildConnectionKey(regionId, neuronId, addedTarget.RegionId, addedTarget.NeuronId));
                }
            }
        }

        EnforceAverageOutDegree(regions, config, ref state, budgets, addedConnections);
    }

    private static void EnforceAverageOutDegree(
        Dictionary<int, MutableRegion> regions,
        ReproduceConfig config,
        ref ulong state,
        MutationBudgets budgets,
        HashSet<ulong> addedConnections)
    {
        var normalizedGlobalCap = NormalizeOutDegreeCap(config.MaxAvgOutDegreeBrain);
        var perRegionCaps = ResolvePerRegionOutDegreeCaps(config);
        if (!normalizedGlobalCap.HasValue && perRegionCaps.Count == 0)
        {
            return;
        }

        if (normalizedGlobalCap.HasValue)
        {
            EnforceAverageOutDegreeLimit(
                regions,
                normalizedGlobalCap.Value,
                sourceRegionIdFilter: null,
                config.PrunePolicy,
                ref state,
                budgets,
                addedConnections);
        }

        if (perRegionCaps.Count == 0)
        {
            return;
        }

        foreach (var regionCap in perRegionCaps.OrderBy(static pair => pair.Key))
        {
            EnforceAverageOutDegreeLimit(
                regions,
                regionCap.Value,
                regionCap.Key,
                config.PrunePolicy,
                ref state,
                budgets,
                addedConnections);
        }
    }

    private static void EnforceAverageOutDegreeLimit(
        Dictionary<int, MutableRegion> regions,
        float maxAvgOutDegree,
        int? sourceRegionIdFilter,
        PrunePolicy prunePolicy,
        ref ulong state,
        MutationBudgets budgets,
        HashSet<ulong> addedConnections)
    {
        if (!TryGetOutDegreeTotals(regions, sourceRegionIdFilter, out var existingNeurons, out var totalAxons))
        {
            return;
        }

        var allowedAxons = ResolveAllowedAxons(maxAvgOutDegree, existingNeurons);
        while (totalAxons > allowedAxons)
        {
            var candidates = CollectPruneCandidates(regions, addedConnections, sourceRegionIdFilter);
            if (candidates.Count == 0)
            {
                break;
            }

            var selected = SelectPruneCandidate(candidates, prunePolicy, ref state);
            if (!regions.TryGetValue(selected.SourceRegionId, out var region))
            {
                break;
            }

            var neuron = region.Neurons[selected.SourceNeuronId];
            if (selected.AxonIndex < 0 || selected.AxonIndex >= neuron.Axons.Count)
            {
                break;
            }

            neuron.Axons.RemoveAt(selected.AxonIndex);
            totalAxons--;
            budgets.ConsumeAxonRemoved();
        }
    }

    private static int ResolveAllowedAxons(float maxAvgOutDegree, int existingNeurons)
    {
        var allowedAxons = (int)Math.Floor(maxAvgOutDegree * existingNeurons);
        if (allowedAxons < 0)
        {
            allowedAxons = 0;
        }

        return allowedAxons;
    }

    private static bool TryGetOutDegreeTotals(
        IReadOnlyDictionary<int, MutableRegion> regions,
        int? sourceRegionIdFilter,
        out int existingNeurons,
        out int totalAxons)
    {
        existingNeurons = 0;
        totalAxons = 0;
        foreach (var pair in regions)
        {
            if (sourceRegionIdFilter.HasValue && pair.Key != sourceRegionIdFilter.Value)
            {
                continue;
            }

            var region = pair.Value;
            for (var neuronId = 0; neuronId < region.Neurons.Count; neuronId++)
            {
                var neuron = region.Neurons[neuronId];
                if (!neuron.Exists)
                {
                    continue;
                }

                existingNeurons++;
                totalAxons += neuron.Axons.Count;
            }
        }

        return existingNeurons > 0;
    }

    private static float? NormalizeOutDegreeCap(float cap)
    {
        if (cap <= 0f || float.IsNaN(cap) || float.IsInfinity(cap))
        {
            return null;
        }

        return cap;
    }

    private static Dictionary<int, float> ResolvePerRegionOutDegreeCaps(ReproduceConfig config)
    {
        var caps = new Dictionary<int, float>();
        foreach (var cap in config.PerRegionOutDegreeCaps)
        {
            if (cap.RegionId > NbnConstants.RegionMaxId)
            {
                continue;
            }

            var normalized = NormalizeOutDegreeCap(cap.MaxAvgOutDegree);
            if (!normalized.HasValue)
            {
                continue;
            }

            var regionId = (int)cap.RegionId;
            if (caps.TryGetValue(regionId, out var existing))
            {
                caps[regionId] = Math.Min(existing, normalized.Value);
                continue;
            }

            caps.Add(regionId, normalized.Value);
        }

        return caps;
    }

    private static List<PruneCandidate> CollectPruneCandidates(
        Dictionary<int, MutableRegion> regions,
        HashSet<ulong> addedConnections,
        int? sourceRegionIdFilter = null)
    {
        var candidates = new List<PruneCandidate>();
        foreach (var pair in regions)
        {
            var regionId = pair.Key;
            if (sourceRegionIdFilter.HasValue && regionId != sourceRegionIdFilter.Value)
            {
                continue;
            }

            var region = pair.Value;
            for (var neuronId = 0; neuronId < region.Neurons.Count; neuronId++)
            {
                var neuron = region.Neurons[neuronId];
                if (!neuron.Exists)
                {
                    continue;
                }

                for (var axonIndex = 0; axonIndex < neuron.Axons.Count; axonIndex++)
                {
                    var axon = neuron.Axons[axonIndex];
                    var key = BuildConnectionKey(regionId, neuronId, axon.TargetRegionId, axon.TargetNeuronId);
                    candidates.Add(new PruneCandidate(
                        regionId,
                        neuronId,
                        axonIndex,
                        MathF.Abs(axon.StrengthCode - 15.5f),
                        addedConnections.Contains(key)));
                }
            }
        }

        return candidates;
    }

    private static PruneCandidate SelectPruneCandidate(
        List<PruneCandidate> candidates,
        PrunePolicy policy,
        ref ulong state)
    {
        switch (policy)
        {
            case PrunePolicy.PruneNewConnectionsFirst:
            {
                var newest = candidates.FirstOrDefault(static candidate => candidate.IsNewConnection);
                if (newest.IsNewConnection)
                {
                    return newest;
                }

                break;
            }
            case PrunePolicy.PruneRandom:
            {
                var index = (int)(NextRandom(ref state) % (ulong)candidates.Count);
                return candidates[index];
            }
        }

        return candidates
            .OrderBy(static candidate => candidate.StrengthDistance)
            .ThenBy(static candidate => candidate.SourceRegionId)
            .ThenBy(static candidate => candidate.SourceNeuronId)
            .ThenBy(static candidate => candidate.AxonIndex)
            .First();
    }

    private static List<NbnRegionSection> BuildSectionsFromMutableRegions(
        Dictionary<int, MutableRegion> regions,
        uint stride,
        MutationBudgets budgets)
    {
        var sections = new List<NbnRegionSection>(regions.Count);
        foreach (var regionId in regions.Keys.OrderBy(static id => id))
        {
            if (!regions.TryGetValue(regionId, out var region))
            {
                continue;
            }

            if (regionId != NbnConstants.InputRegionId
                && regionId != NbnConstants.OutputRegionId
                && CountExistingNeurons(region) == 0)
            {
                continue;
            }

            var neurons = new NeuronRecord[region.Neurons.Count];
            var axons = new List<AxonRecord>();
            for (var neuronId = 0; neuronId < region.Neurons.Count; neuronId++)
            {
                var mutable = region.Neurons[neuronId];
                var exists = mutable.Exists;
                if ((regionId == NbnConstants.InputRegionId || regionId == NbnConstants.OutputRegionId) && !exists)
                {
                    exists = true;
                }

                var removedAxons = 0;
                var normalizedAxons = exists
                    ? NormalizeAxons(regionId, mutable.Axons, regions, out removedAxons)
                    : new List<AxonRecord>();
                if (removedAxons > 0)
                {
                    budgets.ConsumeAxonsRemoved(removedAxons);
                }

                if (normalizedAxons.Count > NbnConstants.MaxAxonsPerNeuron)
                {
                    var trimCount = normalizedAxons.Count - NbnConstants.MaxAxonsPerNeuron;
                    normalizedAxons.RemoveRange(NbnConstants.MaxAxonsPerNeuron, trimCount);
                    budgets.ConsumeAxonsRemoved(trimCount);
                }

                neurons[neuronId] = new NeuronRecord(
                    axonCount: (ushort)normalizedAxons.Count,
                    paramBCode: mutable.Template.ParamBCode,
                    paramACode: mutable.Template.ParamACode,
                    activationThresholdCode: mutable.Template.ActivationThresholdCode,
                    preActivationThresholdCode: mutable.Template.PreActivationThresholdCode,
                    resetFunctionId: mutable.Template.ResetFunctionId,
                    activationFunctionId: mutable.Template.ActivationFunctionId,
                    accumulationFunctionId: mutable.Template.AccumulationFunctionId,
                    exists: exists);
                axons.AddRange(normalizedAxons);
            }

            var checkpoints = NbnBinary.BuildCheckpoints(neurons, stride);
            sections.Add(new NbnRegionSection(
                (byte)regionId,
                (uint)neurons.Length,
                (ulong)axons.Count,
                stride,
                (uint)checkpoints.Length,
                checkpoints,
                neurons,
                axons.ToArray()));
        }

        return sections;
    }

    private static List<AxonRecord> NormalizeAxons(
        int sourceRegionId,
        List<AxonRecord> axons,
        IReadOnlyDictionary<int, MutableRegion> regions,
        out int removedAxons)
    {
        var normalized = new List<AxonRecord>(axons.Count);
        var seenTargets = new HashSet<uint>();
        removedAxons = 0;

        for (var i = 0; i < axons.Count; i++)
        {
            var axon = axons[i];
            if (!IsValidTarget(sourceRegionId, axon, regions))
            {
                removedAxons++;
                continue;
            }

            var targetKey = BuildTargetKey(axon.TargetRegionId, axon.TargetNeuronId);
            if (!seenTargets.Add(targetKey))
            {
                removedAxons++;
                continue;
            }

            normalized.Add(axon);
        }

        normalized.Sort(static (left, right) =>
        {
            var regionComparison = left.TargetRegionId.CompareTo(right.TargetRegionId);
            return regionComparison != 0
                ? regionComparison
                : left.TargetNeuronId.CompareTo(right.TargetNeuronId);
        });
        return normalized;
    }

    private static bool IsValidTarget(
        int sourceRegionId,
        AxonRecord axon,
        IReadOnlyDictionary<int, MutableRegion> regions)
    {
        if (!IsValidTargetRegion(sourceRegionId, axon.TargetRegionId))
        {
            return false;
        }

        if (!regions.TryGetValue(axon.TargetRegionId, out var targetRegion))
        {
            return false;
        }

        if (axon.TargetNeuronId < 0 || axon.TargetNeuronId >= targetRegion.Neurons.Count)
        {
            return false;
        }

        return targetRegion.Neurons[axon.TargetNeuronId].Exists;
    }

    private static bool IsValidTargetRegion(int sourceRegionId, int targetRegionId)
    {
        if (targetRegionId == NbnConstants.InputRegionId)
        {
            return false;
        }

        if (sourceRegionId == NbnConstants.OutputRegionId && targetRegionId == NbnConstants.OutputRegionId)
        {
            return false;
        }

        return true;
    }

    private static bool TrySelectTarget(
        IReadOnlyDictionary<int, MutableRegion> regions,
        int sourceRegionId,
        HashSet<uint> existingTargets,
        uint? excludedTarget,
        ref ulong state,
        out TargetLocus target)
    {
        var candidates = new List<TargetLocus>();
        foreach (var regionId in regions.Keys.OrderBy(static id => id))
        {
            if (!regions.TryGetValue(regionId, out var region) || !IsValidTargetRegion(sourceRegionId, regionId))
            {
                continue;
            }

            for (var neuronId = 0; neuronId < region.Neurons.Count; neuronId++)
            {
                if (!region.Neurons[neuronId].Exists)
                {
                    continue;
                }

                var targetKey = BuildTargetKey(regionId, neuronId);
                if (existingTargets.Contains(targetKey) || (excludedTarget.HasValue && excludedTarget.Value == targetKey))
                {
                    continue;
                }

                candidates.Add(new TargetLocus(regionId, neuronId));
            }
        }

        if (candidates.Count == 0)
        {
            target = default;
            return false;
        }

        var index = (int)(NextRandom(ref state) % (ulong)candidates.Count);
        target = candidates[index];
        return true;
    }

    private static bool HandleNeuronDeletionSideEffects(
        Dictionary<int, MutableRegion> regions,
        int targetRegionId,
        int targetNeuronId,
        float rerouteInboundProbability,
        int inboundRerouteMaxRingDistance,
        ref ulong state,
        MutationBudgets budgets)
    {
        if (!regions.TryGetValue(targetRegionId, out var targetRegion))
        {
            return false;
        }

        if (targetNeuronId < 0 || targetNeuronId >= targetRegion.Neurons.Count)
        {
            return false;
        }

        RerouteOrRemoveInboundAxons(
            regions,
            targetRegionId,
            targetNeuronId,
            rerouteInboundProbability,
            inboundRerouteMaxRingDistance,
            ref state,
            budgets);

        var targetNeuron = targetRegion.Neurons[targetNeuronId];
        if (targetNeuron.Axons.Count > 0)
        {
            budgets.ConsumeAxonsRemoved(targetNeuron.Axons.Count);
            targetNeuron.Axons.Clear();
        }

        return true;
    }

    private static void RerouteOrRemoveInboundAxons(
        Dictionary<int, MutableRegion> regions,
        int targetRegionId,
        int targetNeuronId,
        float rerouteInboundProbability,
        int inboundRerouteMaxRingDistance,
        ref ulong state,
        MutationBudgets budgets)
    {
        foreach (var pair in regions)
        {
            var sourceRegionId = pair.Key;
            var sourceRegion = pair.Value;

            for (var sourceNeuronId = 0; sourceNeuronId < sourceRegion.Neurons.Count; sourceNeuronId++)
            {
                if (sourceRegionId == targetRegionId && sourceNeuronId == targetNeuronId)
                {
                    continue;
                }

                var sourceNeuron = sourceRegion.Neurons[sourceNeuronId];
                if (!sourceNeuron.Exists || sourceNeuron.Axons.Count == 0)
                {
                    continue;
                }

                var existingTargets = new HashSet<uint>(sourceNeuron.Axons.Count);
                for (var i = 0; i < sourceNeuron.Axons.Count; i++)
                {
                    existingTargets.Add(BuildTargetKey(sourceNeuron.Axons[i].TargetRegionId, sourceNeuron.Axons[i].TargetNeuronId));
                }

                for (var axonIndex = sourceNeuron.Axons.Count - 1; axonIndex >= 0; axonIndex--)
                {
                    var axon = sourceNeuron.Axons[axonIndex];
                    if (axon.TargetRegionId != targetRegionId || axon.TargetNeuronId != targetNeuronId)
                    {
                        continue;
                    }

                    var currentTargetKey = BuildTargetKey(axon.TargetRegionId, axon.TargetNeuronId);
                    existingTargets.Remove(currentTargetKey);

                    if (ShouldMutate(rerouteInboundProbability, ref state)
                        && TrySelectInboundRerouteTarget(
                            regions,
                            sourceRegionId,
                            targetRegionId,
                            targetNeuronId,
                            inboundRerouteMaxRingDistance,
                            existingTargets,
                            ref state,
                            out var rerouteTarget))
                    {
                        sourceNeuron.Axons[axonIndex] = new AxonRecord(
                            axon.StrengthCode,
                            rerouteTarget.NeuronId,
                            (byte)rerouteTarget.RegionId);
                        existingTargets.Add(BuildTargetKey(rerouteTarget.RegionId, rerouteTarget.NeuronId));
                        budgets.AxonsRerouted++;
                        continue;
                    }

                    sourceNeuron.Axons.RemoveAt(axonIndex);
                    budgets.ConsumeAxonRemoved();
                }
            }
        }
    }

    private static bool TrySelectInboundRerouteTarget(
        IReadOnlyDictionary<int, MutableRegion> regions,
        int sourceRegionId,
        int targetRegionId,
        int deletedNeuronId,
        int inboundRerouteMaxRingDistance,
        HashSet<uint> existingTargets,
        ref ulong state,
        out TargetLocus target)
    {
        target = default;
        if (!IsValidTargetRegion(sourceRegionId, targetRegionId))
        {
            return false;
        }

        if (!regions.TryGetValue(targetRegionId, out var targetRegion))
        {
            return false;
        }

        var span = targetRegion.Neurons.Count;
        if (span <= 1)
        {
            return false;
        }

        var candidates = new List<WeightedTargetCandidate>(span);
        for (var neuronId = 0; neuronId < span; neuronId++)
        {
            if (neuronId == deletedNeuronId || !targetRegion.Neurons[neuronId].Exists)
            {
                continue;
            }

            var targetKey = BuildTargetKey(targetRegionId, neuronId);
            if (existingTargets.Contains(targetKey))
            {
                continue;
            }

            var delta = Math.Abs(neuronId - deletedNeuronId);
            var ringDistance = Math.Min(delta, span - delta);
            if (inboundRerouteMaxRingDistance > 0 && ringDistance > inboundRerouteMaxRingDistance)
            {
                continue;
            }

            var weight = 1f / (1f + ringDistance);
            candidates.Add(new WeightedTargetCandidate(new TargetLocus(targetRegionId, neuronId), weight));
        }

        if (candidates.Count == 0)
        {
            return false;
        }

        var totalWeight = 0f;
        for (var i = 0; i < candidates.Count; i++)
        {
            totalWeight += candidates[i].Weight;
        }

        if (totalWeight <= 0f)
        {
            return false;
        }

        var draw = NextUnitFloat(ref state) * totalWeight;
        for (var i = 0; i < candidates.Count; i++)
        {
            var candidate = candidates[i];
            if (draw <= candidate.Weight)
            {
                target = candidate.Target;
                return true;
            }

            draw -= candidate.Weight;
        }

        target = candidates[candidates.Count - 1].Target;
        return true;
    }

    private static int CountExistingNeurons(MutableRegion region)
    {
        var count = 0;
        for (var i = 0; i < region.Neurons.Count; i++)
        {
            if (region.Neurons[i].Exists)
            {
                count++;
            }
        }

        return count;
    }

    private static int FindSingleExistingNeuron(MutableRegion region)
    {
        for (var i = 0; i < region.Neurons.Count; i++)
        {
            if (region.Neurons[i].Exists)
            {
                return i;
            }
        }

        return -1;
    }

    private static bool ShouldMutate(float probability, ref ulong state)
        => probability > 0f && NextUnitFloat(ref state) < probability;

    private static float ClampProbability(float probability)
        => float.IsNaN(probability) ? 0f : Math.Clamp(probability, 0f, 1f);

    private static uint BuildTargetKey(int targetRegionId, int targetNeuronId)
        => ((uint)targetRegionId << NbnConstants.AddressNeuronBits) | (uint)targetNeuronId;

    private static ulong BuildConnectionKey(int sourceRegionId, int sourceNeuronId, int targetRegionId, int targetNeuronId)
        => ((ulong)(sourceRegionId & NbnConstants.RegionMaxId) << 59)
           | (((ulong)sourceNeuronId & 0x3FFFFFUL) << 37)
           | ((ulong)(targetRegionId & NbnConstants.RegionMaxId) << 32)
           | ((uint)targetNeuronId & 0x3FFFFFUL);

    private static bool ChooseParentA(float probability, ref ulong state)
    {
        if (probability <= 0f)
        {
            return false;
        }

        if (probability >= 1f)
        {
            return true;
        }

        return NextUnitFloat(ref state) < probability;
    }

    private static bool NeuronFunctionsDiffer(NeuronRecord left, NeuronRecord right)
        => left.ActivationFunctionId != right.ActivationFunctionId
           || left.ResetFunctionId != right.ResetFunctionId
           || left.AccumulationFunctionId != right.AccumulationFunctionId;

    private static float ResolveSelectionProbability(float chooseA, float chooseB)
    {
        var a = Math.Max(chooseA, 0f);
        var b = Math.Max(chooseB, 0f);
        if (a <= 0f && b <= 0f)
        {
            return 1f;
        }

        var total = a + b;
        if (total <= 0f)
        {
            return 1f;
        }

        return Math.Clamp(a / total, 0f, 1f);
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

    private static bool AreFormatContractsCompatible(NbnHeaderV2 parentA, NbnHeaderV2 parentB)
    {
        if (parentA.AxonStride != parentB.AxonStride)
        {
            return false;
        }

        return QuantizationMapEquals(parentA.Quantization.Strength, parentB.Quantization.Strength)
               && QuantizationMapEquals(parentA.Quantization.PreActivationThreshold, parentB.Quantization.PreActivationThreshold)
               && QuantizationMapEquals(parentA.Quantization.ActivationThreshold, parentB.Quantization.ActivationThreshold)
               && QuantizationMapEquals(parentA.Quantization.ParamA, parentB.Quantization.ParamA)
               && QuantizationMapEquals(parentA.Quantization.ParamB, parentB.Quantization.ParamB);
    }

    private static bool QuantizationMapEquals(QuantizationMap left, QuantizationMap right)
        => left.MapType == right.MapType
           && left.Min.Equals(right.Min)
           && left.Max.Equals(right.Max)
           && left.Gamma.Equals(right.Gamma);

    private static bool HaveMatchingRegionPresence(NbnHeaderV2 parentA, NbnHeaderV2 parentB)
    {
        for (var i = 0; i < NbnConstants.RegionCount; i++)
        {
            var aPresent = parentA.Regions[i].NeuronSpan > 0;
            var bPresent = parentB.Regions[i].NeuronSpan > 0;
            if (aPresent != bPresent)
            {
                return false;
            }
        }

        return true;
    }

    private static Dictionary<int, NbnRegionSection> BuildSectionMap(IReadOnlyList<NbnRegionSection> regions)
    {
        var map = new Dictionary<int, NbnRegionSection>(regions.Count);
        foreach (var region in regions)
        {
            map[region.RegionId] = region;
        }

        return map;
    }

    private static float ComputeRegionSpanScore(NbnHeaderV2 parentA, NbnHeaderV2 parentB, float tolerance, out bool spanMismatch)
    {
        spanMismatch = false;
        var totalScore = 0f;
        var compared = 0;

        for (var i = 0; i < NbnConstants.RegionCount; i++)
        {
            var spanA = parentA.Regions[i].NeuronSpan;
            var spanB = parentB.Regions[i].NeuronSpan;
            if (spanA == 0 && spanB == 0)
            {
                continue;
            }

            if (spanA == 0 || spanB == 0)
            {
                spanMismatch = true;
                continue;
            }

            var maxSpan = Math.Max(spanA, spanB);
            var diffRatio = maxSpan == 0 ? 0f : MathF.Abs(spanA - spanB) / maxSpan;
            if (diffRatio > tolerance)
            {
                spanMismatch = true;
            }

            totalScore += 1f - Math.Clamp(diffRatio, 0f, 1f);
            compared++;
        }

        if (compared == 0)
        {
            return 1f;
        }

        return Math.Clamp(totalScore / compared, 0f, 1f);
    }

    private static float ComputeFunctionDistance(
        IReadOnlyDictionary<int, NbnRegionSection> sectionsA,
        IReadOnlyDictionary<int, NbnRegionSection> sectionsB)
    {
        var totalDistance = 0f;
        var compared = 0;

        for (var regionId = 0; regionId < NbnConstants.RegionCount; regionId++)
        {
            if (!sectionsA.TryGetValue(regionId, out var sectionA) || !sectionsB.TryGetValue(regionId, out var sectionB))
            {
                continue;
            }

            totalDistance += ComputeRegionFunctionDistance(sectionA, sectionB);
            compared++;
        }

        if (compared == 0)
        {
            return 0f;
        }

        return Math.Clamp(totalDistance / compared, 0f, 1f);
    }

    private static float ComputeRegionFunctionDistance(NbnRegionSection sectionA, NbnRegionSection sectionB)
    {
        var activationA = new int[64];
        var activationB = new int[64];
        var resetA = new int[64];
        var resetB = new int[64];
        var accumA = new int[4];
        var accumB = new int[4];
        var neuronTotalA = 0;
        var neuronTotalB = 0;

        for (var i = 0; i < sectionA.NeuronRecords.Length; i++)
        {
            var neuron = sectionA.NeuronRecords[i];
            if (!neuron.Exists)
            {
                continue;
            }

            activationA[neuron.ActivationFunctionId]++;
            resetA[neuron.ResetFunctionId]++;
            accumA[neuron.AccumulationFunctionId]++;
            neuronTotalA++;
        }

        for (var i = 0; i < sectionB.NeuronRecords.Length; i++)
        {
            var neuron = sectionB.NeuronRecords[i];
            if (!neuron.Exists)
            {
                continue;
            }

            activationB[neuron.ActivationFunctionId]++;
            resetB[neuron.ResetFunctionId]++;
            accumB[neuron.AccumulationFunctionId]++;
            neuronTotalB++;
        }

        var activationDistance = ComputeNormalizedCountDistance(activationA, neuronTotalA, activationB, neuronTotalB);
        var resetDistance = ComputeNormalizedCountDistance(resetA, neuronTotalA, resetB, neuronTotalB);
        var accumDistance = ComputeNormalizedCountDistance(accumA, neuronTotalA, accumB, neuronTotalB);
        return Math.Clamp((activationDistance + resetDistance + accumDistance) / 3f, 0f, 1f);
    }

    private static float ComputeConnectivityDistance(
        IReadOnlyDictionary<int, NbnRegionSection> sectionsA,
        IReadOnlyDictionary<int, NbnRegionSection> sectionsB)
    {
        var totalDistance = 0f;
        var compared = 0;

        for (var regionId = 0; regionId < NbnConstants.RegionCount; regionId++)
        {
            if (!sectionsA.TryGetValue(regionId, out var sectionA) || !sectionsB.TryGetValue(regionId, out var sectionB))
            {
                continue;
            }

            var histA = BuildConnectivityHistogram(sectionA);
            var histB = BuildConnectivityHistogram(sectionB);
            var outDegreeDistance = ComputeNormalizedCountDistance(
                histA.OutDegreeCounts,
                histA.OutDegreeTotal,
                histB.OutDegreeCounts,
                histB.OutDegreeTotal);
            var targetRegionDistance = ComputeNormalizedCountDistance(
                histA.TargetRegionCounts,
                histA.TargetRegionTotal,
                histB.TargetRegionCounts,
                histB.TargetRegionTotal);

            totalDistance += Math.Clamp((outDegreeDistance + targetRegionDistance) / 2f, 0f, 1f);
            compared++;
        }

        if (compared == 0)
        {
            return 0f;
        }

        return Math.Clamp(totalDistance / compared, 0f, 1f);
    }

    private static ConnectivityHistogram BuildConnectivityHistogram(NbnRegionSection section)
    {
        var outDegreeCounts = new int[NbnConstants.MaxAxonsPerNeuron + 1];
        var targetRegionCounts = new int[NbnConstants.RegionCount];
        var outDegreeTotal = 0;
        var targetRegionTotal = 0;
        var axonIndex = 0;

        for (var i = 0; i < section.NeuronRecords.Length; i++)
        {
            var neuron = section.NeuronRecords[i];
            var axonCount = neuron.AxonCount;
            if (neuron.Exists)
            {
                outDegreeCounts[Math.Min((int)axonCount, NbnConstants.MaxAxonsPerNeuron)]++;
                outDegreeTotal++;

                for (var j = 0; j < axonCount; j++)
                {
                    var axon = section.AxonRecords[axonIndex + j];
                    targetRegionCounts[axon.TargetRegionId]++;
                    targetRegionTotal++;
                }
            }

            axonIndex += axonCount;
        }

        return new ConnectivityHistogram(outDegreeCounts, outDegreeTotal, targetRegionCounts, targetRegionTotal);
    }

    private static float ComputeSpotCheckOverlap(
        IReadOnlyDictionary<int, NbnRegionSection> sectionsA,
        IReadOnlyDictionary<int, NbnRegionSection> sectionsB,
        ulong seed)
    {
        var sharedLoci = BuildSharedExistingLoci(sectionsA, sectionsB);
        if (sharedLoci.Count == 0)
        {
            return 1f;
        }

        var sampleCount = Math.Min(SpotCheckSampleCount, sharedLoci.Count);
        var sampleIndices = SelectSampleIndices(sharedLoci.Count, sampleCount, seed);
        var startsA = BuildAxonStartsByRegion(sectionsA);
        var startsB = BuildAxonStartsByRegion(sectionsB);

        var totalScore = 0f;
        for (var i = 0; i < sampleIndices.Length; i++)
        {
            var locus = sharedLoci[sampleIndices[i]];
            var sectionA = sectionsA[locus.RegionId];
            var sectionB = sectionsB[locus.RegionId];
            var targetsA = ExtractTargets(sectionA, startsA[locus.RegionId], locus.NeuronId);
            var targetsB = ExtractTargets(sectionB, startsB[locus.RegionId], locus.NeuronId);
            totalScore += ComputeJaccardSimilarity(targetsA, targetsB);
        }

        return Math.Clamp(totalScore / sampleIndices.Length, 0f, 1f);
    }

    private static List<NeuronLocus> BuildSharedExistingLoci(
        IReadOnlyDictionary<int, NbnRegionSection> sectionsA,
        IReadOnlyDictionary<int, NbnRegionSection> sectionsB)
    {
        var loci = new List<NeuronLocus>();

        for (var regionId = 0; regionId < NbnConstants.RegionCount; regionId++)
        {
            if (!sectionsA.TryGetValue(regionId, out var sectionA) || !sectionsB.TryGetValue(regionId, out var sectionB))
            {
                continue;
            }

            var neuronCount = Math.Min(sectionA.NeuronRecords.Length, sectionB.NeuronRecords.Length);
            for (var neuronId = 0; neuronId < neuronCount; neuronId++)
            {
                if (sectionA.NeuronRecords[neuronId].Exists && sectionB.NeuronRecords[neuronId].Exists)
                {
                    loci.Add(new NeuronLocus(regionId, neuronId));
                }
            }
        }

        return loci;
    }

    private static int[] SelectSampleIndices(int totalCount, int sampleCount, ulong seed)
    {
        var indices = new int[totalCount];
        for (var i = 0; i < totalCount; i++)
        {
            indices[i] = i;
        }

        var state = seed == 0 ? DefaultSpotCheckSeed : seed;
        for (var i = 0; i < sampleCount; i++)
        {
            var random = NextRandom(ref state);
            var swapIndex = i + (int)(random % (ulong)(totalCount - i));
            (indices[i], indices[swapIndex]) = (indices[swapIndex], indices[i]);
        }

        var selected = new int[sampleCount];
        Array.Copy(indices, selected, sampleCount);
        return selected;
    }

    private static ulong NextRandom(ref ulong state)
    {
        state += 0x9E3779B97F4A7C15UL;
        var z = state;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }

    private static float NextUnitFloat(ref ulong state)
        => (float)(NextRandom(ref state) / (double)ulong.MaxValue);

    private static Dictionary<int, int[]> BuildAxonStartsByRegion(IReadOnlyDictionary<int, NbnRegionSection> sections)
    {
        var startsByRegion = new Dictionary<int, int[]>(sections.Count);
        foreach (var pair in sections)
        {
            startsByRegion[pair.Key] = BuildAxonStarts(pair.Value);
        }

        return startsByRegion;
    }

    private static int[] BuildAxonStarts(NbnRegionSection section)
    {
        var starts = new int[section.NeuronRecords.Length];
        var cursor = 0;
        for (var i = 0; i < section.NeuronRecords.Length; i++)
        {
            starts[i] = cursor;
            cursor += section.NeuronRecords[i].AxonCount;
        }

        return starts;
    }

    private static HashSet<uint> ExtractTargets(NbnRegionSection section, int[] starts, int neuronId)
    {
        var targets = new HashSet<uint>();
        var neuron = section.NeuronRecords[neuronId];
        var start = starts[neuronId];
        for (var i = 0; i < neuron.AxonCount; i++)
        {
            var axon = section.AxonRecords[start + i];
            var target = ((uint)axon.TargetRegionId << NbnConstants.AddressNeuronBits) | (uint)axon.TargetNeuronId;
            targets.Add(target);
        }

        return targets;
    }

    private static float ComputeJaccardSimilarity(HashSet<uint> left, HashSet<uint> right)
    {
        if (left.Count == 0 && right.Count == 0)
        {
            return 1f;
        }

        if (left.Count == 0 || right.Count == 0)
        {
            return 0f;
        }

        var intersection = 0;
        if (left.Count <= right.Count)
        {
            foreach (var item in left)
            {
                if (right.Contains(item))
                {
                    intersection++;
                }
            }
        }
        else
        {
            foreach (var item in right)
            {
                if (left.Contains(item))
                {
                    intersection++;
                }
            }
        }

        var union = left.Count + right.Count - intersection;
        return union == 0 ? 1f : Math.Clamp(intersection / (float)union, 0f, 1f);
    }

    private static float ResolveSpanTolerance(ReproduceConfig? config)
        => Math.Clamp(config?.MaxRegionSpanDiffRatio ?? 0f, 0f, 1f);

    private static float ResolveDistanceThreshold(float configuredThreshold)
        => Math.Clamp(configuredThreshold, 0f, 1f);

    private static float ResolveRequiredSpotOverlap(float maxConnectivityDistance)
        => Math.Clamp(1f - maxConnectivityDistance, MinRequiredSpotOverlap, MaxRequiredSpotOverlap);

    private static float ComputeSimilarityScore(
        float regionSpanScore,
        float functionScore,
        float connectivityScore,
        float spotCheckOverlap)
    {
        var total = Math.Clamp(regionSpanScore, 0f, 1f)
                    + Math.Clamp(functionScore, 0f, 1f)
                    + Math.Clamp(connectivityScore, 0f, 1f)
                    + Math.Clamp(spotCheckOverlap, 0f, 1f);
        return Math.Clamp(total / 4f, 0f, 1f);
    }

    private static float ComputeNormalizedCountDistance(int[] leftCounts, int leftTotal, int[] rightCounts, int rightTotal)
    {
        if (leftTotal <= 0 && rightTotal <= 0)
        {
            return 0f;
        }

        if (leftTotal <= 0 || rightTotal <= 0)
        {
            return 1f;
        }

        var leftScale = 1f / leftTotal;
        var rightScale = 1f / rightTotal;
        var distance = 0f;
        for (var i = 0; i < leftCounts.Length; i++)
        {
            var leftValue = leftCounts[i] * leftScale;
            var rightValue = rightCounts[i] * rightScale;
            distance += MathF.Abs(leftValue - rightValue);
        }

        return Math.Clamp(distance * 0.5f, 0f, 1f);
    }

    private static int CountPresentRegions(NbnHeaderV2 header)
    {
        var count = 0;
        for (var i = 0; i < header.Regions.Length; i++)
        {
            if (header.Regions[i].NeuronSpan > 0)
            {
                count++;
            }
        }

        return count;
    }

    private static async Task<LoadParentResult> TryLoadParentAsync(ArtifactRef reference, string label)
    {
        var prefix = label == "a" ? "repro_parent_a" : "repro_parent_b";
        if (!string.IsNullOrWhiteSpace(reference.MediaType) && !IsNbnMediaType(reference.MediaType))
        {
            return new LoadParentResult(null, $"{prefix}_media_type_invalid");
        }

        if (!reference.TryToSha256Bytes(out var hashBytes) || hashBytes.Length != Sha256Hash.Length)
        {
            return new LoadParentResult(null, $"{prefix}_sha256_invalid");
        }

        var hash = new Sha256Hash(hashBytes);
        var storeRoot = ResolveArtifactRoot(reference.StoreUri);
        var store = new LocalArtifactStore(new ArtifactStoreOptions(storeRoot));
        var manifest = await store.TryGetManifestAsync(hash).ConfigureAwait(false);
        if (manifest is null)
        {
            return new LoadParentResult(null, $"{prefix}_artifact_not_found");
        }

        if (!IsNbnMediaType(manifest.MediaType))
        {
            return new LoadParentResult(null, $"{prefix}_media_type_invalid");
        }

        byte[] bytes;
        await using (var stream = store.OpenArtifactStream(manifest))
        {
            bytes = await ReadAllBytesAsync(stream, reference.SizeBytes).ConfigureAwait(false);
        }

        NbnHeaderV2 header;
        List<NbnRegionSection> sections;
        try
        {
            header = NbnBinary.ReadNbnHeader(bytes);
            sections = ReadRegions(bytes, header);
        }
        catch
        {
            return new LoadParentResult(null, $"{prefix}_parse_failed");
        }

        var validation = NbnBinaryValidator.ValidateNbn(header, sections);
        if (!validation.IsValid)
        {
            return new LoadParentResult(null, MapValidationAbortReason(validation, prefix));
        }

        return new LoadParentResult(new ParsedParent(header, sections), null);
    }

    private static async Task<TransformParentResult> ResolveTransformParentAsync(
        ParsedParent baseParent,
        ArtifactRef parentDef,
        ArtifactRef? parentState,
        StrengthSource strengthSource,
        string label)
    {
        if (strengthSource != StrengthSource.StrengthLiveCodes)
        {
            return new TransformParentResult(baseParent);
        }

        if (!HasArtifactRef(parentState))
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_ref_missing");
            return new TransformParentResult(baseParent);
        }

        if (!string.IsNullOrWhiteSpace(parentState!.MediaType) && !IsNbsMediaType(parentState.MediaType))
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_media_type_invalid");
            return new TransformParentResult(baseParent);
        }

        if (!parentState.TryToSha256Bytes(out var stateHashBytes) || stateHashBytes.Length != Sha256Hash.Length)
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_sha256_invalid");
            return new TransformParentResult(baseParent);
        }

        var stateHash = new Sha256Hash(stateHashBytes);
        var stateRoot = ResolveArtifactRoot(parentState.StoreUri ?? parentDef.StoreUri);
        var store = new LocalArtifactStore(new ArtifactStoreOptions(stateRoot));
        var manifest = await store.TryGetManifestAsync(stateHash).ConfigureAwait(false);
        if (manifest is null)
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_artifact_not_found");
            return new TransformParentResult(baseParent);
        }

        if (!IsNbsMediaType(manifest.MediaType))
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_media_type_invalid");
            return new TransformParentResult(baseParent);
        }

        byte[] stateBytes;
        await using (var stream = store.OpenArtifactStream(manifest))
        {
            stateBytes = await ReadAllBytesAsync(stream, parentState.SizeBytes).ConfigureAwait(false);
        }

        NbsHeaderV2 stateHeader;
        List<NbsRegionSection> stateRegions;
        NbsOverlaySection? overlaySection;
        try
        {
            stateHeader = NbnBinary.ReadNbsHeader(stateBytes);
            stateRegions = ReadNbsRegions(stateBytes, baseParent.Header, stateHeader, out overlaySection);
        }
        catch
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_parse_failed");
            return new TransformParentResult(baseParent);
        }

        var stateValidation = NbnBinaryValidator.ValidateNbs(stateHeader, stateRegions, overlaySection);
        if (!stateValidation.IsValid)
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_validation_failed", stateValidation.Issues.Count);
            return new TransformParentResult(baseParent);
        }

        if (!IsParentStateCompatibleWithBase(parentDef, baseParent.Header, stateHeader, stateRegions))
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_incompatible_with_base");
            return new TransformParentResult(baseParent);
        }

        if (overlaySection is null)
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_overlay_missing");
            return new TransformParentResult(baseParent);
        }

        if (overlaySection.Records.Length == 0)
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_overlay_empty");
            return new TransformParentResult(baseParent);
        }

        var applied = ApplyOverlayStrengthCodes(baseParent, overlaySection);
        if (applied.MatchedRoutes == 0)
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_overlay_no_matching_routes");
            return new TransformParentResult(baseParent);
        }

        if (applied.IgnoredRoutes > 0)
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_overlay_ignored_routes", applied.IgnoredRoutes);
        }

        ReproductionTelemetry.RecordStrengthOverlayApplied(label, applied.MatchedRoutes);
        return new TransformParentResult(applied.Parent);
    }

    private static List<NbsRegionSection> ReadNbsRegions(
        ReadOnlySpan<byte> nbsBytes,
        NbnHeaderV2 baseHeader,
        NbsHeaderV2 stateHeader,
        out NbsOverlaySection? overlays)
    {
        var offset = NbnBinary.NbsHeaderBytes;
        var includeEnabledBitset = stateHeader.EnabledBitsetIncluded;
        var regions = new List<NbsRegionSection>(NbnConstants.RegionCount);
        for (var regionId = 0; regionId < baseHeader.Regions.Length; regionId++)
        {
            var entry = baseHeader.Regions[regionId];
            if (entry.NeuronSpan == 0)
            {
                continue;
            }

            var section = NbnBinary.ReadNbsRegionSection(nbsBytes, offset, includeEnabledBitset);
            regions.Add(section);
            offset += section.ByteLength;
        }

        overlays = null;
        if (stateHeader.AxonOverlayIncluded)
        {
            overlays = NbnBinary.ReadNbsOverlaySection(nbsBytes, offset);
        }

        return regions;
    }

    private static bool IsParentStateCompatibleWithBase(
        ArtifactRef parentDef,
        NbnHeaderV2 baseHeader,
        NbsHeaderV2 stateHeader,
        IReadOnlyList<NbsRegionSection> stateRegions)
    {
        if (!parentDef.TryToSha256Bytes(out var baseHashBytes) || baseHashBytes.Length != Sha256Hash.Length)
        {
            return false;
        }

        if (stateHeader.BaseNbnSha256 is null
            || stateHeader.BaseNbnSha256.Length != Sha256Hash.Length
            || !stateHeader.BaseNbnSha256.AsSpan().SequenceEqual(baseHashBytes))
        {
            return false;
        }

        var expectedRegionCount = 0;
        for (var regionId = 0; regionId < baseHeader.Regions.Length; regionId++)
        {
            if (baseHeader.Regions[regionId].NeuronSpan > 0)
            {
                expectedRegionCount++;
            }
        }

        if (stateRegions.Count != expectedRegionCount)
        {
            return false;
        }

        for (var i = 0; i < stateRegions.Count; i++)
        {
            var section = stateRegions[i];
            if (section.RegionId >= baseHeader.Regions.Length)
            {
                return false;
            }

            var entry = baseHeader.Regions[section.RegionId];
            if (entry.NeuronSpan == 0 || section.NeuronSpan != entry.NeuronSpan)
            {
                return false;
            }
        }

        return true;
    }

    private static OverlayApplyResult ApplyOverlayStrengthCodes(ParsedParent baseParent, NbsOverlaySection overlaySection)
    {
        var overlayMap = new Dictionary<(int FromRegion, int FromNeuron, int ToRegion, int ToNeuron), byte>();
        var invalidAddressCount = 0;

        for (var i = 0; i < overlaySection.Records.Length; i++)
        {
            var record = overlaySection.Records[i];
            DecodeAddress(record.FromAddress, out var fromRegion, out var fromNeuron);
            DecodeAddress(record.ToAddress, out var toRegion, out var toNeuron);
            if (!IsValidAddress(fromRegion, fromNeuron) || !IsValidAddress(toRegion, toNeuron))
            {
                invalidAddressCount++;
                continue;
            }

            var normalizedStrength = (byte)Math.Clamp((int)record.StrengthCode, 0, 31);
            overlayMap[(fromRegion, fromNeuron, toRegion, toNeuron)] = normalizedStrength;
        }

        if (overlayMap.Count == 0)
        {
            return new OverlayApplyResult(baseParent, 0, invalidAddressCount);
        }

        var matchedRoutes = new HashSet<(int FromRegion, int FromNeuron, int ToRegion, int ToNeuron)>();
        var sections = new List<NbnRegionSection>(baseParent.Regions.Count);
        var changed = false;

        for (var sectionIndex = 0; sectionIndex < baseParent.Regions.Count; sectionIndex++)
        {
            var section = baseParent.Regions[sectionIndex];
            var axonStarts = BuildAxonStarts(section);
            AxonRecord[]? rewrittenAxons = null;

            for (var neuronId = 0; neuronId < section.NeuronRecords.Length; neuronId++)
            {
                var neuron = section.NeuronRecords[neuronId];
                var axonStart = axonStarts[neuronId];
                for (var axonOffset = 0; axonOffset < neuron.AxonCount; axonOffset++)
                {
                    var axonIndex = axonStart + axonOffset;
                    var current = rewrittenAxons is null ? section.AxonRecords[axonIndex] : rewrittenAxons[axonIndex];
                    var route = ((int)section.RegionId, neuronId, (int)current.TargetRegionId, current.TargetNeuronId);
                    if (!overlayMap.TryGetValue(route, out var overlayStrength))
                    {
                        continue;
                    }

                    matchedRoutes.Add(route);
                    if (current.StrengthCode == overlayStrength)
                    {
                        continue;
                    }

                    rewrittenAxons ??= (AxonRecord[])section.AxonRecords.Clone();
                    rewrittenAxons[axonIndex] = new AxonRecord(overlayStrength, current.TargetNeuronId, current.TargetRegionId);
                    changed = true;
                }
            }

            sections.Add(rewrittenAxons is null
                ? section
                : new NbnRegionSection(
                    section.RegionId,
                    section.NeuronSpan,
                    section.TotalAxons,
                    section.Stride,
                    section.CheckpointCount,
                    section.Checkpoints,
                    section.NeuronRecords,
                    rewrittenAxons));
        }

        var ignoredRoutes = Math.Max(overlayMap.Count - matchedRoutes.Count, 0) + invalidAddressCount;
        if (!changed)
        {
            return new OverlayApplyResult(baseParent, matchedRoutes.Count, ignoredRoutes);
        }

        return new OverlayApplyResult(new ParsedParent(baseParent.Header, sections), matchedRoutes.Count, ignoredRoutes);
    }

    private static bool IsValidAddress(int regionId, int neuronId)
        => regionId >= NbnConstants.RegionMinId
           && regionId <= NbnConstants.RegionMaxId
           && neuronId >= 0
           && neuronId <= NbnConstants.MaxAddressNeuronId;

    private static void DecodeAddress(uint address, out int regionId, out int neuronId)
    {
        regionId = (int)(address >> NbnConstants.AddressNeuronBits);
        neuronId = (int)(address & NbnConstants.AddressNeuronMask);
    }

    private static string MapValidationAbortReason(NbnValidationResult validation, string prefix)
    {
        foreach (var issue in validation.Issues)
        {
            if (IsIoInvariantIssue(issue.Message))
            {
                return $"{prefix}_io_invariants_invalid";
            }
        }

        return $"{prefix}_format_invalid";
    }

    private static bool IsIoInvariantIssue(string message)
        => message.Contains("Axons may not target the input region.", StringComparison.Ordinal)
           || message.Contains("Output region axons may not target the output region.", StringComparison.Ordinal)
           || message.Contains("Duplicate axons from the same source neuron are not allowed.", StringComparison.Ordinal)
           || message.Contains("Input and output regions must not contain deleted neurons.", StringComparison.Ordinal);

    private static List<NbnRegionSection> ReadRegions(ReadOnlySpan<byte> nbnBytes, NbnHeaderV2 header)
    {
        var sections = new List<NbnRegionSection>(NbnConstants.RegionCount);
        for (var regionId = 0; regionId < header.Regions.Length; regionId++)
        {
            var entry = header.Regions[regionId];
            if (entry.NeuronSpan == 0 || entry.Offset == 0)
            {
                continue;
            }

            sections.Add(NbnBinary.ReadNbnRegionSection(nbnBytes, entry.Offset));
        }

        return sections;
    }

    private static async Task<byte[]> ReadAllBytesAsync(Stream stream, ulong reportedSize)
    {
        var capacity = reportedSize > 0 && reportedSize < int.MaxValue ? (int)reportedSize : 0;
        using var ms = capacity > 0 ? new MemoryStream(capacity) : new MemoryStream();
        await stream.CopyToAsync(ms).ConfigureAwait(false);
        return ms.ToArray();
    }

    private static bool IsNbnMediaType(string mediaType)
        => string.Equals(mediaType, NbnMediaType, StringComparison.OrdinalIgnoreCase);

    private static bool IsNbsMediaType(string mediaType)
        => string.Equals(mediaType, NbsMediaType, StringComparison.OrdinalIgnoreCase);

    private static string ResolveArtifactRoot(string? storeUri)
    {
        if (!string.IsNullOrWhiteSpace(storeUri))
        {
            if (Uri.TryCreate(storeUri, UriKind.Absolute, out var uri) && uri.IsFile)
            {
                return uri.LocalPath;
            }

            if (!storeUri.Contains("://", StringComparison.Ordinal))
            {
                return storeUri;
            }
        }

        var envRoot = Environment.GetEnvironmentVariable("NBN_ARTIFACT_ROOT");
        if (!string.IsNullOrWhiteSpace(envRoot))
        {
            return envRoot;
        }

        return Path.Combine(Environment.CurrentDirectory, "artifacts");
    }

    private async Task<ReproduceResult> ApplySpawnPolicyAsync(IContext context, ReproduceResult result, ReproduceConfig? config)
    {
        var policy = config?.SpawnChild ?? SpawnChildPolicy.SpawnChildDefaultOn;
        if (policy == SpawnChildPolicy.SpawnChildNever)
        {
            return result;
        }

        if (_ioGatewayPid is null)
        {
            return CreateSpawnFailureResult(result, "repro_spawn_unavailable");
        }

        if (!HasArtifactRef(result.ChildDef))
        {
            return CreateSpawnFailureResult(result, "repro_child_artifact_missing");
        }

        try
        {
            var response = await context
                .RequestAsync<ProtoIo.SpawnBrainViaIOAck>(
                    _ioGatewayPid,
                    new ProtoIo.SpawnBrainViaIO
                    {
                        Request = new ProtoControl.SpawnBrain
                        {
                            BrainDef = result.ChildDef
                        }
                    },
                    DefaultRequestTimeout)
                .ConfigureAwait(false);

            if (response?.Ack?.BrainId is not null
                && response.Ack.BrainId.TryToGuid(out var childBrainId)
                && childBrainId != Guid.Empty)
            {
                result.Spawned = true;
                result.ChildBrainId = response.Ack.BrainId;
                return result;
            }

            return CreateSpawnFailureResult(result, "repro_spawn_failed");
        }
        catch
        {
            return CreateSpawnFailureResult(result, "repro_spawn_request_failed");
        }
    }

    private static ReproduceResult CreateSpawnFailureResult(ReproduceResult result, string reason)
    {
        result.Report ??= new SimilarityReport();
        result.Report.Compatible = false;
        result.Report.AbortReason = reason;
        result.Spawned = false;
        result.ChildBrainId = null;
        return result;
    }

    private static bool HasArtifactRef(ArtifactRef? reference)
        => reference is not null
           && reference.Sha256 is not null
           && reference.Sha256.Value is not null
           && reference.Sha256.Value.Length == Sha256Hash.Length;

    private static ReproduceResult CreateSuccessResult(
        ArtifactRef childDef,
        MutationSummary summary,
        float regionSpanScore,
        float functionScore,
        float connectivityScore,
        float similarityScore,
        int regionsPresentA,
        int regionsPresentB,
        int regionsPresentChild)
        => new()
        {
            Report = new SimilarityReport
            {
                Compatible = true,
                AbortReason = string.Empty,
                RegionSpanScore = Math.Clamp(regionSpanScore, 0f, 1f),
                FunctionScore = Math.Clamp(functionScore, 0f, 1f),
                ConnectivityScore = Math.Clamp(connectivityScore, 0f, 1f),
                SimilarityScore = Math.Clamp(similarityScore, 0f, 1f),
                RegionsPresentA = (uint)Math.Max(regionsPresentA, 0),
                RegionsPresentB = (uint)Math.Max(regionsPresentB, 0),
                RegionsPresentChild = (uint)Math.Max(regionsPresentChild, 0)
            },
            Summary = summary,
            ChildDef = childDef,
            Spawned = false
        };

    private static ReproduceResult CreateAbortResult(
        string reason,
        float regionSpanScore = 0f,
        float functionScore = 0f,
        float connectivityScore = 0f,
        float similarityScore = 0f,
        int regionsPresentA = 0,
        int regionsPresentB = 0,
        int regionsPresentChild = 0)
        => new()
        {
            Report = new SimilarityReport
            {
                Compatible = false,
                AbortReason = reason,
                RegionSpanScore = Math.Clamp(regionSpanScore, 0f, 1f),
                FunctionScore = Math.Clamp(functionScore, 0f, 1f),
                ConnectivityScore = Math.Clamp(connectivityScore, 0f, 1f),
                SimilarityScore = Math.Clamp(similarityScore, 0f, 1f),
                RegionsPresentA = (uint)Math.Max(regionsPresentA, 0),
                RegionsPresentB = (uint)Math.Max(regionsPresentB, 0),
                RegionsPresentChild = (uint)Math.Max(regionsPresentChild, 0)
            },
            Summary = new MutationSummary(),
            Spawned = false
        };

    private sealed record ParsedParent(
        NbnHeaderV2 Header,
        IReadOnlyList<NbnRegionSection> Regions);

    private sealed record LoadParentResult(
        ParsedParent? Parsed,
        string? AbortReason);

    private sealed record TransformParentResult(
        ParsedParent Parsed);

    private sealed record OverlayApplyResult(
        ParsedParent Parent,
        int MatchedRoutes,
        int IgnoredRoutes);

    private sealed record ResolvedParentArtifact(
        ArtifactRef? ParentDef,
        ArtifactRef? ParentState,
        string? AbortReason);

    private sealed record ChildBuildResult(
        ArtifactRef? ChildDef,
        MutationSummary? Summary,
        string? AbortReason,
        int RegionsPresentChild = 0);

    private sealed record ConnectivityHistogram(
        int[] OutDegreeCounts,
        int OutDegreeTotal,
        int[] TargetRegionCounts,
        int TargetRegionTotal);

    private sealed class MutableRegion
    {
        public MutableRegion(int regionId, List<MutableNeuron> neurons)
        {
            RegionId = regionId;
            Neurons = neurons;
        }

        public int RegionId { get; }
        public List<MutableNeuron> Neurons { get; }
    }

    private sealed class MutableNeuron
    {
        public MutableNeuron(NeuronRecord template, bool exists, List<AxonRecord> axons)
        {
            Template = template;
            Exists = exists;
            Axons = axons;
        }

        public NeuronRecord Template { get; }
        public bool Exists { get; set; }
        public List<AxonRecord> Axons { get; }
    }

    private sealed class MutationBudgets
    {
        public MutationBudgets(
            int maxNeuronsAdded,
            int maxNeuronsRemoved,
            int maxAxonsAdded,
            int maxAxonsRemoved,
            int maxRegionsAdded,
            int maxRegionsRemoved)
        {
            MaxNeuronsAdded = maxNeuronsAdded;
            MaxNeuronsRemoved = maxNeuronsRemoved;
            MaxAxonsAdded = maxAxonsAdded;
            MaxAxonsRemoved = maxAxonsRemoved;
            MaxRegionsAdded = maxRegionsAdded;
            MaxRegionsRemoved = maxRegionsRemoved;
        }

        public int MaxNeuronsAdded { get; }
        public int MaxNeuronsRemoved { get; }
        public int MaxAxonsAdded { get; }
        public int MaxAxonsRemoved { get; }
        public int MaxRegionsAdded { get; }
        public int MaxRegionsRemoved { get; }

        public uint NeuronsAdded { get; private set; }
        public uint NeuronsRemoved { get; private set; }
        public uint AxonsAdded { get; private set; }
        public uint AxonsRemoved { get; private set; }
        public uint RegionsAdded { get; private set; }
        public uint RegionsRemoved { get; private set; }
        public uint AxonsRerouted { get; set; }
        public uint FunctionsMutated { get; set; }
        public uint StrengthCodesChanged { get; private set; }

        public bool CanAddNeuron => NeuronsAdded < (uint)MaxNeuronsAdded;
        public bool CanRemoveNeuron => NeuronsRemoved < (uint)MaxNeuronsRemoved;
        public bool CanAddAxon => AxonsAdded < (uint)MaxAxonsAdded;
        public bool CanRemoveAxon => AxonsRemoved < (uint)MaxAxonsRemoved;
        public bool CanAddRegion => RegionsAdded < (uint)MaxRegionsAdded;
        public bool CanRemoveRegion => RegionsRemoved < (uint)MaxRegionsRemoved;

        public void ConsumeNeuronAdded()
        {
            if (CanAddNeuron)
            {
                NeuronsAdded++;
            }
        }

        public void ConsumeNeuronRemoved()
        {
            if (CanRemoveNeuron)
            {
                NeuronsRemoved++;
            }
        }

        public void ConsumeAxonAdded()
        {
            if (CanAddAxon)
            {
                AxonsAdded++;
            }
        }

        public void ConsumeAxonRemoved()
        {
            if (CanRemoveAxon)
            {
                AxonsRemoved++;
            }
        }

        public void ConsumeAxonsRemoved(int count)
        {
            if (count <= 0)
            {
                return;
            }

            for (var i = 0; i < count && CanRemoveAxon; i++)
            {
                AxonsRemoved++;
            }
        }

        public void ConsumeRegionAdded()
        {
            if (CanAddRegion)
            {
                RegionsAdded++;
            }
        }

        public void ConsumeRegionRemoved()
        {
            if (CanRemoveRegion)
            {
                RegionsRemoved++;
            }
        }

        public void RecordStrengthCodeChanged()
        {
            StrengthCodesChanged++;
        }
    }

    private enum ValueSelectionMode : byte
    {
        ParentA = 0,
        ParentB = 1,
        Average = 2,
        Mutate = 3
    }

    private readonly record struct TargetLocus(int RegionId, int NeuronId);

    private readonly record struct WeightedTargetCandidate(TargetLocus Target, float Weight);

    private readonly record struct PruneCandidate(
        int SourceRegionId,
        int SourceNeuronId,
        int AxonIndex,
        float StrengthDistance,
        bool IsNewConnection);

    private sealed record NeuronLocus(int RegionId, int NeuronId);
}
