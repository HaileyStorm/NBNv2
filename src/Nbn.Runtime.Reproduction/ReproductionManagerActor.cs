using Nbn.Proto;
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
    private const string NbnMediaType = "application/x-nbn";
    private const int SpotCheckSampleCount = 32;
    private const float MinRequiredSpotOverlap = 0.35f;
    private const float MaxRequiredSpotOverlap = 0.95f;
    private const ulong DefaultSpotCheckSeed = 0x9E3779B97F4A7C15UL;

    public async Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case ReproduceByBrainIdsRequest message:
                context.Respond(HandleReproduceByBrainIds(message));
                break;
            case ReproduceByArtifactsRequest message:
                context.Respond(await HandleReproduceByArtifactsAsync(message).ConfigureAwait(false));
                break;
        }
    }

    private static ReproduceResult HandleReproduceByBrainIds(ReproduceByBrainIdsRequest request)
    {
        if (request.ParentA is null || request.ParentB is null)
        {
            return CreateAbortResult("repro_missing_parent_brain_ids");
        }

        return CreateAbortResult("repro_parent_resolution_unavailable");
    }

    private static async Task<ReproduceResult> HandleReproduceByArtifactsAsync(ReproduceByArtifactsRequest request)
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

            if (request.StrengthSource != StrengthSource.StrengthBaseOnly)
            {
                return CreateAbortResult("repro_strength_source_not_supported");
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

            return await EvaluateSimilarityGatesAndBuildChildAsync(
                    parentA.Parsed!,
                    parentB.Parsed!,
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

    private static async Task<ReproduceResult> EvaluateSimilarityGatesAndBuildChildAsync(
        ParsedParent parentA,
        ParsedParent parentB,
        ArtifactRef parentARef,
        ArtifactRef parentBRef,
        ReproduceConfig? config,
        ulong seed)
    {
        var presentA = CountPresentRegions(parentA.Header);
        var presentB = CountPresentRegions(parentB.Header);

        if (!AreFormatContractsCompatible(parentA.Header, parentB.Header))
        {
            return CreateAbortResult("repro_format_incompatible", regionsPresentA: presentA, regionsPresentB: presentB);
        }

        if (!HaveMatchingRegionPresence(parentA.Header, parentB.Header))
        {
            return CreateAbortResult("repro_region_presence_mismatch", regionsPresentA: presentA, regionsPresentB: presentB);
        }

        var sectionMapA = BuildSectionMap(parentA.Regions);
        var sectionMapB = BuildSectionMap(parentB.Regions);
        var spanTolerance = ResolveSpanTolerance(config);
        var spanScore = ComputeRegionSpanScore(parentA.Header, parentB.Header, spanTolerance, out var spanMismatch);
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
                parentA,
                parentB,
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

        return CreateSuccessResult(
            childBuild.ChildDef!,
            childBuild.Summary ?? new MutationSummary(),
            spanScore,
            functionScore,
            connectivityScore,
            similarityScore,
            presentA,
            presentB);
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
        return new ChildBuildResult(childRef, summary, null);
    }

    private static List<NbnRegionSection> BuildChildSections(
        ParsedParent parentA,
        ParsedParent parentB,
        ReproduceConfig? config,
        ulong seed,
        out MutationSummary summary)
    {
        var chooseAProbability = ResolveSelectionProbability(
            config?.ProbChooseParentA ?? 0f,
            config?.ProbChooseParentB ?? 0f);

        var sectionsA = BuildSectionMap(parentA.Regions);
        var sectionsB = BuildSectionMap(parentB.Regions);
        var childSections = new List<NbnRegionSection>(sectionsA.Count);
        var state = seed == 0 ? DefaultSpotCheckSeed : seed;
        var functionsMutated = 0u;

        for (var regionId = 0; regionId < NbnConstants.RegionCount; regionId++)
        {
            if (!sectionsA.TryGetValue(regionId, out var sectionA) || !sectionsB.TryGetValue(regionId, out var sectionB))
            {
                continue;
            }

            var startsA = BuildAxonStarts(sectionA);
            var startsB = BuildAxonStarts(sectionB);
            var neuronCount = Math.Min(sectionA.NeuronRecords.Length, sectionB.NeuronRecords.Length);
            var childNeurons = new NeuronRecord[neuronCount];
            var childAxons = new List<AxonRecord>(Math.Max(sectionA.AxonRecords.Length, sectionB.AxonRecords.Length));

            for (var neuronId = 0; neuronId < neuronCount; neuronId++)
            {
                var chooseA = ChooseParentA(chooseAProbability, ref state);
                var sourceSection = chooseA ? sectionA : sectionB;
                var sourceStarts = chooseA ? startsA : startsB;
                var sourceNeuron = sourceSection.NeuronRecords[neuronId];
                childNeurons[neuronId] = sourceNeuron;

                if (!chooseA && NeuronFunctionsDiffer(sectionA.NeuronRecords[neuronId], sourceNeuron))
                {
                    functionsMutated++;
                }

                var axonStart = sourceStarts[neuronId];
                for (var axonOffset = 0; axonOffset < sourceNeuron.AxonCount; axonOffset++)
                {
                    childAxons.Add(sourceSection.AxonRecords[axonStart + axonOffset]);
                }
            }

            var checkpoints = NbnBinary.BuildCheckpoints(childNeurons, parentA.Header.AxonStride);
            childSections.Add(new NbnRegionSection(
                (byte)regionId,
                (uint)childNeurons.Length,
                (ulong)childAxons.Count,
                parentA.Header.AxonStride,
                (uint)checkpoints.Length,
                checkpoints,
                childNeurons,
                childAxons.ToArray()));
        }

        summary = new MutationSummary
        {
            FunctionsMutated = functionsMutated
        };
        return childSections;
    }

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

    private static ReproduceResult CreateSuccessResult(
        ArtifactRef childDef,
        MutationSummary summary,
        float regionSpanScore,
        float functionScore,
        float connectivityScore,
        float similarityScore,
        int regionsPresentA,
        int regionsPresentB)
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
                RegionsPresentChild = (uint)Math.Max(regionsPresentA, 0)
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

    private sealed record ChildBuildResult(
        ArtifactRef? ChildDef,
        MutationSummary? Summary,
        string? AbortReason);

    private sealed record ConnectivityHistogram(
        int[] OutDegreeCounts,
        int OutDegreeTotal,
        int[] TargetRegionCounts,
        int TargetRegionTotal);

    private sealed record NeuronLocus(int RegionId, int NeuronId);
}
