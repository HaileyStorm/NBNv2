using Nbn.Proto;
using Nbn.Proto.Repro;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Quantization;
using Proto;

namespace Nbn.Runtime.Reproduction;

public sealed partial class ReproductionManagerActor
{
    private async Task<ReproduceResult> EvaluateSimilarityGatesAndBuildChildAsync(
        IContext context,
        PreparedReproductionRun execution,
        ulong seed)
    {
        var gateParentA = execution.GateParentA;
        var gateParentB = execution.GateParentB;
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
        var spanTolerance = ResolveSpanTolerance(execution.Config);
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
        var maxFunctionDistance = ResolveDistanceThreshold(execution.Config?.MaxFunctionHistDistance ?? 0f);
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
        var maxConnectivityDistance = ResolveDistanceThreshold(execution.Config?.MaxConnectivityHistDistance ?? 0f);
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

        if (execution.AssessmentOnly)
        {
            return CreateAssessmentResult(
                spanScore,
                functionScore,
                connectivityScore,
                similarityScore,
                presentA,
                presentB);
        }

        var childBuild = await BuildAndStoreChildDefinitionAsync(
                execution.TransformParentA,
                execution.TransformParentB,
                execution.ParentARef,
                execution.ParentBRef,
                execution.Config,
                seed,
                execution.ManualIoNeuronAdds,
                execution.ManualIoNeuronRemoves)
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
            childBuild.LineageSimilarityScore,
            childBuild.LineageParentASimilarityScore,
            childBuild.LineageParentBSimilarityScore,
            presentA,
            presentB,
            childBuild.RegionsPresentChild);
        return await ApplySpawnPolicyAsync(context, result, execution.Config).ConfigureAwait(false);
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

    private static LineageSimilarityScores ComputeLineageSimilarityScores(
        ParsedParent parentA,
        ParsedParent parentB,
        NbnHeaderV2 childHeader,
        IReadOnlyList<NbnRegionSection> childSections,
        float spanTolerance,
        ulong seed)
    {
        var child = new ParsedParent(childHeader, childSections);
        var parentAScore = ComputeLineageSimilarityScore(
            parentA,
            child,
            spanTolerance,
            seed ^ 0x91E10DA5C79E7B1DUL);
        var parentBScore = ComputeLineageSimilarityScore(
            parentB,
            child,
            spanTolerance,
            seed ^ 0xC2B2AE3D27D4EB4FUL);

        return new LineageSimilarityScores(
            LineageSimilarityScore: Math.Clamp((parentAScore + parentBScore) * 0.5f, 0f, 1f),
            ParentASimilarityScore: parentAScore,
            ParentBSimilarityScore: parentBScore);
    }

    private static float ComputeLineageSimilarityScore(
        ParsedParent parent,
        ParsedParent child,
        float spanTolerance,
        ulong seed)
    {
        var sectionMapParent = BuildSectionMap(parent.Regions);
        var sectionMapChild = BuildSectionMap(child.Regions);
        var spanScore = ComputeRegionSpanScore(
            parent.Header,
            child.Header,
            spanTolerance,
            out _);
        var functionScore = 1f - ComputeFunctionDistance(sectionMapParent, sectionMapChild);
        var connectivityScore = 1f - ComputeConnectivityDistance(sectionMapParent, sectionMapChild);
        var spotCheckOverlap = ComputeSpotCheckOverlap(sectionMapParent, sectionMapChild, seed);
        return ComputeSimilarityScore(spanScore, functionScore, connectivityScore, spotCheckOverlap);
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

}
