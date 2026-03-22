using System;
using System.Collections.Generic;
using System.Linq;
using Nbn.Proto.Speciation;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class SpeciationPanelViewModel
{
    private static List<SpeciesPopulationMeta> OrderFlowSpeciesForDisplay(
        IReadOnlyList<SpeciesPopulationMeta> selectedSpecies)
    {
        return selectedSpecies
            .OrderBy(item => item.FirstSeenOrder)
            .ThenBy(item => item.SpeciesId, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static List<SpeciesPopulationMeta> SelectFlowChartSpecies(
        IReadOnlyList<SpeciesPopulationMeta> speciesOrder,
        int visibleSpeciesLimit,
        bool includeNewestSpecies)
    {
        if (speciesOrder.Count <= visibleSpeciesLimit)
        {
            return speciesOrder.ToList();
        }

        var founderFamilies = BuildFlowFounderFamilies(speciesOrder, includeNewestSpecies);
        if (founderFamilies.Count <= 1)
        {
            if (includeNewestSpecies && founderFamilies.Count == 1)
            {
                return founderFamilies[0].RankedSpecies
                    .Take(visibleSpeciesLimit)
                    .ToList();
            }

            return speciesOrder
                .Take(visibleSpeciesLimit)
                .ToList();
        }

        var selectedFamilies = founderFamilies
            .OrderByDescending(family => family.TotalPopulation)
            .ThenBy(family => family.Founder.FirstSeenOrder)
            .ThenBy(family => family.Founder.SpeciesId, StringComparer.OrdinalIgnoreCase)
            .Take(Math.Min(visibleSpeciesLimit, founderFamilies.Count))
            .OrderBy(family => family.Founder.FirstSeenOrder)
            .ThenBy(family => family.Founder.SpeciesId, StringComparer.OrdinalIgnoreCase)
            .ToList();
        var selectedSpecies = new List<SpeciesPopulationMeta>(visibleSpeciesLimit);
        var nextSpeciesIndexByFamily = new int[selectedFamilies.Count];
        for (var familyIndex = 0; familyIndex < selectedFamilies.Count && selectedSpecies.Count < visibleSpeciesLimit; familyIndex++)
        {
            selectedSpecies.Add(selectedFamilies[familyIndex].Founder);
            nextSpeciesIndexByFamily[familyIndex] = 1;
        }

        while (selectedSpecies.Count < visibleSpeciesLimit)
        {
            var addedInRound = false;
            for (var familyIndex = 0; familyIndex < selectedFamilies.Count && selectedSpecies.Count < visibleSpeciesLimit; familyIndex++)
            {
                var rankedSpecies = selectedFamilies[familyIndex].RankedSpecies;
                if (nextSpeciesIndexByFamily[familyIndex] >= rankedSpecies.Length)
                {
                    continue;
                }

                selectedSpecies.Add(rankedSpecies[nextSpeciesIndexByFamily[familyIndex]]);
                nextSpeciesIndexByFamily[familyIndex]++;
                addedInRound = true;
            }

            if (!addedInRound)
            {
                break;
            }
        }

        return selectedSpecies;
    }

    private static List<FlowFounderFamily> BuildFlowFounderFamilies(
        IReadOnlyList<SpeciesPopulationMeta> speciesOrder,
        bool includeNewestSpecies)
    {
        var metaBySpeciesId = speciesOrder.ToDictionary(item => item.SpeciesId, StringComparer.OrdinalIgnoreCase);
        var speciesByFounderId = new Dictionary<string, List<SpeciesPopulationMeta>>(StringComparer.OrdinalIgnoreCase);
        foreach (var species in speciesOrder)
        {
            var founderSpeciesId = ResolveFlowFounderSpeciesId(species.SpeciesId, metaBySpeciesId);
            if (!speciesByFounderId.TryGetValue(founderSpeciesId, out var familySpecies))
            {
                familySpecies = new List<SpeciesPopulationMeta>();
                speciesByFounderId[founderSpeciesId] = familySpecies;
            }

            familySpecies.Add(species);
        }

        var families = new List<FlowFounderFamily>(speciesByFounderId.Count);
        foreach (var entry in speciesByFounderId)
        {
            if (!metaBySpeciesId.TryGetValue(entry.Key, out var founder))
            {
                founder = entry.Value
                    .OrderBy(item => item.FirstSeenOrder)
                    .ThenBy(item => item.SpeciesId, StringComparer.OrdinalIgnoreCase)
                    .First();
            }

            var descendants = entry.Value
                .Where(item => !string.Equals(item.SpeciesId, founder.SpeciesId, StringComparison.OrdinalIgnoreCase))
                .ToArray();
            var rankedSpecies = BuildFlowFounderFamilyRanking(founder, descendants, includeNewestSpecies);
            families.Add(new FlowFounderFamily(
                Founder: founder,
                RankedSpecies: rankedSpecies,
                TotalPopulation: entry.Value.Sum(item => item.TotalCount)));
        }

        return families;
    }

    private static SpeciesPopulationMeta[] BuildFlowFounderFamilyRanking(
        SpeciesPopulationMeta founder,
        IReadOnlyList<SpeciesPopulationMeta> descendants,
        bool includeNewestSpecies)
    {
        if (descendants.Count == 0)
        {
            return [founder];
        }

        var populationRanked = descendants
            .OrderByDescending(item => item.TotalCount)
            .ThenBy(item => item.FirstSeenOrder)
            .ThenBy(item => item.SpeciesId, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (!includeNewestSpecies)
        {
            return populationRanked
                .Prepend(founder)
                .ToArray();
        }

        var newestRanked = descendants
            .Where(item => item.TotalCount > 2)
            .OrderByDescending(item => item.FirstAssignedMs)
            .ThenByDescending(item => item.FirstSeenOrder)
            .ThenBy(item => item.SpeciesId, StringComparer.OrdinalIgnoreCase)
            .Concat(
                descendants
                    .Where(item => item.TotalCount <= 2)
                    .OrderByDescending(item => item.FirstAssignedMs)
                    .ThenByDescending(item => item.FirstSeenOrder)
                    .ThenBy(item => item.SpeciesId, StringComparer.OrdinalIgnoreCase))
            .ToArray();
        var ranked = new List<SpeciesPopulationMeta>(descendants.Count + 1) { founder };
        var seenSpeciesIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            founder.SpeciesId
        };

        void TryAdd(SpeciesPopulationMeta candidate)
        {
            if (seenSpeciesIds.Add(candidate.SpeciesId))
            {
                ranked.Add(candidate);
            }
        }

        var maxCount = Math.Max(populationRanked.Length, newestRanked.Length);
        for (var index = 0; index < maxCount; index++)
        {
            if (index < populationRanked.Length)
            {
                TryAdd(populationRanked[index]);
            }

            if (index < newestRanked.Length)
            {
                TryAdd(newestRanked[index]);
            }
        }

        return ranked.ToArray();
    }

    private static string ResolveFlowFounderSpeciesId(
        string speciesId,
        IReadOnlyDictionary<string, SpeciesPopulationMeta> metaBySpeciesId)
    {
        if (string.IsNullOrWhiteSpace(speciesId)
            || !metaBySpeciesId.TryGetValue(speciesId, out var current))
        {
            return speciesId;
        }

        var currentSpeciesId = current.SpeciesId;
        var visitedSpeciesIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        while (!string.IsNullOrWhiteSpace(current.ParentSpeciesId) && visitedSpeciesIds.Add(currentSpeciesId))
        {
            if (!metaBySpeciesId.TryGetValue(current.ParentSpeciesId, out var parent))
            {
                break;
            }

            current = parent;
            currentSpeciesId = current.SpeciesId;
        }

        return currentSpeciesId;
    }

    private static IReadOnlyList<SpeciationFlowChartSampleItem> BuildFlowChartAreaSamples(
        IReadOnlyList<EpochPopulationRow> epochRows,
        IReadOnlyList<FlowSpeciesRowBands[]> bandsByEpoch,
        IReadOnlyList<SpeciesPopulationMeta> flowSpecies,
        int speciesIndex,
        bool isSingleEpochRowSampling,
        FlowChartRenderLayout layout)
    {
        var samples = new List<SpeciationFlowChartSampleItem>(epochRows.Count);
        var usableWidth = Math.Max(1d, layout.PlotWidth - (layout.PaddingX * 2d));
        var usableHeight = Math.Max(1d, layout.PlotHeight - (layout.PaddingY * 2d));
        var yStep = epochRows.Count > 1 ? usableHeight / (epochRows.Count - 1) : 0d;
        for (var epochIndex = 0; epochIndex < epochRows.Count; epochIndex++)
        {
            var bands = BuildFlowChartSampleBands(bandsByEpoch[epochIndex][speciesIndex], layout.PaddingX, usableWidth);
            if (bands.Count == 0)
            {
                continue;
            }

            var row = epochRows[epochIndex];
            var populationCount = ResolveFlowChartRowPopulationCount(row, flowSpecies, speciesIndex);
            if (populationCount <= 0 || row.TotalCount <= 0)
            {
                continue;
            }

            var rowLabel = isSingleEpochRowSampling
                ? $"Sample {epochIndex + 1} (Epoch {row.EpochId})"
                : $"Epoch {row.EpochId}";
            samples.Add(new SpeciationFlowChartSampleItem(
                RowLabel: rowLabel,
                PopulationCount: populationCount,
                TotalCount: row.TotalCount,
                Share: populationCount / (double)row.TotalCount,
                CenterY: layout.PaddingY + (epochIndex * yStep),
                Bands: bands));
        }

        return samples;
    }

    private static IReadOnlyList<SpeciationFlowChartSampleBand> BuildFlowChartSampleBands(
        FlowSpeciesRowBands rowBands,
        double paddingX,
        double usableWidth)
    {
        var bands = new List<SpeciationFlowChartSampleBand>(2);

        void TryAdd(double start, double end)
        {
            if (!double.IsFinite(start) || !double.IsFinite(end) || end <= start)
            {
                return;
            }

            bands.Add(new SpeciationFlowChartSampleBand(
                StartX: paddingX + (Math.Clamp(start, 0d, 1d) * usableWidth),
                EndX: paddingX + (Math.Clamp(end, 0d, 1d) * usableWidth)));
        }

        TryAdd(rowBands.PrimaryStart, rowBands.PrimaryEnd);
        TryAdd(rowBands.SecondaryStart, rowBands.SecondaryEnd);
        return bands;
    }

    private static int ResolveFlowChartRowPopulationCount(
        EpochPopulationRow row,
        IReadOnlyList<SpeciesPopulationMeta> flowSpecies,
        int speciesIndex)
    {
        var species = flowSpecies[speciesIndex];
        if (!string.Equals(species.SpeciesId, "(other)", StringComparison.Ordinal))
        {
            return row.Counts.TryGetValue(species.SpeciesId, out var count)
                ? Math.Max(0, count)
                : 0;
        }

        var visibleSpeciesTotal = 0;
        for (var index = 0; index < flowSpecies.Count; index++)
        {
            if (index == speciesIndex)
            {
                continue;
            }

            if (row.Counts.TryGetValue(flowSpecies[index].SpeciesId, out var count))
            {
                visibleSpeciesTotal += Math.Max(0, count);
            }
        }

        return Math.Max(0, row.TotalCount - visibleSpeciesTotal);
    }

    private static string CombineFlowAreaPaths(params string[] paths)
    {
        return string.Join(
            " ",
            paths.Where(path => !string.IsNullOrWhiteSpace(path)));
    }

    private static void AddFlowAreaTransitionCaps(
        double[] starts,
        double[] ends,
        IReadOnlyList<double> edgeAnchors)
    {
        if (starts.Length == 0
            || starts.Length != ends.Length
            || starts.Length != edgeAnchors.Count)
        {
            return;
        }

        for (var rowIndex = 0; rowIndex < starts.Length - 1; rowIndex++)
        {
            var currentValid = double.IsFinite(starts[rowIndex]) && double.IsFinite(ends[rowIndex]);
            var nextValid = double.IsFinite(starts[rowIndex + 1]) && double.IsFinite(ends[rowIndex + 1]);
            if (!currentValid && nextValid && double.IsFinite(edgeAnchors[rowIndex]))
            {
                starts[rowIndex] = edgeAnchors[rowIndex];
                ends[rowIndex] = edgeAnchors[rowIndex];
            }

            if (currentValid && !nextValid && double.IsFinite(edgeAnchors[rowIndex + 1]))
            {
                starts[rowIndex + 1] = edgeAnchors[rowIndex + 1];
                ends[rowIndex + 1] = edgeAnchors[rowIndex + 1];
            }
        }
    }

    private static int[] BuildFlowVisibleParentIndices(
        IReadOnlyList<SpeciesPopulationMeta> flowSpecies,
        IReadOnlyList<SpeciesPopulationMeta> allSpecies)
    {
        var visibleParents = Enumerable.Repeat(-1, flowSpecies.Count).ToArray();
        if (flowSpecies.Count == 0)
        {
            return visibleParents;
        }

        var indexBySpeciesId = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var speciesIndex = 0; speciesIndex < flowSpecies.Count; speciesIndex++)
        {
            var speciesId = flowSpecies[speciesIndex].SpeciesId;
            if (!string.IsNullOrWhiteSpace(speciesId) && !indexBySpeciesId.ContainsKey(speciesId))
            {
                indexBySpeciesId[speciesId] = speciesIndex;
            }
        }

        var metaBySpeciesId = new Dictionary<string, SpeciesPopulationMeta>(StringComparer.OrdinalIgnoreCase);
        foreach (var species in allSpecies)
        {
            var speciesId = NormalizeSpeciesId(species.SpeciesId);
            if (!metaBySpeciesId.ContainsKey(speciesId))
            {
                metaBySpeciesId[speciesId] = species;
            }
        }

        for (var speciesIndex = 0; speciesIndex < flowSpecies.Count; speciesIndex++)
        {
            var species = flowSpecies[speciesIndex];
            var parentSpeciesId = species.ParentSpeciesId;
            if (string.IsNullOrWhiteSpace(parentSpeciesId))
            {
                continue;
            }

            var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var currentParentId = parentSpeciesId;
            while (!string.IsNullOrWhiteSpace(currentParentId))
            {
                var normalizedParentId = NormalizeSpeciesId(currentParentId);
                if (!visited.Add(normalizedParentId))
                {
                    break;
                }

                if (indexBySpeciesId.TryGetValue(normalizedParentId, out var visibleParentIndex)
                    && visibleParentIndex != speciesIndex)
                {
                    visibleParents[speciesIndex] = visibleParentIndex;
                    break;
                }

                if (!metaBySpeciesId.TryGetValue(normalizedParentId, out var parentMeta))
                {
                    break;
                }

                currentParentId = parentMeta.ParentSpeciesId;
            }
        }

        return visibleParents;
    }

    private static List<int>[] BuildFlowChildIndices(
        IReadOnlyList<SpeciesPopulationMeta> flowSpecies,
        IReadOnlyList<int> visibleParentBySpeciesIndex)
    {
        var childrenByParent = Enumerable.Range(0, flowSpecies.Count)
            .Select(_ => new List<int>())
            .ToArray();
        for (var speciesIndex = 0; speciesIndex < flowSpecies.Count; speciesIndex++)
        {
            var parentIndex = visibleParentBySpeciesIndex[speciesIndex];
            if (parentIndex >= 0)
            {
                childrenByParent[parentIndex].Add(speciesIndex);
            }
        }

        foreach (var children in childrenByParent)
        {
            children.Sort((left, right) =>
            {
                var firstSeenComparison = flowSpecies[left].FirstSeenOrder.CompareTo(flowSpecies[right].FirstSeenOrder);
                if (firstSeenComparison != 0)
                {
                    return firstSeenComparison;
                }

                return string.Compare(
                    flowSpecies[left].SpeciesId,
                    flowSpecies[right].SpeciesId,
                    StringComparison.OrdinalIgnoreCase);
            });
        }

        return childrenByParent;
    }

    private static void ResolveFlowLineageRowBands(
        EpochPopulationRow row,
        IReadOnlyList<SpeciesPopulationMeta> flowSpecies,
        bool includeOtherSpecies,
        IReadOnlyList<int> rootIndices,
        IReadOnlyList<List<int>> childIndicesByParent,
        FlowSpeciesRowBands[] rowBands)
    {
        var speciesCount = flowSpecies.Count;
        var countsBySpecies = new double[speciesCount];
        var selectedTotal = 0;
        for (var speciesIndex = 0; speciesIndex < speciesCount; speciesIndex++)
        {
            int count;
            if (includeOtherSpecies && speciesIndex == speciesCount - 1)
            {
                count = Math.Max(0, row.TotalCount - selectedTotal);
            }
            else
            {
                count = row.Counts.TryGetValue(flowSpecies[speciesIndex].SpeciesId, out var value)
                    ? Math.Max(0, value)
                    : 0;
                selectedTotal += count;
            }

            countsBySpecies[speciesIndex] = count;
        }

        var subtreeTotals = new double[speciesCount];
        double ComputeSubtreeTotal(int speciesIndex)
        {
            if (subtreeTotals[speciesIndex] > 0d)
            {
                return subtreeTotals[speciesIndex];
            }

            var total = countsBySpecies[speciesIndex];
            foreach (var childIndex in childIndicesByParent[speciesIndex])
            {
                total += ComputeSubtreeTotal(childIndex);
            }

            subtreeTotals[speciesIndex] = total;
            return total;
        }

        var visibleTotal = 0d;
        foreach (var rootIndex in rootIndices)
        {
            visibleTotal += ComputeSubtreeTotal(rootIndex);
        }

        if (visibleTotal <= 1e-6d)
        {
            return;
        }

        void AllocateSpecies(int speciesIndex, double start, double end)
        {
            var nodeTotal = subtreeTotals[speciesIndex];
            if (nodeTotal <= 1e-6d || end <= start)
            {
                return;
            }

            var activeChildren = childIndicesByParent[speciesIndex]
                .Where(childIndex => subtreeTotals[childIndex] > 1e-6d)
                .ToList();
            var ownCount = countsBySpecies[speciesIndex];
            var spanWidth = end - start;
            var childStart = start;
            var childEnd = end;
            if (ownCount > 1e-6d)
            {
                if (activeChildren.Count == 0)
                {
                    rowBands[speciesIndex] = new FlowSpeciesRowBands(start, end, double.NaN, double.NaN);
                    return;
                }

                var ownWidth = spanWidth * (ownCount / nodeTotal);
                var leftOwnWidth = ownWidth * 0.5d;
                var rightOwnWidth = ownWidth - leftOwnWidth;
                var primaryEnd = Math.Min(end, start + leftOwnWidth);
                var secondaryStart = Math.Max(start, end - rightOwnWidth);
                rowBands[speciesIndex] = new FlowSpeciesRowBands(start, primaryEnd, secondaryStart, end);
                childStart = primaryEnd;
                childEnd = secondaryStart;
            }

            if (activeChildren.Count == 0 || childEnd <= childStart)
            {
                return;
            }

            var childClusterWidth = childEnd - childStart;
            var childTotal = activeChildren.Sum(childIndex => subtreeTotals[childIndex]);
            var cursor = childStart;
            for (var childOrdinal = 0; childOrdinal < activeChildren.Count; childOrdinal++)
            {
                var childIndex = activeChildren[childOrdinal];
                var nextChildEnd = childOrdinal == activeChildren.Count - 1
                    ? childStart + childClusterWidth
                    : Math.Min(childStart + childClusterWidth, cursor + (childClusterWidth * (subtreeTotals[childIndex] / childTotal)));
                AllocateSpecies(childIndex, cursor, nextChildEnd);
                cursor = nextChildEnd;
            }
        }

        var rootCursor = 0d;
        var activeRoots = rootIndices
            .Where(rootIndex => subtreeTotals[rootIndex] > 1e-6d)
            .ToList();
        for (var rootOrdinal = 0; rootOrdinal < activeRoots.Count; rootOrdinal++)
        {
            var rootIndex = activeRoots[rootOrdinal];
            var rootEnd = rootOrdinal == activeRoots.Count - 1
                ? 1d
                : Math.Min(1d, rootCursor + (subtreeTotals[rootIndex] / visibleTotal));
            AllocateSpecies(rootIndex, rootCursor, rootEnd);
            rootCursor = rootEnd;
        }
    }

    private readonly record struct FlowFounderFamily(
        SpeciesPopulationMeta Founder,
        SpeciesPopulationMeta[] RankedSpecies,
        int TotalPopulation);

    private readonly record struct FlowSpeciesRowBands(
        double PrimaryStart,
        double PrimaryEnd,
        double SecondaryStart,
        double SecondaryEnd)
    {
        public static FlowSpeciesRowBands Empty => new(double.NaN, double.NaN, double.NaN, double.NaN);

        public double TotalWidth
        {
            get
            {
                var total = 0d;
                if (double.IsFinite(PrimaryStart) && double.IsFinite(PrimaryEnd) && PrimaryEnd > PrimaryStart)
                {
                    total += PrimaryEnd - PrimaryStart;
                }

                if (double.IsFinite(SecondaryStart) && double.IsFinite(SecondaryEnd) && SecondaryEnd > SecondaryStart)
                {
                    total += SecondaryEnd - SecondaryStart;
                }

                return total;
            }
        }
    }
}
