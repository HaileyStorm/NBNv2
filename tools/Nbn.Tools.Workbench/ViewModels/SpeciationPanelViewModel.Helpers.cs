using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using Nbn.Proto.Speciation;
using Nbn.Shared;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class SpeciationPanelViewModel
{
    private static (List<EpochPopulationRow> EpochRows, List<SpeciesPopulationMeta> SpeciesOrder) BuildEpochPopulationFrame(IReadOnlyList<SpeciationMembershipRecord> history)
    {
        if (history.Count == 0)
        {
            return (new List<EpochPopulationRow>(), new List<SpeciesPopulationMeta>());
        }

        var orderedHistory = OrderHistoryForSampling(history);
        return ShouldUseSingleEpochRowSampling(orderedHistory)
            ? BuildSingleEpochPopulationFrame(orderedHistory)
            : BuildAggregatedEpochPopulationFrame(orderedHistory);
    }

    private static (List<EpochPopulationRow> EpochRows, List<SpeciesPopulationMeta> SpeciesOrder) BuildAggregatedEpochPopulationFrame(
        IReadOnlyList<SpeciationMembershipRecord> history)
    {
        var speciesStats = new Dictionary<string, SpeciesPopulationMeta>(StringComparer.OrdinalIgnoreCase);
        var nextFirstSeenOrder = 1;
        var epochRows = history
            .GroupBy(entry => (long)entry.EpochId)
            .OrderBy(group => group.Key)
            .Select(group =>
            {
                var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                foreach (var record in group)
                {
                    var speciesId = NormalizeSpeciesId(record.SpeciesId);
                    var speciesName = BuildCompactSpeciesName(record.SpeciesDisplayName, speciesId);
                    var parentSpeciesId = TryExtractLineageParentSpeciesFromMetadata(record.DecisionMetadataJson, out var parentId, out _)
                        ? NormalizeSpeciesId(parentId)
                        : string.Empty;
                    counts.TryGetValue(speciesId, out var prior);
                    counts[speciesId] = prior + 1;
                    if (speciesStats.TryGetValue(speciesId, out var existing))
                    {
                        speciesStats[speciesId] = existing with
                        {
                            DisplayName = string.IsNullOrWhiteSpace(existing.DisplayName) ? speciesName : existing.DisplayName,
                            TotalCount = existing.TotalCount + 1,
                            ParentSpeciesId = string.IsNullOrWhiteSpace(existing.ParentSpeciesId) ? parentSpeciesId : existing.ParentSpeciesId
                        };
                    }
                    else
                    {
                        speciesStats[speciesId] = new SpeciesPopulationMeta(
                            speciesId,
                            speciesName,
                            1,
                            record.AssignedMs,
                            nextFirstSeenOrder++,
                            parentSpeciesId);
                    }
                }

                return new EpochPopulationRow(group.Key, counts, counts.Values.Sum());
            })
            .ToList();
        var speciesOrder = speciesStats.Values
            .OrderByDescending(item => item.TotalCount)
            .ThenBy(item => item.FirstSeenOrder)
            .ThenBy(item => item.SpeciesId, StringComparer.OrdinalIgnoreCase)
            .ToList();
        return (epochRows, speciesOrder);
    }

    private static (List<EpochPopulationRow> EpochRows, List<SpeciesPopulationMeta> SpeciesOrder) BuildSingleEpochPopulationFrame(
        IReadOnlyList<SpeciationMembershipRecord> orderedHistory)
    {
        var speciesStats = new Dictionary<string, SpeciesPopulationMeta>(StringComparer.OrdinalIgnoreCase);
        var nextFirstSeenOrder = 1;
        var runningCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var epochRows = new List<EpochPopulationRow>(orderedHistory.Count);
        var runningTotal = 0;
        foreach (var record in orderedHistory)
        {
            var speciesId = NormalizeSpeciesId(record.SpeciesId);
            var speciesName = BuildCompactSpeciesName(record.SpeciesDisplayName, speciesId);
            var parentSpeciesId = TryExtractLineageParentSpeciesFromMetadata(record.DecisionMetadataJson, out var parentId, out _)
                ? NormalizeSpeciesId(parentId)
                : string.Empty;
            runningCounts.TryGetValue(speciesId, out var prior);
            runningCounts[speciesId] = prior + 1;
            runningTotal++;

            if (speciesStats.TryGetValue(speciesId, out var existing))
            {
                speciesStats[speciesId] = existing with
                {
                    DisplayName = string.IsNullOrWhiteSpace(existing.DisplayName) ? speciesName : existing.DisplayName,
                    TotalCount = existing.TotalCount + 1,
                    ParentSpeciesId = string.IsNullOrWhiteSpace(existing.ParentSpeciesId) ? parentSpeciesId : existing.ParentSpeciesId
                };
            }
            else
            {
                speciesStats[speciesId] = new SpeciesPopulationMeta(
                    speciesId,
                    speciesName,
                    1,
                    record.AssignedMs,
                    nextFirstSeenOrder++,
                    parentSpeciesId);
            }

            epochRows.Add(
                new EpochPopulationRow(
                    EpochId: (long)record.EpochId,
                    Counts: new Dictionary<string, int>(runningCounts, StringComparer.OrdinalIgnoreCase),
                    TotalCount: runningTotal));
        }

        var speciesOrder = speciesStats.Values
            .OrderByDescending(item => item.TotalCount)
            .ThenBy(item => item.FirstSeenOrder)
            .ThenBy(item => item.SpeciesId, StringComparer.OrdinalIgnoreCase)
            .ToList();
        return (epochRows, speciesOrder);
    }

    private static SplitProximityChartSnapshot BuildSplitProximityChartSnapshot(
        IReadOnlyList<SpeciationMembershipRecord> history,
        long currentEpochId,
        double fallbackSplitThreshold,
        double fallbackSplitGuardMargin,
        IReadOnlyDictionary<string, string> speciesColors)
    {
        if (history.Count == 0)
        {
            return SplitProximityChartSnapshot.Empty("Split proximity (current epoch): (n/a)");
        }

        var boundedFallbackSplit = Clamp01(fallbackSplitThreshold);
        var orderedHistory = OrderHistoryForSampling(history);
        var useSingleEpochRowSampling = ShouldUseSingleEpochRowSampling(orderedHistory);
        var speciesMeta = new Dictionary<string, SplitProximitySpeciesMeta>(StringComparer.OrdinalIgnoreCase);
        var epochRows = useSingleEpochRowSampling
            ? BuildSingleEpochSplitProximityRows(orderedHistory, boundedFallbackSplit, fallbackSplitGuardMargin, speciesMeta)
            : BuildAggregatedEpochSplitProximityRows(orderedHistory, boundedFallbackSplit, fallbackSplitGuardMargin, speciesMeta);

        if (epochRows.Count == 0 || speciesMeta.Count == 0)
        {
            return SplitProximityChartSnapshot.Empty("Split proximity (current epoch): (n/a, no similarity scores)");
        }

        var selectedSpecies = speciesMeta.Values
            .OrderByDescending(item => item.LatestAssignedMs)
            .ThenByDescending(item => item.SampleCount)
            .ThenBy(item => item.SpeciesId, StringComparer.OrdinalIgnoreCase)
            .Take(SplitProximityTopSpeciesLimit)
            .ToList();
        if (selectedSpecies.Count == 0)
        {
            return SplitProximityChartSnapshot.Empty("Split proximity (current epoch): (n/a, no species evidence)");
        }

        var allValues = new List<double>(selectedSpecies.Count * Math.Max(1, epochRows.Count));
        foreach (var species in selectedSpecies)
        {
            foreach (var row in epochRows)
            {
                if (row.ValuesBySpecies.TryGetValue(species.SpeciesId, out var point))
                {
                    allValues.Add(point.MinProximity);
                }
            }
        }

        if (allValues.Count == 0)
        {
            return SplitProximityChartSnapshot.Empty("Split proximity (current epoch): (n/a, no proximity samples)");
        }

        var rawMin = allValues.Min();
        var rawMax = allValues.Max();
        if (!double.IsFinite(rawMin) || !double.IsFinite(rawMax))
        {
            return SplitProximityChartSnapshot.Empty("Split proximity (current epoch): (n/a, invalid proximity values)");
        }

        if (!(rawMax > rawMin))
        {
            var padding = Math.Max(Math.Abs(rawMax), 0.001d) * 0.05d;
            rawMin -= padding;
            rawMax += padding;
        }

        var yMin = TransformSignedLog(rawMin);
        var yMax = TransformSignedLog(rawMax);
        if (!(yMax > yMin))
        {
            yMax = yMin + 0.001d;
        }

        var midRawValue = rawMin <= 0d && rawMax >= 0d
            ? 0d
            : (rawMin + rawMax) * 0.5d;

        var series = new List<SpeciationLineChartSeriesItem>(selectedSpecies.Count);
        var legend = new List<SpeciationChartLegendItem>(selectedSpecies.Count);
        foreach (var species in selectedSpecies)
        {
            var rawValues = epochRows
                .Select(row => row.ValuesBySpecies.TryGetValue(species.SpeciesId, out var point)
                    ? point.MinProximity
                    : double.NaN)
                .ToArray();
            var values = rawValues
                .Select(TransformSignedLogOrNan)
                .ToArray();
            var path = BuildLinePath(
                values,
                yMin: yMin,
                yMax: yMax,
                plotWidth: PopulationChartPlotWidth,
                plotHeight: PopulationChartPlotHeight,
                paddingX: PopulationChartPaddingX,
                paddingY: PopulationChartPaddingY);
            if (string.IsNullOrWhiteSpace(path))
            {
                continue;
            }

            var latestValue = TryGetLatestFiniteValue(rawValues, out var latest)
                ? latest
                : double.NaN;
            var latestLabel = double.IsFinite(latestValue)
                ? FormatSignedDelta(latestValue)
                : "n/a";
            var color = ResolveSpeciesColor(species.SpeciesId, speciesColors);
            series.Add(new SpeciationLineChartSeriesItem(species.SpeciesId, species.DisplayName, color, path, latestLabel));
            legend.Add(new SpeciationChartLegendItem(species.SpeciesId, species.DisplayName, color, 2d, string.Empty, true));
        }

        if (series.Count == 0)
        {
            return SplitProximityChartSnapshot.Empty("Split proximity (current epoch): (n/a, no drawable species series)");
        }

        var targetEpoch = currentEpochId > 0
            ? currentEpochId
            : epochRows[^1].EpochId;
        var targetRow = epochRows.LastOrDefault(row => row.EpochId == targetEpoch);
        if (targetRow.ValuesBySpecies is null || targetRow.ValuesBySpecies.Count == 0)
        {
            targetRow = epochRows[^1];
        }

        var currentEpochSummary = BuildSplitProximitySummaryLabel(
            targetEpoch,
            targetRow,
            speciesMeta);

        var minEpoch = epochRows[0].EpochId;
        var maxEpoch = epochRows[^1].EpochId;
        var legendColumns = Math.Clamp(series.Count <= 1 ? 2 : series.Count, 2, 4);
        var rangeLabel = useSingleEpochRowSampling
            ? $"Epoch {minEpoch} row samples ({epochRows.Count} samples; most recent {series.Count}/{speciesMeta.Count} species with split-proximity samples)."
            : $"Epochs {minEpoch}..{maxEpoch} ({epochRows.Count} samples; most recent {series.Count}/{speciesMeta.Count} species with split-proximity samples).";
        return new SplitProximityChartSnapshot(
            RangeLabel: rangeLabel,
            MetricLabel: "Min lineage similarity minus effective split threshold per species (signed log10(1+|delta|) y-axis; <=0 means split-trigger zone).",
            YTopLabel: FormatSignedDelta(rawMax),
            YMidLabel: FormatSignedDelta(midRawValue),
            YBottomLabel: FormatSignedDelta(rawMin),
            LegendColumns: legendColumns,
            CurrentEpochSummaryLabel: currentEpochSummary,
            Series: series,
            Legend: legend);
    }

    private static IReadOnlyList<SpeciationEpochSummaryItem> BuildEpochSummaries(IReadOnlyList<SpeciationMembershipRecord> history)
    {
        return history
            .GroupBy(entry => entry.EpochId)
            .OrderBy(group => group.Key)
            .Select(group =>
            {
                var ordered = group
                    .OrderBy(entry => entry.AssignedMs)
                    .ThenBy(entry => entry.SpeciesId, StringComparer.Ordinal)
                    .ToList();
                var firstAssigned = ordered.Count == 0 ? "(n/a)" : FormatTimestamp(ordered[0].AssignedMs);
                var lastAssigned = ordered.Count == 0 ? "(n/a)" : FormatTimestamp(ordered[^1].AssignedMs);
                var speciesCount = ordered
                    .Select(entry => entry.SpeciesId)
                    .Where(speciesId => !string.IsNullOrWhiteSpace(speciesId))
                    .Distinct(StringComparer.Ordinal)
                    .Count();

                return new SpeciationEpochSummaryItem(
                    EpochId: (long)group.Key,
                    MembershipCount: ordered.Count,
                    SpeciesCount: speciesCount,
                    FirstAssigned: $"first {firstAssigned}",
                    LastAssigned: $"last {lastAssigned}");
            })
            .ToList();
    }

    private static CladogramSnapshot BuildCladogramSnapshot(
        IReadOnlyList<SpeciationMembershipRecord> history,
        IReadOnlyDictionary<string, string> speciesColors)
    {
        if (history.Count == 0)
        {
            return CladogramSnapshot.Empty("Cladogram: (no data)");
        }

        var orderedHistory = OrderHistoryForSampling(history);
        var speciesMeta = new Dictionary<string, CladogramSpeciesMeta>(StringComparer.OrdinalIgnoreCase);
        var parentByChild = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var countsBySpecies = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var divergenceEdgeCount = 0;
        foreach (var record in orderedHistory)
        {
            var speciesId = NormalizeSpeciesId(record.SpeciesId);
            var speciesName = BuildCompactSpeciesName(record.SpeciesDisplayName, speciesId);
            if (speciesMeta.TryGetValue(speciesId, out var existingMeta))
            {
                speciesMeta[speciesId] = existingMeta with
                {
                    DisplayName = string.IsNullOrWhiteSpace(existingMeta.DisplayName) ? speciesName : existingMeta.DisplayName
                };
            }
            else
            {
                speciesMeta[speciesId] = new CladogramSpeciesMeta(speciesId, speciesName);
            }

            countsBySpecies.TryGetValue(speciesId, out var priorCount);
            countsBySpecies[speciesId] = priorCount + 1;

            if (!string.Equals(record.DecisionReason, "lineage_diverged_new_species", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!TryExtractLineageParentSpeciesFromMetadata(record.DecisionMetadataJson, out var parentSpeciesId, out var parentSpeciesName))
            {
                continue;
            }

            var parentId = NormalizeSpeciesId(parentSpeciesId);
            if (string.Equals(parentId, speciesId, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!speciesMeta.ContainsKey(parentId))
            {
                speciesMeta[parentId] = new CladogramSpeciesMeta(parentId, BuildCompactSpeciesName(parentSpeciesName, parentId));
            }

            if (!parentByChild.ContainsKey(speciesId))
            {
                parentByChild[speciesId] = parentId;
                divergenceEdgeCount++;
            }
        }

        if (speciesMeta.Count == 0)
        {
            return CladogramSnapshot.Empty("Cladogram: (no data)");
        }

        var childrenByParent = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var (childSpeciesId, parentSpeciesId) in parentByChild)
        {
            if (!childrenByParent.TryGetValue(parentSpeciesId, out var children))
            {
                children = new List<string>();
                childrenByParent[parentSpeciesId] = children;
            }

            children.Add(childSpeciesId);
        }

        foreach (var children in childrenByParent.Values)
        {
            children.Sort((left, right) =>
            {
                countsBySpecies.TryGetValue(left, out var leftCount);
                countsBySpecies.TryGetValue(right, out var rightCount);
                var countComparison = rightCount.CompareTo(leftCount);
                return countComparison != 0
                    ? countComparison
                    : string.Compare(left, right, StringComparison.OrdinalIgnoreCase);
            });
        }

        var roots = speciesMeta.Keys
            .Where(speciesId => !parentByChild.ContainsKey(speciesId))
            .OrderByDescending(speciesId => countsBySpecies.TryGetValue(speciesId, out var count) ? count : 0)
            .ThenBy(speciesId => speciesId, StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (roots.Count == 0)
        {
            roots.AddRange(speciesMeta.Keys.OrderBy(speciesId => speciesId, StringComparer.OrdinalIgnoreCase));
        }

        var items = new List<SpeciationCladogramItem>(roots.Count);
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var root in roots)
        {
            var node = BuildCladogramNode(
                speciesId: root,
                isRoot: true,
                speciesMeta: speciesMeta,
                countsBySpecies: countsBySpecies,
                childrenByParent: childrenByParent,
                visited: visited,
                speciesColors: speciesColors);
            if (node is not null)
            {
                items.Add(node);
            }
        }

        foreach (var speciesId in speciesMeta.Keys.OrderBy(species => species, StringComparer.OrdinalIgnoreCase))
        {
            if (visited.Contains(speciesId))
            {
                continue;
            }

            var disconnectedRoot = BuildCladogramNode(
                speciesId: speciesId,
                isRoot: true,
                speciesMeta: speciesMeta,
                countsBySpecies: countsBySpecies,
                childrenByParent: childrenByParent,
                visited: visited,
                speciesColors: speciesColors);
            if (disconnectedRoot is not null)
            {
                items.Add(disconnectedRoot);
            }
        }

        var rangeLabel = $"Cladogram edges from divergence decisions: {divergenceEdgeCount} across {speciesMeta.Count} species.";
        return new CladogramSnapshot(
            RangeLabel: rangeLabel,
            MetricLabel: "Parent -> child lineage edges inferred from lineage_diverged_new_species decisions.",
            Items: items);
    }

    private static SpeciationCladogramItem? BuildCladogramNode(
        string speciesId,
        bool isRoot,
        IReadOnlyDictionary<string, CladogramSpeciesMeta> speciesMeta,
        IReadOnlyDictionary<string, int> countsBySpecies,
        IReadOnlyDictionary<string, List<string>> childrenByParent,
        ISet<string> visited,
        IReadOnlyDictionary<string, string> speciesColors)
    {
        if (!visited.Add(speciesId))
        {
            return null;
        }

        if (!speciesMeta.TryGetValue(speciesId, out var meta))
        {
            meta = new CladogramSpeciesMeta(speciesId, speciesId);
        }

        countsBySpecies.TryGetValue(speciesId, out var count);
        var childNodes = new List<SpeciationCladogramItem>();
        if (childrenByParent.TryGetValue(speciesId, out var children) && children.Count > 0)
        {
            for (var index = 0; index < children.Count; index++)
            {
                var childNode = BuildCladogramNode(
                    speciesId: children[index],
                    isRoot: false,
                    speciesMeta: speciesMeta,
                    countsBySpecies: countsBySpecies,
                    childrenByParent: childrenByParent,
                    visited: visited,
                    speciesColors: speciesColors);
                if (childNode is not null)
                {
                    childNodes.Add(childNode);
                }
            }
        }

        var detailLabel = $"members {count} | direct derived {childNodes.Count}";
        return new SpeciationCladogramItem(
            speciesId: speciesId,
            speciesDisplayName: meta.DisplayName,
            detailLabel: detailLabel,
            color: ResolveSpeciesColor(speciesId, speciesColors),
            isRoot: isRoot,
            children: childNodes);
    }

    private static IReadOnlyDictionary<string, string> BuildSpeciesColorMap(
        IReadOnlyList<SpeciationMembershipRecord> history,
        IReadOnlyDictionary<string, string> overrides)
    {
        if (history.Count == 0)
        {
            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        var orderedHistory = OrderHistoryForSampling(history);
        var speciesColors = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var recentColors = new Queue<string>(SpeciesColorRecentWindow);
        var nextPaletteIndex = 0;
        foreach (var record in orderedHistory)
        {
            var speciesId = NormalizeSpeciesId(record.SpeciesId);
            if (speciesColors.ContainsKey(speciesId))
            {
                continue;
            }

            var color = TakeNextSpeciesPaletteColor(nextPaletteIndex, recentColors, out nextPaletteIndex);
            speciesColors[speciesId] = color;
            recentColors.Enqueue(color);
            if (recentColors.Count > SpeciesColorRecentWindow)
            {
                recentColors.Dequeue();
            }
        }

        foreach (var entry in overrides)
        {
            var speciesId = NormalizeSpeciesId(entry.Key);
            var colorHex = NormalizeHexColor(entry.Value);
            if (!string.IsNullOrWhiteSpace(speciesId) && !string.IsNullOrWhiteSpace(colorHex))
            {
                speciesColors[speciesId] = colorHex;
            }
        }

        return speciesColors;
    }

    private static IReadOnlyList<SpeciationColorPickerSwatchItem> BuildSpeciesColorPickerPalette()
        => SpeciesChartPalette
            .Select(NormalizeHexColor)
            .Where(colorHex => !string.IsNullOrWhiteSpace(colorHex))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Take(Math.Max(SpeciesColorPickerOptionCount, SpeciesChartPalette.Length))
            .Select(colorHex => new SpeciationColorPickerSwatchItem(colorHex))
            .ToArray();

    private static bool TryExtractLineageParentSpeciesFromMetadata(
        string? metadataJson,
        out string speciesId,
        out string speciesDisplayName)
    {
        speciesId = string.Empty;
        speciesDisplayName = string.Empty;
        if (string.IsNullOrWhiteSpace(metadataJson))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(metadataJson);
            var root = document.RootElement;
            if (!root.TryGetProperty("lineage", out var lineage))
            {
                return false;
            }

            if (!TryGetJsonString(lineage, "source_species_id", out speciesId)
                && !TryGetJsonString(lineage, "sourceSpeciesId", out speciesId)
                && !TryGetJsonString(lineage, "dominant_species_id", out speciesId)
                && !TryGetJsonString(lineage, "dominantSpeciesId", out speciesId))
            {
                return false;
            }

            TryGetJsonString(lineage, "source_species_display_name", out speciesDisplayName);
            if (string.IsNullOrWhiteSpace(speciesDisplayName))
            {
                TryGetJsonString(lineage, "sourceSpeciesDisplayName", out speciesDisplayName);
            }

            if (string.IsNullOrWhiteSpace(speciesDisplayName))
            {
                TryGetJsonString(lineage, "dominant_species_display_name", out speciesDisplayName);
            }

            if (string.IsNullOrWhiteSpace(speciesDisplayName))
            {
                TryGetJsonString(lineage, "dominantSpeciesDisplayName", out speciesDisplayName);
            }

            return !string.IsNullOrWhiteSpace(speciesId);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static List<EpochSplitProximityRow> BuildAggregatedEpochSplitProximityRows(
        IReadOnlyList<SpeciationMembershipRecord> history,
        double fallbackSplitThreshold,
        double fallbackSplitGuardMargin,
        Dictionary<string, SplitProximitySpeciesMeta> speciesMeta)
    {
        return history
            .GroupBy(entry => (long)entry.EpochId)
            .OrderBy(group => group.Key)
            .Select(group =>
            {
                var bySpecies = new Dictionary<string, SplitProximitySpeciesPoint>(StringComparer.OrdinalIgnoreCase);
                foreach (var record in group)
                {
                    if (!TryExtractSplitProximity(
                            record.DecisionMetadataJson,
                            fallbackSplitThreshold,
                            fallbackSplitGuardMargin,
                            out var similarity,
                            out var splitThreshold,
                            out var proximity))
                    {
                        continue;
                    }

                    var speciesId = NormalizeSpeciesId(record.SpeciesId);
                    var speciesName = BuildCompactSpeciesName(record.SpeciesDisplayName, speciesId);
                    if (speciesMeta.TryGetValue(speciesId, out var existingMeta))
                    {
                        speciesMeta[speciesId] = existingMeta with
                        {
                            DisplayName = string.IsNullOrWhiteSpace(existingMeta.DisplayName) ? speciesName : existingMeta.DisplayName,
                            SampleCount = existingMeta.SampleCount + 1,
                            LatestAssignedMs = Math.Max(existingMeta.LatestAssignedMs, record.AssignedMs)
                        };
                    }
                    else
                    {
                        speciesMeta[speciesId] = new SplitProximitySpeciesMeta(speciesId, speciesName, 1, record.AssignedMs);
                    }

                    if (bySpecies.TryGetValue(speciesId, out var existingPoint))
                    {
                        var nextCount = existingPoint.SampleCount + 1;
                        if (proximity < existingPoint.MinProximity)
                        {
                            bySpecies[speciesId] = new SplitProximitySpeciesPoint(proximity, similarity, splitThreshold, nextCount);
                        }
                        else
                        {
                            bySpecies[speciesId] = existingPoint with { SampleCount = nextCount };
                        }
                    }
                    else
                    {
                        bySpecies[speciesId] = new SplitProximitySpeciesPoint(
                            MinProximity: proximity,
                            MinSimilarity: similarity,
                            SplitThreshold: splitThreshold,
                            SampleCount: 1);
                    }
                }

                return new EpochSplitProximityRow(group.Key, bySpecies);
            })
            .ToList();
    }

    private static List<EpochSplitProximityRow> BuildSingleEpochSplitProximityRows(
        IReadOnlyList<SpeciationMembershipRecord> orderedHistory,
        double fallbackSplitThreshold,
        double fallbackSplitGuardMargin,
        Dictionary<string, SplitProximitySpeciesMeta> speciesMeta)
    {
        var rows = new List<EpochSplitProximityRow>(orderedHistory.Count);
        var rollingBySpecies = new Dictionary<string, SplitProximitySpeciesPoint>(StringComparer.OrdinalIgnoreCase);
        foreach (var record in orderedHistory)
        {
            if (TryExtractSplitProximity(
                    record.DecisionMetadataJson,
                    fallbackSplitThreshold,
                    fallbackSplitGuardMargin,
                    out var similarity,
                    out var splitThreshold,
                    out var proximity))
            {
                var speciesId = NormalizeSpeciesId(record.SpeciesId);
                var speciesName = BuildCompactSpeciesName(record.SpeciesDisplayName, speciesId);
                if (speciesMeta.TryGetValue(speciesId, out var existingMeta))
                {
                    speciesMeta[speciesId] = existingMeta with
                    {
                        DisplayName = string.IsNullOrWhiteSpace(existingMeta.DisplayName) ? speciesName : existingMeta.DisplayName,
                        SampleCount = existingMeta.SampleCount + 1,
                        LatestAssignedMs = Math.Max(existingMeta.LatestAssignedMs, record.AssignedMs)
                    };
                }
                else
                {
                    speciesMeta[speciesId] = new SplitProximitySpeciesMeta(speciesId, speciesName, 1, record.AssignedMs);
                }

                if (rollingBySpecies.TryGetValue(speciesId, out var existingPoint))
                {
                    var nextCount = existingPoint.SampleCount + 1;
                    if (proximity < existingPoint.MinProximity)
                    {
                        rollingBySpecies[speciesId] = new SplitProximitySpeciesPoint(proximity, similarity, splitThreshold, nextCount);
                    }
                    else
                    {
                        rollingBySpecies[speciesId] = existingPoint with { SampleCount = nextCount };
                    }
                }
                else
                {
                    rollingBySpecies[speciesId] = new SplitProximitySpeciesPoint(
                        MinProximity: proximity,
                        MinSimilarity: similarity,
                        SplitThreshold: splitThreshold,
                        SampleCount: 1);
                }
            }

            if (rollingBySpecies.Count > 0)
            {
                rows.Add(
                    new EpochSplitProximityRow(
                        EpochId: (long)record.EpochId,
                        ValuesBySpecies: new Dictionary<string, SplitProximitySpeciesPoint>(rollingBySpecies, StringComparer.OrdinalIgnoreCase)));
            }
        }

        return rows;
    }

    private static DivergenceSnapshot BuildCurrentEpochDivergenceSnapshot(
        IReadOnlyList<SpeciationMembershipRecord> history,
        long currentEpochId)
    {
        if (history.Count == 0)
        {
            return new DivergenceSnapshot("Max within-species divergence (current epoch): (n/a)");
        }

        var targetEpoch = currentEpochId > 0
            ? currentEpochId
            : history.Max(entry => (long)entry.EpochId);
        double? maxDivergence = null;
        double? minSimilarity = null;
        string maxBrainLabel = "(unknown)";
        var sampleCount = 0;

        foreach (var record in history.Where(entry => (long)entry.EpochId == targetEpoch))
        {
            if (!TryExtractAssignedSpeciesSimilarityScore(record.DecisionMetadataJson, out var similarity))
            {
                continue;
            }

            var boundedSimilarity = Clamp01(similarity);
            var divergence = 1d - boundedSimilarity;
            sampleCount++;
            if (!minSimilarity.HasValue || boundedSimilarity < minSimilarity.Value)
            {
                minSimilarity = boundedSimilarity;
            }

            if (maxDivergence.HasValue && divergence <= maxDivergence.Value)
            {
                continue;
            }

            maxDivergence = divergence;
            maxBrainLabel = record.BrainId?.TryToGuid(out var brainId) == true && brainId != Guid.Empty
                ? brainId.ToString("D")
                : "(none)";
        }

        if (!maxDivergence.HasValue || !minSimilarity.HasValue)
        {
            return new DivergenceSnapshot($"Max within-species divergence (epoch {targetEpoch}): (n/a, no similarity scores)");
        }

        var label =
            $"Max within-species divergence (epoch {targetEpoch}) = {maxDivergence.Value:0.###} (min assigned-species similarity {minSimilarity.Value:0.###}, samples={sampleCount}, brain={maxBrainLabel}).";
        return new DivergenceSnapshot(label);
    }

    private static bool TryExtractAssignedSpeciesSimilarityScore(string? metadataJson, out double similarityScore)
    {
        similarityScore = 0d;
        if (string.IsNullOrWhiteSpace(metadataJson))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(metadataJson);
            var root = document.RootElement;
            if (root.TryGetProperty("lineage", out var lineage)
                && TryGetSimilarityFromElement(
                    lineage,
                    out similarityScore,
                    "intra_species_similarity_sample",
                    "intraSpeciesSimilaritySample",
                    "lineage_assignment_similarity_score",
                    "lineageAssignmentSimilarityScore",
                    "dominant_species_similarity_score",
                    "dominantSpeciesSimilarityScore",
                    "lineage_similarity_score",
                    "lineageSimilarityScore",
                    "similarity_score",
                    "similarityScore"))
            {
                return true;
            }

            if (TryGetSimilarityFromElement(
                    root,
                    out similarityScore,
                    "intra_species_similarity_sample",
                    "intraSpeciesSimilaritySample",
                    "lineage_assignment_similarity_score",
                    "lineageAssignmentSimilarityScore",
                    "dominant_species_similarity_score",
                    "dominantSpeciesSimilarityScore",
                    "lineage_similarity_score",
                    "lineageSimilarityScore"))
            {
                return true;
            }

            if (root.TryGetProperty("report", out var report) && TryGetSimilarityFromElement(report, out similarityScore))
            {
                return true;
            }

            if (root.TryGetProperty("scores", out var scores) && TryGetSimilarityFromElement(scores, out similarityScore))
            {
                return true;
            }
        }
        catch (JsonException)
        {
        }

        return false;
    }

    private static bool TryExtractSourceSpeciesSimilarityScore(string? metadataJson, out double similarityScore)
    {
        similarityScore = 0d;
        if (string.IsNullOrWhiteSpace(metadataJson))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(metadataJson);
            var root = document.RootElement;
            if (root.TryGetProperty("lineage", out var lineage)
                && TryGetSimilarityFromElement(
                    lineage,
                    out similarityScore,
                    "source_species_similarity_score",
                    "sourceSpeciesSimilarityScore",
                    "dominant_species_similarity_score",
                    "dominantSpeciesSimilarityScore",
                    "lineage_similarity_score",
                    "lineageSimilarityScore",
                    "similarity_score",
                    "similarityScore"))
            {
                return true;
            }

            if (TryGetSimilarityFromElement(
                    root,
                    out similarityScore,
                    "source_species_similarity_score",
                    "sourceSpeciesSimilarityScore",
                    "dominant_species_similarity_score",
                    "dominantSpeciesSimilarityScore",
                    "lineage_similarity_score",
                    "lineageSimilarityScore"))
            {
                return true;
            }

            if (root.TryGetProperty("report", out var report) && TryGetSimilarityFromElement(report, out similarityScore))
            {
                return true;
            }

            if (root.TryGetProperty("scores", out var scores) && TryGetSimilarityFromElement(scores, out similarityScore))
            {
                return true;
            }
        }
        catch (JsonException)
        {
        }

        return false;
    }

    private static bool TryGetSimilarityFromElement(
        JsonElement element,
        out double similarityScore,
        params string[] propertyNames)
    {
        similarityScore = 0d;
        if (propertyNames.Length == 0)
        {
            propertyNames = ["similarity_score", "similarityScore"];
        }

        foreach (var propertyName in propertyNames)
        {
            if (TryGetJsonDouble(element, propertyName, out similarityScore))
            {
                return true;
            }
        }

        if (element.TryGetProperty("scores", out var scores)
            && (TryGetJsonDouble(scores, propertyNames[0], out similarityScore)
                || (propertyNames.Length > 1 && TryGetJsonDouble(scores, propertyNames[1], out similarityScore))
                || (propertyNames.Length > 2 && TryGetJsonDouble(scores, propertyNames[2], out similarityScore))
                || (propertyNames.Length > 3 && TryGetJsonDouble(scores, propertyNames[3], out similarityScore))))
        {
            return true;
        }

        return false;
    }

    private static bool TryGetJsonDouble(JsonElement element, string propertyName, out double value)
    {
        value = 0d;
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return false;
        }

        return property.ValueKind switch
        {
            JsonValueKind.Number when property.TryGetDouble(out value) => true,
            JsonValueKind.String when double.TryParse(
                property.GetString(),
                NumberStyles.Float,
                CultureInfo.InvariantCulture,
                out value) => true,
            _ => false
        };
    }

    private static bool TryGetJsonString(JsonElement element, string propertyName, out string value)
    {
        value = string.Empty;
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return false;
        }

        var raw = property.ValueKind switch
        {
            JsonValueKind.String => property.GetString(),
            JsonValueKind.Number => property.GetRawText(),
            _ => null
        };
        if (string.IsNullOrWhiteSpace(raw))
        {
            return false;
        }

        value = raw.Trim();
        return value.Length > 0;
    }

    private static bool TryExtractSplitProximity(
        string? metadataJson,
        double fallbackSplitThreshold,
        double fallbackSplitGuardMargin,
        out double similarity,
        out double splitThreshold,
        out double proximity)
    {
        similarity = 0d;
        splitThreshold = Clamp01(fallbackSplitThreshold);
        var splitGuardMargin = Clamp01(fallbackSplitGuardMargin);
        var hasExplicitEffectiveSplitThreshold = false;
        var hasExplicitProximity = false;
        proximity = 0d;
        if (!string.IsNullOrWhiteSpace(metadataJson))
        {
            try
            {
                using var document = JsonDocument.Parse(metadataJson);
                var root = document.RootElement;
                if (root.TryGetProperty("lineage", out var lineage))
                {
                    if (TryGetJsonDouble(lineage, "assigned_split_proximity_to_dynamic_threshold", out proximity)
                        || TryGetJsonDouble(lineage, "assignedSplitProximityToDynamicThreshold", out proximity)
                        || TryGetJsonDouble(lineage, "split_proximity_to_dynamic_threshold", out proximity)
                        || TryGetJsonDouble(lineage, "splitProximityToDynamicThreshold", out proximity)
                        || TryGetJsonDouble(lineage, "source_split_proximity_to_dynamic_threshold", out proximity)
                        || TryGetJsonDouble(lineage, "sourceSplitProximityToDynamicThreshold", out proximity))
                    {
                        hasExplicitProximity = true;
                        hasExplicitEffectiveSplitThreshold = true;
                    }
                    else if (TryGetJsonDouble(lineage, "assigned_split_proximity_to_policy_threshold", out proximity)
                             || TryGetJsonDouble(lineage, "assignedSplitProximityToPolicyThreshold", out proximity)
                             || TryGetJsonDouble(lineage, "split_proximity_to_policy_threshold", out proximity)
                             || TryGetJsonDouble(lineage, "splitProximityToPolicyThreshold", out proximity)
                             || TryGetJsonDouble(lineage, "source_split_proximity_to_policy_threshold", out proximity)
                             || TryGetJsonDouble(lineage, "sourceSplitProximityToPolicyThreshold", out proximity))
                    {
                        hasExplicitProximity = true;
                    }
                }
                else if (TryGetJsonDouble(root, "assigned_split_proximity_to_dynamic_threshold", out proximity)
                         || TryGetJsonDouble(root, "assignedSplitProximityToDynamicThreshold", out proximity)
                         || TryGetJsonDouble(root, "split_proximity_to_dynamic_threshold", out proximity)
                         || TryGetJsonDouble(root, "splitProximityToDynamicThreshold", out proximity)
                         || TryGetJsonDouble(root, "source_split_proximity_to_dynamic_threshold", out proximity)
                         || TryGetJsonDouble(root, "sourceSplitProximityToDynamicThreshold", out proximity))
                {
                    hasExplicitProximity = true;
                    hasExplicitEffectiveSplitThreshold = true;
                }
                else if (TryGetJsonDouble(root, "assigned_split_proximity_to_policy_threshold", out proximity)
                         || TryGetJsonDouble(root, "assignedSplitProximityToPolicyThreshold", out proximity)
                         || TryGetJsonDouble(root, "split_proximity_to_policy_threshold", out proximity)
                         || TryGetJsonDouble(root, "splitProximityToPolicyThreshold", out proximity)
                         || TryGetJsonDouble(root, "source_split_proximity_to_policy_threshold", out proximity)
                         || TryGetJsonDouble(root, "sourceSplitProximityToPolicyThreshold", out proximity))
                {
                    hasExplicitProximity = true;
                }

                if (root.TryGetProperty("assignment_policy", out var policy)
                    && (TryGetJsonDouble(policy, "lineage_assigned_dynamic_split_threshold", out splitThreshold)
                        || TryGetJsonDouble(policy, "lineageAssignedDynamicSplitThreshold", out splitThreshold)
                        || TryGetJsonDouble(policy, "lineage_dynamic_split_threshold", out splitThreshold)
                        || TryGetJsonDouble(policy, "lineageDynamicSplitThreshold", out splitThreshold)
                        || TryGetJsonDouble(policy, "lineage_source_dynamic_split_threshold", out splitThreshold)
                        || TryGetJsonDouble(policy, "lineageSourceDynamicSplitThreshold", out splitThreshold)
                        || TryGetJsonDouble(policy, "lineage_split_threshold", out splitThreshold)
                        || TryGetJsonDouble(policy, "lineageSplitThreshold", out splitThreshold)))
                {
                    splitThreshold = Clamp01(splitThreshold);
                    hasExplicitEffectiveSplitThreshold =
                        TryGetJsonDouble(policy, "lineage_assigned_dynamic_split_threshold", out _)
                        || TryGetJsonDouble(policy, "lineageAssignedDynamicSplitThreshold", out _)
                        || TryGetJsonDouble(policy, "lineage_dynamic_split_threshold", out _)
                        || TryGetJsonDouble(policy, "lineageDynamicSplitThreshold", out _)
                        || TryGetJsonDouble(policy, "lineage_source_dynamic_split_threshold", out _)
                        || TryGetJsonDouble(policy, "lineageSourceDynamicSplitThreshold", out _);
                    if (TryGetJsonDouble(policy, "lineage_split_guard_margin", out var policySplitGuardMargin)
                        || TryGetJsonDouble(policy, "lineageSplitGuardMargin", out policySplitGuardMargin))
                    {
                        splitGuardMargin = Clamp01(policySplitGuardMargin);
                    }
                }
                else if (TryGetJsonDouble(root, "lineage_dynamic_split_threshold", out var rootDynamicSplitThreshold)
                         || TryGetJsonDouble(root, "lineageDynamicSplitThreshold", out rootDynamicSplitThreshold))
                {
                    splitThreshold = Clamp01(rootDynamicSplitThreshold);
                    hasExplicitEffectiveSplitThreshold = true;
                }
                else if (TryGetJsonDouble(root, "lineage_split_threshold", out var rootSplitThreshold)
                         || TryGetJsonDouble(root, "lineageSplitThreshold", out rootSplitThreshold))
                {
                    splitThreshold = Clamp01(rootSplitThreshold);
                    if (TryGetJsonDouble(root, "lineage_split_guard_margin", out var rootSplitGuardMargin)
                        || TryGetJsonDouble(root, "lineageSplitGuardMargin", out rootSplitGuardMargin))
                    {
                        splitGuardMargin = Clamp01(rootSplitGuardMargin);
                    }
                }
            }
            catch (JsonException)
            {
                // Fallback split threshold is used when metadata is malformed.
            }
        }

        if (!hasExplicitEffectiveSplitThreshold)
        {
            splitThreshold = Math.Max(0d, splitThreshold - splitGuardMargin);
        }

        if (hasExplicitProximity)
        {
            similarity = Clamp01(splitThreshold + proximity);
            return true;
        }

        if (!TryExtractAssignedSpeciesSimilarityScore(metadataJson, out similarity)
            && !TryExtractSourceSpeciesSimilarityScore(metadataJson, out similarity))
        {
            return false;
        }

        similarity = Clamp01(similarity);
        proximity = similarity - splitThreshold;
        return true;
    }

    private static bool TryGetLatestFiniteValue(IReadOnlyList<double> values, out double latest)
    {
        for (var index = values.Count - 1; index >= 0; index--)
        {
            if (double.IsFinite(values[index]))
            {
                latest = values[index];
                return true;
            }
        }

        latest = double.NaN;
        return false;
    }

    private static string BuildSplitProximitySummaryLabel(
        long targetEpoch,
        EpochSplitProximityRow row,
        IReadOnlyDictionary<string, SplitProximitySpeciesMeta> speciesMeta)
    {
        if (row.ValuesBySpecies is null || row.ValuesBySpecies.Count == 0)
        {
            return $"Split proximity (epoch {targetEpoch}): (n/a, no similarity scores)";
        }

        var selected = row.ValuesBySpecies
            .OrderBy(item => item.Value.MinProximity)
            .ThenBy(item => item.Key, StringComparer.OrdinalIgnoreCase)
            .First();
        var speciesId = selected.Key;
        var speciesName = speciesMeta.TryGetValue(speciesId, out var meta)
            ? meta.DisplayName
            : speciesId;
        var point = selected.Value;
        return
            $"Split proximity (epoch {targetEpoch}) min={FormatSignedDelta(point.MinProximity)} in {speciesName} " +
            $"[assignment sim {point.MinSimilarity:0.###} vs effective split {point.SplitThreshold:0.###}, samples={point.SampleCount}].";
    }

    private static string FormatSimilarityRange(ulong samples, double? min, double? max)
    {
        if (samples == 0 || !min.HasValue || !max.HasValue)
        {
            return "n/a";
        }

        return $"{min.Value:0.###}..{max.Value:0.###} ({samples.ToString(CultureInfo.InvariantCulture)})";
    }

    private static string BuildLinePath(
        IReadOnlyList<double> values,
        double yMin,
        double yMax,
        double plotWidth,
        double plotHeight,
        double paddingX,
        double paddingY)
    {
        if (values.Count == 0)
        {
            return string.Empty;
        }

        var boundedMin = double.IsFinite(yMin) ? yMin : 0d;
        var boundedMax = double.IsFinite(yMax) ? yMax : 1d;
        if (!(boundedMax > boundedMin))
        {
            boundedMax = boundedMin + 1d;
        }

        var usableWidth = Math.Max(1d, plotWidth - (paddingX * 2d));
        var usableHeight = Math.Max(1d, plotHeight - (paddingY * 2d));
        var xStep = values.Count > 1 ? usableWidth / (values.Count - 1) : 0d;
        var builder = new StringBuilder(values.Count * 26);
        var hasPoint = false;
        var finitePointCount = 0;
        var firstX = 0d;
        var firstY = 0d;
        var segmentOpen = false;
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (!double.IsFinite(value))
            {
                segmentOpen = false;
                continue;
            }

            var x = paddingX + (i * xStep);
            var ratio = Math.Clamp((value - boundedMin) / (boundedMax - boundedMin), 0d, 1d);
            var y = paddingY + ((1d - ratio) * usableHeight);
            builder.Append(segmentOpen ? " L " : "M ");
            builder.Append(x.ToString("0.###", CultureInfo.InvariantCulture));
            builder.Append(' ');
            builder.Append(y.ToString("0.###", CultureInfo.InvariantCulture));
            if (finitePointCount == 0)
            {
                firstX = x;
                firstY = y;
            }

            finitePointCount++;
            segmentOpen = true;
            hasPoint = true;
        }

        if (!hasPoint)
        {
            return string.Empty;
        }

        if (finitePointCount == 1)
        {
            var halfWidth = Math.Clamp(usableWidth * 0.01d, 1.5d, 6d);
            var minX = paddingX;
            var maxX = paddingX + usableWidth;
            var startX = Math.Max(minX, firstX - halfWidth);
            var endX = Math.Min(maxX, firstX + halfWidth);
            if (endX <= startX)
            {
                endX = Math.Min(maxX, startX + 1d);
            }

            return string.Create(
                CultureInfo.InvariantCulture,
                $"M {startX:0.###} {firstY:0.###} L {endX:0.###} {firstY:0.###}");
        }

        return builder.ToString();
    }

    private static string BuildFlowAreaPath(
        IReadOnlyList<double> starts,
        IReadOnlyList<double> ends,
        double plotWidth,
        double plotHeight,
        double paddingX,
        double paddingY)
    {
        if (starts.Count == 0 || starts.Count != ends.Count)
        {
            return string.Empty;
        }

        var validIndices = new List<int>(starts.Count);
        var hasArea = false;
        for (var i = 0; i < starts.Count; i++)
        {
            if (!double.IsFinite(starts[i]) || !double.IsFinite(ends[i]))
            {
                continue;
            }

            validIndices.Add(i);
            if ((ends[i] - starts[i]) > 1e-6d)
            {
                hasArea = true;
            }
        }

        if (!hasArea || validIndices.Count == 0)
        {
            return string.Empty;
        }

        var usableWidth = Math.Max(1d, plotWidth - (paddingX * 2d));
        var usableHeight = Math.Max(1d, plotHeight - (paddingY * 2d));
        var yStep = starts.Count > 1 ? usableHeight / (starts.Count - 1) : 0d;
        var builder = new StringBuilder(validIndices.Count * 48);
        for (var pointIndex = 0; pointIndex < validIndices.Count; pointIndex++)
        {
            var i = validIndices[pointIndex];
            var x = paddingX + (Math.Clamp(ends[i], 0d, 1d) * usableWidth);
            var y = paddingY + (i * yStep);
            builder.Append(pointIndex == 0 ? "M " : " L ");
            builder.Append(x.ToString("0.###", CultureInfo.InvariantCulture));
            builder.Append(' ');
            builder.Append(y.ToString("0.###", CultureInfo.InvariantCulture));
        }

        for (var pointIndex = validIndices.Count - 1; pointIndex >= 0; pointIndex--)
        {
            var i = validIndices[pointIndex];
            var x = paddingX + (Math.Clamp(starts[i], 0d, 1d) * usableWidth);
            var y = paddingY + (i * yStep);
            builder.Append(" L ");
            builder.Append(x.ToString("0.###", CultureInfo.InvariantCulture));
            builder.Append(' ');
            builder.Append(y.ToString("0.###", CultureInfo.InvariantCulture));
        }

        builder.Append(" Z");
        return builder.ToString();
    }

    private static string ResolveSpeciesColor(string speciesId, IReadOnlyDictionary<string, string> speciesColors)
    {
        var normalizedSpeciesId = NormalizeSpeciesId(speciesId);
        return speciesColors.TryGetValue(normalizedSpeciesId, out var mappedColor) && !string.IsNullOrWhiteSpace(mappedColor)
            ? mappedColor
            : ResolveSpeciesColor(normalizedSpeciesId);
    }

    private static string ResolveSpeciesColor(string speciesId)
    {
        if (string.IsNullOrWhiteSpace(speciesId))
        {
            return NormalizeHexColor(SpeciesChartPalette[0]);
        }

        var hash = ComputeSpeciesColorHash(speciesId);
        var paletteIndex = (int)(hash % (uint)SpeciesChartPalette.Length);
        return NormalizeHexColor(SpeciesChartPalette[paletteIndex]);
    }

    private static string ResolveSpeciesColor(string speciesId, IEnumerable<string> recentColors)
    {
        if (string.IsNullOrWhiteSpace(speciesId))
        {
            return NormalizeHexColor(SpeciesChartPalette[0]);
        }

        var hash = ComputeSpeciesColorHash(speciesId);
        var startIndex = (int)(hash % (uint)SpeciesChartPalette.Length);
        return TakeNextSpeciesPaletteColor(startIndex, recentColors, out _);
    }

    private static string TakeNextSpeciesPaletteColor(int startIndex, IEnumerable<string> recentColors, out int nextPaletteIndex)
    {
        var guardedColors = recentColors
            .Where(color => !string.IsNullOrWhiteSpace(color))
            .Take(SpeciesColorRecentWindow)
            .Select(NormalizeHexColor)
            .Where(color => !string.IsNullOrWhiteSpace(color))
            .ToArray();
        for (var attempt = 0; attempt < SpeciesChartPalette.Length; attempt++)
        {
            var paletteIndex = (startIndex + attempt) % SpeciesChartPalette.Length;
            var candidate = NormalizeHexColor(SpeciesChartPalette[paletteIndex]);
            if (guardedColors.All(guardedColor => !AreColorsTooSimilar(candidate, guardedColor)))
            {
                nextPaletteIndex = (paletteIndex + 1) % SpeciesChartPalette.Length;
                return candidate;
            }
        }

        var fallbackIndex = ((startIndex % SpeciesChartPalette.Length) + SpeciesChartPalette.Length) % SpeciesChartPalette.Length;
        nextPaletteIndex = (fallbackIndex + 1) % SpeciesChartPalette.Length;
        return NormalizeHexColor(SpeciesChartPalette[fallbackIndex]);
    }

    private static uint ComputeSpeciesColorHash(string speciesId)
    {
        unchecked
        {
            uint hash = 2166136261;
            foreach (var ch in speciesId)
            {
                hash ^= ch;
                hash *= 16777619;
            }

            return hash;
        }
    }

    private static bool AreColorsTooSimilar(string leftHex, string rightHex)
    {
        if (!TryParseHexColor(leftHex, out var left) || !TryParseHexColor(rightHex, out var right))
        {
            return string.Equals(leftHex, rightHex, StringComparison.OrdinalIgnoreCase);
        }

        var redDelta = left.Red - right.Red;
        var greenDelta = left.Green - right.Green;
        var blueDelta = left.Blue - right.Blue;
        var distance = Math.Sqrt((redDelta * redDelta) + (greenDelta * greenDelta) + (blueDelta * blueDelta));
        return distance < AdjacentSpeciesColorMinDistance;
    }

    private static bool TryParseHexColor(string value, out (int Red, int Green, int Blue) color)
    {
        color = default;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var normalized = value.Trim();
        if (normalized.Length != 7 || normalized[0] != '#')
        {
            return false;
        }

        if (!int.TryParse(normalized.AsSpan(1, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var red)
            || !int.TryParse(normalized.AsSpan(3, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var green)
            || !int.TryParse(normalized.AsSpan(5, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var blue))
        {
            return false;
        }

        color = (red, green, blue);
        return true;
    }

    private static string HslToHex(double hue, double saturation, double lightness)
    {
        hue = hue - Math.Floor(hue);
        saturation = Math.Clamp(saturation, 0d, 1d);
        lightness = Math.Clamp(lightness, 0d, 1d);

        if (saturation <= 1e-6d)
        {
            var gray = ToByte(lightness);
            return $"#{gray:X2}{gray:X2}{gray:X2}";
        }

        var q = lightness < 0.5d
            ? lightness * (1d + saturation)
            : lightness + saturation - (lightness * saturation);
        var p = (2d * lightness) - q;

        var r = HueToRgb(p, q, hue + (1d / 3d));
        var g = HueToRgb(p, q, hue);
        var b = HueToRgb(p, q, hue - (1d / 3d));
        return $"#{ToByte(r):X2}{ToByte(g):X2}{ToByte(b):X2}";
    }

    private static double HueToRgb(double p, double q, double t)
    {
        if (t < 0d)
        {
            t += 1d;
        }
        else if (t > 1d)
        {
            t -= 1d;
        }

        if (t < (1d / 6d))
        {
            return p + ((q - p) * 6d * t);
        }

        if (t < 0.5d)
        {
            return q;
        }

        if (t < (2d / 3d))
        {
            return p + ((q - p) * ((2d / 3d) - t) * 6d);
        }

        return p;
    }

    private static byte ToByte(double value)
    {
        var clamped = Math.Clamp(value, 0d, 1d);
        return (byte)Math.Round(clamped * 255d, MidpointRounding.AwayFromZero);
    }

    private static string NormalizeHexColor(string colorHex)
    {
        if (string.IsNullOrWhiteSpace(colorHex))
        {
            return string.Empty;
        }

        var normalized = colorHex.Trim();
        if (normalized.StartsWith('#'))
        {
            normalized = normalized[1..];
        }

        return normalized.Length switch
        {
            6 => $"#{normalized.ToUpperInvariant()}",
            8 => $"#{normalized.ToUpperInvariant()}",
            _ => string.Empty
        };
    }

    private static string WithAlpha(string colorHex, byte alpha)
    {
        if (string.IsNullOrWhiteSpace(colorHex))
        {
            return $"#{alpha:X2}808080";
        }

        var normalized = colorHex.Trim();
        if (normalized.StartsWith('#'))
        {
            normalized = normalized[1..];
        }

        if (normalized.Length == 8)
        {
            return $"#{alpha:X2}{normalized[2..]}";
        }

        if (normalized.Length == 6)
        {
            return $"#{alpha:X2}{normalized}";
        }

        return colorHex;
    }

    private static string FormatAxisValue(double value)
    {
        if (!double.IsFinite(value))
        {
            return "n/a";
        }

        var abs = Math.Abs(value);
        if (abs >= 1_000_000_000d)
        {
            return $"{value / 1_000_000_000d:0.##}B";
        }

        if (abs >= 1_000_000d)
        {
            return $"{value / 1_000_000d:0.##}M";
        }

        if (abs >= 1_000d)
        {
            return $"{value / 1_000d:0.##}K";
        }

        return value.ToString("0.###", CultureInfo.InvariantCulture);
    }

    private static string FormatSignedDelta(double value)
    {
        if (!double.IsFinite(value))
        {
            return "n/a";
        }

        return value.ToString("+0.###;-0.###;0", CultureInfo.InvariantCulture);
    }

    private static double TransformSignedLogOrNan(double value)
    {
        if (!double.IsFinite(value))
        {
            return double.NaN;
        }

        return TransformSignedLog(value);
    }

    private static double TransformSignedLog(double value)
    {
        if (!double.IsFinite(value))
        {
            return 0d;
        }

        var magnitude = Math.Abs(value);
        if (magnitude <= 0d)
        {
            return 0d;
        }

        var transformedMagnitude = Math.Log10(1d + magnitude);
        return value < 0d ? -transformedMagnitude : transformedMagnitude;
    }

    private static string NormalizeSpeciesId(string? speciesId)
        => string.IsNullOrWhiteSpace(speciesId) ? "(unknown)" : speciesId.Trim();

    private static string NormalizeSpeciesName(string? speciesName, string speciesId)
        => string.IsNullOrWhiteSpace(speciesName) ? speciesId : speciesName.Trim();

    private static string BuildCompactSpeciesName(string? speciesName, string? speciesId)
    {
        var normalizedId = NormalizeSpeciesId(speciesId);
        var lineageCodeLabel = TryExtractLineageCodeLabel(speciesName);
        if (lineageCodeLabel.Length > 0)
        {
            return lineageCodeLabel;
        }

        if (!string.IsNullOrWhiteSpace(speciesName))
        {
            return speciesName.Trim();
        }

        var tokens = normalizedId
            .Split(['.', '-', '_', '/', ':'], StringSplitOptions.RemoveEmptyEntries)
            .ToList();
        if (tokens.Count > 1 && string.Equals(tokens[0], "species", StringComparison.OrdinalIgnoreCase))
        {
            tokens.RemoveAt(0);
        }

        if (tokens.Count > 1 && IsOpaqueSpeciesToken(tokens[^1]))
        {
            tokens.RemoveAt(tokens.Count - 1);
        }

        var parts = tokens
            .Select(FormatCompactSpeciesToken)
            .Where(part => part.Length > 0)
            .ToArray();
        if (parts.Length > 0)
        {
            return string.Join(' ', parts);
        }

        return normalizedId.Length <= 24
            ? normalizedId
            : normalizedId[..24] + "...";
    }

    private static string TryExtractLineageCodeLabel(string? speciesName)
    {
        if (string.IsNullOrWhiteSpace(speciesName))
        {
            return string.Empty;
        }

        var trimmed = speciesName.Trim();
        var openIndex = trimmed.LastIndexOf('[');
        if (openIndex < 0 || !trimmed.EndsWith("]", StringComparison.Ordinal) || openIndex >= trimmed.Length - 2)
        {
            return string.Empty;
        }

        var code = trimmed[(openIndex + 1)..^1].Trim();
        if (code.Length == 0)
        {
            return string.Empty;
        }

        return code.All(ch => char.IsDigit(ch) || (char.IsLetter(ch) && char.IsUpper(ch)))
            ? $"[{code}]"
            : string.Empty;
    }

    private static bool IsOpaqueSpeciesToken(string token)
    {
        var trimmed = token?.Trim() ?? string.Empty;
        if (trimmed.Length < 8)
        {
            return false;
        }

        var opaqueChars = trimmed.Count(ch => char.IsLetterOrDigit(ch));
        return opaqueChars == trimmed.Length && trimmed.Any(char.IsDigit);
    }

    private static string FormatCompactSpeciesToken(string token)
    {
        var trimmed = token?.Trim() ?? string.Empty;
        if (trimmed.Length == 0)
        {
            return string.Empty;
        }

        if (trimmed.Length == 1)
        {
            return char.ToUpperInvariant(trimmed[0]).ToString(CultureInfo.InvariantCulture);
        }

        return char.ToUpperInvariant(trimmed[0]) + trimmed[1..];
    }

    private static string FormatAxisNumber(double value)
    {
        if (!double.IsFinite(value))
        {
            return "(n/a)";
        }

        return Math.Abs(value - Math.Round(value)) < 0.0001d
            ? Math.Round(value).ToString("0", CultureInfo.InvariantCulture)
            : value.ToString("0.##", CultureInfo.InvariantCulture);
    }

    private static void ReplaceItems<T>(ObservableCollection<T> target, IReadOnlyList<T> source)
    {
        target.Clear();
        foreach (var item in source)
        {
            target.Add(item);
        }
    }

    private static bool AreClose(double left, double right)
        => Math.Abs(left - right) <= 0.5d;

    private static string QuoteIfNeeded(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "\"\"";
        }

        var trimmed = value.Trim();
        return trimmed.Contains(' ') ? $"\"{trimmed}\"" : trimmed;
    }

    private static string NormalizeRunPressureModeToken(string? rawMode)
    {
        if (string.IsNullOrWhiteSpace(rawMode))
        {
            return "neutral";
        }

        return rawMode.Trim().ToLowerInvariant() switch
        {
            "divergence" => "divergence",
            "diverge" => "divergence",
            "explore" => "divergence",
            "exploratory" => "divergence",
            "neutral" => "neutral",
            "none" => "neutral",
            "off" => "neutral",
            "stability" => "stability",
            "stable" => "stability",
            "stabilize" => "stability",
            _ => "neutral"
        };
    }

    private static string NormalizeParentSelectionBiasToken(string? rawMode)
    {
        if (string.IsNullOrWhiteSpace(rawMode))
        {
            return "neutral";
        }

        return rawMode.Trim().ToLowerInvariant() switch
        {
            "divergence" => "divergence",
            "diverge" => "divergence",
            "explore" => "divergence",
            "exploratory" => "divergence",
            "neutral" => "neutral",
            "none" => "neutral",
            "off" => "neutral",
            "stability" => "stability",
            "stable" => "stability",
            "stabilize" => "stability",
            _ => "neutral"
        };
    }

    private static List<SpeciationMembershipRecord> OrderHistoryForSampling(IReadOnlyList<SpeciationMembershipRecord> history)
    {
        return history
            .OrderBy(entry => (long)entry.EpochId)
            .ThenBy(entry => entry.AssignedMs)
            .ThenBy(entry => entry.BrainId is not null && entry.BrainId.TryToGuid(out var brainId) ? brainId : Guid.Empty)
            .ToList();
    }

    private static bool ShouldUseSingleEpochRowSampling(IReadOnlyList<SpeciationMembershipRecord> orderedHistory)
    {
        if (orderedHistory.Count <= 1)
        {
            return false;
        }

        var epochId = (long)orderedHistory[0].EpochId;
        for (var i = 1; i < orderedHistory.Count; i++)
        {
            if ((long)orderedHistory[i].EpochId != epochId)
            {
                return false;
            }
        }

        return true;
    }

    private static int ParseInt(string raw, int fallback)
    {
        return int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static int RoundToNonNegativeInt(double raw, int fallback)
    {
        if (double.IsNaN(raw) || double.IsInfinity(raw))
        {
            return Math.Max(0, fallback);
        }

        return (int)Math.Max(0d, Math.Round(raw, MidpointRounding.AwayFromZero));
    }

    private static uint ParseUInt(string raw, uint fallback)
    {
        return uint.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static ulong ParseULong(string raw, ulong fallback)
    {
        return ulong.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static double ParseDouble(string raw, double fallback)
    {
        return double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static bool ParseBool(string raw, bool fallback)
    {
        if (bool.TryParse(raw, out var parsed))
        {
            return parsed;
        }

        return raw.Trim() switch
        {
            "1" => true,
            "0" => false,
            _ => fallback
        };
    }

    private static bool TryParsePort(string raw, out int port)
    {
        return int.TryParse(raw, out port) && port > 0 && port < 65536;
    }

    private static string BuildBar(int count, int maxCount)
    {
        if (count <= 0 || maxCount <= 0)
        {
            return string.Empty;
        }

        var width = Math.Clamp((int)Math.Round((count / (double)maxCount) * 16d, MidpointRounding.AwayFromZero), 1, 16);
        return new string('#', width);
    }

    private static double Clamp01(double value)
    {
        if (value < 0d)
        {
            return 0d;
        }

        if (value > 1d)
        {
            return 1d;
        }

        return value;
    }

    private static string FormatTimestamp(ulong ms)
    {
        if (ms == 0)
        {
            return "(n/a)";
        }

        try
        {
            return DateTimeOffset.FromUnixTimeMilliseconds((long)ms).ToLocalTime().ToString("g");
        }
        catch
        {
            return "(n/a)";
        }
    }

    private readonly record struct DivergenceSnapshot(string Label);

    private readonly record struct PopulationChartSnapshot(
        string RangeLabel,
        string MetricLabel,
        string YTopLabel,
        string YMidLabel,
        string YBottomLabel,
        int LegendColumns,
        IReadOnlyList<SpeciationLineChartSeriesItem> Series,
        IReadOnlyList<SpeciationChartLegendItem> Legend);

    private readonly record struct FlowChartSnapshot(
        string RangeLabel,
        string StartEpochLabel,
        string MidEpochLabel,
        string EndEpochLabel,
        int LegendColumns,
        IReadOnlyList<SpeciationFlowChartAreaItem> Areas,
        IReadOnlyList<SpeciationChartLegendItem> Legend);

    private readonly record struct FlowChartRenderLayout(
        double PlotWidth,
        double PlotHeight,
        double PaddingX,
        double PaddingY,
        int VisibleSpeciesLimit,
        int MaxLegendColumns,
        bool IncludeNewestSpecies);

    private readonly record struct FlowChartSourceFrame(
        IReadOnlyList<EpochPopulationRow> EpochRows,
        IReadOnlyList<SpeciesPopulationMeta> SpeciesOrder,
        IReadOnlyDictionary<string, string> SpeciesColors);

    private readonly record struct FlowChartHoverState(
        string SpeciesId,
        double PointerX,
        double PointerY);

    private readonly record struct SpeciationChartSourceFrame(
        IReadOnlyList<SpeciationMembershipRecord> ChartHistory,
        IReadOnlyList<SpeciationMembershipRecord> ColorSourceHistory,
        IReadOnlyList<SpeciationMembershipRecord> CladogramHistory);

    private readonly record struct SplitProximityChartSnapshot(
        string RangeLabel,
        string MetricLabel,
        string YTopLabel,
        string YMidLabel,
        string YBottomLabel,
        int LegendColumns,
        string CurrentEpochSummaryLabel,
        IReadOnlyList<SpeciationLineChartSeriesItem> Series,
        IReadOnlyList<SpeciationChartLegendItem> Legend)
    {
        public static SplitProximityChartSnapshot Empty(string summaryLabel)
            => new(
                RangeLabel: "Epochs: (no data)",
                MetricLabel: "Min lineage similarity minus effective split threshold per species (signed log10(1+|delta|) y-axis; <=0 means split-trigger zone).",
                YTopLabel: "0",
                YMidLabel: "0",
                YBottomLabel: "0",
                LegendColumns: 2,
                CurrentEpochSummaryLabel: summaryLabel,
                Series: Array.Empty<SpeciationLineChartSeriesItem>(),
                Legend: Array.Empty<SpeciationChartLegendItem>());
    }

    private readonly record struct CladogramSnapshot(
        string RangeLabel,
        string MetricLabel,
        IReadOnlyList<SpeciationCladogramItem> Items)
    {
        public static CladogramSnapshot Empty(string rangeLabel)
            => new(
                RangeLabel: rangeLabel,
                MetricLabel: "Parent -> child lineage edges inferred from divergence decisions.",
                Items: Array.Empty<SpeciationCladogramItem>());
    }

    private readonly record struct EpochPopulationRow(
        long EpochId,
        Dictionary<string, int> Counts,
        int TotalCount);

    private readonly record struct SpeciesPopulationMeta(
        string SpeciesId,
        string DisplayName,
        int TotalCount,
        ulong FirstAssignedMs,
        int FirstSeenOrder,
        string ParentSpeciesId);

    private readonly record struct EpochSplitProximityRow(
        long EpochId,
        Dictionary<string, SplitProximitySpeciesPoint> ValuesBySpecies);

    private readonly record struct SplitProximitySpeciesPoint(
        double MinProximity,
        double MinSimilarity,
        double SplitThreshold,
        int SampleCount);

    private readonly record struct SplitProximitySpeciesMeta(
        string SpeciesId,
        string DisplayName,
        int SampleCount,
        ulong LatestAssignedMs);

    private readonly record struct CladogramSpeciesMeta(
        string SpeciesId,
        string DisplayName);
}
