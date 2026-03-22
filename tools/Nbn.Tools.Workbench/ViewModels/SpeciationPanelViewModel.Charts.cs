using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class SpeciationPanelViewModel
{
    /// <summary>
    /// Gets the fixed inline population-chart width.
    /// </summary>
    public double PopulationChartWidth => PopulationChartPlotWidth;

    /// <summary>
    /// Gets the fixed inline population-chart height.
    /// </summary>
    public double PopulationChartHeight => PopulationChartPlotHeight;

    /// <summary>
    /// Gets the fixed inline flow-chart width.
    /// </summary>
    public double FlowChartWidth => FlowChartPlotWidth;

    /// <summary>
    /// Gets the fixed inline flow-chart height.
    /// </summary>
    public double FlowChartHeight => FlowChartPlotHeight;

    /// <summary>
    /// Gets the current expanded flow-chart width.
    /// </summary>
    public double ExpandedFlowChartWidth => _expandedFlowChartPlotWidth;

    /// <summary>
    /// Gets the current expanded flow-chart height.
    /// </summary>
    public double ExpandedFlowChartHeight => _expandedFlowChartPlotHeight;

    /// <summary>
    /// Updates the expanded flow-chart viewport and rebuilds responsive chart state when dimensions change.
    /// </summary>
    public void UpdateExpandedFlowChartViewport(double plotWidth, double plotHeight, double windowWidth)
    {
        var normalizedPlotWidth = Math.Max(ExpandedFlowChartMinPlotWidth, plotWidth);
        var normalizedPlotHeight = Math.Max(ExpandedFlowChartMinPlotHeight, plotHeight);
        var normalizedWindowWidth = Math.Max(normalizedPlotWidth, windowWidth);
        var changed = false;
        if (!AreClose(_expandedFlowChartPlotWidth, normalizedPlotWidth))
        {
            _expandedFlowChartPlotWidth = normalizedPlotWidth;
            OnPropertyChanged(nameof(ExpandedFlowChartWidth));
            changed = true;
        }

        if (!AreClose(_expandedFlowChartPlotHeight, normalizedPlotHeight))
        {
            _expandedFlowChartPlotHeight = normalizedPlotHeight;
            OnPropertyChanged(nameof(ExpandedFlowChartHeight));
            changed = true;
        }

        if (!AreClose(_expandedFlowChartWindowWidth, normalizedWindowWidth))
        {
            _expandedFlowChartWindowWidth = normalizedWindowWidth;
            changed = true;
        }

        if (changed)
        {
            RefreshFlowChartsFromLatestSource();
        }
    }

    /// <summary>
    /// Overrides the rendered chart color for a species and refreshes chart projections.
    /// </summary>
    public void SetSpeciesColorOverride(string speciesId, string colorHex)
    {
        var normalizedSpeciesId = NormalizeSpeciesId(speciesId);
        if (string.IsNullOrWhiteSpace(normalizedSpeciesId)
            || string.Equals(normalizedSpeciesId, "(other)", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var normalizedColor = NormalizeHexColor(colorHex);
        if (string.IsNullOrWhiteSpace(normalizedColor))
        {
            return;
        }

        if (_speciesColorOverrides.TryGetValue(normalizedSpeciesId, out var existingColor)
            && string.Equals(existingColor, normalizedColor, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        _speciesColorOverrides[normalizedSpeciesId] = normalizedColor;
        RefreshSpeciationChartsFromLatestSource();
    }

    /// <summary>
    /// Updates the inline flow-chart hover card from the current pointer position.
    /// </summary>
    public void UpdateFlowChartHover(SpeciationFlowChartAreaItem area, double pointerX, double pointerY)
        => UpdateFlowChartHoverCore(area, pointerX, pointerY, expanded: false);

    /// <summary>
    /// Clears the inline flow-chart hover card.
    /// </summary>
    public void ClearFlowChartHover()
        => ClearFlowChartHoverCore(expanded: false);

    /// <summary>
    /// Updates the expanded flow-chart hover card from the current pointer position.
    /// </summary>
    public void UpdateExpandedFlowChartHover(SpeciationFlowChartAreaItem area, double pointerX, double pointerY)
        => UpdateFlowChartHoverCore(area, pointerX, pointerY, expanded: true);

    /// <summary>
    /// Clears the expanded flow-chart hover card.
    /// </summary>
    public void ClearExpandedFlowChartHover()
        => ClearFlowChartHoverCore(expanded: true);

    private int ParseLiveChartIntervalSecondsOrDefault()
    {
        var parsed = ParseInt(LiveChartsIntervalSecondsText, DefaultLiveChartIntervalSeconds);
        return Math.Clamp(parsed, MinLiveChartIntervalSeconds, MaxLiveChartIntervalSeconds);
    }

    private static PopulationChartSnapshot BuildPopulationChartSnapshot(
        IReadOnlyList<EpochPopulationRow> epochRows,
        IReadOnlyList<SpeciesPopulationMeta> speciesOrder,
        IReadOnlyDictionary<string, string> speciesColors)
    {
        if (epochRows.Count == 0 || speciesOrder.Count == 0)
        {
            return new PopulationChartSnapshot(
                RangeLabel: "Epochs: (no data)",
                MetricLabel: "Population count by species (log10(1+count) y-axis).",
                YTopLabel: "0",
                YMidLabel: "0",
                YBottomLabel: "0",
                LegendColumns: 2,
                Series: Array.Empty<SpeciationLineChartSeriesItem>(),
                Legend: Array.Empty<SpeciationChartLegendItem>());
        }

        var totalSpeciesCount = speciesOrder.Count;
        var selectedSpecies = speciesOrder
            .Take(PopulationChartTopSpeciesLimit)
            .ToList();
        var maxCount = Math.Max(1, epochRows.SelectMany(row => row.Counts.Values).DefaultIfEmpty(0).Max());
        var logYAxisMax = Math.Max(Math.Log10(maxCount + 1d), 0.05d);
        var series = new List<SpeciationLineChartSeriesItem>(selectedSpecies.Count);
        var legend = new List<SpeciationChartLegendItem>(selectedSpecies.Count);
        foreach (var species in selectedSpecies)
        {
            var rawValues = epochRows
                .Select(row => row.Counts.TryGetValue(species.SpeciesId, out var count) ? count : 0)
                .ToArray();
            var values = rawValues
                .Select(value => Math.Log10(Math.Max(0, value) + 1d))
                .ToArray();
            var path = BuildLinePath(
                values,
                yMin: 0d,
                yMax: logYAxisMax,
                plotWidth: PopulationChartPlotWidth,
                plotHeight: PopulationChartPlotHeight,
                paddingX: PopulationChartPaddingX,
                paddingY: PopulationChartPaddingY);
            if (string.IsNullOrWhiteSpace(path))
            {
                continue;
            }

            var color = ResolveSpeciesColor(species.SpeciesId, speciesColors);
            var latestCount = rawValues.Length == 0 ? 0 : rawValues[^1];
            var latestCountLabel = latestCount.ToString(CultureInfo.InvariantCulture);
            series.Add(new SpeciationLineChartSeriesItem(species.SpeciesId, species.DisplayName, color, path, latestCountLabel));
            legend.Add(new SpeciationChartLegendItem(species.SpeciesId, species.DisplayName, color, 2d, string.Empty, true));
        }

        var legendColumns = Math.Clamp(series.Count <= 1 ? 2 : series.Count, 2, 4);
        var minEpoch = epochRows[0].EpochId;
        var maxEpoch = epochRows[^1].EpochId;
        var topScopeLabel = totalSpeciesCount > selectedSpecies.Count
            ? $" top {selectedSpecies.Count}/{totalSpeciesCount} species by population."
            : string.Empty;
        var rangeLabel = minEpoch == maxEpoch && epochRows.Count > 1
            ? $"Epoch {minEpoch} row samples ({epochRows.Count} samples){topScopeLabel}"
            : $"Epochs {minEpoch}..{maxEpoch} ({epochRows.Count} samples){topScopeLabel}";
        return new PopulationChartSnapshot(
            RangeLabel: rangeLabel,
            MetricLabel: "Population count by species (log10(1+count) y-axis).",
            YTopLabel: FormatAxisValue(maxCount),
            YMidLabel: FormatAxisValue(Math.Max(0d, Math.Pow(10d, logYAxisMax * 0.5d) - 1d)),
            YBottomLabel: "0",
            LegendColumns: legendColumns,
            Series: series,
            Legend: legend);
    }

    private static FlowChartSnapshot BuildFlowChartSnapshot(
        IReadOnlyList<EpochPopulationRow> epochRows,
        IReadOnlyList<SpeciesPopulationMeta> speciesOrder,
        IReadOnlyDictionary<string, string> speciesColors,
        FlowChartRenderLayout layout)
    {
        if (epochRows.Count == 0 || speciesOrder.Count == 0)
        {
            return new FlowChartSnapshot(
                RangeLabel: "Epochs: (no data)",
                StartEpochLabel: "(n/a)",
                MidEpochLabel: "(n/a)",
                EndEpochLabel: "(n/a)",
                LegendColumns: 2,
                Areas: Array.Empty<SpeciationFlowChartAreaItem>(),
                Legend: Array.Empty<SpeciationChartLegendItem>());
        }

        var totalSpeciesCount = speciesOrder.Count;
        var showAllSpecies = totalSpeciesCount <= layout.VisibleSpeciesLimit + 1;
        var selectedSpecies = showAllSpecies
            ? speciesOrder.ToList()
            : SelectFlowChartSpecies(
                speciesOrder,
                layout.VisibleSpeciesLimit,
                layout.IncludeNewestSpecies);
        var orderedSelectedSpecies = OrderFlowSpeciesForDisplay(selectedSpecies);
        var includeOtherSpecies = !showAllSpecies && totalSpeciesCount > selectedSpecies.Count;
        var flowSpecies = new List<SpeciesPopulationMeta>(orderedSelectedSpecies.Count + (includeOtherSpecies ? 1 : 0));
        flowSpecies.AddRange(orderedSelectedSpecies);
        if (includeOtherSpecies)
        {
            flowSpecies.Add(new SpeciesPopulationMeta("(other)", "Other species", 0, 0UL, int.MaxValue, string.Empty));
        }

        var speciesCount = flowSpecies.Count;
        var epochCount = epochRows.Count;
        var minEpoch = epochRows[0].EpochId;
        var maxEpoch = epochRows[^1].EpochId;
        var isSingleEpochRowSampling = minEpoch == maxEpoch && epochRows.Count > 1;
        var visibleParentBySpeciesIndex = BuildFlowVisibleParentIndices(flowSpecies, speciesOrder);
        var childIndicesByParent = BuildFlowChildIndices(flowSpecies, visibleParentBySpeciesIndex);
        var rootIndices = Enumerable.Range(0, speciesCount)
            .Where(index => visibleParentBySpeciesIndex[index] < 0)
            .ToArray();
        var bandsByEpoch = new List<FlowSpeciesRowBands[]>(epochCount);
        for (var epochIndex = 0; epochIndex < epochCount; epochIndex++)
        {
            var row = epochRows[epochIndex];
            var rowBands = Enumerable.Range(0, speciesCount)
                .Select(_ => FlowSpeciesRowBands.Empty)
                .ToArray();
            ResolveFlowLineageRowBands(
                row,
                flowSpecies,
                includeOtherSpecies,
                rootIndices,
                childIndicesByParent,
                rowBands);

            bandsByEpoch.Add(rowBands);
        }

        var areas = new List<SpeciationFlowChartAreaItem>(speciesCount);
        var legend = new List<SpeciationChartLegendItem>(speciesCount);
        for (var speciesIndex = 0; speciesIndex < speciesCount; speciesIndex++)
        {
            var primaryStarts = new double[epochCount];
            var primaryEnds = new double[epochCount];
            var secondaryStarts = new double[epochCount];
            var secondaryEnds = new double[epochCount];
            for (var epochIndex = 0; epochIndex < epochCount; epochIndex++)
            {
                var rowBands = bandsByEpoch[epochIndex][speciesIndex];
                primaryStarts[epochIndex] = rowBands.PrimaryStart;
                primaryEnds[epochIndex] = rowBands.PrimaryEnd;
                secondaryStarts[epochIndex] = rowBands.SecondaryStart;
                secondaryEnds[epochIndex] = rowBands.SecondaryEnd;
            }

            AddFlowAreaTransitionCaps(secondaryStarts, secondaryEnds, primaryEnds);
            var path = CombineFlowAreaPaths(
                BuildFlowAreaPath(
                    primaryStarts,
                    primaryEnds,
                    plotWidth: layout.PlotWidth,
                    plotHeight: layout.PlotHeight,
                    paddingX: layout.PaddingX,
                    paddingY: layout.PaddingY),
                BuildFlowAreaPath(
                    secondaryStarts,
                    secondaryEnds,
                    plotWidth: layout.PlotWidth,
                    plotHeight: layout.PlotHeight,
                    paddingX: layout.PaddingX,
                    paddingY: layout.PaddingY));
            if (string.IsNullOrWhiteSpace(path))
            {
                continue;
            }

            var species = flowSpecies[speciesIndex];
            var color = string.Equals(species.SpeciesId, "(other)", StringComparison.Ordinal)
                ? "#6B7280"
                : ResolveSpeciesColor(species.SpeciesId, speciesColors);
            var fill = color;
            var lastShare = bandsByEpoch[^1][speciesIndex].TotalWidth;
            var lastShareLabel = lastShare.ToString("P1", CultureInfo.InvariantCulture);
            var samples = BuildFlowChartAreaSamples(
                epochRows,
                bandsByEpoch,
                flowSpecies,
                speciesIndex,
                isSingleEpochRowSampling,
                layout);
            areas.Add(new SpeciationFlowChartAreaItem(species.SpeciesId, species.DisplayName, fill, color, path, lastShareLabel, samples));
            legend.Add(new SpeciationChartLegendItem(
                species.SpeciesId,
                species.DisplayName,
                fill,
                10d,
                string.Empty,
                !string.Equals(species.SpeciesId, "(other)", StringComparison.OrdinalIgnoreCase)));
        }

        var topAxisLabel = isSingleEpochRowSampling
            ? "1"
            : minEpoch.ToString(CultureInfo.InvariantCulture);
        var midAxisLabel = isSingleEpochRowSampling
            ? FormatAxisNumber((epochRows.Count + 1d) * 0.5d)
            : FormatAxisNumber((minEpoch + maxEpoch) * 0.5d);
        var bottomAxisLabel = isSingleEpochRowSampling
            ? epochRows.Count.ToString(CultureInfo.InvariantCulture)
            : maxEpoch.ToString(CultureInfo.InvariantCulture);
        var legendColumns = Math.Clamp(areas.Count <= 1 ? 2 : areas.Count, 2, layout.MaxLegendColumns);
        var topScopeLabel = BuildFlowChartVisibilityScopeLabel(selectedSpecies.Count, totalSpeciesCount, includeOtherSpecies);
        var rangeLabel = minEpoch == maxEpoch && epochRows.Count > 1
            ? $"Stacked share across loaded rows for epoch {minEpoch} ({epochRows.Count} samples).{topScopeLabel}"
            : $"Stacked share of total population per epoch ({minEpoch}..{maxEpoch}).{topScopeLabel}";
        return new FlowChartSnapshot(
            RangeLabel: rangeLabel,
            StartEpochLabel: topAxisLabel,
            MidEpochLabel: midAxisLabel,
            EndEpochLabel: bottomAxisLabel,
            LegendColumns: legendColumns,
            Areas: areas,
            Legend: legend);
    }

    private void ApplyPopulationChartSnapshot(PopulationChartSnapshot snapshot)
    {
        ReplaceItems(PopulationChartSeries, snapshot.Series);
        ReplaceItems(PopulationChartLegend, snapshot.Legend);
        PopulationChartRangeLabel = snapshot.RangeLabel;
        PopulationChartMetricLabel = snapshot.MetricLabel;
        PopulationChartYAxisTopLabel = snapshot.YTopLabel;
        PopulationChartYAxisMidLabel = snapshot.YMidLabel;
        PopulationChartYAxisBottomLabel = snapshot.YBottomLabel;
        PopulationChartLegendColumns = snapshot.LegendColumns;
    }

    private void ApplyFlowChartSnapshot(FlowChartSnapshot snapshot)
    {
        ReplaceItems(FlowChartAreas, snapshot.Areas);
        ReplaceItems(FlowChartLegend, snapshot.Legend);
        FlowChartRangeLabel = snapshot.RangeLabel;
        FlowChartStartEpochLabel = snapshot.StartEpochLabel;
        FlowChartMidEpochLabel = snapshot.MidEpochLabel;
        FlowChartEndEpochLabel = snapshot.EndEpochLabel;
        FlowChartLegendColumns = snapshot.LegendColumns;
        RestoreFlowChartHover(expanded: false);
    }

    private void ApplyExpandedFlowChartSnapshot(FlowChartSnapshot snapshot)
    {
        ReplaceItems(ExpandedFlowChartAreas, snapshot.Areas);
        ReplaceItems(ExpandedFlowChartLegend, snapshot.Legend);
        ExpandedFlowChartRangeLabel = snapshot.RangeLabel;
        ExpandedFlowChartStartEpochLabel = snapshot.StartEpochLabel;
        ExpandedFlowChartMidEpochLabel = snapshot.MidEpochLabel;
        ExpandedFlowChartEndEpochLabel = snapshot.EndEpochLabel;
        ExpandedFlowChartLegendColumns = snapshot.LegendColumns;
        RestoreFlowChartHover(expanded: true);
    }

    private void RefreshFlowChartsFromLatestSource()
    {
        if (!_lastFlowChartSource.HasValue)
        {
            return;
        }

        var source = _lastFlowChartSource.Value;
        var inlineSnapshot = BuildFlowChartSnapshot(
            source.EpochRows,
            source.SpeciesOrder,
            source.SpeciesColors,
            BuildInlineFlowChartRenderLayout());
        var expandedSnapshot = BuildFlowChartSnapshot(
            source.EpochRows,
            source.SpeciesOrder,
            source.SpeciesColors,
            BuildExpandedFlowChartRenderLayout());

        _dispatcher.Post(() =>
        {
            ApplyFlowChartSnapshot(inlineSnapshot);
            ApplyExpandedFlowChartSnapshot(expandedSnapshot);
        });
    }

    private void RefreshSpeciationChartsFromLatestSource()
    {
        if (!_lastSpeciationChartSource.HasValue)
        {
            return;
        }

        var source = _lastSpeciationChartSource.Value;
        var speciesColors = BuildSpeciesColorMap(source.ColorSourceHistory, _speciesColorOverrides);
        var populationFrame = BuildEpochPopulationFrame(source.ChartHistory);
        var flowChartSource = new FlowChartSourceFrame(populationFrame.EpochRows, populationFrame.SpeciesOrder, speciesColors);
        var populationSnapshot = BuildPopulationChartSnapshot(
            populationFrame.EpochRows,
            populationFrame.SpeciesOrder,
            speciesColors);
        var inlineFlowSnapshot = BuildFlowChartSnapshot(
            flowChartSource.EpochRows,
            flowChartSource.SpeciesOrder,
            flowChartSource.SpeciesColors,
            BuildInlineFlowChartRenderLayout());
        var expandedFlowSnapshot = BuildFlowChartSnapshot(
            flowChartSource.EpochRows,
            flowChartSource.SpeciesOrder,
            flowChartSource.SpeciesColors,
            BuildExpandedFlowChartRenderLayout());
        var splitProximitySnapshot = BuildSplitProximityChartSnapshot(
            source.ChartHistory,
            CurrentEpochId,
            ParseDouble(LineageSplitThreshold, 0.88d),
            ParseDouble(LineageSplitGuardMargin, 0.02d),
            speciesColors);
        var cladogramSnapshot = BuildCladogramSnapshot(source.CladogramHistory, speciesColors);

        _dispatcher.Post(() =>
        {
            _lastFlowChartSource = flowChartSource;
            ApplyPopulationChartSnapshot(populationSnapshot);
            ApplyFlowChartSnapshot(inlineFlowSnapshot);
            ApplyExpandedFlowChartSnapshot(expandedFlowSnapshot);
            CurrentEpochSplitProximityLabel = splitProximitySnapshot.CurrentEpochSummaryLabel;
            ApplySplitProximityChartSnapshot(splitProximitySnapshot);
            ApplyCladogramSnapshot(cladogramSnapshot);
        });
    }

    private FlowChartRenderLayout BuildInlineFlowChartRenderLayout()
        => new(
            PlotWidth: FlowChartPlotWidth,
            PlotHeight: FlowChartPlotHeight,
            PaddingX: FlowChartPaddingX,
            PaddingY: FlowChartPaddingY,
            VisibleSpeciesLimit: FlowChartTopSpeciesLimit,
            MaxLegendColumns: 4,
            IncludeNewestSpecies: IncludeNewestSpeciesInFlowChart);

    private FlowChartRenderLayout BuildExpandedFlowChartRenderLayout()
    {
        var visibleSpeciesLimit = _expandedFlowChartWindowWidth >= ExpandedFlowChartWideWindowThreshold
            ? ExpandedFlowChartUltraWideTopSpeciesLimit
            : ExpandedFlowChartTopSpeciesLimit;
        var maxLegendColumns = Math.Clamp((int)Math.Floor(_expandedFlowChartWindowWidth / 280d), 4, 8);
        return new FlowChartRenderLayout(
            PlotWidth: Math.Max(ExpandedFlowChartMinPlotWidth, _expandedFlowChartPlotWidth),
            PlotHeight: Math.Max(ExpandedFlowChartMinPlotHeight, _expandedFlowChartPlotHeight),
            PaddingX: FlowChartPaddingX,
            PaddingY: FlowChartPaddingY,
            VisibleSpeciesLimit: visibleSpeciesLimit,
            MaxLegendColumns: maxLegendColumns,
            IncludeNewestSpecies: IncludeNewestSpeciesInFlowChart);
    }

    private void UpdateFlowChartHoverCore(SpeciationFlowChartAreaItem area, double pointerX, double pointerY, bool expanded)
    {
        if (area.Samples.Count == 0)
        {
            ClearFlowChartHoverCore(expanded);
            return;
        }

        var sample = ResolveFlowChartHoverSample(area.Samples, pointerX, pointerY);
        if (sample is null)
        {
            ClearFlowChartHoverCore(expanded);
            return;
        }

        var title = area.Label;
        var detail = $"{sample.RowLabel}{Environment.NewLine}Population {sample.PopulationCount} of {sample.TotalCount} ({sample.Share.ToString("P1", CultureInfo.InvariantCulture)})";
        var text = $"{title}{Environment.NewLine}{detail}";
        var chartWidth = expanded ? ExpandedFlowChartWidth : FlowChartWidth;
        var chartHeight = expanded ? ExpandedFlowChartHeight : FlowChartHeight;
        var hoverLeft = Math.Clamp(pointerX + FlowChartHoverCardOffset, 4d, Math.Max(4d, chartWidth - FlowChartHoverCardMaxWidth));
        var hoverTop = Math.Clamp(pointerY + FlowChartHoverCardOffset, 4d, Math.Max(4d, chartHeight - FlowChartHoverCardMaxHeight));

        if (expanded)
        {
            _lastExpandedFlowChartHoverState = new FlowChartHoverState(area.SpeciesId, pointerX, pointerY);
            ExpandedFlowChartHoverCardTitle = title;
            ExpandedFlowChartHoverCardDetail = detail;
            ExpandedFlowChartHoverCardText = text;
            ExpandedFlowChartHoverCardSwatchColor = area.Stroke;
            ExpandedFlowChartHoverCardLeft = hoverLeft;
            ExpandedFlowChartHoverCardTop = hoverTop;
            IsExpandedFlowChartHoverCardVisible = true;
            return;
        }

        _lastFlowChartHoverState = new FlowChartHoverState(area.SpeciesId, pointerX, pointerY);
        FlowChartHoverCardTitle = title;
        FlowChartHoverCardDetail = detail;
        FlowChartHoverCardText = text;
        FlowChartHoverCardSwatchColor = area.Stroke;
        FlowChartHoverCardLeft = hoverLeft;
        FlowChartHoverCardTop = hoverTop;
        IsFlowChartHoverCardVisible = true;
    }

    private void ClearFlowChartHoverCore(bool expanded)
    {
        if (expanded)
        {
            _lastExpandedFlowChartHoverState = null;
            ExpandedFlowChartHoverCardTitle = string.Empty;
            ExpandedFlowChartHoverCardDetail = string.Empty;
            ExpandedFlowChartHoverCardText = string.Empty;
            ExpandedFlowChartHoverCardSwatchColor = "Transparent";
            IsExpandedFlowChartHoverCardVisible = false;
            return;
        }

        _lastFlowChartHoverState = null;
        FlowChartHoverCardTitle = string.Empty;
        FlowChartHoverCardDetail = string.Empty;
        FlowChartHoverCardText = string.Empty;
        FlowChartHoverCardSwatchColor = "Transparent";
        IsFlowChartHoverCardVisible = false;
    }

    private void RestoreFlowChartHover(bool expanded)
    {
        var hoverState = expanded ? _lastExpandedFlowChartHoverState : _lastFlowChartHoverState;
        if (!hoverState.HasValue)
        {
            ClearFlowChartHoverPresentation(expanded);
            return;
        }

        var areas = expanded
            ? (IEnumerable<SpeciationFlowChartAreaItem>)ExpandedFlowChartAreas
            : FlowChartAreas;
        var matchingArea = areas.FirstOrDefault(item => string.Equals(item.SpeciesId, hoverState.Value.SpeciesId, StringComparison.OrdinalIgnoreCase));
        if (matchingArea is null)
        {
            ClearFlowChartHoverCore(expanded);
            return;
        }

        UpdateFlowChartHoverCore(matchingArea, hoverState.Value.PointerX, hoverState.Value.PointerY, expanded);
    }

    private void ClearFlowChartHoverPresentation(bool expanded)
    {
        if (expanded)
        {
            ExpandedFlowChartHoverCardTitle = string.Empty;
            ExpandedFlowChartHoverCardDetail = string.Empty;
            ExpandedFlowChartHoverCardText = string.Empty;
            ExpandedFlowChartHoverCardSwatchColor = "Transparent";
            IsExpandedFlowChartHoverCardVisible = false;
            return;
        }

        FlowChartHoverCardTitle = string.Empty;
        FlowChartHoverCardDetail = string.Empty;
        FlowChartHoverCardText = string.Empty;
        FlowChartHoverCardSwatchColor = "Transparent";
        IsFlowChartHoverCardVisible = false;
    }

    private static string BuildFlowChartVisibilityScopeLabel(int visibleSpeciesCount, int totalSpeciesCount, bool includeOtherSpecies)
        => includeOtherSpecies
            ? $" {visibleSpeciesCount}/{totalSpeciesCount} visible species + Other."
            : $" {visibleSpeciesCount}/{totalSpeciesCount} visible species.";

    private static SpeciationFlowChartSampleItem? ResolveFlowChartHoverSample(
        IReadOnlyList<SpeciationFlowChartSampleItem> samples,
        double pointerX,
        double pointerY)
    {
        if (samples.Count == 0 || !double.IsFinite(pointerY))
        {
            return null;
        }

        var candidates = double.IsFinite(pointerX)
            ? samples
                .Where(sample => sample.Bands.Any(band => pointerX >= band.StartX - 0.5d && pointerX <= band.EndX + 0.5d))
                .ToArray()
            : Array.Empty<SpeciationFlowChartSampleItem>();
        var searchSpace = candidates.Length > 0 ? candidates : samples;
        SpeciationFlowChartSampleItem? best = null;
        var bestDistance = double.MaxValue;
        foreach (var sample in searchSpace)
        {
            var distance = Math.Abs(sample.CenterY - pointerY);
            if (distance < bestDistance)
            {
                best = sample;
                bestDistance = distance;
            }
        }

        return best;
    }

    private void ApplySplitProximityChartSnapshot(SplitProximityChartSnapshot snapshot)
    {
        ReplaceItems(SplitProximityChartSeries, snapshot.Series);
        ReplaceItems(SplitProximityChartLegend, snapshot.Legend);
        SplitProximityChartRangeLabel = snapshot.RangeLabel;
        SplitProximityChartMetricLabel = snapshot.MetricLabel;
        SplitProximityChartYAxisTopLabel = snapshot.YTopLabel;
        SplitProximityChartYAxisMidLabel = snapshot.YMidLabel;
        SplitProximityChartYAxisBottomLabel = snapshot.YBottomLabel;
        SplitProximityChartLegendColumns = snapshot.LegendColumns;
    }

    private void ApplyCladogramSnapshot(CladogramSnapshot snapshot)
    {
        var priorExpansionBySpecies = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
        CaptureCladogramExpansionState(CladogramItems, priorExpansionBySpecies);
        foreach (var root in snapshot.Items)
        {
            ApplyCladogramExpansionState(root, priorExpansionBySpecies);
        }

        ReplaceItems(CladogramItems, snapshot.Items);
        CladogramRangeLabel = snapshot.RangeLabel;
        CladogramMetricLabel = snapshot.MetricLabel;
    }

    private static void CaptureCladogramExpansionState(
        IEnumerable<SpeciationCladogramItem> items,
        IDictionary<string, bool> expansionBySpecies)
    {
        foreach (var item in items)
        {
            expansionBySpecies[item.SpeciesId] = item.IsExpanded;
            CaptureCladogramExpansionState(item.Children, expansionBySpecies);
        }
    }

    private static bool ApplyCladogramExpansionState(
        SpeciationCladogramItem node,
        IReadOnlyDictionary<string, bool> priorExpansionBySpecies)
    {
        var wasKnown = priorExpansionBySpecies.TryGetValue(node.SpeciesId, out var priorExpanded);
        var subtreeContainsNewSpecies = !wasKnown;
        foreach (var child in node.Children)
        {
            if (ApplyCladogramExpansionState(child, priorExpansionBySpecies))
            {
                subtreeContainsNewSpecies = true;
            }
        }

        node.IsExpanded = subtreeContainsNewSpecies || (wasKnown ? priorExpanded : true);
        return subtreeContainsNewSpecies;
    }
}
