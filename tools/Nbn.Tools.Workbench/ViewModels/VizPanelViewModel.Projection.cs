using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Platform.Storage;
using Nbn.Proto.Io;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class VizPanelViewModel
{
    private void RefreshActivityProjection()
    {
        var miniChartTopN = ParseMiniActivityTopNOrDefault();
        var miniChartTickWindow = ParseMiniActivityTickWindowOrDefault();
        var latestTickHint = GetLatestTickForCurrentSelection();
        var options = new VizActivityProjectionOptions(
            TickWindow: ParseTickWindowOrDefault(),
            IncludeLowSignalEvents: IncludeLowSignalEvents,
            FocusRegionId: TryParseRegionId(RegionFocusText, out var regionId) ? regionId : null,
            TopSeriesCount: miniChartTopN,
            EnableMiniChart: ShowMiniActivityChart,
            MiniChartTickWindow: miniChartTickWindow,
            MiniChartMinTickFloor: _miniChartMinTickFloor,
            LatestTickHint: latestTickHint > 0 ? latestTickHint : null);
        var eventsSnapshot = _filteredProjectionEvents.ToList();
        var topology = BuildTopologySnapshotForSelectedBrain();
        var interaction = BuildCanvasInteractionState();
        var colorMode = SelectedCanvasColorMode.Mode;
        var renderOptions = BuildCanvasRenderOptions();

        if (eventsSnapshot.Count == 0)
        {
            var refreshVersion = Interlocked.Increment(ref _projectionLayoutRefreshVersion);
            CancelAndDisposeProjectionLayoutCts();
            var result = BuildProjectionAndCanvasSnapshot(eventsSnapshot, options, topology, interaction, colorMode, renderOptions);
            ApplyProjectionAndCanvasSnapshot(refreshVersion, result);
            return;
        }

        var version = Interlocked.Increment(ref _projectionLayoutRefreshVersion);
        var cts = new CancellationTokenSource();
        var previousCts = Interlocked.Exchange(ref _projectionLayoutRefreshCts, cts);
        previousCts?.Cancel();
        previousCts?.Dispose();
        _ = Task.Run(
                () => BuildProjectionAndCanvasSnapshot(eventsSnapshot, options, topology, interaction, colorMode, renderOptions, cts.Token),
                cts.Token)
            .ContinueWith(task =>
            {
                if (cts.IsCancellationRequested || task.IsCanceled)
                {
                    return;
                }

                if (task.IsFaulted)
                {
                    _dispatcher.Post(() =>
                    {
                        if (version == Volatile.Read(ref _projectionLayoutRefreshVersion))
                        {
                            var error = task.Exception?.GetBaseException().Message ?? "unknown error";
                            Status = $"Visualizer projection refresh failed: {error}";
                        }
                    });
                    return;
                }

                var result = task.Result;
                _dispatcher.Post(() => ApplyProjectionAndCanvasSnapshot(version, result));
            }, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default);
    }

    private void RefreshCanvasLayoutOnly()
    {
        InvalidatePendingProjectionLayoutRefresh();
        var frameStart = Stopwatch.GetTimestamp();
        if (_currentProjection is null)
        {
            SetActivityCanvasDimensions(VizActivityCanvasLayoutBuilder.CanvasWidth, VizActivityCanvasLayoutBuilder.CanvasHeight);
            ReplaceItems(CanvasNodes, Array.Empty<VizActivityCanvasNode>());
            ReplaceItems(CanvasEdges, Array.Empty<VizActivityCanvasEdge>());
            RebuildCanvasHitIndex(Array.Empty<VizActivityCanvasNode>(), Array.Empty<VizActivityCanvasEdge>());
            ActivityCanvasLegend = "Canvas renderer awaiting activity.";
            ActivityInteractionSummary = "Selected: none | Hover: none";
            ActivityPinnedSummary = "Pinned: none.";
            _lastCanvasLayoutBuildMs = 0;
            _lastCanvasApplyMs = 0;
            _lastCanvasNodeDiffStats = CollectionDiffStats.Empty;
            _lastCanvasEdgeDiffStats = CollectionDiffStats.Empty;
            _lastCanvasFrameMs = StopwatchElapsedMs(frameStart);
            OnPropertyChanged(nameof(TogglePinSelectionLabel));
            return;
        }

        var topology = BuildTopologySnapshotForSelectedBrain();
        var interaction = BuildCanvasInteractionState();
        var renderOptions = BuildCanvasRenderOptions();
        var layoutStart = Stopwatch.GetTimestamp();
        var canvas = VizActivityCanvasLayoutBuilder.Build(
            _currentProjection,
            _currentProjectionOptions,
            interaction,
            topology,
            SelectedCanvasColorMode.Mode,
            renderOptions);
        var finalized = FinalizeCanvasLayout(
            canvas,
            _currentProjection,
            _currentProjectionOptions,
            topology,
            SelectedCanvasColorMode.Mode,
            renderOptions);
        canvas = finalized.Canvas;
        _lastCanvasLayoutBuildMs = StopwatchElapsedMs(layoutStart) + finalized.AdditionalLayoutMs;

        var applyStart = Stopwatch.GetTimestamp();
        _lastCanvasNodeDiffStats = ApplyKeyedDiff(CanvasNodes, canvas.Nodes, static item => item.NodeKey);
        _lastCanvasEdgeDiffStats = ApplyKeyedDiff(CanvasEdges, canvas.Edges, static item => item.RouteLabel);
        RebuildCanvasHitIndex(canvas.Nodes, canvas.Edges);
        ActivityCanvasLegend = $"{canvas.Legend} | Color mode: {SelectedCanvasColorMode.Label} ({SelectedCanvasColorMode.LegendHint}) | Curve: {SelectedCanvasTransferCurve.Label} ({SelectedCanvasTransferCurve.LegendHint})";
        UpdateCanvasInteractionSummaries(canvas.Nodes, canvas.Edges);
        RefreshCanvasHoverCard(canvas.Nodes, canvas.Edges);
        _lastCanvasApplyMs = StopwatchElapsedMs(applyStart);
        _lastCanvasFrameMs = StopwatchElapsedMs(frameStart);
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
    }

    private ProjectionCanvasSnapshot BuildProjectionAndCanvasSnapshot(
        IReadOnlyList<VizEventItem> eventsSnapshot,
        VizActivityProjectionOptions options,
        VizActivityCanvasTopology topology,
        VizActivityCanvasInteractionState interaction,
        VizActivityCanvasColorMode colorMode,
        VizActivityCanvasRenderOptions renderOptions,
        CancellationToken cancellationToken = default)
    {
        var projectionStart = Stopwatch.GetTimestamp();
        var projection = VizActivityProjectionBuilder.Build(eventsSnapshot, options);
        cancellationToken.ThrowIfCancellationRequested();
        var projectionMs = StopwatchElapsedMs(projectionStart);

        var layoutStart = Stopwatch.GetTimestamp();
        var canvas = VizActivityCanvasLayoutBuilder.Build(
            projection,
            options,
            interaction,
            topology,
            colorMode,
            renderOptions);
        cancellationToken.ThrowIfCancellationRequested();
        var layoutMs = StopwatchElapsedMs(layoutStart);
        var miniChart = BuildMiniActivityChartRenderSnapshot(projection.MiniChart);
        cancellationToken.ThrowIfCancellationRequested();
        return new ProjectionCanvasSnapshot(projection, options, canvas, miniChart, projectionMs, layoutMs);
    }

    private void ApplyProjectionAndCanvasSnapshot(int refreshVersion, ProjectionCanvasSnapshot snapshot)
    {
        if (refreshVersion != Volatile.Read(ref _projectionLayoutRefreshVersion))
        {
            return;
        }

        _currentProjection = snapshot.Projection;
        _currentProjectionOptions = snapshot.Options;

        ReplaceItems(ActivityStats, snapshot.Projection.Stats);
        ReplaceItems(RegionActivity, snapshot.Projection.Regions.Take(SnapshotRegionRows).ToList());
        ReplaceItems(EdgeActivity, snapshot.Projection.Edges.Take(SnapshotEdgeRows).ToList());
        ReplaceItems(TickActivity, snapshot.Projection.Ticks);
        ActivitySummary = snapshot.Projection.Summary;
        ApplyMiniActivityChartSnapshot(snapshot.MiniChart);

        var layoutMs = snapshot.LayoutMs;
        var finalized = FinalizeCanvasLayout(
            snapshot.Canvas,
            snapshot.Projection,
            snapshot.Options,
            BuildTopologySnapshotForSelectedBrain(),
            SelectedCanvasColorMode.Mode,
            BuildCanvasRenderOptions());
        var canvas = finalized.Canvas;
        layoutMs += finalized.AdditionalLayoutMs;

        var applyStart = Stopwatch.GetTimestamp();
        _lastCanvasNodeDiffStats = ApplyKeyedDiff(CanvasNodes, canvas.Nodes, static item => item.NodeKey);
        _lastCanvasEdgeDiffStats = ApplyKeyedDiff(CanvasEdges, canvas.Edges, static item => item.RouteLabel);
        RebuildCanvasHitIndex(canvas.Nodes, canvas.Edges);
        ActivityCanvasLegend = $"{canvas.Legend} | Color mode: {SelectedCanvasColorMode.Label} ({SelectedCanvasColorMode.LegendHint}) | Curve: {SelectedCanvasTransferCurve.Label} ({SelectedCanvasTransferCurve.LegendHint})";
        UpdateCanvasInteractionSummaries(canvas.Nodes, canvas.Edges);
        RefreshCanvasHoverCard(canvas.Nodes, canvas.Edges);
        _lastCanvasApplyMs = StopwatchElapsedMs(applyStart);
        _lastProjectionBuildMs = snapshot.ProjectionMs;
        _lastCanvasLayoutBuildMs = layoutMs;
        _lastCanvasFrameMs = snapshot.ProjectionMs + layoutMs + _lastCanvasApplyMs;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
    }

    private static MiniActivityChartRenderSnapshot BuildMiniActivityChartRenderSnapshot(VizMiniActivityChart chart)
    {
        var yModeLabel = chart.UseSignedLinearScale
            ? "y-axis linear (signed buffer)"
            : "y-axis log(1+score)";
        if (!chart.Enabled)
        {
            return new MiniActivityChartRenderSnapshot(
                Enabled: false,
                SeriesLabel: chart.ModeLabel,
                RangeLabel: "Ticks: mini chart disabled.",
                MetricLabel: $"{chart.MetricLabel} | {yModeLabel} | toggle on to resume tracking",
                YAxisTopLabel: "0",
                YAxisMidLabel: "0",
                YAxisBottomLabel: "0",
                LegendColumns: 2,
                TickCount: 0,
                Series: Array.Empty<VizMiniActivityChartSeriesItem>());
        }

        if (chart.Series.Count == 0 || chart.Ticks.Count == 0)
        {
            return new MiniActivityChartRenderSnapshot(
                Enabled: true,
                SeriesLabel: chart.ModeLabel,
                RangeLabel: "Ticks: awaiting activity.",
                MetricLabel: $"{chart.MetricLabel} | {yModeLabel} | no ranked series in current window",
                YAxisTopLabel: "0",
                YAxisMidLabel: "0",
                YAxisBottomLabel: "0",
                LegendColumns: 2,
                TickCount: 0,
                Series: Array.Empty<VizMiniActivityChartSeriesItem>());
        }

        var useSignedLinearScale = chart.UseSignedLinearScale;
        var (yMin, yMax) = useSignedLinearScale
            ? ResolveMiniChartSignedYAxisBounds(chart)
            : (0f, ResolveMiniChartYAxisMax(chart));
        var rows = new List<VizMiniActivityChartSeriesItem>(chart.Series.Count);
        foreach (var series in chart.Series)
        {
            var stroke = GetMiniActivitySeriesStroke(series.Key);
            var pathData = BuildMiniActivitySeriesPath(
                series.Values,
                MiniActivityChartPlotWidth,
                MiniActivityChartPlotHeight,
                MiniActivityChartPlotPaddingX,
                MiniActivityChartPlotPaddingY,
                yMin,
                yMax,
                useSignedLinearScale: useSignedLinearScale);
            rows.Add(new VizMiniActivityChartSeriesItem(
                series.Key,
                series.Label,
                stroke,
                pathData));
        }

        var legendColumns = Math.Clamp(rows.Count <= 1 ? 2 : rows.Count, 2, 4);
        var metricRange = useSignedLinearScale
            ? $"range {yMin:0.###}..{yMax:0.###}"
            : $"y-max {yMax:0.###} (peak {chart.PeakScore:0.###})";
        var midLabel = useSignedLinearScale
            ? FormatMiniChartAxisValue((yMin + yMax) * 0.5f)
            : FormatMiniChartAxisValue(MiniChartValueFromLogRatio(yMax, 0.5f));
        var bottomLabel = useSignedLinearScale ? FormatMiniChartAxisValue(yMin) : "0";
        return new MiniActivityChartRenderSnapshot(
            Enabled: true,
            SeriesLabel: chart.ModeLabel,
            RangeLabel: $"Ticks {chart.MinTick}..{chart.MaxTick}",
            MetricLabel: $"{chart.MetricLabel} | {yModeLabel} | {metricRange}",
            YAxisTopLabel: FormatMiniChartAxisValue(yMax),
            YAxisMidLabel: midLabel,
            YAxisBottomLabel: bottomLabel,
            LegendColumns: legendColumns,
            TickCount: chart.Ticks.Count,
            Series: rows);
    }

    private static float ResolveMiniChartYAxisMax(VizMiniActivityChart chart)
    {
        var rawPeak = chart.PeakScore > 0f && float.IsFinite(chart.PeakScore)
            ? chart.PeakScore
            : 1f;
        var samples = new List<float>();
        foreach (var series in chart.Series)
        {
            foreach (var value in series.Values)
            {
                if (value > 0f && float.IsFinite(value))
                {
                    samples.Add(value);
                }
            }
        }

        if (samples.Count == 0)
        {
            return rawPeak;
        }

        samples.Sort();
        var percentileIndex = (int)Math.Ceiling(samples.Count * 0.95) - 1;
        percentileIndex = Math.Clamp(percentileIndex, 0, samples.Count - 1);
        var p95 = samples[percentileIndex];
        var robustHeadroom = p95 * 1.2f;
        var floorFromPeak = rawPeak * 0.2f;
        return Math.Max(1f, Math.Max(robustHeadroom, floorFromPeak));
    }

    private static (float Min, float Max) ResolveMiniChartSignedYAxisBounds(VizMiniActivityChart chart)
    {
        var samples = new List<float>();
        foreach (var series in chart.Series)
        {
            foreach (var value in series.Values)
            {
                if (float.IsFinite(value))
                {
                    samples.Add(value);
                }
            }
        }

        if (samples.Count == 0)
        {
            return (-1f, 1f);
        }

        samples.Sort();
        var p05Index = (int)Math.Ceiling(samples.Count * 0.05) - 1;
        var p95Index = (int)Math.Ceiling(samples.Count * 0.95) - 1;
        p05Index = Math.Clamp(p05Index, 0, samples.Count - 1);
        p95Index = Math.Clamp(p95Index, 0, samples.Count - 1);

        var robustMin = Math.Min(samples[0], samples[p05Index]);
        var robustMax = Math.Max(samples[^1], samples[p95Index]);
        var min = Math.Min(chart.MinScore, robustMin);
        var max = robustMax;
        min = Math.Min(min, 0f);
        max = Math.Max(max, 0f);

        var span = max - min;
        if (!(span > 1e-5f) || !float.IsFinite(span))
        {
            var pad = Math.Max(0.5f, MathF.Max(MathF.Abs(min), MathF.Abs(max)) * 0.25f);
            return (min - pad, max + pad);
        }

        var headroom = span * 0.1f;
        return (min - headroom, max + headroom);
    }

    private static string BuildMiniActivitySeriesPath(
        IReadOnlyList<float> values,
        double plotWidth,
        double plotHeight,
        double paddingX,
        double paddingY,
        float yMin,
        float yMax,
        bool useSignedLinearScale)
    {
        if (values.Count == 0)
        {
            return string.Empty;
        }

        var builder = new StringBuilder(values.Count * 24);
        var usableWidth = Math.Max(1.0, plotWidth - (paddingX * 2.0));
        var usableHeight = Math.Max(
            1.0,
            plotHeight - ((paddingY + MiniActivityChartPathInsetPx) * 2.0));
        var xStep = values.Count > 1
            ? usableWidth / (values.Count - 1)
            : 0d;
        var yOffset = paddingY + MiniActivityChartPathInsetPx;

        for (var i = 0; i < values.Count; i++)
        {
            var x = paddingX + (i * xStep);
            var ratio = useSignedLinearScale
                ? MiniChartLinearRatio(values[i], yMin, yMax)
                : MiniChartLogRatio(values[i], yMax);
            var y = yOffset + ((1f - ratio) * usableHeight);
            builder.Append(i == 0 ? "M " : " L ");
            builder.Append(x.ToString("0.###", CultureInfo.InvariantCulture));
            builder.Append(' ');
            builder.Append(y.ToString("0.###", CultureInfo.InvariantCulture));
        }

        return builder.ToString();
    }

    private static float MiniChartLinearRatio(float value, float yMin, float yMax)
    {
        if (!float.IsFinite(value))
        {
            return 0f;
        }

        var min = float.IsFinite(yMin) ? yMin : 0f;
        var max = float.IsFinite(yMax) ? yMax : 1f;
        if (!(max > min))
        {
            return 0f;
        }

        var clamped = Math.Clamp(value, min, max);
        return (clamped - min) / (max - min);
    }

    private static float MiniChartLogRatio(float value, float yMax)
    {
        var boundedMax = yMax > 0f && float.IsFinite(yMax) ? yMax : 1f;
        var boundedValue = Math.Clamp(float.IsFinite(value) ? value : 0f, 0f, boundedMax);
        var denominator = MathF.Log(1f + boundedMax);
        if (!(denominator > 0f) || !float.IsFinite(denominator))
        {
            return boundedValue / boundedMax;
        }

        var numerator = MathF.Log(1f + boundedValue);
        return Math.Clamp(numerator / denominator, 0f, 1f);
    }

    private static float MiniChartValueFromLogRatio(float yMax, float ratio)
    {
        var boundedMax = yMax > 0f && float.IsFinite(yMax) ? yMax : 1f;
        var boundedRatio = Math.Clamp(ratio, 0f, 1f);
        var denominator = MathF.Log(1f + boundedMax);
        if (!(denominator > 0f) || !float.IsFinite(denominator))
        {
            return boundedMax * boundedRatio;
        }

        var scaled = denominator * boundedRatio;
        var value = MathF.Exp(scaled) - 1f;
        return Math.Clamp(value, 0f, boundedMax);
    }

    private static string GetMiniActivitySeriesStroke(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return MiniActivityChartSeriesPalette[0];
        }

        var hash = 17;
        foreach (var ch in key)
        {
            hash = (hash * 31) + ch;
        }

        var index = Math.Abs(hash) % MiniActivityChartSeriesPalette.Length;
        return MiniActivityChartSeriesPalette[index];
    }

    private static string FormatMiniChartAxisValue(float value)
    {
        if (!float.IsFinite(value))
        {
            return "n/a";
        }

        var abs = MathF.Abs(value);
        if (abs >= 1_000_000_000f)
        {
            return $"{value / 1_000_000_000f:0.##}B";
        }

        if (abs >= 1_000_000f)
        {
            return $"{value / 1_000_000f:0.##}M";
        }

        if (abs >= 1_000f)
        {
            return $"{value / 1_000f:0.##}K";
        }

        return value.ToString("0.###", CultureInfo.InvariantCulture);
    }

    private void ApplyMiniActivityChartSnapshot(MiniActivityChartRenderSnapshot snapshot)
    {
        ReplaceItems(MiniActivityChartSeries, snapshot.Series);
        MiniActivityChartSeriesLabel = snapshot.SeriesLabel;
        if (snapshot.TickCount > 0)
        {
            var tickRateHz = _currentTargetTickHz.HasValue && _currentTargetTickHz.Value > 0f && float.IsFinite(_currentTargetTickHz.Value)
                ? _currentTargetTickHz.Value
                : DefaultMiniActivityTickRateHz;
            var approxSeconds = snapshot.TickCount / tickRateHz;
            MiniActivityChartRangeLabel = $"{snapshot.RangeLabel} (~{approxSeconds:0.##}s)";
        }
        else
        {
            MiniActivityChartRangeLabel = snapshot.RangeLabel;
        }

        MiniActivityChartMetricLabel = snapshot.MetricLabel;
        MiniActivityYAxisTopLabel = snapshot.YAxisTopLabel;
        MiniActivityYAxisMidLabel = snapshot.YAxisMidLabel;
        MiniActivityYAxisBottomLabel = snapshot.YAxisBottomLabel;
        MiniActivityLegendColumns = snapshot.LegendColumns;
    }

    private VizActivityCanvasInteractionState BuildCanvasInteractionState()
        => new(
            _selectedCanvasNodeKey,
            _selectedCanvasRouteLabel,
            _hoverCanvasNodeKey,
            _hoverCanvasRouteLabel,
            _pinnedCanvasNodes,
            _pinnedCanvasRoutes);

    private VizActivityCanvasRenderOptions BuildCanvasRenderOptions()
        => new(
            SelectedLayoutMode.Mode,
            _canvasViewportScale,
            new VizActivityCanvasLodOptions(
                EnableAdaptiveLod,
                ParseLodRouteBudgetOrDefault(LodLowZoomBudgetText, DefaultLodLowZoomBudget),
                ParseLodRouteBudgetOrDefault(LodMediumZoomBudgetText, DefaultLodMediumZoomBudget),
                ParseLodRouteBudgetOrDefault(LodHighZoomBudgetText, DefaultLodHighZoomBudget)),
            SelectedCanvasTransferCurve.Curve);

    private static int ParseLodRouteBudgetOrDefault(string value, int fallback)
        => TryParseLodRouteBudget(value, out var parsed) ? parsed : fallback;

    private void InvalidatePendingProjectionLayoutRefresh()
    {
        Interlocked.Increment(ref _projectionLayoutRefreshVersion);
        CancelAndDisposeProjectionLayoutCts();
    }

    private void CancelAndDisposeProjectionLayoutCts()
    {
        var cts = Interlocked.Exchange(ref _projectionLayoutRefreshCts, null);
        if (cts is null)
        {
            return;
        }

        cts.Cancel();
        cts.Dispose();
    }

    private void UpdateLodSummary()
    {
        var low = ParseLodRouteBudgetOrDefault(LodLowZoomBudgetText, DefaultLodLowZoomBudget);
        var medium = ParseLodRouteBudgetOrDefault(LodMediumZoomBudgetText, DefaultLodMediumZoomBudget);
        var high = ParseLodRouteBudgetOrDefault(LodHighZoomBudgetText, DefaultLodHighZoomBudget);
        LodSummary = EnableAdaptiveLod
            ? $"Adaptive LOD enabled (routes low/med/high: {low}/{medium}/{high})."
            : "Adaptive LOD disabled (full-route fidelity mode).";
    }

    private static int GetViewportScaleTier(double scale)
    {
        if (scale < 0.9)
        {
            return 0;
        }

        if (scale < 1.8)
        {
            return 1;
        }

        return 2;
    }

    private VizActivityCanvasLayout NormalizeCanvasLayout(VizActivityCanvasLayout canvas)
    {
        var minX = double.PositiveInfinity;
        var minY = double.PositiveInfinity;
        var maxX = double.NegativeInfinity;
        var maxY = double.NegativeInfinity;
        var hasGeometry = false;
        foreach (var node in canvas.Nodes)
        {
            hasGeometry = true;
            minX = Math.Min(minX, node.Left);
            minY = Math.Min(minY, node.Top);
            maxX = Math.Max(maxX, node.Left + node.Diameter);
            maxY = Math.Max(maxY, node.Top + node.Diameter);
        }

        foreach (var edge in canvas.Edges)
        {
            hasGeometry = true;
            var edgePadding = Math.Max(2.0, edge.HitTestThickness * 0.5);
            minX = Math.Min(minX, Math.Min(edge.SourceX, Math.Min(edge.ControlX, edge.TargetX)) - edgePadding);
            minY = Math.Min(minY, Math.Min(edge.SourceY, Math.Min(edge.ControlY, edge.TargetY)) - edgePadding);
            maxX = Math.Max(maxX, Math.Max(edge.SourceX, Math.Max(edge.ControlX, edge.TargetX)) + edgePadding);
            maxY = Math.Max(maxY, Math.Max(edge.SourceY, Math.Max(edge.ControlY, edge.TargetY)) + edgePadding);
        }

        if (!hasGeometry)
        {
            minX = 0.0;
            minY = 0.0;
            maxX = Math.Max(1.0, canvas.Width);
            maxY = Math.Max(1.0, canvas.Height);
        }

        minX -= CanvasBoundsPadding;
        minY -= CanvasBoundsPadding;
        maxX += CanvasBoundsPadding;
        maxY += CanvasBoundsPadding;

        var contentWidth = Math.Max(1.0, maxX - minX);
        var contentHeight = Math.Max(1.0, maxY - minY);
        var width = Math.Max(VizActivityCanvasLayoutBuilder.CanvasWidth, contentWidth + (CanvasNavigationPadding * 2.0));
        var height = Math.Max(VizActivityCanvasLayoutBuilder.CanvasHeight, contentHeight + (CanvasNavigationPadding * 2.0));
        width = StabilizeCanvasDimension(ActivityCanvasWidth, width);
        height = StabilizeCanvasDimension(ActivityCanvasHeight, height);
        var offsetX = ((width - contentWidth) / 2.0) - minX;
        var offsetY = ((height - contentHeight) / 2.0) - minY;
        SetActivityCanvasDimensions(width, height);

        var needsOffset = Math.Abs(offsetX) > 0.0001 || Math.Abs(offsetY) > 0.0001;
        var needsResize = Math.Abs(canvas.Width - width) > 0.0001 || Math.Abs(canvas.Height - height) > 0.0001;
        if (!needsOffset && !needsResize)
        {
            return canvas;
        }

        if (!needsOffset)
        {
            return canvas with
            {
                Width = width,
                Height = height
            };
        }

        var shiftedNodes = new List<VizActivityCanvasNode>(canvas.Nodes.Count);
        foreach (var node in canvas.Nodes)
        {
            shiftedNodes.Add(node with
            {
                Left = node.Left + offsetX,
                Top = node.Top + offsetY
            });
        }

        var shiftedEdges = new List<VizActivityCanvasEdge>(canvas.Edges.Count);
        foreach (var edge in canvas.Edges)
        {
            var sourceX = edge.SourceX + offsetX;
            var sourceY = edge.SourceY + offsetY;
            var controlX = edge.ControlX + offsetX;
            var controlY = edge.ControlY + offsetY;
            var targetX = edge.TargetX + offsetX;
            var targetY = edge.TargetY + offsetY;
            shiftedEdges.Add(edge with
            {
                SourceX = sourceX,
                SourceY = sourceY,
                ControlX = controlX,
                ControlY = controlY,
                TargetX = targetX,
                TargetY = targetY
            });
        }

        return new VizActivityCanvasLayout(
            width,
            height,
            canvas.Legend,
            shiftedNodes,
            shiftedEdges);
    }

    private (VizActivityCanvasLayout Canvas, double AdditionalLayoutMs) FinalizeCanvasLayout(
        VizActivityCanvasLayout canvas,
        VizActivityProjection projection,
        VizActivityProjectionOptions options,
        VizActivityCanvasTopology topology,
        VizActivityCanvasColorMode colorMode,
        VizActivityCanvasRenderOptions renderOptions)
    {
        var normalized = NormalizeCanvasLayout(canvas);
        if (!TrimCanvasInteractionToLayout(normalized.Nodes, normalized.Edges))
        {
            return (normalized, 0.0);
        }

        var rebuildStart = Stopwatch.GetTimestamp();
        normalized = NormalizeCanvasLayout(
            VizActivityCanvasLayoutBuilder.Build(
                projection,
                options,
                BuildCanvasInteractionState(),
                topology,
                colorMode,
                renderOptions));
        return (normalized, StopwatchElapsedMs(rebuildStart));
    }

    private void SetActivityCanvasDimensions(double width, double height)
    {
        ActivityCanvasWidth = Math.Max(1.0, width);
        ActivityCanvasHeight = Math.Max(1.0, height);
    }

    private static double StabilizeCanvasDimension(double current, double target)
    {
        var safeCurrent = Math.Max(1.0, current);
        var safeTarget = Math.Max(1.0, target);
        return Math.Abs(safeTarget - safeCurrent) <= CanvasDimensionJitterTolerance
            ? safeCurrent
            : safeTarget;
    }
    private void RebaseMiniChartWindowOnCadenceChange(float? previousTickHz, float nextTickHz)
    {
        if (!previousTickHz.HasValue
            || !float.IsFinite(previousTickHz.Value)
            || previousTickHz.Value <= 0f
            || !float.IsFinite(nextTickHz)
            || nextTickHz <= 0f)
        {
            return;
        }

        var previous = previousTickHz.Value;
        var relativeDelta = Math.Abs(nextTickHz - previous) / previous;
        if (relativeDelta < 0.01f)
        {
            return;
        }

        var latestTick = GetLatestTickForCurrentSelection();
        if (latestTick == 0)
        {
            return;
        }

        _miniChartMinTickFloor = latestTick;
    }
    private static CollectionDiffStats ApplyKeyedDiff<T>(
        ObservableCollection<T> target,
        IReadOnlyList<T> source,
        Func<T, string?> keySelector)
    {
        if (source.Count == 0)
        {
            var removedCount = target.Count;
            if (removedCount > 0)
            {
                target.Clear();
            }

            return new CollectionDiffStats(0, removedCount, 0, 0);
        }

        var sourceKeys = new List<string>(source.Count);
        var sourceKeySet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var item in source)
        {
            var key = keySelector(item);
            if (string.IsNullOrWhiteSpace(key) || !sourceKeySet.Add(key))
            {
                return ReplaceItemsWithStats(target, source);
            }

            sourceKeys.Add(key);
        }

        var removed = 0;
        var added = 0;
        var moved = 0;
        var updated = 0;
        for (var i = target.Count - 1; i >= 0; i--)
        {
            var existingKey = keySelector(target[i]);
            if (!string.IsNullOrWhiteSpace(existingKey) && sourceKeySet.Contains(existingKey))
            {
                continue;
            }

            target.RemoveAt(i);
            removed++;
        }

        for (var desiredIndex = 0; desiredIndex < source.Count; desiredIndex++)
        {
            var desiredItem = source[desiredIndex];
            var desiredKey = sourceKeys[desiredIndex];
            var hasMatchingItemAtDesiredIndex = desiredIndex < target.Count
                && string.Equals(keySelector(target[desiredIndex]), desiredKey, StringComparison.OrdinalIgnoreCase);
            if (hasMatchingItemAtDesiredIndex)
            {
                if (!EqualityComparer<T>.Default.Equals(target[desiredIndex], desiredItem))
                {
                    target[desiredIndex] = desiredItem;
                    updated++;
                }

                continue;
            }

            var existingIndex = -1;
            for (var scan = desiredIndex + 1; scan < target.Count; scan++)
            {
                if (string.Equals(keySelector(target[scan]), desiredKey, StringComparison.OrdinalIgnoreCase))
                {
                    existingIndex = scan;
                    break;
                }
            }

            if (existingIndex >= 0)
            {
                target.Move(existingIndex, desiredIndex);
                moved++;
                if (!EqualityComparer<T>.Default.Equals(target[desiredIndex], desiredItem))
                {
                    target[desiredIndex] = desiredItem;
                    updated++;
                }

                continue;
            }

            target.Insert(desiredIndex, desiredItem);
            added++;
        }

        while (target.Count > source.Count)
        {
            target.RemoveAt(target.Count - 1);
            removed++;
        }

        return new CollectionDiffStats(added, removed, moved, updated);
    }

    private static CollectionDiffStats ReplaceItemsWithStats<T>(ObservableCollection<T> target, IReadOnlyList<T> source)
    {
        var removedCount = target.Count;
        target.Clear();
        foreach (var item in source)
        {
            target.Add(item);
        }

        return new CollectionDiffStats(source.Count, removedCount, 0, 0);
    }
}
