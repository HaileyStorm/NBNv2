using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed record VizActivityProjectionOptions(
    int TickWindow,
    bool IncludeLowSignalEvents,
    uint? FocusRegionId,
    int TopSeriesCount = 8,
    bool EnableMiniChart = true,
    int MiniChartTickWindow = 64,
    ulong? MiniChartMinTickFloor = null,
    ulong? LatestTickHint = null);

public sealed record VizActivityProjection(
    string Summary,
    IReadOnlyList<VizActivityStatItem> Stats,
    IReadOnlyList<VizRegionActivityItem> Regions,
    IReadOnlyList<VizEdgeActivityItem> Edges,
    IReadOnlyList<VizTickActivityItem> Ticks,
    IReadOnlyList<VizEventItem> WindowEvents,
    VizMiniActivityChart MiniChart);

public sealed record VizActivityStatItem(string Label, string Value, string Detail);

public sealed record VizRegionActivityItem(
    uint RegionId,
    string RegionLabel,
    int EventCount,
    ulong LastTick,
    int FiredCount,
    int AxonCount,
    string DominantType,
    float AverageMagnitude,
    float AverageSignedValue);

public sealed record VizEdgeActivityItem(
    string RouteLabel,
    int EventCount,
    ulong LastTick,
    float AverageMagnitude,
    float AverageStrength,
    float AverageSignedValue,
    float AverageSignedStrength,
    uint? SourceRegionId,
    uint? TargetRegionId);

public sealed record VizTickActivityItem(
    ulong TickId,
    int EventCount,
    int FiredCount,
    int AxonCount,
    int BufferCount);

public sealed record VizMiniActivityChart(
    bool Enabled,
    string ModeLabel,
    string MetricLabel,
    ulong MinTick,
    ulong MaxTick,
    IReadOnlyList<ulong> Ticks,
    IReadOnlyList<VizMiniActivitySeries> Series,
    float PeakScore,
    float MinScore = 0f,
    bool UseSignedLinearScale = false);

public sealed record VizMiniActivitySeries(
    string Key,
    string Label,
    float TotalScore,
    ulong LastActiveTick,
    IReadOnlyList<float> Values);

public static class VizActivityProjectionBuilder
{
    private const int DefaultTickWindow = 64;
    private const int MaxTickWindow = 4096;
    private const int MaxTickRows = 20;
    private const int DefaultMiniChartTopSeriesCount = 8;
    private const int MaxMiniChartTopSeriesCount = 32;
    private const float LowSignalThreshold = 1e-5f;
    private const string MiniChartScoreMetricLabel = "score = 1 + |value| + |strength| per event contribution";
    private const string MiniChartFocusBufferMetricLabel = "value = neuron buffer (signed, VizNeuronBuffer)";

    public static VizActivityProjection Build(IEnumerable<VizEventItem> events, VizActivityProjectionOptions options)
    {
        var source = events?.ToList() ?? new List<VizEventItem>();
        var latestTickHint = options.LatestTickHint.GetValueOrDefault();
        var hasLatestTickHint = options.LatestTickHint.HasValue;
        if (source.Count == 0)
        {
            var minTickFromHint = hasLatestTickHint
                ? ComputeWindowStart(latestTickHint, NormalizeTickWindow(options.MiniChartTickWindow))
                : 0UL;
            minTickFromHint = ApplyMiniChartTickFloor(minTickFromHint, latestTickHint, options.MiniChartMinTickFloor);
            var miniChart = BuildMiniChart(
                windowed: Array.Empty<VizEventItem>(),
                minTick: minTickFromHint,
                maxTick: latestTickHint,
                options);
            return new VizActivityProjection(
                "No visualization events in the current filter.",
                new[] { new VizActivityStatItem("Events", "0", "Awaiting stream data") },
                Array.Empty<VizRegionActivityItem>(),
                Array.Empty<VizEdgeActivityItem>(),
                Array.Empty<VizTickActivityItem>(),
                Array.Empty<VizEventItem>(),
                miniChart);
        }

        var tickWindow = NormalizeTickWindow(options.TickWindow);
        var filtered = source
            .Where(item => ShouldKeepEvent(item, options.IncludeLowSignalEvents))
            .ToList();

        if (filtered.Count == 0)
        {
            var minTickFromHint = hasLatestTickHint
                ? ComputeWindowStart(latestTickHint, NormalizeTickWindow(options.MiniChartTickWindow))
                : 0UL;
            minTickFromHint = ApplyMiniChartTickFloor(minTickFromHint, latestTickHint, options.MiniChartMinTickFloor);
            var miniChart = BuildMiniChart(
                windowed: Array.Empty<VizEventItem>(),
                minTick: minTickFromHint,
                maxTick: latestTickHint,
                options);
            return new VizActivityProjection(
                "All events were filtered out by current options.",
                new[]
                {
                    new VizActivityStatItem("Events", "0", "Try enabling low-signal events.")
                },
                Array.Empty<VizRegionActivityItem>(),
                Array.Empty<VizEdgeActivityItem>(),
                Array.Empty<VizTickActivityItem>(),
                Array.Empty<VizEventItem>(),
                miniChart);
        }

        var latestTick = filtered.Max(item => item.TickId);
        if (hasLatestTickHint && latestTickHint > latestTick)
        {
            latestTick = latestTickHint;
        }

        var minTick = ComputeWindowStart(latestTick, tickWindow);
        var windowed = filtered
            .Where(item => item.TickId >= minTick)
            .ToList();

        var chartTickWindow = NormalizeTickWindow(options.MiniChartTickWindow);
        var chartMinTick = ComputeWindowStart(latestTick, chartTickWindow);
        chartMinTick = ApplyMiniChartTickFloor(chartMinTick, latestTick, options.MiniChartMinTickFloor);

        var regionRows = BuildRegionRows(windowed);
        var edgeRows = BuildEdgeRows(windowed);
        var tickRows = BuildTickRows(windowed);
        var uniqueRegionCount = regionRows.Select(item => item.RegionId).Distinct().Count();
        var uniqueTypeCount = windowed
            .Select(item => item.Type)
            .Where(type => !string.IsNullOrWhiteSpace(type))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Count();
        var focusCount = options.FocusRegionId is uint focusRegion
            ? windowed.Count(item => EventTouchesRegion(item, focusRegion))
            : windowed.Count;

        var stats = new List<VizActivityStatItem>
        {
            new("Events in window", windowed.Count.ToString(CultureInfo.InvariantCulture), $"Filtered total: {filtered.Count}"),
            new("Ticks covered", tickRows.Count.ToString(CultureInfo.InvariantCulture), $"Window: {tickWindow} ticks"),
            new("Regions active", uniqueRegionCount.ToString(CultureInfo.InvariantCulture), "By event region id"),
            new("Edge routes", edgeRows.Count.ToString(CultureInfo.InvariantCulture), "Unique source/target pairs"),
            new("Event types", uniqueTypeCount.ToString(CultureInfo.InvariantCulture), "Distinct visualization event types")
        };

        if (options.FocusRegionId is uint regionId)
        {
            stats.Add(new(
                "Focus coverage",
                focusCount.ToString(CultureInfo.InvariantCulture),
                $"Events touching R{regionId}"));
        }

        var summary = options.FocusRegionId is uint focused
            ? $"Ticks {minTick}..{latestTick} | {windowed.Count} events | focus R{focused}: {focusCount}"
            : $"Ticks {minTick}..{latestTick} | {windowed.Count} events | {uniqueRegionCount} regions";
        var chart = BuildMiniChart(filtered, chartMinTick, latestTick, options);

        return new VizActivityProjection(summary, stats, regionRows, edgeRows, tickRows, windowed, chart);
    }

    private static IReadOnlyList<VizRegionActivityItem> BuildRegionRows(IReadOnlyList<VizEventItem> events)
    {
        return events
            .Select(item => new { Item = item, RegionId = TryParseRegion(item.Region, out var regionId) ? regionId : (uint?)null })
            .Where(row => row.RegionId.HasValue)
            .GroupBy(row => row.RegionId!.Value)
            .Select(group =>
            {
                var items = group.Select(row => row.Item).ToList();
                var dominantType = items
                    .GroupBy(item => item.Type, StringComparer.OrdinalIgnoreCase)
                    .OrderByDescending(typeGroup => typeGroup.Count())
                    .ThenBy(typeGroup => typeGroup.Key, StringComparer.OrdinalIgnoreCase)
                    .Select(typeGroup => typeGroup.Key)
                    .FirstOrDefault() ?? "unknown";

                return new VizRegionActivityItem(
                    group.Key,
                    $"R{group.Key}",
                    items.Count,
                    items.Max(item => item.TickId),
                    items.Count(item => IsFiredType(item.Type)),
                    items.Count(item => IsAxonType(item.Type)),
                    dominantType,
                    AverageMagnitude(items.Select(item => item.Value)),
                    AverageSigned(items.Select(item => item.Value)));
            })
            .OrderByDescending(item => item.EventCount)
            .ThenByDescending(item => item.LastTick)
            .ThenBy(item => item.RegionId)
            .ToList();
    }

    private static IReadOnlyList<VizEdgeActivityItem> BuildEdgeRows(IReadOnlyList<VizEventItem> events)
    {
        var grouped = events
            .Where(item => !string.IsNullOrWhiteSpace(item.Source) && !string.IsNullOrWhiteSpace(item.Target))
            .GroupBy(item => $"{item.Source}->{item.Target}", StringComparer.Ordinal)
            .Select(group =>
            {
                var first = group.First();
                var sourceRegion = TryParseAddressRegion(first.Source, out var sourceRid) ? sourceRid : (uint?)null;
                var targetRegion = TryParseAddressRegion(first.Target, out var targetRid) ? targetRid : (uint?)null;
                var routeLabel = BuildRouteLabel(first.Source, first.Target, sourceRegion, targetRegion);

                return new VizEdgeActivityItem(
                    routeLabel,
                    group.Count(),
                    group.Max(item => item.TickId),
                    AverageMagnitude(group.Select(item => item.Value)),
                    AverageMagnitude(group.Select(item => item.Strength)),
                    AverageSigned(group.Select(item => item.Value)),
                    AverageSigned(group.Select(item => item.Strength)),
                    sourceRegion,
                    targetRegion);
            });

        return grouped
            .OrderByDescending(item => item.EventCount)
            .ThenByDescending(item => item.LastTick)
            .ThenBy(item => item.RouteLabel, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static IReadOnlyList<VizTickActivityItem> BuildTickRows(IReadOnlyList<VizEventItem> events)
    {
        return events
            .GroupBy(item => item.TickId)
            .Select(group => new VizTickActivityItem(
                group.Key,
                group.Count(),
                group.Count(item => IsFiredType(item.Type)),
                group.Count(item => IsAxonType(item.Type)),
                group.Count(item => IsBufferType(item.Type))))
            .OrderByDescending(item => item.TickId)
            .Take(MaxTickRows)
            .ToList();
    }

    private static VizMiniActivityChart BuildMiniChart(
        IReadOnlyList<VizEventItem> windowed,
        ulong minTick,
        ulong maxTick,
        VizActivityProjectionOptions options)
    {
        var focusMode = options.FocusRegionId.HasValue;
        var topSeriesCount = NormalizeTopSeriesCount(options.TopSeriesCount);
        var modeLabel = options.FocusRegionId is uint focusRegionId
            ? $"Top {topSeriesCount} neurons in R{focusRegionId}"
            : $"Top {topSeriesCount} regions";
        var metricLabel = focusMode ? MiniChartFocusBufferMetricLabel : MiniChartScoreMetricLabel;

        if (!options.EnableMiniChart)
        {
            return new VizMiniActivityChart(
                Enabled: false,
                ModeLabel: modeLabel,
                MetricLabel: metricLabel,
                MinTick: 0,
                MaxTick: 0,
                Ticks: Array.Empty<ulong>(),
                Series: Array.Empty<VizMiniActivitySeries>(),
                PeakScore: 0f,
                MinScore: 0f,
                UseSignedLinearScale: focusMode);
        }

        if (windowed.Count == 0 || maxTick < minTick)
        {
            return new VizMiniActivityChart(
                Enabled: true,
                ModeLabel: modeLabel,
                MetricLabel: metricLabel,
                MinTick: 0,
                MaxTick: 0,
                Ticks: Array.Empty<ulong>(),
                Series: Array.Empty<VizMiniActivitySeries>(),
                PeakScore: 0f,
                MinScore: 0f,
                UseSignedLinearScale: focusMode);
        }

        var effectiveMinTick = minTick;
        if (maxTick < effectiveMinTick)
        {
            return new VizMiniActivityChart(
                Enabled: true,
                ModeLabel: modeLabel,
                MetricLabel: metricLabel,
                MinTick: 0,
                MaxTick: 0,
                Ticks: Array.Empty<ulong>(),
                Series: Array.Empty<VizMiniActivitySeries>(),
                PeakScore: 0f,
                MinScore: 0f,
                UseSignedLinearScale: focusMode);
        }

        var tickCount = (int)(maxTick - effectiveMinTick + 1);
        var ticks = new List<ulong>(tickCount);
        for (var i = 0; i < tickCount; i++)
        {
            ticks.Add(effectiveMinTick + (ulong)i);
        }

        var trendByEntity = options.FocusRegionId is uint focusedRegionId
            ? BuildFocusNeuronTrendMap(windowed, effectiveMinTick, maxTick, tickCount, focusedRegionId)
            : BuildRegionTrendMap(windowed, effectiveMinTick, tickCount);
        var series = trendByEntity.Values
            .OrderByDescending(item => item.TotalScore)
            .ThenByDescending(item => item.LastActiveTick)
            .ThenBy(item => item.Label, StringComparer.Ordinal)
            .Take(topSeriesCount)
            .Select(item => new VizMiniActivitySeries(
                item.Key,
                item.Label,
                item.TotalScore,
                item.LastActiveTick,
                item.Values))
            .ToList();

        var peakScore = 0f;
        var minScore = 0f;
        var hasFiniteValue = false;
        foreach (var item in series)
        {
            foreach (var value in item.Values)
            {
                if (!float.IsFinite(value))
                {
                    continue;
                }

                if (focusMode)
                {
                    var abs = MathF.Abs(value);
                    if (abs > peakScore)
                    {
                        peakScore = abs;
                    }

                    if (!hasFiniteValue || value < minScore)
                    {
                        minScore = value;
                    }

                    hasFiniteValue = true;
                }
                else if (value > peakScore)
                {
                    peakScore = value;
                }
            }
        }

        return new VizMiniActivityChart(
            Enabled: true,
            ModeLabel: modeLabel,
            MetricLabel: metricLabel,
            MinTick: effectiveMinTick,
            MaxTick: maxTick,
            Ticks: ticks,
            Series: series,
            PeakScore: peakScore,
            MinScore: hasFiniteValue ? minScore : 0f,
            UseSignedLinearScale: focusMode);
    }

    private static Dictionary<string, TrendAccumulator> BuildRegionTrendMap(
        IReadOnlyList<VizEventItem> events,
        ulong minTick,
        int tickCount)
    {
        var trends = new Dictionary<string, TrendAccumulator>(StringComparer.Ordinal);
        foreach (var item in events)
        {
            if (item.TickId < minTick)
            {
                continue;
            }

            var tickIndex = (int)(item.TickId - minTick);
            if (tickIndex < 0 || tickIndex >= tickCount)
            {
                continue;
            }

            var score = ComputeActivityScore(item);
            if (!(score > 0f))
            {
                continue;
            }

            var hasEventRegion = TryParseRegion(item.Region, out var eventRegionId);
            var hasSourceRegion = TryParseAddressRegion(item.Source, out var sourceRegionId);
            var hasTargetRegion = TryParseAddressRegion(item.Target, out var targetRegionId);

            if (hasEventRegion)
            {
                AddRegionContribution(trends, eventRegionId, score, tickIndex, item.TickId, tickCount);
            }

            if (hasSourceRegion && (!hasEventRegion || sourceRegionId != eventRegionId))
            {
                AddRegionContribution(trends, sourceRegionId, score, tickIndex, item.TickId, tickCount);
            }

            if (hasTargetRegion
                && (!hasEventRegion || targetRegionId != eventRegionId)
                && (!hasSourceRegion || targetRegionId != sourceRegionId))
            {
                AddRegionContribution(trends, targetRegionId, score, tickIndex, item.TickId, tickCount);
            }
        }

        return trends;
    }

    private static Dictionary<string, TrendAccumulator> BuildFocusNeuronTrendMap(
        IReadOnlyList<VizEventItem> events,
        ulong minTick,
        ulong maxTick,
        int tickCount,
        uint focusRegionId)
    {
        var trends = new Dictionary<string, TrendAccumulator>(StringComparer.Ordinal);
        var baselineByNeuron = new Dictionary<string, (float Value, ulong TickId)>(StringComparer.Ordinal);
        foreach (var item in events)
        {
            if (!IsBufferType(item.Type))
            {
                continue;
            }

            if (!TryParseAddress(item.Source, out var sourceRegionId, out var sourceNeuronId)
                || sourceRegionId != focusRegionId
                || !float.IsFinite(item.Value))
            {
                continue;
            }

            var key = $"neuron:{focusRegionId}:{sourceNeuronId}";
            if (!trends.TryGetValue(key, out var accumulator))
            {
                accumulator = new TrendAccumulator(key, $"R{focusRegionId}N{sourceNeuronId}", tickCount);
                trends[key] = accumulator;
            }

            if (item.TickId < minTick)
            {
                if (!baselineByNeuron.TryGetValue(key, out var baseline) || item.TickId >= baseline.TickId)
                {
                    baselineByNeuron[key] = (item.Value, item.TickId);
                }

                continue;
            }

            if (item.TickId > maxTick)
            {
                continue;
            }

            var tickIndex = (int)(item.TickId - minTick);
            if (tickIndex < 0 || tickIndex >= tickCount)
            {
                continue;
            }

            AddNeuronBufferSample(
                trends,
                focusRegionId,
                sourceNeuronId,
                item.Value,
                tickIndex,
                item.TickId,
                tickCount);
        }

        foreach (var (key, accumulator) in trends)
        {
            var hasCurrent = baselineByNeuron.TryGetValue(key, out var baseline);
            var current = hasCurrent ? baseline.Value : 0f;
            for (var tickIndex = 0; tickIndex < tickCount; tickIndex++)
            {
                if (accumulator.TryGetSample(tickIndex, out var explicitValue))
                {
                    current = explicitValue;
                    hasCurrent = true;
                    continue;
                }

                if (!hasCurrent)
                {
                    continue;
                }

                var tickId = minTick + (ulong)tickIndex;
                accumulator.SetHeldSample(tickIndex, current, MathF.Abs(current), tickId);
            }
        }

        return trends;
    }

    private static float ComputeActivityScore(VizEventItem item)
    {
        var score = 1f + MathF.Abs(item.Value) + MathF.Abs(item.Strength);
        return float.IsFinite(score) ? score : 0f;
    }

    private static void AddRegionContribution(
        IDictionary<string, TrendAccumulator> trends,
        uint regionId,
        float score,
        int tickIndex,
        ulong tickId,
        int tickCount)
    {
        var key = $"region:{regionId}";
        if (!trends.TryGetValue(key, out var accumulator))
        {
            accumulator = new TrendAccumulator(key, $"R{regionId}", tickCount);
            trends[key] = accumulator;
        }

        accumulator.Add(tickIndex, score, tickId);
    }

    private static void AddNeuronContribution(
        IDictionary<string, TrendAccumulator> trends,
        uint regionId,
        uint neuronId,
        float score,
        int tickIndex,
        ulong tickId,
        int tickCount)
    {
        var key = $"neuron:{regionId}:{neuronId}";
        if (!trends.TryGetValue(key, out var accumulator))
        {
            accumulator = new TrendAccumulator(key, $"R{regionId}N{neuronId}", tickCount);
            trends[key] = accumulator;
        }

        accumulator.Add(tickIndex, score, tickId);
    }

    private static void AddNeuronBufferSample(
        IDictionary<string, TrendAccumulator> trends,
        uint regionId,
        uint neuronId,
        float bufferValue,
        int tickIndex,
        ulong tickId,
        int tickCount)
    {
        var key = $"neuron:{regionId}:{neuronId}";
        if (!trends.TryGetValue(key, out var accumulator))
        {
            accumulator = new TrendAccumulator(key, $"R{regionId}N{neuronId}", tickCount);
            trends[key] = accumulator;
        }

        accumulator.SetSample(tickIndex, bufferValue, MathF.Abs(bufferValue), tickId);
    }

    private static bool ShouldKeepEvent(VizEventItem item, bool includeLowSignalEvents)
    {
        if (includeLowSignalEvents || !UsesSignalMagnitude(item.Type))
        {
            return true;
        }

        return Math.Abs(item.Value) > LowSignalThreshold || Math.Abs(item.Strength) > LowSignalThreshold;
    }

    private static bool EventTouchesRegion(VizEventItem item, uint regionId)
    {
        if (TryParseRegion(item.Region, out var rowRegion) && rowRegion == regionId)
        {
            return true;
        }

        if (TryParseAddressRegion(item.Source, out var sourceRegion) && sourceRegion == regionId)
        {
            return true;
        }

        return TryParseAddressRegion(item.Target, out var targetRegion) && targetRegion == regionId;
    }

    private static bool UsesSignalMagnitude(string? type)
    {
        if (string.IsNullOrWhiteSpace(type))
        {
            return false;
        }

        // Keep low-signal filtering focused on event magnitudes that represent
        // sparse transport/spike activity. Buffer streams are sampled state and
        // must remain visible (including zeros) so charts keep advancing.
        return IsAxonType(type) || IsFiredType(type);
    }

    private static ulong ComputeWindowStart(ulong latestTick, int windowSize)
    {
        var clampedWindow = NormalizeTickWindow(windowSize);
        return latestTick > (ulong)(clampedWindow - 1)
            ? latestTick - (ulong)(clampedWindow - 1)
            : 0;
    }

    private static ulong ApplyMiniChartTickFloor(ulong minTick, ulong maxTick, ulong? minTickFloor)
    {
        if (!minTickFloor.HasValue)
        {
            return minTick;
        }

        var floor = minTickFloor.Value;
        if (floor > maxTick)
        {
            return maxTick;
        }

        return Math.Max(minTick, floor);
    }

    private static bool IsAxonType(string? type)
        => !string.IsNullOrWhiteSpace(type) && type.Contains("AXON", StringComparison.OrdinalIgnoreCase);

    private static bool IsFiredType(string? type)
        => !string.IsNullOrWhiteSpace(type) && type.Contains("FIRED", StringComparison.OrdinalIgnoreCase);

    private static bool IsBufferType(string? type)
        => !string.IsNullOrWhiteSpace(type) && type.Contains("BUFFER", StringComparison.OrdinalIgnoreCase);

    private static int NormalizeTickWindow(int value)
    {
        if (value <= 0)
        {
            return DefaultTickWindow;
        }

        return Math.Min(value, MaxTickWindow);
    }

    private static int NormalizeTopSeriesCount(int value)
    {
        if (value <= 0)
        {
            return DefaultMiniChartTopSeriesCount;
        }

        return Math.Min(value, MaxMiniChartTopSeriesCount);
    }

    private static float AverageMagnitude(IEnumerable<float> values)
    {
        var total = 0f;
        var count = 0;
        foreach (var value in values)
        {
            total += Math.Abs(value);
            count++;
        }

        return count == 0 ? 0f : total / count;
    }

    private static float AverageSigned(IEnumerable<float> values)
    {
        var total = 0f;
        var count = 0;
        foreach (var value in values)
        {
            total += value;
            count++;
        }

        return count == 0 ? 0f : total / count;
    }

    private static string BuildRouteLabel(string source, string target, uint? sourceRegion, uint? targetRegion)
    {
        if (sourceRegion is null || targetRegion is null)
        {
            return $"{source} -> {target}";
        }

        return $"R{sourceRegion}N{NeuronIdFromAddress(source)} -> R{targetRegion}N{NeuronIdFromAddress(target)}";
    }

    private static string NeuronIdFromAddress(string addressText)
    {
        if (!uint.TryParse(addressText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var address))
        {
            if (TryParseRegionToken(addressText, out _, out var remainder)
                && !string.IsNullOrWhiteSpace(remainder)
                && (remainder[0] == 'N' || remainder[0] == 'n'))
            {
                var neuronText = remainder[1..];
                if (uint.TryParse(neuronText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var neuronId))
                {
                    return neuronId.ToString(CultureInfo.InvariantCulture);
                }
            }

            return "?";
        }

        return (address & NbnConstants.AddressNeuronMask).ToString(CultureInfo.InvariantCulture);
    }

    private static bool TryParseRegion(string? value, out uint regionId)
    {
        regionId = 0;
        if (!uint.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            if (!TryParseRegionToken(value, out parsed, out _))
            {
                return false;
            }
        }

        if (parsed > NbnConstants.RegionMaxId)
        {
            return false;
        }

        regionId = parsed;
        return true;
    }

    private static bool TryParseAddressRegion(string? value, out uint regionId)
    {
        regionId = 0;
        if (!uint.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            if (!TryParseRegionToken(value, out regionId, out _))
            {
                return false;
            }

            return true;
        }

        var parsedRegion = parsed >> NbnConstants.AddressNeuronBits;
        if (parsedRegion > NbnConstants.RegionMaxId)
        {
            return false;
        }

        regionId = parsedRegion;
        return true;
    }

    private static bool TryParseAddress(string? value, out uint regionId, out uint neuronId)
    {
        regionId = 0;
        neuronId = 0;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        if (uint.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var encodedAddress))
        {
            var parsedRegion = encodedAddress >> NbnConstants.AddressNeuronBits;
            if (parsedRegion > NbnConstants.RegionMaxId)
            {
                return false;
            }

            regionId = parsedRegion;
            neuronId = encodedAddress & NbnConstants.AddressNeuronMask;
            return true;
        }

        if (!TryParseRegionToken(value, out var parsedRegionId, out var remainder)
            || string.IsNullOrWhiteSpace(remainder)
            || (remainder[0] != 'N' && remainder[0] != 'n'))
        {
            return false;
        }

        if (!uint.TryParse(remainder[1..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedNeuronId))
        {
            return false;
        }

        regionId = parsedRegionId;
        neuronId = parsedNeuronId;
        return true;
    }

    private static bool TryParseRegionToken(string? value, out uint regionId, out string remainder)
    {
        regionId = 0;
        remainder = string.Empty;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        if (trimmed.Length < 2 || (trimmed[0] != 'R' && trimmed[0] != 'r'))
        {
            return false;
        }

        var end = 1;
        while (end < trimmed.Length && char.IsDigit(trimmed[end]))
        {
            end++;
        }

        if (end == 1)
        {
            return false;
        }

        var number = trimmed[1..end];
        if (!uint.TryParse(number, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            || parsed > NbnConstants.RegionMaxId)
        {
            return false;
        }

        regionId = parsed;
        remainder = end < trimmed.Length ? trimmed[end..] : string.Empty;
        return true;
    }

    private sealed class TrendAccumulator
    {
        private readonly bool[] _sampled;

        public TrendAccumulator(string key, string label, int tickCount)
        {
            Key = key;
            Label = label;
            Values = new float[tickCount];
            _sampled = new bool[tickCount];
        }

        public string Key { get; }

        public string Label { get; }

        public float[] Values { get; }

        public float TotalScore { get; private set; }

        public ulong LastActiveTick { get; private set; }

        public void Add(int tickIndex, float score, ulong tickId)
        {
            Values[tickIndex] += score;
            _sampled[tickIndex] = true;
            TotalScore += score;
            if (tickId >= LastActiveTick)
            {
                LastActiveTick = tickId;
            }
        }

        public void SetSample(int tickIndex, float value, float rankingScore, ulong tickId)
        {
            Values[tickIndex] = value;
            _sampled[tickIndex] = true;
            TotalScore += rankingScore;
            if (tickId >= LastActiveTick)
            {
                LastActiveTick = tickId;
            }
        }

        public bool TryGetSample(int tickIndex, out float value)
        {
            if (tickIndex < 0 || tickIndex >= _sampled.Length || !_sampled[tickIndex])
            {
                value = 0f;
                return false;
            }

            value = Values[tickIndex];
            return true;
        }

        public void SetHeldSample(int tickIndex, float value, float rankingScore, ulong tickId)
        {
            if (tickIndex < 0 || tickIndex >= _sampled.Length || _sampled[tickIndex])
            {
                return;
            }

            Values[tickIndex] = value;
            _sampled[tickIndex] = true;
            TotalScore += rankingScore;
            if (tickId >= LastActiveTick)
            {
                LastActiveTick = tickId;
            }
        }
    }
}
