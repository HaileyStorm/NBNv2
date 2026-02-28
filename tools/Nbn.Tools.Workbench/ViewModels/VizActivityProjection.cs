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
    int MiniChartTickWindow = 64);

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
    float PeakScore);

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
    private const string MiniChartMetricLabel = "score = |value| + |strength| per event contribution";

    public static VizActivityProjection Build(IEnumerable<VizEventItem> events, VizActivityProjectionOptions options)
    {
        var source = events?.ToList() ?? new List<VizEventItem>();
        if (source.Count == 0)
        {
            var miniChart = BuildMiniChart(Array.Empty<VizEventItem>(), minTick: 0, maxTick: 0, options);
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
            var miniChart = BuildMiniChart(Array.Empty<VizEventItem>(), minTick: 0, maxTick: 0, options);
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
        var minTick = latestTick > (ulong)(tickWindow - 1) ? latestTick - (ulong)(tickWindow - 1) : 0;
        var windowed = filtered
            .Where(item => item.TickId >= minTick)
            .ToList();

        var chartTickWindow = NormalizeTickWindow(options.MiniChartTickWindow);
        var chartMinTick = latestTick > (ulong)(chartTickWindow - 1) ? latestTick - (ulong)(chartTickWindow - 1) : 0;
        var chartWindowed = filtered
            .Where(item => item.TickId >= chartMinTick)
            .ToList();

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
        var chart = BuildMiniChart(chartWindowed, chartMinTick, latestTick, options);

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
        var topSeriesCount = NormalizeTopSeriesCount(options.TopSeriesCount);
        var modeLabel = options.FocusRegionId is uint focusRegionId
            ? $"Top {topSeriesCount} neurons in R{focusRegionId}"
            : $"Top {topSeriesCount} regions";

        if (!options.EnableMiniChart)
        {
            return new VizMiniActivityChart(
                Enabled: false,
                ModeLabel: modeLabel,
                MetricLabel: MiniChartMetricLabel,
                MinTick: 0,
                MaxTick: 0,
                Ticks: Array.Empty<ulong>(),
                Series: Array.Empty<VizMiniActivitySeries>(),
                PeakScore: 0f);
        }

        if (windowed.Count == 0 || maxTick < minTick)
        {
            return new VizMiniActivityChart(
                Enabled: true,
                ModeLabel: modeLabel,
                MetricLabel: MiniChartMetricLabel,
                MinTick: 0,
                MaxTick: 0,
                Ticks: Array.Empty<ulong>(),
                Series: Array.Empty<VizMiniActivitySeries>(),
                PeakScore: 0f);
        }

        var earliestAvailableTick = windowed.Min(item => item.TickId);
        var effectiveMinTick = Math.Max(minTick, earliestAvailableTick);
        if (maxTick < effectiveMinTick)
        {
            return new VizMiniActivityChart(
                Enabled: true,
                ModeLabel: modeLabel,
                MetricLabel: MiniChartMetricLabel,
                MinTick: 0,
                MaxTick: 0,
                Ticks: Array.Empty<ulong>(),
                Series: Array.Empty<VizMiniActivitySeries>(),
                PeakScore: 0f);
        }

        var tickCount = (int)(maxTick - effectiveMinTick + 1);
        var ticks = new List<ulong>(tickCount);
        for (var i = 0; i < tickCount; i++)
        {
            ticks.Add(effectiveMinTick + (ulong)i);
        }

        var trendByEntity = options.FocusRegionId is uint focusedRegionId
            ? BuildFocusNeuronTrendMap(windowed, effectiveMinTick, tickCount, focusedRegionId)
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
        foreach (var item in series)
        {
            foreach (var value in item.Values)
            {
                if (value > peakScore)
                {
                    peakScore = value;
                }
            }
        }

        return new VizMiniActivityChart(
            Enabled: true,
            ModeLabel: modeLabel,
            MetricLabel: MiniChartMetricLabel,
            MinTick: effectiveMinTick,
            MaxTick: maxTick,
            Ticks: ticks,
            Series: series,
            PeakScore: peakScore);
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
        int tickCount,
        uint focusRegionId)
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

            var hasSource = TryParseAddress(item.Source, out var sourceRegionId, out var sourceNeuronId);
            var hasTarget = TryParseAddress(item.Target, out var targetRegionId, out var targetNeuronId);
            var sourceMatches = hasSource && sourceRegionId == focusRegionId;
            var targetMatches = hasTarget && targetRegionId == focusRegionId;
            if (!sourceMatches && !targetMatches)
            {
                continue;
            }

            if (sourceMatches)
            {
                AddNeuronContribution(
                    trends,
                    focusRegionId,
                    sourceNeuronId,
                    score,
                    tickIndex,
                    item.TickId,
                    tickCount);
            }

            if (targetMatches && (!sourceMatches || targetNeuronId != sourceNeuronId))
            {
                AddNeuronContribution(
                    trends,
                    focusRegionId,
                    targetNeuronId,
                    score,
                    tickIndex,
                    item.TickId,
                    tickCount);
            }
        }

        return trends;
    }

    private static float ComputeActivityScore(VizEventItem item)
    {
        var score = MathF.Abs(item.Value) + MathF.Abs(item.Strength);
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

        return type.Contains("AXON", StringComparison.OrdinalIgnoreCase)
               || type.Contains("NEURON", StringComparison.OrdinalIgnoreCase);
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
        public TrendAccumulator(string key, string label, int tickCount)
        {
            Key = key;
            Label = label;
            Values = new float[tickCount];
        }

        public string Key { get; }

        public string Label { get; }

        public float[] Values { get; }

        public float TotalScore { get; private set; }

        public ulong LastActiveTick { get; private set; }

        public void Add(int tickIndex, float score, ulong tickId)
        {
            Values[tickIndex] += score;
            TotalScore += score;
            if (tickId >= LastActiveTick)
            {
                LastActiveTick = tickId;
            }
        }
    }
}
