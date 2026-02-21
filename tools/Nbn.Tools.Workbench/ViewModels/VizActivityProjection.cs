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
    uint? FocusRegionId);

public sealed record VizActivityProjection(
    string Summary,
    IReadOnlyList<VizActivityStatItem> Stats,
    IReadOnlyList<VizRegionActivityItem> Regions,
    IReadOnlyList<VizEdgeActivityItem> Edges,
    IReadOnlyList<VizTickActivityItem> Ticks,
    IReadOnlyList<VizEventItem> WindowEvents);

public sealed record VizActivityStatItem(string Label, string Value, string Detail);

public sealed record VizRegionActivityItem(
    uint RegionId,
    string RegionLabel,
    int EventCount,
    ulong LastTick,
    int FiredCount,
    int AxonCount,
    string DominantType,
    float AverageMagnitude);

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

public static class VizActivityProjectionBuilder
{
    private const int DefaultTickWindow = 64;
    private const int MaxTickWindow = 4096;
    private const int MaxTickRows = 20;
    private const float LowSignalThreshold = 1e-5f;

    public static VizActivityProjection Build(IEnumerable<VizEventItem> events, VizActivityProjectionOptions options)
    {
        var source = events?.ToList() ?? new List<VizEventItem>();
        if (source.Count == 0)
        {
            return new VizActivityProjection(
                "No visualization events in the current filter.",
                new[] { new VizActivityStatItem("Events", "0", "Awaiting stream data") },
                Array.Empty<VizRegionActivityItem>(),
                Array.Empty<VizEdgeActivityItem>(),
                Array.Empty<VizTickActivityItem>(),
                Array.Empty<VizEventItem>());
        }

        var tickWindow = NormalizeTickWindow(options.TickWindow);
        var filtered = source
            .Where(item => ShouldKeepEvent(item, options.IncludeLowSignalEvents))
            .ToList();

        if (filtered.Count == 0)
        {
            return new VizActivityProjection(
                "All events were filtered out by current options.",
                new[]
                {
                    new VizActivityStatItem("Events", "0", "Try enabling low-signal events.")
                },
                Array.Empty<VizRegionActivityItem>(),
                Array.Empty<VizEdgeActivityItem>(),
                Array.Empty<VizTickActivityItem>(),
                Array.Empty<VizEventItem>());
        }

        var latestTick = filtered.Max(item => item.TickId);
        var minTick = latestTick > (ulong)(tickWindow - 1) ? latestTick - (ulong)(tickWindow - 1) : 0;
        var windowed = filtered
            .Where(item => item.TickId >= minTick)
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

        return new VizActivityProjection(summary, stats, regionRows, edgeRows, tickRows, windowed);
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
                    AverageMagnitude(items.Select(item => item.Value)));
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
}
