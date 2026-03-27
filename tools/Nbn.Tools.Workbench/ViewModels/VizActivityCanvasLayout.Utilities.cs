using System;
using System.Collections.Generic;
using System.Globalization;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;

namespace Nbn.Tools.Workbench.ViewModels;

public static partial class VizActivityCanvasLayoutBuilder
{
    private static ulong ResolveLatestTick(VizActivityProjection projection)
    {
        if (projection.Ticks.Count > 0)
        {
            return projection.Ticks.Max(item => item.TickId);
        }

        if (projection.Regions.Count > 0)
        {
            return projection.Regions.Max(item => item.LastTick);
        }

        return 0;
    }

    private static string BuildFocusedDisplayRouteLabel(string sourceKey, string targetKey, uint focusRegionId)
        => $"{BuildFocusedNodeLabel(sourceKey, focusRegionId)} -> {BuildFocusedNodeLabel(targetKey, focusRegionId)}";

    private static string BuildFocusedNodeLabel(string nodeKey, uint focusRegionId)
    {
        if (string.IsNullOrWhiteSpace(nodeKey))
        {
            return "?";
        }

        if (nodeKey.StartsWith("neuron:", StringComparison.OrdinalIgnoreCase))
        {
            var suffix = nodeKey.Substring("neuron:".Length);
            if (uint.TryParse(suffix, NumberStyles.Integer, CultureInfo.InvariantCulture, out var address))
            {
                var regionId = RegionFromAddress(address);
                return regionId == focusRegionId
                    ? $"N{NeuronFromAddress(address)}"
                    : $"R{regionId}";
            }
        }

        if (nodeKey.StartsWith("gateway:", StringComparison.OrdinalIgnoreCase))
        {
            var suffix = nodeKey.Substring("gateway:".Length);
            if (uint.TryParse(suffix, NumberStyles.Integer, CultureInfo.InvariantCulture, out var regionId))
            {
                return $"R{regionId}";
            }
        }

        if (nodeKey.StartsWith("region:", StringComparison.OrdinalIgnoreCase))
        {
            var suffix = nodeKey.Substring("region:".Length);
            if (uint.TryParse(suffix, NumberStyles.Integer, CultureInfo.InvariantCulture, out var regionId))
            {
                return $"R{regionId}";
            }
        }

        return nodeKey;
    }

    private static string GetFocusedEdgeKind(uint sourceRegionId, uint targetRegionId, uint focusRegionId, bool hasReverse)
    {
        if (sourceRegionId == focusRegionId && targetRegionId == focusRegionId)
        {
            return "internal";
        }

        if (hasReverse)
        {
            return "bidirectional";
        }

        return sourceRegionId == focusRegionId ? "outbound" : "inbound";
    }

    private static string GetFocusedEdgeDirectionColor(string kind, bool isDormant)
    {
        var color = kind switch
        {
            "internal" => "#6C757D",
            "outbound" => "#7C8DA6",
            "inbound" => "#5F7896",
            "bidirectional" => "#B8A04D",
            _ => "#7A838A"
        };

        return isDormant ? DimColor(color) : color;
    }

    private static string GetRegionEdgeKind(VizActivityCanvasRegionRoute route, bool hasReverse)
    {
        if (route.SourceRegionId == route.TargetRegionId)
        {
            return "self";
        }

        if (hasReverse)
        {
            return "bidirectional";
        }

        return GetRegionSlice(route.SourceRegionId) <= GetRegionSlice(route.TargetRegionId)
            ? "feed-forward"
            : "feedback";
    }

    private static string GetRegionDirectionDashPattern(string edgeKind)
    {
        return edgeKind switch
        {
            "feed-forward" => string.Empty,
            "feedback" => "9 5",
            "bidirectional" => "2 4",
            "self" => "12 4 2 4",
            _ => "7 4"
        };
    }

    private static string GetFocusedDirectionDashPattern(string kind)
    {
        return kind switch
        {
            "internal" => string.Empty,
            "outbound" => "8 4",
            "inbound" => "2 4",
            "bidirectional" => "12 4 2 4",
            _ => "7 4"
        };
    }

    private static string GetRegionEdgeDirectionColor(string edgeKind, bool isDormant)
    {
        var color = edgeKind switch
        {
            "self" => "#6C757D",
            "bidirectional" => "#B8A04D",
            "feed-forward" => "#6B7F99",
            "feedback" => "#8A6F94",
            _ => "#7A838A"
        };

        return isDormant ? DimColor(color) : color;
    }

    private static string GetActivityEdgeColor(
        double signedSignal,
        double intensity,
        bool isDormant,
        VizActivityCanvasTransferCurve curve)
    {
        var neutral = isDormant ? "#4F565D" : "#56606A";
        var curvedSignal = ApplySignedTransferCurve(signedSignal, curve);
        var curvedIntensity = ApplyPositiveTransferCurve(intensity, curve);
        if (Math.Abs(curvedSignal) < 1e-5)
        {
            return BlendColor(neutral, "#8EA4B8", curvedIntensity * 0.45);
        }

        var target = curvedSignal > 0 ? "#E69F00" : "#0072B2";
        var blend = Math.Clamp((0.30 + (0.70 * curvedIntensity)) * Math.Abs(curvedSignal), 0.0, 1.0);
        return BlendColor(neutral, target, blend);
    }

    private static string ResolveRegionFillColor(
        VizActivityCanvasColorMode colorMode,
        uint regionId,
        float signedValue,
        float reserveValue,
        int reserveSampleCount,
        double loadRatio,
        double tickRecency,
        double structureRatio,
        bool isDormant,
        VizActivityCanvasTransferCurve curve)
        => colorMode switch
        {
            VizActivityCanvasColorMode.Topology => GetTopologyFillColor(regionId, isDormant),
            VizActivityCanvasColorMode.Activity => GetActivityFillColor(regionId, loadRatio, tickRecency, isDormant, curve),
            VizActivityCanvasColorMode.EnergyReserve => GetEnergyReserveFillColor(reserveValue, reserveSampleCount, isDormant, curve),
            VizActivityCanvasColorMode.EnergyCostPressure => GetEnergyCostPressureFillColor(loadRatio, tickRecency, structureRatio, reserveValue, reserveSampleCount, isDormant, curve),
            _ => GetStateFillColor(signedValue, isDormant, curve)
        };

    private static string ResolveFocusFillColor(
        VizActivityCanvasColorMode colorMode,
        uint regionId,
        float signedValue,
        float reserveValue,
        int reserveSampleCount,
        double activityRatio,
        double tickRecency,
        double structureRatio,
        bool isDormant,
        VizActivityCanvasTransferCurve curve)
        => colorMode switch
        {
            VizActivityCanvasColorMode.Topology => GetTopologyFillColor(regionId, isDormant),
            VizActivityCanvasColorMode.Activity => GetActivityFillColor(regionId, activityRatio, tickRecency, isDormant, curve),
            VizActivityCanvasColorMode.EnergyReserve => GetEnergyReserveFillColor(reserveValue, reserveSampleCount, isDormant, curve),
            VizActivityCanvasColorMode.EnergyCostPressure => GetEnergyCostPressureFillColor(activityRatio, tickRecency, structureRatio, reserveValue, reserveSampleCount, isDormant, curve),
            _ => GetStateFillColor(signedValue, isDormant, curve)
        };

    private static string GetTopologyFillColor(uint regionId, bool isDormant)
    {
        var color = GetSliceColor(GetRegionSlice(regionId));
        return isDormant ? DimColor(color) : color;
    }

    private static string GetTopologyStrokeColor(uint regionId, bool isDormant)
    {
        var color = DarkenColor(GetSliceColor(GetRegionSlice(regionId)));
        return isDormant ? DimColor(color) : color;
    }

    private static string GetActivityFillColor(
        uint regionId,
        double loadRatio,
        double recency,
        bool isDormant,
        VizActivityCanvasTransferCurve curve)
    {
        var baseColor = GetTopologyFillColor(regionId, isDormant);
        var neutral = isDormant ? "#4E5863" : "#5B6772";
        var curvedLoad = ApplyPositiveTransferCurve(loadRatio, curve);
        var intensity = Math.Clamp((0.65 * curvedLoad) + (0.35 * recency), 0.0, 1.0);
        return BlendColor(neutral, baseColor, 0.18 + (0.72 * intensity));
    }

    private static string GetStateFillColor(
        double signedValue,
        bool isDormant,
        VizActivityCanvasTransferCurve curve)
    {
        var clamped = ApplySignedTransferCurve(Clamp(signedValue, -1.0, 1.0), curve);
        var neutral = isDormant ? "#4E5863" : "#5E6873";
        var magnitude = Math.Abs(clamped);
        if (magnitude < 1e-5)
        {
            return neutral;
        }

        var target = clamped >= 0 ? "#E69F00" : "#0072B2";
        var minBlend = curve == VizActivityCanvasTransferCurve.PerceptualLog
            ? (isDormant ? 0.12 : 0.20)
            : (isDormant ? 0.22 : 0.38);
        var maxBlend = curve == VizActivityCanvasTransferCurve.PerceptualLog
            ? (isDormant ? 0.84 : 1.00)
            : (isDormant ? 0.60 : 0.88);
        return BlendColor(neutral, target, minBlend + ((maxBlend - minBlend) * magnitude));
    }

    private static string GetEnergyReserveFillColor(
        double reserveValue,
        int reserveSampleCount,
        bool isDormant,
        VizActivityCanvasTransferCurve curve)
    {
        var neutral = isDormant ? "#4E5863" : "#5E6873";
        var normalized = ApplySignedTransferCurve(NormalizeEnergySignal(reserveValue), curve);
        var magnitude = Math.Abs(normalized);
        if (magnitude < 1e-5)
        {
            return reserveSampleCount > 0
                ? BlendColor(neutral, "#8EA4B8", isDormant ? 0.16 : 0.22)
                : neutral;
        }

        var target = normalized >= 0 ? "#E69F00" : "#0072B2";
        var confidence = reserveSampleCount > 0 ? 1.0 : 0.65;
        var minBlend = isDormant ? 0.20 : 0.34;
        var maxBlend = isDormant ? 0.58 : 0.86;
        var blend = (minBlend + ((maxBlend - minBlend) * magnitude)) * confidence;
        return BlendColor(neutral, target, Clamp(blend, 0.0, 1.0));
    }

    private static string GetEnergyCostPressureFillColor(
        double activityRatio,
        double tickRecency,
        double structureRatio,
        double reserveValue,
        int reserveSampleCount,
        bool isDormant,
        VizActivityCanvasTransferCurve curve)
    {
        var activity = ApplyPositiveTransferCurve(Clamp(activityRatio, 0.0, 1.0), curve);
        var recency = Clamp(tickRecency, 0.0, 1.0);
        var structure = Clamp(structureRatio, 0.0, 1.0);
        var reserveMagnitude = Math.Abs(ApplySignedTransferCurve(NormalizeEnergySignal(reserveValue), curve));
        var reserveDeficit = reserveSampleCount > 0
            ? 1.0 - reserveMagnitude
            : 0.6;
        var dynamicLoad = (0.58 * activity) + (0.24 * recency) + (0.18 * structure);
        var pressure = Clamp((0.80 * dynamicLoad) + (0.20 * reserveDeficit), 0.0, 1.0);
        var cool = isDormant ? "#4F5E6B" : "#0072B2";
        var warm = isDormant ? "#8A6A3F" : "#D55E00";
        var minBlend = isDormant ? 0.18 : 0.24;
        return BlendColor(cool, warm, minBlend + ((1.0 - minBlend) * pressure));
    }

    private static double NormalizeEnergySignal(double value)
    {
        if (!double.IsFinite(value))
        {
            return 0.0;
        }

        var normalized = value / (1.0 + Math.Abs(value));
        return Clamp(normalized, -1.0, 1.0);
    }

    private static double ApplyPositiveTransferCurve(double value, VizActivityCanvasTransferCurve curve)
    {
        var clamped = Clamp(value, 0.0, 1.0);
        if (curve != VizActivityCanvasTransferCurve.PerceptualLog)
        {
            return clamped;
        }

        var denominator = Math.Log(1.0 + PerceptualLogGain);
        if (denominator <= 1e-8)
        {
            return clamped;
        }

        return Clamp(Math.Log(1.0 + (PerceptualLogGain * clamped)) / denominator, 0.0, 1.0);
    }

    private static double ApplySignedTransferCurve(double value, VizActivityCanvasTransferCurve curve)
    {
        var clamped = Clamp(value, -1.0, 1.0);
        if (curve != VizActivityCanvasTransferCurve.PerceptualLog)
        {
            return clamped;
        }

        var magnitude = ApplyPositiveTransferCurve(Math.Abs(clamped), curve);
        return clamped < 0.0 ? -magnitude : magnitude;
    }

    private static (string Fill, string Stroke) GetGatewayPalette(string role)
    {
        return role switch
        {
            "inbound" => ("#457B9D", "#345A73"),
            "outbound" => ("#F4A261", "#B8753D"),
            "bidirectional" => ("#E9C46A", "#B79543"),
            _ => ("#6C757D", "#4C5258")
        };
    }

    private static string GetSliceColor(int slice)
    {
        return slice switch
        {
            -3 => "#0072B2",
            -2 => "#56B4E9",
            -1 => "#009E73",
            0 => "#CC79A7",
            1 => "#F0E442",
            2 => "#E69F00",
            3 => "#D55E00",
            _ => "#7A838A"
        };
    }

    private static string DarkenColor(string hex)
    {
        if (!TryParseHexColor(hex, out var r, out var g, out var b))
        {
            return hex;
        }

        return ToHex(
            (byte)(r * 0.72),
            (byte)(g * 0.72),
            (byte)(b * 0.72));
    }

    private static string DimColor(string hex)
    {
        if (!TryParseHexColor(hex, out var r, out var g, out var b))
        {
            return hex;
        }

        return ToHex(
            (byte)(r * 0.45),
            (byte)(g * 0.45),
            (byte)(b * 0.45));
    }

    private static bool TryParseHexColor(string hex, out byte r, out byte g, out byte b)
    {
        r = 0;
        g = 0;
        b = 0;
        if (string.IsNullOrWhiteSpace(hex) || hex.Length != 7 || hex[0] != '#')
        {
            return false;
        }

        return byte.TryParse(hex[1..3], NumberStyles.HexNumber, CultureInfo.InvariantCulture, out r)
               && byte.TryParse(hex[3..5], NumberStyles.HexNumber, CultureInfo.InvariantCulture, out g)
               && byte.TryParse(hex[5..7], NumberStyles.HexNumber, CultureInfo.InvariantCulture, out b);
    }

    private static string ToHex(byte r, byte g, byte b)
        => $"#{r:X2}{g:X2}{b:X2}";

    private static string BlendColor(string fromHex, string toHex, double toWeight)
    {
        if (!TryParseHexColor(fromHex, out var fromR, out var fromG, out var fromB))
        {
            return toHex;
        }

        if (!TryParseHexColor(toHex, out var toR, out var toG, out var toB))
        {
            return fromHex;
        }

        var weight = Math.Clamp(toWeight, 0.0, 1.0);
        var inverse = 1.0 - weight;
        return ToHex(
            (byte)Math.Clamp(Math.Round((fromR * inverse) + (toR * weight)), 0, 255),
            (byte)Math.Clamp(Math.Round((fromG * inverse) + (toG * weight)), 0, 255),
            (byte)Math.Clamp(Math.Round((fromB * inverse) + (toB * weight)), 0, 255));
    }

    private static string FormatBufferAverage(RegionBufferMetric metrics)
        => FormatBufferAverage(metrics.BufferCount, metrics.AverageBufferValue);

    private static string FormatBufferLatest(RegionBufferMetric metrics)
        => FormatBufferLatest(metrics.BufferCount, metrics.LatestBufferValue, metrics.LatestBufferTick);

    private static string FormatBufferAverage(int bufferCount, float averageBufferValue)
        => bufferCount > 0
            ? averageBufferValue.ToString("0.###", CultureInfo.InvariantCulture)
            : "n/a";

    private static string FormatBufferLatest(int bufferCount, float latestBufferValue, ulong latestBufferTick)
        => bufferCount > 0
            ? FormattableString.Invariant($"{latestBufferValue:0.###}@{latestBufferTick}")
            : "n/a";

    private static string NodeKeyForRegion(uint regionId)
        => $"region:{regionId}";

    private static string NodeKeyForNeuron(uint address)
        => $"neuron:{address}";

    private static string NodeKeyForGateway(uint regionId)
        => $"gateway:{regionId}";

    private static bool TryParseAddress(string? value, out uint address)
    {
        address = 0;
        if (!uint.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            if (!TryParseRegionToken(value, out var parsedRegionId, out var remainder)
                || string.IsNullOrWhiteSpace(remainder)
                || (remainder[0] != 'N' && remainder[0] != 'n'))
            {
                return false;
            }

            var neuronText = remainder[1..];
            if (!uint.TryParse(neuronText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var neuronId))
            {
                return false;
            }

            address = ComposeAddress(parsedRegionId, neuronId);
            return true;
        }

        var regionId = parsed >> NbnConstants.AddressNeuronBits;
        if (regionId > NbnConstants.RegionMaxId)
        {
            return false;
        }

        address = parsed;
        return true;
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

    private static uint ComposeAddress(uint regionId, uint neuronId)
        => (regionId << NbnConstants.AddressNeuronBits) | (neuronId & NbnConstants.AddressNeuronMask);

    private static uint RegionFromAddress(uint address)
        => address >> NbnConstants.AddressNeuronBits;

    private static uint NeuronFromAddress(uint address)
        => address & NbnConstants.AddressNeuronMask;

    private static bool IsAxonType(string? type)
        => !string.IsNullOrWhiteSpace(type) && type.Contains("AXON", StringComparison.OrdinalIgnoreCase);

    private static bool IsFiredType(string? type)
        => !string.IsNullOrWhiteSpace(type) && type.Contains("FIRED", StringComparison.OrdinalIgnoreCase);

    private static bool IsBufferType(string? type)
        => !string.IsNullOrWhiteSpace(type) && type.Contains("BUFFER", StringComparison.OrdinalIgnoreCase);

    private static bool IsValueMetricType(string? type)
        => IsAxonType(type) || IsFiredType(type) || IsBufferType(type);

    private static bool TouchesFocusRegion(uint sourceAddress, uint targetAddress, uint focusRegionId)
    {
        var sourceRegion = RegionFromAddress(sourceAddress);
        var targetRegion = RegionFromAddress(targetAddress);
        return sourceRegion == focusRegionId || targetRegion == focusRegionId;
    }

    private static void MergeRegionNode(
        IDictionary<uint, RegionNodeSource> byRegion,
        uint? regionId,
        int eventCount,
        ulong lastTick)
    {
        if (!regionId.HasValue)
        {
            return;
        }

        MergeRegionNode(byRegion, regionId.Value, eventCount, lastTick);
    }

    private static void MergeRegionNode(
        IDictionary<uint, RegionNodeSource> byRegion,
        uint regionId,
        int eventCount,
        ulong lastTick)
    {
        if (byRegion.TryGetValue(regionId, out var existing))
        {
            byRegion[regionId] = new RegionNodeSource(
                existing.RegionId,
                existing.EventCount + Math.Max(0, eventCount),
                Math.Max(existing.LastTick, lastTick),
                existing.FiredCount,
                existing.AxonCount,
                existing.DominantType,
                existing.AverageMagnitude,
                existing.SignedValue);
            return;
        }

        byRegion[regionId] = new RegionNodeSource(regionId, Math.Max(0, eventCount), lastTick, 0, Math.Max(0, eventCount), "unknown", 0f, 0f);
    }

    private static double TickRecency(ulong itemTick, ulong latestTick, int tickWindow)
    {
        if (latestTick == 0 || itemTick >= latestTick)
        {
            return 1.0;
        }

        var safeWindow = Math.Max(1, tickWindow);
        var delta = latestTick - itemTick;
        var normalized = 1.0 - Math.Clamp((double)delta / safeWindow, 0.0, 1.0);
        return Math.Clamp(normalized, 0.0, 1.0);
    }

    private static int GetRegionSlice(uint regionId)
    {
        if (regionId == 0)
        {
            return -3;
        }

        if (regionId <= 3)
        {
            return -2;
        }

        if (regionId <= 8)
        {
            return -1;
        }

        if (regionId <= 22)
        {
            return 0;
        }

        if (regionId <= 27)
        {
            return 1;
        }

        if (regionId <= 30)
        {
            return 2;
        }

        return 3;
    }

    private static double Clamp(double value, double min, double max)
        => Math.Max(min, Math.Min(max, value));
}
