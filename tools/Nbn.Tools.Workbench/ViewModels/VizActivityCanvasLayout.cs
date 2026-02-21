using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed record VizActivityCanvasLayout(
    double Width,
    double Height,
    string Legend,
    IReadOnlyList<VizActivityCanvasNode> Nodes,
    IReadOnlyList<VizActivityCanvasEdge> Edges);

public sealed record VizActivityCanvasInteractionState(
    uint? SelectedRegionId,
    string? SelectedRouteLabel,
    uint? HoverRegionId,
    string? HoverRouteLabel,
    IReadOnlySet<uint> PinnedRegionIds,
    IReadOnlySet<string> PinnedRouteLabels)
{
    public static VizActivityCanvasInteractionState Empty { get; } = new(
        null,
        null,
        null,
        null,
        new HashSet<uint>(),
        new HashSet<string>(StringComparer.OrdinalIgnoreCase));

    public bool IsSelectedRegion(uint regionId) => SelectedRegionId.HasValue && SelectedRegionId.Value == regionId;

    public bool IsHoveredRegion(uint regionId) => HoverRegionId.HasValue && HoverRegionId.Value == regionId;

    public bool IsSelectedRoute(string? routeLabel) => RouteEquals(SelectedRouteLabel, routeLabel);

    public bool IsHoveredRoute(string? routeLabel) => RouteEquals(HoverRouteLabel, routeLabel);

    public bool IsRegionPinned(uint regionId) => PinnedRegionIds.Contains(regionId);

    public bool IsRoutePinned(string? routeLabel)
    {
        if (string.IsNullOrWhiteSpace(routeLabel))
        {
            return false;
        }

        foreach (var pinned in PinnedRouteLabels)
        {
            if (RouteEquals(pinned, routeLabel))
            {
                return true;
            }
        }

        return false;
    }

    private static bool RouteEquals(string? left, string? right)
        => string.Equals(left, right, StringComparison.OrdinalIgnoreCase);
}

public sealed record VizActivityCanvasNode(
    uint RegionId,
    string Label,
    string Detail,
    double Left,
    double Top,
    double Diameter,
    double FillOpacity,
    double PulseOpacity,
    double StrokeThickness,
    bool IsFocused,
    ulong LastTick,
    int EventCount,
    bool IsSelected,
    bool IsHovered,
    bool IsPinned);

public sealed record VizActivityCanvasEdge(
    string RouteLabel,
    string Detail,
    string PathData,
    double StrokeThickness,
    double Opacity,
    bool IsFocused,
    ulong LastTick,
    int EventCount,
    uint? SourceRegionId,
    uint? TargetRegionId,
    bool IsSelected,
    bool IsHovered,
    bool IsPinned);

public static class VizActivityCanvasLayoutBuilder
{
    public const double CanvasWidth = 860;
    public const double CanvasHeight = 420;
    private const double CenterX = CanvasWidth / 2.0;
    private const double CenterY = CanvasHeight / 2.0;
    private const double CanvasPadding = 26;
    private const double MinNodeRadius = 14;
    private const double MaxNodeRadius = 30;
    private const double BaseEdgeStroke = 1.2;
    private const double MaxEdgeStrokeBoost = 2.8;

    public static VizActivityCanvasLayout Build(
        VizActivityProjection projection,
        VizActivityProjectionOptions options,
        VizActivityCanvasInteractionState? interaction = null)
    {
        interaction ??= VizActivityCanvasInteractionState.Empty;

        var latestTick = projection.Ticks.Count > 0
            ? projection.Ticks.Max(item => item.TickId)
            : projection.Regions.Count > 0
                ? projection.Regions.Max(item => item.LastTick)
                : 0UL;

        var nodeSource = BuildNodeSources(projection);
        if (nodeSource.Count == 0)
        {
            return new VizActivityCanvasLayout(
                CanvasWidth,
                CanvasHeight,
                "Canvas renderer awaiting region activity.",
                Array.Empty<VizActivityCanvasNode>(),
                Array.Empty<VizActivityCanvasEdge>());
        }

        var positions = BuildRegionPositions(nodeSource.Keys);
        ApplyFocusAnchoring(options.FocusRegionId, positions);

        var maxNodeEvents = Math.Max(1, nodeSource.Values.Max(item => item.EventCount));
        var nodeByRegion = new Dictionary<uint, VizActivityCanvasNode>();
        var nodes = new List<VizActivityCanvasNode>(nodeSource.Count);

        foreach (var (regionId, stats) in nodeSource.OrderBy(item => item.Key))
        {
            var position = positions[regionId];
            var loadRatio = Math.Clamp((double)stats.EventCount / maxNodeEvents, 0.0, 1.0);
            var radius = MinNodeRadius + ((MaxNodeRadius - MinNodeRadius) * loadRatio);
            var tickRecency = TickRecency(stats.LastTick, latestTick, options.TickWindow);
            var isFocused = options.FocusRegionId.HasValue && options.FocusRegionId.Value == regionId;
            var isSelected = interaction.IsSelectedRegion(regionId);
            var isHovered = interaction.IsHoveredRegion(regionId);
            var isPinned = interaction.IsRegionPinned(regionId);
            var emphasis = (isSelected ? 0.25 : 0.0) + (isHovered ? 0.14 : 0.0) + (isPinned ? 0.18 : 0.0);
            var fillOpacity = Math.Clamp(0.35 + (0.5 * loadRatio) + emphasis, 0.25, 1.0);
            var pulseOpacity = Math.Clamp(0.2 + (0.75 * tickRecency) + (emphasis * 0.7), 0.2, 1.0);
            var strokeThickness = 1.4
                                  + (isFocused ? 1.6 : 0.0)
                                  + (isPinned ? 0.9 : 0.0)
                                  + (isHovered ? 0.6 : 0.0)
                                  + (isSelected ? 1.1 : 0.0);
            var diameter = radius * 2.0;
            var detail = $"R{regionId} | events {stats.EventCount} | last tick {stats.LastTick} | fired {stats.FiredCount} | axon {stats.AxonCount}";
            if (isSelected || isHovered || isPinned)
            {
                detail = $"{detail} | selected {isSelected} | hover {isHovered} | pinned {isPinned}";
            }

            var node = new VizActivityCanvasNode(
                regionId,
                $"R{regionId}",
                detail,
                position.X - radius,
                position.Y - radius,
                diameter,
                fillOpacity,
                pulseOpacity,
                strokeThickness,
                isFocused,
                stats.LastTick,
                stats.EventCount,
                isSelected,
                isHovered,
                isPinned);

            nodes.Add(node);
            nodeByRegion[regionId] = node;
        }

        var edges = BuildEdges(projection, options, nodeByRegion, latestTick, interaction);
        var legend = $"Regions {nodes.Count} | Routes {edges.Count} | Latest tick {latestTick}";

        return new VizActivityCanvasLayout(CanvasWidth, CanvasHeight, legend, nodes, edges);
    }

    private static Dictionary<uint, RegionNodeSource> BuildNodeSources(VizActivityProjection projection)
    {
        var byRegion = projection.Regions.ToDictionary(
            item => item.RegionId,
            item => new RegionNodeSource(item.RegionId, item.EventCount, item.LastTick, item.FiredCount, item.AxonCount));

        foreach (var edge in projection.Edges)
        {
            MergeEdgeEndpoint(byRegion, edge.SourceRegionId, edge.EventCount, edge.LastTick);
            MergeEdgeEndpoint(byRegion, edge.TargetRegionId, edge.EventCount, edge.LastTick);
        }

        return byRegion;
    }

    private static void MergeEdgeEndpoint(
        Dictionary<uint, RegionNodeSource> byRegion,
        uint? regionId,
        int edgeEventCount,
        ulong lastTick)
    {
        if (!regionId.HasValue)
        {
            return;
        }

        if (byRegion.TryGetValue(regionId.Value, out var existing))
        {
            var merged = new RegionNodeSource(
                existing.RegionId,
                existing.EventCount + edgeEventCount,
                Math.Max(existing.LastTick, lastTick),
                existing.FiredCount,
                existing.AxonCount);
            byRegion[regionId.Value] = merged;
            return;
        }

        byRegion[regionId.Value] = new RegionNodeSource(regionId.Value, edgeEventCount, lastTick, 0, edgeEventCount);
    }

    private static Dictionary<uint, CanvasPoint> BuildRegionPositions(IEnumerable<uint> regionIds)
    {
        var groupsBySlice = regionIds
            .GroupBy(GetRegionSlice)
            .OrderBy(group => group.Key)
            .ToDictionary(group => group.Key, group => group.OrderBy(region => region).ToList());

        const int minSlice = -3;
        const int maxSlice = 3;
        var xStep = (CanvasWidth - (CanvasPadding * 2.0)) / (maxSlice - minSlice);
        var positions = new Dictionary<uint, CanvasPoint>();

        foreach (var (slice, regions) in groupsBySlice)
        {
            var x = CanvasPadding + ((slice - minSlice) * xStep);
            var count = regions.Count;
            var yStep = count <= 1
                ? 0.0
                : (CanvasHeight - (CanvasPadding * 2.0)) / (count - 1);

            for (var index = 0; index < count; index++)
            {
                var y = count == 1
                    ? CenterY
                    : CanvasPadding + (index * yStep);
                positions[regions[index]] = new CanvasPoint(x, y);
            }
        }

        return positions;
    }

    private static void ApplyFocusAnchoring(uint? focusRegionId, Dictionary<uint, CanvasPoint> positions)
    {
        if (!focusRegionId.HasValue || !positions.TryGetValue(focusRegionId.Value, out var focusPoint))
        {
            return;
        }

        var deltaX = CenterX - focusPoint.X;
        var deltaY = CenterY - focusPoint.Y;
        var keys = positions.Keys.ToList();
        foreach (var key in keys)
        {
            var current = positions[key];
            var shifted = new CanvasPoint(
                Clamp(current.X + deltaX, CanvasPadding, CanvasWidth - CanvasPadding),
                Clamp(current.Y + deltaY, CanvasPadding, CanvasHeight - CanvasPadding));
            positions[key] = shifted;
        }
    }

    private static IReadOnlyList<VizActivityCanvasEdge> BuildEdges(
        VizActivityProjection projection,
        VizActivityProjectionOptions options,
        IReadOnlyDictionary<uint, VizActivityCanvasNode> nodeByRegion,
        ulong latestTick,
        VizActivityCanvasInteractionState interaction)
    {
        if (projection.Edges.Count == 0)
        {
            return Array.Empty<VizActivityCanvasEdge>();
        }

        var maxEdgeEvents = Math.Max(1, projection.Edges.Max(item => item.EventCount));
        var edges = new List<VizActivityCanvasEdge>(projection.Edges.Count);

        foreach (var edge in projection.Edges)
        {
            if (!edge.SourceRegionId.HasValue
                || !edge.TargetRegionId.HasValue
                || !nodeByRegion.TryGetValue(edge.SourceRegionId.Value, out var sourceNode)
                || !nodeByRegion.TryGetValue(edge.TargetRegionId.Value, out var targetNode))
            {
                continue;
            }

            var sourceCenter = new CanvasPoint(sourceNode.Left + (sourceNode.Diameter / 2.0), sourceNode.Top + (sourceNode.Diameter / 2.0));
            var targetCenter = new CanvasPoint(targetNode.Left + (targetNode.Diameter / 2.0), targetNode.Top + (targetNode.Diameter / 2.0));
            var curve = BuildEdgeCurve(sourceCenter, targetCenter, sourceNode.RegionId == targetNode.RegionId);
            var normalizedLoad = Math.Clamp((double)edge.EventCount / maxEdgeEvents, 0.0, 1.0);
            var recency = TickRecency(edge.LastTick, latestTick, options.TickWindow);
            var isFocused = options.FocusRegionId.HasValue
                            && (edge.SourceRegionId.Value == options.FocusRegionId.Value
                                || edge.TargetRegionId.Value == options.FocusRegionId.Value);
            var isSelected = interaction.IsSelectedRoute(edge.RouteLabel);
            var isHovered = interaction.IsHoveredRoute(edge.RouteLabel);
            var isPinned = interaction.IsRoutePinned(edge.RouteLabel);
            var emphasis = (isSelected ? 0.22 : 0.0) + (isHovered ? 0.12 : 0.0) + (isPinned ? 0.16 : 0.0);
            var thickness = BaseEdgeStroke
                            + (normalizedLoad * MaxEdgeStrokeBoost)
                            + (isFocused ? 0.8 : 0.0)
                            + (isPinned ? 0.8 : 0.0)
                            + (isHovered ? 0.5 : 0.0)
                            + (isSelected ? 0.9 : 0.0);
            var opacity = Math.Clamp(0.2 + (0.55 * recency) + (isFocused ? 0.15 : 0.0) + emphasis, 0.2, 1.0);
            var detail = $"{edge.RouteLabel} | events {edge.EventCount} | last tick {edge.LastTick} | avg |v| {edge.AverageMagnitude:0.###}";
            if (isSelected || isHovered || isPinned)
            {
                detail = $"{detail} | selected {isSelected} | hover {isHovered} | pinned {isPinned}";
            }

            edges.Add(new VizActivityCanvasEdge(
                edge.RouteLabel,
                detail,
                curve,
                thickness,
                opacity,
                isFocused,
                edge.LastTick,
                edge.EventCount,
                edge.SourceRegionId,
                edge.TargetRegionId,
                isSelected,
                isHovered,
                isPinned));
        }

        return edges
            .OrderByDescending(item => item.IsSelected)
            .ThenByDescending(item => item.IsHovered)
            .ThenByDescending(item => item.IsPinned)
            .ThenByDescending(item => item.IsFocused)
            .ThenByDescending(item => item.LastTick)
            .ThenByDescending(item => item.EventCount)
            .ToList();
    }

    private static string BuildEdgeCurve(CanvasPoint source, CanvasPoint target, bool isSelfLoop)
    {
        if (isSelfLoop)
        {
            var loopSize = 22;
            var c1 = new CanvasPoint(source.X + loopSize, source.Y - (loopSize * 1.3));
            var end = new CanvasPoint(source.X - 1, source.Y - 1);
            return FormattableString.Invariant(
                $"M {source.X:0.###} {source.Y:0.###} Q {c1.X:0.###} {c1.Y:0.###} {end.X:0.###} {end.Y:0.###}");
        }

        var midX = (source.X + target.X) / 2.0;
        var midY = (source.Y + target.Y) / 2.0;
        var deltaX = target.X - source.X;
        var deltaY = target.Y - source.Y;
        var length = Math.Max(1.0, Math.Sqrt((deltaX * deltaX) + (deltaY * deltaY)));
        var normalX = -deltaY / length;
        var normalY = deltaX / length;
        var curvature = Math.Min(42.0, 14.0 + (length * 0.12));
        var control = new CanvasPoint(midX + (normalX * curvature), midY + (normalY * curvature));

        return FormattableString.Invariant(
            $"M {source.X:0.###} {source.Y:0.###} Q {control.X:0.###} {control.Y:0.###} {target.X:0.###} {target.Y:0.###}");
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

    private readonly record struct CanvasPoint(double X, double Y);

    private sealed record RegionNodeSource(uint RegionId, int EventCount, ulong LastTick, int FiredCount, int AxonCount);
}
