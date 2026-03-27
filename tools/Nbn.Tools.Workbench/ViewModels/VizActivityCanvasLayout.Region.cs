using System;
using System.Collections.Generic;
using System.Linq;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;

namespace Nbn.Tools.Workbench.ViewModels;

public static partial class VizActivityCanvasLayoutBuilder
{
    private static VizActivityCanvasLayout BuildRegionCanvas(
        VizActivityProjection projection,
        VizActivityProjectionOptions options,
        VizActivityCanvasTopology topology,
        VizActivityCanvasInteractionState interaction,
        ulong latestTick,
        VizActivityCanvasColorMode colorMode,
        VizActivityCanvasRenderOptions renderOptions)
    {
        var nodeSource = BuildRegionNodeSources(projection, topology);
        if (nodeSource.Count == 0)
        {
            return new VizActivityCanvasLayout(
                CanvasWidth,
                CanvasHeight,
                "Canvas renderer awaiting region activity.",
                Array.Empty<VizActivityCanvasNode>(),
                Array.Empty<VizActivityCanvasEdge>());
        }

        var positions = BuildRegionPositions(nodeSource.Keys, renderOptions.LayoutMode, out var used3DProjection, out var fellBackTo2D);
        var regionBufferMetrics = BuildRegionBufferMetrics(projection.WindowEvents);
        var regionRouteDegrees = BuildRegionRouteDegrees(projection, topology);
        var maxNodeEvents = Math.Max(1, nodeSource.Values.Max(item => item.EventCount));
        var maxRouteDegree = Math.Max(
            1,
            nodeSource.Keys.Max(regionId =>
            {
                regionRouteDegrees.TryGetValue(regionId, out var degree);
                return degree.OutboundCount + degree.InboundCount;
            }));
        var nodes = new List<VizActivityCanvasNode>(nodeSource.Count);
        var nodeByRegion = new Dictionary<uint, VizActivityCanvasNode>();

        foreach (var (regionId, stats) in nodeSource.OrderBy(item => item.Key))
        {
            var position = positions[regionId];
            var nodeKey = NodeKeyForRegion(regionId);
            var isSelected = interaction.IsSelectedNode(nodeKey);
            var isHovered = interaction.IsHoveredNode(nodeKey);
            var isPinned = interaction.IsNodePinned(nodeKey);
            var isDormant = stats.EventCount <= 0 || stats.LastTick == 0;
            var loadRatio = isDormant ? 0.0 : Math.Clamp((double)stats.EventCount / maxNodeEvents, 0.0, 1.0);
            var tickRecency = isDormant ? 0.0 : TickRecency(stats.LastTick, latestTick, options.TickWindow);
            regionBufferMetrics.TryGetValue(regionId, out var bufferMetrics);
            regionRouteDegrees.TryGetValue(regionId, out var routeDegree);
            var structureRatio = Math.Clamp((double)(routeDegree.OutboundCount + routeDegree.InboundCount) / maxRouteDegree, 0.0, 1.0);
            var reserveValue = bufferMetrics.BufferCount > 0 ? bufferMetrics.LatestBufferValue : stats.SignedValue;
            var stateSignal = bufferMetrics.BufferCount > 0 ? reserveValue : stats.SignedValue;
            var emphasis = (isSelected ? 0.25 : 0.0) + (isHovered ? 0.14 : 0.0) + (isPinned ? 0.18 : 0.0);
            var stroke = GetTopologyStrokeColor(regionId, isDormant);
            var fill = ResolveRegionFillColor(
                colorMode,
                regionId,
                stateSignal,
                reserveValue,
                bufferMetrics.BufferCount,
                loadRatio,
                tickRecency,
                structureRatio,
                isDormant,
                renderOptions.ColorCurve);
            var fillOpacity = Math.Clamp((isDormant ? 0.14 : 0.36 + (0.45 * loadRatio)) + emphasis, 0.12, 1.0);
            var pulseOpacity = Math.Clamp((isDormant ? 0.1 : 0.25 + (0.45 * tickRecency)) + (emphasis * 0.6), 0.08, 1.0);
            var strokeThickness = 1.3
                                  + (isPinned ? 0.8 : 0.0)
                                  + (isHovered ? 0.6 : 0.0)
                                  + (isSelected ? 0.9 : 0.0);
            var radius = MinRegionNodeRadius
                         + ((MaxRegionNodeRadius - MinRegionNodeRadius) * ((structureRatio * 0.78) + (loadRatio * 0.22)));
            var bufferAverageText = FormatBufferAverage(bufferMetrics);
            var bufferLatestText = FormatBufferLatest(bufferMetrics);
            var detail = $"R{regionId} | events {stats.EventCount} | last tick {stats.LastTick}"
                         + $" | fired {stats.FiredCount} | axon {stats.AxonCount}"
                         + $" | dom {stats.DominantType} | avg |v| {stats.AverageMagnitude:0.###} | avg v {stats.SignedValue:0.###}"
                         + $" | routes out {routeDegree.OutboundCount} in {routeDegree.InboundCount}"
                         + $" | value n={bufferMetrics.BufferCount} avg={bufferAverageText} latest={bufferLatestText}";
            if (isDormant)
            {
                detail = $"{detail} | inactive in window";
            }

            var node = new VizActivityCanvasNode(
                nodeKey,
                regionId,
                null,
                regionId,
                $"R{regionId}",
                detail,
                position.X - radius,
                position.Y - radius,
                radius * 2.0,
                fill,
                stroke,
                fillOpacity,
                pulseOpacity,
                strokeThickness,
                false,
                stats.LastTick,
                stats.EventCount,
                isSelected,
                isHovered,
                isPinned);

            nodes.Add(node);
            nodeByRegion[regionId] = node;
        }

        var edges = BuildRegionEdges(projection, options, topology, nodeByRegion, interaction, latestTick, renderOptions);
        var layoutLegend = used3DProjection
            ? "Layout 3D-projected"
            : fellBackTo2D
                ? "Layout 3D->2D fallback"
                : "Layout 2D-axial";
        var legend = $"Region map | Regions {nodes.Count} | Routes {edges.Count} | Latest tick {latestTick} | {layoutLegend}";
        return new VizActivityCanvasLayout(CanvasWidth, CanvasHeight, legend, nodes, edges);
    }

    private static Dictionary<uint, RegionNodeSource> BuildRegionNodeSources(
        VizActivityProjection projection,
        VizActivityCanvasTopology topology)
    {
        var byRegion = projection.Regions.ToDictionary(
            item => item.RegionId,
            item => new RegionNodeSource(
                item.RegionId,
                item.EventCount,
                item.LastTick,
                item.FiredCount,
                item.AxonCount,
                item.DominantType,
                item.AverageMagnitude,
                item.AverageSignedValue));

        foreach (var edge in projection.Edges)
        {
            MergeRegionNode(byRegion, edge.SourceRegionId, edge.EventCount, edge.LastTick);
            MergeRegionNode(byRegion, edge.TargetRegionId, edge.EventCount, edge.LastTick);
        }

        foreach (var regionId in topology.Regions)
        {
            MergeRegionNode(byRegion, regionId, 0, 0);
        }

        MergeRegionNode(byRegion, (uint)NbnConstants.InputRegionId, 0, 0);
        MergeRegionNode(byRegion, (uint)NbnConstants.OutputRegionId, 0, 0);
        return byRegion;
    }

    private static Dictionary<uint, RegionBufferMetric> BuildRegionBufferMetrics(IReadOnlyList<VizEventItem> events)
    {
        var byRegion = new Dictionary<uint, RegionBufferMetric>();
        foreach (var item in events)
        {
            if (!IsValueMetricType(item.Type))
            {
                continue;
            }

            uint regionId;
            if (TryParseRegion(item.Region, out var parsedRegion))
            {
                regionId = parsedRegion;
            }
            else if (TryParseAddress(item.Source, out var sourceAddress))
            {
                regionId = RegionFromAddress(sourceAddress);
            }
            else
            {
                continue;
            }

            byRegion.TryGetValue(regionId, out var existing);
            byRegion[regionId] = existing.Merge(item.TickId, item.Value);
        }

        return byRegion;
    }

    private static Dictionary<uint, RegionRouteDegree> BuildRegionRouteDegrees(
        VizActivityProjection projection,
        VizActivityCanvasTopology topology)
    {
        var byRegion = new Dictionary<uint, RegionRouteDegree>();

        static void AddRoute(IDictionary<uint, RegionRouteDegree> target, uint sourceRegionId, uint targetRegionId)
        {
            target.TryGetValue(sourceRegionId, out var sourceDegree);
            target[sourceRegionId] = sourceDegree.IncrementOutbound();

            target.TryGetValue(targetRegionId, out var targetDegree);
            target[targetRegionId] = targetDegree.IncrementInbound();
        }

        foreach (var edge in projection.Edges)
        {
            if (!edge.SourceRegionId.HasValue || !edge.TargetRegionId.HasValue)
            {
                continue;
            }

            AddRoute(byRegion, edge.SourceRegionId.Value, edge.TargetRegionId.Value);
        }

        foreach (var route in topology.RegionRoutes)
        {
            AddRoute(byRegion, route.SourceRegionId, route.TargetRegionId);
        }

        return byRegion;
    }

    private static IReadOnlyList<VizActivityCanvasEdge> BuildRegionEdges(
        VizActivityProjection projection,
        VizActivityProjectionOptions options,
        VizActivityCanvasTopology topology,
        IReadOnlyDictionary<uint, VizActivityCanvasNode> nodeByRegion,
        VizActivityCanvasInteractionState interaction,
        ulong latestTick,
        VizActivityCanvasRenderOptions renderOptions)
    {
        var aggregates = new Dictionary<VizActivityCanvasRegionRoute, RouteAggregate>();
        foreach (var edge in projection.Edges)
        {
            if (!edge.SourceRegionId.HasValue || !edge.TargetRegionId.HasValue)
            {
                continue;
            }

            var key = new VizActivityCanvasRegionRoute(edge.SourceRegionId.Value, edge.TargetRegionId.Value);
            if (!aggregates.TryGetValue(key, out var existing))
            {
                existing = RouteAggregate.Empty;
            }

            aggregates[key] = existing.Merge(
                edge.EventCount,
                edge.LastTick,
                edge.AverageMagnitude,
                edge.AverageStrength,
                edge.AverageSignedValue,
                edge.AverageSignedStrength,
                routeCountIncrement: 1);
        }

        foreach (var route in topology.RegionRoutes)
        {
            if (!aggregates.ContainsKey(route))
            {
                aggregates[route] = RouteAggregate.Empty;
            }
        }

        if (aggregates.Count == 0)
        {
            return Array.Empty<VizActivityCanvasEdge>();
        }

        var maxEdgeEvents = Math.Max(1, aggregates.Values.Max(item => item.EventCount));
        var routeSet = new HashSet<VizActivityCanvasRegionRoute>(aggregates.Keys);
        var edges = new List<VizActivityCanvasEdge>(aggregates.Count);

        foreach (var (route, aggregate) in aggregates)
        {
            if (!nodeByRegion.TryGetValue(route.SourceRegionId, out var sourceNode)
                || !nodeByRegion.TryGetValue(route.TargetRegionId, out var targetNode))
            {
                continue;
            }

            var reverse = new VizActivityCanvasRegionRoute(route.TargetRegionId, route.SourceRegionId);
            var hasReverse = routeSet.Contains(reverse) && reverse != route;
            var edgeKind = GetRegionEdgeKind(route, hasReverse);
            var routeLabel = $"R{route.SourceRegionId} -> R{route.TargetRegionId}";
            var isSelected = interaction.IsSelectedRoute(routeLabel);
            var isHovered = interaction.IsHoveredRoute(routeLabel);
            var isPinned = interaction.IsRoutePinned(routeLabel);
            var directionDashArray = GetRegionDirectionDashPattern(edgeKind);
            var isDormant = aggregate.EventCount <= 0 || aggregate.LastTick == 0;
            var recency = isDormant ? 0.0 : TickRecency(aggregate.LastTick, latestTick, options.TickWindow);
            var normalizedLoad = isDormant ? 0.0 : Math.Clamp((double)aggregate.EventCount / maxEdgeEvents, 0.0, 1.0);
            var normalizedStrength = Math.Clamp((double)Math.Abs(aggregate.AverageStrength), 0.0, 1.0);
            var signedSignal = Clamp(aggregate.SignedValue * 0.75f + aggregate.SignedStrength * 0.25f, -1.0, 1.0);
            var emphasis = (isSelected ? 0.22 : 0.0) + (isHovered ? 0.12 : 0.0) + (isPinned ? 0.16 : 0.0);
            var thickness = BaseEdgeStroke
                            + (normalizedLoad * MaxEdgeStrokeBoost)
                            + (normalizedStrength * 0.7)
                            + 1.1
                            + (isPinned ? 0.8 : 0.0)
                            + (isHovered ? 0.5 : 0.0)
                            + (isSelected ? 0.9 : 0.0);
            var opacity = Math.Clamp((isDormant ? 0.27 : 0.32 + (0.45 * recency)) + emphasis, 0.22, 1.0);
            var activityIntensity = isDormant
                ? 0.0
                : Math.Clamp((0.45 * normalizedLoad) + (0.25 * recency) + (0.30 * Math.Abs(signedSignal)), 0.0, 1.0);
            var activityOpacity = Math.Clamp((isDormant ? 0.20 : 0.26 + (0.62 * activityIntensity)) + (emphasis * 0.5), 0.18, 1.0);
            var activityThickness = Math.Max(0.7, thickness - 2.2);
            var hitTestThickness = Math.Max(9.0, thickness + 7.0);
            var sourceCenter = new CanvasPoint(sourceNode.Left + (sourceNode.Diameter / 2.0), sourceNode.Top + (sourceNode.Diameter / 2.0));
            var targetCenter = new CanvasPoint(targetNode.Left + (targetNode.Diameter / 2.0), targetNode.Top + (targetNode.Diameter / 2.0));
            var sourceRadius = sourceNode.Diameter / 2.0;
            var targetRadius = targetNode.Diameter / 2.0;
            var curveDirection = hasReverse && route.SourceRegionId > route.TargetRegionId ? -1 : 1;
            var curve = BuildEdgeCurve(sourceCenter, targetCenter, route.SourceRegionId == route.TargetRegionId, curveDirection, sourceRadius, targetRadius);
            var detail = $"{routeLabel} | {edgeKind} | routes {aggregate.RouteCount} | events {aggregate.EventCount} | last tick {aggregate.LastTick}"
                         + $" | avg |v| {aggregate.AverageMagnitude:0.###} | avg v {aggregate.SignedValue:0.###}"
                         + $" | avg |s| {aggregate.AverageStrength:0.###} | avg s {aggregate.SignedStrength:0.###}";
            if (isDormant)
            {
                detail = $"{detail} | topology only (no events in window)";
            }

            var directionStroke = GetRegionEdgeDirectionColor(edgeKind, isDormant);
            var activityStroke = GetActivityEdgeColor(signedSignal, activityIntensity, isDormant, renderOptions.ColorCurve);
            edges.Add(new VizActivityCanvasEdge(
                routeLabel,
                detail,
                curve.PathData,
                curve.Start.X,
                curve.Start.Y,
                curve.Control.X,
                curve.Control.Y,
                curve.End.X,
                curve.End.Y,
                directionStroke,
                directionDashArray,
                activityStroke,
                thickness,
                activityThickness,
                hitTestThickness,
                opacity,
                activityOpacity,
                false,
                aggregate.LastTick,
                aggregate.EventCount,
                route.SourceRegionId,
                route.TargetRegionId,
                isSelected,
                isHovered,
                isPinned));
        }

        return edges
            .OrderByDescending(item => item.IsSelected)
            .ThenByDescending(item => item.IsHovered)
            .ThenByDescending(item => item.IsPinned)
            .ThenByDescending(item => item.EventCount)
            .ThenByDescending(item => item.LastTick)
            .ThenBy(item => item.RouteLabel, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private sealed record RegionNodeSource(
        uint RegionId,
        int EventCount,
        ulong LastTick,
        int FiredCount,
        int AxonCount,
        string DominantType,
        float AverageMagnitude,
        float SignedValue);

    private readonly record struct RegionBufferMetric(
        int BufferCount,
        ulong LatestBufferTick,
        float LatestBufferValue,
        float AverageBufferValue)
    {
        public static RegionBufferMetric Empty { get; } = new(0, 0, 0f, 0f);

        public RegionBufferMetric Merge(ulong tickId, float value)
        {
            var nextCount = BufferCount + 1;
            var weight = nextCount == 0 ? 0f : 1f / nextCount;
            var nextAverage = (AverageBufferValue * (1f - weight)) + (value * weight);
            if (tickId >= LatestBufferTick)
            {
                return new RegionBufferMetric(nextCount, tickId, value, nextAverage);
            }

            return new RegionBufferMetric(nextCount, LatestBufferTick, LatestBufferValue, nextAverage);
        }
    }

    private readonly record struct RegionRouteDegree(int OutboundCount, int InboundCount)
    {
        public static RegionRouteDegree Empty { get; } = new(0, 0);

        public RegionRouteDegree IncrementOutbound()
            => new(OutboundCount + 1, InboundCount);

        public RegionRouteDegree IncrementInbound()
            => new(OutboundCount, InboundCount + 1);
    }

    private readonly record struct RouteAggregate(
        int EventCount,
        ulong LastTick,
        float AverageMagnitude,
        float AverageStrength,
        float SignedValue,
        float SignedStrength,
        int RouteCount)
    {
        public static RouteAggregate Empty { get; } = new(0, 0, 0f, 0f, 0f, 0f, 0);

        public RouteAggregate Merge(
            int eventCount,
            ulong lastTick,
            float averageMagnitude,
            float averageStrength,
            float averageSignedValue,
            float averageSignedStrength,
            int routeCountIncrement = 1)
        {
            var nextCount = EventCount + Math.Max(0, eventCount);
            var weight = nextCount == 0 ? 0f : (float)Math.Max(0, eventCount) / nextCount;
            var nextMagnitude = (AverageMagnitude * (1f - weight)) + (averageMagnitude * weight);
            var nextStrength = (AverageStrength * (1f - weight)) + (averageStrength * weight);
            var nextSignedValue = (SignedValue * (1f - weight)) + (averageSignedValue * weight);
            var nextSignedStrength = (SignedStrength * (1f - weight)) + (averageSignedStrength * weight);
            var nextRouteCount = RouteCount + Math.Max(0, routeCountIncrement);
            return new RouteAggregate(nextCount, Math.Max(LastTick, lastTick), nextMagnitude, nextStrength, nextSignedValue, nextSignedStrength, nextRouteCount);
        }
    }
}
