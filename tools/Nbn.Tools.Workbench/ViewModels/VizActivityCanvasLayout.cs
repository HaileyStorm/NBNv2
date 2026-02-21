using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed record VizActivityCanvasLayout(
    double Width,
    double Height,
    string Legend,
    IReadOnlyList<VizActivityCanvasNode> Nodes,
    IReadOnlyList<VizActivityCanvasEdge> Edges);

public sealed record VizActivityCanvasInteractionState(
    string? SelectedNodeKey,
    string? SelectedRouteLabel,
    string? HoverNodeKey,
    string? HoverRouteLabel,
    IReadOnlySet<string> PinnedNodeKeys,
    IReadOnlySet<string> PinnedRouteLabels)
{
    public static VizActivityCanvasInteractionState Empty { get; } = new(
        null,
        null,
        null,
        null,
        new HashSet<string>(StringComparer.OrdinalIgnoreCase),
        new HashSet<string>(StringComparer.OrdinalIgnoreCase));

    public bool IsSelectedNode(string? nodeKey) => KeyEquals(SelectedNodeKey, nodeKey);

    public bool IsHoveredNode(string? nodeKey) => KeyEquals(HoverNodeKey, nodeKey);

    public bool IsSelectedRoute(string? routeLabel) => KeyEquals(SelectedRouteLabel, routeLabel);

    public bool IsHoveredRoute(string? routeLabel) => KeyEquals(HoverRouteLabel, routeLabel);

    public bool IsNodePinned(string? nodeKey)
    {
        if (string.IsNullOrWhiteSpace(nodeKey))
        {
            return false;
        }

        foreach (var pinned in PinnedNodeKeys)
        {
            if (KeyEquals(pinned, nodeKey))
            {
                return true;
            }
        }

        return false;
    }

    public bool IsRoutePinned(string? routeLabel)
    {
        if (string.IsNullOrWhiteSpace(routeLabel))
        {
            return false;
        }

        foreach (var pinned in PinnedRouteLabels)
        {
            if (KeyEquals(pinned, routeLabel))
            {
                return true;
            }
        }

        return false;
    }

    private static bool KeyEquals(string? left, string? right)
        => string.Equals(left, right, StringComparison.OrdinalIgnoreCase);
}

public readonly record struct VizActivityCanvasRegionRoute(uint SourceRegionId, uint TargetRegionId);

public readonly record struct VizActivityCanvasNeuronRoute(uint SourceAddress, uint TargetAddress);

public sealed record VizActivityCanvasTopology(
    IReadOnlySet<uint> Regions,
    IReadOnlySet<VizActivityCanvasRegionRoute> RegionRoutes,
    IReadOnlySet<uint> NeuronAddresses,
    IReadOnlySet<VizActivityCanvasNeuronRoute> NeuronRoutes)
{
    public static VizActivityCanvasTopology Empty { get; } = new(
        new HashSet<uint>(),
        new HashSet<VizActivityCanvasRegionRoute>(),
        new HashSet<uint>(),
        new HashSet<VizActivityCanvasNeuronRoute>());
}

public sealed record VizActivityCanvasNode(
    string NodeKey,
    uint RegionId,
    uint? NeuronId,
    uint NavigateRegionId,
    string Label,
    string Detail,
    double Left,
    double Top,
    double Diameter,
    string Fill,
    string Stroke,
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
    double SourceX,
    double SourceY,
    double ControlX,
    double ControlY,
    double TargetX,
    double TargetY,
    string Stroke,
    string DirectionDashArray,
    string ActivityStroke,
    double StrokeThickness,
    double ActivityStrokeThickness,
    double HitTestThickness,
    double Opacity,
    double ActivityOpacity,
    bool IsFocused,
    ulong LastTick,
    int EventCount,
    uint? SourceRegionId,
    uint? TargetRegionId,
    bool IsSelected,
    bool IsHovered,
    bool IsPinned);

public enum VizActivityCanvasColorMode
{
    StateValue = 0,
    Activity = 1,
    Topology = 2
}

public static class VizActivityCanvasLayoutBuilder
{
    public const double CanvasWidth = 860;
    public const double CanvasHeight = 420;
    private const double CenterX = CanvasWidth / 2.0;
    private const double CenterY = CanvasHeight / 2.0;
    private const double CanvasPadding = 26;
    private const double MinRegionNodeRadius = 16;
    private const double MaxRegionNodeRadius = 29;
    private const double MinFocusNeuronNodeRadius = 11;
    private const double MaxFocusNeuronNodeRadius = 19;
    private const double MinGatewayNodeRadius = 11;
    private const double MaxGatewayNodeRadius = 16;
    private const double RegionNodePositionPadding = CanvasPadding + MaxRegionNodeRadius + 4;
    private const double EdgeControlPadding = CanvasPadding;
    private const double EdgeNodeClearance = 4;
    private const double BaseEdgeStroke = 1.1;
    private const double MaxEdgeStrokeBoost = 2.8;
    private const int EdgeCurveCacheMaxEntries = 4096;
    private static readonly object EdgeCurveCacheGate = new();
    private static readonly Dictionary<CanvasEdgeCurveKey, CanvasEdgeCurve> EdgeCurveCache = new();
    private static readonly Queue<CanvasEdgeCurveKey> EdgeCurveCacheOrder = new();

    public static VizActivityCanvasLayout Build(
        VizActivityProjection projection,
        VizActivityProjectionOptions options,
        VizActivityCanvasInteractionState? interaction = null,
        VizActivityCanvasTopology? topology = null,
        VizActivityCanvasColorMode colorMode = VizActivityCanvasColorMode.StateValue)
    {
        interaction ??= VizActivityCanvasInteractionState.Empty;
        topology ??= VizActivityCanvasTopology.Empty;

        var latestTick = ResolveLatestTick(projection);
        if (options.FocusRegionId is uint focusRegionId)
        {
            return BuildFocusedNeuronCanvas(projection, options, topology, interaction, latestTick, focusRegionId, colorMode);
        }

        return BuildRegionCanvas(projection, options, topology, interaction, latestTick, colorMode);
    }

    private static VizActivityCanvasLayout BuildRegionCanvas(
        VizActivityProjection projection,
        VizActivityProjectionOptions options,
        VizActivityCanvasTopology topology,
        VizActivityCanvasInteractionState interaction,
        ulong latestTick,
        VizActivityCanvasColorMode colorMode)
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

        var positions = BuildRegionPositions(nodeSource.Keys);
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
            var emphasis = (isSelected ? 0.25 : 0.0) + (isHovered ? 0.14 : 0.0) + (isPinned ? 0.18 : 0.0);
            var stroke = GetTopologyStrokeColor(regionId, isDormant);
            var fill = ResolveRegionFillColor(colorMode, regionId, stats.SignedValue, loadRatio, tickRecency, isDormant);
            var fillOpacity = Math.Clamp((isDormant ? 0.14 : 0.36 + (0.45 * loadRatio)) + emphasis, 0.12, 1.0);
            var pulseOpacity = Math.Clamp((isDormant ? 0.1 : 0.25 + (0.45 * tickRecency)) + (emphasis * 0.6), 0.08, 1.0);
            var strokeThickness = 1.3
                                  + (isPinned ? 0.8 : 0.0)
                                  + (isHovered ? 0.6 : 0.0)
                                  + (isSelected ? 0.9 : 0.0);
            regionBufferMetrics.TryGetValue(regionId, out var bufferMetrics);
            regionRouteDegrees.TryGetValue(regionId, out var routeDegree);
            var structureRatio = Math.Clamp((double)(routeDegree.OutboundCount + routeDegree.InboundCount) / maxRouteDegree, 0.0, 1.0);
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

        var edges = BuildRegionEdges(projection, options, topology, nodeByRegion, interaction, latestTick);
        var legend = $"Region map | Regions {nodes.Count} | Routes {edges.Count} | Latest tick {latestTick}";
        return new VizActivityCanvasLayout(CanvasWidth, CanvasHeight, legend, nodes, edges);
    }

    private static VizActivityCanvasLayout BuildFocusedNeuronCanvas(
        VizActivityProjection projection,
        VizActivityProjectionOptions options,
        VizActivityCanvasTopology topology,
        VizActivityCanvasInteractionState interaction,
        ulong latestTick,
        uint focusRegionId,
        VizActivityCanvasColorMode colorMode)
    {
        var routes = BuildFocusRoutes(projection, topology, focusRegionId);
        var focusNeuronStats = BuildFocusNeuronStats(routes, projection.WindowEvents, topology, focusRegionId);
        var gatewayStats = BuildGatewayStats(routes, focusRegionId);
        var regionAggregates = projection.Regions.ToDictionary(item => item.RegionId);
        var regionBufferMetrics = BuildRegionBufferMetrics(projection.WindowEvents);
        var regionRouteDegrees = BuildRegionRouteDegrees(projection, topology);

        var nodes = new List<VizActivityCanvasNode>();
        var nodeByKey = new Dictionary<string, VizActivityCanvasNode>(StringComparer.OrdinalIgnoreCase);

        var sortedNeurons = focusNeuronStats.Keys.OrderBy(NeuronFromAddress).ToList();
        var neuronMaxOrbit = Math.Min(
            CenterX - (CanvasPadding + MaxFocusNeuronNodeRadius + 6.0),
            (CenterY - (CanvasPadding + MaxFocusNeuronNodeRadius + 6.0)) / 0.94);
        var neuronMinOrbit = Math.Min(64.0, neuronMaxOrbit);
        var neuronPositions = BuildConcentricPositions(
            sortedNeurons.Count,
            minRadius: neuronMinOrbit,
            maxRadius: neuronMaxOrbit,
            yScale: 0.94,
            minCenterSpacing: (MaxFocusNeuronNodeRadius * 2.0) + 2.0);
        var maxNeuronEvents = Math.Max(1, focusNeuronStats.Values.Max(item => item.EventCount));
        var maxNeuronFlowDegree = Math.Max(1, focusNeuronStats.Values.Max(item => item.OutboundCount + item.InboundCount));
        for (var index = 0; index < sortedNeurons.Count; index++)
        {
            var address = sortedNeurons[index];
            var stats = focusNeuronStats[address];
            var neuronId = NeuronFromAddress(address);
            var nodeKey = NodeKeyForNeuron(address);
            var position = neuronPositions[index];
            var isSelected = interaction.IsSelectedNode(nodeKey);
            var isHovered = interaction.IsHoveredNode(nodeKey);
            var isPinned = interaction.IsNodePinned(nodeKey);
            var isDormant = stats.EventCount <= 0 || stats.LastTick == 0;
            var loadRatio = isDormant ? 0.0 : Math.Clamp((double)stats.EventCount / maxNeuronEvents, 0.0, 1.0);
            var structureRatio = Math.Clamp((double)(stats.OutboundCount + stats.InboundCount) / maxNeuronFlowDegree, 0.0, 1.0);
            var radius = MinFocusNeuronNodeRadius
                         + ((MaxFocusNeuronNodeRadius - MinFocusNeuronNodeRadius) * ((structureRatio * 0.75) + (loadRatio * 0.25)));
            var emphasis = (isSelected ? 0.24 : 0.0) + (isHovered ? 0.12 : 0.0) + (isPinned ? 0.16 : 0.0);
            var fillOpacity = Math.Clamp((isDormant ? 0.15 : 0.34 + (0.45 * loadRatio)) + emphasis, 0.12, 1.0);
            var pulseOpacity = Math.Clamp((isDormant ? 0.1 : 0.28 + (0.35 * TickRecency(stats.LastTick, latestTick, options.TickWindow))) + (emphasis * 0.6), 0.08, 1.0);
            var strokeThickness = 1.2 + (isPinned ? 0.8 : 0.0) + (isHovered ? 0.6 : 0.0) + (isSelected ? 0.9 : 0.0);
            var stroke = GetTopologyStrokeColor(focusRegionId, isDormant);
            var fill = ResolveFocusFillColor(colorMode, focusRegionId, stats.AverageValue, loadRatio, isDormant);
            var bufferAverageText = FormatBufferAverage(stats.BufferCount, stats.AverageBufferValue);
            var bufferLatestText = FormatBufferLatest(stats.BufferCount, stats.LatestBufferValue, stats.LatestBufferTick);
            var detail = $"R{focusRegionId}N{neuronId} | events {stats.EventCount} | last tick {stats.LastTick}"
                         + $" | fired {stats.FiredCount} | out {stats.OutboundCount} in {stats.InboundCount}"
                         + $" | avg v {stats.AverageValue:0.###}"
                         + $" | value n={stats.BufferCount} avg={bufferAverageText} latest={bufferLatestText}";
            if (isDormant)
            {
                detail = $"{detail} | inactive in window";
            }

            var node = new VizActivityCanvasNode(
                nodeKey,
                focusRegionId,
                neuronId,
                focusRegionId,
                $"N{neuronId}",
                detail,
                position.X - radius,
                position.Y - radius,
                radius * 2.0,
                fill,
                stroke,
                fillOpacity,
                pulseOpacity,
                strokeThickness,
                true,
                stats.LastTick,
                stats.EventCount,
                isSelected,
                isHovered,
                isPinned);
            nodes.Add(node);
            nodeByKey[nodeKey] = node;
        }

        var sortedGateways = gatewayStats.Keys.OrderBy(item => item).ToList();
        var gatewayPositions = BuildCircularPositions(sortedGateways.Count, 168, 198, yScale: 0.9);
        var maxGatewayRouteDegree = Math.Max(
            1,
            sortedGateways.Count == 0
                ? 1
                : sortedGateways.Max(regionId =>
                {
                    regionRouteDegrees.TryGetValue(regionId, out var degree);
                    return degree.OutboundCount + degree.InboundCount;
                }));
        for (var index = 0; index < sortedGateways.Count; index++)
        {
            var regionId = sortedGateways[index];
            var stats = gatewayStats[regionId];
            var nodeKey = NodeKeyForGateway(regionId);
            var position = gatewayPositions[index];
            var isSelected = interaction.IsSelectedNode(nodeKey);
            var isHovered = interaction.IsHoveredNode(nodeKey);
            var isPinned = interaction.IsNodePinned(nodeKey);
            var emphasis = (isSelected ? 0.24 : 0.0) + (isHovered ? 0.12 : 0.0) + (isPinned ? 0.16 : 0.0);
            var fillOpacity = Math.Clamp(0.24 + (stats.EventCount > 0 ? 0.22 : 0.0) + emphasis, 0.18, 1.0);
            var pulseOpacity = Math.Clamp(0.16 + (stats.EventCount > 0 ? 0.24 : 0.0) + (emphasis * 0.6), 0.14, 1.0);
            var strokeThickness = 1.2 + (isPinned ? 0.7 : 0.0) + (isHovered ? 0.5 : 0.0) + (isSelected ? 0.8 : 0.0);
            var role = stats.HasInbound && stats.HasOutbound
                ? "bidirectional"
                : stats.HasInbound
                    ? "inbound"
                    : "outbound";
            var (_, topologyStroke) = GetGatewayPalette(role);
            regionAggregates.TryGetValue(regionId, out var aggregate);
            regionBufferMetrics.TryGetValue(regionId, out var bufferMetrics);
            regionRouteDegrees.TryGetValue(regionId, out var routeDegree);
            var aggregateEventCount = aggregate?.EventCount ?? 0;
            var aggregateLastTick = aggregate?.LastTick ?? 0;
            var aggregateFired = aggregate?.FiredCount ?? 0;
            var aggregateAxon = aggregate?.AxonCount ?? 0;
            var aggregateDominantType = aggregate?.DominantType ?? "unknown";
            var aggregateAverageMagnitude = aggregate?.AverageMagnitude ?? 0f;
            var aggregateAverageSigned = aggregate?.AverageSignedValue ?? 0f;
            var aggregateDormant = aggregateEventCount <= 0 || aggregateLastTick == 0;
            var structureRatio = Math.Clamp((double)(routeDegree.OutboundCount + routeDegree.InboundCount) / maxGatewayRouteDegree, 0.0, 1.0);
            var activityRatio = aggregateEventCount > 0 ? 1.0 : 0.0;
            var radius = MinGatewayNodeRadius
                         + ((MaxGatewayNodeRadius - MinGatewayNodeRadius) * ((structureRatio * 0.8) + (activityRatio * 0.2)));
            var fill = ResolveFocusFillColor(
                colorMode,
                regionId,
                aggregateAverageSigned,
                activityRatio,
                isDormant: aggregateDormant);
            var stroke = aggregateDormant ? DimColor(topologyStroke) : topologyStroke;
            var bufferAverageText = FormatBufferAverage(bufferMetrics);
            var bufferLatestText = FormatBufferLatest(bufferMetrics);
            var detail = $"R{regionId} gateway | {role} | events {stats.EventCount} | last tick {stats.LastTick}"
                         + $" | agg events {aggregateEventCount} | agg last tick {aggregateLastTick}"
                         + $" | fired {aggregateFired} | axon {aggregateAxon}"
                         + $" | dom {aggregateDominantType} | avg |v| {aggregateAverageMagnitude:0.###} | avg v {aggregateAverageSigned:0.###}"
                         + $" | routes out {routeDegree.OutboundCount} in {routeDegree.InboundCount}"
                         + $" | value n={bufferMetrics.BufferCount} avg={bufferAverageText} latest={bufferLatestText}";
            if (stats.EventCount == 0 && aggregateEventCount == 0)
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
            nodeByKey[nodeKey] = node;
        }

        var edges = BuildFocusedEdges(routes, nodeByKey, focusRegionId, options.TickWindow, latestTick, interaction);
        var gatewayCount = sortedGateways.Count;
        var legend = $"Focus R{focusRegionId} | Neurons {sortedNeurons.Count} | Gateways {gatewayCount} | Routes {edges.Count} | Latest tick {latestTick}";
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
        ulong latestTick)
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
            var opacity = Math.Clamp((isDormant ? 0.16 : 0.32 + (0.45 * recency)) + emphasis, 0.14, 1.0);
            var activityIntensity = isDormant
                ? 0.0
                : Math.Clamp((0.45 * normalizedLoad) + (0.25 * recency) + (0.30 * Math.Abs(signedSignal)), 0.0, 1.0);
            var activityOpacity = Math.Clamp((isDormant ? 0.14 : 0.26 + (0.62 * activityIntensity)) + (emphasis * 0.5), 0.12, 1.0);
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
            var activityStroke = GetActivityEdgeColor(signedSignal, activityIntensity, isDormant);
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

    private static Dictionary<VizActivityCanvasNeuronRoute, FocusRouteAggregate> BuildFocusRoutes(
        VizActivityProjection projection,
        VizActivityCanvasTopology topology,
        uint focusRegionId)
    {
        var routes = new Dictionary<VizActivityCanvasNeuronRoute, FocusRouteAggregate>();

        foreach (var item in projection.WindowEvents)
        {
            if (!IsAxonType(item.Type))
            {
                continue;
            }

            if (!TryParseAddress(item.Source, out var sourceAddress) || !TryParseAddress(item.Target, out var targetAddress))
            {
                continue;
            }

            if (!TouchesFocusRegion(sourceAddress, targetAddress, focusRegionId))
            {
                continue;
            }

            var key = new VizActivityCanvasNeuronRoute(sourceAddress, targetAddress);
            if (!routes.TryGetValue(key, out var existing))
            {
                existing = FocusRouteAggregate.Empty;
            }

            routes[key] = existing.Merge(
                item.TickId,
                item.Value,
                item.Strength);
        }

        foreach (var route in topology.NeuronRoutes)
        {
            if (!TouchesFocusRegion(route.SourceAddress, route.TargetAddress, focusRegionId))
            {
                continue;
            }

            if (!routes.ContainsKey(route))
            {
                routes[route] = FocusRouteAggregate.Empty.WithRoutePresence();
            }
        }

        return routes;
    }

    private static Dictionary<uint, FocusNeuronStat> BuildFocusNeuronStats(
        IReadOnlyDictionary<VizActivityCanvasNeuronRoute, FocusRouteAggregate> routes,
        IReadOnlyList<VizEventItem> events,
        VizActivityCanvasTopology topology,
        uint focusRegionId)
    {
        var byNeuronAddress = new Dictionary<uint, FocusNeuronStat>();
        foreach (var address in topology.NeuronAddresses)
        {
            if (RegionFromAddress(address) != focusRegionId)
            {
                continue;
            }

            byNeuronAddress[address] = FocusNeuronStat.Empty;
        }

        foreach (var (route, aggregate) in routes)
        {
            if (RegionFromAddress(route.SourceAddress) == focusRegionId)
            {
                byNeuronAddress.TryGetValue(route.SourceAddress, out var existing);
                byNeuronAddress[route.SourceAddress] = existing.WithOutboundCount(aggregate.EventCount, aggregate.LastTick);
            }

            if (RegionFromAddress(route.TargetAddress) == focusRegionId)
            {
                byNeuronAddress.TryGetValue(route.TargetAddress, out var existing);
                byNeuronAddress[route.TargetAddress] = existing.WithInboundCount(aggregate.EventCount, aggregate.LastTick);
            }
        }

        foreach (var item in events)
        {
            if (IsFiredType(item.Type))
            {
                if (!TryParseAddress(item.Source, out var sourceAddress) || RegionFromAddress(sourceAddress) != focusRegionId)
                {
                    continue;
                }

                byNeuronAddress.TryGetValue(sourceAddress, out var existing);
                byNeuronAddress[sourceAddress] = existing
                    .WithFired(item.TickId)
                    .WithValueSample(item.TickId, item.Value, includeEventCount: false);
                continue;
            }

            if (!IsValueMetricType(item.Type))
            {
                continue;
            }

            if (!TryParseAddress(item.Source, out var bufferAddress) || RegionFromAddress(bufferAddress) != focusRegionId)
            {
                continue;
            }

            byNeuronAddress.TryGetValue(bufferAddress, out var current);
            byNeuronAddress[bufferAddress] = current.WithValueSample(item.TickId, item.Value, includeEventCount: IsBufferType(item.Type));
        }

        if (byNeuronAddress.Count == 0)
        {
            var defaultAddress = ComposeAddress(focusRegionId, 0);
            byNeuronAddress[defaultAddress] = FocusNeuronStat.Empty;
        }

        return byNeuronAddress;
    }

    private static Dictionary<uint, GatewayStat> BuildGatewayStats(
        IReadOnlyDictionary<VizActivityCanvasNeuronRoute, FocusRouteAggregate> routes,
        uint focusRegionId)
    {
        var byRegion = new Dictionary<uint, GatewayStat>();
        foreach (var (route, aggregate) in routes)
        {
            var sourceRegion = RegionFromAddress(route.SourceAddress);
            var targetRegion = RegionFromAddress(route.TargetAddress);
            if (sourceRegion == focusRegionId && targetRegion != focusRegionId)
            {
                byRegion.TryGetValue(targetRegion, out var existing);
                byRegion[targetRegion] = existing.WithOutbound(aggregate.EventCount, aggregate.LastTick);
            }
            else if (targetRegion == focusRegionId && sourceRegion != focusRegionId)
            {
                byRegion.TryGetValue(sourceRegion, out var existing);
                byRegion[sourceRegion] = existing.WithInbound(aggregate.EventCount, aggregate.LastTick);
            }
        }

        return byRegion;
    }

    private static IReadOnlyList<VizActivityCanvasEdge> BuildFocusedEdges(
        IReadOnlyDictionary<VizActivityCanvasNeuronRoute, FocusRouteAggregate> routes,
        IReadOnlyDictionary<string, VizActivityCanvasNode> nodeByKey,
        uint focusRegionId,
        int tickWindow,
        ulong latestTick,
        VizActivityCanvasInteractionState interaction)
    {
        if (routes.Count == 0)
        {
            return Array.Empty<VizActivityCanvasEdge>();
        }

        var displayRoutes = new Dictionary<FocusDisplayRouteKey, FocusRouteAggregate>();

        foreach (var (route, aggregate) in routes)
        {
            var sourceRegion = RegionFromAddress(route.SourceAddress);
            var targetRegion = RegionFromAddress(route.TargetAddress);
            var sourceKey = sourceRegion == focusRegionId
                ? NodeKeyForNeuron(route.SourceAddress)
                : NodeKeyForGateway(sourceRegion);
            var targetKey = targetRegion == focusRegionId
                ? NodeKeyForNeuron(route.TargetAddress)
                : NodeKeyForGateway(targetRegion);

            if (!nodeByKey.TryGetValue(sourceKey, out var sourceNode)
                || !nodeByKey.TryGetValue(targetKey, out var targetNode))
            {
                continue;
            }

            // Collapse route fan-out into displayed node/gateway routes so rendering
            // scales with visible edges instead of raw axon count.
            var key = new FocusDisplayRouteKey(sourceKey, targetKey, sourceRegion, targetRegion);
            if (!displayRoutes.TryGetValue(key, out var existing))
            {
                displayRoutes[key] = aggregate;
                continue;
            }

            displayRoutes[key] = MergeFocusRouteAggregate(existing, aggregate);
        }

        if (displayRoutes.Count == 0)
        {
            return Array.Empty<VizActivityCanvasEdge>();
        }

        var routeSet = new HashSet<(string SourceKey, string TargetKey)>(
            displayRoutes.Keys.Select(item => (item.SourceNodeKey, item.TargetNodeKey)));
        var maxEdgeEvents = Math.Max(1, displayRoutes.Values.Max(item => item.EventCount));
        var edges = new List<VizActivityCanvasEdge>(displayRoutes.Count);

        foreach (var (key, aggregate) in displayRoutes)
        {
            if (!nodeByKey.TryGetValue(key.SourceNodeKey, out var sourceNode)
                || !nodeByKey.TryGetValue(key.TargetNodeKey, out var targetNode))
            {
                continue;
            }

            var hasReverse = routeSet.Contains((key.TargetNodeKey, key.SourceNodeKey))
                             && !string.Equals(key.SourceNodeKey, key.TargetNodeKey, StringComparison.OrdinalIgnoreCase);
            var kind = GetFocusedEdgeKind(key.SourceRegionId, key.TargetRegionId, focusRegionId, hasReverse);
            var routeLabel = BuildFocusedDisplayRouteLabel(key.SourceNodeKey, key.TargetNodeKey, focusRegionId);
            var isSelected = interaction.IsSelectedRoute(routeLabel);
            var isHovered = interaction.IsHoveredRoute(routeLabel);
            var isPinned = interaction.IsRoutePinned(routeLabel);
            var directionDashArray = GetFocusedDirectionDashPattern(kind);
            var isDormant = aggregate.EventCount <= 0 || aggregate.LastTick == 0;
            var recency = isDormant ? 0.0 : TickRecency(aggregate.LastTick, latestTick, tickWindow);
            var normalizedLoad = isDormant ? 0.0 : Math.Clamp((double)aggregate.EventCount / maxEdgeEvents, 0.0, 1.0);
            var normalizedStrength = Math.Clamp((double)Math.Abs(aggregate.AverageStrength), 0.0, 1.0);
            var signedSignal = Clamp(aggregate.SignedValue * 0.75f + aggregate.SignedStrength * 0.25f, -1.0, 1.0);
            var emphasis = (isSelected ? 0.22 : 0.0) + (isHovered ? 0.12 : 0.0) + (isPinned ? 0.16 : 0.0);
            var thickness = BaseEdgeStroke
                            + (normalizedLoad * MaxEdgeStrokeBoost)
                            + (normalizedStrength * 0.7)
                            + 1.1
                            + (kind == "bidirectional" ? 0.25 : 0.0)
                            + (isPinned ? 0.8 : 0.0)
                            + (isHovered ? 0.5 : 0.0)
                            + (isSelected ? 0.9 : 0.0);
            var opacity = Math.Clamp((isDormant ? 0.17 : 0.34 + (0.44 * recency)) + emphasis, 0.15, 1.0);
            var activityIntensity = isDormant
                ? 0.0
                : Math.Clamp((0.45 * normalizedLoad) + (0.25 * recency) + (0.30 * Math.Abs(signedSignal)), 0.0, 1.0);
            var activityOpacity = Math.Clamp((isDormant ? 0.14 : 0.28 + (0.60 * activityIntensity)) + (emphasis * 0.5), 0.12, 1.0);
            var activityThickness = Math.Max(0.7, thickness - 2.2);
            var hitTestThickness = Math.Max(9.0, thickness + 7.0);
            var sourceCenter = new CanvasPoint(sourceNode.Left + (sourceNode.Diameter / 2.0), sourceNode.Top + (sourceNode.Diameter / 2.0));
            var targetCenter = new CanvasPoint(targetNode.Left + (targetNode.Diameter / 2.0), targetNode.Top + (targetNode.Diameter / 2.0));
            var sourceRadius = sourceNode.Diameter / 2.0;
            var targetRadius = targetNode.Diameter / 2.0;
            var curveDirection = hasReverse && string.CompareOrdinal(key.SourceNodeKey, key.TargetNodeKey) > 0 ? -1 : 1;
            var curve = BuildEdgeCurve(
                sourceCenter,
                targetCenter,
                string.Equals(key.SourceNodeKey, key.TargetNodeKey, StringComparison.OrdinalIgnoreCase),
                curveDirection,
                sourceRadius,
                targetRadius);
            var detail = $"{routeLabel} | {kind} | routes {aggregate.RouteCount} | events {aggregate.EventCount} | last tick {aggregate.LastTick}"
                         + $" | avg |v| {aggregate.AverageMagnitude:0.###} | avg v {aggregate.SignedValue:0.###}"
                         + $" | avg |s| {aggregate.AverageStrength:0.###} | avg s {aggregate.SignedStrength:0.###}";
            if (isDormant)
            {
                detail = $"{detail} | topology only (no events in window)";
            }

            var directionStroke = GetFocusedEdgeDirectionColor(kind, isDormant);
            var activityStroke = GetActivityEdgeColor(signedSignal, activityIntensity, isDormant);
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
                true,
                aggregate.LastTick,
                aggregate.EventCount,
                key.SourceRegionId,
                key.TargetRegionId,
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

    private static FocusRouteAggregate MergeFocusRouteAggregate(FocusRouteAggregate current, FocusRouteAggregate next)
    {
        var combinedRouteCount = current.RouteCount + next.RouteCount;
        if (current.EventCount <= 0)
        {
            return new FocusRouteAggregate(
                next.EventCount,
                Math.Max(current.LastTick, next.LastTick),
                next.AverageMagnitude,
                next.AverageStrength,
                next.SignedValue,
                next.SignedStrength,
                combinedRouteCount);
        }

        if (next.EventCount <= 0)
        {
            return new FocusRouteAggregate(
                current.EventCount,
                Math.Max(current.LastTick, next.LastTick),
                current.AverageMagnitude,
                current.AverageStrength,
                current.SignedValue,
                current.SignedStrength,
                combinedRouteCount);
        }

        var combinedCount = current.EventCount + next.EventCount;
        var nextWeight = (float)next.EventCount / combinedCount;
        var currentWeight = 1f - nextWeight;
        return new FocusRouteAggregate(
            combinedCount,
            Math.Max(current.LastTick, next.LastTick),
            (current.AverageMagnitude * currentWeight) + (next.AverageMagnitude * nextWeight),
            (current.AverageStrength * currentWeight) + (next.AverageStrength * nextWeight),
            (current.SignedValue * currentWeight) + (next.SignedValue * nextWeight),
            (current.SignedStrength * currentWeight) + (next.SignedStrength * nextWeight),
            combinedRouteCount);
    }

    private static Dictionary<uint, CanvasPoint> BuildRegionPositions(IEnumerable<uint> regionIds)
    {
        var groupsBySlice = regionIds
            .GroupBy(GetRegionSlice)
            .OrderBy(group => group.Key)
            .ToDictionary(group => group.Key, group => group.OrderBy(region => region).ToList());

        const int minSlice = -3;
        const int maxSlice = 3;
        var sliceSpan = Math.Max(1, maxSlice - minSlice);
        var availableWidth = CanvasWidth - (RegionNodePositionPadding * 2.0);
        var availableHeight = CanvasHeight - (RegionNodePositionPadding * 2.0);
        var compressedHalfWidth = (availableWidth * 0.72) / 2.0;
        var maxLaneHalfHeight = Math.Max(54.0, availableHeight * 0.62);
        var positions = new Dictionary<uint, CanvasPoint>();

        foreach (var (slice, regions) in groupsBySlice)
        {
            var normalizedSlice = ((double)(slice - minSlice) / sliceSpan) * 2.0 - 1.0;
            var axisX = CenterX + (normalizedSlice * compressedHalfWidth);
            var depthRatio = 1.0 - (Math.Abs(slice) / (double)maxSlice);
            var sliceWave = slice switch
            {
                -3 or 0 or 3 => 0.0,
                _ => (((slice - minSlice) & 1) == 0 ? 1.0 : -1.0) * (34.0 + (24.0 * depthRatio))
            };
            var laneCenterY = CenterY + sliceWave;
            var count = regions.Count;
            var laneHalfHeight = count <= 1
                ? 0.0
                : Math.Clamp(
                    (24.0 + ((count - 1) * 16.0)) * (0.78 + (0.32 * depthRatio)),
                    42.0,
                    maxLaneHalfHeight);
            var laneHalfWidth = count <= 1 ? 0.0 : 7.0 + (4.0 * depthRatio);

            for (var index = 0; index < count; index++)
            {
                var laneOffset = count <= 1
                    ? 0.0
                    : ((double)index / (count - 1) * 2.0) - 1.0;
                var x = axisX + (laneOffset * laneHalfWidth);
                var y = laneCenterY + (laneOffset * laneHalfHeight);
                positions[regions[index]] = new CanvasPoint(
                    Clamp(x, RegionNodePositionPadding, CanvasWidth - RegionNodePositionPadding),
                    Clamp(y, RegionNodePositionPadding, CanvasHeight - RegionNodePositionPadding));
            }
        }

        return positions;
    }

    private static IReadOnlyList<CanvasPoint> BuildConcentricPositions(
        int count,
        double minRadius,
        double maxRadius,
        double yScale,
        double minCenterSpacing)
    {
        if (count <= 0)
        {
            return Array.Empty<CanvasPoint>();
        }

        if (count == 1)
        {
            return new[] { new CanvasPoint(CenterX, CenterY) };
        }

        var safeYScale = Math.Clamp(yScale, 0.45, 1.15);
        var safeSpacing = Math.Max(8.0, minCenterSpacing);
        var safeMinRadius = Math.Max(0.0, minRadius);
        var safeMaxRadius = Math.Max(safeMinRadius, maxRadius);
        var ringGap = Math.Max(24.0, safeSpacing + 12.0);
        var positions = new List<CanvasPoint>(count);

        var remaining = count;
        var ringIndex = 0;
        while (remaining > 0)
        {
            var radius = safeMinRadius + (ringIndex * ringGap);
            if (radius > safeMaxRadius)
            {
                radius = safeMaxRadius;
            }

            var effectiveCircumference = 2.0 * Math.PI * Math.Max(1.0, radius * safeYScale);
            var naturalCapacity = Math.Max(6, (int)Math.Floor(effectiveCircumference / safeSpacing));
            var ringCount = Math.Min(naturalCapacity, remaining);
            var nextRadius = Math.Min(safeMaxRadius, radius + ringGap);
            var nextRingGap = nextRadius - radius;
            var canPlaceSafeNextRing = nextRingGap >= (safeSpacing - 0.5);
            if (remaining > ringCount && !canPlaceSafeNextRing)
            {
                ringCount = remaining;
                naturalCapacity = Math.Max(naturalCapacity, ringCount);
            }

            var angleStep = (2.0 * Math.PI) / naturalCapacity;
            var ringPhase = (ringIndex * Math.PI) / Math.Max(3, naturalCapacity);
            for (var index = 0; index < ringCount; index++)
            {
                var slot = ringCount == naturalCapacity
                    ? index
                    : (int)Math.Floor(((index + 0.5) * naturalCapacity) / ringCount);
                var angle = ringPhase + (slot * angleStep);
                var x = CenterX + (Math.Cos(angle) * radius);
                var y = CenterY + (Math.Sin(angle) * radius * safeYScale);
                positions.Add(new CanvasPoint(
                    Clamp(x, CanvasPadding, CanvasWidth - CanvasPadding),
                    Clamp(y, CanvasPadding, CanvasHeight - CanvasPadding)));
            }

            remaining -= ringCount;
            ringIndex++;
            if (radius >= safeMaxRadius)
            {
                break;
            }
        }

        if (positions.Count >= count)
        {
            return positions;
        }

        // Fallback for extreme densities: fill the remaining slots on the outer orbit.
        var fallbackRadius = safeMaxRadius;
        var fallbackRemaining = count - positions.Count;
        var fallbackStep = (2.0 * Math.PI) / fallbackRemaining;
        for (var index = 0; index < fallbackRemaining; index++)
        {
            var angle = (index * fallbackStep) + (Math.PI / 7.0);
            var x = CenterX + (Math.Cos(angle) * fallbackRadius);
            var y = CenterY + (Math.Sin(angle) * fallbackRadius * safeYScale);
            positions.Add(new CanvasPoint(
                Clamp(x, CanvasPadding, CanvasWidth - CanvasPadding),
                Clamp(y, CanvasPadding, CanvasHeight - CanvasPadding)));
        }

        return positions;
    }

    private static IReadOnlyList<CanvasPoint> BuildCircularPositions(int count, double minRadius, double maxRadius, double yScale)
    {
        if (count <= 0)
        {
            return Array.Empty<CanvasPoint>();
        }

        if (count == 1)
        {
            return new[] { new CanvasPoint(CenterX, CenterY) };
        }

        var radius = Math.Clamp(minRadius + (count * 3.0), minRadius, maxRadius);
        var positions = new List<CanvasPoint>(count);
        for (var index = 0; index < count; index++)
        {
            var angle = (2.0 * Math.PI * index) / count;
            var x = CenterX + (Math.Cos(angle) * radius);
            var y = CenterY + (Math.Sin(angle) * radius * yScale);
            positions.Add(new CanvasPoint(
                Clamp(x, CanvasPadding, CanvasWidth - CanvasPadding),
                Clamp(y, CanvasPadding, CanvasHeight - CanvasPadding)));
        }

        return positions;
    }

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

    private static string BuildFocusedRouteLabel(uint sourceAddress, uint targetAddress, uint focusRegionId)
    {
        var sourceRegion = RegionFromAddress(sourceAddress);
        var targetRegion = RegionFromAddress(targetAddress);
        var sourceText = sourceRegion == focusRegionId
            ? $"N{NeuronFromAddress(sourceAddress)}"
            : $"R{sourceRegion}";
        var targetText = targetRegion == focusRegionId
            ? $"N{NeuronFromAddress(targetAddress)}"
            : $"R{targetRegion}";
        return $"{sourceText} -> {targetText}";
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

    private static string GetActivityEdgeColor(double signedSignal, double intensity, bool isDormant)
    {
        var neutral = isDormant ? "#4F565D" : "#56606A";
        if (Math.Abs(signedSignal) < 1e-5)
        {
            return BlendColor(neutral, "#8EA4B8", intensity * 0.45);
        }

        var target = signedSignal > 0 ? "#E69F00" : "#0072B2";
        var blend = Math.Clamp((0.30 + (0.70 * intensity)) * Math.Abs(signedSignal), 0.0, 1.0);
        return BlendColor(neutral, target, blend);
    }

    private static string ResolveRegionFillColor(
        VizActivityCanvasColorMode colorMode,
        uint regionId,
        float signedValue,
        double loadRatio,
        double tickRecency,
        bool isDormant)
        => colorMode switch
        {
            VizActivityCanvasColorMode.Topology => GetTopologyFillColor(regionId, isDormant),
            VizActivityCanvasColorMode.Activity => GetActivityFillColor(regionId, loadRatio, tickRecency, isDormant),
            _ => GetStateFillColor(signedValue, isDormant)
        };

    private static string ResolveFocusFillColor(
        VizActivityCanvasColorMode colorMode,
        uint regionId,
        float signedValue,
        double activityRatio,
        bool isDormant)
        => colorMode switch
        {
            VizActivityCanvasColorMode.Topology => GetTopologyFillColor(regionId, isDormant),
            VizActivityCanvasColorMode.Activity => GetActivityFillColor(regionId, activityRatio, activityRatio, isDormant),
            _ => GetStateFillColor(signedValue, isDormant)
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

    private static string GetActivityFillColor(uint regionId, double loadRatio, double recency, bool isDormant)
    {
        var baseColor = GetTopologyFillColor(regionId, isDormant);
        var neutral = isDormant ? "#4E5863" : "#5B6772";
        var intensity = Math.Clamp((0.65 * loadRatio) + (0.35 * recency), 0.0, 1.0);
        return BlendColor(neutral, baseColor, 0.18 + (0.72 * intensity));
    }

    private static string GetStateFillColor(double signedValue, bool isDormant)
    {
        var clamped = Clamp(signedValue, -1.0, 1.0);
        var neutral = isDormant ? "#4E5863" : "#5E6873";
        var magnitude = Math.Abs(clamped);
        if (magnitude < 1e-5)
        {
            return neutral;
        }

        var target = clamped >= 0 ? "#E69F00" : "#0072B2";
        var minBlend = isDormant ? 0.22 : 0.38;
        var maxBlend = isDormant ? 0.60 : 0.88;
        return BlendColor(neutral, target, minBlend + ((maxBlend - minBlend) * magnitude));
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

    private static CanvasEdgeCurve BuildEdgeCurve(
        CanvasPoint source,
        CanvasPoint target,
        bool isSelfLoop,
        int curveDirection,
        double sourceRadius,
        double targetRadius)
    {
        var minX = EdgeControlPadding;
        var maxX = CanvasWidth - EdgeControlPadding;
        var minY = EdgeControlPadding;
        var maxY = CanvasHeight - EdgeControlPadding;
        sourceRadius = Math.Max(0.0, sourceRadius);
        targetRadius = Math.Max(0.0, targetRadius);
        var normalizedDirection = curveDirection < 0 ? -1 : 1;
        var cacheKey = new CanvasEdgeCurveKey(
            QuantizeCurveCoord(source.X),
            QuantizeCurveCoord(source.Y),
            QuantizeCurveCoord(target.X),
            QuantizeCurveCoord(target.Y),
            isSelfLoop,
            normalizedDirection,
            QuantizeCurveCoord(sourceRadius),
            QuantizeCurveCoord(targetRadius));
        lock (EdgeCurveCacheGate)
        {
            if (EdgeCurveCache.TryGetValue(cacheKey, out var cached))
            {
                return cached;
            }
        }

        CanvasEdgeCurve curve;
        if (isSelfLoop)
        {
            var radialOffset = Math.Max(10.0, sourceRadius * 1.02);
            var tangentialOffset = Math.Max(7.0, sourceRadius * 0.38);
            var apexOffset = Math.Max(16.0, sourceRadius * 1.58);
            var start = new CanvasPoint(
                Clamp(source.X + radialOffset, minX, maxX),
                Clamp(source.Y - tangentialOffset, minY, maxY));
            var control = new CanvasPoint(
                Clamp(source.X + apexOffset, minX, maxX),
                Clamp(source.Y - apexOffset, minY, maxY));
            var end = new CanvasPoint(
                Clamp(source.X + (radialOffset * 0.46), minX, maxX),
                Clamp(source.Y - (radialOffset * 1.04), minY, maxY));
            var pathData = FormattableString.Invariant($"M {start.X:0.###} {start.Y:0.###} Q {control.X:0.###} {control.Y:0.###} {end.X:0.###} {end.Y:0.###}");
            curve = new CanvasEdgeCurve(pathData, start, control, end);
        }
        else
        {
            var deltaX = target.X - source.X;
            var deltaY = target.Y - source.Y;
            var length = Math.Sqrt((deltaX * deltaX) + (deltaY * deltaY));
            if (length < 1e-4)
            {
                length = 1e-4;
            }

            var unitX = deltaX / length;
            var unitY = deltaY / length;
            var startOffset = sourceRadius + EdgeNodeClearance;
            var endOffset = targetRadius + EdgeNodeClearance;
            var startX = source.X + (unitX * startOffset);
            var startY = source.Y + (unitY * startOffset);
            var endX = target.X - (unitX * endOffset);
            var endY = target.Y - (unitY * endOffset);
            var adjustedDeltaX = endX - startX;
            var adjustedDeltaY = endY - startY;
            var adjustedLength = Math.Sqrt((adjustedDeltaX * adjustedDeltaX) + (adjustedDeltaY * adjustedDeltaY));
            if (adjustedLength < 4.0)
            {
                startX = source.X + (unitX * (sourceRadius * 0.5));
                startY = source.Y + (unitY * (sourceRadius * 0.5));
                endX = target.X - (unitX * (targetRadius * 0.5));
                endY = target.Y - (unitY * (targetRadius * 0.5));
                adjustedDeltaX = endX - startX;
                adjustedDeltaY = endY - startY;
                adjustedLength = Math.Sqrt((adjustedDeltaX * adjustedDeltaX) + (adjustedDeltaY * adjustedDeltaY));
            }

            var safeLength = Math.Max(adjustedLength, 1e-4);
            var midX = (startX + endX) / 2.0;
            var midY = (startY + endY) / 2.0;
            var normalX = -adjustedDeltaY / safeLength;
            var normalY = adjustedDeltaX / safeLength;
            var curvature = Math.Min(48.0, 16.0 + (length * 0.12)) * normalizedDirection;
            var start = new CanvasPoint(
                Clamp(startX, minX, maxX),
                Clamp(startY, minY, maxY));
            var control = new CanvasPoint(
                Clamp(midX + (normalX * curvature), minX, maxX),
                Clamp(midY + (normalY * curvature), minY, maxY));
            var end = new CanvasPoint(
                Clamp(endX, minX, maxX),
                Clamp(endY, minY, maxY));
            var pathData = FormattableString.Invariant($"M {start.X:0.###} {start.Y:0.###} Q {control.X:0.###} {control.Y:0.###} {end.X:0.###} {end.Y:0.###}");
            curve = new CanvasEdgeCurve(pathData, start, control, end);
        }

        lock (EdgeCurveCacheGate)
        {
            if (!EdgeCurveCache.ContainsKey(cacheKey))
            {
                if (EdgeCurveCache.Count >= EdgeCurveCacheMaxEntries && EdgeCurveCacheOrder.Count > 0)
                {
                    var evicted = EdgeCurveCacheOrder.Dequeue();
                    EdgeCurveCache.Remove(evicted);
                }

                EdgeCurveCache[cacheKey] = curve;
                EdgeCurveCacheOrder.Enqueue(cacheKey);
                return curve;
            }

            return EdgeCurveCache[cacheKey];
        }
    }

    private static int QuantizeCurveCoord(double value)
        => (int)Math.Round(value * 1000.0, MidpointRounding.AwayFromZero);

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

    private readonly record struct CanvasEdgeCurve(string PathData, CanvasPoint Start, CanvasPoint Control, CanvasPoint End);

    private readonly record struct CanvasEdgeCurveKey(
        int SourceX,
        int SourceY,
        int TargetX,
        int TargetY,
        bool IsSelfLoop,
        int CurveDirection,
        int SourceRadius,
        int TargetRadius);

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

    private readonly record struct FocusRouteAggregate(
        int EventCount,
        ulong LastTick,
        float AverageMagnitude,
        float AverageStrength,
        float SignedValue,
        float SignedStrength,
        int RouteCount)
    {
        public static FocusRouteAggregate Empty { get; } = new(0, 0, 0f, 0f, 0f, 0f, 0);

        public FocusRouteAggregate Merge(ulong tickId, float value, float strength)
        {
            var nextCount = EventCount + 1;
            var weight = nextCount == 0 ? 0f : 1f / nextCount;
            var nextMagnitude = (AverageMagnitude * (1f - weight)) + (Math.Abs(value) * weight);
            var nextStrength = (AverageStrength * (1f - weight)) + (Math.Abs(strength) * weight);
            var nextSignedValue = (SignedValue * (1f - weight)) + (value * weight);
            var nextSignedStrength = (SignedStrength * (1f - weight)) + (strength * weight);
            return new FocusRouteAggregate(nextCount, Math.Max(LastTick, tickId), nextMagnitude, nextStrength, nextSignedValue, nextSignedStrength, Math.Max(1, RouteCount));
        }

        public FocusRouteAggregate WithRoutePresence()
            => RouteCount >= 1 ? this : new FocusRouteAggregate(EventCount, LastTick, AverageMagnitude, AverageStrength, SignedValue, SignedStrength, 1);
    }

    private readonly record struct FocusDisplayRouteKey(
        string SourceNodeKey,
        string TargetNodeKey,
        uint SourceRegionId,
        uint TargetRegionId);

    private readonly record struct FocusNeuronStat(
        int EventCount,
        ulong LastTick,
        int FiredCount,
        int OutboundCount,
        int InboundCount,
        int BufferCount,
        ulong LatestBufferTick,
        float LatestBufferValue,
        float AverageBufferValue)
    {
        public static FocusNeuronStat Empty { get; } = new(0, 0, 0, 0, 0, 0, 0, 0f, 0f);
        public float LatestValue => LatestBufferValue;
        public float AverageValue => AverageBufferValue;

        public FocusNeuronStat Merge(int eventCount, ulong lastTick)
            => new(
                EventCount + Math.Max(0, eventCount),
                Math.Max(LastTick, lastTick),
                FiredCount,
                OutboundCount,
                InboundCount,
                BufferCount,
                LatestBufferTick,
                LatestBufferValue,
                AverageBufferValue);

        public FocusNeuronStat WithOutboundCount(int eventCount, ulong lastTick)
            => new(
                EventCount + Math.Max(0, eventCount),
                Math.Max(LastTick, lastTick),
                FiredCount,
                OutboundCount + Math.Max(0, eventCount),
                InboundCount,
                BufferCount,
                LatestBufferTick,
                LatestBufferValue,
                AverageBufferValue);

        public FocusNeuronStat WithInboundCount(int eventCount, ulong lastTick)
            => new(
                EventCount + Math.Max(0, eventCount),
                Math.Max(LastTick, lastTick),
                FiredCount,
                OutboundCount,
                InboundCount + Math.Max(0, eventCount),
                BufferCount,
                LatestBufferTick,
                LatestBufferValue,
                AverageBufferValue);

        public FocusNeuronStat WithFired(ulong tickId)
            => new(
                EventCount + 1,
                Math.Max(LastTick, tickId),
                FiredCount + 1,
                OutboundCount,
                InboundCount,
                BufferCount,
                LatestBufferTick,
                LatestBufferValue,
                AverageBufferValue);

        public FocusNeuronStat WithBuffer(ulong tickId, float value)
            => WithValueSample(tickId, value, includeEventCount: true);

        public FocusNeuronStat WithValueSample(ulong tickId, float value, bool includeEventCount)
        {
            var nextBufferCount = BufferCount + 1;
            var weight = nextBufferCount == 0 ? 0f : 1f / nextBufferCount;
            var nextAverageBuffer = (AverageBufferValue * (1f - weight)) + (value * weight);
            if (tickId >= LatestBufferTick)
            {
                return new FocusNeuronStat(
                    EventCount + (includeEventCount ? 1 : 0),
                    Math.Max(LastTick, tickId),
                    FiredCount,
                    OutboundCount,
                    InboundCount,
                    nextBufferCount,
                    tickId,
                    value,
                    nextAverageBuffer);
            }

            return new FocusNeuronStat(
                EventCount + (includeEventCount ? 1 : 0),
                Math.Max(LastTick, tickId),
                FiredCount,
                OutboundCount,
                InboundCount,
                nextBufferCount,
                LatestBufferTick,
                LatestBufferValue,
                nextAverageBuffer);
        }
    }

    private readonly record struct GatewayStat(bool HasInbound, bool HasOutbound, int EventCount, ulong LastTick)
    {
        public GatewayStat WithInbound(int eventCount, ulong lastTick)
            => new(true, HasOutbound, EventCount + Math.Max(0, eventCount), Math.Max(LastTick, lastTick));

        public GatewayStat WithOutbound(int eventCount, ulong lastTick)
            => new(HasInbound, true, EventCount + Math.Max(0, eventCount), Math.Max(LastTick, lastTick));
    }
}
