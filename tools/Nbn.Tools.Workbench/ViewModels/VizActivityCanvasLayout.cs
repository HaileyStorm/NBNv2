using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Nbn.Shared;

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
    string Stroke,
    string DirectionDashArray,
    string ActivityStroke,
    double StrokeThickness,
    double ActivityStrokeThickness,
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

public static class VizActivityCanvasLayoutBuilder
{
    public const double CanvasWidth = 860;
    public const double CanvasHeight = 420;
    private const double CenterX = CanvasWidth / 2.0;
    private const double CenterY = CanvasHeight / 2.0;
    private const double CanvasPadding = 26;
    private const double MinNodeRadius = 13;
    private const double MaxNodeRadius = 30;
    private const double BaseEdgeStroke = 1.1;
    private const double MaxEdgeStrokeBoost = 2.8;

    public static VizActivityCanvasLayout Build(
        VizActivityProjection projection,
        VizActivityProjectionOptions options,
        VizActivityCanvasInteractionState? interaction = null,
        VizActivityCanvasTopology? topology = null)
    {
        interaction ??= VizActivityCanvasInteractionState.Empty;
        topology ??= VizActivityCanvasTopology.Empty;

        var latestTick = ResolveLatestTick(projection);
        if (options.FocusRegionId is uint focusRegionId)
        {
            return BuildFocusedNeuronCanvas(projection, options, topology, interaction, latestTick, focusRegionId);
        }

        return BuildRegionCanvas(projection, options, topology, interaction, latestTick);
    }

    private static VizActivityCanvasLayout BuildRegionCanvas(
        VizActivityProjection projection,
        VizActivityProjectionOptions options,
        VizActivityCanvasTopology topology,
        VizActivityCanvasInteractionState interaction,
        ulong latestTick)
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
        var maxNodeEvents = Math.Max(1, nodeSource.Values.Max(item => item.EventCount));
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
            var radius = MinNodeRadius + ((MaxNodeRadius - MinNodeRadius) * loadRatio);
            var tickRecency = isDormant ? 0.0 : TickRecency(stats.LastTick, latestTick, options.TickWindow);
            var emphasis = (isSelected ? 0.25 : 0.0) + (isHovered ? 0.14 : 0.0) + (isPinned ? 0.18 : 0.0);
            var (fill, stroke) = GetRegionNodePalette(regionId, isDormant);
            var fillOpacity = Math.Clamp((isDormant ? 0.14 : 0.36 + (0.45 * loadRatio)) + emphasis, 0.12, 1.0);
            var pulseOpacity = Math.Clamp((isDormant ? 0.1 : 0.25 + (0.45 * tickRecency)) + (emphasis * 0.6), 0.08, 1.0);
            var strokeThickness = 1.3
                                  + (isPinned ? 0.8 : 0.0)
                                  + (isHovered ? 0.6 : 0.0)
                                  + (isSelected ? 0.9 : 0.0);
            var detail = $"R{regionId} | events {stats.EventCount} | last tick {stats.LastTick} | fired {stats.FiredCount} | axon {stats.AxonCount}";
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
        uint focusRegionId)
    {
        var routes = BuildFocusRoutes(projection, topology, focusRegionId);
        var focusNeuronStats = BuildFocusNeuronStats(routes, topology, focusRegionId);
        var gatewayStats = BuildGatewayStats(routes, focusRegionId);

        var nodes = new List<VizActivityCanvasNode>();
        var nodeByKey = new Dictionary<string, VizActivityCanvasNode>(StringComparer.OrdinalIgnoreCase);

        var sortedNeurons = focusNeuronStats.Keys.OrderBy(NeuronFromAddress).ToList();
        var neuronPositions = BuildCircularPositions(sortedNeurons.Count, 92, 162, yScale: 0.78);
        var maxNeuronEvents = Math.Max(1, focusNeuronStats.Values.Max(item => item.EventCount));
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
            var radius = 10.5 + (8.0 * loadRatio);
            var emphasis = (isSelected ? 0.24 : 0.0) + (isHovered ? 0.12 : 0.0) + (isPinned ? 0.16 : 0.0);
            var fillOpacity = Math.Clamp((isDormant ? 0.15 : 0.34 + (0.45 * loadRatio)) + emphasis, 0.12, 1.0);
            var pulseOpacity = Math.Clamp((isDormant ? 0.1 : 0.28 + (0.35 * TickRecency(stats.LastTick, latestTick, options.TickWindow))) + (emphasis * 0.6), 0.08, 1.0);
            var strokeThickness = 1.2 + (isPinned ? 0.8 : 0.0) + (isHovered ? 0.6 : 0.0) + (isSelected ? 0.9 : 0.0);
            var (fill, stroke) = isDormant ? ("#2E3F46", "#4E6A73") : ("#2A9D8F", "#1B6B63");
            var detail = $"R{focusRegionId}N{neuronId} | events {stats.EventCount} | last tick {stats.LastTick}";
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
            var (fill, stroke) = GetGatewayPalette(role);
            var detail = $"R{regionId} gateway | {role} | events {stats.EventCount} | last tick {stats.LastTick}";
            if (stats.EventCount == 0)
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
                position.X - 13,
                position.Y - 13,
                26,
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
            item => new RegionNodeSource(item.RegionId, item.EventCount, item.LastTick, item.FiredCount, item.AxonCount));

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
                edge.AverageSignedStrength);
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
            var sourceCenter = new CanvasPoint(sourceNode.Left + (sourceNode.Diameter / 2.0), sourceNode.Top + (sourceNode.Diameter / 2.0));
            var targetCenter = new CanvasPoint(targetNode.Left + (targetNode.Diameter / 2.0), targetNode.Top + (targetNode.Diameter / 2.0));
            var curveDirection = hasReverse && route.SourceRegionId > route.TargetRegionId ? -1 : 1;
            var curve = BuildEdgeCurve(sourceCenter, targetCenter, route.SourceRegionId == route.TargetRegionId, curveDirection);
            var detail = $"{routeLabel} | {edgeKind} | events {aggregate.EventCount} | last tick {aggregate.LastTick} | avg |v| {aggregate.AverageMagnitude:0.###} | avg v {aggregate.SignedValue:0.###}";
            if (isDormant)
            {
                detail = $"{detail} | inactive in window";
            }

            var directionStroke = GetRegionEdgeDirectionColor(edgeKind, isDormant);
            var activityStroke = GetActivityEdgeColor(signedSignal, activityIntensity, isDormant);
            edges.Add(new VizActivityCanvasEdge(
                routeLabel,
                detail,
                curve,
                directionStroke,
                directionDashArray,
                activityStroke,
                thickness,
                activityThickness,
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
                routes[route] = FocusRouteAggregate.Empty;
            }
        }

        return routes;
    }

    private static Dictionary<uint, FocusNeuronStat> BuildFocusNeuronStats(
        IReadOnlyDictionary<VizActivityCanvasNeuronRoute, FocusRouteAggregate> routes,
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
                byNeuronAddress[route.SourceAddress] = existing.Merge(aggregate.EventCount, aggregate.LastTick);
            }

            if (RegionFromAddress(route.TargetAddress) == focusRegionId)
            {
                byNeuronAddress.TryGetValue(route.TargetAddress, out var existing);
                byNeuronAddress[route.TargetAddress] = existing.Merge(aggregate.EventCount, aggregate.LastTick);
            }
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

        var routeSet = new HashSet<VizActivityCanvasNeuronRoute>(routes.Keys);
        var maxEdgeEvents = Math.Max(1, routes.Values.Max(item => item.EventCount));
        var edges = new List<VizActivityCanvasEdge>(routes.Count);

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

            var reverse = new VizActivityCanvasNeuronRoute(route.TargetAddress, route.SourceAddress);
            var hasReverse = routeSet.Contains(reverse) && reverse != route;
            var kind = GetFocusedEdgeKind(sourceRegion, targetRegion, focusRegionId, hasReverse);
            var routeLabel = BuildFocusedRouteLabel(route.SourceAddress, route.TargetAddress, focusRegionId);
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
            var sourceCenter = new CanvasPoint(sourceNode.Left + (sourceNode.Diameter / 2.0), sourceNode.Top + (sourceNode.Diameter / 2.0));
            var targetCenter = new CanvasPoint(targetNode.Left + (targetNode.Diameter / 2.0), targetNode.Top + (targetNode.Diameter / 2.0));
            var curveDirection = hasReverse && string.CompareOrdinal(sourceKey, targetKey) > 0 ? -1 : 1;
            var curve = BuildEdgeCurve(sourceCenter, targetCenter, sourceKey == targetKey, curveDirection);
            var detail = $"{routeLabel} | {kind} | events {aggregate.EventCount} | last tick {aggregate.LastTick} | avg |v| {aggregate.AverageMagnitude:0.###} | avg v {aggregate.SignedValue:0.###}";
            if (isDormant)
            {
                detail = $"{detail} | inactive in window";
            }

            var directionStroke = GetFocusedEdgeDirectionColor(kind, isDormant);
            var activityStroke = GetActivityEdgeColor(signedSignal, activityIntensity, isDormant);
            edges.Add(new VizActivityCanvasEdge(
                routeLabel,
                detail,
                curve,
                directionStroke,
                directionDashArray,
                activityStroke,
                thickness,
                activityThickness,
                opacity,
                activityOpacity,
                true,
                aggregate.LastTick,
                aggregate.EventCount,
                sourceRegion,
                targetRegion,
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
            return BlendColor(neutral, "#B8C46A", intensity * 0.6);
        }

        var target = signedSignal > 0 ? "#2ECC71" : "#E74C3C";
        var blend = Math.Clamp((0.30 + (0.70 * intensity)) * Math.Abs(signedSignal), 0.0, 1.0);
        return BlendColor(neutral, target, blend);
    }

    private static (string Fill, string Stroke) GetRegionNodePalette(uint regionId, bool isDormant)
    {
        var color = GetSliceColor(GetRegionSlice(regionId));
        return isDormant
            ? (DimColor(color), "#5A6670")
            : (color, DarkenColor(color));
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
            -3 => "#457B9D",
            -2 => "#4F9D69",
            -1 => "#7AA37A",
            0 => "#2A9D8F",
            1 => "#E9C46A",
            2 => "#F4A261",
            3 => "#E76F51",
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
                existing.AxonCount);
            return;
        }

        byRegion[regionId] = new RegionNodeSource(regionId, Math.Max(0, eventCount), lastTick, 0, Math.Max(0, eventCount));
    }

    private static string BuildEdgeCurve(CanvasPoint source, CanvasPoint target, bool isSelfLoop, int curveDirection)
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
        var curvature = Math.Min(48.0, 16.0 + (length * 0.12)) * curveDirection;
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

    private readonly record struct RouteAggregate(
        int EventCount,
        ulong LastTick,
        float AverageMagnitude,
        float AverageStrength,
        float SignedValue,
        float SignedStrength)
    {
        public static RouteAggregate Empty { get; } = new(0, 0, 0f, 0f, 0f, 0f);

        public RouteAggregate Merge(
            int eventCount,
            ulong lastTick,
            float averageMagnitude,
            float averageStrength,
            float averageSignedValue,
            float averageSignedStrength)
        {
            var nextCount = EventCount + Math.Max(0, eventCount);
            var weight = nextCount == 0 ? 0f : (float)Math.Max(0, eventCount) / nextCount;
            var nextMagnitude = (AverageMagnitude * (1f - weight)) + (averageMagnitude * weight);
            var nextStrength = (AverageStrength * (1f - weight)) + (averageStrength * weight);
            var nextSignedValue = (SignedValue * (1f - weight)) + (averageSignedValue * weight);
            var nextSignedStrength = (SignedStrength * (1f - weight)) + (averageSignedStrength * weight);
            return new RouteAggregate(nextCount, Math.Max(LastTick, lastTick), nextMagnitude, nextStrength, nextSignedValue, nextSignedStrength);
        }
    }

    private readonly record struct FocusRouteAggregate(
        int EventCount,
        ulong LastTick,
        float AverageMagnitude,
        float AverageStrength,
        float SignedValue,
        float SignedStrength)
    {
        public static FocusRouteAggregate Empty { get; } = new(0, 0, 0f, 0f, 0f, 0f);

        public FocusRouteAggregate Merge(ulong tickId, float value, float strength)
        {
            var nextCount = EventCount + 1;
            var weight = nextCount == 0 ? 0f : 1f / nextCount;
            var nextMagnitude = (AverageMagnitude * (1f - weight)) + (Math.Abs(value) * weight);
            var nextStrength = (AverageStrength * (1f - weight)) + (Math.Abs(strength) * weight);
            var nextSignedValue = (SignedValue * (1f - weight)) + (value * weight);
            var nextSignedStrength = (SignedStrength * (1f - weight)) + (strength * weight);
            return new FocusRouteAggregate(nextCount, Math.Max(LastTick, tickId), nextMagnitude, nextStrength, nextSignedValue, nextSignedStrength);
        }
    }

    private readonly record struct FocusNeuronStat(int EventCount, ulong LastTick)
    {
        public static FocusNeuronStat Empty { get; } = new(0, 0);

        public FocusNeuronStat Merge(int eventCount, ulong lastTick)
            => new(EventCount + Math.Max(0, eventCount), Math.Max(LastTick, lastTick));
    }

    private readonly record struct GatewayStat(bool HasInbound, bool HasOutbound, int EventCount, ulong LastTick)
    {
        public GatewayStat WithInbound(int eventCount, ulong lastTick)
            => new(true, HasOutbound, EventCount + Math.Max(0, eventCount), Math.Max(LastTick, lastTick));

        public GatewayStat WithOutbound(int eventCount, ulong lastTick)
            => new(HasInbound, true, EventCount + Math.Max(0, eventCount), Math.Max(LastTick, lastTick));
    }
}
