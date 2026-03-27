using System;
using System.Collections.Generic;
using System.Linq;
using Nbn.Tools.Workbench.Models;

namespace Nbn.Tools.Workbench.ViewModels;

public static partial class VizActivityCanvasLayoutBuilder
{
    private static VizActivityCanvasLayout BuildFocusedNeuronCanvas(
        VizActivityProjection projection,
        VizActivityProjectionOptions options,
        VizActivityCanvasTopology topology,
        VizActivityCanvasInteractionState interaction,
        ulong latestTick,
        uint focusRegionId,
        VizActivityCanvasColorMode colorMode,
        VizActivityCanvasRenderOptions renderOptions)
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
        var visibleNeuronMaxOrbit = Math.Min(
            CenterX - (CanvasPadding + MaxFocusNeuronNodeRadius + 6.0),
            (CenterY - (CanvasPadding + MaxFocusNeuronNodeRadius + 6.0)) / 0.94);
        var overflowOrbitBoost = Math.Clamp((sortedNeurons.Count - 16) * 4.5, 0.0, 260.0);
        var neuronMaxOrbit = visibleNeuronMaxOrbit + overflowOrbitBoost;
        var neuronMinOrbit = Math.Min(64.0, visibleNeuronMaxOrbit);
        var neuronPositions = BuildConcentricPositions(
            sortedNeurons.Count,
            minRadius: neuronMinOrbit,
            maxRadius: neuronMaxOrbit,
            yScale: 0.94,
            minCenterSpacing: (MaxFocusNeuronNodeRadius * 2.0) + 2.0,
            ringGapPadding: 34.0,
            minRingGap: 56.0,
            clampToCanvas: false);
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
            var tickRecency = isDormant ? 0.0 : TickRecency(stats.LastTick, latestTick, options.TickWindow);
            var radius = MinFocusNeuronNodeRadius
                         + ((MaxFocusNeuronNodeRadius - MinFocusNeuronNodeRadius) * ((structureRatio * 0.75) + (loadRatio * 0.25)));
            var emphasis = (isSelected ? 0.24 : 0.0) + (isHovered ? 0.12 : 0.0) + (isPinned ? 0.16 : 0.0);
            var fillOpacity = Math.Clamp((isDormant ? 0.15 : 0.34 + (0.45 * loadRatio)) + emphasis, 0.12, 1.0);
            var pulseOpacity = Math.Clamp((isDormant ? 0.1 : 0.28 + (0.35 * tickRecency)) + (emphasis * 0.6), 0.08, 1.0);
            var strokeThickness = 1.2 + (isPinned ? 0.8 : 0.0) + (isHovered ? 0.6 : 0.0) + (isSelected ? 0.9 : 0.0);
            var stroke = GetTopologyStrokeColor(focusRegionId, isDormant);
            var reserveValue = stats.BufferCount > 0 ? stats.LatestBufferValue : stats.AverageValue;
            var stateSignal = stats.BufferCount > 0 ? stats.LatestValue : stats.AverageValue;
            var fill = ResolveFocusFillColor(
                colorMode,
                focusRegionId,
                stateSignal,
                reserveValue,
                stats.BufferCount,
                loadRatio,
                tickRecency,
                structureRatio,
                isDormant,
                renderOptions.ColorCurve);
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
        var gatewayMinOrbit = neuronMaxOrbit + MaxFocusNeuronNodeRadius + MaxGatewayNodeRadius + 34.0;
        var gatewayMaxOrbit = gatewayMinOrbit + 56.0;
        var gatewayPositions = BuildFocusedGatewayPositions(
            sortedGateways,
            focusRegionId,
            gatewayMinOrbit,
            gatewayMaxOrbit,
            yScale: 0.9,
            clampToCanvas: false);
        var maxGatewayRouteDegree = Math.Max(
            1,
            sortedGateways.Count == 0
                ? 1
                : sortedGateways.Max(regionId =>
                {
                    regionRouteDegrees.TryGetValue(regionId, out var degree);
                    return degree.OutboundCount + degree.InboundCount;
                }));
        var maxGatewayAggregateEvents = Math.Max(
            1,
            sortedGateways.Count == 0
                ? 1
                : sortedGateways.Max(regionId =>
                {
                    regionAggregates.TryGetValue(regionId, out var aggregate);
                    return aggregate?.EventCount ?? 0;
                }));
        for (var index = 0; index < sortedGateways.Count; index++)
        {
            var regionId = sortedGateways[index];
            var stats = gatewayStats[regionId];
            var nodeKey = NodeKeyForGateway(regionId);
            if (!gatewayPositions.TryGetValue(regionId, out var position))
            {
                continue;
            }

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
            var aggregateLoadRatio = aggregateDormant ? 0.0 : Math.Clamp((double)aggregateEventCount / maxGatewayAggregateEvents, 0.0, 1.0);
            var aggregateRecency = aggregateDormant ? 0.0 : TickRecency(aggregateLastTick, latestTick, options.TickWindow);
            var radius = MinGatewayNodeRadius
                         + ((MaxGatewayNodeRadius - MinGatewayNodeRadius) * ((structureRatio * 0.8) + (activityRatio * 0.2)));
            var reserveValue = bufferMetrics.BufferCount > 0 ? bufferMetrics.LatestBufferValue : aggregateAverageSigned;
            var stateSignal = bufferMetrics.BufferCount > 0 ? reserveValue : aggregateAverageSigned;
            var fill = ResolveFocusFillColor(
                colorMode,
                regionId,
                stateSignal,
                reserveValue,
                bufferMetrics.BufferCount,
                aggregateLoadRatio,
                aggregateRecency,
                structureRatio,
                isDormant: aggregateDormant,
                curve: renderOptions.ColorCurve);
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

        var edgeBuild = BuildFocusedEdges(routes, nodeByKey, focusRegionId, options.TickWindow, latestTick, interaction, renderOptions);
        var edges = edgeBuild.Edges;
        var gatewayCount = sortedGateways.Count;
        var lodLegend = edgeBuild.LodApplied
            ? $" | LOD routes {edges.Count}/{edgeBuild.TotalDisplayRoutes} (budget {edgeBuild.RouteBudgetUsed})"
            : string.Empty;
        var layoutLegend = renderOptions.LayoutMode == VizActivityCanvasLayoutMode.Axial3DExperimental
            ? " | 3D fallback to 2D in focus mode"
            : string.Empty;
        var legend = $"Focus R{focusRegionId} | Neurons {sortedNeurons.Count} | Gateways {gatewayCount} | Routes {edges.Count} | Latest tick {latestTick}{lodLegend}{layoutLegend}";
        return new VizActivityCanvasLayout(CanvasWidth, CanvasHeight, legend, nodes, edges);
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

    private static FocusedEdgeBuildResult BuildFocusedEdges(
        IReadOnlyDictionary<VizActivityCanvasNeuronRoute, FocusRouteAggregate> routes,
        IReadOnlyDictionary<string, VizActivityCanvasNode> nodeByKey,
        uint focusRegionId,
        int tickWindow,
        ulong latestTick,
        VizActivityCanvasInteractionState interaction,
        VizActivityCanvasRenderOptions renderOptions)
    {
        if (routes.Count == 0)
        {
            return FocusedEdgeBuildResult.Empty;
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
            return FocusedEdgeBuildResult.Empty;
        }

        var totalDisplayRoutes = displayRoutes.Count;
        var routeBudget = totalDisplayRoutes;
        var lodApplied = false;
        var lodOptions = renderOptions.Lod;
        if (lodOptions.Enabled && totalDisplayRoutes > 0)
        {
            routeBudget = ResolveAdaptiveRouteBudget(
                lodOptions,
                renderOptions.ViewportScale,
                nodeByKey.Count,
                totalDisplayRoutes);
            if (routeBudget < totalDisplayRoutes)
            {
                displayRoutes = ApplyFocusedRouteLod(displayRoutes, interaction, focusRegionId, latestTick, routeBudget);
                lodApplied = displayRoutes.Count < totalDisplayRoutes;
            }
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
            var opacity = Math.Clamp((isDormant ? 0.29 : 0.34 + (0.44 * recency)) + emphasis, 0.24, 1.0);
            var activityIntensity = isDormant
                ? 0.0
                : Math.Clamp((0.45 * normalizedLoad) + (0.25 * recency) + (0.30 * Math.Abs(signedSignal)), 0.0, 1.0);
            var activityOpacity = Math.Clamp((isDormant ? 0.21 : 0.28 + (0.60 * activityIntensity)) + (emphasis * 0.5), 0.18, 1.0);
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
                true,
                aggregate.LastTick,
                aggregate.EventCount,
                key.SourceRegionId,
                key.TargetRegionId,
                isSelected,
                isHovered,
                isPinned));
        }

        var orderedEdges = edges
            .OrderByDescending(item => item.IsSelected)
            .ThenByDescending(item => item.IsHovered)
            .ThenByDescending(item => item.IsPinned)
            .ThenByDescending(item => item.EventCount)
            .ThenByDescending(item => item.LastTick)
            .ThenBy(item => item.RouteLabel, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return new FocusedEdgeBuildResult(
            orderedEdges,
            totalDisplayRoutes,
            routeBudget,
            lodApplied);
    }

    private static int ResolveAdaptiveRouteBudget(
        VizActivityCanvasLodOptions options,
        double viewportScale,
        int nodeCount,
        int routeCount)
    {
        if (!options.Enabled)
        {
            return routeCount;
        }

        var safeScale = double.IsFinite(viewportScale) && viewportScale > 0.0
            ? viewportScale
            : 1.0;
        var baseBudget = safeScale < 0.9
            ? options.LowZoomRouteBudget
            : safeScale < 1.8
                ? options.MediumZoomRouteBudget
                : options.HighZoomRouteBudget;
        baseBudget = Math.Clamp(baseBudget, MinAdaptiveRouteBudget, MaxAdaptiveRouteBudget);

        var safeNodeCount = Math.Max(1, nodeCount);
        var density = routeCount / (double)safeNodeCount;
        var densityScale = density switch
        {
            >= 8.0 => 0.55,
            >= 5.0 => 0.68,
            >= 3.0 => 0.82,
            _ => 1.0
        };
        var scaledBudget = (int)Math.Round(baseBudget * densityScale, MidpointRounding.AwayFromZero);
        return Math.Clamp(scaledBudget, MinAdaptiveRouteBudget, MaxAdaptiveRouteBudget);
    }

    private static Dictionary<FocusDisplayRouteKey, FocusRouteAggregate> ApplyFocusedRouteLod(
        IReadOnlyDictionary<FocusDisplayRouteKey, FocusRouteAggregate> displayRoutes,
        VizActivityCanvasInteractionState interaction,
        uint focusRegionId,
        ulong latestTick,
        int routeBudget)
    {
        if (displayRoutes.Count <= routeBudget)
        {
            return new Dictionary<FocusDisplayRouteKey, FocusRouteAggregate>(displayRoutes);
        }

        var candidates = new List<FocusRouteLodCandidate>(displayRoutes.Count);
        foreach (var (key, aggregate) in displayRoutes)
        {
            var routeLabel = BuildFocusedDisplayRouteLabel(key.SourceNodeKey, key.TargetNodeKey, focusRegionId);
            var isSelected = interaction.IsSelectedRoute(routeLabel);
            var isHovered = interaction.IsHoveredRoute(routeLabel);
            var isPinned = interaction.IsRoutePinned(routeLabel);
            var recency = aggregate.LastTick == 0 ? 0.0 : TickRecency(aggregate.LastTick, latestTick, tickWindow: 512);
            var score = (isSelected ? 2_000_000.0 : 0.0)
                        + (isHovered ? 1_100_000.0 : 0.0)
                        + (isPinned ? 850_000.0 : 0.0)
                        + (aggregate.EventCount * 2_100.0)
                        + (aggregate.RouteCount * 280.0)
                        + (Math.Abs(aggregate.AverageMagnitude) * 180.0)
                        + (Math.Abs(aggregate.AverageStrength) * 180.0)
                        + (recency * 1_400.0);
            candidates.Add(new FocusRouteLodCandidate(key, aggregate, isSelected, isHovered, isPinned, score));
        }

        var ordered = candidates
            .OrderByDescending(item => item.IsSelected)
            .ThenByDescending(item => item.IsHovered)
            .ThenByDescending(item => item.IsPinned)
            .ThenByDescending(item => item.Score)
            .ToList();

        var keep = new Dictionary<FocusDisplayRouteKey, FocusRouteAggregate>();
        foreach (var item in ordered)
        {
            if (keep.Count >= routeBudget)
            {
                break;
            }

            if (!keep.ContainsKey(item.Key))
            {
                keep[item.Key] = item.Aggregate;
            }
        }

        return keep;
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

    private readonly record struct FocusedEdgeBuildResult(
        IReadOnlyList<VizActivityCanvasEdge> Edges,
        int TotalDisplayRoutes,
        int RouteBudgetUsed,
        bool LodApplied)
    {
        public static FocusedEdgeBuildResult Empty { get; } = new(
            Array.Empty<VizActivityCanvasEdge>(),
            0,
            0,
            false);
    }

    private readonly record struct FocusRouteLodCandidate(
        FocusDisplayRouteKey Key,
        FocusRouteAggregate Aggregate,
        bool IsSelected,
        bool IsHovered,
        bool IsPinned,
        double Score);

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
