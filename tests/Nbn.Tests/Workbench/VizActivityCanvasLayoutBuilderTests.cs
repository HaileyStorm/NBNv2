using System.Globalization;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Workbench;

public class VizActivityCanvasLayoutBuilderTests
{
    [Fact]
    public void Build_IsDeterministicForSameProjection()
    {
        var projection = BuildProjection();
        var options = new VizActivityProjectionOptions(TickWindow: 32, IncludeLowSignalEvents: true, FocusRegionId: null);

        var first = VizActivityCanvasLayoutBuilder.Build(projection, options);
        var second = VizActivityCanvasLayoutBuilder.Build(projection, options);

        Assert.Equal(first.Legend, second.Legend);
        Assert.Equal(first.Nodes.Count, second.Nodes.Count);
        Assert.Equal(first.Edges.Count, second.Edges.Count);
        Assert.Equal(first.Nodes, second.Nodes);
        Assert.Equal(first.Edges, second.Edges);
    }

    [Fact]
    public void Build_FocusModeCentersSingleNeuronWhenOnlyOneKnown()
    {
        var projection = BuildProjection();
        var options = new VizActivityProjectionOptions(TickWindow: 32, IncludeLowSignalEvents: true, FocusRegionId: 23);

        var layout = VizActivityCanvasLayoutBuilder.Build(projection, options);
        var focused = layout.Nodes.Single(item => item.RegionId == 23 && item.NeuronId.HasValue);
        var centerX = focused.Left + (focused.Diameter / 2.0);
        var centerY = focused.Top + (focused.Diameter / 2.0);

        Assert.InRange(centerX, VizActivityCanvasLayoutBuilder.CanvasWidth / 2.0 - 0.001, VizActivityCanvasLayoutBuilder.CanvasWidth / 2.0 + 0.001);
        Assert.InRange(centerY, VizActivityCanvasLayoutBuilder.CanvasHeight / 2.0 - 0.001, VizActivityCanvasLayoutBuilder.CanvasHeight / 2.0 + 0.001);
    }

    [Fact]
    public void Build_UsesTickRecencyForNodeAndEdgeVisualState()
    {
        var events = new List<VizEventItem>
        {
            CreateEvent("VizAxonSent", tick: 95, region: 1, source: Address(1, 1), target: Address(2, 2), value: 0.6f, strength: 0.2f),
            CreateEvent("VizAxonSent", tick: 110, region: 2, source: Address(2, 2), target: Address(3, 3), value: 0.8f, strength: 0.4f),
            CreateEvent("VizNeuronFired", tick: 111, region: 3, source: Address(3, 3), target: Address(3, 3), value: 1.0f, strength: 0.1f),
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 24, IncludeLowSignalEvents: true, FocusRegionId: null));

        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 24, IncludeLowSignalEvents: true, FocusRegionId: null));

        var newestNode = layout.Nodes.MaxBy(item => item.LastTick)!;
        var oldestNode = layout.Nodes.MinBy(item => item.LastTick)!;
        Assert.True(newestNode.PulseOpacity >= oldestNode.PulseOpacity);

        var newestEdge = layout.Edges.MaxBy(item => item.LastTick)!;
        var oldestEdge = layout.Edges.MinBy(item => item.LastTick)!;
        Assert.True(newestEdge.Opacity >= oldestEdge.Opacity);
    }

    [Fact]
    public void Build_AppliesSelectionHoverAndPinFlags()
    {
        var projection = BuildProjection();
        var options = new VizActivityProjectionOptions(TickWindow: 32, IncludeLowSignalEvents: true, FocusRegionId: null);
        var baseline = VizActivityCanvasLayoutBuilder.Build(projection, options);
        var selectedRoute = baseline.Edges[0].RouteLabel;
        var selectedNode = baseline.Nodes.Single(item => item.RegionId == 23);
        var hoveredPinnedNode = baseline.Nodes.Single(item => item.RegionId == 31);
        var interaction = new VizActivityCanvasInteractionState(
            SelectedNodeKey: selectedNode.NodeKey,
            SelectedRouteLabel: selectedRoute,
            HoverNodeKey: hoveredPinnedNode.NodeKey,
            HoverRouteLabel: selectedRoute,
            PinnedNodeKeys: new HashSet<string>(StringComparer.OrdinalIgnoreCase) { hoveredPinnedNode.NodeKey },
            PinnedRouteLabels: new HashSet<string>(StringComparer.OrdinalIgnoreCase) { selectedRoute });

        var layout = VizActivityCanvasLayoutBuilder.Build(projection, options, interaction);
        selectedNode = layout.Nodes.Single(item => item.RegionId == 23);
        hoveredPinnedNode = layout.Nodes.Single(item => item.RegionId == 31);
        var selectedEdge = layout.Edges.Single(item => string.Equals(item.RouteLabel, selectedRoute, StringComparison.OrdinalIgnoreCase));

        Assert.True(selectedNode.IsSelected);
        Assert.True(hoveredPinnedNode.IsHovered);
        Assert.True(hoveredPinnedNode.IsPinned);
        Assert.True(selectedEdge.IsSelected);
        Assert.True(selectedEdge.IsHovered);
        Assert.True(selectedEdge.IsPinned);
    }

    [Fact]
    public void Build_BoostsVisualStrengthForSelectedEntities()
    {
        var projection = BuildProjection();
        var options = new VizActivityProjectionOptions(TickWindow: 32, IncludeLowSignalEvents: true, FocusRegionId: null);
        var baseline = VizActivityCanvasLayoutBuilder.Build(projection, options, VizActivityCanvasInteractionState.Empty);
        var selectedNode = baseline.Nodes[0];
        var selectedRoute = baseline.Edges[0].RouteLabel;
        var selected = VizActivityCanvasLayoutBuilder.Build(
            projection,
            options,
            new VizActivityCanvasInteractionState(
                SelectedNodeKey: selectedNode.NodeKey,
                SelectedRouteLabel: selectedRoute,
                HoverNodeKey: null,
                HoverRouteLabel: null,
                PinnedNodeKeys: new HashSet<string>(StringComparer.OrdinalIgnoreCase),
                PinnedRouteLabels: new HashSet<string>(StringComparer.OrdinalIgnoreCase)));

        var baselineNode = baseline.Nodes.Single(item => string.Equals(item.NodeKey, selectedNode.NodeKey, StringComparison.OrdinalIgnoreCase));
        var selectedNodeState = selected.Nodes.Single(item => string.Equals(item.NodeKey, selectedNode.NodeKey, StringComparison.OrdinalIgnoreCase));
        Assert.True(selectedNodeState.StrokeThickness > baselineNode.StrokeThickness);
        Assert.True(selectedNodeState.FillOpacity >= baselineNode.FillOpacity);

        var baselineEdge = baseline.Edges.Single(item => string.Equals(item.RouteLabel, selectedRoute, StringComparison.OrdinalIgnoreCase));
        var selectedEdgeState = selected.Edges.Single(item => string.Equals(item.RouteLabel, selectedRoute, StringComparison.OrdinalIgnoreCase));
        Assert.True(selectedEdgeState.StrokeThickness > baselineEdge.StrokeThickness);
        Assert.True(selectedEdgeState.Opacity >= baselineEdge.Opacity);
    }

    [Fact]
    public void Build_IncludesTopologyRegionsAndRoutesWhenWindowInactive()
    {
        var projection = VizActivityProjectionBuilder.Build(
            Array.Empty<VizEventItem>(),
            new VizActivityProjectionOptions(TickWindow: 32, IncludeLowSignalEvents: true, FocusRegionId: null));
        var topology = new VizActivityCanvasTopology(
            new HashSet<uint> { 0, 5, 31 },
            new HashSet<VizActivityCanvasRegionRoute> { new(0, 5) },
            new HashSet<uint>(),
            new HashSet<VizActivityCanvasNeuronRoute>());

        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 32, IncludeLowSignalEvents: true, FocusRegionId: null),
            VizActivityCanvasInteractionState.Empty,
            topology);

        Assert.Contains(layout.Nodes, node => node.RegionId == 5);
        Assert.Contains(layout.Edges, edge => string.Equals(edge.RouteLabel, "R0 -> R5", StringComparison.OrdinalIgnoreCase));
        Assert.All(layout.Edges.Where(edge => edge.EventCount == 0), edge => Assert.True(edge.Opacity < 0.4));
    }

    [Fact]
    public void Build_FocusModeShowsBidirectionalGatewayRoutes()
    {
        var events = new List<VizEventItem>
        {
            CreateEvent("VizAxonSent", tick: 200, region: 13, source: Address(13, 2), target: Address(21, 0), value: 1f, strength: 0.3f),
            CreateEvent("VizAxonSent", tick: 201, region: 21, source: Address(21, 0), target: Address(13, 2), value: 1f, strength: 0.3f)
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 32, IncludeLowSignalEvents: true, FocusRegionId: 13));
        var topology = new VizActivityCanvasTopology(
            new HashSet<uint> { 13, 21 },
            new HashSet<VizActivityCanvasRegionRoute> { new(13, 21), new(21, 13) },
            new HashSet<uint> { uint.Parse(Address(13, 2), CultureInfo.InvariantCulture), uint.Parse(Address(21, 0), CultureInfo.InvariantCulture) },
            new HashSet<VizActivityCanvasNeuronRoute>
            {
                new(uint.Parse(Address(13, 2), CultureInfo.InvariantCulture), uint.Parse(Address(21, 0), CultureInfo.InvariantCulture)),
                new(uint.Parse(Address(21, 0), CultureInfo.InvariantCulture), uint.Parse(Address(13, 2), CultureInfo.InvariantCulture))
            });

        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 32, IncludeLowSignalEvents: true, FocusRegionId: 13),
            VizActivityCanvasInteractionState.Empty,
            topology);

        Assert.Contains(layout.Nodes, node => node.Label == "N2");
        Assert.Contains(layout.Nodes, node => node.Label == "R21");
        Assert.Contains(layout.Edges, edge => edge.Detail.Contains("bidirectional", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Build_UsesSignedActivityColor_WithSeparateDirectionBorder()
    {
        var events = new List<VizEventItem>
        {
            CreateEvent("VizAxonSent", tick: 300, region: 9, source: Address(9, 1), target: Address(23, 4), value: 1.0f, strength: 0.6f),
            CreateEvent("VizAxonSent", tick: 301, region: 23, source: Address(23, 4), target: Address(9, 1), value: -1.0f, strength: -0.5f)
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 32, IncludeLowSignalEvents: true, FocusRegionId: null));
        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 32, IncludeLowSignalEvents: true, FocusRegionId: null));

        var positive = layout.Edges.Single(edge => string.Equals(edge.RouteLabel, "R9 -> R23", StringComparison.OrdinalIgnoreCase));
        var negative = layout.Edges.Single(edge => string.Equals(edge.RouteLabel, "R23 -> R9", StringComparison.OrdinalIgnoreCase));

        Assert.NotEqual(positive.Stroke, positive.ActivityStroke);
        Assert.NotEqual(negative.Stroke, negative.ActivityStroke);
        Assert.NotEqual(positive.ActivityStroke, negative.ActivityStroke);
        Assert.True(positive.ActivityOpacity > 0.2);
        Assert.True(negative.ActivityOpacity > 0.2);
        Assert.True(positive.ActivityStrokeThickness < positive.StrokeThickness);
    }

    [Fact]
    public void Build_DoesNotThrow_WhenRegionNodesExistWithoutAnyRoutes()
    {
        var projection = VizActivityProjectionBuilder.Build(
            Array.Empty<VizEventItem>(),
            new VizActivityProjectionOptions(TickWindow: 32, IncludeLowSignalEvents: true, FocusRegionId: null));
        var topology = new VizActivityCanvasTopology(
            new HashSet<uint> { 0, 31 },
            new HashSet<VizActivityCanvasRegionRoute>(),
            new HashSet<uint>(),
            new HashSet<VizActivityCanvasNeuronRoute>());

        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 32, IncludeLowSignalEvents: true, FocusRegionId: null),
            VizActivityCanvasInteractionState.Empty,
            topology);

        Assert.Equal(2, layout.Nodes.Count);
        Assert.Empty(layout.Edges);
    }

    private static VizActivityProjection BuildProjection()
    {
        var events = new List<VizEventItem>
        {
            CreateEvent("VizNeuronFired", tick: 120, region: 0, source: Address(0, 0), target: Address(9, 1), value: 0.5f, strength: 0.1f),
            CreateEvent("VizAxonSent", tick: 121, region: 9, source: Address(9, 1), target: Address(23, 4), value: 0.8f, strength: 0.4f),
            CreateEvent("VizNeuronFired", tick: 122, region: 23, source: Address(23, 4), target: Address(31, 2), value: 1.1f, strength: 0.2f),
            CreateEvent("VizAxonSent", tick: 122, region: 23, source: Address(23, 4), target: Address(31, 2), value: 0.7f, strength: 0.3f),
            CreateEvent("VizNeuronBuffer", tick: 123, region: 31, source: Address(31, 2), target: Address(23, 4), value: 0.2f, strength: 0f),
        };

        return VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 32, IncludeLowSignalEvents: true, FocusRegionId: null));
    }

    private static VizEventItem CreateEvent(
        string type,
        ulong tick,
        uint region,
        string source,
        string target,
        float value,
        float strength)
    {
        return new VizEventItem(
            Time: DateTimeOffset.UnixEpoch.AddMilliseconds(tick),
            Type: type,
            BrainId: Guid.Empty.ToString("D"),
            TickId: tick,
            Region: region.ToString(CultureInfo.InvariantCulture),
            Source: source,
            Target: target,
            Value: value,
            Strength: strength,
            EventId: Guid.NewGuid().ToString("D"));
    }

    private static string Address(uint regionId, uint neuronId)
    {
        var value = (regionId << NbnConstants.AddressNeuronBits) | (neuronId & NbnConstants.AddressNeuronMask);
        return value.ToString(CultureInfo.InvariantCulture);
    }
}
