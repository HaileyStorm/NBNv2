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
        Assert.All(
            first.Edges.Zip(second.Edges, (left, right) => (left, right)),
            pair => Assert.Same(pair.left.PathData, pair.right.PathData));
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
        Assert.True(positive.HitTestThickness > positive.StrokeThickness);
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

    [Fact]
    public void Build_FocusModeUsesNeuronFiredSourceForNodeActivityWithoutAxonRoutes()
    {
        const uint focusRegionId = 24;
        var source = Address(focusRegionId, 7);
        var projection = VizActivityProjectionBuilder.Build(
            new[]
            {
                CreateEvent("VizNeuronFired", tick: 500, region: focusRegionId, source: source, target: string.Empty, value: 1.2f, strength: 0f)
            },
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId));

        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId),
            VizActivityCanvasInteractionState.Empty,
            VizActivityCanvasTopology.Empty);

        var focusedNeuron = Assert.Single(layout.Nodes, node => node.RegionId == focusRegionId && node.NeuronId == 7);
        Assert.True(focusedNeuron.EventCount > 0);
        Assert.Contains("value n=1", focusedNeuron.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("avg=1.2", focusedNeuron.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("latest=1.2@500", focusedNeuron.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Empty(layout.Edges);
    }

    [Fact]
    public void Build_FocusModeDoesNotTreatNeuronFiredEventsAsAxonRoutes()
    {
        const uint focusRegionId = 0;
        var sourceAddress = uint.Parse(Address(focusRegionId, 1), CultureInfo.InvariantCulture);
        var targetAddress = uint.Parse(Address(focusRegionId, 2), CultureInfo.InvariantCulture);
        var topology = new VizActivityCanvasTopology(
            new HashSet<uint> { focusRegionId },
            new HashSet<VizActivityCanvasRegionRoute>(),
            new HashSet<uint> { sourceAddress, targetAddress },
            new HashSet<VizActivityCanvasNeuronRoute> { new(sourceAddress, targetAddress) });
        var projection = VizActivityProjectionBuilder.Build(
            new[]
            {
                CreateEvent("VizNeuronFired", tick: 700, region: focusRegionId, source: Address(focusRegionId, 1), target: Address(focusRegionId, 2), value: 1f, strength: 0f)
            },
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId));

        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId),
            VizActivityCanvasInteractionState.Empty,
            topology);

        var edge = Assert.Single(layout.Edges);
        Assert.Equal(0, edge.EventCount);
    }

    [Fact]
    public void Build_FocusModeHandlesDenseRegionWithoutThrowing()
    {
        const uint focusRegionId = 0;
        const int neuronCount = 24;
        const int routeCount = 369;
        var events = new List<VizEventItem>(routeCount);
        var neuronRoutes = new HashSet<VizActivityCanvasNeuronRoute>();
        var neuronAddresses = new HashSet<uint>();

        for (var neuron = 0; neuron < neuronCount; neuron++)
        {
            neuronAddresses.Add(uint.Parse(Address(focusRegionId, (uint)neuron), CultureInfo.InvariantCulture));
        }

        for (var i = 0; i < routeCount; i++)
        {
            var src = (uint)(i % neuronCount);
            var dst = (uint)((i * 7 + 3) % neuronCount);
            var source = Address(focusRegionId, src);
            var target = Address(focusRegionId, dst);
            var value = (i % 2 == 0) ? 0.8f : -0.6f;
            var strength = (i % 3 == 0) ? 0.4f : -0.3f;
            events.Add(CreateEvent("VizAxonSent", (ulong)(2000 + i), focusRegionId, source, target, value, strength));
            neuronRoutes.Add(new(
                uint.Parse(source, CultureInfo.InvariantCulture),
                uint.Parse(target, CultureInfo.InvariantCulture)));
        }

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId));
        var topology = new VizActivityCanvasTopology(
            new HashSet<uint> { focusRegionId },
            new HashSet<VizActivityCanvasRegionRoute>(),
            neuronAddresses,
            neuronRoutes);

        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId),
            VizActivityCanvasInteractionState.Empty,
            topology);

        Assert.Equal(neuronCount, layout.Nodes.Count(item => item.RegionId == focusRegionId && item.NeuronId.HasValue));
        Assert.True(layout.Edges.Count > 0);
    }

    [Fact]
    public void Build_FocusModeDenseRegionAvoidsNeuronOverlap()
    {
        const uint focusRegionId = 0;
        const int neuronCount = 48;
        const int routeCount = 560;
        var events = new List<VizEventItem>(routeCount);
        var neuronRoutes = new HashSet<VizActivityCanvasNeuronRoute>();
        var neuronAddresses = new HashSet<uint>();

        for (var neuron = 0; neuron < neuronCount; neuron++)
        {
            neuronAddresses.Add(uint.Parse(Address(focusRegionId, (uint)neuron), CultureInfo.InvariantCulture));
        }

        for (var i = 0; i < routeCount; i++)
        {
            var src = (uint)(i % neuronCount);
            var dst = (uint)((i * 11 + 5) % neuronCount);
            var source = Address(focusRegionId, src);
            var target = Address(focusRegionId, dst);
            var value = (i % 2 == 0) ? 0.9f : -0.7f;
            var strength = (i % 3 == 0) ? 0.5f : -0.35f;
            events.Add(CreateEvent("VizAxonSent", (ulong)(4200 + i), focusRegionId, source, target, value, strength));
            neuronRoutes.Add(new(
                uint.Parse(source, CultureInfo.InvariantCulture),
                uint.Parse(target, CultureInfo.InvariantCulture)));
        }

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 96, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId));
        var topology = new VizActivityCanvasTopology(
            new HashSet<uint> { focusRegionId },
            new HashSet<VizActivityCanvasRegionRoute>(),
            neuronAddresses,
            neuronRoutes);

        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 96, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId),
            VizActivityCanvasInteractionState.Empty,
            topology);

        var neurons = layout.Nodes
            .Where(item => item.RegionId == focusRegionId && item.NeuronId.HasValue)
            .OrderBy(item => item.NeuronId!.Value)
            .ToList();
        Assert.Equal(neuronCount, neurons.Count);

        for (var left = 0; left < neurons.Count; left++)
        {
            var leftNode = neurons[left];
            var leftCenterX = leftNode.Left + (leftNode.Diameter / 2.0);
            var leftCenterY = leftNode.Top + (leftNode.Diameter / 2.0);
            for (var right = left + 1; right < neurons.Count; right++)
            {
                var rightNode = neurons[right];
                var rightCenterX = rightNode.Left + (rightNode.Diameter / 2.0);
                var rightCenterY = rightNode.Top + (rightNode.Diameter / 2.0);
                var minimumDistance = ((leftNode.Diameter + rightNode.Diameter) / 2.0) - 0.2;
                var actualDistance = Distance(leftCenterX, leftCenterY, rightCenterX, rightCenterY);
                Assert.True(
                    actualDistance >= minimumDistance,
                    $"Nodes {leftNode.Label} and {rightNode.Label} overlap. actual={actualDistance:0.###}, required={minimumDistance:0.###}");
            }
        }
    }

    [Fact]
    public void Build_FocusModeDenseRegionUsesWiderRadialLayerSpacingWithoutForcingFullOnscreenFit()
    {
        const uint focusRegionId = 0;
        const int neuronCount = 80;
        const int routeCount = 960;
        const double focusYScale = 0.94;
        var events = new List<VizEventItem>(routeCount);
        var neuronRoutes = new HashSet<VizActivityCanvasNeuronRoute>();
        var neuronAddresses = new HashSet<uint>();

        for (var neuron = 0; neuron < neuronCount; neuron++)
        {
            neuronAddresses.Add(uint.Parse(Address(focusRegionId, (uint)neuron), CultureInfo.InvariantCulture));
        }

        for (var i = 0; i < routeCount; i++)
        {
            var src = (uint)(i % neuronCount);
            var dst = (uint)((i * 13 + 7) % neuronCount);
            var source = Address(focusRegionId, src);
            var target = Address(focusRegionId, dst);
            var value = (i % 2 == 0) ? 0.85f : -0.75f;
            var strength = (i % 3 == 0) ? 0.55f : -0.4f;
            events.Add(CreateEvent("VizAxonSent", (ulong)(5200 + i), focusRegionId, source, target, value, strength));
            neuronRoutes.Add(new(
                uint.Parse(source, CultureInfo.InvariantCulture),
                uint.Parse(target, CultureInfo.InvariantCulture)));
        }

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 128, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId));
        var topology = new VizActivityCanvasTopology(
            new HashSet<uint> { focusRegionId },
            new HashSet<VizActivityCanvasRegionRoute>(),
            neuronAddresses,
            neuronRoutes);

        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 128, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId),
            VizActivityCanvasInteractionState.Empty,
            topology);

        var neurons = layout.Nodes
            .Where(item => item.RegionId == focusRegionId && item.NeuronId.HasValue)
            .ToList();
        Assert.Equal(neuronCount, neurons.Count);

        var centerX = VizActivityCanvasLayoutBuilder.CanvasWidth / 2.0;
        var centerY = VizActivityCanvasLayoutBuilder.CanvasHeight / 2.0;
        var normalizedRadii = neurons
            .Select(node =>
            {
                var x = node.Left + (node.Diameter / 2.0);
                var y = node.Top + (node.Diameter / 2.0);
                var dx = x - centerX;
                var dy = (y - centerY) / focusYScale;
                return Math.Sqrt((dx * dx) + (dy * dy));
            })
            .OrderBy(value => value)
            .ToList();

        var bandCenters = new List<double>();
        var bandCounts = new List<int>();
        foreach (var radius in normalizedRadii)
        {
            if (bandCenters.Count == 0 || Math.Abs(radius - bandCenters[^1]) > 16.0)
            {
                bandCenters.Add(radius);
                bandCounts.Add(1);
                continue;
            }

            var bandIndex = bandCenters.Count - 1;
            bandCounts[bandIndex]++;
            bandCenters[bandIndex] = ((bandCenters[bandIndex] * (bandCounts[bandIndex] - 1)) + radius) / bandCounts[bandIndex];
        }

        Assert.True(bandCenters.Count >= 4, $"Expected at least 4 radial bands but saw {bandCenters.Count}.");
        var minBandGap = bandCenters
            .Zip(bandCenters.Skip(1), (left, right) => right - left)
            .Min();
        Assert.True(minBandGap >= 50.0, $"Expected wider radial layer spacing; observed minimum gap {minBandGap:0.###}.");

        Assert.Contains(
            neurons,
            node => node.Left < 0
                    || node.Top < 0
                    || (node.Left + node.Diameter) > VizActivityCanvasLayoutBuilder.CanvasWidth
                    || (node.Top + node.Diameter) > VizActivityCanvasLayoutBuilder.CanvasHeight);
    }

    [Fact]
    public void Build_FocusModeGatewayNodesRemainOutsideNeuronOrbitWithoutOverlap()
    {
        const uint focusRegionId = 0;
        const int neuronCount = 41;
        const int routeCount = 385;
        var gatewayRegions = new[] { 8u, 13u, 23u, 31u };
        var events = new List<VizEventItem>(routeCount);
        var neuronRoutes = new HashSet<VizActivityCanvasNeuronRoute>();
        var neuronAddresses = new HashSet<uint>();

        for (var neuron = 0; neuron < neuronCount; neuron++)
        {
            neuronAddresses.Add(uint.Parse(Address(focusRegionId, (uint)neuron), CultureInfo.InvariantCulture));
        }

        for (var i = 0; i < routeCount; i++)
        {
            string source;
            string target;
            uint eventRegion;
            if (i % 11 == 0)
            {
                var gateway = gatewayRegions[(i / 11) % gatewayRegions.Length];
                source = Address(gateway, (uint)((i * 7 + 5) % 19));
                target = Address(focusRegionId, (uint)((i * 5 + 3) % neuronCount));
                eventRegion = gateway;
            }
            else if (i % 3 == 0)
            {
                source = Address(focusRegionId, (uint)(i % neuronCount));
                var gateway = gatewayRegions[(i / 3) % gatewayRegions.Length];
                target = Address(gateway, (uint)((i * 3 + 1) % 17));
                eventRegion = focusRegionId;
            }
            else
            {
                source = Address(focusRegionId, (uint)(i % neuronCount));
                target = Address(focusRegionId, (uint)((i * 9 + 7) % neuronCount));
                eventRegion = focusRegionId;
            }

            var sourceAddress = uint.Parse(source, CultureInfo.InvariantCulture);
            var targetAddress = uint.Parse(target, CultureInfo.InvariantCulture);
            neuronAddresses.Add(sourceAddress);
            neuronAddresses.Add(targetAddress);
            neuronRoutes.Add(new(sourceAddress, targetAddress));

            var value = (i % 2 == 0) ? 0.82f : -0.74f;
            var strength = (i % 3 == 0) ? 0.49f : -0.36f;
            events.Add(CreateEvent("VizAxonSent", (ulong)(6600 + i), eventRegion, source, target, value, strength));
        }

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 96, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId));
        var topology = new VizActivityCanvasTopology(
            new HashSet<uint>(gatewayRegions) { focusRegionId },
            new HashSet<VizActivityCanvasRegionRoute>(),
            neuronAddresses,
            neuronRoutes);

        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 96, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId),
            VizActivityCanvasInteractionState.Empty,
            topology);

        var neurons = layout.Nodes.Where(node => node.RegionId == focusRegionId && node.NeuronId.HasValue).ToList();
        var gateways = layout.Nodes.Where(node => node.RegionId != focusRegionId && !node.NeuronId.HasValue).ToList();
        Assert.Equal(neuronCount, neurons.Count);
        Assert.Equal(gatewayRegions.Length, gateways.Count);

        var centerX = VizActivityCanvasLayoutBuilder.CanvasWidth / 2.0;
        var centerY = VizActivityCanvasLayoutBuilder.CanvasHeight / 2.0;
        var maxNeuronDistanceFromCenter = neurons
            .Select(node =>
            {
                var x = node.Left + (node.Diameter / 2.0);
                var y = node.Top + (node.Diameter / 2.0);
                return Distance(x, y, centerX, centerY);
            })
            .Max();
        var minGatewayDistanceFromCenter = gateways
            .Select(node =>
            {
                var x = node.Left + (node.Diameter / 2.0);
                var y = node.Top + (node.Diameter / 2.0);
                return Distance(x, y, centerX, centerY);
            })
            .Min();
        Assert.True(
            minGatewayDistanceFromCenter > maxNeuronDistanceFromCenter + 20.0,
            $"Expected gateway orbit to remain outside neuron orbit. gatewayMin={minGatewayDistanceFromCenter:0.###}, neuronMax={maxNeuronDistanceFromCenter:0.###}");

        foreach (var gateway in gateways)
        {
            var gatewayCenterX = gateway.Left + (gateway.Diameter / 2.0);
            var gatewayCenterY = gateway.Top + (gateway.Diameter / 2.0);
            foreach (var neuron in neurons)
            {
                var neuronCenterX = neuron.Left + (neuron.Diameter / 2.0);
                var neuronCenterY = neuron.Top + (neuron.Diameter / 2.0);
                var minimumDistance = ((gateway.Diameter + neuron.Diameter) / 2.0) - 0.2;
                var actualDistance = Distance(gatewayCenterX, gatewayCenterY, neuronCenterX, neuronCenterY);
                Assert.True(
                    actualDistance >= minimumDistance,
                    $"Gateway {gateway.Label} overlaps neuron {neuron.Label}. actual={actualDistance:0.###}, required={minimumDistance:0.###}");
            }
        }

        Assert.Contains(
            layout.Nodes,
            node => node.Left < 0
                    || node.Top < 0
                    || (node.Left + node.Diameter) > VizActivityCanvasLayoutBuilder.CanvasWidth
                    || (node.Top + node.Diameter) > VizActivityCanvasLayoutBuilder.CanvasHeight);
    }

    [Fact]
    public void Build_FocusModeCollapsesGatewayFanoutIntoSingleDisplayedRoute()
    {
        const uint focusRegionId = 0;
        var topology = new VizActivityCanvasTopology(
            new HashSet<uint> { focusRegionId, 8 },
            new HashSet<VizActivityCanvasRegionRoute>(),
            new HashSet<uint>
            {
                uint.Parse(Address(focusRegionId, 0), CultureInfo.InvariantCulture)
            },
            new HashSet<VizActivityCanvasNeuronRoute>
            {
                new(uint.Parse(Address(focusRegionId, 0), CultureInfo.InvariantCulture), uint.Parse(Address(8, 0), CultureInfo.InvariantCulture)),
                new(uint.Parse(Address(focusRegionId, 0), CultureInfo.InvariantCulture), uint.Parse(Address(8, 1), CultureInfo.InvariantCulture)),
                new(uint.Parse(Address(focusRegionId, 0), CultureInfo.InvariantCulture), uint.Parse(Address(8, 2), CultureInfo.InvariantCulture))
            });

        var projection = VizActivityProjectionBuilder.Build(
            Array.Empty<VizEventItem>(),
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId));

        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId),
            VizActivityCanvasInteractionState.Empty,
            topology);

        Assert.Single(layout.Edges, edge => string.Equals(edge.RouteLabel, "N0 -> R8", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(layout.Edges.Count, layout.Edges.Select(edge => edge.RouteLabel).Distinct(StringComparer.OrdinalIgnoreCase).Count());
    }

    [Fact]
    public void Build_RegionEdgesReportAggregatedUnderlyingRouteCount()
    {
        var events = new List<VizEventItem>
        {
            CreateEvent("VizAxonSent", tick: 600, region: 9, source: Address(9, 1), target: Address(23, 4), value: 0.9f, strength: 0.2f),
            CreateEvent("VizAxonSent", tick: 601, region: 9, source: Address(9, 7), target: Address(23, 9), value: 0.7f, strength: 0.4f),
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null));
        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null));

        var edge = Assert.Single(layout.Edges, item => string.Equals(item.RouteLabel, "R9 -> R23", StringComparison.OrdinalIgnoreCase));
        Assert.Contains("routes 2", edge.Detail, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Build_RegionEdgeEndpointsStartOutsideNodeBodies()
    {
        var events = new List<VizEventItem>
        {
            CreateEvent("VizAxonSent", tick: 610, region: 0, source: Address(0, 0), target: Address(31, 0), value: 0.8f, strength: 0.4f),
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null));
        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null));

        var sourceNode = Assert.Single(layout.Nodes, item => item.RegionId == 0);
        var targetNode = Assert.Single(layout.Nodes, item => item.RegionId == 31);
        var edge = Assert.Single(layout.Edges, item => string.Equals(item.RouteLabel, "R0 -> R31", StringComparison.OrdinalIgnoreCase));
        var sourceCenter = (X: sourceNode.Left + (sourceNode.Diameter / 2.0), Y: sourceNode.Top + (sourceNode.Diameter / 2.0));
        var targetCenter = (X: targetNode.Left + (targetNode.Diameter / 2.0), Y: targetNode.Top + (targetNode.Diameter / 2.0));

        var sourceDistance = Distance(sourceCenter.X, sourceCenter.Y, edge.SourceX, edge.SourceY);
        var targetDistance = Distance(targetCenter.X, targetCenter.Y, edge.TargetX, edge.TargetY);
        Assert.True(sourceDistance >= sourceNode.Diameter / 2.0);
        Assert.True(targetDistance >= targetNode.Diameter / 2.0);
    }

    [Fact]
    public void Build_RegionSelfLoopEdgeArcsOutsideNode()
    {
        var events = new List<VizEventItem>
        {
            CreateEvent("VizAxonSent", tick: 620, region: 16, source: Address(16, 0), target: Address(16, 1), value: 0.5f, strength: 0.2f),
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null));
        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null));

        var node = Assert.Single(layout.Nodes, item => item.RegionId == 16);
        var edge = Assert.Single(layout.Edges, item => string.Equals(item.RouteLabel, "R16 -> R16", StringComparison.OrdinalIgnoreCase));
        var centerX = node.Left + (node.Diameter / 2.0);
        var centerY = node.Top + (node.Diameter / 2.0);

        Assert.True(Distance(centerX, centerY, edge.SourceX, edge.SourceY) >= node.Diameter / 2.0);
        Assert.True(edge.ControlY < node.Top);
    }

    [Fact]
    public void Build_RegionNodeDetailUsesValueMetricsWhenBufferSamplesAreAbsent()
    {
        var events = new List<VizEventItem>
        {
            CreateEvent("VizNeuronFired", tick: 630, region: 9, source: Address(9, 1), target: Address(9, 1), value: 0.75f, strength: 0f),
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null));
        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null));

        var node = Assert.Single(layout.Nodes, item => item.RegionId == 9);
        Assert.Contains("value n=1", node.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("avg=0.75", node.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("latest=0.75@630", node.Detail, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Build_RegionNodeDetailShowsNotAvailableWhenNoValueEvents()
    {
        var projection = VizActivityProjectionBuilder.Build(
            Array.Empty<VizEventItem>(),
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null));
        var topology = new VizActivityCanvasTopology(
            new HashSet<uint> { 0, 5, 31 },
            new HashSet<VizActivityCanvasRegionRoute>(),
            new HashSet<uint>(),
            new HashSet<VizActivityCanvasNeuronRoute>());
        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null),
            VizActivityCanvasInteractionState.Empty,
            topology);

        var node = Assert.Single(layout.Nodes, item => item.RegionId == 5);
        Assert.Contains("value n=0", node.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("avg=n/a", node.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("latest=n/a", node.Detail, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Build_RegionNodeDiameterRemainsStableAcrossLoadChanges()
    {
        const uint regionId = 9;
        var lowEvents = new List<VizEventItem>
        {
            CreateEvent("VizNeuronFired", tick: 640, region: regionId, source: Address(regionId, 0), target: Address(regionId, 0), value: 0.1f, strength: 0f),
        };
        var highEvents = Enumerable.Range(0, 25)
            .Select(index => CreateEvent(
                "VizNeuronFired",
                tick: (ulong)(700 + index),
                region: regionId,
                source: Address(regionId, (uint)(index % 3)),
                target: Address(regionId, (uint)((index + 1) % 3)),
                value: 1f,
                strength: 0f))
            .ToList();

        var lowLayout = VizActivityCanvasLayoutBuilder.Build(
            VizActivityProjectionBuilder.Build(lowEvents, new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null)),
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null));
        var highLayout = VizActivityCanvasLayoutBuilder.Build(
            VizActivityProjectionBuilder.Build(highEvents, new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null)),
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null));

        var lowNode = Assert.Single(lowLayout.Nodes, item => item.RegionId == regionId);
        var highNode = Assert.Single(highLayout.Nodes, item => item.RegionId == regionId);
        Assert.Equal(lowNode.Diameter, highNode.Diameter);
    }

    [Fact]
    public void Build_RegionNodeDetailIncludesBufferAndRouteDegreeMetrics()
    {
        var projection = BuildProjection();
        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 32, IncludeLowSignalEvents: true, FocusRegionId: null));

        var outputNode = Assert.Single(layout.Nodes, node => node.RegionId == 31);
        Assert.Contains("value n=1", outputNode.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("routes out", outputNode.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("avg |v|", outputNode.Detail, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Build_FocusNeuronDetailIncludesBufferAndFlowMetrics()
    {
        const uint focusRegionId = 11;
        var events = new List<VizEventItem>
        {
            CreateEvent("VizAxonSent", tick: 700, region: focusRegionId, source: Address(focusRegionId, 2), target: Address(focusRegionId, 3), value: 1.1f, strength: 0.2f),
            CreateEvent("VizNeuronFired", tick: 701, region: focusRegionId, source: Address(focusRegionId, 2), target: Address(focusRegionId, 3), value: 0.9f, strength: 0f),
            CreateEvent("VizNeuronBuffer", tick: 702, region: focusRegionId, source: Address(focusRegionId, 2), target: Address(focusRegionId, 3), value: 0.42f, strength: 0f),
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId));
        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId),
            VizActivityCanvasInteractionState.Empty,
            VizActivityCanvasTopology.Empty);

        var neuron = Assert.Single(layout.Nodes, node => node.RegionId == focusRegionId && node.NeuronId == 2);
        Assert.Contains("fired 1", neuron.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("out 1", neuron.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("value n=3", neuron.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("latest=0.42@702", neuron.Detail, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Build_RegionNodesRemainFullyVisibleWithinCanvasBounds()
    {
        var events = Enumerable.Range(0, NbnConstants.RegionCount)
            .Select(index =>
            {
                var regionId = (uint)index;
                var tick = (ulong)(1000 + index);
                return CreateEvent(
                    "VizNeuronFired",
                    tick,
                    regionId,
                    Address(regionId, 0),
                    Address(regionId, 0),
                    value: 1f,
                    strength: 0f);
            })
            .ToList();

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 128, IncludeLowSignalEvents: true, FocusRegionId: null));
        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 128, IncludeLowSignalEvents: true, FocusRegionId: null));

        Assert.All(layout.Nodes, node =>
        {
            Assert.InRange(node.Left, 0d, VizActivityCanvasLayoutBuilder.CanvasWidth);
            Assert.InRange(node.Top, 0d, VizActivityCanvasLayoutBuilder.CanvasHeight);
            Assert.InRange(node.Left + node.Diameter, 0d, VizActivityCanvasLayoutBuilder.CanvasWidth);
            Assert.InRange(node.Top + node.Diameter, 0d, VizActivityCanvasLayoutBuilder.CanvasHeight);
        });
    }

    [Fact]
    public void Build_RegionLayoutAvoidsSingleAxisCollapseAcrossSlices()
    {
        var regions = new uint[] { 0, 1, 4, 9, 23, 28, 31 };
        var events = regions
            .Select((regionId, index) => CreateEvent(
                "VizNeuronFired",
                tick: (ulong)(3000 + index),
                region: regionId,
                source: Address(regionId, 0),
                target: Address(regionId, 0),
                value: 1f,
                strength: 0f))
            .ToList();

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null));
        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null));

        var centerYs = regions
            .Select(regionId => Assert.Single(layout.Nodes, node => node.RegionId == regionId))
            .Select(node => node.Top + (node.Diameter / 2.0))
            .ToList();
        var ySpread = centerYs.Max() - centerYs.Min();

        Assert.True(ySpread >= 60d, $"Expected vertical spread across slices but got {ySpread:0.##}.");
    }

    [Fact]
    public void Build_RegionLayoutUsesCompactHorizontalSpan()
    {
        var events = Enumerable.Range(0, NbnConstants.RegionCount)
            .Select(index =>
            {
                var regionId = (uint)index;
                var tick = (ulong)(4000 + index);
                return CreateEvent(
                    "VizNeuronFired",
                    tick,
                    regionId,
                    Address(regionId, 0),
                    Address(regionId, 0),
                    value: 1f,
                    strength: 0f);
            })
            .ToList();

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null));
        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: null));

        var centerXs = layout.Nodes
            .Select(node => node.Left + (node.Diameter / 2.0))
            .ToList();
        var xSpread = centerXs.Max() - centerXs.Min();

        Assert.InRange(xSpread, 300d, 620d);
    }

    [Fact]
    public void Build_FocusGatewayDetailIncludesRegionAggregateMetrics()
    {
        const uint focusRegionId = 13;
        const uint gatewayRegionId = 21;
        var events = new List<VizEventItem>
        {
            CreateEvent("VizAxonSent", tick: 900, region: focusRegionId, source: Address(focusRegionId, 2), target: Address(gatewayRegionId, 0), value: 0.7f, strength: 0.3f),
            CreateEvent("VizAxonSent", tick: 901, region: gatewayRegionId, source: Address(gatewayRegionId, 0), target: Address(focusRegionId, 2), value: -0.4f, strength: -0.2f),
            CreateEvent("VizNeuronFired", tick: 902, region: gatewayRegionId, source: Address(gatewayRegionId, 0), target: Address(focusRegionId, 2), value: 1f, strength: 0f),
            CreateEvent("VizNeuronBuffer", tick: 903, region: gatewayRegionId, source: Address(gatewayRegionId, 0), target: Address(focusRegionId, 2), value: 0.12f, strength: 0f)
        };
        var topology = new VizActivityCanvasTopology(
            new HashSet<uint> { focusRegionId, gatewayRegionId },
            new HashSet<VizActivityCanvasRegionRoute> { new(focusRegionId, gatewayRegionId), new(gatewayRegionId, focusRegionId) },
            new HashSet<uint>
            {
                uint.Parse(Address(focusRegionId, 2), CultureInfo.InvariantCulture),
                uint.Parse(Address(gatewayRegionId, 0), CultureInfo.InvariantCulture)
            },
            new HashSet<VizActivityCanvasNeuronRoute>
            {
                new(uint.Parse(Address(focusRegionId, 2), CultureInfo.InvariantCulture), uint.Parse(Address(gatewayRegionId, 0), CultureInfo.InvariantCulture)),
                new(uint.Parse(Address(gatewayRegionId, 0), CultureInfo.InvariantCulture), uint.Parse(Address(focusRegionId, 2), CultureInfo.InvariantCulture))
            });

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId));
        var layout = VizActivityCanvasLayoutBuilder.Build(
            projection,
            new VizActivityProjectionOptions(TickWindow: 64, IncludeLowSignalEvents: true, FocusRegionId: focusRegionId),
            VizActivityCanvasInteractionState.Empty,
            topology);

        var gateway = Assert.Single(layout.Nodes, node => node.Label == $"R{gatewayRegionId}");
        Assert.Contains("agg events", gateway.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("fired", gateway.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("avg |v|", gateway.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("routes out", gateway.Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("value n=", gateway.Detail, StringComparison.OrdinalIgnoreCase);
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

    private static double Distance(double x1, double y1, double x2, double y2)
    {
        var dx = x2 - x1;
        var dy = y2 - y1;
        return Math.Sqrt((dx * dx) + (dy * dy));
    }
}
