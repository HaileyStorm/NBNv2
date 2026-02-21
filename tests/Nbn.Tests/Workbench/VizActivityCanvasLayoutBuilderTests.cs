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
    public void Build_AnchorsFocusedRegionAtCanvasCenter()
    {
        var projection = BuildProjection();
        var options = new VizActivityProjectionOptions(TickWindow: 32, IncludeLowSignalEvents: true, FocusRegionId: 23);

        var layout = VizActivityCanvasLayoutBuilder.Build(projection, options);
        var focused = layout.Nodes.Single(item => item.RegionId == 23);
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
