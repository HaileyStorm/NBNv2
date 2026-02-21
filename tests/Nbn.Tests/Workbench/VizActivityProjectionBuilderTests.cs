using System.Globalization;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Workbench;

public class VizActivityProjectionBuilderTests
{
    [Fact]
    public void Build_GroupsRegionEdgeAndTickActivity()
    {
        var events = new List<VizEventItem>
        {
            CreateEvent("VizNeuronFired", tick: 100, region: 1, source: Address(1, 5), target: Address(2, 9), value: 0.7f, strength: 0.3f),
            CreateEvent("VizAxonSent", tick: 100, region: 1, source: Address(1, 5), target: Address(2, 9), value: 0.4f, strength: 0.5f),
            CreateEvent("VizNeuronBuffer", tick: 101, region: 2, source: Address(2, 9), target: Address(1, 5), value: 0.2f, strength: 0f),
            CreateEvent("VizNeuronFired", tick: 101, region: 2, source: Address(2, 9), target: Address(3, 1), value: 0.9f, strength: 0.1f)
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(
                TickWindow: 16,
                IncludeLowSignalEvents: true,
                FocusRegionId: null));

        Assert.Equal(2, projection.Regions.Count);
        Assert.Equal(2, projection.Ticks.Count);
        Assert.NotEmpty(projection.Edges);

        var regionOne = projection.Regions.Single(item => item.RegionId == 1);
        Assert.Equal(2, regionOne.EventCount);
        Assert.Equal(1, regionOne.FiredCount);
        Assert.Equal(1, regionOne.AxonCount);

        var tick101 = projection.Ticks.Single(item => item.TickId == 101);
        Assert.Equal(2, tick101.EventCount);
        Assert.Equal(1, tick101.FiredCount);
        Assert.Equal(1, tick101.BufferCount);
    }

    [Fact]
    public void Build_AppliesTickWindowAndLowSignalFiltering_WithoutCullingEdgesByFocus()
    {
        var events = new List<VizEventItem>
        {
            CreateEvent("VizAxonSent", tick: 90, region: 2, source: Address(2, 7), target: Address(2, 8), value: 0.9f, strength: 0.3f),
            CreateEvent("VizAxonSent", tick: 100, region: 2, source: Address(1, 1), target: Address(2, 2), value: 0.8f, strength: 0.4f),
            CreateEvent("VizAxonSent", tick: 102, region: 4, source: Address(3, 3), target: Address(4, 4), value: 0.7f, strength: 0.5f),
            CreateEvent("VizAxonSent", tick: 103, region: 2, source: Address(2, 2), target: Address(2, 3), value: 0f, strength: 0f),
            CreateEvent("VizTick", tick: 104, region: 2, source: string.Empty, target: string.Empty, value: 0f, strength: 0f)
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(
                TickWindow: 5,
                IncludeLowSignalEvents: false,
                FocusRegionId: 2));

        Assert.All(projection.Ticks, tick => Assert.InRange(tick.TickId, 100UL, 104UL));
        Assert.Equal(2, projection.Edges.Count);
        Assert.Contains(projection.Edges, edge => edge.SourceRegionId == 1 && edge.TargetRegionId == 2);
        Assert.Contains(projection.Edges, edge => edge.SourceRegionId == 3 && edge.TargetRegionId == 4);

        var focusCoverage = projection.Stats.Single(item => item.Label == "Focus coverage");
        Assert.Equal("2", focusCoverage.Value);

        var totalEvents = projection.Ticks.Sum(item => item.EventCount);
        Assert.Equal(3, totalEvents);
    }

    [Fact]
    public void Build_ParsesPrefixedRegionAndAddressText()
    {
        var events = new List<VizEventItem>
        {
            new(
                Time: DateTimeOffset.UtcNow,
                Type: "VizNeuronFired",
                BrainId: Guid.Empty.ToString("D"),
                TickId: 200,
                Region: "R2",
                Source: "R2N9",
                Target: "R31N1",
                Value: 1.0f,
                Strength: 0.5f,
                EventId: Guid.NewGuid().ToString("D"))
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(
                TickWindow: 16,
                IncludeLowSignalEvents: true,
                FocusRegionId: null));

        var region = projection.Regions.Single();
        Assert.Equal((uint)2, region.RegionId);

        var edge = projection.Edges.Single();
        Assert.Equal((uint?)2, edge.SourceRegionId);
        Assert.Equal((uint?)31, edge.TargetRegionId);
        Assert.Equal("R2N9 -> R31N1", edge.RouteLabel);
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
            Time: DateTimeOffset.UtcNow,
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
