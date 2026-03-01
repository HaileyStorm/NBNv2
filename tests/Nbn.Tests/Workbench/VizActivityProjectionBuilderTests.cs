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

    [Fact]
    public void Build_DoesNotCapRegionOrEdgeRowsUsedByCanvas()
    {
        var events = new List<VizEventItem>();
        for (uint region = 0; region < 20; region++)
        {
            var next = (region + 1) % 20;
            events.Add(CreateEvent(
                type: "VizAxonSent",
                tick: 300 + region,
                region: region,
                source: Address(region, 1),
                target: Address(next, 2),
                value: 0.75f,
                strength: 0.25f));
        }

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(
                TickWindow: 64,
                IncludeLowSignalEvents: true,
                FocusRegionId: null));

        Assert.True(projection.Regions.Count >= 20);
        Assert.True(projection.Edges.Count >= 20);
    }

    [Fact]
    public void Build_MiniChart_FullBrainTracksTopRegionsByDeterministicScore()
    {
        var events = new List<VizEventItem>
        {
            CreateEvent("VizAxonSent", tick: 100, region: 1, source: Address(1, 1), target: Address(2, 2), value: 1.0f, strength: 0.5f),
            CreateEvent("VizNeuronFired", tick: 101, region: 1, source: Address(1, 3), target: Address(3, 1), value: 0.5f, strength: 0f),
            CreateEvent("VizAxonSent", tick: 101, region: 2, source: Address(2, 4), target: Address(1, 5), value: 0.2f, strength: 0.3f)
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(
                TickWindow: 8,
                IncludeLowSignalEvents: true,
                FocusRegionId: null,
                TopSeriesCount: 2,
                EnableMiniChart: true,
                MiniChartTickWindow: 8));

        var chart = projection.MiniChart;
        Assert.True(chart.Enabled);
        Assert.Equal("Top 2 regions", chart.ModeLabel);
        Assert.Equal((ulong)94, chart.MinTick);
        Assert.Equal((ulong)101, chart.MaxTick);
        Assert.Equal(8, chart.Ticks.Count);
        Assert.Equal(2, chart.Series.Count);

        var first = chart.Series[0];
        var second = chart.Series[1];
        Assert.Equal("R1", first.Label);
        Assert.Equal("R2", second.Label);
        Assert.Equal(5.5f, first.TotalScore, 3);
        Assert.Equal(4.0f, second.TotalScore, 3);
        Assert.Equal(8, first.Values.Count);
        Assert.Equal(8, second.Values.Count);
        Assert.True(first.Values.Take(6).All(value => Math.Abs(value) <= 1e-6f));
        Assert.True(second.Values.Take(6).All(value => Math.Abs(value) <= 1e-6f));
        Assert.Equal(2.5f, first.Values[6], 3);
        Assert.Equal(3.0f, first.Values[7], 3);
        Assert.Equal(2.5f, second.Values[6], 3);
        Assert.Equal(1.5f, second.Values[7], 3);
    }

    [Fact]
    public void Build_MiniChart_FocusModeTracksTopNeurons_AndIncludesOutputRegionNeurons()
    {
        var outputRegion = (uint)NbnConstants.OutputRegionId;
        var events = new List<VizEventItem>
        {
            CreateEvent("VizAxonSent", tick: 200, region: 5, source: Address(5, 9), target: Address(outputRegion, 1), value: 1.0f, strength: 0.1f),
            CreateEvent("VizAxonSent", tick: 201, region: 8, source: Address(8, 2), target: Address(outputRegion, 3), value: 0.4f, strength: 0.2f),
            CreateEvent("VizAxonSent", tick: 201, region: 6, source: Address(6, 5), target: Address(outputRegion, 1), value: 0.3f, strength: 0.1f)
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(
                TickWindow: 8,
                IncludeLowSignalEvents: true,
                FocusRegionId: outputRegion,
                TopSeriesCount: 3,
                EnableMiniChart: true,
                MiniChartTickWindow: 8));

        var chart = projection.MiniChart;
        Assert.True(chart.Enabled);
        Assert.Equal("Top 3 neurons in R31", chart.ModeLabel);
        Assert.Equal(2, chart.Series.Count);

        var first = chart.Series[0];
        var second = chart.Series[1];
        Assert.Equal("R31N1", first.Label);
        Assert.Equal("R31N3", second.Label);
        Assert.Equal(8, first.Values.Count);
        Assert.Equal(8, second.Values.Count);
        Assert.True(first.Values.Take(6).All(value => Math.Abs(value) <= 1e-6f));
        Assert.True(second.Values.Take(7).All(value => Math.Abs(value) <= 1e-6f));
        Assert.InRange(first.Values[6], 2.099f, 2.101f);
        Assert.InRange(first.Values[7], 1.399f, 1.401f);
        Assert.InRange(second.Values[7], 1.599f, 1.601f);
    }

    [Fact]
    public void Build_MiniChart_WhenDisabled_ReturnsNoSeries()
    {
        var events = new List<VizEventItem>
        {
            CreateEvent("VizNeuronFired", tick: 10, region: 1, source: Address(1, 0), target: Address(2, 0), value: 0.8f, strength: 0.2f)
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(
                TickWindow: 8,
                IncludeLowSignalEvents: true,
                FocusRegionId: null,
                TopSeriesCount: 5,
                EnableMiniChart: false,
                MiniChartTickWindow: 8));

        Assert.False(projection.MiniChart.Enabled);
        Assert.Empty(projection.MiniChart.Series);
        Assert.Empty(projection.MiniChart.Ticks);
    }

    [Fact]
    public void Build_MiniChart_TieBreaksDeterministicallyByLabel()
    {
        var events = new List<VizEventItem>
        {
            CreateEvent("VizAxonSent", tick: 300, region: 1, source: Address(1, 1), target: Address(1, 2), value: 0.2f, strength: 0f),
            CreateEvent("VizAxonSent", tick: 300, region: 2, source: Address(2, 1), target: Address(2, 2), value: 0.2f, strength: 0f)
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(
                TickWindow: 8,
                IncludeLowSignalEvents: true,
                FocusRegionId: null,
                TopSeriesCount: 2,
                EnableMiniChart: true,
                MiniChartTickWindow: 8));

        var labels = projection.MiniChart.Series.Select(item => item.Label).ToList();
        Assert.Equal(new[] { "R1", "R2" }, labels);
    }

    [Fact]
    public void Build_MiniChart_KeepsConfiguredRollingTickSpanWhenEventsAreSparse()
    {
        var events = new List<VizEventItem>
        {
            CreateEvent("VizAxonSent", tick: 100, region: 1, source: Address(1, 1), target: Address(2, 2), value: 0.4f, strength: 0.1f),
            CreateEvent("VizAxonSent", tick: 150, region: 1, source: Address(1, 1), target: Address(2, 2), value: 0.6f, strength: 0.2f)
        };

        var projection = VizActivityProjectionBuilder.Build(
            events,
            new VizActivityProjectionOptions(
                TickWindow: 64,
                IncludeLowSignalEvents: true,
                FocusRegionId: null,
                TopSeriesCount: 2,
                EnableMiniChart: true,
                MiniChartTickWindow: 16));

        var chart = projection.MiniChart;
        Assert.Equal((ulong)135, chart.MinTick);
        Assert.Equal((ulong)150, chart.MaxTick);
        Assert.Equal(16, chart.Ticks.Count);
        var region = Assert.Single(chart.Series, item => item.Label == "R1");
        Assert.Equal(16, region.Values.Count);
        Assert.True(region.Values.Take(15).All(value => Math.Abs(value) <= 1e-6f));
        Assert.InRange(region.Values[15], 1.79f, 1.81f);
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
