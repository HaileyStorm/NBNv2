using System.Reflection;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Headless;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;
using System.Linq;
using Xunit;

namespace Nbn.Tests.Workbench;

public class DesignerEdgeInteractionTests
{
    private static readonly object AvaloniaInitGate = new();
    private static bool s_avaloniaInitialized;

    [Fact]
    public void RefreshEdges_AssignsBundleMetadata_ForVisibleOutboundEdges()
    {
        var (vm, region, source, visibleTarget, offPageTarget) = CreateInputRegionScenario(pageSize: 8);
        AddAxon(source, visibleTarget);
        AddAxon(source, offPageTarget);
        region.UpdateCounts();
        vm.Brain!.UpdateTotals();

        vm.SelectNeuronCommand.Execute(source);

        var outboundEdges = vm.VisibleEdges
            .Where(edge => edge.Kind == DesignerEdgeKind.OutboundInternal)
            .ToList();

        Assert.Equal(2, outboundEdges.Count);

        var bundleIndices = outboundEdges
            .Select(edge => ReadPrivateInt(edge, "_bundleIndex"))
            .OrderBy(value => value)
            .ToArray();
        var bundleCounts = outboundEdges
            .Select(edge => ReadPrivateInt(edge, "_bundleCount"))
            .ToArray();

        Assert.Equal(new[] { 0, 1 }, bundleIndices);
        Assert.All(bundleCounts, count => Assert.Equal(2, count));
    }

    [Fact]
    public void FocusEdgeEndpointCommand_NavigatesToOffPageEndpoint()
    {
        var (vm, region, source, _, offPageTarget) = CreateInputRegionScenario(pageSize: 2);
        AddAxon(source, offPageTarget);
        region.UpdateCounts();
        vm.Brain!.UpdateTotals();
        vm.SelectNeuronCommand.Execute(source);

        var outboundExternal = vm.VisibleEdges.Single(edge => edge.Kind == DesignerEdgeKind.OutboundExternal);
        Assert.True(outboundExternal.CanNavigate);
        Assert.Equal(0, outboundExternal.NavigationRegionId);
        Assert.Equal(offPageTarget.NeuronId, outboundExternal.NavigationNeuronId);

        vm.FocusEdgeEndpointCommand.Execute(outboundExternal);

        Assert.Equal(region, vm.SelectedRegion);
        Assert.Equal(offPageTarget, vm.SelectedNeuron);
        Assert.Equal(1, vm.RegionPageIndex);
        Assert.Equal("Focused edge endpoint R0 N2.", vm.Status);
    }

    [Fact]
    public void FocusEdgeEndpointCommand_SetsUnavailableStatus_WhenEndpointIsMissing()
    {
        var vm = CreateViewModel();
        var edge = new DesignerEdgeViewModel(
            new Point(0, 0),
            new Point(10, 10),
            false,
            false,
            DesignerEdgeKind.OutboundExternal,
            navigationRegionId: 99,
            navigationNeuronId: 0);

        vm.FocusEdgeEndpointCommand.Execute(edge);

        Assert.Equal("Edge endpoint is no longer available.", vm.Status);
    }

    [Fact]
    public void RefreshEdges_UpdatesOffPageLabels_AndAnalyticsSummary()
    {
        var (vm, region, source, visibleTarget, offPageTarget) = CreateInputRegionScenario(pageSize: 2);
        AddAxon(source, visibleTarget);
        AddAxon(source, offPageTarget);
        AddAxon(offPageTarget, source);
        region.UpdateCounts();
        vm.Brain!.UpdateTotals();

        vm.SelectNeuronCommand.Execute(source);

        Assert.Equal(
            "Out: 1 visible, 1 external/off-page. In: 0 visible, 1 external/off-page. Click off-page labels to jump.",
            vm.EdgeSummary);
        Assert.Equal(
            "Density: light (3 edges). External/off-page: 66.7% (2). Page coverage: 66.7% (2/3). Dominant target: R0 (2).",
            vm.EdgeAnalyticsSummary);

        var outboundExternal = vm.VisibleEdges.Single(edge => edge.Kind == DesignerEdgeKind.OutboundExternal);
        var inboundExternal = vm.VisibleEdges.Single(edge => edge.Kind == DesignerEdgeKind.InboundExternal);

        Assert.Equal("-> P2 N2", outboundExternal.Label);
        Assert.Equal("<- P2 N2", inboundExternal.Label);
        Assert.True(outboundExternal.CanNavigate);
        Assert.True(inboundExternal.CanNavigate);
        Assert.Equal(0, outboundExternal.NavigationRegionId);
        Assert.Equal(0, inboundExternal.NavigationRegionId);
        Assert.Equal(2, outboundExternal.NavigationNeuronId);
        Assert.Equal(2, inboundExternal.NavigationNeuronId);
    }

    [Fact]
    public void RefreshEdges_ExtendsOffPageEdgesBeyondVisibleNeuronEnvelope()
    {
        const double outerMargin = 28;
        const double canvasMargin = 14;

        var (vm, region, source, _, offPageTarget) = CreateInputRegionScenario(pageSize: 2);
        AddAxon(source, offPageTarget);
        AddAxon(offPageTarget, source);
        region.UpdateCounts();
        vm.Brain!.UpdateTotals();

        vm.SelectNeuronCommand.Execute(source);

        var externalEdges = vm.VisibleEdges
            .Where(edge => edge.Kind is DesignerEdgeKind.OutboundExternal or DesignerEdgeKind.InboundExternal)
            .ToList();
        Assert.NotEmpty(externalEdges);

        var sourceCenter = new Point(source.CanvasX + vm.CanvasNodeRadius, source.CanvasY + vm.CanvasNodeRadius);
        var furthestVisibleNeuronEdge = vm.VisibleNeurons
            .Select(neuron => Distance(
                sourceCenter,
                new Point(neuron.CanvasX + vm.CanvasNodeRadius, neuron.CanvasY + vm.CanvasNodeRadius)) + vm.CanvasNodeRadius)
            .DefaultIfEmpty(vm.CanvasNodeRadius)
            .Max();
        var minExpectedRadius = furthestVisibleNeuronEdge + outerMargin;

        foreach (var edge in externalEdges)
        {
            var offPagePoint = edge.Kind == DesignerEdgeKind.InboundExternal ? edge.Start : edge.End;
            var radius = Distance(sourceCenter, offPagePoint);
            Assert.True(radius >= minExpectedRadius - 0.5, $"Off-page radius {radius:0.###} was below envelope minimum {minExpectedRadius:0.###}.");
            Assert.InRange(offPagePoint.X, canvasMargin, vm.CanvasWidth - canvasMargin);
            Assert.InRange(offPagePoint.Y, canvasMargin, vm.CanvasHeight - canvasMargin);
        }
    }

    private static DesignerPanelViewModel CreateViewModel()
    {
        EnsureAvaloniaInitialized();
        var connections = new ConnectionViewModel();
        var client = new WorkbenchClient(new NullWorkbenchEventSink());
        var vm = new DesignerPanelViewModel(connections, client);
        vm.NewBrainCommand.Execute(null);
        return vm;
    }

    private static (DesignerPanelViewModel vm, DesignerRegionViewModel region, DesignerNeuronViewModel source, DesignerNeuronViewModel visibleTarget, DesignerNeuronViewModel offPageTarget)
        CreateInputRegionScenario(int pageSize)
    {
        var vm = CreateViewModel();
        var brain = vm.Brain!;
        var region = brain.Regions[0];

        vm.SelectRegionCommand.Execute(null);

        while (region.Neurons.Count < 3)
        {
            var neuronId = region.Neurons.Count;
            region.Neurons.Add(new DesignerNeuronViewModel(region.RegionId, neuronId, exists: true, isRequired: true));
        }

        region.UpdateCounts();
        brain.UpdateTotals();

        vm.RegionPageSizeText = pageSize.ToString();
        vm.SelectRegionCommand.Execute(region);

        var source = region.Neurons[0];
        source.Axons.Clear();
        source.UpdateAxonCount();

        var visibleTarget = region.Neurons[1];
        visibleTarget.Axons.Clear();
        visibleTarget.UpdateAxonCount();

        var offPageTarget = region.Neurons[2];
        offPageTarget.Axons.Clear();
        offPageTarget.UpdateAxonCount();

        region.UpdateCounts();
        brain.UpdateTotals();
        return (vm, region, source, visibleTarget, offPageTarget);
    }

    private static void AddAxon(DesignerNeuronViewModel source, DesignerNeuronViewModel target, int strengthCode = 16)
    {
        source.Axons.Add(new DesignerAxonViewModel(target.RegionId, target.NeuronId, strengthCode));
        source.UpdateAxonCount();
    }

    private static int ReadPrivateInt(object target, string fieldName)
    {
        var field = target.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        return (int)field!.GetValue(target)!;
    }

    private static double Distance(Point a, Point b)
    {
        var dx = a.X - b.X;
        var dy = a.Y - b.Y;
        return Math.Sqrt((dx * dx) + (dy * dy));
    }

    private static void EnsureAvaloniaInitialized()
    {
        lock (AvaloniaInitGate)
        {
            if (s_avaloniaInitialized || Application.Current is not null)
            {
                s_avaloniaInitialized = true;
                return;
            }

            try
            {
                AppBuilder.Configure<Application>()
                    .UseHeadless(new AvaloniaHeadlessPlatformOptions())
                    .SetupWithoutStarting();
            }
            catch (InvalidOperationException ex)
                when (ex.Message.Contains("Setup was already called", StringComparison.Ordinal))
            {
                // Another test class initialized Avalonia first; reuse that process-level setup.
            }

            s_avaloniaInitialized = true;
        }
    }

    private sealed class NullWorkbenchEventSink : IWorkbenchEventSink
    {
        public void OnOutputEvent(OutputEventItem item) { }
        public void OnOutputVectorEvent(OutputVectorEventItem item) { }
        public void OnDebugEvent(DebugEventItem item) { }
        public void OnVizEvent(VizEventItem item) { }
        public void OnBrainTerminated(BrainTerminatedItem item) { }
        public void OnIoStatus(string status, bool connected) { }
        public void OnObsStatus(string status, bool connected) { }
        public void OnSettingsStatus(string status, bool connected) { }
        public void OnHiveMindStatus(string status, bool connected) { }
        public void OnSettingChanged(SettingItem item) { }
    }
}
