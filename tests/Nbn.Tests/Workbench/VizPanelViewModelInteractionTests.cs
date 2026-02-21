using System.Collections.ObjectModel;
using System.Reflection;
using System.Threading.Tasks;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Workbench;

public class VizPanelViewModelInteractionTests
{
    private static readonly MethodInfo RebuildHitIndexMethod =
        typeof(VizPanelViewModel).GetMethod(
            "RebuildCanvasHitIndex",
            BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("RebuildCanvasHitIndex method not found.");
    private static readonly MethodInfo BuildCanvasDiagnosticsReportMethod =
        typeof(VizPanelViewModel).GetMethod(
            "BuildCanvasDiagnosticsReport",
            BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("BuildCanvasDiagnosticsReport method not found.");
    private static readonly MethodInfo ApplyKeyedDiffMethod =
        typeof(VizPanelViewModel).GetMethod(
            "ApplyKeyedDiff",
            BindingFlags.Static | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("ApplyKeyedDiff method not found.");

    [Fact]
    public void TryResolveCanvasHit_NodeHoverStickyTolerance_ResolvesNearMiss()
    {
        var vm = CreateViewModel();
        var node = new VizActivityCanvasNode(
            NodeKey: "region:9",
            RegionId: 9,
            NeuronId: null,
            NavigateRegionId: 9,
            Label: "R9",
            Detail: "node detail",
            Left: 100,
            Top: 100,
            Diameter: 20,
            Fill: "#2A9D8F",
            Stroke: "#1B6B63",
            FillOpacity: 0.8,
            PulseOpacity: 0.6,
            StrokeThickness: 1.5,
            IsFocused: false,
            LastTick: 42,
            EventCount: 3,
            IsSelected: false,
            IsHovered: false,
            IsPinned: false);

        RebuildHitIndexMethod.Invoke(
            vm,
            new object[]
            {
                new List<VizActivityCanvasNode> { node },
                new List<VizActivityCanvasEdge>()
            });
        vm.SetCanvasNodeHover(node);

        var nearMissX = (node.Left + (node.Diameter / 2.0)) + (node.Diameter / 2.0) + 4.5;
        var nearMissY = node.Top + (node.Diameter / 2.0);
        var hit = vm.TryResolveCanvasHit(nearMissX, nearMissY, out var hitNode, out var hitEdge);

        Assert.True(hit);
        Assert.NotNull(hitNode);
        Assert.Null(hitEdge);
        Assert.Equal(node.NodeKey, hitNode!.NodeKey);
    }

    [Fact]
    public void TryResolveCanvasHit_EdgeHoverStickyTolerance_ResolvesNearMiss()
    {
        var vm = CreateViewModel();
        var edge = new VizActivityCanvasEdge(
            RouteLabel: "R9 -> R10",
            Detail: "edge detail",
            PathData: "M 20 20 Q 80 20 140 20",
            SourceX: 20,
            SourceY: 20,
            ControlX: 80,
            ControlY: 20,
            TargetX: 140,
            TargetY: 20,
            Stroke: "#7A838A",
            DirectionDashArray: string.Empty,
            ActivityStroke: "#2ECC71",
            StrokeThickness: 2.0,
            ActivityStrokeThickness: 1.2,
            HitTestThickness: 10.0,
            Opacity: 0.9,
            ActivityOpacity: 0.7,
            IsFocused: false,
            LastTick: 100,
            EventCount: 5,
            SourceRegionId: 9,
            TargetRegionId: 10,
            IsSelected: false,
            IsHovered: false,
            IsPinned: false);

        RebuildHitIndexMethod.Invoke(
            vm,
            new object[]
            {
                new List<VizActivityCanvasNode>(),
                new List<VizActivityCanvasEdge> { edge }
            });
        vm.SetCanvasEdgeHover(edge);

        var hit = vm.TryResolveCanvasHit(80, 28.5, out var hitNode, out var hitEdge);

        Assert.True(hit);
        Assert.Null(hitNode);
        Assert.NotNull(hitEdge);
        Assert.Equal(edge.RouteLabel, hitEdge!.RouteLabel);
    }

    [Fact]
    public async Task ClearCanvasHoverDeferred_SubsequentHoverCancelsPendingClear()
    {
        var vm = CreateViewModel();
        var node = CreateNode("region:9", "R9", left: 100, top: 100);

        RebuildHitIndexMethod.Invoke(
            vm,
            new object[]
            {
                new List<VizActivityCanvasNode> { node },
                new List<VizActivityCanvasEdge>()
            });
        vm.SetCanvasNodeHover(node);
        vm.ClearCanvasHoverDeferred(delayMs: 20);
        vm.SetCanvasNodeHover(node);
        await Task.Delay(90);

        Assert.True(vm.IsCanvasHoverCardVisible);
        Assert.Equal(node.Detail, vm.CanvasHoverCardText);
    }

    [Fact]
    public void BuildCanvasDiagnosticsReport_IncludesPerformanceMetrics()
    {
        var vm = CreateViewModel();
        var report = (string)BuildCanvasDiagnosticsReportMethod.Invoke(vm, Array.Empty<object>())!;

        Assert.Contains("perf projection_ms=", report);
        Assert.Contains("hit_test last_ms=", report);
        Assert.Contains("queue pending=", report);
        Assert.Contains("canvas_diff nodes(", report);
    }

    [Fact]
    public void ApplyKeyedDiff_ReordersAndRetainsUnchangedItems()
    {
        var first = CreateNode("region:9", "R9", left: 20, top: 30);
        var second = CreateNode("region:10", "R10", left: 80, top: 40);
        var secondUpdated = second with { Detail = "updated-detail" };
        var target = new ObservableCollection<VizActivityCanvasNode> { first, second };
        var source = new List<VizActivityCanvasNode> { secondUpdated, first };
        var keySelector = new Func<VizActivityCanvasNode, string?>(item => item.NodeKey);

        var typedMethod = ApplyKeyedDiffMethod.MakeGenericMethod(typeof(VizActivityCanvasNode));
        _ = typedMethod.Invoke(null, new object[] { target, source, keySelector });

        Assert.Equal(2, target.Count);
        Assert.Equal(secondUpdated.NodeKey, target[0].NodeKey);
        Assert.Equal("updated-detail", target[0].Detail);
        Assert.Same(first, target[1]);
    }

    private static VizActivityCanvasNode CreateNode(string key, string label, double left, double top)
    {
        return new VizActivityCanvasNode(
            NodeKey: key,
            RegionId: 9,
            NeuronId: null,
            NavigateRegionId: 9,
            Label: label,
            Detail: $"{label} detail",
            Left: left,
            Top: top,
            Diameter: 20,
            Fill: "#2A9D8F",
            Stroke: "#1B6B63",
            FillOpacity: 0.8,
            PulseOpacity: 0.6,
            StrokeThickness: 1.5,
            IsFocused: false,
            LastTick: 42,
            EventCount: 3,
            IsSelected: false,
            IsHovered: false,
            IsPinned: false);
    }

    private static VizPanelViewModel CreateViewModel()
    {
        var dispatcher = new UiDispatcher();
        var client = new WorkbenchClient(new NullWorkbenchEventSink());
        var io = new IoPanelViewModel(client, dispatcher);
        return new VizPanelViewModel(dispatcher, io);
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
