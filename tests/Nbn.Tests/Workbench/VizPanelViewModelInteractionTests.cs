using System.Collections.ObjectModel;
using System.Reflection;
using System.Threading;
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
    private static readonly MethodInfo UpdateCanvasInteractionSummariesMethod =
        typeof(VizPanelViewModel).GetMethod(
            "UpdateCanvasInteractionSummaries",
            BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("UpdateCanvasInteractionSummaries method not found.");
    private static readonly MethodInfo ApplyKeyedDiffMethod =
        typeof(VizPanelViewModel).GetMethod(
            "ApplyKeyedDiff",
            BindingFlags.Static | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("ApplyKeyedDiff method not found.");
    private static readonly MethodInfo EnsureDefinitionTopologyCoverageMethod =
        typeof(VizPanelViewModel).GetMethod(
            "EnsureDefinitionTopologyCoverage",
            BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("EnsureDefinitionTopologyCoverage method not found.");
    private static readonly FieldInfo DefinitionTopologyGateField =
        typeof(VizPanelViewModel).GetField(
            "_definitionTopologyGate",
            BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("_definitionTopologyGate field not found.");
    private static readonly FieldInfo PendingDefinitionHydrationKeysField =
        typeof(VizPanelViewModel).GetField(
            "_pendingDefinitionHydrationKeys",
            BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("_pendingDefinitionHydrationKeys field not found.");
    private static readonly FieldInfo SelectedCanvasNodeKeyField =
        typeof(VizPanelViewModel).GetField(
            "_selectedCanvasNodeKey",
            BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("_selectedCanvasNodeKey field not found.");
    private static readonly FieldInfo SelectedCanvasRouteLabelField =
        typeof(VizPanelViewModel).GetField(
            "_selectedCanvasRouteLabel",
            BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("_selectedCanvasRouteLabel field not found.");

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
    public void TryResolveCanvasHit_PrefersOverlappingNode_OverStickyHoveredEdge()
    {
        var vm = CreateViewModel();
        var node = CreateNode("region:9", "R9", left: 70, top: 10);
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
                new List<VizActivityCanvasNode> { node },
                new List<VizActivityCanvasEdge> { edge }
            });
        vm.SetCanvasEdgeHover(edge);

        var hit = vm.TryResolveCanvasHit(80, 20, out var hitNode, out var hitEdge);

        Assert.True(hit);
        Assert.NotNull(hitNode);
        Assert.Null(hitEdge);
        Assert.Equal(node.NodeKey, hitNode!.NodeKey);
    }

    [Fact]
    public void TryResolveCanvasHoverHit_PrefersOverlappingNode_OverStickyHoveredEdge()
    {
        var vm = CreateViewModel();
        var node = CreateNode("region:9", "R9", left: 70, top: 10);
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
                new List<VizActivityCanvasNode> { node },
                new List<VizActivityCanvasEdge> { edge }
            });
        vm.SetCanvasEdgeHover(edge);

        var hit = vm.TryResolveCanvasHoverHit(80, 20, out var hitNode, out var hitEdge);

        Assert.True(hit);
        Assert.NotNull(hitNode);
        Assert.Null(hitEdge);
        Assert.Equal(node.NodeKey, hitNode!.NodeKey);
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
        await Task.Delay(140);

        Assert.True(vm.IsCanvasHoverCardVisible);
        Assert.Equal(node.Detail, vm.CanvasHoverCardText);
    }

    [Fact]
    public async Task KeepCanvasHoverAlive_CancelsDeferredClear()
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
        vm.ClearCanvasHoverDeferred(delayMs: 40);
        vm.KeepCanvasHoverAlive();
        await Task.Delay(140);

        Assert.True(vm.IsCanvasHoverCardVisible);
        Assert.Equal(node.Detail, vm.CanvasHoverCardText);
    }

    [Fact]
    public void ClearCanvasHover_ClearsHoverImmediately()
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
        vm.ClearCanvasHover();

        Assert.False(vm.IsCanvasHoverCardVisible);
        Assert.Equal(string.Empty, vm.CanvasHoverCardText);
    }

    [Fact]
    public void SelectCanvasNode_PopulatesSelectionDetailsPanel()
    {
        var vm = CreateViewModel();
        var brain = new BrainListItem(Guid.NewGuid(), "test", true);
        vm.KnownBrains.Add(brain);
        vm.SelectedBrain = brain;

        var node = CreateNode(
            key: "region:0:neuron:2",
            label: "R0/N2",
            left: 100,
            top: 100,
            regionId: 0,
            neuronId: 2,
            navigateRegionId: 0);
        SetCanvasSelection(vm, node.NodeKey, null);
        UpdateInteractionSummaries(
            vm,
            new List<VizActivityCanvasNode> { node },
            new List<VizActivityCanvasEdge>());

        Assert.True(vm.HasCanvasSelection);
        Assert.Contains("Selected Node", vm.CanvasSelectionTitle, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("region=R0", vm.CanvasSelectionIdentity, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("events=", vm.CanvasSelectionRuntime, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("focus_region=R0", vm.CanvasSelectionContext, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Ready: pulse", vm.CanvasSelectionActionHint, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SelectCanvasEdge_PopulatesSelectionRouteContextPanel()
    {
        var vm = CreateViewModel();
        var edge = CreateEdge();
        SetCanvasSelection(vm, null, edge.RouteLabel);
        UpdateInteractionSummaries(
            vm,
            new List<VizActivityCanvasNode>(),
            new List<VizActivityCanvasEdge> { edge });

        Assert.True(vm.HasCanvasSelection);
        Assert.Contains("Selected Route", vm.CanvasSelectionTitle, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("source=R9", vm.CanvasSelectionIdentity, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("relation=cross-region", vm.CanvasSelectionContext, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Route actions available", vm.CanvasSelectionActionHint, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RouteFocusActions_AreOnlyAvailable_ForSelectedEdge()
    {
        var vm = CreateViewModel();
        var node = CreateNode("region:9", "R9", left: 100, top: 100, regionId: 9, neuronId: null, navigateRegionId: 9);
        var edge = CreateEdge();

        SetCanvasSelection(vm, node.NodeKey, null);
        UpdateInteractionSummaries(
            vm,
            new List<VizActivityCanvasNode> { node },
            new List<VizActivityCanvasEdge> { edge });

        Assert.False(vm.HasSelectedRouteSource);
        Assert.False(vm.HasSelectedRouteTarget);
        Assert.False(vm.FocusSelectedRouteSourceCommand.CanExecute(null));
        Assert.False(vm.FocusSelectedRouteTargetCommand.CanExecute(null));

        SetCanvasSelection(vm, null, edge.RouteLabel);
        UpdateInteractionSummaries(
            vm,
            new List<VizActivityCanvasNode> { node },
            new List<VizActivityCanvasEdge> { edge });

        Assert.True(vm.HasSelectedRouteSource);
        Assert.True(vm.HasSelectedRouteTarget);
        Assert.True(vm.FocusSelectedRouteSourceCommand.CanExecute(null));
        Assert.True(vm.FocusSelectedRouteTargetCommand.CanExecute(null));
    }

    [Fact]
    public void PrepareInputPulseCommand_IsDisabled_ForAggregateNodeSelection()
    {
        var vm = CreateViewModel();
        var brain = new BrainListItem(Guid.NewGuid(), "test", true);
        vm.KnownBrains.Add(brain);
        vm.SelectedBrain = brain;

        var node = CreateNode(
            key: "region:9",
            label: "R9",
            left: 100,
            top: 100,
            regionId: 9,
            neuronId: null,
            navigateRegionId: 9);
        SetCanvasSelection(vm, node.NodeKey, null);
        UpdateInteractionSummaries(
            vm,
            new List<VizActivityCanvasNode> { node },
            new List<VizActivityCanvasEdge>());

        Assert.False(vm.PrepareInputPulseCommand.CanExecute(null));
        Assert.Contains("aggregate-only", vm.CanvasSelectionActionHint, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PrepareInputPulseCommand_IsEnabled_ForNonInputNeuronSelection()
    {
        var vm = CreateViewModel();
        var brain = new BrainListItem(Guid.NewGuid(), "test", true);
        vm.KnownBrains.Add(brain);
        vm.SelectedBrain = brain;

        var node = CreateNode(
            key: "region:9:neuron:5",
            label: "R9/N5",
            left: 100,
            top: 100,
            regionId: 9,
            neuronId: 5,
            navigateRegionId: 9);
        SetCanvasSelection(vm, node.NodeKey, null);
        UpdateInteractionSummaries(
            vm,
            new List<VizActivityCanvasNode> { node },
            new List<VizActivityCanvasEdge>());
        vm.CanvasNodes.Clear();
        vm.CanvasNodes.Add(node);
        vm.CanvasEdges.Clear();

        vm.SelectedInputPulseValueText = "0.75";
        Assert.True(vm.PrepareInputPulseCommand.CanExecute(null));
    }

    [Fact]
    public void PrepareInputPulseCommand_SendsRuntimePulseImmediately_WithValidationFlow()
    {
        var vm = CreateViewModel();
        var brain = new BrainListItem(Guid.NewGuid(), "test", true);
        vm.KnownBrains.Add(brain);
        vm.SelectedBrain = brain;

        var node = CreateNode(
            key: "region:0:neuron:3",
            label: "R0/N3",
            left: 120,
            top: 90,
            regionId: 0,
            neuronId: 3,
            navigateRegionId: 0);
        SetCanvasSelection(vm, node.NodeKey, null);
        UpdateInteractionSummaries(
            vm,
            new List<VizActivityCanvasNode> { node },
            new List<VizActivityCanvasEdge>());
        vm.CanvasNodes.Clear();
        vm.CanvasNodes.Add(node);
        vm.CanvasEdges.Clear();

        vm.SelectedInputPulseValueText = "not-a-number";
        Assert.False(vm.PrepareInputPulseCommand.CanExecute(null));
        Assert.Contains("finite float", vm.CanvasSelectionActionHint, StringComparison.OrdinalIgnoreCase);

        vm.SelectedInputPulseValueText = "1.25";
        Assert.True(vm.PrepareInputPulseCommand.CanExecute(null));

        vm.PrepareInputPulseCommand.Execute(null);
        Assert.Contains("Runtime pulse queued:", vm.Status, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PrepareInputPulseCommand_IsEnabled_ForRouteNeuronEndpointSelection()
    {
        var vm = CreateViewModel();
        var brain = new BrainListItem(Guid.NewGuid(), "test", true);
        vm.KnownBrains.Add(brain);
        vm.SelectedBrain = brain;

        var edge = CreateEdge() with
        {
            RouteLabel = "N7 -> R10",
            SourceRegionId = 9,
            TargetRegionId = 10
        };

        SetCanvasSelection(vm, null, edge.RouteLabel);
        UpdateInteractionSummaries(
            vm,
            new List<VizActivityCanvasNode>(),
            new List<VizActivityCanvasEdge> { edge });
        vm.CanvasNodes.Clear();
        vm.CanvasEdges.Clear();
        vm.CanvasEdges.Add(edge);

        vm.SelectedInputPulseValueText = "0.25";
        Assert.True(vm.PrepareInputPulseCommand.CanExecute(null));

        vm.PrepareInputPulseCommand.Execute(null);
        Assert.Contains("Runtime pulse queued:", vm.Status, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ApplyRuntimeStateCommand_RequiresAtLeastOneValue_AndSendsWhenValid()
    {
        var vm = CreateViewModel();
        var brain = new BrainListItem(Guid.NewGuid(), "test", true);
        vm.KnownBrains.Add(brain);
        vm.SelectedBrain = brain;

        var node = CreateNode(
            key: "region:9:neuron:3",
            label: "R9/N3",
            left: 120,
            top: 90,
            regionId: 9,
            neuronId: 3,
            navigateRegionId: 9);
        SetCanvasSelection(vm, node.NodeKey, null);
        UpdateInteractionSummaries(
            vm,
            new List<VizActivityCanvasNode> { node },
            new List<VizActivityCanvasEdge>());
        vm.CanvasNodes.Clear();
        vm.CanvasNodes.Add(node);
        vm.CanvasEdges.Clear();

        vm.SelectedBufferValueText = string.Empty;
        vm.SelectedAccumulatorValueText = string.Empty;
        Assert.False(vm.ApplyRuntimeStateCommand.CanExecute(null));

        vm.SelectedBufferValueText = "not-a-number";
        Assert.False(vm.ApplyRuntimeStateCommand.CanExecute(null));
        Assert.Contains("finite float", vm.CanvasSelectionActionHint, StringComparison.OrdinalIgnoreCase);

        vm.SelectedBufferValueText = "0.5";
        Assert.True(vm.ApplyRuntimeStateCommand.CanExecute(null));

        vm.ApplyRuntimeStateCommand.Execute(null);
        Assert.Contains("Runtime state queued:", vm.Status, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryResolveCanvasHit_NodeHitPadding_ResolvesNearMissWithoutStickyHover()
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

        var nearMissX = (node.Left + node.Diameter) + 1.8;
        var nearMissY = node.Top + (node.Diameter / 2.0);
        var hit = vm.TryResolveCanvasHit(nearMissX, nearMissY, out var hitNode, out var hitEdge);

        Assert.True(hit);
        Assert.NotNull(hitNode);
        Assert.Null(hitEdge);
        Assert.Equal(node.NodeKey, hitNode!.NodeKey);
    }

    [Fact]
    public void TryResolveCanvasHoverHit_IsStricterThanClickHitPaddingForNodes()
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

        var nearMissX = (node.Left + node.Diameter) + 1.8;
        var nearMissY = node.Top + (node.Diameter / 2.0);
        var clickHit = vm.TryResolveCanvasHit(nearMissX, nearMissY, out _, out _);
        var hoverHit = vm.TryResolveCanvasHoverHit(nearMissX, nearMissY, out _, out _);

        Assert.True(clickHit);
        Assert.False(hoverHit);
    }

    [Fact]
    public void TryResolveCanvasHoverHit_IsStricterThanClickHitPaddingForEdges()
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

        var clickHit = vm.TryResolveCanvasHit(80, 23.8, out _, out _);
        var hoverHit = vm.TryResolveCanvasHoverHit(80, 23.8, out _, out _);

        Assert.True(clickHit);
        Assert.False(hoverHit);
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

    [Fact]
    public void EnsureDefinitionTopologyCoverage_DedupesRepeatedRequests()
    {
        var vm = CreateViewModel();
        var gate = (SemaphoreSlim)DefinitionTopologyGateField.GetValue(vm)!;
        Assert.True(gate.Wait(0));
        try
        {
            var brain = new BrainListItem(Guid.NewGuid(), "test", true);
            vm.KnownBrains.Add(brain);
            vm.SelectedBrain = brain;

            EnsureDefinitionTopologyCoverageMethod.Invoke(vm, Array.Empty<object>());
            EnsureDefinitionTopologyCoverageMethod.Invoke(vm, Array.Empty<object>());

            var pending = PendingDefinitionHydrationKeysField.GetValue(vm)!;
            var pendingCount = (int)(pending.GetType().GetProperty("Count")?.GetValue(pending) ?? 0);
            Assert.Equal(1, pendingCount);
        }
        finally
        {
            gate.Release();
        }
    }

    private static void UpdateInteractionSummaries(
        VizPanelViewModel vm,
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges)
    {
        UpdateCanvasInteractionSummariesMethod.Invoke(vm, new object[] { nodes, edges });
    }

    private static void SetCanvasSelection(VizPanelViewModel vm, string? nodeKey, string? routeLabel)
    {
        SelectedCanvasNodeKeyField.SetValue(vm, nodeKey);
        SelectedCanvasRouteLabelField.SetValue(vm, routeLabel);
    }

    private static VizActivityCanvasNode CreateNode(
        string key,
        string label,
        double left,
        double top,
        uint regionId = 9,
        uint? neuronId = null,
        uint? navigateRegionId = null)
    {
        var targetRegion = navigateRegionId ?? regionId;
        return new VizActivityCanvasNode(
            NodeKey: key,
            RegionId: regionId,
            NeuronId: neuronId,
            NavigateRegionId: targetRegion,
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

    private static VizActivityCanvasEdge CreateEdge()
    {
        return new VizActivityCanvasEdge(
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
