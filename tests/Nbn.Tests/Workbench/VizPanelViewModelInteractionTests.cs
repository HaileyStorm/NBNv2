using System.Collections.ObjectModel;
using System.Globalization;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Proto.Viz;
using Nbn.Shared;
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
    private static readonly MethodInfo ApplyVisualizationCadenceAsyncMethod =
        typeof(VizPanelViewModel).GetMethod(
            "ApplyVisualizationCadenceAsync",
            BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("ApplyVisualizationCadenceAsync method not found.");
    private static readonly MethodInfo ApplyTickCadenceAsyncMethod =
        typeof(VizPanelViewModel).GetMethod(
            "ApplyTickCadenceAsync",
            BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("ApplyTickCadenceAsync method not found.");

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
    public void SelectedCanvasColorModeAndCurve_UpdateHintsAndTooltips()
    {
        var vm = CreateViewModel();
        var topologyMode = Assert.Single(
            vm.CanvasColorModeOptions,
            option => option.Mode == VizActivityCanvasColorMode.Topology);
        var linearCurve = Assert.Single(
            vm.CanvasTransferCurveOptions,
            option => option.Curve == VizActivityCanvasTransferCurve.Linear);

        vm.SelectedCanvasColorMode = topologyMode;
        vm.SelectedCanvasTransferCurve = linearCurve;

        Assert.Equal(topologyMode.LegendHint, vm.CanvasColorModeHint);
        Assert.Equal(topologyMode.Tooltip, vm.CanvasColorModeTooltip);
        Assert.Equal(linearCurve.LegendHint, vm.CanvasColorCurveHint);
        Assert.Equal(linearCurve.Tooltip, vm.CanvasColorCurveTooltip);
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
    public void SelectedBrainChange_ResetsRegionFocusToFullBrain()
    {
        var vm = CreateViewModel();
        var brainA = new BrainListItem(Guid.NewGuid(), "A", true);
        var brainB = new BrainListItem(Guid.NewGuid(), "B", true);
        vm.KnownBrains.Add(brainA);
        vm.KnownBrains.Add(brainB);

        vm.SelectedBrain = brainA;
        Assert.True(vm.ZoomToRegion(9));
        Assert.Equal("9", vm.RegionFocusText);
        Assert.Equal((uint)9, vm.ActiveFocusRegionId);

        vm.SelectedBrain = brainB;

        Assert.Equal(string.Empty, vm.RegionFocusText);
        Assert.Null(vm.ActiveFocusRegionId);
    }

    [Fact]
    public void HandleEmptyCanvasDoubleClick_FocusedDefaultView_SwitchesToFullBrain()
    {
        var vm = CreateViewModel();

        Assert.True(vm.ZoomToRegion(9));
        var action = vm.HandleEmptyCanvasDoubleClick(isDefaultCanvasView: true);

        Assert.Equal(VizPanelViewModel.EmptyCanvasDoubleClickAction.ShowFullBrain, action);
        Assert.Equal(string.Empty, vm.RegionFocusText);
        Assert.Null(vm.ActiveFocusRegionId);
    }

    [Fact]
    public void HandleEmptyCanvasDoubleClick_FocusedNonDefaultView_ResetsViewWithoutChangingFocus()
    {
        var vm = CreateViewModel();

        Assert.True(vm.ZoomToRegion(9));
        var action = vm.HandleEmptyCanvasDoubleClick(isDefaultCanvasView: false);

        Assert.Equal(VizPanelViewModel.EmptyCanvasDoubleClickAction.ResetView, action);
        Assert.Equal("9", vm.RegionFocusText);
        Assert.Equal((uint)9, vm.ActiveFocusRegionId);
    }

    [Fact]
    public void TryClearCanvasSelectionFromEmptyClick_WithSelection_ClearsSelection()
    {
        var vm = CreateViewModel();
        var node = CreateNode("region:9", "R9", left: 100, top: 100);
        vm.CanvasNodes.Add(node);
        vm.CanvasEdges.Clear();
        SetCanvasSelection(vm, node.NodeKey, null);
        UpdateInteractionSummaries(
            vm,
            new List<VizActivityCanvasNode> { node },
            new List<VizActivityCanvasEdge>());

        var cleared = vm.TryClearCanvasSelectionFromEmptyClick();

        Assert.True(cleared);
        Assert.Null(SelectedCanvasNodeKeyField.GetValue(vm));
        Assert.Null(SelectedCanvasRouteLabelField.GetValue(vm));
        Assert.Contains("Selection cleared", vm.Status, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryClearCanvasSelectionFromEmptyClick_WithoutSelection_ReturnsFalse()
    {
        var vm = CreateViewModel();

        var cleared = vm.TryClearCanvasSelectionFromEmptyClick();

        Assert.False(cleared);
    }

    [Fact]
    public void SetBrains_PreservesSelectedBrainAcrossRefreshObjects()
    {
        var vm = CreateViewModel();
        var brainAId = Guid.NewGuid();
        var brainBId = Guid.NewGuid();
        var firstA = new BrainListItem(brainAId, "A", true);
        var firstB = new BrainListItem(brainBId, "B", true);
        vm.SetBrains(new[] { firstA, firstB });

        vm.SelectedBrain = firstB;

        var refreshedA = new BrainListItem(brainAId, "A", true);
        var refreshedB = new BrainListItem(brainBId, "B", true);
        vm.SetBrains(new[] { refreshedA, refreshedB });

        Assert.Equal(brainBId, vm.SelectedBrain?.BrainId);
        Assert.Equal(refreshedB.Display, vm.SelectedBrain?.Display);
    }

    [Fact]
    public void SetBrains_DoesNotForceFirstBrainWhenSelectionTemporarilyMissing()
    {
        var vm = CreateViewModel();
        var brainA = new BrainListItem(Guid.NewGuid(), "A", true);
        var brainB = new BrainListItem(Guid.NewGuid(), "B", true);
        vm.SetBrains(new[] { brainA, brainB });

        vm.SelectedBrain = brainB;
        var refreshedOnlyA = new BrainListItem(brainA.BrainId, "A", true);
        vm.SetBrains(new[] { refreshedOnlyA });

        Assert.Equal(brainB.BrainId, vm.SelectedBrain?.BrainId);
        Assert.Contains(vm.KnownBrains, entry => entry.BrainId == brainB.BrainId);
    }

    [Fact]
    public void SetBrains_MissingSelectionAfterThreshold_FallsBackToAvailableBrain()
    {
        var vm = CreateViewModel();
        var brainA = new BrainListItem(Guid.NewGuid(), "A", true);
        var brainB = new BrainListItem(Guid.NewGuid(), "B", true);
        vm.SetBrains(new[] { brainA, brainB });
        vm.SelectedBrain = brainB;

        vm.SetBrains(new[] { new BrainListItem(brainA.BrainId, "A", true) });
        vm.SetBrains(new[] { new BrainListItem(brainA.BrainId, "A", true) });
        Assert.Equal(brainB.BrainId, vm.SelectedBrain?.BrainId);

        vm.SetBrains(new[] { new BrainListItem(brainA.BrainId, "A", true) });

        Assert.Equal(brainA.BrainId, vm.SelectedBrain?.BrainId);
        Assert.Single(vm.KnownBrains);
    }

    [Fact]
    public void SetBrains_SingleEmptyRefresh_PreservesSelectionAndKnownBrains()
    {
        var vm = CreateViewModel();
        var brainA = new BrainListItem(Guid.NewGuid(), "A", true);
        var brainB = new BrainListItem(Guid.NewGuid(), "B", true);
        vm.SetBrains(new[] { brainA, brainB });
        vm.SelectedBrain = brainB;

        vm.SetBrains(Array.Empty<BrainListItem>());

        Assert.Equal(brainB.BrainId, vm.SelectedBrain?.BrainId);
        Assert.Equal(2, vm.KnownBrains.Count);
    }

    [Fact]
    public void SetBrains_RepeatedEmptyRefreshes_ClearSelectionAfterThreshold()
    {
        var vm = CreateViewModel();
        var brainA = new BrainListItem(Guid.NewGuid(), "A", true);
        var brainB = new BrainListItem(Guid.NewGuid(), "B", true);
        vm.SetBrains(new[] { brainA, brainB });
        vm.SelectedBrain = brainB;

        vm.SetBrains(Array.Empty<BrainListItem>());
        vm.SetBrains(Array.Empty<BrainListItem>());
        Assert.Equal(brainB.BrainId, vm.SelectedBrain?.BrainId);

        vm.SetBrains(Array.Empty<BrainListItem>());

        Assert.Null(vm.SelectedBrain);
        Assert.Empty(vm.KnownBrains);
        Assert.Equal("No brains reported.", vm.Status);
    }

    [Fact]
    public void SelectedBrainEnergySummary_RefreshesFromBrainInfo()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        var brainId = Guid.NewGuid();
        client.BrainInfoById[brainId] = new BrainInfo
        {
            EnergyRemaining = 4321,
            EnergyRateUnitsPerSecond = 7,
            LastTickCost = 3,
            CostEnabled = true,
            EnergyEnabled = true
        };
        var brain = new BrainListItem(brainId, "Running", true);
        vm.KnownBrains.Add(brain);

        vm.SelectedBrain = brain;

        var refreshed = SpinWait.SpinUntil(
            () => vm.SelectedBrainEnergySummary.Contains("4,321 units", StringComparison.OrdinalIgnoreCase),
            TimeSpan.FromSeconds(2));
        Assert.True(refreshed);
        Assert.Contains("rate 7/s", vm.SelectedBrainEnergySummary, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("cost+energy on", vm.SelectedBrainEnergySummary, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ApplyHiveMindTickStatus_UpdatesCadenceAndOverrideSummary()
    {
        var vm = CreateViewModel();

        vm.ApplyHiveMindTickStatus(targetTickHz: 8f, hasOverride: false, overrideTickHz: 0f);

        Assert.Equal("Current cadence: 8 Hz (125 ms/tick).", vm.TickCadenceSummary);
        Assert.Equal("Tick cadence control is not set. Current runtime target 8 Hz (125 ms/tick).", vm.TickRateOverrideSummary);

        vm.ApplyHiveMindTickStatus(targetTickHz: 12.5f, hasOverride: true, overrideTickHz: 25f);

        Assert.Equal("Current cadence: 12.5 Hz (80 ms/tick).", vm.TickCadenceSummary);
        Assert.Equal("Tick cadence control target: 25 Hz (40 ms/tick). Current runtime target 12.5 Hz (80 ms/tick).", vm.TickRateOverrideSummary);
    }

    [Theory]
    [InlineData("12.5", 12.5f)]
    [InlineData("12.5hz", 12.5f)]
    [InlineData("80ms", 12.5f)]
    [InlineData("40 ms", 25f)]
    public void TryParseTickRateOverrideInput_AcceptsHzAndMsFormats(string text, float expectedHz)
    {
        var ok = VizPanelViewModel.TryParseTickRateOverrideInput(text, out var hz);

        Assert.True(ok);
        Assert.Equal(expectedHz, hz, 3);
    }

    [Theory]
    [InlineData("")]
    [InlineData("0")]
    [InlineData("0ms")]
    [InlineData("-5")]
    [InlineData("abc")]
    public void TryParseTickRateOverrideInput_RejectsInvalidValues(string text)
    {
        var ok = VizPanelViewModel.TryParseTickRateOverrideInput(text, out var hz);

        Assert.False(ok);
        Assert.Equal(0f, hz);
    }

    [Fact]
    public async Task ApplyTickCadenceAsync_RejectsOutOfRangeCadence()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        vm.TickRateOverrideText = "0.5ms";

        await InvokePrivateAsync(ApplyTickCadenceAsyncMethod, vm);

        Assert.Equal("Tick cadence must be between 1 ms and 1000 ms (or equivalent Hz).", vm.Status);
        Assert.Empty(client.SetSettingCalls);
    }

    [Fact]
    public async Task ApplyTickCadenceAsync_UpdatesCurrentCadenceLabel()
    {
        var client = new FakeWorkbenchClient
        {
            HiveMindStatusResponse = new HiveMindStatus
            {
                TargetTickHz = 10f,
                HasTickRateOverride = true,
                TickRateOverrideHz = 10f
            }
        };
        var vm = CreateViewModel(client);
        vm.TickRateOverrideText = "10hz";

        await InvokePrivateAsync(ApplyTickCadenceAsyncMethod, vm);

        Assert.Equal("Current cadence: 10 Hz (100 ms/tick).", vm.TickCadenceSummary);
        Assert.Equal("Tick cadence control target: 10 Hz (100 ms/tick). Current runtime target 10 Hz (100 ms/tick).", vm.TickRateOverrideSummary);
        Assert.Equal("100ms", vm.TickRateOverrideText);
    }

    [Fact]
    public async Task ApplyVisualizationCadenceAsync_RejectsOutOfRangeCadence()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        vm.VizCadenceText = "50ms";

        await InvokePrivateAsync(ApplyVisualizationCadenceAsyncMethod, vm);

        Assert.Equal("Viz cadence must be 100-3000 ms (or equivalent Hz).", vm.Status);
        Assert.Empty(client.SetSettingCalls);
    }

    [Fact]
    public async Task ApplyVisualizationCadenceAsync_ClampsToTickCadenceFloor()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        vm.TickRateOverrideText = "500ms";
        vm.VizCadenceText = "100ms";

        await InvokePrivateAsync(ApplyVisualizationCadenceAsyncMethod, vm);

        Assert.Equal("500", client.SettingsByKey[VisualizationSettingsKeys.TickMinIntervalMsKey]);
        Assert.Equal("500", client.SettingsByKey[VisualizationSettingsKeys.StreamMinIntervalMsKey]);
        Assert.Equal("Viz cadence clamped to 500 ms to stay at or slower than tick cadence.", vm.Status);
        Assert.Equal(500d, vm.VizCadenceSliderEffectiveMin);
        Assert.Equal(500d, vm.VizCadenceSliderMs);
        Assert.Equal("500ms", vm.VizCadenceText);
    }

    [Fact]
    public void CanvasSelectionPanel_DefaultsCollapsedAndCollapsesWhenSelectionClears()
    {
        var vm = CreateViewModel();

        Assert.False(vm.IsCanvasSelectionExpanded);
        Assert.False(vm.IsCanvasSelectionDetailsVisible);
        Assert.False(vm.ToggleCanvasSelectionExpandedCommand.CanExecute(null));

        var node = CreateNode("region:9", "R9", left: 100, top: 100);
        SetCanvasSelection(vm, node.NodeKey, null);
        UpdateInteractionSummaries(
            vm,
            new List<VizActivityCanvasNode> { node },
            new List<VizActivityCanvasEdge>());

        Assert.True(vm.HasCanvasSelection);
        Assert.False(vm.IsCanvasSelectionExpanded);
        Assert.False(vm.IsCanvasSelectionDetailsVisible);
        Assert.True(vm.ToggleCanvasSelectionExpandedCommand.CanExecute(null));

        vm.ToggleCanvasSelectionExpandedCommand.Execute(null);
        Assert.True(vm.IsCanvasSelectionExpanded);
        Assert.True(vm.IsCanvasSelectionDetailsVisible);

        SetCanvasSelection(vm, null, null);
        UpdateInteractionSummaries(
            vm,
            new List<VizActivityCanvasNode>(),
            new List<VizActivityCanvasEdge>());

        Assert.False(vm.HasCanvasSelection);
        Assert.False(vm.IsCanvasSelectionExpanded);
        Assert.False(vm.IsCanvasSelectionDetailsVisible);
        Assert.False(vm.ToggleCanvasSelectionExpandedCommand.CanExecute(null));
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

    [Fact]
    public void AddVizEvent_GlobalTickWithoutBrainId_IsSuppressed()
    {
        var vm = CreateViewModel();
        vm.AddVizEvent(CreateVizEvent(
            type: VizEventType.VizTick.ToString(),
            brainId: string.Empty,
            tickId: 11));

        Assert.Empty(vm.VizEvents);
    }

    [Fact]
    public void AddVizEvent_NonGlobalWithoutBrainId_RemainsVisibleWithSelectedBrain()
    {
        var vm = CreateViewModel();
        var brain = new BrainListItem(Guid.NewGuid(), "Running", true);
        vm.KnownBrains.Add(brain);
        vm.SelectedBrain = brain;

        vm.AddVizEvent(CreateVizEvent(
            type: VizEventType.VizNeuronFired.ToString(),
            brainId: string.Empty,
            tickId: 12,
            region: "0",
            source: "42",
            target: "99"));

        var item = Assert.Single(vm.VizEvents);
        Assert.Equal(VizEventType.VizNeuronFired.ToString(), item.Type);
        Assert.Equal(string.Empty, item.BrainId);
    }

    [Fact]
    public void MiniActivityChart_DefaultsToEnabledWithTopN8()
    {
        var vm = CreateViewModel();

        Assert.True(vm.ShowMiniActivityChart);
        Assert.Equal("8", vm.MiniActivityTopNText);
        Assert.Equal("3", vm.MiniActivityRangeSecondsText);
        Assert.Contains("score =", vm.MiniActivityChartMetricLabel, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("log", vm.MiniActivityChartMetricLabel, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MiniActivityChart_DisableToggle_ClearsRenderedSeries()
    {
        var vm = CreateViewModel();
        vm.ShowMiniActivityChart = false;

        Assert.Empty(vm.MiniActivityChartSeries);
        Assert.Equal("Ticks: mini chart disabled.", vm.MiniActivityChartRangeLabel);

        vm.ShowMiniActivityChart = true;

        Assert.Equal("Ticks: awaiting activity.", vm.MiniActivityChartRangeLabel);
    }

    [Fact]
    public void ApplyActivityOptions_InvalidMiniTopN_ShowsValidationError()
    {
        var vm = CreateViewModel();
        vm.MiniActivityTopNText = "0";

        vm.ApplyActivityOptionsCommand.Execute(null);

        Assert.Contains("Mini chart Top N", vm.Status, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ApplyActivityOptions_InvalidMiniRangeSeconds_ShowsValidationError()
    {
        var vm = CreateViewModel();
        vm.MiniActivityRangeSecondsText = "0.1";

        vm.ApplyActivityOptionsCommand.Execute(null);

        Assert.Contains("Mini chart range", vm.Status, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MiniActivityChart_UsesProjectionHistoryBeyondVisibleEventBuffer()
    {
        var vm = CreateViewModel();
        var brain = new BrainListItem(Guid.NewGuid(), "Running", true);
        vm.KnownBrains.Add(brain);
        vm.SelectedBrain = brain;

        for (ulong tick = 1; tick <= 20; tick++)
        {
            for (var i = 0; i < 50; i++)
            {
                vm.AddVizEvent(CreateVizEvent(
                    type: VizEventType.VizAxonSent.ToString(),
                    brainId: brain.BrainId.ToString("D"),
                    tickId: tick,
                    region: "1",
                    source: "65537",
                    target: "131073",
                    value: 0.25f,
                    strength: 0.15f));
            }
        }

        var chartReady = SpinWait.SpinUntil(
            () => vm.MiniActivityChartRangeLabel.Contains("Ticks 0..20", StringComparison.OrdinalIgnoreCase),
            TimeSpan.FromSeconds(5));

        Assert.True(chartReady);
        Assert.Equal(400, vm.VizEvents.Count);
        Assert.Contains("Ticks 0..20", vm.MiniActivityChartRangeLabel, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MiniActivityChart_FocusMode_UsesSignedBufferLinearAxis()
    {
        var vm = CreateViewModel();
        var brain = new BrainListItem(Guid.NewGuid(), "Running", true);
        vm.KnownBrains.Add(brain);
        vm.SelectedBrain = brain;
        vm.RegionFocusText = "0";

        vm.AddVizEvent(CreateVizEvent(
            type: VizEventType.VizNeuronBuffer.ToString(),
            brainId: brain.BrainId.ToString("D"),
            tickId: 10,
            region: "0",
            source: "1",
            target: "1",
            value: -0.5f));
        vm.AddVizEvent(CreateVizEvent(
            type: VizEventType.VizNeuronBuffer.ToString(),
            brainId: brain.BrainId.ToString("D"),
            tickId: 11,
            region: "0",
            source: "1",
            target: "1",
            value: 0.3f));

        var rendered = SpinWait.SpinUntil(
            () => vm.MiniActivityChartMetricLabel.Contains("linear", StringComparison.OrdinalIgnoreCase),
            TimeSpan.FromSeconds(5));

        Assert.True(rendered);
        Assert.Contains("buffer", vm.MiniActivityChartMetricLabel, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("log(", vm.MiniActivityChartMetricLabel, StringComparison.OrdinalIgnoreCase);
        Assert.NotEqual("0", vm.MiniActivityYAxisBottomLabel);
    }

    [Fact]
    public void MiniActivityChart_LeadingZeroPlateau_RemainsAnchoredAtLeftEdge()
    {
        var vm = CreateViewModel();
        var brain = new BrainListItem(Guid.NewGuid(), "Running", true);
        vm.KnownBrains.Add(brain);
        vm.SelectedBrain = brain;

        vm.AddVizEvent(CreateVizEvent(
            type: VizEventType.VizAxonSent.ToString(),
            brainId: brain.BrainId.ToString("D"),
            tickId: 100,
            region: "1",
            source: "65537",
            target: "131073",
            value: 0.9f,
            strength: 0.3f));

        var rendered = SpinWait.SpinUntil(
            () => vm.MiniActivityChartSeries.Count > 0
                  && vm.MiniActivityChartRangeLabel.Contains("Ticks 41..100", StringComparison.OrdinalIgnoreCase),
            TimeSpan.FromSeconds(5));
        Assert.True(rendered);

        foreach (var series in vm.MiniActivityChartSeries)
        {
            var (x, y) = ParseMiniChartFirstPoint(series.PathData);
            Assert.InRange(x, 5.95, 6.05);
            Assert.InRange(y, 80.5, 81.9);
        }
    }

    [Fact]
    public void MiniActivityChart_CadenceIncrease_RebasesWindowToCurrentTick()
    {
        var vm = CreateViewModel();
        var brain = new BrainListItem(Guid.NewGuid(), "Running", true);
        vm.KnownBrains.Add(brain);
        vm.SelectedBrain = brain;

        vm.ApplyHiveMindTickStatus(targetTickHz: 8f, hasOverride: false, overrideTickHz: 0f);
        vm.AddVizEvent(CreateVizEvent(
            type: VizEventType.VizAxonSent.ToString(),
            brainId: brain.BrainId.ToString("D"),
            tickId: 100,
            region: "1",
            source: "65537",
            target: "131073",
            value: 0.9f,
            strength: 0.3f));

        var initialRendered = SpinWait.SpinUntil(
            () => vm.MiniActivityChartRangeLabel.Contains("Ticks 77..100", StringComparison.OrdinalIgnoreCase),
            TimeSpan.FromSeconds(5));
        Assert.True(initialRendered);

        vm.ApplyHiveMindTickStatus(targetTickHz: 20f, hasOverride: false, overrideTickHz: 0f);
        vm.AddVizEvent(CreateVizEvent(
            type: VizEventType.VizAxonSent.ToString(),
            brainId: brain.BrainId.ToString("D"),
            tickId: 101,
            region: "1",
            source: "65537",
            target: "131073",
            value: 0.8f,
            strength: 0.2f));

        var rebased = SpinWait.SpinUntil(
            () => vm.MiniActivityChartRangeLabel.Contains("Ticks 100..101", StringComparison.OrdinalIgnoreCase),
            TimeSpan.FromSeconds(5));
        Assert.True(rebased);
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

    private static VizPanelViewModel CreateViewModel(WorkbenchClient? client = null)
    {
        var dispatcher = new UiDispatcher();
        client ??= new WorkbenchClient(new NullWorkbenchEventSink());
        var io = new IoPanelViewModel(client, dispatcher);
        return new VizPanelViewModel(dispatcher, io);
    }

    private static async Task InvokePrivateAsync(MethodInfo method, object instance)
    {
        var task = method.Invoke(instance, Array.Empty<object>()) as Task;
        Assert.NotNull(task);
        await task!;
    }

    private static (double X, double Y) ParseMiniChartFirstPoint(string pathData)
    {
        Assert.False(string.IsNullOrWhiteSpace(pathData));
        var tokens = pathData.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        Assert.True(tokens.Length >= 3);
        Assert.Equal("M", tokens[0]);

        var x = double.Parse(tokens[1], CultureInfo.InvariantCulture);
        var y = double.Parse(tokens[2], CultureInfo.InvariantCulture);
        return (x, y);
    }

    private static VizEventItem CreateVizEvent(
        string type,
        string brainId,
        ulong tickId,
        string region = "",
        string source = "",
        string target = "",
        float value = 0,
        float strength = 0,
        string eventId = "")
    {
        return new VizEventItem(
            DateTimeOffset.UtcNow,
            type,
            brainId,
            tickId,
            region,
            source,
            target,
            value,
            strength,
            eventId);
    }

    private sealed class FakeWorkbenchClient : WorkbenchClient
    {
        public FakeWorkbenchClient()
            : base(new NullWorkbenchEventSink())
        {
        }

        public Dictionary<Guid, BrainInfo> BrainInfoById { get; } = new();
        public Dictionary<string, string> SettingsByKey { get; } = new(StringComparer.OrdinalIgnoreCase);
        public List<(string Key, string Value)> SetSettingCalls { get; } = new();
        public HiveMindStatus? HiveMindStatusResponse { get; set; }

        public override Task<BrainInfo?> RequestBrainInfoAsync(Guid brainId)
        {
            if (BrainInfoById.TryGetValue(brainId, out var info))
            {
                return Task.FromResult<BrainInfo?>(info);
            }

            return Task.FromResult<BrainInfo?>(null);
        }

        public override Task<SettingValue?> SetSettingAsync(string key, string value)
        {
            SetSettingCalls.Add((key, value));
            SettingsByKey[key] = value;
            return Task.FromResult<SettingValue?>(new SettingValue
            {
                Key = key,
                Value = value,
                UpdatedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            });
        }

        public override Task<HiveMindStatus?> GetHiveMindStatusAsync()
            => Task.FromResult(HiveMindStatusResponse);
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
