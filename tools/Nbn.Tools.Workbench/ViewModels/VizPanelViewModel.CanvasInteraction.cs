using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Platform.Storage;
using Nbn.Proto.Io;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class VizPanelViewModel
{
    public void SetCanvasViewportScale(double scale)
    {
        if (!double.IsFinite(scale) || scale <= 0.0)
        {
            scale = 1.0;
        }

        var previousTier = GetViewportScaleTier(_canvasViewportScale);
        _canvasViewportScale = scale;
        if (EnableAdaptiveLod && previousTier != GetViewportScaleTier(_canvasViewportScale))
        {
            RefreshCanvasLayoutOnly();
        }
    }

    public void SelectCanvasNode(VizActivityCanvasNode? node)
    {
        if (node is null)
        {
            ClearCanvasSelection();
            return;
        }

        _selectedCanvasNodeKey = node.NodeKey;
        _selectedCanvasRouteLabel = null;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
        RefreshCanvasLayoutOnly();
        Status = $"Selected node {node.Label}.";
        UpdateCanvasInteractionSummaries(CanvasNodes, CanvasEdges);
    }

    public void SelectCanvasEdge(VizActivityCanvasEdge? edge)
    {
        if (edge is null)
        {
            ClearCanvasSelection();
            return;
        }

        _selectedCanvasRouteLabel = edge.RouteLabel;
        _selectedCanvasNodeKey = null;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
        RefreshCanvasLayoutOnly();
        Status = $"Selected route {edge.RouteLabel}.";
        UpdateCanvasInteractionSummaries(CanvasNodes, CanvasEdges);
    }

    public void SetCanvasNodeHover(VizActivityCanvasNode? node, double pointerX = double.NaN, double pointerY = double.NaN)
    {
        if (node is null)
        {
            ClearCanvasHover();
            return;
        }

        CancelPendingHoverClear();
        var nextNode = node.NodeKey;
        if (string.Equals(_hoverCanvasNodeKey, nextNode, StringComparison.OrdinalIgnoreCase) && _hoverCanvasRouteLabel is null)
        {
            UpdateCanvasHoverCard(node.Detail, pointerX, pointerY);
            return;
        }

        _hoverCanvasNodeKey = nextNode;
        _hoverCanvasRouteLabel = null;
        UpdateCanvasHoverCard(node.Detail, pointerX, pointerY);
        UpdateCanvasInteractionSummaries(CanvasNodes, CanvasEdges);
    }

    public void SetCanvasEdgeHover(VizActivityCanvasEdge? edge, double pointerX = double.NaN, double pointerY = double.NaN)
    {
        if (edge is null)
        {
            ClearCanvasHover();
            return;
        }

        CancelPendingHoverClear();
        var nextRoute = edge.RouteLabel;
        if (string.Equals(_hoverCanvasRouteLabel, nextRoute, StringComparison.OrdinalIgnoreCase) && string.IsNullOrWhiteSpace(_hoverCanvasNodeKey))
        {
            UpdateCanvasHoverCard(edge.Detail, pointerX, pointerY);
            return;
        }

        _hoverCanvasRouteLabel = nextRoute;
        _hoverCanvasNodeKey = null;
        UpdateCanvasHoverCard(edge.Detail, pointerX, pointerY);
        UpdateCanvasInteractionSummaries(CanvasNodes, CanvasEdges);
    }

    public void ClearCanvasHoverDeferred(int delayMs = HoverClearDelayMs)
    {
        if (string.IsNullOrWhiteSpace(_hoverCanvasNodeKey)
            && string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel)
            && !IsCanvasHoverCardVisible
            && string.IsNullOrWhiteSpace(CanvasHoverCardText))
        {
            return;
        }

        var revision = Interlocked.Increment(ref _hoverClearRevision);
        var safeDelayMs = Math.Max(0, delayMs);
        _ = Task.Run(async () =>
        {
            await Task.Delay(safeDelayMs).ConfigureAwait(false);
            _dispatcher.Post(() =>
            {
                if (revision != _hoverClearRevision)
                {
                    return;
                }

                ClearCanvasHover();
            });
        });
    }

    public void ClearCanvasHover()
    {
        CancelPendingHoverClear();
        var hadHover = !string.IsNullOrWhiteSpace(_hoverCanvasNodeKey)
                       || !string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel)
                       || IsCanvasHoverCardVisible
                       || !string.IsNullOrWhiteSpace(CanvasHoverCardText);
        if (!hadHover)
        {
            return;
        }

        _hoverCanvasNodeKey = null;
        _hoverCanvasRouteLabel = null;
        CanvasHoverCardText = string.Empty;
        IsCanvasHoverCardVisible = false;
        UpdateCanvasInteractionSummaries(CanvasNodes, CanvasEdges);
    }

    public void KeepCanvasHoverAlive()
    {
        var hasHover = !string.IsNullOrWhiteSpace(_hoverCanvasNodeKey)
                       || !string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel)
                       || IsCanvasHoverCardVisible
                       || !string.IsNullOrWhiteSpace(CanvasHoverCardText);
        if (!hasHover)
        {
            return;
        }

        CancelPendingHoverClear();
    }

    public bool TryResolveCanvasHit(double pointerX, double pointerY, out VizActivityCanvasNode? node, out VizActivityCanvasEdge? edge)
    {
        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            return TryResolveCanvasHitCore(
                pointerX,
                pointerY,
                stickyNodePadding: StickyNodeHitPadding,
                stickyEdgePadding: StickyEdgeHitPadding,
                nodeHitPadding: NodeHitPadding,
                edgeHitThresholdScale: 0.5,
                edgeHitThresholdMin: 4.0,
                out node,
                out edge);
        }
        finally
        {
            RecordHitTestDuration(startedAt);
        }
    }

    public bool TryResolveCanvasHoverHit(double pointerX, double pointerY, out VizActivityCanvasNode? node, out VizActivityCanvasEdge? edge)
    {
        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            return TryResolveCanvasHitCore(
                pointerX,
                pointerY,
                stickyNodePadding: HoverStickyNodeHitPadding,
                stickyEdgePadding: HoverStickyEdgeHitPadding,
                nodeHitPadding: HoverNodeHitPadding,
                edgeHitThresholdScale: HoverEdgeHitThresholdScale,
                edgeHitThresholdMin: HoverEdgeHitThresholdMin,
                out node,
                out edge);
        }
        finally
        {
            RecordHitTestDuration(startedAt);
        }
    }

    public bool TrySelectHoveredCanvasItem(bool togglePin)
    {
        if (!string.IsNullOrWhiteSpace(_hoverCanvasNodeKey))
        {
            var node = _canvasNodeByKey.TryGetValue(_hoverCanvasNodeKey!, out var keyedNode)
                ? keyedNode
                : CanvasNodes.FirstOrDefault(item => string.Equals(item.NodeKey, _hoverCanvasNodeKey, StringComparison.OrdinalIgnoreCase));
            if (node is not null)
            {
                if (togglePin)
                {
                    TogglePinCanvasNode(node);
                }
                else
                {
                    SelectCanvasNode(node);
                }

                return true;
            }
        }

        if (string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel))
        {
            return false;
        }

        var edge = _canvasEdgeByRoute.TryGetValue(_hoverCanvasRouteLabel!, out var keyedEdge)
            ? keyedEdge
            : CanvasEdges.FirstOrDefault(item => string.Equals(item.RouteLabel, _hoverCanvasRouteLabel, StringComparison.OrdinalIgnoreCase));
        if (edge is null)
        {
            return false;
        }

        if (togglePin)
        {
            TogglePinCanvasEdge(edge);
        }
        else
        {
            SelectCanvasEdge(edge);
        }

        return true;
    }

    public void TogglePinCanvasNode(VizActivityCanvasNode? node)
    {
        if (node is null)
        {
            return;
        }

        if (!_pinnedCanvasNodes.Add(node.NodeKey))
        {
            _pinnedCanvasNodes.Remove(node.NodeKey);
        }

        _selectedCanvasNodeKey = node.NodeKey;
        _selectedCanvasRouteLabel = null;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
        RefreshCanvasLayoutOnly();
    }

    public void TogglePinCanvasEdge(VizActivityCanvasEdge? edge)
    {
        if (edge is null || string.IsNullOrWhiteSpace(edge.RouteLabel))
        {
            return;
        }

        if (!_pinnedCanvasRoutes.Add(edge.RouteLabel))
        {
            _pinnedCanvasRoutes.Remove(edge.RouteLabel);
        }

        _selectedCanvasRouteLabel = edge.RouteLabel;
        _selectedCanvasNodeKey = null;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
        RefreshCanvasLayoutOnly();
    }

    private void NavigateCanvasRelative(int delta)
    {
        if (CanvasNodes.Count == 0)
        {
            Status = "No canvas nodes available for navigation.";
            return;
        }

        var ordered = CanvasNodes
            .OrderByDescending(item => item.EventCount)
            .ThenByDescending(item => item.LastTick)
            .ThenBy(item => item.RegionId)
            .ThenBy(item => item.NodeKey, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var currentIndex = !string.IsNullOrWhiteSpace(_selectedCanvasNodeKey)
            ? ordered.FindIndex(item => string.Equals(item.NodeKey, _selectedCanvasNodeKey, StringComparison.OrdinalIgnoreCase))
            : delta >= 0 ? -1 : 0;
        var nextIndex = ((currentIndex + delta) % ordered.Count + ordered.Count) % ordered.Count;
        var next = ordered[nextIndex];
        SelectCanvasNode(next);
        Status = $"Canvas selection: {next.Label}.";
    }

    private void NavigateToCanvasSelection()
    {
        var regionId = GetCurrentSelectionRegionId(CanvasNodes, CanvasEdges);
        if (!regionId.HasValue)
        {
            Status = "Select a node or route before navigating focus.";
            return;
        }

        RegionFocusText = regionId.Value.ToString(CultureInfo.InvariantCulture);
        RefreshFilteredEvents();
        if (SelectedBrain is not null)
        {
            QueueDefinitionTopologyHydration(SelectedBrain.BrainId, regionId.Value);
        }
        Status = $"Canvas navigation focused region {regionId.Value}.";
    }

    private void ToggleCanvasSelectionExpanded()
    {
        if (!HasCanvasSelection)
        {
            IsCanvasSelectionExpanded = false;
            return;
        }

        IsCanvasSelectionExpanded = !IsCanvasSelectionExpanded;
    }

    private void TogglePinForCurrentSelection()
    {
        if (!string.IsNullOrWhiteSpace(_selectedCanvasNodeKey))
        {
            var nodeKey = _selectedCanvasNodeKey!;
            var node = CanvasNodes.FirstOrDefault(item => string.Equals(item.NodeKey, nodeKey, StringComparison.OrdinalIgnoreCase));
            if (!_pinnedCanvasNodes.Add(nodeKey))
            {
                _pinnedCanvasNodes.Remove(nodeKey);
            }

            RefreshCanvasLayoutOnly();
            var label = node?.Label ?? nodeKey;
            Status = _pinnedCanvasNodes.Contains(nodeKey)
                ? $"Pinned node {label}."
                : $"Unpinned node {label}.";
            return;
        }

        if (!string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel))
        {
            var routeLabel = _selectedCanvasRouteLabel!;
            if (!_pinnedCanvasRoutes.Add(routeLabel))
            {
                _pinnedCanvasRoutes.Remove(routeLabel);
            }

            RefreshCanvasLayoutOnly();
            Status = _pinnedCanvasRoutes.Contains(routeLabel)
                ? $"Pinned route {routeLabel}."
                : $"Unpinned route {routeLabel}.";
            return;
        }

        Status = "Select a node or route before pinning.";
    }

    private void ClearCanvasInteraction()
    {
        ResetCanvasInteractionState(clearPins: true);
        RefreshCanvasLayoutOnly();
        Status = "Canvas interaction reset.";
    }

    private void ClearCanvasSelection()
    {
        if (string.IsNullOrWhiteSpace(_selectedCanvasNodeKey) && string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel))
        {
            return;
        }

        _selectedCanvasNodeKey = null;
        _selectedCanvasRouteLabel = null;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
        RefreshCanvasLayoutOnly();
        UpdateCanvasInteractionSummaries(CanvasNodes, CanvasEdges);
    }

    private void ResetCanvasInteractionState(bool clearPins)
    {
        _selectedCanvasNodeKey = null;
        _selectedCanvasRouteLabel = null;
        _selectedRouteSourceRegionId = null;
        _selectedRouteTargetRegionId = null;
        _hoverCanvasNodeKey = null;
        _hoverCanvasRouteLabel = null;
        IsCanvasSelectionExpanded = false;
        CanvasHoverCardText = string.Empty;
        IsCanvasHoverCardVisible = false;
        if (clearPins)
        {
            _pinnedCanvasNodes.Clear();
            _pinnedCanvasRoutes.Clear();
        }

        ToggleCanvasSelectionExpandedCommand.RaiseCanExecuteChanged();
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
    }

    private bool TrimCanvasInteractionToLayout(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges)
    {
        var changed = false;
        var validNodes = new HashSet<string>(
            nodes.Select(item => item.NodeKey).Where(key => !string.IsNullOrWhiteSpace(key)),
            StringComparer.OrdinalIgnoreCase);
        var validRoutes = new HashSet<string>(
            edges.Select(item => item.RouteLabel).Where(label => !string.IsNullOrWhiteSpace(label)),
            StringComparer.OrdinalIgnoreCase);

        if (!string.IsNullOrWhiteSpace(_selectedCanvasNodeKey) && !validNodes.Contains(_selectedCanvasNodeKey!))
        {
            _selectedCanvasNodeKey = null;
            changed = true;
        }

        if (!string.IsNullOrWhiteSpace(_hoverCanvasNodeKey) && !validNodes.Contains(_hoverCanvasNodeKey!))
        {
            _hoverCanvasNodeKey = null;
            changed = true;
        }

        if (!string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel) && !validRoutes.Contains(_selectedCanvasRouteLabel!))
        {
            _selectedCanvasRouteLabel = null;
            changed = true;
        }

        if (!string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel) && !validRoutes.Contains(_hoverCanvasRouteLabel!))
        {
            _hoverCanvasRouteLabel = null;
            changed = true;
        }

        var pinnedNodesBefore = _pinnedCanvasNodes.Count;
        var pinnedRoutesBefore = _pinnedCanvasRoutes.Count;
        _pinnedCanvasNodes.RemoveWhere(key => !validNodes.Contains(key));
        _pinnedCanvasRoutes.RemoveWhere(route => !validRoutes.Contains(route));
        if (_pinnedCanvasNodes.Count != pinnedNodesBefore || _pinnedCanvasRoutes.Count != pinnedRoutesBefore)
        {
            changed = true;
        }

        return changed;
    }

    private void UpdateCanvasHoverCard(string detail, double pointerX, double pointerY)
    {
        CanvasHoverCardText = detail;

        if (!double.IsNaN(pointerX) && !double.IsNaN(pointerY))
        {
            CanvasHoverCardLeft = Clamp(pointerX + HoverCardOffset, 4.0, Math.Max(4.0, ActivityCanvasWidth - HoverCardMaxWidth));
            CanvasHoverCardTop = Clamp(pointerY + HoverCardOffset, 4.0, Math.Max(4.0, ActivityCanvasHeight - HoverCardMaxHeight));
        }

        IsCanvasHoverCardVisible = !string.IsNullOrWhiteSpace(detail);
    }

    private void RefreshCanvasHoverCard(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges)
    {
        var hoverNode = !string.IsNullOrWhiteSpace(_hoverCanvasNodeKey)
            ? nodes.FirstOrDefault(item => string.Equals(item.NodeKey, _hoverCanvasNodeKey, StringComparison.OrdinalIgnoreCase))
            : null;
        if (hoverNode is not null)
        {
            UpdateCanvasHoverCard(hoverNode.Detail, double.NaN, double.NaN);
            return;
        }

        var hoverEdge = !string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel)
            ? edges.FirstOrDefault(item => string.Equals(item.RouteLabel, _hoverCanvasRouteLabel, StringComparison.OrdinalIgnoreCase))
            : null;
        if (hoverEdge is not null)
        {
            UpdateCanvasHoverCard(hoverEdge.Detail, double.NaN, double.NaN);
            return;
        }

        CanvasHoverCardText = string.Empty;
        IsCanvasHoverCardVisible = false;
    }

    private VizActivityCanvasNode? TryGetSelectedCanvasNode(IReadOnlyList<VizActivityCanvasNode> nodes)
        => !string.IsNullOrWhiteSpace(_selectedCanvasNodeKey)
            ? nodes.FirstOrDefault(item => string.Equals(item.NodeKey, _selectedCanvasNodeKey, StringComparison.OrdinalIgnoreCase))
            : null;

    private VizActivityCanvasEdge? TryGetSelectedCanvasEdge(IReadOnlyList<VizActivityCanvasEdge> edges)
        => !string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel)
            ? edges.FirstOrDefault(item => string.Equals(item.RouteLabel, _selectedCanvasRouteLabel, StringComparison.OrdinalIgnoreCase))
            : null;

    private void UpdateCanvasInteractionSummaries(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges)
    {
        var selectedNode = TryGetSelectedCanvasNode(nodes);
        var selectedEdge = TryGetSelectedCanvasEdge(edges);

        var hoverNode = !string.IsNullOrWhiteSpace(_hoverCanvasNodeKey)
            ? nodes.FirstOrDefault(item => string.Equals(item.NodeKey, _hoverCanvasNodeKey, StringComparison.OrdinalIgnoreCase))
            : null;
        var hoverEdge = !string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel)
            ? edges.FirstOrDefault(item => string.Equals(item.RouteLabel, _hoverCanvasRouteLabel, StringComparison.OrdinalIgnoreCase))
            : null;

        var selectedSummary = selectedNode is not null
            ? $"Selected node {selectedNode.Label} (events {selectedNode.EventCount}, tick {selectedNode.LastTick})"
            : selectedEdge is not null
                ? $"Selected route {selectedEdge.RouteLabel} (events {selectedEdge.EventCount}, tick {selectedEdge.LastTick})"
                : "Selected: none";
        var hoverSummary = hoverNode is not null
            ? $"Hover node {hoverNode.Label} (events {hoverNode.EventCount}, tick {hoverNode.LastTick})"
            : hoverEdge is not null
                ? $"Hover route {hoverEdge.RouteLabel} (events {hoverEdge.EventCount}, tick {hoverEdge.LastTick})"
                : "Hover: none";

        ActivityInteractionSummary = $"{selectedSummary} | {hoverSummary}";
        ActivityPinnedSummary = BuildPinnedSummary(nodes, edges);
        UpdateCanvasSelectionPanelState(selectedNode, selectedEdge);
    }

    private void UpdateCanvasSelectionPanelState(
        VizActivityCanvasNode? selectedNode,
        VizActivityCanvasEdge? selectedEdge)
    {
        _selectedRouteSourceRegionId = selectedEdge?.SourceRegionId;
        _selectedRouteTargetRegionId = selectedEdge?.TargetRegionId;
        OnPropertyChanged(nameof(HasSelectedRouteSource));
        OnPropertyChanged(nameof(HasSelectedRouteTarget));

        HasCanvasSelection = selectedNode is not null || selectedEdge is not null;
        OnPropertyChanged(nameof(IsCanvasSelectionDetailsVisible));
        ToggleCanvasSelectionExpandedCommand.RaiseCanExecuteChanged();
        if (!HasCanvasSelection && IsCanvasSelectionExpanded)
        {
            IsCanvasSelectionExpanded = false;
        }

        if (selectedNode is not null)
        {
            CanvasSelectionTitle = $"Selected Node {selectedNode.Label}";
            CanvasSelectionIdentity = BuildNodeSelectionIdentity(selectedNode);
            CanvasSelectionRuntime = BuildNodeSelectionRuntime(selectedNode);
            CanvasSelectionContext = BuildNodeSelectionContext(selectedNode);
            CanvasSelectionDetail = selectedNode.Detail;
        }
        else if (selectedEdge is not null)
        {
            CanvasSelectionTitle = $"Selected Route {selectedEdge.RouteLabel}";
            CanvasSelectionIdentity = BuildEdgeSelectionIdentity(selectedEdge);
            CanvasSelectionRuntime = BuildEdgeSelectionRuntime(selectedEdge);
            CanvasSelectionContext = BuildEdgeSelectionContext(selectedEdge);
            CanvasSelectionDetail = selectedEdge.Detail;
        }
        else
        {
            CanvasSelectionTitle = "Selected: none";
            CanvasSelectionIdentity = "Identity: none.";
            CanvasSelectionRuntime = "Runtime: n/a.";
            CanvasSelectionContext = "Context: n/a.";
            CanvasSelectionDetail = "Select a node or route to inspect identity, runtime stats, and route context.";
            _selectedRouteSourceRegionId = null;
            _selectedRouteTargetRegionId = null;
        }

        UpdateCanvasSelectionActionHint(selectedNode, selectedEdge);
        RaiseCanvasSelectionActionCanExecuteChanged();
    }

    private void RaiseCanvasSelectionActionCanExecuteChanged()
    {
        FocusSelectedRouteSourceCommand.RaiseCanExecuteChanged();
        FocusSelectedRouteTargetCommand.RaiseCanExecuteChanged();
        PrepareInputPulseCommand.RaiseCanExecuteChanged();
        ApplyRuntimeStateCommand.RaiseCanExecuteChanged();
    }

    private void UpdateCanvasSelectionActionHint(
        VizActivityCanvasNode? selectedNode,
        VizActivityCanvasEdge? selectedEdge)
    {
        if (!HasCanvasSelection)
        {
            CanvasSelectionActionHint = "Select a node or route to view available actions.";
            return;
        }

        var routeHintPrefix = string.Empty;
        if (selectedEdge is not null)
        {
            var sourceText = selectedEdge.SourceRegionId.HasValue ? $"source R{selectedEdge.SourceRegionId.Value}" : "source n/a";
            var targetText = selectedEdge.TargetRegionId.HasValue ? $"target R{selectedEdge.TargetRegionId.Value}" : "target n/a";
            routeHintPrefix = $"Route actions available ({sourceText}, {targetText}). ";
        }

        if (SelectedBrain is null)
        {
            CanvasSelectionActionHint = routeHintPrefix + "Select a brain to enable runtime actions.";
            return;
        }

        if (!TryResolveRuntimeNeuronTargetFromSelection(selectedNode, selectedEdge, out var target, out var targetReason))
        {
            CanvasSelectionActionHint = routeHintPrefix + targetReason;
            return;
        }

        if (!TryParseInputPulseValue(out var pulseValue))
        {
            CanvasSelectionActionHint = "Pulse value must be a finite float.";
            return;
        }

        if (!TryParseOptionalRuntimeStateValue(
                SelectedBufferValueText,
                "Buffer value",
                out var setBuffer,
                out var bufferValue,
                out var bufferReason))
        {
            CanvasSelectionActionHint = bufferReason;
            return;
        }

        if (!TryParseOptionalRuntimeStateValue(
                SelectedAccumulatorValueText,
                "Accumulator value",
                out var setAccumulator,
                out var accumulatorValue,
                out var accumulatorReason))
        {
            CanvasSelectionActionHint = accumulatorReason;
            return;
        }

        string runtimeStateHint;
        if (!setBuffer && !setAccumulator)
        {
            runtimeStateHint = "State write: enter buffer and/or accumulator value (blank skips).";
        }
        else
        {
            var stateParts = new List<string>(2);
            if (setBuffer)
            {
                stateParts.Add(FormattableString.Invariant($"buffer {bufferValue:0.###}"));
            }

            if (setAccumulator)
            {
                stateParts.Add(FormattableString.Invariant($"accumulator {accumulatorValue:0.###}"));
            }

            runtimeStateHint = $"State ready: {string.Join(", ", stateParts)}.";
        }

        CanvasSelectionActionHint = FormattableString.Invariant(
            $"{routeHintPrefix}Ready: pulse R{target.RegionId}/N{target.NeuronId} with value {pulseValue:0.###}. {runtimeStateHint}");
    }

    private bool CanPrepareInputPulseForSelection()
        => TryBuildRuntimePulseRequest(CanvasNodes, CanvasEdges, out _, out _);

    private void PrepareInputPulseForSelection()
    {
        if (!TryBuildRuntimePulseRequest(CanvasNodes, CanvasEdges, out var request, out var reason))
        {
            Status = reason;
            UpdateCanvasSelectionActionHint(TryGetSelectedCanvasNode(CanvasNodes), TryGetSelectedCanvasEdge(CanvasEdges));
            RaiseCanvasSelectionActionCanExecuteChanged();
            return;
        }

        if (_brain.TrySendRuntimeNeuronPulseSelected(request.RegionId, request.NeuronId, request.Value, out var ioStatus))
        {
            Status = ioStatus;
        }
        else
        {
            Status = $"Runtime action failed validation: {ioStatus}";
        }

        UpdateCanvasSelectionActionHint(TryGetSelectedCanvasNode(CanvasNodes), TryGetSelectedCanvasEdge(CanvasEdges));
        RaiseCanvasSelectionActionCanExecuteChanged();
    }

    private bool CanApplyRuntimeStateForSelection()
        => TryBuildRuntimeStateWriteRequest(CanvasNodes, CanvasEdges, out _, out _);

    private void ApplyRuntimeStateForSelection()
    {
        if (!TryBuildRuntimeStateWriteRequest(CanvasNodes, CanvasEdges, out var request, out var reason))
        {
            Status = reason;
            UpdateCanvasSelectionActionHint(TryGetSelectedCanvasNode(CanvasNodes), TryGetSelectedCanvasEdge(CanvasEdges));
            RaiseCanvasSelectionActionCanExecuteChanged();
            return;
        }

        if (_brain.TrySetRuntimeNeuronStateSelected(
                request.RegionId,
                request.NeuronId,
                request.SetBuffer,
                request.BufferValue,
                request.SetAccumulator,
                request.AccumulatorValue,
                out var ioStatus))
        {
            Status = ioStatus;
        }
        else
        {
            Status = $"Runtime action failed validation: {ioStatus}";
        }

        UpdateCanvasSelectionActionHint(TryGetSelectedCanvasNode(CanvasNodes), TryGetSelectedCanvasEdge(CanvasEdges));
        RaiseCanvasSelectionActionCanExecuteChanged();
    }

    private bool TryBuildRuntimePulseRequest(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges,
        out RuntimePulseRequest request,
        out string reason)
    {
        request = default;
        if (!TryBuildRuntimeNeuronTargetRequest(nodes, edges, out var target, out reason))
        {
            return false;
        }

        if (!TryParseInputPulseValue(out var value))
        {
            reason = "Pulse value must be a finite float.";
            return false;
        }

        request = new RuntimePulseRequest(target.RegionId, target.NeuronId, value);
        reason = string.Empty;
        return true;
    }

    private bool TryBuildRuntimeStateWriteRequest(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges,
        out RuntimeStateWriteRequest request,
        out string reason)
    {
        request = default;
        if (!TryBuildRuntimeNeuronTargetRequest(nodes, edges, out var target, out reason))
        {
            return false;
        }

        if (!TryParseOptionalRuntimeStateValue(
                SelectedBufferValueText,
                "Buffer value",
                out var setBuffer,
                out var bufferValue,
                out reason))
        {
            return false;
        }

        if (!TryParseOptionalRuntimeStateValue(
                SelectedAccumulatorValueText,
                "Accumulator value",
                out var setAccumulator,
                out var accumulatorValue,
                out reason))
        {
            return false;
        }

        if (!setBuffer && !setAccumulator)
        {
            reason = "Enter a finite buffer and/or accumulator value.";
            return false;
        }

        request = new RuntimeStateWriteRequest(
            target.RegionId,
            target.NeuronId,
            setBuffer,
            bufferValue,
            setAccumulator,
            accumulatorValue);
        reason = string.Empty;
        return true;
    }

    private bool TryBuildRuntimeNeuronTargetRequest(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges,
        out RuntimeNeuronTargetRequest request,
        out string reason)
    {
        request = default;
        if (SelectedBrain is null)
        {
            reason = "Select a brain to enable runtime actions.";
            return false;
        }

        if (TryResolveRuntimeNeuronTargetRequest(nodes, edges, out request, out reason))
        {
            return true;
        }

        return false;
    }

    private static bool TryResolveRuntimeNeuronTargetFromSelection(
        VizActivityCanvasNode? selectedNode,
        VizActivityCanvasEdge? selectedEdge,
        out RuntimeNeuronTargetRequest request,
        out string reason)
    {
        request = default;
        if (selectedNode is not null)
        {
            if (selectedNode.NeuronId.HasValue)
            {
                request = new RuntimeNeuronTargetRequest(selectedNode.RegionId, selectedNode.NeuronId.Value);
                reason = string.Empty;
                return true;
            }

            reason = "Selected node is aggregate-only. Select a neuron node (R#/N#) for runtime actions.";
            return false;
        }

        if (selectedEdge is not null)
        {
            if (TryResolveRuntimeNeuronTargetFromEdge(selectedEdge, out request))
            {
                reason = string.Empty;
                return true;
            }

            reason = "Selected route does not resolve to a neuron endpoint. Focus a region and select a route containing N#.";
            return false;
        }

        reason = "Select a neuron node (R#/N#) or a route endpoint (N#) to stage runtime actions.";
        return false;
    }

    private bool TryResolveRuntimeNeuronTargetRequest(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges,
        out RuntimeNeuronTargetRequest request,
        out string reason)
    {
        var selectedNode = TryGetSelectedCanvasNode(nodes);
        var selectedEdge = TryGetSelectedCanvasEdge(edges);
        return TryResolveRuntimeNeuronTargetFromSelection(selectedNode, selectedEdge, out request, out reason);
    }

    private static bool TryResolveRuntimeNeuronTargetFromEdge(
        VizActivityCanvasEdge edge,
        out RuntimeNeuronTargetRequest request)
    {
        request = default;

        if (string.IsNullOrWhiteSpace(edge.RouteLabel))
        {
            return false;
        }

        var separatorIndex = edge.RouteLabel.IndexOf("->", StringComparison.Ordinal);
        if (separatorIndex <= 0 || separatorIndex >= edge.RouteLabel.Length - 2)
        {
            return false;
        }

        var sourceToken = edge.RouteLabel[..separatorIndex].Trim();
        var targetToken = edge.RouteLabel[(separatorIndex + 2)..].Trim();

        if (edge.SourceRegionId.HasValue && TryParseNeuronRouteToken(sourceToken, out var sourceNeuronId))
        {
            request = new RuntimeNeuronTargetRequest(edge.SourceRegionId.Value, sourceNeuronId);
            return true;
        }

        if (edge.TargetRegionId.HasValue && TryParseNeuronRouteToken(targetToken, out var targetNeuronId))
        {
            request = new RuntimeNeuronTargetRequest(edge.TargetRegionId.Value, targetNeuronId);
            return true;
        }

        return false;
    }

    private static bool TryParseNeuronRouteToken(string token, out uint neuronId)
    {
        neuronId = 0;
        if (string.IsNullOrWhiteSpace(token))
        {
            return false;
        }

        var trimmed = token.Trim();
        if (trimmed.StartsWith("N", StringComparison.OrdinalIgnoreCase))
        {
            return uint.TryParse(trimmed.AsSpan(1), NumberStyles.Integer, CultureInfo.InvariantCulture, out neuronId);
        }

        var slashNeuron = trimmed.IndexOf("/N", StringComparison.OrdinalIgnoreCase);
        if (slashNeuron >= 0)
        {
            return uint.TryParse(trimmed.AsSpan(slashNeuron + 2), NumberStyles.Integer, CultureInfo.InvariantCulture, out neuronId);
        }

        return false;
    }

    private static bool TryParseOptionalRuntimeStateValue(
        string valueText,
        string label,
        out bool hasValue,
        out float value,
        out string reason)
    {
        hasValue = false;
        value = 0f;
        reason = string.Empty;

        if (string.IsNullOrWhiteSpace(valueText))
        {
            return true;
        }

        if (!float.TryParse(valueText, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            || !float.IsFinite(parsed))
        {
            reason = $"{label} must be a finite float (or blank to skip).";
            return false;
        }

        hasValue = true;
        value = parsed;
        reason = string.Empty;
        return true;
    }

    private bool TryParseInputPulseValue(out float value)
    {
        value = 0f;
        if (!float.TryParse(SelectedInputPulseValueText, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            || !float.IsFinite(parsed))
        {
            return false;
        }

        value = parsed;
        return true;
    }

    private void FocusSelectedRouteSourceRegion()
    {
        if (!_selectedRouteSourceRegionId.HasValue)
        {
            Status = "Selected route does not expose a source region.";
            return;
        }

        FocusCanvasRegion(_selectedRouteSourceRegionId.Value, "Route source focus");
    }

    private void FocusSelectedRouteTargetRegion()
    {
        if (!_selectedRouteTargetRegionId.HasValue)
        {
            Status = "Selected route does not expose a target region.";
            return;
        }

        FocusCanvasRegion(_selectedRouteTargetRegionId.Value, "Route target focus");
    }

    private void FocusCanvasRegion(uint regionId, string origin)
    {
        RegionFocusText = regionId.ToString(CultureInfo.InvariantCulture);
        RefreshFilteredEvents();
        if (SelectedBrain is not null)
        {
            QueueDefinitionTopologyHydration(SelectedBrain.BrainId, regionId);
        }

        Status = $"{origin}: region {regionId}.";
    }

    private static string BuildNodeSelectionIdentity(VizActivityCanvasNode node)
    {
        var neuronText = node.NeuronId.HasValue ? $"N{node.NeuronId.Value}" : "aggregate";
        return $"Identity: key={node.NodeKey} | region=R{node.RegionId} | neuron={neuronText}.";
    }

    private static string BuildNodeSelectionRuntime(VizActivityCanvasNode node)
        => $"Runtime: events={node.EventCount}, last_tick={node.LastTick}, focused={(node.IsFocused ? "yes" : "no")}, pinned={(node.IsPinned ? "yes" : "no")}.";

    private static string BuildNodeSelectionContext(VizActivityCanvasNode node)
        => FormattableString.Invariant($"Context: focus_region=R{node.NavigateRegionId}, canvas=({node.Left:0.#},{node.Top:0.#}), diameter={node.Diameter:0.#}.");

    private static string BuildEdgeSelectionIdentity(VizActivityCanvasEdge edge)
    {
        var sourceText = edge.SourceRegionId.HasValue ? $"R{edge.SourceRegionId.Value}" : "n/a";
        var targetText = edge.TargetRegionId.HasValue ? $"R{edge.TargetRegionId.Value}" : "n/a";
        return $"Identity: route={edge.RouteLabel} | source={sourceText} | target={targetText}.";
    }

    private static string BuildEdgeSelectionRuntime(VizActivityCanvasEdge edge)
        => $"Runtime: events={edge.EventCount}, last_tick={edge.LastTick}, focused={(edge.IsFocused ? "yes" : "no")}, pinned={(edge.IsPinned ? "yes" : "no")}.";

    private static string BuildEdgeSelectionContext(VizActivityCanvasEdge edge)
    {
        var relation = edge.SourceRegionId.HasValue && edge.TargetRegionId.HasValue && edge.SourceRegionId.Value == edge.TargetRegionId.Value
            ? "intra-region"
            : "cross-region";
        return FormattableString.Invariant(
            $"Context: relation={relation}, path src({edge.SourceX:0.#},{edge.SourceY:0.#}) ctrl({edge.ControlX:0.#},{edge.ControlY:0.#}) dst({edge.TargetX:0.#},{edge.TargetY:0.#}).");
    }

    private static string BuildPinnedSummary(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges)
    {
        var pinnedRegions = nodes
            .Where(item => item.IsPinned)
            .OrderBy(item => item.RegionId)
            .Select(item => item.Label)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Take(6)
            .ToList();
        var pinnedRoutes = edges
            .Where(item => item.IsPinned)
            .Select(item => item.RouteLabel)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Take(3)
            .ToList();

        var pinnedRegionCount = nodes.Count(item => item.IsPinned);
        var pinnedRouteCount = edges.Count(item => item.IsPinned);
        if (pinnedRegionCount == 0 && pinnedRouteCount == 0)
        {
            return "Pinned: none.";
        }

        var regionSuffix = pinnedRegionCount > pinnedRegions.Count ? $" (+{pinnedRegionCount - pinnedRegions.Count})" : string.Empty;
        var routeSuffix = pinnedRouteCount > pinnedRoutes.Count ? $" (+{pinnedRouteCount - pinnedRoutes.Count})" : string.Empty;
        var regionText = pinnedRegions.Count == 0 ? "none" : string.Join(", ", pinnedRegions) + regionSuffix;
        var routeText = pinnedRoutes.Count == 0 ? "none" : string.Join(" | ", pinnedRoutes) + routeSuffix;
        return $"Pinned nodes: {regionText} | routes: {routeText}";
    }

    private void CancelPendingHoverClear()
    {
        Interlocked.Increment(ref _hoverClearRevision);
    }

    private void RebuildCanvasHitIndex(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges)
    {
        _canvasNodeSnapshot = nodes;
        _canvasEdgeSnapshot = edges;
        _nodeHitIndex.Clear();
        _edgeHitIndex.Clear();
        _nodeHitCandidates.Clear();
        _edgeHitCandidates.Clear();
        _canvasNodeByKey.Clear();
        _canvasEdgeByRoute.Clear();

        for (var index = 0; index < nodes.Count; index++)
        {
            var node = nodes[index];
            if (!string.IsNullOrWhiteSpace(node.NodeKey))
            {
                _canvasNodeByKey[node.NodeKey] = node;
            }

            AddIndexEntry(
                _nodeHitIndex,
                index,
                node.Left - NodeHitPadding,
                node.Top - NodeHitPadding,
                node.Left + node.Diameter + NodeHitPadding,
                node.Top + node.Diameter + NodeHitPadding);
        }

        for (var index = 0; index < edges.Count; index++)
        {
            var edge = edges[index];
            if (!string.IsNullOrWhiteSpace(edge.RouteLabel))
            {
                _canvasEdgeByRoute[edge.RouteLabel] = edge;
            }

            var threshold = Math.Max(4.0, edge.HitTestThickness * 0.5) + EdgeHitIndexPadding;
            var minX = Math.Min(edge.SourceX, Math.Min(edge.ControlX, edge.TargetX)) - threshold;
            var minY = Math.Min(edge.SourceY, Math.Min(edge.ControlY, edge.TargetY)) - threshold;
            var maxX = Math.Max(edge.SourceX, Math.Max(edge.ControlX, edge.TargetX)) + threshold;
            var maxY = Math.Max(edge.SourceY, Math.Max(edge.ControlY, edge.TargetY)) + threshold;

            AddIndexEntry(_edgeHitIndex, index, minX, minY, maxX, maxY);
        }
    }

    private static void AddIndexEntry(
        IDictionary<long, List<int>> index,
        int itemIndex,
        double minX,
        double minY,
        double maxX,
        double maxY)
    {
        if (!double.IsFinite(minX)
            || !double.IsFinite(minY)
            || !double.IsFinite(maxX)
            || !double.IsFinite(maxY))
        {
            return;
        }

        var startX = (int)Math.Floor(minX / HitTestCellSize);
        var endX = (int)Math.Floor(maxX / HitTestCellSize);
        var startY = (int)Math.Floor(minY / HitTestCellSize);
        var endY = (int)Math.Floor(maxY / HitTestCellSize);

        for (var cellX = startX; cellX <= endX; cellX++)
        {
            for (var cellY = startY; cellY <= endY; cellY++)
            {
                var key = CellKey(cellX, cellY);
                if (!index.TryGetValue(key, out var entries))
                {
                    entries = new List<int>(4);
                    index[key] = entries;
                }

                entries.Add(itemIndex);
            }
        }
    }

    private static void CollectHitCandidates(
        IReadOnlyDictionary<long, List<int>> index,
        double pointerX,
        double pointerY,
        ISet<int> candidates)
    {
        candidates.Clear();

        var cellX = (int)Math.Floor(pointerX / HitTestCellSize);
        var cellY = (int)Math.Floor(pointerY / HitTestCellSize);
        for (var dx = -1; dx <= 1; dx++)
        {
            for (var dy = -1; dy <= 1; dy++)
            {
                if (!index.TryGetValue(CellKey(cellX + dx, cellY + dy), out var entries))
                {
                    continue;
                }

                foreach (var indexValue in entries)
                {
                    candidates.Add(indexValue);
                }
            }
        }
    }

    private static long CellKey(int cellX, int cellY)
        => ((long)cellX << 32) ^ (uint)cellY;

    private VizActivityCanvasNode? HitTestCanvasNodeInsideCircle(double pointerX, double pointerY)
        => HitTestCanvasNode(pointerX, pointerY, nodeHitPadding: 0);

    private bool TryResolveCanvasHitCore(
        double pointerX,
        double pointerY,
        double stickyNodePadding,
        double stickyEdgePadding,
        double nodeHitPadding,
        double edgeHitThresholdScale,
        double edgeHitThresholdMin,
        out VizActivityCanvasNode? node,
        out VizActivityCanvasEdge? edge)
    {
        if (TryResolveStickyHoverHit(
                pointerX,
                pointerY,
                stickyNodePadding,
                stickyEdgePadding,
                edgeHitThresholdScale,
                edgeHitThresholdMin,
                out node,
                out edge))
        {
            return PreferNodeHitOverEdge(pointerX, pointerY, ref node, ref edge);
        }

        node = HitTestCanvasNode(pointerX, pointerY, nodeHitPadding);
        if (node is not null)
        {
            edge = null;
            return true;
        }

        edge = HitTestCanvasEdge(pointerX, pointerY, edgeHitThresholdScale, edgeHitThresholdMin);
        return edge is not null;
    }

    private bool PreferNodeHitOverEdge(
        double pointerX,
        double pointerY,
        ref VizActivityCanvasNode? node,
        ref VizActivityCanvasEdge? edge)
    {
        if (edge is null)
        {
            return true;
        }

        var nodeInsideCircle = HitTestCanvasNodeInsideCircle(pointerX, pointerY);
        if (nodeInsideCircle is null)
        {
            return true;
        }

        node = nodeInsideCircle;
        edge = null;
        return true;
    }

    private bool TryResolveStickyHoverHit(
        double pointerX,
        double pointerY,
        double stickyNodePadding,
        double stickyEdgePadding,
        double edgeHitThresholdScale,
        double edgeHitThresholdMin,
        out VizActivityCanvasNode? node,
        out VizActivityCanvasEdge? edge)
    {
        if (!string.IsNullOrWhiteSpace(_hoverCanvasNodeKey)
            && _canvasNodeByKey.TryGetValue(_hoverCanvasNodeKey!, out var hoveredNode))
        {
            var centerX = hoveredNode.Left + (hoveredNode.Diameter / 2.0);
            var centerY = hoveredNode.Top + (hoveredNode.Diameter / 2.0);
            var radius = (hoveredNode.Diameter / 2.0) + stickyNodePadding;
            var dx = pointerX - centerX;
            var dy = pointerY - centerY;
            if ((dx * dx) + (dy * dy) <= radius * radius)
            {
                node = hoveredNode;
                edge = null;
                return true;
            }
        }

        if (!string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel)
            && _canvasEdgeByRoute.TryGetValue(_hoverCanvasRouteLabel!, out var hoveredEdge))
        {
            var threshold = Math.Max(edgeHitThresholdMin, hoveredEdge.HitTestThickness * edgeHitThresholdScale) + stickyEdgePadding;
            var distance = DistanceToQuadraticBezier(
                pointerX,
                pointerY,
                hoveredEdge.SourceX,
                hoveredEdge.SourceY,
                hoveredEdge.ControlX,
                hoveredEdge.ControlY,
                hoveredEdge.TargetX,
                hoveredEdge.TargetY);
            if (distance <= threshold)
            {
                node = null;
                edge = hoveredEdge;
                return true;
            }
        }

        node = null;
        edge = null;
        return false;
    }

    private VizActivityCanvasNode? HitTestCanvasNode(double pointerX, double pointerY, double nodeHitPadding)
    {
        if (_canvasNodeSnapshot.Count == 0)
        {
            return null;
        }

        CollectHitCandidates(_nodeHitIndex, pointerX, pointerY, _nodeHitCandidates);
        var shouldFallbackToFullScan = _nodeHitCandidates.Count == 0 && _canvasNodeSnapshot.Count <= 10;

        VizActivityCanvasNode? best = null;
        var bestDistance = double.MaxValue;
        if (_nodeHitCandidates.Count > 0)
        {
            foreach (var index in _nodeHitCandidates)
            {
                var node = _canvasNodeSnapshot[index];
                var centerX = node.Left + (node.Diameter / 2.0);
                var centerY = node.Top + (node.Diameter / 2.0);
                var radius = (node.Diameter / 2.0) + nodeHitPadding;
                var dx = pointerX - centerX;
                var dy = pointerY - centerY;
                var distanceSquared = (dx * dx) + (dy * dy);
                var radiusSquared = radius * radius;
                if (distanceSquared > radiusSquared)
                {
                    continue;
                }

                if (best is null
                    || distanceSquared < (bestDistance - HitDistanceTieEpsilon)
                    || (Math.Abs(distanceSquared - bestDistance) <= HitDistanceTieEpsilon
                        && string.Compare(node.NodeKey, best.NodeKey, StringComparison.OrdinalIgnoreCase) < 0))
                {
                    bestDistance = distanceSquared;
                    best = node;
                }
            }

            return best;
        }

        if (!shouldFallbackToFullScan)
        {
            return null;
        }

        foreach (var node in _canvasNodeSnapshot)
        {
            var centerX = node.Left + (node.Diameter / 2.0);
            var centerY = node.Top + (node.Diameter / 2.0);
            var radius = (node.Diameter / 2.0) + nodeHitPadding;
            var dx = pointerX - centerX;
            var dy = pointerY - centerY;
            var distanceSquared = (dx * dx) + (dy * dy);
            var radiusSquared = radius * radius;
            if (distanceSquared > radiusSquared)
            {
                continue;
            }

            if (best is null
                || distanceSquared < (bestDistance - HitDistanceTieEpsilon)
                || (Math.Abs(distanceSquared - bestDistance) <= HitDistanceTieEpsilon
                    && string.Compare(node.NodeKey, best.NodeKey, StringComparison.OrdinalIgnoreCase) < 0))
            {
                bestDistance = distanceSquared;
                best = node;
            }
        }

        return best;
    }

    private VizActivityCanvasEdge? HitTestCanvasEdge(
        double pointerX,
        double pointerY,
        double edgeHitThresholdScale,
        double edgeHitThresholdMin)
    {
        if (_canvasEdgeSnapshot.Count == 0)
        {
            return null;
        }

        CollectHitCandidates(_edgeHitIndex, pointerX, pointerY, _edgeHitCandidates);
        var shouldFallbackToFullScan = _edgeHitCandidates.Count == 0 && _canvasEdgeSnapshot.Count <= 6;

        VizActivityCanvasEdge? best = null;
        var bestDistance = double.MaxValue;
        if (_edgeHitCandidates.Count > 0)
        {
            foreach (var index in _edgeHitCandidates)
            {
                var edge = _canvasEdgeSnapshot[index];
                var threshold = Math.Max(edgeHitThresholdMin, edge.HitTestThickness * edgeHitThresholdScale);
                var distance = DistanceToQuadraticBezier(
                    pointerX,
                    pointerY,
                    edge.SourceX,
                    edge.SourceY,
                    edge.ControlX,
                    edge.ControlY,
                    edge.TargetX,
                    edge.TargetY);
                if (distance > threshold)
                {
                    continue;
                }

                if (best is null
                    || distance < (bestDistance - HitDistanceTieEpsilon)
                    || (Math.Abs(distance - bestDistance) <= HitDistanceTieEpsilon
                        && string.Compare(edge.RouteLabel, best.RouteLabel, StringComparison.OrdinalIgnoreCase) < 0))
                {
                    bestDistance = distance;
                    best = edge;
                }
            }

            return best;
        }

        if (!shouldFallbackToFullScan)
        {
            return null;
        }

        foreach (var edge in _canvasEdgeSnapshot)
        {
            var threshold = Math.Max(edgeHitThresholdMin, edge.HitTestThickness * edgeHitThresholdScale);
            var distance = DistanceToQuadraticBezier(
                pointerX,
                pointerY,
                edge.SourceX,
                edge.SourceY,
                edge.ControlX,
                edge.ControlY,
                edge.TargetX,
                edge.TargetY);
            if (distance > threshold)
            {
                continue;
            }

            if (best is null
                || distance < (bestDistance - HitDistanceTieEpsilon)
                || (Math.Abs(distance - bestDistance) <= HitDistanceTieEpsilon
                    && string.Compare(edge.RouteLabel, best.RouteLabel, StringComparison.OrdinalIgnoreCase) < 0))
            {
                bestDistance = distance;
                best = edge;
            }
        }

        return best;
    }

    private static double DistanceToQuadraticBezier(
        double pointX,
        double pointY,
        double startX,
        double startY,
        double controlX,
        double controlY,
        double endX,
        double endY)
    {
        var minDistance = double.MaxValue;
        var previousX = startX;
        var previousY = startY;
        for (var sample = 1; sample <= EdgeHitSamples; sample++)
        {
            var t = (double)sample / EdgeHitSamples;
            var oneMinusT = 1.0 - t;
            var curveX = (oneMinusT * oneMinusT * startX) + (2.0 * oneMinusT * t * controlX) + (t * t * endX);
            var curveY = (oneMinusT * oneMinusT * startY) + (2.0 * oneMinusT * t * controlY) + (t * t * endY);
            var distance = DistanceToSegment(pointX, pointY, previousX, previousY, curveX, curveY);
            if (distance < minDistance)
            {
                minDistance = distance;
            }

            previousX = curveX;
            previousY = curveY;
        }

        return minDistance;
    }

    private static double DistanceToSegment(
        double pointX,
        double pointY,
        double startX,
        double startY,
        double endX,
        double endY)
    {
        var dx = endX - startX;
        var dy = endY - startY;
        var lengthSquared = (dx * dx) + (dy * dy);
        if (lengthSquared <= double.Epsilon)
        {
            var fx = pointX - startX;
            var fy = pointY - startY;
            return Math.Sqrt((fx * fx) + (fy * fy));
        }

        var projection = ((pointX - startX) * dx + (pointY - startY) * dy) / lengthSquared;
        var clamped = Math.Max(0.0, Math.Min(1.0, projection));
        var closestX = startX + (clamped * dx);
        var closestY = startY + (clamped * dy);
        var diffX = pointX - closestX;
        var diffY = pointY - closestY;
        return Math.Sqrt((diffX * diffX) + (diffY * diffY));
    }

    private void RecordHitTestDuration(long startTimestamp)
    {
        var elapsedMs = StopwatchElapsedMs(startTimestamp);
        _lastHitTestMs = elapsedMs;
        _hitTestSamples++;
        _avgHitTestMs += (elapsedMs - _avgHitTestMs) / _hitTestSamples;
        if (elapsedMs > _maxHitTestMs)
        {
            _maxHitTestMs = elapsedMs;
        }
    }

    private static double StopwatchElapsedMs(long startTimestamp)
        => ((double)(Stopwatch.GetTimestamp() - startTimestamp) * 1000.0) / Stopwatch.Frequency;

    private bool IsCurrentSelectionPinned()
    {
        if (!string.IsNullOrWhiteSpace(_selectedCanvasNodeKey))
        {
            return _pinnedCanvasNodes.Contains(_selectedCanvasNodeKey!);
        }

        return !string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel)
               && _pinnedCanvasRoutes.Contains(_selectedCanvasRouteLabel!);
    }

    private uint? GetCurrentSelectionRegionId(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges)
    {
        if (!string.IsNullOrWhiteSpace(_selectedCanvasNodeKey))
        {
            var selectedNode = nodes.FirstOrDefault(item => string.Equals(item.NodeKey, _selectedCanvasNodeKey, StringComparison.OrdinalIgnoreCase));
            return selectedNode?.NavigateRegionId;
        }

        if (string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel))
        {
            return null;
        }

        var selectedEdge = edges.FirstOrDefault(item => string.Equals(item.RouteLabel, _selectedCanvasRouteLabel, StringComparison.OrdinalIgnoreCase));
        return selectedEdge?.TargetRegionId ?? selectedEdge?.SourceRegionId;
    }

    private static uint ComposeAddressForTopology(uint regionId, uint neuronId)
        => (regionId << NbnConstants.AddressNeuronBits) | (neuronId & NbnConstants.AddressNeuronMask);

    private static bool TryParseRegionForTopology(string? value, out uint regionId)
    {
        regionId = 0;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        if (uint.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedNumeric))
        {
            if (parsedNumeric > NbnConstants.RegionMaxId)
            {
                return false;
            }

            regionId = parsedNumeric;
            return true;
        }

        if (!TryParseRegionToken(value, out var parsedToken, out _))
        {
            return false;
        }

        regionId = parsedToken;
        return true;
    }

    private static bool TryParseAddressForTopology(string? value, out uint address)
    {
        address = 0;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        if (uint.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            var parsedRegion = parsed >> NbnConstants.AddressNeuronBits;
            if (parsedRegion > NbnConstants.RegionMaxId)
            {
                return false;
            }

            address = parsed;
            return true;
        }

        if (!TryParseRegionToken(value, out var regionId, out var remainder)
            || string.IsNullOrWhiteSpace(remainder)
            || (remainder[0] != 'N' && remainder[0] != 'n'))
        {
            return false;
        }

        var neuronText = remainder[1..];
        if (!uint.TryParse(neuronText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var neuronId))
        {
            return false;
        }

        address = (regionId << NbnConstants.AddressNeuronBits) | (neuronId & NbnConstants.AddressNeuronMask);
        return true;
    }

    private static bool TryParseRegionToken(string? value, out uint regionId, out string remainder)
    {
        regionId = 0;
        remainder = string.Empty;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        if (trimmed.Length < 2 || (trimmed[0] != 'R' && trimmed[0] != 'r'))
        {
            return false;
        }

        var end = 1;
        while (end < trimmed.Length && char.IsDigit(trimmed[end]))
        {
            end++;
        }

        if (end == 1)
        {
            return false;
        }

        var number = trimmed[1..end];
        if (!uint.TryParse(number, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            || parsed > NbnConstants.RegionMaxId)
        {
            return false;
        }

        regionId = parsed;
        remainder = end < trimmed.Length ? trimmed[end..] : string.Empty;
        return true;
    }
}
