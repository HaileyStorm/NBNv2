using System;
using System.Collections.Generic;
using System.Diagnostics;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Input;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tools.Workbench.Views.Panels;

public partial class VizPanel : UserControl
{
    private const double HoverHitTestMinIntervalMs = 16;
    private const double HoverHitTestMinMovePx = 1.2;
    private const double PressProbeDistancePx = 5.0;
    private const int HoverTargetSwitchSamples = 2;
    private const int HoverTargetClearSamples = 4;
    private const int HoverExitClearDelayMs = 220;
    private const double HoverNoHitRetentionDistancePx = 8.0;
    private static readonly Point[] HoverProbeOffsets =
    {
        new(0, 0)
    };
    private static readonly Point[] PressProbeOffsets =
    {
        new(0, 0),
        new(PressProbeDistancePx, 0),
        new(-PressProbeDistancePx, 0),
        new(0, PressProbeDistancePx),
        new(0, -PressProbeDistancePx),
        new(PressProbeDistancePx * 0.6, PressProbeDistancePx * 0.6),
        new(-PressProbeDistancePx * 0.6, PressProbeDistancePx * 0.6),
        new(PressProbeDistancePx * 0.6, -PressProbeDistancePx * 0.6),
        new(-PressProbeDistancePx * 0.6, -PressProbeDistancePx * 0.6)
    };
    private long _lastHoverHitTestTimestamp;
    private Point _lastHoverHitTestPoint;
    private bool _hasHoverHitTestPoint;
    private string _hoverCommittedSignature = string.Empty;
    private string _hoverCandidateSignature = string.Empty;
    private int _hoverCandidateSamples;
    private VizActivityCanvasNode? _hoverCandidateNode;
    private VizActivityCanvasEdge? _hoverCandidateEdge;
    private Point _lastCommittedHoverPoint;
    private bool _hasCommittedHoverPoint;

    public VizPanel()
    {
        InitializeComponent();
    }

    private VizPanelViewModel? ViewModel => DataContext as VizPanelViewModel;

    private void ActivityCanvasPointerMoved(object? sender, PointerEventArgs e)
    {
        if (ViewModel is null || sender is not Visual visual)
        {
            return;
        }

        var point = e.GetPosition(visual);
        if (!ShouldProcessHoverPointerMove(point))
        {
            if (!string.IsNullOrEmpty(_hoverCommittedSignature))
            {
                if (TryResolveCanvasHitWithProbe(point, HoverProbeOffsets, hoverMode: true, out _, out _))
                {
                    ViewModel.KeepCanvasHoverAlive();
                }
                else
                {
                    ViewModel.ClearCanvasHoverDeferred(HoverExitClearDelayMs);
                }
            }

            return;
        }

        if (!TryResolveCanvasHitWithProbe(point, HoverProbeOffsets, hoverMode: true, out var node, out var edge))
        {
            node = null;
            edge = null;
        }

        ApplyHoverSample(point, node, edge);
    }

    private void ActivityCanvasPointerPressed(object? sender, PointerPressedEventArgs e)
    {
        if (ViewModel is null || sender is not Visual visual || e.Handled)
        {
            return;
        }

        var point = e.GetPosition(visual);
        var pointer = e.GetCurrentPoint(visual).Properties;
        var hasHit = TryResolveCanvasHitWithProbe(point, PressProbeOffsets, hoverMode: false, out var node, out var edge);
        if (!hasHit)
        {
            if (ViewModel.TrySelectHoveredCanvasItem(pointer.IsRightButtonPressed))
            {
                e.Handled = true;
            }

            return;
        }
        if (node is not null)
        {
            ViewModel.SetCanvasNodeHover(node, point.X, point.Y);
            if (pointer.IsRightButtonPressed)
            {
                ViewModel.TogglePinCanvasNode(node);
            }
            else
            {
                ViewModel.SelectCanvasNode(node);
            }

            e.Handled = true;
            return;
        }

        if (edge is null)
        {
            return;
        }

        if (pointer.IsRightButtonPressed)
        {
            ViewModel.SetCanvasEdgeHover(edge, point.X, point.Y);
            ViewModel.TogglePinCanvasEdge(edge);
        }
        else
        {
            ViewModel.SetCanvasEdgeHover(edge, point.X, point.Y);
            ViewModel.SelectCanvasEdge(edge);
        }

        e.Handled = true;
    }

    private void ActivityCanvasPointerExited(object? sender, PointerEventArgs e)
    {
        if (ViewModel is not null && sender is Visual visual)
        {
            var point = e.GetPosition(visual);
            var inside = point.X >= 0
                && point.Y >= 0
                && point.X <= visual.Bounds.Width
                && point.Y <= visual.Bounds.Height;
            if (inside
                && TryResolveCanvasHitWithProbe(point, HoverProbeOffsets, hoverMode: true, out var node, out var edge))
            {
                ApplyHoverSample(point, node, edge);
                return;
            }
        }

        _hasHoverHitTestPoint = false;
        ResetHoverStability();
        ViewModel?.ClearCanvasHoverDeferred(HoverExitClearDelayMs);
    }

    private bool ShouldProcessHoverPointerMove(Point point)
    {
        var now = Stopwatch.GetTimestamp();
        if (!_hasHoverHitTestPoint)
        {
            _hasHoverHitTestPoint = true;
            _lastHoverHitTestTimestamp = now;
            _lastHoverHitTestPoint = point;
            return true;
        }

        var elapsedMs = ((double)(now - _lastHoverHitTestTimestamp) * 1000.0) / Stopwatch.Frequency;
        var dx = point.X - _lastHoverHitTestPoint.X;
        var dy = point.Y - _lastHoverHitTestPoint.Y;
        var movedEnough = ((dx * dx) + (dy * dy)) >= (HoverHitTestMinMovePx * HoverHitTestMinMovePx);
        if (elapsedMs < HoverHitTestMinIntervalMs && !movedEnough)
        {
            return false;
        }

        _lastHoverHitTestTimestamp = now;
        _lastHoverHitTestPoint = point;
        return true;
    }

    private bool TryResolveCanvasHitWithProbe(
        Point point,
        IReadOnlyList<Point> probeOffsets,
        bool hoverMode,
        out VizActivityCanvasNode? node,
        out VizActivityCanvasEdge? edge)
    {
        node = null;
        edge = null;
        if (ViewModel is null)
        {
            return false;
        }

        foreach (var offset in probeOffsets)
        {
            var hasHit = hoverMode
                ? ViewModel.TryResolveCanvasHoverHit(point.X + offset.X, point.Y + offset.Y, out node, out edge)
                : ViewModel.TryResolveCanvasHit(point.X + offset.X, point.Y + offset.Y, out node, out edge);
            if (hasHit)
            {
                return true;
            }
        }

        return false;
    }

    private void ApplyHoverSample(Point pointer, VizActivityCanvasNode? node, VizActivityCanvasEdge? edge)
    {
        if (ViewModel is null)
        {
            return;
        }

        var signature = BuildHoverSignature(node, edge);
        if (!string.Equals(_hoverCandidateSignature, signature, StringComparison.Ordinal))
        {
            _hoverCandidateSignature = signature;
            _hoverCandidateSamples = 1;
        }
        else
        {
            _hoverCandidateSamples++;
        }

        _hoverCandidateNode = node;
        _hoverCandidateEdge = edge;

        if (string.Equals(_hoverCommittedSignature, signature, StringComparison.Ordinal))
        {
            ApplyHoverResolution(pointer, node, edge, signature);
            return;
        }

        var requiredSamples = GetRequiredHoverSamples(signature);
        if (_hoverCandidateSamples < requiredSamples)
        {
            if (string.IsNullOrEmpty(signature))
            {
                ViewModel.ClearCanvasHoverDeferred(HoverExitClearDelayMs);
            }
            else
            {
                ViewModel.KeepCanvasHoverAlive();
            }

            return;
        }

        ApplyHoverResolution(pointer, _hoverCandidateNode, _hoverCandidateEdge, signature);
    }

    private void ApplyHoverResolution(
        Point pointer,
        VizActivityCanvasNode? node,
        VizActivityCanvasEdge? edge,
        string signature)
    {
        if (ViewModel is null)
        {
            return;
        }

        if (node is not null)
        {
            _hoverCommittedSignature = signature;
            _lastCommittedHoverPoint = pointer;
            _hasCommittedHoverPoint = true;
            ViewModel.SetCanvasNodeHover(node, pointer.X, pointer.Y);
            return;
        }

        if (edge is not null)
        {
            _hoverCommittedSignature = signature;
            _lastCommittedHoverPoint = pointer;
            _hasCommittedHoverPoint = true;
            ViewModel.SetCanvasEdgeHover(edge, pointer.X, pointer.Y);
            return;
        }

        if (ShouldRetainCommittedHover(pointer))
        {
            ViewModel.ClearCanvasHoverDeferred(HoverExitClearDelayMs);
            return;
        }

        _hoverCommittedSignature = string.Empty;
        _hasCommittedHoverPoint = false;
        ViewModel.ClearCanvasHoverDeferred();
    }

    private bool ShouldRetainCommittedHover(Point pointer)
    {
        if (string.IsNullOrEmpty(_hoverCommittedSignature) || !_hasCommittedHoverPoint)
        {
            return false;
        }

        var dx = pointer.X - _lastCommittedHoverPoint.X;
        var dy = pointer.Y - _lastCommittedHoverPoint.Y;
        return ((dx * dx) + (dy * dy)) <= (HoverNoHitRetentionDistancePx * HoverNoHitRetentionDistancePx);
    }

    private int GetRequiredHoverSamples(string signature)
    {
        if (string.IsNullOrEmpty(_hoverCommittedSignature))
        {
            return 1;
        }

        return string.IsNullOrEmpty(signature)
            ? HoverTargetClearSamples
            : HoverTargetSwitchSamples;
    }

    private void ResetHoverStability()
    {
        _hoverCommittedSignature = string.Empty;
        _hoverCandidateSignature = string.Empty;
        _hoverCandidateSamples = 0;
        _hoverCandidateNode = null;
        _hoverCandidateEdge = null;
        _hasCommittedHoverPoint = false;
    }

    private static string BuildHoverSignature(VizActivityCanvasNode? node, VizActivityCanvasEdge? edge)
    {
        if (node is not null && !string.IsNullOrWhiteSpace(node.NodeKey))
        {
            return $"node:{node.NodeKey}";
        }

        if (edge is not null && !string.IsNullOrWhiteSpace(edge.RouteLabel))
        {
            return $"edge:{edge.RouteLabel}";
        }

        return string.Empty;
    }
}
