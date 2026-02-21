using System.Collections.Generic;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Interactivity;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tools.Workbench.Views.Panels;

public partial class VizPanel : UserControl
{
    private const double PressProbeDistancePx = 5.0;
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

    public VizPanel()
    {
        InitializeComponent();
        AddHandler(
            InputElement.PointerMovedEvent,
            VizRootPointerMoved,
            RoutingStrategies.Bubble,
            handledEventsToo: true);
        AddHandler(
            InputElement.PointerExitedEvent,
            VizRootPointerExited,
            RoutingStrategies.Bubble,
            handledEventsToo: true);
    }

    private VizPanelViewModel? ViewModel => DataContext as VizPanelViewModel;

    private void VizRootPointerMoved(object? sender, PointerEventArgs e)
    {
        if (ViewModel is null)
        {
            return;
        }

        if (!TryGetCanvasPointerPoint(e, out var point, out var isInsideCanvas))
        {
            return;
        }

        if (!isInsideCanvas)
        {
            ViewModel.ClearCanvasHover();
            return;
        }

        UpdateCanvasHover(point);
    }

    private void VizRootPointerExited(object? sender, PointerEventArgs e)
    {
        ViewModel?.ClearCanvasHover();
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

    private void UpdateCanvasHover(Point point)
    {
        if (ViewModel is null)
        {
            return;
        }

        if (!TryResolveCanvasHitWithProbe(point, HoverProbeOffsets, hoverMode: true, out var node, out var edge))
        {
            ViewModel.ClearCanvasHover();
            return;
        }

        if (node is not null)
        {
            ViewModel.SetCanvasNodeHover(node, point.X, point.Y);
            return;
        }

        if (edge is not null)
        {
            ViewModel.SetCanvasEdgeHover(edge, point.X, point.Y);
            return;
        }

        ViewModel.ClearCanvasHover();
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

    private bool TryGetCanvasPointerPoint(PointerEventArgs e, out Point point, out bool inside)
    {
        point = default;
        inside = false;
        var canvas = ActivityCanvasSurface;
        if (canvas is null || !canvas.IsVisible || canvas.Bounds.Width <= 0 || canvas.Bounds.Height <= 0)
        {
            return false;
        }

        point = e.GetPosition(canvas);
        inside = point.X >= 0
            && point.Y >= 0
            && point.X <= canvas.Bounds.Width
            && point.Y <= canvas.Bounds.Height;
        return true;
    }

}
