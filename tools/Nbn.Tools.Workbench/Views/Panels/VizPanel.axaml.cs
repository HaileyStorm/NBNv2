using System.Diagnostics;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Input;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tools.Workbench.Views.Panels;

public partial class VizPanel : UserControl
{
    private const double HoverHitTestMinIntervalMs = 16;
    private const double HoverHitTestMinMovePx = 1.8;
    private long _lastHoverHitTestTimestamp;
    private Point _lastHoverHitTestPoint;
    private bool _hasHoverHitTestPoint;

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
            return;
        }

        if (!ViewModel.TryResolveCanvasHit(point.X, point.Y, out var node, out var edge))
        {
            ViewModel.ClearCanvasHoverDeferred();
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

        ViewModel.ClearCanvasHoverDeferred();
    }

    private void ActivityCanvasPointerPressed(object? sender, PointerPressedEventArgs e)
    {
        if (ViewModel is null || sender is not Visual visual || e.Handled)
        {
            return;
        }

        var point = e.GetPosition(visual);
        var pointer = e.GetCurrentPoint(visual).Properties;
        var hasHit = ViewModel.TryResolveCanvasHit(point.X, point.Y, out var node, out var edge);
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
            ViewModel.TogglePinCanvasEdge(edge);
        }
        else
        {
            ViewModel.SelectCanvasEdge(edge);
        }

        e.Handled = true;
    }

    private void ActivityCanvasPointerExited(object? sender, PointerEventArgs e)
    {
        _hasHoverHitTestPoint = false;
        ViewModel?.ClearCanvasHoverDeferred();
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
}
