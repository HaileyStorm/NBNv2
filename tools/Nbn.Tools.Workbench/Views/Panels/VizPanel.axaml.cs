using Avalonia;
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.VisualTree;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tools.Workbench.Views.Panels;

public partial class VizPanel : UserControl
{
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

        if (!TryResolveCanvasItem(e.Source, out var node, out var edge))
        {
            ViewModel.ClearCanvasHover();
            return;
        }

        var point = e.GetPosition(visual);
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

    private void ActivityCanvasPointerPressed(object? sender, PointerPressedEventArgs e)
    {
        if (ViewModel is null || sender is not Visual visual || e.Handled)
        {
            return;
        }

        if (!TryResolveCanvasItem(e.Source, out var node, out var edge))
        {
            return;
        }

        var pointer = e.GetCurrentPoint(visual).Properties;
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
        ViewModel?.ClearCanvasHover();
    }

    private static bool TryResolveCanvasItem(object? source, out VizActivityCanvasNode? node, out VizActivityCanvasEdge? edge)
    {
        node = null;
        edge = null;
        if (source is not AvaloniaObject sourceObject)
        {
            return false;
        }

        AvaloniaObject? current = sourceObject;
        while (current is not null)
        {
            if (current is StyledElement styled)
            {
                if (styled.DataContext is VizActivityCanvasNode canvasNode)
                {
                    node = canvasNode;
                    return true;
                }

                if (styled.DataContext is VizActivityCanvasEdge canvasEdge)
                {
                    edge = canvasEdge;
                    return true;
                }
            }

            current = (current as Visual)?.GetVisualParent();
        }

        return false;
    }
}
