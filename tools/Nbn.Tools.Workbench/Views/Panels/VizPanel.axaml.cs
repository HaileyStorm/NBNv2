using Avalonia.Controls;
using Avalonia.Input;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tools.Workbench.Views.Panels;

public partial class VizPanel : UserControl
{
    public VizPanel()
    {
        InitializeComponent();
    }

    private VizPanelViewModel? ViewModel => DataContext as VizPanelViewModel;

    private void CanvasNodePointerEntered(object? sender, PointerEventArgs e)
    {
        if (ViewModel is null || sender is not Control { DataContext: VizActivityCanvasNode node })
        {
            return;
        }

        ViewModel.SetCanvasNodeHover(node);
    }

    private void CanvasNodePointerExited(object? sender, PointerEventArgs e)
    {
        ViewModel?.SetCanvasNodeHover(null);
    }

    private void CanvasNodePointerPressed(object? sender, PointerPressedEventArgs e)
    {
        if (ViewModel is null || sender is not Control { DataContext: VizActivityCanvasNode node } control)
        {
            return;
        }

        var pointer = e.GetCurrentPoint(control).Properties;
        if (pointer.IsRightButtonPressed)
        {
            ViewModel.TogglePinCanvasNode(node);
        }
        else
        {
            ViewModel.SelectCanvasNode(node);
        }

        e.Handled = true;
    }

    private void CanvasEdgePointerEntered(object? sender, PointerEventArgs e)
    {
        if (ViewModel is null || sender is not Control { DataContext: VizActivityCanvasEdge edge })
        {
            return;
        }

        ViewModel.SetCanvasEdgeHover(edge);
    }

    private void CanvasEdgePointerExited(object? sender, PointerEventArgs e)
    {
        ViewModel?.SetCanvasEdgeHover(null);
    }

    private void CanvasEdgePointerPressed(object? sender, PointerPressedEventArgs e)
    {
        if (ViewModel is null || sender is not Control { DataContext: VizActivityCanvasEdge edge } control)
        {
            return;
        }

        var pointer = e.GetCurrentPoint(control).Properties;
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
}
