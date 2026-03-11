using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Threading;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tools.Workbench.Views.Windows;

public partial class SpeciationFlowChartWindow : Window
{
    public SpeciationFlowChartWindow()
    {
        InitializeComponent();
        Opened += (_, _) => Dispatcher.UIThread.Post(RefreshViewport, DispatcherPriority.Loaded);
        SizeChanged += (_, _) => RefreshViewport();
    }

    private SpeciationPanelViewModel? ViewModel => DataContext as SpeciationPanelViewModel;

    private void FlowChartAreaPointerMoved(object? sender, PointerEventArgs e)
    {
        if (ViewModel is null || sender is not Control { DataContext: SpeciationFlowChartAreaItem area })
        {
            return;
        }

        var point = e.GetPosition(ExpandedFlowChartPlot);
        ViewModel.UpdateExpandedFlowChartHover(area, point.X, point.Y);
    }

    private void FlowChartAreaPointerExited(object? sender, PointerEventArgs e)
        => ViewModel?.ClearExpandedFlowChartHover();

    private void FlowChartPlotPointerExited(object? sender, PointerEventArgs e)
        => ViewModel?.ClearExpandedFlowChartHover();

    private void RefreshViewport()
    {
        if (ViewModel is null)
        {
            return;
        }

        if (ExpandedFlowChartPlot.Bounds.Width <= 1d || ExpandedFlowChartPlot.Bounds.Height <= 1d)
        {
            return;
        }

        ViewModel.UpdateExpandedFlowChartViewport(
            ExpandedFlowChartPlot.Bounds.Width,
            ExpandedFlowChartPlot.Bounds.Height,
            Bounds.Width);
    }
}
