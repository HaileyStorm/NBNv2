using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Threading;
using Avalonia.Interactivity;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tools.Workbench.Views.Windows;

public partial class SpeciationFlowChartWindow : Window
{
    private string? _legendColorPickerSpeciesId;

    public SpeciationFlowChartWindow()
    {
        InitializeComponent();
        Opened += (_, _) => Dispatcher.UIThread.Post(RefreshViewport, DispatcherPriority.Loaded);
        SizeChanged += (_, _) => RefreshViewport();
        Closed += (_, _) => CloseLegendColorPicker();
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

    private void LegendSwatch_Click(object? sender, RoutedEventArgs e)
    {
        if (ViewModel is null
            || sender is not Control { DataContext: SpeciationChartLegendItem legendItem }
            || !legendItem.IsColorEditable)
        {
            return;
        }

        _legendColorPickerSpeciesId = legendItem.SpeciesId;
        LegendColorPickerPopup.IsOpen = true;
    }

    private void LegendColorPickerSwatch_Click(object? sender, RoutedEventArgs e)
    {
        if (ViewModel is null
            || string.IsNullOrWhiteSpace(_legendColorPickerSpeciesId)
            || sender is not Control { DataContext: SpeciationColorPickerSwatchItem colorOption })
        {
            return;
        }

        ViewModel.SetSpeciesColorOverride(_legendColorPickerSpeciesId, colorOption.ColorHex);
        CloseLegendColorPicker();
    }

    private void LegendColorPickerCard_PointerPressed(object? sender, PointerPressedEventArgs e)
    {
        if (ReferenceEquals(e.Source, sender))
        {
            CloseLegendColorPicker();
            e.Handled = true;
        }
    }

    private void LegendColorPickerPopup_Closed(object? sender, System.EventArgs e)
        => _legendColorPickerSpeciesId = null;

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

    private void CloseLegendColorPicker()
    {
        if (!LegendColorPickerPopup.IsOpen)
        {
            _legendColorPickerSpeciesId = null;
            return;
        }

        LegendColorPickerPopup.IsOpen = false;
        _legendColorPickerSpeciesId = null;
    }
}
