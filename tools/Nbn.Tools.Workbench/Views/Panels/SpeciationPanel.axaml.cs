using System;
using Avalonia.Controls;
using Avalonia.Controls.Primitives;
using Avalonia.Input;
using Avalonia.Interactivity;
using Nbn.Tools.Workbench.ViewModels;
using Nbn.Tools.Workbench.Views.Windows;

namespace Nbn.Tools.Workbench.Views.Panels;

public partial class SpeciationPanel : UserControl
{
    private SpeciationFlowChartWindow? _flowChartWindow;
    private string? _legendColorPickerSpeciesId;

    public SpeciationPanel()
    {
        InitializeComponent();
        DetachedFromVisualTree += (_, _) =>
        {
            CloseLegendColorPicker();
            CloseFlowChartWindow();
        };
    }

    private SpeciationPanelViewModel? ViewModel => DataContext as SpeciationPanelViewModel;

    private void OpenFlowChartPopout_Click(object? sender, RoutedEventArgs e)
    {
        var viewModel = ViewModel;
        if (viewModel is null)
        {
            return;
        }

        if (_flowChartWindow is { IsVisible: true })
        {
            _flowChartWindow.Activate();
            return;
        }

        _flowChartWindow = new SpeciationFlowChartWindow
        {
            DataContext = viewModel
        };
        _flowChartWindow.Closed += FlowChartWindowClosed;
        if (TopLevel.GetTopLevel(this) is Window owner)
        {
            _flowChartWindow.Show(owner);
            return;
        }

        _flowChartWindow.Show();
    }

    private void FlowChartAreaPointerMoved(object? sender, PointerEventArgs e)
    {
        if (ViewModel is null || sender is not Control { DataContext: SpeciationFlowChartAreaItem area })
        {
            return;
        }

        var point = e.GetPosition(InlineFlowChartPlot);
        ViewModel.UpdateFlowChartHover(area, point.X, point.Y);
    }

    private void FlowChartAreaPointerExited(object? sender, PointerEventArgs e)
        => ViewModel?.ClearFlowChartHover();

    private void FlowChartPlotPointerExited(object? sender, PointerEventArgs e)
        => ViewModel?.ClearFlowChartHover();

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

    private void LegendColorPickerPopup_Closed(object? sender, EventArgs e)
        => _legendColorPickerSpeciesId = null;

    private void FlowChartWindowClosed(object? sender, EventArgs e)
    {
        if (_flowChartWindow is null)
        {
            return;
        }

        _flowChartWindow.Closed -= FlowChartWindowClosed;
        _flowChartWindow = null;
    }

    private void CloseFlowChartWindow()
    {
        if (_flowChartWindow is null)
        {
            return;
        }

        _flowChartWindow.Closed -= FlowChartWindowClosed;
        _flowChartWindow.Close();
        _flowChartWindow = null;
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
