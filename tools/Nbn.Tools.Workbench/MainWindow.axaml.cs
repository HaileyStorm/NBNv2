using System;
using Avalonia.Controls;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tools.Workbench;

public partial class MainWindow : Window
{
    private readonly ShellViewModel _viewModel = new();

    public MainWindow()
    {
        InitializeComponent();
        DataContext = _viewModel;
    }

    protected override void OnClosed(EventArgs e)
    {
        try
        {
            _viewModel.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
        catch
        {
        }

        try
        {
            WorkbenchProcessRegistry.Default.CleanupStale();
            LocalDemoRunner.CleanupStaleProcesses();
        }
        catch
        {
        }
        base.OnClosed(e);
    }
}
