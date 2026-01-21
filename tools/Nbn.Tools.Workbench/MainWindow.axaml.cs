using System;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tools.Workbench;

public partial class MainWindow : Window
{
    private readonly ShellViewModel _viewModel = new();
    private bool _shutdownStarted;

    public MainWindow()
    {
        InitializeComponent();
        DataContext = _viewModel;
    }

    protected override void OnClosing(WindowClosingEventArgs e)
    {
        if (_shutdownStarted)
        {
            base.OnClosing(e);
            return;
        }

        _shutdownStarted = true;
        e.Cancel = true;
        _ = ShutdownAsync();
        base.OnClosing(e);
    }

    private async Task ShutdownAsync()
    {
        try
        {
            await _viewModel.DisposeAsync().ConfigureAwait(false);
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

        try
        {
            if (Application.Current?.ApplicationLifetime is Avalonia.Controls.ApplicationLifetimes.IClassicDesktopStyleApplicationLifetime desktop)
            {
                desktop.Shutdown();
            }
        }
        catch
        {
        }

        _ = Task.Run(async () =>
        {
            await Task.Delay(1500).ConfigureAwait(false);
            try
            {
                Environment.Exit(0);
            }
            catch
            {
            }
        });
    }
}
