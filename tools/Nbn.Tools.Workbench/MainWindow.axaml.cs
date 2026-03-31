using System;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tools.Workbench;

public partial class MainWindow : Window
{
    private static readonly TimeSpan ShutdownGraceTimeout = TimeSpan.FromSeconds(8);
    private static readonly TimeSpan ForcedExitTimeout = TimeSpan.FromSeconds(12);

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
        try
        {
            Hide();
        }
        catch
        {
        }

        _ = ShutdownAsync();
        base.OnClosing(e);
    }

    private async Task ShutdownAsync()
    {
        using var forcedExitCts = new CancellationTokenSource();
        var forcedExitTask = ForceExitAfterDelayAsync(forcedExitCts.Token);

        try
        {
            try
            {
                await _viewModel.DisposeAsync().AsTask().WaitAsync(ShutdownGraceTimeout).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                WorkbenchLog.Warn($"Workbench shutdown exceeded {ShutdownGraceTimeout.TotalSeconds:0}s grace timeout; forcing exit.");
            }
        }
        catch
        {
        }

        try
        {
            WorkbenchProcessRegistry.Default.CleanupStale();
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
        finally
        {
            try
            {
                forcedExitCts.Cancel();
                await forcedExitTask.ConfigureAwait(false);
            }
            catch
            {
            }
        }
    }

    private static Task ForceExitAfterDelayAsync(CancellationToken cancellationToken)
        => Task.Run(async () =>
        {
            try
            {
                await Task.Delay(ForcedExitTimeout, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            try
            {
                Environment.Exit(0);
            }
            catch
            {
            }
        }, CancellationToken.None);
}
