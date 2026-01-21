using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;

namespace Nbn.Tools.Workbench;

public partial class App : Application
{
    public override void Initialize()
    {
        AvaloniaXamlLoader.Load(this);
    }

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            desktop.MainWindow = new MainWindow();
            desktop.MainWindow.Closing += (_, _) =>
            {
                try
                {
                    desktop.Shutdown();
                }
                catch
                {
                }

                try
                {
                    Environment.Exit(0);
                }
                catch
                {
                }
            };
            desktop.MainWindow.Closed += (_, _) =>
            {
                try
                {
                    Environment.Exit(0);
                }
                catch
                {
                }
            };
            desktop.Exit += (_, _) =>
            {
                try
                {
                    Environment.Exit(0);
                }
                catch
                {
                }
            };
        }

        base.OnFrameworkInitializationCompleted();
    }
}
