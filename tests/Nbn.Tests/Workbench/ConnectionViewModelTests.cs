using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Workbench;

public sealed class ConnectionViewModelTests
{
    [Fact]
    public void SettingsDbPath_DefaultsToLocalAppDataPath()
    {
        var vm = new ConnectionViewModel();
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(localAppData))
        {
            Assert.Equal("settingsmonitor.db", vm.SettingsDbPath);
            return;
        }

        var expected = Path.Combine(localAppData, "Nbn.Workbench", "settingsmonitor.db");
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        Assert.True(string.Equals(expected, vm.SettingsDbPath, comparison));
    }

    [Fact]
    public void ObservabilityEndpointFields_UseExpectedDefaults()
    {
        var vm = new ConnectionViewModel();

        Assert.Equal("127.0.0.1", vm.ObsHost);
        Assert.Equal("12060", vm.ObsPortText);
        Assert.Equal("DebugHub", vm.DebugHub);
        Assert.Equal("VisualizationHub", vm.VizHub);
    }
}
