using Nbn.Shared;
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

    [Fact]
    public void LocalClientBindHost_DefaultsToAllInterfaces()
    {
        var vm = new ConnectionViewModel();

        Assert.Equal(NetworkAddressDefaults.DefaultBindHost, vm.LocalBindHost);
    }

    [Fact]
    public void WorkerLimitFields_UseExpectedDefaults()
    {
        var vm = new ConnectionViewModel();

        Assert.Equal("90", vm.WorkerCpuLimitPercentText);
        Assert.Equal("90", vm.WorkerRamLimitPercentText);
        Assert.Equal("90", vm.WorkerGpuLimitPercentText);
        Assert.Equal("90", vm.WorkerVramLimitPercentText);
    }

    [Fact]
    public void ServiceReadiness_AllowsPositiveStatuses_WhenFlagsLag()
    {
        var vm = new ConnectionViewModel
        {
            SettingsStatus = "Ready",
            IoStatus = "Connected",
            HiveMindStatus = "Online",
            ReproStatus = "Online",
            ObsStatus = "Connected",
            SpeciationStatus = "Online"
        };

        Assert.True(vm.HasSpawnServiceReadiness());
        Assert.True(vm.HasReproductionServiceReadiness());
        Assert.True(vm.HasDebugServiceReadiness());
        Assert.True(vm.HasSpeciationServiceReadiness());
    }

    [Fact]
    public void ServiceReadiness_FailsWithoutFlagsOrPositiveStatuses()
    {
        var vm = new ConnectionViewModel
        {
            SettingsStatus = "Disconnected",
            IoStatus = "Offline",
            HiveMindStatus = "Offline",
            ReproStatus = "Offline",
            ObsStatus = "Offline",
            SpeciationStatus = "Offline"
        };

        Assert.False(vm.HasSpawnServiceReadiness());
        Assert.False(vm.HasReproductionServiceReadiness());
        Assert.False(vm.HasDebugServiceReadiness());
        Assert.False(vm.HasSpeciationServiceReadiness());
    }

    [Fact]
    public void BuildSpawnReadinessGuidance_ReportsOnlyMissingServices()
    {
        var vm = new ConnectionViewModel
        {
            SettingsConnected = true,
            IoDiscoverable = true,
            HiveMindDiscoverable = false,
            HiveMindStatus = "Offline"
        };

        Assert.Equal("Connect HiveMind first.", vm.BuildSpawnReadinessGuidance());
    }
}
