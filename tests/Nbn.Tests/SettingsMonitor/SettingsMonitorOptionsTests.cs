using Nbn.Runtime.SettingsMonitor;
using Nbn.Shared;

namespace Nbn.Tests.SettingsMonitor;

public sealed class SettingsMonitorOptionsTests
{
    [Fact]
    public void FromArgs_UsesLocalAppDataDefaultDatabasePath_WhenDbNotSpecified()
    {
        var options = SettingsMonitorOptions.FromArgs(Array.Empty<string>());

        var expected = SettingsMonitorOptions.GetDefaultDatabasePath();
        Assert.Equal(expected, options.DatabasePath);
    }

    [Fact]
    public void FromArgs_UsesExplicitDatabasePath_WhenDbArgumentProvided()
    {
        var explicitPath = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"), "settingsmonitor.db");
        var options = SettingsMonitorOptions.FromArgs(new[] { "--db", explicitPath });

        Assert.Equal(explicitPath, options.DatabasePath);
    }

    [Fact]
    public void FromArgs_DefaultsBindHost_ToAllInterfaces()
    {
        var options = SettingsMonitorOptions.FromArgs(Array.Empty<string>());

        Assert.Equal(NetworkAddressDefaults.DefaultBindHost, options.BindHost);
    }
}
