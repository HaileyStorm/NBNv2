using Nbn.Runtime.SettingsMonitor;

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
}
