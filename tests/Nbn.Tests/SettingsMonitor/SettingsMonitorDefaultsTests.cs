using Nbn.Runtime.SettingsMonitor;
using Nbn.Shared;

namespace Nbn.Tests.SettingsMonitor;

public sealed class SettingsMonitorDefaultsTests
{
    [Fact]
    public void DefaultSettings_Include_SystemCostEnergyMasterKey()
    {
        Assert.True(SettingsMonitorDefaults.DefaultSettings.TryGetValue(
            CostEnergySettingsKeys.SystemEnabledKey,
            out var value));
        Assert.Equal("true", value);
    }

    [Fact]
    public void DefaultSettings_Include_SystemPlasticityMasterKey()
    {
        Assert.True(SettingsMonitorDefaults.DefaultSettings.TryGetValue(
            PlasticitySettingsKeys.SystemEnabledKey,
            out var value));
        Assert.Equal("true", value);
    }

    [Fact]
    public void DefaultSettings_Include_SystemPlasticityRateKey()
    {
        Assert.True(SettingsMonitorDefaults.DefaultSettings.TryGetValue(
            PlasticitySettingsKeys.SystemRateKey,
            out var value));
        Assert.Equal("0.001", value);
    }

    [Fact]
    public void DefaultSettings_Include_SystemPlasticityModeKey()
    {
        Assert.True(SettingsMonitorDefaults.DefaultSettings.TryGetValue(
            PlasticitySettingsKeys.SystemProbabilisticUpdatesKey,
            out var value));
        Assert.Equal("true", value);
    }
}
