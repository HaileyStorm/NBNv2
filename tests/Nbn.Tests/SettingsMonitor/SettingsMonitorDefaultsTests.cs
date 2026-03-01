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
    public void DefaultSettings_Include_RemoteCostKeys()
    {
        Assert.True(SettingsMonitorDefaults.DefaultSettings.TryGetValue(
            CostEnergySettingsKeys.RemoteCostEnabledKey,
            out var enabled));
        Assert.Equal("false", enabled);

        Assert.True(SettingsMonitorDefaults.DefaultSettings.TryGetValue(
            CostEnergySettingsKeys.RemoteCostPerBatchKey,
            out var perBatch));
        Assert.Equal("0", perBatch);

        Assert.True(SettingsMonitorDefaults.DefaultSettings.TryGetValue(
            CostEnergySettingsKeys.RemoteCostPerContributionKey,
            out var perContribution));
        Assert.Equal("0", perContribution);
    }

    [Fact]
    public void DefaultSettings_Include_CostTierMultipliers()
    {
        Assert.True(SettingsMonitorDefaults.DefaultSettings.TryGetValue(
            CostEnergySettingsKeys.TierAMultiplierKey,
            out var tierA));
        Assert.Equal("1", tierA);

        Assert.True(SettingsMonitorDefaults.DefaultSettings.TryGetValue(
            CostEnergySettingsKeys.TierBMultiplierKey,
            out var tierB));
        Assert.Equal("1", tierB);

        Assert.True(SettingsMonitorDefaults.DefaultSettings.TryGetValue(
            CostEnergySettingsKeys.TierCMultiplierKey,
            out var tierC));
        Assert.Equal("1", tierC);
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
