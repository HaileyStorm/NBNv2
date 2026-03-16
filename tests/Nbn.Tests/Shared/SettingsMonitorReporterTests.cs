using Nbn.Shared;

namespace Nbn.Tests.Shared;

public sealed class SettingsMonitorReporterTests
{
    [Fact]
    public void BuildDefaultCapabilities_ProvideNonZeroCpuAndCapacityLimits()
    {
        var capabilities = SettingsMonitorReporter.BuildDefaultCapabilities();

        Assert.True(capabilities.CpuCores >= 1);
        Assert.Equal((uint)100, capabilities.CpuLimitPercent);
        Assert.Equal((uint)100, capabilities.RamLimitPercent);
        Assert.Equal((uint)100, capabilities.StorageLimitPercent);
        Assert.Equal((uint)100, capabilities.GpuComputeLimitPercent);
        Assert.Equal((uint)100, capabilities.GpuVramLimitPercent);
        Assert.True(capabilities.RamTotalBytes >= capabilities.RamFreeBytes);
        Assert.True(capabilities.StorageTotalBytes >= capabilities.StorageFreeBytes);
    }
}
