using Nbn.Runtime.WorkerNode;

namespace Nbn.Tests.WorkerNode;

public sealed class WorkerNodeCapabilityProviderTests
{
    [Fact]
    public void GetCapabilities_UsesProbedBaselineAndAppliesAvailabilityScaling()
    {
        var now = DateTimeOffset.UtcNow;
        var provider = new WorkerNodeCapabilityProvider(
            availability: new WorkerResourceAvailability(cpuPercent: 50, ramPercent: 25, storagePercent: 10, gpuComputePercent: 40, gpuVramPercent: 30),
            clock: () => now,
            baselineProbe: () => new WorkerNodeCapabilityProvider.WorkerCapabilityBaseline(
                CpuCores: 16,
                RamFreeBytes: 8_000,
                RamTotalBytes: 32_000,
                StorageFreeBytes: 90_000,
                StorageTotalBytes: 120_000,
                HasGpu: true,
                GpuName: "gpu0",
                VramFreeBytes: 12_000,
                VramTotalBytes: 16_000,
                IlgpuCudaAvailable: true,
                IlgpuOpenclAvailable: false),
            scoreProbe: _ => new WorkerNodeCapabilityProvider.WorkerCapabilityScores(
                CpuScore: 88f,
                GpuScore: 66f));

        var capabilities = provider.GetCapabilities();

        Assert.Equal((uint)16, capabilities.CpuCores);
        Assert.Equal((ulong)8_000, capabilities.RamFreeBytes);
        Assert.Equal((ulong)90_000, capabilities.StorageFreeBytes);
        Assert.Equal(88f, capabilities.CpuScore);
        Assert.True(capabilities.HasGpu);
        Assert.Equal("gpu0", capabilities.GpuName);
        Assert.Equal((ulong)12_000, capabilities.VramFreeBytes);
        Assert.Equal(66f, capabilities.GpuScore, 3);
        Assert.True(capabilities.IlgpuCudaAvailable);
        Assert.False(capabilities.IlgpuOpenclAvailable);
        Assert.Equal((ulong)32_000, capabilities.RamTotalBytes);
        Assert.Equal((ulong)120_000, capabilities.StorageTotalBytes);
        Assert.Equal((ulong)16_000, capabilities.VramTotalBytes);
        Assert.Equal((uint)50, capabilities.CpuLimitPercent);
        Assert.Equal((uint)25, capabilities.RamLimitPercent);
        Assert.Equal((uint)10, capabilities.StorageLimitPercent);
        Assert.Equal((uint)40, capabilities.GpuComputeLimitPercent);
        Assert.Equal((uint)30, capabilities.GpuVramLimitPercent);
    }

    [Fact]
    public void GetCapabilities_CachesBaselineAndScoreUntilInvalidated()
    {
        var now = DateTimeOffset.UtcNow;
        var baselineCalls = 0;
        var scoreCalls = 0;

        var provider = new WorkerNodeCapabilityProvider(
            availability: new WorkerResourceAvailability(cpuPercent: 100, ramPercent: 100, storagePercent: 100, gpuComputePercent: 100, gpuVramPercent: 100),
            probeRefreshInterval: TimeSpan.FromMinutes(5),
            clock: () => now,
            baselineProbe: () =>
            {
                baselineCalls++;
                return new WorkerNodeCapabilityProvider.WorkerCapabilityBaseline(
                    CpuCores: 8,
                    RamFreeBytes: (ulong)(1_000 * baselineCalls),
                    RamTotalBytes: 16_000,
                    StorageFreeBytes: 2_000,
                    StorageTotalBytes: 8_000,
                    HasGpu: false,
                    GpuName: string.Empty,
                    VramFreeBytes: 0,
                    VramTotalBytes: 0,
                    IlgpuCudaAvailable: false,
                    IlgpuOpenclAvailable: false);
            },
            scoreProbe: _ =>
            {
                scoreCalls++;
                return new WorkerNodeCapabilityProvider.WorkerCapabilityScores(
                    CpuScore: 10f * scoreCalls,
                    GpuScore: 0f);
            });

        var first = provider.GetCapabilities();
        var second = provider.GetCapabilities();

        now = now.AddMinutes(2);
        var third = provider.GetCapabilities();

        now = now.AddMinutes(5);
        var fourth = provider.GetCapabilities();
        provider.MarkDirty();
        var fifth = provider.GetCapabilities();

        Assert.Equal(2, baselineCalls);
        Assert.Equal(2, scoreCalls);
        Assert.Equal((ulong)1_000, first.RamFreeBytes);
        Assert.Equal((ulong)1_000, second.RamFreeBytes);
        Assert.Equal((ulong)1_000, third.RamFreeBytes);
        Assert.Equal((ulong)2_000, fourth.RamFreeBytes);
        Assert.Equal((ulong)2_000, fifth.RamFreeBytes);
        Assert.Equal(10f, first.CpuScore);
        Assert.Equal(10f, second.CpuScore);
        Assert.Equal(10f, third.CpuScore);
        Assert.Equal(10f, fourth.CpuScore);
        Assert.Equal(20f, fifth.CpuScore);
    }
}
