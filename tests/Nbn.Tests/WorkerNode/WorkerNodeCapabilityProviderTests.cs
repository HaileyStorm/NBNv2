using Nbn.Runtime.WorkerNode;

namespace Nbn.Tests.WorkerNode;

public sealed class WorkerNodeCapabilityProviderTests
{
    [Fact]
    public void GetCapabilities_UsesProbedBaselineAndAppliesAvailabilityScaling()
    {
        var now = DateTimeOffset.UtcNow;
        var provider = new WorkerNodeCapabilityProvider(
            availability: new WorkerResourceAvailability(cpuPercent: 50, ramPercent: 25, storagePercent: 10, gpuPercent: 40),
            clock: () => now,
            baselineProbe: () => new WorkerNodeCapabilityProvider.WorkerCapabilityBaseline(
                CpuCores: 16,
                RamFreeBytes: 8_000,
                StorageFreeBytes: 90_000,
                HasGpu: true,
                GpuName: "gpu0",
                VramFreeBytes: 12_000,
                IlgpuCudaAvailable: true,
                IlgpuOpenclAvailable: false),
            scoreProbe: _ => new WorkerNodeCapabilityProvider.WorkerCapabilityScores(
                CpuScore: 88f,
                GpuScore: 66f));

        var capabilities = provider.GetCapabilities();

        Assert.Equal((uint)8, capabilities.CpuCores);
        Assert.Equal((ulong)2_000, capabilities.RamFreeBytes);
        Assert.Equal((ulong)9_000, capabilities.StorageFreeBytes);
        Assert.Equal(44f, capabilities.CpuScore);
        Assert.True(capabilities.HasGpu);
        Assert.Equal("gpu0", capabilities.GpuName);
        Assert.Equal((ulong)4_800, capabilities.VramFreeBytes);
        Assert.Equal(26.4f, capabilities.GpuScore, 3);
        Assert.True(capabilities.IlgpuCudaAvailable);
        Assert.False(capabilities.IlgpuOpenclAvailable);
    }

    [Fact]
    public void GetCapabilities_CachesBaselineAndScoreUntilRefreshIntervalsExpire()
    {
        var now = DateTimeOffset.UtcNow;
        var baselineCalls = 0;
        var scoreCalls = 0;

        var provider = new WorkerNodeCapabilityProvider(
            availability: new WorkerResourceAvailability(cpuPercent: 100, ramPercent: 100, storagePercent: 100, gpuPercent: 100),
            probeRefreshInterval: TimeSpan.FromMinutes(5),
            benchmarkRefreshInterval: TimeSpan.FromMinutes(1),
            clock: () => now,
            baselineProbe: () =>
            {
                baselineCalls++;
                return new WorkerNodeCapabilityProvider.WorkerCapabilityBaseline(
                    CpuCores: 8,
                    RamFreeBytes: (ulong)(1_000 * baselineCalls),
                    StorageFreeBytes: 2_000,
                    HasGpu: false,
                    GpuName: string.Empty,
                    VramFreeBytes: 0,
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

        Assert.Equal(2, baselineCalls);
        Assert.Equal(3, scoreCalls);
        Assert.Equal((ulong)1_000, first.RamFreeBytes);
        Assert.Equal((ulong)1_000, second.RamFreeBytes);
        Assert.Equal((ulong)1_000, third.RamFreeBytes);
        Assert.Equal((ulong)2_000, fourth.RamFreeBytes);
        Assert.Equal(10f, first.CpuScore);
        Assert.Equal(10f, second.CpuScore);
        Assert.Equal(20f, third.CpuScore);
        Assert.Equal(30f, fourth.CpuScore);
    }
}
