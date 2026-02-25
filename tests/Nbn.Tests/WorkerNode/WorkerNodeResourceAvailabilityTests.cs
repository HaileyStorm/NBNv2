using Nbn.Proto.Settings;
using Nbn.Runtime.WorkerNode;
using Proto;

namespace Nbn.Tests.WorkerNode;

[Collection("WorkerNodeEnvSerial")]
public sealed class WorkerNodeResourceAvailabilityTests
{
    [Fact]
    public void FromArgs_Defaults_ResourceAvailability_ToConfiguredDefaultPercent()
    {
        using var _ = new EnvironmentVariableScope(
            ("NBN_WORKER_CPU_PCT", null),
            ("NBN_WORKER_RAM_PCT", null),
            ("NBN_WORKER_STORAGE_PCT", null),
            ("NBN_WORKER_GPU_PCT", null));

        var options = WorkerNodeOptions.FromArgs([]);

        Assert.Equal(WorkerResourceAvailability.DefaultPercent, options.ResourceAvailability.CpuPercent);
        Assert.Equal(WorkerResourceAvailability.DefaultPercent, options.ResourceAvailability.RamPercent);
        Assert.Equal(WorkerResourceAvailability.DefaultPercent, options.ResourceAvailability.StoragePercent);
        Assert.Equal(WorkerResourceAvailability.DefaultPercent, options.ResourceAvailability.GpuPercent);
    }

    [Fact]
    public void FromArgs_ParsesAndClamps_ResourceAvailability_FromCli()
    {
        using var _ = new EnvironmentVariableScope(
            ("NBN_WORKER_CPU_PCT", null),
            ("NBN_WORKER_RAM_PCT", null),
            ("NBN_WORKER_STORAGE_PCT", null),
            ("NBN_WORKER_GPU_PCT", null));

        var options = WorkerNodeOptions.FromArgs(
        [
            "--cpu-pct", "50",
            "--ram_pct", "-10",
            "--storage-pct", "999",
            "--gpu_pct=0"
        ]);

        Assert.Equal(50, options.ResourceAvailability.CpuPercent);
        Assert.Equal(0, options.ResourceAvailability.RamPercent);
        Assert.Equal(100, options.ResourceAvailability.StoragePercent);
        Assert.Equal(0, options.ResourceAvailability.GpuPercent);
    }

    [Fact]
    public void FromArgs_Parses_ResourceAvailability_FromEnv_And_CliOverrides()
    {
        using var _ = new EnvironmentVariableScope(
            ("NBN_WORKER_CPU_PCT", "25"),
            ("NBN_WORKER_RAM_PCT", "30"),
            ("NBN_WORKER_STORAGE_PCT", "40"),
            ("NBN_WORKER_GPU_PCT", "50"));

        var options = WorkerNodeOptions.FromArgs(["--cpu-pct", "101", "--gpu-pct", "80"]);

        Assert.Equal(100, options.ResourceAvailability.CpuPercent);
        Assert.Equal(30, options.ResourceAvailability.RamPercent);
        Assert.Equal(40, options.ResourceAvailability.StoragePercent);
        Assert.Equal(80, options.ResourceAvailability.GpuPercent);
    }

    [Fact]
    public void FromArgs_Throws_On_Invalid_Percentage_FromCli()
    {
        using var _ = new EnvironmentVariableScope(
            ("NBN_WORKER_CPU_PCT", null),
            ("NBN_WORKER_RAM_PCT", null),
            ("NBN_WORKER_STORAGE_PCT", null),
            ("NBN_WORKER_GPU_PCT", null));

        var exception = Assert.Throws<ArgumentException>(() => WorkerNodeOptions.FromArgs(["--ram-pct", "not-a-number"]));
        Assert.Contains("--ram-pct", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FromArgs_Throws_On_Invalid_Percentage_FromEnvironment()
    {
        using var _ = new EnvironmentVariableScope(
            ("NBN_WORKER_CPU_PCT", null),
            ("NBN_WORKER_RAM_PCT", null),
            ("NBN_WORKER_STORAGE_PCT", null),
            ("NBN_WORKER_GPU_PCT", "oops"));

        var exception = Assert.Throws<ArgumentException>(() => WorkerNodeOptions.FromArgs([]));
        Assert.Contains("NBN_WORKER_GPU_PCT", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WorkerCapabilityScaling_AppliesAvailability_ToCapabilities()
    {
        var baseline = new NodeCapabilities
        {
            CpuCores = 8,
            RamFreeBytes = 1_000,
            StorageFreeBytes = 10_000,
            HasGpu = true,
            GpuName = "gpu",
            VramFreeBytes = 20_000,
            CpuScore = 80f,
            GpuScore = 60f,
            IlgpuCudaAvailable = true,
            IlgpuOpenclAvailable = true
        };

        var scaled = WorkerCapabilityScaling.ApplyScale(
            baseline,
            new WorkerResourceAvailability(cpuPercent: 50, ramPercent: 25, storagePercent: 10, gpuPercent: 0));

        Assert.Equal((uint)4, scaled.CpuCores);
        Assert.Equal((ulong)250, scaled.RamFreeBytes);
        Assert.Equal((ulong)1_000, scaled.StorageFreeBytes);
        Assert.Equal(40f, scaled.CpuScore);
        Assert.False(scaled.HasGpu);
        Assert.Equal(string.Empty, scaled.GpuName);
        Assert.Equal((ulong)0, scaled.VramFreeBytes);
        Assert.Equal(0f, scaled.GpuScore);
        Assert.False(scaled.IlgpuCudaAvailable);
        Assert.False(scaled.IlgpuOpenclAvailable);
    }

    [Fact]
    public void WorkerCapabilityScaling_PreservesMinimumCpuCore_WhenPercentageIsPositive()
    {
        var baseline = new NodeCapabilities
        {
            CpuCores = 1,
            RamFreeBytes = 0,
            StorageFreeBytes = 0,
            HasGpu = false
        };

        var scaled = WorkerCapabilityScaling.ApplyScale(
            baseline,
            new WorkerResourceAvailability(cpuPercent: 1, ramPercent: 100, storagePercent: 100, gpuPercent: 100));

        Assert.Equal((uint)1, scaled.CpuCores);
    }

    [Fact]
    public async Task WorkerNodeSnapshot_ExposesConfiguredResourceAvailability()
    {
        var system = new ActorSystem();
        var workerPid = system.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(
                Guid.NewGuid(),
                "127.0.0.1:12041",
                resourceAvailability: new WorkerResourceAvailability(cpuPercent: 130, ramPercent: -20, storagePercent: 45, gpuPercent: 0))));

        var snapshot = await system.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());

        Assert.Equal(100, snapshot.ResourceAvailability.CpuPercent);
        Assert.Equal(0, snapshot.ResourceAvailability.RamPercent);
        Assert.Equal(45, snapshot.ResourceAvailability.StoragePercent);
        Assert.Equal(0, snapshot.ResourceAvailability.GpuPercent);

        await system.ShutdownAsync();
    }

    private sealed class EnvironmentVariableScope : IDisposable
    {
        private readonly Dictionary<string, string?> _originals = new(StringComparer.Ordinal);

        public EnvironmentVariableScope(params (string Key, string? Value)[] values)
        {
            foreach (var (key, value) in values)
            {
                _originals[key] = Environment.GetEnvironmentVariable(key);
                Environment.SetEnvironmentVariable(key, value);
            }
        }

        public void Dispose()
        {
            foreach (var (key, value) in _originals)
            {
                Environment.SetEnvironmentVariable(key, value);
            }
        }
    }
}

[CollectionDefinition("WorkerNodeEnvSerial", DisableParallelization = true)]
public sealed class WorkerNodeEnvSerialCollection
{
}
