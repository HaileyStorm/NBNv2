using Nbn.Proto.Settings;
using Nbn.Runtime.WorkerNode;
using Nbn.Shared;
using Proto;

namespace Nbn.Tests.WorkerNode;

[Collection("WorkerNodeEnvSerial")]
public sealed class WorkerNodeResourceAvailabilityTests
{
    [Fact]
    public void FromArgs_Defaults_ResourceAvailability_ToConfiguredDefaultPercent()
    {
        using var _ = new EnvironmentVariableScope(
            ("NBN_WORKER_COUNT", null),
            ("NBN_WORKER_CPU_PCT", null),
            ("NBN_WORKER_RAM_PCT", null),
            ("NBN_WORKER_STORAGE_PCT", null),
            ("NBN_WORKER_GPU_PCT", null),
            ("NBN_WORKER_GPU_COMPUTE_PCT", null),
            ("NBN_WORKER_GPU_VRAM_PCT", null));

        var options = WorkerNodeOptions.FromArgs([]);

        Assert.Equal(WorkerResourceAvailability.DefaultPercent, options.ResourceAvailability.CpuPercent);
        Assert.Equal(WorkerResourceAvailability.DefaultPercent, options.ResourceAvailability.RamPercent);
        Assert.Equal(WorkerResourceAvailability.DefaultPercent, options.ResourceAvailability.StoragePercent);
        Assert.Equal(WorkerResourceAvailability.DefaultPercent, options.ResourceAvailability.GpuComputePercent);
        Assert.Equal(WorkerResourceAvailability.DefaultPercent, options.ResourceAvailability.GpuVramPercent);
    }

    [Fact]
    public void FromArgs_Defaults_WorkerCount_ToOne()
    {
        using var _ = new EnvironmentVariableScope(("NBN_WORKER_COUNT", null));

        var options = WorkerNodeOptions.FromArgs([]);

        Assert.Equal(1, options.WorkerCount);
        Assert.Equal("worker-node", options.ResolveRootActorName(0));
        Assert.Equal(
            NodeIdentity.DeriveNodeId("127.0.0.1:12041"),
            options.ResolveWorkerNodeId("127.0.0.1:12041", 0));
    }

    [Fact]
    public void FromArgs_ParsesWorkerCount_AndDerivesUniqueSharedPortIdentities()
    {
        using var _ = new EnvironmentVariableScope(("NBN_WORKER_COUNT", null));
        var configuredNodeId = Guid.NewGuid();

        var options = WorkerNodeOptions.FromArgs(
        [
            "--worker-count", "3",
            "--root-name", "worker-node",
            "--worker-node-id", configuredNodeId.ToString("D")
        ]);

        Assert.Equal(3, options.WorkerCount);
        Assert.Equal("worker-node", options.ResolveRootActorName(0));
        Assert.Equal("worker-node-2", options.ResolveRootActorName(1));
        Assert.Equal("worker-node-3", options.ResolveRootActorName(2));

        var address = "127.0.0.1:12041";
        var firstNodeId = options.ResolveWorkerNodeId(address, 0);
        var secondNodeId = options.ResolveWorkerNodeId(address, 1);
        var thirdNodeId = options.ResolveWorkerNodeId(address, 2);

        Assert.Equal(configuredNodeId, firstNodeId);
        Assert.Equal(NodeIdentity.DeriveNodeId(address, "worker-node-2"), secondNodeId);
        Assert.Equal(NodeIdentity.DeriveNodeId(address, "worker-node-3"), thirdNodeId);
        Assert.NotEqual(Guid.Empty, secondNodeId);
        Assert.NotEqual(Guid.Empty, thirdNodeId);
        Assert.NotEqual(firstNodeId, secondNodeId);
        Assert.NotEqual(secondNodeId, thirdNodeId);
    }

    [Fact]
    public void FromArgs_RejectsInvalidWorkerCount_FromCli()
    {
        using var _ = new EnvironmentVariableScope(("NBN_WORKER_COUNT", null));

        var exception = Assert.Throws<ArgumentException>(() => WorkerNodeOptions.FromArgs(["--worker-count", "0"]));

        Assert.Contains("--worker-count", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FromArgs_ParsesAndClamps_ResourceAvailability_FromCli()
    {
        using var _ = new EnvironmentVariableScope(
            ("NBN_WORKER_CPU_PCT", null),
            ("NBN_WORKER_RAM_PCT", null),
            ("NBN_WORKER_STORAGE_PCT", null),
            ("NBN_WORKER_GPU_PCT", null),
            ("NBN_WORKER_GPU_COMPUTE_PCT", null),
            ("NBN_WORKER_GPU_VRAM_PCT", null));

        var options = WorkerNodeOptions.FromArgs(
        [
            "--cpu-pct", "50",
            "--ram_pct", "-10",
            "--storage-pct", "999",
            "--gpu-compute-pct", "0",
            "--gpu-vram-pct", "999"
        ]);

        Assert.Equal(50, options.ResourceAvailability.CpuPercent);
        Assert.Equal(0, options.ResourceAvailability.RamPercent);
        Assert.Equal(100, options.ResourceAvailability.StoragePercent);
        Assert.Equal(0, options.ResourceAvailability.GpuComputePercent);
        Assert.Equal(100, options.ResourceAvailability.GpuVramPercent);
    }

    [Fact]
    public void FromArgs_Parses_ResourceAvailability_FromEnv_And_CliOverrides()
    {
        using var _ = new EnvironmentVariableScope(
            ("NBN_WORKER_CPU_PCT", "25"),
            ("NBN_WORKER_RAM_PCT", "30"),
            ("NBN_WORKER_STORAGE_PCT", "40"),
            ("NBN_WORKER_GPU_COMPUTE_PCT", "50"),
            ("NBN_WORKER_GPU_VRAM_PCT", "60"),
            ("NBN_WORKER_GPU_PCT", null));

        var options = WorkerNodeOptions.FromArgs(["--cpu-pct", "101", "--gpu-vram-pct", "80"]);

        Assert.Equal(100, options.ResourceAvailability.CpuPercent);
        Assert.Equal(30, options.ResourceAvailability.RamPercent);
        Assert.Equal(40, options.ResourceAvailability.StoragePercent);
        Assert.Equal(50, options.ResourceAvailability.GpuComputePercent);
        Assert.Equal(80, options.ResourceAvailability.GpuVramPercent);
    }

    [Fact]
    public void FromArgs_Throws_On_Invalid_Percentage_FromCli()
    {
        using var _ = new EnvironmentVariableScope(
            ("NBN_WORKER_CPU_PCT", null),
            ("NBN_WORKER_RAM_PCT", null),
            ("NBN_WORKER_STORAGE_PCT", null),
            ("NBN_WORKER_GPU_PCT", null),
            ("NBN_WORKER_GPU_COMPUTE_PCT", null),
            ("NBN_WORKER_GPU_VRAM_PCT", null));

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
            ("NBN_WORKER_GPU_PCT", "oops"),
            ("NBN_WORKER_GPU_COMPUTE_PCT", null),
            ("NBN_WORKER_GPU_VRAM_PCT", null));

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
            new WorkerResourceAvailability(cpuPercent: 50, ramPercent: 25, storagePercent: 10, gpuComputePercent: 0, gpuVramPercent: 35));

        Assert.Equal((uint)8, scaled.CpuCores);
        Assert.Equal((ulong)1_000, scaled.RamFreeBytes);
        Assert.Equal((ulong)10_000, scaled.StorageFreeBytes);
        Assert.Equal(80f, scaled.CpuScore);
        Assert.True(scaled.HasGpu);
        Assert.Equal("gpu", scaled.GpuName);
        Assert.Equal((ulong)20_000, scaled.VramFreeBytes);
        Assert.Equal(60f, scaled.GpuScore);
        Assert.True(scaled.IlgpuCudaAvailable);
        Assert.True(scaled.IlgpuOpenclAvailable);
        Assert.Equal((uint)50, scaled.CpuLimitPercent);
        Assert.Equal((uint)25, scaled.RamLimitPercent);
        Assert.Equal((uint)10, scaled.StorageLimitPercent);
        Assert.Equal((uint)0, scaled.GpuComputeLimitPercent);
        Assert.Equal((uint)35, scaled.GpuVramLimitPercent);
    }

    [Fact]
    public void WorkerCapabilityScaling_PublishesExplicitLimitMetadata()
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
            new WorkerResourceAvailability(cpuPercent: 1, ramPercent: 100, storagePercent: 100, gpuComputePercent: 100, gpuVramPercent: 100));

        Assert.Equal((uint)1, scaled.CpuCores);
        Assert.Equal((uint)1, scaled.CpuLimitPercent);
    }

    [Fact]
    public async Task WorkerNodeSnapshot_ExposesConfiguredResourceAvailability()
    {
        var system = new ActorSystem();
        var workerPid = system.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(
                Guid.NewGuid(),
                "127.0.0.1:12041",
                resourceAvailability: new WorkerResourceAvailability(cpuPercent: 130, ramPercent: -20, storagePercent: 45, gpuComputePercent: 0, gpuVramPercent: 25))));

        var snapshot = await system.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());

        Assert.Equal(100, snapshot.ResourceAvailability.CpuPercent);
        Assert.Equal(0, snapshot.ResourceAvailability.RamPercent);
        Assert.Equal(45, snapshot.ResourceAvailability.StoragePercent);
        Assert.Equal(0, snapshot.ResourceAvailability.GpuComputePercent);
        Assert.Equal(25, snapshot.ResourceAvailability.GpuVramPercent);

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
