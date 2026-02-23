using Nbn.Runtime.SettingsMonitor;
using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Tests.SettingsMonitor;

public sealed class SettingsMonitorWorkerInventoryTests
{
    [Fact]
    public async Task StoreSnapshotQuery_ReturnsLatestCapabilityPerWorker()
    {
        using var db = new TempDatabaseScope();
        var store = new SettingsMonitorStore(db.DatabasePath);
        await store.InitializeAsync();

        var workerA = Guid.NewGuid();
        var workerB = Guid.NewGuid();

        await store.UpsertNodeAsync(new NodeRegistration(workerA, "worker-a", "127.0.0.1:12040", "RegionHost"), timeMs: 100);
        await store.UpsertNodeAsync(new NodeRegistration(workerB, "worker-b", "127.0.0.1:12041", "RegionHost"), timeMs: 100);

        await store.RecordHeartbeatAsync(new NodeHeartbeat(
            workerA,
            200,
            new NodeCapabilities(
                CpuCores: 8,
                RamFreeBytes: 1_024,
                HasGpu: true,
                GpuName: "GPU-old",
                VramFreeBytes: 2_048,
                CpuScore: 10.5f,
                GpuScore: 20.5f,
                IlgpuCudaAvailable: true,
                IlgpuOpenclAvailable: false)));

        await store.RecordHeartbeatAsync(new NodeHeartbeat(
            workerA,
            250,
            new NodeCapabilities(
                CpuCores: 16,
                RamFreeBytes: 4_096,
                HasGpu: true,
                GpuName: "GPU-new",
                VramFreeBytes: 8_192,
                CpuScore: 30.5f,
                GpuScore: 40.5f,
                IlgpuCudaAvailable: true,
                IlgpuOpenclAvailable: true)));

        await store.MarkNodeOfflineAsync(workerB, timeMs: 260);

        var snapshot = await store.GetWorkerInventorySnapshotAsync();

        Assert.True(snapshot.SnapshotMs > 0);
        Assert.Equal(2, snapshot.Workers.Count);

        var rowA = snapshot.Workers.Single(worker => worker.NodeId == workerA);
        Assert.True(rowA.IsAlive);
        Assert.True(rowA.HasCapabilities);
        Assert.True(rowA.IsReady);
        Assert.Equal(250, rowA.CapabilityTimeMs);
        Assert.Equal((uint)16, rowA.CpuCores);
        Assert.Equal(4_096, rowA.RamFreeBytes);
        Assert.True(rowA.HasGpu);
        Assert.Equal("GPU-new", rowA.GpuName);
        Assert.Equal(8_192, rowA.VramFreeBytes);
        Assert.Equal(30.5f, rowA.CpuScore);
        Assert.Equal(40.5f, rowA.GpuScore);
        Assert.True(rowA.IlgpuCudaAvailable);
        Assert.True(rowA.IlgpuOpenclAvailable);

        var rowB = snapshot.Workers.Single(worker => worker.NodeId == workerB);
        Assert.False(rowB.IsAlive);
        Assert.False(rowB.HasCapabilities);
        Assert.False(rowB.IsReady);
        Assert.Equal(0, rowB.CapabilityTimeMs);
        Assert.Equal((uint)0, rowB.CpuCores);
    }

    [Fact]
    public async Task ActorSnapshotRequest_MergesRuntimeNodeStateWithStoredCapabilities()
    {
        using var db = new TempDatabaseScope();
        var store = new SettingsMonitorStore(db.DatabasePath);
        await store.InitializeAsync();

        var persistedWorker = Guid.NewGuid();
        await store.UpsertNodeAsync(new NodeRegistration(persistedWorker, "persisted-worker", "127.0.0.1:13040", "RegionHost"), timeMs: 100);
        await store.RecordHeartbeatAsync(new NodeHeartbeat(
            persistedWorker,
            120,
            new NodeCapabilities(
                CpuCores: 12,
                RamFreeBytes: 2_048,
                HasGpu: false,
                GpuName: null,
                VramFreeBytes: 0,
                CpuScore: 12.5f,
                GpuScore: 0.0f,
                IlgpuCudaAvailable: false,
                IlgpuOpenclAvailable: false)));

        var transientWorker = Guid.NewGuid();
        var system = new ActorSystem();
        var root = system.Root;
        var actor = root.Spawn(Props.FromProducer(() => new SettingsMonitorActor(store)));

        root.Send(actor, new ProtoSettings.NodeOffline
        {
            NodeId = persistedWorker.ToProtoUuid(),
            LogicalName = "runtime-offline-worker"
        });

        root.Send(actor, new ProtoSettings.NodeHeartbeat
        {
            NodeId = transientWorker.ToProtoUuid(),
            TimeMs = 200,
            Caps = new ProtoSettings.NodeCapabilities
            {
                CpuCores = 4,
                RamFreeBytes = 512,
                HasGpu = false,
                GpuName = string.Empty,
                VramFreeBytes = 0,
                CpuScore = 1.0f,
                GpuScore = 0.0f,
                IlgpuCudaAvailable = false,
                IlgpuOpenclAvailable = false
            }
        });

        var response = await root.RequestAsync<ProtoSettings.WorkerInventorySnapshotResponse>(
            actor,
            new ProtoSettings.WorkerInventorySnapshotRequest());

        Assert.NotNull(response);
        Assert.True(response.SnapshotMs > 0);
        Assert.Equal(2, response.Workers.Count);

        var persisted = response.Workers.Single(worker => worker.NodeId.TryToGuid(out var id) && id == persistedWorker);
        Assert.Equal("runtime-offline-worker", persisted.LogicalName);
        Assert.False(persisted.IsAlive);
        Assert.True(persisted.HasCapabilities);
        Assert.False(persisted.IsReady);
        Assert.Equal((ulong)120, persisted.CapabilityTimeMs);
        Assert.Equal((uint)12, persisted.Capabilities.CpuCores);

        var transient = response.Workers.Single(worker => worker.NodeId.TryToGuid(out var id) && id == transientWorker);
        Assert.True(transient.IsAlive);
        Assert.False(transient.HasCapabilities);
        Assert.False(transient.IsReady);
        Assert.Equal((ulong)0, transient.CapabilityTimeMs);
        Assert.Equal((uint)0, transient.Capabilities.CpuCores);
    }

    private sealed class TempDatabaseScope : IDisposable
    {
        private readonly string _directoryPath;
        public string DatabasePath { get; }

        public TempDatabaseScope()
        {
            _directoryPath = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directoryPath);
            DatabasePath = Path.Combine(_directoryPath, "settings-monitor.db");
        }

        public void Dispose()
        {
            try
            {
                if (Directory.Exists(_directoryPath))
                {
                    Directory.Delete(_directoryPath, recursive: true);
                }
            }
            catch
            {
            }
        }
    }
}
