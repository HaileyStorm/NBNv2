using System.Collections;
using System.Reflection;
using Dapper;
using Microsoft.Data.Sqlite;
using Nbn.Runtime.SettingsMonitor;
using Nbn.Runtime.WorkerNode;
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
        var timeProvider = new MutableTimeProvider(100);
        var store = new SettingsMonitorStore(db.DatabasePath, timeProvider);
        await store.InitializeAsync();

        var workerA = Guid.NewGuid();
        var workerB = Guid.NewGuid();

        await store.UpsertNodeAsync(new NodeRegistration(workerA, "worker-a", "127.0.0.1:12040", "RegionHost"), timeMs: 100);
        await store.UpsertNodeAsync(new NodeRegistration(workerB, "worker-b", "127.0.0.1:12041", "RegionHost"), timeMs: 100);

        timeProvider.SetUtcNowMs(200);
        await store.RecordHeartbeatAsync(new NodeHeartbeat(
            workerA,
            200,
            CreateCapabilities(
                cpuCores: 8,
                ramFreeBytes: 1_024,
                storageFreeBytes: 10_240,
                hasGpu: true,
                gpuName: "GPU-old",
                vramFreeBytes: 2_048,
                cpuScore: 10.5f,
                gpuScore: 20.5f,
                ilgpuCudaAvailable: true,
                ilgpuOpenclAvailable: false)));

        timeProvider.SetUtcNowMs(250);
        await store.RecordHeartbeatAsync(new NodeHeartbeat(
            workerA,
            250,
            CreateCapabilities(
                cpuCores: 16,
                ramFreeBytes: 4_096,
                storageFreeBytes: 20_480,
                hasGpu: true,
                gpuName: "GPU-new",
                vramFreeBytes: 8_192,
                cpuScore: 30.5f,
                gpuScore: 40.5f,
                ilgpuCudaAvailable: true,
                ilgpuOpenclAvailable: true)));

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
        Assert.Equal(20_480, rowA.StorageFreeBytes);
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
        var timeProvider = new MutableTimeProvider(100);
        var store = new SettingsMonitorStore(db.DatabasePath, timeProvider);
        await store.InitializeAsync();

        var persistedWorker = Guid.NewGuid();
        await store.UpsertNodeAsync(new NodeRegistration(persistedWorker, "persisted-worker", "127.0.0.1:13040", "RegionHost"), timeMs: 100);
        timeProvider.SetUtcNowMs(120);
        await store.RecordHeartbeatAsync(new NodeHeartbeat(
            persistedWorker,
            120,
            CreateCapabilities(
                cpuCores: 12,
                ramFreeBytes: 2_048,
                storageFreeBytes: 8_192,
                cpuScore: 12.5f)));

        timeProvider.SetUtcNowMs(200);
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
                StorageFreeBytes = 4_096,
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
        Assert.Equal((ulong)8_192, persisted.Capabilities.StorageFreeBytes);

        var transient = response.Workers.Single(worker => worker.NodeId.TryToGuid(out var id) && id == transientWorker);
        Assert.True(transient.IsAlive);
        Assert.False(transient.HasCapabilities);
        Assert.False(transient.IsReady);
        Assert.Equal((ulong)0, transient.CapabilityTimeMs);
        Assert.Equal((uint)0, transient.Capabilities.CpuCores);
        Assert.Equal((ulong)0, transient.Capabilities.StorageFreeBytes);
    }

    [Fact]
    public async Task StoreSnapshotQuery_PreservesScaledWorkerCapabilities()
    {
        using var db = new TempDatabaseScope();
        var timeProvider = new MutableTimeProvider(100);
        var store = new SettingsMonitorStore(db.DatabasePath, timeProvider);
        await store.InitializeAsync();

        var worker = Guid.NewGuid();
        await store.UpsertNodeAsync(new NodeRegistration(worker, "scaled-worker", "127.0.0.1:14040", "worker-node"), timeMs: 100);

        var scaled = WorkerCapabilityScaling.ApplyScale(
            new ProtoSettings.NodeCapabilities
            {
                CpuCores = 16,
                RamFreeBytes = 40_000,
                RamTotalBytes = 80_000,
                StorageFreeBytes = 80_000,
                StorageTotalBytes = 120_000,
                HasGpu = true,
                GpuName = "gpu0",
                VramFreeBytes = 20_000,
                VramTotalBytes = 24_000,
                CpuScore = 100f,
                GpuScore = 50f,
                IlgpuCudaAvailable = true,
                IlgpuOpenclAvailable = true
            },
            new WorkerResourceAvailability(cpuPercent: 25, ramPercent: 50, storagePercent: 10, gpuComputePercent: 0, gpuVramPercent: 0));

        timeProvider.SetUtcNowMs(200);
        await store.RecordHeartbeatAsync(new NodeHeartbeat(
            worker,
            200,
            CreateCapabilities(
                cpuCores: scaled.CpuCores,
                ramFreeBytes: (long)scaled.RamFreeBytes,
                ramTotalBytes: (long)scaled.RamTotalBytes,
                storageFreeBytes: (long)scaled.StorageFreeBytes,
                storageTotalBytes: (long)scaled.StorageTotalBytes,
                hasGpu: scaled.HasGpu,
                gpuName: scaled.GpuName,
                vramFreeBytes: (long)scaled.VramFreeBytes,
                vramTotalBytes: (long)scaled.VramTotalBytes,
                cpuScore: scaled.CpuScore,
                gpuScore: scaled.GpuScore,
                ilgpuCudaAvailable: scaled.IlgpuCudaAvailable,
                ilgpuOpenclAvailable: scaled.IlgpuOpenclAvailable,
                cpuLimitPercent: scaled.CpuLimitPercent,
                ramLimitPercent: scaled.RamLimitPercent,
                storageLimitPercent: scaled.StorageLimitPercent,
                gpuComputeLimitPercent: scaled.GpuComputeLimitPercent,
                gpuVramLimitPercent: scaled.GpuVramLimitPercent)));

        var snapshot = await store.GetWorkerInventorySnapshotAsync();
        var row = snapshot.Workers.Single(entry => entry.NodeId == worker);

        Assert.Equal((uint)16, row.CpuCores);
        Assert.Equal(40_000, row.RamFreeBytes);
        Assert.Equal(80_000, row.StorageFreeBytes);
        Assert.True(row.HasGpu);
        Assert.Equal("gpu0", row.GpuName);
        Assert.Equal(20_000, row.VramFreeBytes);
        Assert.Equal(100f, row.CpuScore);
        Assert.Equal(50f, row.GpuScore);
        Assert.True(row.IlgpuCudaAvailable);
        Assert.True(row.IlgpuOpenclAvailable);
        Assert.Equal((uint)25, row.CpuLimitPercent);
        Assert.Equal((uint)50, row.RamLimitPercent);
        Assert.Equal((uint)10, row.StorageLimitPercent);
        Assert.Equal((uint)0, row.GpuComputeLimitPercent);
        Assert.Equal((uint)0, row.GpuVramLimitPercent);
    }

    [Fact]
    public async Task RecordHeartbeatAsync_DuplicateTimestampForSameWorker_UpdatesCapabilitiesInsteadOfFailing()
    {
        using var db = new TempDatabaseScope();
        var timeProvider = new MutableTimeProvider(100);
        var store = new SettingsMonitorStore(db.DatabasePath, timeProvider);
        await store.InitializeAsync();

        var worker = Guid.NewGuid();
        await store.UpsertNodeAsync(new NodeRegistration(worker, "worker-a", "127.0.0.1:12040", "worker-node"), timeMs: 100);

        await store.RecordHeartbeatAsync(new NodeHeartbeat(
            worker,
            250,
            CreateCapabilities(
                cpuCores: 8,
                ramFreeBytes: 1_024,
                storageFreeBytes: 10_240,
                cpuScore: 10f)));

        await store.RecordHeartbeatAsync(new NodeHeartbeat(
            worker,
            250,
            CreateCapabilities(
                cpuCores: 16,
                ramFreeBytes: 4_096,
                storageFreeBytes: 20_480,
                cpuScore: 30f)));

        var snapshot = await store.GetWorkerInventorySnapshotAsync();
        var row = snapshot.Workers.Single(entry => entry.NodeId == worker);

        Assert.True(row.IsAlive);
        Assert.True(row.HasCapabilities);
        Assert.True(row.IsReady);
        Assert.Equal(250, row.CapabilityTimeMs);
        Assert.Equal((uint)16, row.CpuCores);
        Assert.Equal(4_096, row.RamFreeBytes);
        Assert.Equal(20_480, row.StorageFreeBytes);
        Assert.Equal(30f, row.CpuScore);
    }

    [Fact]
    public async Task PruneStaleNodeCapabilitiesAsync_RemovesOldRowsButKeepsLatestPerWorker()
    {
        using var db = new TempDatabaseScope();
        var timeProvider = new MutableTimeProvider(100);
        var store = new SettingsMonitorStore(db.DatabasePath, timeProvider);
        await store.InitializeAsync();

        var worker = Guid.NewGuid();
        await store.UpsertNodeAsync(new NodeRegistration(worker, "worker-a", "127.0.0.1:12040", "worker-node"), timeMs: 100);
        await store.RecordHeartbeatAsync(new NodeHeartbeat(worker, 100, CreateCapabilities(cpuCores: 8, ramFreeBytes: 1_024, storageFreeBytes: 10_240, cpuScore: 1f)));
        await store.RecordHeartbeatAsync(new NodeHeartbeat(worker, 200, CreateCapabilities(cpuCores: 8, ramFreeBytes: 2_048, storageFreeBytes: 10_240, cpuScore: 2f)));
        await store.RecordHeartbeatAsync(new NodeHeartbeat(worker, 300, CreateCapabilities(cpuCores: 8, ramFreeBytes: 3_072, storageFreeBytes: 10_240, cpuScore: 3f)));

        var deleted = await store.PruneStaleNodeCapabilitiesAsync(cutoffMs: 250);

        Assert.Equal(2, deleted);
        await using var connection = new SqliteConnection($"Data Source={db.DatabasePath}");
        var remaining = (await connection.QueryAsync<long>(
                "SELECT time_ms FROM node_capabilities WHERE node_id = @node_id ORDER BY time_ms;",
                new { node_id = worker.ToString("D") }))
            .ToArray();
        Assert.Equal(new[] { 300L }, remaining);

        var snapshot = await store.GetWorkerInventorySnapshotAsync();
        var row = Assert.Single(snapshot.Workers);
        Assert.Equal(300, row.CapabilityTimeMs);
        Assert.Equal(3_072, row.RamFreeBytes);
        Assert.Equal(3f, row.CpuScore);
    }

    [Fact]
    public async Task ObservedSettingsCache_PrunesOldKeys_WhenKeyChurnExceedsCap()
    {
        using var db = new TempDatabaseScope();
        var store = new SettingsMonitorStore(db.DatabasePath);
        await store.InitializeAsync();
        var actor = new SettingsMonitorActor(store);
        var remember = typeof(SettingsMonitorActor).GetMethod(
            "RememberObservedSetting",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(remember);

        for (var index = 0; index < 4_200; index++)
        {
            remember!.Invoke(actor, new object[] { $"key.{index:0000}", "value", (long)index });
        }

        var observed = GetObservedSettings(actor);
        Assert.Equal(4_096, observed.Count);
        Assert.False(observed.Contains("key.0000"));
        Assert.True(observed.Contains("key.4199"));

        var shouldPublish = typeof(SettingsMonitorActor).GetMethod(
            "ShouldPublishObservedSetting",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(shouldPublish);
        Assert.False((bool)shouldPublish!.Invoke(actor, new object[] { "key.0000", "value", 0L })!);
        Assert.True((bool)shouldPublish.Invoke(actor, new object[] { "key.new", "value", 99_999L })!);
    }

    private static NodeCapabilities CreateCapabilities(
        uint cpuCores,
        long ramFreeBytes,
        long storageFreeBytes,
        bool hasGpu = false,
        string? gpuName = null,
        long vramFreeBytes = 0,
        float cpuScore = 0f,
        float gpuScore = 0f,
        bool ilgpuCudaAvailable = false,
        bool ilgpuOpenclAvailable = false,
        long ramTotalBytes = 8_192,
        long storageTotalBytes = 32_768,
        long vramTotalBytes = 4_096,
        uint cpuLimitPercent = 100,
        uint ramLimitPercent = 100,
        uint storageLimitPercent = 100,
        uint gpuComputeLimitPercent = 100,
        uint gpuVramLimitPercent = 100,
        float processCpuLoadPercent = 0f,
        long processRamUsedBytes = 0)
        => new(
            CpuCores: cpuCores,
            RamFreeBytes: ramFreeBytes,
            StorageFreeBytes: storageFreeBytes,
            HasGpu: hasGpu,
            GpuName: gpuName,
            VramFreeBytes: vramFreeBytes,
            CpuScore: cpuScore,
            GpuScore: gpuScore,
            IlgpuCudaAvailable: ilgpuCudaAvailable,
            IlgpuOpenclAvailable: ilgpuOpenclAvailable,
            RamTotalBytes: ramTotalBytes,
            StorageTotalBytes: storageTotalBytes,
            VramTotalBytes: vramTotalBytes,
            CpuLimitPercent: cpuLimitPercent,
            RamLimitPercent: ramLimitPercent,
            StorageLimitPercent: storageLimitPercent,
            GpuComputeLimitPercent: gpuComputeLimitPercent,
            GpuVramLimitPercent: gpuVramLimitPercent,
            ProcessCpuLoadPercent: processCpuLoadPercent,
            ProcessRamUsedBytes: processRamUsedBytes);

    private static IDictionary GetObservedSettings(SettingsMonitorActor actor)
    {
        var field = typeof(SettingsMonitorActor).GetField("_observedSettings", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        return Assert.IsAssignableFrom<IDictionary>(field!.GetValue(actor));
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

    private sealed class MutableTimeProvider : TimeProvider
    {
        private DateTimeOffset _utcNow;

        public MutableTimeProvider(long utcNowMs)
        {
            _utcNow = DateTimeOffset.FromUnixTimeMilliseconds(utcNowMs);
        }

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void SetUtcNowMs(long utcNowMs)
        {
            _utcNow = DateTimeOffset.FromUnixTimeMilliseconds(utcNowMs);
        }
    }
}
