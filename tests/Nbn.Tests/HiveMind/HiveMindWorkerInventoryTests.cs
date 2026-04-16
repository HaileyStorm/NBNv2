using System.Reflection;
using Nbn.Proto.Control;
using Nbn.Runtime.HiveMind;
using Nbn.Runtime.WorkerNode;
using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Tests.HiveMind;

public sealed class HiveMindWorkerInventoryTests
{
    [Fact]
    public async Task PlacementWorkerInventory_Excludes_Unready_Offline_And_Stale_Workers()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var staleAfterMs = 5_000;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(workerInventoryRefreshMs: 1000, workerInventoryStaleAfterMs: staleAfterMs))));

        var freshWorkerId = Guid.NewGuid();
        var staleWorkerId = Guid.NewGuid();
        var unreadyWorkerId = Guid.NewGuid();
        var offlineWorkerId = Guid.NewGuid();
        var scaled = WorkerCapabilityScaling.ApplyScale(
            new ProtoSettings.NodeCapabilities
            {
                CpuCores = 24,
                RamFreeBytes = 16UL * 1024 * 1024 * 1024,
                RamTotalBytes = 32UL * 1024 * 1024 * 1024,
                StorageFreeBytes = 100UL * 1024 * 1024 * 1024,
                StorageTotalBytes = 150UL * 1024 * 1024 * 1024,
                HasGpu = true,
                VramFreeBytes = 24UL * 1024 * 1024 * 1024,
                VramTotalBytes = 32UL * 1024 * 1024 * 1024,
                CpuScore = 89f,
                GpuScore = 88f
            },
            new WorkerResourceAvailability(cpuPercent: 50, ramPercent: 50, storagePercent: 50, gpuComputePercent: 50, gpuVramPercent: 50));

        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nowMs,
            Workers =
            {
                BuildWorker(
                    freshWorkerId,
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "worker-a:12040",
                    rootActorName: "region-host",
                    cpuCores: scaled.CpuCores,
                    ramFreeBytes: (long)scaled.RamFreeBytes,
                    ramTotalBytes: (long)scaled.RamTotalBytes,
                    storageFreeBytes: (long)scaled.StorageFreeBytes,
                    storageTotalBytes: (long)scaled.StorageTotalBytes,
                    hasGpu: scaled.HasGpu,
                    vramFreeBytes: (long)scaled.VramFreeBytes,
                    vramTotalBytes: (long)scaled.VramTotalBytes,
                    cpuScore: scaled.CpuScore,
                    gpuScore: scaled.GpuScore,
                    cpuLimitPercent: scaled.CpuLimitPercent,
                    ramLimitPercent: scaled.RamLimitPercent,
                    storageLimitPercent: scaled.StorageLimitPercent,
                    gpuComputeLimitPercent: scaled.GpuComputeLimitPercent,
                    gpuVramLimitPercent: scaled.GpuVramLimitPercent),
                BuildWorker(
                    staleWorkerId,
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs - staleAfterMs - 1,
                    capabilityTimeMs: nowMs - staleAfterMs - 1,
                    address: "worker-stale:12040",
                    rootActorName: "region-host"),
                BuildWorker(
                    unreadyWorkerId,
                    isAlive: true,
                    isReady: false,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "worker-unready:12040",
                    rootActorName: "region-host"),
                BuildWorker(
                    offlineWorkerId,
                    isAlive: false,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "worker-offline:12040",
                    rootActorName: "region-host")
            }
        });

        var inventory = await root.RequestAsync<PlacementWorkerInventory>(
            hiveMind,
            new PlacementWorkerInventoryRequest());

        Assert.Equal((ulong)nowMs, inventory.SnapshotMs);
        Assert.Equal((uint)4, inventory.TotalWorkersSeen);
        Assert.Single(inventory.Workers);
        Assert.Equal(3, inventory.ExcludedWorkers.Count);
        Assert.Contains(inventory.ExclusionCounts, static entry => entry.ReasonCode == "not_ready" && entry.Count == 1);
        Assert.Contains(inventory.ExclusionCounts, static entry => entry.ReasonCode == "not_alive" && entry.Count == 1);
        Assert.Contains(inventory.ExclusionCounts, static entry => entry.ReasonCode == "stale_last_seen" && entry.Count == 1);
        Assert.Contains(inventory.ExclusionCounts, static entry => entry.ReasonCode == "stale_capabilities" && entry.Count == 1);

        var worker = inventory.Workers[0];
        Assert.Equal(freshWorkerId.ToProtoUuid().Value, worker.WorkerNodeId.Value);
        Assert.True(worker.IsAlive);
        Assert.Equal("worker-a:12040", worker.WorkerAddress);
        Assert.Equal("region-host", worker.WorkerRootActorName);
        Assert.Equal(scaled.CpuCores, worker.CpuCores);
        Assert.Equal(scaled.RamFreeBytes, worker.RamFreeBytes);
        Assert.Equal(scaled.StorageFreeBytes, worker.StorageFreeBytes);
        Assert.Equal(scaled.CpuScore, worker.CpuScore);
        Assert.Equal(scaled.GpuScore, worker.GpuScore);
        Assert.True(worker.HasGpu);
        Assert.Equal(scaled.VramFreeBytes, worker.VramFreeBytes);
        Assert.Equal(scaled.RamTotalBytes, worker.RamTotalBytes);
        Assert.Equal(scaled.StorageTotalBytes, worker.StorageTotalBytes);
        Assert.Equal(scaled.VramTotalBytes, worker.VramTotalBytes);
        Assert.Equal(scaled.CpuLimitPercent, worker.CpuLimitPercent);
        Assert.Equal(scaled.RamLimitPercent, worker.RamLimitPercent);
        Assert.Equal(scaled.StorageLimitPercent, worker.StorageLimitPercent);
        Assert.Equal(scaled.GpuComputeLimitPercent, worker.GpuComputeLimitPercent);
        Assert.Equal(scaled.GpuVramLimitPercent, worker.GpuVramLimitPercent);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task PlacementWorkerInventory_IncludesGeneratedRootSiblings_ForConfiguredCustomWorkerRoot()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(workerInventoryRefreshMs: 1000, workerInventoryStaleAfterMs: 5_000))));

        root.Send(hiveMind, new ProtoSettings.SettingValue
        {
            Key = ServiceEndpointSettings.WorkerNodeKey,
            Value = ServiceEndpointSettings.EncodeValue("127.0.0.1:12041", "gpu-worker")
        });

        var firstWorkerId = Guid.NewGuid();
        var secondWorkerId = Guid.NewGuid();
        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nowMs,
            Workers =
            {
                BuildWorker(
                    firstWorkerId,
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "127.0.0.1:12041",
                    rootActorName: "gpu-worker",
                    logicalName: "custom-worker"),
                BuildWorker(
                    secondWorkerId,
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "127.0.0.1:12041",
                    rootActorName: "gpu-worker-2",
                    logicalName: "custom-worker")
            }
        });

        var inventory = await root.RequestAsync<PlacementWorkerInventory>(
            hiveMind,
            new PlacementWorkerInventoryRequest());

        Assert.Equal(2, inventory.Workers.Count);
        Assert.Contains(inventory.Workers, worker => worker.WorkerNodeId.Value == firstWorkerId.ToProtoUuid().Value);
        Assert.Contains(inventory.Workers, worker => worker.WorkerNodeId.Value == secondWorkerId.ToProtoUuid().Value);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task WorkerInventory_PrunesUnseenStaleUntrackedWorkers()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var staleAfterMs = 1_000;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(workerInventoryRefreshMs: 100, workerInventoryStaleAfterMs: staleAfterMs))));

        var retainedWorkerId = Guid.NewGuid();
        var staleWorkerId = Guid.NewGuid();
        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nowMs,
            Workers =
            {
                BuildWorker(
                    retainedWorkerId,
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "worker-retained:12040",
                    rootActorName: "region-host"),
                BuildWorker(
                    staleWorkerId,
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs - staleAfterMs - 1,
                    capabilityTimeMs: nowMs - staleAfterMs - 1,
                    address: "worker-stale:12040",
                    rootActorName: "region-host")
            }
        });

        var nextSnapshotMs = nowMs + staleAfterMs + 100;
        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nextSnapshotMs,
            Workers =
            {
                BuildWorker(
                    retainedWorkerId,
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nextSnapshotMs,
                    capabilityTimeMs: nextSnapshotMs,
                    address: "worker-retained:12040",
                    rootActorName: "region-host")
            }
        });

        var inventory = await root.RequestAsync<PlacementWorkerInventory>(
            hiveMind,
            new PlacementWorkerInventoryRequest());

        Assert.Equal((uint)1, inventory.TotalWorkersSeen);
        Assert.Single(inventory.Workers);
        Assert.Equal(retainedWorkerId.ToProtoUuid().Value, inventory.Workers[0].WorkerNodeId.Value);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task PlacementWorkerInventory_Excludes_Workers_Without_PlannerUsable_Capacity()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(workerInventoryRefreshMs: 1000, workerInventoryStaleAfterMs: 15_000))));

        var eligibleWorkerId = Guid.NewGuid();
        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nowMs,
            Workers =
            {
                BuildWorker(
                    Guid.NewGuid(),
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "worker-missing-root:12040",
                    rootActorName: string.Empty,
                    logicalName: "nbn.worker"),
                BuildWorker(
                    Guid.NewGuid(),
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "worker-zero-storage:12040",
                    rootActorName: "worker-node",
                    logicalName: "nbn.worker",
                    storageFreeBytes: 8L * 1024 * 1024 * 1024,
                    storageTotalBytes: 0),
                BuildWorker(
                    Guid.NewGuid(),
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "worker-zero-score:12040",
                    rootActorName: "worker-node",
                    logicalName: "nbn.worker",
                    cpuScore: 0f,
                    hasGpu: false),
                BuildWorker(
                    eligibleWorkerId,
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "worker-ready:12040",
                    rootActorName: "worker-node",
                    logicalName: "nbn.worker")
            }
        });

        var inventory = await root.RequestAsync<PlacementWorkerInventory>(
            hiveMind,
            new PlacementWorkerInventoryRequest());

        Assert.Equal((uint)4, inventory.TotalWorkersSeen);
        Assert.Single(inventory.Workers);
        Assert.Equal(3, inventory.ExcludedWorkers.Count);
        Assert.Contains(inventory.ExclusionCounts, static entry => entry.ReasonCode == "missing_worker_root_actor" && entry.Count == 1);
        Assert.Contains(inventory.ExclusionCounts, static entry => entry.ReasonCode == "no_effective_storage" && entry.Count == 1);
        Assert.Contains(inventory.ExclusionCounts, static entry => entry.ReasonCode == "no_effective_compute_capacity" && entry.Count == 1);
        Assert.Equal(eligibleWorkerId.ToProtoUuid().Value, inventory.Workers[0].WorkerNodeId.Value);
        Assert.Equal("worker-ready:12040", inventory.Workers[0].WorkerAddress);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task PlacementWorkerInventory_Keeps_VramLimited_Workers_When_CpuFallback_Is_Still_Usable()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(workerInventoryRefreshMs: 1000, workerInventoryStaleAfterMs: 15_000))));

        var workerId = Guid.NewGuid();
        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nowMs,
            Workers =
            {
                BuildWorker(
                    workerId,
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "worker-vram-limited:12040",
                    rootActorName: "worker-node",
                    logicalName: "nbn.worker",
                    cpuScore: 35f,
                    hasGpu: true,
                    gpuScore: 90f,
                    vramFreeBytes: 6L * 1024 * 1024 * 1024,
                    vramTotalBytes: 8L * 1024 * 1024 * 1024,
                    gpuVramLimitPercent: 0)
            }
        });

        var inventory = await root.RequestAsync<PlacementWorkerInventory>(
            hiveMind,
            new PlacementWorkerInventoryRequest());

        Assert.Single(inventory.Workers);
        Assert.Empty(inventory.ExcludedWorkers);
        Assert.DoesNotContain(inventory.ExclusionCounts, static entry => entry.ReasonCode == "pressure_violation");
        Assert.Equal(workerId.ToProtoUuid().Value, inventory.Workers[0].WorkerNodeId.Value);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task HiveMind_Pulls_SettingsMonitor_WorkerInventory_Periodically()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var workerId = Guid.NewGuid();

        var firstRequest = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondRequest = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        var settingsProbe = root.Spawn(Props.FromProducer(() => new WorkerInventoryProbe(
            workerId,
            firstRequest,
            secondRequest)));

        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(workerInventoryRefreshMs: 50, workerInventoryStaleAfterMs: 5_000),
            settingsPid: settingsProbe)));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var first = await firstRequest.Task.WaitAsync(cts.Token);
        var second = await secondRequest.Task.WaitAsync(cts.Token);

        Assert.True(first >= 1);
        Assert.True(second >= 2);

        await WaitForAsync(
            async () =>
            {
                var current = await root.RequestAsync<PlacementWorkerInventory>(
                    hiveMind,
                    new PlacementWorkerInventoryRequest());
                return current.Workers.Count == 1;
            },
            timeoutMs: 2_000);

        var inventory = await root.RequestAsync<PlacementWorkerInventory>(
            hiveMind,
            new PlacementWorkerInventoryRequest());

        Assert.Single(inventory.Workers);
        Assert.Equal(workerId.ToProtoUuid().Value, inventory.Workers[0].WorkerNodeId.Value);
        Assert.True(inventory.Workers[0].IsAlive);
        Assert.Equal(4u, inventory.Workers[0].CpuCores);
        Assert.Equal(2UL * 1024 * 1024 * 1024, inventory.Workers[0].RamFreeBytes);
        Assert.Equal(25UL * 1024 * 1024 * 1024, inventory.Workers[0].StorageFreeBytes);
        Assert.False(inventory.Workers[0].HasGpu);
        Assert.Equal(0UL, inventory.Workers[0].VramFreeBytes);
        Assert.Equal(10f, inventory.Workers[0].CpuScore);
        Assert.Equal(0f, inventory.Workers[0].GpuScore);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task PlacementWorkerInventory_Excludes_NonWorker_ServiceNodes_Even_When_Ready()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(workerInventoryRefreshMs: 1000, workerInventoryStaleAfterMs: 15_000))));

        var workerId = Guid.NewGuid();
        var serviceNodeId = Guid.NewGuid();
        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nowMs,
            Workers =
            {
                BuildWorker(
                    workerId,
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "127.0.0.1:12041",
                    rootActorName: "worker-node",
                    logicalName: "nbn.worker"),
                BuildWorker(
                    serviceNodeId,
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "127.0.0.1:12020",
                    rootActorName: "HiveMind",
                    logicalName: "nbn.hivemind")
            }
        });

        var inventory = await root.RequestAsync<PlacementWorkerInventory>(
            hiveMind,
            new PlacementWorkerInventoryRequest());

        Assert.Equal((uint)2, inventory.TotalWorkersSeen);
        Assert.Single(inventory.Workers);
        Assert.Single(inventory.ExcludedWorkers);
        Assert.Contains(inventory.ExclusionCounts, static entry => entry.ReasonCode == "not_worker_candidate" && entry.Count == 1);
        Assert.Equal(workerId.ToProtoUuid().Value, inventory.Workers[0].WorkerNodeId.Value);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task PlacementWorkerInventory_Exposes_InterWorkerLatencyTelemetry()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var actor = new HiveMindActor(CreateOptions(workerInventoryRefreshMs: 1000, workerInventoryStaleAfterMs: 15_000));
        var hiveMind = root.Spawn(Props.FromProducer(() => actor));

        var workerAId = Guid.NewGuid();
        var workerBId = Guid.NewGuid();

        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nowMs,
            Workers =
            {
                BuildWorker(
                    workerAId,
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: string.Empty,
                    rootActorName: "worker-a",
                    logicalName: "nbn.worker"),
                BuildWorker(
                    workerBId,
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: string.Empty,
                    rootActorName: "worker-b",
                    logicalName: "nbn.worker")
            }
        });

        await WaitForAsync(
            async () =>
            {
                var current = await root.RequestAsync<PlacementWorkerInventory>(
                    hiveMind,
                    new PlacementWorkerInventoryRequest());
                return current.Workers.Count == 2;
            },
            timeoutMs: 2_000);

        SetPeerLatencyMeasurement(actor, workerAId, 1.25f, 1, nowMs);
        SetPeerLatencyMeasurement(actor, workerBId, 1.5f, 1, nowMs);

        var inventory = await root.RequestAsync<PlacementWorkerInventory>(
            hiveMind,
            new PlacementWorkerInventoryRequest());
        Assert.NotNull(inventory);
        Assert.Equal(2, inventory.Workers.Count);
        Assert.All(
            inventory.Workers,
            worker =>
            {
                Assert.Equal((uint)1, worker.PeerLatencySampleCount);
                Assert.True(worker.AveragePeerLatencyMs > 0f);
            });

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task PlacementWorkerInventory_UsesSettingsSnapshotClock_ForCrossMachineFreshness()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var actor = new HiveMindActor(CreateOptions(workerInventoryRefreshMs: 1000, workerInventoryStaleAfterMs: 5_000));
        var hiveMind = root.Spawn(Props.FromProducer(() => actor));

        var workerId = Guid.NewGuid();
        const long snapshotMs = 10_000;

        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)snapshotMs,
            Workers =
            {
                BuildWorker(
                    workerId,
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: snapshotMs,
                    capabilityTimeMs: snapshotMs,
                    address: "worker-skewed:12040",
                    rootActorName: "worker-node",
                    logicalName: "nbn.worker")
            }
        });

        var inventory = await root.RequestAsync<PlacementWorkerInventory>(
            hiveMind,
            new PlacementWorkerInventoryRequest());

        var worker = Assert.Single(inventory.Workers);
        Assert.Equal(workerId.ToProtoUuid().Value, worker.WorkerNodeId.Value);
        Assert.True(worker.IsAlive);
        Assert.Equal("worker-skewed:12040", worker.WorkerAddress);

        await system.ShutdownAsync();
    }

    private static ProtoSettings.WorkerReadinessCapability BuildWorker(
        Guid nodeId,
        bool isAlive,
        bool isReady,
        long lastSeenMs,
        long capabilityTimeMs,
        string address,
        string rootActorName,
        string logicalName = "",
        uint cpuCores = 4,
        long ramFreeBytes = 2L * 1024 * 1024 * 1024,
        long storageFreeBytes = 25L * 1024 * 1024 * 1024,
        bool hasGpu = false,
        long vramFreeBytes = 0,
        float cpuScore = 10f,
        float gpuScore = 0f,
        uint cpuLimitPercent = 100,
        uint ramLimitPercent = 100,
        uint storageLimitPercent = 100,
        uint gpuComputeLimitPercent = 100,
        uint gpuVramLimitPercent = 100,
        long ramTotalBytes = 4L * 1024 * 1024 * 1024,
        long storageTotalBytes = 50L * 1024 * 1024 * 1024,
        long vramTotalBytes = 8L * 1024 * 1024 * 1024)
    {
        var hasCapabilities = capabilityTimeMs > 0;
        return new ProtoSettings.WorkerReadinessCapability
        {
            NodeId = nodeId.ToProtoUuid(),
            LogicalName = logicalName,
            Address = address,
            RootActorName = rootActorName,
            IsAlive = isAlive,
            IsReady = isReady,
            LastSeenMs = lastSeenMs > 0 ? (ulong)lastSeenMs : 0,
            HasCapabilities = hasCapabilities,
            CapabilityTimeMs = hasCapabilities ? (ulong)capabilityTimeMs : 0,
            Capabilities = new ProtoSettings.NodeCapabilities
            {
                CpuCores = cpuCores,
                RamFreeBytes = ramFreeBytes > 0 ? (ulong)ramFreeBytes : 0,
                RamTotalBytes = ramTotalBytes > 0 ? (ulong)ramTotalBytes : 0,
                StorageFreeBytes = storageFreeBytes > 0 ? (ulong)storageFreeBytes : 0,
                StorageTotalBytes = storageTotalBytes > 0 ? (ulong)storageTotalBytes : 0,
                HasGpu = hasGpu,
                VramFreeBytes = vramFreeBytes > 0 ? (ulong)vramFreeBytes : 0,
                VramTotalBytes = vramTotalBytes > 0 ? (ulong)vramTotalBytes : 0,
                CpuScore = cpuScore,
                GpuScore = gpuScore,
                CpuLimitPercent = cpuLimitPercent,
                RamLimitPercent = ramLimitPercent,
                StorageLimitPercent = storageLimitPercent,
                GpuComputeLimitPercent = gpuComputeLimitPercent,
                GpuVramLimitPercent = gpuVramLimitPercent
            }
        };
    }

    private static HiveMindOptions CreateOptions(
        int workerInventoryRefreshMs = 2_000,
        int workerInventoryStaleAfterMs = 15_000)
        => new(
            BindHost: "127.0.0.1",
            Port: 0,
            AdvertisedHost: null,
            AdvertisedPort: null,
            TargetTickHz: 50f,
            MinTickHz: 10f,
            ComputeTimeoutMs: 500,
            DeliverTimeoutMs: 500,
            BackpressureDecay: 0.9f,
            BackpressureRecovery: 1.1f,
            LateBackpressureThreshold: 2,
            TimeoutRescheduleThreshold: 3,
            TimeoutPauseThreshold: 6,
            RescheduleMinTicks: 10,
            RescheduleMinMinutes: 1,
            RescheduleQuietMs: 50,
            RescheduleSimulatedMs: 50,
            AutoStart: false,
            EnableOpenTelemetry: false,
            EnableOtelMetrics: false,
            EnableOtelTraces: false,
            EnableOtelConsoleExporter: false,
            OtlpEndpoint: null,
            ServiceName: "nbn.hivemind.tests",
            SettingsDbPath: null,
            SettingsHost: null,
            SettingsPort: 0,
            SettingsName: "SettingsMonitor",
            IoAddress: null,
            IoName: null,
            WorkerInventoryRefreshMs: workerInventoryRefreshMs,
            WorkerInventoryStaleAfterMs: workerInventoryStaleAfterMs);

    private sealed class WorkerInventoryProbe : IActor
    {
        private readonly Guid _workerId;
        private readonly TaskCompletionSource<int> _firstRequest;
        private readonly TaskCompletionSource<int> _secondRequest;
        private int _requestCount;

        public WorkerInventoryProbe(
            Guid workerId,
            TaskCompletionSource<int> firstRequest,
            TaskCompletionSource<int> secondRequest)
        {
            _workerId = workerId;
            _firstRequest = firstRequest;
            _secondRequest = secondRequest;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is ProtoSettings.WorkerInventorySnapshotRequest)
            {
                _requestCount++;
                _firstRequest.TrySetResult(_requestCount);
                if (_requestCount >= 2)
                {
                    _secondRequest.TrySetResult(_requestCount);
                }

                var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                context.Respond(new ProtoSettings.WorkerInventorySnapshotResponse
                {
                    SnapshotMs = (ulong)nowMs,
                    Workers =
                    {
                        BuildWorker(
                            _workerId,
                            isAlive: true,
                            isReady: true,
                            lastSeenMs: nowMs,
                            capabilityTimeMs: nowMs,
                            address: "probe-worker:12040",
                            rootActorName: "probe-root")
                    }
                });
            }

            return Task.CompletedTask;
        }
    }

    private static void SetPeerLatencyMeasurement(
        HiveMindActor actor,
        Guid workerId,
        float averagePeerLatencyMs,
        int sampleCount,
        long snapshotMs)
    {
        var catalogField = typeof(HiveMindActor).GetField("_workerCatalog", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(catalogField);

        var catalog = catalogField!.GetValue(actor);
        Assert.NotNull(catalog);

        var args = new object?[] { workerId, null };
        var found = (bool)catalog!.GetType().GetMethod("TryGetValue")!.Invoke(catalog, args)!;
        Assert.True(found);

        var entry = args[1];
        Assert.NotNull(entry);

        entry!.GetType().GetProperty("AveragePeerLatencyMs")!.SetValue(entry, averagePeerLatencyMs);
        entry.GetType().GetProperty("PeerLatencySampleCount")!.SetValue(entry, sampleCount);
        entry.GetType().GetProperty("PeerLatencySnapshotMs")!.SetValue(entry, snapshotMs);
    }

    private static async Task WaitForAsync(Func<Task<bool>> predicate, int timeoutMs)
    {
        using var cts = new CancellationTokenSource(timeoutMs);
        while (true)
        {
            if (await predicate().ConfigureAwait(false))
            {
                return;
            }

            try
            {
                await Task.Delay(20, cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        throw new TimeoutException($"Condition was not met within {timeoutMs} ms.");
    }
}
