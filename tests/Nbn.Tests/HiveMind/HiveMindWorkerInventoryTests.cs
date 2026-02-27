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
                StorageFreeBytes = 100UL * 1024 * 1024 * 1024,
                HasGpu = true,
                VramFreeBytes = 24UL * 1024 * 1024 * 1024,
                CpuScore = 89f,
                GpuScore = 88f
            },
            new WorkerResourceAvailability(cpuPercent: 50, ramPercent: 50, storagePercent: 50, gpuPercent: 50));

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
                    storageFreeBytes: (long)scaled.StorageFreeBytes,
                    hasGpu: scaled.HasGpu,
                    vramFreeBytes: (long)scaled.VramFreeBytes,
                    cpuScore: scaled.CpuScore,
                    gpuScore: scaled.GpuScore),
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
        Assert.Single(inventory.Workers);

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

        Assert.Single(inventory.Workers);
        Assert.Equal(workerId.ToProtoUuid().Value, inventory.Workers[0].WorkerNodeId.Value);

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
        float gpuScore = 0f)
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
                StorageFreeBytes = storageFreeBytes > 0 ? (ulong)storageFreeBytes : 0,
                HasGpu = hasGpu,
                VramFreeBytes = vramFreeBytes > 0 ? (ulong)vramFreeBytes : 0,
                CpuScore = cpuScore,
                GpuScore = gpuScore
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
}
