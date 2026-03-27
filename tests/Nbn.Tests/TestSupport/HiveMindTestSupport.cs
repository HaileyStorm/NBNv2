using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Tests.TestSupport;

internal static class HiveMindTestSupport
{
    public static void PrimeWorkers(IRootContext root, PID hiveMind, PID workerPid, Guid workerId)
        => PrimeWorkers(root, hiveMind, (workerPid, workerId, true, true));

    public static void PrimeWorkers(
        IRootContext root,
        PID hiveMind,
        params (PID WorkerPid, Guid WorkerId, bool IsReady, bool IsAlive)[] workers)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nowMs,
            Workers =
            {
                workers.Select(worker => BuildWorker(
                    worker.WorkerId,
                    isAlive: worker.IsAlive,
                    isReady: worker.IsReady,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: string.Empty,
                    rootActorName: worker.WorkerPid.Id))
            }
        });
    }

    public static ProtoSettings.WorkerReadinessCapability BuildWorker(
        Guid nodeId,
        bool isAlive,
        bool isReady,
        long lastSeenMs,
        long capabilityTimeMs,
        string address,
        string rootActorName)
        => new()
        {
            NodeId = nodeId.ToProtoUuid(),
            Address = address,
            RootActorName = rootActorName,
            IsAlive = isAlive,
            IsReady = isReady,
            LastSeenMs = lastSeenMs > 0 ? (ulong)lastSeenMs : 0,
            HasCapabilities = capabilityTimeMs > 0,
            CapabilityTimeMs = capabilityTimeMs > 0 ? (ulong)capabilityTimeMs : 0,
            Capabilities = new ProtoSettings.NodeCapabilities
            {
                CpuCores = 8,
                RamFreeBytes = 8UL * 1024 * 1024 * 1024,
                RamTotalBytes = 16UL * 1024 * 1024 * 1024,
                StorageFreeBytes = 64UL * 1024 * 1024 * 1024,
                StorageTotalBytes = 128UL * 1024 * 1024 * 1024,
                HasGpu = true,
                VramFreeBytes = 8UL * 1024 * 1024 * 1024,
                VramTotalBytes = 16UL * 1024 * 1024 * 1024,
                CpuScore = 40f,
                GpuScore = 80f,
                CpuLimitPercent = 100,
                RamLimitPercent = 100,
                StorageLimitPercent = 100,
                GpuComputeLimitPercent = 100,
                GpuVramLimitPercent = 100
            }
        };

    public static HiveMindOptions CreateHiveMindOptions(
        int assignmentTimeoutMs = 1_000,
        int retryBackoffMs = 10,
        int maxRetries = 1,
        int reconcileTimeoutMs = 1_000,
        int rescheduleMinTicks = 10,
        int rescheduleMinMinutes = 1,
        int rescheduleQuietMs = 50)
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
            RescheduleMinTicks: rescheduleMinTicks,
            RescheduleMinMinutes: rescheduleMinMinutes,
            RescheduleQuietMs: rescheduleQuietMs,
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
            WorkerInventoryRefreshMs: 2_000,
            WorkerInventoryStaleAfterMs: 10_000,
            PlacementAssignmentTimeoutMs: assignmentTimeoutMs,
            PlacementAssignmentRetryBackoffMs: retryBackoffMs,
            PlacementAssignmentMaxRetries: maxRetries,
            PlacementReconcileTimeoutMs: reconcileTimeoutMs);
}
