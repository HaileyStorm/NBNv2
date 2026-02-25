using System.Reflection;
using Nbn.Proto.Control;
using Nbn.Runtime.HiveMind;
using Nbn.Runtime.WorkerNode;
using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Tests.HiveMind;

public sealed class HiveMindPlacementPlannerTests
{
    [Fact]
    public async Task RequestPlacement_Rejects_When_No_Eligible_Workers()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(workerInventoryStaleAfterMs: 5_000))));

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nowMs,
            Workers =
            {
                BuildWorker(
                    Guid.NewGuid(),
                    isAlive: false,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "offline-worker:12040",
                    rootActorName: "region-host"),
                BuildWorker(
                    Guid.NewGuid(),
                    isAlive: true,
                    isReady: false,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "unready-worker:12040",
                    rootActorName: "region-host"),
                BuildWorker(
                    Guid.NewGuid(),
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs - 6_000,
                    capabilityTimeMs: nowMs - 6_000,
                    address: "stale-worker:12040",
                    rootActorName: "region-host")
            }
        });

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 1,
                OutputWidth = 1
            });

        Assert.False(ack.Accepted);
        Assert.Equal(PlacementFailureReason.PlacementFailureWorkerUnavailable, ack.FailureReason);
        Assert.Equal(PlacementLifecycleState.PlacementLifecycleFailed, ack.LifecycleState);
        Assert.False(string.IsNullOrWhiteSpace(ack.RequestId));

        var lifecycle = await root.RequestAsync<PlacementLifecycleInfo>(
            hiveMind,
            new GetPlacementLifecycle
            {
                BrainId = brainId.ToProtoUuid()
            });

        Assert.Equal(PlacementLifecycleState.PlacementLifecycleFailed, lifecycle.LifecycleState);
        Assert.Equal(PlacementFailureReason.PlacementFailureWorkerUnavailable, lifecycle.FailureReason);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RequestPlacement_Accepts_When_Plan_Is_Produced_And_Stored()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var actor = new HiveMindActor(CreateOptions(workerInventoryStaleAfterMs: 10_000));
        var hiveMind = root.Spawn(Props.FromProducer(() => actor));

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var eligibleWorkerId = Guid.NewGuid();
        var scaled = WorkerCapabilityScaling.ApplyScale(
            new ProtoSettings.NodeCapabilities
            {
                CpuCores = 12,
                RamFreeBytes = 24UL * 1024 * 1024 * 1024,
                StorageFreeBytes = 140UL * 1024 * 1024 * 1024,
                CpuScore = 80f
            },
            new WorkerResourceAvailability(cpuPercent: 50, ramPercent: 25, storagePercent: 30, gpuPercent: 100));
        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nowMs,
            Workers =
            {
                BuildWorker(
                    eligibleWorkerId,
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "worker-a:12040",
                    rootActorName: "region-host",
                    cpuCores: scaled.CpuCores,
                    ramFreeBytes: (long)scaled.RamFreeBytes,
                    storageFreeBytes: (long)scaled.StorageFreeBytes,
                    cpuScore: scaled.CpuScore),
                BuildWorker(
                    Guid.NewGuid(),
                    isAlive: true,
                    isReady: false,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "worker-b:12040",
                    rootActorName: "region-host")
            }
        });

        var brainId = Guid.NewGuid();
        var requestId = "planner-store-check";
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3,
                RequestId = requestId
            });

        Assert.True(ack.Accepted);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, ack.FailureReason);
        Assert.Equal(requestId, ack.RequestId);

        var plannedPlacement = GetPlannedPlacement(actor, brainId);
        Assert.NotNull(plannedPlacement);

        var stored = plannedPlacement!;
        Assert.Equal(ack.PlacementEpoch, stored.PlacementEpoch);
        Assert.Equal(requestId, stored.RequestId);
        Assert.Single(stored.EligibleWorkers);
        Assert.Equal(6, stored.Assignments.Count);
        Assert.Equal(scaled.CpuCores, stored.EligibleWorkers[0].CpuCores);
        Assert.Equal((long)scaled.RamFreeBytes, stored.EligibleWorkers[0].RamFreeBytes);
        Assert.Equal((long)scaled.StorageFreeBytes, stored.EligibleWorkers[0].StorageFreeBytes);
        Assert.Equal(scaled.CpuScore, stored.EligibleWorkers[0].CpuScore);

        foreach (var assignment in stored.Assignments)
        {
            Assert.Equal(ack.PlacementEpoch, assignment.PlacementEpoch);
            Assert.NotNull(assignment.BrainId);
            Assert.NotNull(assignment.WorkerNodeId);
            Assert.True(assignment.BrainId!.TryToGuid(out var assignmentBrainId));
            Assert.True(assignment.WorkerNodeId!.TryToGuid(out var assignmentWorkerId));
            Assert.Equal(brainId, assignmentBrainId);
            Assert.Equal(eligibleWorkerId, assignmentWorkerId);
        }

        await system.ShutdownAsync();
    }

    [Fact]
    public void PlacementPlanner_Uses_Only_Eligible_Workers_And_Is_Deterministic()
    {
        var workerA = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        var workerB = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        var staleWorker = Guid.Parse("cccccccc-cccc-cccc-cccc-cccccccccccc");
        var offlineWorker = Guid.Parse("dddddddd-dddd-dddd-dddd-dddddddddddd");
        var unreadyWorker = Guid.Parse("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee");
        var brainId = Guid.Parse("11111111-2222-3333-4444-555555555555");

        var workers = new[]
        {
            new PlacementPlanner.WorkerCandidate(workerB, "worker-b:12040", "region-host", true, true, true, 8, 8L * 1024 * 1024 * 1024, 40L * 1024 * 1024 * 1024, true, 8L * 1024 * 1024 * 1024, 20f, 70f),
            new PlacementPlanner.WorkerCandidate(staleWorker, "worker-c:12040", "region-host", true, true, false, 8, 8L * 1024 * 1024 * 1024, 40L * 1024 * 1024 * 1024, true, 8L * 1024 * 1024 * 1024, 30f, 65f),
            new PlacementPlanner.WorkerCandidate(workerA, "worker-a:12040", "region-host", true, true, true, 16, 16L * 1024 * 1024 * 1024, 80L * 1024 * 1024 * 1024, true, 16L * 1024 * 1024 * 1024, 40f, 50f),
            new PlacementPlanner.WorkerCandidate(offlineWorker, "worker-d:12040", "region-host", false, true, true, 16, 16L * 1024 * 1024 * 1024, 80L * 1024 * 1024 * 1024, true, 16L * 1024 * 1024 * 1024, 45f, 80f),
            new PlacementPlanner.WorkerCandidate(unreadyWorker, "worker-e:12040", "region-host", true, false, true, 16, 16L * 1024 * 1024 * 1024, 80L * 1024 * 1024 * 1024, false, 0, 35f, 0f)
        };

        var shardPlan = new ShardPlan
        {
            Mode = (ShardPlanMode)1,
            ShardCount = 2
        };
        var plannerInputs = new PlacementPlanner.PlannerInputs(
            BrainId: brainId,
            PlacementEpoch: 2,
            RequestId: "req-42",
            RequestedMs: 100,
            PlannedMs: 101,
            WorkerSnapshotMs: 99,
            ShardStride: 1024,
            RequestedShardPlan: shardPlan,
            Regions: new[]
            {
                new PlacementPlanner.RegionSpan(0, 4),
                new PlacementPlanner.RegionSpan(1, 4096),
                new PlacementPlanner.RegionSpan(31, 2)
            });

        var builtFirst = PlacementPlanner.TryBuildPlan(
            plannerInputs,
            workers,
            out var firstPlan,
            out var firstFailureReason,
            out _);
        var builtSecond = PlacementPlanner.TryBuildPlan(
            plannerInputs,
            workers.Reverse().ToArray(),
            out var secondPlan,
            out var secondFailureReason,
            out _);

        Assert.True(builtFirst);
        Assert.True(builtSecond);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, firstFailureReason);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, secondFailureReason);

        Assert.Equal(new[] { workerA, workerB }, firstPlan.EligibleWorkers.Select(static worker => worker.NodeId).ToArray());
        Assert.Equal(firstPlan.Assignments.Count, secondPlan.Assignments.Count);
        Assert.Equal(8, firstPlan.Assignments.Count);

        var firstSignatures = firstPlan.Assignments.Select(AssignmentSignature).ToArray();
        var secondSignatures = secondPlan.Assignments.Select(AssignmentSignature).ToArray();
        Assert.Equal(firstSignatures, secondSignatures);

        var assignedWorkers = firstPlan.Assignments.Select(AssignmentWorkerId).ToArray();
        Assert.Contains(workerA, assignedWorkers);
        Assert.Contains(workerB, assignedWorkers);
        Assert.Equal(1, firstPlan.Assignments.Count(assignment =>
            assignment.Target == PlacementAssignmentTarget.PlacementTargetRegionShard
            && assignment.RegionId == 1
            && assignment.ShardIndex == 1
            && AssignmentWorkerId(assignment) == workerB));
    }

    private static PlacementPlanner.PlacementPlanningResult? GetPlannedPlacement(HiveMindActor actor, Guid brainId)
    {
        var brainsField = typeof(HiveMindActor).GetField("_brains", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(brainsField);

        var brains = brainsField!.GetValue(actor);
        Assert.NotNull(brains);

        var tryGetValue = brains!.GetType().GetMethod("TryGetValue");
        Assert.NotNull(tryGetValue);

        var args = new object?[] { brainId, null };
        var found = (bool)tryGetValue!.Invoke(brains, args)!;
        Assert.True(found);

        var brainState = args[1];
        Assert.NotNull(brainState);

        var plannedPlacementProperty = brainState!.GetType().GetProperty("PlannedPlacement", BindingFlags.Instance | BindingFlags.Public);
        Assert.NotNull(plannedPlacementProperty);

        return plannedPlacementProperty!.GetValue(brainState) as PlacementPlanner.PlacementPlanningResult;
    }

    private static string AssignmentSignature(PlacementAssignment assignment)
        => $"{assignment.Target}:{AssignmentWorkerId(assignment):N}:{assignment.ActorName}:{assignment.AssignmentId}";

    private static Guid AssignmentWorkerId(PlacementAssignment assignment)
    {
        Assert.NotNull(assignment.WorkerNodeId);
        Assert.True(assignment.WorkerNodeId!.TryToGuid(out var workerId));
        return workerId;
    }

    private static ProtoSettings.WorkerReadinessCapability BuildWorker(
        Guid nodeId,
        bool isAlive,
        bool isReady,
        long lastSeenMs,
        long capabilityTimeMs,
        string address,
        string rootActorName,
        uint cpuCores = 8,
        long ramFreeBytes = 8L * 1024 * 1024 * 1024,
        long storageFreeBytes = 40L * 1024 * 1024 * 1024,
        float cpuScore = 30f)
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
                CpuCores = cpuCores,
                RamFreeBytes = ramFreeBytes > 0 ? (ulong)ramFreeBytes : 0,
                StorageFreeBytes = storageFreeBytes > 0 ? (ulong)storageFreeBytes : 0,
                CpuScore = cpuScore
            }
        };

    private static HiveMindOptions CreateOptions(int workerInventoryStaleAfterMs)
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
            WorkerInventoryRefreshMs: 2_000,
            WorkerInventoryStaleAfterMs: workerInventoryStaleAfterMs);
}
