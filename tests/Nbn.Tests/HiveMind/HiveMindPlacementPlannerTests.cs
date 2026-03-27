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
        var actor = new HiveMindActor(CreateOptions(workerInventoryStaleAfterMs: 5_000));
        var hiveMind = root.Spawn(Props.FromProducer(() => actor));

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

        var inventory = await root.RequestAsync<PlacementWorkerInventory>(
            hiveMind,
            new PlacementWorkerInventoryRequest());
        Assert.Empty(inventory.Workers);

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
        Assert.Null(GetPlannedPlacement(actor, brainId));

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
                RamTotalBytes = 32UL * 1024 * 1024 * 1024,
                StorageFreeBytes = 140UL * 1024 * 1024 * 1024,
                StorageTotalBytes = 180UL * 1024 * 1024 * 1024,
                CpuScore = 80f
            },
            new WorkerResourceAvailability(cpuPercent: 50, ramPercent: 25, storagePercent: 30, gpuComputePercent: 100, gpuVramPercent: 100));
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
    public async Task RequestPlacement_Accepts_When_WorkerRoot_Comes_From_Configured_ServiceEndpoint()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var actor = new HiveMindActor(CreateOptions(workerInventoryStaleAfterMs: 10_000));
        var hiveMind = root.Spawn(Props.FromProducer(() => actor));

        root.Send(hiveMind, new ProtoSettings.SettingValue
        {
            Key = ServiceEndpointSettings.WorkerNodeKey,
            Value = ServiceEndpointSettings.EncodeValue("127.0.0.1:12041", "custom-worker-root")
        });

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
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
                    address: "127.0.0.1:12041",
                    rootActorName: "custom-worker-root",
                    logicalName: "custom-placement-worker")
            }
        });

        var inventory = await root.RequestAsync<PlacementWorkerInventory>(
            hiveMind,
            new PlacementWorkerInventoryRequest());
        var inventoryWorker = Assert.Single(inventory.Workers);
        Assert.Equal(workerId.ToProtoUuid().Value, inventoryWorker.WorkerNodeId.Value);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 2
            });

        Assert.True(ack.Accepted);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, ack.FailureReason);
        Assert.NotNull(GetPlannedPlacement(actor, brainId));

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RequestPlacement_Excludes_ServiceNodes_From_EligibleWorkers()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var actor = new HiveMindActor(CreateOptions(workerInventoryStaleAfterMs: 10_000));
        var hiveMind = root.Spawn(Props.FromProducer(() => actor));

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
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
                    address: "worker-a:12040",
                    rootActorName: "worker-node",
                    logicalName: "nbn.worker"),
                BuildWorker(
                    Guid.NewGuid(),
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: "hivemind:12020",
                    rootActorName: "HiveMind",
                    logicalName: "nbn.hivemind")
            }
        });

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 2
            });

        Assert.True(ack.Accepted);

        var plannedPlacement = GetPlannedPlacement(actor, brainId);
        Assert.NotNull(plannedPlacement);
        var stored = plannedPlacement!;
        Assert.Single(stored.EligibleWorkers);
        Assert.Equal(workerId, stored.EligibleWorkers[0].NodeId);
        Assert.All(
            stored.Assignments,
            assignment =>
            {
                Assert.NotNull(assignment.WorkerNodeId);
                Assert.True(assignment.WorkerNodeId!.TryToGuid(out var assignmentWorkerId));
                Assert.Equal(workerId, assignmentWorkerId);
            });

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
            CreateWorkerCandidate(workerB, "worker-b:12040", isAlive: true, isReady: true, isFresh: true, cpuCores: 8, ramFreeBytes: 8L * 1024 * 1024 * 1024, storageFreeBytes: 40L * 1024 * 1024 * 1024, hasGpu: true, vramFreeBytes: 8L * 1024 * 1024 * 1024, cpuScore: 20f, gpuScore: 70f),
            CreateWorkerCandidate(staleWorker, "worker-c:12040", isAlive: true, isReady: true, isFresh: false, cpuCores: 8, ramFreeBytes: 8L * 1024 * 1024 * 1024, storageFreeBytes: 40L * 1024 * 1024 * 1024, hasGpu: true, vramFreeBytes: 8L * 1024 * 1024 * 1024, cpuScore: 30f, gpuScore: 65f),
            CreateWorkerCandidate(workerA, "worker-a:12040", isAlive: true, isReady: true, isFresh: true, cpuCores: 16, ramFreeBytes: 16L * 1024 * 1024 * 1024, storageFreeBytes: 80L * 1024 * 1024 * 1024, hasGpu: true, vramFreeBytes: 16L * 1024 * 1024 * 1024, cpuScore: 40f, gpuScore: 50f),
            CreateWorkerCandidate(offlineWorker, "worker-d:12040", isAlive: false, isReady: true, isFresh: true, cpuCores: 16, ramFreeBytes: 16L * 1024 * 1024 * 1024, storageFreeBytes: 80L * 1024 * 1024 * 1024, hasGpu: true, vramFreeBytes: 16L * 1024 * 1024 * 1024, cpuScore: 45f, gpuScore: 80f),
            CreateWorkerCandidate(unreadyWorker, "worker-e:12040", isAlive: true, isReady: false, isFresh: true, cpuCores: 16, ramFreeBytes: 16L * 1024 * 1024 * 1024, storageFreeBytes: 80L * 1024 * 1024 * 1024, hasGpu: false, vramFreeBytes: 0, cpuScore: 35f, gpuScore: 0f)
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
            },
            CurrentWorkerNodeIds: Array.Empty<Guid>());

        var builtFirst = PlacementPlanner.TryBuildPlan(
            plannerInputs,
            workers,
            out var firstPlan,
            out var firstFailureReason,
            out _);
        var reversedWorkers = workers.ToArray();
        Array.Reverse(reversedWorkers);
        var builtSecond = PlacementPlanner.TryBuildPlan(
            plannerInputs,
            reversedWorkers,
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
        Assert.DoesNotContain(workerB, assignedWorkers);
        Assert.Equal(2, firstPlan.Assignments.Count(assignment =>
            assignment.Target == PlacementAssignmentTarget.PlacementTargetRegionShard
            && assignment.RegionId == 1
            && AssignmentWorkerId(assignment) == workerA));
    }

    [Fact]
    public void PlacementPlanner_Does_Not_Prefer_GpuWorkers_When_ComputeBackendPreference_Is_Cpu()
    {
        var cpuWorker = Guid.Parse("71000000-0000-0000-0000-000000000001");
        var gpuWorker = Guid.Parse("71000000-0000-0000-0000-000000000002");

        var workers = new[]
        {
            CreateWorkerCandidate(
                gpuWorker,
                "worker-gpu:12040",
                cpuCores: 8,
                cpuScore: 20f,
                hasGpu: true,
                gpuScore: 95f,
                vramFreeBytes: 12L * 1024 * 1024 * 1024),
            CreateWorkerCandidate(
                cpuWorker,
                "worker-cpu:12040",
                cpuCores: 16,
                cpuScore: 80f,
                hasGpu: false,
                gpuScore: 0f,
                vramFreeBytes: 0,
                vramTotalBytes: 0)
        };

        var plannerInputs = new PlacementPlanner.PlannerInputs(
            BrainId: Guid.NewGuid(),
            PlacementEpoch: 7,
            RequestId: "cpu-backend-preference",
            RequestedMs: 100,
            PlannedMs: 101,
            WorkerSnapshotMs: 99,
            ShardStride: 1024,
            RequestedShardPlan: new ShardPlan
            {
                Mode = (ShardPlanMode)1,
                ShardCount = 1
            },
            Regions: new[]
            {
                new PlacementPlanner.RegionSpan(0, 4),
                new PlacementPlanner.RegionSpan(1, 8192),
                new PlacementPlanner.RegionSpan(31, 2)
            },
            CurrentWorkerNodeIds: Array.Empty<Guid>(),
            ComputeBackendPreference: RegionShardComputeBackendPreference.Cpu);

        var built = PlacementPlanner.TryBuildPlan(
            plannerInputs,
            workers,
            out var plan,
            out var failureReason,
            out var failureMessage);

        Assert.True(built, failureMessage);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, failureReason);

        var computeWorkers = plan.Assignments
            .Where(static assignment =>
                assignment.Target == PlacementAssignmentTarget.PlacementTargetRegionShard
                && assignment.RegionId == 1)
            .Select(AssignmentWorkerId)
            .Distinct()
            .ToArray();

        Assert.Equal([cpuWorker], computeWorkers);
    }

    [Theory]
    [InlineData(RegionShardComputeBackendPreference.Auto)]
    [InlineData(RegionShardComputeBackendPreference.Gpu)]
    public void PlacementPlanner_Prefers_Effective_GpuWorkers_When_BackendPreference_Allows_It(
        RegionShardComputeBackendPreference computeBackendPreference)
    {
        var cpuWorker = Guid.Parse("72000000-0000-0000-0000-000000000001");
        var gpuWorker = Guid.Parse("72000000-0000-0000-0000-000000000002");

        var workers = new[]
        {
            CreateWorkerCandidate(
                gpuWorker,
                "worker-gpu:12040",
                cpuCores: 8,
                cpuScore: 20f,
                hasGpu: true,
                gpuScore: 95f,
                vramFreeBytes: 12L * 1024 * 1024 * 1024),
            CreateWorkerCandidate(
                cpuWorker,
                "worker-cpu:12040",
                cpuCores: 16,
                cpuScore: 80f,
                hasGpu: false,
                gpuScore: 0f,
                vramFreeBytes: 0,
                vramTotalBytes: 0)
        };

        var plannerInputs = new PlacementPlanner.PlannerInputs(
            BrainId: Guid.NewGuid(),
            PlacementEpoch: 8,
            RequestId: $"gpu-backend-preference-{computeBackendPreference}",
            RequestedMs: 100,
            PlannedMs: 101,
            WorkerSnapshotMs: 99,
            ShardStride: 1024,
            RequestedShardPlan: new ShardPlan
            {
                Mode = (ShardPlanMode)1,
                ShardCount = 1
            },
            Regions: new[]
            {
                new PlacementPlanner.RegionSpan(0, 4),
                new PlacementPlanner.RegionSpan(1, 8192),
                new PlacementPlanner.RegionSpan(31, 2)
            },
            CurrentWorkerNodeIds: Array.Empty<Guid>(),
            ComputeBackendPreference: computeBackendPreference);

        var built = PlacementPlanner.TryBuildPlan(
            plannerInputs,
            workers,
            out var plan,
            out var failureReason,
            out var failureMessage);

        Assert.True(built, failureMessage);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, failureReason);

        var computeWorkers = plan.Assignments
            .Where(static assignment =>
                assignment.Target == PlacementAssignmentTarget.PlacementTargetRegionShard
                && assignment.RegionId == 1)
            .Select(AssignmentWorkerId)
            .Distinct()
            .ToArray();

        Assert.Equal([gpuWorker], computeWorkers);
    }

    [Fact]
    public void PlacementPlanner_Prefers_LowLatency_CurrentLocality_Over_Freeer_Remote_Worker()
    {
        var workerA = Guid.Parse("10000000-0000-0000-0000-000000000001");
        var workerB = Guid.Parse("10000000-0000-0000-0000-000000000002");
        var workerC = Guid.Parse("10000000-0000-0000-0000-000000000003");
        var brainId = Guid.Parse("20000000-0000-0000-0000-000000000001");

        var workers = new[]
        {
            CreateWorkerCandidate(workerA, "worker-a:12040", averagePeerLatencyMs: 1.5f, peerLatencySampleCount: 2, hostedBrainCount: 2),
            CreateWorkerCandidate(workerB, "worker-b:12040", averagePeerLatencyMs: 1.7f, peerLatencySampleCount: 2, hostedBrainCount: 2),
            CreateWorkerCandidate(workerC, "worker-c:12040", averagePeerLatencyMs: 35f, peerLatencySampleCount: 2, hostedBrainCount: 0)
        };

        var plannerInputs = new PlacementPlanner.PlannerInputs(
            BrainId: brainId,
            PlacementEpoch: 3,
            RequestId: "locality-first",
            RequestedMs: 100,
            PlannedMs: 101,
            WorkerSnapshotMs: 99,
            ShardStride: 1024,
            RequestedShardPlan: new ShardPlan
            {
                Mode = (ShardPlanMode)1,
                ShardCount = 2
            },
            Regions: new[]
            {
                new PlacementPlanner.RegionSpan(0, 4),
                new PlacementPlanner.RegionSpan(1, 16_384),
                new PlacementPlanner.RegionSpan(31, 2)
            },
            CurrentWorkerNodeIds: new[] { workerA, workerB });

        var built = PlacementPlanner.TryBuildPlan(
            plannerInputs,
            workers,
            out var plan,
            out var failureReason,
            out var failureMessage);

        Assert.True(built, failureMessage);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, failureReason);

        var assignedWorkers = plan.Assignments
            .Where(static assignment => assignment.Target == PlacementAssignmentTarget.PlacementTargetRegionShard)
            .Select(AssignmentWorkerId)
            .Distinct()
            .OrderBy(static id => id)
            .ToArray();

        Assert.Equal(new[] { workerA, workerB }, assignedWorkers);
        Assert.DoesNotContain(plan.Assignments, assignment => AssignmentWorkerId(assignment) == workerC);
    }

    [Fact]
    public void PlacementPlanner_Rejects_Pressured_And_VramLimited_GpuWorkers()
    {
        var goodWorker = Guid.Parse("50000000-0000-0000-0000-000000000001");
        var pressuredWorker = Guid.Parse("50000000-0000-0000-0000-000000000002");
        var vramLimitedWorker = Guid.Parse("50000000-0000-0000-0000-000000000003");

        var workers = new[]
        {
            CreateWorkerCandidate(
                goodWorker,
                "worker-good:12040",
                gpuScore: 55f,
                vramFreeBytes: 12L * 1024 * 1024 * 1024,
                vramTotalBytes: 16L * 1024 * 1024 * 1024,
                gpuComputeLimitPercent: 100,
                gpuVramLimitPercent: 100),
            CreateWorkerCandidate(
                pressuredWorker,
                "worker-hot:12040",
                gpuScore: 99f,
                processCpuLoadPercent: 92f,
                cpuLimitPercent: 50,
                gpuComputeLimitPercent: 100,
                gpuVramLimitPercent: 100),
            CreateWorkerCandidate(
                vramLimitedWorker,
                "worker-vram:12040",
                gpuScore: 90f,
                vramFreeBytes: 12L * 1024 * 1024 * 1024,
                vramTotalBytes: 16L * 1024 * 1024 * 1024,
                gpuComputeLimitPercent: 100,
                gpuVramLimitPercent: 0)
        };

        var plannerInputs = new PlacementPlanner.PlannerInputs(
            BrainId: Guid.NewGuid(),
            PlacementEpoch: 5,
            RequestId: "gpu-limits",
            RequestedMs: 100,
            PlannedMs: 101,
            WorkerSnapshotMs: 99,
            ShardStride: 1024,
            RequestedShardPlan: new ShardPlan
            {
                Mode = (ShardPlanMode)1,
                ShardCount = 1
            },
            Regions: new[]
            {
                new PlacementPlanner.RegionSpan(0, 4),
                new PlacementPlanner.RegionSpan(1, 12_288),
                new PlacementPlanner.RegionSpan(31, 2)
            },
            CurrentWorkerNodeIds: Array.Empty<Guid>());

        var built = PlacementPlanner.TryBuildPlan(
            plannerInputs,
            workers,
            out var plan,
            out var failureReason,
            out var failureMessage);

        Assert.True(built, failureMessage);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, failureReason);
        Assert.Equal([goodWorker], plan.EligibleWorkers.Select(static worker => worker.NodeId).ToArray());
        Assert.All(
            plan.Assignments.Where(static assignment => assignment.Target == PlacementAssignmentTarget.PlacementTargetRegionShard),
            assignment => Assert.Equal(goodWorker, AssignmentWorkerId(assignment)));
    }

    [Fact]
    public void PlacementPlanner_Falls_Back_To_CpuCores_When_BenchmarkScore_Is_Zero()
    {
        var workerId = Guid.Parse("60000000-0000-0000-0000-000000000001");
        var workers = new[]
        {
            CreateWorkerCandidate(
                workerId,
                "worker-zero-score:12040",
                cpuCores: 12,
                cpuScore: 0f,
                hasGpu: false,
                gpuScore: 0f)
        };

        var plannerInputs = new PlacementPlanner.PlannerInputs(
            BrainId: Guid.NewGuid(),
            PlacementEpoch: 6,
            RequestId: "cpu-core-fallback",
            RequestedMs: 100,
            PlannedMs: 101,
            WorkerSnapshotMs: 99,
            ShardStride: 1024,
            RequestedShardPlan: new ShardPlan
            {
                Mode = (ShardPlanMode)1,
                ShardCount = 1
            },
            Regions: new[]
            {
                new PlacementPlanner.RegionSpan(0, 4),
                new PlacementPlanner.RegionSpan(1, 8192),
                new PlacementPlanner.RegionSpan(31, 2)
            },
            CurrentWorkerNodeIds: Array.Empty<Guid>());

        var built = PlacementPlanner.TryBuildPlan(
            plannerInputs,
            workers,
            out var plan,
            out var failureReason,
            out var failureMessage);

        Assert.True(built, failureMessage);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, failureReason);
        Assert.Equal([workerId], plan.EligibleWorkers.Select(static worker => worker.NodeId).ToArray());
        Assert.All(
            plan.Assignments.Where(static assignment => assignment.Target == PlacementAssignmentTarget.PlacementTargetRegionShard),
            assignment => Assert.Equal(workerId, AssignmentWorkerId(assignment)));
    }

    [Fact]
    public void PlacementPlanner_Falls_Back_To_HigherLatency_Worker_When_LocalitySubset_Is_Insufficient()
    {
        var workerA = Guid.Parse("30000000-0000-0000-0000-000000000001");
        var workerB = Guid.Parse("30000000-0000-0000-0000-000000000002");
        var workerC = Guid.Parse("30000000-0000-0000-0000-000000000003");
        var brainId = Guid.Parse("40000000-0000-0000-0000-000000000001");

        var workers = new[]
        {
            CreateWorkerCandidate(workerA, "worker-a:12040", averagePeerLatencyMs: 1.4f, peerLatencySampleCount: 2, hostedBrainCount: 2),
            CreateWorkerCandidate(workerB, "worker-b:12040", averagePeerLatencyMs: 1.6f, peerLatencySampleCount: 2, hostedBrainCount: 2),
            CreateWorkerCandidate(workerC, "worker-c:12040", averagePeerLatencyMs: 40f, peerLatencySampleCount: 2, hostedBrainCount: 0)
        };

        var plannerInputs = new PlacementPlanner.PlannerInputs(
            BrainId: brainId,
            PlacementEpoch: 4,
            RequestId: "locality-fallback",
            RequestedMs: 100,
            PlannedMs: 101,
            WorkerSnapshotMs: 99,
            ShardStride: 1024,
            RequestedShardPlan: new ShardPlan
            {
                Mode = (ShardPlanMode)1,
                ShardCount = 3
            },
            Regions: new[]
            {
                new PlacementPlanner.RegionSpan(0, 4),
                new PlacementPlanner.RegionSpan(1, 24_576),
                new PlacementPlanner.RegionSpan(31, 2)
            },
            CurrentWorkerNodeIds: new[] { workerA, workerB });

        var built = PlacementPlanner.TryBuildPlan(
            plannerInputs,
            workers,
            out var plan,
            out var failureReason,
            out var failureMessage);

        Assert.True(built, failureMessage);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, failureReason);

        var computeWorkers = plan.Assignments
            .Where(static assignment =>
                assignment.Target == PlacementAssignmentTarget.PlacementTargetRegionShard
                && assignment.RegionId == 1)
            .Select(AssignmentWorkerId)
            .Distinct()
            .OrderBy(static id => id)
            .ToArray();

        Assert.Equal(new[] { workerA, workerB, workerC }, computeWorkers);
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

    private static PlacementPlanner.WorkerCandidate CreateWorkerCandidate(
        Guid nodeId,
        string workerAddress,
        bool isAlive = true,
        bool isReady = true,
        bool isFresh = true,
        uint cpuCores = 8,
        long ramFreeBytes = 8L * 1024 * 1024 * 1024,
        long ramTotalBytes = 16L * 1024 * 1024 * 1024,
        long storageFreeBytes = 40L * 1024 * 1024 * 1024,
        long storageTotalBytes = 80L * 1024 * 1024 * 1024,
        bool hasGpu = true,
        long vramFreeBytes = 8L * 1024 * 1024 * 1024,
        long vramTotalBytes = 16L * 1024 * 1024 * 1024,
        float cpuScore = 30f,
        float gpuScore = 60f,
        uint cpuLimitPercent = 100,
        uint ramLimitPercent = 100,
        uint storageLimitPercent = 100,
        uint gpuComputeLimitPercent = 100,
        uint gpuVramLimitPercent = 100,
        float processCpuLoadPercent = 0f,
        long processRamUsedBytes = 0,
        float pressureLimitTolerancePercent = (float)WorkerCapabilitySettingsKeys.DefaultPressureLimitTolerancePercent,
        float averagePeerLatencyMs = 0f,
        uint peerLatencySampleCount = 0,
        int hostedBrainCount = 0)
        => new(
            nodeId,
            workerAddress,
            "region-host",
            isAlive,
            isReady,
            isFresh,
            cpuCores,
            ramFreeBytes,
            ramTotalBytes,
            storageFreeBytes,
            storageTotalBytes,
            hasGpu,
            vramFreeBytes,
            vramTotalBytes,
            cpuScore,
            gpuScore,
            cpuLimitPercent,
            ramLimitPercent,
            storageLimitPercent,
            gpuComputeLimitPercent,
            gpuVramLimitPercent,
            processCpuLoadPercent,
            processRamUsedBytes,
            pressureLimitTolerancePercent,
            averagePeerLatencyMs,
            peerLatencySampleCount,
            hostedBrainCount);

    private static ProtoSettings.WorkerReadinessCapability BuildWorker(
        Guid nodeId,
        bool isAlive,
        bool isReady,
        long lastSeenMs,
        long capabilityTimeMs,
        string address,
        string rootActorName,
        string logicalName = "",
        uint cpuCores = 8,
        long ramFreeBytes = 8L * 1024 * 1024 * 1024,
        long ramTotalBytes = 16L * 1024 * 1024 * 1024,
        long storageFreeBytes = 40L * 1024 * 1024 * 1024,
        long storageTotalBytes = 80L * 1024 * 1024 * 1024,
        float cpuScore = 30f,
        uint cpuLimitPercent = 100,
        uint ramLimitPercent = 100,
        uint storageLimitPercent = 100)
        => new()
        {
            NodeId = nodeId.ToProtoUuid(),
            LogicalName = logicalName,
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
                RamTotalBytes = ramTotalBytes > 0 ? (ulong)ramTotalBytes : 0,
                StorageFreeBytes = storageFreeBytes > 0 ? (ulong)storageFreeBytes : 0,
                StorageTotalBytes = storageTotalBytes > 0 ? (ulong)storageTotalBytes : 0,
                CpuScore = cpuScore,
                CpuLimitPercent = cpuLimitPercent,
                RamLimitPercent = ramLimitPercent,
                StorageLimitPercent = storageLimitPercent
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
