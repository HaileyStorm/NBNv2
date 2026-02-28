using Microsoft.Data.Sqlite;
using Nbn.Proto.Control;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.IO;
using Nbn.Runtime.RegionHost;
using Nbn.Runtime.SettingsMonitor;
using Nbn.Runtime.WorkerNode;
using Nbn.Shared;
using Nbn.Tests.Format;
using Nbn.Tests.TestSupport;
using Proto;
using ProtoControl = Nbn.Proto.Control;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Tests.WorkerNode;

public sealed class WorkerNodeDiscoveryAndPlacementTests
{
    [Fact]
    public async Task DiscoveryBootstrap_And_LiveUpdate_AppliesKnownEndpoints()
    {
        using var metrics = new MeterCollector(WorkerNodeTelemetry.MeterNameValue);
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerAddress = "127.0.0.1:12041";
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, workerAddress)));

        await using var discovery = new ServiceEndpointDiscoveryClient(harness.System, harness.SettingsPid);

        var initialHiveMind = new ServiceEndpoint("127.0.0.1:12020", "HiveMind");
        var initialIo = new ServiceEndpoint("127.0.0.1:12050", "io-gateway");
        await discovery.PublishAsync(ServiceEndpointSettings.HiveMindKey, initialHiveMind);
        await discovery.PublishAsync(ServiceEndpointSettings.IoGatewayKey, initialIo);

        var known = await discovery.ResolveKnownAsync();
        harness.Root.Send(workerPid, new WorkerNodeActor.DiscoverySnapshotApplied(known));

        var initialState = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());
        Assert.NotNull(initialState.HiveMindEndpoint);
        Assert.NotNull(initialState.IoGatewayEndpoint);
        Assert.Equal(initialHiveMind, initialState.HiveMindEndpoint!.Value.Endpoint);
        Assert.Equal(initialIo, initialState.IoGatewayEndpoint!.Value.Endpoint);
        var workerTag = workerNodeId.ToString("D");
        Assert.Equal(
            2,
            metrics.SumLong(
                "nbn.workernode.discovery.endpoint.observed",
                ("worker_node_id", workerTag),
                ("failure_reason", "none"),
                ("outcome", "snapshot_registered")));

        var updatedIo = new ServiceEndpoint("127.0.0.1:12051", "io-gateway");
        var updateSeen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        discovery.EndpointChanged += registration =>
        {
            harness.Root.Send(workerPid, new WorkerNodeActor.EndpointRegistrationObserved(registration));
            if (registration.Key == ServiceEndpointSettings.IoGatewayKey && registration.Endpoint == updatedIo)
            {
                updateSeen.TrySetResult();
            }
        };

        await discovery.SubscribeAsync([ServiceEndpointSettings.HiveMindKey, ServiceEndpointSettings.IoGatewayKey]);
        await Task.Delay(50);
        await discovery.PublishAsync(ServiceEndpointSettings.IoGatewayKey, updatedIo);
        await updateSeen.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await WaitForAsync(
            async () =>
            {
                var state = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
                    workerPid,
                    new WorkerNodeActor.GetWorkerNodeSnapshot());
                return state.IoGatewayEndpoint.HasValue
                       && state.IoGatewayEndpoint.Value.Endpoint == updatedIo;
            },
            timeoutMs: 5_000);

        Assert.Equal(
            1,
            metrics.SumLong(
                "nbn.workernode.discovery.endpoint.observed",
                ("worker_node_id", workerTag),
                ("target", ServiceEndpointSettings.IoGatewayKey),
                ("failure_reason", "none"),
                ("outcome", "update_updated")));
    }

    [Fact]
    public async Task DiscoverySnapshot_MissingEndpoint_ClearsStaleRegistration()
    {
        using var metrics = new MeterCollector(WorkerNodeTelemetry.MeterNameValue);
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        await using var discovery = new ServiceEndpointDiscoveryClient(harness.System, harness.SettingsPid);
        await discovery.PublishAsync(
            ServiceEndpointSettings.HiveMindKey,
            new ServiceEndpoint("127.0.0.1:12020", "HiveMind"));
        await discovery.PublishAsync(
            ServiceEndpointSettings.IoGatewayKey,
            new ServiceEndpoint("127.0.0.1:12050", "io-gateway"));

        var known = await discovery.ResolveKnownAsync();
        harness.Root.Send(workerPid, new WorkerNodeActor.DiscoverySnapshotApplied(known));

        var hiveOnly = new Dictionary<string, ServiceEndpointRegistration>(StringComparer.Ordinal)
        {
            [ServiceEndpointSettings.HiveMindKey] = known[ServiceEndpointSettings.HiveMindKey]
        };
        harness.Root.Send(workerPid, new WorkerNodeActor.DiscoverySnapshotApplied(hiveOnly));

        await WaitForAsync(
            async () =>
            {
                var state = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
                    workerPid,
                    new WorkerNodeActor.GetWorkerNodeSnapshot());
                return state.HiveMindEndpoint.HasValue && state.IoGatewayEndpoint is null;
            },
            timeoutMs: 5_000);

        var workerTag = workerNodeId.ToString("D");
        Assert.Equal(
            1,
            metrics.SumLong(
                "nbn.workernode.discovery.endpoint.observed",
                ("worker_node_id", workerTag),
                ("target", ServiceEndpointSettings.IoGatewayKey),
                ("failure_reason", "endpoint_missing"),
                ("outcome", "snapshot_missing")));
    }

    [Fact]
    public async Task DiscoveryUpdate_RemovedHiveMind_ClearsEndpoint_And_ResolveFallsBackToHint()
    {
        using var metrics = new MeterCollector(WorkerNodeTelemetry.MeterNameValue);
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        await using var discovery = new ServiceEndpointDiscoveryClient(harness.System, harness.SettingsPid);
        await discovery.PublishAsync(
            ServiceEndpointSettings.HiveMindKey,
            new ServiceEndpoint("127.0.0.1:12020", "HiveMind"));
        await discovery.PublishAsync(
            ServiceEndpointSettings.IoGatewayKey,
            new ServiceEndpoint("127.0.0.1:12050", "io-gateway"));

        var known = await discovery.ResolveKnownAsync();
        harness.Root.Send(workerPid, new WorkerNodeActor.DiscoverySnapshotApplied(known));

        var seedAck = await SendPlacementRequestViaNamedSenderAsync(
            harness.System,
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = BuildAssignment(
                    assignmentId: "seed-hint",
                    brainId: Guid.NewGuid(),
                    workerNodeId: workerNodeId,
                    placementEpoch: 1,
                    regionId: 0,
                    shardIndex: 0,
                    target: PlacementAssignmentTarget.PlacementTargetBrainRoot,
                    actorName: $"brain-{Guid.NewGuid():N}-root")
            });
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, seedAck.State);

        harness.Root.Send(workerPid, new WorkerNodeActor.EndpointStateObserved(
            new ServiceEndpointObservation(
                ServiceEndpointSettings.HiveMindKey,
                ServiceEndpointObservationKind.Removed,
                null,
                "endpoint_removed",
                (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())));

        await WaitForAsync(
            async () =>
            {
                var state = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
                    workerPid,
                    new WorkerNodeActor.GetWorkerNodeSnapshot());
                return state.HiveMindEndpoint is null;
            },
            timeoutMs: 5_000);

        var fallbackAck = await harness.Root.RequestAsync<PlacementAssignmentAck>(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = BuildAssignment(
                    assignmentId: "after-removal",
                    brainId: Guid.NewGuid(),
                    workerNodeId: workerNodeId,
                    placementEpoch: 2,
                    regionId: 0,
                    shardIndex: 0,
                    target: PlacementAssignmentTarget.PlacementTargetBrainRoot,
                    actorName: $"brain-{Guid.NewGuid():N}-root")
            });
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, fallbackAck.State);

        var workerTag = workerNodeId.ToString("D");
        Assert.Equal(
            1,
            metrics.SumLong(
                "nbn.workernode.discovery.endpoint.observed",
                ("worker_node_id", workerTag),
                ("target", ServiceEndpointSettings.HiveMindKey),
                ("failure_reason", "endpoint_removed"),
                ("outcome", "update_removed")));

        Assert.True(
            metrics.SumLong(
                "nbn.workernode.discovery.endpoint.resolve",
                ("worker_node_id", workerTag),
                ("target", ServiceEndpointSettings.HiveMindKey),
                ("failure_reason", "endpoint_missing"),
                ("outcome", "resolved_hint")) >= 1);
    }

    [Fact]
    public async Task TickCompletionForwarding_UsesHiveMindHint_WhenEndpointMissing()
    {
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        var brainId = Guid.NewGuid();
        var seedRequest = new PlacementAssignmentRequest
        {
            Assignment = BuildAssignment(
                assignmentId: "seed-hint",
                brainId: brainId,
                workerNodeId: workerNodeId,
                placementEpoch: 1,
                regionId: 0,
                shardIndex: 0,
                target: PlacementAssignmentTarget.PlacementTargetBrainRoot,
                actorName: $"brain-{brainId:N}-root")
        };

        var ackTcs = new TaskCompletionSource<PlacementAssignmentAck>(TaskCreationOptions.RunContinuationsAsynchronously);
        var completionTcs = new TaskCompletionSource<TickComputeDone>(TaskCreationOptions.RunContinuationsAsynchronously);
        var hintSenderName = $"worker-test-hivemind-hint-{Guid.NewGuid():N}";
        var hintSenderPid = harness.Root.SpawnNamed(
            Props.FromProducer(() => new HiveMindForwardProbeActor(workerPid, seedRequest, ackTcs, completionTcs)),
            hintSenderName);

        try
        {
            var seedAck = await ackTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, seedAck.State);

            var shardId = Nbn.Shared.Addressing.ShardId32.From(1, 0);
            harness.Root.Send(workerPid, new TickComputeDone
            {
                TickId = 42,
                BrainId = brainId.ToProtoUuid(),
                RegionId = (uint)shardId.RegionId,
                ShardId = shardId.ToProtoShardId32(),
                ComputeMs = 1
            });

            var forwarded = await completionTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal((ulong)42, forwarded.TickId);
            Assert.True(forwarded.BrainId.TryToGuid(out var forwardedBrainId));
            Assert.Equal(brainId, forwardedBrainId);
        }
        finally
        {
            harness.Root.Stop(hintSenderPid);
        }
    }

    [Fact]
    public async Task DiscoveryUpdate_InvalidHiveMind_InvalidateEndpoint_And_EmitsTelemetry()
    {
        using var metrics = new MeterCollector(WorkerNodeTelemetry.MeterNameValue);
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        await using var discovery = new ServiceEndpointDiscoveryClient(harness.System, harness.SettingsPid);
        await discovery.PublishAsync(
            ServiceEndpointSettings.HiveMindKey,
            new ServiceEndpoint("127.0.0.1:12020", "HiveMind"));
        await discovery.PublishAsync(
            ServiceEndpointSettings.IoGatewayKey,
            new ServiceEndpoint("127.0.0.1:12050", "io-gateway"));

        var known = await discovery.ResolveKnownAsync();
        harness.Root.Send(workerPid, new WorkerNodeActor.DiscoverySnapshotApplied(known));

        harness.Root.Send(workerPid, new WorkerNodeActor.EndpointStateObserved(
            new ServiceEndpointObservation(
                ServiceEndpointSettings.HiveMindKey,
                ServiceEndpointObservationKind.Invalid,
                null,
                "endpoint_parse_failed",
                (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())));

        await WaitForAsync(
            async () =>
            {
                var state = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
                    workerPid,
                    new WorkerNodeActor.GetWorkerNodeSnapshot());
                return state.HiveMindEndpoint is null;
            },
            timeoutMs: 5_000);

        var workerTag = workerNodeId.ToString("D");
        Assert.Equal(
            1,
            metrics.SumLong(
                "nbn.workernode.discovery.endpoint.observed",
                ("worker_node_id", workerTag),
                ("target", ServiceEndpointSettings.HiveMindKey),
                ("failure_reason", "endpoint_parse_failed"),
                ("outcome", "update_invalidated")));
    }

    [Fact]
    public async Task PlacementAssignmentRequest_TargetingWorker_ReturnsReadyAck()
    {
        using var metrics = new MeterCollector(WorkerNodeTelemetry.MeterNameValue);
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        var brainId = Guid.NewGuid();
        var request = new PlacementAssignmentRequest
        {
            Assignment = BuildAssignment(
                assignmentId: "assign-1",
                brainId: brainId,
                workerNodeId: workerNodeId,
                placementEpoch: 7,
                regionId: 1,
                shardIndex: 2)
        };

        var ack = await harness.Root.RequestAsync<PlacementAssignmentAck>(workerPid, request);

        Assert.Equal("assign-1", ack.AssignmentId);
        Assert.Equal(brainId.ToProtoUuid().Value, ack.BrainId.Value);
        Assert.Equal<ulong>(7, ack.PlacementEpoch);
        Assert.True(ack.Accepted);
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, ack.State);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, ack.FailureReason);

        var state = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());
        Assert.Equal(1, state.TrackedAssignmentCount);

        var brainTag = brainId.ToString("D");
        Assert.Equal(1, metrics.SumLong("nbn.workernode.placement.assignment.hosted.accepted", "brain_id", brainTag));
        Assert.Equal(0, metrics.SumLong("nbn.workernode.placement.assignment.hosted.failed", "brain_id", brainTag));
        Assert.True(metrics.CountDouble("nbn.workernode.placement.assignment.hosting.ms", "brain_id", brainTag) >= 1);
    }

    [Fact]
    public async Task PlacementAssignmentRequest_TargetingOtherWorker_ReturnsFailedAck_And_EmitsFailedTelemetry()
    {
        using var metrics = new MeterCollector(WorkerNodeTelemetry.MeterNameValue);
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        var brainId = Guid.NewGuid();
        var request = new PlacementAssignmentRequest
        {
            Assignment = BuildAssignment(
                assignmentId: "assign-other-worker",
                brainId: brainId,
                workerNodeId: Guid.NewGuid(),
                placementEpoch: 7,
                regionId: 1,
                shardIndex: 2)
        };

        var ack = await harness.Root.RequestAsync<PlacementAssignmentAck>(workerPid, request);
        Assert.Equal("assign-other-worker", ack.AssignmentId);
        Assert.False(ack.Accepted);
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentFailed, ack.State);
        Assert.Equal(PlacementFailureReason.PlacementFailureWorkerUnavailable, ack.FailureReason);

        var state = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());
        Assert.Equal(0, state.TrackedAssignmentCount);

        var brainTag = brainId.ToString("D");
        Assert.Equal(1, metrics.SumLong("nbn.workernode.placement.assignment.hosted.failed", "brain_id", brainTag));
        Assert.Equal(0, metrics.SumLong("nbn.workernode.placement.assignment.hosted.accepted", "brain_id", brainTag));
    }

    [Fact]
    public async Task PlacementUnassignmentRequest_TargetingOtherWorker_IsRejected_And_PreservesAssignment()
    {
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        var brainId = Guid.NewGuid();
        var assignment = BuildAssignment(
            assignmentId: "assign-foreign-unassign",
            brainId: brainId,
            workerNodeId: workerNodeId,
            placementEpoch: 7,
            regionId: 1,
            shardIndex: 2);
        var assignAck = await harness.Root.RequestAsync<PlacementAssignmentAck>(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = assignment
            });
        Assert.True(assignAck.Accepted);

        var unassignAck = await harness.Root.RequestAsync<PlacementUnassignmentAck>(
            workerPid,
            new PlacementUnassignmentRequest
            {
                Assignment = BuildAssignment(
                    assignmentId: "assign-foreign-unassign",
                    brainId: brainId,
                    workerNodeId: Guid.NewGuid(),
                    placementEpoch: 7,
                    regionId: 1,
                    shardIndex: 2)
            });
        Assert.False(unassignAck.Accepted);
        Assert.Equal(PlacementFailureReason.PlacementFailureWorkerUnavailable, unassignAck.FailureReason);

        var state = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());
        Assert.Equal(1, state.TrackedAssignmentCount);

        var reconcile = await harness.Root.RequestAsync<PlacementReconcileReport>(
            workerPid,
            new PlacementReconcileRequest
            {
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = 7
            });
        var observed = Assert.Single(reconcile.Assignments);
        Assert.Equal("assign-foreign-unassign", observed.AssignmentId);
    }

    [Fact]
    public async Task PlacementAssignmentRequest_DuplicateReadyAssignment_IsIdempotent()
    {
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        var brainId = Guid.NewGuid();
        var request = new PlacementAssignmentRequest
        {
            Assignment = BuildAssignment(
                assignmentId: "assign-dup",
                brainId: brainId,
                workerNodeId: workerNodeId,
                placementEpoch: 5,
                regionId: 2,
                shardIndex: 0)
        };

        var firstAck = await harness.Root.RequestAsync<PlacementAssignmentAck>(workerPid, request);
        var secondAck = await harness.Root.RequestAsync<PlacementAssignmentAck>(workerPid, request);
        Assert.True(firstAck.Accepted);
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, firstAck.State);
        Assert.True(secondAck.Accepted);
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, secondAck.State);
        Assert.Equal("already_ready", secondAck.Message);

        var state = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());
        Assert.Equal(1, state.TrackedAssignmentCount);

        var reconcile = await harness.Root.RequestAsync<PlacementReconcileReport>(
            workerPid,
            new PlacementReconcileRequest
            {
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = 5
            });
        var observed = Assert.Single(reconcile.Assignments);
        Assert.Equal("assign-dup", observed.AssignmentId);
    }

    [Fact]
    public async Task PlacementUnassignmentRequest_RemovesAssignment_And_ReconcileNoLongerReportsIt()
    {
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        var brainId = Guid.NewGuid();
        var assignment = BuildAssignment(
            assignmentId: "assign-unassign",
            brainId: brainId,
            workerNodeId: workerNodeId,
            placementEpoch: 5,
            regionId: 2,
            shardIndex: 0);
        var assignAck = await harness.Root.RequestAsync<PlacementAssignmentAck>(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = assignment
            });
        Assert.True(assignAck.Accepted);

        var unassignAck = await harness.Root.RequestAsync<PlacementUnassignmentAck>(
            workerPid,
            new PlacementUnassignmentRequest
            {
                Assignment = assignment.Clone()
            });
        Assert.True(unassignAck.Accepted);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, unassignAck.FailureReason);
        Assert.Equal("unassigned", unassignAck.Message);

        var state = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());
        Assert.Equal(0, state.TrackedAssignmentCount);

        var reconcile = await harness.Root.RequestAsync<PlacementReconcileReport>(
            workerPid,
            new PlacementReconcileRequest
            {
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = 5
            });
        Assert.Empty(reconcile.Assignments);
    }

    [Fact]
    public async Task PlacementUnassignmentRequest_Duplicate_IsIdempotent()
    {
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        var brainId = Guid.NewGuid();
        var assignment = BuildAssignment(
            assignmentId: "assign-unassign-idempotent",
            brainId: brainId,
            workerNodeId: workerNodeId,
            placementEpoch: 5,
            regionId: 2,
            shardIndex: 0);
        var assignAck = await harness.Root.RequestAsync<PlacementAssignmentAck>(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = assignment
            });
        Assert.True(assignAck.Accepted);

        var firstAck = await harness.Root.RequestAsync<PlacementUnassignmentAck>(
            workerPid,
            new PlacementUnassignmentRequest
            {
                Assignment = assignment.Clone()
            });
        var secondAck = await harness.Root.RequestAsync<PlacementUnassignmentAck>(
            workerPid,
            new PlacementUnassignmentRequest
            {
                Assignment = assignment.Clone()
            });

        Assert.True(firstAck.Accepted);
        Assert.Equal("unassigned", firstAck.Message);
        Assert.True(secondAck.Accepted);
        Assert.Equal("already_unassigned", secondAck.Message);

        var state = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());
        Assert.Equal(0, state.TrackedAssignmentCount);
    }

    [Fact]
    public async Task PlacementAssignmentRequest_StaleEpoch_ReturnsFailedAck_And_PreservesTrackedAssignments()
    {
        using var metrics = new MeterCollector(WorkerNodeTelemetry.MeterNameValue);
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        var brainId = Guid.NewGuid();
        var active = new PlacementAssignmentRequest
        {
            Assignment = BuildAssignment(
                assignmentId: "assign-active",
                brainId: brainId,
                workerNodeId: workerNodeId,
                placementEpoch: 8,
                regionId: 2,
                shardIndex: 0)
        };

        var stale = new PlacementAssignmentRequest
        {
            Assignment = BuildAssignment(
                assignmentId: "assign-stale",
                brainId: brainId,
                workerNodeId: workerNodeId,
                placementEpoch: 7,
                regionId: 3,
                shardIndex: 0)
        };

        var firstAck = await harness.Root.RequestAsync<PlacementAssignmentAck>(workerPid, active);
        var staleAck = await harness.Root.RequestAsync<PlacementAssignmentAck>(workerPid, stale);
        Assert.True(firstAck.Accepted);
        Assert.False(staleAck.Accepted);
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentFailed, staleAck.State);
        Assert.Equal(PlacementFailureReason.PlacementFailureInternalError, staleAck.FailureReason);
        Assert.Contains("older than hosted epoch", staleAck.Message, StringComparison.OrdinalIgnoreCase);

        var state = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());
        Assert.Equal(1, state.TrackedAssignmentCount);

        var reconcile = await harness.Root.RequestAsync<PlacementReconcileReport>(
            workerPid,
            new PlacementReconcileRequest
            {
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = 8
            });
        var observed = Assert.Single(reconcile.Assignments);
        Assert.Equal("assign-active", observed.AssignmentId);

        var brainTag = brainId.ToString("D");
        Assert.Equal(1, metrics.SumLong("nbn.workernode.placement.assignment.hosted.failed", "brain_id", brainTag));
    }

    [Fact]
    public async Task PlacementAssignmentRequest_NewerEpoch_ResetsPriorBrainAssignments()
    {
        using var metrics = new MeterCollector(WorkerNodeTelemetry.MeterNameValue);
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        var brainId = Guid.NewGuid();
        await harness.Root.RequestAsync<PlacementAssignmentAck>(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = BuildAssignment(
                    assignmentId: "assign-old-a",
                    brainId: brainId,
                    workerNodeId: workerNodeId,
                    placementEpoch: 3,
                    regionId: 4,
                    shardIndex: 0)
            });
        await harness.Root.RequestAsync<PlacementAssignmentAck>(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = BuildAssignment(
                    assignmentId: "assign-old-b",
                    brainId: brainId,
                    workerNodeId: workerNodeId,
                    placementEpoch: 3,
                    regionId: 5,
                    shardIndex: 1)
            });

        var promotedAck = await harness.Root.RequestAsync<PlacementAssignmentAck>(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = BuildAssignment(
                    assignmentId: "assign-new",
                    brainId: brainId,
                    workerNodeId: workerNodeId,
                    placementEpoch: 4,
                    regionId: 6,
                    shardIndex: 0)
            });
        Assert.True(promotedAck.Accepted);
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, promotedAck.State);

        var state = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());
        Assert.Equal(1, state.TrackedAssignmentCount);

        var oldEpochReport = await harness.Root.RequestAsync<PlacementReconcileReport>(
            workerPid,
            new PlacementReconcileRequest
            {
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = 3
            });
        Assert.Empty(oldEpochReport.Assignments);

        var newEpochReport = await harness.Root.RequestAsync<PlacementReconcileReport>(
            workerPid,
            new PlacementReconcileRequest
            {
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = 4
            });
        var observed = Assert.Single(newEpochReport.Assignments);
        Assert.Equal("assign-new", observed.AssignmentId);

        var workerTag = workerNodeId.ToString("D");
        var brainTag = brainId.ToString("D");
        Assert.Equal(
            1,
            metrics.SumLong(
                "nbn.workernode.placement.reconcile.reported",
                ("worker_node_id", workerTag),
                ("brain_id", brainTag),
                ("placement_epoch", "3"),
                ("target", "reconcile"),
                ("failure_reason", "placement_epoch_mismatch"),
                ("outcome", "empty"),
                ("assignment_count", "0")));
        Assert.Equal(
            1,
            metrics.SumLong(
                "nbn.workernode.placement.reconcile.reported",
                ("worker_node_id", workerTag),
                ("brain_id", brainTag),
                ("placement_epoch", "4"),
                ("target", "reconcile"),
                ("failure_reason", "none"),
                ("outcome", "matched"),
                ("assignment_count", "1")));
    }

    [Fact]
    public async Task PlacementAssignmentRequest_AssignmentIdConflict_IsRejected_And_PreservesOriginalAssignment()
    {
        using var metrics = new MeterCollector(WorkerNodeTelemetry.MeterNameValue);
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        var brainId = Guid.NewGuid();
        var accepted = await harness.Root.RequestAsync<PlacementAssignmentAck>(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = BuildAssignment(
                    assignmentId: "assign-conflict",
                    brainId: brainId,
                    workerNodeId: workerNodeId,
                    placementEpoch: 6,
                    regionId: 7,
                    shardIndex: 0,
                    target: PlacementAssignmentTarget.PlacementTargetRegionShard)
            });
        Assert.True(accepted.Accepted);

        var conflictAck = await harness.Root.RequestAsync<PlacementAssignmentAck>(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = BuildAssignment(
                    assignmentId: "assign-conflict",
                    brainId: brainId,
                    workerNodeId: workerNodeId,
                    placementEpoch: 6,
                    regionId: 0,
                    shardIndex: 0,
                    target: PlacementAssignmentTarget.PlacementTargetInputCoordinator)
            });
        Assert.False(conflictAck.Accepted);
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentFailed, conflictAck.State);
        Assert.Equal(PlacementFailureReason.PlacementFailureAssignmentRejected, conflictAck.FailureReason);
        Assert.Contains("conflicts with an existing ready assignment", conflictAck.Message, StringComparison.OrdinalIgnoreCase);

        var state = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());
        Assert.Equal(1, state.TrackedAssignmentCount);

        var reconcile = await harness.Root.RequestAsync<PlacementReconcileReport>(
            workerPid,
            new PlacementReconcileRequest
            {
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = 6
            });
        var observed = Assert.Single(reconcile.Assignments);
        Assert.Equal(PlacementAssignmentTarget.PlacementTargetRegionShard, observed.Target);
        Assert.Equal((uint)7, observed.RegionId);

        var brainTag = brainId.ToString("D");
        Assert.Equal(1, metrics.SumLong("nbn.workernode.placement.assignment.hosted.failed", "brain_id", brainTag));
    }

    [Fact]
    public async Task PlacementReconcileRequest_ReturnsTrackedAssignments()
    {
        using var metrics = new MeterCollector(WorkerNodeTelemetry.MeterNameValue);
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        var brainId = Guid.NewGuid();
        var first = new PlacementAssignmentRequest
        {
            Assignment = BuildAssignment(
                assignmentId: "assign-a",
                brainId: brainId,
                workerNodeId: workerNodeId,
                placementEpoch: 3,
                regionId: 4,
                shardIndex: 0)
        };
        var second = new PlacementAssignmentRequest
        {
            Assignment = BuildAssignment(
                assignmentId: "assign-b",
                brainId: brainId,
                workerNodeId: workerNodeId,
                placementEpoch: 3,
                regionId: 5,
                shardIndex: 1)
        };

        await harness.Root.RequestAsync<PlacementAssignmentAck>(workerPid, first);
        await harness.Root.RequestAsync<PlacementAssignmentAck>(workerPid, second);

        var report = await harness.Root.RequestAsync<PlacementReconcileReport>(
            workerPid,
            new PlacementReconcileRequest
            {
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = 3
            });

        Assert.Equal(brainId.ToProtoUuid().Value, report.BrainId.Value);
        Assert.Equal<ulong>(3, report.PlacementEpoch);
        Assert.Equal(PlacementReconcileState.PlacementReconcileMatched, report.ReconcileState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, report.FailureReason);
        Assert.Equal(2, report.Assignments.Count);
        Assert.Equal(new[] { "assign-a", "assign-b" }, report.Assignments.Select(static entry => entry.AssignmentId).ToArray());
        Assert.All(report.Assignments, assignment => Assert.Equal(workerNodeId.ToProtoUuid().Value, assignment.WorkerNodeId.Value));

        var workerTag = workerNodeId.ToString("D");
        var brainTag = brainId.ToString("D");
        Assert.Equal(
            1,
            metrics.SumLong(
                "nbn.workernode.placement.reconcile.requested",
                ("worker_node_id", workerTag),
                ("brain_id", brainTag),
                ("placement_epoch", "3"),
                ("target", "reconcile"),
                ("failure_reason", "none")));
        Assert.Equal(
            1,
            metrics.SumLong(
                "nbn.workernode.placement.reconcile.reported",
                ("worker_node_id", workerTag),
                ("brain_id", brainTag),
                ("placement_epoch", "3"),
                ("target", "reconcile"),
                ("failure_reason", "none"),
                ("outcome", "matched"),
                ("assignment_count", "2")));
    }

    [Fact]
    public async Task PlacementAssignments_BrainRoot_And_Router_AreHosted_And_Wired()
    {
        using var metrics = new MeterCollector(WorkerNodeTelemetry.MeterNameValue);
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        var brainId = Guid.NewGuid();
        var rootName = $"brain-{brainId:N}-root-test";
        var routerName = $"brain-{brainId:N}-router-test";

        var rootAck = await harness.Root.RequestAsync<PlacementAssignmentAck>(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = BuildAssignment(
                    assignmentId: "assign-root",
                    brainId: brainId,
                    workerNodeId: workerNodeId,
                    placementEpoch: 9,
                    regionId: 0,
                    shardIndex: 0,
                    target: PlacementAssignmentTarget.PlacementTargetBrainRoot,
                    actorName: rootName,
                    neuronStart: 0,
                    neuronCount: 0)
            });
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, rootAck.State);

        var routerAck = await harness.Root.RequestAsync<PlacementAssignmentAck>(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = BuildAssignment(
                    assignmentId: "assign-router",
                    brainId: brainId,
                    workerNodeId: workerNodeId,
                    placementEpoch: 9,
                    regionId: 0,
                    shardIndex: 0,
                    target: PlacementAssignmentTarget.PlacementTargetSignalRouter,
                    actorName: routerName,
                    neuronStart: 0,
                    neuronCount: 0)
            });
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, routerAck.State);

        var reconcile = await harness.Root.RequestAsync<PlacementReconcileReport>(
            workerPid,
            new PlacementReconcileRequest
            {
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = 9
            });

        Assert.Equal(2, reconcile.Assignments.Count);
        var rootObserved = Assert.Single(reconcile.Assignments, static entry => entry.AssignmentId == "assign-root");
        var routerObserved = Assert.Single(reconcile.Assignments, static entry => entry.AssignmentId == "assign-router");
        Assert.EndsWith($"/{rootName}", rootObserved.ActorPid, StringComparison.Ordinal);
        Assert.EndsWith($"/{routerName}", routerObserved.ActorPid, StringComparison.Ordinal);

        var workerTag = workerNodeId.ToString("D");
        Assert.True(
            metrics.SumLong(
                "nbn.workernode.discovery.endpoint.resolve",
                ("worker_node_id", workerTag),
                ("target", ServiceEndpointSettings.HiveMindKey),
                ("failure_reason", "endpoint_missing")) >= 1);
    }

    [Fact]
    public async Task PlacementAssignments_RegionShard_And_Coordinators_Host_LiveActors()
    {
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        var brainId = Guid.NewGuid();
        var inputName = $"brain-{brainId:N}-input-test";
        var outputName = $"brain-{brainId:N}-output-test";
        var shardName = $"brain-{brainId:N}-r5-s0-test";

        var inputAck = await harness.Root.RequestAsync<PlacementAssignmentAck>(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = BuildAssignment(
                    assignmentId: "assign-input",
                    brainId: brainId,
                    workerNodeId: workerNodeId,
                    placementEpoch: 4,
                    regionId: 0,
                    shardIndex: 0,
                    target: PlacementAssignmentTarget.PlacementTargetInputCoordinator,
                    actorName: inputName,
                    neuronStart: 0,
                    neuronCount: 0)
            });
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, inputAck.State);

        var outputAck = await harness.Root.RequestAsync<PlacementAssignmentAck>(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = BuildAssignment(
                    assignmentId: "assign-output",
                    brainId: brainId,
                    workerNodeId: workerNodeId,
                    placementEpoch: 4,
                    regionId: 31,
                    shardIndex: 0,
                    target: PlacementAssignmentTarget.PlacementTargetOutputCoordinator,
                    actorName: outputName,
                    neuronStart: 0,
                    neuronCount: 0)
            });
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, outputAck.State);

        var shardAck = await harness.Root.RequestAsync<PlacementAssignmentAck>(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = BuildAssignment(
                    assignmentId: "assign-shard",
                    brainId: brainId,
                    workerNodeId: workerNodeId,
                    placementEpoch: 4,
                    regionId: 5,
                    shardIndex: 0,
                    target: PlacementAssignmentTarget.PlacementTargetRegionShard,
                    actorName: shardName,
                    neuronStart: 0,
                    neuronCount: 8)
            });
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, shardAck.State);

        var reconcile = await harness.Root.RequestAsync<PlacementReconcileReport>(
            workerPid,
            new PlacementReconcileRequest
            {
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = 4
            });

        Assert.Equal(3, reconcile.Assignments.Count);
        var inputObserved = Assert.Single(reconcile.Assignments, static assignment => assignment.AssignmentId == "assign-input");
        var outputObserved = Assert.Single(reconcile.Assignments, static assignment => assignment.AssignmentId == "assign-output");
        var shardObserved = Assert.Single(reconcile.Assignments, static assignment => assignment.AssignmentId == "assign-shard");
        Assert.EndsWith($"/{inputName}", inputObserved.ActorPid, StringComparison.Ordinal);
        Assert.EndsWith($"/{outputName}", outputObserved.ActorPid, StringComparison.Ordinal);
        Assert.EndsWith($"/{shardName}", shardObserved.ActorPid, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PlacementAssignments_DefaultWorkerRoles_AcceptFullPlacementSet()
    {
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "127.0.0.1:12041")));

        var brainId = Guid.NewGuid();
        var placementEpoch = 10UL;

        async Task<PlacementAssignmentAck> SendAsync(
            string assignmentId,
            PlacementAssignmentTarget target,
            uint regionId,
            uint shardIndex,
            uint neuronStart,
            uint neuronCount,
            string actorName)
            => await harness.Root.RequestAsync<PlacementAssignmentAck>(
                workerPid,
                new PlacementAssignmentRequest
                {
                    Assignment = BuildAssignment(
                        assignmentId: assignmentId,
                        brainId: brainId,
                        workerNodeId: workerNodeId,
                        placementEpoch: placementEpoch,
                        regionId: regionId,
                        shardIndex: shardIndex,
                        target: target,
                        actorName: actorName,
                        neuronStart: neuronStart,
                        neuronCount: neuronCount)
                });

        var rootAck = await SendAsync(
            assignmentId: "assign-all-root",
            target: PlacementAssignmentTarget.PlacementTargetBrainRoot,
            regionId: 0,
            shardIndex: 0,
            neuronStart: 0,
            neuronCount: 0,
            actorName: $"brain-{brainId:N}-root-fullset");
        var routerAck = await SendAsync(
            assignmentId: "assign-all-router",
            target: PlacementAssignmentTarget.PlacementTargetSignalRouter,
            regionId: 0,
            shardIndex: 0,
            neuronStart: 0,
            neuronCount: 0,
            actorName: $"brain-{brainId:N}-router-fullset");
        var inputAck = await SendAsync(
            assignmentId: "assign-all-input",
            target: PlacementAssignmentTarget.PlacementTargetInputCoordinator,
            regionId: 0,
            shardIndex: 0,
            neuronStart: 0,
            neuronCount: 0,
            actorName: $"brain-{brainId:N}-input-fullset");
        var outputAck = await SendAsync(
            assignmentId: "assign-all-output",
            target: PlacementAssignmentTarget.PlacementTargetOutputCoordinator,
            regionId: 31,
            shardIndex: 0,
            neuronStart: 0,
            neuronCount: 0,
            actorName: $"brain-{brainId:N}-output-fullset");
        var shardAck = await SendAsync(
            assignmentId: "assign-all-shard",
            target: PlacementAssignmentTarget.PlacementTargetRegionShard,
            regionId: 5,
            shardIndex: 0,
            neuronStart: 0,
            neuronCount: 8,
            actorName: $"brain-{brainId:N}-r5-s0-fullset");

        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, rootAck.State);
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, routerAck.State);
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, inputAck.State);
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, outputAck.State);
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, shardAck.State);

        var snapshot = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());
        Assert.Equal(5, snapshot.TrackedAssignmentCount);
        Assert.Equal(WorkerServiceRole.All, snapshot.EnabledRoles);
    }

    [Fact]
    public async Task PlacementAssignmentRequest_DisabledServiceRole_IsRejected_WithActionableReason()
    {
        await using var harness = await WorkerHarness.CreateAsync();

        var workerNodeId = Guid.NewGuid();
        var enabledRoles = WorkerServiceRole.All & ~WorkerServiceRole.RegionShard;
        var workerPid = harness.Root.Spawn(
            Props.FromProducer(() => new WorkerNodeActor(
                workerNodeId,
                "127.0.0.1:12041",
                enabledRoles: enabledRoles)));

        var ack = await harness.Root.RequestAsync<PlacementAssignmentAck>(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = BuildAssignment(
                    assignmentId: "assign-disabled-shard",
                    brainId: Guid.NewGuid(),
                    workerNodeId: workerNodeId,
                    placementEpoch: 1,
                    regionId: 5,
                    shardIndex: 0,
                    target: PlacementAssignmentTarget.PlacementTargetRegionShard,
                    neuronStart: 0,
                    neuronCount: 8)
            });

        Assert.Equal(PlacementAssignmentState.PlacementAssignmentFailed, ack.State);
        Assert.False(ack.Accepted);
        Assert.Equal(PlacementFailureReason.PlacementFailureAssignmentRejected, ack.FailureReason);
        Assert.Contains("service role 'region-shard'", ack.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("--service-role region-shard", ack.Message, StringComparison.OrdinalIgnoreCase);

        var snapshot = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());
        Assert.Equal(0, snapshot.TrackedAssignmentCount);
        Assert.Equal(enabledRoles, snapshot.EnabledRoles);
    }

    [Fact]
    public async Task PlacementAssignmentRequest_ArtifactBackedShardLoadFailure_ReturnsFailedAck()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-worker-artifact-load-fail-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            await using var harness = await WorkerHarness.CreateAsync();

            var workerNodeId = Guid.NewGuid();
            var ioName = $"io-{Guid.NewGuid():N}";
            var workerName = $"worker-{Guid.NewGuid():N}";
            var workerPid = harness.Root.SpawnNamed(
                Props.FromProducer(() => new WorkerNodeActor(
                    workerNodeId,
                    "worker.local",
                    artifactStore: new ThrowingArtifactStore("simulated artifact store load failure"))),
                workerName);
            var ioPid = harness.Root.SpawnNamed(
                Props.FromProducer(() => new IoGatewayActor(CreateIoOptions())),
                ioName);

            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var known = new Dictionary<string, ServiceEndpointRegistration>(StringComparer.Ordinal)
            {
                [ServiceEndpointSettings.IoGatewayKey] = new ServiceEndpointRegistration(
                    ServiceEndpointSettings.IoGatewayKey,
                    new ServiceEndpoint(string.Empty, ioName),
                    nowMs)
            };
            harness.Root.Send(workerPid, new WorkerNodeActor.DiscoverySnapshotApplied(known));

            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var minimalNbn = NbnTestVectors.CreateMinimalNbn();
            var manifest = await store.StoreAsync(new MemoryStream(minimalNbn), "application/x-nbn");
            var brainDef = manifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifest.ByteLength, "application/x-nbn", artifactRoot);

            var brainId = Guid.NewGuid();
            harness.Root.Send(ioPid, new Nbn.Proto.Io.RegisterBrain
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 4,
                OutputWidth = 4,
                BaseDefinition = brainDef
            });

            await WaitForAsync(
                async () =>
                {
                    var info = await harness.Root.RequestAsync<Nbn.Proto.Io.BrainInfo>(
                        ioPid,
                        new Nbn.Proto.Io.BrainInfoRequest
                        {
                            BrainId = brainId.ToProtoUuid()
                        });

                    return info.BaseDefinition is not null
                           && info.BaseDefinition.TryToSha256Hex(out var infoSha)
                           && string.Equals(infoSha, brainDef.ToSha256Hex(), StringComparison.OrdinalIgnoreCase);
                },
                timeoutMs: 2_000);

            var ack = await harness.Root.RequestAsync<PlacementAssignmentAck>(
                workerPid,
                new PlacementAssignmentRequest
                {
                    Assignment = BuildAssignment(
                        assignmentId: "assign-artifact-load-fail",
                        brainId: brainId,
                        workerNodeId: workerNodeId,
                        placementEpoch: 1,
                        regionId: 0,
                        shardIndex: 0,
                        target: PlacementAssignmentTarget.PlacementTargetRegionShard,
                        neuronStart: 0,
                        neuronCount: 2)
                });

            Assert.False(ack.Accepted);
            Assert.Equal(PlacementAssignmentState.PlacementAssignmentFailed, ack.State);
            Assert.Equal(PlacementFailureReason.PlacementFailureInternalError, ack.FailureReason);
            Assert.Contains("artifact-backed shard load failed", ack.Message, StringComparison.OrdinalIgnoreCase);

            var snapshot = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
                workerPid,
                new WorkerNodeActor.GetWorkerNodeSnapshot());
            Assert.Equal(0, snapshot.TrackedAssignmentCount);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task PlacementAssignmentRequest_BrainInfoMissingBaseDefinition_UsesExportFallbackForArtifactBackedShard()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-worker-export-fallback-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            await using var harness = await WorkerHarness.CreateAsync();
            var artifactStore = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));

            var richNbn = NbnTestVectors.CreateRichNbnVector().Bytes;
            var manifest = await artifactStore.StoreAsync(new MemoryStream(richNbn), "application/x-nbn");
            var brainDef = manifest.ArtifactId.Bytes.ToArray()
                .ToArtifactRef((ulong)manifest.ByteLength, "application/x-nbn", artifactRoot);

            var brainId = Guid.NewGuid();
            var workerNodeId = Guid.NewGuid();
            var ioName = $"io-{Guid.NewGuid():N}";
            var workerName = $"worker-{Guid.NewGuid():N}";
            _ = harness.Root.SpawnNamed(
                Props.FromProducer(() => new DelayedMetadataProbeActor(brainId, brainDef, exportMissCount: 0)),
                ioName);
            var workerPid = harness.Root.SpawnNamed(
                Props.FromProducer(() => new WorkerNodeActor(
                    workerNodeId,
                    "worker.local",
                    artifactStore: artifactStore)),
                workerName);

            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var known = new Dictionary<string, ServiceEndpointRegistration>(StringComparer.Ordinal)
            {
                [ServiceEndpointSettings.IoGatewayKey] = new ServiceEndpointRegistration(
                    ServiceEndpointSettings.IoGatewayKey,
                    new ServiceEndpoint(string.Empty, ioName),
                    nowMs)
            };
            harness.Root.Send(workerPid, new WorkerNodeActor.DiscoverySnapshotApplied(known));

            var ack = await harness.Root.RequestAsync<PlacementAssignmentAck>(
                workerPid,
                new PlacementAssignmentRequest
                {
                    Assignment = BuildAssignment(
                        assignmentId: "assign-export-fallback",
                        brainId: brainId,
                        workerNodeId: workerNodeId,
                        placementEpoch: 1,
                        regionId: 1,
                        shardIndex: 0,
                        target: PlacementAssignmentTarget.PlacementTargetRegionShard,
                        neuronStart: 0,
                        neuronCount: 4)
                });

            Assert.True(ack.Accepted);
            Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, ack.State);

            var reconcile = await harness.Root.RequestAsync<PlacementReconcileReport>(
                workerPid,
                new PlacementReconcileRequest
                {
                    BrainId = brainId.ToProtoUuid(),
                    PlacementEpoch = 1
                });
            _ = Assert.Single(reconcile.Assignments, static assignment => assignment.AssignmentId == "assign-export-fallback");
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task PlacementAssignmentRequest_ArtifactMetadataAppearsAfterTransientMiss_RetriesAndLoadsArtifactBackedShard()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-worker-metadata-retry-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            await using var harness = await WorkerHarness.CreateAsync();
            var artifactStore = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));

            var richNbn = NbnTestVectors.CreateRichNbnVector().Bytes;
            var manifest = await artifactStore.StoreAsync(new MemoryStream(richNbn), "application/x-nbn");
            var brainDef = manifest.ArtifactId.Bytes.ToArray()
                .ToArtifactRef((ulong)manifest.ByteLength, "application/x-nbn", artifactRoot);

            var brainId = Guid.NewGuid();
            var workerNodeId = Guid.NewGuid();
            var ioName = $"io-{Guid.NewGuid():N}";
            var workerName = $"worker-{Guid.NewGuid():N}";

            _ = harness.Root.SpawnNamed(
                Props.FromProducer(() => new DelayedMetadataProbeActor(brainId, brainDef, exportMissCount: 2)),
                ioName);
            var workerPid = harness.Root.SpawnNamed(
                Props.FromProducer(() => new WorkerNodeActor(
                    workerNodeId,
                    "worker.local",
                    artifactStore: artifactStore)),
                workerName);

            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var known = new Dictionary<string, ServiceEndpointRegistration>(StringComparer.Ordinal)
            {
                [ServiceEndpointSettings.IoGatewayKey] = new ServiceEndpointRegistration(
                    ServiceEndpointSettings.IoGatewayKey,
                    new ServiceEndpoint(string.Empty, ioName),
                    nowMs)
            };
            harness.Root.Send(workerPid, new WorkerNodeActor.DiscoverySnapshotApplied(known));

            var ack = await harness.Root.RequestAsync<PlacementAssignmentAck>(
                workerPid,
                new PlacementAssignmentRequest
                {
                    Assignment = BuildAssignment(
                        assignmentId: "assign-metadata-retry",
                        brainId: brainId,
                        workerNodeId: workerNodeId,
                        placementEpoch: 1,
                        regionId: 1,
                        shardIndex: 0,
                        target: PlacementAssignmentTarget.PlacementTargetRegionShard,
                        neuronStart: 0,
                        neuronCount: 4)
                });

            Assert.True(ack.Accepted);
            Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, ack.State);

            var reconcile = await harness.Root.RequestAsync<PlacementReconcileReport>(
                workerPid,
                new PlacementReconcileRequest
                {
                    BrainId = brainId.ToProtoUuid(),
                    PlacementEpoch = 1
                });
            _ = Assert.Single(reconcile.Assignments, static assignment => assignment.AssignmentId == "assign-metadata-retry");
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task PlacementAssignmentRequest_ArtifactMetadataUnavailable_ReturnsRetryableFailureAck()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-worker-metadata-missing-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            await using var harness = await WorkerHarness.CreateAsync();
            var artifactStore = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));

            var richNbn = NbnTestVectors.CreateRichNbnVector().Bytes;
            var manifest = await artifactStore.StoreAsync(new MemoryStream(richNbn), "application/x-nbn");
            var brainDef = manifest.ArtifactId.Bytes.ToArray()
                .ToArtifactRef((ulong)manifest.ByteLength, "application/x-nbn", artifactRoot);

            var brainId = Guid.NewGuid();
            var workerNodeId = Guid.NewGuid();
            var ioName = $"io-{Guid.NewGuid():N}";
            var workerName = $"worker-{Guid.NewGuid():N}";

            _ = harness.Root.SpawnNamed(
                Props.FromProducer(() => new DelayedMetadataProbeActor(brainId, brainDef, exportMissCount: int.MaxValue)),
                ioName);
            var workerPid = harness.Root.SpawnNamed(
                Props.FromProducer(() => new WorkerNodeActor(
                    workerNodeId,
                    "worker.local",
                    artifactStore: artifactStore)),
                workerName);

            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var known = new Dictionary<string, ServiceEndpointRegistration>(StringComparer.Ordinal)
            {
                [ServiceEndpointSettings.IoGatewayKey] = new ServiceEndpointRegistration(
                    ServiceEndpointSettings.IoGatewayKey,
                    new ServiceEndpoint(string.Empty, ioName),
                    nowMs)
            };
            harness.Root.Send(workerPid, new WorkerNodeActor.DiscoverySnapshotApplied(known));

            var ack = await harness.Root.RequestAsync<PlacementAssignmentAck>(
                workerPid,
                new PlacementAssignmentRequest
                {
                    Assignment = BuildAssignment(
                        assignmentId: "assign-metadata-missing",
                        brainId: brainId,
                        workerNodeId: workerNodeId,
                        placementEpoch: 1,
                        regionId: 1,
                        shardIndex: 0,
                        target: PlacementAssignmentTarget.PlacementTargetRegionShard,
                        neuronStart: 0,
                        neuronCount: 4)
                });

            Assert.False(ack.Accepted);
            Assert.Equal(PlacementAssignmentState.PlacementAssignmentFailed, ack.State);
            Assert.True(ack.Retryable);
            Assert.Equal(PlacementFailureReason.PlacementFailureWorkerUnavailable, ack.FailureReason);
            Assert.True(ack.RetryAfterMs > 0);
            Assert.Contains("artifact metadata unavailable", ack.Message, StringComparison.OrdinalIgnoreCase);

            var snapshot = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
                workerPid,
                new WorkerNodeActor.GetWorkerNodeSnapshot());
            Assert.Equal(0, snapshot.TrackedAssignmentCount);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task PlacementAssignmentRequest_MetadataMissingWithHiveHint_ReturnsRetryableFailureAck()
    {
        await using var harness = await WorkerHarness.CreateAsync();

        var brainId = Guid.NewGuid();
        var workerNodeId = Guid.NewGuid();
        var workerName = $"worker-{Guid.NewGuid():N}";
        var workerPid = harness.Root.SpawnNamed(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "worker.local")),
            workerName);

        var assignmentAck = new TaskCompletionSource<PlacementAssignmentAck>(TaskCreationOptions.RunContinuationsAsynchronously);
        var request = new PlacementAssignmentRequest
        {
            Assignment = BuildAssignment(
                assignmentId: "assign-hive-hint-metadata-miss",
                brainId: brainId,
                workerNodeId: workerNodeId,
                placementEpoch: 1,
                regionId: 1,
                shardIndex: 0,
                target: PlacementAssignmentTarget.PlacementTargetRegionShard,
                neuronStart: 0,
                neuronCount: 4)
        };

        _ = harness.Root.SpawnNamed(
            Props.FromProducer(() => new HiveHintMetadataMissProbeActor(workerPid, request, assignmentAck)),
            $"hive-hint-{Guid.NewGuid():N}");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var ack = await assignmentAck.Task.WaitAsync(cts.Token);

        Assert.False(ack.Accepted);
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentFailed, ack.State);
        Assert.True(ack.Retryable);
        Assert.Equal(PlacementFailureReason.PlacementFailureWorkerUnavailable, ack.FailureReason);
        Assert.Contains("artifact metadata unavailable", ack.Message, StringComparison.OrdinalIgnoreCase);

        var snapshot = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());
        Assert.Equal(0, snapshot.TrackedAssignmentCount);
    }

    [Fact]
    public async Task PlacementAssignmentRequest_MetadataMissingWithoutEndpointContext_DoesNotTrackHostedAssignment()
    {
        await using var harness = await WorkerHarness.CreateAsync();

        var brainId = Guid.NewGuid();
        var workerNodeId = Guid.NewGuid();
        var workerName = $"worker-{Guid.NewGuid():N}";
        var workerPid = harness.Root.SpawnNamed(
            Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "worker.local")),
            workerName);

        harness.Root.Send(
            workerPid,
            new PlacementAssignmentRequest
            {
                Assignment = BuildAssignment(
                    assignmentId: "assign-metadata-missing-no-context",
                    brainId: brainId,
                    workerNodeId: workerNodeId,
                    placementEpoch: 1,
                    regionId: 1,
                    shardIndex: 0,
                    target: PlacementAssignmentTarget.PlacementTargetRegionShard,
                    neuronStart: 0,
                    neuronCount: 4)
            });
        await Task.Delay(150);

        var snapshot = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            workerPid,
            new WorkerNodeActor.GetWorkerNodeSnapshot());
        Assert.Equal(0, snapshot.TrackedAssignmentCount);
    }

    [Fact]
    public async Task PlacementAssignmentRequest_BaseDefinitionStoreUriDifferentFromWorkerRoot_LoadsArtifactBackedShard()
    {
        var sourceArtifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-worker-source-store-{Guid.NewGuid():N}");
        var workerArtifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-worker-runtime-store-{Guid.NewGuid():N}");
        Directory.CreateDirectory(sourceArtifactRoot);
        Directory.CreateDirectory(workerArtifactRoot);

        try
        {
            await using var harness = await WorkerHarness.CreateAsync();

            var sourceStore = new LocalArtifactStore(new ArtifactStoreOptions(sourceArtifactRoot));
            var richNbn = NbnTestVectors.CreateRichNbnVector().Bytes;
            var manifest = await sourceStore.StoreAsync(new MemoryStream(richNbn), "application/x-nbn");
            var brainDef = manifest.ArtifactId.Bytes.ToArray()
                .ToArtifactRef((ulong)manifest.ByteLength, "application/x-nbn", sourceArtifactRoot);

            var brainId = Guid.NewGuid();
            var workerNodeId = Guid.NewGuid();
            var ioName = $"io-{Guid.NewGuid():N}";
            var workerName = $"worker-{Guid.NewGuid():N}";

            _ = harness.Root.SpawnNamed(
                Props.FromProducer(() => new FixedBrainInfoWithBaseDefinitionActor(brainDef, inputWidth: 2, outputWidth: 2)),
                ioName);
            var workerPid = harness.Root.SpawnNamed(
                Props.FromProducer(() => new WorkerNodeActor(
                    workerNodeId,
                    "worker.local",
                    artifactRootPath: workerArtifactRoot)),
                workerName);

            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var known = new Dictionary<string, ServiceEndpointRegistration>(StringComparer.Ordinal)
            {
                [ServiceEndpointSettings.IoGatewayKey] = new ServiceEndpointRegistration(
                    ServiceEndpointSettings.IoGatewayKey,
                    new ServiceEndpoint(string.Empty, ioName),
                    nowMs)
            };
            harness.Root.Send(workerPid, new WorkerNodeActor.DiscoverySnapshotApplied(known));

            var ack = await harness.Root.RequestAsync<PlacementAssignmentAck>(
                workerPid,
                new PlacementAssignmentRequest
                {
                    Assignment = BuildAssignment(
                        assignmentId: "assign-store-uri-differs",
                        brainId: brainId,
                        workerNodeId: workerNodeId,
                        placementEpoch: 1,
                        regionId: NbnConstants.InputRegionId,
                        shardIndex: 0,
                        target: PlacementAssignmentTarget.PlacementTargetRegionShard,
                        neuronStart: 0,
                        neuronCount: 1)
                });

            Assert.True(ack.Accepted);
            Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, ack.State);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(sourceArtifactRoot))
            {
                Directory.Delete(sourceArtifactRoot, recursive: true);
            }

            if (Directory.Exists(workerArtifactRoot))
            {
                Directory.Delete(workerArtifactRoot, recursive: true);
            }
        }
    }

    private static IoOptions CreateIoOptions()
        => new(
            BindHost: "127.0.0.1",
            Port: 0,
            AdvertisedHost: null,
            AdvertisedPort: null,
            GatewayName: IoNames.Gateway,
            ServerName: "nbn.io.tests",
            SettingsHost: null,
            SettingsPort: 0,
            SettingsName: "SettingsMonitor",
            HiveMindAddress: null,
            HiveMindName: null,
            ReproAddress: null,
            ReproName: null);

    private static PlacementAssignment BuildAssignment(
        string assignmentId,
        Guid brainId,
        Guid workerNodeId,
        ulong placementEpoch,
        uint regionId,
        uint shardIndex,
        PlacementAssignmentTarget target = PlacementAssignmentTarget.PlacementTargetRegionShard,
        string? actorName = null,
        uint neuronStart = 0,
        uint neuronCount = 64)
        => new()
        {
            AssignmentId = assignmentId,
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = placementEpoch,
            Target = target,
            WorkerNodeId = workerNodeId.ToProtoUuid(),
            RegionId = regionId,
            ShardIndex = shardIndex,
            NeuronStart = neuronStart,
            NeuronCount = neuronCount,
            ActorName = actorName ?? $"region-{regionId}-shard-{shardIndex}"
        };

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

        throw new Xunit.Sdk.XunitException($"Condition was not met within {timeoutMs} ms.");
    }

    private static async Task<PlacementAssignmentAck> SendPlacementRequestViaNamedSenderAsync(
        ActorSystem system,
        PID workerPid,
        PlacementAssignmentRequest request)
    {
        var completion = new TaskCompletionSource<PlacementAssignmentAck>(TaskCreationOptions.RunContinuationsAsynchronously);
        var senderPid = system.Root.SpawnNamed(
            Props.FromProducer(() => new PlacementRequestProbeActor(workerPid, request, completion)),
            $"worker-test-probe-{Guid.NewGuid():N}");

        try
        {
            return await completion.Task.WaitAsync(TimeSpan.FromSeconds(5));
        }
        finally
        {
            system.Root.Stop(senderPid);
        }
    }

    private sealed class PlacementRequestProbeActor : IActor
    {
        private readonly PID _workerPid;
        private readonly PlacementAssignmentRequest _request;
        private readonly TaskCompletionSource<PlacementAssignmentAck> _completion;

        public PlacementRequestProbeActor(
            PID workerPid,
            PlacementAssignmentRequest request,
            TaskCompletionSource<PlacementAssignmentAck> completion)
        {
            _workerPid = workerPid;
            _request = request;
            _completion = completion;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Started:
                    context.Request(_workerPid, _request);
                    break;
                case PlacementAssignmentAck ack:
                    _completion.TrySetResult(ack);
                    context.Stop(context.Self);
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class HiveMindForwardProbeActor : IActor
    {
        private readonly PID _workerPid;
        private readonly PlacementAssignmentRequest _seedRequest;
        private readonly TaskCompletionSource<PlacementAssignmentAck> _assignmentAck;
        private readonly TaskCompletionSource<TickComputeDone> _computeDone;

        public HiveMindForwardProbeActor(
            PID workerPid,
            PlacementAssignmentRequest seedRequest,
            TaskCompletionSource<PlacementAssignmentAck> assignmentAck,
            TaskCompletionSource<TickComputeDone> computeDone)
        {
            _workerPid = workerPid;
            _seedRequest = seedRequest;
            _assignmentAck = assignmentAck;
            _computeDone = computeDone;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Started:
                    context.Request(_workerPid, _seedRequest);
                    break;
                case PlacementAssignmentAck ack:
                    _assignmentAck.TrySetResult(ack);
                    break;
                case TickComputeDone done:
                    _computeDone.TrySetResult(done);
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingArtifactStore : IArtifactStore
    {
        private readonly string _message;

        public ThrowingArtifactStore(string message)
        {
            _message = string.IsNullOrWhiteSpace(message) ? "artifact store failure" : message;
        }

        public Task<ArtifactManifest> StoreAsync(
            Stream content,
            string mediaType,
            ArtifactStoreWriteOptions? options = null,
            CancellationToken cancellationToken = default)
            => throw new InvalidOperationException(_message);

        public Task<ArtifactManifest?> TryGetManifestAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException(_message);

        public Task<bool> ContainsAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException(_message);

        public Task<Stream?> TryOpenArtifactAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException(_message);
    }

    private sealed class ExportBrainDefinitionProbeActor : IActor
    {
        private readonly Guid _brainId;
        private readonly Nbn.Proto.ArtifactRef _baseDefinition;
        private readonly uint _inputWidth;
        private readonly uint _outputWidth;

        public ExportBrainDefinitionProbeActor(Guid brainId, Nbn.Proto.ArtifactRef baseDefinition, uint inputWidth = 3, uint outputWidth = 2)
        {
            _brainId = brainId;
            _baseDefinition = baseDefinition.Clone();
            _inputWidth = inputWidth;
            _outputWidth = outputWidth;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is ProtoControl.GetBrainIoInfo infoRequest
                && infoRequest.BrainId is not null
                && infoRequest.BrainId.TryToGuid(out var infoBrainId)
                && infoBrainId == _brainId)
            {
                context.Respond(new ProtoControl.BrainIoInfo
                {
                    BrainId = infoRequest.BrainId,
                    InputWidth = _inputWidth,
                    OutputWidth = _outputWidth
                });
                return Task.CompletedTask;
            }

            if (context.Message is not Nbn.Proto.Io.ExportBrainDefinition request)
            {
                return Task.CompletedTask;
            }

            if (request.BrainId is not null
                && request.BrainId.TryToGuid(out var brainId)
                && brainId == _brainId)
            {
                context.Respond(new Nbn.Proto.Io.BrainDefinitionReady
                {
                    BrainId = request.BrainId,
                    BrainDef = _baseDefinition.Clone()
                });
                return Task.CompletedTask;
            }

            context.Respond(new Nbn.Proto.Io.BrainDefinitionReady
            {
                BrainId = request.BrainId
            });
            return Task.CompletedTask;
        }
    }

    private sealed class DelayedMetadataProbeActor : IActor
    {
        private readonly Guid _brainId;
        private readonly Nbn.Proto.ArtifactRef _baseDefinition;
        private int _remainingExportMisses;

        public DelayedMetadataProbeActor(Guid brainId, Nbn.Proto.ArtifactRef baseDefinition, int exportMissCount)
        {
            _brainId = brainId;
            _baseDefinition = baseDefinition.Clone();
            _remainingExportMisses = Math.Max(0, exportMissCount);
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Nbn.Proto.Io.BrainInfoRequest infoRequest:
                {
                    var response = new Nbn.Proto.Io.BrainInfo
                    {
                        BrainId = infoRequest.BrainId,
                        InputWidth = 3,
                        OutputWidth = 2,
                        BaseDefinition = new Nbn.Proto.ArtifactRef(),
                        LastSnapshot = new Nbn.Proto.ArtifactRef()
                    };
                    context.Respond(response);
                    return Task.CompletedTask;
                }
                case Nbn.Proto.Io.ExportBrainDefinition exportRequest:
                {
                    if (exportRequest.BrainId is not null
                        && exportRequest.BrainId.TryToGuid(out var requestedBrainId)
                        && requestedBrainId == _brainId
                        && _remainingExportMisses <= 0)
                    {
                        context.Respond(new Nbn.Proto.Io.BrainDefinitionReady
                        {
                            BrainId = exportRequest.BrainId,
                            BrainDef = _baseDefinition.Clone()
                        });
                        return Task.CompletedTask;
                    }

                    if (_remainingExportMisses > 0)
                    {
                        _remainingExportMisses--;
                    }

                    context.Respond(new Nbn.Proto.Io.BrainDefinitionReady
                    {
                        BrainId = exportRequest.BrainId
                    });
                    return Task.CompletedTask;
                }
            }

            return Task.CompletedTask;
        }
    }

    private sealed class HiveHintMetadataMissProbeActor : IActor
    {
        private readonly PID _workerPid;
        private readonly PlacementAssignmentRequest _request;
        private readonly TaskCompletionSource<PlacementAssignmentAck> _ack;

        public HiveHintMetadataMissProbeActor(
            PID workerPid,
            PlacementAssignmentRequest request,
            TaskCompletionSource<PlacementAssignmentAck> ack)
        {
            _workerPid = workerPid;
            _request = request;
            _ack = ack;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Started:
                    context.Request(_workerPid, _request);
                    break;
                case PlacementAssignmentAck ack:
                    _ack.TrySetResult(ack);
                    break;
                case Nbn.Proto.Io.ExportBrainDefinition exportRequest:
                    context.Respond(new Nbn.Proto.Io.BrainDefinitionReady
                    {
                        BrainId = exportRequest.BrainId
                    });
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class FixedBrainInfoWithBaseDefinitionActor : IActor
    {
        private readonly Nbn.Proto.ArtifactRef _baseDefinition;
        private readonly uint _inputWidth;
        private readonly uint _outputWidth;

        public FixedBrainInfoWithBaseDefinitionActor(Nbn.Proto.ArtifactRef baseDefinition, uint inputWidth, uint outputWidth)
        {
            _baseDefinition = baseDefinition.Clone();
            _inputWidth = inputWidth;
            _outputWidth = outputWidth;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is not Nbn.Proto.Io.BrainInfoRequest request)
            {
                return Task.CompletedTask;
            }

            context.Respond(new Nbn.Proto.Io.BrainInfo
            {
                BrainId = request.BrainId,
                InputWidth = _inputWidth,
                OutputWidth = _outputWidth,
                BaseDefinition = _baseDefinition.Clone(),
                LastSnapshot = new Nbn.Proto.ArtifactRef()
            });
            return Task.CompletedTask;
        }
    }

    private sealed class WorkerHarness : IAsyncDisposable
    {
        private readonly TempDatabaseScope _databaseScope;

        private WorkerHarness(TempDatabaseScope databaseScope, ActorSystem system, PID settingsPid)
        {
            _databaseScope = databaseScope;
            System = system;
            SettingsPid = settingsPid;
        }

        public ActorSystem System { get; }

        public PID SettingsPid { get; }

        public IRootContext Root => System.Root;

        public static async Task<WorkerHarness> CreateAsync()
        {
            var databaseScope = new TempDatabaseScope();
            var store = new SettingsMonitorStore(databaseScope.DatabasePath);
            await store.InitializeAsync();

            var system = new ActorSystem();
            var settingsPid = system.Root.Spawn(Props.FromProducer(() => new SettingsMonitorActor(store)));
            return new WorkerHarness(databaseScope, system, settingsPid);
        }

        public async ValueTask DisposeAsync()
        {
            await System.ShutdownAsync();
            _databaseScope.Dispose();
        }
    }

    private sealed class TempDatabaseScope : IDisposable
    {
        private readonly string _directoryPath;

        public TempDatabaseScope()
        {
            _directoryPath = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directoryPath);
            DatabasePath = Path.Combine(_directoryPath, "settings-monitor.db");
        }

        public string DatabasePath { get; }

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
