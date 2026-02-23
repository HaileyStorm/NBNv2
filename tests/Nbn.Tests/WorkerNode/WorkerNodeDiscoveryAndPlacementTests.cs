using Nbn.Proto.Control;
using Nbn.Runtime.SettingsMonitor;
using Nbn.Runtime.WorkerNode;
using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Tests.WorkerNode;

public sealed class WorkerNodeDiscoveryAndPlacementTests
{
    [Fact]
    public async Task DiscoveryBootstrap_And_LiveUpdate_AppliesKnownEndpoints()
    {
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
    }

    [Fact]
    public async Task PlacementAssignmentRequest_TargetingWorker_ReturnsReadyAck()
    {
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
    }

    [Fact]
    public async Task PlacementReconcileRequest_ReturnsTrackedAssignments()
    {
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
    }

    [Fact]
    public async Task PlacementAssignments_BrainRoot_And_Router_AreHosted_And_Wired()
    {
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
