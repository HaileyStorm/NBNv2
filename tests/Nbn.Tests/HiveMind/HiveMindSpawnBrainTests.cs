using Microsoft.Data.Sqlite;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Nbn.Tests.Format;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;
using Xunit.Sdk;

namespace Nbn.Tests.HiveMind;

public sealed class HiveMindSpawnBrainTests
{
    [Fact]
    public async Task SpawnBrain_Returns_BrainId_After_Placement_Completes_And_Registers_ControllerPids()
    {
        var (artifactRoot, brainDef) = await StoreBrainDefinitionAsync();
        try
        {
            var system = new ActorSystem();
            var root = system.Root;

            var workerId = Guid.NewGuid();
            var workerProbe = new SpawnPlacementWorkerProbe(workerId, dropAcks: false);
            var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 1_000,
                    retryBackoffMs: 10,
                    maxRetries: 1,
                    reconcileTimeoutMs: 1_000))));

            PrimeWorkers(root, hiveMind, workerPid, workerId);

            var spawnAck = await root.RequestAsync<SpawnBrainAck>(
                hiveMind,
                new SpawnBrain
                {
                    BrainDef = brainDef
                });

            Assert.True(spawnAck.BrainId.TryToGuid(out var brainId));
            Assert.NotEqual(Guid.Empty, brainId);

            var lifecycle = await root.RequestAsync<PlacementLifecycleInfo>(
                hiveMind,
                new GetPlacementLifecycle
                {
                    BrainId = brainId.ToProtoUuid()
                });

            Assert.True(
                lifecycle.LifecycleState == PlacementLifecycleState.PlacementLifecycleAssigned
                || lifecycle.LifecycleState == PlacementLifecycleState.PlacementLifecycleRunning);
            Assert.Equal(PlacementReconcileState.PlacementReconcileMatched, lifecycle.ReconcileState);

            var routing = await root.RequestAsync<BrainRoutingInfo>(
                hiveMind,
                new GetBrainRouting
                {
                    BrainId = brainId.ToProtoUuid()
                });

            Assert.Equal("worker.local/brain-root", routing.BrainRootPid);
            Assert.Equal("worker.local/signal-router", routing.SignalRouterPid);
            Assert.True(workerProbe.ReconcileRequestCount >= 1);

            await system.ShutdownAsync();
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
    public async Task SpawnBrain_Returns_WorkerUnavailable_Details_When_NoWorkers_AreAvailable()
    {
        var (artifactRoot, brainDef) = await StoreBrainDefinitionAsync();
        try
        {
            var system = new ActorSystem();
            var root = system.Root;

            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 250,
                    retryBackoffMs: 10,
                    maxRetries: 1,
                    reconcileTimeoutMs: 250))));

            var spawnAck = await root.RequestAsync<SpawnBrainAck>(
                hiveMind,
                new SpawnBrain
                {
                    BrainDef = brainDef
                });

            Assert.True(spawnAck.BrainId.TryToGuid(out var brainId));
            Assert.Equal(Guid.Empty, brainId);
            Assert.Equal("spawn_worker_unavailable", spawnAck.FailureReasonCode);
            Assert.Contains("No eligible workers are available for placement", spawnAck.FailureMessage, StringComparison.Ordinal);

            await system.ShutdownAsync();
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
    public async Task SpawnBrain_Returns_Empty_BrainId_When_Assignment_Lifecycle_Fails()
    {
        var (artifactRoot, brainDef) = await StoreBrainDefinitionAsync();
        try
        {
            var system = new ActorSystem();
            var root = system.Root;

            var workerId = Guid.NewGuid();
            var workerProbe = new SpawnPlacementWorkerProbe(workerId, dropAcks: true);
            var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 100,
                    retryBackoffMs: 10,
                    maxRetries: 1,
                    reconcileTimeoutMs: 300))));

            PrimeWorkers(root, hiveMind, workerPid, workerId);

            var spawnAck = await root.RequestAsync<SpawnBrainAck>(
                hiveMind,
                new SpawnBrain
                {
                    BrainDef = brainDef
                });

            Assert.True(spawnAck.BrainId.TryToGuid(out var brainId));
            Assert.Equal(Guid.Empty, brainId);
            Assert.Equal("spawn_assignment_timeout", spawnAck.FailureReasonCode);
            Assert.Contains("timed out", spawnAck.FailureMessage, StringComparison.OrdinalIgnoreCase);
            Assert.True(workerProbe.AssignmentRequestCount >= 2);

            await WaitForAsync(
                async () =>
                {
                    var status = await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus());
                    return status.RegisteredBrains == 0;
                },
                timeoutMs: 2_000);

            await system.ShutdownAsync();
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
    public async Task SpawnBrain_Returns_ReconcileMismatch_Details_When_Reconcile_Does_Not_Match_Assignments()
    {
        var (artifactRoot, brainDef) = await StoreBrainDefinitionAsync();
        try
        {
            var system = new ActorSystem();
            var root = system.Root;

            var workerId = Guid.NewGuid();
            var workerProbe = new SpawnPlacementWorkerProbe(
                workerId,
                dropAcks: false,
                reconcileBehavior: SpawnReconcileBehavior.Mismatch);
            var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 250,
                    retryBackoffMs: 10,
                    maxRetries: 1,
                    reconcileTimeoutMs: 300))));

            PrimeWorkers(root, hiveMind, workerPid, workerId);

            var spawnAck = await root.RequestAsync<SpawnBrainAck>(
                hiveMind,
                new SpawnBrain
                {
                    BrainDef = brainDef
                });

            Assert.True(spawnAck.BrainId.TryToGuid(out var brainId));
            Assert.Equal(Guid.Empty, brainId);
            Assert.Equal("spawn_reconcile_mismatch", spawnAck.FailureReasonCode);
            Assert.Contains("mismatch", spawnAck.FailureMessage, StringComparison.OrdinalIgnoreCase);

            await system.ShutdownAsync();
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
    public async Task SpawnBrain_Returns_ReconcileTimeout_Details_When_Reconcile_Reports_Do_Not_Arrive()
    {
        var (artifactRoot, brainDef) = await StoreBrainDefinitionAsync();
        try
        {
            var system = new ActorSystem();
            var root = system.Root;

            var workerId = Guid.NewGuid();
            var workerProbe = new SpawnPlacementWorkerProbe(
                workerId,
                dropAcks: false,
                reconcileBehavior: SpawnReconcileBehavior.Drop);
            var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 250,
                    retryBackoffMs: 10,
                    maxRetries: 1,
                    reconcileTimeoutMs: 100))));

            PrimeWorkers(root, hiveMind, workerPid, workerId);

            var spawnAck = await root.RequestAsync<SpawnBrainAck>(
                hiveMind,
                new SpawnBrain
                {
                    BrainDef = brainDef
                });

            Assert.True(spawnAck.BrainId.TryToGuid(out var brainId));
            Assert.Equal(Guid.Empty, brainId);
            Assert.Equal("spawn_reconcile_timeout", spawnAck.FailureReasonCode);
            Assert.Contains("timed out", spawnAck.FailureMessage, StringComparison.OrdinalIgnoreCase);

            await system.ShutdownAsync();
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

    private static async Task<(string ArtifactRoot, ArtifactRef BrainDef)> StoreBrainDefinitionAsync()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-hivemind-spawn-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
        var richNbn = NbnTestVectors.CreateRichNbnVector();
        var manifest = await store.StoreAsync(new MemoryStream(richNbn.Bytes), "application/x-nbn");
        var brainDef = manifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifest.ByteLength, "application/x-nbn", artifactRoot);
        return (artifactRoot, brainDef);
    }

    private static void PrimeWorkers(IRootContext root, PID hiveMind, PID workerPid, Guid workerId)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
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
                    address: string.Empty,
                    rootActorName: workerPid.Id)
            }
        });
    }

    private static ProtoSettings.WorkerReadinessCapability BuildWorker(
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
                HasGpu = true,
                VramFreeBytes = 8UL * 1024 * 1024 * 1024,
                CpuScore = 40f,
                GpuScore = 80f
            }
        };

    private static HiveMindOptions CreateOptions(
        int assignmentTimeoutMs,
        int retryBackoffMs,
        int maxRetries,
        int reconcileTimeoutMs)
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
            WorkerInventoryStaleAfterMs: 10_000,
            PlacementAssignmentTimeoutMs: assignmentTimeoutMs,
            PlacementAssignmentRetryBackoffMs: retryBackoffMs,
            PlacementAssignmentMaxRetries: maxRetries,
            PlacementReconcileTimeoutMs: reconcileTimeoutMs);

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

        throw new XunitException($"Condition was not met within {timeoutMs} ms.");
    }

    private enum SpawnReconcileBehavior
    {
        Matched,
        Mismatch,
        Drop
    }

    private sealed class SpawnPlacementWorkerProbe : IActor
    {
        private readonly Guid _workerId;
        private readonly bool _dropAcks;
        private readonly SpawnReconcileBehavior _reconcileBehavior;
        private readonly Dictionary<string, PlacementAssignment> _knownAssignments = new(StringComparer.Ordinal);

        public SpawnPlacementWorkerProbe(
            Guid workerId,
            bool dropAcks,
            SpawnReconcileBehavior reconcileBehavior = SpawnReconcileBehavior.Matched)
        {
            _workerId = workerId;
            _dropAcks = dropAcks;
            _reconcileBehavior = reconcileBehavior;
        }

        public int AssignmentRequestCount { get; private set; }
        public int ReconcileRequestCount { get; private set; }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case PlacementAssignmentRequest request:
                    HandlePlacementAssignmentRequest(context, request);
                    break;
                case PlacementReconcileRequest request:
                    HandlePlacementReconcileRequest(context, request);
                    break;
            }

            return Task.CompletedTask;
        }

        private void HandlePlacementAssignmentRequest(IContext context, PlacementAssignmentRequest request)
        {
            if (request.Assignment is null)
            {
                return;
            }

            var assignment = request.Assignment.Clone();
            AssignmentRequestCount++;
            _knownAssignments[assignment.AssignmentId] = assignment;

            if (_dropAcks)
            {
                return;
            }

            context.Respond(new PlacementAssignmentAck
            {
                AssignmentId = assignment.AssignmentId,
                BrainId = assignment.BrainId,
                PlacementEpoch = assignment.PlacementEpoch,
                State = PlacementAssignmentState.PlacementAssignmentReady,
                Accepted = true,
                Retryable = false,
                FailureReason = PlacementFailureReason.PlacementFailureNone,
                Message = "ready"
            });
        }

        private void HandlePlacementReconcileRequest(IContext context, PlacementReconcileRequest request)
        {
            ReconcileRequestCount++;
            if (_reconcileBehavior == SpawnReconcileBehavior.Drop)
            {
                return;
            }

            var report = new PlacementReconcileReport
            {
                BrainId = request.BrainId,
                PlacementEpoch = request.PlacementEpoch,
                ReconcileState = PlacementReconcileState.PlacementReconcileMatched
            };

            var assignments = _knownAssignments.Values
                .OrderBy(static value => value.AssignmentId, StringComparer.Ordinal)
                .ToList();
            if (_reconcileBehavior == SpawnReconcileBehavior.Mismatch && assignments.Count > 0)
            {
                assignments.RemoveAt(0);
            }

            foreach (var assignment in assignments)
            {
                var actorPid = assignment.Target switch
                {
                    PlacementAssignmentTarget.PlacementTargetBrainRoot => "worker.local/brain-root",
                    PlacementAssignmentTarget.PlacementTargetSignalRouter => "worker.local/signal-router",
                    _ => context.Self.Id
                };

                report.Assignments.Add(new PlacementObservedAssignment
                {
                    AssignmentId = assignment.AssignmentId,
                    Target = assignment.Target,
                    WorkerNodeId = _workerId.ToProtoUuid(),
                    RegionId = assignment.RegionId,
                    ShardIndex = assignment.ShardIndex,
                    ActorPid = actorPid
                });
            }

            context.Respond(report);
        }
    }
}
