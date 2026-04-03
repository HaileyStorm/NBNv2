using Microsoft.Data.Sqlite;
using System.Diagnostics;
using System.Reflection;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.HiveMind;
using Nbn.Runtime.IO;
using Nbn.Runtime.WorkerNode;
using Nbn.Shared;
using Nbn.Tests.Format;
using Proto;
using ProtoIo = Nbn.Proto.Io;
using ProtoControl = Nbn.Proto.Control;
using ProtoSettings = Nbn.Proto.Settings;
using Xunit.Sdk;

namespace Nbn.Tests.HiveMind;

public sealed class HiveMindSpawnBrainTests
{
    [Fact]
    public void SenderMatchesPid_Treats_AddresslessSender_AsEquivalent_WhenActorIdMatches()
    {
        var sender = new PID(string.Empty, "worker-node-1/brain-root");
        var expected = new PID("192.168.0.14:12041", "worker-node-1/brain-root");

        Assert.True(InvokeSenderMatchesPid(sender, expected));
    }

    [Fact]
    public void SenderMatchesPid_DoesNotMatch_AddresslessSender_WhenActorIdDiffers()
    {
        var sender = new PID(string.Empty, "worker-node-2/brain-root");
        var expected = new PID("192.168.0.14:12041", "worker-node-1/brain-root");

        Assert.False(InvokeSenderMatchesPid(sender, expected));
    }

    [Fact]
    public async Task SpawnBrain_Returns_BrainId_After_Placement_Completes_And_Registers_ControllerPids()
    {
        var (artifactRoot, brainDef) = await StoreBrainDefinitionAsync();
        try
        {
            var system = new ActorSystem();
            var root = system.Root;

            var workerId = Guid.NewGuid();
            var ioName = $"io-{Guid.NewGuid():N}";
            var metadataName = $"brain-info-{Guid.NewGuid():N}";
            var workerAddress = "worker.local";

            var ioPid = new PID(string.Empty, ioName);

            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 1_000,
                    retryBackoffMs: 10,
                    maxRetries: 1,
                    reconcileTimeoutMs: 1_000),
                ioPid: ioPid)));
            var ioGateway = root.SpawnNamed(
                Props.FromProducer(() => new IoGatewayActor(CreateIoOptions(), hiveMindPid: hiveMind)),
                ioName);
            var metadata = root.SpawnNamed(
                Props.FromProducer(() => new FixedBrainInfoActor(brainDef, inputWidth: 4, outputWidth: 4)),
                metadataName);
            var workerPid = root.Spawn(
                Props.FromProducer(() => new WorkerNodeActor(workerId, workerAddress, artifactRootPath: artifactRoot)));

            PrimeWorkerDiscoveryEndpoints(root, workerPid, hiveMind.Id, metadata.Id);
            PrimeWorkers(root, hiveMind, workerPid, workerId);

            await WaitForAsync(
                async () =>
                {
                    var worker = await root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
                        workerPid,
                        new WorkerNodeActor.GetWorkerNodeSnapshot());
                    return worker.IoGatewayEndpoint.HasValue;
                },
                timeoutMs: 2_000);

            var response = await root.RequestAsync<ProtoIo.SpawnBrainViaIOAck>(
                ioGateway,
                new ProtoIo.SpawnBrainViaIO
                {
                    Request = new SpawnBrain
                    {
                        BrainDef = brainDef
                    }
                },
                TimeSpan.FromSeconds(70));

            Assert.NotNull(response.Ack);
            var spawnAck = response.Ack;
            Assert.True(spawnAck.BrainId.TryToGuid(out var brainId));
            var status = await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus());
            var workerStatus = await root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
                workerPid,
                new WorkerNodeActor.GetWorkerNodeSnapshot());
            Assert.True(
                brainId != Guid.Empty,
                $"Expected non-empty brain id but received failure={spawnAck.FailureReasonCode} message={spawnAck.FailureMessage}; hivemindTickLoop={status.TickLoopEnabled} brains={status.RegisteredBrains} pendingCompute={status.PendingCompute} pendingDeliver={status.PendingDeliver} workerAssignments={workerStatus.TrackedAssignmentCount} workerPid={workerPid.Address}/{workerPid.Id}");
            Assert.True(string.IsNullOrWhiteSpace(spawnAck.FailureReasonCode));
            Assert.True(string.IsNullOrWhiteSpace(spawnAck.FailureMessage));
            Assert.True(spawnAck.AcceptedForPlacement);
            Assert.False(spawnAck.PlacementReady);
            Assert.True(string.IsNullOrWhiteSpace(response.FailureReasonCode));
            Assert.True(string.IsNullOrWhiteSpace(response.FailureMessage));

            var readyAck = await AwaitSpawnPlacementAsync(root, hiveMind, brainId);
            Assert.True(readyAck.BrainId.TryToGuid(out var readyBrainId));
            Assert.Equal(brainId, readyBrainId);
            Assert.True(readyAck.AcceptedForPlacement);
            Assert.True(readyAck.PlacementReady);
            Assert.True(string.IsNullOrWhiteSpace(readyAck.FailureReasonCode));
            Assert.True(string.IsNullOrWhiteSpace(readyAck.FailureMessage));

            await WaitForAsync(
                async () =>
                {
                    var status = await root.RequestAsync<PlacementLifecycleInfo>(
                        hiveMind,
                        new GetPlacementLifecycle
                        {
                            BrainId = brainId.ToProtoUuid()
                        });

                    return status.LifecycleState == PlacementLifecycleState.PlacementLifecycleAssigned
                           || status.LifecycleState == PlacementLifecycleState.PlacementLifecycleRunning;
                },
                timeoutMs: 5_000);

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

            Assert.StartsWith("worker.local/", routing.BrainRootPid, StringComparison.Ordinal);
            Assert.EndsWith($"brain-{brainId:N}-root", routing.BrainRootPid, StringComparison.Ordinal);
            Assert.StartsWith("worker.local/", routing.SignalRouterPid, StringComparison.Ordinal);
            Assert.EndsWith($"brain-{brainId:N}-router", routing.SignalRouterPid, StringComparison.Ordinal);

            var ioInfo = await root.RequestAsync<ProtoControl.BrainIoInfo>(
                hiveMind,
                new ProtoControl.GetBrainIoInfo
                {
                    BrainId = brainId.ToProtoUuid()
                });

            Assert.StartsWith("worker.local/", ioInfo.InputCoordinatorPid, StringComparison.Ordinal);
            Assert.EndsWith($"brain-{brainId:N}-input", ioInfo.InputCoordinatorPid, StringComparison.Ordinal);
            Assert.StartsWith("worker.local/", ioInfo.OutputCoordinatorPid, StringComparison.Ordinal);
            Assert.EndsWith($"brain-{brainId:N}-output", ioInfo.OutputCoordinatorPid, StringComparison.Ordinal);
            Assert.False(ioInfo.IoGatewayOwnsInputCoordinator);
            Assert.False(ioInfo.IoGatewayOwnsOutputCoordinator);

            var workerSnapshot = await root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
                workerPid,
                new WorkerNodeActor.GetWorkerNodeSnapshot());
            Assert.True(workerSnapshot.TrackedAssignmentCount >= 5);

            await system.ShutdownAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                DeleteDirectoryWithRetries(artifactRoot);
            }
        }
    }

    [Fact]
    public async Task SpawnBrain_DirectHiveMindRequest_Registers_RemoteCoordinator_Metadata_With_Io()
    {
        var (artifactRoot, brainDef) = await StoreBrainDefinitionAsync();
        try
        {
            var system = new ActorSystem();
            var root = system.Root;

            var workerId = Guid.NewGuid();
            var metadataName = $"brain-info-{Guid.NewGuid():N}";
            var workerAddress = "worker.local";
            var registerTcs = new TaskCompletionSource<ProtoIo.RegisterBrain>(TaskCreationOptions.RunContinuationsAsynchronously);

            var ioProbe = root.SpawnNamed(
                Props.FromProducer(() => new IoRegisterProbeActor(
                    registerTcs,
                    register => !string.IsNullOrWhiteSpace(register.InputCoordinatorPid)
                                && !string.IsNullOrWhiteSpace(register.OutputCoordinatorPid))),
                $"io-register-{Guid.NewGuid():N}");
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 1_000,
                    retryBackoffMs: 10,
                    maxRetries: 1,
                    reconcileTimeoutMs: 1_000),
                ioPid: ioProbe)));
            var metadata = root.SpawnNamed(
                Props.FromProducer(() => new FixedBrainInfoActor(brainDef, inputWidth: 4, outputWidth: 4)),
                metadataName);
            var workerPid = root.Spawn(
                Props.FromProducer(() => new WorkerNodeActor(workerId, workerAddress, artifactRootPath: artifactRoot)));

            PrimeWorkerDiscoveryEndpoints(root, workerPid, hiveMind.Id, metadata.Id);
            PrimeWorkers(root, hiveMind, workerPid, workerId);

            await WaitForAsync(
                async () =>
                {
                    var worker = await root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
                        workerPid,
                        new WorkerNodeActor.GetWorkerNodeSnapshot());
                    return worker.IoGatewayEndpoint.HasValue;
                },
                timeoutMs: 2_000);

            var spawnAck = await root.RequestAsync<SpawnBrainAck>(
                hiveMind,
                new SpawnBrain
                {
                    BrainDef = brainDef
                },
                TimeSpan.FromSeconds(70));

            Assert.True(spawnAck.BrainId.TryToGuid(out var brainId));
            Assert.NotEqual(Guid.Empty, brainId);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var register = await registerTcs.Task.WaitAsync(cts.Token);

            Assert.StartsWith("worker.local/", register.InputCoordinatorPid, StringComparison.Ordinal);
            Assert.EndsWith($"brain-{brainId:N}-input", register.InputCoordinatorPid, StringComparison.Ordinal);
            Assert.StartsWith("worker.local/", register.OutputCoordinatorPid, StringComparison.Ordinal);
            Assert.EndsWith($"brain-{brainId:N}-output", register.OutputCoordinatorPid, StringComparison.Ordinal);
            Assert.False(register.IoGatewayOwnsInputCoordinator);
            Assert.False(register.IoGatewayOwnsOutputCoordinator);

            await system.ShutdownAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                DeleteDirectoryWithRetries(artifactRoot);
            }
        }
    }

    [Fact]
    public async Task SpawnBrain_BurstOnSameWorker_DoesNot_FalseTimeout_While_Queued_Assignments_Drain()
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
	                autoRespondAssignments: false);
	            var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 250,
                    retryBackoffMs: 0,
                    maxRetries: 0,
                    reconcileTimeoutMs: 100))));

            PrimeWorkers(root, hiveMind, workerPid, workerId);

            async Task<Guid> SpawnAsync()
            {
                var ack = await root.RequestAsync<SpawnBrainAck>(
                    hiveMind,
                    new SpawnBrain
                    {
                        BrainDef = brainDef
                    },
                    TimeSpan.FromSeconds(10));
                Assert.True(ack.BrainId.TryToGuid(out var brainId));
                Assert.True(ack.AcceptedForPlacement);
                Assert.False(ack.PlacementReady);
                Assert.True(string.IsNullOrWhiteSpace(ack.FailureReasonCode), ack.FailureMessage);
                return brainId;
            }

            var brainA = await SpawnAsync();
            var brainB = await SpawnAsync();

            var awaitPlacementA = AwaitSpawnPlacementAsync(root, hiveMind, brainA, timeoutMs: 0);
            var awaitPlacementB = AwaitSpawnPlacementAsync(root, hiveMind, brainB, timeoutMs: 0);
            await Task.Delay(50);
            Assert.True(
                workerProbe.AssignmentRequestCount < 9,
                $"Expected the worker lane to avoid dispatching the full brain batch immediately, but saw {workerProbe.AssignmentRequestCount} assignment requests.");

            for (var iteration = 0; iteration < 80 && (!awaitPlacementA.IsCompleted || !awaitPlacementB.IsCompleted); iteration++)
            {
                var released = await root.RequestAsync<ReleaseNextDeferredSpawnAssignmentAckResult>(
                    workerPid,
                    new ReleaseNextDeferredSpawnAssignmentAck());
                if (!released.Released)
                {
                    await Task.Delay(25);
                    continue;
                }

                await Task.Delay(60);
            }

            var readyAckA = await awaitPlacementA.WaitAsync(TimeSpan.FromSeconds(5));
            var readyAckB = await awaitPlacementB.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.True(readyAckA.AcceptedForPlacement);
            Assert.True(readyAckA.PlacementReady);
            Assert.True(string.IsNullOrWhiteSpace(readyAckA.FailureReasonCode), readyAckA.FailureMessage);

            Assert.True(readyAckB.AcceptedForPlacement);
            Assert.True(readyAckB.PlacementReady);
            Assert.True(string.IsNullOrWhiteSpace(readyAckB.FailureReasonCode), readyAckB.FailureMessage);

            await system.ShutdownAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                DeleteDirectoryWithRetries(artifactRoot);
            }
        }
    }

    [Fact]
    public async Task SpawnBrain_RetryableAssignmentTimeout_DoesNot_Advance_To_Later_Assignments_Before_Retrying_Current()
    {
        var (artifactRoot, brainDef) = await StoreBrainDefinitionAsync();
        try
        {
            var system = new ActorSystem();
            var root = system.Root;

            var workerId = Guid.NewGuid();
            var workerProbe = new SpawnPlacementWorkerProbe(
                workerId,
                dropAcks: true);
            var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 120,
                    retryBackoffMs: 10,
                    maxRetries: 3,
                    reconcileTimeoutMs: 500))));

            PrimeWorkers(root, hiveMind, workerPid, workerId);

            var spawnAck = await root.RequestAsync<SpawnBrainAck>(
                hiveMind,
                new SpawnBrain
                {
                    BrainDef = brainDef
                },
                TimeSpan.FromSeconds(10));

            Assert.True(spawnAck.BrainId.TryToGuid(out var brainId));
            Assert.True(spawnAck.AcceptedForPlacement);
            Assert.False(spawnAck.PlacementReady);

            await Task.Delay(220);

            var observedBeforeRelease = await root.RequestAsync<ObservedSpawnAssignmentIdsResult>(
                workerPid,
                new GetObservedSpawnAssignmentIds(),
                TimeSpan.FromSeconds(5));
            Assert.NotEmpty(observedBeforeRelease.AssignmentIds);
            _ = Assert.Single(observedBeforeRelease.AssignmentIds.Distinct(StringComparer.Ordinal));

            root.Send(workerPid, new SetSpawnPlacementWorkerDropAcks(false));

            var placementAck = await AwaitSpawnPlacementAsync(root, hiveMind, brainId, timeoutMs: 10_000);
            Assert.True(placementAck.AcceptedForPlacement);
            Assert.True(placementAck.PlacementReady);
            Assert.True(string.IsNullOrWhiteSpace(placementAck.FailureReasonCode), placementAck.FailureMessage);

            await system.ShutdownAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                DeleteDirectoryWithRetries(artifactRoot);
            }
        }
    }

    [Fact]
    public async Task SpawnBrain_RetryableAssignmentTimeout_UsesSingleAttemptTimeoutWindow_ForRetry()
    {
        var (artifactRoot, brainDef) = await StoreBrainDefinitionAsync();
        try
        {
            var system = new ActorSystem();
            var root = system.Root;

            var workerId = Guid.NewGuid();
            var workerProbe = new SpawnPlacementWorkerProbe(
                workerId,
                dropAcks: true);
            var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 120,
                    retryBackoffMs: 10,
                    maxRetries: 3,
                    reconcileTimeoutMs: 500))));

            PrimeWorkers(root, hiveMind, workerPid, workerId);

            var spawnAck = await root.RequestAsync<SpawnBrainAck>(
                hiveMind,
                new SpawnBrain
                {
                    BrainDef = brainDef
                },
                TimeSpan.FromSeconds(10));

            Assert.True(spawnAck.BrainId.TryToGuid(out _));
            Assert.True(spawnAck.AcceptedForPlacement);
            Assert.False(spawnAck.PlacementReady);

            await Task.Delay(220);

            var assignmentRequestCount = await root.RequestAsync<int>(
                workerPid,
                new GetSpawnPlacementAssignmentRequestCount(),
                TimeSpan.FromSeconds(5));
            Assert.True(
                assignmentRequestCount >= 2,
                $"Expected retry dispatch after the single-attempt timeout window, but observed only {assignmentRequestCount} assignment dispatch(es).");

            await system.ShutdownAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                DeleteDirectoryWithRetries(artifactRoot);
            }
        }
    }

    [Fact]
    public async Task AwaitSpawnPlacement_DefaultRuntimeWait_TimesOutOnIdleProgress_Without_WorstCaseSerialBudget()
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
                autoRespondAssignments: false,
                acceptBeforeReady: true);
            var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 250,
                    retryBackoffMs: 20,
                    maxRetries: 2,
                    reconcileTimeoutMs: 250))));

            PrimeWorkers(root, hiveMind, workerPid, workerId);

            var spawnAck = await root.RequestAsync<SpawnBrainAck>(
                hiveMind,
                new SpawnBrain
                {
                    BrainDef = brainDef
                },
                TimeSpan.FromSeconds(10));

            Assert.True(spawnAck.BrainId.TryToGuid(out var brainId));
            Assert.True(spawnAck.AcceptedForPlacement);
            Assert.False(spawnAck.PlacementReady);

            var started = Stopwatch.StartNew();
            var waitAck = await root.RequestAsync<SpawnBrainAck>(
                hiveMind,
                new AwaitSpawnPlacement
                {
                    BrainId = brainId.ToProtoUuid(),
                    TimeoutMs = 0
                },
                TimeSpan.FromSeconds(5));
            started.Stop();

            Assert.True(waitAck.AcceptedForPlacement);
            Assert.False(waitAck.PlacementReady);
            Assert.Equal("spawn_wait_timeout", waitAck.FailureReasonCode);
            Assert.True(
                started.Elapsed < TimeSpan.FromSeconds(2),
                $"Expected idle-progress timeout well below the old worst-case wait budget, but waited {started.Elapsed}.");

            await system.ShutdownAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                DeleteDirectoryWithRetries(artifactRoot);
            }
        }
    }

    [Fact]
    public async Task SpawnBrainViaIo_AfterFailedSpawnTimeout_CanStillPlaceANewBrain()
    {
        var (artifactRoot, brainDef) = await StoreBrainDefinitionAsync();
        try
        {
            var system = new ActorSystem();
            var root = system.Root;

            var workerId = Guid.NewGuid();
            var ioName = $"io-{Guid.NewGuid():N}";
            var metadataName = $"brain-info-{Guid.NewGuid():N}";
            var ioPid = new PID(string.Empty, ioName);
            var workerProbe = new SpawnPlacementWorkerProbe(workerId, dropAcks: true);
            var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 150,
                    retryBackoffMs: 0,
                    maxRetries: 0,
                    reconcileTimeoutMs: 500),
                ioPid: ioPid)));
            var ioGateway = root.SpawnNamed(
                Props.FromProducer(() => new IoGatewayActor(CreateIoOptions(), hiveMindPid: hiveMind)),
                ioName);
            _ = root.SpawnNamed(
                Props.FromProducer(() => new FixedBrainInfoActor(brainDef, inputWidth: 4, outputWidth: 4)),
                metadataName);

            PrimeWorkerDiscoveryEndpoints(root, workerPid, hiveMind.Id, metadataName);
            PrimeWorkers(root, hiveMind, workerPid, workerId);

            var failedSpawn = await root.RequestAsync<ProtoIo.SpawnBrainViaIOAck>(
                ioGateway,
                new ProtoIo.SpawnBrainViaIO
                {
                    Request = new SpawnBrain
                    {
                        BrainDef = brainDef
                    }
                },
                TimeSpan.FromSeconds(10));
            Assert.NotNull(failedSpawn.Ack);
            Assert.True(failedSpawn.Ack.BrainId.TryToGuid(out var failedBrainId));
            Assert.NotEqual(Guid.Empty, failedBrainId);

            var failedPlacement = await AwaitSpawnPlacementViaIoAsync(root, ioGateway, failedBrainId, timeoutMs: 2_000);
            Assert.NotNull(failedPlacement.Ack);
            Assert.True(failedPlacement.Ack.AcceptedForPlacement);
            Assert.False(failedPlacement.Ack.PlacementReady);
            Assert.False(string.IsNullOrWhiteSpace(failedPlacement.Ack.FailureReasonCode));
            var failedKill = await root.RequestAsync<ProtoIo.KillBrainViaIOAck>(
                ioGateway,
                new ProtoIo.KillBrainViaIO
                {
                    Request = new KillBrain
                    {
                        BrainId = failedBrainId.ToProtoUuid(),
                        Reason = "failed_spawn_timeout_cleanup"
                    }
                },
                TimeSpan.FromSeconds(5));
            Assert.True(failedKill.Accepted, failedKill.FailureMessage);
            await Task.Delay(250);

            root.Send(workerPid, new SetSpawnPlacementWorkerDropAcks(false));

            var successfulSpawn = await root.RequestAsync<ProtoIo.SpawnBrainViaIOAck>(
                ioGateway,
                new ProtoIo.SpawnBrainViaIO
                {
                    Request = new SpawnBrain
                    {
                        BrainDef = brainDef
                    }
                },
                TimeSpan.FromSeconds(10));
            Assert.NotNull(successfulSpawn.Ack);
            Assert.True(successfulSpawn.Ack.BrainId.TryToGuid(out var successfulBrainId));
            Assert.NotEqual(Guid.Empty, successfulBrainId);
            Assert.NotEqual(failedBrainId, successfulBrainId);

            var successfulPlacement = await AwaitSpawnPlacementViaIoAsync(root, ioGateway, successfulBrainId, timeoutMs: 10_000);
            Assert.NotNull(successfulPlacement.Ack);
            Assert.True(successfulPlacement.Ack.AcceptedForPlacement);
            Assert.True(successfulPlacement.Ack.PlacementReady);
            Assert.True(string.IsNullOrWhiteSpace(successfulPlacement.Ack.FailureReasonCode), successfulPlacement.Ack.FailureMessage);

            await system.ShutdownAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                DeleteDirectoryWithRetries(artifactRoot);
            }
        }
    }

    [Fact]
    public async Task SpawnBrain_Respects_WorkerPlacementAccepted_Then_Ready()
    {
        var (artifactRoot, brainDef) = await StoreBrainDefinitionAsync();
        try
        {
            var system = new ActorSystem();
            var root = system.Root;

            var workerId = Guid.NewGuid();
            var ioName = $"io-{Guid.NewGuid():N}";
            var metadataName = $"brain-info-{Guid.NewGuid():N}";
            var ioPid = new PID(string.Empty, ioName);
            var workerProbe = new SpawnPlacementWorkerProbe(
                workerId,
                dropAcks: false,
                autoRespondAssignments: false,
                acceptBeforeReady: true);
            var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 250,
                    retryBackoffMs: 0,
                    maxRetries: 0,
                    reconcileTimeoutMs: 500),
                ioPid: ioPid)));
            var ioGateway = root.SpawnNamed(
                Props.FromProducer(() => new IoGatewayActor(CreateIoOptions(), hiveMindPid: hiveMind)),
                ioName);
            _ = root.SpawnNamed(
                Props.FromProducer(() => new FixedBrainInfoActor(brainDef, inputWidth: 4, outputWidth: 4)),
                metadataName);

            PrimeWorkerDiscoveryEndpoints(root, workerPid, hiveMind.Id, metadataName);
            PrimeWorkers(root, hiveMind, workerPid, workerId);

            var response = await root.RequestAsync<ProtoIo.SpawnBrainViaIOAck>(
                ioGateway,
                new ProtoIo.SpawnBrainViaIO
                {
                    Request = new SpawnBrain
                    {
                        BrainDef = brainDef
                    }
                },
                TimeSpan.FromSeconds(10));

            Assert.NotNull(response.Ack);
            Assert.True(response.Ack.BrainId.TryToGuid(out var brainId));

            var awaitPlacement = AwaitSpawnPlacementAsync(root, hiveMind, brainId, timeoutMs: 0);
            await Task.Delay(150);
            Assert.False(awaitPlacement.IsCompleted);

            for (var iteration = 0; iteration < 20 && !awaitPlacement.IsCompleted; iteration++)
            {
                var released = await root.RequestAsync<ReleaseNextDeferredSpawnAssignmentAckResult>(
                    workerPid,
                    new ReleaseNextDeferredSpawnAssignmentAck());
                if (!released.Released)
                {
                    await Task.Delay(25);
                    continue;
                }

                await Task.Delay(25);
            }

            var readyAck = await awaitPlacement.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(readyAck.AcceptedForPlacement);
            Assert.True(readyAck.PlacementReady);
            Assert.True(string.IsNullOrWhiteSpace(readyAck.FailureReasonCode), readyAck.FailureMessage);

            await system.ShutdownAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                DeleteDirectoryWithRetries(artifactRoot);
            }
        }
    }

    [Fact]
    public async Task SpawnBrain_AcceptedAssignments_ContinueDispatching_BeforeDeferredReady()
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
                autoRespondAssignments: false,
                acceptBeforeReady: true);
            var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 250,
                    retryBackoffMs: 0,
                    maxRetries: 0,
                    reconcileTimeoutMs: 500))));

            PrimeWorkers(root, hiveMind, workerPid, workerId);

            var spawnAck = await root.RequestAsync<SpawnBrainAck>(
                hiveMind,
                new SpawnBrain
                {
                    BrainDef = brainDef
                },
                TimeSpan.FromSeconds(10));

            Assert.True(spawnAck.BrainId.TryToGuid(out var brainId));
            Assert.True(spawnAck.AcceptedForPlacement);
            Assert.False(spawnAck.PlacementReady);

            var awaitPlacement = AwaitSpawnPlacementAsync(root, hiveMind, brainId, timeoutMs: 0);
            await WaitForAsync(
                () => Task.FromResult(workerProbe.AssignmentRequestCount > 1),
                timeoutMs: 1_000);
            Assert.False(awaitPlacement.IsCompleted);

            for (var iteration = 0; iteration < 32 && !awaitPlacement.IsCompleted; iteration++)
            {
                var released = await root.RequestAsync<ReleaseNextDeferredSpawnAssignmentAckResult>(
                    workerPid,
                    new ReleaseNextDeferredSpawnAssignmentAck());
                if (!released.Released)
                {
                    await Task.Delay(25);
                    continue;
                }

                await Task.Delay(25);
            }

            var readyAck = await awaitPlacement.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(readyAck.AcceptedForPlacement);
            Assert.True(readyAck.PlacementReady);
            Assert.True(string.IsNullOrWhiteSpace(readyAck.FailureReasonCode), readyAck.FailureMessage);

            await system.ShutdownAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                DeleteDirectoryWithRetries(artifactRoot);
            }
        }
    }

    [Fact]
    public async Task SpawnBrain_Processes_WorkerPlacementFailure_AfterAccepted()
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
                autoRespondAssignments: false,
                acceptBeforeReady: true,
                failAfterAccepted: true);
            var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 250,
                    retryBackoffMs: 0,
                    maxRetries: 0,
                    reconcileTimeoutMs: 500))));

            PrimeWorkers(root, hiveMind, workerPid, workerId);

            var spawnAck = await root.RequestAsync<SpawnBrainAck>(
                hiveMind,
                new SpawnBrain
                {
                    BrainDef = brainDef
                },
                TimeSpan.FromSeconds(10));

            Assert.True(spawnAck.BrainId.TryToGuid(out var brainId));
            Assert.True(spawnAck.AcceptedForPlacement);
            Assert.False(spawnAck.PlacementReady);

            var awaitPlacement = AwaitSpawnPlacementAsync(root, hiveMind, brainId, timeoutMs: 5_000);
            for (var iteration = 0; iteration < 32 && !awaitPlacement.IsCompleted; iteration++)
            {
                var released = await root.RequestAsync<ReleaseNextDeferredSpawnAssignmentAckResult>(
                    workerPid,
                    new ReleaseNextDeferredSpawnAssignmentAck());
                if (!released.Released)
                {
                    await Task.Delay(25);
                    continue;
                }

                await Task.Delay(25);
            }

            var failedAck = await awaitPlacement.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(failedAck.AcceptedForPlacement);
            Assert.False(failedAck.PlacementReady);
            Assert.Equal("spawn_internal_error", failedAck.FailureReasonCode);
            Assert.Contains("failed_after_accepted", failedAck.FailureMessage, StringComparison.Ordinal);

            await system.ShutdownAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                DeleteDirectoryWithRetries(artifactRoot);
            }
        }
    }

    [Fact]
    public async Task SpawnBrain_IncludesExplicitReplyPid_OnPlacementAssignmentRequests()
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
                    assignmentTimeoutMs: 250,
                    retryBackoffMs: 0,
                    maxRetries: 0,
                    reconcileTimeoutMs: 500))));

            PrimeWorkers(root, hiveMind, workerPid, workerId);

            var spawnAck = await root.RequestAsync<SpawnBrainAck>(
                hiveMind,
                new SpawnBrain
                {
                    BrainDef = brainDef
                },
                TimeSpan.FromSeconds(10));

            Assert.True(spawnAck.BrainId.TryToGuid(out var brainId));
            var readyAck = await AwaitSpawnPlacementAsync(root, hiveMind, brainId, timeoutMs: 5_000);
            Assert.True(readyAck.PlacementReady);

            var observedReplyPids = await root.RequestAsync<ObservedSpawnReplyPidsResult>(
                workerPid,
                new GetObservedSpawnReplyPids(),
                TimeSpan.FromSeconds(5));

            Assert.NotEmpty(observedReplyPids.ReplyPids);
            Assert.All(observedReplyPids.ReplyPids, replyPid =>
            {
                Assert.False(string.IsNullOrWhiteSpace(replyPid));
                Assert.Contains(hiveMind.Id, replyPid, StringComparison.Ordinal);
            });

            await system.ShutdownAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                DeleteDirectoryWithRetries(artifactRoot);
            }
        }
    }

    [Fact]
    public async Task SetOutputVectorSource_PerBrainOverride_Remains_Isolated_From_Later_Global_Default_Changes()
    {
        var (artifactRoot, brainDef) = await StoreBrainDefinitionAsync();
        try
        {
            var system = new ActorSystem();
            var root = system.Root;

            var workerId = Guid.NewGuid();
            var metadataName = $"brain-info-{Guid.NewGuid():N}";
            var workerAddress = "worker.local";

            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 1_000,
                    retryBackoffMs: 10,
                    maxRetries: 1,
                    reconcileTimeoutMs: 1_000))));
            var metadata = root.SpawnNamed(
                Props.FromProducer(() => new FixedBrainInfoActor(brainDef, inputWidth: 4, outputWidth: 4)),
                metadataName);
            var workerPid = root.Spawn(
                Props.FromProducer(() => new WorkerNodeActor(workerId, workerAddress, artifactRootPath: artifactRoot)));

            PrimeWorkerDiscoveryEndpoints(root, workerPid, hiveMind.Id, metadata.Id);
            PrimeWorkers(root, hiveMind, workerPid, workerId);

            await WaitForAsync(
                async () =>
                {
                    var worker = await root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
                        workerPid,
                        new WorkerNodeActor.GetWorkerNodeSnapshot());
                    return worker.IoGatewayEndpoint.HasValue;
                },
                timeoutMs: 2_000);

            async Task<Guid> SpawnBrainAsync()
            {
                var ack = await root.RequestAsync<SpawnBrainAck>(
                    hiveMind,
                    new SpawnBrain
                    {
                        BrainDef = brainDef
                    },
                    TimeSpan.FromSeconds(70));

                Assert.True(ack.BrainId.TryToGuid(out var brainId));
                Assert.NotEqual(Guid.Empty, brainId);
                return brainId;
            }

            var brainA = await SpawnBrainAsync();
            var brainB = await SpawnBrainAsync();

            var perBrainAck = await root.RequestAsync<ProtoControl.SetOutputVectorSourceAck>(
                hiveMind,
                new ProtoControl.SetOutputVectorSource
                {
                    BrainId = brainA.ToProtoUuid(),
                    OutputVectorSource = OutputVectorSource.Potential
                });

            Assert.True(perBrainAck.Accepted);
            Assert.NotNull(perBrainAck.BrainId);
            Assert.True(perBrainAck.BrainId.TryToGuid(out var acknowledgedBrainId));
            Assert.Equal(brainA, acknowledgedBrainId);
            Assert.Equal(OutputVectorSource.Potential, perBrainAck.OutputVectorSource);

            var globalAck = await root.RequestAsync<ProtoControl.SetOutputVectorSourceAck>(
                hiveMind,
                new ProtoControl.SetOutputVectorSource
                {
                    OutputVectorSource = OutputVectorSource.Buffer
                });

            Assert.True(globalAck.Accepted);
            Assert.Equal(OutputVectorSource.Buffer, globalAck.OutputVectorSource);
            Assert.Null(globalAck.BrainId);

            var brainC = await SpawnBrainAsync();

            async Task<ProtoControl.BrainIoInfo> GetIoInfoAsync(Guid brainId)
                => await root.RequestAsync<ProtoControl.BrainIoInfo>(
                    hiveMind,
                    new ProtoControl.GetBrainIoInfo
                    {
                        BrainId = brainId.ToProtoUuid()
                    });

            var infoA = await GetIoInfoAsync(brainA);
            var infoB = await GetIoInfoAsync(brainB);
            var infoC = await GetIoInfoAsync(brainC);

            Assert.Equal(OutputVectorSource.Potential, infoA.OutputVectorSource);
            Assert.Equal(OutputVectorSource.Buffer, infoB.OutputVectorSource);
            Assert.Equal(OutputVectorSource.Buffer, infoC.OutputVectorSource);

            await system.ShutdownAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                DeleteDirectoryWithRetries(artifactRoot);
            }
        }
    }

    [Fact]
    public async Task SpawnBrain_Returns_AssignmentRejected_Details_When_WorkerRole_DisablesBrainRoot()
    {
        var (artifactRoot, brainDef) = await StoreBrainDefinitionAsync();
        try
        {
            var system = new ActorSystem();
            var root = system.Root;

            var workerId = Guid.NewGuid();
            var ioName = $"io-{Guid.NewGuid():N}";
            var metadataName = $"brain-info-{Guid.NewGuid():N}";
            var workerAddress = "worker.local";
            var ioPid = new PID(string.Empty, ioName);

            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 1_000,
                    retryBackoffMs: 10,
                    maxRetries: 1,
                    reconcileTimeoutMs: 1_000),
                ioPid: ioPid)));
            var ioGateway = root.SpawnNamed(
                Props.FromProducer(() => new IoGatewayActor(CreateIoOptions(), hiveMindPid: hiveMind)),
                ioName);
            var metadata = root.SpawnNamed(
                Props.FromProducer(() => new FixedBrainInfoActor(brainDef, inputWidth: 4, outputWidth: 4)),
                metadataName);
            var workerRoles = WorkerServiceRole.All & ~WorkerServiceRole.BrainRoot;
            var workerPid = root.Spawn(
                Props.FromProducer(() => new WorkerNodeActor(
                    workerId,
                    workerAddress,
                    artifactRootPath: artifactRoot,
                    enabledRoles: workerRoles)));

            PrimeWorkerDiscoveryEndpoints(root, workerPid, hiveMind.Id, metadata.Id);
            PrimeWorkers(root, hiveMind, workerPid, workerId);

            await WaitForAsync(
                async () =>
                {
                    var worker = await root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
                        workerPid,
                        new WorkerNodeActor.GetWorkerNodeSnapshot());
                    return worker.IoGatewayEndpoint.HasValue;
                },
                timeoutMs: 2_000);

            var response = await root.RequestAsync<ProtoIo.SpawnBrainViaIOAck>(
                ioGateway,
                new ProtoIo.SpawnBrainViaIO
                {
                    Request = new SpawnBrain
                    {
                        BrainDef = brainDef
                    }
                },
                TimeSpan.FromSeconds(70));

            Assert.NotNull(response.Ack);
            var spawnAck = response.Ack;
            Assert.True(spawnAck.BrainId.TryToGuid(out var brainId));
            Assert.NotEqual(Guid.Empty, brainId);
            Assert.True(spawnAck.AcceptedForPlacement);
            Assert.False(spawnAck.PlacementReady);
            Assert.True(string.IsNullOrWhiteSpace(spawnAck.FailureReasonCode));
            Assert.True(string.IsNullOrWhiteSpace(spawnAck.FailureMessage));
            Assert.True(string.IsNullOrWhiteSpace(response.FailureReasonCode));
            Assert.True(string.IsNullOrWhiteSpace(response.FailureMessage));

            var waitResponse = await AwaitSpawnPlacementViaIoAsync(root, ioGateway, brainId);
            Assert.NotNull(waitResponse.Ack);
            Assert.True(waitResponse.Ack.BrainId.TryToGuid(out var waitedBrainId));
            Assert.Equal(brainId, waitedBrainId);
            Assert.True(waitResponse.Ack.AcceptedForPlacement);
            Assert.False(waitResponse.Ack.PlacementReady);
            Assert.Equal("spawn_assignment_rejected", waitResponse.Ack.FailureReasonCode);
            Assert.Contains("service role 'brain-root'", waitResponse.Ack.FailureMessage, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("disabled", waitResponse.Ack.FailureMessage, StringComparison.OrdinalIgnoreCase);
            Assert.Equal("spawn_assignment_rejected", waitResponse.FailureReasonCode);
            Assert.Equal(waitResponse.Ack.FailureMessage, waitResponse.FailureMessage);

            var workerSnapshot = await root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
                workerPid,
                new WorkerNodeActor.GetWorkerNodeSnapshot());
            Assert.Equal(0, workerSnapshot.TrackedAssignmentCount);
            Assert.Equal(workerRoles, workerSnapshot.EnabledRoles);

            await system.ShutdownAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                DeleteDirectoryWithRetries(artifactRoot);
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
                DeleteDirectoryWithRetries(artifactRoot);
            }
        }
    }

    [Fact]
    public async Task SpawnBrain_Uses_ExplicitIoWidths_When_BaseDefinitionMetadata_Cannot_Be_Resolved()
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
                BrainDef = new string('e', 64).ToArtifactRef(128, "application/x-nbn", "memory+missing://placement/base"),
                InputWidth = 3,
                OutputWidth = 2
            });

        Assert.True(spawnAck.BrainId.TryToGuid(out var brainId));
        Assert.Equal(Guid.Empty, brainId);
        Assert.Equal("spawn_worker_unavailable", spawnAck.FailureReasonCode);
        Assert.Contains("No eligible workers are available for placement", spawnAck.FailureMessage, StringComparison.Ordinal);

        await system.ShutdownAsync();
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
            Assert.NotEqual(Guid.Empty, brainId);
            Assert.True(spawnAck.AcceptedForPlacement);
            Assert.False(spawnAck.PlacementReady);
            Assert.True(string.IsNullOrWhiteSpace(spawnAck.FailureReasonCode));
            Assert.True(string.IsNullOrWhiteSpace(spawnAck.FailureMessage));

            var failureAck = await AwaitSpawnPlacementAsync(root, hiveMind, brainId);
            Assert.True(failureAck.BrainId.TryToGuid(out var failedBrainId));
            Assert.Equal(brainId, failedBrainId);
            Assert.True(failureAck.AcceptedForPlacement);
            Assert.False(failureAck.PlacementReady);
            Assert.Equal("spawn_assignment_timeout", failureAck.FailureReasonCode);
            Assert.Contains("timed out", failureAck.FailureMessage, StringComparison.OrdinalIgnoreCase);
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
                DeleteDirectoryWithRetries(artifactRoot);
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
            Assert.NotEqual(Guid.Empty, brainId);
            Assert.True(spawnAck.AcceptedForPlacement);
            Assert.False(spawnAck.PlacementReady);

            var failureAck = await AwaitSpawnPlacementAsync(root, hiveMind, brainId);
            Assert.True(failureAck.BrainId.TryToGuid(out var failedBrainId));
            Assert.Equal(brainId, failedBrainId);
            Assert.True(failureAck.AcceptedForPlacement);
            Assert.False(failureAck.PlacementReady);
            Assert.Equal("spawn_reconcile_mismatch", failureAck.FailureReasonCode);
            Assert.Contains("mismatch", failureAck.FailureMessage, StringComparison.OrdinalIgnoreCase);

            await system.ShutdownAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                DeleteDirectoryWithRetries(artifactRoot);
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
            Assert.NotEqual(Guid.Empty, brainId);
            Assert.True(spawnAck.AcceptedForPlacement);
            Assert.False(spawnAck.PlacementReady);

            var failureAck = await AwaitSpawnPlacementAsync(root, hiveMind, brainId);
            Assert.True(failureAck.BrainId.TryToGuid(out var failedBrainId));
            Assert.Equal(brainId, failedBrainId);
            Assert.True(failureAck.AcceptedForPlacement);
            Assert.False(failureAck.PlacementReady);
            Assert.Equal("spawn_reconcile_timeout", failureAck.FailureReasonCode);
            Assert.Contains("timed out", failureAck.FailureMessage, StringComparison.OrdinalIgnoreCase);

            await system.ShutdownAsync();
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                DeleteDirectoryWithRetries(artifactRoot);
            }
        }
    }

    private static async Task<(string ArtifactRoot, ArtifactRef BrainDef)> StoreBrainDefinitionAsync()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-hivemind-spawn-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
        var minimalNbn = NbnTestVectors.CreateMinimalNbn();
        var manifest = await store.StoreAsync(new MemoryStream(minimalNbn), "application/x-nbn");
        var brainDef = manifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifest.ByteLength, "application/x-nbn", artifactRoot);
        return (artifactRoot, brainDef);
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

    private static void PrimeWorkerDiscoveryEndpoints(IRootContext root, PID workerPid, string hiveName, string ioName)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var known = new Dictionary<string, ServiceEndpointRegistration>(StringComparer.Ordinal)
        {
            [ServiceEndpointSettings.HiveMindKey] = new ServiceEndpointRegistration(
                ServiceEndpointSettings.HiveMindKey,
                new ServiceEndpoint(string.Empty, hiveName),
                nowMs),
            [ServiceEndpointSettings.IoGatewayKey] = new ServiceEndpointRegistration(
                ServiceEndpointSettings.IoGatewayKey,
                new ServiceEndpoint(string.Empty, ioName),
                nowMs)
        };

        root.Send(workerPid, new WorkerNodeActor.DiscoverySnapshotApplied(known));
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

    private static bool InvokeSenderMatchesPid(PID sender, PID expected)
    {
        var method = typeof(HiveMindActor).GetMethod(
            "SenderMatchesPid",
            BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        return Assert.IsType<bool>(method!.Invoke(null, new object?[] { sender, expected }));
    }

    private static void DeleteDirectoryWithRetries(string artifactRoot)
    {
        if (!Directory.Exists(artifactRoot))
        {
            return;
        }

        for (var attempt = 0; attempt < 5; attempt++)
        {
            try
            {
                Directory.Delete(artifactRoot, recursive: true);
                return;
            }
            catch (IOException) when (attempt < 4)
            {
                SqliteConnection.ClearAllPools();
                Thread.Sleep(50);
            }
            catch (UnauthorizedAccessException) when (attempt < 4)
            {
                SqliteConnection.ClearAllPools();
                Thread.Sleep(50);
            }
        }
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

        throw new XunitException($"Condition was not met within {timeoutMs} ms.");
    }

    private static Task<SpawnBrainAck> AwaitSpawnPlacementAsync(
        IRootContext root,
        PID hiveMind,
        Guid brainId,
        ulong timeoutMs = 5_000)
        => root.RequestAsync<SpawnBrainAck>(
            hiveMind,
            new AwaitSpawnPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                TimeoutMs = timeoutMs
            },
            TimeSpan.FromMilliseconds((double)Math.Max((ulong)100, timeoutMs + 1_000)));

    private static Task<ProtoIo.AwaitSpawnPlacementViaIOAck> AwaitSpawnPlacementViaIoAsync(
        IRootContext root,
        PID ioGateway,
        Guid brainId,
        ulong timeoutMs = 5_000)
        => root.RequestAsync<ProtoIo.AwaitSpawnPlacementViaIOAck>(
            ioGateway,
            new ProtoIo.AwaitSpawnPlacementViaIO
            {
                BrainId = brainId.ToProtoUuid(),
                TimeoutMs = timeoutMs
            },
            TimeSpan.FromMilliseconds((double)Math.Max((ulong)100, timeoutMs + 1_000)));

    private sealed class FixedBrainInfoActor : IActor
    {
        private readonly ArtifactRef _baseDefinition;
        private readonly uint _inputWidth;
        private readonly uint _outputWidth;

        public FixedBrainInfoActor(ArtifactRef baseDefinition, uint inputWidth, uint outputWidth)
        {
            _baseDefinition = baseDefinition.Clone();
            _inputWidth = inputWidth;
            _outputWidth = outputWidth;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is not ProtoIo.BrainInfoRequest request)
            {
                return Task.CompletedTask;
            }

            context.Respond(new ProtoIo.BrainInfo
            {
                BrainId = request.BrainId,
                InputWidth = _inputWidth,
                OutputWidth = _outputWidth,
                BaseDefinition = _baseDefinition.Clone(),
                LastSnapshot = new ArtifactRef()
            });

            return Task.CompletedTask;
        }
    }

    private enum SpawnReconcileBehavior
    {
        Matched,
        Mismatch,
        Drop
    }

    private sealed record ReleaseNextDeferredSpawnAssignmentAck;

    private sealed record ReleaseNextDeferredSpawnAssignmentAckResult(bool Released);

    private sealed record SetSpawnPlacementWorkerDropAcks(bool DropAcks);

    private sealed record GetObservedSpawnAssignmentIds;

    private sealed record GetSpawnPlacementAssignmentRequestCount;

    private sealed record ObservedSpawnAssignmentIdsResult(IReadOnlyList<string> AssignmentIds);

    private sealed record GetObservedSpawnReplyPids;

    private sealed record ObservedSpawnReplyPidsResult(IReadOnlyList<string> ReplyPids);

    private sealed class SpawnPlacementWorkerProbe : IActor
    {
        private readonly Guid _workerId;
        private bool _dropAcks;
        private readonly SpawnReconcileBehavior _reconcileBehavior;
        private readonly bool _autoRespondAssignments;
        private readonly bool _acceptBeforeReady;
        private readonly bool _failAfterAccepted;
        private readonly Dictionary<string, PlacementAssignment> _knownAssignments = new(StringComparer.Ordinal);
        private readonly List<(PID Sender, PlacementAssignmentAck Ack)> _deferredAssignmentAcks = new();
        private readonly List<string> _observedAssignmentIds = new();
        private readonly List<string> _observedReplyPids = new();

        public SpawnPlacementWorkerProbe(
            Guid workerId,
            bool dropAcks,
            SpawnReconcileBehavior reconcileBehavior = SpawnReconcileBehavior.Matched,
            bool autoRespondAssignments = true,
            bool acceptBeforeReady = false,
            bool failAfterAccepted = false)
        {
            _workerId = workerId;
            _dropAcks = dropAcks;
            _reconcileBehavior = reconcileBehavior;
            _autoRespondAssignments = autoRespondAssignments;
            _acceptBeforeReady = acceptBeforeReady;
            _failAfterAccepted = failAfterAccepted;
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
                case ReleaseNextDeferredSpawnAssignmentAck:
                    FlushNextDeferredAssignmentAck(context);
                    break;
                case SetSpawnPlacementWorkerDropAcks update:
                    _dropAcks = update.DropAcks;
                    break;
                case GetObservedSpawnAssignmentIds:
                    context.Respond(new ObservedSpawnAssignmentIdsResult(_observedAssignmentIds.ToArray()));
                    break;
                case GetSpawnPlacementAssignmentRequestCount:
                    context.Respond(AssignmentRequestCount);
                    break;
                case GetObservedSpawnReplyPids:
                    context.Respond(new ObservedSpawnReplyPidsResult(_observedReplyPids.ToArray()));
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
            _observedAssignmentIds.Add(assignment.AssignmentId);
            _observedReplyPids.Add(request.ReplyPid ?? string.Empty);

            if (_dropAcks)
            {
                return;
            }

            if (context.Sender is null)
            {
                return;
            }

            if (_acceptBeforeReady)
            {
                context.Request(context.Sender, new PlacementAssignmentAck
                {
                    AssignmentId = assignment.AssignmentId,
                    BrainId = assignment.BrainId,
                    PlacementEpoch = assignment.PlacementEpoch,
                    State = PlacementAssignmentState.PlacementAssignmentAccepted,
                    Accepted = true,
                    Retryable = false,
                    FailureReason = PlacementFailureReason.PlacementFailureNone,
                    Message = "accepted"
                });
            }

            var ack = new PlacementAssignmentAck
            {
                AssignmentId = assignment.AssignmentId,
                BrainId = assignment.BrainId,
                PlacementEpoch = assignment.PlacementEpoch,
                State = _failAfterAccepted
                    ? PlacementAssignmentState.PlacementAssignmentFailed
                    : PlacementAssignmentState.PlacementAssignmentReady,
                Accepted = !_failAfterAccepted,
                Retryable = false,
                FailureReason = _failAfterAccepted
                    ? PlacementFailureReason.PlacementFailureInternalError
                    : PlacementFailureReason.PlacementFailureNone,
                Message = _failAfterAccepted ? "failed_after_accepted" : "ready"
            };

            if (_autoRespondAssignments)
            {
                context.Request(context.Sender, ack);
            }
            else
            {
                _deferredAssignmentAcks.Add((context.Sender, ack));
            }
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

            if (context.Sender is null)
            {
                return;
            }

            context.Request(context.Sender, report);
        }

        private void FlushNextDeferredAssignmentAck(IContext context)
        {
            if (_deferredAssignmentAcks.Count == 0)
            {
                context.Respond(new ReleaseNextDeferredSpawnAssignmentAckResult(false));
                return;
            }

            var pending = _deferredAssignmentAcks[0];
            _deferredAssignmentAcks.RemoveAt(0);
            context.Request(pending.Sender, pending.Ack.Clone());
            context.Respond(new ReleaseNextDeferredSpawnAssignmentAckResult(true));
        }
    }

    private sealed class IoRegisterProbeActor : IActor
    {
        private readonly TaskCompletionSource<ProtoIo.RegisterBrain> _tcs;
        private readonly Func<ProtoIo.RegisterBrain, bool> _predicate;

        public IoRegisterProbeActor(
            TaskCompletionSource<ProtoIo.RegisterBrain> tcs,
            Func<ProtoIo.RegisterBrain, bool> predicate)
        {
            _tcs = tcs;
            _predicate = predicate;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is ProtoIo.RegisterBrain register && _predicate(register))
            {
                _tcs.TrySetResult(register);
            }

            return Task.CompletedTask;
        }
    }
}
