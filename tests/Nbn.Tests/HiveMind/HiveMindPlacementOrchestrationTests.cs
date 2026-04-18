using System.Collections;
using System.Diagnostics;
using System.Reflection;
using Microsoft.Data.Sqlite;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.HiveMind;
using Nbn.Shared.Format;
using Nbn.Tests.Format;
using Nbn.Shared;
using Nbn.Tests.TestSupport;
using Proto;
using ProtoIo = Nbn.Proto.Io;
using ProtoSettings = Nbn.Proto.Settings;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Tests.HiveMind;

[Collection("HiveMindSerial")]
public sealed class HiveMindPlacementOrchestrationTests
{
    [Fact]
    public async Task RequestPlacement_Dispatches_Assignments_And_Completes_Reconcile()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(workerId, dropAcks: false, failFirstRetryable: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

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
                       && status.ReconcileState == PlacementReconcileState.PlacementReconcileMatched;
            },
            timeoutMs: 4_000);

        Assert.Equal(6, workerProbe.AssignmentRequestCount);
        Assert.True(workerProbe.ReconcileRequestCount >= 1);
        var brainTag = brainId.ToString("D");
        Assert.Equal(1, metrics.SumLong("nbn.hivemind.placement.request.accepted", "brain_id", brainTag));
        Assert.Equal(6, metrics.SumLong("nbn.hivemind.placement.assignment.dispatch", "brain_id", brainTag));
        Assert.Equal(6, metrics.SumLong("nbn.hivemind.placement.assignment.ack", "brain_id", brainTag));
        Assert.True(metrics.CountDouble("nbn.hivemind.placement.assignment.ack_latency.ms", "brain_id", brainTag) >= 6);
        Assert.Equal(1, metrics.SumLong("nbn.hivemind.placement.reconcile.matched", "brain_id", brainTag));

        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.requested") >= 1);
        Assert.True(debugSnapshot.Count("placement.reconcile.matched") >= 1);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task UnregisterBrain_Dispatches_Unassignments_And_CleansWorkerAssignments()
    {
        var system = new ActorSystem();
        var root = system.Root;

        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(workerId, dropAcks: false, failFirstRetryable: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000))));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var placementAck = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(placementAck.Accepted);

        await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 4_000);

        root.Send(hiveMind, new UnregisterBrain
        {
            BrainId = brainId.ToProtoUuid()
        });

        await WaitForAsync(
            () => Task.FromResult(workerProbe.UnassignmentRequestCount >= 6 && workerProbe.ActiveAssignmentCount == 0),
            timeoutMs: 4_000);

        var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        Assert.Equal(PlacementLifecycleState.PlacementLifecycleUnknown, finalStatus.LifecycleState);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RequestPlacement_Replacement_Dispatches_Unassignments_For_PreviousEpoch()
    {
        var system = new ActorSystem();
        var root = system.Root;

        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(workerId, dropAcks: false, failFirstRetryable: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000))));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var firstAck = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(firstAck.Accepted);
        await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 4_000);

        var secondAck = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 3,
                OutputWidth = 2
            });
        Assert.True(secondAck.Accepted);
        Assert.True(secondAck.PlacementEpoch > firstAck.PlacementEpoch);

        await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 4_000);

        await WaitForAsync(
            () => Task.FromResult(workerProbe.CountUnassignmentsForEpoch(firstAck.PlacementEpoch) >= 6),
            timeoutMs: 4_000);

        Assert.True(workerProbe.AssignmentRequestCount >= 12);
        Assert.Equal(6, workerProbe.ActiveAssignmentCount);
        Assert.All(workerProbe.ActivePlacementEpochs, epoch => Assert.Equal(secondAck.PlacementEpoch, epoch));

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task Queued_Reschedule_Retries_After_MinTick_Window_Clears()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var actor = new HiveMindActor(CreateOptions(
            assignmentTimeoutMs: 1_000,
            retryBackoffMs: 10,
            maxRetries: 1,
            reconcileTimeoutMs: 500,
            rescheduleMinTicks: 2,
            rescheduleMinMinutes: 0,
            rescheduleQuietMs: 10));
        var hiveMind = root.Spawn(Props.FromProducer(() => actor));

        SetPrivateField(actor, "_rescheduleInProgress", true);
        SetPrivateField(actor, "_rescheduleQueued", true);
        SetPrivateField(actor, "_queuedRescheduleReason", "queued-timeout");
        SetPrivateField(actor, "_lastRescheduleTick", 5UL);
        SetPrivateField(actor, "_lastCompletedTickId", 5UL);
        SetPrivateField(actor, "_lastRescheduleAt", DateTime.UtcNow);

        root.Send(hiveMind, CreateHiveMindPrivateMessage("RescheduleCompleted", "initial", true));

        await Task.Delay(100);
        Assert.Equal<ulong>(5, GetPrivateField<ulong>(actor, "_lastRescheduleTick"));
        Assert.False((await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus())).RescheduleInProgress);

        SetPrivateField(actor, "_lastCompletedTickId", 7UL);

        await WaitForAsync(
            () => Task.FromResult(GetPrivateField<ulong>(actor, "_lastRescheduleTick") == 7UL),
            timeoutMs: 3_000);

        Assert.False(GetPrivateField<bool>(actor, "_rescheduleQueued"));
        Assert.Null(GetPrivateField<string?>(actor, "_queuedRescheduleReason"));

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task PendingReschedule_Waits_For_New_Shards_And_OutputSinkRefresh_Before_Completion()
    {
        var artifactRoot = TempDirectoryScope.Create("nbn-hivemind-reschedule", clearSqlitePools: true);
        ActorSystem? system = null;

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var richNbn = NbnTestVectors.CreateRichNbnVector();
            var baseManifest = await store.StoreAsync(new MemoryStream(richNbn.Bytes), "application/x-nbn");
            var baseRef = baseManifest.ArtifactId.Bytes.ToArray().ToArtifactRef(
                (ulong)baseManifest.ByteLength,
                "application/x-nbn",
                artifactRoot);

            system = new ActorSystem();
            var root = system.Root;

            var workerAId = Guid.NewGuid();
            var workerBId = Guid.NewGuid();
            var workerA = new PlacementWorkerProbe(workerAId, dropAcks: false, failFirstRetryable: false);
            var workerB = new PlacementWorkerProbe(
                workerBId,
                dropAcks: false,
                failFirstRetryable: false,
                autoRespondAssignments: false,
                autoRespondReconcile: false);
            var workerAPid = root.Spawn(Props.FromProducer(() => workerA));
            var workerBPid = root.Spawn(Props.FromProducer(() => workerB));
            var actor = new HiveMindActor(CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000,
                rescheduleMinMinutes: 0));
            var hiveMind = root.Spawn(Props.FromProducer(() => actor));

            PrimeWorkers(root, hiveMind, workerAPid, workerAId);

            var brainId = Guid.NewGuid();
            var firstAck = await root.RequestAsync<PlacementAck>(
                hiveMind,
                new RequestPlacement
                {
                    BrainId = brainId.ToProtoUuid(),
                    BaseDef = baseRef,
                    InputWidth = 3,
                    OutputWidth = 2
                });
            Assert.True(firstAck.Accepted);
            await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 4_000);

            var region0Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
                brainId,
                ShardId32.From(0, 0),
                neuronStart: 0,
                bufferCodes: new[] { 10, 11, 12 },
                enabledBitset: new byte[] { 0b0000_0111 },
                overlays: Array.Empty<SnapshotOverlayRecord>())));
            var region1Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
                brainId,
                ShardId32.From(1, 0),
                neuronStart: 0,
                bufferCodes: new[] { 20, 21, 22, 23 },
                enabledBitset: new byte[] { 0b0000_1111 },
                overlays: Array.Empty<SnapshotOverlayRecord>())));
            var region31Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
                brainId,
                ShardId32.From(31, 0),
                neuronStart: 0,
                bufferCodes: new[] { 30, 31 },
                enabledBitset: new byte[] { 0b0000_0011 },
                overlays: Array.Empty<SnapshotOverlayRecord>())));

            await root.RequestAsync<SendMessageAck>(workerAPid, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 0,
                ShardIndex = 0,
                ShardPid = PidLabel(region0Shard),
                NeuronStart = 0,
                NeuronCount = 3,
                PlacementEpoch = firstAck.PlacementEpoch,
                AssignmentId = ShardAssignmentId(firstAck, 0)
            }));
            await root.RequestAsync<SendMessageAck>(workerAPid, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 1,
                ShardIndex = 0,
                ShardPid = PidLabel(region1Shard),
                NeuronStart = 0,
                NeuronCount = 4,
                PlacementEpoch = firstAck.PlacementEpoch,
                AssignmentId = ShardAssignmentId(firstAck, 1)
            }));
            await root.RequestAsync<SendMessageAck>(workerAPid, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 31,
                ShardIndex = 0,
                ShardPid = PidLabel(region31Shard),
                NeuronStart = 0,
                NeuronCount = 2,
                PlacementEpoch = firstAck.PlacementEpoch,
                AssignmentId = ShardAssignmentId(firstAck, 31)
            }));
            await WaitForAsync(
                async () =>
                {
                    var status = await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus());
                    return status.RegisteredShards >= 3;
                },
                timeoutMs: 2_000);

            var outputSinkPid = root.Spawn(Props.FromProducer(static () => new NullActor()));
            root.Send(workerAPid, new RegisterOutputSinkForBrain(hiveMind, brainId, outputSinkPid));
            await WaitForAsync(
                () => Task.FromResult(GetOutputSinkPid(actor, brainId) is not null),
                timeoutMs: 2_000);

            PrimeWorkers(root, hiveMind, (workerAPid, workerAId, false, true), (workerBPid, workerBId, true, true));
            await WaitForAsync(
                async () =>
                {
                    var inventory = await root.RequestAsync<PlacementWorkerInventory>(
                        hiveMind,
                        new PlacementWorkerInventoryRequest());
                    return inventory.Workers.Count == 1
                           && inventory.Workers[0].WorkerNodeId.Value.SequenceEqual(workerBId.ToProtoUuid().Value);
                },
                timeoutMs: 2_000);

            var secondAck = await root.RequestAsync<PlacementAck>(
                hiveMind,
                new RequestPlacement
                {
                    BrainId = brainId.ToProtoUuid(),
                    BaseDef = baseRef,
                    InputWidth = 3,
                    OutputWidth = 2
                });
            Assert.True(secondAck.Accepted);

            var pending = CreateHiveMindPrivateObject("PendingRescheduleState", "replacement-check");
            var pendingBrains = pending.GetType().GetProperty("PendingBrains", BindingFlags.Instance | BindingFlags.Public);
            Assert.NotNull(pendingBrains);
            var pendingBrainMap = pendingBrains!.GetValue(pending);
            Assert.NotNull(pendingBrainMap);
            pendingBrainMap!.GetType().GetMethod("Add")!.Invoke(pendingBrainMap, new object?[] { brainId, secondAck.PlacementEpoch });
            SetPrivateField(actor, "_rescheduleInProgress", true);
            SetPrivateField(actor, "_pendingReschedule", pending);

            await WaitForAsync(
                async () =>
                {
                    if (workerA.UnassignmentRequestCount < 7)
                    {
                        return false;
                    }

                    var deferred = await root.RequestAsync<DeferredAssignmentAckSnapshot>(
                        workerBPid,
                        new GetDeferredAssignmentAckSnapshot());
                    return deferred.AssignmentIds.Count > 0;
                },
                timeoutMs: 5_000);

            await DrainDeferredAssignmentAcksUntilAsync(
                root,
                workerBPid,
                () => Task.FromResult(workerB.ReconcileRequestCount >= 1),
                timeoutMs: 5_000);
            root.Send(workerBPid, new ReleaseDeferredReconcileReports());

            await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 5_000);

            var pendingBeforeRegisters = GetPrivateField<object?>(actor, "_pendingReschedule");
            Assert.NotNull(pendingBeforeRegisters);
            Assert.Equal(0, workerB.OutputSinkUpdateCount);

            root.Send(workerBPid, new RegisterHostedShard(hiveMind, brainId, 0, 0, 0, 3, secondAck.PlacementEpoch, ShardAssignmentId(secondAck, 0)));
            root.Send(workerBPid, new RegisterHostedShard(hiveMind, brainId, 1, 0, 0, 4, secondAck.PlacementEpoch, ShardAssignmentId(secondAck, 1)));
            root.Send(workerBPid, new RegisterHostedShard(hiveMind, brainId, 31, 0, 0, 2, secondAck.PlacementEpoch, ShardAssignmentId(secondAck, 31)));

            await WaitForAsync(
                () => Task.FromResult(GetPendingRescheduleBrainCount(actor) == 0),
                timeoutMs: 5_000);
            await WaitForAsync(
                () => Task.FromResult(workerB.OutputSinkUpdateCount >= 1),
                timeoutMs: 5_000);

            var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
            AssertPlacementStable(finalStatus);
            Assert.True(workerB.OutputSinkUpdateCount >= 1);
            Assert.Equal(workerBPid.Id, GetRegisteredShardPid(actor, brainId, 31, 0).Id);

        }
        finally
        {
            if (system is not null)
            {
                await system.ShutdownAsync();
            }
            artifactRoot.Dispose();
        }
    }

    [Fact]
    public async Task Failed_Reschedule_Pauses_Affected_Brain_To_Unblock_Tick_Barriers()
    {
        var artifactRoot = TempDirectoryScope.Create("nbn-hivemind-reschedule-failure-pause", clearSqlitePools: true);
        ActorSystem? system = null;

        try
        {
            var baseRef = await StoreRichBaseDefinitionAsync(artifactRoot);
            system = new ActorSystem();
            var root = system.Root;
            var settingsProbePid = root.Spawn(Props.FromProducer(static () => new BrainStateProbeActor()));

            var workerId = Guid.NewGuid();
            var worker = new PlacementWorkerProbe(workerId, dropAcks: false, failFirstRetryable: false);
            var workerPid = root.Spawn(Props.FromProducer(() => worker));
            var actor = new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 250,
                    retryBackoffMs: 10,
                    maxRetries: 0,
                    reconcileTimeoutMs: 250,
                    rescheduleMinMinutes: 0,
                    rescheduleQuietMs: 10),
                settingsPid: settingsProbePid);
            var hiveMind = root.Spawn(Props.FromProducer(() => actor));

            PrimeWorkers(root, hiveMind, workerPid, workerId);

            var brainId = Guid.NewGuid();
            var placement = await root.RequestAsync<PlacementAck>(
                hiveMind,
                new RequestPlacement
                {
                    BrainId = brainId.ToProtoUuid(),
                    BaseDef = baseRef,
                    InputWidth = 3,
                    OutputWidth = 2
                });
            Assert.True(placement.Accepted);
            await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 5_000);
            await RegisterHostedShardsFromCurrentAssignmentsAsync(
                root,
                hiveMind,
                actor,
                brainId,
                new Dictionary<Guid, PID> { [workerId] = workerPid });
            await WaitForAsync(
                async () =>
                {
                    var status = await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus());
                    return status.RegisteredShards >= 3;
                },
                timeoutMs: 2_000);

            if (Directory.Exists(artifactRoot))
            {
                SqliteConnection.ClearAllPools();
                Directory.Delete(artifactRoot, recursive: true);
            }

            SetPrivateField(actor, "_rescheduleInProgress", true);
            SetPrivateField(actor, "_activeRescheduleAllBrains", false);
            GetPrivateField<HashSet<Guid>>(actor, "_activeRescheduleBrains").Add(brainId);
            root.Send(hiveMind, CreateHiveMindPrivateMessage("RescheduleNow", "test_reschedule_failure"));

            var pauseObserved = await WaitForBrainStateOrTimeoutAsync(root, settingsProbePid, brainId, "Paused", timeoutMs: 5_000);
            var brainStates = await GetBrainStateSnapshotAsync(root, settingsProbePid, brainId);
            Assert.True(
                pauseObserved,
                $"Expected failed reschedule to pause brain. Observed states: {string.Join(", ", brainStates.Events.Select(static entry => $"{entry.State}:{entry.Notes}"))}; pendingReschedule={GetPendingRescheduleBrainCount(actor)}");
            Assert.Contains(
                brainStates.Events,
                entry => string.Equals(entry.State, "Paused", StringComparison.Ordinal)
                         && entry.Notes.Contains("reschedule_failed", StringComparison.OrdinalIgnoreCase));
            var status = await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus());
            Assert.False(status.RescheduleInProgress);
            var lifecycle = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
            Assert.Equal(PlacementLifecycleState.PlacementLifecycleFailed, lifecycle.LifecycleState);
            Assert.Equal(PlacementReconcileState.PlacementReconcileFailed, lifecycle.ReconcileState);

            root.Send(workerPid, new RegisterHostedShard(hiveMind, brainId, 0, 0, 0, 3, lifecycle.PlacementEpoch, ShardAssignmentId(brainId, lifecycle.PlacementEpoch, 0)));
            root.Send(workerPid, new RegisterHostedShard(hiveMind, brainId, 1, 0, 0, 4, lifecycle.PlacementEpoch, ShardAssignmentId(brainId, lifecycle.PlacementEpoch, 1)));
            root.Send(workerPid, new RegisterHostedShard(hiveMind, brainId, 31, 0, 0, 2, lifecycle.PlacementEpoch, ShardAssignmentId(brainId, lifecycle.PlacementEpoch, 31)));
            root.Send(hiveMind, new Nbn.Shared.HiveMind.ResumeBrainRequest(brainId));
            await Task.Delay(100);

            var afterLateShard = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
            Assert.Equal(PlacementLifecycleState.PlacementLifecycleFailed, afterLateShard.LifecycleState);
            Assert.Equal(PlacementReconcileState.PlacementReconcileFailed, afterLateShard.ReconcileState);
        }
        finally
        {
            if (system is not null)
            {
                await system.ShutdownAsync();
            }
            artifactRoot.Dispose();
        }
    }

    [Fact]
    public async Task Stale_PendingReschedule_Epoch_Does_Not_Pause_Newer_Placement()
    {
        var artifactRoot = TempDirectoryScope.Create("nbn-hivemind-stale-reschedule", clearSqlitePools: true);
        ActorSystem? system = null;

        try
        {
            var baseRef = await StoreRichBaseDefinitionAsync(artifactRoot);
            system = new ActorSystem();
            var root = system.Root;
            var settingsProbePid = root.Spawn(Props.FromProducer(static () => new BrainStateProbeActor()));

            var workerId = Guid.NewGuid();
            var worker = new PlacementWorkerProbe(workerId, dropAcks: false, failFirstRetryable: false);
            var workerPid = root.Spawn(Props.FromProducer(() => worker));
            var actor = new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 250,
                    retryBackoffMs: 10,
                    maxRetries: 0,
                    reconcileTimeoutMs: 250,
                    rescheduleMinMinutes: 0,
                    rescheduleQuietMs: 10),
                settingsPid: settingsProbePid);
            var hiveMind = root.Spawn(Props.FromProducer(() => actor));

            PrimeWorkers(root, hiveMind, workerPid, workerId);

            var brainId = Guid.NewGuid();
            var firstAck = await root.RequestAsync<PlacementAck>(
                hiveMind,
                new RequestPlacement
                {
                    BrainId = brainId.ToProtoUuid(),
                    BaseDef = baseRef,
                    InputWidth = 3,
                    OutputWidth = 2
                });
            Assert.True(firstAck.Accepted);
            await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 5_000);

            var stalePending = CreateHiveMindPrivateObject("PendingRescheduleState", "stale-reschedule");
            var pendingBrains = stalePending.GetType().GetProperty("PendingBrains", BindingFlags.Instance | BindingFlags.Public);
            Assert.NotNull(pendingBrains);
            var pendingBrainMap = pendingBrains!.GetValue(stalePending);
            Assert.NotNull(pendingBrainMap);
            pendingBrainMap!.GetType().GetMethod("Add")!.Invoke(pendingBrainMap, new object?[] { brainId, firstAck.PlacementEpoch });
            SetPrivateField(actor, "_rescheduleInProgress", true);
            SetPrivateField(actor, "_pendingReschedule", stalePending);

            var secondAck = await root.RequestAsync<PlacementAck>(
                hiveMind,
                new RequestPlacement
                {
                    BrainId = brainId.ToProtoUuid(),
                    BaseDef = baseRef,
                    InputWidth = 3,
                    OutputWidth = 2
                });
            Assert.True(secondAck.Accepted);
            Assert.True(secondAck.PlacementEpoch > firstAck.PlacementEpoch);

            await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 5_000);
            await WaitForAsync(
                () => Task.FromResult(GetPendingRescheduleBrainCount(actor) == 0),
                timeoutMs: 5_000);

            var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
            AssertPlacementStable(finalStatus);
            var states = await GetBrainStateSnapshotAsync(root, settingsProbePid, brainId);
            Assert.DoesNotContain(states.Events, entry => string.Equals(entry.State, "Paused", StringComparison.Ordinal));
        }
        finally
        {
            if (system is not null)
            {
                await system.ShutdownAsync();
            }
            artifactRoot.Dispose();
        }
    }

    [Fact]
    public async Task Retryable_AssignmentAck_Is_Retried_Then_Completes_Reconcile()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(workerId, dropAcks: false, failFirstRetryable: true);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 1_000,
                retryBackoffMs: 10,
                maxRetries: 2,
                reconcileTimeoutMs: 1_000),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

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
                       && status.ReconcileState == PlacementReconcileState.PlacementReconcileMatched;
            },
            timeoutMs: 5_000);

        Assert.True(workerProbe.RetryDispatchCount >= 1);
        var brainTag = brainId.ToString("D");
        Assert.True(metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag) >= 1);
        Assert.True(metrics.SumLong("nbn.hivemind.placement.assignment.ack", "brain_id", brainTag) >= 7);

        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.assignment.retry") >= 1);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task Missing_AssignmentAck_Exhausts_Retry_And_Fails_Placement()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(workerId, dropAcks: true, failFirstRetryable: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 100,
                retryBackoffMs: 10,
                maxRetries: 1,
                reconcileTimeoutMs: 500),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

        await WaitForAsync(
            async () =>
            {
                var status = await root.RequestAsync<PlacementLifecycleInfo>(
                    hiveMind,
                    new GetPlacementLifecycle
                    {
                        BrainId = brainId.ToProtoUuid()
                    });

                return status.LifecycleState == PlacementLifecycleState.PlacementLifecycleFailed
                       && status.FailureReason == PlacementFailureReason.PlacementFailureAssignmentTimeout;
            },
            timeoutMs: 5_000);

        Assert.True(workerProbe.AssignmentRequestCount >= 2);
        var brainTag = brainId.ToString("D");
        Assert.True(metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag) >= 1);
        Assert.True(metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag) >= 1);
        Assert.Equal(0, metrics.SumLong("nbn.hivemind.placement.reconcile.matched", "brain_id", brainTag));

        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.assignment.timeout") >= 1);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task NonRetryable_AssignmentAck_Fails_Placement_Without_Retry()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(
            workerId,
            dropAcks: false,
            failFirstRetryable: false,
            failFirstNonRetryable: true);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 1_000,
                retryBackoffMs: 10,
                maxRetries: 2,
                reconcileTimeoutMs: 1_000),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

        await WaitForAsync(
            async () =>
            {
                var status = await root.RequestAsync<PlacementLifecycleInfo>(
                    hiveMind,
                    new GetPlacementLifecycle
                    {
                        BrainId = brainId.ToProtoUuid()
                    });

                return status.LifecycleState == PlacementLifecycleState.PlacementLifecycleFailed;
            },
            timeoutMs: 5_000);

        var finalStatus = await root.RequestAsync<PlacementLifecycleInfo>(
            hiveMind,
            new GetPlacementLifecycle
            {
                BrainId = brainId.ToProtoUuid()
            });
        Assert.Contains(
            finalStatus.FailureReason,
            new[]
            {
                PlacementFailureReason.PlacementFailureWorkerUnavailable,
                PlacementFailureReason.PlacementFailureAssignmentRejected
            });

        Assert.Equal(0, workerProbe.RetryDispatchCount);
        var brainTag = brainId.ToString("D");
        Assert.Equal(0, metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag));
        Assert.Equal(0, metrics.SumLong("nbn.hivemind.placement.reconcile.matched", "brain_id", brainTag));
        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.assignment.failed") >= 1);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ReconcileMismatch_Fails_Placement_With_Mismatch_Observability()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(
            workerId,
            dropAcks: false,
            failFirstRetryable: false,
            reconcileBehavior: ReconcileBehavior.Mismatch);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 1_000,
                retryBackoffMs: 10,
                maxRetries: 1,
                reconcileTimeoutMs: 500),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

        await WaitForAsync(
            async () =>
            {
                var status = await root.RequestAsync<PlacementLifecycleInfo>(
                    hiveMind,
                    new GetPlacementLifecycle
                    {
                        BrainId = brainId.ToProtoUuid()
                    });

                return status.LifecycleState == PlacementLifecycleState.PlacementLifecycleFailed
                       && status.FailureReason == PlacementFailureReason.PlacementFailureReconcileMismatch;
            },
            timeoutMs: 5_000);

        var brainTag = brainId.ToString("D");
        Assert.True(metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag) >= 1);
        Assert.Equal(0, metrics.SumLong("nbn.hivemind.placement.reconcile.matched", "brain_id", brainTag));
        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.reconcile.mismatch") >= 1);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ReconcileTimeout_Fails_Placement_With_Timeout_Observability()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(
            workerId,
            dropAcks: false,
            failFirstRetryable: false,
            reconcileBehavior: ReconcileBehavior.Drop);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 1_000,
                retryBackoffMs: 10,
                maxRetries: 1,
                reconcileTimeoutMs: 100),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

        await WaitForAsync(
            async () =>
            {
                var status = await root.RequestAsync<PlacementLifecycleInfo>(
                    hiveMind,
                    new GetPlacementLifecycle
                    {
                        BrainId = brainId.ToProtoUuid()
                    });

                return status.LifecycleState == PlacementLifecycleState.PlacementLifecycleFailed
                       && status.FailureReason == PlacementFailureReason.PlacementFailureReconcileMismatch;
            },
            timeoutMs: 5_000);

        var brainTag = brainId.ToString("D");
        Assert.True(metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag) >= 1);
        Assert.True(metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag) >= 1);
        Assert.True(
            metrics.SumLong(
                "nbn.hivemind.placement.reconcile.failed",
                ("brain_id", brainTag),
                ("placement_epoch", ack.PlacementEpoch.ToString()),
                ("target", "reconcile"),
                ("failure_reason", "reconcile_timeout")) >= 1);
        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.reconcile.timeout") >= 1);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task Completed_Placement_Ignores_Late_Failed_AssignmentAck_For_Same_Epoch()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(workerId, dropAcks: false, failFirstRetryable: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

        await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 4_000);

        var baselineStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        var brainTag = brainId.ToString("D");
        var baselineAck = metrics.SumLong("nbn.hivemind.placement.assignment.ack", "brain_id", brainTag);
        var baselineRetry = metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag);
        var baselineReconcileMatched = metrics.SumLong("nbn.hivemind.placement.reconcile.matched", "brain_id", brainTag);
        var baselineReconcileFailed = metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag);
        var baselineDebug = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var ignoredBefore = baselineDebug.Count("placement.assignment_ack.ignored");

        root.Send(hiveMind, new PlacementAssignmentAck
        {
            AssignmentId = "late-failed-assignment",
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = baselineStatus.PlacementEpoch,
            State = PlacementAssignmentState.PlacementAssignmentFailed,
            Accepted = false,
            Retryable = false,
            FailureReason = PlacementFailureReason.PlacementFailureWorkerUnavailable,
            Message = "late failed ack"
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
                return snapshot.Count("placement.assignment_ack.ignored") > ignoredBefore;
            },
            timeoutMs: 4_000);

        var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        AssertPlacementStable(finalStatus);
        Assert.Equal(baselineStatus.PlacementEpoch, finalStatus.PlacementEpoch);

        Assert.Equal(baselineAck, metrics.SumLong("nbn.hivemind.placement.assignment.ack", "brain_id", brainTag));
        Assert.Equal(baselineRetry, metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag));
        Assert.Equal(baselineReconcileMatched, metrics.SumLong("nbn.hivemind.placement.reconcile.matched", "brain_id", brainTag));
        Assert.Equal(baselineReconcileFailed, metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag));

        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.assignment_ack.ignored") > ignoredBefore);
        Assert.Equal(0, debugSnapshot.Count("placement.assignment.failed"));

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task Completed_Placement_Ignores_Late_Failed_ReconcileReport_For_Same_Epoch()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(workerId, dropAcks: false, failFirstRetryable: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

        await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 4_000);

        var baselineStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        var brainTag = brainId.ToString("D");
        var baselineReconcileMatched = metrics.SumLong("nbn.hivemind.placement.reconcile.matched", "brain_id", brainTag);
        var baselineReconcileFailed = metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag);
        var baselineTimeout = metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag);
        var baselineIgnored = metrics.SumLong("nbn.hivemind.placement.reconcile.ignored", "brain_id", brainTag);
        var baselineDebug = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var ignoredBefore = baselineDebug.Count("placement.reconcile.ignored");

        root.Send(hiveMind, new PlacementReconcileReport
        {
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = baselineStatus.PlacementEpoch,
            ReconcileState = PlacementReconcileState.PlacementReconcileFailed,
            FailureReason = PlacementFailureReason.PlacementFailureReconcileMismatch,
            Message = "late failed reconcile"
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
                return snapshot.Count("placement.reconcile.ignored") > ignoredBefore;
            },
            timeoutMs: 4_000);

        var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        AssertPlacementStable(finalStatus);
        Assert.Equal(baselineStatus.PlacementEpoch, finalStatus.PlacementEpoch);

        Assert.Equal(baselineReconcileMatched, metrics.SumLong("nbn.hivemind.placement.reconcile.matched", "brain_id", brainTag));
        Assert.Equal(baselineReconcileFailed, metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag));
        Assert.Equal(baselineTimeout, metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag));
        Assert.Equal(
            baselineIgnored + 1,
            metrics.SumLong(
                "nbn.hivemind.placement.reconcile.ignored",
                ("brain_id", brainTag),
                ("placement_epoch", baselineStatus.PlacementEpoch.ToString()),
                ("target", "reconcile"),
                ("failure_reason", "execution_completed")));

        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.reconcile.ignored") > ignoredBefore);
        Assert.Equal(0, debugSnapshot.Count("placement.reconcile.mismatch"));

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task Stale_Epoch_AssignmentAck_And_ReconcileReport_Are_Ignored_Without_Lifecycle_Regression()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(workerId, dropAcks: false, failFirstRetryable: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

        await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 4_000);

        var baselineStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        var staleEpoch = baselineStatus.PlacementEpoch > 0 ? baselineStatus.PlacementEpoch - 1UL : baselineStatus.PlacementEpoch + 1UL;
        var brainTag = brainId.ToString("D");
        var baselineAck = metrics.SumLong("nbn.hivemind.placement.assignment.ack", "brain_id", brainTag);
        var baselineReconcileMatched = metrics.SumLong("nbn.hivemind.placement.reconcile.matched", "brain_id", brainTag);
        var baselineReconcileFailed = metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag);
        var baselineTimeout = metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag);
        var baselineIgnoredMetric = metrics.SumLong("nbn.hivemind.placement.reconcile.ignored", "brain_id", brainTag);
        var baselineDebug = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var assignmentIgnoredBefore = baselineDebug.Count("placement.assignment_ack.ignored");
        var reconcileIgnoredBefore = baselineDebug.Count("placement.reconcile.ignored");

        root.Send(hiveMind, new PlacementAssignmentAck
        {
            AssignmentId = "stale-failed-assignment",
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = staleEpoch,
            State = PlacementAssignmentState.PlacementAssignmentFailed,
            Accepted = false,
            Retryable = false,
            FailureReason = PlacementFailureReason.PlacementFailureWorkerUnavailable,
            Message = "stale failed ack"
        });
        root.Send(hiveMind, new PlacementReconcileReport
        {
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = staleEpoch,
            ReconcileState = PlacementReconcileState.PlacementReconcileFailed,
            FailureReason = PlacementFailureReason.PlacementFailureReconcileMismatch,
            Message = "stale failed reconcile"
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
                return snapshot.Count("placement.assignment_ack.ignored") > assignmentIgnoredBefore
                       && snapshot.Count("placement.reconcile.ignored") > reconcileIgnoredBefore;
            },
            timeoutMs: 4_000);

        var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        AssertPlacementStable(finalStatus);
        Assert.Equal(baselineStatus.PlacementEpoch, finalStatus.PlacementEpoch);

        Assert.Equal(baselineAck, metrics.SumLong("nbn.hivemind.placement.assignment.ack", "brain_id", brainTag));
        Assert.Equal(baselineReconcileMatched, metrics.SumLong("nbn.hivemind.placement.reconcile.matched", "brain_id", brainTag));
        Assert.Equal(baselineReconcileFailed, metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag));
        Assert.Equal(baselineTimeout, metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag));
        Assert.Equal(
            baselineIgnoredMetric + 1,
            metrics.SumLong(
                "nbn.hivemind.placement.reconcile.ignored",
                ("brain_id", brainTag),
                ("placement_epoch", staleEpoch.ToString()),
                ("target", "reconcile"),
                ("failure_reason", "placement_epoch_mismatch")));

        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.assignment_ack.ignored") > assignmentIgnoredBefore);
        Assert.True(debugSnapshot.Count("placement.reconcile.ignored") > reconcileIgnoredBefore);
        Assert.Equal(0, debugSnapshot.Count("placement.assignment.failed"));
        Assert.Equal(0, debugSnapshot.Count("placement.reconcile.mismatch"));

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task InFlight_Placement_Ignores_Unknown_And_Missing_AssignmentAck_And_Completes_Reconcile()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(
            workerId,
            dropAcks: false,
            failFirstRetryable: false,
            autoRespondAssignments: false,
            autoRespondReconcile: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

        await WaitForAsync(
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return workerProbe.AssignmentRequestCount > 0
                       && status.PlacementEpoch == ack.PlacementEpoch
                       && status.LifecycleState == PlacementLifecycleState.PlacementLifecycleAssigning;
            },
            timeoutMs: 4_000);

        var brainTag = brainId.ToString("D");
        var baselineRetry = metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag);
        var baselineAssignmentTimeout = metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag);
        var baselineReconcileFailed = metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag);
        var baselineReconcileTimeout = metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag);
        var baselineDebug = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var ignoredBefore = baselineDebug.Count("placement.assignment_ack.ignored");

        root.Send(hiveMind, new PlacementAssignmentAck
        {
            AssignmentId = "unknown-inflight-assignment",
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = ack.PlacementEpoch,
            State = PlacementAssignmentState.PlacementAssignmentFailed,
            Accepted = false,
            Retryable = false,
            FailureReason = PlacementFailureReason.PlacementFailureWorkerUnavailable,
            Message = "unknown assignment id while assigning"
        });
        root.Send(hiveMind, new PlacementAssignmentAck
        {
            AssignmentId = string.Empty,
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = ack.PlacementEpoch,
            State = PlacementAssignmentState.PlacementAssignmentFailed,
            Accepted = false,
            Retryable = false,
            FailureReason = PlacementFailureReason.PlacementFailureWorkerUnavailable,
            Message = "missing assignment id while assigning"
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
                return snapshot.Count("placement.assignment_ack.ignored") >= ignoredBefore + 2;
            },
            timeoutMs: 4_000);

        var inFlightStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        Assert.Equal(PlacementLifecycleState.PlacementLifecycleAssigning, inFlightStatus.LifecycleState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, inFlightStatus.FailureReason);

        await DrainDeferredAssignmentAcksUntilAsync(
            root,
            workerPid,
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return status.LifecycleState == PlacementLifecycleState.PlacementLifecycleReconciling;
            },
            timeoutMs: 4_000);

        root.Send(workerPid, new ReleaseDeferredReconcileReports());

        await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 4_000);

        var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        AssertPlacementStable(finalStatus);
        Assert.Equal(ack.PlacementEpoch, finalStatus.PlacementEpoch);
        Assert.True(workerProbe.ReconcileRequestCount >= 1);

        Assert.Equal(baselineRetry, metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag));
        Assert.Equal(baselineAssignmentTimeout, metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag));
        Assert.Equal(baselineReconcileFailed, metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag));
        Assert.Equal(baselineReconcileTimeout, metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag));

        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.assignment_ack.ignored") >= ignoredBefore + 2);
        Assert.Equal(0, debugSnapshot.Count("placement.assignment.failed"));

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task InFlight_Placement_WithRegisteredShard_DoesNotEnterTickCompute()
    {
        var system = new ActorSystem();
        var root = system.Root;

        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(
            workerId,
            dropAcks: false,
            failFirstRetryable: false,
            autoRespondAssignments: false,
            autoRespondReconcile: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000))));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var placementAck = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(placementAck.Accepted);

        await WaitForAsync(
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return workerProbe.AssignmentRequestCount > 0
                       && status.PlacementEpoch == placementAck.PlacementEpoch
                       && status.LifecycleState == PlacementLifecycleState.PlacementLifecycleAssigning;
            },
            timeoutMs: 4_000);

        var controller = root.Spawn(Props.FromProducer(static () => new TickCountingControllerActor()));
        root.Send(controller, new ForwardControlPlaneMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(controller),
            SignalRouterPid = PidLabel(controller)
        }));
        root.Send(workerPid, new RegisterHostedShard(hiveMind, brainId, 0, 0, 0, 1, placementAck.PlacementEpoch, ShardAssignmentId(placementAck, 0)));

        await WaitForAsync(
            async () =>
            {
                var lifecycle = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                var status = await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus());
                return lifecycle.PlacementEpoch == placementAck.PlacementEpoch
                       && lifecycle.LifecycleState is PlacementLifecycleState.PlacementLifecycleAssigned
                           or PlacementLifecycleState.PlacementLifecycleAssigning
                       && status.RegisteredShards >= 1;
            },
            timeoutMs: 2_000);

        root.Send(hiveMind, new Nbn.Shared.HiveMind.StartTickLoop());

        await WaitForAsync(
            async () =>
            {
                var status = await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus());
                return status.LastCompletedTickId >= 1 && status.PendingCompute == 0 && status.PendingDeliver == 0;
            },
            timeoutMs: 1_000);
        var tickStatus = await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus());
        var controllerSnapshot = await root.RequestAsync<TickCountingControllerSnapshot>(
            controller,
            new GetTickCountingControllerSnapshot());

        root.Send(hiveMind, new Nbn.Shared.HiveMind.StopTickLoop());

        Assert.True(tickStatus.LastCompletedTickId >= 1);
        Assert.Equal(0, controllerSnapshot.TickComputeCount);
        var inFlightStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        Assert.NotEqual(PlacementLifecycleState.PlacementLifecycleFailed, inFlightStatus.LifecycleState);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task InFlight_Assigning_Ignores_Duplicate_Failed_Ack_For_Ready_Assignment_And_Completes_Reconcile()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(
            workerId,
            dropAcks: false,
            failFirstRetryable: false,
            autoRespondAssignments: false,
            autoRespondReconcile: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

        await WaitForAsync(
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return workerProbe.AssignmentRequestCount > 0
                       && status.PlacementEpoch == ack.PlacementEpoch
                       && status.LifecycleState == PlacementLifecycleState.PlacementLifecycleAssigning;
            },
            timeoutMs: 4_000);

        var brainTag = brainId.ToString("D");
        var baselineRetry = metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag);
        var baselineAssignmentTimeout = metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag);
        var baselineReconcileFailed = metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag);
        var baselineReconcileTimeout = metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag);
        var baselineAck = metrics.SumLong("nbn.hivemind.placement.assignment.ack", "brain_id", brainTag);
        var baselineDebug = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var ignoredBefore = baselineDebug.Count("placement.assignment_ack.ignored");

        var released = await root.RequestAsync<ReleaseNextDeferredAssignmentAckResult>(workerPid, new ReleaseNextDeferredAssignmentAck());
        Assert.True(released.Released);
        Assert.False(string.IsNullOrWhiteSpace(released.AssignmentId));

        await WaitForAsync(
            () => Task.FromResult(metrics.SumLong("nbn.hivemind.placement.assignment.ack", "brain_id", brainTag) >= baselineAck + 1),
            timeoutMs: 4_000);

        var preNoiseStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        Assert.Equal(PlacementLifecycleState.PlacementLifecycleAssigning, preNoiseStatus.LifecycleState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, preNoiseStatus.FailureReason);

        root.Send(hiveMind, new PlacementAssignmentAck
        {
            AssignmentId = released.AssignmentId,
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = ack.PlacementEpoch,
            State = PlacementAssignmentState.PlacementAssignmentFailed,
            Accepted = false,
            Retryable = false,
            FailureReason = PlacementFailureReason.PlacementFailureWorkerUnavailable,
            Message = "duplicate failed ack for already-ready assignment"
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
                return snapshot.Count("placement.assignment_ack.ignored") > ignoredBefore;
            },
            timeoutMs: 4_000);

        var assigningStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        Assert.Equal(PlacementLifecycleState.PlacementLifecycleAssigning, assigningStatus.LifecycleState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, assigningStatus.FailureReason);

        await DrainDeferredAssignmentAcksUntilAsync(
            root,
            workerPid,
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return status.LifecycleState == PlacementLifecycleState.PlacementLifecycleReconciling;
            },
            timeoutMs: 4_000);

        var reconcilingStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        Assert.Equal(PlacementLifecycleState.PlacementLifecycleReconciling, reconcilingStatus.LifecycleState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, reconcilingStatus.FailureReason);

        root.Send(workerPid, new ReleaseDeferredReconcileReports());

        await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 4_000);

        var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        AssertPlacementStable(finalStatus);
        Assert.Equal(ack.PlacementEpoch, finalStatus.PlacementEpoch);

        Assert.Equal(baselineRetry, metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag));
        Assert.Equal(baselineAssignmentTimeout, metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag));
        Assert.Equal(baselineReconcileFailed, metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag));
        Assert.Equal(baselineReconcileTimeout, metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag));

        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.assignment_ack.ignored") > ignoredBefore);
        Assert.Equal(0, debugSnapshot.Count("placement.assignment.failed"));

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task InFlight_Assigning_Ignores_Foreign_Failed_Ack_For_Tracked_Assignment_And_Completes_Reconcile()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(
            workerId,
            dropAcks: false,
            failFirstRetryable: false,
            autoRespondAssignments: false,
            autoRespondReconcile: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var foreignSenderPid = root.Spawn(Props.FromProducer(static () => new ForwardControlPlaneActor()));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

        await WaitForAsync(
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                if (workerProbe.AssignmentRequestCount == 0
                    || status.PlacementEpoch != ack.PlacementEpoch
                    || status.LifecycleState != PlacementLifecycleState.PlacementLifecycleAssigning)
                {
                    return false;
                }

                var pendingAcks = await root.RequestAsync<DeferredAssignmentAckSnapshot>(workerPid, new GetDeferredAssignmentAckSnapshot());
                return pendingAcks.AssignmentIds.Count > 0;
            },
            timeoutMs: 4_000);

        var trackedAssignmentId = (await root.RequestAsync<DeferredAssignmentAckSnapshot>(
            workerPid,
            new GetDeferredAssignmentAckSnapshot())).AssignmentIds.First();
        Assert.False(string.IsNullOrWhiteSpace(trackedAssignmentId));

        var brainTag = brainId.ToString("D");
        var baselineRetry = metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag);
        var baselineAssignmentTimeout = metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag);
        var baselineReconcileFailed = metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag);
        var baselineReconcileTimeout = metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag);
        var baselineDebug = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var ignoredBefore = baselineDebug.Count("placement.assignment_ack.ignored");

        root.Send(foreignSenderPid, new ForwardControlPlaneMessage(
            hiveMind,
            new PlacementAssignmentAck
            {
                AssignmentId = trackedAssignmentId,
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = ack.PlacementEpoch,
                State = PlacementAssignmentState.PlacementAssignmentFailed,
                Accepted = false,
                Retryable = false,
                FailureReason = PlacementFailureReason.PlacementFailureWorkerUnavailable,
                Message = "foreign failed ack for tracked assignment while assigning"
            }));

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
                return snapshot.Count("placement.assignment_ack.ignored") > ignoredBefore;
            },
            timeoutMs: 4_000);

        var assigningStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        Assert.Equal(PlacementLifecycleState.PlacementLifecycleAssigning, assigningStatus.LifecycleState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, assigningStatus.FailureReason);

        await DrainDeferredAssignmentAcksUntilAsync(
            root,
            workerPid,
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return status.LifecycleState == PlacementLifecycleState.PlacementLifecycleReconciling;
            },
            timeoutMs: 4_000);

        root.Send(workerPid, new ReleaseDeferredReconcileReports());

        await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 4_000);

        var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        AssertPlacementStable(finalStatus);
        Assert.Equal(ack.PlacementEpoch, finalStatus.PlacementEpoch);
        Assert.True(workerProbe.ReconcileRequestCount >= 1);

        Assert.Equal(baselineRetry, metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag));
        Assert.Equal(baselineAssignmentTimeout, metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag));
        Assert.Equal(baselineReconcileFailed, metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag));
        Assert.Equal(baselineReconcileTimeout, metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag));

        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.assignment_ack.ignored") > ignoredBefore);
        Assert.Equal(0, debugSnapshot.Count("placement.assignment.failed"));

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task InFlight_Assigning_Ignores_Foreign_Ready_Ack_For_Tracked_Assignments_And_Does_Not_Prematurely_Reconcile()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(
            workerId,
            dropAcks: false,
            failFirstRetryable: false,
            autoRespondAssignments: false,
            autoRespondReconcile: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var foreignSenderPid = root.Spawn(Props.FromProducer(static () => new ForwardControlPlaneActor()));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

        await WaitForAsync(
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                if (workerProbe.AssignmentRequestCount == 0
                    || status.PlacementEpoch != ack.PlacementEpoch
                    || status.LifecycleState != PlacementLifecycleState.PlacementLifecycleAssigning)
                {
                    return false;
                }

                var pendingAcks = await root.RequestAsync<DeferredAssignmentAckSnapshot>(workerPid, new GetDeferredAssignmentAckSnapshot());
                return pendingAcks.AssignmentIds.Count > 0;
            },
            timeoutMs: 4_000);

        var trackedAssignmentIds = (await root.RequestAsync<DeferredAssignmentAckSnapshot>(
            workerPid,
            new GetDeferredAssignmentAckSnapshot())).AssignmentIds.ToArray();
        Assert.NotEmpty(trackedAssignmentIds);

        var brainTag = brainId.ToString("D");
        var baselineRetry = metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag);
        var baselineAssignmentTimeout = metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag);
        var baselineReconcileFailed = metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag);
        var baselineReconcileTimeout = metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag);
        var baselineDebug = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var ignoredBefore = baselineDebug.Count("placement.assignment_ack.ignored");

        foreach (var assignmentId in trackedAssignmentIds)
        {
            root.Send(foreignSenderPid, new ForwardControlPlaneMessage(
                hiveMind,
                new PlacementAssignmentAck
                {
                    AssignmentId = assignmentId,
                    BrainId = brainId.ToProtoUuid(),
                    PlacementEpoch = ack.PlacementEpoch,
                    State = PlacementAssignmentState.PlacementAssignmentReady,
                    Accepted = true,
                    Retryable = false,
                    FailureReason = PlacementFailureReason.PlacementFailureNone,
                    Message = "foreign ready ack for tracked assignment while assigning"
                }));
        }

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
                return snapshot.Count("placement.assignment_ack.ignored") >= ignoredBefore + trackedAssignmentIds.Length;
            },
            timeoutMs: 4_000);

        var assigningStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        Assert.Equal(PlacementLifecycleState.PlacementLifecycleAssigning, assigningStatus.LifecycleState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, assigningStatus.FailureReason);
        Assert.Equal(0, workerProbe.ReconcileRequestCount);

        await DrainDeferredAssignmentAcksUntilAsync(
            root,
            workerPid,
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return status.LifecycleState == PlacementLifecycleState.PlacementLifecycleReconciling;
            },
            timeoutMs: 4_000);

        root.Send(workerPid, new ReleaseDeferredReconcileReports());

        await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 4_000);

        var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        AssertPlacementStable(finalStatus);
        Assert.Equal(ack.PlacementEpoch, finalStatus.PlacementEpoch);
        Assert.True(workerProbe.ReconcileRequestCount >= 1);

        Assert.Equal(baselineRetry, metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag));
        Assert.Equal(baselineAssignmentTimeout, metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag));
        Assert.Equal(baselineReconcileFailed, metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag));
        Assert.Equal(baselineReconcileTimeout, metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag));

        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.assignment_ack.ignored") >= ignoredBefore + trackedAssignmentIds.Length);
        Assert.Equal(0, debugSnapshot.Count("placement.assignment.failed"));

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task InFlight_Reconciling_Ignores_Foreign_Failed_Report_And_Completes_Matched()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(
            workerId,
            dropAcks: false,
            failFirstRetryable: false,
            autoRespondAssignments: false,
            autoRespondReconcile: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var foreignSenderPid = root.Spawn(Props.FromProducer(static () => new ForwardControlPlaneActor()));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

        await WaitForAsync(
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return workerProbe.AssignmentRequestCount > 0
                       && status.PlacementEpoch == ack.PlacementEpoch
                       && status.LifecycleState == PlacementLifecycleState.PlacementLifecycleAssigning;
            },
            timeoutMs: 4_000);

        var brainTag = brainId.ToString("D");
        var baselineRetry = metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag);
        var baselineAssignmentTimeout = metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag);
        var baselineReconcileFailed = metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag);
        var baselineReconcileTimeout = metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag);
        var baselineIgnoredMetric = metrics.SumLong("nbn.hivemind.placement.reconcile.ignored", "brain_id", brainTag);
        var baselineDebug = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var ignoredBefore = baselineDebug.Count("placement.reconcile.ignored");
        var responseMismatchBefore = baselineDebug.Count("placement.reconcile.response_mismatch");

        await DrainDeferredAssignmentAcksUntilAsync(
            root,
            workerPid,
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return status.LifecycleState == PlacementLifecycleState.PlacementLifecycleReconciling;
            },
            timeoutMs: 4_000);

        var preNoiseStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        Assert.Equal(PlacementLifecycleState.PlacementLifecycleReconciling, preNoiseStatus.LifecycleState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, preNoiseStatus.FailureReason);

        root.Send(foreignSenderPid, new ForwardControlPlaneMessage(
            hiveMind,
            new PlacementReconcileReport
            {
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = ack.PlacementEpoch,
                ReconcileState = PlacementReconcileState.PlacementReconcileFailed,
                FailureReason = PlacementFailureReason.PlacementFailureReconcileMismatch,
                Message = "foreign failed reconcile while reconciling"
            }));

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
                return snapshot.Count("placement.reconcile.ignored") > ignoredBefore;
            },
            timeoutMs: 4_000);

        var reconcilingStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        Assert.Equal(PlacementLifecycleState.PlacementLifecycleReconciling, reconcilingStatus.LifecycleState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, reconcilingStatus.FailureReason);

        root.Send(workerPid, new ReleaseDeferredReconcileReports());

        await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 4_000);

        var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        AssertPlacementStable(finalStatus);
        Assert.Equal(ack.PlacementEpoch, finalStatus.PlacementEpoch);
        Assert.True(workerProbe.ReconcileRequestCount >= 1);

        Assert.Equal(baselineRetry, metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag));
        Assert.Equal(baselineAssignmentTimeout, metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag));
        Assert.Equal(baselineReconcileFailed, metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag));
        Assert.Equal(baselineReconcileTimeout, metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag));
        Assert.Equal(
            baselineIgnoredMetric + 1,
            metrics.SumLong(
                "nbn.hivemind.placement.reconcile.ignored",
                ("brain_id", brainTag),
                ("placement_epoch", ack.PlacementEpoch.ToString()),
                ("target", "reconcile"),
                ("failure_reason", "sender_not_pending_worker")));

        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.reconcile.ignored") > ignoredBefore);
        Assert.Equal(0, debugSnapshot.Count("placement.reconcile.failed"));
        Assert.True(debugSnapshot.Count("placement.reconcile.response_mismatch") > responseMismatchBefore);
        Assert.True(debugSnapshot.CountMessageContains("placement.reconcile.response_mismatch", "sender_not_pending_worker") >= 1);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task InFlight_Reconciling_Ignores_Foreign_Failed_Report_Forging_Pending_Worker_And_Completes_Matched()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(
            workerId,
            dropAcks: false,
            failFirstRetryable: false,
            autoRespondAssignments: false,
            autoRespondReconcile: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var foreignSenderPid = root.Spawn(Props.FromProducer(static () => new ForwardControlPlaneActor()));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

        await WaitForAsync(
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return workerProbe.AssignmentRequestCount > 0
                       && status.PlacementEpoch == ack.PlacementEpoch
                       && status.LifecycleState == PlacementLifecycleState.PlacementLifecycleAssigning;
            },
            timeoutMs: 4_000);

        var brainTag = brainId.ToString("D");
        var baselineRetry = metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag);
        var baselineAssignmentTimeout = metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag);
        var baselineReconcileFailed = metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag);
        var baselineReconcileTimeout = metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag);
        var baselineIgnoredMetric = metrics.SumLong("nbn.hivemind.placement.reconcile.ignored", "brain_id", brainTag);
        var baselineDebug = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var ignoredBefore = baselineDebug.Count("placement.reconcile.ignored");
        var responseMismatchBefore = baselineDebug.Count("placement.reconcile.response_mismatch");

        await DrainDeferredAssignmentAcksUntilAsync(
            root,
            workerPid,
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return status.LifecycleState == PlacementLifecycleState.PlacementLifecycleReconciling;
            },
            timeoutMs: 4_000);

        var preNoiseStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        Assert.Equal(PlacementLifecycleState.PlacementLifecycleReconciling, preNoiseStatus.LifecycleState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, preNoiseStatus.FailureReason);

        root.Send(foreignSenderPid, new ForwardControlPlaneMessage(
            hiveMind,
            new PlacementReconcileReport
            {
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = ack.PlacementEpoch,
                ReconcileState = PlacementReconcileState.PlacementReconcileFailed,
                FailureReason = PlacementFailureReason.PlacementFailureReconcileMismatch,
                Message = "foreign failed reconcile while forging pending worker",
                Assignments =
                {
                    new PlacementObservedAssignment
                    {
                        AssignmentId = "forged-worker-attribution",
                        Target = PlacementAssignmentTarget.PlacementTargetBrainRoot,
                        WorkerNodeId = workerId.ToProtoUuid(),
                        ActorPid = foreignSenderPid.Id
                    }
                }
            }));

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
                return snapshot.Count("placement.reconcile.ignored") > ignoredBefore;
            },
            timeoutMs: 4_000);

        var reconcilingStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        Assert.Equal(PlacementLifecycleState.PlacementLifecycleReconciling, reconcilingStatus.LifecycleState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, reconcilingStatus.FailureReason);

        root.Send(workerPid, new ReleaseDeferredReconcileReports());

        await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 4_000);

        var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        AssertPlacementStable(finalStatus);
        Assert.Equal(ack.PlacementEpoch, finalStatus.PlacementEpoch);
        Assert.True(workerProbe.ReconcileRequestCount >= 1);

        Assert.Equal(baselineRetry, metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag));
        Assert.Equal(baselineAssignmentTimeout, metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag));
        Assert.Equal(baselineReconcileFailed, metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag));
        Assert.Equal(baselineReconcileTimeout, metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag));
        Assert.Equal(
            baselineIgnoredMetric + 1,
            metrics.SumLong(
                "nbn.hivemind.placement.reconcile.ignored",
                ("brain_id", brainTag),
                ("worker_node_id", workerId.ToString("D")),
                ("placement_epoch", ack.PlacementEpoch.ToString()),
                ("target", "brain_root"),
                ("failure_reason", "sender_not_pending_worker")));

        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.reconcile.ignored") > ignoredBefore);
        Assert.Equal(0, debugSnapshot.Count("placement.reconcile.failed"));
        Assert.Equal(0, debugSnapshot.Count("placement.reconcile.mismatch"));
        Assert.True(debugSnapshot.Count("placement.reconcile.response_mismatch") > responseMismatchBefore);
        Assert.True(debugSnapshot.CountMessageContains("placement.reconcile.response_mismatch", "sender_not_pending_worker") >= 1);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task InFlight_Reconciling_Ignores_Worker_Sender_When_Observed_Worker_Does_Not_Match_And_Completes_Matched()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(
            workerId,
            dropAcks: false,
            failFirstRetryable: false,
            autoRespondAssignments: false,
            autoRespondReconcile: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

        await WaitForAsync(
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return workerProbe.AssignmentRequestCount > 0
                       && status.PlacementEpoch == ack.PlacementEpoch
                       && status.LifecycleState == PlacementLifecycleState.PlacementLifecycleAssigning;
            },
            timeoutMs: 4_000);

        var brainTag = brainId.ToString("D");
        var baselineRetry = metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag);
        var baselineAssignmentTimeout = metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag);
        var baselineReconcileFailed = metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag);
        var baselineReconcileTimeout = metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag);
        var baselineIgnoredMetric = metrics.SumLong("nbn.hivemind.placement.reconcile.ignored", "brain_id", brainTag);
        var baselineDebug = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var ignoredBefore = baselineDebug.Count("placement.reconcile.ignored");
        var responseMismatchBefore = baselineDebug.Count("placement.reconcile.response_mismatch");

        await DrainDeferredAssignmentAcksUntilAsync(
            root,
            workerPid,
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return status.LifecycleState == PlacementLifecycleState.PlacementLifecycleReconciling;
            },
            timeoutMs: 4_000);

        var preNoiseStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        Assert.Equal(PlacementLifecycleState.PlacementLifecycleReconciling, preNoiseStatus.LifecycleState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, preNoiseStatus.FailureReason);

        root.Send(workerPid, new ForwardControlPlaneMessage(
            hiveMind,
            new PlacementReconcileReport
            {
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = ack.PlacementEpoch,
                ReconcileState = PlacementReconcileState.PlacementReconcileFailed,
                FailureReason = PlacementFailureReason.PlacementFailureReconcileMismatch,
                Message = "worker sender mismatched observed worker attribution",
                Assignments =
                {
                    new PlacementObservedAssignment
                    {
                        AssignmentId = "observed-worker-mismatch",
                        Target = PlacementAssignmentTarget.PlacementTargetBrainRoot,
                        WorkerNodeId = Guid.NewGuid().ToProtoUuid(),
                        ActorPid = workerPid.Id
                    }
                }
            }));

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
                return snapshot.Count("placement.reconcile.ignored") > ignoredBefore;
            },
            timeoutMs: 4_000);

        var reconcilingStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        Assert.Equal(PlacementLifecycleState.PlacementLifecycleReconciling, reconcilingStatus.LifecycleState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, reconcilingStatus.FailureReason);

        root.Send(workerPid, new ReleaseDeferredReconcileReports());

        await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 4_000);

        var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        AssertPlacementStable(finalStatus);
        Assert.Equal(ack.PlacementEpoch, finalStatus.PlacementEpoch);
        Assert.True(workerProbe.ReconcileRequestCount >= 1);

        Assert.Equal(baselineRetry, metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag));
        Assert.Equal(baselineAssignmentTimeout, metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag));
        Assert.Equal(baselineReconcileFailed, metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag));
        Assert.Equal(baselineReconcileTimeout, metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag));
        Assert.Equal(
            baselineIgnoredMetric + 1,
            metrics.SumLong(
                "nbn.hivemind.placement.reconcile.ignored",
                ("brain_id", brainTag),
                ("worker_node_id", workerId.ToString("D")),
                ("placement_epoch", ack.PlacementEpoch.ToString()),
                ("target", "brain_root"),
                ("failure_reason", "payload_worker_mismatch")));

        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.reconcile.ignored") > ignoredBefore);
        Assert.Equal(0, debugSnapshot.Count("placement.reconcile.failed"));
        Assert.Equal(0, debugSnapshot.Count("placement.reconcile.mismatch"));
        Assert.True(debugSnapshot.Count("placement.reconcile.response_mismatch") > responseMismatchBefore);
        Assert.True(debugSnapshot.CountMessageContains("placement.reconcile.response_mismatch", "payload_worker_mismatch") >= 1);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task InFlight_Placement_Ignores_Early_ReconcileReport_Before_Reconcile_Start_And_Completes()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var root = system.Root;

        var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));
        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(
            workerId,
            dropAcks: false,
            failFirstRetryable: false,
            autoRespondAssignments: false,
            autoRespondReconcile: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 2_000,
                retryBackoffMs: 50,
                maxRetries: 1,
                reconcileTimeoutMs: 2_000),
            debugHubPid: debugProbePid)));

        PrimeWorkers(root, hiveMind, workerPid, workerId);

        var brainId = Guid.NewGuid();
        var ack = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 3
            });
        Assert.True(ack.Accepted);

        await WaitForAsync(
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return workerProbe.AssignmentRequestCount > 0
                       && status.PlacementEpoch == ack.PlacementEpoch
                       && status.LifecycleState == PlacementLifecycleState.PlacementLifecycleAssigning;
            },
            timeoutMs: 4_000);

        var brainTag = brainId.ToString("D");
        var baselineRetry = metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag);
        var baselineAssignmentTimeout = metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag);
        var baselineReconcileFailed = metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag);
        var baselineReconcileTimeout = metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag);
        var baselineIgnoredMetric = metrics.SumLong("nbn.hivemind.placement.reconcile.ignored", "brain_id", brainTag);
        var baselineDebug = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        var ignoredBefore = baselineDebug.Count("placement.reconcile.ignored");
        var responseMismatchBefore = baselineDebug.Count("placement.reconcile.response_mismatch");

        root.Send(hiveMind, new PlacementReconcileReport
        {
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = ack.PlacementEpoch,
            ReconcileState = PlacementReconcileState.PlacementReconcileFailed,
            FailureReason = PlacementFailureReason.PlacementFailureReconcileMismatch,
            Message = "early failed reconcile"
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
                return snapshot.Count("placement.reconcile.ignored") > ignoredBefore;
            },
            timeoutMs: 4_000);

        var assigningStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        Assert.Equal(PlacementLifecycleState.PlacementLifecycleAssigning, assigningStatus.LifecycleState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, assigningStatus.FailureReason);

        await DrainDeferredAssignmentAcksUntilAsync(
            root,
            workerPid,
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return status.LifecycleState == PlacementLifecycleState.PlacementLifecycleReconciling;
            },
            timeoutMs: 4_000);

        root.Send(workerPid, new ReleaseDeferredReconcileReports());

        await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 4_000);

        var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
        AssertPlacementStable(finalStatus);
        Assert.Equal(ack.PlacementEpoch, finalStatus.PlacementEpoch);
        Assert.True(workerProbe.ReconcileRequestCount >= 1);

        Assert.Equal(baselineRetry, metrics.SumLong("nbn.hivemind.placement.assignment.retry", "brain_id", brainTag));
        Assert.Equal(baselineAssignmentTimeout, metrics.SumLong("nbn.hivemind.placement.assignment.timeout", "brain_id", brainTag));
        Assert.Equal(baselineReconcileFailed, metrics.SumLong("nbn.hivemind.placement.reconcile.failed", "brain_id", brainTag));
        Assert.Equal(baselineReconcileTimeout, metrics.SumLong("nbn.hivemind.placement.reconcile.timeout", "brain_id", brainTag));
        Assert.Equal(
            baselineIgnoredMetric + 1,
            metrics.SumLong(
                "nbn.hivemind.placement.reconcile.ignored",
                ("brain_id", brainTag),
                ("placement_epoch", ack.PlacementEpoch.ToString()),
                ("target", "reconcile"),
                ("failure_reason", "reconcile_not_requested")));

        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.reconcile.ignored") > ignoredBefore);
        Assert.Equal(0, debugSnapshot.Count("placement.reconcile.mismatch"));
        Assert.Equal(responseMismatchBefore, debugSnapshot.Count("placement.reconcile.response_mismatch"));

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task OfflineWorker_Triggers_ArtifactBacked_Recovery_And_Emits_Recovering_To_Active()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var artifactRoot = TempDirectoryScope.Create("nbn-hivemind-recovery-success", clearSqlitePools: true);
        ActorSystem? system = null;

        try
        {
            var baseRef = await StoreRichBaseDefinitionAsync(artifactRoot);
            system = new ActorSystem();
            var root = system.Root;
            var settingsProbePid = root.Spawn(Props.FromProducer(static () => new BrainStateProbeActor()));
            var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));

            var workerAId = Guid.NewGuid();
            var workerBId = Guid.NewGuid();
            var workerA = new PlacementWorkerProbe(workerAId, dropAcks: false, failFirstRetryable: false);
            var workerB = new PlacementWorkerProbe(workerBId, dropAcks: false, failFirstRetryable: false);
            var workerAPid = root.Spawn(Props.FromProducer(() => workerA));
            var workerBPid = root.Spawn(Props.FromProducer(() => workerB));
            var actor = new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 2_000,
                    retryBackoffMs: 50,
                    maxRetries: 1,
                    reconcileTimeoutMs: 2_000,
                    rescheduleMinMinutes: 0),
                debugHubPid: debugProbePid,
                settingsPid: settingsProbePid,
                debugStreamEnabled: true);
            var hiveMind = root.Spawn(Props.FromProducer(() => actor));

            PrimeWorkers(root, hiveMind, (workerAPid, workerAId, true, true), (workerBPid, workerBId, true, true));

            var brainId = Guid.NewGuid();
            var placement = await root.RequestAsync<PlacementAck>(
                hiveMind,
                new RequestPlacement
                {
                    BrainId = brainId.ToProtoUuid(),
                    BaseDef = baseRef,
                    InputWidth = 3,
                    OutputWidth = 2,
                    ShardPlan = new ShardPlan
                    {
                        Mode = ShardPlanMode.ShardPlanFixed,
                        ShardCount = 2
                    }
                });
            Assert.True(placement.Accepted);
            await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 5_000);

            var initialShardProbes = await RegisterSnapshotShardsFromCurrentAssignmentsAsync(root, hiveMind, actor, brainId);
            var snapshotReady = await root.RequestAsync<ProtoIo.SnapshotReady>(
                hiveMind,
                new ProtoIo.RequestSnapshot
                {
                    BrainId = brainId.ToProtoUuid()
                });
            Assert.NotNull(snapshotReady.Snapshot);
            Assert.True(snapshotReady.Snapshot.TryToSha256Bytes(out _));

            var failedWorkerId = initialShardProbes
                .GroupBy(static probe => probe.WorkerId)
                .OrderByDescending(static group => group.Count())
                .First()
                .Key;
            var workerPidById = new Dictionary<Guid, PID>
            {
                [workerAId] = workerAPid,
                [workerBId] = workerBPid
            };

            root.Stop(workerPidById[failedWorkerId]);
            foreach (var shardProbe in initialShardProbes.Where(probe => probe.WorkerId == failedWorkerId))
            {
                root.Stop(shardProbe.Pid);
            }

            PrimeWorkers(
                root,
                hiveMind,
                (workerAPid, workerAId, true, workerAId != failedWorkerId),
                (workerBPid, workerBId, true, workerBId != failedWorkerId));

            await WaitForBrainStateAsync(root, settingsProbePid, brainId, "Recovering", timeoutMs: 5_000);
            await WaitForAsync(
                async () =>
                {
                    var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                    return status.PlacementEpoch > placement.PlacementEpoch;
                },
                timeoutMs: 5_000);

            var recoveryAssignments = GetCurrentRegionShardAssignments(actor, brainId);
            Assert.NotEmpty(recoveryAssignments);
            Assert.All(
                recoveryAssignments,
                assignment => Assert.NotEqual(failedWorkerId, ToGuidOrThrow(assignment.WorkerNodeId)));

            await RegisterHostedShardsFromCurrentAssignmentsAsync(root, hiveMind, actor, brainId, workerPidById);
            await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 5_000);
            await WaitForBrainStateAsync(root, settingsProbePid, brainId, "Active", timeoutMs: 5_000);

            var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
            AssertPlacementStable(finalStatus);
            Assert.True(finalStatus.PlacementEpoch > placement.PlacementEpoch);

            var brainStates = await GetBrainStateSnapshotAsync(root, settingsProbePid, brainId);
            AssertStateSequence(brainStates, "Recovering", "Active");

            var brainTag = brainId.ToString("D");
            Assert.Equal(1, metrics.SumLong("nbn.hivemind.recovery.requested", "brain_id", brainTag));
            Assert.Equal(1, metrics.SumLong("nbn.hivemind.recovery.completed", "brain_id", brainTag));
            Assert.Equal(0, metrics.SumLong("nbn.hivemind.recovery.failed", "brain_id", brainTag));

            var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
            Assert.True(debugSnapshot.Count("brain.recovering") >= 1);
            Assert.True(debugSnapshot.Count("brain.recovered") >= 1);
        }
        finally
        {
            if (system is not null)
            {
                await system.ShutdownAsync();
            }
            artifactRoot.Dispose();
        }
    }

    [Fact]
    public async Task RequestPlacement_Queues_SameWorker_Assignments_Without_Starting_All_Timeouts_At_Once()
    {
        var artifactRoot = TempDirectoryScope.Create("nbn-hivemind-worker-dispatch-queue", clearSqlitePools: true);
        ActorSystem? system = null;

        try
        {
            var baseRef = await StoreRichBaseDefinitionAsync(artifactRoot);
            system = new ActorSystem();
            var root = system.Root;

            var workerId = Guid.NewGuid();
            var workerProbe = new PlacementWorkerProbe(
                workerId,
                dropAcks: false,
                failFirstRetryable: false,
                autoRespondAssignments: false,
                autoRespondReconcile: true);
            var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 200,
                    retryBackoffMs: 10,
                    maxRetries: 0,
                    reconcileTimeoutMs: 500))));

            PrimeWorkers(root, hiveMind, workerPid, workerId);

            async Task<Guid> RequestPlacementAsync(Guid brainId)
            {
                var ack = await root.RequestAsync<PlacementAck>(
                    hiveMind,
                    new RequestPlacement
                    {
                        BrainId = brainId.ToProtoUuid(),
                        BaseDef = baseRef,
                        InputWidth = 3,
                        OutputWidth = 2,
                        ShardPlan = new ShardPlan
                        {
                            Mode = ShardPlanMode.ShardPlanFixed,
                            ShardCount = 2
                        }
                    });
                Assert.True(ack.Accepted);
                return brainId;
            }

            var brainA = await RequestPlacementAsync(Guid.NewGuid());
            var brainB = await RequestPlacementAsync(Guid.NewGuid());

            for (var iteration = 0; iteration < 40; iteration++)
            {
                var deferred = await root.RequestAsync<DeferredAssignmentAckSnapshot>(
                    workerPid,
                    new GetDeferredAssignmentAckSnapshot());
                var deferredBrains = deferred.Entries.Select(static entry => entry.BrainId).Distinct().ToArray();
                Assert.True(
                    deferredBrains.Length <= 1,
                    $"Expected queued dispatches from at most one brain at a time, saw {deferredBrains.Length}: {string.Join(", ", deferredBrains.Select(static brainId => brainId.ToString("D")))}");

                var statusA = await GetPlacementLifecycleAsync(root, hiveMind, brainA);
                var statusB = await GetPlacementLifecycleAsync(root, hiveMind, brainB);
                var aStable = statusA.LifecycleState is PlacementLifecycleState.PlacementLifecycleAssigned or PlacementLifecycleState.PlacementLifecycleRunning;
                var bStable = statusB.LifecycleState is PlacementLifecycleState.PlacementLifecycleAssigned or PlacementLifecycleState.PlacementLifecycleRunning;
                if (aStable && bStable)
                {
                    AssertPlacementStable(statusA);
                    AssertPlacementStable(statusB);
                    await system.ShutdownAsync();
                    return;
                }

                var released = await root.RequestAsync<ReleaseNextDeferredAssignmentAckResult>(
                    workerPid,
                    new ReleaseNextDeferredAssignmentAck());
                Assert.True(released.Released, "Expected a deferred placement assignment ack to be available.");
                await Task.Delay(25);
            }

            throw new Xunit.Sdk.XunitException("Queued placement assignments did not drain to stable placement for both brains.");
        }
        finally
        {
            if (system is not null)
            {
                await system.ShutdownAsync();
            }
            artifactRoot.Dispose();
        }
    }

    [Fact]
    public async Task OfflineWorker_Triggers_ArtifactBacked_Recovery_And_Preserves_Paused_State()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var artifactRoot = TempDirectoryScope.Create("nbn-hivemind-recovery-paused", clearSqlitePools: true);
        ActorSystem? system = null;

        try
        {
            var baseRef = await StoreRichBaseDefinitionAsync(artifactRoot);
            system = new ActorSystem();
            var root = system.Root;
            var settingsProbePid = root.Spawn(Props.FromProducer(static () => new BrainStateProbeActor()));
            var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));

            var workerAId = Guid.NewGuid();
            var workerBId = Guid.NewGuid();
            var workerA = new PlacementWorkerProbe(workerAId, dropAcks: false, failFirstRetryable: false);
            var workerB = new PlacementWorkerProbe(workerBId, dropAcks: false, failFirstRetryable: false);
            var workerAPid = root.Spawn(Props.FromProducer(() => workerA));
            var workerBPid = root.Spawn(Props.FromProducer(() => workerB));
            var actor = new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 2_000,
                    retryBackoffMs: 50,
                    maxRetries: 1,
                    reconcileTimeoutMs: 2_000,
                    rescheduleMinMinutes: 0),
                debugHubPid: debugProbePid,
                settingsPid: settingsProbePid,
                debugStreamEnabled: true);
            var hiveMind = root.Spawn(Props.FromProducer(() => actor));

            PrimeWorkers(root, hiveMind, (workerAPid, workerAId, true, true), (workerBPid, workerBId, true, true));

            var brainId = Guid.NewGuid();
            var placement = await root.RequestAsync<PlacementAck>(
                hiveMind,
                new RequestPlacement
                {
                    BrainId = brainId.ToProtoUuid(),
                    BaseDef = baseRef,
                    InputWidth = 3,
                    OutputWidth = 2,
                    ShardPlan = new ShardPlan
                    {
                        Mode = ShardPlanMode.ShardPlanFixed,
                        ShardCount = 2
                    }
                });
            Assert.True(placement.Accepted);
            await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 5_000);

            root.Send(hiveMind, new Nbn.Shared.HiveMind.PauseBrainRequest(brainId, "operator_pause"));
            await WaitForBrainStateAsync(root, settingsProbePid, brainId, "Paused", timeoutMs: 5_000);

            var initialShardProbes = await RegisterSnapshotShardsFromCurrentAssignmentsAsync(root, hiveMind, actor, brainId);
            var snapshotReady = await root.RequestAsync<ProtoIo.SnapshotReady>(
                hiveMind,
                new ProtoIo.RequestSnapshot
                {
                    BrainId = brainId.ToProtoUuid()
                });
            Assert.NotNull(snapshotReady.Snapshot);
            Assert.True(snapshotReady.Snapshot.TryToSha256Bytes(out _));

            var failedWorkerId = initialShardProbes
                .GroupBy(static probe => probe.WorkerId)
                .OrderByDescending(static group => group.Count())
                .First()
                .Key;
            var workerPidById = new Dictionary<Guid, PID>
            {
                [workerAId] = workerAPid,
                [workerBId] = workerBPid
            };

            root.Stop(workerPidById[failedWorkerId]);
            foreach (var shardProbe in initialShardProbes.Where(probe => probe.WorkerId == failedWorkerId))
            {
                root.Stop(shardProbe.Pid);
            }

            PrimeWorkers(
                root,
                hiveMind,
                (workerAPid, workerAId, true, workerAId != failedWorkerId),
                (workerBPid, workerBId, true, workerBId != failedWorkerId));

            await WaitForBrainStateAsync(root, settingsProbePid, brainId, "Recovering", timeoutMs: 5_000);
            await WaitForAsync(
                async () =>
                {
                    var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                    return status.PlacementEpoch > placement.PlacementEpoch;
                },
                timeoutMs: 5_000);

            var recoveryAssignments = GetCurrentRegionShardAssignments(actor, brainId);
            Assert.NotEmpty(recoveryAssignments);
            Assert.All(
                recoveryAssignments,
                assignment => Assert.NotEqual(failedWorkerId, ToGuidOrThrow(assignment.WorkerNodeId)));

            await RegisterHostedShardsFromCurrentAssignmentsAsync(root, hiveMind, actor, brainId, workerPidById);
            await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 5_000);
            await WaitForAsync(
                async () =>
                {
                    var snapshot = await GetBrainStateSnapshotAsync(root, settingsProbePid, brainId);
                    return snapshot.Events.Count >= 2
                           && string.Equals(snapshot.Events[^1].State, "Paused", StringComparison.Ordinal)
                           && string.Equals(snapshot.Events[^2].State, "Recovering", StringComparison.Ordinal);
                },
                timeoutMs: 5_000);

            var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
            AssertPlacementStable(finalStatus);
            Assert.True(finalStatus.PlacementEpoch > placement.PlacementEpoch);

            var brainStates = await GetBrainStateSnapshotAsync(root, settingsProbePid, brainId);
            AssertStateSequence(brainStates, "Paused", "Recovering", "Paused");
            Assert.Equal("Paused", brainStates.Events[^1].State);
            var recoveringIndex = brainStates.Events
                .Select((entry, index) => (entry, index))
                .First(pair => string.Equals(pair.entry.State, "Recovering", StringComparison.Ordinal))
                .index;
            Assert.DoesNotContain(
                brainStates.Events.Skip(recoveringIndex + 1),
                entry => string.Equals(entry.State, "Active", StringComparison.Ordinal));

            var brainTag = brainId.ToString("D");
            Assert.Equal(1, metrics.SumLong("nbn.hivemind.recovery.requested", "brain_id", brainTag));
            Assert.Equal(1, metrics.SumLong("nbn.hivemind.recovery.completed", "brain_id", brainTag));
            Assert.Equal(0, metrics.SumLong("nbn.hivemind.recovery.failed", "brain_id", brainTag));

            var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
            Assert.True(debugSnapshot.Count("brain.recovering") >= 1);
            Assert.True(debugSnapshot.Count("brain.recovered") >= 1);
        }
        finally
        {
            if (system is not null)
            {
                await system.ShutdownAsync();
            }
            artifactRoot.Dispose();
        }
    }

    [Fact]
    public async Task OfflineOnlyWorker_Fails_Recovery_And_Emits_Recovering_To_Dead()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var artifactRoot = TempDirectoryScope.Create("nbn-hivemind-recovery-failure", clearSqlitePools: true);
        ActorSystem? system = null;

        try
        {
            var baseRef = await StoreRichBaseDefinitionAsync(artifactRoot);
            system = new ActorSystem();
            var root = system.Root;
            var settingsProbePid = root.Spawn(Props.FromProducer(static () => new BrainStateProbeActor()));
            var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));

            var workerId = Guid.NewGuid();
            var worker = new PlacementWorkerProbe(workerId, dropAcks: false, failFirstRetryable: false);
            var workerPid = root.Spawn(Props.FromProducer(() => worker));
            var actor = new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 2_000,
                    retryBackoffMs: 50,
                    maxRetries: 1,
                    reconcileTimeoutMs: 2_000,
                    rescheduleMinMinutes: 0),
                debugHubPid: debugProbePid,
                settingsPid: settingsProbePid,
                debugStreamEnabled: true);
            var hiveMind = root.Spawn(Props.FromProducer(() => actor));

            PrimeWorkers(root, hiveMind, workerPid, workerId);

            var brainId = Guid.NewGuid();
            var placement = await root.RequestAsync<PlacementAck>(
                hiveMind,
                new RequestPlacement
                {
                    BrainId = brainId.ToProtoUuid(),
                    BaseDef = baseRef,
                    InputWidth = 3,
                    OutputWidth = 2
                });
            Assert.True(placement.Accepted);
            await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 5_000);

            var initialShardProbes = await RegisterSnapshotShardsFromCurrentAssignmentsAsync(root, hiveMind, actor, brainId);
            var snapshotReady = await root.RequestAsync<ProtoIo.SnapshotReady>(
                hiveMind,
                new ProtoIo.RequestSnapshot
                {
                    BrainId = brainId.ToProtoUuid()
                });
            Assert.NotNull(snapshotReady.Snapshot);
            Assert.True(snapshotReady.Snapshot.TryToSha256Bytes(out _));

            root.Stop(workerPid);
            foreach (var shardProbe in initialShardProbes)
            {
                root.Stop(shardProbe.Pid);
            }

            PrimeWorkers(root, hiveMind, (workerPid, workerId, true, false));

            await WaitForBrainStateAsync(root, settingsProbePid, brainId, "Recovering", timeoutMs: 5_000);
            await WaitForBrainStateAsync(root, settingsProbePid, brainId, "Dead", timeoutMs: 5_000);

            var brainStates = await GetBrainStateSnapshotAsync(root, settingsProbePid, brainId);
            AssertStateSequence(brainStates, "Recovering", "Dead");

            var brainTag = brainId.ToString("D");
            Assert.Equal(1, metrics.SumLong("nbn.hivemind.recovery.requested", "brain_id", brainTag));
            Assert.Equal(0, metrics.SumLong("nbn.hivemind.recovery.completed", "brain_id", brainTag));
            Assert.Equal(1, metrics.SumLong("nbn.hivemind.recovery.failed", "brain_id", brainTag));

            var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
            Assert.True(debugSnapshot.Count("brain.recovering") >= 1);
            Assert.True(debugSnapshot.Count("brain.recovery.failed") >= 1);
            Assert.True(debugSnapshot.Count("brain.terminated") >= 1);
        }
        finally
        {
            if (system is not null)
            {
                await system.ShutdownAsync();
            }
            artifactRoot.Dispose();
        }
    }

    [Fact]
    public async Task Unexpected_Shard_Unregister_Triggers_FullBrainRecovery_And_Emits_Recovering_To_Active()
    {
        using var metrics = new MeterCollector(HiveMindTelemetry.MeterNameValue);
        var artifactRoot = TempDirectoryScope.Create("nbn-hivemind-shard-recovery", clearSqlitePools: true);
        ActorSystem? system = null;

        try
        {
            var baseRef = await StoreRichBaseDefinitionAsync(artifactRoot);
            system = new ActorSystem();
            var root = system.Root;
            var settingsProbePid = root.Spawn(Props.FromProducer(static () => new BrainStateProbeActor()));
            var debugProbePid = root.Spawn(Props.FromProducer(static () => new DebugProbeActor()));

            var workerAId = Guid.NewGuid();
            var workerBId = Guid.NewGuid();
            var workerA = new PlacementWorkerProbe(workerAId, dropAcks: false, failFirstRetryable: false);
            var workerB = new PlacementWorkerProbe(workerBId, dropAcks: false, failFirstRetryable: false);
            var workerAPid = root.Spawn(Props.FromProducer(() => workerA));
            var workerBPid = root.Spawn(Props.FromProducer(() => workerB));
            var workerPidById = new Dictionary<Guid, PID>
            {
                [workerAId] = workerAPid,
                [workerBId] = workerBPid
            };
            var actor = new HiveMindActor(
                CreateOptions(
                    assignmentTimeoutMs: 2_000,
                    retryBackoffMs: 50,
                    maxRetries: 1,
                    reconcileTimeoutMs: 2_000,
                    rescheduleMinMinutes: 0),
                debugHubPid: debugProbePid,
                settingsPid: settingsProbePid,
                debugStreamEnabled: true);
            var hiveMind = root.Spawn(Props.FromProducer(() => actor));

            PrimeWorkers(root, hiveMind, (workerAPid, workerAId, true, true), (workerBPid, workerBId, true, true));

            var brainId = Guid.NewGuid();
            var placement = await root.RequestAsync<PlacementAck>(
                hiveMind,
                new RequestPlacement
                {
                    BrainId = brainId.ToProtoUuid(),
                    BaseDef = baseRef,
                    InputWidth = 3,
                    OutputWidth = 2,
                    ShardPlan = new ShardPlan
                    {
                        Mode = ShardPlanMode.ShardPlanFixed,
                        ShardCount = 2
                    }
                });
            Assert.True(placement.Accepted);
            await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 5_000);

            var initialShardProbes = await RegisterSnapshotShardsFromCurrentAssignmentsAsync(root, hiveMind, actor, brainId);
            var lostShard = initialShardProbes.First();
            var snapshotReady = await root.RequestAsync<ProtoIo.SnapshotReady>(
                hiveMind,
                new ProtoIo.RequestSnapshot
                {
                    BrainId = brainId.ToProtoUuid()
                });
            Assert.NotNull(snapshotReady.Snapshot);
            Assert.True(snapshotReady.Snapshot.TryToSha256Bytes(out _));

            await root.RequestAsync<SendMessageAck>(
                lostShard.Pid,
                new SendMessage(
                    hiveMind,
                    new UnregisterShard
                    {
                        BrainId = brainId.ToProtoUuid(),
                        RegionId = (uint)lostShard.RegionId,
                        ShardIndex = (uint)lostShard.ShardIndex,
                        PlacementEpoch = placement.PlacementEpoch,
                        AssignmentId = ShardAssignmentId(placement, lostShard.RegionId, lostShard.ShardIndex)
                    }));
            root.Stop(lostShard.Pid);

            await WaitForBrainStateAsync(root, settingsProbePid, brainId, "Recovering", timeoutMs: 5_000);
            await WaitForAsync(
                async () =>
                {
                    var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                    return status.PlacementEpoch > placement.PlacementEpoch;
                },
                timeoutMs: 5_000);

            await RegisterHostedShardsFromCurrentAssignmentsAsync(root, hiveMind, actor, brainId, workerPidById);
            await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 5_000);
            await WaitForBrainStateAsync(root, settingsProbePid, brainId, "Active", timeoutMs: 5_000);

            var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
            AssertPlacementStable(finalStatus);
            Assert.True(finalStatus.PlacementEpoch > placement.PlacementEpoch);

            var brainStates = await GetBrainStateSnapshotAsync(root, settingsProbePid, brainId);
            AssertStateSequence(brainStates, "Recovering", "Active");

            var brainTag = brainId.ToString("D");
            Assert.Equal(1, metrics.SumLong("nbn.hivemind.recovery.requested", "brain_id", brainTag));
            Assert.Equal(1, metrics.SumLong("nbn.hivemind.recovery.completed", "brain_id", brainTag));
            Assert.Equal(0, metrics.SumLong("nbn.hivemind.recovery.failed", "brain_id", brainTag));

            var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
            Assert.True(debugSnapshot.Count("brain.recovering") >= 1);
            Assert.True(debugSnapshot.Count("brain.recovered") >= 1);
        }
        finally
        {
            if (system is not null)
            {
                await system.ShutdownAsync();
            }
            artifactRoot.Dispose();
        }
    }

    private static void PrimeWorkers(IRootContext root, PID hiveMind, PID workerPid, Guid workerId)
        => HiveMindTestSupport.PrimeWorkers(root, hiveMind, workerPid, workerId);

    private static void PrimeWorkers(
        IRootContext root,
        PID hiveMind,
        params (PID WorkerPid, Guid WorkerId, bool IsReady, bool IsAlive)[] workers)
        => HiveMindTestSupport.PrimeWorkers(root, hiveMind, workers);

    private static Task WaitForAsync(Func<Task<bool>> predicate, int timeoutMs)
        => AsyncTestHelpers.WaitForAsync(predicate, timeoutMs);

    private static async Task WaitForPlacementMatchedAsync(IRootContext root, PID hiveMind, Guid brainId, int timeoutMs)
    {
        await WaitForAsync(
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return IsAssignedOrRunning(status.LifecycleState)
                       && status.ReconcileState == PlacementReconcileState.PlacementReconcileMatched;
            },
            timeoutMs: timeoutMs);
    }

    private static async Task DrainDeferredAssignmentAcksUntilAsync(
        IRootContext root,
        PID workerPid,
        Func<Task<bool>> completionPredicate,
        int timeoutMs,
        int idleDelayMs = 25)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.ElapsedMilliseconds < timeoutMs)
        {
            if (await completionPredicate())
            {
                return;
            }

            var released = await root.RequestAsync<ReleaseNextDeferredAssignmentAckResult>(workerPid, new ReleaseNextDeferredAssignmentAck());
            if (!released.Released)
            {
                await Task.Delay(idleDelayMs);
                continue;
            }

            await Task.Delay(idleDelayMs);
        }

        throw new Xunit.Sdk.XunitException("Deferred placement assignment acknowledgements did not drain before the timeout elapsed.");
    }

    private static async Task<PlacementLifecycleInfo> GetPlacementLifecycleAsync(IRootContext root, PID hiveMind, Guid brainId)
        => await root.RequestAsync<PlacementLifecycleInfo>(
            hiveMind,
            new GetPlacementLifecycle
            {
                BrainId = brainId.ToProtoUuid()
            });

    private static void AssertPlacementStable(PlacementLifecycleInfo status)
    {
        Assert.True(
            IsAssignedOrRunning(status.LifecycleState),
            $"Expected placement lifecycle to remain Assigned/Running, actual={status.LifecycleState}.");
        Assert.Equal(PlacementReconcileState.PlacementReconcileMatched, status.ReconcileState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, status.FailureReason);
    }

    private static bool IsAssignedOrRunning(PlacementLifecycleState state)
        => state == PlacementLifecycleState.PlacementLifecycleAssigned
           || state == PlacementLifecycleState.PlacementLifecycleRunning;

    private static object CreateHiveMindPrivateMessage(string typeName, params object?[] args)
    {
        var messageType = typeof(HiveMindActor).GetNestedType(typeName, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(messageType);
        return Activator.CreateInstance(
            messageType!,
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public,
            binder: null,
            args: args,
            culture: null)!;
    }

    private static object CreateHiveMindPrivateObject(string typeName, params object?[] args)
        => CreateHiveMindPrivateMessage(typeName, args);

    private static void SetPrivateField<T>(HiveMindActor actor, string fieldName, T value)
    {
        var field = typeof(HiveMindActor).GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        field!.SetValue(actor, value);
    }

    private static T GetPrivateField<T>(HiveMindActor actor, string fieldName)
    {
        var field = typeof(HiveMindActor).GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        return (T)field!.GetValue(actor)!;
    }

    private static PID GetRegisteredShardPid(HiveMindActor actor, Guid brainId, int regionId, int shardIndex)
    {
        var brainState = GetBrainState(actor, brainId);

        var shardsProperty = brainState!.GetType().GetProperty("Shards", BindingFlags.Instance | BindingFlags.Public);
        Assert.NotNull(shardsProperty);

        var shards = shardsProperty!.GetValue(brainState);
        Assert.NotNull(shards);

        var shardArgs = new object?[] { ShardId32.From(regionId, shardIndex), null };
        var foundShard = (bool)shards!.GetType().GetMethod("TryGetValue")!.Invoke(shards, shardArgs)!;
        Assert.True(foundShard);

        return Assert.IsType<PID>(shardArgs[1]);
    }

    private static PID? GetOutputSinkPid(HiveMindActor actor, Guid brainId)
    {
        var brainState = GetBrainState(actor, brainId);
        var outputSinkProperty = brainState.GetType().GetProperty("OutputSinkPid", BindingFlags.Instance | BindingFlags.Public);
        Assert.NotNull(outputSinkProperty);
        return outputSinkProperty!.GetValue(brainState) as PID;
    }

    private static object GetBrainState(HiveMindActor actor, Guid brainId)
    {
        var brainsField = typeof(HiveMindActor).GetField("_brains", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(brainsField);

        var brains = brainsField!.GetValue(actor);
        Assert.NotNull(brains);

        var brainArgs = new object?[] { brainId, null };
        var foundBrain = (bool)brains!.GetType().GetMethod("TryGetValue")!.Invoke(brains, brainArgs)!;
        Assert.True(foundBrain);
        Assert.NotNull(brainArgs[1]);
        return brainArgs[1]!;
    }

    private static int GetPendingRescheduleBrainCount(HiveMindActor actor)
    {
        var pending = GetPrivateField<object?>(actor, "_pendingReschedule");
        if (pending is null)
        {
            return 0;
        }

        var pendingBrains = pending.GetType().GetProperty("PendingBrains", BindingFlags.Instance | BindingFlags.Public);
        Assert.NotNull(pendingBrains);
        var map = pendingBrains!.GetValue(pending);
        Assert.NotNull(map);

        var countProperty = map!.GetType().GetProperty("Count", BindingFlags.Instance | BindingFlags.Public);
        Assert.NotNull(countProperty);
        return (int)countProperty!.GetValue(map)!;
    }

    private static IReadOnlyList<PlacementAssignment> GetCurrentRegionShardAssignments(HiveMindActor actor, Guid brainId)
    {
        var brainState = GetBrainState(actor, brainId);
        var executionProperty = brainState.GetType().GetProperty("PlacementExecution", BindingFlags.Instance | BindingFlags.Public);
        Assert.NotNull(executionProperty);
        var execution = executionProperty!.GetValue(brainState);
        Assert.NotNull(execution);

        var assignmentsProperty = execution!.GetType().GetProperty("Assignments", BindingFlags.Instance | BindingFlags.Public);
        Assert.NotNull(assignmentsProperty);
        var assignmentsMap = assignmentsProperty!.GetValue(execution);
        Assert.NotNull(assignmentsMap);

        var valuesProperty = assignmentsMap!.GetType().GetProperty("Values", BindingFlags.Instance | BindingFlags.Public);
        Assert.NotNull(valuesProperty);
        var values = Assert.IsAssignableFrom<IEnumerable>(valuesProperty!.GetValue(assignmentsMap));

        return values
            .Cast<object>()
            .Select(entry => entry.GetType().GetProperty("Assignment", BindingFlags.Instance | BindingFlags.Public)!.GetValue(entry))
            .OfType<PlacementAssignment>()
            .Where(static assignment => assignment.Target == PlacementAssignmentTarget.PlacementTargetRegionShard)
            .OrderBy(static assignment => assignment.RegionId)
            .ThenBy(static assignment => assignment.ShardIndex)
            .Select(static assignment => assignment.Clone())
            .ToArray();
    }

    private static IReadOnlyDictionary<Guid, PID> GetCurrentPlacementWorkerTargets(HiveMindActor actor, Guid brainId)
    {
        var brainState = GetBrainState(actor, brainId);
        var executionProperty = brainState.GetType().GetProperty("PlacementExecution", BindingFlags.Instance | BindingFlags.Public);
        Assert.NotNull(executionProperty);
        var execution = executionProperty!.GetValue(brainState);
        Assert.NotNull(execution);

        var workerTargetsProperty = execution!.GetType().GetProperty("WorkerTargets", BindingFlags.Instance | BindingFlags.Public);
        Assert.NotNull(workerTargetsProperty);
        var workerTargets = Assert.IsAssignableFrom<Dictionary<Guid, PID>>(workerTargetsProperty!.GetValue(execution));
        return workerTargets.ToDictionary(static pair => pair.Key, static pair => pair.Value);
    }

    private static async Task<IReadOnlyList<RegisteredShardProbe>> RegisterSnapshotShardsFromCurrentAssignmentsAsync(
        IRootContext root,
        PID hiveMind,
        HiveMindActor actor,
        Guid brainId)
    {
        var assignments = GetCurrentRegionShardAssignments(actor, brainId);
        var workerTargets = GetCurrentPlacementWorkerTargets(actor, brainId);
        Assert.NotEmpty(assignments);

        var probes = new List<RegisteredShardProbe>(assignments.Count);
        foreach (var assignment in assignments)
        {
            var neuronCount = Math.Max(1, (int)assignment.NeuronCount);
            var bufferCodes = Enumerable.Range(0, neuronCount)
                .Select(index => (int)assignment.RegionId * 100 + (int)assignment.ShardIndex * 10 + index)
                .ToArray();
            var pid = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
                brainId,
                ShardId32.From((int)assignment.RegionId, (int)assignment.ShardIndex),
                neuronStart: (int)assignment.NeuronStart,
                bufferCodes: bufferCodes,
                enabledBitset: BuildEnabledBitset(neuronCount),
                overlays: Array.Empty<SnapshotOverlayRecord>())));
            var workerId = ToGuidOrThrow(assignment.WorkerNodeId);
            Assert.True(workerTargets.TryGetValue(workerId, out var workerPid), $"Missing worker target for {workerId:D}.");

            await root.RequestAsync<SendMessageAck>(
                workerPid!,
                new SendMessage(
                    hiveMind,
                    new RegisterShard
                    {
                        BrainId = brainId.ToProtoUuid(),
                        RegionId = assignment.RegionId,
                        ShardIndex = assignment.ShardIndex,
                        ShardPid = PidLabel(pid),
                        NeuronStart = assignment.NeuronStart,
                        NeuronCount = assignment.NeuronCount,
                        PlacementEpoch = assignment.PlacementEpoch,
                        AssignmentId = assignment.AssignmentId
                    }));

            probes.Add(new RegisteredShardProbe(
                ToGuidOrThrow(assignment.WorkerNodeId),
                (int)assignment.RegionId,
                (int)assignment.ShardIndex,
                pid));
        }

        await WaitForRegisteredShardCountAsync(root, hiveMind, brainId, probes.Count, timeoutMs: 5_000);
        return probes;
    }

    private static async Task RegisterHostedShardsFromCurrentAssignmentsAsync(
        IRootContext root,
        PID hiveMind,
        HiveMindActor actor,
        Guid brainId,
        IReadOnlyDictionary<Guid, PID> workerPidById)
    {
        var assignments = GetCurrentRegionShardAssignments(actor, brainId);
        Assert.NotEmpty(assignments);

        foreach (var assignment in assignments)
        {
            var workerId = ToGuidOrThrow(assignment.WorkerNodeId);
            Assert.True(workerPidById.TryGetValue(workerId, out var workerPid), $"Missing worker pid for {workerId:D}.");
            root.Send(
                workerPid!,
                new RegisterHostedShard(
                    hiveMind,
                    brainId,
                    assignment.RegionId,
                    assignment.ShardIndex,
                    assignment.NeuronStart,
                    assignment.NeuronCount,
                    assignment.PlacementEpoch,
                    assignment.AssignmentId));
        }

        await WaitForRegisteredShardCountAsync(root, hiveMind, brainId, assignments.Count, timeoutMs: 5_000);
    }

    private static async Task WaitForRegisteredShardCountAsync(
        IRootContext root,
        PID hiveMind,
        Guid brainId,
        int expectedCount,
        int timeoutMs)
    {
        await WaitForAsync(
            async () =>
            {
                var status = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
                return status.RegisteredShards >= expectedCount;
            },
            timeoutMs: timeoutMs);
    }

    private static async Task<Nbn.Proto.ArtifactRef> StoreRichBaseDefinitionAsync(string artifactRoot)
    {
        var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
        var richNbn = NbnTestVectors.CreateRichNbnVector();
        var manifest = await store.StoreAsync(new MemoryStream(richNbn.Bytes), "application/x-nbn");
        return manifest.ArtifactId.Bytes.ToArray().ToArtifactRef(
            (ulong)manifest.ByteLength,
            "application/x-nbn",
            artifactRoot);
    }

    private static async Task WaitForBrainStateAsync(
        IRootContext root,
        PID settingsProbePid,
        Guid brainId,
        string state,
        int timeoutMs)
    {
        await WaitForAsync(
            async () =>
            {
                var snapshot = await GetBrainStateSnapshotAsync(root, settingsProbePid, brainId);
                return snapshot.ContainsState(state);
            },
                timeoutMs: timeoutMs);
    }

    private static async Task<bool> WaitForBrainStateOrTimeoutAsync(
        IRootContext root,
        PID settingsProbePid,
        Guid brainId,
        string state,
        int timeoutMs)
    {
        var deadline = Stopwatch.StartNew();
        while (deadline.ElapsedMilliseconds < timeoutMs)
        {
            var snapshot = await GetBrainStateSnapshotAsync(root, settingsProbePid, brainId);
            if (snapshot.ContainsState(state))
            {
                return true;
            }

            await Task.Delay(20);
        }

        return false;
    }

    private static async Task<BrainStateEventSnapshot> GetBrainStateSnapshotAsync(
        IRootContext root,
        PID settingsProbePid,
        Guid brainId)
        => await root.RequestAsync<BrainStateEventSnapshot>(settingsProbePid, new GetBrainStateSnapshot(brainId));

    private static void AssertStateSequence(BrainStateEventSnapshot snapshot, params string[] states)
    {
        var searchIndex = 0;
        foreach (var state in states)
        {
            var matchIndex = snapshot.Events
                .Skip(searchIndex)
                .ToList()
                .FindIndex(entry => string.Equals(entry.State, state, StringComparison.Ordinal));
            Assert.True(matchIndex >= 0, $"State '{state}' was not observed. Observed: {string.Join(", ", snapshot.Events.Select(static entry => entry.State))}");
            searchIndex += matchIndex + 1;
        }
    }

    private static byte[] BuildEnabledBitset(int neuronCount)
    {
        var bytes = new byte[(Math.Max(1, neuronCount) + 7) / 8];
        for (var index = 0; index < neuronCount; index++)
        {
            bytes[index / 8] |= (byte)(1 << (index % 8));
        }

        return bytes;
    }

    private static Guid ToGuidOrThrow(Nbn.Proto.Uuid uuid)
    {
        Assert.True(uuid.TryToGuid(out var guid));
        return guid;
    }

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private static string ShardAssignmentId(PlacementAck placement, int regionId, int shardIndex = 0)
        => $"{placement.RequestId}:{placement.PlacementEpoch}:region-{regionId}-shard-{shardIndex}";

    private static string ShardAssignmentId(Guid brainId, ulong placementEpoch, int regionId, int shardIndex = 0)
        => $"{brainId:N}:{placementEpoch}:{placementEpoch}:region-{regionId}-shard-{shardIndex}";

    private static HiveMindOptions CreateOptions(
        int assignmentTimeoutMs,
        int retryBackoffMs,
        int maxRetries,
        int reconcileTimeoutMs,
        int rescheduleMinTicks = 10,
        int rescheduleMinMinutes = 1,
        int rescheduleQuietMs = 50)
        => HiveMindTestSupport.CreateHiveMindOptions(
            assignmentTimeoutMs: assignmentTimeoutMs,
            retryBackoffMs: retryBackoffMs,
            maxRetries: maxRetries,
            reconcileTimeoutMs: reconcileTimeoutMs,
            rescheduleMinTicks: rescheduleMinTicks,
            rescheduleMinMinutes: rescheduleMinMinutes,
            rescheduleQuietMs: rescheduleQuietMs);

    private sealed record GetDebugProbeSnapshot;
    private sealed record SendMessage(PID Target, object Message);
    private sealed record SendMessageAck;
    private sealed record ForwardControlPlaneMessage(PID Target, object Message);
    private sealed record GetBrainStateSnapshot(Guid BrainId);
    private sealed record BrainStateEvent(Guid BrainId, string State, string Notes);
    private sealed record RegisteredShardProbe(Guid WorkerId, int RegionId, int ShardIndex, PID Pid);
    private sealed record ReleaseNextDeferredAssignmentAck;
    private sealed record ReleaseNextDeferredAssignmentAckResult(string AssignmentId, bool Released);
    private sealed record GetDeferredAssignmentAckSnapshot;
    private sealed record DeferredAssignmentAckEntry(Guid BrainId, string AssignmentId);
    private sealed record DeferredAssignmentAckSnapshot(
        IReadOnlyList<string> AssignmentIds,
        IReadOnlyList<DeferredAssignmentAckEntry> Entries);
    private sealed record ReleaseDeferredAssignmentAcks;
    private sealed record ReleaseDeferredReconcileReports;
    private sealed record RegisterHostedShard(
        PID HiveMind,
        Guid BrainId,
        uint RegionId,
        uint ShardIndex,
        uint NeuronStart,
        uint NeuronCount,
        ulong PlacementEpoch,
        string AssignmentId);
    private sealed record RegisterOutputSinkForBrain(PID HiveMind, Guid BrainId, PID OutputSinkPid);
    private sealed record GetTickCountingControllerSnapshot;
    private sealed record TickCountingControllerSnapshot(int TickComputeCount);

    private sealed record DebugProbeSnapshot(IReadOnlyDictionary<string, int> Counts, IReadOnlyList<DebugProbeEvent> Events)
    {
        public int Count(string category)
            => Counts.TryGetValue(category, out var value) ? value : 0;

        public int CountMessageContains(string category, string fragment)
        {
            if (string.IsNullOrWhiteSpace(category) || string.IsNullOrWhiteSpace(fragment))
            {
                return 0;
            }

            var count = 0;
            foreach (var entry in Events)
            {
                if (string.Equals(entry.Summary, category, StringComparison.Ordinal)
                    && entry.Message.Contains(fragment, StringComparison.Ordinal))
                {
                    count++;
                }
            }

            return count;
        }
    }

    private sealed record DebugProbeEvent(string Summary, string Message);

    private sealed record BrainStateEventSnapshot(IReadOnlyList<BrainStateEvent> Events)
    {
        public bool ContainsState(string state)
            => Events.Any(entry => string.Equals(entry.State, state, StringComparison.Ordinal));
    }

    private sealed class DebugProbeActor : IActor
    {
        private readonly Dictionary<string, int> _counts = new(StringComparer.Ordinal);
        private readonly List<DebugProbeEvent> _events = new();

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case DebugOutbound outbound:
                    var summary = outbound.Summary ?? string.Empty;
                    if (summary.Length > 0)
                    {
                        _counts[summary] = _counts.TryGetValue(summary, out var count) ? count + 1 : 1;
                        _events.Add(new DebugProbeEvent(summary, outbound.Message ?? string.Empty));
                    }
                    break;
                case GetDebugProbeSnapshot:
                    context.Respond(new DebugProbeSnapshot(
                        new Dictionary<string, int>(_counts, StringComparer.Ordinal),
                        _events.ToArray()));
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class BrainStateProbeActor : IActor
    {
        private readonly List<BrainStateEvent> _events = new();

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case ProtoSettings.BrainRegistered message when message.BrainId.TryToGuid(out var brainId):
                    _events.Add(new BrainStateEvent(brainId, message.State ?? string.Empty, "registered"));
                    break;
                case ProtoSettings.BrainStateChanged message when message.BrainId.TryToGuid(out var changedBrainId):
                    _events.Add(new BrainStateEvent(changedBrainId, message.State ?? string.Empty, message.Notes ?? string.Empty));
                    break;
                case ProtoSettings.BrainUnregistered message when message.BrainId.TryToGuid(out var unregisteredBrainId):
                    _events.Add(new BrainStateEvent(unregisteredBrainId, "Dead", "unregistered"));
                    break;
                case GetBrainStateSnapshot request:
                    context.Respond(new BrainStateEventSnapshot(
                        _events.Where(entry => entry.BrainId == request.BrainId).ToArray()));
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class ForwardControlPlaneActor : IActor
    {
        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is ForwardControlPlaneMessage forward)
            {
                context.Request(forward.Target, forward.Message);
            }

            return Task.CompletedTask;
        }
    }

    private sealed class TickCountingControllerActor : IActor
    {
        private int _tickComputeCount;

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case ForwardControlPlaneMessage forward:
                    context.Request(forward.Target, forward.Message);
                    break;
                case TickCompute:
                    _tickComputeCount++;
                    break;
                case GetTickCountingControllerSnapshot:
                    context.Respond(new TickCountingControllerSnapshot(_tickComputeCount));
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class SnapshotShardProbe : IActor
    {
        private readonly Guid _brainId;
        private readonly ShardId32 _shardId;
        private readonly int _neuronStart;
        private readonly int[] _bufferCodes;
        private readonly byte[] _enabledBitset;
        private readonly IReadOnlyList<SnapshotOverlayRecord> _overlays;

        public SnapshotShardProbe(
            Guid brainId,
            ShardId32 shardId,
            int neuronStart,
            int[] bufferCodes,
            byte[] enabledBitset,
            IReadOnlyList<SnapshotOverlayRecord> overlays)
        {
            _brainId = brainId;
            _shardId = shardId;
            _neuronStart = neuronStart;
            _bufferCodes = bufferCodes;
            _enabledBitset = enabledBitset;
            _overlays = overlays;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is SendMessage send)
            {
                context.Request(send.Target, send.Message);
                context.Respond(new SendMessageAck());
                return Task.CompletedTask;
            }

            if (context.Message is not CaptureShardSnapshot capture)
            {
                return Task.CompletedTask;
            }

            var response = new CaptureShardSnapshotAck
            {
                BrainId = _brainId.ToProtoUuid(),
                RegionId = (uint)_shardId.RegionId,
                ShardIndex = (uint)_shardId.ShardIndex,
                NeuronStart = (uint)_neuronStart,
                NeuronCount = (uint)_bufferCodes.Length,
                Success = capture.BrainId is not null
                          && capture.BrainId.TryToGuid(out var requestedBrainId)
                          && requestedBrainId == _brainId
                          && capture.RegionId == (uint)_shardId.RegionId
                          && capture.ShardIndex == (uint)_shardId.ShardIndex
            };

            if (!response.Success)
            {
                response.Error = "mismatch";
                context.Respond(response);
                return Task.CompletedTask;
            }

            response.BufferCodes.AddRange(_bufferCodes);
            response.EnabledBitset = Google.Protobuf.ByteString.CopyFrom(_enabledBitset);
            response.Overlays.Add(_overlays);
            context.Respond(response);
            return Task.CompletedTask;
        }
    }

    private sealed class NullActor : IActor
    {
        public Task ReceiveAsync(IContext context)
            => Task.CompletedTask;
    }

    private sealed class PlacementWorkerProbe : IActor
    {
        private readonly Guid _workerId;
        private readonly bool _dropAcks;
        private readonly bool _failFirstRetryable;
        private readonly bool _failFirstNonRetryable;
        private readonly ReconcileBehavior _reconcileBehavior;
        private readonly bool _autoRespondAssignments;
        private readonly bool _autoRespondReconcile;
        private bool _failedOne;
        private readonly Dictionary<string, int> _attempts = new(StringComparer.Ordinal);
        private readonly Dictionary<string, PlacementAssignment> _knownAssignments = new(StringComparer.Ordinal);
        private readonly List<ulong> _unassignmentEpochs = new();
        private readonly List<(PID Sender, PlacementAssignmentAck Ack)> _deferredAssignmentAcks = new();
        private readonly List<(PID Sender, PlacementReconcileReport Report)> _deferredReconcileReports = new();
        private bool _drainDeferredAssignmentAcks;

        public PlacementWorkerProbe(
            Guid workerId,
            bool dropAcks,
            bool failFirstRetryable,
            bool failFirstNonRetryable = false,
            ReconcileBehavior reconcileBehavior = ReconcileBehavior.Matched,
            bool autoRespondAssignments = true,
            bool autoRespondReconcile = true)
        {
            _workerId = workerId;
            _dropAcks = dropAcks;
            _failFirstRetryable = failFirstRetryable;
            _failFirstNonRetryable = failFirstNonRetryable;
            _reconcileBehavior = reconcileBehavior;
            _autoRespondAssignments = autoRespondAssignments;
            _autoRespondReconcile = autoRespondReconcile;
        }

        public int AssignmentRequestCount { get; private set; }
        public int RetryDispatchCount { get; private set; }
        public int ReconcileRequestCount { get; private set; }
        public int UnassignmentRequestCount { get; private set; }
        public int OutputSinkUpdateCount { get; private set; }
        public int ActiveAssignmentCount => _knownAssignments.Count;
        public IReadOnlyList<ulong> ActivePlacementEpochs
            => _knownAssignments.Values.Select(static value => value.PlacementEpoch).Distinct().OrderBy(static value => value).ToArray();

        public int CountUnassignmentsForEpoch(ulong placementEpoch)
            => _unassignmentEpochs.Count(epoch => epoch == placementEpoch);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case PlacementAssignmentRequest request:
                    HandlePlacementAssignmentRequest(context, request);
                    break;
                case PlacementUnassignmentRequest request:
                    HandlePlacementUnassignmentRequest(context, request);
                    break;
                case PlacementReconcileRequest request:
                    HandlePlacementReconcileRequest(context, request);
                    break;
                case ForwardControlPlaneMessage forward:
                    context.Request(forward.Target, forward.Message);
                    break;
                case ReleaseNextDeferredAssignmentAck:
                    FlushNextDeferredAssignmentAck(context);
                    break;
                case GetDeferredAssignmentAckSnapshot:
                    context.Respond(new DeferredAssignmentAckSnapshot(
                        _deferredAssignmentAcks
                            .Select(static pending => pending.Ack.AssignmentId ?? string.Empty)
                            .Where(static assignmentId => !string.IsNullOrWhiteSpace(assignmentId))
                            .ToArray(),
                        _deferredAssignmentAcks
                            .Select(static pending => pending.Ack.BrainId.TryToGuid(out var brainId)
                                ? new DeferredAssignmentAckEntry(brainId, pending.Ack.AssignmentId ?? string.Empty)
                                : new DeferredAssignmentAckEntry(Guid.Empty, pending.Ack.AssignmentId ?? string.Empty))
                            .Where(static entry => entry.BrainId != Guid.Empty && !string.IsNullOrWhiteSpace(entry.AssignmentId))
                            .ToArray()));
                    break;
                case ReleaseDeferredAssignmentAcks:
                    FlushDeferredAssignmentAcks(context);
                    break;
                case ReleaseDeferredReconcileReports:
                    FlushDeferredReconcileReports(context);
                    break;
                case RegisterHostedShard register:
                    context.Request(register.HiveMind, new RegisterShard
                    {
                        BrainId = register.BrainId.ToProtoUuid(),
                        RegionId = register.RegionId,
                        ShardIndex = register.ShardIndex,
                        ShardPid = PidLabel(context.Self),
                        NeuronStart = register.NeuronStart,
                        NeuronCount = register.NeuronCount,
                        PlacementEpoch = register.PlacementEpoch,
                        AssignmentId = register.AssignmentId
                    });
                    break;
                case SendMessage send:
                    context.Request(send.Target, send.Message);
                    context.Respond(new SendMessageAck());
                    break;
                case RegisterOutputSinkForBrain register:
                    context.Request(register.HiveMind, new RegisterOutputSink
                    {
                        BrainId = register.BrainId.ToProtoUuid(),
                        OutputPid = PidLabel(register.OutputSinkPid)
                    });
                    break;
                case UpdateShardOutputSink:
                    OutputSinkUpdateCount++;
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

            var attempts = _attempts.TryGetValue(assignment.AssignmentId, out var existing) ? existing + 1 : 1;
            _attempts[assignment.AssignmentId] = attempts;
            if (attempts > 1)
            {
                RetryDispatchCount++;
            }

            if (_dropAcks)
            {
                return;
            }

            PlacementAssignmentAck ack;
            if (_failFirstRetryable && !_failedOne)
            {
                _failedOne = true;
                ack = new PlacementAssignmentAck
                {
                    AssignmentId = assignment.AssignmentId,
                    BrainId = assignment.BrainId,
                    PlacementEpoch = assignment.PlacementEpoch,
                    State = PlacementAssignmentState.PlacementAssignmentFailed,
                    Accepted = false,
                    Retryable = true,
                    FailureReason = PlacementFailureReason.PlacementFailureWorkerUnavailable,
                    RetryAfterMs = 0,
                    Message = "retry once"
                };
            }
            else if (_failFirstNonRetryable && !_failedOne)
            {
                _failedOne = true;
                ack = new PlacementAssignmentAck
                {
                    AssignmentId = assignment.AssignmentId,
                    BrainId = assignment.BrainId,
                    PlacementEpoch = assignment.PlacementEpoch,
                    State = PlacementAssignmentState.PlacementAssignmentFailed,
                    Accepted = false,
                    Retryable = false,
                    FailureReason = PlacementFailureReason.PlacementFailureWorkerUnavailable,
                    RetryAfterMs = 0,
                    Message = "rejected"
                };
            }
            else
            {
                ack = new PlacementAssignmentAck
                {
                    AssignmentId = assignment.AssignmentId,
                    BrainId = assignment.BrainId,
                    PlacementEpoch = assignment.PlacementEpoch,
                    State = PlacementAssignmentState.PlacementAssignmentReady,
                    Accepted = true,
                    Retryable = false,
                    FailureReason = PlacementFailureReason.PlacementFailureNone,
                    Message = "ready"
                };
            }

            if (context.Sender is null)
            {
                return;
            }

            if (_autoRespondAssignments || _drainDeferredAssignmentAcks)
            {
                context.Request(context.Sender, ack);
            }
            else
            {
                _deferredAssignmentAcks.Add((context.Sender, ack));
            }
        }

        private void HandlePlacementUnassignmentRequest(IContext context, PlacementUnassignmentRequest request)
        {
            if (request.Assignment is null)
            {
                return;
            }

            var assignment = request.Assignment.Clone();
            UnassignmentRequestCount++;
            _unassignmentEpochs.Add(assignment.PlacementEpoch);
            var removed = _knownAssignments.Remove(assignment.AssignmentId);
            _attempts.Remove(assignment.AssignmentId);

            if (context.Sender is null)
            {
                return;
            }

            context.Request(context.Sender, new PlacementUnassignmentAck
            {
                AssignmentId = assignment.AssignmentId,
                BrainId = assignment.BrainId,
                PlacementEpoch = assignment.PlacementEpoch,
                Accepted = true,
                Retryable = false,
                FailureReason = PlacementFailureReason.PlacementFailureNone,
                Message = removed ? "unassigned" : "already_unassigned",
                RetryAfterMs = 0
            });
        }

        private void HandlePlacementReconcileRequest(IContext context, PlacementReconcileRequest request)
        {
            ReconcileRequestCount++;
            if (_reconcileBehavior == ReconcileBehavior.Drop)
            {
                return;
            }

            var report = new PlacementReconcileReport
            {
                BrainId = request.BrainId,
                PlacementEpoch = request.PlacementEpoch,
                ReconcileState = PlacementReconcileState.PlacementReconcileMatched
            };

            var index = 0;
            foreach (var assignment in _knownAssignments.Values.OrderBy(static value => value.AssignmentId, StringComparer.Ordinal))
            {
                if (_reconcileBehavior == ReconcileBehavior.Mismatch && index == 0)
                {
                    index++;
                    continue;
                }

                report.Assignments.Add(new PlacementObservedAssignment
                {
                    AssignmentId = assignment.AssignmentId,
                    Target = assignment.Target,
                    WorkerNodeId = _workerId.ToProtoUuid(),
                    RegionId = assignment.RegionId,
                    ShardIndex = assignment.ShardIndex,
                    ActorPid = context.Self.Id
                });
                index++;
            }

            if (context.Sender is null)
            {
                return;
            }

            if (_autoRespondReconcile)
            {
                context.Request(context.Sender, report);
            }
            else
            {
                _deferredReconcileReports.Add((context.Sender, report));
            }
        }

        private void FlushDeferredAssignmentAcks(IContext context)
        {
            _drainDeferredAssignmentAcks = true;
            foreach (var pending in _deferredAssignmentAcks)
            {
                context.Request(pending.Sender, pending.Ack.Clone());
            }

            _deferredAssignmentAcks.Clear();
        }

        private void FlushNextDeferredAssignmentAck(IContext context)
        {
            if (_deferredAssignmentAcks.Count == 0)
            {
                context.Respond(new ReleaseNextDeferredAssignmentAckResult(string.Empty, false));
                return;
            }

            var pending = _deferredAssignmentAcks[0];
            _deferredAssignmentAcks.RemoveAt(0);
            context.Request(pending.Sender, pending.Ack.Clone());
            context.Respond(new ReleaseNextDeferredAssignmentAckResult(pending.Ack.AssignmentId ?? string.Empty, true));
        }

        private void FlushDeferredReconcileReports(IContext context)
        {
            foreach (var pending in _deferredReconcileReports)
            {
                context.Request(pending.Sender, pending.Report.Clone());
            }

            _deferredReconcileReports.Clear();
        }
    }

    private enum ReconcileBehavior
    {
        Matched = 0,
        Mismatch = 1,
        Drop = 2
    }
}
