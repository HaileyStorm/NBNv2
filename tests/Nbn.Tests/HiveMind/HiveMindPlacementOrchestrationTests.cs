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
using ProtoSettings = Nbn.Proto.Settings;
using Xunit.Sdk;
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
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-hivemind-reschedule-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);
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

            await root.RequestAsync<SendMessageAck>(region0Shard, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 0,
                ShardIndex = 0,
                ShardPid = PidLabel(region0Shard),
                NeuronStart = 0,
                NeuronCount = 3
            }));
            await root.RequestAsync<SendMessageAck>(region1Shard, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 1,
                ShardIndex = 0,
                ShardPid = PidLabel(region1Shard),
                NeuronStart = 0,
                NeuronCount = 4
            }));
            await root.RequestAsync<SendMessageAck>(region31Shard, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 31,
                ShardIndex = 0,
                ShardPid = PidLabel(region31Shard),
                NeuronStart = 0,
                NeuronCount = 2
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
                    return deferred.AssignmentIds.Count >= 7;
                },
                timeoutMs: 5_000);

            root.Send(workerBPid, new ReleaseDeferredAssignmentAcks());
            await WaitForAsync(
                () => Task.FromResult(workerB.ReconcileRequestCount >= 1),
                timeoutMs: 5_000);
            root.Send(workerBPid, new ReleaseDeferredReconcileReports());

            await WaitForPlacementMatchedAsync(root, hiveMind, brainId, timeoutMs: 5_000);

            var pendingBeforeRegisters = GetPrivateField<object?>(actor, "_pendingReschedule");
            Assert.NotNull(pendingBeforeRegisters);
            Assert.Equal(0, workerB.OutputSinkUpdateCount);

            root.Send(workerBPid, new RegisterHostedShard(hiveMind, brainId, 0, 0, 0, 3));
            root.Send(workerBPid, new RegisterHostedShard(hiveMind, brainId, 1, 0, 0, 4));
            root.Send(workerBPid, new RegisterHostedShard(hiveMind, brainId, 31, 0, 0, 2));

            await WaitForAsync(
                () => Task.FromResult(GetPendingRescheduleBrainCount(actor) == 0),
                timeoutMs: 5_000);

            var finalStatus = await GetPlacementLifecycleAsync(root, hiveMind, brainId);
            AssertPlacementStable(finalStatus);
            Assert.True(workerB.OutputSinkUpdateCount >= 1);
            Assert.Equal(workerBPid.Id, GetRegisteredShardPid(actor, brainId, 31, 0).Id);

        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (system is not null)
            {
                await system.ShutdownAsync();
            }

            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
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

        root.Send(workerPid, new ReleaseDeferredAssignmentAcks());

        await WaitForAsync(
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

        root.Send(workerPid, new ReleaseDeferredAssignmentAcks());

        await WaitForAsync(
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

        root.Send(workerPid, new ReleaseDeferredAssignmentAcks());

        await WaitForAsync(
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

        root.Send(workerPid, new ReleaseDeferredAssignmentAcks());

        await WaitForAsync(
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

        root.Send(workerPid, new ReleaseDeferredAssignmentAcks());

        await WaitForAsync(
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

        root.Send(workerPid, new ReleaseDeferredAssignmentAcks());

        await WaitForAsync(
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

        root.Send(workerPid, new ReleaseDeferredAssignmentAcks());

        await WaitForAsync(
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

        root.Send(workerPid, new ReleaseDeferredAssignmentAcks());

        await WaitForAsync(
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

    private static void PrimeWorkers(IRootContext root, PID hiveMind, PID workerPid, Guid workerId)
        => PrimeWorkers(root, hiveMind, (workerPid, workerId, true, true));

    private static void PrimeWorkers(
        IRootContext root,
        PID hiveMind,
        params (PID WorkerPid, Guid WorkerId, bool IsReady, bool IsAlive)[] workers)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nowMs,
            Workers = { workers.Select(worker => BuildWorker(
                worker.WorkerId,
                isAlive: worker.IsAlive,
                isReady: worker.IsReady,
                lastSeenMs: nowMs,
                capabilityTimeMs: nowMs,
                address: string.Empty,
                rootActorName: worker.WorkerPid.Id)) }
        });
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

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

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
        int reconcileTimeoutMs,
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

    private sealed record GetDebugProbeSnapshot;
    private sealed record SendMessage(PID Target, object Message);
    private sealed record SendMessageAck;
    private sealed record ForwardControlPlaneMessage(PID Target, object Message);
    private sealed record ReleaseNextDeferredAssignmentAck;
    private sealed record ReleaseNextDeferredAssignmentAckResult(string AssignmentId, bool Released);
    private sealed record GetDeferredAssignmentAckSnapshot;
    private sealed record DeferredAssignmentAckSnapshot(IReadOnlyList<string> AssignmentIds);
    private sealed record ReleaseDeferredAssignmentAcks;
    private sealed record ReleaseDeferredReconcileReports;
    private sealed record RegisterHostedShard(PID HiveMind, Guid BrainId, uint RegionId, uint ShardIndex, uint NeuronStart, uint NeuronCount);
    private sealed record RegisterOutputSinkForBrain(PID HiveMind, Guid BrainId, PID OutputSinkPid);

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
                        NeuronCount = register.NeuronCount
                    });
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

            if (_autoRespondAssignments)
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
