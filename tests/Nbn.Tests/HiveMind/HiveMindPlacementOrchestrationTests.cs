using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Nbn.Tests.TestSupport;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;
using Xunit.Sdk;

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

    private sealed record GetDebugProbeSnapshot;
    private sealed record ForwardControlPlaneMessage(PID Target, object Message);
    private sealed record ReleaseNextDeferredAssignmentAck;
    private sealed record ReleaseNextDeferredAssignmentAckResult(string AssignmentId, bool Released);
    private sealed record GetDeferredAssignmentAckSnapshot;
    private sealed record DeferredAssignmentAckSnapshot(IReadOnlyList<string> AssignmentIds);
    private sealed record ReleaseDeferredAssignmentAcks;
    private sealed record ReleaseDeferredReconcileReports;

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
