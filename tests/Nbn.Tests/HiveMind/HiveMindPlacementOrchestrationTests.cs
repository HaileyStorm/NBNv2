using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Nbn.Tests.TestSupport;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;
using Xunit.Sdk;

namespace Nbn.Tests.HiveMind;

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
        var debugSnapshot = await root.RequestAsync<DebugProbeSnapshot>(debugProbePid, new GetDebugProbeSnapshot());
        Assert.True(debugSnapshot.Count("placement.reconcile.timeout") >= 1);

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

    private sealed record DebugProbeSnapshot(IReadOnlyDictionary<string, int> Counts)
    {
        public int Count(string category)
            => Counts.TryGetValue(category, out var value) ? value : 0;
    }

    private sealed class DebugProbeActor : IActor
    {
        private readonly Dictionary<string, int> _counts = new(StringComparer.Ordinal);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case DebugOutbound outbound:
                    var summary = outbound.Summary ?? string.Empty;
                    if (summary.Length > 0)
                    {
                        _counts[summary] = _counts.TryGetValue(summary, out var count) ? count + 1 : 1;
                    }
                    break;
                case GetDebugProbeSnapshot:
                    context.Respond(new DebugProbeSnapshot(new Dictionary<string, int>(_counts, StringComparer.Ordinal)));
                    break;
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
        private bool _failedOne;
        private readonly Dictionary<string, int> _attempts = new(StringComparer.Ordinal);
        private readonly Dictionary<string, PlacementAssignment> _knownAssignments = new(StringComparer.Ordinal);

        public PlacementWorkerProbe(
            Guid workerId,
            bool dropAcks,
            bool failFirstRetryable,
            bool failFirstNonRetryable = false,
            ReconcileBehavior reconcileBehavior = ReconcileBehavior.Matched)
        {
            _workerId = workerId;
            _dropAcks = dropAcks;
            _failFirstRetryable = failFirstRetryable;
            _failFirstNonRetryable = failFirstNonRetryable;
            _reconcileBehavior = reconcileBehavior;
        }

        public int AssignmentRequestCount { get; private set; }
        public int RetryDispatchCount { get; private set; }
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

            if (_failFirstRetryable && !_failedOne)
            {
                _failedOne = true;
                context.Respond(new PlacementAssignmentAck
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
                });
                return;
            }

            if (_failFirstNonRetryable && !_failedOne)
            {
                _failedOne = true;
                context.Respond(new PlacementAssignmentAck
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
                });
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

            context.Respond(report);
        }
    }

    private enum ReconcileBehavior
    {
        Matched = 0,
        Mismatch = 1,
        Drop = 2
    }
}
