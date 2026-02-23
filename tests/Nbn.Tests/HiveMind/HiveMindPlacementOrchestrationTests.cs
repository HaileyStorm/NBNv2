using Nbn.Proto.Control;
using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;
using Xunit.Sdk;

namespace Nbn.Tests.HiveMind;

public sealed class HiveMindPlacementOrchestrationTests
{
    [Fact]
    public async Task RequestPlacement_Dispatches_Assignments_And_Completes_Reconcile()
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

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task Retryable_AssignmentAck_Is_Retried_Then_Completes_Reconcile()
    {
        var system = new ActorSystem();
        var root = system.Root;

        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(workerId, dropAcks: false, failFirstRetryable: true);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 1_000,
                retryBackoffMs: 10,
                maxRetries: 2,
                reconcileTimeoutMs: 1_000))));

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

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task Missing_AssignmentAck_Exhausts_Retry_And_Fails_Placement()
    {
        var system = new ActorSystem();
        var root = system.Root;

        var workerId = Guid.NewGuid();
        var workerProbe = new PlacementWorkerProbe(workerId, dropAcks: true, failFirstRetryable: false);
        var workerPid = root.Spawn(Props.FromProducer(() => workerProbe));
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(
            CreateOptions(
                assignmentTimeoutMs: 100,
                retryBackoffMs: 10,
                maxRetries: 1,
                reconcileTimeoutMs: 500))));

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

    private sealed class PlacementWorkerProbe : IActor
    {
        private readonly Guid _workerId;
        private readonly bool _dropAcks;
        private readonly bool _failFirstRetryable;
        private bool _failedOne;
        private readonly Dictionary<string, int> _attempts = new(StringComparer.Ordinal);
        private readonly Dictionary<string, PlacementAssignment> _knownAssignments = new(StringComparer.Ordinal);

        public PlacementWorkerProbe(Guid workerId, bool dropAcks, bool failFirstRetryable)
        {
            _workerId = workerId;
            _dropAcks = dropAcks;
            _failFirstRetryable = failFirstRetryable;
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
            var report = new PlacementReconcileReport
            {
                BrainId = request.BrainId,
                PlacementEpoch = request.PlacementEpoch,
                ReconcileState = PlacementReconcileState.PlacementReconcileMatched
            };

            foreach (var assignment in _knownAssignments.Values.OrderBy(static value => value.AssignmentId, StringComparer.Ordinal))
            {
                report.Assignments.Add(new PlacementObservedAssignment
                {
                    AssignmentId = assignment.AssignmentId,
                    Target = assignment.Target,
                    WorkerNodeId = _workerId.ToProtoUuid(),
                    RegionId = assignment.RegionId,
                    ShardIndex = assignment.ShardIndex,
                    ActorPid = context.Self.Id
                });
            }

            context.Respond(report);
        }
    }
}
