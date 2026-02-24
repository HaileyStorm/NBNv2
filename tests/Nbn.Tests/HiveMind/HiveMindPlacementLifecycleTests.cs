using Nbn.Proto.Control;
using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Tests.HiveMind;

public class HiveMindPlacementLifecycleTests
{
    [Fact]
    public async Task RequestPlacement_Returns_Lifecycle_Metadata_And_Status()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));
        PrimeEligibleWorker(root, hiveMind);

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
        Assert.Equal<ulong>(1, ack.PlacementEpoch);
        Assert.Equal(1, (int)ack.LifecycleState);
        Assert.Equal(0, (int)ack.FailureReason);
        Assert.False(string.IsNullOrWhiteSpace(ack.RequestId));

        var status = await root.RequestAsync<PlacementLifecycleInfo>(
            hiveMind,
            new GetPlacementLifecycle
            {
                BrainId = brainId.ToProtoUuid()
            });

        Assert.Equal(brainId.ToProtoUuid().Value, status.BrainId.Value);
        Assert.Equal<ulong>(1, status.PlacementEpoch);
        Assert.Contains(
            status.LifecycleState,
            new[]
            {
                PlacementLifecycleState.PlacementLifecycleRequested,
                PlacementLifecycleState.PlacementLifecycleAssigning
            });
        Assert.Equal(0u, status.RegisteredShards);
        Assert.False(string.IsNullOrWhiteSpace(status.RequestId));

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RegisterBrain_AndShard_Advance_Placement_Lifecycle()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));
        PrimeEligibleWorker(root, hiveMind);

        var brainId = Guid.NewGuid();
        var placement = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 1,
                OutputWidth = 1
            });
        Assert.True(placement.Accepted);

        var controllerSender = root.Spawn(Props.FromProducer(() => new ManualSenderActor()));
        await root.RequestAsync<SendMessageAck>(controllerSender, new SendMessage(hiveMind, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            BrainRootPid = PidLabel(controllerSender),
            SignalRouterPid = PidLabel(controllerSender)
        }));

        var assigned = await root.RequestAsync<PlacementLifecycleInfo>(
            hiveMind,
            new GetPlacementLifecycle
            {
                BrainId = brainId.ToProtoUuid()
            });
        Assert.Equal(3, (int)assigned.LifecycleState);

        var shardSender = root.Spawn(Props.FromProducer(() => new ManualSenderActor()));
        await root.RequestAsync<SendMessageAck>(shardSender, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = 1,
            ShardIndex = 0,
            ShardPid = PidLabel(shardSender),
            NeuronStart = 0,
            NeuronCount = 8
        }));

        var running = await root.RequestAsync<PlacementLifecycleInfo>(
            hiveMind,
            new GetPlacementLifecycle
            {
                BrainId = brainId.ToProtoUuid()
            });
        Assert.Equal(4, (int)running.LifecycleState);
        Assert.Equal(1, (int)running.ReconcileState);
        Assert.Equal(1u, running.RegisteredShards);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task PlacementAssignmentAck_Failure_With_Unknown_AssignmentId_Is_Ignored()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));
        PrimeEligibleWorker(root, hiveMind);

        var brainId = Guid.NewGuid();
        var placement = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 1,
                OutputWidth = 1
            });

        root.Send(hiveMind, new PlacementAssignmentAck
        {
            AssignmentId = "assign-1",
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = placement.PlacementEpoch,
            State = (PlacementAssignmentState)4,
            Accepted = false,
            Retryable = false,
            FailureReason = (PlacementFailureReason)3,
            Message = "rejected"
        });

        var status = await root.RequestAsync<PlacementLifecycleInfo>(
            hiveMind,
            new GetPlacementLifecycle
            {
                BrainId = brainId.ToProtoUuid()
            });

        Assert.NotEqual(PlacementLifecycleState.PlacementLifecycleFailed, status.LifecycleState);
        Assert.Equal(PlacementFailureReason.PlacementFailureNone, status.FailureReason);
        Assert.NotEqual(PlacementReconcileState.PlacementReconcileFailed, status.ReconcileState);

        await system.ShutdownAsync();
    }

    private static void PrimeEligibleWorker(IRootContext root, PID hiveMind)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nowMs,
            Workers =
            {
                BuildWorker(
                    Guid.NewGuid(),
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: string.Empty,
                    rootActorName: "region-host")
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
            LastSeenMs = (ulong)lastSeenMs,
            HasCapabilities = true,
            CapabilityTimeMs = (ulong)capabilityTimeMs,
            Capabilities = new ProtoSettings.NodeCapabilities
            {
                CpuCores = 8,
                RamFreeBytes = 4UL * 1024 * 1024 * 1024,
                CpuScore = 15f
            }
        };

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private sealed record SendMessage(PID Target, object Message);
    private sealed record SendMessageAck;

    private sealed class ManualSenderActor : IActor
    {
        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is not SendMessage send)
            {
                return Task.CompletedTask;
            }

            context.Request(send.Target, send.Message);
            context.Respond(new SendMessageAck());
            return Task.CompletedTask;
        }
    }

    private static HiveMindOptions CreateOptions()
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
            IoName: null);
}
