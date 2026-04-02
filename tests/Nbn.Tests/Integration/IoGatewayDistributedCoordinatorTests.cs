using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Runtime.IO;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;
using Xunit.Sdk;

namespace Nbn.Tests.Integration;

public sealed class IoGatewayDistributedCoordinatorTests
{
    [Fact]
    public async Task EnsureBrainEntry_Bootstraps_RemoteCoordinators_From_HiveMindMetadata()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 4)));
        var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 2)));
        var routerPid = root.Spawn(Props.FromProducer(() => new IoGatewayRegistrationProbeActor(brainId)));
        var hivePid = root.Spawn(Props.FromProducer(() => new BrainIoInfoHiveProbeActor(
            brainId,
            inputPid,
            outputPid,
            routerPid,
            inputWidth: 4,
            outputWidth: 2,
            inputMode: ProtoControl.InputCoordinatorMode.DirtyOnChange)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));
        var subscriberPid = root.Spawn(Props.FromProducer(() => new OutputSubscriberProbeActor()));

        root.Send(subscriberPid, new OutputSubscriberProbeActor.SubscribeGateway(gateway, brainId));

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<BrainIoInfoHiveProbeActor.Snapshot>(
                    hivePid,
                    new BrainIoInfoHiveProbeActor.GetSnapshot());
                return snapshot.RegisterOutputSinkCount > 0;
            },
            timeoutMs: 2_000);

        var preInputRouterSnapshot = await root.RequestAsync<IoGatewayRegistrationProbeActor.Snapshot>(
            routerPid,
            new IoGatewayRegistrationProbeActor.GetSnapshot());
        Assert.True(preInputRouterSnapshot.RegistrationCount >= 1);
        Assert.Equal(ProtoControl.InputCoordinatorMode.DirtyOnChange, preInputRouterSnapshot.LastInputCoordinatorMode);
        Assert.False(preInputRouterSnapshot.LastInputTickDrainArmed);
        Assert.False(preInputRouterSnapshot.ObservedArmedTickDrainRegistration);

        root.Send(gateway, new InputWrite
        {
            BrainId = brainId.ToProtoUuid(),
            InputIndex = 1,
            Value = 0.75f
        });

        var drain = await root.RequestAsync<InputDrain>(
            gateway,
            new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 7
            });

        var contribution = Assert.Single(drain.Contribs);
        Assert.Equal(1u, contribution.TargetNeuronId);
        Assert.Equal(0.75f, contribution.Value);

        var hiveSnapshot = await root.RequestAsync<BrainIoInfoHiveProbeActor.Snapshot>(
            hivePid,
            new BrainIoInfoHiveProbeActor.GetSnapshot());
        Assert.Equal(PidLabel(outputPid), hiveSnapshot.LastOutputSinkPid);

        var routerSnapshot = await root.RequestAsync<IoGatewayRegistrationProbeActor.Snapshot>(
            routerPid,
            new IoGatewayRegistrationProbeActor.GetSnapshot());
        Assert.Equal(3, routerSnapshot.RegistrationCount);
        Assert.False(string.IsNullOrWhiteSpace(routerSnapshot.LastIoGatewayPid));
        Assert.Equal(ProtoControl.InputCoordinatorMode.DirtyOnChange, routerSnapshot.LastInputCoordinatorMode);
        Assert.False(routerSnapshot.LastInputTickDrainArmed);
        Assert.True(routerSnapshot.ObservedArmedTickDrainRegistration);

        var bootstrapOutputTick = 8UL;
        await WaitForAsync(
            async () =>
            {
                root.Send(outputPid, new OutputEvent
                {
                    BrainId = brainId.ToProtoUuid(),
                    OutputIndex = 1,
                    Value = 0.9f,
                    TickId = bootstrapOutputTick
                });
                root.Send(outputPid, new OutputVectorEvent
                {
                    BrainId = brainId.ToProtoUuid(),
                    TickId = bootstrapOutputTick,
                    Values = { 0.1f, 0.9f }
                });
                bootstrapOutputTick++;

                var snapshot = await root.RequestAsync<OutputSubscriberProbeActor.Snapshot>(
                    subscriberPid,
                    new OutputSubscriberProbeActor.GetSnapshot());
                return snapshot.SingleCount >= 1 && snapshot.VectorCount >= 1;
            },
            timeoutMs: 2_000);

        var subscriberSnapshot = await root.RequestAsync<OutputSubscriberProbeActor.Snapshot>(
            subscriberPid,
            new OutputSubscriberProbeActor.GetSnapshot());
        Assert.NotNull(subscriberSnapshot.LastSingle);
        Assert.NotNull(subscriberSnapshot.LastVector);
        Assert.Equal(1u, subscriberSnapshot.LastSingle!.OutputIndex);
        Assert.Equal(0.9f, subscriberSnapshot.LastSingle.Value);
        Assert.Equal([0.1f, 0.9f], subscriberSnapshot.LastVector!.Values.ToArray());

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task BrainInfoRequest_Tolerates_Slow_HiveMindMetadataBootstrap()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 4)));
        var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 2)));
        var routerPid = root.Spawn(Props.FromProducer(() => new IoGatewayRegistrationProbeActor(brainId)));
        var hivePid = root.Spawn(Props.FromProducer(() => new BrainIoInfoHiveProbeActor(
            brainId,
            inputPid,
            outputPid,
            routerPid,
            inputWidth: 4,
            outputWidth: 2,
            inputMode: ProtoControl.InputCoordinatorMode.DirtyOnChange,
            brainIoInfoDelay: TimeSpan.FromMilliseconds(1500))));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));

        var info = await root.RequestAsync<BrainInfo>(
            gateway,
            new BrainInfoRequest { BrainId = brainId.ToProtoUuid() },
            TimeSpan.FromSeconds(5));

        Assert.Equal((uint)4, info.InputWidth);
        Assert.Equal((uint)2, info.OutputWidth);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task GetPlacementWorkerInventory_Proxies_HiveMindPlacementInventory()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var expectedWorkerNodeId = Guid.NewGuid();
        var expectedInventory = new ProtoControl.PlacementWorkerInventory
        {
            SnapshotMs = 321
        };
        expectedInventory.Workers.Add(new ProtoControl.PlacementWorkerInventoryEntry
        {
            WorkerNodeId = expectedWorkerNodeId.ToProtoUuid(),
            WorkerAddress = "127.0.0.1:12041",
            WorkerRootActorName = "worker-node",
            IsAlive = true,
            CpuScore = 25f,
            CpuLimitPercent = 100,
            RamFreeBytes = 2048,
            RamTotalBytes = 4096,
            ProcessRamUsedBytes = 256,
            RamLimitPercent = 100,
            StorageFreeBytes = 4096,
            StorageTotalBytes = 8192,
            StorageLimitPercent = 100
        });
        var hivePid = root.Spawn(Props.FromProducer(() => new PlacementWorkerInventoryProbeActor(expectedInventory)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));

        var result = await root.RequestAsync<PlacementWorkerInventoryResult>(
            gateway,
            new GetPlacementWorkerInventory(),
            TimeSpan.FromSeconds(5));

        Assert.True(result.Success);
        Assert.Equal(string.Empty, result.FailureReasonCode);
        Assert.Equal(string.Empty, result.FailureMessage);
        Assert.Equal((ulong)321, result.Inventory.SnapshotMs);
        var worker = Assert.Single(result.Inventory.Workers);
        Assert.True(worker.WorkerNodeId.TryToGuid(out var workerNodeId));
        Assert.Equal(expectedWorkerNodeId, workerNodeId);
        Assert.Equal("127.0.0.1:12041", worker.WorkerAddress);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task GetPlacementWorkerInventory_ReportsUnavailable_WhenHiveMindIsMissing()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: null)));

        var result = await root.RequestAsync<PlacementWorkerInventoryResult>(
            gateway,
            new GetPlacementWorkerInventory(),
            TimeSpan.FromSeconds(5));

        Assert.False(result.Success);
        Assert.Equal("capacity_unavailable", result.FailureReasonCode);
        Assert.Contains("HiveMind endpoint is not configured", result.FailureMessage, StringComparison.Ordinal);
        Assert.Empty(result.Inventory.Workers);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task EnsureBrainEntry_Bootstraps_ArtifactMetadata_From_HiveMindExports()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 4)));
        var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 2)));
        var routerPid = root.Spawn(Props.FromProducer(() => new IoGatewayRegistrationProbeActor(brainId)));
        var baseDefinition = new string('a', 64).ToArtifactRef(128, "application/x-nbn", "http://100.123.130.93:12091/");
        var snapshot = new string('b', 64).ToArtifactRef(64, "application/x-nbs", "http://100.123.130.93:12091/");
        var hivePid = root.Spawn(Props.FromProducer(() => new BrainIoInfoHiveProbeActor(
            brainId,
            inputPid,
            outputPid,
            routerPid,
            inputWidth: 4,
            outputWidth: 2,
            inputMode: ProtoControl.InputCoordinatorMode.DirtyOnChange,
            baseDefinition: baseDefinition,
            snapshot: snapshot)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));

        var info = await root.RequestAsync<BrainInfo>(
            gateway,
            new BrainInfoRequest { BrainId = brainId.ToProtoUuid() },
            TimeSpan.FromSeconds(5));

        Assert.Equal((uint)4, info.InputWidth);
        Assert.Equal((uint)2, info.OutputWidth);
        Assert.Equal(baseDefinition.ToSha256Hex(), info.BaseDefinition.ToSha256Hex());
        Assert.Equal(baseDefinition.StoreUri, info.BaseDefinition.StoreUri);
        Assert.Equal(snapshot.ToSha256Hex(), info.LastSnapshot.ToSha256Hex());
        Assert.Equal(snapshot.StoreUri, info.LastSnapshot.StoreUri);

        await system.ShutdownAsync();
    }

    private sealed class PlacementWorkerInventoryProbeActor : IActor
    {
        private readonly ProtoControl.PlacementWorkerInventory _inventory;

        public PlacementWorkerInventoryProbeActor(ProtoControl.PlacementWorkerInventory inventory)
        {
            _inventory = inventory.Clone();
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is ProtoControl.PlacementWorkerInventoryRequest)
            {
                context.Respond(_inventory.Clone());
            }

            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task RegisterBrain_Skips_Placeholder_OutputSink_Until_RemoteCoordinator_Is_Known()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 1)));
        var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 1)));
        var routerPid = root.Spawn(Props.FromProducer(() => new IoGatewayRegistrationProbeActor(brainId)));
        var hivePid = root.Spawn(Props.FromProducer(() => new BrainIoInfoHiveProbeActor(
            brainId,
            inputPid,
            outputPid,
            routerPid,
            inputWidth: 1,
            outputWidth: 1,
            inputMode: ProtoControl.InputCoordinatorMode.DirtyOnChange)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            InputCoordinatorPid = PidLabel(inputPid),
            IoGatewayOwnsInputCoordinator = false,
            IoGatewayOwnsOutputCoordinator = false
        });

        await Task.Delay(100);

        var placeholderSnapshot = await root.RequestAsync<BrainIoInfoHiveProbeActor.Snapshot>(
            hivePid,
            new BrainIoInfoHiveProbeActor.GetSnapshot());
        Assert.Equal(0, placeholderSnapshot.RegisterOutputSinkCount);
        Assert.Equal(string.Empty, placeholderSnapshot.LastOutputSinkPid);

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            InputCoordinatorPid = PidLabel(inputPid),
            OutputCoordinatorPid = PidLabel(outputPid),
            IoGatewayOwnsInputCoordinator = false,
            IoGatewayOwnsOutputCoordinator = false
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<BrainIoInfoHiveProbeActor.Snapshot>(
                    hivePid,
                    new BrainIoInfoHiveProbeActor.GetSnapshot());
                return snapshot.RegisterOutputSinkCount > 0;
            },
            timeoutMs: 2_000);

        var remoteSnapshot = await root.RequestAsync<BrainIoInfoHiveProbeActor.Snapshot>(
            hivePid,
            new BrainIoInfoHiveProbeActor.GetSnapshot());
        Assert.Equal(PidLabel(outputPid), remoteSnapshot.LastOutputSinkPid);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RegisterBrain_Registers_Local_OutputSink_When_GatewayOwnsCoordinator()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 1)));
        var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 1)));
        var routerPid = root.Spawn(Props.FromProducer(() => new IoGatewayRegistrationProbeActor(brainId)));
        var hivePid = root.Spawn(Props.FromProducer(() => new BrainIoInfoHiveProbeActor(
            brainId,
            inputPid,
            outputPid,
            routerPid,
            inputWidth: 1,
            outputWidth: 1,
            inputMode: ProtoControl.InputCoordinatorMode.DirtyOnChange)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            IoGatewayOwnsInputCoordinator = true,
            IoGatewayOwnsOutputCoordinator = true
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<BrainIoInfoHiveProbeActor.Snapshot>(
                    hivePid,
                    new BrainIoInfoHiveProbeActor.GetSnapshot());
                return snapshot.RegisterOutputSinkCount > 0;
            },
            timeoutMs: 2_000);

        var snapshot = await root.RequestAsync<BrainIoInfoHiveProbeActor.Snapshot>(
            hivePid,
            new BrainIoInfoHiveProbeActor.GetSnapshot());
        Assert.Contains(IoNames.OutputCoordinatorPrefix, snapshot.LastOutputSinkPid);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RegisterBrain_Replays_InputState_And_OutputSubscriptions_Across_CoordinatorMoves()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPidA = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 3)));
        var outputPidA = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 3)));
        var inputPidB = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 3)));
        var outputPidB = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 3)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));
        var subscriberPid = root.Spawn(Props.FromProducer(() => new OutputSubscriberProbeActor()));

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 3,
            OutputWidth = 3,
            InputCoordinatorMode = ProtoControl.InputCoordinatorMode.ReplayLatestVector,
            InputCoordinatorPid = PidLabel(inputPidA),
            OutputCoordinatorPid = PidLabel(outputPidA),
            IoGatewayOwnsInputCoordinator = false,
            IoGatewayOwnsOutputCoordinator = false
        });
        root.Send(subscriberPid, new OutputSubscriberProbeActor.SubscribeGateway(gateway, brainId));
        root.Send(gateway, new InputVector
        {
            BrainId = brainId.ToProtoUuid(),
            Values = { 0.25f, 0.5f, 0.75f }
        });

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 3,
            OutputWidth = 3,
            InputCoordinatorMode = ProtoControl.InputCoordinatorMode.ReplayLatestVector,
            InputCoordinatorPid = PidLabel(inputPidB),
            OutputCoordinatorPid = PidLabel(outputPidB),
            IoGatewayOwnsInputCoordinator = false,
            IoGatewayOwnsOutputCoordinator = false
        });

        var drain = await root.RequestAsync<InputDrain>(
            gateway,
            new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 42
            });

        Assert.Equal(3, drain.Contribs.Count);
        Assert.Equal([0.25f, 0.5f, 0.75f], drain.Contribs.Select(static contrib => contrib.Value).ToArray());

        var movedOutputTick = 43UL;
        await WaitForAsync(
            async () =>
            {
                root.Send(outputPidB, new OutputEvent
                {
                    BrainId = brainId.ToProtoUuid(),
                    OutputIndex = 2,
                    Value = 0.6f,
                    TickId = movedOutputTick
                });
                root.Send(outputPidB, new OutputVectorEvent
                {
                    BrainId = brainId.ToProtoUuid(),
                    TickId = movedOutputTick,
                    Values = { 0.2f, 0.4f, 0.6f }
                });
                movedOutputTick++;

                var snapshot = await root.RequestAsync<OutputSubscriberProbeActor.Snapshot>(
                    subscriberPid,
                    new OutputSubscriberProbeActor.GetSnapshot());
                return snapshot.SingleCount >= 1 && snapshot.VectorCount >= 1;
            },
            timeoutMs: 2_000);

        var subscriberSnapshot = await root.RequestAsync<OutputSubscriberProbeActor.Snapshot>(
            subscriberPid,
            new OutputSubscriberProbeActor.GetSnapshot());
        Assert.Equal(2u, subscriberSnapshot.LastSingle!.OutputIndex);
        Assert.Equal(0.6f, subscriberSnapshot.LastSingle.Value);
        Assert.Equal([0.2f, 0.4f, 0.6f], subscriberSnapshot.LastVector!.Values.ToArray());

        root.Send(gateway, new UnregisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "cleanup"
        });

        root.Send(inputPidB, new InputWrite
        {
            BrainId = brainId.ToProtoUuid(),
            InputIndex = 0,
            Value = 1f
        });

        var directDrain = await root.RequestAsync<InputDrain>(
            inputPidB,
            new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 99
            },
            TimeSpan.FromSeconds(1));
        Assert.Equal(3, directDrain.Contribs.Count);
        Assert.Equal(1f, directDrain.Contribs[0].Value);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RegisterBrain_DoesNotReplay_RejectedNonFiniteInputVector_Across_CoordinatorMoves()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPidA = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 3)));
        var outputPidA = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 1)));
        var inputPidB = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 3)));
        var outputPidB = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 1)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 3,
            OutputWidth = 1,
            InputCoordinatorMode = ProtoControl.InputCoordinatorMode.ReplayLatestVector,
            InputCoordinatorPid = PidLabel(inputPidA),
            OutputCoordinatorPid = PidLabel(outputPidA),
            IoGatewayOwnsInputCoordinator = false,
            IoGatewayOwnsOutputCoordinator = false
        });
        root.Send(gateway, new InputVector
        {
            BrainId = brainId.ToProtoUuid(),
            Values = { 0.25f, 0.5f, 0.75f }
        });
        root.Send(gateway, new InputVector
        {
            BrainId = brainId.ToProtoUuid(),
            Values = { 1f, float.NaN, 3f }
        });

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 3,
            OutputWidth = 1,
            InputCoordinatorMode = ProtoControl.InputCoordinatorMode.ReplayLatestVector,
            InputCoordinatorPid = PidLabel(inputPidB),
            OutputCoordinatorPid = PidLabel(outputPidB),
            IoGatewayOwnsInputCoordinator = false,
            IoGatewayOwnsOutputCoordinator = false
        });

        var drain = await root.RequestAsync<InputDrain>(
            gateway,
            new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 42
            });

        Assert.Equal([0.25f, 0.5f, 0.75f], drain.Contribs.Select(static contrib => contrib.Value).ToArray());

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task DrainInputs_Replaces_Stale_RemoteInputCoordinator_When_HiveMind_Drops_InputPid()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(
            brainId,
            3,
            ProtoControl.InputCoordinatorMode.ReplayLatestVector)));
        var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 1)));
        var routerPid = root.Spawn(Props.FromProducer(() => new InputRouterProbeActor(brainId)));
        var hivePid = root.Spawn(Props.FromProducer(() => new BrainIoInfoHiveProbeActor(
            brainId,
            inputPid,
            outputPid,
            routerPid,
            inputWidth: 3,
            outputWidth: 1,
            inputMode: ProtoControl.InputCoordinatorMode.ReplayLatestVector)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));

        root.Send(gateway, new InputVector
        {
            BrainId = brainId.ToProtoUuid(),
            Values = { 0.25f, 0.5f, 0.75f }
        });

        await WaitForAsync(
            async () =>
            {
                var seededDrain = await root.RequestAsync<InputDrain>(
                    inputPid,
                    new DrainInputs
                    {
                        BrainId = brainId.ToProtoUuid(),
                        TickId = 1
                    });
                return seededDrain.Contribs.Select(static contrib => contrib.Value).SequenceEqual([0.25f, 0.5f, 0.75f]);
            },
            timeoutMs: 2_000);

        root.Stop(inputPid);
        await Task.Delay(100);
        root.Send(hivePid, new BrainIoInfoHiveProbeActor.UpdateInputCoordinator(null));

        var recoveredDrain = await root.RequestAsync<InputDrain>(
            gateway,
            new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 42
            },
            TimeSpan.FromSeconds(5));

        Assert.Equal([0.25f, 0.5f, 0.75f], recoveredDrain.Contribs.Select(static contrib => contrib.Value).ToArray());

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ApplyBrainRuntimeResetAtBarrier_ClearsInputCoordinatorState_Across_CoordinatorMoves()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPidA = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(
            brainId,
            3,
            ProtoControl.InputCoordinatorMode.ReplayLatestVector)));
        var outputPidA = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 1)));
        var inputPidB = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(
            brainId,
            3,
            ProtoControl.InputCoordinatorMode.ReplayLatestVector)));
        var outputPidB = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 1)));
        var routerPid = root.Spawn(Props.FromProducer(() => new InputRouterProbeActor(brainId)));
        var hivePid = root.Spawn(Props.FromProducer(() => new BrainIoInfoHiveProbeActor(
            brainId,
            inputPidA,
            outputPidA,
            routerPid,
            inputWidth: 3,
            outputWidth: 1,
            inputMode: ProtoControl.InputCoordinatorMode.ReplayLatestVector)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 3,
            OutputWidth = 1,
            InputCoordinatorMode = ProtoControl.InputCoordinatorMode.ReplayLatestVector,
            InputCoordinatorPid = PidLabel(inputPidA),
            OutputCoordinatorPid = PidLabel(outputPidA),
            IoGatewayOwnsInputCoordinator = false,
            IoGatewayOwnsOutputCoordinator = false
        });
        root.Send(gateway, new InputVector
        {
            BrainId = brainId.ToProtoUuid(),
            Values = { 0.25f, 0.5f, 0.75f }
        });

        var resetAck = await root.RequestAsync<IoCommandAck>(
            gateway,
            new Nbn.Shared.HiveMind.ApplyBrainRuntimeResetAtBarrier(brainId, ResetBuffer: true, ResetAccumulator: true));
        Assert.True(resetAck.Success);

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 3,
            OutputWidth = 1,
            InputCoordinatorMode = ProtoControl.InputCoordinatorMode.ReplayLatestVector,
            InputCoordinatorPid = PidLabel(inputPidB),
            OutputCoordinatorPid = PidLabel(outputPidB),
            IoGatewayOwnsInputCoordinator = false,
            IoGatewayOwnsOutputCoordinator = false
        });

        var drain = await root.RequestAsync<InputDrain>(
            gateway,
            new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 77
            });

        Assert.Equal([0f, 0f, 0f], drain.Contribs.Select(static contrib => contrib.Value).ToArray());

        var routerSnapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
            routerPid,
            new InputRouterProbeActor.GetSnapshot());
        Assert.Equal(1, routerSnapshot.ResetCount);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ResetBrainRuntimeState_StaysReentrant_During_ConcurrentMultiBrainBarrierCallbacks()
    {
        const int brainCount = 4;

        var system = new ActorSystem();
        var root = system.Root;
        var brains = new List<(Guid BrainId, PID InputPid, PID RouterPid)>(brainCount);
        var routes = new List<ConcurrentBarrierResetHiveProbeActor.BrainRoute>(brainCount);

        for (var i = 0; i < brainCount; i++)
        {
            var brainId = Guid.NewGuid();
            var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(
                brainId,
                2,
                ProtoControl.InputCoordinatorMode.ReplayLatestVector)));
            var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 1)));
            var routerPid = root.Spawn(Props.FromProducer(() => new InputRouterProbeActor(brainId)));

            brains.Add((brainId, inputPid, routerPid));
            routes.Add(new ConcurrentBarrierResetHiveProbeActor.BrainRoute(
                brainId,
                inputPid,
                outputPid,
                routerPid,
                InputWidth: 2,
                OutputWidth: 1,
                InputMode: ProtoControl.InputCoordinatorMode.ReplayLatestVector));
        }

        var hivePid = root.Spawn(Props.FromProducer(() => new ConcurrentBarrierResetHiveProbeActor(routes, brainCount)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));
        root.Send(hivePid, new ConcurrentBarrierResetHiveProbeActor.UpdateGateway(gateway));

        foreach (var brain in brains)
        {
            root.Send(gateway, new RegisterBrain
            {
                BrainId = brain.BrainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 1,
                InputCoordinatorMode = ProtoControl.InputCoordinatorMode.ReplayLatestVector,
                InputCoordinatorPid = PidLabel(brain.InputPid),
                OutputCoordinatorPid = PidLabel(routes.Single(route => route.BrainId == brain.BrainId).OutputCoordinatorPid),
                IoGatewayOwnsInputCoordinator = false,
                IoGatewayOwnsOutputCoordinator = false
            });
            var info = await root.RequestAsync<BrainInfo>(
                gateway,
                new BrainInfoRequest { BrainId = brain.BrainId.ToProtoUuid() },
                TimeSpan.FromSeconds(5));
            Assert.Equal((uint)2, info.InputWidth);
            Assert.Equal((uint)1, info.OutputWidth);

            root.Send(brain.InputPid, new InputVector
            {
                BrainId = brain.BrainId.ToProtoUuid(),
                Values = { 0.25f, 0.5f }
            });
        }

        await WaitForAsync(
            async () =>
            {
                foreach (var brain in brains)
                {
                    var drain = await root.RequestAsync<InputDrain>(
                        brain.InputPid,
                        new DrainInputs
                        {
                            BrainId = brain.BrainId.ToProtoUuid(),
                            TickId = 1
                        },
                        TimeSpan.FromSeconds(5));
                    if (!drain.Contribs.Select(static contrib => contrib.Value).SequenceEqual([0.25f, 0.5f]))
                    {
                        return false;
                    }
                }

                return true;
            },
            timeoutMs: 2_000);

        var resetTasks = brains.Select(
                brain => root.RequestAsync<IoCommandAck>(
                    gateway,
                    new ResetBrainRuntimeState
                    {
                        BrainId = brain.BrainId.ToProtoUuid(),
                        ResetBuffer = true,
                        ResetAccumulator = true
                    },
                    TimeSpan.FromSeconds(5)))
            .ToArray();

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<ConcurrentBarrierResetHiveProbeActor.Snapshot>(
                    hivePid,
                    new ConcurrentBarrierResetHiveProbeActor.GetSnapshot());
                return snapshot.PendingCount == brainCount;
            },
            timeoutMs: 2_000);

        Assert.All(resetTasks, static task => Assert.False(task.IsCompleted));

        await root.RequestAsync<ConcurrentBarrierResetHiveProbeActor.ReleaseBatchAck>(
            hivePid,
            new ConcurrentBarrierResetHiveProbeActor.ReleaseBatch());

        var resetAcks = await Task.WhenAll(resetTasks).WaitAsync(TimeSpan.FromSeconds(5));
        foreach (var ack in resetAcks)
        {
            Assert.True(ack.Success, ack.Message);
            Assert.DoesNotContain("hivemind_request_failed", ack.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("runtime_reset_failed", ack.Message, StringComparison.Ordinal);
        }

        foreach (var brain in brains)
        {
            var routerSnapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
                brain.RouterPid,
                new InputRouterProbeActor.GetSnapshot());
            Assert.Equal(1, routerSnapshot.ResetCount);

            var drain = await root.RequestAsync<InputDrain>(
                brain.InputPid,
                new DrainInputs
                {
                    BrainId = brain.BrainId.ToProtoUuid(),
                    TickId = 99
                });
            Assert.Equal([0f, 0f], drain.Contribs.Select(static contrib => contrib.Value).ToArray());
        }

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task QueuedOutputUnsubscribe_BeforeBrainRegistration_Prevents_ReplayedSubscription()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 1)));
        var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 1)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));
        var subscriberPid = root.Spawn(Props.FromProducer(() => new OutputSubscriberProbeActor()));
        var subscriberActor = PidLabel(subscriberPid);

        root.Send(gateway, new SubscribeOutputs
        {
            BrainId = brainId.ToProtoUuid(),
            SubscriberActor = subscriberActor
        });
        root.Send(gateway, new SubscribeOutputsVector
        {
            BrainId = brainId.ToProtoUuid(),
            SubscriberActor = subscriberActor
        });
        root.Send(gateway, new UnsubscribeOutputs
        {
            BrainId = brainId.ToProtoUuid(),
            SubscriberActor = subscriberActor
        });
        root.Send(gateway, new UnsubscribeOutputsVector
        {
            BrainId = brainId.ToProtoUuid(),
            SubscriberActor = subscriberActor
        });

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            InputCoordinatorPid = PidLabel(inputPid),
            OutputCoordinatorPid = PidLabel(outputPid),
            IoGatewayOwnsInputCoordinator = false,
            IoGatewayOwnsOutputCoordinator = false
        });

        await WaitForAsync(
            async () =>
            {
                var info = await root.RequestAsync<BrainInfo>(
                    gateway,
                    new BrainInfoRequest { BrainId = brainId.ToProtoUuid() });
                return info.OutputWidth == 1;
            },
            timeoutMs: 2_000);

        root.Send(outputPid, new OutputEvent
        {
            BrainId = brainId.ToProtoUuid(),
            OutputIndex = 0,
            Value = 0.9f,
            TickId = 1
        });
        root.Send(outputPid, new OutputVectorEvent
        {
            BrainId = brainId.ToProtoUuid(),
            TickId = 1,
            Values = { 0.9f }
        });

        await Task.Delay(150);

        var snapshot = await root.RequestAsync<OutputSubscriberProbeActor.Snapshot>(
            subscriberPid,
            new OutputSubscriberProbeActor.GetSnapshot());
        Assert.Equal(0, snapshot.SingleCount);
        Assert.Equal(0, snapshot.VectorCount);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ForwardInput_Refreshes_RouterRegistration_When_BrainRouting_Changes()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 1)));
        var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 1)));
        var routerA = root.Spawn(Props.FromProducer(() => new InputRouterProbeActor(brainId)));
        var routerB = root.Spawn(Props.FromProducer(() => new InputRouterProbeActor(brainId)));
        var hivePid = root.Spawn(Props.FromProducer(() => new BrainIoInfoHiveProbeActor(
            brainId,
            inputPid,
            outputPid,
            routerA,
            inputWidth: 1,
            outputWidth: 1,
            inputMode: ProtoControl.InputCoordinatorMode.ReplayLatestVector)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));

        root.Send(gateway, new InputVector
        {
            BrainId = brainId.ToProtoUuid(),
            Values = { 0.25f }
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
                    routerA,
                    new InputRouterProbeActor.GetSnapshot());
                return snapshot.InputVectorCount >= 1 && snapshot.RegisterIoGatewayCount >= 1;
            },
            timeoutMs: 2_000);

        root.Send(hivePid, new BrainIoInfoHiveProbeActor.UpdateRouter(routerB));

        root.Send(gateway, new InputVector
        {
            BrainId = brainId.ToProtoUuid(),
            Values = { 0.5f }
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
                    routerB,
                    new InputRouterProbeActor.GetSnapshot());
                return snapshot.InputVectorCount >= 1 && snapshot.RegisterIoGatewayCount >= 1;
            },
            timeoutMs: 2_000);

        var routerASnapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
            routerA,
            new InputRouterProbeActor.GetSnapshot());
        var routerBSnapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
            routerB,
            new InputRouterProbeActor.GetSnapshot());

        Assert.Equal(1, routerASnapshot.InputVectorCount);
        Assert.Equal(1, routerBSnapshot.InputVectorCount);
        Assert.Equal(1, routerASnapshot.RegisterIoGatewayCount);
        Assert.Equal(1, routerBSnapshot.RegisterIoGatewayCount);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ForwardInput_ReRegisters_IoGateway_When_Same_Router_Forgets_Registration()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 1)));
        var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 1)));
        var router = root.Spawn(Props.FromProducer(() => new InputRouterProbeActor(brainId)));
        var hivePid = root.Spawn(Props.FromProducer(() => new BrainIoInfoHiveProbeActor(
            brainId,
            inputPid,
            outputPid,
            router,
            inputWidth: 1,
            outputWidth: 1,
            inputMode: ProtoControl.InputCoordinatorMode.ReplayLatestVector)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));

        root.Send(gateway, new InputVector
        {
            BrainId = brainId.ToProtoUuid(),
            Values = { 0.25f }
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
                    router,
                    new InputRouterProbeActor.GetSnapshot());
                return snapshot.InputVectorCount >= 1
                    && snapshot.RegisterIoGatewayCount >= 1
                    && snapshot.HasIoGatewayRegistration;
            },
            timeoutMs: 2_000);

        root.Send(router, new InputRouterProbeActor.ForgetIoGatewayRegistration());

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
                    router,
                    new InputRouterProbeActor.GetSnapshot());
                return !snapshot.HasIoGatewayRegistration;
            },
            timeoutMs: 2_000);

        root.Send(gateway, new InputVector
        {
            BrainId = brainId.ToProtoUuid(),
            Values = { 0.5f }
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
                    router,
                    new InputRouterProbeActor.GetSnapshot());
                return snapshot.InputVectorCount >= 2
                    && snapshot.RegisterIoGatewayCount >= 2
                    && snapshot.HasIoGatewayRegistration;
            },
            timeoutMs: 2_000);

        var routerSnapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
            router,
            new InputRouterProbeActor.GetSnapshot());
        Assert.Equal(2, routerSnapshot.InputVectorCount);
        Assert.Equal(2, routerSnapshot.RegisterIoGatewayCount);
        Assert.True(routerSnapshot.HasIoGatewayRegistration);

        await system.ShutdownAsync();
    }

    private static IoOptions CreateOptions()
        => new(
            BindHost: "127.0.0.1",
            Port: 0,
            AdvertisedHost: null,
            AdvertisedPort: null,
            GatewayName: "io-gateway",
            ServerName: "nbn.io.tests",
            SettingsHost: null,
            SettingsPort: 0,
            SettingsName: "SettingsMonitor",
            HiveMindAddress: null,
            HiveMindName: null,
            ReproAddress: null,
            ReproName: null,
            SpeciationAddress: null,
            SpeciationName: null);

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

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

    private sealed class BrainIoInfoHiveProbeActor : IActor
    {
        private readonly Guid _brainId;
        private PID? _inputCoordinatorPid;
        private PID? _outputCoordinatorPid;
        private PID _routerPid;
        private readonly uint _inputWidth;
        private readonly uint _outputWidth;
        private readonly ProtoControl.InputCoordinatorMode _inputMode;
        private readonly TimeSpan _brainIoInfoDelay;
        private readonly ArtifactRef? _baseDefinition;
        private readonly ArtifactRef? _snapshot;
        private int _registerOutputSinkCount;
        private string _lastOutputSinkPid = string.Empty;

        public BrainIoInfoHiveProbeActor(
            Guid brainId,
            PID? inputCoordinatorPid,
            PID? outputCoordinatorPid,
            PID routerPid,
            uint inputWidth,
            uint outputWidth,
            ProtoControl.InputCoordinatorMode inputMode,
            TimeSpan? brainIoInfoDelay = null,
            ArtifactRef? baseDefinition = null,
            ArtifactRef? snapshot = null)
        {
            _brainId = brainId;
            _inputCoordinatorPid = inputCoordinatorPid;
            _outputCoordinatorPid = outputCoordinatorPid;
            _routerPid = routerPid;
            _inputWidth = inputWidth;
            _outputWidth = outputWidth;
            _inputMode = inputMode;
            _brainIoInfoDelay = brainIoInfoDelay ?? TimeSpan.Zero;
            _baseDefinition = baseDefinition?.Clone();
            _snapshot = snapshot?.Clone();
        }

        public sealed record GetSnapshot;

        public sealed record UpdateRouter(PID RouterPid);

        public sealed record UpdateInputCoordinator(PID? InputCoordinatorPid);

        public sealed record UpdateOutputCoordinator(PID? OutputCoordinatorPid);

        public sealed record Snapshot(int RegisterOutputSinkCount, string LastOutputSinkPid);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case ProtoControl.GetBrainIoInfo request when request.BrainId.TryToGuid(out var requestBrainId) && requestBrainId == _brainId:
                    if (_brainIoInfoDelay > TimeSpan.Zero)
                    {
                        return RespondBrainIoInfoAsync(context, request);
                    }

                    context.Respond(BuildBrainIoInfo(request.BrainId));
                    break;
                case ProtoControl.GetBrainRouting request when request.BrainId.TryToGuid(out var routingBrainId) && routingBrainId == _brainId:
                    context.Respond(new ProtoControl.BrainRoutingInfo
                    {
                        BrainId = request.BrainId,
                        SignalRouterPid = PidLabel(_routerPid)
                    });
                    break;
                case UpdateRouter update:
                    _routerPid = update.RouterPid;
                    break;
                case UpdateInputCoordinator update:
                    _inputCoordinatorPid = update.InputCoordinatorPid;
                    break;
                case UpdateOutputCoordinator update:
                    _outputCoordinatorPid = update.OutputCoordinatorPid;
                    break;
                case ProtoControl.RegisterOutputSink register when register.BrainId.TryToGuid(out var registeredBrainId) && registeredBrainId == _brainId:
                    _registerOutputSinkCount++;
                    _lastOutputSinkPid = register.OutputPid ?? string.Empty;
                    break;
                case Nbn.Shared.HiveMind.RequestBrainRuntimeReset reset when reset.BrainId == _brainId:
                    return HandleRequestBrainRuntimeResetAsync(context, reset);
                case ExportBrainDefinition export when export.BrainId.TryToGuid(out var exportBrainId) && exportBrainId == _brainId:
                    context.Respond(new BrainDefinitionReady
                    {
                        BrainId = export.BrainId,
                        BrainDef = _baseDefinition?.Clone() ?? new ArtifactRef()
                    });
                    break;
                case RequestSnapshot snapshotRequest when snapshotRequest.BrainId.TryToGuid(out var snapshotBrainId) && snapshotBrainId == _brainId:
                    context.Respond(new SnapshotReady
                    {
                        BrainId = snapshotRequest.BrainId,
                        Snapshot = _snapshot?.Clone() ?? new ArtifactRef()
                    });
                    break;
                case GetSnapshot:
                    context.Respond(new Snapshot(_registerOutputSinkCount, _lastOutputSinkPid));
                    break;
            }

            return Task.CompletedTask;
        }

        private async Task RespondBrainIoInfoAsync(IContext context, ProtoControl.GetBrainIoInfo request)
        {
            await Task.Delay(_brainIoInfoDelay).ConfigureAwait(false);
            context.Respond(BuildBrainIoInfo(request.BrainId));
        }

        private async Task HandleRequestBrainRuntimeResetAsync(IContext context, Nbn.Shared.HiveMind.RequestBrainRuntimeReset message)
        {
            if (context.Sender is null)
            {
                context.Respond(new IoCommandAck
                {
                    BrainId = message.BrainId.ToProtoUuid(),
                    Command = "reset_brain_runtime_state",
                    Success = false,
                    Message = "gateway_sender_missing"
                });
                return;
            }

            var ack = await context.RequestAsync<IoCommandAck>(
                    context.Sender,
                    new Nbn.Shared.HiveMind.ApplyBrainRuntimeResetAtBarrier(message.BrainId, message.ResetBuffer, message.ResetAccumulator),
                    TimeSpan.FromSeconds(2))
                .ConfigureAwait(false);

            context.Respond(ack ?? new IoCommandAck
            {
                BrainId = message.BrainId.ToProtoUuid(),
                Command = "reset_brain_runtime_state",
                Success = false,
                Message = "gateway_reset_empty_response"
            });
        }

        private ProtoControl.BrainIoInfo BuildBrainIoInfo(Nbn.Proto.Uuid brainId)
            => new()
            {
                BrainId = brainId,
                InputWidth = _inputWidth,
                OutputWidth = _outputWidth,
                InputCoordinatorMode = _inputMode,
                OutputVectorSource = ProtoControl.OutputVectorSource.Potential,
                InputCoordinatorPid = _inputCoordinatorPid is null ? string.Empty : PidLabel(_inputCoordinatorPid),
                OutputCoordinatorPid = _outputCoordinatorPid is null ? string.Empty : PidLabel(_outputCoordinatorPid),
                IoGatewayOwnsInputCoordinator = false,
                IoGatewayOwnsOutputCoordinator = false
            };
    }

    private sealed class InputRouterProbeActor : IActor
    {
        private readonly Guid _brainId;
        private int _inputVectorCount;
        private int _registerIoGatewayCount;
        private int _resetCount;
        private bool _hasIoGatewayRegistration;

        public InputRouterProbeActor(Guid brainId)
        {
            _brainId = brainId;
        }

        public sealed record GetSnapshot;
        public sealed record ForgetIoGatewayRegistration;

        public sealed record Snapshot(int InputVectorCount, int RegisterIoGatewayCount, bool HasIoGatewayRegistration, int ResetCount);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case InputVector input when input.BrainId.TryToGuid(out var brainId) && brainId == _brainId:
                    _inputVectorCount++;
                    break;
                case ResetBrainRuntimeState reset
                    when reset.BrainId.TryToGuid(out var resetBrainId)
                         && resetBrainId == _brainId:
                    _resetCount++;
                    context.Respond(new IoCommandAck
                    {
                        BrainId = reset.BrainId,
                        Command = "reset_brain_runtime_state",
                        Success = true,
                        Message = "router_reset_applied"
                    });
                    break;
                case RegisterIoGateway register when register.BrainId.TryToGuid(out var registeredBrainId) && registeredBrainId == _brainId:
                    _registerIoGatewayCount++;
                    _hasIoGatewayRegistration = true;
                    break;
                case ForgetIoGatewayRegistration:
                    _hasIoGatewayRegistration = false;
                    break;
                case GetSnapshot:
                    context.Respond(new Snapshot(_inputVectorCount, _registerIoGatewayCount, _hasIoGatewayRegistration, _resetCount));
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class ConcurrentBarrierResetHiveProbeActor : IActor
    {
        private readonly Dictionary<Guid, BrainRoute> _brains;
        private readonly int _expectedResetCount;
        private readonly List<PendingReset> _pendingResets = new();
        private PID? _gatewayPid;
        private bool _batchReleased;

        public ConcurrentBarrierResetHiveProbeActor(IEnumerable<BrainRoute> brains, int expectedResetCount)
        {
            _brains = brains.ToDictionary(static route => route.BrainId);
            _expectedResetCount = expectedResetCount;
        }

        public sealed record BrainRoute(
            Guid BrainId,
            PID InputCoordinatorPid,
            PID OutputCoordinatorPid,
            PID RouterPid,
            uint InputWidth,
            uint OutputWidth,
            ProtoControl.InputCoordinatorMode InputMode);

        private sealed class PendingReset
        {
            public PendingReset(Nbn.Shared.HiveMind.RequestBrainRuntimeReset request)
            {
                Request = request;
                Completion = new TaskCompletionSource<IoCommandAck>(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            public Nbn.Shared.HiveMind.RequestBrainRuntimeReset Request { get; }

            public TaskCompletionSource<IoCommandAck> Completion { get; }
        }

        public sealed record GetSnapshot;

        public sealed record Snapshot(int PendingCount, bool BatchReleased);

        public sealed record ReleaseBatch;

        public sealed record ReleaseBatchAck;

        public sealed record UpdateGateway(PID GatewayPid);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case UpdateGateway update:
                    _gatewayPid = update.GatewayPid;
                    break;
                case ProtoControl.GetBrainIoInfo request
                    when request.BrainId.TryToGuid(out var infoBrainId)
                         && _brains.TryGetValue(infoBrainId, out var infoRoute):
                    context.Respond(new ProtoControl.BrainIoInfo
                    {
                        BrainId = request.BrainId,
                        InputWidth = infoRoute.InputWidth,
                        OutputWidth = infoRoute.OutputWidth,
                        InputCoordinatorMode = infoRoute.InputMode,
                        OutputVectorSource = ProtoControl.OutputVectorSource.Potential,
                        InputCoordinatorPid = PidLabel(infoRoute.InputCoordinatorPid),
                        OutputCoordinatorPid = PidLabel(infoRoute.OutputCoordinatorPid),
                        IoGatewayOwnsInputCoordinator = false,
                        IoGatewayOwnsOutputCoordinator = false
                    });
                    break;
                case ProtoControl.GetBrainRouting request
                    when request.BrainId.TryToGuid(out var routingBrainId)
                         && _brains.TryGetValue(routingBrainId, out var routingRoute):
                    context.Respond(new ProtoControl.BrainRoutingInfo
                    {
                        BrainId = request.BrainId,
                        SignalRouterPid = PidLabel(routingRoute.RouterPid)
                    });
                    break;
                case Nbn.Shared.HiveMind.RequestBrainRuntimeReset reset
                    when _brains.ContainsKey(reset.BrainId):
                    if (context.Sender is null)
                    {
                        context.Respond(new IoCommandAck
                        {
                            BrainId = reset.BrainId.ToProtoUuid(),
                            Command = "reset_brain_runtime_state",
                            Success = false,
                            Message = "gateway_sender_missing"
                        });
                        break;
                    }

                    var pendingReset = new PendingReset(reset);
                    _pendingResets.Add(pendingReset);
                    context.ReenterAfter(
                        pendingReset.Completion.Task,
                        task =>
                        {
                            if (task.IsCompletedSuccessfully)
                            {
                                context.Respond(task.Result);
                            }
                            else
                            {
                                context.Respond(new IoCommandAck
                                {
                                    BrainId = reset.BrainId.ToProtoUuid(),
                                    Command = "reset_brain_runtime_state",
                                    Success = false,
                                    Message = $"gateway_apply_failed:{task.Exception?.GetBaseException().Message ?? "unknown_error"}"
                                });
                            }

                            return Task.CompletedTask;
                        });
                    break;
                case ReleaseBatch:
                    if (!_batchReleased && _pendingResets.Count == _expectedResetCount)
                    {
                        _batchReleased = true;
                        var pending = _pendingResets.ToArray();
                        context.ReenterAfter(
                            ApplyPendingResetsAsync(context, _gatewayPid, pending),
                            task =>
                            {
                                if (task.IsCompletedSuccessfully)
                                {
                                    foreach (var result in task.Result)
                                    {
                                        result.Pending.Completion.TrySetResult(result.Ack);
                                    }
                                }
                                else
                                {
                                    var detail = task.Exception?.GetBaseException().Message ?? "unknown_error";
                                    foreach (var item in pending)
                                    {
                                        item.Completion.TrySetResult(new IoCommandAck
                                        {
                                            BrainId = item.Request.BrainId.ToProtoUuid(),
                                            Command = "reset_brain_runtime_state",
                                            Success = false,
                                            Message = $"gateway_apply_failed:{detail}"
                                        });
                                    }
                                }

                                _pendingResets.Clear();
                                return Task.CompletedTask;
                            });
                    }

                    context.Respond(new ReleaseBatchAck());
                    break;
                case GetSnapshot:
                    context.Respond(new Snapshot(_pendingResets.Count, _batchReleased));
                    break;
            }

            return Task.CompletedTask;
        }

        private static async Task<(PendingReset Pending, IoCommandAck Ack)[]> ApplyPendingResetsAsync(
            IContext context,
            PID? gatewayPid,
            IReadOnlyList<PendingReset> pending)
        {
            if (gatewayPid is null)
            {
                return pending.Select(
                        item => (
                            item,
                            new IoCommandAck
                            {
                                BrainId = item.Request.BrainId.ToProtoUuid(),
                                Command = "reset_brain_runtime_state",
                                Success = false,
                                Message = "gateway_pid_missing"
                            }))
                    .ToArray();
            }

            return await Task.WhenAll(
                    pending.Select(
                        async item =>
                        {
                            try
                            {
                                var ack = await context.RequestAsync<IoCommandAck>(
                                        gatewayPid,
                                        new Nbn.Shared.HiveMind.ApplyBrainRuntimeResetAtBarrier(
                                            item.Request.BrainId,
                                            item.Request.ResetBuffer,
                                            item.Request.ResetAccumulator),
                                        TimeSpan.FromSeconds(2))
                                    .ConfigureAwait(false);

                                return (
                                    item,
                                    ack ?? new IoCommandAck
                                    {
                                        BrainId = item.Request.BrainId.ToProtoUuid(),
                                        Command = "reset_brain_runtime_state",
                                        Success = false,
                                        Message = "gateway_apply_empty_response"
                                    });
                            }
                            catch (Exception ex)
                            {
                                return (
                                    item,
                                    new IoCommandAck
                                    {
                                        BrainId = item.Request.BrainId.ToProtoUuid(),
                                        Command = "reset_brain_runtime_state",
                                        Success = false,
                                        Message = $"gateway_apply_failed:{ex.GetBaseException().Message}"
                                    });
                            }
                        }))
                .ConfigureAwait(false);
        }
    }

    private sealed class IoGatewayRegistrationProbeActor : IActor
    {
        private readonly Guid _brainId;
        private int _registrationCount;
        private string _lastIoGatewayPid = string.Empty;
        private ProtoControl.InputCoordinatorMode _lastInputCoordinatorMode = ProtoControl.InputCoordinatorMode.DirtyOnChange;
        private bool _lastInputTickDrainArmed;
        private bool _observedArmedTickDrainRegistration;

        public IoGatewayRegistrationProbeActor(Guid brainId)
        {
            _brainId = brainId;
        }

        public sealed record GetSnapshot;

        public sealed record Snapshot(
            int RegistrationCount,
            string LastIoGatewayPid,
            ProtoControl.InputCoordinatorMode LastInputCoordinatorMode,
            bool LastInputTickDrainArmed,
            bool ObservedArmedTickDrainRegistration);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case RegisterIoGateway register
                    when register.BrainId is not null
                         && register.BrainId.TryToGuid(out var brainId)
                         && brainId == _brainId:
                    _registrationCount++;
                    _lastIoGatewayPid = register.IoGatewayPid ?? string.Empty;
                    _lastInputCoordinatorMode = register.InputCoordinatorMode;
                    _lastInputTickDrainArmed = register.InputTickDrainArmed;
                    _observedArmedTickDrainRegistration |= register.InputTickDrainArmed;
                    break;
                case GetSnapshot:
                    context.Respond(new Snapshot(
                        _registrationCount,
                        _lastIoGatewayPid,
                        _lastInputCoordinatorMode,
                        _lastInputTickDrainArmed,
                        _observedArmedTickDrainRegistration));
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class OutputSubscriberProbeActor : IActor
    {
        private int _singleCount;
        private int _vectorCount;
        private OutputEvent? _lastSingle;
        private OutputVectorEvent? _lastVector;

        public sealed record SubscribeGateway(PID GatewayPid, Guid BrainId, bool SubscribeSingles = true, bool SubscribeVectors = true);

        public sealed record GetSnapshot;

        public sealed record Snapshot(int SingleCount, int VectorCount, OutputEvent? LastSingle, OutputVectorEvent? LastVector);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case SubscribeGateway subscribe:
                    var subscriberActor = PidLabel(context.Self);
                    if (subscribe.SubscribeSingles)
                    {
                        context.Send(subscribe.GatewayPid, new SubscribeOutputs
                        {
                            BrainId = subscribe.BrainId.ToProtoUuid(),
                            SubscriberActor = subscriberActor
                        });
                    }

                    if (subscribe.SubscribeVectors)
                    {
                        context.Send(subscribe.GatewayPid, new SubscribeOutputsVector
                        {
                            BrainId = subscribe.BrainId.ToProtoUuid(),
                            SubscriberActor = subscriberActor
                        });
                    }

                    break;
                case OutputEvent output:
                    _singleCount++;
                    _lastSingle = output.Clone();
                    break;
                case OutputVectorEvent vector:
                    _vectorCount++;
                    _lastVector = vector.Clone();
                    break;
                case GetSnapshot:
                    context.Respond(new Snapshot(_singleCount, _vectorCount, _lastSingle?.Clone(), _lastVector?.Clone()));
                    break;
            }

            return Task.CompletedTask;
        }
    }
}
