using Microsoft.Data.Sqlite;
using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.HiveMind;
using Nbn.Runtime.IO;
using Nbn.Runtime.WorkerNode;
using Nbn.Shared;
using Nbn.Tests.Format;
using Proto;
using ProtoControl = Nbn.Proto.Control;
using ProtoSettings = Nbn.Proto.Settings;
using Repro = Nbn.Proto.Repro;

namespace Nbn.Tests.Integration;

public class IoGatewayArtifactReferenceTests
{
    [Fact]
    public async Task SpawnBrainViaIO_Forwards_To_HiveMind_And_Passes_Through_Success_Ack()
    {
        var (artifactRoot, brainDef) = await StoreBrainDefinitionAsync();
        try
        {
            var system = new ActorSystem();
            var root = system.Root;

            var workerNodeId = Guid.NewGuid();
            var ioName = $"io-{Guid.NewGuid():N}";
            var metadataName = $"brain-info-{Guid.NewGuid():N}";

            var ioPid = new PID(string.Empty, ioName);
            var hiveMind = root.Spawn(
                Props.FromProducer(() => new HiveMindActor(CreateHiveOptions(), ioPid: ioPid)));
            var gateway = root.SpawnNamed(
                Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hiveMind)),
                ioName);
            var metadata = root.SpawnNamed(
                Props.FromProducer(() => new FixedBrainInfoActor(brainDef, inputWidth: 4, outputWidth: 4)),
                metadataName);
            var worker = root.Spawn(
                Props.FromProducer(() => new WorkerNodeActor(workerNodeId, "worker.local", artifactRootPath: artifactRoot)));

            PrimeWorkerDiscoveryEndpoints(root, worker, hiveMind.Id, metadata.Id);
            PrimeWorkers(root, hiveMind, worker, workerNodeId);

            var response = await root.RequestAsync<SpawnBrainViaIOAck>(
                gateway,
                new SpawnBrainViaIO
                {
                    Request = new ProtoControl.SpawnBrain
                    {
                        BrainDef = brainDef
                    }
                },
                TimeSpan.FromSeconds(70));

            Assert.NotNull(response.Ack);
            Assert.True(response.Ack.BrainId.TryToGuid(out var brainId));
            Assert.True(
                brainId != Guid.Empty,
                $"Expected non-empty brain id but received failure={response.Ack.FailureReasonCode} message={response.Ack.FailureMessage}");
            Assert.True(string.IsNullOrWhiteSpace(response.Ack.FailureReasonCode));
            Assert.True(string.IsNullOrWhiteSpace(response.Ack.FailureMessage));
            Assert.True(string.IsNullOrWhiteSpace(response.FailureReasonCode));
            Assert.True(string.IsNullOrWhiteSpace(response.FailureMessage));

            await WaitForAsync(
                async () =>
                {
                    var lifecycle = await root.RequestAsync<ProtoControl.PlacementLifecycleInfo>(
                        hiveMind,
                        new ProtoControl.GetPlacementLifecycle
                        {
                            BrainId = brainId.ToProtoUuid()
                        });

                    return lifecycle.LifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned
                           || lifecycle.LifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning;
                },
                timeoutMs: 5_000);

            var routing = await root.RequestAsync<ProtoControl.BrainRoutingInfo>(
                hiveMind,
                new ProtoControl.GetBrainRouting
                {
                    BrainId = brainId.ToProtoUuid()
                });
            Assert.Equal($"worker.local/brain-{brainId:N}-root", routing.BrainRootPid);
            Assert.Equal($"worker.local/brain-{brainId:N}-router", routing.SignalRouterPid);

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
    public async Task SpawnBrainViaIO_EndToEnd_WhenWorkerArtifactLoadFails_Returns_Actionable_Failure()
    {
        var (artifactRoot, brainDef) = await StoreBrainDefinitionAsync();
        try
        {
            var system = new ActorSystem();
            var root = system.Root;

            var workerNodeId = Guid.NewGuid();
            var ioName = $"io-{Guid.NewGuid():N}";
            var metadataName = $"brain-info-{Guid.NewGuid():N}";

            var ioPid = new PID(string.Empty, ioName);
            var hiveMind = root.Spawn(
                Props.FromProducer(() => new HiveMindActor(CreateHiveOptions(), ioPid: ioPid)));
            var gateway = root.SpawnNamed(
                Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hiveMind)),
                ioName);
            var metadata = root.SpawnNamed(
                Props.FromProducer(() => new FixedBrainInfoActor(brainDef, inputWidth: 4, outputWidth: 4)),
                metadataName);
            var worker = root.Spawn(
                Props.FromProducer(() => new WorkerNodeActor(
                    workerNodeId,
                    "worker.local",
                    artifactStore: new ThrowingArtifactStore("simulated artifact store load failure"))));

            PrimeWorkerDiscoveryEndpoints(root, worker, hiveMind.Id, metadata.Id);
            PrimeWorkers(root, hiveMind, worker, workerNodeId);

            var response = await root.RequestAsync<SpawnBrainViaIOAck>(
                gateway,
                new SpawnBrainViaIO
                {
                    Request = new ProtoControl.SpawnBrain
                    {
                        BrainDef = brainDef
                    }
                },
                TimeSpan.FromSeconds(70));

            Assert.NotNull(response.Ack);
            Assert.True(response.Ack.BrainId.TryToGuid(out var brainId));
            Assert.Equal(Guid.Empty, brainId);
            Assert.Equal("spawn_internal_error", response.Ack.FailureReasonCode);
            Assert.Contains("artifact-backed shard load failed", response.Ack.FailureMessage, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(response.Ack.FailureReasonCode, response.FailureReasonCode);
            Assert.Equal(response.Ack.FailureMessage, response.FailureMessage);

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
    public async Task SpawnBrainViaIO_Forwards_To_HiveMind_And_Passes_Through_Empty_Ack()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var forwarded = new TaskCompletionSource<ProtoControl.SpawnBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var hiveProbe = root.Spawn(Props.FromProducer(() => new HiveSpawnProbe(
            forwarded,
            new ProtoControl.SpawnBrainAck
            {
                BrainId = Guid.Empty.ToProtoUuid(),
                FailureReasonCode = "spawn_worker_unavailable",
                FailureMessage = "Spawn failed: no eligible worker was available for the placement plan."
            })));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hiveProbe)));

        var brainDef = new string('8', 64).ToArtifactRef(98, "application/x-nbn", "test-store");
        var response = await root.RequestAsync<SpawnBrainViaIOAck>(
            gateway,
            new SpawnBrainViaIO
            {
                Request = new ProtoControl.SpawnBrain
                {
                    BrainDef = brainDef
                }
            });

        Assert.NotNull(response.Ack);
        Assert.True(response.Ack.BrainId.TryToGuid(out var actualBrainId));
        Assert.Equal(Guid.Empty, actualBrainId);
        Assert.Equal("spawn_worker_unavailable", response.Ack.FailureReasonCode);
        Assert.Equal("Spawn failed: no eligible worker was available for the placement plan.", response.Ack.FailureMessage);
        Assert.Equal(response.Ack.FailureReasonCode, response.FailureReasonCode);
        Assert.Equal(response.Ack.FailureMessage, response.FailureMessage);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var forwardedRequest = await forwarded.Task.WaitAsync(cts.Token);
        Assert.NotNull(forwardedRequest.BrainDef);
        Assert.Equal(brainDef.ToSha256Hex(), forwardedRequest.BrainDef.ToSha256Hex());

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task SpawnBrainViaIO_Waits_For_Slow_HiveMind_Ack_Within_SpawnTimeout()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var forwarded = new TaskCompletionSource<ProtoControl.SpawnBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var expectedBrainId = Guid.NewGuid();
        var hiveProbe = root.Spawn(Props.FromProducer(() => new HiveSpawnProbe(
            forwarded,
            new ProtoControl.SpawnBrainAck
            {
                BrainId = expectedBrainId.ToProtoUuid()
            },
            responseDelay: TimeSpan.FromSeconds(16))));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hiveProbe)));

        var brainDef = new string('6', 64).ToArtifactRef(96, "application/x-nbn", "test-store");
        var response = await root.RequestAsync<SpawnBrainViaIOAck>(
            gateway,
            new SpawnBrainViaIO
            {
                Request = new ProtoControl.SpawnBrain
                {
                    BrainDef = brainDef
                }
            },
            TimeSpan.FromSeconds(30));

        Assert.NotNull(response.Ack);
        Assert.True(response.Ack.BrainId.TryToGuid(out var actualBrainId));
        Assert.Equal(expectedBrainId, actualBrainId);
        Assert.True(string.IsNullOrWhiteSpace(response.Ack.FailureReasonCode));
        Assert.True(string.IsNullOrWhiteSpace(response.Ack.FailureMessage));
        Assert.True(string.IsNullOrWhiteSpace(response.FailureReasonCode));
        Assert.True(string.IsNullOrWhiteSpace(response.FailureMessage));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var forwardedRequest = await forwarded.Task.WaitAsync(cts.Token);
        Assert.NotNull(forwardedRequest.BrainDef);
        Assert.Equal(brainDef.ToSha256Hex(), forwardedRequest.BrainDef.ToSha256Hex());

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task SpawnBrainViaIO_Returns_Actionable_Failure_When_HiveMind_Is_Unavailable()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));

        var brainDef = new string('7', 64).ToArtifactRef(97, "application/x-nbn", "test-store");
        var response = await root.RequestAsync<SpawnBrainViaIOAck>(
            gateway,
            new SpawnBrainViaIO
            {
                Request = new ProtoControl.SpawnBrain
                {
                    BrainDef = brainDef
                }
            });

        Assert.NotNull(response.Ack);
        Assert.True(response.Ack.BrainId.TryToGuid(out var actualBrainId));
        Assert.Equal(Guid.Empty, actualBrainId);
        Assert.Equal("spawn_unavailable", response.Ack.FailureReasonCode);
        Assert.Equal("Spawn failed: HiveMind endpoint is not configured.", response.Ack.FailureMessage);
        Assert.Equal(response.Ack.FailureReasonCode, response.FailureReasonCode);
        Assert.Equal(response.Ack.FailureMessage, response.FailureMessage);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ExportBrainDefinition_Returns_Registered_BaseDefinition()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var baseDef = new string('a', 64).ToArtifactRef(123, "application/x-nbn", "test-store");

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            BaseDefinition = baseDef
        });

        var ready = await root.RequestAsync<BrainDefinitionReady>(
            gateway,
            new ExportBrainDefinition
            {
                BrainId = brainId.ToProtoUuid(),
                RebaseOverlays = false
            });

        Assert.NotNull(ready.BrainDef);
        Assert.True(ready.BrainDef.TryToSha256Hex(out var exportedSha));
        Assert.Equal(baseDef.ToSha256Hex(), exportedSha);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ExportBrainDefinition_RebaseOverlays_Forwards_To_HiveMind_And_Preserves_Local_BaseDefinition()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var forwarded = new TaskCompletionSource<ExportBrainDefinition>(TaskCreationOptions.RunContinuationsAsynchronously);
        var rebasedDefinition = new string('c', 64).ToArtifactRef(222, "application/x-nbn", "test-store");
        var hiveProbe = root.Spawn(Props.FromProducer(() => new HiveExportProbe(forwarded, rebasedDefinition)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hiveProbe)));

        var brainId = Guid.NewGuid();
        var baseDef = new string('a', 64).ToArtifactRef(123, "application/x-nbn", "test-store");
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            BaseDefinition = baseDef
        });

        var rebasedReady = await root.RequestAsync<BrainDefinitionReady>(
            gateway,
            new ExportBrainDefinition
            {
                BrainId = brainId.ToProtoUuid(),
                RebaseOverlays = true
            });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var forwardedRequest = await forwarded.Task.WaitAsync(cts.Token);
        Assert.True(forwardedRequest.RebaseOverlays);

        Assert.NotNull(rebasedReady.BrainDef);
        Assert.True(rebasedReady.BrainDef.TryToSha256Hex(out var rebasedSha));
        Assert.Equal(rebasedDefinition.ToSha256Hex(), rebasedSha);

        var nonRebasedReady = await root.RequestAsync<BrainDefinitionReady>(
            gateway,
            new ExportBrainDefinition
            {
                BrainId = brainId.ToProtoUuid(),
                RebaseOverlays = false
            });

        Assert.NotNull(nonRebasedReady.BrainDef);
        Assert.True(nonRebasedReady.BrainDef.TryToSha256Hex(out var baseSha));
        Assert.Equal(baseDef.ToSha256Hex(), baseSha);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RequestSnapshot_Returns_Registered_LastSnapshot()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var snapshot = new string('b', 64).ToArtifactRef(456, "application/x-nbs", "test-store");

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            LastSnapshot = snapshot
        });

        var ready = await root.RequestAsync<SnapshotReady>(
            gateway,
            new RequestSnapshot
            {
                BrainId = brainId.ToProtoUuid()
            });

        Assert.NotNull(ready.Snapshot);
        Assert.True(ready.Snapshot.TryToSha256Hex(out var snapshotSha));
        Assert.Equal(snapshot.ToSha256Hex(), snapshotSha);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RequestSnapshot_Forwards_RuntimeState_To_HiveMind_And_Uses_Fresh_Snapshot()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var forwarded = new TaskCompletionSource<RequestSnapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
        var freshSnapshot = new string('f', 64).ToArtifactRef(654, "application/x-nbs", "test-store");
        var hiveProbe = root.Spawn(Props.FromProducer(() => new HiveSnapshotProbe(forwarded, freshSnapshot)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hiveProbe)));

        var brainId = Guid.NewGuid();
        var staleSnapshot = new string('e', 64).ToArtifactRef(321, "application/x-nbs", "test-store");
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            LastSnapshot = staleSnapshot,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 42,
                CostEnabled = true,
                EnergyEnabled = true,
                PlasticityEnabled = true
            }
        });

        var ready = await root.RequestAsync<SnapshotReady>(
            gateway,
            new RequestSnapshot
            {
                BrainId = brainId.ToProtoUuid()
            });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var forwardedRequest = await forwarded.Task.WaitAsync(cts.Token);
        Assert.True(forwardedRequest.HasRuntimeState);
        Assert.Equal(42, forwardedRequest.EnergyRemaining);
        Assert.True(forwardedRequest.CostEnabled);
        Assert.True(forwardedRequest.EnergyEnabled);
        Assert.True(forwardedRequest.PlasticityEnabled);

        Assert.NotNull(ready.Snapshot);
        Assert.True(ready.Snapshot.TryToSha256Hex(out var snapshotSha));
        Assert.Equal(freshSnapshot.ToSha256Hex(), snapshotSha);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task BrainInfo_Returns_Full_Energy_Plasticity_And_Homeostasis_State()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 3,
            OutputWidth = 2,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 1234,
                EnergyRateUnitsPerSecond = 17,
                CostEnabled = true,
                EnergyEnabled = false,
                PlasticityEnabled = true,
                PlasticityRate = 0.125f,
                PlasticityProbabilisticUpdates = true,
                HomeostasisEnabled = true,
                HomeostasisTargetMode = ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero,
                HomeostasisUpdateMode = ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
                HomeostasisBaseProbability = 0.2f,
                HomeostasisMinStepCodes = 3,
                HomeostasisEnergyCouplingEnabled = true,
                HomeostasisEnergyTargetScale = 0.6f,
                HomeostasisEnergyProbabilityScale = 1.8f,
                LastTickCost = 41
            }
        });

        var info = await root.RequestAsync<BrainInfo>(
            gateway,
            new BrainInfoRequest
            {
                BrainId = brainId.ToProtoUuid()
            });

        Assert.Equal((uint)3, info.InputWidth);
        Assert.Equal((uint)2, info.OutputWidth);
        Assert.True(info.CostEnabled);
        Assert.False(info.EnergyEnabled);
        Assert.Equal(1234, info.EnergyRemaining);
        Assert.Equal(17, info.EnergyRateUnitsPerSecond);
        Assert.True(info.PlasticityEnabled);
        Assert.Equal(0.125f, info.PlasticityRate);
        Assert.True(info.PlasticityProbabilisticUpdates);
        Assert.True(info.HomeostasisEnabled);
        Assert.Equal(ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero, info.HomeostasisTargetMode);
        Assert.Equal(ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep, info.HomeostasisUpdateMode);
        Assert.Equal(0.2f, info.HomeostasisBaseProbability);
        Assert.Equal((uint)3, info.HomeostasisMinStepCodes);
        Assert.True(info.HomeostasisEnergyCouplingEnabled);
        Assert.Equal(0.6f, info.HomeostasisEnergyTargetScale);
        Assert.Equal(1.8f, info.HomeostasisEnergyProbabilityScale);
        Assert.Equal(41, info.LastTickCost);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task SetFlags_Forwards_RuntimeConfig_To_HiveMind_And_Updates_BrainInfo()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var costEnergy = new TaskCompletionSource<ProtoControl.SetBrainCostEnergy>(TaskCreationOptions.RunContinuationsAsynchronously);
        var plasticity = new TaskCompletionSource<ProtoControl.SetBrainPlasticity>(TaskCreationOptions.RunContinuationsAsynchronously);
        var homeostasis = new TaskCompletionSource<ProtoControl.SetBrainHomeostasis>(TaskCreationOptions.RunContinuationsAsynchronously);
        var hiveProbe = root.Spawn(Props.FromProducer(() => new HiveConfigProbe(costEnergy, plasticity, homeostasis)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hiveProbe)));

        var brainId = Guid.NewGuid();
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 2,
            OutputWidth = 2
        });

        var registered = await root.RequestAsync<BrainInfo>(gateway, new BrainInfoRequest
        {
            BrainId = brainId.ToProtoUuid()
        });
        Assert.Equal((uint)2, registered.InputWidth);
        Assert.Equal((uint)2, registered.OutputWidth);

        root.Send(gateway, new SetCostEnergyEnabled
        {
            BrainId = brainId.ToProtoUuid(),
            CostEnabled = true,
            EnergyEnabled = false
        });

        root.Send(gateway, new SetPlasticityEnabled
        {
            BrainId = brainId.ToProtoUuid(),
            PlasticityEnabled = true,
            PlasticityRate = 0.25f,
            ProbabilisticUpdates = false
        });

        root.Send(gateway, new SetHomeostasisEnabled
        {
            BrainId = brainId.ToProtoUuid(),
            HomeostasisEnabled = true,
            HomeostasisTargetMode = ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero,
            HomeostasisUpdateMode = ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
            HomeostasisBaseProbability = 0.22f,
            HomeostasisMinStepCodes = 2,
            HomeostasisEnergyCouplingEnabled = true,
            HomeostasisEnergyTargetScale = 0.7f,
            HomeostasisEnergyProbabilityScale = 1.3f
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var costUpdate = await costEnergy.Task.WaitAsync(cts.Token);
        var plasticityUpdate = await plasticity.Task.WaitAsync(cts.Token);
        var homeostasisUpdate = await homeostasis.Task.WaitAsync(cts.Token);

        Assert.True(costUpdate.BrainId.TryToGuid(out var costBrainId));
        Assert.Equal(brainId, costBrainId);
        Assert.True(costUpdate.CostEnabled);
        Assert.False(costUpdate.EnergyEnabled);

        Assert.True(plasticityUpdate.BrainId.TryToGuid(out var plasticityBrainId));
        Assert.Equal(brainId, plasticityBrainId);
        Assert.True(plasticityUpdate.PlasticityEnabled);
        Assert.Equal(0.25f, plasticityUpdate.PlasticityRate);
        Assert.False(plasticityUpdate.ProbabilisticUpdates);

        Assert.True(homeostasisUpdate.BrainId.TryToGuid(out var homeostasisBrainId));
        Assert.Equal(brainId, homeostasisBrainId);
        Assert.True(homeostasisUpdate.HomeostasisEnabled);
        Assert.Equal(ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero, homeostasisUpdate.HomeostasisTargetMode);
        Assert.Equal(ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep, homeostasisUpdate.HomeostasisUpdateMode);
        Assert.Equal(0.22f, homeostasisUpdate.HomeostasisBaseProbability);
        Assert.Equal((uint)2, homeostasisUpdate.HomeostasisMinStepCodes);
        Assert.True(homeostasisUpdate.HomeostasisEnergyCouplingEnabled);
        Assert.Equal(0.7f, homeostasisUpdate.HomeostasisEnergyTargetScale);
        Assert.Equal(1.3f, homeostasisUpdate.HomeostasisEnergyProbabilityScale);

        var info = await root.RequestAsync<BrainInfo>(gateway, new BrainInfoRequest
        {
            BrainId = brainId.ToProtoUuid()
        });

        Assert.True(info.CostEnabled);
        Assert.False(info.EnergyEnabled);
        Assert.True(info.PlasticityEnabled);
        Assert.Equal(0.25f, info.PlasticityRate);
        Assert.False(info.PlasticityProbabilisticUpdates);
        Assert.True(info.HomeostasisEnabled);
        Assert.Equal(0.22f, info.HomeostasisBaseProbability);
        Assert.Equal((uint)2, info.HomeostasisMinStepCodes);
        Assert.True(info.HomeostasisEnergyCouplingEnabled);
        Assert.Equal(0.7f, info.HomeostasisEnergyTargetScale);
        Assert.Equal(1.3f, info.HomeostasisEnergyProbabilityScale);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task SetFlags_Request_Returns_IoCommandAck_With_RuntimeState()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 250,
                EnergyRateUnitsPerSecond = 5,
                CostEnabled = false,
                EnergyEnabled = true,
                PlasticityEnabled = false,
                PlasticityRate = 0f,
                PlasticityProbabilisticUpdates = false
            }
        });

        var flagsAck = await root.RequestAsync<IoCommandAck>(gateway, new SetCostEnergyEnabled
        {
            BrainId = brainId.ToProtoUuid(),
            CostEnabled = true,
            EnergyEnabled = false
        });

        Assert.True(flagsAck.Success);
        Assert.Equal("set_cost_energy", flagsAck.Command);
        Assert.True(flagsAck.HasEnergyState);
        Assert.NotNull(flagsAck.EnergyState);
        Assert.True(flagsAck.EnergyState.CostEnabled);
        Assert.False(flagsAck.EnergyState.EnergyEnabled);
        Assert.Equal(250, flagsAck.EnergyState.EnergyRemaining);

        var plasticityAck = await root.RequestAsync<IoCommandAck>(gateway, new SetPlasticityEnabled
        {
            BrainId = brainId.ToProtoUuid(),
            PlasticityEnabled = true,
            PlasticityRate = 0.125f,
            ProbabilisticUpdates = true
        });

        Assert.True(plasticityAck.Success);
        Assert.Equal("set_plasticity", plasticityAck.Command);
        Assert.True(plasticityAck.HasEnergyState);
        Assert.NotNull(plasticityAck.EnergyState);
        Assert.True(plasticityAck.EnergyState.PlasticityEnabled);
        Assert.Equal(0.125f, plasticityAck.EnergyState.PlasticityRate);
        Assert.True(plasticityAck.EnergyState.PlasticityProbabilisticUpdates);

        var homeostasisAck = await root.RequestAsync<IoCommandAck>(gateway, new SetHomeostasisEnabled
        {
            BrainId = brainId.ToProtoUuid(),
            HomeostasisEnabled = true,
            HomeostasisTargetMode = ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero,
            HomeostasisUpdateMode = ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
            HomeostasisBaseProbability = 0.2f,
            HomeostasisMinStepCodes = 4,
            HomeostasisEnergyCouplingEnabled = true,
            HomeostasisEnergyTargetScale = 0.8f,
            HomeostasisEnergyProbabilityScale = 1.4f
        });

        Assert.True(homeostasisAck.Success);
        Assert.Equal("set_homeostasis", homeostasisAck.Command);
        Assert.True(homeostasisAck.HasEnergyState);
        Assert.NotNull(homeostasisAck.EnergyState);
        Assert.True(homeostasisAck.EnergyState.HomeostasisEnabled);
        Assert.Equal(ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero, homeostasisAck.EnergyState.HomeostasisTargetMode);
        Assert.Equal(ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep, homeostasisAck.EnergyState.HomeostasisUpdateMode);
        Assert.Equal(0.2f, homeostasisAck.EnergyState.HomeostasisBaseProbability);
        Assert.Equal((uint)4, homeostasisAck.EnergyState.HomeostasisMinStepCodes);
        Assert.True(homeostasisAck.EnergyState.HomeostasisEnergyCouplingEnabled);
        Assert.Equal(0.8f, homeostasisAck.EnergyState.HomeostasisEnergyTargetScale);
        Assert.Equal(1.4f, homeostasisAck.EnergyState.HomeostasisEnergyProbabilityScale);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task CommandAck_InvalidPlasticityRate_Returns_Failure()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1
        });

        var ack = await root.RequestAsync<IoCommandAck>(gateway, new SetPlasticityEnabled
        {
            BrainId = brainId.ToProtoUuid(),
            PlasticityEnabled = true,
            PlasticityRate = float.NaN,
            ProbabilisticUpdates = false
        });

        Assert.False(ack.Success);
        Assert.Equal("set_plasticity", ack.Command);
        Assert.Equal("plasticity_rate_invalid", ack.Message);
        Assert.True(ack.HasEnergyState);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task CommandAck_InvalidHomeostasisProbability_Returns_Failure()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1
        });

        var ack = await root.RequestAsync<IoCommandAck>(gateway, new SetHomeostasisEnabled
        {
            BrainId = brainId.ToProtoUuid(),
            HomeostasisEnabled = true,
            HomeostasisTargetMode = ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero,
            HomeostasisUpdateMode = ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
            HomeostasisBaseProbability = 1.1f,
            HomeostasisMinStepCodes = 1,
            HomeostasisEnergyCouplingEnabled = false,
            HomeostasisEnergyTargetScale = 1f,
            HomeostasisEnergyProbabilityScale = 1f
        });

        Assert.False(ack.Success);
        Assert.Equal("set_homeostasis", ack.Command);
        Assert.Equal("homeostasis_probability_invalid", ack.Message);
        Assert.True(ack.HasEnergyState);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RegisterBrain_RuntimeConfig_Updates_Config_Without_Resetting_Balance()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 777,
                EnergyRateUnitsPerSecond = 12,
                CostEnabled = false,
                EnergyEnabled = false,
                PlasticityEnabled = false,
                PlasticityRate = 0f,
                PlasticityProbabilisticUpdates = false,
                LastTickCost = 0
            }
        });

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            HasRuntimeConfig = true,
            CostEnabled = true,
            EnergyEnabled = true,
            PlasticityEnabled = true,
            PlasticityRate = 0.5f,
            PlasticityProbabilisticUpdates = true,
            HomeostasisEnabled = true,
            HomeostasisTargetMode = ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero,
            HomeostasisUpdateMode = ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
            HomeostasisBaseProbability = 0.18f,
            HomeostasisMinStepCodes = 2,
            HomeostasisEnergyCouplingEnabled = true,
            HomeostasisEnergyTargetScale = 0.75f,
            HomeostasisEnergyProbabilityScale = 1.25f,
            LastTickCost = 44
        });

        var info = await root.RequestAsync<BrainInfo>(gateway, new BrainInfoRequest
        {
            BrainId = brainId.ToProtoUuid()
        });

        Assert.Equal(777, info.EnergyRemaining);
        Assert.Equal(12, info.EnergyRateUnitsPerSecond);
        Assert.True(info.CostEnabled);
        Assert.True(info.EnergyEnabled);
        Assert.True(info.PlasticityEnabled);
        Assert.Equal(0.5f, info.PlasticityRate);
        Assert.True(info.PlasticityProbabilisticUpdates);
        Assert.True(info.HomeostasisEnabled);
        Assert.Equal(ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero, info.HomeostasisTargetMode);
        Assert.Equal(ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep, info.HomeostasisUpdateMode);
        Assert.Equal(0.18f, info.HomeostasisBaseProbability);
        Assert.Equal((uint)2, info.HomeostasisMinStepCodes);
        Assert.True(info.HomeostasisEnergyCouplingEnabled);
        Assert.Equal(0.75f, info.HomeostasisEnergyTargetScale);
        Assert.Equal(1.25f, info.HomeostasisEnergyProbabilityScale);
        Assert.Equal(44, info.LastTickCost);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ApplyTickCost_DuplicateTick_IsIgnored()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 100,
                CostEnabled = true,
                EnergyEnabled = true
            }
        });

        root.Send(gateway, new ApplyTickCost(brainId, 25, 7));
        root.Send(gateway, new ApplyTickCost(brainId, 25, 7));

        var info = await root.RequestAsync<BrainInfo>(gateway, new BrainInfoRequest
        {
            BrainId = brainId.ToProtoUuid()
        });

        Assert.Equal(93, info.EnergyRemaining);
        Assert.Equal(7, info.LastTickCost);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ApplyTickCost_OlderTickArrivingLate_IsIgnored()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 100,
                CostEnabled = true,
                EnergyEnabled = true
            }
        });

        root.Send(gateway, new ApplyTickCost(brainId, 40, 11));
        root.Send(gateway, new ApplyTickCost(brainId, 39, 3));

        var info = await root.RequestAsync<BrainInfo>(gateway, new BrainInfoRequest
        {
            BrainId = brainId.ToProtoUuid()
        });

        Assert.Equal(89, info.EnergyRemaining);
        Assert.Equal(11, info.LastTickCost);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ApplyTickCost_DuplicateDepletionTick_SendsSingleKill()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var firstKill = new TaskCompletionSource<ProtoControl.KillBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondKill = new TaskCompletionSource<ProtoControl.KillBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var hiveProbe = root.Spawn(Props.FromProducer(() => new HiveKillProbe(firstKill, secondKill)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hiveProbe)));

        var brainId = Guid.NewGuid();
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 5,
                CostEnabled = true,
                EnergyEnabled = true
            }
        });

        root.Send(gateway, new ApplyTickCost(brainId, 77, 10));
        root.Send(gateway, new ApplyTickCost(brainId, 77, 10));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var kill = await firstKill.Task.WaitAsync(cts.Token);

        Assert.True(kill.BrainId.TryToGuid(out var killedBrainId));
        Assert.Equal(brainId, killedBrainId);
        Assert.Equal("energy_exhausted", kill.Reason);

        await Task.Delay(100);
        Assert.False(secondKill.Task.IsCompleted);

        var info = await root.RequestAsync<BrainInfo>(gateway, new BrainInfoRequest
        {
            BrainId = brainId.ToProtoUuid()
        });
        Assert.Equal(-5, info.EnergyRemaining);
        Assert.Equal(10, info.LastTickCost);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task HandleBrainTerminated_EnergyExhausted_Uses_LocalEnergyState_ForBroadcast()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var firstKill = new TaskCompletionSource<ProtoControl.KillBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondKill = new TaskCompletionSource<ProtoControl.KillBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var hiveProbe = root.Spawn(Props.FromProducer(() => new HiveKillProbe(firstKill, secondKill)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hiveProbe)));
        var connected = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var terminated = new TaskCompletionSource<ProtoControl.BrainTerminated>(TaskCreationOptions.RunContinuationsAsynchronously);
        root.Spawn(Props.FromProducer(() => new TerminationClientProbe(gateway, connected, terminated)));

        var brainId = Guid.NewGuid();
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 5,
                CostEnabled = true,
                EnergyEnabled = true
            }
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await connected.Task.WaitAsync(cts.Token);

        root.Send(gateway, new ApplyTickCost(brainId, 88, 10));
        var kill = await firstKill.Task.WaitAsync(cts.Token);
        Assert.True(kill.BrainId.TryToGuid(out var killedBrainId));
        Assert.Equal(brainId, killedBrainId);

        var terminationTimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        root.Send(gateway, new ProtoControl.BrainTerminated
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "energy_exhausted",
            BaseDef = new ArtifactRef(),
            LastSnapshot = new ArtifactRef(),
            LastEnergyRemaining = 0,
            LastTickCost = 0,
            TimeMs = terminationTimeMs
        });

        var broadcast = await terminated.Task.WaitAsync(cts.Token);
        Assert.Equal("energy_exhausted", broadcast.Reason);
        Assert.Equal(-5, broadcast.LastEnergyRemaining);
        Assert.Equal(10, broadcast.LastTickCost);

        var info = await root.RequestAsync<BrainInfo>(gateway, new BrainInfoRequest
        {
            BrainId = brainId.ToProtoUuid()
        });
        Assert.Equal((uint)0, info.InputWidth);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task HandleBrainTerminated_StaleMessage_DoesNotRemoveRespawnedBrain()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 111,
                CostEnabled = true,
                EnergyEnabled = true
            }
        });

        root.Send(gateway, new UnregisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "restart"
        });

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 2,
            OutputWidth = 2,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 222,
                CostEnabled = true,
                EnergyEnabled = true
            }
        });

        root.Send(gateway, new ProtoControl.BrainTerminated
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "energy_exhausted",
            TimeMs = 1
        });

        var info = await root.RequestAsync<BrainInfo>(gateway, new BrainInfoRequest
        {
            BrainId = brainId.ToProtoUuid()
        });

        Assert.Equal((uint)2, info.InputWidth);
        Assert.Equal((uint)2, info.OutputWidth);
        Assert.Equal(222, info.EnergyRemaining);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task HandleBrainTerminated_NonEnergyReason_Uses_LocalEnergyState_ForBroadcast()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));
        var connected = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var terminated = new TaskCompletionSource<ProtoControl.BrainTerminated>(TaskCreationOptions.RunContinuationsAsynchronously);
        root.Spawn(Props.FromProducer(() => new TerminationClientProbe(gateway, connected, terminated)));

        var brainId = Guid.NewGuid();
        var baseDef = new string('a', 64).ToArtifactRef(123, "application/x-nbn", "test-store");
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            BaseDefinition = baseDef,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 50,
                CostEnabled = true,
                EnergyEnabled = true
            }
        });

        root.Send(gateway, new ApplyTickCost(brainId, 14, 7));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await connected.Task.WaitAsync(cts.Token);

        var terminationTimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        root.Send(gateway, new ProtoControl.BrainTerminated
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "killed",
            BaseDef = new ArtifactRef(),
            LastSnapshot = new ArtifactRef(),
            LastEnergyRemaining = 0,
            LastTickCost = 0,
            TimeMs = terminationTimeMs
        });

        var broadcast = await terminated.Task.WaitAsync(cts.Token);
        Assert.Equal("killed", broadcast.Reason);
        Assert.Equal(43, broadcast.LastEnergyRemaining);
        Assert.Equal(7, broadcast.LastTickCost);
        Assert.True(broadcast.BaseDef.TryToSha256Hex(out var broadcastBaseSha));
        Assert.Equal(baseDef.ToSha256Hex(), broadcastBaseSha);

        var info = await root.RequestAsync<BrainInfo>(gateway, new BrainInfoRequest
        {
            BrainId = brainId.ToProtoUuid()
        });
        Assert.Equal((uint)0, info.InputWidth);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task HandleBrainTerminated_StaleMessage_AfterReRegisterWithEnergyState_IsIgnored()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 100,
                CostEnabled = true,
                EnergyEnabled = true
            }
        });

        var firstInfo = await root.RequestAsync<BrainInfo>(gateway, new BrainInfoRequest
        {
            BrainId = brainId.ToProtoUuid()
        });
        Assert.Equal(100, firstInfo.EnergyRemaining);

        var staleTimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await Task.Delay(20);

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 222,
                CostEnabled = true,
                EnergyEnabled = true
            }
        });

        root.Send(gateway, new ProtoControl.BrainTerminated
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "energy_exhausted",
            TimeMs = staleTimeMs
        });

        var info = await root.RequestAsync<BrainInfo>(gateway, new BrainInfoRequest
        {
            BrainId = brainId.ToProtoUuid()
        });
        Assert.Equal((uint)1, info.InputWidth);
        Assert.Equal(222, info.EnergyRemaining);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RegisterBrain_WithEnergyState_ResetsDepletionLatch_AndTickCostEpoch()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var firstKill = new TaskCompletionSource<ProtoControl.KillBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondKill = new TaskCompletionSource<ProtoControl.KillBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var hiveProbe = root.Spawn(Props.FromProducer(() => new HiveKillProbe(firstKill, secondKill)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hiveProbe)));

        var brainId = Guid.NewGuid();
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 5,
                CostEnabled = true,
                EnergyEnabled = true
            }
        });

        root.Send(gateway, new ApplyTickCost(brainId, 50, 10));
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var firstKillMessage = await firstKill.Task.WaitAsync(cts.Token);
        Assert.True(firstKillMessage.BrainId.TryToGuid(out var firstKilledBrainId));
        Assert.Equal(brainId, firstKilledBrainId);

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 200,
                CostEnabled = true,
                EnergyEnabled = true
            }
        });

        root.Send(gateway, new ApplyTickCost(brainId, 40, 5));
        var info = await root.RequestAsync<BrainInfo>(gateway, new BrainInfoRequest
        {
            BrainId = brainId.ToProtoUuid()
        });
        Assert.Equal(195, info.EnergyRemaining);
        Assert.Equal(5, info.LastTickCost);

        root.Send(gateway, new ApplyTickCost(brainId, 41, 300));
        var secondKillMessage = await secondKill.Task.WaitAsync(cts.Token);
        Assert.True(secondKillMessage.BrainId.TryToGuid(out var secondKilledBrainId));
        Assert.Equal(brainId, secondKilledBrainId);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task HandleBrainTerminated_ConfigOnlyReRegister_DoesNotResetEpochTimestamp()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            EnergyState = new Nbn.Proto.Io.BrainEnergyState
            {
                EnergyRemaining = 100,
                CostEnabled = true,
                EnergyEnabled = true
            }
        });

        var firstInfo = await root.RequestAsync<BrainInfo>(gateway, new BrainInfoRequest
        {
            BrainId = brainId.ToProtoUuid()
        });
        Assert.Equal((uint)1, firstInfo.InputWidth);
        Assert.Equal(100, firstInfo.EnergyRemaining);

        var terminationTimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await Task.Delay(20);

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            HasRuntimeConfig = true,
            CostEnabled = false,
            EnergyEnabled = false,
            PlasticityEnabled = true,
            PlasticityRate = 0.25f,
            PlasticityProbabilisticUpdates = false,
            LastTickCost = 12
        });

        root.Send(gateway, new ProtoControl.BrainTerminated
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "killed",
            TimeMs = terminationTimeMs
        });

        var info = await root.RequestAsync<BrainInfo>(gateway, new BrainInfoRequest
        {
            BrainId = brainId.ToProtoUuid()
        });
        Assert.Equal((uint)0, info.InputWidth);
        Assert.Equal((uint)0, info.OutputWidth);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ReproduceByBrainIds_Returns_AbortReport_When_Repro_Is_Unavailable()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));

        var response = await root.RequestAsync<Nbn.Proto.Io.ReproduceResult>(
            gateway,
            new ReproduceByBrainIds
            {
                Request = new Repro.ReproduceByBrainIdsRequest
                {
                    ParentA = Guid.NewGuid().ToProtoUuid(),
                    ParentB = Guid.NewGuid().ToProtoUuid(),
                    Config = new Repro.ReproduceConfig()
                }
            });

        Assert.NotNull(response.Result);
        Assert.NotNull(response.Result.Report);
        Assert.False(response.Result.Report.Compatible);
        Assert.Equal("repro_unavailable", response.Result.Report.AbortReason);
        Assert.False(response.Result.Spawned);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ReproduceByArtifacts_Adds_AbortReport_When_Repro_Response_Lacks_Report()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var reproProbe = root.Spawn(Props.FromProducer(() => new ReproEmptyResponseProbe()));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), reproPid: reproProbe)));

        var response = await root.RequestAsync<Nbn.Proto.Io.ReproduceResult>(
            gateway,
            new ReproduceByArtifacts
            {
                Request = new Repro.ReproduceByArtifactsRequest()
            });

        Assert.NotNull(response.Result);
        Assert.NotNull(response.Result.Report);
        Assert.False(response.Result.Report.Compatible);
        Assert.Equal("repro_missing_report", response.Result.Report.AbortReason);
        Assert.Equal(0f, response.Result.Report.SimilarityScore);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ReproduceByBrainIds_Returns_AbortReport_When_Repro_Response_Type_Is_Invalid()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var reproProbe = root.Spawn(Props.FromProducer(() => new ReproInvalidResponseProbe()));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), reproPid: reproProbe)));

        var response = await root.RequestAsync<Nbn.Proto.Io.ReproduceResult>(
            gateway,
            new ReproduceByBrainIds
            {
                Request = new Repro.ReproduceByBrainIdsRequest
                {
                    ParentA = Guid.NewGuid().ToProtoUuid(),
                    ParentB = Guid.NewGuid().ToProtoUuid(),
                    Config = new Repro.ReproduceConfig()
                }
            });

        Assert.NotNull(response.Result);
        Assert.NotNull(response.Result.Report);
        Assert.False(response.Result.Report.Compatible);
        Assert.Equal("repro_request_failed", response.Result.Report.AbortReason);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ReproduceByArtifacts_Preserves_Detailed_Result_From_Repro_Response()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var expectedChildBrainId = Guid.NewGuid();
        var expectedChildDef = new string('d', 64).ToArtifactRef(321, "application/x-nbn", "test-store");
        var expected = new Repro.ReproduceResult
        {
            Report = new Repro.SimilarityReport
            {
                Compatible = true,
                AbortReason = string.Empty,
                SimilarityScore = 0.77f,
                RegionSpanScore = 0.7f,
                FunctionScore = 0.8f,
                ConnectivityScore = 0.9f
            },
            Summary = new Repro.MutationSummary
            {
                NeuronsAdded = 1,
                NeuronsRemoved = 2,
                AxonsAdded = 3,
                AxonsRemoved = 4,
                AxonsRerouted = 5,
                FunctionsMutated = 6,
                StrengthCodesChanged = 7
            },
            ChildDef = expectedChildDef,
            Spawned = true,
            ChildBrainId = expectedChildBrainId.ToProtoUuid()
        };

        var reproProbe = root.Spawn(Props.FromProducer(() => new ReproFixedResponseProbe(expected)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), reproPid: reproProbe)));

        var response = await root.RequestAsync<Nbn.Proto.Io.ReproduceResult>(
            gateway,
            new ReproduceByArtifacts
            {
                Request = new Repro.ReproduceByArtifactsRequest()
            });

        Assert.NotNull(response.Result);
        Assert.NotNull(response.Result.Report);
        Assert.True(response.Result.Report.Compatible);
        Assert.Equal(string.Empty, response.Result.Report.AbortReason);
        Assert.Equal(0.77f, response.Result.Report.SimilarityScore);
        Assert.Equal(0.7f, response.Result.Report.RegionSpanScore);
        Assert.Equal(0.8f, response.Result.Report.FunctionScore);
        Assert.Equal(0.9f, response.Result.Report.ConnectivityScore);
        Assert.NotNull(response.Result.Summary);
        Assert.Equal((uint)1, response.Result.Summary.NeuronsAdded);
        Assert.Equal((uint)2, response.Result.Summary.NeuronsRemoved);
        Assert.Equal((uint)3, response.Result.Summary.AxonsAdded);
        Assert.Equal((uint)4, response.Result.Summary.AxonsRemoved);
        Assert.Equal((uint)5, response.Result.Summary.AxonsRerouted);
        Assert.Equal((uint)6, response.Result.Summary.FunctionsMutated);
        Assert.Equal((uint)7, response.Result.Summary.StrengthCodesChanged);
        Assert.NotNull(response.Result.ChildDef);
        Assert.Equal(expectedChildDef.ToSha256Hex(), response.Result.ChildDef.ToSha256Hex());
        Assert.True(response.Result.Spawned);
        Assert.NotNull(response.Result.ChildBrainId);
        Assert.True(response.Result.ChildBrainId.TryToGuid(out var actualChildBrainId));
        Assert.Equal(expectedChildBrainId, actualChildBrainId);

        await system.ShutdownAsync();
    }

    private static async Task<(string ArtifactRoot, ArtifactRef BrainDef)> StoreBrainDefinitionAsync()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-io-spawn-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
        var minimalNbn = NbnTestVectors.CreateMinimalNbn();
        var manifest = await store.StoreAsync(new MemoryStream(minimalNbn), "application/x-nbn");
        var brainDef = manifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifest.ByteLength, "application/x-nbn", artifactRoot);
        return (artifactRoot, brainDef);
    }

    private static HiveMindOptions CreateHiveOptions(
        int assignmentTimeoutMs = 1_000,
        int retryBackoffMs = 10,
        int maxRetries = 1,
        int reconcileTimeoutMs = 1_000)
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
                HasGpu = true,
                VramFreeBytes = 8UL * 1024 * 1024 * 1024,
                CpuScore = 40f,
                GpuScore = 80f
            }
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

        throw new TimeoutException($"Condition was not met within {timeoutMs} ms.");
    }

    private static IoOptions CreateOptions()
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

    private sealed class ThrowingArtifactStore : IArtifactStore
    {
        private readonly string _message;

        public ThrowingArtifactStore(string message)
        {
            _message = string.IsNullOrWhiteSpace(message) ? "artifact store failure" : message;
        }

        public Task<ArtifactManifest> StoreAsync(
            Stream content,
            string mediaType,
            ArtifactStoreWriteOptions? options = null,
            CancellationToken cancellationToken = default)
            => throw new InvalidOperationException(_message);

        public Task<ArtifactManifest?> TryGetManifestAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException(_message);

        public Task<bool> ContainsAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException(_message);

        public Task<Stream?> TryOpenArtifactAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException(_message);
    }

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
            if (context.Message is not BrainInfoRequest request)
            {
                return Task.CompletedTask;
            }

            context.Respond(new BrainInfo
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

    private sealed class HiveSpawnProbe : IActor
    {
        private readonly TaskCompletionSource<ProtoControl.SpawnBrain> _request;
        private readonly ProtoControl.SpawnBrainAck _ack;
        private readonly TimeSpan? _responseDelay;

        public HiveSpawnProbe(
            TaskCompletionSource<ProtoControl.SpawnBrain> request,
            ProtoControl.SpawnBrainAck ack,
            TimeSpan? responseDelay = null)
        {
            _request = request;
            _ack = ack;
            _responseDelay = responseDelay;
        }

        public async Task ReceiveAsync(IContext context)
        {
            if (context.Message is ProtoControl.SpawnBrain request)
            {
                _request.TrySetResult(request.Clone());
                if (_responseDelay.HasValue && _responseDelay.Value > TimeSpan.Zero)
                {
                    await Task.Delay(_responseDelay.Value).ConfigureAwait(false);
                }

                context.Respond(_ack.Clone());
            }
        }
    }

    private sealed class ReproFixedResponseProbe : IActor
    {
        private readonly Repro.ReproduceResult _response;

        public ReproFixedResponseProbe(Repro.ReproduceResult response)
        {
            _response = response;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Repro.ReproduceByBrainIdsRequest:
                case Repro.ReproduceByArtifactsRequest:
                    context.Respond(_response.Clone());
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class HiveConfigProbe : IActor
    {
        private readonly TaskCompletionSource<ProtoControl.SetBrainCostEnergy> _costEnergy;
        private readonly TaskCompletionSource<ProtoControl.SetBrainPlasticity> _plasticity;
        private readonly TaskCompletionSource<ProtoControl.SetBrainHomeostasis> _homeostasis;

        public HiveConfigProbe(
            TaskCompletionSource<ProtoControl.SetBrainCostEnergy> costEnergy,
            TaskCompletionSource<ProtoControl.SetBrainPlasticity> plasticity,
            TaskCompletionSource<ProtoControl.SetBrainHomeostasis> homeostasis)
        {
            _costEnergy = costEnergy;
            _plasticity = plasticity;
            _homeostasis = homeostasis;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case ProtoControl.GetBrainRouting:
                    context.Respond(new ProtoControl.BrainRoutingInfo());
                    break;
                case ProtoControl.SetBrainCostEnergy costEnergy:
                    _costEnergy.TrySetResult(costEnergy);
                    break;
                case ProtoControl.SetBrainPlasticity plasticity:
                    _plasticity.TrySetResult(plasticity);
                    break;
                case ProtoControl.SetBrainHomeostasis homeostasis:
                    _homeostasis.TrySetResult(homeostasis);
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class HiveSnapshotProbe : IActor
    {
        private readonly TaskCompletionSource<RequestSnapshot> _request;
        private readonly ArtifactRef _snapshot;

        public HiveSnapshotProbe(TaskCompletionSource<RequestSnapshot> request, ArtifactRef snapshot)
        {
            _request = request;
            _snapshot = snapshot;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case ProtoControl.GetBrainRouting:
                    context.Respond(new ProtoControl.BrainRoutingInfo());
                    break;
                case RequestSnapshot request:
                    _request.TrySetResult(request);
                    context.Respond(new SnapshotReady
                    {
                        BrainId = request.BrainId,
                        Snapshot = _snapshot
                    });
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class HiveExportProbe : IActor
    {
        private readonly TaskCompletionSource<ExportBrainDefinition> _request;
        private readonly ArtifactRef _rebasedDefinition;

        public HiveExportProbe(TaskCompletionSource<ExportBrainDefinition> request, ArtifactRef rebasedDefinition)
        {
            _request = request;
            _rebasedDefinition = rebasedDefinition;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case ProtoControl.GetBrainRouting:
                    context.Respond(new ProtoControl.BrainRoutingInfo());
                    break;
                case ExportBrainDefinition request:
                    _request.TrySetResult(request);
                    context.Respond(new BrainDefinitionReady
                    {
                        BrainId = request.BrainId,
                        BrainDef = _rebasedDefinition
                    });
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class HiveKillProbe : IActor
    {
        private readonly TaskCompletionSource<ProtoControl.KillBrain> _firstKill;
        private readonly TaskCompletionSource<ProtoControl.KillBrain> _secondKill;

        public HiveKillProbe(
            TaskCompletionSource<ProtoControl.KillBrain> firstKill,
            TaskCompletionSource<ProtoControl.KillBrain> secondKill)
        {
            _firstKill = firstKill;
            _secondKill = secondKill;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case ProtoControl.GetBrainRouting:
                    context.Respond(new ProtoControl.BrainRoutingInfo());
                    break;
                case ProtoControl.KillBrain kill:
                    if (!_firstKill.Task.IsCompleted)
                    {
                        _firstKill.TrySetResult(kill);
                    }
                    else
                    {
                        _secondKill.TrySetResult(kill);
                    }
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class TerminationClientProbe : IActor
    {
        private readonly PID _gateway;
        private readonly TaskCompletionSource<bool> _connected;
        private readonly TaskCompletionSource<ProtoControl.BrainTerminated> _terminated;

        public TerminationClientProbe(
            PID gateway,
            TaskCompletionSource<bool> connected,
            TaskCompletionSource<ProtoControl.BrainTerminated> terminated)
        {
            _gateway = gateway;
            _connected = connected;
            _terminated = terminated;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Started:
                    context.Request(_gateway, new Connect { ClientName = "termination-test-client" });
                    break;
                case ConnectAck:
                    _connected.TrySetResult(true);
                    break;
                case ProtoControl.BrainTerminated brainTerminated:
                    _terminated.TrySetResult(brainTerminated);
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class ReproEmptyResponseProbe : IActor
    {
        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Repro.ReproduceByBrainIdsRequest:
                case Repro.ReproduceByArtifactsRequest:
                    context.Respond(new Repro.ReproduceResult());
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class ReproInvalidResponseProbe : IActor
    {
        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Repro.ReproduceByBrainIdsRequest:
                case Repro.ReproduceByArtifactsRequest:
                    context.Respond("unexpected-response-type");
                    break;
            }

            return Task.CompletedTask;
        }
    }
}
