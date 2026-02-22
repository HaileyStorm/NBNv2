using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Runtime.IO;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Tests.Integration;

public class IoGatewayArtifactReferenceTests
{
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
    public async Task BrainInfo_Returns_Full_Energy_And_Plasticity_State()
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
        var hiveProbe = root.Spawn(Props.FromProducer(() => new HiveConfigProbe(costEnergy, plasticity)));
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

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var costUpdate = await costEnergy.Task.WaitAsync(cts.Token);
        var plasticityUpdate = await plasticity.Task.WaitAsync(cts.Token);

        Assert.True(costUpdate.BrainId.TryToGuid(out var costBrainId));
        Assert.Equal(brainId, costBrainId);
        Assert.True(costUpdate.CostEnabled);
        Assert.False(costUpdate.EnergyEnabled);

        Assert.True(plasticityUpdate.BrainId.TryToGuid(out var plasticityBrainId));
        Assert.Equal(brainId, plasticityBrainId);
        Assert.True(plasticityUpdate.PlasticityEnabled);
        Assert.Equal(0.25f, plasticityUpdate.PlasticityRate);
        Assert.False(plasticityUpdate.ProbabilisticUpdates);

        var info = await root.RequestAsync<BrainInfo>(gateway, new BrainInfoRequest
        {
            BrainId = brainId.ToProtoUuid()
        });

        Assert.True(info.CostEnabled);
        Assert.False(info.EnergyEnabled);
        Assert.True(info.PlasticityEnabled);
        Assert.Equal(0.25f, info.PlasticityRate);
        Assert.False(info.PlasticityProbabilisticUpdates);

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
        Assert.Equal(44, info.LastTickCost);

        await system.ShutdownAsync();
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

    private sealed class HiveConfigProbe : IActor
    {
        private readonly TaskCompletionSource<ProtoControl.SetBrainCostEnergy> _costEnergy;
        private readonly TaskCompletionSource<ProtoControl.SetBrainPlasticity> _plasticity;

        public HiveConfigProbe(
            TaskCompletionSource<ProtoControl.SetBrainCostEnergy> costEnergy,
            TaskCompletionSource<ProtoControl.SetBrainPlasticity> plasticity)
        {
            _costEnergy = costEnergy;
            _plasticity = plasticity;
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
}
