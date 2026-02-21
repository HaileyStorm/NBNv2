using Nbn.Proto.Io;
using Nbn.Runtime.IO;
using Nbn.Shared;
using Proto;

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
}
