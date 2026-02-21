using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Proto;

namespace Nbn.Tests.HiveMind;

public class HiveMindArtifactReferenceTests
{
    [Fact]
    public async Task ExportBrainDefinition_Returns_BaseDefinition_From_Placement()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var baseDef = new string('A', 64).ToLowerInvariant().ToArtifactRef(128, "application/x-nbn", "test-store");
        var placement = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                BaseDef = baseDef,
                InputWidth = 1,
                OutputWidth = 1
            });

        Assert.True(placement.Accepted);

        var ready = await root.RequestAsync<BrainDefinitionReady>(
            hiveMind,
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
    public async Task RequestSnapshot_Returns_LastSnapshot_From_Placement()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));

        var brainId = Guid.NewGuid();
        var baseDef = new string('C', 64).ToLowerInvariant().ToArtifactRef(128, "application/x-nbn", "test-store");
        var snapshot = new string('D', 64).ToLowerInvariant().ToArtifactRef(96, "application/x-nbs", "test-store");

        var placement = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                BaseDef = baseDef,
                LastSnapshot = snapshot,
                InputWidth = 1,
                OutputWidth = 1
            });

        Assert.True(placement.Accepted);

        var ready = await root.RequestAsync<SnapshotReady>(
            hiveMind,
            new RequestSnapshot
            {
                BrainId = brainId.ToProtoUuid()
            });

        Assert.NotNull(ready.Snapshot);
        Assert.True(ready.Snapshot.TryToSha256Hex(out var snapshotSha));
        Assert.Equal(snapshot.ToSha256Hex(), snapshotSha);

        await system.ShutdownAsync();
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
