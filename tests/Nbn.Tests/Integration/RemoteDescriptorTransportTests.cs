using System.Net;
using System.Net.Sockets;
using Microsoft.Data.Sqlite;
using Nbn.Proto;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Viz;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.HiveMind;
using Nbn.Runtime.Reproduction;
using Nbn.Runtime.Speciation;
using Nbn.Runtime.WorkerNode;
using Nbn.Shared;
using Nbn.Tests.Format;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;
using Repro = Nbn.Proto.Repro;

namespace Nbn.Tests.Integration;

public class RemoteDescriptorTransportTests
{
    [Fact]
    public async Task HiveMindRemote_Transports_IoExportDefinition_Messages()
    {
        var receiverPort = GetFreePort();
        var senderPort = GetFreePort();

        var options = HiveMindOptions.FromArgs(new[]
        {
            "--bind-host", "127.0.0.1",
            "--port", receiverPort.ToString(),
            "--settings-host", "127.0.0.1",
            "--settings-port", "12010",
            "--settings-name", "SettingsMonitor",
            "--autostart", "false"
        });

        await using var receiver = await RemoteTestNode.StartAsync(HiveMindRemote.BuildConfig(options));
        receiver.Root.SpawnNamed(Props.FromProducer(() => new ExportProbeActor()), "export-probe");

        var senderConfig = RemoteConfig.BindToLocalhost(senderPort).WithProtoMessages(
            NbnCommonReflection.Descriptor,
            NbnIoReflection.Descriptor);
        await using var sender = await RemoteTestNode.StartAsync(senderConfig);

        var brainId = Guid.NewGuid();
        var target = new PID(receiver.Address, "export-probe");
        var response = await sender.Root.RequestAsync<BrainDefinitionReady>(
            target,
            new ExportBrainDefinition { BrainId = brainId.ToProtoUuid() },
            TimeSpan.FromSeconds(5));

        Assert.NotNull(response);
        Assert.True(response.BrainId.TryToGuid(out var returnedBrainId));
        Assert.Equal(brainId, returnedBrainId);
    }

    [Fact]
    public async Task WorkerNodeRemote_Transports_VisualizationEvent_Messages()
    {
        var receiverPort = GetFreePort();
        var senderPort = GetFreePort();

        var options = WorkerNodeOptions.FromArgs(new[]
        {
            "--bind-host", "127.0.0.1",
            "--port", receiverPort.ToString(),
            "--settings-host", "127.0.0.1",
            "--settings-port", "12010",
            "--settings-name", "SettingsMonitor"
        });

        await using var receiver = await RemoteTestNode.StartAsync(WorkerNodeRemote.BuildConfig(options));
        receiver.Root.SpawnNamed(Props.FromProducer(() => new VizProbeActor()), "viz-probe");

        var senderConfig = RemoteConfig.BindToLocalhost(senderPort).WithProtoMessages(
            NbnCommonReflection.Descriptor,
            NbnVizReflection.Descriptor);
        await using var sender = await RemoteTestNode.StartAsync(senderConfig);

        var expected = new VisualizationEvent
        {
            EventId = "evt-1",
            TimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Type = VizEventType.VizTick,
            TickId = 42
        };

        var target = new PID(receiver.Address, "viz-probe");
        var response = await sender.Root.RequestAsync<VisualizationEvent>(target, expected, TimeSpan.FromSeconds(5));

        Assert.NotNull(response);
        Assert.Equal(expected.EventId, response.EventId);
        Assert.Equal(expected.Type, response.Type);
        Assert.Equal(expected.TickId, response.TickId);
    }

    [Fact]
    public async Task WorkerNodeRemote_Transports_DebugOutbound_Messages()
    {
        var receiverPort = GetFreePort();
        var senderPort = GetFreePort();

        var options = WorkerNodeOptions.FromArgs(new[]
        {
            "--bind-host", "127.0.0.1",
            "--port", receiverPort.ToString(),
            "--settings-host", "127.0.0.1",
            "--settings-port", "12010",
            "--settings-name", "SettingsMonitor"
        });

        await using var receiver = await RemoteTestNode.StartAsync(WorkerNodeRemote.BuildConfig(options));
        receiver.Root.SpawnNamed(Props.FromProducer(() => new DebugProbeActor()), "debug-probe");

        var senderConfig = RemoteConfig.BindToLocalhost(senderPort).WithProtoMessages(
            NbnCommonReflection.Descriptor,
            NbnDebugReflection.Descriptor);
        await using var sender = await RemoteTestNode.StartAsync(senderConfig);

        var expected = new DebugOutbound
        {
            Severity = Severity.SevInfo,
            Context = "transport",
            Summary = "probe",
            Message = "roundtrip"
        };

        var target = new PID(receiver.Address, "debug-probe");
        var response = await sender.Root.RequestAsync<DebugOutbound>(target, expected, TimeSpan.FromSeconds(5));

        Assert.NotNull(response);
        Assert.Equal(expected.Severity, response.Severity);
        Assert.Equal(expected.Context, response.Context);
        Assert.Equal(expected.Summary, response.Summary);
        Assert.Equal(expected.Message, response.Message);
    }

    [Fact]
    public async Task SpeciationRemote_Transports_Reproduction_AssessCompatibilityByArtifacts_Messages()
    {
        var receiverPort = GetFreePort();
        var senderPort = GetFreePort();
        var artifactRoot = Path.Combine(
            Path.GetTempPath(),
            $"nbn-remote-spec-repro-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var parentA = NbnTestVectors.CreateMinimalNbn();
            var parentB = NbnTestVectors.CreateMinimalNbn();
            var manifestA = await store.StoreAsync(new MemoryStream(parentA), "application/x-nbn");
            var manifestB = await store.StoreAsync(new MemoryStream(parentB), "application/x-nbn");
            var parentARef = manifestA.ArtifactId.Bytes.ToArray().ToArtifactRef(
                (ulong)manifestA.ByteLength,
                "application/x-nbn",
                artifactRoot);
            var parentBRef = manifestB.ArtifactId.Bytes.ToArray().ToArtifactRef(
                (ulong)manifestB.ByteLength,
                "application/x-nbn",
                artifactRoot);

            var receiverOptions = ReproductionOptions.FromArgs(new[]
            {
                "--bind-host", "127.0.0.1",
                "--port", receiverPort.ToString(),
                "--settings-host", "127.0.0.1",
                "--settings-port", "12010",
                "--settings-name", "SettingsMonitor"
            });
            await using var receiver = await RemoteTestNode.StartAsync(
                ReproductionRemote.BuildConfig(receiverOptions));
            receiver.Root.SpawnNamed(
                Props.FromProducer(() => new ReproductionManagerActor()),
                receiverOptions.ManagerName);

            var senderOptions = SpeciationOptions.FromArgs(new[]
            {
                "--bind-host", "127.0.0.1",
                "--port", senderPort.ToString(),
                "--settings-host", "127.0.0.1",
                "--settings-port", "12010",
                "--settings-name", "SettingsMonitor"
            });
            await using var sender = await RemoteTestNode.StartAsync(
                SpeciationRemote.BuildConfig(senderOptions));

            var target = new PID(receiver.Address, receiverOptions.ManagerName);
            var response = await sender.Root.RequestAsync<Repro.ReproduceResult>(
                target,
                new Repro.AssessCompatibilityByArtifactsRequest
                {
                    ParentADef = parentARef,
                    ParentBDef = parentBRef,
                    Seed = 1234,
                    RunCount = 1,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxRegionSpanDiffRatio = 0f
                    }
                },
                TimeSpan.FromSeconds(5));

            Assert.NotNull(response);
            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.True(response.Report.SimilarityScore > 0f);
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

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class ExportProbeActor : IActor
    {
        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is ExportBrainDefinition request)
            {
                context.Respond(new BrainDefinitionReady { BrainId = request.BrainId });
            }

            return Task.CompletedTask;
        }
    }

    private sealed class VizProbeActor : IActor
    {
        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is VisualizationEvent message)
            {
                context.Respond(message);
            }

            return Task.CompletedTask;
        }
    }

    private sealed class DebugProbeActor : IActor
    {
        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is DebugOutbound message)
            {
                context.Respond(message);
            }

            return Task.CompletedTask;
        }
    }

    private sealed class RemoteTestNode : IAsyncDisposable
    {
        private readonly RemoteConfig _config;

        private RemoteTestNode(ActorSystem system, RemoteConfig config)
        {
            System = system;
            _config = config;
            Address = $"{config.AdvertisedHost ?? config.Host}:{config.AdvertisedPort ?? config.Port}";
        }

        public ActorSystem System { get; }

        public IRootContext Root => System.Root;

        public string Address { get; }

        public static async Task<RemoteTestNode> StartAsync(RemoteConfig config)
        {
            var system = new ActorSystem();
            system.WithRemote(config);
            await system.Remote().StartAsync();
            return new RemoteTestNode(system, config);
        }

        public async ValueTask DisposeAsync()
        {
            await System.Remote().ShutdownAsync(true);
            await System.ShutdownAsync();
        }
    }
}
