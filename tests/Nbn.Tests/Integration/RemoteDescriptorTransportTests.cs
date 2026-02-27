using System.Net;
using System.Net.Sockets;
using Nbn.Proto;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Viz;
using Nbn.Runtime.HiveMind;
using Nbn.Runtime.WorkerNode;
using Nbn.Shared;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

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
