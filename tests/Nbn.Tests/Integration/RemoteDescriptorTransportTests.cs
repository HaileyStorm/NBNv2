using System.Net;
using System.Net.Sockets;
using Microsoft.Data.Sqlite;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Proto.Viz;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.HiveMind;
using Nbn.Runtime.IO;
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
    public async Task IoGateway_Forwards_InputWrite_To_WorkerHosted_InputCoordinator()
    {
        var receiverPort = GetFreePort();
        var senderPort = GetFreePort();
        var workerId = Guid.NewGuid();
        var brainId = Guid.NewGuid();

        var options = WorkerNodeOptions.FromArgs(new[]
        {
            "--bind-host", "127.0.0.1",
            "--port", receiverPort.ToString(),
            "--settings-host", "127.0.0.1",
            "--settings-port", "12010",
            "--settings-name", "SettingsMonitor"
        });

        await using var receiver = await RemoteTestNode.StartAsync(WorkerNodeRemote.BuildConfig(options));
        var workerRoot = receiver.Root.SpawnNamed(
            Props.FromProducer(() => new WorkerNodeActor(workerId, receiver.Address)),
            options.RootActorName);

        var senderConfig = RemoteConfig.BindToLocalhost(senderPort).WithProtoMessages(
            NbnCommonReflection.Descriptor,
            NbnControlReflection.Descriptor,
            NbnIoReflection.Descriptor,
            NbnSignalsReflection.Descriptor);
        await using var sender = await RemoteTestNode.StartAsync(senderConfig);

        var assignment = new PlacementAssignment
        {
            AssignmentId = $"assign-input-{brainId:N}",
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = 1,
            Target = PlacementAssignmentTarget.PlacementTargetInputCoordinator,
            WorkerNodeId = workerId.ToProtoUuid(),
            RegionId = 0,
            ShardIndex = 0,
            NeuronStart = 0,
            NeuronCount = 4,
            ActorName = $"brain-{brainId:N}-input"
        };

        var workerTarget = new PID(receiver.Address, workerRoot.Id);
        var assignmentAck = await sender.Root.RequestAsync<PlacementAssignmentAck>(
            workerTarget,
            new PlacementAssignmentRequest { Assignment = assignment },
            TimeSpan.FromSeconds(5));
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, assignmentAck.State);

        var reconcile = await sender.Root.RequestAsync<PlacementReconcileReport>(
            workerTarget,
            new PlacementReconcileRequest
            {
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = 1
            },
            TimeSpan.FromSeconds(5));
        var observed = Assert.Single(reconcile.Assignments);
        Assert.Equal(assignment.AssignmentId, observed.AssignmentId);
        Assert.False(string.IsNullOrWhiteSpace(observed.ActorPid));
        Assert.True(TryParsePid(observed.ActorPid, out var coordinatorPid));

        var directAck = await sender.Root.RequestAsync<IoCommandAck>(
            coordinatorPid,
            new InputWrite
            {
                BrainId = brainId.ToProtoUuid(),
                InputIndex = 0,
                Value = 0.5f
            },
            TimeSpan.FromSeconds(5));
        Assert.True(directAck.Success);

        var directDrain = await sender.Root.RequestAsync<InputDrain>(
            coordinatorPid,
            new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 8
            },
            TimeSpan.FromSeconds(5));
        var directContribution = Assert.Single(directDrain.Contribs);
        Assert.Equal(0u, directContribution.TargetNeuronId);
        Assert.Equal(0.5f, directContribution.Value);

        var gateway = sender.Root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateIoOptions())));
        sender.Root.Send(gateway, new Nbn.Proto.Io.RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            InputCoordinatorMode = InputCoordinatorMode.DirtyOnChange,
            InputCoordinatorPid = observed.ActorPid,
            IoGatewayOwnsInputCoordinator = false,
            IoGatewayOwnsOutputCoordinator = true
        });

        await WaitForAsync(
            async () =>
            {
                var info = await sender.Root.RequestAsync<BrainInfo>(
                    gateway,
                    new BrainInfoRequest { BrainId = brainId.ToProtoUuid() },
                    TimeSpan.FromSeconds(5));
                return info.InputWidth == 1;
            },
            timeout: TimeSpan.FromSeconds(5));

        sender.Root.Send(gateway, new InputWrite
        {
            BrainId = brainId.ToProtoUuid(),
            InputIndex = 0,
            Value = 0.75f
        });

        InputDrain? drain = null;
        await WaitForAsync(
            async () =>
            {
                drain = await sender.Root.RequestAsync<InputDrain>(
                    gateway,
                    new DrainInputs
                    {
                        BrainId = brainId.ToProtoUuid(),
                        TickId = 9
                    },
                    TimeSpan.FromSeconds(5));
                return drain.Contribs.Count == 1;
            },
            timeout: TimeSpan.FromSeconds(5));

        var contribution = Assert.Single(drain!.Contribs);
        Assert.Equal(0u, contribution.TargetNeuronId);
        Assert.Equal(0.75f, contribution.Value);
    }

    [Fact]
    public async Task IoGateway_Forwards_OutputSubscriptions_With_ExplicitSubscriberActor_To_WorkerHosted_OutputCoordinator()
    {
        var receiverPort = GetFreePort();
        var senderPort = GetFreePort();
        var workerId = Guid.NewGuid();
        var brainId = Guid.NewGuid();

        var options = WorkerNodeOptions.FromArgs(new[]
        {
            "--bind-host", "127.0.0.1",
            "--port", receiverPort.ToString(),
            "--settings-host", "127.0.0.1",
            "--settings-port", "12010",
            "--settings-name", "SettingsMonitor"
        });

        await using var receiver = await RemoteTestNode.StartAsync(WorkerNodeRemote.BuildConfig(options));
        var workerRoot = receiver.Root.SpawnNamed(
            Props.FromProducer(() => new WorkerNodeActor(workerId, receiver.Address)),
            options.RootActorName);

        var senderConfig = RemoteConfig.BindToLocalhost(senderPort).WithProtoMessages(
            NbnCommonReflection.Descriptor,
            NbnControlReflection.Descriptor,
            NbnIoReflection.Descriptor,
            NbnSignalsReflection.Descriptor);
        await using var sender = await RemoteTestNode.StartAsync(senderConfig);

        var assignment = new PlacementAssignment
        {
            AssignmentId = $"assign-output-{brainId:N}",
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = 1,
            Target = PlacementAssignmentTarget.PlacementTargetOutputCoordinator,
            WorkerNodeId = workerId.ToProtoUuid(),
            RegionId = NbnConstants.OutputRegionId,
            ShardIndex = 0,
            NeuronStart = 0,
            NeuronCount = 1,
            ActorName = $"brain-{brainId:N}-output"
        };

        var workerTarget = new PID(receiver.Address, workerRoot.Id);
        var assignmentAck = await sender.Root.RequestAsync<PlacementAssignmentAck>(
            workerTarget,
            new PlacementAssignmentRequest { Assignment = assignment },
            TimeSpan.FromSeconds(5));
        Assert.Equal(PlacementAssignmentState.PlacementAssignmentReady, assignmentAck.State);

        var reconcile = await sender.Root.RequestAsync<PlacementReconcileReport>(
            workerTarget,
            new PlacementReconcileRequest
            {
                BrainId = brainId.ToProtoUuid(),
                PlacementEpoch = 1
            },
            TimeSpan.FromSeconds(5));
        var observed = Assert.Single(reconcile.Assignments);
        Assert.Equal(assignment.AssignmentId, observed.AssignmentId);
        Assert.False(string.IsNullOrWhiteSpace(observed.ActorPid));
        Assert.True(TryParsePid(observed.ActorPid, out var coordinatorPid));

        var gateway = sender.Root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateIoOptions())));
        sender.Root.Send(gateway, new Nbn.Proto.Io.RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            OutputCoordinatorPid = observed.ActorPid,
            IoGatewayOwnsInputCoordinator = true,
            IoGatewayOwnsOutputCoordinator = false
        });

        await WaitForAsync(
            async () =>
            {
                var info = await sender.Root.RequestAsync<BrainInfo>(
                    gateway,
                    new BrainInfoRequest { BrainId = brainId.ToProtoUuid() },
                    TimeSpan.FromSeconds(5));
                return info.OutputWidth == 1;
            },
            timeout: TimeSpan.FromSeconds(5));

        var singleTcs = new TaskCompletionSource<OutputEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        var vectorTcs = new TaskCompletionSource<OutputVectorEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        var probePid = sender.Root.SpawnNamed(
            Props.FromProducer(() => new OutputProbeActor(singleTcs, vectorTcs)),
            "output-probe");
        var explicitSubscriberActor = $"{sender.Address}/{probePid.Id}";

        sender.Root.Send(gateway, new SubscribeOutputs
        {
            BrainId = brainId.ToProtoUuid(),
            SubscriberActor = explicitSubscriberActor
        });
        sender.Root.Send(gateway, new SubscribeOutputsVector
        {
            BrainId = brainId.ToProtoUuid(),
            SubscriberActor = explicitSubscriberActor
        });

        var tickId = 20UL;
        await WaitForAsync(
            async () =>
            {
                sender.Root.Send(coordinatorPid, new OutputEvent
                {
                    BrainId = brainId.ToProtoUuid(),
                    OutputIndex = 0,
                    Value = 0.9f,
                    TickId = tickId
                });
                sender.Root.Send(coordinatorPid, new OutputVectorEvent
                {
                    BrainId = brainId.ToProtoUuid(),
                    TickId = tickId,
                    Values = { 0.9f }
                });
                tickId++;

                await Task.Delay(20).ConfigureAwait(false);
                return singleTcs.Task.IsCompletedSuccessfully && vectorTcs.Task.IsCompletedSuccessfully;
            },
            timeout: TimeSpan.FromSeconds(5));

        var single = await singleTcs.Task;
        var vector = await vectorTcs.Task;
        Assert.Equal(0u, single.OutputIndex);
        Assert.Equal(0.9f, single.Value);
        Assert.Equal([0.9f], vector.Values.ToArray());
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

    private static IoOptions CreateIoOptions()
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

    private static async Task WaitForAsync(Func<Task<bool>> predicate, TimeSpan timeout)
    {
        var started = DateTime.UtcNow;
        while (DateTime.UtcNow - started < timeout)
        {
            if (await predicate().ConfigureAwait(false))
            {
                return;
            }

            await Task.Delay(20).ConfigureAwait(false);
        }

        throw new TimeoutException($"Condition was not met within {timeout}.");
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

    private sealed class OutputProbeActor : IActor
    {
        private readonly TaskCompletionSource<OutputEvent> _singleTcs;
        private readonly TaskCompletionSource<OutputVectorEvent> _vectorTcs;

        public OutputProbeActor(
            TaskCompletionSource<OutputEvent> singleTcs,
            TaskCompletionSource<OutputVectorEvent> vectorTcs)
        {
            _singleTcs = singleTcs;
            _vectorTcs = vectorTcs;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case OutputEvent output:
                    _singleTcs.TrySetResult(output.Clone());
                    break;
                case OutputVectorEvent vector:
                    _vectorTcs.TrySetResult(vector.Clone());
                    break;
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

    private static bool TryParsePid(string? value, out PID pid)
    {
        pid = new PID();
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        var slashIndex = trimmed.IndexOf('/');
        if (slashIndex <= 0)
        {
            pid.Id = trimmed;
            return true;
        }

        var address = trimmed[..slashIndex];
        var id = trimmed[(slashIndex + 1)..];
        if (string.IsNullOrWhiteSpace(id))
        {
            return false;
        }

        pid = new PID(address, id);
        return true;
    }
}
