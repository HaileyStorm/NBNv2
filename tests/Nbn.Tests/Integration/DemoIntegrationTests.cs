using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Google.Protobuf;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.Brain;
using Nbn.Runtime.HiveMind;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.HiveMind;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;
using Xunit;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Tests.Integration;

[Collection("Distributed")]
public class DemoIntegrationTests
{
    static DemoIntegrationTests()
    {
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    [Fact]
    public async Task DemoStyle_EndToEnd_Produces_Output()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-demo-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            await using var hiveNode = await RemoteTestNode.StartAsync(BuildHiveMindConfig(GetFreePort()));
            await using var brainNode = await RemoteTestNode.StartAsync(BuildRemoteConfig(GetFreePort()));
            await using var regionNode = await RemoteTestNode.StartAsync(BuildRemoteConfig(GetFreePort()));

            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var nbnBytes = BuildDemoNbn();
            var manifest = await store.StoreAsync(new MemoryStream(nbnBytes), "application/x-nbn");
            var nbnRef = BuildArtifactRef(manifest);

            var options = CreateOptions(hiveNode.Port);
            var hiveMindLocal = hiveNode.Root.SpawnNamed(
                Props.FromProducer(() => new HiveMindActor(options)),
                HiveMindNames.HiveMind);
            var hiveMindRemote = new PID(hiveNode.Address, hiveMindLocal.Id);

            var brainId = Guid.NewGuid();
            var routerPid = brainNode.Root.SpawnNamed(
                Props.FromProducer(() => new BrainSignalRouterActor(brainId)),
                "demo-router");
            var brainRootPid = brainNode.Root.SpawnNamed(
                Props.FromProducer(() => new BrainRootActor(brainId, hiveMindRemote, autoSpawnSignalRouter: false)),
                "BrainRoot");
            brainNode.Root.Send(brainRootPid, new SetSignalRouter(routerPid));

            var router = await WaitForSignalRouter(brainNode.Root, brainRootPid, TimeSpan.FromSeconds(5));
            var routerRemote = EnsureAddress(router, brainNode.Address);

            var outputTcs = new TaskCompletionSource<OutputEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
            var outputSink = brainNode.Root.Spawn(Props.FromProducer(() => new OutputSinkActor(brainId, outputTcs)));
            var outputRemote = EnsureAddress(outputSink, brainNode.Address);

            var region1Load = await RegionShardArtifactLoader.LoadAsync(
                store,
                nbnRef,
                nbsRef: null,
                regionId: 1,
                neuronStart: 0,
                neuronCount: 1,
                expectedBrainId: brainId);

            var routing = RegionShardRoutingTable.CreateSingleShard(region1Load.Header.Regions);

            var region1Config = new RegionShardActorConfig(
                brainId,
                ShardId32.From(1, 0),
                routerRemote,
                OutputSink: null,
                TickSink: hiveMindRemote,
                routing);
            var region1Pid = regionNode.Root.Spawn(Props.FromProducer(() => new RegionShardActor(region1Load.State, region1Config)));

            var region31Load = await RegionShardArtifactLoader.LoadAsync(
                store,
                nbnRef,
                nbsRef: null,
                regionId: NbnConstants.OutputRegionId,
                neuronStart: 0,
                neuronCount: 1,
                expectedBrainId: brainId);

            var region31Config = new RegionShardActorConfig(
                brainId,
                ShardId32.From(NbnConstants.OutputRegionId, 0),
                routerRemote,
                outputRemote,
                TickSink: hiveMindRemote,
                routing);
            var region31Pid = regionNode.Root.Spawn(Props.FromProducer(() => new RegionShardActor(region31Load.State, region31Config)));

            var region1Remote = EnsureAddress(region1Pid, regionNode.Address);
            var region31Remote = EnsureAddress(region31Pid, regionNode.Address);

            regionNode.Root.Send(hiveMindRemote, new Nbn.Proto.Control.RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 1,
                ShardIndex = 0,
                ShardPid = PidLabel(region1Remote)
            });
            regionNode.Root.Send(hiveMindRemote, new Nbn.Proto.Control.RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = (uint)NbnConstants.OutputRegionId,
                ShardIndex = 0,
                ShardPid = PidLabel(region31Remote)
            });

            await WaitForStatus(
                hiveNode.Root,
                hiveMindLocal,
                status => status.RegisteredBrains == 1 && status.RegisteredShards >= 2,
                TimeSpan.FromSeconds(5));

            await WaitForRoutingTable(brainNode.Root, router, table => table.Count == 2, TimeSpan.FromSeconds(5));

            hiveNode.Root.Send(hiveMindLocal, new StartTickLoop());

            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var output = await outputTcs.Task.WaitAsync(timeoutCts.Token);

            Assert.True(output.BrainId.TryToGuid(out var outputBrain) && outputBrain == brainId);
            Assert.Equal(0u, output.OutputIndex);
            Assert.True(output.TickId >= 2);
            Assert.True(output.Value > 0f);

            var status = await WaitForStatus(
                hiveNode.Root,
                hiveMindLocal,
                s => s.LastCompletedTickId >= output.TickId && s.PendingCompute == 0 && s.PendingDeliver == 0,
                TimeSpan.FromSeconds(5));

            hiveNode.Root.Send(hiveMindLocal, new StopTickLoop());

            Assert.True(status.LastCompletedTickId >= output.TickId);
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }

    private static ArtifactRef BuildArtifactRef(ArtifactManifest manifest)
    {
        return new ArtifactRef
        {
            Sha256 = new Sha256 { Value = ByteString.CopyFrom(manifest.ArtifactId.Bytes.ToArray()) },
            MediaType = manifest.MediaType,
            SizeBytes = (ulong)manifest.ByteLength
        };
    }

    private static byte[] BuildDemoNbn()
    {
        var stride = 1024u;
        var sections = new List<NbnRegionSection>();
        var directory = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        ulong offset = NbnBinary.NbnHeaderBytes;

        offset = AddRegionSection(0, 1, stride, ref directory, sections, offset);

        var demoAxons = new[]
        {
            new AxonRecord(strengthCode: 31, targetNeuronId: 0, targetRegionId: 1),
            new AxonRecord(strengthCode: 31, targetNeuronId: 0, targetRegionId: NbnConstants.OutputRegionId)
        };

        offset = AddRegionSection(
            1,
            1,
            stride,
            ref directory,
            sections,
            offset,
            neuronFactory: _ => new NeuronRecord(
                axonCount: 2,
                paramBCode: 0,
                paramACode: 40,
                activationThresholdCode: 0,
                preActivationThresholdCode: 0,
                resetFunctionId: 0,
                activationFunctionId: 17,
                accumulationFunctionId: 0,
                exists: true),
            axons: demoAxons);

        offset = AddRegionSection(
            NbnConstants.OutputRegionId,
            1,
            stride,
            ref directory,
            sections,
            offset,
            neuronFactory: _ => new NeuronRecord(
                axonCount: 0,
                paramBCode: 0,
                paramACode: 0,
                activationThresholdCode: 0,
                preActivationThresholdCode: 0,
                resetFunctionId: 0,
                activationFunctionId: 1,
                accumulationFunctionId: 0,
                exists: true));

        var header = new NbnHeaderV2(
            "NBN2",
            2,
            1,
            10,
            brainSeed: 1,
            axonStride: stride,
            flags: 0,
            quantization: QuantizationSchemas.DefaultNbn,
            regions: directory);

        return NbnBinary.WriteNbn(header, sections);
    }

    private static ulong AddRegionSection(
        int regionId,
        uint neuronSpan,
        uint stride,
        ref NbnRegionDirectoryEntry[] directory,
        List<NbnRegionSection> sections,
        ulong offset,
        Func<int, NeuronRecord>? neuronFactory = null,
        AxonRecord[]? axons = null)
    {
        var neurons = new NeuronRecord[neuronSpan];
        for (var i = 0; i < neurons.Length; i++)
        {
            neurons[i] = neuronFactory?.Invoke(i) ?? new NeuronRecord(
                axonCount: 0,
                paramBCode: 0,
                paramACode: 0,
                activationThresholdCode: 0,
                preActivationThresholdCode: 0,
                resetFunctionId: 0,
                activationFunctionId: 1,
                accumulationFunctionId: 0,
                exists: true);
        }

        ulong totalAxons = 0;
        for (var i = 0; i < neurons.Length; i++)
        {
            totalAxons += neurons[i].AxonCount;
        }

        axons ??= Array.Empty<AxonRecord>();
        if ((ulong)axons.Length != totalAxons)
        {
            throw new InvalidOperationException($"Region {regionId} axon count mismatch. Expected {totalAxons}, got {axons.Length}.");
        }

        var checkpointCount = (uint)((neuronSpan + stride - 1) / stride + 1);
        var checkpoints = new ulong[checkpointCount];
        var checkpointIndex = 1;
        var running = 0UL;
        uint nextBoundary = stride;
        for (var i = 0; i < neurons.Length; i++)
        {
            running += neurons[i].AxonCount;
            if ((uint)(i + 1) == nextBoundary && checkpointIndex < checkpointCount)
            {
                checkpoints[checkpointIndex++] = running;
                nextBoundary += stride;
            }
        }

        checkpoints[0] = 0;
        checkpoints[checkpointCount - 1] = running;
        var section = new NbnRegionSection(
            (byte)regionId,
            neuronSpan,
            totalAxons,
            stride,
            checkpointCount,
            checkpoints,
            neurons,
            axons);

        directory[regionId] = new NbnRegionDirectoryEntry(neuronSpan, totalAxons, offset, 0);
        sections.Add(section);
        return offset + (ulong)section.ByteLength;
    }

    private static RemoteConfig BuildHiveMindConfig(int port)
    {
        var options = CreateOptions(port);
        return HiveMindRemote.BuildConfig(options);
    }

    private static RemoteConfig BuildRemoteConfig(int port)
    {
        return RemoteConfig
            .BindTo("127.0.0.1", port)
            .WithProtoMessages(
                NbnCommonReflection.Descriptor,
                NbnControlReflection.Descriptor,
                NbnSignalsReflection.Descriptor,
                NbnIoReflection.Descriptor);
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static PID EnsureAddress(PID pid, string address)
        => string.IsNullOrWhiteSpace(pid.Address) ? new PID(address, pid.Id) : pid;

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private static async Task<PID> WaitForSignalRouter(IRootContext root, PID brainRoot, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            var response = await root.RequestAsync<SignalRouterResponse>(brainRoot, new GetSignalRouter());
            if (response.SignalRouter is not null)
            {
                return response.SignalRouter;
            }

            await Task.Delay(20);
        }

        throw new TimeoutException("Signal router did not become available.");
    }

    private static async Task WaitForRoutingTable(
        IRootContext root,
        PID router,
        Func<RoutingTableSnapshot, bool> predicate,
        TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            var snapshot = await root.RequestAsync<RoutingTableSnapshot>(router, new GetRoutingTable());
            if (predicate(snapshot))
            {
                return;
            }

            await Task.Delay(20);
        }

        throw new TimeoutException("Routing table did not reach expected state.");
    }

    private static async Task<HiveMindStatus> WaitForStatus(
        IRootContext root,
        PID hiveMind,
        Func<HiveMindStatus, bool> predicate,
        TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        HiveMindStatus? lastStatus = null;
        while (sw.Elapsed < timeout)
        {
            var status = await root.RequestAsync<HiveMindStatus>(hiveMind, new GetHiveMindStatus());
            lastStatus = status;
            if (predicate(status))
            {
                return status;
            }

            await Task.Delay(20);
        }

        throw new TimeoutException($"HiveMind status did not reach expected state. Last: brains={lastStatus?.RegisteredBrains}, shards={lastStatus?.RegisteredShards}, pendingCompute={lastStatus?.PendingCompute}, pendingDeliver={lastStatus?.PendingDeliver}.");
    }

    private static HiveMindOptions CreateOptions(
        int port,
        float targetTickHz = 50f,
        float minTickHz = 10f,
        int computeTimeoutMs = 500,
        int deliverTimeoutMs = 500,
        float backpressureDecay = 0.9f,
        float backpressureRecovery = 1.1f,
        int lateBackpressureThreshold = 2)
        => new(
            BindHost: "127.0.0.1",
            Port: port,
            AdvertisedHost: null,
            AdvertisedPort: null,
            TargetTickHz: targetTickHz,
            MinTickHz: minTickHz,
            ComputeTimeoutMs: computeTimeoutMs,
            DeliverTimeoutMs: deliverTimeoutMs,
            BackpressureDecay: backpressureDecay,
            BackpressureRecovery: backpressureRecovery,
            LateBackpressureThreshold: lateBackpressureThreshold,
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
            SettingsName: "SettingsMonitor");

    private sealed class OutputSinkActor : IActor
    {
        private readonly Guid _brainId;
        private readonly TaskCompletionSource<OutputEvent> _tcs;

        public OutputSinkActor(Guid brainId, TaskCompletionSource<OutputEvent> tcs)
        {
            _brainId = brainId;
            _tcs = tcs;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is OutputEvent output
                && output.BrainId.TryToGuid(out var brainId)
                && brainId == _brainId)
            {
                _tcs.TrySetResult(output);
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
        public int Port => _config.AdvertisedPort ?? _config.Port;

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
