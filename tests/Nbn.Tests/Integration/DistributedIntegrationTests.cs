using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Nbn.Proto;
using Nbn.Proto.Control;
using ProtoControl = Nbn.Proto.Control;
using Nbn.Proto.Signal;
using Nbn.Runtime.Brain;
using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Nbn.Shared.HiveMind;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;
using Xunit;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Tests.Integration;

[Collection("Distributed")]
public class DistributedIntegrationTests
{
    static DistributedIntegrationTests()
    {
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    [Fact]
    public async Task TickBarrier_Completes_Across_Remote_Systems()
    {
        var hivePort = GetFreePort();
        var brainPort = GetFreePort();
        var shardPort = GetFreePort();

        await using var hiveNode = await RemoteTestNode.StartAsync(BuildHiveMindConfig(hivePort));
        await using var brainNode = await RemoteTestNode.StartAsync(BuildRemoteConfig(brainPort));
        await using var shardNode = await RemoteTestNode.StartAsync(BuildRemoteConfig(shardPort));

        var options = CreateOptions(hivePort);
        var hiveMindLocal = hiveNode.Root.SpawnNamed(
            Props.FromProducer(() => new HiveMindActor(options)),
            HiveMindNames.HiveMind);

        var hiveMindRemote = new PID(hiveNode.Address, hiveMindLocal.Id);

        var brainId = Guid.NewGuid();
        var brainRoot = brainNode.Root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, hiveMindRemote)));
        var router = await WaitForSignalRouter(brainNode.Root, brainRoot, TimeSpan.FromSeconds(5));
        var routerRemote = EnsureAddress(router, brainNode.Address);

        var signalTcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var sink = shardNode.Root.Spawn(Props.FromProducer(() => new SignalSinkActor(signalTcs, expected: 1)));

        var shardId = ShardId32.From(1, 0);
        var shardActor = shardNode.Root.Spawn(
            Props.FromProducer(() => new RemoteShardActor(brainId, shardId, routerRemote, sink, hiveMindRemote)));
        var shardRemote = EnsureAddress(shardActor, shardNode.Address);

        shardNode.Root.Send(hiveMindRemote, new Nbn.Proto.Control.RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(shardRemote)
        });

        await WaitForStatus(hiveNode.Root, hiveMindLocal, s => s.RegisteredBrains == 1, TimeSpan.FromSeconds(5));
        await WaitForRoutingTable(brainNode.Root, router, table => table.Count == 1, TimeSpan.FromSeconds(5));

        hiveNode.Root.Send(hiveMindLocal, new StartTickLoop());

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await signalTcs.Task.WaitAsync(timeoutCts.Token);

        var status = await WaitForStatus(
            hiveNode.Root,
            hiveMindLocal,
            s => s.LastCompletedTickId >= 1 && s.PendingCompute == 0 && s.PendingDeliver == 0,
            TimeSpan.FromSeconds(5));

        hiveNode.Root.Send(hiveMindLocal, new StopTickLoop());

        Assert.True(status.LastCompletedTickId >= 1);
    }

    [Fact]
    public async Task RegisterBrain_Remote_Assigns_Addresses()
    {
        var hivePort = GetFreePort();
        var brainPort = GetFreePort();

        await using var hiveNode = await RemoteTestNode.StartAsync(BuildHiveMindConfig(hivePort));
        await using var brainNode = await RemoteTestNode.StartAsync(BuildRemoteConfig(brainPort));

        var options = CreateOptions(hivePort);
        var hiveMindLocal = hiveNode.Root.SpawnNamed(
            Props.FromProducer(() => new HiveMindActor(options)),
            HiveMindNames.HiveMind);

        var hiveMindRemote = new PID(hiveNode.Address, hiveMindLocal.Id);

        var brainId = Guid.NewGuid();
        var brainRoot = brainNode.Root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, hiveMindRemote)));
        _ = await WaitForSignalRouter(brainNode.Root, brainRoot, TimeSpan.FromSeconds(5));

        var info = await WaitForRoutingInfo(hiveNode.Root, hiveMindLocal, brainId, TimeSpan.FromSeconds(5));

        Assert.False(string.IsNullOrWhiteSpace(info.BrainRootPid));
        Assert.False(string.IsNullOrWhiteSpace(info.SignalRouterPid));
        Assert.StartsWith(brainNode.Address + "/", info.BrainRootPid);
        Assert.StartsWith(brainNode.Address + "/", info.SignalRouterPid);
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
                NbnSignalsReflection.Descriptor);
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

    private static async Task<ProtoControl.HiveMindStatus> WaitForStatus(
        IRootContext root,
        PID hiveMind,
        Func<ProtoControl.HiveMindStatus, bool> predicate,
        TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        ProtoControl.HiveMindStatus? lastStatus = null;
        while (sw.Elapsed < timeout)
        {
            var status = await root.RequestAsync<ProtoControl.HiveMindStatus>(
                hiveMind,
                new ProtoControl.GetHiveMindStatus());
            lastStatus = status;
            if (predicate(status))
            {
                return status;
            }

            await Task.Delay(20);
        }

        throw new TimeoutException($"HiveMind status did not reach expected state. Last: brains={lastStatus?.RegisteredBrains}, shards={lastStatus?.RegisteredShards}, pendingCompute={lastStatus?.PendingCompute}, pendingDeliver={lastStatus?.PendingDeliver}.");
    }

    private static async Task<ProtoControl.BrainRoutingInfo> WaitForRoutingInfo(
        IRootContext root,
        PID hiveMind,
        Guid brainId,
        TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        ProtoControl.BrainRoutingInfo? lastInfo = null;
        while (sw.Elapsed < timeout)
        {
            var info = await root.RequestAsync<ProtoControl.BrainRoutingInfo>(
                hiveMind,
                new ProtoControl.GetBrainRouting { BrainId = brainId.ToProtoUuid() });
            lastInfo = info;
            if (!string.IsNullOrWhiteSpace(info.BrainRootPid) || !string.IsNullOrWhiteSpace(info.SignalRouterPid))
            {
                return info;
            }

            await Task.Delay(20);
        }

        throw new TimeoutException($"HiveMind routing info did not become available. Last: root={lastInfo?.BrainRootPid}, router={lastInfo?.SignalRouterPid}.");
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

    private sealed record SignalDelivered;

    private sealed class SignalSinkActor : IActor
    {
        private readonly TaskCompletionSource<int> _tcs;
        private readonly int _expected;
        private int _count;

        public SignalSinkActor(TaskCompletionSource<int> tcs, int expected)
        {
            _tcs = tcs;
            _expected = expected;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is SignalDelivered)
            {
                if (Interlocked.Increment(ref _count) >= _expected)
                {
                    _tcs.TrySetResult(_count);
                }
            }

            return Task.CompletedTask;
        }
    }

    private sealed class RemoteShardActor : IActor
    {
        private readonly Guid _brainId;
        private readonly ShardId32 _shardId;
        private readonly uint _regionId;
        private readonly PID _router;
        private readonly PID _signalSink;
        private readonly PID _tickSink;

        public RemoteShardActor(Guid brainId, ShardId32 shardId, PID router, PID signalSink, PID tickSink)
        {
            _brainId = brainId;
            _shardId = shardId;
            _regionId = (uint)shardId.RegionId;
            _router = router;
            _signalSink = signalSink;
            _tickSink = tickSink;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case TickCompute tick:
                    HandleTickCompute(context, tick);
                    break;
                case SignalBatch batch:
                    HandleSignalBatch(context, batch);
                    break;
            }

            return Task.CompletedTask;
        }

        private void HandleTickCompute(IContext context, TickCompute tick)
        {
            var outbox = new OutboxBatch
            {
                BrainId = _brainId.ToProtoUuid(),
                TickId = tick.TickId,
                DestRegionId = _regionId,
                DestShardId = _shardId.ToProtoShardId32()
            };
            outbox.Contribs.Add(new Contribution { TargetNeuronId = 0, Value = 1f });
            context.Send(_router, outbox);

            var done = new TickComputeDone
            {
                TickId = tick.TickId,
                BrainId = _brainId.ToProtoUuid(),
                RegionId = _regionId,
                ShardId = _shardId.ToProtoShardId32(),
                ComputeMs = 1,
                TickCostTotal = 0,
                CostAccum = 0,
                CostActivation = 0,
                CostReset = 0,
                CostDistance = 0,
                CostRemote = 0,
                FiredCount = 0,
                OutBatches = 1,
                OutContribs = 1
            };

            context.Send(_tickSink, done);
        }

        private void HandleSignalBatch(IContext context, SignalBatch batch)
        {
            var ack = new SignalBatchAck
            {
                BrainId = _brainId.ToProtoUuid(),
                RegionId = _regionId,
                ShardId = _shardId.ToProtoShardId32(),
                TickId = batch.TickId
            };

            context.Send(context.Sender ?? _router, ack);
            context.Send(_signalSink, new SignalDelivered());
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
