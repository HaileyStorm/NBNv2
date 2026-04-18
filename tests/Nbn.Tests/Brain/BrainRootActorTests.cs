using Nbn.Proto.Io;
using Nbn.Runtime.Brain;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;

namespace Nbn.Tests.Brain;

public sealed class BrainRootActorTests
{
    [Fact]
    public async Task SetSignalRouter_ReplaysCachedRoutingAndIoRegistration()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var routingTcs = new TaskCompletionSource<SetRoutingTable>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ioGatewayTcs = new TaskCompletionSource<RegisterIoGateway>(TaskCreationOptions.RunContinuationsAsynchronously);
        var routerPid = root.Spawn(Props.FromProducer(() => new RouterProbeActor(routingTcs, ioGatewayTcs)));
        var brainRootPid = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, autoSpawnSignalRouter: false)));

        var route = new ShardRoute(
            ShardId32.From(regionId: 7, shardIndex: 0).Value,
            new PID("127.0.0.1:12041", "region-shard"));

        root.Send(brainRootPid, new SetRoutingTable(new RoutingTableSnapshot(new[] { route })));
        root.Send(brainRootPid, new RegisterIoGateway
        {
            BrainId = brainId.ToProtoUuid(),
            IoGatewayPid = "127.0.0.1:12042/IoGateway",
            InputCoordinatorMode = Nbn.Proto.Control.InputCoordinatorMode.ReplayLatestVector,
            InputTickDrainArmed = true
        });
        root.Send(brainRootPid, new SetSignalRouter(routerPid));

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var routing = await routingTcs.Task.WaitAsync(timeoutCts.Token);
        var ioGateway = await ioGatewayTcs.Task.WaitAsync(timeoutCts.Token);
        var response = await root.RequestAsync<SignalRouterResponse>(brainRootPid, new GetSignalRouter(), TimeSpan.FromSeconds(2));

        Assert.Equal(routerPid, response.SignalRouter);
        Assert.Single(routing.Table.Routes);
        Assert.Equal(route, routing.Table.Routes[0]);
        Assert.Equal("127.0.0.1:12042/IoGateway", ioGateway.IoGatewayPid);
        Assert.Equal(Nbn.Proto.Control.InputCoordinatorMode.ReplayLatestVector, ioGateway.InputCoordinatorMode);
        Assert.True(ioGateway.InputTickDrainArmed);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RequestStyleInputVector_ForwardsThroughAttachedSignalRouter_AndReturnsAck()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputTcs = new TaskCompletionSource<InputVector>(TaskCreationOptions.RunContinuationsAsynchronously);
        var routerPid = root.Spawn(Props.FromProducer(() => new RouterProbeActor(inputVectorTcs: inputTcs)));
        var brainRootPid = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, autoSpawnSignalRouter: false)));
        root.Send(brainRootPid, new SetSignalRouter(routerPid));

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var ack = await root.RequestAsync<IoCommandAck>(
                brainRootPid,
                new InputVector
                {
                    BrainId = brainId.ToProtoUuid(),
                    Values = { 0.25f, 0.75f }
                },
                TimeSpan.FromSeconds(2))
            .WaitAsync(timeoutCts.Token);
        var forwarded = await inputTcs.Task.WaitAsync(timeoutCts.Token);

        Assert.True(ack.Success, ack.Message);
        Assert.Equal("input_vector", ack.Command);
        Assert.True(forwarded.BrainId.TryToGuid(out var forwardedBrainId));
        Assert.Equal(brainId, forwardedBrainId);
        Assert.Equal(new[] { 0.25f, 0.75f }, forwarded.Values.ToArray());

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RequestStyleInputVector_ReturnsFailure_WhenSignalRouterUnavailable()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var brainRootPid = root.Spawn(Props.FromProducer(() => new BrainRootActor(brainId, autoSpawnSignalRouter: false)));

        var ack = await root.RequestAsync<IoCommandAck>(
            brainRootPid,
            new InputVector
            {
                BrainId = brainId.ToProtoUuid(),
                Values = { 1f }
            },
            TimeSpan.FromSeconds(2));

        Assert.False(ack.Success);
        Assert.Equal("input_vector", ack.Command);
        Assert.Equal("signal_router_unavailable", ack.Message);

        await system.ShutdownAsync();
    }

    private sealed class RouterProbeActor : IActor
    {
        private readonly TaskCompletionSource<SetRoutingTable>? _routingTcs;
        private readonly TaskCompletionSource<RegisterIoGateway>? _ioGatewayTcs;
        private readonly TaskCompletionSource<InputVector>? _inputVectorTcs;

        public RouterProbeActor(
            TaskCompletionSource<SetRoutingTable>? routingTcs = null,
            TaskCompletionSource<RegisterIoGateway>? ioGatewayTcs = null,
            TaskCompletionSource<InputVector>? inputVectorTcs = null)
        {
            _routingTcs = routingTcs;
            _ioGatewayTcs = ioGatewayTcs;
            _inputVectorTcs = inputVectorTcs;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case SetRoutingTable setRouting:
                    _routingTcs?.TrySetResult(setRouting);
                    break;
                case RegisterIoGateway registerIoGateway:
                    _ioGatewayTcs?.TrySetResult(registerIoGateway);
                    break;
                case InputVector inputVector:
                    _inputVectorTcs?.TrySetResult(inputVector);
                    context.Respond(new IoCommandAck
                    {
                        BrainId = inputVector.BrainId,
                        Command = "input_vector",
                        Success = true,
                        Message = "accepted"
                    });
                    break;
            }

            return Task.CompletedTask;
        }
    }
}
