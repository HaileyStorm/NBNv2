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
            IoGatewayPid = "127.0.0.1:12042/IoGateway"
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

        await system.ShutdownAsync();
    }

    private sealed class RouterProbeActor : IActor
    {
        private readonly TaskCompletionSource<SetRoutingTable> _routingTcs;
        private readonly TaskCompletionSource<RegisterIoGateway> _ioGatewayTcs;

        public RouterProbeActor(
            TaskCompletionSource<SetRoutingTable> routingTcs,
            TaskCompletionSource<RegisterIoGateway> ioGatewayTcs)
        {
            _routingTcs = routingTcs;
            _ioGatewayTcs = ioGatewayTcs;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case SetRoutingTable setRouting:
                    _routingTcs.TrySetResult(setRouting);
                    break;
                case RegisterIoGateway registerIoGateway:
                    _ioGatewayTcs.TrySetResult(registerIoGateway);
                    break;
            }

            return Task.CompletedTask;
        }
    }
}
