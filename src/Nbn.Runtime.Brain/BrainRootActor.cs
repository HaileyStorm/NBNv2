using Nbn.Proto.Control;
using Proto;

namespace Nbn.Runtime.Brain;

public sealed class BrainRootActor : IActor
{
    private readonly Props? _signalRouterProps;
    private PID? _signalRouterPid;
    private RoutingTableSnapshot _routingSnapshot = RoutingTableSnapshot.Empty;

    public BrainRootActor(Guid brainId, Props? signalRouterProps = null)
    {
        _signalRouterProps = signalRouterProps ?? Props.FromProducer(() => new BrainSignalRouterActor(brainId));
    }

    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
                EnsureSignalRouter(context);
                break;
            case SetSignalRouter setSignalRouter:
                AttachSignalRouter(context, setSignalRouter.SignalRouter);
                break;
            case GetSignalRouter:
                context.Respond(new SignalRouterResponse(_signalRouterPid));
                break;
            case SetRoutingTable setRouting:
                ApplyRoutingTable(context, setRouting.Table);
                break;
            case GetRoutingTable:
                context.Respond(_routingSnapshot);
                break;
            case TickCompute tickCompute:
                ForwardToSignalRouter(context, tickCompute);
                break;
            case TickDeliver tickDeliver:
                ForwardToSignalRouter(context, tickDeliver);
                break;
            case TickComputeDone tickComputeDone:
                ForwardToParent(context, tickComputeDone);
                break;
            case TickDeliverDone tickDeliverDone:
                ForwardToParent(context, tickDeliverDone);
                break;
            case Terminated terminated:
                HandleTerminated(terminated);
                break;
        }

        return Task.CompletedTask;
    }

    private void EnsureSignalRouter(IContext context)
    {
        if (_signalRouterPid is not null || _signalRouterProps is null)
        {
            return;
        }

        _signalRouterPid = context.Spawn(_signalRouterProps);
        context.Watch(_signalRouterPid);

        if (_routingSnapshot.Count > 0)
        {
            context.Send(_signalRouterPid, new SetRoutingTable(_routingSnapshot));
        }
    }

    private void AttachSignalRouter(IContext context, PID? signalRouter)
    {
        if (signalRouter is null)
        {
            return;
        }

        if (_signalRouterPid is not null && !_signalRouterPid.Equals(signalRouter))
        {
            context.Unwatch(_signalRouterPid);
        }

        _signalRouterPid = signalRouter;
        context.Watch(signalRouter);

        if (_routingSnapshot.Count > 0)
        {
            context.Send(signalRouter, new SetRoutingTable(_routingSnapshot));
        }
    }

    private void ApplyRoutingTable(IContext context, RoutingTableSnapshot? snapshot)
    {
        _routingSnapshot = snapshot ?? RoutingTableSnapshot.Empty;

        if (_signalRouterPid is null)
        {
            return;
        }

        context.Send(_signalRouterPid, new SetRoutingTable(_routingSnapshot));
    }

    private void ForwardToSignalRouter(IContext context, object message)
    {
        if (_signalRouterPid is null)
        {
            return;
        }

        context.Send(_signalRouterPid, message);
    }

    private static void ForwardToParent(IContext context, object message)
    {
        if (context.Parent is null)
        {
            return;
        }

        context.Send(context.Parent, message);
    }

    private void HandleTerminated(Terminated terminated)
    {
        if (_signalRouterPid is not null && terminated.Who.Equals(_signalRouterPid))
        {
            _signalRouterPid = null;
        }
    }
}
