using Nbn.Proto.Io;
using Nbn.Shared.HiveMind;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.Brain;

/// <summary>
/// Coordinates BrainRoot lifecycle messages, cached router state, and HiveMind registration updates.
/// </summary>
public sealed class BrainRootActor : IActor
{
    private static readonly TimeSpan RouterRequestTimeout = TimeSpan.FromSeconds(5);
    private readonly Guid _brainId;
    private readonly Props? _signalRouterProps;
    private readonly PID? _hiveMindPid;
    private readonly bool _autoSpawnSignalRouter;
    private PID? _signalRouterPid;
    private RoutingTableSnapshot _routingSnapshot = RoutingTableSnapshot.Empty;
    private RegisterIoGateway? _pendingIoGateway;

    /// <summary>
    /// Creates a brain root actor for the supplied brain and optional runtime endpoints.
    /// </summary>
    /// <param name="brainId">Owning brain identifier.</param>
    /// <param name="hiveMindPid">Optional HiveMind PID that receives brain registration updates.</param>
    /// <param name="signalRouterProps">Optional signal-router props used when the root auto-spawns its router.</param>
    /// <param name="autoSpawnSignalRouter">Whether the root should spawn a router during startup when one is not attached.</param>
    public BrainRootActor(Guid brainId, PID? hiveMindPid = null, Props? signalRouterProps = null, bool autoSpawnSignalRouter = true)
    {
        _brainId = brainId;
        _hiveMindPid = hiveMindPid;
        _autoSpawnSignalRouter = autoSpawnSignalRouter;
        _signalRouterProps = signalRouterProps ?? (autoSpawnSignalRouter
            ? Props.FromProducer(() => new BrainSignalRouterActor(brainId))
            : null);
    }

    /// <summary>
    /// Handles brain lifecycle, routing-table, tick-forwarding, and IO gateway coordination messages.
    /// </summary>
    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
                EnsureSignalRouter(context);
                NotifyHiveMind(context, forceRegister: true);
                break;
            case Stopping:
                UnregisterHiveMind(context);
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
            case ProtoControl.TickCompute tickCompute:
                ForwardToSignalRouter(context, tickCompute);
                break;
            case ProtoControl.TickDeliver tickDeliver:
                ForwardToSignalRouter(context, tickDeliver);
                break;
            case ProtoControl.TickComputeDone tickComputeDone:
                ForwardToHiveMind(context, tickComputeDone);
                break;
            case ProtoControl.TickDeliverDone tickDeliverDone:
                ForwardToHiveMind(context, tickDeliverDone);
                break;
            case RegisterIoGateway registerIoGateway:
                HandleRegisterIoGateway(context, registerIoGateway);
                break;
            case InputWrite inputWrite:
                ForwardIoCommandToSignalRouter(context, inputWrite, inputWrite.BrainId, "input_write");
                break;
            case InputVector inputVector:
                ForwardIoCommandToSignalRouter(context, inputVector, inputVector.BrainId, "input_vector");
                break;
            case ResetBrainRuntimeState reset:
                ForwardIoCommandToSignalRouter(context, reset, reset.BrainId, "reset_brain_runtime_state");
                break;
            case ApplyBrainRuntimeResetAtBarrier resetAtBarrier:
                ForwardIoCommandToSignalRouter(
                    context,
                    resetAtBarrier,
                    resetAtBarrier.BrainId.ToProtoUuid(),
                    "reset_brain_runtime_state");
                break;
            case RuntimeNeuronPulse pulse:
                ForwardRuntimeCommandToSignalRouter(context, pulse);
                break;
            case RuntimeNeuronStateWrite stateWrite:
                ForwardRuntimeCommandToSignalRouter(context, stateWrite);
                break;
            case Terminated terminated:
                HandleTerminated(terminated);
                break;
        }

        return Task.CompletedTask;
    }

    private void EnsureSignalRouter(IContext context)
    {
        if (!_autoSpawnSignalRouter || _signalRouterPid is not null || _signalRouterProps is null)
        {
            return;
        }

        _signalRouterPid = context.Spawn(_signalRouterProps);
        context.Watch(_signalRouterPid);
        ReplayCachedSignalRouterState(context, _signalRouterPid);

        NotifyHiveMind(context, forceRegister: true);
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
        ReplayCachedSignalRouterState(context, signalRouter);

        NotifyHiveMind(context, forceRegister: false);
    }

    private void ReplayCachedSignalRouterState(IContext context, PID signalRouter)
    {
        if (_routingSnapshot.Count > 0)
        {
            context.Send(signalRouter, new SetRoutingTable(_routingSnapshot));
        }

        if (_pendingIoGateway is not null)
        {
            context.Send(signalRouter, _pendingIoGateway);
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

    private void NotifyHiveMind(IContext context, bool forceRegister)
    {
        if (_hiveMindPid is null)
        {
            return;
        }

        var brainRootPid = ToRemotePid(context, context.Self);
        var routerPid = _signalRouterPid is null ? null : ToRemotePid(context, _signalRouterPid);

        if (forceRegister)
        {
            context.Request(_hiveMindPid, new ProtoControl.RegisterBrain
            {
                BrainId = _brainId.ToProtoUuid(),
                BrainRootPid = PidToString(brainRootPid),
                SignalRouterPid = routerPid is null ? string.Empty : PidToString(routerPid)
            });
            return;
        }

        if (routerPid is not null)
        {
            context.Request(_hiveMindPid, new ProtoControl.UpdateBrainSignalRouter
            {
                BrainId = _brainId.ToProtoUuid(),
                SignalRouterPid = PidToString(routerPid)
            });
        }
    }

    private static PID ToRemotePid(IContext context, PID pid)
    {
        if (!string.IsNullOrWhiteSpace(pid.Address))
        {
            return pid;
        }

        var address = context.System.Address;
        if (string.IsNullOrWhiteSpace(address))
        {
            return pid;
        }

        return new PID(address, pid.Id);
    }

    private static string PidToString(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private void UnregisterHiveMind(IContext context)
    {
        if (_hiveMindPid is null)
        {
            return;
        }

        context.Request(_hiveMindPid, new ProtoControl.UnregisterBrain
        {
            BrainId = _brainId.ToProtoUuid()
        });
    }

    private void ForwardToSignalRouter(IContext context, object message)
    {
        if (_signalRouterPid is null)
        {
            return;
        }

        // Use Request so remoting populates Sender for replies (TickDeliverDone, etc.).
        context.Request(_signalRouterPid, message);
    }

    private void ForwardIoCommandToSignalRouter(IContext context, object message, Nbn.Proto.Uuid? brainId, string command)
    {
        if (_signalRouterPid is null)
        {
            context.Respond(new IoCommandAck
            {
                BrainId = brainId?.Clone(),
                Command = command,
                Success = false,
                Message = "signal_router_unavailable"
            });
            return;
        }

        context.ReenterAfter(
            context.RequestAsync<IoCommandAck>(_signalRouterPid, message, RouterRequestTimeout),
            task =>
            {
                if (task.IsCompletedSuccessfully && task.Result is not null)
                {
                    context.Respond(task.Result);
                    return Task.CompletedTask;
                }

                var detail = task.Exception?.GetBaseException().Message ?? "empty_response";
                context.Respond(new IoCommandAck
                {
                    BrainId = brainId?.Clone(),
                    Command = command,
                    Success = false,
                    Message = $"signal_router_request_failed:{detail}"
                });
                return Task.CompletedTask;
            });
    }

    private void ForwardRuntimeCommandToSignalRouter(IContext context, object message)
    {
        if (_signalRouterPid is null)
        {
            return;
        }

        context.Send(_signalRouterPid, message);
    }

    private void ForwardToHiveMind(IContext context, object message)
    {
        if (_hiveMindPid is not null)
        {
            context.Request(_hiveMindPid, message);
            return;
        }

        if (context.Parent is not null)
        {
            context.Request(context.Parent, message);
        }
    }

    private void HandleRegisterIoGateway(IContext context, RegisterIoGateway message)
    {
        if (message.BrainId is null || !message.BrainId.TryToGuid(out var guid) || guid != _brainId)
        {
            return;
        }

        _pendingIoGateway = message;
        if (_signalRouterPid is not null)
        {
            context.Send(_signalRouterPid, message);
        }
    }

    private void HandleTerminated(Terminated terminated)
    {
        if (_signalRouterPid is not null && terminated.Who.Equals(_signalRouterPid))
        {
            _signalRouterPid = null;
        }
    }
}
