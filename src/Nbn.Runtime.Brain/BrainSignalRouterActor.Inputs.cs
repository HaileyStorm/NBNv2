using System.Diagnostics;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Shared;
using Proto;

namespace Nbn.Runtime.Brain;

public sealed partial class BrainSignalRouterActor
{
    private void HandleTickDeliver(IContext context, TickDeliver tickDeliver)
    {
        ExpirePendingInputDrains(tickDeliver.TickId);
        ExpirePendingDeliveries(tickDeliver.TickId);

        if (_pendingDeliveries.ContainsKey(tickDeliver.TickId) || _pendingInputDrains.ContainsKey(tickDeliver.TickId))
        {
            return;
        }

        if (!ShouldDrainInputs()
            && LogInputDiagnostics
            && _routingTable.Entries.Any(entry => entry.ShardId.RegionId == NbnConstants.InputRegionId))
        {
            LogInput(
                $"TickDeliver skip-drain tick={tickDeliver.TickId} ioGateway={PidLabel(_ioGatewayPid)} inputRoutes={_routingTable.Entries.Count(entry => entry.ShardId.RegionId == NbnConstants.InputRegionId)}");
        }

        if (ShouldDrainInputs())
        {
            var replyTo = context.Sender ?? context.Parent;
            _pendingInputDrains[tickDeliver.TickId] = new PendingInputDrain(
                tickDeliver.TickId,
                replyTo,
                Stopwatch.StartNew());

            if (LogInputTraceDiagnostics)
            {
                LogInput($"TickDeliver request-drain tick={tickDeliver.TickId} ioGateway={PidLabel(_ioGatewayPid)}");
            }

            context.Request(_ioGatewayPid!, new DrainInputs
            {
                BrainId = _brainIdProto,
                TickId = tickDeliver.TickId
            });
            return;
        }

        ProcessTickDeliver(context, tickDeliver.TickId, context.Sender ?? context.Parent, null, Stopwatch.StartNew());
    }

    private void HandleInputWrite(IContext context, InputWrite message)
    {
        if (!IsForBrain(message.BrainId))
        {
            return;
        }

        CaptureIoGateway(context.Sender);
        if (LogInputDiagnostics)
        {
            LogInput($"InputWrite received sender={PidLabel(context.Sender)} ioGateway={PidLabel(_ioGatewayPid)} index={message.InputIndex} value={message.Value:0.###}");
        }
    }

    private void HandleInputVector(IContext context, InputVector message)
    {
        if (!IsForBrain(message.BrainId))
        {
            return;
        }

        CaptureIoGateway(context.Sender);
        if (LogInputDiagnostics)
        {
            LogInput($"InputVector received sender={PidLabel(context.Sender)} ioGateway={PidLabel(_ioGatewayPid)} width={message.Values.Count}");
        }
    }

    private void HandleRuntimeNeuronPulse(IContext context, RuntimeNeuronPulse message)
    {
        if (!IsForBrain(message.BrainId))
        {
            return;
        }

        CaptureIoGateway(context.Sender);

        if (!float.IsFinite(message.Value))
        {
            return;
        }

        DispatchToRegionShards(context, message.TargetRegionId, message);
    }

    private void HandleRuntimeNeuronStateWrite(IContext context, RuntimeNeuronStateWrite message)
    {
        if (!IsForBrain(message.BrainId))
        {
            return;
        }

        CaptureIoGateway(context.Sender);

        if (!message.SetBuffer && !message.SetAccumulator)
        {
            return;
        }

        if ((message.SetBuffer && !float.IsFinite(message.BufferValue))
            || (message.SetAccumulator && !float.IsFinite(message.AccumulatorValue)))
        {
            return;
        }

        DispatchToRegionShards(context, message.TargetRegionId, message);
    }

    private void HandleInputDrain(IContext context, InputDrain message)
    {
        if (!IsForBrain(message.BrainId))
        {
            return;
        }

        CaptureIoGateway(context.Sender);

        if (!_pendingInputDrains.TryGetValue(message.TickId, out var pending))
        {
            if (LogInputTraceDiagnostics || message.Contribs.Count > 0)
            {
                LogInput($"InputDrain ignored tick={message.TickId} sender={PidLabel(context.Sender)} contribs={message.Contribs.Count}");
            }

            return;
        }

        _pendingInputDrains.Remove(message.TickId);
        if (LogInputTraceDiagnostics || message.Contribs.Count > 0)
        {
            LogInput($"InputDrain accepted tick={message.TickId} sender={PidLabel(context.Sender)} contribs={message.Contribs.Count}");
        }

        ProcessTickDeliver(context, message.TickId, pending.ReplyTo, message.Contribs, pending.Stopwatch);
    }

    private void HandleRegisterIoGateway(IContext context, RegisterIoGateway message)
    {
        if (!IsForBrain(message.BrainId))
        {
            return;
        }

        if (!string.IsNullOrWhiteSpace(message.IoGatewayPid)
            && TryParsePid(message.IoGatewayPid, out var parsed))
        {
            _ioGatewayPid = parsed;
            if (LogInputDiagnostics)
            {
                LogInput($"RegisterIoGateway explicit sender={PidLabel(context.Sender)} ioGateway={PidLabel(_ioGatewayPid)}");
            }

            return;
        }

        CaptureIoGateway(context.Sender);
        if (LogInputDiagnostics)
        {
            LogInput($"RegisterIoGateway sender-capture sender={PidLabel(context.Sender)} ioGateway={PidLabel(_ioGatewayPid)}");
        }
    }

    private void ExpirePendingInputDrains(ulong currentTickId)
    {
        if (_pendingInputDrains.Count == 0)
        {
            return;
        }

        List<ulong>? expired = null;
        foreach (var entry in _pendingInputDrains)
        {
            if (entry.Key < currentTickId)
            {
                expired ??= new List<ulong>();
                expired.Add(entry.Key);
            }
        }

        if (expired is null)
        {
            return;
        }

        foreach (var tickId in expired)
        {
            _pendingInputDrains.Remove(tickId);
            _pendingOutboxes.Remove(tickId);
        }

        // If drain requests are expiring, the cached IO PID is likely stale/unreachable.
        // Clear it so subsequent ticks can proceed without repeatedly waiting on drain responses.
        _ioGatewayPid = null;

        if (LogDelivery)
        {
            Log($"Pending input drains expired before tick={currentTickId}. expired={string.Join(",", expired)} ioGatewayCleared=true");
        }
    }

    private bool ShouldDrainInputs()
        => _ioGatewayPid is not null
           && _routingTable.Entries.Any(entry => entry.ShardId.RegionId == NbnConstants.InputRegionId);

    private void CaptureIoGateway(PID? sender)
    {
        if (sender is null)
        {
            return;
        }

        _ioGatewayPid = sender;
    }

    private sealed class PendingInputDrain
    {
        public PendingInputDrain(ulong tickId, PID? replyTo, Stopwatch stopwatch)
        {
            TickId = tickId;
            ReplyTo = replyTo;
            Stopwatch = stopwatch;
        }

        public ulong TickId { get; }
        public PID? ReplyTo { get; }
        public Stopwatch Stopwatch { get; }
    }
}
