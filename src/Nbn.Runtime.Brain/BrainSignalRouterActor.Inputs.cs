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
        _inputDrainPending = true;
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
        _inputDrainPending = true;
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

    private async Task HandleResetBrainRuntimeStateAsync(IContext context, ResetBrainRuntimeState message)
    {
        if (!IsForBrain(message.BrainId))
        {
            context.Respond(new IoCommandAck
            {
                BrainId = message.BrainId,
                Command = "reset_brain_runtime_state",
                Success = false,
                Message = "brain_id_mismatch"
            });
            return;
        }

        CaptureIoGateway(context.Sender);

        if (!message.ResetBuffer && !message.ResetAccumulator)
        {
            context.Respond(new IoCommandAck
            {
                BrainId = message.BrainId,
                Command = "reset_brain_runtime_state",
                Success = false,
                Message = "nothing_requested"
            });
            return;
        }

        if (_pendingOutboxes.Count > 0 || _pendingDeliveries.Count > 0 || _pendingInputDrains.Count > 0)
        {
            context.Respond(new IoCommandAck
            {
                BrainId = message.BrainId,
                Command = "reset_brain_runtime_state",
                Success = false,
                Message = "tick_phase_in_progress"
            });
            return;
        }

        if (_routingTable.Count == 0)
        {
            context.Respond(new IoCommandAck
            {
                BrainId = message.BrainId,
                Command = "reset_brain_runtime_state",
                Success = false,
                Message = "no_region_shards"
            });
            return;
        }

        try
        {
            var targets = _routingTable.Entries
                .Select(static entry => entry.Pid)
                .Distinct(PidEqualityComparer.Instance)
                .ToArray();
            var acks = await Task.WhenAll(
                    targets.Select(target => context.RequestAsync<IoCommandAck>(target, message, RuntimeStateResetTimeout)))
                .ConfigureAwait(false);
            var failedAck = acks.FirstOrDefault(ack => ack is null || !ack.Success);
            if (failedAck is not null)
            {
                context.Respond(new IoCommandAck
                {
                    BrainId = _brainIdProto,
                    Command = "reset_brain_runtime_state",
                    Success = false,
                    Message = failedAck.Message
                });
                return;
            }

            context.Respond(new IoCommandAck
            {
                BrainId = _brainIdProto,
                Command = "reset_brain_runtime_state",
                Success = true,
                Message = $"applied_shards={targets.Length}"
            });
            if (message.ResetAccumulator)
            {
                _inputDrainPending = false;
            }
        }
        catch (Exception ex)
        {
            context.Respond(new IoCommandAck
            {
                BrainId = _brainIdProto,
                Command = "reset_brain_runtime_state",
                Success = false,
                Message = $"reset_request_failed:{ex.GetBaseException().Message}"
            });
        }
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
        if (_inputCoordinatorModeKnown && _inputCoordinatorMode != InputCoordinatorMode.ReplayLatestVector)
        {
            _inputDrainPending = false;
        }
        if (LogInputTraceDiagnostics || message.Contribs.Count > 0)
        {
            LogInput(
                $"InputDrain accepted tick={message.TickId} sender={PidLabel(context.Sender)} contribs={message.Contribs.Count} drainPending={_inputDrainPending} modeKnown={_inputCoordinatorModeKnown}");
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
            if (message.HasInputCoordinatorMode)
            {
                _inputCoordinatorMode = NormalizeInputCoordinatorMode(message.InputCoordinatorMode);
                _inputCoordinatorModeKnown = true;
            }
            ApplyInputDrainRegistration(message);
            if (LogInputDiagnostics)
            {
                LogInput(
                    $"RegisterIoGateway explicit sender={PidLabel(context.Sender)} ioGateway={PidLabel(_ioGatewayPid)} mode={_inputCoordinatorMode} modeKnown={_inputCoordinatorModeKnown} drainPending={_inputDrainPending}");
            }

            return;
        }

        CaptureIoGateway(context.Sender);
        if (message.HasInputCoordinatorMode)
        {
            _inputCoordinatorMode = NormalizeInputCoordinatorMode(message.InputCoordinatorMode);
            _inputCoordinatorModeKnown = true;
        }
        ApplyInputDrainRegistration(message);
        if (LogInputDiagnostics)
        {
            LogInput(
                $"RegisterIoGateway sender-capture sender={PidLabel(context.Sender)} ioGateway={PidLabel(_ioGatewayPid)} mode={_inputCoordinatorMode} modeKnown={_inputCoordinatorModeKnown} drainPending={_inputDrainPending}");
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
    {
        if (_ioGatewayPid is null
            || !_routingTable.Entries.Any(entry => entry.ShardId.RegionId == NbnConstants.InputRegionId))
        {
            return false;
        }

        if (!_inputCoordinatorModeKnown)
        {
            return true;
        }

        return _inputCoordinatorMode == InputCoordinatorMode.ReplayLatestVector || _inputDrainPending;
    }

    private static InputCoordinatorMode NormalizeInputCoordinatorMode(InputCoordinatorMode mode)
        => mode == InputCoordinatorMode.ReplayLatestVector
            ? mode
            : InputCoordinatorMode.DirtyOnChange;

    private void ApplyInputDrainRegistration(RegisterIoGateway message)
    {
        if (message.HasInputTickDrainArmed)
        {
            _inputDrainPending = (_inputCoordinatorModeKnown
                                  && _inputCoordinatorMode == InputCoordinatorMode.ReplayLatestVector)
                                 || message.InputTickDrainArmed;
            return;
        }

        if (message.HasInputCoordinatorMode)
        {
            // Older/mode-only registrations should preserve the pre-optimization behavior
            // until IO sends an explicit drain-arm hint or a drain response clears the state.
            _inputDrainPending = true;
        }
    }

    private void CaptureIoGateway(PID? sender)
    {
        if (sender is null)
        {
            return;
        }

        _ioGatewayPid = sender;
    }

    private sealed class PidEqualityComparer : IEqualityComparer<PID>
    {
        public static readonly PidEqualityComparer Instance = new();

        public bool Equals(PID? x, PID? y)
        {
            if (x is null || y is null)
            {
                return x is null && y is null;
            }

            return string.Equals(x.Address, y.Address, StringComparison.Ordinal)
                   && string.Equals(x.Id, y.Id, StringComparison.Ordinal);
        }

        public int GetHashCode(PID obj)
        {
            var address = obj.Address ?? string.Empty;
            var id = obj.Id ?? string.Empty;
            return HashCode.Combine(address, id);
        }
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
