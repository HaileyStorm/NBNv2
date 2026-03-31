using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.IO;

/// <summary>
/// Requests a width increase for an input coordinator.
/// </summary>
public sealed record UpdateInputWidth(uint InputWidth);

/// <summary>
/// Requests an input coordinator mode update.
/// </summary>
public sealed record UpdateInputCoordinatorMode(ProtoControl.InputCoordinatorMode Mode);

/// <summary>
/// Buffers external input writes for a single brain and emits them at drain boundaries.
/// </summary>
public sealed class InputCoordinatorActor : IActor
{
    private readonly Guid _brainId;
    private int _inputWidth;
    private float[] _values;
    private bool[] _dirty;
    private readonly List<int> _dirtyIndices = new();
    private ProtoControl.InputCoordinatorMode _mode;

    /// <summary>
    /// Initializes a per-brain input coordinator with the supplied width and drain mode.
    /// </summary>
    public InputCoordinatorActor(
        Guid brainId,
        uint inputWidth,
        ProtoControl.InputCoordinatorMode mode = ProtoControl.InputCoordinatorMode.DirtyOnChange)
    {
        _brainId = brainId;
        _inputWidth = checked((int)inputWidth);
        _values = new float[_inputWidth];
        _dirty = new bool[_inputWidth];
        _mode = NormalizeMode(mode);
    }

    /// <summary>
    /// Handles input writes, drain requests, and coordinator configuration updates.
    /// </summary>
    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case InputWrite message:
                Respond(context, "input_write", HandleInputWrite(message));
                break;
            case InputVector message:
                Respond(context, "input_vector", HandleInputVector(message));
                break;
            case DrainInputs message:
                Drain(context, message);
                break;
            case ResetBrainRuntimeState message:
                HandleRuntimeStateReset(context, message);
                break;
            case UpdateInputWidth message:
                ApplyInputWidthUpdate(message);
                Respond(context, "update_input_width", success: true);
                break;
            case UpdateInputCoordinatorMode message:
                ApplyMode(message);
                Respond(context, "update_input_mode", success: true);
                break;
        }

        return Task.CompletedTask;
    }

    private bool HandleInputWrite(InputWrite message)
    {
        if (!TryMatchBrain(message.BrainId))
        {
            return false;
        }

        if (!float.IsFinite(message.Value))
        {
            return false;
        }

        var index = message.InputIndex;
        if (index >= (uint)_inputWidth)
        {
            return false;
        }

        _values[index] = message.Value;
        MarkDirty((int)index);
        return true;
    }

    private bool HandleInputVector(InputVector message)
    {
        if (!TryMatchBrain(message.BrainId))
        {
            return false;
        }

        if (message.Values.Count != _inputWidth)
        {
            return false;
        }

        for (var i = 0; i < _inputWidth; i++)
        {
            if (!float.IsFinite(message.Values[i]))
            {
                return false;
            }
        }

        for (var i = 0; i < _inputWidth; i++)
        {
            _values[i] = message.Values[i];
            MarkDirty(i);
        }

        return true;
    }

    private void Drain(IContext context, DrainInputs message)
    {
        if (!TryMatchBrain(message.BrainId))
        {
            context.Respond(CreateDrain(message.TickId));
            return;
        }

        if (_mode == ProtoControl.InputCoordinatorMode.ReplayLatestVector)
        {
            context.Respond(BuildFullVectorDrain(message.TickId));
            return;
        }

        if (_dirtyIndices.Count == 0)
        {
            context.Respond(CreateDrain(message.TickId));
            return;
        }

        var drain = CreateDrain(message.TickId);
        foreach (var index in _dirtyIndices)
        {
            drain.Contribs.Add(new Contribution
            {
                TargetNeuronId = (uint)index,
                Value = _values[index]
            });
            _dirty[index] = false;
        }

        _dirtyIndices.Clear();
        context.Respond(drain);
    }

    private InputDrain BuildFullVectorDrain(ulong tickId)
    {
        var drain = CreateDrain(tickId);

        if (_inputWidth <= 0)
        {
            ClearDirtyState();
            return drain;
        }

        for (var i = 0; i < _inputWidth; i++)
        {
            drain.Contribs.Add(new Contribution
            {
                TargetNeuronId = (uint)i,
                Value = _values[i]
            });
        }

        ClearDirtyState();
        return drain;
    }

    private InputDrain CreateDrain(ulong tickId)
        => new()
        {
            BrainId = _brainId.ToProtoUuid(),
            TickId = tickId
        };

    private bool TryMatchBrain(Nbn.Proto.Uuid? brainId)
    {
        if (brainId is null || !brainId.TryToGuid(out var guid))
        {
            return false;
        }

        return guid == _brainId;
    }

    private void MarkDirty(int index)
    {
        if (_dirty[index])
        {
            return;
        }

        _dirty[index] = true;
        _dirtyIndices.Add(index);
    }

    private void ApplyMode(UpdateInputCoordinatorMode message)
    {
        _mode = NormalizeMode(message.Mode);
        if (_mode == ProtoControl.InputCoordinatorMode.ReplayLatestVector)
        {
            ClearDirtyState();
        }
    }

    private void ApplyInputWidthUpdate(UpdateInputWidth message)
    {
        var requestedWidth = checked((int)message.InputWidth);
        if (requestedWidth <= _inputWidth)
        {
            return;
        }

        Array.Resize(ref _values, requestedWidth);
        Array.Resize(ref _dirty, requestedWidth);
        _inputWidth = requestedWidth;
    }

    private void HandleRuntimeStateReset(IContext context, ResetBrainRuntimeState message)
    {
        if (!TryMatchBrain(message.BrainId))
        {
            Respond(context, "reset_brain_runtime_state", success: false);
            return;
        }

        if (!message.ResetAccumulator)
        {
            Respond(context, "reset_brain_runtime_state", success: true);
            return;
        }

        Array.Clear(_values);
        ClearDirtyState();
        Respond(context, "reset_brain_runtime_state", success: true);
    }

    private void ClearDirtyState()
    {
        foreach (var index in _dirtyIndices)
        {
            _dirty[index] = false;
        }

        _dirtyIndices.Clear();
    }

    private static ProtoControl.InputCoordinatorMode NormalizeMode(ProtoControl.InputCoordinatorMode mode)
    {
        return mode switch
        {
            ProtoControl.InputCoordinatorMode.ReplayLatestVector => mode,
            _ => ProtoControl.InputCoordinatorMode.DirtyOnChange
        };
    }

    private void Respond(IContext context, string command, bool success)
    {
        if (context.Sender is null)
        {
            return;
        }

        context.Respond(new IoCommandAck
        {
            BrainId = _brainId.ToProtoUuid(),
            Command = command,
            Success = success,
            Message = success ? "applied" : "ignored"
        });
    }
}
