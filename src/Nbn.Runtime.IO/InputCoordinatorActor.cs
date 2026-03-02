using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.IO;

public sealed record UpdateInputCoordinatorMode(ProtoControl.InputCoordinatorMode Mode);

public sealed class InputCoordinatorActor : IActor
{
    private readonly Guid _brainId;
    private readonly int _inputWidth;
    private readonly float[] _values;
    private readonly bool[] _dirty;
    private readonly List<int> _dirtyIndices = new();
    private ProtoControl.InputCoordinatorMode _mode;

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

    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case InputWrite message:
                HandleInputWrite(message);
                break;
            case InputVector message:
                HandleInputVector(message);
                break;
            case DrainInputs message:
                Drain(context, message);
                break;
            case UpdateInputCoordinatorMode message:
                ApplyMode(message);
                break;
        }

        return Task.CompletedTask;
    }

    private void HandleInputWrite(InputWrite message)
    {
        if (!TryMatchBrain(message.BrainId))
        {
            return;
        }

        var index = message.InputIndex;
        if (index >= (uint)_inputWidth)
        {
            return;
        }

        _values[index] = message.Value;
        MarkDirty((int)index);
    }

    private void HandleInputVector(InputVector message)
    {
        if (!TryMatchBrain(message.BrainId))
        {
            return;
        }

        if (message.Values.Count != _inputWidth)
        {
            return;
        }

        for (var i = 0; i < _inputWidth; i++)
        {
            _values[i] = message.Values[i];
            MarkDirty(i);
        }
    }

    private void Drain(IContext context, DrainInputs message)
    {
        if (!TryMatchBrain(message.BrainId))
        {
            context.Respond(new InputDrain
            {
                BrainId = _brainId.ToProtoUuid(),
                TickId = message.TickId
            });
            return;
        }

        if (_mode == ProtoControl.InputCoordinatorMode.ReplayLatestVector)
        {
            context.Respond(BuildFullVectorDrain(message.TickId));
            return;
        }

        if (_dirtyIndices.Count == 0)
        {
            context.Respond(new InputDrain
            {
                BrainId = _brainId.ToProtoUuid(),
                TickId = message.TickId
            });
            return;
        }

        var contribs = new List<Contribution>(_dirtyIndices.Count);
        foreach (var index in _dirtyIndices)
        {
            contribs.Add(new Contribution
            {
                TargetNeuronId = (uint)index,
                Value = _values[index]
            });
            _dirty[index] = false;
        }

        _dirtyIndices.Clear();
        var drain = new InputDrain
        {
            BrainId = _brainId.ToProtoUuid(),
            TickId = message.TickId
        };
        drain.Contribs.AddRange(contribs);
        context.Respond(drain);
    }

    private InputDrain BuildFullVectorDrain(ulong tickId)
    {
        var drain = new InputDrain
        {
            BrainId = _brainId.ToProtoUuid(),
            TickId = tickId
        };

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
}

