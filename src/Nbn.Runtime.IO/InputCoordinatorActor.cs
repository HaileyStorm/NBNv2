using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Shared;
using Proto;

namespace Nbn.Runtime.IO;

public sealed class InputCoordinatorActor : IActor
{
    private readonly Guid _brainId;
    private readonly int _inputWidth;
    private readonly float[] _values;
    private readonly bool[] _dirty;
    private readonly List<int> _dirtyIndices = new();

    public InputCoordinatorActor(Guid brainId, uint inputWidth)
    {
        _brainId = brainId;
        _inputWidth = checked((int)inputWidth);
        _values = new float[_inputWidth];
        _dirty = new bool[_inputWidth];
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
}
