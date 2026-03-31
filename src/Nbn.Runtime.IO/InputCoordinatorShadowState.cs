using System.Threading.Tasks;
using Nbn.Proto.Io;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.IO;

internal sealed class InputCoordinatorShadowState
{
    private readonly Guid _brainId;
    private float[] _values;
    private bool[] _dirty;
    private readonly List<int> _dirtyIndices = new();
    private ProtoControl.InputCoordinatorMode _mode;

    public InputCoordinatorShadowState(
        Guid brainId,
        uint inputWidth,
        ProtoControl.InputCoordinatorMode mode)
    {
        _brainId = brainId;
        _values = new float[Math.Max(0, checked((int)inputWidth))];
        _dirty = new bool[_values.Length];
        _mode = NormalizeMode(mode);
    }

    public void UpdateConfiguration(uint inputWidth, ProtoControl.InputCoordinatorMode mode)
    {
        var requestedWidth = Math.Max(0, checked((int)inputWidth));
        if (requestedWidth != _values.Length)
        {
            Array.Resize(ref _values, requestedWidth);
            Array.Resize(ref _dirty, requestedWidth);
            if (requestedWidth == 0)
            {
                _dirtyIndices.Clear();
            }
            else
            {
                _dirtyIndices.RemoveAll(static index => index < 0);
                _dirtyIndices.RemoveAll(index => index >= requestedWidth);
            }
        }

        _mode = NormalizeMode(mode);
        if (_mode == ProtoControl.InputCoordinatorMode.ReplayLatestVector)
        {
            ClearDirtyState();
        }
    }

    public void Apply(InputWrite message)
    {
        if (!float.IsFinite(message.Value))
        {
            return;
        }

        if (message.InputIndex >= (uint)_values.Length)
        {
            return;
        }

        var index = checked((int)message.InputIndex);
        _values[index] = message.Value;
        MarkDirty(index);
    }

    public void Apply(InputVector message)
    {
        if (message.Values.Count != _values.Length)
        {
            return;
        }

        for (var i = 0; i < _values.Length; i++)
        {
            if (!float.IsFinite(message.Values[i]))
            {
                return;
            }
        }

        for (var i = 0; i < _values.Length; i++)
        {
            _values[i] = message.Values[i];
            MarkDirty(i);
        }
    }

    public void ApplyDrain(InputDrain drain)
    {
        if (_mode == ProtoControl.InputCoordinatorMode.ReplayLatestVector)
        {
            ClearDirtyState();
            return;
        }

        foreach (var contribution in drain.Contribs)
        {
            if (contribution.TargetNeuronId >= (uint)_dirty.Length)
            {
                continue;
            }

            var index = checked((int)contribution.TargetNeuronId);
            _dirty[index] = false;
            _dirtyIndices.Remove(index);
        }
    }

    public void ResetRuntimeState(bool resetAccumulator)
    {
        if (!resetAccumulator)
        {
            return;
        }

        Array.Clear(_values);
        ClearDirtyState();
    }

    public async Task ReplayToAsync(Func<object, Task> dispatchAsync)
    {
        if (_mode == ProtoControl.InputCoordinatorMode.ReplayLatestVector)
        {
            var vector = new InputVector
            {
                BrainId = _brainId.ToProtoUuid()
            };
            vector.Values.Add(_values);
            await dispatchAsync(vector).ConfigureAwait(false);
            return;
        }

        foreach (var index in _dirtyIndices.ToArray())
        {
            if (index < 0 || index >= _values.Length)
            {
                continue;
            }

            await dispatchAsync(new InputWrite
            {
                BrainId = _brainId.ToProtoUuid(),
                InputIndex = (uint)index,
                Value = _values[index]
            }).ConfigureAwait(false);
        }
    }

    private void MarkDirty(int index)
    {
        if (_mode == ProtoControl.InputCoordinatorMode.ReplayLatestVector)
        {
            return;
        }

        if (_dirty[index])
        {
            return;
        }

        _dirty[index] = true;
        _dirtyIndices.Add(index);
    }

    private void ClearDirtyState()
    {
        foreach (var index in _dirtyIndices)
        {
            if (index >= 0 && index < _dirty.Length)
            {
                _dirty[index] = false;
            }
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
