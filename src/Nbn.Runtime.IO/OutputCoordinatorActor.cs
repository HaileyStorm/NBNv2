using Nbn.Proto.Io;
using Nbn.Shared;
using Proto;

namespace Nbn.Runtime.IO;

public sealed record EmitOutput(uint OutputIndex, float Value, ulong TickId);

public sealed record EmitOutputVector(IReadOnlyList<float> Values, ulong TickId);

public sealed class OutputCoordinatorActor : IActor
{
    private readonly Nbn.Proto.Uuid _brainIdProto;
    private int _outputWidth;
    private readonly Dictionary<string, PID> _outputSubscribers = new(StringComparer.Ordinal);
    private readonly Dictionary<string, PID> _vectorSubscribers = new(StringComparer.Ordinal);

    public OutputCoordinatorActor(Guid brainId, uint outputWidth)
    {
        _brainIdProto = brainId.ToProtoUuid();
        _outputWidth = checked((int)outputWidth);
    }

    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case SubscribeOutputs:
                AddSubscriber(context.Sender, _outputSubscribers);
                break;
            case UnsubscribeOutputs:
                RemoveSubscriber(context.Sender, _outputSubscribers);
                break;
            case SubscribeOutputsVector:
                AddSubscriber(context.Sender, _vectorSubscribers);
                break;
            case UnsubscribeOutputsVector:
                RemoveSubscriber(context.Sender, _vectorSubscribers);
                break;
            case OutputEvent outputEvent:
                EmitSingle(context, new EmitOutput(outputEvent.OutputIndex, outputEvent.Value, outputEvent.TickId));
                break;
            case OutputVectorEvent outputVector:
                EmitVector(context, new EmitOutputVector(outputVector.Values, outputVector.TickId));
                break;
            case EmitOutput message:
                EmitSingle(context, message);
                break;
            case EmitOutputVector message:
                EmitVector(context, message);
                break;
        }

        return Task.CompletedTask;
    }

    private void EmitSingle(IContext context, EmitOutput message)
    {
        EnsureOutputWidth((int)message.OutputIndex + 1);

        if (message.OutputIndex >= (uint)_outputWidth)
        {
            return;
        }

        if (_outputSubscribers.Count == 0)
        {
            return;
        }

        var evt = new OutputEvent
        {
            BrainId = _brainIdProto,
            OutputIndex = message.OutputIndex,
            Value = message.Value,
            TickId = message.TickId
        };

        foreach (var subscriber in _outputSubscribers.Values)
        {
            context.Send(subscriber, evt);
        }
    }

    private void EmitVector(IContext context, EmitOutputVector message)
    {
        EnsureOutputWidth(message.Values.Count);

        if (message.Values.Count != _outputWidth)
        {
            return;
        }

        if (_vectorSubscribers.Count == 0)
        {
            return;
        }

        var evt = new OutputVectorEvent
        {
            BrainId = _brainIdProto,
            TickId = message.TickId
        };
        evt.Values.Add(message.Values);

        foreach (var subscriber in _vectorSubscribers.Values)
        {
            context.Send(subscriber, evt);
        }
    }

    private void EnsureOutputWidth(int candidate)
    {
        if (candidate <= 0)
        {
            return;
        }

        if (_outputWidth <= 0)
        {
            _outputWidth = candidate;
            return;
        }

        if (candidate > _outputWidth)
        {
            _outputWidth = candidate;
        }
    }

    private static void AddSubscriber(PID? sender, Dictionary<string, PID> set)
    {
        if (sender is null)
        {
            return;
        }

        set[PidKey(sender)] = sender;
    }

    private static void RemoveSubscriber(PID? sender, Dictionary<string, PID> set)
    {
        if (sender is null)
        {
            return;
        }

        set.Remove(PidKey(sender));
    }

    private static string PidKey(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";
}
