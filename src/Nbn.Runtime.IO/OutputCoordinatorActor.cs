using Nbn.Proto.Io;
using Nbn.Shared;
using Nbn.Shared.IO;
using Proto;

namespace Nbn.Runtime.IO;

public sealed record EmitOutput(uint OutputIndex, float Value, ulong TickId);

public sealed class OutputCoordinatorActor : IActor
{
    private const int MaxPendingVectorTicks = 16;
    private readonly Guid _brainId;
    private readonly Nbn.Proto.Uuid _brainIdProto;
    private readonly int _outputWidth;
    private readonly Dictionary<string, PID> _outputSubscribers = new(StringComparer.Ordinal);
    private readonly Dictionary<string, PID> _vectorSubscribers = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, PendingVectorTick> _pendingVectors = new();
    private ulong _latestCompletedVectorTick;

    public OutputCoordinatorActor(Guid brainId, uint outputWidth)
    {
        _brainId = brainId;
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
                EmitLegacyVector(context, outputVector);
                break;
            case EmitOutput message:
                EmitSingle(context, message);
                break;
            case EmitOutputVectorSegment message:
                EmitVectorSegment(context, message);
                break;
        }

        return Task.CompletedTask;
    }

    private void EmitSingle(IContext context, EmitOutput message)
    {
        if (message.OutputIndex >= (uint)_outputWidth)
        {
            RecordSingleReject("output_index_out_of_range");
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

    private void EmitLegacyVector(IContext context, OutputVectorEvent message)
    {
        if (message.Values.Count != _outputWidth)
        {
            RecordVectorReject("vector_width_mismatch");
            return;
        }

        EmitVectorSegment(
            context,
            new EmitOutputVectorSegment(0, message.Values, message.TickId));
    }

    private void EmitVectorSegment(IContext context, EmitOutputVectorSegment message)
    {
        if (_outputWidth <= 0)
        {
            RecordVectorReject("output_width_invalid");
            return;
        }

        if (message.TickId <= _latestCompletedVectorTick)
        {
            RecordVectorReject("tick_already_completed");
            return;
        }

        if (message.Values.Count <= 0)
        {
            RecordVectorReject("segment_empty");
            return;
        }

        if (message.OutputIndexStart > int.MaxValue)
        {
            RecordVectorReject("segment_start_out_of_range");
            return;
        }

        var startIndex = (int)message.OutputIndexStart;
        var valueCount = message.Values.Count;
        if (startIndex < 0 || startIndex >= _outputWidth)
        {
            RecordVectorReject("segment_start_out_of_range");
            return;
        }

        if ((long)startIndex + valueCount > _outputWidth)
        {
            RecordVectorReject("segment_exceeds_output_width");
            return;
        }

        var pending = GetOrCreatePendingTick(message.TickId);
        for (var i = 0; i < valueCount; i++)
        {
            if (pending.Filled[startIndex + i])
            {
                RecordVectorReject("segment_overlap");
                return;
            }
        }

        for (var i = 0; i < valueCount; i++)
        {
            var outputIndex = startIndex + i;
            pending.Values[outputIndex] = message.Values[i];
            pending.Filled[outputIndex] = true;
        }

        pending.FilledCount += valueCount;
        if (pending.FilledCount < _outputWidth)
        {
            return;
        }

        PublishVector(context, message.TickId, pending.Values);
    }

    private PendingVectorTick GetOrCreatePendingTick(ulong tickId)
    {
        if (_pendingVectors.TryGetValue(tickId, out var existing))
        {
            return existing;
        }

        if (_pendingVectors.Count >= MaxPendingVectorTicks)
        {
            var oldest = _pendingVectors.Keys.Min();
            _pendingVectors.Remove(oldest);
            RecordVectorReject("pending_tick_evicted");
        }

        var pending = new PendingVectorTick(new float[_outputWidth], new bool[_outputWidth]);
        _pendingVectors[tickId] = pending;
        return pending;
    }

    private void PublishVector(IContext context, ulong tickId, IReadOnlyList<float> values)
    {
        _pendingVectors.Remove(tickId);
        _latestCompletedVectorTick = Math.Max(_latestCompletedVectorTick, tickId);
        DropStalePendingTicks();

        IoTelemetry.RecordOutputVectorPublished(_brainId, _outputWidth);
        if (_vectorSubscribers.Count == 0)
        {
            return;
        }

        var evt = new OutputVectorEvent
        {
            BrainId = _brainIdProto,
            TickId = tickId
        };
        evt.Values.Add(values);

        foreach (var subscriber in _vectorSubscribers.Values)
        {
            context.Send(subscriber, evt);
        }
    }

    private void DropStalePendingTicks()
    {
        if (_pendingVectors.Count == 0)
        {
            return;
        }

        var staleTicks = new List<ulong>();
        foreach (var tickId in _pendingVectors.Keys)
        {
            if (tickId <= _latestCompletedVectorTick)
            {
                staleTicks.Add(tickId);
            }
        }

        foreach (var tickId in staleTicks)
        {
            _pendingVectors.Remove(tickId);
            RecordVectorReject("pending_tick_superseded");
        }
    }

    private void RecordSingleReject(string reason)
    {
        IoTelemetry.RecordOutputSingleRejected(_brainId, reason, _outputWidth);
        Console.WriteLine($"OutputCoordinator[{_brainId:D}] single output rejected: {reason}.");
    }

    private void RecordVectorReject(string reason)
    {
        IoTelemetry.RecordOutputVectorRejected(_brainId, reason, _outputWidth);
        Console.WriteLine($"OutputCoordinator[{_brainId:D}] output vector rejected: {reason}.");
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

    private sealed class PendingVectorTick
    {
        public PendingVectorTick(float[] values, bool[] filled)
        {
            Values = values;
            Filled = filled;
        }

        public float[] Values { get; }
        public bool[] Filled { get; }
        public int FilledCount { get; set; }
    }
}
