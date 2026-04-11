using Nbn.Proto.Io;
using Nbn.Shared;
using Nbn.Shared.IO;
using Proto;

namespace Nbn.Runtime.IO;

/// <summary>
/// Emits a single output value for a brain at a specific tick.
/// </summary>
public sealed record EmitOutput(uint OutputIndex, float Value, ulong TickId);

/// <summary>
/// Requests a width increase for an output coordinator.
/// </summary>
public sealed record UpdateOutputWidth(uint OutputWidth);

/// <summary>
/// Clears pending output state after a barrier-coordinated runtime reset and rejects late
/// output messages from ticks older than the supplied floor.
/// </summary>
public sealed record ApplyOutputCoordinatorRuntimeReset(Guid BrainId, ulong MinimumAcceptedTickId);

/// <summary>
/// Aggregates per-brain output events and publishes them to external subscribers.
/// </summary>
public sealed class OutputCoordinatorActor : IActor
{
    private static readonly bool LogOutput = IsEnvTrue("NBN_IO_LOG_OUTPUT");
    private static readonly TimeSpan LatestOnlyFlushDelay = TimeSpan.FromMilliseconds(1);
    private const int MaxPendingVectorTicks = 16;
    private readonly Guid _brainId;
    private readonly Nbn.Proto.Uuid _brainIdProto;
    private int _outputWidth;
    private readonly Dictionary<string, OutputSubscriberState> _outputSubscribers = new(StringComparer.Ordinal);
    private readonly Dictionary<string, OutputSubscriberState> _vectorSubscribers = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _subscriberWatchRefCounts = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, PendingVectorTick> _pendingVectors = new();
    private ulong _latestCompletedVectorTick;
    private ulong _minimumAcceptedTickId = 1;

    /// <summary>
    /// Initializes a per-brain output coordinator with the supplied width.
    /// </summary>
    public OutputCoordinatorActor(Guid brainId, uint outputWidth)
    {
        _brainId = brainId;
        _brainIdProto = brainId.ToProtoUuid();
        _outputWidth = Math.Max(1, checked((int)outputWidth));
    }

    /// <summary>
    /// Handles subscription changes, output emission, and width updates.
    /// </summary>
    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case SubscribeOutputs subscribe:
                HandleSubscriptionCommand(
                    context,
                    new SubscriptionCommand(
                        subscribe.SubscriberActor,
                        ResolveDeliveryMode(subscribe.DeliveryMode),
                        _outputSubscribers,
                        "subscribe_outputs",
                        "subscribe single",
                        "single",
                        true));
                break;
            case UnsubscribeOutputs unsubscribe:
                HandleSubscriptionCommand(
                    context,
                    new SubscriptionCommand(
                        unsubscribe.SubscriberActor,
                        OutputSubscriptionDeliveryMode.Exact,
                        _outputSubscribers,
                        "unsubscribe_outputs",
                        "unsubscribe single",
                        "single",
                        false));
                break;
            case SubscribeOutputsVector subscribeVector:
                HandleSubscriptionCommand(
                    context,
                    new SubscriptionCommand(
                        subscribeVector.SubscriberActor,
                        ResolveDeliveryMode(subscribeVector.DeliveryMode),
                        _vectorSubscribers,
                        "subscribe_outputs_vector",
                        "subscribe vector",
                        "vector",
                        true));
                break;
            case UnsubscribeOutputsVector unsubscribeVector:
                HandleSubscriptionCommand(
                    context,
                    new SubscriptionCommand(
                        unsubscribeVector.SubscriberActor,
                        OutputSubscriptionDeliveryMode.Exact,
                        _vectorSubscribers,
                        "unsubscribe_outputs_vector",
                        "unsubscribe vector",
                        "vector",
                        false));
                break;
            case Terminated terminated:
                HandleSubscriberTerminated(context, terminated);
                break;
            case OutputEvent outputEvent:
                EmitSingle(context, new EmitOutput(outputEvent.OutputIndex, outputEvent.Value, outputEvent.TickId));
                break;
            case OutputVectorEvent outputVector:
                EmitLegacyVector(context, outputVector);
                break;
            case OutputVectorSegment segment:
                EmitProtoVectorSegment(context, segment);
                break;
            case EmitOutput message:
                EmitSingle(context, message);
                break;
            case UpdateOutputWidth message:
                ApplyOutputWidthUpdate(message);
                Respond(context, "update_output_width", success: true);
                break;
            case ApplyOutputCoordinatorRuntimeReset message:
                HandleRuntimeReset(context, message);
                break;
            case EmitOutputVectorSegment message:
                EmitVectorSegment(context, message);
                break;
            case FlushLatestOutputSubscriberRequest message:
                FlushLatestOutputSubscriber(context, message);
                break;
        }

        return Task.CompletedTask;
    }

    private void HandleSubscriptionCommand(IContext context, SubscriptionCommand command)
    {
        var subscriber = ResolveSubscriberPid(context, command.SubscriberActor);
        if (command.Subscribe)
        {
            AddSubscriber(context, subscriber, command.DeliveryMode, command.Set);
        }
        else
        {
            RemoveSubscriber(context, subscriber, command.Set, command.Stream);
        }

        if (LogOutput)
        {
            Console.WriteLine(
                $"OutputCoordinator[{_brainId:D}] {command.LogLabel} sender={PidLabel(context.Sender)} resolved={PidLabel(subscriber)} mode={command.DeliveryMode} total={command.Set.Count}.");
        }

        Respond(context, command.Command, success: subscriber is not null);
    }

    private void HandleSubscriberTerminated(IContext context, Terminated terminated)
    {
        if (terminated.Who is null)
        {
            return;
        }

        var key = PidKey(terminated.Who);
        var removedSingle = RemoveSubscriberByKey(context, key, _outputSubscribers, "single", "terminated", releaseWatch: true);
        var removedVector = RemoveSubscriberByKey(context, key, _vectorSubscribers, "vector", "terminated", releaseWatch: true);
        if (LogOutput && (removedSingle || removedVector))
        {
            Console.WriteLine(
                $"OutputCoordinator[{_brainId:D}] removed terminated subscriber={PidLabel(terminated.Who)} single={removedSingle} vector={removedVector}.");
        }
    }

    private void ApplyOutputWidthUpdate(UpdateOutputWidth message)
    {
        var requestedWidth = checked((int)message.OutputWidth);
        if (requestedWidth <= _outputWidth)
        {
            return;
        }

        foreach (var pending in _pendingVectors.Values)
        {
            var values = pending.Values;
            var filled = pending.Filled;
            Array.Resize(ref values, requestedWidth);
            Array.Resize(ref filled, requestedWidth);
            pending.Values = values;
            pending.Filled = filled;
        }

        _outputWidth = requestedWidth;
    }

    private void EmitSingle(IContext context, EmitOutput message)
    {
        if (message.TickId < _minimumAcceptedTickId)
        {
            RecordSingleReject("tick_superseded_by_reset");
            return;
        }

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
            SendOrQueueSingle(context, subscriber, evt);
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

    private void EmitProtoVectorSegment(IContext context, OutputVectorSegment message)
    {
        if (message.BrainId is null || !message.BrainId.TryToGuid(out var brainId) || brainId != _brainId)
        {
            RecordVectorReject("brain_id_mismatch");
            return;
        }

        if (LogOutput)
        {
            Console.WriteLine(
                $"OutputCoordinator[{_brainId:D}] receive proto segment tick={message.TickId} start={message.OutputIndexStart} values={message.Values.Count} sender={PidLabel(context.Sender)}.");
        }

        EmitVectorSegment(
            context,
            new EmitOutputVectorSegment(message.OutputIndexStart, message.Values, message.TickId));
    }

    private void EmitVectorSegment(IContext context, EmitOutputVectorSegment message)
    {
        if (message.TickId < _minimumAcceptedTickId)
        {
            RecordVectorReject("tick_superseded_by_reset");
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
        if (startIndex >= _outputWidth)
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
        if (LogOutput)
        {
            Console.WriteLine(
                $"OutputCoordinator[{_brainId:D}] publish vector tick={tickId} width={values.Count} vectorSubs={_vectorSubscribers.Count}.");
        }

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
            SendOrQueueVector(context, subscriber, evt);
        }
    }

    private void SendOrQueueSingle(IContext context, OutputSubscriberState subscriber, OutputEvent evt)
    {
        if (subscriber.DeliveryMode == OutputSubscriptionDeliveryMode.LatestOnly)
        {
            QueueLatestSingle(context, subscriber, evt);
            return;
        }

        context.Send(subscriber.Pid, evt);
    }

    private void SendOrQueueVector(IContext context, OutputSubscriberState subscriber, OutputVectorEvent evt)
    {
        if (subscriber.DeliveryMode == OutputSubscriptionDeliveryMode.LatestOnly)
        {
            QueueLatestVector(context, subscriber, evt);
            return;
        }

        context.Send(subscriber.Pid, evt);
    }

    private void QueueLatestSingle(IContext context, OutputSubscriberState subscriber, OutputEvent evt)
    {
        if (subscriber.PendingSingle is not null)
        {
            IoTelemetry.RecordOutputSubscriberDropped(
                _brainId,
                stream: "single",
                reason: "latest_replaced",
                subscriber.DeliveryMode,
                _outputWidth);
        }

        subscriber.PendingSingle = evt;
        ScheduleLatestOnlyFlush(context, subscriber, vector: false);
    }

    private void QueueLatestVector(IContext context, OutputSubscriberState subscriber, OutputVectorEvent evt)
    {
        if (subscriber.PendingVector is not null)
        {
            IoTelemetry.RecordOutputSubscriberDropped(
                _brainId,
                stream: "vector",
                reason: "latest_replaced",
                subscriber.DeliveryMode,
                _outputWidth);
        }

        subscriber.PendingVector = evt;
        ScheduleLatestOnlyFlush(context, subscriber, vector: true);
    }

    private static void ScheduleLatestOnlyFlush(IContext context, OutputSubscriberState subscriber, bool vector)
    {
        if (subscriber.FlushScheduled)
        {
            return;
        }

        subscriber.FlushScheduled = true;
        ScheduleSelf(context, LatestOnlyFlushDelay, new FlushLatestOutputSubscriberRequest(subscriber.Key, vector));
    }

    private void FlushLatestOutputSubscriber(IContext context, FlushLatestOutputSubscriberRequest message)
    {
        var subscribers = message.Vector ? _vectorSubscribers : _outputSubscribers;
        if (!subscribers.TryGetValue(message.SubscriberKey, out var subscriber))
        {
            return;
        }

        subscriber.FlushScheduled = false;
        if (message.Vector)
        {
            var output = subscriber.PendingVector;
            subscriber.PendingVector = null;
            if (output is not null)
            {
                context.Send(subscriber.Pid, output);
            }

            return;
        }

        var single = subscriber.PendingSingle;
        subscriber.PendingSingle = null;
        if (single is not null)
        {
            context.Send(subscriber.Pid, single);
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

    private void HandleRuntimeReset(IContext context, ApplyOutputCoordinatorRuntimeReset message)
    {
        if (message.BrainId != _brainId)
        {
            Respond(context, "reset_brain_runtime_state", success: false);
            return;
        }

        var minimumAcceptedTickId = message.MinimumAcceptedTickId == 0 ? 1UL : message.MinimumAcceptedTickId;
        _pendingVectors.Clear();
        _minimumAcceptedTickId = Math.Max(_minimumAcceptedTickId, minimumAcceptedTickId);
        _latestCompletedVectorTick = Math.Max(_latestCompletedVectorTick, _minimumAcceptedTickId - 1);
        Respond(context, "reset_brain_runtime_state", success: true);
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

    private void AddSubscriber(
        IContext context,
        PID? sender,
        OutputSubscriptionDeliveryMode deliveryMode,
        Dictionary<string, OutputSubscriberState> set)
    {
        if (sender is null)
        {
            return;
        }

        var key = PidKey(sender);
        var normalizedMode = ResolveDeliveryMode(deliveryMode);
        if (set.TryGetValue(key, out var existing))
        {
            existing.Update(sender, normalizedMode);
            return;
        }

        set[key] = new OutputSubscriberState(key, sender, normalizedMode);
        RetainSubscriberWatch(context, sender);
    }

    private void RemoveSubscriber(
        IContext context,
        PID? sender,
        Dictionary<string, OutputSubscriberState> set,
        string stream)
    {
        if (sender is null)
        {
            return;
        }

        RemoveSubscriberByKey(context, PidKey(sender), set, stream, "unsubscribe", releaseWatch: true);
    }

    private bool RemoveSubscriberByKey(
        IContext context,
        string key,
        Dictionary<string, OutputSubscriberState> set,
        string stream,
        string reason,
        bool releaseWatch)
    {
        if (!set.Remove(key, out var removed))
        {
            return false;
        }

        removed.PendingSingle = null;
        removed.PendingVector = null;
        if (releaseWatch)
        {
            ReleaseSubscriberWatch(context, removed.Pid);
        }

        IoTelemetry.RecordOutputSubscriberRemoved(_brainId, stream, reason, removed.DeliveryMode, _outputWidth);
        return true;
    }

    private void RetainSubscriberWatch(IContext context, PID pid)
    {
        var key = PidKey(pid);
        if (_subscriberWatchRefCounts.TryGetValue(key, out var count))
        {
            _subscriberWatchRefCounts[key] = count + 1;
            return;
        }

        _subscriberWatchRefCounts[key] = 1;
        try
        {
            context.Watch(pid);
        }
        catch
        {
            // Watch is a cleanup optimization; subscription delivery still works without it.
        }
    }

    private void ReleaseSubscriberWatch(IContext context, PID pid)
    {
        var key = PidKey(pid);
        if (!_subscriberWatchRefCounts.TryGetValue(key, out var count))
        {
            return;
        }

        if (count > 1)
        {
            _subscriberWatchRefCounts[key] = count - 1;
            return;
        }

        _subscriberWatchRefCounts.Remove(key);
        try
        {
            context.Unwatch(pid);
        }
        catch
        {
        }
    }

    private static void ScheduleSelf(IContext context, TimeSpan delay, object message)
    {
        if (delay <= TimeSpan.Zero)
        {
            context.Send(context.Self, message);
            return;
        }

        context.ReenterAfter(Task.Delay(delay), _ =>
        {
            context.Send(context.Self, message);
            return Task.CompletedTask;
        });
    }

    private static string PidKey(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private static OutputSubscriptionDeliveryMode ResolveDeliveryMode(OutputSubscriptionDeliveryMode deliveryMode)
        => deliveryMode == OutputSubscriptionDeliveryMode.LatestOnly
            ? OutputSubscriptionDeliveryMode.LatestOnly
            : OutputSubscriptionDeliveryMode.Exact;

    private static PID? ResolveSubscriberPid(IContext context, string? subscriberActor)
    {
        if (TryParsePid(subscriberActor, out var parsed))
        {
            return parsed;
        }

        return context.Sender;
    }

    private static string PidLabel(PID? pid)
    {
        if (pid is null)
        {
            return "<missing>";
        }

        return string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";
    }

    private void Respond(IContext context, string command, bool success)
    {
        if (context.Sender is null)
        {
            return;
        }

        context.Respond(new IoCommandAck
        {
            BrainId = _brainIdProto,
            Command = command,
            Success = success,
            Message = success ? "applied" : "ignored"
        });
    }

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "1" or "true" or "yes" or "on" => true,
            _ => false
        };
    }

    private static bool TryParsePid(string? value, out PID pid)
    {
        pid = new PID(string.Empty, string.Empty);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        var slash = trimmed.IndexOf('/');
        if (slash <= 0)
        {
            pid = new PID(string.Empty, trimmed);
            return true;
        }

        var address = trimmed[..slash];
        var id = trimmed[(slash + 1)..];
        if (string.IsNullOrWhiteSpace(id))
        {
            return false;
        }

        pid = new PID(address, id);
        return true;
    }

    private sealed record SubscriptionCommand(
        string? SubscriberActor,
        OutputSubscriptionDeliveryMode DeliveryMode,
        Dictionary<string, OutputSubscriberState> Set,
        string Command,
        string LogLabel,
        string Stream,
        bool Subscribe);

    private sealed record FlushLatestOutputSubscriberRequest(string SubscriberKey, bool Vector);

    private sealed class OutputSubscriberState
    {
        public OutputSubscriberState(string key, PID pid, OutputSubscriptionDeliveryMode deliveryMode)
        {
            Key = key;
            Pid = pid;
            DeliveryMode = deliveryMode;
        }

        public string Key { get; }
        public PID Pid { get; private set; }
        public OutputSubscriptionDeliveryMode DeliveryMode { get; private set; }
        public bool FlushScheduled { get; set; }
        public OutputEvent? PendingSingle { get; set; }
        public OutputVectorEvent? PendingVector { get; set; }

        public void Update(PID pid, OutputSubscriptionDeliveryMode deliveryMode)
        {
            Pid = pid;
            if (DeliveryMode != deliveryMode)
            {
                PendingSingle = null;
                PendingVector = null;
                FlushScheduled = false;
            }

            DeliveryMode = deliveryMode;
        }
    }

    private sealed class PendingVectorTick
    {
        public PendingVectorTick(float[] values, bool[] filled)
        {
            Values = values;
            Filled = filled;
        }

        public float[] Values { get; set; }
        public bool[] Filled { get; set; }
        public int FilledCount { get; set; }
    }
}
