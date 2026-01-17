using System.Diagnostics;
using Nbn.Proto.Viz;
using Proto;

namespace Nbn.Runtime.Observability;

public sealed class VizHubActor : IActor
{
    private readonly Dictionary<string, PID> _subscribers = new(StringComparer.Ordinal);

    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
                return Task.CompletedTask;
            case Nbn.Proto.Viz.VizSubscribe subscribe:
                HandleSubscribe(context, subscribe.SubscriberActor);
                return Task.CompletedTask;
            case Nbn.Proto.Viz.VizUnsubscribe unsubscribe:
                HandleUnsubscribe(context, unsubscribe.SubscriberActor);
                return Task.CompletedTask;
            case Nbn.Proto.Viz.VizFlushAll:
                FlushAll(context);
                return Task.CompletedTask;
            case PID pid:
                HandleSubscribe(context, pid);
                return Task.CompletedTask;
            case VisualizationEvent vizEvent:
                HandleEvent(context, vizEvent);
                return Task.CompletedTask;
            case Terminated terminated:
                HandleTerminated(terminated);
                return Task.CompletedTask;
        }

        return Task.CompletedTask;
    }

    private void HandleSubscribe(IContext context, string? subscriberActor)
    {
        if (!TryParsePid(subscriberActor, out var pid))
        {
            return;
        }

        HandleSubscribe(context, pid);
    }

    private void HandleSubscribe(IContext context, PID pid)
    {
        var key = PidKey(pid);
        if (_subscribers.TryAdd(key, pid))
        {
            context.Watch(pid);
            ObservabilityTelemetry.Metrics.VizSubscribers.Add(1);
        }
        else
        {
            _subscribers[key] = pid;
        }
    }

    private void HandleUnsubscribe(IContext context, string? subscriberActor)
    {
        if (!TryParsePid(subscriberActor, out var pid))
        {
            return;
        }

        HandleUnsubscribe(context, pid);
    }

    private void HandleUnsubscribe(IContext context, PID pid)
    {
        var key = PidKey(pid);
        if (_subscribers.Remove(key))
        {
            context.Unwatch(pid);
            ObservabilityTelemetry.Metrics.VizSubscribers.Add(-1);
        }
    }

    private void FlushAll(IContext context)
    {
        foreach (var pid in _subscribers.Values)
        {
            context.Unwatch(pid);
        }

        if (_subscribers.Count > 0)
        {
            ObservabilityTelemetry.Metrics.VizSubscribers.Add(-_subscribers.Count);
        }

        _subscribers.Clear();
    }

    private void HandleEvent(IContext context, VisualizationEvent vizEvent)
    {
        ObservabilityTelemetry.Metrics.VizEventTotal.Add(
            1,
            new KeyValuePair<string, object?>("type", vizEvent.Type.ToString()));

        if (_subscribers.Count == 0)
        {
            return;
        }

        using var activity = ObservabilityTelemetry.ActivitySource.HasListeners()
            ? ObservabilityTelemetry.ActivitySource.StartActivity("viz.publish", ActivityKind.Internal)
            : null;

        activity?.SetTag("viz.type", vizEvent.Type.ToString());

        var delivered = 0;
        foreach (var subscriber in _subscribers.Values)
        {
            context.Send(subscriber, vizEvent);
            delivered++;
        }

        if (delivered > 0)
        {
            ObservabilityTelemetry.Metrics.VizDeliveredTotal.Add(
                delivered,
                new KeyValuePair<string, object?>("type", vizEvent.Type.ToString()));
        }
    }

    private void HandleTerminated(Terminated terminated)
    {
        var key = PidKey(terminated.Who);
        if (_subscribers.Remove(key))
        {
            ObservabilityTelemetry.Metrics.VizSubscribers.Add(-1);
        }
    }

    private static string PidKey(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private static bool TryParsePid(string? value, out PID pid)
    {
        pid = new PID();
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        var slashIndex = trimmed.IndexOf('/');
        if (slashIndex <= 0)
        {
            pid.Id = trimmed;
            return true;
        }

        var address = trimmed[..slashIndex];
        var id = trimmed[(slashIndex + 1)..];

        if (string.IsNullOrWhiteSpace(id))
        {
            return false;
        }

        pid.Address = address;
        pid.Id = id;
        return true;
    }
}
