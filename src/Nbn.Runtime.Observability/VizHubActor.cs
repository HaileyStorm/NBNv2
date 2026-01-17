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
            case VizSubscribe subscribe:
                HandleSubscribe(context, subscribe.Subscriber);
                return Task.CompletedTask;
            case VizUnsubscribe unsubscribe:
                HandleUnsubscribe(context, unsubscribe.Subscriber);
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

    private void HandleUnsubscribe(IContext context, PID pid)
    {
        var key = PidKey(pid);
        if (_subscribers.Remove(key))
        {
            context.Unwatch(pid);
            ObservabilityTelemetry.Metrics.VizSubscribers.Add(-1);
        }
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
}
