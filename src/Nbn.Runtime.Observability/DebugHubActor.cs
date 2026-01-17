using System.Diagnostics;
using System.Text.RegularExpressions;
using Nbn.Proto;
using Nbn.Proto.Debug;
using Proto;

namespace Nbn.Runtime.Observability;

public sealed class DebugHubActor : IActor
{
    private readonly Dictionary<string, DebugSubscriber> _subscribers = new(StringComparer.Ordinal);
    private readonly TimeProvider _timeProvider;

    public DebugHubActor(TimeProvider? timeProvider = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
                return Task.CompletedTask;
            case DebugSubscribe subscribe:
                HandleSubscribe(context, subscribe);
                return Task.CompletedTask;
            case DebugUnsubscribe unsubscribe:
                HandleUnsubscribe(context, unsubscribe);
                return Task.CompletedTask;
            case DebugFlushAll:
                FlushAll(context);
                return Task.CompletedTask;
            case DebugOutbound outbound:
                HandleOutbound(context, outbound);
                return Task.CompletedTask;
            case DebugInbound inbound:
                HandleInbound(context, inbound);
                return Task.CompletedTask;
            case Terminated terminated:
                HandleTerminated(terminated);
                return Task.CompletedTask;
        }

        return Task.CompletedTask;
    }

    private void HandleSubscribe(IContext context, DebugSubscribe subscribe)
    {
        if (!TryParsePid(subscribe.SubscriberActor, out var pid))
        {
            return;
        }

        var key = PidKey(pid);
        var regex = CompileRegex(subscribe.ContextRegex);
        var subscriber = new DebugSubscriber(pid, subscribe.MinSeverity, regex);

        if (_subscribers.TryAdd(key, subscriber))
        {
            context.Watch(pid);
            ObservabilityTelemetry.Metrics.DebugSubscribers.Add(1);
        }
        else
        {
            _subscribers[key] = subscriber;
        }
    }

    private void HandleUnsubscribe(IContext context, DebugUnsubscribe unsubscribe)
    {
        if (!TryParsePid(unsubscribe.SubscriberActor, out var pid))
        {
            return;
        }

        var key = PidKey(pid);
        if (_subscribers.Remove(key))
        {
            context.Unwatch(pid);
            ObservabilityTelemetry.Metrics.DebugSubscribers.Add(-1);
        }
    }

    private void FlushAll(IContext context)
    {
        foreach (var entry in _subscribers.Values)
        {
            context.Unwatch(entry.Pid);
        }

        if (_subscribers.Count > 0)
        {
            ObservabilityTelemetry.Metrics.DebugSubscribers.Add(-_subscribers.Count);
        }

        _subscribers.Clear();
    }

    private void HandleOutbound(IContext context, DebugOutbound outbound)
    {
        var inbound = new DebugInbound
        {
            Outbound = outbound,
            ServerReceivedMs = NowMs()
        };

        Publish(context, inbound);
    }

    private void HandleInbound(IContext context, DebugInbound inbound)
    {
        if (inbound.ServerReceivedMs == 0)
        {
            inbound.ServerReceivedMs = NowMs();
        }

        Publish(context, inbound);
    }

    private void Publish(IContext context, DebugInbound inbound)
    {
        var outbound = inbound.Outbound;
        var severity = outbound?.Severity ?? default;
        ObservabilityTelemetry.Metrics.DebugInboundTotal.Add(1, new KeyValuePair<string, object?>("severity", severity.ToString()));

        if (_subscribers.Count == 0)
        {
            return;
        }

        using var activity = ObservabilityTelemetry.ActivitySource.HasListeners()
            ? ObservabilityTelemetry.ActivitySource.StartActivity("debug.publish", ActivityKind.Internal)
            : null;

        activity?.SetTag("debug.severity", severity.ToString());
        activity?.SetTag("debug.context", outbound?.Context ?? string.Empty);

        var contextValue = outbound?.Context ?? string.Empty;
        var delivered = 0;

        foreach (var subscriber in _subscribers.Values)
        {
            if (!subscriber.Accepts(severity, contextValue))
            {
                continue;
            }

            context.Send(subscriber.Pid, inbound);
            delivered++;
        }

        if (delivered > 0)
        {
            ObservabilityTelemetry.Metrics.DebugDeliveredTotal.Add(delivered, new KeyValuePair<string, object?>("severity", severity.ToString()));
        }
    }

    private void HandleTerminated(Terminated terminated)
    {
        var key = PidKey(terminated.Who);
        if (_subscribers.Remove(key))
        {
            ObservabilityTelemetry.Metrics.DebugSubscribers.Add(-1);
        }
    }

    private ulong NowMs()
    {
        var now = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        return now <= 0 ? 0UL : (ulong)now;
    }

    private static Regex? CompileRegex(string? pattern)
    {
        if (string.IsNullOrWhiteSpace(pattern))
        {
            return null;
        }

        try
        {
            return new Regex(pattern, RegexOptions.Compiled | RegexOptions.CultureInvariant);
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

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

    private static string PidKey(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private sealed record DebugSubscriber(PID Pid, Severity MinSeverity, Regex? ContextRegex)
    {
        public bool Accepts(Severity severity, string context)
        {
            if (severity < MinSeverity)
            {
                return false;
            }

            if (ContextRegex is null)
            {
                return true;
            }

            return ContextRegex.IsMatch(context);
        }
    }
}
