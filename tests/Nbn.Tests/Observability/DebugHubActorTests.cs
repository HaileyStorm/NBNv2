using Nbn.Proto;
using Nbn.Proto.Debug;
using Nbn.Runtime.Observability;
using Proto;

namespace Nbn.Tests.Observability;

public sealed class DebugHubActorTests
{
    [Fact]
    public async Task Subscribe_WithScopedFilters_DeliversOnlyMatchingEvents()
    {
        var system = new ActorSystem();
        var root = system.Root;

        var hub = root.Spawn(Props.FromProducer(() => new DebugHubActor()));
        var probe = root.Spawn(Props.FromProducer(static () => new DebugInboundProbeActor()));
        var subscriber = PidLabel(probe);

        root.Send(hub, new DebugSubscribe
        {
            SubscriberActor = subscriber,
            MinSeverity = Severity.SevWarn,
            IncludeContextPrefixes = { "hivemind." },
            ExcludeContextPrefixes = { "hivemind.tick" },
            IncludeSummaryPrefixes = { "brain." },
            ExcludeSummaryPrefixes = { "brain.terminated" }
        });

        root.Send(hub, NewOutbound(Severity.SevInfo, "hivemind.brain.spawned", "brain.spawned"));
        root.Send(hub, NewOutbound(Severity.SevWarn, "hivemind.brain.spawned", "brain.spawned"));
        root.Send(hub, NewOutbound(Severity.SevDebug, "hivemind.tick.timeout", "tick.timeout"));
        root.Send(hub, NewOutbound(Severity.SevDebug, "region.signal.late", "brain.spawned"));
        root.Send(hub, NewOutbound(Severity.SevDebug, "hivemind.brain.terminated", "brain.terminated"));
        root.Send(hub, NewOutbound(Severity.SevDebug, "hivemind.brain.spawned", "tick.override"));

        var snapshot = await WaitForEventsAsync(root, probe, minEventCount: 1, TimeSpan.FromSeconds(2));
        Assert.Single(snapshot.Events);
        Assert.Equal("hivemind.brain.spawned", snapshot.Events[0].Context);
        Assert.Equal("brain.spawned", snapshot.Events[0].Summary);
        Assert.Equal(Severity.SevWarn, snapshot.Events[0].Severity);
    }

    [Fact]
    public async Task Subscribe_WithoutScopedPrefixes_PreservesSeverityAndRegexBehavior()
    {
        var system = new ActorSystem();
        var root = system.Root;

        var hub = root.Spawn(Props.FromProducer(() => new DebugHubActor()));
        var probe = root.Spawn(Props.FromProducer(static () => new DebugInboundProbeActor()));
        var subscriber = PidLabel(probe);

        root.Send(hub, new DebugSubscribe
        {
            SubscriberActor = subscriber,
            MinSeverity = Severity.SevWarn,
            ContextRegex = "^hivemind\\..*"
        });

        root.Send(hub, NewOutbound(Severity.SevWarn, "hivemind.brain.spawned", "brain.spawned"));
        root.Send(hub, NewOutbound(Severity.SevDebug, "hivemind.brain.spawned", "brain.spawned"));
        root.Send(hub, NewOutbound(Severity.SevWarn, "region.signal.late", "signal.late"));

        var snapshot = await WaitForEventsAsync(root, probe, minEventCount: 1, TimeSpan.FromSeconds(2));
        Assert.Single(snapshot.Events);
        Assert.Equal("hivemind.brain.spawned", snapshot.Events[0].Context);
        Assert.Equal(Severity.SevWarn, snapshot.Events[0].Severity);
    }

    private static DebugOutbound NewOutbound(Severity severity, string context, string summary)
        => new()
        {
            Severity = severity,
            Context = context,
            Summary = summary,
            Message = $"{context}:{summary}",
            SenderActor = "test-producer",
            SenderNode = "test-node",
            TimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private static async Task<DebugProbeSnapshot> WaitForEventsAsync(IRootContext root, PID probe, int minEventCount, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        DebugProbeSnapshot latest = new(Array.Empty<DebugProbeEvent>());
        while (DateTime.UtcNow <= deadline)
        {
            latest = await root.RequestAsync<DebugProbeSnapshot>(probe, new GetDebugProbeSnapshot(), timeout).ConfigureAwait(false);
            if (latest.Events.Count >= minEventCount)
            {
                return latest;
            }

            await Task.Delay(20).ConfigureAwait(false);
        }

        return latest;
    }

    private sealed record GetDebugProbeSnapshot;

    private sealed record DebugProbeSnapshot(IReadOnlyList<DebugProbeEvent> Events);

    private sealed record DebugProbeEvent(Severity Severity, string Context, string Summary);

    private sealed class DebugInboundProbeActor : IActor
    {
        private readonly List<DebugProbeEvent> _events = new();

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case DebugInbound inbound:
                    if (inbound.Outbound is not null)
                    {
                        _events.Add(new DebugProbeEvent(
                            inbound.Outbound.Severity,
                            inbound.Outbound.Context ?? string.Empty,
                            inbound.Outbound.Summary ?? string.Empty));
                    }

                    break;
                case GetDebugProbeSnapshot:
                    context.Respond(new DebugProbeSnapshot(_events.ToArray()));
                    break;
            }

            return Task.CompletedTask;
        }
    }
}
