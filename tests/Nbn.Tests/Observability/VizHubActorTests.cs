using Nbn.Proto.Viz;
using Nbn.Runtime.Observability;
using Proto;

namespace Nbn.Tests.Observability;

public sealed class VizHubActorTests
{
    [Fact]
    public async Task Subscribe_DeliversEventsToAllSubscribers()
    {
        var system = new ActorSystem();
        var root = system.Root;

        var hub = root.Spawn(Props.FromProducer(() => new VizHubActor()));
        var probeA = root.Spawn(Props.FromProducer(static () => new VizProbeActor()));
        var probeB = root.Spawn(Props.FromProducer(static () => new VizProbeActor()));

        root.Send(hub, new VizSubscribe { SubscriberActor = PidLabel(probeA) });
        root.Send(hub, new VizSubscribe { SubscriberActor = PidLabel(probeB) });
        root.Send(hub, NewEvent("evt-1", VizEventType.VizTick, tickId: 12));

        var snapshotA = await WaitForEventsAsync(root, probeA, minEventCount: 1, TimeSpan.FromSeconds(2));
        var snapshotB = await WaitForEventsAsync(root, probeB, minEventCount: 1, TimeSpan.FromSeconds(2));

        Assert.Single(snapshotA.Events);
        Assert.Single(snapshotB.Events);
        Assert.Equal("evt-1", snapshotA.Events[0].EventId);
        Assert.Equal(VizEventType.VizTick, snapshotA.Events[0].Type);
        Assert.Equal((ulong)12, snapshotA.Events[0].TickId);
        Assert.Equal("evt-1", snapshotB.Events[0].EventId);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task Unsubscribe_RemovesOnlyRequestedSubscriber()
    {
        var system = new ActorSystem();
        var root = system.Root;

        var hub = root.Spawn(Props.FromProducer(() => new VizHubActor()));
        var probeA = root.Spawn(Props.FromProducer(static () => new VizProbeActor()));
        var probeB = root.Spawn(Props.FromProducer(static () => new VizProbeActor()));
        var subscriberA = PidLabel(probeA);

        root.Send(hub, new VizSubscribe { SubscriberActor = subscriberA });
        root.Send(hub, new VizSubscribe { SubscriberActor = PidLabel(probeB) });
        root.Send(hub, NewEvent("evt-1", VizEventType.VizTick, tickId: 1));

        await WaitForEventsAsync(root, probeA, minEventCount: 1, TimeSpan.FromSeconds(2));
        await WaitForEventsAsync(root, probeB, minEventCount: 1, TimeSpan.FromSeconds(2));

        root.Send(hub, new VizUnsubscribe { SubscriberActor = subscriberA });
        root.Send(hub, NewEvent("evt-2", VizEventType.VizNeuronBuffer, tickId: 2));

        await Task.Delay(150);

        var snapshotA = await root.RequestAsync<VizProbeSnapshot>(probeA, new GetVizProbeSnapshot(), TimeSpan.FromSeconds(2));
        var snapshotB = await WaitForEventsAsync(root, probeB, minEventCount: 2, TimeSpan.FromSeconds(2));

        Assert.Single(snapshotA.Events);
        Assert.Equal(2, snapshotB.Events.Count);
        Assert.Equal("evt-2", snapshotB.Events[1].EventId);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task FlushAll_StopsFurtherDelivery()
    {
        var system = new ActorSystem();
        var root = system.Root;

        var hub = root.Spawn(Props.FromProducer(() => new VizHubActor()));
        var probe = root.Spawn(Props.FromProducer(static () => new VizProbeActor()));

        root.Send(hub, new VizSubscribe { SubscriberActor = PidLabel(probe) });
        root.Send(hub, NewEvent("evt-1", VizEventType.VizTick, tickId: 1));

        var firstSnapshot = await WaitForEventsAsync(root, probe, minEventCount: 1, TimeSpan.FromSeconds(2));
        Assert.Single(firstSnapshot.Events);

        root.Send(hub, new VizFlushAll());
        root.Send(hub, NewEvent("evt-2", VizEventType.VizNeuronBuffer, tickId: 2));

        await Task.Delay(150);

        var finalSnapshot = await root.RequestAsync<VizProbeSnapshot>(probe, new GetVizProbeSnapshot(), TimeSpan.FromSeconds(2));
        Assert.Single(finalSnapshot.Events);

        await system.ShutdownAsync();
    }

    private static VisualizationEvent NewEvent(string eventId, VizEventType type, ulong tickId)
        => new()
        {
            EventId = eventId,
            TimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Type = type,
            TickId = tickId
        };

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private static async Task<VizProbeSnapshot> WaitForEventsAsync(IRootContext root, PID probe, int minEventCount, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        VizProbeSnapshot latest = new(Array.Empty<VizProbeEvent>());
        while (DateTime.UtcNow <= deadline)
        {
            latest = await root.RequestAsync<VizProbeSnapshot>(probe, new GetVizProbeSnapshot(), timeout).ConfigureAwait(false);
            if (latest.Events.Count >= minEventCount)
            {
                return latest;
            }

            await Task.Delay(20).ConfigureAwait(false);
        }

        return latest;
    }

    private sealed record GetVizProbeSnapshot;

    private sealed record VizProbeSnapshot(IReadOnlyList<VizProbeEvent> Events);

    private sealed record VizProbeEvent(string EventId, VizEventType Type, ulong TickId);

    private sealed class VizProbeActor : IActor
    {
        private readonly List<VizProbeEvent> _events = new();

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case VisualizationEvent vizEvent:
                    _events.Add(new VizProbeEvent(
                        vizEvent.EventId ?? string.Empty,
                        vizEvent.Type,
                        vizEvent.TickId));
                    break;
                case GetVizProbeSnapshot:
                    context.Respond(new VizProbeSnapshot(_events.ToArray()));
                    break;
            }

            return Task.CompletedTask;
        }
    }
}
