using System.Diagnostics;
using Nbn.Proto.Control;
using Nbn.Proto.Signal;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;

namespace Nbn.Runtime.Brain;

public sealed class BrainSignalRouterActor : IActor
{
    private readonly Guid _brainId;
    private readonly Nbn.Proto.Uuid _brainIdProto;
    private readonly RoutingTable _routingTable = new();
    private readonly Dictionary<ulong, TickOutbox> _pendingOutboxes = new();
    private readonly Dictionary<ulong, PendingDeliver> _pendingDeliveries = new();
    private RoutingTableSnapshot _routingSnapshot = RoutingTableSnapshot.Empty;

    public BrainSignalRouterActor(Guid brainId)
    {
        _brainId = brainId;
        _brainIdProto = brainId.ToProtoUuid();
    }

    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case SetRoutingTable setRouting:
                ApplyRoutingTable(setRouting.Table);
                break;
            case GetRoutingTable:
                context.Respond(_routingSnapshot);
                break;
            case TickCompute tickCompute:
                HandleTickCompute(context, tickCompute);
                break;
            case OutboxBatch outboxBatch:
                HandleOutboxBatch(outboxBatch);
                break;
            case TickDeliver tickDeliver:
                HandleTickDeliver(context, tickDeliver);
                break;
            case SignalBatchAck ack:
                HandleSignalBatchAck(context, ack);
                break;
            case TickComputeDone tickComputeDone:
                ForwardToParent(context, tickComputeDone);
                break;
        }

        return Task.CompletedTask;
    }

    private void ApplyRoutingTable(RoutingTableSnapshot? snapshot)
    {
        _routingSnapshot = snapshot ?? RoutingTableSnapshot.Empty;
        _routingTable.Replace(_routingSnapshot.Routes);
    }

    private void HandleTickCompute(IContext context, TickCompute tickCompute)
    {
        if (_routingTable.Count == 0)
        {
            return;
        }

        foreach (var route in _routingTable.Entries)
        {
            context.Send(route.Pid, tickCompute);
        }
    }

    private void HandleOutboxBatch(OutboxBatch outboxBatch)
    {
        if (!IsForBrain(outboxBatch.BrainId))
        {
            return;
        }

        var tickId = outboxBatch.TickId;
        if (!_pendingOutboxes.TryGetValue(tickId, out var outbox))
        {
            outbox = new TickOutbox();
            _pendingOutboxes[tickId] = outbox;
        }

        outbox.Add(outboxBatch);
    }

    private void HandleTickDeliver(IContext context, TickDeliver tickDeliver)
    {
        if (_pendingDeliveries.ContainsKey(tickDeliver.TickId))
        {
            return;
        }

        var deliveredBatches = 0u;
        var deliveredContribs = 0u;
        var expectedAcks = 0;

        if (_pendingOutboxes.TryGetValue(tickDeliver.TickId, out var outbox))
        {
            foreach (var entry in outbox.Destinations)
            {
                if (!_routingTable.TryGetPid(entry.Key, out var pid) || pid is null)
                {
                    continue;
                }

                var signalBatch = new SignalBatch
                {
                    BrainId = _brainIdProto,
                    RegionId = entry.Value.RegionId,
                    ShardId = entry.Key.ToProtoShardId32(),
                    TickId = tickDeliver.TickId
                };

                signalBatch.Contribs.AddRange(entry.Value.Contribs);
                context.Send(pid, signalBatch);

                deliveredBatches++;
                deliveredContribs += (uint)entry.Value.Contribs.Count;
                expectedAcks++;
            }

            _pendingOutboxes.Remove(tickDeliver.TickId);
        }

        if (expectedAcks == 0)
        {
            var deliverDone = new TickDeliverDone
            {
                TickId = tickDeliver.TickId,
                BrainId = _brainIdProto,
                DeliverMs = 0,
                DeliveredBatches = 0,
                DeliveredContribs = 0
            };

            context.Respond(deliverDone);
            return;
        }

        var pending = new PendingDeliver(
            tickDeliver.TickId,
            context.Sender ?? context.Parent,
            Stopwatch.StartNew(),
            deliveredBatches,
            deliveredContribs,
            expectedAcks);

        _pendingDeliveries[tickDeliver.TickId] = pending;
    }

    private void HandleSignalBatchAck(IContext context, SignalBatchAck ack)
    {
        if (!IsForBrain(ack.BrainId))
        {
            return;
        }

        if (!_pendingDeliveries.TryGetValue(ack.TickId, out var pending))
        {
            return;
        }

        pending.AckCount++;
        if (pending.AckCount < pending.ExpectedAcks)
        {
            return;
        }

        _pendingDeliveries.Remove(ack.TickId);
        pending.Stopwatch.Stop();

        var deliverDone = new TickDeliverDone
        {
            TickId = ack.TickId,
            BrainId = _brainIdProto,
            DeliverMs = (ulong)pending.Stopwatch.Elapsed.TotalMilliseconds,
            DeliveredBatches = pending.DeliveredBatches,
            DeliveredContribs = pending.DeliveredContribs
        };

        var replyTo = pending.ReplyTo ?? context.Parent;
        if (replyTo is not null)
        {
            context.Send(replyTo, deliverDone);
        }
    }

    private static void ForwardToParent(IContext context, object message)
    {
        if (context.Parent is null)
        {
            return;
        }

        context.Send(context.Parent, message);
    }

    private bool IsForBrain(Nbn.Proto.Uuid? brainId)
    {
        if (brainId is null)
        {
            return true;
        }

        return brainId.TryToGuid(out var guid) && guid == _brainId;
    }

    private sealed class TickOutbox
    {
        private readonly Dictionary<ShardId32, DestinationBatch> _destinations = new();

        public IReadOnlyDictionary<ShardId32, DestinationBatch> Destinations => _destinations;

        public void Add(OutboxBatch batch)
        {
            var shardId = batch.DestShardId.ToShardId32();
            if (!_destinations.TryGetValue(shardId, out var destination))
            {
                destination = new DestinationBatch(batch.DestRegionId);
                _destinations[shardId] = destination;
            }

            destination.Add(batch.Contribs);
        }
    }

    private sealed class DestinationBatch
    {
        public DestinationBatch(uint regionId)
        {
            RegionId = regionId;
        }

        public uint RegionId { get; }

        public List<Contribution> Contribs { get; } = new();

        public void Add(IEnumerable<Contribution> contribs)
        {
            foreach (var contrib in contribs)
            {
                Contribs.Add(contrib);
            }
        }
    }

    private sealed class PendingDeliver
    {
        public PendingDeliver(
            ulong tickId,
            PID? replyTo,
            Stopwatch stopwatch,
            uint deliveredBatches,
            uint deliveredContribs,
            int expectedAcks)
        {
            TickId = tickId;
            ReplyTo = replyTo;
            Stopwatch = stopwatch;
            DeliveredBatches = deliveredBatches;
            DeliveredContribs = deliveredContribs;
            ExpectedAcks = expectedAcks;
        }

        public ulong TickId { get; }
        public PID? ReplyTo { get; }
        public Stopwatch Stopwatch { get; }
        public uint DeliveredBatches { get; }
        public uint DeliveredContribs { get; }
        public int ExpectedAcks { get; }
        public int AckCount { get; set; }
    }
}
