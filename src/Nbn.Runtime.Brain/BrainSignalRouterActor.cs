using System.Diagnostics;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;

namespace Nbn.Runtime.Brain;

public sealed class BrainSignalRouterActor : IActor
{
    private static readonly bool LogDelivery = IsEnvTrue("NBN_BRAIN_LOG_DELIVERY");
    private readonly Guid _brainId;
    private readonly Nbn.Proto.Uuid _brainIdProto;
    private readonly RoutingTable _routingTable = new();
    private readonly Dictionary<ulong, TickOutbox> _pendingOutboxes = new();
    private readonly Dictionary<ulong, PendingDeliver> _pendingDeliveries = new();
    private readonly Dictionary<uint, float> _pendingInputs = new();
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
            case InputWrite inputWrite:
                HandleInputWrite(inputWrite);
                break;
            case InputVector inputVector:
                HandleInputVector(inputVector);
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
        ExpirePendingDeliveries(tickDeliver.TickId);

        if (_pendingDeliveries.ContainsKey(tickDeliver.TickId))
        {
            return;
        }

        var deliveredBatches = 0u;
        var deliveredContribs = 0u;
        var expectedAcks = 0;
        var fallbackRoutes = 0;
        var missingRouteLogged = false;
        var inputRoutes = Array.Empty<ShardRoute>();
        List<Contribution>? inputContribs = null;

        var outboxDestinationCount = 0;
        if (_pendingOutboxes.TryGetValue(tickDeliver.TickId, out var outbox))
        {
            outboxDestinationCount = outbox.Destinations.Count;
            foreach (var entry in outbox.Destinations)
            {
                if (!_routingTable.TryGetPid(entry.Key, out var pid) || pid is null)
                {
                    if (!TryGetFallbackPid(entry.Value.RegionId, out pid))
                    {
                        if (LogDelivery && !missingRouteLogged)
                        {
                            Log($"TickDeliver missing route tick={tickDeliver.TickId} destShard={entry.Key} destRegion={entry.Value.RegionId} routes={FormatRoutes()}");
                            missingRouteLogged = true;
                        }
                        continue;
                    }

                    fallbackRoutes++;
                }

                if (pid is null)
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
                // Use Request so RegionShard can reply with SignalBatchAck to this router.
                context.Request(pid, signalBatch);

                deliveredBatches++;
                deliveredContribs += (uint)entry.Value.Contribs.Count;
                expectedAcks++;
            }

            _pendingOutboxes.Remove(tickDeliver.TickId);
        }

        if (_pendingInputs.Count > 0)
        {
            inputRoutes = _routingTable.Entries
                .Where(entry => entry.ShardId.RegionId == NbnConstants.InputRegionId)
                .ToArray();

            if (inputRoutes.Length > 0)
            {
                inputContribs = new List<Contribution>(_pendingInputs.Count);
                foreach (var entry in _pendingInputs)
                {
                    inputContribs.Add(new Contribution
                    {
                        TargetNeuronId = entry.Key,
                        Value = entry.Value
                    });
                }

                foreach (var route in inputRoutes)
                {
                    var signalBatch = new SignalBatch
                    {
                        BrainId = _brainIdProto,
                        RegionId = (uint)NbnConstants.InputRegionId,
                        ShardId = route.ShardId.ToProtoShardId32(),
                        TickId = tickDeliver.TickId
                    };

                    signalBatch.Contribs.AddRange(inputContribs);
                    context.Request(route.Pid, signalBatch);

                    deliveredBatches++;
                    deliveredContribs += (uint)inputContribs.Count;
                    expectedAcks++;
                }

                _pendingInputs.Clear();
            }
        }

        if (LogDelivery)
        {
            Log($"TickDeliver start tick={tickDeliver.TickId} outboxDestinations={outboxDestinationCount} inputRoutes={inputRoutes.Length} deliveredBatches={deliveredBatches} deliveredContribs={deliveredContribs} expectedAcks={expectedAcks} routes={_routingTable.Count} fallbackRoutes={fallbackRoutes}");
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

            var replyTo = context.Sender ?? context.Parent;
            if (replyTo is not null)
            {
                context.Send(replyTo, deliverDone);
            }
            else if (LogDelivery)
            {
                Log($"TickDeliver done dropped (no reply target) tick={tickDeliver.TickId}");
            }
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
            BrainTelemetry.RecordLateAck();
            if (LogDelivery)
            {
                var senderLabel = context.Sender is null ? "(null)" : $"{context.Sender.Address}/{context.Sender.Id}";
                Log($"SignalBatchAck late or unknown tick={ack.TickId} sender={senderLabel} region={ack.RegionId} shard={ack.ShardId?.Value ?? 0}");
            }
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

    private void HandleInputWrite(InputWrite message)
    {
        if (!IsForBrain(message.BrainId))
        {
            return;
        }

        _pendingInputs[message.InputIndex] = message.Value;
    }

    private void HandleInputVector(InputVector message)
    {
        if (!IsForBrain(message.BrainId))
        {
            return;
        }

        if (message.Values.Count == 0)
        {
            return;
        }

        for (var i = 0; i < message.Values.Count; i++)
        {
            _pendingInputs[(uint)i] = message.Values[i];
        }
    }

    private void ExpirePendingDeliveries(ulong currentTickId)
    {
        if (_pendingDeliveries.Count == 0)
        {
            return;
        }

        List<ulong>? expired = null;
        foreach (var entry in _pendingDeliveries)
        {
            if (entry.Key < currentTickId)
            {
                expired ??= new List<ulong>();
                expired.Add(entry.Key);
            }
        }

        if (expired is null)
        {
            return;
        }

        foreach (var tickId in expired)
        {
            _pendingDeliveries.Remove(tickId);
        }

        BrainTelemetry.RecordDeliveryTimeout(expired.Count);

        if (LogDelivery)
        {
            Log($"Pending deliveries expired before tick={currentTickId}. expired={string.Join(",", expired)}");
        }
    }

    private bool TryGetFallbackPid(uint regionId, out PID? pid)
    {
        pid = null;
        PID? candidate = null;

        foreach (var entry in _routingTable.Entries)
        {
            if (entry.ShardId.RegionId != (int)regionId)
            {
                continue;
            }

            if (candidate is not null)
            {
                pid = null;
                return false;
            }

            candidate = entry.Pid;
        }

        if (candidate is null)
        {
            pid = null;
            return false;
        }

        pid = candidate;
        return true;
    }

    private string FormatRoutes()
    {
        if (_routingTable.Count == 0)
        {
            return "(none)";
        }

        var routes = new string[_routingTable.Entries.Count];
        for (var i = 0; i < _routingTable.Entries.Count; i++)
        {
            routes[i] = _routingTable.Entries[i].ShardId.ToString();
        }

        return string.Join(",", routes);
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

    private static void Log(string message)
        => Console.WriteLine($"[{DateTime.UtcNow:O}] [BrainSignalRouter] {message}");

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Equals("1", StringComparison.OrdinalIgnoreCase)
               || value.Equals("true", StringComparison.OrdinalIgnoreCase)
               || value.Equals("yes", StringComparison.OrdinalIgnoreCase)
               || value.Equals("y", StringComparison.OrdinalIgnoreCase);
    }
}
