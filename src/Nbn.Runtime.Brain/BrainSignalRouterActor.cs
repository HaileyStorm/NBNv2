using System.Diagnostics;
using System.Globalization;
using System.Net;
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
    private static readonly bool LogInputDiagnostics = IsEnvTrue("NBN_INPUT_DIAGNOSTICS_ENABLED");
    private static readonly bool LogInputTraceDiagnostics = IsEnvTrue("NBN_INPUT_TRACE_DIAGNOSTICS_ENABLED");
    private static readonly bool LogTickDiagnostics = IsEnvTrue("NBN_RUNTIME_TICK_DIAGNOSTICS_ENABLED");
    private readonly Guid _brainId;
    private readonly Nbn.Proto.Uuid _brainIdProto;
    private readonly RoutingTable _routingTable = new();
    private readonly Dictionary<ulong, TickOutbox> _pendingOutboxes = new();
    private readonly Dictionary<ulong, PendingDeliver> _pendingDeliveries = new();
    private readonly Dictionary<ulong, PendingInputDrain> _pendingInputDrains = new();
    private PID? _ioGatewayPid;
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
                return HandleSetRoutingTableAsync(context, setRouting.Table);
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
                HandleInputWrite(context, inputWrite);
                break;
            case InputVector inputVector:
                HandleInputVector(context, inputVector);
                break;
            case RuntimeNeuronPulse runtimePulse:
                HandleRuntimeNeuronPulse(context, runtimePulse);
                break;
            case RuntimeNeuronStateWrite runtimeStateWrite:
                HandleRuntimeNeuronStateWrite(context, runtimeStateWrite);
                break;
            case InputDrain inputDrain:
                HandleInputDrain(context, inputDrain);
                break;
            case RegisterIoGateway registerIoGateway:
                return HandleRegisterIoGatewayAsync(context, registerIoGateway);
            case SignalBatchAck ack:
                HandleSignalBatchAck(context, ack);
                break;
            case TickComputeDone tickComputeDone:
                HandleTickComputeDone(context, tickComputeDone);
                break;
        }

        return Task.CompletedTask;
    }

    private async Task HandleSetRoutingTableAsync(IContext context, RoutingTableSnapshot? snapshot)
    {
        _routingSnapshot = snapshot ?? RoutingTableSnapshot.Empty;
        if (_routingSnapshot.Count == 0)
        {
            _routingTable.Replace(_routingSnapshot.Routes);
            if (LogTickDiagnostics)
            {
                Log("SetRoutingTable applied empty snapshot.");
            }
            return;
        }

        var resolvedRoutes = new List<ShardRoute>(_routingSnapshot.Routes.Count);
        foreach (var route in _routingSnapshot.Routes)
        {
            var resolvedRoute = await ResolveRoutingRouteAsync(context, route).ConfigureAwait(false);
            if (resolvedRoute is null)
            {
                continue;
            }

            resolvedRoutes.Add(resolvedRoute);
        }

        _routingSnapshot = new RoutingTableSnapshot(resolvedRoutes);
        _routingTable.Replace(_routingSnapshot.Routes);
        if (LogTickDiagnostics)
        {
            Log($"SetRoutingTable applied {_routingSnapshot.Count} route(s): {FormatRoutes()}");
        }
    }

    private async Task<ShardRoute?> ResolveRoutingRouteAsync(IContext context, ShardRoute route)
    {
        var preferredLocalPid = TryGetPreferredLocalRoutePid(context, route);
        if (preferredLocalPid is not null)
        {
            return route with { Pid = preferredLocalPid };
        }

        var resolvedPid = route.Pid;
        if (!string.IsNullOrWhiteSpace(route.ActorReference))
        {
            var resolved = await RoutablePidReference.ResolveAsync(route.ActorReference).ConfigureAwait(false);
            if (resolved is not null)
            {
                resolvedPid = resolved;
            }
        }

        return resolvedPid is null
            ? null
            : route with { Pid = resolvedPid };
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
            if (LogTickDiagnostics)
            {
                Log($"TickCompute skipped tick={tickCompute.TickId} reason=no_routes");
            }
            return;
        }

        if (LogTickDiagnostics)
        {
            Log(
                $"TickCompute dispatch tick={tickCompute.TickId} routes={_routingTable.Count} targets={string.Join(", ", _routingTable.Entries.Select(static entry => PidLabel(entry.Pid)))}");
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
        ExpirePendingInputDrains(tickDeliver.TickId);
        ExpirePendingDeliveries(tickDeliver.TickId);

        if (_pendingDeliveries.ContainsKey(tickDeliver.TickId) || _pendingInputDrains.ContainsKey(tickDeliver.TickId))
        {
            return;
        }

        if (!ShouldDrainInputs()
            && LogInputDiagnostics
            && _routingTable.Entries.Any(entry => entry.ShardId.RegionId == NbnConstants.InputRegionId))
        {
            LogInput(
                $"TickDeliver skip-drain tick={tickDeliver.TickId} ioGateway={PidLabel(_ioGatewayPid)} inputRoutes={_routingTable.Entries.Count(entry => entry.ShardId.RegionId == NbnConstants.InputRegionId)}");
        }

        if (ShouldDrainInputs())
        {
            var replyTo = context.Sender ?? context.Parent;
            _pendingInputDrains[tickDeliver.TickId] = new PendingInputDrain(
                tickDeliver.TickId,
                replyTo,
                Stopwatch.StartNew());

            if (LogInputTraceDiagnostics)
            {
                LogInput($"TickDeliver request-drain tick={tickDeliver.TickId} ioGateway={PidLabel(_ioGatewayPid)}");
            }

            context.Request(_ioGatewayPid!, new DrainInputs
            {
                BrainId = _brainIdProto,
                TickId = tickDeliver.TickId
            });
            return;
        }

        ProcessTickDeliver(context, tickDeliver.TickId, context.Sender ?? context.Parent, null, Stopwatch.StartNew());
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

        if (ack.ShardId is null)
        {
            EmitSignalBatchAckIgnored(context, ack, "missing_shard");
            return;
        }

        var key = new PendingAckKey(ack.RegionId, ack.ShardId.ToShardId32());
        if (!pending.PendingAckTargets.TryGetValue(key, out var expectedTarget))
        {
            var reason = pending.AckedKeys.Contains(key) ? "duplicate" : "untracked_payload";
            EmitSignalBatchAckIgnored(context, ack, reason);
            return;
        }

        if (!SenderMatchesActorReferenceOrPid(context.Sender, expectedTarget.ActorReference, expectedTarget.SenderPid))
        {
            EmitSignalBatchAckIgnored(context, ack, "sender_mismatch", expectedTarget.SenderPid);
            return;
        }

        pending.PendingAckTargets.Remove(key);
        pending.AckedKeys.Add(key);
        if (pending.PendingAckTargets.Count > 0)
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
            context.Request(replyTo, deliverDone);
        }
    }

    private void HandleTickComputeDone(IContext context, TickComputeDone tickComputeDone)
    {
        if (LogTickDiagnostics)
        {
            Log(
                $"TickComputeDone forward tick={tickComputeDone.TickId} brain={_brainId:D} shard={(tickComputeDone.ShardId is null ? "<missing>" : tickComputeDone.ShardId.ToShardId32().ToString())} sender={PidLabel(context.Sender)} parent={PidLabel(context.Parent)}");
        }

        ForwardToParent(context, tickComputeDone);
    }

    private void HandleInputWrite(IContext context, InputWrite message)
    {
        if (!IsForBrain(message.BrainId))
        {
            return;
        }

        CaptureIoGateway(context.Sender);
        if (LogInputDiagnostics)
        {
            LogInput($"InputWrite received sender={PidLabel(context.Sender)} ioGateway={PidLabel(_ioGatewayPid)} index={message.InputIndex} value={message.Value:0.###}");
        }
    }

    private void HandleInputVector(IContext context, InputVector message)
    {
        if (!IsForBrain(message.BrainId))
        {
            return;
        }

        CaptureIoGateway(context.Sender);
        if (LogInputDiagnostics)
        {
            LogInput($"InputVector received sender={PidLabel(context.Sender)} ioGateway={PidLabel(_ioGatewayPid)} width={message.Values.Count}");
        }
    }

    private void HandleRuntimeNeuronPulse(IContext context, RuntimeNeuronPulse message)
    {
        if (!IsForBrain(message.BrainId))
        {
            return;
        }

        CaptureIoGateway(context.Sender);

        if (!float.IsFinite(message.Value))
        {
            return;
        }

        DispatchToRegionShards(context, message.TargetRegionId, message);
    }

    private void HandleRuntimeNeuronStateWrite(IContext context, RuntimeNeuronStateWrite message)
    {
        if (!IsForBrain(message.BrainId))
        {
            return;
        }

        CaptureIoGateway(context.Sender);

        if (!message.SetBuffer && !message.SetAccumulator)
        {
            return;
        }

        if ((message.SetBuffer && !float.IsFinite(message.BufferValue))
            || (message.SetAccumulator && !float.IsFinite(message.AccumulatorValue)))
        {
            return;
        }

        DispatchToRegionShards(context, message.TargetRegionId, message);
    }

    private void DispatchToRegionShards(IContext context, uint regionId, object message)
    {
        if (_routingTable.Count == 0)
        {
            return;
        }

        foreach (var entry in _routingTable.Entries)
        {
            if (entry.ShardId.RegionId != (int)regionId)
            {
                continue;
            }

            context.Send(entry.Pid, message);
        }
    }

    private void HandleInputDrain(IContext context, InputDrain message)
    {
        if (!IsForBrain(message.BrainId))
        {
            return;
        }

        CaptureIoGateway(context.Sender);

        if (!_pendingInputDrains.TryGetValue(message.TickId, out var pending))
        {
            if (LogInputTraceDiagnostics || message.Contribs.Count > 0)
            {
                LogInput($"InputDrain ignored tick={message.TickId} sender={PidLabel(context.Sender)} contribs={message.Contribs.Count}");
            }

            return;
        }

        _pendingInputDrains.Remove(message.TickId);
        if (LogInputTraceDiagnostics || message.Contribs.Count > 0)
        {
            LogInput($"InputDrain accepted tick={message.TickId} sender={PidLabel(context.Sender)} contribs={message.Contribs.Count}");
        }

        ProcessTickDeliver(context, message.TickId, pending.ReplyTo, message.Contribs, pending.Stopwatch);
    }

    private async Task HandleRegisterIoGatewayAsync(IContext context, RegisterIoGateway message)
    {
        if (!IsForBrain(message.BrainId))
        {
            return;
        }

        var parsed = await RoutablePidReference.ResolveAsync(message.IoGatewayPid).ConfigureAwait(false);
        if (parsed is not null)
        {
            _ioGatewayPid = parsed;
            if (LogInputDiagnostics)
            {
                LogInput($"RegisterIoGateway explicit sender={PidLabel(context.Sender)} ioGateway={PidLabel(_ioGatewayPid)}");
            }

            return;
        }

        CaptureIoGateway(context.Sender);
        if (LogInputDiagnostics)
        {
            LogInput($"RegisterIoGateway sender-capture sender={PidLabel(context.Sender)} ioGateway={PidLabel(_ioGatewayPid)}");
        }
    }

    private void ProcessTickDeliver(
        IContext context,
        ulong tickId,
        PID? replyTo,
        IReadOnlyList<Contribution>? inputContribs,
        Stopwatch? stopwatch)
    {
        var deliveredBatches = 0u;
        var deliveredContribs = 0u;
        var expectedAckTargets = new Dictionary<PendingAckKey, PendingAckTarget>();
        var fallbackRoutes = 0;
        var missingRouteLogged = false;
        var inputRoutes = Array.Empty<ShardRoute>();

        var outboxDestinationCount = 0;
        if (_pendingOutboxes.TryGetValue(tickId, out var outbox))
        {
            outboxDestinationCount = outbox.Destinations.Count;
            foreach (var entry in outbox.Destinations)
            {
                var destinationShardId = entry.Key;
                if (!_routingTable.TryGetRoute(entry.Key, out var route) || route is null || route.Pid is null)
                {
                    if (!TryGetFallbackRoute(entry.Value.RegionId, out var fallbackRoute))
                    {
                        if (LogDelivery && !missingRouteLogged)
                        {
                            Log($"TickDeliver missing route tick={tickId} destShard={entry.Key} destRegion={entry.Value.RegionId} routes={FormatRoutes()}");
                            missingRouteLogged = true;
                        }
                        continue;
                    }

                    fallbackRoutes++;
                    route = fallbackRoute;
                    destinationShardId = route.ShardId;
                }

                if (route?.Pid is null)
                {
                    continue;
                }

                var signalBatch = new SignalBatch
                {
                    BrainId = _brainIdProto,
                    RegionId = entry.Value.RegionId,
                    ShardId = destinationShardId.ToProtoShardId32(),
                    TickId = tickId
                };

                signalBatch.Contribs.AddRange(entry.Value.Contribs);
                // Use Request so RegionShard can reply with SignalBatchAck to this router.
                context.Request(route.Pid, signalBatch);
                expectedAckTargets[new PendingAckKey(entry.Value.RegionId, destinationShardId)] = new PendingAckTarget(route.Pid, route.ActorReference);

                deliveredBatches++;
                deliveredContribs += (uint)entry.Value.Contribs.Count;
            }

            _pendingOutboxes.Remove(tickId);
        }

        if (inputContribs is not null && inputContribs.Count > 0)
        {
            inputRoutes = _routingTable.Entries
                .Where(entry => entry.ShardId.RegionId == NbnConstants.InputRegionId)
                .ToArray();

            if (inputRoutes.Length > 0)
            {
                foreach (var route in inputRoutes)
                {
                    var signalBatch = new SignalBatch
                    {
                        BrainId = _brainIdProto,
                        RegionId = (uint)NbnConstants.InputRegionId,
                        ShardId = route.ShardId.ToProtoShardId32(),
                        TickId = tickId
                    };

                    signalBatch.Contribs.AddRange(inputContribs);
                    context.Request(route.Pid, signalBatch);
                    expectedAckTargets[new PendingAckKey((uint)NbnConstants.InputRegionId, route.ShardId)] = new PendingAckTarget(route.Pid, route.ActorReference);

                    deliveredBatches++;
                    deliveredContribs += (uint)inputContribs.Count;
                }
            }
        }

        var expectedAcks = expectedAckTargets.Count;

        if (LogDelivery)
        {
            Log($"TickDeliver start tick={tickId} outboxDestinations={outboxDestinationCount} inputRoutes={inputRoutes.Length} deliveredBatches={deliveredBatches} deliveredContribs={deliveredContribs} expectedAcks={expectedAcks} routes={_routingTable.Count} fallbackRoutes={fallbackRoutes}");
        }

        if (expectedAcks == 0)
        {
            var deliverDone = new TickDeliverDone
            {
                TickId = tickId,
                BrainId = _brainIdProto,
                DeliverMs = 0,
                DeliveredBatches = 0,
                DeliveredContribs = 0
            };

            if (replyTo is not null)
            {
                context.Request(replyTo, deliverDone);
            }
            else if (LogDelivery)
            {
                Log($"TickDeliver done dropped (no reply target) tick={tickId}");
            }
            return;
        }

        stopwatch ??= Stopwatch.StartNew();
        var pending = new PendingDeliver(
            tickId,
            replyTo,
            stopwatch,
            deliveredBatches,
            deliveredContribs,
            expectedAckTargets);

        _pendingDeliveries[tickId] = pending;
    }

    private void ExpirePendingInputDrains(ulong currentTickId)
    {
        if (_pendingInputDrains.Count == 0)
        {
            return;
        }

        List<ulong>? expired = null;
        foreach (var entry in _pendingInputDrains)
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
            _pendingInputDrains.Remove(tickId);
            _pendingOutboxes.Remove(tickId);
        }

        if (LogDelivery)
        {
            Log($"Pending input drains expired before tick={currentTickId}. expired={string.Join(",", expired)} ioGateway={PidLabel(_ioGatewayPid)}");
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

    private bool ShouldDrainInputs()
        => _ioGatewayPid is not null
           && _routingTable.Entries.Any(entry => entry.ShardId.RegionId == NbnConstants.InputRegionId);

    private void CaptureIoGateway(PID? sender)
    {
        if (sender is null)
        {
            return;
        }

        _ioGatewayPid = sender;
    }
    private bool TryGetFallbackRoute(uint regionId, out ShardRoute route)
    {
        route = null!;
        ShardRoute? candidate = null;

        foreach (var entry in _routingTable.Entries)
        {
            if (entry.ShardId.RegionId != (int)regionId)
            {
                continue;
            }

            if (candidate is not null)
            {
                route = null!;
                return false;
            }

            candidate = entry;
        }

        if (candidate is null)
        {
            route = null!;
            return false;
        }

        route = candidate;
        return true;
    }

    private PID? TryGetPreferredLocalRoutePid(IContext context, ShardRoute route)
    {
        if (IsLocalPid(route.Pid))
        {
            return route.Pid;
        }

        if (!_routingTable.TryGetRoute(route.ShardId, out var existingRoute)
            || existingRoute is null
            || !IsLocalPid(existingRoute.Pid))
        {
            return TryBuildLocalizedLocalPidForLocalSystem(context.System.Address, route);
        }

        if (RouteTargetsSameLocalActor(route, existingRoute.Pid))
        {
            return existingRoute.Pid;
        }

        return TryBuildLocalizedLocalPidForLocalSystem(context.System.Address, route);
    }

    private static bool IsLocalPid(PID? pid)
        => pid is not null
           && (string.IsNullOrWhiteSpace(pid.Address)
               || string.Equals(pid.Address, "nonhost", StringComparison.OrdinalIgnoreCase));

    private static bool RouteTargetsSameLocalActor(ShardRoute route, PID localPid)
    {
        if (route.Pid is not null
            && ActorIdsEquivalent(route.Pid.Id, localPid.Id)
            && IsLocalAddress(route.Pid.Address))
        {
            return true;
        }

        if (string.IsNullOrWhiteSpace(route.ActorReference))
        {
            return false;
        }

        if (RoutablePidReference.TryDecode(route.ActorReference, out var endpointSet))
        {
            if (!ActorIdsEquivalent(endpointSet.ActorName, localPid.Id))
            {
                return false;
            }

            return endpointSet.Candidates.Any(static candidate =>
                TryParseEndpoint(candidate.HostPort, out var host, out _)
                && NetworkAddressDefaults.IsLocalHost(host));
        }

        return RoutablePidReference.TryParsePlainPid(route.ActorReference, out var parsed)
               && ActorIdsEquivalent(parsed.Id, localPid.Id)
               && IsLocalAddress(parsed.Address);
    }

    private static PID? TryBuildLocalizedLocalPid(ShardRoute route)
        => TryBuildLocalizedLocalPidForLocalSystem(localSystemAddress: null, route);

    private static PID? TryBuildLocalizedLocalPidForLocalSystem(string? localSystemAddress, ShardRoute route)
    {
        var actorId = ResolveLocalActorId(localSystemAddress, route);
        if (string.IsNullOrWhiteSpace(actorId))
        {
            return null;
        }

        return new PID("nonhost", actorId);
    }

    private static string ResolveLocalActorId(string? localSystemAddress, ShardRoute route)
    {
        if (route.Pid is not null && IsSameProcessLocalAddress(localSystemAddress, route.Pid.Address))
        {
            return route.Pid.Id;
        }

        if (string.IsNullOrWhiteSpace(route.ActorReference))
        {
            return string.Empty;
        }

        if (RoutablePidReference.TryDecode(route.ActorReference, out var endpointSet))
        {
            return endpointSet.Candidates.Any(candidate =>
                    IsSameProcessLocalHostPort(localSystemAddress, candidate.HostPort))
                ? endpointSet.ActorName
                : string.Empty;
        }

        return RoutablePidReference.TryParsePlainPid(route.ActorReference, out var parsed)
               && IsSameProcessLocalAddress(localSystemAddress, parsed.Address)
            ? parsed.Id
            : string.Empty;
    }

    private static bool IsSameProcessLocalHostPort(string? localSystemAddress, string? hostPort)
    {
        if (string.IsNullOrWhiteSpace(hostPort))
        {
            return false;
        }

        var normalizedHostPort = hostPort.Trim();
        var slashIndex = normalizedHostPort.IndexOf('/');
        if (slashIndex >= 0)
        {
            normalizedHostPort = normalizedHostPort[..slashIndex];
        }

        return IsSameProcessLocalAddress(localSystemAddress, normalizedHostPort);
    }

    private static bool IsSameProcessLocalAddress(string? localSystemAddress, string? address)
    {
        if (string.IsNullOrWhiteSpace(address))
        {
            return true;
        }

        if (string.IsNullOrWhiteSpace(localSystemAddress))
        {
            return IsLocalAddress(address);
        }

        if (!TryParseEndpoint(localSystemAddress, out _, out var localPort)
            || !TryParseEndpoint(address, out var host, out var routePort))
        {
            return false;
        }

        return localPort == routePort && NetworkAddressDefaults.IsLocalHost(host);
    }

    private static bool IsLocalAddress(string? address)
    {
        if (string.IsNullOrWhiteSpace(address))
        {
            return true;
        }

        return TryParseEndpoint(address, out var host, out _)
               && NetworkAddressDefaults.IsLocalHost(host);
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

    private static bool SenderMatchesPid(PID? sender, PID expected)
    {
        if (sender is null)
        {
            return false;
        }

        return expected.Equals(sender)
               || PidEquals(sender, expected)
               || PidHasEquivalentEndpoint(sender, expected);
    }

    private static bool SenderMatchesActorReferenceOrPid(PID? sender, string? actorReference, PID expected)
    {
        if (SenderMatchesPid(sender, expected))
        {
            return true;
        }

        return SenderMatchesActorReference(sender, actorReference);
    }

    private static bool SenderMatchesActorReference(PID? sender, string? actorReference)
    {
        if (sender is null || string.IsNullOrWhiteSpace(actorReference))
        {
            return false;
        }

        if (RoutablePidReference.TryDecode(actorReference, out var endpointSet))
        {
            foreach (var candidate in endpointSet.Candidates)
            {
                if (SenderMatchesPid(sender, candidate.ToEndpoint().ToPid()))
                {
                    return true;
                }
            }

            return false;
        }

        return RoutablePidReference.TryParsePlainPid(actorReference, out var parsed)
               && SenderMatchesPid(sender, parsed);
    }

    private static bool PidEquals(PID? left, PID right)
        => left is not null
           && string.Equals(left.Address ?? string.Empty, right.Address ?? string.Empty, StringComparison.OrdinalIgnoreCase)
           && string.Equals(left.Id ?? string.Empty, right.Id ?? string.Empty, StringComparison.Ordinal);
    
    private static bool PidHasEquivalentEndpoint(PID sender, PID expected)
    {
        if (!ActorIdsEquivalent(sender.Id, expected.Id))
        {
            return false;
        }

        if (HasEquivalentProcessLocalEndpoint(sender.Address, expected.Address)
            || HasEquivalentProcessLocalEndpoint(expected.Address, sender.Address))
        {
            return true;
        }

        if (!TryParseEndpoint(sender.Address, out var senderHost, out var senderPort)
            || !TryParseEndpoint(expected.Address, out var expectedHost, out var expectedPort))
        {
            return false;
        }

        if (senderPort != expectedPort)
        {
            return false;
        }

        if (string.Equals(senderHost, expectedHost, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var senderClass = ClassifyEndpointHost(senderHost);
        var expectedClass = ClassifyEndpointHost(expectedHost);
        if (senderClass == EndpointHostClass.Loopback && expectedClass == EndpointHostClass.Loopback)
        {
            return true;
        }

        if ((senderClass == EndpointHostClass.Wildcard && expectedClass == EndpointHostClass.Loopback)
            || (senderClass == EndpointHostClass.Loopback && expectedClass == EndpointHostClass.Wildcard))
        {
            return true;
        }

        return HostsResolveToSameAddress(senderHost, expectedHost);
    }

    private static bool HasEquivalentProcessLocalEndpoint(string? address, string? otherAddress)
    {
        if (!string.IsNullOrWhiteSpace(address)
            && !string.Equals(address, "nonhost", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return TryParseEndpoint(otherAddress, out var otherHost, out _)
               && NetworkAddressDefaults.IsLocalHost(otherHost);
    }

    private static bool TryParseEndpoint(string? address, out string host, out int port)
    {
        host = string.Empty;
        port = 0;
        if (string.IsNullOrWhiteSpace(address))
        {
            return false;
        }

        var trimmed = address.Trim();
        if (trimmed.Length == 0)
        {
            return false;
        }

        if (trimmed[0] == '[')
        {
            var closingBracket = trimmed.IndexOf(']');
            if (closingBracket <= 1 || closingBracket >= trimmed.Length - 1 || trimmed[closingBracket + 1] != ':')
            {
                return false;
            }

            var bracketHost = trimmed[1..closingBracket];
            var bracketPort = trimmed[(closingBracket + 2)..];
            if (!int.TryParse(bracketPort, NumberStyles.None, CultureInfo.InvariantCulture, out port) || port <= 0)
            {
                return false;
            }

            host = bracketHost;
            return !string.IsNullOrWhiteSpace(host);
        }

        var separator = trimmed.LastIndexOf(':');
        if (separator <= 0 || separator >= trimmed.Length - 1)
        {
            return false;
        }

        var hostToken = trimmed[..separator];
        var portToken = trimmed[(separator + 1)..];
        if (!int.TryParse(portToken, NumberStyles.None, CultureInfo.InvariantCulture, out port) || port <= 0)
        {
            return false;
        }

        host = hostToken;
        return !string.IsNullOrWhiteSpace(host);
    }

    private static EndpointHostClass ClassifyEndpointHost(string host)
    {
        var normalized = host.Trim();
        if (normalized.StartsWith("[", StringComparison.Ordinal) && normalized.EndsWith("]", StringComparison.Ordinal))
        {
            normalized = normalized[1..^1];
        }

        if (normalized.Equals("localhost", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("::1", StringComparison.OrdinalIgnoreCase))
        {
            return EndpointHostClass.Loopback;
        }

        if (normalized.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("::", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("*", StringComparison.Ordinal)
            || normalized.Equals("+", StringComparison.Ordinal))
        {
            return EndpointHostClass.Wildcard;
        }

        return EndpointHostClass.Other;
    }

    private static bool HostsResolveToSameAddress(string senderHost, string expectedHost)
    {
        var senderAddresses = ResolveEndpointHostAddresses(senderHost);
        if (senderAddresses.Count == 0)
        {
            return false;
        }

        var expectedAddresses = ResolveEndpointHostAddresses(expectedHost);
        if (expectedAddresses.Count == 0)
        {
            return false;
        }

        foreach (var senderAddress in senderAddresses)
        {
            if (expectedAddresses.Contains(senderAddress, StringComparer.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static IReadOnlyList<string> ResolveEndpointHostAddresses(string host)
    {
        var normalized = host.Trim();
        if (normalized.StartsWith("[", StringComparison.Ordinal) && normalized.EndsWith("]", StringComparison.Ordinal))
        {
            normalized = normalized[1..^1];
        }

        if (normalized.Length == 0)
        {
            return Array.Empty<string>();
        }

        if (IPAddress.TryParse(normalized, out var parsed))
        {
            return [NormalizeComparableAddress(parsed)];
        }

        try
        {
            return Dns.GetHostAddresses(normalized)
                .Select(NormalizeComparableAddress)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
        catch
        {
            return Array.Empty<string>();
        }
    }

    private static string NormalizeComparableAddress(IPAddress address)
    {
        if (address.IsIPv4MappedToIPv6)
        {
            address = address.MapToIPv4();
        }

        return address.ToString();
    }

    private static bool ActorIdsEquivalent(string? left, string? right)
    {
        var normalizedLeft = NormalizeActorId(left);
        var normalizedRight = NormalizeActorId(right);
        if (string.Equals(normalizedLeft, normalizedRight, StringComparison.Ordinal))
        {
            return true;
        }

        if (normalizedLeft.Length == 0 || normalizedRight.Length == 0)
        {
            return false;
        }

        return normalizedLeft.EndsWith("/" + normalizedRight, StringComparison.Ordinal)
               || normalizedRight.EndsWith("/" + normalizedLeft, StringComparison.Ordinal);
    }

    private static string NormalizeActorId(string? actorId)
    {
        if (string.IsNullOrWhiteSpace(actorId))
        {
            return string.Empty;
        }

        return string.Join(
            "/",
            actorId
                .Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Where(static segment => !segment.StartsWith("$", StringComparison.Ordinal)));
    }

    private void EmitSignalBatchAckIgnored(
        IContext context,
        SignalBatchAck ack,
        string reason,
        PID? expectedSender = null)
    {
        var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
        var expectedLabel = expectedSender is null ? string.Empty : $" expectedSender={PidLabel(expectedSender)}";
        var shardLabel = ack.ShardId is null ? "<missing>" : ack.ShardId.ToShardId32().ToString();
        Log(
            $"SignalBatchAck ignored tick={ack.TickId} reason={reason} region={ack.RegionId} shard={shardLabel} sender={senderLabel}{expectedLabel}");
    }

    private static string PidLabel(PID? pid)
        => pid is null
            ? "<missing>"
            : string.IsNullOrWhiteSpace(pid.Address)
                ? pid.Id
                : $"{pid.Address}/{pid.Id}";

    private enum EndpointHostClass
    {
        Loopback,
        Wildcard,
        Other
    }

    private static void ForwardToParent(IContext context, object message)
    {
        if (context.Parent is null)
        {
            return;
        }

        context.Request(context.Parent, message);
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
            Dictionary<PendingAckKey, PendingAckTarget> pendingAckTargets)
        {
            TickId = tickId;
            ReplyTo = replyTo;
            Stopwatch = stopwatch;
            DeliveredBatches = deliveredBatches;
            DeliveredContribs = deliveredContribs;
            PendingAckTargets = pendingAckTargets;
        }

        public ulong TickId { get; }
        public PID? ReplyTo { get; }
        public Stopwatch Stopwatch { get; }
        public uint DeliveredBatches { get; }
        public uint DeliveredContribs { get; }
        public Dictionary<PendingAckKey, PendingAckTarget> PendingAckTargets { get; }
        public HashSet<PendingAckKey> AckedKeys { get; } = new();
    }

    private readonly record struct PendingAckKey(uint RegionId, ShardId32 ShardId);
    private readonly record struct PendingAckTarget(PID SenderPid, string ActorReference);

    private sealed class PendingInputDrain
    {
        public PendingInputDrain(ulong tickId, PID? replyTo, Stopwatch stopwatch)
        {
            TickId = tickId;
            ReplyTo = replyTo;
            Stopwatch = stopwatch;
        }

        public ulong TickId { get; }
        public PID? ReplyTo { get; }
        public Stopwatch Stopwatch { get; }
    }

    private static void Log(string message)
        => Console.WriteLine($"[{DateTime.UtcNow:O}] [BrainSignalRouter] {message}");

    private void LogInput(string message)
        => Console.WriteLine($"[{DateTime.UtcNow:O}] [BrainSignalRouterInput] brain={_brainId:D} {message}");

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
