using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.IO;

public sealed partial class IoGatewayActor
{
    private async Task ForwardInputAsync(IContext context, object message)
    {
        if (!TryGetBrainId(message, out var brainId))
        {
            return;
        }

        var hadBrainEntry = TryGetBrainEntry(message, out var entry);
        if (!hadBrainEntry)
        {
            entry = await EnsureBrainEntryAsync(context, brainId).ConfigureAwait(false);
        }

        if (entry is not null)
        {
            TrackInputState(entry, message);
            await DispatchCoordinatorMessageAsync(context, entry.InputPid, message).ConfigureAwait(false);
        }

        if (_hiveMindPid is null)
        {
            return;
        }

        _routerRegistration.TryGetValue(brainId, out var registeredRouterBeforeForward);
        var routerPid = await ResolveRouterPidAsync(context, brainId, allowCached: false).ConfigureAwait(false);
        if (hadBrainEntry
            && routerPid is not null
            && string.Equals(registeredRouterBeforeForward, PidLabel(routerPid), StringComparison.Ordinal))
        {
            RegisterIoGatewayPid(context, brainId, routerPid, force: true);
        }

        if (LogInputDiagnostics)
        {
            Console.WriteLine(
                $"[IoGatewayInput] forward type={message.GetType().Name} brain={brainId} input={PidLabel(entry?.InputPid)} router={PidLabel(routerPid)} payload={DescribeInputPayload(message)}");
        }

        if (routerPid is null)
        {
            return;
        }

        context.Send(routerPid, message);
    }

    private async Task ForwardRuntimeNeuronAsync(IContext context, object message)
    {
        if (!TryGetBrainId(message, out var brainId))
        {
            return;
        }

        var routerPid = await ResolveRouterPidAsync(context, brainId, allowCached: false).ConfigureAwait(false);
        if (routerPid is null)
        {
            return;
        }

        context.Send(routerPid, message);
    }

    private async Task ForwardOutputAsync(IContext context, object message)
    {
        if (!TryGetBrainId(message, out var brainId))
        {
            RespondOutputCommandAck(context, message, Guid.Empty.ToProtoUuid(), success: false, "brain_id_invalid");
            return;
        }

        if (!TryGetBrainEntry(message, out var entry))
        {
            TrackPendingOutputSubscription(context, brainId, message);
            entry = await EnsureBrainEntryAsync(context, brainId).ConfigureAwait(false);
        }

        if (entry is null)
        {
            if (LogOutput)
            {
                Console.WriteLine($"IoGateway output forward dropped type={message.GetType().Name} sender={PidLabel(context.Sender)} reason=brain_entry_missing.");
            }

            RespondOutputCommandAck(context, message, brainId.ToProtoUuid(), success: true, "queued");
            return;
        }

        var replayedPendingSubscriptions = MergePendingOutputSubscriptions(entry);
        TrackOutputSubscription(context, entry, message);

        if (LogOutput)
        {
            var subscriberActor = message switch
            {
                SubscribeOutputs subscribe => subscribe.SubscriberActor,
                UnsubscribeOutputs unsubscribe => unsubscribe.SubscriberActor,
                SubscribeOutputsVector subscribeVector => subscribeVector.SubscriberActor,
                UnsubscribeOutputsVector unsubscribeVector => unsubscribeVector.SubscriberActor,
                _ => string.Empty
            };
            Console.WriteLine(
                $"IoGateway output forward type={message.GetType().Name} sender={PidLabel(context.Sender)} subscriber={subscriberActor} target={PidLabel(entry.OutputPid)}.");
        }

        try
        {
            if (replayedPendingSubscriptions)
            {
                await ReplayOutputSubscriptionsAsync(context, entry).ConfigureAwait(false);
            }

            var ack = await DispatchCoordinatorMessageAsync(
                    context,
                    entry.OutputPid,
                    NormalizeOutputSubscriptionMessage(context, message))
                .ConfigureAwait(false);
            if (context.Sender is not null)
            {
                context.Respond(ack);
            }
        }
        catch (Exception ex)
        {
            if (LogOutput)
            {
                Console.WriteLine(
                    $"IoGateway output forward failed type={message.GetType().Name} brain={brainId} sender={PidLabel(context.Sender)} error={ex.GetBaseException().Message}.");
            }

            RespondOutputCommandAck(context, message, brainId.ToProtoUuid(), success: false, ex.GetBaseException().Message);
        }
    }

    private async Task HandleDrainInputsAsync(IContext context, DrainInputs message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            context.Respond(new InputDrain
            {
                BrainId = message.BrainId,
                TickId = message.TickId
            });
            return;
        }

        if (!_brains.TryGetValue(brainId, out var entry))
        {
            entry = await EnsureBrainEntryAsync(context, brainId).ConfigureAwait(false);
            if (entry is null)
            {
                context.Respond(new InputDrain
                {
                    BrainId = message.BrainId,
                    TickId = message.TickId
                });
                return;
            }
        }

        try
        {
            if (LogInputTraceDiagnostics)
            {
                Console.WriteLine(
                    $"[IoGatewayInput] drain request brain={brainId} tick={message.TickId} input={PidLabel(entry.InputPid)}");
            }

            var drain = await context.RequestAsync<InputDrain>(entry.InputPid, message, DefaultRequestTimeout);
            entry.InputState.ApplyDrain(drain);
            if (LogInputTraceDiagnostics || drain.Contribs.Count > 0)
            {
                Console.WriteLine(
                    $"[IoGatewayInput] drain response brain={brainId} tick={message.TickId} contribs={drain.Contribs.Count} input={PidLabel(entry.InputPid)}");
            }

            context.Respond(drain);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"DrainInputs failed for {brainId}: {ex.Message}");
            context.Respond(new InputDrain
            {
                BrainId = message.BrainId,
                TickId = message.TickId
            });
        }
    }

    private async Task EnsureIoGatewayRegisteredAsync(IContext context, Guid brainId)
    {
        if (_hiveMindPid is null)
        {
            return;
        }

        try
        {
            await ResolveRouterPidAsync(context, brainId, allowCached: false).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"RegisterIoGateway failed for {brainId}: {ex.Message}");
        }
    }

    private async Task<BrainIoEntry?> EnsureBrainEntryAsync(IContext context, Guid brainId, TimeSpan? requestTimeout = null)
    {
        if (_brains.TryGetValue(brainId, out var existing))
        {
            return existing;
        }

        if (_hiveMindPid is null)
        {
            return null;
        }

        try
        {
            var info = await context.RequestAsync<ProtoControl.BrainIoInfo>(
                    _hiveMindPid,
                    new ProtoControl.GetBrainIoInfo { BrainId = brainId.ToProtoUuid() },
                    requestTimeout ?? DefaultRequestTimeout)
                .ConfigureAwait(false);

            if (info is null || info.InputWidth == 0)
            {
                return null;
            }

            var register = new RegisterBrain
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = info.InputWidth,
                OutputWidth = info.OutputWidth,
                InputCoordinatorMode = info.InputCoordinatorMode,
                OutputVectorSource = info.OutputVectorSource,
                InputCoordinatorPid = info.InputCoordinatorPid,
                OutputCoordinatorPid = info.OutputCoordinatorPid,
                IoGatewayOwnsInputCoordinator = info.IoGatewayOwnsInputCoordinator,
                IoGatewayOwnsOutputCoordinator = info.IoGatewayOwnsOutputCoordinator
            };
            await RegisterBrainAsync(context, register);

            if (_brains.TryGetValue(brainId, out var entry))
            {
                return entry;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"EnsureBrainEntry failed for {brainId}: {ex.Message}");
        }

        return null;
    }

    private async Task EnsureOutputSinkRegisteredAsync(
        IContext context,
        Guid brainId,
        PID outputPid,
        bool shouldRegisterOutputSink)
    {
        if (!shouldRegisterOutputSink)
        {
            return;
        }

        if (_hiveMindPid is null)
        {
            Console.WriteLine($"RegisterOutputSink skipped (no HiveMind PID) for {brainId}");
            return;
        }

        try
        {
            var outputLabel = PidLabel(ToRemotePid(context, outputPid));
            context.Request(_hiveMindPid, new ProtoControl.RegisterOutputSink
            {
                BrainId = brainId.ToProtoUuid(),
                OutputPid = outputLabel
            });

            Console.WriteLine($"RegisterOutputSink sent for {brainId} -> {outputLabel}");
            await Task.CompletedTask;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"RegisterOutputSink failed for {brainId}: {ex.Message}");
        }
    }

    private bool TryGetBrainEntry(object message, out BrainIoEntry entry)
    {
        entry = null!;

        switch (message)
        {
            case InputWrite inputWrite when TryGetBrainId(inputWrite.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case InputVector inputVector when TryGetBrainId(inputVector.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case SubscribeOutputs subscribe when TryGetBrainId(subscribe.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case UnsubscribeOutputs unsubscribe when TryGetBrainId(unsubscribe.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case SubscribeOutputsVector subscribeVector when TryGetBrainId(subscribeVector.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case UnsubscribeOutputsVector unsubscribeVector when TryGetBrainId(unsubscribeVector.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case EnergyCredit energyCredit when TryGetBrainId(energyCredit.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case EnergyRate energyRate when TryGetBrainId(energyRate.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case SetCostEnergyEnabled costEnergy when TryGetBrainId(costEnergy.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case SetPlasticityEnabled plasticity when TryGetBrainId(plasticity.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case SetHomeostasisEnabled homeostasis when TryGetBrainId(homeostasis.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
        }

        return false;
    }

    private static bool TryGetBrainId(object message, out Guid guid)
    {
        guid = Guid.Empty;

        switch (message)
        {
            case InputWrite inputWrite:
                return TryGetBrainId(inputWrite.BrainId, out guid);
            case InputVector inputVector:
                return TryGetBrainId(inputVector.BrainId, out guid);
            case SubscribeOutputs subscribe:
                return TryGetBrainId(subscribe.BrainId, out guid);
            case UnsubscribeOutputs unsubscribe:
                return TryGetBrainId(unsubscribe.BrainId, out guid);
            case SubscribeOutputsVector subscribeVector:
                return TryGetBrainId(subscribeVector.BrainId, out guid);
            case UnsubscribeOutputsVector unsubscribeVector:
                return TryGetBrainId(unsubscribeVector.BrainId, out guid);
            case EnergyCredit energyCredit:
                return TryGetBrainId(energyCredit.BrainId, out guid);
            case EnergyRate energyRate:
                return TryGetBrainId(energyRate.BrainId, out guid);
            case SetCostEnergyEnabled costEnergy:
                return TryGetBrainId(costEnergy.BrainId, out guid);
            case SetPlasticityEnabled plasticity:
                return TryGetBrainId(plasticity.BrainId, out guid);
            case SetHomeostasisEnabled homeostasis:
                return TryGetBrainId(homeostasis.BrainId, out guid);
            case RuntimeNeuronPulse pulse:
                return TryGetBrainId(pulse.BrainId, out guid);
            case RuntimeNeuronStateWrite stateWrite:
                return TryGetBrainId(stateWrite.BrainId, out guid);
        }

        return false;
    }

    private static bool TryGetBrainId(Uuid? brainId, out Guid guid)
    {
        if (brainId is null)
        {
            guid = Guid.Empty;
            return false;
        }

        return brainId.TryToGuid(out guid);
    }

    private static void TrackInputState(BrainIoEntry entry, object message)
    {
        switch (message)
        {
            case InputWrite inputWrite:
                entry.InputState.Apply(inputWrite);
                break;
            case InputVector inputVector:
                entry.InputState.Apply(inputVector);
                break;
        }
    }

    private static void TrackOutputSubscription(IContext context, BrainIoEntry entry, object message)
    {
        switch (message)
        {
            case SubscribeOutputs subscribe:
                AddSubscriber(context, subscribe.SubscriberActor, entry.OutputSubscribers);
                break;
            case UnsubscribeOutputs unsubscribe:
                RemoveSubscriber(context, unsubscribe.SubscriberActor, entry.OutputSubscribers);
                break;
            case SubscribeOutputsVector subscribeVector:
                AddSubscriber(context, subscribeVector.SubscriberActor, entry.OutputVectorSubscribers);
                break;
            case UnsubscribeOutputsVector unsubscribeVector:
                RemoveSubscriber(context, unsubscribeVector.SubscriberActor, entry.OutputVectorSubscribers);
                break;
        }
    }

    private void TrackPendingOutputSubscription(IContext context, Guid brainId, object message)
    {
        switch (message)
        {
            case SubscribeOutputs subscribe:
                AddSubscriber(context, subscribe.SubscriberActor, GetPendingSubscriberSet(brainId, vector: false));
                break;
            case UnsubscribeOutputs unsubscribe:
                RemovePendingSubscriber(context, unsubscribe.SubscriberActor, brainId, vector: false);
                break;
            case SubscribeOutputsVector subscribeVector:
                AddSubscriber(context, subscribeVector.SubscriberActor, GetPendingSubscriberSet(brainId, vector: true));
                break;
            case UnsubscribeOutputsVector unsubscribeVector:
                RemovePendingSubscriber(context, unsubscribeVector.SubscriberActor, brainId, vector: true);
                break;
        }
    }

    private bool MergePendingOutputSubscriptions(BrainIoEntry entry)
    {
        var merged = false;

        if (_pendingOutputSubscribers.Remove(entry.BrainId, out var pendingSingles))
        {
            foreach (var subscriber in pendingSingles)
            {
                entry.OutputSubscribers[subscriber.Key] = subscriber.Value;
            }

            merged = pendingSingles.Count > 0;
        }

        if (_pendingOutputVectorSubscribers.Remove(entry.BrainId, out var pendingVectors))
        {
            foreach (var subscriber in pendingVectors)
            {
                entry.OutputVectorSubscribers[subscriber.Key] = subscriber.Value;
            }

            merged = merged || pendingVectors.Count > 0;
        }

        return merged;
    }

    private static void AddSubscriber(IContext context, string? subscriberActor, Dictionary<string, PID> set)
    {
        var subscriber = ResolveSubscriberPid(context, subscriberActor);
        if (subscriber is null)
        {
            return;
        }

        set[PidKey(subscriber)] = subscriber;
    }

    private static void RemoveSubscriber(IContext context, string? subscriberActor, Dictionary<string, PID> set)
    {
        var subscriber = ResolveSubscriberPid(context, subscriberActor);
        if (subscriber is null)
        {
            return;
        }

        set.Remove(PidKey(subscriber));
    }

    private Dictionary<string, PID> GetPendingSubscriberSet(Guid brainId, bool vector)
    {
        var source = vector ? _pendingOutputVectorSubscribers : _pendingOutputSubscribers;
        if (!source.TryGetValue(brainId, out var set))
        {
            set = new Dictionary<string, PID>(StringComparer.Ordinal);
            source.Add(brainId, set);
        }

        return set;
    }

    private void RemovePendingSubscriber(IContext context, string? subscriberActor, Guid brainId, bool vector)
    {
        var source = vector ? _pendingOutputVectorSubscribers : _pendingOutputSubscribers;
        if (!source.TryGetValue(brainId, out var set))
        {
            return;
        }

        RemoveSubscriber(context, subscriberActor, set);
        if (set.Count == 0)
        {
            source.Remove(brainId);
        }
    }

    private static PID? ResolveSubscriberPid(IContext context, string? subscriberActor)
    {
        if (TryParsePid(subscriberActor, out var parsed) && parsed is not null)
        {
            return ToRemotePid(context, parsed);
        }

        return context.Sender is null ? null : ToRemotePid(context, context.Sender);
    }

    private static object NormalizeOutputSubscriptionMessage(IContext context, object message)
    {
        return message switch
        {
            SubscribeOutputs subscribe => new SubscribeOutputs
            {
                BrainId = subscribe.BrainId,
                SubscriberActor = ResolveSubscriberActorLabel(context, subscribe.SubscriberActor)
            },
            UnsubscribeOutputs unsubscribe => new UnsubscribeOutputs
            {
                BrainId = unsubscribe.BrainId,
                SubscriberActor = ResolveSubscriberActorLabel(context, unsubscribe.SubscriberActor)
            },
            SubscribeOutputsVector subscribeVector => new SubscribeOutputsVector
            {
                BrainId = subscribeVector.BrainId,
                SubscriberActor = ResolveSubscriberActorLabel(context, subscribeVector.SubscriberActor)
            },
            UnsubscribeOutputsVector unsubscribeVector => new UnsubscribeOutputsVector
            {
                BrainId = unsubscribeVector.BrainId,
                SubscriberActor = ResolveSubscriberActorLabel(context, unsubscribeVector.SubscriberActor)
            },
            _ => message
        };
    }

    private static string ResolveSubscriberActorLabel(IContext context, string? subscriberActor)
    {
        if (!string.IsNullOrWhiteSpace(subscriberActor))
        {
            return subscriberActor;
        }

        var subscriberPid = ResolveSubscriberPid(context, subscriberActor);
        return subscriberPid is null ? string.Empty : PidLabel(subscriberPid);
    }

    private static bool UpdateCoordinatorReference(
        IContext context,
        PID currentPid,
        bool currentlyOwned,
        PID requestedPid,
        bool requestedOwned,
        out PID updatedPid,
        out bool updatedOwned)
    {
        updatedPid = currentPid;
        updatedOwned = currentlyOwned;
        if (PidEquals(currentPid, requestedPid) && currentlyOwned == requestedOwned)
        {
            return false;
        }

        if (currentlyOwned && !PidEquals(currentPid, requestedPid))
        {
            context.Stop(currentPid);
        }

        updatedPid = requestedPid;
        updatedOwned = requestedOwned;
        return true;
    }

    private static async Task ReplayInputStateAsync(IContext context, BrainIoEntry entry)
    {
        await DispatchCoordinatorMessageAsync(
                context,
                entry.InputPid,
                new UpdateInputCoordinatorMode(entry.InputCoordinatorMode))
            .ConfigureAwait(false);
        await entry.InputState
            .ReplayToAsync(message => DispatchCoordinatorMessageAsync(context, entry.InputPid, message))
            .ConfigureAwait(false);
    }

    private static async Task ReplayOutputSubscriptionsAsync(IContext context, BrainIoEntry entry)
    {
        foreach (var subscriber in entry.OutputSubscribers.Values)
        {
            await DispatchCoordinatorMessageAsync(context, entry.OutputPid, new SubscribeOutputs
            {
                BrainId = entry.BrainId.ToProtoUuid(),
                SubscriberActor = PidLabel(subscriber)
            }).ConfigureAwait(false);
        }

        foreach (var subscriber in entry.OutputVectorSubscribers.Values)
        {
            await DispatchCoordinatorMessageAsync(context, entry.OutputPid, new SubscribeOutputsVector
            {
                BrainId = entry.BrainId.ToProtoUuid(),
                SubscriberActor = PidLabel(subscriber)
            }).ConfigureAwait(false);
        }
    }

    private static async Task<IoCommandAck> DispatchCoordinatorMessageAsync(IContext context, PID target, object message)
    {
        return await context.RequestAsync<IoCommandAck>(target, message, DefaultRequestTimeout).ConfigureAwait(false);
    }

    private static void RespondOutputCommandAck(IContext context, object message, Uuid? brainId, bool success, string ackMessage)
    {
        var command = message switch
        {
            SubscribeOutputs => "subscribe_outputs",
            UnsubscribeOutputs => "unsubscribe_outputs",
            SubscribeOutputsVector => "subscribe_outputs_vector",
            UnsubscribeOutputsVector => "unsubscribe_outputs_vector",
            _ => "output_command"
        };

        RespondCommandAck(context, brainId, command, success, ackMessage);
    }

    private async Task<PID?> ResolveRouterPidAsync(IContext context, Guid brainId, bool allowCached = true)
    {
        _routerCache.TryGetValue(brainId, out var cached);
        if (allowCached && cached is not null)
        {
            if (LogInputTraceDiagnostics)
            {
                Console.WriteLine($"[IoGatewayInput] router cached brain={brainId} router={PidLabel(cached)}");
            }

            return cached;
        }

        if (_hiveMindPid is null)
        {
            return null;
        }

        try
        {
            var info = await context.RequestAsync<ProtoControl.BrainRoutingInfo>(
                    _hiveMindPid,
                    new ProtoControl.GetBrainRouting { BrainId = brainId.ToProtoUuid() },
                    DefaultRequestTimeout)
                .ConfigureAwait(false);

            if (info is null)
            {
                return cached;
            }

            if (TryParsePid(info.SignalRouterPid, out var routerPid) && routerPid is not null)
            {
                if (LogInputTraceDiagnostics || !PidEquals(cached, routerPid))
                {
                    Console.WriteLine(
                        $"[IoGatewayInput] router resolved brain={brainId} target=signal_router previous={PidLabel(cached)} current={PidLabel(routerPid)}");
                }

                if (!PidEquals(cached, routerPid))
                {
                    _routerRegistration.Remove(brainId);
                }

                _routerCache[brainId] = routerPid;
                RegisterIoGatewayPid(context, brainId, routerPid);
                return routerPid;
            }

            if (TryParsePid(info.BrainRootPid, out var rootPid) && rootPid is not null)
            {
                if (LogInputTraceDiagnostics || !PidEquals(cached, rootPid))
                {
                    Console.WriteLine(
                        $"[IoGatewayInput] router resolved brain={brainId} target=brain_root previous={PidLabel(cached)} current={PidLabel(rootPid)}");
                }

                if (!PidEquals(cached, rootPid))
                {
                    _routerRegistration.Remove(brainId);
                }

                _routerCache[brainId] = rootPid;
                RegisterIoGatewayPid(context, brainId, rootPid);
                return rootPid;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Resolve router failed: {ex.Message}");
        }

        return cached;
    }

    private static string DescribeInputPayload(object message)
    {
        return message switch
        {
            InputWrite inputWrite => $"index={inputWrite.InputIndex} value={inputWrite.Value:0.###}",
            InputVector inputVector => $"width={inputVector.Values.Count}",
            RuntimeNeuronPulse pulse => $"region={pulse.TargetRegionId} neuron={pulse.TargetNeuronId} value={pulse.Value:0.###}",
            RuntimeNeuronStateWrite stateWrite => $"region={stateWrite.TargetRegionId} neuron={stateWrite.TargetNeuronId} buffer={stateWrite.SetBuffer} accumulator={stateWrite.SetAccumulator}",
            _ => string.Empty
        };
    }

    private void RegisterIoGatewayPid(IContext context, Guid brainId, PID routerPid, bool force = false)
    {
        var routerLabel = PidLabel(routerPid);
        if (!force
            && _routerRegistration.TryGetValue(brainId, out var registered)
            && registered == routerLabel)
        {
            return;
        }

        var selfPid = ToRemotePid(context, context.Self);
        context.Send(routerPid, new RegisterIoGateway
        {
            BrainId = brainId.ToProtoUuid(),
            IoGatewayPid = PidLabel(selfPid)
        });

        _routerRegistration[brainId] = routerLabel;
    }
}
