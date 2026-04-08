using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Shared;
using Nbn.Shared.HiveMind;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.IO;

public sealed partial class IoGatewayActor
{
    private async Task ForwardInputAsync(IContext context, object message)
    {
        if (!TryGetBrainId(message, out var brainId))
        {
            RespondInputCommandAck(context, message, null, success: false, "brain_id_invalid");
            return;
        }

        var hadBrainEntry = TryGetBrainEntry(message, out var entry);
        if (!hadBrainEntry)
        {
            entry = await EnsureBrainEntryAsync(context, brainId).ConfigureAwait(false);
        }
        if (entry is null)
        {
            RespondInputCommandAck(context, message, brainId.ToProtoUuid(), success: false, "brain_io_unavailable");
            return;
        }

        var shouldRequestTickDrainBeforeForward = false;
        var inputAck = default(IoCommandAck);
        try
        {
            shouldRequestTickDrainBeforeForward = entry.InputState.ShouldRequestTickDrain;
            TrackInputState(entry, message);
            inputAck = await DispatchCoordinatorMessageAsync(context, entry.InputPid, message).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            RespondInputCommandAck(context, message, brainId.ToProtoUuid(), success: false, $"brain_input_request_failed:{ex.GetBaseException().Message}");
            return;
        }

        if (inputAck is null)
        {
            RespondInputCommandAck(context, message, brainId.ToProtoUuid(), success: false, "brain_input_empty_response");
            return;
        }

        if (!inputAck.Success)
        {
            context.Respond(inputAck);
            return;
        }

        if (_hiveMindPid is null)
        {
            RespondInputCommandAck(context, message, brainId.ToProtoUuid(), success: false, "hivemind_unavailable");
            return;
        }

        _routerRegistration.TryGetValue(brainId, out var registeredRouterBeforeForward);
        var routerPid = await ResolveRouterPidAsync(context, brainId, allowCached: false).ConfigureAwait(false);
        var registrationTargetLabel = routerPid is null
            ? string.Empty
            : PidLabel(ResolveRegistrationTarget(brainId, routerPid));
        if (hadBrainEntry
            && routerPid is not null
            && string.Equals(registeredRouterBeforeForward, registrationTargetLabel, StringComparison.Ordinal))
        {
            RegisterIoGatewayPid(context, brainId, routerPid, force: true);
        }
        else if (entry is not null
                 && routerPid is not null
                 && shouldRequestTickDrainBeforeForward != entry.InputState.ShouldRequestTickDrain)
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
            RespondInputCommandAck(context, message, brainId.ToProtoUuid(), success: false, "brain_router_unavailable");
            return;
        }

        context.Send(routerPid, message);
        if (context.Sender is not null)
        {
            context.Respond(inputAck);
        }
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

    private void ForwardRuntimeStateReset(IContext context, ResetBrainRuntimeState message)
    {
        if (!TryGetBrainId(message, out var brainId))
        {
            RespondCommandAck(context, message.BrainId, "reset_brain_runtime_state", success: false, "brain_id_invalid");
            return;
        }

        if (_hiveMindPid is null)
        {
            RespondCommandAck(context, message.BrainId, "reset_brain_runtime_state", success: false, "hivemind_unavailable");
            return;
        }

        var requestTask = context.RequestAsync<IoCommandAck>(
            _hiveMindPid,
            new RequestBrainRuntimeReset(brainId, message.ResetBuffer, message.ResetAccumulator),
            DefaultRequestTimeout);
        context.ReenterAfter(
            requestTask,
            completed =>
            {
                if (completed.IsCompletedSuccessfully)
                {
                    if (completed.Result is not null)
                    {
                        context.Respond(completed.Result);
                    }
                    else
                    {
                        RespondCommandAck(context, message.BrainId, "reset_brain_runtime_state", success: false, "hivemind_empty_response");
                    }

                    return Task.CompletedTask;
                }

                var detail = completed.Exception?.GetBaseException().Message ?? "request canceled";
                RespondCommandAck(
                    context,
                    message.BrainId,
                    "reset_brain_runtime_state",
                    success: false,
                    $"hivemind_request_failed:{detail}");
                return Task.CompletedTask;
            });
    }

    private async Task ApplyRuntimeStateResetAtBarrierAsync(IContext context, ApplyBrainRuntimeResetAtBarrier message)
    {
        var resetMessage = new ResetBrainRuntimeState
        {
            BrainId = message.BrainId.ToProtoUuid(),
            ResetBuffer = message.ResetBuffer,
            ResetAccumulator = message.ResetAccumulator
        };
        await ApplyRuntimeStateResetAsync(
                context,
                message.BrainId,
                resetMessage,
                message.MinimumAcceptedTickId)
            .ConfigureAwait(false);
    }

    private async Task ApplyRuntimeStateResetAsync(
        IContext context,
        Guid brainId,
        ResetBrainRuntimeState message,
        ulong minimumAcceptedTickId = 0)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            entry = await EnsureBrainEntryAsync(context, brainId).ConfigureAwait(false);
        }

        if (entry is null)
        {
            RespondCommandAck(context, message.BrainId, "reset_brain_runtime_state", success: false, "brain_io_unavailable");
            return;
        }

        var resetInputCoordinatorState = message.ResetAccumulator;

        var routerPid = await ResolveRouterPidAsync(context, brainId, allowCached: false).ConfigureAwait(false);
        if (routerPid is null)
        {
            RespondCommandAck(context, message.BrainId, "reset_brain_runtime_state", success: false, "brain_router_unavailable");
            return;
        }

        try
        {
            object routerReset = minimumAcceptedTickId > 0
                ? new ApplyBrainRuntimeResetAtBarrier(
                    brainId,
                    message.ResetBuffer,
                    message.ResetAccumulator,
                    minimumAcceptedTickId)
                : message;
            var routerAck = await context.RequestAsync<IoCommandAck>(routerPid, routerReset, DefaultRequestTimeout).ConfigureAwait(false);
            if (routerAck is null)
            {
                RespondCommandAck(context, message.BrainId, "reset_brain_runtime_state", success: false, "brain_router_empty_response");
                return;
            }

            if (!routerAck.Success)
            {
                context.Respond(routerAck);
                return;
            }

            if (resetInputCoordinatorState)
            {
                var shouldRequestTickDrainBefore = entry.InputState.ShouldRequestTickDrain;
                var inputAck = await DispatchCoordinatorMessageAsync(context, entry.InputPid, message).ConfigureAwait(false);
                if (inputAck is null)
                {
                    RespondCommandAck(context, message.BrainId, "reset_brain_runtime_state", success: false, "brain_input_empty_response");
                    return;
                }

                if (!inputAck.Success)
                {
                    RespondCommandAck(
                        context,
                        message.BrainId,
                        "reset_brain_runtime_state",
                        success: false,
                        $"brain_input_reset_failed:{inputAck.Message}");
                    return;
                }

                entry.InputState.ResetRuntimeState(resetAccumulator: true);
                if (shouldRequestTickDrainBefore != entry.InputState.ShouldRequestTickDrain)
                {
                    RegisterIoGatewayPid(context, brainId, routerPid, force: true);
                }
            }

            var outputAck = await DispatchCoordinatorMessageAsync(
                    context,
                    entry.OutputPid,
                    new ApplyOutputCoordinatorRuntimeReset(brainId, minimumAcceptedTickId))
                .ConfigureAwait(false);
            if (outputAck is null)
            {
                RespondCommandAck(context, message.BrainId, "reset_brain_runtime_state", success: false, "brain_output_empty_response");
                return;
            }

            if (!outputAck.Success)
            {
                RespondCommandAck(
                    context,
                    message.BrainId,
                    "reset_brain_runtime_state",
                    success: false,
                    $"brain_output_reset_failed:{outputAck.Message}");
                return;
            }

            context.Respond(routerAck);
        }
        catch (Exception ex)
        {
            RespondCommandAck(
                context,
                message.BrainId,
                "reset_brain_runtime_state",
                success: false,
                $"brain_router_request_failed:{ex.GetBaseException().Message}");
        }
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
            var drain = await RequestInputDrainAsync(context, entry, message, DefaultRequestTimeout).ConfigureAwait(false);
            context.Respond(drain);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"DrainInputs failed for {brainId}: {ex.Message}");

            if (IsMissingInputCoordinatorFailure(ex))
            {
                var recoveredEntry = await TryRecoverInputCoordinatorAsync(context, brainId, entry).ConfigureAwait(false);
                if (recoveredEntry is not null)
                {
                    try
                    {
                        var recoveredDrain = await RequestInputDrainAsync(
                                context,
                                recoveredEntry,
                                message,
                                DefaultRequestTimeout)
                            .ConfigureAwait(false);
                        context.Respond(recoveredDrain);
                        return;
                    }
                    catch (Exception retryEx)
                    {
                        Console.WriteLine($"DrainInputs retry failed for {brainId}: {retryEx.Message}");
                    }
                }
            }

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

    private async Task<BrainIoEntry?> EnsureBrainEntryAsync(
        IContext context,
        Guid brainId,
        TimeSpan? requestTimeout = null,
        bool bootstrapOnly = false)
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
            var timeout = requestTimeout ?? DefaultRequestTimeout;
            var register = await TryBuildRegisterBrainRegistrationAsync(
                    context,
                    brainId,
                    timeout,
                    includeArtifacts: !bootstrapOnly)
                .ConfigureAwait(false);
            if (register is null)
            {
                return null;
            }

            await RegisterBrainAsync(context, register, bootstrapOnly: bootstrapOnly);

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

    private async Task<Nbn.Proto.Io.RegisterBrain?> TryBuildRegisterBrainRegistrationAsync(
        IContext context,
        Guid brainId,
        TimeSpan requestTimeout,
        bool includeArtifacts)
    {
        if (_hiveMindPid is null)
        {
            return null;
        }

        var info = await context.RequestAsync<ProtoControl.BrainIoInfo>(
                _hiveMindPid,
                new ProtoControl.GetBrainIoInfo { BrainId = brainId.ToProtoUuid() },
                requestTimeout)
            .ConfigureAwait(false);

        if (info is null
            || (info.InputWidth == 0
                && info.OutputWidth == 0
                && string.IsNullOrWhiteSpace(info.InputCoordinatorPid)
                && string.IsNullOrWhiteSpace(info.OutputCoordinatorPid)
                && !info.IoGatewayOwnsInputCoordinator
                && !info.IoGatewayOwnsOutputCoordinator))
        {
            return null;
        }

        var register = new Nbn.Proto.Io.RegisterBrain
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

        if (includeArtifacts)
        {
            await TryPopulateArtifactMetadataFromHiveMindAsync(context, brainId, register, requestTimeout).ConfigureAwait(false);
        }

        return register;
    }

    private async Task<InputDrain> RequestInputDrainAsync(
        IContext context,
        BrainIoEntry entry,
        DrainInputs message,
        TimeSpan timeout)
    {
        if (LogInputTraceDiagnostics)
        {
            Console.WriteLine(
                $"[IoGatewayInput] drain request brain={entry.BrainId} tick={message.TickId} input={PidLabel(entry.InputPid)}");
        }

        var shouldRequestTickDrainBefore = entry.InputState.ShouldRequestTickDrain;
        var drain = await context.RequestAsync<InputDrain>(entry.InputPid, message, timeout).ConfigureAwait(false);
        entry.InputState.ApplyDrain(drain);
        if (shouldRequestTickDrainBefore != entry.InputState.ShouldRequestTickDrain
            && _routerCache.TryGetValue(entry.BrainId, out var routerPid))
        {
            RegisterIoGatewayPid(context, entry.BrainId, routerPid, force: true);
        }
        if (LogInputTraceDiagnostics || drain.Contribs.Count > 0)
        {
            Console.WriteLine(
                $"[IoGatewayInput] drain response brain={entry.BrainId} tick={message.TickId} contribs={drain.Contribs.Count} input={PidLabel(entry.InputPid)}");
        }

        return drain;
    }

    private static bool IsMissingInputCoordinatorFailure(Exception ex)
    {
        var failure = ex.GetBaseException();
        if (failure is TimeoutException)
        {
            return false;
        }

        var detail = failure.Message;
        return !string.IsNullOrWhiteSpace(detail)
               && (detail.Contains("no longer exists", StringComparison.OrdinalIgnoreCase)
                   || detail.Contains("unknown actor", StringComparison.OrdinalIgnoreCase));
    }

    private async Task<BrainIoEntry?> TryRecoverInputCoordinatorAsync(
        IContext context,
        Guid brainId,
        BrainIoEntry failedEntry)
    {
        var failedInputPid = failedEntry.InputPid;
        Nbn.Proto.Io.RegisterBrain? register;
        try
        {
            register = await TryBuildRegisterBrainRegistrationAsync(
                    context,
                    brainId,
                    DefaultRequestTimeout,
                    includeArtifacts: false)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Recover input coordinator failed for {brainId}: {ex.Message}");
            return null;
        }

        if (register is null)
        {
            return null;
        }

        BrainIoEntry refreshedEntry;
        try
        {
            await RegisterBrainAsync(context, register, bootstrapOnly: false).ConfigureAwait(false);
            if (!_brains.TryGetValue(brainId, out refreshedEntry!))
            {
                return null;
            }

            if (string.IsNullOrWhiteSpace(register.InputCoordinatorPid)
                && PidEquals(failedInputPid, refreshedEntry.InputPid))
            {
                refreshedEntry = await ReplaceWithOwnedInputCoordinatorFallbackAsync(
                        context,
                        refreshedEntry,
                        register.InputWidth,
                        register.InputCoordinatorMode)
                    .ConfigureAwait(false);
            }
            else if (PidEquals(failedInputPid, refreshedEntry.InputPid))
            {
                await ReplayInputStateAsync(context, refreshedEntry, DefaultRequestTimeout).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Recover input coordinator replay failed for {brainId}: {ex.Message}");
            return null;
        }

        if (LogInputTraceDiagnostics)
        {
            Console.WriteLine(
                $"[IoGatewayInput] drain recovery brain={brainId} previous={PidLabel(failedInputPid)} current={PidLabel(refreshedEntry.InputPid)} ownsInput={refreshedEntry.OwnsInputCoordinator}");
        }

        return refreshedEntry;
    }

    private async Task<BrainIoEntry> ReplaceWithOwnedInputCoordinatorFallbackAsync(
        IContext context,
        BrainIoEntry entry,
        uint requestedInputWidth,
        ProtoControl.InputCoordinatorMode requestedInputMode)
    {
        var inputWidth = entry.InputWidth;
        if (requestedInputWidth > inputWidth)
        {
            inputWidth = requestedInputWidth;
        }

        var inputMode = NormalizeInputCoordinatorMode(requestedInputMode);
        var previousPid = entry.InputPid;
        var previousOwned = entry.OwnsInputCoordinator;
        var replacementPid = SpawnOwnedInputCoordinator(context, entry.BrainId, inputWidth, inputMode);

        if (previousOwned && !PidEquals(previousPid, replacementPid))
        {
            context.Stop(previousPid);
        }

        entry.InputPid = replacementPid;
        entry.OwnsInputCoordinator = true;
        entry.InputWidth = inputWidth;
        entry.InputCoordinatorMode = inputMode;
        entry.InputState.UpdateConfiguration(inputWidth, inputMode);
        await ReplayInputStateAsync(context, entry, DefaultRequestTimeout).ConfigureAwait(false);
        return entry;
    }

    private async Task TryPopulateArtifactMetadataFromHiveMindAsync(
        IContext context,
        Guid brainId,
        Nbn.Proto.Io.RegisterBrain register,
        TimeSpan requestTimeout)
    {
        if (_hiveMindPid is null)
        {
            return;
        }

        try
        {
            var definition = await context.RequestAsync<BrainDefinitionReady>(
                    _hiveMindPid,
                    new ExportBrainDefinition
                    {
                        BrainId = brainId.ToProtoUuid(),
                        RebaseOverlays = false
                    },
                    requestTimeout)
                .ConfigureAwait(false);
            if (definition is not null && HasArtifactRef(definition.BrainDef))
            {
                register.BaseDefinition = definition.BrainDef;
            }
        }
        catch (Exception ex)
        {
            if (LogMetadataDiagnostics)
            {
                Console.WriteLine(
                    $"[IoGatewayMeta] EnsureBrainEntry export bootstrap failed brain={brainId} detail={ex.GetBaseException().Message}");
            }
        }

        try
        {
            var snapshot = await context.RequestAsync<SnapshotReady>(
                    _hiveMindPid,
                    new RequestSnapshot
                    {
                        BrainId = brainId.ToProtoUuid()
                    },
                    requestTimeout)
                .ConfigureAwait(false);
            if (snapshot is not null && HasArtifactRef(snapshot.Snapshot))
            {
                register.LastSnapshot = snapshot.Snapshot;
            }
        }
        catch (Exception ex)
        {
            if (LogMetadataDiagnostics)
            {
                Console.WriteLine(
                    $"[IoGatewayMeta] EnsureBrainEntry snapshot bootstrap failed brain={brainId} detail={ex.GetBaseException().Message}");
            }
        }
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
            case ResetBrainRuntimeState resetState when TryGetBrainId(resetState.BrainId, out var brainId):
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
            case ResetBrainRuntimeState resetState:
                return TryGetBrainId(resetState.BrainId, out guid);
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
        if (!TryGetOutputSubscriptionCommand(message, out var command))
        {
            return;
        }

        ApplySubscriberChange(
            context,
            command.SubscriberActor,
            command.Subscribe,
            command.IsVector ? entry.OutputVectorSubscribers : entry.OutputSubscribers);
    }

    private void TrackPendingOutputSubscription(IContext context, Guid brainId, object message)
    {
        if (!TryGetOutputSubscriptionCommand(message, out var command))
        {
            return;
        }

        if (command.Subscribe)
        {
            ApplySubscriberChange(
                context,
                command.SubscriberActor,
                true,
                GetPendingSubscriberSet(brainId, command.IsVector));
            return;
        }

        RemovePendingSubscriber(context, command.SubscriberActor, brainId, command.IsVector);
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

    private static void ApplySubscriberChange(
        IContext context,
        string? subscriberActor,
        bool subscribe,
        Dictionary<string, PID> set)
    {
        if (subscribe)
        {
            AddSubscriber(context, subscriberActor, set);
            return;
        }

        RemoveSubscriber(context, subscriberActor, set);
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
        if (!TryGetOutputSubscriptionCommand(message, out var command))
        {
            return message;
        }

        return CreateOutputSubscriptionMessage(
            command,
            ResolveSubscriberActorLabel(context, command.SubscriberActor));
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

    private static async Task ReplayInputStateAsync(
        IContext context,
        BrainIoEntry entry,
        TimeSpan? timeout = null)
    {
        await DispatchCoordinatorMessageAsync(
                context,
                entry.InputPid,
                new UpdateInputCoordinatorMode(entry.InputCoordinatorMode),
                timeout)
            .ConfigureAwait(false);
        await entry.InputState
            .ReplayToAsync(message => DispatchCoordinatorMessageAsync(context, entry.InputPid, message, timeout))
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

    private static async Task<IoCommandAck> DispatchCoordinatorMessageAsync(
        IContext context,
        PID target,
        object message,
        TimeSpan? timeout = null)
    {
        return await context.RequestAsync<IoCommandAck>(target, message, timeout ?? DefaultRequestTimeout).ConfigureAwait(false);
    }

    private static void RespondOutputCommandAck(IContext context, object message, Uuid? brainId, bool success, string ackMessage)
    {
        var command = TryGetOutputSubscriptionCommand(message, out var subscription)
            ? subscription.Command
            : "output_command";

        RespondCommandAck(context, brainId, command, success, ackMessage);
    }

    private static void RespondInputCommandAck(IContext context, object message, Uuid? brainId, bool success, string ackMessage)
    {
        var command = message switch
        {
            InputWrite => "input_write",
            InputVector => "input_vector",
            _ => "input_command"
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

            var hasRootPid = TryParsePid(info.BrainRootPid, out var rootPid) && rootPid is not null;
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

                var registrationTarget = hasRootPid ? rootPid! : routerPid;
                if (!_routerRegistrationTargetCache.TryGetValue(brainId, out var cachedRegistrationTarget)
                    || !PidEquals(cachedRegistrationTarget, registrationTarget))
                {
                    _routerRegistration.Remove(brainId);
                    _routerRegistrationTargetCache[brainId] = registrationTarget;
                }

                _routerCache[brainId] = routerPid;
                RegisterIoGatewayPid(context, brainId, routerPid);
                return routerPid;
            }

            if (hasRootPid)
            {
                if (LogInputTraceDiagnostics || !PidEquals(cached, rootPid!))
                {
                    Console.WriteLine(
                        $"[IoGatewayInput] router resolved brain={brainId} target=brain_root previous={PidLabel(cached)} current={PidLabel(rootPid)}");
                }

                if (!PidEquals(cached, rootPid!))
                {
                    _routerRegistration.Remove(brainId);
                }

                _routerCache[brainId] = rootPid!;
                _routerRegistrationTargetCache[brainId] = rootPid!;
                RegisterIoGatewayPid(context, brainId, rootPid!);
                return rootPid!;
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
            ResetBrainRuntimeState resetState => $"reset_buffer={resetState.ResetBuffer} reset_accumulator={resetState.ResetAccumulator}",
            _ => string.Empty
        };
    }

    private void RegisterIoGatewayPid(IContext context, Guid brainId, PID routerPid, bool force = false)
    {
        var registrationTarget = ResolveRegistrationTarget(brainId, routerPid);
        var routerLabel = PidLabel(registrationTarget);
        if (!force
            && _routerRegistration.TryGetValue(brainId, out var registered)
            && registered == routerLabel)
        {
            return;
        }

        var selfPid = ToRemotePid(context, context.Self);
        var register = new RegisterIoGateway
        {
            BrainId = brainId.ToProtoUuid(),
            IoGatewayPid = PidLabel(selfPid)
        };
        if (_brains.TryGetValue(brainId, out var entry))
        {
            register.InputCoordinatorMode = entry.InputCoordinatorMode;
            register.InputTickDrainArmed = entry.InputState.ShouldRequestTickDrain;
        }

        context.Send(registrationTarget, register);

        _routerRegistration[brainId] = routerLabel;
    }

    private PID ResolveRegistrationTarget(Guid brainId, PID routerPid)
        => _routerRegistrationTargetCache.TryGetValue(brainId, out var target)
            ? target
            : routerPid;

    private static bool TryGetOutputSubscriptionCommand(object message, out OutputSubscriptionCommand command)
    {
        switch (message)
        {
            case SubscribeOutputs subscribe:
                command = new OutputSubscriptionCommand(subscribe.BrainId, subscribe.SubscriberActor, false, true, "subscribe_outputs");
                return true;
            case UnsubscribeOutputs unsubscribe:
                command = new OutputSubscriptionCommand(unsubscribe.BrainId, unsubscribe.SubscriberActor, false, false, "unsubscribe_outputs");
                return true;
            case SubscribeOutputsVector subscribeVector:
                command = new OutputSubscriptionCommand(subscribeVector.BrainId, subscribeVector.SubscriberActor, true, true, "subscribe_outputs_vector");
                return true;
            case UnsubscribeOutputsVector unsubscribeVector:
                command = new OutputSubscriptionCommand(unsubscribeVector.BrainId, unsubscribeVector.SubscriberActor, true, false, "unsubscribe_outputs_vector");
                return true;
            default:
                command = default;
                return false;
        }
    }

    private static object CreateOutputSubscriptionMessage(OutputSubscriptionCommand command, string subscriberActor)
    {
        if (command.Subscribe)
        {
            return command.IsVector
                ? new SubscribeOutputsVector
                {
                    BrainId = command.BrainId,
                    SubscriberActor = subscriberActor
                }
                : new SubscribeOutputs
                {
                    BrainId = command.BrainId,
                    SubscriberActor = subscriberActor
                };
        }

        return command.IsVector
            ? new UnsubscribeOutputsVector
            {
                BrainId = command.BrainId,
                SubscriberActor = subscriberActor
            }
            : new UnsubscribeOutputs
            {
                BrainId = command.BrainId,
                SubscriberActor = subscriberActor
            };
    }

    private readonly record struct OutputSubscriptionCommand(
        Uuid? BrainId,
        string? SubscriberActor,
        bool IsVector,
        bool Subscribe,
        string Command);
}
