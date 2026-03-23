using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.IO;

public sealed partial class IoGatewayActor
{
    private async Task HandleSpawnBrain(IContext context, SpawnBrainViaIO message)
    {
        if (_hiveMindPid is null)
        {
            var ack = BuildSpawnFailureAck(
                reasonCode: "spawn_unavailable",
                failureMessage: "Spawn failed: HiveMind endpoint is not configured.");
            context.Respond(new SpawnBrainViaIOAck
            {
                Ack = ack,
                FailureReasonCode = ack.FailureReasonCode,
                FailureMessage = ack.FailureMessage
            });
            return;
        }

        try
        {
            var ack = await context.RequestAsync<ProtoControl.SpawnBrainAck>(_hiveMindPid, message.Request, SpawnRequestTimeout);
            if (ack is null)
            {
                ack = BuildSpawnFailureAck(
                    reasonCode: "spawn_empty_response",
                    failureMessage: "Spawn failed: HiveMind returned an empty spawn acknowledgment.");
            }

            context.Respond(new SpawnBrainViaIOAck
            {
                Ack = ack,
                FailureReasonCode = ack.FailureReasonCode,
                FailureMessage = ack.FailureMessage
            });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"SpawnBrainViaIO failed: {ex.Message}");
            var ack = BuildSpawnFailureAck(
                reasonCode: "spawn_request_failed",
                failureMessage: $"Spawn failed: request forwarding to HiveMind failed ({ex.GetBaseException().Message}).");
            context.Respond(new SpawnBrainViaIOAck
            {
                Ack = ack,
                FailureReasonCode = ack.FailureReasonCode,
                FailureMessage = ack.FailureMessage
            });
        }
    }

    private async Task HandleBrainInfoAsync(IContext context, BrainInfoRequest message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            return;
        }

        if (!_brains.TryGetValue(brainId, out var entry))
        {
            entry = await EnsureBrainEntryAsync(context, brainId, BrainInfoResolveTimeout).ConfigureAwait(false);
        }

        if (entry is null)
        {
            var missing = new BrainInfo
            {
                BrainId = message.BrainId,
                InputWidth = 0,
                OutputWidth = 0,
                CostEnabled = false,
                EnergyEnabled = false,
                EnergyRemaining = 0,
                PlasticityEnabled = true,
                EnergyRateUnitsPerSecond = 0,
                PlasticityRate = DefaultPlasticityRate,
                PlasticityProbabilisticUpdates = true,
                HomeostasisEnabled = true,
                HomeostasisTargetMode = ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero,
                HomeostasisUpdateMode = ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
                HomeostasisBaseProbability = 0.01f,
                HomeostasisMinStepCodes = 1,
                HomeostasisEnergyCouplingEnabled = false,
                HomeostasisEnergyTargetScale = 1f,
                HomeostasisEnergyProbabilityScale = 1f,
                PlasticityDelta = DefaultPlasticityDelta,
                PlasticityRebaseThreshold = 0,
                PlasticityRebaseThresholdPct = 0f,
                PlasticityEnergyCostModulationEnabled = false,
                PlasticityEnergyCostReferenceTickCost = DefaultPlasticityEnergyCostReferenceTickCost,
                PlasticityEnergyCostResponseStrength = DefaultPlasticityEnergyCostResponseStrength,
                PlasticityEnergyCostMinScale = DefaultPlasticityEnergyCostMinScale,
                PlasticityEnergyCostMaxScale = DefaultPlasticityEnergyCostMaxScale,
                InputCoordinatorMode = DefaultInputCoordinatorMode,
                OutputVectorSource = DefaultOutputVectorSource,
                LastTickCost = 0,
                BaseDefinition = new ArtifactRef(),
                LastSnapshot = new ArtifactRef()
            };
            context.Respond(missing);

            if (LogMetadataDiagnostics)
            {
                Console.WriteLine(
                    $"[IoGatewayMeta] BrainInfo missing brain={brainId} requester={PidLabel(context.Sender)}");
            }
            return;
        }

        entry.Energy.Accrue();
        var response = new BrainInfo
        {
            BrainId = message.BrainId,
            InputWidth = entry.InputWidth,
            OutputWidth = entry.OutputWidth,
            CostEnabled = entry.Energy.CostEnabled,
            EnergyEnabled = entry.Energy.EnergyEnabled,
            EnergyRemaining = entry.Energy.EnergyRemaining,
            PlasticityEnabled = entry.Energy.PlasticityEnabled,
            EnergyRateUnitsPerSecond = entry.Energy.EnergyRateUnitsPerSecond,
            PlasticityRate = entry.Energy.PlasticityRate,
            PlasticityProbabilisticUpdates = entry.Energy.PlasticityProbabilisticUpdates,
            PlasticityDelta = entry.Energy.PlasticityDelta,
            PlasticityRebaseThreshold = entry.Energy.PlasticityRebaseThreshold,
            PlasticityRebaseThresholdPct = entry.Energy.PlasticityRebaseThresholdPct,
            PlasticityEnergyCostModulationEnabled = entry.Energy.PlasticityEnergyCostModulationEnabled,
            PlasticityEnergyCostReferenceTickCost = entry.Energy.PlasticityEnergyCostReferenceTickCost,
            PlasticityEnergyCostResponseStrength = entry.Energy.PlasticityEnergyCostResponseStrength,
            PlasticityEnergyCostMinScale = entry.Energy.PlasticityEnergyCostMinScale,
            PlasticityEnergyCostMaxScale = entry.Energy.PlasticityEnergyCostMaxScale,
            HomeostasisEnabled = entry.Energy.HomeostasisEnabled,
            HomeostasisTargetMode = entry.Energy.HomeostasisTargetMode,
            HomeostasisUpdateMode = entry.Energy.HomeostasisUpdateMode,
            HomeostasisBaseProbability = entry.Energy.HomeostasisBaseProbability,
            HomeostasisMinStepCodes = entry.Energy.HomeostasisMinStepCodes,
            HomeostasisEnergyCouplingEnabled = entry.Energy.HomeostasisEnergyCouplingEnabled,
            HomeostasisEnergyTargetScale = entry.Energy.HomeostasisEnergyTargetScale,
            HomeostasisEnergyProbabilityScale = entry.Energy.HomeostasisEnergyProbabilityScale,
            LastTickCost = entry.Energy.LastTickCost,
            InputCoordinatorMode = entry.InputCoordinatorMode,
            OutputVectorSource = entry.OutputVectorSource,
            BaseDefinition = entry.BaseDefinition ?? new ArtifactRef(),
            LastSnapshot = entry.LastSnapshot ?? new ArtifactRef()
        };
        context.Respond(response);

        if (LogMetadataDiagnostics && !HasArtifactRef(response.BaseDefinition))
        {
            Console.WriteLine(
                $"[IoGatewayMeta] BrainInfo no-base brain={brainId} requester={PidLabel(context.Sender)} input={response.InputWidth} output={response.OutputWidth} snapshot={ArtifactLabel(response.LastSnapshot)}");
        }
    }

    private async Task RegisterBrainAsync(IContext context, RegisterBrain message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            return;
        }

        var inputCoordinatorMode = NormalizeInputCoordinatorMode(message.InputCoordinatorMode);
        var outputVectorSource = NormalizeOutputVectorSource(message.OutputVectorSource);
        var registeredInputPid = await RoutablePidReference.ResolveAsync(message.InputCoordinatorPid).ConfigureAwait(false);
        var registeredOutputPid = await RoutablePidReference.ResolveAsync(message.OutputCoordinatorPid).ConfigureAwait(false);
        var hasRegisteredInputPid = registeredInputPid is not null;
        var hasRegisteredOutputPid = registeredOutputPid is not null;
        var registeredInputOwned = hasRegisteredInputPid && message.IoGatewayOwnsInputCoordinator;
        var registeredOutputOwned = hasRegisteredOutputPid && message.IoGatewayOwnsOutputCoordinator;
        var shouldRegisterOutputSink = hasRegisteredOutputPid || message.IoGatewayOwnsOutputCoordinator;

        if (_brains.TryGetValue(brainId, out var existing))
        {
            var inputWidthChanged = false;
            var outputWidthChanged = false;
            if (existing.InputWidth != message.InputWidth || existing.OutputWidth != message.OutputWidth)
            {
                if (message.InputWidth > existing.InputWidth && message.InputWidth > 0)
                {
                    existing.InputWidth = message.InputWidth;
                    inputWidthChanged = true;
                }

                if (message.OutputWidth > existing.OutputWidth && message.OutputWidth > 0)
                {
                    existing.OutputWidth = message.OutputWidth;
                    outputWidthChanged = true;
                }

                if (existing.InputWidth != message.InputWidth && existing.OutputWidth != message.OutputWidth)
                {
                    Console.WriteLine($"RegisterBrain width mismatch for {brainId}. Keeping existing widths.");
                }
            }

            if (message.BaseDefinition is not null)
            {
                existing.BaseDefinition = message.BaseDefinition;
            }

            if (message.LastSnapshot is not null)
            {
                existing.LastSnapshot = message.LastSnapshot;
            }

            if (message.EnergyState is not null)
            {
                existing.Energy.ResetFrom(message.EnergyState);
                existing.EnergyDepletedSignaled = false;
                existing.LastAppliedTickCostId = null;
                existing.RegisteredAtMs = NowMs();
            }

            if (message.HasRuntimeConfig)
            {
                var effectiveDelta = ResolvePlasticityDelta(message.PlasticityRate, message.PlasticityDelta);
                existing.Energy.SetRuntimeConfig(
                    message.CostEnabled,
                    message.EnergyEnabled,
                    message.PlasticityEnabled,
                    message.PlasticityRate,
                    message.PlasticityProbabilisticUpdates,
                    effectiveDelta,
                    message.PlasticityRebaseThreshold,
                    message.PlasticityRebaseThresholdPct,
                    message.PlasticityEnergyCostModulationEnabled,
                    message.PlasticityEnergyCostReferenceTickCost,
                    message.PlasticityEnergyCostResponseStrength,
                    message.PlasticityEnergyCostMinScale,
                    message.PlasticityEnergyCostMaxScale,
                    message.HomeostasisEnabled,
                    message.HomeostasisTargetMode,
                    message.HomeostasisUpdateMode,
                    message.HomeostasisBaseProbability,
                    message.HomeostasisMinStepCodes,
                    message.HomeostasisEnergyCouplingEnabled,
                    message.HomeostasisEnergyTargetScale,
                    message.HomeostasisEnergyProbabilityScale,
                    message.LastTickCost);
            }

            var inputModeChanged = existing.InputCoordinatorMode != inputCoordinatorMode;
            existing.InputCoordinatorMode = inputCoordinatorMode;
            existing.OutputVectorSource = outputVectorSource;
            existing.InputState.UpdateConfiguration(existing.InputWidth, inputCoordinatorMode);

            var inputCoordinatorChanged = false;
            if (hasRegisteredInputPid && registeredInputPid is not null)
            {
                inputCoordinatorChanged = UpdateCoordinatorReference(
                    context,
                    existing.InputPid,
                    existing.OwnsInputCoordinator,
                    registeredInputPid,
                    registeredInputOwned,
                    out var updatedInputPid,
                    out var updatedOwnsInputCoordinator);
                existing.InputPid = updatedInputPid;
                existing.OwnsInputCoordinator = updatedOwnsInputCoordinator;
            }

            var outputCoordinatorChanged = false;
            if (hasRegisteredOutputPid && registeredOutputPid is not null)
            {
                outputCoordinatorChanged = UpdateCoordinatorReference(
                    context,
                    existing.OutputPid,
                    existing.OwnsOutputCoordinator,
                    registeredOutputPid,
                    registeredOutputOwned,
                    out var updatedOutputPid,
                    out var updatedOwnsOutputCoordinator);
                existing.OutputPid = updatedOutputPid;
                existing.OwnsOutputCoordinator = updatedOwnsOutputCoordinator;
            }

            if (inputWidthChanged || inputCoordinatorChanged)
            {
                await DispatchCoordinatorMessageAsync(
                        context,
                        existing.InputPid,
                        new UpdateInputWidth(existing.InputWidth))
                    .ConfigureAwait(false);
            }

            if (inputModeChanged || inputCoordinatorChanged)
            {
                await DispatchCoordinatorMessageAsync(
                        context,
                        existing.InputPid,
                        new UpdateInputCoordinatorMode(inputCoordinatorMode))
                    .ConfigureAwait(false);
            }

            if (outputWidthChanged || outputCoordinatorChanged)
            {
                await DispatchCoordinatorMessageAsync(
                        context,
                        existing.OutputPid,
                        new UpdateOutputWidth(existing.OutputWidth))
                    .ConfigureAwait(false);
            }

            if (inputCoordinatorChanged)
            {
                await ReplayInputStateAsync(context, existing).ConfigureAwait(false);
            }

            if (outputCoordinatorChanged)
            {
                await ReplayOutputSubscriptionsAsync(context, existing).ConfigureAwait(false);
            }

            if (MergePendingOutputSubscriptions(existing))
            {
                await ReplayOutputSubscriptionsAsync(context, existing).ConfigureAwait(false);
            }

            if (LogMetadataDiagnostics)
            {
                Console.WriteLine(
                    $"[IoGatewayMeta] RegisterBrain update brain={brainId} input={existing.InputWidth} output={existing.OutputWidth} base={ArtifactLabel(existing.BaseDefinition)} snapshot={ArtifactLabel(existing.LastSnapshot)} runtimeConfig={message.HasRuntimeConfig}");
            }

            await EnsureIoGatewayRegisteredAsync(context, brainId);
            await EnsureOutputSinkRegisteredAsync(context, brainId, existing.OutputPid, shouldRegisterOutputSink);
            return;
        }

        var ownsInputCoordinator = !hasRegisteredInputPid || registeredInputOwned;
        var ownsOutputCoordinator = !hasRegisteredOutputPid || registeredOutputOwned;
        var inputPid = hasRegisteredInputPid && registeredInputPid is not null
            ? registeredInputPid
            : SpawnOwnedInputCoordinator(context, brainId, message.InputWidth, inputCoordinatorMode);
        var outputPid = hasRegisteredOutputPid && registeredOutputPid is not null
            ? registeredOutputPid
            : SpawnOwnedOutputCoordinator(context, brainId, message.OutputWidth);

        var energy = new BrainEnergyState();
        if (message.EnergyState is not null)
        {
            energy.ResetFrom(message.EnergyState);
        }

        if (message.HasRuntimeConfig)
        {
            var effectiveDelta = ResolvePlasticityDelta(message.PlasticityRate, message.PlasticityDelta);
            energy.SetRuntimeConfig(
                message.CostEnabled,
                message.EnergyEnabled,
                message.PlasticityEnabled,
                message.PlasticityRate,
                message.PlasticityProbabilisticUpdates,
                effectiveDelta,
                message.PlasticityRebaseThreshold,
                message.PlasticityRebaseThresholdPct,
                message.PlasticityEnergyCostModulationEnabled,
                message.PlasticityEnergyCostReferenceTickCost,
                message.PlasticityEnergyCostResponseStrength,
                message.PlasticityEnergyCostMinScale,
                message.PlasticityEnergyCostMaxScale,
                message.HomeostasisEnabled,
                message.HomeostasisTargetMode,
                message.HomeostasisUpdateMode,
                message.HomeostasisBaseProbability,
                message.HomeostasisMinStepCodes,
                message.HomeostasisEnergyCouplingEnabled,
                message.HomeostasisEnergyTargetScale,
                message.HomeostasisEnergyProbabilityScale,
                message.LastTickCost);
        }

        var entry = new BrainIoEntry(
            brainId,
            inputPid,
            outputPid,
            ownsInputCoordinator,
            ownsOutputCoordinator,
            message.InputWidth,
            message.OutputWidth,
            energy,
            inputCoordinatorMode,
            outputVectorSource)
        {
            BaseDefinition = message.BaseDefinition,
            LastSnapshot = message.LastSnapshot
        };

        _brains.Add(brainId, entry);

        await DispatchCoordinatorMessageAsync(
                context,
                entry.InputPid,
                new UpdateInputWidth(entry.InputWidth))
            .ConfigureAwait(false);
        await DispatchCoordinatorMessageAsync(
                context,
                entry.InputPid,
                new UpdateInputCoordinatorMode(inputCoordinatorMode))
            .ConfigureAwait(false);
        await DispatchCoordinatorMessageAsync(
                context,
                entry.OutputPid,
                new UpdateOutputWidth(entry.OutputWidth))
            .ConfigureAwait(false);

        if (MergePendingOutputSubscriptions(entry))
        {
            await ReplayOutputSubscriptionsAsync(context, entry).ConfigureAwait(false);
        }

        if (LogMetadataDiagnostics)
        {
            Console.WriteLine(
                $"[IoGatewayMeta] RegisterBrain add brain={brainId} input={entry.InputWidth} output={entry.OutputWidth} base={ArtifactLabel(entry.BaseDefinition)} snapshot={ArtifactLabel(entry.LastSnapshot)} runtimeConfig={message.HasRuntimeConfig}");
        }

        await EnsureIoGatewayRegisteredAsync(context, brainId);
        await EnsureOutputSinkRegisteredAsync(context, brainId, outputPid, shouldRegisterOutputSink);
    }

    private void UnregisterBrain(IContext context, UnregisterBrain message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            return;
        }

        if (!_brains.TryGetValue(brainId, out var entry))
        {
            return;
        }

        StopAndRemoveBrain(context, entry);
    }

    private void UpdateSnapshot(UpdateBrainSnapshot message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var entry))
        {
            return;
        }

        entry.LastSnapshot = message.Snapshot;
    }

    private void HandleBrainTerminated(IContext context, ProtoControl.BrainTerminated message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            return;
        }

        var outbound = message;
        if (_brains.TryGetValue(brainId, out var entry))
        {
            if (message.TimeMs > 0 && message.TimeMs < (ulong)entry.RegisteredAtMs)
            {
                return;
            }

            if (HasArtifactRef(message.BaseDef))
            {
                entry.BaseDefinition = message.BaseDef;
            }

            if (HasArtifactRef(message.LastSnapshot))
            {
                entry.LastSnapshot = message.LastSnapshot;
            }

            outbound = BuildTerminatedFromEntry(message, entry);
            StopAndRemoveBrain(context, entry);
        }

        BroadcastToClients(context, outbound);
    }

    private void HandleExportBrainDefinition(IContext context, ExportBrainDefinition message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            context.Respond(new BrainDefinitionReady());
            if (LogMetadataDiagnostics)
            {
                Console.WriteLine("[IoGatewayMeta] ExportBrainDefinition invalid-brain-id");
            }
            return;
        }

        _brains.TryGetValue(brainId, out var entry);
        if (!message.RebaseOverlays && entry is not null && HasArtifactRef(entry.BaseDefinition))
        {
            if (LogMetadataDiagnostics)
            {
                Console.WriteLine(
                    $"[IoGatewayMeta] ExportBrainDefinition local-hit brain={brainId} base={ArtifactLabel(entry.BaseDefinition)} rebase={message.RebaseOverlays}");
            }

            context.Respond(new BrainDefinitionReady
            {
                BrainId = message.BrainId,
                BrainDef = entry.BaseDefinition
            });
            return;
        }

        if (_hiveMindPid is null)
        {
            if (entry is not null && HasArtifactRef(entry.BaseDefinition))
            {
                if (LogMetadataDiagnostics)
                {
                    Console.WriteLine(
                        $"[IoGatewayMeta] ExportBrainDefinition hive-missing local-fallback brain={brainId} base={ArtifactLabel(entry.BaseDefinition)}");
                }

                context.Respond(new BrainDefinitionReady
                {
                    BrainId = message.BrainId,
                    BrainDef = entry.BaseDefinition
                });
            }
            else
            {
                if (LogMetadataDiagnostics)
                {
                    Console.WriteLine(
                        $"[IoGatewayMeta] ExportBrainDefinition empty brain={brainId} reason=hive-missing-local-missing");
                }

                context.Respond(new BrainDefinitionReady { BrainId = message.BrainId });
            }
            return;
        }

        var exportTask = context.RequestAsync<BrainDefinitionReady>(_hiveMindPid, message, ExportRequestTimeout);
        context.ReenterAfter(exportTask, completed =>
        {
            if (completed.IsCompletedSuccessfully)
            {
                var ready = completed.Result;
                if (ready is not null
                    && HasArtifactRef(ready.BrainDef)
                    && !message.RebaseOverlays
                    && _brains.TryGetValue(brainId, out var existing))
                {
                    existing.BaseDefinition = ready.BrainDef;
                }

                if (ready is not null)
                {
                    if (LogMetadataDiagnostics)
                    {
                        Console.WriteLine(
                            $"[IoGatewayMeta] ExportBrainDefinition hive-response brain={brainId} base={ArtifactLabel(ready.BrainDef)}");
                    }

                    context.Respond(ready);
                    return Task.CompletedTask;
                }
            }

            if (completed.IsFaulted || completed.IsCanceled)
            {
                var detail = completed.Exception?.GetBaseException().Message ?? "request canceled";
                Console.WriteLine($"ExportBrainDefinition failed for {brainId}: {detail}");
            }

            if (entry is not null && HasArtifactRef(entry.BaseDefinition))
            {
                if (LogMetadataDiagnostics)
                {
                    Console.WriteLine(
                        $"[IoGatewayMeta] ExportBrainDefinition fallback-local brain={brainId} base={ArtifactLabel(entry.BaseDefinition)}");
                }

                context.Respond(new BrainDefinitionReady
                {
                    BrainId = message.BrainId,
                    BrainDef = entry.BaseDefinition
                });
                return Task.CompletedTask;
            }

            if (LogMetadataDiagnostics)
            {
                Console.WriteLine(
                    $"[IoGatewayMeta] ExportBrainDefinition empty brain={brainId} reason=hive-failed-local-missing");
            }

            context.Respond(new BrainDefinitionReady { BrainId = message.BrainId });
            return Task.CompletedTask;
        });
    }

    private async Task HandleRequestSnapshotAsync(IContext context, RequestSnapshot message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            context.Respond(new SnapshotReady());
            return;
        }

        _brains.TryGetValue(brainId, out var entry);

        if (_hiveMindPid is null)
        {
            if (entry is not null && HasArtifactRef(entry.LastSnapshot))
            {
                context.Respond(new SnapshotReady
                {
                    BrainId = message.BrainId,
                    Snapshot = entry.LastSnapshot
                });
                return;
            }

            context.Respond(new SnapshotReady { BrainId = message.BrainId });
            return;
        }

        try
        {
            var request = new RequestSnapshot
            {
                BrainId = message.BrainId
            };

            if (entry is not null)
            {
                request.HasRuntimeState = true;
                request.EnergyRemaining = entry.Energy.EnergyRemaining;
                request.CostEnabled = entry.Energy.CostEnabled;
                request.EnergyEnabled = entry.Energy.EnergyEnabled;
                request.PlasticityEnabled = entry.Energy.PlasticityEnabled;
            }

            var ready = await context.RequestAsync<SnapshotReady>(_hiveMindPid, request, DefaultRequestTimeout).ConfigureAwait(false);
            if (ready is not null
                && HasArtifactRef(ready.Snapshot)
                && _brains.TryGetValue(brainId, out var existing))
            {
                existing.LastSnapshot = ready.Snapshot;
            }

            if (ready is not null && HasArtifactRef(ready.Snapshot))
            {
                context.Respond(ready);
                return;
            }

            if (entry is not null && HasArtifactRef(entry.LastSnapshot))
            {
                context.Respond(new SnapshotReady
                {
                    BrainId = message.BrainId,
                    Snapshot = entry.LastSnapshot
                });
                return;
            }

            context.Respond(new SnapshotReady { BrainId = message.BrainId });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"RequestSnapshot failed for {brainId}: {ex.Message}");
            if (entry is not null && HasArtifactRef(entry.LastSnapshot))
            {
                context.Respond(new SnapshotReady
                {
                    BrainId = message.BrainId,
                    Snapshot = entry.LastSnapshot
                });
                return;
            }

            context.Respond(new SnapshotReady { BrainId = message.BrainId });
        }
    }

    private static ProtoControl.SpawnBrainAck BuildSpawnFailureAck(string reasonCode, string failureMessage)
    {
        var normalizedReasonCode = string.IsNullOrWhiteSpace(reasonCode)
            ? "spawn_failed"
            : reasonCode.Trim();
        var normalizedFailureMessage = string.IsNullOrWhiteSpace(failureMessage)
            ? "Spawn failed."
            : failureMessage.Trim();
        return new ProtoControl.SpawnBrainAck
        {
            BrainId = Guid.Empty.ToProtoUuid(),
            FailureReasonCode = normalizedReasonCode,
            FailureMessage = normalizedFailureMessage
        };
    }

    private static bool HasArtifactRef(ArtifactRef? reference)
        => reference is not null
           && reference.Sha256 is not null
           && reference.Sha256.Value is not null
           && reference.Sha256.Value.Length == 32;

    private static string ArtifactLabel(ArtifactRef? reference)
    {
        if (!HasArtifactRef(reference))
        {
            return "missing";
        }

        return reference!.TryToSha256Hex(out var sha)
            ? sha[..Math.Min(12, sha.Length)]
            : "present";
    }

    private void StopAndRemoveBrain(IContext context, BrainIoEntry entry)
    {
        if (entry.OwnsInputCoordinator)
        {
            context.Stop(entry.InputPid);
        }

        if (entry.OwnsOutputCoordinator)
        {
            context.Stop(entry.OutputPid);
        }

        _brains.Remove(entry.BrainId);
        _routerCache.Remove(entry.BrainId);
        _routerRegistration.Remove(entry.BrainId);
    }

    private static PID SpawnOwnedInputCoordinator(
        IContext context,
        Guid brainId,
        uint inputWidth,
        ProtoControl.InputCoordinatorMode inputCoordinatorMode)
    {
        var inputName = IoNames.InputCoordinatorPrefix + brainId.ToString("N");
        try
        {
            return context.SpawnNamed(
                Props.FromProducer(() => new InputCoordinatorActor(brainId, inputWidth, inputCoordinatorMode)),
                inputName);
        }
        catch
        {
            return context.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, inputWidth, inputCoordinatorMode)));
        }
    }

    private static PID SpawnOwnedOutputCoordinator(IContext context, Guid brainId, uint outputWidth)
    {
        var outputName = IoNames.OutputCoordinatorPrefix + brainId.ToString("N");
        try
        {
            return context.SpawnNamed(
                Props.FromProducer(() => new OutputCoordinatorActor(brainId, outputWidth)),
                outputName);
        }
        catch
        {
            return context.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, outputWidth)));
        }
    }
}
