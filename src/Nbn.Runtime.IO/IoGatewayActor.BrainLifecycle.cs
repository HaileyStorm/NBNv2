using System.Diagnostics;
using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Shared;
using PauseBrainRequest = Nbn.Shared.HiveMind.PauseBrainRequest;
using ResumeBrainRequest = Nbn.Shared.HiveMind.ResumeBrainRequest;
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
            var ack = await RequestHiveMindAsync<ProtoControl.SpawnBrainAck>(
                    context,
                    message.Request,
                    SpawnRequestTimeout,
                    "SpawnBrainViaIO")
                .ConfigureAwait(false);
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

    private void HandleAwaitSpawnPlacement(IContext context, AwaitSpawnPlacementViaIO message)
    {
        if (_hiveMindPid is null)
        {
            var ack = BuildSpawnFailureAck(
                reasonCode: "spawn_unavailable",
                failureMessage: "Spawn wait failed: HiveMind endpoint is not configured.");
            context.Respond(new AwaitSpawnPlacementViaIOAck
            {
                Ack = ack,
                FailureReasonCode = ack.FailureReasonCode,
                FailureMessage = ack.FailureMessage
            });
            return;
        }

        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            var ack = BuildSpawnFailureAck(
                reasonCode: "spawn_invalid_request",
                failureMessage: "Spawn wait failed: brain_id is required.");
            context.Respond(new AwaitSpawnPlacementViaIOAck
            {
                Ack = ack,
                FailureReasonCode = ack.FailureReasonCode,
                FailureMessage = ack.FailureMessage
            });
            return;
        }

        var replyTo = context.Sender;
        var system = context.System;
        var startedAtTimestamp = Stopwatch.GetTimestamp();

        try
        {
            var requestedTimeoutMs = message.TimeoutMs;
            var transportTimeoutMs = requestedTimeoutMs >= (ulong)int.MaxValue - 250
                ? int.MaxValue
                : requestedTimeoutMs == 0
                ? (int)Math.Max(100, TimeSpan.FromMinutes(10).TotalMilliseconds)
                : (int)Math.Max(100, requestedTimeoutMs + 250UL);

            _ = CompleteAwaitSpawnPlacementAsync(
                system,
                context.Self,
                replyTo,
                brainId,
                requestedTimeoutMs,
                transportTimeoutMs,
                startedAtTimestamp);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"AwaitSpawnPlacementViaIO failed: {ex.Message}");
            var ack = BuildSpawnFailureAck(
                reasonCode: "spawn_request_failed",
                failureMessage: $"Spawn wait failed: request forwarding to HiveMind failed ({ex.GetBaseException().Message}).");
            RecordAwaitSpawnPlacementTelemetry(ack, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero);
            context.Respond(new AwaitSpawnPlacementViaIOAck
            {
                Ack = ack,
                FailureReasonCode = ack.FailureReasonCode,
                FailureMessage = ack.FailureMessage
            });
        }
    }

    private async Task CompleteAwaitSpawnPlacementAsync(
        ActorSystem system,
        PID self,
        PID? replyTo,
        Guid brainId,
        ulong requestedTimeoutMs,
        int transportTimeoutMs,
        long startedAtTimestamp)
    {
        ProtoControl.SpawnBrainAck ack;
        var hiveMindWait = TimeSpan.Zero;
        var metadataVisibilityWait = TimeSpan.Zero;
        try
        {
            ack = await system.Root.RequestAsync<ProtoControl.SpawnBrainAck>(
                    _hiveMindPid!,
                    new ProtoControl.AwaitSpawnPlacement
                    {
                        BrainId = brainId.ToProtoUuid(),
                        TimeoutMs = requestedTimeoutMs
                    },
                    TimeSpan.FromMilliseconds(transportTimeoutMs))
                .ConfigureAwait(false)
                ?? BuildSpawnFailureAck(
                    reasonCode: "spawn_empty_response",
                    failureMessage: "Spawn wait failed: HiveMind returned an empty placement acknowledgment.");
            var hiveMindCompletedAtTimestamp = Stopwatch.GetTimestamp();
            hiveMindWait = Stopwatch.GetElapsedTime(startedAtTimestamp, hiveMindCompletedAtTimestamp);

            if (ack.AcceptedForPlacement
                && ack.PlacementReady
                && ack.BrainId.TryToGuid(out var awaitedBrainId)
                && awaitedBrainId != Guid.Empty)
            {
                var visibilityTimeout = ResolveBrainInfoVisibilityTimeout(requestedTimeoutMs, hiveMindWait);
                try
                {
                    var register = await TryBuildBootstrapRegisterBrainRegistrationAsync(system, awaitedBrainId, visibilityTimeout)
                        .ConfigureAwait(false);
                    metadataVisibilityWait = Stopwatch.GetElapsedTime(hiveMindCompletedAtTimestamp);
                    if (register is null)
                    {
                        Console.WriteLine(
                            $"AwaitSpawnPlacementViaIO metadata not visible after placement: brain={awaitedBrainId:N} visibility={FormatElapsed(visibilityTimeout)} total={FormatElapsed(Stopwatch.GetElapsedTime(startedAtTimestamp))} hivemind={FormatElapsed(hiveMindWait)} metadata={FormatElapsed(metadataVisibilityWait)}");
                    }
                    else
                    {
                        system.Root.Send(self, register);
                        MaybeLogSlowPlacementVisibility(
                            awaitedBrainId,
                            Stopwatch.GetElapsedTime(startedAtTimestamp),
                            hiveMindWait,
                            metadataVisibilityWait,
                            ack.PlacementReady);
                    }
                }
                catch (Exception registerEx)
                {
                    Console.WriteLine($"AwaitSpawnPlacementViaIO metadata resolve failed: {registerEx.GetBaseException().Message}");
                    metadataVisibilityWait = Stopwatch.GetElapsedTime(hiveMindCompletedAtTimestamp);
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"AwaitSpawnPlacementViaIO failed: {ex.Message}");
            ack = BuildSpawnFailureAck(
                reasonCode: "spawn_request_failed",
                failureMessage: $"Spawn wait failed: request forwarding to HiveMind failed ({ex.GetBaseException().Message}).");
        }

        RecordAwaitSpawnPlacementTelemetry(ack, Stopwatch.GetElapsedTime(startedAtTimestamp), hiveMindWait, metadataVisibilityWait);
        var response = new AwaitSpawnPlacementViaIOAck
        {
            Ack = ack.Clone(),
            FailureReasonCode = ack.FailureReasonCode,
            FailureMessage = ack.FailureMessage
        };
        if (replyTo is not null)
        {
            system.Root.Send(replyTo, response);
        }
    }

    private static TimeSpan ResolveBrainInfoVisibilityTimeout(ulong requestedTimeoutMs, TimeSpan hiveMindWait)
    {
        if (requestedTimeoutMs == 0)
        {
            return BrainInfoResolveTimeout;
        }

        var requestedTimeout = TimeSpan.FromMilliseconds(Math.Min(requestedTimeoutMs, (ulong)long.MaxValue));
        var remaining = requestedTimeout - hiveMindWait;
        return remaining <= TimeSpan.FromMilliseconds(100)
            ? TimeSpan.FromMilliseconds(100)
            : remaining;
    }

    private async Task<RegisterBrain?> TryBuildBootstrapRegisterBrainRegistrationAsync(
        ActorSystem system,
        Guid brainId,
        TimeSpan requestTimeout)
    {
        if (_hiveMindPid is null)
        {
            return null;
        }

        var info = await system.Root.RequestAsync<ProtoControl.BrainIoInfo>(
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

        return new RegisterBrain
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
    }

    private static void RecordAwaitSpawnPlacementTelemetry(
        ProtoControl.SpawnBrainAck ack,
        TimeSpan totalWait,
        TimeSpan hiveMindWait,
        TimeSpan metadataVisibilityWait)
    {
        Guid? brainId = null;
        if (ack.BrainId?.TryToGuid(out var parsedBrainId) == true && parsedBrainId != Guid.Empty)
        {
            brainId = parsedBrainId;
        }

        var outcome = string.IsNullOrWhiteSpace(ack.FailureReasonCode)
            ? ack.PlacementReady
                ? "ready"
                : "not_ready"
            : ack.FailureReasonCode;
        IoTelemetry.RecordAwaitSpawnPlacement(
            brainId,
            outcome,
            ack.PlacementReady,
            totalWait,
            hiveMindWait,
            metadataVisibilityWait);
    }

    private static void MaybeLogSlowPlacementVisibility(
        Guid brainId,
        TimeSpan totalWait,
        TimeSpan hiveMindWait,
        TimeSpan metadataVisibilityWait,
        bool placementReady)
    {
        if (totalWait < TimeSpan.FromSeconds(2)
            && metadataVisibilityWait < TimeSpan.FromSeconds(1))
        {
            return;
        }

        Console.WriteLine(
            $"AwaitSpawnPlacementViaIO slow path: brain={brainId:N} placement_ready={placementReady} total={FormatElapsed(totalWait)} hivemind={FormatElapsed(hiveMindWait)} metadata={FormatElapsed(metadataVisibilityWait)}");
    }

    private static string FormatElapsed(TimeSpan elapsed)
        => $"{elapsed.TotalMilliseconds:0}ms";

    private void HandleKillBrain(IContext context, KillBrainViaIO message)
    {
        if (_hiveMindPid is null)
        {
            context.Respond(new KillBrainViaIOAck
            {
                Accepted = false,
                FailureReasonCode = "kill_unavailable",
                FailureMessage = "Kill failed: HiveMind endpoint is not configured."
            });
            return;
        }

        var request = message.Request;
        if (request is null || !TryGetBrainId(request.BrainId, out _))
        {
            context.Respond(new KillBrainViaIOAck
            {
                Accepted = false,
                FailureReasonCode = "kill_invalid_request",
                FailureMessage = "Kill failed: brain_id is required."
            });
            return;
        }

        try
        {
            context.Request(_hiveMindPid, request);
            context.Respond(new KillBrainViaIOAck
            {
                Accepted = true,
                FailureReasonCode = string.Empty,
                FailureMessage = string.Empty
            });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"KillBrainViaIO failed: {ex.Message}");
            context.Respond(new KillBrainViaIOAck
            {
                Accepted = false,
                FailureReasonCode = "kill_request_failed",
                FailureMessage = $"Kill failed: request forwarding to HiveMind failed ({ex.GetBaseException().Message})."
            });
        }
    }

    private async Task HandlePauseBrain(IContext context, ProtoControl.PauseBrain message)
    {
        if (_hiveMindPid is null)
        {
            RespondCommandAck(context, message.BrainId, "pause_brain", success: false, "pause_unavailable");
            return;
        }

        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            RespondCommandAck(context, message.BrainId, "pause_brain", success: false, "pause_invalid_request");
            return;
        }

        try
        {
            var ack = await context.RequestAsync<IoCommandAck>(
                    _hiveMindPid,
                    new PauseBrainRequest(brainId, message.Reason),
                    DefaultRequestTimeout)
                .ConfigureAwait(false);
            if (ack is null)
            {
                RespondCommandAck(context, message.BrainId, "pause_brain", success: false, "pause_empty_response");
                return;
            }

            context.Respond(ack);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"PauseBrain failed: {ex.Message}");
            RespondCommandAck(
                context,
                message.BrainId,
                "pause_brain",
                success: false,
                $"pause_request_failed:{ex.GetBaseException().Message}");
        }
    }

    private async Task HandleResumeBrain(IContext context, ProtoControl.ResumeBrain message)
    {
        if (_hiveMindPid is null)
        {
            RespondCommandAck(context, message.BrainId, "resume_brain", success: false, "resume_unavailable");
            return;
        }

        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            RespondCommandAck(context, message.BrainId, "resume_brain", success: false, "resume_invalid_request");
            return;
        }

        try
        {
            var ack = await context.RequestAsync<IoCommandAck>(
                    _hiveMindPid,
                    new ResumeBrainRequest(brainId),
                    DefaultRequestTimeout)
                .ConfigureAwait(false);
            if (ack is null)
            {
                RespondCommandAck(context, message.BrainId, "resume_brain", success: false, "resume_empty_response");
                return;
            }

            context.Respond(ack);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"ResumeBrain failed: {ex.Message}");
            RespondCommandAck(
                context,
                message.BrainId,
                "resume_brain",
                success: false,
                $"resume_request_failed:{ex.GetBaseException().Message}");
        }
    }

    private async Task HandleSetOutputVectorSourceAsync(IContext context, SetOutputVectorSource message)
    {
        if (message.BrainId is not null && !TryGetBrainId(message.BrainId, out _))
        {
            context.Respond(new SetOutputVectorSourceAck
            {
                Success = false,
                FailureReasonCode = "output_vector_source_invalid_brain_id",
                FailureMessage = "Output vector source update failed: brain_id is invalid.",
                OutputVectorSource = DefaultOutputVectorSource,
                BrainId = message.BrainId.Clone()
            });
            return;
        }

        if (_hiveMindPid is null)
        {
            context.Respond(new SetOutputVectorSourceAck
            {
                Success = false,
                FailureReasonCode = "output_vector_source_unavailable",
                FailureMessage = "Output vector source update failed: HiveMind endpoint is not configured.",
                OutputVectorSource = DefaultOutputVectorSource,
                BrainId = message.BrainId?.Clone()
            });
            return;
        }

        try
        {
            var ack = await context.RequestAsync<ProtoControl.SetOutputVectorSourceAck>(
                _hiveMindPid,
                new ProtoControl.SetOutputVectorSource
                {
                    OutputVectorSource = message.OutputVectorSource,
                    BrainId = message.BrainId?.Clone()
                },
                DefaultRequestTimeout);
            if (ack is null)
            {
                context.Respond(new SetOutputVectorSourceAck
                {
                    Success = false,
                    FailureReasonCode = "output_vector_source_empty_response",
                    FailureMessage = "Output vector source update failed: HiveMind returned an empty acknowledgment.",
                    OutputVectorSource = DefaultOutputVectorSource,
                    BrainId = message.BrainId?.Clone()
                });
                return;
            }

            context.Respond(new SetOutputVectorSourceAck
            {
                Success = ack.Accepted,
                FailureReasonCode = ack.Accepted ? string.Empty : "output_vector_source_rejected",
                FailureMessage = ack.Message ?? string.Empty,
                OutputVectorSource = ack.OutputVectorSource,
                BrainId = ack.BrainId?.Clone()
            });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"SetOutputVectorSource failed: {ex.Message}");
            context.Respond(new SetOutputVectorSourceAck
            {
                Success = false,
                FailureReasonCode = "output_vector_source_request_failed",
                FailureMessage = $"Output vector source update failed: request forwarding to HiveMind failed ({ex.GetBaseException().Message}).",
                OutputVectorSource = DefaultOutputVectorSource,
                BrainId = message.BrainId?.Clone()
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

    private async Task RegisterBrainAsync(IContext context, RegisterBrain message, bool bootstrapOnly = false)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            return;
        }

        var inputCoordinatorMode = NormalizeInputCoordinatorMode(message.InputCoordinatorMode);
        var outputVectorSource = NormalizeOutputVectorSource(message.OutputVectorSource);
        var hasRegisteredInputPid = TryParsePid(message.InputCoordinatorPid, out var registeredInputPid)
                                    && registeredInputPid is not null;
        var hasRegisteredOutputPid = TryParsePid(message.OutputCoordinatorPid, out var registeredOutputPid)
                                     && registeredOutputPid is not null;
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
                ApplyRuntimeConfig(existing.Energy, message);
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

            if (!bootstrapOnly)
            {
                await SynchronizeCoordinatorConfigurationAsync(
                        context,
                        existing,
                        updateInputWidth: inputWidthChanged || inputCoordinatorChanged,
                        updateInputMode: inputModeChanged || inputCoordinatorChanged,
                        updateOutputWidth: outputWidthChanged || outputCoordinatorChanged)
                    .ConfigureAwait(false);

                if (inputCoordinatorChanged)
                {
                    await ReplayInputStateAsync(context, existing).ConfigureAwait(false);
                }

                if (outputCoordinatorChanged)
                {
                    await ReplayOutputSubscriptionsAsync(context, existing).ConfigureAwait(false);
                }

                if ((inputModeChanged || inputCoordinatorChanged)
                    && _routerCache.TryGetValue(brainId, out var routerPid))
                {
                    RegisterIoGatewayPid(context, brainId, routerPid, force: true);
                }

                if (MergePendingOutputSubscriptions(existing))
                {
                    await ReplayOutputSubscriptionsAsync(context, existing).ConfigureAwait(false);
                }
            }
            else
            {
                MergePendingOutputSubscriptions(existing);
            }

            if (LogMetadataDiagnostics)
            {
                Console.WriteLine(
                    $"[IoGatewayMeta] RegisterBrain update brain={brainId} input={existing.InputWidth} output={existing.OutputWidth} base={ArtifactLabel(existing.BaseDefinition)} snapshot={ArtifactLabel(existing.LastSnapshot)} runtimeConfig={message.HasRuntimeConfig}");
            }

            if (bootstrapOnly)
            {
                return;
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
            ApplyRuntimeConfig(energy, message);
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

        if (bootstrapOnly)
        {
            if (LogMetadataDiagnostics)
            {
                Console.WriteLine(
                    $"[IoGatewayMeta] RegisterBrain bootstrap add brain={brainId} input={entry.InputWidth} output={entry.OutputWidth} base={ArtifactLabel(entry.BaseDefinition)} snapshot={ArtifactLabel(entry.LastSnapshot)}");
            }
            return;
        }

        await SynchronizeCoordinatorConfigurationAsync(
                context,
                entry,
                updateInputWidth: true,
                updateInputMode: true,
                updateOutputWidth: true)
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

    private static void ApplyRuntimeConfig(BrainEnergyState energy, RegisterBrain message)
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

    private static async Task SynchronizeCoordinatorConfigurationAsync(
        IContext context,
        BrainIoEntry entry,
        bool updateInputWidth,
        bool updateInputMode,
        bool updateOutputWidth)
    {
        if (updateInputWidth)
        {
            await DispatchCoordinatorMessageAsync(
                    context,
                    entry.InputPid,
                    new UpdateInputWidth(entry.InputWidth))
                .ConfigureAwait(false);
        }

        if (updateInputMode)
        {
            await DispatchCoordinatorMessageAsync(
                    context,
                    entry.InputPid,
                    new UpdateInputCoordinatorMode(entry.InputCoordinatorMode))
                .ConfigureAwait(false);
        }

        if (updateOutputWidth)
        {
            await DispatchCoordinatorMessageAsync(
                    context,
                    entry.OutputPid,
                    new UpdateOutputWidth(entry.OutputWidth))
                .ConfigureAwait(false);
        }
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
                    Snapshot = entry.LastSnapshot,
                    SnapshotSource = "cached_last_snapshot"
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
                    Snapshot = entry.LastSnapshot,
                    SnapshotSource = "cached_last_snapshot"
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
                    Snapshot = entry.LastSnapshot,
                    SnapshotSource = "cached_last_snapshot"
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
            FailureMessage = normalizedFailureMessage,
            AcceptedForPlacement = false,
            PlacementReady = false,
            PlacementEpoch = 0,
            LifecycleState = ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
            ReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileFailed
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
        _routerRegistrationTargetCache.Remove(entry.BrainId);
        _routerRegistration.Remove(entry.BrainId);
        _pendingOutputSubscribers.Remove(entry.BrainId);
        _pendingOutputVectorSubscribers.Remove(entry.BrainId);
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
