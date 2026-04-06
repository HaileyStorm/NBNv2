using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.IO;

public sealed partial class IoGatewayActor
{
    private void ApplyEnergyCredit(IContext context, EnergyCredit message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            RespondCommandAck(context, message.BrainId, "energy_credit", success: false, "brain_not_found");
            return;
        }

        entry.Energy.ApplyCredit(message.Amount);
        if (entry.Energy.EnergyRemaining >= 0)
        {
            entry.EnergyDepletedSignaled = false;
        }

        RespondCommandAck(context, message.BrainId, "energy_credit", success: true, "applied", entry);
    }

    private void ApplyEnergyRate(IContext context, EnergyRate message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            RespondCommandAck(context, message.BrainId, "energy_rate", success: false, "brain_not_found");
            return;
        }

        entry.Energy.SetEnergyRate(message.UnitsPerSecond);
        RespondCommandAck(context, message.BrainId, "energy_rate", success: true, "applied", entry);
    }

    private void ApplyCostEnergyFlags(IContext context, SetCostEnergyEnabled message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            RespondCommandAck(context, message.BrainId, "set_cost_energy", success: false, "brain_not_found");
            return;
        }

        var enabled = message.CostEnabled && message.EnergyEnabled;
        entry.Energy.SetCostEnergyEnabled(enabled, enabled);
        Console.WriteLine(
            $"Cost/Energy override applied for brain {entry.BrainId}: enabled={enabled} remaining={entry.Energy.EnergyRemaining} rate={entry.Energy.EnergyRateUnitsPerSecond}/s");

        var ackMessage = "applied";
        if (_hiveMindPid is not null)
        {
            context.Request(_hiveMindPid, new ProtoControl.SetBrainCostEnergy
            {
                BrainId = message.BrainId,
                CostEnabled = enabled,
                EnergyEnabled = enabled
            });
        }
        else
        {
            ackMessage = "applied_local_only_hivemind_unavailable";
        }

        RespondCommandAck(context, message.BrainId, "set_cost_energy", success: true, ackMessage, entry);
    }

    private void ApplyPlasticityFlags(IContext context, SetPlasticityEnabled message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            RespondCommandAck(context, message.BrainId, "set_plasticity", success: false, "brain_not_found");
            return;
        }

        if (!float.IsFinite(message.PlasticityRate) || message.PlasticityRate < 0f)
        {
            RespondCommandAck(context, message.BrainId, "set_plasticity", success: false, "plasticity_rate_invalid", entry);
            return;
        }

        if (!float.IsFinite(message.PlasticityDelta) || message.PlasticityDelta < 0f)
        {
            RespondCommandAck(context, message.BrainId, "set_plasticity", success: false, "plasticity_delta_invalid", entry);
            return;
        }

        if (!IsFiniteInRange(message.PlasticityRebaseThresholdPct, 0f, 1f))
        {
            RespondCommandAck(context, message.BrainId, "set_plasticity", success: false, "plasticity_rebase_threshold_pct_invalid", entry);
            return;
        }

        if (!TryNormalizePlasticityEnergyCostModulation(
                message.PlasticityEnergyCostModulationEnabled,
                message.PlasticityEnergyCostReferenceTickCost,
                message.PlasticityEnergyCostResponseStrength,
                message.PlasticityEnergyCostMinScale,
                message.PlasticityEnergyCostMaxScale,
                out var modulationReferenceTickCost,
                out var modulationResponseStrength,
                out var modulationMinScale,
                out var modulationMaxScale))
        {
            RespondCommandAck(context, message.BrainId, "set_plasticity", success: false, "plasticity_energy_cost_modulation_invalid", entry);
            return;
        }

        var effectiveDelta = ResolvePlasticityDelta(message.PlasticityRate, message.PlasticityDelta);
        var configuredPlasticityEnabled = message.PlasticityEnabled;
        var effectivePlasticityEnabled = entry.Energy.PlasticityEnabled;
        var ackMessage = "accepted_pending_authoritative_state";
        if (_hiveMindPid is not null)
        {
            context.Request(_hiveMindPid, new ProtoControl.SetBrainPlasticity
            {
                BrainId = message.BrainId,
                PlasticityEnabled = message.PlasticityEnabled,
                PlasticityRate = message.PlasticityRate,
                ProbabilisticUpdates = message.ProbabilisticUpdates,
                PlasticityDelta = effectiveDelta,
                PlasticityRebaseThreshold = message.PlasticityRebaseThreshold,
                PlasticityRebaseThresholdPct = message.PlasticityRebaseThresholdPct,
                PlasticityEnergyCostModulationEnabled = message.PlasticityEnergyCostModulationEnabled,
                PlasticityEnergyCostReferenceTickCost = modulationReferenceTickCost,
                PlasticityEnergyCostResponseStrength = modulationResponseStrength,
                PlasticityEnergyCostMinScale = modulationMinScale,
                PlasticityEnergyCostMaxScale = modulationMaxScale
            });
        }
        else
        {
            entry.Energy.SetPlasticity(
                message.PlasticityEnabled,
                message.PlasticityRate,
                message.ProbabilisticUpdates,
                effectiveDelta,
                message.PlasticityRebaseThreshold,
                message.PlasticityRebaseThresholdPct,
                message.PlasticityEnergyCostModulationEnabled,
                modulationReferenceTickCost,
                modulationResponseStrength,
                modulationMinScale,
                modulationMaxScale);
            ackMessage = "applied_local_only_hivemind_unavailable";
            effectivePlasticityEnabled = entry.Energy.PlasticityEnabled;
        }

        RespondCommandAck(
            context,
            message.BrainId,
            "set_plasticity",
            success: true,
            ackMessage,
            entry,
            configuredPlasticityEnabled,
            effectivePlasticityEnabled);
    }

    private void ApplyHomeostasisFlags(IContext context, SetHomeostasisEnabled message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            RespondCommandAck(context, message.BrainId, "set_homeostasis", success: false, "brain_not_found");
            return;
        }

        if (!float.IsFinite(message.HomeostasisBaseProbability)
            || message.HomeostasisBaseProbability < 0f
            || message.HomeostasisBaseProbability > 1f)
        {
            RespondCommandAck(context, message.BrainId, "set_homeostasis", success: false, "homeostasis_probability_invalid", entry);
            return;
        }

        if (!IsSupportedHomeostasisTargetMode(message.HomeostasisTargetMode))
        {
            RespondCommandAck(context, message.BrainId, "set_homeostasis", success: false, "homeostasis_target_mode_invalid", entry);
            return;
        }

        if (message.HomeostasisUpdateMode != ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep)
        {
            RespondCommandAck(context, message.BrainId, "set_homeostasis", success: false, "homeostasis_update_mode_invalid", entry);
            return;
        }

        if (message.HomeostasisMinStepCodes == 0)
        {
            RespondCommandAck(context, message.BrainId, "set_homeostasis", success: false, "homeostasis_min_step_codes_invalid", entry);
            return;
        }

        if (!IsFiniteInRange(message.HomeostasisEnergyTargetScale, 0f, 4f))
        {
            RespondCommandAck(context, message.BrainId, "set_homeostasis", success: false, "homeostasis_energy_target_scale_invalid", entry);
            return;
        }

        if (!IsFiniteInRange(message.HomeostasisEnergyProbabilityScale, 0f, 4f))
        {
            RespondCommandAck(context, message.BrainId, "set_homeostasis", success: false, "homeostasis_energy_probability_scale_invalid", entry);
            return;
        }

        entry.Energy.SetHomeostasis(
            message.HomeostasisEnabled,
            message.HomeostasisTargetMode,
            message.HomeostasisUpdateMode,
            message.HomeostasisBaseProbability,
            message.HomeostasisMinStepCodes,
            message.HomeostasisEnergyCouplingEnabled,
            message.HomeostasisEnergyTargetScale,
            message.HomeostasisEnergyProbabilityScale);

        var ackMessage = "applied";
        if (_hiveMindPid is not null)
        {
            context.Request(_hiveMindPid, new ProtoControl.SetBrainHomeostasis
            {
                BrainId = message.BrainId,
                HomeostasisEnabled = message.HomeostasisEnabled,
                HomeostasisTargetMode = message.HomeostasisTargetMode,
                HomeostasisUpdateMode = message.HomeostasisUpdateMode,
                HomeostasisBaseProbability = message.HomeostasisBaseProbability,
                HomeostasisMinStepCodes = message.HomeostasisMinStepCodes,
                HomeostasisEnergyCouplingEnabled = message.HomeostasisEnergyCouplingEnabled,
                HomeostasisEnergyTargetScale = message.HomeostasisEnergyTargetScale,
                HomeostasisEnergyProbabilityScale = message.HomeostasisEnergyProbabilityScale
            });
        }
        else
        {
            ackMessage = "applied_local_only_hivemind_unavailable";
        }

        RespondCommandAck(context, message.BrainId, "set_homeostasis", success: true, ackMessage, entry);
    }

    private async Task HandleSynchronizeBrainRuntimeConfigAsync(IContext context, SynchronizeBrainRuntimeConfig message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            RespondCommandAck(context, message.BrainId, "sync_brain_runtime_config", success: false, "invalid_brain_id");
            return;
        }

        if (!_brains.TryGetValue(brainId, out var entry))
        {
            RespondCommandAck(context, message.BrainId, "sync_brain_runtime_config", success: false, "brain_not_found");
            return;
        }

        if (_hiveMindPid is null)
        {
            RespondCommandAck(context, message.BrainId, "sync_brain_runtime_config", success: false, "hivemind_unavailable", entry);
            return;
        }

        try
        {
            var ack = await context.RequestAsync<IoCommandAck>(
                    _hiveMindPid,
                    new ProtoControl.SynchronizeBrainRuntimeConfig
                    {
                        BrainId = message.BrainId.Clone()
                    },
                    DefaultRequestTimeout)
                .ConfigureAwait(false);
            if (ack is null)
            {
                RespondCommandAck(context, message.BrainId, "sync_brain_runtime_config", success: false, "empty_response", entry);
                return;
            }

            context.Respond(ack);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"SynchronizeBrainRuntimeConfig failed: {ex.Message}");
            RespondCommandAck(
                context,
                message.BrainId,
                "sync_brain_runtime_config",
                success: false,
                $"request_failed:{ex.GetBaseException().Message}",
                entry);
        }
    }

    private void ApplyTickCost(IContext context, ApplyTickCost message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var entry))
        {
            return;
        }

        if (entry.LastAppliedTickCostId is ulong lastAppliedTickCostId && message.TickId <= lastAppliedTickCostId)
        {
            return;
        }

        entry.LastAppliedTickCostId = message.TickId;

        if (!entry.Energy.CostEnabled)
        {
            return;
        }

        entry.Energy.ApplyTickCost(message.TickCost);
        if (!entry.Energy.EnergyEnabled || entry.Energy.EnergyRemaining >= 0)
        {
            return;
        }

        if (entry.EnergyDepletedSignaled)
        {
            return;
        }

        entry.EnergyDepletedSignaled = true;
        Console.WriteLine(
            $"Energy depleted for brain {entry.BrainId}: tick={message.TickId} remaining={entry.Energy.EnergyRemaining} last_tick_cost={entry.Energy.LastTickCost} rate={entry.Energy.EnergyRateUnitsPerSecond}/s");

        if (_hiveMindPid is null)
        {
            var terminated = BuildEnergyTerminated(entry, message.TickCost);
            BroadcastToClients(context, terminated);
            StopAndRemoveBrain(context, entry);
            return;
        }

        context.Request(_hiveMindPid, new ProtoControl.KillBrain
        {
            BrainId = entry.BrainId.ToProtoUuid(),
            Reason = "energy_exhausted"
        });
    }

    private static ProtoControl.InputCoordinatorMode NormalizeInputCoordinatorMode(ProtoControl.InputCoordinatorMode mode)
    {
        return mode switch
        {
            ProtoControl.InputCoordinatorMode.ReplayLatestVector => mode,
            _ => DefaultInputCoordinatorMode
        };
    }

    private static ProtoControl.OutputVectorSource NormalizeOutputVectorSource(ProtoControl.OutputVectorSource source)
    {
        return source switch
        {
            ProtoControl.OutputVectorSource.Buffer => source,
            _ => DefaultOutputVectorSource
        };
    }

    private static bool IsSupportedHomeostasisTargetMode(ProtoControl.HomeostasisTargetMode mode)
    {
        return mode == ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero
               || mode == ProtoControl.HomeostasisTargetMode.HomeostasisTargetFixed;
    }

    private static bool IsFiniteInRange(float value, float min, float max)
    {
        return float.IsFinite(value) && value >= min && value <= max;
    }

    private static bool TryNormalizePlasticityEnergyCostModulation(
        bool enabled,
        long referenceTickCost,
        float responseStrength,
        float minScale,
        float maxScale,
        out long normalizedReferenceTickCost,
        out float normalizedResponseStrength,
        out float normalizedMinScale,
        out float normalizedMaxScale)
    {
        normalizedReferenceTickCost = DefaultPlasticityEnergyCostReferenceTickCost;
        normalizedResponseStrength = DefaultPlasticityEnergyCostResponseStrength;
        normalizedMinScale = DefaultPlasticityEnergyCostMinScale;
        normalizedMaxScale = DefaultPlasticityEnergyCostMaxScale;

        if (!enabled)
        {
            var hasExplicitConfiguration = referenceTickCost > 0
                                           || (float.IsFinite(responseStrength) && responseStrength > 0f)
                                           || (float.IsFinite(minScale) && minScale > 0f)
                                           || (float.IsFinite(maxScale) && maxScale > 0f);
            if (!hasExplicitConfiguration)
            {
                return true;
            }

            if (referenceTickCost > 0)
            {
                normalizedReferenceTickCost = referenceTickCost;
            }

            if (float.IsFinite(responseStrength) && responseStrength >= 0f)
            {
                normalizedResponseStrength = Math.Clamp(responseStrength, 0f, 8f);
            }

            var hasExplicitScale = float.IsFinite(minScale)
                                   && float.IsFinite(maxScale)
                                   && (minScale > 0f || maxScale > 0f);
            if (hasExplicitScale)
            {
                normalizedMinScale = Math.Clamp(minScale, 0f, 1f);
                normalizedMaxScale = Math.Clamp(maxScale, 0f, 1f);
                if (normalizedMaxScale < normalizedMinScale)
                {
                    normalizedMaxScale = normalizedMinScale;
                }
            }

            return true;
        }

        if (referenceTickCost <= 0
            || !IsFiniteInRange(responseStrength, 0f, 8f)
            || !IsFiniteInRange(minScale, 0f, 1f)
            || !IsFiniteInRange(maxScale, 0f, 1f)
            || maxScale < minScale)
        {
            return false;
        }

        normalizedReferenceTickCost = referenceTickCost;
        normalizedResponseStrength = responseStrength;
        normalizedMinScale = minScale;
        normalizedMaxScale = maxScale;
        return true;
    }

    private static float ResolvePlasticityDelta(float plasticityRate, float plasticityDelta)
    {
        if (plasticityDelta > 0f)
        {
            return plasticityDelta;
        }

        return plasticityRate > 0f ? plasticityRate : 0f;
    }

    private static Nbn.Proto.Io.BrainEnergyState BuildCommandEnergyState(BrainEnergyState energy)
    {
        return new Nbn.Proto.Io.BrainEnergyState
        {
            EnergyRemaining = energy.EnergyRemaining,
            EnergyRateUnitsPerSecond = energy.EnergyRateUnitsPerSecond,
            CostEnabled = energy.CostEnabled,
            EnergyEnabled = energy.EnergyEnabled,
            PlasticityEnabled = energy.PlasticityEnabled,
            PlasticityRate = energy.PlasticityRate,
            PlasticityProbabilisticUpdates = energy.PlasticityProbabilisticUpdates,
            PlasticityDelta = energy.PlasticityDelta,
            PlasticityRebaseThreshold = energy.PlasticityRebaseThreshold,
            PlasticityRebaseThresholdPct = energy.PlasticityRebaseThresholdPct,
            PlasticityEnergyCostModulationEnabled = energy.PlasticityEnergyCostModulationEnabled,
            PlasticityEnergyCostReferenceTickCost = energy.PlasticityEnergyCostReferenceTickCost,
            PlasticityEnergyCostResponseStrength = energy.PlasticityEnergyCostResponseStrength,
            PlasticityEnergyCostMinScale = energy.PlasticityEnergyCostMinScale,
            PlasticityEnergyCostMaxScale = energy.PlasticityEnergyCostMaxScale,
            HomeostasisEnabled = energy.HomeostasisEnabled,
            HomeostasisTargetMode = energy.HomeostasisTargetMode,
            HomeostasisUpdateMode = energy.HomeostasisUpdateMode,
            HomeostasisBaseProbability = energy.HomeostasisBaseProbability,
            HomeostasisMinStepCodes = energy.HomeostasisMinStepCodes,
            HomeostasisEnergyCouplingEnabled = energy.HomeostasisEnergyCouplingEnabled,
            HomeostasisEnergyTargetScale = energy.HomeostasisEnergyTargetScale,
            HomeostasisEnergyProbabilityScale = energy.HomeostasisEnergyProbabilityScale,
            LastTickCost = energy.LastTickCost
        };
    }

    private static void RespondCommandAck(
        IContext context,
        Uuid? brainId,
        string command,
        bool success,
        string message,
        BrainIoEntry? entry = null,
        bool? configuredPlasticityEnabled = null,
        bool? effectivePlasticityEnabled = null)
    {
        if (context.Sender is null)
        {
            return;
        }

        var ack = new IoCommandAck
        {
            BrainId = brainId ?? Guid.Empty.ToProtoUuid(),
            Command = command,
            Success = success,
            Message = message ?? string.Empty,
            HasEnergyState = entry is not null,
            HasConfiguredPlasticityEnabled = configuredPlasticityEnabled.HasValue,
            ConfiguredPlasticityEnabled = configuredPlasticityEnabled.GetValueOrDefault(),
            HasEffectivePlasticityEnabled = effectivePlasticityEnabled.HasValue,
            EffectivePlasticityEnabled = effectivePlasticityEnabled.GetValueOrDefault()
        };

        if (entry is not null)
        {
            ack.EnergyState = BuildCommandEnergyState(entry.Energy);
        }

        context.Respond(ack);
    }

    private ProtoControl.BrainTerminated BuildEnergyTerminated(BrainIoEntry entry, long lastTickCost)
    {
        return new ProtoControl.BrainTerminated
        {
            BrainId = entry.BrainId.ToProtoUuid(),
            Reason = "energy_exhausted",
            BaseDef = entry.BaseDefinition ?? new ArtifactRef(),
            LastSnapshot = entry.LastSnapshot ?? new ArtifactRef(),
            LastEnergyRemaining = entry.Energy.EnergyRemaining,
            LastTickCost = lastTickCost,
            TimeMs = (ulong)NowMs()
        };
    }

    private ProtoControl.BrainTerminated BuildTerminatedFromEntry(ProtoControl.BrainTerminated message, BrainIoEntry entry)
    {
        var baseDef = HasArtifactRef(message.BaseDef) ? message.BaseDef : entry.BaseDefinition ?? new ArtifactRef();
        var lastSnapshot = HasArtifactRef(message.LastSnapshot) ? message.LastSnapshot : entry.LastSnapshot ?? new ArtifactRef();
        var lastEnergyRemaining = entry.Energy.EnergyRemaining;
        var lastTickCost = entry.Energy.LastTickCost;
        var timeMs = message.TimeMs == 0 ? (ulong)NowMs() : message.TimeMs;

        return new ProtoControl.BrainTerminated
        {
            BrainId = message.BrainId,
            Reason = message.Reason,
            BaseDef = baseDef,
            LastSnapshot = lastSnapshot,
            LastEnergyRemaining = lastEnergyRemaining,
            LastTickCost = lastTickCost,
            TimeMs = timeMs
        };
    }
}
