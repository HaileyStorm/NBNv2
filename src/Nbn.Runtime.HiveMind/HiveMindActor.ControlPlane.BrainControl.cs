using Nbn.Shared;
using Nbn.Shared.HiveMind;
using Proto;
using ProtoIo = Nbn.Proto.Io;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private void KillBrain(IContext context, Guid brainId, string? reason)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return;
        }

        var terminationReason = string.IsNullOrWhiteSpace(reason) ? "killed" : reason.Trim();
        if (string.Equals(terminationReason, "energy_exhausted", StringComparison.OrdinalIgnoreCase))
        {
            HiveMindTelemetry.RecordEnergyDepleted(brainId);
            EmitDebug(context, ProtoSeverity.SevWarn, "energy.depleted", $"Brain {brainId} energy depleted.");
        }

        if (_ioPid is not null)
        {
            var ioPid = ResolveSendTargetPid(context, _ioPid);
            context.Send(ioPid, new ProtoControl.BrainTerminated
            {
                BrainId = brainId.ToProtoUuid(),
                Reason = terminationReason,
                BaseDef = brain.BaseDefinition ?? new Nbn.Proto.ArtifactRef(),
                LastSnapshot = brain.LastSnapshot ?? new Nbn.Proto.ArtifactRef(),
                LastEnergyRemaining = 0,
                LastTickCost = brain.LastTickCost,
                TimeMs = (ulong)NowMs()
            });
        }

        UnregisterBrain(context, brainId, terminationReason, notifyIoUnregister: false);
    }

    private void HandleRegisterOutputSink(IContext context, ProtoControl.RegisterOutputSink message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        if (!IsControlPlaneBrainMutationAuthorized(context, brainId, out var brain, out var reason))
        {
            EmitControlPlaneMutationIgnored(context, "control.register_output_sink", brainId, reason);
            HiveMindTelemetry.RecordOutputSinkMutationRejected(brainId, reason);
            return;
        }

        PID? outputPid = null;
        if (!string.IsNullOrWhiteSpace(message.OutputPid))
        {
            if (!TryParsePid(message.OutputPid, out var parsed))
            {
                return;
            }

            outputPid = parsed;
        }

        brain.OutputSinkPid = outputPid;
        UpdateOutputSinks(context, brain);
        if (outputPid is null)
        {
            Log($"Output sink cleared for brain {brainId}.");
        }
        else
        {
            Log($"Output sink registered for brain {brainId}: {PidLabel(outputPid)}");
        }
    }

    private void HandleSetBrainVisualization(IContext context, ProtoControl.SetBrainVisualization message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        if (!_brains.TryGetValue(brainId, out var brain))
        {
            const string missingReason = "brain_not_registered";
            EmitControlPlaneMutationIgnored(context, "control.set_brain_visualization", brainId, missingReason);
            HiveMindTelemetry.RecordSetBrainVisualizationRejected(brainId, missingReason);
            return;
        }

        if (context.Sender is not null
            && !IsControlPlaneBrainMutationAuthorized(context, brainId, out _, out var reason))
        {
            EmitControlPlaneMutationIgnored(context, "control.set_brain_visualization", brainId, reason);
            HiveMindTelemetry.RecordSetBrainVisualizationRejected(brainId, reason);
            return;
        }

        var focusRegionId = message.HasFocusRegion ? (uint?)message.FocusRegionId : null;
        var subscriber = ResolveVisualizationSubscriber(context, message);
        SetBrainVisualization(context, brain, subscriber, message.Enabled, focusRegionId);
    }

    private void HandleSetBrainCostEnergy(IContext context, ProtoControl.SetBrainCostEnergy message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        if (!IsControlPlaneBrainMutationAuthorized(context, brainId, out var brain, out var reason))
        {
            EmitControlPlaneMutationIgnored(context, "control.set_brain_cost_energy", brainId, reason);
            HiveMindTelemetry.RecordSetBrainCostEnergyRejected(brainId, reason);
            return;
        }

        var perBrainCostEnergyEnabled = message.CostEnabled && message.EnergyEnabled;
        if (ResolvePerBrainCostEnergyEnabled(brain) == perBrainCostEnergyEnabled)
        {
            return;
        }

        brain.CostEnergyEnabled = perBrainCostEnergyEnabled;
        UpdateShardRuntimeConfig(context, brain);
        RegisterBrainWithIo(context, brain, force: true);
    }

    private void HandleSetBrainPlasticity(IContext context, ProtoControl.SetBrainPlasticity message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        if (!IsControlPlaneBrainMutationAuthorized(context, brainId, out var brain, out var reason))
        {
            EmitControlPlaneMutationIgnored(context, "control.set_brain_plasticity", brainId, reason);
            HiveMindTelemetry.RecordSetBrainPlasticityRejected(brainId, reason);
            return;
        }

        if (!float.IsFinite(message.PlasticityRate)
            || message.PlasticityRate < 0f
            || !float.IsFinite(message.PlasticityDelta)
            || message.PlasticityDelta < 0f
            || !IsFiniteInRange(message.PlasticityRebaseThresholdPct, 0f, 1f))
        {
            EmitControlPlaneMutationIgnored(context, "control.set_brain_plasticity", brainId, "invalid_plasticity_config");
            HiveMindTelemetry.RecordSetBrainPlasticityRejected(brainId, "invalid_plasticity_config");
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
            EmitControlPlaneMutationIgnored(context, "control.set_brain_plasticity", brainId, "invalid_plasticity_config");
            HiveMindTelemetry.RecordSetBrainPlasticityRejected(brainId, "invalid_plasticity_config");
            return;
        }

        var effectiveDelta = ResolvePlasticityDelta(message.PlasticityRate, message.PlasticityDelta);
        if (brain.PlasticityEnabled == message.PlasticityEnabled
            && Math.Abs(brain.PlasticityRate - message.PlasticityRate) < 0.000001f
            && brain.PlasticityProbabilisticUpdates == message.ProbabilisticUpdates
            && Math.Abs(brain.PlasticityDelta - effectiveDelta) < 0.000001f
            && brain.PlasticityRebaseThreshold == message.PlasticityRebaseThreshold
            && Math.Abs(brain.PlasticityRebaseThresholdPct - message.PlasticityRebaseThresholdPct) < 0.000001f
            && brain.PlasticityEnergyCostModulationEnabled == message.PlasticityEnergyCostModulationEnabled
            && brain.PlasticityEnergyCostReferenceTickCost == modulationReferenceTickCost
            && Math.Abs(brain.PlasticityEnergyCostResponseStrength - modulationResponseStrength) < 0.000001f
            && Math.Abs(brain.PlasticityEnergyCostMinScale - modulationMinScale) < 0.000001f
            && Math.Abs(brain.PlasticityEnergyCostMaxScale - modulationMaxScale) < 0.000001f)
        {
            return;
        }

        brain.PlasticityEnabled = message.PlasticityEnabled;
        brain.PlasticityRate = message.PlasticityRate;
        brain.PlasticityProbabilisticUpdates = message.ProbabilisticUpdates;
        brain.PlasticityDelta = effectiveDelta;
        brain.PlasticityRebaseThreshold = message.PlasticityRebaseThreshold;
        brain.PlasticityRebaseThresholdPct = message.PlasticityRebaseThresholdPct;
        brain.PlasticityEnergyCostModulationEnabled = message.PlasticityEnergyCostModulationEnabled;
        brain.PlasticityEnergyCostReferenceTickCost = modulationReferenceTickCost;
        brain.PlasticityEnergyCostResponseStrength = modulationResponseStrength;
        brain.PlasticityEnergyCostMinScale = modulationMinScale;
        brain.PlasticityEnergyCostMaxScale = modulationMaxScale;
        UpdateShardRuntimeConfig(context, brain);
        RegisterBrainWithIo(context, brain, force: true);
    }

    private void HandleSetBrainHomeostasis(IContext context, ProtoControl.SetBrainHomeostasis message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        if (!IsControlPlaneBrainMutationAuthorized(context, brainId, out var brain, out var reason))
        {
            EmitControlPlaneMutationIgnored(context, "control.set_brain_homeostasis", brainId, reason);
            return;
        }

        if (!IsSupportedHomeostasisTargetMode(message.HomeostasisTargetMode)
            || message.HomeostasisUpdateMode != ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep
            || !float.IsFinite(message.HomeostasisBaseProbability)
            || message.HomeostasisBaseProbability < 0f
            || message.HomeostasisBaseProbability > 1f
            || message.HomeostasisMinStepCodes == 0
            || !IsFiniteInRange(message.HomeostasisEnergyTargetScale, 0f, 4f)
            || !IsFiniteInRange(message.HomeostasisEnergyProbabilityScale, 0f, 4f))
        {
            EmitControlPlaneMutationIgnored(context, "control.set_brain_homeostasis", brainId, "invalid_homeostasis_config");
            return;
        }

        if (brain.HomeostasisEnabled == message.HomeostasisEnabled
            && brain.HomeostasisTargetMode == message.HomeostasisTargetMode
            && brain.HomeostasisUpdateMode == message.HomeostasisUpdateMode
            && Math.Abs(brain.HomeostasisBaseProbability - message.HomeostasisBaseProbability) < 0.000001f
            && brain.HomeostasisMinStepCodes == message.HomeostasisMinStepCodes
            && brain.HomeostasisEnergyCouplingEnabled == message.HomeostasisEnergyCouplingEnabled
            && Math.Abs(brain.HomeostasisEnergyTargetScale - message.HomeostasisEnergyTargetScale) < 0.000001f
            && Math.Abs(brain.HomeostasisEnergyProbabilityScale - message.HomeostasisEnergyProbabilityScale) < 0.000001f)
        {
            return;
        }

        brain.HomeostasisEnabled = message.HomeostasisEnabled;
        brain.HomeostasisTargetMode = message.HomeostasisTargetMode;
        brain.HomeostasisUpdateMode = message.HomeostasisUpdateMode;
        brain.HomeostasisBaseProbability = message.HomeostasisBaseProbability;
        brain.HomeostasisMinStepCodes = message.HomeostasisMinStepCodes;
        brain.HomeostasisEnergyCouplingEnabled = message.HomeostasisEnergyCouplingEnabled;
        brain.HomeostasisEnergyTargetScale = message.HomeostasisEnergyTargetScale;
        brain.HomeostasisEnergyProbabilityScale = message.HomeostasisEnergyProbabilityScale;
        UpdateShardRuntimeConfig(context, brain);
        RegisterBrainWithIo(context, brain, force: true);
    }

    private void HandleDirectRuntimeRewardControl(
        IContext context,
        ProtoControl.DirectRuntimeRewardControlRequest message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            context.Respond(CreateDirectRuntimeRewardControlResponse(
                message,
                accepted: false,
                "invalid_brain_id",
                "Invalid brain id.",
                appliedTickFloor: _lastCompletedTickId + 1));
            return;
        }

        if (!_brains.TryGetValue(brainId, out var brain))
        {
            context.Respond(CreateDirectRuntimeRewardControlResponse(
                message,
                accepted: false,
                "brain_not_registered",
                "Direct runtime reward-control target brain is not registered.",
                appliedTickFloor: _lastCompletedTickId + 1));
            return;
        }

        if (!IsTrustedIoSender(context))
        {
            var reason = context.Sender is null ? "trusted_io_sender_missing" : "sender_not_trusted_io";
            EmitControlPlaneMutationIgnored(context, "control.direct_runtime_reward_control", brainId, reason);
            context.Respond(CreateDirectRuntimeRewardControlResponse(
                message,
                accepted: false,
                reason,
                "Direct runtime reward-control requests must enter through the trusted IO gateway.",
                appliedTickFloor: _lastCompletedTickId + 1));
            return;
        }

        if (!TryValidateDirectRuntimeRewardControlShape(message, out var failureReason, out var failureMessage))
        {
            context.Respond(CreateDirectRuntimeRewardControlResponse(
                message,
                accepted: false,
                failureReason,
                failureMessage,
                appliedTickFloor: _lastCompletedTickId + 1));
            return;
        }

        if (brain.PendingRuntimeReset is not null)
        {
            context.Respond(CreateDirectRuntimeRewardControlResponse(
                message,
                accepted: false,
                "runtime_reset_pending",
                "Direct runtime reward-control cannot be accepted while a runtime reset is pending for the brain.",
                appliedTickFloor: _lastCompletedTickId + 1));
            return;
        }

        var actionKey = BuildDirectRuntimeRewardControlActionKey(message);
        if (brain.DirectRuntimeRewardControlRecords.ContainsKey(actionKey)
            || brain.PendingDirectRuntimeRewardControls.ContainsKey(actionKey))
        {
            context.Respond(CreateDirectRuntimeRewardControlResponse(
                message,
                accepted: false,
                "duplicate_action",
                "Direct runtime reward-control action was already applied for this controller/action id.",
                appliedTickFloor: _lastCompletedTickId + 1));
            return;
        }

        if (HasConflictingDirectRuntimeRewardControlActionId(brain, message))
        {
            context.Respond(CreateDirectRuntimeRewardControlResponse(
                message,
                accepted: false,
                "duplicate_action",
                "Direct runtime reward-control action_id was already used with different reward/action provenance.",
                appliedTickFloor: _lastCompletedTickId + 1));
            return;
        }

        if (HasConflictingDirectRuntimeRewardControlSurfaceTick(brain, message))
        {
            context.Respond(CreateDirectRuntimeRewardControlResponse(
                message,
                accepted: false,
                "surface_action_tick_conflict",
                "Direct runtime reward-control surface already has an accepted action for this action tick.",
                appliedTickFloor: _lastCompletedTickId + 1));
            return;
        }

        if (HasStaleDirectRuntimeRewardControlAction(brain, message))
        {
            context.Respond(CreateDirectRuntimeRewardControlResponse(
                message,
                accepted: false,
                "stale_action",
                "Direct runtime reward-control action is stale for this controller/objective/reward/surface.",
                appliedTickFloor: _lastCompletedTickId + 1));
            return;
        }

        if (brain.PendingDirectRuntimeRewardControls.Count > 0)
        {
            context.Respond(CreateDirectRuntimeRewardControlResponse(
                message,
                accepted: false,
                "barrier_work_already_pending",
                "Direct runtime reward-control cannot queue while another direct runtime-control action is pending.",
                appliedTickFloor: ResolveDirectRuntimeRewardControlAppliedTickFloor(brain)));
            return;
        }

        if (brain.PendingRuntimeReset is not null)
        {
            context.Respond(CreateDirectRuntimeRewardControlResponse(
                message,
                accepted: false,
                "barrier_work_already_pending",
                "Direct runtime reward-control cannot queue while a runtime reset is pending for this brain.",
                appliedTickFloor: ResolveDirectRuntimeRewardControlAppliedTickFloor(brain)));
            return;
        }

        if (!TryValidateDirectRuntimeRewardControlTiming(message, brain, out failureReason, out failureMessage, out var appliedTickFloor))
        {
            context.Respond(CreateDirectRuntimeRewardControlResponse(
                message,
                accepted: false,
                failureReason,
                failureMessage,
                appliedTickFloor));
            return;
        }

        if (ShouldQueueDirectRuntimeRewardControl(brain))
        {
            if (brain.PendingRuntimeReset is not null)
            {
                context.Respond(CreateDirectRuntimeRewardControlResponse(
                    message,
                    accepted: false,
                    "barrier_work_already_pending",
                    "Direct runtime reward-control cannot queue while another barrier runtime-control action is pending.",
                    appliedTickFloor));
                return;
            }

            var pending = new PendingDirectRuntimeRewardControlState(actionKey, message, appliedTickFloor);
            brain.PendingDirectRuntimeRewardControls[actionKey] = pending;
            context.ReenterAfter(
                pending.Completion.Task,
                task =>
                {
                    context.Respond(task.IsCompletedSuccessfully
                        ? task.Result
                        : CreateDirectRuntimeRewardControlResponse(
                            message,
                            accepted: false,
                            "queued_action_failed",
                            $"Direct runtime reward-control queued action failed: {task.Exception?.GetBaseException().Message ?? "unknown_error"}",
                            appliedTickFloor));
                    return Task.CompletedTask;
                });
            return;
        }

        StartImmediateDirectRuntimeRewardControl(
            context,
            brain,
            message,
            actionKey,
            appliedTickFloor);
    }

    private void StartPendingDirectRuntimeRewardControl(IContext context, BrainState brain)
    {
        if (brain.PendingDirectRuntimeRewardControls.Count == 0
            || !_pendingBarrierWorkBrains.Add(brain.BrainId))
        {
            return;
        }

        _pendingDirectRuntimeRewardControlSnapshots[brain.BrainId] = CaptureDirectRuntimeRewardControlRuntimeConfig(brain);

        foreach (var pending in brain.PendingDirectRuntimeRewardControls.Values
                     .OrderBy(static item => item.Request.ActionTickId)
                     .ThenBy(static item => item.ActionKey, StringComparer.Ordinal))
        {
            ApplyDirectRuntimeRewardControlSurface(
                context,
                brain,
                pending.Request);
        }

        context.ReenterAfter(SynchronizeBrainRuntimeConfigAsync(context, brain), task =>
        {
            CompletePendingDirectRuntimeRewardControl(context, brain.BrainId, task);
            return Task.CompletedTask;
        });
    }

    private void CompletePendingDirectRuntimeRewardControl(
        IContext context,
        Guid brainId,
        Task<ProtoIo.IoCommandAck> task)
    {
        _pendingBarrierWorkBrains.Remove(brainId);
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return;
        }

        var pendingControls = brain.PendingDirectRuntimeRewardControls.Values.ToArray();
        brain.PendingDirectRuntimeRewardControls.Clear();
        _pendingDirectRuntimeRewardControlSnapshots.Remove(brainId, out var snapshot);

        var ack = task.IsCompletedSuccessfully && task.Result is not null
            ? task.Result
            : CreateRuntimeConfigSyncAck(
                brainId,
                success: false,
                $"runtime_config_sync_failed:{task.Exception?.GetBaseException().Message ?? "empty_response"}");

        if (ack.Success)
        {
            RegisterBrainWithIo(context, brain, force: true);
        }
        else if (snapshot is not null)
        {
            RestoreDirectRuntimeRewardControlRuntimeConfig(brain, snapshot);
            RegisterBrainWithIo(context, brain, force: true);
        }

        foreach (var pending in pendingControls)
        {
            if (ack.Success)
            {
                RecordDirectRuntimeRewardControl(
                    brain,
                    pending.Request,
                    pending.ActionKey,
                    pending.AppliedTickFloor);
            }

            pending.Completion.TrySetResult(CreateDirectRuntimeRewardControlResponse(
                pending.Request,
                accepted: ack.Success,
                ack.Success ? string.Empty : "runtime_config_sync_failed",
                ack.Success ? "accepted" : ack.Message,
                pending.AppliedTickFloor));
        }

        if (_phase == TickPhase.Deliver && _tick is not null && _pendingDeliver.Contains(brainId))
        {
            if (RemovePendingDeliver(brainId))
            {
                _tick.CompletedDeliverCount++;
                ReportBrainTick(context, brainId, _tick.TickId);
                MaybeCompleteDeliver(context);
            }

            return;
        }

        if (_tickLoopEnabled && !_rescheduleInProgress && _phase == TickPhase.Idle && _pendingBarrierWorkBrains.Count == 0)
        {
            ScheduleNextTick(context, TimeSpan.Zero);
        }
    }

    private void StartImmediateDirectRuntimeRewardControl(
        IContext context,
        BrainState brain,
        ProtoControl.DirectRuntimeRewardControlRequest message,
        string actionKey,
        ulong appliedTickFloor)
    {
        var pending = new PendingDirectRuntimeRewardControlState(actionKey, message, appliedTickFloor);
        brain.PendingDirectRuntimeRewardControls[actionKey] = pending;
        _pendingDirectRuntimeRewardControlSnapshots[brain.BrainId] = CaptureDirectRuntimeRewardControlRuntimeConfig(brain);
        ApplyDirectRuntimeRewardControlSurface(context, brain, message);
        context.ReenterAfter(SynchronizeBrainRuntimeConfigAsync(context, brain), task =>
        {
            context.Respond(CompleteImmediateDirectRuntimeRewardControl(context, brain.BrainId, pending, task));
            return Task.CompletedTask;
        });
    }

    private ProtoControl.DirectRuntimeRewardControlResponse CompleteImmediateDirectRuntimeRewardControl(
        IContext context,
        Guid brainId,
        PendingDirectRuntimeRewardControlState pending,
        Task<ProtoIo.IoCommandAck> task)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            _pendingDirectRuntimeRewardControlSnapshots.Remove(brainId);
            return CreateDirectRuntimeRewardControlResponse(
                pending.Request,
                accepted: false,
                "brain_unregistered",
                "Direct runtime reward-control target brain was unregistered before the action could complete.",
                pending.AppliedTickFloor);
        }

        brain.PendingDirectRuntimeRewardControls.Remove(pending.ActionKey);
        _pendingDirectRuntimeRewardControlSnapshots.Remove(brainId, out var snapshot);

        var ack = task.IsCompletedSuccessfully && task.Result is not null
            ? task.Result
            : CreateRuntimeConfigSyncAck(
                brainId,
                success: false,
                $"runtime_config_sync_failed:{task.Exception?.GetBaseException().Message ?? "empty_response"}");

        if (ack.Success)
        {
            RegisterBrainWithIo(context, brain, force: true);
            RecordDirectRuntimeRewardControl(brain, pending.Request, pending.ActionKey, pending.AppliedTickFloor);
            return CreateDirectRuntimeRewardControlResponse(
                pending.Request,
                accepted: true,
                string.Empty,
                "accepted",
                pending.AppliedTickFloor);
        }

        if (snapshot is not null)
        {
            RestoreDirectRuntimeRewardControlRuntimeConfig(brain, snapshot);
            RegisterBrainWithIo(context, brain, force: true);
        }

        return CreateDirectRuntimeRewardControlResponse(
            pending.Request,
            accepted: false,
            "runtime_config_sync_failed",
            ack.Message,
            pending.AppliedTickFloor);
    }

    private static void RecordDirectRuntimeRewardControl(
        BrainState brain,
        ProtoControl.DirectRuntimeRewardControlRequest message,
        string actionKey,
        ulong appliedTickFloor)
    {
        brain.DirectRuntimeRewardControlRecords[actionKey] = new DirectRuntimeRewardControlRecord(
            NormalizeDirectRuntimeRewardControlToken(message.ControllerId),
            NormalizeDirectRuntimeRewardControlToken(message.ActionId),
            NormalizeDirectRuntimeRewardControlToken(message.ObjectiveName),
            NormalizeDirectRuntimeRewardControlToken(message.RewardSignal),
            message.ObservationTickId,
            message.ActionTickId,
            message.Surface,
            message.Reward,
            message.ControlValue,
            appliedTickFloor);
        TrimDirectRuntimeRewardControlRecords(brain);
    }

    private bool TryValidateDirectRuntimeRewardControlShape(
        ProtoControl.DirectRuntimeRewardControlRequest message,
        out string failureReason,
        out string failureMessage)
    {
        failureReason = string.Empty;
        failureMessage = string.Empty;

        if (string.IsNullOrWhiteSpace(message.ControllerId))
        {
            failureReason = "controller_id_required";
            failureMessage = "Direct runtime reward-control controller_id is required.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(message.ActionId))
        {
            failureReason = "action_id_required";
            failureMessage = "Direct runtime reward-control action_id is required.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(message.ObjectiveName))
        {
            failureReason = "objective_name_required";
            failureMessage = "Direct runtime reward-control objective_name is required.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(message.RewardSignal))
        {
            failureReason = "reward_signal_required";
            failureMessage = "Direct runtime reward-control reward_signal is required.";
            return false;
        }

        if (!float.IsFinite(message.Reward))
        {
            failureReason = "reward_non_finite";
            failureMessage = "Direct runtime reward-control reward must be finite.";
            return false;
        }

        if (!float.IsFinite(message.ControlValue))
        {
            failureReason = "control_value_non_finite";
            failureMessage = "Direct runtime reward-control control_value must be finite.";
            return false;
        }

        switch (message.Surface)
        {
            case ProtoControl.DirectRuntimeRewardControlSurface.PlasticityRate:
                if (message.ControlValue < 0f || message.ControlValue > 1f)
                {
                    failureReason = "control_value_out_of_range";
                    failureMessage = "Direct runtime reward-control plasticity_rate must be in [0,1].";
                    return false;
                }
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.CostEnergyEnabled:
            case ProtoControl.DirectRuntimeRewardControlSurface.PlasticityEnabled:
            case ProtoControl.DirectRuntimeRewardControlSurface.PlasticityProbabilisticUpdates:
            case ProtoControl.DirectRuntimeRewardControlSurface.HomeostasisEnabled:
            case ProtoControl.DirectRuntimeRewardControlSurface.HomeostasisEnergyCouplingEnabled:
                if (!IsDirectRuntimeRewardControlBoolean(message.ControlValue))
                {
                    failureReason = "control_value_out_of_range";
                    failureMessage = $"Direct runtime reward-control {FormatDirectRuntimeRewardControlSurface(message.Surface)} must be 0 or 1.";
                    return false;
                }
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.PlasticityDelta:
                if (message.ControlValue < 0f || message.ControlValue > 1f)
                {
                    failureReason = "control_value_out_of_range";
                    failureMessage = "Direct runtime reward-control plasticity_delta must be in [0,1].";
                    return false;
                }
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.PlasticityRebaseThresholdPct:
                if (message.ControlValue < 0f || message.ControlValue > 1f)
                {
                    failureReason = "control_value_out_of_range";
                    failureMessage = "Direct runtime reward-control plasticity_rebase_threshold_pct must be in [0,1].";
                    return false;
                }
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.HomeostasisBaseProbability:
                if (message.ControlValue < 0f || message.ControlValue > 1f)
                {
                    failureReason = "control_value_out_of_range";
                    failureMessage = "Direct runtime reward-control homeostasis_base_probability must be in [0,1].";
                    return false;
                }
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.HomeostasisEnergyTargetScale:
                if (message.ControlValue < 0f || message.ControlValue > 4f)
                {
                    failureReason = "control_value_out_of_range";
                    failureMessage = "Direct runtime reward-control homeostasis_energy_target_scale must be in [0,4].";
                    return false;
                }
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.HomeostasisEnergyProbabilityScale:
                if (message.ControlValue < 0f || message.ControlValue > 4f)
                {
                    failureReason = "control_value_out_of_range";
                    failureMessage = "Direct runtime reward-control homeostasis_energy_probability_scale must be in [0,4].";
                    return false;
                }
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.OutputVectorSource:
                if (!IsDirectRuntimeRewardControlOutputVectorSource(message.ControlValue))
                {
                    failureReason = "control_value_out_of_range";
                    failureMessage = "Direct runtime reward-control output_vector_source must be 0 for potential or 1 for buffer.";
                    return false;
                }
                break;
            default:
                failureReason = "unsupported_surface";
                failureMessage = "Direct runtime reward-control surface is not supported.";
                return false;
        }

        return true;
    }

    private bool TryValidateDirectRuntimeRewardControlTiming(
        ProtoControl.DirectRuntimeRewardControlRequest message,
        BrainState brain,
        out string failureReason,
        out string failureMessage,
        out ulong appliedTickFloor)
    {
        failureReason = string.Empty;
        failureMessage = string.Empty;
        appliedTickFloor = ResolveDirectRuntimeRewardControlAppliedTickFloor(brain);

        if (message.ObservationTickId > _lastCompletedTickId)
        {
            failureReason = "observation_tick_not_completed";
            failureMessage = "Direct runtime reward-control observation_tick_id must refer to a completed tick.";
            return false;
        }

        if (message.ActionTickId <= message.ObservationTickId)
        {
            failureReason = "action_tick_not_after_observation";
            failureMessage = "Direct runtime reward-control action_tick_id must be after observation_tick_id.";
            return false;
        }

        if (message.ActionTickId < appliedTickFloor)
        {
            failureReason = "action_tick_stale";
            failureMessage = "Direct runtime reward-control action_tick_id must not target a completed or already-visible tick.";
            return false;
        }

        if (message.ActionTickId > appliedTickFloor)
        {
            failureReason = "action_tick_not_next";
            failureMessage = "Direct runtime reward-control action_tick_id must target the next barrier-visible tick.";
            return false;
        }

        return true;
    }

    private ulong ResolveDirectRuntimeRewardControlAppliedTickFloor(BrainState brain)
    {
        if (_tick is null || _phase == TickPhase.Idle || !CanDispatchTickToBrain(brain))
        {
            return _lastCompletedTickId + 1;
        }

        return Math.Max(_lastCompletedTickId, _tick.TickId) + 1;
    }

    private bool ShouldQueueDirectRuntimeRewardControl(BrainState brain)
        => _tick is not null
           && _phase != TickPhase.Idle
           && CanDispatchTickToBrain(brain)
           && brain.PendingRuntimeReset is null
           && (_phase == TickPhase.Compute
               || (_phase == TickPhase.Deliver && _pendingDeliver.Contains(brain.BrainId)));

    private void ApplyDirectRuntimeRewardControlSurface(
        IContext context,
        BrainState brain,
        ProtoControl.DirectRuntimeRewardControlRequest message)
    {
        switch (message.Surface)
        {
            case ProtoControl.DirectRuntimeRewardControlSurface.PlasticityRate:
                ApplyDirectRuntimeRewardControlPlasticityRate(context, brain, message.ControlValue);
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.HomeostasisBaseProbability:
                ApplyDirectRuntimeRewardControlHomeostasisBaseProbability(context, brain, message.ControlValue);
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.CostEnergyEnabled:
                ApplyDirectRuntimeRewardControlCostEnergyEnabled(context, brain, message.ControlValue);
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.PlasticityEnabled:
                ApplyDirectRuntimeRewardControlPlasticityEnabled(context, brain, message.ControlValue);
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.PlasticityProbabilisticUpdates:
                ApplyDirectRuntimeRewardControlPlasticityProbabilisticUpdates(context, brain, message.ControlValue);
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.PlasticityDelta:
                ApplyDirectRuntimeRewardControlPlasticityDelta(context, brain, message.ControlValue);
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.PlasticityRebaseThresholdPct:
                ApplyDirectRuntimeRewardControlPlasticityRebaseThresholdPct(context, brain, message.ControlValue);
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.HomeostasisEnabled:
                ApplyDirectRuntimeRewardControlHomeostasisEnabled(context, brain, message.ControlValue);
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.HomeostasisEnergyCouplingEnabled:
                ApplyDirectRuntimeRewardControlHomeostasisEnergyCouplingEnabled(context, brain, message.ControlValue);
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.HomeostasisEnergyTargetScale:
                ApplyDirectRuntimeRewardControlHomeostasisEnergyTargetScale(context, brain, message.ControlValue);
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.HomeostasisEnergyProbabilityScale:
                ApplyDirectRuntimeRewardControlHomeostasisEnergyProbabilityScale(context, brain, message.ControlValue);
                break;
            case ProtoControl.DirectRuntimeRewardControlSurface.OutputVectorSource:
                ApplyDirectRuntimeRewardControlOutputVectorSource(context, brain, message.ControlValue);
                break;
        }
    }

    private static bool IsDirectRuntimeRewardControlBoolean(float controlValue)
        => Math.Abs(controlValue) < 0.000001f || Math.Abs(controlValue - 1f) < 0.000001f;

    private static bool IsDirectRuntimeRewardControlOutputVectorSource(float controlValue)
        => Math.Abs(controlValue) < 0.000001f || Math.Abs(controlValue - 1f) < 0.000001f;

    private static bool ReadDirectRuntimeRewardControlBoolean(float controlValue)
        => Math.Abs(controlValue - 1f) < 0.000001f;

    private static ProtoControl.OutputVectorSource ReadDirectRuntimeRewardControlOutputVectorSource(float controlValue)
        => Math.Abs(controlValue - 1f) < 0.000001f
            ? ProtoControl.OutputVectorSource.Buffer
            : ProtoControl.OutputVectorSource.Potential;

    private static string FormatDirectRuntimeRewardControlSurface(ProtoControl.DirectRuntimeRewardControlSurface surface)
        => surface switch
        {
            ProtoControl.DirectRuntimeRewardControlSurface.PlasticityRate => "plasticity_rate",
            ProtoControl.DirectRuntimeRewardControlSurface.CostEnergyEnabled => "cost_energy_enabled",
            ProtoControl.DirectRuntimeRewardControlSurface.PlasticityEnabled => "plasticity_enabled",
            ProtoControl.DirectRuntimeRewardControlSurface.PlasticityProbabilisticUpdates => "plasticity_probabilistic_updates",
            ProtoControl.DirectRuntimeRewardControlSurface.PlasticityDelta => "plasticity_delta",
            ProtoControl.DirectRuntimeRewardControlSurface.PlasticityRebaseThresholdPct => "plasticity_rebase_threshold_pct",
            ProtoControl.DirectRuntimeRewardControlSurface.HomeostasisEnabled => "homeostasis_enabled",
            ProtoControl.DirectRuntimeRewardControlSurface.HomeostasisBaseProbability => "homeostasis_base_probability",
            ProtoControl.DirectRuntimeRewardControlSurface.HomeostasisEnergyCouplingEnabled => "homeostasis_energy_coupling_enabled",
            ProtoControl.DirectRuntimeRewardControlSurface.HomeostasisEnergyTargetScale => "homeostasis_energy_target_scale",
            ProtoControl.DirectRuntimeRewardControlSurface.HomeostasisEnergyProbabilityScale => "homeostasis_energy_probability_scale",
            ProtoControl.DirectRuntimeRewardControlSurface.OutputVectorSource => "output_vector_source",
            _ => "unknown"
        };

    private void ApplyDirectRuntimeRewardControlPlasticityRate(
        IContext context,
        BrainState brain,
        float plasticityRate)
    {
        brain.PlasticityRate = plasticityRate;
        brain.PlasticityDelta = ResolvePlasticityDelta(plasticityRate, brain.PlasticityDelta);
    }

    private void ApplyDirectRuntimeRewardControlCostEnergyEnabled(
        IContext context,
        BrainState brain,
        float enabled)
    {
        brain.CostEnergyEnabled = ReadDirectRuntimeRewardControlBoolean(enabled);
    }

    private void ApplyDirectRuntimeRewardControlPlasticityEnabled(
        IContext context,
        BrainState brain,
        float enabled)
    {
        brain.PlasticityEnabled = ReadDirectRuntimeRewardControlBoolean(enabled);
    }

    private void ApplyDirectRuntimeRewardControlPlasticityProbabilisticUpdates(
        IContext context,
        BrainState brain,
        float enabled)
    {
        brain.PlasticityProbabilisticUpdates = ReadDirectRuntimeRewardControlBoolean(enabled);
    }

    private void ApplyDirectRuntimeRewardControlPlasticityDelta(
        IContext context,
        BrainState brain,
        float plasticityDelta)
    {
        brain.PlasticityDelta = plasticityDelta;
    }

    private void ApplyDirectRuntimeRewardControlPlasticityRebaseThresholdPct(
        IContext context,
        BrainState brain,
        float thresholdPct)
    {
        brain.PlasticityRebaseThresholdPct = thresholdPct;
    }

    private void ApplyDirectRuntimeRewardControlHomeostasisEnabled(
        IContext context,
        BrainState brain,
        float enabled)
    {
        brain.HomeostasisEnabled = ReadDirectRuntimeRewardControlBoolean(enabled);
    }

    private void ApplyDirectRuntimeRewardControlHomeostasisBaseProbability(
        IContext context,
        BrainState brain,
        float probability)
    {
        brain.HomeostasisBaseProbability = probability;
    }

    private void ApplyDirectRuntimeRewardControlHomeostasisEnergyCouplingEnabled(
        IContext context,
        BrainState brain,
        float enabled)
    {
        brain.HomeostasisEnergyCouplingEnabled = ReadDirectRuntimeRewardControlBoolean(enabled);
    }

    private void ApplyDirectRuntimeRewardControlHomeostasisEnergyTargetScale(
        IContext context,
        BrainState brain,
        float scale)
    {
        brain.HomeostasisEnergyTargetScale = scale;
    }

    private void ApplyDirectRuntimeRewardControlHomeostasisEnergyProbabilityScale(
        IContext context,
        BrainState brain,
        float scale)
    {
        brain.HomeostasisEnergyProbabilityScale = scale;
    }

    private void ApplyDirectRuntimeRewardControlOutputVectorSource(
        IContext context,
        BrainState brain,
        float source)
    {
        brain.OutputVectorSource = ReadDirectRuntimeRewardControlOutputVectorSource(source);
        brain.HasExplicitOutputVectorSource = true;
    }

    private static DirectRuntimeRewardControlRuntimeConfigSnapshot CaptureDirectRuntimeRewardControlRuntimeConfig(BrainState brain)
        => new(
            brain.CostEnergyEnabled,
            brain.PlasticityEnabled,
            brain.PlasticityRate,
            brain.PlasticityProbabilisticUpdates,
            brain.PlasticityDelta,
            brain.PlasticityRebaseThresholdPct,
            brain.HomeostasisEnabled,
            brain.HomeostasisBaseProbability,
            brain.HomeostasisEnergyCouplingEnabled,
            brain.HomeostasisEnergyTargetScale,
            brain.HomeostasisEnergyProbabilityScale,
            brain.OutputVectorSource,
            brain.HasExplicitOutputVectorSource);

    private static void RestoreDirectRuntimeRewardControlRuntimeConfig(
        BrainState brain,
        DirectRuntimeRewardControlRuntimeConfigSnapshot snapshot)
    {
        brain.CostEnergyEnabled = snapshot.CostEnergyEnabled;
        brain.PlasticityEnabled = snapshot.PlasticityEnabled;
        brain.PlasticityRate = snapshot.PlasticityRate;
        brain.PlasticityProbabilisticUpdates = snapshot.PlasticityProbabilisticUpdates;
        brain.PlasticityDelta = snapshot.PlasticityDelta;
        brain.PlasticityRebaseThresholdPct = snapshot.PlasticityRebaseThresholdPct;
        brain.HomeostasisEnabled = snapshot.HomeostasisEnabled;
        brain.HomeostasisBaseProbability = snapshot.HomeostasisBaseProbability;
        brain.HomeostasisEnergyCouplingEnabled = snapshot.HomeostasisEnergyCouplingEnabled;
        brain.HomeostasisEnergyTargetScale = snapshot.HomeostasisEnergyTargetScale;
        brain.HomeostasisEnergyProbabilityScale = snapshot.HomeostasisEnergyProbabilityScale;
        brain.OutputVectorSource = snapshot.OutputVectorSource;
        brain.HasExplicitOutputVectorSource = snapshot.HasExplicitOutputVectorSource;
    }

    private static ProtoControl.DirectRuntimeRewardControlResponse CreateDirectRuntimeRewardControlResponse(
        ProtoControl.DirectRuntimeRewardControlRequest request,
        bool accepted,
        string failureReason,
        string message,
        ulong appliedTickFloor)
        => new()
        {
            Accepted = accepted,
            FailureReasonCode = failureReason,
            Message = message,
            BrainId = request.BrainId?.Clone() ?? new Nbn.Proto.Uuid(),
            ControllerId = request.ControllerId ?? string.Empty,
            ActionId = request.ActionId ?? string.Empty,
            Surface = request.Surface,
            AppliedTickFloor = appliedTickFloor,
            Reward = request.Reward,
            ControlValue = request.ControlValue
        };

    private static string BuildDirectRuntimeRewardControlActionKey(
        ProtoControl.DirectRuntimeRewardControlRequest message)
        => string.Join(
            "\u001f",
            NormalizeDirectRuntimeRewardControlToken(message.ControllerId),
            NormalizeDirectRuntimeRewardControlToken(message.ObjectiveName),
            NormalizeDirectRuntimeRewardControlToken(message.RewardSignal),
            NormalizeDirectRuntimeRewardControlToken(message.ActionId),
            message.ObservationTickId.ToString(System.Globalization.CultureInfo.InvariantCulture),
            message.ActionTickId.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ((int)message.Surface).ToString(System.Globalization.CultureInfo.InvariantCulture));

    private static bool HasConflictingDirectRuntimeRewardControlActionId(
        BrainState brain,
        ProtoControl.DirectRuntimeRewardControlRequest message)
    {
        var controllerId = NormalizeDirectRuntimeRewardControlToken(message.ControllerId);
        var actionId = NormalizeDirectRuntimeRewardControlToken(message.ActionId);
        var actionKey = BuildDirectRuntimeRewardControlActionKey(message);
        return EnumerateDirectRuntimeRewardControlRecords(brain).Any(
            record => string.Equals(record.ActionKey, actionKey, StringComparison.Ordinal) is false
                      && string.Equals(record.Value.ControllerId, controllerId, StringComparison.Ordinal)
                      && string.Equals(record.Value.ActionId, actionId, StringComparison.Ordinal));
    }

    private static bool HasConflictingDirectRuntimeRewardControlSurfaceTick(
        BrainState brain,
        ProtoControl.DirectRuntimeRewardControlRequest message)
    {
        return EnumerateDirectRuntimeRewardControlRecords(brain).Any(
            record => record.Value.Surface == message.Surface
                      && record.Value.ActionTickId == message.ActionTickId);
    }

    private static bool HasStaleDirectRuntimeRewardControlAction(
        BrainState brain,
        ProtoControl.DirectRuntimeRewardControlRequest message)
    {
        var controllerId = NormalizeDirectRuntimeRewardControlToken(message.ControllerId);
        var objectiveName = NormalizeDirectRuntimeRewardControlToken(message.ObjectiveName);
        var rewardSignal = NormalizeDirectRuntimeRewardControlToken(message.RewardSignal);
        return EnumerateDirectRuntimeRewardControlRecords(brain).Select(static record => record.Value).Any(
            record => string.Equals(record.ControllerId, controllerId, StringComparison.Ordinal)
                      && string.Equals(record.ObjectiveName, objectiveName, StringComparison.Ordinal)
                      && string.Equals(record.RewardSignal, rewardSignal, StringComparison.Ordinal)
                      && record.Surface == message.Surface
                      && (record.ActionTickId >= message.ActionTickId
                          || record.ObservationTickId >= message.ObservationTickId));
    }

    private static IEnumerable<(string ActionKey, DirectRuntimeRewardControlRecord Value)> EnumerateDirectRuntimeRewardControlRecords(
        BrainState brain)
    {
        foreach (var record in brain.DirectRuntimeRewardControlRecords)
        {
            yield return (record.Key, record.Value);
        }

        foreach (var pending in brain.PendingDirectRuntimeRewardControls.Values)
        {
            yield return (pending.ActionKey, CreateDirectRuntimeRewardControlRecord(pending.Request, pending.AppliedTickFloor));
        }
    }

    private static DirectRuntimeRewardControlRecord CreateDirectRuntimeRewardControlRecord(
        ProtoControl.DirectRuntimeRewardControlRequest message,
        ulong appliedTickFloor)
        => new(
            NormalizeDirectRuntimeRewardControlToken(message.ControllerId),
            NormalizeDirectRuntimeRewardControlToken(message.ActionId),
            NormalizeDirectRuntimeRewardControlToken(message.ObjectiveName),
            NormalizeDirectRuntimeRewardControlToken(message.RewardSignal),
            message.ObservationTickId,
            message.ActionTickId,
            message.Surface,
            message.Reward,
            message.ControlValue,
            appliedTickFloor);

    private static string NormalizeDirectRuntimeRewardControlToken(string? value)
        => (value ?? string.Empty).Trim();

    private static void TrimDirectRuntimeRewardControlRecords(BrainState brain)
    {
        const int maxRecords = 1024;
        if (brain.DirectRuntimeRewardControlRecords.Count <= maxRecords)
        {
            return;
        }

        foreach (var key in brain.DirectRuntimeRewardControlRecords
                     .OrderBy(static entry => entry.Value.AppliedTickFloor)
                     .ThenBy(static entry => entry.Key, StringComparer.Ordinal)
                     .Take(brain.DirectRuntimeRewardControlRecords.Count - maxRecords)
                     .Select(static entry => entry.Key)
                     .ToArray())
        {
            brain.DirectRuntimeRewardControlRecords.Remove(key);
        }
    }

    private void HandlePauseBrainControl(IContext context, ProtoControl.PauseBrain message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            RespondPauseResumeAck(context, message.BrainId, "pause_brain", success: false, "invalid_brain_id");
            return;
        }

        if (!IsControlPlaneBrainMutationAuthorized(context, brainId, out _, out var reason))
        {
            EmitControlPlaneMutationIgnored(context, "control.pause_brain", brainId, reason);
            HiveMindTelemetry.RecordPauseBrainRejected(brainId, reason);
            RespondPauseResumeAck(context, message.BrainId, "pause_brain", success: false, reason);
            return;
        }

        PauseBrain(context, brainId, message.Reason);
        RespondPauseResumeAck(context, message.BrainId, "pause_brain", success: true, "applied");
    }

    private void HandleResumeBrainControl(IContext context, ProtoControl.ResumeBrain message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            RespondPauseResumeAck(context, message.BrainId, "resume_brain", success: false, "invalid_brain_id");
            return;
        }

        if (!IsControlPlaneBrainMutationAuthorized(context, brainId, out _, out var reason))
        {
            EmitControlPlaneMutationIgnored(context, "control.resume_brain", brainId, reason);
            HiveMindTelemetry.RecordResumeBrainRejected(brainId, reason);
            RespondPauseResumeAck(context, message.BrainId, "resume_brain", success: false, reason);
            return;
        }

        ResumeBrain(context, brainId);
        RespondPauseResumeAck(context, message.BrainId, "resume_brain", success: true, "applied");
    }

    private void HandlePauseBrainRequest(IContext context, PauseBrainRequest message)
    {
        if (!_brains.ContainsKey(message.BrainId))
        {
            RespondPauseResumeAck(context, message.BrainId.ToProtoUuid(), "pause_brain", success: false, "brain_not_registered");
            return;
        }

        PauseBrain(context, message.BrainId, message.Reason);
        RespondPauseResumeAck(context, message.BrainId.ToProtoUuid(), "pause_brain", success: true, "applied");
    }

    private void HandleResumeBrainRequest(IContext context, ResumeBrainRequest message)
    {
        if (!_brains.ContainsKey(message.BrainId))
        {
            RespondPauseResumeAck(context, message.BrainId.ToProtoUuid(), "resume_brain", success: false, "brain_not_registered");
            return;
        }

        ResumeBrain(context, message.BrainId);
        RespondPauseResumeAck(context, message.BrainId.ToProtoUuid(), "resume_brain", success: true, "applied");
    }

    private static void RespondPauseResumeAck(
        IContext context,
        Nbn.Proto.Uuid? brainId,
        string command,
        bool success,
        string message)
    {
        if (context.Sender is null)
        {
            return;
        }

        context.Respond(new ProtoIo.IoCommandAck
        {
            BrainId = brainId?.Clone(),
            Command = command,
            Success = success,
            Message = message ?? string.Empty
        });
    }

    private void HandleKillBrainControl(IContext context, ProtoControl.KillBrain message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        if (!IsControlPlaneBrainMutationAuthorized(context, brainId, out _, out var reason))
        {
            EmitControlPlaneMutationIgnored(context, "control.kill_brain", brainId, reason);
            HiveMindTelemetry.RecordKillBrainRejected(brainId, reason);
            return;
        }

        KillBrain(context, brainId, message.Reason);
    }
}
