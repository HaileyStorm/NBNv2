using Nbn.Proto.Viz;
using Nbn.Runtime.IO;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoIo = Nbn.Proto.Io;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private void RegisterBrainInternal(IContext context, Guid brainId, PID? brainRootPid, PID? routerPid, int? pausePriority = null)
    {
        var isNew = !_brains.TryGetValue(brainId, out var brain) || brain is null;
        if (isNew)
        {
            brain = new BrainState(brainId)
            {
                SpawnedMs = NowMs()
            };
            _brains[brainId] = brain;
        }

        brainRootPid = NormalizePid(context, brainRootPid);
        routerPid = NormalizePid(context, routerPid);

        if (routerPid is not null && string.IsNullOrWhiteSpace(routerPid.Address))
        {
            var fallbackAddress = brainRootPid?.Address;
            if (!string.IsNullOrWhiteSpace(fallbackAddress))
            {
                routerPid = new PID(fallbackAddress, routerPid.Id);
            }
        }

        if (brainRootPid is not null && string.IsNullOrWhiteSpace(brainRootPid.Address))
        {
            var fallbackAddress = routerPid?.Address;
            if (!string.IsNullOrWhiteSpace(fallbackAddress))
            {
                brainRootPid = new PID(fallbackAddress, brainRootPid.Id);
            }
        }

        var brainState = brain ?? throw new InvalidOperationException("Brain state was not initialized.");
        if (pausePriority.HasValue)
        {
            brainState.PausePriority = pausePriority.Value;
        }

        if (brainRootPid is not null)
        {
            brainState.BrainRootPid = brainRootPid;
        }

        if (routerPid is not null)
        {
            brainState.SignalRouterPid = routerPid;
        }

        if (brainState.PlacementEpoch > 0
            && (brainState.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleRequested
                || brainState.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigning
                || brainState.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleUnknown))
        {
            UpdatePlacementLifecycle(
                brainState,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned,
                ProtoControl.PlacementFailureReason.PlacementFailureNone);
        }

        UpdateRoutingTable(context, brainState);
        RegisterBrainWithIo(context, brainState);

        ReportBrainRegistration(context, brainState);
        TryCompletePendingSpawn(context, brainState);

        if (isNew)
        {
            EmitVizEvent(context, VizEventType.VizBrainSpawned, brainId: brainState.BrainId);
            EmitDebug(context, ProtoSeverity.SevInfo, "brain.spawned", $"Registered brain {brainState.BrainId}.");
        }

        EmitVizEvent(
            context,
            brainState.Paused ? VizEventType.VizBrainPaused : VizEventType.VizBrainActive,
            brainId: brainState.BrainId);
    }

    private void UpdateBrainSignalRouter(IContext context, Guid brainId, PID routerPid)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            brain = new BrainState(brainId);
            _brains.Add(brainId, brain);
        }

        routerPid = NormalizePid(context, routerPid) ?? routerPid;
        if (routerPid.Address.Length == 0 && brain.BrainRootPid is not null && brain.BrainRootPid.Address.Length > 0)
        {
            routerPid = new PID(brain.BrainRootPid.Address, routerPid.Id);
        }

        brain.SignalRouterPid = routerPid;
        UpdateRoutingTable(context, brain);
    }

    private void UnregisterBrain(IContext context, Guid brainId, string reason = "unregistered", bool notifyIoUnregister = true)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return;
        }

        DispatchPlacementUnassignments(context, brain, brain.PlacementExecution, reason);
        ReleaseBrainVisualizationSubscribers(context, brain);
        _brains.Remove(brainId);

        if (_pendingSpawns.Remove(brainId, out var pendingSpawn))
        {
            var reasonCode = string.Equals(reason, "spawn_timeout", StringComparison.OrdinalIgnoreCase)
                ? "spawn_timeout"
                : !string.IsNullOrWhiteSpace(brain.SpawnFailureReasonCode)
                    ? brain.SpawnFailureReasonCode
                    : ToSpawnFailureReasonCode(brain.PlacementFailureReason);
            var failureMessage = string.Equals(reason, "spawn_timeout", StringComparison.OrdinalIgnoreCase)
                ? "Spawn timed out while waiting for placement completion."
                : !string.IsNullOrWhiteSpace(brain.SpawnFailureMessage)
                    ? brain.SpawnFailureMessage
                    : BuildSpawnFailureMessage(brain.PlacementFailureReason, detail: null, fallbackReasonCode: reasonCode);
            pendingSpawn.SetFailure(reasonCode, failureMessage);
            pendingSpawn.Completion.TrySetResult(false);
        }

        if (notifyIoUnregister && _ioPid is not null)
        {
            var ioPid = ResolveSendTargetPid(context, _ioPid);
            context.Send(ioPid, new ProtoIo.UnregisterBrain
            {
                BrainId = brainId.ToProtoUuid(),
                Reason = reason
            });
        }

        ReportBrainUnregistered(context, brainId);
        EmitVizEvent(context, VizEventType.VizBrainTerminated, brainId: brainId);
        EmitDebug(context, ProtoSeverity.SevWarn, "brain.terminated", $"Brain {brainId} unregistered. reason={reason}");

        if (_phase == TickPhase.Compute)
        {
            RemovePendingComputeForBrain(brainId);
        }

        if (_phase == TickPhase.Deliver)
        {
            if (RemovePendingDeliver(brainId))
            {
                MaybeCompleteDeliver(context);
            }
        }
    }

    private void RegisterShardInternal(IContext context, Guid brainId, int regionId, int shardIndex, PID shardPid, int neuronStart, int neuronCount)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            brain = new BrainState(brainId);
            _brains.Add(brainId, brain);
        }

        if (!ShardId32.TryFrom(regionId, shardIndex, out var shardId))
        {
            Log($"RegisterShard invalid shard index: brain {brainId} region {regionId} shardIndex {shardIndex}.");
            return;
        }

        brain.Shards.TryGetValue(shardId, out var previousShardPid);
        var normalized = NormalizePid(context, shardPid) ?? shardPid;
        brain.Shards[shardId] = normalized;
        brain.ShardRegistrationEpochs[shardId] = brain.PlacementEpoch;
        SendShardVisualizationUpdate(
            context,
            brainId,
            shardId,
            normalized,
            brain.VisualizationEnabled,
            brain.VisualizationFocusRegionId,
            _vizStreamMinIntervalMs);
        var effectiveCostEnergyEnabled = ResolveEffectiveCostEnergyEnabled(brain);
        var effectivePlasticityEnabled = ResolveEffectivePlasticityEnabled(brain);
        var effectivePlasticityDelta = ResolvePlasticityDelta(brain.PlasticityRate, brain.PlasticityDelta);
        SendShardRuntimeConfigUpdate(
            context,
            brainId,
            shardId,
            normalized,
            effectiveCostEnergyEnabled,
            effectiveCostEnergyEnabled,
            effectivePlasticityEnabled,
            brain.PlasticityRate,
            brain.PlasticityProbabilisticUpdates,
            effectivePlasticityDelta,
            brain.PlasticityRebaseThreshold,
            brain.PlasticityRebaseThresholdPct,
            brain.PlasticityEnergyCostModulationEnabled,
            brain.PlasticityEnergyCostReferenceTickCost,
            brain.PlasticityEnergyCostResponseStrength,
            brain.PlasticityEnergyCostMinScale,
            brain.PlasticityEnergyCostMaxScale,
            brain.HomeostasisEnabled,
            brain.HomeostasisTargetMode,
            brain.HomeostasisUpdateMode,
            brain.HomeostasisBaseProbability,
            brain.HomeostasisMinStepCodes,
            brain.HomeostasisEnergyCouplingEnabled,
            brain.HomeostasisEnergyTargetScale,
            brain.HomeostasisEnergyProbabilityScale,
            _remoteCostEnabled,
            _remoteCostPerBatch,
            _remoteCostPerContribution,
            _costTierAMultiplier,
            _costTierBMultiplier,
            _costTierCMultiplier,
            _outputVectorSource,
            _debugStreamEnabled,
            _debugMinSeverity);
        UpdateRoutingTable(context, brain);
        if (brain.PlacementEpoch > 0
            && (brain.PlacementExecution is null || brain.PlacementExecution.Completed))
        {
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning,
                ProtoControl.PlacementFailureReason.PlacementFailureNone);
            brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileMatched;
            TryCompletePendingSpawn(context, brain);
        }
        EmitVizEvent(
            context,
            VizEventType.VizShardSpawned,
            brainId: brainId,
            regionId: (uint)regionId,
            shardId: shardId);

        if (neuronCount > 0)
        {
            var span = neuronStart + neuronCount;
            if (regionId == NbnConstants.InputRegionId && span > brain.InputWidth)
            {
                brain.InputWidth = span;
            }

            if (regionId == NbnConstants.OutputRegionId && span > brain.OutputWidth)
            {
                brain.OutputWidth = span;
            }
        }

        if (regionId == NbnConstants.OutputRegionId && brain.OutputSinkPid is not null)
        {
            SendOutputSinkUpdate(context, brainId, shardId, normalized, brain.OutputSinkPid);
            Log($"Output shard registered; pushed sink for brain {brainId} shard {shardId}");
        }

        RegisterBrainWithIo(context, brain);

        if (_phase == TickPhase.Compute && _tick is not null)
        {
            var key = new ShardKey(brainId, shardId);
            var pendingSenderUpdated = false;
            var previousPendingSenderLabel = "<missing>";
            if (_pendingCompute.Contains(key))
            {
                if (_pendingComputeSenders.TryGetValue(key, out var existingPendingSender))
                {
                    previousPendingSenderLabel = PidLabel(existingPendingSender);
                }

                _pendingComputeSenders[key] = normalized;
                pendingSenderUpdated = true;
            }

            Log($"Shard registered mid-compute for brain {brainId}; will start next tick.");
            if (LogVizDiagnostics)
            {
                var priorShardLabel = previousShardPid is null ? "<new>" : PidLabel(previousShardPid);
                var replacedExisting = previousShardPid is not null;
                var pidChanged = previousShardPid is not null && !PidEquals(previousShardPid, normalized);
                var updatedSenderLabel = PidLabel(normalized);
                Log(
                    $"VizDiag register-shard brain={brainId} shard={shardId} tick={_tick.TickId} replacedExisting={replacedExisting} pidChanged={pidChanged} pendingKey={_pendingCompute.Contains(key)} pendingSenderUpdated={pendingSenderUpdated} previousShardPid={priorShardLabel} updatedShardPid={updatedSenderLabel} previousPendingSender={previousPendingSenderLabel} pendingCompute={_pendingCompute.Count}");
            }
        }

        EmitDebug(
            context,
            ProtoSeverity.SevDebug,
            "shard.registered",
            $"Brain={brainId} region={regionId} shard={shardId} neurons={neuronStart}:{neuronCount}");
    }

    private void UnregisterShardInternal(IContext context, Guid brainId, int regionId, int shardIndex)
    {
        if (_brains.TryGetValue(brainId, out var brain))
        {
            if (!ShardId32.TryFrom(regionId, shardIndex, out var shardId))
            {
                Log($"UnregisterShard invalid shard index: brain {brainId} region {regionId} shardIndex {shardIndex}.");
                return;
            }

            var shouldRecover = !brain.RecoveryInProgress
                && brain.PlacementEpoch > 0
                && (brain.PlacementExecution is null || brain.PlacementExecution.Completed)
                && (brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned
                    || brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning);
            var removed = brain.Shards.Remove(shardId);
            brain.ShardRegistrationEpochs.Remove(shardId);
            UpdateRoutingTable(context, brain);
            if (!removed)
            {
                return;
            }

            if (shouldRecover)
            {
                brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileRequiresAction;
                UpdatePlacementLifecycle(
                    brain,
                    ProtoControl.PlacementLifecycleState.PlacementLifecycleReconciling,
                    ProtoControl.PlacementFailureReason.PlacementFailureNone);
                RequestBrainRecovery(
                    context,
                    brainId,
                    trigger: "shard_loss",
                    detail: $"Unexpected shard unregister for region={regionId} shard={shardIndex}.");
            }
            else if (brain.PlacementEpoch > 0 && brain.Shards.Count == 0)
            {
                brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileRequiresAction;
                UpdatePlacementLifecycle(
                    brain,
                    ProtoControl.PlacementLifecycleState.PlacementLifecycleReconciling,
                    ProtoControl.PlacementFailureReason.PlacementFailureNone);
            }
        }

        if (_phase != TickPhase.Compute || _tick is null)
        {
            return;
        }

        if (ShardId32.TryFrom(regionId, shardIndex, out var pendingShardId)
            && RemovePendingCompute(new ShardKey(brainId, pendingShardId)))
        {
            _tick.ExpectedComputeCount = Math.Max(_tick.CompletedComputeCount, _tick.ExpectedComputeCount - 1);
            MaybeCompleteCompute(context);
        }
    }

    private void HandleRegisterBrain(IContext context, ProtoControl.RegisterBrain message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        var brainRootPid = ParsePid(message.BrainRootPid);
        var routerPid = ParsePid(message.SignalRouterPid);

        if (!IsRegisterBrainAuthorized(context, brainId, brainRootPid, routerPid, out var reason))
        {
            EmitControlPlaneMutationIgnored(context, "control.register_brain", brainId, reason);
            return;
        }

        RegisterBrainInternal(
            context,
            brainId,
            brainRootPid,
            routerPid,
            message.HasPausePriority ? message.PausePriority : null);
    }

    private void HandleUpdateBrainSignalRouter(IContext context, ProtoControl.UpdateBrainSignalRouter message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        var routerPid = ParsePid(message.SignalRouterPid);
        if (routerPid is null)
        {
            return;
        }

        if (!IsUpdateBrainSignalRouterAuthorized(context, brainId, routerPid, out var reason))
        {
            EmitControlPlaneMutationIgnored(context, "control.update_brain_signal_router", brainId, reason);
            return;
        }

        UpdateBrainSignalRouter(context, brainId, routerPid);
    }

    private void HandleUnregisterBrain(IContext context, ProtoControl.UnregisterBrain message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId) || !_brains.TryGetValue(brainId, out var brain))
        {
            return;
        }

        if (context.Sender is not null)
        {
            if (!IsValidControllerBootstrapSender(context, brain.BrainRootPid, brain.SignalRouterPid))
            {
                EmitControlPlaneMutationIgnored(context, "control.unregister_brain", brainId, "sender_mismatch");
                return;
            }

            if ((brain.PlacementExecution is not null && !brain.PlacementExecution.Completed) || brain.RecoveryInProgress)
            {
                // Hosted controller teardown from the old placement can race with replacement/recovery.
                // External/system teardown remains senderless and should continue to remove the brain.
                var reason = brain.RecoveryInProgress ? "recovery_in_progress" : "placement_in_flight";
                EmitControlPlaneMutationIgnored(context, "control.unregister_brain", brainId, reason);
                return;
            }
        }

        UnregisterBrain(context, brainId);
    }

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

    private void HandleRegisterShard(IContext context, ProtoControl.RegisterShard message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        var shardPid = ParsePid(message.ShardPid);
        if (shardPid is null)
        {
            return;
        }

        var regionId = (int)message.RegionId;
        var shardIndex = (int)message.ShardIndex;
        if (!ShardId32.TryFrom(regionId, shardIndex, out var shardId))
        {
            Log($"RegisterShard invalid shard index: brain {brainId} region {regionId} shardIndex {shardIndex}.");
            return;
        }

        var normalizedShardPid = NormalizePid(context, shardPid) ?? shardPid;
        if (!IsRegisterShardAuthorized(context, brainId, shardId, normalizedShardPid, out var reason))
        {
            EmitControlPlaneMutationIgnored(context, "control.register_shard", brainId, reason, shardId);
            return;
        }

        RegisterShardInternal(
            context,
            brainId,
            regionId,
            shardIndex,
            normalizedShardPid,
            (int)message.NeuronStart,
            (int)message.NeuronCount);
    }

    private void HandleUnregisterShard(IContext context, ProtoControl.UnregisterShard message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        var regionId = (int)message.RegionId;
        var shardIndex = (int)message.ShardIndex;
        if (!ShardId32.TryFrom(regionId, shardIndex, out var shardId))
        {
            Log($"UnregisterShard invalid shard index: brain {brainId} region {regionId} shardIndex {shardIndex}.");
            return;
        }

        if (!IsUnregisterShardAuthorized(context, brainId, shardId, out var reason))
        {
            EmitControlPlaneMutationIgnored(context, "control.unregister_shard", brainId, reason, shardId);
            return;
        }

        UnregisterShardInternal(context, brainId, regionId, shardIndex);
    }

    private bool IsRegisterBrainAuthorized(
        IContext context,
        Guid brainId,
        PID? brainRootPid,
        PID? routerPid,
        out string reason)
    {
        if (_brains.TryGetValue(brainId, out var brain) && HasTrustedController(brain))
        {
            if (IsTrustedControllerSender(context.Sender, brain))
            {
                reason = string.Empty;
                return true;
            }

            reason = context.Sender is null
                ? "trusted_controller_sender_missing"
                : "sender_not_trusted_controller";
            return false;
        }

        if (context.Sender is null)
        {
            reason = "bootstrap_sender_missing";
            return false;
        }

        if (IsValidControllerBootstrapSender(context, brainRootPid, routerPid))
        {
            reason = string.Empty;
            return true;
        }

        reason = "bootstrap_sender_mismatch";
        return false;
    }

    private bool IsUpdateBrainSignalRouterAuthorized(
        IContext context,
        Guid brainId,
        PID routerPid,
        out string reason)
    {
        if (_brains.TryGetValue(brainId, out var brain) && HasTrustedController(brain))
        {
            if (IsTrustedControllerSender(context.Sender, brain))
            {
                reason = string.Empty;
                return true;
            }

            reason = context.Sender is null
                ? "trusted_controller_sender_missing"
                : "sender_not_trusted_controller";
            return false;
        }

        if (context.Sender is null)
        {
            reason = "bootstrap_sender_missing";
            return false;
        }

        if (IsValidControllerBootstrapSender(context, null, routerPid))
        {
            reason = string.Empty;
            return true;
        }

        reason = "bootstrap_sender_mismatch";
        return false;
    }

    private bool IsRegisterShardAuthorized(
        IContext context,
        Guid brainId,
        ShardId32 shardId,
        PID shardPid,
        out string reason)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            reason = context.Sender is null
                ? "brain_not_registered_sender_missing"
                : "brain_not_registered_sender_not_allowed";
            return false;
        }

        if (context.Sender is null)
        {
            reason = "sender_missing";
            return false;
        }

        var senderIsTrustedController = IsTrustedControllerSender(context.Sender, brain);
        var senderIsAuthorizedWorker = IsPlacementAuthorizedWorkerSender(context.Sender, brain);
        var senderMatchesShardPid = SenderMatchesPid(context.Sender, shardPid);

        if (brain.Shards.TryGetValue(shardId, out var existingShardPid))
        {
            if (PidEquals(existingShardPid, shardPid))
            {
                reason = string.Empty;
                return true;
            }

            if (senderIsTrustedController || senderIsAuthorizedWorker)
            {
                reason = string.Empty;
                return true;
            }

            reason = "overwrite_sender_not_authorized";
            return false;
        }

        if (senderIsTrustedController || senderIsAuthorizedWorker || senderMatchesShardPid)
        {
            reason = string.Empty;
            return true;
        }

        reason = "sender_not_authorized";
        return false;
    }

    private bool IsUnregisterShardAuthorized(
        IContext context,
        Guid brainId,
        ShardId32 shardId,
        out string reason)
    {
        reason = string.Empty;
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return true;
        }

        if (!brain.Shards.TryGetValue(shardId, out var existingShardPid))
        {
            return true;
        }

        if (context.Sender is null)
        {
            reason = "sender_missing";
            return false;
        }

        if (SenderMatchesPid(context.Sender, existingShardPid)
            || IsTrustedControllerSender(context.Sender, brain)
            || IsPlacementAuthorizedWorkerSender(context.Sender, brain))
        {
            return true;
        }

        reason = "sender_not_authorized";
        return false;
    }

    private bool IsControlPlaneBrainMutationAuthorized(
        IContext context,
        Guid brainId,
        out BrainState brain,
        out string reason)
    {
        if (!_brains.TryGetValue(brainId, out brain!))
        {
            reason = context.Sender is null
                ? "brain_not_registered_sender_missing"
                : "brain_not_registered_sender_not_allowed";
            return false;
        }

        if (context.Sender is null)
        {
            reason = "sender_missing";
            return false;
        }

        if (IsTrustedControllerSender(context.Sender, brain)
            || IsPlacementAuthorizedWorkerSender(context.Sender, brain)
            || IsTrustedIoSender(context))
        {
            reason = string.Empty;
            return true;
        }

        reason = "sender_not_authorized";
        return false;
    }

    private static bool HasTrustedController(BrainState brain)
        => brain.BrainRootPid is not null || brain.SignalRouterPid is not null;

    private static bool IsTrustedControllerSender(PID? sender, BrainState brain)
    {
        if (sender is null)
        {
            return false;
        }

        return (brain.BrainRootPid is not null && SenderMatchesPid(sender, brain.BrainRootPid))
               || (brain.SignalRouterPid is not null && SenderMatchesPid(sender, brain.SignalRouterPid));
    }

    private bool IsPlacementAuthorizedWorkerSender(PID? sender, BrainState brain)
    {
        if (sender is null)
        {
            return false;
        }

        if (brain.PlacementExecution is not null)
        {
            foreach (var workerPid in brain.PlacementExecution.WorkerTargets.Values)
            {
                if (SenderMatchesPid(sender, workerPid))
                {
                    return true;
                }
            }
        }

        foreach (var worker in _workerCatalog.Values)
        {
            if (!worker.IsAlive || !worker.IsReady || !worker.IsFresh || string.IsNullOrWhiteSpace(worker.WorkerRootActorName))
            {
                continue;
            }

            var workerPid = string.IsNullOrWhiteSpace(worker.WorkerAddress)
                ? new PID { Id = worker.WorkerRootActorName }
                : new PID(worker.WorkerAddress, worker.WorkerRootActorName);
            if (SenderMatchesPid(sender, workerPid))
            {
                return true;
            }
        }

        return false;
    }

    private bool IsTrustedIoSender(IContext context)
    {
        var normalizedIoPid = NormalizePid(context, _ioPid);
        return normalizedIoPid is not null && SenderMatchesPid(context.Sender, normalizedIoPid);
    }

    private static bool IsValidControllerBootstrapSender(
        IContext context,
        PID? brainRootPid,
        PID? routerPid)
    {
        var normalizedBrainRoot = NormalizePid(context, brainRootPid);
        if (normalizedBrainRoot is not null && SenderMatchesPid(context.Sender, normalizedBrainRoot))
        {
            return true;
        }

        var normalizedRouter = NormalizePid(context, routerPid);
        return normalizedRouter is not null && SenderMatchesPid(context.Sender, normalizedRouter);
    }

    private void EmitControlPlaneMutationIgnored(
        IContext context,
        string topic,
        Guid brainId,
        string reason,
        ShardId32? shardId = null)
    {
        var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
        var shardLabel = shardId is null ? string.Empty : $" shard={shardId.Value}";
        EmitDebug(
            context,
            ProtoSeverity.SevDebug,
            $"{topic}.ignored",
            $"Ignored {topic}. reason={reason} brain={brainId:D}{shardLabel} sender={senderLabel}.");
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

    private void HandlePauseBrainControl(IContext context, ProtoControl.PauseBrain message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        if (!IsControlPlaneBrainMutationAuthorized(context, brainId, out _, out var reason))
        {
            EmitControlPlaneMutationIgnored(context, "control.pause_brain", brainId, reason);
            HiveMindTelemetry.RecordPauseBrainRejected(brainId, reason);
            return;
        }

        PauseBrain(context, brainId, message.Reason);
    }

    private void HandleResumeBrainControl(IContext context, ProtoControl.ResumeBrain message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        if (!IsControlPlaneBrainMutationAuthorized(context, brainId, out _, out var reason))
        {
            EmitControlPlaneMutationIgnored(context, "control.resume_brain", brainId, reason);
            HiveMindTelemetry.RecordResumeBrainRejected(brainId, reason);
            return;
        }

        ResumeBrain(context, brainId);
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
