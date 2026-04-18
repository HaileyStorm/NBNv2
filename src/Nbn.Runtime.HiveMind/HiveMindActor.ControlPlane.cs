using Nbn.Proto.Viz;
using Nbn.Runtime.IO;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Nbn.Shared.HiveMind;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoIo = Nbn.Proto.Io;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private bool HandleControlPlaneMessage(IContext context)
    {
        switch (context.Message)
        {
            case ProtoControl.RegisterBrain message:
                HandleRegisterBrain(context, message);
                return true;
            case ProtoControl.UpdateBrainSignalRouter message:
                HandleUpdateBrainSignalRouter(context, message);
                return true;
            case ProtoControl.UnregisterBrain message:
                HandleUnregisterBrain(context, message);
                return true;
            case ProtoControl.RegisterShard message:
                HandleRegisterShard(context, message);
                return true;
            case ProtoControl.UnregisterShard message:
                HandleUnregisterShard(context, message);
                return true;
            case ProtoControl.RegisterOutputSink message:
                HandleRegisterOutputSink(context, message);
                return true;
            case ProtoControl.SetBrainVisualization message:
                HandleSetBrainVisualization(context, message);
                return true;
            case ProtoControl.SetBrainCostEnergy message:
                HandleSetBrainCostEnergy(context, message);
                return true;
            case ProtoControl.SetBrainPlasticity message:
                HandleSetBrainPlasticity(context, message);
                return true;
            case ProtoControl.SetBrainHomeostasis message:
                HandleSetBrainHomeostasis(context, message);
                return true;
            case PauseBrainRequest message:
                HandlePauseBrainRequest(context, message);
                return true;
            case ResumeBrainRequest message:
                HandleResumeBrainRequest(context, message);
                return true;
            case ProtoControl.PauseBrain message:
                HandlePauseBrainControl(context, message);
                return true;
            case ProtoControl.ResumeBrain message:
                HandleResumeBrainControl(context, message);
                return true;
            case ProtoControl.KillBrain message:
                HandleKillBrainControl(context, message);
                return true;
            default:
                return false;
        }
    }

    private void RegisterBrainInternal(IContext context, Guid brainId, PID? brainRootPid, PID? routerPid, int? pausePriority = null)
    {
        var isNew = !_brains.TryGetValue(brainId, out var brain) || brain is null;
        if (isNew)
        {
            brain = new BrainState(brainId, _outputVectorSource)
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
            brain = new BrainState(brainId, _outputVectorSource);
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

        RemoveQueuedPlacementDispatches(context, brainId, brain.PlacementEpoch);
        DispatchPlacementUnassignments(context, brain, brain.PlacementExecution, reason);
        ReleaseBrainVisualizationSubscribers(context, brain);
        if (brain.PendingRuntimeReset is not null)
        {
            brain.PendingRuntimeReset.Completion.TrySetResult(
                BuildRuntimeResetAck(brainId, success: false, $"brain_unregistered:{reason}"));
            _pendingBarrierResets.Remove(brainId);
            brain.PendingRuntimeReset = null;
        }
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
            RememberCompletedSpawn(new CompletedSpawnState(
                brainId,
                pendingSpawn.PlacementEpoch,
                AcceptedForPlacement: true,
                PlacementReady: false,
                brain.PlacementLifecycleState,
                brain.PlacementReconcileState,
                reasonCode,
                failureMessage));
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
            brain = new BrainState(brainId, _outputVectorSource);
            _brains.Add(brainId, brain);
        }

        if (!ShardId32.TryFrom(regionId, shardIndex, out var shardId))
        {
            Log($"RegisterShard invalid shard index: brain {brainId} region {regionId} shardIndex {shardIndex}.");
            return;
        }

        if (brain.PlacementEpoch > 0
            && (brain.PlacementExecution is null || brain.PlacementExecution.Completed)
            && brain.PlacementLifecycleState is
                ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed
                or ProtoControl.PlacementLifecycleState.PlacementLifecycleTerminated)
        {
            Log($"RegisterShard ignored for failed placement: brain {brainId} region {regionId} shardIndex {shardIndex}.");
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
        SendShardRuntimeConfigUpdate(context, shardId, normalized, CreateShardRuntimeConfigUpdate(brain, shardId));
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
}
