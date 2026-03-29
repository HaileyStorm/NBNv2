using Nbn.Runtime.Brain;
using Nbn.Runtime.IO;
using Nbn.Shared;
using Nbn.Shared.HiveMind;
using Nbn.Shared.Addressing;
using Proto;
using ProtoIo = Nbn.Proto.Io;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private bool HandleRuntimeSurfaceMessage(IContext context)
    {
        switch (context.Message)
        {
            case ProtoControl.GetBrainIoInfo message:
                if (message.BrainId is not null && message.BrainId.TryToGuid(out var ioBrainId))
                {
                    context.Respond(BuildBrainIoInfo(ioBrainId));
                }
                else
                {
                    context.Respond(new ProtoControl.BrainIoInfo());
                }

                return true;
            case GetBrainRouting message:
                context.Respond(BuildRoutingInfo(message.BrainId));
                return true;
            case ProtoControl.GetBrainRouting message:
                if (message.BrainId is not null && message.BrainId.TryToGuid(out var routingBrainId))
                {
                    context.Respond(BuildRoutingInfoProto(routingBrainId));
                }
                else
                {
                    context.Respond(new ProtoControl.BrainRoutingInfo());
                }

                return true;
            default:
                return false;
        }
    }

    private BrainRoutingInfo BuildRoutingInfo(Guid brainId)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return new BrainRoutingInfo(brainId, null, null, 0, 0);
        }

        return new BrainRoutingInfo(
            brain.BrainId,
            brain.BrainRootPid,
            brain.SignalRouterPid,
            brain.Shards.Count,
            brain.RoutingSnapshot.Count);
    }

    private ProtoControl.BrainRoutingInfo BuildRoutingInfoProto(Guid brainId)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return new ProtoControl.BrainRoutingInfo
            {
                BrainId = brainId.ToProtoUuid()
            };
        }

        return new ProtoControl.BrainRoutingInfo
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            BrainRootPid = brain.BrainRootPid is null ? string.Empty : PidLabel(brain.BrainRootPid),
            SignalRouterPid = brain.SignalRouterPid is null ? string.Empty : PidLabel(brain.SignalRouterPid),
            ShardCount = (uint)brain.Shards.Count,
            RoutingCount = (uint)brain.RoutingSnapshot.Count
        };
    }

    private ProtoControl.BrainIoInfo BuildBrainIoInfo(Guid brainId)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return new ProtoControl.BrainIoInfo
            {
                BrainId = brainId.ToProtoUuid(),
                InputCoordinatorMode = _inputCoordinatorMode,
                OutputVectorSource = _outputVectorSource,
                InputCoordinatorPid = string.Empty,
                OutputCoordinatorPid = string.Empty,
                IoGatewayOwnsInputCoordinator = false,
                IoGatewayOwnsOutputCoordinator = false
            };
        }

        var outputCoordinatorPid = brain.OutputCoordinatorPid ?? brain.OutputSinkPid;
        var ioGatewayOwnsInputCoordinator = ResolveIoGatewayOwnsInputCoordinator(brain);
        var ioGatewayOwnsOutputCoordinator = ResolveIoGatewayOwnsOutputCoordinator(brain, outputCoordinatorPid);

        return new ProtoControl.BrainIoInfo
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            InputWidth = (uint)Math.Max(0, brain.InputWidth),
            OutputWidth = (uint)Math.Max(0, brain.OutputWidth),
            InputCoordinatorMode = _inputCoordinatorMode,
            OutputVectorSource = brain.OutputVectorSource,
            InputCoordinatorPid = brain.InputCoordinatorPid is null ? string.Empty : PidLabel(brain.InputCoordinatorPid),
            OutputCoordinatorPid = outputCoordinatorPid is null ? string.Empty : PidLabel(outputCoordinatorPid),
            IoGatewayOwnsInputCoordinator = ioGatewayOwnsInputCoordinator,
            IoGatewayOwnsOutputCoordinator = ioGatewayOwnsOutputCoordinator
        };
    }

    private void UpdateRoutingTable(IContext? context, BrainState brain)
    {
        var snapshot = RoutingTableSnapshot.Empty;
        if (brain.Shards.Count > 0)
        {
            var routes = new List<ShardRoute>(brain.Shards.Count);
            foreach (var entry in brain.Shards)
            {
                routes.Add(new ShardRoute(entry.Key.Value, entry.Value));
            }

            snapshot = new RoutingTableSnapshot(routes);
        }

        brain.RoutingSnapshot = snapshot;

        if (context is null)
        {
            return;
        }

        if (brain.SignalRouterPid is not null)
        {
            SendRoutingTable(context, brain.SignalRouterPid, brain.RoutingSnapshot, "SignalRouter");
        }

        if (brain.BrainRootPid is not null && brain.BrainRootPid != brain.SignalRouterPid)
        {
            SendRoutingTable(context, brain.BrainRootPid, brain.RoutingSnapshot, "BrainRoot");
        }
    }

    private void UpdateOutputSinks(IContext context, BrainState brain)
    {
        foreach (var entry in brain.Shards)
        {
            if (entry.Key.RegionId != NbnConstants.OutputRegionId)
            {
                continue;
            }

            SendOutputSinkUpdate(context, brain.BrainId, entry.Key, entry.Value, brain.OutputSinkPid);
        }

        if (brain.OutputSinkPid is null)
        {
            Log($"Output sink missing for brain {brain.BrainId}; output shards were cleared.");
        }
    }

    private VisualizationSubscriber ResolveVisualizationSubscriber(IContext context, ProtoControl.SetBrainVisualization message)
    {
        if (TryParsePid(message.SubscriberActor, out var parsedSubscriberPid))
        {
            var normalizedSubscriberPid = NormalizePid(context, parsedSubscriberPid) ?? parsedSubscriberPid;
            normalizedSubscriberPid = ResolveSendTargetPid(context, normalizedSubscriberPid);
            return new VisualizationSubscriber(PidLabel(normalizedSubscriberPid), normalizedSubscriberPid);
        }

        if (context.Sender is not null)
        {
            var normalizedSender = NormalizePid(context, context.Sender) ?? context.Sender;
            normalizedSender = ResolveSendTargetPid(context, normalizedSender);
            return new VisualizationSubscriber(PidLabel(normalizedSender), normalizedSender);
        }

        // Legacy senderless requests map to one shared slot to preserve compatibility.
        return new VisualizationSubscriber("legacy:senderless", null);
    }

    private void SetBrainVisualization(
        IContext context,
        BrainState brain,
        VisualizationSubscriber subscriber,
        bool enabled,
        uint? focusRegionId)
    {
        if (enabled)
        {
            var isNewSubscription = !brain.VisualizationSubscribers.ContainsKey(subscriber.Key);
            brain.VisualizationSubscribers[subscriber.Key] = new VisualizationSubscriberPreference(
                subscriber.Key,
                focusRegionId,
                subscriber.Pid);
            if (isNewSubscription)
            {
                RetainVisualizationSubscriberLease(context, subscriber);
            }
            else if (subscriber.Pid is not null)
            {
                RefreshVisualizationSubscriberLeasePid(context, subscriber);
            }
        }
        else if (brain.VisualizationSubscribers.Remove(subscriber.Key))
        {
            ReleaseVisualizationSubscriberLease(context, subscriber.Key);
        }

        ApplyEffectiveVisualizationScope(context, brain);
    }

    private void RetainVisualizationSubscriberLease(IContext context, VisualizationSubscriber subscriber)
    {
        if (_vizSubscriberLeases.TryGetValue(subscriber.Key, out var existingLease))
        {
            existingLease.Retain(context, subscriber.Pid);
            return;
        }

        var lease = new VisualizationSubscriberLease(subscriber.Key, subscriber.Pid);
        lease.Retain(context, subscriber.Pid);
        _vizSubscriberLeases.Add(subscriber.Key, lease);
    }

    private void RefreshVisualizationSubscriberLeasePid(IContext context, VisualizationSubscriber subscriber)
    {
        if (subscriber.Pid is null || !_vizSubscriberLeases.TryGetValue(subscriber.Key, out var lease))
        {
            return;
        }

        lease.RefreshPid(context, subscriber.Pid);
    }

    private void ReleaseVisualizationSubscriberLease(IContext context, string subscriberKey)
    {
        if (!_vizSubscriberLeases.TryGetValue(subscriberKey, out var lease))
        {
            return;
        }

        if (!lease.Release(context))
        {
            return;
        }

        _vizSubscriberLeases.Remove(subscriberKey);
    }

    private void ApplyEffectiveVisualizationScope(IContext context, BrainState brain)
    {
        var nextEnabled = brain.VisualizationSubscribers.Count > 0;
        var nextFocusRegionId = nextEnabled
            ? ComputeEffectiveVisualizationFocus(brain.VisualizationSubscribers.Values)
            : null;
        if (brain.VisualizationEnabled == nextEnabled && brain.VisualizationFocusRegionId == nextFocusRegionId)
        {
            return;
        }

        brain.VisualizationEnabled = nextEnabled;
        brain.VisualizationFocusRegionId = nextEnabled ? nextFocusRegionId : null;
        foreach (var entry in brain.Shards)
        {
            SendShardVisualizationUpdate(
                context,
                brain.BrainId,
                entry.Key,
                entry.Value,
                nextEnabled,
                brain.VisualizationFocusRegionId,
                _vizStreamMinIntervalMs);
        }

        EmitDebug(
            context,
            ProtoSeverity.SevDebug,
            "viz.toggle",
            $"Brain={brain.BrainId} enabled={nextEnabled} focus={(brain.VisualizationFocusRegionId.HasValue ? $"R{brain.VisualizationFocusRegionId.Value}" : "all")} subscribers={brain.VisualizationSubscribers.Count} shards={brain.Shards.Count}");
    }

    private static uint? ComputeEffectiveVisualizationFocus(IEnumerable<VisualizationSubscriberPreference> preferences)
    {
        uint? commonFocusRegionId = null;
        var hasFocusedSubscriber = false;
        foreach (var preference in preferences)
        {
            if (!preference.FocusRegionId.HasValue)
            {
                // Any full-brain subscriber requires full-brain emission.
                return null;
            }

            var focusRegionId = preference.FocusRegionId.Value;
            if (!hasFocusedSubscriber)
            {
                commonFocusRegionId = focusRegionId;
                hasFocusedSubscriber = true;
                continue;
            }

            if (commonFocusRegionId != focusRegionId)
            {
                // Conflicting focus subscriptions fall back to full-brain emission.
                return null;
            }
        }

        return hasFocusedSubscriber ? commonFocusRegionId : null;
    }

    private void HandleVisualizationSubscriberTerminated(IContext context, PID terminatedPid)
    {
        if (!TryResolveVisualizationSubscriberKey(terminatedPid, out var subscriberKey))
        {
            return;
        }

        RemoveVisualizationSubscriber(context, subscriberKey);
    }

    private bool TryResolveVisualizationSubscriberKey(PID terminatedPid, out string subscriberKey)
    {
        subscriberKey = PidLabel(terminatedPid);
        if (_vizSubscriberLeases.ContainsKey(subscriberKey))
        {
            return true;
        }

        foreach (var lease in _vizSubscriberLeases)
        {
            if (lease.Value.Matches(terminatedPid))
            {
                subscriberKey = lease.Key;
                return true;
            }
        }

        return false;
    }

    private void RemoveVisualizationSubscriber(IContext context, string subscriberKey)
    {
        if (!_vizSubscriberLeases.Remove(subscriberKey, out var lease))
        {
            return;
        }

        lease.Unwatch(context);
        foreach (var brain in _brains.Values)
        {
            if (!brain.VisualizationSubscribers.Remove(subscriberKey))
            {
                continue;
            }

            ApplyEffectiveVisualizationScope(context, brain);
        }
    }

    private void ReleaseBrainVisualizationSubscribers(IContext context, BrainState brain)
    {
        if (brain.VisualizationSubscribers.Count == 0)
        {
            return;
        }

        foreach (var key in brain.VisualizationSubscribers.Keys.ToList())
        {
            ReleaseVisualizationSubscriberLease(context, key);
        }

        brain.VisualizationSubscribers.Clear();
        brain.VisualizationEnabled = false;
        brain.VisualizationFocusRegionId = null;
    }

    private void UpdateShardRuntimeConfig(IContext context, BrainState brain)
    {
        var effectiveCostEnergyEnabled = ResolveEffectiveCostEnergyEnabled(brain);
        var effectivePlasticityEnabled = ResolveEffectivePlasticityEnabled(brain);
        var effectivePlasticityDelta = ResolvePlasticityDelta(brain.PlasticityRate, brain.PlasticityDelta);
        foreach (var entry in brain.Shards)
        {
            SendShardRuntimeConfigUpdate(
                context,
                brain.BrainId,
                entry.Key,
                entry.Value,
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
                brain.OutputVectorSource,
                _debugStreamEnabled,
                _debugMinSeverity);
        }
    }

    private void RegisterBrainWithIo(IContext context, BrainState brain, bool force = false)
    {
        if (_ioPid is null)
        {
            if (LogMetadataDiagnostics)
            {
                Log(
                    $"MetaDiag register hm->io skipped brain={brain.BrainId} epoch={brain.PlacementEpoch} reason=io_pid_unavailable force={force}");
            }

            return;
        }

        var rawInputWidth = (uint)Math.Max(0, brain.InputWidth);
        var rawOutputWidth = (uint)Math.Max(0, brain.OutputWidth);
        if ((rawInputWidth == 0 || rawOutputWidth == 0)
            && !HasArtifactRef(brain.BaseDefinition)
            && !HasArtifactRef(brain.LastSnapshot))
        {
            if (LogMetadataDiagnostics)
            {
                Log(
                    $"MetaDiag register hm->io skipped brain={brain.BrainId} epoch={brain.PlacementEpoch} reason=invalid_widths input={rawInputWidth} output={rawOutputWidth} force={force}");
            }

            return;
        }

        var inputWidth = rawInputWidth == 0 ? 1u : rawInputWidth;
        var outputWidth = rawOutputWidth == 0 ? 1u : rawOutputWidth;
        var inputCoordinatorPidLabel = brain.InputCoordinatorPid is null
            ? string.Empty
            : PidLabel(brain.InputCoordinatorPid);
        var outputCoordinatorPid = brain.OutputCoordinatorPid ?? brain.OutputSinkPid;
        var outputCoordinatorPidLabel = outputCoordinatorPid is null
            ? string.Empty
            : PidLabel(outputCoordinatorPid);
        var ioGatewayOwnsInputCoordinator = ResolveIoGatewayOwnsInputCoordinator(brain);
        var ioGatewayOwnsOutputCoordinator = ResolveIoGatewayOwnsOutputCoordinator(brain, outputCoordinatorPid);

        if (!force
            && brain.IoRegistered
            && brain.IoRegisteredInputWidth == inputWidth
            && brain.IoRegisteredOutputWidth == outputWidth
            && brain.IoRegisteredInputCoordinatorMode == _inputCoordinatorMode
            && brain.IoRegisteredOutputVectorSource == brain.OutputVectorSource
            && brain.IoRegisteredOwnsInputCoordinator == ioGatewayOwnsInputCoordinator
            && brain.IoRegisteredOwnsOutputCoordinator == ioGatewayOwnsOutputCoordinator
            && string.Equals(brain.IoRegisteredInputCoordinatorPid, inputCoordinatorPidLabel, StringComparison.Ordinal)
            && string.Equals(brain.IoRegisteredOutputCoordinatorPid, outputCoordinatorPidLabel, StringComparison.Ordinal))
        {
            if (LogMetadataDiagnostics)
            {
                Log(
                    $"MetaDiag register hm->io skipped brain={brain.BrainId} epoch={brain.PlacementEpoch} reason=already_registered input={inputWidth} output={outputWidth}");
            }

            return;
        }

        var effectiveCostEnergyEnabled = ResolveEffectiveCostEnergyEnabled(brain);
        var effectivePlasticityEnabled = ResolveEffectivePlasticityEnabled(brain);
        var effectivePlasticityDelta = ResolvePlasticityDelta(brain.PlasticityRate, brain.PlasticityDelta);
        var register = new ProtoIo.RegisterBrain
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            InputWidth = inputWidth,
            OutputWidth = outputWidth,
            HasRuntimeConfig = true,
            CostEnabled = effectiveCostEnergyEnabled,
            EnergyEnabled = effectiveCostEnergyEnabled,
            PlasticityEnabled = effectivePlasticityEnabled,
            PlasticityRate = brain.PlasticityRate,
            PlasticityProbabilisticUpdates = brain.PlasticityProbabilisticUpdates,
            PlasticityDelta = effectivePlasticityDelta,
            PlasticityRebaseThreshold = brain.PlasticityRebaseThreshold,
            PlasticityRebaseThresholdPct = brain.PlasticityRebaseThresholdPct,
            PlasticityEnergyCostModulationEnabled = brain.PlasticityEnergyCostModulationEnabled,
            PlasticityEnergyCostReferenceTickCost = brain.PlasticityEnergyCostReferenceTickCost,
            PlasticityEnergyCostResponseStrength = brain.PlasticityEnergyCostResponseStrength,
            PlasticityEnergyCostMinScale = brain.PlasticityEnergyCostMinScale,
            PlasticityEnergyCostMaxScale = brain.PlasticityEnergyCostMaxScale,
            HomeostasisEnabled = brain.HomeostasisEnabled,
            HomeostasisTargetMode = brain.HomeostasisTargetMode,
            HomeostasisUpdateMode = brain.HomeostasisUpdateMode,
            HomeostasisBaseProbability = brain.HomeostasisBaseProbability,
            HomeostasisMinStepCodes = brain.HomeostasisMinStepCodes,
            HomeostasisEnergyCouplingEnabled = brain.HomeostasisEnergyCouplingEnabled,
            HomeostasisEnergyTargetScale = brain.HomeostasisEnergyTargetScale,
            HomeostasisEnergyProbabilityScale = brain.HomeostasisEnergyProbabilityScale,
            InputCoordinatorMode = _inputCoordinatorMode,
            OutputVectorSource = brain.OutputVectorSource,
            LastTickCost = brain.LastTickCost,
            InputCoordinatorPid = inputCoordinatorPidLabel,
            OutputCoordinatorPid = outputCoordinatorPidLabel,
            IoGatewayOwnsInputCoordinator = ioGatewayOwnsInputCoordinator,
            IoGatewayOwnsOutputCoordinator = ioGatewayOwnsOutputCoordinator
        };

        if (brain.BaseDefinition is not null)
        {
            register.BaseDefinition = brain.BaseDefinition;
        }

        if (brain.LastSnapshot is not null)
        {
            register.LastSnapshot = brain.LastSnapshot;
        }

        var ioPid = ResolveSendTargetPid(context, _ioPid);
        context.Send(ioPid, register);

        if (LogMetadataDiagnostics)
        {
            Log(
                $"MetaDiag register hm->io sent brain={brain.BrainId} epoch={brain.PlacementEpoch} io={PidLabel(ioPid)} input={inputWidth} output={outputWidth} base={ArtifactLabel(brain.BaseDefinition)} snapshot={ArtifactLabel(brain.LastSnapshot)} force={force}");
        }

        brain.IoRegistered = true;
        brain.IoRegisteredInputWidth = inputWidth;
        brain.IoRegisteredOutputWidth = outputWidth;
        brain.IoRegisteredInputCoordinatorMode = _inputCoordinatorMode;
        brain.IoRegisteredOutputVectorSource = brain.OutputVectorSource;
        brain.IoRegisteredOwnsInputCoordinator = ioGatewayOwnsInputCoordinator;
        brain.IoRegisteredOwnsOutputCoordinator = ioGatewayOwnsOutputCoordinator;
        brain.IoRegisteredInputCoordinatorPid = inputCoordinatorPidLabel;
        brain.IoRegisteredOutputCoordinatorPid = outputCoordinatorPidLabel;
    }

    private static bool ResolveIoGatewayOwnsInputCoordinator(BrainState brain)
        => brain.InputCoordinatorPid is null
           && !HasPlacementAssignmentTarget(brain, ProtoControl.PlacementAssignmentTarget.PlacementTargetInputCoordinator);

    private static bool ResolveIoGatewayOwnsOutputCoordinator(BrainState brain, PID? outputCoordinatorPid)
        => outputCoordinatorPid is null
           && !HasPlacementAssignmentTarget(brain, ProtoControl.PlacementAssignmentTarget.PlacementTargetOutputCoordinator);

    private static bool HasPlacementAssignmentTarget(BrainState brain, ProtoControl.PlacementAssignmentTarget target)
    {
        var execution = brain.PlacementExecution;
        if (execution is null)
        {
            return false;
        }

        foreach (var assignmentState in execution.Assignments.Values)
        {
            if (assignmentState.Assignment.Target == target
                && assignmentState.Assignment.PlacementEpoch == brain.PlacementEpoch)
            {
                return true;
            }
        }

        return false;
    }

    private static void SendOutputSinkUpdate(IContext context, Guid brainId, ShardId32 shardId, PID shardPid, PID? outputSink)
    {
        try
        {
            context.Send(shardPid, new ProtoControl.UpdateShardOutputSink
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = (uint)shardId.RegionId,
                ShardIndex = (uint)shardId.ShardIndex,
                OutputPid = outputSink is null ? string.Empty : PidLabel(outputSink)
            });
        }
        catch (Exception ex)
        {
            LogError($"Failed to update output sink for shard {shardId}: {ex.Message}");
        }
    }

    private static void SendShardVisualizationUpdate(
        IContext context,
        Guid brainId,
        ShardId32 shardId,
        PID shardPid,
        bool enabled,
        uint? focusRegionId,
        uint vizStreamMinIntervalMs)
    {
        try
        {
            context.Send(shardPid, new ProtoControl.UpdateShardVisualization
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = (uint)shardId.RegionId,
                ShardIndex = (uint)shardId.ShardIndex,
                Enabled = enabled,
                HasFocusRegion = focusRegionId.HasValue,
                FocusRegionId = focusRegionId ?? 0,
                VizStreamMinIntervalMs = vizStreamMinIntervalMs
            });
        }
        catch (Exception ex)
        {
            LogError($"Failed to update shard visualization for shard {shardId}: {ex.Message}");
        }
    }

    private static void SendShardRuntimeConfigUpdate(
        IContext context,
        Guid brainId,
        ShardId32 shardId,
        PID shardPid,
        bool costEnabled,
        bool energyEnabled,
        bool plasticityEnabled,
        float plasticityRate,
        bool plasticityProbabilisticUpdates,
        float plasticityDelta,
        uint plasticityRebaseThreshold,
        float plasticityRebaseThresholdPct,
        bool plasticityEnergyCostModulationEnabled,
        long plasticityEnergyCostReferenceTickCost,
        float plasticityEnergyCostResponseStrength,
        float plasticityEnergyCostMinScale,
        float plasticityEnergyCostMaxScale,
        bool homeostasisEnabled,
        ProtoControl.HomeostasisTargetMode homeostasisTargetMode,
        ProtoControl.HomeostasisUpdateMode homeostasisUpdateMode,
        float homeostasisBaseProbability,
        uint homeostasisMinStepCodes,
        bool homeostasisEnergyCouplingEnabled,
        float homeostasisEnergyTargetScale,
        float homeostasisEnergyProbabilityScale,
        bool remoteCostEnabled,
        long remoteCostPerBatch,
        long remoteCostPerContribution,
        float costTierAMultiplier,
        float costTierBMultiplier,
        float costTierCMultiplier,
        ProtoControl.OutputVectorSource outputVectorSource,
        bool debugEnabled,
        ProtoSeverity debugMinSeverity)
    {
        try
        {
            context.Send(shardPid, new ProtoControl.UpdateShardRuntimeConfig
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = (uint)shardId.RegionId,
                ShardIndex = (uint)shardId.ShardIndex,
                CostEnabled = costEnabled,
                EnergyEnabled = energyEnabled,
                PlasticityEnabled = plasticityEnabled,
                PlasticityRate = plasticityRate,
                ProbabilisticUpdates = plasticityProbabilisticUpdates,
                PlasticityDelta = plasticityDelta,
                PlasticityRebaseThreshold = plasticityRebaseThreshold,
                PlasticityRebaseThresholdPct = plasticityRebaseThresholdPct,
                PlasticityEnergyCostModulationEnabled = plasticityEnergyCostModulationEnabled,
                PlasticityEnergyCostReferenceTickCost = plasticityEnergyCostReferenceTickCost,
                PlasticityEnergyCostResponseStrength = plasticityEnergyCostResponseStrength,
                PlasticityEnergyCostMinScale = plasticityEnergyCostMinScale,
                PlasticityEnergyCostMaxScale = plasticityEnergyCostMaxScale,
                HomeostasisEnabled = homeostasisEnabled,
                HomeostasisTargetMode = homeostasisTargetMode,
                HomeostasisUpdateMode = homeostasisUpdateMode,
                HomeostasisBaseProbability = homeostasisBaseProbability,
                HomeostasisMinStepCodes = homeostasisMinStepCodes,
                HomeostasisEnergyCouplingEnabled = homeostasisEnergyCouplingEnabled,
                HomeostasisEnergyTargetScale = homeostasisEnergyTargetScale,
                HomeostasisEnergyProbabilityScale = homeostasisEnergyProbabilityScale,
                RemoteCostEnabled = remoteCostEnabled,
                RemoteCostPerBatch = remoteCostPerBatch,
                RemoteCostPerContribution = remoteCostPerContribution,
                CostTierAMultiplier = costTierAMultiplier,
                CostTierBMultiplier = costTierBMultiplier,
                CostTierCMultiplier = costTierCMultiplier,
                OutputVectorSource = outputVectorSource,
                DebugEnabled = debugEnabled,
                DebugMinSeverity = debugMinSeverity
            });
        }
        catch (Exception ex)
        {
            LogError($"Failed to update shard runtime config for shard {shardId}: {ex.Message}");
        }
    }


}
