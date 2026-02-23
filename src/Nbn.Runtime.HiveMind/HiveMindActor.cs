using System.Security.Cryptography;
using System.Text;
using Nbn.Proto.Debug;
using Nbn.Proto.Viz;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.Brain;
using Nbn.Runtime.IO;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Nbn.Shared.Format;
using Nbn.Shared.HiveMind;
using Nbn.Shared.Quantization;
using Nbn.Shared.Validation;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoIo = Nbn.Proto.Io;
using ProtoSettings = Nbn.Proto.Settings;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed class HiveMindActor : IActor
{
    private readonly HiveMindOptions _options;
    private readonly BackpressureController _backpressure;
    private readonly PID? _settingsPid;
    private readonly PID? _ioPid;
    private readonly PID? _debugHubPid;
    private readonly PID? _vizHubPid;
    private readonly Dictionary<Guid, BrainState> _brains = new();
    private readonly Dictionary<Guid, WorkerCatalogEntry> _workerCatalog = new();
    private readonly HashSet<ShardKey> _pendingCompute = new();
    private readonly HashSet<Guid> _pendingDeliver = new();
    private ulong _vizSequence;
    private long _workerCatalogSnapshotMs;
    private static readonly TimeSpan SnapshotShardRequestTimeout = TimeSpan.FromSeconds(5);
    private static readonly QuantizationMap SnapshotBufferQuantization = QuantizationSchemas.DefaultBuffer;

    private TickState? _tick;
    private TickPhase _phase = TickPhase.Idle;
    private bool _tickLoopEnabled;
    private bool _rescheduleInProgress;
    private bool _rescheduleQueued;
    private ulong _lastRescheduleTick;
    private DateTime _lastRescheduleAt;
    private string? _queuedRescheduleReason;
    private ulong _lastCompletedTickId;

    public HiveMindActor(
        HiveMindOptions options,
        PID? debugHubPid = null,
        PID? vizHubPid = null,
        PID? ioPid = null,
        PID? settingsPid = null)
    {
        _options = options;
        _backpressure = new BackpressureController(options);
        _tickLoopEnabled = options.AutoStart;
        _settingsPid = settingsPid ?? BuildSettingsPid(options);
        _ioPid = ioPid ?? BuildIoPid(options);
        var resolvedObs = ObservabilityTargets.Resolve(options.SettingsHost);
        _debugHubPid = debugHubPid ?? resolvedObs.DebugHub;
        _vizHubPid = vizHubPid ?? resolvedObs.VizHub;
    }

    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
                if (_tickLoopEnabled)
                {
                    ScheduleNextTick(context, TimeSpan.Zero);
                }

                if (_settingsPid is not null)
                {
                    ScheduleSelf(context, TimeSpan.Zero, new RefreshWorkerInventoryTick());
                }
                break;
            case StartTickLoop:
                _tickLoopEnabled = true;
                if (_phase == TickPhase.Idle && !_rescheduleInProgress)
                {
                    ScheduleNextTick(context, TimeSpan.Zero);
                }
                break;
            case StopTickLoop:
                _tickLoopEnabled = false;
                break;
            case TickStart:
                if (_tickLoopEnabled && !_rescheduleInProgress && _phase == TickPhase.Idle)
                {
                    StartTick(context);
                }
                break;
            case ProtoControl.RegisterBrain message:
                HandleRegisterBrain(context, message);
                break;
            case ProtoControl.UpdateBrainSignalRouter message:
                HandleUpdateBrainSignalRouter(context, message);
                break;
            case ProtoControl.UnregisterBrain message:
                HandleUnregisterBrain(context, message);
                break;
            case ProtoControl.RegisterShard message:
                HandleRegisterShard(context, message);
                break;
            case ProtoControl.UnregisterShard message:
                HandleUnregisterShard(context, message);
                break;
            case ProtoControl.RegisterOutputSink message:
                HandleRegisterOutputSink(context, message);
                break;
            case ProtoControl.SetBrainVisualization message:
                HandleSetBrainVisualization(context, message);
                break;
            case ProtoControl.SetBrainCostEnergy message:
                HandleSetBrainCostEnergy(context, message);
                break;
            case ProtoControl.SetBrainPlasticity message:
                HandleSetBrainPlasticity(context, message);
                break;
            case ProtoControl.GetBrainIoInfo message:
                if (message.BrainId is not null && message.BrainId.TryToGuid(out var ioBrainId))
                {
                    context.Respond(BuildBrainIoInfo(ioBrainId));
                }
                else
                {
                    context.Respond(new ProtoControl.BrainIoInfo());
                }
                break;
            case ProtoIo.ExportBrainDefinition message:
                HandleExportBrainDefinition(context, message);
                break;
            case ProtoIo.RequestSnapshot message:
                HandleRequestSnapshot(context, message);
                break;
            case ProtoControl.RequestPlacement message:
                HandleRequestPlacement(context, message);
                break;
            case ProtoControl.GetPlacementLifecycle message:
                if (message.BrainId is not null && message.BrainId.TryToGuid(out var placementBrainId))
                {
                    context.Respond(BuildPlacementLifecycleInfo(placementBrainId));
                }
                else
                {
                    context.Respond(new ProtoControl.PlacementLifecycleInfo());
                }
                break;
            case ProtoControl.PlacementWorkerInventoryRequest:
                context.Respond(BuildPlacementWorkerInventory());
                break;
            case ProtoControl.PlacementAssignmentAck message:
                HandlePlacementAssignmentAck(context, message);
                break;
            case ProtoControl.PlacementReconcileReport message:
                HandlePlacementReconcileReport(context, message);
                break;
            case ProtoSettings.WorkerInventorySnapshotResponse message:
                HandleWorkerInventorySnapshotResponse(message);
                break;
            case PauseBrainRequest message:
                PauseBrain(context, message.BrainId, message.Reason);
                break;
            case ResumeBrainRequest message:
                ResumeBrain(context, message.BrainId);
                break;
            case ProtoControl.PauseBrain message:
                if (message.BrainId.TryToGuid(out var pauseId))
                {
                    PauseBrain(context, pauseId, message.Reason);
                }
                break;
            case ProtoControl.ResumeBrain message:
                if (message.BrainId.TryToGuid(out var resumeId))
                {
                    ResumeBrain(context, resumeId);
                }
                break;
            case ProtoControl.KillBrain message:
                if (message.BrainId.TryToGuid(out var killId))
                {
                    KillBrain(context, killId, message.Reason);
                }
                break;
            case ProtoControl.TickComputeDone message:
                HandleTickComputeDone(context, message);
                break;
            case ProtoControl.TickDeliverDone message:
                HandleTickDeliverDone(context, message);
                break;
            case TickPhaseTimeout message:
                HandleTickPhaseTimeout(context, message);
                break;
            case RefreshWorkerInventoryTick:
                RefreshWorkerInventory(context);
                break;
            case RescheduleNow message:
                BeginReschedule(context, message);
                break;
            case RescheduleCompleted message:
                CompleteReschedule(context, message);
                break;
            case ProtoControl.GetHiveMindStatus:
                context.Respond(BuildStatus());
                break;
            case ProtoControl.SetTickRateOverride message:
                HandleSetTickRateOverride(context, message);
                break;
            case GetBrainRouting message:
                context.Respond(BuildRoutingInfo(message.BrainId));
                break;
            case ProtoControl.GetBrainRouting message:
                if (message.BrainId is not null && message.BrainId.TryToGuid(out var routingBrainId))
                {
                    context.Respond(BuildRoutingInfoProto(routingBrainId));
                }
                else
                {
                    context.Respond(new ProtoControl.BrainRoutingInfo());
                }
                break;
        }

        return Task.CompletedTask;
    }

    private void RegisterBrainInternal(IContext context, Guid brainId, PID? brainRootPid, PID? routerPid)
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
        if (!_brains.Remove(brainId))
        {
            return;
        }

        if (notifyIoUnregister && _ioPid is not null)
        {
            context.Send(_ioPid, new ProtoIo.UnregisterBrain
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
            if (_pendingDeliver.Remove(brainId))
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

        var normalized = NormalizePid(context, shardPid) ?? shardPid;
        brain.Shards[shardId] = normalized;
        SendShardVisualizationUpdate(
            context,
            brainId,
            shardId,
            normalized,
            brain.VisualizationEnabled,
            brain.VisualizationFocusRegionId);
        SendShardRuntimeConfigUpdate(
            context,
            brainId,
            shardId,
            normalized,
            brain.CostEnabled,
            brain.EnergyEnabled,
            brain.PlasticityEnabled,
            brain.PlasticityRate,
            brain.PlasticityProbabilisticUpdates);
        UpdateRoutingTable(context, brain);
        if (brain.PlacementEpoch > 0)
        {
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning,
                ProtoControl.PlacementFailureReason.PlacementFailureNone);
            brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileMatched;
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
            Log($"Shard registered mid-compute for brain {brainId}; will start next tick.");
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

            brain.Shards.Remove(shardId);
            UpdateRoutingTable(context, brain);
            if (brain.PlacementEpoch > 0 && brain.Shards.Count == 0)
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
            && _pendingCompute.Remove(new ShardKey(brainId, pendingShardId)))
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
        RegisterBrainInternal(context, brainId, brainRootPid, routerPid);
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

        UpdateBrainSignalRouter(context, brainId, routerPid);
    }

    private void HandleUnregisterBrain(IContext context, ProtoControl.UnregisterBrain message)
    {
        if (TryGetGuid(message.BrainId, out var brainId))
        {
            UnregisterBrain(context, brainId);
        }
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
            context.Send(_ioPid, new ProtoControl.BrainTerminated
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

        RegisterShardInternal(
            context,
            brainId,
            (int)message.RegionId,
            (int)message.ShardIndex,
            shardPid,
            (int)message.NeuronStart,
            (int)message.NeuronCount);
    }

    private void HandleUnregisterShard(IContext context, ProtoControl.UnregisterShard message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        UnregisterShardInternal(context, brainId, (int)message.RegionId, (int)message.ShardIndex);
    }

    private void HandleRegisterOutputSink(IContext context, ProtoControl.RegisterOutputSink message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        if (!_brains.TryGetValue(brainId, out var brain))
        {
            brain = new BrainState(brainId)
            {
                SpawnedMs = NowMs()
            };
            _brains[brainId] = brain;
        }

        if (!TryParsePid(message.OutputPid, out var outputPid))
        {
            return;
        }

        brain.OutputSinkPid = outputPid;
        UpdateOutputSinks(context, brain);
        Log($"Output sink registered for brain {brainId}: {PidLabel(outputPid)}");
    }

    private void HandleSetBrainVisualization(IContext context, ProtoControl.SetBrainVisualization message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        if (!_brains.TryGetValue(brainId, out var brain))
        {
            brain = new BrainState(brainId)
            {
                SpawnedMs = NowMs()
            };
            _brains[brainId] = brain;
        }

        var focusRegionId = message.HasFocusRegion ? (uint?)message.FocusRegionId : null;
        SetBrainVisualization(context, brain, message.Enabled, focusRegionId);
    }

    private void HandleSetBrainCostEnergy(IContext context, ProtoControl.SetBrainCostEnergy message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return;
        }

        if (brain.CostEnabled == message.CostEnabled && brain.EnergyEnabled == message.EnergyEnabled)
        {
            return;
        }

        brain.CostEnabled = message.CostEnabled;
        brain.EnergyEnabled = message.EnergyEnabled;
        UpdateShardRuntimeConfig(context, brain);
        RegisterBrainWithIo(context, brain, force: true);
    }

    private void HandleSetBrainPlasticity(IContext context, ProtoControl.SetBrainPlasticity message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return;
        }

        if (brain.PlasticityEnabled == message.PlasticityEnabled
            && Math.Abs(brain.PlasticityRate - message.PlasticityRate) < 0.000001f
            && brain.PlasticityProbabilisticUpdates == message.ProbabilisticUpdates)
        {
            return;
        }

        brain.PlasticityEnabled = message.PlasticityEnabled;
        brain.PlasticityRate = message.PlasticityRate;
        brain.PlasticityProbabilisticUpdates = message.ProbabilisticUpdates;
        UpdateShardRuntimeConfig(context, brain);
        RegisterBrainWithIo(context, brain, force: true);
    }

    private void HandleRequestPlacement(IContext context, ProtoControl.RequestPlacement message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            context.Respond(new ProtoControl.PlacementAck
            {
                Accepted = false,
                Message = "Invalid brain id.",
                PlacementEpoch = 0,
                LifecycleState = ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
                FailureReason = ProtoControl.PlacementFailureReason.PlacementFailureInvalidBrain,
                AcceptedMs = (ulong)NowMs()
            });
            return;
        }

        if (!_brains.TryGetValue(brainId, out var brain))
        {
            brain = new BrainState(brainId)
            {
                SpawnedMs = NowMs()
            };
            _brains[brainId] = brain;
        }

        var nowMs = NowMs();
        brain.PlacementEpoch = brain.PlacementEpoch >= ulong.MaxValue ? 1UL : brain.PlacementEpoch + 1UL;
        brain.PlacementRequestedMs = nowMs;
        brain.PlacementRequestId = string.IsNullOrWhiteSpace(message.RequestId)
            ? $"{brainId:N}:{brain.PlacementEpoch}"
            : message.RequestId;
        brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileUnknown;

        if (message.BaseDef is not null && message.BaseDef.Sha256 is not null && message.BaseDef.Sha256.Value.Length == 32)
        {
            brain.BaseDefinition = message.BaseDef;
        }

        if (message.LastSnapshot is not null && message.LastSnapshot.Sha256 is not null && message.LastSnapshot.Sha256.Value.Length == 32)
        {
            brain.LastSnapshot = message.LastSnapshot;
        }

        if (message.InputWidth > 0)
        {
            brain.InputWidth = Math.Max(brain.InputWidth, (int)message.InputWidth);
        }

        if (message.OutputWidth > 0)
        {
            brain.OutputWidth = Math.Max(brain.OutputWidth, (int)message.OutputWidth);
        }

        brain.RequestedShardPlan = message.ShardPlan is null ? null : message.ShardPlan.Clone();

        if (!TryBuildPlacementPlan(brain, nowMs, out var plannedPlacement, out var failureReason, out var failureMessage))
        {
            brain.PlannedPlacement = null;
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
                failureReason);

            RegisterBrainWithIo(context, brain, force: true);

            var failedPlanLabel = message.ShardPlan is null ? "none" : message.ShardPlan.Mode.ToString();
            Log(
                $"Placement request rejected for brain {brainId} epoch={brain.PlacementEpoch} request={brain.PlacementRequestId} plan={failedPlanLabel} reason={failureReason}: {failureMessage}");

            context.Respond(new ProtoControl.PlacementAck
            {
                Accepted = false,
                Message = failureMessage,
                PlacementEpoch = brain.PlacementEpoch,
                LifecycleState = brain.PlacementLifecycleState,
                FailureReason = brain.PlacementFailureReason,
                AcceptedMs = (ulong)brain.PlacementUpdatedMs,
                RequestId = brain.PlacementRequestId
            });
            return;
        }

        brain.PlannedPlacement = plannedPlacement;
        UpdatePlacementLifecycle(
            brain,
            ProtoControl.PlacementLifecycleState.PlacementLifecycleRequested,
            ProtoControl.PlacementFailureReason.PlacementFailureNone);

        RegisterBrainWithIo(context, brain, force: true);

        var planLabel = message.ShardPlan is null ? "none" : message.ShardPlan.Mode.ToString();
        Log(
            $"Placement requested for brain {brainId} epoch={brain.PlacementEpoch} request={brain.PlacementRequestId} plan={planLabel} input={message.InputWidth} output={message.OutputWidth} assignments={plannedPlacement.Assignments.Count} workers={plannedPlacement.EligibleWorkers.Count}");

        context.Respond(new ProtoControl.PlacementAck
        {
            Accepted = true,
            Message = "Placement request accepted.",
            PlacementEpoch = brain.PlacementEpoch,
            LifecycleState = brain.PlacementLifecycleState,
            FailureReason = brain.PlacementFailureReason,
            AcceptedMs = (ulong)brain.PlacementUpdatedMs,
            RequestId = brain.PlacementRequestId
        });
    }

    private void HandlePlacementAssignmentAck(IContext context, ProtoControl.PlacementAssignmentAck message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId) || !_brains.TryGetValue(brainId, out var brain))
        {
            return;
        }

        if (brain.PlacementEpoch == 0 || message.PlacementEpoch != brain.PlacementEpoch)
        {
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.assignment_ack.ignored",
                $"Ignored assignment ack for brain {brainId}; epoch={message.PlacementEpoch} current={brain.PlacementEpoch}.");
            return;
        }

        if (!message.Accepted || message.State == ProtoControl.PlacementAssignmentState.PlacementAssignmentFailed)
        {
            var failureReason = message.FailureReason == ProtoControl.PlacementFailureReason.PlacementFailureNone
                ? ProtoControl.PlacementFailureReason.PlacementFailureAssignmentRejected
                : message.FailureReason;
            UpdatePlacementLifecycle(brain, ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed, failureReason);
            brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileFailed;
            return;
        }

        switch (message.State)
        {
            case ProtoControl.PlacementAssignmentState.PlacementAssignmentPending:
            case ProtoControl.PlacementAssignmentState.PlacementAssignmentAccepted:
                UpdatePlacementLifecycle(
                    brain,
                    ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigning,
                    ProtoControl.PlacementFailureReason.PlacementFailureNone);
                break;
            case ProtoControl.PlacementAssignmentState.PlacementAssignmentReady:
                UpdatePlacementLifecycle(
                    brain,
                    ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned,
                    ProtoControl.PlacementFailureReason.PlacementFailureNone);
                break;
            case ProtoControl.PlacementAssignmentState.PlacementAssignmentDraining:
                brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileRequiresAction;
                UpdatePlacementLifecycle(
                    brain,
                    ProtoControl.PlacementLifecycleState.PlacementLifecycleReconciling,
                    ProtoControl.PlacementFailureReason.PlacementFailureNone);
                break;
            default:
                break;
        }
    }

    private void HandlePlacementReconcileReport(IContext context, ProtoControl.PlacementReconcileReport message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId) || !_brains.TryGetValue(brainId, out var brain))
        {
            return;
        }

        if (brain.PlacementEpoch == 0 || message.PlacementEpoch != brain.PlacementEpoch)
        {
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.reconcile.ignored",
                $"Ignored reconcile report for brain {brainId}; epoch={message.PlacementEpoch} current={brain.PlacementEpoch}.");
            return;
        }

        brain.PlacementReconcileState = message.ReconcileState;
        switch (message.ReconcileState)
        {
            case ProtoControl.PlacementReconcileState.PlacementReconcileMatched:
                UpdatePlacementLifecycle(
                    brain,
                    brain.Shards.Count > 0
                        ? ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning
                        : ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned,
                    ProtoControl.PlacementFailureReason.PlacementFailureNone);
                break;
            case ProtoControl.PlacementReconcileState.PlacementReconcileRequiresAction:
                UpdatePlacementLifecycle(
                    brain,
                    ProtoControl.PlacementLifecycleState.PlacementLifecycleReconciling,
                    ProtoControl.PlacementFailureReason.PlacementFailureNone);
                break;
            case ProtoControl.PlacementReconcileState.PlacementReconcileFailed:
                UpdatePlacementLifecycle(
                    brain,
                    ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
                    message.FailureReason == ProtoControl.PlacementFailureReason.PlacementFailureNone
                        ? ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch
                        : message.FailureReason);
                break;
        }
    }

    private ProtoControl.PlacementLifecycleInfo BuildPlacementLifecycleInfo(Guid brainId)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return new ProtoControl.PlacementLifecycleInfo
            {
                BrainId = brainId.ToProtoUuid(),
                LifecycleState = ProtoControl.PlacementLifecycleState.PlacementLifecycleUnknown,
                FailureReason = ProtoControl.PlacementFailureReason.PlacementFailureNone,
                ReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileUnknown
            };
        }

        var info = new ProtoControl.PlacementLifecycleInfo
        {
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = brain.PlacementEpoch,
            LifecycleState = brain.PlacementLifecycleState,
            FailureReason = brain.PlacementFailureReason,
            ReconcileState = brain.PlacementReconcileState,
            RequestedMs = brain.PlacementRequestedMs > 0 ? (ulong)brain.PlacementRequestedMs : 0,
            UpdatedMs = brain.PlacementUpdatedMs > 0 ? (ulong)brain.PlacementUpdatedMs : 0,
            RequestId = brain.PlacementRequestId,
            RegisteredShards = (uint)brain.Shards.Count
        };

        if (brain.RequestedShardPlan is not null)
        {
            info.ShardPlan = brain.RequestedShardPlan.Clone();
        }

        return info;
    }

    private bool TryBuildPlacementPlan(
        BrainState brain,
        long nowMs,
        out PlacementPlanner.PlacementPlanningResult plan,
        out ProtoControl.PlacementFailureReason failureReason,
        out string failureMessage)
    {
        RefreshWorkerCatalogFreshness(nowMs);

        var snapshotMs = _workerCatalogSnapshotMs > 0 ? (ulong)_workerCatalogSnapshotMs : (ulong)nowMs;
        var workers = _workerCatalog.Values
            .Select(static entry => new PlacementPlanner.WorkerCandidate(
                entry.NodeId,
                entry.WorkerAddress,
                entry.WorkerRootActorName,
                entry.IsAlive,
                entry.IsReady,
                entry.IsFresh))
            .ToArray();

        return PlacementPlanner.TryBuildPlan(
            brain.BrainId,
            brain.PlacementEpoch,
            brain.PlacementRequestId,
            brain.PlacementRequestedMs,
            nowMs,
            snapshotMs,
            workers,
            out plan,
            out failureReason,
            out failureMessage);
    }

    private void UpdatePlacementLifecycle(
        BrainState brain,
        ProtoControl.PlacementLifecycleState state,
        ProtoControl.PlacementFailureReason failureReason)
    {
        brain.PlacementLifecycleState = state;
        brain.PlacementFailureReason = failureReason;
        brain.PlacementUpdatedMs = NowMs();
    }

    private void RefreshWorkerInventory(IContext context)
    {
        if (_settingsPid is null)
        {
            return;
        }

        try
        {
            context.Request(_settingsPid, new ProtoSettings.WorkerInventorySnapshotRequest());
        }
        catch (Exception ex)
        {
            LogError($"WorkerInventorySnapshot request failed: {ex.Message}");
        }
        finally
        {
            ScheduleSelf(
                context,
                TimeSpan.FromMilliseconds(_options.WorkerInventoryRefreshMs),
                new RefreshWorkerInventoryTick());
        }
    }

    private void HandleWorkerInventorySnapshotResponse(ProtoSettings.WorkerInventorySnapshotResponse message)
    {
        var snapshotMs = message.SnapshotMs > 0 ? (long)message.SnapshotMs : NowMs();
        _workerCatalogSnapshotMs = snapshotMs;

        foreach (var worker in message.Workers)
        {
            if (!TryGetGuid(worker.NodeId, out var nodeId))
            {
                continue;
            }

            if (!_workerCatalog.TryGetValue(nodeId, out var entry))
            {
                entry = new WorkerCatalogEntry(nodeId);
                _workerCatalog[nodeId] = entry;
            }

            var capabilities = worker.Capabilities ?? new ProtoSettings.NodeCapabilities();
            var hasCapabilities = worker.HasCapabilities;
            var capabilitySnapshotMs = hasCapabilities
                ? (worker.CapabilityTimeMs > 0 ? (long)worker.CapabilityTimeMs : snapshotMs)
                : 0;

            entry.WorkerAddress = worker.Address ?? string.Empty;
            entry.WorkerRootActorName = worker.RootActorName ?? string.Empty;
            entry.IsAlive = worker.IsAlive;
            entry.IsReady = worker.IsReady;
            entry.LastSeenMs = worker.LastSeenMs > 0 ? (long)worker.LastSeenMs : 0;
            entry.CpuCores = hasCapabilities ? capabilities.CpuCores : 0;
            entry.RamFreeBytes = hasCapabilities ? (long)capabilities.RamFreeBytes : 0;
            entry.HasGpu = hasCapabilities && capabilities.HasGpu;
            entry.VramFreeBytes = hasCapabilities ? (long)capabilities.VramFreeBytes : 0;
            entry.CpuScore = hasCapabilities ? capabilities.CpuScore : 0f;
            entry.GpuScore = hasCapabilities ? capabilities.GpuScore : 0f;
            entry.CapabilitySnapshotMs = capabilitySnapshotMs;
            entry.LastUpdatedMs = snapshotMs;
        }

        RefreshWorkerCatalogFreshness(snapshotMs);
    }

    private ProtoControl.PlacementWorkerInventory BuildPlacementWorkerInventory()
    {
        var nowMs = NowMs();
        RefreshWorkerCatalogFreshness(nowMs);

        var snapshotMs = _workerCatalogSnapshotMs > 0 ? _workerCatalogSnapshotMs : nowMs;
        var inventory = new ProtoControl.PlacementWorkerInventory
        {
            SnapshotMs = (ulong)snapshotMs
        };

        foreach (var entry in _workerCatalog.Values
                     .Where(static worker => worker.IsAlive && worker.IsReady && worker.IsFresh)
                     .OrderBy(static worker => worker.WorkerAddress, StringComparer.Ordinal)
                     .ThenBy(static worker => worker.NodeId))
        {
            inventory.Workers.Add(new ProtoControl.PlacementWorkerInventoryEntry
            {
                WorkerNodeId = entry.NodeId.ToProtoUuid(),
                WorkerAddress = entry.WorkerAddress,
                WorkerRootActorName = entry.WorkerRootActorName,
                IsAlive = entry.IsAlive,
                LastSeenMs = ToProtoMs(entry.LastSeenMs),
                CpuCores = entry.CpuCores,
                RamFreeBytes = ToProtoBytes(entry.RamFreeBytes),
                HasGpu = entry.HasGpu,
                VramFreeBytes = ToProtoBytes(entry.VramFreeBytes),
                CpuScore = entry.CpuScore,
                GpuScore = entry.GpuScore,
                CapabilityEpoch = ToProtoMs(entry.CapabilitySnapshotMs)
            });
        }

        return inventory;
    }

    private void RefreshWorkerCatalogFreshness(long nowMs)
    {
        foreach (var worker in _workerCatalog.Values)
        {
            worker.IsFresh = IsWorkerFresh(worker, nowMs);
        }
    }

    private bool IsWorkerFresh(WorkerCatalogEntry worker, long nowMs)
    {
        var staleAfterMs = Math.Max(1, _options.WorkerInventoryStaleAfterMs);
        return IsFreshTimestamp(worker.LastSeenMs, nowMs, staleAfterMs)
               && IsFreshTimestamp(worker.CapabilitySnapshotMs, nowMs, staleAfterMs);
    }

    private static bool IsFreshTimestamp(long timestampMs, long nowMs, int staleAfterMs)
    {
        if (timestampMs <= 0)
        {
            return false;
        }

        return nowMs - timestampMs <= staleAfterMs;
    }

    private void PauseBrain(IContext context, Guid brainId, string? reason)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return;
        }

        brain.Paused = true;
        brain.PausedReason = reason;

        if (_phase == TickPhase.Compute)
        {
            RemovePendingComputeForBrain(brainId);
            MaybeCompleteCompute(context);
        }

        if (_phase == TickPhase.Deliver && _pendingDeliver.Remove(brainId))
        {
            MaybeCompleteDeliver(context);
        }

        ReportBrainState(context, brainId, "Paused", reason);
        EmitVizEvent(context, VizEventType.VizBrainPaused, brainId: brainId);
        EmitDebug(context, ProtoSeverity.SevInfo, "brain.paused", $"Brain {brainId} paused. reason={reason ?? "none"}");
    }

    private void ResumeBrain(IContext context, Guid brainId)
    {
        if (_brains.TryGetValue(brainId, out var brain))
        {
            brain.Paused = false;
            brain.PausedReason = null;
            ReportBrainState(context, brainId, "Active", null);
            EmitVizEvent(context, VizEventType.VizBrainActive, brainId: brainId);
            EmitDebug(context, ProtoSeverity.SevInfo, "brain.resumed", $"Brain {brainId} resumed.");
        }
    }

    private void StartTick(IContext context)
    {
        _tick = new TickState(_lastCompletedTickId + 1, DateTime.UtcNow);
        EmitVizEvent(context, VizEventType.VizTick, tickId: _tick.TickId);
        _phase = TickPhase.Compute;
        _pendingCompute.Clear();
        _pendingDeliver.Clear();

        _tick.ComputeStartedUtc = _tick.StartedUtc;

        foreach (var brain in _brains.Values)
        {
            if (brain.Paused || brain.Shards.Count == 0)
            {
                continue;
            }

            if (brain.RoutingSnapshot.Count == 0)
            {
                LogError($"Routing snapshot missing for brain {brain.BrainId} with {brain.Shards.Count} shard(s).");
            }

            var computeTarget = brain.BrainRootPid ?? brain.SignalRouterPid;
            if (computeTarget is null)
            {
                LogError($"TickCompute skipped: missing BrainRoot/SignalRouter PID for brain {brain.BrainId}.");
                continue;
            }

            foreach (var shardId in brain.Shards.Keys)
            {
                _pendingCompute.Add(new ShardKey(brain.BrainId, shardId));
            }

            context.Send(
                computeTarget,
                new ProtoControl.TickCompute
                {
                    TickId = _tick.TickId,
                    TargetTickHz = _backpressure.TargetTickHz
                });
        }

        _tick.ExpectedComputeCount = _pendingCompute.Count;

        if (_pendingCompute.Count == 0)
        {
            CompleteComputePhase(context);
            return;
        }

        SchedulePhaseTimeout(context, TickPhase.Compute, _tick.TickId, _options.ComputeTimeoutMs);
    }

    private void HandleTickComputeDone(IContext context, ProtoControl.TickComputeDone message)
    {
        if (_tick is null)
        {
            if (message.TickId <= _lastCompletedTickId)
            {
                HiveMindTelemetry.RecordLateComputeAfterCompletion();
            }
            return;
        }

        if (message.TickId != _tick.TickId || _phase != TickPhase.Compute)
        {
            if (message.TickId <= _tick.TickId)
            {
                _tick.LateComputeCount++;
            }
            return;
        }

        if (!message.BrainId.TryToGuid(out var brainId) || message.ShardId is null)
        {
            return;
        }

        var shardId = message.ShardId.ToShardId32();
        var key = new ShardKey(brainId, shardId);

        if (!_pendingCompute.Remove(key))
        {
            _tick.LateComputeCount++;
            return;
        }

        if (message.TickCostTotal != 0)
        {
            var updated = message.TickCostTotal;
            if (_tick.BrainTickCosts.TryGetValue(brainId, out var existing))
            {
                updated += existing;
            }

            _tick.BrainTickCosts[brainId] = updated;
        }

        _tick.CompletedComputeCount++;
        MaybeCompleteCompute(context);
    }

    private void HandleTickDeliverDone(IContext context, ProtoControl.TickDeliverDone message)
    {
        if (_tick is null)
        {
            if (message.TickId <= _lastCompletedTickId)
            {
                HiveMindTelemetry.RecordLateDeliverAfterCompletion();
            }
            return;
        }

        if (message.TickId != _tick.TickId || _phase != TickPhase.Deliver)
        {
            if (message.TickId <= _tick.TickId)
            {
                _tick.LateDeliverCount++;
            }
            return;
        }

        if (!message.BrainId.TryToGuid(out var brainId))
        {
            return;
        }

        if (!_pendingDeliver.Remove(brainId))
        {
            _tick.LateDeliverCount++;
            return;
        }

        _tick.CompletedDeliverCount++;
        ReportBrainTick(context, brainId, message.TickId);
        MaybeCompleteDeliver(context);
    }

    private void HandleTickPhaseTimeout(IContext context, TickPhaseTimeout message)
    {
        if (_tick is null || message.TickId != _tick.TickId || _phase != message.Phase)
        {
            return;
        }

        switch (message.Phase)
        {
            case TickPhase.Compute:
                _tick.ComputeTimedOut = true;
                if (_pendingCompute.Count > 0)
                {
                    LogError($"TickCompute timeout: tick {_tick.TickId} pending={_pendingCompute.Count}");
                    EmitDebug(
                        context,
                        ProtoSeverity.SevError,
                        "tick.compute.timeout",
                        $"Tick {_tick.TickId} compute timeout pending={_pendingCompute.Count}");
                }
                _pendingCompute.Clear();
                CompleteComputePhase(context);
                break;
            case TickPhase.Deliver:
                _tick.DeliverTimedOut = true;
                if (_pendingDeliver.Count > 0)
                {
                    var pendingBrains = string.Join(",", _pendingDeliver);
                    LogError($"TickDeliver timeout: tick {_tick.TickId} pendingBrains={pendingBrains}");
                    EmitDebug(
                        context,
                        ProtoSeverity.SevError,
                        "tick.deliver.timeout",
                        $"Tick {_tick.TickId} deliver timeout pendingBrains={pendingBrains}");
                }
                _pendingDeliver.Clear();
                CompleteTick(context);
                break;
        }
    }

    private void MaybeCompleteCompute(IContext context)
    {
        if (_pendingCompute.Count == 0)
        {
            CompleteComputePhase(context);
        }
    }

    private void CompleteComputePhase(IContext context)
    {
        if (_tick is null || _phase != TickPhase.Compute)
        {
            return;
        }

        _tick.ComputeCompletedUtc = DateTime.UtcNow;
        _phase = TickPhase.Deliver;
        _tick.DeliverStartedUtc = DateTime.UtcNow;

        foreach (var brain in _brains.Values)
        {
            if (brain.Paused || brain.Shards.Count == 0)
            {
                continue;
            }

            if (brain.RoutingSnapshot.Count == 0)
            {
                LogError($"Routing snapshot missing for brain {brain.BrainId} with {brain.Shards.Count} shard(s).");
            }

            var deliverTarget = brain.BrainRootPid ?? brain.SignalRouterPid;
            if (deliverTarget is null)
            {
                LogError($"TickDeliver skipped: missing BrainRoot/SignalRouter PID for brain {brain.BrainId}.");
                continue;
            }
            _pendingDeliver.Add(brain.BrainId);
            context.Send(deliverTarget, new ProtoControl.TickDeliver { TickId = _tick.TickId });
        }

        _tick.ExpectedDeliverCount = _pendingDeliver.Count;

        if (_pendingDeliver.Count == 0)
        {
            CompleteTick(context);
            return;
        }

        SchedulePhaseTimeout(context, TickPhase.Deliver, _tick.TickId, _options.DeliverTimeoutMs);
    }

    private void MaybeCompleteDeliver(IContext context)
    {
        if (_pendingDeliver.Count == 0)
        {
            CompleteTick(context);
        }
    }

    private void CompleteTick(IContext context)
    {
        if (_tick is null)
        {
            _phase = TickPhase.Idle;
            return;
        }

        _tick.DeliverCompletedUtc = DateTime.UtcNow;
        _phase = TickPhase.Idle;

        var outcome = new TickOutcome(
            _tick.TickId,
            SafeDuration(_tick.ComputeStartedUtc, _tick.ComputeCompletedUtc),
            SafeDuration(_tick.DeliverStartedUtc, _tick.DeliverCompletedUtc),
            _tick.ComputeTimedOut,
            _tick.DeliverTimedOut,
            _tick.LateComputeCount,
            _tick.LateDeliverCount,
            _tick.ExpectedComputeCount,
            _tick.CompletedComputeCount,
            _tick.ExpectedDeliverCount,
            _tick.CompletedDeliverCount);

        var elapsed = DateTime.UtcNow - _tick.StartedUtc;
        var completedTickId = _tick.TickId;
        var tickCosts = _tick.BrainTickCosts;
        _tick = null;
        _lastCompletedTickId = completedTickId;

        HiveMindTelemetry.RecordTickOutcome(outcome, _backpressure.TargetTickHz);
        ApplyTickCosts(context, completedTickId, tickCosts);

        var decision = _backpressure.Evaluate(outcome);

        if (decision.RequestReschedule)
        {
            RequestReschedule(context, decision.Reason);
            HiveMindTelemetry.RecordReschedule(decision.Reason);
        }

        if (decision.RequestPause)
        {
            PauseAllBrains(context, decision.Reason);
            HiveMindTelemetry.RecordPause(decision.Reason);
        }

        ScheduleNextTick(context, ComputeTickDelay(elapsed, decision.TargetTickHz));
    }

    private void ApplyTickCosts(IContext context, ulong tickId, Dictionary<Guid, long> costs)
    {
        if (_ioPid is null || costs.Count == 0)
        {
            return;
        }

        foreach (var entry in costs)
        {
            var cost = entry.Value;
            if (cost == 0)
            {
                continue;
            }

            if (_brains.TryGetValue(entry.Key, out var brain))
            {
                brain.LastTickCost = cost;
            }

            HiveMindTelemetry.RecordBrainTickCost(entry.Key, cost);
            context.Send(_ioPid, new ApplyTickCost(entry.Key, tickId, cost));
        }
    }

    private void ScheduleNextTick(IContext context, TimeSpan delay)
    {
        if (!_tickLoopEnabled || _rescheduleInProgress)
        {
            return;
        }

        ScheduleSelf(context, delay, new TickStart());
    }

    private void SchedulePhaseTimeout(IContext context, TickPhase phase, ulong tickId, int timeoutMs)
    {
        if (timeoutMs <= 0)
        {
            return;
        }

        ScheduleSelf(context, TimeSpan.FromMilliseconds(timeoutMs), new TickPhaseTimeout(tickId, phase));
    }

    private void RemovePendingComputeForBrain(Guid brainId)
    {
        if (_pendingCompute.Count == 0)
        {
            return;
        }

        var removeKeys = new List<ShardKey>();
        foreach (var key in _pendingCompute)
        {
            if (key.BrainId == brainId)
            {
                removeKeys.Add(key);
            }
        }

        if (_tick is null)
        {
            foreach (var key in removeKeys)
            {
                _pendingCompute.Remove(key);
            }

            return;
        }

        foreach (var key in removeKeys)
        {
            if (_pendingCompute.Remove(key))
            {
                _tick.ExpectedComputeCount = Math.Max(_tick.CompletedComputeCount, _tick.ExpectedComputeCount - 1);
            }
        }
    }

    private void PauseAllBrains(IContext context, string reason)
    {
        foreach (var brain in _brains.Values)
        {
            brain.Paused = true;
            brain.PausedReason = reason;
        }

        if (_phase == TickPhase.Compute)
        {
            _pendingCompute.Clear();
            MaybeCompleteCompute(context);
        }

        if (_phase == TickPhase.Deliver)
        {
            _pendingDeliver.Clear();
            MaybeCompleteDeliver(context);
        }

        Log($"Paused all brains: {reason}");

        foreach (var brain in _brains.Values)
        {
            ReportBrainState(context, brain.BrainId, "Paused", reason);
        }
    }

    private void RequestReschedule(IContext context, string reason)
    {
        if (_rescheduleInProgress)
        {
            _rescheduleQueued = true;
            _queuedRescheduleReason ??= reason;
            return;
        }

        var now = DateTime.UtcNow;
        if (_lastRescheduleTick > 0 && (_lastCompletedTickId - _lastRescheduleTick) < (ulong)_options.RescheduleMinTicks)
        {
            _rescheduleQueued = true;
            _queuedRescheduleReason ??= reason;
            return;
        }

        if (_lastRescheduleAt != default && now - _lastRescheduleAt < TimeSpan.FromMinutes(_options.RescheduleMinMinutes))
        {
            _rescheduleQueued = true;
            _queuedRescheduleReason ??= reason;
            return;
        }

        _rescheduleInProgress = true;
        _lastRescheduleAt = now;
        _lastRescheduleTick = _lastCompletedTickId;

        ScheduleSelf(context, TimeSpan.FromMilliseconds(_options.RescheduleQuietMs), new RescheduleNow(reason));
    }

    private void BeginReschedule(IContext context, RescheduleNow message)
    {
        Log($"Reschedule started: {message.Reason}");
        ScheduleSelf(
            context,
            TimeSpan.FromMilliseconds(_options.RescheduleSimulatedMs),
            new RescheduleCompleted(message.Reason, true));
    }

    private void CompleteReschedule(IContext context, RescheduleCompleted message)
    {
        _rescheduleInProgress = false;
        Log($"Reschedule completed: {message.Reason} (success={message.Success})");

        if (_rescheduleQueued)
        {
            _rescheduleQueued = false;
            var queuedReason = _queuedRescheduleReason ?? "queued";
            _queuedRescheduleReason = null;
            RequestReschedule(context, queuedReason);
            return;
        }

        if (_tickLoopEnabled && _phase == TickPhase.Idle)
        {
            ScheduleNextTick(context, TimeSpan.Zero);
        }
    }

    private static TimeSpan SafeDuration(DateTime start, DateTime end)
    {
        if (start == default || end == default || end < start)
        {
            return TimeSpan.Zero;
        }

        return end - start;
    }

    private static TimeSpan ComputeTickDelay(TimeSpan elapsed, float targetTickHz)
    {
        if (targetTickHz <= 0)
        {
            return TimeSpan.Zero;
        }

        var period = TimeSpan.FromSeconds(1d / targetTickHz);
        var delay = period - elapsed;
        return delay <= TimeSpan.Zero ? TimeSpan.Zero : delay;
    }

    private static void Log(string message)
        => Console.WriteLine($"[{DateTime.UtcNow:O}] [HiveMind] {message}");

    private static void LogError(string message)
        => Console.WriteLine($"[{DateTime.UtcNow:O}] [HiveMind][ERROR] {message}");

    private static void SendRoutingTable(IContext context, PID pid, RoutingTableSnapshot snapshot, string label)
    {
        if (!string.IsNullOrWhiteSpace(pid.Address) && string.IsNullOrWhiteSpace(context.System.Address))
        {
            LogError($"Routing table not sent to {label} {PidLabel(pid)} because remoting is not configured.");
            return;
        }

        try
        {
            context.Send(pid, new SetRoutingTable(snapshot));
        }
        catch (Exception ex)
        {
            LogError($"Failed to send routing table to {label} {PidLabel(pid)}: {ex.Message}");
        }
    }

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private static PID? NormalizePid(IContext context, PID? pid)
    {
        if (pid is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(pid.Address))
        {
            return pid;
        }

        var senderAddress = context.Sender?.Address;
        if (!string.IsNullOrWhiteSpace(senderAddress))
        {
            return new PID(senderAddress, pid.Id);
        }

        return pid;
    }

    private static bool TryGetGuid(Nbn.Proto.Uuid? uuid, out Guid guid)
    {
        if (uuid is null)
        {
            guid = Guid.Empty;
            return false;
        }

        return uuid.TryToGuid(out guid);
    }

    private static PID? ParsePid(string? value)
        => TryParsePid(value, out var pid) ? pid : null;

    private static bool TryParsePid(string? value, out PID pid)
    {
        pid = new PID();
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        var slashIndex = trimmed.IndexOf('/');
        if (slashIndex <= 0)
        {
            pid.Id = trimmed;
            return true;
        }

        var address = trimmed[..slashIndex];
        var id = trimmed[(slashIndex + 1)..];

        if (string.IsNullOrWhiteSpace(id))
        {
            return false;
        }

        pid.Address = address;
        pid.Id = id;
        return true;
    }

    private ProtoControl.HiveMindStatus BuildStatus()
        => new()
        {
            LastCompletedTickId = _lastCompletedTickId,
            TickLoopEnabled = _tickLoopEnabled,
            TargetTickHz = _backpressure.TargetTickHz,
            PendingCompute = (uint)_pendingCompute.Count,
            PendingDeliver = (uint)_pendingDeliver.Count,
            RescheduleInProgress = _rescheduleInProgress,
            RegisteredBrains = (uint)_brains.Count,
            RegisteredShards = (uint)_brains.Values.Sum(brain => brain.Shards.Count),
            HasTickRateOverride = _backpressure.HasTickRateOverride,
            TickRateOverrideHz = _backpressure.TickRateOverrideHz
        };

    private void HandleSetTickRateOverride(IContext context, ProtoControl.SetTickRateOverride message)
    {
        float? requestedOverride = message.ClearOverride ? null : message.TargetTickHz;
        var accepted = _backpressure.TrySetTickRateOverride(requestedOverride, out var summary);
        if (accepted)
        {
            EmitDebug(context, ProtoSeverity.SevInfo, "tick.override", summary);
        }
        else
        {
            EmitDebug(context, ProtoSeverity.SevWarn, "tick.override.invalid", summary);
        }

        context.Respond(new ProtoControl.SetTickRateOverrideAck
        {
            Accepted = accepted,
            Message = summary,
            TargetTickHz = _backpressure.TargetTickHz,
            HasOverride = _backpressure.HasTickRateOverride,
            OverrideTickHz = _backpressure.TickRateOverrideHz
        });
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
                BrainId = brainId.ToProtoUuid()
            };
        }

        return new ProtoControl.BrainIoInfo
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            InputWidth = (uint)Math.Max(0, brain.InputWidth),
            OutputWidth = (uint)Math.Max(0, brain.OutputWidth)
        };
    }

    private void HandleExportBrainDefinition(IContext context, ProtoIo.ExportBrainDefinition message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            context.Respond(new ProtoIo.BrainDefinitionReady());
            return;
        }

        if (!_brains.TryGetValue(brainId, out var brain) || brain.BaseDefinition is null)
        {
            context.Respond(new ProtoIo.BrainDefinitionReady
            {
                BrainId = brainId.ToProtoUuid()
            });
            return;
        }

        if (!message.RebaseOverlays || !HasArtifactRef(brain.BaseDefinition) || brain.Shards.Count == 0)
        {
            context.Respond(new ProtoIo.BrainDefinitionReady
            {
                BrainId = brainId.ToProtoUuid(),
                BrainDef = brain.BaseDefinition
            });
            return;
        }

        var fallbackDefinition = brain.BaseDefinition!;
        var storeRootPath = ResolveArtifactRoot(fallbackDefinition.StoreUri);
        var request = new RebasedDefinitionBuildRequest(
            brain.BrainId,
            fallbackDefinition,
            _lastCompletedTickId,
            new Dictionary<ShardId32, PID>(brain.Shards),
            storeRootPath,
            string.IsNullOrWhiteSpace(fallbackDefinition.StoreUri) ? storeRootPath : fallbackDefinition.StoreUri);

        var rebaseTask = BuildAndStoreRebasedDefinitionAsync(context.System, request);
        context.ReenterAfter(rebaseTask, task =>
        {
            if (task is { IsCompletedSuccessfully: true } && HasArtifactRef(task.Result))
            {
                context.Respond(new ProtoIo.BrainDefinitionReady
                {
                    BrainId = brainId.ToProtoUuid(),
                    BrainDef = task.Result
                });
                return Task.CompletedTask;
            }

            if (task.Exception is not null)
            {
                LogError($"Rebased export failed for brain {brainId}: {task.Exception.GetBaseException().Message}");
            }

            context.Respond(new ProtoIo.BrainDefinitionReady
            {
                BrainId = brainId.ToProtoUuid(),
                BrainDef = fallbackDefinition
            });
            return Task.CompletedTask;
        });
    }

    private void HandleRequestSnapshot(IContext context, ProtoIo.RequestSnapshot message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            context.Respond(new ProtoIo.SnapshotReady());
            return;
        }

        if (!_brains.TryGetValue(brainId, out var brain))
        {
            context.Respond(new ProtoIo.SnapshotReady
            {
                BrainId = brainId.ToProtoUuid()
            });
            return;
        }

        if (!HasArtifactRef(brain.BaseDefinition) || brain.Shards.Count == 0)
        {
            RespondSnapshot(context, brainId, brain.LastSnapshot);
            return;
        }

        var storeRootPath = ResolveArtifactRoot(brain.BaseDefinition!.StoreUri);
        var request = new SnapshotBuildRequest(
            brain.BrainId,
            brain.BaseDefinition!,
            _lastCompletedTickId,
            message.HasRuntimeState ? message.EnergyRemaining : 0L,
            message.HasRuntimeState ? message.CostEnabled : brain.CostEnabled,
            message.HasRuntimeState ? message.EnergyEnabled : brain.EnergyEnabled,
            message.HasRuntimeState ? message.PlasticityEnabled : brain.PlasticityEnabled,
            new Dictionary<ShardId32, PID>(brain.Shards),
            storeRootPath,
            string.IsNullOrWhiteSpace(brain.BaseDefinition.StoreUri) ? storeRootPath : brain.BaseDefinition.StoreUri);

        var snapshotTask = BuildAndStoreSnapshotAsync(context.System, request);
        context.ReenterAfter(snapshotTask, task =>
        {
            if (task is { IsCompletedSuccessfully: true } && task.Result is not null)
            {
                var snapshot = task.Result;
                if (_brains.TryGetValue(brainId, out var liveBrain))
                {
                    liveBrain.LastSnapshot = snapshot;
                    if (_ioPid is not null)
                    {
                        context.Send(_ioPid, new UpdateBrainSnapshot(brainId, snapshot));
                    }

                    RegisterBrainWithIo(context, liveBrain, force: true);
                }

                context.Respond(new ProtoIo.SnapshotReady
                {
                    BrainId = brainId.ToProtoUuid(),
                    Snapshot = snapshot
                });
                return Task.CompletedTask;
            }

            if (task.Exception is not null)
            {
                LogError($"Live snapshot generation failed for brain {brainId}: {task.Exception.GetBaseException().Message}");
            }

            if (_brains.TryGetValue(brainId, out var liveFallback))
            {
                RespondSnapshot(context, brainId, liveFallback.LastSnapshot);
            }
            else
            {
                context.Respond(new ProtoIo.SnapshotReady
                {
                    BrainId = brainId.ToProtoUuid()
                });
            }

            return Task.CompletedTask;
        });
    }

    private static async Task<List<ProtoControl.CaptureShardSnapshotAck>> CaptureShardSnapshotsAsync(
        ActorSystem system,
        Guid brainId,
        ulong tickId,
        IReadOnlyDictionary<ShardId32, PID> shards)
    {
        var captures = new List<ProtoControl.CaptureShardSnapshotAck>(shards.Count);
        foreach (var entry in shards.OrderBy(static pair => pair.Key.RegionId).ThenBy(static pair => pair.Key.ShardIndex))
        {
            var capture = await system.Root.RequestAsync<ProtoControl.CaptureShardSnapshotAck>(
                    entry.Value,
                    new ProtoControl.CaptureShardSnapshot
                    {
                        BrainId = brainId.ToProtoUuid(),
                        RegionId = (uint)entry.Key.RegionId,
                        ShardIndex = (uint)entry.Key.ShardIndex,
                        TickId = tickId
                    },
                    SnapshotShardRequestTimeout)
                .ConfigureAwait(false);

            if (capture is null)
            {
                throw new InvalidOperationException($"Snapshot capture returned null for shard {entry.Key}.");
            }

            if (!capture.Success)
            {
                var error = string.IsNullOrWhiteSpace(capture.Error) ? "unknown" : capture.Error;
                throw new InvalidOperationException($"Snapshot capture failed for shard {entry.Key}: {error}");
            }

            captures.Add(capture);
        }

        return captures;
    }

    private static async Task<Nbn.Proto.ArtifactRef?> BuildAndStoreRebasedDefinitionAsync(ActorSystem system, RebasedDefinitionBuildRequest request)
    {
        var store = new LocalArtifactStore(new ArtifactStoreOptions(request.StoreRootPath));
        if (!request.BaseDefinition.TryToSha256Bytes(out var baseHashBytes))
        {
            throw new InvalidOperationException("Base definition sha256 is required to build rebased exports.");
        }

        var baseHash = new Sha256Hash(baseHashBytes);
        var nbnStream = await store.TryOpenArtifactAsync(baseHash).ConfigureAwait(false);
        if (nbnStream is null)
        {
            throw new InvalidOperationException($"Base NBN artifact {baseHash.ToHex()} not found in store.");
        }

        byte[] nbnBytes;
        await using (nbnStream)
        using (var ms = new MemoryStream())
        {
            await nbnStream.CopyToAsync(ms).ConfigureAwait(false);
            nbnBytes = ms.ToArray();
        }

        var baseHeader = NbnBinary.ReadNbnHeader(nbnBytes);
        var captures = await CaptureShardSnapshotsAsync(system, request.BrainId, request.SnapshotTickId, request.Shards).ConfigureAwait(false);
        var (_, overlays) = BuildSnapshotSections(baseHeader, captures);
        HiveMindTelemetry.RecordRebaseOverlayRecords(request.BrainId, overlays.Count);
        if (overlays.Count == 0)
        {
            return request.BaseDefinition;
        }

        var rebasedSections = RebaseDefinitionWithOverlays(baseHeader, nbnBytes, overlays);
        var rebasedHeader = CloneHeader(baseHeader);
        var validation = NbnBinaryValidator.ValidateNbn(rebasedHeader, rebasedSections);
        if (!validation.IsValid)
        {
            var errors = string.Join("; ", validation.Issues.Select(static issue => issue.ToString()));
            throw new InvalidOperationException($"Generated rebased definition failed validation: {errors}");
        }

        var bytes = NbnBinary.WriteNbn(rebasedHeader, rebasedSections);
        await using var rebasedStream = new MemoryStream(bytes, writable: false);
        var manifest = await store.StoreAsync(rebasedStream, "application/x-nbn").ConfigureAwait(false);
        return manifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifest.ByteLength, "application/x-nbn", request.StoreUri);
    }

    private static List<NbnRegionSection> RebaseDefinitionWithOverlays(
        NbnHeaderV2 baseHeader,
        ReadOnlySpan<byte> nbnBytes,
        IReadOnlyList<NbsOverlayRecord> overlays)
    {
        var sectionMap = new Dictionary<int, NbnRegionSection>();
        for (var regionId = 0; regionId < baseHeader.Regions.Length; regionId++)
        {
            var entry = baseHeader.Regions[regionId];
            if (entry.NeuronSpan == 0)
            {
                continue;
            }

            var source = NbnBinary.ReadNbnRegionSection(nbnBytes, entry.Offset);
            sectionMap[regionId] = CloneRegionSection(source);
        }

        var routeMap = BuildAxonRouteIndex(sectionMap);
        foreach (var overlay in overlays)
        {
            var routeKey = (overlay.FromAddress, overlay.ToAddress);
            if (!routeMap.TryGetValue(routeKey, out var location))
            {
                throw new InvalidOperationException($"Overlay route {overlay.FromAddress}->{overlay.ToAddress} does not exist in the base definition.");
            }

            var section = sectionMap[location.RegionId];
            var axon = section.AxonRecords[location.AxonIndex];
            var strengthCode = (byte)Math.Clamp((int)overlay.StrengthCode, 0, 31);
            if (axon.StrengthCode == strengthCode)
            {
                continue;
            }

            section.AxonRecords[location.AxonIndex] = new Nbn.Shared.Packing.AxonRecord(strengthCode, axon.TargetNeuronId, axon.TargetRegionId);
        }

        return sectionMap
            .OrderBy(static pair => pair.Key)
            .Select(static pair => pair.Value)
            .ToList();
    }

    private static Dictionary<(uint From, uint To), RebasedAxonLocation> BuildAxonRouteIndex(IReadOnlyDictionary<int, NbnRegionSection> sections)
    {
        var routeMap = new Dictionary<(uint From, uint To), RebasedAxonLocation>();
        foreach (var pair in sections.OrderBy(static item => item.Key))
        {
            var section = pair.Value;
            var axonCursor = 0;
            for (var neuronId = 0; neuronId < section.NeuronRecords.Length; neuronId++)
            {
                var fromAddress = Nbn.Shared.Addressing.Address32.From(section.RegionId, neuronId).Value;
                var axonCount = section.NeuronRecords[neuronId].AxonCount;
                for (var axonOffset = 0; axonOffset < axonCount; axonOffset++)
                {
                    var axonIndex = axonCursor + axonOffset;
                    if ((uint)axonIndex >= (uint)section.AxonRecords.Length)
                    {
                        throw new InvalidOperationException($"Axon index {axonIndex} out of range for region {section.RegionId}.");
                    }

                    var axon = section.AxonRecords[axonIndex];
                    var toAddress = Nbn.Shared.Addressing.Address32.From(axon.TargetRegionId, axon.TargetNeuronId).Value;
                    if (!routeMap.TryAdd((fromAddress, toAddress), new RebasedAxonLocation(pair.Key, axonIndex)))
                    {
                        throw new InvalidOperationException($"Duplicate axon route {fromAddress}->{toAddress} in base definition.");
                    }
                }

                axonCursor += axonCount;
            }

            if (axonCursor != section.AxonRecords.Length)
            {
                throw new InvalidOperationException($"Region {section.RegionId} axon traversal mismatch.");
            }
        }

        return routeMap;
    }

    private static NbnRegionSection CloneRegionSection(NbnRegionSection section)
    {
        var checkpoints = (ulong[])section.Checkpoints.Clone();
        var neurons = (Nbn.Shared.Packing.NeuronRecord[])section.NeuronRecords.Clone();
        var axons = (Nbn.Shared.Packing.AxonRecord[])section.AxonRecords.Clone();
        return new NbnRegionSection(
            section.RegionId,
            section.NeuronSpan,
            section.TotalAxons,
            section.Stride,
            section.CheckpointCount,
            checkpoints,
            neurons,
            axons);
    }

    private static NbnHeaderV2 CloneHeader(NbnHeaderV2 header)
    {
        var regions = new NbnRegionDirectoryEntry[header.Regions.Length];
        Array.Copy(header.Regions, regions, header.Regions.Length);
        return new NbnHeaderV2(
            header.Magic,
            header.Version,
            header.Endianness,
            header.HeaderBytesPow2,
            header.BrainSeed,
            header.AxonStride,
            header.Flags,
            header.Quantization,
            regions);
    }

    private readonly record struct RebasedAxonLocation(int RegionId, int AxonIndex);

    private static async Task<Nbn.Proto.ArtifactRef?> BuildAndStoreSnapshotAsync(ActorSystem system, SnapshotBuildRequest request)
    {
        var store = new LocalArtifactStore(new ArtifactStoreOptions(request.StoreRootPath));
        if (!request.BaseDefinition.TryToSha256Bytes(out var baseHashBytes))
        {
            throw new InvalidOperationException("Base definition sha256 is required to build snapshots.");
        }

        var baseHash = new Sha256Hash(baseHashBytes);
        var nbnStream = await store.TryOpenArtifactAsync(baseHash).ConfigureAwait(false);
        if (nbnStream is null)
        {
            throw new InvalidOperationException($"Base NBN artifact {baseHash.ToHex()} not found in store.");
        }

        byte[] nbnBytes;
        await using (nbnStream)
        using (var ms = new MemoryStream())
        {
            await nbnStream.CopyToAsync(ms).ConfigureAwait(false);
            nbnBytes = ms.ToArray();
        }

        var baseHeader = NbnBinary.ReadNbnHeader(nbnBytes);
        var captures = await CaptureShardSnapshotsAsync(system, request.BrainId, request.SnapshotTickId, request.Shards).ConfigureAwait(false);

        var (regions, overlays) = BuildSnapshotSections(baseHeader, captures);
        HiveMindTelemetry.RecordSnapshotOverlayRecords(request.BrainId, overlays.Count);
        var flags = 0x1u;
        if (overlays.Count > 0)
        {
            flags |= 0x2u;
        }

        if (request.CostEnabled)
        {
            flags |= 0x4u;
        }

        if (request.EnergyEnabled)
        {
            flags |= 0x8u;
        }

        if (request.PlasticityEnabled)
        {
            flags |= 0x10u;
        }

        var header = new NbsHeaderV2(
            magic: "NBS2",
            version: 2,
            endianness: 1,
            headerBytesPow2: 9,
            brainId: request.BrainId,
            snapshotTickId: request.SnapshotTickId,
            timestampMs: (ulong)NowMs(),
            energyRemaining: request.EnergyRemaining,
            baseNbnSha256: baseHashBytes,
            flags: flags,
            bufferMap: SnapshotBufferQuantization);

        NbsOverlaySection? overlaySection = null;
        if (overlays.Count > 0)
        {
            overlaySection = new NbsOverlaySection(overlays.ToArray(), NbnBinary.GetNbsOverlaySectionSize(overlays.Count));
        }

        var validation = NbnBinaryValidator.ValidateNbs(header, regions, overlaySection);
        if (!validation.IsValid)
        {
            var errors = string.Join("; ", validation.Issues.Select(static issue => issue.ToString()));
            throw new InvalidOperationException($"Generated snapshot failed validation: {errors}");
        }

        var bytes = NbnBinary.WriteNbs(header, regions, overlays);
        await using var snapshotStream = new MemoryStream(bytes, writable: false);
        var manifest = await store.StoreAsync(snapshotStream, "application/x-nbs").ConfigureAwait(false);

        return manifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifest.ByteLength, "application/x-nbs", request.StoreUri);
    }

    private static (List<NbsRegionSection> Regions, List<NbsOverlayRecord> Overlays) BuildSnapshotSections(
        NbnHeaderV2 baseHeader,
        IReadOnlyList<ProtoControl.CaptureShardSnapshotAck> captures)
    {
        var regions = new Dictionary<int, SnapshotRegionBuffer>();
        for (var regionId = 0; regionId < baseHeader.Regions.Length; regionId++)
        {
            var span = (int)baseHeader.Regions[regionId].NeuronSpan;
            if (span == 0)
            {
                continue;
            }

            regions[regionId] = new SnapshotRegionBuffer(span);
        }

        var overlayMap = new Dictionary<(uint From, uint To), byte>();
        foreach (var capture in captures)
        {
            var regionId = (int)capture.RegionId;
            if (!regions.TryGetValue(regionId, out var region))
            {
                throw new InvalidOperationException($"Capture returned unknown region {regionId}.");
            }

            var neuronStart = checked((int)capture.NeuronStart);
            var neuronCount = checked((int)capture.NeuronCount);
            if (neuronCount != capture.BufferCodes.Count)
            {
                throw new InvalidOperationException($"Capture buffer count mismatch for region {regionId}: expected {neuronCount}, got {capture.BufferCodes.Count}.");
            }

            var enabledBytes = capture.EnabledBitset is null ? Array.Empty<byte>() : capture.EnabledBitset.ToByteArray();
            var expectedEnabledBytes = (neuronCount + 7) / 8;
            if (enabledBytes.Length != expectedEnabledBytes)
            {
                throw new InvalidOperationException($"Capture enabled bitset length mismatch for region {regionId}: expected {expectedEnabledBytes}, got {enabledBytes.Length}.");
            }

            for (var i = 0; i < neuronCount; i++)
            {
                var globalNeuron = neuronStart + i;
                if ((uint)globalNeuron >= (uint)region.BufferCodes.Length)
                {
                    throw new InvalidOperationException($"Capture neuron index {globalNeuron} is out of range for region {regionId}.");
                }

                if (region.Assigned[globalNeuron])
                {
                    throw new InvalidOperationException($"Capture overlap detected for region {regionId} neuron {globalNeuron}.");
                }

                region.Assigned[globalNeuron] = true;
                var code = capture.BufferCodes[i];
                code = Math.Clamp(code, 0, ushort.MaxValue);
                region.BufferCodes[globalNeuron] = unchecked((short)(ushort)code);

                if ((enabledBytes[i / 8] & (1 << (i % 8))) != 0)
                {
                    region.EnabledBitset[globalNeuron / 8] |= (byte)(1 << (globalNeuron % 8));
                }
            }

            foreach (var overlay in capture.Overlays)
            {
                var strengthCode = (byte)Math.Clamp((int)overlay.StrengthCode, 0, 31);
                overlayMap[(overlay.FromAddress, overlay.ToAddress)] = strengthCode;
            }
        }

        var regionSections = new List<NbsRegionSection>(regions.Count);
        foreach (var pair in regions.OrderBy(static item => item.Key))
        {
            var regionId = pair.Key;
            var region = pair.Value;
            if (region.Assigned.Any(static assigned => !assigned))
            {
                throw new InvalidOperationException($"Capture did not fully cover region {regionId}.");
            }

            regionSections.Add(new NbsRegionSection((byte)regionId, (uint)region.BufferCodes.Length, region.BufferCodes, region.EnabledBitset));
        }

        var overlayRecords = overlayMap
            .OrderBy(static item => item.Key.From)
            .ThenBy(static item => item.Key.To)
            .Select(static item => new NbsOverlayRecord(item.Key.From, item.Key.To, item.Value))
            .ToList();

        return (regionSections, overlayRecords);
    }

    private void RespondSnapshot(IContext context, Guid brainId, Nbn.Proto.ArtifactRef? snapshot)
    {
        if (HasArtifactRef(snapshot))
        {
            context.Respond(new ProtoIo.SnapshotReady
            {
                BrainId = brainId.ToProtoUuid(),
                Snapshot = snapshot
            });
            return;
        }

        context.Respond(new ProtoIo.SnapshotReady
        {
            BrainId = brainId.ToProtoUuid()
        });
    }

    private static bool HasArtifactRef(Nbn.Proto.ArtifactRef? reference)
        => reference is not null
           && reference.Sha256 is not null
           && reference.Sha256.Value is not null
           && reference.Sha256.Value.Length == 32;

    private static string ResolveArtifactRoot(string? storeUri)
    {
        if (!string.IsNullOrWhiteSpace(storeUri))
        {
            if (Uri.TryCreate(storeUri, UriKind.Absolute, out var uri) && uri.IsFile)
            {
                return uri.LocalPath;
            }

            if (!storeUri.Contains("://", StringComparison.Ordinal))
            {
                return storeUri;
            }
        }

        var envRoot = Environment.GetEnvironmentVariable("NBN_ARTIFACT_ROOT");
        if (!string.IsNullOrWhiteSpace(envRoot))
        {
            return envRoot;
        }

        return Path.Combine(Environment.CurrentDirectory, "artifacts");
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
        if (brain.OutputSinkPid is null)
        {
            Log($"Output sink missing for brain {brain.BrainId}; output shards will not emit until registered.");
            return;
        }

        foreach (var entry in brain.Shards)
        {
            if (entry.Key.RegionId != NbnConstants.OutputRegionId)
            {
                continue;
            }

            SendOutputSinkUpdate(context, brain.BrainId, entry.Key, entry.Value, brain.OutputSinkPid);
        }
    }

    private void SetBrainVisualization(IContext context, BrainState brain, bool enabled, uint? focusRegionId)
    {
        if (brain.VisualizationEnabled == enabled && brain.VisualizationFocusRegionId == focusRegionId)
        {
            return;
        }

        brain.VisualizationEnabled = enabled;
        brain.VisualizationFocusRegionId = enabled ? focusRegionId : null;
        foreach (var entry in brain.Shards)
        {
            SendShardVisualizationUpdate(
                context,
                brain.BrainId,
                entry.Key,
                entry.Value,
                enabled,
                brain.VisualizationFocusRegionId);
        }

        EmitDebug(
            context,
            ProtoSeverity.SevDebug,
            "viz.toggle",
            $"Brain={brain.BrainId} enabled={enabled} focus={(brain.VisualizationFocusRegionId.HasValue ? $"R{brain.VisualizationFocusRegionId.Value}" : "all")} shards={brain.Shards.Count}");
    }

    private void UpdateShardRuntimeConfig(IContext context, BrainState brain)
    {
        foreach (var entry in brain.Shards)
        {
            SendShardRuntimeConfigUpdate(
                context,
                brain.BrainId,
                entry.Key,
                entry.Value,
                brain.CostEnabled,
                brain.EnergyEnabled,
                brain.PlasticityEnabled,
                brain.PlasticityRate,
                brain.PlasticityProbabilisticUpdates);
        }
    }

    private void RegisterBrainWithIo(IContext context, BrainState brain, bool force = false)
    {
        if (_ioPid is null)
        {
            return;
        }

        var inputWidth = (uint)Math.Max(0, brain.InputWidth);
        var outputWidth = (uint)Math.Max(0, brain.OutputWidth);
        if (inputWidth == 0 || outputWidth == 0)
        {
            return;
        }

        if (!force && brain.IoRegistered && brain.IoRegisteredInputWidth == inputWidth && brain.IoRegisteredOutputWidth == outputWidth)
        {
            return;
        }

        var register = new ProtoIo.RegisterBrain
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            InputWidth = inputWidth,
            OutputWidth = outputWidth,
            HasRuntimeConfig = true,
            CostEnabled = brain.CostEnabled,
            EnergyEnabled = brain.EnergyEnabled,
            PlasticityEnabled = brain.PlasticityEnabled,
            PlasticityRate = brain.PlasticityRate,
            PlasticityProbabilisticUpdates = brain.PlasticityProbabilisticUpdates,
            LastTickCost = brain.LastTickCost
        };

        if (brain.BaseDefinition is not null)
        {
            register.BaseDefinition = brain.BaseDefinition;
        }

        if (brain.LastSnapshot is not null)
        {
            register.LastSnapshot = brain.LastSnapshot;
        }

        context.Send(_ioPid, register);

        brain.IoRegistered = true;
        brain.IoRegisteredInputWidth = inputWidth;
        brain.IoRegisteredOutputWidth = outputWidth;
    }

    private static void SendOutputSinkUpdate(IContext context, Guid brainId, ShardId32 shardId, PID shardPid, PID outputSink)
    {
        try
        {
            context.Send(shardPid, new ProtoControl.UpdateShardOutputSink
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = (uint)shardId.RegionId,
                ShardIndex = (uint)shardId.ShardIndex,
                OutputPid = PidLabel(outputSink)
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
        uint? focusRegionId)
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
                FocusRegionId = focusRegionId ?? 0
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
        bool plasticityProbabilisticUpdates)
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
                ProbabilisticUpdates = plasticityProbabilisticUpdates
            });
        }
        catch (Exception ex)
        {
            LogError($"Failed to update shard runtime config for shard {shardId}: {ex.Message}");
        }
    }

    private static void ScheduleSelf(IContext context, TimeSpan delay, object message)
    {
        if (delay <= TimeSpan.Zero)
        {
            context.Send(context.Self, message);
            return;
        }

        context.ReenterAfter(Task.Delay(delay), _ =>
        {
            context.Send(context.Self, message);
            return Task.CompletedTask;
        });
    }

    private sealed record TickStart;
    private sealed record TickPhaseTimeout(ulong TickId, TickPhase Phase);
    private sealed record RefreshWorkerInventoryTick;
    private sealed record RescheduleNow(string Reason);
    private sealed record RescheduleCompleted(string Reason, bool Success);

    private enum TickPhase
    {
        Idle,
        Compute,
        Deliver
    }

    private sealed class TickState
    {
        public TickState(ulong tickId, DateTime startedUtc)
        {
            TickId = tickId;
            StartedUtc = startedUtc;
        }

        public ulong TickId { get; }
        public DateTime StartedUtc { get; }
        public DateTime ComputeStartedUtc { get; set; }
        public DateTime ComputeCompletedUtc { get; set; }
        public DateTime DeliverStartedUtc { get; set; }
        public DateTime DeliverCompletedUtc { get; set; }
        public bool ComputeTimedOut { get; set; }
        public bool DeliverTimedOut { get; set; }
        public int ExpectedComputeCount { get; set; }
        public int CompletedComputeCount { get; set; }
        public int ExpectedDeliverCount { get; set; }
        public int CompletedDeliverCount { get; set; }
        public int LateComputeCount { get; set; }
        public int LateDeliverCount { get; set; }
        public Dictionary<Guid, long> BrainTickCosts { get; } = new();
    }

    private sealed class BrainState
    {
        public BrainState(Guid brainId)
        {
            BrainId = brainId;
        }

        public Guid BrainId { get; }
        public PID? BrainRootPid { get; set; }
        public PID? SignalRouterPid { get; set; }
        public PID? OutputSinkPid { get; set; }
        public int InputWidth { get; set; }
        public int OutputWidth { get; set; }
        public uint IoRegisteredInputWidth { get; set; }
        public uint IoRegisteredOutputWidth { get; set; }
        public bool IoRegistered { get; set; }
        public Nbn.Proto.ArtifactRef? BaseDefinition { get; set; }
        public Nbn.Proto.ArtifactRef? LastSnapshot { get; set; }
        public long LastTickCost { get; set; }
        public bool CostEnabled { get; set; }
        public bool EnergyEnabled { get; set; }
        public bool PlasticityEnabled { get; set; }
        public float PlasticityRate { get; set; }
        public bool PlasticityProbabilisticUpdates { get; set; }
        public bool VisualizationEnabled { get; set; }
        public uint? VisualizationFocusRegionId { get; set; }
        public bool Paused { get; set; }
        public string? PausedReason { get; set; }
        public long SpawnedMs { get; set; }
        public ulong PlacementEpoch { get; set; }
        public long PlacementRequestedMs { get; set; }
        public long PlacementUpdatedMs { get; set; }
        public string PlacementRequestId { get; set; } = string.Empty;
        public ProtoControl.ShardPlan? RequestedShardPlan { get; set; }
        public PlacementPlanner.PlacementPlanningResult? PlannedPlacement { get; set; }
        public ProtoControl.PlacementLifecycleState PlacementLifecycleState { get; set; }
            = ProtoControl.PlacementLifecycleState.PlacementLifecycleUnknown;
        public ProtoControl.PlacementFailureReason PlacementFailureReason { get; set; }
            = ProtoControl.PlacementFailureReason.PlacementFailureNone;
        public ProtoControl.PlacementReconcileState PlacementReconcileState { get; set; }
            = ProtoControl.PlacementReconcileState.PlacementReconcileUnknown;
        public Dictionary<ShardId32, PID> Shards { get; } = new();
        public RoutingTableSnapshot RoutingSnapshot { get; set; } = RoutingTableSnapshot.Empty;
    }

    private sealed class WorkerCatalogEntry
    {
        public WorkerCatalogEntry(Guid nodeId)
        {
            NodeId = nodeId;
        }

        public Guid NodeId { get; }
        public string WorkerAddress { get; set; } = string.Empty;
        public string WorkerRootActorName { get; set; } = string.Empty;
        public bool IsAlive { get; set; }
        public bool IsReady { get; set; }
        public bool IsFresh { get; set; }
        public long LastSeenMs { get; set; }
        public long CapabilitySnapshotMs { get; set; }
        public long LastUpdatedMs { get; set; }
        public uint CpuCores { get; set; }
        public long RamFreeBytes { get; set; }
        public bool HasGpu { get; set; }
        public long VramFreeBytes { get; set; }
        public float CpuScore { get; set; }
        public float GpuScore { get; set; }
    }

    private sealed record SnapshotBuildRequest(
        Guid BrainId,
        Nbn.Proto.ArtifactRef BaseDefinition,
        ulong SnapshotTickId,
        long EnergyRemaining,
        bool CostEnabled,
        bool EnergyEnabled,
        bool PlasticityEnabled,
        Dictionary<ShardId32, PID> Shards,
        string StoreRootPath,
        string StoreUri);

    private sealed record RebasedDefinitionBuildRequest(
        Guid BrainId,
        Nbn.Proto.ArtifactRef BaseDefinition,
        ulong SnapshotTickId,
        Dictionary<ShardId32, PID> Shards,
        string StoreRootPath,
        string StoreUri);

    private sealed class SnapshotRegionBuffer
    {
        public SnapshotRegionBuffer(int neuronSpan)
        {
            BufferCodes = new short[neuronSpan];
            EnabledBitset = new byte[(neuronSpan + 7) / 8];
            Assigned = new bool[neuronSpan];
        }

        public short[] BufferCodes { get; }
        public byte[] EnabledBitset { get; }
        public bool[] Assigned { get; }
    }

    private readonly record struct ShardKey(Guid BrainId, ShardId32 ShardId);

    private void ReportBrainRegistration(IContext context, BrainState brain)
    {
        if (_settingsPid is null)
        {
            return;
        }

        var controllerPid = brain.BrainRootPid ?? brain.SignalRouterPid;
        var nodeAddress = controllerPid is null ? string.Empty : ResolveNodeAddress(context, controllerPid);
        var nodeId = string.IsNullOrWhiteSpace(nodeAddress) ? Guid.Empty : DeriveNodeId(nodeAddress);

        var message = new ProtoSettings.BrainRegistered
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            SpawnedMs = brain.SpawnedMs > 0 ? (ulong)brain.SpawnedMs : 0,
            LastTickId = _lastCompletedTickId,
            State = brain.Paused ? "Paused" : "Active",
            ControllerNodeAddress = nodeAddress,
            ControllerNodeLogicalName = nodeAddress,
            ControllerRootActorName = controllerPid?.Id ?? string.Empty,
            ControllerActorName = controllerPid is null ? string.Empty : PidLabel(controllerPid)
        };

        if (nodeId != Guid.Empty)
        {
            message.ControllerNodeId = nodeId.ToProtoUuid();
        }

        context.Send(_settingsPid, message);
    }

    private void ReportBrainUnregistered(IContext context, Guid brainId)
    {
        if (_settingsPid is null)
        {
            return;
        }

        context.Send(_settingsPid, new ProtoSettings.BrainUnregistered
        {
            BrainId = brainId.ToProtoUuid(),
            TimeMs = (ulong)NowMs()
        });
    }

    private void ReportBrainState(IContext context, Guid brainId, string state, string? notes)
    {
        if (_settingsPid is null)
        {
            return;
        }

        context.Send(_settingsPid, new ProtoSettings.BrainStateChanged
        {
            BrainId = brainId.ToProtoUuid(),
            State = state,
            Notes = notes ?? string.Empty
        });
    }

    private void ReportBrainTick(IContext context, Guid brainId, ulong tickId)
    {
        if (_settingsPid is null)
        {
            return;
        }

        context.Send(_settingsPid, new ProtoSettings.BrainTick
        {
            BrainId = brainId.ToProtoUuid(),
            LastTickId = tickId
        });

        context.Send(_settingsPid, new ProtoSettings.BrainControllerHeartbeat
        {
            BrainId = brainId.ToProtoUuid(),
            TimeMs = (ulong)NowMs()
        });
    }

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static ulong ToProtoMs(long value)
        => value > 0 ? (ulong)value : 0;

    private static ulong ToProtoBytes(long value)
        => value > 0 ? (ulong)value : 0;

    private static string ResolveNodeAddress(IContext context, PID pid)
    {
        if (!string.IsNullOrWhiteSpace(pid.Address))
        {
            return pid.Address;
        }

        var systemAddress = context.System.Address;
        return string.IsNullOrWhiteSpace(systemAddress) ? "local" : systemAddress;
    }

    private static Guid DeriveNodeId(string address)
        => NodeIdentity.DeriveNodeId(address);

    private void EmitVizEvent(
        IContext context,
        VizEventType type,
        Guid? brainId = null,
        ulong tickId = 0,
        uint regionId = 0,
        ShardId32? shardId = null)
    {
        if (!IsVisualizationEmissionEnabled(brainId)
            || _vizHubPid is null
            || !ObservabilityTargets.CanSend(context, _vizHubPid))
        {
            return;
        }

        var evt = new VisualizationEvent
        {
            EventId = $"hm-{++_vizSequence}",
            TimeMs = (ulong)NowMs(),
            Type = type,
            TickId = tickId,
            RegionId = regionId
        };

        if (brainId.HasValue)
        {
            evt.BrainId = brainId.Value.ToProtoUuid();
        }

        if (shardId.HasValue)
        {
            evt.ShardId = shardId.Value.ToProtoShardId32();
        }

        context.Send(_vizHubPid, evt);
    }

    private bool IsVisualizationEmissionEnabled(Guid? brainId)
    {
        if (brainId.HasValue)
        {
            return _brains.TryGetValue(brainId.Value, out var brain) && brain.VisualizationEnabled;
        }

        foreach (var brain in _brains.Values)
        {
            if (brain.VisualizationEnabled)
            {
                return true;
            }
        }

        return false;
    }

    private void EmitDebug(IContext context, ProtoSeverity severity, string category, string message)
    {
        if (_debugHubPid is null || !ObservabilityTargets.CanSend(context, _debugHubPid))
        {
            return;
        }

        context.Send(_debugHubPid, new DebugOutbound
        {
            Severity = severity,
            Context = $"hivemind.{category}",
            Summary = category,
            Message = message,
            SenderActor = PidLabel(context.Self),
            SenderNode = context.System.Address ?? string.Empty,
            TimeMs = (ulong)NowMs()
        });
    }

    private static PID? BuildSettingsPid(HiveMindOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.SettingsHost))
        {
            return null;
        }

        if (options.SettingsPort <= 0)
        {
            return null;
        }

        if (string.IsNullOrWhiteSpace(options.SettingsName))
        {
            return null;
        }

        return new PID($"{options.SettingsHost}:{options.SettingsPort}", options.SettingsName);
    }

    private static PID? BuildIoPid(HiveMindOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.IoAddress))
        {
            return null;
        }

        if (string.IsNullOrWhiteSpace(options.IoName))
        {
            return null;
        }

        return new PID(options.IoAddress, options.IoName);
    }
}
