using System.Globalization;
using System.Reflection;
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
    private bool _debugStreamEnabled;
    private bool _systemCostEnergyEnabled = true;
    private bool _remoteCostEnabled;
    private long _remoteCostPerBatch;
    private long _remoteCostPerContribution;
    private float _costTierAMultiplier = 1f;
    private float _costTierBMultiplier = 1f;
    private float _costTierCMultiplier = 1f;
    private bool _systemPlasticityEnabled = true;
    private ProtoSeverity _debugMinSeverity;
    private bool _debugSettingsSubscribed;
    private readonly Dictionary<Guid, BrainState> _brains = new();
    private readonly Dictionary<Guid, PendingSpawnState> _pendingSpawns = new();
    private readonly Dictionary<Guid, WorkerCatalogEntry> _workerCatalog = new();
    private readonly HashSet<ShardKey> _pendingCompute = new();
    private readonly Dictionary<ShardKey, PID> _pendingComputeSenders = new();
    private readonly HashSet<Guid> _pendingDeliver = new();
    private readonly Dictionary<Guid, PID> _pendingDeliverSenders = new();
    private readonly Dictionary<string, VisualizationSubscriberLease> _vizSubscriberLeases = new(StringComparer.Ordinal);
    private readonly HashSet<string> _knownSettingsNodeAddresses = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _activeSettingsNodeAddresses = new(StringComparer.OrdinalIgnoreCase);
    private ulong _vizSequence;
    private long _nextVisualizationShardSyncMs;
    private long _workerCatalogSnapshotMs;
    private static readonly bool LogTickBarrier = IsEnvTrue("NBN_HIVEMIND_LOG_TICK_BARRIER");
    private static readonly bool LogVizDiagnostics = IsEnvTrue("NBN_VIZ_DIAGNOSTICS_ENABLED");
    private static readonly bool LogMetadataDiagnostics =
        IsEnvTrue("NBN_METADATA_DIAGNOSTICS_ENABLED") || IsEnvTrue("NBN_HIVEMIND_METADATA_DIAGNOSTICS_ENABLED");
    private static readonly TimeSpan VisualizationSubscriberSweepInterval = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan VisualizationShardSyncInterval = TimeSpan.FromSeconds(1);
    private static readonly PropertyInfo? ProcessRegistryProperty = typeof(ActorSystem).GetProperty(
        "ProcessRegistry",
        BindingFlags.Instance | BindingFlags.Public);
    private static readonly MethodInfo? ProcessRegistryLookupMethod = ResolveProcessRegistryLookupMethod();
    private static readonly TimeSpan SnapshotShardRequestTimeout = TimeSpan.FromSeconds(5);
    private static readonly QuantizationMap SnapshotBufferQuantization = QuantizationSchemas.DefaultBuffer;
    private const float DefaultPlasticityRate = 0.001f;
    private const float DefaultPlasticityDelta = DefaultPlasticityRate;

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
        PID? settingsPid = null,
        bool? debugStreamEnabled = null,
        ProtoSeverity? debugMinSeverity = null)
    {
        _options = options;
        _backpressure = new BackpressureController(options);
        _tickLoopEnabled = options.AutoStart;
        _settingsPid = settingsPid ?? BuildSettingsPid(options);
        _ioPid = ioPid ?? BuildIoPid(options);
        var resolvedObs = ObservabilityTargets.Resolve(options.SettingsHost);
        _debugHubPid = debugHubPid ?? resolvedObs.DebugHub;
        _vizHubPid = vizHubPid ?? resolvedObs.VizHub;
        _debugStreamEnabled = debugStreamEnabled ?? _settingsPid is null;
        _debugMinSeverity = NormalizeDebugSeverity(debugMinSeverity ?? ProtoSeverity.SevDebug);
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

                    ScheduleSelf(context, VisualizationSubscriberSweepInterval, new SweepVisualizationSubscribers());

                    if (_settingsPid is not null)
                    {
                        EnsureDebugSettingsSubscription(context);
                        RefreshDebugSettings(context);
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
                case ProtoControl.SetBrainHomeostasis message:
                    HandleSetBrainHomeostasis(context, message);
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
                case ProtoControl.SpawnBrain message:
                    HandleSpawnBrain(context, message);
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
                case ProtoControl.PlacementUnassignmentAck message:
                    HandlePlacementUnassignmentAck(context, message);
                    break;
                case ProtoControl.PlacementReconcileReport message:
                    HandlePlacementReconcileReport(context, message);
                    break;
                case DispatchPlacementPlan message:
                    HandleDispatchPlacementPlan(context, message);
                    break;
                case RetryPlacementAssignment message:
                    HandleRetryPlacementAssignment(context, message);
                    break;
                case PlacementAssignmentTimeout message:
                    HandlePlacementAssignmentTimeout(context, message);
                    break;
                case PlacementReconcileTimeout message:
                    HandlePlacementReconcileTimeout(context, message);
                    break;
                case SpawnCompletionTimeout message:
                    HandleSpawnCompletionTimeout(context, message);
                    break;
                case ProtoSettings.WorkerInventorySnapshotResponse message:
                    HandleWorkerInventorySnapshotResponse(message);
                    break;
                case ProtoSettings.NodeListResponse message:
                    HandleNodeListResponse(message);
                    break;
                case ProtoSettings.SettingValue message:
                    HandleSettingValue(context, message);
                    break;
                case ProtoSettings.SettingChanged message:
                    HandleSettingChanged(context, message);
                    break;
                case SweepVisualizationSubscribers:
                    HandleSweepVisualizationSubscribers(context);
                    break;
                case PauseBrainRequest message:
                    PauseBrain(context, message.BrainId, message.Reason);
                    break;
                case ResumeBrainRequest message:
                    ResumeBrain(context, message.BrainId);
                    break;
                case ProtoControl.PauseBrain message:
                    HandlePauseBrainControl(context, message);
                    break;
                case ProtoControl.ResumeBrain message:
                    HandleResumeBrainControl(context, message);
                    break;
                case ProtoControl.KillBrain message:
                    HandleKillBrainControl(context, message);
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
                case Terminated message:
                    HandleVisualizationSubscriberTerminated(context, message.Who);
                    break;
        }

        return Task.CompletedTask;
    }

    private void EnsureDebugSettingsSubscription(IContext context)
    {
        if (_settingsPid is null || _debugSettingsSubscribed)
        {
            return;
        }

        context.Send(_settingsPid, new ProtoSettings.SettingSubscribe
        {
            SubscriberActor = PidLabel(context.Self)
        });
        _debugSettingsSubscribed = true;
    }

    private void RefreshDebugSettings(IContext context)
    {
        if (_settingsPid is null)
        {
            return;
        }

        context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = DebugSettingsKeys.EnabledKey });
        context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = DebugSettingsKeys.MinSeverityKey });
        foreach (var key in CostEnergySettingsKeys.AllKeys)
        {
            context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = key });
        }
        context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = PlasticitySettingsKeys.SystemEnabledKey });
    }

    private void HandleSettingValue(IContext context, ProtoSettings.SettingValue message)
    {
        if (message is null)
        {
            return;
        }

        if (TryApplyDebugSetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
        }

        if (TryApplySystemCostEnergySetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplySystemPlasticitySetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
            RegisterAllBrainsWithIo(context);
        }
    }

    private void HandleSettingChanged(IContext context, ProtoSettings.SettingChanged message)
    {
        if (message is null)
        {
            return;
        }

        if (TryApplyDebugSetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
        }

        if (TryApplySystemCostEnergySetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplySystemPlasticitySetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
            RegisterAllBrainsWithIo(context);
        }
    }

    private bool TryApplyDebugSetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        if (string.Equals(key, DebugSettingsKeys.EnabledKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseDebugEnabledSetting(value, _debugStreamEnabled);
            if (parsed == _debugStreamEnabled)
            {
                return false;
            }

            _debugStreamEnabled = parsed;
            return true;
        }

        if (string.Equals(key, DebugSettingsKeys.MinSeverityKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseDebugSeveritySetting(value, _debugMinSeverity);
            if (parsed == _debugMinSeverity)
            {
                return false;
            }

            _debugMinSeverity = parsed;
            return true;
        }

        return false;
    }

    private bool TryApplySystemCostEnergySetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        if (string.Equals(key, CostEnergySettingsKeys.SystemEnabledKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseBooleanSetting(value, _systemCostEnergyEnabled);
            if (parsed == _systemCostEnergyEnabled)
            {
                return false;
            }

            _systemCostEnergyEnabled = parsed;
            return true;
        }

        if (string.Equals(key, CostEnergySettingsKeys.RemoteCostEnabledKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseBooleanSetting(value, _remoteCostEnabled);
            if (parsed == _remoteCostEnabled)
            {
                return false;
            }

            _remoteCostEnabled = parsed;
            return true;
        }

        if (string.Equals(key, CostEnergySettingsKeys.RemoteCostPerBatchKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseNonNegativeInt64Setting(value, _remoteCostPerBatch);
            if (parsed == _remoteCostPerBatch)
            {
                return false;
            }

            _remoteCostPerBatch = parsed;
            return true;
        }

        if (string.Equals(key, CostEnergySettingsKeys.RemoteCostPerContributionKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseNonNegativeInt64Setting(value, _remoteCostPerContribution);
            if (parsed == _remoteCostPerContribution)
            {
                return false;
            }

            _remoteCostPerContribution = parsed;
            return true;
        }

        if (string.Equals(key, CostEnergySettingsKeys.TierAMultiplierKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParsePositiveFiniteFloatSetting(value, _costTierAMultiplier);
            if (Math.Abs(parsed - _costTierAMultiplier) < 0.000001f)
            {
                return false;
            }

            _costTierAMultiplier = parsed;
            return true;
        }

        if (string.Equals(key, CostEnergySettingsKeys.TierBMultiplierKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParsePositiveFiniteFloatSetting(value, _costTierBMultiplier);
            if (Math.Abs(parsed - _costTierBMultiplier) < 0.000001f)
            {
                return false;
            }

            _costTierBMultiplier = parsed;
            return true;
        }

        if (string.Equals(key, CostEnergySettingsKeys.TierCMultiplierKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParsePositiveFiniteFloatSetting(value, _costTierCMultiplier);
            if (Math.Abs(parsed - _costTierCMultiplier) < 0.000001f)
            {
                return false;
            }

            _costTierCMultiplier = parsed;
            return true;
        }

        return false;
    }

    private bool TryApplySystemPlasticitySetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, PlasticitySettingsKeys.SystemEnabledKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var parsed = ParseBooleanSetting(value, _systemPlasticityEnabled);
        if (parsed == _systemPlasticityEnabled)
        {
            return false;
        }

        _systemPlasticityEnabled = parsed;
        return true;
    }

    private void UpdateAllShardRuntimeConfig(IContext context)
    {
        foreach (var brain in _brains.Values)
        {
            UpdateShardRuntimeConfig(context, brain);
        }
    }

    private void RegisterAllBrainsWithIo(IContext context)
    {
        foreach (var brain in _brains.Values)
        {
            RegisterBrainWithIo(context, brain, force: true);
        }
    }

    private static bool ParseDebugEnabledSetting(string? value, bool fallback)
        => ParseBooleanSetting(value, fallback);

    private static bool ParseBooleanSetting(string? value, bool fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "1" or "true" or "yes" or "on" => true,
            "0" or "false" or "no" or "off" => false,
            _ => fallback
        };
    }

    private static long ParseNonNegativeInt64Setting(string? value, long fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !long.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return fallback;
        }

        return Math.Max(0L, parsed);
    }

    private static float ParsePositiveFiniteFloatSetting(string? value, float fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !float.TryParse(value.Trim(), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsed)
            || !float.IsFinite(parsed)
            || parsed <= 0f)
        {
            return fallback;
        }

        return parsed;
    }

    private static ProtoSeverity ParseDebugSeveritySetting(string? value, ProtoSeverity fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        if (Enum.TryParse<ProtoSeverity>(value, ignoreCase: true, out var direct))
        {
            return NormalizeDebugSeverity(direct);
        }

        var normalized = value.Trim().ToLowerInvariant();
        return normalized switch
        {
            "trace" or "sev_trace" => ProtoSeverity.SevTrace,
            "debug" or "sev_debug" => ProtoSeverity.SevDebug,
            "info" or "sev_info" => ProtoSeverity.SevInfo,
            "warn" or "warning" or "sev_warn" => ProtoSeverity.SevWarn,
            "error" or "sev_error" => ProtoSeverity.SevError,
            "fatal" or "sev_fatal" => ProtoSeverity.SevFatal,
            _ => fallback
        };
    }

    private static ProtoSeverity NormalizeDebugSeverity(ProtoSeverity severity)
    {
        return severity switch
        {
            ProtoSeverity.SevTrace or ProtoSeverity.SevDebug or ProtoSeverity.SevInfo or ProtoSeverity.SevWarn or ProtoSeverity.SevError or ProtoSeverity.SevFatal => severity,
            _ => ProtoSeverity.SevInfo
        };
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
        SendShardVisualizationUpdate(
            context,
            brainId,
            shardId,
            normalized,
            brain.VisualizationEnabled,
            brain.VisualizationFocusRegionId);
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

        if (!IsUpdateBrainSignalRouterAuthorized(context, brainId, routerPid, out var reason))
        {
            EmitControlPlaneMutationIgnored(context, "control.update_brain_signal_router", brainId, reason);
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

        var effectiveDelta = ResolvePlasticityDelta(message.PlasticityRate, message.PlasticityDelta);
        if (brain.PlasticityEnabled == message.PlasticityEnabled
            && Math.Abs(brain.PlasticityRate - message.PlasticityRate) < 0.000001f
            && brain.PlasticityProbabilisticUpdates == message.ProbabilisticUpdates
            && Math.Abs(brain.PlasticityDelta - effectiveDelta) < 0.000001f
            && brain.PlasticityRebaseThreshold == message.PlasticityRebaseThreshold
            && Math.Abs(brain.PlasticityRebaseThresholdPct - message.PlasticityRebaseThresholdPct) < 0.000001f)
        {
            return;
        }

        brain.PlasticityEnabled = message.PlasticityEnabled;
        brain.PlasticityRate = message.PlasticityRate;
        brain.PlasticityProbabilisticUpdates = message.ProbabilisticUpdates;
        brain.PlasticityDelta = effectiveDelta;
        brain.PlasticityRebaseThreshold = message.PlasticityRebaseThreshold;
        brain.PlasticityRebaseThresholdPct = message.PlasticityRebaseThresholdPct;
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

    private void HandleSpawnBrain(IContext context, ProtoControl.SpawnBrain message)
    {
        Guid brainId = Guid.Empty;
        try
        {
            if (message.BrainDef is null
                || !HasArtifactRef(message.BrainDef)
                || !string.Equals(message.BrainDef.MediaType, "application/x-nbn", StringComparison.OrdinalIgnoreCase))
            {
                context.Respond(BuildSpawnFailureAck(
                    reasonCode: "spawn_invalid_request",
                    failureMessage: "Spawn request rejected: brain definition must be a valid application/x-nbn artifact reference."));
                return;
            }

            do
            {
                brainId = Guid.NewGuid();
            } while (_brains.ContainsKey(brainId) || _pendingSpawns.ContainsKey(brainId));

            var placementAck = ProcessPlacementRequest(
                context,
                new ProtoControl.RequestPlacement
                {
                    BrainId = brainId.ToProtoUuid(),
                    BaseDef = message.BrainDef.Clone(),
                    RequestId = $"spawn:{brainId:N}",
                    RequestedMs = (ulong)NowMs()
                });

            if (!placementAck.Accepted || !_brains.TryGetValue(brainId, out var brain))
            {
                if (_brains.ContainsKey(brainId))
                {
                    UnregisterBrain(context, brainId, reason: "spawn_failed");
                }

                var reasonCode = ToSpawnFailureReasonCode(placementAck.FailureReason);
                var failureMessage = !string.IsNullOrWhiteSpace(placementAck.Message)
                    ? placementAck.Message
                    : BuildSpawnFailureMessage(
                        placementAck.FailureReason,
                        detail: null,
                        fallbackReasonCode: reasonCode);
                context.Respond(BuildSpawnFailureAck(reasonCode, failureMessage));
                return;
            }

            var pending = new PendingSpawnState(brain.BrainId, brain.PlacementEpoch);
            _pendingSpawns[brain.BrainId] = pending;

            ScheduleSelf(
                context,
                TimeSpan.FromMilliseconds(ComputeSpawnCompletionTimeoutMs()),
                new SpawnCompletionTimeout(brain.BrainId, brain.PlacementEpoch));

            context.ReenterAfter(
                pending.Completion.Task,
                task =>
                {
                    var completed = task.IsCompletedSuccessfully && task.Result;
                    if (completed)
                    {
                        context.Respond(new ProtoControl.SpawnBrainAck
                        {
                            BrainId = brain.BrainId.ToProtoUuid()
                        });
                        return Task.CompletedTask;
                    }

                    context.Respond(BuildSpawnFailureAck(
                        reasonCode: pending.FailureReasonCode,
                        failureMessage: pending.FailureMessage));
                    return Task.CompletedTask;
                });
        }
        catch (Exception ex)
        {
            if (brainId != Guid.Empty)
            {
                if (_pendingSpawns.Remove(brainId, out var pending))
                {
                    pending.SetFailure(
                        reasonCode: "spawn_internal_error",
                        failureMessage: $"Spawn failed: internal error while preparing placement ({ex.GetBaseException().Message}).");
                    pending.Completion.TrySetResult(false);
                }

                if (_brains.ContainsKey(brainId))
                {
                    try
                    {
                        UnregisterBrain(context, brainId, reason: "spawn_internal_error");
                    }
                    catch (Exception cleanupEx)
                    {
                        LogError($"Spawn cleanup failed for brain {brainId}: {cleanupEx.GetBaseException().Message}");
                        _brains.Remove(brainId);
                    }
                }
            }

            LogError($"Spawn failed while preparing brain {brainId}: {ex}");
            context.Respond(BuildSpawnFailureAck(
                reasonCode: "spawn_internal_error",
                failureMessage: $"Spawn failed: internal error while preparing placement ({ex.GetBaseException().Message})."));
        }
    }

    private void HandleRequestPlacement(IContext context, ProtoControl.RequestPlacement message)
        => context.Respond(ProcessPlacementRequest(context, message));

    private ProtoControl.PlacementAck ProcessPlacementRequest(IContext context, ProtoControl.RequestPlacement message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            HiveMindTelemetry.RecordPlacementRequestRejected(
                brainId: null,
                placementEpoch: 0,
                failureReason: "invalid_brain_id");
            return new ProtoControl.PlacementAck
            {
                Accepted = false,
                Message = "Invalid brain id.",
                PlacementEpoch = 0,
                LifecycleState = ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
                FailureReason = ProtoControl.PlacementFailureReason.PlacementFailureInvalidBrain,
                AcceptedMs = (ulong)NowMs()
            };
        }

        if (!_brains.TryGetValue(brainId, out var brain))
        {
            brain = new BrainState(brainId)
            {
                SpawnedMs = NowMs()
            };
            _brains[brainId] = brain;
        }

        var previousExecution = brain.PlacementExecution;
        if (previousExecution is not null)
        {
            DispatchPlacementUnassignments(context, brain, previousExecution, reason: "placement_replaced");
        }

        var nowMs = NowMs();
        brain.PlacementEpoch = brain.PlacementEpoch >= ulong.MaxValue ? 1UL : brain.PlacementEpoch + 1UL;
        brain.PlacementRequestedMs = nowMs;
        brain.PlacementRequestId = string.IsNullOrWhiteSpace(message.RequestId)
            ? $"{brainId:N}:{brain.PlacementEpoch}"
            : message.RequestId;
        brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileUnknown;
        brain.SpawnFailureReasonCode = string.Empty;
        brain.SpawnFailureMessage = string.Empty;

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
            brain.PlacementExecution = null;
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
                failureReason);
            brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileFailed;
            SetSpawnFailureDetails(
                brain,
                ToSpawnFailureReasonCode(failureReason),
                BuildSpawnFailureMessage(failureReason, failureMessage));

            RegisterBrainWithIo(context, brain, force: true);

            var failedPlanLabel = message.ShardPlan is null ? "none" : message.ShardPlan.Mode.ToString();
            Log(
                $"Placement request rejected for brain {brainId} epoch={brain.PlacementEpoch} request={brain.PlacementRequestId} plan={failedPlanLabel} reason={failureReason}: {failureMessage}");
            HiveMindTelemetry.RecordPlacementRequestRejected(
                brainId,
                brain.PlacementEpoch,
                ToFailureReasonLabel(failureReason));
            EmitDebug(
                context,
                ProtoSeverity.SevWarn,
                "placement.request.rejected",
                $"Placement request rejected for brain {brainId} epoch={brain.PlacementEpoch} reason={ToFailureReasonLabel(failureReason)}.");

            return new ProtoControl.PlacementAck
            {
                Accepted = false,
                Message = failureMessage,
                PlacementEpoch = brain.PlacementEpoch,
                LifecycleState = brain.PlacementLifecycleState,
                FailureReason = brain.PlacementFailureReason,
                AcceptedMs = (ulong)brain.PlacementUpdatedMs,
                RequestId = brain.PlacementRequestId
            };
        }

        if (!TryCreatePlacementExecution(context, brain, plannedPlacement, out var executionFailure))
        {
            brain.PlannedPlacement = null;
            brain.PlacementExecution = null;
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
                ProtoControl.PlacementFailureReason.PlacementFailureInternalError);
            brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileFailed;
            SetSpawnFailureDetails(
                brain,
                ToSpawnFailureReasonCode(ProtoControl.PlacementFailureReason.PlacementFailureInternalError),
                BuildSpawnFailureMessage(
                    ProtoControl.PlacementFailureReason.PlacementFailureInternalError,
                    executionFailure));
            HiveMindTelemetry.RecordPlacementRequestRejected(
                brainId,
                brain.PlacementEpoch,
                ToFailureReasonLabel(ProtoControl.PlacementFailureReason.PlacementFailureInternalError));
            EmitDebug(
                context,
                ProtoSeverity.SevWarn,
                "placement.request.rejected",
                $"Placement request rejected for brain {brainId} epoch={brain.PlacementEpoch} reason={ToFailureReasonLabel(ProtoControl.PlacementFailureReason.PlacementFailureInternalError)}.");

            return new ProtoControl.PlacementAck
            {
                Accepted = false,
                Message = executionFailure,
                PlacementEpoch = brain.PlacementEpoch,
                LifecycleState = brain.PlacementLifecycleState,
                FailureReason = brain.PlacementFailureReason,
                AcceptedMs = (ulong)brain.PlacementUpdatedMs,
                RequestId = brain.PlacementRequestId
            };
        }

        brain.PlannedPlacement = plannedPlacement.Clone();
        UpdateBrainIoWidthsFromPlannedAssignments(brain, plannedPlacement);
        UpdatePlacementLifecycle(
            brain,
            ProtoControl.PlacementLifecycleState.PlacementLifecycleRequested,
            ProtoControl.PlacementFailureReason.PlacementFailureNone);
        brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileUnknown;

        RegisterBrainWithIo(context, brain, force: true);

        var planLabel = message.ShardPlan is null ? "none" : message.ShardPlan.Mode.ToString();
        var plannerWarnings = plannedPlacement.PlannerWarnings.Count == 0
            ? "none"
            : string.Join("|", plannedPlacement.PlannerWarnings);
        Log(
            $"Placement requested for brain {brainId} epoch={brain.PlacementEpoch} request={brain.PlacementRequestId} plan={planLabel} input={message.InputWidth} output={message.OutputWidth} assignments={plannedPlacement.Assignments.Count} workers={plannedPlacement.EligibleWorkers.Count} warnings={plannerWarnings}");
        HiveMindTelemetry.RecordPlacementRequestAccepted(
            brainId,
            brain.PlacementEpoch,
            plannedPlacement.Assignments.Count,
            plannedPlacement.EligibleWorkers.Count);
        EmitDebug(
            context,
            ProtoSeverity.SevInfo,
            "placement.requested",
            $"Placement requested for brain {brainId} epoch={brain.PlacementEpoch} assignments={plannedPlacement.Assignments.Count} workers={plannedPlacement.EligibleWorkers.Count}.");

        ScheduleSelf(context, TimeSpan.Zero, new DispatchPlacementPlan(brainId, brain.PlacementEpoch));

        return new ProtoControl.PlacementAck
        {
            Accepted = true,
            Message = "Placement request accepted.",
            PlacementEpoch = brain.PlacementEpoch,
            LifecycleState = brain.PlacementLifecycleState,
            FailureReason = brain.PlacementFailureReason,
            AcceptedMs = (ulong)brain.PlacementUpdatedMs,
            RequestId = brain.PlacementRequestId
        };
    }

    private void HandlePlacementUnassignmentAck(IContext context, ProtoControl.PlacementUnassignmentAck message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        var result = message.Accepted ? "accepted" : "rejected";
        var assignmentId = string.IsNullOrWhiteSpace(message.AssignmentId) ? "<missing>" : message.AssignmentId;
        var failure = ToFailureReasonLabel(message.FailureReason);
        EmitDebug(
            context,
            ProtoSeverity.SevDebug,
            "placement.unassignment.ack",
            $"Placement unassignment ack for brain {brainId} assignment={assignmentId} epoch={message.PlacementEpoch} result={result} failure={failure}.");
    }

    private void HandlePlacementAssignmentAck(IContext context, ProtoControl.PlacementAssignmentAck message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId) || !_brains.TryGetValue(brainId, out var brain))
        {
            return;
        }

        var execution = brain.PlacementExecution;

        if (brain.PlacementEpoch == 0 || message.PlacementEpoch != brain.PlacementEpoch)
        {
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.assignment_ack.ignored",
                $"Ignored assignment ack for brain {brainId}; epoch={message.PlacementEpoch} current={brain.PlacementEpoch}.");
            return;
        }

        if (execution is not null
            && execution.PlacementEpoch == brain.PlacementEpoch
            && execution.Completed)
        {
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.assignment_ack.ignored",
                $"Ignored assignment ack for brain {brainId}; placement execution already completed for epoch={message.PlacementEpoch}.");
            return;
        }

        if (execution is not null && execution.PlacementEpoch == brain.PlacementEpoch)
        {
            if (string.IsNullOrWhiteSpace(message.AssignmentId)
                || !execution.Assignments.TryGetValue(message.AssignmentId, out var trackedAssignment))
            {
                var assignmentId = string.IsNullOrWhiteSpace(message.AssignmentId)
                    ? "<missing>"
                    : message.AssignmentId;
                EmitDebug(
                    context,
                    ProtoSeverity.SevDebug,
                    "placement.assignment_ack.ignored",
                    $"Ignored assignment ack for brain {brainId}; assignment={assignmentId} is not tracked for epoch={message.PlacementEpoch}.");
                return;
            }

            if (trackedAssignment.Ready || trackedAssignment.Failed || !trackedAssignment.AwaitingAck)
            {
                EmitDebug(
                    context,
                    ProtoSeverity.SevDebug,
                    "placement.assignment_ack.ignored",
                    $"Ignored assignment ack for brain {brainId}; assignment={trackedAssignment.Assignment.AssignmentId} is not awaiting ack for epoch={message.PlacementEpoch}.");
                return;
            }

            if (!TryGetGuid(trackedAssignment.Assignment.WorkerNodeId, out var plannedWorkerId))
            {
                EmitDebug(
                    context,
                    ProtoSeverity.SevDebug,
                    "placement.assignment_ack.ignored",
                    $"Ignored assignment ack for brain {brainId}; assignment={trackedAssignment.Assignment.AssignmentId} reason=planned_worker_invalid.");
                return;
            }

            if (!execution.WorkerTargets.TryGetValue(plannedWorkerId, out var plannedWorkerPid))
            {
                EmitDebug(
                    context,
                    ProtoSeverity.SevDebug,
                    "placement.assignment_ack.ignored",
                    $"Ignored assignment ack for brain {brainId}; assignment={trackedAssignment.Assignment.AssignmentId} reason=planned_worker_unresolved plannedWorker={plannedWorkerId:D}.");
                return;
            }

            if (!SenderMatchesPid(context.Sender, plannedWorkerPid))
            {
                var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
                EmitDebug(
                    context,
                    ProtoSeverity.SevDebug,
                    "placement.assignment_ack.ignored",
                    $"Ignored assignment ack for brain {brainId}; assignment={trackedAssignment.Assignment.AssignmentId} reason=sender_worker_mismatch sender={senderLabel} plannedWorker={plannedWorkerId:D} plannedPid={PidLabel(plannedWorkerPid)}.");
                return;
            }

            trackedAssignment.AwaitingAck = false;
            trackedAssignment.AcceptedMs = NowMs();
            var target = ToPlacementTargetLabel(trackedAssignment.Assignment.Target);
            var ackFailureReason = message.FailureReason == ProtoControl.PlacementFailureReason.PlacementFailureNone
                ? (!message.Accepted || message.State == ProtoControl.PlacementAssignmentState.PlacementAssignmentFailed
                    ? ToFailureReasonLabel(ProtoControl.PlacementFailureReason.PlacementFailureAssignmentRejected)
                    : "none")
                : ToFailureReasonLabel(message.FailureReason);
            var ackLatencyMs = trackedAssignment.LastDispatchMs > 0
                ? trackedAssignment.AcceptedMs - trackedAssignment.LastDispatchMs
                : 0;
            HiveMindTelemetry.RecordPlacementAssignmentAck(
                brain.BrainId,
                brain.PlacementEpoch,
                target,
                ToAssignmentStateLabel(message.State),
                message.Accepted,
                message.Retryable,
                ackLatencyMs > 0 ? ackLatencyMs : 0,
                plannedWorkerId,
                ackFailureReason);

            if (!message.Accepted || message.State == ProtoControl.PlacementAssignmentState.PlacementAssignmentFailed)
            {
                var failureReason = message.FailureReason == ProtoControl.PlacementFailureReason.PlacementFailureNone
                    ? ProtoControl.PlacementFailureReason.PlacementFailureAssignmentRejected
                    : message.FailureReason;

                if (message.Retryable && CanRetryAssignment(trackedAssignment))
                {
                    var backoffMs = message.RetryAfterMs > 0
                        ? (int)Math.Min(int.MaxValue, message.RetryAfterMs)
                        : _options.PlacementAssignmentRetryBackoffMs;
                    var nextAttempt = trackedAssignment.Attempt + 1;
                    ScheduleSelf(
                        context,
                        TimeSpan.FromMilliseconds(Math.Max(0, backoffMs)),
                        new RetryPlacementAssignment(brain.BrainId, brain.PlacementEpoch, trackedAssignment.Assignment.AssignmentId, nextAttempt));
                    HiveMindTelemetry.RecordPlacementAssignmentRetry(
                        brain.BrainId,
                        brain.PlacementEpoch,
                        target,
                        nextAttempt,
                        "ack_retryable",
                        plannedWorkerId);
                    EmitDebug(
                        context,
                        ProtoSeverity.SevInfo,
                        "placement.assignment.retry",
                        $"Placement assignment {trackedAssignment.Assignment.AssignmentId} for brain {brain.BrainId} scheduled retry attempt={nextAttempt} target={target} reason=ack_retryable.");
                    UpdatePlacementLifecycle(
                        brain,
                        ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigning,
                        ProtoControl.PlacementFailureReason.PlacementFailureNone);
                    return;
                }

                trackedAssignment.Failed = true;
                EmitDebug(
                    context,
                    ProtoSeverity.SevWarn,
                    "placement.assignment.failed",
                    $"Placement assignment {trackedAssignment.Assignment.AssignmentId} for brain {brain.BrainId} failed target={target} reason={ToFailureReasonLabel(failureReason)}.");
                FailPlacementExecution(
                    context,
                    brain,
                    failureReason,
                    ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
                    ToSpawnFailureReasonCode(failureReason),
                    BuildSpawnFailureMessage(failureReason, message.Message));
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
                    trackedAssignment.Accepted = true;
                    break;
                case ProtoControl.PlacementAssignmentState.PlacementAssignmentReady:
                    trackedAssignment.Accepted = true;
                    trackedAssignment.Ready = true;
                    trackedAssignment.ReadyMs = NowMs();
                    if (trackedAssignment.LastDispatchMs > 0)
                    {
                        HiveMindTelemetry.RecordPlacementAssignmentReadyLatency(
                            brain.BrainId,
                            brain.PlacementEpoch,
                            target,
                            trackedAssignment.ReadyMs - trackedAssignment.LastDispatchMs,
                            plannedWorkerId);
                    }
                    UpdatePlacementLifecycle(
                        brain,
                        ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigning,
                        ProtoControl.PlacementFailureReason.PlacementFailureNone);
                    MaybeStartReconcile(context, brain);
                    break;
                case ProtoControl.PlacementAssignmentState.PlacementAssignmentDraining:
                    brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileRequiresAction;
                    UpdatePlacementLifecycle(
                        brain,
                        ProtoControl.PlacementLifecycleState.PlacementLifecycleReconciling,
                        ProtoControl.PlacementFailureReason.PlacementFailureNone);
                    break;
            }

            return;
        }

        HandlePlacementAssignmentAckLegacy(context, brain, message);
    }

    private void HandlePlacementAssignmentAckLegacy(IContext context, BrainState brain, ProtoControl.PlacementAssignmentAck message)
    {
        if (!message.Accepted || message.State == ProtoControl.PlacementAssignmentState.PlacementAssignmentFailed)
        {
            var failureReason = message.FailureReason == ProtoControl.PlacementFailureReason.PlacementFailureNone
                ? ProtoControl.PlacementFailureReason.PlacementFailureAssignmentRejected
                : message.FailureReason;
            UpdatePlacementLifecycle(brain, ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed, failureReason);
            brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileFailed;
            TryCompletePendingSpawn(context, brain);
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

        TryCompletePendingSpawn(context, brain);
    }

    private void HandlePlacementReconcileReport(IContext context, ProtoControl.PlacementReconcileReport message)
    {
        var reconcileTarget = ResolveReconcileTargetLabel(message);
        var observedWorkerId = TryResolveObservedWorkerNodeId(message, out var parsedWorkerId)
            ? parsedWorkerId
            : (Guid?)null;

        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.reconcile.ignored",
                $"Ignored reconcile report with invalid brain id; epoch={message.PlacementEpoch}.");
            return;
        }

        if (!_brains.TryGetValue(brainId, out var brain))
        {
            HiveMindTelemetry.RecordPlacementReconcileIgnored(
                brainId,
                message.PlacementEpoch,
                "brain_not_tracked",
                observedWorkerId,
                reconcileTarget);
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.reconcile.ignored",
                $"Ignored reconcile report for brain {brainId}; reason=brain_not_tracked epoch={message.PlacementEpoch}.");
            return;
        }

        var execution = brain.PlacementExecution;

        if (brain.PlacementEpoch == 0 || message.PlacementEpoch != brain.PlacementEpoch)
        {
            HiveMindTelemetry.RecordPlacementReconcileIgnored(
                brain.BrainId,
                message.PlacementEpoch,
                "placement_epoch_mismatch",
                observedWorkerId,
                reconcileTarget);
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.reconcile.ignored",
                $"Ignored reconcile report for brain {brainId}; epoch={message.PlacementEpoch} current={brain.PlacementEpoch}.");
            return;
        }

        if (execution is not null
            && execution.PlacementEpoch == brain.PlacementEpoch
            && execution.Completed)
        {
            HiveMindTelemetry.RecordPlacementReconcileIgnored(
                brain.BrainId,
                message.PlacementEpoch,
                "execution_completed",
                observedWorkerId,
                reconcileTarget);
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.reconcile.ignored",
                $"Ignored reconcile report for brain {brainId}; placement execution already completed for epoch={message.PlacementEpoch}.");
            return;
        }

        if (execution is not null && execution.PlacementEpoch == brain.PlacementEpoch)
        {
            if (!execution.ReconcileRequested)
            {
                HiveMindTelemetry.RecordPlacementReconcileIgnored(
                    brain.BrainId,
                    message.PlacementEpoch,
                    "reconcile_not_requested",
                    observedWorkerId,
                    reconcileTarget);
                EmitDebug(
                    context,
                    ProtoSeverity.SevDebug,
                    "placement.reconcile.ignored",
                    $"Ignored reconcile report for brain {brainId}; reconcile has not started for epoch={message.PlacementEpoch}.");
                return;
            }

            HandleTrackedPlacementReconcileReport(context, brain, message);
            return;
        }

        HandlePlacementReconcileReportLegacy(context, brain, message);
    }

    private void HandlePlacementReconcileReportLegacy(IContext context, BrainState brain, ProtoControl.PlacementReconcileReport message)
    {
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

        TryCompletePendingSpawn(context, brain);
    }

    private void HandleDispatchPlacementPlan(IContext context, DispatchPlacementPlan message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var brain))
        {
            return;
        }

        if (brain.PlacementEpoch == 0
            || brain.PlacementExecution is null
            || brain.PlacementExecution.PlacementEpoch != message.PlacementEpoch
            || brain.PlacementExecution.Completed)
        {
            return;
        }

        if (brain.PlacementExecution.Assignments.Count == 0)
        {
            FailPlacementExecution(
                context,
                brain,
                ProtoControl.PlacementFailureReason.PlacementFailureInternalError,
                ProtoControl.PlacementReconcileState.PlacementReconcileFailed);
            return;
        }

        UpdatePlacementLifecycle(
            brain,
            ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigning,
            ProtoControl.PlacementFailureReason.PlacementFailureNone);

        foreach (var assignment in brain.PlacementExecution.Assignments.Values.OrderBy(static entry => entry.Assignment.AssignmentId, StringComparer.Ordinal))
        {
            DispatchPlacementAssignment(context, brain, assignment, 1);
        }
    }

    private void HandleRetryPlacementAssignment(IContext context, RetryPlacementAssignment message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var brain)
            || brain.PlacementExecution is null
            || brain.PlacementExecution.PlacementEpoch != message.PlacementEpoch
            || brain.PlacementExecution.Completed)
        {
            return;
        }

        if (!brain.PlacementExecution.Assignments.TryGetValue(message.AssignmentId, out var assignment)
            || assignment.Ready
            || assignment.Failed)
        {
            return;
        }

        DispatchPlacementAssignment(context, brain, assignment, message.Attempt);
    }

    private void HandlePlacementAssignmentTimeout(IContext context, PlacementAssignmentTimeout message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var brain)
            || brain.PlacementExecution is null
            || brain.PlacementExecution.PlacementEpoch != message.PlacementEpoch
            || brain.PlacementExecution.Completed)
        {
            return;
        }

        if (!brain.PlacementExecution.Assignments.TryGetValue(message.AssignmentId, out var assignment))
        {
            return;
        }

        if (!assignment.AwaitingAck || assignment.Attempt != message.Attempt || assignment.Ready || assignment.Failed)
        {
            return;
        }

        var target = ToPlacementTargetLabel(assignment.Assignment.Target);
        var assignmentWorkerId = TryGetGuid(assignment.Assignment.WorkerNodeId, out var parsedWorkerId)
            ? parsedWorkerId
            : (Guid?)null;
        assignment.AwaitingAck = false;
        if (CanRetryAssignment(assignment))
        {
            var nextAttempt = assignment.Attempt + 1;
            HiveMindTelemetry.RecordPlacementAssignmentTimeout(
                brain.BrainId,
                brain.PlacementEpoch,
                target,
                assignment.Attempt,
                willRetry: true,
                assignmentWorkerId);
            HiveMindTelemetry.RecordPlacementAssignmentRetry(
                brain.BrainId,
                brain.PlacementEpoch,
                target,
                nextAttempt,
                "timeout",
                assignmentWorkerId);
            EmitDebug(
                context,
                ProtoSeverity.SevWarn,
                "placement.assignment.retry",
                $"Placement assignment {assignment.Assignment.AssignmentId} for brain {brain.BrainId} timed out at attempt={assignment.Attempt}; scheduling retry attempt={nextAttempt}.");
            ScheduleSelf(
                context,
                TimeSpan.FromMilliseconds(Math.Max(0, _options.PlacementAssignmentRetryBackoffMs)),
                new RetryPlacementAssignment(brain.BrainId, brain.PlacementEpoch, assignment.Assignment.AssignmentId, nextAttempt));
            return;
        }

        HiveMindTelemetry.RecordPlacementAssignmentTimeout(
            brain.BrainId,
            brain.PlacementEpoch,
            target,
            assignment.Attempt,
            willRetry: false,
            assignmentWorkerId);
        EmitDebug(
            context,
            ProtoSeverity.SevWarn,
            "placement.assignment.timeout",
            $"Placement assignment {assignment.Assignment.AssignmentId} for brain {brain.BrainId} timed out at attempt={assignment.Attempt} with no retries remaining.");
        FailPlacementExecution(
            context,
            brain,
            ProtoControl.PlacementFailureReason.PlacementFailureAssignmentTimeout,
            ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
            "spawn_assignment_timeout",
            "Spawn failed: placement assignment acknowledgements timed out and retry budget was exhausted.");
    }

    private void HandlePlacementReconcileTimeout(IContext context, PlacementReconcileTimeout message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var brain)
            || brain.PlacementExecution is null
            || brain.PlacementExecution.PlacementEpoch != message.PlacementEpoch
            || !brain.PlacementExecution.ReconcileRequested
            || brain.PlacementExecution.Completed)
        {
            return;
        }

        if (brain.PlacementExecution.PendingReconcileWorkers.Count == 0)
        {
            return;
        }

        HiveMindTelemetry.RecordPlacementReconcileTimeout(
            brain.BrainId,
            brain.PlacementEpoch,
            brain.PlacementExecution.PendingReconcileWorkers.Count);
        HiveMindTelemetry.RecordPlacementReconcileFailed(
            brain.BrainId,
            brain.PlacementEpoch,
            "reconcile_timeout");
        FailPlacementExecution(
            context,
            brain,
            ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch,
            ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
            "spawn_reconcile_timeout",
            "Spawn failed: placement reconcile timed out before all workers reported.");
        EmitDebug(
            context,
            ProtoSeverity.SevWarn,
            "placement.reconcile.timeout",
            $"Placement reconcile timed out for brain {brain.BrainId} epoch={brain.PlacementEpoch} pendingWorkers={brain.PlacementExecution.PendingReconcileWorkers.Count}.");
    }

    private void HandleTrackedPlacementReconcileReport(IContext context, BrainState brain, ProtoControl.PlacementReconcileReport message)
    {
        var execution = brain.PlacementExecution;
        if (execution is null)
        {
            return;
        }

        var reconcileTarget = ResolveReconcileTargetLabel(message);
        if (!TryResolveReconcileWorkerNodeId(context.Sender, execution, message, out var workerId, out var attributionReason))
        {
            var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
            var telemetryWorkerId = workerId != Guid.Empty
                ? workerId
                : TryResolveObservedWorkerNodeId(message, out var observedWorkerId)
                    ? observedWorkerId
                    : (Guid?)null;
            HiveMindTelemetry.RecordPlacementReconcileIgnored(
                brain.BrainId,
                message.PlacementEpoch,
                attributionReason,
                telemetryWorkerId,
                reconcileTarget);
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "placement.reconcile.ignored",
                $"Ignored reconcile report for brain {brain.BrainId}; reason={attributionReason} sender={senderLabel} epoch={message.PlacementEpoch}.");
            if (IsReconcileAttributionMismatchReason(attributionReason))
            {
                EmitDebug(
                    context,
                    ProtoSeverity.SevWarn,
                    "placement.reconcile.response_mismatch",
                    $"Reconcile attribution mismatch for brain {brain.BrainId}; reason={attributionReason} sender={senderLabel} epoch={message.PlacementEpoch}.");
            }
            return;
        }

        execution.PendingReconcileWorkers.Remove(workerId);

        switch (message.ReconcileState)
        {
            case ProtoControl.PlacementReconcileState.PlacementReconcileFailed:
                var reconcileFailureReason = message.FailureReason == ProtoControl.PlacementFailureReason.PlacementFailureNone
                    ? ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch
                    : message.FailureReason;
                HiveMindTelemetry.RecordPlacementReconcileFailed(
                    brain.BrainId,
                    brain.PlacementEpoch,
                    ToFailureReasonLabel(reconcileFailureReason),
                    workerId,
                    reconcileTarget);
                EmitDebug(
                    context,
                    ProtoSeverity.SevWarn,
                    "placement.reconcile.failed",
                    $"Placement reconcile failed for brain {brain.BrainId} epoch={brain.PlacementEpoch} reason={ToFailureReasonLabel(reconcileFailureReason)}.");
                FailPlacementExecution(
                    context,
                    brain,
                    reconcileFailureReason,
                    ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
                    ToSpawnFailureReasonCode(reconcileFailureReason),
                    BuildSpawnFailureMessage(reconcileFailureReason, message.Message));
                return;
            case ProtoControl.PlacementReconcileState.PlacementReconcileRequiresAction:
                execution.RequiresReconcileAction = true;
                break;
            case ProtoControl.PlacementReconcileState.PlacementReconcileMatched:
                break;
        }

        foreach (var observed in message.Assignments)
        {
            if (!string.IsNullOrWhiteSpace(observed.AssignmentId))
            {
                execution.ObservedAssignments[observed.AssignmentId] = observed.Clone();
            }
        }

        if (execution.PendingReconcileWorkers.Count > 0)
        {
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleReconciling,
                ProtoControl.PlacementFailureReason.PlacementFailureNone);
            return;
        }

        if (!TryValidateReconcileMatches(execution, out var mismatch))
        {
            HiveMindTelemetry.RecordPlacementReconcileFailed(
                brain.BrainId,
                brain.PlacementEpoch,
                ToFailureReasonLabel(ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch),
                workerId,
                reconcileTarget);
            FailPlacementExecution(
                context,
                brain,
                ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch,
                ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
                "spawn_reconcile_mismatch",
                $"Spawn failed: placement reconcile mismatch ({mismatch}).");
            EmitDebug(
                context,
                ProtoSeverity.SevWarn,
                "placement.reconcile.mismatch",
                $"Placement reconcile mismatch for brain {brain.BrainId}: {mismatch}");
            return;
        }

        execution.Completed = true;
        brain.PlacementReconcileState = execution.RequiresReconcileAction
            ? ProtoControl.PlacementReconcileState.PlacementReconcileRequiresAction
            : ProtoControl.PlacementReconcileState.PlacementReconcileMatched;

        if (execution.RequiresReconcileAction)
        {
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleReconciling,
                ProtoControl.PlacementFailureReason.PlacementFailureNone);
            return;
        }

        ApplyObservedControlAssignments(context, brain, execution);
        HiveMindTelemetry.RecordPlacementReconcileMatched(
            brain.BrainId,
            brain.PlacementEpoch,
            execution.ObservedAssignments.Count,
            workerId,
            reconcileTarget);
        EmitDebug(
            context,
            ProtoSeverity.SevInfo,
            "placement.reconcile.matched",
            $"Placement reconcile matched for brain {brain.BrainId} epoch={brain.PlacementEpoch} assignments={execution.ObservedAssignments.Count}.");

        UpdatePlacementLifecycle(
            brain,
            brain.Shards.Count > 0
                ? ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning
                : ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned,
            ProtoControl.PlacementFailureReason.PlacementFailureNone);
        TryCompletePendingSpawn(context, brain);
    }

    private void ApplyObservedControlAssignments(IContext context, BrainState brain, PlacementExecutionState execution)
    {
        var updated = false;
        if (TryGetObservedControlPid(execution, ProtoControl.PlacementAssignmentTarget.PlacementTargetBrainRoot, out var observedRoot)
            && !PidEquals(brain.BrainRootPid, observedRoot))
        {
            brain.BrainRootPid = NormalizePid(context, observedRoot) ?? observedRoot;
            updated = true;
        }

        if (TryGetObservedControlPid(execution, ProtoControl.PlacementAssignmentTarget.PlacementTargetSignalRouter, out var observedRouter)
            && !PidEquals(brain.SignalRouterPid, observedRouter))
        {
            brain.SignalRouterPid = NormalizePid(context, observedRouter) ?? observedRouter;
            updated = true;
        }

        if (brain.SignalRouterPid is not null && string.IsNullOrWhiteSpace(brain.SignalRouterPid.Address))
        {
            var fallbackAddress = brain.BrainRootPid?.Address;
            if (!string.IsNullOrWhiteSpace(fallbackAddress))
            {
                brain.SignalRouterPid = new PID(fallbackAddress, brain.SignalRouterPid.Id);
                updated = true;
            }
        }

        if (brain.BrainRootPid is not null && string.IsNullOrWhiteSpace(brain.BrainRootPid.Address))
        {
            var fallbackAddress = brain.SignalRouterPid?.Address;
            if (!string.IsNullOrWhiteSpace(fallbackAddress))
            {
                brain.BrainRootPid = new PID(fallbackAddress, brain.BrainRootPid.Id);
                updated = true;
            }
        }

        if (!updated)
        {
            return;
        }

        UpdateRoutingTable(context, brain);
        ReportBrainRegistration(context, brain);
    }

    private static bool TryGetObservedControlPid(
        PlacementExecutionState execution,
        ProtoControl.PlacementAssignmentTarget target,
        out PID pid)
    {
        foreach (var observed in execution.ObservedAssignments.Values.OrderBy(static value => value.AssignmentId, StringComparer.Ordinal))
        {
            if (observed.Target != target || !TryParsePid(observed.ActorPid, out var observedPid))
            {
                continue;
            }

            pid = observedPid;
            return true;
        }

        pid = new PID();
        return false;
    }

    private static bool PidEquals(PID? left, PID right)
        => left is not null
           && string.Equals(left.Address ?? string.Empty, right.Address ?? string.Empty, StringComparison.OrdinalIgnoreCase)
           && string.Equals(left.Id ?? string.Empty, right.Id ?? string.Empty, StringComparison.Ordinal);

    private static bool SenderMatchesPid(PID? sender, PID expected)
    {
        if (sender is null)
        {
            return false;
        }

        return expected.Equals(sender)
               || PidEquals(sender, expected)
               || PidHasEquivalentEndpoint(sender, expected);
    }

    private static bool PidHasEquivalentEndpoint(PID sender, PID expected)
    {
        if (!string.Equals(sender.Id ?? string.Empty, expected.Id ?? string.Empty, StringComparison.Ordinal))
        {
            return false;
        }

        if (!TryParseEndpoint(sender.Address, out var senderHost, out var senderPort)
            || !TryParseEndpoint(expected.Address, out var expectedHost, out var expectedPort))
        {
            return false;
        }

        if (senderPort != expectedPort)
        {
            return false;
        }

        if (string.Equals(senderHost, expectedHost, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var senderClass = ClassifyEndpointHost(senderHost);
        var expectedClass = ClassifyEndpointHost(expectedHost);
        if (senderClass == EndpointHostClass.Loopback && expectedClass == EndpointHostClass.Loopback)
        {
            return true;
        }

        return (senderClass == EndpointHostClass.Wildcard && expectedClass == EndpointHostClass.Loopback)
               || (senderClass == EndpointHostClass.Loopback && expectedClass == EndpointHostClass.Wildcard);
    }

    private static bool TryParseEndpoint(string? address, out string host, out int port)
    {
        host = string.Empty;
        port = 0;
        if (string.IsNullOrWhiteSpace(address))
        {
            return false;
        }

        var trimmed = address.Trim();
        if (trimmed.Length == 0)
        {
            return false;
        }

        if (trimmed[0] == '[')
        {
            var closingBracket = trimmed.IndexOf(']');
            if (closingBracket <= 1 || closingBracket >= trimmed.Length - 1 || trimmed[closingBracket + 1] != ':')
            {
                return false;
            }

            var bracketHost = trimmed[1..closingBracket];
            var bracketPort = trimmed[(closingBracket + 2)..];
            if (!int.TryParse(bracketPort, NumberStyles.None, CultureInfo.InvariantCulture, out port) || port <= 0)
            {
                return false;
            }

            host = bracketHost;
            return !string.IsNullOrWhiteSpace(host);
        }

        var separator = trimmed.LastIndexOf(':');
        if (separator <= 0 || separator >= trimmed.Length - 1)
        {
            return false;
        }

        var hostToken = trimmed[..separator];
        var portToken = trimmed[(separator + 1)..];
        if (!int.TryParse(portToken, NumberStyles.None, CultureInfo.InvariantCulture, out port) || port <= 0)
        {
            return false;
        }

        host = hostToken;
        return !string.IsNullOrWhiteSpace(host);
    }

    private static EndpointHostClass ClassifyEndpointHost(string host)
    {
        var normalized = host.Trim();
        if (normalized.StartsWith("[", StringComparison.Ordinal) && normalized.EndsWith("]", StringComparison.Ordinal))
        {
            normalized = normalized[1..^1];
        }

        if (normalized.Equals("localhost", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("::1", StringComparison.OrdinalIgnoreCase))
        {
            return EndpointHostClass.Loopback;
        }

        if (normalized.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("::", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("*", StringComparison.Ordinal)
            || normalized.Equals("+", StringComparison.Ordinal))
        {
            return EndpointHostClass.Wildcard;
        }

        return EndpointHostClass.Other;
    }

    private static string NormalizeEndpointAddress(string? address)
    {
        if (string.IsNullOrWhiteSpace(address))
        {
            return string.Empty;
        }

        if (TryParseEndpoint(address, out var host, out var port))
        {
            return $"{host.Trim().ToLowerInvariant()}:{port.ToString(CultureInfo.InvariantCulture)}";
        }

        return address.Trim().ToLowerInvariant();
    }

    private static bool IsLikelyLocalSubscriberPid(ActorSystem system, PID? pid)
    {
        if (pid is null)
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(pid.Address))
        {
            return true;
        }

        if (string.IsNullOrWhiteSpace(system.Address))
        {
            return false;
        }

        if (string.Equals(pid.Address, system.Address, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var probeSender = new PID(pid.Address, "probe");
        var probeExpected = new PID(system.Address, "probe");
        return PidHasEquivalentEndpoint(probeSender, probeExpected);
    }

    private static bool TryLookupProcessInRegistry(ActorSystem system, PID pid, out object? process)
    {
        process = null;
        if (ProcessRegistryProperty is null || ProcessRegistryLookupMethod is null)
        {
            return false;
        }

        try
        {
            var registry = ProcessRegistryProperty.GetValue(system);
            if (registry is null)
            {
                return false;
            }

            process = ProcessRegistryLookupMethod.Invoke(registry, new object?[] { pid });
            return true;
        }
        catch
        {
            process = null;
            return false;
        }
    }

    private static MethodInfo? ResolveProcessRegistryLookupMethod()
    {
        var registryType = ProcessRegistryProperty?.PropertyType;
        if (registryType is null)
        {
            return null;
        }

        return registryType
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .FirstOrDefault(static method =>
            {
                var parameters = method.GetParameters();
                return parameters.Length == 1
                       && parameters[0].ParameterType == typeof(PID)
                       && method.ReturnType != typeof(void)
                       && method.ReturnType != typeof(bool)
                       && !method.ReturnType.IsByRef;
            });
    }

    private static string ToPlacementTargetLabel(ProtoControl.PlacementAssignmentTarget target)
        => target switch
        {
            ProtoControl.PlacementAssignmentTarget.PlacementTargetBrainRoot => "brain_root",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetSignalRouter => "signal_router",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetInputCoordinator => "input_coordinator",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetOutputCoordinator => "output_coordinator",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetRegionShard => "region_shard",
            _ => "unknown"
        };

    private static string ToAssignmentStateLabel(ProtoControl.PlacementAssignmentState state)
        => state switch
        {
            ProtoControl.PlacementAssignmentState.PlacementAssignmentPending => "pending",
            ProtoControl.PlacementAssignmentState.PlacementAssignmentAccepted => "accepted",
            ProtoControl.PlacementAssignmentState.PlacementAssignmentReady => "ready",
            ProtoControl.PlacementAssignmentState.PlacementAssignmentDraining => "draining",
            ProtoControl.PlacementAssignmentState.PlacementAssignmentFailed => "failed",
            _ => "unknown"
        };

    private static string ToFailureReasonLabel(ProtoControl.PlacementFailureReason reason)
        => reason switch
        {
            ProtoControl.PlacementFailureReason.PlacementFailureNone => "none",
            ProtoControl.PlacementFailureReason.PlacementFailureInvalidBrain => "invalid_brain",
            ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable => "worker_unavailable",
            ProtoControl.PlacementFailureReason.PlacementFailureAssignmentRejected => "assignment_rejected",
            ProtoControl.PlacementFailureReason.PlacementFailureAssignmentTimeout => "assignment_timeout",
            ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch => "reconcile_mismatch",
            ProtoControl.PlacementFailureReason.PlacementFailureInternalError => "internal_error",
            _ => "unknown"
        };

    private static string ToSpawnFailureReasonCode(ProtoControl.PlacementFailureReason reason)
        => reason switch
        {
            ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable => "spawn_worker_unavailable",
            ProtoControl.PlacementFailureReason.PlacementFailureAssignmentRejected => "spawn_assignment_rejected",
            ProtoControl.PlacementFailureReason.PlacementFailureAssignmentTimeout => "spawn_assignment_timeout",
            ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch => "spawn_reconcile_mismatch",
            ProtoControl.PlacementFailureReason.PlacementFailureInternalError => "spawn_internal_error",
            ProtoControl.PlacementFailureReason.PlacementFailureInvalidBrain => "spawn_invalid_request",
            _ => "spawn_failed"
        };

    private static string BuildSpawnFailureMessage(
        ProtoControl.PlacementFailureReason reason,
        string? detail,
        string? fallbackReasonCode = null)
    {
        if (!string.IsNullOrWhiteSpace(detail))
        {
            return detail.Trim();
        }

        var reasonCode = string.IsNullOrWhiteSpace(fallbackReasonCode)
            ? ToSpawnFailureReasonCode(reason)
            : fallbackReasonCode.Trim();

        return reasonCode switch
        {
            "spawn_invalid_request" => "Spawn request rejected: invalid brain definition request.",
            "spawn_timeout" => "Spawn timed out while waiting for placement completion.",
            "spawn_worker_unavailable" => "Spawn failed: no eligible worker was available for the placement plan.",
            "spawn_assignment_rejected" => "Spawn failed: a worker rejected one or more placement assignments.",
            "spawn_assignment_timeout" => "Spawn failed: placement assignment acknowledgements timed out and retry budget was exhausted.",
            "spawn_reconcile_timeout" => "Spawn failed: placement reconcile timed out before workers reported final assignments.",
            "spawn_reconcile_mismatch" => "Spawn failed: reconcile results did not match planned assignments.",
            "spawn_internal_error" => "Spawn failed: an internal placement error occurred.",
            _ => "Spawn failed before placement completed."
        };
    }

    private static void SetSpawnFailureDetails(BrainState brain, string reasonCode, string failureMessage)
    {
        var normalizedReasonCode = string.IsNullOrWhiteSpace(reasonCode)
            ? "spawn_failed"
            : reasonCode.Trim();
        var normalizedFailureMessage = string.IsNullOrWhiteSpace(failureMessage)
            ? BuildSpawnFailureMessage(
                ProtoControl.PlacementFailureReason.PlacementFailureNone,
                detail: null,
                fallbackReasonCode: normalizedReasonCode)
            : failureMessage.Trim();
        brain.SpawnFailureReasonCode = normalizedReasonCode;
        brain.SpawnFailureMessage = normalizedFailureMessage;
    }

    private static ProtoControl.SpawnBrainAck BuildSpawnFailureAck(string? reasonCode, string? failureMessage)
    {
        var normalizedReasonCode = string.IsNullOrWhiteSpace(reasonCode)
            ? "spawn_failed"
            : reasonCode.Trim();
        var normalizedFailureMessage = string.IsNullOrWhiteSpace(failureMessage)
            ? BuildSpawnFailureMessage(
                ProtoControl.PlacementFailureReason.PlacementFailureNone,
                detail: null,
                fallbackReasonCode: normalizedReasonCode)
            : failureMessage.Trim();
        return new ProtoControl.SpawnBrainAck
        {
            BrainId = Guid.Empty.ToProtoUuid(),
            FailureReasonCode = normalizedReasonCode,
            FailureMessage = normalizedFailureMessage
        };
    }

    private void DispatchPlacementUnassignments(
        IContext context,
        BrainState brain,
        PlacementExecutionState? execution,
        string reason)
    {
        if (execution is null || execution.Assignments.Count == 0)
        {
            return;
        }

        foreach (var trackedAssignment in execution.Assignments.Values.OrderBy(static entry => entry.Assignment.AssignmentId, StringComparer.Ordinal))
        {
            var assignment = trackedAssignment.Assignment;
            if (!TryGetGuid(assignment.WorkerNodeId, out var workerNodeId)
                || !execution.WorkerTargets.TryGetValue(workerNodeId, out var workerPid))
            {
                continue;
            }

            try
            {
                context.Request(
                    workerPid,
                    new ProtoControl.PlacementUnassignmentRequest
                    {
                        Assignment = assignment.Clone()
                    });
                EmitDebug(
                    context,
                    ProtoSeverity.SevDebug,
                    "placement.unassignment.dispatch",
                    $"Placement unassignment {assignment.AssignmentId} for brain {brain.BrainId} dispatched reason={reason} target={ToPlacementTargetLabel(assignment.Target)}.");
            }
            catch (Exception ex)
            {
                EmitDebug(
                    context,
                    ProtoSeverity.SevWarn,
                    "placement.unassignment.dispatch_failed",
                    $"Placement unassignment {assignment.AssignmentId} for brain {brain.BrainId} dispatch failed reason={reason}: {ex.GetBaseException().Message}");
                LogError($"Failed to dispatch placement unassignment {assignment.AssignmentId} for brain {brain.BrainId}: {ex.Message}");
            }
        }
    }

    private void DispatchPlacementAssignment(
        IContext context,
        BrainState brain,
        PlacementAssignmentExecutionState assignment,
        int attempt)
    {
        if (brain.PlacementExecution is null)
        {
            return;
        }

        var target = ToPlacementTargetLabel(assignment.Assignment.Target);
        var hasWorkerNodeId = TryGetGuid(assignment.Assignment.WorkerNodeId, out var workerNodeId);
        var telemetryWorkerNodeId = hasWorkerNodeId ? workerNodeId : (Guid?)null;
        if (!hasWorkerNodeId
            || !brain.PlacementExecution.WorkerTargets.TryGetValue(workerNodeId, out var workerPid))
        {
            assignment.Failed = true;
            HiveMindTelemetry.RecordPlacementAssignmentDispatchFailed(
                brain.BrainId,
                brain.PlacementEpoch,
                target,
                Math.Max(1, attempt),
                ToFailureReasonLabel(ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable),
                telemetryWorkerNodeId);
            EmitDebug(
                context,
                ProtoSeverity.SevWarn,
                "placement.assignment.dispatch_failed",
                $"Placement assignment {assignment.Assignment.AssignmentId} for brain {brain.BrainId} could not resolve worker target={target}.");
            FailPlacementExecution(
                context,
                brain,
                ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable,
                ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
                "spawn_worker_unavailable",
                "Spawn failed: worker target was unavailable while dispatching placement assignments.");
            return;
        }

        assignment.Attempt = Math.Max(1, attempt);
        assignment.AwaitingAck = true;
        assignment.LastDispatchMs = NowMs();
        HiveMindTelemetry.RecordPlacementAssignmentDispatch(
            brain.BrainId,
            brain.PlacementEpoch,
            target,
            assignment.Attempt,
            telemetryWorkerNodeId);
        EmitDebug(
            context,
            ProtoSeverity.SevDebug,
            "placement.assignment.dispatch",
            $"Placement assignment {assignment.Assignment.AssignmentId} for brain {brain.BrainId} dispatched target={target} attempt={assignment.Attempt}.");

        try
        {
            context.Request(
                workerPid,
                new ProtoControl.PlacementAssignmentRequest
                {
                    Assignment = assignment.Assignment.Clone()
                });
        }
        catch (Exception ex)
        {
            assignment.AwaitingAck = false;
            assignment.Failed = true;
            HiveMindTelemetry.RecordPlacementAssignmentDispatchFailed(
                brain.BrainId,
                brain.PlacementEpoch,
                target,
                assignment.Attempt,
                ToFailureReasonLabel(ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable),
                telemetryWorkerNodeId);
            EmitDebug(
                context,
                ProtoSeverity.SevWarn,
                "placement.assignment.dispatch_failed",
                $"Placement assignment {assignment.Assignment.AssignmentId} for brain {brain.BrainId} dispatch failed target={target}: {ex.GetBaseException().Message}");
            FailPlacementExecution(
                context,
                brain,
                ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable,
                ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
                "spawn_worker_unavailable",
                "Spawn failed: placement assignment dispatch threw while contacting a worker.");
            LogError($"Failed to dispatch placement assignment {assignment.Assignment.AssignmentId} for brain {brain.BrainId}: {ex.Message}");
            return;
        }

        ScheduleSelf(
            context,
            TimeSpan.FromMilliseconds(Math.Max(100, _options.PlacementAssignmentTimeoutMs)),
            new PlacementAssignmentTimeout(
                brain.BrainId,
                brain.PlacementEpoch,
                assignment.Assignment.AssignmentId,
                assignment.Attempt));
    }

    private void MaybeStartReconcile(IContext context, BrainState brain)
    {
        var execution = brain.PlacementExecution;
        if (execution is null || execution.Completed || execution.ReconcileRequested)
        {
            return;
        }

        if (execution.Assignments.Values.Any(static assignment => !assignment.Ready))
        {
            UpdatePlacementLifecycle(
                brain,
                ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigning,
                ProtoControl.PlacementFailureReason.PlacementFailureNone);
            return;
        }

        execution.ReconcileRequested = true;
        execution.PendingReconcileWorkers.Clear();
        execution.ObservedAssignments.Clear();
        execution.RequiresReconcileAction = false;
        foreach (var assignment in execution.Assignments.Values)
        {
            if (TryGetGuid(assignment.Assignment.WorkerNodeId, out var workerNodeId))
            {
                execution.PendingReconcileWorkers.Add(workerNodeId);
            }
        }

        UpdatePlacementLifecycle(
            brain,
            ProtoControl.PlacementLifecycleState.PlacementLifecycleReconciling,
            ProtoControl.PlacementFailureReason.PlacementFailureNone);
        EmitDebug(
            context,
            ProtoSeverity.SevInfo,
            "placement.reconcile.requested",
            $"Placement reconcile requested for brain {brain.BrainId} epoch={brain.PlacementEpoch} workers={execution.PendingReconcileWorkers.Count}.");

        foreach (var workerNodeId in execution.PendingReconcileWorkers.ToArray())
        {
            if (!execution.WorkerTargets.TryGetValue(workerNodeId, out var workerPid))
            {
                continue;
            }

            try
            {
                context.Request(
                    workerPid,
                    new ProtoControl.PlacementReconcileRequest
                    {
                        BrainId = brain.BrainId.ToProtoUuid(),
                        PlacementEpoch = brain.PlacementEpoch
                    });
            }
            catch (Exception ex)
            {
                HiveMindTelemetry.RecordPlacementReconcileFailed(
                    brain.BrainId,
                    brain.PlacementEpoch,
                    ToFailureReasonLabel(ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable),
                    workerNodeId);
                EmitDebug(
                    context,
                    ProtoSeverity.SevWarn,
                    "placement.reconcile.failed",
                    $"Placement reconcile dispatch failed for brain {brain.BrainId} worker={workerNodeId}: {ex.GetBaseException().Message}");
                FailPlacementExecution(
                    context,
                    brain,
                    ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable,
                    ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
                    "spawn_worker_unavailable",
                    "Spawn failed: placement reconcile dispatch could not reach a worker.");
                LogError($"Failed to dispatch reconcile request for brain {brain.BrainId}: {ex.Message}");
                return;
            }
        }

        if (execution.PendingReconcileWorkers.Count == 0)
        {
            execution.Completed = true;
            brain.PlacementReconcileState = ProtoControl.PlacementReconcileState.PlacementReconcileMatched;
            HiveMindTelemetry.RecordPlacementReconcileMatched(
                brain.BrainId,
                brain.PlacementEpoch,
                observedAssignments: 0);
            EmitDebug(
                context,
                ProtoSeverity.SevInfo,
                "placement.reconcile.matched",
                $"Placement reconcile matched for brain {brain.BrainId} epoch={brain.PlacementEpoch} assignments=0.");
            UpdatePlacementLifecycle(
                brain,
                brain.Shards.Count > 0
                    ? ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning
                    : ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned,
                ProtoControl.PlacementFailureReason.PlacementFailureNone);
            TryCompletePendingSpawn(context, brain);
            return;
        }

        ScheduleSelf(
            context,
            TimeSpan.FromMilliseconds(Math.Max(100, _options.PlacementReconcileTimeoutMs)),
            new PlacementReconcileTimeout(brain.BrainId, brain.PlacementEpoch));
    }

    private static bool TryResolveReconcileWorkerNodeId(
        PID? sender,
        PlacementExecutionState execution,
        ProtoControl.PlacementReconcileReport message,
        out Guid workerId,
        out string reason)
    {
        if (sender is null)
        {
            workerId = Guid.Empty;
            reason = "sender_missing";
            return false;
        }

        var senderWorkerId = Guid.Empty;
        var senderWorkerMatches = 0;
        foreach (var target in execution.WorkerTargets)
        {
            if (execution.PendingReconcileWorkers.Contains(target.Key)
                && SenderMatchesPid(sender, target.Value))
            {
                senderWorkerId = target.Key;
                senderWorkerMatches++;
                if (senderWorkerMatches > 1)
                {
                    workerId = Guid.Empty;
                    reason = "sender_worker_ambiguous";
                    return false;
                }
            }
        }

        if (senderWorkerMatches == 0)
        {
            workerId = Guid.Empty;
            reason = "sender_not_pending_worker";
            return false;
        }

        foreach (var observed in message.Assignments)
        {
            if (observed.WorkerNodeId is null)
            {
                continue;
            }

            if (!TryGetGuid(observed.WorkerNodeId, out var observedWorkerId))
            {
                workerId = senderWorkerId;
                reason = "payload_worker_invalid";
                return false;
            }

            if (observedWorkerId != senderWorkerId)
            {
                workerId = senderWorkerId;
                reason = "payload_worker_mismatch";
                return false;
            }
        }

        workerId = senderWorkerId;
        reason = string.Empty;
        return true;
    }

    private static string ResolveReconcileTargetLabel(ProtoControl.PlacementReconcileReport message)
    {
        if (message.Assignments.Count == 0)
        {
            return "reconcile";
        }

        return ToPlacementTargetLabel(message.Assignments[0].Target);
    }

    private static bool TryResolveObservedWorkerNodeId(ProtoControl.PlacementReconcileReport message, out Guid workerNodeId)
    {
        foreach (var observed in message.Assignments)
        {
            if (observed.WorkerNodeId is not null && TryGetGuid(observed.WorkerNodeId, out workerNodeId))
            {
                return true;
            }
        }

        workerNodeId = Guid.Empty;
        return false;
    }

    private static bool IsReconcileAttributionMismatchReason(string reason)
    {
        if (string.IsNullOrWhiteSpace(reason))
        {
            return false;
        }

        return reason.StartsWith("sender_", StringComparison.Ordinal)
               || reason.StartsWith("payload_", StringComparison.Ordinal);
    }

    private static bool TryValidateReconcileMatches(PlacementExecutionState execution, out string mismatch)
    {
        foreach (var assignment in execution.Assignments.Values)
        {
            var assignmentId = assignment.Assignment.AssignmentId;
            if (!execution.ObservedAssignments.TryGetValue(assignmentId, out var observed))
            {
                mismatch = $"missing assignment '{assignmentId}'";
                return false;
            }

            if (observed.Target != assignment.Assignment.Target)
            {
                mismatch = $"target mismatch for '{assignmentId}'";
                return false;
            }

            if (!TryGetGuid(observed.WorkerNodeId, out var observedWorker)
                || !TryGetGuid(assignment.Assignment.WorkerNodeId, out var plannedWorker)
                || observedWorker != plannedWorker)
            {
                mismatch = $"worker mismatch for '{assignmentId}'";
                return false;
            }

            if (observed.RegionId != assignment.Assignment.RegionId || observed.ShardIndex != assignment.Assignment.ShardIndex)
            {
                mismatch = $"shard mismatch for '{assignmentId}'";
                return false;
            }
        }

        mismatch = string.Empty;
        return true;
    }

    private bool TryCreatePlacementExecution(
        IContext context,
        BrainState brain,
        PlacementPlanner.PlacementPlanningResult plan,
        out string failureMessage)
    {
        var workerTargets = new Dictionary<Guid, PID>();
        foreach (var worker in plan.EligibleWorkers)
        {
            if (string.IsNullOrWhiteSpace(worker.WorkerRootActorName))
            {
                failureMessage = $"Worker {worker.NodeId} has no root actor name for placement orchestration.";
                return false;
            }

            var workerPid = string.IsNullOrWhiteSpace(worker.WorkerAddress)
                ? new PID(string.Empty, worker.WorkerRootActorName)
                : new PID(worker.WorkerAddress, worker.WorkerRootActorName);

            workerTargets[worker.NodeId] = NormalizePid(context, workerPid) ?? workerPid;
        }

        var execution = new PlacementExecutionState(brain.PlacementEpoch, workerTargets);
        foreach (var assignment in plan.Assignments)
        {
            if (string.IsNullOrWhiteSpace(assignment.AssignmentId))
            {
                continue;
            }

            execution.Assignments[assignment.AssignmentId] = new PlacementAssignmentExecutionState(assignment.Clone());
        }

        if (execution.Assignments.Count == 0)
        {
            failureMessage = "Placement plan produced no assignments.";
            return false;
        }

        brain.PlacementExecution = execution;
        failureMessage = string.Empty;
        return true;
    }

    private static void UpdateBrainIoWidthsFromPlannedAssignments(
        BrainState brain,
        PlacementPlanner.PlacementPlanningResult plannedPlacement)
    {
        foreach (var assignment in plannedPlacement.Assignments)
        {
            if (assignment.Target != ProtoControl.PlacementAssignmentTarget.PlacementTargetRegionShard
                || assignment.NeuronCount == 0)
            {
                continue;
            }

            var span64 = (long)assignment.NeuronStart + assignment.NeuronCount;
            if (span64 <= 0)
            {
                continue;
            }

            var span = span64 > int.MaxValue ? int.MaxValue : (int)span64;
            if (assignment.RegionId == NbnConstants.InputRegionId && span > brain.InputWidth)
            {
                brain.InputWidth = span;
            }

            if (assignment.RegionId == NbnConstants.OutputRegionId && span > brain.OutputWidth)
            {
                brain.OutputWidth = span;
            }
        }
    }

    private bool CanRetryAssignment(PlacementAssignmentExecutionState assignment)
        => assignment.Attempt <= _options.PlacementAssignmentMaxRetries;

    private void FailPlacementExecution(
        IContext context,
        BrainState brain,
        ProtoControl.PlacementFailureReason failureReason,
        ProtoControl.PlacementReconcileState reconcileState,
        string? spawnFailureReasonCode = null,
        string? spawnFailureMessage = null)
    {
        brain.PlacementReconcileState = reconcileState;
        SetSpawnFailureDetails(
            brain,
            spawnFailureReasonCode ?? ToSpawnFailureReasonCode(failureReason),
            string.IsNullOrWhiteSpace(spawnFailureMessage)
                ? BuildSpawnFailureMessage(failureReason, detail: null)
                : spawnFailureMessage);
        if (brain.PlacementExecution is not null)
        {
            brain.PlacementExecution.Completed = true;
            foreach (var assignment in brain.PlacementExecution.Assignments.Values)
            {
                assignment.AwaitingAck = false;
                if (!assignment.Ready)
                {
                    assignment.Failed = true;
                }
            }
        }

        UpdatePlacementLifecycle(
            brain,
            ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
            failureReason);
        TryCompletePendingSpawn(context, brain);
    }

    private void HandleSpawnCompletionTimeout(IContext context, SpawnCompletionTimeout message)
    {
        if (!_pendingSpawns.TryGetValue(message.BrainId, out var pending)
            || pending.PlacementEpoch != message.PlacementEpoch)
        {
            return;
        }

        _pendingSpawns.Remove(message.BrainId);
        pending.SetFailure(
            reasonCode: "spawn_timeout",
            failureMessage: "Spawn timed out while waiting for placement completion.");
        pending.Completion.TrySetResult(false);
        if (_brains.ContainsKey(message.BrainId))
        {
            UnregisterBrain(context, message.BrainId, reason: "spawn_timeout");
        }
    }

    private void TryCompletePendingSpawn(IContext context, BrainState brain)
    {
        if (!_pendingSpawns.TryGetValue(brain.BrainId, out var pending))
        {
            return;
        }

        if (pending.PlacementEpoch != brain.PlacementEpoch)
        {
            if (pending.PlacementEpoch < brain.PlacementEpoch)
            {
                _pendingSpawns.Remove(brain.BrainId);
                pending.SetFailure(
                    reasonCode: "spawn_failed",
                    failureMessage: "Spawn failed: placement epoch changed before completion.");
                pending.Completion.TrySetResult(false);
            }

            return;
        }

        var execution = brain.PlacementExecution;
        if (execution is not null
            && execution.PlacementEpoch == pending.PlacementEpoch
            && !execution.Completed)
        {
            return;
        }

        if (brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned
            || brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning)
        {
            _pendingSpawns.Remove(brain.BrainId);
            pending.Completion.TrySetResult(true);
            return;
        }

        if (brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed
            || brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleTerminated)
        {
            _pendingSpawns.Remove(brain.BrainId);
            pending.SetFailure(
                reasonCode: string.IsNullOrWhiteSpace(brain.SpawnFailureReasonCode)
                    ? ToSpawnFailureReasonCode(brain.PlacementFailureReason)
                    : brain.SpawnFailureReasonCode,
                failureMessage: string.IsNullOrWhiteSpace(brain.SpawnFailureMessage)
                    ? BuildSpawnFailureMessage(brain.PlacementFailureReason, detail: null)
                    : brain.SpawnFailureMessage);
            pending.Completion.TrySetResult(false);
            UnregisterBrain(context, brain.BrainId, reason: "spawn_failed");
        }
    }

    private int ComputeSpawnCompletionTimeoutMs()
    {
        var attempts = Math.Max(1, _options.PlacementAssignmentMaxRetries + 1);
        var assignmentWindow = (long)Math.Max(100, _options.PlacementAssignmentTimeoutMs) * attempts;
        var retryWindow = (long)Math.Max(0, _options.PlacementAssignmentRetryBackoffMs) * Math.Max(0, attempts - 1);
        var reconcileWindow = Math.Max(100, _options.PlacementReconcileTimeoutMs);
        var timeoutMs = assignmentWindow + retryWindow + reconcileWindow + 250L;
        return (int)Math.Min(int.MaxValue, Math.Max(500L, timeoutMs));
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

        if (!TryResolvePlacementRegions(brain, out var regions, out var shardStride, out var regionWarning, out var regionFailure))
        {
            plan = new PlacementPlanner.PlacementPlanningResult(
                brain.PlacementEpoch,
                brain.PlacementRequestId,
                brain.PlacementRequestedMs,
                nowMs,
                _workerCatalogSnapshotMs > 0 ? (ulong)_workerCatalogSnapshotMs : (ulong)nowMs,
                Array.Empty<PlacementPlanner.WorkerCandidate>(),
                Array.Empty<ProtoControl.PlacementAssignment>(),
                regionFailure is null ? Array.Empty<string>() : new[] { regionFailure });
            failureReason = ProtoControl.PlacementFailureReason.PlacementFailureInternalError;
            failureMessage = regionFailure ?? "Unable to derive placement regions from request metadata.";
            return false;
        }

        var snapshotMs = _workerCatalogSnapshotMs > 0 ? (ulong)_workerCatalogSnapshotMs : (ulong)nowMs;
        var workers = _workerCatalog.Values
            .Where(static entry => IsPlacementWorkerCandidate(entry.LogicalName, entry.WorkerRootActorName))
            .Select(static entry => new PlacementPlanner.WorkerCandidate(
                entry.NodeId,
                entry.WorkerAddress,
                entry.WorkerRootActorName,
                entry.IsAlive,
                entry.IsReady,
                entry.IsFresh,
                entry.CpuCores,
                entry.RamFreeBytes,
                entry.StorageFreeBytes,
                entry.HasGpu,
                entry.VramFreeBytes,
                entry.CpuScore,
                entry.GpuScore))
            .ToArray();

        var plannerInputs = new PlacementPlanner.PlannerInputs(
            brain.BrainId,
            brain.PlacementEpoch,
            brain.PlacementRequestId,
            brain.PlacementRequestedMs,
            nowMs,
            snapshotMs,
            shardStride,
            brain.RequestedShardPlan,
            regions);
        var planned = PlacementPlanner.TryBuildPlan(
            plannerInputs,
            workers,
            out plan,
            out failureReason,
            out failureMessage);
        if (!planned)
        {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(regionWarning))
        {
            var warnings = plan.PlannerWarnings.Concat(new[] { regionWarning }).ToArray();
            plan = new PlacementPlanner.PlacementPlanningResult(
                plan.PlacementEpoch,
                plan.RequestId,
                plan.RequestedMs,
                plan.PlannedMs,
                plan.WorkerSnapshotMs,
                plan.EligibleWorkers,
                plan.Assignments,
                warnings);
        }

        return true;
    }

    private bool TryResolvePlacementRegions(
        BrainState brain,
        out IReadOnlyList<PlacementPlanner.RegionSpan> regions,
        out int shardStride,
        out string? warningMessage,
        out string? failureMessage)
    {
        if (HasArtifactRef(brain.BaseDefinition)
            && TryReadPlacementHeader(brain.BaseDefinition!, out var header))
        {
            var fromHeader = new List<PlacementPlanner.RegionSpan>();
            for (var regionId = 0; regionId < header.Regions.Length; regionId++)
            {
                var neuronSpan = (int)header.Regions[regionId].NeuronSpan;
                if (neuronSpan <= 0)
                {
                    continue;
                }

                fromHeader.Add(new PlacementPlanner.RegionSpan(regionId, neuronSpan));
            }

            if (fromHeader.Count > 0)
            {
                regions = fromHeader;
                shardStride = (int)Math.Max(1u, header.AxonStride);
                warningMessage = null;
                failureMessage = null;
                return true;
            }
        }

        var fallback = new List<PlacementPlanner.RegionSpan>();
        if (brain.InputWidth > 0)
        {
            fallback.Add(new PlacementPlanner.RegionSpan(NbnConstants.InputRegionId, brain.InputWidth));
        }

        if (brain.OutputWidth > 0)
        {
            fallback.Add(new PlacementPlanner.RegionSpan(NbnConstants.OutputRegionId, brain.OutputWidth));
        }

        if (fallback.Count == 0)
        {
            regions = Array.Empty<PlacementPlanner.RegionSpan>();
            shardStride = NbnConstants.DefaultAxonStride;
            warningMessage = null;
            failureMessage = "Placement planning requires either resolvable base definition metadata or non-zero input/output widths.";
            return false;
        }

        regions = fallback;
        shardStride = NbnConstants.DefaultAxonStride;
        warningMessage = "Placement planner used fallback IO-only regions because base definition metadata was unavailable.";
        failureMessage = null;
        return true;
    }

    private static bool TryReadPlacementHeader(Nbn.Proto.ArtifactRef baseDefinition, out NbnHeaderV2 header)
    {
        header = default!;
        if (!baseDefinition.TryToSha256Bytes(out var baseHashBytes))
        {
            return false;
        }

        var storeRoot = ResolveArtifactRoot(baseDefinition.StoreUri);
        var store = new LocalArtifactStore(new ArtifactStoreOptions(storeRoot));

        try
        {
            var stream = store.TryOpenArtifactAsync(new Sha256Hash(baseHashBytes))
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
            if (stream is null)
            {
                return false;
            }

            using (stream)
            {
                var headerBytes = new byte[NbnBinary.NbnHeaderBytes];
                var offset = 0;
                while (offset < headerBytes.Length)
                {
                    var read = stream.Read(headerBytes, offset, headerBytes.Length - offset);
                    if (read <= 0)
                    {
                        return false;
                    }

                    offset += read;
                }

                header = NbnBinary.ReadNbnHeader(headerBytes);
                return true;
            }
        }
        catch
        {
            return false;
        }
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
            context.Request(_settingsPid, new ProtoSettings.NodeListRequest());
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
            entry.LogicalName = worker.LogicalName ?? string.Empty;
            entry.WorkerRootActorName = worker.RootActorName ?? string.Empty;
            entry.IsAlive = worker.IsAlive;
            entry.IsReady = worker.IsReady;
            entry.LastSeenMs = worker.LastSeenMs > 0 ? (long)worker.LastSeenMs : 0;
            entry.CpuCores = hasCapabilities ? capabilities.CpuCores : 0;
            entry.RamFreeBytes = hasCapabilities ? (long)capabilities.RamFreeBytes : 0;
            entry.StorageFreeBytes = hasCapabilities ? (long)capabilities.StorageFreeBytes : 0;
            entry.HasGpu = hasCapabilities && capabilities.HasGpu;
            entry.VramFreeBytes = hasCapabilities ? (long)capabilities.VramFreeBytes : 0;
            entry.CpuScore = hasCapabilities ? capabilities.CpuScore : 0f;
            entry.GpuScore = hasCapabilities ? capabilities.GpuScore : 0f;
            entry.CapabilitySnapshotMs = capabilitySnapshotMs;
            entry.LastUpdatedMs = snapshotMs;
        }

        RefreshWorkerCatalogFreshness(snapshotMs);
    }

    private void HandleNodeListResponse(ProtoSettings.NodeListResponse message)
    {
        _activeSettingsNodeAddresses.Clear();
        foreach (var node in message.Nodes)
        {
            var normalizedAddress = NormalizeEndpointAddress(node.Address);
            if (string.IsNullOrWhiteSpace(normalizedAddress))
            {
                continue;
            }

            _knownSettingsNodeAddresses.Add(normalizedAddress);
            if (node.IsAlive)
            {
                _activeSettingsNodeAddresses.Add(normalizedAddress);
            }
        }
    }

    private void HandleSweepVisualizationSubscribers(IContext context)
    {
        try
        {
            SweepSubscribersBySettingsNodeLiveness(context);
            SweepSubscribersByLocalProcessLiveness(context);
            SyncVisualizationScopeToShards(context);
        }
        finally
        {
            ScheduleSelf(context, VisualizationSubscriberSweepInterval, new SweepVisualizationSubscribers());
        }
    }

    private void SyncVisualizationScopeToShards(IContext context)
    {
        if (_brains.Count == 0)
        {
            return;
        }

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (nowMs < _nextVisualizationShardSyncMs)
        {
            return;
        }

        _nextVisualizationShardSyncMs = nowMs + (long)VisualizationShardSyncInterval.TotalMilliseconds;
        foreach (var brain in _brains.Values)
        {
            if (!brain.VisualizationEnabled || brain.Shards.Count == 0)
            {
                continue;
            }

            foreach (var entry in brain.Shards)
            {
                SendShardVisualizationUpdate(
                    context,
                    brain.BrainId,
                    entry.Key,
                    entry.Value,
                    enabled: true,
                    brain.VisualizationFocusRegionId);
            }
        }
    }

    private void SweepSubscribersBySettingsNodeLiveness(IContext context)
    {
        if (_vizSubscriberLeases.Count == 0 || _knownSettingsNodeAddresses.Count == 0)
        {
            return;
        }

        foreach (var entry in _vizSubscriberLeases.ToArray())
        {
            var pid = entry.Value.Pid;
            if (pid is null || string.IsNullOrWhiteSpace(pid.Address))
            {
                continue;
            }

            var normalizedAddress = NormalizeEndpointAddress(pid.Address);
            if (string.IsNullOrWhiteSpace(normalizedAddress))
            {
                continue;
            }

            if (!_knownSettingsNodeAddresses.Contains(normalizedAddress)
                || _activeSettingsNodeAddresses.Contains(normalizedAddress))
            {
                continue;
            }

            RemoveVisualizationSubscriber(context, entry.Key);
        }
    }

    private void SweepSubscribersByLocalProcessLiveness(IContext context)
    {
        if (_vizSubscriberLeases.Count == 0)
        {
            return;
        }

        foreach (var entry in _vizSubscriberLeases.ToArray())
        {
            var pid = entry.Value.Pid;
            if (!IsLikelyLocalSubscriberPid(context.System, pid))
            {
                continue;
            }

            if (!TryLookupProcessInRegistry(context.System, pid!, out var process))
            {
                continue;
            }

            if (process is null || process.GetType().Name.Contains("DeadLetter", StringComparison.OrdinalIgnoreCase))
            {
                RemoveVisualizationSubscriber(context, entry.Key);
            }
        }
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
                     .Where(static worker =>
                         worker.IsAlive
                         && worker.IsReady
                         && worker.IsFresh
                         && IsPlacementWorkerCandidate(worker.LogicalName, worker.WorkerRootActorName))
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
                CapabilityEpoch = ToProtoMs(entry.CapabilitySnapshotMs),
                StorageFreeBytes = ToProtoBytes(entry.StorageFreeBytes)
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

    private static bool IsPlacementWorkerCandidate(string? logicalName, string? rootActorName)
    {
        if (string.IsNullOrWhiteSpace(logicalName))
        {
            // Legacy/synthetic test snapshots may not set logical names.
            return true;
        }

        var normalizedLogical = logicalName.Trim();
        if (normalizedLogical.StartsWith("nbn.worker", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (string.IsNullOrWhiteSpace(rootActorName))
        {
            return false;
        }

        var normalizedRoot = rootActorName.Trim();
        if (normalizedRoot.StartsWith("worker-node", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // Legacy RegionHost workers can still be considered compute-capable workers.
        return normalizedRoot.Equals("regionhost", StringComparison.OrdinalIgnoreCase)
               || normalizedRoot.Equals("region-host", StringComparison.OrdinalIgnoreCase)
               || normalizedRoot.StartsWith("region-host-", StringComparison.OrdinalIgnoreCase)
               || normalizedRoot.StartsWith("regionhost-", StringComparison.OrdinalIgnoreCase);
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

        if (_phase == TickPhase.Deliver && RemovePendingDeliver(brainId))
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
        EmitTickVisualizationEvents(context, _tick.TickId);
        _phase = TickPhase.Compute;
        ClearPendingCompute();
        ClearPendingDeliver();

        _tick.ComputeStartedUtc = _tick.StartedUtc;

        foreach (var brain in _brains.Values)
        {
            if (!CanDispatchTickToBrain(brain))
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

            foreach (var (shardId, senderPid) in brain.Shards)
            {
                var key = new ShardKey(brain.BrainId, shardId);
                _pendingCompute.Add(key);
                _pendingComputeSenders[key] = senderPid;
            }

            if (LogTickBarrier)
            {
                Log(
                    $"TickCompute dispatch tick={_tick.TickId} brain={brain.BrainId} target={PidLabel(computeTarget)} shards={brain.Shards.Count} pendingCompute={_pendingCompute.Count}");
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

    private void EmitTickVisualizationEvents(IContext context, ulong tickId)
    {
        EmitVizEvent(context, VizEventType.VizTick, tickId: tickId);
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
            EmitTickComputeDoneIgnored(context, message, "invalid_payload");
            return;
        }

        var shardId = message.ShardId.ToShardId32();
        var key = new ShardKey(brainId, shardId);

        if (LogTickBarrier)
        {
            var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
            Log($"TickComputeDone recv tick={message.TickId} brain={brainId} shard={shardId} sender={senderLabel}");
        }

        if (!_pendingComputeSenders.TryGetValue(key, out var expectedSender))
        {
            EmitTickComputeDoneIgnored(context, message, "untracked_payload");
            return;
        }

        var senderMatchesExpected = SenderMatchesPid(context.Sender, expectedSender);
        var senderMatchesTrustedController = !senderMatchesExpected
            && _brains.TryGetValue(brainId, out var brain)
            && IsTrustedControllerSender(context.Sender, brain);
        if (!senderMatchesExpected && !senderMatchesTrustedController)
        {
            EmitTickComputeDoneIgnored(context, message, "sender_mismatch", expectedSender);
            return;
        }

        if (!RemovePendingCompute(key))
        {
            EmitTickComputeDoneIgnored(context, message, "untracked_payload", expectedSender);
            return;
        }

        if (LogVizDiagnostics)
        {
            var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
            Log(
                $"VizDiag TickComputeDone accepted tick={message.TickId} brain={brainId} shard={shardId} sender={senderLabel} expectedSender={PidLabel(expectedSender)} pendingAfter={_pendingCompute.Count}");
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
            EmitTickDeliverDoneIgnored(context, message, "invalid_payload");
            return;
        }

        if (LogTickBarrier)
        {
            var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
            Log($"TickDeliverDone recv tick={message.TickId} brain={brainId} sender={senderLabel}");
        }

        if (!_pendingDeliverSenders.TryGetValue(brainId, out var expectedSender))
        {
            EmitTickDeliverDoneIgnored(context, message, "untracked_payload");
            return;
        }

        var senderMatchesExpected = SenderMatchesPid(context.Sender, expectedSender);
        var senderMatchesTrustedController = !senderMatchesExpected
            && _brains.TryGetValue(brainId, out var brain)
            && IsTrustedControllerSender(context.Sender, brain);
        if (!senderMatchesExpected && !senderMatchesTrustedController)
        {
            EmitTickDeliverDoneIgnored(context, message, "sender_mismatch", expectedSender);
            return;
        }

        if (!RemovePendingDeliver(brainId))
        {
            EmitTickDeliverDoneIgnored(context, message, "untracked_payload", expectedSender);
            return;
        }

        if (LogVizDiagnostics)
        {
            var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
            Log(
                $"VizDiag TickDeliverDone accepted tick={message.TickId} brain={brainId} sender={senderLabel} expectedSender={PidLabel(expectedSender)} pendingAfter={_pendingDeliver.Count}");
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
                    if (LogVizDiagnostics)
                    {
                        LogError($"TickCompute timeout detail: {DescribePendingCompute()}");
                    }
                }
                ClearPendingCompute();
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
                    if (LogVizDiagnostics)
                    {
                        LogError($"TickDeliver timeout detail: {DescribePendingDeliver()}");
                    }
                }
                ClearPendingDeliver();
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
            if (!CanDispatchTickToBrain(brain))
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
            _pendingDeliverSenders[brain.BrainId] = deliverTarget;
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

        var ioPid = ResolveSendTargetPid(context, _ioPid);
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
            context.Send(ioPid, new ApplyTickCost(entry.Key, tickId, cost));
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

    private static bool CanDispatchTickToBrain(BrainState brain)
    {
        // Placement reconciliation metadata can lag behind already-hosted shards.
        // Tick dispatch should follow runnable state (paused + shard availability).
        return !brain.Paused && brain.Shards.Count > 0;
    }

    private void SchedulePhaseTimeout(IContext context, TickPhase phase, ulong tickId, int timeoutMs)
    {
        if (timeoutMs <= 0)
        {
            return;
        }

        ScheduleSelf(context, TimeSpan.FromMilliseconds(timeoutMs), new TickPhaseTimeout(tickId, phase));
    }

    private bool RemovePendingCompute(ShardKey key)
    {
        _pendingComputeSenders.Remove(key);
        return _pendingCompute.Remove(key);
    }

    private bool RemovePendingDeliver(Guid brainId)
    {
        _pendingDeliverSenders.Remove(brainId);
        return _pendingDeliver.Remove(brainId);
    }

    private void ClearPendingCompute()
    {
        _pendingCompute.Clear();
        _pendingComputeSenders.Clear();
    }

    private void ClearPendingDeliver()
    {
        _pendingDeliver.Clear();
        _pendingDeliverSenders.Clear();
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
                RemovePendingCompute(key);
            }

            return;
        }

        foreach (var key in removeKeys)
        {
            if (RemovePendingCompute(key))
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
            ClearPendingCompute();
            MaybeCompleteCompute(context);
        }

        if (_phase == TickPhase.Deliver)
        {
            ClearPendingDeliver();
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

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Trim().ToLowerInvariant() is "1" or "true" or "yes" or "on";
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

    private void EmitTickComputeDoneIgnored(
        IContext context,
        ProtoControl.TickComputeDone message,
        string reason,
        PID? expectedSender = null)
    {
        var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
        var expectedLabel = expectedSender is null ? string.Empty : $" expectedSender={PidLabel(expectedSender)}";
        var brainLabel = message.BrainId is null || !message.BrainId.TryToGuid(out var brainId)
            ? "<invalid>"
            : brainId.ToString("D");
        var shardLabel = message.ShardId is null
            ? "<missing>"
            : message.ShardId.ToShardId32().ToString();
        EmitDebug(
            context,
            ProtoSeverity.SevDebug,
            "tick.compute_done.ignored",
            $"Ignored TickComputeDone. reason={reason} tick={message.TickId} brain={brainLabel} shard={shardLabel} sender={senderLabel}{expectedLabel}.");
        if (LogTickBarrier || LogVizDiagnostics)
        {
            Log($"TickComputeDone ignored reason={reason} tick={message.TickId} brain={brainLabel} shard={shardLabel} sender={senderLabel}{expectedLabel}");
        }
    }

    private void EmitTickDeliverDoneIgnored(
        IContext context,
        ProtoControl.TickDeliverDone message,
        string reason,
        PID? expectedSender = null)
    {
        var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
        var expectedLabel = expectedSender is null ? string.Empty : $" expectedSender={PidLabel(expectedSender)}";
        var brainLabel = message.BrainId is null || !message.BrainId.TryToGuid(out var brainId)
            ? "<invalid>"
            : brainId.ToString("D");
        EmitDebug(
            context,
            ProtoSeverity.SevDebug,
            "tick.deliver_done.ignored",
            $"Ignored TickDeliverDone. reason={reason} tick={message.TickId} brain={brainLabel} sender={senderLabel}{expectedLabel}.");
        if (LogTickBarrier || LogVizDiagnostics)
        {
            Log($"TickDeliverDone ignored reason={reason} tick={message.TickId} brain={brainLabel} sender={senderLabel}{expectedLabel}");
        }
    }

    private string DescribePendingCompute(int maxItems = 10)
    {
        if (_pendingCompute.Count == 0)
        {
            return "none";
        }

        var sb = new StringBuilder();
        var index = 0;
        foreach (var key in _pendingCompute)
        {
            if (index > 0)
            {
                sb.Append("; ");
            }

            var senderLabel = _pendingComputeSenders.TryGetValue(key, out var sender)
                ? PidLabel(sender)
                : "<missing>";
            sb.Append($"brain={key.BrainId:D} shard={key.ShardId} sender={senderLabel}");
            index++;
            if (index >= maxItems)
            {
                break;
            }
        }

        if (_pendingCompute.Count > index)
        {
            sb.Append($"; +{_pendingCompute.Count - index} more");
        }

        return sb.ToString();
    }

    private string DescribePendingDeliver(int maxItems = 10)
    {
        if (_pendingDeliver.Count == 0)
        {
            return "none";
        }

        var sb = new StringBuilder();
        var index = 0;
        foreach (var brainId in _pendingDeliver)
        {
            if (index > 0)
            {
                sb.Append("; ");
            }

            var senderLabel = _pendingDeliverSenders.TryGetValue(brainId, out var sender)
                ? PidLabel(sender)
                : "<missing>";
            sb.Append($"brain={brainId:D} sender={senderLabel}");
            index++;
            if (index >= maxItems)
            {
                break;
            }
        }

        if (_pendingDeliver.Count > index)
        {
            sb.Append($"; +{_pendingDeliver.Count - index} more");
        }

        return sb.ToString();
    }

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

    private static PID ResolveSendTargetPid(IContext context, PID pid)
    {
        if (!string.IsNullOrWhiteSpace(pid.Address))
        {
            return pid;
        }

        var systemAddress = context.System.Address;
        if (string.IsNullOrWhiteSpace(systemAddress))
        {
            return pid;
        }

        return new PID(systemAddress, pid.Id);
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

    private static bool IsSupportedHomeostasisTargetMode(ProtoControl.HomeostasisTargetMode mode)
    {
        return mode == ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero
               || mode == ProtoControl.HomeostasisTargetMode.HomeostasisTargetFixed;
    }

    private static bool IsFiniteInRange(float value, float min, float max)
    {
        return float.IsFinite(value) && value >= min && value <= max;
    }

    private static float ResolvePlasticityDelta(float plasticityRate, float plasticityDelta)
    {
        if (plasticityDelta > 0f)
        {
            return plasticityDelta;
        }

        return plasticityRate > 0f ? plasticityRate : 0f;
    }

    private static bool ResolvePerBrainCostEnergyEnabled(BrainState brain)
        => brain.CostEnergyEnabled;

    private bool ResolveEffectiveCostEnergyEnabled(BrainState brain)
        => _systemCostEnergyEnabled && ResolvePerBrainCostEnergyEnabled(brain);

    private bool ResolveEffectivePlasticityEnabled(BrainState brain)
        => _systemPlasticityEnabled && brain.PlasticityEnabled;

    private bool ResolveSnapshotCostEnergyEnabled(BrainState brain, ProtoIo.RequestSnapshot message)
    {
        var requestedCostEnergyEnabled = message.HasRuntimeState
            ? message.CostEnabled && message.EnergyEnabled
            : ResolvePerBrainCostEnergyEnabled(brain);
        return _systemCostEnergyEnabled && requestedCostEnergyEnabled;
    }

    private bool ResolveSnapshotPlasticityEnabled(BrainState brain, ProtoIo.RequestSnapshot message)
    {
        var requestedPlasticityEnabled = message.HasRuntimeState
            ? message.PlasticityEnabled
            : brain.PlasticityEnabled;
        return _systemPlasticityEnabled && requestedPlasticityEnabled;
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

        var snapshotCostEnergyEnabled = ResolveSnapshotCostEnergyEnabled(brain, message);
        var snapshotPlasticityEnabled = ResolveSnapshotPlasticityEnabled(brain, message);
        var storeRootPath = ResolveArtifactRoot(brain.BaseDefinition!.StoreUri);
        var request = new SnapshotBuildRequest(
            brain.BrainId,
            brain.BaseDefinition!,
            _lastCompletedTickId,
            message.HasRuntimeState ? message.EnergyRemaining : 0L,
            snapshotCostEnergyEnabled,
            snapshotCostEnergyEnabled,
            snapshotPlasticityEnabled,
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
                        var ioPid = ResolveSendTargetPid(context, _ioPid);
                        context.Send(ioPid, new UpdateBrainSnapshot(brainId, snapshot));
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

    private static string ArtifactLabel(Nbn.Proto.ArtifactRef? reference)
    {
        if (!HasArtifactRef(reference))
        {
            return "missing";
        }

        return reference!.TryToSha256Hex(out var sha)
            ? sha[..Math.Min(12, sha.Length)]
            : "present";
    }

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
                brain.VisualizationFocusRegionId);
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

        if (!force && brain.IoRegistered && brain.IoRegisteredInputWidth == inputWidth && brain.IoRegisteredOutputWidth == outputWidth)
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
            HomeostasisEnabled = brain.HomeostasisEnabled,
            HomeostasisTargetMode = brain.HomeostasisTargetMode,
            HomeostasisUpdateMode = brain.HomeostasisUpdateMode,
            HomeostasisBaseProbability = brain.HomeostasisBaseProbability,
            HomeostasisMinStepCodes = brain.HomeostasisMinStepCodes,
            HomeostasisEnergyCouplingEnabled = brain.HomeostasisEnergyCouplingEnabled,
            HomeostasisEnergyTargetScale = brain.HomeostasisEnergyTargetScale,
            HomeostasisEnergyProbabilityScale = brain.HomeostasisEnergyProbabilityScale,
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
        bool plasticityProbabilisticUpdates,
        float plasticityDelta,
        uint plasticityRebaseThreshold,
        float plasticityRebaseThresholdPct,
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
                DebugEnabled = debugEnabled,
                DebugMinSeverity = debugMinSeverity
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
    private sealed record SweepVisualizationSubscribers;
    private sealed record RescheduleNow(string Reason);
    private sealed record RescheduleCompleted(string Reason, bool Success);
    private sealed record DispatchPlacementPlan(Guid BrainId, ulong PlacementEpoch);
    private sealed record RetryPlacementAssignment(Guid BrainId, ulong PlacementEpoch, string AssignmentId, int Attempt);
    private sealed record PlacementAssignmentTimeout(Guid BrainId, ulong PlacementEpoch, string AssignmentId, int Attempt);
    private sealed record PlacementReconcileTimeout(Guid BrainId, ulong PlacementEpoch);
    private sealed record SpawnCompletionTimeout(Guid BrainId, ulong PlacementEpoch);

    private enum TickPhase
    {
        Idle,
        Compute,
        Deliver
    }

    private sealed record VisualizationSubscriber(string Key, PID? Pid);

    private sealed record VisualizationSubscriberPreference(string Key, uint? FocusRegionId, PID? Pid);

    private sealed class VisualizationSubscriberLease
    {
        private PID? _pid;
        private int _refCount;

        public VisualizationSubscriberLease(string key, PID? pid)
        {
            Key = key;
            _pid = pid;
        }

        public string Key { get; }
        public PID? Pid => _pid;

        public void Retain(IContext context, PID? pid)
        {
            _refCount++;
            RefreshPid(context, pid);
        }

        public bool Release(IContext context)
        {
            _refCount = Math.Max(0, _refCount - 1);
            if (_refCount > 0)
            {
                return false;
            }

            Unwatch(context);
            return true;
        }

        public void RefreshPid(IContext context, PID? pid)
        {
            if (pid is null)
            {
                return;
            }

            if (_pid is not null && SenderMatchesPid(_pid, pid))
            {
                return;
            }

            if (_pid is not null)
            {
                context.Unwatch(_pid);
            }

            _pid = pid;
            context.Watch(pid);
        }

        public bool Matches(PID pid)
        {
            if (_pid is null)
            {
                return false;
            }

            if (SenderMatchesPid(pid, _pid) || SenderMatchesPid(_pid, pid))
            {
                return true;
            }

            return string.Equals(_pid.Id ?? string.Empty, pid.Id ?? string.Empty, StringComparison.Ordinal);
        }

        public void Unwatch(IContext context)
        {
            if (_pid is null)
            {
                return;
            }

            context.Unwatch(_pid);
            _pid = null;
        }
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
        public bool CostEnergyEnabled { get; set; }
        public bool PlasticityEnabled { get; set; } = true;
        public float PlasticityRate { get; set; } = DefaultPlasticityRate;
        public bool PlasticityProbabilisticUpdates { get; set; } = true;
        public float PlasticityDelta { get; set; } = DefaultPlasticityDelta;
        public uint PlasticityRebaseThreshold { get; set; }
        public float PlasticityRebaseThresholdPct { get; set; }
        public bool HomeostasisEnabled { get; set; } = true;
        public ProtoControl.HomeostasisTargetMode HomeostasisTargetMode { get; set; } = ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero;
        public ProtoControl.HomeostasisUpdateMode HomeostasisUpdateMode { get; set; } = ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep;
        public float HomeostasisBaseProbability { get; set; } = 0.01f;
        public uint HomeostasisMinStepCodes { get; set; } = 1;
        public bool HomeostasisEnergyCouplingEnabled { get; set; }
        public float HomeostasisEnergyTargetScale { get; set; } = 1f;
        public float HomeostasisEnergyProbabilityScale { get; set; } = 1f;
        public bool VisualizationEnabled { get; set; }
        public uint? VisualizationFocusRegionId { get; set; }
        public Dictionary<string, VisualizationSubscriberPreference> VisualizationSubscribers { get; } = new(StringComparer.Ordinal);
        public bool Paused { get; set; }
        public string? PausedReason { get; set; }
        public long SpawnedMs { get; set; }
        public ulong PlacementEpoch { get; set; }
        public long PlacementRequestedMs { get; set; }
        public long PlacementUpdatedMs { get; set; }
        public string PlacementRequestId { get; set; } = string.Empty;
        public ProtoControl.ShardPlan? RequestedShardPlan { get; set; }
        public PlacementPlanner.PlacementPlanningResult? PlannedPlacement { get; set; }
        public PlacementExecutionState? PlacementExecution { get; set; }
        public ProtoControl.PlacementLifecycleState PlacementLifecycleState { get; set; }
            = ProtoControl.PlacementLifecycleState.PlacementLifecycleUnknown;
        public ProtoControl.PlacementFailureReason PlacementFailureReason { get; set; }
            = ProtoControl.PlacementFailureReason.PlacementFailureNone;
        public string SpawnFailureReasonCode { get; set; } = string.Empty;
        public string SpawnFailureMessage { get; set; } = string.Empty;
        public ProtoControl.PlacementReconcileState PlacementReconcileState { get; set; }
            = ProtoControl.PlacementReconcileState.PlacementReconcileUnknown;
        public Dictionary<ShardId32, PID> Shards { get; } = new();
        public RoutingTableSnapshot RoutingSnapshot { get; set; } = RoutingTableSnapshot.Empty;
    }

    private sealed class PendingSpawnState
    {
        public PendingSpawnState(Guid brainId, ulong placementEpoch)
        {
            BrainId = brainId;
            PlacementEpoch = placementEpoch;
        }

        public Guid BrainId { get; }
        public ulong PlacementEpoch { get; }
        public string FailureReasonCode { get; private set; } = "spawn_failed";
        public string FailureMessage { get; private set; } = "Spawn failed before placement completed.";
        public TaskCompletionSource<bool> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void SetFailure(string reasonCode, string failureMessage)
        {
            FailureReasonCode = string.IsNullOrWhiteSpace(reasonCode) ? "spawn_failed" : reasonCode.Trim();
            FailureMessage = string.IsNullOrWhiteSpace(failureMessage)
                ? "Spawn failed before placement completed."
                : failureMessage.Trim();
        }
    }

    private enum EndpointHostClass
    {
        Other = 0,
        Loopback = 1,
        Wildcard = 2
    }

    private sealed class WorkerCatalogEntry
    {
        public WorkerCatalogEntry(Guid nodeId)
        {
            NodeId = nodeId;
        }

        public Guid NodeId { get; }
        public string LogicalName { get; set; } = string.Empty;
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
        public long StorageFreeBytes { get; set; }
        public bool HasGpu { get; set; }
        public long VramFreeBytes { get; set; }
        public float CpuScore { get; set; }
        public float GpuScore { get; set; }
    }

    private sealed class PlacementExecutionState
    {
        public PlacementExecutionState(ulong placementEpoch, Dictionary<Guid, PID> workerTargets)
        {
            PlacementEpoch = placementEpoch;
            WorkerTargets = workerTargets;
        }

        public ulong PlacementEpoch { get; }
        public Dictionary<Guid, PID> WorkerTargets { get; }
        public Dictionary<string, PlacementAssignmentExecutionState> Assignments { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, ProtoControl.PlacementObservedAssignment> ObservedAssignments { get; } = new(StringComparer.Ordinal);
        public HashSet<Guid> PendingReconcileWorkers { get; } = new();
        public bool ReconcileRequested { get; set; }
        public bool RequiresReconcileAction { get; set; }
        public bool Completed { get; set; }
    }

    private sealed class PlacementAssignmentExecutionState
    {
        public PlacementAssignmentExecutionState(ProtoControl.PlacementAssignment assignment)
        {
            Assignment = assignment;
        }

        public ProtoControl.PlacementAssignment Assignment { get; }
        public int Attempt { get; set; }
        public bool AwaitingAck { get; set; }
        public bool Accepted { get; set; }
        public bool Ready { get; set; }
        public bool Failed { get; set; }
        public long LastDispatchMs { get; set; }
        public long AcceptedMs { get; set; }
        public long ReadyMs { get; set; }
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
        if (!_debugStreamEnabled || severity < _debugMinSeverity)
        {
            return;
        }

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
