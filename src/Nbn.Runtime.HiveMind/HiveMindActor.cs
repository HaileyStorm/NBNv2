using System.Globalization;
using System.Net;
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

public sealed partial class HiveMindActor : IActor
{
    private readonly HiveMindOptions _options;
    private readonly BackpressureController _backpressure;
    private readonly PID? _settingsPid;
    private readonly PID? _configuredIoPid;
    private string _configuredWorkerRootActorName = string.Empty;
    private PID? _ioPid;
    private readonly PID? _debugHubPid;
    private readonly PID? _vizHubPid;
    private bool _debugStreamEnabled;
    private bool _systemCostEnergyEnabled;
    private bool _remoteCostEnabled;
    private long _remoteCostPerBatch;
    private long _remoteCostPerContribution;
    private float _costTierAMultiplier = 1f;
    private float _costTierBMultiplier = 1f;
    private float _costTierCMultiplier = 1f;
    private bool _systemPlasticityEnabled = true;
    private ProtoControl.InputCoordinatorMode _inputCoordinatorMode =
        ProtoControl.InputCoordinatorMode.DirtyOnChange;
    private ProtoControl.OutputVectorSource _outputVectorSource =
        ProtoControl.OutputVectorSource.Potential;
    private uint _vizTickMinIntervalMs = 250;
    private uint _vizStreamMinIntervalMs = 250;
    private int _workerCapabilityBenchmarkRefreshSeconds = WorkerCapabilitySettingsKeys.DefaultBenchmarkRefreshSeconds;
    private int _workerPressureRebalanceWindow = WorkerCapabilitySettingsKeys.DefaultPressureRebalanceWindow;
    private double _workerPressureViolationRatio = WorkerCapabilitySettingsKeys.DefaultPressureViolationRatio;
    private float _workerPressureLimitTolerancePercent = (float)WorkerCapabilitySettingsKeys.DefaultPressureLimitTolerancePercent;
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
    private ulong _lastVizTickEmittedTickId;
    private long _nextVisualizationShardSyncMs;
    private long _workerCatalogSnapshotMs;
    private long _workerCatalogSnapshotReceivedLocalMs;
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
    private static readonly TimeSpan PlacementPeerLatencyRefreshInterval = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan PlacementPeerLatencyProbeTimeout = TimeSpan.FromMilliseconds(250);
    private const float DefaultPlasticityRate = 0.001f;
    private const float DefaultPlasticityDelta = DefaultPlasticityRate;
    private const long DefaultPlasticityEnergyCostReferenceTickCost = 100;
    private const float DefaultPlasticityEnergyCostResponseStrength = 1f;
    private const float DefaultPlasticityEnergyCostMinScale = 0.1f;
    private const float DefaultPlasticityEnergyCostMaxScale = 1f;

    private TickState? _tick;
    private TickPhase _phase = TickPhase.Idle;
    private bool _tickLoopEnabled;
    private bool _rescheduleInProgress;
    private bool _rescheduleQueued;
    private bool _queuedRescheduleRetryScheduled;
    private ulong _lastRescheduleTick;
    private DateTime _lastRescheduleAt;
    private string? _queuedRescheduleReason;
    private bool _activeRescheduleAllBrains = true;
    private readonly HashSet<Guid> _activeRescheduleBrains = new();
    private bool _queuedRescheduleAllBrains;
    private readonly HashSet<Guid> _queuedRescheduleBrains = new();
    private ulong _lastCompletedTickId;
    private Task<IReadOnlyList<WorkerPeerLatencyMeasurement>>? _peerLatencyRefreshTask;
    private long _lastPeerLatencyRefreshMs;
    private PendingRescheduleState? _pendingReschedule;

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
        _configuredIoPid = ioPid ?? BuildIoPid(options);
        _ioPid = _configuredIoPid;
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
                        ScheduleSelf(context, TimeSpan.Zero, new RefreshWorkerCapabilitiesTick());
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
                    HandleWorkerInventorySnapshotResponse(context, message);
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
                case RefreshWorkerCapabilitiesTick:
                    RefreshWorkerCapabilities(context);
                    break;
                case RescheduleNow message:
                    BeginReschedule(context, message);
                    break;
                case RescheduleCompleted message:
                    CompleteReschedule(context, message);
                    break;
                case RetryQueuedReschedule:
                    HandleRetryQueuedReschedule(context);
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
        foreach (var key in IoCoordinatorSettingsKeys.AllKeys)
        {
            context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = key });
        }
        foreach (var key in TickSettingsKeys.AllKeys)
        {
            context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = key });
        }
        foreach (var key in VisualizationSettingsKeys.AllKeys)
        {
            context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = key });
        }
        foreach (var key in WorkerCapabilitySettingsKeys.AllKeys)
        {
            context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = key });
        }

        context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = ServiceEndpointSettings.IoGatewayKey });
        context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = ServiceEndpointSettings.WorkerNodeKey });
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

        if (TryApplyIoEndpointSetting(message.Key, message.Value))
        {
            RegisterAllBrainsWithIo(context);
        }

        TryApplyWorkerEndpointSetting(message.Key, message.Value);

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

        if (TryApplyInputCoordinatorModeSetting(message.Key, message.Value))
        {
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplyOutputVectorSourceSetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplyTickRateOverrideSetting(context, message.Key, message.Value))
        {
            // Tick-rate status is reported via HiveMindStatus on next status poll.
        }

        if (TryApplyVisualizationTickMinIntervalSetting(message.Key, message.Value))
        {
            // Tick sampling applies on next tick dispatch automatically.
        }

        if (TryApplyVisualizationStreamMinIntervalSetting(message.Key, message.Value))
        {
            UpdateAllShardVisualizationConfig(context);
        }

        TryApplyWorkerCapabilitySetting(context, message.Key, message.Value);
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

        if (TryApplyIoEndpointSetting(message.Key, message.Value))
        {
            RegisterAllBrainsWithIo(context);
        }

        TryApplyWorkerEndpointSetting(message.Key, message.Value);

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

        if (TryApplyInputCoordinatorModeSetting(message.Key, message.Value))
        {
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplyOutputVectorSourceSetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplyTickRateOverrideSetting(context, message.Key, message.Value))
        {
            // Tick-rate status is reported via HiveMindStatus on next status poll.
        }

        if (TryApplyVisualizationTickMinIntervalSetting(message.Key, message.Value))
        {
            // Tick sampling applies on next tick dispatch automatically.
        }

        if (TryApplyVisualizationStreamMinIntervalSetting(message.Key, message.Value))
        {
            UpdateAllShardVisualizationConfig(context);
        }

        TryApplyWorkerCapabilitySetting(context, message.Key, message.Value);
    }

    private bool TryApplyIoEndpointSetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, ServiceEndpointSettings.IoGatewayKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var nextPid = _configuredIoPid;
        if (ServiceEndpointSettings.TryParseValue(value, out var endpoint))
        {
            nextPid = endpoint.ToPid();
        }

        if (SamePid(_ioPid, nextPid))
        {
            return false;
        }

        _ioPid = nextPid;
        return true;
    }

    private bool TryApplyWorkerEndpointSetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, ServiceEndpointSettings.WorkerNodeKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var nextRootActorName = string.Empty;
        if (ServiceEndpointSettings.TryParseValue(value, out var endpoint))
        {
            nextRootActorName = endpoint.ActorName.Trim();
        }

        if (string.Equals(
                _configuredWorkerRootActorName,
                nextRootActorName,
                StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        _configuredWorkerRootActorName = nextRootActorName;
        return true;
    }

    private static bool SamePid(PID? left, PID? right)
    {
        if (left is null && right is null)
        {
            return true;
        }

        if (left is null || right is null)
        {
            return false;
        }

        return string.Equals(left.Address, right.Address, StringComparison.Ordinal)
               && string.Equals(left.Id, right.Id, StringComparison.Ordinal);
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

    private bool TryApplyInputCoordinatorModeSetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, IoCoordinatorSettingsKeys.InputCoordinatorModeKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var parsed = ParseInputCoordinatorModeSetting(value, _inputCoordinatorMode);
        if (parsed == _inputCoordinatorMode)
        {
            return false;
        }

        _inputCoordinatorMode = parsed;
        return true;
    }

    private bool TryApplyOutputVectorSourceSetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, IoCoordinatorSettingsKeys.OutputVectorSourceKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var parsed = ParseOutputVectorSourceSetting(value, _outputVectorSource);
        if (parsed == _outputVectorSource)
        {
            return false;
        }

        _outputVectorSource = parsed;
        return true;
    }

    private bool TryApplyTickRateOverrideSetting(IContext context, string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, TickSettingsKeys.CadenceHzKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (!TryParseTickRateOverrideSetting(value, out var requestedOverride))
        {
            EmitDebug(
                context,
                ProtoSeverity.SevWarn,
                "tick.override.setting.invalid",
                $"Ignoring invalid tick cadence setting '{TickSettingsKeys.CadenceHzKey}'='{value ?? string.Empty}'.");
            return false;
        }

        if (HasEquivalentTickRateOverride(requestedOverride))
        {
            return false;
        }

        var accepted = _backpressure.TrySetTickRateOverride(requestedOverride, out var summary);
        if (accepted)
        {
            EmitDebug(context, ProtoSeverity.SevInfo, "tick.override.setting", summary);
            return true;
        }

        EmitDebug(context, ProtoSeverity.SevWarn, "tick.override.setting.invalid", summary);
        return false;
    }

    private bool HasEquivalentTickRateOverride(float? requestedOverride)
    {
        if (!requestedOverride.HasValue)
        {
            return !_backpressure.HasTickRateOverride;
        }

        if (!_backpressure.HasTickRateOverride)
        {
            return false;
        }

        return MathF.Abs(requestedOverride.Value - _backpressure.TickRateOverrideHz) <= 1e-3f;
    }

    private bool TryApplyVisualizationTickMinIntervalSetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, VisualizationSettingsKeys.TickMinIntervalMsKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var parsed = ParseVisualizationMinIntervalSetting(value, _vizTickMinIntervalMs);
        if (parsed == _vizTickMinIntervalMs)
        {
            return false;
        }

        _vizTickMinIntervalMs = parsed;
        return true;
    }

    private bool TryApplyVisualizationStreamMinIntervalSetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, VisualizationSettingsKeys.StreamMinIntervalMsKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var parsed = ParseVisualizationMinIntervalSetting(value, _vizStreamMinIntervalMs);
        if (parsed == _vizStreamMinIntervalMs)
        {
            return false;
        }

        _vizStreamMinIntervalMs = parsed;
        return true;
    }

    private bool TryApplyWorkerCapabilitySetting(IContext context, string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        if (string.Equals(key, WorkerCapabilitySettingsKeys.BenchmarkRefreshSecondsKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseWorkerCapabilityRefreshSeconds(value, _workerCapabilityBenchmarkRefreshSeconds);
            if (parsed == _workerCapabilityBenchmarkRefreshSeconds)
            {
                return false;
            }

            _workerCapabilityBenchmarkRefreshSeconds = parsed;
            ScheduleSelf(context, TimeSpan.Zero, new RefreshWorkerCapabilitiesTick());
            return true;
        }

        if (string.Equals(key, WorkerCapabilitySettingsKeys.PressureRebalanceWindowKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseWorkerPressureWindow(value, _workerPressureRebalanceWindow);
            if (parsed == _workerPressureRebalanceWindow)
            {
                return false;
            }

            _workerPressureRebalanceWindow = parsed;
            return true;
        }

        if (string.Equals(key, WorkerCapabilitySettingsKeys.PressureViolationRatioKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseWorkerPressureRatio(value, _workerPressureViolationRatio);
            if (Math.Abs(parsed - _workerPressureViolationRatio) <= 0.0001d)
            {
                return false;
            }

            _workerPressureViolationRatio = parsed;
            return true;
        }

        if (string.Equals(key, WorkerCapabilitySettingsKeys.PressureLimitTolerancePercentKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseWorkerPressureTolerance(value, _workerPressureLimitTolerancePercent);
            if (Math.Abs(parsed - _workerPressureLimitTolerancePercent) <= 0.0001f)
            {
                return false;
            }

            _workerPressureLimitTolerancePercent = parsed;
            return true;
        }

        return false;
    }

    private void UpdateAllShardRuntimeConfig(IContext context)
    {
        foreach (var brain in _brains.Values)
        {
            UpdateShardRuntimeConfig(context, brain);
        }
    }

    private void UpdateAllShardVisualizationConfig(IContext context)
    {
        foreach (var brain in _brains.Values)
        {
            foreach (var entry in brain.Shards)
            {
                SendShardVisualizationUpdate(
                    context,
                    brain.BrainId,
                    entry.Key,
                    entry.Value,
                    brain.VisualizationEnabled,
                    brain.VisualizationFocusRegionId,
                    _vizStreamMinIntervalMs);
            }
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

    private static ProtoControl.InputCoordinatorMode ParseInputCoordinatorModeSetting(
        string? value,
        ProtoControl.InputCoordinatorMode fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        var normalized = value.Trim().ToLowerInvariant().Replace('-', '_');
        return normalized switch
        {
            "0" or "dirty" or "dirty_on_change" => ProtoControl.InputCoordinatorMode.DirtyOnChange,
            "1" or "replay" or "replay_latest_vector" => ProtoControl.InputCoordinatorMode.ReplayLatestVector,
            _ => fallback
        };
    }

    private static ProtoControl.OutputVectorSource ParseOutputVectorSourceSetting(
        string? value,
        ProtoControl.OutputVectorSource fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        var normalized = value.Trim().ToLowerInvariant().Replace('-', '_');
        return normalized switch
        {
            "0" or "potential" => ProtoControl.OutputVectorSource.Potential,
            "1" or "buffer" => ProtoControl.OutputVectorSource.Buffer,
            _ => fallback
        };
    }

    private static bool TryParseTickRateOverrideSetting(string? value, out float? overrideHz)
    {
        overrideHz = null;
        if (string.IsNullOrWhiteSpace(value))
        {
            return true;
        }

        var trimmed = value.Trim();
        var normalized = trimmed.ToLowerInvariant();
        if (normalized is "0" or "off" or "none" or "clear" or "default")
        {
            return true;
        }

        if (normalized.EndsWith("ms", StringComparison.Ordinal))
        {
            var numeric = trimmed[..^2].Trim();
            if (!float.TryParse(numeric, NumberStyles.Float, CultureInfo.InvariantCulture, out var ms)
                || !float.IsFinite(ms)
                || ms <= 0f)
            {
                return false;
            }

            overrideHz = 1000f / ms;
            return float.IsFinite(overrideHz.Value) && overrideHz.Value > 0f;
        }

        if (normalized.EndsWith("hz", StringComparison.Ordinal))
        {
            trimmed = trimmed[..^2].Trim();
        }

        if (!float.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out var hz)
            || !float.IsFinite(hz)
            || hz <= 0f)
        {
            return false;
        }

        overrideHz = hz;
        return true;
    }

    private static uint ParseVisualizationMinIntervalSetting(string? value, uint fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !uint.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return fallback;
        }

        return Math.Min(parsed, 60_000u);
    }

    private static uint ComputeVizStride(float targetTickHz, uint minIntervalMs)
    {
        if (minIntervalMs == 0u || !float.IsFinite(targetTickHz) || targetTickHz <= 0f)
        {
            return 1u;
        }

        var tickMs = 1000f / targetTickHz;
        if (!float.IsFinite(tickMs) || tickMs <= 0f || tickMs >= minIntervalMs)
        {
            return 1u;
        }

        var stride = (uint)Math.Ceiling(minIntervalMs / tickMs);
        return Math.Max(1u, stride);
    }

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

    private static int ParseWorkerCapabilityRefreshSeconds(string? value, int fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !int.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return fallback;
        }

        return Math.Max(0, parsed);
    }

    private static int ParseWorkerPressureWindow(string? value, int fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !int.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return fallback;
        }

        return Math.Max(1, parsed);
    }

    private static double ParseWorkerPressureRatio(string? value, double fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !double.TryParse(value.Trim(), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsed)
            || !double.IsFinite(parsed))
        {
            return fallback;
        }

        return Math.Clamp(parsed, 0d, 1d);
    }

    private static float ParseWorkerPressureTolerance(string? value, float fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !float.TryParse(value.Trim(), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsed)
            || !float.IsFinite(parsed))
        {
            return fallback;
        }

        return Math.Max(0f, parsed);
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

    private void RefreshWorkerCapabilities(IContext context)
    {
        try
        {
            var request = new ProtoControl.WorkerCapabilityRefreshRequest
            {
                RequestedMs = (ulong)NowMs(),
                Reason = "settings_cadence"
            };

            foreach (var worker in _workerCatalog.Values
                         .Where(entry =>
                             entry.IsAlive
                             && IsPlacementWorkerCandidate(entry.LogicalName, entry.WorkerRootActorName)
                             && !string.IsNullOrWhiteSpace(entry.WorkerAddress)
                             && !string.IsNullOrWhiteSpace(entry.WorkerRootActorName))
                         .OrderBy(static entry => entry.WorkerAddress, StringComparer.Ordinal)
                         .ThenBy(static entry => entry.NodeId))
            {
                context.Send(new PID(worker.WorkerAddress, worker.WorkerRootActorName), request);
            }
        }
        catch (Exception ex)
        {
            LogError($"Worker capability refresh request failed: {ex.Message}");
        }
        finally
        {
            ScheduleSelf(context, ResolveWorkerCapabilityRefreshInterval(), new RefreshWorkerCapabilitiesTick());
        }
    }

    private TimeSpan ResolveWorkerCapabilityRefreshInterval()
    {
        if (_workerCapabilityBenchmarkRefreshSeconds > 0)
        {
            return TimeSpan.FromSeconds(_workerCapabilityBenchmarkRefreshSeconds);
        }

        return TimeSpan.FromMilliseconds(Math.Max(1, _options.WorkerInventoryRefreshMs));
    }

    private void HandleWorkerInventorySnapshotResponse(IContext context, ProtoSettings.WorkerInventorySnapshotResponse message)
    {
        var receivedLocalMs = NowMs();
        var snapshotMs = message.SnapshotMs > 0 ? (long)message.SnapshotMs : receivedLocalMs;
        _workerCatalogSnapshotMs = snapshotMs;
        _workerCatalogSnapshotReceivedLocalMs = receivedLocalMs;
        var seenWorkerIds = new HashSet<Guid>();

        foreach (var worker in message.Workers)
        {
            if (!TryGetGuid(worker.NodeId, out var nodeId))
            {
                continue;
            }

            seenWorkerIds.Add(nodeId);
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
            entry.RamTotalBytes = hasCapabilities ? (long)capabilities.RamTotalBytes : 0;
            entry.StorageTotalBytes = hasCapabilities ? (long)capabilities.StorageTotalBytes : 0;
            entry.VramTotalBytes = hasCapabilities ? (long)capabilities.VramTotalBytes : 0;
            entry.CpuLimitPercent = hasCapabilities ? capabilities.CpuLimitPercent : 0u;
            entry.RamLimitPercent = hasCapabilities ? capabilities.RamLimitPercent : 0u;
            entry.StorageLimitPercent = hasCapabilities ? capabilities.StorageLimitPercent : 0u;
            entry.GpuComputeLimitPercent = hasCapabilities ? capabilities.GpuComputeLimitPercent : 0u;
            entry.GpuVramLimitPercent = hasCapabilities ? capabilities.GpuVramLimitPercent : 0u;
            entry.ProcessCpuLoadPercent = hasCapabilities ? capabilities.ProcessCpuLoadPercent : 0f;
            entry.ProcessRamUsedBytes = hasCapabilities ? (long)capabilities.ProcessRamUsedBytes : 0;
            entry.CapabilitySnapshotMs = capabilitySnapshotMs;
            entry.LastUpdatedMs = snapshotMs;
        }

        RefreshWorkerCatalogFreshness(receivedLocalMs);
        DetectWorkerLossRecoveries(context, snapshotMs, seenWorkerIds);
        MaybeRequestWorkerPressureReschedule(context, snapshotMs);
        MaybeRefreshPeerLatency(context, force: false);
    }

    private void DetectWorkerLossRecoveries(IContext context, long snapshotMs, IReadOnlySet<Guid> seenWorkerIds)
    {
        foreach (var brain in _brains.Values.OrderBy(static value => value.BrainId))
        {
            if (!CanTriggerAutomaticRecovery(brain))
            {
                continue;
            }

            foreach (var workerNodeId in GetTrackedWorkerNodeIds(brain))
            {
                if (!TryGetWorkerLossReason(workerNodeId, snapshotMs, seenWorkerIds, out var lossReason))
                {
                    continue;
                }

                RequestBrainRecovery(
                    context,
                    brain.BrainId,
                    trigger: $"worker_loss:{lossReason}",
                    detail: $"Worker {workerNodeId:D} became unavailable ({lossReason}).");
                break;
            }
        }
    }

    private void MaybeRequestWorkerPressureReschedule(IContext context, long snapshotMs)
    {
        var candidateBrainIds = new HashSet<Guid>();
        foreach (var worker in _workerCatalog.Values
                     .Where(entry =>
                         entry.IsAlive
                         && entry.IsReady
                         && entry.IsFresh
                         && IsPlacementWorkerCandidate(entry.LogicalName, entry.WorkerRootActorName)))
        {
            var violating = IsWorkerPressureViolation(worker);
            RecordWorkerPressureSample(worker, violating);

            var sampleCount = worker.PressureSamples.Count;
            var violationRatio = sampleCount == 0
                ? 0d
                : worker.PressureViolationCount / (double)sampleCount;

            if (violationRatio < _workerPressureViolationRatio)
            {
                worker.PressureRebalanceRequested = false;
                continue;
            }

            if (!violating || worker.PressureRebalanceRequested)
            {
                continue;
            }

            worker.PressureRebalanceRequested = true;
            foreach (var brainId in GetTrackedBrainIdsForWorker(worker.NodeId))
            {
                candidateBrainIds.Add(brainId);
            }
        }

        if (candidateBrainIds.Count > 0)
        {
            RequestReschedule(context, $"worker_pressure:{snapshotMs}", candidateBrainIds.ToArray());
        }
    }

    private void RecordWorkerPressureSample(WorkerCatalogEntry worker, bool violating)
    {
        var window = Math.Max(1, _workerPressureRebalanceWindow);
        worker.PressureSamples.Enqueue(violating);
        if (violating)
        {
            worker.PressureViolationCount++;
        }

        while (worker.PressureSamples.Count > window)
        {
            if (worker.PressureSamples.Dequeue())
            {
                worker.PressureViolationCount = Math.Max(0, worker.PressureViolationCount - 1);
            }
        }
    }

    private bool IsWorkerPressureViolation(WorkerCatalogEntry worker)
    {
        var ramTotalBytes = ToUnsignedBytes(worker.RamTotalBytes);
        var storageTotalBytes = ToUnsignedBytes(worker.StorageTotalBytes);
        var vramTotalBytes = ToUnsignedBytes(worker.VramTotalBytes);
        return WorkerCapabilityMath.IsCpuOverLimit(
                   worker.ProcessCpuLoadPercent,
                   worker.CpuLimitPercent,
                   _workerPressureLimitTolerancePercent)
               || WorkerCapabilityMath.IsRamOverLimit(
                   ToUnsignedBytes(worker.ProcessRamUsedBytes),
                   ramTotalBytes,
                   worker.RamLimitPercent,
                   _workerPressureLimitTolerancePercent)
               || WorkerCapabilityMath.IsStorageOverLimit(
                   ToUnsignedBytes(worker.StorageFreeBytes),
                   storageTotalBytes,
                   worker.StorageLimitPercent,
                   _workerPressureLimitTolerancePercent)
               || (worker.HasGpu
                   && WorkerCapabilityMath.IsVramOverLimit(
                       ToUnsignedBytes(worker.VramFreeBytes),
                       vramTotalBytes,
                       worker.GpuVramLimitPercent,
                       _workerPressureLimitTolerancePercent));
    }

    private IReadOnlyList<Guid> GetTrackedBrainIdsForWorker(Guid workerNodeId)
    {
        var trackedBrainIds = new HashSet<Guid>();
        foreach (var brain in _brains.Values)
        {
            if (GetCurrentPlacementWorkerNodeIds(brain).Contains(workerNodeId))
            {
                trackedBrainIds.Add(brain.BrainId);
            }
        }

        return trackedBrainIds.ToArray();
    }

    private bool TryGetWorkerLossReason(
        Guid workerNodeId,
        long snapshotMs,
        IReadOnlySet<Guid> seenWorkerIds,
        out string lossReason)
    {
        lossReason = string.Empty;
        if (!_workerCatalog.TryGetValue(workerNodeId, out var worker))
        {
            lossReason = "worker_missing";
            return true;
        }

        if (!worker.IsAlive)
        {
            lossReason = "worker_offline";
            return true;
        }

        if (!worker.IsReady)
        {
            lossReason = "worker_unready";
            return true;
        }

        if (!worker.IsFresh)
        {
            lossReason = "worker_stale";
            return true;
        }

        if (!IsPlacementWorkerCandidate(worker.LogicalName, worker.WorkerRootActorName))
        {
            lossReason = "worker_not_placeable";
            return true;
        }

        if (snapshotMs > 0 && worker.LastUpdatedMs < snapshotMs && !seenWorkerIds.Contains(workerNodeId))
        {
            lossReason = "worker_missing_from_snapshot";
            return true;
        }

        return false;
    }

    private static IEnumerable<Guid> GetTrackedWorkerNodeIds(BrainState brain)
    {
        if (brain.PlacementExecution is null)
        {
            return Array.Empty<Guid>();
        }

        return brain.PlacementExecution.WorkerTargets.Keys.ToArray();
    }

    private static bool CanTriggerAutomaticRecovery(BrainState brain)
        => !brain.RecoveryInProgress
           && brain.PlacementEpoch > 0
           && brain.Shards.Count > 0
           && (brain.PlacementExecution is null || brain.PlacementExecution.Completed)
           && (brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned
               || brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning);

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
                    brain.VisualizationFocusRegionId,
                    _vizStreamMinIntervalMs);
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
                     .Where(worker =>
                         worker.IsAlive
                         && worker.IsReady
                         && worker.IsFresh
                         && IsPlacementWorkerCandidate(worker.LogicalName, worker.WorkerRootActorName))
                     .Where(IsPlacementInventoryEligibleWorker)
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
                StorageFreeBytes = ToProtoBytes(entry.StorageFreeBytes),
                AveragePeerLatencyMs = entry.AveragePeerLatencyMs,
                PeerLatencySampleCount = (uint)Math.Max(0, entry.PeerLatencySampleCount),
                RamTotalBytes = ToProtoBytes(entry.RamTotalBytes),
                StorageTotalBytes = ToProtoBytes(entry.StorageTotalBytes),
                VramTotalBytes = ToProtoBytes(entry.VramTotalBytes),
                CpuLimitPercent = entry.CpuLimitPercent,
                RamLimitPercent = entry.RamLimitPercent,
                StorageLimitPercent = entry.StorageLimitPercent,
                GpuComputeLimitPercent = entry.GpuComputeLimitPercent,
                GpuVramLimitPercent = entry.GpuVramLimitPercent,
                ProcessCpuLoadPercent = entry.ProcessCpuLoadPercent,
                ProcessRamUsedBytes = ToProtoBytes(entry.ProcessRamUsedBytes)
            });
        }

        return inventory;
    }

    private bool IsPlacementInventoryEligibleWorker(WorkerCatalogEntry worker)
    {
        if (string.IsNullOrWhiteSpace(worker.WorkerRootActorName)
            || IsWorkerPressureViolation(worker))
        {
            return false;
        }

        var effectiveRamFreeBytes = WorkerCapabilityMath.EffectiveRamFreeBytes(
            ToUnsignedBytes(worker.RamFreeBytes),
            ToUnsignedBytes(worker.RamTotalBytes),
            ToUnsignedBytes(worker.ProcessRamUsedBytes),
            worker.RamLimitPercent);
        var effectiveStorageFreeBytes = WorkerCapabilityMath.EffectiveStorageFreeBytes(
            ToUnsignedBytes(worker.StorageFreeBytes),
            ToUnsignedBytes(worker.StorageTotalBytes),
            worker.StorageLimitPercent);
        if (effectiveRamFreeBytes == 0 || effectiveStorageFreeBytes == 0)
        {
            return false;
        }

        var effectiveCpuScore = WorkerCapabilityMath.EffectiveCpuScore(worker.CpuScore, worker.CpuLimitPercent);
        if (effectiveCpuScore > 0f)
        {
            return true;
        }

        if (!worker.HasGpu)
        {
            return false;
        }

        var effectiveGpuScore = WorkerCapabilityMath.EffectiveGpuScore(worker.GpuScore, worker.GpuComputeLimitPercent);
        var effectiveVramFreeBytes = WorkerCapabilityMath.EffectiveVramFreeBytes(
            ToUnsignedBytes(worker.VramFreeBytes),
            ToUnsignedBytes(worker.VramTotalBytes),
            worker.GpuVramLimitPercent);
        return effectiveGpuScore > 0f && effectiveVramFreeBytes > 0;
    }
    private void RefreshWorkerCatalogFreshness(long localNowMs)
    {
        var referenceMs = ResolveWorkerCatalogReferenceTimeMs(localNowMs);
        foreach (var worker in _workerCatalog.Values)
        {
            worker.IsFresh = IsWorkerFresh(worker, referenceMs);
        }
    }

    private long ResolveWorkerCatalogReferenceTimeMs(long fallbackLocalNowMs)
    {
        if (_workerCatalogSnapshotMs <= 0)
        {
            return fallbackLocalNowMs;
        }

        if (_workerCatalogSnapshotReceivedLocalMs <= 0
            || fallbackLocalNowMs <= _workerCatalogSnapshotReceivedLocalMs)
        {
            return _workerCatalogSnapshotMs;
        }

        return _workerCatalogSnapshotMs + (fallbackLocalNowMs - _workerCatalogSnapshotReceivedLocalMs);
    }

    private void MaybeRefreshPeerLatency(IContext context, bool force)
    {
        var task = EnsurePeerLatencyRefreshTask(context, force);
        if (task is null)
        {
            return;
        }

        context.ReenterAfter(
            task,
            completed => ApplyPeerLatencyRefreshResult(task, completed));
    }

    private Task<IReadOnlyList<WorkerPeerLatencyMeasurement>>? EnsurePeerLatencyRefreshTask(IContext context, bool force)
    {
        if (_peerLatencyRefreshTask is not null && !_peerLatencyRefreshTask.IsCompleted)
        {
            return _peerLatencyRefreshTask;
        }

        var nowMs = NowMs();
        if (!force
            && _lastPeerLatencyRefreshMs > 0
            && nowMs - _lastPeerLatencyRefreshMs < (long)PlacementPeerLatencyRefreshInterval.TotalMilliseconds)
        {
            return null;
        }

        var probeTargets = BuildPeerLatencyProbeTargets(nowMs);
        if (probeTargets.Count < 2)
        {
            ClearPeerLatencyMeasurements(nowMs);
            _lastPeerLatencyRefreshMs = nowMs;
            return null;
        }

        _lastPeerLatencyRefreshMs = nowMs;
        _peerLatencyRefreshTask = CollectPeerLatencyMeasurementsAsync(context.System, probeTargets);
        return _peerLatencyRefreshTask;
    }

    private List<PeerLatencyProbeTarget> BuildPeerLatencyProbeTargets(long nowMs)
    {
        RefreshWorkerCatalogFreshness(nowMs);
        return _workerCatalog.Values
            .Where(entry =>
                entry.IsAlive
                && entry.IsReady
                && entry.IsFresh
                && IsPlacementWorkerCandidate(entry.LogicalName, entry.WorkerRootActorName)
                && !string.IsNullOrWhiteSpace(entry.WorkerRootActorName))
            .OrderBy(static entry => entry.WorkerAddress, StringComparer.Ordinal)
            .ThenBy(static entry => entry.NodeId)
            .Select(static entry => new PeerLatencyProbeTarget(
                entry.NodeId,
                entry.WorkerAddress,
                entry.WorkerRootActorName))
            .ToList();
    }

    private void ApplyPeerLatencyRefreshResult(
        Task<IReadOnlyList<WorkerPeerLatencyMeasurement>> refreshTask,
        Task<IReadOnlyList<WorkerPeerLatencyMeasurement>> completed)
    {
        if (ReferenceEquals(_peerLatencyRefreshTask, refreshTask))
        {
            _peerLatencyRefreshTask = null;
        }

        if (!completed.IsCompletedSuccessfully)
        {
            return;
        }

        ApplyPeerLatencyMeasurements(completed.Result, NowMs());
    }

    private void ApplyPeerLatencyMeasurements(IReadOnlyList<WorkerPeerLatencyMeasurement> measurements, long snapshotMs)
    {
        var byWorker = measurements.ToDictionary(static measurement => measurement.WorkerNodeId);
        foreach (var entry in _workerCatalog.Values)
        {
            if (byWorker.TryGetValue(entry.NodeId, out var measurement))
            {
                entry.AveragePeerLatencyMs = measurement.AveragePeerLatencyMs;
                entry.PeerLatencySampleCount = measurement.SampleCount;
                entry.PeerLatencySnapshotMs = snapshotMs;
                continue;
            }

            entry.AveragePeerLatencyMs = 0f;
            entry.PeerLatencySampleCount = 0;
            entry.PeerLatencySnapshotMs = snapshotMs;
        }
    }

    private void ClearPeerLatencyMeasurements(long snapshotMs)
    {
        foreach (var entry in _workerCatalog.Values)
        {
            entry.AveragePeerLatencyMs = 0f;
            entry.PeerLatencySampleCount = 0;
            entry.PeerLatencySnapshotMs = snapshotMs;
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

    private static async Task<IReadOnlyList<WorkerPeerLatencyMeasurement>> CollectPeerLatencyMeasurementsAsync(
        ActorSystem system,
        IReadOnlyList<PeerLatencyProbeTarget> probeTargets)
    {
        if (probeTargets.Count < 2)
        {
            return Array.Empty<WorkerPeerLatencyMeasurement>();
        }

        var measurements = new List<WorkerPeerLatencyMeasurement>(probeTargets.Count);
        foreach (var worker in probeTargets)
        {
            var peerTargets = probeTargets
                .Where(peer => peer.NodeId != worker.NodeId)
                .OrderBy(static peer => peer.WorkerAddress, StringComparer.Ordinal)
                .ThenBy(static peer => peer.NodeId)
                .ToArray();
            if (peerTargets.Length == 0)
            {
                measurements.Add(new WorkerPeerLatencyMeasurement(worker.NodeId, 0f, 0));
                continue;
            }

            var request = new ProtoControl.PlacementPeerLatencyRequest
            {
                TimeoutMs = (uint)Math.Max(50, PlacementPeerLatencyProbeTimeout.TotalMilliseconds)
            };
            foreach (var peer in peerTargets)
            {
                request.Peers.Add(new ProtoControl.PlacementPeerTarget
                {
                    WorkerNodeId = peer.NodeId.ToProtoUuid(),
                    WorkerAddress = peer.WorkerAddress,
                    WorkerRootActorName = peer.WorkerRootActorName
                });
            }

            var target = new PID(worker.WorkerAddress, worker.WorkerRootActorName);
            var timeoutMs = Math.Max(
                250,
                peerTargets.Length * (int)PlacementPeerLatencyProbeTimeout.TotalMilliseconds + 250);
            try
            {
                var response = await system.Root.RequestAsync<ProtoControl.PlacementPeerLatencyResponse>(
                        target,
                        request,
                        TimeSpan.FromMilliseconds(timeoutMs))
                    .ConfigureAwait(false);
                if (response is null || !TryGetGuid(response.WorkerNodeId, out var workerNodeId))
                {
                    measurements.Add(new WorkerPeerLatencyMeasurement(worker.NodeId, 0f, 0));
                    continue;
                }

                measurements.Add(new WorkerPeerLatencyMeasurement(
                    workerNodeId,
                    response.AveragePeerLatencyMs,
                    (int)response.SampleCount));
            }
            catch
            {
                measurements.Add(new WorkerPeerLatencyMeasurement(worker.NodeId, 0f, 0));
            }
        }

        return measurements;
    }

    private bool IsPlacementWorkerCandidate(string? logicalName, string? rootActorName)
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

        if (!string.IsNullOrWhiteSpace(_configuredWorkerRootActorName)
            && !string.IsNullOrWhiteSpace(rootActorName)
            && string.Equals(
                rootActorName.Trim(),
                _configuredWorkerRootActorName,
                StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return MatchesKnownWorkerRootActor(rootActorName);
    }

    private static bool MatchesKnownWorkerRootActor(string? rootActorName)
    {
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
            PersistTickRateOverrideSetting(context);
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

    private void PersistTickRateOverrideSetting(IContext context)
    {
        if (_settingsPid is null)
        {
            return;
        }

        var value = _backpressure.HasTickRateOverride
            ? _backpressure.TickRateOverrideHz.ToString("0.###", CultureInfo.InvariantCulture)
            : string.Empty;

        context.Send(_settingsPid, new ProtoSettings.SettingSet
        {
            Key = TickSettingsKeys.CadenceHzKey,
            Value = value
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
            OutputVectorSource = _outputVectorSource,
            InputCoordinatorPid = brain.InputCoordinatorPid is null ? string.Empty : PidLabel(brain.InputCoordinatorPid),
            OutputCoordinatorPid = outputCoordinatorPid is null ? string.Empty : PidLabel(outputCoordinatorPid),
            IoGatewayOwnsInputCoordinator = ioGatewayOwnsInputCoordinator,
            IoGatewayOwnsOutputCoordinator = ioGatewayOwnsOutputCoordinator
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
            brain.HomeostasisEnabled,
            brain.HomeostasisTargetMode,
            brain.HomeostasisUpdateMode,
            brain.HomeostasisBaseProbability,
            brain.HomeostasisMinStepCodes,
            brain.HomeostasisEnergyCouplingEnabled,
            brain.HomeostasisEnergyTargetScale,
            brain.HomeostasisEnergyProbabilityScale,
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
        var store = new ArtifactStoreResolver(new ArtifactStoreResolverOptions(request.StoreRootPath))
            .Resolve(request.StoreUri);
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
        var store = new ArtifactStoreResolver(new ArtifactStoreResolverOptions(request.StoreRootPath))
            .Resolve(request.StoreUri);
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
            bufferMap: SnapshotBufferQuantization,
            homeostasisConfig: new NbsHomeostasisConfig(
                request.HomeostasisEnabled,
                request.HomeostasisTargetMode,
                request.HomeostasisUpdateMode,
                request.HomeostasisBaseProbability,
                request.HomeostasisMinStepCodes,
                request.HomeostasisEnergyCouplingEnabled,
                request.HomeostasisEnergyTargetScale,
                request.HomeostasisEnergyProbabilityScale));

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

    private static ArtifactStoreResolver CreateArtifactStoreResolver(string? storeUri)
        => new(new ArtifactStoreResolverOptions(ResolveArtifactRoot(storeUri)));

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
                _outputVectorSource,
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
            && brain.IoRegisteredOutputVectorSource == _outputVectorSource
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
            OutputVectorSource = _outputVectorSource,
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
        brain.IoRegisteredOutputVectorSource = _outputVectorSource;
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
    private sealed record RefreshWorkerCapabilitiesTick;
    private sealed record SweepVisualizationSubscribers;
    private sealed record RescheduleNow(string Reason);
    private sealed record RescheduleCompleted(string Reason, bool Success);
    private sealed record RetryQueuedReschedule;
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
        public PID? InputCoordinatorPid { get; set; }
        public PID? OutputCoordinatorPid { get; set; }
        public PID? OutputSinkPid { get; set; }
        public int InputWidth { get; set; }
        public int OutputWidth { get; set; }
        public uint IoRegisteredInputWidth { get; set; }
        public uint IoRegisteredOutputWidth { get; set; }
        public string IoRegisteredInputCoordinatorPid { get; set; } = string.Empty;
        public string IoRegisteredOutputCoordinatorPid { get; set; } = string.Empty;
        public bool IoRegisteredOwnsInputCoordinator { get; set; }
        public bool IoRegisteredOwnsOutputCoordinator { get; set; }
        public ProtoControl.InputCoordinatorMode IoRegisteredInputCoordinatorMode { get; set; } =
            ProtoControl.InputCoordinatorMode.DirtyOnChange;
        public ProtoControl.OutputVectorSource IoRegisteredOutputVectorSource { get; set; } =
            ProtoControl.OutputVectorSource.Potential;
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
        public bool PlasticityEnergyCostModulationEnabled { get; set; }
        public long PlasticityEnergyCostReferenceTickCost { get; set; } = DefaultPlasticityEnergyCostReferenceTickCost;
        public float PlasticityEnergyCostResponseStrength { get; set; } = DefaultPlasticityEnergyCostResponseStrength;
        public float PlasticityEnergyCostMinScale { get; set; } = DefaultPlasticityEnergyCostMinScale;
        public float PlasticityEnergyCostMaxScale { get; set; } = DefaultPlasticityEnergyCostMaxScale;
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
        public bool RecoveryInProgress { get; set; }
        public string RecoveryReason { get; set; } = string.Empty;
        public ulong RecoveryPlacementEpoch { get; set; }
        public long RecoveryStartedMs { get; set; }
        public long SpawnedMs { get; set; }
        public int PausePriority { get; set; }
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
        public Dictionary<ShardId32, ulong> ShardRegistrationEpochs { get; } = new();
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
        public long RamTotalBytes { get; set; }
        public long StorageTotalBytes { get; set; }
        public long VramTotalBytes { get; set; }
        public uint CpuLimitPercent { get; set; }
        public uint RamLimitPercent { get; set; }
        public uint StorageLimitPercent { get; set; }
        public uint GpuComputeLimitPercent { get; set; }
        public uint GpuVramLimitPercent { get; set; }
        public float ProcessCpuLoadPercent { get; set; }
        public long ProcessRamUsedBytes { get; set; }
        public float AveragePeerLatencyMs { get; set; }
        public int PeerLatencySampleCount { get; set; }
        public long PeerLatencySnapshotMs { get; set; }
        public Queue<bool> PressureSamples { get; } = new();
        public int PressureViolationCount { get; set; }
        public bool PressureRebalanceRequested { get; set; }
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

    private sealed record PlacementStateSnapshot(
        ulong PlacementEpoch,
        long PlacementRequestedMs,
        long PlacementUpdatedMs,
        string PlacementRequestId,
        ProtoControl.ShardPlan? RequestedShardPlan,
        PlacementPlanner.PlacementPlanningResult? PlannedPlacement,
        PlacementExecutionState? PlacementExecution,
        ProtoControl.PlacementLifecycleState PlacementLifecycleState,
        ProtoControl.PlacementFailureReason PlacementFailureReason,
        ProtoControl.PlacementReconcileState PlacementReconcileState,
        string SpawnFailureReasonCode,
        string SpawnFailureMessage,
        bool HomeostasisEnabled,
        ProtoControl.HomeostasisTargetMode HomeostasisTargetMode,
        ProtoControl.HomeostasisUpdateMode HomeostasisUpdateMode,
        float HomeostasisBaseProbability,
        uint HomeostasisMinStepCodes,
        bool HomeostasisEnergyCouplingEnabled,
        float HomeostasisEnergyTargetScale,
        float HomeostasisEnergyProbabilityScale);

    private readonly record struct PeerLatencyProbeTarget(
        Guid NodeId,
        string WorkerAddress,
        string WorkerRootActorName);

    private readonly record struct WorkerPeerLatencyMeasurement(
        Guid WorkerNodeId,
        float AveragePeerLatencyMs,
        int SampleCount);

    private sealed record RescheduleBrainCandidate(
        Guid BrainId,
        ulong CurrentPlacementEpoch,
        Nbn.Proto.ArtifactRef BaseDefinition,
        Nbn.Proto.ArtifactRef? LastSnapshot,
        int InputWidth,
        int OutputWidth,
        ProtoControl.ShardPlan? ShardPlan,
        bool CostEnergyEnabled,
        bool PlasticityEnabled,
        bool HomeostasisEnabled,
        ProtoControl.HomeostasisTargetMode HomeostasisTargetMode,
        ProtoControl.HomeostasisUpdateMode HomeostasisUpdateMode,
        float HomeostasisBaseProbability,
        uint HomeostasisMinStepCodes,
        bool HomeostasisEnergyCouplingEnabled,
        float HomeostasisEnergyTargetScale,
        float HomeostasisEnergyProbabilityScale,
        ulong CurrentTickId,
        Dictionary<ShardId32, PID> Shards,
        bool IsRecovery);

    private sealed record RescheduleCandidateBuildResult(
        IReadOnlyList<RescheduleBrainCandidate> Candidates,
        IReadOnlyDictionary<Guid, string> Failures);

    private sealed record ReschedulePlacementRequest(
        Guid BrainId,
        ulong PreviousPlacementEpoch,
        ProtoControl.RequestPlacement Request);

    private sealed record ReschedulePreparationResult(
        IReadOnlyList<ReschedulePlacementRequest> Requests,
        IReadOnlyDictionary<Guid, string> Failures);

    private sealed class PendingRescheduleState
    {
        public PendingRescheduleState(string reason)
        {
            Reason = string.IsNullOrWhiteSpace(reason) ? "reschedule" : reason;
        }

        public string Reason { get; }
        public Dictionary<Guid, ulong> PendingBrains { get; } = new();
        public Dictionary<Guid, string> Failures { get; } = new();
    }

    private sealed record SnapshotBuildRequest(
        Guid BrainId,
        Nbn.Proto.ArtifactRef BaseDefinition,
        ulong SnapshotTickId,
        long EnergyRemaining,
        bool CostEnabled,
        bool EnergyEnabled,
        bool PlasticityEnabled,
        bool HomeostasisEnabled,
        ProtoControl.HomeostasisTargetMode HomeostasisTargetMode,
        ProtoControl.HomeostasisUpdateMode HomeostasisUpdateMode,
        float HomeostasisBaseProbability,
        uint HomeostasisMinStepCodes,
        bool HomeostasisEnergyCouplingEnabled,
        float HomeostasisEnergyTargetScale,
        float HomeostasisEnergyProbabilityScale,
        Dictionary<ShardId32, PID> Shards,
        string StoreRootPath,
        string StoreUri);

    private readonly record struct BackpressurePauseCandidate(
        Guid BrainId,
        long SpawnedMs,
        int PausePriority,
        long EnergyRemaining);

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
            State = brain.RecoveryInProgress
                ? "Recovering"
                : brain.Paused
                    ? "Paused"
                    : "Active",
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

    private static ulong ToUnsignedBytes(long value)
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
