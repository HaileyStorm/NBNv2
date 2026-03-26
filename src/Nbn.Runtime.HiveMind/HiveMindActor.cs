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
}
