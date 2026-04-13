using System.Reflection;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.HiveMind;
using Nbn.Shared.Quantization;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

/// <summary>
/// Coordinates global tick progression, placement, worker inventory, and runtime control surfaces for hosted brains.
/// </summary>
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
    private int _workerRegionShardGpuNeuronThreshold = WorkerCapabilitySettingsKeys.DefaultRegionShardGpuNeuronThreshold;
    private ProtoSeverity _debugMinSeverity;
    private bool _debugSettingsSubscribed;
    private readonly Dictionary<Guid, BrainState> _brains = new();
    private readonly Dictionary<Guid, PendingSpawnState> _pendingSpawns = new();
    private readonly Dictionary<Guid, CompletedSpawnState> _completedSpawns = new();
    private readonly Queue<Guid> _completedSpawnOrder = new();
    private readonly Dictionary<Guid, WorkerPlacementDispatchState> _workerPlacementDispatches = new();
    private readonly Dictionary<Guid, WorkerCatalogEntry> _workerCatalog = new();
    private readonly HashSet<ShardKey> _pendingCompute = new();
    private readonly Dictionary<ShardKey, PID> _pendingComputeSenders = new();
    private readonly HashSet<Guid> _pendingDeliver = new();
    private readonly Dictionary<Guid, PID> _pendingDeliverSenders = new();
    private readonly HashSet<Guid> _pendingBarrierResets = new();
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
    private static readonly TimeSpan RuntimeConfigSyncTimeout = TimeSpan.FromSeconds(5);
    private static readonly QuantizationMap SnapshotBufferQuantization = QuantizationSchemas.DefaultBuffer;
    private static readonly TimeSpan PlacementPeerLatencyRefreshInterval = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan PlacementPeerLatencyProbeTimeout = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan RuntimeResetBarrierTimeout = TimeSpan.FromSeconds(15);
    private const int MaxCompletedSpawnResults = 1024;
    private const int MaxWorkerCatalogEntries = 4096;
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

    /// <summary>
    /// Initializes a HiveMind actor with the supplied runtime dependencies and optional observability overrides.
    /// </summary>
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

    /// <inheritdoc />
    public Task ReceiveAsync(IContext context)
    {
        _ = HandleActorLifecycleMessage(context)
            || HandleControlPlaneMessage(context)
            || HandleArtifactMessage(context)
            || HandlePlacementMessage(context)
            || HandleSettingsMessage(context)
            || HandleWorkerInventoryMessage(context)
            || HandleTickAndRescheduleMessage(context)
            || HandleRuntimeSurfaceMessage(context);

        return Task.CompletedTask;
    }

    private bool HandleActorLifecycleMessage(IContext context)
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

                return true;
            case StartTickLoop:
                _tickLoopEnabled = true;
                if (_phase == TickPhase.Idle && !_rescheduleInProgress)
                {
                    ScheduleNextTick(context, TimeSpan.Zero);
                }

                return true;
            case StopTickLoop:
                _tickLoopEnabled = false;
                return true;
            case TickStart:
                if (_tickLoopEnabled && !_rescheduleInProgress && _phase == TickPhase.Idle && _pendingBarrierResets.Count == 0)
                {
                    StartTick(context);
                }

                return true;
            default:
                return false;
        }
    }
}
