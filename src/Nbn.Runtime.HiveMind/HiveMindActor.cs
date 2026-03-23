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
    private readonly IReadOnlyList<ServiceEndpointCandidate>? _localEndpointCandidates;
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
    private readonly Dictionary<Guid, ServiceEndpointSet> _nodeEndpointSets = new();
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
        ProtoSeverity? debugMinSeverity = null,
        IReadOnlyList<ServiceEndpointCandidate>? localEndpointCandidates = null)
    {
        _options = options;
        _localEndpointCandidates = localEndpointCandidates;
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
                    return HandleRegisterBrainAsync(context, message);
                case ProtoControl.UpdateBrainSignalRouter message:
                    return HandleUpdateBrainSignalRouterAsync(context, message);
                case ProtoControl.UnregisterBrain message:
                    HandleUnregisterBrain(context, message);
                    break;
                case ProtoControl.RegisterShard message:
                    return HandleRegisterShardAsync(context, message);
                case ProtoControl.UnregisterShard message:
                    HandleUnregisterShard(context, message);
                    break;
                case ProtoControl.RegisterOutputSink message:
                    return HandleRegisterOutputSinkAsync(context, message);
                case ProtoControl.SetBrainVisualization message:
                    return HandleSetBrainVisualizationAsync(context, message);
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
                    return HandleRequestPlacementAsync(context, message);
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
                    return HandlePlacementReconcileReportAsync(context, message);
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
                    HandleNodeListResponse(context, message);
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
                    return RefreshWorkerCapabilitiesAsync(context);
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
        public string BrainRootActorReference { get; set; } = string.Empty;
        public PID? SignalRouterPid { get; set; }
        public string SignalRouterActorReference { get; set; } = string.Empty;
        public PID? InputCoordinatorPid { get; set; }
        public string InputCoordinatorActorReference { get; set; } = string.Empty;
        public PID? OutputCoordinatorPid { get; set; }
        public string OutputCoordinatorActorReference { get; set; } = string.Empty;
        public PID? OutputSinkPid { get; set; }
        public string OutputSinkActorReference { get; set; } = string.Empty;
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
        public Dictionary<ShardId32, string> ShardActorReferences { get; } = new();
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
        public string WorkerActorReference { get; set; } = string.Empty;
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
        string WorkerRootActorName,
        string WorkerActorReference);

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
