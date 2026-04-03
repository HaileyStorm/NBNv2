using Nbn.Runtime.Brain;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;
using ProtoIo = Nbn.Proto.Io;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
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

    private sealed class PendingRuntimeResetState
    {
        public PendingRuntimeResetState(Guid brainId, bool resetBuffer, bool resetAccumulator)
        {
            BrainId = brainId;
            ResetBuffer = resetBuffer;
            ResetAccumulator = resetAccumulator;
        }

        public Guid BrainId { get; }
        public bool ResetBuffer { get; }
        public bool ResetAccumulator { get; }
        public TaskCompletionSource<ProtoIo.IoCommandAck> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class BrainState
    {
        public BrainState(Guid brainId, ProtoControl.OutputVectorSource outputVectorSource)
        {
            BrainId = brainId;
            OutputVectorSource = NormalizeOutputVectorSource(outputVectorSource);
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
        public ProtoControl.OutputVectorSource OutputVectorSource { get; set; } =
            ProtoControl.OutputVectorSource.Potential;
        public bool HasExplicitOutputVectorSource { get; set; }
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
        public PendingRuntimeResetState? PendingRuntimeReset { get; set; }
    }

    private sealed class PendingSpawnState
    {
        public PendingSpawnState(Guid brainId, ulong placementEpoch, int defaultWaitTimeoutMs)
        {
            BrainId = brainId;
            PlacementEpoch = placementEpoch;
            DefaultWaitTimeoutMs = Math.Max(50, defaultWaitTimeoutMs);
            _progressVersion = 1;
        }

        public Guid BrainId { get; }
        public ulong PlacementEpoch { get; }
        public int DefaultWaitTimeoutMs { get; }
        public string FailureReasonCode { get; private set; } = "spawn_failed";
        public string FailureMessage { get; private set; } = "Spawn failed before placement completed.";
        public TaskCompletionSource<bool> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int ProgressVersion => Volatile.Read(ref _progressVersion);

        private int _progressVersion;

        public void SetFailure(string reasonCode, string failureMessage)
        {
            FailureReasonCode = string.IsNullOrWhiteSpace(reasonCode) ? "spawn_failed" : reasonCode.Trim();
            FailureMessage = string.IsNullOrWhiteSpace(failureMessage)
                ? "Spawn failed before placement completed."
                : failureMessage.Trim();
        }

        public void NoteProgress()
            => Interlocked.Increment(ref _progressVersion);
    }

    private readonly record struct PendingSpawnAwaitResult(bool Completed, bool TimedOut);

    private readonly record struct CompletedSpawnState(
        Guid BrainId,
        ulong PlacementEpoch,
        bool AcceptedForPlacement,
        bool PlacementReady,
        ProtoControl.PlacementLifecycleState LifecycleState,
        ProtoControl.PlacementReconcileState ReconcileState,
        string FailureReasonCode,
        string FailureMessage);

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

    private sealed class WorkerPlacementDispatchState
    {
        public Queue<QueuedPlacementDispatchBatch> Pending { get; } = new();
        public ActiveWorkerPlacementDispatch? Active { get; set; }
    }

    private sealed class ActiveWorkerPlacementDispatch
    {
        public ActiveWorkerPlacementDispatch(Guid brainId, ulong placementEpoch, Guid workerNodeId, IEnumerable<QueuedPlacementDispatchAttempt> assignments)
        {
            BrainId = brainId;
            PlacementEpoch = placementEpoch;
            WorkerNodeId = workerNodeId;
            PendingAssignments = new Queue<QueuedPlacementDispatchAttempt>(assignments);
        }

        public Guid BrainId { get; }
        public ulong PlacementEpoch { get; }
        public Guid WorkerNodeId { get; }
        public Queue<QueuedPlacementDispatchAttempt> PendingAssignments { get; }
        public HashSet<string> InFlightAssignmentIds { get; } = new(StringComparer.Ordinal);
        public int RemainingAssignmentCount => PendingAssignments.Count + InFlightAssignmentIds.Count;
    }

    private readonly record struct QueuedPlacementDispatchBatch(
        Guid BrainId,
        ulong PlacementEpoch,
        Guid WorkerNodeId,
        IReadOnlyList<QueuedPlacementDispatchAttempt> Assignments);

    private readonly record struct QueuedPlacementDispatchAttempt(
        string AssignmentId,
        int Attempt);

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
}
