using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Quantization;
using Nbn.Shared.Sharding;
using ProtoControl = Nbn.Proto.Control;
using SharedShardPlanMode = Nbn.Shared.Sharding.ShardPlanMode;

namespace Nbn.Runtime.HiveMind;

/// <summary>
/// Builds deterministic placement assignments from a worker snapshot without mutating HiveMind runtime state.
/// </summary>
public static class PlacementPlanner
{
    private const int NeuronLocalityThresholdPerWorker = 8192;

    /// <summary>
    /// Describes one worker snapshot considered during placement planning.
    /// </summary>
    public readonly record struct WorkerCandidate(
        Guid NodeId,
        string WorkerAddress,
        string WorkerRootActorName,
        bool IsAlive,
        bool IsReady,
        bool IsFresh,
        uint CpuCores,
        long RamFreeBytes,
        long RamTotalBytes,
        long StorageFreeBytes,
        long StorageTotalBytes,
        bool HasGpu,
        long VramFreeBytes,
        long VramTotalBytes,
        float CpuScore,
        float GpuScore,
        uint CpuLimitPercent,
        uint RamLimitPercent,
        uint StorageLimitPercent,
        uint GpuComputeLimitPercent,
        uint GpuVramLimitPercent,
        float ProcessCpuLoadPercent,
        long ProcessRamUsedBytes,
        float PressureLimitTolerancePercent,
        float AveragePeerLatencyMs,
        uint PeerLatencySampleCount,
        int HostedBrainCount);

    /// <summary>
    /// Describes the neuron span requested for a single region.
    /// </summary>
    public readonly record struct RegionSpan(int RegionId, int NeuronSpan);

    /// <summary>
    /// Captures the immutable inputs needed to build a placement plan for one brain request.
    /// </summary>
    public readonly record struct PlannerInputs(
        Guid BrainId,
        ulong PlacementEpoch,
        string RequestId,
        long RequestedMs,
        long PlannedMs,
        ulong WorkerSnapshotMs,
        int ShardStride,
        ProtoControl.ShardPlan? RequestedShardPlan,
        IReadOnlyList<RegionSpan> Regions,
        IReadOnlyList<Guid> CurrentWorkerNodeIds,
        RegionShardComputeBackendPreference ComputeBackendPreference = RegionShardComputeBackendPreference.Auto);

    /// <summary>
    /// Contains the deterministic result of a placement planning attempt.
    /// </summary>
    public sealed class PlacementPlanningResult
    {
        public PlacementPlanningResult(
            ulong placementEpoch,
            string requestId,
            long requestedMs,
            long plannedMs,
            ulong workerSnapshotMs,
            IReadOnlyList<WorkerCandidate> eligibleWorkers,
            IReadOnlyList<ProtoControl.PlacementAssignment> assignments,
            IReadOnlyList<string> plannerWarnings)
        {
            PlacementEpoch = placementEpoch;
            RequestId = requestId;
            RequestedMs = requestedMs;
            PlannedMs = plannedMs;
            WorkerSnapshotMs = workerSnapshotMs;
            EligibleWorkers = eligibleWorkers.ToArray();
            Assignments = assignments.Select(CloneAssignment).ToArray();
            PlannerWarnings = plannerWarnings.ToArray();
        }

        /// <summary>
        /// Gets the placement epoch the plan was produced for.
        /// </summary>
        public ulong PlacementEpoch { get; }

        /// <summary>
        /// Gets the caller-supplied request identifier carried through the plan.
        /// </summary>
        public string RequestId { get; }

        /// <summary>
        /// Gets the original request timestamp, in Unix milliseconds.
        /// </summary>
        public long RequestedMs { get; }

        /// <summary>
        /// Gets the planning timestamp, in Unix milliseconds.
        /// </summary>
        public long PlannedMs { get; }

        /// <summary>
        /// Gets the worker snapshot timestamp used to score candidates.
        /// </summary>
        public ulong WorkerSnapshotMs { get; }

        /// <summary>
        /// Gets the eligible workers considered while building the plan.
        /// </summary>
        public IReadOnlyList<WorkerCandidate> EligibleWorkers { get; }

        /// <summary>
        /// Gets the placement assignments selected for the request.
        /// </summary>
        public IReadOnlyList<ProtoControl.PlacementAssignment> Assignments { get; }

        /// <summary>
        /// Gets any non-fatal warnings emitted while normalizing the request.
        /// </summary>
        public IReadOnlyList<string> PlannerWarnings { get; }

        /// <summary>
        /// Clones the result so callers can reuse it without sharing mutable proto payload instances.
        /// </summary>
        public PlacementPlanningResult Clone()
            => new(
                PlacementEpoch,
                RequestId,
                RequestedMs,
                PlannedMs,
                WorkerSnapshotMs,
                EligibleWorkers,
                Assignments,
                PlannerWarnings);
    }

    /// <summary>
    /// Attempts to build a placement plan from the supplied request and worker snapshot.
    /// </summary>
    /// <param name="inputs">The normalized planning request for one brain.</param>
    /// <param name="workers">The worker candidates available at the snapshot boundary.</param>
    /// <param name="plan">Receives the successful plan or failure context built during evaluation.</param>
    /// <param name="failureReason">Receives the placement failure reason when planning cannot succeed.</param>
    /// <param name="failureMessage">Receives a human-readable planning failure detail.</param>
    /// <returns><see langword="true"/> when planning succeeds; otherwise, <see langword="false"/>.</returns>
    public static bool TryBuildPlan(
        PlannerInputs inputs,
        IEnumerable<WorkerCandidate> workers,
        out PlacementPlanningResult plan,
        out ProtoControl.PlacementFailureReason failureReason,
        out string failureMessage)
    {
        var normalizedRequestId = string.IsNullOrWhiteSpace(inputs.RequestId)
            ? $"{inputs.BrainId:N}:{inputs.PlacementEpoch}"
            : inputs.RequestId.Trim();
        var eligibleWorkers = workers
            .Where(static worker =>
                worker.IsAlive
                && worker.IsReady
                && worker.IsFresh
                && !string.IsNullOrWhiteSpace(worker.WorkerRootActorName)
                && IsWorkerWithinLimits(worker))
            .OrderBy(static worker => worker.WorkerAddress ?? string.Empty, StringComparer.Ordinal)
            .ThenBy(static worker => worker.NodeId)
            .ToArray();

        if (eligibleWorkers.Length == 0)
        {
            plan = new PlacementPlanningResult(
                inputs.PlacementEpoch,
                normalizedRequestId,
                inputs.RequestedMs,
                inputs.PlannedMs,
                inputs.WorkerSnapshotMs,
                Array.Empty<WorkerCandidate>(),
                Array.Empty<ProtoControl.PlacementAssignment>(),
                Array.Empty<string>());
            failureReason = ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable;
            failureMessage = "No eligible workers are available for placement.";
            return false;
        }

        if (inputs.Regions is null || inputs.Regions.Count == 0)
        {
            plan = new PlacementPlanningResult(
                inputs.PlacementEpoch,
                normalizedRequestId,
                inputs.RequestedMs,
                inputs.PlannedMs,
                inputs.WorkerSnapshotMs,
                eligibleWorkers,
                Array.Empty<ProtoControl.PlacementAssignment>(),
                Array.Empty<string>());
            failureReason = ProtoControl.PlacementFailureReason.PlacementFailureInternalError;
            failureMessage = "Placement planning failed because no region spans were available.";
            return false;
        }

        var stride = Math.Max(1, inputs.ShardStride);
        var shardPlan = BuildShardPlan(inputs.Regions, stride, inputs.RequestedShardPlan);
        var warnings = shardPlan.Warnings.ToList();
        IReadOnlyList<ProtoControl.PlacementAssignment> assignments;
        try
        {
            assignments = BuildAssignments(
                inputs.BrainId,
                inputs.PlacementEpoch,
                normalizedRequestId,
                stride,
                eligibleWorkers,
                shardPlan,
                inputs.CurrentWorkerNodeIds,
                inputs.ComputeBackendPreference);
        }
        catch (InvalidOperationException ex)
        {
            plan = new PlacementPlanningResult(
                inputs.PlacementEpoch,
                normalizedRequestId,
                inputs.RequestedMs,
                inputs.PlannedMs,
                inputs.WorkerSnapshotMs,
                eligibleWorkers,
                Array.Empty<ProtoControl.PlacementAssignment>(),
                shardPlan.Warnings);
            failureReason = ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable;
            failureMessage = ex.Message;
            return false;
        }

        plan = new PlacementPlanningResult(
            inputs.PlacementEpoch,
            normalizedRequestId,
            inputs.RequestedMs,
            inputs.PlannedMs,
            inputs.WorkerSnapshotMs,
            eligibleWorkers,
            assignments,
            warnings);
        failureReason = ProtoControl.PlacementFailureReason.PlacementFailureNone;
        failureMessage = string.Empty;
        return true;
    }

    private static IReadOnlyList<ProtoControl.PlacementAssignment> BuildAssignments(
        Guid brainId,
        ulong placementEpoch,
        string requestId,
        int stride,
        IReadOnlyList<WorkerCandidate> eligibleWorkers,
        ShardPlanResult shardPlan,
        IReadOnlyList<Guid> currentWorkerNodeIds,
        RegionShardComputeBackendPreference computeBackendPreference)
    {
        var assignments = new List<ProtoControl.PlacementAssignment>();
        var currentWorkers = currentWorkerNodeIds?.Count > 0
            ? currentWorkerNodeIds.ToHashSet()
            : new HashSet<Guid>();
        var computeShards = shardPlan.Regions
            .OrderBy(static region => region.Key)
            .SelectMany(static region => region.Value)
            .Where(static shard => shard.RegionId != NbnConstants.InputRegionId && shard.RegionId != NbnConstants.OutputRegionId)
            .ToArray();
        var preferGpuWorkload = RegionShardComputeBackendPreferenceResolver.IsGpuExecutionEnabled(computeBackendPreference)
                                && computeShards.Any(shard => shard.NeuronCount >= Math.Max(4096, stride * 2));
        var controlWorker = SelectPrimaryWorker(eligibleWorkers, currentWorkers, preferGpuWorkload);
        var computeWorkers = SelectComputeWorkers(eligibleWorkers, currentWorkers, computeShards, controlWorker, preferGpuWorkload);
        var assignedNeurons = computeWorkers.ToDictionary(static worker => worker.NodeId, static _ => 0);

        AddControlAssignment(assignments, brainId, placementEpoch, requestId, controlWorker, ProtoControl.PlacementAssignmentTarget.PlacementTargetBrainRoot);
        AddControlAssignment(assignments, brainId, placementEpoch, requestId, controlWorker, ProtoControl.PlacementAssignmentTarget.PlacementTargetSignalRouter);
        AddControlAssignment(assignments, brainId, placementEpoch, requestId, controlWorker, ProtoControl.PlacementAssignmentTarget.PlacementTargetInputCoordinator);
        AddControlAssignment(assignments, brainId, placementEpoch, requestId, controlWorker, ProtoControl.PlacementAssignmentTarget.PlacementTargetOutputCoordinator);

        foreach (var region in shardPlan.Regions.OrderBy(static entry => entry.Key))
        {
            foreach (var shard in region.Value.OrderBy(static span => span.ShardIndex))
            {
                var worker = SelectShardWorker(
                    shard,
                    stride,
                    eligibleWorkers,
                    computeWorkers,
                    controlWorker,
                    assignedNeurons,
                    currentWorkers,
                    computeBackendPreference);
                assignments.Add(BuildShardAssignment(brainId, placementEpoch, requestId, worker, shard));
            }
        }

        return assignments;
    }

    private static ShardPlanResult BuildShardPlan(
        IReadOnlyList<RegionSpan> regions,
        int stride,
        ProtoControl.ShardPlan? requestedShardPlan)
    {
        var regionDirectory = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        foreach (var region in regions)
        {
            if (region.RegionId < NbnConstants.RegionMinId || region.RegionId > NbnConstants.RegionMaxId || region.NeuronSpan <= 0)
            {
                continue;
            }

            regionDirectory[region.RegionId] = new NbnRegionDirectoryEntry((uint)region.NeuronSpan, 0, 0, 0);
        }

        var header = new NbnHeaderV2(
            magic: "NBN2",
            version: 2,
            endianness: 1,
            headerBytesPow2: 10,
            brainSeed: 0,
            axonStride: (uint)stride,
            flags: 0,
            quantization: QuantizationSchemas.DefaultNbn,
            regions: regionDirectory);

        var mode = requestedShardPlan is null
            ? SharedShardPlanMode.SingleShardPerRegion
            : (SharedShardPlanMode)Math.Clamp((int)requestedShardPlan.Mode, 0, 2);
        var fixedShardCount = requestedShardPlan is not null && requestedShardPlan.ShardCount > 0
            ? (int)requestedShardPlan.ShardCount
            : (int?)null;
        var maxNeuronsPerShard = requestedShardPlan is not null && requestedShardPlan.MaxNeuronsPerShard > 0
            ? (int)requestedShardPlan.MaxNeuronsPerShard
            : (int?)null;

        return ShardPlanner.BuildPlan(header, mode, fixedShardCount, maxNeuronsPerShard);
    }

    private static WorkerCandidate SelectPrimaryWorker(
        IReadOnlyList<WorkerCandidate> eligibleWorkers,
        IReadOnlySet<Guid> currentWorkers,
        bool preferGpu)
        => OrderWorkersForBrain(eligibleWorkers, currentWorkers, preferGpu).First();

    private static IReadOnlyList<WorkerCandidate> SelectComputeWorkers(
        IReadOnlyList<WorkerCandidate> eligibleWorkers,
        IReadOnlySet<Guid> currentWorkers,
        IReadOnlyList<ShardPlanSpan> computeShards,
        WorkerCandidate controlWorker,
        bool preferGpu)
    {
        var desiredWorkerCount = DetermineDesiredWorkerCount(computeShards, eligibleWorkers.Count);
        var selected = new List<WorkerCandidate> { controlWorker };
        foreach (var worker in OrderWorkersForBrain(eligibleWorkers, currentWorkers, preferGpu))
        {
            if (selected.Count >= desiredWorkerCount)
            {
                break;
            }

            if (selected.Any(existing => existing.NodeId == worker.NodeId))
            {
                continue;
            }

            selected.Add(worker);
        }

        return selected;
    }

    private static int DetermineDesiredWorkerCount(IReadOnlyList<ShardPlanSpan> computeShards, int eligibleWorkerCount)
    {
        if (eligibleWorkerCount <= 1 || computeShards.Count == 0)
        {
            return 1;
        }

        var totalComputeNeurons = computeShards.Sum(static shard => Math.Max(1, shard.NeuronCount));
        var desired = Math.Max(1, (int)Math.Ceiling(totalComputeNeurons / (double)NeuronLocalityThresholdPerWorker));
        desired = Math.Min(desired, computeShards.Count);
        return Math.Clamp(desired, 1, eligibleWorkerCount);
    }

    private static IOrderedEnumerable<WorkerCandidate> OrderWorkersForBrain(
        IEnumerable<WorkerCandidate> workers,
        IReadOnlySet<Guid> currentWorkers,
        bool preferGpu)
        => workers
            .OrderBy(static worker => worker.PeerLatencySampleCount == 0 ? 1 : 0)
            .ThenBy(static worker => worker.PeerLatencySampleCount > 0 ? worker.AveragePeerLatencyMs : float.MaxValue)
            .ThenBy(worker => currentWorkers.Contains(worker.NodeId) ? 0 : 1)
            .ThenBy(static worker => worker.HostedBrainCount)
            .ThenBy(static worker => worker.ProcessCpuLoadPercent)
            .ThenByDescending(worker => preferGpu && HasEffectiveGpu(worker) ? 1 : 0)
            .ThenByDescending(worker => preferGpu ? EffectiveGpuScore(worker) : EffectiveCpuPlacementScore(worker))
            .ThenByDescending(static worker => EffectiveCpuCores(worker))
            .ThenByDescending(static worker => EffectiveRamFreeBytes(worker))
            .ThenByDescending(static worker => EffectiveStorageFreeBytes(worker))
            .ThenByDescending(static worker => EffectiveGpuScore(worker))
            .ThenByDescending(static worker => EffectiveVramFreeBytes(worker))
            .ThenBy(static worker => worker.WorkerAddress ?? string.Empty, StringComparer.Ordinal)
            .ThenBy(static worker => worker.NodeId);

    private static WorkerCandidate SelectShardWorker(
        ShardPlanSpan shard,
        int stride,
        IReadOnlyList<WorkerCandidate> eligibleWorkers,
        IReadOnlyList<WorkerCandidate> computeWorkers,
        WorkerCandidate controlWorker,
        Dictionary<Guid, int> assignedNeurons,
        IReadOnlySet<Guid> currentWorkers,
        RegionShardComputeBackendPreference computeBackendPreference)
    {
        if (shard.RegionId == NbnConstants.InputRegionId || shard.RegionId == NbnConstants.OutputRegionId)
        {
            return controlWorker;
        }

        var preferGpu = RegionShardComputeBackendPreferenceResolver.IsGpuExecutionEnabled(computeBackendPreference)
                        && shard.NeuronCount >= Math.Max(4096, stride * 2)
                        && eligibleWorkers.Any(static worker => HasEffectiveGpu(worker));
        var candidatePool = computeWorkers
            .Where(worker => CanPlaceShardOnWorker(shard, worker, preferGpu))
            .ToArray();
        if (candidatePool.Length == 0)
        {
            candidatePool = eligibleWorkers
                .Where(worker => CanPlaceShardOnWorker(shard, worker, preferGpu))
                .ToArray();
        }

        if (candidatePool.Length == 0)
        {
            throw new InvalidOperationException("No eligible worker satisfied the current capacity/pressure limits for the shard plan.");
        }

        var selected = candidatePool
            .OrderBy(worker => assignedNeurons.TryGetValue(worker.NodeId, out var load) ? load : 0)
            .ThenBy(static worker => worker.PeerLatencySampleCount == 0 ? 1 : 0)
            .ThenBy(static worker => worker.PeerLatencySampleCount > 0 ? worker.AveragePeerLatencyMs : float.MaxValue)
            .ThenBy(worker => currentWorkers.Contains(worker.NodeId) ? 0 : 1)
            .ThenBy(static worker => worker.HostedBrainCount)
            .ThenBy(static worker => worker.ProcessCpuLoadPercent)
            .ThenByDescending(worker => preferGpu && HasEffectiveGpu(worker) ? 1 : 0)
            .ThenByDescending(worker => preferGpu ? EffectiveGpuScore(worker) : EffectiveCpuPlacementScore(worker))
            .ThenByDescending(static worker => EffectiveCpuCores(worker))
            .ThenByDescending(static worker => EffectiveRamFreeBytes(worker))
            .ThenByDescending(static worker => EffectiveStorageFreeBytes(worker))
            .ThenByDescending(static worker => EffectiveGpuScore(worker))
            .ThenByDescending(static worker => EffectiveVramFreeBytes(worker))
            .ThenBy(static worker => worker.WorkerAddress ?? string.Empty, StringComparer.Ordinal)
            .ThenBy(static worker => worker.NodeId)
            .First();

        assignedNeurons[selected.NodeId] = assignedNeurons.TryGetValue(selected.NodeId, out var current)
            ? current + Math.Max(1, shard.NeuronCount)
            : Math.Max(1, shard.NeuronCount);
        return selected;
    }

    private static bool IsWorkerWithinLimits(WorkerCandidate worker)
        => !WorkerCapabilityMath.IsCpuOverLimit(worker.ProcessCpuLoadPercent, worker.CpuLimitPercent, worker.PressureLimitTolerancePercent)
           && !WorkerCapabilityMath.IsRamOverLimit(
               ToUnsignedBytes(worker.ProcessRamUsedBytes),
               ToUnsignedBytes(worker.RamTotalBytes),
               worker.RamLimitPercent,
               worker.PressureLimitTolerancePercent)
           && !WorkerCapabilityMath.IsStorageOverLimit(
               ToUnsignedBytes(worker.StorageFreeBytes),
               ToUnsignedBytes(worker.StorageTotalBytes),
               worker.StorageLimitPercent,
               worker.PressureLimitTolerancePercent)
           && (!worker.HasGpu
               || !WorkerCapabilityMath.IsVramOverLimit(
                   ToUnsignedBytes(worker.VramFreeBytes),
                   ToUnsignedBytes(worker.VramTotalBytes),
                   worker.GpuVramLimitPercent,
                   worker.PressureLimitTolerancePercent));

    private static bool CanPlaceShardOnWorker(ShardPlanSpan shard, WorkerCandidate worker, bool preferGpu)
    {
        if (EffectiveRamFreeBytes(worker) == 0 || EffectiveStorageFreeBytes(worker) == 0)
        {
            return false;
        }

        if (!preferGpu)
        {
            return EffectiveCpuPlacementScore(worker) > 0f;
        }

        return HasEffectiveGpu(worker)
               && EffectiveGpuScore(worker) > 0f
               && EffectiveVramFreeBytes(worker) > 0;
    }

    private static bool HasEffectiveGpu(WorkerCandidate worker)
        => worker.HasGpu
           && worker.GpuComputeLimitPercent > 0
           && worker.GpuVramLimitPercent > 0;

    private static uint EffectiveCpuCores(WorkerCandidate worker)
        => WorkerCapabilityMath.EffectiveCpuCores(worker.CpuCores, worker.CpuLimitPercent);

    private static float EffectiveCpuScore(WorkerCandidate worker)
        => WorkerCapabilityMath.EffectiveCpuScore(worker.CpuScore, worker.CpuLimitPercent);

    private static float EffectiveCpuPlacementScore(WorkerCandidate worker)
    {
        var effectiveScore = EffectiveCpuScore(worker);
        if (effectiveScore > 0f)
        {
            return effectiveScore;
        }

        var effectiveCores = EffectiveCpuCores(worker);
        return effectiveCores > 0 ? effectiveCores / 1000f : 0f;
    }

    private static float EffectiveGpuScore(WorkerCandidate worker)
        => WorkerCapabilityMath.EffectiveGpuScore(worker.GpuScore, worker.GpuComputeLimitPercent);

    private static ulong EffectiveRamFreeBytes(WorkerCandidate worker)
        => WorkerCapabilityMath.EffectiveRamFreeBytes(
            ToUnsignedBytes(worker.RamFreeBytes),
            ToUnsignedBytes(worker.RamTotalBytes),
            ToUnsignedBytes(worker.ProcessRamUsedBytes),
            worker.RamLimitPercent);

    private static ulong EffectiveStorageFreeBytes(WorkerCandidate worker)
        => WorkerCapabilityMath.EffectiveStorageFreeBytes(
            ToUnsignedBytes(worker.StorageFreeBytes),
            ToUnsignedBytes(worker.StorageTotalBytes),
            worker.StorageLimitPercent);

    private static ulong EffectiveVramFreeBytes(WorkerCandidate worker)
        => WorkerCapabilityMath.EffectiveVramFreeBytes(
            ToUnsignedBytes(worker.VramFreeBytes),
            ToUnsignedBytes(worker.VramTotalBytes),
            worker.GpuVramLimitPercent);

    private static ulong ToUnsignedBytes(long value)
        => value > 0 ? (ulong)value : 0UL;

    private static void AddControlAssignment(
        List<ProtoControl.PlacementAssignment> assignments,
        Guid brainId,
        ulong placementEpoch,
        string requestId,
        WorkerCandidate worker,
        ProtoControl.PlacementAssignmentTarget target)
    {
        assignments.Add(new ProtoControl.PlacementAssignment
        {
            AssignmentId = BuildControlAssignmentId(requestId, placementEpoch, target),
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = placementEpoch,
            Target = target,
            WorkerNodeId = worker.NodeId.ToProtoUuid(),
            ActorName = BuildControlActorName(brainId, target)
        });
    }

    private static ProtoControl.PlacementAssignment BuildShardAssignment(
        Guid brainId,
        ulong placementEpoch,
        string requestId,
        WorkerCandidate worker,
        ShardPlanSpan shard)
        => new()
        {
            AssignmentId = BuildShardAssignmentId(requestId, placementEpoch, shard.RegionId, shard.ShardIndex),
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = placementEpoch,
            Target = ProtoControl.PlacementAssignmentTarget.PlacementTargetRegionShard,
            WorkerNodeId = worker.NodeId.ToProtoUuid(),
            RegionId = (uint)shard.RegionId,
            ShardIndex = (uint)shard.ShardIndex,
            NeuronStart = (uint)shard.NeuronStart,
            NeuronCount = (uint)shard.NeuronCount,
            ActorName = BuildShardActorName(brainId, shard.RegionId, shard.ShardIndex)
        };

    private static string BuildControlAssignmentId(
        string requestId,
        ulong placementEpoch,
        ProtoControl.PlacementAssignmentTarget target)
        => $"{requestId}:{placementEpoch}:{ControlTargetToken(target)}";

    private static string BuildShardAssignmentId(string requestId, ulong placementEpoch, int regionId, int shardIndex)
        => $"{requestId}:{placementEpoch}:region-{regionId}-shard-{shardIndex}";

    private static string BuildControlActorName(Guid brainId, ProtoControl.PlacementAssignmentTarget target)
    {
        var brainToken = brainId.ToString("N");
        return target switch
        {
            ProtoControl.PlacementAssignmentTarget.PlacementTargetBrainRoot => $"brain-{brainToken}-root",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetSignalRouter => $"brain-{brainToken}-router",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetInputCoordinator => $"brain-{brainToken}-input",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetOutputCoordinator => $"brain-{brainToken}-output",
            _ => $"brain-{brainToken}-target-{(int)target}"
        };
    }

    private static string BuildShardActorName(Guid brainId, int regionId, int shardIndex)
        => $"brain-{brainId:N}-r{regionId}-s{shardIndex}";

    private static string ControlTargetToken(ProtoControl.PlacementAssignmentTarget target)
        => target switch
        {
            ProtoControl.PlacementAssignmentTarget.PlacementTargetBrainRoot => "brain-root",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetSignalRouter => "signal-router",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetInputCoordinator => "input-coordinator",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetOutputCoordinator => "output-coordinator",
            _ => $"target-{(int)target}"
        };

    private static ProtoControl.PlacementAssignment CloneAssignment(ProtoControl.PlacementAssignment assignment)
        => new()
        {
            AssignmentId = assignment.AssignmentId,
            BrainId = assignment.BrainId?.Clone(),
            PlacementEpoch = assignment.PlacementEpoch,
            Target = assignment.Target,
            WorkerNodeId = assignment.WorkerNodeId?.Clone(),
            RegionId = assignment.RegionId,
            ShardIndex = assignment.ShardIndex,
            NeuronStart = assignment.NeuronStart,
            NeuronCount = assignment.NeuronCount,
            ActorName = assignment.ActorName
        };
}
