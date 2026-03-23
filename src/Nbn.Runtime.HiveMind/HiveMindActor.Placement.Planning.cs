using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
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
        var currentWorkerNodeIds = GetCurrentPlacementWorkerNodeIds(brain);
        var hostedBrainCounts = BuildHostedBrainCounts(brain.BrainId);
        var workers = _workerCatalog.Values
            .Where(entry => IsPlacementWorkerCandidate(entry.LogicalName, entry.WorkerRootActorName))
            .Select(entry => new PlacementPlanner.WorkerCandidate(
                entry.NodeId,
                entry.WorkerAddress,
                entry.WorkerRootActorName,
                entry.IsAlive,
                entry.IsReady,
                entry.IsFresh,
                entry.CpuCores,
                entry.RamFreeBytes,
                entry.RamTotalBytes,
                entry.StorageFreeBytes,
                entry.StorageTotalBytes,
                entry.HasGpu,
                entry.VramFreeBytes,
                entry.VramTotalBytes,
                entry.CpuScore,
                entry.GpuScore,
                entry.CpuLimitPercent,
                entry.RamLimitPercent,
                entry.StorageLimitPercent,
                entry.GpuComputeLimitPercent,
                entry.GpuVramLimitPercent,
                entry.ProcessCpuLoadPercent,
                entry.ProcessRamUsedBytes,
                _workerPressureLimitTolerancePercent,
                entry.AveragePeerLatencyMs,
                (uint)Math.Max(0, entry.PeerLatencySampleCount),
                hostedBrainCounts.TryGetValue(entry.NodeId, out var hostedBrainCount) ? hostedBrainCount : 0))
            .ToArray();

        if (!workers.Any(static worker =>
                worker.IsAlive
                && worker.IsReady
                && worker.IsFresh
                && !string.IsNullOrWhiteSpace(worker.WorkerRootActorName)))
        {
            plan = new PlacementPlanner.PlacementPlanningResult(
                brain.PlacementEpoch,
                brain.PlacementRequestId,
                brain.PlacementRequestedMs,
                nowMs,
                snapshotMs,
                Array.Empty<PlacementPlanner.WorkerCandidate>(),
                Array.Empty<ProtoControl.PlacementAssignment>(),
                regionWarning is null ? Array.Empty<string>() : new[] { regionWarning });
            failureReason = ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable;
            failureMessage = "No eligible workers are available for placement.";
            return false;
        }

        var plannerInputs = new PlacementPlanner.PlannerInputs(
            brain.BrainId,
            brain.PlacementEpoch,
            brain.PlacementRequestId,
            brain.PlacementRequestedMs,
            nowMs,
            snapshotMs,
            shardStride,
            brain.RequestedShardPlan,
            regions,
            currentWorkerNodeIds,
            RegionShardComputeBackendPreferenceResolver.Resolve());
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

    private IReadOnlyList<Guid> GetCurrentPlacementWorkerNodeIds(BrainState brain)
    {
        if (brain.PlacementExecution is null)
        {
            return Array.Empty<Guid>();
        }

        var workerIds = new HashSet<Guid>();
        foreach (var assignment in brain.PlacementExecution.Assignments.Values)
        {
            if (TryGetGuid(assignment.Assignment.WorkerNodeId, out var workerNodeId))
            {
                workerIds.Add(workerNodeId);
            }
        }

        return workerIds.OrderBy(static id => id).ToArray();
    }

    private Dictionary<Guid, int> BuildHostedBrainCounts(Guid excludedBrainId)
    {
        var counts = new Dictionary<Guid, int>();
        foreach (var brain in _brains.Values)
        {
            if (brain.BrainId == excludedBrainId)
            {
                continue;
            }

            foreach (var workerNodeId in GetCurrentPlacementWorkerNodeIds(brain))
            {
                counts[workerNodeId] = counts.TryGetValue(workerNodeId, out var current)
                    ? current + 1
                    : 1;
            }
        }

        return counts;
    }

    private static PlacementStateSnapshot CapturePlacementState(BrainState brain)
        => new(
            brain.PlacementEpoch,
            brain.PlacementRequestedMs,
            brain.PlacementUpdatedMs,
            brain.PlacementRequestId,
            brain.RequestedShardPlan?.Clone(),
            brain.PlannedPlacement?.Clone(),
            brain.PlacementExecution,
            brain.PlacementLifecycleState,
            brain.PlacementFailureReason,
            brain.PlacementReconcileState,
            brain.SpawnFailureReasonCode,
            brain.SpawnFailureMessage,
            brain.HomeostasisEnabled,
            brain.HomeostasisTargetMode,
            brain.HomeostasisUpdateMode,
            brain.HomeostasisBaseProbability,
            brain.HomeostasisMinStepCodes,
            brain.HomeostasisEnergyCouplingEnabled,
            brain.HomeostasisEnergyTargetScale,
            brain.HomeostasisEnergyProbabilityScale);

    private static void RestorePlacementState(BrainState brain, PlacementStateSnapshot snapshot)
    {
        brain.PlacementEpoch = snapshot.PlacementEpoch;
        brain.PlacementRequestedMs = snapshot.PlacementRequestedMs;
        brain.PlacementUpdatedMs = snapshot.PlacementUpdatedMs;
        brain.PlacementRequestId = snapshot.PlacementRequestId;
        brain.RequestedShardPlan = snapshot.RequestedShardPlan?.Clone();
        brain.PlannedPlacement = snapshot.PlannedPlacement?.Clone();
        brain.PlacementExecution = snapshot.PlacementExecution;
        brain.PlacementLifecycleState = snapshot.PlacementLifecycleState;
        brain.PlacementFailureReason = snapshot.PlacementFailureReason;
        brain.PlacementReconcileState = snapshot.PlacementReconcileState;
        brain.SpawnFailureReasonCode = snapshot.SpawnFailureReasonCode;
        brain.SpawnFailureMessage = snapshot.SpawnFailureMessage;
        RestorePlacementHomeostasis(brain, snapshot);
    }

    private static void RestorePlacementHomeostasis(BrainState brain, PlacementStateSnapshot snapshot)
    {
        brain.HomeostasisEnabled = snapshot.HomeostasisEnabled;
        brain.HomeostasisTargetMode = snapshot.HomeostasisTargetMode;
        brain.HomeostasisUpdateMode = snapshot.HomeostasisUpdateMode;
        brain.HomeostasisBaseProbability = snapshot.HomeostasisBaseProbability;
        brain.HomeostasisMinStepCodes = snapshot.HomeostasisMinStepCodes;
        brain.HomeostasisEnergyCouplingEnabled = snapshot.HomeostasisEnergyCouplingEnabled;
        brain.HomeostasisEnergyTargetScale = snapshot.HomeostasisEnergyTargetScale;
        brain.HomeostasisEnergyProbabilityScale = snapshot.HomeostasisEnergyProbabilityScale;
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

        try
        {
            var store = CreateArtifactStoreResolver(baseDefinition.StoreUri).Resolve(baseDefinition.StoreUri);
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

    private static bool TryReadPlacementSnapshotHeader(Nbn.Proto.ArtifactRef snapshot, out NbsHeaderV2 header)
    {
        header = default!;
        if (!snapshot.TryToSha256Bytes(out var snapshotHashBytes))
        {
            return false;
        }

        try
        {
            var store = CreateArtifactStoreResolver(snapshot.StoreUri).Resolve(snapshot.StoreUri);
            var stream = store.TryOpenArtifactAsync(new Sha256Hash(snapshotHashBytes))
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
            if (stream is null)
            {
                return false;
            }

            using (stream)
            {
                var headerBytes = new byte[NbnBinary.NbsHeaderBytes];
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

                header = NbnBinary.ReadNbsHeader(headerBytes);
                return true;
            }
        }
        catch
        {
            return false;
        }
    }

    private static void ApplySnapshotHomeostasisConfig(BrainState brain, NbsHomeostasisConfig config)
    {
        brain.HomeostasisEnabled = config.Enabled;
        brain.HomeostasisTargetMode = config.TargetMode;
        brain.HomeostasisUpdateMode = config.UpdateMode;
        brain.HomeostasisBaseProbability = config.BaseProbability;
        brain.HomeostasisMinStepCodes = config.MinStepCodes;
        brain.HomeostasisEnergyCouplingEnabled = config.EnergyCouplingEnabled;
        brain.HomeostasisEnergyTargetScale = config.EnergyTargetScale;
        brain.HomeostasisEnergyProbabilityScale = config.EnergyProbabilityScale;
    }

    private static bool IsValidSnapshotHomeostasisConfig(NbsHomeostasisConfig config)
    {
        return IsSupportedHomeostasisTargetMode(config.TargetMode)
               && config.UpdateMode == ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep
               && float.IsFinite(config.BaseProbability)
               && config.BaseProbability >= 0f
               && config.BaseProbability <= 1f
               && config.MinStepCodes != 0
               && IsFiniteInRange(config.EnergyTargetScale, 0f, 4f)
               && IsFiniteInRange(config.EnergyProbabilityScale, 0f, 4f);
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
}
