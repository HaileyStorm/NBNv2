using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private bool HandleWorkerInventoryMessage(IContext context)
    {
        switch (context.Message)
        {
            case ProtoSettings.WorkerInventorySnapshotResponse message:
                HandleWorkerInventorySnapshotResponse(context, message);
                return true;
            case ProtoSettings.NodeListResponse message:
                HandleNodeListResponse(message);
                return true;
            case SweepVisualizationSubscribers:
                HandleSweepVisualizationSubscribers(context);
                return true;
            case RefreshWorkerInventoryTick:
                RefreshWorkerInventory(context);
                return true;
            case RefreshWorkerCapabilitiesTick:
                RefreshWorkerCapabilities(context);
                return true;
            case Terminated message:
                HandleVisualizationSubscriberTerminated(context, message.Who);
                return true;
            default:
                return false;
        }
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
        var hasCpuFallback = WorkerCapabilityMath.HasEffectiveCpuPlacementCapacity(
            worker.CpuScore,
            worker.CpuCores,
            worker.CpuLimitPercent);
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
                   && !hasCpuFallback
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
}
