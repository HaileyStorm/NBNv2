using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
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

    private async Task RefreshWorkerCapabilitiesAsync(IContext context)
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
                             && !string.IsNullOrWhiteSpace(entry.WorkerRootActorName))
                         .OrderBy(static entry => entry.WorkerAddress, StringComparer.Ordinal)
                         .ThenBy(static entry => entry.NodeId))
            {
                var target = await ResolveWorkerTargetPidAsync(context, worker).ConfigureAwait(false);
                if (target is not null)
                {
                    context.Send(target, request);
                }
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
            entry.WorkerActorReference = BuildWorkerActorReference(nodeId, entry.WorkerAddress, entry.WorkerRootActorName);
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

    private void HandleNodeListResponse(IContext context, ProtoSettings.NodeListResponse message)
    {
        _activeSettingsNodeAddresses.Clear();
        foreach (var node in message.Nodes)
        {
            if (node.NodeId is not null
                && node.NodeId.TryToGuid(out var nodeId)
                && _settingsPid is not null)
            {
                context.Request(_settingsPid, new ProtoSettings.SettingGet
                {
                    Key = NodeEndpointSetSettings.BuildKey(nodeId)
                });
            }

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
                WorkerActorReference = entry.WorkerActorReference,
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
                entry.WorkerRootActorName,
                entry.WorkerActorReference))
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
                    WorkerRootActorName = peer.WorkerRootActorName,
                    WorkerActorReference = peer.WorkerActorReference
                });
            }

            var target = await ResolveWorkerActorReferenceAsync(worker.WorkerActorReference).ConfigureAwait(false);
            if (target is null)
            {
                measurements.Add(new WorkerPeerLatencyMeasurement(worker.NodeId, 0f, 0));
                continue;
            }
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

    private string BuildWorkerActorReference(Guid nodeId, string workerAddress, string workerRootActorName)
    {
        if (string.IsNullOrWhiteSpace(workerRootActorName))
        {
            return string.Empty;
        }

        if (_nodeEndpointSets.TryGetValue(nodeId, out var endpointSet) && endpointSet.Candidates.Count > 0)
        {
            return RoutablePidReference.Encode(endpointSet.Candidates, workerRootActorName.Trim());
        }

        return string.IsNullOrWhiteSpace(workerAddress)
            ? workerRootActorName.Trim()
            : $"{workerAddress.Trim()}/{workerRootActorName.Trim()}";
    }

    private static async Task<PID?> ResolveWorkerActorReferenceAsync(string? workerActorReference)
        => await RoutablePidReference.ResolveAsync(workerActorReference).ConfigureAwait(false);

    private async Task<PID?> ResolveWorkerTargetPidAsync(IContext context, WorkerCatalogEntry worker)
    {
        var resolved = await ResolveWorkerActorReferenceAsync(worker.WorkerActorReference).ConfigureAwait(false);
        return resolved is null ? null : ResolveSendTargetPid(context, resolved);
    }
}
