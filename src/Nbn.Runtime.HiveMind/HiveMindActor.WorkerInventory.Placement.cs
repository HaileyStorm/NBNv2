using Nbn.Shared;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private ProtoControl.PlacementWorkerInventory BuildPlacementWorkerInventory()
    {
        var nowMs = NowMs();
        RefreshWorkerCatalogFreshness(nowMs);

        var snapshotMs = _workerCatalogSnapshotMs > 0 ? _workerCatalogSnapshotMs : nowMs;
        var inventory = new ProtoControl.PlacementWorkerInventory
        {
            SnapshotMs = (ulong)snapshotMs,
            TotalWorkersSeen = (uint)Math.Max(0, _workerCatalog.Count)
        };

        var exclusionCounts = new Dictionary<string, int>(StringComparer.Ordinal);

        foreach (var entry in _workerCatalog.Values
                     .OrderBy(static worker => worker.WorkerAddress, StringComparer.Ordinal)
                     .ThenBy(static worker => worker.NodeId))
        {
            var reasons = GetPlacementInventoryExclusionReasons(entry, snapshotMs);
            if (reasons.Count == 0)
            {
                inventory.Workers.Add(CreatePlacementWorkerInventoryEntry(entry));
                continue;
            }

            var excluded = new ProtoControl.PlacementWorkerExclusionDiagnostic
            {
                WorkerNodeId = entry.NodeId.ToProtoUuid(),
                WorkerAddress = entry.WorkerAddress,
                WorkerRootActorName = entry.WorkerRootActorName
            };
            excluded.ReasonCodes.Add(reasons);
            inventory.ExcludedWorkers.Add(excluded);

            foreach (var reason in reasons)
            {
                exclusionCounts.TryGetValue(reason, out var count);
                exclusionCounts[reason] = count + 1;
            }
        }

        foreach (var entry in exclusionCounts.OrderBy(static item => item.Key, StringComparer.Ordinal))
        {
            inventory.ExclusionCounts.Add(new ProtoControl.PlacementWorkerExclusionCount
            {
                ReasonCode = entry.Key,
                Count = (uint)Math.Max(0, entry.Value)
            });
        }

        return inventory;
    }

    private ProtoControl.PlacementWorkerInventoryEntry CreatePlacementWorkerInventoryEntry(WorkerCatalogEntry entry)
    {
        return new ProtoControl.PlacementWorkerInventoryEntry
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
        };
    }

    private IReadOnlyList<string> GetPlacementInventoryExclusionReasons(WorkerCatalogEntry worker, long referenceMs)
    {
        var reasons = new List<string>();
        if (!worker.IsAlive)
        {
            reasons.Add("not_alive");
        }

        if (!worker.IsReady)
        {
            reasons.Add("not_ready");
        }

        var staleAfterMs = Math.Max(1, _options.WorkerInventoryStaleAfterMs);
        if (!IsFreshTimestamp(worker.LastSeenMs, referenceMs, staleAfterMs))
        {
            reasons.Add("stale_last_seen");
        }

        if (!IsFreshTimestamp(worker.CapabilitySnapshotMs, referenceMs, staleAfterMs))
        {
            reasons.Add("stale_capabilities");
        }

        if (!IsPlacementWorkerCandidate(worker.LogicalName, worker.WorkerRootActorName))
        {
            reasons.Add("not_worker_candidate");
        }

        if (string.IsNullOrWhiteSpace(worker.WorkerRootActorName))
        {
            reasons.Add("missing_worker_root_actor");
        }

        if (IsWorkerPressureViolation(worker))
        {
            reasons.Add("pressure_violation");
        }

        var effectiveRamFreeBytes = WorkerCapabilityMath.EffectiveRamFreeBytes(
            ToUnsignedBytes(worker.RamFreeBytes),
            ToUnsignedBytes(worker.RamTotalBytes),
            ToUnsignedBytes(worker.ProcessRamUsedBytes),
            worker.RamLimitPercent);
        if (effectiveRamFreeBytes == 0)
        {
            reasons.Add("no_effective_ram");
        }

        var effectiveStorageFreeBytes = WorkerCapabilityMath.EffectiveStorageFreeBytes(
            ToUnsignedBytes(worker.StorageFreeBytes),
            ToUnsignedBytes(worker.StorageTotalBytes),
            worker.StorageLimitPercent);
        if (effectiveStorageFreeBytes == 0)
        {
            reasons.Add("no_effective_storage");
        }

        var effectiveCpuScore = WorkerCapabilityMath.EffectiveCpuScore(worker.CpuScore, worker.CpuLimitPercent);
        if (effectiveCpuScore <= 0f)
        {
            var hasUsableGpu = false;
            if (worker.HasGpu)
            {
                var effectiveGpuScore = WorkerCapabilityMath.EffectiveGpuScore(worker.GpuScore, worker.GpuComputeLimitPercent);
                var effectiveVramFreeBytes = WorkerCapabilityMath.EffectiveVramFreeBytes(
                    ToUnsignedBytes(worker.VramFreeBytes),
                    ToUnsignedBytes(worker.VramTotalBytes),
                    worker.GpuVramLimitPercent);
                hasUsableGpu = effectiveGpuScore > 0f && effectiveVramFreeBytes > 0;
            }

            if (!hasUsableGpu)
            {
                reasons.Add("no_effective_compute_capacity");
            }
        }

        return reasons;
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

        if (MatchesConfiguredWorkerRootActor(rootActorName))
        {
            return true;
        }

        return MatchesKnownWorkerRootActor(rootActorName);
    }

    private bool MatchesConfiguredWorkerRootActor(string? rootActorName)
        => MatchesRootActorNameOrGeneratedSibling(rootActorName, _configuredWorkerRootActorName);

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

    private static bool MatchesRootActorNameOrGeneratedSibling(string? rootActorName, string? configuredRootActorName)
    {
        if (string.IsNullOrWhiteSpace(rootActorName) || string.IsNullOrWhiteSpace(configuredRootActorName))
        {
            return false;
        }

        var root = rootActorName.Trim();
        var configured = configuredRootActorName.Trim();
        if (root.Equals(configured, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (!root.StartsWith(configured + "-", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var suffix = root[(configured.Length + 1)..];
        return int.TryParse(suffix, out var index) && index > 1;
    }
}
