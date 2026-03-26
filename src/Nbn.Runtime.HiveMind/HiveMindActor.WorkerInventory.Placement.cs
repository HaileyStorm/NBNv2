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
}
