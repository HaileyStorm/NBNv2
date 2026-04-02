using System;
using System.Collections.Generic;
using System.Linq;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class OrchestratorPanelViewModel
{
    private static bool IsFresh(ulong lastSeenMs, long nowMs)
    {
        if (lastSeenMs == 0)
        {
            return false;
        }

        var delta = nowMs - (long)lastSeenMs;
        if (delta < 0)
        {
            return false;
        }

        return delta <= StaleNodeMs;
    }

    private static bool IsSpawnRecent(ulong spawnedMs, long nowMs)
    {
        if (spawnedMs == 0)
        {
            return false;
        }

        var delta = nowMs - (long)spawnedMs;
        if (delta < 0)
        {
            return false;
        }

        return delta <= SpawnVisibilityGraceMs;
    }

    private WorkerEndpointState BuildWorkerEndpointState(
        IReadOnlyList<Nbn.Proto.Settings.NodeStatus> nodes,
        IReadOnlyList<Nbn.Proto.Settings.WorkerReadinessCapability> inventory,
        IReadOnlyDictionary<Guid, HashSet<Guid>> workerBrainHints,
        IReadOnlyDictionary<(Guid NodeId, Guid BrainId), WorkerBrainBackendHint> workerBrainBackends,
        long nowMs)
    {
        foreach (var worker in inventory)
        {
            if (worker.NodeId is null || !worker.NodeId.TryToGuid(out var nodeId))
            {
                continue;
            }

            if (!IsWorkerHostCandidate(worker.LogicalName, worker.RootActorName))
            {
                continue;
            }

            UpdateWorkerEndpointSnapshot(
                nodeId,
                worker.LogicalName,
                worker.Address,
                worker.RootActorName,
                (long)worker.LastSeenMs,
                worker.IsAlive,
                hasCapabilitySnapshot: true,
                isReady: worker.IsReady,
                hasCapabilities: worker.HasCapabilities,
                hasGpu: worker.Capabilities?.HasGpu,
                cpuScore: worker.Capabilities?.CpuScore,
                gpuScore: worker.Capabilities?.GpuScore,
                ilgpuCudaAvailable: worker.Capabilities?.IlgpuCudaAvailable,
                ilgpuOpenclAvailable: worker.Capabilities?.IlgpuOpenclAvailable,
                placementStatus: DescribeWorkerPlacementStatus(worker, out var placementDetail),
                placementDetail: placementDetail);
        }

        foreach (var node in nodes)
        {
            if (node.NodeId is null || !node.NodeId.TryToGuid(out var nodeId))
            {
                continue;
            }

            if (!IsWorkerHostCandidate(node.LogicalName, node.RootActorName))
            {
                continue;
            }

            UpdateWorkerEndpointSnapshot(
                nodeId,
                node.LogicalName,
                node.Address,
                node.RootActorName,
                (long)node.LastSeenMs,
                node.IsAlive);
        }

        var displayEntries = new List<WorkerEndpointDisplayEntry>();
        var staleNodeIds = new List<Guid>();
        var activeCount = 0;
        var limitedCount = 0;
        var degradedCount = 0;
        var failedCount = 0;

        foreach (var entry in _workerEndpointCache.Values)
        {
            var (status, remove) = ClassifyWorkerEndpointStatus(entry, nowMs);
            if (remove)
            {
                staleNodeIds.Add(entry.NodeId);
                continue;
            }

            var brainHints = DescribeWorkerBrainHints(workerBrainHints, workerBrainBackends, entry.NodeId);
            var capabilityChips = DescribeWorkerCapabilityChips(entry);
            displayEntries.Add(new WorkerEndpointDisplayEntry(
                entry,
                status,
                WorkbenchWorkerHostGrouping.ResolveHostGroupKey(
                    entry.Address,
                    entry.LogicalName,
                    entry.RootActorName,
                    entry.NodeId),
                WorkbenchWorkerHostGrouping.ResolveHostDisplayName(
                    entry.Address,
                    entry.LogicalName,
                    entry.NodeId),
                new WorkerEndpointItem(
                    entry.NodeId,
                    entry.LogicalName,
                    entry.Address,
                    entry.RootActorName,
                    brainHints.Preview,
                    FormatUpdated(entry.LastSeenMs),
                    status,
                    entry.PlacementDetail,
                    capabilityChips.CpuCapability,
                    capabilityChips.GpuCapability,
                    brainHints.Count)));
        }

        foreach (var staleNodeId in staleNodeIds)
        {
            _workerEndpointCache.Remove(staleNodeId);
        }

        foreach (var group in displayEntries
                     .GroupBy(
                         entry => entry.GroupKey,
                         StringComparer.Ordinal))
        {
            var entries = group
                .OrderBy(entry => entry.Rank)
                .ThenByDescending(entry => entry.Snapshot.LastSeenMs)
                .ThenBy(entry => entry.Row.LogicalName, StringComparer.OrdinalIgnoreCase)
                .ToArray();
            var status = ResolveWorkerNodeStatus(entries.Select(static entry => entry.Status));
            switch (status)
            {
                case "active":
                    activeCount++;
                    break;
                case "limited":
                    limitedCount++;
                    break;
                case "degraded":
                    degradedCount++;
                    break;
                default:
                    failedCount++;
                    break;
            }
        }

        var orderedEntries = displayEntries
            .OrderBy(entry => entry.Rank)
            .ThenByDescending(entry => entry.Snapshot.LastSeenMs)
            .ThenBy(entry => entry.Row.LogicalName, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var orderedRows = orderedEntries
            .Select(static entry => entry.Row)
            .ToArray();
        var orderedGroups = orderedEntries
            .GroupBy(entry => entry.GroupKey, StringComparer.Ordinal)
            .Select(group => new WorkerNodeGroupItem(
                group.First().GroupLabel,
                WorkbenchWorkerHostGrouping.FormatWorkerCountLabel(group.Count()),
                group.Select(static entry => entry.Row).ToArray()))
            .ToArray();

        var summary = BuildWorkerEndpointSummary(
            activeCount,
            limitedCount,
            degradedCount,
            failedCount,
            displayEntries.Count);
        return new WorkerEndpointState(orderedRows, orderedGroups, activeCount, limitedCount, degradedCount, failedCount, summary);
    }

    private WorkbenchSystemLoadSummary BuildSystemLoadState(
        IReadOnlyList<Nbn.Proto.Settings.WorkerReadinessCapability> inventory,
        Nbn.Proto.Control.HiveMindStatus? hiveMindStatus,
        long nowMs)
    {
        var workers = WorkbenchSystemLoadSummaryBuilder.FilterWorkers(
            inventory,
            nowMs,
            worker => worker.NodeId is not null
                      && worker.NodeId.TryToGuid(out _)
                      && IsWorkerHostCandidate(worker.LogicalName, worker.RootActorName));
        var history = _systemLoadHistory.Record(workers, hiveMindStatus, nowMs);
        return WorkbenchSystemLoadSummaryBuilder.Build(workers, hiveMindStatus, history);
    }

    private static long ResolveWorkerReferenceTimeMs(
        Nbn.Proto.Settings.WorkerInventorySnapshotResponse? workerInventoryResponse,
        long fallbackNowMs)
    {
        var snapshotMs = workerInventoryResponse is not null
            ? (long)workerInventoryResponse.SnapshotMs
            : 0;
        return snapshotMs > 0 ? snapshotMs : fallbackNowMs;
    }

    private static long ResolveBrainsReferenceTimeMs(
        Nbn.Proto.Settings.BrainListResponse? response,
        long fallbackNowMs)
    {
        if (response is null)
        {
            return fallbackNowMs;
        }

        var referenceMs = 0L;
        foreach (var controller in response.Controllers)
        {
            referenceMs = Math.Max(referenceMs, (long)controller.LastSeenMs);
        }

        foreach (var brain in response.Brains)
        {
            referenceMs = Math.Max(referenceMs, (long)brain.SpawnedMs);
        }

        return referenceMs > 0 ? referenceMs : fallbackNowMs;
    }

    private static long ResolveSettingsReferenceTimeMs(
        Nbn.Proto.Settings.WorkerInventorySnapshotResponse? workerInventoryResponse,
        IReadOnlyList<Nbn.Proto.Settings.NodeStatus> nodes,
        IReadOnlyList<Nbn.Proto.Settings.BrainControllerStatus> controllers,
        IReadOnlyList<Nbn.Proto.Settings.BrainStatus> brains,
        long fallbackNowMs)
    {
        var referenceMs = ResolveWorkerReferenceTimeMs(workerInventoryResponse, fallbackNowMs);
        foreach (var node in nodes)
        {
            referenceMs = Math.Max(referenceMs, (long)node.LastSeenMs);
        }

        foreach (var controller in controllers)
        {
            referenceMs = Math.Max(referenceMs, (long)controller.LastSeenMs);
        }

        foreach (var brain in brains)
        {
            referenceMs = Math.Max(referenceMs, (long)brain.SpawnedMs);
        }

        return referenceMs > 0 ? referenceMs : fallbackNowMs;
    }

    private void UpdateWorkerEndpointSnapshot(
        Guid nodeId,
        string? logicalName,
        string? address,
        string? rootActorName,
        long lastSeenMs,
        bool isAlive,
        bool? hasCapabilitySnapshot = null,
        bool? isReady = null,
        bool? hasCapabilities = null,
        bool? hasGpu = null,
        float? cpuScore = null,
        float? gpuScore = null,
        bool? ilgpuCudaAvailable = null,
        bool? ilgpuOpenclAvailable = null,
        string? placementStatus = null,
        string? placementDetail = null)
    {
        if (!_workerEndpointCache.TryGetValue(nodeId, out var snapshot))
        {
            snapshot = new WorkerEndpointSnapshot
            {
                NodeId = nodeId
            };
            _workerEndpointCache[nodeId] = snapshot;
        }

        if (!string.IsNullOrWhiteSpace(logicalName))
        {
            snapshot.LogicalName = logicalName.Trim();
        }

        if (!string.IsNullOrWhiteSpace(address))
        {
            snapshot.Address = address.Trim();
        }

        if (!string.IsNullOrWhiteSpace(rootActorName))
        {
            snapshot.RootActorName = rootActorName.Trim();
        }

        var previousLastSeen = snapshot.LastSeenMs;
        snapshot.LastSeenMs = Math.Max(snapshot.LastSeenMs, lastSeenMs);
        if (lastSeenMs >= previousLastSeen || !isAlive)
        {
            snapshot.IsAlive = isAlive;
        }

        if (hasCapabilitySnapshot.HasValue)
        {
            snapshot.HasCapabilitySnapshot = hasCapabilitySnapshot.Value;
        }

        if (isReady.HasValue)
        {
            snapshot.IsReady = isReady.Value;
        }

        if (hasCapabilities.HasValue)
        {
            snapshot.HasCapabilities = hasCapabilities.Value;
        }

        if (hasGpu.HasValue)
        {
            snapshot.HasGpu = hasGpu.Value;
        }

        if (cpuScore.HasValue)
        {
            snapshot.CpuScore = cpuScore.Value;
        }

        if (gpuScore.HasValue)
        {
            snapshot.GpuScore = gpuScore.Value;
        }

        if (ilgpuCudaAvailable.HasValue)
        {
            snapshot.IlgpuCudaAvailable = ilgpuCudaAvailable.Value;
        }

        if (ilgpuOpenclAvailable.HasValue)
        {
            snapshot.IlgpuOpenclAvailable = ilgpuOpenclAvailable.Value;
        }

        if (!string.IsNullOrWhiteSpace(placementStatus))
        {
            snapshot.PlacementStatus = placementStatus.Trim();
        }

        if (!string.IsNullOrWhiteSpace(placementDetail))
        {
            snapshot.PlacementDetail = placementDetail.Trim();
        }
    }

    private static (string Status, bool Remove) ClassifyWorkerEndpointStatus(WorkerEndpointSnapshot snapshot, long nowMs)
    {
        if (snapshot.LastSeenMs <= 0)
        {
            return ("failed", false);
        }

        var ageMs = nowMs - snapshot.LastSeenMs;
        if (ageMs < 0)
        {
            ageMs = 0;
        }

        if (ageMs > WorkerRemoveAfterMs)
        {
            return ("failed", true);
        }

        if (!snapshot.IsAlive || ageMs > WorkerFailedAfterMs)
        {
            return ("failed", false);
        }

        if (ageMs > StaleNodeMs)
        {
            return ("degraded", false);
        }

        if (snapshot.HasCapabilitySnapshot)
        {
            if (!snapshot.HasCapabilities || !snapshot.IsReady)
            {
                return ("degraded", false);
            }

            if (string.Equals(snapshot.PlacementStatus, "limited", StringComparison.OrdinalIgnoreCase))
            {
                return ("limited", false);
            }
        }

        return ("active", false);
    }

    private static int WorkerStatusRank(string status)
    {
        return status switch
        {
            "active" => 0,
            "limited" => 1,
            "degraded" => 2,
            "failed" => 3,
            _ => 4
        };
    }

    private static string BuildWorkerEndpointSummary(
        int activeCount,
        int limitedCount,
        int degradedCount,
        int failedCount,
        int workerCount)
    {
        var parts = new List<string>();
        if (activeCount > 0)
        {
            parts.Add(FormatCount(activeCount, "active"));
        }

        if (limitedCount > 0)
        {
            parts.Add(FormatCount(limitedCount, "limited"));
        }

        if (degradedCount > 0)
        {
            parts.Add(FormatCount(degradedCount, "degraded"));
        }

        if (failedCount > 0)
        {
            parts.Add(FormatCount(failedCount, "failed"));
        }

        var summary = parts.Count == 0
            ? "No active nodes."
            : string.Join(", ", parts);

        var hostCount = activeCount + limitedCount + degradedCount + failedCount;
        if (hostCount > 0 && workerCount > hostCount)
        {
            summary += $" ({WorkbenchWorkerHostGrouping.FormatWorkerCountLabel(workerCount)})";
        }

        return summary;
    }

    private static string BuildWorkerCardSummary(string summary, int activeBrainCount)
    {
        if (string.IsNullOrWhiteSpace(summary)
            || string.Equals(summary, "No active nodes.", StringComparison.Ordinal))
        {
            return summary;
        }

        var normalizedActiveBrainCount = Math.Max(0, activeBrainCount);
        if (normalizedActiveBrainCount == 0)
        {
            return summary;
        }

        var brainLabel = $"{normalizedActiveBrainCount} brain{(normalizedActiveBrainCount == 1 ? string.Empty : "s")}";
        return summary.EndsWith(")", StringComparison.Ordinal)
            ? $"{summary[..^1]}, {brainLabel})"
            : $"{summary} ({brainLabel})";
    }

    private static string DescribeWorkerPlacementStatus(Nbn.Proto.Settings.WorkerReadinessCapability worker, out string detail)
    {
        detail = string.Empty;
        var caps = worker.Capabilities ?? new Nbn.Proto.Settings.NodeCapabilities();

        if (!worker.HasCapabilities)
        {
            detail = "Capability warm-up in progress.";
            return "degraded";
        }

        if (string.IsNullOrWhiteSpace(worker.RootActorName))
        {
            detail = "Missing worker root actor.";
            return "limited";
        }

        if (WorkerCapabilityMath.IsCpuOverLimit(caps.ProcessCpuLoadPercent, caps.CpuLimitPercent, 0f))
        {
            detail = $"CPU load {caps.ProcessCpuLoadPercent:0.#}% > {caps.CpuLimitPercent}% limit.";
            return "limited";
        }

        if (WorkerCapabilityMath.IsRamOverLimit(
                caps.ProcessRamUsedBytes,
                caps.RamTotalBytes,
                caps.RamLimitPercent,
                0f))
        {
            detail = $"RAM use exceeds {caps.RamLimitPercent}% limit.";
            return "limited";
        }

        var storageUsedPercent = caps.StorageTotalBytes == 0
            ? 0f
            : WorkerCapabilityMath.ComputeUsedPercent(caps.StorageFreeBytes, caps.StorageTotalBytes);
        if (WorkerCapabilityMath.IsStorageOverLimit(
                caps.StorageFreeBytes,
                caps.StorageTotalBytes,
                caps.StorageLimitPercent,
                0f))
        {
            detail = $"Storage used {storageUsedPercent:0.#}% > {caps.StorageLimitPercent}% limit.";
            return "limited";
        }

        if (caps.RamFreeBytes == 0 || caps.RamTotalBytes == 0)
        {
            detail = "Missing RAM capacity telemetry.";
            return "limited";
        }

        if (caps.StorageFreeBytes == 0 || caps.StorageTotalBytes == 0)
        {
            detail = "Missing storage capacity telemetry.";
            return "limited";
        }

        if (caps.CpuCores == 0)
        {
            detail = "Missing CPU core telemetry.";
            return "limited";
        }

        detail = "Placement ready.";
        return "active";
    }

    private static string FormatCount(int count, string label)
    {
        var suffix = count == 1 ? "node" : "nodes";
        return $"{count} {label} {suffix}";
    }

    private static string ResolveWorkerNodeStatus(IEnumerable<string> statuses)
    {
        var statusList = statuses.ToArray();
        if (statusList.Any(static status => string.Equals(status, "active", StringComparison.OrdinalIgnoreCase)))
        {
            return "active";
        }

        if (statusList.Any(static status => string.Equals(status, "limited", StringComparison.OrdinalIgnoreCase)))
        {
            return "limited";
        }

        if (statusList.Any(static status => string.Equals(status, "degraded", StringComparison.OrdinalIgnoreCase)))
        {
            return "degraded";
        }

        return "failed";
    }

    private static WorkerBrainHintSummary DescribeWorkerBrainHints(
        IReadOnlyDictionary<Guid, HashSet<Guid>> workerBrainHints,
        IReadOnlyDictionary<(Guid NodeId, Guid BrainId), WorkerBrainBackendHint> workerBrainBackends,
        Guid nodeId)
        => DescribeWorkerBrainHints(workerBrainHints, workerBrainBackends, new[] { nodeId });

    private static WorkerBrainHintSummary DescribeWorkerBrainHints(
        IReadOnlyDictionary<Guid, HashSet<Guid>> workerBrainHints,
        IReadOnlyDictionary<(Guid NodeId, Guid BrainId), WorkerBrainBackendHint> workerBrainBackends,
        IEnumerable<Guid> nodeIds)
    {
        var resolvedNodeIds = nodeIds.Distinct().ToArray();
        if (resolvedNodeIds.Length == 0)
        {
            return new WorkerBrainHintSummary(0, "none");
        }

        var brainIds = new HashSet<Guid>();
        foreach (var nodeId in resolvedNodeIds)
        {
            if (!workerBrainHints.TryGetValue(nodeId, out var nodeBrainIds))
            {
                continue;
            }

            foreach (var brainId in nodeBrainIds)
            {
                brainIds.Add(brainId);
            }
        }

        if (brainIds.Count == 0)
        {
            return new WorkerBrainHintSummary(0, "none");
        }

        var gpuBrains = new List<string>();
        var cpuBrains = new List<string>();
        var mixedBrains = new List<string>();
        var otherBrains = new List<string>();
        foreach (var brainId in brainIds.OrderBy(AbbreviateBrainId, StringComparer.Ordinal))
        {
            var abbreviated = AbbreviateBrainId(brainId);
            var backend = ResolveWorkerBrainBackendHint(resolvedNodeIds, brainId, workerBrainBackends);
            if (backend.HasValue)
            {
                if (backend == WorkerBrainBackendHint.Gpu)
                {
                    gpuBrains.Add(abbreviated);
                }
                else if (backend == WorkerBrainBackendHint.Mixed)
                {
                    mixedBrains.Add(abbreviated);
                }
                else
                {
                    cpuBrains.Add(abbreviated);
                }
            }
            else
            {
                otherBrains.Add(abbreviated);
            }
        }

        var segments = new List<string>();
        if (gpuBrains.Count > 0)
        {
            segments.Add($"GPU {FormatBrainHintPreview(gpuBrains)}");
        }

        if (cpuBrains.Count > 0)
        {
            segments.Add($"CPU {FormatBrainHintPreview(cpuBrains)}");
        }

        if (mixedBrains.Count > 0)
        {
            segments.Add($"Mixed {FormatBrainHintPreview(mixedBrains)}");
        }

        if (otherBrains.Count > 0)
        {
            segments.Add(segments.Count == 0
                ? FormatBrainHintPreview(otherBrains)
                : $"Other {FormatBrainHintPreview(otherBrains)}");
        }

        return new WorkerBrainHintSummary(brainIds.Count, string.Join("; ", segments));
    }

    private static WorkerBrainBackendHint? ResolveWorkerBrainBackendHint(
        IReadOnlyList<Guid> nodeIds,
        Guid brainId,
        IReadOnlyDictionary<(Guid NodeId, Guid BrainId), WorkerBrainBackendHint> workerBrainBackends)
    {
        WorkerBrainBackendHint? resolved = null;
        foreach (var nodeId in nodeIds)
        {
            if (!workerBrainBackends.TryGetValue((nodeId, brainId), out var candidate))
            {
                continue;
            }

            if (candidate == WorkerBrainBackendHint.Mixed)
            {
                return candidate;
            }

            if (!resolved.HasValue || candidate == WorkerBrainBackendHint.Gpu)
            {
                resolved = candidate;
            }
        }

        return resolved;
    }

    private static string FormatBrainHintPreview(IReadOnlyList<string> brains)
        => brains.Count <= MaxWorkerBrainHints
            ? string.Join(", ", brains)
            : $"{string.Join(", ", brains.Take(MaxWorkerBrainHints))}, ...";

    private static string AbbreviateBrainId(Guid brainId)
    {
        var compact = brainId.ToString("N");
        return compact.Length <= 4 ? compact : compact[^4..];
    }

    private static string AbbreviateHostedActorBrainId(Guid brainId)
    {
        var compact = brainId.ToString("N");
        return compact.Length <= 8 ? compact : compact[^8..];
    }

    private sealed record WorkerEndpointState(
        IReadOnlyList<WorkerEndpointItem> Rows,
        IReadOnlyList<WorkerNodeGroupItem> Groups,
        int ActiveCount,
        int LimitedCount,
        int DegradedCount,
        int FailedCount,
        string SummaryText);

    private sealed record WorkerBrainHintSummary(
        int Count,
        string Preview);

    private sealed record WorkerEndpointDisplayEntry(
        WorkerEndpointSnapshot Snapshot,
        string Status,
        string GroupKey,
        string GroupLabel,
        WorkerEndpointItem Row)
    {
        public int Rank => WorkerStatusRank(Status);
    }

    private enum WorkerBrainBackendHint
    {
        Cpu = 0,
        Gpu = 1,
        Mixed = 2
    }

    private sealed class WorkerEndpointSnapshot
    {
        public Guid NodeId { get; init; }
        public string LogicalName { get; set; } = string.Empty;
        public string Address { get; set; } = string.Empty;
        public string RootActorName { get; set; } = string.Empty;
        public long LastSeenMs { get; set; }
        public bool IsAlive { get; set; }
        public bool HasCapabilitySnapshot { get; set; }
        public bool IsReady { get; set; }
        public bool HasCapabilities { get; set; }
        public bool HasGpu { get; set; }
        public float CpuScore { get; set; }
        public float GpuScore { get; set; }
        public bool IlgpuCudaAvailable { get; set; }
        public bool IlgpuOpenclAvailable { get; set; }
        public string PlacementStatus { get; set; } = string.Empty;
        public string PlacementDetail { get; set; } = string.Empty;
    }

    private sealed record WorkerCapabilityChips(
        string CpuCapability,
        string GpuCapability);

    private static WorkerCapabilityChips DescribeWorkerCapabilityChips(WorkerEndpointSnapshot snapshot)
    {
        if (!snapshot.HasCapabilitySnapshot || !snapshot.HasCapabilities)
        {
            return new WorkerCapabilityChips(string.Empty, string.Empty);
        }

        var cpuScore = FormatCompactCapabilityScore(snapshot.CpuScore);
        var cpuCapability = string.IsNullOrWhiteSpace(cpuScore)
            ? "CPU"
            : $"CPU {cpuScore}";
        var gpuScore = FormatCompactCapabilityScore(snapshot.GpuScore);
        if (snapshot.IlgpuCudaAvailable)
        {
            return new WorkerCapabilityChips(
                cpuCapability,
                string.IsNullOrWhiteSpace(gpuScore)
                    ? "CUDA"
                    : $"CUDA {gpuScore}");
        }

        if (snapshot.IlgpuOpenclAvailable)
        {
            return new WorkerCapabilityChips(
                cpuCapability,
                string.IsNullOrWhiteSpace(gpuScore)
                    ? "OpenCL"
                    : $"OpenCL {gpuScore}");
        }

        if (snapshot.HasGpu)
        {
            return new WorkerCapabilityChips(
                cpuCapability,
                string.IsNullOrWhiteSpace(gpuScore)
                    ? string.Empty
                    : $"GPU {gpuScore}");
        }

        return new WorkerCapabilityChips(cpuCapability, string.Empty);
    }

    private static WorkerCapabilityChips DescribeWorkerCapabilityChips(IReadOnlyList<WorkerEndpointSnapshot> snapshots)
    {
        if (snapshots.Count == 1)
        {
            return DescribeWorkerCapabilityChips(snapshots[0]);
        }

        var capableSnapshots = snapshots
            .Where(static snapshot => snapshot.HasCapabilitySnapshot && snapshot.HasCapabilities)
            .ToArray();
        if (capableSnapshots.Length == 0)
        {
            return new WorkerCapabilityChips(string.Empty, string.Empty);
        }

        var cpuScore = FormatCompactCapabilityScore(capableSnapshots.Max(static snapshot => snapshot.CpuScore));
        var cpuCapability = string.IsNullOrWhiteSpace(cpuScore)
            ? "CPU"
            : $"CPU {cpuScore}";

        var cudaSnapshot = capableSnapshots
            .Where(static snapshot => snapshot.IlgpuCudaAvailable)
            .OrderByDescending(static snapshot => snapshot.GpuScore)
            .FirstOrDefault();
        if (cudaSnapshot is not null)
        {
            var gpuScore = FormatCompactCapabilityScore(cudaSnapshot.GpuScore);
            return new WorkerCapabilityChips(
                cpuCapability,
                string.IsNullOrWhiteSpace(gpuScore)
                    ? "CUDA"
                    : $"CUDA {gpuScore}");
        }

        var openClSnapshot = capableSnapshots
            .Where(static snapshot => snapshot.IlgpuOpenclAvailable)
            .OrderByDescending(static snapshot => snapshot.GpuScore)
            .FirstOrDefault();
        if (openClSnapshot is not null)
        {
            var gpuScore = FormatCompactCapabilityScore(openClSnapshot.GpuScore);
            return new WorkerCapabilityChips(
                cpuCapability,
                string.IsNullOrWhiteSpace(gpuScore)
                    ? "OpenCL"
                    : $"OpenCL {gpuScore}");
        }

        var gpuSnapshot = capableSnapshots
            .Where(static snapshot => snapshot.HasGpu)
            .OrderByDescending(static snapshot => snapshot.GpuScore)
            .FirstOrDefault();
        if (gpuSnapshot is not null)
        {
            var gpuScore = FormatCompactCapabilityScore(gpuSnapshot.GpuScore);
            return new WorkerCapabilityChips(
                cpuCapability,
                string.IsNullOrWhiteSpace(gpuScore)
                    ? string.Empty
                    : $"GPU {gpuScore}");
        }

        return new WorkerCapabilityChips(cpuCapability, string.Empty);
    }

    private static string FormatCompactCapabilityScore(float value)
    {
        if (!float.IsFinite(value) || value <= 0f)
        {
            return string.Empty;
        }

        if (value >= 100_000f)
        {
            return $"{MathF.Round(value / 1000f):0}k";
        }

        if (value >= 1_000f)
        {
            return $"{value / 1000f:0.#}k";
        }

        return $"{value:0.#}";
    }

}
