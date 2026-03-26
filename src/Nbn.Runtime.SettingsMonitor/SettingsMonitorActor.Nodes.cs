using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Runtime.SettingsMonitor;

public sealed partial class SettingsMonitorActor
{
    private void HandleNodeOnline(IContext context, ProtoSettings.NodeOnline message)
    {
        if (!TryGetGuid(message.NodeId, out var nodeId))
        {
            return;
        }

        UpdateNodeSnapshot(nodeId, message.LogicalName, message.Address, message.RootActorName, NowMs(), true);

        var task = _store.UpsertNodeAsync(
            new NodeRegistration(
                nodeId,
                message.LogicalName ?? string.Empty,
                message.Address ?? string.Empty,
                message.RootActorName ?? string.Empty),
            NowMs());

        context.ReenterAfter(task, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"NodeOnline upsert failed: {completed.Exception?.GetBaseException().Message}");
            }

            return Task.CompletedTask;
        });
    }

    private void HandleNodeOffline(IContext context, ProtoSettings.NodeOffline message)
    {
        if (!TryGetGuid(message.NodeId, out var nodeId))
        {
            return;
        }

        UpdateNodeSnapshot(nodeId, message.LogicalName, null, null, NowMs(), false);

        var task = _store.MarkNodeOfflineAsync(nodeId, NowMs());
        context.ReenterAfter(task, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"NodeOffline update failed: {completed.Exception?.GetBaseException().Message}");
            }

            return Task.CompletedTask;
        });
    }

    private void HandleNodeHeartbeat(IContext context, ProtoSettings.NodeHeartbeat message)
    {
        if (!TryGetGuid(message.NodeId, out var nodeId))
        {
            return;
        }

        var caps = message.Caps ?? new ProtoSettings.NodeCapabilities();
        UpdateNodeSnapshot(nodeId, null, null, null, message.TimeMs > 0 ? (long)message.TimeMs : NowMs(), true);
        var heartbeat = new NodeHeartbeat(
            nodeId,
            message.TimeMs > 0 ? (long)message.TimeMs : NowMs(),
            new NodeCapabilities(
                caps.CpuCores,
                (long)caps.RamFreeBytes,
                (long)caps.StorageFreeBytes,
                caps.HasGpu,
                string.IsNullOrWhiteSpace(caps.GpuName) ? null : caps.GpuName,
                (long)caps.VramFreeBytes,
                caps.CpuScore,
                caps.GpuScore,
                caps.IlgpuCudaAvailable,
                caps.IlgpuOpenclAvailable,
                (long)caps.RamTotalBytes,
                (long)caps.StorageTotalBytes,
                (long)caps.VramTotalBytes,
                caps.CpuLimitPercent,
                caps.RamLimitPercent,
                caps.StorageLimitPercent,
                caps.GpuComputeLimitPercent,
                caps.GpuVramLimitPercent,
                caps.ProcessCpuLoadPercent,
                (long)caps.ProcessRamUsedBytes));

        var task = _store.RecordHeartbeatAsync(heartbeat);
        context.ReenterAfter(task, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"NodeHeartbeat update failed: {completed.Exception?.GetBaseException().Message}");
            }

            return Task.CompletedTask;
        });
    }

    private void HandleNodeList(IContext context)
    {
        var task = _store.ListNodesAsync();
        context.ReenterAfter(task, completed =>
        {
            var response = new ProtoSettings.NodeListResponse();
            try
            {
                if (completed.IsFaulted)
                {
                    LogError($"NodeList failed: {completed.Exception?.GetBaseException().Message}");
                }
                else
                {
                    var merged = new Dictionary<Guid, NodeStatus>();
                    foreach (var node in completed.Result)
                    {
                        merged[node.NodeId] = node;
                    }

                    foreach (var snapshot in _nodes.Values)
                    {
                        if (merged.TryGetValue(snapshot.NodeId, out var existing))
                        {
                            existing.LastSeenMs = Math.Max(existing.LastSeenMs, snapshot.LastSeenMs);
                            existing.IsAlive = snapshot.IsAlive;
                            if (!string.IsNullOrWhiteSpace(snapshot.LogicalName))
                            {
                                existing.LogicalName = snapshot.LogicalName;
                            }

                            if (!string.IsNullOrWhiteSpace(snapshot.Address))
                            {
                                existing.Address = snapshot.Address;
                            }

                            if (!string.IsNullOrWhiteSpace(snapshot.RootActorName))
                            {
                                existing.RootActorName = snapshot.RootActorName;
                            }

                            continue;
                        }

                        merged[snapshot.NodeId] = snapshot;
                    }

                    foreach (var node in merged.Values)
                    {
                        response.Nodes.Add(new ProtoSettings.NodeStatus
                        {
                            NodeId = node.NodeId.ToProtoUuid(),
                            LogicalName = node.LogicalName ?? string.Empty,
                            Address = node.Address ?? string.Empty,
                            RootActorName = node.RootActorName ?? string.Empty,
                            LastSeenMs = (ulong)node.LastSeenMs,
                            IsAlive = node.IsAlive
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"NodeList response failed: {ex.GetBaseException().Message}");
            }

            context.Respond(response);
            return Task.CompletedTask;
        });
    }

    private void HandleWorkerInventorySnapshot(IContext context)
    {
        var task = _store.GetWorkerInventorySnapshotAsync();
        context.ReenterAfter(task, completed =>
        {
            var response = new ProtoSettings.WorkerInventorySnapshotResponse
            {
                SnapshotMs = (ulong)NowMs()
            };

            try
            {
                if (completed.IsFaulted)
                {
                    LogError($"WorkerInventorySnapshot failed: {completed.Exception?.GetBaseException().Message}");
                }
                else
                {
                    response.SnapshotMs = (ulong)completed.Result.SnapshotMs;
                    var merged = new Dictionary<Guid, WorkerReadinessCapability>();
                    foreach (var worker in completed.Result.Workers)
                    {
                        merged[worker.NodeId] = worker;
                    }

                    foreach (var snapshot in _nodes.Values)
                    {
                        if (merged.TryGetValue(snapshot.NodeId, out var existing))
                        {
                            existing.LastSeenMs = Math.Max(existing.LastSeenMs, snapshot.LastSeenMs);
                            existing.IsAlive = snapshot.IsAlive;
                            existing.IsReady = existing.IsAlive && existing.HasCapabilities;
                            if (!string.IsNullOrWhiteSpace(snapshot.LogicalName))
                            {
                                existing.LogicalName = snapshot.LogicalName;
                            }

                            if (!string.IsNullOrWhiteSpace(snapshot.Address))
                            {
                                existing.Address = snapshot.Address;
                            }

                            if (!string.IsNullOrWhiteSpace(snapshot.RootActorName))
                            {
                                existing.RootActorName = snapshot.RootActorName;
                            }

                            continue;
                        }

                        merged[snapshot.NodeId] = new WorkerReadinessCapability
                        {
                            NodeId = snapshot.NodeId,
                            LogicalName = snapshot.LogicalName ?? string.Empty,
                            Address = snapshot.Address ?? string.Empty,
                            RootActorName = snapshot.RootActorName ?? string.Empty,
                            IsAlive = snapshot.IsAlive,
                            IsReady = false,
                            LastSeenMs = snapshot.LastSeenMs,
                            HasCapabilities = false,
                            CapabilityTimeMs = 0
                        };
                    }

                    foreach (var worker in merged.Values
                                 .OrderBy(static worker => worker.LogicalName, StringComparer.Ordinal)
                                 .ThenBy(static worker => worker.NodeId))
                    {
                        response.Workers.Add(new ProtoSettings.WorkerReadinessCapability
                        {
                            NodeId = worker.NodeId.ToProtoUuid(),
                            LogicalName = worker.LogicalName ?? string.Empty,
                            Address = worker.Address ?? string.Empty,
                            RootActorName = worker.RootActorName ?? string.Empty,
                            IsAlive = worker.IsAlive,
                            IsReady = worker.IsReady,
                            LastSeenMs = (ulong)worker.LastSeenMs,
                            HasCapabilities = worker.HasCapabilities,
                            CapabilityTimeMs = (ulong)worker.CapabilityTimeMs,
                            Capabilities = new ProtoSettings.NodeCapabilities
                            {
                                CpuCores = worker.CpuCores,
                                RamFreeBytes = (ulong)worker.RamFreeBytes,
                                StorageFreeBytes = (ulong)worker.StorageFreeBytes,
                                HasGpu = worker.HasGpu,
                                GpuName = worker.GpuName ?? string.Empty,
                                VramFreeBytes = (ulong)worker.VramFreeBytes,
                                CpuScore = worker.CpuScore,
                                GpuScore = worker.GpuScore,
                                IlgpuCudaAvailable = worker.IlgpuCudaAvailable,
                                IlgpuOpenclAvailable = worker.IlgpuOpenclAvailable,
                                RamTotalBytes = (ulong)Math.Max(0, worker.RamTotalBytes),
                                StorageTotalBytes = (ulong)Math.Max(0, worker.StorageTotalBytes),
                                VramTotalBytes = (ulong)Math.Max(0, worker.VramTotalBytes),
                                CpuLimitPercent = worker.CpuLimitPercent,
                                RamLimitPercent = worker.RamLimitPercent,
                                StorageLimitPercent = worker.StorageLimitPercent,
                                GpuComputeLimitPercent = worker.GpuComputeLimitPercent,
                                GpuVramLimitPercent = worker.GpuVramLimitPercent,
                                ProcessCpuLoadPercent = worker.ProcessCpuLoadPercent,
                                ProcessRamUsedBytes = (ulong)Math.Max(0, worker.ProcessRamUsedBytes)
                            }
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"WorkerInventorySnapshot response failed: {ex.GetBaseException().Message}");
            }

            context.Respond(response);
            return Task.CompletedTask;
        });
    }

    private void UpdateNodeSnapshot(
        Guid nodeId,
        string? logicalName,
        string? address,
        string? rootActorName,
        long timeMs,
        bool isAlive)
    {
        if (!_nodes.TryGetValue(nodeId, out var snapshot))
        {
            snapshot = new NodeStatus
            {
                NodeId = nodeId,
                LogicalName = logicalName ?? string.Empty,
                Address = address ?? string.Empty,
                RootActorName = rootActorName ?? string.Empty,
                LastSeenMs = timeMs,
                IsAlive = isAlive
            };

            _nodes[nodeId] = snapshot;
            return;
        }

        if (!string.IsNullOrWhiteSpace(logicalName))
        {
            snapshot.LogicalName = logicalName;
        }

        if (!string.IsNullOrWhiteSpace(address))
        {
            snapshot.Address = address;
        }

        if (!string.IsNullOrWhiteSpace(rootActorName))
        {
            snapshot.RootActorName = rootActorName;
        }

        snapshot.LastSeenMs = Math.Max(snapshot.LastSeenMs, timeMs);
        snapshot.IsAlive = isAlive;
    }

    private static bool TryGetGuid(Nbn.Proto.Uuid? uuid, out Guid guid)
    {
        if (uuid is null)
        {
            guid = Guid.Empty;
            return false;
        }

        return uuid.TryToGuid(out guid);
    }

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}
