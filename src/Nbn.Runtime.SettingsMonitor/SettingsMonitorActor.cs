using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Runtime.SettingsMonitor;

public sealed class SettingsMonitorActor : IActor
{
    private readonly SettingsMonitorStore _store;
    private readonly Dictionary<string, PID> _subscribers = new(StringComparer.Ordinal);
    private readonly Dictionary<Guid, NodeStatus> _nodes = new();

    public SettingsMonitorActor(SettingsMonitorStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
    }

    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
                Initialize(context);
                break;
            case ProtoSettings.NodeOnline message:
                HandleNodeOnline(context, message);
                break;
            case ProtoSettings.NodeOffline message:
                HandleNodeOffline(context, message);
                break;
            case ProtoSettings.NodeHeartbeat message:
                HandleNodeHeartbeat(context, message);
                break;
            case ProtoSettings.NodeListRequest:
                HandleNodeList(context);
                break;
            case ProtoSettings.WorkerInventorySnapshotRequest:
                HandleWorkerInventorySnapshot(context);
                break;
            case ProtoSettings.SettingGet message:
                HandleSettingGet(context, message);
                break;
            case ProtoSettings.SettingSet message:
                HandleSettingSet(context, message);
                break;
            case ProtoSettings.BrainListRequest:
                HandleBrainList(context);
                break;
            case ProtoSettings.SettingListRequest:
                HandleSettingList(context);
                break;
            case ProtoSettings.BrainRegistered message:
                HandleBrainRegistered(context, message);
                break;
            case ProtoSettings.BrainStateChanged message:
                HandleBrainStateChanged(context, message);
                break;
            case ProtoSettings.BrainTick message:
                HandleBrainTick(context, message);
                break;
            case ProtoSettings.BrainControllerHeartbeat message:
                HandleBrainControllerHeartbeat(context, message);
                break;
            case ProtoSettings.BrainUnregistered message:
                HandleBrainUnregistered(context, message);
                break;
            case ProtoSettings.SettingSubscribe subscribe:
                HandleSettingSubscribe(context, subscribe);
                break;
            case ProtoSettings.SettingUnsubscribe unsubscribe:
                HandleSettingUnsubscribe(context, unsubscribe);
                break;
            case Terminated terminated:
                HandleTerminated(terminated);
                break;
        }

        return Task.CompletedTask;
    }

    private void Initialize(IContext context)
    {
        var task = _store.EnsureDefaultSettingsAsync();
        context.ReenterAfter(task, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"Failed to initialize default settings: {completed.Exception?.GetBaseException().Message}");
            }

            return Task.CompletedTask;
        });
    }

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
        var heartbeat = new Nbn.Runtime.SettingsMonitor.NodeHeartbeat(
            nodeId,
            message.TimeMs > 0 ? (long)message.TimeMs : NowMs(),
            new Nbn.Runtime.SettingsMonitor.NodeCapabilities(
                caps.CpuCores,
                (long)caps.RamFreeBytes,
                caps.HasGpu,
                string.IsNullOrWhiteSpace(caps.GpuName) ? null : caps.GpuName,
                (long)caps.VramFreeBytes,
                caps.CpuScore,
                caps.GpuScore,
                caps.IlgpuCudaAvailable,
                caps.IlgpuOpenclAvailable));

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
                                HasGpu = worker.HasGpu,
                                GpuName = worker.GpuName ?? string.Empty,
                                VramFreeBytes = (ulong)worker.VramFreeBytes,
                                CpuScore = worker.CpuScore,
                                GpuScore = worker.GpuScore,
                                IlgpuCudaAvailable = worker.IlgpuCudaAvailable,
                                IlgpuOpenclAvailable = worker.IlgpuOpenclAvailable
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

    private void HandleSettingGet(IContext context, ProtoSettings.SettingGet message)
    {
        if (string.IsNullOrWhiteSpace(message.Key))
        {
            return;
        }

        var task = _store.GetSettingAsync(message.Key);
        context.ReenterAfter(task, completed =>
        {
            var entry = completed.Result;
            var updatedMs = entry?.UpdatedMs ?? 0;
            var value = entry?.Value ?? string.Empty;

            context.Respond(new ProtoSettings.SettingValue
            {
                Key = message.Key,
                Value = value,
                UpdatedMs = (ulong)updatedMs
            });

            return Task.CompletedTask;
        });
    }

    private void HandleSettingSet(IContext context, ProtoSettings.SettingSet message)
    {
        if (string.IsNullOrWhiteSpace(message.Key))
        {
            return;
        }

        var updatedMs = NowMs();
        var task = _store.SetSettingAsync(message.Key, message.Value ?? string.Empty, updatedMs);
        context.ReenterAfter(task, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"SettingSet failed: {completed.Exception?.GetBaseException().Message}");
            }

            context.Respond(new ProtoSettings.SettingValue
            {
                Key = message.Key,
                Value = message.Value ?? string.Empty,
                UpdatedMs = (ulong)updatedMs
            });

            if (!completed.IsFaulted)
            {
                PublishSettingChanged(context, message.Key, message.Value ?? string.Empty, updatedMs);
            }

            return Task.CompletedTask;
        });
    }

    private void HandleSettingList(IContext context)
    {
        var task = _store.ListSettingsAsync();
        context.ReenterAfter(task, completed =>
        {
            var response = new ProtoSettings.SettingListResponse();
            try
            {
                if (completed.IsFaulted)
                {
                    LogError($"SettingList failed: {completed.Exception?.GetBaseException().Message}");
                }
                else
                {
                    foreach (var entry in completed.Result)
                    {
                        response.Settings.Add(new ProtoSettings.SettingValue
                        {
                            Key = entry.Key,
                            Value = entry.Value,
                            UpdatedMs = (ulong)entry.UpdatedMs
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"SettingList response failed: {ex.GetBaseException().Message}");
            }

            context.Respond(response);
            return Task.CompletedTask;
        });
    }

    private void HandleBrainList(IContext context)
    {
        var brainsTask = _store.ListBrainsAsync();
        var controllersTask = _store.ListBrainControllersAsync();

        context.ReenterAfter(Task.WhenAll(brainsTask, controllersTask), completed =>
        {
            var response = new ProtoSettings.BrainListResponse();
            try
            {
                if (completed.IsFaulted)
                {
                    LogError($"BrainList failed: {completed.Exception?.GetBaseException().Message}");
                }
                else
                {
                    foreach (var brain in brainsTask.Result)
                    {
                        response.Brains.Add(new ProtoSettings.BrainStatus
                        {
                            BrainId = brain.BrainId.ToProtoUuid(),
                            SpawnedMs = (ulong)brain.SpawnedMs,
                            LastTickId = (ulong)brain.LastTickId,
                            State = brain.State ?? string.Empty
                        });
                    }

                    foreach (var controller in controllersTask.Result)
                    {
                        response.Controllers.Add(new ProtoSettings.BrainControllerStatus
                        {
                            BrainId = controller.BrainId.ToProtoUuid(),
                            NodeId = controller.NodeId.ToProtoUuid(),
                            ActorName = controller.ActorName ?? string.Empty,
                            LastSeenMs = (ulong)controller.LastSeenMs,
                            IsAlive = controller.IsAlive
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"BrainList response failed: {ex.GetBaseException().Message}");
            }

            context.Respond(response);
            return Task.CompletedTask;
        });
    }

    private void HandleBrainRegistered(IContext context, ProtoSettings.BrainRegistered message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        var state = string.IsNullOrWhiteSpace(message.State) ? "Active" : message.State!;
        var spawnedMs = message.SpawnedMs > 0 ? (long)message.SpawnedMs : NowMs();
        var lastTickId = message.LastTickId > 0 ? (long)message.LastTickId : 0;

        var nodeId = Guid.Empty;
        if (TryGetGuid(message.ControllerNodeId, out var parsedNodeId))
        {
            nodeId = parsedNodeId;
        }
        else if (!string.IsNullOrWhiteSpace(message.ControllerNodeAddress))
        {
            nodeId = NodeIdentity.DeriveNodeId(message.ControllerNodeAddress);
        }

        var nodeAddress = message.ControllerNodeAddress ?? string.Empty;
        var nodeLogicalName = !string.IsNullOrWhiteSpace(message.ControllerNodeLogicalName)
            ? message.ControllerNodeLogicalName!
            : nodeAddress;
        var rootActorName = !string.IsNullOrWhiteSpace(message.ControllerRootActorName)
            ? message.ControllerRootActorName!
            : message.ControllerActorName ?? string.Empty;
        var actorName = message.ControllerActorName ?? string.Empty;

        var tasks = new List<Task>(3)
        {
            _store.UpsertBrainAsync(brainId, state, spawnedMs, lastTickId)
        };

        if (nodeId != Guid.Empty && !string.IsNullOrWhiteSpace(nodeAddress))
        {
            tasks.Add(_store.UpsertNodeAsync(new NodeRegistration(nodeId, nodeLogicalName, nodeAddress, rootActorName), NowMs()));
            UpdateNodeSnapshot(nodeId, nodeLogicalName, nodeAddress, rootActorName, NowMs(), true);
        }

        if (nodeId != Guid.Empty && !string.IsNullOrWhiteSpace(actorName))
        {
            tasks.Add(_store.UpsertBrainControllerAsync(
                new BrainControllerRegistration(brainId, nodeId, actorName),
                NowMs()));
        }

        context.ReenterAfter(Task.WhenAll(tasks), completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"BrainRegistered update failed: {completed.Exception?.GetBaseException().Message}");
            }

            return Task.CompletedTask;
        });
    }

    private void HandleBrainStateChanged(IContext context, ProtoSettings.BrainStateChanged message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(message.State))
        {
            return;
        }

        var task = _store.UpdateBrainStateAsync(brainId, message.State!, message.Notes);
        context.ReenterAfter(task, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"BrainStateChanged update failed: {completed.Exception?.GetBaseException().Message}");
            }

            return Task.CompletedTask;
        });
    }

    private void HandleBrainTick(IContext context, ProtoSettings.BrainTick message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        var task = _store.UpdateBrainTickAsync(brainId, (long)message.LastTickId);
        context.ReenterAfter(task, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"BrainTick update failed: {completed.Exception?.GetBaseException().Message}");
            }

            return Task.CompletedTask;
        });
    }

    private void HandleBrainControllerHeartbeat(IContext context, ProtoSettings.BrainControllerHeartbeat message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        var timeMs = message.TimeMs > 0 ? (long)message.TimeMs : NowMs();
        var task = _store.RecordBrainControllerHeartbeatAsync(
            new Nbn.Runtime.SettingsMonitor.BrainControllerHeartbeat(brainId, timeMs));
        context.ReenterAfter(task, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"BrainControllerHeartbeat update failed: {completed.Exception?.GetBaseException().Message}");
            }

            return Task.CompletedTask;
        });
    }

    private void HandleBrainUnregistered(IContext context, ProtoSettings.BrainUnregistered message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        var timeMs = message.TimeMs > 0 ? (long)message.TimeMs : NowMs();
        var updateTask = _store.UpdateBrainStateAsync(brainId, "Dead");
        var controllerTask = _store.MarkBrainControllerOfflineAsync(brainId, timeMs);

        context.ReenterAfter(Task.WhenAll(updateTask, controllerTask), completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"BrainUnregistered update failed: {completed.Exception?.GetBaseException().Message}");
            }

            return Task.CompletedTask;
        });
    }

    private void HandleSettingSubscribe(IContext context, ProtoSettings.SettingSubscribe subscribe)
    {
        if (!TryParsePid(subscribe.SubscriberActor, out var pid))
        {
            return;
        }

        var key = PidKey(pid);
        if (_subscribers.TryAdd(key, pid))
        {
            context.Watch(pid);
        }
        else
        {
            _subscribers[key] = pid;
        }
    }

    private void HandleSettingUnsubscribe(IContext context, ProtoSettings.SettingUnsubscribe unsubscribe)
    {
        if (!TryParsePid(unsubscribe.SubscriberActor, out var pid))
        {
            return;
        }

        var key = PidKey(pid);
        if (_subscribers.Remove(key))
        {
            context.Unwatch(pid);
        }
    }

    private void PublishSettingChanged(IContext context, string key, string value, long updatedMs)
    {
        if (_subscribers.Count == 0)
        {
            return;
        }

        var message = new ProtoSettings.SettingChanged
        {
            Key = key,
            Value = value,
            UpdatedMs = (ulong)updatedMs
        };

        foreach (var subscriber in _subscribers.Values)
        {
            context.Send(subscriber, message);
        }
    }

    private void HandleTerminated(Terminated terminated)
    {
        var key = PidKey(terminated.Who);
        _subscribers.Remove(key);
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

    private static void LogError(string message)
        => Console.WriteLine($"[{DateTime.UtcNow:O}] [SettingsMonitor][ERROR] {message}");

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

    private static bool TryParsePid(string? value, out PID pid)
    {
        pid = new PID();
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        var slashIndex = trimmed.IndexOf('/');
        if (slashIndex <= 0)
        {
            pid.Id = trimmed;
            return true;
        }

        var address = trimmed[..slashIndex];
        var id = trimmed[(slashIndex + 1)..];

        if (string.IsNullOrWhiteSpace(id))
        {
            return false;
        }

        pid.Address = address;
        pid.Id = id;
        return true;
    }

    private static string PidKey(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";
}
