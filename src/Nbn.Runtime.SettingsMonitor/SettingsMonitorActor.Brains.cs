using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Runtime.SettingsMonitor;

public sealed partial class SettingsMonitorActor
{
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
            _store.UpsertBrainAsync(brainId, state, spawnedMs, lastTickId, updatedMs: spawnedMs)
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
            new BrainControllerHeartbeat(brainId, timeMs));
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
        var updateTask = _store.UpdateBrainStateAsync(brainId, "Dead", updatedMs: timeMs);
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
}
