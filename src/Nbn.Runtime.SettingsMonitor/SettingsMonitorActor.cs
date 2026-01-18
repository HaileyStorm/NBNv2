using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Runtime.SettingsMonitor;

public sealed class SettingsMonitorActor : IActor
{
    private readonly SettingsMonitorStore _store;
    private readonly Dictionary<string, PID> _subscribers = new(StringComparer.Ordinal);

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
            foreach (var node in completed.Result)
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
            foreach (var entry in completed.Result)
            {
                response.Settings.Add(new ProtoSettings.SettingValue
                {
                    Key = entry.Key,
                    Value = entry.Value,
                    UpdatedMs = (ulong)entry.UpdatedMs
                });
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

            context.Respond(response);
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
