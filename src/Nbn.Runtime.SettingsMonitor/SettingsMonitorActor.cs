using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Runtime.SettingsMonitor;

public sealed class SettingsMonitorActor : IActor
{
    private readonly SettingsMonitorStore _store;

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
            case ProtoSettings.SettingGet message:
                HandleSettingGet(context, message);
                break;
            case ProtoSettings.SettingSet message:
                HandleSettingSet(context, message);
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

            return Task.CompletedTask;
        });
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
}
