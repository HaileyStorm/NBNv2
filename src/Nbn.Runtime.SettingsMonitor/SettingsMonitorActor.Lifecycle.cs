using System;
using Proto;

namespace Nbn.Runtime.SettingsMonitor;

public sealed partial class SettingsMonitorActor
{
    private void Initialize(IContext context)
    {
        var task = InitializeSettingsAsync();
        context.ReenterAfter(task, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"Failed to initialize default settings: {completed.Exception?.GetBaseException().Message}");
            }

            ScheduleSelf(context, _externalSettingsPollInterval, PollExternalSettings.Instance);
            ScheduleSelf(context, TimeSpan.Zero, PruneStaleDeadBrains.Instance);
            return Task.CompletedTask;
        });
    }

    private async Task InitializeSettingsAsync()
    {
        await _store.EnsureDefaultSettingsAsync().ConfigureAwait(false);
        var settings = await _store.ListSettingsAsync().ConfigureAwait(false);
        ObserveSettings(settings);
    }

    private void HandlePollExternalSettings(IContext context)
    {
        if (_externalSettingsPollInFlight)
        {
            ScheduleSelf(context, _externalSettingsPollInterval, PollExternalSettings.Instance);
            return;
        }

        _externalSettingsPollInFlight = true;
        var task = _store.ListSettingsAsync();
        context.ReenterAfter(task, completed =>
        {
            try
            {
                if (completed.IsFaulted)
                {
                    LogError($"External settings poll failed: {completed.Exception?.GetBaseException().Message}");
                }
                else
                {
                    PublishObservedSettingChanges(context, completed.Result);
                }
            }
            finally
            {
                _externalSettingsPollInFlight = false;
                ScheduleSelf(context, _externalSettingsPollInterval, PollExternalSettings.Instance);
            }

            return Task.CompletedTask;
        });
    }

    private void HandlePruneStaleDeadBrains(IContext context)
    {
        if (_staleDeadBrainPruneInFlight)
        {
            ScheduleSelf(context, _staleDeadBrainPruneInterval, PruneStaleDeadBrains.Instance);
            return;
        }

        _staleDeadBrainPruneInFlight = true;
        var nowMs = NowMs();
        var deadCutoffMs = nowMs - checked((long)_staleDeadBrainRetention.TotalMilliseconds);
        var nonLiveCutoffMs = nowMs - checked((long)_staleNonLiveBrainRetention.TotalMilliseconds);
        var capabilityCutoffMs = nowMs - checked((long)_nodeCapabilityRetention.TotalMilliseconds);
        var task = PruneStaleRowsAsync(deadCutoffMs, nonLiveCutoffMs, capabilityCutoffMs);
        context.ReenterAfter(task, completed =>
        {
            try
            {
                if (completed.IsFaulted)
                {
                    LogError($"Brain-row prune failed: {completed.Exception?.GetBaseException().Message}");
                }
                else
                {
                    if (completed.Result.DeadRows.DeletedBrains > 0 || completed.Result.DeadRows.DeletedControllers > 0)
                    {
                        Console.WriteLine(
                            $"[{DateTime.UtcNow:O}] [SettingsMonitor] Pruned stale dead brain rows: {completed.Result.DeadRows.DeletedBrains} brains, {completed.Result.DeadRows.DeletedControllers} controllers.");
                    }

                    if (completed.Result.NonLiveRows.DeletedBrains > 0 || completed.Result.NonLiveRows.DeletedControllers > 0)
                    {
                        Console.WriteLine(
                            $"[{DateTime.UtcNow:O}] [SettingsMonitor] Pruned stale non-live brain rows: {completed.Result.NonLiveRows.DeletedBrains} brains, {completed.Result.NonLiveRows.DeletedControllers} controllers.");
                    }

                    if (completed.Result.DeletedCapabilityRows > 0)
                    {
                        Console.WriteLine(
                            $"[{DateTime.UtcNow:O}] [SettingsMonitor] Pruned stale node capability rows: {completed.Result.DeletedCapabilityRows} rows.");
                    }
                }
            }
            finally
            {
                _staleDeadBrainPruneInFlight = false;
                ScheduleSelf(context, _staleDeadBrainPruneInterval, PruneStaleDeadBrains.Instance);
            }

            return Task.CompletedTask;
        });
    }

    private async Task<PrunedRetentionCleanup> PruneStaleRowsAsync(
        long deadCutoffMs,
        long nonLiveCutoffMs,
        long capabilityCutoffMs)
    {
        var deadRows = await _store.PruneStaleDeadBrainsAsync(deadCutoffMs).ConfigureAwait(false);
        var nonLiveRows = await _store.PruneStaleNonLiveBrainsAsync(nonLiveCutoffMs).ConfigureAwait(false);
        var capabilityRows = await _store.PruneStaleNodeCapabilitiesAsync(capabilityCutoffMs).ConfigureAwait(false);
        await _store.RunStorageMaintenanceAsync().ConfigureAwait(false);
        return new PrunedRetentionCleanup(deadRows, nonLiveRows, capabilityRows);
    }

    private static void ScheduleSelf(IContext context, TimeSpan delay, object message)
    {
        if (delay <= TimeSpan.Zero)
        {
            context.Send(context.Self, message);
            return;
        }

        context.ReenterAfter(Task.Delay(delay), _ =>
        {
            context.Send(context.Self, message);
            return Task.CompletedTask;
        });
    }

    private static void LogError(string message)
        => Console.WriteLine($"[{DateTime.UtcNow:O}] [SettingsMonitor][ERROR] {message}");

    private sealed record PollExternalSettings
    {
        public static readonly PollExternalSettings Instance = new();
    }

    private sealed record PruneStaleDeadBrains
    {
        public static readonly PruneStaleDeadBrains Instance = new();
    }

    private sealed record PrunedRetentionCleanup(
        SettingsMonitorStore.PrunedBrainRows DeadRows,
        SettingsMonitorStore.PrunedBrainRows NonLiveRows,
        int DeletedCapabilityRows);
}
