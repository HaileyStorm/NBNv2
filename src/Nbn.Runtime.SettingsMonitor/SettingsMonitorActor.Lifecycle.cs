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
}
