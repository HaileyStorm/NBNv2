using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Runtime.SettingsMonitor;

public sealed partial class SettingsMonitorActor
{
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
        var requestedValue = message.Value ?? string.Empty;
        var task = PersistSettingAsync(message.Key, requestedValue, updatedMs);
        context.ReenterAfter(task, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"SettingSet failed: {completed.Exception?.GetBaseException().Message}");
            }

            var entry = completed.IsCompletedSuccessfully
                ? completed.Result
                : new SettingEntry(message.Key, requestedValue, updatedMs);
            context.Respond(new ProtoSettings.SettingValue
            {
                Key = entry.Key,
                Value = entry.Value,
                UpdatedMs = (ulong)entry.UpdatedMs
            });

            if (!completed.IsFaulted)
            {
                var shouldPublish = ShouldPublishObservedSetting(entry.Key, entry.Value, entry.UpdatedMs);
                RememberObservedSetting(entry.Key, entry.Value, entry.UpdatedMs);
                if (shouldPublish)
                {
                    PublishSettingChanged(context, entry.Key, entry.Value, entry.UpdatedMs);
                }
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

    private void PublishObservedSettingChanges(IContext context, IReadOnlyList<SettingEntry> settings)
    {
        foreach (var entry in settings)
        {
            if (!ShouldPublishObservedSetting(entry.Key, entry.Value, entry.UpdatedMs))
            {
                continue;
            }

            PublishSettingChanged(context, entry.Key, entry.Value, entry.UpdatedMs);
        }

        ObserveSettings(settings);
    }

    private void ObserveSettings(IReadOnlyList<SettingEntry> settings)
    {
        foreach (var entry in settings)
        {
            RememberObservedSetting(entry.Key, entry.Value, entry.UpdatedMs);
        }
    }

    private bool ShouldPublishObservedSetting(string key, string value, long updatedMs)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        if (!_observedSettings.TryGetValue(key, out var existing))
        {
            return true;
        }

        if (updatedMs < existing.UpdatedMs)
        {
            return false;
        }

        return updatedMs != existing.UpdatedMs
               || !string.Equals(value, existing.Value, StringComparison.Ordinal);
    }

    private async Task<SettingEntry> PersistSettingAsync(string key, string value, long updatedMs)
    {
        await _store.SetSettingAsync(key, value, updatedMs).ConfigureAwait(false);
        return await _store.GetSettingAsync(key).ConfigureAwait(false)
               ?? new SettingEntry(key, value, updatedMs);
    }

    private void RememberObservedSetting(string key, string value, long updatedMs)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return;
        }

        if (_observedSettings.TryGetValue(key, out var existing)
            && updatedMs < existing.UpdatedMs)
        {
            return;
        }

        _observedSettings[key] = new ObservedSetting(value, updatedMs);
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

    private sealed record ObservedSetting(string Value, long UpdatedMs);
}
