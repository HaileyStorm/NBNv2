using Nbn.Proto.Settings;
using Proto;

namespace Nbn.Shared;

/// <summary>
/// Publishes, resolves, and subscribes to canonical service endpoint settings.
/// </summary>
public sealed class ServiceEndpointDiscoveryClient : IAsyncDisposable
{
    private static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(5);

    private readonly ActorSystem _system;
    private readonly PID _settingsPid;
    private readonly object _gate = new();

    private PID? _subscriberPid;
    private HashSet<string>? _watchedKeys;

    /// <summary>
    /// Creates a discovery client bound to a SettingsMonitor actor PID.
    /// </summary>
    public ServiceEndpointDiscoveryClient(ActorSystem system, PID settingsPid)
    {
        _system = system ?? throw new ArgumentNullException(nameof(system));
        _settingsPid = settingsPid ?? throw new ArgumentNullException(nameof(settingsPid));
    }

    /// <summary>
    /// Raised when a watched setting resolves to a valid endpoint registration.
    /// </summary>
    public event Action<ServiceEndpointRegistration>? EndpointChanged;

    /// <summary>
    /// Raised for all watched setting updates, including removals and parse failures.
    /// </summary>
    public event Action<ServiceEndpointObservation>? EndpointObserved;

    /// <summary>
    /// Creates a discovery client when the SettingsMonitor location is complete.
    /// </summary>
    public static ServiceEndpointDiscoveryClient? Create(
        ActorSystem? system,
        string? settingsHost,
        int settingsPort,
        string? settingsName)
    {
        if (system is null)
        {
            return null;
        }

        var settingsPid = CreateSettingsPid(settingsHost, settingsPort, settingsName);
        if (settingsPid is null)
        {
            return null;
        }

        return new ServiceEndpointDiscoveryClient(system, settingsPid);
    }

    /// <summary>
    /// Attempts to publish a canonical service endpoint registration.
    /// </summary>
    public static async Task<bool> TryPublishAsync(
        ActorSystem? system,
        string? settingsHost,
        int settingsPort,
        string? settingsName,
        string settingKey,
        ServiceEndpoint endpoint,
        CancellationToken cancellationToken = default)
    {
        var client = Create(system, settingsHost, settingsPort, settingsName);
        if (client is null)
        {
            return false;
        }

        try
        {
            await client.PublishAsync(settingKey, endpoint, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Publishes the supplied endpoint under a canonical settings key.
    /// </summary>
    public async Task PublishAsync(
        string settingKey,
        ServiceEndpoint endpoint,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(settingKey))
        {
            throw new ArgumentException("Setting key is required.", nameof(settingKey));
        }

        await RequestSettingsAsync<SettingValue>(
            new SettingSet
            {
                Key = settingKey.Trim(),
                Value = endpoint.ToSettingValue()
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves a single service endpoint registration by key.
    /// </summary>
    public async Task<ServiceEndpointRegistration?> ResolveAsync(
        string? settingKey,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(settingKey))
        {
            return null;
        }

        var settingValue = await RequestSettingsAsync<SettingValue>(
            new SettingGet
            {
                Key = settingKey.Trim()
            },
            cancellationToken).ConfigureAwait(false);

        return ServiceEndpointSettings.TryParseSetting(
            settingValue.Key,
            settingValue.Value,
            settingValue.UpdatedMs,
            out var registration)
            ? registration
            : null;
    }

    /// <summary>
    /// Resolves all known service endpoint registrations returned by a list request.
    /// </summary>
    public async Task<IReadOnlyDictionary<string, ServiceEndpointRegistration>> ResolveFromListAsync(
        CancellationToken cancellationToken = default)
    {
        var response = await RequestSettingsAsync<SettingListResponse>(
            new SettingListRequest(),
            cancellationToken).ConfigureAwait(false);

        var resolved = new Dictionary<string, ServiceEndpointRegistration>(StringComparer.Ordinal);
        foreach (var setting in response.Settings)
        {
            if (!ServiceEndpointSettings.IsKnownKey(setting.Key))
            {
                continue;
            }

            if (!ServiceEndpointSettings.TryParseSetting(setting.Key, setting.Value, setting.UpdatedMs, out var registration))
            {
                continue;
            }

            resolved[registration.Key] = registration;
        }

        return resolved;
    }

    /// <summary>
    /// Resolves the current registration for every canonical service endpoint key.
    /// </summary>
    public async Task<IReadOnlyDictionary<string, ServiceEndpointRegistration>> ResolveKnownAsync(
        CancellationToken cancellationToken = default)
    {
        var resolved = new Dictionary<string, ServiceEndpointRegistration>(StringComparer.Ordinal);
        foreach (var entry in await ResolveFromListAsync(cancellationToken).ConfigureAwait(false))
        {
            resolved[entry.Key] = entry.Value;
        }

        foreach (var key in ServiceEndpointSettings.AllKeys)
        {
            if (resolved.ContainsKey(key))
            {
                continue;
            }

            var registration = await ResolveAsync(key, cancellationToken).ConfigureAwait(false);
            if (registration is not null)
            {
                resolved[key] = registration.Value;
            }
        }

        return resolved;
    }

    /// <summary>
    /// Subscribes to updates for the supplied keys, or all canonical keys when none are provided.
    /// </summary>
    public async Task SubscribeAsync(
        IEnumerable<string>? keys = null,
        CancellationToken cancellationToken = default)
    {
        var keyFilter = BuildKeyFilter(keys);
        var subscriberPid = EnsureSubscriber(keyFilter);

        await EnsureSettingsReachableAsync(cancellationToken).ConfigureAwait(false);

        _system.Root.Send(_settingsPid, new SettingSubscribe
        {
            SubscriberActor = PidLabel(subscriberPid)
        });
    }

    /// <summary>
    /// Stops watching for endpoint updates and tears down the subscriber actor.
    /// </summary>
    public Task UnsubscribeAsync()
    {
        PID? subscriberPid;
        lock (_gate)
        {
            subscriberPid = _subscriberPid;
            _subscriberPid = null;
            _watchedKeys = null;
        }

        if (subscriberPid is null)
        {
            return Task.CompletedTask;
        }

        _system.Root.Send(_settingsPid, new SettingUnsubscribe
        {
            SubscriberActor = PidLabel(subscriberPid)
        });

        _system.Root.Stop(subscriberPid);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Unsubscribes from endpoint updates and releases the relay actor.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await UnsubscribeAsync().ConfigureAwait(false);
    }

    private static PID? CreateSettingsPid(string? settingsHost, int settingsPort, string? settingsName)
    {
        if (string.IsNullOrWhiteSpace(settingsHost) || settingsPort <= 0 || string.IsNullOrWhiteSpace(settingsName))
        {
            return null;
        }

        return new PID($"{settingsHost}:{settingsPort}", settingsName);
    }

    private Task EnsureSettingsReachableAsync(CancellationToken cancellationToken)
        => RequestSettingsAsync<SettingValue>(
            new SettingGet
            {
                Key = ServiceEndpointSettings.HiveMindKey
            },
            cancellationToken);

    private async Task<TResponse> RequestSettingsAsync<TResponse>(
        object request,
        CancellationToken cancellationToken = default)
    {
        using var timeoutCts = CreateTimeoutToken(cancellationToken);
        return await _system.Root.RequestAsync<TResponse>(
            _settingsPid,
            request,
            timeoutCts.Token).ConfigureAwait(false);
    }

    private PID EnsureSubscriber(HashSet<string> keyFilter)
    {
        lock (_gate)
        {
            _watchedKeys = keyFilter;
            if (_subscriberPid is not null)
            {
                return _subscriberPid;
            }

            _subscriberPid = _system.Root.Spawn(
                Props.FromProducer(() => new SettingChangedRelayActor(HandleSettingChanged)));

            return _subscriberPid;
        }
    }

    private void HandleSettingChanged(SettingChanged changed)
    {
        if (changed is null)
        {
            return;
        }

        Action<ServiceEndpointRegistration>? callback;
        Action<ServiceEndpointObservation>? observationCallback;
        HashSet<string>? watchedKeys;
        lock (_gate)
        {
            callback = EndpointChanged;
            observationCallback = EndpointObserved;
            watchedKeys = _watchedKeys;
        }

        if (watchedKeys is null || !watchedKeys.Contains(changed.Key))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(changed.Key))
        {
            NotifyEndpointObserved(
                observationCallback,
                new ServiceEndpointObservation(
                    string.Empty,
                    ServiceEndpointObservationKind.Invalid,
                    null,
                    "unknown_key",
                    changed.UpdatedMs));
            return;
        }

        if (!ServiceEndpointSettings.TryParseSetting(changed.Key, changed.Value, changed.UpdatedMs, out var registration))
        {
            var kind = string.IsNullOrWhiteSpace(changed.Value)
                ? ServiceEndpointObservationKind.Removed
                : ServiceEndpointObservationKind.Invalid;
            var failureReason = kind == ServiceEndpointObservationKind.Removed
                ? "endpoint_removed"
                : "endpoint_parse_failed";
            NotifyEndpointObserved(
                observationCallback,
                new ServiceEndpointObservation(
                    changed.Key.Trim(),
                    kind,
                    null,
                    failureReason,
                    changed.UpdatedMs));
            return;
        }

        if (callback is not null)
        {
            try
            {
                callback(registration);
            }
            catch
            {
            }
        }

        NotifyEndpointObserved(
            observationCallback,
            new ServiceEndpointObservation(
                registration.Key,
                ServiceEndpointObservationKind.Upserted,
                registration,
                "none",
                changed.UpdatedMs));
    }

    private static HashSet<string> BuildKeyFilter(IEnumerable<string>? keys)
    {
        var keyFilter = new HashSet<string>(StringComparer.Ordinal);
        if (keys is not null)
        {
            foreach (var key in keys)
            {
                if (string.IsNullOrWhiteSpace(key))
                {
                    continue;
                }

                keyFilter.Add(key.Trim());
            }
        }

        if (keyFilter.Count == 0)
        {
            foreach (var key in ServiceEndpointSettings.AllKeys)
            {
                keyFilter.Add(key);
            }
        }

        return keyFilter;
    }

    private string PidLabel(PID pid)
    {
        var address = pid.Address;
        if (string.IsNullOrWhiteSpace(address))
        {
            address = _system.Address;
        }

        return string.IsNullOrWhiteSpace(address) ? pid.Id : $"{address}/{pid.Id}";
    }

    private static CancellationTokenSource CreateTimeoutToken(CancellationToken cancellationToken)
    {
        var timeoutCts = cancellationToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : new CancellationTokenSource();

        timeoutCts.CancelAfter(DefaultRequestTimeout);
        return timeoutCts;
    }

    private static void NotifyEndpointObserved(
        Action<ServiceEndpointObservation>? callback,
        ServiceEndpointObservation observation)
    {
        if (callback is null)
        {
            return;
        }

        try
        {
            callback(observation);
        }
        catch
        {
        }
    }

    private sealed class SettingChangedRelayActor : IActor
    {
        private readonly Action<SettingChanged> _onChanged;

        public SettingChangedRelayActor(Action<SettingChanged> onChanged)
        {
            _onChanged = onChanged ?? throw new ArgumentNullException(nameof(onChanged));
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is SettingChanged changed)
            {
                _onChanged(changed);
            }

            return Task.CompletedTask;
        }
    }
}
