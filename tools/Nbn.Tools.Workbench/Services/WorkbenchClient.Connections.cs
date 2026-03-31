using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Proto.Speciation;
using Nbn.Proto.Settings;
using Nbn.Proto.Viz;
using Nbn.Proto.Control;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

namespace Nbn.Tools.Workbench.Services;

public partial class WorkbenchClient
{
    private static readonly TimeSpan RemoteShutdownTimeout = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan ActorSystemShutdownTimeout = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Starts or restarts the local receiver actor system when the bind or advertised endpoint changes.
    /// </summary>
    public async Task EnsureStartedAsync(string bindHost, int port, string? advertisedHost = null, int? advertisedPort = null)
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            var normalizedAdvertisedHost = string.IsNullOrWhiteSpace(advertisedHost)
                ? null
                : advertisedHost.Trim();
            if (_system is not null
                && string.Equals(_bindHost, bindHost, StringComparison.OrdinalIgnoreCase)
                && _bindPort == port
                && string.Equals(_advertisedHost, normalizedAdvertisedHost, StringComparison.OrdinalIgnoreCase)
                && _advertisedPort == advertisedPort)
            {
                return;
            }

            await StopAsync().ConfigureAwait(false);

            _bindHost = bindHost;
            _bindPort = port;
            _advertisedHost = normalizedAdvertisedHost;
            _advertisedPort = advertisedPort;

            var system = new ActorSystem();
            var remoteConfig = WorkbenchRemote.BuildConfig(bindHost, port, normalizedAdvertisedHost, advertisedPort);
            system.WithRemote(remoteConfig);
            await system.Remote().StartAsync().ConfigureAwait(false);

            var receiverPid = system.Root.SpawnNamed(
                Props.FromProducer(() => new WorkbenchReceiverActor(_sink)),
                "workbench-receiver");

            _system = system;
            _root = system.Root;
            _receiverPid = receiverPid;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Connects the Workbench receiver to the configured IO gateway endpoint.
    /// </summary>
    public async Task<ConnectAck?> ConnectIoAsync(string host, int port, string gatewayName, string clientName)
    {
        if (_root is null)
        {
            return null;
        }

        var pid = new PID($"{host}:{port}", gatewayName);
        try
        {
            var ack = await _root.RequestAsync<ConnectAck>(pid, new Connect { ClientName = clientName }, DefaultTimeout)
                .ConfigureAwait(false);
            _ioGatewayPid = pid;
            if (_receiverPid is not null)
            {
                _root.Send(_receiverPid, new SetIoGatewayPid(pid));
            }

            _sink.OnIoStatus($"Connected to {host}:{port}", true);
            return ack;
        }
        catch (Exception ex)
        {
            _sink.OnIoStatus($"IO connect failed: {ex.Message}", false);
            return null;
        }
    }

    /// <summary>
    /// Drops the current IO gateway connection and clears the receiver routing target.
    /// </summary>
    public void DisconnectIo()
    {
        _ioGatewayPid = null;
        if (_receiverPid is not null)
        {
            _root?.Send(_receiverPid, new SetIoGatewayPid(null));
        }

        _sink.OnIoStatus("Disconnected", false);
    }

    /// <summary>
    /// Subscribes the Workbench receiver to SettingsMonitor updates and optionally verifies reachability.
    /// </summary>
    public async Task<bool> ConnectSettingsAsync(string host, int port, string actorName, bool verify = false)
    {
        if (_root is null || _receiverPid is null)
        {
            _sink.OnSettingsStatus("Settings client not initialized.", false);
            return false;
        }

        _settingsPid = new PID($"{host}:{port}", actorName);
        var subscriber = PidLabel(_receiverPid);
        _root.Send(_settingsPid, new SettingSubscribe { SubscriberActor = subscriber });

        if (!verify)
        {
            _sink.OnSettingsStatus($"Subscribed to {host}:{port}", true);
            return true;
        }

        try
        {
            await _root.RequestAsync<NodeListResponse>(_settingsPid, new NodeListRequest(), DefaultTimeout)
                .ConfigureAwait(false);
            _sink.OnSettingsStatus($"Connected to {host}:{port}", true);
            return true;
        }
        catch (Exception ex)
        {
            _settingsPid = null;
            _sink.OnSettingsStatus($"Settings connect failed: {ex.Message}", false);
            return false;
        }
    }

    /// <summary>
    /// Connects to HiveMind and restores the active visualization scope if one was already selected.
    /// </summary>
    public virtual async Task<Nbn.Proto.Control.HiveMindStatus?> ConnectHiveMindAsync(string host, int port, string actorName)
    {
        if (_root is null)
        {
            return null;
        }

        var pid = new PID($"{host}:{port}", actorName);
        try
        {
            var status = await _root.RequestAsync<Nbn.Proto.Control.HiveMindStatus>(
                    pid,
                    new Nbn.Proto.Control.GetHiveMindStatus(),
                    DefaultTimeout)
                .ConfigureAwait(false);
            _hiveMindPid = pid;
            if (_vizBrainEnabled.HasValue)
            {
                _root.Send(_hiveMindPid, BuildVisualizationRequest(_vizBrainEnabled.Value, enabled: true, _vizFocusRegionId));
            }
            _sink.OnHiveMindStatus($"Connected to {host}:{port}", true);
            return status;
        }
        catch (Exception ex)
        {
            _sink.OnHiveMindStatus($"HiveMind connect failed: {ex.Message}", false);
            return null;
        }
    }

    /// <summary>
    /// Clears the current HiveMind connection and disables any active visualization subscription state.
    /// </summary>
    public void DisconnectHiveMind()
    {
        if (_root is not null && _hiveMindPid is not null && _vizBrainEnabled.HasValue)
        {
            _root.Send(_hiveMindPid, BuildVisualizationRequest(_vizBrainEnabled.Value, enabled: false));
        }

        _vizBrainEnabled = null;
        _vizFocusRegionId = null;
        _hiveMindPid = null;
        _sink.OnHiveMindStatus("Disconnected", false);
    }

    /// <summary>
    /// Records the observability endpoint and reports whether its TCP port is currently reachable.
    /// </summary>
    public async Task<bool> ConnectObservabilityAsync(string host, int port, string debugHub, string vizHub, Nbn.Proto.Severity minSeverity, string contextRegex)
    {
        if (_root is null || _receiverPid is null)
        {
            _sink.OnObsStatus("Observability client not initialized.", false);
            return false;
        }

        _ = minSeverity;
        _ = contextRegex;

        _debugHubPid = new PID($"{host}:{port}", debugHub);
        _vizHubPid = new PID($"{host}:{port}", vizHub);

        _debugSubscribed = false;
        _debugSubscriptionKey = null;
        _vizSubscribed = false;
        var reachable = await IsEndpointReachableAsync(host, port, TimeSpan.FromSeconds(2)).ConfigureAwait(false);
        _sink.OnObsStatus(
            reachable
                ? $"Connected to {host}:{port}"
                : $"Obs endpoint unreachable: {host}:{port}",
            reachable);
        return reachable;
    }

    /// <summary>
    /// Updates the remote debug subscription according to the current Workbench filter.
    /// </summary>
    public void SetDebugSubscription(bool enabled, DebugSubscriptionFilter filter)
    {
        if (_root is null || _receiverPid is null || _debugHubPid is null)
        {
            return;
        }

        filter ??= DebugSubscriptionFilter.Default;
        var subscriber = PidLabel(_receiverPid);
        var subscriptionKey = BuildDebugSubscriptionKey(filter);
        if (enabled)
        {
            if (_debugSubscribed && string.Equals(_debugSubscriptionKey, subscriptionKey, StringComparison.Ordinal))
            {
                return;
            }

            _root.Send(_debugHubPid, BuildDebugSubscribe(subscriber, filter));
            _debugSubscribed = true;
            _debugSubscriptionKey = subscriptionKey;
            if (WorkbenchLog.Enabled)
            {
                WorkbenchLog.Info(
                    $"DebugSub enabled=true subscriber={subscriber} hub={PidLabel(_debugHubPid)} min={filter.MinSeverity} regex={filter.ContextRegex}");
            }
            return;
        }

        if (!_debugSubscribed)
        {
            return;
        }

        _root.Send(_debugHubPid, new DebugUnsubscribe { SubscriberActor = subscriber });
        _debugSubscribed = false;
        _debugSubscriptionKey = null;
        if (WorkbenchLog.Enabled)
        {
            WorkbenchLog.Info($"DebugSub enabled=false subscriber={subscriber} hub={PidLabel(_debugHubPid)}");
        }
    }

    /// <summary>
    /// Enables or disables the visualization event stream subscription.
    /// </summary>
    public void SetVizSubscription(bool enabled)
    {
        if (_root is null || _receiverPid is null || _vizHubPid is null)
        {
            return;
        }

        var subscriber = PidLabel(_receiverPid);
        if (enabled)
        {
            if (_vizSubscribed)
            {
                return;
            }

            _root.Send(_vizHubPid, new VizSubscribe { SubscriberActor = subscriber });
            _vizSubscribed = true;
            if (LogVizDiagnostics && WorkbenchLog.Enabled)
            {
                WorkbenchLog.Info($"VizSub enabled=true subscriber={subscriber} hub={PidLabel(_vizHubPid)}");
            }
            return;
        }

        if (!_vizSubscribed)
        {
            return;
        }

        _root.Send(_vizHubPid, new VizUnsubscribe { SubscriberActor = subscriber });
        _vizSubscribed = false;
        if (LogVizDiagnostics && WorkbenchLog.Enabled)
        {
            WorkbenchLog.Info($"VizSub enabled=false subscriber={subscriber} hub={PidLabel(_vizHubPid)}");
        }
    }

    /// <summary>
    /// Updates the brain and optional region whose visualization should be streamed to this client.
    /// </summary>
    public void SetActiveVisualizationBrain(Guid? brainId, uint? focusRegionId)
    {
        if (_vizBrainEnabled == brainId && _vizFocusRegionId == focusRegionId)
        {
            return;
        }

        if (_root is null || _hiveMindPid is null)
        {
            _vizBrainEnabled = brainId;
            _vizFocusRegionId = focusRegionId;
            return;
        }

        if (_vizBrainEnabled.HasValue && _vizBrainEnabled.Value != brainId)
        {
            _root.Send(_hiveMindPid, BuildVisualizationRequest(_vizBrainEnabled.Value, enabled: false));
        }

        _vizBrainEnabled = brainId;
        _vizFocusRegionId = focusRegionId;

        if (brainId.HasValue)
        {
            _root.Send(_hiveMindPid, BuildVisualizationRequest(brainId.Value, enabled: true, focusRegionId));
        }

        if (LogVizDiagnostics && WorkbenchLog.Enabled)
        {
            var brainLabel = brainId.HasValue ? brainId.Value.ToString("D") : "none";
            var focusLabel = focusRegionId.HasValue ? focusRegionId.Value.ToString() : "all";
            WorkbenchLog.Info($"VizScope brain={brainLabel} focus={focusLabel} subscribed={_vizSubscribed}");
        }
    }

    /// <summary>
    /// Unsubscribes from observability event streams and clears the local subscription state.
    /// </summary>
    public void DisconnectObservability(string? contextRegex = null)
    {
        if (_root is null || _receiverPid is null)
        {
            return;
        }

        var subscriber = PidLabel(_receiverPid);
        if (_debugHubPid is not null)
        {
            _root.Send(_debugHubPid, new DebugUnsubscribe { SubscriberActor = subscriber });
        }

        if (_vizHubPid is not null)
        {
            _root.Send(_vizHubPid, new VizUnsubscribe { SubscriberActor = subscriber });
        }

        _debugSubscribed = false;
        _debugSubscriptionKey = null;
        _vizSubscribed = false;
        _sink.OnObsStatus("Disconnected", false);
    }

    /// <summary>
    /// Re-applies the debug subscription filter when debug streaming is currently enabled.
    /// </summary>
    public Task RefreshDebugFilterAsync(DebugSubscriptionFilter filter)
    {
        if (_root is null || _receiverPid is null || _debugHubPid is null || !_debugSubscribed)
        {
            return Task.CompletedTask;
        }

        filter ??= DebugSubscriptionFilter.Default;
        _root.Send(_debugHubPid, BuildDebugSubscribe(PidLabel(_receiverPid), filter));

        return Task.CompletedTask;
    }
    private async Task StopAsync()
    {
        if (_system is null)
        {
            return;
        }

        try
        {
            if (_root is not null && _hiveMindPid is not null && _vizBrainEnabled.HasValue)
            {
                _root.Send(_hiveMindPid, BuildVisualizationRequest(_vizBrainEnabled.Value, enabled: false));
            }

            if (_system.Remote() is not null)
            {
                try
                {
                    await _system.Remote().ShutdownAsync(true).WaitAsync(RemoteShutdownTimeout).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                    WorkbenchLog.Warn($"Workbench client remote shutdown exceeded {RemoteShutdownTimeout.TotalSeconds:0}s timeout.");
                }
            }

            try
            {
                await _system.ShutdownAsync().WaitAsync(ActorSystemShutdownTimeout).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                WorkbenchLog.Warn($"Workbench client actor-system shutdown exceeded {ActorSystemShutdownTimeout.TotalSeconds:0}s timeout.");
            }
        }
        catch (Exception)
        {
        }
        finally
        {
            _system = null;
            _root = null;
            _receiverPid = null;
            _ioGatewayPid = null;
            _debugHubPid = null;
            _vizHubPid = null;
            _settingsPid = null;
            _hiveMindPid = null;
            _debugSubscribed = false;
            _debugSubscriptionKey = null;
            _vizSubscribed = false;
            _vizBrainEnabled = null;
            _vizFocusRegionId = null;
            _advertisedHost = null;
            _advertisedPort = null;
        }
    }

    /// <summary>
    /// Stops the local receiver actor system and releases the client gate.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _gate.Dispose();
    }

    private static DebugSubscribe BuildDebugSubscribe(string subscriber, DebugSubscriptionFilter filter)
    {
        var request = new DebugSubscribe
        {
            SubscriberActor = subscriber,
            MinSeverity = filter.MinSeverity,
            ContextRegex = filter.ContextRegex ?? string.Empty
        };

        request.IncludeContextPrefixes.Add(filter.IncludeContextPrefixes ?? Array.Empty<string>());
        request.ExcludeContextPrefixes.Add(filter.ExcludeContextPrefixes ?? Array.Empty<string>());
        request.IncludeSummaryPrefixes.Add(filter.IncludeSummaryPrefixes ?? Array.Empty<string>());
        request.ExcludeSummaryPrefixes.Add(filter.ExcludeSummaryPrefixes ?? Array.Empty<string>());
        return request;
    }

    private static string BuildDebugSubscriptionKey(DebugSubscriptionFilter filter)
    {
        static string Normalize(IEnumerable<string> values)
            => string.Join("\u001f", values ?? Array.Empty<string>());

        return string.Join(
            "\u001e",
            filter.StreamEnabled ? "1" : "0",
            ((int)filter.MinSeverity).ToString(),
            filter.ContextRegex ?? string.Empty,
            Normalize(filter.IncludeContextPrefixes),
            Normalize(filter.ExcludeContextPrefixes),
            Normalize(filter.IncludeSummaryPrefixes),
            Normalize(filter.ExcludeSummaryPrefixes));
    }

    private string PidLabel(PID pid)
    {
        var address = pid.Address;
        if (string.IsNullOrWhiteSpace(address))
        {
            address = _system?.Address ?? string.Empty;
        }

        if (string.IsNullOrWhiteSpace(address) && !string.IsNullOrWhiteSpace(_bindHost) && _bindPort > 0)
        {
            address = $"{_bindHost}:{_bindPort}";
        }

        return string.IsNullOrWhiteSpace(address) ? pid.Id : $"{address}/{pid.Id}";
    }

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Trim().ToLowerInvariant() is "1" or "true" or "yes" or "on";
    }

    private static async Task<bool> IsEndpointReachableAsync(string host, int port, TimeSpan timeout)
    {
        if (string.IsNullOrWhiteSpace(host) || port <= 0 || port >= 65536)
        {
            return false;
        }

        try
        {
            using var client = new TcpClient();
            using var cts = new CancellationTokenSource(timeout);
            await client.ConnectAsync(host, port, cts.Token).ConfigureAwait(false);
            return client.Connected;
        }
        catch
        {
            return false;
        }
    }

    private SetBrainVisualization BuildVisualizationRequest(Guid brainId, bool enabled, uint? focusRegionId = null)
    {
        var message = new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = enabled,
            HasFocusRegion = focusRegionId.HasValue,
            FocusRegionId = focusRegionId ?? 0
        };

        var subscriberActor = GetVisualizationSubscriberActor();
        if (!string.IsNullOrWhiteSpace(subscriberActor))
        {
            message.SubscriberActor = subscriberActor;
        }

        return message;
    }

    private string? GetVisualizationSubscriberActor()
        => _receiverPid is null ? null : PidLabel(_receiverPid);
}
