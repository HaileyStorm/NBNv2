using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

/// <summary>
/// Hosts the Workbench panels and coordinates client reconnect state across the operator surfaces.
/// </summary>
public sealed class ShellViewModel : ViewModelBase, IWorkbenchEventSink, IAsyncDisposable
{
    private static readonly TimeSpan InitialConnectRetryDelay = TimeSpan.FromMilliseconds(750);
    private static readonly TimeSpan ConnectRetryDelayStep = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan MaxConnectRetryDelay = TimeSpan.FromSeconds(5);
    private readonly UiDispatcher _dispatcher = new();
    private readonly WorkbenchClient _client;
    private readonly IWorkbenchArtifactPublisher _artifactPublisher;
    private NavItemViewModel? _selectedNav;
    private string _receiverLabel = "offline";
    private CancellationTokenSource? _connectCts;
    private int _tickCadenceRefreshVersion;
    private bool IsConnectionActive => _connectCts is not null && !_connectCts.IsCancellationRequested;

    public ShellViewModel()
        : this(client: null, autoConnect: true)
    {
    }

    internal ShellViewModel(WorkbenchClient? client, bool autoConnect, IWorkbenchArtifactPublisher? artifactPublisher = null)
    {
        WorkbenchProcessRegistry.Default.CleanupStale();

        Connections = new ConnectionViewModel();
        _client = client ?? new WorkbenchClient(this);
        _artifactPublisher = artifactPublisher ?? new WorkbenchArtifactPublisher(logInfo: WorkbenchLog.Info, logWarn: WorkbenchLog.Warn);

        Io = new IoPanelViewModel(_client, _dispatcher);
        Viz = new VizPanelViewModel(_dispatcher, Io);
        Viz.VisualizationSelectionChanged += UpdateObservabilitySubscriptions;
        Orchestrator = new OrchestratorPanelViewModel(
            _dispatcher,
            Connections,
            _client,
            OnBrainDiscovered,
            OnBrainsUpdated,
            ConnectAllAsync,
            DisconnectAll);
        Debug = new DebugPanelViewModel(_client, _dispatcher, Connections);
        Debug.SubscriptionSettingsChanged += UpdateObservabilitySubscriptions;
        Repro = new ReproPanelViewModel(_client, Connections, _artifactPublisher);
        Speciation = new SpeciationPanelViewModel(
            _dispatcher,
            Connections,
            _client,
            Orchestrator.StartSpeciationServiceAsync,
            Orchestrator.StopSpeciationServiceAsync,
            Orchestrator.RefreshSettingsAsync);
        Designer = new DesignerPanelViewModel(Connections, _client, OnSpawnedBrainDiscovered, _artifactPublisher);

        Navigation = new ObservableCollection<NavItemViewModel>
        {
            new("Orchestrator", "Nodes + Settings", "O", Orchestrator),
            new("Visualizer", "Activity stream", "V", Viz),
            new("Designer", "Build + import", "S", Designer),
            new("Energy + Plasticity", "System policy controls", "I", Io),
            new("Reproduction", "Spawn variants", "R", Repro),
            new("Speciation", "Taxonomy + simulation", "T", Speciation),
            new("Debug", "Logs & filters", "D", Debug)
        };

        SelectedNav = Navigation[0];

        ConnectAllCommand = new AsyncRelayCommand(ConnectAllAsync);
        DisconnectAllCommand = new RelayCommand(DisconnectAll);
        RefreshSettingsCommand = Orchestrator.RefreshCommand;

        if (autoConnect)
        {
            _ = ConnectAllAsync();
        }
    }

    public ConnectionViewModel Connections { get; }

    public OrchestratorPanelViewModel Orchestrator { get; }

    public IoPanelViewModel Io { get; }

    public DebugPanelViewModel Debug { get; }

    public VizPanelViewModel Viz { get; }

    public ReproPanelViewModel Repro { get; }

    public SpeciationPanelViewModel Speciation { get; }

    public DesignerPanelViewModel Designer { get; }

    public ObservableCollection<NavItemViewModel> Navigation { get; }

    public NavItemViewModel? SelectedNav
    {
        get => _selectedNav;
        set
        {
            if (SetProperty(ref _selectedNav, value))
            {
                OnPropertyChanged(nameof(CurrentPanel));
                UpdateObservabilitySubscriptions();
            }
        }
    }

    public object? CurrentPanel => SelectedNav?.Panel;

    public string ReceiverLabel
    {
        get => _receiverLabel;
        set => SetProperty(ref _receiverLabel, value);
    }

    public AsyncRelayCommand ConnectAllCommand { get; }

    public RelayCommand DisconnectAllCommand { get; }

    public AsyncRelayCommand RefreshSettingsCommand { get; }

    private async Task ConnectAllAsync()
    {
        try
        {
            WorkbenchLog.Info("ConnectAll started.");
            if (!TryParsePort(Connections.LocalPortText, out var localPort))
            {
                Connections.IoStatus = "Local port invalid.";
                return;
            }

            if (!TryParsePort(Connections.IoPortText, out var ioPort))
            {
                Connections.IoStatus = "IO port invalid.";
                return;
            }

            if (!TryParsePort(Connections.ObsPortText, out var obsPort))
            {
                Connections.ObsStatus = "Obs port invalid.";
                return;
            }

            _connectCts?.Cancel();
            _connectCts = new CancellationTokenSource();
            Interlocked.Increment(ref _tickCadenceRefreshVersion);
            var token = _connectCts.Token;

            await _client.EnsureStartedAsync(
                Connections.LocalBindHost,
                localPort,
                Connections.ResolveExplicitLocalAdvertiseHost());
            ReceiverLabel = _client.ReceiverLabel;

            Connections.SettingsStatus = "Connecting...";
            Connections.IoStatus = "Connecting...";
            Connections.ObsStatus = "Connecting...";
            Connections.HiveMindStatus = "Connecting...";

            StartConnectionLoops(token);
        }
        catch (Exception ex)
        {
            Connections.IoStatus = $"Connect failed: {ex.Message}";
            WorkbenchLog.Warn($"ConnectAll failed: {ex.Message}");
        }
    }

    private void DisconnectAll()
    {
        Interlocked.Increment(ref _tickCadenceRefreshVersion);
        _connectCts?.Cancel();
        _connectCts = null;
        SetDisconnectedStatuses();
        _client.DisconnectIo();
        _client.DisconnectSettings();
        _client.DisconnectObservability();
        _client.DisconnectHiveMind();
        WorkbenchLog.Info("Disconnected all services.");
    }

    public void OnOutputEvent(OutputEventItem item)
    {
        Io.AddOutputEvent(item);
        if (Guid.TryParse(item.BrainId, out var brainId))
        {
            OnBrainDiscovered(brainId);
        }
    }

    public void OnOutputVectorEvent(OutputVectorEventItem item)
    {
        Io.AddVectorEvent(item);
        if (Guid.TryParse(item.BrainId, out var brainId))
        {
            OnBrainDiscovered(brainId);
            Io.ObserveTick(brainId, item.TickId);
        }
    }

    public void OnDebugEvent(DebugEventItem item) => Debug.AddDebugEvent(item);

    public void OnVizEvent(VizEventItem item)
    {
        Debug.AddVizEvent(item);
        Viz.AddVizEvent(item);
    }

    public void OnBrainTerminated(BrainTerminatedItem item) => Orchestrator.AddTermination(item);

    public void OnIoStatus(string status, bool connected)
    {
        PostConnectionStatus(() =>
        {
            Connections.IoStatus = NormalizeStatus(status, connected);
            Connections.IoConnected = connected;
            if (connected)
            {
                Io.RefreshSubscriptions();
            }
        });
    }

    public void OnObsStatus(string status, bool connected)
    {
        PostConnectionStatus(() =>
        {
            Connections.ObsStatus = NormalizeStatus(status, connected);
            Connections.ObsConnected = connected;
        });
    }

    public void OnSettingsStatus(string status, bool connected)
    {
        PostConnectionStatus(() =>
        {
            Connections.SettingsStatus = NormalizeStatus(status, connected);
            Connections.SettingsConnected = connected;
        });
    }

    public void OnHiveMindStatus(string status, bool connected)
    {
        PostConnectionStatus(() =>
        {
            Connections.HiveMindStatus = NormalizeStatus(status, connected);
            Connections.HiveMindConnected = connected;
        });
    }

    public void OnSettingChanged(SettingItem item)
    {
        _dispatcher.Post(() =>
        {
            Orchestrator.UpdateSetting(item);
            Io.ApplySetting(item);
            Viz.ApplySetting(item);
            Repro.ApplySetting(item);
            Speciation.ApplySetting(item);
            if (Debug.ApplySetting(item))
            {
                UpdateObservabilitySubscriptions();
            }
        });

        if (string.Equals(item.Key, TickSettingsKeys.CadenceHzKey, StringComparison.OrdinalIgnoreCase))
        {
            var refreshVersion = Interlocked.Increment(ref _tickCadenceRefreshVersion);
            _ = RefreshHiveMindTickCadenceStatusAsync(refreshVersion);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await Speciation.DisposeAsync();
        await Orchestrator.StopAllAsyncForShutdown();
        _connectCts?.Cancel();
        _connectCts = null;
        SetDisconnectedStatuses();
        await _artifactPublisher.DisposeAsync().ConfigureAwait(false);
        await _client.DisposeAsync().ConfigureAwait(false);
    }

    private void OnBrainDiscovered(Guid brainId)
    {
        _dispatcher.Post(() => Viz.AddBrainId(brainId));
    }

    private void OnSpawnedBrainDiscovered(Guid brainId)
    {
        OnBrainDiscovered(brainId);
        _ = Orchestrator.RefreshSettingsAsync();
    }

    private void OnBrainsUpdated(IReadOnlyList<BrainListItem> brains)
    {
        var snapshot = brains.ToList();
        _dispatcher.Post(() =>
        {
            Viz.SetBrains(snapshot);
            Repro.UpdateActiveBrains(snapshot);
            Speciation.UpdateActiveBrains(snapshot);
            var active = snapshot
                .Where(entry => !string.Equals(entry.State, "Dead", StringComparison.OrdinalIgnoreCase))
                .Select(entry => entry.BrainId)
                .ToList();
            Io.UpdateActiveBrains(active);
            UpdateObservabilitySubscriptions();
        });
    }

    private static int ParsePortOrDefault(string value, int fallback)
        => int.TryParse(value, out var port) && port > 0 && port < 65536 ? port : fallback;

    private static bool TryParsePort(string value, out int port)
        => int.TryParse(value, out port) && port > 0 && port < 65536;

    private static (bool IsValid, int Port) TryResolvePort(string value)
        => (TryParsePort(value, out var port), port);

    private void StartConnectionLoops(CancellationToken token)
    {
        _ = ConnectSettingsWithRetryAsync(token);
        _ = ConnectIoWithRetryAsync(token);
        _ = ConnectObservabilityWithRetryAsync(token);
        _ = ConnectHiveMindWithRetryAsync(token);
    }

    private async Task ConnectIoWithRetryAsync(CancellationToken token)
    {
        await RetryConnectAsync(
            tryResolvePort: () => TryResolvePort(Connections.IoPortText),
            onInvalidPort: () =>
            {
                Connections.IoStatus = "IO port invalid.";
                Connections.IoConnected = false;
            },
            connectAsync: port => _client.ConnectIoAsync(
                Connections.IoHost,
                port,
                Connections.IoGateway,
                Connections.ClientName),
            isConnected: ack => ack is not null,
            onConnectedAsync: (port, _) =>
            {
                WorkbenchLog.Info($"IO connected to {Connections.IoHost}:{port}/{Connections.IoGateway}");
                return Task.CompletedTask;
            },
            token).ConfigureAwait(false);
    }

    private async Task ConnectSettingsWithRetryAsync(CancellationToken token)
    {
        await RetryConnectAsync(
            tryResolvePort: () => (true, ParsePortOrDefault(Connections.SettingsPortText, 12010)),
            onInvalidPort: static () => { },
            connectAsync: port => _client.ConnectSettingsAsync(
                Connections.SettingsHost,
                port,
                Connections.SettingsName,
                verify: true),
            isConnected: connected => connected,
            onConnectedAsync: async (port, _) =>
            {
                await Orchestrator.RefreshSettingsAsync().ConfigureAwait(false);
                await SyncWorkbenchSettingsFromSettingsMonitorAsync().ConfigureAwait(false);
                WorkbenchLog.Info($"Settings connected to {Connections.SettingsHost}:{port}/{Connections.SettingsName}");
            },
            token).ConfigureAwait(false);
    }

    private async Task ConnectHiveMindWithRetryAsync(CancellationToken token)
    {
        await RetryConnectAsync(
            tryResolvePort: () => TryResolvePort(Connections.HiveMindPortText),
            onInvalidPort: () =>
            {
                Connections.HiveMindStatus = "HiveMind port invalid.";
                Connections.HiveMindConnected = false;
            },
            connectAsync: port => _client.ConnectHiveMindAsync(
                Connections.HiveMindHost,
                port,
                Connections.HiveMindName),
            isConnected: status => status is not null,
            onConnectedAsync: (port, status) =>
            {
                var connectedStatus = status!;
                _dispatcher.Post(() =>
                {
                    if (!IsConnectionActive)
                    {
                        return;
                    }

                    Viz.ApplyHiveMindTickStatus(
                        connectedStatus.TargetTickHz,
                        connectedStatus.HasTickRateOverride,
                        connectedStatus.TickRateOverrideHz);
                });
                UpdateObservabilitySubscriptions();
                WorkbenchLog.Info($"HiveMind connected to {Connections.HiveMindHost}:{port}/{Connections.HiveMindName}");
                return Task.CompletedTask;
            },
            token).ConfigureAwait(false);
    }

    private async Task ConnectObservabilityWithRetryAsync(CancellationToken token)
    {
        await RetryConnectAsync(
            tryResolvePort: () => TryResolvePort(Connections.ObsPortText),
            onInvalidPort: () =>
            {
                Connections.ObsStatus = "Obs port invalid.";
                Connections.ObsConnected = false;
            },
            connectAsync: port => _client.ConnectObservabilityAsync(
                Connections.ObsHost,
                port,
                Connections.DebugHub,
                Connections.VizHub,
                Debug.SelectedSeverity.Severity,
                Debug.ContextRegex),
            isConnected: connected => connected,
            onConnectedAsync: (port, _) =>
            {
                UpdateObservabilitySubscriptions();
                WorkbenchLog.Info($"Observability subscribed to {Connections.ObsHost}:{port}");
                return Task.CompletedTask;
            },
            token).ConfigureAwait(false);
    }

    private void SetDisconnectedStatuses()
    {
        _dispatcher.Post(() =>
        {
            Connections.IoStatus = "Disconnected";
            Connections.IoConnected = false;
            Connections.ObsStatus = "Disconnected";
            Connections.ObsConnected = false;
            Connections.SettingsStatus = "Disconnected";
            Connections.SettingsConnected = false;
            Connections.HiveMindStatus = "Disconnected";
            Connections.HiveMindConnected = false;
        });
    }

    private async Task SyncWorkbenchSettingsFromSettingsMonitorAsync()
    {
        await ApplySettingsSnapshotAsync(DebugSettingsKeys.AllKeys, item => _ = Debug.ApplySetting(item)).ConfigureAwait(false);
        await ApplySettingsSnapshotAsync(
            CostEnergySettingsKeys.AllKeys
                .Concat(PlasticitySettingsKeys.AllKeys)
                .Concat(IoCoordinatorSettingsKeys.AllKeys),
            item => _ = Io.ApplySetting(item)).ConfigureAwait(false);
        await ApplySettingsSnapshotAsync(
            TickSettingsKeys.AllKeys.Concat(VisualizationSettingsKeys.AllKeys),
            item => _ = Viz.ApplySetting(item)).ConfigureAwait(false);
        await ApplySettingsSnapshotAsync(ReproductionSettingsKeys.AllKeys, item => _ = Repro.ApplySetting(item)).ConfigureAwait(false);
        await ApplySettingsSnapshotAsync(SpeciationSettingsKeys.AllKeys, item => _ = Speciation.ApplySetting(item)).ConfigureAwait(false);
        await ApplySettingsSnapshotAsync(ServiceEndpointSettings.AllKeys, Orchestrator.UpdateSetting).ConfigureAwait(false);
        _dispatcher.Post(UpdateObservabilitySubscriptions);
    }

    private async Task RefreshHiveMindTickCadenceStatusAsync(int refreshVersion)
    {
        var status = await _client.GetHiveMindStatusAsync().ConfigureAwait(false);
        if (status is null || refreshVersion != Volatile.Read(ref _tickCadenceRefreshVersion))
        {
            return;
        }

        _dispatcher.Post(() =>
        {
            if (refreshVersion != Volatile.Read(ref _tickCadenceRefreshVersion))
            {
                return;
            }

            Viz.ApplyHiveMindTickStatus(
                status.TargetTickHz,
                status.HasTickRateOverride,
                status.TickRateOverrideHz);
        });
    }

    private void UpdateObservabilitySubscriptions()
    {
        var isVisualizerTab = SelectedNav?.Panel is VizPanelViewModel;
        var shouldSubscribeViz = isVisualizerTab && Viz.HasSelectedBrain;
        var vizBrainId = shouldSubscribeViz ? Viz.SelectedBrain?.BrainId : null;
        var vizFocusRegionId = shouldSubscribeViz ? Viz.ActiveFocusRegionId : null;
        var debugFilter = Debug.BuildSubscriptionFilter();
        var shouldSubscribeDebug = debugFilter.StreamEnabled;
        _client.SetDebugSubscription(shouldSubscribeDebug, debugFilter);
        _client.SetVizSubscription(shouldSubscribeViz);
        _client.SetActiveVisualizationBrain(vizBrainId, vizFocusRegionId);
    }

    private void PostConnectionStatus(Action update)
    {
        _dispatcher.Post(() =>
        {
            if (!IsConnectionActive)
            {
                return;
            }

            update();
        });
    }

    private async Task RetryConnectAsync<TResult>(
        Func<(bool IsValid, int Port)> tryResolvePort,
        Action onInvalidPort,
        Func<int, Task<TResult>> connectAsync,
        Func<TResult, bool> isConnected,
        Func<int, TResult, Task>? onConnectedAsync,
        CancellationToken token)
    {
        var attempt = 0;

        while (!token.IsCancellationRequested)
        {
            var (isValid, port) = tryResolvePort();
            if (!isValid)
            {
                onInvalidPort();
                return;
            }

            attempt++;
            var result = await connectAsync(port).ConfigureAwait(false);
            if (isConnected(result))
            {
                if (token.IsCancellationRequested)
                {
                    return;
                }

                if (onConnectedAsync is not null)
                {
                    await onConnectedAsync(port, result).ConfigureAwait(false);
                }

                return;
            }

            if (!await DelayConnectRetryAsync(attempt, token).ConfigureAwait(false))
            {
                return;
            }
        }
    }

    private async Task ApplySettingsSnapshotAsync(IEnumerable<string> keys, Action<SettingItem> applySetting)
    {
        foreach (var key in keys)
        {
            var setting = await _client.GetSettingAsync(key).ConfigureAwait(false);
            if (setting is null)
            {
                continue;
            }

            var item = new SettingItem(
                key,
                setting.Value ?? string.Empty,
                setting.UpdatedMs.ToString());
            _dispatcher.Post(() => applySetting(item));
        }
    }

    private static async Task<bool> DelayConnectRetryAsync(int attempt, CancellationToken token)
    {
        var delay = InitialConnectRetryDelay + TimeSpan.FromMilliseconds(ConnectRetryDelayStep.TotalMilliseconds * attempt);
        if (delay > MaxConnectRetryDelay)
        {
            delay = MaxConnectRetryDelay;
        }

        try
        {
            await Task.Delay(delay, token).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private static string NormalizeStatus(string status, bool connected)
    {
        if (connected)
        {
            return "Connected";
        }

        if (string.IsNullOrWhiteSpace(status))
        {
            return "Disconnected";
        }

        var lower = status.Trim().ToLowerInvariant();
        if (lower.Contains("connecting"))
        {
            return "Connecting...";
        }

        if (lower.Contains("invalid"))
        {
            return "Invalid port";
        }

        if (lower.Contains("failed"))
        {
            return "Connect failed";
        }

        if (lower.Contains("offline"))
        {
            return "Offline";
        }

        if (lower.Contains("error"))
        {
            return "Error";
        }

        if (lower.Contains("disconnected"))
        {
            return "Disconnected";
        }

        return status.Length > 24 ? status.Substring(0, 24) + "…" : status;
    }
}
