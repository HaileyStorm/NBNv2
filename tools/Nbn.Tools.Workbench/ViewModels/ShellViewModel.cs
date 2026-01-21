using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class ShellViewModel : ViewModelBase, IWorkbenchEventSink, IAsyncDisposable
{
    private readonly UiDispatcher _dispatcher = new();
    private readonly WorkbenchClient _client;
    private NavItemViewModel? _selectedNav;
    private string _receiverLabel = "offline";
    private CancellationTokenSource? _connectCts;
    private bool IsConnectionActive => _connectCts is not null && !_connectCts.IsCancellationRequested;

    public ShellViewModel()
    {
        WorkbenchProcessRegistry.Default.CleanupStale();
        LocalDemoRunner.CleanupStaleProcesses();

        Connections = new ConnectionViewModel();
        _client = new WorkbenchClient(this);

        Io = new IoPanelViewModel(_client, _dispatcher);
        Viz = new VizPanelViewModel(_dispatcher, Io);
        Orchestrator = new OrchestratorPanelViewModel(_dispatcher, Connections, _client, Viz.AddBrainId, OnBrainsUpdated, ConnectAllAsync, DisconnectAll);
        Debug = new DebugPanelViewModel(_client, _dispatcher);
        Repro = new ReproPanelViewModel(_client);
        Designer = new DesignerPanelViewModel();

        Navigation = new ObservableCollection<NavItemViewModel>
        {
            new("Orchestrator", "Nodes + Settings", "O", Orchestrator),
            new("IO + Energy", "Brain inputs & outputs", "I", Io),
            new("Visualizer", "Activity stream", "V", Viz),
            new("Debug", "Logs & filters", "D", Debug),
            new("Designer", "Build + import", "S", Designer),
            new("Reproduction", "Spawn variants", "R", Repro)
        };

        SelectedNav = Navigation[0];

        ConnectAllCommand = new AsyncRelayCommand(ConnectAllAsync);
        DisconnectAllCommand = new RelayCommand(DisconnectAll);
        RefreshSettingsCommand = Orchestrator.RefreshCommand;

        _ = ConnectAllAsync();
    }

    public ConnectionViewModel Connections { get; }

    public OrchestratorPanelViewModel Orchestrator { get; }

    public IoPanelViewModel Io { get; }

    public DebugPanelViewModel Debug { get; }

    public VizPanelViewModel Viz { get; }

    public ReproPanelViewModel Repro { get; }

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
            var token = _connectCts.Token;

            await _client.EnsureStartedAsync(Connections.LocalBindHost, localPort);
            ReceiverLabel = _client.ReceiverLabel;

            Connections.SettingsStatus = "Connecting...";
            Connections.IoStatus = "Connecting...";
            Connections.ObsStatus = "Connecting...";
            Connections.HiveMindStatus = "Connecting...";

            _ = ConnectSettingsWithRetryAsync(token);
            _ = ConnectIoWithRetryAsync(token);

            _ = ConnectObservabilityWithRetryAsync(token);

            _ = ConnectHiveMindWithRetryAsync(token);
        }
        catch (Exception ex)
        {
            Connections.IoStatus = $"Connect failed: {ex.Message}";
            WorkbenchLog.Warn($"ConnectAll failed: {ex.Message}");
        }
    }

    private void DisconnectAll()
    {
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
            Viz.AddBrainId(brainId);
        }
    }

    public void OnOutputVectorEvent(OutputVectorEventItem item)
    {
        Io.AddVectorEvent(item);
        if (Guid.TryParse(item.BrainId, out var brainId))
        {
            Viz.AddBrainId(brainId);
        }
    }

    public void OnDebugEvent(DebugEventItem item) => Debug.AddDebugEvent(item);

    public void OnVizEvent(VizEventItem item) => Viz.AddVizEvent(item);

    public void OnBrainTerminated(BrainTerminatedItem item) => Orchestrator.AddTermination(item);

    public void OnIoStatus(string status, bool connected)
    {
        _dispatcher.Post(() =>
        {
            if (!IsConnectionActive)
            {
                return;
            }

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
        _dispatcher.Post(() =>
        {
            if (!IsConnectionActive)
            {
                return;
            }

            Connections.ObsStatus = NormalizeStatus(status, connected);
            Connections.ObsConnected = connected;
        });
    }

    public void OnSettingsStatus(string status, bool connected)
    {
        _dispatcher.Post(() =>
        {
            if (!IsConnectionActive)
            {
                return;
            }

            Connections.SettingsStatus = NormalizeStatus(status, connected);
            Connections.SettingsConnected = connected;
        });
    }

    public void OnHiveMindStatus(string status, bool connected)
    {
        _dispatcher.Post(() =>
        {
            if (!IsConnectionActive)
            {
                return;
            }

            Connections.HiveMindStatus = NormalizeStatus(status, connected);
            Connections.HiveMindConnected = connected;
        });
    }

    public void OnSettingChanged(SettingItem item)
    {
        Orchestrator.UpdateSetting(item);
    }

    public async ValueTask DisposeAsync()
    {
        await Orchestrator.StopAllAsyncForShutdown();
        _connectCts?.Cancel();
        _connectCts = null;
        SetDisconnectedStatuses();
        await _client.DisposeAsync().ConfigureAwait(false);
    }

    private void OnBrainsUpdated(IReadOnlyList<BrainListItem> brains)
    {
        Viz.SetBrains(brains);
        var active = brains
            .Where(entry => !string.Equals(entry.State, "Dead", StringComparison.OrdinalIgnoreCase))
            .Select(entry => entry.BrainId)
            .ToList();
        Io.UpdateActiveBrains(active);
    }

    private static int ParsePortOrDefault(string value, int fallback)
        => int.TryParse(value, out var port) && port > 0 && port < 65536 ? port : fallback;

    private static bool TryParsePort(string value, out int port)
        => int.TryParse(value, out port) && port > 0 && port < 65536;

    private async Task ConnectIoWithRetryAsync(CancellationToken token)
    {
        var attempt = 0;

        while (!token.IsCancellationRequested)
        {
            attempt++;
            if (!TryParsePort(Connections.IoPortText, out var ioPort))
            {
                Connections.IoStatus = "IO port invalid.";
                Connections.IoConnected = false;
                return;
            }

            var ack = await _client.ConnectIoAsync(
                Connections.IoHost,
                ioPort,
                Connections.IoGateway,
                Connections.ClientName);
            if (ack is not null)
            {
                WorkbenchLog.Info($"IO connected to {Connections.IoHost}:{ioPort}/{Connections.IoGateway}");
                return;
            }

            try
            {
                await Task.Delay(Math.Min(5000, 750 + attempt * 250), token);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private async Task ConnectSettingsWithRetryAsync(CancellationToken token)
    {
        var attempt = 0;

        while (!token.IsCancellationRequested)
        {
            attempt++;
            var settingsPort = ParsePortOrDefault(Connections.SettingsPortText, 12010);
            var connected = await _client.ConnectSettingsAsync(
                    Connections.SettingsHost,
                    settingsPort,
                    Connections.SettingsName,
                    verify: true)
                .ConfigureAwait(false);

            if (connected)
            {
                if (!token.IsCancellationRequested)
                {
                    await Orchestrator.RefreshSettingsAsync().ConfigureAwait(false);
                }

                WorkbenchLog.Info($"Settings connected to {Connections.SettingsHost}:{settingsPort}/{Connections.SettingsName}");
                return;
            }

            try
            {
                await Task.Delay(Math.Min(5000, 750 + attempt * 250), token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private async Task ConnectHiveMindWithRetryAsync(CancellationToken token)
    {
        var attempt = 0;

        while (!token.IsCancellationRequested)
        {
            if (!TryParsePort(Connections.HiveMindPortText, out var hivePort))
            {
                Connections.HiveMindStatus = "HiveMind port invalid.";
                Connections.HiveMindConnected = false;
                return;
            }

            attempt++;
            var status = await _client.ConnectHiveMindAsync(
                    Connections.HiveMindHost,
                    hivePort,
                    Connections.HiveMindName)
                .ConfigureAwait(false);

            if (status is not null)
            {
                WorkbenchLog.Info($"HiveMind connected to {Connections.HiveMindHost}:{hivePort}/{Connections.HiveMindName}");
                return;
            }

            try
            {
                await Task.Delay(Math.Min(5000, 750 + attempt * 250), token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private async Task ConnectObservabilityWithRetryAsync(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            if (!TryParsePort(Connections.ObsPortText, out var obsPort))
            {
                Connections.ObsStatus = "Obs port invalid.";
                Connections.ObsConnected = false;
                return;
            }

            await _client.ConnectObservabilityAsync(
                Connections.ObsHost,
                obsPort,
                Connections.DebugHub,
                Connections.VizHub,
                Debug.SelectedSeverity.Severity,
                Debug.ContextRegex).ConfigureAwait(false);

            if (token.IsCancellationRequested)
            {
                return;
            }

            WorkbenchLog.Info($"Observability subscribed to {Connections.ObsHost}:{obsPort}");
            return;
        }
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
