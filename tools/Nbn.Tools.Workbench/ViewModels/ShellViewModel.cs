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
    private CancellationTokenSource? _ioReconnectCts;

    public ShellViewModel()
    {
        Connections = new ConnectionViewModel();
        _client = new WorkbenchClient(this);

        Io = new IoPanelViewModel(_client, _dispatcher);
        Viz = new VizPanelViewModel(_dispatcher, Io);
        Orchestrator = new OrchestratorPanelViewModel(_dispatcher, Connections, _client, Viz.AddBrainId, OnBrainsUpdated, ConnectAllAsync);
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

            _ioReconnectCts?.Cancel();
            _ioReconnectCts = new CancellationTokenSource();

            await _client.EnsureStartedAsync(Connections.LocalBindHost, localPort);
            ReceiverLabel = _client.ReceiverLabel;

            await _client.ConnectSettingsAsync(
                Connections.SettingsHost,
                ParsePortOrDefault(Connections.SettingsPortText, 12010),
                Connections.SettingsName);

            await Orchestrator.RefreshSettingsAsync();

            _ = ConnectIoWithRetryAsync(
                Connections.IoHost,
                ioPort,
                Connections.IoGateway,
                Connections.ClientName,
                _ioReconnectCts.Token);

            await _client.ConnectObservabilityAsync(
                Connections.ObsHost,
                obsPort,
                Connections.DebugHub,
                Connections.VizHub,
                Debug.SelectedSeverity.Severity,
                Debug.ContextRegex);

            await ConnectHiveMindAsync();
        }
        catch (Exception ex)
        {
            Connections.IoStatus = $"Connect failed: {ex.Message}";
        }
    }

    private void DisconnectAll()
    {
        _ioReconnectCts?.Cancel();
        _client.DisconnectIo();
        _client.DisconnectSettings();
        _client.DisconnectObservability();
        _client.DisconnectHiveMind();
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
            Connections.IoStatus = status;
            Connections.IoConnected = connected;
        });
    }

    public void OnObsStatus(string status, bool connected)
    {
        _dispatcher.Post(() =>
        {
            Connections.ObsStatus = status;
            Connections.ObsConnected = connected;
        });
    }

    public void OnSettingsStatus(string status, bool connected)
    {
        _dispatcher.Post(() =>
        {
            Connections.SettingsStatus = status;
            Connections.SettingsConnected = connected;
        });
    }

    public void OnHiveMindStatus(string status, bool connected)
    {
        _dispatcher.Post(() =>
        {
            Connections.HiveMindStatus = status;
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
        _ioReconnectCts?.Cancel();
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

    private async Task ConnectIoWithRetryAsync(string host, int port, string gatewayName, string clientName, CancellationToken token)
    {
        var attempt = 0;

        while (!token.IsCancellationRequested)
        {
            attempt++;
            var ack = await _client.ConnectIoAsync(host, port, gatewayName, clientName);
            if (ack is not null)
            {
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

    private async Task ConnectHiveMindAsync()
    {
        if (!TryParsePort(Connections.HiveMindPortText, out var hivePort))
        {
            Connections.HiveMindStatus = "HiveMind port invalid.";
            Connections.HiveMindConnected = false;
            return;
        }

        await _client.ConnectHiveMindAsync(
            Connections.HiveMindHost,
            hivePort,
            Connections.HiveMindName);
    }
}
