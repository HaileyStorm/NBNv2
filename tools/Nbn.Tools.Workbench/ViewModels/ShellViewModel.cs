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

        await _client.EnsureStartedAsync(Connections.LocalBindHost, localPort);
        ReceiverLabel = _client.ReceiverLabel;

        await _client.ConnectIoAsync(
            Connections.IoHost,
            ioPort,
            Connections.IoGateway,
            Connections.ClientName);

        await _client.ConnectSettingsAsync(
            Connections.SettingsHost,
            ParsePortOrDefault(Connections.SettingsPortText, 12010),
            Connections.SettingsName);

        await _client.ConnectObservabilityAsync(
            Connections.ObsHost,
            obsPort,
            Connections.DebugHub,
            Connections.VizHub,
            Debug.SelectedSeverity.Severity,
            Debug.ContextRegex);

        await Orchestrator.RefreshSettingsAsync();
    }

    private void DisconnectAll()
    {
        _client.DisconnectIo();
        _client.DisconnectSettings();
        _client.DisconnectObservability();
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

    public void OnSettingChanged(SettingItem item)
    {
        Orchestrator.UpdateSetting(item);
    }

    public async ValueTask DisposeAsync()
    {
        await Orchestrator.StopDemoAsyncForShutdown();
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
}
