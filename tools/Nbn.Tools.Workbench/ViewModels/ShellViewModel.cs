using System;
using System.Collections.ObjectModel;
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

        Orchestrator = new OrchestratorPanelViewModel(_dispatcher, Connections);
        Io = new IoPanelViewModel(_client, _dispatcher);
        Debug = new DebugPanelViewModel(_client, _dispatcher);
        Viz = new VizPanelViewModel(_dispatcher);
        Repro = new ReproPanelViewModel(_client);
        Designer = new DesignerPanelViewModel();

        Navigation = new ObservableCollection<NavItemViewModel>
        {
            new("Orchestrator", "Nodes + Settings", "?", Orchestrator),
            new("IO + Energy", "Brain inputs & outputs", "?", Io),
            new("Visualizer", "Activity stream", "?", Viz),
            new("Debug", "Logs & filters", "?", Debug),
            new("Designer", "Build + import", "?", Designer),
            new("Reproduction", "Spawn variants", "?", Repro)
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

        await _client.ConnectObservabilityAsync(
            Connections.ObsHost,
            obsPort,
            Connections.DebugHub,
            Connections.VizHub,
            Debug.SelectedSeverity.Severity,
            Debug.ContextRegex);
    }

    private void DisconnectAll()
    {
        _client.DisconnectIo();
        _client.DisconnectObservability();
    }

    public void OnOutputEvent(OutputEventItem item) => Io.AddOutputEvent(item);

    public void OnOutputVectorEvent(OutputVectorEventItem item) => Io.AddVectorEvent(item);

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

    public async ValueTask DisposeAsync()
    {
        await Orchestrator.StopDemoAsyncForShutdown();
        await _client.DisposeAsync().ConfigureAwait(false);
    }

    private static bool TryParsePort(string value, out int port)
        => int.TryParse(value, out port) && port > 0 && port < 65536;
}
