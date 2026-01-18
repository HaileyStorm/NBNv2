using Avalonia.Media;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class ConnectionViewModel : ViewModelBase
{
    private string _localBindHost = "127.0.0.1";
    private string _localPort = "12090";
    private string _ioHost = "127.0.0.1";
    private string _ioPort = "12050";
    private string _ioGateway = "io-gateway";
    private string _obsHost = "127.0.0.1";
    private string _obsPort = "12060";
    private string _debugHub = "DebugHub";
    private string _vizHub = "VisualizationHub";
    private string _clientName = "nbn.workbench";
    private string _settingsDbPath = RepoLocator.ResolvePathFromRepo("tools", "demo", "local-demo", "settingsmonitor.db")
        ?? "settingsmonitor.db";
    private string _settingsHost = "127.0.0.1";
    private string _settingsPortText = "12010";
    private string _settingsName = "SettingsMonitor";
    private string _hiveMindHost = "127.0.0.1";
    private string _hiveMindPortText = "12020";
    private string _hiveMindName = "HiveMind";
    private string _ioStatus = "Disconnected";
    private string _obsStatus = "Disconnected";
    private string _settingsStatus = "Idle";
    private bool _ioConnected;
    private bool _obsConnected;
    private bool _settingsConnected;

    public string LocalBindHost
    {
        get => _localBindHost;
        set => SetProperty(ref _localBindHost, value);
    }

    public string LocalPortText
    {
        get => _localPort;
        set => SetProperty(ref _localPort, value);
    }

    public string IoHost
    {
        get => _ioHost;
        set => SetProperty(ref _ioHost, value);
    }

    public string IoPortText
    {
        get => _ioPort;
        set => SetProperty(ref _ioPort, value);
    }

    public string IoGateway
    {
        get => _ioGateway;
        set => SetProperty(ref _ioGateway, value);
    }

    public string ObsHost
    {
        get => _obsHost;
        set => SetProperty(ref _obsHost, value);
    }

    public string ObsPortText
    {
        get => _obsPort;
        set => SetProperty(ref _obsPort, value);
    }

    public string DebugHub
    {
        get => _debugHub;
        set => SetProperty(ref _debugHub, value);
    }

    public string VizHub
    {
        get => _vizHub;
        set => SetProperty(ref _vizHub, value);
    }

    public string ClientName
    {
        get => _clientName;
        set => SetProperty(ref _clientName, value);
    }

    public string SettingsDbPath
    {
        get => _settingsDbPath;
        set => SetProperty(ref _settingsDbPath, value);
    }

    public string SettingsHost
    {
        get => _settingsHost;
        set => SetProperty(ref _settingsHost, value);
    }

    public string SettingsPortText
    {
        get => _settingsPortText;
        set => SetProperty(ref _settingsPortText, value);
    }

    public string SettingsName
    {
        get => _settingsName;
        set => SetProperty(ref _settingsName, value);
    }

    public string HiveMindHost
    {
        get => _hiveMindHost;
        set => SetProperty(ref _hiveMindHost, value);
    }

    public string HiveMindPortText
    {
        get => _hiveMindPortText;
        set => SetProperty(ref _hiveMindPortText, value);
    }

    public string HiveMindName
    {
        get => _hiveMindName;
        set => SetProperty(ref _hiveMindName, value);
    }

    public string IoStatus
    {
        get => _ioStatus;
        set => SetProperty(ref _ioStatus, value);
    }

    public string ObsStatus
    {
        get => _obsStatus;
        set => SetProperty(ref _obsStatus, value);
    }

    public string SettingsStatus
    {
        get => _settingsStatus;
        set => SetProperty(ref _settingsStatus, value);
    }

    public bool SettingsConnected
    {
        get => _settingsConnected;
        set
        {
            if (SetProperty(ref _settingsConnected, value))
            {
                OnPropertyChanged(nameof(SettingsChipBackground));
                OnPropertyChanged(nameof(SettingsChipBorder));
                OnPropertyChanged(nameof(SettingsStatusLabel));
            }
        }
    }

    public bool IoConnected
    {
        get => _ioConnected;
        set
        {
            if (SetProperty(ref _ioConnected, value))
            {
                OnPropertyChanged(nameof(IoChipBackground));
                OnPropertyChanged(nameof(IoChipBorder));
                OnPropertyChanged(nameof(IoStatusLabel));
            }
        }
    }

    public bool ObsConnected
    {
        get => _obsConnected;
        set
        {
            if (SetProperty(ref _obsConnected, value))
            {
                OnPropertyChanged(nameof(ObsChipBackground));
                OnPropertyChanged(nameof(ObsChipBorder));
                OnPropertyChanged(nameof(ObsStatusLabel));
            }
        }
    }

    public IBrush IoChipBackground => IoConnected ? new SolidColorBrush(Color.Parse("#DBF2EC")) : new SolidColorBrush(Color.Parse("#F4E8E0"));

    public IBrush IoChipBorder => IoConnected ? new SolidColorBrush(Color.Parse("#9FD9C8")) : new SolidColorBrush(Color.Parse("#E1C0AF"));

    public IBrush ObsChipBackground => ObsConnected ? new SolidColorBrush(Color.Parse("#DBF2EC")) : new SolidColorBrush(Color.Parse("#F4E8E0"));

    public IBrush ObsChipBorder => ObsConnected ? new SolidColorBrush(Color.Parse("#9FD9C8")) : new SolidColorBrush(Color.Parse("#E1C0AF"));

    public IBrush SettingsChipBackground => SettingsConnected ? new SolidColorBrush(Color.Parse("#DBF2EC")) : new SolidColorBrush(Color.Parse("#F4E8E0"));

    public IBrush SettingsChipBorder => SettingsConnected ? new SolidColorBrush(Color.Parse("#9FD9C8")) : new SolidColorBrush(Color.Parse("#E1C0AF"));

    public string IoStatusLabel => IoConnected ? "IO connected" : "IO disconnected";

    public string ObsStatusLabel => ObsConnected ? "Obs connected" : "Obs disconnected";

    public string SettingsStatusLabel => SettingsConnected ? "Settings connected" : "Settings disconnected";
}
