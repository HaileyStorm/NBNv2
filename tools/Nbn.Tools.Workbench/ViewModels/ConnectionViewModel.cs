using Avalonia.Media;

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
    private string _settingsDbPath = "settingsmonitor.db";
    private string _ioStatus = "Disconnected";
    private string _obsStatus = "Disconnected";
    private string _settingsStatus = "Idle";
    private bool _ioConnected;
    private bool _obsConnected;

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

    public bool IoConnected
    {
        get => _ioConnected;
        set
        {
            if (SetProperty(ref _ioConnected, value))
            {
                OnPropertyChanged(nameof(IoChipBackground));
                OnPropertyChanged(nameof(IoChipBorder));
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
            }
        }
    }

    public IBrush IoChipBackground => IoConnected ? new SolidColorBrush(Color.Parse("#DBF2EC")) : new SolidColorBrush(Color.Parse("#F4E8E0"));

    public IBrush IoChipBorder => IoConnected ? new SolidColorBrush(Color.Parse("#9FD9C8")) : new SolidColorBrush(Color.Parse("#E1C0AF"));

    public IBrush ObsChipBackground => ObsConnected ? new SolidColorBrush(Color.Parse("#DBF2EC")) : new SolidColorBrush(Color.Parse("#F4E8E0"));

    public IBrush ObsChipBorder => ObsConnected ? new SolidColorBrush(Color.Parse("#9FD9C8")) : new SolidColorBrush(Color.Parse("#E1C0AF"));
}
