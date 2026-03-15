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
    private string _reproHost = "127.0.0.1";
    private string _reproPortText = "12070";
    private string _reproManager = "ReproductionManager";
    private string _speciationHost = "127.0.0.1";
    private string _speciationPortText = "12080";
    private string _speciationManager = "SpeciationManager";
    private string _workerHost = "127.0.0.1";
    private string _workerPortText = "12041";
    private string _workerCapabilityBenchmarkRefreshSecondsText = "3600";
    private string _workerRootName = "worker-node";
    private string _workerLogicalName = "nbn.worker";
    private string _obsHost = "127.0.0.1";
    private string _obsPort = "12060";
    private string _debugHub = "DebugHub";
    private string _vizHub = "VisualizationHub";
    private string _clientName = "nbn.workbench";
    private string _settingsDbPath = BuildDefaultSettingsDbPath();
    private string _settingsHost = "127.0.0.1";
    private string _settingsPortText = "12010";
    private string _settingsName = "SettingsMonitor";
    private string _hiveMindHost = "127.0.0.1";
    private string _hiveMindPortText = "12020";
    private string _hiveMindName = "HiveMind";
    private string _hiveMindStatus = "Disconnected";
    private string _ioStatus = "Disconnected";
    private string _reproStatus = "Disconnected";
    private string _speciationStatus = "Disconnected";
    private string _workerStatus = "Disconnected";
    private string _obsStatus = "Disconnected";
    private string _settingsStatus = "Idle";
    private bool _workbenchLoggingEnabled;
    private bool _ioConnected;
    private bool _reproConnected;
    private bool _workerConnected;
    private bool _obsConnected;
    private bool _settingsConnected;
    private bool _hiveMindConnected;
    private bool _ioDiscoverable;
    private bool _reproDiscoverable;
    private bool _speciationDiscoverable;
    private bool _workerDiscoverable;
    private bool _obsDiscoverable;
    private bool _hiveMindDiscoverable;
    private string _ioEndpointDisplay = "Missing";
    private string _reproEndpointDisplay = "Missing";
    private string _speciationEndpointDisplay = "Missing";
    private string _workerEndpointDisplay = "Missing";
    private string _obsEndpointDisplay = "Missing";
    private string _hiveMindEndpointDisplay = "Missing";

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

    public string ReproHost
    {
        get => _reproHost;
        set => SetProperty(ref _reproHost, value);
    }

    public string ReproPortText
    {
        get => _reproPortText;
        set => SetProperty(ref _reproPortText, value);
    }

    public string ReproManager
    {
        get => _reproManager;
        set => SetProperty(ref _reproManager, value);
    }

    public string SpeciationHost
    {
        get => _speciationHost;
        set => SetProperty(ref _speciationHost, value);
    }

    public string SpeciationPortText
    {
        get => _speciationPortText;
        set => SetProperty(ref _speciationPortText, value);
    }

    public string SpeciationManager
    {
        get => _speciationManager;
        set => SetProperty(ref _speciationManager, value);
    }

    public string WorkerHost
    {
        get => _workerHost;
        set => SetProperty(ref _workerHost, value);
    }

    public string WorkerPortText
    {
        get => _workerPortText;
        set => SetProperty(ref _workerPortText, value);
    }

    public string WorkerCapabilityBenchmarkRefreshSecondsText
    {
        get => _workerCapabilityBenchmarkRefreshSecondsText;
        set => SetProperty(ref _workerCapabilityBenchmarkRefreshSecondsText, value);
    }

    public string WorkerRootName
    {
        get => _workerRootName;
        set => SetProperty(ref _workerRootName, value);
    }

    public string WorkerLogicalName
    {
        get => _workerLogicalName;
        set => SetProperty(ref _workerLogicalName, value);
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

    public bool WorkbenchLoggingEnabled
    {
        get => _workbenchLoggingEnabled;
        set
        {
            if (SetProperty(ref _workbenchLoggingEnabled, value))
            {
                WorkbenchLog.SetEnabled(value);
                OnPropertyChanged(nameof(WorkbenchLogPath));
            }
        }
    }

    public string WorkbenchLogPath => string.IsNullOrWhiteSpace(WorkbenchLog.SessionDirectory)
        ? "(disabled)"
        : WorkbenchLog.SessionDirectory;

    public string HiveMindStatus
    {
        get => _hiveMindStatus;
        set => SetProperty(ref _hiveMindStatus, value);
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

    public string ReproStatus
    {
        get => _reproStatus;
        set => SetProperty(ref _reproStatus, value);
    }

    public string SpeciationStatus
    {
        get => _speciationStatus;
        set => SetProperty(ref _speciationStatus, value);
    }

    public string WorkerStatus
    {
        get => _workerStatus;
        set => SetProperty(ref _workerStatus, value);
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
                OnPropertyChanged(nameof(SettingsChipForeground));
                OnPropertyChanged(nameof(SettingsStatusLabel));
            }
        }
    }

    public bool HiveMindConnected
    {
        get => _hiveMindConnected;
        set
        {
            if (SetProperty(ref _hiveMindConnected, value))
            {
                OnPropertyChanged(nameof(HiveMindChipBackground));
                OnPropertyChanged(nameof(HiveMindChipBorder));
                OnPropertyChanged(nameof(HiveMindStatusLabel));
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

    public bool ReproConnected
    {
        get => _reproConnected;
        set
        {
            if (SetProperty(ref _reproConnected, value))
            {
                OnPropertyChanged(nameof(ReproChipBackground));
                OnPropertyChanged(nameof(ReproChipBorder));
                OnPropertyChanged(nameof(ReproStatusLabel));
            }
        }
    }

    public bool WorkerConnected
    {
        get => _workerConnected;
        set
        {
            if (SetProperty(ref _workerConnected, value))
            {
                OnPropertyChanged(nameof(WorkerChipBackground));
                OnPropertyChanged(nameof(WorkerChipBorder));
                OnPropertyChanged(nameof(WorkerStatusLabel));
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

    public bool HiveMindDiscoverable
    {
        get => _hiveMindDiscoverable;
        set
        {
            if (SetProperty(ref _hiveMindDiscoverable, value))
            {
                OnPropertyChanged(nameof(HiveMindChipBackground));
                OnPropertyChanged(nameof(HiveMindChipBorder));
                OnPropertyChanged(nameof(HiveMindStatusLabel));
            }
        }
    }

    public bool IoDiscoverable
    {
        get => _ioDiscoverable;
        set
        {
            if (SetProperty(ref _ioDiscoverable, value))
            {
                OnPropertyChanged(nameof(IoChipBackground));
                OnPropertyChanged(nameof(IoChipBorder));
                OnPropertyChanged(nameof(IoStatusLabel));
            }
        }
    }

    public bool ReproDiscoverable
    {
        get => _reproDiscoverable;
        set
        {
            if (SetProperty(ref _reproDiscoverable, value))
            {
                OnPropertyChanged(nameof(ReproChipBackground));
                OnPropertyChanged(nameof(ReproChipBorder));
                OnPropertyChanged(nameof(ReproStatusLabel));
            }
        }
    }

    public bool SpeciationDiscoverable
    {
        get => _speciationDiscoverable;
        set
        {
            if (SetProperty(ref _speciationDiscoverable, value))
            {
                OnPropertyChanged(nameof(SpeciationChipBackground));
                OnPropertyChanged(nameof(SpeciationChipBorder));
                OnPropertyChanged(nameof(SpeciationStatusLabel));
            }
        }
    }

    public bool WorkerDiscoverable
    {
        get => _workerDiscoverable;
        set
        {
            if (SetProperty(ref _workerDiscoverable, value))
            {
                OnPropertyChanged(nameof(WorkerChipBackground));
                OnPropertyChanged(nameof(WorkerChipBorder));
                OnPropertyChanged(nameof(WorkerStatusLabel));
            }
        }
    }

    public bool ObsDiscoverable
    {
        get => _obsDiscoverable;
        set
        {
            if (SetProperty(ref _obsDiscoverable, value))
            {
                OnPropertyChanged(nameof(ObsChipBackground));
                OnPropertyChanged(nameof(ObsChipBorder));
                OnPropertyChanged(nameof(ObsStatusLabel));
            }
        }
    }

    public string IoEndpointDisplay
    {
        get => _ioEndpointDisplay;
        set => SetProperty(ref _ioEndpointDisplay, value);
    }

    public string ReproEndpointDisplay
    {
        get => _reproEndpointDisplay;
        set => SetProperty(ref _reproEndpointDisplay, value);
    }

    public string SpeciationEndpointDisplay
    {
        get => _speciationEndpointDisplay;
        set => SetProperty(ref _speciationEndpointDisplay, value);
    }

    public string WorkerEndpointDisplay
    {
        get => _workerEndpointDisplay;
        set => SetProperty(ref _workerEndpointDisplay, value);
    }

    public string ObsEndpointDisplay
    {
        get => _obsEndpointDisplay;
        set => SetProperty(ref _obsEndpointDisplay, value);
    }

    public string HiveMindEndpointDisplay
    {
        get => _hiveMindEndpointDisplay;
        set => SetProperty(ref _hiveMindEndpointDisplay, value);
    }

    public IBrush IoChipBackground => IoDiscoverable ? new SolidColorBrush(Color.Parse("#DBF2EC")) : new SolidColorBrush(Color.Parse("#F4E8E0"));

    public IBrush IoChipBorder => IoDiscoverable ? new SolidColorBrush(Color.Parse("#9FD9C8")) : new SolidColorBrush(Color.Parse("#E1C0AF"));

    public IBrush ObsChipBackground => ObsDiscoverable ? new SolidColorBrush(Color.Parse("#DBF2EC")) : new SolidColorBrush(Color.Parse("#F4E8E0"));

    public IBrush ObsChipBorder => ObsDiscoverable ? new SolidColorBrush(Color.Parse("#9FD9C8")) : new SolidColorBrush(Color.Parse("#E1C0AF"));

    public IBrush ReproChipBackground => ReproDiscoverable ? new SolidColorBrush(Color.Parse("#DBF2EC")) : new SolidColorBrush(Color.Parse("#F4E8E0"));

    public IBrush ReproChipBorder => ReproDiscoverable ? new SolidColorBrush(Color.Parse("#9FD9C8")) : new SolidColorBrush(Color.Parse("#E1C0AF"));

    public IBrush SpeciationChipBackground => SpeciationDiscoverable ? new SolidColorBrush(Color.Parse("#DBF2EC")) : new SolidColorBrush(Color.Parse("#F4E8E0"));

    public IBrush SpeciationChipBorder => SpeciationDiscoverable ? new SolidColorBrush(Color.Parse("#9FD9C8")) : new SolidColorBrush(Color.Parse("#E1C0AF"));

    public IBrush WorkerChipBackground => WorkerDiscoverable ? new SolidColorBrush(Color.Parse("#DBF2EC")) : new SolidColorBrush(Color.Parse("#F4E8E0"));

    public IBrush WorkerChipBorder => WorkerDiscoverable ? new SolidColorBrush(Color.Parse("#9FD9C8")) : new SolidColorBrush(Color.Parse("#E1C0AF"));

    public IBrush SettingsChipBackground => SettingsConnected ? new SolidColorBrush(Color.Parse("#DDF5E5")) : new SolidColorBrush(Color.Parse("#FBE8E8"));

    public IBrush SettingsChipBorder => SettingsConnected ? new SolidColorBrush(Color.Parse("#2D9A5E")) : new SolidColorBrush(Color.Parse("#C04A4A"));

    public IBrush SettingsChipForeground => SettingsConnected ? new SolidColorBrush(Color.Parse("#0F5832")) : new SolidColorBrush(Color.Parse("#6B1F1F"));

    public IBrush HiveMindChipBackground => HiveMindDiscoverable ? new SolidColorBrush(Color.Parse("#DBF2EC")) : new SolidColorBrush(Color.Parse("#F4E8E0"));

    public IBrush HiveMindChipBorder => HiveMindDiscoverable ? new SolidColorBrush(Color.Parse("#9FD9C8")) : new SolidColorBrush(Color.Parse("#E1C0AF"));

    public string IoStatusLabel => IoDiscoverable ? "IO online" : "IO offline";

    public string ObsStatusLabel => ObsDiscoverable ? "Obs online" : "Obs offline";

    public string ReproStatusLabel => ReproDiscoverable ? "Repro online" : "Repro offline";

    public string SpeciationStatusLabel => SpeciationDiscoverable ? "Speciation online" : "Speciation offline";

    public string WorkerStatusLabel => WorkerDiscoverable ? "Worker online" : "Worker offline";

    public string SettingsStatusLabel => SettingsConnected ? "online" : "offline";

    public string HiveMindStatusLabel => HiveMindDiscoverable ? "HiveMind online" : "HiveMind offline";

    public bool IsSettingsServiceReady()
        => SettingsConnected || HasPositiveStatus(SettingsStatus);

    public bool IsHiveMindServiceReady()
        => HiveMindDiscoverable || HiveMindConnected || HasPositiveStatus(HiveMindStatus);

    public bool IsIoServiceReady()
        => IoDiscoverable || IoConnected || HasPositiveStatus(IoStatus);

    public bool IsReproServiceReady()
        => ReproDiscoverable || ReproConnected || HasPositiveStatus(ReproStatus);

    public bool IsObsServiceReady()
        => ObsDiscoverable || ObsConnected || HasPositiveStatus(ObsStatus);

    public bool IsSpeciationServiceReady()
        => SpeciationDiscoverable || HasPositiveStatus(SpeciationStatus);

    public bool HasSpawnServiceReadiness()
        => IsSettingsServiceReady() && IsHiveMindServiceReady() && IsIoServiceReady();

    public bool HasReproductionServiceReadiness()
        => IsSettingsServiceReady() && IsIoServiceReady() && IsReproServiceReady();

    public bool HasDebugServiceReadiness()
        => IsSettingsServiceReady() && IsObsServiceReady();

    public bool HasSpeciationServiceReadiness()
        => IsSettingsServiceReady() && IsSpeciationServiceReady();

    public string BuildSpawnReadinessGuidance()
    {
        var missing = new List<string>(3);
        if (!IsSettingsServiceReady())
        {
            missing.Add("Settings");
        }

        if (!IsHiveMindServiceReady())
        {
            missing.Add("HiveMind");
        }

        if (!IsIoServiceReady())
        {
            missing.Add("IO");
        }

        return missing.Count == 0
            ? "Connect Settings, HiveMind, and IO first."
            : $"Connect {JoinReadableList(missing)} first.";
    }

    private static string JoinReadableList(IReadOnlyList<string> items)
    {
        if (items.Count == 0)
        {
            return string.Empty;
        }

        if (items.Count == 1)
        {
            return items[0];
        }

        if (items.Count == 2)
        {
            return $"{items[0]} and {items[1]}";
        }

        return $"{string.Join(", ", items.Take(items.Count - 1))}, and {items[^1]}";
    }

    private static bool HasPositiveStatus(string? status)
    {
        if (string.IsNullOrWhiteSpace(status))
        {
            return false;
        }

        var normalized = status.Trim();
        return normalized.Equals("connected", StringComparison.OrdinalIgnoreCase)
               || normalized.Equals("online", StringComparison.OrdinalIgnoreCase)
               || normalized.Equals("ready", StringComparison.OrdinalIgnoreCase);
    }

    private static string BuildDefaultSettingsDbPath()
    {
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(localAppData))
        {
            return "settingsmonitor.db";
        }

        var root = Path.Combine(localAppData, "Nbn.Workbench");
        Directory.CreateDirectory(root);
        return Path.Combine(root, "settingsmonitor.db");
    }
}
