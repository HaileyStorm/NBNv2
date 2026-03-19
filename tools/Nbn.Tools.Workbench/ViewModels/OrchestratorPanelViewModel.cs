using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Nbn.Proto.Control;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class OrchestratorPanelViewModel : ViewModelBase
{
    private const int MaxWorkerBrainHints = 2;
    private const int MaxRows = 200;
    private const long StaleNodeMs = 15000;
    private const long WorkerFailedAfterMs = 45000;
    private const long WorkerRemoveAfterMs = 120000;
    private const long SpawnVisibilityGraceMs = 30000;
    private const float LocalDefaultTickHz = 8f;
    private const float LocalDefaultMinTickHz = 2f;
    private const int LocalDefaultWorkerStorageLimitPercent = 95;
    private static readonly TimeSpan SpawnRegistrationTimeout = TimeSpan.FromSeconds(12);
    private static readonly TimeSpan SpawnRegistrationPollInterval = TimeSpan.FromMilliseconds(300);
    private static readonly bool EnableRuntimeDiagnostics = IsEnvTrue("NBN_WORKBENCH_RUNTIME_DIAGNOSTICS_ENABLED");
    private static readonly string ActivityDiagnosticsPeriod =
        ResolveEnvOrDefault("NBN_WORKBENCH_ACTIVITY_DIAGNOSTICS_PERIOD", "64");
    private readonly UiDispatcher _dispatcher;
    private readonly ConnectionViewModel _connections;
    private readonly WorkbenchClient _client;
    private readonly ILocalProjectLaunchPreparer _launchPreparer;
    private readonly LocalServiceRunner _settingsRunner = new();
    private readonly LocalServiceRunner _hiveMindRunner = new();
    private readonly LocalServiceRunner _ioRunner = new();
    private readonly LocalServiceRunner _reproRunner = new();
    private readonly LocalServiceRunner _speciationRunner = new();
    private readonly LocalServiceRunner _workerRunner = new();
    private readonly LocalServiceRunner _obsRunner = new();
    private readonly LocalServiceRunner _profileCurrentSystemRunner = new();
    private readonly Action<Guid>? _brainDiscovered;
    private readonly Action<IReadOnlyList<BrainListItem>>? _brainsUpdated;
    private readonly Func<Task>? _connectAll;
    private readonly Action? _disconnectAll;
    private readonly Dictionary<Guid, WorkerEndpointSnapshot> _workerEndpointCache = new();
    private readonly SemaphoreSlim _refreshGate = new(1, 1);
    private string _statusMessage = "Idle";
    private string _settingsLaunchStatus = "Idle";
    private string _hiveMindLaunchStatus = "Idle";
    private string _ioLaunchStatus = "Idle";
    private string _reproLaunchStatus = "Idle";
    private string _speciationLaunchStatus = "Idle";
    private string _workerLaunchStatus = "Idle";
    private string _obsLaunchStatus = "Idle";
    private string _sampleBrainStatus = "Not running.";
    private string _profileCurrentSystemStatus = "Idle";
    private string _pullSettingsHost = string.Empty;
    private string _pullSettingsPortText = string.Empty;
    private string _pullSettingsName = string.Empty;
    private string _settingsPullStatus = "Pull excludes discovery endpoint settings.";
    private string _lastSeededPullSettingsHost = string.Empty;
    private string _lastSeededPullSettingsPortText = string.Empty;
    private string _lastSeededPullSettingsName = string.Empty;
    private string _workerEndpointSummary = "No active workers.";
    private string _workerCapabilityRefreshSecondsText = WorkerCapabilitySettingsKeys.DefaultBenchmarkRefreshSeconds.ToString(CultureInfo.InvariantCulture);
    private string _workerPressureRebalanceWindowText = WorkerCapabilitySettingsKeys.DefaultPressureRebalanceWindow.ToString(CultureInfo.InvariantCulture);
    private string _workerPressureViolationRatioText = WorkerCapabilityMath.FormatRatio(WorkerCapabilitySettingsKeys.DefaultPressureViolationRatio);
    private string _workerPressureTolerancePercentText = WorkerCapabilityMath.FormatRatio(WorkerCapabilitySettingsKeys.DefaultPressureLimitTolerancePercent);
    private string _workerPolicyStatus = "Settings-backed defaults.";
    private string _workerCapabilityRefreshSecondsServerValue = WorkerCapabilitySettingsKeys.DefaultBenchmarkRefreshSeconds.ToString(CultureInfo.InvariantCulture);
    private string _workerPressureRebalanceWindowServerValue = WorkerCapabilitySettingsKeys.DefaultPressureRebalanceWindow.ToString(CultureInfo.InvariantCulture);
    private string _workerPressureViolationRatioServerValue = WorkerCapabilityMath.FormatRatio(WorkerCapabilitySettingsKeys.DefaultPressureViolationRatio);
    private string _workerPressureTolerancePercentServerValue = WorkerCapabilityMath.FormatRatio(WorkerCapabilitySettingsKeys.DefaultPressureLimitTolerancePercent);
    private bool _workerCapabilityRefreshSecondsDirty;
    private bool _workerPressureRebalanceWindowDirty;
    private bool _workerPressureViolationRatioDirty;
    private bool _workerPressureTolerancePercentDirty;
    private readonly Dictionary<Guid, BrainListItem> _lastBrains = new();
    private readonly CancellationTokenSource _refreshCts = new();
    private static readonly HashSet<string> EndpointRefreshTriggerProperties = new(StringComparer.Ordinal)
    {
        nameof(ConnectionViewModel.SettingsConnected),
        nameof(ConnectionViewModel.SettingsStatus),
        nameof(ConnectionViewModel.SettingsHost),
        nameof(ConnectionViewModel.SettingsPortText),
        nameof(ConnectionViewModel.SettingsName),
        nameof(ConnectionViewModel.HiveMindConnected),
        nameof(ConnectionViewModel.HiveMindDiscoverable),
        nameof(ConnectionViewModel.HiveMindStatus),
        nameof(ConnectionViewModel.HiveMindEndpointDisplay),
        nameof(ConnectionViewModel.IoConnected),
        nameof(ConnectionViewModel.IoDiscoverable),
        nameof(ConnectionViewModel.IoStatus),
        nameof(ConnectionViewModel.IoEndpointDisplay),
        nameof(ConnectionViewModel.ReproConnected),
        nameof(ConnectionViewModel.ReproDiscoverable),
        nameof(ConnectionViewModel.ReproStatus),
        nameof(ConnectionViewModel.ReproEndpointDisplay),
        nameof(ConnectionViewModel.SpeciationDiscoverable),
        nameof(ConnectionViewModel.SpeciationStatus),
        nameof(ConnectionViewModel.SpeciationEndpointDisplay),
        nameof(ConnectionViewModel.ObsConnected),
        nameof(ConnectionViewModel.ObsDiscoverable),
        nameof(ConnectionViewModel.ObsStatus),
        nameof(ConnectionViewModel.ObsEndpointDisplay)
    };
    private readonly TimeSpan _autoRefreshInterval = TimeSpan.FromSeconds(3);
    private Guid? _sampleBrainId;

    public OrchestratorPanelViewModel(
        UiDispatcher dispatcher,
        ConnectionViewModel connections,
        WorkbenchClient client,
        Action<Guid>? brainDiscovered = null,
        Action<IReadOnlyList<BrainListItem>>? brainsUpdated = null,
        Func<Task>? connectAll = null,
        Action? disconnectAll = null,
        ILocalProjectLaunchPreparer? launchPreparer = null)
    {
        _dispatcher = dispatcher;
        _connections = connections;
        _client = client;
        _launchPreparer = launchPreparer ?? new LocalProjectLaunchPreparer();
        _brainDiscovered = brainDiscovered;
        _brainsUpdated = brainsUpdated;
        _connectAll = connectAll;
        _disconnectAll = disconnectAll;
        SeedPullSettingsSourceFromConnections(force: true);
        _connections.PropertyChanged += OnConnectionsPropertyChanged;
        Nodes = new ObservableCollection<NodeStatusItem>();
        WorkerEndpoints = new ObservableCollection<WorkerEndpointItem>();
        Endpoints = new ObservableCollection<EndpointStatusItem>();
        Actors = new ObservableCollection<NodeStatusItem>();
        Settings = new ObservableCollection<SettingEntryViewModel>();
        Terminations = new ObservableCollection<BrainTerminatedItem>();
        RefreshCommand = new AsyncRelayCommand(() => RefreshAsync(force: true));
        ApplySettingsCommand = new AsyncRelayCommand(ApplySettingsAsync);
        PullSettingsCommand = new AsyncRelayCommand(PullSettingsAsync);
        ApplyWorkerPolicyCommand = new AsyncRelayCommand(ApplyWorkerPolicyAsync);
        StartSettingsMonitorCommand = new AsyncRelayCommand(StartSettingsMonitorAsync);
        StopSettingsMonitorCommand = new AsyncRelayCommand(() => StopRunnerAsync(_settingsRunner, value => SettingsLaunchStatus = value));
        StartHiveMindCommand = new AsyncRelayCommand(StartHiveMindAsync);
        StopHiveMindCommand = new AsyncRelayCommand(() => StopRunnerAsync(_hiveMindRunner, value => HiveMindLaunchStatus = value));
        StartIoCommand = new AsyncRelayCommand(StartIoAsync);
        StopIoCommand = new AsyncRelayCommand(() => StopRunnerAsync(_ioRunner, value => IoLaunchStatus = value));
        StartReproCommand = new AsyncRelayCommand(StartReproAsync);
        StopReproCommand = new AsyncRelayCommand(() => StopRunnerAsync(_reproRunner, value => ReproLaunchStatus = value));
        StartSpeciationCommand = new AsyncRelayCommand(StartSpeciationAsync);
        StopSpeciationCommand = new AsyncRelayCommand(() => StopRunnerAsync(_speciationRunner, value => SpeciationLaunchStatus = value));
        StartWorkerCommand = new AsyncRelayCommand(StartWorkerAsync);
        StopWorkerCommand = new AsyncRelayCommand(() => StopRunnerAsync(_workerRunner, value => WorkerLaunchStatus = value));
        StartObsCommand = new AsyncRelayCommand(StartObsAsync);
        StopObsCommand = new AsyncRelayCommand(() => StopRunnerAsync(_obsRunner, value => ObsLaunchStatus = value));
        StartAllCommand = new AsyncRelayCommand(StartAllAsync);
        StopAllCommand = new AsyncRelayCommand(StopAllAsync);
        SpawnSampleBrainCommand = new AsyncRelayCommand(SpawnSampleBrainAsync);
        StopSampleBrainCommand = new AsyncRelayCommand(StopSampleBrainAsync);
        ProfileCurrentSystemCommand = new AsyncRelayCommand(ProfileCurrentSystemAsync);
        RefreshEndpointRows();
        _ = StartAutoRefreshAsync();
    }

    public ObservableCollection<NodeStatusItem> Nodes { get; }
    public ObservableCollection<WorkerEndpointItem> WorkerEndpoints { get; }
    public ObservableCollection<EndpointStatusItem> Endpoints { get; }
    public ObservableCollection<NodeStatusItem> Actors { get; }
    public ObservableCollection<SettingEntryViewModel> Settings { get; }
    public ObservableCollection<BrainTerminatedItem> Terminations { get; }

    public ConnectionViewModel Connections => _connections;

    public string StatusMessage
    {
        get => _statusMessage;
        set => SetProperty(ref _statusMessage, value);
    }

    public AsyncRelayCommand RefreshCommand { get; }

    public AsyncRelayCommand ApplySettingsCommand { get; }

    public AsyncRelayCommand PullSettingsCommand { get; }

    public AsyncRelayCommand ApplyWorkerPolicyCommand { get; }

    public AsyncRelayCommand StartSettingsMonitorCommand { get; }

    public AsyncRelayCommand StopSettingsMonitorCommand { get; }

    public AsyncRelayCommand StartHiveMindCommand { get; }

    public AsyncRelayCommand StopHiveMindCommand { get; }

    public AsyncRelayCommand StartIoCommand { get; }

    public AsyncRelayCommand StopIoCommand { get; }

    public AsyncRelayCommand StartReproCommand { get; }

    public AsyncRelayCommand StopReproCommand { get; }

    public AsyncRelayCommand StartWorkerCommand { get; }

    public AsyncRelayCommand StopWorkerCommand { get; }

    public AsyncRelayCommand StartSpeciationCommand { get; }

    public AsyncRelayCommand StopSpeciationCommand { get; }

    public AsyncRelayCommand StartObsCommand { get; }

    public AsyncRelayCommand StopObsCommand { get; }
    public AsyncRelayCommand StartAllCommand { get; }

    public AsyncRelayCommand StopAllCommand { get; }

    public AsyncRelayCommand SpawnSampleBrainCommand { get; }

    public AsyncRelayCommand StopSampleBrainCommand { get; }

    public AsyncRelayCommand ProfileCurrentSystemCommand { get; }

    public string SettingsLaunchStatus
    {
        get => _settingsLaunchStatus;
        set => SetProperty(ref _settingsLaunchStatus, value);
    }

    public string HiveMindLaunchStatus
    {
        get => _hiveMindLaunchStatus;
        set => SetProperty(ref _hiveMindLaunchStatus, value);
    }

    public string IoLaunchStatus
    {
        get => _ioLaunchStatus;
        set => SetProperty(ref _ioLaunchStatus, value);
    }

    public string ReproLaunchStatus
    {
        get => _reproLaunchStatus;
        set => SetProperty(ref _reproLaunchStatus, value);
    }

    public string SpeciationLaunchStatus
    {
        get => _speciationLaunchStatus;
        set => SetProperty(ref _speciationLaunchStatus, value);
    }

    public string WorkerLaunchStatus
    {
        get => _workerLaunchStatus;
        set => SetProperty(ref _workerLaunchStatus, value);
    }

    public string ObsLaunchStatus
    {
        get => _obsLaunchStatus;
        set => SetProperty(ref _obsLaunchStatus, value);
    }

    public string SampleBrainStatus
    {
        get => _sampleBrainStatus;
        set => SetProperty(ref _sampleBrainStatus, value);
    }

    public string WorkerEndpointSummary
    {
        get => _workerEndpointSummary;
        set => SetProperty(ref _workerEndpointSummary, value);
    }

    public string WorkerCapabilityRefreshSecondsText
    {
        get => _workerCapabilityRefreshSecondsText;
        set
        {
            if (SetProperty(ref _workerCapabilityRefreshSecondsText, value))
            {
                _workerCapabilityRefreshSecondsDirty = !string.Equals(
                    NormalizeWorkerPolicyValue(value),
                    _workerCapabilityRefreshSecondsServerValue,
                    StringComparison.Ordinal);
            }
        }
    }

    public string WorkerPressureRebalanceWindowText
    {
        get => _workerPressureRebalanceWindowText;
        set
        {
            if (SetProperty(ref _workerPressureRebalanceWindowText, value))
            {
                _workerPressureRebalanceWindowDirty = !string.Equals(
                    NormalizeWorkerPolicyValue(value),
                    _workerPressureRebalanceWindowServerValue,
                    StringComparison.Ordinal);
            }
        }
    }

    public string WorkerPressureViolationRatioText
    {
        get => _workerPressureViolationRatioText;
        set
        {
            if (SetProperty(ref _workerPressureViolationRatioText, value))
            {
                _workerPressureViolationRatioDirty = !string.Equals(
                    NormalizeWorkerPolicyValue(value),
                    _workerPressureViolationRatioServerValue,
                    StringComparison.Ordinal);
            }
        }
    }

    public string WorkerPressureTolerancePercentText
    {
        get => _workerPressureTolerancePercentText;
        set
        {
            if (SetProperty(ref _workerPressureTolerancePercentText, value))
            {
                _workerPressureTolerancePercentDirty = !string.Equals(
                    NormalizeWorkerPolicyValue(value),
                    _workerPressureTolerancePercentServerValue,
                    StringComparison.Ordinal);
            }
        }
    }

    public string WorkerPolicyStatus
    {
        get => _workerPolicyStatus;
        set => SetProperty(ref _workerPolicyStatus, value);
    }

    public string ProfileCurrentSystemStatus
    {
        get => _profileCurrentSystemStatus;
        set => SetProperty(ref _profileCurrentSystemStatus, value);
    }

    public string PullSettingsHost
    {
        get => _pullSettingsHost;
        set => SetProperty(ref _pullSettingsHost, value);
    }

    public string PullSettingsPortText
    {
        get => _pullSettingsPortText;
        set => SetProperty(ref _pullSettingsPortText, value);
    }

    public string PullSettingsName
    {
        get => _pullSettingsName;
        set => SetProperty(ref _pullSettingsName, value);
    }

    public string SettingsPullStatus
    {
        get => _settingsPullStatus;
        set => SetProperty(ref _settingsPullStatus, value);
    }

    public void UpdateSetting(SettingItem item)
    {
        _dispatcher.Post(() =>
        {
            var index = Settings.Select((entry, idx) => new { entry, idx })
                .FirstOrDefault(row => string.Equals(row.entry.Key, item.Key, StringComparison.OrdinalIgnoreCase))?.idx ?? -1;

            if (index >= 0)
            {
                Settings[index].UpdateFromServer(item.Value, FormatUpdated(item.Updated), preserveEdits: true);
            }
            else
            {
                Settings.Add(new SettingEntryViewModel(item.Key, item.Value, FormatUpdated(item.Updated)));
            }

            TryApplyWorkerPolicySetting(item);
            TryApplyServiceEndpointSetting(item);
        });
    }

    public void AddTermination(BrainTerminatedItem item)
    {
        _dispatcher.Post(() =>
        {
            Terminations.Insert(0, item);
            Trim(Terminations);
        });
    }

    public async Task StopAllAsyncForShutdown()
    {
        _connections.PropertyChanged -= OnConnectionsPropertyChanged;
        _refreshCts.Cancel();
        _disconnectAll?.Invoke();
        await StopSampleBrainAsync().ConfigureAwait(false);
        await StopRunnerAsync(_workerRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_settingsRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_hiveMindRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_ioRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_reproRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_speciationRunner, value => SpeciationLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_obsRunner, _ => { }).ConfigureAwait(false);
    }

    private async Task StartSettingsMonitorAsync()
    {
        var configuredDbPath = Connections.SettingsDbPath?.Trim();
        var defaultDbPath = BuildDefaultSettingsDbPath();
        var resolvedDbPath = string.IsNullOrWhiteSpace(configuredDbPath) ? defaultDbPath : configuredDbPath;
        var includeDbArg = !string.IsNullOrWhiteSpace(configuredDbPath)
            && !PathsEqual(configuredDbPath, defaultDbPath);

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.SettingsMonitor");
        if (!TryParsePort(Connections.SettingsPortText, out var port))
        {
            SettingsLaunchStatus = "Invalid Settings port.";
            return;
        }

        var args = includeDbArg
            ? $"--db \"{resolvedDbPath}\" --bind-host {Connections.SettingsHost} --port {port}"
            : $"--bind-host {Connections.SettingsHost} --port {port}";
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Runtime.SettingsMonitor", args, "SettingsMonitor").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            SettingsLaunchStatus = launch.Message;
            return;
        }

        var startInfo = launch.StartInfo;
        var result = await _settingsRunner.StartAsync(startInfo, waitForExit: false, label: "SettingsMonitor");
        SettingsLaunchStatus = result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartHiveMindAsync()
    {
        if (!TryParsePort(Connections.HiveMindPortText, out var port))
        {
            HiveMindLaunchStatus = "Invalid HiveMind port.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.HiveMind");
        var settingsDbPath = ResolveSettingsDbPath();
        var args = $"--bind-host {Connections.HiveMindHost} --port {port} --settings-db \"{settingsDbPath}\""
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}"
                 + $" --tick-hz {LocalDefaultTickHz:0.###} --min-tick-hz {LocalDefaultMinTickHz:0.###}";
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Runtime.HiveMind", args, "HiveMind").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            HiveMindLaunchStatus = launch.Message;
            return;
        }

        var startInfo = launch.StartInfo;
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
        var result = await _hiveMindRunner.StartAsync(startInfo, waitForExit: false, label: "HiveMind");
        HiveMindLaunchStatus = result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartIoAsync()
    {
        if (!TryParsePort(Connections.IoPortText, out var port))
        {
            IoLaunchStatus = "Invalid IO port.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.IO");
        var args = $"--bind-host {Connections.IoHost} --port {port}"
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}";
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Runtime.IO", args, "IoGateway").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            IoLaunchStatus = launch.Message;
            return;
        }

        var startInfo = launch.StartInfo;
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
        var result = await _ioRunner.StartAsync(startInfo, waitForExit: false, label: "IoGateway");
        IoLaunchStatus = result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartReproAsync()
    {
        if (!TryParsePort(Connections.ReproPortText, out var reproPort))
        {
            ReproLaunchStatus = "Invalid Reproduction port.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.Reproduction");
        var args = $"--bind-host {Connections.ReproHost} --port {reproPort}"
                 + $" --manager-name {Connections.ReproManager}"
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}";
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Runtime.Reproduction", args, "Reproduction").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            ReproLaunchStatus = launch.Message;
            return;
        }

        var startInfo = launch.StartInfo;
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
        var result = await _reproRunner.StartAsync(startInfo, waitForExit: false, label: "Reproduction");
        ReproLaunchStatus = result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartSpeciationAsync()
    {
        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.Speciation");
        if (!TryParsePort(Connections.SpeciationPortText, out var speciationPort))
        {
            SpeciationLaunchStatus = "Invalid Speciation port.";
            return;
        }

        if (!TryParsePort(Connections.SettingsPortText, out var settingsPort))
        {
            SpeciationLaunchStatus = "Invalid Settings port.";
            return;
        }

        var args = $"--bind-host {Connections.SpeciationHost} --port {speciationPort}"
                 + $" --manager-name {Connections.SpeciationManager}"
                 + $" --settings-host {Connections.SettingsHost} --settings-port {settingsPort} --settings-name {Connections.SettingsName}";
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Runtime.Speciation", args, "Speciation").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            SpeciationLaunchStatus = launch.Message;
            StatusMessage = $"Speciation launch: {launch.Message}";
            return;
        }

        var startInfo = launch.StartInfo;
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
        var result = await _speciationRunner.StartAsync(startInfo, waitForExit: false, label: "Speciation");
        SpeciationLaunchStatus = result.Message;
        StatusMessage = $"Speciation launch: {result.Message}";
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartWorkerAsync()
    {
        if (!TryParsePort(Connections.WorkerPortText, out var workerPort))
        {
            WorkerLaunchStatus = "Invalid worker port.";
            return;
        }

        if (!TryParsePort(Connections.SettingsPortText, out var settingsPort))
        {
            WorkerLaunchStatus = "Invalid Settings port.";
            return;
        }

        if (!TryParsePercent(Connections.WorkerCpuLimitPercentText, out var workerCpuLimitPercent))
        {
            WorkerLaunchStatus = "Invalid worker CPU limit.";
            return;
        }

        if (!TryParsePercent(Connections.WorkerRamLimitPercentText, out var workerRamLimitPercent))
        {
            WorkerLaunchStatus = "Invalid worker RAM limit.";
            return;
        }

        if (!TryParsePercent(Connections.WorkerGpuLimitPercentText, out var workerGpuLimitPercent))
        {
            WorkerLaunchStatus = "Invalid worker GPU limit.";
            return;
        }

        if (!TryParsePercent(Connections.WorkerVramLimitPercentText, out var workerVramLimitPercent))
        {
            WorkerLaunchStatus = "Invalid worker VRAM limit.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.WorkerNode");
        var args = $"--bind-host {Connections.WorkerHost} --port {workerPort}"
                 + $" --logical-name {Connections.WorkerLogicalName}"
                 + $" --root-name {Connections.WorkerRootName}"
                 + $" --cpu-pct {workerCpuLimitPercent}"
                 + $" --ram-pct {workerRamLimitPercent}"
                 + $" --storage-pct {LocalDefaultWorkerStorageLimitPercent}"
                 + $" --gpu-compute-pct {workerGpuLimitPercent}"
                 + $" --gpu-vram-pct {workerVramLimitPercent}"
                 + $" --settings-host {Connections.SettingsHost} --settings-port {settingsPort} --settings-name {Connections.SettingsName}";
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Runtime.WorkerNode", args, "WorkerNode").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            WorkerLaunchStatus = launch.Message;
            return;
        }

        var startInfo = launch.StartInfo;
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
        ApplyObservabilityEnvironment(startInfo);
        var result = await _workerRunner.StartAsync(startInfo, waitForExit: false, label: "WorkerNode");
        WorkerLaunchStatus = result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartObsAsync()
    {
        if (!TryParsePort(Connections.ObsPortText, out var port))
        {
            ObsLaunchStatus = "Invalid Obs port.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.Observability");
        var args = $"--bind-host {Connections.ObsHost} --port {port}"
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}"
                 + " --enable-debug --enable-viz";
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Runtime.Observability", args, "Observability").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            ObsLaunchStatus = launch.Message;
            return;
        }

        var startInfo = launch.StartInfo;
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
        var result = await _obsRunner.StartAsync(startInfo, waitForExit: false, label: "Observability");
        ObsLaunchStatus = result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private static async Task StopRunnerAsync(LocalServiceRunner runner, Action<string> setStatus)
    {
        setStatus(await runner.StopAsync().ConfigureAwait(false));
    }

    public async Task RefreshSettingsAsync()
    {
        await RefreshAsync(force: true).ConfigureAwait(false);
    }

    public Task StartSpeciationServiceAsync()
        => StartSpeciationAsync();

    public Task StopSpeciationServiceAsync()
        => StopRunnerAsync(_speciationRunner, value => SpeciationLaunchStatus = value);

    private async Task RefreshAsync(bool force)
    {
        if (!await _refreshGate.WaitAsync(0).ConfigureAwait(false))
        {
            return;
        }

        if (!Connections.SettingsConnected)
        {
            if (force)
            {
                StatusMessage = "SettingsMonitor not connected.";
                Connections.SettingsStatus = "Disconnected";
            }
            SetDiscoveryUnavailable();
            _refreshGate.Release();
            return;
        }

        if (force)
        {
            StatusMessage = "Loading settings...";
            Connections.SettingsStatus = "Loading";
        }

        try
        {
            var nodesResponseTask = _client.ListNodesAsync();
            var brainsResponseTask = _client.ListBrainsAsync();
            var workerInventoryResponseTask = _client.ListWorkerInventorySnapshotAsync();
            var settingsResponseTask = _client.ListSettingsAsync();

            var nodesResponse = await nodesResponseTask.ConfigureAwait(false);
            var brainsResponse = await brainsResponseTask.ConfigureAwait(false);
            var workerInventoryResponse = await workerInventoryResponseTask.ConfigureAwait(false);
            var settingsResponse = await settingsResponseTask.ConfigureAwait(false);

            var nodes = nodesResponse?.Nodes?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.NodeStatus>();
            var brains = brainsResponse?.Brains?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.BrainStatus>();
            var controllers = brainsResponse?.Controllers?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.BrainControllerStatus>();
            var workerInventory = workerInventoryResponse?.Workers?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.WorkerReadinessCapability>();
            var sortedNodes = nodes
                .OrderByDescending(node => node.LastSeenMs)
                .ThenBy(node => node.LogicalName ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                .ToArray();

            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var actorRowsResult = await BuildActorRowsAsync(controllers, sortedNodes, brains, nowMs).ConfigureAwait(false);
            var workerEndpointState = BuildWorkerEndpointState(
                sortedNodes,
                workerInventory,
                actorRowsResult.WorkerBrainHints,
                nowMs);
            var settings = settingsResponse?.Settings?
                .Select(entry => new SettingItem(
                    entry.Key ?? string.Empty,
                    entry.Value ?? string.Empty,
                    FormatUpdated(entry.UpdatedMs)))
                .ToList() ?? new List<SettingItem>();
            var discoveredServiceEndpoints = BuildServiceEndpointLookup(settings);

            _dispatcher.Post(() =>
            {
                Nodes.Clear();
                WorkerEndpoints.Clear();
                Actors.Clear();
                foreach (var node in sortedNodes)
                {
                    var isFresh = IsFresh(node.LastSeenMs, nowMs);
                    var isAlive = node.IsAlive && isFresh;
                    var seen = DateTimeOffset.FromUnixTimeMilliseconds((long)node.LastSeenMs).ToLocalTime();
                    Nodes.Add(new NodeStatusItem(
                        node.LogicalName ?? string.Empty,
                        node.Address ?? string.Empty,
                        node.RootActorName ?? string.Empty,
                        seen.ToString("g"),
                        isAlive ? "online" : "offline"));
                }

                foreach (var workerRow in workerEndpointState.Rows)
                {
                    WorkerEndpoints.Add(workerRow);
                }

                foreach (var actorRow in actorRowsResult.Rows)
                {
                    Actors.Add(actorRow);
                }

                foreach (var entry in settings)
                {
                    var existing = Settings.FirstOrDefault(item => string.Equals(item.Key, entry.Key, StringComparison.OrdinalIgnoreCase));
                    if (existing is null)
                    {
                        Settings.Add(new SettingEntryViewModel(entry.Key, entry.Value, FormatUpdated(entry.Updated)));
                    }
                    else
                    {
                        existing.UpdateFromServer(entry.Value, FormatUpdated(entry.Updated), preserveEdits: true);
                    }

                    TryApplyWorkerPolicySetting(entry);
                }

                var hiveMindFromSettings = ApplyServiceEndpointSettingsToConnections(settings);
                if (!hiveMindFromSettings)
                {
                    UpdateHiveMindEndpoint(nodes, nowMs);
                }

                WorkerEndpointSummary = workerEndpointState.SummaryText;
                Trim(Nodes);
                Trim(WorkerEndpoints);
                Trim(Actors);
                Trim(Settings);
            });

            UpdateConnectionStatusesFromNodes(nodes, nowMs, workerEndpointState, discoveredServiceEndpoints);

            var controllerMap = controllers
                .Where(entry => entry.BrainId is not null && entry.BrainId.TryToGuid(out _))
                .ToDictionary(
                    entry => entry.BrainId!.ToGuid(),
                    entry =>
                    {
                        var controllerAlive = entry.IsAlive && IsFresh(entry.LastSeenMs, nowMs);
                        return (entry, controllerAlive);
                    });

            var brainEntries = brains.Select(entry =>
            {
                var brainId = entry.BrainId?.ToGuid() ?? Guid.Empty;
                var alive = controllerMap.TryGetValue(brainId, out var controller) && controller.Item2;
                var spawnedRecently = IsSpawnRecent(entry.SpawnedMs, nowMs);
                var item = new BrainListItem(brainId, entry.State ?? string.Empty, alive);
                return (item, spawnedRecently);
            }).Where(entry => entry.item.BrainId != Guid.Empty).ToList();

            var brainListAll = brainEntries
                .Select(entry => entry.item)
                .ToList();
            RecordBrainTerminations(brainListAll);
            var brainList = brainEntries
                .Where(entry => !string.Equals(entry.item.State, "Dead", StringComparison.OrdinalIgnoreCase))
                .Where(entry => entry.item.ControllerAlive || entry.spawnedRecently)
                .Select(entry => entry.item)
                .ToList();
            _brainsUpdated?.Invoke(brainList);

            if (force)
            {
                StatusMessage = "Settings loaded.";
                Connections.SettingsStatus = "Ready";
            }
        }
        catch (Exception ex)
        {
            StatusMessage = $"Settings load failed: {ex.Message}";
            Connections.SettingsStatus = "Error";
            SetDiscoveryUnavailable();
            WorkbenchLog.Warn($"Settings refresh failed: {ex.Message}");
        }
        finally
        {
            _refreshGate.Release();
        }
    }

    private async Task StartAllAsync()
    {
        await StartSettingsMonitorAsync().ConfigureAwait(false);
        await StartHiveMindAsync().ConfigureAwait(false);
        await StartWorkerAsync().ConfigureAwait(false);
        await StartReproAsync().ConfigureAwait(false);
        await StartSpeciationAsync().ConfigureAwait(false);
        await StartIoAsync().ConfigureAwait(false);
        await StartObsAsync().ConfigureAwait(false);
    }

    private async Task StopAllAsync()
    {
        _disconnectAll?.Invoke();
        await StopSampleBrainAsync().ConfigureAwait(false);
        await StopRunnerAsync(_obsRunner, value => ObsLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_ioRunner, value => IoLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_reproRunner, value => ReproLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_speciationRunner, value => SpeciationLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_workerRunner, value => WorkerLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_hiveMindRunner, value => HiveMindLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_settingsRunner, value => SettingsLaunchStatus = value).ConfigureAwait(false);
    }

    private async Task ProfileCurrentSystemAsync()
    {
        if (!HasSpawnServiceReadiness())
        {
            ProfileCurrentSystemStatus = Connections.BuildSpawnReadinessGuidance();
            StatusMessage = $"Profile current system: {ProfileCurrentSystemStatus}";
            return;
        }

        if (!TryParsePort(Connections.SettingsPortText, out var settingsPort))
        {
            ProfileCurrentSystemStatus = "Invalid Settings port.";
            StatusMessage = $"Profile current system: {ProfileCurrentSystemStatus}";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("tools", "Nbn.Tools.PerfProbe");
        var outputDirectory = ResolveProfileCurrentSystemOutputDirectory();
        Directory.CreateDirectory(outputDirectory);
        var bindPort = ResolveProfileClientPort();
        var args = $"current-system --settings-host {Connections.SettingsHost} --settings-port {settingsPort} --settings-name {Connections.SettingsName}"
                 + $" --bind-host {Connections.LocalBindHost} --bind-port {bindPort}"
                 + $" --output-dir \"{outputDirectory}\"";
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Tools.PerfProbe", args, "PerfProbe").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            ProfileCurrentSystemStatus = launch.Message;
            StatusMessage = $"Profile current system: {launch.Message}";
            return;
        }

        ProfileCurrentSystemStatus = "Profiling current runtime...";
        StatusMessage = "Profile current system: running.";
        var result = await _profileCurrentSystemRunner.StartAsync(launch.StartInfo, waitForExit: true, label: "PerfProbe").ConfigureAwait(false);
        if (!result.Success)
        {
            ProfileCurrentSystemStatus = result.Message;
            StatusMessage = $"Profile current system: {result.Message}";
            return;
        }

        var reportPath = Path.Combine(outputDirectory, "perf-report.html");
        if (!File.Exists(reportPath))
        {
            ProfileCurrentSystemStatus = "Perf probe did not produce report artifacts.";
            StatusMessage = $"Profile current system: {ProfileCurrentSystemStatus}";
            return;
        }

        ProfileCurrentSystemStatus = $"Completed. Report: {reportPath}";
        StatusMessage = "Profile current system: completed.";
    }

    private async Task SpawnSampleBrainAsync()
    {
        if (!HasSpawnServiceReadiness())
        {
            SampleBrainStatus = Connections.BuildSpawnReadinessGuidance();
            return;
        }

        if (IsSampleBrainRunning())
        {
            SampleBrainStatus = "Sample brain already running.";
            return;
        }

        _ = await _client.GetPlacementWorkerInventoryAsync().ConfigureAwait(false);

        var runRoot = BuildSampleRunRoot();
        var artifactRoot = Path.Combine(runRoot, "artifacts");
        ResetDirectory(artifactRoot);

        SampleBrainStatus = "Creating sample artifacts...";
        var artifact = await CreateSampleArtifactsAsync(artifactRoot).ConfigureAwait(false);
        if (artifact is null)
        {
            return;
        }

        var artifactPath = string.IsNullOrWhiteSpace(artifact.ArtifactRoot) ? artifactRoot : artifact.ArtifactRoot;
        SampleBrainStatus = "Spawning sample brain via IO/HiveMind worker placement...";
        var spawnAck = await _client.SpawnBrainViaIoAsync(new Nbn.Proto.Control.SpawnBrain
        {
            BrainDef = artifact.Sha256.ToArtifactRef((ulong)Math.Max(0L, artifact.Size), "application/x-nbn", artifactPath)
        }).ConfigureAwait(false);
        if (spawnAck?.BrainId is null || !spawnAck.BrainId.TryToGuid(out var brainId) || brainId == Guid.Empty)
        {
            SampleBrainStatus = SpawnFailureFormatter.Format(
                prefix: "Sample spawn failed",
                ack: spawnAck,
                fallbackMessage: "Sample spawn failed: IO did not return a brain id.");
            return;
        }

        _sampleBrainId = brainId;

        SampleBrainStatus = "Waiting for sample brain placement/runtime readiness...";
        if (!await WaitForBrainRegistrationAsync(brainId).ConfigureAwait(false))
        {
            SampleBrainStatus = $"Sample brain failed to become visualization-ready ({brainId:D}) after IO/HiveMind worker placement.";
            await _client.KillBrainAsync(brainId, "workbench_sample_registration_timeout").ConfigureAwait(false);
            _sampleBrainId = null;
            return;
        }

        _brainDiscovered?.Invoke(brainId);
        await RefreshAsync(force: true).ConfigureAwait(false);
        SampleBrainStatus = $"Sample brain running ({brainId:D}). Spawned via IO; worker placement managed by HiveMind.";
    }

    private async Task StopSampleBrainAsync()
    {
        if (!IsSampleBrainRunning())
        {
            var stoppedId = _sampleBrainId;
            _sampleBrainId = null;
            SampleBrainStatus = stoppedId.HasValue
                ? $"Sample brain not running ({stoppedId:D})."
                : "Sample brain not running.";
            return;
        }

        var brainId = _sampleBrainId;
        var killSent = brainId.HasValue
            && await _client.KillBrainAsync(brainId.Value, "workbench_sample_stop").ConfigureAwait(false);
        _sampleBrainId = null;
        SampleBrainStatus = brainId.HasValue
            ? killSent
                ? $"Sample brain stop requested ({brainId:D})."
                : $"Sample brain stop request failed ({brainId:D})."
            : "Sample brain stop request failed.";
    }

    private bool IsSampleBrainRunning()
        => _sampleBrainId.HasValue;

    private async Task<bool> WaitForBrainRegistrationAsync(Guid brainId)
    {
        var deadline = DateTime.UtcNow + SpawnRegistrationTimeout;
        while (DateTime.UtcNow <= deadline)
        {
            var response = await _client.ListBrainsAsync().ConfigureAwait(false);
            var lifecycle = await _client.GetPlacementLifecycleAsync(brainId).ConfigureAwait(false);
            if (IsBrainRegistered(response, brainId) && IsPlacementVisualizationReady(lifecycle, brainId))
            {
                return true;
            }

            await Task.Delay(SpawnRegistrationPollInterval).ConfigureAwait(false);
        }

        return false;
    }

    private static bool IsBrainRegistered(Nbn.Proto.Settings.BrainListResponse? response, Guid brainId)
    {
        if (response?.Brains is null)
        {
            return false;
        }

        var brainPresentAndActive = false;
        foreach (var entry in response.Brains)
        {
            if (entry.BrainId is null || !entry.BrainId.TryToGuid(out var candidate) || candidate != brainId)
            {
                continue;
            }

            if (string.Equals(entry.State, "Dead", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            brainPresentAndActive = true;
            break;
        }

        if (!brainPresentAndActive)
        {
            return false;
        }

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return HasLiveController(response.Controllers, brainId, nowMs);
    }

    private static bool IsPlacementVisualizationReady(PlacementLifecycleInfo? lifecycle, Guid brainId)
    {
        if (lifecycle?.BrainId is null
            || !lifecycle.BrainId.TryToGuid(out var candidate)
            || candidate != brainId)
        {
            return false;
        }

        return lifecycle.LifecycleState == PlacementLifecycleState.PlacementLifecycleRunning
               && lifecycle.RegisteredShards > 0;
    }

    private static bool HasLiveController(
        IEnumerable<Nbn.Proto.Settings.BrainControllerStatus>? controllers,
        Guid brainId,
        long nowMs)
    {
        if (controllers is null)
        {
            return false;
        }

        foreach (var controller in controllers)
        {
            if (controller.BrainId is null
                || !controller.BrainId.TryToGuid(out var candidate)
                || candidate != brainId)
            {
                continue;
            }

            if (controller.IsAlive && IsFresh(controller.LastSeenMs, nowMs))
            {
                return true;
            }
        }

        return false;
    }

    private async Task StartAutoRefreshAsync()
    {
        while (!_refreshCts.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_autoRefreshInterval, _refreshCts.Token).ConfigureAwait(false);
                await RefreshAsync(force: false).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private bool TryApplyWorkerPolicySetting(SettingItem item)
    {
        if (string.IsNullOrWhiteSpace(item.Key))
        {
            return false;
        }

        if (string.Equals(item.Key, WorkerCapabilitySettingsKeys.BenchmarkRefreshSecondsKey, StringComparison.OrdinalIgnoreCase))
        {
            ApplyWorkerPolicyServerValue(
                NormalizeWorkerPolicyValue(item.Value, _workerCapabilityRefreshSecondsServerValue),
                ref _workerCapabilityRefreshSecondsServerValue,
                ref _workerCapabilityRefreshSecondsDirty,
                value => WorkerCapabilityRefreshSecondsText = value);
            return true;
        }

        if (string.Equals(item.Key, WorkerCapabilitySettingsKeys.PressureRebalanceWindowKey, StringComparison.OrdinalIgnoreCase))
        {
            ApplyWorkerPolicyServerValue(
                NormalizeWorkerPolicyValue(item.Value, _workerPressureRebalanceWindowServerValue),
                ref _workerPressureRebalanceWindowServerValue,
                ref _workerPressureRebalanceWindowDirty,
                value => WorkerPressureRebalanceWindowText = value);
            return true;
        }

        if (string.Equals(item.Key, WorkerCapabilitySettingsKeys.PressureViolationRatioKey, StringComparison.OrdinalIgnoreCase))
        {
            ApplyWorkerPolicyServerValue(
                NormalizeWorkerPolicyValue(item.Value, _workerPressureViolationRatioServerValue),
                ref _workerPressureViolationRatioServerValue,
                ref _workerPressureViolationRatioDirty,
                value => WorkerPressureViolationRatioText = value);
            return true;
        }

        if (string.Equals(item.Key, WorkerCapabilitySettingsKeys.PressureLimitTolerancePercentKey, StringComparison.OrdinalIgnoreCase))
        {
            ApplyWorkerPolicyServerValue(
                NormalizeWorkerPolicyValue(item.Value, _workerPressureTolerancePercentServerValue),
                ref _workerPressureTolerancePercentServerValue,
                ref _workerPressureTolerancePercentDirty,
                value => WorkerPressureTolerancePercentText = value);
            return true;
        }

        return false;
    }

    private async Task ApplyWorkerPolicyAsync()
    {
        if (!Connections.SettingsConnected)
        {
            WorkerPolicyStatus = "SettingsMonitor not connected.";
            return;
        }

        if (!TryParseNonNegativeInt(WorkerCapabilityRefreshSecondsText, out var refreshSeconds))
        {
            WorkerPolicyStatus = "Invalid capability refresh seconds.";
            return;
        }

        if (!TryParsePositiveInt(WorkerPressureRebalanceWindowText, out var window))
        {
            WorkerPolicyStatus = "Invalid pressure window.";
            return;
        }

        if (!TryParseNonNegativeDouble(WorkerPressureViolationRatioText, out var violationRatio))
        {
            WorkerPolicyStatus = "Invalid pressure violation ratio.";
            return;
        }

        if (!TryParseNonNegativeDouble(WorkerPressureTolerancePercentText, out var tolerancePercent))
        {
            WorkerPolicyStatus = "Invalid pressure tolerance percent.";
            return;
        }

        var desired = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [WorkerCapabilitySettingsKeys.BenchmarkRefreshSecondsKey] = refreshSeconds.ToString(CultureInfo.InvariantCulture),
            [WorkerCapabilitySettingsKeys.PressureRebalanceWindowKey] = Math.Max(1, window).ToString(CultureInfo.InvariantCulture),
            [WorkerCapabilitySettingsKeys.PressureViolationRatioKey] = WorkerCapabilityMath.FormatRatio(Math.Clamp(violationRatio, 0d, 1d)),
            [WorkerCapabilitySettingsKeys.PressureLimitTolerancePercentKey] = WorkerCapabilityMath.FormatRatio(Math.Max(0d, tolerancePercent))
        };

        var dirtyKeys = desired
            .Where(entry => !string.Equals(GetWorkerPolicyServerValue(entry.Key), entry.Value, StringComparison.Ordinal))
            .ToArray();
        if (dirtyKeys.Length == 0)
        {
            WorkerPolicyStatus = "No worker policy changes.";
            return;
        }

        WorkerPolicyStatus = $"Applying {dirtyKeys.Length} worker policy setting(s)...";
        foreach (var entry in dirtyKeys)
        {
            var result = await _client.SetSettingAsync(entry.Key, entry.Value).ConfigureAwait(false);
            if (result is null)
            {
                continue;
            }

            _dispatcher.Post(() =>
            {
                MarkWorkerPolicyApplied(
                    result.Key ?? entry.Key,
                    result.Value ?? entry.Value);
            });
        }

        WorkerPolicyStatus = "Worker policy updated.";
    }

    private async Task ApplySettingsAsync()
    {
        if (!Connections.SettingsConnected)
        {
            StatusMessage = "SettingsMonitor not connected.";
            return;
        }

        var dirty = Settings.Where(entry => entry.IsDirty).ToList();
        if (dirty.Count == 0)
        {
            StatusMessage = "No settings changes.";
            return;
        }

        StatusMessage = $"Applying {dirty.Count} setting(s)...";
        foreach (var entry in dirty)
        {
            var result = await _client.SetSettingAsync(entry.Key, entry.Value).ConfigureAwait(false);
            if (result is null)
            {
                continue;
            }

            _dispatcher.Post(() =>
            {
                entry.MarkApplied(result.Value ?? entry.Value, FormatUpdated(result.UpdatedMs));
            });
        }

        StatusMessage = "Settings updated.";
    }

    private async Task PullSettingsAsync()
    {
        if (!Connections.SettingsConnected)
        {
            SettingsPullStatus = "Connect the current SettingsMonitor first.";
            StatusMessage = SettingsPullStatus;
            return;
        }

        var sourceHost = PullSettingsHost?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(sourceHost))
        {
            SettingsPullStatus = "Pull source host is required.";
            StatusMessage = SettingsPullStatus;
            return;
        }

        if (!TryParsePort(PullSettingsPortText, out var sourcePort))
        {
            SettingsPullStatus = "Invalid pull source port.";
            StatusMessage = SettingsPullStatus;
            return;
        }

        var sourceName = PullSettingsName?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(sourceName))
        {
            SettingsPullStatus = "Pull source actor name is required.";
            StatusMessage = SettingsPullStatus;
            return;
        }

        var sourceDisplay = FormatEndpointDisplay($"{sourceHost}:{sourcePort}", sourceName);
        SettingsPullStatus = $"Pulling settings from {sourceDisplay}...";
        StatusMessage = SettingsPullStatus;

        var response = await _client.ListSettingsAsync(sourceHost, sourcePort, sourceName).ConfigureAwait(false);
        if (response is null)
        {
            SettingsPullStatus = $"Pull failed: source {sourceDisplay} is unavailable.";
            StatusMessage = SettingsPullStatus;
            return;
        }

        var imported = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var skippedEndpointCount = 0;
        foreach (var entry in response.Settings)
        {
            var key = entry.Key?.Trim() ?? string.Empty;
            if (key.Length == 0)
            {
                continue;
            }

            if (!IsPullImportableSetting(key))
            {
                skippedEndpointCount++;
                continue;
            }

            imported[key] = entry.Value ?? string.Empty;
        }

        if (imported.Count == 0)
        {
            SettingsPullStatus = skippedEndpointCount > 0
                ? $"No importable settings found at {sourceDisplay}; skipped {skippedEndpointCount} endpoint setting(s)."
                : $"No settings found at {sourceDisplay}.";
            StatusMessage = SettingsPullStatus;
            return;
        }

        var appliedCount = 0;
        foreach (var entry in imported.OrderBy(row => row.Key, StringComparer.OrdinalIgnoreCase))
        {
            var result = await _client.SetSettingAsync(entry.Key, entry.Value).ConfigureAwait(false);
            if (result is null)
            {
                continue;
            }

            appliedCount++;
            ApplyAuthoritativeSettingToView(
                result.Key ?? entry.Key,
                result.Value ?? entry.Value,
                result.UpdatedMs);
        }

        var failedCount = imported.Count - appliedCount;
        SettingsPullStatus = BuildPullSettingsStatus(sourceDisplay, appliedCount, imported.Count, failedCount, skippedEndpointCount);
        StatusMessage = SettingsPullStatus;
    }

    private static void ApplyWorkerPolicyServerValue(
        string normalizedValue,
        ref string serverValue,
        ref bool dirty,
        Action<string> applyValue)
    {
        serverValue = normalizedValue;
        if (!dirty)
        {
            applyValue(normalizedValue);
        }
    }

    private void MarkWorkerPolicyApplied(string key, string value)
    {
        var normalized = NormalizeWorkerPolicyValue(value);
        if (string.Equals(key, WorkerCapabilitySettingsKeys.BenchmarkRefreshSecondsKey, StringComparison.OrdinalIgnoreCase))
        {
            _workerCapabilityRefreshSecondsServerValue = normalized;
            _workerCapabilityRefreshSecondsDirty = false;
            WorkerCapabilityRefreshSecondsText = normalized;
            return;
        }

        if (string.Equals(key, WorkerCapabilitySettingsKeys.PressureRebalanceWindowKey, StringComparison.OrdinalIgnoreCase))
        {
            _workerPressureRebalanceWindowServerValue = normalized;
            _workerPressureRebalanceWindowDirty = false;
            WorkerPressureRebalanceWindowText = normalized;
            return;
        }

        if (string.Equals(key, WorkerCapabilitySettingsKeys.PressureViolationRatioKey, StringComparison.OrdinalIgnoreCase))
        {
            _workerPressureViolationRatioServerValue = normalized;
            _workerPressureViolationRatioDirty = false;
            WorkerPressureViolationRatioText = normalized;
            return;
        }

        if (string.Equals(key, WorkerCapabilitySettingsKeys.PressureLimitTolerancePercentKey, StringComparison.OrdinalIgnoreCase))
        {
            _workerPressureTolerancePercentServerValue = normalized;
            _workerPressureTolerancePercentDirty = false;
            WorkerPressureTolerancePercentText = normalized;
        }
    }

    private string GetWorkerPolicyServerValue(string key)
        => key switch
        {
            var value when string.Equals(value, WorkerCapabilitySettingsKeys.BenchmarkRefreshSecondsKey, StringComparison.OrdinalIgnoreCase)
                => _workerCapabilityRefreshSecondsServerValue,
            var value when string.Equals(value, WorkerCapabilitySettingsKeys.PressureRebalanceWindowKey, StringComparison.OrdinalIgnoreCase)
                => _workerPressureRebalanceWindowServerValue,
            var value when string.Equals(value, WorkerCapabilitySettingsKeys.PressureViolationRatioKey, StringComparison.OrdinalIgnoreCase)
                => _workerPressureViolationRatioServerValue,
            var value when string.Equals(value, WorkerCapabilitySettingsKeys.PressureLimitTolerancePercentKey, StringComparison.OrdinalIgnoreCase)
                => _workerPressureTolerancePercentServerValue,
            _ => string.Empty
        };

    private static void Trim<T>(ObservableCollection<T> collection)
    {
        while (collection.Count > MaxRows)
        {
            collection.RemoveAt(collection.Count - 1);
        }
    }

    private static string FormatUpdated(long updatedMs)
    {
        if (updatedMs <= 0)
        {
            return string.Empty;
        }

        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(updatedMs).ToLocalTime();
        return timestamp.ToString("g");
    }

    private static string FormatUpdated(ulong updatedMs)
        => FormatUpdated((long)updatedMs);

    private static string FormatUpdated(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        if (long.TryParse(value, out var parsed))
        {
            return FormatUpdated(parsed);
        }

        return value;
    }

    private void SeedPullSettingsSourceFromConnections(bool force)
    {
        var currentHost = Connections.SettingsHost?.Trim() ?? string.Empty;
        var currentPort = Connections.SettingsPortText?.Trim() ?? string.Empty;
        var currentName = Connections.SettingsName?.Trim() ?? string.Empty;

        if (force || string.Equals(_pullSettingsHost, _lastSeededPullSettingsHost, StringComparison.Ordinal))
        {
            PullSettingsHost = currentHost;
        }

        if (force || string.Equals(_pullSettingsPortText, _lastSeededPullSettingsPortText, StringComparison.Ordinal))
        {
            PullSettingsPortText = currentPort;
        }

        if (force || string.Equals(_pullSettingsName, _lastSeededPullSettingsName, StringComparison.Ordinal))
        {
            PullSettingsName = currentName;
        }

        _lastSeededPullSettingsHost = currentHost;
        _lastSeededPullSettingsPortText = currentPort;
        _lastSeededPullSettingsName = currentName;
    }

    private static bool IsPullImportableSetting(string key)
        => !string.IsNullOrWhiteSpace(key)
           && !key.Trim().StartsWith(ServiceEndpointSettings.EndpointPrefix, StringComparison.OrdinalIgnoreCase);

    private void ApplyAuthoritativeSettingToView(string key, string value, ulong updatedMs)
        => ApplyAuthoritativeSettingToView(key, value, FormatUpdated(updatedMs));

    private void ApplyAuthoritativeSettingToView(string key, string value, string updatedDisplay)
    {
        _dispatcher.Post(() =>
        {
            var existing = Settings.FirstOrDefault(entry => string.Equals(entry.Key, key, StringComparison.OrdinalIgnoreCase));
            if (existing is null)
            {
                Settings.Add(new SettingEntryViewModel(key, value, updatedDisplay));
            }
            else
            {
                existing.MarkApplied(value, updatedDisplay);
            }

            var item = new SettingItem(key, value, updatedDisplay);
            TryApplyWorkerPolicySetting(item);
            TryApplyServiceEndpointSetting(item);
            Trim(Settings);
        });
    }

    private static string BuildPullSettingsStatus(
        string sourceDisplay,
        int appliedCount,
        int totalCount,
        int failedCount,
        int skippedEndpointCount)
    {
        var status = failedCount == 0
            ? $"Pulled {appliedCount} setting(s) from {sourceDisplay}."
            : $"Pulled {appliedCount} of {totalCount} setting(s) from {sourceDisplay}; {failedCount} failed.";

        if (skippedEndpointCount > 0)
        {
            status += $" Skipped {skippedEndpointCount} endpoint setting(s).";
        }

        return status;
    }

    private void SetDiscoveryUnavailable()
    {
        _dispatcher.Post(() =>
        {
            Nodes.Clear();
            WorkerEndpoints.Clear();
            Actors.Clear();
            WorkerEndpointSummary = "No active workers.";

            Connections.HiveMindDiscoverable = false;
            Connections.HiveMindStatus = "Offline";
            Connections.HiveMindEndpointDisplay = "Missing";

            Connections.IoDiscoverable = false;
            Connections.IoStatus = "Offline";
            Connections.IoEndpointDisplay = "Missing";

            Connections.ReproDiscoverable = false;
            Connections.ReproStatus = "Offline";
            Connections.ReproEndpointDisplay = "Missing";

            Connections.SpeciationDiscoverable = false;
            Connections.SpeciationStatus = "Offline";
            Connections.SpeciationEndpointDisplay = "Missing";

            Connections.WorkerDiscoverable = false;
            Connections.WorkerStatus = "Offline";
            Connections.WorkerEndpointDisplay = "Missing";

            Connections.ObsDiscoverable = false;
            Connections.ObsStatus = "Offline";
            Connections.ObsEndpointDisplay = "Missing";
            RefreshEndpointRows();
        });
    }

    private static IReadOnlyDictionary<string, ServiceEndpoint> BuildServiceEndpointLookup(IEnumerable<SettingItem> settings)
    {
        var lookup = new Dictionary<string, ServiceEndpoint>(StringComparer.OrdinalIgnoreCase);
        foreach (var setting in settings)
        {
            if (string.IsNullOrWhiteSpace(setting.Key)
                || !ServiceEndpointSettings.IsKnownKey(setting.Key)
                || !ServiceEndpointSettings.TryParseValue(setting.Value, out var endpoint))
            {
                continue;
            }

            lookup[setting.Key] = endpoint;
        }

        return lookup;
    }

    private static string ResolveDiscoveredActorName(
        IReadOnlyDictionary<string, ServiceEndpoint> discoveredServiceEndpoints,
        string serviceEndpointKey,
        string fallbackActorName)
    {
        if (discoveredServiceEndpoints.TryGetValue(serviceEndpointKey, out var endpoint)
            && !string.IsNullOrWhiteSpace(endpoint.ActorName))
        {
            return endpoint.ActorName.Trim();
        }

        return fallbackActorName?.Trim() ?? string.Empty;
    }

    private static string ResolveEndpointDisplay(
        IReadOnlyDictionary<string, ServiceEndpoint> discoveredServiceEndpoints,
        string serviceEndpointKey,
        string fallbackActorName)
    {
        if (discoveredServiceEndpoints.TryGetValue(serviceEndpointKey, out var endpoint))
        {
            return FormatEndpointDisplay(endpoint.HostPort, endpoint.ActorName);
        }

        return string.IsNullOrWhiteSpace(fallbackActorName)
            ? "Missing"
            : fallbackActorName.Trim();
    }

    private static string[] BuildActorCandidates(params string?[] names)
    {
        var candidates = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in names)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                continue;
            }

            candidates.Add(name.Trim());
        }

        return candidates.ToArray();
    }

    private static bool IsAnyFreshNodeMatch(
        IReadOnlyList<Nbn.Proto.Settings.NodeStatus> nodes,
        long nowMs,
        params string[] actorCandidates)
    {
        if (actorCandidates.Length == 0)
        {
            return false;
        }

        foreach (var node in nodes)
        {
            if (!node.IsAlive || !IsFresh(node.LastSeenMs, nowMs))
            {
                continue;
            }

            foreach (var actor in actorCandidates)
            {
                if (string.Equals(node.RootActorName, actor, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static string FormatEndpointDisplay(string? hostPort, string? actorName)
    {
        var hostPortToken = string.IsNullOrWhiteSpace(hostPort) ? "?" : hostPort.Trim();
        var actorToken = string.IsNullOrWhiteSpace(actorName) ? "?" : actorName.Trim();
        return $"{hostPortToken}/{actorToken}";
    }

    private bool ApplyServiceEndpointSettingsToConnections(IEnumerable<SettingItem> settings)
    {
        var hiveMindApplied = false;
        foreach (var setting in settings)
        {
            if (!TryApplyServiceEndpointSetting(setting))
            {
                continue;
            }

            if (string.Equals(setting.Key, ServiceEndpointSettings.HiveMindKey, StringComparison.OrdinalIgnoreCase))
            {
                hiveMindApplied = true;
            }
        }

        return hiveMindApplied;
    }

    private bool TryApplyServiceEndpointSetting(SettingItem item)
    {
        if (string.IsNullOrWhiteSpace(item.Key)
            || !ServiceEndpointSettings.IsKnownKey(item.Key)
            || !ServiceEndpointSettings.TryParseValue(item.Value, out var endpoint)
            || !TryParseHostPort(endpoint.HostPort, out var host, out var port))
        {
            return false;
        }

        if (string.Equals(item.Key, ServiceEndpointSettings.HiveMindKey, StringComparison.OrdinalIgnoreCase))
        {
            Connections.HiveMindHost = host;
            Connections.HiveMindPortText = port.ToString();
            Connections.HiveMindName = endpoint.ActorName;
            return true;
        }

        if (string.Equals(item.Key, ServiceEndpointSettings.IoGatewayKey, StringComparison.OrdinalIgnoreCase))
        {
            Connections.IoHost = host;
            Connections.IoPortText = port.ToString();
            Connections.IoGateway = endpoint.ActorName;
            return true;
        }

        if (string.Equals(item.Key, ServiceEndpointSettings.ReproductionManagerKey, StringComparison.OrdinalIgnoreCase))
        {
            Connections.ReproHost = host;
            Connections.ReproPortText = port.ToString();
            Connections.ReproManager = endpoint.ActorName;
            return true;
        }

        if (string.Equals(item.Key, ServiceEndpointSettings.SpeciationManagerKey, StringComparison.OrdinalIgnoreCase))
        {
            Connections.SpeciationHost = host;
            Connections.SpeciationPortText = port.ToString();
            Connections.SpeciationManager = endpoint.ActorName;
            return true;
        }

        if (string.Equals(item.Key, ServiceEndpointSettings.WorkerNodeKey, StringComparison.OrdinalIgnoreCase))
        {
            Connections.WorkerHost = host;
            Connections.WorkerPortText = port.ToString();
            Connections.WorkerRootName = endpoint.ActorName;
            return true;
        }

        if (string.Equals(item.Key, ServiceEndpointSettings.ObservabilityKey, StringComparison.OrdinalIgnoreCase))
        {
            Connections.ObsHost = host;
            Connections.ObsPortText = port.ToString();
            Connections.DebugHub = endpoint.ActorName;
            return true;
        }

        return false;
    }

    private void UpdateHiveMindEndpoint(IEnumerable<Nbn.Proto.Settings.NodeStatus> nodes, long nowMs)
    {
        var match = nodes
            .Where(node => string.Equals(node.RootActorName, Connections.HiveMindName, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(node => IsFresh(node.LastSeenMs, nowMs))
            .ThenByDescending(node => node.LastSeenMs)
            .FirstOrDefault();

        if (match is null)
        {
            return;
        }

        if (!TryParseHostPort(match.Address, out var host, out var port))
        {
            return;
        }

        Connections.HiveMindHost = host;
        Connections.HiveMindPortText = port.ToString();
    }

    private static bool TryParseHostPort(string? address, out string host, out int port)
    {
        host = string.Empty;
        port = 0;

        if (string.IsNullOrWhiteSpace(address))
        {
            return false;
        }

        var trimmed = address.Trim();
        var colonIndex = trimmed.LastIndexOf(':');
        if (colonIndex <= 0 || colonIndex >= trimmed.Length - 1)
        {
            return false;
        }

        var hostPart = trimmed[..colonIndex];
        var portPart = trimmed[(colonIndex + 1)..];

        if (hostPart.StartsWith("[", StringComparison.Ordinal) && hostPart.EndsWith("]", StringComparison.Ordinal))
        {
            hostPart = hostPart[1..^1];
        }

        if (!int.TryParse(portPart, out port))
        {
            return false;
        }

        host = hostPart;
        return !string.IsNullOrWhiteSpace(host);
    }

    private static bool TryParsePort(string value, out int port)
        => int.TryParse(value, out port) && port > 0 && port < 65536;

    private static bool TryParsePercent(string? value, out int percent)
        => int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out percent)
           && percent >= 0
           && percent <= 100;

    private static bool TryParseNonNegativeInt(string value, out int parsed)
        => int.TryParse(value, out parsed) && parsed >= 0;

    private static bool TryParsePositiveInt(string value, out int parsed)
        => int.TryParse(value, out parsed) && parsed > 0;

    private static bool TryParseNonNegativeDouble(string value, out double parsed)
        => double.TryParse(value, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out parsed)
           && double.IsFinite(parsed)
           && parsed >= 0d;

    private static string NormalizeWorkerPolicyValue(string? value, string? fallback = null)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback ?? string.Empty;
        }

        return value.Trim();
    }

    private static bool IsFresh(ulong lastSeenMs, long nowMs)
    {
        if (lastSeenMs == 0)
        {
            return false;
        }

        var delta = nowMs - (long)lastSeenMs;
        if (delta < 0)
        {
            return false;
        }

        return delta <= StaleNodeMs;
    }

    private static bool IsSpawnRecent(ulong spawnedMs, long nowMs)
    {
        if (spawnedMs == 0)
        {
            return false;
        }

        var delta = nowMs - (long)spawnedMs;
        if (delta < 0)
        {
            return false;
        }

        return delta <= SpawnVisibilityGraceMs;
    }

    private WorkerEndpointState BuildWorkerEndpointState(
        IReadOnlyList<Nbn.Proto.Settings.NodeStatus> nodes,
        IReadOnlyList<Nbn.Proto.Settings.WorkerReadinessCapability> inventory,
        IReadOnlyDictionary<Guid, HashSet<Guid>> workerBrainHints,
        long nowMs)
    {
        foreach (var worker in inventory)
        {
            if (worker.NodeId is null || !worker.NodeId.TryToGuid(out var nodeId))
            {
                continue;
            }

            if (!IsWorkerHostCandidate(worker.LogicalName, worker.RootActorName))
            {
                continue;
            }

            UpdateWorkerEndpointSnapshot(
                nodeId,
                worker.LogicalName,
                worker.Address,
                worker.RootActorName,
                (long)worker.LastSeenMs,
                worker.IsAlive,
                hasCapabilitySnapshot: true,
                isReady: worker.IsReady,
                hasCapabilities: worker.HasCapabilities,
                placementStatus: DescribeWorkerPlacementStatus(worker, out var placementDetail),
                placementDetail: placementDetail);
        }

        foreach (var node in nodes)
        {
            if (node.NodeId is null || !node.NodeId.TryToGuid(out var nodeId))
            {
                continue;
            }

            if (!IsWorkerHostCandidate(node.LogicalName, node.RootActorName))
            {
                continue;
            }

            UpdateWorkerEndpointSnapshot(
                nodeId,
                node.LogicalName,
                node.Address,
                node.RootActorName,
                (long)node.LastSeenMs,
                node.IsAlive);
        }

        var rows = new List<(int Rank, long LastSeenMs, WorkerEndpointItem Row)>();
        var staleNodeIds = new List<Guid>();
        var activeCount = 0;
        var limitedCount = 0;
        var degradedCount = 0;
        var failedCount = 0;

        foreach (var entry in _workerEndpointCache.Values)
        {
            var (status, remove) = ClassifyWorkerEndpointStatus(entry, nowMs);
            if (remove)
            {
                staleNodeIds.Add(entry.NodeId);
                continue;
            }

            switch (status)
            {
                case "active":
                    activeCount++;
                    break;
                case "limited":
                    limitedCount++;
                    break;
                case "degraded":
                    degradedCount++;
                    break;
                default:
                    failedCount++;
                    break;
            }

            rows.Add((WorkerStatusRank(status), entry.LastSeenMs, new WorkerEndpointItem(
                entry.NodeId,
                entry.LogicalName,
                entry.Address,
                entry.RootActorName,
                FormatWorkerBrainHints(workerBrainHints, entry.NodeId),
                FormatUpdated(entry.LastSeenMs),
                status,
                entry.PlacementDetail)));
        }

        foreach (var staleNodeId in staleNodeIds)
        {
            _workerEndpointCache.Remove(staleNodeId);
        }

        var orderedRows = rows
            .OrderBy(entry => entry.Rank)
            .ThenByDescending(entry => entry.LastSeenMs)
            .ThenBy(entry => entry.Row.LogicalName, StringComparer.OrdinalIgnoreCase)
            .Select(entry => entry.Row)
            .ToArray();

        var summary = BuildWorkerEndpointSummary(activeCount, limitedCount, degradedCount, failedCount);
        return new WorkerEndpointState(orderedRows, activeCount, limitedCount, degradedCount, failedCount, summary);
    }

    private void UpdateWorkerEndpointSnapshot(
        Guid nodeId,
        string? logicalName,
        string? address,
        string? rootActorName,
        long lastSeenMs,
        bool isAlive,
        bool? hasCapabilitySnapshot = null,
        bool? isReady = null,
        bool? hasCapabilities = null,
        string? placementStatus = null,
        string? placementDetail = null)
    {
        if (!_workerEndpointCache.TryGetValue(nodeId, out var snapshot))
        {
            snapshot = new WorkerEndpointSnapshot
            {
                NodeId = nodeId
            };
            _workerEndpointCache[nodeId] = snapshot;
        }

        if (!string.IsNullOrWhiteSpace(logicalName))
        {
            snapshot.LogicalName = logicalName.Trim();
        }

        if (!string.IsNullOrWhiteSpace(address))
        {
            snapshot.Address = address.Trim();
        }

        if (!string.IsNullOrWhiteSpace(rootActorName))
        {
            snapshot.RootActorName = rootActorName.Trim();
        }

        var previousLastSeen = snapshot.LastSeenMs;
        snapshot.LastSeenMs = Math.Max(snapshot.LastSeenMs, lastSeenMs);
        if (lastSeenMs >= previousLastSeen || !isAlive)
        {
            snapshot.IsAlive = isAlive;
        }

        if (hasCapabilitySnapshot.HasValue)
        {
            snapshot.HasCapabilitySnapshot = hasCapabilitySnapshot.Value;
        }

        if (isReady.HasValue)
        {
            snapshot.IsReady = isReady.Value;
        }

        if (hasCapabilities.HasValue)
        {
            snapshot.HasCapabilities = hasCapabilities.Value;
        }

        if (!string.IsNullOrWhiteSpace(placementStatus))
        {
            snapshot.PlacementStatus = placementStatus.Trim();
        }

        if (!string.IsNullOrWhiteSpace(placementDetail))
        {
            snapshot.PlacementDetail = placementDetail.Trim();
        }
    }

    private static (string Status, bool Remove) ClassifyWorkerEndpointStatus(WorkerEndpointSnapshot snapshot, long nowMs)
    {
        if (snapshot.LastSeenMs <= 0)
        {
            return ("failed", false);
        }

        var ageMs = nowMs - snapshot.LastSeenMs;
        if (ageMs < 0)
        {
            ageMs = 0;
        }

        if (ageMs > WorkerRemoveAfterMs)
        {
            return ("failed", true);
        }

        if (!snapshot.IsAlive || ageMs > WorkerFailedAfterMs)
        {
            return ("failed", false);
        }

        if (ageMs > StaleNodeMs)
        {
            return ("degraded", false);
        }

        if (snapshot.HasCapabilitySnapshot)
        {
            if (!snapshot.HasCapabilities || !snapshot.IsReady)
            {
                return ("degraded", false);
            }

            if (string.Equals(snapshot.PlacementStatus, "limited", StringComparison.OrdinalIgnoreCase))
            {
                return ("limited", false);
            }
        }

        return ("active", false);
    }

    private static int WorkerStatusRank(string status)
    {
        return status switch
        {
            "active" => 0,
            "limited" => 1,
            "degraded" => 2,
            "failed" => 3,
            _ => 4
        };
    }

    private static string BuildWorkerEndpointSummary(int activeCount, int limitedCount, int degradedCount, int failedCount)
    {
        var parts = new List<string>();
        if (activeCount > 0)
        {
            parts.Add(FormatCount(activeCount, "active"));
        }

        if (limitedCount > 0)
        {
            parts.Add(FormatCount(limitedCount, "limited"));
        }

        if (degradedCount > 0)
        {
            parts.Add(FormatCount(degradedCount, "degraded"));
        }

        if (failedCount > 0)
        {
            parts.Add(FormatCount(failedCount, "failed"));
        }

        return parts.Count == 0
            ? "No active workers."
            : string.Join(", ", parts);
    }

    private static string DescribeWorkerPlacementStatus(Nbn.Proto.Settings.WorkerReadinessCapability worker, out string detail)
    {
        detail = string.Empty;
        var caps = worker.Capabilities ?? new Nbn.Proto.Settings.NodeCapabilities();

        if (!worker.HasCapabilities)
        {
            detail = "Capability warm-up in progress.";
            return "degraded";
        }

        if (string.IsNullOrWhiteSpace(worker.RootActorName))
        {
            detail = "Missing worker root actor.";
            return "limited";
        }

        if (WorkerCapabilityMath.IsCpuOverLimit(caps.ProcessCpuLoadPercent, caps.CpuLimitPercent, 0f))
        {
            detail = $"CPU load {caps.ProcessCpuLoadPercent:0.#}% > {caps.CpuLimitPercent}% limit.";
            return "limited";
        }

        if (WorkerCapabilityMath.IsRamOverLimit(
                caps.ProcessRamUsedBytes,
                caps.RamTotalBytes,
                caps.RamLimitPercent,
                0f))
        {
            detail = $"RAM use exceeds {caps.RamLimitPercent}% limit.";
            return "limited";
        }

        var storageUsedPercent = caps.StorageTotalBytes == 0
            ? 0f
            : WorkerCapabilityMath.ComputeUsedPercent(caps.StorageFreeBytes, caps.StorageTotalBytes);
        if (WorkerCapabilityMath.IsStorageOverLimit(
                caps.StorageFreeBytes,
                caps.StorageTotalBytes,
                caps.StorageLimitPercent,
                0f))
        {
            detail = $"Storage used {storageUsedPercent:0.#}% > {caps.StorageLimitPercent}% limit.";
            return "limited";
        }

        if (caps.RamFreeBytes == 0 || caps.RamTotalBytes == 0)
        {
            detail = "Missing RAM capacity telemetry.";
            return "limited";
        }

        if (caps.StorageFreeBytes == 0 || caps.StorageTotalBytes == 0)
        {
            detail = "Missing storage capacity telemetry.";
            return "limited";
        }

        if (caps.CpuCores == 0)
        {
            detail = "Missing CPU core telemetry.";
            return "limited";
        }

        detail = "Placement ready.";
        return "active";
    }

    private static string FormatCount(int count, string label)
    {
        var suffix = count == 1 ? "worker" : "workers";
        return $"{count} {label} {suffix}";
    }

    private static string FormatWorkerBrainHints(
        IReadOnlyDictionary<Guid, HashSet<Guid>> workerBrainHints,
        Guid nodeId)
    {
        if (!workerBrainHints.TryGetValue(nodeId, out var brainIds) || brainIds.Count == 0)
        {
            return "none";
        }

        var abbreviated = brainIds
            .Select(AbbreviateBrainId)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();
        return abbreviated.Length <= MaxWorkerBrainHints
            ? string.Join(", ", abbreviated)
            : $"{string.Join(", ", abbreviated.Take(MaxWorkerBrainHints))}, ...";
    }

    private static string AbbreviateBrainId(Guid brainId)
    {
        var compact = brainId.ToString("N");
        return compact.Length <= 4 ? compact : compact[^4..];
    }

    private static string AbbreviateHostedActorBrainId(Guid brainId)
    {
        var compact = brainId.ToString("N");
        return compact.Length <= 8 ? compact : compact[^8..];
    }

    private void UpdateConnectionStatusesFromNodes(
        IReadOnlyList<Nbn.Proto.Settings.NodeStatus> nodes,
        long nowMs,
        WorkerEndpointState workerEndpointState,
        IReadOnlyDictionary<string, ServiceEndpoint> discoveredServiceEndpoints)
    {
        var hiveActorName = ResolveDiscoveredActorName(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.HiveMindKey,
            Connections.HiveMindName);
        var ioActorName = ResolveDiscoveredActorName(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.IoGatewayKey,
            Connections.IoGateway);
        var reproActorName = ResolveDiscoveredActorName(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.ReproductionManagerKey,
            Connections.ReproManager);
        var speciationActorName = ResolveDiscoveredActorName(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.SpeciationManagerKey,
            Connections.SpeciationManager);
        var obsActorName = ResolveDiscoveredActorName(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.ObservabilityKey,
            Connections.DebugHub);
        var obsCandidates = BuildActorCandidates(obsActorName, Connections.DebugHub, Connections.VizHub);

        var hiveAlive = IsAnyFreshNodeMatch(nodes, nowMs, hiveActorName);
        var ioAlive = IsAnyFreshNodeMatch(nodes, nowMs, ioActorName);
        var reproAlive = IsAnyFreshNodeMatch(nodes, nowMs, reproActorName);
        var speciationAlive = IsAnyFreshNodeMatch(nodes, nowMs, speciationActorName);
        var obsAlive = IsAnyFreshNodeMatch(nodes, nowMs, obsCandidates);

        var hiveEndpointDisplay = ResolveEndpointDisplay(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.HiveMindKey,
            hiveActorName);
        var ioEndpointDisplay = ResolveEndpointDisplay(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.IoGatewayKey,
            ioActorName);
        var reproEndpointDisplay = ResolveEndpointDisplay(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.ReproductionManagerKey,
            reproActorName);
        var speciationEndpointDisplay = ResolveEndpointDisplay(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.SpeciationManagerKey,
            speciationActorName);
        var obsEndpointDisplay = ResolveEndpointDisplay(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.ObservabilityKey,
            obsActorName);
        var workerEndpointDisplay = workerEndpointState.Rows.Count > 0
            ? workerEndpointState.SummaryText
            : "Missing";

        _dispatcher.Post(() =>
        {
            Connections.HiveMindDiscoverable = hiveAlive;
            Connections.HiveMindStatus = hiveAlive ? "Online" : "Offline";
            Connections.HiveMindEndpointDisplay = hiveEndpointDisplay;

            Connections.IoDiscoverable = ioAlive;
            Connections.IoStatus = ioAlive ? "Online" : "Offline";
            Connections.IoEndpointDisplay = ioEndpointDisplay;

            Connections.ReproDiscoverable = reproAlive;
            Connections.ReproStatus = reproAlive ? "Online" : "Offline";
            Connections.ReproEndpointDisplay = reproEndpointDisplay;

            Connections.SpeciationDiscoverable = speciationAlive;
            Connections.SpeciationStatus = speciationAlive ? "Online" : "Offline";
            Connections.SpeciationEndpointDisplay = speciationEndpointDisplay;

            Connections.WorkerDiscoverable = workerEndpointState.ActiveCount > 0 || workerEndpointState.LimitedCount > 0;
            Connections.WorkerStatus = workerEndpointState.Rows.Count > 0
                ? workerEndpointState.SummaryText
                : "Offline";
            Connections.WorkerEndpointDisplay = workerEndpointDisplay;

            Connections.ObsDiscoverable = obsAlive;
            Connections.ObsStatus = obsAlive ? "Online" : "Offline";
            Connections.ObsEndpointDisplay = obsEndpointDisplay;
            RefreshEndpointRows();
        });
    }

    private void RefreshEndpointRows()
    {
        Endpoints.Clear();
        Endpoints.Add(CreateEndpointStatusItem("IO Gateway", Connections.IoEndpointDisplay, Connections.IsIoServiceReady()));
        Endpoints.Add(CreateEndpointStatusItem("Observability", Connections.ObsEndpointDisplay, Connections.IsObsServiceReady()));
        Endpoints.Add(CreateEndpointStatusItem("Reproduction", Connections.ReproEndpointDisplay, Connections.IsReproServiceReady()));
        Endpoints.Add(CreateEndpointStatusItem("Speciation", Connections.SpeciationEndpointDisplay, Connections.IsSpeciationServiceReady()));
        Endpoints.Add(CreateEndpointStatusItem("SettingsMonitor", BuildSettingsEndpointDisplay(), Connections.IsSettingsServiceReady()));
        Endpoints.Add(CreateEndpointStatusItem("HiveMind", Connections.HiveMindEndpointDisplay, Connections.IsHiveMindServiceReady()));
    }

    private bool HasSpawnServiceReadiness()
    {
        return Connections.HasSpawnServiceReadiness();
    }

    private string ResolveProfileCurrentSystemOutputDirectory()
    {
        var root = RepoLocator.ResolvePathFromRepo(".artifacts-temp", "perf-probe");
        if (string.IsNullOrWhiteSpace(root))
        {
            root = Path.Combine(Environment.CurrentDirectory, ".artifacts-temp", "perf-probe");
        }

        return Path.Combine(root, DateTimeOffset.UtcNow.ToString("yyyyMMdd-HHmmss"));
    }

    private int ResolveProfileClientPort()
    {
        return TryParsePort(Connections.LocalPortText, out var localPort)
            ? Math.Max(1024, localPort + 20)
            : 12110;
    }

    private static EndpointStatusItem CreateEndpointStatusItem(string serviceName, string endpointDisplay, bool discoverable)
    {
        var normalizedEndpointDisplay = string.IsNullOrWhiteSpace(endpointDisplay) ? "Missing" : endpointDisplay.Trim();
        return new EndpointStatusItem(
            serviceName,
            normalizedEndpointDisplay,
            discoverable ? "online" : "offline");
    }

    private void OnConnectionsPropertyChanged(object? sender, PropertyChangedEventArgs args)
    {
        if (string.Equals(args.PropertyName, nameof(ConnectionViewModel.SettingsHost), StringComparison.Ordinal)
            || string.Equals(args.PropertyName, nameof(ConnectionViewModel.SettingsPortText), StringComparison.Ordinal)
            || string.Equals(args.PropertyName, nameof(ConnectionViewModel.SettingsName), StringComparison.Ordinal))
        {
            SeedPullSettingsSourceFromConnections(force: false);
        }

        if (string.IsNullOrWhiteSpace(args.PropertyName)
            || !EndpointRefreshTriggerProperties.Contains(args.PropertyName))
        {
            return;
        }

        _dispatcher.Post(RefreshEndpointRows);
    }

    private string BuildSettingsEndpointDisplay()
    {
        var host = Connections.SettingsHost?.Trim() ?? string.Empty;
        var port = Connections.SettingsPortText?.Trim() ?? string.Empty;
        var actorName = Connections.SettingsName?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(host) && string.IsNullOrWhiteSpace(port) && string.IsNullOrWhiteSpace(actorName))
        {
            return "Missing";
        }

        var address = host;
        if (!string.IsNullOrWhiteSpace(port))
        {
            address = string.IsNullOrWhiteSpace(address) ? port : $"{address}:{port}";
        }

        if (!string.IsNullOrWhiteSpace(actorName))
        {
            address = string.IsNullOrWhiteSpace(address) ? actorName : $"{address}/{actorName}";
        }

        return string.IsNullOrWhiteSpace(address) ? "Missing" : address;
    }

    private void RecordBrainTerminations(IReadOnlyList<BrainListItem> current)
    {
        var seen = new HashSet<Guid>();
        foreach (var brain in current)
        {
            seen.Add(brain.BrainId);
            if (_lastBrains.TryGetValue(brain.BrainId, out var previous))
            {
                if (previous.ControllerAlive && !brain.ControllerAlive)
                {
                    AddTermination(new BrainTerminatedItem(
                        DateTimeOffset.UtcNow,
                        brain.BrainId.ToString("D"),
                        "Controller offline",
                        0,
                        0));
                }
                else if (!string.Equals(previous.State, "Dead", StringComparison.OrdinalIgnoreCase)
                         && string.Equals(brain.State, "Dead", StringComparison.OrdinalIgnoreCase))
                {
                    AddTermination(new BrainTerminatedItem(
                        DateTimeOffset.UtcNow,
                        brain.BrainId.ToString("D"),
                        "State dead",
                        0,
                        0));
                }
            }

            _lastBrains[brain.BrainId] = brain;
        }

        var missing = _lastBrains.Keys.Where(id => !seen.Contains(id)).ToList();
        foreach (var brainId in missing)
        {
            if (!_lastBrains.TryGetValue(brainId, out var previous))
            {
                continue;
            }

            AddTermination(new BrainTerminatedItem(
                DateTimeOffset.UtcNow,
                brainId.ToString("D"),
                "Missing from registry",
                0,
                0));
            _lastBrains.Remove(brainId);
        }
    }

    private async Task TriggerReconnectAsync()
    {
        if (_connectAll is null)
        {
            return;
        }

        await Task.Delay(500).ConfigureAwait(false);
        await _connectAll().ConfigureAwait(false);
    }

    private static string BuildSampleRunRoot()
    {
        var baseDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "Nbn.Workbench",
            "sample-brain");
        Directory.CreateDirectory(baseDir);
        return baseDir;
    }

    private static string BuildDefaultSettingsDbPath()
    {
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(localAppData))
        {
            return "settingsmonitor.db";
        }

        var baseDir = Path.Combine(localAppData, "Nbn.Workbench");
        Directory.CreateDirectory(baseDir);
        return Path.Combine(baseDir, "settingsmonitor.db");
    }

    private string ResolveSettingsDbPath()
    {
        var configured = Connections.SettingsDbPath?.Trim();
        return string.IsNullOrWhiteSpace(configured) ? BuildDefaultSettingsDbPath() : configured;
    }

    private static bool PathsEqual(string left, string right)
    {
        try
        {
            var leftFull = Path.GetFullPath(left);
            var rightFull = Path.GetFullPath(right);
            var comparison = OperatingSystem.IsWindows()
                ? StringComparison.OrdinalIgnoreCase
                : StringComparison.Ordinal;
            return string.Equals(leftFull, rightFull, comparison);
        }
        catch
        {
            return false;
        }
    }

    private async Task<HostedActorRowsResult> BuildActorRowsAsync(
        IReadOnlyList<Nbn.Proto.Settings.BrainControllerStatus> controllers,
        IReadOnlyList<Nbn.Proto.Settings.NodeStatus> nodes,
        IReadOnlyList<Nbn.Proto.Settings.BrainStatus> brains,
        long nowMs)
    {
        var rows = new List<(bool IsOnlineWorkerHost, bool IsOnline, long LastSeenMs, NodeStatusItem Row)>();
        var workerBrainHints = new Dictionary<Guid, HashSet<Guid>>();
        var dedupe = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var nodeById = nodes
            .Where(entry => entry.NodeId is not null && entry.NodeId.TryToGuid(out _))
            .ToDictionary(entry => entry.NodeId!.ToGuid(), entry => entry);

        void AddActorRow(
            Guid brainId,
            string actorKind,
            string actorPid,
            string hostLabel,
            string address,
            long lastSeenMs,
            bool isOnline,
            bool hostIsWorker = false,
            uint regionId = 0,
            uint shardIndex = 0)
        {
            var dedupeKey = BuildHostedActorKey(brainId, actorPid, actorKind, regionId, shardIndex);
            if (!dedupe.Add(dedupeKey))
            {
                return;
            }

            var brainToken = AbbreviateHostedActorBrainId(brainId);
            var kindToken = actorKind;
            if (string.Equals(actorKind, "RegionShard", StringComparison.Ordinal))
            {
                kindToken = $"{actorKind} r{regionId} s{shardIndex}";
            }

            var seen = lastSeenMs > 0 ? FormatUpdated(lastSeenMs) : string.Empty;
            var logicalName = $"{hostLabel} - brain {brainToken} {kindToken}";
            var rootActor = string.IsNullOrWhiteSpace(actorPid) ? actorKind : actorPid;
            rows.Add((isOnline && hostIsWorker, isOnline, lastSeenMs, new NodeStatusItem(
                logicalName,
                address,
                rootActor,
                seen,
                isOnline ? "online" : "offline")));
        }

        void AddWorkerBrainHint(Guid nodeId, Guid brainId)
        {
            if (nodeId == Guid.Empty || brainId == Guid.Empty)
            {
                return;
            }

            if (!workerBrainHints.TryGetValue(nodeId, out var brainSet))
            {
                brainSet = new HashSet<Guid>();
                workerBrainHints[nodeId] = brainSet;
            }

            brainSet.Add(brainId);
        }

        foreach (var controller in controllers
                     .Where(entry => entry.BrainId is not null && entry.BrainId.TryToGuid(out _))
                     .OrderByDescending(entry => entry.LastSeenMs)
                     .ThenBy(entry => entry.ActorName ?? string.Empty, StringComparer.OrdinalIgnoreCase))
        {
            var brainId = controller.BrainId!.ToGuid();
            var hostLabel = "controller node";
            var address = string.Empty;
            var hostSeenMs = (long)controller.LastSeenMs;
            var hostIsWorker = false;
            var workerHostNodeId = Guid.Empty;
            if (controller.NodeId is not null
                && controller.NodeId.TryToGuid(out var nodeId)
                && nodeById.TryGetValue(nodeId, out var node))
            {
                hostLabel = string.IsNullOrWhiteSpace(node.LogicalName) ? "controller node" : node.LogicalName!;
                address = node.Address ?? string.Empty;
                hostSeenMs = (long)node.LastSeenMs;
                hostIsWorker = IsWorkerHostCandidate(node);
                if (hostIsWorker)
                {
                    workerHostNodeId = nodeId;
                }
            }

            var actorPid = controller.ActorName?.Trim() ?? string.Empty;
            var isOnline = controller.IsAlive && IsFresh(controller.LastSeenMs, nowMs);
            if (hostIsWorker && isOnline && workerHostNodeId != Guid.Empty)
            {
                AddWorkerBrainHint(workerHostNodeId, brainId);
            }

            AddActorRow(
                brainId,
                actorKind: "Controller",
                actorPid: actorPid,
                hostLabel: hostLabel,
                address: address,
                lastSeenMs: hostSeenMs,
                isOnline: isOnline,
                hostIsWorker: hostIsWorker);
        }

        var activeBrainIds = brains
            .Where(entry => entry.BrainId is not null && entry.BrainId.TryToGuid(out _))
            .Select(entry => entry.BrainId!.ToGuid())
            .Where(id => id != Guid.Empty)
            .Distinct()
            .ToArray();
        if (activeBrainIds.Length == 0)
        {
            return new HostedActorRowsResult(
                rows
                .OrderByDescending(entry => entry.IsOnlineWorkerHost)
                .ThenByDescending(entry => entry.IsOnline)
                .ThenByDescending(entry => entry.LastSeenMs)
                .ThenBy(entry => entry.Row.LogicalName, StringComparer.OrdinalIgnoreCase)
                .Select(entry => entry.Row)
                .ToArray(),
                workerBrainHints);
        }

        var lifecycleTasks = activeBrainIds.Select(async brainId =>
        {
            var lifecycle = await _client.GetPlacementLifecycleAsync(brainId).ConfigureAwait(false);
            return (BrainId: brainId, Lifecycle: lifecycle);
        });
        var lifecycles = await Task.WhenAll(lifecycleTasks).ConfigureAwait(false);
        var reconcileNodes = nodes
            .Where(entry =>
                entry.IsAlive
                && IsFresh(entry.LastSeenMs, nowMs)
                && !string.IsNullOrWhiteSpace(entry.Address)
                && !string.IsNullOrWhiteSpace(entry.RootActorName))
            .ToArray();

        var reconcileTasks = new List<Task<(Guid BrainId, Nbn.Proto.Settings.NodeStatus Node, PlacementReconcileReport? Report)>>();
        foreach (var lifecycleEntry in lifecycles)
        {
            var placementEpoch = lifecycleEntry.Lifecycle?.PlacementEpoch ?? 0;
            if (placementEpoch == 0)
            {
                continue;
            }

            foreach (var node in reconcileNodes)
            {
                reconcileTasks.Add(QueryPlacementReconcileAsync(
                    lifecycleEntry.BrainId,
                    node,
                    placementEpoch));
            }
        }

        var reconcileResults = reconcileTasks.Count == 0
            ? Array.Empty<(Guid BrainId, Nbn.Proto.Settings.NodeStatus Node, PlacementReconcileReport? Report)>()
            : await Task.WhenAll(reconcileTasks).ConfigureAwait(false);
        foreach (var reconcileResult in reconcileResults)
        {
            if (reconcileResult.Report?.Assignments is null
                || reconcileResult.Report.Assignments.Count == 0)
            {
                continue;
            }

            var report = reconcileResult.Report;
            var reportBrainId = report.BrainId is not null && report.BrainId.TryToGuid(out var parsedBrainId)
                ? parsedBrainId
                : reconcileResult.BrainId;

            foreach (var assignment in report.Assignments)
            {
                var actorKind = ToAssignmentTargetLabel(assignment.Target);
                var actorPid = assignment.ActorPid?.Trim() ?? string.Empty;
                var hostLabel = string.IsNullOrWhiteSpace(reconcileResult.Node.LogicalName)
                    ? "host node"
                    : reconcileResult.Node.LogicalName!;
                var address = reconcileResult.Node.Address ?? string.Empty;
                var hostSeenMs = (long)reconcileResult.Node.LastSeenMs;
                var isOnline = reconcileResult.Node.IsAlive && IsFresh(reconcileResult.Node.LastSeenMs, nowMs);
                var hostIsWorker = IsWorkerHostCandidate(reconcileResult.Node);
                var hostNodeId = reconcileResult.Node.NodeId is not null
                                 && reconcileResult.Node.NodeId.TryToGuid(out var reconcileNodeId)
                    ? reconcileNodeId
                    : Guid.Empty;

                if (assignment.WorkerNodeId is not null
                    && assignment.WorkerNodeId.TryToGuid(out var workerNodeId)
                    && nodeById.TryGetValue(workerNodeId, out var workerNode))
                {
                    hostLabel = string.IsNullOrWhiteSpace(workerNode.LogicalName) ? "host node" : workerNode.LogicalName!;
                    address = workerNode.Address ?? address;
                    hostSeenMs = (long)workerNode.LastSeenMs;
                    isOnline = workerNode.IsAlive && IsFresh(workerNode.LastSeenMs, nowMs);
                    hostIsWorker = IsWorkerHostCandidate(workerNode);
                    hostNodeId = workerNodeId;
                }

                if (hostIsWorker && isOnline && hostNodeId != Guid.Empty)
                {
                    AddWorkerBrainHint(hostNodeId, reportBrainId);
                }

                AddActorRow(
                    reportBrainId,
                    actorKind: actorKind,
                    actorPid: actorPid,
                    hostLabel: hostLabel,
                    address: address,
                    lastSeenMs: hostSeenMs,
                    isOnline: isOnline,
                    hostIsWorker: hostIsWorker,
                    regionId: assignment.RegionId,
                    shardIndex: assignment.ShardIndex);
            }
        }

        return new HostedActorRowsResult(
            rows
            .OrderByDescending(entry => entry.IsOnlineWorkerHost)
            .ThenByDescending(entry => entry.IsOnline)
            .ThenByDescending(entry => entry.LastSeenMs)
            .ThenBy(entry => entry.Row.LogicalName, StringComparer.OrdinalIgnoreCase)
            .Select(entry => entry.Row)
            .ToArray(),
            workerBrainHints);

        async Task<(Guid BrainId, Nbn.Proto.Settings.NodeStatus Node, PlacementReconcileReport? Report)> QueryPlacementReconcileAsync(
            Guid brainId,
            Nbn.Proto.Settings.NodeStatus node,
            ulong placementEpoch)
        {
            var report = await _client.RequestPlacementReconcileAsync(
                    node.Address ?? string.Empty,
                    node.RootActorName ?? string.Empty,
                    brainId,
                    placementEpoch)
                .ConfigureAwait(false);
            return (brainId, node, report);
        }
    }

    private static string BuildHostedActorKey(
        Guid brainId,
        string actorPid,
        string actorKind,
        uint regionId,
        uint shardIndex)
    {
        if (!string.IsNullOrWhiteSpace(actorPid))
        {
            return actorPid.Trim();
        }

        return $"{brainId:N}|{actorKind}|{regionId}|{shardIndex}";
    }

    private bool IsWorkerHostCandidate(Nbn.Proto.Settings.NodeStatus node)
    {
        return IsWorkerHostCandidate(node.LogicalName, node.RootActorName);
    }

    private bool IsWorkerHostCandidate(string? logicalName, string? rootActorName)
    {
        if (!string.IsNullOrWhiteSpace(Connections.WorkerLogicalName)
            && !string.IsNullOrWhiteSpace(logicalName)
            && string.Equals(logicalName.Trim(), Connections.WorkerLogicalName.Trim(), StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(Connections.WorkerRootName)
            && !string.IsNullOrWhiteSpace(rootActorName)
            && string.Equals(rootActorName.Trim(), Connections.WorkerRootName.Trim(), StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(logicalName)
            && logicalName.Trim().StartsWith("nbn.worker", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (string.IsNullOrWhiteSpace(rootActorName))
        {
            return false;
        }

        var root = rootActorName.Trim();
        return root.StartsWith("worker-node", StringComparison.OrdinalIgnoreCase)
               || root.Equals("regionhost", StringComparison.OrdinalIgnoreCase)
               || root.Equals("region-host", StringComparison.OrdinalIgnoreCase)
               || root.StartsWith("regionhost-", StringComparison.OrdinalIgnoreCase)
               || root.StartsWith("region-host-", StringComparison.OrdinalIgnoreCase);
    }

    private sealed record HostedActorRowsResult(
        IReadOnlyList<NodeStatusItem> Rows,
        IReadOnlyDictionary<Guid, HashSet<Guid>> WorkerBrainHints);

    private sealed record WorkerEndpointState(
        IReadOnlyList<WorkerEndpointItem> Rows,
        int ActiveCount,
        int LimitedCount,
        int DegradedCount,
        int FailedCount,
        string SummaryText);

    private sealed class WorkerEndpointSnapshot
    {
        public Guid NodeId { get; init; }
        public string LogicalName { get; set; } = string.Empty;
        public string Address { get; set; } = string.Empty;
        public string RootActorName { get; set; } = string.Empty;
        public long LastSeenMs { get; set; }
        public bool IsAlive { get; set; }
        public bool HasCapabilitySnapshot { get; set; }
        public bool IsReady { get; set; }
        public bool HasCapabilities { get; set; }
        public string PlacementStatus { get; set; } = string.Empty;
        public string PlacementDetail { get; set; } = string.Empty;
    }

    private void ApplyObservabilityEnvironment(ProcessStartInfo startInfo)
    {
        if (startInfo.UseShellExecute)
        {
            return;
        }

        var host = Connections.ObsHost?.Trim();
        if (!string.IsNullOrWhiteSpace(host)
            && TryParsePort(Connections.ObsPortText, out var obsPort))
        {
            startInfo.EnvironmentVariables["NBN_OBS_HOST"] = host;
            startInfo.EnvironmentVariables["NBN_OBS_PORT"] = obsPort.ToString();
            startInfo.EnvironmentVariables["NBN_OBS_ADDRESS"] = $"{host}:{obsPort}";
        }

        var debugHub = Connections.DebugHub?.Trim();
        if (!string.IsNullOrWhiteSpace(debugHub))
        {
            startInfo.EnvironmentVariables["NBN_OBS_DEBUG_HUB"] = debugHub;
        }

        var vizHub = Connections.VizHub?.Trim();
        if (!string.IsNullOrWhiteSpace(vizHub))
        {
            startInfo.EnvironmentVariables["NBN_OBS_VIZ_HUB"] = vizHub;
        }
    }

    private static void ApplyRuntimeDiagnosticsEnvironment(ProcessStartInfo startInfo)
    {
        if (!EnableRuntimeDiagnostics || startInfo.UseShellExecute)
        {
            return;
        }

        SetEnvIfMissing(startInfo, "NBN_RUNTIME_METADATA_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_METADATA_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_HIVEMIND_METADATA_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_IO_METADATA_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_REGIONSHARD_ACTIVITY_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_REGIONSHARD_INIT_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_REGIONSHARD_ACTIVITY_DIAGNOSTICS_PERIOD", ActivityDiagnosticsPeriod);
        SetEnvIfMissing(startInfo, "NBN_VIZ_DIAGNOSTICS_ENABLED", "1");
    }

    private static void SetEnvIfMissing(ProcessStartInfo startInfo, string key, string value)
    {
        if (!startInfo.EnvironmentVariables.ContainsKey(key))
        {
            startInfo.EnvironmentVariables[key] = value;
        }
    }

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return !string.IsNullOrWhiteSpace(value)
               && (string.Equals(value, "1", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "yes", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "on", StringComparison.OrdinalIgnoreCase));
    }

    private static string ResolveEnvOrDefault(string key, string fallback)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return string.IsNullOrWhiteSpace(value) ? fallback : value.Trim();
    }

    private static string ToAssignmentTargetLabel(PlacementAssignmentTarget target)
    {
        return target switch
        {
            PlacementAssignmentTarget.PlacementTargetBrainRoot => "BrainHost",
            PlacementAssignmentTarget.PlacementTargetSignalRouter => "SignalRouter",
            PlacementAssignmentTarget.PlacementTargetInputCoordinator => "InputCoordinator",
            PlacementAssignmentTarget.PlacementTargetOutputCoordinator => "OutputCoordinator",
            PlacementAssignmentTarget.PlacementTargetRegionShard => "RegionShard",
            _ => "HostedActor"
        };
    }

    private static void ResetDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
        catch
        {
        }

        Directory.CreateDirectory(path);
    }

    private async Task<SampleArtifact?> CreateSampleArtifactsAsync(string artifactRoot)
    {
        try
        {
            Directory.CreateDirectory(artifactRoot);
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var nbnBytes = DemoNbnBuilder.BuildSampleNbn();
            var manifest = await store.StoreAsync(new MemoryStream(nbnBytes), "application/x-nbn").ConfigureAwait(false);
            return new SampleArtifact(manifest.ArtifactId.ToHex(), manifest.ByteLength, artifactRoot);
        }
        catch (Exception ex)
        {
            SampleBrainStatus = $"Sample artifact failed: {ex.Message}";
            return null;
        }
    }

    private sealed record SampleArtifact(string Sha256, long Size, string ArtifactRoot);
}
