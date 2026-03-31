using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

/// <summary>
/// Coordinates Workbench runtime orchestration, settings, and endpoint status presentation.
/// </summary>
public sealed partial class OrchestratorPanelViewModel : ViewModelBase
{
    private sealed record WorkerBrainBackendProbeCacheEntry(
        WorkerBrainBackendHint? Hint,
        int ExpectedProbeCount,
        long UpdatedMs);

    private const int MaxWorkerBrainHints = 2;
    private const int MaxRows = 200;
    private const int InitialHostedActorBrainRenderLimit = 64;
    private const int HostedActorBrainRenderStep = 64;
    private const long StaleNodeMs = 15000;
    private const long WorkerFailedAfterMs = 45000;
    private const long WorkerRemoveAfterMs = 120000;
    private const long SpawnVisibilityGraceMs = 30000;
    private const long WorkerBrainBackendProbeCacheMs = 15000;
    private const float LocalDefaultTickHz = 8f;
    private const float LocalDefaultMinTickHz = 2f;
    private const int LocalDefaultWorkerStorageLimitPercent = 95;
    private static readonly bool EnableRuntimeDiagnostics = IsEnvTrue("NBN_WORKBENCH_RUNTIME_DIAGNOSTICS_ENABLED");
    private static readonly string ActivityDiagnosticsPeriod =
        ResolveEnvOrDefault("NBN_WORKBENCH_ACTIVITY_DIAGNOSTICS_PERIOD", "64");
    private readonly UiDispatcher _dispatcher;
    private readonly ConnectionViewModel _connections;
    private readonly WorkbenchClient _client;
    private readonly ILocalProjectLaunchPreparer _launchPreparer;
    private readonly ILocalFirewallManager _firewallManager;
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
    private readonly Dictionary<(Guid NodeId, Guid BrainId), WorkerBrainBackendProbeCacheEntry> _workerBrainBackendCache = new();
    private readonly WorkbenchSystemLoadHistoryTracker _systemLoadHistory = new();
    private readonly SemaphoreSlim _refreshGate = new(1, 1);
    private bool _suspendReconnectRequests;
    private bool _canShowMoreHostedActors;
    private int _hostedActorBrainRenderLimit = InitialHostedActorBrainRenderLimit;
    private int _hostedActorRenderedBrainCount;
    private int _hostedActorTotalBrainCount;
    private string _statusMessage = "Idle";
    private string _settingsLaunchStatus = "Idle";
    private string _hiveMindLaunchStatus = "Idle";
    private string _ioLaunchStatus = "Idle";
    private string _reproLaunchStatus = "Idle";
    private string _speciationLaunchStatus = "Idle";
    private string _workerLaunchStatus = "Idle";
    private string _obsLaunchStatus = "Idle";
    private string _profileCurrentSystemStatus = "Idle";
    private string _pullSettingsHost = string.Empty;
    private string _pullSettingsPortText = string.Empty;
    private string _pullSettingsName = string.Empty;
    private string _settingsPullStatus = "Pull excludes discovery endpoint settings.";
    private string _lastSeededPullSettingsHost = string.Empty;
    private string _lastSeededPullSettingsPortText = string.Empty;
    private string _lastSeededPullSettingsName = string.Empty;
    private string _workerEndpointSummary = "No active nodes.";
    private string _hostedActorSummary = "Hosted actors: awaiting current brain state.";
    private string _systemLoadResourceSummary = "Resource usage: awaiting worker telemetry.";
    private string _systemLoadPressureSummary = "Pressure: awaiting HiveMind telemetry.";
    private string _systemLoadTickSummary = "Tick health: awaiting HiveMind status.";
    private string _systemLoadHealthSummary = "Health: awaiting HiveMind status.";
    private string _systemLoadSparklinePathData = WorkbenchSystemLoadSummaryBuilder.EmptySparklinePathData;
    private string _systemLoadSparklineStroke = WorkbenchSystemLoadSummaryBuilder.NeutralSparklineStroke;
    private string _workerCapabilityRefreshSecondsText = WorkerCapabilitySettingsKeys.DefaultBenchmarkRefreshSeconds.ToString(CultureInfo.InvariantCulture);
    private string _workerPressureRebalanceWindowText = WorkerCapabilitySettingsKeys.DefaultPressureRebalanceWindow.ToString(CultureInfo.InvariantCulture);
    private string _workerPressureViolationRatioText = WorkerCapabilityMath.FormatRatio(WorkerCapabilitySettingsKeys.DefaultPressureViolationRatio);
    private string _workerPressureTolerancePercentText = WorkerCapabilityMath.FormatRatio(WorkerCapabilitySettingsKeys.DefaultPressureLimitTolerancePercent);
    private string _workerRegionShardGpuNeuronThresholdText = WorkerCapabilitySettingsKeys.DefaultRegionShardGpuNeuronThreshold.ToString(CultureInfo.InvariantCulture);
    private string _workerPolicyStatus = "Settings-backed defaults.";
    private string _workerCapabilityRefreshSecondsServerValue = WorkerCapabilitySettingsKeys.DefaultBenchmarkRefreshSeconds.ToString(CultureInfo.InvariantCulture);
    private string _workerPressureRebalanceWindowServerValue = WorkerCapabilitySettingsKeys.DefaultPressureRebalanceWindow.ToString(CultureInfo.InvariantCulture);
    private string _workerPressureViolationRatioServerValue = WorkerCapabilityMath.FormatRatio(WorkerCapabilitySettingsKeys.DefaultPressureViolationRatio);
    private string _workerPressureTolerancePercentServerValue = WorkerCapabilityMath.FormatRatio(WorkerCapabilitySettingsKeys.DefaultPressureLimitTolerancePercent);
    private string _workerRegionShardGpuNeuronThresholdServerValue = WorkerCapabilitySettingsKeys.DefaultRegionShardGpuNeuronThreshold.ToString(CultureInfo.InvariantCulture);
    private bool _workerCapabilityRefreshSecondsDirty;
    private bool _workerPressureRebalanceWindowDirty;
    private bool _workerPressureViolationRatioDirty;
    private bool _workerPressureTolerancePercentDirty;
    private bool _workerRegionShardGpuNeuronThresholdDirty;
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

    /// <summary>
    /// Initializes the Workbench orchestration panel with runtime services, launch helpers, and UI callbacks.
    /// </summary>
    public OrchestratorPanelViewModel(
        UiDispatcher dispatcher,
        ConnectionViewModel connections,
        WorkbenchClient client,
        Action<Guid>? brainDiscovered = null,
        Action<IReadOnlyList<BrainListItem>>? brainsUpdated = null,
        Func<Task>? connectAll = null,
        Action? disconnectAll = null,
        ILocalProjectLaunchPreparer? launchPreparer = null,
        ILocalFirewallManager? firewallManager = null)
    {
        _dispatcher = dispatcher;
        _connections = connections;
        _client = client;
        _launchPreparer = launchPreparer ?? new LocalProjectLaunchPreparer();
        _firewallManager = firewallManager ?? new LocalFirewallManager();
        _brainDiscovered = brainDiscovered;
        _brainsUpdated = brainsUpdated;
        _connectAll = connectAll;
        _disconnectAll = disconnectAll;
        SeedPullSettingsSourceFromConnections(force: true);
        _connections.PropertyChanged += OnConnectionsPropertyChanged;
        Nodes = new ObservableCollection<NodeStatusItem>();
        WorkerEndpoints = new ObservableCollection<WorkerEndpointItem>();
        WorkerNodeGroups = new ObservableCollection<WorkerNodeGroupItem>();
        Endpoints = new ObservableCollection<EndpointStatusItem>();
        Actors = new ObservableCollection<NodeStatusItem>();
        ActorNodeGroups = new ObservableCollection<HostedActorNodeGroupItem>();
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
        ProfileCurrentSystemCommand = new AsyncRelayCommand(ProfileCurrentSystemAsync);
        ShowMoreHostedActorsCommand = new AsyncRelayCommand(ShowMoreHostedActorsAsync, () => CanShowMoreHostedActors);
        RefreshEndpointRows();
        _ = StartAutoRefreshAsync();
    }

    public ObservableCollection<NodeStatusItem> Nodes { get; }
    public ObservableCollection<WorkerEndpointItem> WorkerEndpoints { get; }
    public ObservableCollection<WorkerNodeGroupItem> WorkerNodeGroups { get; }
    public ObservableCollection<EndpointStatusItem> Endpoints { get; }
    public ObservableCollection<NodeStatusItem> Actors { get; }
    public ObservableCollection<HostedActorNodeGroupItem> ActorNodeGroups { get; }
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

    public AsyncRelayCommand ProfileCurrentSystemCommand { get; }
    public AsyncRelayCommand ShowMoreHostedActorsCommand { get; }

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

    public string WorkerEndpointSummary
    {
        get => _workerEndpointSummary;
        set => SetProperty(ref _workerEndpointSummary, value);
    }

    public string HostedActorSummary
    {
        get => _hostedActorSummary;
        set => SetProperty(ref _hostedActorSummary, value);
    }

    public bool CanShowMoreHostedActors => _canShowMoreHostedActors;

    public string SystemLoadResourceSummary
    {
        get => _systemLoadResourceSummary;
        set => SetProperty(ref _systemLoadResourceSummary, value);
    }

    public string SystemLoadPressureSummary
    {
        get => _systemLoadPressureSummary;
        set => SetProperty(ref _systemLoadPressureSummary, value);
    }

    public string SystemLoadTickSummary
    {
        get => _systemLoadTickSummary;
        set => SetProperty(ref _systemLoadTickSummary, value);
    }

    public string SystemLoadHealthSummary
    {
        get => _systemLoadHealthSummary;
        set => SetProperty(ref _systemLoadHealthSummary, value);
    }

    public string SystemLoadSparklinePathData
    {
        get => _systemLoadSparklinePathData;
        set => SetProperty(ref _systemLoadSparklinePathData, value);
    }

    public string SystemLoadSparklineStroke
    {
        get => _systemLoadSparklineStroke;
        set => SetProperty(ref _systemLoadSparklineStroke, value);
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

    public string WorkerRegionShardGpuNeuronThresholdText
    {
        get => _workerRegionShardGpuNeuronThresholdText;
        set
        {
            if (SetProperty(ref _workerRegionShardGpuNeuronThresholdText, value))
            {
                _workerRegionShardGpuNeuronThresholdDirty = !string.Equals(
                    NormalizeWorkerPolicyValue(value),
                    _workerRegionShardGpuNeuronThresholdServerValue,
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

    private void SetHostedActorRenderState(int visibleBrainCount, int totalBrainCount, bool canShowMore)
    {
        _hostedActorRenderedBrainCount = visibleBrainCount;
        _hostedActorTotalBrainCount = totalBrainCount;
        _canShowMoreHostedActors = canShowMore;
        HostedActorSummary = BuildHostedActorSummary(visibleBrainCount, totalBrainCount, canShowMore);
        OnPropertyChanged(nameof(CanShowMoreHostedActors));
        ShowMoreHostedActorsCommand.RaiseCanExecuteChanged();
    }

    private static string BuildHostedActorSummary(int visibleBrainCount, int totalBrainCount, bool canShowMore)
    {
        if (totalBrainCount <= 0)
        {
            return "Hosted actors: no known brains.";
        }

        if (visibleBrainCount >= totalBrainCount)
        {
            return $"Hosted actors: {visibleBrainCount} brain{(visibleBrainCount == 1 ? string.Empty : "s")} in scope.";
        }

        return canShowMore
            ? $"Hosted actors: showing {visibleBrainCount} of {totalBrainCount} brains; more live brains are available."
            : $"Hosted actors: showing {visibleBrainCount} of {totalBrainCount} brains.";
    }

    private async Task ShowMoreHostedActorsAsync()
    {
        if (!CanShowMoreHostedActors)
        {
            return;
        }

        _hostedActorBrainRenderLimit = _hostedActorBrainRenderLimit > int.MaxValue - HostedActorBrainRenderStep
            ? int.MaxValue
            : _hostedActorBrainRenderLimit + HostedActorBrainRenderStep;
        await RefreshAsync(force: true).ConfigureAwait(false);
    }
}
