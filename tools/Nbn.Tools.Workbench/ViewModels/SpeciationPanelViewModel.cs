using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Platform.Storage;
using Nbn.Proto;
using Nbn.Proto.Speciation;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class SpeciationPanelViewModel : ViewModelBase, IAsyncDisposable
{
    private const int DefaultLiveChartIntervalSeconds = 2;
    private const int MinLiveChartIntervalSeconds = 1;
    private const int MaxLiveChartIntervalSeconds = 30;
    private const double PopulationChartPlotWidth = 360d;
    private const double PopulationChartPlotHeight = 156d;
    private const double PopulationChartPaddingX = 8d;
    private const double PopulationChartPaddingY = 8d;
    private const double FlowChartPlotWidth = 360d;
    private const double FlowChartPlotHeight = 220d;
    private const double FlowChartPaddingX = 8d;
    private const double FlowChartPaddingY = 8d;
    private static readonly string[] SpeciesChartPalette =
    [
        "#3B82F6",
        "#E76F51",
        "#2A9D8F",
        "#F4A261",
        "#A855F7",
        "#1F7A8C",
        "#BC6C25",
        "#6C8F3A",
        "#D9467A",
        "#6D597A",
        "#00A896",
        "#577590"
    ];

    private readonly UiDispatcher _dispatcher;
    private readonly ConnectionViewModel _connections;
    private readonly WorkbenchClient _client;
    private readonly Func<Task>? _startSpeciationService;
    private readonly Func<Task>? _stopSpeciationService;
    private readonly Func<Task>? _refreshOrchestrator;
    private readonly LocalServiceRunner _evolutionRunner = new();
    private readonly bool _enableLiveChartsAutoRefresh;
    private CancellationTokenSource? _simPollCts;
    private CancellationTokenSource? _liveChartsPollCts;
    private string? _simStdoutLogPath;

    private string _status = "Idle";
    private string _serviceSummary = "Service status not loaded.";
    private string _configStatus = "Config not loaded.";
    private string _historyStatus = "History not loaded.";
    private string _simStatus = "Simulator idle.";
    private string _simSessionId = "(none)";
    private string _simProgress = "No session.";
    private string _simDetailedStats = "No simulator statistics yet.";
    private string _simLastFailure = "(none)";
    private long _currentEpochId;
    private uint _currentMembershipCount;
    private uint _currentSpeciesCount;
    private uint _currentLineageEdgeCount;
    private string _currentEpochMaxDivergenceLabel = "Max divergence (current epoch): (n/a)";
    private bool _configEnabled = true;
    private string _policyVersion = "default";
    private string _defaultSpeciesId = "species.default";
    private string _defaultSpeciesDisplayName = "Default species";
    private string _startupReconcileReason = "startup_reconcile";
    private string _lineageMatchThreshold = "0.70";
    private string _lineageSplitThreshold = "0.60";
    private string _parentConsensusThreshold = "0.50";
    private string _hysteresisMargin = "0.10";
    private bool _createDerivedSpecies = true;
    private string _derivedSpeciesPrefix = "branch";
    private bool _startNewEpochConfirmPending;
    private bool _clearAllHistoryConfirmPending;
    private bool _deleteEpochConfirmPending;
    private long? _deleteEpochConfirmTarget;
    private string _deleteEpochText = string.Empty;
    private string _epochFilterText = string.Empty;
    private string _historyLimitText = "256";
    private string _historyBrainIdText = string.Empty;
    private string _simParentAOverrideFilePath = string.Empty;
    private string _simParentBOverrideFilePath = string.Empty;
    private SpeciationSimulatorBrainOption? _simSelectedParentABrain;
    private SpeciationSimulatorBrainOption? _simSelectedParentBBrain;
    private string _simBindHost = "127.0.0.1";
    private string _simPortText = "12074";
    private string _simSeedText = "12345";
    private string _simIntervalMsText = "1000";
    private string _simStatusSecondsText = "2";
    private string _simTimeoutSecondsText = "10";
    private string _simMaxIterationsText = "0";
    private string _simMaxParentPoolText = "512";
    private string _simMinRunsText = "1";
    private string _simMaxRunsText = "6";
    private string _simGammaText = "1";
    private bool _simCommitToSpeciation = true;
    private bool _simSpawnChildren;
    private bool _liveChartsEnabled;
    private string _liveChartsIntervalSecondsText = DefaultLiveChartIntervalSeconds.ToString(CultureInfo.InvariantCulture);
    private string _liveChartsStatus = "Live chart updates disabled.";
    private string _populationChartRangeLabel = "Epochs: (no data)";
    private string _populationChartMetricLabel = "Population count by species.";
    private string _populationChartYAxisTopLabel = "0";
    private string _populationChartYAxisMidLabel = "0";
    private string _populationChartYAxisBottomLabel = "0";
    private int _populationChartLegendColumns = 2;
    private string _flowChartRangeLabel = "Epochs: (no data)";
    private string _flowChartStartEpochLabel = "(n/a)";
    private string _flowChartEndEpochLabel = "(n/a)";
    private int _flowChartLegendColumns = 2;

    public SpeciationPanelViewModel(
        UiDispatcher dispatcher,
        ConnectionViewModel connections,
        WorkbenchClient client,
        Func<Task>? startSpeciationService = null,
        Func<Task>? stopSpeciationService = null,
        Func<Task>? refreshOrchestrator = null,
        bool enableLiveChartsAutoRefresh = true)
    {
        _dispatcher = dispatcher;
        _connections = connections;
        _client = client;
        _startSpeciationService = startSpeciationService;
        _stopSpeciationService = stopSpeciationService;
        _refreshOrchestrator = refreshOrchestrator;
        _enableLiveChartsAutoRefresh = enableLiveChartsAutoRefresh;
        _simBindHost = _connections.LocalBindHost;

        SpeciesCounts = new ObservableCollection<SpeciationSpeciesCountItem>();
        HistoryRows = new ObservableCollection<SpeciationHistoryItem>();
        EpochSummaries = new ObservableCollection<SpeciationEpochSummaryItem>();
        SimActiveBrains = new ObservableCollection<SpeciationSimulatorBrainOption>();
        PopulationChartSeries = new ObservableCollection<SpeciationLineChartSeriesItem>();
        PopulationChartLegend = new ObservableCollection<SpeciationChartLegendItem>();
        FlowChartAreas = new ObservableCollection<SpeciationFlowChartAreaItem>();
        FlowChartLegend = new ObservableCollection<SpeciationChartLegendItem>();

        RefreshAllCommand = new AsyncRelayCommand(RefreshAllAsync);
        RefreshStatusCommand = new AsyncRelayCommand(RefreshStatusAsync);
        LoadConfigCommand = new AsyncRelayCommand(LoadConfigAsync);
        ApplyConfigCommand = new AsyncRelayCommand(ApplyConfigAsync);
        StartNewEpochCommand = new AsyncRelayCommand(StartNewEpochAsync);
        ClearAllHistoryCommand = new AsyncRelayCommand(ClearAllHistoryAsync);
        DeleteEpochCommand = new AsyncRelayCommand(DeleteEpochAsync);
        RefreshMembershipsCommand = new AsyncRelayCommand(RefreshMembershipsAsync);
        RefreshHistoryCommand = new AsyncRelayCommand(RefreshHistoryAsync);
        StartServiceCommand = new AsyncRelayCommand(StartServiceAsync);
        StopServiceCommand = new AsyncRelayCommand(StopServiceAsync);
        StartSimulatorCommand = new AsyncRelayCommand(StartSimulatorAsync);
        StopSimulatorCommand = new AsyncRelayCommand(StopSimulatorAsync);
        RefreshSimulatorStatusCommand = new AsyncRelayCommand(RefreshSimulatorStatusAsync);
        BrowseSimParentAOverrideFileCommand = new AsyncRelayCommand(() => BrowseSimulatorParentFileAsync(SimulatorParentFileKind.ParentAOverride));
        BrowseSimParentBOverrideFileCommand = new AsyncRelayCommand(() => BrowseSimulatorParentFileAsync(SimulatorParentFileKind.ParentBOverride));

        _liveChartsEnabled = _enableLiveChartsAutoRefresh;
        _liveChartsStatus = _liveChartsEnabled
            ? $"Live updates active ({DefaultLiveChartIntervalSeconds}s)."
            : "Live chart updates disabled.";
        if (_liveChartsEnabled)
        {
            StartLiveChartsPolling();
        }
    }

    public ConnectionViewModel Connections => _connections;

    public ObservableCollection<SpeciationSpeciesCountItem> SpeciesCounts { get; }
    public ObservableCollection<SpeciationHistoryItem> HistoryRows { get; }
    public ObservableCollection<SpeciationEpochSummaryItem> EpochSummaries { get; }
    public ObservableCollection<SpeciationSimulatorBrainOption> SimActiveBrains { get; }
    public ObservableCollection<SpeciationLineChartSeriesItem> PopulationChartSeries { get; }
    public ObservableCollection<SpeciationChartLegendItem> PopulationChartLegend { get; }
    public ObservableCollection<SpeciationFlowChartAreaItem> FlowChartAreas { get; }
    public ObservableCollection<SpeciationChartLegendItem> FlowChartLegend { get; }

    public AsyncRelayCommand RefreshAllCommand { get; }
    public AsyncRelayCommand RefreshStatusCommand { get; }
    public AsyncRelayCommand LoadConfigCommand { get; }
    public AsyncRelayCommand ApplyConfigCommand { get; }
    public AsyncRelayCommand StartNewEpochCommand { get; }
    public AsyncRelayCommand ClearAllHistoryCommand { get; }
    public AsyncRelayCommand DeleteEpochCommand { get; }
    public AsyncRelayCommand RefreshMembershipsCommand { get; }
    public AsyncRelayCommand RefreshHistoryCommand { get; }
    public AsyncRelayCommand StartServiceCommand { get; }
    public AsyncRelayCommand StopServiceCommand { get; }
    public AsyncRelayCommand StartSimulatorCommand { get; }
    public AsyncRelayCommand StopSimulatorCommand { get; }
    public AsyncRelayCommand RefreshSimulatorStatusCommand { get; }
    public AsyncRelayCommand BrowseSimParentAOverrideFileCommand { get; }
    public AsyncRelayCommand BrowseSimParentBOverrideFileCommand { get; }

    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }

    public string ServiceSummary
    {
        get => _serviceSummary;
        set => SetProperty(ref _serviceSummary, value);
    }

    public string ConfigStatus
    {
        get => _configStatus;
        set => SetProperty(ref _configStatus, value);
    }

    public string HistoryStatus
    {
        get => _historyStatus;
        set => SetProperty(ref _historyStatus, value);
    }

    public string SimulatorStatus
    {
        get => _simStatus;
        set => SetProperty(ref _simStatus, value);
    }

    public string SimulatorSessionId
    {
        get => _simSessionId;
        set => SetProperty(ref _simSessionId, value);
    }

    public string SimulatorProgress
    {
        get => _simProgress;
        set => SetProperty(ref _simProgress, value);
    }

    public string SimulatorDetailedStats
    {
        get => _simDetailedStats;
        set => SetProperty(ref _simDetailedStats, value);
    }

    public string SimulatorLastFailure
    {
        get => _simLastFailure;
        set => SetProperty(ref _simLastFailure, value);
    }

    public long CurrentEpochId
    {
        get => _currentEpochId;
        set
        {
            if (SetProperty(ref _currentEpochId, value))
            {
                OnPropertyChanged(nameof(CurrentEpochLabel));
            }
        }
    }

    public string CurrentEpochLabel => CurrentEpochId > 0 ? CurrentEpochId.ToString(CultureInfo.InvariantCulture) : "(unknown)";

    public uint CurrentMembershipCount
    {
        get => _currentMembershipCount;
        set => SetProperty(ref _currentMembershipCount, value);
    }

    public uint CurrentSpeciesCount
    {
        get => _currentSpeciesCount;
        set => SetProperty(ref _currentSpeciesCount, value);
    }

    public uint CurrentLineageEdgeCount
    {
        get => _currentLineageEdgeCount;
        set => SetProperty(ref _currentLineageEdgeCount, value);
    }

    public string CurrentEpochMaxDivergenceLabel
    {
        get => _currentEpochMaxDivergenceLabel;
        set => SetProperty(ref _currentEpochMaxDivergenceLabel, value);
    }

    public bool ConfigEnabled
    {
        get => _configEnabled;
        set => SetProperty(ref _configEnabled, value);
    }

    public string PolicyVersion
    {
        get => _policyVersion;
        set => SetProperty(ref _policyVersion, value);
    }

    public string DefaultSpeciesId
    {
        get => _defaultSpeciesId;
        set => SetProperty(ref _defaultSpeciesId, value);
    }

    public string DefaultSpeciesDisplayName
    {
        get => _defaultSpeciesDisplayName;
        set => SetProperty(ref _defaultSpeciesDisplayName, value);
    }

    public string StartupReconcileReason
    {
        get => _startupReconcileReason;
        set => SetProperty(ref _startupReconcileReason, value);
    }

    public string LineageMatchThreshold
    {
        get => _lineageMatchThreshold;
        set => SetProperty(ref _lineageMatchThreshold, value);
    }

    public string LineageSplitThreshold
    {
        get => _lineageSplitThreshold;
        set => SetProperty(ref _lineageSplitThreshold, value);
    }

    public string ParentConsensusThreshold
    {
        get => _parentConsensusThreshold;
        set => SetProperty(ref _parentConsensusThreshold, value);
    }

    public string HysteresisMargin
    {
        get => _hysteresisMargin;
        set => SetProperty(ref _hysteresisMargin, value);
    }

    public bool CreateDerivedSpecies
    {
        get => _createDerivedSpecies;
        set => SetProperty(ref _createDerivedSpecies, value);
    }

    public string DerivedSpeciesPrefix
    {
        get => _derivedSpeciesPrefix;
        set => SetProperty(ref _derivedSpeciesPrefix, value);
    }

    public string EpochFilterText
    {
        get => _epochFilterText;
        set => SetProperty(ref _epochFilterText, value);
    }

    public string DeleteEpochText
    {
        get => _deleteEpochText;
        set
        {
            if (!SetProperty(ref _deleteEpochText, value))
            {
                return;
            }

            if (_deleteEpochConfirmPending)
            {
                _deleteEpochConfirmPending = false;
                _deleteEpochConfirmTarget = null;
                OnPropertyChanged(nameof(DeleteEpochLabel));
            }
        }
    }

    public string HistoryLimitText
    {
        get => _historyLimitText;
        set => SetProperty(ref _historyLimitText, value);
    }

    public string HistoryBrainIdText
    {
        get => _historyBrainIdText;
        set => SetProperty(ref _historyBrainIdText, value);
    }

    public string StartNewEpochLabel => _startNewEpochConfirmPending ? "Confirm New Epoch" : "Start New Epoch";
    public string ClearAllHistoryLabel => _clearAllHistoryConfirmPending ? "Confirm Clear All" : "Clear All History";
    public string DeleteEpochLabel => _deleteEpochConfirmPending ? "Confirm Delete Epoch" : "Delete Epoch";

    public SpeciationSimulatorBrainOption? SimSelectedParentABrain
    {
        get => _simSelectedParentABrain;
        set => SetProperty(ref _simSelectedParentABrain, value);
    }

    public SpeciationSimulatorBrainOption? SimSelectedParentBBrain
    {
        get => _simSelectedParentBBrain;
        set => SetProperty(ref _simSelectedParentBBrain, value);
    }

    public string SimParentAOverrideFilePath
    {
        get => _simParentAOverrideFilePath;
        set
        {
            if (SetProperty(ref _simParentAOverrideFilePath, value))
            {
                OnPropertyChanged(nameof(SimParentAOverrideFilePathDisplay));
            }
        }
    }

    public string SimParentBOverrideFilePath
    {
        get => _simParentBOverrideFilePath;
        set
        {
            if (SetProperty(ref _simParentBOverrideFilePath, value))
            {
                OnPropertyChanged(nameof(SimParentBOverrideFilePathDisplay));
            }
        }
    }

    public string SimParentAOverrideFilePathDisplay
        => string.IsNullOrWhiteSpace(SimParentAOverrideFilePath)
            ? "(no Parent A override file)"
            : SimParentAOverrideFilePath;

    public string SimParentBOverrideFilePathDisplay
        => string.IsNullOrWhiteSpace(SimParentBOverrideFilePath)
            ? "(no Parent B override file)"
            : SimParentBOverrideFilePath;

    public string SimBindHost
    {
        get => _simBindHost;
        set => SetProperty(ref _simBindHost, value);
    }

    public string SimPortText
    {
        get => _simPortText;
        set => SetProperty(ref _simPortText, value);
    }

    public string SimSeedText
    {
        get => _simSeedText;
        set => SetProperty(ref _simSeedText, value);
    }

    public string SimIntervalMsText
    {
        get => _simIntervalMsText;
        set => SetProperty(ref _simIntervalMsText, value);
    }

    public string SimStatusSecondsText
    {
        get => _simStatusSecondsText;
        set => SetProperty(ref _simStatusSecondsText, value);
    }

    public string SimTimeoutSecondsText
    {
        get => _simTimeoutSecondsText;
        set => SetProperty(ref _simTimeoutSecondsText, value);
    }

    public string SimMaxIterationsText
    {
        get => _simMaxIterationsText;
        set => SetProperty(ref _simMaxIterationsText, value);
    }

    public string SimMaxParentPoolText
    {
        get => _simMaxParentPoolText;
        set => SetProperty(ref _simMaxParentPoolText, value);
    }

    public string SimMinRunsText
    {
        get => _simMinRunsText;
        set => SetProperty(ref _simMinRunsText, value);
    }

    public string SimMaxRunsText
    {
        get => _simMaxRunsText;
        set => SetProperty(ref _simMaxRunsText, value);
    }

    public string SimGammaText
    {
        get => _simGammaText;
        set => SetProperty(ref _simGammaText, value);
    }

    public bool SimCommitToSpeciation
    {
        get => _simCommitToSpeciation;
        set => SetProperty(ref _simCommitToSpeciation, value);
    }

    public bool SimSpawnChildren
    {
        get => _simSpawnChildren;
        set => SetProperty(ref _simSpawnChildren, value);
    }

    public bool SimRunnerActive => _evolutionRunner.IsRunning;

    public bool LiveChartsEnabled
    {
        get => _liveChartsEnabled;
        set
        {
            if (!SetProperty(ref _liveChartsEnabled, value))
            {
                return;
            }

            if (value)
            {
                LiveChartsStatus = $"Live updates active ({ParseLiveChartIntervalSecondsOrDefault()}s).";
                StartLiveChartsPolling();
            }
            else
            {
                StopLiveChartsPolling();
                LiveChartsStatus = "Live chart updates paused.";
            }
        }
    }

    public string LiveChartsIntervalSecondsText
    {
        get => _liveChartsIntervalSecondsText;
        set => SetProperty(ref _liveChartsIntervalSecondsText, value);
    }

    public string LiveChartsStatus
    {
        get => _liveChartsStatus;
        set => SetProperty(ref _liveChartsStatus, value);
    }

    public string PopulationChartRangeLabel
    {
        get => _populationChartRangeLabel;
        set => SetProperty(ref _populationChartRangeLabel, value);
    }

    public string PopulationChartMetricLabel
    {
        get => _populationChartMetricLabel;
        set => SetProperty(ref _populationChartMetricLabel, value);
    }

    public string PopulationChartYAxisTopLabel
    {
        get => _populationChartYAxisTopLabel;
        set => SetProperty(ref _populationChartYAxisTopLabel, value);
    }

    public string PopulationChartYAxisMidLabel
    {
        get => _populationChartYAxisMidLabel;
        set => SetProperty(ref _populationChartYAxisMidLabel, value);
    }

    public string PopulationChartYAxisBottomLabel
    {
        get => _populationChartYAxisBottomLabel;
        set => SetProperty(ref _populationChartYAxisBottomLabel, value);
    }

    public int PopulationChartLegendColumns
    {
        get => _populationChartLegendColumns;
        set => SetProperty(ref _populationChartLegendColumns, value);
    }

    public string FlowChartRangeLabel
    {
        get => _flowChartRangeLabel;
        set => SetProperty(ref _flowChartRangeLabel, value);
    }

    public string FlowChartStartEpochLabel
    {
        get => _flowChartStartEpochLabel;
        set => SetProperty(ref _flowChartStartEpochLabel, value);
    }

    public string FlowChartEndEpochLabel
    {
        get => _flowChartEndEpochLabel;
        set => SetProperty(ref _flowChartEndEpochLabel, value);
    }

    public int FlowChartLegendColumns
    {
        get => _flowChartLegendColumns;
        set => SetProperty(ref _flowChartLegendColumns, value);
    }

    public double PopulationChartWidth => PopulationChartPlotWidth;
    public double PopulationChartHeight => PopulationChartPlotHeight;
    public double FlowChartWidth => FlowChartPlotWidth;
    public double FlowChartHeight => FlowChartPlotHeight;

    public void UpdateActiveBrains(IReadOnlyList<BrainListItem> brains)
    {
        var options = brains
            .Where(entry => entry.BrainId != Guid.Empty)
            .Where(entry => !string.Equals(entry.State, "Dead", StringComparison.OrdinalIgnoreCase))
            .Select(entry => new SpeciationSimulatorBrainOption(entry.BrainId, entry.Display))
            .GroupBy(entry => entry.BrainId)
            .Select(group => group.First())
            .OrderBy(entry => entry.Label, StringComparer.OrdinalIgnoreCase)
            .ToList();
        var selectedAId = SimSelectedParentABrain?.BrainId;
        var selectedBId = SimSelectedParentBBrain?.BrainId;

        _dispatcher.Post(() =>
        {
            SimActiveBrains.Clear();
            foreach (var option in options)
            {
                SimActiveBrains.Add(option);
            }

            SimSelectedParentABrain = selectedAId.HasValue
                ? SimActiveBrains.FirstOrDefault(entry => entry.BrainId == selectedAId.Value)
                : null;
            SimSelectedParentBBrain = selectedBId.HasValue
                ? SimActiveBrains.FirstOrDefault(entry => entry.BrainId == selectedBId.Value)
                : null;

            if (SimSelectedParentABrain is null && SimActiveBrains.Count > 0)
            {
                SimSelectedParentABrain = SimActiveBrains[0];
            }

            if (SimSelectedParentBBrain is null)
            {
                SimSelectedParentBBrain = SimActiveBrains
                    .FirstOrDefault(entry => entry.BrainId != SimSelectedParentABrain?.BrainId);
            }
        });
    }

    public async ValueTask DisposeAsync()
    {
        StopLiveChartsPolling();
        _simPollCts?.Cancel();
        await StopSimulatorAsync().ConfigureAwait(false);
    }

    private async Task RefreshAllAsync()
    {
        await RefreshStatusAsync().ConfigureAwait(false);
        await LoadConfigAsync().ConfigureAwait(false);
        await RefreshMembershipsAsync().ConfigureAwait(false);
        await RefreshHistoryAsync().ConfigureAwait(false);
        await RefreshSimulatorStatusAsync().ConfigureAwait(false);
    }

    private void StartLiveChartsPolling()
    {
        StopLiveChartsPolling();
        _liveChartsPollCts = new CancellationTokenSource();
        _ = PollLiveChartsAsync(_liveChartsPollCts.Token);
    }

    private void StopLiveChartsPolling()
    {
        _liveChartsPollCts?.Cancel();
        _liveChartsPollCts = null;
    }

    private async Task PollLiveChartsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var intervalSeconds = ParseLiveChartIntervalSecondsOrDefault();
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(intervalSeconds), cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (cancellationToken.IsCancellationRequested || !LiveChartsEnabled)
            {
                break;
            }

            await RefreshHistoryAsync().ConfigureAwait(false);
            await RefreshMembershipsAsync().ConfigureAwait(false);
            LiveChartsStatus = $"Live updates active ({intervalSeconds}s).";
        }
    }

    private void ResetHistoryMutationConfirmations()
    {
        if (_clearAllHistoryConfirmPending)
        {
            _clearAllHistoryConfirmPending = false;
            OnPropertyChanged(nameof(ClearAllHistoryLabel));
        }

        if (_deleteEpochConfirmPending || _deleteEpochConfirmTarget.HasValue)
        {
            _deleteEpochConfirmPending = false;
            _deleteEpochConfirmTarget = null;
            OnPropertyChanged(nameof(DeleteEpochLabel));
        }
    }

    private async Task RefreshStatusAsync()
    {
        var response = await _client.GetSpeciationStatusAsync().ConfigureAwait(false);
        if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(response.FailureReason, response.FailureDetail);
            ServiceSummary = $"Status failed: {reason}";
            Status = ServiceSummary;
            return;
        }

        ApplyConfig(response.Config);
        _dispatcher.Post(() =>
        {
            CurrentEpochId = (long)response.CurrentEpoch.EpochId;
            CurrentMembershipCount = response.Status.MembershipCount;
            CurrentSpeciesCount = response.Status.SpeciesCount;
            CurrentLineageEdgeCount = response.Status.LineageEdgeCount;
            ServiceSummary = $"Epoch {CurrentEpochLabel} | memberships={CurrentMembershipCount} species={CurrentSpeciesCount} lineage={CurrentLineageEdgeCount}";
            Status = "Speciation status refreshed.";
        });
    }

    private async Task LoadConfigAsync()
    {
        var response = await _client.GetSpeciationConfigAsync().ConfigureAwait(false);
        if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(response.FailureReason, response.FailureDetail);
            ConfigStatus = $"Load failed: {reason}";
            Status = ConfigStatus;
            return;
        }

        ApplyConfig(response.Config);
        _dispatcher.Post(() =>
        {
            CurrentEpochId = (long)response.CurrentEpoch.EpochId;
            ConfigStatus = "Config loaded.";
            Status = "Speciation config refreshed.";
        });
    }

    private async Task ApplyConfigAsync()
    {
        _startNewEpochConfirmPending = false;
        OnPropertyChanged(nameof(StartNewEpochLabel));

        var config = BuildRuntimeConfigFromDraft();
        var response = await _client.SetSpeciationConfigAsync(config, startNewEpoch: false).ConfigureAwait(false);
        if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(response.FailureReason, response.FailureDetail);
            ConfigStatus = $"Apply failed: {reason}";
            Status = ConfigStatus;
            return;
        }

        ApplyConfig(response.Config);
        _dispatcher.Post(() =>
        {
            CurrentEpochId = (long)response.CurrentEpoch.EpochId;
            ConfigStatus = "Config applied.";
            Status = "Speciation config updated.";
        });

        if (_refreshOrchestrator is not null)
        {
            await _refreshOrchestrator().ConfigureAwait(false);
        }
    }

    private async Task StartNewEpochAsync()
    {
        ResetHistoryMutationConfirmations();
        if (!_startNewEpochConfirmPending)
        {
            _startNewEpochConfirmPending = true;
            OnPropertyChanged(nameof(StartNewEpochLabel));
            ConfigStatus = "Click Start New Epoch again to confirm.";
            Status = ConfigStatus;
            return;
        }

        _startNewEpochConfirmPending = false;
        OnPropertyChanged(nameof(StartNewEpochLabel));

        var config = BuildRuntimeConfigFromDraft();
        var response = await _client.SetSpeciationConfigAsync(config, startNewEpoch: true).ConfigureAwait(false);
        if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(response.FailureReason, response.FailureDetail);
            ConfigStatus = $"New epoch failed: {reason}";
            Status = ConfigStatus;
            return;
        }

        ApplyConfig(response.Config);
        _dispatcher.Post(() =>
        {
            CurrentEpochId = (long)response.CurrentEpoch.EpochId;
            ConfigStatus = $"New epoch started ({CurrentEpochLabel}).";
            Status = "Speciation epoch advanced.";
        });

        await RefreshMembershipsAsync().ConfigureAwait(false);
        await RefreshHistoryAsync().ConfigureAwait(false);
    }

    private async Task ClearAllHistoryAsync()
    {
        _startNewEpochConfirmPending = false;
        OnPropertyChanged(nameof(StartNewEpochLabel));

        if (!_clearAllHistoryConfirmPending)
        {
            _clearAllHistoryConfirmPending = true;
            _deleteEpochConfirmPending = false;
            _deleteEpochConfirmTarget = null;
            OnPropertyChanged(nameof(ClearAllHistoryLabel));
            OnPropertyChanged(nameof(DeleteEpochLabel));
            HistoryStatus = "Click Clear All History again to confirm. This removes all epoch history and starts a new epoch.";
            Status = HistoryStatus;
            return;
        }

        _clearAllHistoryConfirmPending = false;
        OnPropertyChanged(nameof(ClearAllHistoryLabel));
        var response = await _client.ResetSpeciationHistoryAsync().ConfigureAwait(false);
        if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(response.FailureReason, response.FailureDetail);
            HistoryStatus = $"Clear history failed: {reason}";
            Status = HistoryStatus;
            return;
        }

        ApplyConfig(response.Config);
        _dispatcher.Post(() =>
        {
            CurrentEpochId = (long)response.CurrentEpoch.EpochId;
            CurrentMembershipCount = 0;
            CurrentSpeciesCount = 0;
            CurrentLineageEdgeCount = 0;
            CurrentEpochMaxDivergenceLabel = $"Max divergence (epoch {CurrentEpochLabel}): (n/a)";
            HistoryStatus =
                $"History cleared: deleted epochs={response.DeletedEpochCount}, memberships={response.DeletedMembershipCount}, species={response.DeletedSpeciesCount}, decisions={response.DeletedDecisionCount}.";
            Status = HistoryStatus;
        });

        await RefreshStatusAsync().ConfigureAwait(false);
        await RefreshMembershipsAsync().ConfigureAwait(false);
        await RefreshHistoryAsync().ConfigureAwait(false);
    }

    private async Task DeleteEpochAsync()
    {
        _startNewEpochConfirmPending = false;
        OnPropertyChanged(nameof(StartNewEpochLabel));

        if (!long.TryParse(DeleteEpochText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var epochId) || epochId <= 0)
        {
            _deleteEpochConfirmPending = false;
            _deleteEpochConfirmTarget = null;
            OnPropertyChanged(nameof(DeleteEpochLabel));
            HistoryStatus = "Enter a positive epoch id to delete.";
            Status = HistoryStatus;
            return;
        }

        if (!_deleteEpochConfirmPending || _deleteEpochConfirmTarget != epochId)
        {
            _deleteEpochConfirmPending = true;
            _deleteEpochConfirmTarget = epochId;
            _clearAllHistoryConfirmPending = false;
            OnPropertyChanged(nameof(DeleteEpochLabel));
            OnPropertyChanged(nameof(ClearAllHistoryLabel));
            HistoryStatus = $"Click Delete Epoch again to confirm deletion of epoch {epochId}.";
            Status = HistoryStatus;
            return;
        }

        _deleteEpochConfirmPending = false;
        _deleteEpochConfirmTarget = null;
        OnPropertyChanged(nameof(DeleteEpochLabel));

        var response = await _client.DeleteSpeciationEpochAsync(epochId).ConfigureAwait(false);
        if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(response.FailureReason, response.FailureDetail);
            HistoryStatus = $"Delete epoch failed: {reason}";
            Status = HistoryStatus;
            return;
        }

        if (!response.Deleted)
        {
            HistoryStatus = $"Epoch {epochId} was not deleted.";
            Status = HistoryStatus;
            return;
        }

        _dispatcher.Post(() =>
        {
            CurrentEpochId = (long)response.CurrentEpoch.EpochId;
            HistoryStatus =
                $"Deleted epoch {epochId}: memberships={response.DeletedMembershipCount}, species={response.DeletedSpeciesCount}, decisions={response.DeletedDecisionCount}.";
            Status = HistoryStatus;
        });

        await RefreshStatusAsync().ConfigureAwait(false);
        await RefreshMembershipsAsync().ConfigureAwait(false);
        await RefreshHistoryAsync().ConfigureAwait(false);
    }

    private async Task RefreshMembershipsAsync()
    {
        var requestedEpochId = ResolveEpochFilter();
        var response = await _client.ListSpeciationMembershipsAsync(requestedEpochId).ConfigureAwait(false);
        if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(response.FailureReason, response.FailureDetail);
            HistoryStatus = $"Membership load failed: {reason}";
            Status = HistoryStatus;
            return;
        }

        var rows = response.Memberships
            .GroupBy(m => new
            {
                SpeciesId = string.IsNullOrWhiteSpace(m.SpeciesId) ? "(unknown)" : m.SpeciesId.Trim(),
                SpeciesName = string.IsNullOrWhiteSpace(m.SpeciesDisplayName) ? "(unnamed)" : m.SpeciesDisplayName.Trim()
            })
            .Select(group => new
            {
                group.Key.SpeciesId,
                group.Key.SpeciesName,
                Count = group.Count()
            })
            .OrderByDescending(entry => entry.Count)
            .ThenBy(entry => entry.SpeciesId, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var total = rows.Sum(entry => entry.Count);
        var maxCount = rows.Count == 0 ? 0 : rows.Max(entry => entry.Count);

        _dispatcher.Post(() =>
        {
            SpeciesCounts.Clear();
            foreach (var row in rows)
            {
                var ratio = total > 0 ? row.Count / (double)total : 0d;
                var bar = BuildBar(row.Count, maxCount);
                SpeciesCounts.Add(new SpeciationSpeciesCountItem(
                    row.SpeciesId,
                    row.SpeciesName,
                    row.Count,
                    ratio.ToString("P1", CultureInfo.InvariantCulture),
                    bar));
            }

            HistoryStatus = $"Loaded {total} memberships across {rows.Count} species.";
            Status = HistoryStatus;
        });
    }

    private async Task RefreshHistoryAsync()
    {
        var historyLimit = ParseUInt(HistoryLimitText, 256u);
        var epochFilter = ResolveEpochFilter();
        var brainFilter = Guid.TryParse(HistoryBrainIdText, out var parsedBrainId) && parsedBrainId != Guid.Empty
            ? parsedBrainId
            : (Guid?)null;
        var response = await _client.ListSpeciationHistoryAsync(
                epochId: epochFilter,
                brainId: brainFilter,
                limit: historyLimit)
            .ConfigureAwait(false);
        if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(response.FailureReason, response.FailureDetail);
            HistoryStatus = $"History load failed: {reason}";
            Status = HistoryStatus;
            return;
        }

        var historyRows = response.History
            .OrderByDescending(entry => entry.AssignedMs)
            .Select(entry => new SpeciationHistoryItem(
                (long)entry.EpochId,
                entry.BrainId?.TryToGuid(out var brainId) == true ? brainId.ToString("D") : "(none)",
                string.IsNullOrWhiteSpace(entry.SpeciesId) ? "(unknown)" : entry.SpeciesId.Trim(),
                string.IsNullOrWhiteSpace(entry.SpeciesDisplayName) ? "(unnamed)" : entry.SpeciesDisplayName.Trim(),
                string.IsNullOrWhiteSpace(entry.DecisionReason) ? "(none)" : entry.DecisionReason.Trim(),
                FormatTimestamp(entry.AssignedMs)))
            .ToList();

        var epochRows = response.History
            .GroupBy(entry => (long)entry.EpochId)
            .Select(group =>
            {
                var firstAssigned = group.Min(entry => entry.AssignedMs);
                var lastAssigned = group.Max(entry => entry.AssignedMs);
                var speciesCount = group
                    .Select(entry => string.IsNullOrWhiteSpace(entry.SpeciesId) ? "(unknown)" : entry.SpeciesId.Trim())
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .Count();
                return new SpeciationEpochSummaryItem(
                    EpochId: group.Key,
                    MembershipCount: group.Count(),
                    SpeciesCount: speciesCount,
                    FirstAssigned: FormatTimestamp(firstAssigned),
                    LastAssigned: FormatTimestamp(lastAssigned));
            })
            .OrderByDescending(entry => entry.EpochId)
            .ToList();
        var populationSnapshot = BuildPopulationChartSnapshot(response.History);
        var flowSnapshot = BuildFlowChartSnapshot(response.History);
        var divergenceSnapshot = BuildCurrentEpochDivergenceSnapshot(response.History, CurrentEpochId);

        _dispatcher.Post(() =>
        {
            HistoryRows.Clear();
            foreach (var row in historyRows)
            {
                HistoryRows.Add(row);
            }

            EpochSummaries.Clear();
            foreach (var row in epochRows)
            {
                EpochSummaries.Add(row);
            }

            ApplyPopulationChartSnapshot(populationSnapshot);
            ApplyFlowChartSnapshot(flowSnapshot);
            CurrentEpochMaxDivergenceLabel = divergenceSnapshot.Label;
            HistoryStatus = $"History loaded: {historyRows.Count} rows (total={response.TotalRecords}).";
            Status = HistoryStatus;
        });
    }

    private async Task StartServiceAsync()
    {
        if (_startSpeciationService is null)
        {
            ServiceSummary = "Speciation launcher is unavailable.";
            Status = ServiceSummary;
            return;
        }

        await _startSpeciationService().ConfigureAwait(false);
        if (_refreshOrchestrator is not null)
        {
            await _refreshOrchestrator().ConfigureAwait(false);
        }

        ServiceSummary = $"Speciation service launch requested ({Connections.SpeciationStatusLabel}).";
        Status = ServiceSummary;
    }

    private async Task StopServiceAsync()
    {
        if (_stopSpeciationService is null)
        {
            ServiceSummary = "Speciation stopper is unavailable.";
            Status = ServiceSummary;
            return;
        }

        await _stopSpeciationService().ConfigureAwait(false);
        if (_refreshOrchestrator is not null)
        {
            await _refreshOrchestrator().ConfigureAwait(false);
        }

        ServiceSummary = "Speciation service stop requested.";
        Status = ServiceSummary;
    }

    private async Task StartSimulatorAsync()
    {
        if (!TryParsePort(Connections.IoPortText, out var ioPort))
        {
            SimulatorStatus = "Invalid IO port for simulator.";
            Status = SimulatorStatus;
            return;
        }

        if (!TryParsePort(SimPortText, out var simPort))
        {
            SimulatorStatus = "Invalid simulator port.";
            Status = SimulatorStatus;
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("tools", "Nbn.Tools.EvolutionSim");
        if (string.IsNullOrWhiteSpace(projectPath))
        {
            SimulatorStatus = "EvolutionSim project not found.";
            Status = SimulatorStatus;
            return;
        }

        if (!TryResolveSimulatorParentIds(out var parentA, out var parentB, out var parentError))
        {
            SimulatorStatus = parentError;
            Status = SimulatorStatus;
            return;
        }

        var parentDefinitions = await TryResolveSimulatorParentArtifactsAsync(parentA, parentB).ConfigureAwait(false);
        if (!parentDefinitions.Success || parentDefinitions.ParentARef is null || parentDefinitions.ParentBRef is null)
        {
            SimulatorStatus = parentDefinitions.Error;
            Status = SimulatorStatus;
            return;
        }

        var args = BuildEvolutionSimArgs(ioPort, simPort, parentDefinitions.ParentARef, parentDefinitions.ParentBRef);
        var startInfo = new System.Diagnostics.ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = $"run --project \"{projectPath}\" -c Release --no-build -- {args}",
            UseShellExecute = false,
            CreateNoWindow = true
        };
        var startResult = await _evolutionRunner.StartAsync(startInfo, waitForExit: false, label: "EvolutionSim").ConfigureAwait(false);
        SimulatorStatus = startResult.Message;
        Status = $"Evolution simulator: {startResult.Message}";
        SimulatorDetailedStats = startResult.Success
            ? "Starting simulator status polling..."
            : "No simulator statistics yet.";
        OnPropertyChanged(nameof(SimRunnerActive));

        _simStdoutLogPath = ExtractLogPath(startResult.Message);
        _simPollCts?.Cancel();
        if (startResult.Success)
        {
            _simPollCts = new CancellationTokenSource();
            _ = PollSimulatorStatusAsync(_simPollCts.Token);
        }
    }

    private async Task StopSimulatorAsync()
    {
        _simPollCts?.Cancel();
        _simPollCts = null;

        var stopMessage = await _evolutionRunner.StopAsync().ConfigureAwait(false);
        SimulatorStatus = stopMessage;
        Status = $"Evolution simulator: {stopMessage}";
        SimulatorProgress = "No active simulator session.";
        SimulatorDetailedStats = "No simulator statistics yet.";
        OnPropertyChanged(nameof(SimRunnerActive));
    }

    private Task RefreshSimulatorStatusAsync()
    {
        if (string.IsNullOrWhiteSpace(_simStdoutLogPath))
        {
            if (_evolutionRunner.IsRunning)
            {
                SimulatorProgress = "Running (enable Workbench logging for live session details).";
                SimulatorDetailedStats = "Waiting for first simulator status payload.";
            }
            else
            {
                SimulatorProgress = "No active simulator session.";
                SimulatorDetailedStats = "No simulator statistics yet.";
            }

            return Task.CompletedTask;
        }

        if (!File.Exists(_simStdoutLogPath))
        {
            SimulatorProgress = _evolutionRunner.IsRunning
                ? "Waiting for simulator status stream..."
                : "Simulator log not found.";
            SimulatorDetailedStats = "No simulator statistics yet.";
            return Task.CompletedTask;
        }

        var lastLine = ReadLastNonEmptyLine(_simStdoutLogPath);
        if (string.IsNullOrWhiteSpace(lastLine))
        {
            SimulatorProgress = _evolutionRunner.IsRunning
                ? "Waiting for simulator status stream..."
                : "No simulator status rows.";
            SimulatorDetailedStats = "No simulator statistics yet.";
            return Task.CompletedTask;
        }

        if (!TryParseSimulatorStatus(lastLine, out var snapshot))
        {
            return Task.CompletedTask;
        }

        _dispatcher.Post(() =>
        {
            SimulatorSessionId = snapshot.SessionId;
            var childrenLabel = snapshot.ChildrenAddedToPool.ToString(CultureInfo.InvariantCulture);
            if (!SimSpawnChildren && snapshot.ChildrenAddedToPool == 0 && snapshot.ReproductionCalls > 0)
            {
                childrenLabel = "0 (no parent-pool growth yet)";
            }

            var minSimilarityLabel = snapshot.SimilaritySamples > 0 && snapshot.MinSimilarityObserved.HasValue
                ? snapshot.MinSimilarityObserved.Value.ToString("0.###", CultureInfo.InvariantCulture)
                : "n/a";
            var maxSimilarityLabel = snapshot.SimilaritySamples > 0 && snapshot.MaxSimilarityObserved.HasValue
                ? snapshot.MaxSimilarityObserved.Value.ToString("0.###", CultureInfo.InvariantCulture)
                : "n/a";

            SimulatorProgress =
                $"running={snapshot.Running} final={snapshot.Final} iter={snapshot.Iterations} parent_pool_size={snapshot.ParentPoolSize}";
            SimulatorDetailedStats =
                $"compat={snapshot.CompatiblePairs}/{snapshot.CompatibilityChecks} " +
                $"repro_calls={snapshot.ReproductionCalls} repro_fail={snapshot.ReproductionFailures} " +
                $"parent_pool_size={snapshot.ParentPoolSize} children_added_to_pool={childrenLabel} " +
                $"runs={snapshot.ReproductionRunsObserved} runs_mutated={snapshot.ReproductionRunsWithMutations} mutation_events={snapshot.ReproductionMutationEvents} " +
                $"min_similarity={minSimilarityLabel} max_similarity={maxSimilarityLabel} " +
                $"speciation={snapshot.SpeciationCommitSuccesses}/{snapshot.SpeciationCommitAttempts} " +
                $"seed={snapshot.LastSeed}";
            SimulatorLastFailure = string.IsNullOrWhiteSpace(snapshot.LastFailure) ? "(none)" : snapshot.LastFailure;
            if (!snapshot.Running)
            {
                SimulatorStatus = snapshot.Final ? "Completed." : "Stopped.";
            }
        });

        return Task.CompletedTask;
    }

    private async Task PollSimulatorStatusAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(1000, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            await RefreshSimulatorStatusAsync().ConfigureAwait(false);
        }
    }

    private void ApplyConfig(SpeciationRuntimeConfig config)
    {
        if (config is null)
        {
            return;
        }

        var snapshot = ParseSnapshot(config.ConfigSnapshotJson);
        _dispatcher.Post(() =>
        {
            PolicyVersion = string.IsNullOrWhiteSpace(config.PolicyVersion) ? "default" : config.PolicyVersion.Trim();
            DefaultSpeciesId = string.IsNullOrWhiteSpace(config.DefaultSpeciesId) ? "species.default" : config.DefaultSpeciesId.Trim();
            DefaultSpeciesDisplayName = string.IsNullOrWhiteSpace(config.DefaultSpeciesDisplayName)
                ? "Default species"
                : config.DefaultSpeciesDisplayName.Trim();
            StartupReconcileReason = string.IsNullOrWhiteSpace(config.StartupReconcileDecisionReason)
                ? "startup_reconcile"
                : config.StartupReconcileDecisionReason.Trim();

            ConfigEnabled = snapshot.Enabled;
            LineageMatchThreshold = snapshot.MatchThreshold.ToString("0.###", CultureInfo.InvariantCulture);
            LineageSplitThreshold = snapshot.SplitThreshold.ToString("0.###", CultureInfo.InvariantCulture);
            ParentConsensusThreshold = snapshot.ParentConsensusThreshold.ToString("0.###", CultureInfo.InvariantCulture);
            HysteresisMargin = snapshot.HysteresisMargin.ToString("0.###", CultureInfo.InvariantCulture);
            CreateDerivedSpecies = snapshot.CreateDerivedSpecies;
            DerivedSpeciesPrefix = snapshot.DerivedSpeciesPrefix;
        });
    }

    private SpeciationRuntimeConfig BuildRuntimeConfigFromDraft()
    {
        var matchThreshold = Clamp01(ParseDouble(LineageMatchThreshold, 0.70));
        var splitThreshold = Clamp01(ParseDouble(LineageSplitThreshold, Math.Max(0d, matchThreshold - 0.10d)));
        if (splitThreshold > matchThreshold)
        {
            splitThreshold = matchThreshold;
        }

        var parentConsensus = Clamp01(ParseDouble(ParentConsensusThreshold, 0.50));
        var hysteresisMargin = Math.Max(0d, ParseDouble(HysteresisMargin, Math.Max(0d, matchThreshold - splitThreshold)));
        var derivedPrefix = string.IsNullOrWhiteSpace(DerivedSpeciesPrefix) ? "branch" : DerivedSpeciesPrefix.Trim();
        var snapshot = new JsonObject
        {
            ["enabled"] = ConfigEnabled,
            ["assignment_policy"] = new JsonObject
            {
                ["lineage_match_threshold"] = matchThreshold,
                ["lineage_split_threshold"] = splitThreshold,
                ["parent_consensus_threshold"] = parentConsensus,
                ["lineage_hysteresis_margin"] = hysteresisMargin,
                ["create_derived_species_on_divergence"] = CreateDerivedSpecies,
                ["derived_species_prefix"] = derivedPrefix
            }
        };

        return new SpeciationRuntimeConfig
        {
            PolicyVersion = string.IsNullOrWhiteSpace(PolicyVersion) ? "default" : PolicyVersion.Trim(),
            ConfigSnapshotJson = snapshot.ToJsonString(),
            DefaultSpeciesId = string.IsNullOrWhiteSpace(DefaultSpeciesId) ? "species.default" : DefaultSpeciesId.Trim(),
            DefaultSpeciesDisplayName = string.IsNullOrWhiteSpace(DefaultSpeciesDisplayName)
                ? "Default species"
                : DefaultSpeciesDisplayName.Trim(),
            StartupReconcileDecisionReason = string.IsNullOrWhiteSpace(StartupReconcileReason)
                ? "startup_reconcile"
                : StartupReconcileReason.Trim()
        };
    }

    private (bool Enabled, double MatchThreshold, double SplitThreshold, double ParentConsensusThreshold, double HysteresisMargin, bool CreateDerivedSpecies, string DerivedSpeciesPrefix) ParseSnapshot(string snapshotJson)
    {
        var defaults = (
            Enabled: true,
            MatchThreshold: 0.70d,
            SplitThreshold: 0.60d,
            ParentConsensusThreshold: 0.50d,
            HysteresisMargin: 0.10d,
            CreateDerivedSpecies: true,
            DerivedSpeciesPrefix: "branch");

        if (string.IsNullOrWhiteSpace(snapshotJson))
        {
            return defaults;
        }

        try
        {
            var root = JsonNode.Parse(snapshotJson) as JsonObject;
            if (root is null)
            {
                return defaults;
            }

            var policy = root["assignment_policy"] as JsonObject
                         ?? root["assignmentPolicy"] as JsonObject
                         ?? root;
            var enabled = TryReadBool(root, "enabled") ?? defaults.Enabled;
            var match = Clamp01(TryReadDouble(policy, "lineage_match_threshold", "lineageMatchThreshold") ?? defaults.MatchThreshold);
            var split = Clamp01(TryReadDouble(policy, "lineage_split_threshold", "lineageSplitThreshold") ?? defaults.SplitThreshold);
            var consensus = Clamp01(TryReadDouble(policy, "parent_consensus_threshold", "parentConsensusThreshold") ?? defaults.ParentConsensusThreshold);
            var hysteresis = Math.Max(0d, TryReadDouble(policy, "lineage_hysteresis_margin", "lineageHysteresisMargin") ?? defaults.HysteresisMargin);
            var createDerived = TryReadBool(policy, "create_derived_species_on_divergence", "createDerivedSpeciesOnDivergence")
                                ?? defaults.CreateDerivedSpecies;
            var prefix = TryReadString(policy, "derived_species_prefix", "derivedSpeciesPrefix")
                         ?? defaults.DerivedSpeciesPrefix;
            return (enabled, match, split, consensus, hysteresis, createDerived, string.IsNullOrWhiteSpace(prefix) ? "branch" : prefix.Trim());
        }
        catch (JsonException)
        {
            return defaults;
        }
    }

    private static bool? TryReadBool(JsonObject source, params string[] keys)
    {
        foreach (var key in keys)
        {
            if (source[key] is not JsonNode node)
            {
                continue;
            }

            if (node is JsonValue value)
            {
                if (value.TryGetValue<bool>(out var boolValue))
                {
                    return boolValue;
                }

                if (value.TryGetValue<string>(out var stringValue)
                    && bool.TryParse(stringValue, out var parsed))
                {
                    return parsed;
                }
            }
        }

        return null;
    }

    private static double? TryReadDouble(JsonObject source, params string[] keys)
    {
        foreach (var key in keys)
        {
            if (source[key] is not JsonNode node)
            {
                continue;
            }

            if (node is JsonValue value)
            {
                if (value.TryGetValue<double>(out var asDouble))
                {
                    return asDouble;
                }

                if (value.TryGetValue<float>(out var asFloat))
                {
                    return asFloat;
                }

                if (value.TryGetValue<string>(out var asString)
                    && double.TryParse(asString, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
                {
                    return parsed;
                }
            }
        }

        return null;
    }

    private static string? TryReadString(JsonObject source, params string[] keys)
    {
        foreach (var key in keys)
        {
            if (source[key] is JsonValue value && value.TryGetValue<string>(out var stringValue))
            {
                return stringValue;
            }
        }

        return null;
    }

    private static string NormalizeFailure(SpeciationFailureReason reason, string? detail)
    {
        var reasonText = reason.ToString();
        if (string.IsNullOrWhiteSpace(detail))
        {
            return reasonText;
        }

        return $"{reasonText}: {detail.Trim()}";
    }

    private long? ResolveEpochFilter()
    {
        if (string.IsNullOrWhiteSpace(EpochFilterText))
        {
            return CurrentEpochId > 0 ? CurrentEpochId : null;
        }

        return long.TryParse(EpochFilterText.Trim(), out var parsed) && parsed > 0 ? parsed : null;
    }

    private async Task BrowseSimulatorParentFileAsync(SimulatorParentFileKind kind)
    {
        var title = kind == SimulatorParentFileKind.ParentAOverride
            ? "Select Parent A override file"
            : "Select Parent B override file";
        var file = await PickOpenFileAsync(title).ConfigureAwait(false);
        if (file is null)
        {
            return;
        }

        var path = FormatPath(file);
        _dispatcher.Post(() =>
        {
            if (kind == SimulatorParentFileKind.ParentAOverride)
            {
                SimParentAOverrideFilePath = path;
            }
            else
            {
                SimParentBOverrideFilePath = path;
            }
        });
    }

    private bool TryResolveSimulatorParentIds(out Guid parentA, out Guid parentB, out string error)
    {
        if (!TryResolveParentBrainId(
                selected: SimSelectedParentABrain,
                overrideFilePath: SimParentAOverrideFilePath,
                parentLabel: "A",
                out parentA,
                out error))
        {
            parentB = Guid.Empty;
            return false;
        }

        if (!TryResolveParentBrainId(
                selected: SimSelectedParentBBrain,
                overrideFilePath: SimParentBOverrideFilePath,
                parentLabel: "B",
                out parentB,
                out error))
        {
            return false;
        }

        if (parentA == parentB)
        {
            error = "Simulator requires two distinct brain parents.";
            return false;
        }

        error = string.Empty;
        return true;
    }

    private async Task<(bool Success, ArtifactRef? ParentARef, ArtifactRef? ParentBRef, string Error)> TryResolveSimulatorParentArtifactsAsync(Guid parentA, Guid parentB)
    {
        var parentAResolution = await ResolveSimulatorParentArtifactAsync(parentA, "A").ConfigureAwait(false);
        if (!parentAResolution.Success)
        {
            return (false, null, null, parentAResolution.Error);
        }

        var parentBResolution = await ResolveSimulatorParentArtifactAsync(parentB, "B").ConfigureAwait(false);
        if (!parentBResolution.Success)
        {
            return (false, null, null, parentBResolution.Error);
        }

        return (true, parentAResolution.Artifact, parentBResolution.Artifact, string.Empty);
    }

    private async Task<(bool Success, ArtifactRef? Artifact, string Error)> ResolveSimulatorParentArtifactAsync(Guid brainId, string parentLabel)
    {
        ArtifactRef? parentArtifact = await _client.ExportBrainDefinitionAsync(brainId, rebaseOverlays: false).ConfigureAwait(false);
        if (!HasUsableSimulatorParentArtifact(parentArtifact))
        {
            var info = await _client.RequestBrainInfoAsync(brainId).ConfigureAwait(false);
            if (HasUsableSimulatorParentArtifact(info?.BaseDefinition))
            {
                parentArtifact = info!.BaseDefinition;
            }
        }

        if (!HasUsableSimulatorParentArtifact(parentArtifact))
        {
            return (false, null, $"Parent {parentLabel} definition unavailable for brain {brainId:D}. Ensure IO/Hive are connected and the brain is active.");
        }

        if (string.IsNullOrWhiteSpace(parentArtifact!.StoreUri))
        {
            return (false, null, $"Parent {parentLabel} definition missing store_uri for brain {brainId:D}. Configure artifact storage before running simulator.");
        }

        return (true, parentArtifact, string.Empty);
    }

    private static bool HasUsableSimulatorParentArtifact(ArtifactRef? artifactRef)
        => artifactRef is not null && artifactRef.TryToSha256Hex(out _);

    private static bool TryResolveParentBrainId(
        SpeciationSimulatorBrainOption? selected,
        string? overrideFilePath,
        string parentLabel,
        out Guid brainId,
        out string error)
    {
        if (!string.IsNullOrWhiteSpace(overrideFilePath))
        {
            if (!File.Exists(overrideFilePath))
            {
                brainId = Guid.Empty;
                error = $"Parent {parentLabel} override file not found: {overrideFilePath}";
                return false;
            }

            foreach (var rawLine in File.ReadLines(overrideFilePath))
            {
                var line = rawLine.Trim();
                if (line.Length == 0 || line.StartsWith("#", StringComparison.Ordinal))
                {
                    continue;
                }

                if (Guid.TryParse(line, out brainId) && brainId != Guid.Empty)
                {
                    error = string.Empty;
                    return true;
                }

                brainId = Guid.Empty;
                error = $"Parent {parentLabel} override file must contain a brain GUID: {overrideFilePath}";
                return false;
            }

            brainId = Guid.Empty;
            error = $"Parent {parentLabel} override file has no usable brain GUID: {overrideFilePath}";
            return false;
        }

        if (selected is null || selected.BrainId == Guid.Empty)
        {
            brainId = Guid.Empty;
            error = $"Simulator requires Parent {parentLabel}.";
            return false;
        }

        brainId = selected.BrainId;
        error = string.Empty;
        return true;
    }

    private static async Task<IStorageFile?> PickOpenFileAsync(string title)
    {
        var provider = GetStorageProvider();
        if (provider is null)
        {
            return null;
        }

        var options = new FilePickerOpenOptions
        {
            Title = title,
            AllowMultiple = false
        };
        var results = await provider.OpenFilePickerAsync(options).ConfigureAwait(false);
        return results.FirstOrDefault();
    }

    private static IStorageProvider? GetStorageProvider()
    {
        var window = GetMainWindow();
        return window?.StorageProvider;
    }

    private static Window? GetMainWindow()
        => Application.Current?.ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop
            ? desktop.MainWindow
            : null;

    private static string FormatPath(IStorageItem item)
        => item.Path?.LocalPath ?? item.Path?.ToString() ?? item.Name;

    private string BuildEvolutionSimArgs(int ioPort, int simPort, ArtifactRef parentARef, ArtifactRef parentBRef)
    {
        var parentASpec = BuildEvolutionParentSpec(parentARef);
        var parentBSpec = BuildEvolutionParentSpec(parentBRef);
        var args = new List<string>
        {
            "run",
            $"--io-address {Connections.IoHost}:{ioPort}",
            $"--io-id {QuoteIfNeeded(Connections.IoGateway)}",
            $"--settings-address {Connections.SettingsHost}:{Math.Max(1, ParseInt(Connections.SettingsPortText, 12010))}",
            $"--settings-name {QuoteIfNeeded(Connections.SettingsName)}",
            $"--bind-host {QuoteIfNeeded(SimBindHost)}",
            $"--port {simPort}",
            $"--seed {ParseULong(SimSeedText, 12345UL)}",
            $"--interval-ms {ParseInt(SimIntervalMsText, 1000)}",
            $"--status-seconds {Math.Max(1, ParseInt(SimStatusSecondsText, 2))}",
            $"--timeout-seconds {Math.Max(1, ParseInt(SimTimeoutSecondsText, 10))}",
            $"--max-iterations {Math.Max(0, ParseInt(SimMaxIterationsText, 0))}",
            $"--max-parent-pool {Math.Max(2, ParseInt(SimMaxParentPoolText, 512))}",
            $"--min-runs {Math.Max(1, ParseInt(SimMinRunsText, 1))}",
            $"--max-runs {Math.Min(64, Math.Max(1, ParseInt(SimMaxRunsText, 6)))}",
            $"--run-gamma {ParseDouble(SimGammaText, 1d).ToString("0.###", CultureInfo.InvariantCulture)}",
            $"--commit-to-speciation {(SimCommitToSpeciation ? "true" : "false")}",
            $"--spawn-children {(SimSpawnChildren ? "true" : "false")}",
            $"--parent {QuoteIfNeeded(parentASpec)}",
            $"--parent {QuoteIfNeeded(parentBSpec)}",
            "--json"
        };

        return string.Join(" ", args);
    }

    private static string BuildEvolutionParentSpec(ArtifactRef artifactRef)
    {
        if (!artifactRef.TryToSha256Hex(out var sha))
        {
            return string.Empty;
        }

        var storeUri = string.IsNullOrWhiteSpace(artifactRef.StoreUri)
            ? string.Empty
            : artifactRef.StoreUri.Trim();
        var mediaType = string.IsNullOrWhiteSpace(artifactRef.MediaType)
            ? "application/x-nbn"
            : artifactRef.MediaType.Trim();
        return $"{sha},{artifactRef.SizeBytes},{storeUri},{mediaType}";
    }

    private int ParseLiveChartIntervalSecondsOrDefault()
    {
        var parsed = ParseInt(LiveChartsIntervalSecondsText, DefaultLiveChartIntervalSeconds);
        return Math.Clamp(parsed, MinLiveChartIntervalSeconds, MaxLiveChartIntervalSeconds);
    }

    private static PopulationChartSnapshot BuildPopulationChartSnapshot(IReadOnlyList<SpeciationMembershipRecord> history)
    {
        var (epochRows, speciesOrder) = BuildEpochPopulationFrame(history);
        if (epochRows.Count == 0 || speciesOrder.Count == 0)
        {
            return new PopulationChartSnapshot(
                RangeLabel: "Epochs: (no data)",
                MetricLabel: "Population count by species.",
                YTopLabel: "0",
                YMidLabel: "0",
                YBottomLabel: "0",
                LegendColumns: 2,
                Series: Array.Empty<SpeciationLineChartSeriesItem>(),
                Legend: Array.Empty<SpeciationChartLegendItem>());
        }

        var maxCount = Math.Max(1, epochRows.SelectMany(row => row.Counts.Values).DefaultIfEmpty(0).Max());
        var series = new List<SpeciationLineChartSeriesItem>(speciesOrder.Count);
        var legend = new List<SpeciationChartLegendItem>(speciesOrder.Count);
        foreach (var species in speciesOrder)
        {
            var values = epochRows
                .Select(row => row.Counts.TryGetValue(species.SpeciesId, out var count) ? count : 0)
                .Select(value => (double)value)
                .ToArray();
            var path = BuildLinePath(
                values,
                yMin: 0d,
                yMax: maxCount,
                plotWidth: PopulationChartPlotWidth,
                plotHeight: PopulationChartPlotHeight,
                paddingX: PopulationChartPaddingX,
                paddingY: PopulationChartPaddingY);
            if (string.IsNullOrWhiteSpace(path))
            {
                continue;
            }

            var color = ResolveSpeciesColor(species.SpeciesId);
            var latestCount = values.Length == 0 ? 0 : (int)values[^1];
            var latestCountLabel = latestCount.ToString(CultureInfo.InvariantCulture);
            series.Add(new SpeciationLineChartSeriesItem(species.SpeciesId, species.DisplayName, color, path, latestCountLabel));
            legend.Add(new SpeciationChartLegendItem(species.DisplayName, color, $"now {latestCountLabel}"));
        }

        var legendColumns = Math.Clamp(series.Count <= 1 ? 2 : series.Count, 2, 4);
        var minEpoch = epochRows[0].EpochId;
        var maxEpoch = epochRows[^1].EpochId;
        var rangeLabel = $"Epochs {minEpoch}..{maxEpoch} ({epochRows.Count} samples)";
        return new PopulationChartSnapshot(
            RangeLabel: rangeLabel,
            MetricLabel: "Population count by species.",
            YTopLabel: FormatAxisValue(maxCount),
            YMidLabel: FormatAxisValue(maxCount / 2d),
            YBottomLabel: "0",
            LegendColumns: legendColumns,
            Series: series,
            Legend: legend);
    }

    private static FlowChartSnapshot BuildFlowChartSnapshot(IReadOnlyList<SpeciationMembershipRecord> history)
    {
        var (epochRows, speciesOrder) = BuildEpochPopulationFrame(history);
        if (epochRows.Count == 0 || speciesOrder.Count == 0)
        {
            return new FlowChartSnapshot(
                RangeLabel: "Epochs: (no data)",
                StartEpochLabel: "(n/a)",
                EndEpochLabel: "(n/a)",
                LegendColumns: 2,
                Areas: Array.Empty<SpeciationFlowChartAreaItem>(),
                Legend: Array.Empty<SpeciationChartLegendItem>());
        }

        var speciesCount = speciesOrder.Count;
        var epochCount = epochRows.Count;
        var startsByEpoch = new List<double[]>(epochCount);
        var endsByEpoch = new List<double[]>(epochCount);
        foreach (var row in epochRows)
        {
            var starts = new double[speciesCount];
            var ends = new double[speciesCount];
            var cumulative = 0d;
            var total = Math.Max(0, row.TotalCount);
            for (var i = 0; i < speciesCount; i++)
            {
                starts[i] = cumulative;
                var count = row.Counts.TryGetValue(speciesOrder[i].SpeciesId, out var value) ? value : 0;
                var ratio = total > 0 ? count / (double)total : 0d;
                cumulative = Math.Min(1d, cumulative + ratio);
                ends[i] = cumulative;
            }

            startsByEpoch.Add(starts);
            endsByEpoch.Add(ends);
        }

        var areas = new List<SpeciationFlowChartAreaItem>(speciesCount);
        var legend = new List<SpeciationChartLegendItem>(speciesCount);
        for (var speciesIndex = 0; speciesIndex < speciesCount; speciesIndex++)
        {
            var starts = new double[epochCount];
            var ends = new double[epochCount];
            for (var epochIndex = 0; epochIndex < epochCount; epochIndex++)
            {
                starts[epochIndex] = startsByEpoch[epochIndex][speciesIndex];
                ends[epochIndex] = endsByEpoch[epochIndex][speciesIndex];
            }

            var path = BuildFlowAreaPath(
                starts,
                ends,
                plotWidth: FlowChartPlotWidth,
                plotHeight: FlowChartPlotHeight,
                paddingX: FlowChartPaddingX,
                paddingY: FlowChartPaddingY);
            if (string.IsNullOrWhiteSpace(path))
            {
                continue;
            }

            var species = speciesOrder[speciesIndex];
            var color = ResolveSpeciesColor(species.SpeciesId);
            var fill = WithAlpha(color, 0x8C);
            var lastShare = Math.Max(0d, ends[^1] - starts[^1]);
            var lastShareLabel = lastShare.ToString("P1", CultureInfo.InvariantCulture);
            areas.Add(new SpeciationFlowChartAreaItem(species.SpeciesId, species.DisplayName, fill, color, path, lastShareLabel));
            legend.Add(new SpeciationChartLegendItem(species.DisplayName, color, $"now {lastShareLabel}"));
        }

        var minEpoch = epochRows[0].EpochId;
        var maxEpoch = epochRows[^1].EpochId;
        var legendColumns = Math.Clamp(areas.Count <= 1 ? 2 : areas.Count, 2, 4);
        return new FlowChartSnapshot(
            RangeLabel: $"Stacked share of total population per epoch ({minEpoch}..{maxEpoch}).",
            StartEpochLabel: minEpoch.ToString(CultureInfo.InvariantCulture),
            EndEpochLabel: maxEpoch.ToString(CultureInfo.InvariantCulture),
            LegendColumns: legendColumns,
            Areas: areas,
            Legend: legend);
    }

    private void ApplyPopulationChartSnapshot(PopulationChartSnapshot snapshot)
    {
        ReplaceItems(PopulationChartSeries, snapshot.Series);
        ReplaceItems(PopulationChartLegend, snapshot.Legend);
        PopulationChartRangeLabel = snapshot.RangeLabel;
        PopulationChartMetricLabel = snapshot.MetricLabel;
        PopulationChartYAxisTopLabel = snapshot.YTopLabel;
        PopulationChartYAxisMidLabel = snapshot.YMidLabel;
        PopulationChartYAxisBottomLabel = snapshot.YBottomLabel;
        PopulationChartLegendColumns = snapshot.LegendColumns;
    }

    private void ApplyFlowChartSnapshot(FlowChartSnapshot snapshot)
    {
        ReplaceItems(FlowChartAreas, snapshot.Areas);
        ReplaceItems(FlowChartLegend, snapshot.Legend);
        FlowChartRangeLabel = snapshot.RangeLabel;
        FlowChartStartEpochLabel = snapshot.StartEpochLabel;
        FlowChartEndEpochLabel = snapshot.EndEpochLabel;
        FlowChartLegendColumns = snapshot.LegendColumns;
    }

    private static (List<EpochPopulationRow> EpochRows, List<SpeciesPopulationMeta> SpeciesOrder) BuildEpochPopulationFrame(IReadOnlyList<SpeciationMembershipRecord> history)
    {
        if (history.Count == 0)
        {
            return (new List<EpochPopulationRow>(), new List<SpeciesPopulationMeta>());
        }

        var speciesStats = new Dictionary<string, SpeciesPopulationMeta>(StringComparer.OrdinalIgnoreCase);
        var epochRows = history
            .GroupBy(entry => (long)entry.EpochId)
            .OrderBy(group => group.Key)
            .Select(group =>
            {
                var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                foreach (var record in group)
                {
                    var speciesId = NormalizeSpeciesId(record.SpeciesId);
                    var speciesName = NormalizeSpeciesName(record.SpeciesDisplayName, speciesId);
                    counts.TryGetValue(speciesId, out var prior);
                    counts[speciesId] = prior + 1;
                    if (speciesStats.TryGetValue(speciesId, out var existing))
                    {
                        speciesStats[speciesId] = existing with
                        {
                            DisplayName = string.IsNullOrWhiteSpace(existing.DisplayName) ? speciesName : existing.DisplayName,
                            TotalCount = existing.TotalCount + 1
                        };
                    }
                    else
                    {
                        speciesStats[speciesId] = new SpeciesPopulationMeta(speciesId, speciesName, 1);
                    }
                }

                return new EpochPopulationRow(group.Key, counts, counts.Values.Sum());
            })
            .ToList();
        var speciesOrder = speciesStats.Values
            .OrderByDescending(item => item.TotalCount)
            .ThenBy(item => item.SpeciesId, StringComparer.OrdinalIgnoreCase)
            .ToList();
        return (epochRows, speciesOrder);
    }

    private static DivergenceSnapshot BuildCurrentEpochDivergenceSnapshot(
        IReadOnlyList<SpeciationMembershipRecord> history,
        long currentEpochId)
    {
        if (history.Count == 0)
        {
            return new DivergenceSnapshot("Max divergence (current epoch): (n/a)");
        }

        var targetEpoch = currentEpochId > 0
            ? currentEpochId
            : history.Max(entry => (long)entry.EpochId);
        double? maxDivergence = null;
        double? minSimilarity = null;
        string maxBrainLabel = "(unknown)";
        var sampleCount = 0;

        foreach (var record in history.Where(entry => (long)entry.EpochId == targetEpoch))
        {
            if (!TryExtractSimilarityScore(record.DecisionMetadataJson, out var similarity))
            {
                continue;
            }

            var boundedSimilarity = Clamp01(similarity);
            var divergence = 1d - boundedSimilarity;
            sampleCount++;
            if (!minSimilarity.HasValue || boundedSimilarity < minSimilarity.Value)
            {
                minSimilarity = boundedSimilarity;
            }

            if (maxDivergence.HasValue && divergence <= maxDivergence.Value)
            {
                continue;
            }

            maxDivergence = divergence;
            maxBrainLabel = record.BrainId?.TryToGuid(out var brainId) == true && brainId != Guid.Empty
                ? brainId.ToString("D")
                : "(none)";
        }

        if (!maxDivergence.HasValue || !minSimilarity.HasValue)
        {
            return new DivergenceSnapshot($"Max divergence (epoch {targetEpoch}): (n/a, no similarity scores)");
        }

        var label =
            $"Max divergence (epoch {targetEpoch}) = {maxDivergence.Value:0.###} (min similarity {minSimilarity.Value:0.###}, samples={sampleCount}, brain={maxBrainLabel}).";
        return new DivergenceSnapshot(label);
    }

    private static bool TryExtractSimilarityScore(string? metadataJson, out double similarityScore)
    {
        similarityScore = 0d;
        if (string.IsNullOrWhiteSpace(metadataJson))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(metadataJson);
            var root = document.RootElement;
            if (TryGetSimilarityFromElement(root, out similarityScore))
            {
                return true;
            }

            if (root.TryGetProperty("report", out var report) && TryGetSimilarityFromElement(report, out similarityScore))
            {
                return true;
            }

            if (root.TryGetProperty("scores", out var scores) && TryGetSimilarityFromElement(scores, out similarityScore))
            {
                return true;
            }
        }
        catch (JsonException)
        {
        }

        return false;
    }

    private static bool TryGetSimilarityFromElement(JsonElement element, out double similarityScore)
    {
        similarityScore = 0d;
        if (TryGetJsonDouble(element, "similarity_score", out similarityScore)
            || TryGetJsonDouble(element, "similarityScore", out similarityScore))
        {
            return true;
        }

        if (element.TryGetProperty("scores", out var scores)
            && (TryGetJsonDouble(scores, "similarity_score", out similarityScore)
                || TryGetJsonDouble(scores, "similarityScore", out similarityScore)))
        {
            return true;
        }

        return false;
    }

    private static bool TryGetJsonDouble(JsonElement element, string propertyName, out double value)
    {
        value = 0d;
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return false;
        }

        return property.ValueKind switch
        {
            JsonValueKind.Number when property.TryGetDouble(out value) => true,
            JsonValueKind.String when double.TryParse(
                property.GetString(),
                NumberStyles.Float,
                CultureInfo.InvariantCulture,
                out value) => true,
            _ => false
        };
    }

    private static string BuildLinePath(
        IReadOnlyList<double> values,
        double yMin,
        double yMax,
        double plotWidth,
        double plotHeight,
        double paddingX,
        double paddingY)
    {
        if (values.Count == 0)
        {
            return string.Empty;
        }

        var boundedMin = double.IsFinite(yMin) ? yMin : 0d;
        var boundedMax = double.IsFinite(yMax) ? yMax : 1d;
        if (!(boundedMax > boundedMin))
        {
            boundedMax = boundedMin + 1d;
        }

        var usableWidth = Math.Max(1d, plotWidth - (paddingX * 2d));
        var usableHeight = Math.Max(1d, plotHeight - (paddingY * 2d));
        var xStep = values.Count > 1 ? usableWidth / (values.Count - 1) : 0d;
        var builder = new StringBuilder(values.Count * 26);
        for (var i = 0; i < values.Count; i++)
        {
            var x = paddingX + (i * xStep);
            var ratio = Math.Clamp((values[i] - boundedMin) / (boundedMax - boundedMin), 0d, 1d);
            var y = paddingY + ((1d - ratio) * usableHeight);
            builder.Append(i == 0 ? "M " : " L ");
            builder.Append(x.ToString("0.###", CultureInfo.InvariantCulture));
            builder.Append(' ');
            builder.Append(y.ToString("0.###", CultureInfo.InvariantCulture));
        }

        return builder.ToString();
    }

    private static string BuildFlowAreaPath(
        IReadOnlyList<double> starts,
        IReadOnlyList<double> ends,
        double plotWidth,
        double plotHeight,
        double paddingX,
        double paddingY)
    {
        if (starts.Count == 0 || starts.Count != ends.Count)
        {
            return string.Empty;
        }

        var hasArea = false;
        for (var i = 0; i < starts.Count; i++)
        {
            if ((ends[i] - starts[i]) > 1e-6d)
            {
                hasArea = true;
                break;
            }
        }

        if (!hasArea)
        {
            return string.Empty;
        }

        var usableWidth = Math.Max(1d, plotWidth - (paddingX * 2d));
        var usableHeight = Math.Max(1d, plotHeight - (paddingY * 2d));
        var yStep = starts.Count > 1 ? usableHeight / (starts.Count - 1) : 0d;
        var builder = new StringBuilder(starts.Count * 48);
        for (var i = 0; i < starts.Count; i++)
        {
            var x = paddingX + (Math.Clamp(ends[i], 0d, 1d) * usableWidth);
            var y = paddingY + (i * yStep);
            builder.Append(i == 0 ? "M " : " L ");
            builder.Append(x.ToString("0.###", CultureInfo.InvariantCulture));
            builder.Append(' ');
            builder.Append(y.ToString("0.###", CultureInfo.InvariantCulture));
        }

        for (var i = starts.Count - 1; i >= 0; i--)
        {
            var x = paddingX + (Math.Clamp(starts[i], 0d, 1d) * usableWidth);
            var y = paddingY + (i * yStep);
            builder.Append(" L ");
            builder.Append(x.ToString("0.###", CultureInfo.InvariantCulture));
            builder.Append(' ');
            builder.Append(y.ToString("0.###", CultureInfo.InvariantCulture));
        }

        builder.Append(" Z");
        return builder.ToString();
    }

    private static string ResolveSpeciesColor(string speciesId)
    {
        if (string.IsNullOrWhiteSpace(speciesId))
        {
            return SpeciesChartPalette[0];
        }

        var hash = 17;
        foreach (var ch in speciesId)
        {
            hash = (hash * 31) + ch;
        }

        var index = (hash & int.MaxValue) % SpeciesChartPalette.Length;
        return SpeciesChartPalette[index];
    }

    private static string WithAlpha(string colorHex, byte alpha)
    {
        if (string.IsNullOrWhiteSpace(colorHex))
        {
            return $"#{alpha:X2}808080";
        }

        var normalized = colorHex.Trim();
        if (normalized.StartsWith('#'))
        {
            normalized = normalized[1..];
        }

        if (normalized.Length == 8)
        {
            return $"#{alpha:X2}{normalized[2..]}";
        }

        if (normalized.Length == 6)
        {
            return $"#{alpha:X2}{normalized}";
        }

        return colorHex;
    }

    private static string FormatAxisValue(double value)
    {
        if (!double.IsFinite(value))
        {
            return "n/a";
        }

        var abs = Math.Abs(value);
        if (abs >= 1_000_000_000d)
        {
            return $"{value / 1_000_000_000d:0.##}B";
        }

        if (abs >= 1_000_000d)
        {
            return $"{value / 1_000_000d:0.##}M";
        }

        if (abs >= 1_000d)
        {
            return $"{value / 1_000d:0.##}K";
        }

        return value.ToString("0.###", CultureInfo.InvariantCulture);
    }

    private static string NormalizeSpeciesId(string? speciesId)
        => string.IsNullOrWhiteSpace(speciesId) ? "(unknown)" : speciesId.Trim();

    private static string NormalizeSpeciesName(string? speciesName, string speciesId)
        => string.IsNullOrWhiteSpace(speciesName) ? speciesId : speciesName.Trim();

    private static void ReplaceItems<T>(ObservableCollection<T> target, IReadOnlyList<T> source)
    {
        target.Clear();
        foreach (var item in source)
        {
            target.Add(item);
        }
    }

    private static string QuoteIfNeeded(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "\"\"";
        }

        var trimmed = value.Trim();
        return trimmed.Contains(' ') ? $"\"{trimmed}\"" : trimmed;
    }

    private static int ParseInt(string raw, int fallback)
    {
        return int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static uint ParseUInt(string raw, uint fallback)
    {
        return uint.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static ulong ParseULong(string raw, ulong fallback)
    {
        return ulong.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static double ParseDouble(string raw, double fallback)
    {
        return double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static bool TryParsePort(string raw, out int port)
    {
        return int.TryParse(raw, out port) && port > 0 && port < 65536;
    }

    private static string BuildBar(int count, int maxCount)
    {
        if (count <= 0 || maxCount <= 0)
        {
            return string.Empty;
        }

        var width = Math.Clamp((int)Math.Round((count / (double)maxCount) * 16d, MidpointRounding.AwayFromZero), 1, 16);
        return new string('#', width);
    }

    private static double Clamp01(double value)
    {
        if (value < 0d)
        {
            return 0d;
        }

        if (value > 1d)
        {
            return 1d;
        }

        return value;
    }

    private static string FormatTimestamp(ulong ms)
    {
        if (ms == 0)
        {
            return "(n/a)";
        }

        try
        {
            return DateTimeOffset.FromUnixTimeMilliseconds((long)ms).ToLocalTime().ToString("g");
        }
        catch
        {
            return "(n/a)";
        }
    }

    private static string? ExtractLogPath(string message)
    {
        if (string.IsNullOrWhiteSpace(message))
        {
            return null;
        }

        const string token = "Logs:";
        var index = message.IndexOf(token, StringComparison.OrdinalIgnoreCase);
        if (index < 0)
        {
            return null;
        }

        var path = message[(index + token.Length)..].Trim();
        return string.IsNullOrWhiteSpace(path) ? null : path;
    }

    private static string? ReadLastNonEmptyLine(string path)
    {
        try
        {
            var lines = File.ReadAllLines(path);
            for (var i = lines.Length - 1; i >= 0; i--)
            {
                if (!string.IsNullOrWhiteSpace(lines[i]))
                {
                    return lines[i].Trim();
                }
            }
        }
        catch
        {
        }

        return null;
    }

    private static bool TryParseSimulatorStatus(string rawLine, out EvolutionSimStatusSnapshot snapshot)
    {
        snapshot = default;
        if (string.IsNullOrWhiteSpace(rawLine))
        {
            return false;
        }

        var jsonIndex = rawLine.IndexOf('{');
        if (jsonIndex < 0)
        {
            return false;
        }

        try
        {
            using var doc = JsonDocument.Parse(rawLine[jsonIndex..]);
            var root = doc.RootElement;
            if (!root.TryGetProperty("type", out var typeNode)
                || !string.Equals(typeNode.GetString(), "evolution_sim_status", StringComparison.Ordinal))
            {
                return false;
            }

            snapshot = new EvolutionSimStatusSnapshot(
                SessionId: root.TryGetProperty("session_id", out var sessionIdNode) ? sessionIdNode.GetString() ?? "(unknown)" : "(unknown)",
                Running: root.TryGetProperty("running", out var runningNode) && runningNode.GetBoolean(),
                Final: root.TryGetProperty("final", out var finalNode) && finalNode.GetBoolean(),
                Iterations: root.TryGetProperty("iterations", out var iterationsNode) ? iterationsNode.GetUInt64() : 0UL,
                ParentPoolSize: root.TryGetProperty("parent_pool_size", out var poolNode) ? poolNode.GetInt32() : 0,
                CompatibilityChecks: root.TryGetProperty("compatibility_checks", out var checksNode) ? checksNode.GetUInt64() : 0UL,
                CompatiblePairs: root.TryGetProperty("compatible_pairs", out var pairsNode) ? pairsNode.GetUInt64() : 0UL,
                ReproductionCalls: root.TryGetProperty("reproduction_calls", out var callsNode) ? callsNode.GetUInt64() : 0UL,
                ReproductionFailures: root.TryGetProperty("reproduction_failures", out var failuresNode) ? failuresNode.GetUInt64() : 0UL,
                ReproductionRunsObserved: root.TryGetProperty("reproduction_runs_observed", out var runsNode) ? runsNode.GetUInt64() : 0UL,
                ReproductionRunsWithMutations: root.TryGetProperty("reproduction_runs_with_mutations", out var runsMutatedNode) ? runsMutatedNode.GetUInt64() : 0UL,
                ReproductionMutationEvents: root.TryGetProperty("reproduction_mutation_events", out var mutationEventsNode) ? mutationEventsNode.GetUInt64() : 0UL,
                SimilaritySamples: root.TryGetProperty("similarity_samples", out var similaritySamplesNode) ? similaritySamplesNode.GetUInt64() : 0UL,
                MinSimilarityObserved: TryGetJsonDouble(root, "min_similarity_observed", out var minSimilarityObserved) ? minSimilarityObserved : null,
                MaxSimilarityObserved: TryGetJsonDouble(root, "max_similarity_observed", out var maxSimilarityObserved) ? maxSimilarityObserved : null,
                ChildrenAddedToPool: root.TryGetProperty("children_added_to_pool", out var childrenNode) ? childrenNode.GetUInt64() : 0UL,
                SpeciationCommitAttempts: root.TryGetProperty("speciation_commit_attempts", out var attemptsNode) ? attemptsNode.GetUInt64() : 0UL,
                SpeciationCommitSuccesses: root.TryGetProperty("speciation_commit_successes", out var successNode) ? successNode.GetUInt64() : 0UL,
                LastFailure: root.TryGetProperty("last_failure", out var lastFailureNode) ? lastFailureNode.GetString() ?? string.Empty : string.Empty,
                LastSeed: root.TryGetProperty("last_seed", out var lastSeedNode) ? lastSeedNode.GetUInt64() : 0UL);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private readonly record struct DivergenceSnapshot(string Label);

    private readonly record struct PopulationChartSnapshot(
        string RangeLabel,
        string MetricLabel,
        string YTopLabel,
        string YMidLabel,
        string YBottomLabel,
        int LegendColumns,
        IReadOnlyList<SpeciationLineChartSeriesItem> Series,
        IReadOnlyList<SpeciationChartLegendItem> Legend);

    private readonly record struct FlowChartSnapshot(
        string RangeLabel,
        string StartEpochLabel,
        string EndEpochLabel,
        int LegendColumns,
        IReadOnlyList<SpeciationFlowChartAreaItem> Areas,
        IReadOnlyList<SpeciationChartLegendItem> Legend);

    private readonly record struct EpochPopulationRow(
        long EpochId,
        Dictionary<string, int> Counts,
        int TotalCount);

    private readonly record struct SpeciesPopulationMeta(
        string SpeciesId,
        string DisplayName,
        int TotalCount);

    private readonly record struct EvolutionSimStatusSnapshot(
        string SessionId,
        bool Running,
        bool Final,
        ulong Iterations,
        int ParentPoolSize,
        ulong CompatibilityChecks,
        ulong CompatiblePairs,
        ulong ReproductionCalls,
        ulong ReproductionFailures,
        ulong ReproductionRunsObserved,
        ulong ReproductionRunsWithMutations,
        ulong ReproductionMutationEvents,
        ulong SimilaritySamples,
        double? MinSimilarityObserved,
        double? MaxSimilarityObserved,
        ulong ChildrenAddedToPool,
        ulong SpeciationCommitAttempts,
        ulong SpeciationCommitSuccesses,
        string LastFailure,
        ulong LastSeed);

    private enum SimulatorParentFileKind
    {
        ParentAOverride,
        ParentBOverride
    }
}

public sealed record SpeciationSpeciesCountItem(
    string SpeciesId,
    string SpeciesDisplayName,
    int Count,
    string PercentLabel,
    string BarLabel);

public sealed record SpeciationHistoryItem(
    long EpochId,
    string BrainId,
    string SpeciesId,
    string SpeciesDisplayName,
    string DecisionReason,
    string AssignedAt);

public sealed record SpeciationEpochSummaryItem(
    long EpochId,
    int MembershipCount,
    int SpeciesCount,
    string FirstAssigned,
    string LastAssigned);

public sealed record SpeciationLineChartSeriesItem(
    string SpeciesId,
    string Label,
    string Stroke,
    string PathData,
    string LatestCountLabel);

public sealed record SpeciationFlowChartAreaItem(
    string SpeciesId,
    string Label,
    string Fill,
    string Stroke,
    string PathData,
    string LatestShareLabel);

public sealed record SpeciationChartLegendItem(
    string Label,
    string Color,
    string ValueLabel);

public sealed record SpeciationSimulatorBrainOption(Guid BrainId, string Label)
{
    public string BrainIdLabel => BrainId.ToString("D");
}
