using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
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
    private const int PopulationChartTopSpeciesLimit = 12;
    private const int FlowChartTopSpeciesLimit = 8;
    private const int SplitProximityTopSpeciesLimit = 8;
    private const uint DefaultHistoryLimit = 100u;
    private const uint DefaultChartHistoryLimit = 2048u;
    private const uint DefaultCladogramHistoryLimit = 8192u;
    private const uint DefaultVisibleChartWindow = 0u;
    private static readonly TimeSpan MembershipRefreshCadence = TimeSpan.FromSeconds(6);
    private static readonly IReadOnlyList<string> SimRunPressureModeOptions = ["divergence", "neutral", "stability"];
    private static readonly IReadOnlyList<string> SimParentSelectionBiasModeOptions = ["divergence", "neutral", "stability"];
    private static readonly HashSet<string> SpeciationAutoRefreshTriggerProperties =
    [
        nameof(ConnectionViewModel.SettingsConnected),
        nameof(ConnectionViewModel.SpeciationDiscoverable)
    ];
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
    private readonly List<SpeciationSimulatorSeedParentItem> _simAdditionalSeedParents = [];
    private CancellationTokenSource? _simPollCts;
    private CancellationTokenSource? _liveChartsPollCts;
    private string? _simStdoutLogPath;
    private long _simStdoutLogPosition;
    private string? _simLastStatusLine;
    private int _autoRefreshInFlight;
    private DateTimeOffset _lastMembershipRefreshAt = DateTimeOffset.MinValue;
    private uint? _lastPersistedHistoryLimit;

    private string _status = "Idle";
    private string _serviceSummary = "Service status not loaded.";
    private string _configStatus = "Settings-backed draft pending.";
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
    private string _currentEpochMaxDivergenceLabel = "Max within-species divergence (current epoch): (n/a)";
    private string _currentEpochSplitProximityLabel = "Split proximity (current epoch): (n/a)";
    private bool _configEnabled = true;
    private string _policyVersion = "default";
    private string _defaultSpeciesId = "species.default";
    private string _defaultSpeciesDisplayName = "Default species";
    private string _startupReconcileReason = "startup_reconcile";
    private string _lineageMatchThreshold = "0.92";
    private string _lineageSplitThreshold = "0.88";
    private string _parentConsensusThreshold = "0.70";
    private string _hysteresisMargin = "0.04";
    private string _lineageSplitGuardMargin = "0.02";
    private string _lineageMinParentMembershipsBeforeSplit = "1";
    private string _lineageRealignParentMembershipWindow = "3";
    private string _lineageRealignMatchMargin = "0.05";
    private string _lineageHindsightReassignCommitWindow = "6";
    private string _lineageHindsightSimilarityMargin = "0.015";
    private bool _createDerivedSpecies = true;
    private string _derivedSpeciesPrefix = "branch";
    private bool _startNewEpochConfirmPending;
    private bool _clearAllHistoryConfirmPending;
    private bool _deleteEpochConfirmPending;
    private long? _deleteEpochConfirmTarget;
    private string _deleteEpochText = string.Empty;
    private string _epochFilterText = string.Empty;
    private string _historyLimitText = DefaultHistoryLimit.ToString(CultureInfo.InvariantCulture);
    private string _chartWindowText = DefaultVisibleChartWindow.ToString(CultureInfo.InvariantCulture);
    private string _simParentAOverrideFilePath = string.Empty;
    private string _simParentBOverrideFilePath = string.Empty;
    private SpeciationSimulatorBrainOption? _simSelectedParentABrain;
    private SpeciationSimulatorBrainOption? _simSelectedParentBBrain;
    private SpeciationSimulatorBrainOption? _simExtraParentCandidateBrain;
    private string _simBindHost = "127.0.0.1";
    private string _simPortText = "12074";
    private string _simSeedText = "12345";
    private string _simIntervalMsText = "100";
    private string _simStatusSecondsText = "2";
    private string _simTimeoutSecondsText = "10";
    private string _simMaxIterationsText = "0";
    private string _simMaxParentPoolText = "512";
    private string _simMinRunsText = "2";
    private string _simMaxRunsText = "12";
    private string _simGammaText = "1";
    private string _simRunPressureMode = "divergence";
    private string _simParentSelectionBias = "divergence";
    private bool _simCommitToSpeciation = true;
    private bool _simSpawnChildren;
    private bool _liveChartsEnabled;
    private string _liveChartsIntervalSecondsText = DefaultLiveChartIntervalSeconds.ToString(CultureInfo.InvariantCulture);
    private string _liveChartsStatus = "Auto updates pending.";
    private string _populationChartRangeLabel = "Epochs: (no data)";
    private string _populationChartMetricLabel = "Population count by species (log10(1+count) y-axis).";
    private string _populationChartYAxisTopLabel = "0";
    private string _populationChartYAxisMidLabel = "0";
    private string _populationChartYAxisBottomLabel = "0";
    private int _populationChartLegendColumns = 2;
    private string _flowChartRangeLabel = "Epochs: (no data)";
    private string _flowChartStartEpochLabel = "(n/a)";
    private string _flowChartMidEpochLabel = "(n/a)";
    private string _flowChartEndEpochLabel = "(n/a)";
    private int _flowChartLegendColumns = 2;
    private string _splitProximityChartRangeLabel = "Epochs: (no data)";
    private string _splitProximityChartMetricLabel = "Min lineage similarity minus effective split threshold per species (signed log10(1+|delta|) y-axis; <=0 means split-trigger zone).";
    private string _splitProximityChartYAxisTopLabel = "0";
    private string _splitProximityChartYAxisMidLabel = "0";
    private string _splitProximityChartYAxisBottomLabel = "0";
    private int _splitProximityChartLegendColumns = 2;
    private string _cladogramRangeLabel = "Cladogram: (no data)";
    private string _cladogramMetricLabel = "Parent -> child lineage edges inferred from divergence decisions.";
    private string _cladogramKeyLabel = "Key: color strip = species color; each node shows species name + id with membership and direct-derived counts; root badges mark inferred root lineages. New species auto-expand their branch.";

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
        _connections.PropertyChanged += OnConnectionsPropertyChanged;

        SpeciesCounts = new ObservableCollection<SpeciationSpeciesCountItem>();
        EpochSummaries = new ObservableCollection<SpeciationEpochSummaryItem>();
        SimActiveBrains = new ObservableCollection<SpeciationSimulatorBrainOption>();
        SimSeedParents = new ObservableCollection<SpeciationSimulatorSeedParentItem>();
        PopulationChartSeries = new ObservableCollection<SpeciationLineChartSeriesItem>();
        PopulationChartLegend = new ObservableCollection<SpeciationChartLegendItem>();
        FlowChartAreas = new ObservableCollection<SpeciationFlowChartAreaItem>();
        FlowChartLegend = new ObservableCollection<SpeciationChartLegendItem>();
        SplitProximityChartSeries = new ObservableCollection<SpeciationLineChartSeriesItem>();
        SplitProximityChartLegend = new ObservableCollection<SpeciationChartLegendItem>();
        CladogramItems = new ObservableCollection<SpeciationCladogramItem>();

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
        AddSimSeedParentCommand = new RelayCommand(AddSimulatorSeedParentFromCandidate);
        AddSimSeedParentsFromFileCommand = new AsyncRelayCommand(AddSimulatorSeedParentsFromFileAsync);
        ClearSimSeedParentsCommand = new RelayCommand(ClearSimulatorSeedParents);

        _liveChartsEnabled = _enableLiveChartsAutoRefresh;
        _liveChartsStatus = _liveChartsEnabled
            ? $"Auto updates active ({DefaultLiveChartIntervalSeconds}s)."
            : "Auto updates disabled for this session.";
        if (_liveChartsEnabled)
        {
            StartLiveChartsPolling();
        }
    }

    public ConnectionViewModel Connections => _connections;

    public ObservableCollection<SpeciationSpeciesCountItem> SpeciesCounts { get; }
    public ObservableCollection<SpeciationEpochSummaryItem> EpochSummaries { get; }
    public ObservableCollection<SpeciationSimulatorBrainOption> SimActiveBrains { get; }
    public ObservableCollection<SpeciationSimulatorSeedParentItem> SimSeedParents { get; }
    public ObservableCollection<SpeciationLineChartSeriesItem> PopulationChartSeries { get; }
    public ObservableCollection<SpeciationChartLegendItem> PopulationChartLegend { get; }
    public ObservableCollection<SpeciationFlowChartAreaItem> FlowChartAreas { get; }
    public ObservableCollection<SpeciationChartLegendItem> FlowChartLegend { get; }
    public ObservableCollection<SpeciationLineChartSeriesItem> SplitProximityChartSeries { get; }
    public ObservableCollection<SpeciationChartLegendItem> SplitProximityChartLegend { get; }
    public ObservableCollection<SpeciationCladogramItem> CladogramItems { get; }

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
    public RelayCommand AddSimSeedParentCommand { get; }
    public AsyncRelayCommand AddSimSeedParentsFromFileCommand { get; }
    public RelayCommand ClearSimSeedParentsCommand { get; }

    public IReadOnlyList<string> SimRunPressureModes => SimRunPressureModeOptions;
    public IReadOnlyList<string> SimParentSelectionBiasModes => SimParentSelectionBiasModeOptions;

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

    public string CurrentEpochSplitProximityLabel
    {
        get => _currentEpochSplitProximityLabel;
        set => SetProperty(ref _currentEpochSplitProximityLabel, value);
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

    public string LineageSplitGuardMargin
    {
        get => _lineageSplitGuardMargin;
        set => SetProperty(ref _lineageSplitGuardMargin, value);
    }

    public string LineageMinParentMembershipsBeforeSplit
    {
        get => _lineageMinParentMembershipsBeforeSplit;
        set => SetProperty(ref _lineageMinParentMembershipsBeforeSplit, value);
    }

    public string LineageRealignParentMembershipWindow
    {
        get => _lineageRealignParentMembershipWindow;
        set => SetProperty(ref _lineageRealignParentMembershipWindow, value);
    }

    public string LineageRealignMatchMargin
    {
        get => _lineageRealignMatchMargin;
        set => SetProperty(ref _lineageRealignMatchMargin, value);
    }

    public string LineageHindsightReassignCommitWindow
    {
        get => _lineageHindsightReassignCommitWindow;
        set => SetProperty(ref _lineageHindsightReassignCommitWindow, value);
    }

    public string LineageHindsightSimilarityMargin
    {
        get => _lineageHindsightSimilarityMargin;
        set => SetProperty(ref _lineageHindsightSimilarityMargin, value);
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

    public string ChartWindowText
    {
        get => _chartWindowText;
        set => SetProperty(ref _chartWindowText, value);
    }

    public string StartNewEpochLabel => _startNewEpochConfirmPending ? "Confirm New Epoch" : "Start New Epoch";
    public string ClearAllHistoryLabel => _clearAllHistoryConfirmPending ? "Confirm Delete All Epochs" : "Delete All Epochs";
    public string DeleteEpochLabel => _deleteEpochConfirmPending ? "Confirm Delete Epoch" : "Delete Epoch";

    public SpeciationSimulatorBrainOption? SimSelectedParentABrain
    {
        get => _simSelectedParentABrain;
        set
        {
            if (SetProperty(ref _simSelectedParentABrain, value))
            {
                RefreshEffectiveSimulatorSeedParents();
            }
        }
    }

    public SpeciationSimulatorBrainOption? SimSelectedParentBBrain
    {
        get => _simSelectedParentBBrain;
        set
        {
            if (SetProperty(ref _simSelectedParentBBrain, value))
            {
                RefreshEffectiveSimulatorSeedParents();
            }
        }
    }

    public SpeciationSimulatorBrainOption? SimExtraParentCandidateBrain
    {
        get => _simExtraParentCandidateBrain;
        set => SetProperty(ref _simExtraParentCandidateBrain, value);
    }

    public string SimParentAOverrideFilePath
    {
        get => _simParentAOverrideFilePath;
        set
        {
            if (SetProperty(ref _simParentAOverrideFilePath, value))
            {
                OnPropertyChanged(nameof(SimParentAOverrideFilePathDisplay));
                RefreshEffectiveSimulatorSeedParents();
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
                RefreshEffectiveSimulatorSeedParents();
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

    public string SimRunPressureMode
    {
        get => _simRunPressureMode;
        set => SetProperty(ref _simRunPressureMode, value);
    }

    public string SimParentSelectionBias
    {
        get => _simParentSelectionBias;
        set => SetProperty(ref _simParentSelectionBias, value);
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

    public string SimSeedParentsSummary
        => SimSeedParents.Count == 0
            ? "(none)"
            : $"{SimSeedParents.Count} total";

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
                LiveChartsStatus = $"Auto updates active ({ParseLiveChartIntervalSecondsOrDefault()}s).";
                StartLiveChartsPolling();
            }
            else
            {
                StopLiveChartsPolling();
                LiveChartsStatus = "Auto updates paused.";
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

    public string FlowChartMidEpochLabel
    {
        get => _flowChartMidEpochLabel;
        set => SetProperty(ref _flowChartMidEpochLabel, value);
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

    public string SplitProximityChartRangeLabel
    {
        get => _splitProximityChartRangeLabel;
        set => SetProperty(ref _splitProximityChartRangeLabel, value);
    }

    public string SplitProximityChartMetricLabel
    {
        get => _splitProximityChartMetricLabel;
        set => SetProperty(ref _splitProximityChartMetricLabel, value);
    }

    public string SplitProximityChartYAxisTopLabel
    {
        get => _splitProximityChartYAxisTopLabel;
        set => SetProperty(ref _splitProximityChartYAxisTopLabel, value);
    }

    public string SplitProximityChartYAxisMidLabel
    {
        get => _splitProximityChartYAxisMidLabel;
        set => SetProperty(ref _splitProximityChartYAxisMidLabel, value);
    }

    public string SplitProximityChartYAxisBottomLabel
    {
        get => _splitProximityChartYAxisBottomLabel;
        set => SetProperty(ref _splitProximityChartYAxisBottomLabel, value);
    }

    public int SplitProximityChartLegendColumns
    {
        get => _splitProximityChartLegendColumns;
        set => SetProperty(ref _splitProximityChartLegendColumns, value);
    }

    public string CladogramRangeLabel
    {
        get => _cladogramRangeLabel;
        set => SetProperty(ref _cladogramRangeLabel, value);
    }

    public string CladogramMetricLabel
    {
        get => _cladogramMetricLabel;
        set => SetProperty(ref _cladogramMetricLabel, value);
    }

    public string CladogramKeyLabel
    {
        get => _cladogramKeyLabel;
        set => SetProperty(ref _cladogramKeyLabel, value);
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
        var selectedExtraCandidateId = SimExtraParentCandidateBrain?.BrainId;

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

            SimExtraParentCandidateBrain = selectedExtraCandidateId.HasValue
                ? SimActiveBrains.FirstOrDefault(entry => entry.BrainId == selectedExtraCandidateId.Value)
                : SimExtraParentCandidateBrain;
            if (SimExtraParentCandidateBrain is null && SimActiveBrains.Count > 0)
            {
                SimExtraParentCandidateBrain = SimActiveBrains[0];
            }
            RefreshEffectiveSimulatorSeedParents();
        });
    }

    public bool ApplySetting(SettingItem item)
    {
        if (item is null || string.IsNullOrWhiteSpace(item.Key))
        {
            return false;
        }

        var key = item.Key.Trim();
        var value = item.Value?.Trim() ?? string.Empty;
        bool Applied()
        {
            UpdateSettingsBackedConfigStatus();
            return true;
        }

        if (string.Equals(key, SpeciationSettingsKeys.ConfigEnabledKey, StringComparison.OrdinalIgnoreCase))
        {
            ConfigEnabled = ParseBool(value, ConfigEnabled);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.PolicyVersionKey, StringComparison.OrdinalIgnoreCase))
        {
            PolicyVersion = string.IsNullOrWhiteSpace(value) ? PolicyVersion : value;
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.DefaultSpeciesIdKey, StringComparison.OrdinalIgnoreCase))
        {
            DefaultSpeciesId = string.IsNullOrWhiteSpace(value) ? DefaultSpeciesId : value;
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.DefaultSpeciesDisplayNameKey, StringComparison.OrdinalIgnoreCase))
        {
            DefaultSpeciesDisplayName = string.IsNullOrWhiteSpace(value) ? DefaultSpeciesDisplayName : value;
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.StartupReconcileReasonKey, StringComparison.OrdinalIgnoreCase))
        {
            StartupReconcileReason = string.IsNullOrWhiteSpace(value) ? StartupReconcileReason : value;
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageMatchThresholdKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageMatchThreshold = ParseDouble(value, 0.92d).ToString("0.###", CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageSplitThresholdKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageSplitThreshold = ParseDouble(value, 0.88d).ToString("0.###", CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.ParentConsensusThresholdKey, StringComparison.OrdinalIgnoreCase))
        {
            ParentConsensusThreshold = ParseDouble(value, 0.70d).ToString("0.###", CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageHysteresisMarginKey, StringComparison.OrdinalIgnoreCase))
        {
            HysteresisMargin = ParseDouble(value, 0.04d).ToString("0.###", CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageSplitGuardMarginKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageSplitGuardMargin = ParseDouble(value, 0.02d).ToString("0.###", CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageMinParentMembershipsBeforeSplitKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageMinParentMembershipsBeforeSplit = Math.Max(1, ParseInt(value, 1)).ToString(CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageRealignParentMembershipWindowKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageRealignParentMembershipWindow = Math.Max(0, ParseInt(value, 3)).ToString(CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageRealignMatchMarginKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageRealignMatchMargin = ParseDouble(value, 0.05d).ToString("0.###", CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageHindsightReassignCommitWindowKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageHindsightReassignCommitWindow = Math.Max(0, ParseInt(value, 6)).ToString(CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageHindsightSimilarityMarginKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageHindsightSimilarityMargin = ParseDouble(value, 0.015d).ToString("0.###", CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.CreateDerivedSpeciesOnDivergenceKey, StringComparison.OrdinalIgnoreCase))
        {
            CreateDerivedSpecies = ParseBool(value, CreateDerivedSpecies);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.DerivedSpeciesPrefixKey, StringComparison.OrdinalIgnoreCase))
        {
            DerivedSpeciesPrefix = string.IsNullOrWhiteSpace(value) ? DerivedSpeciesPrefix : value;
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.HistoryLimitKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = Math.Max(1u, ParseUInt(value, DefaultHistoryLimit));
            HistoryLimitText = parsed.ToString(CultureInfo.InvariantCulture);
            _lastPersistedHistoryLimit = parsed;
            return Applied();
        }

        return false;
    }

    private void UpdateSettingsBackedConfigStatus()
    {
        if (!string.Equals(ConfigStatus, "Settings-backed draft pending.", StringComparison.Ordinal)
            && !string.Equals(ConfigStatus, "Config not loaded.", StringComparison.Ordinal)
            && !string.Equals(ConfigStatus, "Settings-backed draft active.", StringComparison.Ordinal)
            && !string.Equals(ConfigStatus, "Config loaded.", StringComparison.Ordinal))
        {
            return;
        }

        ConfigStatus = "Settings-backed draft active.";
    }

    public async ValueTask DisposeAsync()
    {
        _connections.PropertyChanged -= OnConnectionsPropertyChanged;
        StopLiveChartsPolling();
        _simPollCts?.Cancel();
        await StopSimulatorAsync().ConfigureAwait(false);
    }

    private async Task RefreshAllAsync()
    {
        await RefreshPaneDataAsync(includeMemberships: true).ConfigureAwait(false);
        await RefreshSimulatorStatusAsync().ConfigureAwait(false);
    }

    private void OnConnectionsPropertyChanged(object? sender, PropertyChangedEventArgs args)
    {
        if (string.IsNullOrWhiteSpace(args.PropertyName)
            || !SpeciationAutoRefreshTriggerProperties.Contains(args.PropertyName))
        {
            return;
        }

        if (!Connections.SettingsConnected || !Connections.SpeciationDiscoverable)
        {
            return;
        }

        if (Interlocked.Exchange(ref _autoRefreshInFlight, 1) != 0)
        {
            return;
        }

        _ = RefreshPaneStateFromConnectionsAsync();
    }

    private async Task RefreshPaneStateFromConnectionsAsync()
    {
        try
        {
            await RefreshPaneDataAsync(includeMemberships: true).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Status = $"Speciation refresh failed: {ex.GetBaseException().Message}";
        }
        finally
        {
            Interlocked.Exchange(ref _autoRefreshInFlight, 0);
        }
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

    private async Task RefreshPaneDataAsync(bool includeMemberships)
    {
        await RefreshStatusAsync().ConfigureAwait(false);
        if (includeMemberships)
        {
            await RefreshMembershipsAsync().ConfigureAwait(false);
        }

        await RefreshHistoryAsync().ConfigureAwait(false);
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

            if (!Connections.SettingsConnected || !Connections.SpeciationDiscoverable)
            {
                LiveChartsStatus = "Waiting for Settings/speciation discovery.";
                continue;
            }

            var includeMemberships = DateTimeOffset.UtcNow - _lastMembershipRefreshAt >= MembershipRefreshCadence;
            await RefreshPaneDataAsync(includeMemberships).ConfigureAwait(false);
            if (_evolutionRunner.IsRunning || !string.IsNullOrWhiteSpace(_simStdoutLogPath))
            {
                await RefreshSimulatorStatusAsync().ConfigureAwait(false);
            }

            LiveChartsStatus = includeMemberships
                ? $"Auto updates active ({intervalSeconds}s, counts every ~{MembershipRefreshCadence.TotalSeconds:0}s)."
                : $"Auto updates active ({intervalSeconds}s).";
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
        await PersistSpeciationSettingsAsync().ConfigureAwait(false);

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
        await PersistSpeciationSettingsAsync().ConfigureAwait(false);

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
            HistoryStatus = "Click Delete All Epochs again to confirm. This removes all epoch history and starts a new epoch.";
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
            CurrentEpochMaxDivergenceLabel = $"Max within-species divergence (epoch {CurrentEpochLabel}): (n/a)";
            CurrentEpochSplitProximityLabel = $"Split proximity (epoch {CurrentEpochLabel}): (n/a)";
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
                SpeciesName = BuildCompactSpeciesName(m.SpeciesDisplayName, m.SpeciesId)
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
            _lastMembershipRefreshAt = DateTimeOffset.UtcNow;
        });
    }

    private async Task RefreshHistoryAsync()
    {
        var historyLimit = Math.Max(1u, ParseUInt(HistoryLimitText, DefaultHistoryLimit));
        HistoryLimitText = historyLimit.ToString(CultureInfo.InvariantCulture);
        if (_lastPersistedHistoryLimit != historyLimit)
        {
            await _client.SetSettingAsync(
                    SpeciationSettingsKeys.HistoryLimitKey,
                    historyLimit.ToString(CultureInfo.InvariantCulture))
                .ConfigureAwait(false);
            _lastPersistedHistoryLimit = historyLimit;
        }
        var chartWindow = ParseUInt(ChartWindowText, DefaultVisibleChartWindow);
        ChartWindowText = chartWindow.ToString(CultureInfo.InvariantCulture);
        var chartHistoryLimit = chartWindow == 0u ? DefaultChartHistoryLimit : chartWindow;
        var historyPageSize = Math.Max(historyLimit, Math.Max(chartHistoryLimit, DefaultCladogramHistoryLimit));
        var epochFilter = ResolveEpochFilter();

        var chartResponse = await LoadCompleteSpeciationHistoryAsync(
                epochFilter,
                brainId: null,
                historyPageSize)
            .ConfigureAwait(false);
        if (chartResponse.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(chartResponse.FailureReason, chartResponse.FailureDetail);
            HistoryStatus = $"History load failed: {reason}";
            Status = HistoryStatus;
            return;
        }

        var chartHistory = TrimHistoryToChartWindow(chartResponse.History, chartWindow);
        var populationFrame = BuildEpochPopulationFrame(chartHistory);
        var epochSummaries = BuildEpochSummaries(chartHistory);
        var populationSnapshot = BuildPopulationChartSnapshot(populationFrame.EpochRows, populationFrame.SpeciesOrder);
        var flowSnapshot = BuildFlowChartSnapshot(populationFrame.EpochRows, populationFrame.SpeciesOrder);
        var divergenceSnapshot = BuildCurrentEpochDivergenceSnapshot(chartHistory, CurrentEpochId);
        var splitProximitySnapshot = BuildSplitProximityChartSnapshot(
            chartHistory,
            CurrentEpochId,
            ParseDouble(LineageSplitThreshold, 0.88d),
            ParseDouble(LineageSplitGuardMargin, 0.02d));
        var cladogramSourceHistory = chartResponse.History;
        var cladogramSnapshot = BuildCladogramSnapshot(cladogramSourceHistory);

        _dispatcher.Post(() =>
        {
            EpochSummaries.Clear();
            foreach (var row in epochSummaries)
            {
                EpochSummaries.Add(row);
            }

            ApplyPopulationChartSnapshot(populationSnapshot);
            ApplyFlowChartSnapshot(flowSnapshot);
            CurrentEpochMaxDivergenceLabel = divergenceSnapshot.Label;
            CurrentEpochSplitProximityLabel = splitProximitySnapshot.CurrentEpochSummaryLabel;
            ApplySplitProximityChartSnapshot(splitProximitySnapshot);
            ApplyCladogramSnapshot(cladogramSnapshot);
            HistoryStatus =
                $"Speciation data loaded: fetched={chartResponse.History.Count} total={chartResponse.TotalRecords}";
            Status = HistoryStatus;
        });
    }

    private async Task<SpeciationListHistoryResponse> LoadCompleteSpeciationHistoryAsync(
        long? epochId,
        Guid? brainId,
        uint pageSize)
    {
        var normalizedPageSize = Math.Max(1u, pageSize);
        var combined = new List<SpeciationMembershipRecord>();
        uint offset = 0;
        uint totalRecords = 0;

        while (true)
        {
            var response = await _client.ListSpeciationHistoryAsync(
                    epochId: epochId,
                    brainId: brainId,
                    limit: normalizedPageSize,
                    offset: offset)
                .ConfigureAwait(false);
            if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
            {
                return response;
            }

            totalRecords = response.TotalRecords;
            if (response.History.Count == 0)
            {
                break;
            }

            combined.AddRange(response.History);
            if ((uint)combined.Count >= totalRecords)
            {
                break;
            }

            var nextOffset = (uint)combined.Count;
            if (nextOffset <= offset)
            {
                break;
            }

            offset = nextOffset;
        }

        var combinedResponse = new SpeciationListHistoryResponse
        {
            FailureReason = SpeciationFailureReason.SpeciationFailureNone,
            FailureDetail = string.Empty,
            TotalRecords = totalRecords
        };
        combinedResponse.History.AddRange(combined);
        return combinedResponse;
    }

    private static IReadOnlyList<SpeciationMembershipRecord> TrimHistoryToChartWindow(
        IReadOnlyList<SpeciationMembershipRecord> history,
        uint chartWindow)
    {
        if (chartWindow == 0u || history.Count == 0 || history.Count <= chartWindow)
        {
            return history;
        }

        var skip = history.Count - (int)chartWindow;
        return history.Skip(skip).ToArray();
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

        if (!TryResolveSimulatorParentPool(out var parentPool, out var parentError))
        {
            SimulatorStatus = parentError;
            Status = SimulatorStatus;
            return;
        }

        var parentDefinitions = await TryResolveSimulatorParentArtifactsAsync(parentPool).ConfigureAwait(false);
        if (!parentDefinitions.Success || parentDefinitions.ParentRefs.Count < 2)
        {
            SimulatorStatus = parentDefinitions.Error;
            Status = SimulatorStatus;
            return;
        }

        var args = BuildEvolutionSimArgs(ioPort, simPort, parentDefinitions.ParentRefs);
        if (string.IsNullOrWhiteSpace(args))
        {
            SimulatorStatus = "Simulator requires at least two usable parent artifact references.";
            Status = SimulatorStatus;
            return;
        }

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
        _simStdoutLogPosition = 0;
        _simLastStatusLine = null;
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
        _simStdoutLogPosition = 0;
        _simLastStatusLine = null;
        _simStdoutLogPath = null;

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

        var lastLine = ReadLatestNonEmptyLine(_simStdoutLogPath, ref _simStdoutLogPosition, ref _simLastStatusLine);
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

            var overallSimilarityLabel = FormatSimilarityRange(
                snapshot.SimilaritySamples,
                snapshot.MinSimilarityObserved,
                snapshot.MaxSimilarityObserved);
            var assessmentSimilarityLabel = FormatSimilarityRange(
                snapshot.AssessmentSimilaritySamples,
                snapshot.MinAssessmentSimilarityObserved,
                snapshot.MaxAssessmentSimilarityObserved);
            var reproductionSimilarityLabel = FormatSimilarityRange(
                snapshot.ReproductionSimilaritySamples,
                snapshot.MinReproductionSimilarityObserved,
                snapshot.MaxReproductionSimilarityObserved);
            var commitSimilarityLabel = FormatSimilarityRange(
                snapshot.SpeciationCommitSimilaritySamples,
                snapshot.MinSpeciationCommitSimilarityObserved,
                snapshot.MaxSpeciationCommitSimilarityObserved);

            SimulatorProgress =
                $"running={snapshot.Running} final={snapshot.Final} iter={snapshot.Iterations} parent_pool_size={snapshot.ParentPoolSize}";
            SimulatorDetailedStats =
                $"compat={snapshot.CompatiblePairs}/{snapshot.CompatibilityChecks} " +
                $"repro_calls={snapshot.ReproductionCalls} repro_fail={snapshot.ReproductionFailures} " +
                $"parent_pool_size={snapshot.ParentPoolSize} children_added_to_pool={childrenLabel} " +
                $"runs={snapshot.ReproductionRunsObserved} runs_mutated={snapshot.ReproductionRunsWithMutations} mutation_events={snapshot.ReproductionMutationEvents} " +
                $"sim_overall={overallSimilarityLabel} sim_assess={assessmentSimilarityLabel} sim_repro={reproductionSimilarityLabel} sim_commit={commitSimilarityLabel} " +
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
            if (!_evolutionRunner.IsRunning)
            {
                break;
            }
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
            LineageSplitGuardMargin = snapshot.LineageSplitGuardMargin.ToString("0.###", CultureInfo.InvariantCulture);
            LineageMinParentMembershipsBeforeSplit = snapshot.LineageMinParentMembershipsBeforeSplit.ToString(CultureInfo.InvariantCulture);
            LineageRealignParentMembershipWindow = snapshot.LineageRealignParentMembershipWindow.ToString(CultureInfo.InvariantCulture);
            LineageRealignMatchMargin = snapshot.LineageRealignMatchMargin.ToString("0.###", CultureInfo.InvariantCulture);
            LineageHindsightReassignCommitWindow = snapshot.LineageHindsightReassignCommitWindow.ToString(CultureInfo.InvariantCulture);
            LineageHindsightSimilarityMargin = snapshot.LineageHindsightSimilarityMargin.ToString("0.###", CultureInfo.InvariantCulture);
            CreateDerivedSpecies = snapshot.CreateDerivedSpecies;
            DerivedSpeciesPrefix = snapshot.DerivedSpeciesPrefix;
        });
    }

    private SpeciationRuntimeConfig BuildRuntimeConfigFromDraft()
    {
        var matchThreshold = Clamp01(ParseDouble(LineageMatchThreshold, 0.92));
        var splitThreshold = Clamp01(ParseDouble(LineageSplitThreshold, Math.Max(0d, matchThreshold - 0.04d)));
        if (splitThreshold > matchThreshold)
        {
            splitThreshold = matchThreshold;
        }

        var parentConsensus = Clamp01(ParseDouble(ParentConsensusThreshold, 0.70));
        var hysteresisMargin = Math.Max(0d, ParseDouble(HysteresisMargin, Math.Max(0d, matchThreshold - splitThreshold)));
        var splitGuardMargin = Clamp01(ParseDouble(LineageSplitGuardMargin, 0.02d));
        var minParentMembershipsBeforeSplit = Math.Max(1, ParseInt(LineageMinParentMembershipsBeforeSplit, 1));
        var realignParentMembershipWindow = Math.Max(0, ParseInt(LineageRealignParentMembershipWindow, 3));
        var realignMatchMargin = Clamp01(ParseDouble(LineageRealignMatchMargin, 0.05d));
        var hindsightReassignCommitWindow = Math.Max(0, ParseInt(LineageHindsightReassignCommitWindow, 6));
        var hindsightSimilarityMargin = Clamp01(ParseDouble(LineageHindsightSimilarityMargin, 0.015d));
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
                ["lineage_split_guard_margin"] = splitGuardMargin,
                ["lineage_min_parent_memberships_before_split"] = minParentMembershipsBeforeSplit,
                ["lineage_realign_parent_membership_window"] = realignParentMembershipWindow,
                ["lineage_realign_match_margin"] = realignMatchMargin,
                ["lineage_hindsight_reassign_commit_window"] = hindsightReassignCommitWindow,
                ["lineage_hindsight_similarity_margin"] = hindsightSimilarityMargin,
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

    private (
        bool Enabled,
        double MatchThreshold,
        double SplitThreshold,
        double ParentConsensusThreshold,
        double HysteresisMargin,
        double LineageSplitGuardMargin,
        int LineageMinParentMembershipsBeforeSplit,
        int LineageRealignParentMembershipWindow,
        double LineageRealignMatchMargin,
        int LineageHindsightReassignCommitWindow,
        double LineageHindsightSimilarityMargin,
        bool CreateDerivedSpecies,
        string DerivedSpeciesPrefix) ParseSnapshot(string snapshotJson)
    {
        var defaults = (
            Enabled: true,
            MatchThreshold: 0.92d,
            SplitThreshold: 0.88d,
            ParentConsensusThreshold: 0.70d,
            HysteresisMargin: 0.04d,
            LineageSplitGuardMargin: 0.02d,
            LineageMinParentMembershipsBeforeSplit: 1,
            LineageRealignParentMembershipWindow: 3,
            LineageRealignMatchMargin: 0.05d,
            LineageHindsightReassignCommitWindow: 6,
            LineageHindsightSimilarityMargin: 0.015d,
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
            var splitGuardMargin = Clamp01(TryReadDouble(policy, "lineage_split_guard_margin", "lineageSplitGuardMargin") ?? defaults.LineageSplitGuardMargin);
            var minParentMembershipsBeforeSplit = Math.Max(
                1,
                RoundToNonNegativeInt(
                    TryReadDouble(policy, "lineage_min_parent_memberships_before_split", "lineageMinParentMembershipsBeforeSplit")
                    ?? defaults.LineageMinParentMembershipsBeforeSplit,
                    defaults.LineageMinParentMembershipsBeforeSplit));
            var realignParentMembershipWindow = Math.Max(
                0,
                RoundToNonNegativeInt(
                    TryReadDouble(policy, "lineage_realign_parent_membership_window", "lineageRealignParentMembershipWindow")
                    ?? defaults.LineageRealignParentMembershipWindow,
                    defaults.LineageRealignParentMembershipWindow));
            var realignMatchMargin = Clamp01(TryReadDouble(policy, "lineage_realign_match_margin", "lineageRealignMatchMargin") ?? defaults.LineageRealignMatchMargin);
            var hindsightReassignCommitWindow = Math.Max(
                0,
                RoundToNonNegativeInt(
                    TryReadDouble(policy, "lineage_hindsight_reassign_commit_window", "lineageHindsightReassignCommitWindow")
                    ?? defaults.LineageHindsightReassignCommitWindow,
                    defaults.LineageHindsightReassignCommitWindow));
            var hindsightSimilarityMargin = Clamp01(
                TryReadDouble(policy, "lineage_hindsight_similarity_margin", "lineageHindsightSimilarityMargin")
                ?? defaults.LineageHindsightSimilarityMargin);
            var createDerived = TryReadBool(policy, "create_derived_species_on_divergence", "createDerivedSpeciesOnDivergence")
                                ?? defaults.CreateDerivedSpecies;
            var prefix = TryReadString(policy, "derived_species_prefix", "derivedSpeciesPrefix")
                         ?? defaults.DerivedSpeciesPrefix;
            return (
                enabled,
                match,
                split,
                consensus,
                hysteresis,
                splitGuardMargin,
                minParentMembershipsBeforeSplit,
                realignParentMembershipWindow,
                realignMatchMargin,
                hindsightReassignCommitWindow,
                hindsightSimilarityMargin,
                createDerived,
                string.IsNullOrWhiteSpace(prefix) ? "branch" : prefix.Trim());
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

    private async Task PersistSpeciationSettingsAsync()
    {
        var updates = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SpeciationSettingsKeys.ConfigEnabledKey] = ConfigEnabled ? "true" : "false",
            [SpeciationSettingsKeys.PolicyVersionKey] = string.IsNullOrWhiteSpace(PolicyVersion) ? "default" : PolicyVersion.Trim(),
            [SpeciationSettingsKeys.DefaultSpeciesIdKey] = string.IsNullOrWhiteSpace(DefaultSpeciesId) ? "species.default" : DefaultSpeciesId.Trim(),
            [SpeciationSettingsKeys.DefaultSpeciesDisplayNameKey] = string.IsNullOrWhiteSpace(DefaultSpeciesDisplayName) ? "Default species" : DefaultSpeciesDisplayName.Trim(),
            [SpeciationSettingsKeys.StartupReconcileReasonKey] = string.IsNullOrWhiteSpace(StartupReconcileReason) ? "startup_reconcile" : StartupReconcileReason.Trim(),
            [SpeciationSettingsKeys.LineageMatchThresholdKey] = Clamp01(ParseDouble(LineageMatchThreshold, 0.92d)).ToString("0.###", CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageSplitThresholdKey] = Clamp01(ParseDouble(LineageSplitThreshold, 0.88d)).ToString("0.###", CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.ParentConsensusThresholdKey] = Clamp01(ParseDouble(ParentConsensusThreshold, 0.70d)).ToString("0.###", CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageHysteresisMarginKey] = Math.Max(0d, ParseDouble(HysteresisMargin, 0.04d)).ToString("0.###", CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageSplitGuardMarginKey] = Clamp01(ParseDouble(LineageSplitGuardMargin, 0.02d)).ToString("0.###", CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageMinParentMembershipsBeforeSplitKey] = Math.Max(1, ParseInt(LineageMinParentMembershipsBeforeSplit, 1)).ToString(CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageRealignParentMembershipWindowKey] = Math.Max(0, ParseInt(LineageRealignParentMembershipWindow, 3)).ToString(CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageRealignMatchMarginKey] = Clamp01(ParseDouble(LineageRealignMatchMargin, 0.05d)).ToString("0.###", CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageHindsightReassignCommitWindowKey] = Math.Max(0, ParseInt(LineageHindsightReassignCommitWindow, 6)).ToString(CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageHindsightSimilarityMarginKey] = Clamp01(ParseDouble(LineageHindsightSimilarityMargin, 0.015d)).ToString("0.###", CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.CreateDerivedSpeciesOnDivergenceKey] = CreateDerivedSpecies ? "true" : "false",
            [SpeciationSettingsKeys.DerivedSpeciesPrefixKey] = string.IsNullOrWhiteSpace(DerivedSpeciesPrefix) ? "branch" : DerivedSpeciesPrefix.Trim(),
            [SpeciationSettingsKeys.HistoryLimitKey] = Math.Max(1u, ParseUInt(HistoryLimitText, DefaultHistoryLimit)).ToString(CultureInfo.InvariantCulture)
        };

        foreach (var (key, value) in updates)
        {
            await _client.SetSettingAsync(key, value).ConfigureAwait(false);
        }
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

    private void AddSimulatorSeedParentFromCandidate()
    {
        if (SimExtraParentCandidateBrain is null || SimExtraParentCandidateBrain.BrainId == Guid.Empty)
        {
            SimulatorStatus = "Select an active brain to add as a seed parent.";
            Status = SimulatorStatus;
            return;
        }

        TryAddSimulatorSeedParent(
            SimExtraParentCandidateBrain.BrainId,
            SimExtraParentCandidateBrain.Label,
            source: "dropdown");
    }

    private async Task AddSimulatorSeedParentsFromFileAsync()
    {
        var file = await PickOpenFileAsync("Select simulator extra-parents file").ConfigureAwait(false);
        if (file is null)
        {
            return;
        }

        var path = FormatPath(file);
        IReadOnlyList<Guid> parentIds;
        try
        {
            parentIds = ParseBrainIdsFromFile(path, "--extra-parents-file");
        }
        catch (Exception ex)
        {
            SimulatorStatus = $"Extra parent file failed: {ex.GetBaseException().Message}";
            Status = SimulatorStatus;
            return;
        }

        if (parentIds.Count == 0)
        {
            SimulatorStatus = $"Extra parent file has no usable brain GUIDs: {path}";
            Status = SimulatorStatus;
            return;
        }

        var labelsByBrainId = SimActiveBrains
            .GroupBy(entry => entry.BrainId)
            .ToDictionary(group => group.Key, group => group.First().Label);
        var added = 0;
        foreach (var parentId in parentIds)
        {
            var label = labelsByBrainId.TryGetValue(parentId, out var activeLabel)
                ? activeLabel
                : parentId.ToString("D");
            if (TryAddSimulatorSeedParent(parentId, label, source: "file", updateStatus: false))
            {
                added++;
            }
        }

        SimulatorStatus = added > 0
            ? $"Added {added} seed parent(s) from file."
            : "No new seed parents were added from file (all duplicates).";
        Status = SimulatorStatus;
    }

    private void ClearSimulatorSeedParents()
    {
        if (_simAdditionalSeedParents.Count == 0)
        {
            return;
        }

        _simAdditionalSeedParents.Clear();
        RefreshEffectiveSimulatorSeedParents();
        SimulatorStatus = "Cleared extra seed parents.";
        Status = SimulatorStatus;
    }

    private bool TryAddSimulatorSeedParent(Guid brainId, string label, string source, bool updateStatus = true)
    {
        if (brainId == Guid.Empty)
        {
            return false;
        }

        if (SimSeedParents.Any(entry => entry.BrainId == brainId)
            || _simAdditionalSeedParents.Any(entry => entry.BrainId == brainId))
        {
            if (updateStatus)
            {
                SimulatorStatus = $"Seed parent already added: {brainId:D}";
                Status = SimulatorStatus;
            }

            return false;
        }

        _simAdditionalSeedParents.Add(new SpeciationSimulatorSeedParentItem(brainId, label, source));
        RefreshEffectiveSimulatorSeedParents();
        if (updateStatus)
        {
            SimulatorStatus = $"Added seed parent: {brainId:D}";
            Status = SimulatorStatus;
        }

        return true;
    }

    private bool TryResolveSimulatorParentPool(out List<Guid> parents, out string error)
    {
        parents = new List<Guid>(2 + _simAdditionalSeedParents.Count);
        var uniqueParents = new HashSet<Guid>();

        if (!TryResolveParentBrainId(
                selected: SimSelectedParentABrain,
                overrideFilePath: SimParentAOverrideFilePath,
                parentLabel: "A",
                out var parentA,
                out error))
        {
            return false;
        }
        uniqueParents.Add(parentA);
        parents.Add(parentA);

        if (!TryResolveParentBrainId(
                selected: SimSelectedParentBBrain,
                overrideFilePath: SimParentBOverrideFilePath,
                parentLabel: "B",
                out var parentB,
                out error))
        {
            return false;
        }
        if (uniqueParents.Add(parentB))
        {
            parents.Add(parentB);
        }

        foreach (var extraParent in _simAdditionalSeedParents)
        {
            if (uniqueParents.Add(extraParent.BrainId))
            {
                parents.Add(extraParent.BrainId);
            }
        }

        if (parents.Count < 2)
        {
            error = "Simulator requires at least two distinct brain parents.";
            return false;
        }

        error = string.Empty;
        return true;
    }

    private async Task<(bool Success, IReadOnlyList<ArtifactRef> ParentRefs, string Error)> TryResolveSimulatorParentArtifactsAsync(
        IReadOnlyList<Guid> parentIds)
    {
        var parentRefs = new List<ArtifactRef>(parentIds.Count);
        for (var index = 0; index < parentIds.Count; index++)
        {
            var parentLabel = $"#{index + 1}";
            var resolution = await ResolveSimulatorParentArtifactAsync(parentIds[index], parentLabel).ConfigureAwait(false);
            if (!resolution.Success || resolution.Artifact is null)
            {
                return (false, Array.Empty<ArtifactRef>(), resolution.Error);
            }

            parentRefs.Add(resolution.Artifact);
        }

        return (true, parentRefs, string.Empty);
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
        if (TryReadParentOverrideGuid(overrideFilePath, parentLabel, out brainId, out error))
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(overrideFilePath))
        {
            brainId = Guid.Empty;
            error = string.IsNullOrWhiteSpace(error)
                ? $"Parent {parentLabel} override file has no usable brain GUID: {overrideFilePath}"
                : error;
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

    private void RefreshEffectiveSimulatorSeedParents()
    {
        var items = new List<SpeciationSimulatorSeedParentItem>(2 + _simAdditionalSeedParents.Count);
        var seen = new HashSet<Guid>();

        AddEffectiveSimulatorSeedParent(
            items,
            seen,
            BuildSimulatorParentSlotItem(SimSelectedParentABrain, SimParentAOverrideFilePath, "A"));
        AddEffectiveSimulatorSeedParent(
            items,
            seen,
            BuildSimulatorParentSlotItem(SimSelectedParentBBrain, SimParentBOverrideFilePath, "B"));

        foreach (var extraParent in _simAdditionalSeedParents)
        {
            AddEffectiveSimulatorSeedParent(
                items,
                seen,
                extraParent with { Label = ResolveSimulatorSeedParentLabel(extraParent.BrainId, extraParent.Label) });
        }

        SimSeedParents.Clear();
        foreach (var item in items)
        {
            SimSeedParents.Add(item);
        }

        OnPropertyChanged(nameof(SimSeedParentsSummary));
    }

    private SpeciationSimulatorSeedParentItem? BuildSimulatorParentSlotItem(
        SpeciationSimulatorBrainOption? selected,
        string? overrideFilePath,
        string parentLabel)
    {
        if (TryReadParentOverrideGuid(overrideFilePath, parentLabel, out var overrideBrainId, out _))
        {
            return new SpeciationSimulatorSeedParentItem(
                overrideBrainId,
                ResolveSimulatorSeedParentLabel(overrideBrainId),
                $"Parent {parentLabel} override");
        }

        if (!string.IsNullOrWhiteSpace(overrideFilePath))
        {
            return null;
        }

        if (selected is null || selected.BrainId == Guid.Empty)
        {
            return null;
        }

        return new SpeciationSimulatorSeedParentItem(
            selected.BrainId,
            ResolveSimulatorSeedParentLabel(selected.BrainId, selected.Label),
            $"Parent {parentLabel}");
    }

    private void AddEffectiveSimulatorSeedParent(
        List<SpeciationSimulatorSeedParentItem> items,
        HashSet<Guid> seen,
        SpeciationSimulatorSeedParentItem? item)
    {
        if (item is null || item.BrainId == Guid.Empty || !seen.Add(item.BrainId))
        {
            return;
        }

        items.Add(item);
    }

    private string ResolveSimulatorSeedParentLabel(Guid brainId, string? fallbackLabel = null)
    {
        var active = SimActiveBrains.FirstOrDefault(entry => entry.BrainId == brainId);
        if (active is not null && !string.IsNullOrWhiteSpace(active.Label))
        {
            return active.Label;
        }

        return string.IsNullOrWhiteSpace(fallbackLabel)
            ? brainId.ToString("D")
            : fallbackLabel;
    }

    private static bool TryReadParentOverrideGuid(
        string? overrideFilePath,
        string parentLabel,
        out Guid brainId,
        out string error)
    {
        brainId = Guid.Empty;
        error = string.Empty;
        if (string.IsNullOrWhiteSpace(overrideFilePath))
        {
            return false;
        }

        if (!File.Exists(overrideFilePath))
        {
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
                return true;
            }

            brainId = Guid.Empty;
            error = $"Parent {parentLabel} override file must contain a brain GUID: {overrideFilePath}";
            return false;
        }

        error = $"Parent {parentLabel} override file has no usable brain GUID: {overrideFilePath}";
        return false;
    }

    private static IReadOnlyList<Guid> ParseBrainIdsFromFile(string path, string sourceLabel)
    {
        if (!File.Exists(path))
        {
            throw new InvalidOperationException($"{sourceLabel} not found: {path}");
        }

        var parentIds = new List<Guid>();
        foreach (var rawLine in File.ReadLines(path))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 || line.StartsWith("#", StringComparison.Ordinal))
            {
                continue;
            }

            if (!Guid.TryParse(line, out var brainId) || brainId == Guid.Empty)
            {
                throw new InvalidOperationException($"Invalid brain GUID '{line}' in {path}.");
            }

            parentIds.Add(brainId);
        }

        return parentIds;
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

    private string BuildEvolutionSimArgs(int ioPort, int simPort, IReadOnlyList<ArtifactRef> parentRefs)
    {
        var parentSpecs = parentRefs
            .Select(BuildEvolutionParentSpec)
            .Where(spec => !string.IsNullOrWhiteSpace(spec))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (parentSpecs.Count < 2)
        {
            return string.Empty;
        }

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
            $"--interval-ms {Math.Max(0, ParseInt(SimIntervalMsText, 100))}",
            $"--status-seconds {Math.Max(1, ParseInt(SimStatusSecondsText, 2))}",
            $"--timeout-seconds {Math.Max(1, ParseInt(SimTimeoutSecondsText, 10))}",
            $"--max-iterations {Math.Max(0, ParseInt(SimMaxIterationsText, 0))}",
            $"--max-parent-pool {Math.Max(2, ParseInt(SimMaxParentPoolText, 512))}",
            $"--min-runs {Math.Max(1, ParseInt(SimMinRunsText, 1))}",
            $"--max-runs {Math.Min(64, Math.Max(1, ParseInt(SimMaxRunsText, 6)))}",
            $"--run-gamma {ParseDouble(SimGammaText, 1d).ToString("0.###", CultureInfo.InvariantCulture)}",
            $"--run-pressure-mode {NormalizeRunPressureModeToken(SimRunPressureMode)}",
            $"--parent-selection-bias {NormalizeParentSelectionBiasToken(SimParentSelectionBias)}",
            $"--commit-to-speciation {(SimCommitToSpeciation ? "true" : "false")}",
            $"--spawn-children {(SimSpawnChildren ? "true" : "false")}",
            "--json"
        };

        foreach (var parentSpec in parentSpecs)
        {
            args.Add($"--parent {QuoteIfNeeded(parentSpec)}");
        }

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

    private static PopulationChartSnapshot BuildPopulationChartSnapshot(
        IReadOnlyList<EpochPopulationRow> epochRows,
        IReadOnlyList<SpeciesPopulationMeta> speciesOrder)
    {
        if (epochRows.Count == 0 || speciesOrder.Count == 0)
        {
            return new PopulationChartSnapshot(
                RangeLabel: "Epochs: (no data)",
                MetricLabel: "Population count by species (log10(1+count) y-axis).",
                YTopLabel: "0",
                YMidLabel: "0",
                YBottomLabel: "0",
                LegendColumns: 2,
                Series: Array.Empty<SpeciationLineChartSeriesItem>(),
                Legend: Array.Empty<SpeciationChartLegendItem>());
        }

        var totalSpeciesCount = speciesOrder.Count;
        var selectedSpecies = speciesOrder
            .Take(PopulationChartTopSpeciesLimit)
            .ToList();
        var maxCount = Math.Max(1, epochRows.SelectMany(row => row.Counts.Values).DefaultIfEmpty(0).Max());
        var logYAxisMax = Math.Max(Math.Log10(maxCount + 1d), 0.05d);
        var series = new List<SpeciationLineChartSeriesItem>(selectedSpecies.Count);
        var legend = new List<SpeciationChartLegendItem>(selectedSpecies.Count);
        foreach (var species in selectedSpecies)
        {
            var rawValues = epochRows
                .Select(row => row.Counts.TryGetValue(species.SpeciesId, out var count) ? count : 0)
                .ToArray();
            var values = rawValues
                .Select(value => Math.Log10(Math.Max(0, value) + 1d))
                .ToArray();
            var path = BuildLinePath(
                values,
                yMin: 0d,
                yMax: logYAxisMax,
                plotWidth: PopulationChartPlotWidth,
                plotHeight: PopulationChartPlotHeight,
                paddingX: PopulationChartPaddingX,
                paddingY: PopulationChartPaddingY);
            if (string.IsNullOrWhiteSpace(path))
            {
                continue;
            }

            var color = ResolveSpeciesColor(species.SpeciesId);
            var latestCount = rawValues.Length == 0 ? 0 : rawValues[^1];
            var latestCountLabel = latestCount.ToString(CultureInfo.InvariantCulture);
            series.Add(new SpeciationLineChartSeriesItem(species.SpeciesId, species.DisplayName, color, path, latestCountLabel));
            legend.Add(new SpeciationChartLegendItem(species.DisplayName, color, string.Empty));
        }

        var legendColumns = Math.Clamp(series.Count <= 1 ? 2 : series.Count, 2, 4);
        var minEpoch = epochRows[0].EpochId;
        var maxEpoch = epochRows[^1].EpochId;
        var topScopeLabel = totalSpeciesCount > selectedSpecies.Count
            ? $" top {selectedSpecies.Count}/{totalSpeciesCount} species by population."
            : string.Empty;
        var rangeLabel = minEpoch == maxEpoch && epochRows.Count > 1
            ? $"Epoch {minEpoch} row samples ({epochRows.Count} samples){topScopeLabel}"
            : $"Epochs {minEpoch}..{maxEpoch} ({epochRows.Count} samples){topScopeLabel}";
        return new PopulationChartSnapshot(
            RangeLabel: rangeLabel,
            MetricLabel: "Population count by species (log10(1+count) y-axis).",
            YTopLabel: FormatAxisValue(maxCount),
            YMidLabel: FormatAxisValue(Math.Max(0d, Math.Pow(10d, logYAxisMax * 0.5d) - 1d)),
            YBottomLabel: "0",
            LegendColumns: legendColumns,
            Series: series,
            Legend: legend);
    }

    private static FlowChartSnapshot BuildFlowChartSnapshot(
        IReadOnlyList<EpochPopulationRow> epochRows,
        IReadOnlyList<SpeciesPopulationMeta> speciesOrder)
    {
        if (epochRows.Count == 0 || speciesOrder.Count == 0)
        {
            return new FlowChartSnapshot(
                RangeLabel: "Epochs: (no data)",
                StartEpochLabel: "(n/a)",
                MidEpochLabel: "(n/a)",
                EndEpochLabel: "(n/a)",
                LegendColumns: 2,
                Areas: Array.Empty<SpeciationFlowChartAreaItem>(),
                Legend: Array.Empty<SpeciationChartLegendItem>());
        }

        var totalSpeciesCount = speciesOrder.Count;
        var selectedSpecies = speciesOrder
            .Take(FlowChartTopSpeciesLimit)
            .ToList();
        var includeOtherSpecies = totalSpeciesCount > selectedSpecies.Count;
        var flowSpecies = new List<SpeciesPopulationMeta>(selectedSpecies.Count + (includeOtherSpecies ? 1 : 0));
        flowSpecies.AddRange(selectedSpecies);
        if (includeOtherSpecies)
        {
            flowSpecies.Add(new SpeciesPopulationMeta("(other)", "Other species", 0));
        }

        var speciesCount = flowSpecies.Count;
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
                var count = 0;
                if (includeOtherSpecies && i == speciesCount - 1)
                {
                    var selectedTotal = selectedSpecies.Sum(species =>
                        row.Counts.TryGetValue(species.SpeciesId, out var value) ? value : 0);
                    count = Math.Max(0, total - selectedTotal);
                }
                else
                {
                    count = row.Counts.TryGetValue(flowSpecies[i].SpeciesId, out var value) ? value : 0;
                }

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

            var species = flowSpecies[speciesIndex];
            var color = string.Equals(species.SpeciesId, "(other)", StringComparison.Ordinal)
                ? "#6B7280"
                : ResolveSpeciesColor(species.SpeciesId);
            var fill = WithAlpha(color, 0x8C);
            var lastShare = Math.Max(0d, ends[^1] - starts[^1]);
            var lastShareLabel = lastShare.ToString("P1", CultureInfo.InvariantCulture);
            areas.Add(new SpeciationFlowChartAreaItem(species.SpeciesId, species.DisplayName, fill, color, path, lastShareLabel));
            legend.Add(new SpeciationChartLegendItem(species.DisplayName, color, string.Empty));
        }

        var minEpoch = epochRows[0].EpochId;
        var maxEpoch = epochRows[^1].EpochId;
        var isSingleEpochRowSampling = minEpoch == maxEpoch && epochRows.Count > 1;
        var topAxisLabel = isSingleEpochRowSampling
            ? "1"
            : minEpoch.ToString(CultureInfo.InvariantCulture);
        var midAxisLabel = isSingleEpochRowSampling
            ? FormatAxisNumber((epochRows.Count + 1d) * 0.5d)
            : FormatAxisNumber((minEpoch + maxEpoch) * 0.5d);
        var bottomAxisLabel = isSingleEpochRowSampling
            ? epochRows.Count.ToString(CultureInfo.InvariantCulture)
            : maxEpoch.ToString(CultureInfo.InvariantCulture);
        var legendColumns = Math.Clamp(areas.Count <= 1 ? 2 : areas.Count, 2, 4);
        var topScopeLabel = includeOtherSpecies
            ? $" top {selectedSpecies.Count}/{totalSpeciesCount} species + Other."
            : string.Empty;
        var rangeLabel = minEpoch == maxEpoch && epochRows.Count > 1
            ? $"Stacked share across loaded rows for epoch {minEpoch} ({epochRows.Count} samples).{topScopeLabel}"
            : $"Stacked share of total population per epoch ({minEpoch}..{maxEpoch}).{topScopeLabel}";
        return new FlowChartSnapshot(
            RangeLabel: rangeLabel,
            StartEpochLabel: topAxisLabel,
            MidEpochLabel: midAxisLabel,
            EndEpochLabel: bottomAxisLabel,
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
        FlowChartMidEpochLabel = snapshot.MidEpochLabel;
        FlowChartEndEpochLabel = snapshot.EndEpochLabel;
        FlowChartLegendColumns = snapshot.LegendColumns;
    }

    private void ApplySplitProximityChartSnapshot(SplitProximityChartSnapshot snapshot)
    {
        ReplaceItems(SplitProximityChartSeries, snapshot.Series);
        ReplaceItems(SplitProximityChartLegend, snapshot.Legend);
        SplitProximityChartRangeLabel = snapshot.RangeLabel;
        SplitProximityChartMetricLabel = snapshot.MetricLabel;
        SplitProximityChartYAxisTopLabel = snapshot.YTopLabel;
        SplitProximityChartYAxisMidLabel = snapshot.YMidLabel;
        SplitProximityChartYAxisBottomLabel = snapshot.YBottomLabel;
        SplitProximityChartLegendColumns = snapshot.LegendColumns;
    }

    private void ApplyCladogramSnapshot(CladogramSnapshot snapshot)
    {
        var priorExpansionBySpecies = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
        CaptureCladogramExpansionState(CladogramItems, priorExpansionBySpecies);
        foreach (var root in snapshot.Items)
        {
            ApplyCladogramExpansionState(root, priorExpansionBySpecies);
        }

        ReplaceItems(CladogramItems, snapshot.Items);
        CladogramRangeLabel = snapshot.RangeLabel;
        CladogramMetricLabel = snapshot.MetricLabel;
    }

    private static void CaptureCladogramExpansionState(
        IEnumerable<SpeciationCladogramItem> items,
        IDictionary<string, bool> expansionBySpecies)
    {
        foreach (var item in items)
        {
            expansionBySpecies[item.SpeciesId] = item.IsExpanded;
            CaptureCladogramExpansionState(item.Children, expansionBySpecies);
        }
    }

    private static bool ApplyCladogramExpansionState(
        SpeciationCladogramItem node,
        IReadOnlyDictionary<string, bool> priorExpansionBySpecies)
    {
        var wasKnown = priorExpansionBySpecies.TryGetValue(node.SpeciesId, out var priorExpanded);
        var subtreeContainsNewSpecies = !wasKnown;
        foreach (var child in node.Children)
        {
            if (ApplyCladogramExpansionState(child, priorExpansionBySpecies))
            {
                subtreeContainsNewSpecies = true;
            }
        }

        node.IsExpanded = subtreeContainsNewSpecies || (wasKnown ? priorExpanded : true);
        return subtreeContainsNewSpecies;
    }

    private static (List<EpochPopulationRow> EpochRows, List<SpeciesPopulationMeta> SpeciesOrder) BuildEpochPopulationFrame(IReadOnlyList<SpeciationMembershipRecord> history)
    {
        if (history.Count == 0)
        {
            return (new List<EpochPopulationRow>(), new List<SpeciesPopulationMeta>());
        }

        var orderedHistory = OrderHistoryForSampling(history);
        return ShouldUseSingleEpochRowSampling(orderedHistory)
            ? BuildSingleEpochPopulationFrame(orderedHistory)
            : BuildAggregatedEpochPopulationFrame(orderedHistory);
    }

    private static (List<EpochPopulationRow> EpochRows, List<SpeciesPopulationMeta> SpeciesOrder) BuildAggregatedEpochPopulationFrame(
        IReadOnlyList<SpeciationMembershipRecord> history)
    {
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
                    var speciesName = BuildCompactSpeciesName(record.SpeciesDisplayName, speciesId);
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

    private static (List<EpochPopulationRow> EpochRows, List<SpeciesPopulationMeta> SpeciesOrder) BuildSingleEpochPopulationFrame(
        IReadOnlyList<SpeciationMembershipRecord> orderedHistory)
    {
        var speciesStats = new Dictionary<string, SpeciesPopulationMeta>(StringComparer.OrdinalIgnoreCase);
        var runningCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var epochRows = new List<EpochPopulationRow>(orderedHistory.Count);
        var runningTotal = 0;
        foreach (var record in orderedHistory)
        {
            var speciesId = NormalizeSpeciesId(record.SpeciesId);
            var speciesName = BuildCompactSpeciesName(record.SpeciesDisplayName, speciesId);
            runningCounts.TryGetValue(speciesId, out var prior);
            runningCounts[speciesId] = prior + 1;
            runningTotal++;

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

            epochRows.Add(
                new EpochPopulationRow(
                    EpochId: (long)record.EpochId,
                    Counts: new Dictionary<string, int>(runningCounts, StringComparer.OrdinalIgnoreCase),
                    TotalCount: runningTotal));
        }

        var speciesOrder = speciesStats.Values
            .OrderByDescending(item => item.TotalCount)
            .ThenBy(item => item.SpeciesId, StringComparer.OrdinalIgnoreCase)
            .ToList();
        return (epochRows, speciesOrder);
    }

    private static SplitProximityChartSnapshot BuildSplitProximityChartSnapshot(
        IReadOnlyList<SpeciationMembershipRecord> history,
        long currentEpochId,
        double fallbackSplitThreshold,
        double fallbackSplitGuardMargin)
    {
        if (history.Count == 0)
        {
            return SplitProximityChartSnapshot.Empty("Split proximity (current epoch): (n/a)");
        }

        var boundedFallbackSplit = Clamp01(fallbackSplitThreshold);
        var orderedHistory = OrderHistoryForSampling(history);
        var useSingleEpochRowSampling = ShouldUseSingleEpochRowSampling(orderedHistory);
        var speciesMeta = new Dictionary<string, SplitProximitySpeciesMeta>(StringComparer.OrdinalIgnoreCase);
        var epochRows = useSingleEpochRowSampling
            ? BuildSingleEpochSplitProximityRows(orderedHistory, boundedFallbackSplit, fallbackSplitGuardMargin, speciesMeta)
            : BuildAggregatedEpochSplitProximityRows(orderedHistory, boundedFallbackSplit, fallbackSplitGuardMargin, speciesMeta);

        if (epochRows.Count == 0 || speciesMeta.Count == 0)
        {
            return SplitProximityChartSnapshot.Empty("Split proximity (current epoch): (n/a, no similarity scores)");
        }

        var selectedSpecies = speciesMeta.Values
            .OrderByDescending(item => item.SampleCount)
            .ThenBy(item => item.SpeciesId, StringComparer.OrdinalIgnoreCase)
            .Take(SplitProximityTopSpeciesLimit)
            .ToList();
        if (selectedSpecies.Count == 0)
        {
            return SplitProximityChartSnapshot.Empty("Split proximity (current epoch): (n/a, no species evidence)");
        }

        var allValues = new List<double>(selectedSpecies.Count * Math.Max(1, epochRows.Count));
        foreach (var species in selectedSpecies)
        {
            foreach (var row in epochRows)
            {
                if (row.ValuesBySpecies.TryGetValue(species.SpeciesId, out var point))
                {
                    allValues.Add(point.MinProximity);
                }
            }
        }

        if (allValues.Count == 0)
        {
            return SplitProximityChartSnapshot.Empty("Split proximity (current epoch): (n/a, no proximity samples)");
        }

        var rawAbsMax = allValues
            .Select(value => Math.Abs(value))
            .DefaultIfEmpty(0d)
            .Max();
        if (!double.IsFinite(rawAbsMax) || rawAbsMax <= 0d)
        {
            rawAbsMax = 0.001d;
        }

        var yAbsMax = Math.Max(0.001d, TransformSignedLog(rawAbsMax));
        var yMin = -yAbsMax;
        var yMax = yAbsMax;

        var series = new List<SpeciationLineChartSeriesItem>(selectedSpecies.Count);
        var legend = new List<SpeciationChartLegendItem>(selectedSpecies.Count);
        foreach (var species in selectedSpecies)
        {
            var rawValues = epochRows
                .Select(row => row.ValuesBySpecies.TryGetValue(species.SpeciesId, out var point)
                    ? point.MinProximity
                    : double.NaN)
                .ToArray();
            var values = rawValues
                .Select(TransformSignedLogOrNan)
                .ToArray();
            var path = BuildLinePath(
                values,
                yMin: yMin,
                yMax: yMax,
                plotWidth: PopulationChartPlotWidth,
                plotHeight: PopulationChartPlotHeight,
                paddingX: PopulationChartPaddingX,
                paddingY: PopulationChartPaddingY);
            if (string.IsNullOrWhiteSpace(path))
            {
                continue;
            }

            var latestValue = TryGetLatestFiniteValue(rawValues, out var latest)
                ? latest
                : double.NaN;
            var latestLabel = double.IsFinite(latestValue)
                ? FormatSignedDelta(latestValue)
                : "n/a";
            var color = ResolveSpeciesColor(species.SpeciesId);
            series.Add(new SpeciationLineChartSeriesItem(species.SpeciesId, species.DisplayName, color, path, latestLabel));
            legend.Add(new SpeciationChartLegendItem(species.DisplayName, color, string.Empty));
        }

        if (series.Count == 0)
        {
            return SplitProximityChartSnapshot.Empty("Split proximity (current epoch): (n/a, no drawable species series)");
        }

        var targetEpoch = currentEpochId > 0
            ? currentEpochId
            : epochRows[^1].EpochId;
        var targetRow = epochRows.LastOrDefault(row => row.EpochId == targetEpoch);
        if (targetRow.ValuesBySpecies is null || targetRow.ValuesBySpecies.Count == 0)
        {
            targetRow = epochRows[^1];
        }

        var currentEpochSummary = BuildSplitProximitySummaryLabel(
            targetEpoch,
            targetRow,
            speciesMeta);

        var minEpoch = epochRows[0].EpochId;
        var maxEpoch = epochRows[^1].EpochId;
        var legendColumns = Math.Clamp(series.Count <= 1 ? 2 : series.Count, 2, 4);
        var rangeLabel = useSingleEpochRowSampling
            ? $"Epoch {minEpoch} row samples ({epochRows.Count} samples; top {series.Count}/{speciesMeta.Count} species by similarity samples)."
            : $"Epochs {minEpoch}..{maxEpoch} ({epochRows.Count} samples; top {series.Count}/{speciesMeta.Count} species by similarity samples).";
        return new SplitProximityChartSnapshot(
            RangeLabel: rangeLabel,
            MetricLabel: "Min lineage similarity minus effective split threshold per species (signed log10(1+|delta|) y-axis; <=0 means split-trigger zone).",
            YTopLabel: FormatSignedDelta(rawAbsMax),
            YMidLabel: "0",
            YBottomLabel: FormatSignedDelta(-rawAbsMax),
            LegendColumns: legendColumns,
            CurrentEpochSummaryLabel: currentEpochSummary,
            Series: series,
            Legend: legend);
    }

    private static IReadOnlyList<SpeciationEpochSummaryItem> BuildEpochSummaries(IReadOnlyList<SpeciationMembershipRecord> history)
    {
        return history
            .GroupBy(entry => entry.EpochId)
            .OrderBy(group => group.Key)
            .Select(group =>
            {
                var ordered = group
                    .OrderBy(entry => entry.AssignedMs)
                    .ThenBy(entry => entry.SpeciesId, StringComparer.Ordinal)
                    .ToList();
                var firstAssigned = ordered.Count == 0 ? "(n/a)" : FormatTimestamp(ordered[0].AssignedMs);
                var lastAssigned = ordered.Count == 0 ? "(n/a)" : FormatTimestamp(ordered[^1].AssignedMs);
                var speciesCount = ordered
                    .Select(entry => entry.SpeciesId)
                    .Where(speciesId => !string.IsNullOrWhiteSpace(speciesId))
                    .Distinct(StringComparer.Ordinal)
                    .Count();

                return new SpeciationEpochSummaryItem(
                    EpochId: (long)group.Key,
                    MembershipCount: ordered.Count,
                    SpeciesCount: speciesCount,
                    FirstAssigned: $"first {firstAssigned}",
                    LastAssigned: $"last {lastAssigned}");
            })
            .ToList();
    }

    private static CladogramSnapshot BuildCladogramSnapshot(IReadOnlyList<SpeciationMembershipRecord> history)
    {
        if (history.Count == 0)
        {
            return CladogramSnapshot.Empty("Cladogram: (no data)");
        }

        var orderedHistory = OrderHistoryForSampling(history);
        var speciesMeta = new Dictionary<string, CladogramSpeciesMeta>(StringComparer.OrdinalIgnoreCase);
        var parentByChild = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var countsBySpecies = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var divergenceEdgeCount = 0;
        foreach (var record in orderedHistory)
        {
            var speciesId = NormalizeSpeciesId(record.SpeciesId);
            var speciesName = BuildCompactSpeciesName(record.SpeciesDisplayName, speciesId);
            if (speciesMeta.TryGetValue(speciesId, out var existingMeta))
            {
                speciesMeta[speciesId] = existingMeta with
                {
                    DisplayName = string.IsNullOrWhiteSpace(existingMeta.DisplayName) ? speciesName : existingMeta.DisplayName
                };
            }
            else
            {
                speciesMeta[speciesId] = new CladogramSpeciesMeta(speciesId, speciesName);
            }

            countsBySpecies.TryGetValue(speciesId, out var priorCount);
            countsBySpecies[speciesId] = priorCount + 1;

            if (!string.Equals(record.DecisionReason, "lineage_diverged_new_species", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!TryExtractDominantSpeciesFromMetadata(record.DecisionMetadataJson, out var parentSpeciesId, out var parentSpeciesName))
            {
                continue;
            }

            var parentId = NormalizeSpeciesId(parentSpeciesId);
            if (string.Equals(parentId, speciesId, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!speciesMeta.ContainsKey(parentId))
            {
                speciesMeta[parentId] = new CladogramSpeciesMeta(parentId, BuildCompactSpeciesName(parentSpeciesName, parentId));
            }

            if (!parentByChild.ContainsKey(speciesId))
            {
                parentByChild[speciesId] = parentId;
                divergenceEdgeCount++;
            }
        }

        if (speciesMeta.Count == 0)
        {
            return CladogramSnapshot.Empty("Cladogram: (no data)");
        }

        var childrenByParent = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var (childSpeciesId, parentSpeciesId) in parentByChild)
        {
            if (!childrenByParent.TryGetValue(parentSpeciesId, out var children))
            {
                children = new List<string>();
                childrenByParent[parentSpeciesId] = children;
            }

            children.Add(childSpeciesId);
        }

        foreach (var children in childrenByParent.Values)
        {
            children.Sort((left, right) =>
            {
                countsBySpecies.TryGetValue(left, out var leftCount);
                countsBySpecies.TryGetValue(right, out var rightCount);
                var countComparison = rightCount.CompareTo(leftCount);
                return countComparison != 0
                    ? countComparison
                    : string.Compare(left, right, StringComparison.OrdinalIgnoreCase);
            });
        }

        var roots = speciesMeta.Keys
            .Where(speciesId => !parentByChild.ContainsKey(speciesId))
            .OrderByDescending(speciesId => countsBySpecies.TryGetValue(speciesId, out var count) ? count : 0)
            .ThenBy(speciesId => speciesId, StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (roots.Count == 0)
        {
            roots.AddRange(speciesMeta.Keys.OrderBy(speciesId => speciesId, StringComparer.OrdinalIgnoreCase));
        }

        var items = new List<SpeciationCladogramItem>(roots.Count);
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var root in roots)
        {
            var node = BuildCladogramNode(
                speciesId: root,
                isRoot: true,
                speciesMeta: speciesMeta,
                countsBySpecies: countsBySpecies,
                childrenByParent: childrenByParent,
                visited: visited);
            if (node is not null)
            {
                items.Add(node);
            }
        }

        foreach (var speciesId in speciesMeta.Keys.OrderBy(species => species, StringComparer.OrdinalIgnoreCase))
        {
            if (visited.Contains(speciesId))
            {
                continue;
            }

            var disconnectedRoot = BuildCladogramNode(
                speciesId: speciesId,
                isRoot: true,
                speciesMeta: speciesMeta,
                countsBySpecies: countsBySpecies,
                childrenByParent: childrenByParent,
                visited: visited);
            if (disconnectedRoot is not null)
            {
                items.Add(disconnectedRoot);
            }
        }

        var rangeLabel = $"Cladogram edges from divergence decisions: {divergenceEdgeCount} across {speciesMeta.Count} species.";
        return new CladogramSnapshot(
            RangeLabel: rangeLabel,
            MetricLabel: "Parent -> child lineage edges inferred from lineage_diverged_new_species decisions.",
            Items: items);
    }

    private static SpeciationCladogramItem? BuildCladogramNode(
        string speciesId,
        bool isRoot,
        IReadOnlyDictionary<string, CladogramSpeciesMeta> speciesMeta,
        IReadOnlyDictionary<string, int> countsBySpecies,
        IReadOnlyDictionary<string, List<string>> childrenByParent,
        ISet<string> visited)
    {
        if (!visited.Add(speciesId))
        {
            return null;
        }

        if (!speciesMeta.TryGetValue(speciesId, out var meta))
        {
            meta = new CladogramSpeciesMeta(speciesId, speciesId);
        }

        countsBySpecies.TryGetValue(speciesId, out var count);
        var childNodes = new List<SpeciationCladogramItem>();
        if (childrenByParent.TryGetValue(speciesId, out var children) && children.Count > 0)
        {
            for (var index = 0; index < children.Count; index++)
            {
                var childNode = BuildCladogramNode(
                    speciesId: children[index],
                    isRoot: false,
                    speciesMeta: speciesMeta,
                    countsBySpecies: countsBySpecies,
                    childrenByParent: childrenByParent,
                    visited: visited);
                if (childNode is not null)
                {
                    childNodes.Add(childNode);
                }
            }
        }

        var detailLabel = $"members {count} | direct derived {childNodes.Count}";
        return new SpeciationCladogramItem(
            speciesId: speciesId,
            speciesDisplayName: meta.DisplayName,
            detailLabel: detailLabel,
            color: ResolveSpeciesColor(speciesId),
            isRoot: isRoot,
            children: childNodes);
    }

    private static bool TryExtractDominantSpeciesFromMetadata(
        string? metadataJson,
        out string speciesId,
        out string speciesDisplayName)
    {
        speciesId = string.Empty;
        speciesDisplayName = string.Empty;
        if (string.IsNullOrWhiteSpace(metadataJson))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(metadataJson);
            var root = document.RootElement;
            if (!root.TryGetProperty("lineage", out var lineage))
            {
                return false;
            }

            if (!TryGetJsonString(lineage, "dominant_species_id", out speciesId)
                && !TryGetJsonString(lineage, "dominantSpeciesId", out speciesId))
            {
                return false;
            }

            TryGetJsonString(lineage, "dominant_species_display_name", out speciesDisplayName);
            if (string.IsNullOrWhiteSpace(speciesDisplayName))
            {
                TryGetJsonString(lineage, "dominantSpeciesDisplayName", out speciesDisplayName);
            }

            return !string.IsNullOrWhiteSpace(speciesId);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static List<EpochSplitProximityRow> BuildAggregatedEpochSplitProximityRows(
        IReadOnlyList<SpeciationMembershipRecord> history,
        double fallbackSplitThreshold,
        double fallbackSplitGuardMargin,
        Dictionary<string, SplitProximitySpeciesMeta> speciesMeta)
    {
        return history
            .GroupBy(entry => (long)entry.EpochId)
            .OrderBy(group => group.Key)
            .Select(group =>
            {
                var bySpecies = new Dictionary<string, SplitProximitySpeciesPoint>(StringComparer.OrdinalIgnoreCase);
                foreach (var record in group)
                {
                    if (!TryExtractSplitProximity(
                            record.DecisionMetadataJson,
                            fallbackSplitThreshold,
                            fallbackSplitGuardMargin,
                            out var similarity,
                            out var splitThreshold,
                            out var proximity))
                    {
                        continue;
                    }

                    var speciesId = NormalizeSpeciesId(record.SpeciesId);
                    var speciesName = BuildCompactSpeciesName(record.SpeciesDisplayName, speciesId);
                    if (speciesMeta.TryGetValue(speciesId, out var existingMeta))
                    {
                        speciesMeta[speciesId] = existingMeta with
                        {
                            DisplayName = string.IsNullOrWhiteSpace(existingMeta.DisplayName) ? speciesName : existingMeta.DisplayName,
                            SampleCount = existingMeta.SampleCount + 1
                        };
                    }
                    else
                    {
                        speciesMeta[speciesId] = new SplitProximitySpeciesMeta(speciesId, speciesName, 1);
                    }

                    if (bySpecies.TryGetValue(speciesId, out var existingPoint))
                    {
                        var nextCount = existingPoint.SampleCount + 1;
                        if (proximity < existingPoint.MinProximity)
                        {
                            bySpecies[speciesId] = new SplitProximitySpeciesPoint(proximity, similarity, splitThreshold, nextCount);
                        }
                        else
                        {
                            bySpecies[speciesId] = existingPoint with { SampleCount = nextCount };
                        }
                    }
                    else
                    {
                        bySpecies[speciesId] = new SplitProximitySpeciesPoint(
                            MinProximity: proximity,
                            MinSimilarity: similarity,
                            SplitThreshold: splitThreshold,
                            SampleCount: 1);
                    }
                }

                return new EpochSplitProximityRow(group.Key, bySpecies);
            })
            .ToList();
    }

    private static List<EpochSplitProximityRow> BuildSingleEpochSplitProximityRows(
        IReadOnlyList<SpeciationMembershipRecord> orderedHistory,
        double fallbackSplitThreshold,
        double fallbackSplitGuardMargin,
        Dictionary<string, SplitProximitySpeciesMeta> speciesMeta)
    {
        var rows = new List<EpochSplitProximityRow>(orderedHistory.Count);
        var rollingBySpecies = new Dictionary<string, SplitProximitySpeciesPoint>(StringComparer.OrdinalIgnoreCase);
        foreach (var record in orderedHistory)
        {
            if (TryExtractSplitProximity(
                    record.DecisionMetadataJson,
                    fallbackSplitThreshold,
                    fallbackSplitGuardMargin,
                    out var similarity,
                    out var splitThreshold,
                    out var proximity))
            {
                var speciesId = NormalizeSpeciesId(record.SpeciesId);
                var speciesName = BuildCompactSpeciesName(record.SpeciesDisplayName, speciesId);
                if (speciesMeta.TryGetValue(speciesId, out var existingMeta))
                {
                    speciesMeta[speciesId] = existingMeta with
                    {
                        DisplayName = string.IsNullOrWhiteSpace(existingMeta.DisplayName) ? speciesName : existingMeta.DisplayName,
                        SampleCount = existingMeta.SampleCount + 1
                    };
                }
                else
                {
                    speciesMeta[speciesId] = new SplitProximitySpeciesMeta(speciesId, speciesName, 1);
                }

                if (rollingBySpecies.TryGetValue(speciesId, out var existingPoint))
                {
                    var nextCount = existingPoint.SampleCount + 1;
                    if (proximity < existingPoint.MinProximity)
                    {
                        rollingBySpecies[speciesId] = new SplitProximitySpeciesPoint(proximity, similarity, splitThreshold, nextCount);
                    }
                    else
                    {
                        rollingBySpecies[speciesId] = existingPoint with { SampleCount = nextCount };
                    }
                }
                else
                {
                    rollingBySpecies[speciesId] = new SplitProximitySpeciesPoint(
                        MinProximity: proximity,
                        MinSimilarity: similarity,
                        SplitThreshold: splitThreshold,
                        SampleCount: 1);
                }
            }

            if (rollingBySpecies.Count > 0)
            {
                rows.Add(
                    new EpochSplitProximityRow(
                        EpochId: (long)record.EpochId,
                        ValuesBySpecies: new Dictionary<string, SplitProximitySpeciesPoint>(rollingBySpecies, StringComparer.OrdinalIgnoreCase)));
            }
        }

        return rows;
    }

    private static DivergenceSnapshot BuildCurrentEpochDivergenceSnapshot(
        IReadOnlyList<SpeciationMembershipRecord> history,
        long currentEpochId)
    {
        if (history.Count == 0)
        {
            return new DivergenceSnapshot("Max within-species divergence (current epoch): (n/a)");
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
            if (!TryExtractAssignedSpeciesSimilarityScore(record.DecisionMetadataJson, out var similarity))
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
            return new DivergenceSnapshot($"Max within-species divergence (epoch {targetEpoch}): (n/a, no similarity scores)");
        }

        var label =
            $"Max within-species divergence (epoch {targetEpoch}) = {maxDivergence.Value:0.###} (min assigned-species similarity {minSimilarity.Value:0.###}, samples={sampleCount}, brain={maxBrainLabel}).";
        return new DivergenceSnapshot(label);
    }

    private static bool TryExtractAssignedSpeciesSimilarityScore(string? metadataJson, out double similarityScore)
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
            if (root.TryGetProperty("lineage", out var lineage)
                && TryGetSimilarityFromElement(
                    lineage,
                    out similarityScore,
                    "intra_species_similarity_sample",
                    "intraSpeciesSimilaritySample",
                    "lineage_assignment_similarity_score",
                    "lineageAssignmentSimilarityScore",
                    "dominant_species_similarity_score",
                    "dominantSpeciesSimilarityScore",
                    "lineage_similarity_score",
                    "lineageSimilarityScore",
                    "similarity_score",
                    "similarityScore"))
            {
                return true;
            }

            if (TryGetSimilarityFromElement(
                    root,
                    out similarityScore,
                    "intra_species_similarity_sample",
                    "intraSpeciesSimilaritySample",
                    "lineage_assignment_similarity_score",
                    "lineageAssignmentSimilarityScore",
                    "dominant_species_similarity_score",
                    "dominantSpeciesSimilarityScore",
                    "lineage_similarity_score",
                    "lineageSimilarityScore"))
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

    private static bool TryExtractSourceSpeciesSimilarityScore(string? metadataJson, out double similarityScore)
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
            if (root.TryGetProperty("lineage", out var lineage)
                && TryGetSimilarityFromElement(
                    lineage,
                    out similarityScore,
                    "source_species_similarity_score",
                    "sourceSpeciesSimilarityScore",
                    "dominant_species_similarity_score",
                    "dominantSpeciesSimilarityScore",
                    "lineage_similarity_score",
                    "lineageSimilarityScore",
                    "similarity_score",
                    "similarityScore"))
            {
                return true;
            }

            if (TryGetSimilarityFromElement(
                    root,
                    out similarityScore,
                    "source_species_similarity_score",
                    "sourceSpeciesSimilarityScore",
                    "dominant_species_similarity_score",
                    "dominantSpeciesSimilarityScore",
                    "lineage_similarity_score",
                    "lineageSimilarityScore"))
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

    private static bool TryGetSimilarityFromElement(
        JsonElement element,
        out double similarityScore,
        params string[] propertyNames)
    {
        similarityScore = 0d;
        if (propertyNames.Length == 0)
        {
            propertyNames = new[] { "similarity_score", "similarityScore" };
        }

        foreach (var propertyName in propertyNames)
        {
            if (TryGetJsonDouble(element, propertyName, out similarityScore))
            {
                return true;
            }
        }

        if (element.TryGetProperty("scores", out var scores)
            && (TryGetJsonDouble(scores, propertyNames[0], out similarityScore)
                || (propertyNames.Length > 1 && TryGetJsonDouble(scores, propertyNames[1], out similarityScore))
                || (propertyNames.Length > 2 && TryGetJsonDouble(scores, propertyNames[2], out similarityScore))
                || (propertyNames.Length > 3 && TryGetJsonDouble(scores, propertyNames[3], out similarityScore))))
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

    private static bool TryGetJsonString(JsonElement element, string propertyName, out string value)
    {
        value = string.Empty;
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return false;
        }

        var raw = property.ValueKind switch
        {
            JsonValueKind.String => property.GetString(),
            JsonValueKind.Number => property.GetRawText(),
            _ => null
        };
        if (string.IsNullOrWhiteSpace(raw))
        {
            return false;
        }

        value = raw.Trim();
        return value.Length > 0;
    }

    private static bool TryExtractSplitProximity(
        string? metadataJson,
        double fallbackSplitThreshold,
        double fallbackSplitGuardMargin,
        out double similarity,
        out double splitThreshold,
        out double proximity)
    {
        similarity = 0d;
        splitThreshold = Clamp01(fallbackSplitThreshold);
        var splitGuardMargin = Clamp01(fallbackSplitGuardMargin);
        var hasExplicitEffectiveSplitThreshold = false;
        var hasExplicitProximity = false;
        proximity = 0d;
        if (!string.IsNullOrWhiteSpace(metadataJson))
        {
            try
            {
                using var document = JsonDocument.Parse(metadataJson);
                var root = document.RootElement;
                if (root.TryGetProperty("lineage", out var lineage))
                {
                    if (TryGetJsonDouble(lineage, "assigned_split_proximity_to_dynamic_threshold", out proximity)
                        || TryGetJsonDouble(lineage, "assignedSplitProximityToDynamicThreshold", out proximity)
                        || TryGetJsonDouble(lineage, "split_proximity_to_dynamic_threshold", out proximity)
                        || TryGetJsonDouble(lineage, "splitProximityToDynamicThreshold", out proximity)
                        || TryGetJsonDouble(lineage, "source_split_proximity_to_dynamic_threshold", out proximity)
                        || TryGetJsonDouble(lineage, "sourceSplitProximityToDynamicThreshold", out proximity))
                    {
                        hasExplicitProximity = true;
                        hasExplicitEffectiveSplitThreshold = true;
                    }
                    else if (TryGetJsonDouble(lineage, "assigned_split_proximity_to_policy_threshold", out proximity)
                             || TryGetJsonDouble(lineage, "assignedSplitProximityToPolicyThreshold", out proximity)
                             || TryGetJsonDouble(lineage, "split_proximity_to_policy_threshold", out proximity)
                             || TryGetJsonDouble(lineage, "splitProximityToPolicyThreshold", out proximity)
                             || TryGetJsonDouble(lineage, "source_split_proximity_to_policy_threshold", out proximity)
                             || TryGetJsonDouble(lineage, "sourceSplitProximityToPolicyThreshold", out proximity))
                    {
                        hasExplicitProximity = true;
                    }
                }
                else if (TryGetJsonDouble(root, "assigned_split_proximity_to_dynamic_threshold", out proximity)
                         || TryGetJsonDouble(root, "assignedSplitProximityToDynamicThreshold", out proximity)
                         || TryGetJsonDouble(root, "split_proximity_to_dynamic_threshold", out proximity)
                         || TryGetJsonDouble(root, "splitProximityToDynamicThreshold", out proximity)
                         || TryGetJsonDouble(root, "source_split_proximity_to_dynamic_threshold", out proximity)
                         || TryGetJsonDouble(root, "sourceSplitProximityToDynamicThreshold", out proximity))
                {
                    hasExplicitProximity = true;
                    hasExplicitEffectiveSplitThreshold = true;
                }
                else if (TryGetJsonDouble(root, "assigned_split_proximity_to_policy_threshold", out proximity)
                         || TryGetJsonDouble(root, "assignedSplitProximityToPolicyThreshold", out proximity)
                         || TryGetJsonDouble(root, "split_proximity_to_policy_threshold", out proximity)
                         || TryGetJsonDouble(root, "splitProximityToPolicyThreshold", out proximity)
                         || TryGetJsonDouble(root, "source_split_proximity_to_policy_threshold", out proximity)
                         || TryGetJsonDouble(root, "sourceSplitProximityToPolicyThreshold", out proximity))
                {
                    hasExplicitProximity = true;
                }

                if (root.TryGetProperty("assignment_policy", out var policy)
                    && (TryGetJsonDouble(policy, "lineage_assigned_dynamic_split_threshold", out splitThreshold)
                        || TryGetJsonDouble(policy, "lineageAssignedDynamicSplitThreshold", out splitThreshold)
                        || TryGetJsonDouble(policy, "lineage_dynamic_split_threshold", out splitThreshold)
                        || TryGetJsonDouble(policy, "lineageDynamicSplitThreshold", out splitThreshold)
                        || TryGetJsonDouble(policy, "lineage_source_dynamic_split_threshold", out splitThreshold)
                        || TryGetJsonDouble(policy, "lineageSourceDynamicSplitThreshold", out splitThreshold)
                        || TryGetJsonDouble(policy, "lineage_split_threshold", out splitThreshold)
                        || TryGetJsonDouble(policy, "lineageSplitThreshold", out splitThreshold)))
                {
                    splitThreshold = Clamp01(splitThreshold);
                    hasExplicitEffectiveSplitThreshold =
                        TryGetJsonDouble(policy, "lineage_assigned_dynamic_split_threshold", out _)
                        || TryGetJsonDouble(policy, "lineageAssignedDynamicSplitThreshold", out _)
                        || 
                        TryGetJsonDouble(policy, "lineage_dynamic_split_threshold", out _)
                        || TryGetJsonDouble(policy, "lineageDynamicSplitThreshold", out _)
                        || TryGetJsonDouble(policy, "lineage_source_dynamic_split_threshold", out _)
                        || TryGetJsonDouble(policy, "lineageSourceDynamicSplitThreshold", out _);
                    if (TryGetJsonDouble(policy, "lineage_split_guard_margin", out var policySplitGuardMargin)
                        || TryGetJsonDouble(policy, "lineageSplitGuardMargin", out policySplitGuardMargin))
                    {
                        splitGuardMargin = Clamp01(policySplitGuardMargin);
                    }
                }
                else if (TryGetJsonDouble(root, "lineage_dynamic_split_threshold", out var rootDynamicSplitThreshold)
                         || TryGetJsonDouble(root, "lineageDynamicSplitThreshold", out rootDynamicSplitThreshold))
                {
                    splitThreshold = Clamp01(rootDynamicSplitThreshold);
                    hasExplicitEffectiveSplitThreshold = true;
                }
                else if (TryGetJsonDouble(root, "lineage_split_threshold", out var rootSplitThreshold)
                         || TryGetJsonDouble(root, "lineageSplitThreshold", out rootSplitThreshold))
                {
                    splitThreshold = Clamp01(rootSplitThreshold);
                    if (TryGetJsonDouble(root, "lineage_split_guard_margin", out var rootSplitGuardMargin)
                        || TryGetJsonDouble(root, "lineageSplitGuardMargin", out rootSplitGuardMargin))
                    {
                        splitGuardMargin = Clamp01(rootSplitGuardMargin);
                    }
                }
            }
            catch (JsonException)
            {
                // Fallback split threshold is used when metadata is malformed.
            }
        }

        if (!hasExplicitEffectiveSplitThreshold)
        {
            splitThreshold = Math.Max(0d, splitThreshold - splitGuardMargin);
        }

        if (hasExplicitProximity)
        {
            similarity = Clamp01(splitThreshold + proximity);
            return true;
        }

        if (!TryExtractAssignedSpeciesSimilarityScore(metadataJson, out similarity)
            && !TryExtractSourceSpeciesSimilarityScore(metadataJson, out similarity))
        {
            return false;
        }

        similarity = Clamp01(similarity);
        proximity = similarity - splitThreshold;
        return true;
    }

    private static bool TryGetLatestFiniteValue(IReadOnlyList<double> values, out double latest)
    {
        for (var index = values.Count - 1; index >= 0; index--)
        {
            if (double.IsFinite(values[index]))
            {
                latest = values[index];
                return true;
            }
        }

        latest = double.NaN;
        return false;
    }

    private static string BuildSplitProximitySummaryLabel(
        long targetEpoch,
        EpochSplitProximityRow row,
        IReadOnlyDictionary<string, SplitProximitySpeciesMeta> speciesMeta)
    {
        if (row.ValuesBySpecies is null || row.ValuesBySpecies.Count == 0)
        {
            return $"Split proximity (epoch {targetEpoch}): (n/a, no similarity scores)";
        }

        var selected = row.ValuesBySpecies
            .OrderBy(item => item.Value.MinProximity)
            .ThenBy(item => item.Key, StringComparer.OrdinalIgnoreCase)
            .First();
        var speciesId = selected.Key;
        var speciesName = speciesMeta.TryGetValue(speciesId, out var meta)
            ? meta.DisplayName
            : speciesId;
        var point = selected.Value;
        return
            $"Split proximity (epoch {targetEpoch}) min={FormatSignedDelta(point.MinProximity)} in {speciesName} " +
            $"[assignment sim {point.MinSimilarity:0.###} vs effective split {point.SplitThreshold:0.###}, samples={point.SampleCount}].";
    }

    private static string FormatSimilarityRange(ulong samples, double? min, double? max)
    {
        if (samples == 0 || !min.HasValue || !max.HasValue)
        {
            return "n/a";
        }

        return $"{min.Value:0.###}..{max.Value:0.###} ({samples.ToString(CultureInfo.InvariantCulture)})";
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
        var hasPoint = false;
        var finitePointCount = 0;
        var firstX = 0d;
        var firstY = 0d;
        var segmentOpen = false;
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (!double.IsFinite(value))
            {
                segmentOpen = false;
                continue;
            }

            var x = paddingX + (i * xStep);
            var ratio = Math.Clamp((value - boundedMin) / (boundedMax - boundedMin), 0d, 1d);
            var y = paddingY + ((1d - ratio) * usableHeight);
            builder.Append(segmentOpen ? " L " : "M ");
            builder.Append(x.ToString("0.###", CultureInfo.InvariantCulture));
            builder.Append(' ');
            builder.Append(y.ToString("0.###", CultureInfo.InvariantCulture));
            if (finitePointCount == 0)
            {
                firstX = x;
                firstY = y;
            }

            finitePointCount++;
            segmentOpen = true;
            hasPoint = true;
        }

        if (!hasPoint)
        {
            return string.Empty;
        }

        if (finitePointCount == 1)
        {
            var halfWidth = Math.Clamp(usableWidth * 0.01d, 1.5d, 6d);
            var minX = paddingX;
            var maxX = paddingX + usableWidth;
            var startX = Math.Max(minX, firstX - halfWidth);
            var endX = Math.Min(maxX, firstX + halfWidth);
            if (endX <= startX)
            {
                endX = Math.Min(maxX, startX + 1d);
            }

            return string.Create(
                CultureInfo.InvariantCulture,
                $"M {startX:0.###} {firstY:0.###} L {endX:0.###} {firstY:0.###}");
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

        unchecked
        {
            uint hash = 2166136261;
            foreach (var ch in speciesId)
            {
                hash ^= ch;
                hash *= 16777619;
            }

            var hue = (hash % 360u) / 360d;
            var saturation = (62d + ((hash >> 9) % 24u)) / 100d;
            var lightness = (42d + ((hash >> 17) % 18u)) / 100d;
            return HslToHex(hue, saturation, lightness);
        }
    }

    private static string HslToHex(double hue, double saturation, double lightness)
    {
        hue = hue - Math.Floor(hue);
        saturation = Math.Clamp(saturation, 0d, 1d);
        lightness = Math.Clamp(lightness, 0d, 1d);

        if (saturation <= 1e-6d)
        {
            var gray = ToByte(lightness);
            return $"#{gray:X2}{gray:X2}{gray:X2}";
        }

        var q = lightness < 0.5d
            ? lightness * (1d + saturation)
            : lightness + saturation - (lightness * saturation);
        var p = (2d * lightness) - q;

        var r = HueToRgb(p, q, hue + (1d / 3d));
        var g = HueToRgb(p, q, hue);
        var b = HueToRgb(p, q, hue - (1d / 3d));
        return $"#{ToByte(r):X2}{ToByte(g):X2}{ToByte(b):X2}";
    }

    private static double HueToRgb(double p, double q, double t)
    {
        if (t < 0d)
        {
            t += 1d;
        }
        else if (t > 1d)
        {
            t -= 1d;
        }

        if (t < (1d / 6d))
        {
            return p + ((q - p) * 6d * t);
        }

        if (t < 0.5d)
        {
            return q;
        }

        if (t < (2d / 3d))
        {
            return p + ((q - p) * ((2d / 3d) - t) * 6d);
        }

        return p;
    }

    private static byte ToByte(double value)
    {
        var clamped = Math.Clamp(value, 0d, 1d);
        return (byte)Math.Round(clamped * 255d, MidpointRounding.AwayFromZero);
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

    private static string FormatSignedDelta(double value)
    {
        if (!double.IsFinite(value))
        {
            return "n/a";
        }

        return value.ToString("+0.###;-0.###;0", CultureInfo.InvariantCulture);
    }

    private static double TransformSignedLogOrNan(double value)
    {
        if (!double.IsFinite(value))
        {
            return double.NaN;
        }

        return TransformSignedLog(value);
    }

    private static double TransformSignedLog(double value)
    {
        if (!double.IsFinite(value))
        {
            return 0d;
        }

        var magnitude = Math.Abs(value);
        if (magnitude <= 0d)
        {
            return 0d;
        }

        var transformedMagnitude = Math.Log10(1d + magnitude);
        return value < 0d ? -transformedMagnitude : transformedMagnitude;
    }

    private static string NormalizeSpeciesId(string? speciesId)
        => string.IsNullOrWhiteSpace(speciesId) ? "(unknown)" : speciesId.Trim();

    private static string NormalizeSpeciesName(string? speciesName, string speciesId)
        => string.IsNullOrWhiteSpace(speciesName) ? speciesId : speciesName.Trim();

    private static string BuildCompactSpeciesName(string? speciesName, string? speciesId)
    {
        var normalizedId = NormalizeSpeciesId(speciesId);
        var lineageCodeLabel = TryExtractLineageCodeLabel(speciesName);
        if (lineageCodeLabel.Length > 0)
        {
            return lineageCodeLabel;
        }

        if (!string.IsNullOrWhiteSpace(speciesName))
        {
            return speciesName.Trim();
        }

        var tokens = normalizedId
            .Split(['.', '-', '_', '/', ':'], StringSplitOptions.RemoveEmptyEntries)
            .ToList();
        if (tokens.Count > 1 && string.Equals(tokens[0], "species", StringComparison.OrdinalIgnoreCase))
        {
            tokens.RemoveAt(0);
        }

        if (tokens.Count > 1 && IsOpaqueSpeciesToken(tokens[^1]))
        {
            tokens.RemoveAt(tokens.Count - 1);
        }

        var parts = tokens
            .Select(FormatCompactSpeciesToken)
            .Where(part => part.Length > 0)
            .ToArray();
        if (parts.Length > 0)
        {
            return string.Join(' ', parts);
        }

        return normalizedId.Length <= 24
            ? normalizedId
            : normalizedId[..24] + "...";
    }

    private static string TryExtractLineageCodeLabel(string? speciesName)
    {
        if (string.IsNullOrWhiteSpace(speciesName))
        {
            return string.Empty;
        }

        var trimmed = speciesName.Trim();
        var openIndex = trimmed.LastIndexOf('[');
        if (openIndex < 0 || !trimmed.EndsWith("]", StringComparison.Ordinal) || openIndex >= trimmed.Length - 2)
        {
            return string.Empty;
        }

        var code = trimmed[(openIndex + 1)..^1].Trim();
        if (code.Length == 0)
        {
            return string.Empty;
        }

        return code.All(ch => char.IsDigit(ch) || (char.IsLetter(ch) && char.IsUpper(ch)))
            ? $"[{code}]"
            : string.Empty;
    }

    private static bool IsOpaqueSpeciesToken(string token)
    {
        var trimmed = token?.Trim() ?? string.Empty;
        if (trimmed.Length < 8)
        {
            return false;
        }

        var opaqueChars = trimmed.Count(ch => char.IsLetterOrDigit(ch));
        return opaqueChars == trimmed.Length && trimmed.Any(char.IsDigit);
    }

    private static string FormatCompactSpeciesToken(string token)
    {
        var trimmed = token?.Trim() ?? string.Empty;
        if (trimmed.Length == 0)
        {
            return string.Empty;
        }

        if (trimmed.Length == 1)
        {
            return char.ToUpperInvariant(trimmed[0]).ToString(CultureInfo.InvariantCulture);
        }

        return char.ToUpperInvariant(trimmed[0]) + trimmed[1..];
    }

    private static string FormatAxisNumber(double value)
    {
        if (!double.IsFinite(value))
        {
            return "(n/a)";
        }

        return Math.Abs(value - Math.Round(value)) < 0.0001d
            ? Math.Round(value).ToString("0", CultureInfo.InvariantCulture)
            : value.ToString("0.##", CultureInfo.InvariantCulture);
    }

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

    private static string NormalizeRunPressureModeToken(string? rawMode)
    {
        if (string.IsNullOrWhiteSpace(rawMode))
        {
            return "neutral";
        }

        return rawMode.Trim().ToLowerInvariant() switch
        {
            "divergence" => "divergence",
            "diverge" => "divergence",
            "explore" => "divergence",
            "exploratory" => "divergence",
            "neutral" => "neutral",
            "none" => "neutral",
            "off" => "neutral",
            "stability" => "stability",
            "stable" => "stability",
            "stabilize" => "stability",
            _ => "neutral"
        };
    }

    private static string NormalizeParentSelectionBiasToken(string? rawMode)
    {
        if (string.IsNullOrWhiteSpace(rawMode))
        {
            return "neutral";
        }

        return rawMode.Trim().ToLowerInvariant() switch
        {
            "divergence" => "divergence",
            "diverge" => "divergence",
            "explore" => "divergence",
            "exploratory" => "divergence",
            "neutral" => "neutral",
            "none" => "neutral",
            "off" => "neutral",
            "stability" => "stability",
            "stable" => "stability",
            "stabilize" => "stability",
            _ => "neutral"
        };
    }

    private static List<SpeciationMembershipRecord> OrderHistoryForSampling(IReadOnlyList<SpeciationMembershipRecord> history)
    {
        return history
            .OrderBy(entry => (long)entry.EpochId)
            .ThenBy(entry => entry.AssignedMs)
            .ThenBy(entry => entry.BrainId is not null && entry.BrainId.TryToGuid(out var brainId) ? brainId : Guid.Empty)
            .ToList();
    }

    private static bool ShouldUseSingleEpochRowSampling(IReadOnlyList<SpeciationMembershipRecord> orderedHistory)
    {
        if (orderedHistory.Count <= 1)
        {
            return false;
        }

        var epochId = (long)orderedHistory[0].EpochId;
        for (var i = 1; i < orderedHistory.Count; i++)
        {
            if ((long)orderedHistory[i].EpochId != epochId)
            {
                return false;
            }
        }

        return true;
    }

    private static int ParseInt(string raw, int fallback)
    {
        return int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static int RoundToNonNegativeInt(double raw, int fallback)
    {
        if (double.IsNaN(raw) || double.IsInfinity(raw))
        {
            return Math.Max(0, fallback);
        }

        return (int)Math.Max(0d, Math.Round(raw, MidpointRounding.AwayFromZero));
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

    private static bool ParseBool(string raw, bool fallback)
    {
        if (bool.TryParse(raw, out var parsed))
        {
            return parsed;
        }

        return raw.Trim() switch
        {
            "1" => true,
            "0" => false,
            _ => fallback
        };
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

    private static string? ReadLatestNonEmptyLine(string path, ref long position, ref string? lastNonEmptyLine)
    {
        try
        {
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            if (position < 0 || position > stream.Length)
            {
                position = 0;
            }

            if (stream.Length <= position)
            {
                return lastNonEmptyLine;
            }

            stream.Seek(position, SeekOrigin.Begin);
            using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
            var chunk = reader.ReadToEnd();
            position = stream.Position;
            if (!string.IsNullOrWhiteSpace(chunk))
            {
                foreach (var line in chunk.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries))
                {
                    var trimmed = line.Trim();
                    if (!string.IsNullOrWhiteSpace(trimmed))
                    {
                        lastNonEmptyLine = trimmed;
                    }
                }
            }
        }
        catch
        {
        }

        return lastNonEmptyLine;
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
                AssessmentSimilaritySamples: root.TryGetProperty("assessment_similarity_samples", out var assessmentSamplesNode)
                    ? assessmentSamplesNode.GetUInt64()
                    : (root.TryGetProperty("similarity_samples", out var overallSamplesNode) ? overallSamplesNode.GetUInt64() : 0UL),
                MinAssessmentSimilarityObserved: TryGetJsonDouble(root, "min_assessment_similarity_observed", out var minAssessmentSimilarityObserved)
                    ? minAssessmentSimilarityObserved
                    : (TryGetJsonDouble(root, "min_similarity_observed", out var legacyMinAssessmentSimilarityObserved) ? legacyMinAssessmentSimilarityObserved : null),
                MaxAssessmentSimilarityObserved: TryGetJsonDouble(root, "max_assessment_similarity_observed", out var maxAssessmentSimilarityObserved)
                    ? maxAssessmentSimilarityObserved
                    : (TryGetJsonDouble(root, "max_similarity_observed", out var legacyMaxAssessmentSimilarityObserved) ? legacyMaxAssessmentSimilarityObserved : null),
                ReproductionSimilaritySamples: root.TryGetProperty("reproduction_similarity_samples", out var reproductionSamplesNode)
                    ? reproductionSamplesNode.GetUInt64()
                    : 0UL,
                MinReproductionSimilarityObserved: TryGetJsonDouble(root, "min_reproduction_similarity_observed", out var minReproductionSimilarityObserved)
                    ? minReproductionSimilarityObserved
                    : null,
                MaxReproductionSimilarityObserved: TryGetJsonDouble(root, "max_reproduction_similarity_observed", out var maxReproductionSimilarityObserved)
                    ? maxReproductionSimilarityObserved
                    : null,
                SpeciationCommitSimilaritySamples: root.TryGetProperty("speciation_commit_similarity_samples", out var commitSamplesNode)
                    ? commitSamplesNode.GetUInt64()
                    : 0UL,
                MinSpeciationCommitSimilarityObserved: TryGetJsonDouble(root, "min_speciation_commit_similarity_observed", out var minCommitSimilarityObserved)
                    ? minCommitSimilarityObserved
                    : null,
                MaxSpeciationCommitSimilarityObserved: TryGetJsonDouble(root, "max_speciation_commit_similarity_observed", out var maxCommitSimilarityObserved)
                    ? maxCommitSimilarityObserved
                    : null,
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
        string MidEpochLabel,
        string EndEpochLabel,
        int LegendColumns,
        IReadOnlyList<SpeciationFlowChartAreaItem> Areas,
        IReadOnlyList<SpeciationChartLegendItem> Legend);

    private readonly record struct SplitProximityChartSnapshot(
        string RangeLabel,
        string MetricLabel,
        string YTopLabel,
        string YMidLabel,
        string YBottomLabel,
        int LegendColumns,
        string CurrentEpochSummaryLabel,
        IReadOnlyList<SpeciationLineChartSeriesItem> Series,
        IReadOnlyList<SpeciationChartLegendItem> Legend)
    {
        public static SplitProximityChartSnapshot Empty(string summaryLabel)
            => new(
                RangeLabel: "Epochs: (no data)",
                MetricLabel: "Min lineage similarity minus effective split threshold per species (signed log10(1+|delta|) y-axis; <=0 means split-trigger zone).",
                YTopLabel: "0",
                YMidLabel: "0",
                YBottomLabel: "0",
                LegendColumns: 2,
                CurrentEpochSummaryLabel: summaryLabel,
                Series: Array.Empty<SpeciationLineChartSeriesItem>(),
                Legend: Array.Empty<SpeciationChartLegendItem>());
    }

    private readonly record struct CladogramSnapshot(
        string RangeLabel,
        string MetricLabel,
        IReadOnlyList<SpeciationCladogramItem> Items)
    {
        public static CladogramSnapshot Empty(string rangeLabel)
            => new(
                RangeLabel: rangeLabel,
                MetricLabel: "Parent -> child lineage edges inferred from divergence decisions.",
                Items: Array.Empty<SpeciationCladogramItem>());
    }

    private readonly record struct EpochPopulationRow(
        long EpochId,
        Dictionary<string, int> Counts,
        int TotalCount);

    private readonly record struct SpeciesPopulationMeta(
        string SpeciesId,
        string DisplayName,
        int TotalCount);

    private readonly record struct EpochSplitProximityRow(
        long EpochId,
        Dictionary<string, SplitProximitySpeciesPoint> ValuesBySpecies);

    private readonly record struct SplitProximitySpeciesPoint(
        double MinProximity,
        double MinSimilarity,
        double SplitThreshold,
        int SampleCount);

    private readonly record struct SplitProximitySpeciesMeta(
        string SpeciesId,
        string DisplayName,
        int SampleCount);

    private readonly record struct CladogramSpeciesMeta(
        string SpeciesId,
        string DisplayName);

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
        ulong AssessmentSimilaritySamples,
        double? MinAssessmentSimilarityObserved,
        double? MaxAssessmentSimilarityObserved,
        ulong ReproductionSimilaritySamples,
        double? MinReproductionSimilarityObserved,
        double? MaxReproductionSimilarityObserved,
        ulong SpeciationCommitSimilaritySamples,
        double? MinSpeciationCommitSimilarityObserved,
        double? MaxSpeciationCommitSimilarityObserved,
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

public sealed class SpeciationCladogramItem : ViewModelBase
{
    private bool _isExpanded;

    public SpeciationCladogramItem(
        string speciesId,
        string speciesDisplayName,
        string detailLabel,
        string color,
        bool isRoot,
        IReadOnlyList<SpeciationCladogramItem>? children = null,
        bool isExpanded = true)
    {
        SpeciesId = speciesId;
        SpeciesDisplayName = speciesDisplayName;
        DetailLabel = detailLabel;
        Color = color;
        IsRoot = isRoot;
        Children = new ObservableCollection<SpeciationCladogramItem>(children ?? Array.Empty<SpeciationCladogramItem>());
        _isExpanded = isExpanded;
    }

    public string SpeciesId { get; }

    public string SpeciesDisplayName { get; }

    public string DetailLabel { get; }

    public string Color { get; }

    public bool IsRoot { get; }

    public string RoleLabel => IsRoot ? "Root lineage" : "Derived lineage";

    public string LineText => $"{SpeciesDisplayName} [{SpeciesId}]";

    public ObservableCollection<SpeciationCladogramItem> Children { get; }

    public bool HasChildren => Children.Count > 0;

    public bool IsLeaf => Children.Count == 0;

    public bool IsExpanded
    {
        get => _isExpanded;
        set => SetProperty(ref _isExpanded, value);
    }
}

public sealed record SpeciationSimulatorBrainOption(Guid BrainId, string Label)
{
    public string BrainIdLabel => BrainId.ToString("D");
}

public sealed record SpeciationSimulatorSeedParentItem(Guid BrainId, string Label, string Source)
{
    public string BrainIdLabel => BrainId.ToString("D");
}
