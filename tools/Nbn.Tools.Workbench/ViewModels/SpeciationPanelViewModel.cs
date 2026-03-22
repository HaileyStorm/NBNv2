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

/// <summary>
/// Coordinates Workbench speciation status, configuration, charts, and simulator controls.
/// </summary>
public sealed partial class SpeciationPanelViewModel : ViewModelBase, IAsyncDisposable
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
    private const double FlowChartHoverCardOffset = 14d;
    private const double FlowChartHoverCardMaxWidth = 320d;
    private const double FlowChartHoverCardMaxHeight = 140d;
    private const int PopulationChartTopSpeciesLimit = 12;
    private const int FlowChartTopSpeciesLimit = 11;
    private const int ExpandedFlowChartTopSpeciesLimit = 23;
    private const int ExpandedFlowChartUltraWideTopSpeciesLimit = 47;
    private const double ExpandedFlowChartWideWindowThreshold = 1700d;
    private const double ExpandedFlowChartDefaultPlotWidth = 1240d;
    private const double ExpandedFlowChartDefaultPlotHeight = 720d;
    private const double ExpandedFlowChartMinPlotWidth = 720d;
    private const double ExpandedFlowChartMinPlotHeight = 360d;
    private const int SpeciesColorPickerOptionCount = 36;
    private const int SplitProximityTopSpeciesLimit = 12;
    private const double AdjacentSpeciesColorMinDistance = 72d;
    private const int SpeciesColorRecentWindow = 3;
    private const double SpeciesColorHueRetryStep = 0.3819660112501051d;
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
        "#0072B2",
        "#D55E00",
        "#009E73",
        "#CC79A7",
        "#F0E442",
        "#56B4E9",
        "#E69F00",
        "#332288",
        "#117733",
        "#AA4499",
        "#88CCEE",
        "#DDCC77",
        "#CC6677",
        "#44AA99",
        "#999933",
        "#882255",
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
        "#577590",
        "#2E86AB",
        "#F2542D",
        "#3FA34D",
        "#F7B267",
        "#7B2CBF",
        "#0FA3B1",
        "#C8553D",
        "#8E9A46",
        "#FF006E",
        "#5C4D7D",
        "#00B4D8",
        "#6B705C",
        "#4361EE",
        "#FF7F11",
        "#2DC653",
        "#FF9F1C",
        "#8338EC",
        "#06D6A0",
        "#EF476F",
        "#118AB2",
        "#FFD166",
        "#073B4C",
        "#FB5607",
        "#3A86FF",
        "#FFBE0B",
        "#8AC926",
        "#FF595E",
        "#1982C4",
        "#6A4C93",
        "#C9184A",
        "#4CC9F0",
        "#F72585"
    ];
    private static readonly IReadOnlyList<SpeciationColorPickerSwatchItem> SpeciesChartColorPickerPalette = BuildSpeciesColorPickerPalette();

    private readonly UiDispatcher _dispatcher;
    private readonly ConnectionViewModel _connections;
    private readonly WorkbenchClient _client;
    private readonly ILocalProjectLaunchPreparer _launchPreparer;
    private readonly ILocalFirewallManager _firewallManager;
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
    private string _simBindHost = NetworkAddressDefaults.DefaultBindHost;
    private string _simPortText = "12074";
    private string _simSeedText = "12345";
    private string _simIntervalMsText = "100";
    private string _simStatusSecondsText = "2";
    private string _simTimeoutSecondsText = "45";
    private string _simMaxIterationsText = "0";
    private string _simMaxParentPoolText = "512";
    private string _simMinRunsText = "2";
    private string _simMaxRunsText = "12";
    private string _simGammaText = "1";
    private string _simRunPressureMode = "divergence";
    private string _simParentSelectionBias = "stability";
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
    private bool _includeNewestSpeciesInFlowChart = true;
    private string _flowChartHoverCardTitle = string.Empty;
    private string _flowChartHoverCardDetail = string.Empty;
    private string _flowChartHoverCardText = string.Empty;
    private string _flowChartHoverCardSwatchColor = "Transparent";
    private bool _isFlowChartHoverCardVisible;
    private double _flowChartHoverCardLeft = 8d;
    private double _flowChartHoverCardTop = 8d;
    private string _expandedFlowChartRangeLabel = "Epochs: (no data)";
    private string _expandedFlowChartStartEpochLabel = "(n/a)";
    private string _expandedFlowChartMidEpochLabel = "(n/a)";
    private string _expandedFlowChartEndEpochLabel = "(n/a)";
    private int _expandedFlowChartLegendColumns = 4;
    private double _expandedFlowChartPlotWidth = ExpandedFlowChartDefaultPlotWidth;
    private double _expandedFlowChartPlotHeight = ExpandedFlowChartDefaultPlotHeight;
    private double _expandedFlowChartWindowWidth = ExpandedFlowChartDefaultPlotWidth;
    private string _expandedFlowChartHoverCardTitle = string.Empty;
    private string _expandedFlowChartHoverCardDetail = string.Empty;
    private string _expandedFlowChartHoverCardText = string.Empty;
    private string _expandedFlowChartHoverCardSwatchColor = "Transparent";
    private bool _isExpandedFlowChartHoverCardVisible;
    private double _expandedFlowChartHoverCardLeft = 8d;
    private double _expandedFlowChartHoverCardTop = 8d;
    private string _splitProximityChartRangeLabel = "Epochs: (no data)";
    private string _splitProximityChartMetricLabel = "Min lineage similarity minus effective split threshold per species (signed log10(1+|delta|) y-axis; <=0 means split-trigger zone).";
    private string _splitProximityChartYAxisTopLabel = "0";
    private string _splitProximityChartYAxisMidLabel = "0";
    private string _splitProximityChartYAxisBottomLabel = "0";
    private int _splitProximityChartLegendColumns = 2;
    private string _cladogramRangeLabel = "Cladogram: (no data)";
    private string _cladogramMetricLabel = "Parent -> child lineage edges inferred from divergence decisions.";
    private string _cladogramKeyLabel = "Key: color strip = species color; each node shows species name + id with membership and direct-derived counts; root badges mark inferred root lineages. New species auto-expand their branch.";
    private readonly Dictionary<string, string> _speciesColorOverrides = new(StringComparer.OrdinalIgnoreCase);
    private FlowChartSourceFrame? _lastFlowChartSource;
    private SpeciationChartSourceFrame? _lastSpeciationChartSource;
    private FlowChartHoverState? _lastFlowChartHoverState;
    private FlowChartHoverState? _lastExpandedFlowChartHoverState;

    /// <summary>
    /// Initializes the Workbench speciation panel with service access, local launch helpers, and UI callbacks.
    /// </summary>
    public SpeciationPanelViewModel(
        UiDispatcher dispatcher,
        ConnectionViewModel connections,
        WorkbenchClient client,
        Func<Task>? startSpeciationService = null,
        Func<Task>? stopSpeciationService = null,
        Func<Task>? refreshOrchestrator = null,
        bool enableLiveChartsAutoRefresh = true,
        ILocalProjectLaunchPreparer? launchPreparer = null,
        ILocalFirewallManager? firewallManager = null)
    {
        _dispatcher = dispatcher;
        _connections = connections;
        _client = client;
        _launchPreparer = launchPreparer ?? new LocalProjectLaunchPreparer();
        _firewallManager = firewallManager ?? new LocalFirewallManager();
        _startSpeciationService = startSpeciationService;
        _stopSpeciationService = stopSpeciationService;
        _refreshOrchestrator = refreshOrchestrator;
        _enableLiveChartsAutoRefresh = enableLiveChartsAutoRefresh;
        _simBindHost = NetworkAddressDefaults.DefaultBindHost;
        _connections.PropertyChanged += OnConnectionsPropertyChanged;

        SpeciesCounts = new ObservableCollection<SpeciationSpeciesCountItem>();
        EpochSummaries = new ObservableCollection<SpeciationEpochSummaryItem>();
        SimActiveBrains = new ObservableCollection<SpeciationSimulatorBrainOption>();
        SimSeedParents = new ObservableCollection<SpeciationSimulatorSeedParentItem>();
        PopulationChartSeries = new ObservableCollection<SpeciationLineChartSeriesItem>();
        PopulationChartLegend = new ObservableCollection<SpeciationChartLegendItem>();
        FlowChartAreas = new ObservableCollection<SpeciationFlowChartAreaItem>();
        FlowChartLegend = new ObservableCollection<SpeciationChartLegendItem>();
        ExpandedFlowChartAreas = new ObservableCollection<SpeciationFlowChartAreaItem>();
        ExpandedFlowChartLegend = new ObservableCollection<SpeciationChartLegendItem>();
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
        AddAllSimSeedParentsCommand = new RelayCommand(AddAllSimulatorSeedParents);
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
    public ObservableCollection<SpeciationFlowChartAreaItem> ExpandedFlowChartAreas { get; }
    public ObservableCollection<SpeciationChartLegendItem> ExpandedFlowChartLegend { get; }
    public ObservableCollection<SpeciationLineChartSeriesItem> SplitProximityChartSeries { get; }
    public ObservableCollection<SpeciationChartLegendItem> SplitProximityChartLegend { get; }
    public ObservableCollection<SpeciationCladogramItem> CladogramItems { get; }
    public IReadOnlyList<SpeciationColorPickerSwatchItem> SpeciesColorPickerPalette => SpeciesChartColorPickerPalette;

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
    public RelayCommand AddAllSimSeedParentsCommand { get; }
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

    public bool IncludeNewestSpeciesInFlowChart
    {
        get => _includeNewestSpeciesInFlowChart;
        set
        {
            if (SetProperty(ref _includeNewestSpeciesInFlowChart, value))
            {
                RefreshFlowChartsFromLatestSource();
            }
        }
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

    public string FlowChartHoverCardText
    {
        get => _flowChartHoverCardText;
        set => SetProperty(ref _flowChartHoverCardText, value);
    }

    public string FlowChartHoverCardTitle
    {
        get => _flowChartHoverCardTitle;
        set => SetProperty(ref _flowChartHoverCardTitle, value);
    }

    public string FlowChartHoverCardDetail
    {
        get => _flowChartHoverCardDetail;
        set => SetProperty(ref _flowChartHoverCardDetail, value);
    }

    public string FlowChartHoverCardSwatchColor
    {
        get => _flowChartHoverCardSwatchColor;
        set => SetProperty(ref _flowChartHoverCardSwatchColor, value);
    }

    public bool IsFlowChartHoverCardVisible
    {
        get => _isFlowChartHoverCardVisible;
        set => SetProperty(ref _isFlowChartHoverCardVisible, value);
    }

    public double FlowChartHoverCardLeft
    {
        get => _flowChartHoverCardLeft;
        set => SetProperty(ref _flowChartHoverCardLeft, value);
    }

    public double FlowChartHoverCardTop
    {
        get => _flowChartHoverCardTop;
        set => SetProperty(ref _flowChartHoverCardTop, value);
    }

    public string ExpandedFlowChartRangeLabel
    {
        get => _expandedFlowChartRangeLabel;
        set => SetProperty(ref _expandedFlowChartRangeLabel, value);
    }

    public string ExpandedFlowChartStartEpochLabel
    {
        get => _expandedFlowChartStartEpochLabel;
        set => SetProperty(ref _expandedFlowChartStartEpochLabel, value);
    }

    public string ExpandedFlowChartMidEpochLabel
    {
        get => _expandedFlowChartMidEpochLabel;
        set => SetProperty(ref _expandedFlowChartMidEpochLabel, value);
    }

    public string ExpandedFlowChartEndEpochLabel
    {
        get => _expandedFlowChartEndEpochLabel;
        set => SetProperty(ref _expandedFlowChartEndEpochLabel, value);
    }

    public int ExpandedFlowChartLegendColumns
    {
        get => _expandedFlowChartLegendColumns;
        set => SetProperty(ref _expandedFlowChartLegendColumns, value);
    }

    public string ExpandedFlowChartHoverCardText
    {
        get => _expandedFlowChartHoverCardText;
        set => SetProperty(ref _expandedFlowChartHoverCardText, value);
    }

    public string ExpandedFlowChartHoverCardTitle
    {
        get => _expandedFlowChartHoverCardTitle;
        set => SetProperty(ref _expandedFlowChartHoverCardTitle, value);
    }

    public string ExpandedFlowChartHoverCardDetail
    {
        get => _expandedFlowChartHoverCardDetail;
        set => SetProperty(ref _expandedFlowChartHoverCardDetail, value);
    }

    public string ExpandedFlowChartHoverCardSwatchColor
    {
        get => _expandedFlowChartHoverCardSwatchColor;
        set => SetProperty(ref _expandedFlowChartHoverCardSwatchColor, value);
    }

    public bool IsExpandedFlowChartHoverCardVisible
    {
        get => _isExpandedFlowChartHoverCardVisible;
        set => SetProperty(ref _isExpandedFlowChartHoverCardVisible, value);
    }

    public double ExpandedFlowChartHoverCardLeft
    {
        get => _expandedFlowChartHoverCardLeft;
        set => SetProperty(ref _expandedFlowChartHoverCardLeft, value);
    }

    public double ExpandedFlowChartHoverCardTop
    {
        get => _expandedFlowChartHoverCardTop;
        set => SetProperty(ref _expandedFlowChartHoverCardTop, value);
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

}
