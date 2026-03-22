using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Platform.Storage;
using Nbn.Proto.Io;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class VizPanelViewModel : ViewModelBase
{
    public enum EmptyCanvasDoubleClickAction
    {
        ResetView = 0,
        ShowFullBrain = 1
    }

    private const int MaxEvents = 400;
    private const int MaxProjectionEvents = 24000;
    private const int MaxEventsPerUiFlush = 96;
    private const int MaxPendingEvents = 1600;
    private const int DefaultTickWindow = 64;
    private const int MaxTickWindow = 4096;
    private const int MinLodRouteBudget = 16;
    private const int MaxLodRouteBudget = 4096;
    private const int DefaultLodLowZoomBudget = 120;
    private const int DefaultLodMediumZoomBudget = 220;
    private const int DefaultLodHighZoomBudget = 360;
    private const uint DefaultVizTickMinIntervalMs = 250u;
    private const uint DefaultVizStreamMinIntervalMs = 250u;
    private const uint MinVisualizationIntervalMs = 100u;
    private const uint MaxVisualizationIntervalMs = 3_000u;
    private const double TickSliderMinMs = 1d;
    private const double TickSliderMaxMs = 1000d;
    private const double VizSliderMinMs = MinVisualizationIntervalMs;
    private const double VizSliderMaxMs = MaxVisualizationIntervalMs;
    private const string DefaultTickOverrideSummary = "Tick cadence control is not set.";
    private const string DefaultTickCadenceSummary = "Current cadence: awaiting HiveMind status.";
    private const int EmptyBrainRefreshClearThreshold = 3;
    private const int SelectionMissRefreshClearThreshold = 3;
    private const int SnapshotRegionRows = 10;
    private const int SnapshotEdgeRows = 14;
    private const int DefaultMiniActivityTopN = 8;
    private const int MinMiniActivityTopN = 1;
    private const int MaxMiniActivityTopN = 32;
    private const double DefaultMiniActivityRangeSeconds = 3d;
    private const double MinMiniActivityRangeSeconds = 0.25d;
    private const double MaxMiniActivityRangeSeconds = 60d;
    private const float DefaultMiniActivityTickRateHz = 20f;
    private const double MiniActivityChartPlotWidth = 220;
    private const double MiniActivityChartPlotHeight = 88;
    private const double MiniActivityChartPlotPaddingX = 6;
    private const double MiniActivityChartPlotPaddingY = 6;
    private const double MiniActivityChartPathInsetPx = 0.8;
    private const double MiniActivityChartOverlayWidthPx = 312;
    private static readonly bool LogVizDiagnostics = IsEnvTrue("NBN_VIZ_DIAGNOSTICS_ENABLED");
    private static readonly TimeSpan StreamingRefreshInterval = TimeSpan.FromMilliseconds(180);
    private static readonly TimeSpan DefinitionHydrationRetryInterval = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan SelectedBrainEnergyRefreshInterval = TimeSpan.FromMilliseconds(750);
    private static readonly string[] MiniActivityChartSeriesPalette =
    {
        "#2A9D8F",
        "#E76F51",
        "#457B9D",
        "#E9C46A",
        "#264653",
        "#F4A261",
        "#8AB17D",
        "#B56576",
        "#6D597A",
        "#F28482",
        "#4D908E",
        "#90BE6D"
    };
    private const int HoverClearDelayMs = 220;
    private const int EdgeHitSamples = 12;
    private const double HitTestCellSize = 54;
    private const double EdgeHitIndexPadding = 4;
    private const double NodeHitPadding = 2.5;
    private const double StickyNodeHitPadding = 7;
    private const double StickyEdgeHitPadding = 8;
    private const double HoverNodeHitPadding = 0.8;
    private const double HoverStickyNodeHitPadding = 3.8;
    private const double HoverStickyEdgeHitPadding = 4.2;
    private const double HoverEdgeHitThresholdScale = 0.34;
    private const double HoverEdgeHitThresholdMin = 2.4;
    private const double HitDistanceTieEpsilon = 0.05;
    private const double HoverCardOffset = 14;
    private const double HoverCardMaxWidth = 420;
    private const double HoverCardMaxHeight = 220;
    private const double CanvasBoundsPadding = 28;
    private const double CanvasNavigationPadding = 220;
    private const double CanvasDimensionJitterTolerance = 24;
    private readonly UiDispatcher _dispatcher;
    private readonly IoPanelViewModel _brain;
    private readonly List<VizEventItem> _allEvents = new();
    private readonly List<VizEventItem> _projectionEvents = new();
    private IReadOnlyList<VizEventItem> _filteredProjectionEvents = Array.Empty<VizEventItem>();
    private readonly Queue<VizEventItem> _pendingEvents = new();
    private readonly object _pendingEventsGate = new();
    private bool _flushScheduled;
    private string _status = "Streaming";
    private string _regionFocusText = string.Empty;
    private string _regionFilterText = string.Empty;
    private string _searchFilterText = string.Empty;
    private string _brainEntryText = string.Empty;
    private string _selectedBrainEnergySummary = "Selected brain energy: n/a (no brain selected).";
    private BrainListItem? _selectedBrain;
    private VizPanelTypeOption _selectedVizType;
    private VizCanvasColorModeOption _selectedCanvasColorMode;
    private VizCanvasTransferCurveOption _selectedCanvasTransferCurve;
    private VizCanvasLayoutModeOption _selectedLayoutMode;
    private bool _suspendSelection;
    private VizEventItem? _selectedEvent;
    private string _selectedPayload = string.Empty;
    private string _tickWindowText = DefaultTickWindow.ToString(CultureInfo.InvariantCulture);
    private string _tickRateOverrideText = string.Empty;
    private string _tickRateOverrideSummary = DefaultTickOverrideSummary;
    private string _tickCadenceSummary = DefaultTickCadenceSummary;
    private double _tickCadenceSliderMs = 100d;
    private bool _tickCadenceSliderSyncInProgress;
    private bool _tickCadenceTextSyncInProgress;
    private string _vizCadenceText = FormattableString.Invariant($"{DefaultVizStreamMinIntervalMs}ms");
    private double _vizCadenceSliderMs = DefaultVizStreamMinIntervalMs;
    private bool _vizCadenceSliderSyncInProgress;
    private bool _vizCadenceTextSyncInProgress;
    private uint _vizTickMinIntervalMs = DefaultVizTickMinIntervalMs;
    private uint _vizStreamMinIntervalMs = DefaultVizStreamMinIntervalMs;
    private string _vizCadenceSummary = BuildVisualizationCadenceSummary(DefaultVizTickMinIntervalMs, DefaultVizStreamMinIntervalMs);
    private bool _includeLowSignalEvents;
    private bool _enableAdaptiveLod = true;
    private string _lodLowZoomBudgetText = DefaultLodLowZoomBudget.ToString(CultureInfo.InvariantCulture);
    private string _lodMediumZoomBudgetText = DefaultLodMediumZoomBudget.ToString(CultureInfo.InvariantCulture);
    private string _lodHighZoomBudgetText = DefaultLodHighZoomBudget.ToString(CultureInfo.InvariantCulture);
    private string _lodSummary = $"Adaptive LOD enabled (routes low/med/high: {DefaultLodLowZoomBudget}/{DefaultLodMediumZoomBudget}/{DefaultLodHighZoomBudget}).";
    private string _activitySummary = "Awaiting visualization events.";
    private string _activityCanvasLegend = "Canvas renderer awaiting activity.";
    private bool _showMiniActivityChart = true;
    private string _miniActivityTopNText = DefaultMiniActivityTopN.ToString(CultureInfo.InvariantCulture);
    private string _miniActivityRangeSecondsText = DefaultMiniActivityRangeSeconds.ToString("0.###", CultureInfo.InvariantCulture);
    private string _miniActivityChartSeriesLabel = $"Top {DefaultMiniActivityTopN} regions by score.";
    private string _miniActivityChartRangeLabel = "Ticks: awaiting activity.";
    private string _miniActivityChartMetricLabel = "score = 1 + |value| + |strength| per event contribution | y-axis log(1+score)";
    private string _miniActivityYAxisTopLabel = "0";
    private string _miniActivityYAxisMidLabel = "0";
    private string _miniActivityYAxisBottomLabel = "0";
    private int _miniActivityLegendColumns = 2;
    private double _activityCanvasWidth = VizActivityCanvasLayoutBuilder.CanvasWidth;
    private double _activityCanvasHeight = VizActivityCanvasLayoutBuilder.CanvasHeight;
    private bool _showProjectionSnapshot;
    private bool _showVisualizationStream;
    private DateTime _nextStreamingRefreshUtc = DateTime.MinValue;
    private ulong _lastRenderedTickId;
    private ulong _latestObservedGlobalTickId;
    private string? _selectedCanvasNodeKey;
    private string? _selectedCanvasRouteLabel;
    private string? _hoverCanvasNodeKey;
    private string? _hoverCanvasRouteLabel;
    private readonly HashSet<string> _pinnedCanvasNodes = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _pinnedCanvasRoutes = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<Guid, BrainCanvasTopologyState> _topologyByBrainId = new();
    private readonly SemaphoreSlim _definitionTopologyGate = new(1, 1);
    private readonly object _pendingDefinitionHydrationGate = new();
    private readonly HashSet<string> _pendingDefinitionHydrationKeys = new(StringComparer.OrdinalIgnoreCase);
    private VizActivityProjection? _currentProjection;
    private VizActivityProjectionOptions _currentProjectionOptions = new(DefaultTickWindow, false, null);
    private string _activityInteractionSummary = "Select a node or route to inspect activity details.";
    private string _activityPinnedSummary = "Pinned: none.";
    private bool _hasCanvasSelection;
    private bool _isCanvasSelectionExpanded;
    private string _canvasSelectionTitle = "Selected: none";
    private string _canvasSelectionIdentity = "Identity: none.";
    private string _canvasSelectionRuntime = "Runtime: n/a.";
    private string _canvasSelectionContext = "Context: n/a.";
    private string _canvasSelectionDetail = "Select a node or route to inspect identity, runtime stats, and route context.";
    private string _canvasSelectionActionHint = "Runtime actions target selected neuron nodes or route endpoints (N...) with an active brain.";
    private string _selectedInputPulseValueText = "1";
    private string _selectedBufferValueText = string.Empty;
    private string _selectedAccumulatorValueText = string.Empty;
    private uint? _selectedRouteSourceRegionId;
    private uint? _selectedRouteTargetRegionId;
    private string _canvasHoverCardText = string.Empty;
    private bool _isCanvasHoverCardVisible;
    private double _canvasHoverCardLeft = 8;
    private double _canvasHoverCardTop = 8;
    private int _hoverClearRevision;
    private IReadOnlyList<VizActivityCanvasNode> _canvasNodeSnapshot = Array.Empty<VizActivityCanvasNode>();
    private IReadOnlyList<VizActivityCanvasEdge> _canvasEdgeSnapshot = Array.Empty<VizActivityCanvasEdge>();
    private readonly Dictionary<long, List<int>> _nodeHitIndex = new();
    private readonly Dictionary<long, List<int>> _edgeHitIndex = new();
    private readonly HashSet<int> _nodeHitCandidates = new();
    private readonly HashSet<int> _edgeHitCandidates = new();
    private readonly Dictionary<string, VizActivityCanvasNode> _canvasNodeByKey = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, VizActivityCanvasEdge> _canvasEdgeByRoute = new(StringComparer.OrdinalIgnoreCase);
    private double _lastProjectionBuildMs;
    private double _lastCanvasLayoutBuildMs;
    private double _lastCanvasApplyMs;
    private double _lastCanvasFrameMs;
    private DateTime _nextDefinitionHydrationRetryUtc = DateTime.MinValue;
    private double _lastFlushBatchMs;
    private int _lastFlushBatchCount;
    private int _maxObservedPendingEvents;
    private long _droppedPendingEvents;
    private double _lastHitTestMs;
    private double _avgHitTestMs;
    private double _maxHitTestMs;
    private long _hitTestSamples;
    private CollectionDiffStats _lastCanvasNodeDiffStats;
    private CollectionDiffStats _lastCanvasEdgeDiffStats;
    private double _canvasViewportScale = 1.0;
    private CancellationTokenSource? _projectionLayoutRefreshCts;
    private int _projectionLayoutRefreshVersion;
    private Guid? _preferredBrainId;
    private int _consecutiveEmptyBrainRefreshes;
    private int _consecutiveSelectionMissRefreshes;
    private float? _currentTargetTickHz;
    private ulong? _miniChartMinTickFloor;
    private DateTime _nextSelectedBrainEnergyRefreshUtc = DateTime.MinValue;
    private int _selectedBrainEnergyRefreshInFlight;

    public VizPanelViewModel(UiDispatcher dispatcher, IoPanelViewModel brain)
    {
        _dispatcher = dispatcher;
        _brain = brain;
        VizEvents = new ObservableCollection<VizEventItem>();
        ActivityStats = new ObservableCollection<VizActivityStatItem>();
        RegionActivity = new ObservableCollection<VizRegionActivityItem>();
        EdgeActivity = new ObservableCollection<VizEdgeActivityItem>();
        TickActivity = new ObservableCollection<VizTickActivityItem>();
        MiniActivityChartSeries = new ObservableCollection<VizMiniActivityChartSeriesItem>();
        CanvasNodes = new ObservableCollection<VizActivityCanvasNode>();
        CanvasEdges = new ObservableCollection<VizActivityCanvasEdge>();
        KnownBrains = new ObservableCollection<BrainListItem>();
        VizPanelTypeOptions = new ObservableCollection<VizPanelTypeOption>(VizPanelTypeOption.CreateDefaults());
        _selectedVizType = VizPanelTypeOptions[0];
        CanvasColorModeOptions = new ObservableCollection<VizCanvasColorModeOption>(VizCanvasColorModeOption.CreateDefaults());
        _selectedCanvasColorMode = CanvasColorModeOptions[0];
        CanvasTransferCurveOptions = new ObservableCollection<VizCanvasTransferCurveOption>(VizCanvasTransferCurveOption.CreateDefaults());
        _selectedCanvasTransferCurve = CanvasTransferCurveOptions[0];
        LayoutModeOptions = new ObservableCollection<VizCanvasLayoutModeOption>(VizCanvasLayoutModeOption.CreateDefaults());
        _selectedLayoutMode = LayoutModeOptions[0];
        ClearCommand = new RelayCommand(Clear);
        AddBrainCommand = new RelayCommand(AddBrainFromEntry);
        ZoomCommand = new RelayCommand(ZoomRegion);
        ShowFullBrainCommand = new RelayCommand(ShowFullBrain);
        ToggleProjectionSnapshotCommand = new RelayCommand(() => ShowProjectionSnapshot = !ShowProjectionSnapshot);
        ToggleVisualizationStreamCommand = new RelayCommand(() => ShowVisualizationStream = !ShowVisualizationStream);
        CopyCanvasDiagnosticsCommand = new AsyncRelayCommand(CopyCanvasDiagnosticsAsync);
        ApplyActivityOptionsCommand = new RelayCommand(ApplyActivityOptions);
        ApplyTickRateOverrideCommand = new AsyncRelayCommand(ApplyTickCadenceAsync);
        ApplyVizCadenceCommand = new AsyncRelayCommand(ApplyVisualizationCadenceAsync);
        ResetVizCadenceCommand = new AsyncRelayCommand(ResetVisualizationCadenceAsync);
        ExportCommand = new AsyncRelayCommand(ExportAsync, () => VizEvents.Count > 0);
        ApplyEnergyCreditCommand = new RelayCommand(() => _brain.ApplyEnergyCreditSelected());
        ApplyEnergyRateCommand = new RelayCommand(() => _brain.ApplyEnergyRateSelected());
        NavigateCanvasPreviousCommand = new RelayCommand(() => NavigateCanvasRelative(-1));
        NavigateCanvasNextCommand = new RelayCommand(() => NavigateCanvasRelative(1));
        NavigateCanvasSelectionCommand = new RelayCommand(NavigateToCanvasSelection);
        ToggleCanvasSelectionExpandedCommand = new RelayCommand(ToggleCanvasSelectionExpanded, () => HasCanvasSelection);
        TogglePinSelectionCommand = new RelayCommand(TogglePinForCurrentSelection);
        ClearCanvasInteractionCommand = new RelayCommand(ClearCanvasInteraction);
        FocusSelectedRouteSourceCommand = new RelayCommand(FocusSelectedRouteSourceRegion, () => _selectedRouteSourceRegionId.HasValue);
        FocusSelectedRouteTargetCommand = new RelayCommand(FocusSelectedRouteTargetRegion, () => _selectedRouteTargetRegionId.HasValue);
        PrepareInputPulseCommand = new RelayCommand(PrepareInputPulseForSelection, CanPrepareInputPulseForSelection);
        ApplyRuntimeStateCommand = new RelayCommand(ApplyRuntimeStateForSelection, CanApplyRuntimeStateForSelection);
        SyncVizCadenceSliderFromText(VizCadenceText);
        UpdateLodSummary();
        RefreshActivityProjection();
    }

    public event Action? VisualizationSelectionChanged;

    public IoPanelViewModel Brain => _brain;

    public ObservableCollection<VizEventItem> VizEvents { get; }
    public ObservableCollection<VizActivityStatItem> ActivityStats { get; }
    public ObservableCollection<VizRegionActivityItem> RegionActivity { get; }
    public ObservableCollection<VizEdgeActivityItem> EdgeActivity { get; }
    public ObservableCollection<VizTickActivityItem> TickActivity { get; }
    public ObservableCollection<VizMiniActivityChartSeriesItem> MiniActivityChartSeries { get; }
    public ObservableCollection<VizActivityCanvasNode> CanvasNodes { get; }
    public ObservableCollection<VizActivityCanvasEdge> CanvasEdges { get; }

    public double ActivityCanvasWidth
    {
        get => _activityCanvasWidth;
        private set => SetProperty(ref _activityCanvasWidth, value);
    }

    public double ActivityCanvasHeight
    {
        get => _activityCanvasHeight;
        private set => SetProperty(ref _activityCanvasHeight, value);
    }
    public string CanvasHoverCardText
    {
        get => _canvasHoverCardText;
        set => SetProperty(ref _canvasHoverCardText, value);
    }

    public bool IsCanvasHoverCardVisible
    {
        get => _isCanvasHoverCardVisible;
        set => SetProperty(ref _isCanvasHoverCardVisible, value);
    }

    public double CanvasHoverCardLeft
    {
        get => _canvasHoverCardLeft;
        set => SetProperty(ref _canvasHoverCardLeft, value);
    }

    public double CanvasHoverCardTop
    {
        get => _canvasHoverCardTop;
        set => SetProperty(ref _canvasHoverCardTop, value);
    }

    public ObservableCollection<BrainListItem> KnownBrains { get; }

    public ObservableCollection<VizPanelTypeOption> VizPanelTypeOptions { get; }

    public ObservableCollection<VizCanvasColorModeOption> CanvasColorModeOptions { get; }

    public ObservableCollection<VizCanvasTransferCurveOption> CanvasTransferCurveOptions { get; }

    public ObservableCollection<VizCanvasLayoutModeOption> LayoutModeOptions { get; }

    public string BrainEntryText
    {
        get => _brainEntryText;
        set => SetProperty(ref _brainEntryText, value);
    }

    public BrainListItem? SelectedBrain
    {
        get => _selectedBrain;
        set
        {
            var previous = _selectedBrain;
            if (SetProperty(ref _selectedBrain, value))
            {
                if (!_suspendSelection)
                {
                    _preferredBrainId = value?.BrainId;
                    if (value is not null && (previous?.BrainId != value.BrainId))
                    {
                        RegionFocusText = string.Empty;
                        _lastRenderedTickId = 0;
                        _nextStreamingRefreshUtc = DateTime.MinValue;
                        _miniChartMinTickFloor = null;
                        _brain.SelectBrain(value.BrainId, preserveOutputs: true);
                        QueueDefinitionTopologyHydration(value.BrainId, TryParseRegionId(RegionFocusText, out var focusRegionId) ? focusRegionId : null);
                    }
                    RefreshFilteredEvents();
                }

                if (value is null)
                {
                    _nextSelectedBrainEnergyRefreshUtc = DateTime.MinValue;
                    SelectedBrainEnergySummary = "Selected brain energy: n/a (no brain selected).";
                }
                else if (previous?.BrainId != value.BrainId)
                {
                    SelectedBrainEnergySummary = FormattableString.Invariant($"Selected brain energy: loading for {value.BrainId:D}...");
                    QueueSelectedBrainEnergyRefresh(force: true);
                }

                OnPropertyChanged(nameof(HasSelectedBrain));
                UpdateCanvasInteractionSummaries(CanvasNodes, CanvasEdges);
                VisualizationSelectionChanged?.Invoke();
            }
        }
    }

    public bool HasSelectedBrain => SelectedBrain is not null;

    public VizPanelTypeOption SelectedVizType
    {
        get => _selectedVizType;
        set
        {
            if (SetProperty(ref _selectedVizType, value))
            {
                RefreshFilteredEvents();
            }
        }
    }

    public VizCanvasColorModeOption SelectedCanvasColorMode
    {
        get => _selectedCanvasColorMode;
        set
        {
            if (SetProperty(ref _selectedCanvasColorMode, value))
            {
                OnPropertyChanged(nameof(CanvasColorModeHint));
                OnPropertyChanged(nameof(CanvasColorModeTooltip));
                RefreshCanvasLayoutOnly();
            }
        }
    }

    public VizCanvasTransferCurveOption SelectedCanvasTransferCurve
    {
        get => _selectedCanvasTransferCurve;
        set
        {
            if (SetProperty(ref _selectedCanvasTransferCurve, value))
            {
                OnPropertyChanged(nameof(CanvasColorCurveHint));
                OnPropertyChanged(nameof(CanvasColorCurveTooltip));
                RefreshCanvasLayoutOnly();
            }
        }
    }

    public VizCanvasLayoutModeOption SelectedLayoutMode
    {
        get => _selectedLayoutMode;
        set
        {
            if (SetProperty(ref _selectedLayoutMode, value))
            {
                RefreshCanvasLayoutOnly();
            }
        }
    }

    public string CanvasColorModeHint => SelectedCanvasColorMode.LegendHint;

    public string CanvasColorModeTooltip => SelectedCanvasColorMode.Tooltip;

    public string CanvasColorCurveHint => SelectedCanvasTransferCurve.LegendHint;

    public string CanvasColorCurveTooltip => SelectedCanvasTransferCurve.Tooltip;

    public bool EnableAdaptiveLod
    {
        get => _enableAdaptiveLod;
        set
        {
            if (SetProperty(ref _enableAdaptiveLod, value))
            {
                UpdateLodSummary();
                RefreshCanvasLayoutOnly();
            }
        }
    }

    public string LodLowZoomBudgetText
    {
        get => _lodLowZoomBudgetText;
        set => SetProperty(ref _lodLowZoomBudgetText, value);
    }

    public string LodMediumZoomBudgetText
    {
        get => _lodMediumZoomBudgetText;
        set => SetProperty(ref _lodMediumZoomBudgetText, value);
    }

    public string LodHighZoomBudgetText
    {
        get => _lodHighZoomBudgetText;
        set => SetProperty(ref _lodHighZoomBudgetText, value);
    }

    public string LodSummary
    {
        get => _lodSummary;
        private set => SetProperty(ref _lodSummary, value);
    }

    public string RegionFocusText
    {
        get => _regionFocusText;
        set
        {
            if (SetProperty(ref _regionFocusText, value))
            {
                OnPropertyChanged(nameof(ActiveFocusRegionId));
            }
        }
    }

    public uint? ActiveFocusRegionId => TryParseRegionId(RegionFocusText, out var regionId) ? regionId : null;

    public string TickWindowText
    {
        get => _tickWindowText;
        set => SetProperty(ref _tickWindowText, value);
    }

    public string TickRateOverrideText
    {
        get => _tickRateOverrideText;
        set
        {
            if (SetProperty(ref _tickRateOverrideText, value))
            {
                SyncTickCadenceSliderFromText(value);
            }
        }
    }

    public string TickRateOverrideSummary
    {
        get => _tickRateOverrideSummary;
        set => SetProperty(ref _tickRateOverrideSummary, value);
    }

    public string TickCadenceSummary
    {
        get => _tickCadenceSummary;
        private set => SetProperty(ref _tickCadenceSummary, value);
    }

    public double TickCadenceSliderMs
    {
        get => _tickCadenceSliderMs;
        set
        {
            var clamped = Math.Clamp(value, TickSliderMinMs, TickSliderMaxMs);
            if (SetProperty(ref _tickCadenceSliderMs, clamped))
            {
                SyncTickCadenceTextFromSlider(clamped);
            }
        }
    }

    public double TickCadenceSliderMin => TickSliderMinMs;

    public double TickCadenceSliderMax => TickSliderMaxMs;

    public string VizCadenceText
    {
        get => _vizCadenceText;
        set
        {
            if (SetProperty(ref _vizCadenceText, value))
            {
                SyncVizCadenceSliderFromText(value);
            }
        }
    }

    public double VizCadenceSliderMs
    {
        get => _vizCadenceSliderMs;
        set
        {
            var clamped = Math.Clamp(value, VizCadenceSliderEffectiveMin, VizSliderMaxMs);
            if (SetProperty(ref _vizCadenceSliderMs, clamped))
            {
                SyncVizCadenceTextFromSlider(clamped);
            }
        }
    }

    public double VizCadenceSliderMin => VizSliderMinMs;

    public double VizCadenceSliderMax => VizSliderMaxMs;

    public double VizCadenceSliderEffectiveMin
    {
        get
        {
            var tickCadenceMs = ResolveEffectiveTickCadenceMs();
            if (!tickCadenceMs.HasValue)
            {
                return VizSliderMinMs;
            }

            return Math.Clamp(tickCadenceMs.Value, VizSliderMinMs, VizSliderMaxMs);
        }
    }

    public string VizCadenceSummary
    {
        get => _vizCadenceSummary;
        private set => SetProperty(ref _vizCadenceSummary, value);
    }

    public bool IncludeLowSignalEvents
    {
        get => _includeLowSignalEvents;
        set
        {
            if (SetProperty(ref _includeLowSignalEvents, value))
            {
                RefreshFilteredEvents();
            }
        }
    }

    public string RegionFilterText
    {
        get => _regionFilterText;
        set
        {
            if (SetProperty(ref _regionFilterText, value))
            {
                RefreshFilteredEvents();
            }
        }
    }

    public string SearchFilterText
    {
        get => _searchFilterText;
        set
        {
            if (SetProperty(ref _searchFilterText, value))
            {
                RefreshFilteredEvents();
            }
        }
    }

    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }

    public string SelectedBrainEnergySummary
    {
        get => _selectedBrainEnergySummary;
        private set => SetProperty(ref _selectedBrainEnergySummary, value);
    }

    public string ActivitySummary
    {
        get => _activitySummary;
        set => SetProperty(ref _activitySummary, value);
    }

    public bool ShowMiniActivityChart
    {
        get => _showMiniActivityChart;
        set
        {
            if (SetProperty(ref _showMiniActivityChart, value))
            {
                RefreshActivityProjection();
            }
        }
    }

    public string MiniActivityTopNText
    {
        get => _miniActivityTopNText;
        set => SetProperty(ref _miniActivityTopNText, value);
    }

    public string MiniActivityRangeSecondsText
    {
        get => _miniActivityRangeSecondsText;
        set => SetProperty(ref _miniActivityRangeSecondsText, value);
    }

    public string MiniActivityChartSeriesLabel
    {
        get => _miniActivityChartSeriesLabel;
        private set => SetProperty(ref _miniActivityChartSeriesLabel, value);
    }

    public string MiniActivityChartRangeLabel
    {
        get => _miniActivityChartRangeLabel;
        private set => SetProperty(ref _miniActivityChartRangeLabel, value);
    }

    public string MiniActivityChartMetricLabel
    {
        get => _miniActivityChartMetricLabel;
        private set => SetProperty(ref _miniActivityChartMetricLabel, value);
    }

    public string MiniActivityYAxisTopLabel
    {
        get => _miniActivityYAxisTopLabel;
        private set => SetProperty(ref _miniActivityYAxisTopLabel, value);
    }

    public string MiniActivityYAxisMidLabel
    {
        get => _miniActivityYAxisMidLabel;
        private set => SetProperty(ref _miniActivityYAxisMidLabel, value);
    }

    public string MiniActivityYAxisBottomLabel
    {
        get => _miniActivityYAxisBottomLabel;
        private set => SetProperty(ref _miniActivityYAxisBottomLabel, value);
    }

    public int MiniActivityLegendColumns
    {
        get => _miniActivityLegendColumns;
        private set => SetProperty(ref _miniActivityLegendColumns, value);
    }

    public double MiniActivityChartWidth => MiniActivityChartPlotWidth;

    public double MiniActivityChartHeight => MiniActivityChartPlotHeight;

    public double MiniActivityChartOverlayWidth => MiniActivityChartOverlayWidthPx;

    public string ActivityCanvasLegend
    {
        get => _activityCanvasLegend;
        set => SetProperty(ref _activityCanvasLegend, value);
    }

    public string ActivityInteractionSummary
    {
        get => _activityInteractionSummary;
        set => SetProperty(ref _activityInteractionSummary, value);
    }

    public string ActivityPinnedSummary
    {
        get => _activityPinnedSummary;
        set => SetProperty(ref _activityPinnedSummary, value);
    }

    public bool HasCanvasSelection
    {
        get => _hasCanvasSelection;
        private set => SetProperty(ref _hasCanvasSelection, value);
    }

    public bool IsCanvasSelectionExpanded
    {
        get => _isCanvasSelectionExpanded;
        private set
        {
            if (SetProperty(ref _isCanvasSelectionExpanded, value))
            {
                OnPropertyChanged(nameof(IsCanvasSelectionDetailsVisible));
                OnPropertyChanged(nameof(CanvasSelectionToggleLabel));
            }
        }
    }

    public bool IsCanvasSelectionDetailsVisible => HasCanvasSelection && IsCanvasSelectionExpanded;

    public string CanvasSelectionToggleLabel => IsCanvasSelectionExpanded ? "Collapse" : "Expand";

    public string CanvasSelectionTitle
    {
        get => _canvasSelectionTitle;
        private set => SetProperty(ref _canvasSelectionTitle, value);
    }

    public string CanvasSelectionIdentity
    {
        get => _canvasSelectionIdentity;
        private set => SetProperty(ref _canvasSelectionIdentity, value);
    }

    public string CanvasSelectionRuntime
    {
        get => _canvasSelectionRuntime;
        private set => SetProperty(ref _canvasSelectionRuntime, value);
    }

    public string CanvasSelectionContext
    {
        get => _canvasSelectionContext;
        private set => SetProperty(ref _canvasSelectionContext, value);
    }

    public string CanvasSelectionDetail
    {
        get => _canvasSelectionDetail;
        private set => SetProperty(ref _canvasSelectionDetail, value);
    }

    public string CanvasSelectionActionHint
    {
        get => _canvasSelectionActionHint;
        private set => SetProperty(ref _canvasSelectionActionHint, value);
    }

    public string SelectedInputPulseValueText
    {
        get => _selectedInputPulseValueText;
        set
        {
            if (SetProperty(ref _selectedInputPulseValueText, value))
            {
                UpdateCanvasSelectionPanelState(TryGetSelectedCanvasNode(CanvasNodes), TryGetSelectedCanvasEdge(CanvasEdges));
            }
        }
    }

    public string SelectedBufferValueText
    {
        get => _selectedBufferValueText;
        set
        {
            if (SetProperty(ref _selectedBufferValueText, value))
            {
                UpdateCanvasSelectionPanelState(TryGetSelectedCanvasNode(CanvasNodes), TryGetSelectedCanvasEdge(CanvasEdges));
            }
        }
    }

    public string SelectedAccumulatorValueText
    {
        get => _selectedAccumulatorValueText;
        set
        {
            if (SetProperty(ref _selectedAccumulatorValueText, value))
            {
                UpdateCanvasSelectionPanelState(TryGetSelectedCanvasNode(CanvasNodes), TryGetSelectedCanvasEdge(CanvasEdges));
            }
        }
    }

    public string TogglePinSelectionLabel => IsCurrentSelectionPinned() ? "Unpin selection" : "Pin selection";

    public bool HasSelectedRouteSource => _selectedRouteSourceRegionId.HasValue;

    public bool HasSelectedRouteTarget => _selectedRouteTargetRegionId.HasValue;

    public string CanvasNavigationHint => "Alt+Left/Right cycle, Alt+Enter navigate, Alt+P pin, Esc clear | Shift+Wheel zoom, Shift+drag or middle-drag pan, double-click empty = reset (focused+default => full brain)";

    public bool ShowProjectionSnapshot
    {
        get => _showProjectionSnapshot;
        set
        {
            if (SetProperty(ref _showProjectionSnapshot, value))
            {
                OnPropertyChanged(nameof(ProjectionSnapshotToggleLabel));
            }
        }
    }

    public string ProjectionSnapshotToggleLabel => ShowProjectionSnapshot ? "Hide snapshot" : "Show snapshot";

    public bool ShowVisualizationStream
    {
        get => _showVisualizationStream;
        set
        {
            if (SetProperty(ref _showVisualizationStream, value))
            {
                OnPropertyChanged(nameof(VisualizationStreamToggleLabel));
            }
        }
    }

    public string VisualizationStreamToggleLabel => ShowVisualizationStream ? "Hide stream" : "Show stream";

    public VizEventItem? SelectedEvent
    {
        get => _selectedEvent;
        set
        {
            if (SetProperty(ref _selectedEvent, value))
            {
                SelectedPayload = BuildPayload(value);
            }
        }
    }

    public string SelectedPayload
    {
        get => _selectedPayload;
        set => SetProperty(ref _selectedPayload, value);
    }

    public RelayCommand ClearCommand { get; }

    public RelayCommand AddBrainCommand { get; }

    public RelayCommand ZoomCommand { get; }

    public RelayCommand ShowFullBrainCommand { get; }

    public RelayCommand ToggleProjectionSnapshotCommand { get; }

    public RelayCommand ToggleVisualizationStreamCommand { get; }

    public AsyncRelayCommand CopyCanvasDiagnosticsCommand { get; }

    public RelayCommand ApplyActivityOptionsCommand { get; }

    public AsyncRelayCommand ApplyTickRateOverrideCommand { get; }

    public AsyncRelayCommand ApplyVizCadenceCommand { get; }

    public AsyncRelayCommand ResetVizCadenceCommand { get; }

    public AsyncRelayCommand ExportCommand { get; }

    public RelayCommand ApplyEnergyCreditCommand { get; }

    public RelayCommand ApplyEnergyRateCommand { get; }

    public RelayCommand NavigateCanvasPreviousCommand { get; }

    public RelayCommand NavigateCanvasNextCommand { get; }

    public RelayCommand NavigateCanvasSelectionCommand { get; }

    public RelayCommand ToggleCanvasSelectionExpandedCommand { get; }

    public RelayCommand TogglePinSelectionCommand { get; }

    public RelayCommand ClearCanvasInteractionCommand { get; }

    public RelayCommand FocusSelectedRouteSourceCommand { get; }

    public RelayCommand FocusSelectedRouteTargetCommand { get; }

    public RelayCommand PrepareInputPulseCommand { get; }

    public RelayCommand ApplyRuntimeStateCommand { get; }

    private readonly record struct CollectionDiffStats(int Added, int Removed, int Moved, int Updated)
    {
        public static CollectionDiffStats Empty { get; } = new(0, 0, 0, 0);

        public override string ToString()
            => $"added={Added} removed={Removed} moved={Moved} updated={Updated}";
    }

    private readonly record struct DefinitionTopologyLoadResult(
        HashSet<uint> Regions,
        HashSet<VizActivityCanvasRegionRoute> RegionRoutes,
        HashSet<uint> NeuronAddresses,
        HashSet<VizActivityCanvasNeuronRoute> NeuronRoutes);

    private readonly record struct DefinitionTopologyLoadAttempt(
        DefinitionTopologyLoadResult? Topology,
        IReadOnlyList<string> RootsTried,
        string? Failure);

    private readonly record struct DefinitionReferenceResolution(
        Nbn.Proto.ArtifactRef? Reference,
        string Source);

    private readonly record struct ProjectionCanvasSnapshot(
        VizActivityProjection Projection,
        VizActivityProjectionOptions Options,
        VizActivityCanvasLayout Canvas,
        MiniActivityChartRenderSnapshot MiniChart,
        double ProjectionMs,
        double LayoutMs);

    private readonly record struct MiniActivityChartRenderSnapshot(
        bool Enabled,
        string SeriesLabel,
        string RangeLabel,
        string MetricLabel,
        string YAxisTopLabel,
        string YAxisMidLabel,
        string YAxisBottomLabel,
        int LegendColumns,
        int TickCount,
        IReadOnlyList<VizMiniActivityChartSeriesItem> Series);

    private readonly record struct RuntimeNeuronTargetRequest(
        uint RegionId,
        uint NeuronId);

    private readonly record struct RuntimePulseRequest(
        uint RegionId,
        uint NeuronId,
        float Value);

    private readonly record struct RuntimeStateWriteRequest(
        uint RegionId,
        uint NeuronId,
        bool SetBuffer,
        float BufferValue,
        bool SetAccumulator,
        float AccumulatorValue);

    private sealed class BrainCanvasTopologyState
    {
        public HashSet<uint> Regions { get; } = new();

        public HashSet<VizActivityCanvasRegionRoute> RegionRoutes { get; } = new();

        public HashSet<uint> NeuronAddresses { get; } = new();

        public HashSet<VizActivityCanvasNeuronRoute> NeuronRoutes { get; } = new();

        public bool HasDefinitionRegionTopology { get; set; }

        public bool HasDefinitionFullTopology { get; set; }

        public string? DefinitionShaHex { get; set; }

        public string? DefinitionSource { get; set; }

        public string DefinitionLoadStatus { get; set; } = "Not attempted.";

        public List<string> LastDefinitionRootsTried { get; } = new();

        public HashSet<uint> DefinitionFocusRegions { get; } = new();
    }
}

public sealed record VizPanelTypeOption(string Label, string? TypeFilter)
{
    public static IReadOnlyList<VizPanelTypeOption> CreateDefaults()
    {
        var options = new List<VizPanelTypeOption> { new("All types", null) };
        foreach (var value in Enum.GetValues<Nbn.Proto.Viz.VizEventType>())
        {
            if (value == Nbn.Proto.Viz.VizEventType.VizUnknown)
            {
                continue;
            }

            options.Add(new VizPanelTypeOption(value.ToString(), value.ToString()));
        }

        return options;
    }
}

public sealed record VizCanvasColorModeOption(string Label, VizActivityCanvasColorMode Mode, string LegendHint, string Tooltip)
{
    public static IReadOnlyList<VizCanvasColorModeOption> CreateDefaults()
        => new[]
        {
            new VizCanvasColorModeOption(
                "State priority",
                VizActivityCanvasColorMode.StateValue,
                "fill=signed state value (orange + / blue -), pulse=activity, border=topology",
                "State value colors: orange (#E69F00) means positive signal, blue (#0072B2) means negative signal, and gray means near-zero or dormant. Magnitude pushes color farther from gray; pulse opacity still tracks recent activity and border stroke follows topology."),
            new VizCanvasColorModeOption(
                "Activity priority",
                VizActivityCanvasColorMode.Activity,
                "fill=activity load+recency, border=topology slice",
                "Activity colors: each region trends toward its topology slice color as activity rises. Gray indicates low/stale activity, while stronger saturation indicates higher recent load. Border remains topology-driven for structural readability."),
            new VizCanvasColorModeOption(
                "Energy: Reserve",
                VizActivityCanvasColorMode.EnergyReserve,
                "fill=signed reserve estimate (orange + / blue -), pulse=activity",
                "Reserve colors: orange (#E69F00) means positive reserve, blue (#0072B2) means negative reserve/debt, and muted gray/blue means near-zero or sparse samples. Magnitude controls distance from neutral."),
            new VizCanvasColorModeOption(
                "Energy: Cost pressure",
                VizActivityCanvasColorMode.EnergyCostPressure,
                "fill=estimated pressure (activity+fanout vs reserve), border=topology",
                "Cost pressure colors: cool blue (#0072B2) means lower pressure and warm orange-red (#D55E00) means higher pressure. Pressure combines activity load, recency, fanout structure, and reserve deficit."),
            new VizCanvasColorModeOption(
                "Topology reference",
                VizActivityCanvasColorMode.Topology,
                "fill=topology slices, pulse=activity",
                "Topology colors: fixed region-slice palette by region id banding (blue/green/magenta/yellow/orange/red). Color is structural, not activity-driven; pulse still reflects recent activity.")
        };
}

public sealed record VizCanvasTransferCurveOption(string Label, VizActivityCanvasTransferCurve Curve, string LegendHint, string Tooltip)
{
    public static IReadOnlyList<VizCanvasTransferCurveOption> CreateDefaults()
        => new[]
        {
            new VizCanvasTransferCurveOption(
                "Perceptual log/symlog",
                VizActivityCanvasTransferCurve.PerceptualLog,
                "log1p for non-negative metrics; symlog for signed metrics",
                "Perceptual mapping boosts low-to-mid activity differences so subtle patterns separate better. Non-negative terms use log(1+gain*x); signed terms use a symmetric log by sign, with high magnitudes gently compressed."),
            new VizCanvasTransferCurveOption(
                "Linear",
                VizActivityCanvasTransferCurve.Linear,
                "literal linear magnitude mapping",
                "Linear mapping keeps color change directly proportional to metric magnitude. Useful for absolute comparisons when low-end contrast amplification is not desired.")
        };
}

public sealed record VizCanvasLayoutModeOption(string Label, VizActivityCanvasLayoutMode Mode)
{
    public static IReadOnlyList<VizCanvasLayoutModeOption> CreateDefaults()
        => new[]
        {
            new VizCanvasLayoutModeOption("Axial 2D", VizActivityCanvasLayoutMode.Axial2D),
            new VizCanvasLayoutModeOption("Projected 3D (R&D)", VizActivityCanvasLayoutMode.Axial3DExperimental)
        };
}

public sealed record VizMiniActivityChartSeriesItem(
    string Key,
    string Label,
    string Stroke,
    string PathData);

public sealed record VizPanelExportItem(
    string Time,
    string Type,
    string BrainId,
    ulong TickId,
    string Region,
    string Source,
    string Target,
    float Value,
    float Strength,
    string EventId)
{
    public static VizPanelExportItem From(VizEventItem item)
        => new(
            item.Time.ToString("O"),
            item.Type,
            item.BrainId,
            item.TickId,
            item.Region,
            item.Source,
            item.Target,
            item.Value,
            item.Strength,
            item.EventId);
}
