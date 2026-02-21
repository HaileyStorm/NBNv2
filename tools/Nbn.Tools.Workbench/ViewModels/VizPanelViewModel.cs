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
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class VizPanelViewModel : ViewModelBase
{
    private const int MaxEvents = 400;
    private const int MaxEventsPerUiFlush = 96;
    private const int MaxPendingEvents = 1600;
    private const int DefaultTickWindow = 64;
    private const int MaxTickWindow = 4096;
    private const int SnapshotRegionRows = 10;
    private const int SnapshotEdgeRows = 14;
    private static readonly TimeSpan StreamingRefreshInterval = TimeSpan.FromMilliseconds(180);
    private static readonly TimeSpan DefinitionHydrationRetryInterval = TimeSpan.FromSeconds(2);
    private const int HoverClearDelayMs = 280;
    private const int EdgeHitSamples = 12;
    private const double HitTestCellSize = 54;
    private const double EdgeHitIndexPadding = 4;
    private const double NodeHitPadding = 2.5;
    private const double StickyNodeHitPadding = 9;
    private const double StickyEdgeHitPadding = 10;
    private const double HitDistanceTieEpsilon = 0.05;
    private const double HoverCardOffset = 14;
    private const double HoverCardMaxWidth = 420;
    private const double HoverCardMaxHeight = 220;
    private readonly UiDispatcher _dispatcher;
    private readonly IoPanelViewModel _brain;
    private readonly List<VizEventItem> _allEvents = new();
    private readonly Queue<VizEventItem> _pendingEvents = new();
    private readonly object _pendingEventsGate = new();
    private bool _flushScheduled;
    private string _status = "Streaming";
    private string _regionFocusText = string.Empty;
    private string _regionFilterText = string.Empty;
    private string _searchFilterText = string.Empty;
    private string _brainEntryText = string.Empty;
    private BrainListItem? _selectedBrain;
    private VizPanelTypeOption _selectedVizType;
    private VizCanvasColorModeOption _selectedCanvasColorMode;
    private bool _suspendSelection;
    private VizEventItem? _selectedEvent;
    private string _selectedPayload = string.Empty;
    private string _tickWindowText = DefaultTickWindow.ToString(CultureInfo.InvariantCulture);
    private string _tickRateOverrideText = string.Empty;
    private string _tickRateOverrideSummary = "Tick override: default backpressure target.";
    private bool _includeLowSignalEvents;
    private string _activitySummary = "Awaiting visualization events.";
    private string _activityCanvasLegend = "Canvas renderer awaiting activity.";
    private bool _showProjectionSnapshot;
    private bool _showVisualizationStream;
    private DateTime _nextStreamingRefreshUtc = DateTime.MinValue;
    private ulong _lastRenderedTickId;
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

    public VizPanelViewModel(UiDispatcher dispatcher, IoPanelViewModel brain)
    {
        _dispatcher = dispatcher;
        _brain = brain;
        VizEvents = new ObservableCollection<VizEventItem>();
        ActivityStats = new ObservableCollection<VizActivityStatItem>();
        RegionActivity = new ObservableCollection<VizRegionActivityItem>();
        EdgeActivity = new ObservableCollection<VizEdgeActivityItem>();
        TickActivity = new ObservableCollection<VizTickActivityItem>();
        CanvasNodes = new ObservableCollection<VizActivityCanvasNode>();
        CanvasEdges = new ObservableCollection<VizActivityCanvasEdge>();
        KnownBrains = new ObservableCollection<BrainListItem>();
        VizPanelTypeOptions = new ObservableCollection<VizPanelTypeOption>(VizPanelTypeOption.CreateDefaults());
        _selectedVizType = VizPanelTypeOptions[0];
        CanvasColorModeOptions = new ObservableCollection<VizCanvasColorModeOption>(VizCanvasColorModeOption.CreateDefaults());
        _selectedCanvasColorMode = CanvasColorModeOptions[0];
        ClearCommand = new RelayCommand(Clear);
        AddBrainCommand = new RelayCommand(AddBrainFromEntry);
        ZoomCommand = new RelayCommand(ZoomRegion);
        ShowFullBrainCommand = new RelayCommand(ShowFullBrain);
        ToggleProjectionSnapshotCommand = new RelayCommand(() => ShowProjectionSnapshot = !ShowProjectionSnapshot);
        ToggleVisualizationStreamCommand = new RelayCommand(() => ShowVisualizationStream = !ShowVisualizationStream);
        CopyCanvasDiagnosticsCommand = new AsyncRelayCommand(CopyCanvasDiagnosticsAsync);
        ApplyActivityOptionsCommand = new RelayCommand(ApplyActivityOptions);
        ApplyTickRateOverrideCommand = new AsyncRelayCommand(ApplyTickRateOverrideAsync);
        ClearTickRateOverrideCommand = new AsyncRelayCommand(ClearTickRateOverrideAsync);
        ExportCommand = new AsyncRelayCommand(ExportAsync, () => VizEvents.Count > 0);
        ApplyEnergyCreditCommand = new RelayCommand(() => _brain.ApplyEnergyCreditSelected());
        ApplyEnergyRateCommand = new RelayCommand(() => _brain.ApplyEnergyRateSelected());
        ApplyCostEnergyCommand = new RelayCommand(() => _brain.ApplyCostEnergySelected());
        NavigateCanvasPreviousCommand = new RelayCommand(() => NavigateCanvasRelative(-1));
        NavigateCanvasNextCommand = new RelayCommand(() => NavigateCanvasRelative(1));
        NavigateCanvasSelectionCommand = new RelayCommand(NavigateToCanvasSelection);
        TogglePinSelectionCommand = new RelayCommand(TogglePinForCurrentSelection);
        ClearCanvasInteractionCommand = new RelayCommand(ClearCanvasInteraction);
        RefreshActivityProjection();
    }

    public event Action? VisualizationSelectionChanged;

    public IoPanelViewModel Brain => _brain;

    public ObservableCollection<VizEventItem> VizEvents { get; }
    public ObservableCollection<VizActivityStatItem> ActivityStats { get; }
    public ObservableCollection<VizRegionActivityItem> RegionActivity { get; }
    public ObservableCollection<VizEdgeActivityItem> EdgeActivity { get; }
    public ObservableCollection<VizTickActivityItem> TickActivity { get; }
    public ObservableCollection<VizActivityCanvasNode> CanvasNodes { get; }
    public ObservableCollection<VizActivityCanvasEdge> CanvasEdges { get; }

    public double ActivityCanvasWidth => VizActivityCanvasLayoutBuilder.CanvasWidth;
    public double ActivityCanvasHeight => VizActivityCanvasLayoutBuilder.CanvasHeight;
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
                    if (value is not null && (previous?.BrainId != value.BrainId))
                    {
                        _lastRenderedTickId = 0;
                        _nextStreamingRefreshUtc = DateTime.MinValue;
                        _brain.SelectBrain(value.BrainId, preserveOutputs: true);
                        QueueDefinitionTopologyHydration(value.BrainId, TryParseRegionId(RegionFocusText, out var focusRegionId) ? focusRegionId : null);
                    }
                    RefreshFilteredEvents();
                }

                OnPropertyChanged(nameof(HasSelectedBrain));
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
                RefreshCanvasLayoutOnly();
            }
        }
    }

    public string CanvasColorModeHint => SelectedCanvasColorMode.LegendHint;

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
        set => SetProperty(ref _tickRateOverrideText, value);
    }

    public string TickRateOverrideSummary
    {
        get => _tickRateOverrideSummary;
        set => SetProperty(ref _tickRateOverrideSummary, value);
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

    public string ActivitySummary
    {
        get => _activitySummary;
        set => SetProperty(ref _activitySummary, value);
    }

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

    public string TogglePinSelectionLabel => IsCurrentSelectionPinned() ? "Unpin selection" : "Pin selection";

    public string CanvasNavigationHint => "Alt+Left/Alt+Right cycle, Alt+Enter navigate, Alt+P pin, Esc clear";

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

    public AsyncRelayCommand ClearTickRateOverrideCommand { get; }

    public AsyncRelayCommand ExportCommand { get; }

    public RelayCommand ApplyEnergyCreditCommand { get; }

    public RelayCommand ApplyEnergyRateCommand { get; }

    public RelayCommand ApplyCostEnergyCommand { get; }

    public RelayCommand NavigateCanvasPreviousCommand { get; }

    public RelayCommand NavigateCanvasNextCommand { get; }

    public RelayCommand NavigateCanvasSelectionCommand { get; }

    public RelayCommand TogglePinSelectionCommand { get; }

    public RelayCommand ClearCanvasInteractionCommand { get; }

    public void AddBrainId(Guid id)
    {
        AddBrainId(id.ToString("D"));
    }

    public void SetBrains(IReadOnlyList<BrainListItem> brains)
    {
        var previousSelection = SelectedBrain;
        var selectedId = previousSelection?.Id;
        _suspendSelection = true;
        KnownBrains.Clear();
        foreach (var brain in brains)
        {
            KnownBrains.Add(brain);
        }

        if (brains.Count == 0)
        {
            SelectedBrain = null;
            _suspendSelection = false;
            Status = "No brains reported.";
            RefreshFilteredEvents();
            return;
        }

        Status = "Streaming";

        BrainListItem? match = null;
        if (!string.IsNullOrWhiteSpace(selectedId))
        {
            match = KnownBrains.FirstOrDefault(entry => entry.Id == selectedId);
        }

        if (KnownBrains.Count > 0 && match is null)
        {
            match = KnownBrains[0];
        }
        SelectedBrain = match;
        _suspendSelection = false;
        if (match is not null)
        {
            _brain.EnsureSelectedBrain(match.BrainId);
            QueueDefinitionTopologyHydration(match.BrainId, TryParseRegionId(RegionFocusText, out var focusRegionId) ? focusRegionId : null);
        }
        RefreshFilteredEvents();
    }

    public void AddVizEvent(VizEventItem item)
    {
        lock (_pendingEventsGate)
        {
            while (_pendingEvents.Count >= MaxPendingEvents)
            {
                _pendingEvents.Dequeue();
                _droppedPendingEvents++;
            }

            _pendingEvents.Enqueue(item);
            _maxObservedPendingEvents = Math.Max(_maxObservedPendingEvents, _pendingEvents.Count);
            if (_flushScheduled)
            {
                return;
            }

            _flushScheduled = true;
        }

        _dispatcher.Post(FlushPendingEvents);
    }

    public void SelectCanvasNode(VizActivityCanvasNode? node)
    {
        if (node is null)
        {
            ClearCanvasSelection();
            return;
        }

        _selectedCanvasNodeKey = node.NodeKey;
        _selectedCanvasRouteLabel = null;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
        RefreshCanvasLayoutOnly();
        Status = $"Selected node {node.Label}.";
    }

    public void SelectCanvasEdge(VizActivityCanvasEdge? edge)
    {
        if (edge is null)
        {
            ClearCanvasSelection();
            return;
        }

        _selectedCanvasRouteLabel = edge.RouteLabel;
        _selectedCanvasNodeKey = null;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
        RefreshCanvasLayoutOnly();
        Status = $"Selected route {edge.RouteLabel}.";
    }

    public void SetCanvasNodeHover(VizActivityCanvasNode? node, double pointerX = double.NaN, double pointerY = double.NaN)
    {
        if (node is null)
        {
            ClearCanvasHover();
            return;
        }

        CancelPendingHoverClear();
        var nextNode = node.NodeKey;
        if (string.Equals(_hoverCanvasNodeKey, nextNode, StringComparison.OrdinalIgnoreCase) && _hoverCanvasRouteLabel is null)
        {
            UpdateCanvasHoverCard(node.Detail, pointerX, pointerY);
            return;
        }

        _hoverCanvasNodeKey = nextNode;
        _hoverCanvasRouteLabel = null;
        UpdateCanvasHoverCard(node.Detail, pointerX, pointerY);
        UpdateCanvasInteractionSummaries(CanvasNodes, CanvasEdges);
    }

    public void SetCanvasEdgeHover(VizActivityCanvasEdge? edge, double pointerX = double.NaN, double pointerY = double.NaN)
    {
        if (edge is null)
        {
            ClearCanvasHover();
            return;
        }

        CancelPendingHoverClear();
        var nextRoute = edge.RouteLabel;
        if (string.Equals(_hoverCanvasRouteLabel, nextRoute, StringComparison.OrdinalIgnoreCase) && string.IsNullOrWhiteSpace(_hoverCanvasNodeKey))
        {
            UpdateCanvasHoverCard(edge.Detail, pointerX, pointerY);
            return;
        }

        _hoverCanvasRouteLabel = nextRoute;
        _hoverCanvasNodeKey = null;
        UpdateCanvasHoverCard(edge.Detail, pointerX, pointerY);
        UpdateCanvasInteractionSummaries(CanvasNodes, CanvasEdges);
    }

    public void ClearCanvasHoverDeferred(int delayMs = HoverClearDelayMs)
    {
        if (string.IsNullOrWhiteSpace(_hoverCanvasNodeKey)
            && string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel)
            && !IsCanvasHoverCardVisible
            && string.IsNullOrWhiteSpace(CanvasHoverCardText))
        {
            return;
        }

        var revision = Interlocked.Increment(ref _hoverClearRevision);
        var safeDelayMs = Math.Max(0, delayMs);
        _ = Task.Run(async () =>
        {
            await Task.Delay(safeDelayMs).ConfigureAwait(false);
            _dispatcher.Post(() =>
            {
                if (revision != _hoverClearRevision)
                {
                    return;
                }

                ClearCanvasHover();
            });
        });
    }

    public void ClearCanvasHover()
    {
        CancelPendingHoverClear();
        var hadHover = !string.IsNullOrWhiteSpace(_hoverCanvasNodeKey)
                       || !string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel)
                       || IsCanvasHoverCardVisible
                       || !string.IsNullOrWhiteSpace(CanvasHoverCardText);
        if (!hadHover)
        {
            return;
        }

        _hoverCanvasNodeKey = null;
        _hoverCanvasRouteLabel = null;
        CanvasHoverCardText = string.Empty;
        IsCanvasHoverCardVisible = false;
        UpdateCanvasInteractionSummaries(CanvasNodes, CanvasEdges);
    }

    public void KeepCanvasHoverAlive()
    {
        var hasHover = !string.IsNullOrWhiteSpace(_hoverCanvasNodeKey)
                       || !string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel)
                       || IsCanvasHoverCardVisible
                       || !string.IsNullOrWhiteSpace(CanvasHoverCardText);
        if (!hasHover)
        {
            return;
        }

        CancelPendingHoverClear();
    }

    public bool TryResolveCanvasHit(double pointerX, double pointerY, out VizActivityCanvasNode? node, out VizActivityCanvasEdge? edge)
    {
        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            // Keep hover locked to the current target while still inside sticky tolerance.
            if (TryResolveStickyHoverHit(pointerX, pointerY, out node, out edge))
            {
                return true;
            }

            node = HitTestCanvasNode(pointerX, pointerY);
            if (node is not null)
            {
                edge = null;
                return true;
            }

            edge = HitTestCanvasEdge(pointerX, pointerY);
            if (edge is not null)
            {
                return true;
            }

            return false;
        }
        finally
        {
            RecordHitTestDuration(startedAt);
        }
    }

    public bool TrySelectHoveredCanvasItem(bool togglePin)
    {
        if (!string.IsNullOrWhiteSpace(_hoverCanvasNodeKey))
        {
            var node = _canvasNodeByKey.TryGetValue(_hoverCanvasNodeKey!, out var keyedNode)
                ? keyedNode
                : CanvasNodes.FirstOrDefault(item => string.Equals(item.NodeKey, _hoverCanvasNodeKey, StringComparison.OrdinalIgnoreCase));
            if (node is not null)
            {
                if (togglePin)
                {
                    TogglePinCanvasNode(node);
                }
                else
                {
                    SelectCanvasNode(node);
                }

                return true;
            }
        }

        if (string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel))
        {
            return false;
        }

        var edge = _canvasEdgeByRoute.TryGetValue(_hoverCanvasRouteLabel!, out var keyedEdge)
            ? keyedEdge
            : CanvasEdges.FirstOrDefault(item => string.Equals(item.RouteLabel, _hoverCanvasRouteLabel, StringComparison.OrdinalIgnoreCase));
        if (edge is null)
        {
            return false;
        }

        if (togglePin)
        {
            TogglePinCanvasEdge(edge);
        }
        else
        {
            SelectCanvasEdge(edge);
        }

        return true;
    }

    public void TogglePinCanvasNode(VizActivityCanvasNode? node)
    {
        if (node is null)
        {
            return;
        }

        if (!_pinnedCanvasNodes.Add(node.NodeKey))
        {
            _pinnedCanvasNodes.Remove(node.NodeKey);
        }

        _selectedCanvasNodeKey = node.NodeKey;
        _selectedCanvasRouteLabel = null;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
        RefreshCanvasLayoutOnly();
    }

    public void TogglePinCanvasEdge(VizActivityCanvasEdge? edge)
    {
        if (edge is null || string.IsNullOrWhiteSpace(edge.RouteLabel))
        {
            return;
        }

        if (!_pinnedCanvasRoutes.Add(edge.RouteLabel))
        {
            _pinnedCanvasRoutes.Remove(edge.RouteLabel);
        }

        _selectedCanvasRouteLabel = edge.RouteLabel;
        _selectedCanvasNodeKey = null;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
        RefreshCanvasLayoutOnly();
    }

    private void AddBrainFromEntry()
    {
        AddBrainId(BrainEntryText);
    }

    private void AddBrainId(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            Status = "Brain ID required.";
            return;
        }

        if (!Guid.TryParse(value, out var guid))
        {
            Status = "Brain ID invalid.";
            return;
        }

        var id = guid.ToString("D");
        var existing = KnownBrains.FirstOrDefault(entry => entry.Id == id);
        if (existing is null)
        {
            existing = new BrainListItem(guid, "manual", false);
            KnownBrains.Add(existing);
        }

        SelectedBrain = existing;
        BrainEntryText = id;
        Status = "Brain selected.";
    }

    private void ZoomRegion()
    {
        if (!TryParseRegionId(RegionFocusText, out var regionId))
        {
            Status = $"Region ID must be {NbnConstants.RegionMinId}-{NbnConstants.RegionMaxId}.";
            return;
        }

        RegionFocusText = regionId.ToString(CultureInfo.InvariantCulture);
        RefreshFilteredEvents();
        if (SelectedBrain is not null)
        {
            QueueDefinitionTopologyHydration(SelectedBrain.BrainId, regionId);
        }
        VisualizationSelectionChanged?.Invoke();
        Status = $"Zoom focus set to region {regionId}.";
    }

    private void ShowFullBrain()
    {
        RegionFocusText = string.Empty;
        RegionFilterText = string.Empty;
        RefreshFilteredEvents();
        if (SelectedBrain is not null)
        {
            QueueDefinitionTopologyHydration(SelectedBrain.BrainId, null);
        }
        VisualizationSelectionChanged?.Invoke();
        Status = "Full-brain view enabled.";
    }

    private void ApplyActivityOptions()
    {
        if (!TryParseTickWindow(TickWindowText, out var tickWindow))
        {
            Status = $"Tick window must be an integer in 1-{MaxTickWindow}.";
            return;
        }

        TickWindowText = tickWindow.ToString(CultureInfo.InvariantCulture);
        RefreshFilteredEvents();
        if (SelectedBrain is not null)
        {
            QueueDefinitionTopologyHydration(SelectedBrain.BrainId, TryParseRegionId(RegionFocusText, out var focusRegionId) ? focusRegionId : null);
        }
        Status = $"Applied activity options (tick window {tickWindow}).";
    }

    private async Task ApplyTickRateOverrideAsync()
    {
        if (!float.TryParse(TickRateOverrideText, NumberStyles.Float, CultureInfo.InvariantCulture, out var targetTickHz)
            || !float.IsFinite(targetTickHz)
            || targetTickHz <= 0f)
        {
            Status = "Tick override must be a positive number.";
            return;
        }

        var ack = await _brain.SetTickRateOverrideAsync(targetTickHz).ConfigureAwait(false);
        UpdateTickRateOverrideStatus(ack, "Tick override request failed: HiveMind unavailable.");
    }

    private async Task ClearTickRateOverrideAsync()
    {
        var ack = await _brain.SetTickRateOverrideAsync(null).ConfigureAwait(false);
        UpdateTickRateOverrideStatus(ack, "Tick override clear failed: HiveMind unavailable.");
    }

    private void UpdateTickRateOverrideStatus(Nbn.Proto.Control.SetTickRateOverrideAck? ack, string fallbackStatus)
    {
        if (ack is null)
        {
            Status = fallbackStatus;
            return;
        }

        var targetTickHz = ack.TargetTickHz;
        if (ack.Accepted)
        {
            TickRateOverrideSummary = ack.HasOverride
                ? $"Tick override active ({ack.OverrideTickHz:0.###} Hz). Current target {targetTickHz:0.###} Hz."
                : $"Tick override cleared. Current target {targetTickHz:0.###} Hz.";

            if (ack.HasOverride)
            {
                TickRateOverrideText = ack.OverrideTickHz.ToString("0.###", CultureInfo.InvariantCulture);
            }
        }

        Status = string.IsNullOrWhiteSpace(ack.Message)
            ? (ack.Accepted ? TickRateOverrideSummary : "Tick override request rejected.")
            : ack.Message;
    }

    private void Clear()
    {
        lock (_pendingEventsGate)
        {
            _pendingEvents.Clear();
            _flushScheduled = false;
        }

        _allEvents.Clear();
        _topologyByBrainId.Clear();
        lock (_pendingDefinitionHydrationGate)
        {
            _pendingDefinitionHydrationKeys.Clear();
        }
        _currentProjection = null;
        _currentProjectionOptions = new VizActivityProjectionOptions(DefaultTickWindow, IncludeLowSignalEvents, null);
        _lastRenderedTickId = 0;
        _nextStreamingRefreshUtc = DateTime.MinValue;
        _nextDefinitionHydrationRetryUtc = DateTime.MinValue;
        VizEvents.Clear();
        SelectedEvent = null;
        SelectedPayload = string.Empty;
        ResetCanvasInteractionState(clearPins: true);
        ExportCommand.RaiseCanExecuteChanged();
        RefreshActivityProjection();
        Status = "Cleared.";
    }

    private async Task ExportAsync()
    {
        if (VizEvents.Count == 0)
        {
            Status = "Nothing to export.";
            return;
        }

        var file = await PickSaveFileAsync("Export viz events", "JSON files", "json", "viz-events.json");
        if (file is null)
        {
            Status = "Export canceled.";
            return;
        }

        try
        {
            var payload = VizEvents.Select(VizPanelExportItem.From).ToList();
            var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
            await WriteAllTextAsync(file, json);
            Status = $"Exported {payload.Count} events to {FormatPath(file)}.";
        }
        catch (Exception ex)
        {
            Status = $"Export failed: {ex.Message}";
        }
    }

    private void RefreshFilteredEvents(bool fromStreaming = false, bool force = false)
    {
        if (fromStreaming && !force && !ShouldRefreshFromStreamingBatch())
        {
            return;
        }

        var selected = SelectedEvent;
        VizEvents.Clear();
        var matched = 0;
        Status = KnownBrains.Count == 0 ? "No brains reported." : "Streaming";

        foreach (var item in _allEvents)
        {
            if (MatchesFilter(item))
            {
                VizEvents.Add(item);
                matched++;
            }
        }

        if (matched == 0 && _allEvents.Count > 0 && SelectedBrain is not null)
        {
            Status = "No events for selected brain.";
        }

        if (selected is not null && VizEvents.Contains(selected))
        {
            SelectedEvent = selected;
        }
        else
        {
            SelectedEvent = null;
        }

        SelectedPayload = BuildPayload(SelectedEvent);
        RefreshActivityProjection();
        EnsureDefinitionTopologyCoverage();
        _lastRenderedTickId = GetLatestTickForCurrentSelection();
        if (fromStreaming)
        {
            _nextStreamingRefreshUtc = DateTime.UtcNow + StreamingRefreshInterval;
        }

        ExportCommand.RaiseCanExecuteChanged();
    }

    private bool ShouldRefreshFromStreamingBatch()
    {
        var latestTick = GetLatestTickForCurrentSelection();
        if (latestTick <= _lastRenderedTickId)
        {
            return false;
        }

        return DateTime.UtcNow >= _nextStreamingRefreshUtc;
    }

    private ulong GetLatestTickForCurrentSelection()
    {
        if (_allEvents.Count == 0)
        {
            return 0;
        }

        if (SelectedBrain is null)
        {
            return _allEvents[0].TickId;
        }

        var selectedBrainId = SelectedBrain.BrainId;
        var hasFocusFilter = TryParseRegionId(RegionFocusText, out var focusRegionId);
        foreach (var item in _allEvents)
        {
            if (!Guid.TryParse(item.BrainId, out var itemBrainId))
            {
                continue;
            }

            if (itemBrainId == selectedBrainId)
            {
                if (hasFocusFilter && !TouchesFocusRegion(item, focusRegionId))
                {
                    continue;
                }

                return item.TickId;
            }
        }

        return 0;
    }

    private static bool TouchesFocusRegion(VizEventItem item, uint focusRegionId)
    {
        if (TryParseRegionForTopology(item.Region, out var eventRegion) && eventRegion == focusRegionId)
        {
            return true;
        }

        if (TryParseAddressForTopology(item.Source, out var sourceAddress)
            && (sourceAddress >> NbnConstants.AddressNeuronBits) == focusRegionId)
        {
            return true;
        }

        if (TryParseAddressForTopology(item.Target, out var targetAddress)
            && (targetAddress >> NbnConstants.AddressNeuronBits) == focusRegionId)
        {
            return true;
        }

        return false;
    }

    private bool MatchesFilter(VizEventItem item, bool ignoreBrain = false)
    {
        if (!ignoreBrain && SelectedBrain is not null)
        {
            if (Guid.TryParse(item.BrainId, out var itemBrainId))
            {
                if (itemBrainId != SelectedBrain.BrainId)
                {
                    return false;
                }
            }
            else if (IsGlobalVisualizerEvent(item.Type))
            {
                // Global-only events (for example VizTick with no BrainId) should not
                // appear when viewing a specific brain.
                return false;
            }
        }

        if (SelectedVizType.TypeFilter is not null && !string.Equals(item.Type, SelectedVizType.TypeFilter, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(RegionFilterText))
        {
            if (!string.Equals(item.Region, RegionFilterText.Trim(), StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        if (!string.IsNullOrWhiteSpace(SearchFilterText))
        {
            var needle = SearchFilterText.Trim();
            if (!ContainsIgnoreCase(item.Type, needle)
                && !ContainsIgnoreCase(item.BrainId, needle)
                && !ContainsIgnoreCase(item.Region, needle)
                && !ContainsIgnoreCase(item.Source, needle)
                && !ContainsIgnoreCase(item.Target, needle)
                && !ContainsIgnoreCase(item.EventId, needle))
            {
                return false;
            }
        }

        return true;
    }

    private void FlushPendingEvents()
    {
        var flushStart = Stopwatch.GetTimestamp();
        List<VizEventItem> batch;
        var hasMore = false;
        lock (_pendingEventsGate)
        {
            if (_pendingEvents.Count == 0)
            {
                _flushScheduled = false;
                return;
            }

            var takeCount = Math.Min(MaxEventsPerUiFlush, _pendingEvents.Count);
            batch = new List<VizEventItem>(takeCount);
            for (var i = 0; i < takeCount; i++)
            {
                batch.Add(_pendingEvents.Dequeue());
            }

            hasMore = _pendingEvents.Count > 0;
            if (!hasMore)
            {
                _flushScheduled = false;
            }
        }

        foreach (var item in batch)
        {
            if (!ShouldIncludeInVisualizer(item))
            {
                continue;
            }

            AccumulateTopology(item);
            _allEvents.Insert(0, item);
        }

        Trim(_allEvents);
        RefreshFilteredEvents(fromStreaming: true, force: !hasMore);
        _lastFlushBatchCount = batch.Count;
        _lastFlushBatchMs = StopwatchElapsedMs(flushStart);

        if (hasMore)
        {
            Avalonia.Threading.Dispatcher.UIThread.Post(FlushPendingEvents, Avalonia.Threading.DispatcherPriority.Background);
        }
    }

    private async Task CopyCanvasDiagnosticsAsync()
    {
        var report = BuildCanvasDiagnosticsReport();
        SelectedPayload = report;

        if (await TrySetClipboardTextAsync(report))
        {
            Status = $"Copied visualizer diagnostics ({report.Length} chars).";
        }
        else
        {
            Status = "Clipboard unavailable; diagnostics copied to payload panel.";
        }
    }

    private string BuildCanvasDiagnosticsReport()
    {
        var sb = new StringBuilder(capacity: 8192);
        var utcNow = DateTimeOffset.UtcNow;
        var selectedBrainText = SelectedBrain?.BrainId.ToString("D") ?? "none";
        var selectedTypeText = SelectedVizType.TypeFilter ?? "all";
        var topology = BuildTopologySnapshotForSelectedBrain();
        int pendingQueueDepth;
        lock (_pendingEventsGate)
        {
            pendingQueueDepth = _pendingEvents.Count;
        }

        sb.AppendLine($"Visualizer diagnostics @ {utcNow:O}");
        sb.AppendLine($"selected_brain={selectedBrainText} known_brains={KnownBrains.Count} selected_type={selectedTypeText}");
        sb.AppendLine($"topology regions={topology.Regions.Count} region_routes={topology.RegionRoutes.Count} neuron_addresses={topology.NeuronAddresses.Count} neuron_routes={topology.NeuronRoutes.Count}");
        if (SelectedBrain is not null && _topologyByBrainId.TryGetValue(SelectedBrain.BrainId, out var state))
        {
            var rootsPreview = state.LastDefinitionRootsTried.Count == 0
                ? "<none>"
                : string.Join(" | ", state.LastDefinitionRootsTried.Take(3));
            sb.AppendLine(
                $"definition source={NormalizeDiagnosticText(state.DefinitionSource)} sha={NormalizeDiagnosticText(state.DefinitionShaHex)} has_region_topology={state.HasDefinitionRegionTopology} has_full_topology={state.HasDefinitionFullTopology} focus_regions={state.DefinitionFocusRegions.Count} status={NormalizeDiagnosticText(state.DefinitionLoadStatus)} roots={rootsPreview}");
        }

        sb.AppendLine(
            $"focus={NormalizeDiagnosticText(RegionFocusText)} region_filter={NormalizeDiagnosticText(RegionFilterText)} search={NormalizeDiagnosticText(SearchFilterText)} tick_window={ParseTickWindowOrDefault()} include_low_signal={IncludeLowSignalEvents}");
        sb.AppendLine(
            $"events all={_allEvents.Count} filtered={VizEvents.Count} canvas_nodes={CanvasNodes.Count} canvas_edges={CanvasEdges.Count} stats={ActivityStats.Count} region_rows={RegionActivity.Count} edge_rows={EdgeActivity.Count} tick_rows={TickActivity.Count}");
        sb.AppendLine(
            $"perf projection_ms={_lastProjectionBuildMs:0.###} layout_ms={_lastCanvasLayoutBuildMs:0.###} apply_ms={_lastCanvasApplyMs:0.###} frame_ms={_lastCanvasFrameMs:0.###} flush_ms={_lastFlushBatchMs:0.###} flush_batch={_lastFlushBatchCount}");
        sb.AppendLine(
            $"hit_test last_ms={_lastHitTestMs:0.###} avg_ms={_avgHitTestMs:0.###} max_ms={_maxHitTestMs:0.###} samples={_hitTestSamples}");
        sb.AppendLine(
            $"queue pending={pendingQueueDepth} pending_peak={_maxObservedPendingEvents} dropped={_droppedPendingEvents}");
        sb.AppendLine(
            $"canvas_diff nodes({_lastCanvasNodeDiffStats}) edges({_lastCanvasEdgeDiffStats})");
        sb.AppendLine($"summary={ActivitySummary}");
        sb.AppendLine($"legend={ActivityCanvasLegend}");
        sb.AppendLine($"interaction={ActivityInteractionSummary}");
        sb.AppendLine($"pinned={ActivityPinnedSummary}");
        sb.AppendLine();

        sb.AppendLine("ActivityStats:");
        foreach (var stat in ActivityStats.Take(20))
        {
            sb.AppendLine($"- {stat.Label}: {stat.Value} ({stat.Detail})");
        }

        sb.AppendLine("CanvasNodes:");
        foreach (var node in CanvasNodes
                     .OrderByDescending(item => item.EventCount)
                     .ThenBy(item => item.RegionId)
                     .Take(24))
        {
            sb.AppendLine(
                $"- {node.Label} key={node.NodeKey} region={node.RegionId} neuron={node.NeuronId?.ToString(CultureInfo.InvariantCulture) ?? "?"} events={node.EventCount} tick={node.LastTick} left={node.Left:0.##} top={node.Top:0.##} size={node.Diameter:0.##} selected={node.IsSelected} hover={node.IsHovered} pinned={node.IsPinned}");
        }

        sb.AppendLine("CanvasEdges:");
        foreach (var edge in CanvasEdges
                     .OrderByDescending(item => item.EventCount)
                     .ThenBy(item => item.RouteLabel, StringComparer.OrdinalIgnoreCase)
                     .Take(24))
        {
            sb.AppendLine(
                $"- {edge.RouteLabel} events={edge.EventCount} tick={edge.LastTick} stroke={edge.StrokeThickness:0.##} opacity={edge.Opacity:0.##} color={edge.Stroke} activity_color={edge.ActivityStroke} activity_stroke={edge.ActivityStrokeThickness:0.##} activity_opacity={edge.ActivityOpacity:0.##} src={edge.SourceRegionId?.ToString(CultureInfo.InvariantCulture) ?? "?"} dst={edge.TargetRegionId?.ToString(CultureInfo.InvariantCulture) ?? "?"} selected={edge.IsSelected} hover={edge.IsHovered} pinned={edge.IsPinned}");
        }

        sb.AppendLine("RecentVizEvents:");
        foreach (var item in VizEvents.Take(40))
        {
            sb.AppendLine(
                $"- tick={item.TickId} type={item.Type} region={NormalizeDiagnosticText(item.Region)} source={NormalizeDiagnosticText(item.Source)} target={NormalizeDiagnosticText(item.Target)} value={item.Value:0.######} strength={item.Strength:0.######}");
        }

        return sb.ToString();
    }

    private static bool ShouldIncludeInVisualizer(VizEventItem item)
    {
        if (Guid.TryParse(item.BrainId, out _))
        {
            return true;
        }

        // Suppress global tick noise; keep other events even when BrainId is absent
        // so Visualizer can still show activity in partially-populated streams.
        return !IsGlobalVisualizerEvent(item.Type);
    }

    private static bool IsGlobalVisualizerEvent(string? type)
        => string.Equals(type, Nbn.Proto.Viz.VizEventType.VizTick.ToString(), StringComparison.OrdinalIgnoreCase);

    private void RefreshActivityProjection()
    {
        var options = new VizActivityProjectionOptions(
            ParseTickWindowOrDefault(),
            IncludeLowSignalEvents,
            TryParseRegionId(RegionFocusText, out var regionId) ? regionId : null);

        var projectionStart = Stopwatch.GetTimestamp();
        var projection = VizActivityProjectionBuilder.Build(VizEvents, options);
        _lastProjectionBuildMs = StopwatchElapsedMs(projectionStart);
        _currentProjection = projection;
        _currentProjectionOptions = options;

        ReplaceItems(ActivityStats, projection.Stats);
        ReplaceItems(RegionActivity, projection.Regions.Take(SnapshotRegionRows).ToList());
        ReplaceItems(EdgeActivity, projection.Edges.Take(SnapshotEdgeRows).ToList());
        ReplaceItems(TickActivity, projection.Ticks);
        ActivitySummary = projection.Summary;
        RefreshCanvasLayoutOnly();
    }

    private void RefreshCanvasLayoutOnly()
    {
        var frameStart = Stopwatch.GetTimestamp();
        if (_currentProjection is null)
        {
            ReplaceItems(CanvasNodes, Array.Empty<VizActivityCanvasNode>());
            ReplaceItems(CanvasEdges, Array.Empty<VizActivityCanvasEdge>());
            RebuildCanvasHitIndex(Array.Empty<VizActivityCanvasNode>(), Array.Empty<VizActivityCanvasEdge>());
            ActivityCanvasLegend = "Canvas renderer awaiting activity.";
            ActivityInteractionSummary = "Selected: none | Hover: none";
            ActivityPinnedSummary = "Pinned: none.";
            _lastCanvasLayoutBuildMs = 0;
            _lastCanvasApplyMs = 0;
            _lastCanvasNodeDiffStats = CollectionDiffStats.Empty;
            _lastCanvasEdgeDiffStats = CollectionDiffStats.Empty;
            _lastCanvasFrameMs = StopwatchElapsedMs(frameStart);
            OnPropertyChanged(nameof(TogglePinSelectionLabel));
            return;
        }

        var topology = BuildTopologySnapshotForSelectedBrain();
        var interaction = new VizActivityCanvasInteractionState(
            _selectedCanvasNodeKey,
            _selectedCanvasRouteLabel,
            _hoverCanvasNodeKey,
            _hoverCanvasRouteLabel,
            _pinnedCanvasNodes,
            _pinnedCanvasRoutes);
        var layoutStart = Stopwatch.GetTimestamp();
        var canvas = VizActivityCanvasLayoutBuilder.Build(
            _currentProjection,
            _currentProjectionOptions,
            interaction,
            topology,
            SelectedCanvasColorMode.Mode);

        if (TrimCanvasInteractionToLayout(canvas.Nodes, canvas.Edges))
        {
            interaction = new VizActivityCanvasInteractionState(
                _selectedCanvasNodeKey,
                _selectedCanvasRouteLabel,
                _hoverCanvasNodeKey,
                _hoverCanvasRouteLabel,
                _pinnedCanvasNodes,
                _pinnedCanvasRoutes);
            canvas = VizActivityCanvasLayoutBuilder.Build(
                _currentProjection,
                _currentProjectionOptions,
                interaction,
                topology,
                SelectedCanvasColorMode.Mode);
        }
        _lastCanvasLayoutBuildMs = StopwatchElapsedMs(layoutStart);

        var applyStart = Stopwatch.GetTimestamp();
        _lastCanvasNodeDiffStats = ApplyKeyedDiff(CanvasNodes, canvas.Nodes, static item => item.NodeKey);
        _lastCanvasEdgeDiffStats = ApplyKeyedDiff(CanvasEdges, canvas.Edges, static item => item.RouteLabel);
        RebuildCanvasHitIndex(canvas.Nodes, canvas.Edges);
        ActivityCanvasLegend = $"{canvas.Legend} | Color mode: {SelectedCanvasColorMode.Label} ({SelectedCanvasColorMode.LegendHint})";
        UpdateCanvasInteractionSummaries(canvas.Nodes, canvas.Edges);
        RefreshCanvasHoverCard(canvas.Nodes, canvas.Edges);
        _lastCanvasApplyMs = StopwatchElapsedMs(applyStart);
        _lastCanvasFrameMs = StopwatchElapsedMs(frameStart);
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
    }

    private void QueueDefinitionTopologyHydration(Guid brainId, uint? focusRegionId)
    {
        var hydrationKey = BuildDefinitionHydrationKey(brainId, focusRegionId);
        lock (_pendingDefinitionHydrationGate)
        {
            if (!_pendingDefinitionHydrationKeys.Add(hydrationKey))
            {
                return;
            }
        }

        _ = HydrateDefinitionTopologyAsync(brainId, focusRegionId, hydrationKey);
    }

    private async Task HydrateDefinitionTopologyAsync(Guid brainId, uint? focusRegionId, string hydrationKey)
    {
        await _definitionTopologyGate.WaitAsync().ConfigureAwait(false);
        try
        {
            var resolvedReference = await ResolveDefinitionArtifactReferenceAsync(brainId).ConfigureAwait(false);
            var artifactRef = resolvedReference.Reference;
            if (artifactRef is null || !artifactRef.TryToSha256Bytes(out var shaBytes))
            {
                _dispatcher.Post(() =>
                {
                    if (!_topologyByBrainId.TryGetValue(brainId, out var state))
                    {
                        state = new BrainCanvasTopologyState();
                        state.Regions.Add((uint)NbnConstants.InputRegionId);
                        state.Regions.Add((uint)NbnConstants.OutputRegionId);
                        _topologyByBrainId[brainId] = state;
                    }

                    state.DefinitionSource = resolvedReference.Source;
                    state.DefinitionLoadStatus = "No definition artifact reference available.";
                    state.LastDefinitionRootsTried.Clear();
                });
                return;
            }

            var definitionShaHex = Convert.ToHexString(shaBytes).ToLowerInvariant();
            var loadAttempt = await TryLoadDefinitionTopologyAsync(artifactRef, focusRegionId).ConfigureAwait(false);
            if (loadAttempt.Topology is null)
            {
                _dispatcher.Post(() =>
                {
                    if (!_topologyByBrainId.TryGetValue(brainId, out var state))
                    {
                        state = new BrainCanvasTopologyState();
                        state.Regions.Add((uint)NbnConstants.InputRegionId);
                        state.Regions.Add((uint)NbnConstants.OutputRegionId);
                        _topologyByBrainId[brainId] = state;
                    }

                    state.DefinitionSource = resolvedReference.Source;
                    state.DefinitionLoadStatus = $"Definition {definitionShaHex[..8]} not found in {loadAttempt.RootsTried.Count} candidate roots.";
                    state.LastDefinitionRootsTried.Clear();
                    state.LastDefinitionRootsTried.AddRange(loadAttempt.RootsTried.Take(12));
                });
                return;
            }

            var loaded = loadAttempt.Topology.Value;

            _dispatcher.Post(() =>
            {
                if (!_topologyByBrainId.TryGetValue(brainId, out var state))
                {
                    state = new BrainCanvasTopologyState();
                    state.Regions.Add((uint)NbnConstants.InputRegionId);
                    state.Regions.Add((uint)NbnConstants.OutputRegionId);
                    _topologyByBrainId[brainId] = state;
                }

                if (state.HasDefinitionRegionTopology
                    && string.Equals(state.DefinitionShaHex, definitionShaHex, StringComparison.OrdinalIgnoreCase)
                    && ((focusRegionId.HasValue && state.DefinitionFocusRegions.Contains(focusRegionId.Value))
                        || (!focusRegionId.HasValue && state.HasDefinitionFullTopology)))
                {
                    return;
                }

                if (!string.Equals(state.DefinitionShaHex, definitionShaHex, StringComparison.OrdinalIgnoreCase))
                {
                    state.Regions.Clear();
                    state.RegionRoutes.Clear();
                    state.NeuronAddresses.Clear();
                    state.NeuronRoutes.Clear();
                    state.DefinitionFocusRegions.Clear();
                    state.HasDefinitionFullTopology = false;
                    state.Regions.Add((uint)NbnConstants.InputRegionId);
                    state.Regions.Add((uint)NbnConstants.OutputRegionId);
                }

                state.DefinitionShaHex = definitionShaHex;
                state.DefinitionSource = resolvedReference.Source;
                state.DefinitionLoadStatus = $"Loaded definition topology ({(focusRegionId.HasValue ? $"focus R{focusRegionId.Value}" : "full brain")}).";
                state.LastDefinitionRootsTried.Clear();
                state.LastDefinitionRootsTried.AddRange(loadAttempt.RootsTried.Take(12));
                state.HasDefinitionRegionTopology = true;
                state.Regions.UnionWith(loaded.Regions);
                state.RegionRoutes.UnionWith(loaded.RegionRoutes);

                if (focusRegionId.HasValue)
                {
                    state.DefinitionFocusRegions.Add(focusRegionId.Value);
                    state.NeuronAddresses.UnionWith(loaded.NeuronAddresses);
                    state.NeuronRoutes.UnionWith(loaded.NeuronRoutes);
                }
                else
                {
                    state.HasDefinitionFullTopology = true;
                }

                if (SelectedBrain?.BrainId == brainId)
                {
                    _nextDefinitionHydrationRetryUtc = DateTime.MinValue;
                    RefreshCanvasLayoutOnly();
                }
            });
        }
        catch (Exception ex)
        {
            _dispatcher.Post(() =>
            {
                if (SelectedBrain?.BrainId == brainId)
                {
                    Status = $"Definition topology load failed: {ex.Message}";
                }
            });
        }
        finally
        {
            _definitionTopologyGate.Release();
            lock (_pendingDefinitionHydrationGate)
            {
                _pendingDefinitionHydrationKeys.Remove(hydrationKey);
            }
        }
    }

    private void EnsureDefinitionTopologyCoverage()
    {
        if (SelectedBrain is null)
        {
            return;
        }

        var focusRegionId = TryParseRegionId(RegionFocusText, out var parsedFocusRegionId)
            ? parsedFocusRegionId
            : (uint?)null;
        if (_topologyByBrainId.TryGetValue(SelectedBrain.BrainId, out var state)
            && state.HasDefinitionRegionTopology
            && ((focusRegionId.HasValue && state.DefinitionFocusRegions.Contains(focusRegionId.Value))
                || (!focusRegionId.HasValue && state.HasDefinitionFullTopology)))
        {
            return;
        }

        var now = DateTime.UtcNow;
        if (now < _nextDefinitionHydrationRetryUtc)
        {
            return;
        }

        _nextDefinitionHydrationRetryUtc = now + DefinitionHydrationRetryInterval;
        QueueDefinitionTopologyHydration(SelectedBrain.BrainId, focusRegionId);
    }

    private static string BuildDefinitionHydrationKey(Guid brainId, uint? focusRegionId)
        => focusRegionId.HasValue
            ? $"{brainId:D}:focus:{focusRegionId.Value}"
            : $"{brainId:D}:full";

    private async Task<DefinitionReferenceResolution> ResolveDefinitionArtifactReferenceAsync(Guid brainId)
    {
        var exported = await _brain.ExportBrainDefinitionReferenceAsync(brainId, rebaseOverlays: false).ConfigureAwait(false);
        if (exported is not null && exported.TryToSha256Bytes(out _))
        {
            return new DefinitionReferenceResolution(exported, "export");
        }

        var info = await _brain.RequestBrainInfoAsync(brainId).ConfigureAwait(false);
        if (info?.BaseDefinition is { } baseDefinition && baseDefinition.TryToSha256Bytes(out _))
        {
            return new DefinitionReferenceResolution(baseDefinition, "brain-info");
        }

        return new DefinitionReferenceResolution(null, "none");
    }

    private static async Task<DefinitionTopologyLoadAttempt> TryLoadDefinitionTopologyAsync(Nbn.Proto.ArtifactRef artifactRef, uint? focusRegionId)
    {
        if (!artifactRef.TryToSha256Bytes(out var shaBytes))
        {
            return new DefinitionTopologyLoadAttempt(null, Array.Empty<string>());
        }

        var hash = Sha256Hash.FromBytes(shaBytes);
        var candidateRoots = ResolveArtifactStoreRoots(artifactRef.StoreUri);
        foreach (var artifactRoot in candidateRoots)
        {
            try
            {
                var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
                await using var stream = await store.TryOpenArtifactAsync(hash).ConfigureAwait(false);
                if (stream is null)
                {
                    continue;
                }

                using var buffer = new MemoryStream();
                await stream.CopyToAsync(buffer).ConfigureAwait(false);
                return new DefinitionTopologyLoadAttempt(BuildDefinitionTopology(buffer.ToArray(), focusRegionId), candidateRoots);
            }
            catch
            {
                // Continue probing other candidate roots.
            }
        }

        return new DefinitionTopologyLoadAttempt(null, candidateRoots);
    }

    private static DefinitionTopologyLoadResult BuildDefinitionTopology(byte[] definitionBytes, uint? focusRegionId)
    {
        var header = NbnBinary.ReadNbnHeader(definitionBytes);
        var regions = new HashSet<uint>();
        var regionRoutes = new HashSet<VizActivityCanvasRegionRoute>();
        var neuronAddresses = new HashSet<uint>();
        var neuronRoutes = new HashSet<VizActivityCanvasNeuronRoute>();

        for (var regionId = (uint)NbnConstants.RegionMinId; regionId <= NbnConstants.RegionMaxId; regionId++)
        {
            var entry = header.Regions[(int)regionId];
            if (entry.NeuronSpan == 0 || entry.Offset == 0)
            {
                continue;
            }

            regions.Add(regionId);
            var section = NbnBinary.ReadNbnRegionSection(definitionBytes, entry.Offset);
            var axonCursor = 0;
            for (var neuronIndex = 0; neuronIndex < section.NeuronRecords.Length; neuronIndex++)
            {
                var neuron = section.NeuronRecords[neuronIndex];
                var sourceAddress = ComposeAddressForTopology(regionId, (uint)neuronIndex);
                if (focusRegionId.HasValue && focusRegionId.Value == regionId)
                {
                    neuronAddresses.Add(sourceAddress);
                }

                var axonCount = (int)neuron.AxonCount;
                for (var offset = 0; offset < axonCount && axonCursor < section.AxonRecords.Length; offset++, axonCursor++)
                {
                    var axon = section.AxonRecords[axonCursor];
                    var targetRegion = (uint)axon.TargetRegionId;
                    var targetAddress = ComposeAddressForTopology(targetRegion, (uint)Math.Max(0, axon.TargetNeuronId));
                    regionRoutes.Add(new VizActivityCanvasRegionRoute(regionId, targetRegion));

                    if (!focusRegionId.HasValue)
                    {
                        continue;
                    }

                    if (regionId != focusRegionId.Value && targetRegion != focusRegionId.Value)
                    {
                        continue;
                    }

                    neuronRoutes.Add(new VizActivityCanvasNeuronRoute(sourceAddress, targetAddress));
                    if (regionId == focusRegionId.Value)
                    {
                        neuronAddresses.Add(sourceAddress);
                    }

                    if (targetRegion == focusRegionId.Value)
                    {
                        neuronAddresses.Add(targetAddress);
                    }
                }
            }
        }

        regions.Add((uint)NbnConstants.InputRegionId);
        regions.Add((uint)NbnConstants.OutputRegionId);
        return new DefinitionTopologyLoadResult(regions, regionRoutes, neuronAddresses, neuronRoutes);
    }

    private static IReadOnlyList<string> ResolveArtifactStoreRoots(string? storeUri)
    {
        var roots = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        static string? NormalizeStoreRoot(string? raw)
        {
            if (string.IsNullOrWhiteSpace(raw))
            {
                return null;
            }

            var trimmed = raw.Trim().Trim('"');
            if (trimmed.Length == 0)
            {
                return null;
            }

            if (Uri.TryCreate(trimmed, UriKind.Absolute, out var uri) && uri.IsFile)
            {
                return uri.LocalPath;
            }

            return trimmed;
        }

        static string BuildDefaultRoot(string suffix)
        {
            var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
            return Path.Combine(localAppData, "Nbn.Workbench", suffix);
        }

        static bool IsArtifactStoreRoot(string path)
            => File.Exists(Path.Combine(path, "artifacts.db"))
               || Directory.Exists(Path.Combine(path, "chunks"));

        void AddCandidate(string? candidate)
        {
            var normalized = NormalizeStoreRoot(candidate);
            if (string.IsNullOrWhiteSpace(normalized))
            {
                return;
            }

            string fullPath;
            try
            {
                fullPath = Path.GetFullPath(normalized);
            }
            catch
            {
                return;
            }

            if (!Directory.Exists(fullPath))
            {
                return;
            }

            var queue = new Queue<(string Path, int Depth)>();
            queue.Enqueue((fullPath, 0));
            while (queue.Count > 0)
            {
                var (path, depth) = queue.Dequeue();
                if (!Directory.Exists(path))
                {
                    continue;
                }

                if (IsArtifactStoreRoot(path) && seen.Add(path))
                {
                    roots.Add(path);
                }

                if (depth >= 2)
                {
                    continue;
                }

                try
                {
                    foreach (var child in Directory.EnumerateDirectories(path))
                    {
                        queue.Enqueue((child, depth + 1));
                    }
                }
                catch
                {
                    // Ignore permission or IO failures while scanning candidates.
                }
            }
        }

        AddCandidate(storeUri);
        AddCandidate(RepoLocator.ResolvePathFromRepo("artifacts"));
        AddCandidate(RepoLocator.ResolvePathFromRepo("tools", "demo", "local-demo"));
        AddCandidate(RepoLocator.ResolvePathFromRepo("tools", "demo", "local-demo", "artifacts"));
        AddCandidate(Directory.GetCurrentDirectory());
        AddCandidate(AppContext.BaseDirectory);
        AddCandidate(BuildDefaultRoot("designer-artifacts"));
        AddCandidate(BuildDefaultRoot(Path.Combine("sample-brain", "artifacts")));
        AddCandidate(BuildDefaultRoot("repro-artifacts"));
        return roots;
    }

    private int ParseTickWindowOrDefault()
    {
        return TryParseTickWindow(TickWindowText, out var tickWindow)
            ? tickWindow
            : DefaultTickWindow;
    }

    private static void ReplaceItems<T>(ObservableCollection<T> target, IReadOnlyList<T> source)
    {
        target.Clear();
        foreach (var item in source)
        {
            target.Add(item);
        }
    }

    private static CollectionDiffStats ApplyKeyedDiff<T>(
        ObservableCollection<T> target,
        IReadOnlyList<T> source,
        Func<T, string?> keySelector)
    {
        if (source.Count == 0)
        {
            var removedCount = target.Count;
            if (removedCount > 0)
            {
                target.Clear();
            }

            return new CollectionDiffStats(0, removedCount, 0, 0);
        }

        var sourceKeys = new List<string>(source.Count);
        var sourceKeySet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var item in source)
        {
            var key = keySelector(item);
            if (string.IsNullOrWhiteSpace(key) || !sourceKeySet.Add(key))
            {
                return ReplaceItemsWithStats(target, source);
            }

            sourceKeys.Add(key);
        }

        var removed = 0;
        var added = 0;
        var moved = 0;
        var updated = 0;
        for (var i = target.Count - 1; i >= 0; i--)
        {
            var existingKey = keySelector(target[i]);
            if (!string.IsNullOrWhiteSpace(existingKey) && sourceKeySet.Contains(existingKey))
            {
                continue;
            }

            target.RemoveAt(i);
            removed++;
        }

        for (var desiredIndex = 0; desiredIndex < source.Count; desiredIndex++)
        {
            var desiredItem = source[desiredIndex];
            var desiredKey = sourceKeys[desiredIndex];
            var hasMatchingItemAtDesiredIndex = desiredIndex < target.Count
                && string.Equals(keySelector(target[desiredIndex]), desiredKey, StringComparison.OrdinalIgnoreCase);
            if (hasMatchingItemAtDesiredIndex)
            {
                if (!EqualityComparer<T>.Default.Equals(target[desiredIndex], desiredItem))
                {
                    target[desiredIndex] = desiredItem;
                    updated++;
                }

                continue;
            }

            var existingIndex = -1;
            for (var scan = desiredIndex + 1; scan < target.Count; scan++)
            {
                if (string.Equals(keySelector(target[scan]), desiredKey, StringComparison.OrdinalIgnoreCase))
                {
                    existingIndex = scan;
                    break;
                }
            }

            if (existingIndex >= 0)
            {
                target.Move(existingIndex, desiredIndex);
                moved++;
                if (!EqualityComparer<T>.Default.Equals(target[desiredIndex], desiredItem))
                {
                    target[desiredIndex] = desiredItem;
                    updated++;
                }

                continue;
            }

            target.Insert(desiredIndex, desiredItem);
            added++;
        }

        while (target.Count > source.Count)
        {
            target.RemoveAt(target.Count - 1);
            removed++;
        }

        return new CollectionDiffStats(added, removed, moved, updated);
    }

    private static CollectionDiffStats ReplaceItemsWithStats<T>(ObservableCollection<T> target, IReadOnlyList<T> source)
    {
        var removedCount = target.Count;
        target.Clear();
        foreach (var item in source)
        {
            target.Add(item);
        }

        return new CollectionDiffStats(source.Count, removedCount, 0, 0);
    }

    private VizActivityCanvasTopology BuildTopologySnapshotForSelectedBrain()
    {
        if (SelectedBrain is null || !_topologyByBrainId.TryGetValue(SelectedBrain.BrainId, out var state))
        {
            return new VizActivityCanvasTopology(
                new HashSet<uint> { (uint)NbnConstants.InputRegionId, (uint)NbnConstants.OutputRegionId },
                new HashSet<VizActivityCanvasRegionRoute>(),
                new HashSet<uint>(),
                new HashSet<VizActivityCanvasNeuronRoute>());
        }

        var regions = new HashSet<uint>(state.Regions) { (uint)NbnConstants.InputRegionId, (uint)NbnConstants.OutputRegionId };
        return new VizActivityCanvasTopology(
            regions,
            new HashSet<VizActivityCanvasRegionRoute>(state.RegionRoutes),
            new HashSet<uint>(state.NeuronAddresses),
            new HashSet<VizActivityCanvasNeuronRoute>(state.NeuronRoutes));
    }

    private void AccumulateTopology(VizEventItem item)
    {
        if (!Guid.TryParse(item.BrainId, out var brainId))
        {
            return;
        }

        if (!_topologyByBrainId.TryGetValue(brainId, out var state))
        {
            state = new BrainCanvasTopologyState();
            state.Regions.Add((uint)NbnConstants.InputRegionId);
            state.Regions.Add((uint)NbnConstants.OutputRegionId);
            _topologyByBrainId[brainId] = state;
        }

        if (TryParseRegionForTopology(item.Region, out var eventRegion))
        {
            state.Regions.Add(eventRegion);
        }

        var hasSource = TryParseAddressForTopology(item.Source, out var sourceAddress);
        var hasTarget = TryParseAddressForTopology(item.Target, out var targetAddress);
        if (hasSource)
        {
            var sourceRegion = sourceAddress >> NbnConstants.AddressNeuronBits;
            state.Regions.Add(sourceRegion);
            state.NeuronAddresses.Add(sourceAddress);
        }

        if (hasTarget)
        {
            var targetRegion = targetAddress >> NbnConstants.AddressNeuronBits;
            state.Regions.Add(targetRegion);
            state.NeuronAddresses.Add(targetAddress);
        }

        if (hasSource && hasTarget)
        {
            var sourceRegion = sourceAddress >> NbnConstants.AddressNeuronBits;
            var targetRegion = targetAddress >> NbnConstants.AddressNeuronBits;
            state.RegionRoutes.Add(new VizActivityCanvasRegionRoute(sourceRegion, targetRegion));
            state.NeuronRoutes.Add(new VizActivityCanvasNeuronRoute(sourceAddress, targetAddress));
        }
    }

    private void NavigateCanvasRelative(int delta)
    {
        if (CanvasNodes.Count == 0)
        {
            Status = "No canvas nodes available for navigation.";
            return;
        }

        var ordered = CanvasNodes
            .OrderByDescending(item => item.EventCount)
            .ThenByDescending(item => item.LastTick)
            .ThenBy(item => item.RegionId)
            .ThenBy(item => item.NodeKey, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var currentIndex = !string.IsNullOrWhiteSpace(_selectedCanvasNodeKey)
            ? ordered.FindIndex(item => string.Equals(item.NodeKey, _selectedCanvasNodeKey, StringComparison.OrdinalIgnoreCase))
            : delta >= 0 ? -1 : 0;
        var nextIndex = ((currentIndex + delta) % ordered.Count + ordered.Count) % ordered.Count;
        var next = ordered[nextIndex];
        SelectCanvasNode(next);
        Status = $"Canvas selection: {next.Label}.";
    }

    private void NavigateToCanvasSelection()
    {
        var regionId = GetCurrentSelectionRegionId(CanvasNodes, CanvasEdges);
        if (!regionId.HasValue)
        {
            Status = "Select a node or route before navigating focus.";
            return;
        }

        RegionFocusText = regionId.Value.ToString(CultureInfo.InvariantCulture);
        RefreshFilteredEvents();
        if (SelectedBrain is not null)
        {
            QueueDefinitionTopologyHydration(SelectedBrain.BrainId, regionId.Value);
        }
        Status = $"Canvas navigation focused region {regionId.Value}.";
    }

    private void TogglePinForCurrentSelection()
    {
        if (!string.IsNullOrWhiteSpace(_selectedCanvasNodeKey))
        {
            var nodeKey = _selectedCanvasNodeKey!;
            var node = CanvasNodes.FirstOrDefault(item => string.Equals(item.NodeKey, nodeKey, StringComparison.OrdinalIgnoreCase));
            if (!_pinnedCanvasNodes.Add(nodeKey))
            {
                _pinnedCanvasNodes.Remove(nodeKey);
            }

            RefreshCanvasLayoutOnly();
            var label = node?.Label ?? nodeKey;
            Status = _pinnedCanvasNodes.Contains(nodeKey)
                ? $"Pinned node {label}."
                : $"Unpinned node {label}.";
            return;
        }

        if (!string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel))
        {
            var routeLabel = _selectedCanvasRouteLabel!;
            if (!_pinnedCanvasRoutes.Add(routeLabel))
            {
                _pinnedCanvasRoutes.Remove(routeLabel);
            }

            RefreshCanvasLayoutOnly();
            Status = _pinnedCanvasRoutes.Contains(routeLabel)
                ? $"Pinned route {routeLabel}."
                : $"Unpinned route {routeLabel}.";
            return;
        }

        Status = "Select a node or route before pinning.";
    }

    private void ClearCanvasInteraction()
    {
        ResetCanvasInteractionState(clearPins: true);
        RefreshCanvasLayoutOnly();
        Status = "Canvas interaction reset.";
    }

    private void ClearCanvasSelection()
    {
        if (string.IsNullOrWhiteSpace(_selectedCanvasNodeKey) && string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel))
        {
            return;
        }

        _selectedCanvasNodeKey = null;
        _selectedCanvasRouteLabel = null;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
        RefreshCanvasLayoutOnly();
    }

    private void ResetCanvasInteractionState(bool clearPins)
    {
        _selectedCanvasNodeKey = null;
        _selectedCanvasRouteLabel = null;
        _hoverCanvasNodeKey = null;
        _hoverCanvasRouteLabel = null;
        CanvasHoverCardText = string.Empty;
        IsCanvasHoverCardVisible = false;
        if (clearPins)
        {
            _pinnedCanvasNodes.Clear();
            _pinnedCanvasRoutes.Clear();
        }

        OnPropertyChanged(nameof(TogglePinSelectionLabel));
    }

    private bool TrimCanvasInteractionToLayout(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges)
    {
        var changed = false;
        var validNodes = new HashSet<string>(
            nodes.Select(item => item.NodeKey).Where(key => !string.IsNullOrWhiteSpace(key)),
            StringComparer.OrdinalIgnoreCase);
        var validRoutes = new HashSet<string>(
            edges.Select(item => item.RouteLabel).Where(label => !string.IsNullOrWhiteSpace(label)),
            StringComparer.OrdinalIgnoreCase);

        if (!string.IsNullOrWhiteSpace(_selectedCanvasNodeKey) && !validNodes.Contains(_selectedCanvasNodeKey!))
        {
            _selectedCanvasNodeKey = null;
            changed = true;
        }

        if (!string.IsNullOrWhiteSpace(_hoverCanvasNodeKey) && !validNodes.Contains(_hoverCanvasNodeKey!))
        {
            _hoverCanvasNodeKey = null;
            changed = true;
        }

        if (!string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel) && !validRoutes.Contains(_selectedCanvasRouteLabel!))
        {
            _selectedCanvasRouteLabel = null;
            changed = true;
        }

        if (!string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel) && !validRoutes.Contains(_hoverCanvasRouteLabel!))
        {
            _hoverCanvasRouteLabel = null;
            changed = true;
        }

        var pinnedNodesBefore = _pinnedCanvasNodes.Count;
        var pinnedRoutesBefore = _pinnedCanvasRoutes.Count;
        _pinnedCanvasNodes.RemoveWhere(key => !validNodes.Contains(key));
        _pinnedCanvasRoutes.RemoveWhere(route => !validRoutes.Contains(route));
        if (_pinnedCanvasNodes.Count != pinnedNodesBefore || _pinnedCanvasRoutes.Count != pinnedRoutesBefore)
        {
            changed = true;
        }

        return changed;
    }

    private void UpdateCanvasHoverCard(string detail, double pointerX, double pointerY)
    {
        CanvasHoverCardText = detail;

        if (!double.IsNaN(pointerX) && !double.IsNaN(pointerY))
        {
            CanvasHoverCardLeft = Clamp(pointerX + HoverCardOffset, 4.0, Math.Max(4.0, ActivityCanvasWidth - HoverCardMaxWidth));
            CanvasHoverCardTop = Clamp(pointerY + HoverCardOffset, 4.0, Math.Max(4.0, ActivityCanvasHeight - HoverCardMaxHeight));
        }

        IsCanvasHoverCardVisible = !string.IsNullOrWhiteSpace(detail);
    }

    private void RefreshCanvasHoverCard(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges)
    {
        var hoverNode = !string.IsNullOrWhiteSpace(_hoverCanvasNodeKey)
            ? nodes.FirstOrDefault(item => string.Equals(item.NodeKey, _hoverCanvasNodeKey, StringComparison.OrdinalIgnoreCase))
            : null;
        if (hoverNode is not null)
        {
            UpdateCanvasHoverCard(hoverNode.Detail, double.NaN, double.NaN);
            return;
        }

        var hoverEdge = !string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel)
            ? edges.FirstOrDefault(item => string.Equals(item.RouteLabel, _hoverCanvasRouteLabel, StringComparison.OrdinalIgnoreCase))
            : null;
        if (hoverEdge is not null)
        {
            UpdateCanvasHoverCard(hoverEdge.Detail, double.NaN, double.NaN);
            return;
        }

        CanvasHoverCardText = string.Empty;
        IsCanvasHoverCardVisible = false;
    }

    private void UpdateCanvasInteractionSummaries(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges)
    {
        var selectedNode = !string.IsNullOrWhiteSpace(_selectedCanvasNodeKey)
            ? nodes.FirstOrDefault(item => string.Equals(item.NodeKey, _selectedCanvasNodeKey, StringComparison.OrdinalIgnoreCase))
            : null;
        var selectedEdge = !string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel)
            ? edges.FirstOrDefault(item => string.Equals(item.RouteLabel, _selectedCanvasRouteLabel, StringComparison.OrdinalIgnoreCase))
            : null;

        var hoverNode = !string.IsNullOrWhiteSpace(_hoverCanvasNodeKey)
            ? nodes.FirstOrDefault(item => string.Equals(item.NodeKey, _hoverCanvasNodeKey, StringComparison.OrdinalIgnoreCase))
            : null;
        var hoverEdge = !string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel)
            ? edges.FirstOrDefault(item => string.Equals(item.RouteLabel, _hoverCanvasRouteLabel, StringComparison.OrdinalIgnoreCase))
            : null;

        var selectedSummary = selectedNode is not null
            ? $"Selected node {selectedNode.Label} (events {selectedNode.EventCount}, tick {selectedNode.LastTick})"
            : selectedEdge is not null
                ? $"Selected route {selectedEdge.RouteLabel} (events {selectedEdge.EventCount}, tick {selectedEdge.LastTick})"
                : "Selected: none";
        var hoverSummary = hoverNode is not null
            ? $"Hover node {hoverNode.Label} (events {hoverNode.EventCount}, tick {hoverNode.LastTick})"
            : hoverEdge is not null
                ? $"Hover route {hoverEdge.RouteLabel} (events {hoverEdge.EventCount}, tick {hoverEdge.LastTick})"
                : "Hover: none";

        ActivityInteractionSummary = $"{selectedSummary} | {hoverSummary}";
        ActivityPinnedSummary = BuildPinnedSummary(nodes, edges);
    }

    private static string BuildPinnedSummary(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges)
    {
        var pinnedRegions = nodes
            .Where(item => item.IsPinned)
            .OrderBy(item => item.RegionId)
            .Select(item => item.Label)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Take(6)
            .ToList();
        var pinnedRoutes = edges
            .Where(item => item.IsPinned)
            .Select(item => item.RouteLabel)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Take(3)
            .ToList();

        var pinnedRegionCount = nodes.Count(item => item.IsPinned);
        var pinnedRouteCount = edges.Count(item => item.IsPinned);
        if (pinnedRegionCount == 0 && pinnedRouteCount == 0)
        {
            return "Pinned: none.";
        }

        var regionSuffix = pinnedRegionCount > pinnedRegions.Count ? $" (+{pinnedRegionCount - pinnedRegions.Count})" : string.Empty;
        var routeSuffix = pinnedRouteCount > pinnedRoutes.Count ? $" (+{pinnedRouteCount - pinnedRoutes.Count})" : string.Empty;
        var regionText = pinnedRegions.Count == 0 ? "none" : string.Join(", ", pinnedRegions) + regionSuffix;
        var routeText = pinnedRoutes.Count == 0 ? "none" : string.Join(" | ", pinnedRoutes) + routeSuffix;
        return $"Pinned nodes: {regionText} | routes: {routeText}";
    }

    private void CancelPendingHoverClear()
    {
        Interlocked.Increment(ref _hoverClearRevision);
    }

    private void RebuildCanvasHitIndex(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges)
    {
        _canvasNodeSnapshot = nodes;
        _canvasEdgeSnapshot = edges;
        _nodeHitIndex.Clear();
        _edgeHitIndex.Clear();
        _nodeHitCandidates.Clear();
        _edgeHitCandidates.Clear();
        _canvasNodeByKey.Clear();
        _canvasEdgeByRoute.Clear();

        for (var index = 0; index < nodes.Count; index++)
        {
            var node = nodes[index];
            if (!string.IsNullOrWhiteSpace(node.NodeKey))
            {
                _canvasNodeByKey[node.NodeKey] = node;
            }

            AddIndexEntry(
                _nodeHitIndex,
                index,
                node.Left - NodeHitPadding,
                node.Top - NodeHitPadding,
                node.Left + node.Diameter + NodeHitPadding,
                node.Top + node.Diameter + NodeHitPadding);
        }

        for (var index = 0; index < edges.Count; index++)
        {
            var edge = edges[index];
            if (!string.IsNullOrWhiteSpace(edge.RouteLabel))
            {
                _canvasEdgeByRoute[edge.RouteLabel] = edge;
            }

            var threshold = Math.Max(4.0, edge.HitTestThickness * 0.5) + EdgeHitIndexPadding;
            var minX = Math.Min(edge.SourceX, Math.Min(edge.ControlX, edge.TargetX)) - threshold;
            var minY = Math.Min(edge.SourceY, Math.Min(edge.ControlY, edge.TargetY)) - threshold;
            var maxX = Math.Max(edge.SourceX, Math.Max(edge.ControlX, edge.TargetX)) + threshold;
            var maxY = Math.Max(edge.SourceY, Math.Max(edge.ControlY, edge.TargetY)) + threshold;

            AddIndexEntry(_edgeHitIndex, index, minX, minY, maxX, maxY);
        }
    }

    private static void AddIndexEntry(
        IDictionary<long, List<int>> index,
        int itemIndex,
        double minX,
        double minY,
        double maxX,
        double maxY)
    {
        if (!double.IsFinite(minX)
            || !double.IsFinite(minY)
            || !double.IsFinite(maxX)
            || !double.IsFinite(maxY))
        {
            return;
        }

        var startX = (int)Math.Floor(minX / HitTestCellSize);
        var endX = (int)Math.Floor(maxX / HitTestCellSize);
        var startY = (int)Math.Floor(minY / HitTestCellSize);
        var endY = (int)Math.Floor(maxY / HitTestCellSize);

        for (var cellX = startX; cellX <= endX; cellX++)
        {
            for (var cellY = startY; cellY <= endY; cellY++)
            {
                var key = CellKey(cellX, cellY);
                if (!index.TryGetValue(key, out var entries))
                {
                    entries = new List<int>(4);
                    index[key] = entries;
                }

                entries.Add(itemIndex);
            }
        }
    }

    private static void CollectHitCandidates(
        IReadOnlyDictionary<long, List<int>> index,
        double pointerX,
        double pointerY,
        ISet<int> candidates)
    {
        candidates.Clear();

        var cellX = (int)Math.Floor(pointerX / HitTestCellSize);
        var cellY = (int)Math.Floor(pointerY / HitTestCellSize);
        for (var dx = -1; dx <= 1; dx++)
        {
            for (var dy = -1; dy <= 1; dy++)
            {
                if (!index.TryGetValue(CellKey(cellX + dx, cellY + dy), out var entries))
                {
                    continue;
                }

                foreach (var indexValue in entries)
                {
                    candidates.Add(indexValue);
                }
            }
        }
    }

    private static long CellKey(int cellX, int cellY)
        => ((long)cellX << 32) ^ (uint)cellY;

    private bool TryResolveStickyHoverHit(
        double pointerX,
        double pointerY,
        out VizActivityCanvasNode? node,
        out VizActivityCanvasEdge? edge)
    {
        if (!string.IsNullOrWhiteSpace(_hoverCanvasNodeKey)
            && _canvasNodeByKey.TryGetValue(_hoverCanvasNodeKey!, out var hoveredNode))
        {
            var centerX = hoveredNode.Left + (hoveredNode.Diameter / 2.0);
            var centerY = hoveredNode.Top + (hoveredNode.Diameter / 2.0);
            var radius = (hoveredNode.Diameter / 2.0) + StickyNodeHitPadding;
            var dx = pointerX - centerX;
            var dy = pointerY - centerY;
            if ((dx * dx) + (dy * dy) <= radius * radius)
            {
                node = hoveredNode;
                edge = null;
                return true;
            }
        }

        if (!string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel)
            && _canvasEdgeByRoute.TryGetValue(_hoverCanvasRouteLabel!, out var hoveredEdge))
        {
            var threshold = Math.Max(4.0, hoveredEdge.HitTestThickness * 0.5) + StickyEdgeHitPadding;
            var distance = DistanceToQuadraticBezier(
                pointerX,
                pointerY,
                hoveredEdge.SourceX,
                hoveredEdge.SourceY,
                hoveredEdge.ControlX,
                hoveredEdge.ControlY,
                hoveredEdge.TargetX,
                hoveredEdge.TargetY);
            if (distance <= threshold)
            {
                node = null;
                edge = hoveredEdge;
                return true;
            }
        }

        node = null;
        edge = null;
        return false;
    }

    private VizActivityCanvasNode? HitTestCanvasNode(double pointerX, double pointerY)
    {
        if (_canvasNodeSnapshot.Count == 0)
        {
            return null;
        }

        CollectHitCandidates(_nodeHitIndex, pointerX, pointerY, _nodeHitCandidates);
        var shouldFallbackToFullScan = _nodeHitCandidates.Count == 0 && _canvasNodeSnapshot.Count <= 10;

        VizActivityCanvasNode? best = null;
        var bestDistance = double.MaxValue;
        if (_nodeHitCandidates.Count > 0)
        {
            foreach (var index in _nodeHitCandidates)
            {
                var node = _canvasNodeSnapshot[index];
                var centerX = node.Left + (node.Diameter / 2.0);
                var centerY = node.Top + (node.Diameter / 2.0);
                var radius = (node.Diameter / 2.0) + NodeHitPadding;
                var dx = pointerX - centerX;
                var dy = pointerY - centerY;
                var distanceSquared = (dx * dx) + (dy * dy);
                var radiusSquared = radius * radius;
                if (distanceSquared > radiusSquared)
                {
                    continue;
                }

                if (best is null
                    || distanceSquared < (bestDistance - HitDistanceTieEpsilon)
                    || (Math.Abs(distanceSquared - bestDistance) <= HitDistanceTieEpsilon
                        && string.Compare(node.NodeKey, best.NodeKey, StringComparison.OrdinalIgnoreCase) < 0))
                {
                    bestDistance = distanceSquared;
                    best = node;
                }
            }

            return best;
        }

        if (!shouldFallbackToFullScan)
        {
            return null;
        }

        foreach (var node in _canvasNodeSnapshot)
        {
            var centerX = node.Left + (node.Diameter / 2.0);
            var centerY = node.Top + (node.Diameter / 2.0);
            var radius = (node.Diameter / 2.0) + NodeHitPadding;
            var dx = pointerX - centerX;
            var dy = pointerY - centerY;
            var distanceSquared = (dx * dx) + (dy * dy);
            var radiusSquared = radius * radius;
            if (distanceSquared > radiusSquared)
            {
                continue;
            }

            if (best is null
                || distanceSquared < (bestDistance - HitDistanceTieEpsilon)
                || (Math.Abs(distanceSquared - bestDistance) <= HitDistanceTieEpsilon
                    && string.Compare(node.NodeKey, best.NodeKey, StringComparison.OrdinalIgnoreCase) < 0))
            {
                bestDistance = distanceSquared;
                best = node;
            }
        }

        return best;
    }

    private VizActivityCanvasEdge? HitTestCanvasEdge(double pointerX, double pointerY)
    {
        if (_canvasEdgeSnapshot.Count == 0)
        {
            return null;
        }

        CollectHitCandidates(_edgeHitIndex, pointerX, pointerY, _edgeHitCandidates);
        var shouldFallbackToFullScan = _edgeHitCandidates.Count == 0 && _canvasEdgeSnapshot.Count <= 6;

        VizActivityCanvasEdge? best = null;
        var bestDistance = double.MaxValue;
        if (_edgeHitCandidates.Count > 0)
        {
            foreach (var index in _edgeHitCandidates)
            {
                var edge = _canvasEdgeSnapshot[index];
                var threshold = Math.Max(4.0, edge.HitTestThickness * 0.5);
                var distance = DistanceToQuadraticBezier(
                    pointerX,
                    pointerY,
                    edge.SourceX,
                    edge.SourceY,
                    edge.ControlX,
                    edge.ControlY,
                    edge.TargetX,
                    edge.TargetY);
                if (distance > threshold)
                {
                    continue;
                }

                if (best is null
                    || distance < (bestDistance - HitDistanceTieEpsilon)
                    || (Math.Abs(distance - bestDistance) <= HitDistanceTieEpsilon
                        && string.Compare(edge.RouteLabel, best.RouteLabel, StringComparison.OrdinalIgnoreCase) < 0))
                {
                    bestDistance = distance;
                    best = edge;
                }
            }

            return best;
        }

        if (!shouldFallbackToFullScan)
        {
            return null;
        }

        foreach (var edge in _canvasEdgeSnapshot)
        {
            var threshold = Math.Max(4.0, edge.HitTestThickness * 0.5);
            var distance = DistanceToQuadraticBezier(
                pointerX,
                pointerY,
                edge.SourceX,
                edge.SourceY,
                edge.ControlX,
                edge.ControlY,
                edge.TargetX,
                edge.TargetY);
            if (distance > threshold)
            {
                continue;
            }

            if (best is null
                || distance < (bestDistance - HitDistanceTieEpsilon)
                || (Math.Abs(distance - bestDistance) <= HitDistanceTieEpsilon
                    && string.Compare(edge.RouteLabel, best.RouteLabel, StringComparison.OrdinalIgnoreCase) < 0))
            {
                bestDistance = distance;
                best = edge;
            }
        }

        return best;
    }

    private static double DistanceToQuadraticBezier(
        double pointX,
        double pointY,
        double startX,
        double startY,
        double controlX,
        double controlY,
        double endX,
        double endY)
    {
        var minDistance = double.MaxValue;
        var previousX = startX;
        var previousY = startY;
        for (var sample = 1; sample <= EdgeHitSamples; sample++)
        {
            var t = (double)sample / EdgeHitSamples;
            var oneMinusT = 1.0 - t;
            var curveX = (oneMinusT * oneMinusT * startX) + (2.0 * oneMinusT * t * controlX) + (t * t * endX);
            var curveY = (oneMinusT * oneMinusT * startY) + (2.0 * oneMinusT * t * controlY) + (t * t * endY);
            var distance = DistanceToSegment(pointX, pointY, previousX, previousY, curveX, curveY);
            if (distance < minDistance)
            {
                minDistance = distance;
            }

            previousX = curveX;
            previousY = curveY;
        }

        return minDistance;
    }

    private static double DistanceToSegment(
        double pointX,
        double pointY,
        double startX,
        double startY,
        double endX,
        double endY)
    {
        var dx = endX - startX;
        var dy = endY - startY;
        var lengthSquared = (dx * dx) + (dy * dy);
        if (lengthSquared <= double.Epsilon)
        {
            var fx = pointX - startX;
            var fy = pointY - startY;
            return Math.Sqrt((fx * fx) + (fy * fy));
        }

        var projection = ((pointX - startX) * dx + (pointY - startY) * dy) / lengthSquared;
        var clamped = Math.Max(0.0, Math.Min(1.0, projection));
        var closestX = startX + (clamped * dx);
        var closestY = startY + (clamped * dy);
        var diffX = pointX - closestX;
        var diffY = pointY - closestY;
        return Math.Sqrt((diffX * diffX) + (diffY * diffY));
    }

    private void RecordHitTestDuration(long startTimestamp)
    {
        var elapsedMs = StopwatchElapsedMs(startTimestamp);
        _lastHitTestMs = elapsedMs;
        _hitTestSamples++;
        _avgHitTestMs += (elapsedMs - _avgHitTestMs) / _hitTestSamples;
        if (elapsedMs > _maxHitTestMs)
        {
            _maxHitTestMs = elapsedMs;
        }
    }

    private static double StopwatchElapsedMs(long startTimestamp)
        => ((double)(Stopwatch.GetTimestamp() - startTimestamp) * 1000.0) / Stopwatch.Frequency;

    private bool IsCurrentSelectionPinned()
    {
        if (!string.IsNullOrWhiteSpace(_selectedCanvasNodeKey))
        {
            return _pinnedCanvasNodes.Contains(_selectedCanvasNodeKey!);
        }

        return !string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel)
               && _pinnedCanvasRoutes.Contains(_selectedCanvasRouteLabel!);
    }

    private uint? GetCurrentSelectionRegionId(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges)
    {
        if (!string.IsNullOrWhiteSpace(_selectedCanvasNodeKey))
        {
            var selectedNode = nodes.FirstOrDefault(item => string.Equals(item.NodeKey, _selectedCanvasNodeKey, StringComparison.OrdinalIgnoreCase));
            return selectedNode?.NavigateRegionId;
        }

        if (string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel))
        {
            return null;
        }

        var selectedEdge = edges.FirstOrDefault(item => string.Equals(item.RouteLabel, _selectedCanvasRouteLabel, StringComparison.OrdinalIgnoreCase));
        return selectedEdge?.TargetRegionId ?? selectedEdge?.SourceRegionId;
    }

    private static uint ComposeAddressForTopology(uint regionId, uint neuronId)
        => (regionId << NbnConstants.AddressNeuronBits) | (neuronId & NbnConstants.AddressNeuronMask);

    private static bool TryParseRegionForTopology(string? value, out uint regionId)
    {
        regionId = 0;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        if (uint.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedNumeric))
        {
            if (parsedNumeric > NbnConstants.RegionMaxId)
            {
                return false;
            }

            regionId = parsedNumeric;
            return true;
        }

        if (!TryParseRegionToken(value, out var parsedToken, out _))
        {
            return false;
        }

        regionId = parsedToken;
        return true;
    }

    private static bool TryParseAddressForTopology(string? value, out uint address)
    {
        address = 0;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        if (uint.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            var parsedRegion = parsed >> NbnConstants.AddressNeuronBits;
            if (parsedRegion > NbnConstants.RegionMaxId)
            {
                return false;
            }

            address = parsed;
            return true;
        }

        if (!TryParseRegionToken(value, out var regionId, out var remainder)
            || string.IsNullOrWhiteSpace(remainder)
            || (remainder[0] != 'N' && remainder[0] != 'n'))
        {
            return false;
        }

        var neuronText = remainder[1..];
        if (!uint.TryParse(neuronText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var neuronId))
        {
            return false;
        }

        address = (regionId << NbnConstants.AddressNeuronBits) | (neuronId & NbnConstants.AddressNeuronMask);
        return true;
    }

    private static bool TryParseRegionToken(string? value, out uint regionId, out string remainder)
    {
        regionId = 0;
        remainder = string.Empty;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        if (trimmed.Length < 2 || (trimmed[0] != 'R' && trimmed[0] != 'r'))
        {
            return false;
        }

        var end = 1;
        while (end < trimmed.Length && char.IsDigit(trimmed[end]))
        {
            end++;
        }

        if (end == 1)
        {
            return false;
        }

        var number = trimmed[1..end];
        if (!uint.TryParse(number, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            || parsed > NbnConstants.RegionMaxId)
        {
            return false;
        }

        regionId = parsed;
        remainder = end < trimmed.Length ? trimmed[end..] : string.Empty;
        return true;
    }

    private static bool TryParseTickWindow(string? value, out int tickWindow)
    {
        tickWindow = DefaultTickWindow;
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return false;
        }

        if (parsed < 1 || parsed > MaxTickWindow)
        {
            return false;
        }

        tickWindow = parsed;
        return true;
    }

    private static bool TryParseRegionId(string? value, out uint regionId)
    {
        regionId = 0;
        if (!uint.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            if (!TryParseRegionToken(value, out parsed, out _))
            {
                return false;
            }
        }

        if (parsed > (uint)NbnConstants.RegionMaxId)
        {
            return false;
        }

        regionId = parsed;
        return true;
    }

    private static bool ContainsIgnoreCase(string? haystack, string needle)
    {
        if (string.IsNullOrEmpty(haystack))
        {
            return false;
        }

        return haystack.IndexOf(needle, StringComparison.OrdinalIgnoreCase) >= 0;
    }

    private static double Clamp(double value, double min, double max)
        => Math.Max(min, Math.Min(max, value));

    private static string BuildPayload(VizEventItem? item)
    {
        if (item is null)
        {
            return string.Empty;
        }

        return $"[{item.Type}] brain={item.BrainId} tick={item.TickId}\nregion={item.Region} source={item.Source} target={item.Target}\nvalue={item.Value} strength={item.Strength}\nEventId={item.EventId}";
    }

    private static string NormalizeDiagnosticText(string? value)
        => string.IsNullOrWhiteSpace(value) ? "<empty>" : value.Trim();

    private static async Task<bool> TrySetClipboardTextAsync(string text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        if (Application.Current?.ApplicationLifetime is IClassicDesktopStyleApplicationLifetime { MainWindow: { } mainWindow }
            && mainWindow.Clipboard is { } clipboard)
        {
            await clipboard.SetTextAsync(text);
            return true;
        }

        return false;
    }

    private static async Task<IStorageFile?> PickSaveFileAsync(string title, string filterName, string extension, string? suggestedName)
    {
        var provider = GetStorageProvider();
        if (provider is null)
        {
            return null;
        }

        var options = new FilePickerSaveOptions
        {
            Title = title,
            DefaultExtension = extension,
            SuggestedFileName = suggestedName,
            FileTypeChoices = new List<FilePickerFileType>
            {
                new(filterName) { Patterns = new List<string> { $"*.{extension}" } }
            }
        };

        return await provider.SaveFilePickerAsync(options);
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

    private static async Task WriteAllTextAsync(IStorageFile file, string content)
    {
        await using var stream = await file.OpenWriteAsync();
        stream.SetLength(0);
        await using var writer = new StreamWriter(stream, new UTF8Encoding(false));
        await writer.WriteAsync(content);
        await writer.FlushAsync();
        await stream.FlushAsync();
    }

    private static string FormatPath(IStorageItem item)
        => item.Path?.LocalPath ?? item.Path?.ToString() ?? item.Name;

    private static void Trim<T>(ICollection<T> collection)
    {
        while (collection.Count > MaxEvents && collection is IList<T> list)
        {
            list.RemoveAt(list.Count - 1);
        }
    }

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
        IReadOnlyList<string> RootsTried);

    private readonly record struct DefinitionReferenceResolution(
        Nbn.Proto.ArtifactRef? Reference,
        string Source);

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

public sealed record VizCanvasColorModeOption(string Label, VizActivityCanvasColorMode Mode, string LegendHint)
{
    public static IReadOnlyList<VizCanvasColorModeOption> CreateDefaults()
        => new[]
        {
            new VizCanvasColorModeOption(
                "State priority",
                VizActivityCanvasColorMode.StateValue,
                "fill=value sign/magnitude, pulse=activity, border=topology"),
            new VizCanvasColorModeOption(
                "Activity priority",
                VizActivityCanvasColorMode.Activity,
                "fill=activity load/recency, border=topology"),
            new VizCanvasColorModeOption(
                "Topology reference",
                VizActivityCanvasColorMode.Topology,
                "fill=topology slices, pulse=activity")
        };
}

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
