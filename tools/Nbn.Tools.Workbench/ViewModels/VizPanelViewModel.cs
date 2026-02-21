using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Platform.Storage;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class VizPanelViewModel : ViewModelBase
{
    private const int MaxEvents = 400;
    private const int DefaultTickWindow = 64;
    private const int MaxTickWindow = 4096;
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
    private bool _suspendSelection;
    private VizEventItem? _selectedEvent;
    private string _selectedPayload = string.Empty;
    private string _tickWindowText = DefaultTickWindow.ToString(CultureInfo.InvariantCulture);
    private bool _includeLowSignalEvents;
    private string _activitySummary = "Awaiting visualization events.";
    private string _activityCanvasLegend = "Canvas renderer awaiting activity.";
    private uint? _selectedCanvasRegionId;
    private string? _selectedCanvasRouteLabel;
    private uint? _hoverCanvasRegionId;
    private string? _hoverCanvasRouteLabel;
    private readonly HashSet<uint> _pinnedCanvasRegions = new();
    private readonly HashSet<string> _pinnedCanvasRoutes = new(StringComparer.OrdinalIgnoreCase);
    private string _activityInteractionSummary = "Select a node or route to inspect activity details.";
    private string _activityPinnedSummary = "Pinned: none.";

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
        ClearCommand = new RelayCommand(Clear);
        AddBrainCommand = new RelayCommand(AddBrainFromEntry);
        ZoomCommand = new RelayCommand(ZoomRegion);
        ShowFullBrainCommand = new RelayCommand(ShowFullBrain);
        ApplyActivityOptionsCommand = new RelayCommand(ApplyActivityOptions);
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

    public ObservableCollection<BrainListItem> KnownBrains { get; }

    public ObservableCollection<VizPanelTypeOption> VizPanelTypeOptions { get; }

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
                        _brain.SelectBrain(value.BrainId, preserveOutputs: true);
                    }
                    RefreshFilteredEvents();
                }
            }
        }
    }

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

    public string RegionFocusText
    {
        get => _regionFocusText;
        set => SetProperty(ref _regionFocusText, value);
    }

    public string TickWindowText
    {
        get => _tickWindowText;
        set => SetProperty(ref _tickWindowText, value);
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

    public RelayCommand ApplyActivityOptionsCommand { get; }

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
        }
        RefreshFilteredEvents();
    }

    public void AddVizEvent(VizEventItem item)
    {
        lock (_pendingEventsGate)
        {
            _pendingEvents.Enqueue(item);
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

        _selectedCanvasRegionId = node.RegionId;
        _selectedCanvasRouteLabel = null;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
        RefreshActivityProjection();
    }

    public void SelectCanvasEdge(VizActivityCanvasEdge? edge)
    {
        if (edge is null)
        {
            ClearCanvasSelection();
            return;
        }

        _selectedCanvasRouteLabel = edge.RouteLabel;
        _selectedCanvasRegionId = null;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
        RefreshActivityProjection();
    }

    public void SetCanvasNodeHover(VizActivityCanvasNode? node)
    {
        var nextRegion = node?.RegionId;
        if (_hoverCanvasRegionId == nextRegion && _hoverCanvasRouteLabel is null)
        {
            return;
        }

        _hoverCanvasRegionId = nextRegion;
        _hoverCanvasRouteLabel = null;
        RefreshActivityProjection();
    }

    public void SetCanvasEdgeHover(VizActivityCanvasEdge? edge)
    {
        var nextRoute = edge?.RouteLabel;
        if (string.Equals(_hoverCanvasRouteLabel, nextRoute, StringComparison.OrdinalIgnoreCase) && !_hoverCanvasRegionId.HasValue)
        {
            return;
        }

        _hoverCanvasRouteLabel = nextRoute;
        _hoverCanvasRegionId = null;
        RefreshActivityProjection();
    }

    public void TogglePinCanvasNode(VizActivityCanvasNode? node)
    {
        if (node is null)
        {
            return;
        }

        if (!_pinnedCanvasRegions.Add(node.RegionId))
        {
            _pinnedCanvasRegions.Remove(node.RegionId);
        }

        _selectedCanvasRegionId = node.RegionId;
        _selectedCanvasRouteLabel = null;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
        RefreshActivityProjection();
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
        _selectedCanvasRegionId = null;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
        RefreshActivityProjection();
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
        RegionFilterText = regionId.ToString(CultureInfo.InvariantCulture);
        Status = $"Zoom focus set to region {regionId}.";
    }

    private void ShowFullBrain()
    {
        RegionFocusText = string.Empty;
        RegionFilterText = string.Empty;
        RefreshFilteredEvents();
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
        Status = $"Applied activity options (tick window {tickWindow}).";
    }

    private void Clear()
    {
        lock (_pendingEventsGate)
        {
            _pendingEvents.Clear();
            _flushScheduled = false;
        }

        _allEvents.Clear();
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

    private void RefreshFilteredEvents()
    {
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
        ExportCommand.RaiseCanExecuteChanged();
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
        while (true)
        {
            List<VizEventItem> batch;
            lock (_pendingEventsGate)
            {
                if (_pendingEvents.Count == 0)
                {
                    _flushScheduled = false;
                    return;
                }

                batch = new List<VizEventItem>(_pendingEvents.Count);
                while (_pendingEvents.Count > 0)
                {
                    batch.Add(_pendingEvents.Dequeue());
                }
            }

            foreach (var item in batch)
            {
                if (!ShouldIncludeInVisualizer(item))
                {
                    continue;
                }

                _allEvents.Insert(0, item);
            }

            Trim(_allEvents);
            RefreshFilteredEvents();
        }
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

        var projection = VizActivityProjectionBuilder.Build(VizEvents, options);
        TrimCanvasInteractionToProjection(projection);

        ReplaceItems(ActivityStats, projection.Stats);
        ReplaceItems(RegionActivity, projection.Regions);
        ReplaceItems(EdgeActivity, projection.Edges);
        ReplaceItems(TickActivity, projection.Ticks);
        ActivitySummary = projection.Summary;

        var interaction = new VizActivityCanvasInteractionState(
            _selectedCanvasRegionId,
            _selectedCanvasRouteLabel,
            _hoverCanvasRegionId,
            _hoverCanvasRouteLabel,
            _pinnedCanvasRegions,
            _pinnedCanvasRoutes);
        var canvas = VizActivityCanvasLayoutBuilder.Build(projection, options, interaction);
        ReplaceItems(CanvasNodes, canvas.Nodes);
        ReplaceItems(CanvasEdges, canvas.Edges);
        ActivityCanvasLegend = canvas.Legend;
        UpdateCanvasInteractionSummaries(canvas.Nodes, canvas.Edges);
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
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
            .ToList();

        var currentIndex = _selectedCanvasRegionId.HasValue
            ? ordered.FindIndex(item => item.RegionId == _selectedCanvasRegionId.Value)
            : delta >= 0 ? -1 : 0;
        var nextIndex = ((currentIndex + delta) % ordered.Count + ordered.Count) % ordered.Count;
        var next = ordered[nextIndex];
        SelectCanvasNode(next);
        Status = $"Canvas selection: region {next.RegionId}.";
    }

    private void NavigateToCanvasSelection()
    {
        var regionId = GetCurrentSelectionRegionId(CanvasEdges);
        if (!regionId.HasValue)
        {
            Status = "Select a node or route before navigating focus.";
            return;
        }

        RegionFocusText = regionId.Value.ToString(CultureInfo.InvariantCulture);
        RegionFilterText = regionId.Value.ToString(CultureInfo.InvariantCulture);
        RefreshFilteredEvents();
        Status = $"Canvas navigation focused region {regionId.Value}.";
    }

    private void TogglePinForCurrentSelection()
    {
        if (_selectedCanvasRegionId.HasValue)
        {
            var regionId = _selectedCanvasRegionId.Value;
            if (!_pinnedCanvasRegions.Add(regionId))
            {
                _pinnedCanvasRegions.Remove(regionId);
            }

            RefreshActivityProjection();
            Status = _pinnedCanvasRegions.Contains(regionId)
                ? $"Pinned region {regionId}."
                : $"Unpinned region {regionId}.";
            return;
        }

        if (!string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel))
        {
            var routeLabel = _selectedCanvasRouteLabel!;
            if (!_pinnedCanvasRoutes.Add(routeLabel))
            {
                _pinnedCanvasRoutes.Remove(routeLabel);
            }

            RefreshActivityProjection();
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
        RefreshActivityProjection();
        Status = "Canvas interaction reset.";
    }

    private void ClearCanvasSelection()
    {
        if (!_selectedCanvasRegionId.HasValue && string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel))
        {
            return;
        }

        _selectedCanvasRegionId = null;
        _selectedCanvasRouteLabel = null;
        OnPropertyChanged(nameof(TogglePinSelectionLabel));
        RefreshActivityProjection();
    }

    private void ResetCanvasInteractionState(bool clearPins)
    {
        _selectedCanvasRegionId = null;
        _selectedCanvasRouteLabel = null;
        _hoverCanvasRegionId = null;
        _hoverCanvasRouteLabel = null;
        if (clearPins)
        {
            _pinnedCanvasRegions.Clear();
            _pinnedCanvasRoutes.Clear();
        }

        OnPropertyChanged(nameof(TogglePinSelectionLabel));
    }

    private void TrimCanvasInteractionToProjection(VizActivityProjection projection)
    {
        var validRegions = new HashSet<uint>(projection.Regions.Select(item => item.RegionId));
        foreach (var edge in projection.Edges)
        {
            if (edge.SourceRegionId.HasValue)
            {
                validRegions.Add(edge.SourceRegionId.Value);
            }

            if (edge.TargetRegionId.HasValue)
            {
                validRegions.Add(edge.TargetRegionId.Value);
            }
        }

        var validRoutes = new HashSet<string>(
            projection.Edges
                .Select(item => item.RouteLabel)
                .Where(label => !string.IsNullOrWhiteSpace(label)),
            StringComparer.OrdinalIgnoreCase);

        if (_selectedCanvasRegionId.HasValue && !validRegions.Contains(_selectedCanvasRegionId.Value))
        {
            _selectedCanvasRegionId = null;
        }

        if (_hoverCanvasRegionId.HasValue && !validRegions.Contains(_hoverCanvasRegionId.Value))
        {
            _hoverCanvasRegionId = null;
        }

        if (!string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel) && !validRoutes.Contains(_selectedCanvasRouteLabel!))
        {
            _selectedCanvasRouteLabel = null;
        }

        if (!string.IsNullOrWhiteSpace(_hoverCanvasRouteLabel) && !validRoutes.Contains(_hoverCanvasRouteLabel!))
        {
            _hoverCanvasRouteLabel = null;
        }

        _pinnedCanvasRegions.RemoveWhere(regionId => !validRegions.Contains(regionId));
        _pinnedCanvasRoutes.RemoveWhere(route => !validRoutes.Contains(route));
    }

    private void UpdateCanvasInteractionSummaries(
        IReadOnlyList<VizActivityCanvasNode> nodes,
        IReadOnlyList<VizActivityCanvasEdge> edges)
    {
        var selectedNode = _selectedCanvasRegionId.HasValue
            ? nodes.FirstOrDefault(item => item.RegionId == _selectedCanvasRegionId.Value)
            : null;
        var selectedEdge = !string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel)
            ? edges.FirstOrDefault(item => string.Equals(item.RouteLabel, _selectedCanvasRouteLabel, StringComparison.OrdinalIgnoreCase))
            : null;

        var hoverNode = _hoverCanvasRegionId.HasValue
            ? nodes.FirstOrDefault(item => item.RegionId == _hoverCanvasRegionId.Value)
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
            ? $"Hover node {hoverNode.Label}"
            : hoverEdge is not null
                ? $"Hover route {hoverEdge.RouteLabel}"
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
        return $"Pinned regions: {regionText} | routes: {routeText}";
    }

    private bool IsCurrentSelectionPinned()
    {
        if (_selectedCanvasRegionId.HasValue)
        {
            return _pinnedCanvasRegions.Contains(_selectedCanvasRegionId.Value);
        }

        return !string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel)
               && _pinnedCanvasRoutes.Contains(_selectedCanvasRouteLabel!);
    }

    private uint? GetCurrentSelectionRegionId(IReadOnlyList<VizActivityCanvasEdge> edges)
    {
        if (_selectedCanvasRegionId.HasValue)
        {
            return _selectedCanvasRegionId.Value;
        }

        if (string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel))
        {
            return null;
        }

        var selectedEdge = edges.FirstOrDefault(item => string.Equals(item.RouteLabel, _selectedCanvasRouteLabel, StringComparison.OrdinalIgnoreCase));
        return selectedEdge?.TargetRegionId ?? selectedEdge?.SourceRegionId;
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
            return false;
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

    private static string BuildPayload(VizEventItem? item)
    {
        if (item is null)
        {
            return string.Empty;
        }

        return $"[{item.Type}] brain={item.BrainId} tick={item.TickId}\nregion={item.Region} source={item.Source} target={item.Target}\nvalue={item.Value} strength={item.Strength}\nEventId={item.EventId}";
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
