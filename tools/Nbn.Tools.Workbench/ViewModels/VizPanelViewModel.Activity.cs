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

public sealed partial class VizPanelViewModel
{
    public void AddBrainId(Guid id)
    {
        AddBrainId(id.ToString("D"));
    }

    public void SetBrains(IReadOnlyList<BrainListItem> brains)
    {
        var previousSelection = SelectedBrain;
        var previousBrainId = previousSelection?.BrainId;
        var preferredBrainId = _preferredBrainId ?? previousBrainId;

        if (brains.Count == 0)
        {
            _consecutiveEmptyBrainRefreshes++;
            if (_consecutiveEmptyBrainRefreshes < EmptyBrainRefreshClearThreshold
                && (previousSelection is not null || KnownBrains.Count > 0))
            {
                return;
            }

            _suspendSelection = true;
            KnownBrains.Clear();
            SelectedBrain = null;
            _suspendSelection = false;
            _preferredBrainId = null;
            _consecutiveEmptyBrainRefreshes = 0;
            _consecutiveSelectionMissRefreshes = 0;
            Status = "No brains reported.";
            RefreshFilteredEvents();
            return;
        }

        _consecutiveEmptyBrainRefreshes = 0;
        var selectedMissingFromSnapshot = preferredBrainId.HasValue
            && !brains.Any(entry => entry.BrainId == preferredBrainId.Value);
        if (selectedMissingFromSnapshot && previousSelection is not null)
        {
            _consecutiveSelectionMissRefreshes++;
        }
        else
        {
            _consecutiveSelectionMissRefreshes = 0;
        }

        var preserveMissingSelection = selectedMissingFromSnapshot
            && previousSelection is not null
            && _consecutiveSelectionMissRefreshes < SelectionMissRefreshClearThreshold;

        var effectiveBrains = brains.ToList();
        if (preserveMissingSelection && previousSelection is not null)
        {
            effectiveBrains.Add(previousSelection);
        }

        _suspendSelection = true;
        _ = ApplyKeyedDiff(KnownBrains, effectiveBrains, static entry => entry.BrainId.ToString("D"));

        Status = "Streaming";

        BrainListItem? match = null;
        if (preferredBrainId.HasValue)
        {
            match = KnownBrains.FirstOrDefault(entry => entry.BrainId == preferredBrainId.Value);
        }

        if (previousSelection is null && KnownBrains.Count > 0 && match is null)
        {
            match = KnownBrains[0];
        }

        var resolvedSelection = match;
        if (resolvedSelection is null && previousSelection is not null)
        {
            resolvedSelection = KnownBrains.FirstOrDefault(entry => entry.BrainId == previousSelection.BrainId);
        }

        if (resolvedSelection is null && KnownBrains.Count > 0 && !preserveMissingSelection)
        {
            resolvedSelection = KnownBrains[0];
        }

        SelectedBrain = resolvedSelection;
        _suspendSelection = false;

        var resolvedBrainId = resolvedSelection?.BrainId;
        if (resolvedBrainId.HasValue)
        {
            _preferredBrainId = resolvedBrainId.Value;
        }

        if (resolvedBrainId.HasValue && resolvedBrainId != previousBrainId)
        {
            _brain.EnsureSelectedBrain(resolvedBrainId.Value);
            QueueDefinitionTopologyHydration(resolvedBrainId.Value, TryParseRegionId(RegionFocusText, out var focusRegionId) ? focusRegionId : null);
        }

        if (resolvedBrainId.HasValue)
        {
            QueueSelectedBrainEnergyRefresh(force: resolvedBrainId != previousBrainId);
        }

        RefreshFilteredEvents();
    }

    public void ApplyHiveMindTickStatus(float targetTickHz, bool hasOverride, float overrideTickHz)
    {
        if (targetTickHz > 0f && float.IsFinite(targetTickHz))
        {
            RebaseMiniChartWindowOnCadenceChange(_currentTargetTickHz, targetTickHz);
            _currentTargetTickHz = targetTickHz;
            TickCadenceSummary = $"Current cadence: {FormatTickCadence(targetTickHz)}.";
            TickRateOverrideSummary = hasOverride
                ? $"Tick cadence control target: {FormatTickCadence(overrideTickHz)}. Current runtime target {FormatTickCadence(targetTickHz)}."
                : $"Tick cadence control is not set. Current runtime target {FormatTickCadence(targetTickHz)}.";
            OnPropertyChanged(nameof(VizCadenceSliderEffectiveMin));
            EnsureVizCadenceSliderWithinEffectiveMinimum();
            RefreshActivityProjection();
            return;
        }

        TickRateOverrideSummary = hasOverride
            ? $"Tick cadence control target: {FormatTickCadence(overrideTickHz)}. Current runtime target n/a."
            : "Tick cadence control is not set.";
        OnPropertyChanged(nameof(VizCadenceSliderEffectiveMin));
        EnsureVizCadenceSliderWithinEffectiveMinimum();
        RefreshActivityProjection();
    }

    public bool ApplySetting(SettingItem item)
    {
        if (string.Equals(item.Key, TickSettingsKeys.CadenceHzKey, StringComparison.OrdinalIgnoreCase))
        {
            if (TryParseTickRateOverrideSettingValue(item.Value, out var overrideHz))
            {
                TickRateOverrideText = overrideHz.HasValue
                    ? (1000d / overrideHz.Value).ToString("0.###", CultureInfo.InvariantCulture) + "ms"
                    : string.Empty;
            }

            return true;
        }

        if (string.Equals(item.Key, VisualizationSettingsKeys.TickMinIntervalMsKey, StringComparison.OrdinalIgnoreCase))
        {
            _vizTickMinIntervalMs = ParseVisualizationIntervalSetting(item.Value, DefaultVizTickMinIntervalMs);
            UpdateVisualizationCadenceFromSettings();
            return true;
        }

        if (string.Equals(item.Key, VisualizationSettingsKeys.StreamMinIntervalMsKey, StringComparison.OrdinalIgnoreCase))
        {
            _vizStreamMinIntervalMs = ParseVisualizationIntervalSetting(item.Value, DefaultVizStreamMinIntervalMs);
            UpdateVisualizationCadenceFromSettings();
            return true;
        }

        return false;
    }

    public void AddVizEvent(VizEventItem item)
    {
        var droppedThisCall = 0;
        lock (_pendingEventsGate)
        {
            if (IsGlobalVisualizerEvent(item.Type) && item.TickId > _latestObservedGlobalTickId)
            {
                _latestObservedGlobalTickId = item.TickId;
            }

            while (_pendingEvents.Count >= MaxPendingEvents)
            {
                _pendingEvents.Dequeue();
                _droppedPendingEvents++;
                droppedThisCall++;
            }

            _pendingEvents.Enqueue(item);
            _maxObservedPendingEvents = Math.Max(_maxObservedPendingEvents, _pendingEvents.Count);
            if (_flushScheduled)
            {
                return;
            }

            _flushScheduled = true;
        }

        if (LogVizDiagnostics && droppedThisCall > 0 && WorkbenchLog.Enabled)
        {
            WorkbenchLog.Warn(
                $"VizQueue drop_count={droppedThisCall} pending={_pendingEvents.Count} total_dropped={_droppedPendingEvents} incoming_type={item.Type} incoming_brain={item.BrainId} incoming_tick={item.TickId}");
        }

        _dispatcher.Post(FlushPendingEvents);
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

        ZoomToRegion(regionId);
    }

    public bool ZoomToRegion(uint regionId)
    {
        if (regionId < NbnConstants.RegionMinId || regionId > NbnConstants.RegionMaxId)
        {
            Status = $"Region ID must be {NbnConstants.RegionMinId}-{NbnConstants.RegionMaxId}.";
            return false;
        }

        RegionFocusText = regionId.ToString(CultureInfo.InvariantCulture);
        RefreshFilteredEvents();
        if (SelectedBrain is not null)
        {
            QueueDefinitionTopologyHydration(SelectedBrain.BrainId, regionId);
        }
        VisualizationSelectionChanged?.Invoke();
        Status = $"Zoom focus set to region {regionId}.";
        return true;
    }

    public EmptyCanvasDoubleClickAction ResolveEmptyCanvasDoubleClickAction(bool isDefaultCanvasView)
    {
        if (ActiveFocusRegionId.HasValue && isDefaultCanvasView)
        {
            return EmptyCanvasDoubleClickAction.ShowFullBrain;
        }

        return EmptyCanvasDoubleClickAction.ResetView;
    }

    public EmptyCanvasDoubleClickAction HandleEmptyCanvasDoubleClick(bool isDefaultCanvasView)
    {
        var action = ResolveEmptyCanvasDoubleClickAction(isDefaultCanvasView);
        if (action == EmptyCanvasDoubleClickAction.ShowFullBrain)
        {
            ShowFullBrain();
        }

        return action;
    }

    public bool TryClearCanvasSelectionFromEmptyClick()
    {
        if (string.IsNullOrWhiteSpace(_selectedCanvasNodeKey) && string.IsNullOrWhiteSpace(_selectedCanvasRouteLabel))
        {
            return false;
        }

        ClearCanvasSelection();
        Status = "Selection cleared.";
        return true;
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

        if (!TryParseMiniActivityTopN(MiniActivityTopNText, out var topN))
        {
            Status = $"Mini chart Top N must be an integer in {MinMiniActivityTopN}-{MaxMiniActivityTopN}.";
            return;
        }

        if (!TryParseMiniActivityRangeSeconds(MiniActivityRangeSecondsText, out var rangeSeconds))
        {
            Status = $"Mini chart range must be a number in {MinMiniActivityRangeSeconds:0.##}-{MaxMiniActivityRangeSeconds:0.##} seconds.";
            return;
        }

        if (!TryParseLodRouteBudget(LodLowZoomBudgetText, out var lowBudget))
        {
            Status = $"LOD low-zoom budget must be an integer in {MinLodRouteBudget}-{MaxLodRouteBudget}.";
            return;
        }

        if (!TryParseLodRouteBudget(LodMediumZoomBudgetText, out var mediumBudget))
        {
            Status = $"LOD medium-zoom budget must be an integer in {MinLodRouteBudget}-{MaxLodRouteBudget}.";
            return;
        }

        if (!TryParseLodRouteBudget(LodHighZoomBudgetText, out var highBudget))
        {
            Status = $"LOD high-zoom budget must be an integer in {MinLodRouteBudget}-{MaxLodRouteBudget}.";
            return;
        }

        TickWindowText = tickWindow.ToString(CultureInfo.InvariantCulture);
        MiniActivityTopNText = topN.ToString(CultureInfo.InvariantCulture);
        MiniActivityRangeSecondsText = rangeSeconds.ToString("0.###", CultureInfo.InvariantCulture);
        LodLowZoomBudgetText = lowBudget.ToString(CultureInfo.InvariantCulture);
        LodMediumZoomBudgetText = mediumBudget.ToString(CultureInfo.InvariantCulture);
        LodHighZoomBudgetText = highBudget.ToString(CultureInfo.InvariantCulture);
        UpdateLodSummary();
        RefreshFilteredEvents();
        if (SelectedBrain is not null)
        {
            QueueDefinitionTopologyHydration(SelectedBrain.BrainId, TryParseRegionId(RegionFocusText, out var focusRegionId) ? focusRegionId : null);
        }
        Status = $"Applied activity options (tick window {tickWindow}, mini chart top N {topN}, mini range {rangeSeconds:0.###}s, adaptive LOD {(EnableAdaptiveLod ? "on" : "off")}).";
    }

    private async Task ApplyTickCadenceAsync()
    {
        if (!TryParseTickRateOverrideInput(TickRateOverrideText, out var targetTickHz))
        {
            Status = "Tick cadence must be a positive value (e.g. 12.5Hz or 80ms).";
            return;
        }

        var tickCadenceMs = 1000d / targetTickHz;
        if (!double.IsFinite(tickCadenceMs)
            || tickCadenceMs < TickSliderMinMs
            || tickCadenceMs > TickSliderMaxMs)
        {
            Status = $"Tick cadence must be between {TickSliderMinMs:0} ms and {TickSliderMaxMs:0} ms (or equivalent Hz).";
            return;
        }

        var setting = await _brain
            .SetSettingAsync(TickSettingsKeys.CadenceHzKey, targetTickHz.ToString("0.###", CultureInfo.InvariantCulture))
            .ConfigureAwait(false);
        if (setting is null)
        {
            Status = "Tick cadence update failed: SettingsMonitor unavailable.";
            return;
        }

        var hiveMindStatus = await _brain.GetHiveMindStatusAsync().ConfigureAwait(false);
        _dispatcher.Post(() =>
        {
            ApplySetting(new SettingItem(setting.Key, setting.Value, setting.UpdatedMs.ToString(CultureInfo.InvariantCulture)));
            if (hiveMindStatus is not null)
            {
                ApplyHiveMindTickStatus(
                    hiveMindStatus.TargetTickHz,
                    hiveMindStatus.HasTickRateOverride,
                    hiveMindStatus.TickRateOverrideHz);
            }
            else
            {
                ApplyHiveMindTickStatus(targetTickHz, hasOverride: true, overrideTickHz: targetTickHz);
            }

            Status = $"Tick cadence target set to {FormatTickCadence(targetTickHz)}.";
        });
    }

    private async Task ApplyVisualizationCadenceAsync()
    {
        if (!TryParseVisualizationIntervalInput(VizCadenceText, out var cadenceMs))
        {
            Status = $"Viz cadence must be {MinVisualizationIntervalMs}-{MaxVisualizationIntervalMs} ms (or equivalent Hz).";
            return;
        }

        var adjustedCadenceMs = NormalizeVizCadenceForTickFloor(cadenceMs, out var enforcedFloorMs);
        var tickResult = await _brain
            .SetSettingAsync(
                VisualizationSettingsKeys.TickMinIntervalMsKey,
                adjustedCadenceMs.ToString(CultureInfo.InvariantCulture))
            .ConfigureAwait(false);
        var streamResult = await _brain
            .SetSettingAsync(
                VisualizationSettingsKeys.StreamMinIntervalMsKey,
                adjustedCadenceMs.ToString(CultureInfo.InvariantCulture))
            .ConfigureAwait(false);

        _dispatcher.Post(() =>
        {
            if (tickResult is not null)
            {
                ApplySetting(new SettingItem(
                    tickResult.Key,
                    tickResult.Value,
                    tickResult.UpdatedMs.ToString(CultureInfo.InvariantCulture)));
            }

            if (streamResult is not null)
            {
                ApplySetting(new SettingItem(
                    streamResult.Key,
                    streamResult.Value,
                    streamResult.UpdatedMs.ToString(CultureInfo.InvariantCulture)));
            }

            if (tickResult is null || streamResult is null)
            {
                Status = "Viz cadence update failed: SettingsMonitor unavailable.";
                return;
            }

            if (enforcedFloorMs.HasValue)
            {
                Status = $"Viz cadence clamped to {enforcedFloorMs.Value} ms to stay at or slower than tick cadence.";
            }
            else
            {
                Status = BuildVisualizationCadenceSummary(
                    ParseVisualizationIntervalSetting(tickResult.Value, adjustedCadenceMs),
                    ParseVisualizationIntervalSetting(streamResult.Value, adjustedCadenceMs));
            }
        });
    }

    private Task ResetVisualizationCadenceAsync()
    {
        VizCadenceText = FormattableString.Invariant($"{DefaultVizStreamMinIntervalMs}ms");
        return ApplyVisualizationCadenceAsync();
    }

    private void Clear()
    {
        lock (_pendingEventsGate)
        {
            _pendingEvents.Clear();
            _latestObservedGlobalTickId = 0;
            _flushScheduled = false;
        }

        _allEvents.Clear();
        _projectionEvents.Clear();
        _filteredProjectionEvents = Array.Empty<VizEventItem>();
        _topologyByBrainId.Clear();
        lock (_pendingDefinitionHydrationGate)
        {
            _pendingDefinitionHydrationKeys.Clear();
        }
        _currentProjection = null;
        _currentProjectionOptions = new VizActivityProjectionOptions(
            DefaultTickWindow,
            IncludeLowSignalEvents,
            null,
            ParseMiniActivityTopNOrDefault(),
            ShowMiniActivityChart,
            ParseMiniActivityTickWindowOrDefault());
        _lastRenderedTickId = 0;
        _nextStreamingRefreshUtc = DateTime.MinValue;
        _nextDefinitionHydrationRetryUtc = DateTime.MinValue;
        _consecutiveEmptyBrainRefreshes = 0;
        _consecutiveSelectionMissRefreshes = 0;
        _miniChartMinTickFloor = null;
        VizEvents.Clear();
        SelectedEvent = null;
        SelectedPayload = string.Empty;
        TickRateOverrideSummary = DefaultTickOverrideSummary;
        TickCadenceSummary = DefaultTickCadenceSummary;
        UpdateVisualizationCadenceSummary();
        _currentTargetTickHz = null;
        OnPropertyChanged(nameof(VizCadenceSliderEffectiveMin));
        EnsureVizCadenceSliderWithinEffectiveMinimum();
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

        var projectionMatched = new List<VizEventItem>();
        foreach (var item in _projectionEvents)
        {
            if (MatchesFilter(item))
            {
                projectionMatched.Add(item);
            }
        }

        _filteredProjectionEvents = projectionMatched;

        if (matched == 0 && _projectionEvents.Count > 0 && SelectedBrain is not null)
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
        var latestTick = ReadLatestGlobalTickId();
        if (_projectionEvents.Count == 0)
        {
            return latestTick;
        }

        if (SelectedBrain is null)
        {
            return Math.Max(latestTick, _projectionEvents[0].TickId);
        }

        var selectedBrainId = SelectedBrain.BrainId;
        var hasFocusFilter = TryParseRegionId(RegionFocusText, out var focusRegionId);
        foreach (var item in _projectionEvents)
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

                return Math.Max(latestTick, item.TickId);
            }
        }

        return latestTick;
    }

    private ulong ReadLatestGlobalTickId()
    {
        lock (_pendingEventsGate)
        {
            return _latestObservedGlobalTickId;
        }
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
        var pendingAfter = 0;
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
            pendingAfter = _pendingEvents.Count;
            if (!hasMore)
            {
                _flushScheduled = false;
            }
        }

        var accepted = 0;
        var skippedByGlobalFilter = 0;
        foreach (var item in batch)
        {
            if (!ShouldIncludeInVisualizer(item))
            {
                skippedByGlobalFilter++;
                continue;
            }

            AccumulateTopology(item);
            _allEvents.Insert(0, item);
            _projectionEvents.Insert(0, item);
            accepted++;
        }

        Trim(_allEvents, MaxEvents);
        Trim(_projectionEvents, MaxProjectionEvents);
        RefreshFilteredEvents(fromStreaming: true, force: !hasMore);
        if (SelectedBrain is not null
            && (accepted > 0 || batch.Any(entry => IsGlobalVisualizerEvent(entry.Type))))
        {
            QueueSelectedBrainEnergyRefresh();
        }
        _lastFlushBatchCount = batch.Count;
        _lastFlushBatchMs = StopwatchElapsedMs(flushStart);

        if (LogVizDiagnostics && WorkbenchLog.Enabled)
        {
            var selectedBrain = SelectedBrain?.BrainId.ToString("D") ?? "none";
            var typeFilter = SelectedVizType.TypeFilter ?? "all";
            WorkbenchLog.Info(
                $"VizFlush batch={batch.Count} accepted={accepted} skipped_global={skippedByGlobalFilter} pending_after={pendingAfter} table_events={_allEvents.Count} projection_events={_projectionEvents.Count} visible_events={VizEvents.Count} selected_brain={selectedBrain} type_filter={typeFilter} focus={NormalizeDiagnosticText(RegionFocusText)}");
        }

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
            $"focus={NormalizeDiagnosticText(RegionFocusText)} region_filter={NormalizeDiagnosticText(RegionFilterText)} search={NormalizeDiagnosticText(SearchFilterText)} tick_window={ParseTickWindowOrDefault()} mini_top_n={ParseMiniActivityTopNOrDefault()} mini_range_s={ParseMiniActivityRangeSecondsOrDefault():0.###} mini_tick_window={ParseMiniActivityTickWindowOrDefault()} tick_hz={(_currentTargetTickHz ?? 0f):0.###} include_low_signal={IncludeLowSignalEvents} color_mode={SelectedCanvasColorMode.Mode} color_curve={SelectedCanvasTransferCurve.Curve} layout_mode={SelectedLayoutMode.Mode} viewport_scale={_canvasViewportScale:0.###} adaptive_lod={EnableAdaptiveLod}");
        sb.AppendLine(
            $"events table={_allEvents.Count} projection={_projectionEvents.Count} filtered_table={VizEvents.Count} filtered_projection={_filteredProjectionEvents.Count} canvas_nodes={CanvasNodes.Count} canvas_edges={CanvasEdges.Count} stats={ActivityStats.Count} region_rows={RegionActivity.Count} edge_rows={EdgeActivity.Count} tick_rows={TickActivity.Count} mini_series={MiniActivityChartSeries.Count} mini_enabled={ShowMiniActivityChart}");
        sb.AppendLine(
            $"perf projection_ms={_lastProjectionBuildMs:0.###} layout_ms={_lastCanvasLayoutBuildMs:0.###} apply_ms={_lastCanvasApplyMs:0.###} frame_ms={_lastCanvasFrameMs:0.###} flush_ms={_lastFlushBatchMs:0.###} flush_batch={_lastFlushBatchCount}");
        sb.AppendLine(
            $"hit_test last_ms={_lastHitTestMs:0.###} avg_ms={_avgHitTestMs:0.###} max_ms={_maxHitTestMs:0.###} samples={_hitTestSamples}");
        sb.AppendLine(
            $"queue pending={pendingQueueDepth} pending_peak={_maxObservedPendingEvents} dropped={_droppedPendingEvents}");
        sb.AppendLine(
            $"canvas_diff nodes({_lastCanvasNodeDiffStats}) edges({_lastCanvasEdgeDiffStats})");
        sb.AppendLine($"summary={ActivitySummary}");
        sb.AppendLine($"mini_series_label={MiniActivityChartSeriesLabel}");
        sb.AppendLine($"mini_range={MiniActivityChartRangeLabel}");
        sb.AppendLine($"mini_metric={MiniActivityChartMetricLabel}");
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

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Trim().ToLowerInvariant() is "1" or "true" or "yes" or "on";
    }

    private int ParseTickWindowOrDefault()
    {
        return TryParseTickWindow(TickWindowText, out var tickWindow)
            ? tickWindow
            : DefaultTickWindow;
    }

    private int ParseMiniActivityTopNOrDefault()
    {
        return TryParseMiniActivityTopN(MiniActivityTopNText, out var topN)
            ? topN
            : DefaultMiniActivityTopN;
    }

    private double ParseMiniActivityRangeSecondsOrDefault()
    {
        return TryParseMiniActivityRangeSeconds(MiniActivityRangeSecondsText, out var seconds)
            ? seconds
            : DefaultMiniActivityRangeSeconds;
    }

    private int ParseMiniActivityTickWindowOrDefault()
    {
        var seconds = ParseMiniActivityRangeSecondsOrDefault();
        var tickRateHz = _currentTargetTickHz.HasValue && _currentTargetTickHz.Value > 0f && float.IsFinite(_currentTargetTickHz.Value)
            ? _currentTargetTickHz.Value
            : DefaultMiniActivityTickRateHz;
        var estimatedTicks = (int)Math.Round(seconds * tickRateHz, MidpointRounding.AwayFromZero);
        return Math.Clamp(estimatedTicks, 1, MaxTickWindow);
    }

    private void QueueSelectedBrainEnergyRefresh(bool force = false)
    {
        var selectedBrain = SelectedBrain;
        if (selectedBrain is null)
        {
            _nextSelectedBrainEnergyRefreshUtc = DateTime.MinValue;
            SelectedBrainEnergySummary = "Selected brain energy: n/a (no brain selected).";
            return;
        }

        var now = DateTime.UtcNow;
        if (!force && now < _nextSelectedBrainEnergyRefreshUtc)
        {
            return;
        }

        if (Interlocked.CompareExchange(ref _selectedBrainEnergyRefreshInFlight, 1, 0) != 0)
        {
            return;
        }

        _nextSelectedBrainEnergyRefreshUtc = now + SelectedBrainEnergyRefreshInterval;
        _ = RefreshSelectedBrainEnergyAsync(selectedBrain.BrainId);
    }

    private async Task RefreshSelectedBrainEnergyAsync(Guid brainId)
    {
        try
        {
            var info = await _brain.RequestBrainInfoAsync(brainId).ConfigureAwait(false);
            _dispatcher.Post(() =>
            {
                if (SelectedBrain?.BrainId != brainId)
                {
                    return;
                }

                SelectedBrainEnergySummary = BuildSelectedBrainEnergySummary(info);
            });
        }
        finally
        {
            Interlocked.Exchange(ref _selectedBrainEnergyRefreshInFlight, 0);
        }
    }

    private static string BuildSelectedBrainEnergySummary(BrainInfo? info)
    {
        if (info is null)
        {
            return "Selected brain energy: unavailable.";
        }

        var enabled = info.CostEnabled && info.EnergyEnabled ? "on" : "off";
        return FormattableString.Invariant(
            $"Selected brain energy: {info.EnergyRemaining:N0} units | rate {info.EnergyRateUnitsPerSecond:N0}/s | last tick cost {info.LastTickCost:N0} | cost+energy {enabled}.");
    }

    private static void ReplaceItems<T>(ObservableCollection<T> target, IReadOnlyList<T> source)
    {
        target.Clear();
        foreach (var item in source)
        {
            target.Add(item);
        }
    }
    internal static bool TryParseTickRateOverrideInput(string? value, out float targetTickHz)
    {
        targetTickHz = 0f;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var normalized = value.Trim();
        var parseAsMilliseconds = false;

        if (normalized.EndsWith("ms", StringComparison.OrdinalIgnoreCase))
        {
            parseAsMilliseconds = true;
            normalized = normalized[..^2].Trim();
        }
        else if (normalized.EndsWith("hz", StringComparison.OrdinalIgnoreCase))
        {
            normalized = normalized[..^2].Trim();
        }

        if (!float.TryParse(normalized, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            || !float.IsFinite(parsed)
            || parsed <= 0f)
        {
            return false;
        }

        if (parseAsMilliseconds)
        {
            targetTickHz = 1000f / parsed;
            return float.IsFinite(targetTickHz) && targetTickHz > 0f;
        }

        targetTickHz = parsed;
        return true;
    }

    private static bool TryParseTickRateOverrideSettingValue(string? value, out float? overrideHz)
    {
        overrideHz = null;
        if (string.IsNullOrWhiteSpace(value))
        {
            return true;
        }

        var normalized = value.Trim().ToLowerInvariant();
        if (normalized is "0" or "off" or "none" or "clear" or "default")
        {
            return true;
        }

        if (!TryParseTickRateOverrideInput(value, out var parsed))
        {
            return false;
        }

        overrideHz = parsed;
        return true;
    }

    private static bool TryParseVisualizationIntervalInput(string? value, out uint intervalMs)
    {
        intervalMs = 0u;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var normalized = value.Trim();
        var parseAsHz = false;
        if (normalized.EndsWith("hz", StringComparison.OrdinalIgnoreCase))
        {
            normalized = normalized[..^2].Trim();
            parseAsHz = true;
        }
        else if (normalized.EndsWith("ms", StringComparison.OrdinalIgnoreCase))
        {
            normalized = normalized[..^2].Trim();
        }

        if (!float.TryParse(normalized, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            || !float.IsFinite(parsed)
            || parsed <= 0f)
        {
            return false;
        }

        if (parseAsHz)
        {
            if (parsed <= 0f)
            {
                return false;
            }

            var computedMs = 1000d / parsed;
            if (!double.IsFinite(computedMs) || computedMs <= 0d)
            {
                return false;
            }

            parsed = (float)computedMs;
        }

        intervalMs = (uint)Math.Round(parsed, MidpointRounding.AwayFromZero);
        return intervalMs >= MinVisualizationIntervalMs && intervalMs <= MaxVisualizationIntervalMs;
    }

    private static uint ParseVisualizationIntervalSetting(string? value, uint fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !uint.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return fallback;
        }

        return Math.Clamp(parsed, MinVisualizationIntervalMs, MaxVisualizationIntervalMs);
    }

    private void UpdateVisualizationCadenceSummary()
    {
        VizCadenceSummary = BuildVisualizationCadenceSummary(_vizTickMinIntervalMs, _vizStreamMinIntervalMs);
    }

    private void UpdateVisualizationCadenceFromSettings()
    {
        UpdateVisualizationCadenceSummary();
        if (_vizTickMinIntervalMs == _vizStreamMinIntervalMs)
        {
            var effectiveMin = VizCadenceSliderEffectiveMin;
            var sliderValue = Math.Clamp((double)_vizTickMinIntervalMs, effectiveMin, VizSliderMaxMs);
            _vizCadenceSliderSyncInProgress = true;
            try
            {
                VizCadenceSliderMs = sliderValue;
            }
            finally
            {
                _vizCadenceSliderSyncInProgress = false;
            }

            VizCadenceText = FormattableString.Invariant($"{_vizTickMinIntervalMs}ms");
        }
        else
        {
            var effectiveMin = VizCadenceSliderEffectiveMin;
            var sliderValue = Math.Clamp((double)_vizStreamMinIntervalMs, effectiveMin, VizSliderMaxMs);
            _vizCadenceSliderSyncInProgress = true;
            try
            {
                VizCadenceSliderMs = sliderValue;
            }
            finally
            {
                _vizCadenceSliderSyncInProgress = false;
            }

            VizCadenceText = FormattableString.Invariant($"{_vizStreamMinIntervalMs}ms");
        }
    }

    private double? ResolveEffectiveTickCadenceMs()
    {
        if (TryParseTickRateOverrideInput(TickRateOverrideText, out var overrideHz))
        {
            var overrideMs = 1000d / overrideHz;
            if (double.IsFinite(overrideMs) && overrideMs > 0d)
            {
                return overrideMs;
            }
        }

        if (_currentTargetTickHz.HasValue && float.IsFinite(_currentTargetTickHz.Value) && _currentTargetTickHz.Value > 0f)
        {
            var targetMs = 1000d / _currentTargetTickHz.Value;
            if (double.IsFinite(targetMs) && targetMs > 0d)
            {
                return targetMs;
            }
        }

        return null;
    }

    private uint NormalizeVizCadenceForTickFloor(uint requestedCadenceMs, out uint? enforcedFloorMs)
    {
        enforcedFloorMs = null;
        var tickCadenceMs = ResolveEffectiveTickCadenceMs();
        if (!tickCadenceMs.HasValue)
        {
            return requestedCadenceMs;
        }

        var floorMs = (uint)Math.Clamp(
            Math.Ceiling(tickCadenceMs.Value),
            (double)MinVisualizationIntervalMs,
            MaxVisualizationIntervalMs);
        if (requestedCadenceMs >= floorMs)
        {
            return requestedCadenceMs;
        }

        enforcedFloorMs = floorMs;
        return floorMs;
    }

    private void SyncTickCadenceTextFromSlider(double sliderMs)
    {
        if (_tickCadenceSliderSyncInProgress)
        {
            return;
        }

        _tickCadenceTextSyncInProgress = true;
        try
        {
            TickRateOverrideText = FormattableString.Invariant($"{sliderMs:0.###}ms");
        }
        finally
        {
            _tickCadenceTextSyncInProgress = false;
        }
    }

    private void SyncTickCadenceSliderFromText(string? text)
    {
        if (_tickCadenceTextSyncInProgress || !TryParseTickRateOverrideInput(text, out var targetHz))
        {
            return;
        }

        var targetMs = 1000d / targetHz;
        if (!double.IsFinite(targetMs) || targetMs <= 0d)
        {
            return;
        }

        _tickCadenceSliderSyncInProgress = true;
        try
        {
            TickCadenceSliderMs = Math.Clamp(targetMs, TickSliderMinMs, TickSliderMaxMs);
        }
        finally
        {
            _tickCadenceSliderSyncInProgress = false;
        }

        OnPropertyChanged(nameof(VizCadenceSliderEffectiveMin));
        EnsureVizCadenceSliderWithinEffectiveMinimum();
    }

    private void SyncVizCadenceTextFromSlider(double sliderMs)
    {
        if (_vizCadenceSliderSyncInProgress)
        {
            return;
        }

        var effectiveMin = VizCadenceSliderEffectiveMin;
        var normalizedMs = Math.Clamp(sliderMs, effectiveMin, VizSliderMaxMs);
        _vizCadenceTextSyncInProgress = true;
        try
        {
            VizCadenceText = FormattableString.Invariant($"{normalizedMs:0.###}ms");
        }
        finally
        {
            _vizCadenceTextSyncInProgress = false;
        }
    }

    private void SyncVizCadenceSliderFromText(string? text)
    {
        if (_vizCadenceTextSyncInProgress || !TryParseVisualizationIntervalInput(text, out var parsedIntervalMs))
        {
            return;
        }

        var effectiveMin = VizCadenceSliderEffectiveMin;
        var sliderTarget = Math.Clamp((double)parsedIntervalMs, effectiveMin, VizSliderMaxMs);

        _vizCadenceSliderSyncInProgress = true;
        try
        {
            VizCadenceSliderMs = sliderTarget;
        }
        finally
        {
            _vizCadenceSliderSyncInProgress = false;
        }
    }

    private void EnsureVizCadenceSliderWithinEffectiveMinimum()
    {
        var effectiveMin = VizCadenceSliderEffectiveMin;
        if (_vizCadenceSliderMs >= effectiveMin)
        {
            return;
        }

        _vizCadenceSliderSyncInProgress = true;
        try
        {
            VizCadenceSliderMs = effectiveMin;
        }
        finally
        {
            _vizCadenceSliderSyncInProgress = false;
        }
    }

    private static string BuildVisualizationCadenceSummary(uint tickMinIntervalMs, uint streamMinIntervalMs)
    {
        var tickSummary = tickMinIntervalMs == 0u ? "every tick" : $">= {tickMinIntervalMs} ms";
        var streamSummary = streamMinIntervalMs == 0u ? "every tick" : $">= {streamMinIntervalMs} ms";
        return $"Viz cadence settings: tick events {tickSummary}, stream events {streamSummary}.";
    }

    private static string FormatTickCadence(float tickHz)
    {
        if (!float.IsFinite(tickHz) || tickHz <= 0f)
        {
            return "n/a";
        }

        var cadenceMs = 1000d / tickHz;
        return FormattableString.Invariant($"{tickHz:0.###} Hz ({cadenceMs:0.###} ms/tick)");
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

    private static bool TryParseMiniActivityTopN(string? value, out int topN)
    {
        topN = DefaultMiniActivityTopN;
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return false;
        }

        if (parsed < MinMiniActivityTopN || parsed > MaxMiniActivityTopN)
        {
            return false;
        }

        topN = parsed;
        return true;
    }

    private static bool TryParseMiniActivityRangeSeconds(string? value, out double seconds)
    {
        seconds = DefaultMiniActivityRangeSeconds;
        if (!double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
        {
            return false;
        }

        if (!double.IsFinite(parsed) || parsed < MinMiniActivityRangeSeconds || parsed > MaxMiniActivityRangeSeconds)
        {
            return false;
        }

        seconds = parsed;
        return true;
    }

    private static bool TryParseLodRouteBudget(string? value, out int routeBudget)
    {
        routeBudget = 0;
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return false;
        }

        if (parsed < MinLodRouteBudget || parsed > MaxLodRouteBudget)
        {
            return false;
        }

        routeBudget = parsed;
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

    private static void Trim<T>(ICollection<T> collection, int maxCount)
    {
        var boundedMax = Math.Max(1, maxCount);
        while (collection.Count > boundedMax && collection is IList<T> list)
        {
            list.RemoveAt(list.Count - 1);
        }
    }
}
