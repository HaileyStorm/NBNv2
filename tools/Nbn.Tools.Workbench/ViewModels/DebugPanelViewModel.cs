using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Nbn.Proto.Viz;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class DebugPanelViewModel : ViewModelBase, IAsyncDisposable
{
    private const int MaxEvents = 400;
    private static readonly TimeSpan SystemLoadRefreshInterval = TimeSpan.FromSeconds(3);
    private static readonly HashSet<string> SystemLoadRefreshTriggerProperties =
    [
        nameof(ConnectionViewModel.SettingsConnected),
        nameof(ConnectionViewModel.SettingsStatus),
        nameof(ConnectionViewModel.HiveMindConnected),
        nameof(ConnectionViewModel.HiveMindDiscoverable),
        nameof(ConnectionViewModel.HiveMindStatus)
    ];
    private readonly UiDispatcher _dispatcher;
    private readonly WorkbenchClient _client;
    private readonly ConnectionViewModel? _connections;
    private readonly WorkbenchSystemLoadHistoryTracker _systemLoadHistory = new();
    private readonly List<DebugEventItem> _allEvents = new();
    private readonly List<VizEventItem> _allVizEvents = new();
    private CancellationTokenSource? _systemLoadRefreshCts;
    private int _systemLoadRefreshInFlight;
    private bool _streamEnabled;
    private SeverityOption _selectedSeverity;
    private string _contextRegex = string.Empty;
    private string _includeContextPrefixes = string.Empty;
    private string _excludeContextPrefixes = string.Empty;
    private string _includeSummaryPrefixes = string.Empty;
    private string _excludeSummaryPrefixes = string.Empty;
    private string _textFilter = string.Empty;
    private string _status = "Idle";
    private DebugEventItem? _selectedEvent;
    private string _selectedPayload = string.Empty;
    private VizTypeOption _selectedVizType;
    private string _vizRegionFilterText = string.Empty;
    private string _vizSearchFilterText = string.Empty;
    private string _vizStatus = "Streaming";
    private VizEventItem? _selectedVizEvent;
    private string _selectedVizPayload = string.Empty;
    private string _systemLoadResourceSummary = "Resource usage: awaiting worker telemetry.";
    private string _systemLoadPressureSummary = "Pressure: awaiting HiveMind telemetry.";
    private string _systemLoadTickSummary = "Tick health: awaiting HiveMind status.";
    private string _systemLoadHealthSummary = "Health: awaiting HiveMind status.";
    private string _systemLoadSparklinePathData = WorkbenchSystemLoadSummaryBuilder.EmptySparklinePathData;
    private string _systemLoadSparklineStroke = WorkbenchSystemLoadSummaryBuilder.NeutralSparklineStroke;

    public DebugPanelViewModel(WorkbenchClient client, UiDispatcher dispatcher, ConnectionViewModel? connections = null)
    {
        _client = client;
        _dispatcher = dispatcher;
        _connections = connections;
        DebugEvents = new ObservableCollection<DebugEventItem>();
        SeverityOptions = new ObservableCollection<SeverityOption>(SeverityOption.CreateDefaults());
        _selectedSeverity = SeverityOptions[2];
        VizEvents = new ObservableCollection<VizEventItem>();
        VizTypeOptions = new ObservableCollection<VizTypeOption>(VizTypeOption.CreateDefaults());
        _selectedVizType = VizTypeOptions[0];

        ApplyFilterCommand = new AsyncRelayCommand(ApplyFilterAsync);
        ExportCommand = new AsyncRelayCommand(ExportAsync, () => DebugEvents.Count > 0);
        ClearCommand = new RelayCommand(Clear);
        ExportVizCommand = new AsyncRelayCommand(ExportVizAsync, () => VizEvents.Count > 0);
        ClearVizCommand = new RelayCommand(ClearViz);

        if (_connections is not null)
        {
            _connections.PropertyChanged += OnConnectionsPropertyChanged;
            StartSystemLoadPolling();
            RequestSystemLoadRefresh();
        }
    }

    public ObservableCollection<DebugEventItem> DebugEvents { get; }

    public ObservableCollection<SeverityOption> SeverityOptions { get; }

    public ObservableCollection<VizEventItem> VizEvents { get; }

    public ObservableCollection<VizTypeOption> VizTypeOptions { get; }

    /// <summary>
    /// Raised when the persisted debug subscription settings change and observability subscriptions should refresh.
    /// </summary>
    public event Action? SubscriptionSettingsChanged;

    public bool StreamEnabled
    {
        get => _streamEnabled;
        set
        {
            if (SetProperty(ref _streamEnabled, value))
            {
                SubscriptionSettingsChanged?.Invoke();
            }
        }
    }

    public SeverityOption SelectedSeverity
    {
        get => _selectedSeverity;
        set => SetProperty(ref _selectedSeverity, value);
    }

    public string ContextRegex
    {
        get => _contextRegex;
        set => SetProperty(ref _contextRegex, value);
    }

    public string IncludeContextPrefixes
    {
        get => _includeContextPrefixes;
        set => SetProperty(ref _includeContextPrefixes, value);
    }

    public string ExcludeContextPrefixes
    {
        get => _excludeContextPrefixes;
        set => SetProperty(ref _excludeContextPrefixes, value);
    }

    public string IncludeSummaryPrefixes
    {
        get => _includeSummaryPrefixes;
        set => SetProperty(ref _includeSummaryPrefixes, value);
    }

    public string ExcludeSummaryPrefixes
    {
        get => _excludeSummaryPrefixes;
        set => SetProperty(ref _excludeSummaryPrefixes, value);
    }

    public string TextFilter
    {
        get => _textFilter;
        set
        {
            if (SetProperty(ref _textFilter, value))
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

    public string VizStatus
    {
        get => _vizStatus;
        set => SetProperty(ref _vizStatus, value);
    }

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

    public DebugEventItem? SelectedEvent
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

    public VizTypeOption SelectedVizType
    {
        get => _selectedVizType;
        set
        {
            if (SetProperty(ref _selectedVizType, value))
            {
                RefreshFilteredVizEvents();
            }
        }
    }

    public string VizRegionFilterText
    {
        get => _vizRegionFilterText;
        set
        {
            if (SetProperty(ref _vizRegionFilterText, value))
            {
                RefreshFilteredVizEvents();
            }
        }
    }

    public string VizSearchFilterText
    {
        get => _vizSearchFilterText;
        set
        {
            if (SetProperty(ref _vizSearchFilterText, value))
            {
                RefreshFilteredVizEvents();
            }
        }
    }

    public VizEventItem? SelectedVizEvent
    {
        get => _selectedVizEvent;
        set
        {
            if (SetProperty(ref _selectedVizEvent, value))
            {
                SelectedVizPayload = BuildVizPayload(value);
            }
        }
    }

    public string SelectedVizPayload
    {
        get => _selectedVizPayload;
        set => SetProperty(ref _selectedVizPayload, value);
    }

    public AsyncRelayCommand ApplyFilterCommand { get; }

    public AsyncRelayCommand ExportCommand { get; }

    public RelayCommand ClearCommand { get; }

    public AsyncRelayCommand ExportVizCommand { get; }

    public RelayCommand ClearVizCommand { get; }

    public ValueTask DisposeAsync()
    {
        if (_connections is not null)
        {
            _connections.PropertyChanged -= OnConnectionsPropertyChanged;
        }

        _systemLoadRefreshCts?.Cancel();
        _systemLoadRefreshCts = null;
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Records a debug event and reapplies the active text filter.
    /// </summary>
    public void AddDebugEvent(DebugEventItem item)
    {
        _dispatcher.Post(() =>
        {
            _allEvents.Insert(0, item);
            Trim(_allEvents);
            RefreshFilteredEvents();
        });
    }

    /// <summary>
    /// Records a visualization event and reapplies the active visualization filters.
    /// </summary>
    public void AddVizEvent(VizEventItem item)
    {
        _dispatcher.Post(() =>
        {
            _allVizEvents.Insert(0, item);
            Trim(_allVizEvents);
            RefreshFilteredVizEvents();
        });
    }

    /// <summary>
    /// Builds the current debug subscription request from the panel filters.
    /// </summary>
    public DebugSubscriptionFilter BuildSubscriptionFilter()
        => new(
            StreamEnabled: StreamEnabled,
            MinSeverity: SelectedSeverity.Severity,
            ContextRegex: ContextRegex ?? string.Empty,
            IncludeContextPrefixes: SplitList(IncludeContextPrefixes),
            ExcludeContextPrefixes: SplitList(ExcludeContextPrefixes),
            IncludeSummaryPrefixes: SplitList(IncludeSummaryPrefixes),
            ExcludeSummaryPrefixes: SplitList(ExcludeSummaryPrefixes));

    /// <summary>
    /// Applies a SettingsMonitor-backed debug setting to the local draft state.
    /// </summary>
    public bool ApplySetting(SettingItem item)
    {
        if (string.IsNullOrWhiteSpace(item.Key))
        {
            return false;
        }

        var key = item.Key.Trim();
        return ApplyPostedSetting(key, DebugSettingsKeys.EnabledKey, () => StreamEnabled = ParseBool(item.Value))
            || ApplyPostedSetting(
                key,
                DebugSettingsKeys.MinSeverityKey,
                () =>
                {
                    var parsed = ParseSeverity(item.Value);
                    var selected = SeverityOptions.FirstOrDefault(option => option.Severity == parsed) ?? SeverityOptions[2];
                    SelectedSeverity = selected;
                })
            || ApplyPostedSetting(key, DebugSettingsKeys.ContextRegexKey, () => ContextRegex = item.Value ?? string.Empty)
            || ApplyPostedSetting(key, DebugSettingsKeys.IncludeContextPrefixesKey, () => IncludeContextPrefixes = item.Value ?? string.Empty)
            || ApplyPostedSetting(key, DebugSettingsKeys.ExcludeContextPrefixesKey, () => ExcludeContextPrefixes = item.Value ?? string.Empty)
            || ApplyPostedSetting(key, DebugSettingsKeys.IncludeSummaryPrefixesKey, () => IncludeSummaryPrefixes = item.Value ?? string.Empty)
            || ApplyPostedSetting(key, DebugSettingsKeys.ExcludeSummaryPrefixesKey, () => ExcludeSummaryPrefixes = item.Value ?? string.Empty);
    }

    private async Task ApplyFilterAsync()
    {
        if (_connections is not null && !_connections.HasDebugServiceReadiness())
        {
            Status = "Connect Settings and Observability first.";
            return;
        }

        Status = "Applying filter...";
        var filter = BuildSubscriptionFilter();
        var settingsApplied = true;

        settingsApplied &= await TryApplyDebugSettingAsync(
            DebugSettingsKeys.EnabledKey,
            filter.StreamEnabled ? "true" : "false").ConfigureAwait(false);
        settingsApplied &= await TryApplyDebugSettingAsync(
            DebugSettingsKeys.MinSeverityKey,
            filter.MinSeverity.ToString()).ConfigureAwait(false);
        settingsApplied &= await TryApplyDebugSettingAsync(
            DebugSettingsKeys.ContextRegexKey,
            filter.ContextRegex).ConfigureAwait(false);
        settingsApplied &= await TryApplyDebugSettingAsync(
            DebugSettingsKeys.IncludeContextPrefixesKey,
            string.Join(",", filter.IncludeContextPrefixes)).ConfigureAwait(false);
        settingsApplied &= await TryApplyDebugSettingAsync(
            DebugSettingsKeys.ExcludeContextPrefixesKey,
            string.Join(",", filter.ExcludeContextPrefixes)).ConfigureAwait(false);
        settingsApplied &= await TryApplyDebugSettingAsync(
            DebugSettingsKeys.IncludeSummaryPrefixesKey,
            string.Join(",", filter.IncludeSummaryPrefixes)).ConfigureAwait(false);
        settingsApplied &= await TryApplyDebugSettingAsync(
            DebugSettingsKeys.ExcludeSummaryPrefixesKey,
            string.Join(",", filter.ExcludeSummaryPrefixes)).ConfigureAwait(false);

        await _client.RefreshDebugFilterAsync(filter).ConfigureAwait(false);
        Status = settingsApplied ? "Filter updated." : "Filter updated (settings unavailable).";
    }

    private async Task<bool> TryApplyDebugSettingAsync(string key, string value)
    {
        var result = await _client.SetSettingAsync(key, value ?? string.Empty).ConfigureAwait(false);
        return result is not null;
    }

    private void Clear()
    {
        _allEvents.Clear();
        DebugEvents.Clear();
        SelectedEvent = null;
        ExportCommand.RaiseCanExecuteChanged();
        Status = "Cleared.";
    }

    private void ClearViz()
    {
        _allVizEvents.Clear();
        VizEvents.Clear();
        SelectedVizEvent = null;
        SelectedVizPayload = string.Empty;
        ExportVizCommand.RaiseCanExecuteChanged();
        VizStatus = "Cleared.";
    }

    private async Task ExportAsync()
    {
        if (DebugEvents.Count == 0)
        {
            Status = "Nothing to export.";
            return;
        }

        var file = await WorkbenchStorageDialogs.PickSaveFileAsync("Export debug events", "JSON files", "json", "debug-events.json").ConfigureAwait(false);
        if (file is null)
        {
            Status = "Export canceled.";
            return;
        }

        try
        {
            var payload = DebugEvents.Select(DebugExportItem.From).ToList();
            var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
            await WorkbenchStorageDialogs.WriteAllTextAsync(file, json).ConfigureAwait(false);
            Status = $"Exported {payload.Count} events to {WorkbenchStorageDialogs.FormatPath(file)}.";
        }
        catch (Exception ex)
        {
            Status = $"Export failed: {ex.Message}";
        }
    }

    private async Task ExportVizAsync()
    {
        if (VizEvents.Count == 0)
        {
            VizStatus = "Nothing to export.";
            return;
        }

        var file = await WorkbenchStorageDialogs.PickSaveFileAsync("Export viz events", "JSON files", "json", "viz-events.json").ConfigureAwait(false);
        if (file is null)
        {
            VizStatus = "Export canceled.";
            return;
        }

        try
        {
            var payload = VizEvents.Select(VizExportItem.From).ToList();
            var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
            await WorkbenchStorageDialogs.WriteAllTextAsync(file, json).ConfigureAwait(false);
            VizStatus = $"Exported {payload.Count} events to {WorkbenchStorageDialogs.FormatPath(file)}.";
        }
        catch (Exception ex)
        {
            VizStatus = $"Export failed: {ex.Message}";
        }
    }

    private void RefreshFilteredEvents()
    {
        var selected = SelectedEvent;
        DebugEvents.Clear();

        foreach (var item in _allEvents)
        {
            if (MatchesFilter(item))
            {
                DebugEvents.Add(item);
            }
        }

        if (selected is not null && DebugEvents.Contains(selected))
        {
            SelectedEvent = selected;
        }
        else
        {
            SelectedEvent = DebugEvents.FirstOrDefault();
        }

        ExportCommand.RaiseCanExecuteChanged();
    }

    private void RefreshFilteredVizEvents()
    {
        var selected = SelectedVizEvent;
        VizEvents.Clear();
        VizStatus = "Streaming";

        foreach (var item in _allVizEvents)
        {
            if (MatchesVizFilter(item))
            {
                VizEvents.Add(item);
            }
        }

        if (selected is not null && VizEvents.Contains(selected))
        {
            SelectedVizEvent = selected;
        }
        else
        {
            SelectedVizEvent = null;
        }

        ExportVizCommand.RaiseCanExecuteChanged();
    }

    private bool MatchesFilter(DebugEventItem item)
    {
        if (string.IsNullOrWhiteSpace(TextFilter))
        {
            return true;
        }

        var needle = TextFilter.Trim();
        return ContainsIgnoreCase(item.Context, needle)
            || ContainsIgnoreCase(item.Summary, needle)
            || ContainsIgnoreCase(item.Message, needle)
            || ContainsIgnoreCase(item.Severity, needle)
            || ContainsIgnoreCase(item.SenderActor, needle)
            || ContainsIgnoreCase(item.SenderNode, needle);
    }

    private bool MatchesVizFilter(VizEventItem item)
    {
        if (SelectedVizType.TypeFilter is not null && !string.Equals(item.Type, SelectedVizType.TypeFilter, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(VizRegionFilterText))
        {
            if (!string.Equals(item.Region, VizRegionFilterText.Trim(), StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        if (!string.IsNullOrWhiteSpace(VizSearchFilterText))
        {
            var needle = VizSearchFilterText.Trim();
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

    private static IReadOnlyList<string> SplitList(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return Array.Empty<string>();
        }

        var entries = value
            .Split([',', ';', '\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(static entry => entry.Length > 0)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        return entries.Length == 0 ? Array.Empty<string>() : entries;
    }

    private static bool ParseBool(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "1" or "true" or "yes" or "on" => true,
            _ => false
        };
    }

    private static Nbn.Proto.Severity ParseSeverity(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return Nbn.Proto.Severity.SevInfo;
        }

        if (Enum.TryParse<Nbn.Proto.Severity>(value, ignoreCase: true, out var direct))
        {
            return direct;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "trace" or "sev_trace" => Nbn.Proto.Severity.SevTrace,
            "debug" or "sev_debug" => Nbn.Proto.Severity.SevDebug,
            "info" or "sev_info" => Nbn.Proto.Severity.SevInfo,
            "warn" or "warning" or "sev_warn" => Nbn.Proto.Severity.SevWarn,
            "error" or "sev_error" => Nbn.Proto.Severity.SevError,
            "fatal" or "sev_fatal" => Nbn.Proto.Severity.SevFatal,
            _ => Nbn.Proto.Severity.SevInfo
        };
    }

    private static bool ContainsIgnoreCase(string? haystack, string needle)
    {
        if (string.IsNullOrEmpty(haystack))
        {
            return false;
        }

        return haystack.IndexOf(needle, StringComparison.OrdinalIgnoreCase) >= 0;
    }

    private static string BuildPayload(DebugEventItem? item)
    {
        if (item is null)
        {
            return string.Empty;
        }

        return $"[{item.Severity}] {item.Context}\n{item.Summary}\n\n{item.Message}\n\nSender: {item.SenderActor}\nNode: {item.SenderNode}";
    }

    private static string BuildVizPayload(VizEventItem? item)
    {
        if (item is null)
        {
            return string.Empty;
        }

        return $"[{item.Type}] brain={item.BrainId} tick={item.TickId}\nregion={item.Region} source={item.Source} target={item.Target}\nvalue={item.Value} strength={item.Strength}\nEventId={item.EventId}";
    }

    private bool ApplyPostedSetting(string actualKey, string expectedKey, Action apply)
    {
        if (!string.Equals(actualKey, expectedKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        _dispatcher.Post(apply);
        return true;
    }

    private static void Trim<T>(ICollection<T> collection)
    {
        while (collection.Count > MaxEvents && collection is IList<T> list)
        {
            list.RemoveAt(list.Count - 1);
        }
    }

    private void OnConnectionsPropertyChanged(object? sender, PropertyChangedEventArgs args)
    {
        if (string.IsNullOrWhiteSpace(args.PropertyName)
            || !SystemLoadRefreshTriggerProperties.Contains(args.PropertyName))
        {
            return;
        }

        RequestSystemLoadRefresh();
    }

    private void StartSystemLoadPolling()
    {
        if (_connections is null)
        {
            return;
        }

        _systemLoadRefreshCts?.Cancel();
        _systemLoadRefreshCts = new CancellationTokenSource();
        _ = PollSystemLoadAsync(_systemLoadRefreshCts.Token);
    }

    private void RequestSystemLoadRefresh()
    {
        if (Interlocked.Exchange(ref _systemLoadRefreshInFlight, 1) != 0)
        {
            return;
        }

        _ = RefreshSystemLoadSnapshotAsync();
    }

    private async Task PollSystemLoadAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(SystemLoadRefreshInterval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            RequestSystemLoadRefresh();
        }
    }

    private async Task RefreshSystemLoadSnapshotAsync()
    {
        try
        {
            if (_connections is null)
            {
                return;
            }

            var settingsReady = _connections.IsSettingsServiceReady();
            var hiveReady = _connections.IsHiveMindServiceReady();
            if (!settingsReady && !hiveReady)
            {
                _dispatcher.Post(() =>
                {
                    SystemLoadResourceSummary = "Resource usage: connect Settings to load worker telemetry.";
                    SystemLoadPressureSummary = "Pressure: connect HiveMind to view current and recent worker pressure.";
                    SystemLoadTickSummary = "Tick health: connect HiveMind to view recent timeout and cadence pressure.";
                    SystemLoadHealthSummary = "Health: connect HiveMind to view long-window trend and early warning signals.";
                    SystemLoadSparklinePathData = WorkbenchSystemLoadSummaryBuilder.EmptySparklinePathData;
                    SystemLoadSparklineStroke = WorkbenchSystemLoadSummaryBuilder.NeutralSparklineStroke;
                    _systemLoadHistory.Clear();
                });
                return;
            }

            var inventoryTask = settingsReady
                ? _client.ListWorkerInventorySnapshotAsync()
                : Task.FromResult<Nbn.Proto.Settings.WorkerInventorySnapshotResponse?>(null);
            var hiveMindTask = hiveReady
                ? _client.GetHiveMindStatusAsync()
                : Task.FromResult<Nbn.Proto.Control.HiveMindStatus?>(null);

            var inventory = await inventoryTask.ConfigureAwait(false);
            var hiveMindStatus = await hiveMindTask.ConfigureAwait(false);
            var referenceMs = inventory is not null && inventory.SnapshotMs > 0
                ? (long)inventory.SnapshotMs
                : DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var workers = inventory?.Workers?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.WorkerReadinessCapability>();
            var filteredWorkers = WorkbenchSystemLoadSummaryBuilder.FilterWorkers(
                workers,
                referenceMs,
                worker => WorkbenchWorkerHostGrouping.IsWorkerHostCandidate(
                    _connections,
                    worker.LogicalName,
                    worker.RootActorName));
            var history = _systemLoadHistory.Record(filteredWorkers, hiveMindStatus, referenceMs);
            var summary = WorkbenchSystemLoadSummaryBuilder.Build(filteredWorkers, hiveMindStatus, history);

            _dispatcher.Post(() =>
            {
                SystemLoadResourceSummary = summary.ResourceSummary;
                SystemLoadPressureSummary = summary.PressureSummary;
                SystemLoadTickSummary = summary.TickSummary;
                SystemLoadHealthSummary = summary.HealthSummary;
                SystemLoadSparklinePathData = summary.SparklinePathData;
                SystemLoadSparklineStroke = summary.SparklineStroke;
            });
        }
        finally
        {
            Interlocked.Exchange(ref _systemLoadRefreshInFlight, 0);
        }
    }
}

public sealed record SeverityOption(string Label, Nbn.Proto.Severity Severity)
{
    public static IReadOnlyList<SeverityOption> CreateDefaults()
        => new List<SeverityOption>
        {
            new("Trace", Nbn.Proto.Severity.SevTrace),
            new("Debug", Nbn.Proto.Severity.SevDebug),
            new("Info", Nbn.Proto.Severity.SevInfo),
            new("Warn", Nbn.Proto.Severity.SevWarn),
            new("Error", Nbn.Proto.Severity.SevError),
            new("Fatal", Nbn.Proto.Severity.SevFatal)
        };
}

public sealed record DebugExportItem(
    string Time,
    string Severity,
    string Context,
    string Summary,
    string Message,
    string SenderActor,
    string SenderNode)
{
    public static DebugExportItem From(DebugEventItem item)
        => new(
            item.Time.ToString("O"),
            item.Severity,
            item.Context,
            item.Summary,
            item.Message,
            item.SenderActor,
            item.SenderNode);
}

public sealed record VizTypeOption(string Label, string? TypeFilter)
{
    public static IReadOnlyList<VizTypeOption> CreateDefaults()
    {
        var options = new List<VizTypeOption> { new("All types", null) };
        foreach (var value in Enum.GetValues<VizEventType>())
        {
            if (value == VizEventType.VizUnknown)
            {
                continue;
            }

            options.Add(new VizTypeOption(value.ToString(), value.ToString()));
        }

        return options;
    }
}

public sealed record VizExportItem(
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
    public static VizExportItem From(VizEventItem item)
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
