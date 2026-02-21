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
    private string _status = "Streaming";
    private string _regionFocusText = "0";
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

    public VizPanelViewModel(UiDispatcher dispatcher, IoPanelViewModel brain)
    {
        _dispatcher = dispatcher;
        _brain = brain;
        VizEvents = new ObservableCollection<VizEventItem>();
        ActivityStats = new ObservableCollection<VizActivityStatItem>();
        RegionActivity = new ObservableCollection<VizRegionActivityItem>();
        EdgeActivity = new ObservableCollection<VizEdgeActivityItem>();
        TickActivity = new ObservableCollection<VizTickActivityItem>();
        KnownBrains = new ObservableCollection<BrainListItem>();
        VizPanelTypeOptions = new ObservableCollection<VizPanelTypeOption>(VizPanelTypeOption.CreateDefaults());
        _selectedVizType = VizPanelTypeOptions[0];
        ClearCommand = new RelayCommand(Clear);
        AddBrainCommand = new RelayCommand(AddBrainFromEntry);
        ZoomCommand = new RelayCommand(ZoomRegion);
        ApplyActivityOptionsCommand = new RelayCommand(ApplyActivityOptions);
        ExportCommand = new AsyncRelayCommand(ExportAsync, () => VizEvents.Count > 0);
        ApplyEnergyCreditCommand = new RelayCommand(() => _brain.ApplyEnergyCreditSelected());
        ApplyEnergyRateCommand = new RelayCommand(() => _brain.ApplyEnergyRateSelected());
        ApplyCostEnergyCommand = new RelayCommand(() => _brain.ApplyCostEnergySelected());
        RefreshActivityProjection();
    }

    public IoPanelViewModel Brain => _brain;

    public ObservableCollection<VizEventItem> VizEvents { get; }
    public ObservableCollection<VizActivityStatItem> ActivityStats { get; }
    public ObservableCollection<VizRegionActivityItem> RegionActivity { get; }
    public ObservableCollection<VizEdgeActivityItem> EdgeActivity { get; }
    public ObservableCollection<VizTickActivityItem> TickActivity { get; }

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

    public RelayCommand ApplyActivityOptionsCommand { get; }

    public AsyncRelayCommand ExportCommand { get; }

    public RelayCommand ApplyEnergyCreditCommand { get; }

    public RelayCommand ApplyEnergyRateCommand { get; }

    public RelayCommand ApplyCostEnergyCommand { get; }

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
        _dispatcher.Post(() =>
        {
            _allEvents.Insert(0, item);
            Trim(_allEvents);
            RefreshFilteredEvents();
        });
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

        RegionFilterText = regionId.ToString(CultureInfo.InvariantCulture);
        Status = $"Zoom focus set to region {regionId}.";
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
        _allEvents.Clear();
        VizEvents.Clear();
        SelectedEvent = null;
        SelectedPayload = string.Empty;
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
            Status = "No events for selected brain; showing all.";
            foreach (var item in _allEvents)
            {
                if (MatchesFilter(item, ignoreBrain: true))
                {
                    VizEvents.Add(item);
                }
            }
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
        if (!ignoreBrain && SelectedBrain is not null
            && !string.Equals(item.BrainId, SelectedBrain.Id, StringComparison.OrdinalIgnoreCase))
        {
            return false;
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

    private void RefreshActivityProjection()
    {
        var options = new VizActivityProjectionOptions(
            ParseTickWindowOrDefault(),
            IncludeLowSignalEvents,
            TryParseRegionId(RegionFocusText, out var regionId) ? regionId : null);

        var projection = VizActivityProjectionBuilder.Build(VizEvents, options);

        ReplaceItems(ActivityStats, projection.Stats);
        ReplaceItems(RegionActivity, projection.Regions);
        ReplaceItems(EdgeActivity, projection.Edges);
        ReplaceItems(TickActivity, projection.Ticks);
        ActivitySummary = projection.Summary;
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
