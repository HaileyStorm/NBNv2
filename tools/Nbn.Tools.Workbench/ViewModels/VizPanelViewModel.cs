using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text.Json;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class VizPanelViewModel : ViewModelBase
{
    private const int MaxEvents = 400;
    private readonly UiDispatcher _dispatcher;
    private readonly IoPanelViewModel _brain;
    private readonly List<VizEventItem> _allEvents = new();
    private string _status = "Streaming";
    private string _regionFocusText = "0";
    private string _regionFilterText = string.Empty;
    private string _searchFilterText = string.Empty;
    private string _brainEntryText = string.Empty;
    private BrainListItem? _selectedBrain;
    private VizTypeOption _selectedVizType;
    private bool _suspendSelection;
    private VizEventItem? _selectedEvent;
    private string _selectedPayload = string.Empty;

    public VizPanelViewModel(UiDispatcher dispatcher, IoPanelViewModel brain)
    {
        _dispatcher = dispatcher;
        _brain = brain;
        VizEvents = new ObservableCollection<VizEventItem>();
        KnownBrains = new ObservableCollection<BrainListItem>();
        VizTypeOptions = new ObservableCollection<VizTypeOption>(VizTypeOption.CreateDefaults());
        _selectedVizType = VizTypeOptions[0];
        ClearCommand = new RelayCommand(Clear);
        AddBrainCommand = new RelayCommand(AddBrainFromEntry);
        ZoomCommand = new RelayCommand(ZoomRegion);
        ExportCommand = new AsyncRelayCommand(ExportAsync, () => VizEvents.Count > 0);
        ApplyEnergyCreditCommand = new RelayCommand(() => _brain.ApplyEnergyCreditSelected());
        ApplyEnergyRateCommand = new RelayCommand(() => _brain.ApplyEnergyRateSelected());
        ApplyCostEnergyCommand = new RelayCommand(() => _brain.ApplyCostEnergySelected());
    }

    public IoPanelViewModel Brain => _brain;

    public ObservableCollection<VizEventItem> VizEvents { get; }

    public ObservableCollection<BrainListItem> KnownBrains { get; }

    public ObservableCollection<VizTypeOption> VizTypeOptions { get; }

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
            if (SetProperty(ref _selectedBrain, value))
            {
                if (!_suspendSelection)
                {
                    _brain.SelectBrain(value?.BrainId);
                    RefreshFilteredEvents();
                }
            }
        }
    }

    public VizTypeOption SelectedVizType
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
        var selectedId = SelectedBrain?.Id;
        _suspendSelection = true;
        KnownBrains.Clear();
        foreach (var brain in brains)
        {
            KnownBrains.Add(brain);
        }

        BrainListItem? match = null;
        if (!string.IsNullOrWhiteSpace(selectedId))
        {
            match = KnownBrains.FirstOrDefault(entry => entry.Id == selectedId);
            if (match is not null)
            {
                SelectedBrain = match;
                _suspendSelection = false;
                _brain.SelectBrain(match.BrainId);
                return;
            }
        }

        if (KnownBrains.Count > 0)
        {
            match = KnownBrains[0];
        }
        SelectedBrain = match;
        _suspendSelection = false;
        _brain.SelectBrain(match?.BrainId);
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
        if (int.TryParse(RegionFocusText, out var regionId))
        {
            Status = $"Zoom focus set to region {regionId}.";
        }
        else
        {
            Status = "Region ID invalid.";
        }
    }

    private void Clear()
    {
        _allEvents.Clear();
        VizEvents.Clear();
        SelectedEvent = null;
        ExportCommand.RaiseCanExecuteChanged();
        Status = "Cleared.";
    }

    private async Task ExportAsync()
    {
        if (VizEvents.Count == 0)
        {
            Status = "Nothing to export.";
            return;
        }

        var path = await PickSaveFileAsync("Export viz events", "JSON files", "json", "viz-events.json");
        if (string.IsNullOrWhiteSpace(path))
        {
            Status = "Export canceled.";
            return;
        }

        try
        {
            var payload = VizEvents.Select(VizExportItem.From).ToList();
            var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(path, json);
            Status = $"Exported {payload.Count} events.";
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

        foreach (var item in _allEvents)
        {
            if (MatchesFilter(item))
            {
                VizEvents.Add(item);
            }
        }

        if (selected is not null && VizEvents.Contains(selected))
        {
            SelectedEvent = selected;
        }
        else
        {
            SelectedEvent = VizEvents.FirstOrDefault();
        }

        ExportCommand.RaiseCanExecuteChanged();
    }

    private bool MatchesFilter(VizEventItem item)
    {
        if (SelectedBrain is not null && !string.Equals(item.BrainId, SelectedBrain.Id, StringComparison.OrdinalIgnoreCase))
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

    private static async Task<string?> PickSaveFileAsync(string title, string filterName, string extension, string? suggestedName)
    {
        var window = GetMainWindow();
        if (window is null)
        {
            return null;
        }

        var dialog = new SaveFileDialog
        {
            Title = title,
            DefaultExtension = extension,
            InitialFileName = suggestedName,
            Filters = new List<FileDialogFilter>
            {
                new() { Name = filterName, Extensions = new List<string> { extension } }
            }
        };

        return await dialog.ShowAsync(window);
    }

    private static Window? GetMainWindow()
    {
        if (Application.Current?.ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            return desktop.MainWindow;
        }

        return null;
    }

    private static void Trim<T>(ICollection<T> collection)
    {
        while (collection.Count > MaxEvents && collection is IList<T> list)
        {
            list.RemoveAt(list.Count - 1);
        }
    }
}

public sealed record VizTypeOption(string Label, string? TypeFilter)
{
    public static IReadOnlyList<VizTypeOption> CreateDefaults()
    {
        var options = new List<VizTypeOption> { new("All types", null) };
        foreach (var value in Enum.GetValues<Nbn.Proto.Viz.VizEventType>())
        {
            if (value == Nbn.Proto.Viz.VizEventType.VizUnknown)
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
