using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Platform.Storage;
using Nbn.Proto.Viz;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class DebugPanelViewModel : ViewModelBase
{
    private const int MaxEvents = 400;
    private readonly UiDispatcher _dispatcher;
    private readonly WorkbenchClient _client;
    private readonly List<DebugEventItem> _allEvents = new();
    private readonly List<VizEventItem> _allVizEvents = new();
    private SeverityOption _selectedSeverity;
    private string _contextRegex = string.Empty;
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

    public DebugPanelViewModel(WorkbenchClient client, UiDispatcher dispatcher)
    {
        _client = client;
        _dispatcher = dispatcher;
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
    }

    public ObservableCollection<DebugEventItem> DebugEvents { get; }

    public ObservableCollection<SeverityOption> SeverityOptions { get; }

    public ObservableCollection<VizEventItem> VizEvents { get; }

    public ObservableCollection<VizTypeOption> VizTypeOptions { get; }

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

    public void AddDebugEvent(DebugEventItem item)
    {
        _dispatcher.Post(() =>
        {
            _allEvents.Insert(0, item);
            Trim(_allEvents);
            RefreshFilteredEvents();
        });
    }

    public void AddVizEvent(VizEventItem item)
    {
        _dispatcher.Post(() =>
        {
            _allVizEvents.Insert(0, item);
            Trim(_allVizEvents);
            RefreshFilteredVizEvents();
        });
    }

    private async Task ApplyFilterAsync()
    {
        Status = "Applying filter...";
        await _client.RefreshDebugFilterAsync(SelectedSeverity.Severity, ContextRegex);
        Status = "Filter updated.";
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

        var file = await PickSaveFileAsync("Export debug events", "JSON files", "json", "debug-events.json");
        if (file is null)
        {
            Status = "Export canceled.";
            return;
        }

        try
        {
            var payload = DebugEvents.Select(DebugExportItem.From).ToList();
            var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
            await WriteAllTextAsync(file, json);
            Status = $"Exported {payload.Count} events to {FormatPath(file)}.";
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

        var file = await PickSaveFileAsync("Export viz events", "JSON files", "json", "viz-events.json");
        if (file is null)
        {
            VizStatus = "Export canceled.";
            return;
        }

        try
        {
            var payload = VizEvents.Select(VizExportItem.From).ToList();
            var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
            await WriteAllTextAsync(file, json);
            VizStatus = $"Exported {payload.Count} events to {FormatPath(file)}.";
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
