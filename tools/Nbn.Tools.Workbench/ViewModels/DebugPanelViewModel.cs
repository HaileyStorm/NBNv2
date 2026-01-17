using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class DebugPanelViewModel : ViewModelBase
{
    private const int MaxEvents = 400;
    private readonly UiDispatcher _dispatcher;
    private readonly WorkbenchClient _client;
    private SeverityOption _selectedSeverity;
    private string _contextRegex = string.Empty;
    private string _status = "Idle";

    public DebugPanelViewModel(WorkbenchClient client, UiDispatcher dispatcher)
    {
        _client = client;
        _dispatcher = dispatcher;
        DebugEvents = new ObservableCollection<DebugEventItem>();
        SeverityOptions = new ObservableCollection<SeverityOption>(SeverityOption.CreateDefaults());
        _selectedSeverity = SeverityOptions[2];

        ApplyFilterCommand = new AsyncRelayCommand(ApplyFilterAsync);
        ClearCommand = new RelayCommand(Clear);
    }

    public ObservableCollection<DebugEventItem> DebugEvents { get; }

    public ObservableCollection<SeverityOption> SeverityOptions { get; }

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

    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }

    public AsyncRelayCommand ApplyFilterCommand { get; }

    public RelayCommand ClearCommand { get; }

    public void AddDebugEvent(DebugEventItem item)
    {
        _dispatcher.Post(() =>
        {
            DebugEvents.Insert(0, item);
            Trim(DebugEvents);
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
        DebugEvents.Clear();
        Status = "Cleared.";
    }

    private static void Trim<T>(ObservableCollection<T> collection)
    {
        while (collection.Count > MaxEvents)
        {
            collection.RemoveAt(collection.Count - 1);
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
