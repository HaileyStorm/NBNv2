using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class VizPanelViewModel : ViewModelBase
{
    private const int MaxEvents = 400;
    private readonly UiDispatcher _dispatcher;
    private readonly IoPanelViewModel _brain;
    private string _status = "Streaming";
    private string _regionFocusText = "0";
    private string _brainEntryText = string.Empty;
    private BrainListItem? _selectedBrain;

    public VizPanelViewModel(UiDispatcher dispatcher, IoPanelViewModel brain)
    {
        _dispatcher = dispatcher;
        _brain = brain;
        VizEvents = new ObservableCollection<VizEventItem>();
        KnownBrains = new ObservableCollection<BrainListItem>();
        ClearCommand = new RelayCommand(Clear);
        AddBrainCommand = new RelayCommand(AddBrainFromEntry);
        ZoomCommand = new RelayCommand(ZoomRegion);
    }

    public IoPanelViewModel Brain => _brain;

    public ObservableCollection<VizEventItem> VizEvents { get; }

    public ObservableCollection<BrainListItem> KnownBrains { get; }

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
                if (value is not null)
                {
                    _brain.BrainIdText = value.Id;
                }
            }
        }
    }

    public string RegionFocusText
    {
        get => _regionFocusText;
        set => SetProperty(ref _regionFocusText, value);
    }

    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }

    public RelayCommand ClearCommand { get; }

    public RelayCommand AddBrainCommand { get; }

    public RelayCommand ZoomCommand { get; }

    public void AddBrainId(Guid id)
    {
        AddBrainId(id.ToString("D"));
    }

    public void SetBrains(IReadOnlyList<BrainListItem> brains)
    {
        var selectedId = SelectedBrain?.Id;
        KnownBrains.Clear();
        foreach (var brain in brains)
        {
            KnownBrains.Add(brain);
        }

        if (!string.IsNullOrWhiteSpace(selectedId))
        {
            SelectedBrain = KnownBrains.FirstOrDefault(entry => entry.Id == selectedId);
        }
    }

    public void AddVizEvent(VizEventItem item)
    {
        if (SelectedBrain is not null && !string.Equals(item.BrainId, SelectedBrain.Id, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        _dispatcher.Post(() =>
        {
            VizEvents.Insert(0, item);
            Trim(VizEvents);
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
        VizEvents.Clear();
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
