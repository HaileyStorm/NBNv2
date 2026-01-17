using System.Collections.ObjectModel;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class VizPanelViewModel : ViewModelBase
{
    private const int MaxEvents = 400;
    private readonly UiDispatcher _dispatcher;
    private string _status = "Streaming";

    public VizPanelViewModel(UiDispatcher dispatcher)
    {
        _dispatcher = dispatcher;
        VizEvents = new ObservableCollection<VizEventItem>();
        ClearCommand = new RelayCommand(Clear);
    }

    public ObservableCollection<VizEventItem> VizEvents { get; }

    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }

    public RelayCommand ClearCommand { get; }

    public void AddVizEvent(VizEventItem item)
    {
        _dispatcher.Post(() =>
        {
            VizEvents.Insert(0, item);
            Trim(VizEvents);
        });
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
