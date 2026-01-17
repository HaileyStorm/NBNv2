namespace Nbn.Tools.Workbench.ViewModels;

public sealed class DesignerPanelViewModel : ViewModelBase
{
    private string _status = "Designer tooling pending.";

    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }

    public RelayCommand PlaceholderCommand { get; }

    public DesignerPanelViewModel()
    {
        PlaceholderCommand = new RelayCommand(() => Status = "Designer actions are wired for IO export/import." );
    }
}
