namespace Nbn.Tools.Workbench.ViewModels;

public sealed class SettingEntryViewModel : ViewModelBase
{
    private readonly string _key;
    private string _value;
    private string _updated;
    private string _originalValue;

    public SettingEntryViewModel(string key, string value, string updated)
    {
        _key = key;
        _value = value;
        _originalValue = value;
        _updated = updated;
    }

    public string Key => _key;

    public string Value
    {
        get => _value;
        set
        {
            if (SetProperty(ref _value, value))
            {
                OnPropertyChanged(nameof(IsDirty));
            }
        }
    }

    public string Updated
    {
        get => _updated;
        set => SetProperty(ref _updated, value);
    }

    public bool IsDirty => !string.Equals(_value, _originalValue, StringComparison.Ordinal);

    public void UpdateFromServer(string value, string updated, bool preserveEdits)
    {
        _updated = updated;

        if (preserveEdits && IsDirty)
        {
            _originalValue = value;
            OnPropertyChanged(nameof(Updated));
            OnPropertyChanged(nameof(IsDirty));
            return;
        }

        _originalValue = value;
        _value = value;
        OnPropertyChanged(nameof(Value));
        OnPropertyChanged(nameof(Updated));
        OnPropertyChanged(nameof(IsDirty));
    }

    public void MarkApplied(string value, string updated)
    {
        _originalValue = value;
        _value = value;
        _updated = updated;
        OnPropertyChanged(nameof(Value));
        OnPropertyChanged(nameof(Updated));
        OnPropertyChanged(nameof(IsDirty));
    }
}
