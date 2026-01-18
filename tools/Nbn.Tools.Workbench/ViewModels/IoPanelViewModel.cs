using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Globalization;
using Nbn.Proto.Io;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class IoPanelViewModel : ViewModelBase
{
    private const int MaxEvents = 300;
    private readonly WorkbenchClient _client;
    private readonly UiDispatcher _dispatcher;
    private string _brainIdText = string.Empty;
    private string _inputIndexText = "0";
    private string _inputValueText = "0";
    private string _inputVectorText = string.Empty;
    private string _energyCreditText = "1000";
    private string _energyRateText = "0";
    private string _plasticityRateText = "0.001";
    private bool _costEnabled;
    private bool _energyEnabled;
    private bool _costEnergyEnabled;
    private bool _plasticityEnabled;
    private PlasticityModeOption _selectedPlasticityMode;
    private string _brainInfoSummary = "No brain selected.";
    private string _activeBrainsSummary = "No active brains loaded.";
    private List<Guid> _activeBrains = new();
    private Guid? _selectedBrainId;

    public IoPanelViewModel(WorkbenchClient client, UiDispatcher dispatcher)
    {
        _client = client;
        _dispatcher = dispatcher;
        OutputEvents = new ObservableCollection<OutputEventItem>();
        VectorEvents = new ObservableCollection<OutputVectorEventItem>();
        PlasticityModes = new ObservableCollection<PlasticityModeOption>
        {
            new("Probabilistic", true),
            new("Absolute", false)
        };
        _selectedPlasticityMode = PlasticityModes[0];

        RequestInfoCommand = new AsyncRelayCommand(RequestInfoAsync);
        SubscribeOutputsCommand = new RelayCommand(() => Subscribe(false));
        UnsubscribeOutputsCommand = new RelayCommand(() => Unsubscribe(false));
        SubscribeVectorCommand = new RelayCommand(() => Subscribe(true));
        UnsubscribeVectorCommand = new RelayCommand(() => Unsubscribe(true));
        SendInputCommand = new RelayCommand(SendInput);
        SendVectorCommand = new RelayCommand(SendVector);
        ApplyEnergyCreditCommand = new RelayCommand(ApplyEnergyCredit);
        ApplyEnergyRateCommand = new RelayCommand(ApplyEnergyRate);
        ApplyCostEnergyCommand = new RelayCommand(ApplyCostEnergy);
        ApplyPlasticityCommand = new RelayCommand(ApplyPlasticity);
        ClearOutputsCommand = new RelayCommand(ClearOutputs);
    }

    public ObservableCollection<OutputEventItem> OutputEvents { get; }

    public ObservableCollection<OutputVectorEventItem> VectorEvents { get; }

    public string BrainIdText
    {
        get => _brainIdText;
        set => SetProperty(ref _brainIdText, value);
    }

    public string InputIndexText
    {
        get => _inputIndexText;
        set => SetProperty(ref _inputIndexText, value);
    }

    public string InputValueText
    {
        get => _inputValueText;
        set => SetProperty(ref _inputValueText, value);
    }

    public string InputVectorText
    {
        get => _inputVectorText;
        set => SetProperty(ref _inputVectorText, value);
    }

    public string EnergyCreditText
    {
        get => _energyCreditText;
        set => SetProperty(ref _energyCreditText, value);
    }

    public string EnergyRateText
    {
        get => _energyRateText;
        set => SetProperty(ref _energyRateText, value);
    }

    public bool CostEnabled
    {
        get => _costEnabled;
        set
        {
            if (SetProperty(ref _costEnabled, value))
            {
                UpdateCostEnergyCombined();
            }
        }
    }

    public bool EnergyEnabled
    {
        get => _energyEnabled;
        set
        {
            if (SetProperty(ref _energyEnabled, value))
            {
                UpdateCostEnergyCombined();
            }
        }
    }

    public bool CostEnergyEnabled
    {
        get => _costEnergyEnabled;
        set
        {
            if (SetProperty(ref _costEnergyEnabled, value))
            {
                if (_costEnabled != value)
                {
                    _costEnabled = value;
                    OnPropertyChanged(nameof(CostEnabled));
                }

                if (_energyEnabled != value)
                {
                    _energyEnabled = value;
                    OnPropertyChanged(nameof(EnergyEnabled));
                }
            }
        }
    }

    public bool PlasticityEnabled
    {
        get => _plasticityEnabled;
        set => SetProperty(ref _plasticityEnabled, value);
    }

    public string PlasticityRateText
    {
        get => _plasticityRateText;
        set => SetProperty(ref _plasticityRateText, value);
    }

    public ObservableCollection<PlasticityModeOption> PlasticityModes { get; }

    public PlasticityModeOption SelectedPlasticityMode
    {
        get => _selectedPlasticityMode;
        set => SetProperty(ref _selectedPlasticityMode, value);
    }

    public string BrainInfoSummary
    {
        get => _brainInfoSummary;
        set => SetProperty(ref _brainInfoSummary, value);
    }

    public string ActiveBrainsSummary
    {
        get => _activeBrainsSummary;
        set => SetProperty(ref _activeBrainsSummary, value);
    }

    public AsyncRelayCommand RequestInfoCommand { get; }

    public RelayCommand SubscribeOutputsCommand { get; }

    public RelayCommand UnsubscribeOutputsCommand { get; }

    public RelayCommand SubscribeVectorCommand { get; }

    public RelayCommand UnsubscribeVectorCommand { get; }

    public RelayCommand SendInputCommand { get; }

    public RelayCommand SendVectorCommand { get; }

    public RelayCommand ApplyEnergyCreditCommand { get; }

    public RelayCommand ApplyEnergyRateCommand { get; }

    public RelayCommand ApplyCostEnergyCommand { get; }

    public RelayCommand ApplyPlasticityCommand { get; }

    public RelayCommand ClearOutputsCommand { get; }

    public void AddOutputEvent(OutputEventItem item)
    {
        if (_selectedBrainId is not null && !string.Equals(item.BrainId, _selectedBrainId.Value.ToString("D"), StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        _dispatcher.Post(() =>
        {
            OutputEvents.Insert(0, item);
            Trim(OutputEvents);
        });
    }

    public void AddVectorEvent(OutputVectorEventItem item)
    {
        if (_selectedBrainId is not null && !string.Equals(item.BrainId, _selectedBrainId.Value.ToString("D"), StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        _dispatcher.Post(() =>
        {
            VectorEvents.Insert(0, item);
            Trim(VectorEvents);
        });
    }

    public void SelectBrain(Guid? brainId)
    {
        if (_selectedBrainId == brainId)
        {
            return;
        }

        if (_selectedBrainId.HasValue)
        {
            _client.UnsubscribeOutputs(_selectedBrainId.Value, vector: false);
            _client.UnsubscribeOutputs(_selectedBrainId.Value, vector: true);
        }

        _selectedBrainId = brainId;
        BrainIdText = brainId?.ToString("D") ?? string.Empty;
        OutputEvents.Clear();
        VectorEvents.Clear();

        if (brainId.HasValue)
        {
            _client.SubscribeOutputs(brainId.Value, vector: false);
            _client.SubscribeOutputs(brainId.Value, vector: true);
            _ = _client.RequestBrainInfoAsync(brainId.Value, ApplyBrainInfo);
        }
        else
        {
            BrainInfoSummary = "No brain selected.";
        }
    }

    public void UpdateActiveBrains(IReadOnlyList<Guid> brains)
    {
        _activeBrains = brains.Distinct().ToList();
        ActiveBrainsSummary = _activeBrains.Count == 0
            ? "No active brains loaded."
            : $"Active brains: {_activeBrains.Count}";
    }

    private async Task RequestInfoAsync()
    {
        if (!TryGetBrainId(out var brainId))
        {
            BrainInfoSummary = "Invalid BrainId.";
            return;
        }

        await _client.RequestBrainInfoAsync(brainId, ApplyBrainInfo);
    }

    private void ApplyBrainInfo(BrainInfo? info)
    {
        if (info is null)
        {
            BrainInfoSummary = "Brain not found or IO unavailable.";
            return;
        }

        CostEnabled = info.CostEnabled;
        EnergyEnabled = info.EnergyEnabled;
        PlasticityEnabled = info.PlasticityEnabled;

        BrainInfoSummary = $"Inputs: {info.InputWidth} | Outputs: {info.OutputWidth} | Energy: {info.EnergyRemaining}";
    }

    private void Subscribe(bool vector)
    {
        if (!TryGetBrainId(out var brainId))
        {
            BrainInfoSummary = "Invalid BrainId.";
            return;
        }

        _client.SubscribeOutputs(brainId, vector);
    }

    private void Unsubscribe(bool vector)
    {
        if (!TryGetBrainId(out var brainId))
        {
            BrainInfoSummary = "Invalid BrainId.";
            return;
        }

        _client.UnsubscribeOutputs(brainId, vector);
    }

    private void SendInput()
    {
        if (!TryGetBrainId(out var brainId))
        {
            BrainInfoSummary = "Invalid BrainId.";
            return;
        }

        if (!uint.TryParse(InputIndexText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var index))
        {
            BrainInfoSummary = "Input index invalid.";
            return;
        }

        if (!float.TryParse(InputValueText, NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
        {
            BrainInfoSummary = "Input value invalid.";
            return;
        }

        _client.SendInput(brainId, index, value);
    }

    private void SendVector()
    {
        if (!TryGetBrainId(out var brainId))
        {
            BrainInfoSummary = "Invalid BrainId.";
            return;
        }

        var values = ParseVector(InputVectorText);
        if (values.Count == 0)
        {
            BrainInfoSummary = "Vector is empty.";
            return;
        }

        _client.SendInputVector(brainId, values);
    }

    private void ApplyEnergyCredit()
    {
        if (!long.TryParse(EnergyCreditText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var amount))
        {
            BrainInfoSummary = "Credit value invalid.";
            return;
        }
        ForEachTargetBrain(brainId => _client.SendEnergyCredit(brainId, amount));
    }

    private void ApplyEnergyRate()
    {
        if (!long.TryParse(EnergyRateText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var rate))
        {
            BrainInfoSummary = "Rate value invalid.";
            return;
        }
        ForEachTargetBrain(brainId => _client.SendEnergyRate(brainId, rate));
    }

    private void ApplyCostEnergy()
    {
        var enabled = CostEnergyEnabled;
        ForEachTargetBrain(brainId => _client.SetCostEnergy(brainId, enabled, enabled));
    }

    private void ApplyPlasticity()
    {
        if (!float.TryParse(PlasticityRateText, NumberStyles.Float, CultureInfo.InvariantCulture, out var rate))
        {
            BrainInfoSummary = "Plasticity rate invalid.";
            return;
        }

        var probabilistic = SelectedPlasticityMode?.Probabilistic ?? true;
        ForEachTargetBrain(brainId => _client.SetPlasticity(brainId, PlasticityEnabled, rate, probabilistic));
    }

    private void ClearOutputs()
    {
        OutputEvents.Clear();
        VectorEvents.Clear();
    }

    private void UpdateCostEnergyCombined()
    {
        var combined = _costEnabled && _energyEnabled;
        if (_costEnergyEnabled == combined)
        {
            return;
        }

        _costEnergyEnabled = combined;
        OnPropertyChanged(nameof(CostEnergyEnabled));
    }

    private bool TryGetBrainId(out Guid brainId)
    {
        if (Guid.TryParse(BrainIdText, out brainId))
        {
            return true;
        }

        brainId = Guid.Empty;
        return false;
    }

    private void ForEachTargetBrain(Action<Guid> action)
    {
        if (_activeBrains.Count > 0)
        {
            foreach (var brainId in _activeBrains)
            {
                action(brainId);
            }

            return;
        }

        if (!TryGetBrainId(out var fallbackBrainId))
        {
            BrainInfoSummary = "No active brains available.";
            return;
        }

        action(fallbackBrainId);
    }

    private static IReadOnlyList<float> ParseVector(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return Array.Empty<float>();
        }

        var parts = raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var values = new List<float>(parts.Length);
        foreach (var part in parts)
        {
            if (float.TryParse(part, NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
            {
                values.Add(value);
            }
        }

        return values;
    }

    private static void Trim<T>(ObservableCollection<T> collection)
    {
        while (collection.Count > MaxEvents)
        {
            collection.RemoveAt(collection.Count - 1);
        }
    }
}

public sealed record PlasticityModeOption(string Label, bool Probabilistic);
