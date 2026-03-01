using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Globalization;
using Nbn.Proto.Control;
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
    private string _lastSuggestedInputVector = string.Empty;
    private string _energyCreditText = "1000";
    private string _energyRateText = "0";
    private string _plasticityRateText = "0.001";
    private string _homeostasisBaseProbabilityText = "0.01";
    private string _homeostasisMinStepCodesText = "1";
    private string _homeostasisEnergyTargetScaleText = "1";
    private string _homeostasisEnergyProbabilityScaleText = "1";
    private bool _filterZeroOutputs = true;
    private bool _filterZeroVectorOutputs = true;
    private bool _pauseVectorUiUpdates;
    private bool _autoSendInputVectorEveryTick;
    private bool _costEnabled;
    private bool _energyEnabled;
    private bool _costEnergyEnabled;
    private bool _plasticityEnabled;
    private bool _homeostasisEnabled = true;
    private bool _homeostasisEnergyCouplingEnabled;
    private PlasticityModeOption _selectedPlasticityMode;
    private HomeostasisTargetModeOption _selectedHomeostasisTargetMode;
    private HomeostasisUpdateModeOption _selectedHomeostasisUpdateMode;
    private string _brainInfoSummary = "No brain selected.";
    private string _activeBrainsSummary = "No active brains loaded.";
    private string _lastOutputTickLabel = "-";
    private Guid _lastAutoVectorSendBrainId = Guid.Empty;
    private ulong _lastAutoVectorSendTickId;
    private bool _hasLastAutoVectorSendTick;
    private List<Guid> _activeBrains = new();
    private Guid? _selectedBrainId;
    private int _selectedBrainInputWidth = -1;

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
        HomeostasisTargetModes = new ObservableCollection<HomeostasisTargetModeOption>
        {
            new("Zero", HomeostasisTargetMode.HomeostasisTargetZero),
            new("Fixed (0)", HomeostasisTargetMode.HomeostasisTargetFixed)
        };
        _selectedHomeostasisTargetMode = HomeostasisTargetModes[0];
        HomeostasisUpdateModes = new ObservableCollection<HomeostasisUpdateModeOption>
        {
            new("Probabilistic Quantized Step", HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep)
        };
        _selectedHomeostasisUpdateMode = HomeostasisUpdateModes[0];

        RequestInfoCommand = new AsyncRelayCommand(RequestInfoAsync);
        SubscribeOutputsCommand = new RelayCommand(() => Subscribe(false));
        UnsubscribeOutputsCommand = new RelayCommand(() => Unsubscribe(false));
        SubscribeVectorCommand = new RelayCommand(() => Subscribe(true));
        UnsubscribeVectorCommand = new RelayCommand(() => Unsubscribe(true));
        SendInputCommand = new RelayCommand(SendInput);
        SendVectorCommand = new RelayCommand(SendVector, () => !AutoSendInputVectorEveryTick);
        ApplyEnergyCreditCommand = new RelayCommand(ApplyEnergyCredit);
        ApplyEnergyRateCommand = new RelayCommand(ApplyEnergyRate);
        ApplyCostEnergyCommand = new RelayCommand(ApplyCostEnergy);
        ApplyPlasticityCommand = new RelayCommand(ApplyPlasticity);
        ApplyHomeostasisCommand = new RelayCommand(ApplyHomeostasis);
        ClearOutputsCommand = new RelayCommand(ClearOutputs);
        ClearVectorOutputsCommand = new RelayCommand(ClearVectorOutputs);
        ToggleVectorUiUpdatesCommand = new RelayCommand(ToggleVectorUiUpdates);
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

    public bool HomeostasisEnabled
    {
        get => _homeostasisEnabled;
        set => SetProperty(ref _homeostasisEnabled, value);
    }

    public bool HomeostasisEnergyCouplingEnabled
    {
        get => _homeostasisEnergyCouplingEnabled;
        set => SetProperty(ref _homeostasisEnergyCouplingEnabled, value);
    }

    public bool FilterZeroOutputs
    {
        get => _filterZeroOutputs;
        set => SetProperty(ref _filterZeroOutputs, value);
    }

    public bool FilterZeroVectorOutputs
    {
        get => _filterZeroVectorOutputs;
        set => SetProperty(ref _filterZeroVectorOutputs, value);
    }

    public bool PauseVectorUiUpdates
    {
        get => _pauseVectorUiUpdates;
        private set
        {
            if (SetProperty(ref _pauseVectorUiUpdates, value))
            {
                OnPropertyChanged(nameof(VectorUiUpdatesButtonLabel));
                OnPropertyChanged(nameof(VectorUiUpdatesStatus));
            }
        }
    }

    public string VectorUiUpdatesButtonLabel => PauseVectorUiUpdates ? "Resume UI" : "Pause UI";

    public string VectorUiUpdatesStatus => PauseVectorUiUpdates
        ? "UI updates paused (events still received)."
        : "UI updates live.";

    public bool AutoSendInputVectorEveryTick
    {
        get => _autoSendInputVectorEveryTick;
        set
        {
            if (SetProperty(ref _autoSendInputVectorEveryTick, value))
            {
                if (!value)
                {
                    ResetAutoVectorSendTickGate();
                }

                SendVectorCommand.RaiseCanExecuteChanged();
            }
        }
    }

    public string PlasticityRateText
    {
        get => _plasticityRateText;
        set => SetProperty(ref _plasticityRateText, value);
    }

    public string HomeostasisBaseProbabilityText
    {
        get => _homeostasisBaseProbabilityText;
        set => SetProperty(ref _homeostasisBaseProbabilityText, value);
    }

    public string HomeostasisMinStepCodesText
    {
        get => _homeostasisMinStepCodesText;
        set => SetProperty(ref _homeostasisMinStepCodesText, value);
    }

    public string HomeostasisEnergyTargetScaleText
    {
        get => _homeostasisEnergyTargetScaleText;
        set => SetProperty(ref _homeostasisEnergyTargetScaleText, value);
    }

    public string HomeostasisEnergyProbabilityScaleText
    {
        get => _homeostasisEnergyProbabilityScaleText;
        set => SetProperty(ref _homeostasisEnergyProbabilityScaleText, value);
    }

    public ObservableCollection<PlasticityModeOption> PlasticityModes { get; }

    public PlasticityModeOption SelectedPlasticityMode
    {
        get => _selectedPlasticityMode;
        set => SetProperty(ref _selectedPlasticityMode, value);
    }

    public ObservableCollection<HomeostasisTargetModeOption> HomeostasisTargetModes { get; }

    public HomeostasisTargetModeOption SelectedHomeostasisTargetMode
    {
        get => _selectedHomeostasisTargetMode;
        set => SetProperty(ref _selectedHomeostasisTargetMode, value);
    }

    public ObservableCollection<HomeostasisUpdateModeOption> HomeostasisUpdateModes { get; }

    public HomeostasisUpdateModeOption SelectedHomeostasisUpdateMode
    {
        get => _selectedHomeostasisUpdateMode;
        set => SetProperty(ref _selectedHomeostasisUpdateMode, value);
    }

    public string BrainInfoSummary
    {
        get => _brainInfoSummary;
        set => SetProperty(ref _brainInfoSummary, value);
    }

    public string LastOutputTickLabel
    {
        get => _lastOutputTickLabel;
        set => SetProperty(ref _lastOutputTickLabel, value);
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

    public RelayCommand ApplyHomeostasisCommand { get; }

    public RelayCommand ClearOutputsCommand { get; }

    public RelayCommand ClearVectorOutputsCommand { get; }

    public RelayCommand ToggleVectorUiUpdatesCommand { get; }

    public void AddOutputEvent(OutputEventItem item)
    {
        if (_selectedBrainId is not null && !string.Equals(item.BrainId, _selectedBrainId.Value.ToString("D"), StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        _dispatcher.Post(() =>
        {
            LastOutputTickLabel = item.TickId.ToString();
            if (FilterZeroOutputs && item.IsZero)
            {
                return;
            }

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
            LastOutputTickLabel = item.TickId.ToString();
            if (PauseVectorUiUpdates)
            {
                return;
            }

            if (FilterZeroVectorOutputs && item.AllZero)
            {
                return;
            }

            VectorEvents.Insert(0, item);
            Trim(VectorEvents);
        });
    }

    public void ObserveTick(ulong tickId)
    {
        _dispatcher.Post(() =>
        {
            if (!_selectedBrainId.HasValue)
            {
                return;
            }

            TryAutoSendInputVectorForTick(_selectedBrainId.Value, tickId);
        });
    }

    public void SelectBrain(Guid? brainId, bool preserveOutputs = false)
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
        _selectedBrainInputWidth = -1;
        BrainIdText = brainId?.ToString("D") ?? string.Empty;
        ResetAutoVectorSendTickGate();
        if (!preserveOutputs)
        {
            OutputEvents.Clear();
            VectorEvents.Clear();
            LastOutputTickLabel = "-";
        }

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

    public void EnsureSelectedBrain(Guid brainId)
    {
        if (_selectedBrainId.HasValue && _selectedBrainId.Value == brainId)
        {
            RefreshSubscriptions();
            return;
        }

        SelectBrain(brainId, preserveOutputs: true);
    }

    public Task<BrainInfo?> RequestBrainInfoAsync(Guid brainId)
        => _client.RequestBrainInfoAsync(brainId);

    public Task<Nbn.Proto.ArtifactRef?> ExportBrainDefinitionReferenceAsync(Guid brainId, bool rebaseOverlays = false)
        => _client.ExportBrainDefinitionAsync(brainId, rebaseOverlays);

    public Task<Nbn.Proto.Control.SetTickRateOverrideAck?> SetTickRateOverrideAsync(float? targetTickHz)
        => _client.SetTickRateOverrideAsync(targetTickHz);

    public void UpdateActiveBrains(IReadOnlyList<Guid> brains)
    {
        _activeBrains = brains.Distinct().ToList();
        ActiveBrainsSummary = _activeBrains.Count == 0
            ? "No active brains loaded."
            : $"Active brains: {_activeBrains.Count}";
    }

    public void RefreshSubscriptions()
    {
        if (!_selectedBrainId.HasValue)
        {
            return;
        }

        _client.SubscribeOutputs(_selectedBrainId.Value, vector: false);
        _client.SubscribeOutputs(_selectedBrainId.Value, vector: true);
        _ = _client.RequestBrainInfoAsync(_selectedBrainId.Value, ApplyBrainInfo);
    }

    public void ApplyEnergyCreditSelected()
    {
        _ = ApplyEnergyCreditSelectedAsync();
    }

    public void ApplyEnergyRateSelected()
    {
        _ = ApplyEnergyRateSelectedAsync();
    }

    public void ApplyCostEnergySelected()
    {
        _ = ApplyCostEnergySelectedAsync();
    }

    public bool TrySendInputSelected(uint index, float value, out string status)
    {
        if (!TryGetSelectedBrain(out var brainId))
        {
            status = "No brain selected.";
            return false;
        }

        if (!float.IsFinite(value))
        {
            BrainInfoSummary = "Input value invalid.";
            status = "Input value invalid.";
            return false;
        }

        _client.SendInput(brainId, index, value);
        status = FormattableString.Invariant($"Input pulse queued: brain {brainId:D}, index {index}, value {value:0.###}.");
        BrainInfoSummary = status;
        return true;
    }

    public bool TrySendRuntimeNeuronPulseSelected(uint regionId, uint neuronId, float value, out string status)
    {
        if (!TryGetSelectedBrain(out var brainId))
        {
            status = "No brain selected.";
            return false;
        }

        if (!float.IsFinite(value))
        {
            BrainInfoSummary = "Pulse value invalid.";
            status = "Pulse value invalid.";
            return false;
        }

        _client.SendRuntimeNeuronPulse(brainId, regionId, neuronId, value);
        status = FormattableString.Invariant($"Runtime pulse queued: brain {brainId:D}, R{regionId}/N{neuronId}, value {value:0.###}.");
        BrainInfoSummary = status;
        return true;
    }

    public bool TrySetRuntimeNeuronStateSelected(
        uint regionId,
        uint neuronId,
        bool setBuffer,
        float bufferValue,
        bool setAccumulator,
        float accumulatorValue,
        out string status)
    {
        if (!TryGetSelectedBrain(out var brainId))
        {
            status = "No brain selected.";
            return false;
        }

        if (!setBuffer && !setAccumulator)
        {
            status = "Specify at least one runtime value.";
            BrainInfoSummary = status;
            return false;
        }

        if (setBuffer && !float.IsFinite(bufferValue))
        {
            status = "Buffer value invalid.";
            BrainInfoSummary = status;
            return false;
        }

        if (setAccumulator && !float.IsFinite(accumulatorValue))
        {
            status = "Accumulator value invalid.";
            BrainInfoSummary = status;
            return false;
        }

        _client.SendRuntimeNeuronStateWrite(
            brainId,
            regionId,
            neuronId,
            setBuffer,
            bufferValue,
            setAccumulator,
            accumulatorValue);

        var updates = new List<string>(2);
        if (setBuffer)
        {
            updates.Add(FormattableString.Invariant($"buffer={bufferValue:0.###}"));
        }

        if (setAccumulator)
        {
            updates.Add(FormattableString.Invariant($"accumulator={accumulatorValue:0.###}"));
        }

        status = FormattableString.Invariant(
            $"Runtime state queued: brain {brainId:D}, R{regionId}/N{neuronId}, {string.Join(", ", updates)}.");
        BrainInfoSummary = status;
        return true;
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
            _selectedBrainInputWidth = -1;
            BrainInfoSummary = "Brain not found or IO unavailable.";
            return;
        }

        var inputWidth = checked((int)Math.Max(0, info.InputWidth));
        _selectedBrainInputWidth = inputWidth;
        CostEnabled = info.CostEnabled;
        EnergyEnabled = info.EnergyEnabled;
        PlasticityEnabled = info.PlasticityEnabled;
        HomeostasisEnabled = info.HomeostasisEnabled;
        HomeostasisEnergyCouplingEnabled = info.HomeostasisEnergyCouplingEnabled;
        EnergyRateText = info.EnergyRateUnitsPerSecond.ToString(CultureInfo.InvariantCulture);
        PlasticityRateText = info.PlasticityRate.ToString("0.######", CultureInfo.InvariantCulture);
        HomeostasisBaseProbabilityText = info.HomeostasisBaseProbability.ToString("0.######", CultureInfo.InvariantCulture);
        HomeostasisMinStepCodesText = info.HomeostasisMinStepCodes.ToString(CultureInfo.InvariantCulture);
        HomeostasisEnergyTargetScaleText = info.HomeostasisEnergyTargetScale.ToString("0.######", CultureInfo.InvariantCulture);
        HomeostasisEnergyProbabilityScaleText = info.HomeostasisEnergyProbabilityScale.ToString("0.######", CultureInfo.InvariantCulture);

        var selectedMode = PlasticityModes.FirstOrDefault(mode => mode.Probabilistic == info.PlasticityProbabilisticUpdates);
        if (selectedMode is not null)
        {
            SelectedPlasticityMode = selectedMode;
        }

        var selectedTargetMode = HomeostasisTargetModes.FirstOrDefault(mode => mode.Mode == info.HomeostasisTargetMode);
        if (selectedTargetMode is not null)
        {
            SelectedHomeostasisTargetMode = selectedTargetMode;
        }

        var selectedUpdateMode = HomeostasisUpdateModes.FirstOrDefault(mode => mode.Mode == info.HomeostasisUpdateMode);
        if (selectedUpdateMode is not null)
        {
            SelectedHomeostasisUpdateMode = selectedUpdateMode;
        }

        var shouldRegenerateSuggestion = string.IsNullOrWhiteSpace(InputVectorText);
        if (!shouldRegenerateSuggestion
            && string.Equals(InputVectorText, _lastSuggestedInputVector, StringComparison.Ordinal))
        {
            if (!TryParseVector(InputVectorText, out var existingSuggestedValues, out _)
                || existingSuggestedValues.Count != inputWidth)
            {
                shouldRegenerateSuggestion = true;
            }
        }

        if (shouldRegenerateSuggestion)
        {
            var suggestedVector = BuildSuggestedVector(inputWidth);
            _lastSuggestedInputVector = suggestedVector;
            InputVectorText = suggestedVector;
        }

        var plasticityModeLabel = info.PlasticityProbabilisticUpdates ? "probabilistic" : "absolute";
        var homeostasisTargetLabel = SelectedHomeostasisTargetMode?.Label ?? info.HomeostasisTargetMode.ToString();
        var homeostasisUpdateLabel = SelectedHomeostasisUpdateMode?.Label ?? info.HomeostasisUpdateMode.ToString();
        BrainInfoSummary =
            $"Inputs: {info.InputWidth} | Outputs: {info.OutputWidth} | Energy: {info.EnergyRemaining} @ {info.EnergyRateUnitsPerSecond}/s | LastCost: {info.LastTickCost} | Plasticity: {(info.PlasticityEnabled ? "on" : "off")} ({plasticityModeLabel}, {info.PlasticityRate:0.######}) | Homeostasis: {(info.HomeostasisEnabled ? "on" : "off")} ({homeostasisTargetLabel}, {homeostasisUpdateLabel}, p={info.HomeostasisBaseProbability:0.######}, step={info.HomeostasisMinStepCodes}, coupling={(info.HomeostasisEnergyCouplingEnabled ? "on" : "off")})";
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

        if (!TryGetValidatedInputVector(brainId, updateSummaryOnFailure: true, out var values))
        {
            return;
        }

        _client.SendInputVector(brainId, values);
    }

    private void ApplyEnergyCredit()
    {
        _ = ApplyEnergyCreditAsync();
    }

    private void ApplyEnergyRate()
    {
        _ = ApplyEnergyRateAsync();
    }

    private void ApplyCostEnergy()
    {
        _ = ApplyCostEnergyAsync();
    }

    private void ApplyPlasticity()
    {
        _ = ApplyPlasticityAsync();
    }

    private void ApplyHomeostasis()
    {
        _ = ApplyHomeostasisAsync();
    }

    private void ClearOutputs()
    {
        OutputEvents.Clear();
        VectorEvents.Clear();
    }

    private void ClearVectorOutputs()
    {
        VectorEvents.Clear();
    }

    private void ToggleVectorUiUpdates()
    {
        PauseVectorUiUpdates = !PauseVectorUiUpdates;
    }

    private void TryAutoSendInputVectorForTick(Guid brainId, ulong tickId)
    {
        if (!AutoSendInputVectorEveryTick
            || !_selectedBrainId.HasValue
            || _selectedBrainId.Value != brainId)
        {
            return;
        }

        if (_hasLastAutoVectorSendTick
            && _lastAutoVectorSendBrainId == brainId
            && _lastAutoVectorSendTickId == tickId)
        {
            return;
        }

        if (!TryGetValidatedInputVector(brainId, updateSummaryOnFailure: true, out var values))
        {
            return;
        }

        _lastAutoVectorSendBrainId = brainId;
        _lastAutoVectorSendTickId = tickId;
        _hasLastAutoVectorSendTick = true;
        _client.SendInputVector(brainId, values);
    }

    private void ResetAutoVectorSendTickGate()
    {
        _lastAutoVectorSendBrainId = Guid.Empty;
        _lastAutoVectorSendTickId = 0;
        _hasLastAutoVectorSendTick = false;
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

    private bool TryGetSelectedBrain(out Guid brainId)
    {
        if (_selectedBrainId.HasValue)
        {
            brainId = _selectedBrainId.Value;
            return true;
        }

        BrainInfoSummary = "No brain selected.";
        brainId = Guid.Empty;
        return false;
    }

    private async Task ApplyEnergyCreditSelectedAsync()
    {
        if (!TryGetSelectedBrain(out var brainId))
        {
            return;
        }

        if (!long.TryParse(EnergyCreditText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var amount))
        {
            BrainInfoSummary = "Credit value invalid.";
            return;
        }

        var result = await _client.SendEnergyCreditAsync(brainId, amount).ConfigureAwait(false);
        ApplyCommandResultToSummary("Energy credit", new[] { result });
    }

    private async Task ApplyEnergyRateSelectedAsync()
    {
        if (!TryGetSelectedBrain(out var brainId))
        {
            return;
        }

        if (!long.TryParse(EnergyRateText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var rate))
        {
            BrainInfoSummary = "Rate value invalid.";
            return;
        }

        var result = await _client.SendEnergyRateAsync(brainId, rate).ConfigureAwait(false);
        ApplyCommandResultToSummary("Energy rate", new[] { result });
    }

    private async Task ApplyCostEnergySelectedAsync()
    {
        if (!TryGetSelectedBrain(out var brainId))
        {
            return;
        }

        var result = await _client.SetCostEnergyAsync(brainId, CostEnabled, EnergyEnabled).ConfigureAwait(false);
        ApplyCommandResultToSummary("Cost/Energy flags", new[] { result });
    }

    private async Task ApplyEnergyCreditAsync()
    {
        if (!long.TryParse(EnergyCreditText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var amount))
        {
            BrainInfoSummary = "Credit value invalid.";
            return;
        }

        if (!TryGetTargetBrains(out var targets))
        {
            return;
        }

        var results = await Task.WhenAll(targets.Select(brainId => _client.SendEnergyCreditAsync(brainId, amount))).ConfigureAwait(false);
        ApplyCommandResultToSummary("Energy credit", results);
    }

    private async Task ApplyEnergyRateAsync()
    {
        if (!long.TryParse(EnergyRateText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var rate))
        {
            BrainInfoSummary = "Rate value invalid.";
            return;
        }

        if (!TryGetTargetBrains(out var targets))
        {
            return;
        }

        var results = await Task.WhenAll(targets.Select(brainId => _client.SendEnergyRateAsync(brainId, rate))).ConfigureAwait(false);
        ApplyCommandResultToSummary("Energy rate", results);
    }

    private async Task ApplyCostEnergyAsync()
    {
        if (!TryGetTargetBrains(out var targets))
        {
            return;
        }

        var costEnabled = CostEnabled;
        var energyEnabled = EnergyEnabled;
        var results = await Task.WhenAll(targets.Select(brainId => _client.SetCostEnergyAsync(brainId, costEnabled, energyEnabled))).ConfigureAwait(false);
        ApplyCommandResultToSummary("Cost/Energy flags", results);
    }

    private async Task ApplyPlasticityAsync()
    {
        if (!TryParsePlasticityRate(out var rate))
        {
            BrainInfoSummary = "Plasticity rate invalid.";
            return;
        }

        if (!TryGetTargetBrains(out var targets))
        {
            return;
        }

        var probabilistic = SelectedPlasticityMode?.Probabilistic ?? true;
        var enabled = PlasticityEnabled;
        var results = await Task.WhenAll(targets.Select(brainId => _client.SetPlasticityAsync(brainId, enabled, rate, probabilistic))).ConfigureAwait(false);
        ApplyCommandResultToSummary("Plasticity", results);
    }

    private async Task ApplyHomeostasisAsync()
    {
        if (!TryParseHomeostasisBaseProbability(out var baseProbability))
        {
            BrainInfoSummary = "Homeostasis probability invalid.";
            return;
        }

        if (!TryParseHomeostasisMinStepCodes(out var minStepCodes))
        {
            BrainInfoSummary = "Homeostasis min-step-codes invalid.";
            return;
        }

        if (!TryParseHomeostasisScale(HomeostasisEnergyTargetScaleText, out var energyTargetScale))
        {
            BrainInfoSummary = "Homeostasis energy-target-scale invalid.";
            return;
        }

        if (!TryParseHomeostasisScale(HomeostasisEnergyProbabilityScaleText, out var energyProbabilityScale))
        {
            BrainInfoSummary = "Homeostasis energy-probability-scale invalid.";
            return;
        }

        if (!TryGetTargetBrains(out var targets))
        {
            return;
        }

        var enabled = HomeostasisEnabled;
        var targetMode = SelectedHomeostasisTargetMode?.Mode ?? HomeostasisTargetMode.HomeostasisTargetZero;
        var updateMode = SelectedHomeostasisUpdateMode?.Mode ?? HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep;
        var energyCouplingEnabled = HomeostasisEnergyCouplingEnabled;
        var results = await Task.WhenAll(targets.Select(brainId => _client.SetHomeostasisAsync(
            brainId,
            enabled,
            targetMode,
            updateMode,
            baseProbability,
            minStepCodes,
            energyCouplingEnabled,
            energyTargetScale,
            energyProbabilityScale))).ConfigureAwait(false);
        ApplyCommandResultToSummary("Homeostasis", results);
    }

    private bool TryGetTargetBrains(out IReadOnlyList<Guid> targets)
    {
        if (_activeBrains.Count > 0)
        {
            targets = _activeBrains;
            return true;
        }

        if (!TryGetBrainId(out var fallbackBrainId))
        {
            BrainInfoSummary = "No active brains available.";
            targets = Array.Empty<Guid>();
            return false;
        }

        targets = new[] { fallbackBrainId };
        return true;
    }

    private bool TryParsePlasticityRate(out float rate)
    {
        if (!float.TryParse(PlasticityRateText, NumberStyles.Float, CultureInfo.InvariantCulture, out rate))
        {
            return false;
        }

        return float.IsFinite(rate) && rate >= 0f;
    }

    private bool TryParseHomeostasisBaseProbability(out float probability)
    {
        if (!float.TryParse(HomeostasisBaseProbabilityText, NumberStyles.Float, CultureInfo.InvariantCulture, out probability))
        {
            return false;
        }

        return float.IsFinite(probability) && probability >= 0f && probability <= 1f;
    }

    private bool TryParseHomeostasisMinStepCodes(out uint minStepCodes)
    {
        if (!uint.TryParse(HomeostasisMinStepCodesText, NumberStyles.Integer, CultureInfo.InvariantCulture, out minStepCodes))
        {
            return false;
        }

        return minStepCodes > 0;
    }

    private static bool TryParseHomeostasisScale(string text, out float scale)
    {
        if (!float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out scale))
        {
            return false;
        }

        return float.IsFinite(scale) && scale >= 0f && scale <= 4f;
    }

    private void ApplyCommandResultToSummary(string operation, IReadOnlyList<IoCommandResult> results)
    {
        if (results.Count == 0)
        {
            return;
        }

        BrainEnergyState? selectedState = null;
        foreach (var result in results)
        {
            if (!result.Success || result.EnergyState is null)
            {
                continue;
            }

            if (_selectedBrainId.HasValue && result.BrainId == _selectedBrainId.Value)
            {
                selectedState = result.EnergyState;
            }
        }

        var successes = results.Where(result => result.Success).ToList();
        var failures = results.Where(result => !result.Success).ToList();
        string summary;
        if (failures.Count == 0)
        {
            summary = FormattableString.Invariant($"{operation}: applied to {successes.Count} brain(s).");
        }
        else
        {
            var failedIds = string.Join(", ", failures.Select(result => FormattableString.Invariant($"{result.BrainId:D} ({result.Message})")));
            if (successes.Count == 0)
            {
                summary = FormattableString.Invariant($"{operation}: failed for {failures.Count} brain(s): {failedIds}");
            }
            else
            {
                summary = FormattableString.Invariant($"{operation}: {successes.Count}/{results.Count} succeeded. Failed: {failedIds}");
            }
        }

        _dispatcher.Post(() =>
        {
            if (selectedState is not null)
            {
                CostEnabled = selectedState.CostEnabled;
                EnergyEnabled = selectedState.EnergyEnabled;
                PlasticityEnabled = selectedState.PlasticityEnabled;
                HomeostasisEnabled = selectedState.HomeostasisEnabled;
                HomeostasisEnergyCouplingEnabled = selectedState.HomeostasisEnergyCouplingEnabled;
                EnergyRateText = selectedState.EnergyRateUnitsPerSecond.ToString(CultureInfo.InvariantCulture);
                PlasticityRateText = selectedState.PlasticityRate.ToString("0.######", CultureInfo.InvariantCulture);
                HomeostasisBaseProbabilityText = selectedState.HomeostasisBaseProbability.ToString("0.######", CultureInfo.InvariantCulture);
                HomeostasisMinStepCodesText = selectedState.HomeostasisMinStepCodes.ToString(CultureInfo.InvariantCulture);
                HomeostasisEnergyTargetScaleText = selectedState.HomeostasisEnergyTargetScale.ToString("0.######", CultureInfo.InvariantCulture);
                HomeostasisEnergyProbabilityScaleText = selectedState.HomeostasisEnergyProbabilityScale.ToString("0.######", CultureInfo.InvariantCulture);

                var selectedMode = PlasticityModes.FirstOrDefault(mode => mode.Probabilistic == selectedState.PlasticityProbabilisticUpdates);
                if (selectedMode is not null)
                {
                    SelectedPlasticityMode = selectedMode;
                }

                var targetMode = HomeostasisTargetModes.FirstOrDefault(mode => mode.Mode == selectedState.HomeostasisTargetMode);
                if (targetMode is not null)
                {
                    SelectedHomeostasisTargetMode = targetMode;
                }

                var updateMode = HomeostasisUpdateModes.FirstOrDefault(mode => mode.Mode == selectedState.HomeostasisUpdateMode);
                if (updateMode is not null)
                {
                    SelectedHomeostasisUpdateMode = updateMode;
                }
            }

            BrainInfoSummary = summary;
        });
    }

    private bool TryGetValidatedInputVector(Guid brainId, bool updateSummaryOnFailure, out IReadOnlyList<float> values)
    {
        if (!TryParseVector(InputVectorText, out values, out var parseError))
        {
            if (updateSummaryOnFailure)
            {
                BrainInfoSummary = parseError;
            }

            return false;
        }

        if (!TryValidateInputVectorWidth(brainId, values.Count, out var widthError))
        {
            if (updateSummaryOnFailure)
            {
                BrainInfoSummary = widthError;
            }

            return false;
        }

        return true;
    }

    private bool TryValidateInputVectorWidth(Guid brainId, int width, out string error)
    {
        if (!_selectedBrainId.HasValue
            || _selectedBrainId.Value != brainId
            || _selectedBrainInputWidth < 0
            || _selectedBrainInputWidth == width)
        {
            error = string.Empty;
            return true;
        }

        error = FormattableString.Invariant(
            $"Input vector width mismatch for brain {brainId:D}: expected {_selectedBrainInputWidth}, got {width}.");
        return false;
    }

    private static bool TryParseVector(string raw, out IReadOnlyList<float> values, out string error)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            values = Array.Empty<float>();
            error = "Vector is empty.";
            return false;
        }

        var parts = raw.Split(',', StringSplitOptions.TrimEntries);
        var parsedValues = new List<float>(parts.Length);
        for (var i = 0; i < parts.Length; i++)
        {
            var part = parts[i];
            if (string.IsNullOrWhiteSpace(part))
            {
                values = Array.Empty<float>();
                error = FormattableString.Invariant($"Vector value #{i + 1} is empty.");
                return false;
            }

            if (!float.TryParse(part, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) || !float.IsFinite(value))
            {
                values = Array.Empty<float>();
                error = FormattableString.Invariant($"Vector value #{i + 1} is invalid.");
                return false;
            }

            parsedValues.Add(value);
        }

        if (parsedValues.Count == 0)
        {
            values = Array.Empty<float>();
            error = "Vector is empty.";
            return false;
        }

        values = parsedValues;
        error = string.Empty;
        return true;
    }

    private static void Trim<T>(ObservableCollection<T> collection)
    {
        while (collection.Count > MaxEvents)
        {
            collection.RemoveAt(collection.Count - 1);
        }
    }

    private static string BuildSuggestedVector(int inputWidth)
    {
        if (inputWidth <= 0)
        {
            return string.Empty;
        }

        var count = inputWidth;
        const double minMagnitude = 0.15d;
        var values = new string[count];
        for (var i = 0; i < count; i++)
        {
            var magnitude = minMagnitude + ((1d - minMagnitude) * Math.Sqrt(Random.Shared.NextDouble()));
            var sign = Random.Shared.Next(0, 2) == 0 ? -1d : 1d;
            var value = sign * magnitude;
            values[i] = value.ToString("0.##", CultureInfo.InvariantCulture);
        }

        return string.Join(",", values);
    }
}

public sealed record PlasticityModeOption(string Label, bool Probabilistic);
public sealed record HomeostasisTargetModeOption(string Label, HomeostasisTargetMode Mode);
public sealed record HomeostasisUpdateModeOption(string Label, HomeostasisUpdateMode Mode);

