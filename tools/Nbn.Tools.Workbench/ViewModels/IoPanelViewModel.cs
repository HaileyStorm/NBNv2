using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Globalization;
using System.Threading;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Shared;
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
    private bool _systemCostEnergyEnabled;
    private bool _systemCostEnergyEnabledDraft;
    private bool _systemSettingsSyncInProgress;
    private bool _plasticityEnabled = true;
    private bool _systemPlasticityEnabled = true;
    private bool _systemPlasticityEnabledDraft = true;
    private string _systemPlasticityRateText = "0.001";
    private string _systemPlasticityRateTextDraft = "0.001";
    private bool _systemPlasticityProbabilisticUpdates = true;
    private bool _systemPlasticityProbabilisticUpdatesDraft = true;
    private bool _homeostasisEnabled = true;
    private bool _homeostasisEnergyCouplingEnabled;
    private PlasticityModeOption _selectedPlasticityMode;
    private PlasticityModeOption _selectedSystemPlasticityModeDraft;
    private InputCoordinatorModeOption _selectedInputCoordinatorMode = null!;
    private InputCoordinatorModeOption _selectedInputCoordinatorModeDraft = null!;
    private OutputVectorSourceOption _selectedOutputVectorSource = null!;
    private OutputVectorSourceOption _selectedOutputVectorSourceDraft = null!;
    private HomeostasisTargetModeOption _selectedHomeostasisTargetMode;
    private HomeostasisUpdateModeOption _selectedHomeostasisUpdateMode;
    private string _brainInfoSummary = "No brain selected.";
    private string _activeBrainsSummary = "No active brains loaded.";
    private string _feedbackBrainSummary = "No known brain selected.";
    private string _lastOutputTickLabel = "-";
    private bool _selectedBrainInputReplayEveryTick;
    private Guid _lastAutoVectorSendBrainId = Guid.Empty;
    private ulong _lastAutoVectorSendTickId;
    private bool _hasLastAutoVectorSendTick;
    private bool _systemInputReplayEveryTick;
    private List<Guid> _activeBrains = new();
    private readonly Dictionary<Guid, KnownBrainOption> _knownBrainsById = new();
    private KnownBrainOption? _selectedFeedbackBrain;
    private Guid? _selectedBrainId;
    private int _selectedBrainInputWidth = -1;
    private readonly SemaphoreSlim _selectedBrainCommandGate = new(1, 1);

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
        _selectedSystemPlasticityModeDraft = PlasticityModes[0];
        InputCoordinatorModes = new ObservableCollection<InputCoordinatorModeOption>
        {
            new("Dirty on change", InputCoordinatorMode.DirtyOnChange, "dirty_on_change"),
            new("Replay latest vector every tick", InputCoordinatorMode.ReplayLatestVector, "replay_latest_vector")
        };
        _selectedInputCoordinatorMode = InputCoordinatorModes[0];
        _selectedInputCoordinatorModeDraft = InputCoordinatorModes[0];
        OutputVectorSources = new ObservableCollection<OutputVectorSourceOption>
        {
            new("Potential (activation)", OutputVectorSource.Potential, "potential"),
            new("Buffer (persistent)", OutputVectorSource.Buffer, "buffer")
        };
        _selectedOutputVectorSource = OutputVectorSources[0];
        _selectedOutputVectorSourceDraft = OutputVectorSources[0];
        KnownBrains = new ObservableCollection<KnownBrainOption>();
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
        ApplySystemPlasticityModeRateCommand = new RelayCommand(ApplySystemPlasticityModeRate);
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

                OnPropertyChanged(nameof(CostEnergySuppressed));
            }
        }
    }

    public bool CostEnergySuppressed
    {
        get => !CostEnergyEnabled;
        set
        {
            var enabled = !value;
            if (CostEnergyEnabled == enabled)
            {
                return;
            }

            CostEnergyEnabled = enabled;
            ApplyCostEnergySelected();
        }
    }

    public bool SystemCostEnergyEnabled
    {
        get => _systemCostEnergyEnabled;
        private set
        {
            if (SetProperty(ref _systemCostEnergyEnabled, value))
            {
                OnPropertyChanged(nameof(CostEnergyOverrideAvailable));
                OnPropertyChanged(nameof(CostEnergyOverrideUnavailable));
                OnPropertyChanged(nameof(SystemCostEnergyStateLabel));
            }
        }
    }

    public bool SystemCostEnergyEnabledDraft
    {
        get => _systemCostEnergyEnabledDraft;
        set
        {
            if (SetProperty(ref _systemCostEnergyEnabledDraft, value)
                && !_systemSettingsSyncInProgress)
            {
                _ = ApplyCostEnergyAsync();
            }
        }
    }

    public bool CostEnergyOverrideAvailable => SystemCostEnergyEnabled;
    public bool CostEnergyOverrideUnavailable => !CostEnergyOverrideAvailable;

    public string SystemCostEnergyStateLabel => SystemCostEnergyEnabled
        ? "System policy: enabled"
        : "System policy: disabled (override unavailable)";

    public bool PlasticityEnabled
    {
        get => _plasticityEnabled;
        set
        {
            if (SetProperty(ref _plasticityEnabled, value))
            {
                OnPropertyChanged(nameof(PlasticitySuppressed));
            }
        }
    }

    public bool PlasticitySuppressed
    {
        get => !PlasticityEnabled;
        set
        {
            var enabled = !value;
            if (PlasticityEnabled == enabled)
            {
                return;
            }

            PlasticityEnabled = enabled;
            ApplyPlasticitySelected();
        }
    }

    public bool SystemPlasticityEnabled
    {
        get => _systemPlasticityEnabled;
        private set
        {
            if (SetProperty(ref _systemPlasticityEnabled, value))
            {
                OnPropertyChanged(nameof(PlasticityOverrideAvailable));
                OnPropertyChanged(nameof(PlasticityOverrideUnavailable));
                OnPropertyChanged(nameof(SystemPlasticityStateLabel));
            }
        }
    }

    public bool SystemPlasticityEnabledDraft
    {
        get => _systemPlasticityEnabledDraft;
        set
        {
            if (SetProperty(ref _systemPlasticityEnabledDraft, value)
                && !_systemSettingsSyncInProgress)
            {
                _ = ApplyPlasticityAsync();
            }
        }
    }

    public bool PlasticityOverrideAvailable => SystemPlasticityEnabled;
    public bool PlasticityOverrideUnavailable => !PlasticityOverrideAvailable;

    public string SystemPlasticityStateLabel => SystemPlasticityEnabled
        ? "System policy: enabled"
        : "System policy: disabled (override unavailable)";

    public string SystemPlasticityRateText
    {
        get => _systemPlasticityRateText;
        private set => SetProperty(ref _systemPlasticityRateText, value);
    }

    public string SystemPlasticityRateTextDraft
    {
        get => _systemPlasticityRateTextDraft;
        set => SetProperty(ref _systemPlasticityRateTextDraft, value);
    }

    public bool SystemPlasticityProbabilisticUpdates
    {
        get => _systemPlasticityProbabilisticUpdates;
        private set => SetProperty(ref _systemPlasticityProbabilisticUpdates, value);
    }

    public bool SystemPlasticityProbabilisticUpdatesDraft
    {
        get => _systemPlasticityProbabilisticUpdatesDraft;
        set
        {
            if (SetProperty(ref _systemPlasticityProbabilisticUpdatesDraft, value))
            {
                var selectedMode = PlasticityModes.FirstOrDefault(mode => mode.Probabilistic == value);
                if (selectedMode is not null && !ReferenceEquals(SelectedSystemPlasticityModeDraft, selectedMode))
                {
                    _selectedSystemPlasticityModeDraft = selectedMode;
                    OnPropertyChanged(nameof(SelectedSystemPlasticityModeDraft));
                }
            }
        }
    }

    public PlasticityModeOption SelectedSystemPlasticityModeDraft
    {
        get => _selectedSystemPlasticityModeDraft;
        set
        {
            if (SetProperty(ref _selectedSystemPlasticityModeDraft, value) && value is not null)
            {
                SystemPlasticityProbabilisticUpdatesDraft = value.Probabilistic;
            }
        }
    }

    public ObservableCollection<InputCoordinatorModeOption> InputCoordinatorModes { get; }

    public InputCoordinatorModeOption SelectedInputCoordinatorMode
    {
        get => _selectedInputCoordinatorMode;
        private set
        {
            var normalized = value ?? InputCoordinatorModes[0];
            if (SetProperty(ref _selectedInputCoordinatorMode, normalized))
            {
                _systemInputReplayEveryTick = normalized.Mode == InputCoordinatorMode.ReplayLatestVector;
                OnPropertyChanged(nameof(SystemInputCoordinatorModeStateLabel));
                OnPropertyChanged(nameof(AutoSendInputVectorEveryTickAvailable));
                if (_systemInputReplayEveryTick && AutoSendInputVectorEveryTick)
                {
                    AutoSendInputVectorEveryTick = false;
                }
            }
        }
    }

    public InputCoordinatorModeOption SelectedInputCoordinatorModeDraft
    {
        get => _selectedInputCoordinatorModeDraft;
        set
        {
            var normalized = value ?? InputCoordinatorModes[0];
            if (SetProperty(ref _selectedInputCoordinatorModeDraft, normalized)
                && !_systemSettingsSyncInProgress)
            {
                _ = ApplyInputCoordinatorModeAsync();
            }
        }
    }

    public string SystemInputCoordinatorModeStateLabel =>
        $"System policy: {FormatInputCoordinatorMode(SelectedInputCoordinatorMode.Mode)}";

    public ObservableCollection<OutputVectorSourceOption> OutputVectorSources { get; }

    public OutputVectorSourceOption SelectedOutputVectorSource
    {
        get => _selectedOutputVectorSource;
        private set
        {
            var normalized = value ?? OutputVectorSources[0];
            if (SetProperty(ref _selectedOutputVectorSource, normalized))
            {
                OnPropertyChanged(nameof(SystemOutputVectorSourceStateLabel));
            }
        }
    }

    public OutputVectorSourceOption SelectedOutputVectorSourceDraft
    {
        get => _selectedOutputVectorSourceDraft;
        set
        {
            var normalized = value ?? OutputVectorSources[0];
            if (SetProperty(ref _selectedOutputVectorSourceDraft, normalized)
                && !_systemSettingsSyncInProgress)
            {
                _ = ApplyOutputVectorSourceAsync();
            }
        }
    }

    public string SystemOutputVectorSourceStateLabel =>
        $"System policy: {FormatOutputVectorSource(SelectedOutputVectorSource.Source)}";

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
            var normalized = value && !AutoSendInputVectorEveryTickAvailable
                ? false
                : value;
            if (SetProperty(ref _autoSendInputVectorEveryTick, normalized))
            {
                if (!normalized)
                {
                    ResetAutoVectorSendTickGate();
                }

                SendVectorCommand.RaiseCanExecuteChanged();
            }
        }
    }

    public bool AutoSendInputVectorEveryTickAvailable => !_selectedBrainInputReplayEveryTick && !_systemInputReplayEveryTick;

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

    public ObservableCollection<KnownBrainOption> KnownBrains { get; }

    public KnownBrainOption? SelectedFeedbackBrain
    {
        get => _selectedFeedbackBrain;
        set
        {
            if (SetProperty(ref _selectedFeedbackBrain, value))
            {
                _ = RefreshFeedbackBrainSummaryAsync();
            }
        }
    }

    public string FeedbackBrainSummary
    {
        get => _feedbackBrainSummary;
        set => SetProperty(ref _feedbackBrainSummary, value);
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

    public RelayCommand ApplySystemPlasticityModeRateCommand { get; }

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

    public void ObserveTick(Guid brainId, ulong tickId)
    {
        _dispatcher.Post(() =>
        {
            if (!_selectedBrainId.HasValue || _selectedBrainId.Value != brainId)
            {
                return;
            }

            TryAutoSendInputVectorForTick(brainId, tickId);
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
        _selectedBrainInputReplayEveryTick = false;
        OnPropertyChanged(nameof(AutoSendInputVectorEveryTickAvailable));
        BrainIdText = brainId?.ToString("D") ?? string.Empty;
        TrackKnownBrain(brainId);
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

    public Task<SettingValue?> SetSettingAsync(string key, string value)
        => _client.SetSettingAsync(key, value);

    public void UpdateActiveBrains(IReadOnlyList<Guid> brains)
    {
        _activeBrains = brains.Distinct().ToList();
        ActiveBrainsSummary = _activeBrains.Count == 0
            ? "No active brains loaded."
            : $"Active brains: {_activeBrains.Count}";

        var activeSet = new HashSet<Guid>(_activeBrains);
        foreach (var option in _knownBrainsById.Values)
        {
            option.IsActive = activeSet.Contains(option.BrainId);
        }

        foreach (var brainId in _activeBrains)
        {
            if (_knownBrainsById.ContainsKey(brainId))
            {
                continue;
            }

            var option = new KnownBrainOption(brainId, isActive: true);
            _knownBrainsById[brainId] = option;
            KnownBrains.Add(option);
        }

        if (SelectedFeedbackBrain is null && KnownBrains.Count > 0)
        {
            SelectedFeedbackBrain = KnownBrains[0];
        }
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
        QueueSelectedBrainCommand(ApplyEnergyCreditSelectedAsync);
    }

    public void ApplyEnergyRateSelected()
    {
        QueueSelectedBrainCommand(ApplyEnergyRateSelectedAsync);
    }

    public void ApplyCostEnergySelected()
    {
        QueueSelectedBrainCommand(ApplyCostEnergySelectedAsync);
    }

    public void ApplyPlasticitySelected()
    {
        QueueSelectedBrainCommand(ApplyPlasticitySelectedAsync);
    }

    public bool ApplySetting(SettingItem item)
    {
        if (item is null || string.IsNullOrWhiteSpace(item.Key))
        {
            return false;
        }

        if (string.Equals(item.Key, CostEnergySettingsKeys.SystemEnabledKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseBooleanSetting(item.Value, SystemCostEnergyEnabled);
            _systemSettingsSyncInProgress = true;
            try
            {
                SystemCostEnergyEnabled = parsed;
                SystemCostEnergyEnabledDraft = parsed;
            }
            finally
            {
                _systemSettingsSyncInProgress = false;
            }
            return true;
        }

        if (string.Equals(item.Key, PlasticitySettingsKeys.SystemEnabledKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseBooleanSetting(item.Value, SystemPlasticityEnabled);
            _systemSettingsSyncInProgress = true;
            try
            {
                SystemPlasticityEnabled = parsed;
                SystemPlasticityEnabledDraft = parsed;
            }
            finally
            {
                _systemSettingsSyncInProgress = false;
            }
            return true;
        }

        if (string.Equals(item.Key, PlasticitySettingsKeys.SystemRateKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseSystemPlasticityRateText(item.Value, fallback: SystemPlasticityRateText);
            SystemPlasticityRateText = parsed;
            _systemSettingsSyncInProgress = true;
            try
            {
                SystemPlasticityRateTextDraft = parsed;
            }
            finally
            {
                _systemSettingsSyncInProgress = false;
            }
            return true;
        }

        if (string.Equals(item.Key, PlasticitySettingsKeys.SystemProbabilisticUpdatesKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseBooleanSetting(item.Value, SystemPlasticityProbabilisticUpdates);
            SystemPlasticityProbabilisticUpdates = parsed;
            var selectedMode = PlasticityModes.FirstOrDefault(mode => mode.Probabilistic == parsed) ?? PlasticityModes[0];
            _systemSettingsSyncInProgress = true;
            try
            {
                SystemPlasticityProbabilisticUpdatesDraft = parsed;
                SelectedSystemPlasticityModeDraft = selectedMode;
            }
            finally
            {
                _systemSettingsSyncInProgress = false;
            }
            return true;
        }

        if (string.Equals(item.Key, IoCoordinatorSettingsKeys.InputCoordinatorModeKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseInputCoordinatorModeSetting(item.Value, SelectedInputCoordinatorMode.Mode);
            var selectedMode = InputCoordinatorModes.FirstOrDefault(option => option.Mode == parsed) ?? InputCoordinatorModes[0];
            _systemSettingsSyncInProgress = true;
            try
            {
                SelectedInputCoordinatorMode = selectedMode;
                SelectedInputCoordinatorModeDraft = selectedMode;
            }
            finally
            {
                _systemSettingsSyncInProgress = false;
            }
            return true;
        }

        if (string.Equals(item.Key, IoCoordinatorSettingsKeys.OutputVectorSourceKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseOutputVectorSourceSetting(item.Value, SelectedOutputVectorSource.Source);
            var selectedSource = OutputVectorSources.FirstOrDefault(option => option.Source == parsed) ?? OutputVectorSources[0];
            _systemSettingsSyncInProgress = true;
            try
            {
                SelectedOutputVectorSource = selectedSource;
                SelectedOutputVectorSourceDraft = selectedSource;
            }
            finally
            {
                _systemSettingsSyncInProgress = false;
            }
            return true;
        }

        return false;
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
            _selectedBrainInputReplayEveryTick = false;
            OnPropertyChanged(nameof(AutoSendInputVectorEveryTickAvailable));
            BrainInfoSummary = "Brain not found or IO unavailable.";
            return;
        }

        var inputWidth = checked((int)Math.Max(0, info.InputWidth));
        _selectedBrainInputWidth = inputWidth;
        _selectedBrainInputReplayEveryTick =
            info.InputCoordinatorMode == InputCoordinatorMode.ReplayLatestVector;
        OnPropertyChanged(nameof(AutoSendInputVectorEveryTickAvailable));
        if (_selectedBrainInputReplayEveryTick && AutoSendInputVectorEveryTick)
        {
            AutoSendInputVectorEveryTick = false;
        }
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

        BrainInfoSummary = BuildBrainSummary(info);
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

    private void ApplySystemPlasticityModeRate()
    {
        _ = ApplySystemPlasticityModeRateAsync();
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
            || !AutoSendInputVectorEveryTickAvailable
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
        OnPropertyChanged(nameof(CostEnergySuppressed));
    }

    private void QueueSelectedBrainCommand(Func<Task> command)
    {
        _ = RunSelectedBrainCommandAsync(command);
    }

    private async Task RunSelectedBrainCommandAsync(Func<Task> command)
    {
        await _selectedBrainCommandGate.WaitAsync().ConfigureAwait(false);
        try
        {
            await command().ConfigureAwait(false);
        }
        finally
        {
            _selectedBrainCommandGate.Release();
        }
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

        var enabled = CostEnergyEnabled;
        var result = await _client.SetCostEnergyAsync(brainId, enabled, enabled).ConfigureAwait(false);
        ApplyCommandResultToSummary("Cost/Energy flags", new[] { result });
    }

    private async Task ApplyPlasticitySelectedAsync()
    {
        if (!TryGetSelectedBrain(out var brainId))
        {
            return;
        }

        if (!TryParsePlasticityRate(out var rate))
        {
            BrainInfoSummary = "Plasticity rate invalid.";
            return;
        }

        var probabilistic = SelectedPlasticityMode?.Probabilistic ?? true;
        var enabled = PlasticityEnabled;
        var result = await _client.SetPlasticityAsync(brainId, enabled, rate, probabilistic).ConfigureAwait(false);
        ApplyCommandResultToSummary("Plasticity", new[] { result });
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
        var enabled = SystemCostEnergyEnabledDraft;
        var result = await _client.SetSettingAsync(CostEnergySettingsKeys.SystemEnabledKey, enabled ? "true" : "false").ConfigureAwait(false);
        ApplySystemBooleanSettingToSummary("Cost/Energy system policy", CostEnergySettingsKeys.SystemEnabledKey, enabled, result);
    }

    private async Task ApplyPlasticityAsync()
    {
        var enabled = SystemPlasticityEnabledDraft;
        var result = await _client.SetSettingAsync(PlasticitySettingsKeys.SystemEnabledKey, enabled ? "true" : "false").ConfigureAwait(false);
        ApplySystemBooleanSettingToSummary("Plasticity system policy", PlasticitySettingsKeys.SystemEnabledKey, enabled, result);
    }

    private async Task ApplyInputCoordinatorModeAsync()
    {
        var selected = SelectedInputCoordinatorModeDraft ?? InputCoordinatorModes[0];
        var result = await _client.SetSettingAsync(
                IoCoordinatorSettingsKeys.InputCoordinatorModeKey,
                selected.SettingValue)
            .ConfigureAwait(false);
        _dispatcher.Post(() =>
        {
            if (result is null)
            {
                BrainInfoSummary = "Input coordinator mode: update failed (settings unavailable).";
                return;
            }

            if (!string.Equals(result.Key, IoCoordinatorSettingsKeys.InputCoordinatorModeKey, StringComparison.OrdinalIgnoreCase))
            {
                BrainInfoSummary = $"Input coordinator mode: update returned unexpected key '{result.Key}'.";
                return;
            }

            ApplySetting(new SettingItem(result.Key, result.Value, result.UpdatedMs.ToString(CultureInfo.InvariantCulture)));
            var persisted = ParseInputCoordinatorModeSetting(result.Value, selected.Mode);
            BrainInfoSummary = $"Input coordinator mode: applied ({FormatInputCoordinatorMode(persisted)}).";
            if (_selectedBrainId.HasValue)
            {
                _ = _client.RequestBrainInfoAsync(_selectedBrainId.Value, ApplyBrainInfo);
            }
        });
    }

    private async Task ApplyOutputVectorSourceAsync()
    {
        var selected = SelectedOutputVectorSourceDraft ?? OutputVectorSources[0];
        var result = await _client.SetSettingAsync(
                IoCoordinatorSettingsKeys.OutputVectorSourceKey,
                selected.SettingValue)
            .ConfigureAwait(false);
        _dispatcher.Post(() =>
        {
            if (result is null)
            {
                BrainInfoSummary = "Output vector source: update failed (settings unavailable).";
                return;
            }

            if (!string.Equals(result.Key, IoCoordinatorSettingsKeys.OutputVectorSourceKey, StringComparison.OrdinalIgnoreCase))
            {
                BrainInfoSummary = $"Output vector source: update returned unexpected key '{result.Key}'.";
                return;
            }

            ApplySetting(new SettingItem(result.Key, result.Value, result.UpdatedMs.ToString(CultureInfo.InvariantCulture)));
            var persisted = ParseOutputVectorSourceSetting(result.Value, selected.Source);
            BrainInfoSummary = $"Output vector source: applied ({FormatOutputVectorSource(persisted)}).";
            if (_selectedBrainId.HasValue)
            {
                _ = _client.RequestBrainInfoAsync(_selectedBrainId.Value, ApplyBrainInfo);
            }
        });
    }

    private async Task ApplySystemPlasticityModeRateAsync()
    {
        if (!TryParseSystemPlasticityRateDraft(out var rate, out var rateText))
        {
            BrainInfoSummary = "Plasticity system rate invalid.";
            return;
        }

        var probabilistic = SelectedSystemPlasticityModeDraft?.Probabilistic ?? SystemPlasticityProbabilisticUpdatesDraft;
        var modeValue = probabilistic ? "true" : "false";
        var rateResult = await _client.SetSettingAsync(PlasticitySettingsKeys.SystemRateKey, rateText).ConfigureAwait(false);
        var modeResult = await _client.SetSettingAsync(PlasticitySettingsKeys.SystemProbabilisticUpdatesKey, modeValue).ConfigureAwait(false);
        var runtimeApplied = 0;
        var runtimeSkipped = 0;
        var runtimeFailed = 0;

        if (TryGetTargetBrains(out var targets) && targets.Count > 0)
        {
            foreach (var brainId in targets)
            {
                var info = await _client.RequestBrainInfoAsync(brainId).ConfigureAwait(false);
                if (info is null)
                {
                    runtimeSkipped++;
                    continue;
                }

                var result = await _client.SetPlasticityAsync(
                        brainId,
                        info.PlasticityEnabled,
                        rate,
                        probabilistic)
                    .ConfigureAwait(false);
                if (result.Success)
                {
                    runtimeApplied++;
                }
                else
                {
                    runtimeFailed++;
                }
            }
        }

        _dispatcher.Post(() =>
        {
            if (rateResult is null || modeResult is null)
            {
                BrainInfoSummary = "Plasticity system mode/rate: update failed (settings unavailable).";
                return;
            }

            if (!string.Equals(rateResult.Key, PlasticitySettingsKeys.SystemRateKey, StringComparison.OrdinalIgnoreCase)
                || !string.Equals(modeResult.Key, PlasticitySettingsKeys.SystemProbabilisticUpdatesKey, StringComparison.OrdinalIgnoreCase))
            {
                BrainInfoSummary = "Plasticity system mode/rate: update returned unexpected keys.";
                return;
            }

            ApplySetting(new SettingItem(rateResult.Key, rateResult.Value, rateResult.UpdatedMs.ToString(CultureInfo.InvariantCulture)));
            ApplySetting(new SettingItem(modeResult.Key, modeResult.Value, modeResult.UpdatedMs.ToString(CultureInfo.InvariantCulture)));
            var appliedMode = ParseBooleanSetting(modeResult.Value, probabilistic) ? "probabilistic" : "absolute";
            BrainInfoSummary = $"Plasticity system mode/rate: applied ({appliedMode}, rate={rate:0.######}); runtime updated={runtimeApplied}, skipped={runtimeSkipped}, failed={runtimeFailed}.";
        });
    }

    private void ApplySystemBooleanSettingToSummary(string operation, string key, bool requested, SettingValue? result)
    {
        _dispatcher.Post(() =>
        {
            if (result is null)
            {
                BrainInfoSummary = $"{operation}: update failed (settings unavailable).";
                return;
            }

            if (!string.Equals(result.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                BrainInfoSummary = $"{operation}: update returned unexpected key '{result.Key}'.";
                return;
            }

            ApplySetting(new SettingItem(result.Key, result.Value, result.UpdatedMs.ToString(CultureInfo.InvariantCulture)));
            var persisted = ParseBooleanSetting(result.Value, requested);
            BrainInfoSummary = $"{operation}: {(persisted ? "enabled" : "disabled")}.";
        });
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

    private void TrackKnownBrain(Guid? brainId)
    {
        if (!brainId.HasValue)
        {
            return;
        }

        if (_knownBrainsById.ContainsKey(brainId.Value))
        {
            return;
        }

        var option = new KnownBrainOption(brainId.Value, isActive: _activeBrains.Contains(brainId.Value));
        _knownBrainsById[brainId.Value] = option;
        KnownBrains.Add(option);
    }

    private async Task RefreshFeedbackBrainSummaryAsync()
    {
        var selected = SelectedFeedbackBrain;
        if (selected is null)
        {
            FeedbackBrainSummary = "No known brain selected.";
            return;
        }

        var brainId = selected.BrainId;
        var info = await _client.RequestBrainInfoAsync(brainId).ConfigureAwait(false);
        _dispatcher.Post(() =>
        {
            if (SelectedFeedbackBrain?.BrainId != brainId)
            {
                return;
            }

            if (info is null)
            {
                FeedbackBrainSummary = $"Brain {brainId:D}: unavailable.";
                return;
            }

            FeedbackBrainSummary = BuildBrainSummary(info);
        });
    }

    private string BuildBrainSummary(BrainInfo info)
    {
        var plasticityModeLabel = info.PlasticityProbabilisticUpdates ? "probabilistic" : "absolute";
        var homeostasisTargetLabel = HomeostasisTargetModes.FirstOrDefault(mode => mode.Mode == info.HomeostasisTargetMode)?.Label
                                     ?? info.HomeostasisTargetMode.ToString();
        var homeostasisUpdateLabel = HomeostasisUpdateModes.FirstOrDefault(mode => mode.Mode == info.HomeostasisUpdateMode)?.Label
                                     ?? info.HomeostasisUpdateMode.ToString();
        var inputCoordinatorModeLabel = FormatInputCoordinatorMode(info.InputCoordinatorMode);
        var outputVectorSourceLabel = FormatOutputVectorSource(info.OutputVectorSource);
        return
            $"Inputs: {info.InputWidth} | Outputs: {info.OutputWidth} | IO: input={inputCoordinatorModeLabel}, output_vector={outputVectorSourceLabel} | Energy: {info.EnergyRemaining} @ {info.EnergyRateUnitsPerSecond}/s | LastCost: {info.LastTickCost} | Plasticity: {(info.PlasticityEnabled ? "on" : "off")} ({plasticityModeLabel}, {info.PlasticityRate:0.######}) | Homeostasis: {(info.HomeostasisEnabled ? "on" : "off")} ({homeostasisTargetLabel}, {homeostasisUpdateLabel}, p={info.HomeostasisBaseProbability:0.######}, step={info.HomeostasisMinStepCodes}, coupling={(info.HomeostasisEnergyCouplingEnabled ? "on" : "off")})";
    }

    private static string FormatInputCoordinatorMode(InputCoordinatorMode mode)
    {
        return mode switch
        {
            InputCoordinatorMode.ReplayLatestVector => "replay_latest_vector",
            _ => "dirty_on_change"
        };
    }

    private static string FormatOutputVectorSource(OutputVectorSource source)
    {
        return source switch
        {
            OutputVectorSource.Buffer => "buffer",
            _ => "potential"
        };
    }

    private bool TryParseSystemPlasticityRateDraft(out float rate, out string normalizedText)
    {
        if (!float.TryParse(SystemPlasticityRateTextDraft, NumberStyles.Float, CultureInfo.InvariantCulture, out rate)
            || !float.IsFinite(rate)
            || rate < 0f)
        {
            normalizedText = string.Empty;
            return false;
        }

        normalizedText = rate.ToString("0.######", CultureInfo.InvariantCulture);
        return true;
    }

    private static string ParseSystemPlasticityRateText(string? value, string fallback)
    {
        if (!float.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            || !float.IsFinite(parsed)
            || parsed < 0f)
        {
            return fallback;
        }

        return parsed.ToString("0.######", CultureInfo.InvariantCulture);
    }

    private static InputCoordinatorMode ParseInputCoordinatorModeSetting(string? value, InputCoordinatorMode fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "0" or "dirty" or "dirty_on_change" => InputCoordinatorMode.DirtyOnChange,
            "1" or "replay" or "replay_latest_vector" => InputCoordinatorMode.ReplayLatestVector,
            _ => fallback
        };
    }

    private static OutputVectorSource ParseOutputVectorSourceSetting(string? value, OutputVectorSource fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "0" or "potential" => OutputVectorSource.Potential,
            "1" or "buffer" => OutputVectorSource.Buffer,
            _ => fallback
        };
    }

    private static bool ParseBooleanSetting(string? value, bool fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "1" or "true" or "yes" or "on" => true,
            "0" or "false" or "no" or "off" => false,
            _ => fallback
        };
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
public sealed record InputCoordinatorModeOption(string Label, InputCoordinatorMode Mode, string SettingValue);
public sealed record OutputVectorSourceOption(string Label, OutputVectorSource Source, string SettingValue);
public sealed record HomeostasisTargetModeOption(string Label, HomeostasisTargetMode Mode);
public sealed record HomeostasisUpdateModeOption(string Label, HomeostasisUpdateMode Mode);

public sealed class KnownBrainOption : ViewModelBase
{
    private bool _isActive;

    public KnownBrainOption(Guid brainId, bool isActive)
    {
        BrainId = brainId;
        _isActive = isActive;
    }

    public Guid BrainId { get; }

    public bool IsActive
    {
        get => _isActive;
        set
        {
            if (SetProperty(ref _isActive, value))
            {
                OnPropertyChanged(nameof(Label));
            }
        }
    }

    public string Label => IsActive
        ? $"{BrainId:D} (active)"
        : $"{BrainId:D} (known)";
}


