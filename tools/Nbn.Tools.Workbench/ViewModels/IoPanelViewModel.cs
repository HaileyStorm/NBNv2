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

public sealed partial class IoPanelViewModel : ViewModelBase
{
    private const int MaxEvents = 300;
    private static readonly bool LogInputDiagnostics =
        IsEnvTrue("NBN_VIZ_DIAGNOSTICS_ENABLED") || IsEnvTrue("NBN_INPUT_DIAGNOSTICS_ENABLED");
    private readonly WorkbenchClient _client;
    private readonly UiDispatcher _dispatcher;
    private readonly Func<int, string> _buildSuggestedVector;
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
    private bool _randomizeInputVectorAfterEverySend;
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

    public IoPanelViewModel(WorkbenchClient client, UiDispatcher dispatcher, Func<int, string>? buildSuggestedVector = null)
    {
        _client = client;
        _dispatcher = dispatcher;
        _buildSuggestedVector = buildSuggestedVector ?? BuildSuggestedVector;
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

    public bool RandomizeInputVectorAfterEverySend
    {
        get => _randomizeInputVectorAfterEverySend;
        set => SetProperty(ref _randomizeInputVectorAfterEverySend, value);
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
