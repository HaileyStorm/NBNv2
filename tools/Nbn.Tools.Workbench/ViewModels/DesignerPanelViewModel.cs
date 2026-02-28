using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Media;
using Avalonia.Platform.Storage;
using Avalonia.Threading;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
using Nbn.Shared.Sharding;
using Nbn.Shared.Validation;
using Nbn.Tools.Workbench.Services;
using ProtoControl = Nbn.Proto.Control;
using ProtoShardPlanMode = Nbn.Proto.Control.ShardPlanMode;
using SharedShardPlanMode = Nbn.Shared.Sharding.ShardPlanMode;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class DesignerPanelViewModel : ViewModelBase
{
    private const string NoDocumentStatus = "No file loaded.";
    private const string NoDesignStatus = "Create or import a .nbn to edit.";
    private static readonly TimeSpan SpawnRegistrationTimeout = TimeSpan.FromSeconds(12);
    private static readonly TimeSpan SpawnRegistrationPollInterval = TimeSpan.FromMilliseconds(300);
    private static readonly bool LogSpawnDiagnostics = IsEnvTrue("NBN_WORKBENCH_SPAWN_DIAGNOSTICS_ENABLED");
    private const int DefaultActivationFunctionId = 11; // ACT_TANH (internal)
    private const int DefaultInputActivationFunctionId = 1; // ACT_IDENTITY
    private const int DefaultOutputActivationFunctionId = 11; // ACT_TANH
    private const int DefaultInputResetFunctionId = 0; // RESET_ZERO
    private const int ActivityDriverActivationFunctionId = 17; // ACT_ADD
    private const int ActivityDriverResetFunctionId = 0; // RESET_ZERO
    private const int ActivityDriverParamACode = 63;
    private const int ActivityDriverStrengthCode = 31;
    private const int MaxKnownActivationFunctionId = 29;
    private const int MaxKnownResetFunctionId = 60;
    private const string FunctionLegendText = "Legend: B=buffer, I=inbox, P=potential, T=activation threshold, A=Param A, Bp=Param B, K=out-degree.";
    private const string ParameterHelpText = "Param A/B are per-neuron parameters used by some activation functions. Pre-Act Thresh gates activation (B must exceed it). Act Thresh gates firing (|P| must exceed it).";
    private const string ResetPendingText = "Reset pending: changes not exported. Click again to confirm.";
    private const double BaseCanvasNodeSize = 40;
    private const double BaseCanvasGap = 18;
    private const double CanvasPadding = 24;
    private const double OffPageEdgeOuterMargin = 28;
    private const double OffPageEdgeCanvasMargin = 14;
    private const double OffPageEdgeFallbackRadiusPadding = 26;
    private const double MinCanvasWidth = 920;
    private const double MinCanvasHeight = 620;
    private const int RegionIntrasliceUnit = 3;
    private const int RegionAxialUnit = 5;
    private const double InterRegionWeight = 1.25;
    private const double IntraRegionWeight = 0.85;
    private const double ForwardRegionWeight = 1.2;
    private const double BackwardRegionWeight = 0.85;
    private const double OutputRegionWeight = 1.2;
    private const double AxonCountLowBiasPower = 1.7;
    private const double OutputAxonCountLowBiasPower = 2.6;
    private static readonly int[] ActivationFunctionIds = Enumerable.Range(0, MaxKnownActivationFunctionId + 1).ToArray();
    private static readonly int[] RandomInternalActivationFunctionIds = Enumerable.Range(1, MaxKnownActivationFunctionId).ToArray();
    private static readonly int[] InputAllowedActivationFunctionIds = { 1, 6, 7, 11, 16, 17, 25 };
    private static readonly int[] OutputAllowedActivationFunctionIds = { 1, 5, 6, 7, 8, 11, 14, 16, 17, 18, 19, 20, 23, 24, 25 };
    private static readonly int[] ResetFunctionIds = Enumerable.Range(0, MaxKnownResetFunctionId + 1).ToArray();
    private static readonly int[] InputAllowedResetFunctionIds = { 0, 1, 3, 17, 30 };
    private static readonly int[] AccumulationFunctionIds = { 0, 1, 2, 3 };
    private static readonly double[] ActivationFunctionWeights = BuildActivationFunctionWeights();
    private static readonly double[] ResetFunctionWeights = BuildResetFunctionWeights();
    private static readonly double[] AccumulationFunctionWeights = BuildAccumulationFunctionWeights();
    private readonly ConnectionViewModel _connections;
    private readonly WorkbenchClient _client;
    private readonly Dictionary<Guid, DesignerSpawnState> _spawnedBrains = new();
    private string _status = "Designer ready.";
    private string _loadedSummary = NoDocumentStatus;
    private string _validationSummary = "Validation not run.";
    private bool _validationHasRun;
    private bool _validationPassed;
    private bool _designDirty;
    private bool _resetPending;
    private DesignerDocumentType _documentType = DesignerDocumentType.None;
    private byte[]? _snapshotBytes;
    private string? _documentPath;
    private NbsHeaderV2? _nbsHeader;
    private IReadOnlyList<NbsRegionSection>? _nbsRegions;
    private NbsOverlaySection? _nbsOverlay;
    private DesignerBrainViewModel? _brain;
    private DesignerRegionViewModel? _selectedRegion;
    private DesignerNeuronViewModel? _selectedNeuron;
    private DesignerAxonViewModel? _selectedAxon;
    private int _defaultAxonStrength = 24;
    private int _regionPageSize = 256;
    private string _regionPageSizeText = "256";
    private int _regionPageIndex;
    private string _regionPageIndexText = "1";
    private int _regionPageCount = 1;
    private string _regionPageSummary = string.Empty;
    private string _regionSizeText = "0";
    private string _jumpNeuronIdText = "0";
    private string _axonTargetRegionText = "1";
    private string _axonTargetNeuronText = "0";
    private double _canvasZoom = 0.9;
    private string _canvasZoomText = "0.9";
    private double _canvasWidth;
    private double _canvasHeight;
    private bool _regionRefreshPending;
    private bool _suppressNeuronConstraintEnforcement;
    private string _edgeSummary = string.Empty;
    private string _edgeAnalyticsSummary = "Select a neuron to inspect edge analytics.";
    private string _snapshotTickText = "0";
    private string _snapshotEnergyText = "0";
    private bool _snapshotIncludeEnabledBitset = true;
    private bool _snapshotCostEnergyEnabled;
    private bool _snapshotPlasticityEnabled;
    private readonly IReadOnlyList<ShardPlanOption> _shardPlanOptions;
    private ShardPlanOption _selectedShardPlan;
    private string _spawnArtifactRoot = BuildDefaultArtifactRoot();
    private string _spawnShardCountText = "1";
    private string _spawnShardTargetNeuronsText = "0";
    private readonly RandomBrainOptionsViewModel _randomOptions;

    public DesignerPanelViewModel(ConnectionViewModel connections, WorkbenchClient client)
    {
        _connections = connections;
        _client = client;
        _spawnArtifactRoot = BuildDefaultArtifactRoot();
        _randomOptions = new RandomBrainOptionsViewModel();
        _shardPlanOptions = BuildShardPlanOptions();
        _selectedShardPlan = _shardPlanOptions[0];
        ValidationIssues = new ObservableCollection<string>();
        VisibleNeurons = new ObservableCollection<DesignerNeuronViewModel>();
        VisibleEdges = new ObservableCollection<DesignerEdgeViewModel>();

        ActivationFunctions = new ObservableCollection<DesignerFunctionOption>(BuildActivationFunctions());
        ResetFunctions = new ObservableCollection<DesignerFunctionOption>(BuildResetFunctions());
        AccumulationFunctions = new ObservableCollection<DesignerFunctionOption>(BuildAccumulationFunctions());

        NewBrainCommand = new RelayCommand(NewBrain);
        NewRandomBrainCommand = new RelayCommand(NewRandomBrain);
        ResetBrainCommand = new RelayCommand(ResetBrain, () => CanResetBrain);
        ImportNbnCommand = new AsyncRelayCommand(ImportNbnAsync);
        ImportNbsCommand = new AsyncRelayCommand(ImportNbsAsync);
        ExportCommand = new AsyncRelayCommand(ExportAsync, () => _documentType != DesignerDocumentType.None);
        ExportSnapshotCommand = new AsyncRelayCommand(ExportSnapshotAsync, () => CanExportSnapshot);
        ValidateCommand = new RelayCommand(Validate, () => _documentType != DesignerDocumentType.None);
        SpawnBrainCommand = new AsyncRelayCommand(SpawnBrainAsync, () => CanSpawnBrain);

        SelectRegionCommand = new RelayCommand<DesignerRegionViewModel>(SelectRegion);
        SelectNeuronCommand = new RelayCommand<DesignerNeuronViewModel>(SelectNeuron);
        SelectAxonCommand = new RelayCommand<DesignerAxonViewModel>(SelectAxon);
        AddNeuronCommand = new RelayCommand(AddNeuron, () => CanAddNeuron);
        ToggleNeuronEnabledCommand = new RelayCommand(ToggleNeuronEnabled, () => CanToggleNeuron);
        ClearSelectionCommand = new RelayCommand(ClearSelection, () => CanEditDesign);
        RemoveAxonCommand = new RelayCommand(RemoveSelectedAxon, () => CanRemoveAxon);
        RandomizeSeedCommand = new RelayCommand(RandomizeSeed, () => Brain is not null);
        RandomizeBrainIdCommand = new RelayCommand(RandomizeBrainId, () => Brain is not null);
        ApplyRegionSizeCommand = new RelayCommand(ApplyRegionSize, () => CanEditDesign && SelectedRegion is not null);
        PreviousRegionPageCommand = new RelayCommand(PreviousRegionPage, () => CanEditDesign && RegionPageIndex > 0);
        NextRegionPageCommand = new RelayCommand(NextRegionPage, () => CanEditDesign && RegionPageIndex + 1 < RegionPageCount);
        FirstRegionPageCommand = new RelayCommand(FirstRegionPage, () => CanEditDesign && RegionPageIndex > 0);
        LastRegionPageCommand = new RelayCommand(LastRegionPage, () => CanEditDesign && RegionPageIndex + 1 < RegionPageCount);
        JumpToNeuronCommand = new RelayCommand(JumpToNeuron, () => CanEditDesign && SelectedRegion is not null);
        AddAxonByIdCommand = new RelayCommand(AddAxonById, () => CanEditDesign && SelectedNeuron is not null);
        FocusEdgeEndpointCommand = new RelayCommand<DesignerEdgeViewModel>(FocusEdgeEndpoint);
    }

    public ObservableCollection<string> ValidationIssues { get; }

    public ObservableCollection<DesignerFunctionOption> ActivationFunctions { get; }
    public ObservableCollection<DesignerFunctionOption> ResetFunctions { get; }
    public ObservableCollection<DesignerFunctionOption> AccumulationFunctions { get; }
    public ObservableCollection<DesignerNeuronViewModel> VisibleNeurons { get; }
    public ObservableCollection<DesignerEdgeViewModel> VisibleEdges { get; }
    public RandomBrainOptionsViewModel RandomOptions => _randomOptions;

    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }

    public string LoadedSummary
    {
        get => _loadedSummary;
        set => SetProperty(ref _loadedSummary, value);
    }

    public string ValidationSummary
    {
        get => _validationSummary;
        set => SetProperty(ref _validationSummary, value);
    }

    public DesignerBrainViewModel? Brain
    {
        get => _brain;
        private set
        {
            if (_brain is not null)
            {
                _brain.PropertyChanged -= OnBrainPropertyChanged;
            }

            if (SetProperty(ref _brain, value))
            {
                if (_brain is not null)
                {
                    _brain.PropertyChanged += OnBrainPropertyChanged;
                }

                OnPropertyChanged(nameof(IsDesignLoaded));
                OnPropertyChanged(nameof(CanEditDesign));
                OnPropertyChanged(nameof(IsDesignVisible));
                OnPropertyChanged(nameof(IsSnapshotVisible));
                OnPropertyChanged(nameof(DesignHint));
                OnPropertyChanged(nameof(CanExportSnapshot));
                OnPropertyChanged(nameof(CanSpawnBrain));
                OnPropertyChanged(nameof(CanResetBrain));
                UpdateCommandStates();
            }
        }
    }

    public DesignerRegionViewModel? SelectedRegion
    {
        get => _selectedRegion;
        private set
        {
            var previousRegion = _selectedRegion;
            if (SetProperty(ref _selectedRegion, value))
            {
                if (previousRegion is not null)
                {
                    previousRegion.Neurons.CollectionChanged -= OnSelectedRegionNeuronsChanged;
                }

                if (_selectedRegion is not null)
                {
                    _selectedRegion.Neurons.CollectionChanged += OnSelectedRegionNeuronsChanged;
                }

                OnPropertyChanged(nameof(SelectedRegionLabel));
                UpdateRegionSizeText();
                UpdateJumpNeuronText();
                if (SelectedRegion is not null)
                {
                    AxonTargetRegionText = SelectedRegion.RegionId == NbnConstants.InputRegionId
                        ? "1"
                        : SelectedRegion.RegionId.ToString();
                }
                SetRegionPageIndex(0);
                RefreshRegionView();
                UpdateCommandStates();
            }
        }
    }

    public DesignerNeuronViewModel? SelectedNeuron
    {
        get => _selectedNeuron;
        private set
        {
            if (_selectedNeuron is not null)
            {
                _selectedNeuron.PropertyChanged -= OnSelectedNeuronChanged;
            }

            if (SetProperty(ref _selectedNeuron, value))
            {
                if (_selectedNeuron is not null)
                {
                    _selectedNeuron.PropertyChanged += OnSelectedNeuronChanged;
                }

                OnPropertyChanged(nameof(SelectedNeuronLabel));
                OnPropertyChanged(nameof(HasNeuronSelection));
                OnPropertyChanged(nameof(SelectedNeuronUsesParamA));
                OnPropertyChanged(nameof(SelectedNeuronUsesParamB));
                OnPropertyChanged(nameof(SelectedActivationDescription));
                OnPropertyChanged(nameof(SelectedResetDescription));
                OnPropertyChanged(nameof(SelectedAccumulationDescription));
                OnPropertyChanged(nameof(SelectedNeuronConstraintHint));
                UpdateJumpNeuronText();
                RefreshEdges();
                UpdateCommandStates();
            }
        }
    }

    public DesignerAxonViewModel? SelectedAxon
    {
        get => _selectedAxon;
        private set
        {
            if (_selectedAxon is not null)
            {
                _selectedAxon.PropertyChanged -= OnSelectedAxonChanged;
            }

            if (SetProperty(ref _selectedAxon, value))
            {
                if (_selectedAxon is not null)
                {
                    _selectedAxon.PropertyChanged += OnSelectedAxonChanged;
                }

                OnPropertyChanged(nameof(HasAxonSelection));
                RefreshEdges();
                UpdateCommandStates();
            }
        }
    }

    public int DefaultAxonStrength
    {
        get => _defaultAxonStrength;
        set => SetProperty(ref _defaultAxonStrength, Clamp(value, 0, 31));
    }

    public string RegionPageSizeText
    {
        get => _regionPageSizeText;
        set
        {
            if (!SetProperty(ref _regionPageSizeText, value))
            {
                return;
            }

            if (int.TryParse(value, out var parsed) && parsed > 0)
            {
                _regionPageSize = parsed;
                SetRegionPageIndex(RegionPageIndex);
                RefreshRegionView();
            }
        }
    }

    public int RegionPageIndex
    {
        get => _regionPageIndex;
        private set
        {
            if (SetProperty(ref _regionPageIndex, value))
            {
                _regionPageIndexText = (_regionPageIndex + 1).ToString();
                OnPropertyChanged(nameof(RegionPageIndexText));
                UpdateRegionPageSummary();
                UpdateCommandStates();
            }
        }
    }

    public string RegionPageIndexText
    {
        get => _regionPageIndexText;
        set
        {
            if (!SetProperty(ref _regionPageIndexText, value))
            {
                return;
            }

            if (int.TryParse(value, out var parsed))
            {
                if (parsed - 1 != RegionPageIndex)
                {
                    SetRegionPageIndex(parsed - 1);
                }
            }
        }
    }

    public int RegionPageCount
    {
        get => _regionPageCount;
        private set => SetProperty(ref _regionPageCount, value);
    }

    public string RegionPageSummary
    {
        get => _regionPageSummary;
        private set => SetProperty(ref _regionPageSummary, value);
    }

    public string RegionSizeText
    {
        get => _regionSizeText;
        set => SetProperty(ref _regionSizeText, value);
    }

    public string JumpNeuronIdText
    {
        get => _jumpNeuronIdText;
        set => SetProperty(ref _jumpNeuronIdText, value);
    }

    public string AxonTargetRegionText
    {
        get => _axonTargetRegionText;
        set => SetProperty(ref _axonTargetRegionText, value);
    }

    public string AxonTargetNeuronText
    {
        get => _axonTargetNeuronText;
        set => SetProperty(ref _axonTargetNeuronText, value);
    }

    public double CanvasZoom
    {
        get => _canvasZoom;
        set
        {
            if (SetProperty(ref _canvasZoom, Clamp(value, 0.5, 2.5)))
            {
                CanvasZoomText = _canvasZoom.ToString("0.##");
                OnPropertyChanged(nameof(CanvasNodeSize));
                OnPropertyChanged(nameof(CanvasNodeRadius));
                OnPropertyChanged(nameof(CanvasNodeGap));
                RefreshRegionView();
            }
        }
    }

    public string CanvasZoomText
    {
        get => _canvasZoomText;
        set
        {
            if (!SetProperty(ref _canvasZoomText, value))
            {
                return;
            }

            if (double.TryParse(value, out var parsed))
            {
                CanvasZoom = parsed;
            }
        }
    }

    public double CanvasNodeSize => BaseCanvasNodeSize * CanvasZoom;
    public double CanvasNodeRadius => CanvasNodeSize / 2;
    public double CanvasNodeGap => BaseCanvasGap * CanvasZoom;

    public double CanvasWidth
    {
        get => _canvasWidth;
        private set => SetProperty(ref _canvasWidth, value);
    }

    public double CanvasHeight
    {
        get => _canvasHeight;
        private set => SetProperty(ref _canvasHeight, value);
    }

    public string EdgeSummary
    {
        get => _edgeSummary;
        private set => SetProperty(ref _edgeSummary, value);
    }

    public string EdgeAnalyticsSummary
    {
        get => _edgeAnalyticsSummary;
        private set => SetProperty(ref _edgeAnalyticsSummary, value);
    }

    public string SnapshotTickText
    {
        get => _snapshotTickText;
        set => SetProperty(ref _snapshotTickText, value);
    }

    public string SnapshotEnergyText
    {
        get => _snapshotEnergyText;
        set => SetProperty(ref _snapshotEnergyText, value);
    }

    public bool SnapshotIncludeEnabledBitset
    {
        get => _snapshotIncludeEnabledBitset;
        set => SetProperty(ref _snapshotIncludeEnabledBitset, value);
    }

    public bool SnapshotCostEnergyEnabled
    {
        get => _snapshotCostEnergyEnabled;
        set => SetProperty(ref _snapshotCostEnergyEnabled, value);
    }

    public bool SnapshotPlasticityEnabled
    {
        get => _snapshotPlasticityEnabled;
        set => SetProperty(ref _snapshotPlasticityEnabled, value);
    }

    public IReadOnlyList<ShardPlanOption> ShardPlanOptions => _shardPlanOptions;

    public ShardPlanOption SelectedShardPlan
    {
        get => _selectedShardPlan;
        set => SetProperty(ref _selectedShardPlan, value);
    }

    public string SpawnArtifactRoot
    {
        get => _spawnArtifactRoot;
        set => SetProperty(ref _spawnArtifactRoot, value);
    }

    public string SpawnShardCountText
    {
        get => _spawnShardCountText;
        set => SetProperty(ref _spawnShardCountText, value);
    }

    public string SpawnShardTargetNeuronsText
    {
        get => _spawnShardTargetNeuronsText;
        set => SetProperty(ref _spawnShardTargetNeuronsText, value);
    }

    public bool IsDesignLoaded => _documentType == DesignerDocumentType.Nbn && Brain is not null;
    public bool IsSnapshotLoaded => _documentType == DesignerDocumentType.Nbs;
    public bool CanEditDesign => IsDesignLoaded;
    public bool CanAddNeuron => CanEditDesign && SelectedRegion is not null;
    public bool CanToggleNeuron => CanEditDesign && SelectedNeuron is not null && !SelectedNeuron.IsRequired;
    public bool CanRemoveAxon => CanEditDesign && SelectedAxon is not null && SelectedNeuron is not null;
    public bool CanExportSnapshot => IsDesignLoaded;
    public bool CanSpawnBrain => IsDesignLoaded;
    public bool CanResetBrain => IsDesignLoaded;
    public bool IsDesignDirty => _designDirty;

    public bool IsDesignVisible => IsDesignLoaded;
    public bool IsSnapshotVisible => IsSnapshotLoaded;
    public bool HasNeuronSelection => SelectedNeuron is not null;
    public bool HasAxonSelection => SelectedAxon is not null;

    public string DesignHint => IsDesignLoaded ? string.Empty : NoDesignStatus;

    public string SelectedRegionLabel => SelectedRegion is null ? "No region selected" : SelectedRegion.Label;
    public string SelectedNeuronLabel => SelectedNeuron is null
        ? "No neuron selected"
        : $"Region {SelectedNeuron.RegionId} / Neuron {SelectedNeuron.NeuronId}";

    public bool SelectedNeuronUsesParamA => SelectedNeuron is not null && UsesParamA(SelectedNeuron.ActivationFunctionId);

    public bool SelectedNeuronUsesParamB => SelectedNeuron is not null && UsesParamB(SelectedNeuron.ActivationFunctionId);

    public string SelectedActivationDescription => DescribeActivation(SelectedNeuron?.ActivationFunctionId ?? 0);

    public string SelectedResetDescription => DescribeReset(SelectedNeuron?.ResetFunctionId ?? 0);

    public string SelectedAccumulationDescription => DescribeAccumulation(SelectedNeuron?.AccumulationFunctionId ?? 0);

    public string SelectedNeuronConstraintHint => BuildNeuronConstraintHint(SelectedNeuron);

    public string FunctionLegend => FunctionLegendText;

    public string ParameterHelp => ParameterHelpText;

    public string ResetBrainButtonLabel => _resetPending ? "Confirm Reset" : "Reset Brain";

    public IBrush ResetBrainButtonBackground => _resetPending ? DesignerBrushes.Accent : DesignerBrushes.SurfaceAlt;

    public IBrush ResetBrainButtonForeground => _resetPending ? DesignerBrushes.OnAccent : DesignerBrushes.Ink;

    public IBrush ResetBrainButtonBorder => _resetPending ? DesignerBrushes.Accent : DesignerBrushes.Border;

    public RelayCommand NewBrainCommand { get; }
    public RelayCommand NewRandomBrainCommand { get; }
    public RelayCommand ResetBrainCommand { get; }
    public AsyncRelayCommand ImportNbnCommand { get; }
    public AsyncRelayCommand ImportNbsCommand { get; }
    public AsyncRelayCommand ExportCommand { get; }
    public AsyncRelayCommand ExportSnapshotCommand { get; }
    public RelayCommand ValidateCommand { get; }
    public AsyncRelayCommand SpawnBrainCommand { get; }
    public RelayCommand<DesignerRegionViewModel> SelectRegionCommand { get; }
    public RelayCommand<DesignerNeuronViewModel> SelectNeuronCommand { get; }
    public RelayCommand<DesignerAxonViewModel> SelectAxonCommand { get; }
    public RelayCommand AddNeuronCommand { get; }
    public RelayCommand ToggleNeuronEnabledCommand { get; }
    public RelayCommand ClearSelectionCommand { get; }
    public RelayCommand RemoveAxonCommand { get; }
    public RelayCommand RandomizeSeedCommand { get; }
    public RelayCommand RandomizeBrainIdCommand { get; }
    public RelayCommand ApplyRegionSizeCommand { get; }
    public RelayCommand PreviousRegionPageCommand { get; }
    public RelayCommand NextRegionPageCommand { get; }
    public RelayCommand FirstRegionPageCommand { get; }
    public RelayCommand LastRegionPageCommand { get; }
    public RelayCommand JumpToNeuronCommand { get; }
    public RelayCommand AddAxonByIdCommand { get; }
    public RelayCommand<DesignerEdgeViewModel> FocusEdgeEndpointCommand { get; }

    private void NewBrain()
    {
        ClearResetConfirmation();
        var seed = GenerateSeed();
        var brainId = Guid.NewGuid();
        var brain = new DesignerBrainViewModel("Untitled Brain", brainId, seed, 1024);
        for (var i = 0; i < NbnConstants.RegionCount; i++)
        {
            brain.Regions.Add(new DesignerRegionViewModel(i));
        }

        var inputRegion = brain.Regions[NbnConstants.InputRegionId];
        AddDefaultNeuron(inputRegion);

        var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
        AddDefaultNeuron(outputRegion);

        brain.UpdateTotals();

        SetDocumentType(DesignerDocumentType.Nbn);
        Brain = brain;
        _snapshotBytes = null;
        _documentPath = null;
        _nbsHeader = null;
        _nbsRegions = null;
        _nbsOverlay = null;

        SelectRegion(inputRegion);
        SelectNeuron(inputRegion.Neurons.FirstOrDefault());
        SetDesignDirty(true);
        ResetValidation();
        UpdateLoadedSummary();
        Status = "New brain created.";
        RefreshRegionView();
        ExportCommand.RaiseCanExecuteChanged();
        ValidateCommand.RaiseCanExecuteChanged();
        SpawnBrainCommand.RaiseCanExecuteChanged();
        ResetBrainCommand.RaiseCanExecuteChanged();
    }

    private void NewRandomBrain()
    {
        ClearResetConfirmation();
        if (!RandomOptions.TryBuildOptions(GenerateSeed, out var options, out var error))
        {
            Status = error ?? "Random brain options invalid.";
            return;
        }
        var seed = options.Seed;
        var brainId = Guid.NewGuid();
        var brain = new DesignerBrainViewModel("Random Brain", brainId, seed, NbnConstants.DefaultAxonStride);

        var rng = new Random(unchecked((int)seed));
        var selectedRegions = SelectRegions(rng, options);

        var activationPickers = BuildRegionFunctionPickers(
            rng,
            options.ActivationMode,
            options.ActivationFixedId,
            ActivationFunctionIds,
            ActivationFunctionWeights,
            GetRandomAllowedActivationFunctionIdsForRegion,
            GetDefaultActivationFunctionIdForRegion);
        var resetPickers = BuildRegionFunctionPickers(
            rng,
            options.ResetMode,
            options.ResetFixedId,
            ResetFunctionIds,
            ResetFunctionWeights,
            GetAllowedResetFunctionIdsForRegion,
            GetDefaultResetFunctionIdForRegion);
        var accumulationPicker = CreateFunctionPicker(rng, options.AccumulationMode, options.AccumulationFixedId, AccumulationFunctionIds, AccumulationFunctionWeights);
        var preActivationPicker = CreateCenteredCodePicker(rng, options.ThresholdMode, options.PreActivationMin, options.PreActivationMin, options.PreActivationMax);
        var activationThresholdPicker = CreateLowBiasedCodePicker(rng, options.ThresholdMode, options.ActivationThresholdMin, options.ActivationThresholdMin, options.ActivationThresholdMax);
        var paramAPicker = CreateCenteredCodePicker(rng, options.ParamMode, options.ParamAMin, options.ParamAMin, options.ParamAMax);
        var paramBPicker = CreateCenteredCodePicker(rng, options.ParamMode, options.ParamBMin, options.ParamBMin, options.ParamBMax);

        for (var i = 0; i < NbnConstants.RegionCount; i++)
        {
            var region = new DesignerRegionViewModel(i);
            var activationPicker = activationPickers[i];
            var resetPicker = resetPickers[i];
            var targetCount = 0;
            if (i == NbnConstants.InputRegionId || i == NbnConstants.OutputRegionId)
            {
                targetCount = i == NbnConstants.InputRegionId
                    ? PickRangeCount(rng, options.InputNeuronMin, options.InputNeuronMax)
                    : PickRangeCount(rng, options.OutputNeuronMin, options.OutputNeuronMax);
            }
            else if (selectedRegions.Contains(i))
            {
                targetCount = PickCount(rng, options.NeuronCountMode, options.NeuronsPerRegion, options.NeuronCountMin, options.NeuronCountMax);
            }

            for (var n = 0; n < targetCount; n++)
            {
                var neuron = CreateDefaultNeuron(region, n);
                var activationId = activationPicker();
                neuron.ActivationFunctionId = activationId;
                neuron.ResetFunctionId = resetPicker();
                neuron.AccumulationFunctionId = accumulationPicker();
                neuron.PreActivationThresholdCode = preActivationPicker();
                neuron.ActivationThresholdCode = activationThresholdPicker();
                neuron.ParamACode = UsesParamA(activationId) ? paramAPicker() : 0;
                neuron.ParamBCode = UsesParamB(activationId) ? paramBPicker() : 0;
                region.Neurons.Add(neuron);
            }

            region.UpdateCounts();
            brain.Regions.Add(region);
        }

        var regionsWithNeurons = brain.Regions.Where(r => r.NeuronCount > 0).ToList();
        foreach (var region in regionsWithNeurons)
        {
            var validTargets = BuildValidTargetsForRegion(region, regionsWithNeurons, options);
            if (validTargets.Count == 0)
            {
                continue;
            }

            var targetWeights = BuildTargetWeights(region.RegionId, validTargets, options.TargetBiasMode);
            foreach (var neuron in region.Neurons)
            {
                if (!neuron.Exists)
                {
                    continue;
                }

                var axonMin = options.AxonCountMin;
                var axonMax = options.AxonCountMax;
                var biasPower = AxonCountLowBiasPower;
                if (region.RegionId == NbnConstants.OutputRegionId)
                {
                    axonMin = 0;
                    axonMax = Math.Min(axonMax, 10);
                    if (axonMax < axonMin)
                    {
                        axonMin = axonMax;
                    }

                    biasPower = OutputAxonCountLowBiasPower;
                }

                var targetAxons = PickBiasedCount(rng, options.AxonCountMode, options.AxonsPerNeuron, axonMin, axonMax, biasPower);
                var maxTargets = MaxDistinctTargets(region.RegionId, validTargets, options.AllowSelfLoops);
                if (maxTargets == 0)
                {
                    continue;
                }

                if (targetAxons > maxTargets)
                {
                    targetAxons = maxTargets;
                }

                if (targetAxons == 0)
                {
                    continue;
                }

                var seen = new HashSet<(int regionId, int neuronId)>();
                var attempts = 0;
                var maxAttempts = Math.Max(targetAxons * 6, maxTargets * 3);
                while (seen.Count < targetAxons && attempts < maxAttempts)
                {
                    attempts++;
                    var targetRegion = validTargets[PickWeightedIndex(rng, targetWeights)];
                    if (targetRegion.NeuronCount == 0)
                    {
                        continue;
                    }

                    var targetNeuronId = PickTargetNeuronId(rng, neuron.NeuronId, region.RegionId, targetRegion, options.TargetBiasMode);
                    if (!options.AllowSelfLoops
                        && targetRegion.RegionId == region.RegionId
                        && targetNeuronId == neuron.NeuronId)
                    {
                        continue;
                    }

                    if (!seen.Add((targetRegion.RegionId, targetNeuronId)))
                    {
                        continue;
                    }

                    var strength = PickStrengthCode(rng, options.StrengthDistribution, options.StrengthMinCode, options.StrengthMaxCode);
                    neuron.Axons.Add(new DesignerAxonViewModel(targetRegion.RegionId, targetNeuronId, strength));
                }

                neuron.UpdateAxonCount();
            }

            region.UpdateCounts();
        }

        EnsureNeuronOutboundCoverage(rng, brain, options);
        EnsureRegionInboundConnectivity(rng, brain, options);
        EnsureOutputInbound(rng, brain, options);
        if (options.SeedBaselineActivityPath)
        {
            EnsureBaselineActivityPath(rng, brain);
        }
        var normalizedFunctions = NormalizeBrainFunctionConstraints(brain);
        brain.UpdateTotals();

        SetDocumentType(DesignerDocumentType.Nbn);
        Brain = brain;
        _snapshotBytes = null;
        _documentPath = null;
        _nbsHeader = null;
        _nbsRegions = null;
        _nbsOverlay = null;

        var inputRegion = brain.Regions[NbnConstants.InputRegionId];
        SelectRegion(inputRegion);
        SelectNeuron(inputRegion.Neurons.FirstOrDefault());
        SetDesignDirty(true);
        ResetValidation();
        UpdateLoadedSummary();
        Status = normalizedFunctions == 0
            ? "Random brain created."
            : $"Random brain created. Normalized {normalizedFunctions} neuron function setting(s) for IO constraints.";
        RefreshRegionView();
        ExportCommand.RaiseCanExecuteChanged();
        ValidateCommand.RaiseCanExecuteChanged();
        SpawnBrainCommand.RaiseCanExecuteChanged();
        ResetBrainCommand.RaiseCanExecuteChanged();
    }

    private void ResetBrain()
    {
        if (Brain is null)
        {
            Status = "No design loaded.";
            return;
        }

        if (IsDesignDirty && !_resetPending)
        {
            _resetPending = true;
            OnPropertyChanged(nameof(ResetBrainButtonLabel));
            OnPropertyChanged(nameof(ResetBrainButtonBackground));
            OnPropertyChanged(nameof(ResetBrainButtonForeground));
            OnPropertyChanged(nameof(ResetBrainButtonBorder));
            Status = ResetPendingText;
            return;
        }

        ClearResetConfirmation();

        foreach (var region in Brain.Regions)
        {
            region.Neurons.Clear();
            if (region.IsInput || region.IsOutput)
            {
                region.Neurons.Add(CreateDefaultNeuron(region, 0));
            }

            region.UpdateCounts();
        }

        Brain.UpdateTotals();
        SelectRegion(Brain.Regions[NbnConstants.InputRegionId]);
        SelectNeuron(Brain.Regions[NbnConstants.InputRegionId].Neurons.FirstOrDefault());
        SetDesignDirty(true);
        ResetValidation();
        UpdateLoadedSummary();
        RefreshRegionView();
        Status = "Brain reset.";
    }

    private async Task ImportNbnAsync()
    {
        var file = await PickOpenFileAsync("Import .nbn", "NBN files", "nbn");
        if (file is null)
        {
            Status = "Import canceled.";
            return;
        }

        try
        {
            var bytes = await ReadAllBytesAsync(file);
            _ = TryImportNbnFromBytes(bytes, file.Name, FormatPath(file));
        }
        catch (Exception ex)
        {
            Status = $"Import failed: {ex.Message}";
        }
    }

    internal bool TryImportNbnFromBytes(byte[] bytes, string fileName, string? documentPath = null)
    {
        if (bytes is null)
        {
            throw new ArgumentNullException(nameof(bytes));
        }

        if (string.IsNullOrWhiteSpace(fileName))
        {
            fileName = "Imported";
        }

        try
        {
            var header = NbnBinary.ReadNbnHeader(bytes);
            var regions = ReadNbnRegions(bytes, header);
            var validation = NbnBinaryValidator.ValidateNbn(header, regions);
            if (!validation.IsValid)
            {
                Status = $"Import failed: Invalid .nbn ({fileName}): {FormatValidationIssueSummary(validation)}";
                return false;
            }

            var brain = BuildDesignerBrainFromNbn(header, regions, Path.GetFileNameWithoutExtension(fileName));
            var normalizedFunctions = NormalizeBrainFunctionConstraints(brain);

            SetDocumentType(DesignerDocumentType.Nbn);
            Brain = brain;
            _snapshotBytes = null;
            _documentPath = documentPath ?? fileName;
            _nbsHeader = null;
            _nbsRegions = null;
            _nbsOverlay = null;

            var region0 = brain.Regions[NbnConstants.InputRegionId];
            SelectRegion(region0);
            SelectNeuron(region0.Neurons.FirstOrDefault());

            LoadedSummary = BuildDesignSummary(brain, fileName);
            Status = normalizedFunctions == 0
                ? "NBN imported."
                : $"NBN imported. Normalized {normalizedFunctions} neuron function setting(s) for IO constraints.";
            SetDesignDirty(normalizedFunctions > 0);
            ResetValidation();
            RefreshRegionView();
            ExportCommand.RaiseCanExecuteChanged();
            ValidateCommand.RaiseCanExecuteChanged();
            SpawnBrainCommand.RaiseCanExecuteChanged();
            ResetBrainCommand.RaiseCanExecuteChanged();
            return true;
        }
        catch (Exception ex)
        {
            Status = $"Import failed: {ex.Message}";
            return false;
        }
    }

    private async Task ImportNbsAsync()
    {
        var file = await PickOpenFileAsync("Import .nbs", "NBS files", "nbs");
        if (file is null)
        {
            Status = "Import canceled.";
            return;
        }

        try
        {
            var bytes = await ReadAllBytesAsync(file);
            var header = NbnBinary.ReadNbsHeader(bytes);
            ReadNbsSections(bytes, header, out var regions, out var overlay);

            SetDocumentType(DesignerDocumentType.Nbs);
            _snapshotBytes = bytes;
            _documentPath = FormatPath(file);
            _nbsHeader = header;
            _nbsRegions = regions;
            _nbsOverlay = overlay;
            Brain = null;
            SelectRegion(null);
            ClearSelection();

            LoadedSummary = BuildNbsSummary(file.Name, header, regions, overlay);
            Status = "NBS imported.";
            SetDesignDirty(false);
            ResetValidation();
            ExportCommand.RaiseCanExecuteChanged();
            ValidateCommand.RaiseCanExecuteChanged();
            SpawnBrainCommand.RaiseCanExecuteChanged();
            ResetBrainCommand.RaiseCanExecuteChanged();
        }
        catch (Exception ex)
        {
            Status = $"Import failed: {ex.Message}";
        }
    }

    private async Task ExportAsync()
    {
        if (_documentType == DesignerDocumentType.None)
        {
            Status = "Nothing to export.";
            return;
        }

        if (_documentType == DesignerDocumentType.Nbs)
        {
            if (_snapshotBytes is null)
            {
                Status = "Snapshot data missing.";
                return;
            }

            var file = await PickSaveFileAsync("Export .nbs", "NBS files", "nbs", SuggestedName("nbs"));
            if (file is null)
            {
                Status = "Export canceled.";
                return;
            }

            try
            {
                await WriteAllBytesAsync(file, _snapshotBytes);
                Status = $"Exported to {FormatPath(file)}.";
            }
            catch (Exception ex)
            {
                Status = $"Export failed: {ex.Message}";
            }

            return;
        }

        if (!TryBuildNbn(out var header, out var sections, out var error))
        {
            Status = error ?? "Export failed.";
            return;
        }

        var suggestedName = SuggestedName("nbn");
        var saveFile = await PickSaveFileAsync("Export .nbn", "NBN files", "nbn", suggestedName);
        if (saveFile is null)
        {
            Status = "Export canceled.";
            return;
        }

        try
        {
            var bytes = NbnBinary.WriteNbn(header, sections);
            await WriteAllBytesAsync(saveFile, bytes);
            SetDesignDirty(false);
            Status = $"Exported to {FormatPath(saveFile)}.";
        }
        catch (Exception ex)
        {
            Status = $"Export failed: {ex.Message}";
        }
    }

    private async Task ExportSnapshotAsync()
    {
        if (!CanExportSnapshot)
        {
            Status = "No design loaded.";
            return;
        }

        if (!TryBuildSnapshot(out var snapshotBytes, out var error))
        {
            Status = error ?? "Snapshot export failed.";
            return;
        }

        var saveFile = await PickSaveFileAsync("Export .nbs", "NBS files", "nbs", SuggestedName("nbs"));
        if (saveFile is null)
        {
            Status = "Export canceled.";
            return;
        }

        try
        {
            await WriteAllBytesAsync(saveFile, snapshotBytes);
            Status = $"Snapshot exported to {FormatPath(saveFile)}.";
        }
        catch (Exception ex)
        {
            Status = $"Export failed: {ex.Message}";
        }
    }

    private async Task SpawnBrainAsync()
    {
        ClearResetConfirmation();
        if (Brain is null || !IsDesignLoaded)
        {
            Status = "No design loaded.";
            return;
        }

        if (!_connections.SettingsConnected || !_connections.HiveMindConnected || !_connections.IoConnected)
        {
            Status = "Connect Settings, HiveMind, and IO first.";
            return;
        }

        if (!_validationHasRun)
        {
            Validate();
        }

        if (!_validationPassed)
        {
            Status = "Spawn canceled: validation failed.";
            return;
        }

        Status = "Spawning brain...";

        try
        {
            var shardPlanMode = SelectedShardPlan.Value;

            int? shardCount = null;
            int? maxNeuronsPerShard = null;
            if (shardPlanMode == ShardPlanMode.FixedShardCount)
            {
                if (!TryParseOptionalNonNegativeInt(SpawnShardCountText, out shardCount))
                {
                    Status = "Invalid shard count.";
                    return;
                }
            }
            else if (shardPlanMode == ShardPlanMode.MaxNeuronsPerShard)
            {
                if (!TryParseOptionalNonNegativeInt(SpawnShardTargetNeuronsText, out maxNeuronsPerShard))
                {
                    Status = "Invalid shard target size.";
                    return;
                }
            }

            if (!TryBuildNbn(out var header, out var sections, out var error))
            {
                Status = error ?? "Spawn failed.";
                return;
            }

            var sharedPlanMode = ToSharedShardPlanMode(shardPlanMode);
            ShardPlanResult shardPlan;
            try
            {
                shardPlan = ShardPlanner.BuildPlan(header, sharedPlanMode, shardCount, maxNeuronsPerShard);
            }
            catch (Exception ex)
            {
                Status = $"Shard plan failed: {ex.Message}";
                return;
            }

            var plannedShardCount = shardPlan.Regions.Sum(entry => entry.Value.Count);
            if (plannedShardCount == 0)
            {
                Status = "Shard plan produced no shards.";
                return;
            }

            var designBrainId = Brain.BrainId;
            if (_spawnedBrains.TryGetValue(designBrainId, out var existing))
            {
                var response = await _client.ListBrainsAsync().ConfigureAwait(false);
                if (IsBrainRegistered(response, existing.RuntimeBrainId))
                {
                    Status = $"Brain already spawned ({existing.RuntimeBrainId:D}).";
                    return;
                }

                _spawnedBrains.Remove(designBrainId);
            }

            var artifactRoot = string.IsNullOrWhiteSpace(SpawnArtifactRoot) ? BuildDefaultArtifactRoot() : SpawnArtifactRoot;
            var brainArtifactRoot = Path.Combine(artifactRoot, designBrainId.ToString("N"));
            Directory.CreateDirectory(brainArtifactRoot);

            var nbnBytes = NbnBinary.WriteNbn(header, sections);
            var store = new LocalArtifactStore(new ArtifactStoreOptions(brainArtifactRoot));
            var manifest = await store.StoreAsync(new MemoryStream(nbnBytes), "application/x-nbn");
            var artifactRef = manifest.ArtifactId.Bytes.ToArray()
                .ToArtifactRef((ulong)Math.Max(0L, manifest.ByteLength), "application/x-nbn", brainArtifactRoot);
            if (LogSpawnDiagnostics && WorkbenchLog.Enabled)
            {
                var artifactSha = manifest.ArtifactId.ToHex().ToLowerInvariant();
                var regionCount = sections.Count;
                var neuronCount = sections.Sum(section => (long)section.NeuronSpan);
                WorkbenchLog.Info(
                    $"SpawnDiag designBrain={designBrainId:D} artifactSha={artifactSha} artifactRoot={brainArtifactRoot} regions={regionCount} neurons={neuronCount} shardPlan={sharedPlanMode} plannedShards={plannedShardCount}");
            }

            Status = "Spawning brain via IO/HiveMind worker placement...";
            var spawnAck = await _client.SpawnBrainViaIoAsync(new ProtoControl.SpawnBrain
            {
                BrainDef = artifactRef
            }).ConfigureAwait(false);

            if (spawnAck?.BrainId is null
                || !spawnAck.BrainId.TryToGuid(out var spawnedBrainId)
                || spawnedBrainId == Guid.Empty)
            {
                Status = SpawnFailureFormatter.Format(
                    prefix: "Spawn failed",
                    ack: spawnAck,
                    fallbackMessage: "Spawn failed: IO did not return a brain id.");
                return;
            }

            Status = "Waiting for brain registration after IO/HiveMind worker placement...";
            if (!await WaitForBrainRegistrationAsync(spawnedBrainId).ConfigureAwait(false))
            {
                Status = $"Spawn failed: brain {spawnedBrainId:D} did not register after IO/HiveMind worker placement.";
                await _client.KillBrainAsync(spawnedBrainId, "designer_managed_spawn_registration_timeout").ConfigureAwait(false);
                return;
            }

            _spawnedBrains[designBrainId] = DesignerSpawnState.Create(spawnedBrainId);
            if (LogSpawnDiagnostics && WorkbenchLog.Enabled)
            {
                WorkbenchLog.Info(
                    $"SpawnDiag runtimeBrain={spawnedBrainId:D} designBrain={designBrainId:D} status=registered");
            }
            Status = $"Brain spawned ({spawnedBrainId:D}). Spawned via IO; worker placement managed by HiveMind.";
            if (shardPlan.Warnings.Count > 0)
            {
                Status = $"{Status} Plan warnings: {string.Join(" ", shardPlan.Warnings)}";
            }
        }
        catch (Exception ex)
        {
            Status = $"Spawn failed: {ex.Message}";
        }
    }

    private void Validate()
    {
        if (_documentType == DesignerDocumentType.None)
        {
            Status = "Nothing to validate.";
            return;
        }

        NbnValidationResult result;
        switch (_documentType)
        {
            case DesignerDocumentType.Nbn:
                if (!TryBuildNbn(out var header, out var regions, out var error))
                {
                    Status = error ?? "Validation failed.";
                    return;
                }

                result = NbnBinaryValidator.ValidateNbn(header, regions);
                break;
            case DesignerDocumentType.Nbs:
                if (_nbsHeader is null || _nbsRegions is null)
                {
                    Status = "NBS not loaded.";
                    return;
                }

                result = NbnBinaryValidator.ValidateNbs(_nbsHeader, _nbsRegions, _nbsOverlay);
                break;
            default:
                Status = "Validation not available.";
                return;
        }

        ValidationIssues.Clear();
        foreach (var issue in result.Issues)
        {
            ValidationIssues.Add(issue.ToString());
        }

        _validationHasRun = true;
        _validationPassed = result.IsValid;
        ValidationSummary = result.IsValid
            ? "Validation passed."
            : $"Validation found {result.Issues.Count} issue(s).";
        Status = "Validation complete.";
    }

    private void AddNeuron()
    {
        if (SelectedRegion is null || Brain is null)
        {
            return;
        }

        var neuron = CreateDefaultNeuron(SelectedRegion, SelectedRegion.Neurons.Count);
        SelectedRegion.Neurons.Add(neuron);
        SelectedRegion.UpdateCounts();
        UpdateRegionSizeText();
        Brain.UpdateTotals();
        EnsureNeuronVisible(neuron);
        SelectNeuron(neuron);
        UpdateLoadedSummary();
        MarkDesignDirty();
        RefreshRegionView();
        Status = $"Neuron {neuron.NeuronId} added to region {SelectedRegion.RegionId}.";
    }

    private void ToggleNeuronEnabled()
    {
        if (SelectedNeuron is null || SelectedRegion is null)
        {
            return;
        }

        if (SelectedNeuron.IsRequired)
        {
            Status = "Input and output neurons cannot be disabled.";
            return;
        }

        SelectedNeuron.Exists = !SelectedNeuron.Exists;
        if (!SelectedNeuron.Exists)
        {
            RemoveInboundAxons(SelectedRegion.RegionId, neuronId => neuronId == SelectedNeuron.NeuronId);
            SelectedNeuron.Axons.Clear();
            SelectedNeuron.UpdateAxonCount();
            SelectedAxon = null;
        }

        SelectedRegion.UpdateCounts();
        Brain?.UpdateTotals();
        RefreshRegionView();
        UpdateLoadedSummary();
        MarkDesignDirty();
        Status = SelectedNeuron.Exists
            ? $"Neuron {SelectedNeuron.NeuronId} re-enabled."
            : $"Neuron {SelectedNeuron.NeuronId} disabled.";
    }

    private void ClearSelection()
    {
        SelectNeuron(null);
        SelectAxon(null);
        RefreshEdges();
    }

    private void RemoveSelectedAxon()
    {
        if (SelectedNeuron is null || SelectedAxon is null || SelectedRegion is null)
        {
            return;
        }

        SelectedNeuron.Axons.Remove(SelectedAxon);
        SelectedNeuron.UpdateAxonCount();
        SelectedRegion.UpdateCounts();
        Brain?.UpdateTotals();
        UpdateLoadedSummary();
        MarkDesignDirty();
        Status = "Axon removed.";
        SelectAxon(null);
        RefreshEdges();
    }

    private void RandomizeSeed()
    {
        if (Brain is null)
        {
            return;
        }

        Brain.SetSeed(GenerateSeed());
        UpdateLoadedSummary();
        MarkDesignDirty();
        Status = "Brain seed randomized.";
    }

    private void RandomizeBrainId()
    {
        if (Brain is null)
        {
            return;
        }

        Brain.SetBrainId(Guid.NewGuid());
        MarkDesignDirty();
        Status = "Brain ID randomized.";
    }

    private void SelectRegion(DesignerRegionViewModel? region)
    {
        if (region is null)
        {
            if (SelectedRegion is not null)
            {
                SelectedRegion.IsSelected = false;
                SelectedRegion = null;
            }

            return;
        }

        if (SelectedRegion == region)
        {
            return;
        }

        if (SelectedRegion is not null)
        {
            SelectedRegion.IsSelected = false;
        }

        SelectedRegion = region;
        SelectedRegion.IsSelected = true;
        SelectNeuron(null);
    }

    private void SelectNeuron(DesignerNeuronViewModel? neuron)
    {
        if (SelectedNeuron == neuron)
        {
            return;
        }

        if (SelectedNeuron is not null)
        {
            SelectedNeuron.IsSelected = false;
        }

        SelectedNeuron = neuron;
        if (SelectedNeuron is not null)
        {
            SelectedNeuron.IsSelected = true;
            EnsureNeuronVisible(SelectedNeuron);
        }

        SelectAxon(null);
    }

    private void SelectAxon(DesignerAxonViewModel? axon)
    {
        if (SelectedAxon == axon)
        {
            return;
        }

        if (SelectedAxon is not null)
        {
            SelectedAxon.IsSelected = false;
        }

        SelectedAxon = axon;
        if (SelectedAxon is not null)
        {
            SelectedAxon.IsSelected = true;
        }

        RefreshEdges();
    }

    private bool TryAddAxon(DesignerNeuronViewModel source, DesignerNeuronViewModel target, out string? message)
    {
        message = null;
        if (SelectedRegion is null)
        {
            message = "Select a region first.";
            return false;
        }

        if (!source.Exists)
        {
            message = "Source neuron is disabled.";
            return false;
        }

        if (!target.Exists)
        {
            message = "Target neuron is disabled.";
            return false;
        }

        if (target.RegionId == NbnConstants.InputRegionId)
        {
            message = "Axons cannot target the input region.";
            return false;
        }

        if (source.RegionId == NbnConstants.OutputRegionId && target.RegionId == NbnConstants.OutputRegionId)
        {
            message = "Output region neurons cannot target output region.";
            return false;
        }

        if (source.Axons.Count >= NbnConstants.MaxAxonsPerNeuron)
        {
            message = "Source neuron already has max axons.";
            return false;
        }

        if (source.Axons.Any(axon => axon.TargetRegionId == target.RegionId && axon.TargetNeuronId == target.NeuronId))
        {
            message = "Duplicate axon not allowed.";
            return false;
        }

        var axon = new DesignerAxonViewModel(target.RegionId, target.NeuronId, DefaultAxonStrength);
        source.Axons.Add(axon);
        source.UpdateAxonCount();

        var region = Brain?.Regions.FirstOrDefault(r => r.RegionId == source.RegionId);
        region?.UpdateCounts();
        Brain?.UpdateTotals();
        UpdateLoadedSummary();
        MarkDesignDirty();
        SelectNeuron(source);
        SelectAxon(axon);
        message = $"Axon added: R{source.RegionId} N{source.NeuronId} -> R{target.RegionId} N{target.NeuronId}.";
        RefreshEdges();
        return true;
    }

    private void ApplyRegionSize()
    {
        if (SelectedRegion is null || Brain is null)
        {
            return;
        }

        if (!int.TryParse(RegionSizeText, out var targetCount) || targetCount < 0)
        {
            Status = "Region size must be a non-negative integer.";
            return;
        }

        if ((SelectedRegion.IsInput || SelectedRegion.IsOutput) && targetCount < 1)
        {
            Status = "Input/output regions must have at least one neuron.";
            return;
        }

        var current = SelectedRegion.Neurons.Count;
        if (targetCount == current)
        {
            Status = "Region size already set.";
            return;
        }

        if (targetCount > current)
        {
            for (var i = current; i < targetCount; i++)
            {
                SelectedRegion.Neurons.Add(CreateDefaultNeuron(SelectedRegion, i));
            }

            SelectedRegion.UpdateCounts();
            RegionSizeText = targetCount.ToString();
            Brain.UpdateTotals();
            UpdateLoadedSummary();
            MarkDesignDirty();
            EnsureNeuronVisible(SelectedRegion.Neurons.Last());
            Status = $"Region {SelectedRegion.RegionId} expanded to {targetCount} neurons.";
            RefreshRegionView();
            return;
        }

        RemoveInboundAxons(SelectedRegion.RegionId, neuronId => neuronId >= targetCount);
        for (var i = current - 1; i >= targetCount; i--)
        {
            SelectedRegion.Neurons.RemoveAt(i);
        }

        if (SelectedNeuron is not null && SelectedNeuron.RegionId == SelectedRegion.RegionId && SelectedNeuron.NeuronId >= targetCount)
        {
            SelectNeuron(null);
        }

        SelectedRegion.UpdateCounts();
        RegionSizeText = targetCount.ToString();
        Brain.UpdateTotals();
        UpdateLoadedSummary();
        MarkDesignDirty();
        RefreshRegionView();
        Status = targetCount == 0
            ? $"Region {SelectedRegion.RegionId} cleared."
            : $"Region {SelectedRegion.RegionId} trimmed to {targetCount} neurons.";
    }

    private void PreviousRegionPage()
    {
        SetRegionPageIndex(RegionPageIndex - 1);
    }

    private void NextRegionPage()
    {
        SetRegionPageIndex(RegionPageIndex + 1);
    }

    private void FirstRegionPage()
    {
        SetRegionPageIndex(0);
    }

    private void LastRegionPage()
    {
        SetRegionPageIndex(RegionPageCount - 1);
    }

    private void JumpToNeuron()
    {
        if (SelectedRegion is null)
        {
            Status = "Select a region first.";
            return;
        }

        if (!int.TryParse(JumpNeuronIdText, out var neuronId))
        {
            Status = "Neuron ID must be a number.";
            return;
        }

        if (neuronId < 0 || neuronId >= SelectedRegion.Neurons.Count)
        {
            Status = "Neuron ID is out of range.";
            return;
        }

        var neuron = SelectedRegion.Neurons[neuronId];
        EnsureNeuronVisible(neuron);
        SelectNeuron(neuron);
        Status = $"Focused neuron {neuron.NeuronId} in region {SelectedRegion.RegionId}.";
    }

    private void FocusEdgeEndpoint(DesignerEdgeViewModel? edge)
    {
        if (edge is null || !edge.CanNavigate || Brain is null)
        {
            return;
        }

        var targetRegionId = edge.NavigationRegionId;
        var targetNeuronId = edge.NavigationNeuronId;
        if (!targetRegionId.HasValue || !targetNeuronId.HasValue)
        {
            return;
        }

        var region = Brain.Regions.FirstOrDefault(candidate => candidate.RegionId == targetRegionId.Value);
        if (region is null || targetNeuronId.Value < 0 || targetNeuronId.Value >= region.NeuronCount)
        {
            Status = "Edge endpoint is no longer available.";
            return;
        }

        var neuron = region.Neurons[targetNeuronId.Value];
        SelectRegion(region);
        EnsureNeuronVisible(neuron);
        SelectNeuron(neuron);
        Status = $"Focused edge endpoint R{region.RegionId} N{neuron.NeuronId}.";
    }

    private void AddAxonById()
    {
        if (Brain is null || SelectedNeuron is null)
        {
            return;
        }

        if (!int.TryParse(AxonTargetRegionText, out var targetRegionId))
        {
            Status = "Target region must be a number.";
            return;
        }

        if (!int.TryParse(AxonTargetNeuronText, out var targetNeuronId))
        {
            Status = "Target neuron must be a number.";
            return;
        }

        var region = Brain.Regions.FirstOrDefault(r => r.RegionId == targetRegionId);
        if (region is null || region.NeuronCount == 0)
        {
            Status = "Target region is empty or missing.";
            return;
        }

        if (targetNeuronId < 0 || targetNeuronId >= region.Neurons.Count)
        {
            Status = "Target neuron is out of range.";
            return;
        }

        var targetNeuron = region.Neurons[targetNeuronId];
        if (!targetNeuron.Exists)
        {
            Status = "Target neuron is disabled.";
            return;
        }

        if (TryAddAxon(SelectedNeuron, targetNeuron, out var message))
        {
            Status = message ?? "Axon added.";
        }
        else
        {
            Status = message ?? "Unable to add axon.";
        }
    }

    private void MarkDesignDirty()
    {
        ClearResetConfirmation();
        SetDesignDirty(true);
        ResetValidation();
    }

    private void SetRegionPageIndex(int index)
    {
        var clamped = Math.Clamp(index, 0, Math.Max(RegionPageCount - 1, 0));
        if (RegionPageIndex == clamped)
        {
            RegionPageIndexText = (RegionPageIndex + 1).ToString();
            UpdateRegionPageSummary();
            return;
        }

        RegionPageIndex = clamped;
        RefreshRegionView();
    }

    private void UpdateRegionPageSummary()
    {
        if (SelectedRegion is null)
        {
            RegionPageSummary = "No region selected.";
            return;
        }

        var total = SelectedRegion.Neurons.Count;
        if (total == 0)
        {
            RegionPageSummary = "No neurons in region.";
            return;
        }

        var start = RegionPageIndex * _regionPageSize;
        var end = Math.Min(total, start + _regionPageSize);
        RegionPageSummary = $"Showing {start}-{end - 1} of {total}";
    }

    private void OnSelectedRegionNeuronsChanged(object? sender, NotifyCollectionChangedEventArgs e)
    {
        QueueRegionRefresh();
    }

    private void QueueRegionRefresh()
    {
        if (_regionRefreshPending)
        {
            return;
        }

        _regionRefreshPending = true;
        Dispatcher.UIThread.Post(() =>
        {
            _regionRefreshPending = false;
            RefreshRegionView();
        }, DispatcherPriority.Background);
    }

    private void RefreshRegionView()
    {
        VisibleNeurons.Clear();

        if (SelectedRegion is null)
        {
            CanvasWidth = 0;
            CanvasHeight = 0;
            EdgeSummary = string.Empty;
            return;
        }

        var total = SelectedRegion.Neurons.Count;
        RegionPageCount = Math.Max(1, (int)Math.Ceiling(total / (double)_regionPageSize));
        UpdateCommandStates();
        if (RegionPageIndex >= RegionPageCount)
        {
            RegionPageIndex = Math.Max(RegionPageCount - 1, 0);
        }

        var startIndex = RegionPageIndex * _regionPageSize;
        var endIndex = Math.Min(total, startIndex + _regionPageSize);

        for (var i = startIndex; i < endIndex; i++)
        {
            VisibleNeurons.Add(SelectedRegion.Neurons[i]);
        }

        UpdateCanvasLayout();
        UpdateRegionPageSummary();
        RefreshEdges();
    }

    private void UpdateCanvasLayout()
    {
        var count = VisibleNeurons.Count;
        if (count == 0)
        {
            CanvasWidth = 0;
            CanvasHeight = 0;
            return;
        }

        var nodeSize = CanvasNodeSize;
        var gap = CanvasNodeGap;
        var visibleIds = VisibleNeurons.Select(neuron => neuron.NeuronId).ToHashSet();
        var internalOut = new Dictionary<int, int>(count);
        var internalIn = new Dictionary<int, int>(count);
        var externalOut = new Dictionary<int, int>(count);
        var externalIn = new Dictionary<int, int>(count);
        foreach (var neuron in VisibleNeurons)
        {
            internalOut[neuron.NeuronId] = 0;
            internalIn[neuron.NeuronId] = 0;
            externalOut[neuron.NeuronId] = 0;
            externalIn[neuron.NeuronId] = 0;
        }

        var selectedRegionId = SelectedRegion?.RegionId ?? -1;
        foreach (var neuron in VisibleNeurons)
        {
            foreach (var axon in neuron.Axons)
            {
                if (axon.TargetRegionId == selectedRegionId && visibleIds.Contains(axon.TargetNeuronId))
                {
                    internalOut[neuron.NeuronId]++;
                    if (internalIn.TryGetValue(axon.TargetNeuronId, out var inbound))
                    {
                        internalIn[axon.TargetNeuronId] = inbound + 1;
                    }
                }
                else
                {
                    externalOut[neuron.NeuronId]++;
                }
            }
        }

        if (Brain is not null && selectedRegionId >= 0)
        {
            foreach (var sourceRegion in Brain.Regions)
            {
                foreach (var sourceNeuron in sourceRegion.Neurons)
                {
                    if (!sourceNeuron.Exists)
                    {
                        continue;
                    }

                    foreach (var axon in sourceNeuron.Axons)
                    {
                        if (axon.TargetRegionId != selectedRegionId || !visibleIds.Contains(axon.TargetNeuronId))
                        {
                            continue;
                        }

                        var isVisibleInternalSource = sourceRegion.RegionId == selectedRegionId
                                                      && visibleIds.Contains(sourceNeuron.NeuronId);
                        if (!isVisibleInternalSource && externalIn.TryGetValue(axon.TargetNeuronId, out var inbound))
                        {
                            externalIn[axon.TargetNeuronId] = inbound + 1;
                        }
                    }
                }
            }
        }

        var ordered = VisibleNeurons
            .OrderByDescending(neuron =>
                (internalOut[neuron.NeuronId] * 3)
                + (internalIn[neuron.NeuronId] * 3)
                + (externalOut[neuron.NeuronId] * 2)
                + (externalIn[neuron.NeuronId] * 2)
                + neuron.AxonCount)
            .ThenBy(neuron => neuron.NeuronId)
            .ToList();

        var ringAssignments = new List<(DesignerNeuronViewModel neuron, int ring, int slot, int slotCount)>(count);
        var index = 0;
        var ring = 0;
        while (index < ordered.Count)
        {
            var ringCapacity = ring == 0 ? 1 : Math.Max(8, ring * 12);
            var slotCount = Math.Min(ringCapacity, ordered.Count - index);
            for (var slot = 0; slot < slotCount; slot++)
            {
                ringAssignments.Add((ordered[index + slot], ring, slot, slotCount));
            }

            index += slotCount;
            ring++;
        }

        var maxRing = ringAssignments.Count == 0 ? 0 : ringAssignments.Max(entry => entry.ring);
        var ringSpacing = nodeSize + (gap * 1.5);
        var radius = (maxRing * ringSpacing) + nodeSize;

        CanvasWidth = Math.Max(MinCanvasWidth, (radius * 2) + (CanvasPadding * 2));
        CanvasHeight = Math.Max(MinCanvasHeight, (radius * 2) + (CanvasPadding * 2));

        var centerX = CanvasWidth / 2.0;
        var centerY = CanvasHeight / 2.0;
        foreach (var assignment in ringAssignments)
        {
            double x;
            double y;
            if (assignment.ring == 0)
            {
                x = centerX - (nodeSize / 2.0);
                y = centerY - (nodeSize / 2.0);
            }
            else
            {
                var angle = ((Math.PI * 2.0) * assignment.slot / Math.Max(1, assignment.slotCount)) - (Math.PI / 2.0);
                var ringRadius = assignment.ring * ringSpacing;
                x = centerX + (Math.Cos(angle) * ringRadius) - (nodeSize / 2.0);
                y = centerY + (Math.Sin(angle) * ringRadius) - (nodeSize / 2.0);
            }

            assignment.neuron.CanvasX = Clamp(x, CanvasPadding, Math.Max(CanvasPadding, CanvasWidth - CanvasPadding - nodeSize));
            assignment.neuron.CanvasY = Clamp(y, CanvasPadding, Math.Max(CanvasPadding, CanvasHeight - CanvasPadding - nodeSize));
        }
    }

    private void RefreshEdges()
    {
        VisibleEdges.Clear();
        EdgeSummary = string.Empty;
        EdgeAnalyticsSummary = "Select a neuron to inspect edge analytics.";

        if (SelectedRegion is null || VisibleNeurons.Count == 0)
        {
            return;
        }

        var source = SelectedNeuron;

        if (source is null || source.RegionId != SelectedRegion.RegionId)
        {
            return;
        }

        var positions = new Dictionary<int, Point>();
        foreach (var neuron in VisibleNeurons)
        {
            positions[neuron.NeuronId] = new Point(neuron.CanvasX + CanvasNodeRadius, neuron.CanvasY + CanvasNodeRadius);
        }

        if (!positions.TryGetValue(source.NeuronId, out var start))
        {
            return;
        }

        var offPageRadius = ComputeOffPageEdgeRadius(start, positions);

        var visibleOutbound = 0;
        var offPageOutbound = 0;
        var visibleInbound = 0;
        var offPageInbound = 0;

        var visibleOutboundTargets = new List<(DesignerAxonViewModel Axon, Point End, bool IsSelected)>();
        var visibleInboundSources = new List<(int SourceRegionId, int SourceNeuronId, Point Start)>();
        var offPageOutboundAxons = new List<(DesignerAxonViewModel Axon, bool IsSelected)>();
        var offPageInboundSources = new List<(int SourceRegionId, int SourceNeuronId)>();

        foreach (var axon in source.Axons)
        {
            if (axon.TargetRegionId != SelectedRegion.RegionId)
            {
                offPageOutbound++;
                offPageOutboundAxons.Add((axon, SelectedAxon is not null
                    && SelectedAxon.TargetRegionId == axon.TargetRegionId
                    && SelectedAxon.TargetNeuronId == axon.TargetNeuronId));
                continue;
            }

            if (!positions.TryGetValue(axon.TargetNeuronId, out var end))
            {
                offPageOutbound++;
                offPageOutboundAxons.Add((axon, SelectedAxon is not null
                    && SelectedAxon.TargetRegionId == axon.TargetRegionId
                    && SelectedAxon.TargetNeuronId == axon.TargetNeuronId));
                continue;
            }

            var isSelected = SelectedAxon is not null
                && SelectedAxon.TargetRegionId == axon.TargetRegionId
                && SelectedAxon.TargetNeuronId == axon.TargetNeuronId;

            visibleOutboundTargets.Add((axon, end, isSelected));
        }

        for (var i = 0; i < visibleOutboundTargets.Count; i++)
        {
            var entry = visibleOutboundTargets[i];
            VisibleEdges.Add(new DesignerEdgeViewModel(
                start,
                entry.End,
                false,
                entry.IsSelected,
                DesignerEdgeKind.OutboundInternal,
                bundleIndex: i,
                bundleCount: visibleOutboundTargets.Count));
            visibleOutbound++;
        }

        if (Brain is not null)
        {
            foreach (var sourceRegion in Brain.Regions)
            {
                foreach (var sourceNeuron in sourceRegion.Neurons)
                {
                    if (!sourceNeuron.Exists)
                    {
                        continue;
                    }

                    if (!sourceNeuron.Axons.Any(axon => axon.TargetRegionId == source.RegionId && axon.TargetNeuronId == source.NeuronId))
                    {
                        continue;
                    }

                    if (sourceRegion.RegionId == source.RegionId && positions.TryGetValue(sourceNeuron.NeuronId, out var inboundStart))
                    {
                        if (sourceNeuron.NeuronId == source.NeuronId)
                        {
                            continue;
                        }

                        visibleInboundSources.Add((sourceRegion.RegionId, sourceNeuron.NeuronId, inboundStart));
                    }
                    else
                    {
                        offPageInbound++;
                        offPageInboundSources.Add((sourceRegion.RegionId, sourceNeuron.NeuronId));
                    }
                }
            }
        }

        for (var i = 0; i < visibleInboundSources.Count; i++)
        {
            var entry = visibleInboundSources[i];
            VisibleEdges.Add(new DesignerEdgeViewModel(
                entry.Start,
                start,
                false,
                false,
                DesignerEdgeKind.InboundInternal,
                bundleIndex: i,
                bundleCount: visibleInboundSources.Count));
            visibleInbound++;
        }

        if (offPageOutboundAxons.Count > 0)
        {
            for (var i = 0; i < offPageOutboundAxons.Count; i++)
            {
                var entry = offPageOutboundAxons[i];
                var angle = BuildArcAngle(i, offPageOutboundAxons.Count, -0.55 * Math.PI, 0.35 * Math.PI);
                var endPoint = ClampToCanvas(
                    new Point(start.X + (Math.Cos(angle) * offPageRadius), start.Y + (Math.Sin(angle) * offPageRadius)),
                    CanvasWidth,
                    CanvasHeight,
                    OffPageEdgeCanvasMargin);
                var labelText = BuildOutboundOffPageLabel(entry.Axon);
                var labelPoint = new Point(endPoint.X + 4, endPoint.Y - 6);
                labelPoint = ClampToCanvas(labelPoint, CanvasWidth, CanvasHeight, OffPageEdgeCanvasMargin);
                VisibleEdges.Add(new DesignerEdgeViewModel(
                    start,
                    endPoint,
                    false,
                    entry.IsSelected,
                    DesignerEdgeKind.OutboundExternal,
                    labelText,
                    labelPoint,
                    i,
                    offPageOutboundAxons.Count,
                    entry.Axon.TargetRegionId,
                    entry.Axon.TargetNeuronId));
            }
        }

        if (offPageInboundSources.Count > 0)
        {
            for (var i = 0; i < offPageInboundSources.Count; i++)
            {
                var entry = offPageInboundSources[i];
                var angle = BuildArcAngle(i, offPageInboundSources.Count, 0.55 * Math.PI, 1.45 * Math.PI);
                var endPoint = ClampToCanvas(
                    new Point(start.X + (Math.Cos(angle) * offPageRadius), start.Y + (Math.Sin(angle) * offPageRadius)),
                    CanvasWidth,
                    CanvasHeight,
                    OffPageEdgeCanvasMargin);
                var labelText = BuildInboundOffPageLabel(entry.SourceRegionId, entry.SourceNeuronId);
                var labelPoint = new Point(endPoint.X - 38, endPoint.Y - 6);
                labelPoint = ClampToCanvas(labelPoint, CanvasWidth, CanvasHeight, OffPageEdgeCanvasMargin);
                VisibleEdges.Add(new DesignerEdgeViewModel(
                    endPoint,
                    start,
                    false,
                    false,
                    DesignerEdgeKind.InboundExternal,
                    labelText,
                    labelPoint,
                    i,
                    offPageInboundSources.Count,
                    entry.SourceRegionId,
                    entry.SourceNeuronId));
            }
        }

        var summary = source.Axons.Count == 0 && visibleInbound == 0 && offPageInbound == 0
            ? "No inbound or outbound axons."
            : $"Out: {visibleOutbound} visible, {offPageOutbound} external/off-page. In: {visibleInbound} visible, {offPageInbound} external/off-page.";
        if (offPageOutbound + offPageInbound > 0)
        {
            summary += " Click off-page labels to jump.";
        }

        EdgeSummary = summary;
        EdgeAnalyticsSummary = BuildEdgeAnalyticsSummary(source, visibleOutbound, offPageOutbound, visibleInbound, offPageInbound);
    }

    private string BuildEdgeAnalyticsSummary(
        DesignerNeuronViewModel source,
        int visibleOutbound,
        int offPageOutbound,
        int visibleInbound,
        int offPageInbound)
    {
        var totalOutbound = visibleOutbound + offPageOutbound;
        var totalInbound = visibleInbound + offPageInbound;
        var totalEdges = totalOutbound + totalInbound;
        if (totalEdges == 0)
        {
            return "Density: idle (0 edges).";
        }

        var externalEdges = offPageOutbound + offPageInbound;
        var externalPct = (externalEdges * 100.0) / totalEdges;
        var pressure = totalEdges switch
        {
            < 8 => "light",
            < 20 => "moderate",
            < 45 => "dense",
            _ => "saturated"
        };

        var regionTotal = Math.Max(1, SelectedRegion?.NeuronCount ?? 0);
        var pageCoveragePct = (VisibleNeurons.Count * 100.0) / regionTotal;
        var dominantTargetGroup = source.Axons
            .GroupBy(axon => axon.TargetRegionId)
            .OrderByDescending(group => group.Count())
            .ThenBy(group => group.Key)
            .FirstOrDefault();
        var dominantTarget = dominantTargetGroup is null
            ? "none"
            : $"R{dominantTargetGroup.Key} ({dominantTargetGroup.Count()})";

        return $"Density: {pressure} ({totalEdges} edges). External/off-page: {externalPct:0.#}% ({externalEdges}). "
             + $"Page coverage: {pageCoveragePct:0.#}% ({VisibleNeurons.Count}/{regionTotal}). Dominant target: {dominantTarget}.";
    }

    private static double BuildArcAngle(int index, int total, double startAngle, double endAngle)
    {
        if (total <= 1)
        {
            return (startAngle + endAngle) / 2.0;
        }

        var t = index / (double)(total - 1);
        return startAngle + ((endAngle - startAngle) * t);
    }

    private string BuildOutboundOffPageLabel(DesignerAxonViewModel axon)
    {
        if (SelectedRegion is null || axon.TargetRegionId != SelectedRegion.RegionId)
        {
            return $"-> R{axon.TargetRegionId} N{axon.TargetNeuronId}";
        }

        var page = _regionPageSize == 0 ? 0 : axon.TargetNeuronId / _regionPageSize;
        return $"-> P{page + 1} N{axon.TargetNeuronId}";
    }

    private string BuildInboundOffPageLabel(int sourceRegionId, int sourceNeuronId)
    {
        if (SelectedRegion is null || sourceRegionId != SelectedRegion.RegionId)
        {
            return $"<- R{sourceRegionId} N{sourceNeuronId}";
        }

        var page = _regionPageSize == 0 ? 0 : sourceNeuronId / _regionPageSize;
        return $"<- P{page + 1} N{sourceNeuronId}";
    }

    private double ComputeOffPageEdgeRadius(Point start, IReadOnlyDictionary<int, Point> positions)
    {
        var minRadius = CanvasNodeRadius + OffPageEdgeFallbackRadiusPadding;
        foreach (var center in positions.Values)
        {
            var dx = center.X - start.X;
            var dy = center.Y - start.Y;
            var centerDistance = Math.Sqrt((dx * dx) + (dy * dy));
            var edgeDistance = centerDistance + CanvasNodeRadius + OffPageEdgeOuterMargin;
            if (edgeDistance > minRadius)
            {
                minRadius = edgeDistance;
            }
        }

        var maxRadius = Math.Min(
            Math.Min(start.X - OffPageEdgeCanvasMargin, CanvasWidth - start.X - OffPageEdgeCanvasMargin),
            Math.Min(start.Y - OffPageEdgeCanvasMargin, CanvasHeight - start.Y - OffPageEdgeCanvasMargin));
        var floorRadius = CanvasNodeRadius + OffPageEdgeFallbackRadiusPadding;
        if (maxRadius <= floorRadius)
        {
            return floorRadius;
        }

        if (minRadius > maxRadius)
        {
            return maxRadius;
        }

        return minRadius;
    }

    private static Point ClampToCanvas(Point point, double width, double height, double margin = 10)
    {
        var normalizedMargin = Math.Max(0, margin);
        var minX = Math.Min(normalizedMargin, Math.Max(0, width));
        var minY = Math.Min(normalizedMargin, Math.Max(0, height));
        var maxX = Math.Max(minX, width - normalizedMargin);
        var maxY = Math.Max(minY, height - normalizedMargin);
        return new Point(
            Clamp(point.X, minX, maxX),
            Clamp(point.Y, minY, maxY));
    }

    private void EnsureNeuronVisible(DesignerNeuronViewModel neuron)
    {
        if (SelectedRegion is null)
        {
            return;
        }

        if (neuron.RegionId != SelectedRegion.RegionId)
        {
            var region = Brain?.Regions.FirstOrDefault(r => r.RegionId == neuron.RegionId);
            if (region is not null)
            {
                SelectRegion(region);
            }
        }

        var targetPage = _regionPageSize == 0 ? 0 : neuron.NeuronId / _regionPageSize;
        if (targetPage != RegionPageIndex)
        {
            SetRegionPageIndex(targetPage);
        }
    }

    private void UpdateRegionSizeText()
    {
        RegionSizeText = SelectedRegion?.NeuronCount.ToString() ?? "0";
    }

    private void UpdateJumpNeuronText()
    {
        if (SelectedNeuron is not null)
        {
            JumpNeuronIdText = SelectedNeuron.NeuronId.ToString();
        }
        else if (SelectedRegion is not null)
        {
            JumpNeuronIdText = "0";
        }
    }

    private void RemoveInboundAxons(int targetRegionId, Func<int, bool> neuronPredicate)
    {
        if (Brain is null)
        {
            return;
        }

        foreach (var region in Brain.Regions)
        {
            foreach (var neuron in region.Neurons)
            {
                if (neuron.Axons.Count == 0)
                {
                    continue;
                }

                var removed = 0;
                for (var i = neuron.Axons.Count - 1; i >= 0; i--)
                {
                    var axon = neuron.Axons[i];
                    if (axon.TargetRegionId == targetRegionId && neuronPredicate(axon.TargetNeuronId))
                    {
                        if (SelectedAxon == axon)
                        {
                            SelectedAxon = null;
                        }

                        neuron.Axons.RemoveAt(i);
                        removed++;
                    }
                }

                if (removed > 0)
                {
                    neuron.UpdateAxonCount();
                }
            }

            region.UpdateCounts();
        }

        Brain.UpdateTotals();
    }

    private bool TryBuildSnapshot(out byte[] snapshotBytes, out string? error)
    {
        snapshotBytes = Array.Empty<byte>();
        error = null;

        if (!TryBuildNbn(out var header, out var sections, out var buildError))
        {
            error = buildError ?? "Unable to build base NBN.";
            return false;
        }

        if (Brain is null)
        {
            error = "No design loaded.";
            return false;
        }

        if (!ulong.TryParse(SnapshotTickText, out var tickId))
        {
            error = "Snapshot tick must be a number.";
            return false;
        }

        if (!long.TryParse(SnapshotEnergyText, out var energy))
        {
            error = "Snapshot energy must be a number.";
            return false;
        }

        var nbnBytes = NbnBinary.WriteNbn(header, sections);
        var hash = SHA256.HashData(nbnBytes);
        var flags = 0u;
        if (SnapshotIncludeEnabledBitset)
        {
            flags |= 0x1u;
        }

        if (SnapshotCostEnergyEnabled)
        {
            flags |= 0x4u;
            flags |= 0x8u;
        }

        if (SnapshotPlasticityEnabled)
        {
            flags |= 0x10u;
        }

        var regions = new List<NbsRegionSection>();
        foreach (var region in Brain.Regions)
        {
            if (region.NeuronCount == 0)
            {
                continue;
            }

            var buffer = new short[region.NeuronCount];
            byte[]? enabledBitset = null;
            if (SnapshotIncludeEnabledBitset)
            {
                enabledBitset = new byte[(region.NeuronCount + 7) / 8];
                for (var i = 0; i < region.NeuronCount; i++)
                {
                    if (region.Neurons[i].Exists)
                    {
                        enabledBitset[i / 8] |= (byte)(1 << (i % 8));
                    }
                }
            }

            regions.Add(new NbsRegionSection((byte)region.RegionId, (uint)region.NeuronCount, buffer, enabledBitset));
        }

        var headerNbs = new NbsHeaderV2(
            "NBS2",
            2,
            1,
            9,
            Brain.BrainId,
            tickId,
            (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            energy,
            hash,
            flags,
            QuantizationSchemas.DefaultBuffer);

        snapshotBytes = NbnBinary.WriteNbs(headerNbs, regions);
        return true;
    }

    private void OnBrainPropertyChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (e.PropertyName is nameof(DesignerBrainViewModel.Name)
            or nameof(DesignerBrainViewModel.BrainSeed)
            or nameof(DesignerBrainViewModel.BrainSeedText)
            or nameof(DesignerBrainViewModel.AxonStride)
            or nameof(DesignerBrainViewModel.AxonStrideText)
            or nameof(DesignerBrainViewModel.BrainId)
            or nameof(DesignerBrainViewModel.BrainIdText))
        {
            UpdateLoadedSummary();
            MarkDesignDirty();
        }
    }

    private void OnSelectedNeuronChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (_suppressNeuronConstraintEnforcement)
        {
            return;
        }

        if (e.PropertyName is nameof(DesignerNeuronViewModel.ActivationFunctionId)
            or nameof(DesignerNeuronViewModel.ResetFunctionId)
            or nameof(DesignerNeuronViewModel.AccumulationFunctionId)
            or nameof(DesignerNeuronViewModel.ParamACode)
            or nameof(DesignerNeuronViewModel.ParamBCode)
            or nameof(DesignerNeuronViewModel.ActivationThresholdCode)
            or nameof(DesignerNeuronViewModel.PreActivationThresholdCode)
            or nameof(DesignerNeuronViewModel.Exists))
        {
            MarkDesignDirty();

            if ((e.PropertyName == nameof(DesignerNeuronViewModel.ActivationFunctionId)
                 || e.PropertyName == nameof(DesignerNeuronViewModel.ResetFunctionId))
                && SelectedNeuron is not null
                && NormalizeNeuronFunctionConstraints(SelectedNeuron, out var statusMessage)
                && !string.IsNullOrWhiteSpace(statusMessage))
            {
                Status = statusMessage;
            }

            if (e.PropertyName == nameof(DesignerNeuronViewModel.ActivationFunctionId))
            {
                OnPropertyChanged(nameof(SelectedNeuronUsesParamA));
                OnPropertyChanged(nameof(SelectedNeuronUsesParamB));
                OnPropertyChanged(nameof(SelectedActivationDescription));
                OnPropertyChanged(nameof(SelectedNeuronConstraintHint));
            }

            if (e.PropertyName == nameof(DesignerNeuronViewModel.ResetFunctionId))
            {
                OnPropertyChanged(nameof(SelectedResetDescription));
                OnPropertyChanged(nameof(SelectedNeuronConstraintHint));
            }

            if (e.PropertyName == nameof(DesignerNeuronViewModel.AccumulationFunctionId))
            {
                OnPropertyChanged(nameof(SelectedAccumulationDescription));
            }
        }
    }

    private void OnSelectedAxonChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (e.PropertyName is nameof(DesignerAxonViewModel.StrengthCode))
        {
            MarkDesignDirty();
            RefreshEdges();
        }
    }

    private void UpdateCommandStates()
    {
        AddNeuronCommand.RaiseCanExecuteChanged();
        ToggleNeuronEnabledCommand.RaiseCanExecuteChanged();
        ClearSelectionCommand.RaiseCanExecuteChanged();
        RemoveAxonCommand.RaiseCanExecuteChanged();
        RandomizeSeedCommand.RaiseCanExecuteChanged();
        RandomizeBrainIdCommand.RaiseCanExecuteChanged();
        ApplyRegionSizeCommand.RaiseCanExecuteChanged();
        PreviousRegionPageCommand.RaiseCanExecuteChanged();
        NextRegionPageCommand.RaiseCanExecuteChanged();
        FirstRegionPageCommand.RaiseCanExecuteChanged();
        LastRegionPageCommand.RaiseCanExecuteChanged();
        JumpToNeuronCommand.RaiseCanExecuteChanged();
        AddAxonByIdCommand.RaiseCanExecuteChanged();
        ExportSnapshotCommand.RaiseCanExecuteChanged();
        ResetBrainCommand.RaiseCanExecuteChanged();
        SpawnBrainCommand.RaiseCanExecuteChanged();
    }

    private void SetDocumentType(DesignerDocumentType documentType)
    {
        if (_documentType == documentType)
        {
            return;
        }

        _documentType = documentType;
        OnPropertyChanged(nameof(IsDesignLoaded));
        OnPropertyChanged(nameof(IsSnapshotLoaded));
        OnPropertyChanged(nameof(IsDesignVisible));
        OnPropertyChanged(nameof(IsSnapshotVisible));
        OnPropertyChanged(nameof(DesignHint));
        OnPropertyChanged(nameof(CanExportSnapshot));
        OnPropertyChanged(nameof(CanSpawnBrain));
        OnPropertyChanged(nameof(CanResetBrain));
    }

    private void ResetValidation()
    {
        ValidationIssues.Clear();
        ValidationSummary = "Validation not run.";
        _validationHasRun = false;
        _validationPassed = false;
    }

    private static string FormatValidationIssueSummary(NbnValidationResult result)
    {
        if (result.Issues.Count == 0)
        {
            return "Validation failed.";
        }

        var firstIssue = result.Issues[0].ToString();
        return result.Issues.Count == 1
            ? firstIssue
            : $"{firstIssue} (+{result.Issues.Count - 1} more issue(s))";
    }

    private void SetDesignDirty(bool isDirty)
    {
        if (SetProperty(ref _designDirty, isDirty, nameof(IsDesignDirty)))
        {
            if (!isDirty)
            {
                ClearResetConfirmation();
            }
        }
    }

    private void ClearResetConfirmation()
    {
        if (!_resetPending)
        {
            return;
        }

        _resetPending = false;
        OnPropertyChanged(nameof(ResetBrainButtonLabel));
        OnPropertyChanged(nameof(ResetBrainButtonBackground));
        OnPropertyChanged(nameof(ResetBrainButtonForeground));
        OnPropertyChanged(nameof(ResetBrainButtonBorder));
    }

    private void UpdateLoadedSummary()
    {
        if (_documentType == DesignerDocumentType.Nbn && Brain is not null)
        {
            LoadedSummary = BuildDesignSummary(Brain, _documentPath ?? Brain.Name);
        }
        else if (_documentType == DesignerDocumentType.Nbs && _nbsHeader is not null && _nbsRegions is not null)
        {
            LoadedSummary = BuildNbsSummary(_documentPath ?? "Snapshot", _nbsHeader, _nbsRegions, _nbsOverlay);
        }
        else
        {
            LoadedSummary = NoDocumentStatus;
        }
    }

    private bool TryBuildNbn(out NbnHeaderV2 header, out List<NbnRegionSection> sections, out string? error)
    {
        header = null!;
        sections = new List<NbnRegionSection>();
        error = null;

        if (Brain is null)
        {
            error = "No design loaded.";
            return false;
        }

        if (Brain.AxonStride == 0)
        {
            error = "Axon stride must be greater than zero.";
            return false;
        }

        foreach (var region in Brain.Regions)
        {
            region.UpdateCounts();
        }
        Brain.UpdateTotals();

        var directory = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        var regionCounts = Brain.Regions.ToDictionary(region => region.RegionId, region => region.NeuronCount);
        var regionMap = Brain.Regions.ToDictionary(region => region.RegionId, region => region);
        ulong offset = NbnBinary.NbnHeaderBytes;

        for (var i = 0; i < Brain.Regions.Count; i++)
        {
            var region = Brain.Regions[i];
            var neuronSpan = region.NeuronCount;
            if ((region.IsInput || region.IsOutput) && neuronSpan == 0)
            {
                error = "Input and output regions must contain neurons.";
                return false;
            }

            if (neuronSpan == 0)
            {
                directory[region.RegionId] = new NbnRegionDirectoryEntry(0, 0, 0, 0);
                continue;
            }

            if (neuronSpan > NbnConstants.MaxAxonTargetNeuronId)
            {
                error = $"Region {region.RegionId} exceeds max neuron span.";
                return false;
            }

            var neuronRecords = new NeuronRecord[neuronSpan];
            var axonRecords = new List<AxonRecord>();

            for (var neuronIndex = 0; neuronIndex < neuronSpan; neuronIndex++)
            {
                var neuron = region.Neurons[neuronIndex];
                if ((region.IsInput || region.IsOutput) && !neuron.Exists)
                {
                    error = $"Neuron {neuron.NeuronId} in region {region.RegionId} must exist.";
                    return false;
                }

                if (!neuron.Exists && neuron.Axons.Count > 0)
                {
                    error = $"Neuron {neuron.NeuronId} in region {region.RegionId} is disabled but has axons.";
                    return false;
                }

                if (neuron.Axons.Count > NbnConstants.MaxAxonsPerNeuron)
                {
                    error = $"Neuron {neuron.NeuronId} in region {region.RegionId} exceeds max axons.";
                    return false;
                }

                if (!IsActivationFunctionAllowedForRegion(region.RegionId, neuron.ActivationFunctionId))
                {
                    error = region.RegionId == NbnConstants.OutputRegionId
                        ? $"Output neuron {neuron.NeuronId} uses activation {neuron.ActivationFunctionId}, which is not allowed for output region."
                        : region.RegionId == NbnConstants.InputRegionId
                            ? $"Input neuron {neuron.NeuronId} uses activation {neuron.ActivationFunctionId}, which is not allowed for input region."
                            : $"Neuron {neuron.NeuronId} in region {region.RegionId} uses unknown activation function {neuron.ActivationFunctionId}.";
                    return false;
                }

                if (!IsResetFunctionAllowedForRegion(region.RegionId, neuron.ResetFunctionId))
                {
                    error = region.RegionId == NbnConstants.InputRegionId
                        ? $"Input neuron {neuron.NeuronId} uses reset {neuron.ResetFunctionId}, which is not allowed for input region."
                        : $"Neuron {neuron.NeuronId} in region {region.RegionId} uses unknown reset function {neuron.ResetFunctionId}.";
                    return false;
                }

                var targets = new HashSet<(int regionId, int neuronId)>();
                var orderedAxons = neuron.Axons
                    .OrderBy(axon => axon.TargetRegionId)
                    .ThenBy(axon => axon.TargetNeuronId)
                    .ToList();

                foreach (var axon in orderedAxons)
                {
                    if (axon.TargetRegionId == NbnConstants.InputRegionId)
                    {
                        error = "Axons cannot target input region.";
                        return false;
                    }

                    if (region.RegionId == NbnConstants.OutputRegionId && axon.TargetRegionId == NbnConstants.OutputRegionId)
                    {
                        error = "Output region neurons cannot target output region.";
                        return false;
                    }

                    if (!regionCounts.TryGetValue(axon.TargetRegionId, out var targetSpan) || targetSpan == 0)
                    {
                        error = $"Target region {axon.TargetRegionId} is missing.";
                        return false;
                    }

                    if (axon.TargetNeuronId < 0 || axon.TargetNeuronId >= targetSpan)
                    {
                        error = $"Target neuron {axon.TargetNeuronId} is out of range for region {axon.TargetRegionId}.";
                        return false;
                    }

                    if (regionMap.TryGetValue(axon.TargetRegionId, out var targetRegion)
                        && !targetRegion.Neurons[axon.TargetNeuronId].Exists)
                    {
                        error = $"Target neuron {axon.TargetNeuronId} in region {axon.TargetRegionId} is disabled.";
                        return false;
                    }

                    if (!targets.Add((axon.TargetRegionId, axon.TargetNeuronId)))
                    {
                        error = $"Duplicate axon from neuron {neuron.NeuronId} in region {region.RegionId}.";
                        return false;
                    }

                    axonRecords.Add(new AxonRecord((byte)axon.StrengthCode, axon.TargetNeuronId, (byte)axon.TargetRegionId));
                }

                neuronRecords[neuronIndex] = new NeuronRecord(
                    (ushort)orderedAxons.Count,
                    (byte)neuron.ParamBCode,
                    (byte)neuron.ParamACode,
                    (byte)neuron.ActivationThresholdCode,
                    (byte)neuron.PreActivationThresholdCode,
                    (byte)neuron.ResetFunctionId,
                    (byte)neuron.ActivationFunctionId,
                    (byte)neuron.AccumulationFunctionId,
                    neuron.Exists);
            }

            var checkpointCount = (uint)((neuronSpan + Brain.AxonStride - 1) / Brain.AxonStride + 1);
            var checkpoints = NbnBinary.BuildCheckpoints(neuronRecords, Brain.AxonStride);
            var section = new NbnRegionSection(
                (byte)region.RegionId,
                (uint)neuronSpan,
                (ulong)axonRecords.Count,
                Brain.AxonStride,
                checkpointCount,
                checkpoints,
                neuronRecords,
                axonRecords.ToArray());

            sections.Add(section);
            directory[region.RegionId] = new NbnRegionDirectoryEntry((uint)neuronSpan, (ulong)axonRecords.Count, offset, 0);
            offset += (ulong)section.ByteLength;
        }

        header = new NbnHeaderV2(
            "NBN2",
            2,
            1,
            10,
            Brain.BrainSeed,
            Brain.AxonStride,
            0,
            QuantizationSchemas.DefaultNbn,
            directory);

        return true;
    }

    private static DesignerBrainViewModel BuildDesignerBrainFromNbn(NbnHeaderV2 header, IReadOnlyList<NbnRegionSection> regions, string? name)
    {
        var brain = new DesignerBrainViewModel(name ?? "Imported Brain", Guid.NewGuid(), header.BrainSeed, header.AxonStride);
        var regionMap = regions.ToDictionary(region => (int)region.RegionId, region => region);

        for (var i = 0; i < NbnConstants.RegionCount; i++)
        {
            var regionVm = new DesignerRegionViewModel(i);
            if (regionMap.TryGetValue(i, out var section))
            {
                for (var n = 0; n < section.NeuronRecords.Length; n++)
                {
                    var record = section.NeuronRecords[n];
                    var neuronVm = new DesignerNeuronViewModel(i, n, record.Exists, i == 0 || i == NbnConstants.OutputRegionId)
                    {
                        ActivationFunctionId = record.ActivationFunctionId,
                        ResetFunctionId = record.ResetFunctionId,
                        AccumulationFunctionId = record.AccumulationFunctionId,
                        ParamACode = record.ParamACode,
                        ParamBCode = record.ParamBCode,
                        ActivationThresholdCode = record.ActivationThresholdCode,
                        PreActivationThresholdCode = record.PreActivationThresholdCode
                    };

                    regionVm.Neurons.Add(neuronVm);
                }

                var axonIndex = 0;
                for (var n = 0; n < section.NeuronRecords.Length; n++)
                {
                    var neuronVm = regionVm.Neurons[n];
                    var axonCount = section.NeuronRecords[n].AxonCount;
                    for (var a = 0; a < axonCount; a++)
                    {
                        var axonRecord = section.AxonRecords[axonIndex++];
                        neuronVm.Axons.Add(new DesignerAxonViewModel(axonRecord.TargetRegionId, axonRecord.TargetNeuronId, axonRecord.StrengthCode));
                    }

                    neuronVm.UpdateAxonCount();
                }
            }

            regionVm.UpdateCounts();
            brain.Regions.Add(regionVm);
        }

        brain.UpdateTotals();
        return brain;
    }

    private static DesignerNeuronViewModel CreateDefaultNeuron(DesignerRegionViewModel region, int neuronId)
    {
        var isRequired = region.IsInput || region.IsOutput;
        return new DesignerNeuronViewModel(region.RegionId, neuronId, true, isRequired)
        {
            ActivationFunctionId = GetDefaultActivationFunctionIdForRegion(region.RegionId),
            ResetFunctionId = GetDefaultResetFunctionIdForRegion(region.RegionId),
            AccumulationFunctionId = 0,
            ParamACode = 0,
            ParamBCode = 0,
            ActivationThresholdCode = 0,
            PreActivationThresholdCode = 0
        };
    }

    private static void AddDefaultNeuron(DesignerRegionViewModel region)
    {
        var neuron = CreateDefaultNeuron(region, 0);
        region.Neurons.Add(neuron);
        region.UpdateCounts();
    }

    private static IReadOnlyList<NbnRegionSection> ReadNbnRegions(byte[] data, NbnHeaderV2 header)
    {
        var regions = new List<NbnRegionSection>();
        for (var i = 0; i < header.Regions.Length; i++)
        {
            var entry = header.Regions[i];
            if (entry.NeuronSpan == 0 || entry.Offset == 0)
            {
                continue;
            }

            regions.Add(NbnBinary.ReadNbnRegionSection(data, entry.Offset));
        }

        return regions;
    }

    private static void ReadNbsSections(byte[] data, NbsHeaderV2 header, out IReadOnlyList<NbsRegionSection> regions, out NbsOverlaySection? overlay)
    {
        overlay = null;
        var list = new List<NbsRegionSection>();
        var offset = NbnBinary.NbsHeaderBytes;

        while (offset < data.Length)
        {
            if (header.AxonOverlayIncluded && data.Length - offset >= 4)
            {
                var overlayCount = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(offset, 4));
                var overlaySize = NbnBinary.GetNbsOverlaySectionSize((int)overlayCount);
                if (overlaySize > 0 && offset + overlaySize == data.Length)
                {
                    overlay = NbnBinary.ReadNbsOverlaySection(data, offset);
                    offset += overlay.ByteLength;
                    break;
                }
            }

            var region = NbnBinary.ReadNbsRegionSection(data, offset, header.EnabledBitsetIncluded);
            list.Add(region);
            offset += region.ByteLength;
        }

        regions = list;
    }

    private static string BuildDesignSummary(DesignerBrainViewModel brain, string? label)
    {
        var regionCount = brain.Regions.Count(region => region.NeuronCount > 0);
        var name = string.IsNullOrWhiteSpace(label) ? brain.Name : label;
        return $"Design: {name} - regions {regionCount} - neurons {brain.TotalNeurons} - axons {brain.TotalAxons} - stride {brain.AxonStride}";
    }

    private static string BuildNbsSummary(string fileName, NbsHeaderV2 header, IReadOnlyList<NbsRegionSection> regions, NbsOverlaySection? overlay)
    {
        var overlayCount = overlay?.Records.Length ?? 0;
        return $"Loaded NBS: {fileName} - regions {regions.Count} - overlay {overlayCount} - tick {header.SnapshotTickId}";
    }

    private static IReadOnlyList<DesignerFunctionOption> BuildActivationFunctions()
    {
        var list = new List<DesignerFunctionOption>
        {
            new(0, "ACT_NONE (0)", "potential = 0"),
            new(1, "ACT_IDENTITY (1)", "potential = B"),
            new(2, "ACT_STEP_UP (2)", "potential = (B <= 0) ? 0 : 1"),
            new(3, "ACT_STEP_MID (3)", "potential = (B < 0) ? -1 : (B == 0 ? 0 : 1)"),
            new(4, "ACT_STEP_DOWN (4)", "potential = (B < 0) ? -1 : 0"),
            new(5, "ACT_ABS (5)", "potential = abs(B)"),
            new(6, "ACT_CLAMP (6)", "potential = clamp(B, -1, +1)"),
            new(7, "ACT_RELU (7)", "potential = max(0, B)"),
            new(8, "ACT_NRELU (8)", "potential = min(B, 0)"),
            new(9, "ACT_SIN (9)", "potential = sin(B)"),
            new(10, "ACT_TAN (10)", "potential = clamp(tan(B), -1, +1)"),
            new(11, "ACT_TANH (11)", "potential = tanh(B)"),
            new(12, "ACT_ELU (12)", "potential = (B > 0) ? B : A*(exp(B)-1)", usesParamA: true),
            new(13, "ACT_EXP (13)", "potential = exp(B)"),
            new(14, "ACT_PRELU (14)", "potential = (B >= 0) ? B : A*B", usesParamA: true),
            new(15, "ACT_LOG (15)", "potential = (B == 0) ? 0 : log(B)"),
            new(16, "ACT_MULT (16)", "potential = B * A", usesParamA: true),
            new(17, "ACT_ADD (17)", "potential = B + A", usesParamA: true),
            new(18, "ACT_SIG (18)", "potential = 1 / (1 + exp(-B))"),
            new(19, "ACT_SILU (19)", "potential = B / (1 + exp(-B))"),
            new(20, "ACT_PCLAMP (20)", "potential = (Bp <= A) ? 0 : clamp(B, A, Bp)", usesParamA: true, usesParamB: true),
            new(21, "ACT_MODL (21)", "potential = B % A", usesParamA: true),
            new(22, "ACT_MODR (22)", "potential = A % B", usesParamA: true),
            new(23, "ACT_SOFTP (23)", "potential = log(1 + exp(B))"),
            new(24, "ACT_SELU (24)", "potential = Bp * (B >= 0 ? B : A*(exp(B)-1))", usesParamA: true, usesParamB: true),
            new(25, "ACT_LIN (25)", "potential = A*B + Bp", usesParamA: true, usesParamB: true),
            new(26, "ACT_LOGB (26)", "potential = (A == 0) ? 0 : log(B, A)", usesParamA: true),
            new(27, "ACT_POW (27)", "potential = pow(B, A)", usesParamA: true),
            new(28, "ACT_GAUSS (28)", "potential = exp((-B)^2)"),
            new(29, "ACT_QUAD (29)", "potential = A*(B^2) + Bp*B", usesParamA: true, usesParamB: true)
        };

        return list;
    }

    private static IReadOnlyList<DesignerFunctionOption> BuildResetFunctions()
    {
        var names = new Dictionary<int, string>
        {
            { 0, "RESET_ZERO" },
            { 1, "RESET_HOLD" },
            { 2, "RESET_CLAMP_POTENTIAL" },
            { 3, "RESET_CLAMP1" },
            { 4, "RESET_POTENTIAL_CLAMP_BUFFER" },
            { 5, "RESET_NEG_POTENTIAL_CLAMP_BUFFER" },
            { 6, "RESET_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER" },
            { 7, "RESET_TENTH_POTENTIAL_CLAMP_BUFFER" },
            { 8, "RESET_HALF_POTENTIAL_CLAMP_BUFFER" },
            { 9, "RESET_DOUBLE_POTENTIAL_CLAMP_BUFFER" },
            { 10, "RESET_FIVEX_POTENTIAL_CLAMP_BUFFER" },
            { 11, "RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER" },
            { 12, "RESET_NEG_TENTH_POTENTIAL_CLAMP_BUFFER" },
            { 13, "RESET_NEG_HALF_POTENTIAL_CLAMP_BUFFER" },
            { 14, "RESET_NEG_DOUBLE_POTENTIAL_CLAMP_BUFFER" },
            { 15, "RESET_NEG_FIVEX_POTENTIAL_CLAMP_BUFFER" },
            { 16, "RESET_INVERSE_POTENTIAL_CLAMP_BUFFER" },
            { 17, "RESET_POTENTIAL_CLAMP1" },
            { 18, "RESET_NEG_POTENTIAL_CLAMP1" },
            { 19, "RESET_HUNDREDTHS_POTENTIAL_CLAMP1" },
            { 20, "RESET_TENTH_POTENTIAL_CLAMP1" },
            { 21, "RESET_HALF_POTENTIAL_CLAMP1" },
            { 22, "RESET_DOUBLE_POTENTIAL_CLAMP1" },
            { 23, "RESET_FIVEX_POTENTIAL_CLAMP1" },
            { 24, "RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP1" },
            { 25, "RESET_NEG_TENTH_POTENTIAL_CLAMP1" },
            { 26, "RESET_NEG_HALF_POTENTIAL_CLAMP1" },
            { 27, "RESET_NEG_DOUBLE_POTENTIAL_CLAMP1" },
            { 28, "RESET_NEG_FIVEX_POTENTIAL_CLAMP1" },
            { 29, "RESET_INVERSE_POTENTIAL_CLAMP1" },
            { 30, "RESET_POTENTIAL" },
            { 31, "RESET_NEG_POTENTIAL" },
            { 32, "RESET_HUNDREDTHS_POTENTIAL" },
            { 33, "RESET_TENTH_POTENTIAL" },
            { 34, "RESET_HALF_POTENTIAL" },
            { 35, "RESET_DOUBLE_POTENTIAL" },
            { 36, "RESET_FIVEX_POTENTIAL" },
            { 37, "RESET_NEG_HUNDREDTHS_POTENTIAL" },
            { 38, "RESET_NEG_TENTH_POTENTIAL" },
            { 39, "RESET_NEG_HALF_POTENTIAL" },
            { 40, "RESET_NEG_DOUBLE_POTENTIAL" },
            { 41, "RESET_NEG_FIVEX_POTENTIAL" },
            { 42, "RESET_INVERSE_POTENTIAL" },
            { 43, "RESET_HALF" },
            { 44, "RESET_TENTH" },
            { 45, "RESET_HUNDREDTH" },
            { 46, "RESET_NEGATIVE" },
            { 47, "RESET_NEG_HALF" },
            { 48, "RESET_NEG_TENTH" },
            { 49, "RESET_NEG_HUNDREDTH" },
            { 50, "RESET_DOUBLE_CLAMP1" },
            { 51, "RESET_FIVEX_CLAMP1" },
            { 52, "RESET_NEG_DOUBLE_CLAMP1" },
            { 53, "RESET_NEG_FIVEX_CLAMP1" },
            { 54, "RESET_DOUBLE" },
            { 55, "RESET_FIVEX" },
            { 56, "RESET_NEG_DOUBLE" },
            { 57, "RESET_NEG_FIVEX" },
            { 58, "RESET_DIVIDE_AXON_CT" },
            { 59, "RESET_INVERSE_CLAMP1" },
            { 60, "RESET_INVERSE" }
        };

        var descriptions = new Dictionary<int, string>
        {
            { 0, "new = 0" },
            { 1, "new = clamp(B, -T, +T)" },
            { 2, "new = clamp(B, -|P|, +|P|)" },
            { 3, "new = clamp(B, -1, +1)" },
            { 4, "new = clamp(P, -|B|, +|B|)" },
            { 5, "new = clamp(-P, -|B|, +|B|)" },
            { 6, "new = clamp(0.01*P, -|B|, +|B|)" },
            { 7, "new = clamp(0.1*P, -|B|, +|B|)" },
            { 8, "new = clamp(0.5*P, -|B|, +|B|)" },
            { 9, "new = clamp(2*P, -|B|, +|B|)" },
            { 10, "new = clamp(5*P, -|B|, +|B|)" },
            { 11, "new = clamp(-0.01*P, -|B|, +|B|)" },
            { 12, "new = clamp(-0.1*P, -|B|, +|B|)" },
            { 13, "new = clamp(-0.5*P, -|B|, +|B|)" },
            { 14, "new = clamp(-2*P, -|B|, +|B|)" },
            { 15, "new = clamp(-5*P, -|B|, +|B|)" },
            { 16, "new = clamp(1/P, -|B|, +|B|)" },
            { 17, "new = clamp(P, -1, +1)" },
            { 18, "new = clamp(-P, -1, +1)" },
            { 19, "new = clamp(0.01*P, -1, +1)" },
            { 20, "new = clamp(0.1*P, -1, +1)" },
            { 21, "new = clamp(0.5*P, -1, +1)" },
            { 22, "new = clamp(2*P, -1, +1)" },
            { 23, "new = clamp(5*P, -1, +1)" },
            { 24, "new = clamp(-0.01*P, -1, +1)" },
            { 25, "new = clamp(-0.1*P, -1, +1)" },
            { 26, "new = clamp(-0.5*P, -1, +1)" },
            { 27, "new = clamp(-2*P, -1, +1)" },
            { 28, "new = clamp(-5*P, -1, +1)" },
            { 29, "new = clamp(1/P, -1, +1)" },
            { 30, "new = clamp(P, -T, +T)" },
            { 31, "new = clamp(-P, -T, +T)" },
            { 32, "new = clamp(0.01*P, -T, +T)" },
            { 33, "new = clamp(0.1*P, -T, +T)" },
            { 34, "new = clamp(0.5*P, -T, +T)" },
            { 35, "new = clamp(2*P, -T, +T)" },
            { 36, "new = clamp(5*P, -T, +T)" },
            { 37, "new = clamp(-0.01*P, -T, +T)" },
            { 38, "new = clamp(-0.1*P, -T, +T)" },
            { 39, "new = clamp(-0.5*P, -T, +T)" },
            { 40, "new = clamp(-2*P, -T, +T)" },
            { 41, "new = clamp(-5*P, -T, +T)" },
            { 42, "new = clamp(1/P, -T, +T)" },
            { 43, "new = clamp(0.5*B, -T, +T)" },
            { 44, "new = clamp(0.1*B, -T, +T)" },
            { 45, "new = clamp(0.01*B, -T, +T)" },
            { 46, "new = clamp(-B, -T, +T)" },
            { 47, "new = clamp(-0.5*B, -T, +T)" },
            { 48, "new = clamp(-0.1*B, -T, +T)" },
            { 49, "new = clamp(-0.01*B, -T, +T)" },
            { 50, "new = clamp(2*B, -1, +1)" },
            { 51, "new = clamp(5*B, -1, +1)" },
            { 52, "new = clamp(-2*B, -1, +1)" },
            { 53, "new = clamp(-5*B, -1, +1)" },
            { 54, "new = clamp(2*B, -T, +T)" },
            { 55, "new = clamp(5*B, -T, +T)" },
            { 56, "new = clamp(-2*B, -T, +T)" },
            { 57, "new = clamp(-5*B, -T, +T)" },
            { 58, "new = clamp(B / max(1,K), -T, +T)" },
            { 59, "new = clamp(-1/B, -1, +1)" },
            { 60, "new = clamp(-1/B, -T, +T)" }
        };

        var list = new List<DesignerFunctionOption>();
        for (var i = 0; i <= MaxKnownResetFunctionId; i++)
        {
            if (names.TryGetValue(i, out var name))
            {
                var description = descriptions.TryGetValue(i, out var detail) ? detail : string.Empty;
                list.Add(new DesignerFunctionOption(i, $"{name} ({i})", description));
            }
            else
            {
                list.Add(new DesignerFunctionOption(i, $"UNKNOWN ({i})", "Undefined reset function ID."));
            }
        }

        return list;
    }

    private static IReadOnlyList<DesignerFunctionOption> BuildAccumulationFunctions()
    {
        return new List<DesignerFunctionOption>
        {
            new(0, "ACCUM_SUM (0)", "B = B + I"),
            new(1, "ACCUM_PRODUCT (1)", "B = B * I (if any input)"),
            new(2, "ACCUM_MAX (2)", "B = max(B, I)"),
            new(3, "ACCUM_NONE (3)", "No merge")
        };
    }

    private bool UsesParamA(int id)
    {
        var option = ActivationFunctions.FirstOrDefault(entry => entry.Id == id);
        return option?.UsesParamA ?? false;
    }

    private bool UsesParamB(int id)
    {
        var option = ActivationFunctions.FirstOrDefault(entry => entry.Id == id);
        return option?.UsesParamB ?? false;
    }

    private string DescribeActivation(int id)
        => ActivationFunctions.FirstOrDefault(entry => entry.Id == id)?.Description ?? "Reserved";

    private string DescribeReset(int id)
        => ResetFunctions.FirstOrDefault(entry => entry.Id == id)?.Description ?? "Reserved";

    private string DescribeAccumulation(int id)
        => AccumulationFunctions.FirstOrDefault(entry => entry.Id == id)?.Description ?? "Reserved";

    private string BuildNeuronConstraintHint(DesignerNeuronViewModel? neuron)
    {
        if (neuron is null)
        {
            return "Select a neuron to view region-specific function constraints.";
        }

        if (neuron.RegionId == NbnConstants.InputRegionId)
        {
            return "Input neurons use constrained activation/reset sets for stable external signal handling.";
        }

        if (neuron.RegionId == NbnConstants.OutputRegionId)
        {
            return "Output neurons use a constrained activation set for readable external outputs.";
        }

        return "Internal neurons allow all defined activation IDs 0-29 and reset IDs 0-60.";
    }

    private int NormalizeBrainFunctionConstraints(DesignerBrainViewModel brain)
    {
        var normalized = 0;
        foreach (var region in brain.Regions)
        {
            foreach (var neuron in region.Neurons)
            {
                if (NormalizeNeuronFunctionConstraints(neuron, out _, includeStatus: false))
                {
                    normalized++;
                }
            }
        }

        return normalized;
    }

    private bool NormalizeNeuronFunctionConstraints(DesignerNeuronViewModel neuron, out string? statusMessage, bool includeStatus = true)
    {
        statusMessage = null;
        var previousActivation = neuron.ActivationFunctionId;
        var previousReset = neuron.ResetFunctionId;
        var previousParamA = neuron.ParamACode;
        var previousParamB = neuron.ParamBCode;

        _suppressNeuronConstraintEnforcement = true;
        try
        {
            if (!IsActivationFunctionAllowedForRegion(neuron.RegionId, neuron.ActivationFunctionId))
            {
                neuron.ActivationFunctionId = GetDefaultActivationFunctionIdForRegion(neuron.RegionId);
            }

            if (!IsResetFunctionAllowedForRegion(neuron.RegionId, neuron.ResetFunctionId))
            {
                neuron.ResetFunctionId = GetDefaultResetFunctionIdForRegion(neuron.RegionId);
            }

            if (!UsesParamA(neuron.ActivationFunctionId))
            {
                neuron.ParamACode = 0;
            }

            if (!UsesParamB(neuron.ActivationFunctionId))
            {
                neuron.ParamBCode = 0;
            }
        }
        finally
        {
            _suppressNeuronConstraintEnforcement = false;
        }

        var changed = previousActivation != neuron.ActivationFunctionId
            || previousReset != neuron.ResetFunctionId
            || previousParamA != neuron.ParamACode
            || previousParamB != neuron.ParamBCode;

        if (changed && includeStatus)
        {
            statusMessage = $"Neuron R{neuron.RegionId} N{neuron.NeuronId} function settings were normalized for region constraints.";
        }

        return changed;
    }

    private static IReadOnlyList<int> GetAllowedActivationFunctionIdsForRegion(int regionId)
    {
        if (regionId == NbnConstants.InputRegionId)
        {
            return InputAllowedActivationFunctionIds;
        }

        if (regionId == NbnConstants.OutputRegionId)
        {
            return OutputAllowedActivationFunctionIds;
        }

        return ActivationFunctionIds;
    }

    private static IReadOnlyList<int> GetRandomAllowedActivationFunctionIdsForRegion(int regionId)
    {
        if (regionId == NbnConstants.InputRegionId)
        {
            return InputAllowedActivationFunctionIds;
        }

        if (regionId == NbnConstants.OutputRegionId)
        {
            return OutputAllowedActivationFunctionIds;
        }

        return RandomInternalActivationFunctionIds;
    }

    private static IReadOnlyList<int> GetAllowedResetFunctionIdsForRegion(int regionId)
    {
        if (regionId == NbnConstants.InputRegionId)
        {
            return InputAllowedResetFunctionIds;
        }

        return ResetFunctionIds;
    }

    private static int GetDefaultActivationFunctionIdForRegion(int regionId)
    {
        if (regionId == NbnConstants.InputRegionId)
        {
            return DefaultInputActivationFunctionId;
        }

        if (regionId == NbnConstants.OutputRegionId)
        {
            return DefaultOutputActivationFunctionId;
        }

        return DefaultActivationFunctionId;
    }

    private static int GetDefaultResetFunctionIdForRegion(int regionId)
    {
        if (regionId == NbnConstants.InputRegionId)
        {
            return DefaultInputResetFunctionId;
        }

        return 0;
    }

    private static bool IsActivationFunctionAllowedForRegion(int regionId, int functionId)
        => IsKnownActivationFunctionId(functionId)
           && GetAllowedActivationFunctionIdsForRegion(regionId).Contains(functionId);

    private static bool IsResetFunctionAllowedForRegion(int regionId, int functionId)
        => IsKnownResetFunctionId(functionId)
           && GetAllowedResetFunctionIdsForRegion(regionId).Contains(functionId);

    private static bool IsKnownActivationFunctionId(int functionId)
        => functionId >= 0 && functionId <= MaxKnownActivationFunctionId;

    private static bool IsKnownResetFunctionId(int functionId)
        => functionId >= 0 && functionId <= MaxKnownResetFunctionId;

    private static ulong GenerateSeed()
    {
        Span<byte> buffer = stackalloc byte[8];
        RandomNumberGenerator.Fill(buffer);
        return BinaryPrimitives.ReadUInt64LittleEndian(buffer);
    }

    private static HashSet<int> SelectRegions(Random rng, RandomBrainGenerationOptions options)
    {
        if (options.RegionSelectionMode == RandomRegionSelectionMode.ExplicitList)
        {
            return options.ExplicitRegions.ToHashSet();
        }

        var total = NbnConstants.RegionCount - 2;
        if (options.RegionCount <= 0 || total <= 0)
        {
            return new HashSet<int>();
        }

        if (options.RegionSelectionMode == RandomRegionSelectionMode.Contiguous)
        {
            var startMax = total - options.RegionCount + 1;
            var start = rng.Next(1, Math.Max(2, startMax + 1));
            var list = new HashSet<int>();
            for (var id = start; id < start + options.RegionCount; id++)
            {
                list.Add(id);
            }

            return list;
        }

        var available = Enumerable.Range(1, total).ToList();
        var selected = new HashSet<int>();
        var picks = Math.Min(options.RegionCount, available.Count);
        for (var i = 0; i < picks; i++)
        {
            var index = rng.Next(available.Count);
            selected.Add(available[index]);
            available.RemoveAt(index);
        }

        return selected;
    }

    private static Func<int>[] BuildRegionFunctionPickers(
        Random rng,
        RandomFunctionSelectionMode mode,
        int fixedId,
        IReadOnlyList<int> ids,
        IReadOnlyList<double> weights,
        Func<int, IReadOnlyList<int>> allowedIdSelector,
        Func<int, int> defaultSelector)
    {
        var pickers = new Func<int>[NbnConstants.RegionCount];
        for (var regionId = 0; regionId < NbnConstants.RegionCount; regionId++)
        {
            var allowedIds = allowedIdSelector(regionId);
            if (allowedIds.Count == 0)
            {
                allowedIds = ids;
            }

            var regionFixedId = ResolveRegionFixedFunctionId(fixedId, allowedIds, defaultSelector(regionId));
            var regionWeights = BuildSubsetWeights(ids, weights, allowedIds);
            pickers[regionId] = CreateFunctionPicker(rng, mode, regionFixedId, allowedIds, regionWeights);
        }

        return pickers;
    }

    private static double[] BuildSubsetWeights(IReadOnlyList<int> allIds, IReadOnlyList<double> allWeights, IReadOnlyList<int> subsetIds)
    {
        var weightById = new Dictionary<int, double>(allIds.Count);
        var count = Math.Min(allIds.Count, allWeights.Count);
        for (var i = 0; i < count; i++)
        {
            weightById[allIds[i]] = allWeights[i];
        }

        var subsetWeights = new double[subsetIds.Count];
        for (var i = 0; i < subsetIds.Count; i++)
        {
            subsetWeights[i] = weightById.TryGetValue(subsetIds[i], out var weight)
                ? weight
                : 1.0;
        }

        return subsetWeights;
    }

    private static int ResolveRegionFixedFunctionId(int fixedId, IReadOnlyList<int> allowedIds, int fallbackId)
    {
        if (allowedIds.Contains(fixedId))
        {
            return fixedId;
        }

        if (allowedIds.Contains(fallbackId))
        {
            return fallbackId;
        }

        return allowedIds.Count > 0 ? allowedIds[0] : fixedId;
    }

    private static Func<int> CreateFunctionPicker(Random rng, RandomFunctionSelectionMode mode, int fixedId, IReadOnlyList<int> ids, IReadOnlyList<double> weights)
    {
        return mode switch
        {
            RandomFunctionSelectionMode.Fixed => () => fixedId,
            RandomFunctionSelectionMode.Random => () => ids[rng.Next(ids.Count)],
            RandomFunctionSelectionMode.Weighted => () => ids[PickWeightedIndex(rng, weights)],
            _ => () => fixedId
        };
    }

    private static Func<int> CreateCodePicker(Random rng, RandomRangeMode mode, int fixedValue, int min, int max)
    {
        if (mode == RandomRangeMode.Fixed || min == max)
        {
            return () => fixedValue;
        }

        return () => rng.Next(min, max + 1);
    }

    private static Func<int> CreateCenteredCodePicker(Random rng, RandomRangeMode mode, int fixedValue, int min, int max)
    {
        if (mode == RandomRangeMode.Fixed || min == max)
        {
            return () => fixedValue;
        }

        return () => PickCenteredInt(rng, min, max);
    }

    private static Func<int> CreateLowBiasedCodePicker(Random rng, RandomRangeMode mode, int fixedValue, int min, int max)
    {
        if (mode == RandomRangeMode.Fixed || min == max)
        {
            return () => fixedValue;
        }

        return () => PickLowBiasedInt(rng, min, max, 1.8);
    }

    private static int PickCount(Random rng, RandomCountMode mode, int fixedValue, int min, int max)
    {
        if (mode == RandomCountMode.Fixed || min == max)
        {
            return fixedValue;
        }

        return rng.Next(min, max + 1);
    }

    private static int PickRangeCount(Random rng, int min, int max)
    {
        if (min == max)
        {
            return min;
        }

        if (max < min)
        {
            (min, max) = (max, min);
        }

        return rng.Next(min, max + 1);
    }

    private static int PickBiasedCount(Random rng, RandomCountMode mode, int fixedValue, int min, int max, double biasPower)
    {
        if (mode == RandomCountMode.Fixed || min == max)
        {
            return fixedValue;
        }

        if (max < min)
        {
            (min, max) = (max, min);
        }

        var range = max - min;
        var sample = Math.Pow(rng.NextDouble(), biasPower);
        var value = min + (int)Math.Round(sample * range);
        if (value < min)
        {
            return min;
        }

        if (value > max)
        {
            return max;
        }

        return value;
    }


    private static int PickTargetNeuronId(Random rng, int sourceNeuronId, int sourceRegionId, DesignerRegionViewModel targetRegion, RandomTargetBiasMode bias)
    {
        if (targetRegion.NeuronCount <= 1)
        {
            return 0;
        }

        if (bias == RandomTargetBiasMode.DistanceWeighted && targetRegion.RegionId == sourceRegionId)
        {
            var maxDistance = targetRegion.NeuronCount / 2;
            if (maxDistance == 0)
            {
                return 0;
            }

            var distance = (int)Math.Round(Math.Pow(rng.NextDouble(), 2) * maxDistance);
            var direction = rng.Next(2) == 0 ? -1 : 1;
            var candidate = sourceNeuronId + (direction * distance);
            var wrapped = candidate % targetRegion.NeuronCount;
            if (wrapped < 0)
            {
                wrapped += targetRegion.NeuronCount;
            }

            return wrapped;
        }

        return rng.Next(targetRegion.NeuronCount);
    }

    private static int PickCenteredInt(Random rng, int min, int max)
    {
        if (min == max)
        {
            return min;
        }

        if (max < min)
        {
            (min, max) = (max, min);
        }

        var sample = (rng.NextDouble() + rng.NextDouble() + rng.NextDouble()) / 3.0;
        var range = max - min;
        var value = min + (int)Math.Round(sample * range);
        if (value < min)
        {
            return min;
        }

        if (value > max)
        {
            return max;
        }

        return value;
    }

    private static int PickLowBiasedInt(Random rng, int min, int max, double biasPower)
    {
        if (min == max)
        {
            return min;
        }

        if (max < min)
        {
            (min, max) = (max, min);
        }

        var range = max - min;
        var sample = Math.Pow(rng.NextDouble(), biasPower);
        var value = min + (int)Math.Round(sample * range);
        if (value < min)
        {
            return min;
        }

        if (value > max)
        {
            return max;
        }

        return value;
    }

    private static int PickStrengthCode(Random rng, RandomStrengthDistribution distribution, int min, int max)
    {
        if (min == max)
        {
            return min;
        }

        var range = max - min;
        return distribution switch
        {
            RandomStrengthDistribution.Centered => min + (int)Math.Round(((rng.NextDouble() + rng.NextDouble()) * 0.5) * range),
            RandomStrengthDistribution.Normal => PickNormal(rng, min, max),
            _ => rng.Next(min, max + 1)
        };
    }

    private static int PickNormal(Random rng, int min, int max)
    {
        if (min == max)
        {
            return min;
        }

        var mean = (min + max) / 2.0;
        var stdDev = Math.Max(0.5, (max - min) / 6.0);
        var u1 = Math.Max(double.Epsilon, rng.NextDouble());
        var u2 = rng.NextDouble();
        var standard = Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2.0 * Math.PI * u2);
        var sample = mean + (standard * stdDev);
        var rounded = (int)Math.Round(sample);
        if (rounded < min)
        {
            return min;
        }

        if (rounded > max)
        {
            return max;
        }

        return rounded;
    }

    private static double[] BuildTargetWeights(int sourceRegionId, IReadOnlyList<DesignerRegionViewModel> targets, RandomTargetBiasMode bias)
    {
        var weights = new double[targets.Count];
        var sourceZ = RegionZ(sourceRegionId);
        for (var i = 0; i < targets.Count; i++)
        {
            var target = targets[i];
            var weight = bias switch
            {
                RandomTargetBiasMode.RegionWeighted => Math.Max(1, target.NeuronCount),
                RandomTargetBiasMode.DistanceWeighted => 1.0 / (1.0 + ComputeRegionDistance(sourceRegionId, target.RegionId)),
                _ => 1.0
            };

            if (target.RegionId == sourceRegionId)
            {
                weight *= IntraRegionWeight;
            }
            else
            {
                weight *= InterRegionWeight;
            }

            var targetZ = RegionZ(target.RegionId);
            if (targetZ > sourceZ)
            {
                weight *= ForwardRegionWeight;
            }
            else if (targetZ < sourceZ)
            {
                weight *= BackwardRegionWeight;
            }

            if (target.RegionId == NbnConstants.OutputRegionId)
            {
                weight *= OutputRegionWeight;
            }

            weights[i] = weight;
        }

        return weights;
    }

    private static int ComputeRegionDistance(int sourceRegionId, int destRegionId)
    {
        if (sourceRegionId == destRegionId)
        {
            return 0;
        }

        var sourceZ = RegionZ(sourceRegionId);
        var destZ = RegionZ(destRegionId);
        if (sourceZ == destZ)
        {
            return RegionIntrasliceUnit;
        }

        return RegionAxialUnit * Math.Abs(destZ - sourceZ);
    }

    private static int RegionZ(int regionId)
    {
        if (regionId == 0)
        {
            return -3;
        }

        if (regionId <= 3)
        {
            return -2;
        }

        if (regionId <= 8)
        {
            return -1;
        }

        if (regionId <= 22)
        {
            return 0;
        }

        if (regionId <= 27)
        {
            return 1;
        }

        if (regionId <= 30)
        {
            return 2;
        }

        return 3;
    }

    private static int MaxDistinctTargets(int sourceRegionId, IReadOnlyList<DesignerRegionViewModel> targets, bool allowSelfLoops)
    {
        var maxTargets = 0;
        foreach (var target in targets)
        {
            if (target.NeuronCount == 0)
            {
                continue;
            }

            if (!allowSelfLoops && target.RegionId == sourceRegionId)
            {
                maxTargets += Math.Max(0, target.NeuronCount - 1);
            }
            else
            {
                maxTargets += target.NeuronCount;
            }
        }

        return maxTargets;
    }

    private static List<DesignerRegionViewModel> BuildValidTargetsForRegion(
        DesignerRegionViewModel sourceRegion,
        IReadOnlyList<DesignerRegionViewModel> regionsWithNeurons,
        RandomBrainGenerationOptions options)
    {
        return regionsWithNeurons
            .Where(target => target.RegionId != NbnConstants.InputRegionId
                             && !(sourceRegion.RegionId == NbnConstants.OutputRegionId && target.RegionId == NbnConstants.OutputRegionId))
            .Where(target => options.AllowInterRegion || target.RegionId == sourceRegion.RegionId)
            .Where(target => options.AllowIntraRegion || target.RegionId != sourceRegion.RegionId)
            .ToList();
    }

    private static bool TryAddRandomAxonFromNeuron(
        Random rng,
        DesignerNeuronViewModel sourceNeuron,
        DesignerRegionViewModel sourceRegion,
        IReadOnlyList<DesignerRegionViewModel> validTargets,
        IReadOnlyList<double> targetWeights,
        RandomBrainGenerationOptions options,
        int maxAttempts = 64)
    {
        if (!sourceNeuron.Exists || sourceNeuron.Axons.Count >= NbnConstants.MaxAxonsPerNeuron || validTargets.Count == 0)
        {
            return false;
        }

        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            var targetRegion = validTargets[PickWeightedIndex(rng, targetWeights)];
            if (targetRegion.NeuronCount == 0 || targetRegion.RegionId == NbnConstants.InputRegionId)
            {
                continue;
            }

            if (sourceRegion.RegionId == NbnConstants.OutputRegionId && targetRegion.RegionId == NbnConstants.OutputRegionId)
            {
                continue;
            }

            var targetNeuronId = PickTargetNeuronId(rng, sourceNeuron.NeuronId, sourceRegion.RegionId, targetRegion, options.TargetBiasMode);
            if (!options.AllowSelfLoops
                && targetRegion.RegionId == sourceRegion.RegionId
                && targetNeuronId == sourceNeuron.NeuronId)
            {
                continue;
            }

            if (sourceNeuron.Axons.Any(axon => axon.TargetRegionId == targetRegion.RegionId && axon.TargetNeuronId == targetNeuronId))
            {
                continue;
            }

            var strength = PickStrengthCode(rng, options.StrengthDistribution, options.StrengthMinCode, options.StrengthMaxCode);
            sourceNeuron.Axons.Add(new DesignerAxonViewModel(targetRegion.RegionId, targetNeuronId, strength));
            sourceNeuron.UpdateAxonCount();
            return true;
        }

        return false;
    }

    private static void EnsureNeuronOutboundCoverage(Random rng, DesignerBrainViewModel brain, RandomBrainGenerationOptions options)
    {
        if (options.AxonCountMax <= 0)
        {
            return;
        }

        var regionsWithNeurons = brain.Regions.Where(region => region.NeuronCount > 0).ToList();
        foreach (var sourceRegion in regionsWithNeurons)
        {
            if (sourceRegion.RegionId == NbnConstants.OutputRegionId)
            {
                continue;
            }

            var validTargets = BuildValidTargetsForRegion(sourceRegion, regionsWithNeurons, options);
            if (validTargets.Count == 0)
            {
                continue;
            }

            var targetWeights = BuildTargetWeights(sourceRegion.RegionId, validTargets, options.TargetBiasMode);
            var changed = false;
            foreach (var neuron in sourceRegion.Neurons)
            {
                if (!neuron.Exists || neuron.Axons.Count > 0)
                {
                    continue;
                }

                if (TryAddRandomAxonFromNeuron(rng, neuron, sourceRegion, validTargets, targetWeights, options))
                {
                    changed = true;
                }
            }

            if (changed)
            {
                sourceRegion.UpdateCounts();
            }
        }
    }

    private static void EnsureRegionInboundConnectivity(Random rng, DesignerBrainViewModel brain, RandomBrainGenerationOptions options)
    {
        if (!options.AllowInterRegion || options.AxonCountMax <= 0)
        {
            return;
        }

        var regionsWithNeurons = brain.Regions.Where(region => region.NeuronCount > 0).ToList();
        foreach (var targetRegion in regionsWithNeurons)
        {
            if (targetRegion.RegionId == NbnConstants.InputRegionId)
            {
                continue;
            }

            var hasInbound = regionsWithNeurons
                .Where(source => source.RegionId != targetRegion.RegionId)
                .SelectMany(source => source.Neurons.Where(neuron => neuron.Exists))
                .Any(neuron => neuron.Axons.Any(axon => axon.TargetRegionId == targetRegion.RegionId));

            if (hasInbound)
            {
                continue;
            }

            var sourceRegions = regionsWithNeurons
                .Where(source => source.RegionId != targetRegion.RegionId && source.NeuronCount > 0)
                .ToList();

            if (sourceRegions.Count == 0)
            {
                continue;
            }

            for (var attempt = 0; attempt < 128; attempt++)
            {
                var sourceRegion = sourceRegions[rng.Next(sourceRegions.Count)];
                var sourceNeuron = sourceRegion.Neurons[rng.Next(sourceRegion.NeuronCount)];
                var singleTarget = new[] { targetRegion };
                var singleWeight = new[] { 1.0 };
                if (TryAddRandomAxonFromNeuron(rng, sourceNeuron, sourceRegion, singleTarget, singleWeight, options, maxAttempts: 24))
                {
                    sourceRegion.UpdateCounts();
                    break;
                }
            }
        }
    }

    private static void EnsureOutputInbound(Random rng, DesignerBrainViewModel brain, RandomBrainGenerationOptions options)
    {
        if (!options.AllowInterRegion || options.AxonCountMax <= 0)
        {
            return;
        }

        var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
        if (outputRegion.NeuronCount == 0)
        {
            return;
        }

        var hasInbound = brain.Regions
            .Where(region => region.RegionId != NbnConstants.OutputRegionId)
            .SelectMany(region => region.Neurons)
            .Any(neuron => neuron.Axons.Any(axon => axon.TargetRegionId == NbnConstants.OutputRegionId));

        if (hasInbound)
        {
            return;
        }

        var sourceRegions = brain.Regions
            .Where(region => region.RegionId != NbnConstants.OutputRegionId && region.NeuronCount > 0)
            .ToList();

        if (sourceRegions.Count == 0)
        {
            return;
        }

        for (var attempt = 0; attempt < 64; attempt++)
        {
            var sourceRegion = sourceRegions[rng.Next(sourceRegions.Count)];
            if (sourceRegion.NeuronCount == 0)
            {
                continue;
            }

            var sourceNeuron = sourceRegion.Neurons[rng.Next(sourceRegion.NeuronCount)];
            if (!sourceNeuron.Exists)
            {
                continue;
            }

            if (sourceNeuron.Axons.Count >= NbnConstants.MaxAxonsPerNeuron)
            {
                continue;
            }

            var targetNeuronId = rng.Next(outputRegion.NeuronCount);
            if (sourceNeuron.Axons.Any(axon => axon.TargetRegionId == NbnConstants.OutputRegionId
                                               && axon.TargetNeuronId == targetNeuronId))
            {
                continue;
            }

            var strength = PickStrengthCode(rng, options.StrengthDistribution, options.StrengthMinCode, options.StrengthMaxCode);
            sourceNeuron.Axons.Add(new DesignerAxonViewModel(NbnConstants.OutputRegionId, targetNeuronId, strength));
            sourceNeuron.UpdateAxonCount();
            sourceRegion.UpdateCounts();
            return;
        }
    }

    private static void EnsureBaselineActivityPath(Random rng, DesignerBrainViewModel brain)
    {
        var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
        if (outputRegion.NeuronCount == 0)
        {
            return;
        }

        var outputNeuron = outputRegion.Neurons.FirstOrDefault(neuron => neuron.Exists) ?? outputRegion.Neurons[0];
        outputNeuron.Exists = true;
        outputNeuron.ActivationFunctionId = DefaultInputActivationFunctionId;
        outputNeuron.ResetFunctionId = DefaultInputResetFunctionId;
        outputNeuron.AccumulationFunctionId = 0;
        outputNeuron.PreActivationThresholdCode = 0;
        outputNeuron.ActivationThresholdCode = 0;
        outputNeuron.ParamACode = 0;
        outputNeuron.ParamBCode = 0;

        var candidateRegions = brain.Regions
            .Where(region => region.RegionId != NbnConstants.InputRegionId
                             && region.RegionId != NbnConstants.OutputRegionId
                             && region.NeuronCount > 0)
            .ToList();

        if (candidateRegions.Count == 0)
        {
            var fallbackRegion = brain.Regions.FirstOrDefault(region =>
                region.RegionId != NbnConstants.InputRegionId
                && region.RegionId != NbnConstants.OutputRegionId);
            if (fallbackRegion is null)
            {
                return;
            }

            if (fallbackRegion.NeuronCount == 0)
            {
                fallbackRegion.Neurons.Add(CreateDefaultNeuron(fallbackRegion, 0));
                fallbackRegion.UpdateCounts();
            }

            candidateRegions.Add(fallbackRegion);
        }

        var sourceRegion = candidateRegions[rng.Next(candidateRegions.Count)];
        var sourceNeuron = sourceRegion.Neurons.FirstOrDefault(neuron => neuron.Exists) ?? sourceRegion.Neurons[0];
        sourceNeuron.Exists = true;
        sourceNeuron.ActivationFunctionId = ActivityDriverActivationFunctionId;
        sourceNeuron.ResetFunctionId = ActivityDriverResetFunctionId;
        sourceNeuron.AccumulationFunctionId = 0;
        sourceNeuron.PreActivationThresholdCode = 0;
        sourceNeuron.ActivationThresholdCode = 0;
        sourceNeuron.ParamACode = ActivityDriverParamACode;
        sourceNeuron.ParamBCode = 0;

        var hasOutputEdge = sourceNeuron.Axons.Any(axon => axon.TargetRegionId == NbnConstants.OutputRegionId
                                                           && axon.TargetNeuronId == outputNeuron.NeuronId);
        if (!hasOutputEdge && sourceNeuron.Axons.Count < NbnConstants.MaxAxonsPerNeuron)
        {
            sourceNeuron.Axons.Add(new DesignerAxonViewModel(
                NbnConstants.OutputRegionId,
                outputNeuron.NeuronId,
                ActivityDriverStrengthCode));
        }

        var outputAxon = sourceNeuron.Axons.FirstOrDefault(axon => axon.TargetRegionId == NbnConstants.OutputRegionId
                                                                    && axon.TargetNeuronId == outputNeuron.NeuronId);
        if (outputAxon is not null)
        {
            outputAxon.StrengthCode = ActivityDriverStrengthCode;
        }

        sourceNeuron.UpdateAxonCount();
        sourceRegion.UpdateCounts();
        outputRegion.UpdateCounts();
    }

    private static int PickWeightedIndex(Random rng, IReadOnlyList<double> weights)
    {
        var total = 0.0;
        for (var i = 0; i < weights.Count; i++)
        {
            total += Math.Max(0.0, weights[i]);
        }

        if (total <= 0)
        {
            return rng.Next(weights.Count);
        }

        var roll = rng.NextDouble() * total;
        var cumulative = 0.0;
        for (var i = 0; i < weights.Count; i++)
        {
            cumulative += Math.Max(0.0, weights[i]);
            if (roll <= cumulative)
            {
                return i;
            }
        }

        return weights.Count - 1;
    }

    private static double[] BuildActivationFunctionWeights()
    {
        var costs = new[]
        {
            0.0, // ACT_NONE
            1.0, // ACT_IDENTITY
            1.0, // ACT_STEP_UP
            1.0, // ACT_STEP_MID
            1.0, // ACT_STEP_DOWN
            1.1, // ACT_ABS
            1.1, // ACT_CLAMP
            1.1, // ACT_RELU
            1.1, // ACT_NRELU
            1.4, // ACT_SIN
            1.6, // ACT_TAN
            1.6, // ACT_TANH
            1.8, // ACT_ELU
            1.8, // ACT_EXP
            1.4, // ACT_PRELU
            1.9, // ACT_LOG
            1.2, // ACT_MULT
            1.2, // ACT_ADD
            2.0, // ACT_SIG
            2.0, // ACT_SILU
            1.3, // ACT_PCLAMP
            2.6, // ACT_MODL
            2.6, // ACT_MODR
            2.8, // ACT_SOFTP
            2.8, // ACT_SELU
            1.4, // ACT_LIN
            3.0, // ACT_LOGB
            3.5, // ACT_POW
            5.0, // ACT_GAUSS
            6.0  // ACT_QUAD
        };

        return BuildInverseCostWeights(costs, minWeight: 0.1, maxWeight: 2.4);
    }

    private static double[] BuildResetFunctionWeights()
    {
        var costs = new[]
        {
            0.2, // RESET_ZERO
            1.0, // RESET_HOLD
            1.0, // RESET_CLAMP_POTENTIAL
            1.0, // RESET_CLAMP1
            1.0, // RESET_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_NEG_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_TENTH_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_HALF_POTENTIAL_CLAMP_BUFFER
            1.2, // RESET_DOUBLE_POTENTIAL_CLAMP_BUFFER
            1.3, // RESET_FIVEX_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_NEG_TENTH_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_NEG_HALF_POTENTIAL_CLAMP_BUFFER
            1.2, // RESET_NEG_DOUBLE_POTENTIAL_CLAMP_BUFFER
            1.3, // RESET_NEG_FIVEX_POTENTIAL_CLAMP_BUFFER
            1.8, // RESET_INVERSE_POTENTIAL_CLAMP_BUFFER
            1.0, // RESET_POTENTIAL_CLAMP1
            1.0, // RESET_NEG_POTENTIAL_CLAMP1
            1.0, // RESET_HUNDREDTHS_POTENTIAL_CLAMP1
            1.0, // RESET_TENTH_POTENTIAL_CLAMP1
            1.0, // RESET_HALF_POTENTIAL_CLAMP1
            1.2, // RESET_DOUBLE_POTENTIAL_CLAMP1
            1.3, // RESET_FIVEX_POTENTIAL_CLAMP1
            1.0, // RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP1
            1.0, // RESET_NEG_TENTH_POTENTIAL_CLAMP1
            1.0, // RESET_NEG_HALF_POTENTIAL_CLAMP1
            1.2, // RESET_NEG_DOUBLE_POTENTIAL_CLAMP1
            1.3, // RESET_NEG_FIVEX_POTENTIAL_CLAMP1
            1.8, // RESET_INVERSE_POTENTIAL_CLAMP1
            1.0, // RESET_POTENTIAL
            1.0, // RESET_NEG_POTENTIAL
            1.0, // RESET_HUNDREDTHS_POTENTIAL
            1.0, // RESET_TENTH_POTENTIAL
            1.0, // RESET_HALF_POTENTIAL
            1.2, // RESET_DOUBLE_POTENTIAL
            1.3, // RESET_FIVEX_POTENTIAL
            1.0, // RESET_NEG_HUNDREDTHS_POTENTIAL
            1.0, // RESET_NEG_TENTH_POTENTIAL
            1.0, // RESET_NEG_HALF_POTENTIAL
            1.2, // RESET_NEG_DOUBLE_POTENTIAL
            1.3, // RESET_NEG_FIVEX_POTENTIAL
            1.8, // RESET_INVERSE_POTENTIAL
            1.0, // RESET_HALF
            1.0, // RESET_TENTH
            1.0, // RESET_HUNDREDTH
            1.0, // RESET_NEGATIVE
            1.0, // RESET_NEG_HALF
            1.0, // RESET_NEG_TENTH
            1.0, // RESET_NEG_HUNDREDTH
            1.2, // RESET_DOUBLE_CLAMP1
            1.3, // RESET_FIVEX_CLAMP1
            1.2, // RESET_NEG_DOUBLE_CLAMP1
            1.3, // RESET_NEG_FIVEX_CLAMP1
            1.2, // RESET_DOUBLE
            1.3, // RESET_FIVEX
            1.2, // RESET_NEG_DOUBLE
            1.3, // RESET_NEG_FIVEX
            1.1, // RESET_DIVIDE_AXON_CT
            1.8, // RESET_INVERSE_CLAMP1
            1.8  // RESET_INVERSE
        };

        return BuildInverseCostWeights(costs, minWeight: 0.1, maxWeight: 2.4);
    }

    private static double[] BuildAccumulationFunctionWeights()
    {
        var costs = new[] { 1.0, 1.2, 1.0, 0.1 };
        return BuildInverseCostWeights(costs, minWeight: 0.2, maxWeight: 3.0);
    }

    private static double[] BuildInverseCostWeights(IReadOnlyList<double> costs, double minWeight, double maxWeight)
    {
        var weights = new double[costs.Count];
        for (var i = 0; i < costs.Count; i++)
        {
            var cost = costs[i];
            var weight = cost <= 0.0 ? minWeight : 1.0 / cost;
            if (weight < minWeight)
            {
                weight = minWeight;
            }
            else if (weight > maxWeight)
            {
                weight = maxWeight;
            }

            weights[i] = weight;
        }

        return weights;
    }

    private static string BuildDefaultArtifactRoot()
    {
        var baseDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "Nbn.Workbench",
            "designer-artifacts");
        Directory.CreateDirectory(baseDir);
        return baseDir;
    }

    private async Task<bool> WaitForBrainRegistrationAsync(Guid brainId)
    {
        var deadline = DateTime.UtcNow + SpawnRegistrationTimeout;
        while (DateTime.UtcNow <= deadline)
        {
            var response = await _client.ListBrainsAsync().ConfigureAwait(false);
            if (IsBrainRegistered(response, brainId))
            {
                return true;
            }

            await Task.Delay(SpawnRegistrationPollInterval).ConfigureAwait(false);
        }

        return false;
    }

    private static bool IsBrainRegistered(Nbn.Proto.Settings.BrainListResponse? response, Guid brainId)
    {
        if (response?.Brains is null)
        {
            return false;
        }

        foreach (var entry in response.Brains)
        {
            if (entry.BrainId is null || !entry.BrainId.TryToGuid(out var candidate) || candidate != brainId)
            {
                continue;
            }

            return !string.Equals(entry.State, "Dead", StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    private string SuggestedName(string extension)
    {
        if (!string.IsNullOrWhiteSpace(_documentPath))
        {
            var name = Path.GetFileNameWithoutExtension(_documentPath);
            return $"{name}.{extension}";
        }

        return $"brain.{extension}";
    }

    private static async Task<IStorageFile?> PickOpenFileAsync(string title, string filterName, string extension)
    {
        var provider = GetStorageProvider();
        if (provider is null)
        {
            return null;
        }

        var options = new FilePickerOpenOptions
        {
            Title = title,
            AllowMultiple = false,
            FileTypeFilter = new List<FilePickerFileType>
            {
                new(filterName) { Patterns = new List<string> { $"*.{extension}" } }
            }
        };

        var results = await provider.OpenFilePickerAsync(options);
        return results.FirstOrDefault();
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

    private static async Task<byte[]> ReadAllBytesAsync(IStorageFile file)
    {
        await using var stream = await file.OpenReadAsync();
        using var buffer = new MemoryStream();
        await stream.CopyToAsync(buffer);
        return buffer.ToArray();
    }

    private static async Task WriteAllBytesAsync(IStorageFile file, byte[] bytes)
    {
        await using var stream = await file.OpenWriteAsync();
        stream.SetLength(0);
        await stream.WriteAsync(bytes);
        await stream.FlushAsync();
    }

    private static SharedShardPlanMode ToSharedShardPlanMode(ShardPlanMode mode)
        => mode switch
        {
            ShardPlanMode.FixedShardCount => SharedShardPlanMode.FixedShardCountPerRegion,
            ShardPlanMode.MaxNeuronsPerShard => SharedShardPlanMode.MaxNeuronsPerShard,
            _ => SharedShardPlanMode.SingleShardPerRegion
        };

    private static ProtoShardPlanMode ToProtoShardPlanMode(ShardPlanMode mode)
        => mode switch
        {
            ShardPlanMode.FixedShardCount => ProtoShardPlanMode.ShardPlanFixed,
            ShardPlanMode.MaxNeuronsPerShard => ProtoShardPlanMode.ShardPlanMaxNeurons,
            _ => ProtoShardPlanMode.ShardPlanSingle
        };

    private static ProtoControl.RequestPlacement BuildPlacementRequest(
        Guid brainId,
        int inputWidth,
        int outputWidth,
        string artifactSha,
        long artifactSize,
        ShardPlanMode shardPlanMode,
        int? shardCount,
        int? maxNeuronsPerShard,
        string artifactRoot)
    {
        var request = new ProtoControl.RequestPlacement
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = (uint)Math.Max(0, inputWidth),
            OutputWidth = (uint)Math.Max(0, outputWidth),
            ShardPlan = new ProtoControl.ShardPlan
            {
                Mode = ToProtoShardPlanMode(shardPlanMode)
            }
        };

        if (!string.IsNullOrWhiteSpace(artifactSha))
        {
            request.BaseDef = artifactSha.ToArtifactRef((ulong)Math.Max(0, artifactSize), "application/x-nbn", artifactRoot);
        }

        if (shardCount is { } count && count > 0)
        {
            request.ShardPlan.ShardCount = (uint)count;
        }

        if (maxNeuronsPerShard is { } max && max > 0)
        {
            request.ShardPlan.MaxNeuronsPerShard = (uint)max;
        }

        return request;
    }

    private static IReadOnlyList<ShardPlanOption> BuildShardPlanOptions()
        => new List<ShardPlanOption>
        {
            new("Single shard per region", ShardPlanMode.SingleShardPerRegion, "Use one shard per region (IO regions stay single)."),
            new("Fixed shard count", ShardPlanMode.FixedShardCount, "Split non-IO regions into N shards (stride-aligned)."),
            new("Max neurons per shard", ShardPlanMode.MaxNeuronsPerShard, "Split non-IO regions by target size (stride-aligned).")
        };

    private static bool TryParseOptionalNonNegativeInt(string value, out int? result)
    {
        result = null;
        if (string.IsNullOrWhiteSpace(value))
        {
            return true;
        }

        if (!int.TryParse(value, out var parsed) || parsed < 0)
        {
            return false;
        }

        if (parsed == 0)
        {
            return true;
        }

        result = parsed;
        return true;
    }

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return !string.IsNullOrWhiteSpace(value)
               && (string.Equals(value, "1", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "yes", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "on", StringComparison.OrdinalIgnoreCase));
    }

    private sealed class DesignerSpawnState
    {
        private DesignerSpawnState(Guid runtimeBrainId)
        {
            RuntimeBrainId = runtimeBrainId;
        }

        public static DesignerSpawnState Create(Guid runtimeBrainId)
            => new(runtimeBrainId);

        public Guid RuntimeBrainId { get; }
    }

    private static string FormatPath(IStorageItem item)
        => item.Path?.LocalPath ?? item.Path?.ToString() ?? item.Name;

    private static int Clamp(int value, int min, int max)
        => value < min ? min : value > max ? max : value;

    private static double Clamp(double value, double min, double max)
        => value < min ? min : value > max ? max : value;
}

public enum DesignerDocumentType
{
    None,
    Nbn,
    Nbs
}

public enum ShardPlanMode
{
    SingleShardPerRegion,
    FixedShardCount,
    MaxNeuronsPerShard
}

public sealed record ShardPlanOption(string Label, ShardPlanMode Value, string Description);

