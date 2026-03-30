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

/// <summary>
/// Coordinates Workbench Designer document editing, artifact workflows, and spawn preparation.
/// </summary>
public sealed partial class DesignerPanelViewModel : ViewModelBase
{
    private const string NoDocumentStatus = "No file loaded.";
    private const string NoDesignStatus = "Create or import a .nbn to edit.";
    private const long StaleControllerMs = 15000;
    private const ulong PlacementWaitTimeoutMs = 5_000;
    private static readonly TimeSpan SpawnRegistrationTimeout = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan SpawnRegistrationPollInterval = TimeSpan.FromMilliseconds(300);
    private static readonly bool LogSpawnDiagnostics = IsEnvTrue("NBN_WORKBENCH_SPAWN_DIAGNOSTICS_ENABLED");
    private const int DefaultActivationFunctionId = 11; // ACT_TANH (internal)
    private const int DefaultInputActivationFunctionId = 1; // ACT_IDENTITY
    private const int DefaultOutputActivationFunctionId = 11; // ACT_TANH
    private const int DefaultInputResetFunctionId = 0; // RESET_ZERO
    private const int ResetHoldFunctionId = 1; // RESET_HOLD
    private const int ResetHalfFunctionId = 43; // RESET_HALF
    private const int ResetTenthFunctionId = 44; // RESET_TENTH
    private const int ResetHundredthFunctionId = 45; // RESET_HUNDREDTH
    private const int ActivityDriverActivationFunctionId = 17; // ACT_ADD
    private const int ActivityDriverResetFunctionId = 0; // RESET_ZERO
    private const int ActivityDriverParamACode = 63;
    private const int ActivityDriverStrengthCode = 31;
    private const int GuidedPathStrengthMinCode = 20;
    private const int GuidedPathStrengthMaxCode = 27;
    private const int RecurrentBridgeStrengthMinCode = 17;
    private const int RecurrentBridgeStrengthMaxCode = 19;
    private const double OutputEndpointRecurrenceProbability = 0.82;
    private const double OutputEndpointRecurrenceProbabilityWithoutBaseline = 0.62;
    private const int AccumulationFunctionSumId = 0;
    private const int AccumulationFunctionNoneId = 3;
    private const int MaxRandomPreActivationThresholdCode = 36;
    private const int MaxRandomActivationThresholdCode = 40;
    private const int MaxRandomOutputPreActivationThresholdCode = 16;
    private const int MaxRandomOutputActivationThresholdCode = 16;
    private const int MinRandomParamCode = 13;
    private const int MaxRandomParamCode = 50;
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
    private const double InterRegionWeight = 0.48;
    private const double IntraRegionWeight = 2.3;
    private const double ForwardRegionWeight = 1.0;
    private const double BackwardRegionWeight = 1.05;
    private const double OutputRegionWeight = 1.12;
    private const double AxonCountLowBiasPower = 1.7;
    private const double OutputAxonCountLowBiasPower = 2.6;
    private static readonly int[] ActivationFunctionIds = Enumerable.Range(0, MaxKnownActivationFunctionId + 1).ToArray();
    private static readonly int[] RandomInternalActivationFunctionIds = { 1, 2, 3, 4, 6, 9, 11, 18, 20 };
    private static readonly int[] ParamAActivationFunctionIds = { 12, 14, 16, 17, 20, 21, 22, 24, 25, 26, 27, 29 };
    private static readonly int[] ParamBActivationFunctionIds = { 20, 24, 25, 29 };
    private static readonly int[] InputAllowedActivationFunctionIds = { 1, 6, 7, 11, 16, 17, 25 };
    private static readonly int[] OutputAllowedActivationFunctionIds = { 1, 5, 6, 7, 8, 11, 14, 16, 17, 18, 19, 20, 23, 24, 25 };
    private static readonly int[] RandomOutputActivationFunctionIds = { 6, 11, 18 };
    private static readonly int[] ResetFunctionIds = Enumerable.Range(0, MaxKnownResetFunctionId + 1).ToArray();
    private static readonly int[] InputAllowedResetFunctionIds = { 0, 1, 3, 17, 30 };
    private static readonly int[] RandomOutputResetFunctionIds = { DefaultInputResetFunctionId };
    private static readonly int[] RandomInternalResetFunctionIds =
    {
        ResetHoldFunctionId,
        ResetHalfFunctionId,
        ResetTenthFunctionId,
        ResetHundredthFunctionId,
        DefaultInputResetFunctionId
    };
    private static readonly int[] AccumulationFunctionIds = { 0, 1, 2, 3 };
    private static readonly int[] RandomAccumulationFunctionIds = { 0, 1, 2 };
    private static readonly double[] RandomAccumulationFunctionWeights = { 1.3, 0.35, 1.1 };
    private static readonly double[] ActivationFunctionWeights = BuildActivationFunctionWeights();
    private static readonly double[] ResetFunctionWeights = BuildResetFunctionWeights();
    private static readonly double[] AccumulationFunctionWeights = BuildAccumulationFunctionWeights();
    private readonly ConnectionViewModel _connections;
    private readonly WorkbenchClient _client;
    private readonly IWorkbenchArtifactPublisher _artifactPublisher;
    private readonly Action<Guid>? _brainDiscovered;
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
    private readonly IReadOnlyList<ShardPlanOption> _shardPlanOptions;
    private ShardPlanOption _selectedShardPlan;
    private string _artifactStoreUri = BuildDefaultArtifactRoot();
    private string _definitionArtifactShaText = string.Empty;
    private string _definitionArtifactStoreUriText = string.Empty;
    private string _definitionArtifactSummary = "No definition artifact stored.";
    private string _snapshotArtifactShaText = string.Empty;
    private string _snapshotArtifactStoreUriText = string.Empty;
    private string _snapshotArtifactSummary = "No snapshot artifact stored.";
    private string _spawnArtifactRoot = BuildDefaultArtifactRoot();
    private string _spawnShardCountText = "1";
    private string _spawnShardTargetNeuronsText = "0";
    private readonly RandomBrainOptionsViewModel _randomOptions;

    /// <summary>
    /// Initializes the Designer panel state and commands.
    /// </summary>
    public DesignerPanelViewModel(
        ConnectionViewModel connections,
        WorkbenchClient client,
        Action<Guid>? brainDiscovered = null,
        IWorkbenchArtifactPublisher? artifactPublisher = null)
    {
        _connections = connections;
        _client = client;
        _artifactPublisher = artifactPublisher ?? new WorkbenchArtifactPublisher(logInfo: WorkbenchLog.Info, logWarn: WorkbenchLog.Warn);
        _brainDiscovered = brainDiscovered;
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
        SaveDefinitionArtifactCommand = new AsyncRelayCommand(SaveDefinitionArtifactAsync, () => IsDesignLoaded);
        SaveSnapshotArtifactCommand = new AsyncRelayCommand(SaveSnapshotArtifactAsync, () => IsDesignLoaded || IsSnapshotLoaded);
        LoadDefinitionArtifactCommand = new AsyncRelayCommand(LoadDefinitionArtifactAsync, CanLoadDefinitionArtifact);
        LoadSnapshotArtifactCommand = new AsyncRelayCommand(LoadSnapshotArtifactAsync, CanLoadSnapshotArtifact);
        RestoreArtifactBrainCommand = new AsyncRelayCommand(RestoreArtifactBrainAsync, CanRestoreArtifactBrain);
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

    public string ArtifactStoreUri
    {
        get => _artifactStoreUri;
        set => SetProperty(ref _artifactStoreUri, value);
    }

    public string DefinitionArtifactShaText
    {
        get => _definitionArtifactShaText;
        set
        {
            if (SetProperty(ref _definitionArtifactShaText, value))
            {
                LoadDefinitionArtifactCommand.RaiseCanExecuteChanged();
                RestoreArtifactBrainCommand.RaiseCanExecuteChanged();
            }
        }
    }

    public string DefinitionArtifactStoreUriText
    {
        get => _definitionArtifactStoreUriText;
        set => SetProperty(ref _definitionArtifactStoreUriText, value);
    }

    public string DefinitionArtifactSummary
    {
        get => _definitionArtifactSummary;
        private set => SetProperty(ref _definitionArtifactSummary, value);
    }

    public string SnapshotArtifactShaText
    {
        get => _snapshotArtifactShaText;
        set
        {
            if (SetProperty(ref _snapshotArtifactShaText, value))
            {
                LoadSnapshotArtifactCommand.RaiseCanExecuteChanged();
            }
        }
    }

    public string SnapshotArtifactStoreUriText
    {
        get => _snapshotArtifactStoreUriText;
        set => SetProperty(ref _snapshotArtifactStoreUriText, value);
    }

    public string SnapshotArtifactSummary
    {
        get => _snapshotArtifactSummary;
        private set => SetProperty(ref _snapshotArtifactSummary, value);
    }

    public bool SnapshotIncludeEnabledBitset
    {
        get => _snapshotIncludeEnabledBitset;
        set => SetProperty(ref _snapshotIncludeEnabledBitset, value);
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
    public AsyncRelayCommand SaveDefinitionArtifactCommand { get; }
    public AsyncRelayCommand SaveSnapshotArtifactCommand { get; }
    public AsyncRelayCommand LoadDefinitionArtifactCommand { get; }
    public AsyncRelayCommand LoadSnapshotArtifactCommand { get; }
    public AsyncRelayCommand RestoreArtifactBrainCommand { get; }
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

    private void SelectHomeNeuron(DesignerBrainViewModel brain)
    {
        var inputRegion = brain.Regions[NbnConstants.InputRegionId];
        SelectRegion(inputRegion);
        SelectNeuron(inputRegion.Neurons.FirstOrDefault());
    }

    private void ApplyLoadedBrainDocument(
        DesignerBrainViewModel brain,
        string loadedLabel,
        string status,
        bool isDirty,
        string? documentPath = null)
    {
        SetDocumentType(DesignerDocumentType.Nbn);
        Brain = brain;
        _snapshotBytes = null;
        _documentPath = documentPath;
        _nbsHeader = null;
        _nbsRegions = null;
        _nbsOverlay = null;

        SelectHomeNeuron(brain);
        LoadedSummary = BuildDesignSummary(brain, loadedLabel);
        Status = status;
        SetDesignDirty(isDirty);
        ResetValidation();
        RefreshRegionView();
        RaiseDocumentWorkflowCommandStates();
    }

    private void ApplyLoadedSnapshotDocument(
        byte[] snapshotBytes,
        string loadedLabel,
        NbsHeaderV2 header,
        IReadOnlyList<NbsRegionSection> regions,
        NbsOverlaySection? overlay,
        string status,
        string? documentPath = null)
    {
        SetDocumentType(DesignerDocumentType.Nbs);
        _snapshotBytes = snapshotBytes;
        _documentPath = documentPath;
        _nbsHeader = header;
        _nbsRegions = regions;
        _nbsOverlay = overlay;
        Brain = null;
        SelectRegion(null);
        ClearSelection();

        LoadedSummary = BuildNbsSummary(loadedLabel, header, regions, overlay);
        Status = status;
        SetDesignDirty(false);
        ResetValidation();
        RaiseDocumentWorkflowCommandStates();
    }

    private void RaiseDocumentWorkflowCommandStates()
    {
        ExportCommand.RaiseCanExecuteChanged();
        ValidateCommand.RaiseCanExecuteChanged();
        SpawnBrainCommand.RaiseCanExecuteChanged();
        ResetBrainCommand.RaiseCanExecuteChanged();
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

/// <summary>
/// Tracks which document type is currently loaded into the Designer.
/// </summary>
public enum DesignerDocumentType
{
    None,
    Nbn,
    Nbs
}

/// <summary>
/// Describes how the Designer should ask runtime placement to shard a spawned brain.
/// </summary>
public enum ShardPlanMode
{
    SingleShardPerRegion,
    FixedShardCount,
    MaxNeuronsPerShard
}

/// <summary>
/// Labels a shard-plan choice for binding in the Designer UI.
/// </summary>
public sealed record ShardPlanOption(string Label, ShardPlanMode Value, string Description);
