using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Platform.Storage;
using Nbn.Proto;
using Nbn.Proto.Repro;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class ReproPanelViewModel : ViewModelBase
{
    private readonly WorkbenchClient _client;
    private readonly ConnectionViewModel? _connections;
    private readonly IWorkbenchArtifactPublisher _artifactPublisher;
    private string _parentAGuidText = string.Empty;
    private string _parentBGuidText = string.Empty;
    private string _parentADefPath = string.Empty;
    private string _parentAStatePath = string.Empty;
    private string _parentBDefPath = string.Empty;
    private string _parentBStatePath = string.Empty;
    private string _artifactStoreRoot = BuildDefaultArtifactRoot();
    private string _seedText = string.Empty;
    private string _maxRegionSpanDiffRatio = ReproductionSettings.FormatFloat(ReproductionSettings.DefaultMaxRegionSpanDiffRatio);
    private string _maxFunctionHistDistance = ReproductionSettings.FormatFloat(ReproductionSettings.DefaultMaxFunctionHistDistance);
    private string _maxConnectivityHistDistance = ReproductionSettings.FormatFloat(ReproductionSettings.DefaultMaxConnectivityHistDistance);
    private string _maxAvgOutDegree = "100";
    private string _perRegionOutDegreeCaps = string.Empty;
    private string _probAddNeuronToEmptyRegion = "0";
    private string _probRemoveLastNeuronFromRegion = "0";
    private string _probDisableNeuron = "0.01";
    private string _probReactivateNeuron = "0.01";
    private string _probAddAxon = "0.05";
    private string _probRemoveAxon = "0.02";
    private string _probRerouteAxon = "0.02";
    private string _probRerouteInboundAxonOnDelete = "0.50";
    private string _inboundRerouteMaxRingDistance = "0";
    private string _probChooseParentA = "0.45";
    private string _probChooseParentB = "0.45";
    private string _probAverage = "0.05";
    private string _probMutate = "0.05";
    private string _probChooseFuncA = "0.50";
    private string _probMutateFunc = "0.02";
    private bool _strengthTransformEnabled;
    private string _probStrengthChooseA = "0.35";
    private string _probStrengthChooseB = "0.35";
    private string _probStrengthAverage = "0.20";
    private string _probStrengthWeightedAverage = "0.05";
    private string _strengthWeightA = "0.50";
    private string _strengthWeightB = "0.50";
    private string _probStrengthMutate = "0.05";
    private string _maxNeuronsAddedAbs = "0";
    private string _maxNeuronsAddedPct = "0";
    private string _maxNeuronsRemovedAbs = "0";
    private string _maxNeuronsRemovedPct = "0";
    private string _maxAxonsAddedAbs = "0";
    private string _maxAxonsAddedPct = "0";
    private string _maxAxonsRemovedAbs = "0";
    private string _maxAxonsRemovedPct = "0";
    private string _maxRegionsAddedAbs = "0";
    private string _maxRegionsRemovedAbs = "0";
    private StrengthSourceOption _selectedStrengthSource;
    private SpawnPolicyOption _selectedSpawnPolicy;
    private PrunePolicyOption _selectedPrunePolicy;
    private ReproBrainOption? _selectedParentABrain;
    private ReproBrainOption? _selectedParentBBrain;
    private string _status = "Idle";
    private string _similaritySummary = "No result yet.";
    private string _mutationSummary = "No result yet.";

    public ReproPanelViewModel(
        WorkbenchClient client,
        ConnectionViewModel? connections = null,
        IWorkbenchArtifactPublisher? artifactPublisher = null)
    {
        _client = client;
        _connections = connections;
        _artifactPublisher = artifactPublisher ?? new WorkbenchArtifactPublisher(logInfo: WorkbenchLog.Info, logWarn: WorkbenchLog.Warn);
        StrengthSources = new List<StrengthSourceOption>
        {
            new("Base only", StrengthSource.StrengthBaseOnly),
            new("Live codes", StrengthSource.StrengthLiveCodes)
        };
        SpawnPolicies = new List<SpawnPolicyOption>
        {
            new("Default (On)", SpawnChildPolicy.SpawnChildDefaultOn),
            new("Always", SpawnChildPolicy.SpawnChildAlways),
            new("Never", SpawnChildPolicy.SpawnChildNever)
        };
        PrunePolicies = new List<PrunePolicyOption>
        {
            new("Lowest abs strength", PrunePolicy.PruneLowestAbsStrengthFirst),
            new("New connections first", PrunePolicy.PruneNewConnectionsFirst),
            new("Random", PrunePolicy.PruneRandom)
        };
        _selectedStrengthSource = StrengthSources[0];
        _selectedSpawnPolicy = SpawnPolicies[0];
        _selectedPrunePolicy = PrunePolicies[0];

        ActiveBrains = new ObservableCollection<ReproBrainOption>();

        RunCommand = new AsyncRelayCommand(RunAsync);
        BrowseParentADefCommand = new AsyncRelayCommand(() => BrowseParentFileAsync(ParentFileKind.ParentADef));
        BrowseParentAStateCommand = new AsyncRelayCommand(() => BrowseParentFileAsync(ParentFileKind.ParentAState));
        BrowseParentBDefCommand = new AsyncRelayCommand(() => BrowseParentFileAsync(ParentFileKind.ParentBDef));
        BrowseParentBStateCommand = new AsyncRelayCommand(() => BrowseParentFileAsync(ParentFileKind.ParentBState));
        ClearParentFilesCommand = new RelayCommand(ClearParentFiles);
    }

    public IReadOnlyList<StrengthSourceOption> StrengthSources { get; }

    public IReadOnlyList<SpawnPolicyOption> SpawnPolicies { get; }

    public IReadOnlyList<PrunePolicyOption> PrunePolicies { get; }

    public ObservableCollection<ReproBrainOption> ActiveBrains { get; }

    public StrengthSourceOption SelectedStrengthSource
    {
        get => _selectedStrengthSource;
        set => SetProperty(ref _selectedStrengthSource, value);
    }

    public SpawnPolicyOption SelectedSpawnPolicy
    {
        get => _selectedSpawnPolicy;
        set => SetProperty(ref _selectedSpawnPolicy, value);
    }

    public PrunePolicyOption SelectedPrunePolicy
    {
        get => _selectedPrunePolicy;
        set => SetProperty(ref _selectedPrunePolicy, value);
    }

    public ReproBrainOption? SelectedParentABrain
    {
        get => _selectedParentABrain;
        set => SetProperty(ref _selectedParentABrain, value);
    }

    public ReproBrainOption? SelectedParentBBrain
    {
        get => _selectedParentBBrain;
        set => SetProperty(ref _selectedParentBBrain, value);
    }

    public string ParentAGuidText
    {
        get => _parentAGuidText;
        set => SetProperty(ref _parentAGuidText, value);
    }

    public string ParentBGuidText
    {
        get => _parentBGuidText;
        set => SetProperty(ref _parentBGuidText, value);
    }

    public string ParentADefPath
    {
        get => _parentADefPath;
        set => SetProperty(ref _parentADefPath, value);
    }

    public string ParentAStatePath
    {
        get => _parentAStatePath;
        set => SetProperty(ref _parentAStatePath, value);
    }

    public string ParentBDefPath
    {
        get => _parentBDefPath;
        set => SetProperty(ref _parentBDefPath, value);
    }

    public string ParentBStatePath
    {
        get => _parentBStatePath;
        set => SetProperty(ref _parentBStatePath, value);
    }

    public string ArtifactStoreRoot
    {
        get => _artifactStoreRoot;
        set => SetProperty(ref _artifactStoreRoot, value);
    }

    public string SeedText
    {
        get => _seedText;
        set => SetProperty(ref _seedText, value);
    }

    public string MaxRegionSpanDiffRatio
    {
        get => _maxRegionSpanDiffRatio;
        set => SetProperty(ref _maxRegionSpanDiffRatio, value);
    }

    public string MaxFunctionHistDistance
    {
        get => _maxFunctionHistDistance;
        set => SetProperty(ref _maxFunctionHistDistance, value);
    }

    public string MaxConnectivityHistDistance
    {
        get => _maxConnectivityHistDistance;
        set => SetProperty(ref _maxConnectivityHistDistance, value);
    }

    public string MaxAvgOutDegree
    {
        get => _maxAvgOutDegree;
        set => SetProperty(ref _maxAvgOutDegree, value);
    }

    public string PerRegionOutDegreeCaps
    {
        get => _perRegionOutDegreeCaps;
        set => SetProperty(ref _perRegionOutDegreeCaps, value);
    }

    public string ProbAddNeuronToEmptyRegion
    {
        get => _probAddNeuronToEmptyRegion;
        set => SetProperty(ref _probAddNeuronToEmptyRegion, value);
    }

    public string ProbRemoveLastNeuronFromRegion
    {
        get => _probRemoveLastNeuronFromRegion;
        set => SetProperty(ref _probRemoveLastNeuronFromRegion, value);
    }

    public string ProbDisableNeuron
    {
        get => _probDisableNeuron;
        set => SetProperty(ref _probDisableNeuron, value);
    }

    public string ProbReactivateNeuron
    {
        get => _probReactivateNeuron;
        set => SetProperty(ref _probReactivateNeuron, value);
    }

    public string ProbAddAxon
    {
        get => _probAddAxon;
        set => SetProperty(ref _probAddAxon, value);
    }

    public string ProbRemoveAxon
    {
        get => _probRemoveAxon;
        set => SetProperty(ref _probRemoveAxon, value);
    }

    public string ProbRerouteAxon
    {
        get => _probRerouteAxon;
        set => SetProperty(ref _probRerouteAxon, value);
    }

    public string ProbRerouteInboundAxonOnDelete
    {
        get => _probRerouteInboundAxonOnDelete;
        set => SetProperty(ref _probRerouteInboundAxonOnDelete, value);
    }

    public string InboundRerouteMaxRingDistance
    {
        get => _inboundRerouteMaxRingDistance;
        set => SetProperty(ref _inboundRerouteMaxRingDistance, value);
    }

    public string ProbChooseParentA
    {
        get => _probChooseParentA;
        set => SetProperty(ref _probChooseParentA, value);
    }

    public string ProbChooseParentB
    {
        get => _probChooseParentB;
        set => SetProperty(ref _probChooseParentB, value);
    }

    public string ProbAverage
    {
        get => _probAverage;
        set => SetProperty(ref _probAverage, value);
    }

    public string ProbMutate
    {
        get => _probMutate;
        set => SetProperty(ref _probMutate, value);
    }

    public string ProbChooseFuncA
    {
        get => _probChooseFuncA;
        set => SetProperty(ref _probChooseFuncA, value);
    }

    public string ProbMutateFunc
    {
        get => _probMutateFunc;
        set => SetProperty(ref _probMutateFunc, value);
    }

    public bool StrengthTransformEnabled
    {
        get => _strengthTransformEnabled;
        set => SetProperty(ref _strengthTransformEnabled, value);
    }

    public string ProbStrengthChooseA
    {
        get => _probStrengthChooseA;
        set => SetProperty(ref _probStrengthChooseA, value);
    }

    public string ProbStrengthChooseB
    {
        get => _probStrengthChooseB;
        set => SetProperty(ref _probStrengthChooseB, value);
    }

    public string ProbStrengthAverage
    {
        get => _probStrengthAverage;
        set => SetProperty(ref _probStrengthAverage, value);
    }

    public string ProbStrengthWeightedAverage
    {
        get => _probStrengthWeightedAverage;
        set => SetProperty(ref _probStrengthWeightedAverage, value);
    }

    public string StrengthWeightA
    {
        get => _strengthWeightA;
        set => SetProperty(ref _strengthWeightA, value);
    }

    public string StrengthWeightB
    {
        get => _strengthWeightB;
        set => SetProperty(ref _strengthWeightB, value);
    }

    public string ProbStrengthMutate
    {
        get => _probStrengthMutate;
        set => SetProperty(ref _probStrengthMutate, value);
    }

    public string MaxNeuronsAddedAbs
    {
        get => _maxNeuronsAddedAbs;
        set => SetProperty(ref _maxNeuronsAddedAbs, value);
    }

    public string MaxNeuronsAddedPct
    {
        get => _maxNeuronsAddedPct;
        set => SetProperty(ref _maxNeuronsAddedPct, value);
    }

    public string MaxNeuronsRemovedAbs
    {
        get => _maxNeuronsRemovedAbs;
        set => SetProperty(ref _maxNeuronsRemovedAbs, value);
    }

    public string MaxNeuronsRemovedPct
    {
        get => _maxNeuronsRemovedPct;
        set => SetProperty(ref _maxNeuronsRemovedPct, value);
    }

    public string MaxAxonsAddedAbs
    {
        get => _maxAxonsAddedAbs;
        set => SetProperty(ref _maxAxonsAddedAbs, value);
    }

    public string MaxAxonsAddedPct
    {
        get => _maxAxonsAddedPct;
        set => SetProperty(ref _maxAxonsAddedPct, value);
    }

    public string MaxAxonsRemovedAbs
    {
        get => _maxAxonsRemovedAbs;
        set => SetProperty(ref _maxAxonsRemovedAbs, value);
    }

    public string MaxAxonsRemovedPct
    {
        get => _maxAxonsRemovedPct;
        set => SetProperty(ref _maxAxonsRemovedPct, value);
    }

    public string MaxRegionsAddedAbs
    {
        get => _maxRegionsAddedAbs;
        set => SetProperty(ref _maxRegionsAddedAbs, value);
    }

    public string MaxRegionsRemovedAbs
    {
        get => _maxRegionsRemovedAbs;
        set => SetProperty(ref _maxRegionsRemovedAbs, value);
    }

    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }

    public string SimilaritySummary
    {
        get => _similaritySummary;
        set => SetProperty(ref _similaritySummary, value);
    }

    public string MutationSummary
    {
        get => _mutationSummary;
        set => SetProperty(ref _mutationSummary, value);
    }

    public AsyncRelayCommand RunCommand { get; }

    public AsyncRelayCommand BrowseParentADefCommand { get; }

    public AsyncRelayCommand BrowseParentAStateCommand { get; }

    public AsyncRelayCommand BrowseParentBDefCommand { get; }

    public AsyncRelayCommand BrowseParentBStateCommand { get; }

    public RelayCommand ClearParentFilesCommand { get; }
}

public sealed record ReproBrainOption(Guid BrainId, string Label)
{
    public string BrainIdLabel => BrainId.ToString("D");
}

public sealed record StrengthSourceOption(string Label, StrengthSource Value);

public sealed record SpawnPolicyOption(string Label, SpawnChildPolicy Value);

public sealed record PrunePolicyOption(string Label, PrunePolicy Value);
