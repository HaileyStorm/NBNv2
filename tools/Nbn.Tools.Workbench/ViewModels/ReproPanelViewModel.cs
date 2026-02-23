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

public sealed class ReproPanelViewModel : ViewModelBase
{
    private readonly WorkbenchClient _client;
    private string _parentAGuidText = string.Empty;
    private string _parentBGuidText = string.Empty;
    private string _parentADefPath = string.Empty;
    private string _parentAStatePath = string.Empty;
    private string _parentBDefPath = string.Empty;
    private string _parentBStatePath = string.Empty;
    private string _artifactStoreRoot = BuildDefaultArtifactRoot();
    private string _seedText = string.Empty;
    private string _maxRegionSpanDiffRatio = "0.15";
    private string _maxFunctionHistDistance = "0.25";
    private string _maxConnectivityHistDistance = "0.25";
    private string _maxAvgOutDegree = "100";
    private string _perRegionOutDegreeCaps = string.Empty;
    private string _probAddNeuronToEmptyRegion = "0.02";
    private string _probRemoveLastNeuronFromRegion = "0.01";
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

    public ReproPanelViewModel(WorkbenchClient client)
    {
        _client = client;
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

    public void UpdateActiveBrains(IReadOnlyList<BrainListItem> brains)
    {
        var selectedA = SelectedParentABrain?.BrainId;
        var selectedB = SelectedParentBBrain?.BrainId;

        var active = brains
            .Where(entry => !string.Equals(entry.State, "Dead", StringComparison.OrdinalIgnoreCase))
            .OrderBy(entry => entry.BrainId)
            .Select(entry => new ReproBrainOption(entry.BrainId, entry.Display))
            .ToList();

        ActiveBrains.Clear();
        foreach (var entry in active)
        {
            ActiveBrains.Add(entry);
        }

        SelectedParentABrain = selectedA.HasValue
            ? ActiveBrains.FirstOrDefault(item => item.BrainId == selectedA.Value)
            : ActiveBrains.FirstOrDefault();

        SelectedParentBBrain = selectedB.HasValue
            ? ActiveBrains.FirstOrDefault(item => item.BrainId == selectedB.Value)
            : ActiveBrains.Skip(1).FirstOrDefault();
    }

    private async Task RunAsync()
    {
        var config = BuildConfig();
        var seed = ParseUlong(SeedText, (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        Status = "Submitting request...";

        Nbn.Proto.Repro.ReproduceResult? result;
        if (ShouldUseArtifactParents())
        {
            result = await RunByArtifactsAsync(config, seed);
        }
        else
        {
            result = await RunByBrainIdsAsync(config, seed);
        }

        if (result is null)
        {
            Status = "Repro request failed.";
            return;
        }

        var report = result.Report;
        var summary = result.Summary;
        var score = ComputeSimilarityScore(report);
        var compatibleText = report is null ? "Unknown" : report.Compatible ? "True" : "False";
        var abortReason = ResolveAbortReason(report);
        var regionSpanScore = report?.RegionSpanScore ?? 0f;
        var functionScore = report?.FunctionScore ?? 0f;
        var connectivityScore = report?.ConnectivityScore ?? 0f;
        var childRegionCount = report?.RegionsPresentChild ?? 0u;

        SimilaritySummary = $"Score: {score:0.000} | Compatible: {compatibleText} | Abort: {abortReason} | Region span: {regionSpanScore:0.000} | Function: {functionScore:0.000} | Connectivity: {connectivityScore:0.000} | Child regions: {childRegionCount}";
        MutationSummary = $"+N{summary?.NeuronsAdded ?? 0u} -N{summary?.NeuronsRemoved ?? 0u} +A{summary?.AxonsAdded ?? 0u} -A{summary?.AxonsRemoved ?? 0u} reroute={summary?.AxonsRerouted ?? 0u} func={summary?.FunctionsMutated ?? 0u} strength={summary?.StrengthCodesChanged ?? 0u}";

        var childLabel = result.ChildBrainId is not null && result.ChildBrainId.TryToGuid(out var childGuid)
            ? childGuid.ToString("D")
            : "unknown";
        if (result.Spawned)
        {
            Status = report is not null && !report.Compatible
                ? $"Spawned child {childLabel} (abort: {abortReason})"
                : $"Spawned child {childLabel}";
            return;
        }

        Status = report is not null && !report.Compatible
            ? $"Aborted: {abortReason} (not spawned)."
            : "Completed (not spawned).";
    }

    private bool ShouldUseArtifactParents()
        => !string.IsNullOrWhiteSpace(ParentADefPath) && !string.IsNullOrWhiteSpace(ParentBDefPath);

    private async Task<Nbn.Proto.Repro.ReproduceResult?> RunByBrainIdsAsync(ReproduceConfig config, ulong seed)
    {
        if (!TryResolveParentId(SelectedParentABrain, ParentAGuidText, out var parentA)
            || !TryResolveParentId(SelectedParentBBrain, ParentBGuidText, out var parentB))
        {
            Status = "Select active parents or provide valid parent GUIDs.";
            return null;
        }

        if (parentA == parentB)
        {
            Status = "Parent A and Parent B must be different.";
            return null;
        }

        var request = new ReproduceByBrainIdsRequest
        {
            ParentA = parentA.ToProtoUuid(),
            ParentB = parentB.ToProtoUuid(),
            StrengthSource = SelectedStrengthSource.Value,
            Config = config,
            Seed = seed
        };

        return await _client.ReproduceByBrainIdsAsync(request);
    }

    private async Task<Nbn.Proto.Repro.ReproduceResult?> RunByArtifactsAsync(ReproduceConfig config, ulong seed)
    {
        try
        {
            var parentADef = await StoreArtifactReferenceAsync(ParentADefPath, "application/x-nbn");
            var parentBDef = await StoreArtifactReferenceAsync(ParentBDefPath, "application/x-nbn");

            ArtifactRef? parentAState = null;
            ArtifactRef? parentBState = null;
            if (!string.IsNullOrWhiteSpace(ParentAStatePath))
            {
                parentAState = await StoreArtifactReferenceAsync(ParentAStatePath, "application/x-nbs");
            }

            if (!string.IsNullOrWhiteSpace(ParentBStatePath))
            {
                parentBState = await StoreArtifactReferenceAsync(ParentBStatePath, "application/x-nbs");
            }

            var request = new ReproduceByArtifactsRequest
            {
                ParentADef = parentADef,
                ParentBDef = parentBDef,
                ParentAState = parentAState,
                ParentBState = parentBState,
                StrengthSource = SelectedStrengthSource.Value,
                Config = config,
                Seed = seed
            };

            return await _client.ReproduceByArtifactsAsync(request);
        }
        catch (Exception ex)
        {
            Status = $"Artifact upload failed: {ex.Message}";
            return null;
        }
    }

    private async Task<ArtifactRef> StoreArtifactReferenceAsync(string filePath, string mediaType)
    {
        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new InvalidOperationException("Artifact path is required.");
        }

        var fullPath = Path.GetFullPath(filePath);
        if (!File.Exists(fullPath))
        {
            throw new FileNotFoundException("Artifact file not found.", fullPath);
        }

        var artifactRoot = string.IsNullOrWhiteSpace(ArtifactStoreRoot)
            ? BuildDefaultArtifactRoot()
            : ArtifactStoreRoot;
        Directory.CreateDirectory(artifactRoot);

        var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
        await using var stream = File.OpenRead(fullPath);
        var manifest = await store.StoreAsync(stream, mediaType);
        return manifest.ArtifactId.ToHex().ToArtifactRef((ulong)Math.Max(0, manifest.ByteLength), mediaType, artifactRoot);
    }

    private ReproduceConfig BuildConfig()
    {
        return new ReproduceConfig
        {
            MaxRegionSpanDiffRatio = ParseFloat(MaxRegionSpanDiffRatio, 0.15f),
            MaxFunctionHistDistance = ParseFloat(MaxFunctionHistDistance, 0.25f),
            MaxConnectivityHistDistance = ParseFloat(MaxConnectivityHistDistance, 0.25f),
            ProbAddNeuronToEmptyRegion = ParseFloat(ProbAddNeuronToEmptyRegion, 0.02f),
            ProbRemoveLastNeuronFromRegion = ParseFloat(ProbRemoveLastNeuronFromRegion, 0.01f),
            ProbDisableNeuron = ParseFloat(ProbDisableNeuron, 0.01f),
            ProbReactivateNeuron = ParseFloat(ProbReactivateNeuron, 0.01f),
            ProbAddAxon = ParseFloat(ProbAddAxon, 0.05f),
            ProbRemoveAxon = ParseFloat(ProbRemoveAxon, 0.02f),
            ProbRerouteAxon = ParseFloat(ProbRerouteAxon, 0.02f),
            ProbRerouteInboundAxonOnDelete = ParseFloat(ProbRerouteInboundAxonOnDelete, 0.50f),
            InboundRerouteMaxRingDistance = ParseUInt(InboundRerouteMaxRingDistance, 0),
            ProbChooseParentA = ParseFloat(ProbChooseParentA, 0.45f),
            ProbChooseParentB = ParseFloat(ProbChooseParentB, 0.45f),
            ProbAverage = ParseFloat(ProbAverage, 0.05f),
            ProbMutate = ParseFloat(ProbMutate, 0.05f),
            ProbChooseFuncA = ParseFloat(ProbChooseFuncA, 0.50f),
            ProbMutateFunc = ParseFloat(ProbMutateFunc, 0.02f),
            MaxAvgOutDegreeBrain = ParseFloat(MaxAvgOutDegree, 100f),
            PrunePolicy = SelectedPrunePolicy.Value,
            PerRegionOutDegreeCaps = { ParsePerRegionOutDegreeCaps(PerRegionOutDegreeCaps) },
            StrengthTransformEnabled = StrengthTransformEnabled,
            ProbStrengthChooseA = ParseFloat(ProbStrengthChooseA, 0.35f),
            ProbStrengthChooseB = ParseFloat(ProbStrengthChooseB, 0.35f),
            ProbStrengthAverage = ParseFloat(ProbStrengthAverage, 0.20f),
            ProbStrengthWeightedAverage = ParseFloat(ProbStrengthWeightedAverage, 0.05f),
            StrengthWeightA = ParseFloat(StrengthWeightA, 0.50f),
            StrengthWeightB = ParseFloat(StrengthWeightB, 0.50f),
            ProbStrengthMutate = ParseFloat(ProbStrengthMutate, 0.05f),
            Limits = new ReproduceLimits
            {
                MaxNeuronsAddedAbs = ParseUInt(MaxNeuronsAddedAbs, 0),
                MaxNeuronsAddedPct = ParseFloat(MaxNeuronsAddedPct, 0f),
                MaxNeuronsRemovedAbs = ParseUInt(MaxNeuronsRemovedAbs, 0),
                MaxNeuronsRemovedPct = ParseFloat(MaxNeuronsRemovedPct, 0f),
                MaxAxonsAddedAbs = ParseUInt(MaxAxonsAddedAbs, 0),
                MaxAxonsAddedPct = ParseFloat(MaxAxonsAddedPct, 0f),
                MaxAxonsRemovedAbs = ParseUInt(MaxAxonsRemovedAbs, 0),
                MaxAxonsRemovedPct = ParseFloat(MaxAxonsRemovedPct, 0f),
                MaxRegionsAddedAbs = ParseUInt(MaxRegionsAddedAbs, 0),
                MaxRegionsRemovedAbs = ParseUInt(MaxRegionsRemovedAbs, 0)
            },
            SpawnChild = SelectedSpawnPolicy.Value
        };
    }

    private async Task BrowseParentFileAsync(ParentFileKind kind)
    {
        var extension = kind is ParentFileKind.ParentADef or ParentFileKind.ParentBDef ? "nbn" : "nbs";
        var filter = extension.ToUpperInvariant() + " files";
        var file = await PickOpenFileAsync($"Select .{extension} file", filter, extension);
        if (file is null)
        {
            return;
        }

        var path = FormatPath(file);
        switch (kind)
        {
            case ParentFileKind.ParentADef:
                ParentADefPath = path;
                break;
            case ParentFileKind.ParentAState:
                ParentAStatePath = path;
                break;
            case ParentFileKind.ParentBDef:
                ParentBDefPath = path;
                break;
            case ParentFileKind.ParentBState:
                ParentBStatePath = path;
                break;
        }
    }

    private void ClearParentFiles()
    {
        ParentADefPath = string.Empty;
        ParentAStatePath = string.Empty;
        ParentBDefPath = string.Empty;
        ParentBStatePath = string.Empty;
    }

    private static float ComputeSimilarityScore(SimilarityReport? report)
    {
        if (report is null)
        {
            return 0f;
        }

        if (report.SimilarityScore > 0f)
        {
            return Clamp01(report.SimilarityScore);
        }

        var hasAny = false;
        var total = 0f;
        var count = 0;

        if (report.RegionSpanScore > 0f)
        {
            total += report.RegionSpanScore;
            count++;
            hasAny = true;
        }

        if (report.FunctionScore > 0f)
        {
            total += report.FunctionScore;
            count++;
            hasAny = true;
        }

        if (report.ConnectivityScore > 0f)
        {
            total += report.ConnectivityScore;
            count++;
            hasAny = true;
        }

        if (hasAny && count > 0)
        {
            return Clamp01(total / count);
        }

        return report.Compatible ? 1f : 0f;
    }

    private static string ResolveAbortReason(SimilarityReport? report)
    {
        var abortReason = report?.AbortReason;
        return string.IsNullOrWhiteSpace(abortReason) ? "none" : abortReason.Trim();
    }

    private static bool TryResolveParentId(ReproBrainOption? selected, string rawText, out Guid brainId)
    {
        if (selected is not null)
        {
            brainId = selected.BrainId;
            return true;
        }

        if (Guid.TryParse(rawText, out brainId))
        {
            return true;
        }

        brainId = Guid.Empty;
        return false;
    }

    private static float ParseFloat(string value, float fallback)
    {
        return float.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static uint ParseUInt(string value, uint fallback)
    {
        return uint.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static ulong ParseUlong(string value, ulong fallback)
    {
        return ulong.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static IEnumerable<RegionOutDegreeCap> ParsePerRegionOutDegreeCaps(string rawValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            yield break;
        }

        var parsedCaps = new Dictionary<uint, float>();
        var segments = rawValue.Split([',', ';', '\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
        foreach (var segment in segments)
        {
            var pair = segment.Split([':', '='], 2, StringSplitOptions.RemoveEmptyEntries);
            if (pair.Length != 2
                || !uint.TryParse(pair[0].Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var regionId)
                || !float.TryParse(pair[1].Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var maxAvgOutDegree)
                || regionId > NbnConstants.RegionMaxId
                || maxAvgOutDegree <= 0f
                || float.IsNaN(maxAvgOutDegree)
                || float.IsInfinity(maxAvgOutDegree))
            {
                continue;
            }

            if (parsedCaps.TryGetValue(regionId, out var existing))
            {
                parsedCaps[regionId] = Math.Min(existing, maxAvgOutDegree);
                continue;
            }

            parsedCaps.Add(regionId, maxAvgOutDegree);
        }

        foreach (var pair in parsedCaps.OrderBy(static entry => entry.Key))
        {
            yield return new RegionOutDegreeCap
            {
                RegionId = pair.Key,
                MaxAvgOutDegree = pair.Value
            };
        }
    }

    private static float Clamp01(float value)
    {
        if (value < 0f)
        {
            return 0f;
        }

        if (value > 1f)
        {
            return 1f;
        }

        return value;
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

    private static IStorageProvider? GetStorageProvider()
    {
        var window = GetMainWindow();
        return window?.StorageProvider;
    }

    private static Window? GetMainWindow()
        => Application.Current?.ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop
            ? desktop.MainWindow
            : null;

    private static string BuildDefaultArtifactRoot()
    {
        var baseDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "Nbn.Workbench",
            "repro-artifacts");
        Directory.CreateDirectory(baseDir);
        return baseDir;
    }

    private static string FormatPath(IStorageItem item)
        => item.Path?.LocalPath ?? item.Path?.ToString() ?? item.Name;

    private enum ParentFileKind
    {
        ParentADef,
        ParentAState,
        ParentBDef,
        ParentBState
    }
}

public sealed record ReproBrainOption(Guid BrainId, string Label)
{
    public string BrainIdLabel => BrainId.ToString("D");
}

public sealed record StrengthSourceOption(string Label, StrengthSource Value);

public sealed record SpawnPolicyOption(string Label, SpawnChildPolicy Value);

public sealed record PrunePolicyOption(string Label, PrunePolicy Value);
