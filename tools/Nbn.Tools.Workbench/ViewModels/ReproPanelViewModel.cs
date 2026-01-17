using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Nbn.Proto.Repro;
using Nbn.Shared;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class ReproPanelViewModel : ViewModelBase
{
    private readonly WorkbenchClient _client;
    private string _parentA = string.Empty;
    private string _parentB = string.Empty;
    private string _seedText = string.Empty;
    private string _maxRegionSpanDiffRatio = "0.15";
    private string _maxFunctionHistDistance = "0.25";
    private string _maxConnectivityHistDistance = "0.25";
    private string _maxAvgOutDegree = "100";
    private string _probAddNeuron = "0.02";
    private string _probRemoveNeuron = "0.01";
    private string _probAddAxon = "0.05";
    private string _probRemoveAxon = "0.02";
    private string _probRerouteAxon = "0.02";
    private StrengthSourceOption _selectedStrengthSource;
    private SpawnPolicyOption _selectedSpawnPolicy;
    private string _status = "Idle";
    private string _resultSummary = string.Empty;

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
            new("Default", SpawnChildPolicy.SpawnChildDefaultOn),
            new("Never", SpawnChildPolicy.SpawnChildNever),
            new("Always", SpawnChildPolicy.SpawnChildAlways)
        };
        _selectedStrengthSource = StrengthSources[0];
        _selectedSpawnPolicy = SpawnPolicies[0];
        RunCommand = new AsyncRelayCommand(RunAsync);
    }

    public IReadOnlyList<StrengthSourceOption> StrengthSources { get; }

    public IReadOnlyList<SpawnPolicyOption> SpawnPolicies { get; }

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

    public string ParentA
    {
        get => _parentA;
        set => SetProperty(ref _parentA, value);
    }

    public string ParentB
    {
        get => _parentB;
        set => SetProperty(ref _parentB, value);
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

    public string ProbAddNeuron
    {
        get => _probAddNeuron;
        set => SetProperty(ref _probAddNeuron, value);
    }

    public string ProbRemoveNeuron
    {
        get => _probRemoveNeuron;
        set => SetProperty(ref _probRemoveNeuron, value);
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

    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }

    public string ResultSummary
    {
        get => _resultSummary;
        set => SetProperty(ref _resultSummary, value);
    }

    public AsyncRelayCommand RunCommand { get; }

    private async Task RunAsync()
    {
        if (!Guid.TryParse(ParentA, out var parentA) || !Guid.TryParse(ParentB, out var parentB))
        {
            Status = "Invalid parent IDs.";
            return;
        }

        var config = BuildConfig();
        var seed = ParseUlong(SeedText, (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        var request = new ReproduceByBrainIdsRequest
        {
            ParentA = parentA.ToProtoUuid(),
            ParentB = parentB.ToProtoUuid(),
            StrengthSource = SelectedStrengthSource.Value,
            Config = config,
            Seed = seed
        };

        Status = "Submitting request...";
        var result = await _client.ReproduceByBrainIdsAsync(request);
        if (result is null)
        {
            Status = "Repro request failed.";
            return;
        }

        var report = result.Report;
        var summary = result.Summary;
        ResultSummary = $"Compatible: {report?.Compatible} | Regions child: {report?.RegionsPresentChild} | Mutations: +N{summary?.NeuronsAdded} -N{summary?.NeuronsRemoved} +A{summary?.AxonsAdded} -A{summary?.AxonsRemoved}";
        var childLabel = result.ChildBrainId is not null && result.ChildBrainId.TryToGuid(out var childGuid)
            ? childGuid.ToString("D")
            : "unknown";
        Status = result.Spawned ? $"Spawned child {childLabel}" : "Completed (not spawned).";
    }

    private ReproduceConfig BuildConfig()
    {
        return new ReproduceConfig
        {
            MaxRegionSpanDiffRatio = ParseFloat(MaxRegionSpanDiffRatio, 0.15f),
            MaxFunctionHistDistance = ParseFloat(MaxFunctionHistDistance, 0.25f),
            MaxConnectivityHistDistance = ParseFloat(MaxConnectivityHistDistance, 0.25f),
            ProbAddNeuronToEmptyRegion = ParseFloat(ProbAddNeuron, 0.02f),
            ProbRemoveLastNeuronFromRegion = ParseFloat(ProbRemoveNeuron, 0.01f),
            ProbAddAxon = ParseFloat(ProbAddAxon, 0.05f),
            ProbRemoveAxon = ParseFloat(ProbRemoveAxon, 0.02f),
            ProbRerouteAxon = ParseFloat(ProbRerouteAxon, 0.02f),
            MaxAvgOutDegreeBrain = ParseFloat(MaxAvgOutDegree, 100f),
            SpawnChild = SelectedSpawnPolicy.Value
        };
    }

    private static float ParseFloat(string value, float fallback)
    {
        return float.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static ulong ParseUlong(string value, ulong fallback)
    {
        return ulong.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }
}

public sealed record StrengthSourceOption(string Label, StrengthSource Value);

public sealed record SpawnPolicyOption(string Label, SpawnChildPolicy Value);
