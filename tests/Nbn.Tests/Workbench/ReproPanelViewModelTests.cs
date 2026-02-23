using System.Diagnostics;
using Nbn.Proto.Repro;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Workbench;

public class ReproPanelViewModelTests
{
    [Fact]
    public async Task RunCommand_WhenAbortResultReturned_ShowsAbortReasonAndNotSpawnedStatus()
    {
        var client = new FakeWorkbenchClient
        {
            BrainIdsResult = new ReproduceResult
            {
                Report = new SimilarityReport
                {
                    Compatible = false,
                    AbortReason = "repro_region_span_mismatch",
                    RegionSpanScore = 0.4f,
                    FunctionScore = 0.5f,
                    ConnectivityScore = 0.6f,
                    RegionsPresentChild = 0
                }
            }
        };
        var vm = CreateViewModel(client);
        SetActiveBrains(vm);

        vm.RunCommand.Execute(null);
        await WaitForAsync(() => vm.Status.StartsWith("Aborted:", StringComparison.Ordinal));

        Assert.Equal("Aborted: repro_region_span_mismatch (not spawned).", vm.Status);
        Assert.Contains("Score: 0.500", vm.SimilaritySummary, StringComparison.Ordinal);
        Assert.Contains("Compatible: False", vm.SimilaritySummary, StringComparison.Ordinal);
        Assert.Contains("Abort: repro_region_span_mismatch", vm.SimilaritySummary, StringComparison.Ordinal);
        Assert.Equal("+N0 -N0 +A0 -A0 reroute=0 func=0 strength=0", vm.MutationSummary);
    }

    [Fact]
    public async Task RunCommand_WhenSpawnedChildReturned_ShowsSpawnedStatusAndChildGuid()
    {
        var childId = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            BrainIdsResult = new ReproduceResult
            {
                Report = new SimilarityReport
                {
                    Compatible = true,
                    AbortReason = string.Empty,
                    SimilarityScore = 0.77f,
                    RegionSpanScore = 0.7f,
                    FunctionScore = 0.8f,
                    ConnectivityScore = 0.9f,
                    RegionsPresentChild = 3
                },
                Summary = new MutationSummary
                {
                    NeuronsAdded = 1,
                    NeuronsRemoved = 2,
                    AxonsAdded = 3,
                    AxonsRemoved = 4,
                    AxonsRerouted = 5,
                    FunctionsMutated = 6,
                    StrengthCodesChanged = 7
                },
                Spawned = true,
                ChildBrainId = childId.ToProtoUuid()
            }
        };
        var vm = CreateViewModel(client);
        SetActiveBrains(vm);

        vm.RunCommand.Execute(null);
        await WaitForAsync(() => vm.Status.StartsWith("Spawned child", StringComparison.Ordinal));

        Assert.Equal($"Spawned child {childId:D}", vm.Status);
        Assert.Contains("Score: 0.770", vm.SimilaritySummary, StringComparison.Ordinal);
        Assert.Contains("Compatible: True", vm.SimilaritySummary, StringComparison.Ordinal);
        Assert.Contains("Abort: none", vm.SimilaritySummary, StringComparison.Ordinal);
        Assert.Equal("+N1 -N2 +A3 -A4 reroute=5 func=6 strength=7", vm.MutationSummary);
    }

    [Fact]
    public async Task RunCommand_WhenSpawnFlagAndAbortReasonBothPresent_ShowsBothInStatus()
    {
        var childId = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            BrainIdsResult = new ReproduceResult
            {
                Report = new SimilarityReport
                {
                    Compatible = false,
                    AbortReason = "repro_spawn_unavailable",
                    SimilarityScore = 0.2f
                },
                Spawned = true,
                ChildBrainId = childId.ToProtoUuid()
            }
        };
        var vm = CreateViewModel(client);
        SetActiveBrains(vm);

        vm.RunCommand.Execute(null);
        await WaitForAsync(() => vm.Status.StartsWith("Spawned child", StringComparison.Ordinal));

        Assert.Equal($"Spawned child {childId:D} (abort: repro_spawn_unavailable)", vm.Status);
        Assert.Contains("Compatible: False", vm.SimilaritySummary, StringComparison.Ordinal);
        Assert.Contains("Abort: repro_spawn_unavailable", vm.SimilaritySummary, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunCommand_WhenReportAndSummaryMissing_UsesSafeDisplayDefaults()
    {
        var client = new FakeWorkbenchClient
        {
            BrainIdsResult = new ReproduceResult
            {
                Report = null,
                Summary = null,
                Spawned = false
            }
        };
        var vm = CreateViewModel(client);
        SetActiveBrains(vm);

        vm.RunCommand.Execute(null);
        await WaitForAsync(() => vm.Status == "Completed (not spawned).");

        Assert.Equal("Completed (not spawned).", vm.Status);
        Assert.Contains("Score: 0.000", vm.SimilaritySummary, StringComparison.Ordinal);
        Assert.Contains("Compatible: Unknown", vm.SimilaritySummary, StringComparison.Ordinal);
        Assert.Contains("Abort: none", vm.SimilaritySummary, StringComparison.Ordinal);
        Assert.Equal("+N0 -N0 +A0 -A0 reroute=0 func=0 strength=0", vm.MutationSummary);
    }

    [Fact]
    public async Task RunCommand_MapsAllConfiguredKnobs_IntoBrainIdsRequest()
    {
        var client = new FakeWorkbenchClient
        {
            BrainIdsResult = new ReproduceResult
            {
                Report = new SimilarityReport { Compatible = true }
            }
        };
        var vm = CreateViewModel(client);
        SetActiveBrains(vm);

        vm.MaxRegionSpanDiffRatio = "0.11";
        vm.MaxFunctionHistDistance = "0.22";
        vm.MaxConnectivityHistDistance = "0.33";
        vm.MaxAvgOutDegree = "123";
        vm.ProbAddNeuronToEmptyRegion = "0.01";
        vm.ProbRemoveLastNeuronFromRegion = "0.02";
        vm.ProbDisableNeuron = "0.03";
        vm.ProbReactivateNeuron = "0.04";
        vm.ProbAddAxon = "0.05";
        vm.ProbRemoveAxon = "0.06";
        vm.ProbRerouteAxon = "0.07";
        vm.ProbRerouteInboundAxonOnDelete = "0.08";
        vm.ProbChooseParentA = "0.41";
        vm.ProbChooseParentB = "0.42";
        vm.ProbAverage = "0.09";
        vm.ProbMutate = "0.10";
        vm.ProbChooseFuncA = "0.43";
        vm.ProbMutateFunc = "0.11";
        vm.StrengthTransformEnabled = true;
        vm.ProbStrengthChooseA = "0.12";
        vm.ProbStrengthChooseB = "0.13";
        vm.ProbStrengthAverage = "0.14";
        vm.ProbStrengthWeightedAverage = "0.15";
        vm.StrengthWeightA = "0.16";
        vm.StrengthWeightB = "0.84";
        vm.ProbStrengthMutate = "0.17";
        vm.MaxNeuronsAddedAbs = "1";
        vm.MaxNeuronsAddedPct = "0.18";
        vm.MaxNeuronsRemovedAbs = "2";
        vm.MaxNeuronsRemovedPct = "0.19";
        vm.MaxAxonsAddedAbs = "3";
        vm.MaxAxonsAddedPct = "0.20";
        vm.MaxAxonsRemovedAbs = "4";
        vm.MaxAxonsRemovedPct = "0.21";
        vm.MaxRegionsAddedAbs = "5";
        vm.MaxRegionsRemovedAbs = "6";
        vm.SelectedStrengthSource = vm.StrengthSources[1];
        vm.SelectedSpawnPolicy = vm.SpawnPolicies[1];
        vm.SelectedPrunePolicy = vm.PrunePolicies[2];

        vm.RunCommand.Execute(null);
        await WaitForAsync(() => client.LastBrainIdsRequest is not null);

        var request = client.LastBrainIdsRequest!;
        Assert.Equal(StrengthSource.StrengthLiveCodes, request.StrengthSource);
        Assert.NotNull(request.Config);
        Assert.Equal(0.11f, request.Config.MaxRegionSpanDiffRatio, 3);
        Assert.Equal(0.22f, request.Config.MaxFunctionHistDistance, 3);
        Assert.Equal(0.33f, request.Config.MaxConnectivityHistDistance, 3);
        Assert.Equal(123f, request.Config.MaxAvgOutDegreeBrain, 3);
        Assert.Equal(0.01f, request.Config.ProbAddNeuronToEmptyRegion, 3);
        Assert.Equal(0.02f, request.Config.ProbRemoveLastNeuronFromRegion, 3);
        Assert.Equal(0.03f, request.Config.ProbDisableNeuron, 3);
        Assert.Equal(0.04f, request.Config.ProbReactivateNeuron, 3);
        Assert.Equal(0.05f, request.Config.ProbAddAxon, 3);
        Assert.Equal(0.06f, request.Config.ProbRemoveAxon, 3);
        Assert.Equal(0.07f, request.Config.ProbRerouteAxon, 3);
        Assert.Equal(0.08f, request.Config.ProbRerouteInboundAxonOnDelete, 3);
        Assert.Equal(0.41f, request.Config.ProbChooseParentA, 3);
        Assert.Equal(0.42f, request.Config.ProbChooseParentB, 3);
        Assert.Equal(0.09f, request.Config.ProbAverage, 3);
        Assert.Equal(0.10f, request.Config.ProbMutate, 3);
        Assert.Equal(0.43f, request.Config.ProbChooseFuncA, 3);
        Assert.Equal(0.11f, request.Config.ProbMutateFunc, 3);
        Assert.True(request.Config.StrengthTransformEnabled);
        Assert.Equal(0.12f, request.Config.ProbStrengthChooseA, 3);
        Assert.Equal(0.13f, request.Config.ProbStrengthChooseB, 3);
        Assert.Equal(0.14f, request.Config.ProbStrengthAverage, 3);
        Assert.Equal(0.15f, request.Config.ProbStrengthWeightedAverage, 3);
        Assert.Equal(0.16f, request.Config.StrengthWeightA, 3);
        Assert.Equal(0.84f, request.Config.StrengthWeightB, 3);
        Assert.Equal(0.17f, request.Config.ProbStrengthMutate, 3);
        Assert.Equal(PrunePolicy.PruneRandom, request.Config.PrunePolicy);
        Assert.Equal(SpawnChildPolicy.SpawnChildAlways, request.Config.SpawnChild);
        Assert.NotNull(request.Config.Limits);
        Assert.Equal((uint)1, request.Config.Limits.MaxNeuronsAddedAbs);
        Assert.Equal(0.18f, request.Config.Limits.MaxNeuronsAddedPct, 3);
        Assert.Equal((uint)2, request.Config.Limits.MaxNeuronsRemovedAbs);
        Assert.Equal(0.19f, request.Config.Limits.MaxNeuronsRemovedPct, 3);
        Assert.Equal((uint)3, request.Config.Limits.MaxAxonsAddedAbs);
        Assert.Equal(0.20f, request.Config.Limits.MaxAxonsAddedPct, 3);
        Assert.Equal((uint)4, request.Config.Limits.MaxAxonsRemovedAbs);
        Assert.Equal(0.21f, request.Config.Limits.MaxAxonsRemovedPct, 3);
        Assert.Equal((uint)5, request.Config.Limits.MaxRegionsAddedAbs);
        Assert.Equal((uint)6, request.Config.Limits.MaxRegionsRemovedAbs);
    }

    private static ReproPanelViewModel CreateViewModel(WorkbenchClient client)
    {
        return new ReproPanelViewModel(client);
    }

    private static void SetActiveBrains(ReproPanelViewModel vm)
    {
        vm.UpdateActiveBrains(
        [
            new BrainListItem(Guid.NewGuid(), "Active", true),
            new BrainListItem(Guid.NewGuid(), "Active", true)
        ]);
    }

    private static async Task WaitForAsync(Func<bool> predicate, int timeoutMs = 2000)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.ElapsedMilliseconds < timeoutMs)
        {
            if (predicate())
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.True(predicate());
    }

    private sealed class FakeWorkbenchClient : WorkbenchClient
    {
        public ReproduceResult? BrainIdsResult { get; init; }
        public ReproduceResult? ArtifactResult { get; init; }
        public ReproduceByBrainIdsRequest? LastBrainIdsRequest { get; private set; }
        public ReproduceByArtifactsRequest? LastArtifactRequest { get; private set; }

        public FakeWorkbenchClient()
            : base(new NullWorkbenchEventSink())
        {
        }

        public override Task<ReproduceResult?> ReproduceByBrainIdsAsync(ReproduceByBrainIdsRequest request)
        {
            LastBrainIdsRequest = request;
            return Task.FromResult(BrainIdsResult);
        }

        public override Task<ReproduceResult?> ReproduceByArtifactsAsync(ReproduceByArtifactsRequest request)
        {
            LastArtifactRequest = request;
            return Task.FromResult(ArtifactResult);
        }
    }

    private sealed class NullWorkbenchEventSink : IWorkbenchEventSink
    {
        public void OnOutputEvent(OutputEventItem item) { }
        public void OnOutputVectorEvent(OutputVectorEventItem item) { }
        public void OnDebugEvent(DebugEventItem item) { }
        public void OnVizEvent(VizEventItem item) { }
        public void OnBrainTerminated(BrainTerminatedItem item) { }
        public void OnIoStatus(string status, bool connected) { }
        public void OnObsStatus(string status, bool connected) { }
        public void OnSettingsStatus(string status, bool connected) { }
        public void OnHiveMindStatus(string status, bool connected) { }
        public void OnSettingChanged(SettingItem item) { }
    }
}
