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

        public FakeWorkbenchClient()
            : base(new NullWorkbenchEventSink())
        {
        }

        public override Task<ReproduceResult?> ReproduceByBrainIdsAsync(ReproduceByBrainIdsRequest request)
            => Task.FromResult(BrainIdsResult);

        public override Task<ReproduceResult?> ReproduceByArtifactsAsync(ReproduceByArtifactsRequest request)
            => Task.FromResult(ArtifactResult);
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
