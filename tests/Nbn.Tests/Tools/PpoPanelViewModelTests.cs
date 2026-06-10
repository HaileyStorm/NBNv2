using Nbn.Proto;
using Nbn.Proto.Ppo;
using Nbn.Proto.Repro;
using Nbn.Proto.Speciation;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Tools;

public class PpoPanelViewModelTests
{
    [Fact]
    public void UpdateActiveBrains_Selects_Two_Live_Parents_And_Builds_Request()
    {
        var parentA = Guid.NewGuid();
        var parentB = Guid.NewGuid();
        var deadBrain = Guid.NewGuid();
        var viewModel = CreateViewModel();

        viewModel.UpdateActiveBrains([
            new BrainListItem(parentB, "Running", true),
            new BrainListItem(deadBrain, "Dead", false),
            new BrainListItem(parentA, "Active", true)
        ]);

        Assert.Equal(2, viewModel.ActiveBrains.Count);
        Assert.NotNull(viewModel.SelectedParentABrain);
        Assert.NotNull(viewModel.SelectedParentBBrain);
        Assert.NotEqual(viewModel.SelectedParentABrain.BrainId, viewModel.SelectedParentBBrain.BrainId);

        Assert.True(viewModel.TryBuildStartRequest(out var request, out var reason), reason);
        Assert.Equal(2, request.ParentBrainIds.Count);
        Assert.All(request.ParentBrainIds, id => Assert.True(id.TryToGuid(out _)));
        Assert.Equal(SpawnChildPolicy.SpawnChildNever, request.ReproduceConfig.SpawnChild);
        Assert.True(request.ReproduceConfig.ProtectIoRegionNeuronCounts);
        Assert.True(request.ReproduceConfig.ProbMutate > 0f);
        Assert.Equal("reward", request.ObjectiveName);
    }

    [Fact]
    public void TryBuildStartRequest_Rejects_Duplicate_Parents()
    {
        var brainId = Guid.NewGuid();
        var viewModel = CreateViewModel();
        viewModel.UpdateActiveBrains([new BrainListItem(brainId, "Running", true)]);
        viewModel.SelectedParentBBrain = viewModel.SelectedParentABrain;

        Assert.False(viewModel.TryBuildStartRequest(out _, out var reason));
        Assert.Equal("Parent A and Parent B must be different.", reason);
    }

    [Fact]
    public async Task RefreshStatus_Maps_Run_Candidates_Update_And_Chart_Data()
    {
        var client = new FakeWorkbenchClient
        {
            StatusResponse = CreateStatusResponse()
        };
        var viewModel = new PpoPanelViewModel(client, new ConnectionViewModel());

        await viewModel.RefreshStatusAsync();

        Assert.Equal("PPO ready.", viewModel.Status);
        Assert.Equal(2, viewModel.Candidates.Count);
        Assert.Single(viewModel.ObservedParents);
        Assert.Equal(2, viewModel.RewardChartSeries.Count);
        Assert.Equal(2, viewModel.LossChartSeries.Count);
        Assert.Equal(2, viewModel.TrustChartSeries.Count);
        Assert.All(viewModel.RewardChartSeries, series => Assert.StartsWith("M ", series.PathData, StringComparison.Ordinal));
        Assert.NotEmpty(viewModel.ActionBars);
        Assert.NotEmpty(viewModel.CandidateMetricBars);
        Assert.Contains("2 candidate", viewModel.CandidateSummary, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task RecordReward_Sends_Selected_Candidate_And_Adds_Update_To_Charts()
    {
        var client = new FakeWorkbenchClient
        {
            StatusResponse = CreateStatusResponse(),
            RecordResponse = new PpoRecordRewardsResponse
            {
                FailureReason = PpoFailureReason.PpoFailureNone,
                Accepted = true,
                Update = new PpoPolicyUpdateReport
                {
                    UpdateIndex = 2,
                    AcceptedSampleCount = 1,
                    MeanReward = 0.9f,
                    MaxReward = 0.95f,
                    PolicyLoss = 0.2f,
                    ValueLoss = 0.1f,
                    Entropy = 0.6f,
                    ApproximateKl = 0.02f
                }
            }
        };
        var viewModel = new PpoPanelViewModel(client, new ConnectionViewModel());
        await viewModel.RefreshStatusAsync();
        viewModel.RewardText = "0.9";
        viewModel.AccuracyText = "0.8";
        viewModel.FitnessText = "0.7";
        viewModel.GenerationText = "3";
        viewModel.RewardMetadataJson = "{\"judge\":\"operator\"}";

        await viewModel.RecordRewardAsync();

        Assert.NotNull(client.LastRecordRequest);
        var sample = Assert.Single(client.LastRecordRequest.Samples);
        Assert.Equal(viewModel.SelectedCandidate!.RunId, sample.RunId);
        Assert.Equal(viewModel.SelectedCandidate.RunIndex, sample.RunIndex);
        Assert.Equal(0.9f, sample.Reward);
        Assert.Equal(0.8f, sample.Accuracy);
        Assert.Equal(0.7f, sample.Fitness);
        Assert.Equal(3UL, sample.Generation);
        Assert.True(sample.Terminal);
        Assert.Equal("{\"judge\":\"operator\"}", sample.MetadataJson);
        Assert.Equal(2, viewModel.PolicyUpdates.Count);
        Assert.Equal("Updates 1..2", viewModel.RewardChartRangeLabel);
    }

    private static PpoPanelViewModel CreateViewModel()
        => new(new FakeWorkbenchClient(), new ConnectionViewModel());

    private static PpoStatusResponse CreateStatusResponse()
    {
        var parentId = Guid.NewGuid();
        var childA = new string('a', 64).ToArtifactRef(128, "application/x-nbn", "memory://ppo");
        var childB = new string('b', 64).ToArtifactRef(128, "application/x-nbn", "memory://ppo");
        return new PpoStatusResponse
        {
            FailureReason = PpoFailureReason.PpoFailureNone,
            Dependencies = new PpoDependencyStatus
            {
                IoAvailable = true,
                ReproductionAvailable = true,
                SpeciationAvailable = true
            },
            LastRun = new PpoRunDescriptor
            {
                RunId = "ppo-run-1",
                State = PpoRunState.Completed,
                StatusDetail = "completed",
                ObjectiveName = "reward",
                Hyperparameters = new PpoHyperparameters
                {
                    RolloutTickCount = 8,
                    RolloutBatchCount = 2,
                    RewardSignal = "output.reward"
                },
                ExecutionReport = new PpoRolloutExecutionReport
                {
                    PolicyStateJson = "{\"update\":1}"
                }
            },
            LastPolicyUpdate = new PpoPolicyUpdateReport
            {
                UpdateIndex = 1,
                AcceptedSampleCount = 2,
                MeanReward = 0.5f,
                MaxReward = 0.8f,
                PolicyLoss = 0.4f,
                ValueLoss = 0.3f,
                Entropy = 0.7f,
                ApproximateKl = 0.01f
            }
        }.WithRunDetails(parentId, childA, childB);
    }

    private sealed class FakeWorkbenchClient : WorkbenchClient
    {
        public FakeWorkbenchClient()
            : base(new RecordingWorkbenchEventSink())
        {
        }

        public PpoStatusResponse StatusResponse { get; set; } = new()
        {
            FailureReason = PpoFailureReason.PpoFailureNone,
            Dependencies = new PpoDependencyStatus()
        };

        public PpoRecordRewardsResponse RecordResponse { get; set; } = new()
        {
            FailureReason = PpoFailureReason.PpoFailureNone,
            Accepted = true,
            Update = new PpoPolicyUpdateReport()
        };

        public PpoRecordRewardsRequest? LastRecordRequest { get; private set; }

        public override Task<PpoStatusResponse> GetPpoStatusAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(StatusResponse.Clone());

        public override Task<PpoRecordRewardsResponse> RecordPpoRewardsAsync(
            PpoRecordRewardsRequest request,
            CancellationToken cancellationToken = default)
        {
            LastRecordRequest = request.Clone();
            return Task.FromResult(RecordResponse.Clone());
        }
    }

    private sealed class RecordingWorkbenchEventSink : IWorkbenchEventSink
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

internal static class PpoStatusResponseTestExtensions
{
    public static PpoStatusResponse WithRunDetails(
        this PpoStatusResponse response,
        Guid parentId,
        ArtifactRef childA,
        ArtifactRef childB)
    {
        response.LastRun.ExecutionReport.ObservedParents.Add(new PpoObservedParent
        {
            BrainId = parentId.ToProtoUuid(),
            BrainDef = new string('c', 64).ToArtifactRef(256, "application/x-nbn", "memory://ppo"),
            Snapshot = new string('d', 64).ToArtifactRef(64, "application/x-nbs", "memory://ppo"),
            SnapshotTickId = 12,
            SnapshotSource = "live_tick_boundary"
        });
        response.LastRun.ExecutionReport.Candidates.Add(new PpoCandidateResult
        {
            RunIndex = 0,
            Seed = 101,
            ChildDef = childA,
            ReproductionReport = new SimilarityReport
            {
                Compatible = true,
                SimilarityScore = 0.8f
            },
            MutationSummary = new MutationSummary
            {
                NeuronsAdded = 1,
                AxonsAdded = 2
            },
            SpeciationDecision = new SpeciationDecision
            {
                SpeciesId = "alpha",
                Success = true
            },
            OldLogProbability = -0.25f,
            ValueEstimate = 0.4f,
            ActionJson = "{\"parameter_mutation\":0.7,\"add_axon\":0.2,\"remove_axon\":0.1}"
        });
        response.LastRun.ExecutionReport.Candidates.Add(new PpoCandidateResult
        {
            RunIndex = 1,
            Seed = 102,
            ChildDef = childB,
            ReproductionReport = new SimilarityReport
            {
                Compatible = false,
                SimilarityScore = 0.25f
            },
            MutationSummary = new MutationSummary
            {
                AxonsRemoved = 1
            },
            SpeciationDecision = new SpeciationDecision
            {
                SpeciesId = "beta",
                Success = false
            },
            OldLogProbability = -0.5f,
            ValueEstimate = 0.1f,
            ActionJson = "{\"parameter_mutation\":0.1,\"add_axon\":0.1}"
        });
        return response;
    }
}
