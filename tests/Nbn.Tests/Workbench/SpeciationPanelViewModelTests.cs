using System.Diagnostics;
using System.Reflection;
using Nbn.Proto;
using Nbn.Proto.Speciation;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Workbench;

public class SpeciationPanelViewModelTests
{
    [Fact]
    public async Task LoadConfigCommand_ParsesPolicyFields()
    {
        var client = new FakeWorkbenchClient
        {
            GetConfigResponse = new SpeciationGetConfigResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                Config = new SpeciationRuntimeConfig
                {
                    PolicyVersion = "policy-7",
                    DefaultSpeciesId = "species.alpha",
                    DefaultSpeciesDisplayName = "Alpha",
                    StartupReconcileDecisionReason = "reconcile_on_boot",
                    ConfigSnapshotJson = "{\"enabled\":false,\"assignment_policy\":{\"lineage_match_threshold\":0.82,\"lineage_split_threshold\":0.66,\"parent_consensus_threshold\":0.55,\"lineage_hysteresis_margin\":0.12,\"create_derived_species_on_divergence\":false,\"derived_species_prefix\":\"fork\"}}"
                },
                CurrentEpoch = new SpeciationEpochInfo { EpochId = 17 }
            }
        };

        var vm = CreateViewModel(client);

        vm.LoadConfigCommand.Execute(null);
        await WaitForAsync(() => vm.ConfigStatus == "Config loaded.");

        Assert.Equal("policy-7", vm.PolicyVersion);
        Assert.Equal("species.alpha", vm.DefaultSpeciesId);
        Assert.Equal("Alpha", vm.DefaultSpeciesDisplayName);
        Assert.Equal("reconcile_on_boot", vm.StartupReconcileReason);
        Assert.False(vm.ConfigEnabled);
        Assert.Equal("0.82", vm.LineageMatchThreshold);
        Assert.Equal("0.66", vm.LineageSplitThreshold);
        Assert.Equal("0.55", vm.ParentConsensusThreshold);
        Assert.Equal("0.12", vm.HysteresisMargin);
        Assert.False(vm.CreateDerivedSpecies);
        Assert.Equal("fork", vm.DerivedSpeciesPrefix);
        Assert.Equal(17L, vm.CurrentEpochId);
    }

    [Fact]
    public async Task StartNewEpochCommand_RequiresConfirmation_ThenApplies()
    {
        var client = new FakeWorkbenchClient
        {
            SetConfigResponse = new SpeciationSetConfigResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                Config = new SpeciationRuntimeConfig
                {
                    PolicyVersion = "default",
                    ConfigSnapshotJson = "{}",
                    DefaultSpeciesId = "species.default",
                    DefaultSpeciesDisplayName = "Default species",
                    StartupReconcileDecisionReason = "startup_reconcile"
                },
                CurrentEpoch = new SpeciationEpochInfo { EpochId = 42 }
            }
        };

        var vm = CreateViewModel(client);

        vm.StartNewEpochCommand.Execute(null);
        await WaitForAsync(() => vm.ConfigStatus.Contains("confirm", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(0, client.SetConfigCallCount);

        vm.StartNewEpochCommand.Execute(null);
        await WaitForAsync(() => vm.ConfigStatus.StartsWith("New epoch started", StringComparison.Ordinal));

        Assert.Equal(1, client.SetConfigCallCount);
        Assert.True(client.LastStartNewEpoch);
        Assert.Equal(42L, vm.CurrentEpochId);
    }

    [Fact]
    public async Task ClearAllHistoryCommand_RequiresConfirmation_ThenResets()
    {
        var client = new FakeWorkbenchClient
        {
            GetStatusResponse = new SpeciationStatusResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                Status = new SpeciationStatusSnapshot(),
                CurrentEpoch = new SpeciationEpochInfo { EpochId = 100 },
                Config = new SpeciationRuntimeConfig
                {
                    PolicyVersion = "default",
                    ConfigSnapshotJson = "{}",
                    DefaultSpeciesId = "species.default",
                    DefaultSpeciesDisplayName = "Default species",
                    StartupReconcileDecisionReason = "startup_reconcile"
                }
            },
            ResetAllResponse = new SpeciationResetAllResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                CurrentEpoch = new SpeciationEpochInfo { EpochId = 100 },
                Config = new SpeciationRuntimeConfig
                {
                    PolicyVersion = "default",
                    ConfigSnapshotJson = "{}",
                    DefaultSpeciesId = "species.default",
                    DefaultSpeciesDisplayName = "Default species",
                    StartupReconcileDecisionReason = "startup_reconcile"
                },
                DeletedEpochCount = 3,
                DeletedMembershipCount = 12,
                DeletedSpeciesCount = 2,
                DeletedDecisionCount = 12
            }
        };
        var vm = CreateViewModel(client);

        vm.ClearAllHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.HistoryStatus.Contains("confirm", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(0, client.ResetAllCallCount);

        vm.ClearAllHistoryCommand.Execute(null);
        await WaitForAsync(() => client.ResetAllCallCount == 1);
        await WaitForAsync(() => vm.CurrentEpochId == 100L);
    }

    [Fact]
    public async Task DeleteEpochCommand_RequiresConfirmation_ThenDeletesEpoch()
    {
        var client = new FakeWorkbenchClient
        {
            GetStatusResponse = new SpeciationStatusResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                Status = new SpeciationStatusSnapshot(),
                CurrentEpoch = new SpeciationEpochInfo { EpochId = 11 },
                Config = new SpeciationRuntimeConfig
                {
                    PolicyVersion = "default",
                    ConfigSnapshotJson = "{}",
                    DefaultSpeciesId = "species.default",
                    DefaultSpeciesDisplayName = "Default species",
                    StartupReconcileDecisionReason = "startup_reconcile"
                }
            },
            DeleteEpochResponse = new SpeciationDeleteEpochResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                EpochId = 9,
                Deleted = true,
                CurrentEpoch = new SpeciationEpochInfo { EpochId = 11 }
            }
        };
        var vm = CreateViewModel(client);
        vm.DeleteEpochText = "9";

        vm.DeleteEpochCommand.Execute(null);
        await WaitForAsync(() => vm.HistoryStatus.Contains("confirm", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(0, client.DeleteEpochCallCount);

        vm.DeleteEpochCommand.Execute(null);
        await WaitForAsync(() => client.DeleteEpochCallCount == 1);
        Assert.Equal(9L, client.LastDeletedEpochId);
    }

    [Fact]
    public async Task RefreshMembershipsCommand_BuildsSpeciesCounts()
    {
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();
        var brainC = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            MembershipsResponse = new SpeciationListMembershipsResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                Memberships =
                {
                    new SpeciationMembershipRecord { BrainId = brainA.ToProtoUuid(), SpeciesId = "species.a", SpeciesDisplayName = "A" },
                    new SpeciationMembershipRecord { BrainId = brainB.ToProtoUuid(), SpeciesId = "species.b", SpeciesDisplayName = "B" },
                    new SpeciationMembershipRecord { BrainId = brainC.ToProtoUuid(), SpeciesId = "species.a", SpeciesDisplayName = "A" }
                }
            }
        };

        var vm = CreateViewModel(client);
        vm.EpochFilterText = "3";

        vm.RefreshMembershipsCommand.Execute(null);
        await WaitForAsync(() => vm.SpeciesCounts.Count == 2);

        Assert.Equal(3L, client.LastMembershipEpochFilter);
        var top = vm.SpeciesCounts[0];
        Assert.Equal("species.a", top.SpeciesId);
        Assert.Equal(2, top.Count);
        Assert.Equal("66.7 %".Replace(" ", string.Empty), top.PercentLabel.Replace(" ", string.Empty));
        Assert.Equal("Loaded 3 memberships across 2 species.", vm.HistoryStatus);
    }

    [Fact]
    public async Task StartSimulatorCommand_InvalidParentOverrideFile_FailsFast()
    {
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();
        var vm = CreateViewModel(new FakeWorkbenchClient());
        vm.UpdateActiveBrains(
        [
            new BrainListItem(brainA, "Active", true),
            new BrainListItem(brainB, "Active", true)
        ]);
        vm.SimParentAOverrideFilePath = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".txt");

        vm.StartSimulatorCommand.Execute(null);
        await WaitForAsync(() => vm.SimulatorStatus.StartsWith("Parent A override file not found", StringComparison.Ordinal));

        Assert.Contains("Parent A override file not found", vm.SimulatorStatus, StringComparison.Ordinal);
    }

    [Fact]
    public void UpdateActiveBrains_PopulatesSimulatorBrainDropdownDefaults()
    {
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();
        var brainC = Guid.NewGuid();
        var vm = CreateViewModel(new FakeWorkbenchClient());

        vm.UpdateActiveBrains(
        [
            new BrainListItem(brainA, "Active", true),
            new BrainListItem(brainB, "Active", true),
            new BrainListItem(brainC, "Dead", false)
        ]);

        Assert.Equal(2, vm.SimActiveBrains.Count);
        Assert.NotNull(vm.SimSelectedParentABrain);
        Assert.NotNull(vm.SimSelectedParentBBrain);
        Assert.NotEqual(vm.SimSelectedParentABrain!.BrainId, vm.SimSelectedParentBBrain!.BrainId);
    }

    [Fact]
    public async Task StartSimulatorCommand_BrainDropdownMode_RequiresDistinctParents()
    {
        var sharedBrain = Guid.NewGuid();
        var vm = CreateViewModel(new FakeWorkbenchClient());
        vm.SimSelectedParentABrain = new SpeciationSimulatorBrainOption(sharedBrain, sharedBrain.ToString("D"));
        vm.SimSelectedParentBBrain = new SpeciationSimulatorBrainOption(sharedBrain, sharedBrain.ToString("D"));

        vm.StartSimulatorCommand.Execute(null);
        await WaitForAsync(() => vm.SimulatorStatus == "Simulator requires two distinct brain parents.");

        Assert.Equal("Simulator requires two distinct brain parents.", vm.SimulatorStatus);
    }

    [Fact]
    public async Task StartSimulatorCommand_ParentOverrideFile_InvalidGuid_FailsFast()
    {
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();
        var overridePath = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".txt");
        await File.WriteAllTextAsync(overridePath, "not-a-guid");

        try
        {
            var vm = CreateViewModel(new FakeWorkbenchClient());
            vm.UpdateActiveBrains(
            [
                new BrainListItem(brainA, "Active", true),
                new BrainListItem(brainB, "Active", true)
            ]);
            vm.SimParentBOverrideFilePath = overridePath;

            vm.StartSimulatorCommand.Execute(null);
            await WaitForAsync(() => vm.SimulatorStatus.StartsWith("Parent B override file must contain a brain GUID", StringComparison.Ordinal));
            Assert.Contains("Parent B override file must contain a brain GUID", vm.SimulatorStatus, StringComparison.Ordinal);
        }
        finally
        {
            if (File.Exists(overridePath))
            {
                File.Delete(overridePath);
            }
        }
    }

    [Fact]
    public void BuildEvolutionSimArgs_UsesArtifactParentSpecs()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        var parentA = new string('a', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
        var parentB = new string('b', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");

        var method = typeof(SpeciationPanelViewModel).GetMethod(
            "BuildEvolutionSimArgs",
            BindingFlags.NonPublic | BindingFlags.Instance,
            binder: null,
            types: [typeof(int), typeof(int), typeof(ArtifactRef), typeof(ArtifactRef)],
            modifiers: null);
        Assert.NotNull(method);

        var args = (string?)method!.Invoke(vm, [12050, 12074, parentA, parentB]);
        Assert.NotNull(args);
        Assert.Contains("--parent ", args!, StringComparison.Ordinal);
        Assert.DoesNotContain("--parent-brain", args, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RefreshHistoryCommand_BuildsPopulationAndFlowCharts()
    {
        var history = new[]
        {
            new SpeciationMembershipRecord { EpochId = 10, SpeciesId = "species.a", SpeciesDisplayName = "Alpha", AssignedMs = 1000, BrainId = Guid.NewGuid().ToProtoUuid() },
            new SpeciationMembershipRecord { EpochId = 10, SpeciesId = "species.a", SpeciesDisplayName = "Alpha", AssignedMs = 1001, BrainId = Guid.NewGuid().ToProtoUuid() },
            new SpeciationMembershipRecord { EpochId = 10, SpeciesId = "species.b", SpeciesDisplayName = "Beta", AssignedMs = 1002, BrainId = Guid.NewGuid().ToProtoUuid() },
            new SpeciationMembershipRecord { EpochId = 11, SpeciesId = "species.a", SpeciesDisplayName = "Alpha", AssignedMs = 2000, BrainId = Guid.NewGuid().ToProtoUuid() },
            new SpeciationMembershipRecord { EpochId = 11, SpeciesId = "species.b", SpeciesDisplayName = "Beta", AssignedMs = 2001, BrainId = Guid.NewGuid().ToProtoUuid() },
            new SpeciationMembershipRecord { EpochId = 11, SpeciesId = "species.b", SpeciesDisplayName = "Beta", AssignedMs = 2002, BrainId = Guid.NewGuid().ToProtoUuid() }
        };
        var client = new FakeWorkbenchClient
        {
            HistoryResponse = new SpeciationListHistoryResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                TotalRecords = (uint)history.Length,
                History = { history }
            }
        };

        var vm = CreateViewModel(client);
        vm.HistoryLimitText = "64";

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.PopulationChartSeries.Count == 2 && vm.FlowChartAreas.Count == 2);

        Assert.Equal("Epochs 10..11 (2 samples)", vm.PopulationChartRangeLabel);
        Assert.Equal("10", vm.FlowChartStartEpochLabel);
        Assert.Equal("11", vm.FlowChartEndEpochLabel);
        Assert.All(vm.PopulationChartSeries, item => Assert.False(string.IsNullOrWhiteSpace(item.PathData)));
        Assert.All(vm.FlowChartAreas, item => Assert.False(string.IsNullOrWhiteSpace(item.PathData)));
    }

    [Fact]
    public async Task RefreshHistoryCommand_ComputesMaxDivergenceForCurrentEpoch()
    {
        var history = new[]
        {
            new SpeciationMembershipRecord
            {
                EpochId = 17,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.a",
                SpeciesDisplayName = "Alpha",
                AssignedMs = 1000,
                DecisionMetadataJson = "{\"scores\":{\"similarity_score\":0.2}}"
            },
            new SpeciationMembershipRecord
            {
                EpochId = 17,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.b",
                SpeciesDisplayName = "Beta",
                AssignedMs = 1001,
                DecisionMetadataJson = "{\"report\":{\"similarity_score\":0.75}}"
            },
            new SpeciationMembershipRecord
            {
                EpochId = 16,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.c",
                SpeciesDisplayName = "Gamma",
                AssignedMs = 900,
                DecisionMetadataJson = "{\"scores\":{\"similarity_score\":0.01}}"
            }
        };
        var client = new FakeWorkbenchClient
        {
            HistoryResponse = new SpeciationListHistoryResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                TotalRecords = (uint)history.Length,
                History = { history }
            }
        };
        var vm = CreateViewModel(client);
        vm.CurrentEpochId = 17;

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.HistoryRows.Count == history.Length);

        Assert.Contains("epoch 17", vm.CurrentEpochMaxDivergenceLabel, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("0.8", vm.CurrentEpochMaxDivergenceLabel, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task RefreshSimulatorStatusCommand_ParsesExtendedJsonStats()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        var logPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.sim.log");
        var statusLine = "{\"type\":\"evolution_sim_status\",\"final\":true,\"session_id\":\"sess-123\",\"running\":false,\"iterations\":22,\"parent_pool_size\":14,\"compatibility_checks\":100,\"compatible_pairs\":4,\"reproduction_calls\":7,\"reproduction_failures\":3,\"reproduction_runs_observed\":11,\"reproduction_runs_with_mutations\":9,\"reproduction_mutation_events\":23,\"similarity_samples\":11,\"min_similarity_observed\":0.61,\"max_similarity_observed\":0.97,\"children_added_to_pool\":5,\"speciation_commit_attempts\":6,\"speciation_commit_successes\":2,\"last_failure\":\"example_failure\",\"last_seed\":42}";
        await File.WriteAllTextAsync(logPath, statusLine);

        try
        {
            var field = typeof(SpeciationPanelViewModel).GetField("_simStdoutLogPath", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);
            field!.SetValue(vm, logPath);

            vm.RefreshSimulatorStatusCommand.Execute(null);
            await WaitForAsync(() => vm.SimulatorSessionId == "sess-123");

            Assert.Contains("final=True", vm.SimulatorProgress, StringComparison.Ordinal);
            Assert.Contains("parent_pool_size=14", vm.SimulatorProgress, StringComparison.Ordinal);
            Assert.Contains("repro_calls=7", vm.SimulatorDetailedStats, StringComparison.Ordinal);
            Assert.Contains("children_added_to_pool=5", vm.SimulatorDetailedStats, StringComparison.Ordinal);
            Assert.Contains("runs=11", vm.SimulatorDetailedStats, StringComparison.Ordinal);
            Assert.Contains("runs_mutated=9", vm.SimulatorDetailedStats, StringComparison.Ordinal);
            Assert.Contains("mutation_events=23", vm.SimulatorDetailedStats, StringComparison.Ordinal);
            Assert.Contains("min_similarity=0.61", vm.SimulatorDetailedStats, StringComparison.Ordinal);
            Assert.Contains("seed=42", vm.SimulatorDetailedStats, StringComparison.Ordinal);
            Assert.Equal("example_failure", vm.SimulatorLastFailure);
        }
        finally
        {
            if (File.Exists(logPath))
            {
                File.Delete(logPath);
            }
        }
    }

    [Fact]
    public async Task LiveChartsEnabled_AutoRefreshesHistory()
    {
        var client = new FakeWorkbenchClient
        {
            HistoryResponse = new SpeciationListHistoryResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone
            }
        };
        var vm = CreateViewModel(client, enableAutoRefresh: false);
        vm.LiveChartsIntervalSecondsText = "1";
        vm.LiveChartsEnabled = true;

        await WaitForAsync(() => client.HistoryCallCount > 0, timeoutMs: 4000);
        Assert.Contains("active", vm.LiveChartsStatus, StringComparison.OrdinalIgnoreCase);

        vm.LiveChartsEnabled = false;
        await vm.DisposeAsync();
    }

    private static SpeciationPanelViewModel CreateViewModel(FakeWorkbenchClient client, bool enableAutoRefresh = false)
    {
        var connections = new ConnectionViewModel
        {
            IoHost = "127.0.0.1",
            IoPortText = "12050",
            IoGateway = "io-gateway"
        };
        return new SpeciationPanelViewModel(
            new UiDispatcher(),
            connections,
            client,
            startSpeciationService: null,
            stopSpeciationService: null,
            refreshOrchestrator: null,
            enableLiveChartsAutoRefresh: enableAutoRefresh);
    }

    private static async Task WaitForAsync(Func<bool> condition, int timeoutMs = 3000)
    {
        var sw = Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < timeoutMs)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.True(condition(), "Condition was not met within timeout.");
    }

    private sealed class FakeWorkbenchClient : WorkbenchClient
    {
        public SpeciationStatusResponse GetStatusResponse { get; set; } = new()
        {
            FailureReason = SpeciationFailureReason.SpeciationFailureNone,
            Status = new SpeciationStatusSnapshot(),
            CurrentEpoch = new SpeciationEpochInfo(),
            Config = new SpeciationRuntimeConfig
            {
                PolicyVersion = "default",
                ConfigSnapshotJson = "{}",
                DefaultSpeciesId = "species.default",
                DefaultSpeciesDisplayName = "Default species",
                StartupReconcileDecisionReason = "startup_reconcile"
            }
        };

        public SpeciationGetConfigResponse GetConfigResponse { get; set; } = new()
        {
            FailureReason = SpeciationFailureReason.SpeciationFailureNone,
            Config = new SpeciationRuntimeConfig
            {
                PolicyVersion = "default",
                ConfigSnapshotJson = "{}",
                DefaultSpeciesId = "species.default",
                DefaultSpeciesDisplayName = "Default species",
                StartupReconcileDecisionReason = "startup_reconcile"
            },
            CurrentEpoch = new SpeciationEpochInfo()
        };

        public SpeciationSetConfigResponse SetConfigResponse { get; set; } = new()
        {
            FailureReason = SpeciationFailureReason.SpeciationFailureNone,
            Config = new SpeciationRuntimeConfig
            {
                PolicyVersion = "default",
                ConfigSnapshotJson = "{}",
                DefaultSpeciesId = "species.default",
                DefaultSpeciesDisplayName = "Default species",
                StartupReconcileDecisionReason = "startup_reconcile"
            },
            CurrentEpoch = new SpeciationEpochInfo()
        };

        public SpeciationResetAllResponse ResetAllResponse { get; set; } = new()
        {
            FailureReason = SpeciationFailureReason.SpeciationFailureNone,
            CurrentEpoch = new SpeciationEpochInfo(),
            Config = new SpeciationRuntimeConfig
            {
                PolicyVersion = "default",
                ConfigSnapshotJson = "{}",
                DefaultSpeciesId = "species.default",
                DefaultSpeciesDisplayName = "Default species",
                StartupReconcileDecisionReason = "startup_reconcile"
            }
        };

        public SpeciationDeleteEpochResponse DeleteEpochResponse { get; set; } = new()
        {
            FailureReason = SpeciationFailureReason.SpeciationFailureNone,
            CurrentEpoch = new SpeciationEpochInfo()
        };

        public SpeciationListMembershipsResponse MembershipsResponse { get; set; } = new()
        {
            FailureReason = SpeciationFailureReason.SpeciationFailureNone
        };

        public SpeciationListHistoryResponse HistoryResponse { get; set; } = new()
        {
            FailureReason = SpeciationFailureReason.SpeciationFailureNone
        };

        public int SetConfigCallCount { get; private set; }
        public int ResetAllCallCount { get; private set; }
        public int DeleteEpochCallCount { get; private set; }
        public bool LastStartNewEpoch { get; private set; }
        public long LastDeletedEpochId { get; private set; }
        public long? LastMembershipEpochFilter { get; private set; }
        public int HistoryCallCount { get; private set; }

        public FakeWorkbenchClient()
            : base(new NullWorkbenchEventSink())
        {
        }

        public override Task<SpeciationStatusResponse> GetSpeciationStatusAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(GetStatusResponse);

        public override Task<SpeciationGetConfigResponse> GetSpeciationConfigAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(GetConfigResponse);

        public override Task<SpeciationSetConfigResponse> SetSpeciationConfigAsync(
            SpeciationRuntimeConfig config,
            bool startNewEpoch,
            long? applyTimeMs = null,
            CancellationToken cancellationToken = default)
        {
            SetConfigCallCount++;
            LastStartNewEpoch = startNewEpoch;
            return Task.FromResult(SetConfigResponse);
        }

        public override Task<SpeciationResetAllResponse> ResetSpeciationHistoryAsync(
            long? applyTimeMs = null,
            CancellationToken cancellationToken = default)
        {
            ResetAllCallCount++;
            return Task.FromResult(ResetAllResponse);
        }

        public override Task<SpeciationDeleteEpochResponse> DeleteSpeciationEpochAsync(
            long epochId,
            CancellationToken cancellationToken = default)
        {
            DeleteEpochCallCount++;
            LastDeletedEpochId = epochId;
            return Task.FromResult(DeleteEpochResponse);
        }

        public override Task<SpeciationListMembershipsResponse> ListSpeciationMembershipsAsync(
            long? epochId = null,
            CancellationToken cancellationToken = default)
        {
            LastMembershipEpochFilter = epochId;
            return Task.FromResult(MembershipsResponse);
        }

        public override Task<SpeciationListHistoryResponse> ListSpeciationHistoryAsync(
            long? epochId = null,
            Guid? brainId = null,
            uint limit = 256,
            CancellationToken cancellationToken = default)
        {
            HistoryCallCount++;
            return Task.FromResult(HistoryResponse);
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
