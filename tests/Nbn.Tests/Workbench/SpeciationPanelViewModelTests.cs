using System.Diagnostics;
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
    public async Task StartSimulatorCommand_InvalidParentFile_FailsFast()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        vm.SimUseBrainParents = false;
        vm.SimParentsFilePath = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".txt");

        vm.StartSimulatorCommand.Execute(null);
        await WaitForAsync(() => vm.SimulatorStatus.StartsWith("Parent file not found", StringComparison.Ordinal));

        Assert.Contains("Parent file not found", vm.SimulatorStatus, StringComparison.Ordinal);
    }

    private static SpeciationPanelViewModel CreateViewModel(FakeWorkbenchClient client)
    {
        var connections = new ConnectionViewModel
        {
            IoHost = "127.0.0.1",
            IoPortText = "12050",
            IoGateway = "io-gateway"
        };
        return new SpeciationPanelViewModel(new UiDispatcher(), connections, client);
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

        public SpeciationListMembershipsResponse MembershipsResponse { get; set; } = new()
        {
            FailureReason = SpeciationFailureReason.SpeciationFailureNone
        };

        public SpeciationListHistoryResponse HistoryResponse { get; set; } = new()
        {
            FailureReason = SpeciationFailureReason.SpeciationFailureNone
        };

        public int SetConfigCallCount { get; private set; }
        public bool LastStartNewEpoch { get; private set; }
        public long? LastMembershipEpochFilter { get; private set; }

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
            => Task.FromResult(HistoryResponse);
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
