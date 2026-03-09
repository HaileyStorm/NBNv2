using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Nbn.Proto;
using Nbn.Proto.Settings;
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
                    ConfigSnapshotJson = "{\"enabled\":false,\"assignment_policy\":{\"lineage_match_threshold\":0.82,\"lineage_split_threshold\":0.66,\"parent_consensus_threshold\":0.55,\"lineage_hysteresis_margin\":0.12,\"lineage_split_guard_margin\":0.04,\"lineage_min_parent_memberships_before_split\":3,\"lineage_realign_parent_membership_window\":5,\"lineage_realign_match_margin\":0.07,\"lineage_hindsight_reassign_commit_window\":9,\"lineage_hindsight_similarity_margin\":0.023,\"create_derived_species_on_divergence\":false,\"derived_species_prefix\":\"fork\"}}"
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
        Assert.Equal("0.04", vm.LineageSplitGuardMargin);
        Assert.Equal("3", vm.LineageMinParentMembershipsBeforeSplit);
        Assert.Equal("5", vm.LineageRealignParentMembershipWindow);
        Assert.Equal("0.07", vm.LineageRealignMatchMargin);
        Assert.Equal("9", vm.LineageHindsightReassignCommitWindow);
        Assert.Equal("0.023", vm.LineageHindsightSimilarityMargin);
        Assert.False(vm.CreateDerivedSpecies);
        Assert.Equal("fork", vm.DerivedSpeciesPrefix);
        Assert.Equal(17L, vm.CurrentEpochId);
        Assert.Empty(client.SettingUpdateEvents);
    }

    [Fact]
    public void ApplySetting_UpdatesSpeciationDraftFromSettingsMonitorKeys()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());

        Assert.True(vm.ApplySetting(new SettingItem(SpeciationSettingsKeys.LineageSplitThresholdKey, "0.87", "1")));
        Assert.True(vm.ApplySetting(new SettingItem(SpeciationSettingsKeys.LineageHysteresisMarginKey, "0.03", "1")));
        Assert.True(vm.ApplySetting(new SettingItem(SpeciationSettingsKeys.LineageSplitGuardMarginKey, "0.02", "1")));
        Assert.True(vm.ApplySetting(new SettingItem(SpeciationSettingsKeys.LineageHindsightReassignCommitWindowKey, "8", "1")));
        Assert.True(vm.ApplySetting(new SettingItem(SpeciationSettingsKeys.LineageHindsightSimilarityMarginKey, "0.023", "1")));
        Assert.True(vm.ApplySetting(new SettingItem(SpeciationSettingsKeys.HistoryLimitKey, "100", "1")));

        Assert.Equal("0.87", vm.LineageSplitThreshold);
        Assert.Equal("0.03", vm.HysteresisMargin);
        Assert.Equal("0.02", vm.LineageSplitGuardMargin);
        Assert.Equal("8", vm.LineageHindsightReassignCommitWindow);
        Assert.Equal("0.023", vm.LineageHindsightSimilarityMargin);
        Assert.Equal("100", vm.HistoryLimitText);
        Assert.Equal("Settings-backed draft active.", vm.ConfigStatus);
    }

    [Fact]
    public void BuildRuntimeConfigFromDraft_IncludesHindsightPolicyFields()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        vm.LineageHindsightReassignCommitWindow = "9";
        vm.LineageHindsightSimilarityMargin = "0.023";

        var config = InvokeBuildRuntimeConfigFromDraft(vm);
        var root = Assert.IsType<JsonObject>(JsonNode.Parse(config.ConfigSnapshotJson));
        var policy = Assert.IsType<JsonObject>(root["assignment_policy"]);

        Assert.Equal(9, policy["lineage_hindsight_reassign_commit_window"]!.GetValue<int>());
        Assert.Equal(0.023d, policy["lineage_hindsight_similarity_margin"]!.GetValue<double>(), 3);
    }

    [Fact]
    public async Task PersistSpeciationSettingsAsync_SavesHindsightSettings()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        vm.LineageHindsightReassignCommitWindow = "-4";
        vm.LineageHindsightSimilarityMargin = "1.7";

        await InvokePersistSpeciationSettingsAsync(vm);

        Assert.Equal("0", client.SettingUpdates[SpeciationSettingsKeys.LineageHindsightReassignCommitWindowKey]);
        Assert.Equal("1", client.SettingUpdates[SpeciationSettingsKeys.LineageHindsightSimilarityMarginKey]);
    }

    [Fact]
    public async Task RefreshStatusCommand_DoesNotPersistSettings()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);

        vm.RefreshStatusCommand.Execute(null);
        await WaitForAsync(() => vm.ServiceSummary.StartsWith("Epoch", StringComparison.Ordinal));

        Assert.Empty(client.SettingUpdateEvents);
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
        Assert.Equal("A", top.SpeciesDisplayName);
        Assert.Equal(2, top.Count);
        Assert.Equal("66.7 %".Replace(" ", string.Empty), top.PercentLabel.Replace(" ", string.Empty));
        Assert.Equal("Loaded 3 memberships across 2 species.", vm.HistoryStatus);
    }

    [Fact]
    public async Task RefreshMembershipsCommand_UsesCompactSpeciesNameWhenDisplayNameIsMissing()
    {
        var brain = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            MembershipsResponse = new SpeciationListMembershipsResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                Memberships =
                {
                    new SpeciationMembershipRecord
                    {
                        BrainId = brain.ToProtoUuid(),
                        SpeciesId = "species.branch.a1b2c3d4e5f6"
                    }
                }
            }
        };

        var vm = CreateViewModel(client);

        vm.RefreshMembershipsCommand.Execute(null);
        await WaitForAsync(() => vm.SpeciesCounts.Count == 1);

        Assert.Equal("Branch", vm.SpeciesCounts[0].SpeciesDisplayName);
    }

    [Fact]
    public async Task RefreshMembershipsCommand_UsesLineageCodeWhenDisplayNameIncludesBracketedSuffix()
    {
        var brain = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            MembershipsResponse = new SpeciationListMembershipsResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                Memberships =
                {
                    new SpeciationMembershipRecord
                    {
                        BrainId = brain.ToProtoUuid(),
                        SpeciesId = "unclassified-branch-a1b2c3d4e5f6-branch-0f1e2d3c4b5a",
                        SpeciesDisplayName = "Unclassified [AB]"
                    }
                }
            }
        };

        var vm = CreateViewModel(client);

        vm.RefreshMembershipsCommand.Execute(null);
        await WaitForAsync(() => vm.SpeciesCounts.Count == 1);

        Assert.Equal("[AB]", vm.SpeciesCounts[0].SpeciesDisplayName);
    }

    [Fact]
    public async Task RefreshMembershipsCommand_PreservesNumberedRootSpeciesDisplayName()
    {
        var brain = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            MembershipsResponse = new SpeciationListMembershipsResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                Memberships =
                {
                    new SpeciationMembershipRecord
                    {
                        BrainId = brain.ToProtoUuid(),
                        SpeciesId = "unclassified-founder-root-2",
                        SpeciesDisplayName = "Unclassified-2"
                    }
                }
            }
        };

        var vm = CreateViewModel(client);

        vm.RefreshMembershipsCommand.Execute(null);
        await WaitForAsync(() => vm.SpeciesCounts.Count == 1);

        Assert.Equal("Unclassified-2", vm.SpeciesCounts[0].SpeciesDisplayName);
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
    public async Task StartSimulatorCommand_WhenLaunchPreparationFails_ReportsFailure()
    {
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();
        var vm = CreateViewModel(
            new FakeWorkbenchClient(),
            launchPreparer: new FakeLocalProjectLaunchPreparer("Build failed (code 1). CS1000"));
        vm.UpdateActiveBrains(
        [
            new BrainListItem(brainA, "Active", true),
            new BrainListItem(brainB, "Active", true)
        ]);

        vm.StartSimulatorCommand.Execute(null);
        await WaitForAsync(() => vm.SimulatorStatus == "Build failed (code 1). CS1000");

        Assert.Equal("Build failed (code 1). CS1000", vm.SimulatorStatus);
        Assert.Equal("Evolution simulator: Build failed (code 1). CS1000", vm.Status);
        Assert.Equal("No simulator statistics yet.", vm.SimulatorDetailedStats);
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
    public void UpdateActiveBrains_AutoPopulatesEffectiveSeedParents()
    {
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();
        var vm = CreateViewModel(new FakeWorkbenchClient());

        vm.UpdateActiveBrains(
        [
            new BrainListItem(brainA, "Alpha", true),
            new BrainListItem(brainB, "Beta", true)
        ]);

        Assert.Equal(2, vm.SimSeedParents.Count);
        Assert.Contains(vm.SimSeedParents, item => item.BrainId == vm.SimSelectedParentABrain!.BrainId && item.Source == "Parent A");
        Assert.Contains(vm.SimSeedParents, item => item.BrainId == vm.SimSelectedParentBBrain!.BrainId && item.Source == "Parent B");
    }

    [Fact]
    public async Task OverrideFiles_ReplaceSelectedSeedParentSlots()
    {
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();
        var overrideBrain = Guid.NewGuid();
        var overridePath = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".txt");
        await File.WriteAllTextAsync(overridePath, overrideBrain.ToString("D"));

        try
        {
            var vm = CreateViewModel(new FakeWorkbenchClient());
            vm.UpdateActiveBrains(
            [
                new BrainListItem(brainA, "Alpha", true),
                new BrainListItem(brainB, "Beta", true)
            ]);

            vm.SimParentBOverrideFilePath = overridePath;

            Assert.Equal(2, vm.SimSeedParents.Count);
            Assert.Contains(vm.SimSeedParents, item => item.BrainId == vm.SimSelectedParentABrain!.BrainId && item.Source == "Parent A");
            Assert.Contains(vm.SimSeedParents, item => item.BrainId == overrideBrain && item.Source == "Parent B override");
            Assert.DoesNotContain(vm.SimSeedParents, item => item.BrainId == vm.SimSelectedParentBBrain!.BrainId);
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
    public void AddSimSeedParentCommand_AppendsExtrasToEffectiveSeedParents()
    {
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();
        var brainC = Guid.NewGuid();
        var vm = CreateViewModel(new FakeWorkbenchClient());
        vm.UpdateActiveBrains(
        [
            new BrainListItem(brainA, "Alpha", true),
            new BrainListItem(brainB, "Beta", true),
            new BrainListItem(brainC, "Gamma", true)
        ]);
        var seededParentIds = new HashSet<Guid>
        {
            vm.SimSelectedParentABrain!.BrainId,
            vm.SimSelectedParentBBrain!.BrainId
        };
        vm.SimExtraParentCandidateBrain = vm.SimActiveBrains.Single(entry => !seededParentIds.Contains(entry.BrainId));

        vm.AddSimSeedParentCommand.Execute(null);

        Assert.Equal(3, vm.SimSeedParents.Count);
        Assert.Contains(vm.SimSeedParents, item => item.BrainId == vm.SimExtraParentCandidateBrain!.BrainId && item.Source == "dropdown");
    }

    [Fact]
    public async Task StartSimulatorCommand_BrainDropdownMode_RequiresDistinctParents()
    {
        var sharedBrain = Guid.NewGuid();
        var vm = CreateViewModel(new FakeWorkbenchClient());
        vm.SimSelectedParentABrain = new SpeciationSimulatorBrainOption(sharedBrain, sharedBrain.ToString("D"));
        vm.SimSelectedParentBBrain = new SpeciationSimulatorBrainOption(sharedBrain, sharedBrain.ToString("D"));

        vm.StartSimulatorCommand.Execute(null);
        await WaitForAsync(() => vm.SimulatorStatus == "Simulator requires at least two distinct brain parents.");

        Assert.Equal("Simulator requires at least two distinct brain parents.", vm.SimulatorStatus);
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
    public void BuildEvolutionSimArgs_UsesBrainParentIds()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        var parentIds = new List<Guid>
        {
            Guid.Parse("11111111-1111-1111-1111-111111111111"),
            Guid.Parse("22222222-2222-2222-2222-222222222222")
        };

        var method = typeof(SpeciationPanelViewModel).GetMethod(
            "BuildEvolutionSimArgs",
            BindingFlags.NonPublic | BindingFlags.Instance,
            binder: null,
            types: [typeof(int), typeof(int), typeof(IReadOnlyList<Guid>)],
            modifiers: null);
        Assert.NotNull(method);

        var args = (string?)method!.Invoke(vm, [12050, 12074, parentIds]);
        Assert.NotNull(args);
        Assert.Contains("--parent-brain 11111111-1111-1111-1111-111111111111", args!, StringComparison.Ordinal);
        Assert.Contains("--parent-brain 22222222-2222-2222-2222-222222222222", args, StringComparison.Ordinal);
        Assert.DoesNotContain("--parent ", args, StringComparison.Ordinal);
        Assert.Contains("--min-runs 2", args, StringComparison.Ordinal);
        Assert.Contains("--max-runs 12", args, StringComparison.Ordinal);
        Assert.Contains("--run-pressure-mode divergence", args, StringComparison.Ordinal);
        Assert.Contains("--parent-selection-bias stability", args, StringComparison.Ordinal);
        Assert.Contains("--interval-ms 100", args, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildEvolutionSimArgs_NormalizesRunPressureModeAliases()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        var parentIds = new List<Guid>
        {
            Guid.Parse("11111111-1111-1111-1111-111111111111"),
            Guid.Parse("22222222-2222-2222-2222-222222222222")
        };

        var method = typeof(SpeciationPanelViewModel).GetMethod(
            "BuildEvolutionSimArgs",
            BindingFlags.NonPublic | BindingFlags.Instance,
            binder: null,
            types: [typeof(int), typeof(int), typeof(IReadOnlyList<Guid>)],
            modifiers: null);
        Assert.NotNull(method);

        vm.SimRunPressureMode = "stable";
        var stableArgs = (string?)method!.Invoke(vm, [12050, 12074, parentIds]);
        Assert.NotNull(stableArgs);
        Assert.Contains("--run-pressure-mode stability", stableArgs!, StringComparison.Ordinal);

        vm.SimRunPressureMode = "exploratory";
        var exploratoryArgs = (string?)method!.Invoke(vm, [12050, 12074, parentIds]);
        Assert.NotNull(exploratoryArgs);
        Assert.Contains("--run-pressure-mode divergence", exploratoryArgs!, StringComparison.Ordinal);

        vm.SimRunPressureMode = "unexpected-token";
        var fallbackArgs = (string?)method!.Invoke(vm, [12050, 12074, parentIds]);
        Assert.NotNull(fallbackArgs);
        Assert.Contains("--run-pressure-mode neutral", fallbackArgs!, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildEvolutionSimArgs_NormalizesParentSelectionBiasAliases()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        var parentIds = new List<Guid>
        {
            Guid.Parse("11111111-1111-1111-1111-111111111111"),
            Guid.Parse("22222222-2222-2222-2222-222222222222")
        };

        var method = typeof(SpeciationPanelViewModel).GetMethod(
            "BuildEvolutionSimArgs",
            BindingFlags.NonPublic | BindingFlags.Instance,
            binder: null,
            types: [typeof(int), typeof(int), typeof(IReadOnlyList<Guid>)],
            modifiers: null);
        Assert.NotNull(method);

        vm.SimParentSelectionBias = "stable";
        var stableArgs = (string?)method!.Invoke(vm, [12050, 12074, parentIds]);
        Assert.NotNull(stableArgs);
        Assert.Contains("--parent-selection-bias stability", stableArgs!, StringComparison.Ordinal);

        vm.SimParentSelectionBias = "exploratory";
        var divergenceArgs = (string?)method!.Invoke(vm, [12050, 12074, parentIds]);
        Assert.NotNull(divergenceArgs);
        Assert.Contains("--parent-selection-bias divergence", divergenceArgs!, StringComparison.Ordinal);

        vm.SimParentSelectionBias = "unexpected-token";
        var fallbackArgs = (string?)method!.Invoke(vm, [12050, 12074, parentIds]);
        Assert.NotNull(fallbackArgs);
        Assert.Contains("--parent-selection-bias neutral", fallbackArgs!, StringComparison.Ordinal);
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
        Assert.Contains("log10", vm.PopulationChartMetricLabel, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("10", vm.FlowChartStartEpochLabel);
        Assert.Equal("10.5", vm.FlowChartMidEpochLabel);
        Assert.Equal("11", vm.FlowChartEndEpochLabel);
        Assert.All(vm.PopulationChartSeries, item => Assert.False(string.IsNullOrWhiteSpace(item.PathData)));
        Assert.All(vm.FlowChartAreas, item => Assert.False(string.IsNullOrWhiteSpace(item.PathData)));
        Assert.All(vm.PopulationChartLegend, item => Assert.True(string.IsNullOrEmpty(item.ValueLabel)));
        Assert.All(vm.FlowChartLegend, item => Assert.True(string.IsNullOrEmpty(item.ValueLabel)));
    }

    [Fact]
    public async Task RefreshHistoryCommand_FlowChart_IncludesTopElevenSpeciesPlusOtherBucket()
    {
        var history = Enumerable
            .Range(0, 12)
            .SelectMany(index => new[]
            {
                new SpeciationMembershipRecord
                {
                    EpochId = 20,
                    BrainId = Guid.NewGuid().ToProtoUuid(),
                    SpeciesId = $"species.{index:00}",
                    SpeciesDisplayName = $"Species {index:00}",
                    AssignedMs = (ulong)(1_000 + index)
                },
                new SpeciationMembershipRecord
                {
                    EpochId = 21,
                    BrainId = Guid.NewGuid().ToProtoUuid(),
                    SpeciesId = $"species.{index:00}",
                    SpeciesDisplayName = $"Species {index:00}",
                    AssignedMs = (ulong)(2_000 + index)
                }
            })
            .ToArray();
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

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.FlowChartAreas.Count == 12);

        Assert.Contains("top 11/12 species + Other", vm.FlowChartRangeLabel, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(vm.FlowChartAreas, item => string.Equals(item.SpeciesId, "(other)", StringComparison.Ordinal));
        Assert.Contains(vm.FlowChartLegend, item => string.Equals(item.Label, "Other species", StringComparison.Ordinal));
    }

    [Fact]
    public async Task RefreshHistoryCommand_FlowChart_DerivedSpecies_StaysWithinSourceLineageBand()
    {
        var history = CreateMemberships(epochId: 40, speciesId: "species.beta", speciesDisplayName: "Beta", count: 5, assignedMsStart: 1_000)
            .Concat(CreateMemberships(epochId: 40, speciesId: "species.source", speciesDisplayName: "Source", count: 4, assignedMsStart: 2_000))
            .Concat(CreateMemberships(epochId: 40, speciesId: "species.dominant", speciesDisplayName: "Dominant", count: 1, assignedMsStart: 3_000))
            .Concat(CreateMemberships(epochId: 41, speciesId: "species.beta", speciesDisplayName: "Beta", count: 4, assignedMsStart: 4_000))
            .Concat(CreateMemberships(epochId: 41, speciesId: "species.source", speciesDisplayName: "Source", count: 2, assignedMsStart: 5_000))
            .Concat(CreateMemberships(epochId: 41, speciesId: "species.dominant", speciesDisplayName: "Dominant", count: 3, assignedMsStart: 6_000))
            .Concat(new[]
            {
                new SpeciationMembershipRecord
                {
                    EpochId = 41,
                    BrainId = Guid.NewGuid().ToProtoUuid(),
                    SpeciesId = "species.child",
                    SpeciesDisplayName = "Child",
                    AssignedMs = 7_000,
                    DecisionReason = "lineage_diverged_new_species",
                    DecisionMetadataJson = BuildSplitDecisionMetadata(
                        similarity: 0.55,
                        splitThreshold: 0.66,
                        dominantSpeciesId: "species.dominant",
                        dominantSpeciesDisplayName: "Dominant",
                        sourceSpeciesId: "species.source",
                        sourceSpeciesDisplayName: "Source")
                }
            })
            .ToArray();
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

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.FlowChartAreas.Count == 4);

        var childSpans = ExtractFlowRowSpans(vm.FlowChartAreas.Single(item => item.SpeciesId == "species.child").PathData);
        var betaSpans = ExtractFlowRowSpans(vm.FlowChartAreas.Single(item => item.SpeciesId == "species.beta").PathData);
        var dominantSpans = ExtractFlowRowSpans(vm.FlowChartAreas.Single(item => item.SpeciesId == "species.dominant").PathData);

        Assert.Single(childSpans);
        Assert.True(childSpans[0].StartX >= betaSpans[^1].EndX - 0.01d);
        Assert.True(childSpans[0].EndX <= dominantSpans[^1].StartX + 0.01d);
        Assert.True(childSpans[0].EndX > childSpans[0].StartX);
    }

    [Fact]
    public async Task RefreshHistoryCommand_FlowChart_FirstAppearanceWithoutParentAnchor_DoesNotRenderPreOriginRow()
    {
        var history = CreateMemberships(epochId: 50, speciesId: "species.root", speciesDisplayName: "Root", count: 4, assignedMsStart: 1_000)
            .Concat(CreateMemberships(epochId: 50, speciesId: "species.other", speciesDisplayName: "Other", count: 6, assignedMsStart: 2_000))
            .Concat(CreateMemberships(epochId: 51, speciesId: "species.root", speciesDisplayName: "Root", count: 4, assignedMsStart: 3_000))
            .Concat(CreateMemberships(epochId: 51, speciesId: "species.other", speciesDisplayName: "Other", count: 5, assignedMsStart: 4_000))
            .Concat(new[]
            {
                new SpeciationMembershipRecord
                {
                    EpochId = 51,
                    BrainId = Guid.NewGuid().ToProtoUuid(),
                    SpeciesId = "species.new",
                    SpeciesDisplayName = "New",
                    AssignedMs = 5_000
                }
            })
            .ToArray();
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

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.FlowChartAreas.Count == 3);

        var newSpeciesSpans = ExtractFlowRowSpans(vm.FlowChartAreas.Single(item => item.SpeciesId == "species.new").PathData);

        Assert.Single(newSpeciesSpans);
        Assert.True(newSpeciesSpans[0].EndX > newSpeciesSpans[0].StartX);
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
                TotalRecords = 5000,
                History = { history }
            }
        };
        var vm = CreateViewModel(client);
        vm.CurrentEpochId = 17;

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.EpochSummaries.Count == 2);

        Assert.Contains("epoch 17", vm.CurrentEpochMaxDivergenceLabel, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("0.8", vm.CurrentEpochMaxDivergenceLabel, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task RefreshHistoryCommand_UsesAssignedSpeciesSimilarityForCurrentEpochDivergence()
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
                DecisionMetadataJson =
                    "{\"lineage\":{\"lineage_assignment_similarity_score\":0.93,\"dominant_species_similarity_score\":0.41,\"source_species_similarity_score\":0.41}}"
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
        await WaitForAsync(() => vm.EpochSummaries.Count == 1);

        Assert.Contains("within-species divergence", vm.CurrentEpochMaxDivergenceLabel, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("0.07", vm.CurrentEpochMaxDivergenceLabel, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("0.93", vm.CurrentEpochMaxDivergenceLabel, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task RefreshHistoryCommand_BuildsSplitProximitySeriesAndSummary()
    {
        var history = new[]
        {
            new SpeciationMembershipRecord
            {
                EpochId = 30,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.a",
                SpeciesDisplayName = "Alpha",
                AssignedMs = 1000,
                DecisionMetadataJson = "{\"scores\":{\"similarity_score\":0.78},\"assignment_policy\":{\"lineage_split_threshold\":0.66}}"
            },
            new SpeciationMembershipRecord
            {
                EpochId = 30,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.b",
                SpeciesDisplayName = "Beta",
                AssignedMs = 1001,
                DecisionMetadataJson = "{\"scores\":{\"similarity_score\":0.61},\"assignment_policy\":{\"lineage_split_threshold\":0.66}}"
            },
            new SpeciationMembershipRecord
            {
                EpochId = 31,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.a",
                SpeciesDisplayName = "Alpha",
                AssignedMs = 1002,
                DecisionMetadataJson = "{\"scores\":{\"similarity_score\":0.55},\"assignment_policy\":{\"lineage_split_threshold\":0.66}}"
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
        vm.CurrentEpochId = 31;

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.SplitProximityChartSeries.Count == 2);

        Assert.Contains("split threshold", vm.SplitProximityChartMetricLabel, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("epoch 31", vm.CurrentEpochSplitProximityLabel, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("min=-0.", vm.CurrentEpochSplitProximityLabel, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("most recent 2/2 species", vm.SplitProximityChartRangeLabel, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task RefreshHistoryCommand_SplitProximityChart_UsesMostRecentTwelveSpecies()
    {
        var history = new List<SpeciationMembershipRecord>
        {
            new()
            {
                EpochId = 70,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.old",
                SpeciesDisplayName = "Old",
                AssignedMs = 1_000,
                DecisionMetadataJson = BuildSplitDecisionMetadata(0.84d, 0.66d)
            },
            new()
            {
                EpochId = 70,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.old",
                SpeciesDisplayName = "Old",
                AssignedMs = 1_001,
                DecisionMetadataJson = BuildSplitDecisionMetadata(0.83d, 0.66d)
            },
            new()
            {
                EpochId = 70,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.old",
                SpeciesDisplayName = "Old",
                AssignedMs = 1_002,
                DecisionMetadataJson = BuildSplitDecisionMetadata(0.82d, 0.66d)
            }
        };

        history.AddRange(
            Enumerable.Range(1, 12).Select(index => new SpeciationMembershipRecord
            {
                EpochId = 70,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = $"species.new.{index:00}",
                SpeciesDisplayName = $"New {index:00}",
                AssignedMs = (ulong)(2_000 + index),
                DecisionMetadataJson = BuildSplitDecisionMetadata(0.72d + (index * 0.01d), 0.66d)
            }));

        var client = new FakeWorkbenchClient
        {
            HistoryResponse = new SpeciationListHistoryResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                TotalRecords = (uint)history.Count,
                History = { history }
            }
        };
        var vm = CreateViewModel(client);
        vm.CurrentEpochId = 70;

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.SplitProximityChartSeries.Count == 12);

        Assert.Contains("most recent 12/13 species", vm.SplitProximityChartRangeLabel, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(vm.SplitProximityChartSeries, item => string.Equals(item.SpeciesId, "species.old", StringComparison.Ordinal));
        Assert.Contains(vm.SplitProximityChartSeries, item => string.Equals(item.SpeciesId, "species.new.12", StringComparison.Ordinal));
    }

    [Fact]
    public async Task RefreshHistoryCommand_SplitProximityChart_UsesObservedAxisExtents()
    {
        var history = new[]
        {
            new SpeciationMembershipRecord
            {
                EpochId = 80,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.a",
                SpeciesDisplayName = "Alpha",
                AssignedMs = 1_000,
                DecisionMetadataJson = BuildSplitDecisionMetadata(0.92d, 0.80d)
            },
            new SpeciationMembershipRecord
            {
                EpochId = 80,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.b",
                SpeciesDisplayName = "Beta",
                AssignedMs = 1_001,
                DecisionMetadataJson = BuildSplitDecisionMetadata(0.84d, 0.80d)
            },
            new SpeciationMembershipRecord
            {
                EpochId = 81,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.a",
                SpeciesDisplayName = "Alpha",
                AssignedMs = 2_000,
                DecisionMetadataJson = BuildSplitDecisionMetadata(0.97d, 0.80d)
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
        vm.CurrentEpochId = 81;

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.SplitProximityChartSeries.Count == 2);

        Assert.Equal("+0.17", vm.SplitProximityChartYAxisTopLabel);
        Assert.Equal("+0.105", vm.SplitProximityChartYAxisMidLabel);
        Assert.Equal("+0.04", vm.SplitProximityChartYAxisBottomLabel);
    }

    [Fact]
    public async Task RefreshHistoryCommand_UsesDynamicSplitThresholdFromMetadataWhenAvailable()
    {
        var history = new[]
        {
            new SpeciationMembershipRecord
            {
                EpochId = 42,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.a",
                SpeciesDisplayName = "Alpha",
                AssignedMs = 1000,
                DecisionMetadataJson =
                    "{\"scores\":{\"similarity_score\":0.83},\"assignment_policy\":{\"lineage_split_threshold\":0.60,\"lineage_split_guard_margin\":0.00,\"lineage_dynamic_split_threshold\":0.86,\"lineage_split_threshold_source\":\"species_floor\"}}"
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
        vm.CurrentEpochId = 42;

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.SplitProximityChartSeries.Count == 1);

        Assert.Contains("effective split 0.86", vm.CurrentEpochSplitProximityLabel, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("min=-0.03", vm.CurrentEpochSplitProximityLabel, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task RefreshHistoryCommand_PrefersAssignedSplitProximityPerspectiveOverSourcePerspective()
    {
        var history = new[]
        {
            new SpeciationMembershipRecord
            {
                EpochId = 43,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.a",
                SpeciesDisplayName = "Alpha",
                AssignedMs = 1000,
                DecisionMetadataJson =
                    "{\"lineage\":{\"lineage_assignment_similarity_score\":1.00,\"dominant_species_similarity_score\":0.50,\"source_species_similarity_score\":0.50,\"split_proximity_to_dynamic_threshold\":0.16,\"source_split_proximity_to_dynamic_threshold\":-0.34},\"assignment_policy\":{\"lineage_split_threshold\":0.60,\"lineage_split_guard_margin\":0.00,\"lineage_dynamic_split_threshold\":0.70,\"lineage_source_dynamic_split_threshold\":0.84}}"
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
        vm.CurrentEpochId = 43;

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.SplitProximityChartSeries.Count == 1);

        Assert.Contains("effective split 0.7", vm.CurrentEpochSplitProximityLabel, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("min=+0.16", vm.CurrentEpochSplitProximityLabel, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task RefreshHistoryCommand_SingleEpochMultiRowHistory_SpansChartWidth()
    {
        var history = new[]
        {
            new SpeciationMembershipRecord
            {
                EpochId = 55,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.a",
                SpeciesDisplayName = "Alpha",
                AssignedMs = 1000,
                DecisionMetadataJson = "{\"scores\":{\"similarity_score\":0.90},\"assignment_policy\":{\"lineage_split_threshold\":0.66}}"
            },
            new SpeciationMembershipRecord
            {
                EpochId = 55,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.a",
                SpeciesDisplayName = "Alpha",
                AssignedMs = 1001,
                DecisionMetadataJson = "{\"scores\":{\"similarity_score\":0.82},\"assignment_policy\":{\"lineage_split_threshold\":0.66}}"
            },
            new SpeciationMembershipRecord
            {
                EpochId = 55,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.a",
                SpeciesDisplayName = "Alpha",
                AssignedMs = 1002,
                DecisionMetadataJson = "{\"scores\":{\"similarity_score\":0.72},\"assignment_policy\":{\"lineage_split_threshold\":0.66}}"
            },
            new SpeciationMembershipRecord
            {
                EpochId = 55,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.a",
                SpeciesDisplayName = "Alpha",
                AssignedMs = 1003,
                DecisionMetadataJson = "{\"scores\":{\"similarity_score\":0.61},\"assignment_policy\":{\"lineage_split_threshold\":0.66}}"
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
        vm.CurrentEpochId = 55;

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.PopulationChartSeries.Count == 1 && vm.SplitProximityChartSeries.Count == 1);

        Assert.Contains("Epoch 55 row samples", vm.PopulationChartRangeLabel, StringComparison.Ordinal);
        Assert.Contains("Epoch 55 row samples", vm.SplitProximityChartRangeLabel, StringComparison.Ordinal);
        Assert.Equal("1", vm.FlowChartStartEpochLabel);
        Assert.Equal("2.5", vm.FlowChartMidEpochLabel);
        Assert.Equal("4", vm.FlowChartEndEpochLabel);
        Assert.Contains("min=", vm.CurrentEpochSplitProximityLabel, StringComparison.OrdinalIgnoreCase);

        var populationMaxX = ExtractMaxPathX(vm.PopulationChartSeries[0].PathData);
        var splitProximityMaxX = ExtractMaxPathX(vm.SplitProximityChartSeries[0].PathData);
        var requiredMaxX = vm.PopulationChartWidth - 20d;
        Assert.True(
            populationMaxX >= requiredMaxX,
            $"Population chart did not span expected width. maxX={populationMaxX:0.###}, required={requiredMaxX:0.###}, path='{vm.PopulationChartSeries[0].PathData}'.");
        Assert.True(
            splitProximityMaxX >= requiredMaxX,
            $"Split-proximity chart did not span expected width. maxX={splitProximityMaxX:0.###}, required={requiredMaxX:0.###}, path='{vm.SplitProximityChartSeries[0].PathData}'.");
        Assert.Contains(" L ", vm.PopulationChartSeries[0].PathData, StringComparison.Ordinal);
        Assert.Contains(" L ", vm.SplitProximityChartSeries[0].PathData, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RefreshHistoryCommand_SpeciesColors_AvoidAdjacentCollisionsAndStayConsistentAcrossCharts()
    {
        var history = new[]
        {
            new SpeciationMembershipRecord
            {
                EpochId = 90,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.0",
                SpeciesDisplayName = "Root",
                DecisionReason = "explicit_species",
                AssignedMs = 1_000,
                DecisionMetadataJson = BuildSplitDecisionMetadata(0.91d, 0.80d)
            },
            new SpeciationMembershipRecord
            {
                EpochId = 90,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.1",
                SpeciesDisplayName = "Root [A]",
                DecisionReason = "lineage_diverged_new_species",
                AssignedMs = 1_001,
                DecisionMetadataJson = BuildSplitDecisionMetadata(
                    0.89d,
                    0.80d,
                    dominantSpeciesId: "species.0",
                    dominantSpeciesDisplayName: "Root")
            },
            new SpeciationMembershipRecord
            {
                EpochId = 91,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.6",
                SpeciesDisplayName = "Root [B]",
                DecisionReason = "lineage_diverged_new_species",
                AssignedMs = 1_002,
                DecisionMetadataJson = BuildSplitDecisionMetadata(
                    0.88d,
                    0.80d,
                    dominantSpeciesId: "species.0",
                    dominantSpeciesDisplayName: "Root")
            },
            new SpeciationMembershipRecord
            {
                EpochId = 91,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.0",
                SpeciesDisplayName = "Root",
                DecisionReason = "lineage_hysteresis_hold",
                AssignedMs = 2_000,
                DecisionMetadataJson = BuildSplitDecisionMetadata(0.93d, 0.80d)
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

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.SplitProximityChartSeries.Count == 3 && FlattenCladogram(vm.CladogramItems).Count() == 3);

        var rootPopulationColor = vm.PopulationChartSeries.Single(item => item.SpeciesId == "species.0").Stroke;
        var firstChildPopulationColor = vm.PopulationChartSeries.Single(item => item.SpeciesId == "species.1").Stroke;
        var secondChildPopulationColor = vm.PopulationChartSeries.Single(item => item.SpeciesId == "species.6").Stroke;

        Assert.NotEqual(rootPopulationColor, firstChildPopulationColor);
        Assert.NotEqual(firstChildPopulationColor, secondChildPopulationColor);
        Assert.NotEqual(rootPopulationColor, secondChildPopulationColor);
        Assert.True(
            CalculateColorDistance(rootPopulationColor, firstChildPopulationColor) >= 60d,
            $"Expected adjacent first-seen species colors to stay visually distinct, but saw {rootPopulationColor} vs {firstChildPopulationColor}.");
        Assert.True(
            CalculateColorDistance(firstChildPopulationColor, secondChildPopulationColor) >= 60d,
            $"Expected adjacent first-seen species colors to stay visually distinct, but saw {firstChildPopulationColor} vs {secondChildPopulationColor}.");
        Assert.True(
            CalculateColorDistance(rootPopulationColor, secondChildPopulationColor) >= 60d,
            $"Expected the shared species color map to avoid short-window color clustering, but saw {rootPopulationColor} vs {secondChildPopulationColor}.");

        Assert.Equal(rootPopulationColor, vm.FlowChartAreas.Single(item => item.SpeciesId == "species.0").Stroke);
        Assert.Equal(rootPopulationColor, vm.SplitProximityChartSeries.Single(item => item.SpeciesId == "species.0").Stroke);
        Assert.Equal(rootPopulationColor, FlattenCladogram(vm.CladogramItems).Single(item => item.SpeciesId == "species.0").Color);

        Assert.Equal(firstChildPopulationColor, vm.FlowChartAreas.Single(item => item.SpeciesId == "species.1").Stroke);
        Assert.Equal(firstChildPopulationColor, vm.SplitProximityChartSeries.Single(item => item.SpeciesId == "species.1").Stroke);
        Assert.Equal(firstChildPopulationColor, FlattenCladogram(vm.CladogramItems).Single(item => item.SpeciesId == "species.1").Color);

        Assert.Equal(secondChildPopulationColor, vm.FlowChartAreas.Single(item => item.SpeciesId == "species.6").Stroke);
        Assert.Equal(secondChildPopulationColor, vm.SplitProximityChartSeries.Single(item => item.SpeciesId == "species.6").Stroke);
        Assert.Equal(secondChildPopulationColor, FlattenCladogram(vm.CladogramItems).Single(item => item.SpeciesId == "species.6").Color);
    }

    [Fact]
    public async Task RefreshHistoryCommand_BuildsCladogramFromDivergenceLineageMetadata()
    {
        var history = new[]
        {
            new SpeciationMembershipRecord
            {
                EpochId = 9,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.root",
                SpeciesDisplayName = "Root",
                DecisionReason = "explicit_species",
                AssignedMs = 1000
            },
            new SpeciationMembershipRecord
            {
                EpochId = 10,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.child",
                SpeciesDisplayName = "Root [A]",
                DecisionReason = "lineage_diverged_new_species",
                AssignedMs = 1100,
                DecisionMetadataJson = "{\"lineage\":{\"dominant_species_id\":\"species.root\",\"dominant_species_display_name\":\"Root\"}}"
            },
            new SpeciationMembershipRecord
            {
                EpochId = 10,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.child",
                SpeciesDisplayName = "Root [A]",
                DecisionReason = "lineage_hysteresis_hold",
                AssignedMs = 1200,
                DecisionMetadataJson = "{\"lineage\":{\"dominant_species_id\":\"species.root\",\"dominant_species_display_name\":\"Root\"}}"
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

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.CladogramItems.Count > 0);

        var cladogramNodes = FlattenCladogram(vm.CladogramItems).ToArray();
        Assert.Contains("lineage_diverged_new_species", vm.CladogramMetricLabel, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("edges", vm.CladogramRangeLabel, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(cladogramNodes, item => item.SpeciesId == "species.root" && item.LineText.Contains("Root", StringComparison.Ordinal));
        Assert.Contains(cladogramNodes, item => item.SpeciesId == "species.child" && item.LineText.Contains("[A]", StringComparison.Ordinal));
        Assert.All(cladogramNodes, item => Assert.True(item.IsExpanded));
    }

    [Fact]
    public async Task RefreshHistoryCommand_AutoExpandsCladogramBranchWhenNewSpeciesAppear()
    {
        var brainRoot = Guid.NewGuid();
        var brainChild = Guid.NewGuid();
        var brainGrandchild = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            HistoryResponse = new SpeciationListHistoryResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                TotalRecords = 2,
                History =
                {
                    new SpeciationMembershipRecord
                    {
                        EpochId = 9,
                        BrainId = brainRoot.ToProtoUuid(),
                        SpeciesId = "species.root",
                        SpeciesDisplayName = "Root",
                        DecisionReason = "explicit_species",
                        AssignedMs = 1000
                    },
                    new SpeciationMembershipRecord
                    {
                        EpochId = 10,
                        BrainId = brainChild.ToProtoUuid(),
                        SpeciesId = "species.child",
                        SpeciesDisplayName = "Child",
                        DecisionReason = "lineage_diverged_new_species",
                        AssignedMs = 1100,
                        DecisionMetadataJson = "{\"lineage\":{\"dominant_species_id\":\"species.root\",\"dominant_species_display_name\":\"Root\"}}"
                    }
                }
            }
        };
        var vm = CreateViewModel(client);

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.CladogramItems.Count == 1 && vm.CladogramItems[0].Children.Count == 1);

        var initialRoot = vm.CladogramItems[0];
        var initialChild = initialRoot.Children[0];
        initialRoot.IsExpanded = false;
        initialChild.IsExpanded = false;

        client.HistoryResponse = new SpeciationListHistoryResponse
        {
            FailureReason = SpeciationFailureReason.SpeciationFailureNone,
            TotalRecords = 3,
            History =
            {
                new SpeciationMembershipRecord
                {
                    EpochId = 9,
                    BrainId = brainRoot.ToProtoUuid(),
                    SpeciesId = "species.root",
                    SpeciesDisplayName = "Root",
                    DecisionReason = "explicit_species",
                    AssignedMs = 1000
                },
                new SpeciationMembershipRecord
                {
                    EpochId = 10,
                    BrainId = brainChild.ToProtoUuid(),
                    SpeciesId = "species.child",
                    SpeciesDisplayName = "Child",
                    DecisionReason = "lineage_diverged_new_species",
                    AssignedMs = 1100,
                    DecisionMetadataJson = "{\"lineage\":{\"dominant_species_id\":\"species.root\",\"dominant_species_display_name\":\"Root\"}}"
                },
                new SpeciationMembershipRecord
                {
                    EpochId = 11,
                    BrainId = brainGrandchild.ToProtoUuid(),
                    SpeciesId = "species.grandchild",
                    SpeciesDisplayName = "Grandchild",
                    DecisionReason = "lineage_diverged_new_species",
                    AssignedMs = 1200,
                    DecisionMetadataJson = "{\"lineage\":{\"dominant_species_id\":\"species.child\",\"dominant_species_display_name\":\"Child\"}}"
                }
            }
        };

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => FlattenCladogram(vm.CladogramItems).Any(item => string.Equals(item.SpeciesId, "species.grandchild", StringComparison.Ordinal)));

        var refreshedRoot = vm.CladogramItems[0];
        var refreshedChild = refreshedRoot.Children[0];
        Assert.True(refreshedRoot.IsExpanded);
        Assert.True(refreshedChild.IsExpanded);
    }

    [Fact]
    public async Task RefreshHistoryCommand_PagesHistoryBeyondDefaultChartWindow()
    {
        const int totalHistoryCount = 8_200;
        var page0 = Enumerable
            .Range(0, 8_192)
            .Select(index => new SpeciationMembershipRecord
            {
                EpochId = 41,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = index % 2 == 0 ? "species.a" : "species.b",
                SpeciesDisplayName = index % 2 == 0 ? "Alpha" : "Beta",
                AssignedMs = (ulong)(1_000 + index),
                DecisionMetadataJson = "{\"scores\":{\"similarity_score\":0.77},\"assignment_policy\":{\"lineage_split_threshold\":0.66}}"
            })
            .ToArray();
        var page1 = Enumerable
            .Range(page0.Length, totalHistoryCount - page0.Length)
            .Select(index => new SpeciationMembershipRecord
            {
                EpochId = 41,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = index % 2 == 0 ? "species.a" : "species.b",
                SpeciesDisplayName = index % 2 == 0 ? "Alpha" : "Beta",
                AssignedMs = (ulong)(1_000 + index),
                DecisionMetadataJson = "{\"scores\":{\"similarity_score\":0.77},\"assignment_policy\":{\"lineage_split_threshold\":0.66}}"
            })
            .ToArray();
        var client = new FakeWorkbenchClient
        {
            HistoryResponse = new SpeciationListHistoryResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                TotalRecords = (uint)totalHistoryCount
            }
        };
        client.HistoryResponsesByOffset[0] = new SpeciationListHistoryResponse
        {
            FailureReason = SpeciationFailureReason.SpeciationFailureNone,
            TotalRecords = (uint)totalHistoryCount,
            History = { page0 }
        };
        client.HistoryResponsesByOffset[8192] = new SpeciationListHistoryResponse
        {
            FailureReason = SpeciationFailureReason.SpeciationFailureNone,
            TotalRecords = (uint)totalHistoryCount,
            History = { page1 }
        };
        var vm = CreateViewModel(client);
        vm.CurrentEpochId = 41;

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => client.HistoryCallCount == 2 && vm.HistoryStatus.Contains("fetched=8200", StringComparison.Ordinal));

        Assert.Equal(2, client.HistoryCallCount);
        Assert.Equal([8192u, 8192u], client.RequestedHistoryLimits);
        Assert.Equal([0u, 8192u], client.RequestedHistoryOffsets);
        Assert.Equal($"Speciation data loaded: fetched={totalHistoryCount} total={totalHistoryCount}", vm.HistoryStatus);
    }

    [Fact]
    public async Task RefreshHistoryCommand_ChartWindowLimitsTrendCardsWithoutTruncatingCladogram()
    {
        var history = new[]
        {
            new SpeciationMembershipRecord
            {
                EpochId = 99,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.root",
                SpeciesDisplayName = "Root",
                DecisionReason = "explicit_species",
                AssignedMs = 1_000
            },
            new SpeciationMembershipRecord
            {
                EpochId = 99,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.child",
                SpeciesDisplayName = "Root [A]",
                DecisionReason = "lineage_diverged_new_species",
                AssignedMs = 1_001,
                DecisionMetadataJson = "{\"lineage\":{\"dominant_species_id\":\"species.root\",\"dominant_species_display_name\":\"Root\"},\"scores\":{\"similarity_score\":0.88},\"assignment_policy\":{\"lineage_split_threshold\":0.86,\"lineage_split_guard_margin\":0.025}}"
            },
            new SpeciationMembershipRecord
            {
                EpochId = 99,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.root",
                SpeciesDisplayName = "Root",
                DecisionReason = "lineage_hysteresis_hold",
                AssignedMs = 1_002,
                DecisionMetadataJson = "{\"scores\":{\"similarity_score\":0.88},\"assignment_policy\":{\"lineage_split_threshold\":0.86,\"lineage_split_guard_margin\":0.025}}"
            },
            new SpeciationMembershipRecord
            {
                EpochId = 99,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = "species.root",
                SpeciesDisplayName = "Root",
                DecisionReason = "lineage_hysteresis_hold",
                AssignedMs = 1_003,
                DecisionMetadataJson = "{\"scores\":{\"similarity_score\":0.88},\"assignment_policy\":{\"lineage_split_threshold\":0.86,\"lineage_split_guard_margin\":0.025}}"
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
        vm.ChartWindowText = "2";

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => vm.PopulationChartSeries.Count == 1 && FlattenCladogram(vm.CladogramItems).Any(item => string.Equals(item.SpeciesId, "species.child", StringComparison.Ordinal)));

        Assert.Equal("Speciation data loaded: fetched=4 total=4", vm.HistoryStatus);
        Assert.Equal("2", vm.ChartWindowText);
        Assert.Single(vm.EpochSummaries);
        Assert.Single(vm.PopulationChartSeries);
        Assert.Contains(FlattenCladogram(vm.CladogramItems), item => string.Equals(item.SpeciesId, "species.child", StringComparison.Ordinal));
    }

    [Fact]
    public async Task RefreshHistoryCommand_PersistsHistoryLimitOnlyWhenChanged()
    {
        var client = new FakeWorkbenchClient
        {
            HistoryResponse = new SpeciationListHistoryResponse
            {
                FailureReason = SpeciationFailureReason.SpeciationFailureNone,
                TotalRecords = 1,
                History =
                {
                    new SpeciationMembershipRecord
                    {
                        EpochId = 7,
                        BrainId = Guid.NewGuid().ToProtoUuid(),
                        SpeciesId = "species.a",
                        SpeciesDisplayName = "Alpha",
                        AssignedMs = 1_000
                    }
                }
            }
        };
        var vm = CreateViewModel(client);
        vm.HistoryLimitText = "128";

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => client.HistoryCallCount == 1);

        vm.RefreshHistoryCommand.Execute(null);
        await WaitForAsync(() => client.HistoryCallCount == 2);

        Assert.Equal(
            1,
            client.SettingUpdateEvents.Count(entry =>
                string.Equals(entry.Key, SpeciationSettingsKeys.HistoryLimitKey, StringComparison.Ordinal)));
    }

    [Fact]
    public async Task RefreshSimulatorStatusCommand_ParsesExtendedJsonStats()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        var logPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.sim.log");
        var statusLine = "{\"type\":\"evolution_sim_status\",\"final\":true,\"session_id\":\"sess-123\",\"running\":false,\"iterations\":22,\"parent_pool_size\":14,\"compatibility_checks\":100,\"compatible_pairs\":4,\"reproduction_calls\":7,\"reproduction_failures\":3,\"reproduction_runs_observed\":11,\"reproduction_runs_with_mutations\":9,\"reproduction_mutation_events\":23,\"similarity_samples\":11,\"min_similarity_observed\":0.61,\"max_similarity_observed\":0.97,\"assessment_similarity_samples\":9,\"min_assessment_similarity_observed\":0.5,\"max_assessment_similarity_observed\":0.97,\"reproduction_similarity_samples\":7,\"min_reproduction_similarity_observed\":0.72,\"max_reproduction_similarity_observed\":0.96,\"speciation_commit_similarity_samples\":6,\"min_speciation_commit_similarity_observed\":0.73,\"max_speciation_commit_similarity_observed\":0.95,\"children_added_to_pool\":5,\"speciation_commit_attempts\":6,\"speciation_commit_successes\":2,\"last_failure\":\"example_failure\",\"last_seed\":42}";
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
            Assert.Contains("sim_overall=0.61..0.97 (11)", vm.SimulatorDetailedStats, StringComparison.Ordinal);
            Assert.Contains("sim_assess=0.5..0.97 (9)", vm.SimulatorDetailedStats, StringComparison.Ordinal);
            Assert.Contains("sim_repro=0.72..0.96 (7)", vm.SimulatorDetailedStats, StringComparison.Ordinal);
            Assert.Contains("sim_commit=0.73..0.95 (6)", vm.SimulatorDetailedStats, StringComparison.Ordinal);
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

    private static SpeciationPanelViewModel CreateViewModel(
        FakeWorkbenchClient client,
        bool enableAutoRefresh = false,
        ILocalProjectLaunchPreparer? launchPreparer = null)
    {
        var connections = new ConnectionViewModel
        {
            IoHost = "127.0.0.1",
            IoPortText = "12050",
            IoGateway = "io-gateway",
            SettingsConnected = true,
            SpeciationDiscoverable = true
        };
        return new SpeciationPanelViewModel(
            new UiDispatcher(),
            connections,
            client,
            startSpeciationService: null,
            stopSpeciationService: null,
            refreshOrchestrator: null,
            enableLiveChartsAutoRefresh: enableAutoRefresh,
            launchPreparer: launchPreparer);
    }

    private static SpeciationRuntimeConfig InvokeBuildRuntimeConfigFromDraft(SpeciationPanelViewModel vm)
    {
        var method = typeof(SpeciationPanelViewModel).GetMethod(
            "BuildRuntimeConfigFromDraft",
            BindingFlags.NonPublic | BindingFlags.Instance);
        return Assert.IsType<SpeciationRuntimeConfig>(method!.Invoke(vm, null));
    }

    private static async Task InvokePersistSpeciationSettingsAsync(SpeciationPanelViewModel vm)
    {
        var method = typeof(SpeciationPanelViewModel).GetMethod(
            "PersistSpeciationSettingsAsync",
            BindingFlags.NonPublic | BindingFlags.Instance);
        var task = Assert.IsAssignableFrom<Task>(method!.Invoke(vm, null));
        await task;
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

    private static string BuildSplitDecisionMetadata(
        double similarity,
        double splitThreshold,
        double splitGuardMargin = 0d,
        string? dominantSpeciesId = null,
        string? dominantSpeciesDisplayName = null,
        string? sourceSpeciesId = null,
        string? sourceSpeciesDisplayName = null)
    {
        var root = new JsonObject
        {
            ["scores"] = new JsonObject
            {
                ["similarity_score"] = similarity
            },
            ["assignment_policy"] = new JsonObject
            {
                ["lineage_split_threshold"] = splitThreshold,
                ["lineage_split_guard_margin"] = splitGuardMargin
            }
        };

        if (!string.IsNullOrWhiteSpace(sourceSpeciesId)
            || !string.IsNullOrWhiteSpace(sourceSpeciesDisplayName)
            || !string.IsNullOrWhiteSpace(dominantSpeciesId)
            || !string.IsNullOrWhiteSpace(dominantSpeciesDisplayName))
        {
            var lineage = new JsonObject();
            if (!string.IsNullOrWhiteSpace(sourceSpeciesId))
            {
                lineage["source_species_id"] = sourceSpeciesId;
            }

            if (!string.IsNullOrWhiteSpace(sourceSpeciesDisplayName))
            {
                lineage["source_species_display_name"] = sourceSpeciesDisplayName;
            }

            if (!string.IsNullOrWhiteSpace(dominantSpeciesId))
            {
                lineage["dominant_species_id"] = dominantSpeciesId;
            }

            if (!string.IsNullOrWhiteSpace(dominantSpeciesDisplayName))
            {
                lineage["dominant_species_display_name"] = dominantSpeciesDisplayName;
            }

            root["lineage"] = lineage;
        }

        return root.ToJsonString();
    }

    private static IReadOnlyList<(double StartX, double EndX, double Y)> ExtractFlowRowSpans(string pathData)
    {
        Assert.False(string.IsNullOrWhiteSpace(pathData));
        var numericTokens = Regex.Matches(pathData, @"-?\d+(?:\.\d+)?")
            .Select(match => double.Parse(match.Value, CultureInfo.InvariantCulture))
            .ToArray();
        Assert.True(numericTokens.Length >= 4 && numericTokens.Length % 2 == 0);

        var points = new List<(double X, double Y)>(numericTokens.Length / 2);
        for (var i = 0; i < numericTokens.Length; i += 2)
        {
            points.Add((numericTokens[i], numericTokens[i + 1]));
        }

        Assert.True(points.Count % 2 == 0);
        var sampleCount = points.Count / 2;
        var spans = new List<(double StartX, double EndX, double Y)>(sampleCount);
        for (var sampleIndex = 0; sampleIndex < sampleCount; sampleIndex++)
        {
            var endPoint = points[sampleIndex];
            var startPoint = points[points.Count - 1 - sampleIndex];
            Assert.Equal(endPoint.Y, startPoint.Y, 3);
            spans.Add((Math.Min(startPoint.X, endPoint.X), Math.Max(startPoint.X, endPoint.X), endPoint.Y));
        }

        return spans;
    }

    private static double CalculateColorDistance(string leftHex, string rightHex)
    {
        var left = ParseHexColor(leftHex);
        var right = ParseHexColor(rightHex);
        var redDelta = left.Red - right.Red;
        var greenDelta = left.Green - right.Green;
        var blueDelta = left.Blue - right.Blue;
        return Math.Sqrt((redDelta * redDelta) + (greenDelta * greenDelta) + (blueDelta * blueDelta));
    }

    private static (int Red, int Green, int Blue) ParseHexColor(string value)
    {
        Assert.False(string.IsNullOrWhiteSpace(value));
        Assert.StartsWith("#", value, StringComparison.Ordinal);
        Assert.Equal(7, value.Length);
        return (
            int.Parse(value.AsSpan(1, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture),
            int.Parse(value.AsSpan(3, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture),
            int.Parse(value.AsSpan(5, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture));
    }

    private static double ExtractMaxPathX(string pathData)
    {
        if (string.IsNullOrWhiteSpace(pathData))
        {
            return double.NaN;
        }

        var tokens = pathData.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        double? maxX = null;
        for (var i = 0; i < tokens.Length - 1; i++)
        {
            var token = tokens[i];
            if (!string.Equals(token, "M", StringComparison.Ordinal) && !string.Equals(token, "L", StringComparison.Ordinal))
            {
                continue;
            }

            if (double.TryParse(
                    tokens[i + 1],
                    System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture,
                    out var x))
            {
                maxX = maxX.HasValue ? Math.Max(maxX.Value, x) : x;
            }
        }

        return maxX ?? double.NaN;
    }

    private static IEnumerable<SpeciationMembershipRecord> CreateMemberships(
        long epochId,
        string speciesId,
        string speciesDisplayName,
        int count,
        ulong assignedMsStart)
    {
        return Enumerable.Range(0, count)
            .Select(index => new SpeciationMembershipRecord
            {
                EpochId = (ulong)epochId,
                BrainId = Guid.NewGuid().ToProtoUuid(),
                SpeciesId = speciesId,
                SpeciesDisplayName = speciesDisplayName,
                AssignedMs = assignedMsStart + (ulong)index
            });
    }

    private static IEnumerable<SpeciationCladogramItem> FlattenCladogram(IEnumerable<SpeciationCladogramItem> roots)
    {
        foreach (var root in roots)
        {
            yield return root;
            foreach (var child in FlattenCladogram(root.Children))
            {
                yield return child;
            }
        }
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
        public List<uint> RequestedHistoryLimits { get; } = new();
        public List<uint> RequestedHistoryOffsets { get; } = new();
        public Dictionary<uint, SpeciationListHistoryResponse> HistoryResponsesByOffset { get; } = new();
        public Dictionary<string, string> SettingUpdates { get; } = new(StringComparer.Ordinal);
        public List<(string Key, string Value)> SettingUpdateEvents { get; } = new();

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
            uint offset = 0,
            CancellationToken cancellationToken = default)
        {
            HistoryCallCount++;
            RequestedHistoryLimits.Add(limit);
            RequestedHistoryOffsets.Add(offset);
            if (HistoryResponsesByOffset.TryGetValue(offset, out var response))
            {
                return Task.FromResult(response);
            }

            return Task.FromResult(HistoryResponse);
        }

        public override Task<SettingValue?> SetSettingAsync(string key, string value)
        {
            SettingUpdates[key] = value;
            SettingUpdateEvents.Add((key, value));
            return Task.FromResult<SettingValue?>(new SettingValue
            {
                Key = key,
                Value = value,
                UpdatedMs = 1UL
            });
        }
    }

    private sealed class FakeLocalProjectLaunchPreparer(string failureMessage) : ILocalProjectLaunchPreparer
    {
        public Task<LocalProjectLaunchPreparation> PrepareAsync(string projectPath, string exeName, string runtimeArgs, string label)
        {
            return Task.FromResult(new LocalProjectLaunchPreparation(false, null, failureMessage));
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
