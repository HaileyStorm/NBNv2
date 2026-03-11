using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Runtime.SettingsMonitor;
using Nbn.Runtime.Speciation;
using Nbn.Shared;
using Nbn.Tests.TestSupport;
using Proto;
using ProtoIo = Nbn.Proto.Io;
using Repro = Nbn.Proto.Repro;
using ProtoSettings = Nbn.Proto.Settings;
using ProtoSpec = Nbn.Proto.Speciation;

namespace Nbn.Tests.Speciation;

public sealed class SpeciationManagerActorTests
{
    [Fact]
    public async Task Started_ReconcilesKnownBrainsFromSettingsMonitor()
    {
        using var settingsDb = new TempDatabaseScope("settings-monitor.db");
        using var speciationDb = new TempDatabaseScope("speciation.db");

        var settingsStore = new SettingsMonitorStore(settingsDb.DatabasePath);
        await settingsStore.InitializeAsync();

        var system = new ActorSystem();
        try
        {
            var settingsPid = system.Root.Spawn(Props.FromProducer(() => new SettingsMonitorActor(settingsStore)));

            var knownBrains = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
            foreach (var brainId in knownBrains)
            {
                system.Root.Send(settingsPid, new BrainRegistered
                {
                    BrainId = brainId.ToProtoUuid(),
                    SpawnedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    State = "Active"
                });
            }

            await WaitForConditionAsync(
                async () =>
                {
                    var list = await system.Root.RequestAsync<BrainListResponse>(settingsPid, new BrainListRequest());
                    return list.Brains.Count >= knownBrains.Length;
                },
                TimeSpan.FromSeconds(5));

            var speciationStore = new SpeciationStore(speciationDb.DatabasePath);
            var runtimeConfig = CreateRuntimeConfig();
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(speciationStore, runtimeConfig, settingsPid, TimeSpan.FromSeconds(2))));

            await WaitForConditionAsync(
                async () =>
                {
                    var status = await system.Root.RequestAsync<SpeciationStatusResponse>(
                        managerPid,
                        new SpeciationStatusRequest());
                    return status.Status.EpochId > 0
                           && status.Status.MembershipCount == knownBrains.Length;
                },
                TimeSpan.FromSeconds(8));

            var memberships = await system.Root.RequestAsync<SpeciationListMembershipsResponse>(
                managerPid,
                new SpeciationListMembershipsRequest());
            Assert.Equal(knownBrains.Length, memberships.Memberships.Count);

            var membershipByBrain = memberships.Memberships.ToDictionary(
                static item => item.BrainId,
                static item => item);
            foreach (var brainId in knownBrains)
            {
                Assert.True(membershipByBrain.ContainsKey(brainId));
                Assert.Equal(SpeciationOptions.DefaultSpeciesId, membershipByBrain[brainId].SpeciesId);
                Assert.Equal("startup_reconcile", membershipByBrain[brainId].DecisionReason);
            }
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task Started_LoadsRuntimeConfigFromSettingsMonitorKeys()
    {
        using var settingsDb = new TempDatabaseScope("settings-monitor.db");
        using var speciationDb = new TempDatabaseScope("speciation.db");

        var settingsStore = new SettingsMonitorStore(settingsDb.DatabasePath);
        await settingsStore.InitializeAsync();

        var system = new ActorSystem();
        try
        {
            var settingsPid = system.Root.Spawn(Props.FromProducer(() => new SettingsMonitorActor(settingsStore)));
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.PolicyVersionKey,
                Value = "settings-policy-v2"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.DefaultSpeciesIdKey,
                Value = "species.settings"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.DefaultSpeciesDisplayNameKey,
                Value = "Settings Species"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.StartupReconcileReasonKey,
                Value = "settings_reconcile"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.ConfigEnabledKey,
                Value = "false"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.LineageMatchThresholdKey,
                Value = "0.91"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.LineageSplitThresholdKey,
                Value = "0.87"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.ParentConsensusThresholdKey,
                Value = "0.66"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.LineageHysteresisMarginKey,
                Value = "0.03"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.LineageSplitGuardMarginKey,
                Value = "0.01"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.LineageMinParentMembershipsBeforeSplitKey,
                Value = "2"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.LineageRealignParentMembershipWindowKey,
                Value = "4"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.LineageRealignMatchMarginKey,
                Value = "0.06"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.LineageHindsightReassignCommitWindowKey,
                Value = "9"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.LineageHindsightSimilarityMarginKey,
                Value = "0.022"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.CreateDerivedSpeciesOnDivergenceKey,
                Value = "false"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = SpeciationSettingsKeys.DerivedSpeciesPrefixKey,
                Value = "twig"
            });

            var fallbackRuntimeConfig = new SpeciationRuntimeConfig(
                PolicyVersion: "cli-policy",
                ConfigSnapshotJson: "{\"assignment_policy\":{\"lineage_match_threshold\":0.33}}",
                DefaultSpeciesId: "cli-species",
                DefaultSpeciesDisplayName: "CLI Species",
                StartupReconcileDecisionReason: "cli_reconcile");

            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    fallbackRuntimeConfig,
                    settingsPid: settingsPid,
                    settingsRequestTimeout: TimeSpan.FromSeconds(2))));

            await WaitForConditionAsync(
                async () =>
                {
                    var config = await system.Root.RequestAsync<ProtoSpec.SpeciationGetConfigResponse>(
                        managerPid,
                        new ProtoSpec.SpeciationGetConfigRequest());
                    return config.FailureReason == ProtoSpec.SpeciationFailureReason.SpeciationFailureNone
                           && string.Equals(config.Config.PolicyVersion, "settings-policy-v2", StringComparison.Ordinal)
                           && string.Equals(config.Config.DefaultSpeciesId, "species.settings", StringComparison.Ordinal);
                },
                TimeSpan.FromSeconds(8));

            var response = await system.Root.RequestAsync<ProtoSpec.SpeciationGetConfigResponse>(
                managerPid,
                new ProtoSpec.SpeciationGetConfigRequest());
            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, response.FailureReason);
            Assert.Equal("settings-policy-v2", response.Config.PolicyVersion);
            Assert.Equal("species.settings", response.Config.DefaultSpeciesId);
            Assert.Equal("Settings Species", response.Config.DefaultSpeciesDisplayName);
            Assert.Equal("settings_reconcile", response.Config.StartupReconcileDecisionReason);

            using var snapshot = JsonDocument.Parse(response.Config.ConfigSnapshotJson);
            Assert.False(snapshot.RootElement.GetProperty("enabled").GetBoolean());
            var policy = snapshot.RootElement.GetProperty("assignment_policy");
            Assert.Equal(0.91d, policy.GetProperty("lineage_match_threshold").GetDouble(), 3);
            Assert.Equal(0.87d, policy.GetProperty("lineage_split_threshold").GetDouble(), 3);
            Assert.Equal(0.66d, policy.GetProperty("parent_consensus_threshold").GetDouble(), 3);
            Assert.Equal(0.03d, policy.GetProperty("lineage_hysteresis_margin").GetDouble(), 3);
            Assert.Equal(0.01d, policy.GetProperty("lineage_split_guard_margin").GetDouble(), 3);
            Assert.Equal(2, policy.GetProperty("lineage_min_parent_memberships_before_split").GetInt32());
            Assert.Equal(4, policy.GetProperty("lineage_realign_parent_membership_window").GetInt32());
            Assert.Equal(0.06d, policy.GetProperty("lineage_realign_match_margin").GetDouble(), 3);
            Assert.Equal(9, policy.GetProperty("lineage_hindsight_reassign_commit_window").GetInt32());
            Assert.Equal(0.022d, policy.GetProperty("lineage_hindsight_similarity_margin").GetDouble(), 3);
            Assert.False(policy.GetProperty("create_derived_species_on_divergence").GetBoolean());
            Assert.Equal("twig", policy.GetProperty("derived_species_prefix").GetString());
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ResetEpoch_PreservesHistory_And_EnforcesImmutabilityWithinCurrentEpoch()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var system = new ActorSystem();
        try
        {
            var runtimeConfig = CreateRuntimeConfig();
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            var firstEpoch = await WaitForEpochAsync(system, managerPid);
            var brainId = Guid.NewGuid();

            var firstAssign = await system.Root.RequestAsync<SpeciationAssignMembershipResponse>(
                managerPid,
                new SpeciationAssignMembershipRequest(
                    new SpeciationAssignment(
                        brainId,
                        "species-one",
                        "Species One",
                        "policy-v1",
                        "manual_assign",
                        "{\"phase\":1}"),
                    DecisionTimeMs: 100));
            Assert.True(firstAssign.Success);
            Assert.True(firstAssign.Created);

            var reset = await system.Root.RequestAsync<SpeciationResetEpochResponse>(
                managerPid,
                new SpeciationResetEpochRequest(
                    PolicyVersion: "policy-v2",
                    ConfigSnapshotJson: "{\"phase\":2}",
                    ResetTimeMs: 200));
            Assert.Equal(firstEpoch.EpochId, reset.PreviousEpoch.EpochId);
            Assert.True(reset.CurrentEpoch.EpochId > firstEpoch.EpochId);

            var epochOneMemberships = await system.Root.RequestAsync<SpeciationListMembershipsResponse>(
                managerPid,
                new SpeciationListMembershipsRequest(firstEpoch.EpochId));
            Assert.Single(epochOneMemberships.Memberships);
            Assert.Equal("species-one", epochOneMemberships.Memberships[0].SpeciesId);

            var epochTwoMemberships = await system.Root.RequestAsync<SpeciationListMembershipsResponse>(
                managerPid,
                new SpeciationListMembershipsRequest(reset.CurrentEpoch.EpochId));
            Assert.Empty(epochTwoMemberships.Memberships);

            var secondAssign = await system.Root.RequestAsync<SpeciationAssignMembershipResponse>(
                managerPid,
                new SpeciationAssignMembershipRequest(
                    new SpeciationAssignment(
                        brainId,
                        "species-two",
                        "Species Two",
                        "policy-v2",
                        "manual_assign",
                        "{\"phase\":2}"),
                    DecisionTimeMs: 210));
            Assert.True(secondAssign.Success);
            Assert.True(secondAssign.Created);

            var immutableConflict = await system.Root.RequestAsync<SpeciationAssignMembershipResponse>(
                managerPid,
                new SpeciationAssignMembershipRequest(
                    new SpeciationAssignment(
                        brainId,
                        "species-three",
                        "Species Three",
                        "policy-v2",
                        "manual_reassign",
                        "{\"phase\":2-reassign}"),
                    DecisionTimeMs: 220));
            Assert.False(immutableConflict.Success);
            Assert.False(immutableConflict.Created);
            Assert.True(immutableConflict.ImmutableConflict);
            Assert.Equal("membership_immutable", immutableConflict.FailureReason);
            Assert.NotNull(immutableConflict.Membership);
            Assert.Equal("species-two", immutableConflict.Membership!.SpeciesId);

            var epochOneAfter = await system.Root.RequestAsync<SpeciationListMembershipsResponse>(
                managerPid,
                new SpeciationListMembershipsRequest(firstEpoch.EpochId));
            Assert.Single(epochOneAfter.Memberships);
            Assert.Equal("species-one", epochOneAfter.Memberships[0].SpeciesId);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoResetAll_ClearsHistoryAndStartsFreshEpoch()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig();
        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            var firstEpoch = await WaitForEpochAsync(system, managerPid);
            var brainId = Guid.NewGuid();

            var assign = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = brainId.ToProtoUuid()
                    },
                    SpeciesId = "species-one",
                    SpeciesDisplayName = "Species One",
                    DecisionReason = "seed"
                });
            Assert.True(assign.Decision.Success);

            var reset = await system.Root.RequestAsync<ProtoSpec.SpeciationResetAllResponse>(
                managerPid,
                new ProtoSpec.SpeciationResetAllRequest());

            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, reset.FailureReason);
            Assert.Equal(1UL, reset.CurrentEpoch.EpochId);
            Assert.True(reset.DeletedEpochCount >= 1);
            Assert.True(reset.DeletedMembershipCount >= 1);

            var oldEpochHistory = await system.Root.RequestAsync<ProtoSpec.SpeciationListHistoryResponse>(
                managerPid,
                new ProtoSpec.SpeciationListHistoryRequest
                {
                    HasEpochId = true,
                    EpochId = (ulong)firstEpoch.EpochId
                });
            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, oldEpochHistory.FailureReason);
            Assert.Empty(oldEpochHistory.History);
            Assert.Equal((uint)0, oldEpochHistory.TotalRecords);

            var currentMemberships = await system.Root.RequestAsync<ProtoSpec.SpeciationListMembershipsResponse>(
                managerPid,
                new ProtoSpec.SpeciationListMembershipsRequest
                {
                    HasEpochId = true,
                    EpochId = reset.CurrentEpoch.EpochId
                });
            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, currentMemberships.FailureReason);
            Assert.Empty(currentMemberships.Memberships);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoDeleteEpoch_DeletesHistoricalEpoch_AndRejectsCurrentEpoch()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig();
        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            var firstEpoch = await WaitForEpochAsync(system, managerPid);
            var setConfig = await system.Root.RequestAsync<ProtoSpec.SpeciationSetConfigResponse>(
                managerPid,
                new ProtoSpec.SpeciationSetConfigRequest
                {
                    Config = new ProtoSpec.SpeciationRuntimeConfig
                    {
                        PolicyVersion = "policy-v2",
                        ConfigSnapshotJson = "{\"mode\":\"v2\"}",
                        DefaultSpeciesId = runtimeConfig.DefaultSpeciesId,
                        DefaultSpeciesDisplayName = runtimeConfig.DefaultSpeciesDisplayName,
                        StartupReconcileDecisionReason = runtimeConfig.StartupReconcileDecisionReason
                    },
                    StartNewEpoch = true
                });
            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, setConfig.FailureReason);
            var secondEpochId = (long)setConfig.CurrentEpoch.EpochId;
            Assert.True(secondEpochId > firstEpoch.EpochId);

            var deleteFirst = await system.Root.RequestAsync<ProtoSpec.SpeciationDeleteEpochResponse>(
                managerPid,
                new ProtoSpec.SpeciationDeleteEpochRequest
                {
                    EpochId = (ulong)firstEpoch.EpochId
                });
            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, deleteFirst.FailureReason);
            Assert.True(deleteFirst.Deleted);
            Assert.Equal((ulong)firstEpoch.EpochId, deleteFirst.EpochId);

            var deletedEpochHistory = await system.Root.RequestAsync<ProtoSpec.SpeciationListHistoryResponse>(
                managerPid,
                new ProtoSpec.SpeciationListHistoryRequest
                {
                    HasEpochId = true,
                    EpochId = (ulong)firstEpoch.EpochId
                });
            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, deletedEpochHistory.FailureReason);
            Assert.Empty(deletedEpochHistory.History);
            Assert.Equal((uint)0, deletedEpochHistory.TotalRecords);

            var deleteCurrent = await system.Root.RequestAsync<ProtoSpec.SpeciationDeleteEpochResponse>(
                managerPid,
                new ProtoSpec.SpeciationDeleteEpochRequest
                {
                    EpochId = (ulong)secondEpochId
                });
            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureInvalidRequest, deleteCurrent.FailureReason);
            Assert.False(deleteCurrent.Deleted);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task Restart_ReloadsPersistedEpochAndMembershipState()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig();
        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            var firstEpoch = await WaitForEpochAsync(system, managerPid);
            var brainId = Guid.NewGuid();
            var firstAssign = await system.Root.RequestAsync<SpeciationAssignMembershipResponse>(
                managerPid,
                new SpeciationAssignMembershipRequest(
                    new SpeciationAssignment(
                        brainId,
                        "species-restart",
                        "Species Restart",
                        runtimeConfig.PolicyVersion,
                        "manual_assign",
                        "{\"phase\":\"before-restart\"}")));
            Assert.True(firstAssign.Success);

            system.Root.Stop(managerPid);
            await Task.Delay(100);

            var restartedPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            var restartedEpoch = await WaitForEpochAsync(system, restartedPid);
            Assert.Equal(firstEpoch.EpochId, restartedEpoch.EpochId);

            await WaitForConditionAsync(
                async () =>
                {
                    var memberships = await system.Root.RequestAsync<SpeciationListMembershipsResponse>(
                        restartedPid,
                        new SpeciationListMembershipsRequest(firstEpoch.EpochId));
                    return memberships.Memberships.Count == 1;
                },
                TimeSpan.FromSeconds(5));

            var restoredMemberships = await system.Root.RequestAsync<SpeciationListMembershipsResponse>(
                restartedPid,
                new SpeciationListMembershipsRequest(firstEpoch.EpochId));
            Assert.Single(restoredMemberships.Memberships);
            Assert.Equal(brainId, restoredMemberships.Memberships[0].BrainId);
            Assert.Equal("species-restart", restoredMemberships.Memberships[0].SpeciesId);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoEvaluate_DryRun_WithArtifactUri_DoesNotMutateMembership()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig();
        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            var epoch = await WaitForEpochAsync(system, managerPid);

            var response = await system.Root.RequestAsync<ProtoSpec.SpeciationEvaluateResponse>(
                managerPid,
                new ProtoSpec.SpeciationEvaluateRequest
                {
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactUri = "store://speciation/test/artifact-a"
                    },
                    SpeciesId = "species-eval",
                    SpeciesDisplayName = "Species Eval",
                    DecisionReason = "dry_run_eval",
                    DecisionMetadataJson = "{\"source\":\"proto-eval\"}",
                    PolicyVersion = runtimeConfig.PolicyVersion
                });

            Assert.NotNull(response.Decision);
            Assert.True(response.Decision.Success);
            Assert.False(response.Decision.Committed);
            Assert.Equal(ProtoSpec.SpeciationApplyMode.DryRun, response.Decision.ApplyMode);
            Assert.Equal(ProtoSpec.SpeciationCandidateMode.ArtifactUri, response.Decision.CandidateMode);
            Assert.Equal("species-eval", response.Decision.SpeciesId);

            var memberships = await system.Root.RequestAsync<ProtoSpec.SpeciationListMembershipsResponse>(
                managerPid,
                new ProtoSpec.SpeciationListMembershipsRequest
                {
                    HasEpochId = true,
                    EpochId = (ulong)epoch.EpochId
                });
            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, memberships.FailureReason);
            Assert.Empty(memberships.Memberships);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_BrainId_PersistsMembership_And_ReportsImmutableConflict()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig();
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var brainId = Guid.NewGuid();

            var firstAssign = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = brainId.ToProtoUuid()
                    },
                    SpeciesId = "species-commit",
                    SpeciesDisplayName = "Species Commit",
                    DecisionReason = "proto_assign",
                    DecisionMetadataJson = "{\"source\":\"proto-assign\"}",
                    PolicyVersion = runtimeConfig.PolicyVersion
                });

            Assert.True(firstAssign.Decision.Success);
            Assert.True(firstAssign.Decision.Created);
            Assert.True(firstAssign.Decision.Committed);
            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, firstAssign.Decision.FailureReason);
            Assert.NotNull(firstAssign.Decision.Membership);
            Assert.Equal("species-commit", firstAssign.Decision.Membership.SpeciesId);

            var query = await system.Root.RequestAsync<ProtoSpec.SpeciationQueryMembershipResponse>(
                managerPid,
                new ProtoSpec.SpeciationQueryMembershipRequest
                {
                    BrainId = brainId.ToProtoUuid()
                });
            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, query.FailureReason);
            Assert.True(query.Found);
            Assert.Equal("species-commit", query.Membership.SpeciesId);

            var secondAssign = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = brainId.ToProtoUuid()
                    },
                    SpeciesId = "species-other",
                    SpeciesDisplayName = "Species Other",
                    DecisionReason = "proto_reassign",
                    DecisionMetadataJson = "{\"source\":\"proto-reassign\"}",
                    PolicyVersion = runtimeConfig.PolicyVersion
                });

            Assert.False(secondAssign.Decision.Success);
            Assert.False(secondAssign.Decision.Created);
            Assert.False(secondAssign.Decision.Committed);
            Assert.True(secondAssign.Decision.ImmutableConflict);
            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureMembershipImmutable, secondAssign.Decision.FailureReason);

            var history = await system.Root.RequestAsync<ProtoSpec.SpeciationListHistoryResponse>(
                managerPid,
                new ProtoSpec.SpeciationListHistoryRequest
                {
                    HasBrainId = true,
                    BrainId = brainId.ToProtoUuid()
                });
            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, history.FailureReason);
            Assert.Equal((uint)1, history.TotalRecords);
            Assert.Single(history.History);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoListHistory_ReturnsPagedRowsWithOffsetInAssignmentOrder()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig();
        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            var epoch = await WaitForEpochAsync(system, managerPid);
            var brainIds = Enumerable.Range(0, 4).Select(_ => Guid.NewGuid()).ToArray();
            foreach (var brainId in brainIds)
            {
                var assign = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                    managerPid,
                    new ProtoSpec.SpeciationAssignRequest
                    {
                        ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                        Candidate = new ProtoSpec.SpeciationCandidateRef
                        {
                            BrainId = brainId.ToProtoUuid()
                        },
                        SpeciesId = "species-page",
                        SpeciesDisplayName = "Species Page",
                        DecisionReason = "proto_page_seed",
                        DecisionMetadataJson = "{\"source\":\"proto-page\"}",
                        PolicyVersion = runtimeConfig.PolicyVersion
                    });

                Assert.True(assign.Decision.Success);
                await Task.Delay(2);
            }

            var history = await system.Root.RequestAsync<ProtoSpec.SpeciationListHistoryResponse>(
                managerPid,
                new ProtoSpec.SpeciationListHistoryRequest
                {
                    HasEpochId = true,
                    EpochId = (ulong)epoch.EpochId,
                    Limit = 2,
                    Offset = 1
                });

            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, history.FailureReason);
            Assert.Equal((uint)4, history.TotalRecords);
            Assert.Equal(2, history.History.Count);
            Assert.True(history.History[0].BrainId.TryToGuid(out var secondBrainId));
            Assert.True(history.History[1].BrainId.TryToGuid(out var thirdBrainId));
            Assert.Equal(brainIds[1], secondBrainId);
            Assert.Equal(brainIds[2], thirdBrainId);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoBatch_Commit_RespectsPerItemApplyMode_And_CandidateValidation()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig();
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.97f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var brainId = Guid.NewGuid();
            var artifactRef = new string('a', 64).ToArtifactRef(123, "application/x-nbn", "artifact-store");

            var batch = await system.Root.RequestAsync<ProtoSpec.SpeciationBatchEvaluateApplyResponse>(
                managerPid,
                new ProtoSpec.SpeciationBatchEvaluateApplyRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Items =
                    {
                        new ProtoSpec.SpeciationBatchItem
                        {
                            ItemId = "brain-commit",
                            Candidate = new ProtoSpec.SpeciationCandidateRef
                            {
                                BrainId = brainId.ToProtoUuid()
                            },
                            SpeciesId = "species-batch",
                            SpeciesDisplayName = "Species Batch",
                            DecisionReason = "batch_commit"
                        },
                        new ProtoSpec.SpeciationBatchItem
                        {
                            ItemId = "artifact-commit",
                            Candidate = new ProtoSpec.SpeciationCandidateRef
                            {
                                ArtifactRef = artifactRef
                            },
                            SpeciesId = "species-artifact",
                            SpeciesDisplayName = "Species Artifact",
                            DecisionReason = "batch_commit_artifact"
                        },
                        new ProtoSpec.SpeciationBatchItem
                        {
                            ItemId = "artifact-dry-run",
                            HasApplyModeOverride = true,
                            ApplyModeOverride = ProtoSpec.SpeciationApplyMode.DryRun,
                            Candidate = new ProtoSpec.SpeciationCandidateRef
                            {
                                ArtifactUri = "store://speciation/test/artifact-b"
                            },
                            SpeciesId = "species-dry",
                            SpeciesDisplayName = "Species Dry",
                            DecisionReason = "batch_dry_run"
                        }
                    }
                });

            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, batch.FailureReason);
            Assert.Equal((uint)3, batch.RequestedCount);
            Assert.Equal((uint)3, batch.ProcessedCount);
            Assert.Equal((uint)2, batch.CommittedCount);
            Assert.Equal(3, batch.Results.Count);

            var committed = batch.Results.Single(item => item.ItemId == "brain-commit").Decision;
            Assert.True(committed.Success);
            Assert.True(committed.Committed);
            Assert.Equal(ProtoSpec.SpeciationCandidateMode.BrainId, committed.CandidateMode);

            var artifactCommitted = batch.Results.Single(item => item.ItemId == "artifact-commit").Decision;
            Assert.True(artifactCommitted.Success);
            Assert.True(artifactCommitted.Committed);
            Assert.Equal(ProtoSpec.SpeciationCandidateMode.ArtifactRef, artifactCommitted.CandidateMode);
            Assert.NotNull(artifactCommitted.Membership);
            Assert.False(string.IsNullOrWhiteSpace(artifactCommitted.Membership.SourceArtifactRef));

            var dryRun = batch.Results.Single(item => item.ItemId == "artifact-dry-run").Decision;
            Assert.True(dryRun.Success);
            Assert.False(dryRun.Committed);
            Assert.Equal(ProtoSpec.SpeciationApplyMode.DryRun, dryRun.ApplyMode);
            Assert.Equal(ProtoSpec.SpeciationCandidateMode.ArtifactUri, dryRun.CandidateMode);

            var query = await system.Root.RequestAsync<ProtoSpec.SpeciationQueryMembershipResponse>(
                managerPid,
                new ProtoSpec.SpeciationQueryMembershipRequest
                {
                    BrainId = brainId.ToProtoUuid()
                });
            Assert.True(query.Found);
            Assert.Equal("species-batch", query.Membership.SpeciesId);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_NoLineage_UsesRequestedSpeciesDisplayNameOnDefaultAssignment()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig();
        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            await WaitForEpochAsync(system, managerPid);
            var brainId = Guid.NewGuid();

            var assign = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = brainId.ToProtoUuid()
                    },
                    SpeciesDisplayName = "Seed Species",
                    DecisionReason = "proto_assign_named_seed"
                });

            Assert.True(assign.Decision.Success);
            Assert.True(assign.Decision.Created);
            Assert.True(assign.Decision.Committed);
            Assert.Equal(runtimeConfig.DefaultSpeciesId, assign.Decision.SpeciesId);
            Assert.Equal("Seed Species", assign.Decision.SpeciesDisplayName);
            Assert.Equal("lineage_unavailable_default", assign.Decision.DecisionReason);
            Assert.NotNull(assign.Decision.Membership);
            Assert.Equal(runtimeConfig.DefaultSpeciesId, assign.Decision.Membership.SpeciesId);
            Assert.Equal("Seed Species", assign.Decision.Membership.SpeciesDisplayName);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_ArtifactRef_PersistsMembership_And_IsImmutableWithinEpoch()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig();
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var artifactRef = new string('b', 64).ToArtifactRef(321, "application/x-nbn", "artifact-store");
            var equivalentArtifactRef = new string('b', 64).ToArtifactRef(321, "application/x-nbn", "artifact-store-copy");

            var firstAssign = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = artifactRef
                    },
                    SpeciesId = "species-artifact",
                    SpeciesDisplayName = "Species Artifact",
                    DecisionReason = "artifact_commit"
                });

            Assert.True(firstAssign.Decision.Success);
            Assert.True(firstAssign.Decision.Committed);
            Assert.Equal(ProtoSpec.SpeciationCandidateMode.ArtifactRef, firstAssign.Decision.CandidateMode);
            Assert.NotNull(firstAssign.Decision.Membership);
            Assert.False(string.IsNullOrWhiteSpace(firstAssign.Decision.Membership.SourceArtifactRef));
            using (var metadata = JsonDocument.Parse(firstAssign.Decision.DecisionMetadataJson))
            {
                var candidateArtifact = metadata.RootElement
                    .GetProperty("lineage")
                    .GetProperty("candidate_artifact_ref");
                Assert.Equal(new string('b', 64), candidateArtifact.GetProperty("sha256_hex").GetString());
                Assert.Equal((ulong)321, candidateArtifact.GetProperty("size_bytes").GetUInt64());
                Assert.Equal("application/x-nbn", candidateArtifact.GetProperty("media_type").GetString());
                Assert.Equal("artifact-store", candidateArtifact.GetProperty("store_uri").GetString());
            }

            var secondAssign = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = equivalentArtifactRef
                    },
                    SpeciesId = "species-other",
                    SpeciesDisplayName = "Species Other",
                    DecisionReason = "artifact_reassign"
                });

            Assert.False(secondAssign.Decision.Success);
            Assert.False(secondAssign.Decision.Committed);
            Assert.True(secondAssign.Decision.ImmutableConflict);
            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureMembershipImmutable, secondAssign.Decision.FailureReason);
            Assert.Equal(ProtoSpec.SpeciationCandidateMode.ArtifactRef, secondAssign.Decision.CandidateMode);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_AutoLineageAssignment_PersistsProvenanceAndLineageEdges()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.80d,
            lineageSplitThreshold: 0.40d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch"));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var parentA = Guid.NewGuid();
            var parentB = Guid.NewGuid();
            var child = Guid.NewGuid();

            foreach (var parent in new[] { parentA, parentB })
            {
                var seededParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                    managerPid,
                    new ProtoSpec.SpeciationAssignRequest
                    {
                        ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                        Candidate = new ProtoSpec.SpeciationCandidateRef
                        {
                            BrainId = parent.ToProtoUuid()
                        },
                        SpeciesId = "species-alpha",
                        SpeciesDisplayName = "Species Alpha",
                        DecisionReason = "seed_parent_species",
                        DecisionMetadataJson = "{\"source\":\"seed\"}"
                    });
                Assert.True(seededParent.Decision.Success);
                Assert.True(seededParent.Decision.Created);
            }

            var assign = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = child.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parentA.ToProtoUuid() },
                        new ProtoSpec.SpeciationParentRef { BrainId = parentB.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"report\":{\"similarity_score\":0.93,\"function_score\":0.81,\"connectivity_score\":0.79}}"
                });

            Assert.True(assign.Decision.Success);
            Assert.True(assign.Decision.Created);
            Assert.True(assign.Decision.Committed);
            Assert.Equal("species-alpha", assign.Decision.SpeciesId);
            Assert.Equal("lineage_inherit_similarity_match", assign.Decision.DecisionReason);

            using var metadata = JsonDocument.Parse(assign.Decision.DecisionMetadataJson);
            Assert.Equal("lineage_inherit", metadata.RootElement.GetProperty("assignment_strategy").GetString());
            Assert.Equal("policy-v1", metadata.RootElement.GetProperty("policy_version").GetString());
            Assert.Equal(0.93d, metadata.RootElement.GetProperty("scores").GetProperty("similarity_score").GetDouble(), 3);

            var status = await system.Root.RequestAsync<ProtoSpec.SpeciationStatusResponse>(
                managerPid,
                new ProtoSpec.SpeciationStatusRequest());
            Assert.Equal((uint)2, status.Status.LineageEdgeCount);

            var history = await system.Root.RequestAsync<ProtoSpec.SpeciationListHistoryResponse>(
                managerPid,
                new ProtoSpec.SpeciationListHistoryRequest
                {
                    HasBrainId = true,
                    BrainId = child.ToProtoUuid()
                });
            Assert.Equal((uint)1, history.TotalRecords);
            Assert.Single(history.History);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_ArtifactParents_ResolveLineageMembershipEvidence()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.80d,
            lineageSplitThreshold: 0.40d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch"));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);

            var parentARef = new string('a', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var parentBRef = new string('b', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var childRef = new string('c', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");

            foreach (var parentRef in new[] { parentARef, parentBRef })
            {
                var seededParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                    managerPid,
                    new ProtoSpec.SpeciationAssignRequest
                    {
                        ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                        Candidate = new ProtoSpec.SpeciationCandidateRef
                        {
                            ArtifactRef = parentRef
                        },
                        SpeciesId = "species-alpha",
                        SpeciesDisplayName = "Species Alpha",
                        DecisionReason = "seed_parent_species",
                        DecisionMetadataJson = "{\"source\":\"seed\"}"
                    });
                Assert.True(seededParent.Decision.Success);
                Assert.True(seededParent.Decision.Created);
            }

            var assign = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = childRef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentARef },
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentBRef }
                    },
                    DecisionMetadataJson = "{\"report\":{\"similarity_score\":0.92}}"
                });

            Assert.True(assign.Decision.Success);
            Assert.True(assign.Decision.Created);
            Assert.Equal("species-alpha", assign.Decision.SpeciesId);
            Assert.Equal("lineage_inherit_similarity_match", assign.Decision.DecisionReason);

            using var metadata = JsonDocument.Parse(assign.Decision.DecisionMetadataJson);
            Assert.Equal(2, metadata.RootElement.GetProperty("lineage").GetProperty("parent_membership_count").GetInt32());

            var status = await system.Root.RequestAsync<ProtoSpec.SpeciationStatusResponse>(
                managerPid,
                new ProtoSpec.SpeciationStatusRequest());
            Assert.Equal((uint)2, status.Status.LineageEdgeCount);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_ArtifactParents_ResolveLineageMembershipEvidenceAcrossEquivalentExports()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.80d,
            lineageSplitThreshold: 0.40d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch"));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);

            var parentASeedRef = new string('a', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store-a");
            var parentBSeedRef = new string('b', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store-b");
            var parentAEquivalentRef = new string('a', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store-a-copy");
            var parentBEquivalentRef = new string('b', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store-b-copy");
            var childRef = new string('c', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store-child");

            foreach (var parentRef in new[] { parentASeedRef, parentBSeedRef })
            {
                var seededParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                    managerPid,
                    new ProtoSpec.SpeciationAssignRequest
                    {
                        ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                        Candidate = new ProtoSpec.SpeciationCandidateRef
                        {
                            ArtifactRef = parentRef
                        },
                        SpeciesId = "species-alpha",
                        SpeciesDisplayName = "Species Alpha",
                        DecisionReason = "seed_parent_species",
                        DecisionMetadataJson = "{\"source\":\"seed\"}"
                    });
                Assert.True(seededParent.Decision.Success);
                Assert.True(seededParent.Decision.Created);
            }

            var assign = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = childRef
                    },
                    SpeciesDisplayName = "Ignored No-Lineage Hint",
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentAEquivalentRef },
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentBEquivalentRef }
                    },
                    DecisionMetadataJson = "{\"report\":{\"similarity_score\":0.92}}"
                });

            Assert.True(assign.Decision.Success);
            Assert.True(assign.Decision.Created);
            Assert.Equal("species-alpha", assign.Decision.SpeciesId);
            Assert.Equal("Species Alpha", assign.Decision.SpeciesDisplayName);
            Assert.Equal("lineage_inherit_similarity_match", assign.Decision.DecisionReason);

            using var metadata = JsonDocument.Parse(assign.Decision.DecisionMetadataJson);
            Assert.Equal(2, metadata.RootElement.GetProperty("lineage").GetProperty("parent_membership_count").GetInt32());
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_HysteresisBand_ReusesPriorLineageSpecies()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.80d,
            lineageSplitThreshold: 0.40d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch"));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var parentA = Guid.NewGuid();
            var parentB = Guid.NewGuid();
            var childSeed = Guid.NewGuid();
            var childBand = Guid.NewGuid();

            var parentBSeed = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parentB.ToProtoUuid()
                    },
                    SpeciesId = "species-beta",
                    SpeciesDisplayName = "Species Beta",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 10
                });
            Assert.True(parentBSeed.Decision.Success);

            var parentASeed = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parentA.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 20
                });
            Assert.True(parentASeed.Decision.Success);

            var seedDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = childSeed.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parentA.ToProtoUuid() },
                        new ProtoSpec.SpeciationParentRef { BrainId = parentB.ToProtoUuid() }
                    },
                    DecisionMetadataJson =
                        "{\"lineage\":{\"lineage_similarity_score\":0.60,\"parent_a_similarity_score\":0.60,\"parent_b_similarity_score\":0.30}}"
                });
            Assert.True(seedDecision.Decision.Success);
            Assert.Equal("species-alpha", seedDecision.Decision.SpeciesId);

            var hysteresisDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = childBand.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parentB.ToProtoUuid() },
                        new ProtoSpec.SpeciationParentRef { BrainId = parentA.ToProtoUuid() }
                    },
                    DecisionMetadataJson =
                        "{\"lineage\":{\"lineage_similarity_score\":0.58,\"parent_a_similarity_score\":0.30,\"parent_b_similarity_score\":0.58}}"
                });

            Assert.True(hysteresisDecision.Decision.Success);
            Assert.True(hysteresisDecision.Decision.Created);
            Assert.Equal("species-alpha", hysteresisDecision.Decision.SpeciesId);
            Assert.Equal("lineage_hysteresis_hold", hysteresisDecision.Decision.DecisionReason);

            using var metadata = JsonDocument.Parse(hysteresisDecision.Decision.DecisionMetadataJson);
            Assert.Equal("hysteresis_hold", metadata.RootElement.GetProperty("assignment_strategy").GetString());
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.Equal("species-alpha", lineage.GetProperty("source_species_id").GetString());
            Assert.Equal(0.58d, lineage.GetProperty("source_species_similarity_score").GetDouble(), 3);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_LowSimilarity_CreatesDeterministicDerivedSpecies()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.80d,
            lineageSplitThreshold: 0.30d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch"));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var parent = Guid.NewGuid();
            var childA = Guid.NewGuid();
            var childB = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parent.ToProtoUuid()
                    },
                    SpeciesId = "species-gamma",
                    SpeciesDisplayName = "Species Gamma",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var firstChild = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = childA.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"report\":{\"similarity_score\":0.10}}"
                });

            var secondChild = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = childB.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"report\":{\"similarity_score\":0.12}}"
                });

            Assert.True(firstChild.Decision.Success);
            Assert.True(secondChild.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", firstChild.Decision.DecisionReason);
            Assert.Equal("lineage_diverged_new_species", secondChild.Decision.DecisionReason);
            Assert.StartsWith("species-gamma-branch-", firstChild.Decision.SpeciesId, StringComparison.Ordinal);
            Assert.Equal(firstChild.Decision.SpeciesId, secondChild.Decision.SpeciesId);
            Assert.Matches("^Species Gamma \\[[A-Z]+\\]$", firstChild.Decision.SpeciesDisplayName);
            Assert.Equal(firstChild.Decision.SpeciesDisplayName, secondChild.Decision.SpeciesDisplayName);
            using var metadata = JsonDocument.Parse(secondChild.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.Equal("compatibility_assessment", lineage.GetProperty("assigned_species_similarity_source").GetString());
            Assert.Equal(0.96d, lineage.GetProperty("lineage_assignment_similarity_score").GetDouble(), 3);
            Assert.Equal(0.96d, lineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_DefaultLineagePolicy_UsesConservativeThresholdsAndAutoDisplayNames()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig("{}");
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var parentA = Guid.NewGuid();
            var parentB = Guid.NewGuid();
            var parentC = Guid.NewGuid();
            var child = Guid.NewGuid();

            var seedParentA = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parentA.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParentA.Decision.Success);
            Assert.Equal("Species Alpha", seedParentA.Decision.SpeciesDisplayName);

            var seedParentB = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parentB.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParentB.Decision.Success);

            var seedParentC = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parentC.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParentC.Decision.Success);

            var childDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = child.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parentA.ToProtoUuid() },
                        new ProtoSpec.SpeciationParentRef { BrainId = parentB.ToProtoUuid() },
                        new ProtoSpec.SpeciationParentRef { BrainId = parentC.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"report\":{\"similarity_score\":0.40}}"
                });

            Assert.True(childDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", childDecision.Decision.DecisionReason);
            Assert.Matches("^Species Alpha \\[[A-Z]+\\]$", childDecision.Decision.SpeciesDisplayName);

            using var metadata = JsonDocument.Parse(childDecision.Decision.DecisionMetadataJson);
            var policy = metadata.RootElement.GetProperty("assignment_policy");
            Assert.Equal(0.92d, policy.GetProperty("lineage_match_threshold").GetDouble(), 3);
            Assert.Equal(0.88d, policy.GetProperty("lineage_split_threshold").GetDouble(), 3);
            Assert.Equal(0.86d, policy.GetProperty("lineage_effective_split_threshold").GetDouble(), 3);
            Assert.Equal(0.02d, policy.GetProperty("lineage_split_guard_margin").GetDouble(), 3);
            Assert.Equal(1, policy.GetProperty("lineage_min_parent_memberships_before_split").GetInt32());
            Assert.Equal(0.70d, policy.GetProperty("parent_consensus_threshold").GetDouble(), 3);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_SplitGuard_MinParentMembershipsBlocksEarlySplit()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.80d,
            lineageSplitThreshold: 0.30d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 2));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var parent = Guid.NewGuid();
            var child = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parent.ToProtoUuid()
                    },
                    SpeciesId = "species-gamma",
                    SpeciesDisplayName = "Species Gamma",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var decision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = child.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"report\":{\"similarity_score\":0.10}}"
                });

            Assert.True(decision.Decision.Success);
            Assert.True(decision.Decision.Created);
            Assert.Equal("species-gamma", decision.Decision.SpeciesId);
            Assert.Equal("lineage_split_guarded_parent_evidence", decision.Decision.DecisionReason);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_RecentSplitRealignWindow_ReusesDerivedSpeciesNearMatchThreshold()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.80d,
            lineageSplitThreshold: 0.30d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 3,
            lineageRealignMatchMargin: 0.08d));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var parent = Guid.NewGuid();
            var childSplit = Guid.NewGuid();
            var childRealign = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parent.ToProtoUuid()
                    },
                    SpeciesId = "species-gamma",
                    SpeciesDisplayName = "Species Gamma",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var firstChild = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = childSplit.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"report\":{\"similarity_score\":0.10}}"
                });
            Assert.True(firstChild.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", firstChild.Decision.DecisionReason);
            Assert.StartsWith("species-gamma-branch-", firstChild.Decision.SpeciesId, StringComparison.Ordinal);

            var secondChild = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = childRealign.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"report\":{\"similarity_score\":0.84}}"
                });

            Assert.True(secondChild.Decision.Success);
            Assert.True(secondChild.Decision.Created);
            Assert.Equal(firstChild.Decision.SpeciesId, secondChild.Decision.SpeciesId);
            Assert.Equal("lineage_realign_recent_split", secondChild.Decision.DecisionReason);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_SecondSeedFounderCanSplitFromFirstFounderUsingPairwiseSeedSimilarity()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.92d,
            lineageSplitThreshold: 0.88d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0.04d,
            lineageSplitGuardMargin: 0.02d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            await WaitForEpochAsync(system, managerPid);

            var founderA = Guid.NewGuid();
            var founderB = Guid.NewGuid();
            const string founderSeedMetadata =
                "{\"lineage\":{\"lineage_similarity_score\":0.24,\"parent_a_similarity_score\":1.0,\"parent_b_similarity_score\":0.24}}";

            var firstFounder = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = founderA.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = founderA.ToProtoUuid() },
                        new ProtoSpec.SpeciationParentRef { BrainId = founderB.ToProtoUuid() }
                    },
                    DecisionMetadataJson = founderSeedMetadata
                });

            Assert.True(firstFounder.Decision.Success);
            Assert.Equal("lineage_unavailable_default", firstFounder.Decision.DecisionReason);
            Assert.Equal("default-species", firstFounder.Decision.SpeciesId);

            var secondFounder = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = founderB.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = founderB.ToProtoUuid() },
                        new ProtoSpec.SpeciationParentRef { BrainId = founderA.ToProtoUuid() }
                    },
                    DecisionMetadataJson = founderSeedMetadata
                });

            Assert.True(secondFounder.Decision.Success);
            Assert.Equal("lineage_diverged_founder_root_species", secondFounder.Decision.DecisionReason);
            Assert.NotEqual(firstFounder.Decision.SpeciesId, secondFounder.Decision.SpeciesId);
            var rootDisplayStem = firstFounder.Decision.SpeciesDisplayName;
            Assert.Equal(rootDisplayStem + "-2", secondFounder.Decision.SpeciesDisplayName);

            var memberships = await system.Root.RequestAsync<ProtoSpec.SpeciationListMembershipsResponse>(
                managerPid,
                new ProtoSpec.SpeciationListMembershipsRequest());
            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, memberships.FailureReason);
            var firstFounderMembership = Assert.Single(
                memberships.Memberships,
                record => record.BrainId.ToGuid() == founderA);
            var secondFounderMembership = Assert.Single(
                memberships.Memberships,
                record => record.BrainId.ToGuid() == founderB);
            Assert.Equal(rootDisplayStem + "-1", firstFounderMembership.SpeciesDisplayName);
            Assert.Equal(rootDisplayStem + "-2", secondFounderMembership.SpeciesDisplayName);

            using var metadata = JsonDocument.Parse(secondFounder.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.Equal(string.Empty, lineage.GetProperty("source_species_id").GetString());
            Assert.Equal(0.24d, lineage.GetProperty("source_species_similarity_score").GetDouble(), 3);
            Assert.Equal(1, lineage.GetProperty("parent_membership_count").GetInt32());

            var founderChild = Guid.NewGuid();
            var founderChildDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = founderChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = founderB.ToProtoUuid() }
                    },
                    DecisionMetadataJson =
                        "{\"lineage\":{\"lineage_similarity_score\":0.95,\"parent_a_similarity_score\":0.95}}"
                });

            Assert.True(founderChildDecision.Decision.Success);
            Assert.Equal(secondFounder.Decision.SpeciesId, founderChildDecision.Decision.SpeciesId);
            Assert.Equal("lineage_inherit_similarity_match", founderChildDecision.Decision.DecisionReason);

            static string ExtractLineageCode(string displayName)
            {
                var closeIndex = displayName.LastIndexOf(']');
                var openIndex = displayName.LastIndexOf('[');
                Assert.True(openIndex >= 0 && closeIndex > openIndex);
                return displayName[(openIndex + 1)..closeIndex];
            }

            var firstRootDerivedBrain = Guid.NewGuid();
            var firstRootDerivedDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = firstRootDerivedBrain.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = founderA.ToProtoUuid() }
                    },
                    DecisionMetadataJson =
                        "{\"lineage\":{\"lineage_similarity_score\":0.24,\"parent_a_similarity_score\":0.24}}"
                });
            var secondRootDerivedBrain = Guid.NewGuid();
            var secondRootDerivedDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = secondRootDerivedBrain.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = founderB.ToProtoUuid() }
                    },
                    DecisionMetadataJson =
                        "{\"lineage\":{\"lineage_similarity_score\":0.24,\"parent_a_similarity_score\":0.24}}"
                });

            Assert.Equal("lineage_diverged_new_species", firstRootDerivedDecision.Decision.DecisionReason);
            Assert.Equal("lineage_diverged_new_species", secondRootDerivedDecision.Decision.DecisionReason);
            Assert.StartsWith(rootDisplayStem + "-1 [", firstRootDerivedDecision.Decision.SpeciesDisplayName, StringComparison.Ordinal);
            Assert.StartsWith(rootDisplayStem + "-2 [", secondRootDerivedDecision.Decision.SpeciesDisplayName, StringComparison.Ordinal);
            var firstRootLineageCode = ExtractLineageCode(firstRootDerivedDecision.Decision.SpeciesDisplayName);
            var secondRootLineageCode = ExtractLineageCode(secondRootDerivedDecision.Decision.SpeciesDisplayName);
            Assert.True(firstRootLineageCode.Length >= 2);
            Assert.True(secondRootLineageCode.Length >= 2);
            Assert.StartsWith("A", firstRootLineageCode, StringComparison.Ordinal);
            Assert.StartsWith("B", secondRootLineageCode, StringComparison.Ordinal);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public void BuildRootSpeciesLineagePrefix_RemainsUniqueBeyondTwentySixRoots()
    {
        var method = typeof(SpeciationManagerActor).GetMethod(
            "BuildRootSpeciesLineagePrefix",
            BindingFlags.Static | BindingFlags.NonPublic);
        Assert.NotNull(method);

        var prefixes = new[]
        {
            (Ordinal: 1, Expected: "A"),
            (Ordinal: 26, Expected: "Z"),
            (Ordinal: 27, Expected: "AA"),
            (Ordinal: 28, Expected: "AB"),
            (Ordinal: 52, Expected: "AZ"),
            (Ordinal: 53, Expected: "BA")
        };

        foreach (var (ordinal, expected) in prefixes)
        {
            var actual = Assert.IsType<string>(method!.Invoke(null, [ordinal]));
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_SecondSeedArtifactFounderCreatesIndependentRootSpecies()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.92d,
            lineageSplitThreshold: 0.88d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0.04d,
            lineageSplitGuardMargin: 0.02d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            await WaitForEpochAsync(system, managerPid);

            var founderARef = new string('a', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store-a");
            var founderBRef = new string('b', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store-b");
            var founderChildRef = new string('c', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store-c");
            const string founderSeedMetadata =
                "{\"lineage\":{\"lineage_similarity_score\":0.24,\"parent_a_similarity_score\":1.0,\"parent_b_similarity_score\":0.24}}";

            var firstFounder = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = founderARef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = founderARef },
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = founderBRef }
                    },
                    DecisionMetadataJson = founderSeedMetadata
                });

            Assert.True(firstFounder.Decision.Success);
            Assert.Equal("lineage_unavailable_default", firstFounder.Decision.DecisionReason);

            var secondFounder = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = founderBRef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = founderBRef },
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = founderARef }
                    },
                    DecisionMetadataJson = founderSeedMetadata
                });

            Assert.True(secondFounder.Decision.Success);
            Assert.Equal("lineage_diverged_founder_root_species", secondFounder.Decision.DecisionReason);
            Assert.NotEqual(firstFounder.Decision.SpeciesId, secondFounder.Decision.SpeciesId);
            var rootDisplayStem = firstFounder.Decision.SpeciesDisplayName;
            Assert.Equal(rootDisplayStem + "-2", secondFounder.Decision.SpeciesDisplayName);

            var memberships = await system.Root.RequestAsync<ProtoSpec.SpeciationListMembershipsResponse>(
                managerPid,
                new ProtoSpec.SpeciationListMembershipsRequest());
            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, memberships.FailureReason);
            var firstFounderMembership = Assert.Single(
                memberships.Memberships,
                record => string.Equals(record.SpeciesId, firstFounder.Decision.SpeciesId, StringComparison.Ordinal));
            var secondFounderMembership = Assert.Single(
                memberships.Memberships,
                record => string.Equals(record.SpeciesId, secondFounder.Decision.SpeciesId, StringComparison.Ordinal));
            Assert.Equal(rootDisplayStem + "-1", firstFounderMembership.SpeciesDisplayName);
            Assert.Equal(rootDisplayStem + "-2", secondFounderMembership.SpeciesDisplayName);

            using (var founderMetadata = JsonDocument.Parse(secondFounder.Decision.DecisionMetadataJson))
            {
                var lineage = founderMetadata.RootElement.GetProperty("lineage");
                Assert.Equal(string.Empty, lineage.GetProperty("source_species_id").GetString());
                Assert.Equal(0.24d, lineage.GetProperty("source_species_similarity_score").GetDouble(), 3);
            }

            var founderChild = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = founderChildRef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = founderBRef }
                    },
                    DecisionMetadataJson =
                        "{\"lineage\":{\"lineage_similarity_score\":0.95,\"parent_a_similarity_score\":0.95}}"
                });

            Assert.True(founderChild.Decision.Success);
            Assert.Equal(secondFounder.Decision.SpeciesId, founderChild.Decision.SpeciesId);
            Assert.Equal("lineage_inherit_similarity_match", founderChild.Decision.DecisionReason);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_LineageSimilarityScore_TakesPrecedenceOverGenericSimilarity()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.80d,
            lineageSplitThreshold: 0.30d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch"));
        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            await WaitForEpochAsync(system, managerPid);
            var parent = Guid.NewGuid();
            var child = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parent.ToProtoUuid()
                    },
                    SpeciesId = "species-gamma",
                    SpeciesDisplayName = "Species Gamma",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var decision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = child.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.10},\"report\":{\"similarity_score\":0.95}}"
                });

            Assert.True(decision.Decision.Success);
            Assert.True(decision.Decision.Created);
            Assert.Equal("lineage_diverged_new_species", decision.Decision.DecisionReason);
            Assert.StartsWith("species-gamma-branch-", decision.Decision.SpeciesId, StringComparison.Ordinal);
            using var metadata = JsonDocument.Parse(decision.Decision.DecisionMetadataJson);
            Assert.Equal(
                0.10d,
                metadata.RootElement.GetProperty("lineage").GetProperty("lineage_similarity_score").GetDouble(),
                3);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_DynamicSpeciesFloor_SplitsNearThresholdWhenIntraSpeciesFloorIsHigher()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.95d,
            lineageSplitThreshold: 0.60d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0.04d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1));
        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            await WaitForEpochAsync(system, managerPid);
            var parent = Guid.NewGuid();
            var childInSpecies = Guid.NewGuid();
            var childNearThreshold = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var floorSeedDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = childInSpecies.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.90}}"
                });
            Assert.True(floorSeedDecision.Decision.Success);
            Assert.Equal("species-alpha", floorSeedDecision.Decision.SpeciesId);

            var splitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = childNearThreshold.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.83}}"
                });

            Assert.True(splitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", splitDecision.Decision.DecisionReason);
            Assert.StartsWith("species-alpha-branch-", splitDecision.Decision.SpeciesId, StringComparison.Ordinal);

            using var metadata = JsonDocument.Parse(splitDecision.Decision.DecisionMetadataJson);
            var policy = metadata.RootElement.GetProperty("assignment_policy");
            Assert.Equal("species_floor", policy.GetProperty("lineage_source_split_threshold_source").GetString());
            Assert.Equal(0.60d, policy.GetProperty("lineage_source_policy_effective_split_threshold").GetDouble(), 3);
            Assert.Equal(0.86d, policy.GetProperty("lineage_source_dynamic_split_threshold").GetDouble(), 3);
            Assert.Equal(0.90d, policy.GetProperty("lineage_source_species_floor_similarity_score").GetDouble(), 3);

            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.True(lineage.TryGetProperty("source_split_proximity_to_dynamic_threshold", out var dynamicProximity));
            Assert.True(dynamicProximity.GetDouble() < 0d);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_RecentSplitRealign_PrecedesDynamicSpeciesFloorSplit()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.95d,
            lineageSplitThreshold: 0.60d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0.04d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 3,
            lineageRealignMatchMargin: 0.05d));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var parent = Guid.NewGuid();
            var childInSpecies = Guid.NewGuid();
            var splitChild = Guid.NewGuid();
            var realignChild = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var floorSeedDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = childInSpecies.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.90}}"
                });
            Assert.True(floorSeedDecision.Decision.Success);
            Assert.Equal("species-alpha", floorSeedDecision.Decision.SpeciesId);

            var splitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = splitChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.83}}"
                });
            Assert.True(splitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", splitDecision.Decision.DecisionReason);

            var realignDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = realignChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.84}}"
                });

            Assert.True(realignDecision.Decision.Success);
            Assert.Equal(splitDecision.Decision.SpeciesId, realignDecision.Decision.SpeciesId);
            Assert.Equal("lineage_realign_recent_split", realignDecision.Decision.DecisionReason);
            using var metadata = JsonDocument.Parse(realignDecision.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.Equal("compatibility_assessment", lineage.GetProperty("assigned_species_similarity_source").GetString());
            Assert.Equal("brain_ids", lineage.GetProperty("assigned_species_compatibility_mode").GetString());
            Assert.Equal(0.96d, lineage.GetProperty("lineage_assignment_similarity_score").GetDouble(), 3);
            Assert.Equal(0.96d, lineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
            Assert.True(lineage.GetProperty("split_proximity_to_dynamic_threshold").GetDouble() > 0d);
            Assert.True(lineage.GetProperty("source_split_proximity_to_dynamic_threshold").GetDouble() < 0d);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_RecentSplitRealign_FallsBackWhenCompatibilityDoesNotBeatSourceSpecies()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.80d,
            lineageSplitThreshold: 0.30d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 3,
            lineageRealignMatchMargin: 0.08d));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.70f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var parent = Guid.NewGuid();
            var splitChild = Guid.NewGuid();
            var realignChild = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parent.ToProtoUuid()
                    },
                    SpeciesId = "species-gamma",
                    SpeciesDisplayName = "Species Gamma",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var splitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = splitChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"report\":{\"similarity_score\":0.10}}"
                });
            Assert.True(splitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", splitDecision.Decision.DecisionReason);

            var realignDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = realignChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"report\":{\"similarity_score\":0.84}}"
                });

            Assert.True(realignDecision.Decision.Success);
            Assert.Equal("species-gamma", realignDecision.Decision.SpeciesId);
            Assert.Equal("lineage_bootstrap_compatibility_required", realignDecision.Decision.DecisionReason);
            Assert.NotEqual(splitDecision.Decision.SpeciesId, realignDecision.Decision.SpeciesId);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_DynamicSpeciesFloor_RebuildsFromPersistedHistoryOnStartup()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.95d,
            lineageSplitThreshold: 0.60d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0.04d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1));

        var parent = Guid.NewGuid();
        var firstChild = Guid.NewGuid();

        var firstSystem = new ActorSystem();
        try
        {
            var firstManagerPid = firstSystem.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            await WaitForEpochAsync(firstSystem, firstManagerPid);
            var seedParent = await firstSystem.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                firstManagerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var floorSeedDecision = await firstSystem.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                firstManagerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = firstChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.90}}"
                });
            Assert.True(floorSeedDecision.Decision.Success);
            Assert.Equal("species-alpha", floorSeedDecision.Decision.SpeciesId);
        }
        finally
        {
            await firstSystem.ShutdownAsync();
        }

        var secondSystem = new ActorSystem();
        try
        {
            var secondManagerPid = secondSystem.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            await WaitForEpochAsync(secondSystem, secondManagerPid);
            var postRestartChild = Guid.NewGuid();
            var splitDecision = await secondSystem.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                secondManagerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = postRestartChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.83}}"
                });

            Assert.True(splitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", splitDecision.Decision.DecisionReason);
            using var metadata = JsonDocument.Parse(splitDecision.Decision.DecisionMetadataJson);
            var policy = metadata.RootElement.GetProperty("assignment_policy");
            Assert.Equal("species_floor", policy.GetProperty("lineage_source_split_threshold_source").GetString());
            Assert.Equal(0.86d, policy.GetProperty("lineage_source_dynamic_split_threshold").GetDouble(), 3);
        }
        finally
        {
            await secondSystem.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_MixedParentPairwiseSimilarity_DrivesAssignmentAndSpeciesFloor()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.80d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0.04d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var parentBeta = Guid.NewGuid();
            var parentAlpha = Guid.NewGuid();
            var mixedChild = Guid.NewGuid();
            var nearFloorChild = Guid.NewGuid();

            var seedBeta = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parentBeta.ToProtoUuid()
                    },
                    SpeciesId = "species-beta",
                    SpeciesDisplayName = "Species Beta",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 10
                });
            Assert.True(seedBeta.Decision.Success);

            var seedAlpha = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parentAlpha.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 20
                });
            Assert.True(seedAlpha.Decision.Success);

            var mixedDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = mixedChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parentAlpha.ToProtoUuid() },
                        new ProtoSpec.SpeciationParentRef { BrainId = parentBeta.ToProtoUuid() }
                    },
                    DecisionMetadataJson =
                        "{\"lineage\":{\"lineage_similarity_score\":0.82,\"parent_a_similarity_score\":0.94,\"parent_b_similarity_score\":0.60}}"
                });

            Assert.True(mixedDecision.Decision.Success);
            Assert.True(mixedDecision.Decision.Created);
            Assert.Equal("species-alpha", mixedDecision.Decision.SpeciesId);
            Assert.Equal("lineage_inherit_similarity_match", mixedDecision.Decision.DecisionReason);

            using (var mixedMetadata = JsonDocument.Parse(mixedDecision.Decision.DecisionMetadataJson))
            {
                var lineage = mixedMetadata.RootElement.GetProperty("lineage");
                Assert.Equal("species-alpha", lineage.GetProperty("source_species_id").GetString());
                Assert.Equal(0.94d, lineage.GetProperty("dominant_species_similarity_score").GetDouble(), 3);
                Assert.Equal(0.94d, lineage.GetProperty("source_species_similarity_score").GetDouble(), 3);
                Assert.Equal(0.94d, lineage.GetProperty("lineage_assignment_similarity_score").GetDouble(), 3);
                Assert.Equal(0.94d, lineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
                Assert.Equal("species-alpha", lineage.GetProperty("intra_species_similarity_species_id").GetString());
            }

            var splitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = nearFloorChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parentAlpha.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.87,\"parent_a_similarity_score\":0.87}}"
                });

            Assert.True(splitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", splitDecision.Decision.DecisionReason);
            Assert.StartsWith("species-alpha-branch-", splitDecision.Decision.SpeciesId, StringComparison.Ordinal);
            using var splitMetadata = JsonDocument.Parse(splitDecision.Decision.DecisionMetadataJson);
            var policy = splitMetadata.RootElement.GetProperty("assignment_policy");
            Assert.Equal("species_floor", policy.GetProperty("lineage_source_split_threshold_source").GetString());
            Assert.Equal(0.90d, policy.GetProperty("lineage_source_dynamic_split_threshold").GetDouble(), 3);
            Assert.Equal(0.94d, policy.GetProperty("lineage_source_species_floor_similarity_score").GetDouble(), 3);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_MixedParentSplit_RootsDerivedSpeciesFromBestFitSourceSpecies()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.80d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch"));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var parentAlpha = Guid.NewGuid();
            var parentBeta = Guid.NewGuid();
            var mixedSplitChild = Guid.NewGuid();

            var seedAlpha = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parentAlpha.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 10
                });
            Assert.True(seedAlpha.Decision.Success);

            var seedBeta = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parentBeta.ToProtoUuid()
                    },
                    SpeciesId = "species-beta",
                    SpeciesDisplayName = "Species Beta",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 20
                });
            Assert.True(seedBeta.Decision.Success);

            var splitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = mixedSplitChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parentAlpha.ToProtoUuid() },
                        new ProtoSpec.SpeciationParentRef { BrainId = parentBeta.ToProtoUuid() }
                    },
                    DecisionMetadataJson =
                        "{\"lineage\":{\"lineage_similarity_score\":0.45,\"parent_a_similarity_score\":0.78,\"parent_b_similarity_score\":0.30}}"
                });

            Assert.True(splitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", splitDecision.Decision.DecisionReason);
            Assert.StartsWith("species-alpha-branch-", splitDecision.Decision.SpeciesId, StringComparison.Ordinal);

            using var metadata = JsonDocument.Parse(splitDecision.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.Equal("species-alpha", lineage.GetProperty("source_species_id").GetString());
            Assert.Equal("species-alpha", lineage.GetProperty("dominant_species_id").GetString());
            Assert.Equal(0.78d, lineage.GetProperty("source_species_similarity_score").GetDouble(), 3);
            Assert.Equal(1.00d, lineage.GetProperty("lineage_assignment_similarity_score").GetDouble(), 3);
            Assert.Equal(1.00d, lineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_HindsightReassignsRecentSourceMembersWithinSimilarityWindow()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.80d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0.04d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageHindsightReassignCommitWindow: 6,
            lineageHindsightSimilarityMargin: 0.08d));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var sourceParent = Guid.NewGuid();
            var recentWithinWindow = Guid.NewGuid();
            var recentBelowBand = Guid.NewGuid();
            var recentOutsideMargin = Guid.NewGuid();
            var splitFounder = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = sourceParent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 10
                });
            Assert.True(seedParent.Decision.Success);

            var withinWindow = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = recentWithinWindow.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.83,\"parent_a_similarity_score\":0.83}}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 20
                });
            Assert.True(withinWindow.Decision.Success);
            Assert.Equal("species-alpha", withinWindow.Decision.SpeciesId);

            var belowBand = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = recentBelowBand.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.69,\"parent_a_similarity_score\":0.69}}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 25
                });
            Assert.True(belowBand.Decision.Success);
            Assert.Equal("species-alpha", belowBand.Decision.SpeciesId);

            var outsideMargin = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = recentOutsideMargin.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.89,\"parent_a_similarity_score\":0.89}}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 30
                });
            Assert.True(outsideMargin.Decision.Success);
            Assert.Equal("species-alpha", outsideMargin.Decision.SpeciesId);

            var splitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = splitFounder.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.78,\"parent_a_similarity_score\":0.78}}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 40
                });

            Assert.True(splitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", splitDecision.Decision.DecisionReason);
            Assert.StartsWith("species-alpha-branch-", splitDecision.Decision.SpeciesId, StringComparison.Ordinal);

            IReadOnlyDictionary<Guid, SpeciationMembershipRecord>? byBrain = null;
            await WaitForConditionAsync(
                async () =>
                {
                    var memberships = await system.Root.RequestAsync<SpeciationListMembershipsResponse>(
                        managerPid,
                        new SpeciationListMembershipsRequest());
                    byBrain = memberships.Memberships.ToDictionary(item => item.BrainId);
                    return byBrain.TryGetValue(recentWithinWindow, out var membership)
                           && string.Equals(
                               membership.SpeciesId,
                               splitDecision.Decision.SpeciesId,
                               StringComparison.Ordinal);
                },
                TimeSpan.FromSeconds(2));

            Assert.NotNull(byBrain);
            Assert.Equal(splitDecision.Decision.SpeciesId, byBrain[recentWithinWindow].SpeciesId);
            Assert.Equal("lineage_hindsight_recent_reassign", byBrain[recentWithinWindow].DecisionReason);
            Assert.Equal("species-alpha", byBrain[recentBelowBand].SpeciesId);
            Assert.Equal("species-alpha", byBrain[recentOutsideMargin].SpeciesId);

            using var reassignedMetadata = JsonDocument.Parse(byBrain[recentWithinWindow].DecisionMetadataJson);
            var lineage = reassignedMetadata.RootElement.GetProperty("lineage");
            Assert.Equal("species-alpha", lineage.GetProperty("hindsight_source_species_id").GetString());
            Assert.Equal(splitDecision.Decision.SpeciesId, lineage.GetProperty("hindsight_target_species_id").GetString());
            Assert.Equal(0.70d, lineage.GetProperty("hindsight_similarity_lower_bound").GetDouble(), 3);
            Assert.Equal(0.86d, lineage.GetProperty("hindsight_similarity_upper_bound").GetDouble(), 3);
            Assert.Equal("compatibility_assessment", lineage.GetProperty("assigned_species_similarity_source").GetString());
            Assert.Equal(0.96d, lineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
            Assert.True(lineage.GetProperty("split_proximity_to_dynamic_threshold").GetDouble() > 0d);
            Assert.True(
                lineage.GetProperty("split_proximity_to_dynamic_threshold").GetDouble()
                > lineage.GetProperty("source_split_proximity_to_dynamic_threshold").GetDouble());
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_HindsightReassign_SkipsCandidateWhenCompatibilityDoesNotBeatSourceSpecies()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.80d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0.04d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageHindsightReassignCommitWindow: 6,
            lineageHindsightSimilarityMargin: 0.08d));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.70f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var sourceParent = Guid.NewGuid();
            var recentWithinWindow = Guid.NewGuid();
            var splitFounder = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = sourceParent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 10
                });
            Assert.True(seedParent.Decision.Success);

            var withinWindow = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = recentWithinWindow.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.83,\"parent_a_similarity_score\":0.83}}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 20
                });
            Assert.True(withinWindow.Decision.Success);
            Assert.Equal("species-alpha", withinWindow.Decision.SpeciesId);

            var splitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = splitFounder.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.78,\"parent_a_similarity_score\":0.78}}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 40
                });
            Assert.True(splitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", splitDecision.Decision.DecisionReason);

            var memberships = await system.Root.RequestAsync<SpeciationListMembershipsResponse>(
                managerPid,
                new SpeciationListMembershipsRequest());
            var byBrain = memberships.Memberships.ToDictionary(item => item.BrainId);
            Assert.Equal("species-alpha", byBrain[recentWithinWindow].SpeciesId);
            Assert.NotEqual("lineage_hindsight_recent_reassign", byBrain[recentWithinWindow].DecisionReason);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_RecentDerivedSpeciesReuse_PreventsSiblingSplitForNearbySourceMembers()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.80d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0.04d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageHindsightReassignCommitWindow: 6,
            lineageHindsightSimilarityMargin: 0.03d));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.97f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var sourceParent = Guid.NewGuid();
            var alphaChildA = Guid.NewGuid();
            var alphaChildB = Guid.NewGuid();
            var splitFounder = Guid.NewGuid();
            var nearbySourceMember = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = sourceParent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 10
                });
            Assert.True(seedParent.Decision.Success);

            var seedAlphaA = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = alphaChildA.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.94,\"parent_a_similarity_score\":0.94}}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 20
                });
            Assert.True(seedAlphaA.Decision.Success);
            Assert.Equal("species-alpha", seedAlphaA.Decision.SpeciesId);

            var seedAlphaB = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = alphaChildB.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.93,\"parent_a_similarity_score\":0.93}}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 30
                });
            Assert.True(seedAlphaB.Decision.Success);
            Assert.Equal("species-alpha", seedAlphaB.Decision.SpeciesId);

            var splitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = splitFounder.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = alphaChildA.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.78,\"parent_a_similarity_score\":0.78}}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 40
                });

            Assert.True(splitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", splitDecision.Decision.DecisionReason);
            Assert.StartsWith("species-alpha-branch-", splitDecision.Decision.SpeciesId, StringComparison.Ordinal);

            var reuseDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = nearbySourceMember.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = alphaChildB.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.79,\"parent_a_similarity_score\":0.79}}",
                    HasDecisionTimeMs = true,
                    DecisionTimeMs = 50
                });

            Assert.True(reuseDecision.Decision.Success);
            Assert.Equal(splitDecision.Decision.SpeciesId, reuseDecision.Decision.SpeciesId);
            Assert.Equal("lineage_reuse_recent_derived_species", reuseDecision.Decision.DecisionReason);

            using var metadata = JsonDocument.Parse(reuseDecision.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.Equal("species-alpha", lineage.GetProperty("recent_derived_source_species_id").GetString());
            Assert.Equal(0.78d, lineage.GetProperty("recent_derived_founder_similarity_score").GetDouble(), 3);
            Assert.Equal(0.03d, lineage.GetProperty("recent_derived_similarity_margin").GetDouble(), 3);
            Assert.Equal("compatibility_assessment", lineage.GetProperty("assigned_species_similarity_source").GetString());
            Assert.Equal(0.97d, lineage.GetProperty("lineage_assignment_similarity_score").GetDouble(), 3);
            Assert.Equal(0.97d, lineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
            Assert.True(lineage.GetProperty("split_proximity_to_dynamic_threshold").GetDouble() > 0d);
            Assert.True(lineage.GetProperty("source_split_proximity_to_dynamic_threshold").GetDouble() < 0d);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_NewbornDerivedSpecies_SeedsSingletonIntraSpeciesSimilarity()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.70d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            await WaitForEpochAsync(system, managerPid);
            var sourceParent = Guid.NewGuid();
            var firstChild = Guid.NewGuid();
            var secondChild = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = sourceParent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var splitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = firstChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50}}"
                });
            Assert.True(splitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", splitDecision.Decision.DecisionReason);
            using var metadata = JsonDocument.Parse(splitDecision.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            var policy = metadata.RootElement.GetProperty("assignment_policy");
            Assert.Equal(0.50d, lineage.GetProperty("dominant_species_similarity_score").GetDouble(), 3);
            Assert.Equal(0.50d, lineage.GetProperty("source_species_similarity_score").GetDouble(), 3);
            Assert.Equal(1.00d, lineage.GetProperty("lineage_assignment_similarity_score").GetDouble(), 3);
            Assert.Equal(1.00d, lineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
            Assert.Equal(splitDecision.Decision.SpeciesId, lineage.GetProperty("intra_species_similarity_species_id").GetString());
            Assert.Equal(0.50d, policy.GetProperty("lineage_source_species_similarity_score").GetDouble(), 3);
            Assert.Equal(1.00d, policy.GetProperty("lineage_assignment_similarity_score").GetDouble(), 3);
            Assert.Equal(0.70d, policy.GetProperty("lineage_dynamic_split_threshold").GetDouble(), 3);
            Assert.Equal(0.70d, policy.GetProperty("lineage_source_dynamic_split_threshold").GetDouble(), 3);
            Assert.True(lineage.GetProperty("split_proximity_to_dynamic_threshold").GetDouble() > 0d);
            Assert.True(lineage.GetProperty("source_split_proximity_to_dynamic_threshold").GetDouble() < 0d);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_NewbornDerivedSpecies_FounderSingletonDoesNotUnlockNestedSplit()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.70d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            await WaitForEpochAsync(system, managerPid);
            var sourceParent = Guid.NewGuid();
            var firstChild = Guid.NewGuid();
            var secondChild = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = sourceParent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var splitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = firstChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50}}"
                });
            Assert.True(splitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", splitDecision.Decision.DecisionReason);

            var secondSplitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = secondChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = firstChild.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.65,\"parent_a_similarity_score\":0.65}}"
                });
            Assert.True(secondSplitDecision.Decision.Success);
            Assert.Equal(
                "lineage_bootstrap_compatibility_required",
                secondSplitDecision.Decision.DecisionReason);
            Assert.Equal("species-alpha", secondSplitDecision.Decision.SpeciesId);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_NewbornDerivedSpecies_SplitsAgainAfterFirstNonFounderSample()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.70d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var sourceParent = Guid.NewGuid();
            var firstChild = Guid.NewGuid();
            var secondChild = Guid.NewGuid();
            var thirdChild = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = sourceParent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var splitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = firstChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50}}"
                });
            Assert.True(splitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", splitDecision.Decision.DecisionReason);

            var inSpeciesDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = secondChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = firstChild.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.92,\"parent_a_similarity_score\":0.92}}"
                });
            Assert.True(inSpeciesDecision.Decision.Success);
            Assert.Equal(splitDecision.Decision.SpeciesId, inSpeciesDecision.Decision.SpeciesId);
            Assert.Equal("lineage_inherit_similarity_match", inSpeciesDecision.Decision.DecisionReason);
            using (var metadata = JsonDocument.Parse(inSpeciesDecision.Decision.DecisionMetadataJson))
            {
                var lineage = metadata.RootElement.GetProperty("lineage");
                Assert.Equal("compatibility_assessment", lineage.GetProperty("assigned_species_similarity_source").GetString());
                Assert.Equal(0.96d, lineage.GetProperty("lineage_assignment_similarity_score").GetDouble(), 3);
                Assert.Equal(0.96d, lineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
            }

            var secondSplitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = thirdChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = firstChild.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.65,\"parent_a_similarity_score\":0.65}}"
                });
            Assert.True(secondSplitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", secondSplitDecision.Decision.DecisionReason);
            Assert.StartsWith(splitDecision.Decision.SpeciesId + "-branch-", secondSplitDecision.Decision.SpeciesId, StringComparison.Ordinal);
            Assert.NotEqual(splitDecision.Decision.SpeciesId, secondSplitDecision.Decision.SpeciesId);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_NewbornDerivedSpecies_RepeatedDerivedIdUsesCompatibilityBootstrapSample()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.70d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var sourceParent = Guid.NewGuid();
            var firstChild = Guid.NewGuid();
            var secondChild = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = sourceParent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var firstSplitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = firstChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50}}"
                });
            Assert.True(firstSplitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", firstSplitDecision.Decision.DecisionReason);

            var secondSplitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = secondChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50}}"
                });

            Assert.True(secondSplitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", secondSplitDecision.Decision.DecisionReason);
            Assert.Equal(firstSplitDecision.Decision.SpeciesId, secondSplitDecision.Decision.SpeciesId);

            using var metadata = JsonDocument.Parse(secondSplitDecision.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.Equal("compatibility_assessment", lineage.GetProperty("assigned_species_similarity_source").GetString());
            Assert.Equal(0.96d, lineage.GetProperty("lineage_assignment_similarity_score").GetDouble(), 3);
            Assert.Equal(0.96d, lineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
            Assert.True(lineage.GetProperty("split_proximity_to_dynamic_threshold").GetDouble() > 0d);
            Assert.True(lineage.GetProperty("source_split_proximity_to_dynamic_threshold").GetDouble() < 0d);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_ArtifactDerivedSpecies_RepeatedDerivedIdUsesCompatibilityBootstrapSample()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.70d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);

            var parentARef = new string('a', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var parentBRef = new string('b', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var firstChildRef = new string('c', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var secondChildRef = new string('d', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");

            foreach (var parentRef in new[] { parentARef, parentBRef })
            {
                var seededParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                    managerPid,
                    new ProtoSpec.SpeciationAssignRequest
                    {
                        ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                        Candidate = new ProtoSpec.SpeciationCandidateRef
                        {
                            ArtifactRef = parentRef
                        },
                        SpeciesId = "species-alpha",
                        SpeciesDisplayName = "Species Alpha",
                        DecisionReason = "seed_parent_species",
                        DecisionMetadataJson = "{\"source\":\"seed\"}"
                    });
                Assert.True(seededParent.Decision.Success);
            }

            var firstSplitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = firstChildRef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentARef },
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentBRef }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50,\"parent_b_similarity_score\":0.50}}"
                });
            Assert.True(firstSplitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", firstSplitDecision.Decision.DecisionReason);

            var secondSplitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = secondChildRef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentARef },
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentBRef }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50,\"parent_b_similarity_score\":0.50}}"
                });

            Assert.True(secondSplitDecision.Decision.Success);
            Assert.Equal(firstSplitDecision.Decision.SpeciesId, secondSplitDecision.Decision.SpeciesId);

            using var metadata = JsonDocument.Parse(secondSplitDecision.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.Equal("compatibility_assessment", lineage.GetProperty("assigned_species_similarity_source").GetString());
            Assert.Equal(0.96d, lineage.GetProperty("lineage_assignment_similarity_score").GetDouble(), 3);
            Assert.Equal(0.96d, lineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
            Assert.True(lineage.GetProperty("split_proximity_to_dynamic_threshold").GetDouble() > 0d);
            Assert.True(lineage.GetProperty("source_split_proximity_to_dynamic_threshold").GetDouble() < 0d);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_ArtifactDerivedSpecies_HysteresisSeedBootstrapUsesCompatibilitySample()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.92d,
            lineageSplitThreshold: 0.88d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0.04d,
            lineageSplitGuardMargin: 0.02d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);

            var parentARef = new string('a', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var parentBRef = new string('b', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var founderRef = new string('c', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var unknownParentRef = new string('e', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var secondChildRef = new string('d', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");

            foreach (var parentRef in new[] { parentARef, parentBRef })
            {
                var seededParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                    managerPid,
                    new ProtoSpec.SpeciationAssignRequest
                    {
                        ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                        Candidate = new ProtoSpec.SpeciationCandidateRef
                        {
                            ArtifactRef = parentRef
                        },
                        SpeciesId = "species-alpha",
                        SpeciesDisplayName = "Species Alpha",
                        DecisionReason = "seed_parent_species",
                        DecisionMetadataJson = "{\"source\":\"seed\"}"
                    });
                Assert.True(seededParent.Decision.Success);
            }

            var founderDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = founderRef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentARef },
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentBRef }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.86660916,\"parent_a_similarity_score\":0.8854921,\"parent_b_similarity_score\":0.8477262}}"
                });
            Assert.True(founderDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", founderDecision.Decision.DecisionReason);

            var bootstrapDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = secondChildRef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = founderRef },
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = unknownParentRef }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.89079946,\"parent_a_similarity_score\":0.8942623,\"parent_b_similarity_score\":0.8873366}}"
                });

            Assert.True(bootstrapDecision.Decision.Success);
            Assert.Equal(founderDecision.Decision.SpeciesId, bootstrapDecision.Decision.SpeciesId);
            Assert.Equal("lineage_hysteresis_seed", bootstrapDecision.Decision.DecisionReason);

            using var metadata = JsonDocument.Parse(bootstrapDecision.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.Equal(founderDecision.Decision.SpeciesId, lineage.GetProperty("source_species_id").GetString());
            Assert.Equal("compatibility_assessment", lineage.GetProperty("assigned_species_similarity_source").GetString());
            Assert.Equal(0.96d, lineage.GetProperty("lineage_assignment_similarity_score").GetDouble(), 3);
            Assert.Equal(0.96d, lineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
            Assert.True(lineage.GetProperty("split_proximity_to_dynamic_threshold").GetDouble() > 0.09d);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_NewbornDerivedSpecies_CompatibilityAssessmentUsesReproductionSettingsWithSpawnDisabled()
    {
        using var settingsDb = new TempDatabaseScope("settings-monitor.db");
        using var speciationDb = new TempDatabaseScope("speciation.db");

        var settingsStore = new SettingsMonitorStore(settingsDb.DatabasePath);
        await settingsStore.InitializeAsync();

        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.92d,
            lineageSplitThreshold: 0.88d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0.04d,
            lineageSplitGuardMargin: 0.02d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));

        var system = new ActorSystem();
        try
        {
            var settingsPid = system.Root.Spawn(Props.FromProducer(() => new SettingsMonitorActor(settingsStore)));
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = ReproductionSettingsKeys.MaxRegionSpanDiffRatioKey,
                Value = "0.11"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = ReproductionSettingsKeys.MaxFunctionHistDistanceKey,
                Value = "0.22"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = ReproductionSettingsKeys.MaxConnectivityHistDistanceKey,
                Value = "0.33"
            });
            await system.Root.RequestAsync<ProtoSettings.SettingValue>(settingsPid, new ProtoSettings.SettingSet
            {
                Key = ReproductionSettingsKeys.SpawnChildKey,
                Value = "spawn_child_always"
            });

            var capturedConfig = new TaskCompletionSource<CapturedCompatibilityConfig>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproCaptureCompatibilityConfigProbe(
                    CreateCompatibilityAssessmentResult(0.96f),
                    capturedConfig)));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: settingsPid,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);

            var parentARef = new string('a', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var parentBRef = new string('b', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var founderRef = new string('c', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var unknownParentRef = new string('e', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var secondChildRef = new string('d', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");

            foreach (var parentRef in new[] { parentARef, parentBRef })
            {
                var seededParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                    managerPid,
                    new ProtoSpec.SpeciationAssignRequest
                    {
                        ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                        Candidate = new ProtoSpec.SpeciationCandidateRef
                        {
                            ArtifactRef = parentRef
                        },
                        SpeciesId = "species-alpha",
                        SpeciesDisplayName = "Species Alpha",
                        DecisionReason = "seed_parent_species",
                        DecisionMetadataJson = "{\"source\":\"seed\"}"
                    });
                Assert.True(seededParent.Decision.Success);
            }

            var founderDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = founderRef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentARef },
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentBRef }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.86660916,\"parent_a_similarity_score\":0.8854921,\"parent_b_similarity_score\":0.8477262}}"
                });
            Assert.True(founderDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", founderDecision.Decision.DecisionReason);

            var bootstrapDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = secondChildRef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = founderRef },
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = unknownParentRef }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.89079946,\"parent_a_similarity_score\":0.8942623,\"parent_b_similarity_score\":0.8873366}}"
                });

            Assert.True(bootstrapDecision.Decision.Success);
            Assert.Equal(founderDecision.Decision.SpeciesId, bootstrapDecision.Decision.SpeciesId);
            Assert.Equal("lineage_hysteresis_seed", bootstrapDecision.Decision.DecisionReason);

            var config = await capturedConfig.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(0.11f, config.MaxRegionSpanDiffRatio, 3);
            Assert.Equal(0.22f, config.MaxFunctionHistDistance, 3);
            Assert.Equal(0.33f, config.MaxConnectivityHistDistance, 3);
            Assert.Equal(Repro.SpawnChildPolicy.SpawnChildNever, config.SpawnChild);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_NewbornDerivedSpecies_BootstrapFallbacksOnLowCompatibility()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.70d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.60f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var sourceParent = Guid.NewGuid();
            var firstChild = Guid.NewGuid();
            var secondChild = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = sourceParent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var splitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = firstChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50}}"
                });
            Assert.True(splitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", splitDecision.Decision.DecisionReason);

            var bootstrapDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = secondChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = firstChild.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.92,\"parent_a_similarity_score\":0.92}}"
                });

            Assert.True(bootstrapDecision.Decision.Success);
            Assert.Equal("lineage_bootstrap_compatibility_required", bootstrapDecision.Decision.DecisionReason);
            Assert.Equal("species-alpha", bootstrapDecision.Decision.SpeciesId);

            using var metadata = JsonDocument.Parse(bootstrapDecision.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.False(lineage.TryGetProperty("assigned_species_similarity_source", out _));
            Assert.True(lineage.GetProperty("assigned_species_compatibility_attempted").GetBoolean());
            Assert.False(lineage.GetProperty("assigned_species_compatibility_admitted").GetBoolean());
            Assert.Equal("brain_ids", lineage.GetProperty("assigned_species_compatibility_mode").GetString());
            Assert.Equal(0.60d, lineage.GetProperty("assigned_species_compatibility_similarity_score").GetDouble(), 3);
            Assert.True(lineage.GetProperty("assigned_species_compatibility_report_compatible").GetBoolean());
            Assert.Equal(
                "compatibility_similarity_below_threshold",
                lineage.GetProperty("assigned_species_compatibility_failure_reason").GetString());
            Assert.True(lineage.GetProperty("assigned_species_compatibility_elapsed_ms").GetInt64() >= 0L);
            Assert.Equal(1, lineage.GetProperty("assigned_species_compatibility_exemplar_count").GetInt32());
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_NewbornDerivedSpecies_UsesPersistedBrainArtifactProvenanceWhenFounderGoesOffline()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.70d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var sourceParent = Guid.NewGuid();
            var founder = Guid.NewGuid();
            var secondChild = Guid.NewGuid();

            var sourceParentBase = new string('a', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var sourceParentSnapshot = new string('b', 64).ToArtifactRef(64, "application/x-nbs", "artifact-store");
            var founderBase = new string('c', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var founderSnapshot = new string('d', 64).ToArtifactRef(64, "application/x-nbs", "artifact-store");
            var secondChildBase = new string('e', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var secondChildSnapshot = new string('f', 64).ToArtifactRef(64, "application/x-nbs", "artifact-store");

            var ioProbeActor = new MutableIoGatewayProbe(new Dictionary<Guid, ProtoIo.BrainInfo>
            {
                [sourceParent] = new()
                {
                    BrainId = sourceParent.ToProtoUuid(),
                    BaseDefinition = sourceParentBase,
                    LastSnapshot = sourceParentSnapshot,
                    InputWidth = 4,
                    OutputWidth = 2
                },
                [founder] = new()
                {
                    BrainId = founder.ToProtoUuid(),
                    BaseDefinition = founderBase,
                    LastSnapshot = founderSnapshot,
                    InputWidth = 4,
                    OutputWidth = 2
                },
                [secondChild] = new()
                {
                    BrainId = secondChild.ToProtoUuid(),
                    BaseDefinition = secondChildBase,
                    LastSnapshot = secondChildSnapshot,
                    InputWidth = 4,
                    OutputWidth = 2
                }
            });
            var ioPid = system.Root.Spawn(Props.FromProducer(() => ioProbeActor));
            var reproProbeActor = new ReproArtifactsOnlyCompatibilityProbe(CreateCompatibilityAssessmentResult(0.96f));
            var reproPid = system.Root.Spawn(Props.FromProducer(() => reproProbeActor));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid,
                    ioGatewayPid: ioPid)));

            await WaitForEpochAsync(system, managerPid);

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = sourceParent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var founderDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = founder.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50}}"
                });
            Assert.True(founderDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", founderDecision.Decision.DecisionReason);

            using (var founderMetadata = JsonDocument.Parse(founderDecision.Decision.DecisionMetadataJson))
            {
                var lineage = founderMetadata.RootElement.GetProperty("lineage");
                var storedBase = lineage.GetProperty("candidate_brain_base_artifact_ref");
                Assert.Equal(new string('c', 64), storedBase.GetProperty("sha256_hex").GetString());
                var storedSnapshot = lineage.GetProperty("candidate_brain_snapshot_artifact_ref");
                Assert.Equal(new string('d', 64), storedSnapshot.GetProperty("sha256_hex").GetString());
            }

            ioProbeActor.Remove(founder);

            var bootstrapDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = secondChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = founder.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.92,\"parent_a_similarity_score\":0.92}}"
                });

            Assert.True(bootstrapDecision.Decision.Success);
            Assert.Equal(founderDecision.Decision.SpeciesId, bootstrapDecision.Decision.SpeciesId);

            using var bootstrapMetadata = JsonDocument.Parse(bootstrapDecision.Decision.DecisionMetadataJson);
            var bootstrapLineage = bootstrapMetadata.RootElement.GetProperty("lineage");
            Assert.Equal("artifacts", bootstrapLineage.GetProperty("assigned_species_compatibility_mode").GetString());
            Assert.Equal("compatibility_assessment", bootstrapLineage.GetProperty("assigned_species_similarity_source").GetString());
            Assert.Equal(0.96d, bootstrapLineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
            Assert.Equal(1, reproProbeActor.ArtifactCompatibilityRequestCount);
            Assert.Equal(0, reproProbeActor.BrainIdCompatibilityRequestCount);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_NewbornDerivedSpecies_BootstrapFallbackPersistsTimeoutCompatibilityProvenance()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.70d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        var compatibilityTimeout = TimeSpan.FromMilliseconds(50);
        var releaseAssessments = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            var reproProbe = new BlockingCompatibilityProbe(
                CreateCompatibilityAssessmentResult(0.96f),
                releaseAssessments.Task);
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => reproProbe));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid,
                    compatibilityRequestTimeout: compatibilityTimeout)));

            await WaitForEpochAsync(system, managerPid);

            var parentARef = new string('a', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var parentBRef = new string('b', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var founderRef = new string('c', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var secondChildRef = new string('d', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");

            foreach (var parentRef in new[] { parentARef, parentBRef })
            {
                var seededParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                    managerPid,
                    new ProtoSpec.SpeciationAssignRequest
                    {
                        ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                        Candidate = new ProtoSpec.SpeciationCandidateRef
                        {
                            ArtifactRef = parentRef
                        },
                        SpeciesId = "species-alpha",
                        SpeciesDisplayName = "Species Alpha",
                        DecisionReason = "seed_parent_species",
                        DecisionMetadataJson = "{\"source\":\"seed\"}"
                    });
                Assert.True(seededParent.Decision.Success);
            }

            var founderDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = founderRef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentARef },
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentBRef }
                    },
                    DecisionMetadataJson =
                        "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50,\"parent_b_similarity_score\":0.50}}"
                });
            Assert.True(founderDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", founderDecision.Decision.DecisionReason);

            var bootstrapDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = secondChildRef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = founderRef }
                    },
                    DecisionMetadataJson =
                        "{\"lineage\":{\"lineage_similarity_score\":0.92,\"parent_a_similarity_score\":0.92}}"
                });

            Assert.True(bootstrapDecision.Decision.Success);
            Assert.Equal("lineage_bootstrap_compatibility_required", bootstrapDecision.Decision.DecisionReason);
            Assert.Equal("species-alpha", bootstrapDecision.Decision.SpeciesId);

            using var metadata = JsonDocument.Parse(bootstrapDecision.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.False(lineage.TryGetProperty("assigned_species_similarity_source", out _));
            Assert.True(lineage.GetProperty("assigned_species_compatibility_attempted").GetBoolean());
            Assert.False(lineage.GetProperty("assigned_species_compatibility_admitted").GetBoolean());
            Assert.Equal("artifacts", lineage.GetProperty("assigned_species_compatibility_mode").GetString());
            Assert.False(lineage.GetProperty("assigned_species_compatibility_report_compatible").GetBoolean());
            Assert.Equal(
                "repro_request_timeout",
                lineage.GetProperty("assigned_species_compatibility_abort_reason").GetString());
            Assert.Equal(
                "compatibility_request_timeout",
                lineage.GetProperty("assigned_species_compatibility_failure_reason").GetString());
            Assert.True(
                lineage.GetProperty("assigned_species_compatibility_elapsed_ms").GetInt64()
                >= (long)Math.Ceiling(compatibilityTimeout.TotalMilliseconds));
            Assert.Equal(1, lineage.GetProperty("assigned_species_compatibility_exemplar_count").GetInt32());
            Assert.Equal(1, lineage.GetProperty("assigned_species_compatibility_exemplar_brain_ids").GetArrayLength());
            Assert.False(lineage.TryGetProperty("assigned_species_compatibility_similarity_score", out _));
            Assert.Equal(1, reproProbe.CompatibilityRequestCount);
        }
        finally
        {
            releaseAssessments.TrySetResult();
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_FirstFounderHindsightLimitsCompatibilityAssessmentsToBootstrapRoot()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.95d,
            lineageSplitThreshold: 0.40d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0.005d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 8,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var reproProbe = new ReproCountingResponseProbe(CreateCompatibilityAssessmentResult(0.96f));
            var reproPid = system.Root.Spawn(Props.FromProducer(() => reproProbe));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);

            var parentARef = new string('a', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var parentBRef = new string('b', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var sourceChildren = new[] { '1', '2', '3', '4', '5' }
                .Select(digit => new string(digit, 64).ToArtifactRef(256, "application/x-nbn", "artifact-store"))
                .ToArray();
            var founderRef = new string('f', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");

            foreach (var parentRef in new[] { parentARef, parentBRef })
            {
                var seededParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                    managerPid,
                    new ProtoSpec.SpeciationAssignRequest
                    {
                        ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                        Candidate = new ProtoSpec.SpeciationCandidateRef
                        {
                            ArtifactRef = parentRef
                        },
                        SpeciesId = "species-alpha",
                        SpeciesDisplayName = "Species Alpha",
                        DecisionReason = "seed_parent_species",
                        DecisionMetadataJson = "{\"source\":\"seed\"}"
                    });
                Assert.True(seededParent.Decision.Success);
            }

            foreach (var childRef in sourceChildren)
            {
                var sourceDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                    managerPid,
                    new ProtoSpec.SpeciationAssignRequest
                    {
                        ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                        Candidate = new ProtoSpec.SpeciationCandidateRef
                        {
                            ArtifactRef = childRef
                        },
                        Parents =
                        {
                            new ProtoSpec.SpeciationParentRef { ArtifactRef = parentARef },
                            new ProtoSpec.SpeciationParentRef { ArtifactRef = parentBRef }
                        },
                        DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.51,\"parent_a_similarity_score\":0.51,\"parent_b_similarity_score\":0.51}}"
                    });
                Assert.True(sourceDecision.Decision.Success);
                Assert.Equal("species-alpha", sourceDecision.Decision.SpeciesId);
            }

            var founderDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = founderRef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentARef },
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentBRef }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50,\"parent_b_similarity_score\":0.50}}"
                });

            Assert.True(founderDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", founderDecision.Decision.DecisionReason);
            await WaitForConditionAsync(
                () => Task.FromResult(reproProbe.CompatibilityRequestCount == 3),
                TimeSpan.FromSeconds(2));
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_FounderResponseDoesNotWaitForHindsightCompatibilityAssessments()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.95d,
            lineageSplitThreshold: 0.40d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0.005d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 8,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        var releaseAssessments = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            var reproProbe = new BlockingCompatibilityProbe(
                CreateCompatibilityAssessmentResult(0.96f),
                releaseAssessments.Task);
            var reproPid = system.Root.Spawn(Props.FromProducer(() => reproProbe));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);

            var parentARef = new string('a', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var parentBRef = new string('b', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var sourceChildren = new[] { '1', '2', '3', '4', '5' }
                .Select(digit => new string(digit, 64).ToArtifactRef(256, "application/x-nbn", "artifact-store"))
                .ToArray();
            var founderRef = new string('f', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");

            foreach (var parentRef in new[] { parentARef, parentBRef })
            {
                var seededParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                    managerPid,
                    new ProtoSpec.SpeciationAssignRequest
                    {
                        ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                        Candidate = new ProtoSpec.SpeciationCandidateRef
                        {
                            ArtifactRef = parentRef
                        },
                        SpeciesId = "species-alpha",
                        SpeciesDisplayName = "Species Alpha",
                        DecisionReason = "seed_parent_species",
                        DecisionMetadataJson = "{\"source\":\"seed\"}"
                    });
                Assert.True(seededParent.Decision.Success);
            }

            foreach (var childRef in sourceChildren)
            {
                var sourceDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                    managerPid,
                    new ProtoSpec.SpeciationAssignRequest
                    {
                        ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                        Candidate = new ProtoSpec.SpeciationCandidateRef
                        {
                            ArtifactRef = childRef
                        },
                        Parents =
                        {
                            new ProtoSpec.SpeciationParentRef { ArtifactRef = parentARef },
                            new ProtoSpec.SpeciationParentRef { ArtifactRef = parentBRef }
                        },
                        DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.51,\"parent_a_similarity_score\":0.51,\"parent_b_similarity_score\":0.51}}"
                    });
                Assert.True(sourceDecision.Decision.Success);
                Assert.Equal("species-alpha", sourceDecision.Decision.SpeciesId);
            }

            var founderDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = founderRef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentARef },
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentBRef }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50,\"parent_b_similarity_score\":0.50}}"
                },
                TimeSpan.FromSeconds(1));

            Assert.True(founderDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", founderDecision.Decision.DecisionReason);

            releaseAssessments.TrySetResult();
            await WaitForConditionAsync(
                () => Task.FromResult(reproProbe.CompatibilityRequestCount == 3),
                TimeSpan.FromSeconds(2));
        }
        finally
        {
            releaseAssessments.TrySetResult();
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_NewbornDerivedSpecies_BootstrapAdmitsWhenCompatibilityClearsSplitThresholdBelowMatchThreshold()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.86d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.88f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var sourceParent = Guid.NewGuid();
            var firstChild = Guid.NewGuid();
            var secondChild = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = sourceParent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var firstSplitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = firstChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50}}"
                });
            Assert.True(firstSplitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", firstSplitDecision.Decision.DecisionReason);

            var bootstrapDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = secondChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50}}"
                });

            Assert.True(bootstrapDecision.Decision.Success);
            Assert.Equal(firstSplitDecision.Decision.SpeciesId, bootstrapDecision.Decision.SpeciesId);

            using var metadata = JsonDocument.Parse(bootstrapDecision.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.Equal("compatibility_assessment", lineage.GetProperty("assigned_species_similarity_source").GetString());
            Assert.Equal(0.88d, lineage.GetProperty("lineage_assignment_similarity_score").GetDouble(), 3);
            Assert.Equal(0.88d, lineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
            Assert.True(lineage.GetProperty("split_proximity_to_dynamic_threshold").GetDouble() > 0d);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_NewbornDerivedSpecies_ThirdBootstrapMemberUsesSingleEarliestExemplar()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.86d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var reproProbe = new ReproCountingResponseProbe(CreateCompatibilityAssessmentResult(0.88f));
            var reproPid = system.Root.Spawn(Props.FromProducer(() => reproProbe));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var sourceParent = Guid.NewGuid();
            var founder = Guid.NewGuid();
            var secondMember = Guid.NewGuid();
            var thirdMember = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = sourceParent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var founderDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = founder.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50}}"
                });
            Assert.True(founderDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", founderDecision.Decision.DecisionReason);

            var secondDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = secondMember.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50}}"
                });

            Assert.True(secondDecision.Decision.Success);
            Assert.Equal(founderDecision.Decision.SpeciesId, secondDecision.Decision.SpeciesId);
            Assert.Equal(1, reproProbe.CompatibilityRequestCount);

            var thirdDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = thirdMember.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50}}"
                });

            Assert.True(thirdDecision.Decision.Success);
            Assert.Equal(founderDecision.Decision.SpeciesId, thirdDecision.Decision.SpeciesId);
            Assert.Equal(2, reproProbe.CompatibilityRequestCount);

            using var metadata = JsonDocument.Parse(thirdDecision.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.Equal("compatibility_assessment", lineage.GetProperty("assigned_species_similarity_source").GetString());
            Assert.True(lineage.GetProperty("assigned_species_compatibility_admitted").GetBoolean());
            Assert.Equal(1, lineage.GetProperty("assigned_species_compatibility_exemplar_count").GetInt32());
            Assert.Equal(1, lineage.GetProperty("assigned_species_compatibility_exemplar_brain_ids").GetArrayLength());
            Assert.Equal(0.88d, lineage.GetProperty("assigned_species_compatibility_similarity_score").GetDouble(), 3);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_NewbornDerivedSpecies_BootstrapAllowsNearTieAgainstSourceWithinSplitGuardMargin()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.92d,
            lineageSplitThreshold: 0.86d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0.04d,
            lineageSplitGuardMargin: 0.02d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.87f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var sourceParent = Guid.NewGuid();
            var firstChild = Guid.NewGuid();
            var secondChild = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = sourceParent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var firstSplitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = firstChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50}}"
                });
            Assert.True(firstSplitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", firstSplitDecision.Decision.DecisionReason);

            var bootstrapDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = secondChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.885,\"parent_a_similarity_score\":0.885}}"
                });

            Assert.True(bootstrapDecision.Decision.Success);
            Assert.Equal(firstSplitDecision.Decision.SpeciesId, bootstrapDecision.Decision.SpeciesId);

            using var metadata = JsonDocument.Parse(bootstrapDecision.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.Equal("compatibility_assessment", lineage.GetProperty("assigned_species_similarity_source").GetString());
            Assert.Equal(0.87d, lineage.GetProperty("lineage_assignment_similarity_score").GetDouble(), 3);
            Assert.Equal(0.87d, lineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_NewbornDerivedSpecies_UsesManualSimilarityEvenWhenAssessmentIsIncompatible()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.92d,
            lineageSplitThreshold: 0.88d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0.04d,
            lineageSplitGuardMargin: 0.02d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(
                    CreateIncompatibleCompatibilityAssessmentResult(
                        0.96f,
                        "repro_spot_check_overlap_mismatch"))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);

            var parentARef = new string('a', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var parentBRef = new string('b', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var founderRef = new string('c', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var unknownParentRef = new string('e', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");
            var secondChildRef = new string('d', 64).ToArtifactRef(256, "application/x-nbn", "artifact-store");

            foreach (var parentRef in new[] { parentARef, parentBRef })
            {
                var seededParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                    managerPid,
                    new ProtoSpec.SpeciationAssignRequest
                    {
                        ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                        Candidate = new ProtoSpec.SpeciationCandidateRef
                        {
                            ArtifactRef = parentRef
                        },
                        SpeciesId = "species-alpha",
                        SpeciesDisplayName = "Species Alpha",
                        DecisionReason = "seed_parent_species",
                        DecisionMetadataJson = "{\"source\":\"seed\"}"
                    });
                Assert.True(seededParent.Decision.Success);
            }

            var founderDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = founderRef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentARef },
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = parentBRef }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.86660916,\"parent_a_similarity_score\":0.8854921,\"parent_b_similarity_score\":0.8477262}}"
                });
            Assert.True(founderDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", founderDecision.Decision.DecisionReason);

            var bootstrapDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = secondChildRef
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = founderRef },
                        new ProtoSpec.SpeciationParentRef { ArtifactRef = unknownParentRef }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.89079946,\"parent_a_similarity_score\":0.8942623,\"parent_b_similarity_score\":0.8873366}}"
                });

            Assert.True(bootstrapDecision.Decision.Success);
            Assert.Equal(founderDecision.Decision.SpeciesId, bootstrapDecision.Decision.SpeciesId);
            Assert.Equal("lineage_hysteresis_seed", bootstrapDecision.Decision.DecisionReason);

            using var metadata = JsonDocument.Parse(bootstrapDecision.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.Equal(founderDecision.Decision.SpeciesId, lineage.GetProperty("source_species_id").GetString());
            Assert.Equal("compatibility_assessment", lineage.GetProperty("assigned_species_similarity_source").GetString());
            Assert.False(lineage.GetProperty("assigned_species_compatibility_report_compatible").GetBoolean());
            Assert.Equal(
                "repro_spot_check_overlap_mismatch",
                lineage.GetProperty("assigned_species_compatibility_abort_reason").GetString());
            Assert.Equal(0.96d, lineage.GetProperty("lineage_assignment_similarity_score").GetDouble(), 3);
            Assert.Equal(0.96d, lineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
            Assert.True(lineage.GetProperty("split_proximity_to_dynamic_threshold").GetDouble() > 0.09d);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_LongSourceSpeciesId_PreservesDerivedHashSuffixUnderLengthCap()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.70d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0d));
        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            await WaitForEpochAsync(system, managerPid);
            var longSpeciesId = "species-" + new string('a', 110);
            var parentA = Guid.NewGuid();
            var parentB = Guid.NewGuid();
            var splitChildA = Guid.NewGuid();
            var splitChildB = Guid.NewGuid();

            foreach (var parent in new[] { parentA, parentB })
            {
                var seedDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                    managerPid,
                    new ProtoSpec.SpeciationAssignRequest
                    {
                        ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                        Candidate = new ProtoSpec.SpeciationCandidateRef
                        {
                            BrainId = parent.ToProtoUuid()
                        },
                        SpeciesId = longSpeciesId,
                        SpeciesDisplayName = "Species Alpha",
                        DecisionReason = "seed_parent_species",
                        DecisionMetadataJson = "{\"source\":\"seed\"}"
                    });
                Assert.True(seedDecision.Decision.Success);
            }

            var firstSplitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = splitChildA.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parentA.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50}}"
                });
            var secondSplitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = splitChildB.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = parentB.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.49,\"parent_a_similarity_score\":0.49}}"
                });

            Assert.True(firstSplitDecision.Decision.Success);
            Assert.True(secondSplitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", firstSplitDecision.Decision.DecisionReason);
            Assert.Equal("lineage_diverged_new_species", secondSplitDecision.Decision.DecisionReason);
            Assert.NotEqual(firstSplitDecision.Decision.SpeciesId, secondSplitDecision.Decision.SpeciesId);
            Assert.True(firstSplitDecision.Decision.SpeciesId.Length <= 96);
            Assert.True(secondSplitDecision.Decision.SpeciesId.Length <= 96);
            Assert.Matches("-branch-[0-9a-f]{12}$", firstSplitDecision.Decision.SpeciesId);
            Assert.Matches("-branch-[0-9a-f]{12}$", secondSplitDecision.Decision.SpeciesId);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_LowConsensusPairwiseMatch_RescuesRecentDerivedSpecies()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.90d,
            lineageSplitThreshold: 0.70d,
            parentConsensusThreshold: 0.70d,
            derivedSpeciesPrefix: "branch",
            lineageHysteresisMargin: 0d,
            lineageSplitGuardMargin: 0d,
            lineageMinParentMembershipsBeforeSplit: 1,
            lineageRealignParentMembershipWindow: 0,
            lineageRealignMatchMargin: 0d,
            lineageHindsightReassignCommitWindow: 0,
            lineageHindsightSimilarityMargin: 0.02d));
        var system = new ActorSystem();
        try
        {
            var reproPid = system.Root.Spawn(Props.FromProducer(
                () => new ReproFixedResponseProbe(CreateCompatibilityAssessmentResult(0.96f))));
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null,
                    reproductionManagerPid: reproPid)));

            await WaitForEpochAsync(system, managerPid);
            var sourceParent = Guid.NewGuid();
            var splitFounder = Guid.NewGuid();
            var mixedChild = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = sourceParent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var splitDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = splitFounder.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.50,\"parent_a_similarity_score\":0.50}}"
                });
            Assert.True(splitDecision.Decision.Success);
            Assert.Equal("lineage_diverged_new_species", splitDecision.Decision.DecisionReason);

            var rescuedDecision = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = mixedChild.ToProtoUuid()
                    },
                    Parents =
                    {
                        new ProtoSpec.SpeciationParentRef { BrainId = sourceParent.ToProtoUuid() },
                        new ProtoSpec.SpeciationParentRef { BrainId = splitFounder.ToProtoUuid() }
                    },
                    DecisionMetadataJson = "{\"lineage\":{\"lineage_similarity_score\":0.82,\"parent_a_similarity_score\":0.82,\"parent_b_similarity_score\":0.96}}"
                });

            Assert.True(rescuedDecision.Decision.Success);
            Assert.Equal(splitDecision.Decision.SpeciesId, rescuedDecision.Decision.SpeciesId);
            Assert.Equal("lineage_inherit_similarity_match", rescuedDecision.Decision.DecisionReason);

            using var metadata = JsonDocument.Parse(rescuedDecision.Decision.DecisionMetadataJson);
            var lineage = metadata.RootElement.GetProperty("lineage");
            Assert.Equal(splitDecision.Decision.SpeciesId, lineage.GetProperty("source_species_id").GetString());
            Assert.Equal("compatibility_assessment", lineage.GetProperty("assigned_species_similarity_source").GetString());
            Assert.Equal(0.96d, lineage.GetProperty("dominant_species_similarity_score").GetDouble(), 3);
            Assert.Equal(0.96d, lineage.GetProperty("source_species_similarity_score").GetDouble(), 3);
            Assert.Equal(0.96d, lineage.GetProperty("lineage_assignment_similarity_score").GetDouble(), 3);
            Assert.Equal(0.96d, lineage.GetProperty("intra_species_similarity_sample").GetDouble(), 3);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_ParallelRequests_RemainSingleWriterDeterministic()
    {
        using var speciationDb = new TempDatabaseScope("speciation.db");
        var runtimeConfig = CreateRuntimeConfig(CreateLineagePolicyConfigJson(
            lineageMatchThreshold: 0.80d,
            lineageSplitThreshold: 0.40d,
            parentConsensusThreshold: 0.50d,
            derivedSpeciesPrefix: "branch"));
        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

            await WaitForEpochAsync(system, managerPid);
            var parent = Guid.NewGuid();
            var child = Guid.NewGuid();

            var seedParent = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = parent.ToProtoUuid()
                    },
                    SpeciesId = "species-alpha",
                    SpeciesDisplayName = "Species Alpha",
                    DecisionReason = "seed_parent_species",
                    DecisionMetadataJson = "{\"source\":\"seed\"}"
                });
            Assert.True(seedParent.Decision.Success);

            var requests = Enumerable.Range(0, 32)
                .Select(async _ => await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                    managerPid,
                    new ProtoSpec.SpeciationAssignRequest
                    {
                        ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                        Candidate = new ProtoSpec.SpeciationCandidateRef
                        {
                            BrainId = child.ToProtoUuid()
                        },
                        Parents =
                        {
                            new ProtoSpec.SpeciationParentRef { BrainId = parent.ToProtoUuid() }
                        },
                        DecisionMetadataJson = "{\"report\":{\"similarity_score\":0.95}}"
                    }))
                .ToArray();

            var responses = await Task.WhenAll(requests);

            Assert.Equal(1, responses.Count(response => response.Decision.Created));
            Assert.All(responses, response =>
            {
                Assert.True(response.Decision.Success);
                Assert.False(response.Decision.ImmutableConflict);
                Assert.Equal("species-alpha", response.Decision.SpeciesId);
            });

            var history = await system.Root.RequestAsync<ProtoSpec.SpeciationListHistoryResponse>(
                managerPid,
                new ProtoSpec.SpeciationListHistoryRequest
                {
                    HasBrainId = true,
                    BrainId = child.ToProtoUuid()
                });
            Assert.Equal((uint)1, history.TotalRecords);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task Started_ReconcileKnownBrains_RecordsStartupReconcileTelemetry()
    {
        using var metrics = new MeterCollector(SpeciationTelemetry.MeterNameValue);
        using var activities = new ActivityCollector(SpeciationTelemetry.ActivitySource.Name);
        using var settingsDb = new TempDatabaseScope("settings-monitor.db");
        using var speciationDb = new TempDatabaseScope("speciation.db");

        var settingsStore = new SettingsMonitorStore(settingsDb.DatabasePath);
        await settingsStore.InitializeAsync();

        var system = new ActorSystem();
        try
        {
            var settingsPid = system.Root.Spawn(Props.FromProducer(() => new SettingsMonitorActor(settingsStore)));

            var knownBrains = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
            foreach (var brainId in knownBrains)
            {
                system.Root.Send(settingsPid, new BrainRegistered
                {
                    BrainId = brainId.ToProtoUuid(),
                    SpawnedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    State = "Active"
                });
            }

            await WaitForConditionAsync(
                async () =>
                {
                    var list = await system.Root.RequestAsync<BrainListResponse>(settingsPid, new BrainListRequest());
                    return list.Brains.Count >= knownBrains.Length;
                },
                TimeSpan.FromSeconds(5));

            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    CreateRuntimeConfig(),
                    settingsPid,
                    TimeSpan.FromSeconds(2))));

            await WaitForConditionAsync(
                async () =>
                {
                    var status = await system.Root.RequestAsync<SpeciationStatusResponse>(
                        managerPid,
                        new SpeciationStatusRequest());
                    return status.Status.MembershipCount == knownBrains.Length;
                },
                TimeSpan.FromSeconds(5));

            Assert.Equal(
                1,
                metrics.SumLong(
                    "nbn.speciation.startup.reconcile.total",
                    ("outcome", "completed"),
                    ("failure_reason", "none")));
            Assert.Equal(
                knownBrains.Length,
                metrics.SumLong(
                    "nbn.speciation.startup.reconcile.memberships.added",
                    ("outcome", "completed"),
                    ("failure_reason", "none")));

            var activity = Assert.Single(
                activities.CompletedActivities,
                candidate => candidate.OperationName == "speciation.startup.reconcile");
            Assert.Equal("completed", activity.GetTagItem("speciation.outcome")?.ToString());
            Assert.Equal(knownBrains.Length.ToString(), activity.GetTagItem("speciation.added_memberships")?.ToString());
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoAssign_Commit_RecordsDecisionTelemetry_And_StatusSnapshotMetrics()
    {
        using var metrics = new MeterCollector(SpeciationTelemetry.MeterNameValue);
        using var activities = new ActivityCollector(SpeciationTelemetry.ActivitySource.Name);
        using var speciationDb = new TempDatabaseScope("speciation.db");

        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    CreateRuntimeConfig(),
                    settingsPid: null)));

            await WaitForEpochAsync(system, managerPid);

            var response = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        BrainId = Guid.NewGuid().ToProtoUuid()
                    },
                    SpeciesId = "species-ops",
                    SpeciesDisplayName = "Species Ops",
                    DecisionReason = "operator_assign",
                    DecisionMetadataJson = "{\"source\":\"telemetry_test\"}"
                });

            Assert.True(response.Decision.Success);
            Assert.True(response.Decision.Created);
            Assert.True(response.Decision.Committed);

            var status = await system.Root.RequestAsync<ProtoSpec.SpeciationStatusResponse>(
                managerPid,
                new ProtoSpec.SpeciationStatusRequest());

            Assert.Equal((uint)1, status.Status.MembershipCount);
            Assert.Equal((uint)1, status.Status.SpeciesCount);

            Assert.Equal(
                1,
                metrics.SumLong(
                    "nbn.speciation.assignment.decisions",
                    ("operation", "assign"),
                    ("apply_mode", "commit"),
                    ("candidate_mode", "brain_id"),
                    ("decision_reason", "operator_assign"),
                    ("failure_reason", "none"),
                    ("success", "true"),
                    ("committed", "true")));
            Assert.True(
                metrics.CountDouble(
                    "nbn.speciation.assignment.duration.ms",
                    ("operation", "assign"),
                    ("apply_mode", "commit"),
                    ("failure_reason", "none")) >= 1);
            Assert.Equal(
                1,
                metrics.SumLong(
                    "nbn.speciation.status.membership_count",
                    ("source", "status"),
                    ("failure_reason", "none")));
            Assert.Equal(
                1,
                metrics.SumLong(
                    "nbn.speciation.status.species_count",
                    ("source", "status"),
                    ("failure_reason", "none")));

            var assignActivity = Assert.Single(
                activities.CompletedActivities,
                candidate => candidate.OperationName == "speciation.assign");
            Assert.Equal("commit", assignActivity.GetTagItem("speciation.apply_mode")?.ToString());
            Assert.Equal("operator_assign", assignActivity.GetTagItem("speciation.decision_reason")?.ToString());
            Assert.Equal("species-ops", assignActivity.GetTagItem("speciation.species_id")?.ToString());
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ProtoEpochLifecycle_RecordsTransitionTelemetry()
    {
        using var metrics = new MeterCollector(SpeciationTelemetry.MeterNameValue);
        using var activities = new ActivityCollector(SpeciationTelemetry.ActivitySource.Name);
        using var speciationDb = new TempDatabaseScope("speciation.db");

        var system = new ActorSystem();
        try
        {
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    CreateRuntimeConfig(),
                    settingsPid: null)));

            var firstEpoch = await WaitForEpochAsync(system, managerPid);

            var startNewEpoch = await system.Root.RequestAsync<ProtoSpec.SpeciationSetConfigResponse>(
                managerPid,
                new ProtoSpec.SpeciationSetConfigRequest
                {
                    StartNewEpoch = true,
                    Config = new ProtoSpec.SpeciationRuntimeConfig
                    {
                        PolicyVersion = "policy-v2",
                        ConfigSnapshotJson = "{\"mode\":\"epoch-two\"}",
                        DefaultSpeciesId = "default-species",
                        DefaultSpeciesDisplayName = "Default Species",
                        StartupReconcileDecisionReason = "startup_reconcile"
                    }
                });

            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, startNewEpoch.FailureReason);
            Assert.True(startNewEpoch.CurrentEpoch.EpochId > (ulong)firstEpoch.EpochId);

            var deleteEpoch = await system.Root.RequestAsync<ProtoSpec.SpeciationDeleteEpochResponse>(
                managerPid,
                new ProtoSpec.SpeciationDeleteEpochRequest
                {
                    EpochId = (ulong)firstEpoch.EpochId
                });

            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, deleteEpoch.FailureReason);
            Assert.True(deleteEpoch.Deleted);

            var resetAll = await system.Root.RequestAsync<ProtoSpec.SpeciationResetAllResponse>(
                managerPid,
                new ProtoSpec.SpeciationResetAllRequest());

            Assert.Equal(ProtoSpec.SpeciationFailureReason.SpeciationFailureNone, resetAll.FailureReason);

            Assert.Equal(
                1,
                metrics.SumLong(
                    "nbn.speciation.epoch.transition.total",
                    ("transition", "initialize"),
                    ("outcome", "completed"),
                    ("failure_reason", "none")));
            Assert.Equal(
                1,
                metrics.SumLong(
                    "nbn.speciation.epoch.transition.total",
                    ("transition", "start_new_epoch"),
                    ("outcome", "completed"),
                    ("failure_reason", "none")));
            Assert.Equal(
                1,
                metrics.SumLong(
                    "nbn.speciation.epoch.transition.total",
                    ("transition", "delete_epoch"),
                    ("outcome", "completed"),
                    ("failure_reason", "none")));
            Assert.Equal(
                1,
                metrics.SumLong(
                    "nbn.speciation.epoch.transition.total",
                    ("transition", "reset_all"),
                    ("outcome", "completed"),
                    ("failure_reason", "none")));
            Assert.True(
                metrics.CountLong(
                    "nbn.speciation.status.membership_count",
                    ("source", "start_new_epoch"),
                    ("failure_reason", "none")) >= 1);
            Assert.True(
                metrics.CountLong(
                    "nbn.speciation.status.membership_count",
                    ("source", "reset_all"),
                    ("failure_reason", "none")) >= 1);

            Assert.Contains(
                activities.CompletedActivities,
                candidate => candidate.OperationName == "speciation.epoch.transition"
                    && string.Equals(candidate.GetTagItem("speciation.transition")?.ToString(), "reset_all", StringComparison.Ordinal)
                    && string.Equals(candidate.GetTagItem("speciation.outcome")?.ToString(), "completed", StringComparison.Ordinal));
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    private static SpeciationRuntimeConfig CreateRuntimeConfig(string? configSnapshotJson = null)
        => new(
            PolicyVersion: "policy-v1",
            ConfigSnapshotJson: configSnapshotJson ?? "{\"mode\":\"default\"}",
            DefaultSpeciesId: "default-species",
            DefaultSpeciesDisplayName: "Default Species",
            StartupReconcileDecisionReason: "startup_reconcile");

    private static string CreateLineagePolicyConfigJson(
        double lineageMatchThreshold,
        double lineageSplitThreshold,
        double parentConsensusThreshold,
        string derivedSpeciesPrefix,
        double lineageHysteresisMargin = 0.04d,
        double lineageSplitGuardMargin = 0d,
        int lineageMinParentMembershipsBeforeSplit = 1,
        int lineageRealignParentMembershipWindow = 0,
        double lineageRealignMatchMargin = 0d,
        int lineageHindsightReassignCommitWindow = 6,
        double lineageHindsightSimilarityMargin = 0.015d)
    {
        return $$"""
        {
          "assignment_policy": {
            "lineage_match_threshold": {{lineageMatchThreshold.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture)}},
            "lineage_split_threshold": {{lineageSplitThreshold.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture)}},
            "parent_consensus_threshold": {{parentConsensusThreshold.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture)}},
            "lineage_hysteresis_margin": {{lineageHysteresisMargin.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture)}},
            "lineage_split_guard_margin": {{lineageSplitGuardMargin.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture)}},
            "lineage_min_parent_memberships_before_split": {{lineageMinParentMembershipsBeforeSplit}},
            "lineage_realign_parent_membership_window": {{lineageRealignParentMembershipWindow}},
            "lineage_realign_match_margin": {{lineageRealignMatchMargin.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture)}},
            "lineage_hindsight_reassign_commit_window": {{lineageHindsightReassignCommitWindow}},
            "lineage_hindsight_similarity_margin": {{lineageHindsightSimilarityMargin.ToString("0.000", System.Globalization.CultureInfo.InvariantCulture)}},
            "create_derived_species_on_divergence": true,
            "derived_species_prefix": "{{derivedSpeciesPrefix}}"
          }
        }
        """;
    }

    private static async Task<SpeciationEpochInfo> WaitForEpochAsync(
        ActorSystem system,
        PID managerPid,
        TimeSpan? timeout = null)
    {
        var max = timeout ?? TimeSpan.FromSeconds(5);
        SpeciationEpochInfo? epoch = null;
        await WaitForConditionAsync(
            async () =>
            {
                var response = await system.Root.RequestAsync<SpeciationGetCurrentEpochResponse>(
                    managerPid,
                    new SpeciationGetCurrentEpochRequest());
                if (response.Epoch.EpochId <= 0)
                {
                    return false;
                }

                epoch = response.Epoch;
                return true;
            },
            max);

        return epoch!;
    }

    private static async Task WaitForConditionAsync(
        Func<Task<bool>> predicate,
        TimeSpan timeout)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < timeout)
        {
            if (await predicate())
            {
                return;
            }

            await Task.Delay(25);
        }

        throw new TimeoutException($"Condition was not satisfied within {timeout}.");
    }

    private static Repro.ReproduceResult CreateCompatibilityAssessmentResult(float similarityScore)
    {
        return new Repro.ReproduceResult
        {
            Report = new Repro.SimilarityReport
            {
                Compatible = true,
                AbortReason = string.Empty,
                SimilarityScore = similarityScore
            },
            Spawned = false
        };
    }

    private static Repro.ReproduceResult CreateIncompatibleCompatibilityAssessmentResult(
        float similarityScore,
        string abortReason)
    {
        return new Repro.ReproduceResult
        {
            Report = new Repro.SimilarityReport
            {
                Compatible = false,
                AbortReason = abortReason,
                SimilarityScore = similarityScore
            },
            Spawned = false
        };
    }

    private sealed record CapturedCompatibilityConfig(
        float MaxRegionSpanDiffRatio,
        float MaxFunctionHistDistance,
        float MaxConnectivityHistDistance,
        Repro.SpawnChildPolicy SpawnChild);

    private sealed class ReproFixedResponseProbe : IActor
    {
        private readonly Repro.ReproduceResult _response;

        public ReproFixedResponseProbe(Repro.ReproduceResult response)
        {
            _response = response;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Repro.ReproduceByBrainIdsRequest:
                case Repro.ReproduceByArtifactsRequest:
                case Repro.AssessCompatibilityByBrainIdsRequest:
                case Repro.AssessCompatibilityByArtifactsRequest:
                    context.Respond(_response.Clone());
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class ReproCaptureCompatibilityConfigProbe : IActor
    {
        private readonly Repro.ReproduceResult _response;
        private readonly TaskCompletionSource<CapturedCompatibilityConfig> _capturedConfig;

        public ReproCaptureCompatibilityConfigProbe(
            Repro.ReproduceResult response,
            TaskCompletionSource<CapturedCompatibilityConfig> capturedConfig)
        {
            _response = response;
            _capturedConfig = capturedConfig;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Repro.AssessCompatibilityByBrainIdsRequest request:
                    Capture(request.Config);
                    context.Respond(_response.Clone());
                    break;
                case Repro.AssessCompatibilityByArtifactsRequest request:
                    Capture(request.Config);
                    context.Respond(_response.Clone());
                    break;
                case Repro.ReproduceByBrainIdsRequest:
                case Repro.ReproduceByArtifactsRequest:
                    context.Respond(_response.Clone());
                    break;
            }

            return Task.CompletedTask;
        }

        private void Capture(Repro.ReproduceConfig? config)
        {
            if (config is null)
            {
                return;
            }

            _capturedConfig.TrySetResult(new CapturedCompatibilityConfig(
                config.MaxRegionSpanDiffRatio,
                config.MaxFunctionHistDistance,
                config.MaxConnectivityHistDistance,
                config.SpawnChild));
        }
    }

    private sealed class ReproCountingResponseProbe : IActor
    {
        private readonly Repro.ReproduceResult _response;
        private int _compatibilityRequestCount;

        public ReproCountingResponseProbe(Repro.ReproduceResult response)
        {
            _response = response;
        }

        public int CompatibilityRequestCount => Volatile.Read(ref _compatibilityRequestCount);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Repro.AssessCompatibilityByBrainIdsRequest:
                case Repro.AssessCompatibilityByArtifactsRequest:
                    Interlocked.Increment(ref _compatibilityRequestCount);
                    context.Respond(_response.Clone());
                    break;
                case Repro.ReproduceByBrainIdsRequest:
                case Repro.ReproduceByArtifactsRequest:
                    context.Respond(_response.Clone());
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class BlockingCompatibilityProbe : IActor
    {
        private readonly Repro.ReproduceResult _response;
        private readonly Task _releaseTask;
        private int _compatibilityRequestCount;

        public BlockingCompatibilityProbe(Repro.ReproduceResult response, Task releaseTask)
        {
            _response = response;
            _releaseTask = releaseTask;
        }

        public int CompatibilityRequestCount => Volatile.Read(ref _compatibilityRequestCount);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Repro.AssessCompatibilityByBrainIdsRequest:
                case Repro.AssessCompatibilityByArtifactsRequest:
                    Interlocked.Increment(ref _compatibilityRequestCount);
                    context.ReenterAfter(_releaseTask, _ =>
                    {
                        context.Respond(_response.Clone());
                        return Task.CompletedTask;
                    });
                    break;
                case Repro.ReproduceByBrainIdsRequest:
                case Repro.ReproduceByArtifactsRequest:
                    context.Respond(_response.Clone());
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class MutableIoGatewayProbe : IActor
    {
        private readonly Dictionary<Guid, ProtoIo.BrainInfo> _brainInfo;

        public MutableIoGatewayProbe(IReadOnlyDictionary<Guid, ProtoIo.BrainInfo>? brainInfo = null)
        {
            _brainInfo = brainInfo?.ToDictionary(
                static pair => pair.Key,
                static pair => pair.Value.Clone()) ?? new Dictionary<Guid, ProtoIo.BrainInfo>();
        }

        public void Remove(Guid brainId)
        {
            _brainInfo.Remove(brainId);
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is not ProtoIo.BrainInfoRequest request
                || !request.BrainId.TryToGuid(out var brainId)
                || !_brainInfo.TryGetValue(brainId, out var info))
            {
                context.Respond(new ProtoIo.BrainInfo());
                return Task.CompletedTask;
            }

            context.Respond(info.Clone());
            return Task.CompletedTask;
        }
    }

    private sealed class ReproArtifactsOnlyCompatibilityProbe : IActor
    {
        private readonly Repro.ReproduceResult _artifactResponse;
        private int _artifactCompatibilityRequestCount;
        private int _brainIdCompatibilityRequestCount;

        public ReproArtifactsOnlyCompatibilityProbe(Repro.ReproduceResult artifactResponse)
        {
            _artifactResponse = artifactResponse;
        }

        public int ArtifactCompatibilityRequestCount => Volatile.Read(ref _artifactCompatibilityRequestCount);

        public int BrainIdCompatibilityRequestCount => Volatile.Read(ref _brainIdCompatibilityRequestCount);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Repro.AssessCompatibilityByArtifactsRequest:
                    Interlocked.Increment(ref _artifactCompatibilityRequestCount);
                    context.Respond(_artifactResponse.Clone());
                    break;
                case Repro.AssessCompatibilityByBrainIdsRequest:
                    Interlocked.Increment(ref _brainIdCompatibilityRequestCount);
                    context.Respond(CreateIncompatibleCompatibilityAssessmentResult(
                        0f,
                        "repro_parent_b_brain_not_found"));
                    break;
                case Repro.ReproduceByBrainIdsRequest:
                case Repro.ReproduceByArtifactsRequest:
                    context.Respond(_artifactResponse.Clone());
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class TempDatabaseScope : IDisposable
    {
        private readonly string _directoryPath;

        public TempDatabaseScope(string databaseFileName)
        {
            _directoryPath = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directoryPath);
            DatabasePath = Path.Combine(_directoryPath, databaseFileName);
        }

        public string DatabasePath { get; }

        public void Dispose()
        {
            try
            {
                if (Directory.Exists(_directoryPath))
                {
                    Directory.Delete(_directoryPath, recursive: true);
                }
            }
            catch
            {
            }
        }
    }
}
