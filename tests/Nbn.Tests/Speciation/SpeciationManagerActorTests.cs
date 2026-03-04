using System.Diagnostics;
using System.Text.Json;
using Nbn.Proto.Settings;
using Nbn.Runtime.SettingsMonitor;
using Nbn.Runtime.Speciation;
using Nbn.Shared;
using Proto;
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
                Assert.Equal(runtimeConfig.DefaultSpeciesId, membershipByBrain[brainId].SpeciesId);
                Assert.Equal("startup_reconcile", membershipByBrain[brainId].DecisionReason);
            }
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
            Assert.True((long)reset.CurrentEpoch.EpochId > firstEpoch.EpochId);
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
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

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
    public async Task ProtoBatch_Commit_RespectsPerItemApplyMode_And_CandidateValidation()
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
    public async Task ProtoAssign_Commit_ArtifactRef_PersistsMembership_And_IsImmutableWithinEpoch()
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
            var artifactRef = new string('b', 64).ToArtifactRef(321, "application/x-nbn", "artifact-store");

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

            var secondAssign = await system.Root.RequestAsync<ProtoSpec.SpeciationAssignResponse>(
                managerPid,
                new ProtoSpec.SpeciationAssignRequest
                {
                    ApplyMode = ProtoSpec.SpeciationApplyMode.Commit,
                    Candidate = new ProtoSpec.SpeciationCandidateRef
                    {
                        ArtifactRef = artifactRef
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
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

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
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

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
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

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
                    DecisionMetadataJson = "{\"report\":{\"similarity_score\":0.95}}"
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
                    DecisionMetadataJson = "{\"report\":{\"similarity_score\":0.60}}"
                });

            Assert.True(hysteresisDecision.Decision.Success);
            Assert.True(hysteresisDecision.Decision.Created);
            Assert.Equal("species-alpha", hysteresisDecision.Decision.SpeciesId);
            Assert.Equal("lineage_hysteresis_hold", hysteresisDecision.Decision.DecisionReason);

            using var metadata = JsonDocument.Parse(hysteresisDecision.Decision.DecisionMetadataJson);
            Assert.Equal("hysteresis_hold", metadata.RootElement.GetProperty("assignment_strategy").GetString());
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
            var managerPid = system.Root.Spawn(Props.FromProducer(
                () => new SpeciationManagerActor(
                    new SpeciationStore(speciationDb.DatabasePath),
                    runtimeConfig,
                    settingsPid: null)));

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
        string derivedSpeciesPrefix)
    {
        return $$"""
        {
          "assignment_policy": {
            "lineage_match_threshold": {{lineageMatchThreshold.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture)}},
            "lineage_split_threshold": {{lineageSplitThreshold.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture)}},
            "parent_consensus_threshold": {{parentConsensusThreshold.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture)}},
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
