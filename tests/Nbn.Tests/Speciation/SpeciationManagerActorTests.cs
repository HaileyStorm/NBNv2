using System.Diagnostics;
using Nbn.Proto.Settings;
using Nbn.Runtime.SettingsMonitor;
using Nbn.Runtime.Speciation;
using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

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

    private static SpeciationRuntimeConfig CreateRuntimeConfig()
        => new(
            PolicyVersion: "policy-v1",
            ConfigSnapshotJson: "{\"mode\":\"default\"}",
            DefaultSpeciesId: "default-species",
            DefaultSpeciesDisplayName: "Default Species",
            StartupReconcileDecisionReason: "startup_reconcile");

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
