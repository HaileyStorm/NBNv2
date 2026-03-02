using Dapper;
using Microsoft.Data.Sqlite;
using Nbn.Runtime.Speciation;

namespace Nbn.Tests.Speciation;

public sealed class SpeciationStoreTests
{
    [Fact]
    public async Task InitializeAsync_CreatesRequiredSpeciationSchema()
    {
        using var db = new TempDatabaseScope();
        var store = new SpeciationStore(db.DatabasePath);
        await store.InitializeAsync();

        await using var connection = new SqliteConnection(
            new SqliteConnectionStringBuilder
            {
                DataSource = db.DatabasePath,
                Mode = SqliteOpenMode.ReadWrite
            }.ToString());
        await connection.OpenAsync();

        var tables = (await connection.QueryAsync<string>(
                "SELECT name FROM sqlite_master WHERE type = 'table';"))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.Contains("taxonomy_epochs", tables);
        Assert.Contains("taxonomy_config_snapshots", tables);
        Assert.Contains("species", tables);
        Assert.Contains("speciation_decisions", tables);
        Assert.Contains("species_membership", tables);
        Assert.Contains("lineage_edges", tables);

        var membershipColumns = await GetColumnNamesAsync(connection, "species_membership");
        Assert.Contains("epoch_id", membershipColumns);
        Assert.Contains("brain_id", membershipColumns);
        Assert.Contains("species_id", membershipColumns);
        Assert.Contains("assigned_ms", membershipColumns);
        Assert.Contains("decision_id", membershipColumns);

        var decisionColumns = await GetColumnNamesAsync(connection, "speciation_decisions");
        Assert.Contains("epoch_id", decisionColumns);
        Assert.Contains("policy_version", decisionColumns);
        Assert.Contains("decision_metadata_json", decisionColumns);
        Assert.Contains("decision_reason", decisionColumns);

        var lineageColumns = await GetColumnNamesAsync(connection, "lineage_edges");
        Assert.Contains("epoch_id", lineageColumns);
        Assert.Contains("parent_brain_id", lineageColumns);
        Assert.Contains("child_brain_id", lineageColumns);

        var snapshotColumns = await GetColumnNamesAsync(connection, "taxonomy_config_snapshots");
        Assert.Contains("epoch_id", snapshotColumns);
        Assert.Contains("policy_version", snapshotColumns);
        Assert.Contains("config_snapshot_json", snapshotColumns);
    }

    [Fact]
    public async Task TryAssignMembershipAsync_EnforcesEpochImmutability()
    {
        using var db = new TempDatabaseScope();
        var store = new SpeciationStore(db.DatabasePath);
        await store.InitializeAsync();

        var runtimeConfig = CreateRuntimeConfig();
        var epoch = await store.EnsureCurrentEpochAsync(runtimeConfig, createdMs: 100);
        var brainId = Guid.NewGuid();

        var first = await store.TryAssignMembershipAsync(
            epoch.EpochId,
            new SpeciationAssignment(
                BrainId: brainId,
                SpeciesId: "species-a",
                SpeciesDisplayName: "Species A",
                PolicyVersion: "policy-v1",
                DecisionReason: "manual_assign",
                DecisionMetadataJson: "{\"reason\":\"first\"}"),
            decisionTimeMs: 200);

        var second = await store.TryAssignMembershipAsync(
            epoch.EpochId,
            new SpeciationAssignment(
                BrainId: brainId,
                SpeciesId: "species-b",
                SpeciesDisplayName: "Species B",
                PolicyVersion: "policy-v1",
                DecisionReason: "manual_reassign",
                DecisionMetadataJson: "{\"reason\":\"second\"}"),
            decisionTimeMs: 300);

        Assert.True(first.Created);
        Assert.False(first.ImmutableConflict);
        Assert.False(second.Created);
        Assert.True(second.ImmutableConflict);
        Assert.Equal("species-a", second.Membership.SpeciesId);
        Assert.Equal("Species A", second.Membership.SpeciesDisplayName);

        var members = await store.ListMembershipsAsync(epoch.EpochId);
        Assert.Single(members);
        Assert.Equal("species-a", members[0].SpeciesId);
    }

    [Fact]
    public async Task ResetEpochAsync_CreatesNewEpochWithoutRewritingHistoricalMembership()
    {
        using var db = new TempDatabaseScope();
        var store = new SpeciationStore(db.DatabasePath);
        await store.InitializeAsync();

        var runtimeConfig = CreateRuntimeConfig();
        var firstEpoch = await store.EnsureCurrentEpochAsync(runtimeConfig, createdMs: 1000);
        var brainId = Guid.NewGuid();

        await store.TryAssignMembershipAsync(
            firstEpoch.EpochId,
            new SpeciationAssignment(
                brainId,
                "species-alpha",
                "Species Alpha",
                "policy-v1",
                "manual_assign",
                "{\"epoch\":1}"),
            decisionTimeMs: 1100);

        var secondEpoch = await store.ResetEpochAsync(
            runtimeConfig with
            {
                PolicyVersion = "policy-v2",
                ConfigSnapshotJson = "{\"epoch\":2}"
            },
            resetTimeMs: 2000);

        Assert.True(secondEpoch.EpochId > firstEpoch.EpochId);

        var epochOneMembership = await store.ListMembershipsAsync(firstEpoch.EpochId);
        var epochTwoMembership = await store.ListMembershipsAsync(secondEpoch.EpochId);

        Assert.Single(epochOneMembership);
        Assert.Empty(epochTwoMembership);
        Assert.Equal("species-alpha", epochOneMembership[0].SpeciesId);

        var secondAssign = await store.TryAssignMembershipAsync(
            secondEpoch.EpochId,
            new SpeciationAssignment(
                brainId,
                "species-beta",
                "Species Beta",
                "policy-v2",
                "manual_assign",
                "{\"epoch\":2}"),
            decisionTimeMs: 2200);

        Assert.True(secondAssign.Created);

        var epochOneAfter = await store.ListMembershipsAsync(firstEpoch.EpochId);
        Assert.Single(epochOneAfter);
        Assert.Equal("species-alpha", epochOneAfter[0].SpeciesId);
    }

    [Fact]
    public async Task ReconcileMissingMembershipsAsync_AssignsMissingBrainsDeterministically()
    {
        using var db = new TempDatabaseScope();
        var store = new SpeciationStore(db.DatabasePath);
        await store.InitializeAsync();

        var runtimeConfig = CreateRuntimeConfig();
        var epoch = await store.EnsureCurrentEpochAsync(runtimeConfig, createdMs: 10);
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();
        var brainC = Guid.NewGuid();

        await store.TryAssignMembershipAsync(
            epoch.EpochId,
            new SpeciationAssignment(
                brainB,
                "custom-species",
                "Custom Species",
                "policy-v1",
                "manual_assign",
                "{\"source\":\"manual\"}"),
            decisionTimeMs: 20);

        var reconcile = await store.ReconcileMissingMembershipsAsync(
            epoch.EpochId,
            new[] { brainC, brainA, brainB, brainC, Guid.Empty },
            runtimeConfig,
            decisionMetadataJson: "{\"source\":\"test_reconcile\"}",
            decisionTimeMs: 30);

        Assert.Equal(epoch.EpochId, reconcile.EpochId);
        Assert.Equal(2, reconcile.AddedMemberships);
        Assert.Equal(1, reconcile.ExistingMemberships);
        var expectedAdded = new[] { brainA, brainC }.OrderBy(static id => id).ToArray();
        Assert.Equal(expectedAdded, reconcile.AddedBrainIds);

        var memberships = (await store.ListMembershipsAsync(epoch.EpochId))
            .ToDictionary(static member => member.BrainId, static member => member);
        Assert.Equal(3, memberships.Count);
        Assert.Equal("custom-species", memberships[brainB].SpeciesId);
        Assert.Equal(runtimeConfig.DefaultSpeciesId, memberships[brainA].SpeciesId);
        Assert.Equal(runtimeConfig.DefaultSpeciesId, memberships[brainC].SpeciesId);
    }

    private static SpeciationRuntimeConfig CreateRuntimeConfig()
        => new(
            PolicyVersion: "policy-v1",
            ConfigSnapshotJson: "{\"mode\":\"baseline\"}",
            DefaultSpeciesId: "default-species",
            DefaultSpeciesDisplayName: "Default Species",
            StartupReconcileDecisionReason: "startup_reconcile");

    private static async Task<HashSet<string>> GetColumnNamesAsync(SqliteConnection connection, string tableName)
    {
        var rows = await connection.QueryAsync<TableInfoRow>($"PRAGMA table_info({tableName});");
        return rows
            .Select(static row => row.Name)
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    private sealed class TableInfoRow
    {
        public string Name { get; set; } = string.Empty;
    }

    private sealed class TempDatabaseScope : IDisposable
    {
        private readonly string _directoryPath;

        public TempDatabaseScope()
        {
            _directoryPath = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directoryPath);
            DatabasePath = Path.Combine(_directoryPath, "speciation.db");
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
