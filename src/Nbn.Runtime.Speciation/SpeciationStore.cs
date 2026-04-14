using Dapper;
using Microsoft.Data.Sqlite;

namespace Nbn.Runtime.Speciation;

/// <summary>
/// Persists speciation epochs, memberships, lineage edges, and configuration snapshots.
/// </summary>
public sealed partial class SpeciationStore
{
    static SpeciationStore()
    {
        RegisterTypeHandlers();
    }

    private const string PragmaSql = """
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;
    PRAGMA foreign_keys=ON;
    PRAGMA busy_timeout=5000;
    PRAGMA wal_autocheckpoint=1000;
    PRAGMA journal_size_limit=67108864;
    """;

    private const string CreateSchemaSql = """
CREATE TABLE IF NOT EXISTS taxonomy_epochs (
    epoch_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS taxonomy_config_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    epoch_id INTEGER NOT NULL,
    policy_version TEXT NOT NULL,
    config_snapshot_json TEXT NOT NULL,
    captured_ms INTEGER NOT NULL,
    FOREIGN KEY (epoch_id) REFERENCES taxonomy_epochs(epoch_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS species (
    epoch_id INTEGER NOT NULL,
    species_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    PRIMARY KEY (epoch_id, species_id),
    FOREIGN KEY (epoch_id) REFERENCES taxonomy_epochs(epoch_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS speciation_decisions (
    decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    epoch_id INTEGER NOT NULL,
    brain_id TEXT NOT NULL,
    species_id TEXT NOT NULL,
    decided_ms INTEGER NOT NULL,
    policy_version TEXT NOT NULL,
    decision_reason TEXT NOT NULL,
    decision_metadata_json TEXT NOT NULL,
    source_brain_id TEXT NULL,
    source_artifact_ref TEXT NULL,
    FOREIGN KEY (epoch_id) REFERENCES taxonomy_epochs(epoch_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS species_membership (
    epoch_id INTEGER NOT NULL,
    brain_id TEXT NOT NULL,
    species_id TEXT NOT NULL,
    assigned_ms INTEGER NOT NULL,
    decision_id INTEGER NOT NULL,
    PRIMARY KEY (epoch_id, brain_id),
    FOREIGN KEY (epoch_id, species_id) REFERENCES species(epoch_id, species_id) ON DELETE RESTRICT,
    FOREIGN KEY (decision_id) REFERENCES speciation_decisions(decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS lineage_edges (
    edge_id INTEGER PRIMARY KEY AUTOINCREMENT,
    epoch_id INTEGER NOT NULL,
    parent_brain_id TEXT NOT NULL,
    child_brain_id TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    FOREIGN KEY (epoch_id) REFERENCES taxonomy_epochs(epoch_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_taxonomy_config_epoch_snapshot
    ON taxonomy_config_snapshots(epoch_id, snapshot_id);
CREATE INDEX IF NOT EXISTS idx_species_membership_epoch_species
    ON species_membership(epoch_id, species_id);
CREATE INDEX IF NOT EXISTS idx_species_membership_epoch_assigned_brain
    ON species_membership(epoch_id, assigned_ms, brain_id);
CREATE INDEX IF NOT EXISTS idx_species_membership_epoch_species_assigned_brain
    ON species_membership(epoch_id, species_id, assigned_ms, brain_id);
CREATE INDEX IF NOT EXISTS idx_speciation_decisions_epoch_brain
    ON speciation_decisions(epoch_id, brain_id);
CREATE INDEX IF NOT EXISTS idx_lineage_edges_epoch_child
    ON lineage_edges(epoch_id, child_brain_id);
CREATE INDEX IF NOT EXISTS idx_lineage_edges_epoch_parent
    ON lineage_edges(epoch_id, parent_brain_id);
""";

    private const string SelectLatestEpochSql = """
SELECT
    epoch_id AS EpochId,
    created_ms AS CreatedMs
FROM taxonomy_epochs
ORDER BY epoch_id DESC
LIMIT 1;
""";

    private const string SelectLatestEpochConfigSql = """
SELECT
    policy_version AS PolicyVersion,
    config_snapshot_json AS ConfigSnapshotJson
FROM taxonomy_config_snapshots
WHERE epoch_id = @epoch_id
ORDER BY snapshot_id DESC
LIMIT 1;
""";

    private const string InsertEpochSql = """
INSERT INTO taxonomy_epochs (created_ms)
VALUES (@created_ms);
SELECT CAST(last_insert_rowid() AS INTEGER);
""";

    private const string CountEpochConfigSnapshotsSql = """
SELECT COUNT(1)
FROM taxonomy_config_snapshots
WHERE epoch_id = @epoch_id;
""";

    private const string InsertConfigSnapshotSql = """
INSERT INTO taxonomy_config_snapshots (
    epoch_id,
    policy_version,
    config_snapshot_json,
    captured_ms
) VALUES (
    @epoch_id,
    @policy_version,
    @config_snapshot_json,
    @captured_ms
);
""";

    private const string UpsertSpeciesSql = """
INSERT INTO species (epoch_id, species_id, display_name, created_ms)
VALUES (@epoch_id, @species_id, @display_name, @created_ms)
ON CONFLICT(epoch_id, species_id) DO UPDATE SET
    display_name = excluded.display_name
WHERE species.display_name <> excluded.display_name;
""";

    private const string InsertDecisionSql = """
INSERT INTO speciation_decisions (
    epoch_id,
    brain_id,
    species_id,
    decided_ms,
    policy_version,
    decision_reason,
    decision_metadata_json,
    source_brain_id,
    source_artifact_ref
) VALUES (
    @epoch_id,
    @brain_id,
    @species_id,
    @decided_ms,
    @policy_version,
    @decision_reason,
    @decision_metadata_json,
    @source_brain_id,
    @source_artifact_ref
);
SELECT CAST(last_insert_rowid() AS INTEGER);
""";

    private const string InsertMembershipSql = """
INSERT INTO species_membership (
    epoch_id,
    brain_id,
    species_id,
    assigned_ms,
    decision_id
) VALUES (
    @epoch_id,
    @brain_id,
    @species_id,
    @assigned_ms,
    @decision_id
);
""";

    private const string SelectMembershipSql = """
SELECT
    m.epoch_id AS EpochId,
    m.brain_id AS BrainId,
    m.species_id AS SpeciesId,
    s.display_name AS SpeciesDisplayName,
    m.assigned_ms AS AssignedMs,
    d.policy_version AS PolicyVersion,
    d.decision_reason AS DecisionReason,
    d.decision_metadata_json AS DecisionMetadataJson,
    d.source_brain_id AS SourceBrainId,
    d.source_artifact_ref AS SourceArtifactRef,
    d.decision_id AS DecisionId
FROM species_membership AS m
JOIN species AS s
    ON s.epoch_id = m.epoch_id
   AND s.species_id = m.species_id
JOIN speciation_decisions AS d
    ON d.decision_id = m.decision_id
WHERE m.epoch_id = @epoch_id
  AND m.brain_id = @brain_id;
""";

    private const string ListMembershipsSql = """
SELECT
    m.epoch_id AS EpochId,
    m.brain_id AS BrainId,
    m.species_id AS SpeciesId,
    s.display_name AS SpeciesDisplayName,
    m.assigned_ms AS AssignedMs,
    d.policy_version AS PolicyVersion,
    d.decision_reason AS DecisionReason,
    d.decision_metadata_json AS DecisionMetadataJson,
    d.source_brain_id AS SourceBrainId,
    d.source_artifact_ref AS SourceArtifactRef,
    d.decision_id AS DecisionId
FROM species_membership AS m
JOIN species AS s
    ON s.epoch_id = m.epoch_id
   AND s.species_id = m.species_id
JOIN speciation_decisions AS d
    ON d.decision_id = m.decision_id
WHERE (@epoch_id IS NULL OR m.epoch_id = @epoch_id)
ORDER BY m.epoch_id, m.brain_id
LIMIT @limit OFFSET @offset;
""";

    private const string CountHistorySql = """
SELECT COUNT(*)
FROM species_membership AS m
WHERE (@epoch_id IS NULL OR m.epoch_id = @epoch_id)
  AND (@brain_id IS NULL OR m.brain_id = @brain_id);
""";

    private const string ListHistoryPageSql = """
SELECT
    m.epoch_id AS EpochId,
    m.brain_id AS BrainId,
    m.species_id AS SpeciesId,
    s.display_name AS SpeciesDisplayName,
    m.assigned_ms AS AssignedMs,
    d.policy_version AS PolicyVersion,
    d.decision_reason AS DecisionReason,
    d.decision_metadata_json AS DecisionMetadataJson,
    d.source_brain_id AS SourceBrainId,
    d.source_artifact_ref AS SourceArtifactRef,
    d.decision_id AS DecisionId
FROM species_membership AS m
JOIN species AS s
    ON s.epoch_id = m.epoch_id
   AND s.species_id = m.species_id
JOIN speciation_decisions AS d
    ON d.decision_id = m.decision_id
WHERE (@epoch_id IS NULL OR m.epoch_id = @epoch_id)
  AND (@brain_id IS NULL OR m.brain_id = @brain_id)
ORDER BY m.epoch_id, m.assigned_ms, m.brain_id
LIMIT @limit OFFSET @offset;
""";

    private const string ListRecentMembershipsForSpeciesSql = """
SELECT
    m.epoch_id AS EpochId,
    m.brain_id AS BrainId,
    m.species_id AS SpeciesId,
    s.display_name AS SpeciesDisplayName,
    m.assigned_ms AS AssignedMs,
    d.policy_version AS PolicyVersion,
    d.decision_reason AS DecisionReason,
    d.decision_metadata_json AS DecisionMetadataJson,
    d.source_brain_id AS SourceBrainId,
    d.source_artifact_ref AS SourceArtifactRef,
    d.decision_id AS DecisionId
FROM species_membership AS m
JOIN species AS s
    ON s.epoch_id = m.epoch_id
   AND s.species_id = m.species_id
JOIN speciation_decisions AS d
    ON d.decision_id = m.decision_id
WHERE m.epoch_id = @epoch_id
  AND m.species_id = @species_id
  AND m.assigned_ms <= @max_assigned_ms
ORDER BY m.assigned_ms DESC, m.brain_id DESC
LIMIT @limit;
""";

    private const string ListEarliestMembershipsForSpeciesSql = """
SELECT
    m.epoch_id AS EpochId,
    m.brain_id AS BrainId,
    m.species_id AS SpeciesId,
    s.display_name AS SpeciesDisplayName,
    m.assigned_ms AS AssignedMs,
    d.policy_version AS PolicyVersion,
    d.decision_reason AS DecisionReason,
    d.decision_metadata_json AS DecisionMetadataJson,
    d.source_brain_id AS SourceBrainId,
    d.source_artifact_ref AS SourceArtifactRef,
    d.decision_id AS DecisionId
FROM species_membership AS m
JOIN species AS s
    ON s.epoch_id = m.epoch_id
   AND s.species_id = m.species_id
JOIN speciation_decisions AS d
    ON d.decision_id = m.decision_id
WHERE m.epoch_id = @epoch_id
  AND m.species_id = @species_id
ORDER BY m.assigned_ms ASC, m.brain_id ASC
LIMIT @limit;
""";

    private const string UpdateMembershipReassignSql = """
UPDATE species_membership
SET
    species_id = @species_id,
    assigned_ms = @assigned_ms,
    decision_id = @decision_id
WHERE epoch_id = @epoch_id
  AND brain_id = @brain_id
  AND species_id = @expected_species_id;
""";

    private const string InsertLineageEdgeSql = """
INSERT INTO lineage_edges (
    epoch_id,
    parent_brain_id,
    child_brain_id,
    metadata_json,
    created_ms
) VALUES (
    @epoch_id,
    @parent_brain_id,
    @child_brain_id,
    @metadata_json,
    @created_ms
);
""";

    private const string SelectLatestChildMembershipForParentSql = """
SELECT
    m.epoch_id AS EpochId,
    m.brain_id AS BrainId,
    m.species_id AS SpeciesId,
    s.display_name AS SpeciesDisplayName,
    m.assigned_ms AS AssignedMs,
    d.policy_version AS PolicyVersion,
    d.decision_reason AS DecisionReason,
    d.decision_metadata_json AS DecisionMetadataJson,
    d.source_brain_id AS SourceBrainId,
    d.source_artifact_ref AS SourceArtifactRef,
    d.decision_id AS DecisionId
FROM lineage_edges AS e
JOIN species_membership AS m
    ON m.epoch_id = e.epoch_id
   AND m.brain_id = e.child_brain_id
JOIN species AS s
    ON s.epoch_id = m.epoch_id
   AND s.species_id = m.species_id
JOIN speciation_decisions AS d
    ON d.decision_id = m.decision_id
WHERE e.epoch_id = @epoch_id
  AND e.parent_brain_id = @parent_brain_id
ORDER BY e.created_ms DESC, e.edge_id DESC, m.assigned_ms DESC, m.brain_id
LIMIT 1;
""";

    private const string CountMembershipSql = """
SELECT COUNT(1)
FROM species_membership
WHERE epoch_id = @epoch_id;
""";

    private const string CountMembershipAllSql = """
SELECT COUNT(1)
FROM species_membership;
""";

    private const string CountSpeciesSql = """
SELECT COUNT(1)
FROM species
WHERE epoch_id = @epoch_id;
""";

    private const string CountSpeciesAllSql = """
SELECT COUNT(1)
FROM species;
""";

    private const string CountDecisionSql = """
SELECT COUNT(1)
FROM speciation_decisions
WHERE epoch_id = @epoch_id;
""";

    private const string CountDecisionAllSql = """
SELECT COUNT(1)
FROM speciation_decisions;
""";

    private const string CountLineageSql = """
SELECT COUNT(1)
FROM lineage_edges
WHERE epoch_id = @epoch_id;
""";

    private const string CountLineageAllSql = """
SELECT COUNT(1)
FROM lineage_edges;
""";

    private const string CountEpochSql = """
SELECT COUNT(1)
FROM taxonomy_epochs
WHERE epoch_id = @epoch_id;
""";

    private const string CountEpochAllSql = """
SELECT COUNT(1)
FROM taxonomy_epochs;
""";

    private const string DeleteEpochMembershipSql = """
DELETE FROM species_membership
WHERE epoch_id = @epoch_id;
""";

    private const string DeleteEpochLineageSql = """
DELETE FROM lineage_edges
WHERE epoch_id = @epoch_id;
""";

    private const string DeleteEpochDecisionSql = """
DELETE FROM speciation_decisions
WHERE epoch_id = @epoch_id;
""";

    private const string DeleteEpochSpeciesSql = """
DELETE FROM species
WHERE epoch_id = @epoch_id;
""";

    private const string DeleteEpochConfigSnapshotsSql = """
DELETE FROM taxonomy_config_snapshots
WHERE epoch_id = @epoch_id;
""";

    private const string DeleteEpochSql = """
DELETE FROM taxonomy_epochs
WHERE epoch_id = @epoch_id;
""";

    private const string DeleteAllMembershipSql = """
DELETE FROM species_membership;
""";

    private const string DeleteAllLineageSql = """
DELETE FROM lineage_edges;
""";

    private const string DeleteAllDecisionSql = """
DELETE FROM speciation_decisions;
""";

    private const string DeleteAllSpeciesSql = """
DELETE FROM species;
""";

    private const string DeleteAllConfigSnapshotsSql = """
DELETE FROM taxonomy_config_snapshots;
""";

    private const string DeleteAllEpochsSql = """
DELETE FROM taxonomy_epochs;
""";

    private const string ResetAllAutoincrementSql = """
DELETE FROM sqlite_sequence
WHERE name IN (
    'taxonomy_epochs',
    'taxonomy_config_snapshots',
    'speciation_decisions',
    'lineage_edges'
);
""";

    private readonly string _databasePath;
    private readonly string _connectionString;
    private readonly TimeProvider _timeProvider;
    private const int DefaultMembershipListLimit = 8192;
    private const long StorageMaintenanceMinimumBytes = 512L * 1024L * 1024L;
    private const double StorageMaintenanceFreePageRatio = 0.35d;

    private static readonly object HandlerGate = new();
    private static bool _handlersRegistered;

    /// <summary>
    /// Initializes a SQLite-backed speciation store rooted at the supplied database path.
    /// </summary>
    public SpeciationStore(string databasePath, TimeProvider? timeProvider = null)
    {
        if (string.IsNullOrWhiteSpace(databasePath))
        {
            throw new ArgumentException("Database path is required.", nameof(databasePath));
        }

        _databasePath = databasePath;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared
        }.ToString();
    }
}
