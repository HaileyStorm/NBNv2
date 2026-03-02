using System.Data;
using Dapper;
using Microsoft.Data.Sqlite;

namespace Nbn.Runtime.Speciation;

public sealed class SpeciationStore
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

    private const string InsertSpeciesIfMissingSql = """
INSERT INTO species (epoch_id, species_id, display_name, created_ms)
VALUES (@epoch_id, @species_id, @display_name, @created_ms)
ON CONFLICT(epoch_id, species_id) DO NOTHING;
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
ORDER BY m.epoch_id, m.brain_id;
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

    private const string CountSpeciesSql = """
SELECT COUNT(1)
FROM species
WHERE epoch_id = @epoch_id;
""";

    private const string CountLineageSql = """
SELECT COUNT(1)
FROM lineage_edges
WHERE epoch_id = @epoch_id;
""";

    private readonly string _databasePath;
    private readonly string _connectionString;
    private readonly TimeProvider _timeProvider;

    private static readonly object HandlerGate = new();
    private static bool _handlersRegistered;

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

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var directory = Path.GetDirectoryName(_databasePath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(CreateSchemaSql, cancellationToken: cancellationToken));
    }

    public async Task<SpeciationEpochInfo> EnsureCurrentEpochAsync(
        SpeciationRuntimeConfig runtimeConfig,
        long? createdMs = null,
        CancellationToken cancellationToken = default)
    {
        var normalizedConfig = NormalizeRuntimeConfig(runtimeConfig);
        var epochCreatedMs = createdMs ?? NowMs();

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var epoch = await connection.QuerySingleOrDefaultAsync<EpochRow>(
            new CommandDefinition(SelectLatestEpochSql, transaction: transaction, cancellationToken: cancellationToken));

        long epochId;
        if (epoch is null)
        {
            epochId = await CreateEpochAsync(connection, transaction, epochCreatedMs, cancellationToken);
            epoch = new EpochRow
            {
                EpochId = epochId,
                CreatedMs = epochCreatedMs
            };
        }
        else
        {
            epochId = epoch.EpochId;
        }

        await EnsureEpochConfigSnapshotAsync(
            connection,
            transaction,
            epochId,
            normalizedConfig,
            epochCreatedMs,
            cancellationToken);

        var config = await GetLatestEpochConfigAsync(connection, transaction, epochId, cancellationToken)
                     ?? new EpochConfigRow
                     {
                         PolicyVersion = normalizedConfig.PolicyVersion,
                         ConfigSnapshotJson = normalizedConfig.ConfigSnapshotJson
                     };

        await transaction.CommitAsync(cancellationToken);

        return new SpeciationEpochInfo(
            epochId,
            epoch.CreatedMs,
            config.PolicyVersion,
            config.ConfigSnapshotJson);
    }

    public async Task<SpeciationEpochInfo?> GetCurrentEpochAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var epoch = await connection.QuerySingleOrDefaultAsync<EpochRow>(
            new CommandDefinition(SelectLatestEpochSql, cancellationToken: cancellationToken));
        if (epoch is null)
        {
            return null;
        }

        var config = await connection.QuerySingleOrDefaultAsync<EpochConfigRow>(
            new CommandDefinition(
                SelectLatestEpochConfigSql,
                new { epoch_id = epoch.EpochId },
                cancellationToken: cancellationToken));

        return new SpeciationEpochInfo(
            epoch.EpochId,
            epoch.CreatedMs,
            config?.PolicyVersion ?? "unknown",
            config?.ConfigSnapshotJson ?? "{}");
    }

    public async Task<SpeciationEpochInfo> ResetEpochAsync(
        SpeciationRuntimeConfig runtimeConfig,
        long? resetTimeMs = null,
        CancellationToken cancellationToken = default)
    {
        var normalizedConfig = NormalizeRuntimeConfig(runtimeConfig);
        var createdMs = resetTimeMs ?? NowMs();

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var epochId = await CreateEpochAsync(connection, transaction, createdMs, cancellationToken);
        await connection.ExecuteAsync(
            new CommandDefinition(
                InsertConfigSnapshotSql,
                new
                {
                    epoch_id = epochId,
                    policy_version = normalizedConfig.PolicyVersion,
                    config_snapshot_json = normalizedConfig.ConfigSnapshotJson,
                    captured_ms = createdMs
                },
                transaction,
                cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);

        return new SpeciationEpochInfo(
            epochId,
            createdMs,
            normalizedConfig.PolicyVersion,
            normalizedConfig.ConfigSnapshotJson);
    }

    public async Task<SpeciationAssignOutcome> TryAssignMembershipAsync(
        long epochId,
        SpeciationAssignment assignment,
        long? decisionTimeMs = null,
        CancellationToken cancellationToken = default,
        IReadOnlyList<Guid>? lineageParentBrainIds = null,
        string? lineageMetadataJson = null)
    {
        if (epochId <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(epochId), "Epoch id must be greater than zero.");
        }

        var normalized = NormalizeAssignment(assignment);
        var decidedMs = decisionTimeMs ?? NowMs();
        var normalizedLineageParentIds = NormalizeLineageParentIds(lineageParentBrainIds, normalized.BrainId);
        var normalizedLineageMetadata = normalizedLineageParentIds.Count == 0
            ? null
            : NormalizeJson(
                lineageMetadataJson ?? "{\"source\":\"assignment_lineage_ingest\"}",
                nameof(lineageMetadataJson));

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            var existing = await GetMembershipInternalAsync(
                connection,
                transaction,
                epochId,
                normalized.BrainId,
                cancellationToken);
            if (existing is not null)
            {
                await transaction.RollbackAsync(cancellationToken);
                var immutableConflict = !string.Equals(
                    existing.SpeciesId,
                    normalized.SpeciesId,
                    StringComparison.Ordinal);
                return new SpeciationAssignOutcome(false, immutableConflict, existing);
            }

            await connection.ExecuteAsync(
                new CommandDefinition(
                    InsertSpeciesIfMissingSql,
                    new
                    {
                        epoch_id = epochId,
                        species_id = normalized.SpeciesId,
                        display_name = normalized.SpeciesDisplayName,
                        created_ms = decidedMs
                    },
                    transaction,
                    cancellationToken: cancellationToken));

            var decisionId = await connection.ExecuteScalarAsync<long>(
                new CommandDefinition(
                    InsertDecisionSql,
                    new
                    {
                        epoch_id = epochId,
                        brain_id = normalized.BrainId,
                        species_id = normalized.SpeciesId,
                        decided_ms = decidedMs,
                        policy_version = normalized.PolicyVersion,
                        decision_reason = normalized.DecisionReason,
                        decision_metadata_json = normalized.DecisionMetadataJson,
                        source_brain_id = normalized.SourceBrainId,
                        source_artifact_ref = normalized.SourceArtifactRef
                    },
                    transaction,
                    cancellationToken: cancellationToken));

            await connection.ExecuteAsync(
                new CommandDefinition(
                    InsertMembershipSql,
                    new
                    {
                        epoch_id = epochId,
                        brain_id = normalized.BrainId,
                        species_id = normalized.SpeciesId,
                        assigned_ms = decidedMs,
                        decision_id = decisionId
                    },
                    transaction,
                    cancellationToken: cancellationToken));

            if (normalizedLineageMetadata is not null)
            {
                foreach (var parentBrainId in normalizedLineageParentIds)
                {
                    await connection.ExecuteAsync(
                        new CommandDefinition(
                            InsertLineageEdgeSql,
                            new
                            {
                                epoch_id = epochId,
                                parent_brain_id = parentBrainId,
                                child_brain_id = normalized.BrainId,
                                metadata_json = normalizedLineageMetadata,
                                created_ms = decidedMs
                            },
                            transaction,
                            cancellationToken: cancellationToken));
                }
            }

            var created = await GetMembershipInternalAsync(
                connection,
                transaction,
                epochId,
                normalized.BrainId,
                cancellationToken);
            if (created is null)
            {
                throw new InvalidOperationException(
                    $"Membership insert returned no record for epoch={epochId} brain={normalized.BrainId:D}.");
            }

            await transaction.CommitAsync(cancellationToken);
            return new SpeciationAssignOutcome(true, false, created);
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19)
        {
            await transaction.RollbackAsync(cancellationToken);
            var existing = await GetMembershipAsync(epochId, normalized.BrainId, cancellationToken);
            if (existing is not null)
            {
                var immutableConflict = !string.Equals(
                    existing.SpeciesId,
                    normalized.SpeciesId,
                    StringComparison.Ordinal);
                return new SpeciationAssignOutcome(false, immutableConflict, existing);
            }

            throw;
        }
    }

    public async Task<SpeciationMembershipRecord?> GetMembershipAsync(
        long epochId,
        Guid brainId,
        CancellationToken cancellationToken = default)
    {
        if (epochId <= 0 || brainId == Guid.Empty)
        {
            return null;
        }

        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await GetMembershipInternalAsync(connection, transaction: null, epochId, brainId, cancellationToken);
    }

    public async Task<IReadOnlyList<SpeciationMembershipRecord>> ListMembershipsAsync(
        long? epochId = null,
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<MembershipRow>(
            new CommandDefinition(
                ListMembershipsSql,
                new { epoch_id = epochId },
                cancellationToken: cancellationToken));
        return rows.Select(ToMembershipRecord).ToArray();
    }

    public async Task<SpeciationReconcileResult> ReconcileMissingMembershipsAsync(
        long epochId,
        IReadOnlyList<Guid> brainIds,
        SpeciationRuntimeConfig runtimeConfig,
        string? decisionMetadataJson = null,
        long? decisionTimeMs = null,
        CancellationToken cancellationToken = default)
    {
        if (epochId <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(epochId), "Epoch id must be greater than zero.");
        }

        var normalizedConfig = NormalizeRuntimeConfig(runtimeConfig);
        var normalizedMetadata = decisionMetadataJson is null
            ? "{\"source\":\"reconcile_missing_membership\"}"
            : NormalizeJson(decisionMetadataJson, nameof(decisionMetadataJson));
        var orderedBrains = brainIds
            .Where(static brainId => brainId != Guid.Empty)
            .Distinct()
            .OrderBy(static brainId => brainId)
            .ToArray();

        var added = 0;
        var existing = 0;
        var addedBrainIds = new List<Guid>(orderedBrains.Length);
        foreach (var brainId in orderedBrains)
        {
            var assignment = new SpeciationAssignment(
                brainId,
                normalizedConfig.DefaultSpeciesId,
                normalizedConfig.DefaultSpeciesDisplayName,
                normalizedConfig.PolicyVersion,
                normalizedConfig.StartupReconcileDecisionReason,
                normalizedMetadata);

            var outcome = await TryAssignMembershipAsync(
                epochId,
                assignment,
                decisionTimeMs,
                cancellationToken);
            if (outcome.Created)
            {
                added++;
                addedBrainIds.Add(brainId);
            }
            else
            {
                existing++;
            }
        }

        return new SpeciationReconcileResult(epochId, added, existing, addedBrainIds);
    }

    public async Task RecordLineageEdgeAsync(
        long epochId,
        Guid parentBrainId,
        Guid childBrainId,
        string metadataJson,
        long? createdMs = null,
        CancellationToken cancellationToken = default)
    {
        if (epochId <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(epochId), "Epoch id must be greater than zero.");
        }

        if (parentBrainId == Guid.Empty)
        {
            throw new ArgumentException("Parent brain id is required.", nameof(parentBrainId));
        }

        if (childBrainId == Guid.Empty)
        {
            throw new ArgumentException("Child brain id is required.", nameof(childBrainId));
        }

        var normalizedMetadata = NormalizeJson(metadataJson, nameof(metadataJson));
        var writtenMs = createdMs ?? NowMs();

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(
            new CommandDefinition(
                InsertLineageEdgeSql,
                new
                {
                    epoch_id = epochId,
                    parent_brain_id = parentBrainId,
                    child_brain_id = childBrainId,
                    metadata_json = normalizedMetadata,
                    created_ms = writtenMs
                },
                cancellationToken: cancellationToken));
    }

    public async Task<SpeciationMembershipRecord?> GetLatestChildMembershipForParentAsync(
        long epochId,
        Guid parentBrainId,
        CancellationToken cancellationToken = default)
    {
        if (epochId <= 0 || parentBrainId == Guid.Empty)
        {
            return null;
        }

        await using var connection = await OpenConnectionAsync(cancellationToken);
        var row = await connection.QuerySingleOrDefaultAsync<MembershipRow>(
            new CommandDefinition(
                SelectLatestChildMembershipForParentSql,
                new
                {
                    epoch_id = epochId,
                    parent_brain_id = parentBrainId
                },
                cancellationToken: cancellationToken));
        return row is null ? null : ToMembershipRecord(row);
    }

    public async Task<SpeciationStatusSnapshot> GetStatusAsync(
        long epochId,
        CancellationToken cancellationToken = default)
    {
        if (epochId <= 0)
        {
            return new SpeciationStatusSnapshot(0, 0, 0, 0);
        }

        await using var connection = await OpenConnectionAsync(cancellationToken);
        var memberships = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(
                CountMembershipSql,
                new { epoch_id = epochId },
                cancellationToken: cancellationToken));
        var species = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(
                CountSpeciesSql,
                new { epoch_id = epochId },
                cancellationToken: cancellationToken));
        var lineageEdges = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(
                CountLineageSql,
                new { epoch_id = epochId },
                cancellationToken: cancellationToken));

        return new SpeciationStatusSnapshot(epochId, memberships, species, lineageEdges);
    }

    private static void RegisterTypeHandlers()
    {
        lock (HandlerGate)
        {
            if (_handlersRegistered)
            {
                return;
            }

            SqlMapper.AddTypeHandler(new GuidTextHandler());
            SqlMapper.AddTypeHandler(new NullableGuidTextHandler());
            _handlersRegistered = true;
        }
    }

    private static SpeciationRuntimeConfig NormalizeRuntimeConfig(SpeciationRuntimeConfig runtimeConfig)
    {
        if (runtimeConfig is null)
        {
            throw new ArgumentNullException(nameof(runtimeConfig));
        }

        return new SpeciationRuntimeConfig(
            NormalizeRequired(runtimeConfig.PolicyVersion, nameof(runtimeConfig.PolicyVersion)),
            NormalizeJson(runtimeConfig.ConfigSnapshotJson, nameof(runtimeConfig.ConfigSnapshotJson)),
            NormalizeRequired(runtimeConfig.DefaultSpeciesId, nameof(runtimeConfig.DefaultSpeciesId)),
            NormalizeRequired(runtimeConfig.DefaultSpeciesDisplayName, nameof(runtimeConfig.DefaultSpeciesDisplayName)),
            NormalizeRequired(runtimeConfig.StartupReconcileDecisionReason, nameof(runtimeConfig.StartupReconcileDecisionReason)));
    }

    private static SpeciationAssignment NormalizeAssignment(SpeciationAssignment assignment)
    {
        if (assignment is null)
        {
            throw new ArgumentNullException(nameof(assignment));
        }

        if (assignment.BrainId == Guid.Empty)
        {
            throw new ArgumentException("Brain id is required.", nameof(assignment));
        }

        return assignment with
        {
            SpeciesId = NormalizeRequired(assignment.SpeciesId, nameof(assignment.SpeciesId)),
            SpeciesDisplayName = NormalizeRequired(assignment.SpeciesDisplayName, nameof(assignment.SpeciesDisplayName)),
            PolicyVersion = NormalizeRequired(assignment.PolicyVersion, nameof(assignment.PolicyVersion)),
            DecisionReason = NormalizeRequired(assignment.DecisionReason, nameof(assignment.DecisionReason)),
            DecisionMetadataJson = NormalizeJson(assignment.DecisionMetadataJson, nameof(assignment.DecisionMetadataJson)),
            SourceArtifactRef = string.IsNullOrWhiteSpace(assignment.SourceArtifactRef)
                ? null
                : assignment.SourceArtifactRef.Trim(),
            SourceBrainId = assignment.SourceBrainId == Guid.Empty
                ? null
                : assignment.SourceBrainId
        };
    }

    private static IReadOnlyList<Guid> NormalizeLineageParentIds(
        IReadOnlyList<Guid>? lineageParentBrainIds,
        Guid childBrainId)
    {
        if (lineageParentBrainIds is null || lineageParentBrainIds.Count == 0)
        {
            return Array.Empty<Guid>();
        }

        return lineageParentBrainIds
            .Where(id => id != Guid.Empty && id != childBrainId)
            .Distinct()
            .OrderBy(id => id)
            .ToArray();
    }

    private static string NormalizeRequired(string? value, string paramName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"{paramName} is required.", paramName);
        }

        return value.Trim();
    }

    private static string NormalizeJson(string? value, string paramName)
    {
        if (value is null)
        {
            throw new ArgumentNullException(paramName);
        }

        var trimmed = value.Trim();
        return trimmed.Length == 0 ? "{}" : trimmed;
    }

    private async Task<long> CreateEpochAsync(
        SqliteConnection connection,
        IDbTransaction transaction,
        long createdMs,
        CancellationToken cancellationToken)
    {
        return await connection.ExecuteScalarAsync<long>(
            new CommandDefinition(
                InsertEpochSql,
                new { created_ms = createdMs },
                transaction,
                cancellationToken: cancellationToken));
    }

    private async Task EnsureEpochConfigSnapshotAsync(
        SqliteConnection connection,
        IDbTransaction transaction,
        long epochId,
        SpeciationRuntimeConfig runtimeConfig,
        long capturedMs,
        CancellationToken cancellationToken)
    {
        var count = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(
                CountEpochConfigSnapshotsSql,
                new { epoch_id = epochId },
                transaction,
                cancellationToken: cancellationToken));
        if (count > 0)
        {
            return;
        }

        await connection.ExecuteAsync(
            new CommandDefinition(
                InsertConfigSnapshotSql,
                new
                {
                    epoch_id = epochId,
                    policy_version = runtimeConfig.PolicyVersion,
                    config_snapshot_json = runtimeConfig.ConfigSnapshotJson,
                    captured_ms = capturedMs
                },
                transaction,
                cancellationToken: cancellationToken));
    }

    private async Task<EpochConfigRow?> GetLatestEpochConfigAsync(
        SqliteConnection connection,
        IDbTransaction transaction,
        long epochId,
        CancellationToken cancellationToken)
    {
        return await connection.QuerySingleOrDefaultAsync<EpochConfigRow>(
            new CommandDefinition(
                SelectLatestEpochConfigSql,
                new { epoch_id = epochId },
                transaction,
                cancellationToken: cancellationToken));
    }

    private async Task<SpeciationMembershipRecord?> GetMembershipInternalAsync(
        SqliteConnection connection,
        IDbTransaction? transaction,
        long epochId,
        Guid brainId,
        CancellationToken cancellationToken)
    {
        var row = await connection.QuerySingleOrDefaultAsync<MembershipRow>(
            new CommandDefinition(
                SelectMembershipSql,
                new
                {
                    epoch_id = epochId,
                    brain_id = brainId
                },
                transaction,
                cancellationToken: cancellationToken));

        return row is null ? null : ToMembershipRecord(row);
    }

    private static SpeciationMembershipRecord ToMembershipRecord(MembershipRow row)
    {
        return new SpeciationMembershipRecord(
            row.EpochId,
            row.BrainId,
            row.SpeciesId,
            row.SpeciesDisplayName,
            row.AssignedMs,
            row.PolicyVersion,
            row.DecisionReason,
            row.DecisionMetadataJson,
            row.SourceBrainId,
            row.SourceArtifactRef,
            row.DecisionId);
    }

    private long NowMs() => _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

    private async Task<SqliteConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(PragmaSql, cancellationToken: cancellationToken));
        return connection;
    }

    private sealed class EpochRow
    {
        public long EpochId { get; set; }

        public long CreatedMs { get; set; }
    }

    private sealed class EpochConfigRow
    {
        public string PolicyVersion { get; set; } = "unknown";

        public string ConfigSnapshotJson { get; set; } = "{}";
    }

    private sealed class MembershipRow
    {
        public long EpochId { get; set; }

        public Guid BrainId { get; set; }

        public string SpeciesId { get; set; } = string.Empty;

        public string SpeciesDisplayName { get; set; } = string.Empty;

        public long AssignedMs { get; set; }

        public string PolicyVersion { get; set; } = string.Empty;

        public string DecisionReason { get; set; } = string.Empty;

        public string DecisionMetadataJson { get; set; } = "{}";

        public Guid? SourceBrainId { get; set; }

        public string? SourceArtifactRef { get; set; }

        public long DecisionId { get; set; }
    }

    private sealed class GuidTextHandler : SqlMapper.TypeHandler<Guid>
    {
        public override void SetValue(IDbDataParameter parameter, Guid value)
        {
            parameter.Value = value.ToString("D");
        }

        public override Guid Parse(object value)
        {
            if (value is Guid guid)
            {
                return guid;
            }

            if (value is byte[] bytes && bytes.Length == 16)
            {
                return new Guid(bytes);
            }

            if (value is string text && Guid.TryParse(text, out var parsed))
            {
                return parsed;
            }

            throw new DataException($"Unable to parse Guid from '{value}'.");
        }
    }

    private sealed class NullableGuidTextHandler : SqlMapper.TypeHandler<Guid?>
    {
        public override void SetValue(IDbDataParameter parameter, Guid? value)
        {
            parameter.Value = value?.ToString("D");
        }

        public override Guid? Parse(object value)
        {
            if (value is null || value is DBNull)
            {
                return null;
            }

            if (value is Guid guid)
            {
                return guid;
            }

            if (value is byte[] bytes && bytes.Length == 16)
            {
                return new Guid(bytes);
            }

            if (value is string text && Guid.TryParse(text, out var parsed))
            {
                return parsed;
            }

            throw new DataException($"Unable to parse Guid? from '{value}'.");
        }
    }
}
