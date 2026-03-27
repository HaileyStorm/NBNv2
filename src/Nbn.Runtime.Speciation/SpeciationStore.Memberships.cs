using System.Data;
using Dapper;
using Microsoft.Data.Sqlite;

namespace Nbn.Runtime.Speciation;

public sealed partial class SpeciationStore
{
    /// <summary>
    /// Attempts to create an immutable membership assignment for the specified brain in the epoch.
    /// </summary>
    public async Task<SpeciationAssignOutcome> TryAssignMembershipAsync(
        long epochId,
        SpeciationAssignment assignment,
        long? decisionTimeMs = null,
        CancellationToken cancellationToken = default,
        IReadOnlyList<Guid>? lineageParentBrainIds = null,
        string? lineageMetadataJson = null,
        IReadOnlyList<SpeciationSpeciesDisplayNameUpdate>? speciesDisplayNameUpdates = null)
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
        var normalizedSpeciesDisplayNameUpdates = NormalizeSpeciesDisplayNameUpdates(speciesDisplayNameUpdates);

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

            foreach (var displayNameUpdate in normalizedSpeciesDisplayNameUpdates)
            {
                await connection.ExecuteAsync(
                    new CommandDefinition(
                        UpsertSpeciesSql,
                        new
                        {
                            epoch_id = epochId,
                            species_id = displayNameUpdate.SpeciesId,
                            display_name = displayNameUpdate.SpeciesDisplayName,
                            created_ms = decidedMs
                        },
                        transaction,
                        cancellationToken: cancellationToken));
            }

            await connection.ExecuteAsync(
                new CommandDefinition(
                    UpsertSpeciesSql,
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

    /// <summary>
    /// Returns a single membership record for a brain in the requested epoch.
    /// </summary>
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

    /// <summary>
    /// Lists the most recent memberships for a species up to the supplied assignment timestamp.
    /// </summary>
    public async Task<IReadOnlyList<SpeciationMembershipRecord>> ListRecentMembershipsForSpeciesAsync(
        long epochId,
        string speciesId,
        long maxAssignedMs,
        int limit,
        CancellationToken cancellationToken = default)
    {
        if (epochId <= 0
            || string.IsNullOrWhiteSpace(speciesId)
            || maxAssignedMs <= 0
            || limit <= 0)
        {
            return Array.Empty<SpeciationMembershipRecord>();
        }

        await using var connection = await OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<MembershipRow>(
            new CommandDefinition(
                ListRecentMembershipsForSpeciesSql,
                new
                {
                    epoch_id = epochId,
                    species_id = speciesId.Trim(),
                    max_assigned_ms = maxAssignedMs,
                    limit = limit
                },
                cancellationToken: cancellationToken));
        return rows.Select(ToMembershipRecord).ToArray();
    }

    /// <summary>
    /// Lists the earliest memberships recorded for a species in the epoch.
    /// </summary>
    public async Task<IReadOnlyList<SpeciationMembershipRecord>> ListEarliestMembershipsForSpeciesAsync(
        long epochId,
        string speciesId,
        int limit,
        CancellationToken cancellationToken = default)
    {
        if (epochId <= 0
            || string.IsNullOrWhiteSpace(speciesId)
            || limit <= 0)
        {
            return Array.Empty<SpeciationMembershipRecord>();
        }

        await using var connection = await OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<MembershipRow>(
            new CommandDefinition(
                ListEarliestMembershipsForSpeciesSql,
                new
                {
                    epoch_id = epochId,
                    species_id = speciesId.Trim(),
                    limit = limit
                },
                cancellationToken: cancellationToken));
        return rows.Select(ToMembershipRecord).ToArray();
    }

    /// <summary>
    /// Attempts to rewrite an existing membership when the caller still holds the expected species id.
    /// </summary>
    public async Task<SpeciationReassignOutcome> TryReassignMembershipAsync(
        long epochId,
        Guid brainId,
        string expectedSpeciesId,
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

        if (brainId == Guid.Empty)
        {
            throw new ArgumentException("Brain id is required.", nameof(brainId));
        }

        var normalizedExpectedSpeciesId = NormalizeRequired(expectedSpeciesId, nameof(expectedSpeciesId));
        var normalized = NormalizeAssignment(assignment);
        if (normalized.BrainId != brainId)
        {
            throw new ArgumentException("Assignment brain id must match requested brain id.", nameof(assignment));
        }

        var decidedMs = decisionTimeMs ?? NowMs();
        var normalizedLineageParentIds = NormalizeLineageParentIds(lineageParentBrainIds, normalized.BrainId);
        var normalizedLineageMetadata = normalizedLineageParentIds.Count == 0
            ? null
            : NormalizeJson(
                lineageMetadataJson ?? "{\"source\":\"assignment_lineage_reassign\"}",
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
            if (existing is null)
            {
                await transaction.RollbackAsync(cancellationToken);
                return new SpeciationReassignOutcome(
                    Reassigned: false,
                    ImmutableConflict: false,
                    Membership: null);
            }

            if (!string.Equals(
                    existing.SpeciesId,
                    normalizedExpectedSpeciesId,
                    StringComparison.Ordinal))
            {
                await transaction.RollbackAsync(cancellationToken);
                return new SpeciationReassignOutcome(
                    Reassigned: false,
                    ImmutableConflict: true,
                    Membership: existing);
            }

            if (string.Equals(existing.SpeciesId, normalized.SpeciesId, StringComparison.Ordinal))
            {
                await transaction.RollbackAsync(cancellationToken);
                return new SpeciationReassignOutcome(
                    Reassigned: false,
                    ImmutableConflict: false,
                    Membership: existing);
            }

            await connection.ExecuteAsync(
                new CommandDefinition(
                    UpsertSpeciesSql,
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

            var updatedRows = await connection.ExecuteAsync(
                new CommandDefinition(
                    UpdateMembershipReassignSql,
                    new
                    {
                        epoch_id = epochId,
                        brain_id = normalized.BrainId,
                        expected_species_id = normalizedExpectedSpeciesId,
                        species_id = normalized.SpeciesId,
                        assigned_ms = decidedMs,
                        decision_id = decisionId
                    },
                    transaction,
                    cancellationToken: cancellationToken));
            if (updatedRows != 1)
            {
                await transaction.RollbackAsync(cancellationToken);
                var latest = await GetMembershipAsync(epochId, normalized.BrainId, cancellationToken);
                return new SpeciationReassignOutcome(
                    Reassigned: false,
                    ImmutableConflict: true,
                    Membership: latest);
            }

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

            var reassigned = await GetMembershipInternalAsync(
                connection,
                transaction,
                epochId,
                normalized.BrainId,
                cancellationToken);
            if (reassigned is null)
            {
                throw new InvalidOperationException(
                    $"Membership reassign returned no record for epoch={epochId} brain={normalized.BrainId:D}.");
            }

            await transaction.CommitAsync(cancellationToken);
            return new SpeciationReassignOutcome(
                Reassigned: true,
                ImmutableConflict: false,
                Membership: reassigned);
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19)
        {
            await transaction.RollbackAsync(cancellationToken);
            var existing = await GetMembershipAsync(epochId, normalized.BrainId, cancellationToken);
            return new SpeciationReassignOutcome(
                Reassigned: false,
                ImmutableConflict: true,
                Membership: existing);
        }
    }

    /// <summary>
    /// Assigns the default species to any listed brains that do not yet have memberships in the epoch.
    /// </summary>
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

    /// <summary>
    /// Appends a lineage edge for the epoch without modifying membership state.
    /// </summary>
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

    /// <summary>
    /// Returns the latest child membership reachable from the specified parent lineage edge.
    /// </summary>
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
}
