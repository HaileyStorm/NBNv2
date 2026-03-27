using System.Data;
using Dapper;
using Microsoft.Data.Sqlite;

namespace Nbn.Runtime.Speciation;

public sealed partial class SpeciationStore
{
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

    private static IReadOnlyList<SpeciationSpeciesDisplayNameUpdate> NormalizeSpeciesDisplayNameUpdates(
        IReadOnlyList<SpeciationSpeciesDisplayNameUpdate>? speciesDisplayNameUpdates)
    {
        if (speciesDisplayNameUpdates is null || speciesDisplayNameUpdates.Count == 0)
        {
            return Array.Empty<SpeciationSpeciesDisplayNameUpdate>();
        }

        return speciesDisplayNameUpdates
            .Where(static update => update is not null)
            .Select(static update => new SpeciationSpeciesDisplayNameUpdate(
                NormalizeRequired(update.SpeciesId, nameof(update.SpeciesId)),
                NormalizeRequired(update.SpeciesDisplayName, nameof(update.SpeciesDisplayName))))
            .GroupBy(static update => update.SpeciesId, StringComparer.Ordinal)
            .Select(static group => group.Last())
            .ToArray();
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

    private long NowMs() => _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

    private async Task<SqliteConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(PragmaSql, cancellationToken: cancellationToken));
        return connection;
    }

    private async Task UpsertSpeciesAsync(
        SqliteConnection connection,
        IDbTransaction transaction,
        long epochId,
        string speciesId,
        string speciesDisplayName,
        long createdMs,
        CancellationToken cancellationToken)
    {
        await connection.ExecuteAsync(
            new CommandDefinition(
                UpsertSpeciesSql,
                new
                {
                    epoch_id = epochId,
                    species_id = speciesId,
                    display_name = speciesDisplayName,
                    created_ms = createdMs
                },
                transaction,
                cancellationToken: cancellationToken));
    }

    private async Task UpsertSpeciesDisplayNamesAsync(
        SqliteConnection connection,
        IDbTransaction transaction,
        long epochId,
        IReadOnlyList<SpeciationSpeciesDisplayNameUpdate> speciesDisplayNameUpdates,
        long createdMs,
        CancellationToken cancellationToken)
    {
        foreach (var displayNameUpdate in speciesDisplayNameUpdates)
        {
            await UpsertSpeciesAsync(
                connection,
                transaction,
                epochId,
                displayNameUpdate.SpeciesId,
                displayNameUpdate.SpeciesDisplayName,
                createdMs,
                cancellationToken);
        }
    }

    private async Task<long> InsertDecisionAsync(
        SqliteConnection connection,
        IDbTransaction transaction,
        long epochId,
        SpeciationAssignment assignment,
        long decidedMs,
        CancellationToken cancellationToken)
    {
        return await connection.ExecuteScalarAsync<long>(
            new CommandDefinition(
                InsertDecisionSql,
                new
                {
                    epoch_id = epochId,
                    brain_id = assignment.BrainId,
                    species_id = assignment.SpeciesId,
                    decided_ms = decidedMs,
                    policy_version = assignment.PolicyVersion,
                    decision_reason = assignment.DecisionReason,
                    decision_metadata_json = assignment.DecisionMetadataJson,
                    source_brain_id = assignment.SourceBrainId,
                    source_artifact_ref = assignment.SourceArtifactRef
                },
                transaction,
                cancellationToken: cancellationToken));
    }

    private async Task InsertLineageEdgesAsync(
        SqliteConnection connection,
        IDbTransaction transaction,
        long epochId,
        Guid childBrainId,
        IReadOnlyList<Guid> lineageParentBrainIds,
        string? lineageMetadataJson,
        long createdMs,
        CancellationToken cancellationToken)
    {
        if (lineageParentBrainIds.Count == 0 || lineageMetadataJson is null)
        {
            return;
        }

        foreach (var parentBrainId in lineageParentBrainIds)
        {
            await connection.ExecuteAsync(
                new CommandDefinition(
                    InsertLineageEdgeSql,
                    new
                    {
                        epoch_id = epochId,
                        parent_brain_id = parentBrainId,
                        child_brain_id = childBrainId,
                        metadata_json = lineageMetadataJson,
                        created_ms = createdMs
                    },
                    transaction,
                    cancellationToken: cancellationToken));
        }
    }

    private async Task<SpeciationMembershipRecord> GetRequiredMembershipInternalAsync(
        SqliteConnection connection,
        IDbTransaction transaction,
        long epochId,
        Guid brainId,
        CancellationToken cancellationToken,
        string missingRecordMessage)
    {
        var membership = await GetMembershipInternalAsync(
            connection,
            transaction,
            epochId,
            brainId,
            cancellationToken);
        return membership
            ?? throw new InvalidOperationException(missingRecordMessage);
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

    /// <summary>
    /// Represents a single page of historical membership records and the unpaged total.
    /// </summary>
    public readonly record struct SpeciationHistoryPage(
        IReadOnlyList<SpeciationMembershipRecord> Records,
        int TotalRecords);

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
