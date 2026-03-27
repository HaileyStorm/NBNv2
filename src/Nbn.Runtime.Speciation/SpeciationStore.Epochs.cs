using System.Data;
using Dapper;
using Microsoft.Data.Sqlite;

namespace Nbn.Runtime.Speciation;

public sealed partial class SpeciationStore
{
    /// <summary>
    /// Creates the schema and supporting indexes if they do not already exist.
    /// </summary>
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

    /// <summary>
    /// Ensures a current epoch exists and that it has a configuration snapshot.
    /// </summary>
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

    /// <summary>
    /// Returns the latest persisted epoch, or <see langword="null"/> when none exists.
    /// </summary>
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

    /// <summary>
    /// Creates a fresh epoch and writes the provided runtime configuration snapshot.
    /// </summary>
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
        await InsertConfigSnapshotAsync(
            connection,
            transaction,
            epochId,
            normalizedConfig,
            createdMs,
            cancellationToken);

        await transaction.CommitAsync(cancellationToken);

        return new SpeciationEpochInfo(
            epochId,
            createdMs,
            normalizedConfig.PolicyVersion,
            normalizedConfig.ConfigSnapshotJson);
    }

    /// <summary>
    /// Clears all persisted speciation data, reseeds identity counters, and creates a fresh epoch.
    /// </summary>
    public async Task<SpeciationResetAllResult> ResetAllAsync(
        SpeciationRuntimeConfig runtimeConfig,
        long? resetTimeMs = null,
        CancellationToken cancellationToken = default)
    {
        var normalizedConfig = NormalizeRuntimeConfig(runtimeConfig);
        var createdMs = resetTimeMs ?? NowMs();

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var deletedEpochCount = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(CountEpochAllSql, transaction: transaction, cancellationToken: cancellationToken));
        var deletedMembershipCount = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(CountMembershipAllSql, transaction: transaction, cancellationToken: cancellationToken));
        var deletedSpeciesCount = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(CountSpeciesAllSql, transaction: transaction, cancellationToken: cancellationToken));
        var deletedDecisionCount = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(CountDecisionAllSql, transaction: transaction, cancellationToken: cancellationToken));
        var deletedLineageEdgeCount = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(CountLineageAllSql, transaction: transaction, cancellationToken: cancellationToken));

        await connection.ExecuteAsync(new CommandDefinition(DeleteAllMembershipSql, transaction: transaction, cancellationToken: cancellationToken));
        await connection.ExecuteAsync(new CommandDefinition(DeleteAllLineageSql, transaction: transaction, cancellationToken: cancellationToken));
        await connection.ExecuteAsync(new CommandDefinition(DeleteAllDecisionSql, transaction: transaction, cancellationToken: cancellationToken));
        await connection.ExecuteAsync(new CommandDefinition(DeleteAllSpeciesSql, transaction: transaction, cancellationToken: cancellationToken));
        await connection.ExecuteAsync(new CommandDefinition(DeleteAllConfigSnapshotsSql, transaction: transaction, cancellationToken: cancellationToken));
        await connection.ExecuteAsync(new CommandDefinition(DeleteAllEpochsSql, transaction: transaction, cancellationToken: cancellationToken));
        await connection.ExecuteAsync(new CommandDefinition(ResetAllAutoincrementSql, transaction: transaction, cancellationToken: cancellationToken));

        var epochId = await CreateEpochAsync(connection, transaction, createdMs, cancellationToken);
        await InsertConfigSnapshotAsync(
            connection,
            transaction,
            epochId,
            normalizedConfig,
            createdMs,
            cancellationToken);

        await transaction.CommitAsync(cancellationToken);

        return new SpeciationResetAllResult(
            CurrentEpoch: new SpeciationEpochInfo(
                epochId,
                createdMs,
                normalizedConfig.PolicyVersion,
                normalizedConfig.ConfigSnapshotJson),
            DeletedEpochCount: deletedEpochCount,
            DeletedMembershipCount: deletedMembershipCount,
            DeletedSpeciesCount: deletedSpeciesCount,
            DeletedDecisionCount: deletedDecisionCount,
            DeletedLineageEdgeCount: deletedLineageEdgeCount);
    }

    /// <summary>
    /// Deletes a single epoch and all of its persisted speciation data.
    /// </summary>
    public async Task<SpeciationDeleteEpochResult> DeleteEpochAsync(
        long epochId,
        CancellationToken cancellationToken = default)
    {
        if (epochId <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(epochId), "Epoch id must be greater than zero.");
        }

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var exists = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(
                CountEpochSql,
                new { epoch_id = epochId },
                transaction,
                cancellationToken: cancellationToken));
        if (exists <= 0)
        {
            await transaction.RollbackAsync(cancellationToken);
            return new SpeciationDeleteEpochResult(
                EpochId: epochId,
                Deleted: false,
                DeletedMembershipCount: 0,
                DeletedSpeciesCount: 0,
                DeletedDecisionCount: 0,
                DeletedLineageEdgeCount: 0);
        }

        var deletedMembershipCount = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(
                CountMembershipSql,
                new { epoch_id = epochId },
                transaction,
                cancellationToken: cancellationToken));
        var deletedSpeciesCount = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(
                CountSpeciesSql,
                new { epoch_id = epochId },
                transaction,
                cancellationToken: cancellationToken));
        var deletedDecisionCount = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(
                CountDecisionSql,
                new { epoch_id = epochId },
                transaction,
                cancellationToken: cancellationToken));
        var deletedLineageEdgeCount = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(
                CountLineageSql,
                new { epoch_id = epochId },
                transaction,
                cancellationToken: cancellationToken));

        await connection.ExecuteAsync(
            new CommandDefinition(
                DeleteEpochMembershipSql,
                new { epoch_id = epochId },
                transaction,
                cancellationToken: cancellationToken));
        await connection.ExecuteAsync(
            new CommandDefinition(
                DeleteEpochLineageSql,
                new { epoch_id = epochId },
                transaction,
                cancellationToken: cancellationToken));
        await connection.ExecuteAsync(
            new CommandDefinition(
                DeleteEpochDecisionSql,
                new { epoch_id = epochId },
                transaction,
                cancellationToken: cancellationToken));
        await connection.ExecuteAsync(
            new CommandDefinition(
                DeleteEpochSpeciesSql,
                new { epoch_id = epochId },
                transaction,
                cancellationToken: cancellationToken));
        await connection.ExecuteAsync(
            new CommandDefinition(
                DeleteEpochConfigSnapshotsSql,
                new { epoch_id = epochId },
                transaction,
                cancellationToken: cancellationToken));
        await connection.ExecuteAsync(
            new CommandDefinition(
                DeleteEpochSql,
                new { epoch_id = epochId },
                transaction,
                cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);

        return new SpeciationDeleteEpochResult(
            EpochId: epochId,
            Deleted: true,
            DeletedMembershipCount: deletedMembershipCount,
            DeletedSpeciesCount: deletedSpeciesCount,
            DeletedDecisionCount: deletedDecisionCount,
            DeletedLineageEdgeCount: deletedLineageEdgeCount);
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

        await InsertConfigSnapshotAsync(
            connection,
            transaction,
            epochId,
            runtimeConfig,
            capturedMs,
            cancellationToken);
    }

    private async Task InsertConfigSnapshotAsync(
        SqliteConnection connection,
        IDbTransaction transaction,
        long epochId,
        SpeciationRuntimeConfig runtimeConfig,
        long capturedMs,
        CancellationToken cancellationToken)
    {
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
}
