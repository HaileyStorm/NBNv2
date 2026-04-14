using Dapper;

namespace Nbn.Runtime.Speciation;

public sealed partial class SpeciationStore
{
    /// <summary>
    /// Lists memberships, optionally restricted to a single epoch.
    /// </summary>
    public async Task<IReadOnlyList<SpeciationMembershipRecord>> ListMembershipsAsync(
        long? epochId = null,
        int limit = DefaultMembershipListLimit,
        int offset = 0,
        CancellationToken cancellationToken = default)
    {
        var normalizedLimit = Math.Max(1, limit);
        var normalizedOffset = Math.Max(0, offset);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<MembershipRow>(
            new CommandDefinition(
                ListMembershipsSql,
                new { epoch_id = epochId, limit = normalizedLimit, offset = normalizedOffset },
                cancellationToken: cancellationToken));
        return rows.Select(ToMembershipRecord).ToArray();
    }

    /// <summary>
    /// Returns a paged view of historical memberships, optionally filtered by epoch and brain id.
    /// </summary>
    public async Task<SpeciationHistoryPage> ListHistoryPageAsync(
        long? epochId = null,
        Guid? brainId = null,
        int limit = 256,
        int offset = 0,
        CancellationToken cancellationToken = default)
    {
        var normalizedLimit = Math.Max(1, limit);
        var normalizedOffset = Math.Max(0, offset);

        await using var connection = await OpenConnectionAsync(cancellationToken);
        var parameters = new
        {
            epoch_id = epochId,
            brain_id = brainId.HasValue && brainId.Value != Guid.Empty ? brainId : null,
            limit = normalizedLimit,
            offset = normalizedOffset
        };
        var totalRecords = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(
                CountHistorySql,
                parameters,
                cancellationToken: cancellationToken));
        var rows = await connection.QueryAsync<MembershipRow>(
            new CommandDefinition(
                ListHistoryPageSql,
                parameters,
                cancellationToken: cancellationToken));
        return new SpeciationHistoryPage(
            rows.Select(ToMembershipRecord).ToArray(),
            totalRecords);
    }

    /// <summary>
    /// Returns aggregate counts for the requested epoch.
    /// </summary>
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
}
