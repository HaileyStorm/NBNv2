using Dapper;

namespace Nbn.Runtime.SettingsMonitor;

public sealed partial class SettingsMonitorStore
{
    private const string UpsertBrainControllerSql = """
INSERT INTO brain_controllers (brain_id, node_id, actor_name, last_seen_ms, is_alive)
VALUES (@brain_id, @node_id, @actor_name, @last_seen_ms, @is_alive)
ON CONFLICT(brain_id) DO UPDATE SET
    node_id = excluded.node_id,
    actor_name = excluded.actor_name,
    last_seen_ms = excluded.last_seen_ms,
    is_alive = excluded.is_alive;
""";

    private const string UpdateBrainControllerHeartbeatSql = """
UPDATE brain_controllers
SET last_seen_ms = @last_seen_ms,
    is_alive = 1
WHERE brain_id = @brain_id;
""";

    private const string UpsertBrainSql = """
INSERT INTO brains (
    brain_id,
    base_nbn_sha256,
    last_snapshot_sha256,
    spawned_ms,
    last_tick_id,
    state,
    notes
) VALUES (
    @brain_id,
    @base_nbn_sha256,
    @last_snapshot_sha256,
    @spawned_ms,
    @last_tick_id,
    @state,
    @notes
)
ON CONFLICT(brain_id) DO UPDATE SET
    base_nbn_sha256 = COALESCE(excluded.base_nbn_sha256, brains.base_nbn_sha256),
    last_snapshot_sha256 = COALESCE(excluded.last_snapshot_sha256, brains.last_snapshot_sha256),
    spawned_ms = excluded.spawned_ms,
    last_tick_id = excluded.last_tick_id,
    state = excluded.state,
    notes = COALESCE(excluded.notes, brains.notes);
""";

    private const string UpdateBrainStateSql = """
UPDATE brains
SET state = @state,
    notes = COALESCE(@notes, notes)
WHERE brain_id = @brain_id;
""";

    private const string UpdateBrainTickSql = """
UPDATE brains
SET last_tick_id = @last_tick_id
WHERE brain_id = @brain_id;
""";

    private const string MarkBrainControllerOfflineSql = """
UPDATE brain_controllers
SET is_alive = 0,
    last_seen_ms = @last_seen_ms
WHERE brain_id = @brain_id;
""";

    private const string GetBrainSql = """
SELECT
    brain_id AS BrainId,
    base_nbn_sha256 AS BaseNbnSha256,
    last_snapshot_sha256 AS LastSnapshotSha256,
    spawned_ms AS SpawnedMs,
    last_tick_id AS LastTickId,
    state AS State,
    notes AS Notes
FROM brains
WHERE brain_id = @brain_id;
""";

    private const string GetBrainControllerSql = """
SELECT
    brain_id AS BrainId,
    node_id AS NodeId,
    actor_name AS ActorName,
    last_seen_ms AS LastSeenMs,
    is_alive AS IsAlive
FROM brain_controllers
WHERE brain_id = @brain_id;
""";

    private const string ListBrainControllersSql = """
SELECT
    brain_id AS BrainId,
    node_id AS NodeId,
    actor_name AS ActorName,
    last_seen_ms AS LastSeenMs,
    is_alive AS IsAlive
FROM brain_controllers
ORDER BY brain_id;
""";

    private const string ListBrainsSql = """
SELECT
    brain_id AS BrainId,
    base_nbn_sha256 AS BaseNbnSha256,
    last_snapshot_sha256 AS LastSnapshotSha256,
    spawned_ms AS SpawnedMs,
    last_tick_id AS LastTickId,
    state AS State,
    notes AS Notes
FROM brains
ORDER BY brain_id;
""";

    /// <summary>
    /// Inserts or refreshes the controller row for a hosted brain.
    /// </summary>
    /// <param name="registration">Brain controller identity and owner node.</param>
    /// <param name="timeMs">Optional persisted observation time in milliseconds.</param>
    /// <param name="cancellationToken">Cancels the database write.</param>
    public async Task UpsertBrainControllerAsync(
        BrainControllerRegistration registration,
        long? timeMs = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(registration.BrainId, nameof(registration), "BrainId");
        ThrowIfEmptyGuid(registration.NodeId, nameof(registration), "NodeId");
        ThrowIfNullOrWhiteSpace(registration.ActorName, nameof(registration), "ActorName");

        var nowMs = timeMs ?? NowMs();
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(
            new CommandDefinition(
                UpsertBrainControllerSql,
                new
                {
                    brain_id = ToDatabaseId(registration.BrainId),
                    node_id = ToDatabaseId(registration.NodeId),
                    actor_name = registration.ActorName,
                    last_seen_ms = nowMs,
                    is_alive = 1
                },
                cancellationToken: cancellationToken));
    }

    /// <summary>
    /// Refreshes the liveness timestamp for an already-registered brain controller.
    /// </summary>
    /// <param name="heartbeat">Heartbeat identifying the controller to update.</param>
    /// <param name="cancellationToken">Cancels the database write.</param>
    /// <returns><see langword="true"/> when the controller exists and was updated; otherwise <see langword="false"/>.</returns>
    public async Task<bool> RecordBrainControllerHeartbeatAsync(
        BrainControllerHeartbeat heartbeat,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(heartbeat.BrainId, nameof(heartbeat), "BrainId");

        await using var connection = await OpenConnectionAsync(cancellationToken);
        var updateCount = await connection.ExecuteAsync(
            new CommandDefinition(
                UpdateBrainControllerHeartbeatSql,
                new
                {
                    brain_id = ToDatabaseId(heartbeat.BrainId),
                    last_seen_ms = NowMs()
                },
                cancellationToken: cancellationToken));

        return updateCount > 0;
    }

    /// <summary>
    /// Inserts or refreshes the persisted state row for a brain.
    /// </summary>
    /// <param name="brainId">Brain identifier to upsert.</param>
    /// <param name="state">Persisted runtime state label.</param>
    /// <param name="spawnedMs">Spawn timestamp in milliseconds.</param>
    /// <param name="lastTickId">Last completed tick at the time of persistence.</param>
    /// <param name="baseNbnSha256">Optional base artifact hash.</param>
    /// <param name="lastSnapshotSha256">Optional last snapshot artifact hash.</param>
    /// <param name="notes">Optional operator-visible notes.</param>
    /// <param name="cancellationToken">Cancels the database write.</param>
    public async Task UpsertBrainAsync(
        Guid brainId,
        string state,
        long spawnedMs,
        long lastTickId = 0,
        byte[]? baseNbnSha256 = null,
        byte[]? lastSnapshotSha256 = null,
        string? notes = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(brainId, nameof(brainId), "BrainId");
        ThrowIfNullOrWhiteSpace(state, nameof(state), "State");

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(
            new CommandDefinition(
                UpsertBrainSql,
                new
                {
                    brain_id = ToDatabaseId(brainId),
                    base_nbn_sha256 = baseNbnSha256,
                    last_snapshot_sha256 = lastSnapshotSha256,
                    spawned_ms = spawnedMs,
                    last_tick_id = lastTickId,
                    state,
                    notes
                },
                cancellationToken: cancellationToken));
    }

    /// <summary>
    /// Updates the persisted state label for a brain.
    /// </summary>
    /// <param name="brainId">Brain identifier to update.</param>
    /// <param name="state">New runtime state label.</param>
    /// <param name="notes">Optional replacement notes when provided.</param>
    /// <param name="cancellationToken">Cancels the database write.</param>
    public async Task UpdateBrainStateAsync(
        Guid brainId,
        string state,
        string? notes = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(brainId, nameof(brainId), "BrainId");
        ThrowIfNullOrWhiteSpace(state, nameof(state), "State");

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(
            new CommandDefinition(
                UpdateBrainStateSql,
                new
                {
                    brain_id = ToDatabaseId(brainId),
                    state,
                    notes
                },
                cancellationToken: cancellationToken));
    }

    /// <summary>
    /// Updates the last completed tick for a brain.
    /// </summary>
    /// <param name="brainId">Brain identifier to update.</param>
    /// <param name="lastTickId">Last completed tick identifier.</param>
    /// <param name="cancellationToken">Cancels the database write.</param>
    public async Task UpdateBrainTickAsync(
        Guid brainId,
        long lastTickId,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(brainId, nameof(brainId), "BrainId");

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(
            new CommandDefinition(
                UpdateBrainTickSql,
                new
                {
                    brain_id = ToDatabaseId(brainId),
                    last_tick_id = lastTickId
                },
                cancellationToken: cancellationToken));
    }

    /// <summary>
    /// Marks a brain controller as offline without removing the controller record.
    /// </summary>
    /// <param name="brainId">Brain identifier whose controller should be marked offline.</param>
    /// <param name="timeMs">Optional offline observation time in milliseconds.</param>
    /// <param name="cancellationToken">Cancels the database write.</param>
    public async Task MarkBrainControllerOfflineAsync(
        Guid brainId,
        long? timeMs = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(brainId, nameof(brainId), "BrainId");

        var nowMs = timeMs ?? NowMs();
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(
            new CommandDefinition(
                MarkBrainControllerOfflineSql,
                new
                {
                    brain_id = ToDatabaseId(brainId),
                    last_seen_ms = nowMs
                },
                cancellationToken: cancellationToken));
    }

    /// <summary>
    /// Looks up a single persisted brain controller row.
    /// </summary>
    /// <param name="brainId">Brain identifier to fetch.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    public async Task<BrainControllerStatus?> GetBrainControllerAsync(
        Guid brainId,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(brainId, nameof(brainId), "BrainId");

        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<BrainControllerStatus>(
            new CommandDefinition(
                GetBrainControllerSql,
                new { brain_id = ToDatabaseId(brainId) },
                cancellationToken: cancellationToken));
    }

    /// <summary>
    /// Looks up a single persisted brain state row.
    /// </summary>
    /// <param name="brainId">Brain identifier to fetch.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    public async Task<BrainStatus?> GetBrainAsync(
        Guid brainId,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(brainId, nameof(brainId), "BrainId");

        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<BrainStatus>(
            new CommandDefinition(
                GetBrainSql,
                new { brain_id = ToDatabaseId(brainId) },
                cancellationToken: cancellationToken));
    }

    /// <summary>
    /// Lists all persisted brain controller rows.
    /// </summary>
    /// <param name="cancellationToken">Cancels the query.</param>
    public async Task<IReadOnlyList<BrainControllerStatus>> ListBrainControllersAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<BrainControllerStatus>(
            new CommandDefinition(ListBrainControllersSql, cancellationToken: cancellationToken));
        return rows.AsList();
    }

    /// <summary>
    /// Lists all persisted brain state rows.
    /// </summary>
    /// <param name="cancellationToken">Cancels the query.</param>
    public async Task<IReadOnlyList<BrainStatus>> ListBrainsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<BrainStatus>(
            new CommandDefinition(ListBrainsSql, cancellationToken: cancellationToken));
        return rows.AsList();
    }
}
