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
WHERE brain_id = @brain_id
  AND last_seen_ms <= @last_seen_ms
  AND EXISTS (
      SELECT 1
      FROM brains
      WHERE brain_id = @brain_id
        AND state <> 'Dead'
  );
""";

    private const string UpsertBrainSql = """
INSERT INTO brains (
    brain_id,
    base_nbn_sha256,
    last_snapshot_sha256,
    spawned_ms,
    last_tick_id,
    updated_ms,
    state,
    notes
) VALUES (
    @brain_id,
    @base_nbn_sha256,
    @last_snapshot_sha256,
    @spawned_ms,
    @last_tick_id,
    @updated_ms,
    @state,
    @notes
)
ON CONFLICT(brain_id) DO UPDATE SET
    base_nbn_sha256 = COALESCE(excluded.base_nbn_sha256, brains.base_nbn_sha256),
    last_snapshot_sha256 = COALESCE(excluded.last_snapshot_sha256, brains.last_snapshot_sha256),
    spawned_ms = excluded.spawned_ms,
    last_tick_id = excluded.last_tick_id,
    updated_ms = excluded.updated_ms,
    state = excluded.state,
    notes = COALESCE(excluded.notes, brains.notes);
""";

    private const string UpdateBrainStateSql = """
UPDATE brains
SET updated_ms = @updated_ms,
    state = @state,
    notes = COALESCE(@notes, notes)
WHERE brain_id = @brain_id
  AND (state <> 'Dead' OR @state = 'Dead');
""";

    private const string UpdateBrainTickSql = """
UPDATE brains
SET last_tick_id = @last_tick_id,
    updated_ms = @updated_ms
WHERE brain_id = @brain_id
  AND state <> 'Dead';
""";

    private const string MarkBrainControllerOfflineSql = """
UPDATE brain_controllers
SET is_alive = 0,
    last_seen_ms = @last_seen_ms
WHERE brain_id = @brain_id
  AND last_seen_ms <= @last_seen_ms;
""";

    private const string MarkBrainUnregisteredStateSql = """
UPDATE brains
SET updated_ms = @updated_ms,
    state = 'Dead'
WHERE brain_id = @brain_id
  AND (state <> 'Dead' OR updated_ms <= @updated_ms);
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

    private const string DeleteStaleDeadBrainControllersSql = """
DELETE FROM brain_controllers
WHERE brain_id IN (
    SELECT brain_id
    FROM brains
    WHERE state = 'Dead'
      AND updated_ms <= @cutoff_ms
);
""";

    private const string DeleteStaleDeadBrainsSql = """
DELETE FROM brains
WHERE state = 'Dead'
  AND updated_ms <= @cutoff_ms;
""";

    private const string DeleteStaleNonLiveBrainControllersSql = """
DELETE FROM brain_controllers
WHERE brain_id IN (
    SELECT b.brain_id
    FROM brains b
    LEFT JOIN brain_controllers c ON c.brain_id = b.brain_id
    WHERE b.state <> 'Dead'
      AND b.spawned_ms <= @cutoff_ms
      AND b.updated_ms <= @cutoff_ms
      AND (c.brain_id IS NULL OR c.last_seen_ms <= @cutoff_ms)
);
""";

    private const string DeleteStaleNonLiveBrainsSql = """
DELETE FROM brains
WHERE state <> 'Dead'
  AND spawned_ms <= @cutoff_ms
  AND updated_ms <= @cutoff_ms
  AND NOT EXISTS (
      SELECT 1
      FROM brain_controllers c
      WHERE c.brain_id = brains.brain_id
        AND c.last_seen_ms > @cutoff_ms
  );
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
                    last_seen_ms = heartbeat.TimeMs > 0 ? heartbeat.TimeMs : NowMs()
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
    /// <param name="updatedMs">Optional update timestamp used for retention and freshness ordering.</param>
    /// <param name="cancellationToken">Cancels the database write.</param>
    public async Task UpsertBrainAsync(
        Guid brainId,
        string state,
        long spawnedMs,
        long lastTickId = 0,
        byte[]? baseNbnSha256 = null,
        byte[]? lastSnapshotSha256 = null,
        string? notes = null,
        long? updatedMs = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(brainId, nameof(brainId), "BrainId");
        ThrowIfNullOrWhiteSpace(state, nameof(state), "State");

        var observedMs = updatedMs ?? NowMs();
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
                    updated_ms = observedMs,
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
    /// <param name="updatedMs">Optional update timestamp used for retention and freshness ordering.</param>
    /// <param name="cancellationToken">Cancels the database write.</param>
    public async Task UpdateBrainStateAsync(
        Guid brainId,
        string state,
        string? notes = null,
        long? updatedMs = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(brainId, nameof(brainId), "BrainId");
        ThrowIfNullOrWhiteSpace(state, nameof(state), "State");

        var observedMs = updatedMs ?? NowMs();
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(
            new CommandDefinition(
                UpdateBrainStateSql,
                new
                {
                    brain_id = ToDatabaseId(brainId),
                    updated_ms = observedMs,
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
    /// <param name="updatedMs">Optional update timestamp used for retention and freshness ordering.</param>
    /// <param name="cancellationToken">Cancels the database write.</param>
    public async Task UpdateBrainTickAsync(
        Guid brainId,
        long lastTickId,
        long? updatedMs = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(brainId, nameof(brainId), "BrainId");

        var observedMs = updatedMs ?? NowMs();
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(
            new CommandDefinition(
                UpdateBrainTickSql,
                new
                {
                    brain_id = ToDatabaseId(brainId),
                    last_tick_id = lastTickId,
                    updated_ms = observedMs
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
    /// Atomically marks a brain as dead and its controller row offline using one observed timestamp.
    /// </summary>
    /// <param name="brainId">Brain identifier whose persisted state should be finalized.</param>
    /// <param name="timeMs">Optional unregistration time in milliseconds.</param>
    /// <param name="cancellationToken">Cancels the database write.</param>
    public async Task MarkBrainUnregisteredAsync(
        Guid brainId,
        long? timeMs = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(brainId, nameof(brainId), "BrainId");

        var nowMs = timeMs ?? NowMs();
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await connection.ExecuteAsync(
            new CommandDefinition(
                MarkBrainUnregisteredStateSql,
                new
                {
                    brain_id = ToDatabaseId(brainId),
                    updated_ms = nowMs
                },
                transaction,
                cancellationToken: cancellationToken));
        await connection.ExecuteAsync(
            new CommandDefinition(
                MarkBrainControllerOfflineSql,
                new
                {
                    brain_id = ToDatabaseId(brainId),
                    last_seen_ms = nowMs
                },
                transaction,
                cancellationToken: cancellationToken));
        await transaction.CommitAsync(cancellationToken);
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

    /// <summary>
    /// Deletes persisted dead brain rows and matching controller rows older than the supplied cutoff.
    /// </summary>
    /// <param name="cutoffMs">Inclusive retention cutoff in milliseconds.</param>
    /// <param name="cancellationToken">Cancels the prune operation.</param>
    public async Task<PrunedBrainRows> PruneStaleDeadBrainsAsync(
        long cutoffMs,
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var deletedControllers = await connection.ExecuteAsync(
            new CommandDefinition(
                DeleteStaleDeadBrainControllersSql,
                new { cutoff_ms = cutoffMs },
                transaction,
                cancellationToken: cancellationToken));
        var deletedBrains = await connection.ExecuteAsync(
            new CommandDefinition(
                DeleteStaleDeadBrainsSql,
                new { cutoff_ms = cutoffMs },
                transaction,
                cancellationToken: cancellationToken));
        await transaction.CommitAsync(cancellationToken);
        return new PrunedBrainRows(deletedBrains, deletedControllers);
    }

    /// <summary>
    /// Deletes persisted non-dead brain rows whose spawn age, last brain update, and controller heartbeat are all older than the supplied cutoff.
    /// </summary>
    /// <param name="cutoffMs">Inclusive retention cutoff in milliseconds.</param>
    /// <param name="cancellationToken">Cancels the prune operation.</param>
    public async Task<PrunedBrainRows> PruneStaleNonLiveBrainsAsync(
        long cutoffMs,
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var deletedControllers = await connection.ExecuteAsync(
            new CommandDefinition(
                DeleteStaleNonLiveBrainControllersSql,
                new { cutoff_ms = cutoffMs },
                transaction,
                cancellationToken: cancellationToken));
        var deletedBrains = await connection.ExecuteAsync(
            new CommandDefinition(
                DeleteStaleNonLiveBrainsSql,
                new { cutoff_ms = cutoffMs },
                transaction,
                cancellationToken: cancellationToken));
        await transaction.CommitAsync(cancellationToken);
        return new PrunedBrainRows(deletedBrains, deletedControllers);
    }

    public sealed record PrunedBrainRows(int DeletedBrains, int DeletedControllers);
}
