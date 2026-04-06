using Dapper;
using Microsoft.Data.Sqlite;
using Nbn.Runtime.SettingsMonitor;
using Nbn.Tests.TestSupport;

namespace Nbn.Tests.SettingsMonitor;

public sealed class SettingsMonitorBrainLifecycleTests
{
    [Fact]
    public async Task DeadBrain_IgnoresLateControllerHeartbeat_StateChange_AndTick()
    {
        using var db = new TempDatabaseScope("settings-monitor.db");
        var store = new SettingsMonitorStore(db.DatabasePath);
        await store.InitializeAsync();

        var brainId = Guid.NewGuid();
        var nodeId = Guid.NewGuid();

        await store.UpsertBrainAsync(brainId, "Active", spawnedMs: 100, lastTickId: 12);
        await store.UpsertNodeAsync(new NodeRegistration(nodeId, "worker-a", "127.0.0.1:12041", "worker-node"), timeMs: 100);
        await store.UpsertBrainControllerAsync(
            new BrainControllerRegistration(brainId, nodeId, "worker-node/brain-root"),
            timeMs: 150);

        await store.MarkBrainControllerOfflineAsync(brainId, timeMs: 300);
        await store.UpdateBrainStateAsync(brainId, "Dead");

        await store.RecordBrainControllerHeartbeatAsync(new BrainControllerHeartbeat(brainId, 200));
        await store.UpdateBrainStateAsync(brainId, "Recovering");
        await store.UpdateBrainTickAsync(brainId, 99);

        var brain = await store.GetBrainAsync(brainId);
        var controller = await store.GetBrainControllerAsync(brainId);

        Assert.NotNull(brain);
        Assert.Equal("Dead", brain!.State);
        Assert.Equal(12, brain.LastTickId);

        Assert.NotNull(controller);
        Assert.False(controller!.IsAlive);
        Assert.Equal(300, controller.LastSeenMs);
    }

    [Fact]
    public async Task ExplicitReRegistration_RevivesDeadBrainAndController()
    {
        using var db = new TempDatabaseScope("settings-monitor.db");
        var store = new SettingsMonitorStore(db.DatabasePath);
        await store.InitializeAsync();

        var brainId = Guid.NewGuid();
        var nodeId = Guid.NewGuid();

        await store.UpsertBrainAsync(brainId, "Active", spawnedMs: 100, lastTickId: 12);
        await store.UpsertNodeAsync(new NodeRegistration(nodeId, "worker-a", "127.0.0.1:12041", "worker-node"), timeMs: 100);
        await store.UpsertBrainControllerAsync(
            new BrainControllerRegistration(brainId, nodeId, "worker-node/brain-root"),
            timeMs: 150);
        await store.MarkBrainControllerOfflineAsync(brainId, timeMs: 300);
        await store.UpdateBrainStateAsync(brainId, "Dead");

        await store.UpsertBrainAsync(brainId, "Recovering", spawnedMs: 500, lastTickId: 0);
        await store.UpsertBrainControllerAsync(
            new BrainControllerRegistration(brainId, nodeId, "worker-node/brain-root"),
            timeMs: 500);
        await store.UpdateBrainStateAsync(brainId, "Active");
        await store.RecordBrainControllerHeartbeatAsync(new BrainControllerHeartbeat(brainId, 550));
        await store.UpdateBrainTickAsync(brainId, 77);

        var brain = await store.GetBrainAsync(brainId);
        var controller = await store.GetBrainControllerAsync(brainId);

        Assert.NotNull(brain);
        Assert.Equal("Active", brain!.State);
        Assert.Equal(77, brain.LastTickId);

        Assert.NotNull(controller);
        Assert.True(controller!.IsAlive);
        Assert.Equal(550, controller.LastSeenMs);
    }

    [Fact]
    public async Task MarkBrainUnregisteredAsync_StoresDeadState_And_ControllerOffline_Atomically()
    {
        using var db = new TempDatabaseScope("settings-monitor.db");
        var store = new SettingsMonitorStore(db.DatabasePath);
        await store.InitializeAsync();

        var brainId = Guid.NewGuid();
        var nodeId = Guid.NewGuid();

        await store.UpsertBrainAsync(brainId, "Active", spawnedMs: 100, lastTickId: 12, updatedMs: 100);
        await store.UpsertNodeAsync(new NodeRegistration(nodeId, "worker-a", "127.0.0.1:12041", "worker-node"), timeMs: 100);
        await store.UpsertBrainControllerAsync(
            new BrainControllerRegistration(brainId, nodeId, "worker-node/brain-root"),
            timeMs: 150);

        await store.MarkBrainUnregisteredAsync(brainId, timeMs: 300);

        var brain = await store.GetBrainAsync(brainId);
        var controller = await store.GetBrainControllerAsync(brainId);

        Assert.NotNull(brain);
        Assert.Equal("Dead", brain!.State);
        Assert.NotNull(controller);
        Assert.False(controller!.IsAlive);
        Assert.Equal(300, controller.LastSeenMs);
    }

    [Fact]
    public async Task PruneStaleDeadBrainsAsync_RemovesOnlyDeadRowsOlderThanCutoff()
    {
        using var db = new TempDatabaseScope("settings-monitor.db");
        var store = new SettingsMonitorStore(db.DatabasePath);
        await store.InitializeAsync();

        var nodeId = Guid.NewGuid();
        var oldDeadBrainId = Guid.NewGuid();
        var recentDeadBrainId = Guid.NewGuid();
        var activeBrainId = Guid.NewGuid();

        await store.UpsertNodeAsync(new NodeRegistration(nodeId, "worker-a", "127.0.0.1:12041", "worker-node"), timeMs: 100);

        await store.UpsertBrainAsync(oldDeadBrainId, "Active", spawnedMs: 100, lastTickId: 1, updatedMs: 100);
        await store.UpsertBrainControllerAsync(
            new BrainControllerRegistration(oldDeadBrainId, nodeId, "worker-node/brain-old-root"),
            timeMs: 100);
        await store.MarkBrainControllerOfflineAsync(oldDeadBrainId, timeMs: 150);
        await store.UpdateBrainStateAsync(oldDeadBrainId, "Dead", updatedMs: 200);

        await store.UpsertBrainAsync(recentDeadBrainId, "Active", spawnedMs: 300, lastTickId: 2, updatedMs: 300);
        await store.UpsertBrainControllerAsync(
            new BrainControllerRegistration(recentDeadBrainId, nodeId, "worker-node/brain-recent-root"),
            timeMs: 300);
        await store.MarkBrainControllerOfflineAsync(recentDeadBrainId, timeMs: 900);
        await store.UpdateBrainStateAsync(recentDeadBrainId, "Dead", updatedMs: 900);

        await store.UpsertBrainAsync(activeBrainId, "Active", spawnedMs: 400, lastTickId: 3, updatedMs: 400);
        await store.UpsertBrainControllerAsync(
            new BrainControllerRegistration(activeBrainId, nodeId, "worker-node/brain-live-root"),
            timeMs: 400);

        var result = await store.PruneStaleDeadBrainsAsync(cutoffMs: 500);

        Assert.Equal(1, result.DeletedBrains);
        Assert.Equal(1, result.DeletedControllers);
        Assert.Null(await store.GetBrainAsync(oldDeadBrainId));
        Assert.Null(await store.GetBrainControllerAsync(oldDeadBrainId));
        Assert.NotNull(await store.GetBrainAsync(recentDeadBrainId));
        Assert.NotNull(await store.GetBrainControllerAsync(recentDeadBrainId));
        Assert.NotNull(await store.GetBrainAsync(activeBrainId));
        Assert.NotNull(await store.GetBrainControllerAsync(activeBrainId));
    }

    [Fact]
    public async Task PruneStaleNonLiveBrainsAsync_RemovesOnlyRowsWhoseBrainAndControllerAreBothStale()
    {
        using var db = new TempDatabaseScope("settings-monitor.db");
        var store = new SettingsMonitorStore(db.DatabasePath);
        await store.InitializeAsync();

        var nodeId = Guid.NewGuid();
        var staleActiveBrainId = Guid.NewGuid();
        var staleRecoveringBrainId = Guid.NewGuid();
        var freshControllerBrainId = Guid.NewGuid();
        var recentBrainId = Guid.NewGuid();
        var deadBrainId = Guid.NewGuid();

        await store.UpsertNodeAsync(new NodeRegistration(nodeId, "worker-a", "127.0.0.1:12041", "worker-node"), timeMs: 100);

        await store.UpsertBrainAsync(staleActiveBrainId, "Active", spawnedMs: 100, lastTickId: 1, updatedMs: 100);
        await store.UpsertBrainControllerAsync(
            new BrainControllerRegistration(staleActiveBrainId, nodeId, "worker-node/brain-stale-active-root"),
            timeMs: 100);

        await store.UpsertBrainAsync(staleRecoveringBrainId, "Recovering", spawnedMs: 120, lastTickId: 2, updatedMs: 120);

        await store.UpsertBrainAsync(freshControllerBrainId, "Active", spawnedMs: 140, lastTickId: 3, updatedMs: 140);
        await store.UpsertBrainControllerAsync(
            new BrainControllerRegistration(freshControllerBrainId, nodeId, "worker-node/brain-fresh-controller-root"),
            timeMs: 900);

        await store.UpsertBrainAsync(recentBrainId, "Recovering", spawnedMs: 160, lastTickId: 4, updatedMs: 900);

        await store.UpsertBrainAsync(deadBrainId, "Dead", spawnedMs: 180, lastTickId: 5, updatedMs: 100);
        await store.UpsertBrainControllerAsync(
            new BrainControllerRegistration(deadBrainId, nodeId, "worker-node/brain-dead-root"),
            timeMs: 100);

        var result = await store.PruneStaleNonLiveBrainsAsync(cutoffMs: 500);

        Assert.Equal(2, result.DeletedBrains);
        Assert.Equal(1, result.DeletedControllers);
        Assert.Null(await store.GetBrainAsync(staleActiveBrainId));
        Assert.Null(await store.GetBrainControllerAsync(staleActiveBrainId));
        Assert.Null(await store.GetBrainAsync(staleRecoveringBrainId));
        Assert.NotNull(await store.GetBrainAsync(freshControllerBrainId));
        Assert.NotNull(await store.GetBrainControllerAsync(freshControllerBrainId));
        Assert.NotNull(await store.GetBrainAsync(recentBrainId));
        Assert.NotNull(await store.GetBrainAsync(deadBrainId));
        Assert.NotNull(await store.GetBrainControllerAsync(deadBrainId));
    }

    [Fact]
    public async Task InitializeAsync_MigratesLegacyBrainsTable_ToIncludeUpdatedMs_And_BackfillsLegacyRows()
    {
        using var db = new TempDatabaseScope("settings-monitor.db");
        var timeProvider = new MutableTimeProvider(1_000_000);
        var legacyDeadBrainId = Guid.NewGuid();
        await using (var connection = new SqliteConnection($"Data Source={db.DatabasePath}"))
        {
            await connection.OpenAsync();
            await connection.ExecuteAsync(
                """
                CREATE TABLE brains (
                    brain_id TEXT PRIMARY KEY,
                    base_nbn_sha256 BLOB NULL,
                    last_snapshot_sha256 BLOB NULL,
                    spawned_ms INTEGER NOT NULL,
                    last_tick_id INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    notes TEXT NULL
                );
                """);
            await connection.ExecuteAsync(
                """
                INSERT INTO brains (brain_id, base_nbn_sha256, last_snapshot_sha256, spawned_ms, last_tick_id, state, notes)
                VALUES (@brain_id, NULL, NULL, 250, 12, 'Dead', NULL);
                """,
                new { brain_id = legacyDeadBrainId.ToString("D") });
        }

        var store = new SettingsMonitorStore(db.DatabasePath, timeProvider);
        await store.InitializeAsync();

        await using var migratedConnection = new SqliteConnection($"Data Source={db.DatabasePath}");
        await migratedConnection.OpenAsync();
        var columns = (await migratedConnection.QueryAsync<string>("SELECT name FROM pragma_table_info('brains');")).ToList();
        var updatedMs = await migratedConnection.ExecuteScalarAsync<long>(
            "SELECT updated_ms FROM brains WHERE brain_id = @brain_id;",
            new { brain_id = legacyDeadBrainId.ToString("D") });

        Assert.Contains("updated_ms", columns, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(1_000_000, updatedMs);

        var pruneResult = await store.PruneStaleDeadBrainsAsync(cutoffMs: 999_999);
        Assert.Equal(0, pruneResult.DeletedBrains);
        Assert.NotNull(await store.GetBrainAsync(legacyDeadBrainId));
    }

    private sealed class MutableTimeProvider : TimeProvider
    {
        private DateTimeOffset _utcNow;

        public MutableTimeProvider(long utcNowMs)
        {
            _utcNow = DateTimeOffset.FromUnixTimeMilliseconds(utcNowMs);
        }

        public override DateTimeOffset GetUtcNow() => _utcNow;
    }
}
