using System.Data;
using System.Globalization;
using Dapper;
using Microsoft.Data.Sqlite;

namespace Nbn.Runtime.SettingsMonitor;

public sealed class SettingsMonitorStore
{
    static SettingsMonitorStore()
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
CREATE TABLE IF NOT EXISTS nodes (
    node_id TEXT PRIMARY KEY,
    logical_name TEXT NOT NULL,
    address TEXT NOT NULL,
    root_actor_name TEXT NOT NULL,
    last_seen_ms INTEGER NOT NULL,
    is_alive INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS node_capabilities (
    node_id TEXT NOT NULL,
    time_ms INTEGER NOT NULL,
    cpu_cores INTEGER NOT NULL,
    ram_free_bytes INTEGER NOT NULL,
    has_gpu INTEGER NOT NULL,
    gpu_name TEXT NOT NULL,
    vram_free_bytes INTEGER NOT NULL,
    cpu_score REAL NOT NULL,
    gpu_score REAL NOT NULL,
    ilgpu_cuda_available INTEGER NOT NULL,
    ilgpu_opencl_available INTEGER NOT NULL,
    PRIMARY KEY (node_id, time_ms),
    FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS brain_controllers (
    brain_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    actor_name TEXT NOT NULL,
    last_seen_ms INTEGER NOT NULL,
    is_alive INTEGER NOT NULL,
    FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS brains (
    brain_id TEXT PRIMARY KEY,
    base_nbn_sha256 BLOB NULL,
    last_snapshot_sha256 BLOB NULL,
    spawned_ms INTEGER NOT NULL,
    last_tick_id INTEGER NOT NULL,
    state TEXT NOT NULL,
    notes TEXT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen_ms);
CREATE INDEX IF NOT EXISTS idx_node_caps_node_time ON node_capabilities(node_id, time_ms);
CREATE INDEX IF NOT EXISTS idx_brain_controllers_last_seen ON brain_controllers(last_seen_ms);
CREATE INDEX IF NOT EXISTS idx_brain_controllers_node ON brain_controllers(node_id);
CREATE INDEX IF NOT EXISTS idx_brains_state ON brains(state);
CREATE INDEX IF NOT EXISTS idx_brains_last_tick ON brains(last_tick_id);
""";

    private const string UpsertNodeSql = """
INSERT INTO nodes (node_id, logical_name, address, root_actor_name, last_seen_ms, is_alive)
VALUES (@node_id, @logical_name, @address, @root_actor_name, @last_seen_ms, @is_alive)
ON CONFLICT(node_id) DO UPDATE SET
    logical_name = excluded.logical_name,
    address = excluded.address,
    root_actor_name = excluded.root_actor_name,
    last_seen_ms = excluded.last_seen_ms,
    is_alive = excluded.is_alive;
""";

    private const string UpdateHeartbeatSql = """
UPDATE nodes
SET last_seen_ms = @last_seen_ms,
    is_alive = 1
WHERE node_id = @node_id;
""";

    private const string InsertCapabilitiesSql = """
INSERT INTO node_capabilities (
    node_id,
    time_ms,
    cpu_cores,
    ram_free_bytes,
    has_gpu,
    gpu_name,
    vram_free_bytes,
    cpu_score,
    gpu_score,
    ilgpu_cuda_available,
    ilgpu_opencl_available
) VALUES (
    @node_id,
    @time_ms,
    @cpu_cores,
    @ram_free_bytes,
    @has_gpu,
    @gpu_name,
    @vram_free_bytes,
    @cpu_score,
    @gpu_score,
    @ilgpu_cuda_available,
    @ilgpu_opencl_available
);
""";

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

    private const string MarkOfflineSql = """
UPDATE nodes
SET is_alive = 0,
    last_seen_ms = @last_seen_ms
WHERE node_id = @node_id;
""";

    private const string MarkBrainControllerOfflineSql = """
UPDATE brain_controllers
SET is_alive = 0,
    last_seen_ms = @last_seen_ms
WHERE brain_id = @brain_id;
""";

    private const string UpsertSettingSql = """
INSERT INTO settings (key, value, updated_ms)
VALUES (@key, @value, @updated_ms)
ON CONFLICT(key) DO UPDATE SET
    value = excluded.value,
    updated_ms = excluded.updated_ms;
""";

    private const string InsertSettingIfMissingSql = """
INSERT INTO settings (key, value, updated_ms)
VALUES (@key, @value, @updated_ms)
ON CONFLICT(key) DO NOTHING;
""";

    private const string GetSettingSql = """
SELECT key AS Key, value AS Value, updated_ms AS UpdatedMs
FROM settings
WHERE key = @key;
""";

    private const string ListSettingsSql = """
SELECT key AS Key, value AS Value, updated_ms AS UpdatedMs
FROM settings
ORDER BY key;
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

    private const string GetNodeSql = """
SELECT
    node_id AS NodeId,
    logical_name AS LogicalName,
    address AS Address,
    root_actor_name AS RootActorName,
    last_seen_ms AS LastSeenMs,
    is_alive AS IsAlive
FROM nodes
WHERE node_id = @node_id;
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

    private const string ListNodesSql = """
SELECT
    node_id AS NodeId,
    logical_name AS LogicalName,
    address AS Address,
    root_actor_name AS RootActorName,
    last_seen_ms AS LastSeenMs,
    is_alive AS IsAlive
FROM nodes
ORDER BY logical_name, node_id;
""";

    private const string ListWorkerInventorySnapshotSql = """
SELECT
    n.node_id AS NodeId,
    n.logical_name AS LogicalName,
    n.address AS Address,
    n.root_actor_name AS RootActorName,
    n.last_seen_ms AS LastSeenMs,
    n.is_alive AS IsAlive,
    CASE
        WHEN c.time_ms IS NULL THEN 0
        ELSE 1
    END AS HasCapabilities,
    COALESCE(c.time_ms, 0) AS CapabilityTimeMs,
    COALESCE(c.cpu_cores, 0) AS CpuCores,
    COALESCE(c.ram_free_bytes, 0) AS RamFreeBytes,
    COALESCE(c.has_gpu, 0) AS HasGpu,
    COALESCE(c.gpu_name, '') AS GpuName,
    COALESCE(c.vram_free_bytes, 0) AS VramFreeBytes,
    COALESCE(c.cpu_score, 0.0) AS CpuScore,
    COALESCE(c.gpu_score, 0.0) AS GpuScore,
    COALESCE(c.ilgpu_cuda_available, 0) AS IlgpuCudaAvailable,
    COALESCE(c.ilgpu_opencl_available, 0) AS IlgpuOpenclAvailable,
    CASE
        WHEN n.is_alive = 1 AND c.time_ms IS NOT NULL THEN 1
        ELSE 0
    END AS IsReady
FROM nodes AS n
LEFT JOIN node_capabilities AS c
    ON c.node_id = n.node_id
   AND c.time_ms = (
       SELECT MAX(c2.time_ms)
       FROM node_capabilities AS c2
       WHERE c2.node_id = n.node_id
   )
ORDER BY n.logical_name, n.node_id;
""";

    private readonly string _databasePath;
    private readonly string _connectionString;
    private readonly TimeProvider _timeProvider;
    private static bool _handlersRegistered;
    private static readonly object _handlerLock = new();

    public SettingsMonitorStore(string databasePath, TimeProvider? timeProvider = null)
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

    private static void RegisterTypeHandlers()
    {
        lock (_handlerLock)
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

    public async Task UpsertNodeAsync(
        NodeRegistration registration,
        long? timeMs = null,
        CancellationToken cancellationToken = default)
    {
        if (registration.NodeId == Guid.Empty)
        {
            throw new ArgumentException("NodeId is required.", nameof(registration));
        }

        if (string.IsNullOrWhiteSpace(registration.LogicalName))
        {
            throw new ArgumentException("LogicalName is required.", nameof(registration));
        }

        if (string.IsNullOrWhiteSpace(registration.Address))
        {
            throw new ArgumentException("Address is required.", nameof(registration));
        }

        if (string.IsNullOrWhiteSpace(registration.RootActorName))
        {
            throw new ArgumentException("RootActorName is required.", nameof(registration));
        }

        var nowMs = timeMs ?? NowMs();
        var parameters = new
        {
            node_id = registration.NodeId.ToString("D"),
            logical_name = registration.LogicalName,
            address = registration.Address,
            root_actor_name = registration.RootActorName,
            last_seen_ms = nowMs,
            is_alive = 1
        };

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(UpsertNodeSql, parameters, cancellationToken: cancellationToken));
    }

    public async Task MarkNodeOfflineAsync(
        Guid nodeId,
        long? timeMs = null,
        CancellationToken cancellationToken = default)
    {
        if (nodeId == Guid.Empty)
        {
            throw new ArgumentException("NodeId is required.", nameof(nodeId));
        }

        var nowMs = timeMs ?? NowMs();
        var parameters = new
        {
            node_id = nodeId.ToString("D"),
            last_seen_ms = nowMs
        };

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(MarkOfflineSql, parameters, cancellationToken: cancellationToken));
    }

    public async Task<bool> RecordHeartbeatAsync(
        NodeHeartbeat heartbeat,
        CancellationToken cancellationToken = default)
    {
        if (heartbeat.NodeId == Guid.Empty)
        {
            throw new ArgumentException("NodeId is required.", nameof(heartbeat));
        }

        var capabilities = heartbeat.Capabilities ?? throw new ArgumentException("Capabilities are required.", nameof(heartbeat));

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var updateCount = await connection.ExecuteAsync(
            new CommandDefinition(
                UpdateHeartbeatSql,
                new
                {
                    node_id = heartbeat.NodeId.ToString("D"),
                    last_seen_ms = heartbeat.TimeMs
                },
                transaction,
                cancellationToken: cancellationToken));

        if (updateCount == 0)
        {
            await transaction.RollbackAsync(cancellationToken);
            return false;
        }

        var parameters = new
        {
            node_id = heartbeat.NodeId.ToString("D"),
            time_ms = heartbeat.TimeMs,
            cpu_cores = capabilities.CpuCores,
            ram_free_bytes = capabilities.RamFreeBytes,
            has_gpu = capabilities.HasGpu ? 1 : 0,
            gpu_name = capabilities.GpuName ?? string.Empty,
            vram_free_bytes = capabilities.VramFreeBytes,
            cpu_score = capabilities.CpuScore,
            gpu_score = capabilities.GpuScore,
            ilgpu_cuda_available = capabilities.IlgpuCudaAvailable ? 1 : 0,
            ilgpu_opencl_available = capabilities.IlgpuOpenclAvailable ? 1 : 0
        };

        await connection.ExecuteAsync(
            new CommandDefinition(
                InsertCapabilitiesSql,
                parameters,
                transaction,
                cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    public async Task UpsertBrainControllerAsync(
        BrainControllerRegistration registration,
        long? timeMs = null,
        CancellationToken cancellationToken = default)
    {
        if (registration.BrainId == Guid.Empty)
        {
            throw new ArgumentException("BrainId is required.", nameof(registration));
        }

        if (registration.NodeId == Guid.Empty)
        {
            throw new ArgumentException("NodeId is required.", nameof(registration));
        }

        if (string.IsNullOrWhiteSpace(registration.ActorName))
        {
            throw new ArgumentException("ActorName is required.", nameof(registration));
        }

        var nowMs = timeMs ?? NowMs();
        var parameters = new
        {
            brain_id = registration.BrainId.ToString("D"),
            node_id = registration.NodeId.ToString("D"),
            actor_name = registration.ActorName,
            last_seen_ms = nowMs,
            is_alive = 1
        };

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(UpsertBrainControllerSql, parameters, cancellationToken: cancellationToken));
    }

    public async Task<bool> RecordBrainControllerHeartbeatAsync(
        BrainControllerHeartbeat heartbeat,
        CancellationToken cancellationToken = default)
    {
        if (heartbeat.BrainId == Guid.Empty)
        {
            throw new ArgumentException("BrainId is required.", nameof(heartbeat));
        }

        await using var connection = await OpenConnectionAsync(cancellationToken);
        var updateCount = await connection.ExecuteAsync(
            new CommandDefinition(
                UpdateBrainControllerHeartbeatSql,
                new
                {
                    brain_id = heartbeat.BrainId.ToString("D"),
                    last_seen_ms = heartbeat.TimeMs
                },
                cancellationToken: cancellationToken));

        return updateCount > 0;
    }

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
        if (brainId == Guid.Empty)
        {
            throw new ArgumentException("BrainId is required.", nameof(brainId));
        }

        if (string.IsNullOrWhiteSpace(state))
        {
            throw new ArgumentException("State is required.", nameof(state));
        }

        var parameters = new
        {
            brain_id = brainId.ToString("D"),
            base_nbn_sha256 = baseNbnSha256,
            last_snapshot_sha256 = lastSnapshotSha256,
            spawned_ms = spawnedMs,
            last_tick_id = lastTickId,
            state,
            notes
        };

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(UpsertBrainSql, parameters, cancellationToken: cancellationToken));
    }

    public async Task UpdateBrainStateAsync(
        Guid brainId,
        string state,
        string? notes = null,
        CancellationToken cancellationToken = default)
    {
        if (brainId == Guid.Empty)
        {
            throw new ArgumentException("BrainId is required.", nameof(brainId));
        }

        if (string.IsNullOrWhiteSpace(state))
        {
            throw new ArgumentException("State is required.", nameof(state));
        }

        var parameters = new
        {
            brain_id = brainId.ToString("D"),
            state,
            notes
        };

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(UpdateBrainStateSql, parameters, cancellationToken: cancellationToken));
    }

    public async Task UpdateBrainTickAsync(
        Guid brainId,
        long lastTickId,
        CancellationToken cancellationToken = default)
    {
        if (brainId == Guid.Empty)
        {
            throw new ArgumentException("BrainId is required.", nameof(brainId));
        }

        var parameters = new
        {
            brain_id = brainId.ToString("D"),
            last_tick_id = lastTickId
        };

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(UpdateBrainTickSql, parameters, cancellationToken: cancellationToken));
    }

    public async Task MarkBrainControllerOfflineAsync(
        Guid brainId,
        long? timeMs = null,
        CancellationToken cancellationToken = default)
    {
        if (brainId == Guid.Empty)
        {
            throw new ArgumentException("BrainId is required.", nameof(brainId));
        }

        var nowMs = timeMs ?? NowMs();
        var parameters = new
        {
            brain_id = brainId.ToString("D"),
            last_seen_ms = nowMs
        };

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(MarkBrainControllerOfflineSql, parameters, cancellationToken: cancellationToken));
    }

    public async Task SetSettingAsync(
        string key,
        string value,
        long? updatedMs = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new ArgumentException("Setting key is required.", nameof(key));
        }

        if (value is null)
        {
            throw new ArgumentNullException(nameof(value));
        }

        var timeMs = updatedMs ?? NowMs();
        var parameters = new
        {
            key,
            value,
            updated_ms = timeMs
        };

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(UpsertSettingSql, parameters, cancellationToken: cancellationToken));
    }

    public async Task EnsureDefaultSettingsAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var nowMs = NowMs();

        foreach (var setting in SettingsMonitorDefaults.DefaultSettings)
        {
            await connection.ExecuteAsync(
                new CommandDefinition(
                    InsertSettingIfMissingSql,
                    new
                    {
                        key = setting.Key,
                        value = setting.Value,
                        updated_ms = nowMs
                    },
                    transaction,
                    cancellationToken: cancellationToken));
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<ArtifactCompressionSettings> GetArtifactCompressionSettingsAsync(
        CancellationToken cancellationToken = default)
    {
        var defaults = SettingsMonitorDefaults.ArtifactCompressionDefaults;

        var kindEntry = await GetSettingAsync(SettingsMonitorDefaults.ArtifactChunkCompressionKindKey, cancellationToken);
        var levelEntry = await GetSettingAsync(SettingsMonitorDefaults.ArtifactChunkCompressionLevelKey, cancellationToken);
        var minBytesEntry = await GetSettingAsync(SettingsMonitorDefaults.ArtifactChunkCompressionMinBytesKey, cancellationToken);
        var onlyIfSmallerEntry = await GetSettingAsync(SettingsMonitorDefaults.ArtifactChunkCompressionOnlyIfSmallerKey, cancellationToken);

        var kind = string.IsNullOrWhiteSpace(kindEntry?.Value)
            ? defaults.Kind
            : kindEntry.Value.Trim();

        var level = ParseInt(levelEntry?.Value, defaults.Level);
        var minBytes = ParseInt(minBytesEntry?.Value, defaults.MinBytes);
        var onlyIfSmaller = ParseBool(onlyIfSmallerEntry?.Value, defaults.OnlyIfSmaller);

        return new ArtifactCompressionSettings(kind, level, minBytes, onlyIfSmaller);
    }

    public async Task<SettingEntry?> GetSettingAsync(
        string key,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new ArgumentException("Setting key is required.", nameof(key));
        }

        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<SettingEntry>(
            new CommandDefinition(GetSettingSql, new { key }, cancellationToken: cancellationToken));
    }

    public async Task<IReadOnlyList<SettingEntry>> ListSettingsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<SettingEntry>(
            new CommandDefinition(ListSettingsSql, cancellationToken: cancellationToken));
        return rows.AsList();
    }

    public async Task<NodeStatus?> GetNodeAsync(
        Guid nodeId,
        CancellationToken cancellationToken = default)
    {
        if (nodeId == Guid.Empty)
        {
            throw new ArgumentException("NodeId is required.", nameof(nodeId));
        }

        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<NodeStatus>(
            new CommandDefinition(
                GetNodeSql,
                new { node_id = nodeId.ToString("D") },
                cancellationToken: cancellationToken));
    }

    public async Task<BrainControllerStatus?> GetBrainControllerAsync(
        Guid brainId,
        CancellationToken cancellationToken = default)
    {
        if (brainId == Guid.Empty)
        {
            throw new ArgumentException("BrainId is required.", nameof(brainId));
        }

        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<BrainControllerStatus>(
            new CommandDefinition(
                GetBrainControllerSql,
                new { brain_id = brainId.ToString("D") },
                cancellationToken: cancellationToken));
    }

    public async Task<BrainStatus?> GetBrainAsync(
        Guid brainId,
        CancellationToken cancellationToken = default)
    {
        if (brainId == Guid.Empty)
        {
            throw new ArgumentException("BrainId is required.", nameof(brainId));
        }

        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<BrainStatus>(
            new CommandDefinition(
                GetBrainSql,
                new { brain_id = brainId.ToString("D") },
                cancellationToken: cancellationToken));
    }

    public async Task<IReadOnlyList<BrainControllerStatus>> ListBrainControllersAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<BrainControllerStatus>(
            new CommandDefinition(ListBrainControllersSql, cancellationToken: cancellationToken));
        return rows.AsList();
    }

    public async Task<IReadOnlyList<BrainStatus>> ListBrainsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<BrainStatus>(
            new CommandDefinition(ListBrainsSql, cancellationToken: cancellationToken));
        return rows.AsList();
    }

    public async Task<IReadOnlyList<NodeStatus>> ListNodesAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<NodeStatus>(
            new CommandDefinition(ListNodesSql, cancellationToken: cancellationToken));
        return rows.AsList();
    }

    public async Task<WorkerInventorySnapshot> GetWorkerInventorySnapshotAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<WorkerReadinessCapability>(
            new CommandDefinition(ListWorkerInventorySnapshotSql, cancellationToken: cancellationToken));
        return new WorkerInventorySnapshot(NowMs(), rows.AsList());
    }

    private long NowMs() => _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

    private static int ParseInt(string? value, int fallback)
    {
        return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static bool ParseBool(string? value, bool fallback)
    {
        return bool.TryParse(value, out var parsed) ? parsed : fallback;
    }

    private async Task<SqliteConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(PragmaSql, cancellationToken: cancellationToken));
        return connection;
    }
}
