using Dapper;
using Microsoft.Data.Sqlite;

namespace Nbn.Runtime.SettingsMonitor;

public sealed class SettingsMonitorStore
{
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

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen_ms);
CREATE INDEX IF NOT EXISTS idx_node_caps_node_time ON node_capabilities(node_id, time_ms);
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

    private const string MarkOfflineSql = """
UPDATE nodes
SET is_alive = 0,
    last_seen_ms = @last_seen_ms
WHERE node_id = @node_id;
""";

    private const string UpsertSettingSql = """
INSERT INTO settings (key, value, updated_ms)
VALUES (@key, @value, @updated_ms)
ON CONFLICT(key) DO UPDATE SET
    value = excluded.value,
    updated_ms = excluded.updated_ms;
""";

    private const string GetSettingSql = """
SELECT key AS Key, value AS Value, updated_ms AS UpdatedMs
FROM settings
WHERE key = @key;
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

    private readonly string _databasePath;
    private readonly string _connectionString;
    private readonly TimeProvider _timeProvider;

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

    public async Task<IReadOnlyList<NodeStatus>> ListNodesAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<NodeStatus>(
            new CommandDefinition(ListNodesSql, cancellationToken: cancellationToken));
        return rows.AsList();
    }

    private long NowMs() => _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

    private async Task<SqliteConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(PragmaSql, cancellationToken: cancellationToken));
        return connection;
    }
}
