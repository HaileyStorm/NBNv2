using Dapper;
using Microsoft.Data.Sqlite;

namespace Nbn.Runtime.SettingsMonitor;

public sealed partial class SettingsMonitorStore
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
    storage_free_bytes INTEGER NOT NULL DEFAULT 0,
    has_gpu INTEGER NOT NULL,
    gpu_name TEXT NOT NULL,
    vram_free_bytes INTEGER NOT NULL,
    cpu_score REAL NOT NULL,
    gpu_score REAL NOT NULL,
    ilgpu_cuda_available INTEGER NOT NULL,
    ilgpu_opencl_available INTEGER NOT NULL,
    ram_total_bytes INTEGER NOT NULL DEFAULT 0,
    storage_total_bytes INTEGER NOT NULL DEFAULT 0,
    vram_total_bytes INTEGER NOT NULL DEFAULT 0,
    cpu_limit_percent INTEGER NOT NULL DEFAULT 0,
    ram_limit_percent INTEGER NOT NULL DEFAULT 0,
    storage_limit_percent INTEGER NOT NULL DEFAULT 0,
    gpu_compute_limit_percent INTEGER NOT NULL DEFAULT 0,
    gpu_vram_limit_percent INTEGER NOT NULL DEFAULT 0,
    process_cpu_load_percent REAL NOT NULL DEFAULT 0,
    process_ram_used_bytes INTEGER NOT NULL DEFAULT 0,
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
    updated_ms INTEGER NOT NULL DEFAULT 0,
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

    private const string NodeCapabilitiesTableInfoSql = "PRAGMA table_info(node_capabilities);";

    private const string AddStorageFreeBytesColumnSql = """
ALTER TABLE node_capabilities
ADD COLUMN storage_free_bytes INTEGER NOT NULL DEFAULT 0;
""";

    private const string AddRamTotalBytesColumnSql = """
ALTER TABLE node_capabilities
ADD COLUMN ram_total_bytes INTEGER NOT NULL DEFAULT 0;
""";

    private const string AddStorageTotalBytesColumnSql = """
ALTER TABLE node_capabilities
ADD COLUMN storage_total_bytes INTEGER NOT NULL DEFAULT 0;
""";

    private const string AddVramTotalBytesColumnSql = """
ALTER TABLE node_capabilities
ADD COLUMN vram_total_bytes INTEGER NOT NULL DEFAULT 0;
""";

    private const string AddCpuLimitPercentColumnSql = """
ALTER TABLE node_capabilities
ADD COLUMN cpu_limit_percent INTEGER NOT NULL DEFAULT 0;
""";

    private const string AddRamLimitPercentColumnSql = """
ALTER TABLE node_capabilities
ADD COLUMN ram_limit_percent INTEGER NOT NULL DEFAULT 0;
""";

    private const string AddStorageLimitPercentColumnSql = """
ALTER TABLE node_capabilities
ADD COLUMN storage_limit_percent INTEGER NOT NULL DEFAULT 0;
""";

    private const string AddGpuComputeLimitPercentColumnSql = """
ALTER TABLE node_capabilities
ADD COLUMN gpu_compute_limit_percent INTEGER NOT NULL DEFAULT 0;
""";

    private const string AddGpuVramLimitPercentColumnSql = """
ALTER TABLE node_capabilities
ADD COLUMN gpu_vram_limit_percent INTEGER NOT NULL DEFAULT 0;
""";

    private const string AddProcessCpuLoadPercentColumnSql = """
ALTER TABLE node_capabilities
ADD COLUMN process_cpu_load_percent REAL NOT NULL DEFAULT 0;
""";

    private const string AddProcessRamUsedBytesColumnSql = """
ALTER TABLE node_capabilities
ADD COLUMN process_ram_used_bytes INTEGER NOT NULL DEFAULT 0;
""";

    private const string BrainsTableInfoSql = "PRAGMA table_info(brains);";

    private const string AddBrainsUpdatedMsColumnSql = """
ALTER TABLE brains
ADD COLUMN updated_ms INTEGER NOT NULL DEFAULT 0;
""";

    private static readonly (string ColumnName, string AlterSql)[] NodeCapabilitiesColumnMigrations =
    [
        ("storage_free_bytes", AddStorageFreeBytesColumnSql),
        ("ram_total_bytes", AddRamTotalBytesColumnSql),
        ("storage_total_bytes", AddStorageTotalBytesColumnSql),
        ("vram_total_bytes", AddVramTotalBytesColumnSql),
        ("cpu_limit_percent", AddCpuLimitPercentColumnSql),
        ("ram_limit_percent", AddRamLimitPercentColumnSql),
        ("storage_limit_percent", AddStorageLimitPercentColumnSql),
        ("gpu_compute_limit_percent", AddGpuComputeLimitPercentColumnSql),
        ("gpu_vram_limit_percent", AddGpuVramLimitPercentColumnSql),
        ("process_cpu_load_percent", AddProcessCpuLoadPercentColumnSql),
        ("process_ram_used_bytes", AddProcessRamUsedBytesColumnSql)
    ];

    private static readonly (string ColumnName, string AlterSql)[] BrainsColumnMigrations =
    [
        ("updated_ms", AddBrainsUpdatedMsColumnSql)
    ];

    /// <summary>
    /// Creates the schema and applies additive migrations required by the current runtime.
    /// </summary>
    /// <param name="cancellationToken">Cancels schema initialization.</param>
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var directory = Path.GetDirectoryName(_databasePath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(CreateSchemaSql, cancellationToken: cancellationToken));
        await EnsureNodeCapabilitiesColumnsAsync(connection, cancellationToken);
        await EnsureBrainsColumnsAsync(connection, cancellationToken);
    }

    private static async Task EnsureNodeCapabilitiesColumnsAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        var tableInfo = (await connection.QueryAsync<TableInfoRow>(
                new CommandDefinition(NodeCapabilitiesTableInfoSql, cancellationToken: cancellationToken)))
            .ToArray();
        var knownColumns = tableInfo
            .Select(static column => column.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var (columnName, alterSql) in NodeCapabilitiesColumnMigrations)
        {
            await EnsureNodeCapabilitiesColumnAsync(connection, knownColumns, columnName, alterSql, cancellationToken);
        }
    }

    private static async Task EnsureNodeCapabilitiesColumnAsync(
        SqliteConnection connection,
        ISet<string> knownColumns,
        string columnName,
        string alterSql,
        CancellationToken cancellationToken)
    {
        if (knownColumns.Contains(columnName))
        {
            return;
        }

        await connection.ExecuteAsync(new CommandDefinition(alterSql, cancellationToken: cancellationToken));
        knownColumns.Add(columnName);
    }

    private static async Task EnsureBrainsColumnsAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        var tableInfo = (await connection.QueryAsync<TableInfoRow>(
                new CommandDefinition(BrainsTableInfoSql, cancellationToken: cancellationToken)))
            .ToArray();
        var knownColumns = tableInfo
            .Select(static column => column.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var (columnName, alterSql) in BrainsColumnMigrations)
        {
            await EnsureBrainsColumnAsync(connection, knownColumns, columnName, alterSql, cancellationToken);
        }
    }

    private static async Task EnsureBrainsColumnAsync(
        SqliteConnection connection,
        ISet<string> knownColumns,
        string columnName,
        string alterSql,
        CancellationToken cancellationToken)
    {
        if (knownColumns.Contains(columnName))
        {
            return;
        }

        await connection.ExecuteAsync(new CommandDefinition(alterSql, cancellationToken: cancellationToken));
        knownColumns.Add(columnName);
    }
}
