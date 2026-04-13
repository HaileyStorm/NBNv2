using Dapper;
using Microsoft.Data.Sqlite;

namespace Nbn.Runtime.SettingsMonitor;

public sealed partial class SettingsMonitorStore
{
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
    storage_free_bytes,
    has_gpu,
    gpu_name,
    vram_free_bytes,
    cpu_score,
    gpu_score,
    ilgpu_cuda_available,
    ilgpu_opencl_available,
    ram_total_bytes,
    storage_total_bytes,
    vram_total_bytes,
    cpu_limit_percent,
    ram_limit_percent,
    storage_limit_percent,
    gpu_compute_limit_percent,
    gpu_vram_limit_percent,
    process_cpu_load_percent,
    process_ram_used_bytes
) VALUES (
    @node_id,
    @time_ms,
    @cpu_cores,
    @ram_free_bytes,
    @storage_free_bytes,
    @has_gpu,
    @gpu_name,
    @vram_free_bytes,
    @cpu_score,
    @gpu_score,
    @ilgpu_cuda_available,
    @ilgpu_opencl_available,
    @ram_total_bytes,
    @storage_total_bytes,
    @vram_total_bytes,
    @cpu_limit_percent,
    @ram_limit_percent,
    @storage_limit_percent,
    @gpu_compute_limit_percent,
    @gpu_vram_limit_percent,
    @process_cpu_load_percent,
    @process_ram_used_bytes
) ON CONFLICT(node_id, time_ms) DO UPDATE SET
    cpu_cores = excluded.cpu_cores,
    ram_free_bytes = excluded.ram_free_bytes,
    storage_free_bytes = excluded.storage_free_bytes,
    has_gpu = excluded.has_gpu,
    gpu_name = excluded.gpu_name,
    vram_free_bytes = excluded.vram_free_bytes,
    cpu_score = excluded.cpu_score,
    gpu_score = excluded.gpu_score,
    ilgpu_cuda_available = excluded.ilgpu_cuda_available,
    ilgpu_opencl_available = excluded.ilgpu_opencl_available,
    ram_total_bytes = excluded.ram_total_bytes,
    storage_total_bytes = excluded.storage_total_bytes,
    vram_total_bytes = excluded.vram_total_bytes,
    cpu_limit_percent = excluded.cpu_limit_percent,
    ram_limit_percent = excluded.ram_limit_percent,
    storage_limit_percent = excluded.storage_limit_percent,
    gpu_compute_limit_percent = excluded.gpu_compute_limit_percent,
    gpu_vram_limit_percent = excluded.gpu_vram_limit_percent,
    process_cpu_load_percent = excluded.process_cpu_load_percent,
    process_ram_used_bytes = excluded.process_ram_used_bytes;
""";

    private const string MarkOfflineSql = """
UPDATE nodes
SET is_alive = 0,
    last_seen_ms = @last_seen_ms
WHERE node_id = @node_id;
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
    COALESCE(c.storage_free_bytes, 0) AS StorageFreeBytes,
    COALESCE(c.has_gpu, 0) AS HasGpu,
    COALESCE(c.gpu_name, '') AS GpuName,
    COALESCE(c.vram_free_bytes, 0) AS VramFreeBytes,
    COALESCE(c.cpu_score, 0.0) AS CpuScore,
    COALESCE(c.gpu_score, 0.0) AS GpuScore,
    COALESCE(c.ilgpu_cuda_available, 0) AS IlgpuCudaAvailable,
    COALESCE(c.ilgpu_opencl_available, 0) AS IlgpuOpenclAvailable,
    COALESCE(c.ram_total_bytes, 0) AS RamTotalBytes,
    COALESCE(c.storage_total_bytes, 0) AS StorageTotalBytes,
    COALESCE(c.vram_total_bytes, 0) AS VramTotalBytes,
    COALESCE(c.cpu_limit_percent, 0) AS CpuLimitPercent,
    COALESCE(c.ram_limit_percent, 0) AS RamLimitPercent,
    COALESCE(c.storage_limit_percent, 0) AS StorageLimitPercent,
    COALESCE(c.gpu_compute_limit_percent, 0) AS GpuComputeLimitPercent,
    COALESCE(c.gpu_vram_limit_percent, 0) AS GpuVramLimitPercent,
    COALESCE(c.process_cpu_load_percent, 0.0) AS ProcessCpuLoadPercent,
    COALESCE(c.process_ram_used_bytes, 0) AS ProcessRamUsedBytes,
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

    private const string DeleteStaleNodeCapabilitiesSql = """
DELETE FROM node_capabilities
WHERE time_ms < @cutoff_ms
  AND EXISTS (
      SELECT 1
      FROM node_capabilities AS newer
      WHERE newer.node_id = node_capabilities.node_id
        AND newer.time_ms > node_capabilities.time_ms
  );
""";

    /// <summary>
    /// Inserts or refreshes a node registration row.
    /// </summary>
    /// <param name="registration">Node identity and remoting metadata.</param>
    /// <param name="timeMs">Optional persisted observation time in milliseconds.</param>
    /// <param name="cancellationToken">Cancels the database write.</param>
    public async Task UpsertNodeAsync(
        NodeRegistration registration,
        long? timeMs = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(registration.NodeId, nameof(registration), "NodeId");
        ThrowIfNullOrWhiteSpace(registration.LogicalName, nameof(registration), "LogicalName");
        ThrowIfNullOrWhiteSpace(registration.Address, nameof(registration), "Address");
        ThrowIfNullOrWhiteSpace(registration.RootActorName, nameof(registration), "RootActorName");

        var nowMs = timeMs ?? NowMs();
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(
            new CommandDefinition(
                UpsertNodeSql,
                new
                {
                    node_id = ToDatabaseId(registration.NodeId),
                    logical_name = registration.LogicalName,
                    address = registration.Address,
                    root_actor_name = registration.RootActorName,
                    last_seen_ms = nowMs,
                    is_alive = 1
                },
                cancellationToken: cancellationToken));
    }

    /// <summary>
    /// Marks a node as offline without deleting its historical capability rows.
    /// </summary>
    /// <param name="nodeId">Node identifier to mark offline.</param>
    /// <param name="timeMs">Optional offline observation time in milliseconds.</param>
    /// <param name="cancellationToken">Cancels the database write.</param>
    public async Task MarkNodeOfflineAsync(
        Guid nodeId,
        long? timeMs = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(nodeId, nameof(nodeId), "NodeId");

        var nowMs = timeMs ?? NowMs();
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(
            new CommandDefinition(
                MarkOfflineSql,
                new
                {
                    node_id = ToDatabaseId(nodeId),
                    last_seen_ms = nowMs
                },
                cancellationToken: cancellationToken));
    }

    /// <summary>
    /// Records a node heartbeat and its latest capability sample when the node is already registered.
    /// </summary>
    /// <param name="heartbeat">Observed node heartbeat payload.</param>
    /// <param name="cancellationToken">Cancels the database write.</param>
    /// <returns><see langword="true"/> when the node exists and the heartbeat was persisted; otherwise <see langword="false"/>.</returns>
    public async Task<bool> RecordHeartbeatAsync(
        NodeHeartbeat heartbeat,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(heartbeat.NodeId, nameof(heartbeat), "NodeId");

        var capabilities = heartbeat.Capabilities ?? throw new ArgumentException("Capabilities are required.", nameof(heartbeat));
        var observedMs = heartbeat.TimeMs > 0 ? heartbeat.TimeMs : NowMs();

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var updateCount = await connection.ExecuteAsync(
            new CommandDefinition(
                UpdateHeartbeatSql,
                new
                {
                    node_id = ToDatabaseId(heartbeat.NodeId),
                    last_seen_ms = observedMs
                },
                transaction,
                cancellationToken: cancellationToken));

        if (updateCount == 0)
        {
            await transaction.RollbackAsync(cancellationToken);
            return false;
        }

        await connection.ExecuteAsync(
            new CommandDefinition(
                InsertCapabilitiesSql,
                new
                {
                    node_id = ToDatabaseId(heartbeat.NodeId),
                    time_ms = observedMs,
                    cpu_cores = capabilities.CpuCores,
                    ram_free_bytes = capabilities.RamFreeBytes,
                    storage_free_bytes = capabilities.StorageFreeBytes,
                    has_gpu = capabilities.HasGpu ? 1 : 0,
                    gpu_name = capabilities.GpuName ?? string.Empty,
                    vram_free_bytes = capabilities.VramFreeBytes,
                    cpu_score = capabilities.CpuScore,
                    gpu_score = capabilities.GpuScore,
                    ilgpu_cuda_available = capabilities.IlgpuCudaAvailable ? 1 : 0,
                    ilgpu_opencl_available = capabilities.IlgpuOpenclAvailable ? 1 : 0,
                    ram_total_bytes = capabilities.RamTotalBytes,
                    storage_total_bytes = capabilities.StorageTotalBytes,
                    vram_total_bytes = capabilities.VramTotalBytes,
                    cpu_limit_percent = capabilities.CpuLimitPercent,
                    ram_limit_percent = capabilities.RamLimitPercent,
                    storage_limit_percent = capabilities.StorageLimitPercent,
                    gpu_compute_limit_percent = capabilities.GpuComputeLimitPercent,
                    gpu_vram_limit_percent = capabilities.GpuVramLimitPercent,
                    process_cpu_load_percent = capabilities.ProcessCpuLoadPercent,
                    process_ram_used_bytes = capabilities.ProcessRamUsedBytes
                },
                transaction,
                cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    /// <summary>
    /// Looks up a single node registration row.
    /// </summary>
    /// <param name="nodeId">Node identifier to fetch.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    public async Task<NodeStatus?> GetNodeAsync(
        Guid nodeId,
        CancellationToken cancellationToken = default)
    {
        ThrowIfEmptyGuid(nodeId, nameof(nodeId), "NodeId");

        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<NodeStatus>(
            new CommandDefinition(
                GetNodeSql,
                new { node_id = ToDatabaseId(nodeId) },
                cancellationToken: cancellationToken));
    }

    /// <summary>
    /// Lists all known nodes ordered for stable operator display.
    /// </summary>
    /// <param name="cancellationToken">Cancels the query.</param>
    public async Task<IReadOnlyList<NodeStatus>> ListNodesAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<NodeStatus>(
            new CommandDefinition(ListNodesSql, cancellationToken: cancellationToken));
        return rows.AsList();
    }

    /// <summary>
    /// Returns the latest persisted capability sample for each known worker node.
    /// </summary>
    /// <param name="cancellationToken">Cancels the query.</param>
    public async Task<WorkerInventorySnapshot> GetWorkerInventorySnapshotAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<WorkerReadinessCapability>(
            new CommandDefinition(ListWorkerInventorySnapshotSql, cancellationToken: cancellationToken));
        return new WorkerInventorySnapshot(NowMs(), rows.AsList());
    }

    /// <summary>
    /// Deletes historical capability rows older than the supplied cutoff while retaining each node's latest sample.
    /// </summary>
    /// <param name="cutoffMs">Inclusive retention cutoff in milliseconds.</param>
    /// <param name="cancellationToken">Cancels the prune operation.</param>
    public async Task<int> PruneStaleNodeCapabilitiesAsync(
        long cutoffMs,
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await connection.ExecuteAsync(
            new CommandDefinition(
                DeleteStaleNodeCapabilitiesSql,
                new { cutoff_ms = cutoffMs },
                cancellationToken: cancellationToken));
    }
}
