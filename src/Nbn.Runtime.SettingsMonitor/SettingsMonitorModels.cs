namespace Nbn.Runtime.SettingsMonitor;

/// <summary>
/// Identifies a worker node tracked by SettingsMonitor.
/// </summary>
public sealed record NodeRegistration(
    Guid NodeId,
    string LogicalName,
    string Address,
    string RootActorName);

/// <summary>
/// Captures the latest reported worker capability sample used for placement and reporting.
/// </summary>
public sealed record NodeCapabilities(
    uint CpuCores,
    long RamFreeBytes,
    long StorageFreeBytes,
    bool HasGpu,
    string? GpuName,
    long VramFreeBytes,
    float CpuScore,
    float GpuScore,
    bool IlgpuCudaAvailable,
    bool IlgpuOpenclAvailable,
    long RamTotalBytes,
    long StorageTotalBytes,
    long VramTotalBytes,
    uint CpuLimitPercent,
    uint RamLimitPercent,
    uint StorageLimitPercent,
    uint GpuComputeLimitPercent,
    uint GpuVramLimitPercent,
    float ProcessCpuLoadPercent,
    long ProcessRamUsedBytes);

/// <summary>
/// Represents one observed worker heartbeat and its capability payload.
/// </summary>
public sealed record NodeHeartbeat(
    Guid NodeId,
    long TimeMs,
    NodeCapabilities Capabilities);

/// <summary>
/// Associates a brain with its controller actor location.
/// </summary>
public sealed record BrainControllerRegistration(
    Guid BrainId,
    Guid NodeId,
    string ActorName);

/// <summary>
/// Records a controller heartbeat for a brain at a specific observation time.
/// </summary>
public sealed record BrainControllerHeartbeat(
    Guid BrainId,
    long TimeMs);

/// <summary>
/// Represents the latest merged liveness snapshot for a worker node.
/// </summary>
public sealed class NodeStatus
{
    public Guid NodeId { get; set; }
    public string LogicalName { get; set; } = string.Empty;
    public string Address { get; set; } = string.Empty;
    public string RootActorName { get; set; } = string.Empty;
    public long LastSeenMs { get; set; }
    public bool IsAlive { get; set; }
}

/// <summary>
/// Projects node liveness and capability data into the worker inventory surface used by placement.
/// </summary>
public sealed class WorkerReadinessCapability
{
    public Guid NodeId { get; set; }
    public string LogicalName { get; set; } = string.Empty;
    public string Address { get; set; } = string.Empty;
    public string RootActorName { get; set; } = string.Empty;
    public bool IsAlive { get; set; }
    public bool IsReady { get; set; }
    public long LastSeenMs { get; set; }
    public bool HasCapabilities { get; set; }
    public long CapabilityTimeMs { get; set; }
    public uint CpuCores { get; set; }
    public long RamFreeBytes { get; set; }
    public long StorageFreeBytes { get; set; }
    public bool HasGpu { get; set; }
    public string GpuName { get; set; } = string.Empty;
    public long VramFreeBytes { get; set; }
    public float CpuScore { get; set; }
    public float GpuScore { get; set; }
    public bool IlgpuCudaAvailable { get; set; }
    public bool IlgpuOpenclAvailable { get; set; }
    public long RamTotalBytes { get; set; }
    public long StorageTotalBytes { get; set; }
    public long VramTotalBytes { get; set; }
    public uint CpuLimitPercent { get; set; }
    public uint RamLimitPercent { get; set; }
    public uint StorageLimitPercent { get; set; }
    public uint GpuComputeLimitPercent { get; set; }
    public uint GpuVramLimitPercent { get; set; }
    public float ProcessCpuLoadPercent { get; set; }
    public long ProcessRamUsedBytes { get; set; }
}

/// <summary>
/// Wraps a point-in-time worker inventory query result.
/// </summary>
public sealed record WorkerInventorySnapshot(
    long SnapshotMs,
    IReadOnlyList<WorkerReadinessCapability> Workers);

/// <summary>
/// Represents the latest merged controller status for a brain.
/// </summary>
public sealed class BrainControllerStatus
{
    public Guid BrainId { get; set; }
    public Guid NodeId { get; set; }
    public string ActorName { get; set; } = string.Empty;
    public long LastSeenMs { get; set; }
    public bool IsAlive { get; set; }
}

/// <summary>
/// Represents the persisted state summary for a brain known to SettingsMonitor.
/// </summary>
public sealed class BrainStatus
{
    public Guid BrainId { get; set; }
    public byte[]? BaseNbnSha256 { get; set; }
    public byte[]? LastSnapshotSha256 { get; set; }
    public long SpawnedMs { get; set; }
    public long LastTickId { get; set; }
    public string State { get; set; } = string.Empty;
    public string? Notes { get; set; }
}

/// <summary>
/// Represents one persisted operator setting row.
/// </summary>
public sealed record SettingEntry(
    string Key,
    string Value,
    long UpdatedMs);

/// <summary>
/// Describes the resolved artifact compression policy derived from persisted settings.
/// </summary>
public sealed record ArtifactCompressionSettings(
    string Kind,
    int Level,
    int MinBytes,
    bool OnlyIfSmaller);
