namespace Nbn.Runtime.SettingsMonitor;

public sealed record NodeRegistration(
    Guid NodeId,
    string LogicalName,
    string Address,
    string RootActorName);

public sealed record NodeCapabilities(
    uint CpuCores,
    long RamFreeBytes,
    bool HasGpu,
    string? GpuName,
    long VramFreeBytes,
    float CpuScore,
    float GpuScore,
    bool IlgpuCudaAvailable,
    bool IlgpuOpenclAvailable);

public sealed record NodeHeartbeat(
    Guid NodeId,
    long TimeMs,
    NodeCapabilities Capabilities);

public sealed record BrainControllerRegistration(
    Guid BrainId,
    Guid NodeId,
    string ActorName);

public sealed record BrainControllerHeartbeat(
    Guid BrainId,
    long TimeMs);

public sealed class NodeStatus
{
    public Guid NodeId { get; set; }
    public string LogicalName { get; set; } = string.Empty;
    public string Address { get; set; } = string.Empty;
    public string RootActorName { get; set; } = string.Empty;
    public long LastSeenMs { get; set; }
    public bool IsAlive { get; set; }
}

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
    public bool HasGpu { get; set; }
    public string GpuName { get; set; } = string.Empty;
    public long VramFreeBytes { get; set; }
    public float CpuScore { get; set; }
    public float GpuScore { get; set; }
    public bool IlgpuCudaAvailable { get; set; }
    public bool IlgpuOpenclAvailable { get; set; }
}

public sealed record WorkerInventorySnapshot(
    long SnapshotMs,
    IReadOnlyList<WorkerReadinessCapability> Workers);

public sealed class BrainControllerStatus
{
    public Guid BrainId { get; set; }
    public Guid NodeId { get; set; }
    public string ActorName { get; set; } = string.Empty;
    public long LastSeenMs { get; set; }
    public bool IsAlive { get; set; }
}

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

public sealed record SettingEntry(
    string Key,
    string Value,
    long UpdatedMs);

public sealed record ArtifactCompressionSettings(
    string Kind,
    int Level,
    int MinBytes,
    bool OnlyIfSmaller);
