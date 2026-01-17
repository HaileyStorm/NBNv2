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

public sealed record NodeStatus(
    Guid NodeId,
    string LogicalName,
    string Address,
    string RootActorName,
    long LastSeenMs,
    bool IsAlive);

public sealed record SettingEntry(
    string Key,
    string Value,
    long UpdatedMs);

public sealed record ArtifactCompressionSettings(
    string Kind,
    int Level,
    int MinBytes,
    bool OnlyIfSmaller);
