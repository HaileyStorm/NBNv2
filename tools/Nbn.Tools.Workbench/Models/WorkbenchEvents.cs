using System;

namespace Nbn.Tools.Workbench.Models;

public sealed record OutputEventItem(
    DateTimeOffset Time,
    string TimeLabel,
    string BrainId,
    uint OutputIndex,
    float Value,
    ulong TickId)
{
    public bool IsZero => Math.Abs(Value) <= 1e-6f;
}

public sealed record OutputVectorEventItem(
    DateTimeOffset Time,
    string TimeLabel,
    string BrainId,
    string ValuesPreview,
    bool AllZero,
    ulong TickId);

public sealed record DebugEventItem(
    DateTimeOffset Time,
    string Severity,
    string Context,
    string Summary,
    string Message,
    string SenderActor,
    string SenderNode);

public sealed record VizEventItem(
    DateTimeOffset Time,
    string Type,
    string BrainId,
    ulong TickId,
    string Region,
    string Source,
    string Target,
    float Value,
    float Strength,
    string EventId);

public sealed record BrainTerminatedItem(
    DateTimeOffset Time,
    string BrainId,
    string Reason,
    long EnergyRemaining,
    long LastTickCost);

public sealed record NodeStatusItem(
    string LogicalName,
    string Address,
    string RootActor,
    string LastSeen,
    string Status)
{
    private string StatusKey => (Status ?? string.Empty).Trim().ToLowerInvariant();
    public bool IsOnline => StatusKey is "online" or "active";
    private bool IsDegraded => StatusKey is "degraded" or "warning";
    private bool IsFailed => StatusKey is "failed";
    public string StatusChipBackground => IsOnline
        ? "#DDF5E5"
        : IsDegraded
            ? "#F6E8BF"
            : IsFailed
                ? "#FBE8E8"
                : "#F6E8BF";
    public string StatusChipBorder => IsOnline
        ? "#2D9A5E"
        : IsDegraded
            ? "#C8A13F"
            : IsFailed
                ? "#C04A4A"
                : "#C8A13F";
    public string StatusChipForeground => IsOnline
        ? "#0F5832"
        : IsDegraded
            ? "#5C4511"
            : IsFailed
                ? "#6B1F1F"
                : "#5C4511";
}

public sealed record WorkerEndpointItem(
    Guid NodeId,
    string LogicalName,
    string Address,
    string RootActor,
    string LastSeen,
    string Status)
{
    private string StatusKey => (Status ?? string.Empty).Trim().ToLowerInvariant();
    public bool IsActive => StatusKey is "active" or "online";
    private bool IsDegraded => StatusKey is "degraded" or "warning";
    private bool IsFailed => StatusKey is "failed";
    public string StatusChipBackground => IsActive
        ? "#DDF5E5"
        : IsDegraded
            ? "#F6E8BF"
            : "#FBE8E8";
    public string StatusChipBorder => IsActive
        ? "#2D9A5E"
        : IsDegraded
            ? "#C8A13F"
            : "#C04A4A";
    public string StatusChipForeground => IsActive
        ? "#0F5832"
        : IsDegraded
            ? "#5C4511"
            : "#6B1F1F";
}

public sealed record SettingItem(string Key, string Value, string Updated);

public sealed record BrainListItem(Guid BrainId, string State, bool ControllerAlive)
{
    public string Id => BrainId.ToString("D");
    public string Display => string.IsNullOrWhiteSpace(State) ? Id : $"{Id} ({State})";
}
