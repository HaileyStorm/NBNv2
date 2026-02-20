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
    public bool IsOnline => string.Equals(Status, "online", StringComparison.OrdinalIgnoreCase);
    public string StatusChipBackground => IsOnline ? "#DDF5E5" : "#F6E8BF";
    public string StatusChipBorder => IsOnline ? "#2D9A5E" : "#C8A13F";
    public string StatusChipForeground => IsOnline ? "#0F5832" : "#5C4511";
}

public sealed record SettingItem(string Key, string Value, string Updated);

public sealed record BrainListItem(Guid BrainId, string State, bool ControllerAlive)
{
    public string Id => BrainId.ToString("D");
    public string Display => string.IsNullOrWhiteSpace(State) ? Id : $"{Id} ({State})";
}
