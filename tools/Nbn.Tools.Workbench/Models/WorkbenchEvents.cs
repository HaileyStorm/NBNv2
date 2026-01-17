using System;

namespace Nbn.Tools.Workbench.Models;

public sealed record OutputEventItem(
    DateTimeOffset Time,
    string BrainId,
    uint OutputIndex,
    float Value,
    ulong TickId);

public sealed record OutputVectorEventItem(
    DateTimeOffset Time,
    string BrainId,
    string ValuesPreview,
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
    float Value);

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
    string Status);

public sealed record SettingItem(string Key, string Value, string Updated);
