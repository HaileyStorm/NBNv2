using Nbn.Proto;

namespace Nbn.Runtime.IO;

/// <summary>
/// Applies a tick energy cost to a registered brain after a completed runtime tick.
/// </summary>
public sealed record ApplyTickCost(Guid BrainId, ulong TickId, long TickCost);

/// <summary>
/// Refreshes the last known snapshot reference for a registered brain.
/// </summary>
public sealed record UpdateBrainSnapshot(Guid BrainId, ArtifactRef Snapshot);
