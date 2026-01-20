using Nbn.Proto;

namespace Nbn.Runtime.IO;

public sealed record ApplyTickCost(Guid BrainId, ulong TickId, long TickCost);

public sealed record UpdateBrainSnapshot(Guid BrainId, ArtifactRef Snapshot);
