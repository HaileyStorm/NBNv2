namespace Nbn.Runtime.Artifacts;

public sealed class ArtifactStoreWriteOptions
{
    public IReadOnlyList<ArtifactRegionIndexEntry>? RegionIndex { get; init; }
}
