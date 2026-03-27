namespace Nbn.Runtime.Artifacts;

/// <summary>
/// Carries optional write-time metadata that can be persisted alongside a stored artifact.
/// </summary>
public sealed class ArtifactStoreWriteOptions
{
    /// <summary>
    /// Gets the optional region index metadata persisted for seekable <c>.nbn</c> artifacts.
    /// </summary>
    public IReadOnlyList<ArtifactRegionIndexEntry>? RegionIndex { get; init; }
}
