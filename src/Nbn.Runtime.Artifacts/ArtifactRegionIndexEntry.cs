namespace Nbn.Runtime.Artifacts;

/// <summary>
/// Identifies the byte range for a single region section within a seekable <c>.nbn</c> artifact.
/// </summary>
/// <param name="RegionId">The canonical region identifier for the indexed section.</param>
/// <param name="Offset">The zero-based byte offset of the section within the artifact.</param>
/// <param name="Length">The byte length of the indexed section.</param>
public sealed record ArtifactRegionIndexEntry(int RegionId, long Offset, long Length);
