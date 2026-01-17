namespace Nbn.Runtime.Artifacts;

public sealed record ArtifactCacheEntry(Sha256Hash ArtifactId, string Path, long ByteLength, DateTimeOffset CachedAt);
