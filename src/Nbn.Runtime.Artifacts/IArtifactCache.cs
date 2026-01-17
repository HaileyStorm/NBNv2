namespace Nbn.Runtime.Artifacts;

public interface IArtifactCache
{
    ValueTask<ArtifactCacheEntry?> TryGetAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default);
    ValueTask<ArtifactCacheEntry> EnsureAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default);
    ValueTask<ArtifactCacheEntry> EnsureAsync(ArtifactManifest manifest, CancellationToken cancellationToken = default);
}
