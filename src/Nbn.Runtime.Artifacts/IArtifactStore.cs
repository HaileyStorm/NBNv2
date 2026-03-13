namespace Nbn.Runtime.Artifacts;

public interface IArtifactStore
{
    Task<ArtifactManifest> StoreAsync(Stream content, string mediaType, ArtifactStoreWriteOptions? options = null, CancellationToken cancellationToken = default);
    Task<ArtifactManifest?> TryGetManifestAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default);
    Task<bool> ContainsAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default);
    Task<Stream?> TryOpenArtifactAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default);
    Task<Stream?> TryOpenArtifactRangeAsync(Sha256Hash artifactId, long offset, long length, CancellationToken cancellationToken = default)
        => ArtifactRangeSupport.TryOpenFallbackAsync(this, artifactId, offset, length, cancellationToken);
}
