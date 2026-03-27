namespace Nbn.Runtime.Artifacts;

/// <summary>
/// Defines the content-addressed artifact store contract used by runtime and tooling callers.
/// </summary>
public interface IArtifactStore
{
    /// <summary>
    /// Stores the readable content stream and returns the resulting manifest.
    /// </summary>
    Task<ArtifactManifest> StoreAsync(Stream content, string mediaType, ArtifactStoreWriteOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resolves the manifest for an existing artifact when it is available.
    /// </summary>
    Task<ArtifactManifest?> TryGetManifestAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns <see langword="true"/> when the store already contains the artifact.
    /// </summary>
    Task<bool> ContainsAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens a readable stream for the complete artifact payload when it exists.
    /// </summary>
    Task<Stream?> TryOpenArtifactAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens a readable stream for the requested artifact byte range when the store supports it.
    /// </summary>
    Task<Stream?> TryOpenArtifactRangeAsync(Sha256Hash artifactId, long offset, long length, CancellationToken cancellationToken = default)
        => ArtifactRangeSupport.TryOpenFallbackAsync(this, artifactId, offset, length, cancellationToken);
}
