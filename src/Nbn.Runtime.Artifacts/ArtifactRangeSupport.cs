namespace Nbn.Runtime.Artifacts;

internal static class ArtifactRangeSupport
{
    public static void ValidateRange(long offset, long length)
    {
        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "Offset must be non-negative.");
        }

        if (length < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(length), length, "Length must be non-negative.");
        }
    }

    public static void ValidateRangeWithin(long totalLength, long offset, long length)
    {
        if (totalLength < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(totalLength), totalLength, "Total length must be non-negative.");
        }

        ValidateRange(offset, length);
        if (offset > totalLength || length > totalLength - offset)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "Requested range exceeds artifact length.");
        }
    }

    public static async Task<Stream?> TryOpenFallbackAsync(
        IArtifactStore store,
        Sha256Hash artifactId,
        long offset,
        long length,
        CancellationToken cancellationToken)
    {
        ValidateRange(offset, length);

        var manifest = await store.TryGetManifestAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (manifest is null)
        {
            return null;
        }

        ValidateRangeWithin(manifest.ByteLength, offset, length);
        if (length == 0)
        {
            return new MemoryStream(Array.Empty<byte>(), writable: false);
        }

        var stream = await store.TryOpenArtifactAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (stream is null)
        {
            return null;
        }

        return new ArtifactRangeStream(stream, offset, length);
    }
}
