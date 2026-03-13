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

    public static async Task<Stream?> TryOpenFallbackAsync(
        IArtifactStore store,
        Sha256Hash artifactId,
        long offset,
        long length,
        CancellationToken cancellationToken)
    {
        ValidateRange(offset, length);

        var stream = await store.TryOpenArtifactAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (stream is null)
        {
            return null;
        }

        return new ArtifactRangeStream(stream, offset, length);
    }
}
