using System.Text;

namespace Nbn.Runtime.Artifacts;

/// <summary>
/// Describes a stored artifact, including its identity, media type, chunk layout, and optional region index.
/// </summary>
public sealed class ArtifactManifest
{
    /// <summary>
    /// Initializes a manifest for a content-addressed artifact payload.
    /// </summary>
    public ArtifactManifest(
        Sha256Hash artifactId,
        string mediaType,
        long byteLength,
        IReadOnlyList<ArtifactChunkInfo> chunks,
        IReadOnlyList<ArtifactRegionIndexEntry>? regionIndex = null)
    {
        if (string.IsNullOrWhiteSpace(mediaType))
        {
            throw new ArgumentException("Media type is required.", nameof(mediaType));
        }

        if (byteLength < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(byteLength));
        }

        if (chunks is null)
        {
            throw new ArgumentNullException(nameof(chunks));
        }

        ArtifactId = artifactId;
        MediaType = mediaType;
        ByteLength = byteLength;
        Chunks = chunks;
        RegionIndex = regionIndex ?? Array.Empty<ArtifactRegionIndexEntry>();
    }

    /// <summary>
    /// Gets the SHA-256 identity of the artifact bytes.
    /// </summary>
    public Sha256Hash ArtifactId { get; }

    /// <summary>
    /// Gets the artifact media type associated with the stored payload.
    /// </summary>
    public string MediaType { get; }

    /// <summary>
    /// Gets the total artifact length in bytes.
    /// </summary>
    public long ByteLength { get; }

    /// <summary>
    /// Gets the ordered chunk entries that reconstruct the artifact payload.
    /// </summary>
    public IReadOnlyList<ArtifactChunkInfo> Chunks { get; }

    /// <summary>
    /// Gets the optional region index entries used for selective <c>.nbn</c> reads.
    /// </summary>
    public IReadOnlyList<ArtifactRegionIndexEntry> RegionIndex { get; }

    /// <summary>
    /// Computes the canonical manifest hash used for duplicate-store reconciliation.
    /// </summary>
    public Sha256Hash ComputeManifestHash()
    {
        var bytes = ToCanonicalBytes();
        return Sha256Hash.Compute(bytes);
    }

    /// <summary>
    /// Serializes the manifest to the canonical byte representation hashed by <see cref="ComputeManifestHash"/>.
    /// </summary>
    public byte[] ToCanonicalBytes()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);

        writer.Write((byte)1);
        writer.Write(ArtifactId.Bytes.ToArray());
        writer.Write(MediaType);
        writer.Write(ByteLength);
        writer.Write(Chunks.Count);
        foreach (var chunk in Chunks)
        {
            writer.Write(chunk.Hash.Bytes.ToArray());
            writer.Write(chunk.UncompressedLength);
        }

        writer.Write(RegionIndex.Count);
        foreach (var entry in RegionIndex)
        {
            writer.Write(entry.RegionId);
            writer.Write(entry.Offset);
            writer.Write(entry.Length);
        }

        writer.Flush();
        return stream.ToArray();
    }
}
