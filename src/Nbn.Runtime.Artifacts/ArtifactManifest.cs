using System.Text;

namespace Nbn.Runtime.Artifacts;

public sealed class ArtifactManifest
{
    public ArtifactManifest(
        Sha256Hash artifactId,
        string mediaType,
        long byteLength,
        IReadOnlyList<ArtifactChunkRef> chunks,
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

    public Sha256Hash ArtifactId { get; }
    public string MediaType { get; }
    public long ByteLength { get; }
    public IReadOnlyList<ArtifactChunkRef> Chunks { get; }
    public IReadOnlyList<ArtifactRegionIndexEntry> RegionIndex { get; }

    public Sha256Hash ComputeManifestHash()
    {
        var bytes = ToCanonicalBytes();
        return Sha256Hash.Compute(bytes);
    }

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
