namespace Nbn.Runtime.Artifacts;

public sealed record ArtifactChunkInfo(
    Sha256Hash Hash,
    int UncompressedLength,
    int StoredLength,
    ChunkCompressionKind Compression);
