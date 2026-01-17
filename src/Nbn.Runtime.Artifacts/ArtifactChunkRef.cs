namespace Nbn.Runtime.Artifacts;

public sealed record ArtifactChunkRef(Sha256Hash Hash, int UncompressedLength);
