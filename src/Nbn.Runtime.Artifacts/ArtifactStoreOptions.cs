namespace Nbn.Runtime.Artifacts;

public sealed class ArtifactStoreOptions
{
    public ArtifactStoreOptions(string rootPath)
    {
        if (string.IsNullOrWhiteSpace(rootPath))
        {
            throw new ArgumentException("Root path is required.", nameof(rootPath));
        }

        RootPath = rootPath;
        DatabasePath = Path.Combine(rootPath, "artifacts.db");
        ChunkRootPath = Path.Combine(rootPath, "chunks");
    }

    public string RootPath { get; }
    public string DatabasePath { get; }
    public string ChunkRootPath { get; }
    public CdcChunkerOptions Chunking { get; init; } = new();
    public ChunkCompressionKind ChunkCompression { get; init; } = ChunkCompressionKind.None;
    public int ChunkCompressionLevel { get; init; } = 3;
    public int ChunkCompressionMinBytes { get; init; } = 64 * 1024;
    public bool ChunkCompressionOnlyIfSmaller { get; init; } = true;
}
