namespace Nbn.Runtime.Artifacts;

/// <summary>
/// Configures the on-disk layout and chunking/compression behavior for a local artifact store.
/// </summary>
public sealed class ArtifactStoreOptions
{
    /// <summary>
    /// Initializes local artifact store options rooted at the provided directory.
    /// </summary>
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

    /// <summary>
    /// Gets the root directory that owns the database and chunk tree.
    /// </summary>
    public string RootPath { get; }

    /// <summary>
    /// Gets the SQLite catalog path for artifact and chunk metadata.
    /// </summary>
    public string DatabasePath { get; }

    /// <summary>
    /// Gets the directory that stores chunk payload files.
    /// </summary>
    public string ChunkRootPath { get; }

    /// <summary>
    /// Gets the rolling-chunker options used when splitting artifacts into chunks.
    /// </summary>
    public CdcChunkerOptions Chunking { get; init; } = new();

    /// <summary>
    /// Gets the chunk compression mode applied before chunk writes.
    /// </summary>
    public ChunkCompressionKind ChunkCompression { get; init; } = ChunkCompressionKind.None;

    /// <summary>
    /// Gets the codec-specific compression level used when chunk compression is enabled.
    /// </summary>
    public int ChunkCompressionLevel { get; init; } = 3;

    /// <summary>
    /// Gets the minimum chunk size, in bytes, before compression is considered.
    /// </summary>
    public int ChunkCompressionMinBytes { get; init; } = 64 * 1024;

    /// <summary>
    /// Gets a value indicating whether compressed chunks should only be stored when they shrink the payload.
    /// </summary>
    public bool ChunkCompressionOnlyIfSmaller { get; init; } = true;
}
