namespace Nbn.Runtime.Artifacts;

/// <summary>
/// Configures the node-local cache used to stage full artifacts and cached ranges from remote stores.
/// </summary>
public sealed class ArtifactCacheOptions
{
    /// <summary>
    /// Initializes cache options rooted at the provided directory.
    /// </summary>
    public ArtifactCacheOptions(string rootPath)
    {
        if (string.IsNullOrWhiteSpace(rootPath))
        {
            throw new ArgumentException("Root path is required.", nameof(rootPath));
        }

        RootPath = rootPath;
    }

    /// <summary>
    /// Gets the cache root directory.
    /// </summary>
    public string RootPath { get; }

    /// <summary>
    /// Gets the file-copy buffer size used when materializing cache entries.
    /// </summary>
    public int WriteBufferSize { get; init; } = 256 * 1024;
}
