namespace Nbn.Runtime.Artifacts;

public sealed class ArtifactCacheOptions
{
    public ArtifactCacheOptions(string rootPath)
    {
        if (string.IsNullOrWhiteSpace(rootPath))
        {
            throw new ArgumentException("Root path is required.", nameof(rootPath));
        }

        RootPath = rootPath;
    }

    public string RootPath { get; }
    public int WriteBufferSize { get; init; } = 256 * 1024;
}
