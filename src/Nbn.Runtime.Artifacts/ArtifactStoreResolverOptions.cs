namespace Nbn.Runtime.Artifacts;

/// <summary>
/// Configures how artifact store URIs resolve to local roots, remote caches, and built-in adapters.
/// </summary>
public sealed class ArtifactStoreResolverOptions
{
    /// <summary>
    /// Initializes resolver options using explicit overrides or the standard environment-variable fallbacks.
    /// </summary>
    public ArtifactStoreResolverOptions(
        string? localStoreRootPath = null,
        string? cacheRootPath = null,
        bool enableRemoteCaching = true)
    {
        LocalStoreRootPath = ResolveLocalStoreRootPath(localStoreRootPath);
        CacheRootPath = ResolveCacheRootPath(LocalStoreRootPath, cacheRootPath);
        EnableRemoteCaching = enableRemoteCaching;
    }

    /// <summary>
    /// Gets the local artifact root used for blank, relative-path, and <c>file://</c> store URIs.
    /// </summary>
    public string LocalStoreRootPath { get; }

    /// <summary>
    /// Gets the node-local cache root used when remote caching is enabled.
    /// </summary>
    public string CacheRootPath { get; }

    /// <summary>
    /// Gets a value indicating whether resolver-created remote stores should be wrapped in the node-local cache.
    /// </summary>
    public bool EnableRemoteCaching { get; }

    /// <summary>
    /// Resolves the local artifact root from an explicit override, <c>NBN_ARTIFACT_ROOT</c>, or the default <c>artifacts</c> directory.
    /// </summary>
    public static string ResolveLocalStoreRootPath(string? overridePath = null)
    {
        if (!string.IsNullOrWhiteSpace(overridePath))
        {
            return overridePath.Trim();
        }

        var envRoot = Environment.GetEnvironmentVariable("NBN_ARTIFACT_ROOT");
        if (!string.IsNullOrWhiteSpace(envRoot))
        {
            return envRoot.Trim();
        }

        return Path.Combine(Environment.CurrentDirectory, "artifacts");
    }

    /// <summary>
    /// Resolves the cache root from an explicit override, <c>NBN_ARTIFACT_CACHE_ROOT</c>, or <c>&lt;local-root&gt;/.cache</c>.
    /// </summary>
    public static string ResolveCacheRootPath(string localStoreRootPath, string? overridePath = null)
    {
        if (!string.IsNullOrWhiteSpace(overridePath))
        {
            return overridePath.Trim();
        }

        var envRoot = Environment.GetEnvironmentVariable("NBN_ARTIFACT_CACHE_ROOT");
        if (!string.IsNullOrWhiteSpace(envRoot))
        {
            return envRoot.Trim();
        }

        return Path.Combine(localStoreRootPath, ".cache");
    }
}
