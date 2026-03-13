namespace Nbn.Runtime.Artifacts;

public sealed class ArtifactStoreResolverOptions
{
    public ArtifactStoreResolverOptions(
        string? localStoreRootPath = null,
        string? cacheRootPath = null,
        bool enableRemoteCaching = true)
    {
        LocalStoreRootPath = ResolveLocalStoreRootPath(localStoreRootPath);
        CacheRootPath = ResolveCacheRootPath(LocalStoreRootPath, cacheRootPath);
        EnableRemoteCaching = enableRemoteCaching;
    }

    public string LocalStoreRootPath { get; }
    public string CacheRootPath { get; }
    public bool EnableRemoteCaching { get; }

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
