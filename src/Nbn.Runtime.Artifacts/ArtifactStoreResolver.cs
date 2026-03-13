namespace Nbn.Runtime.Artifacts;

public sealed class ArtifactStoreResolver
{
    private readonly ArtifactStoreResolverOptions _options;
    private readonly Dictionary<string, IArtifactStore> _stores = new(StringComparer.Ordinal);

    public ArtifactStoreResolver(ArtifactStoreResolverOptions? options = null)
    {
        _options = options ?? new ArtifactStoreResolverOptions();
    }

    public IArtifactStore Resolve(string? storeUri)
    {
        var cacheKey = BuildCacheKey(storeUri);
        if (_stores.TryGetValue(cacheKey, out var cached))
        {
            return cached;
        }

        var store = CreateStore(storeUri);
        _stores[cacheKey] = store;
        return store;
    }

    public string Describe(string? storeUri)
    {
        if (TryGetLocalStoreRoot(storeUri, _options.LocalStoreRootPath, out var localRootPath))
        {
            return localRootPath;
        }

        return NormalizeNonFileStoreUri(storeUri);
    }

    public string ResolveStoreUriOrDefault(string? storeUri)
    {
        if (!string.IsNullOrWhiteSpace(storeUri))
        {
            var trimmed = storeUri.Trim();
            if (TryGetLocalStoreRoot(trimmed, _options.LocalStoreRootPath, out _))
            {
                return trimmed;
            }

            return NormalizeNonFileStoreUri(trimmed);
        }

        return _options.LocalStoreRootPath;
    }

    public static bool IsNonFileStoreUri(string? storeUri)
        => TryParseNonFileStoreUri(storeUri, out _);

    public static bool TryGetLocalStoreRoot(string? storeUri, string defaultLocalStoreRootPath, out string localStoreRootPath)
    {
        if (string.IsNullOrWhiteSpace(defaultLocalStoreRootPath))
        {
            throw new ArgumentException("Default local store root path is required.", nameof(defaultLocalStoreRootPath));
        }

        if (string.IsNullOrWhiteSpace(storeUri))
        {
            localStoreRootPath = defaultLocalStoreRootPath;
            return true;
        }

        var trimmed = storeUri.Trim();
        if (!trimmed.Contains("://", StringComparison.Ordinal))
        {
            localStoreRootPath = trimmed;
            return true;
        }

        if (Uri.TryCreate(trimmed, UriKind.Absolute, out var uri) && uri.IsFile)
        {
            localStoreRootPath = uri.LocalPath;
            return true;
        }

        localStoreRootPath = string.Empty;
        return false;
    }

    private IArtifactStore CreateStore(string? storeUri)
    {
        if (TryGetLocalStoreRoot(storeUri, _options.LocalStoreRootPath, out var localStoreRootPath))
        {
            return new LocalArtifactStore(new ArtifactStoreOptions(localStoreRootPath));
        }

        var normalizedStoreUri = NormalizeNonFileStoreUri(storeUri);
        if (!ArtifactStoreRegistry.TryResolve(normalizedStoreUri, out var upstream, out var enableNodeLocalCache))
        {
            throw new InvalidOperationException(
                $"No artifact store adapter is registered for non-file store URI '{normalizedStoreUri}'.");
        }

        if (!_options.EnableRemoteCaching || !enableNodeLocalCache)
        {
            return upstream;
        }

        return new CachingArtifactStore(
            upstream,
            new ArtifactCacheOptions(_options.CacheRootPath));
    }

    private string BuildCacheKey(string? storeUri)
    {
        if (TryGetLocalStoreRoot(storeUri, _options.LocalStoreRootPath, out var localStoreRootPath))
        {
            return localStoreRootPath;
        }

        return NormalizeNonFileStoreUri(storeUri);
    }

    private static string NormalizeNonFileStoreUri(string? storeUri)
    {
        if (!TryParseNonFileStoreUri(storeUri, out var uri))
        {
            throw new InvalidOperationException(
                string.IsNullOrWhiteSpace(storeUri)
                    ? "Artifact store URI is required."
                    : $"Artifact store URI '{storeUri}' is not a valid non-file URI.");
        }

        return ArtifactStoreRegistry.NormalizeStoreUri(uri.AbsoluteUri);
    }

    private static bool TryParseNonFileStoreUri(string? storeUri, out Uri uri)
    {
        uri = default!;
        if (string.IsNullOrWhiteSpace(storeUri))
        {
            return false;
        }

        var trimmed = storeUri.Trim();
        if (!trimmed.Contains("://", StringComparison.Ordinal))
        {
            return false;
        }

        if (!Uri.TryCreate(trimmed, UriKind.Absolute, out var parsedUri) || parsedUri.IsFile)
        {
            return false;
        }

        uri = parsedUri;
        return true;
    }
}
