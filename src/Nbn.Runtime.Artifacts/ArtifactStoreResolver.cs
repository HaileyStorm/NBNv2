namespace Nbn.Runtime.Artifacts;

/// <summary>
/// Resolves artifact store URIs into local stores, registered adapters, or built-in HTTP-backed stores.
/// </summary>
public sealed class ArtifactStoreResolver
{
    private readonly ArtifactStoreResolverOptions _options;
    private readonly Dictionary<string, IArtifactStore> _stores = new(StringComparer.Ordinal);

    /// <summary>
    /// Initializes a resolver using the provided local-root and remote-cache settings.
    /// </summary>
    public ArtifactStoreResolver(ArtifactStoreResolverOptions? options = null)
    {
        _options = options ?? new ArtifactStoreResolverOptions();
    }

    /// <summary>
    /// Resolves the store for the provided URI, reusing cached instances for identical normalized targets.
    /// </summary>
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

    /// <summary>
    /// Returns a human-readable description of the resolved store target.
    /// </summary>
    public string Describe(string? storeUri)
    {
        if (TryGetLocalStoreRoot(storeUri, _options.LocalStoreRootPath, out var localRootPath))
        {
            return localRootPath;
        }

        return NormalizeNonFileStoreUri(storeUri);
    }

    /// <summary>
    /// Returns the explicit store URI when present or the default local root path when the caller omitted one.
    /// </summary>
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

    /// <summary>
    /// Returns <see langword="true"/> when the value is a non-file absolute store URI.
    /// </summary>
    public static bool IsNonFileStoreUri(string? storeUri)
        => TryParseNonFileStoreUri(storeUri, out _);

    /// <summary>
    /// Resolves relative paths, blank values, and <c>file://</c> URIs to a local store root path.
    /// </summary>
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
