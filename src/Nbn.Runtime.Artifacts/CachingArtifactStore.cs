namespace Nbn.Runtime.Artifacts;

public sealed class CachingArtifactStore : IArtifactStore
{
    private readonly IArtifactStore _upstream;
    private readonly LocalArtifactCache _cache;
    private readonly Dictionary<string, ArtifactManifest> _manifestCache = new(StringComparer.OrdinalIgnoreCase);

    public CachingArtifactStore(IArtifactStore upstream, ArtifactCacheOptions cacheOptions)
    {
        _upstream = upstream ?? throw new ArgumentNullException(nameof(upstream));
        _cache = new LocalArtifactCache(upstream, cacheOptions ?? throw new ArgumentNullException(nameof(cacheOptions)));
    }

    public async Task<ArtifactManifest> StoreAsync(
        Stream content,
        string mediaType,
        ArtifactStoreWriteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var manifest = await _upstream.StoreAsync(content, mediaType, options, cancellationToken).ConfigureAwait(false);
        CacheManifest(manifest);
        await _cache.EnsureAsync(manifest, cancellationToken).ConfigureAwait(false);
        return manifest;
    }

    public async Task<ArtifactManifest?> TryGetManifestAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        if (_manifestCache.TryGetValue(artifactId.ToHex(), out var cached))
        {
            return cached;
        }

        var manifest = await _upstream.TryGetManifestAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (manifest is not null)
        {
            CacheManifest(manifest);
        }

        return manifest;
    }

    public async Task<bool> ContainsAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        var cachedEntry = await _cache.TryGetAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (cachedEntry is not null)
        {
            return true;
        }

        return await _upstream.ContainsAsync(artifactId, cancellationToken).ConfigureAwait(false);
    }

    public async Task<Stream?> TryOpenArtifactAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        var cachedEntry = await _cache.TryGetAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (cachedEntry is not null)
        {
            return OpenCachedEntry(cachedEntry);
        }

        var manifest = await TryGetManifestAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (manifest is null)
        {
            return null;
        }

        await _cache.EnsureAsync(manifest, cancellationToken).ConfigureAwait(false);
        cachedEntry = await _cache.TryGetAsync(artifactId, cancellationToken).ConfigureAwait(false);
        return cachedEntry is not null
            ? OpenCachedEntry(cachedEntry)
            : await _upstream.TryOpenArtifactAsync(artifactId, cancellationToken).ConfigureAwait(false);
    }

    private static Stream OpenCachedEntry(ArtifactCacheEntry entry)
        => new FileStream(entry.Path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 64 * 1024, useAsync: true);

    private void CacheManifest(ArtifactManifest manifest)
    {
        _manifestCache[manifest.ArtifactId.ToHex()] = manifest;
    }
}
