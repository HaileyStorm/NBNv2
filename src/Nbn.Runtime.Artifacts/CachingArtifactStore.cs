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
        options = ArtifactRegionIndexBuilder.PopulateIfMissing(content, mediaType, options);
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

    public async Task<Stream?> TryOpenArtifactRangeAsync(Sha256Hash artifactId, long offset, long length, CancellationToken cancellationToken = default)
    {
        ArtifactRangeSupport.ValidateRange(offset, length);

        var cachedEntry = await _cache.TryGetAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (cachedEntry is not null)
        {
            if (offset > cachedEntry.ByteLength || offset + length > cachedEntry.ByteLength)
            {
                throw new ArgumentOutOfRangeException(nameof(offset), offset, "Requested range exceeds artifact length.");
            }

            return new ArtifactRangeStream(OpenCachedEntry(cachedEntry), offset, length);
        }

        var cachedRange = await _cache.TryGetRangeAsync(artifactId, offset, length, cancellationToken).ConfigureAwait(false);
        if (cachedRange is not null)
        {
            return OpenCachedEntry(cachedRange);
        }

        var manifest = await TryGetManifestAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (manifest is null)
        {
            return null;
        }

        if (offset > manifest.ByteLength || offset + length > manifest.ByteLength)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "Requested range exceeds artifact length.");
        }

        var upstreamRange = await _upstream.TryOpenArtifactRangeAsync(artifactId, offset, length, cancellationToken).ConfigureAwait(false);
        if (upstreamRange is null)
        {
            return null;
        }

        await using var _ = upstreamRange;
        var rangeEntry = await _cache.StoreRangeAsync(artifactId, offset, length, upstreamRange, cancellationToken).ConfigureAwait(false);
        return OpenCachedEntry(rangeEntry);
    }

    private static Stream OpenCachedEntry(ArtifactCacheEntry entry)
        => new FileStream(entry.Path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 64 * 1024, useAsync: true);

    private void CacheManifest(ArtifactManifest manifest)
    {
        _manifestCache[manifest.ArtifactId.ToHex()] = manifest;
    }
}
