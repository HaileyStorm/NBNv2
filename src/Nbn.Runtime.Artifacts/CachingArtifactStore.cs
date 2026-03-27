namespace Nbn.Runtime.Artifacts;

/// <summary>
/// Wraps an upstream store with a node-local cache for manifests, full artifacts, and cached ranges.
/// </summary>
public sealed class CachingArtifactStore : IArtifactStore
{
    private readonly IArtifactStore _upstream;
    private readonly LocalArtifactCache _cache;
    private readonly Dictionary<string, ArtifactManifest> _manifestCache = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Initializes a caching wrapper around the provided upstream store.
    /// </summary>
    public CachingArtifactStore(IArtifactStore upstream, ArtifactCacheOptions cacheOptions)
    {
        _upstream = upstream ?? throw new ArgumentNullException(nameof(upstream));
        _cache = new LocalArtifactCache(upstream, cacheOptions ?? throw new ArgumentNullException(nameof(cacheOptions)));
    }

    /// <inheritdoc />
    public async Task<ArtifactManifest> StoreAsync(
        Stream content,
        string mediaType,
        ArtifactStoreWriteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options = ArtifactRegionIndexBuilder.PopulateIfMissing(content, mediaType, options);
        var manifest = await _upstream.StoreAsync(content, mediaType, options, cancellationToken).ConfigureAwait(false);
        CacheManifest(manifest);
        try
        {
            await _cache.EnsureAsync(manifest, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (IsRecoverableCacheFailure(ex))
        {
            // Cache persistence is an optimization; remote-store correctness must not depend on it.
        }

        return manifest;
    }

    /// <inheritdoc />
    public async Task<ArtifactManifest?> TryGetManifestAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        var manifest = await _upstream.TryGetManifestAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (manifest is not null)
        {
            CacheManifest(manifest);
            return manifest;
        }

        return _manifestCache.TryGetValue(artifactId.ToHex(), out var cached)
            ? cached
            : null;
    }

    /// <inheritdoc />
    public async Task<bool> ContainsAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        var cachedEntry = await _cache.TryGetAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (cachedEntry is not null)
        {
            return true;
        }

        return await _upstream.ContainsAsync(artifactId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
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

        try
        {
            await _cache.EnsureAsync(manifest, cancellationToken).ConfigureAwait(false);
            cachedEntry = await _cache.TryGetAsync(artifactId, cancellationToken).ConfigureAwait(false);
            if (cachedEntry is not null)
            {
                return OpenCachedEntry(cachedEntry);
            }
        }
        catch (Exception ex) when (IsRecoverableCacheFailure(ex))
        {
            // Cache persistence is an optimization; remote-store correctness must not depend on it.
        }

        return await _upstream.TryOpenArtifactAsync(artifactId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
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
        try
        {
            var rangeEntry = await _cache.StoreRangeAsync(artifactId, offset, length, upstreamRange, cancellationToken).ConfigureAwait(false);
            return OpenCachedEntry(rangeEntry);
        }
        catch (Exception ex) when (IsRecoverableCacheFailure(ex))
        {
            // Cache persistence is an optimization; remote-store correctness must not depend on it.
            return await _upstream.TryOpenArtifactRangeAsync(artifactId, offset, length, cancellationToken).ConfigureAwait(false);
        }
    }

    private static Stream OpenCachedEntry(ArtifactCacheEntry entry)
        => new FileStream(entry.Path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 64 * 1024, useAsync: true);

    private static bool IsRecoverableCacheFailure(Exception ex)
        => ex is UnauthorizedAccessException
           || ex is IOException
           || ex is DirectoryNotFoundException
           || ex is NotSupportedException
           || ex is PathTooLongException;

    private void CacheManifest(ArtifactManifest manifest)
    {
        _manifestCache[manifest.ArtifactId.ToHex()] = manifest;
    }
}
