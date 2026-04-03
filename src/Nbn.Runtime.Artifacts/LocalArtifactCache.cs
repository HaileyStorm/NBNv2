using System.Collections.Concurrent;

namespace Nbn.Runtime.Artifacts;

public sealed class LocalArtifactCache : IArtifactCache
{
    private readonly IArtifactStore _store;
    private readonly ArtifactCacheOptions _options;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _writeGates = new(StringComparer.OrdinalIgnoreCase);

    public LocalArtifactCache(IArtifactStore store, ArtifactCacheOptions options)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public ValueTask<ArtifactCacheEntry?> TryGetAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        var path = GetArtifactPath(artifactId);
        if (!File.Exists(path))
        {
            return ValueTask.FromResult<ArtifactCacheEntry?>(null);
        }

        var info = new FileInfo(path);
        return ValueTask.FromResult<ArtifactCacheEntry?>(new ArtifactCacheEntry(artifactId, path, info.Length, new DateTimeOffset(info.LastWriteTimeUtc)));
    }

    public async ValueTask<ArtifactCacheEntry> EnsureAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        var existing = await TryGetAsync(artifactId, cancellationToken);
        if (existing is not null)
        {
            return existing;
        }

        var manifest = await _store.TryGetManifestAsync(artifactId, cancellationToken);
        if (manifest is null)
        {
            throw new InvalidOperationException($"Artifact {artifactId} not found in store.");
        }

        return await EnsureAsync(manifest, cancellationToken);
    }

    public async ValueTask<ArtifactCacheEntry> EnsureAsync(ArtifactManifest manifest, CancellationToken cancellationToken = default)
    {
        if (manifest is null)
        {
            throw new ArgumentNullException(nameof(manifest));
        }

        var path = GetArtifactPath(manifest.ArtifactId);
        if (File.Exists(path))
        {
            var info = new FileInfo(path);
            return new ArtifactCacheEntry(manifest.ArtifactId, path, info.Length, new DateTimeOffset(info.LastWriteTimeUtc));
        }

        var gate = GetWriteGate(path);
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (File.Exists(path))
            {
                var info = new FileInfo(path);
                return new ArtifactCacheEntry(manifest.ArtifactId, path, info.Length, new DateTimeOffset(info.LastWriteTimeUtc));
            }

            Directory.CreateDirectory(Path.GetDirectoryName(path)!);

            var tempPath = $"{path}.{Guid.NewGuid():N}.tmp";
            var completed = false;
            try
            {
                await using var output = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, _options.WriteBufferSize, useAsync: true);
                var source = await _store.TryOpenArtifactAsync(manifest.ArtifactId, cancellationToken).ConfigureAwait(false);
                if (source is null)
                {
                    throw new InvalidOperationException($"Artifact {manifest.ArtifactId} could not be opened from source store.");
                }

                await using var _ = source;
                await source.CopyToAsync(output, cancellationToken).ConfigureAwait(false);
                await output.FlushAsync(cancellationToken).ConfigureAwait(false);
                completed = true;
            }
            finally
            {
                if (!completed && File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }
            }

            try
            {
                File.Move(tempPath, path);
            }
            catch (IOException)
            {
                if (File.Exists(path))
                {
                    File.Delete(tempPath);
                }
                else
                {
                    throw;
                }
            }

            return new ArtifactCacheEntry(manifest.ArtifactId, path, manifest.ByteLength, DateTimeOffset.UtcNow);
        }
        finally
        {
            gate.Release();
        }
    }

    public ValueTask<ArtifactCacheEntry?> TryGetRangeAsync(
        Sha256Hash artifactId,
        long offset,
        long length,
        CancellationToken cancellationToken = default)
    {
        ArtifactRangeSupport.ValidateRange(offset, length);

        var path = GetRangePath(artifactId, offset, length);
        if (!File.Exists(path))
        {
            return ValueTask.FromResult<ArtifactCacheEntry?>(null);
        }

        var info = new FileInfo(path);
        if (info.Length != length)
        {
            return ValueTask.FromResult<ArtifactCacheEntry?>(null);
        }

        return ValueTask.FromResult<ArtifactCacheEntry?>(new ArtifactCacheEntry(artifactId, path, info.Length, new DateTimeOffset(info.LastWriteTimeUtc)));
    }

    public async ValueTask<ArtifactCacheEntry> StoreRangeAsync(
        Sha256Hash artifactId,
        long offset,
        long length,
        Stream source,
        CancellationToken cancellationToken = default)
    {
        ArtifactRangeSupport.ValidateRange(offset, length);

        if (source is null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        var path = GetRangePath(artifactId, offset, length);
        if (File.Exists(path))
        {
            var info = new FileInfo(path);
            return new ArtifactCacheEntry(artifactId, path, info.Length, new DateTimeOffset(info.LastWriteTimeUtc));
        }

        var gate = GetWriteGate(path);
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (File.Exists(path))
            {
                var info = new FileInfo(path);
                return new ArtifactCacheEntry(artifactId, path, info.Length, new DateTimeOffset(info.LastWriteTimeUtc));
            }

            Directory.CreateDirectory(Path.GetDirectoryName(path)!);

            var tempPath = $"{path}.{Guid.NewGuid():N}.tmp";
            var completed = false;
            try
            {
                await using var output = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, _options.WriteBufferSize, useAsync: true);
                await source.CopyToAsync(output, cancellationToken).ConfigureAwait(false);
                await output.FlushAsync(cancellationToken).ConfigureAwait(false);
                if (output.Length != length)
                {
                    throw new InvalidOperationException($"Artifact range {artifactId}@{offset}+{length} produced {output.Length} bytes.");
                }

                completed = true;
            }
            finally
            {
                if (!completed && File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }
            }

            try
            {
                File.Move(tempPath, path);
            }
            catch (IOException)
            {
                if (File.Exists(path))
                {
                    File.Delete(tempPath);
                }
                else
                {
                    throw;
                }
            }

            return new ArtifactCacheEntry(artifactId, path, length, DateTimeOffset.UtcNow);
        }
        finally
        {
            gate.Release();
        }
    }

    private SemaphoreSlim GetWriteGate(string path)
        => _writeGates.GetOrAdd(path, static _ => new SemaphoreSlim(1, 1));

    private string GetArtifactPath(Sha256Hash artifactId)
    {
        var hex = artifactId.ToHex();
        if (string.IsNullOrWhiteSpace(hex))
        {
            throw new ArgumentException("Invalid artifact id.", nameof(artifactId));
        }

        return Path.Combine(_options.RootPath, "artifacts", hex);
    }

    private string GetRangePath(Sha256Hash artifactId, long offset, long length)
    {
        var hex = artifactId.ToHex();
        if (string.IsNullOrWhiteSpace(hex))
        {
            throw new ArgumentException("Invalid artifact id.", nameof(artifactId));
        }

        return Path.Combine(_options.RootPath, "ranges", hex, $"{offset}-{length}.bin");
    }
}
