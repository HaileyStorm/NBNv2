namespace Nbn.Runtime.Artifacts;

public sealed class LocalArtifactCache : IArtifactCache
{
    private readonly LocalArtifactStore _store;
    private readonly ArtifactCacheOptions _options;

    public LocalArtifactCache(LocalArtifactStore store, ArtifactCacheOptions options)
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

        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        var tempPath = path + ".tmp";
        var completed = false;

        try
        {
            await using var output = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, _options.WriteBufferSize, useAsync: true);
            await using var source = _store.OpenArtifactStream(manifest);
            await source.CopyToAsync(output, cancellationToken);
            await output.FlushAsync(cancellationToken);
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

    private string GetArtifactPath(Sha256Hash artifactId)
    {
        var hex = artifactId.ToHex();
        if (string.IsNullOrWhiteSpace(hex))
        {
            throw new ArgumentException("Invalid artifact id.", nameof(artifactId));
        }

        return Path.Combine(_options.RootPath, "artifacts", hex);
    }
}
