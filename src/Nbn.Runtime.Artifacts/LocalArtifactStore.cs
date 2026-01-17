using System.Security.Cryptography;

namespace Nbn.Runtime.Artifacts;

public sealed class LocalArtifactStore : IArtifactStore
{
    private readonly ArtifactStoreOptions _options;
    private readonly FastCdcChunker _chunker;
    private readonly ChunkStore _chunkStore;
    private readonly ArtifactStoreDatabase _database;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;

    public LocalArtifactStore(ArtifactStoreOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _chunker = new FastCdcChunker(_options.Chunking);
        _chunkStore = new ChunkStore(_options.ChunkRootPath);
        _database = new ArtifactStoreDatabase(_options.DatabasePath);
    }

    public async Task<ArtifactManifest> StoreAsync(Stream content, string mediaType, ArtifactStoreWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        if (content is null)
        {
            throw new ArgumentNullException(nameof(content));
        }

        if (!content.CanRead)
        {
            throw new ArgumentException("Content stream must be readable.", nameof(content));
        }

        if (string.IsNullOrWhiteSpace(mediaType))
        {
            throw new ArgumentException("Media type is required.", nameof(mediaType));
        }

        var chunks = new List<ArtifactChunkRef>();
        var byteLength = 0L;

        using var artifactHasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

        await _chunker.ChunkAsync(content, async chunk =>
        {
            artifactHasher.AppendData(chunk.Span);
            byteLength += chunk.Length;

            var chunkHash = Sha256Hash.Compute(chunk.Span);
            chunks.Add(new ArtifactChunkRef(chunkHash, chunk.Length));

            await _chunkStore.TryWriteChunkAsync(chunkHash, chunk, cancellationToken);
        }, cancellationToken);

        var artifactId = new Sha256Hash(artifactHasher.GetHashAndReset());
        var manifest = new ArtifactManifest(artifactId, mediaType, byteLength, chunks, options?.RegionIndex);
        var manifestHash = manifest.ComputeManifestHash();

        var inserted = await _database.TryInsertArtifactAsync(manifest, manifestHash, DateTimeOffset.UtcNow, cancellationToken);
        if (!inserted)
        {
            var existing = await _database.TryGetManifestAsync(artifactId, cancellationToken);
            return existing ?? manifest;
        }

        return manifest;
    }

    public async Task<ArtifactManifest?> TryGetManifestAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        return await _database.TryGetManifestAsync(artifactId, cancellationToken);
    }

    public async Task<bool> ContainsAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        return await _database.ArtifactExistsAsync(artifactId, cancellationToken);
    }

    public async Task<Stream?> TryOpenArtifactAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        var manifest = await TryGetManifestAsync(artifactId, cancellationToken);
        if (manifest is null)
        {
            return null;
        }

        return OpenArtifactStream(manifest);
    }

    public Stream OpenArtifactStream(ArtifactManifest manifest)
    {
        if (manifest is null)
        {
            throw new ArgumentNullException(nameof(manifest));
        }

        return new ArtifactChunkStream(_chunkStore, manifest.Chunks);
    }

    internal Stream OpenChunkStream(Sha256Hash chunkHash) => _chunkStore.OpenRead(chunkHash);

    internal string GetChunkPath(Sha256Hash chunkHash) => _chunkStore.GetChunkPath(chunkHash);

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
        {
            return;
        }

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_initialized)
            {
                return;
            }

            Directory.CreateDirectory(_options.RootPath);
            Directory.CreateDirectory(_options.ChunkRootPath);
            await _database.InitializeAsync(cancellationToken);
            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }
}
