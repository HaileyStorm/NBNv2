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

        var chunks = new List<ArtifactChunkInfo>();
        var byteLength = 0L;

        using var artifactHasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

        await _chunker.ChunkAsync(content, async chunk =>
        {
            artifactHasher.AppendData(chunk.Span);
            byteLength += chunk.Length;

            var chunkHash = Sha256Hash.Compute(chunk.Span);
            var writeResult = PrepareChunk(chunk);

            var wrote = await _chunkStore.TryWriteChunkAsync(chunkHash, writeResult.Payload, cancellationToken);
            var compression = writeResult.Compression;
            var storedLength = writeResult.StoredLength;

            if (!wrote)
            {
                var metadata = await _database.TryGetChunkMetadataAsync(chunkHash, cancellationToken);
                if (metadata is null)
                {
                    throw new InvalidOperationException($"Chunk {chunkHash} exists on disk but metadata is missing.");
                }

                compression = ChunkCompression.FromLabel(metadata.Compression);
                storedLength = checked((int)metadata.StoredLength);
            }

            chunks.Add(new ArtifactChunkInfo(chunkHash, chunk.Length, storedLength, compression));
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

    private ChunkWriteResult PrepareChunk(ReadOnlyMemory<byte> chunk)
    {
        if (_options.ChunkCompression == ChunkCompressionKind.None || chunk.Length < _options.ChunkCompressionMinBytes)
        {
            return new ChunkWriteResult(chunk, chunk.Length, ChunkCompressionKind.None);
        }

        if (_options.ChunkCompression == ChunkCompressionKind.Zstd)
        {
            var compressed = ChunkCompression.CompressZstd(chunk.Span, _options.ChunkCompressionLevel);
            if (_options.ChunkCompressionOnlyIfSmaller && compressed.Length >= chunk.Length)
            {
                return new ChunkWriteResult(chunk, chunk.Length, ChunkCompressionKind.None);
            }

            return new ChunkWriteResult(compressed, compressed.Length, ChunkCompressionKind.Zstd);
        }

        return new ChunkWriteResult(chunk, chunk.Length, ChunkCompressionKind.None);
    }

    private readonly struct ChunkWriteResult
    {
        public ChunkWriteResult(ReadOnlyMemory<byte> payload, int storedLength, ChunkCompressionKind compression)
        {
            Payload = payload;
            StoredLength = storedLength;
            Compression = compression;
        }

        public ReadOnlyMemory<byte> Payload { get; }
        public int StoredLength { get; }
        public ChunkCompressionKind Compression { get; }
    }
}
