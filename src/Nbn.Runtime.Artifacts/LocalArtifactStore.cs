using System.Security.Cryptography;

namespace Nbn.Runtime.Artifacts;

/// <summary>
/// Stores artifacts in a local content-addressed chunk store backed by SQLite metadata.
/// </summary>
public sealed class LocalArtifactStore : IArtifactStore
{
    private static readonly TimeSpan ChunkMetadataResolutionTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan ChunkMetadataWaitDelay = TimeSpan.FromMilliseconds(25);
    private readonly ArtifactStoreOptions _options;
    private readonly FastCdcChunker _chunker;
    private readonly ChunkStore _chunkStore;
    private readonly ArtifactStoreDatabase _database;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;

    /// <summary>
    /// Initializes a local artifact store rooted at the provided artifact directory.
    /// </summary>
    public LocalArtifactStore(ArtifactStoreOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _chunker = new FastCdcChunker(_options.Chunking);
        _chunkStore = new ChunkStore(_options.ChunkRootPath);
        _database = new ArtifactStoreDatabase(_options.DatabasePath);
    }

    /// <inheritdoc />
    public async Task<ArtifactManifest> StoreAsync(Stream content, string mediaType, ArtifactStoreWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

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

        options = ArtifactRegionIndexBuilder.PopulateIfMissing(content, mediaType, options);

        var chunks = new List<ArtifactChunkInfo>();
        var byteLength = 0L;

        using var artifactHasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

        await _chunker.ChunkAsync(content, async chunk =>
        {
            artifactHasher.AppendData(chunk.Span);
            byteLength += chunk.Length;

            var chunkHash = Sha256Hash.Compute(chunk.Span);
            var writeResult = PrepareChunk(chunk);

            var wrote = await _chunkStore.TryWriteChunkAsync(chunkHash, writeResult.Payload, cancellationToken).ConfigureAwait(false);
            var compression = writeResult.Compression;
            var storedLength = writeResult.StoredLength;

            if (!wrote)
            {
                var metadata = await ResolveChunkMetadataAsync(chunkHash, chunk, cancellationToken).ConfigureAwait(false);
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

        var inserted = await _database.TryInsertArtifactAsync(manifest, manifestHash, DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
        if (!inserted)
        {
            var existing = await _database.ResolveExistingManifestAsync(manifest, cancellationToken).ConfigureAwait(false);
            return existing ?? manifest;
        }

        return manifest;
    }

    /// <inheritdoc />
    public async Task<ArtifactManifest?> TryGetManifestAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        return await _database.TryGetManifestAsync(artifactId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> ContainsAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        return await _database.ArtifactExistsAsync(artifactId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<Stream?> TryOpenArtifactAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        var manifest = await TryGetManifestAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (manifest is null)
        {
            return null;
        }

        return OpenArtifactStream(manifest);
    }

    /// <inheritdoc />
    public async Task<Stream?> TryOpenArtifactRangeAsync(Sha256Hash artifactId, long offset, long length, CancellationToken cancellationToken = default)
    {
        ArtifactRangeSupport.ValidateRange(offset, length);

        var manifest = await TryGetManifestAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (manifest is null)
        {
            return null;
        }

        if (offset > manifest.ByteLength || offset + length > manifest.ByteLength)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "Requested range exceeds artifact length.");
        }

        if (length == 0)
        {
            return new MemoryStream(Array.Empty<byte>(), writable: false);
        }

        return new ArtifactChunkRangeStream(_chunkStore, manifest.Chunks, offset, length);
    }

    /// <summary>
    /// Opens a readable stream for a manifest that is already known to exist locally.
    /// </summary>
    public Stream OpenArtifactStream(ArtifactManifest manifest)
    {
        if (manifest is null)
        {
            throw new ArgumentNullException(nameof(manifest));
        }

        return new ArtifactChunkStream(_chunkStore, manifest.Chunks);
    }

    internal string GetChunkPath(Sha256Hash chunkHash) => _chunkStore.GetChunkPath(chunkHash);

    private async Task<ArtifactStoreDatabase.ChunkMetadata?> ResolveChunkMetadataAsync(
        Sha256Hash chunkHash,
        ReadOnlyMemory<byte> chunk,
        CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + ChunkMetadataResolutionTimeout;

        while (true)
        {
            var metadata = await _database.TryGetChunkMetadataAsync(chunkHash, cancellationToken);
            if (metadata is not null)
            {
                return metadata;
            }

            var derivedMetadata = await TryDeriveChunkMetadataAsync(chunkHash, chunk, cancellationToken);
            if (derivedMetadata is not null)
            {
                return derivedMetadata;
            }

            if (DateTime.UtcNow >= deadline)
            {
                return null;
            }

            await Task.Delay(ChunkMetadataWaitDelay, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<ArtifactStoreDatabase.ChunkMetadata?> TryDeriveChunkMetadataAsync(
        Sha256Hash chunkHash,
        ReadOnlyMemory<byte> chunk,
        CancellationToken cancellationToken)
    {
        try
        {
            var path = _chunkStore.GetChunkPath(chunkHash);
            if (!File.Exists(path))
            {
                return null;
            }

            var storedBytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
            if (storedBytes.Length == chunk.Length
                && storedBytes.AsSpan().SequenceEqual(chunk.Span))
            {
                return new ArtifactStoreDatabase.ChunkMetadata(chunk.Length, storedBytes.Length, ChunkCompression.NoneLabel);
            }

            try
            {
                var decompressed = ChunkCompression.DecompressZstd(storedBytes, chunk.Length);
                if (decompressed.AsSpan().SequenceEqual(chunk.Span))
                {
                    return new ArtifactStoreDatabase.ChunkMetadata(chunk.Length, storedBytes.Length, ChunkCompression.ZstdLabel);
                }
            }
            catch
            {
                return null;
            }

            return null;
        }
        catch (IOException)
        {
            return null;
        }
    }

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
        {
            return;
        }

        await _initLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_initialized)
            {
                return;
            }

            Directory.CreateDirectory(_options.RootPath);
            Directory.CreateDirectory(_options.ChunkRootPath);
            await _database.InitializeAsync(cancellationToken).ConfigureAwait(false);
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
