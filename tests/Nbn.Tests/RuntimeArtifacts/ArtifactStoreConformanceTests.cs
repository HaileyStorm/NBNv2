using Microsoft.Data.Sqlite;
using Nbn.Runtime.Artifacts;

namespace Nbn.Tests.Artifacts;

public sealed class ArtifactStoreConformanceTests
{
    [Fact]
    public async Task StoreAsync_OverlappingArtifacts_ReusesChunks_AndIncrementsChunkRefCounts()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-conformance-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = CreateStore(artifactRoot);
            var artifactABytes = JoinChunks(
                CreateChunk(0x11),
                CreateChunk(0x55),
                CreateChunk(0x22));
            var artifactBBytes = JoinChunks(
                CreateChunk(0x33),
                CreateChunk(0x55),
                CreateChunk(0x44));

            var manifestA = await store.StoreAsync(new MemoryStream(artifactABytes), "application/x-nbs");
            var manifestB = await store.StoreAsync(new MemoryStream(artifactBBytes), "application/x-nbs");

            Assert.Equal(3, manifestA.Chunks.Count);
            Assert.Equal(3, manifestB.Chunks.Count);

            var sharedHashes = manifestA.Chunks.Select(chunk => chunk.Hash).Intersect(manifestB.Chunks.Select(chunk => chunk.Hash)).ToList();
            var sharedHash = Assert.Single(sharedHashes);
            var chunkRows = await ReadChunkRowsAsync(Path.Combine(artifactRoot, "artifacts.db"));

            Assert.Equal(5, chunkRows.Count);
            Assert.Equal(5, Directory.EnumerateFiles(Path.Combine(artifactRoot, "chunks"), "*", SearchOption.AllDirectories).Count());

            foreach (var chunkRow in chunkRows.Where(row => row.ChunkHex != sharedHash.ToHex()))
            {
                Assert.Equal(1, chunkRow.RefCount);
                Assert.Equal("none", chunkRow.Compression);
                Assert.Equal(ChunkSize, chunkRow.ByteLength);
                Assert.Equal(ChunkSize, chunkRow.StoredLength);
            }

            var sharedRow = Assert.Single(chunkRows, row => row.ChunkHex == sharedHash.ToHex());
            Assert.Equal(2, sharedRow.RefCount);
            Assert.Equal("none", sharedRow.Compression);
            Assert.Equal(ChunkSize, sharedRow.ByteLength);
            Assert.Equal(ChunkSize, sharedRow.StoredLength);

            Assert.Equal(artifactABytes, await ReadArtifactBytesAsync(store, manifestA.ArtifactId));
            Assert.Equal(artifactBBytes, await ReadArtifactBytesAsync(store, manifestB.ArtifactId));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task StoreAsync_CompressedChunks_PersistsCompressionMetadata_AndRoundTripsBytes()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-compression-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = CreateStore(artifactRoot, ChunkCompressionKind.Zstd);
            var payload = JoinChunks(
                CreateChunk(0x41),
                CreateChunk(0x42),
                CreateChunk(0x43));

            var manifest = await store.StoreAsync(new MemoryStream(payload), "application/x-nbs");
            var persistedManifest = await store.TryGetManifestAsync(manifest.ArtifactId);
            var chunkRows = await ReadChunkRowsAsync(Path.Combine(artifactRoot, "artifacts.db"));

            Assert.NotNull(persistedManifest);
            Assert.Equal(manifest.Chunks.Select(chunk => chunk.Hash), persistedManifest!.Chunks.Select(chunk => chunk.Hash));
            Assert.All(manifest.Chunks, chunk =>
            {
                Assert.Equal(ChunkCompressionKind.Zstd, chunk.Compression);
                Assert.True(chunk.StoredLength < chunk.UncompressedLength);
            });
            Assert.All(persistedManifest.Chunks, chunk =>
            {
                Assert.Equal(ChunkCompressionKind.Zstd, chunk.Compression);
                Assert.True(chunk.StoredLength < chunk.UncompressedLength);
            });

            Assert.Equal(3, chunkRows.Count);
            Assert.All(chunkRows, row =>
            {
                Assert.Equal("zstd", row.Compression);
                Assert.Equal(1, row.RefCount);
                Assert.Equal(ChunkSize, row.ByteLength);
                Assert.True(row.StoredLength < row.ByteLength);
            });

            Assert.Equal(payload, await ReadArtifactBytesAsync(store, manifest.ArtifactId));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task StoreAsync_DuplicateArtifactBytes_WithSameMediaType_ReusesExistingManifest_WithoutIncrementingRefCounts()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-duplicate-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = CreateStore(artifactRoot);
            var payload = JoinChunks(
                CreateChunk(0x71),
                CreateChunk(0x72),
                CreateChunk(0x73));

            var firstManifest = await store.StoreAsync(new MemoryStream(payload), "application/x-nbs");
            var secondManifest = await store.StoreAsync(new MemoryStream(payload), "application/x-nbs");
            var chunkRows = await ReadChunkRowsAsync(Path.Combine(artifactRoot, "artifacts.db"));
            var artifactRows = await ReadArtifactRowsAsync(Path.Combine(artifactRoot, "artifacts.db"));

            Assert.Equal(firstManifest.ArtifactId, secondManifest.ArtifactId);
            Assert.Equal(firstManifest.Chunks.Select(chunk => chunk.Hash), secondManifest.Chunks.Select(chunk => chunk.Hash));
            Assert.Equal(3, chunkRows.Count);
            Assert.All(chunkRows, row => Assert.Equal(1, row.RefCount));

            var artifactRow = Assert.Single(artifactRows);
            Assert.Equal(firstManifest.ArtifactId.ToHex(), artifactRow.ArtifactHex);
            Assert.Equal(1, artifactRow.RefCount);
            Assert.Equal(3, Directory.EnumerateFiles(Path.Combine(artifactRoot, "chunks"), "*", SearchOption.AllDirectories).Count());
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }

    private static LocalArtifactStore CreateStore(string artifactRoot, ChunkCompressionKind chunkCompression = ChunkCompressionKind.None)
        => new(new ArtifactStoreOptions(artifactRoot)
        {
            Chunking = new CdcChunkerOptions
            {
                MinChunkSize = ChunkSize,
                AvgChunkSize = ChunkSize,
                MaxChunkSize = ChunkSize,
                ReadBufferSize = ChunkSize
            },
            ChunkCompression = chunkCompression,
            ChunkCompressionLevel = 3,
            ChunkCompressionMinBytes = 1,
            ChunkCompressionOnlyIfSmaller = true
        });

    private static byte[] CreateChunk(byte value)
    {
        var buffer = new byte[ChunkSize];
        Array.Fill(buffer, value);
        return buffer;
    }

    private static byte[] JoinChunks(params byte[][] chunks)
    {
        var payload = new byte[chunks.Sum(chunk => chunk.Length)];
        var offset = 0;
        foreach (var chunk in chunks)
        {
            Buffer.BlockCopy(chunk, 0, payload, offset, chunk.Length);
            offset += chunk.Length;
        }

        return payload;
    }

    private static async Task<byte[]> ReadArtifactBytesAsync(LocalArtifactStore store, Sha256Hash artifactId)
    {
        await using var stream = await store.TryOpenArtifactAsync(artifactId);
        Assert.NotNull(stream);

        using var buffer = new MemoryStream();
        await stream!.CopyToAsync(buffer);
        return buffer.ToArray();
    }

    private static async Task<List<ChunkRow>> ReadChunkRowsAsync(string databasePath)
    {
        await using var connection = new SqliteConnection($"Data Source={databasePath}");
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT lower(hex(chunk_sha256)),
       byte_length,
       stored_length,
       compression,
       ref_count
FROM chunks
ORDER BY lower(hex(chunk_sha256));";

        var rows = new List<ChunkRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new ChunkRow(
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetString(3),
                reader.GetInt64(4)));
        }

        return rows;
    }

    private static async Task<List<ArtifactRow>> ReadArtifactRowsAsync(string databasePath)
    {
        await using var connection = new SqliteConnection($"Data Source={databasePath}");
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT lower(hex(artifact_sha256)),
       ref_count
FROM artifacts
ORDER BY lower(hex(artifact_sha256));";

        var rows = new List<ArtifactRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new ArtifactRow(
                reader.GetString(0),
                reader.GetInt64(1)));
        }

        return rows;
    }

    private const int ChunkSize = 4096;

    private sealed record ChunkRow(string ChunkHex, long ByteLength, long StoredLength, string Compression, long RefCount);

    private sealed record ArtifactRow(string ArtifactHex, long RefCount);
}
