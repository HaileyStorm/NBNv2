using Microsoft.Data.Sqlite;
using Nbn.Runtime.Artifacts;
using Nbn.Tests.Format;

namespace Nbn.Tests.Artifacts;

public sealed class ArtifactStoreMetadataReconciliationTests
{
    [Fact]
    public async Task StoreAsync_DuplicateArtifactBytes_WithRicherRegionIndex_MergesPersistedManifest()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-merge-index-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = CreateStore(artifactRoot);
            var vector = NbnTestVectors.CreateRichNbnVector();

            var firstManifest = await store.StoreAsync(new NonSeekableReadStream(vector.Bytes), "application/x-nbn");
            var secondManifest = await store.StoreAsync(new MemoryStream(vector.Bytes), "application/x-nbn");
            var persistedManifest = await store.TryGetManifestAsync(firstManifest.ArtifactId);
            var expectedRegionIndex = ArtifactRegionIndexBuilder.BuildFromNbnBytes(vector.Bytes);

            Assert.Empty(firstManifest.RegionIndex);
            Assert.NotEmpty(secondManifest.RegionIndex);
            Assert.Equal(expectedRegionIndex, secondManifest.RegionIndex);
            Assert.NotNull(persistedManifest);
            Assert.Equal(expectedRegionIndex, persistedManifest!.RegionIndex);
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
    public async Task StoreAsync_DuplicateArtifactBytes_WithDifferentMediaType_ThrowsExplicitConflict()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-media-conflict-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = CreateStore(artifactRoot);
            var payload = ArtifactStoreTestData.JoinChunks(
                ArtifactStoreTestData.CreateChunk(0x31),
                ArtifactStoreTestData.CreateChunk(0x32));

            var firstManifest = await store.StoreAsync(new MemoryStream(payload), "application/x-nbs");

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.StoreAsync(new MemoryStream(payload), "application/x-nbn"));

            Assert.Contains("already stored as media type", exception.Message, StringComparison.OrdinalIgnoreCase);

            var persistedManifest = await store.TryGetManifestAsync(firstManifest.ArtifactId);
            Assert.NotNull(persistedManifest);
            Assert.Equal("application/x-nbs", persistedManifest!.MediaType);
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
    public async Task TryGetManifestAsync_CachingStore_RefreshesMergedRegionIndexFromUpstream()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-cache-refresh-{Guid.NewGuid():N}");
        var cacheRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-cache-refresh-cache-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);
        Directory.CreateDirectory(cacheRoot);

        try
        {
            var upstream = CreateStore(artifactRoot);
            var cachingStore = new CachingArtifactStore(upstream, new ArtifactCacheOptions(cacheRoot));
            var secondWriter = CreateStore(artifactRoot);
            var vector = NbnTestVectors.CreateRichNbnVector();

            var firstManifest = await cachingStore.StoreAsync(new NonSeekableReadStream(vector.Bytes), "application/x-nbn");
            Assert.Empty(firstManifest.RegionIndex);

            var enrichedManifest = await secondWriter.StoreAsync(new MemoryStream(vector.Bytes), "application/x-nbn");
            var refreshedManifest = await cachingStore.TryGetManifestAsync(firstManifest.ArtifactId);

            Assert.NotEmpty(enrichedManifest.RegionIndex);
            Assert.NotNull(refreshedManifest);
            Assert.Equal(enrichedManifest.RegionIndex, refreshedManifest!.RegionIndex);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }

            if (Directory.Exists(cacheRoot))
            {
                Directory.Delete(cacheRoot, recursive: true);
            }
        }
    }

    private static LocalArtifactStore CreateStore(string artifactRoot)
        => new(new ArtifactStoreOptions(artifactRoot)
        {
            Chunking = new CdcChunkerOptions
            {
                MinChunkSize = ArtifactStoreTestData.ChunkSize,
                AvgChunkSize = ArtifactStoreTestData.ChunkSize,
                MaxChunkSize = ArtifactStoreTestData.ChunkSize,
                ReadBufferSize = ArtifactStoreTestData.ChunkSize
            }
        });

    private sealed class NonSeekableReadStream : Stream
    {
        private readonly byte[] _data;
        private int _position;

        public NonSeekableReadStream(byte[] data)
        {
            _data = data;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => _position;
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var bytesToCopy = Math.Min(count, _data.Length - _position);
            if (bytesToCopy <= 0)
            {
                return 0;
            }

            Buffer.BlockCopy(_data, _position, buffer, offset, bytesToCopy);
            _position += bytesToCopy;
            return bytesToCopy;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var bytesToCopy = Math.Min(buffer.Length, _data.Length - _position);
            if (bytesToCopy <= 0)
            {
                return ValueTask.FromResult(0);
            }

            _data.AsMemory(_position, bytesToCopy).CopyTo(buffer);
            _position += bytesToCopy;
            return ValueTask.FromResult(bytesToCopy);
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private static class ArtifactStoreTestData
    {
        public const int ChunkSize = 4096;

        public static byte[] CreateChunk(byte value)
        {
            var buffer = new byte[ChunkSize];
            Array.Fill(buffer, value);
            return buffer;
        }

        public static byte[] JoinChunks(params byte[][] chunks)
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
    }
}
