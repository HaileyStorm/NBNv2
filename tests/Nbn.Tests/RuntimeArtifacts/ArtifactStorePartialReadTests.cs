using Microsoft.Data.Sqlite;
using Nbn.Runtime.Artifacts;
using Nbn.Tests.Format;
using System.Buffers.Binary;

namespace Nbn.Tests.Artifacts;

public sealed class ArtifactStorePartialReadTests
{
    [Fact]
    public async Task StoreAsync_SeekableNbn_PersistsRegionIndex_AndSupportsRangeReads()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-partial-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var vector = NbnTestVectors.CreateRichNbnVector();

            var manifest = await store.StoreAsync(new MemoryStream(vector.Bytes), "application/x-nbn");
            var persistedManifest = await store.TryGetManifestAsync(manifest.ArtifactId);

            Assert.NotNull(persistedManifest);
            Assert.NotEmpty(manifest.RegionIndex);
            Assert.Equal(manifest.RegionIndex, persistedManifest!.RegionIndex);

            var region1 = manifest.RegionIndex.Single(entry => entry.RegionId == 1);
            await using var stream = await store.TryOpenArtifactRangeAsync(manifest.ArtifactId, region1.Offset, region1.Length);
            Assert.NotNull(stream);

            using var ms = new MemoryStream();
            await stream!.CopyToAsync(ms);

            var expected = vector.Bytes.AsSpan((int)region1.Offset, (int)region1.Length).ToArray();
            Assert.Equal(expected, ms.ToArray());
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
    public async Task StoreAsync_MalformedSeekableNbn_SkipsAutoRegionIndex_AndStillStoresBytes()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-malformed-nbn-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var vector = NbnTestVectors.CreateRichNbnVector();
            var malformed = vector.Bytes.ToArray();

            BinaryPrimitives.WriteUInt32LittleEndian(malformed.AsSpan(0x100, 4), uint.MaxValue);

            var manifest = await store.StoreAsync(new MemoryStream(malformed), "application/x-nbn");
            var persistedManifest = await store.TryGetManifestAsync(manifest.ArtifactId);

            Assert.NotNull(persistedManifest);
            Assert.Empty(manifest.RegionIndex);
            Assert.Empty(persistedManifest!.RegionIndex);

            await using var stream = await store.TryOpenArtifactAsync(manifest.ArtifactId);
            Assert.NotNull(stream);

            using var ms = new MemoryStream();
            await stream!.CopyToAsync(ms);
            Assert.Equal(malformed, ms.ToArray());
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
    public async Task StoreAsync_SeekableNbn_WithOutOfRangeSectionOffsets_SkipsAutoRegionIndex()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-range-bounds-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var vector = NbnTestVectors.CreateRichNbnVector();
            var malformed = vector.Bytes.ToArray();
            var regionEntryOffset = 0x100 + 24;

            BinaryPrimitives.WriteUInt64LittleEndian(malformed.AsSpan(regionEntryOffset + 12, 8), (ulong)malformed.Length + 128UL);

            var manifest = await store.StoreAsync(new MemoryStream(malformed), "application/x-nbn");
            var persistedManifest = await store.TryGetManifestAsync(manifest.ArtifactId);

            Assert.NotNull(persistedManifest);
            Assert.Empty(manifest.RegionIndex);
            Assert.Empty(persistedManifest!.RegionIndex);
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
    public async Task TryOpenArtifactRangeAsync_OverflowingEnd_IsRejectedAcrossLocalAndCachingPaths()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-range-overflow-{Guid.NewGuid():N}");
        var uncachedRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-range-uncached-{Guid.NewGuid():N}");
        var cachedRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-range-cached-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var payload = new byte[] { 1, 2, 3, 4 };
            var local = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var manifest = await local.StoreAsync(new MemoryStream(payload), "application/octet-stream");
            var uncached = new CachingArtifactStore(local, new ArtifactCacheOptions(uncachedRoot));
            var cached = new CachingArtifactStore(local, new ArtifactCacheOptions(cachedRoot));

            await using (var hydrated = await cached.TryOpenArtifactAsync(manifest.ArtifactId))
            {
                Assert.NotNull(hydrated);
            }

            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                () => local.TryOpenArtifactRangeAsync(manifest.ArtifactId, 1, long.MaxValue));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                () => uncached.TryOpenArtifactRangeAsync(manifest.ArtifactId, 1, long.MaxValue));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                () => cached.TryOpenArtifactRangeAsync(manifest.ArtifactId, 1, long.MaxValue));

            await using var localBoundary = await local.TryOpenArtifactRangeAsync(manifest.ArtifactId, payload.Length, 0);
            await using var cachedBoundary = await cached.TryOpenArtifactRangeAsync(manifest.ArtifactId, payload.Length, 0);
            Assert.NotNull(localBoundary);
            Assert.NotNull(cachedBoundary);
            Assert.Equal(0, localBoundary!.Length);
            Assert.Equal(0, cachedBoundary!.Length);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var path in new[] { artifactRoot, uncachedRoot, cachedRoot })
            {
                if (Directory.Exists(path))
                {
                    Directory.Delete(path, recursive: true);
                }
            }
        }
    }
}
