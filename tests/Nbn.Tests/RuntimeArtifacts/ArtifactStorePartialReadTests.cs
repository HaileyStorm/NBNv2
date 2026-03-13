using Microsoft.Data.Sqlite;
using Nbn.Runtime.Artifacts;
using Nbn.Tests.Format;

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
}
