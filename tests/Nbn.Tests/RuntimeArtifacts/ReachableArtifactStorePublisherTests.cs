using Microsoft.Data.Sqlite;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Tests.Format;

namespace Nbn.Tests.Artifacts;

[Collection("ArtifactEnvSerial")]
public sealed class ReachableArtifactStorePublisherTests
{
    [Fact]
    public async Task HttpRoute_StoreAsync_RichNbn_RoundTripsRegionIndexManifest()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-reachable-artifacts-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            await using var publisher = new ReachableArtifactStorePublisher();
            var publication = await publisher.PublishAsync(
                bytes: [0x01],
                mediaType: "application/octet-stream",
                backingStoreRoot: artifactRoot,
                bindHost: NetworkAddressDefaults.LoopbackHost);

            var store = new HttpArtifactStore(publication.BaseUri);
            var vector = NbnTestVectors.CreateRichNbnVector();
            var expectedRegionIndex = ArtifactRegionIndexBuilder.BuildFromNbnBytes(vector.Bytes);

            var manifest = await store.StoreAsync(new MemoryStream(vector.Bytes), "application/x-nbn");
            var persistedManifest = await store.TryGetManifestAsync(manifest.ArtifactId);

            Assert.NotEmpty(expectedRegionIndex);
            Assert.Equal(expectedRegionIndex, manifest.RegionIndex);
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
}
