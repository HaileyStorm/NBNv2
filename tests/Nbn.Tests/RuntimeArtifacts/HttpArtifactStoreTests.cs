using Nbn.Runtime.Artifacts;
using Nbn.Tests.Format;
using Nbn.Tests.TestSupport;

namespace Nbn.Tests.Artifacts;

public sealed class HttpArtifactStoreTests
{
    [Fact]
    public async Task StoreAsync_RichNbn_RoundTripsManifest_FullReads_AndRangeReads_OverHttp()
    {
        await using var server = new HttpArtifactStoreTestServer();
        var store = new HttpArtifactStore(server.BaseUri);
        var vector = NbnTestVectors.CreateRichNbnVector();

        var manifest = await store.StoreAsync(new MemoryStream(vector.Bytes), "application/x-nbn");
        var persisted = await store.TryGetManifestAsync(manifest.ArtifactId);

        Assert.NotNull(persisted);
        Assert.Equal(manifest.ArtifactId, persisted!.ArtifactId);
        Assert.Equal(manifest.MediaType, persisted.MediaType);
        Assert.Equal(manifest.RegionIndex, persisted.RegionIndex);

        await using (var fullStream = await store.TryOpenArtifactAsync(manifest.ArtifactId))
        {
            Assert.NotNull(fullStream);
            Assert.Equal(vector.Bytes, await ReadAllBytesAsync(fullStream!));
        }

        var region = Assert.Single(manifest.RegionIndex, static entry => entry.RegionId == 1);
        await using (var rangeStream = await store.TryOpenArtifactRangeAsync(manifest.ArtifactId, region.Offset, region.Length))
        {
            Assert.NotNull(rangeStream);
            var expected = vector.Bytes.AsSpan((int)region.Offset, (int)region.Length).ToArray();
            Assert.Equal(expected, await ReadAllBytesAsync(rangeStream!));
        }

        Assert.Equal(1, server.StoreRequests);
        Assert.True(server.ManifestRequests >= 1);
        Assert.True(server.ArtifactRequests >= 2);
        Assert.Equal(1, server.RangeRequests);
    }

    [Fact]
    public async Task TryOpenArtifactRangeAsync_WhenServerDoesNotSupportRange_FallsBackToFullArtifactRead()
    {
        await using var server = new HttpArtifactStoreTestServer(supportsRangeRequests: false);
        var store = new HttpArtifactStore(server.BaseUri);
        var vector = NbnTestVectors.CreateRichNbnVector();
        var manifest = await server.SeedAsync(vector.Bytes, "application/x-nbn");
        var region = Assert.Single(manifest.RegionIndex, static entry => entry.RegionId == 1);

        await using var rangeStream = await store.TryOpenArtifactRangeAsync(manifest.ArtifactId, region.Offset, region.Length);
        Assert.NotNull(rangeStream);

        var expected = vector.Bytes.AsSpan((int)region.Offset, (int)region.Length).ToArray();
        Assert.Equal(expected, await ReadAllBytesAsync(rangeStream!));
        Assert.Equal(1, server.RangeRequests);
        Assert.True(server.ArtifactRequests >= 2);
    }

    [Fact]
    public async Task MissingArtifact_ReturnsNulls_InsteadOfThrowing()
    {
        await using var server = new HttpArtifactStoreTestServer();
        var store = new HttpArtifactStore(server.BaseUri);
        var missing = new Sha256Hash(new byte[32]);

        Assert.Null(await store.TryGetManifestAsync(missing));
        Assert.Null(await store.TryOpenArtifactAsync(missing));
        Assert.Null(await store.TryOpenArtifactRangeAsync(missing, 0, 1));
    }

    private static async Task<byte[]> ReadAllBytesAsync(Stream stream)
    {
        using var buffer = new MemoryStream();
        await stream.CopyToAsync(buffer);
        return buffer.ToArray();
    }
}
