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
        Assert.Equal(0, server.RangeRequests);
    }

    [Fact]
    public async Task TryOpenArtifactRangeAsync_DerivesRangeFromVerifiedFullArtifactRead()
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
        Assert.Equal(0, server.RangeRequests);
        Assert.Equal(1, server.ArtifactRequests);
    }

    [Theory]
    [InlineData("truncated")]
    [InlineData("oversized")]
    [InlineData("hash-mismatch")]
    public async Task TryOpenArtifactAsync_WhenPayloadIdentityIsInvalid_RejectsBeforeReturningStream(string corruption)
    {
        await using var server = new HttpArtifactStoreTestServer(
            artifactPayloadTransform: payload => CorruptPayload(payload, corruption));
        var expected = new byte[] { 3, 1, 4, 1, 5, 9 };
        var manifest = await server.SeedAsync(expected, "application/octet-stream");
        var store = new HttpArtifactStore(server.BaseUri);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => store.TryOpenArtifactAsync(manifest.ArtifactId));
    }

    [Fact]
    public async Task CachingStore_WhenHttpPayloadHashIsInvalid_DoesNotPolluteArtifactCache()
    {
        await using var server = new HttpArtifactStoreTestServer(
            artifactPayloadTransform: payload => CorruptPayload(payload, "hash-mismatch"));
        var expected = new byte[] { 2, 7, 1, 8, 2, 8 };
        var manifest = await server.SeedAsync(expected, "application/octet-stream");
        var cacheRoot = Path.Combine(Path.GetTempPath(), $"nbn-http-integrity-cache-{Guid.NewGuid():N}");

        try
        {
            var store = new CachingArtifactStore(
                new HttpArtifactStore(server.BaseUri),
                new ArtifactCacheOptions(cacheRoot));

            await Assert.ThrowsAsync<InvalidDataException>(
                () => store.TryOpenArtifactAsync(manifest.ArtifactId));

            Assert.False(
                Directory.Exists(cacheRoot)
                && Directory.EnumerateFiles(cacheRoot, "*", SearchOption.AllDirectories).Any());
        }
        finally
        {
            if (Directory.Exists(cacheRoot))
            {
                Directory.Delete(cacheRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task MissingArtifact_ReturnsNulls_InsteadOfThrowing()
    {
        await using var server = new HttpArtifactStoreTestServer();
        var store = new HttpArtifactStore(server.BaseUri);
        var missing = new Sha256Hash(new byte[32]);

        Assert.Null(await store.TryGetManifestAsync(missing));
        Assert.Null(await store.TryOpenArtifactAsync(missing));
        Assert.Null(await store.TryOpenArtifactRangeAsync(missing, 0, 0));
        Assert.Null(await store.TryOpenArtifactRangeAsync(missing, 0, 1));
    }

    [Fact]
    public async Task TryOpenArtifactRangeAsync_ZeroLengthBeyondEnd_IsRejected()
    {
        await using var server = new HttpArtifactStoreTestServer();
        var payload = new byte[] { 1, 2, 3 };
        var manifest = await server.SeedAsync(payload, "application/octet-stream");
        var store = new HttpArtifactStore(server.BaseUri);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => store.TryOpenArtifactRangeAsync(manifest.ArtifactId, payload.Length + 1, 0));
    }

    private static async Task<byte[]> ReadAllBytesAsync(Stream stream)
    {
        using var buffer = new MemoryStream();
        await stream.CopyToAsync(buffer);
        return buffer.ToArray();
    }

    private static byte[] CorruptPayload(byte[] payload, string corruption)
        => corruption switch
        {
            "truncated" => payload[..^1],
            "oversized" => [.. payload, 0],
            "hash-mismatch" => payload.Select((value, index) => index == 0 ? (byte)(value ^ 0xff) : value).ToArray(),
            _ => throw new ArgumentOutOfRangeException(nameof(corruption), corruption, "Unknown corruption mode.")
        };
}
