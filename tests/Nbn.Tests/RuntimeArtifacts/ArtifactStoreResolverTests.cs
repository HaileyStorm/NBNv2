using Microsoft.Data.Sqlite;
using Nbn.Runtime.Artifacts;
using Nbn.Tests.Format;
using Nbn.Tests.TestSupport;
using System.Text.Json;

namespace Nbn.Tests.Artifacts;

[Collection("ArtifactEnvSerial")]
public sealed class ArtifactStoreResolverTests
{
    [Fact]
    public async Task Resolve_NonFileStoreUri_Reuses_NodeLocalCache_After_FirstFetch()
    {
        using var remoteScope = new RegisteredArtifactStoreScope();
        var localRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-resolver-{Guid.NewGuid():N}");
        Directory.CreateDirectory(localRoot);

        try
        {
            var payload = new byte[] { 1, 2, 3, 4, 5, 6 };
            var manifest = await remoteScope.Store.StoreAsync(new MemoryStream(payload), "application/x-nbn");
            var resolver = new ArtifactStoreResolver(new ArtifactStoreResolverOptions(localRoot));
            var store = resolver.Resolve(remoteScope.StoreUri);

            await using (var first = await store.TryOpenArtifactAsync(manifest.ArtifactId))
            {
                Assert.NotNull(first);
            }

            Assert.Equal(1, remoteScope.Store.OpenCalls);

            await using (var second = await store.TryOpenArtifactAsync(manifest.ArtifactId))
            {
                Assert.NotNull(second);
            }

            Assert.Equal(1, remoteScope.Store.OpenCalls);
            var cachedArtifactPath = Path.Combine(localRoot, ".cache", "artifacts", manifest.ArtifactId.ToHex());
            Assert.True(File.Exists(cachedArtifactPath));
        }
        finally
        {
            if (Directory.Exists(localRoot))
            {
                Directory.Delete(localRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task Resolve_NonFileStoreUri_Reuses_NodeLocalRangeCache_After_FirstFetch()
    {
        using var remoteScope = new RegisteredArtifactStoreScope();
        var localRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-range-resolver-{Guid.NewGuid():N}");
        Directory.CreateDirectory(localRoot);

        try
        {
            var vector = NbnTestVectors.CreateRichNbnVector();
            var manifest = await remoteScope.Store.StoreAsync(new MemoryStream(vector.Bytes), "application/x-nbn");
            var range = manifest.RegionIndex.Single(entry => entry.RegionId == 1);
            var resolver = new ArtifactStoreResolver(new ArtifactStoreResolverOptions(localRoot));
            var store = resolver.Resolve(remoteScope.StoreUri);

            await using (var first = await store.TryOpenArtifactRangeAsync(manifest.ArtifactId, range.Offset, range.Length))
            {
                Assert.NotNull(first);
            }

            Assert.Equal(1, remoteScope.Store.RangeOpenCalls);

            await using (var second = await store.TryOpenArtifactRangeAsync(manifest.ArtifactId, range.Offset, range.Length))
            {
                Assert.NotNull(second);
            }

            Assert.Equal(1, remoteScope.Store.RangeOpenCalls);
            var cachedRangePath = Path.Combine(localRoot, ".cache", "ranges", manifest.ArtifactId.ToHex(), $"{range.Offset}-{range.Length}.bin");
            Assert.True(File.Exists(cachedRangePath));
        }
        finally
        {
            if (Directory.Exists(localRoot))
            {
                Directory.Delete(localRoot, recursive: true);
            }
        }
    }

    [Fact]
    public void Resolve_UnregisteredNonFileStoreUri_ThrowsExplicitError()
    {
        var resolver = new ArtifactStoreResolver();
        var ex = Assert.Throws<InvalidOperationException>(() => resolver.Resolve("memory+missing://store/artifacts"));
        Assert.Contains("No artifact store adapter is registered", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Resolve_NonFileStoreUri_Uses_EnvironmentUriMap_When_NoRegistrationExists()
    {
        var remoteRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-env-map-{Guid.NewGuid():N}");
        var localRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-env-cache-{Guid.NewGuid():N}");
        Directory.CreateDirectory(remoteRoot);
        Directory.CreateDirectory(localRoot);

        var original = Environment.GetEnvironmentVariable("NBN_ARTIFACT_STORE_URI_MAP");
        var storeUri = $"memory+env://{Guid.NewGuid():N}/artifacts";
        Environment.SetEnvironmentVariable(
            "NBN_ARTIFACT_STORE_URI_MAP",
            JsonSerializer.Serialize(new Dictionary<string, string>
            {
                [storeUri] = remoteRoot
            }));

        try
        {
            var upstream = new LocalArtifactStore(new ArtifactStoreOptions(remoteRoot));
            var manifest = await upstream.StoreAsync(new MemoryStream(new byte[] { 7, 8, 9, 10 }), "application/x-nbn");
            var resolver = new ArtifactStoreResolver(new ArtifactStoreResolverOptions(localRoot));
            var store = resolver.Resolve(storeUri);

            await using (var stream = await store.TryOpenArtifactAsync(manifest.ArtifactId))
            {
                Assert.NotNull(stream);
            }

            var cachedArtifactPath = Path.Combine(localRoot, ".cache", "artifacts", manifest.ArtifactId.ToHex());
            Assert.True(File.Exists(cachedArtifactPath));
        }
        finally
        {
            Environment.SetEnvironmentVariable("NBN_ARTIFACT_STORE_URI_MAP", original);
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(remoteRoot))
            {
                Directory.Delete(remoteRoot, recursive: true);
            }

            if (Directory.Exists(localRoot))
            {
                Directory.Delete(localRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task Resolve_HttpStoreUri_Reuses_NodeLocalCache_After_FirstFetch()
    {
        await using var server = new HttpArtifactStoreTestServer();
        var localRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-http-cache-{Guid.NewGuid():N}");
        Directory.CreateDirectory(localRoot);

        try
        {
            var payload = new byte[] { 11, 12, 13, 14, 15, 16 };
            var manifest = await server.SeedAsync(payload, "application/x-nbn");
            var resolver = new ArtifactStoreResolver(new ArtifactStoreResolverOptions(localRoot));
            var store = resolver.Resolve(server.BaseUri.AbsoluteUri);

            await using (var first = await store.TryOpenArtifactAsync(manifest.ArtifactId))
            {
                Assert.NotNull(first);
            }

            await using (var second = await store.TryOpenArtifactAsync(manifest.ArtifactId))
            {
                Assert.NotNull(second);
            }

            Assert.Equal(1, server.ArtifactRequests);
            Assert.Equal(0, server.RangeRequests);
            var cachedArtifactPath = Path.Combine(localRoot, ".cache", "artifacts", manifest.ArtifactId.ToHex());
            Assert.True(File.Exists(cachedArtifactPath));
        }
        finally
        {
            if (Directory.Exists(localRoot))
            {
                Directory.Delete(localRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task Resolve_HttpStoreUri_WhenCacheRootIsInvalid_FallsBackToUpstreamRead()
    {
        await using var server = new HttpArtifactStoreTestServer();
        var localRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-http-fallback-{Guid.NewGuid():N}");
        Directory.CreateDirectory(localRoot);
        var invalidCacheRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-http-cache-file-{Guid.NewGuid():N}");
        await File.WriteAllTextAsync(invalidCacheRoot, "cache-root-is-a-file");

        try
        {
            var payload = new byte[] { 31, 32, 33, 34, 35, 36 };
            var manifest = await server.SeedAsync(payload, "application/x-nbn");
            var resolver = new ArtifactStoreResolver(new ArtifactStoreResolverOptions(localRoot, invalidCacheRoot));
            var store = resolver.Resolve(server.BaseUri.AbsoluteUri);

            await using (var first = await store.TryOpenArtifactAsync(manifest.ArtifactId))
            {
                Assert.NotNull(first);
                Assert.Equal(payload, await ReadAllBytesAsync(first!));
            }

            await using (var second = await store.TryOpenArtifactAsync(manifest.ArtifactId))
            {
                Assert.NotNull(second);
                Assert.Equal(payload, await ReadAllBytesAsync(second!));
            }

            Assert.Equal(2, server.ArtifactRequests);
            Assert.False(Directory.Exists(Path.Combine(invalidCacheRoot, "artifacts")));
        }
        finally
        {
            if (Directory.Exists(localRoot))
            {
                Directory.Delete(localRoot, recursive: true);
            }

            if (File.Exists(invalidCacheRoot))
            {
                File.Delete(invalidCacheRoot);
            }
        }
    }

    [Fact]
    public async Task Resolve_NonFileStoreUri_Uses_EnvironmentHttpTarget_When_Configured()
    {
        await using var server = new HttpArtifactStoreTestServer();
        var localRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-http-env-root-{Guid.NewGuid():N}");
        Directory.CreateDirectory(localRoot);

        var original = Environment.GetEnvironmentVariable("NBN_ARTIFACT_STORE_URI_MAP");
        var storeUri = $"memory+env-http://{Guid.NewGuid():N}/artifacts";
        Environment.SetEnvironmentVariable(
            "NBN_ARTIFACT_STORE_URI_MAP",
            JsonSerializer.Serialize(new Dictionary<string, string>
            {
                [storeUri] = server.BaseUri.AbsoluteUri
            }));

        try
        {
            var payload = new byte[] { 21, 22, 23, 24 };
            var manifest = await server.SeedAsync(payload, "application/x-nbn");
            var resolver = new ArtifactStoreResolver(new ArtifactStoreResolverOptions(localRoot));
            var store = resolver.Resolve(storeUri);

            await using (var stream = await store.TryOpenArtifactAsync(manifest.ArtifactId))
            {
                Assert.NotNull(stream);
            }

            Assert.Equal(1, server.ArtifactRequests);
            var cachedArtifactPath = Path.Combine(localRoot, ".cache", "artifacts", manifest.ArtifactId.ToHex());
            Assert.True(File.Exists(cachedArtifactPath));
        }
        finally
        {
            Environment.SetEnvironmentVariable("NBN_ARTIFACT_STORE_URI_MAP", original);
            if (Directory.Exists(localRoot))
            {
                Directory.Delete(localRoot, recursive: true);
            }
        }
    }

    [Fact]
    public void ResolveLocalStoreRootPath_WithoutOverrides_Uses_DefaultUserLocalArtifactRoot()
    {
        var originalRoot = Environment.GetEnvironmentVariable("NBN_ARTIFACT_ROOT");
        try
        {
            Environment.SetEnvironmentVariable("NBN_ARTIFACT_ROOT", null);

            var resolved = ArtifactStoreResolverOptions.ResolveLocalStoreRootPath();
            var expected = ArtifactStoreResolverOptions.ResolveDefaultArtifactRootPath();

            Assert.Equal(expected, resolved);
            Assert.NotEqual(Path.Combine(Environment.CurrentDirectory, "artifacts"), resolved);
        }
        finally
        {
            Environment.SetEnvironmentVariable("NBN_ARTIFACT_ROOT", originalRoot);
        }
    }

    [Fact]
    public void ResolveCacheRootPath_WithoutOverrides_Uses_DefaultArtifactCacheUnderResolvedRoot()
    {
        var originalRoot = Environment.GetEnvironmentVariable("NBN_ARTIFACT_ROOT");
        var originalCache = Environment.GetEnvironmentVariable("NBN_ARTIFACT_CACHE_ROOT");
        try
        {
            Environment.SetEnvironmentVariable("NBN_ARTIFACT_ROOT", null);
            Environment.SetEnvironmentVariable("NBN_ARTIFACT_CACHE_ROOT", null);

            var localRoot = ArtifactStoreResolverOptions.ResolveLocalStoreRootPath();
            var cacheRoot = ArtifactStoreResolverOptions.ResolveCacheRootPath(localRoot);

            Assert.Equal(Path.Combine(localRoot, ".cache"), cacheRoot);
        }
        finally
        {
            Environment.SetEnvironmentVariable("NBN_ARTIFACT_ROOT", originalRoot);
            Environment.SetEnvironmentVariable("NBN_ARTIFACT_CACHE_ROOT", originalCache);
        }
    }

    private static async Task<byte[]> ReadAllBytesAsync(Stream stream)
    {
        using var buffer = new MemoryStream();
        await stream.CopyToAsync(buffer);
        return buffer.ToArray();
    }
}
