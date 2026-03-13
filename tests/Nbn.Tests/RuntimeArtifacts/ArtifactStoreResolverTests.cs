using Microsoft.Data.Sqlite;
using Nbn.Runtime.Artifacts;
using Nbn.Tests.Format;
using Nbn.Tests.TestSupport;
using System.Text.Json;

namespace Nbn.Tests.Artifacts;

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
}
