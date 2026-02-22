using Microsoft.Data.Sqlite;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.Reproduction;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Tests.Format;
using Proto;
using Repro = Nbn.Proto.Repro;

namespace Nbn.Tests.Reproduction;

public class ReproductionManagerActorTests
{
    [Fact]
    public async Task ReproduceByBrainIds_Returns_ResolutionUnavailable_Report()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor()));

        var response = await root.RequestAsync<Repro.ReproduceResult>(
            manager,
            new Repro.ReproduceByBrainIdsRequest
            {
                ParentA = Guid.NewGuid().ToProtoUuid(),
                ParentB = Guid.NewGuid().ToProtoUuid(),
                Config = new Repro.ReproduceConfig()
            });

        Assert.NotNull(response.Report);
        Assert.False(response.Report.Compatible);
        Assert.Equal("repro_parent_resolution_unavailable", response.Report.AbortReason);
        Assert.False(response.Spawned);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ReproduceByArtifacts_Missing_ParentA_Returns_AbortReport()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor()));

        var response = await root.RequestAsync<Repro.ReproduceResult>(
            manager,
            new Repro.ReproduceByArtifactsRequest());

        Assert.NotNull(response.Report);
        Assert.False(response.Report.Compatible);
        Assert.Equal("repro_missing_parent_a_def", response.Report.AbortReason);
        Assert.False(response.Spawned);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ReproduceByArtifacts_Invalid_ReferenceMediaType_Returns_AbortReport()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-media-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var parentA = NbnTestVectors.CreateMinimalNbn();
            var parentB = NbnTestVectors.CreateMinimalNbn();

            var manifestA = await store.StoreAsync(new MemoryStream(parentA), "application/x-nbn");
            var manifestB = await store.StoreAsync(new MemoryStream(parentB), "application/x-nbn");

            var parentARef = manifestA.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestA.ByteLength, "application/x-nbs", artifactRoot);
            var parentBRef = manifestB.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestB.ByteLength, "application/x-nbn", artifactRoot);

            var system = new ActorSystem();
            var root = system.Root;
            var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor()));

            var response = await root.RequestAsync<Repro.ReproduceResult>(
                manager,
                new Repro.ReproduceByArtifactsRequest
                {
                    ParentADef = parentARef,
                    ParentBDef = parentBRef,
                    Config = new Repro.ReproduceConfig()
                });

            Assert.NotNull(response.Report);
            Assert.False(response.Report.Compatible);
            Assert.Equal("repro_parent_a_media_type_invalid", response.Report.AbortReason);
            Assert.False(response.Spawned);

            await system.ShutdownAsync();
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
    public async Task ReproduceByArtifacts_RegionPresenceMismatch_Returns_AbortReport()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-presence-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var parentA = NbnTestVectors.CreateMinimalNbn();
            var parentB = DemoNbnBuilder.BuildSampleNbn();

            var manifestA = await store.StoreAsync(new MemoryStream(parentA), "application/x-nbn");
            var manifestB = await store.StoreAsync(new MemoryStream(parentB), "application/x-nbn");

            var parentARef = manifestA.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestA.ByteLength, "application/x-nbn", artifactRoot);
            var parentBRef = manifestB.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestB.ByteLength, "application/x-nbn", artifactRoot);

            var system = new ActorSystem();
            var root = system.Root;
            var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor()));

            var response = await root.RequestAsync<Repro.ReproduceResult>(
                manager,
                new Repro.ReproduceByArtifactsRequest
                {
                    ParentADef = parentARef,
                    ParentBDef = parentBRef,
                    Config = new Repro.ReproduceConfig()
                });

            Assert.NotNull(response.Report);
            Assert.False(response.Report.Compatible);
            Assert.Equal("repro_region_presence_mismatch", response.Report.AbortReason);
            Assert.Equal((uint)2, response.Report.RegionsPresentA);
            Assert.Equal((uint)3, response.Report.RegionsPresentB);
            Assert.False(response.Spawned);

            await system.ShutdownAsync();
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
    public async Task ReproduceByArtifacts_Uses_FileUri_And_Returns_StageBPlaceholder_When_StageA_Passes()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-stagea-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var parentA = NbnTestVectors.CreateMinimalNbn();
            var parentB = NbnTestVectors.CreateMinimalNbn();

            var manifestA = await store.StoreAsync(new MemoryStream(parentA), "application/x-nbn");
            var manifestB = await store.StoreAsync(new MemoryStream(parentB), "application/x-nbn");
            var storeUri = new Uri(artifactRoot, UriKind.Absolute).AbsoluteUri;

            var parentARef = manifestA.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestA.ByteLength, "application/x-nbn", storeUri);
            var parentBRef = manifestB.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestB.ByteLength, "application/x-nbn", storeUri);

            var system = new ActorSystem();
            var root = system.Root;
            var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor()));

            var response = await root.RequestAsync<Repro.ReproduceResult>(
                manager,
                new Repro.ReproduceByArtifactsRequest
                {
                    ParentADef = parentARef,
                    ParentBDef = parentBRef,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxRegionSpanDiffRatio = 0f
                    }
                });

            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.Equal("repro_not_implemented:stage_b", response.Report.AbortReason);
            Assert.Equal(1f, response.Report.RegionSpanScore);
            Assert.Equal((uint)2, response.Report.RegionsPresentA);
            Assert.Equal((uint)2, response.Report.RegionsPresentB);
            Assert.False(response.Spawned);

            await system.ShutdownAsync();
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
    public async Task ReproduceByArtifacts_Uses_EnvironmentFallback_When_StoreUri_Missing()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-env-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        var original = Environment.GetEnvironmentVariable("NBN_ARTIFACT_ROOT");
        Environment.SetEnvironmentVariable("NBN_ARTIFACT_ROOT", artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var parentA = NbnTestVectors.CreateMinimalNbn();
            var parentB = NbnTestVectors.CreateMinimalNbn();

            var manifestA = await store.StoreAsync(new MemoryStream(parentA), "application/x-nbn");
            var manifestB = await store.StoreAsync(new MemoryStream(parentB), "application/x-nbn");

            var parentARef = manifestA.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestA.ByteLength, "application/x-nbn");
            var parentBRef = manifestB.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestB.ByteLength, "application/x-nbn");

            var system = new ActorSystem();
            var root = system.Root;
            var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor()));

            var response = await root.RequestAsync<Repro.ReproduceResult>(
                manager,
                new Repro.ReproduceByArtifactsRequest
                {
                    ParentADef = parentARef,
                    ParentBDef = parentBRef,
                    Config = new Repro.ReproduceConfig()
                });

            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.Equal("repro_not_implemented:stage_b", response.Report.AbortReason);
            Assert.False(response.Spawned);

            await system.ShutdownAsync();
        }
        finally
        {
            Environment.SetEnvironmentVariable("NBN_ARTIFACT_ROOT", original);
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }
}
