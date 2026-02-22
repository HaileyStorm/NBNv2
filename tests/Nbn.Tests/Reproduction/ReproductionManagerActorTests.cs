using Microsoft.Data.Sqlite;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.Reproduction;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
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
    public async Task ReproduceByArtifacts_FunctionHistogramMismatch_Returns_AbortReport()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-func-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var sharedRegion31 = new[]
            {
                CreateNeuron(axonCount: 0),
                CreateNeuron(axonCount: 0)
            };
            var baseRegion0 = new[]
            {
                CreateNeuron(axonCount: 1, activationFunctionId: 1),
                CreateNeuron(axonCount: 1, activationFunctionId: 1)
            };
            var changedRegion0 = new[]
            {
                CreateNeuron(axonCount: 1, activationFunctionId: 1),
                CreateNeuron(axonCount: 1, activationFunctionId: 9)
            };
            var region0Axons = new[]
            {
                new AxonRecord(10, 0, 31),
                new AxonRecord(11, 1, 31)
            };

            var parentA = CreateTwoRegionNbn(baseRegion0, region0Axons, sharedRegion31, Array.Empty<AxonRecord>());
            var parentB = CreateTwoRegionNbn(changedRegion0, region0Axons, sharedRegion31, Array.Empty<AxonRecord>());

            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
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
                    Seed = 17,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 0.05f,
                        MaxConnectivityHistDistance = 1f
                    }
                });

            Assert.NotNull(response.Report);
            Assert.False(response.Report.Compatible);
            Assert.Equal("repro_function_hist_mismatch", response.Report.AbortReason);
            Assert.InRange(response.Report.FunctionScore, 0f, 0.95f);
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
    public async Task ReproduceByArtifacts_ConnectivityHistogramMismatch_Returns_AbortReport()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-connect-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var sharedRegion31 = new[]
            {
                CreateNeuron(axonCount: 0),
                CreateNeuron(axonCount: 0)
            };

            var parentARegion0 = new[]
            {
                CreateNeuron(axonCount: 1, activationFunctionId: 1),
                CreateNeuron(axonCount: 1, activationFunctionId: 1)
            };
            var parentARegion0Axons = new[]
            {
                new AxonRecord(10, 0, 31),
                new AxonRecord(11, 1, 31)
            };

            var parentBRegion0 = new[]
            {
                CreateNeuron(axonCount: 2, activationFunctionId: 1),
                CreateNeuron(axonCount: 0, activationFunctionId: 1)
            };
            var parentBRegion0Axons = new[]
            {
                new AxonRecord(10, 0, 31),
                new AxonRecord(11, 1, 31)
            };

            var parentA = CreateTwoRegionNbn(parentARegion0, parentARegion0Axons, sharedRegion31, Array.Empty<AxonRecord>());
            var parentB = CreateTwoRegionNbn(parentBRegion0, parentBRegion0Axons, sharedRegion31, Array.Empty<AxonRecord>());

            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
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
                    Seed = 17,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 0.1f
                    }
                });

            Assert.NotNull(response.Report);
            Assert.False(response.Report.Compatible);
            Assert.Equal("repro_connectivity_hist_mismatch", response.Report.AbortReason);
            Assert.InRange(response.Report.ConnectivityScore, 0f, 0.95f);
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
    public async Task ReproduceByArtifacts_SpotCheckMismatch_Returns_AbortReport()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-spot-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var sharedRegion31 = new[]
            {
                CreateNeuron(axonCount: 0),
                CreateNeuron(axonCount: 0)
            };
            var sharedRegion0 = new[]
            {
                CreateNeuron(axonCount: 1, activationFunctionId: 1),
                CreateNeuron(axonCount: 1, activationFunctionId: 1)
            };

            var parentAAxons = new[]
            {
                new AxonRecord(10, 0, 31),
                new AxonRecord(11, 1, 31)
            };
            var parentBAxons = new[]
            {
                new AxonRecord(10, 1, 31),
                new AxonRecord(11, 0, 31)
            };

            var parentA = CreateTwoRegionNbn(sharedRegion0, parentAAxons, sharedRegion31, Array.Empty<AxonRecord>());
            var parentB = CreateTwoRegionNbn(sharedRegion0, parentBAxons, sharedRegion31, Array.Empty<AxonRecord>());

            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
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
                    Seed = 111,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 0.4f
                    }
                });

            Assert.NotNull(response.Report);
            Assert.False(response.Report.Compatible);
            Assert.Equal("repro_spot_check_overlap_mismatch", response.Report.AbortReason);
            Assert.InRange(response.Report.ConnectivityScore, 0.99f, 1f);
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
    public async Task ReproduceByArtifacts_Uses_FileUri_And_Persists_ChildArtifact_When_Gates_Pass()
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
                    Seed = 111,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxRegionSpanDiffRatio = 0f
                    }
                });

            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.Equal(string.Empty, response.Report.AbortReason);
            Assert.Equal(1f, response.Report.RegionSpanScore);
            Assert.Equal(1f, response.Report.FunctionScore);
            Assert.Equal(1f, response.Report.ConnectivityScore);
            Assert.Equal((uint)2, response.Report.RegionsPresentA);
            Assert.Equal((uint)2, response.Report.RegionsPresentB);
            Assert.NotNull(response.ChildDef);
            Assert.True(response.ChildDef.TryToSha256Bytes(out var childHash));
            var childStream = await store.TryOpenArtifactAsync(new Sha256Hash(childHash));
            Assert.NotNull(childStream);
            await using (childStream!)
            {
            }
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
                    Seed = 111,
                    Config = new Repro.ReproduceConfig()
                });

            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.Equal(string.Empty, response.Report.AbortReason);
            Assert.NotNull(response.ChildDef);
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

    [Fact]
    public async Task ReproduceByArtifacts_WithSameSeed_ProducesSameChildArtifact()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-seed-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var sharedRegion31 = new[]
            {
                CreateNeuron(axonCount: 0),
                CreateNeuron(axonCount: 0)
            };
            var parentARegion0 = new[]
            {
                CreateNeuron(axonCount: 1, activationFunctionId: 1),
                CreateNeuron(axonCount: 1, activationFunctionId: 1)
            };
            var parentBRegion0 = new[]
            {
                CreateNeuron(axonCount: 1, activationFunctionId: 9),
                CreateNeuron(axonCount: 1, activationFunctionId: 9)
            };
            var parentAAxons = new[]
            {
                new AxonRecord(10, 0, 31),
                new AxonRecord(11, 1, 31)
            };
            var parentBAxons = new[]
            {
                new AxonRecord(12, 1, 31),
                new AxonRecord(13, 0, 31)
            };

            var parentA = CreateTwoRegionNbn(parentARegion0, parentAAxons, sharedRegion31, Array.Empty<AxonRecord>());
            var parentB = CreateTwoRegionNbn(parentBRegion0, parentBAxons, sharedRegion31, Array.Empty<AxonRecord>());

            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var manifestA = await store.StoreAsync(new MemoryStream(parentA), "application/x-nbn");
            var manifestB = await store.StoreAsync(new MemoryStream(parentB), "application/x-nbn");
            var parentARef = manifestA.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestA.ByteLength, "application/x-nbn", artifactRoot);
            var parentBRef = manifestB.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestB.ByteLength, "application/x-nbn", artifactRoot);

            var system = new ActorSystem();
            var root = system.Root;
            var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor()));

            var request = new Repro.ReproduceByArtifactsRequest
            {
                ParentADef = parentARef,
                ParentBDef = parentBRef,
                Seed = 424242,
                Config = new Repro.ReproduceConfig
                {
                    MaxFunctionHistDistance = 1f,
                    MaxConnectivityHistDistance = 1f,
                    ProbChooseParentA = 0.5f,
                    ProbChooseParentB = 0.5f
                }
            };

            var first = await root.RequestAsync<Repro.ReproduceResult>(manager, request);
            var second = await root.RequestAsync<Repro.ReproduceResult>(manager, request);

            Assert.NotNull(first.Report);
            Assert.NotNull(second.Report);
            Assert.True(first.Report.Compatible);
            Assert.True(second.Report.Compatible);
            Assert.NotNull(first.ChildDef);
            Assert.NotNull(second.ChildDef);
            Assert.True(first.ChildDef.TryToSha256Hex(out var firstHash));
            Assert.True(second.ChildDef.TryToSha256Hex(out var secondHash));
            Assert.Equal(firstHash, secondHash);

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

    private static NeuronRecord CreateNeuron(
        ushort axonCount,
        byte activationFunctionId = 1,
        byte resetFunctionId = 0,
        byte accumulationFunctionId = 0)
        => new(
            axonCount: axonCount,
            paramBCode: 0,
            paramACode: 0,
            activationThresholdCode: 0,
            preActivationThresholdCode: 0,
            resetFunctionId: resetFunctionId,
            activationFunctionId: activationFunctionId,
            accumulationFunctionId: accumulationFunctionId,
            exists: true);

    private static byte[] CreateTwoRegionNbn(
        NeuronRecord[] region0Neurons,
        AxonRecord[] region0Axons,
        NeuronRecord[] region31Neurons,
        AxonRecord[] region31Axons)
    {
        var stride = (uint)NbnConstants.DefaultAxonStride;
        var sections = new List<NbnRegionSection>
        {
            CreateRegionSection(0, stride, region0Neurons, region0Axons),
            CreateRegionSection(31, stride, region31Neurons, region31Axons)
        };

        return BuildNbn(sections, stride, NbnTestVectors.SampleBrainSeed);
    }

    private static NbnRegionSection CreateRegionSection(
        int regionId,
        uint stride,
        NeuronRecord[] neurons,
        AxonRecord[] axons)
    {
        ulong expectedAxons = 0;
        for (var i = 0; i < neurons.Length; i++)
        {
            expectedAxons += neurons[i].AxonCount;
        }

        if (expectedAxons != (ulong)axons.Length)
        {
            throw new InvalidOperationException($"Region {regionId} axon count mismatch.");
        }

        var clonedNeurons = (NeuronRecord[])neurons.Clone();
        var clonedAxons = (AxonRecord[])axons.Clone();
        var checkpoints = NbnBinary.BuildCheckpoints(clonedNeurons, stride);

        return new NbnRegionSection(
            (byte)regionId,
            (uint)clonedNeurons.Length,
            (ulong)clonedAxons.Length,
            stride,
            (uint)checkpoints.Length,
            checkpoints,
            clonedNeurons,
            clonedAxons);
    }

    private static byte[] BuildNbn(IReadOnlyList<NbnRegionSection> sections, uint stride, ulong brainSeed)
    {
        var orderedSections = sections.OrderBy(static section => section.RegionId).ToList();
        var directory = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        ulong offset = NbnBinary.NbnHeaderBytes;
        for (var i = 0; i < orderedSections.Count; i++)
        {
            var section = orderedSections[i];
            directory[section.RegionId] = new NbnRegionDirectoryEntry(
                section.NeuronSpan,
                section.TotalAxons,
                offset,
                0);
            offset += (ulong)section.ByteLength;
        }

        var header = new NbnHeaderV2(
            magic: "NBN2",
            version: 2,
            endianness: 1,
            headerBytesPow2: 10,
            brainSeed: brainSeed,
            axonStride: stride,
            flags: 0,
            quantization: QuantizationSchemas.DefaultNbn,
            regions: directory);

        return NbnBinary.WriteNbn(header, orderedSections);
    }
}
