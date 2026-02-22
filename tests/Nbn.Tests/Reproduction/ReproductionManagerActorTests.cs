using Microsoft.Data.Sqlite;
using Nbn.Proto;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.Reproduction;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
using Nbn.Shared.Validation;
using Nbn.Tests.Format;
using Proto;
using ProtoControl = Nbn.Proto.Control;
using ProtoIo = Nbn.Proto.Io;
using Repro = Nbn.Proto.Repro;
using SharedAddress32 = Nbn.Shared.Addressing.Address32;

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
    public async Task ReproduceByBrainIds_Resolves_Parents_FromIoGateway()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-brainid-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var parentABytes = NbnTestVectors.CreateMinimalNbn();
            var parentBBytes = NbnTestVectors.CreateMinimalNbn();
            var manifestA = await store.StoreAsync(new MemoryStream(parentABytes), "application/x-nbn");
            var manifestB = await store.StoreAsync(new MemoryStream(parentBBytes), "application/x-nbn");

            var parentABrainId = Guid.NewGuid();
            var parentBBrainId = Guid.NewGuid();
            var parentARef = manifestA.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestA.ByteLength, "application/x-nbn", artifactRoot);
            var parentBRef = manifestB.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestB.ByteLength, "application/x-nbn", artifactRoot);

            var brainInfo = new Dictionary<Guid, ProtoIo.BrainInfo>
            {
                [parentABrainId] = new ProtoIo.BrainInfo
                {
                    BrainId = parentABrainId.ToProtoUuid(),
                    InputWidth = 1,
                    OutputWidth = 1,
                    BaseDefinition = parentARef,
                    LastSnapshot = new ArtifactRef()
                },
                [parentBBrainId] = new ProtoIo.BrainInfo
                {
                    BrainId = parentBBrainId.ToProtoUuid(),
                    InputWidth = 1,
                    OutputWidth = 1,
                    BaseDefinition = parentBRef,
                    LastSnapshot = new ArtifactRef()
                }
            };

            var system = new ActorSystem();
            var root = system.Root;
            var ioProbe = root.Spawn(Props.FromProducer(() => new ReproIoGatewayProbe(brainInfo)));
            var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor(ioProbe)));

            var response = await root.RequestAsync<Repro.ReproduceResult>(
                manager,
                new Repro.ReproduceByBrainIdsRequest
                {
                    ParentA = parentABrainId.ToProtoUuid(),
                    ParentB = parentBBrainId.ToProtoUuid(),
                    Seed = 19,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxRegionSpanDiffRatio = 0f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
                    }
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
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task ReproduceByBrainIds_StrengthLiveCodes_Uses_ResolvedParentSnapshots()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-brainid-live-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var region0 = new[] { CreateNeuron(axonCount: 1) };
            var region31 = new[] { CreateNeuron(axonCount: 0) };
            var parentAAxons = new[] { new AxonRecord(10, 0, 31) };
            var parentBAxons = new[] { new AxonRecord(7, 0, 31) };
            var parentA = CreateTwoRegionNbn(region0, parentAAxons, region31, Array.Empty<AxonRecord>());
            var parentB = CreateTwoRegionNbn(region0, parentBAxons, region31, Array.Empty<AxonRecord>());
            var parentAOverlay = new[]
            {
                new NbsOverlayRecord(
                    SharedAddress32.From(0, 0).Value,
                    SharedAddress32.From(31, 0).Value,
                    22)
            };
            var parentAState = CreateSnapshotWithOverlays(parentA, parentAOverlay);

            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var manifestA = await store.StoreAsync(new MemoryStream(parentA), "application/x-nbn");
            var manifestB = await store.StoreAsync(new MemoryStream(parentB), "application/x-nbn");
            var manifestAState = await store.StoreAsync(new MemoryStream(parentAState), "application/x-nbs");

            var parentABrainId = Guid.NewGuid();
            var parentBBrainId = Guid.NewGuid();
            var parentARef = manifestA.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestA.ByteLength, "application/x-nbn", artifactRoot);
            var parentBRef = manifestB.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestB.ByteLength, "application/x-nbn", artifactRoot);
            var parentAStateRef = manifestAState.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestAState.ByteLength, "application/x-nbs", artifactRoot);

            var brainInfo = new Dictionary<Guid, ProtoIo.BrainInfo>
            {
                [parentABrainId] = new ProtoIo.BrainInfo
                {
                    BrainId = parentABrainId.ToProtoUuid(),
                    InputWidth = 1,
                    OutputWidth = 1,
                    BaseDefinition = parentARef,
                    LastSnapshot = parentAStateRef
                },
                [parentBBrainId] = new ProtoIo.BrainInfo
                {
                    BrainId = parentBBrainId.ToProtoUuid(),
                    InputWidth = 1,
                    OutputWidth = 1,
                    BaseDefinition = parentBRef,
                    LastSnapshot = new ArtifactRef()
                }
            };

            var system = new ActorSystem();
            var root = system.Root;
            var ioProbe = root.Spawn(Props.FromProducer(() => new ReproIoGatewayProbe(brainInfo)));
            var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor(ioProbe)));

            var response = await root.RequestAsync<Repro.ReproduceResult>(
                manager,
                new Repro.ReproduceByBrainIdsRequest
                {
                    ParentA = parentABrainId.ToProtoUuid(),
                    ParentB = parentBBrainId.ToProtoUuid(),
                    StrengthSource = Repro.StrengthSource.StrengthLiveCodes,
                    Seed = 19,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        ProbChooseParentA = 1f,
                        ProbChooseParentB = 0f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
                    }
                });

            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.NotNull(response.ChildDef);
            var childBytes = await ReadArtifactBytesAsync(store, response.ChildDef!);
            Assert.Equal((byte)22, ReadAxonStrengthCode(childBytes, 0, 0, 0));
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
    public async Task ReproduceByArtifacts_StrengthLiveCodes_Applies_StateOverlays()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-live-overlay-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var region0 = new[] { CreateNeuron(axonCount: 1) };
            var region31 = new[] { CreateNeuron(axonCount: 0) };
            var parentAAxons = new[] { new AxonRecord(10, 0, 31) };
            var parentBAxons = new[] { new AxonRecord(7, 0, 31) };
            var parentA = CreateTwoRegionNbn(region0, parentAAxons, region31, Array.Empty<AxonRecord>());
            var parentB = CreateTwoRegionNbn(region0, parentBAxons, region31, Array.Empty<AxonRecord>());
            var parentAOverlay = new[]
            {
                new NbsOverlayRecord(
                    SharedAddress32.From(0, 0).Value,
                    SharedAddress32.From(31, 0).Value,
                    22)
            };
            var parentAState = CreateSnapshotWithOverlays(parentA, parentAOverlay);

            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var manifestA = await store.StoreAsync(new MemoryStream(parentA), "application/x-nbn");
            var manifestB = await store.StoreAsync(new MemoryStream(parentB), "application/x-nbn");
            var manifestAState = await store.StoreAsync(new MemoryStream(parentAState), "application/x-nbs");
            var parentARef = manifestA.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestA.ByteLength, "application/x-nbn", artifactRoot);
            var parentBRef = manifestB.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestB.ByteLength, "application/x-nbn", artifactRoot);
            var parentAStateRef = manifestAState.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestAState.ByteLength, "application/x-nbs", artifactRoot);

            var system = new ActorSystem();
            var root = system.Root;
            var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor()));

            var baseResponse = await root.RequestAsync<Repro.ReproduceResult>(
                manager,
                new Repro.ReproduceByArtifactsRequest
                {
                    ParentADef = parentARef,
                    ParentBDef = parentBRef,
                    ParentAState = parentAStateRef,
                    StrengthSource = Repro.StrengthSource.StrengthBaseOnly,
                    Seed = 19,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        ProbChooseParentA = 1f,
                        ProbChooseParentB = 0f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
                    }
                });

            var liveResponse = await root.RequestAsync<Repro.ReproduceResult>(
                manager,
                new Repro.ReproduceByArtifactsRequest
                {
                    ParentADef = parentARef,
                    ParentBDef = parentBRef,
                    ParentAState = parentAStateRef,
                    StrengthSource = Repro.StrengthSource.StrengthLiveCodes,
                    Seed = 19,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        ProbChooseParentA = 1f,
                        ProbChooseParentB = 0f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
                    }
                });

            Assert.NotNull(baseResponse.Report);
            Assert.True(baseResponse.Report.Compatible);
            Assert.NotNull(baseResponse.ChildDef);
            Assert.NotNull(liveResponse.Report);
            Assert.True(liveResponse.Report.Compatible);
            Assert.NotNull(liveResponse.ChildDef);

            var baseBytes = await ReadArtifactBytesAsync(store, baseResponse.ChildDef!);
            var liveBytes = await ReadArtifactBytesAsync(store, liveResponse.ChildDef!);
            Assert.Equal((byte)10, ReadAxonStrengthCode(baseBytes, 0, 0, 0));
            Assert.Equal((byte)22, ReadAxonStrengthCode(liveBytes, 0, 0, 0));
            Assert.False(liveResponse.Spawned);

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
    public async Task ReproduceByArtifacts_StrengthLiveCodes_InvalidState_FallsBack_To_BaseStrengths()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-live-invalid-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var region0 = new[] { CreateNeuron(axonCount: 1) };
            var region31 = new[] { CreateNeuron(axonCount: 0) };
            var parentAAxons = new[] { new AxonRecord(10, 0, 31) };
            var parentBAxons = new[] { new AxonRecord(7, 0, 31) };
            var parentA = CreateTwoRegionNbn(region0, parentAAxons, region31, Array.Empty<AxonRecord>());
            var parentB = CreateTwoRegionNbn(region0, parentBAxons, region31, Array.Empty<AxonRecord>());
            var parentAOverlay = new[]
            {
                new NbsOverlayRecord(
                    SharedAddress32.From(0, 0).Value,
                    SharedAddress32.From(31, 0).Value,
                    22)
            };
            var wrongBaseHash = Enumerable.Repeat((byte)1, Sha256Hash.Length).ToArray();
            var invalidParentAState = CreateSnapshotWithOverlays(parentA, parentAOverlay, wrongBaseHash);

            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var manifestA = await store.StoreAsync(new MemoryStream(parentA), "application/x-nbn");
            var manifestB = await store.StoreAsync(new MemoryStream(parentB), "application/x-nbn");
            var invalidStateManifest = await store.StoreAsync(new MemoryStream(invalidParentAState), "application/x-nbs");
            var parentARef = manifestA.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestA.ByteLength, "application/x-nbn", artifactRoot);
            var parentBRef = manifestB.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestB.ByteLength, "application/x-nbn", artifactRoot);
            var invalidStateRef = invalidStateManifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)invalidStateManifest.ByteLength, "application/x-nbs", artifactRoot);

            var system = new ActorSystem();
            var root = system.Root;
            var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor()));

            var baseResponse = await root.RequestAsync<Repro.ReproduceResult>(
                manager,
                new Repro.ReproduceByArtifactsRequest
                {
                    ParentADef = parentARef,
                    ParentBDef = parentBRef,
                    StrengthSource = Repro.StrengthSource.StrengthBaseOnly,
                    Seed = 91,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        ProbChooseParentA = 1f,
                        ProbChooseParentB = 0f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
                    }
                });

            var liveResponse = await root.RequestAsync<Repro.ReproduceResult>(
                manager,
                new Repro.ReproduceByArtifactsRequest
                {
                    ParentADef = parentARef,
                    ParentBDef = parentBRef,
                    ParentAState = invalidStateRef,
                    StrengthSource = Repro.StrengthSource.StrengthLiveCodes,
                    Seed = 91,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        ProbChooseParentA = 1f,
                        ProbChooseParentB = 0f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
                    }
                });

            Assert.NotNull(baseResponse.Report);
            Assert.True(baseResponse.Report.Compatible);
            Assert.NotNull(baseResponse.ChildDef);
            Assert.NotNull(liveResponse.Report);
            Assert.True(liveResponse.Report.Compatible);
            Assert.NotNull(liveResponse.ChildDef);

            var baseBytes = await ReadArtifactBytesAsync(store, baseResponse.ChildDef!);
            var liveBytes = await ReadArtifactBytesAsync(store, liveResponse.ChildDef!);
            Assert.Equal((byte)10, ReadAxonStrengthCode(baseBytes, 0, 0, 0));
            Assert.Equal((byte)10, ReadAxonStrengthCode(liveBytes, 0, 0, 0));
            Assert.False(liveResponse.Spawned);

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
    public async Task ReproduceByArtifacts_Missing_ParentB_Returns_AbortReport()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-missing-parent-b-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var parentA = NbnTestVectors.CreateMinimalNbn();
            var manifestA = await store.StoreAsync(new MemoryStream(parentA), "application/x-nbn");
            var parentARef = manifestA.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestA.ByteLength, "application/x-nbn", artifactRoot);

            var system = new ActorSystem();
            var root = system.Root;
            var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor()));

            var response = await root.RequestAsync<Repro.ReproduceResult>(
                manager,
                new Repro.ReproduceByArtifactsRequest
                {
                    ParentADef = parentARef
                });

            Assert.NotNull(response.Report);
            Assert.False(response.Report.Compatible);
            Assert.Equal("repro_missing_parent_b_def", response.Report.AbortReason);
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
    public async Task ReproduceByArtifacts_RegionSpanMismatch_Returns_AbortReport()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-span-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var sharedRegion31 = new[]
            {
                CreateNeuron(axonCount: 0)
            };
            var region0A = new[]
            {
                CreateNeuron(axonCount: 1, activationFunctionId: 1)
            };
            var region0B = new[]
            {
                CreateNeuron(axonCount: 1, activationFunctionId: 1),
                CreateNeuron(axonCount: 1, activationFunctionId: 1)
            };
            var region0AxonsA = new[]
            {
                new AxonRecord(10, 0, 31)
            };
            var region0AxonsB = new[]
            {
                new AxonRecord(10, 0, 31),
                new AxonRecord(11, 0, 31)
            };

            var parentA = CreateTwoRegionNbn(region0A, region0AxonsA, sharedRegion31, Array.Empty<AxonRecord>());
            var parentB = CreateTwoRegionNbn(region0B, region0AxonsB, sharedRegion31, Array.Empty<AxonRecord>());

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
                    Seed = 23,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxRegionSpanDiffRatio = 0f,
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
                    }
                });

            Assert.NotNull(response.Report);
            Assert.False(response.Report.Compatible);
            Assert.Equal("repro_region_span_mismatch", response.Report.AbortReason);
            Assert.InRange(response.Report.RegionSpanScore, 0f, 0.99f);
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
                        MaxRegionSpanDiffRatio = 0f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
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
                    Config = new Repro.ReproduceConfig
                    {
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
                    }
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
                    ProbChooseParentB = 0.5f,
                    SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
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

    [Fact]
    public async Task ReproduceByArtifacts_WithDifferentSeeds_ProducesDivergentChildArtifacts_WhileRemainingDeterministicPerSeed()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-seed-variation-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var sharedRegion31 = new[]
            {
                CreateNeuron(axonCount: 0),
                CreateNeuron(axonCount: 0),
                CreateNeuron(axonCount: 0),
                CreateNeuron(axonCount: 0)
            };
            var parentARegion0 = new[]
            {
                CreateNeuron(axonCount: 1, activationFunctionId: 1),
                CreateNeuron(axonCount: 1, activationFunctionId: 1),
                CreateNeuron(axonCount: 1, activationFunctionId: 1),
                CreateNeuron(axonCount: 1, activationFunctionId: 1)
            };
            var parentBRegion0 = new[]
            {
                CreateNeuron(axonCount: 1, activationFunctionId: 9),
                CreateNeuron(axonCount: 1, activationFunctionId: 9),
                CreateNeuron(axonCount: 1, activationFunctionId: 9),
                CreateNeuron(axonCount: 1, activationFunctionId: 9)
            };
            var parentAAxons = new[]
            {
                new AxonRecord(10, 0, 31),
                new AxonRecord(11, 1, 31),
                new AxonRecord(12, 2, 31),
                new AxonRecord(13, 3, 31)
            };
            var parentBAxons = new[]
            {
                new AxonRecord(14, 3, 31),
                new AxonRecord(15, 2, 31),
                new AxonRecord(16, 1, 31),
                new AxonRecord(17, 0, 31)
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
            var uniqueHashes = new HashSet<string>(StringComparer.Ordinal);
            var seeds = new ulong[] { 1001, 1002, 1003, 1004, 1005 };

            foreach (var seed in seeds)
            {
                var request = new Repro.ReproduceByArtifactsRequest
                {
                    ParentADef = parentARef,
                    ParentBDef = parentBRef,
                    Seed = seed,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        ProbChooseParentA = 0.5f,
                        ProbChooseParentB = 0.5f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
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
                uniqueHashes.Add(firstHash);
            }

            Assert.True(uniqueHashes.Count > 1, "Expected at least two distinct child artifacts across different seeds.");

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
    public async Task ReproduceByArtifacts_DefaultSpawnPolicy_SpawnsChild_WhenIoGatewayAvailable()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-spawn-default-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var parentA = NbnTestVectors.CreateMinimalNbn();
            var parentB = NbnTestVectors.CreateMinimalNbn();
            var manifestA = await store.StoreAsync(new MemoryStream(parentA), "application/x-nbn");
            var manifestB = await store.StoreAsync(new MemoryStream(parentB), "application/x-nbn");
            var parentARef = manifestA.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestA.ByteLength, "application/x-nbn", artifactRoot);
            var parentBRef = manifestB.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestB.ByteLength, "application/x-nbn", artifactRoot);
            var spawnedBrainId = Guid.NewGuid();

            var system = new ActorSystem();
            var root = system.Root;
            var ioProbe = root.Spawn(Props.FromProducer(() => new ReproIoGatewayProbe(spawnBrainId: spawnedBrainId)));
            var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor(ioProbe)));

            var response = await root.RequestAsync<Repro.ReproduceResult>(
                manager,
                new Repro.ReproduceByArtifactsRequest
                {
                    ParentADef = parentARef,
                    ParentBDef = parentBRef,
                    Seed = 9,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxRegionSpanDiffRatio = 0f
                    }
                });

            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.True(response.Spawned);
            Assert.NotNull(response.ChildDef);
            Assert.NotNull(response.ChildBrainId);
            Assert.True(response.ChildBrainId.TryToGuid(out var actualBrainId));
            Assert.Equal(spawnedBrainId, actualBrainId);

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
    public async Task ReproduceByArtifacts_SpawnAlways_WithoutIoGateway_Returns_SpawnAbort_And_ChildArtifact()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-spawn-always-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var parentA = NbnTestVectors.CreateMinimalNbn();
            var parentB = NbnTestVectors.CreateMinimalNbn();
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
                    Seed = 10,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxRegionSpanDiffRatio = 0f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildAlways
                    }
                });

            Assert.NotNull(response.Report);
            Assert.False(response.Report.Compatible);
            Assert.Equal("repro_spawn_unavailable", response.Report.AbortReason);
            Assert.NotNull(response.ChildDef);
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
    public async Task ReproduceByArtifacts_MutationLimits_Are_Respected_And_Child_Remains_Valid()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-limits-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var parentA = DemoNbnBuilder.BuildSampleNbn();
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
                    Seed = 4242,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        ProbChooseParentA = 1f,
                        ProbChooseParentB = 0f,
                        ProbAddNeuronToEmptyRegion = 1f,
                        ProbRemoveLastNeuronFromRegion = 1f,
                        ProbDisableNeuron = 1f,
                        ProbReactivateNeuron = 1f,
                        ProbAddAxon = 1f,
                        ProbRemoveAxon = 1f,
                        ProbRerouteAxon = 1f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever,
                        Limits = new Repro.ReproduceLimits
                        {
                            MaxNeuronsAddedAbs = 1,
                            MaxNeuronsRemovedAbs = 1,
                            MaxAxonsAddedAbs = 1,
                            MaxAxonsRemovedAbs = 1,
                            MaxRegionsAddedAbs = 1,
                            MaxRegionsRemovedAbs = 1
                        }
                    }
                });

            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.NotNull(response.Summary);
            Assert.InRange((int)response.Summary.NeuronsAdded, 0, 1);
            Assert.InRange((int)response.Summary.NeuronsRemoved, 0, 1);
            Assert.InRange((int)response.Summary.AxonsAdded, 0, 1);
            Assert.InRange((int)response.Summary.AxonsRemoved, 0, 1);
            Assert.NotNull(response.ChildDef);
            Assert.True(response.ChildDef.TryToSha256Bytes(out var childHash));

            await using var childStream = await store.TryOpenArtifactAsync(new Sha256Hash(childHash));
            Assert.NotNull(childStream);
            using var ms = new MemoryStream();
            await childStream!.CopyToAsync(ms);
            var childBytes = ms.ToArray();
            var childHeader = NbnBinary.ReadNbnHeader(childBytes);
            var childSections = ReadSections(childBytes, childHeader);
            var validation = NbnBinaryValidator.ValidateNbn(childHeader, childSections);
            Assert.True(validation.IsValid, string.Join(" | ", validation.Issues.Select(issue => issue.Message)));

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
    public async Task ReproduceByArtifacts_AdditionLimits_Use_Smaller_Of_Absolute_And_Percentage()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-limit-effective-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var parentA = DemoNbnBuilder.BuildSampleNbn();
            var parentB = DemoNbnBuilder.BuildSampleNbn();
            var parentHeader = NbnBinary.ReadNbnHeader(parentA);
            var parentSections = ReadSections(parentA, parentHeader);
            var baselineNeurons = parentSections.Sum(section => section.NeuronRecords.Count(neuron => neuron.Exists));
            var baselineAxons = parentSections.Sum(section => section.NeuronRecords.Where(neuron => neuron.Exists).Sum(neuron => neuron.AxonCount));
            Assert.True(baselineNeurons > 0);
            Assert.True(baselineAxons > 0);

            var neuronPctLimit = 1.9f / baselineNeurons;
            var axonPctLimit = 1.9f / baselineAxons;
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
                    Seed = 9191,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        ProbChooseParentA = 0.5f,
                        ProbChooseParentB = 0.5f,
                        ProbAddNeuronToEmptyRegion = 1f,
                        ProbAddAxon = 1f,
                        ProbRemoveAxon = 0f,
                        ProbRerouteAxon = 0f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever,
                        Limits = new Repro.ReproduceLimits
                        {
                            MaxNeuronsAddedAbs = 12,
                            MaxNeuronsAddedPct = neuronPctLimit,
                            MaxAxonsAddedAbs = 12,
                            MaxAxonsAddedPct = axonPctLimit
                        }
                    }
                });

            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.NotNull(response.Summary);
            Assert.InRange((int)response.Summary.NeuronsAdded, 0, 1);
            Assert.InRange((int)response.Summary.AxonsAdded, 0, 1);
            Assert.NotNull(response.ChildDef);

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
    public async Task ReproduceByArtifacts_MutationAcrossSeeds_Preserves_IoAndDuplicateInvariants()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-invariants-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var parentA = DemoNbnBuilder.BuildSampleNbn();
            var parentB = DemoNbnBuilder.BuildSampleNbn();
            var manifestA = await store.StoreAsync(new MemoryStream(parentA), "application/x-nbn");
            var manifestB = await store.StoreAsync(new MemoryStream(parentB), "application/x-nbn");
            var parentARef = manifestA.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestA.ByteLength, "application/x-nbn", artifactRoot);
            var parentBRef = manifestB.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestB.ByteLength, "application/x-nbn", artifactRoot);

            var system = new ActorSystem();
            var root = system.Root;
            var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor()));

            for (ulong seed = 101; seed <= 105; seed++)
            {
                var response = await root.RequestAsync<Repro.ReproduceResult>(
                    manager,
                    new Repro.ReproduceByArtifactsRequest
                    {
                        ParentADef = parentARef,
                        ParentBDef = parentBRef,
                        Seed = seed,
                        Config = new Repro.ReproduceConfig
                        {
                            MaxFunctionHistDistance = 1f,
                            MaxConnectivityHistDistance = 1f,
                            ProbChooseParentA = 0.5f,
                            ProbChooseParentB = 0.5f,
                            ProbAddNeuronToEmptyRegion = 1f,
                            ProbRemoveLastNeuronFromRegion = 1f,
                            ProbDisableNeuron = 1f,
                            ProbReactivateNeuron = 1f,
                            ProbAddAxon = 1f,
                            ProbRemoveAxon = 1f,
                            ProbRerouteAxon = 1f,
                            SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever,
                            Limits = new Repro.ReproduceLimits
                            {
                                MaxNeuronsAddedAbs = 8,
                                MaxNeuronsRemovedAbs = 8,
                                MaxAxonsAddedAbs = 12,
                                MaxAxonsRemovedAbs = 12,
                                MaxRegionsAddedAbs = 4,
                                MaxRegionsRemovedAbs = 4
                            }
                        }
                    });

                Assert.NotNull(response.Report);
                Assert.True(response.Report.Compatible, response.Report.AbortReason);
                Assert.NotNull(response.ChildDef);
                var childBytes = await ReadArtifactBytesAsync(store, response.ChildDef);
                var childHeader = NbnBinary.ReadNbnHeader(childBytes);
                var childSections = ReadSections(childBytes, childHeader);
                var validation = NbnBinaryValidator.ValidateNbn(childHeader, childSections);
                Assert.True(validation.IsValid, string.Join(" | ", validation.Issues.Select(issue => issue.Message)));
                AssertIoAndDuplicateInvariants(childSections);
            }

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

    private static byte[] CreateSnapshotWithOverlays(
        byte[] baseNbn,
        IReadOnlyList<NbsOverlayRecord> overlays,
        byte[]? baseHashOverride = null)
    {
        var header = NbnBinary.ReadNbnHeader(baseNbn);
        var regions = new List<NbsRegionSection>(NbnConstants.RegionCount);
        for (var regionId = 0; regionId < header.Regions.Length; regionId++)
        {
            var entry = header.Regions[regionId];
            if (entry.NeuronSpan == 0)
            {
                continue;
            }

            regions.Add(new NbsRegionSection(
                (byte)regionId,
                entry.NeuronSpan,
                new short[checked((int)entry.NeuronSpan)],
                enabledBitset: null));
        }

        var baseHash = baseHashOverride ?? Sha256Hash.Compute(baseNbn).Bytes.ToArray();
        var nbsHeader = new NbsHeaderV2(
            magic: "NBS2",
            version: 2,
            endianness: 1,
            headerBytesPow2: 9,
            brainId: Guid.NewGuid(),
            snapshotTickId: 1,
            timestampMs: 1,
            energyRemaining: 0,
            baseNbnSha256: baseHash,
            flags: 0x2u,
            bufferMap: QuantizationSchemas.DefaultBuffer);

        return NbnBinary.WriteNbs(nbsHeader, regions, overlays);
    }

    private static async Task<byte[]> ReadArtifactBytesAsync(LocalArtifactStore store, ArtifactRef reference)
    {
        Assert.True(reference.TryToSha256Bytes(out var hashBytes));
        await using var stream = await store.TryOpenArtifactAsync(new Sha256Hash(hashBytes));
        Assert.NotNull(stream);
        using var ms = new MemoryStream();
        await stream!.CopyToAsync(ms);
        return ms.ToArray();
    }

    private static byte ReadAxonStrengthCode(byte[] nbnBytes, int regionId, int neuronId, int axonOffset)
    {
        var header = NbnBinary.ReadNbnHeader(nbnBytes);
        var entry = header.Regions[regionId];
        var section = NbnBinary.ReadNbnRegionSection(nbnBytes, entry.Offset);

        var start = 0;
        for (var i = 0; i < neuronId; i++)
        {
            start += section.NeuronRecords[i].AxonCount;
        }

        return section.AxonRecords[start + axonOffset].StrengthCode;
    }

    private static void AssertIoAndDuplicateInvariants(IReadOnlyList<NbnRegionSection> sections)
    {
        foreach (var section in sections)
        {
            var axonStart = 0;
            for (var neuronId = 0; neuronId < section.NeuronRecords.Length; neuronId++)
            {
                var neuron = section.NeuronRecords[neuronId];
                var seenTargets = new HashSet<uint>();
                for (var i = 0; i < neuron.AxonCount; i++)
                {
                    var axon = section.AxonRecords[axonStart + i];
                    Assert.NotEqual(0, (int)axon.TargetRegionId);
                    if (section.RegionId == NbnConstants.OutputRegionId)
                    {
                        Assert.NotEqual(NbnConstants.OutputRegionId, (int)axon.TargetRegionId);
                    }

                    var targetKey = ((uint)axon.TargetRegionId << 22) | (uint)axon.TargetNeuronId;
                    Assert.True(
                        seenTargets.Add(targetKey),
                        $"Duplicate axon target for region={section.RegionId}, neuron={neuronId}, target={axon.TargetRegionId}:{axon.TargetNeuronId}.");
                }

                axonStart += neuron.AxonCount;
            }
        }
    }

    private sealed class ReproIoGatewayProbe : IActor
    {
        private readonly IReadOnlyDictionary<Guid, ProtoIo.BrainInfo> _brainInfo;
        private readonly Guid _spawnBrainId;

        public ReproIoGatewayProbe(
            IReadOnlyDictionary<Guid, ProtoIo.BrainInfo>? brainInfo = null,
            Guid? spawnBrainId = null)
        {
            _brainInfo = brainInfo ?? new Dictionary<Guid, ProtoIo.BrainInfo>();
            _spawnBrainId = spawnBrainId ?? Guid.Empty;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case ProtoIo.BrainInfoRequest request:
                    if (request.BrainId is not null
                        && request.BrainId.TryToGuid(out var brainId)
                        && _brainInfo.TryGetValue(brainId, out var info))
                    {
                        context.Respond(info);
                    }
                    else
                    {
                        context.Respond(new ProtoIo.BrainInfo
                        {
                            BrainId = request.BrainId ?? new Uuid(),
                            InputWidth = 0,
                            OutputWidth = 0,
                            BaseDefinition = new ArtifactRef(),
                            LastSnapshot = new ArtifactRef()
                        });
                    }

                    break;
                case ProtoIo.SpawnBrainViaIO:
                    context.Respond(new ProtoIo.SpawnBrainViaIOAck
                    {
                        Ack = new ProtoControl.SpawnBrainAck
                        {
                            BrainId = _spawnBrainId.ToProtoUuid()
                        }
                    });
                    break;
            }

            return Task.CompletedTask;
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

    private static List<NbnRegionSection> ReadSections(ReadOnlySpan<byte> nbnBytes, NbnHeaderV2 header)
    {
        var sections = new List<NbnRegionSection>(NbnConstants.RegionCount);
        for (var regionId = 0; regionId < header.Regions.Length; regionId++)
        {
            var entry = header.Regions[regionId];
            if (entry.NeuronSpan == 0 || entry.Offset == 0)
            {
                continue;
            }

            sections.Add(NbnBinary.ReadNbnRegionSection(nbnBytes, entry.Offset));
        }

        return sections;
    }
}
