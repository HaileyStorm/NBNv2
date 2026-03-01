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
    public async Task ReproduceByArtifacts_ValueAndFunctionKnobs_AreAppliedToChildTemplate()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-value-func-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var parentARegion0 = new[]
            {
                new NeuronRecord(
                    axonCount: 0,
                    paramBCode: 20,
                    paramACode: 10,
                    activationThresholdCode: 6,
                    preActivationThresholdCode: 8,
                    resetFunctionId: 1,
                    activationFunctionId: 2,
                    accumulationFunctionId: 0,
                    exists: true)
            };
            var parentBRegion0 = new[]
            {
                new NeuronRecord(
                    axonCount: 0,
                    paramBCode: 40,
                    paramACode: 30,
                    activationThresholdCode: 14,
                    preActivationThresholdCode: 20,
                    resetFunctionId: 3,
                    activationFunctionId: 4,
                    accumulationFunctionId: 2,
                    exists: true)
            };
            var region31 = new[] { CreateNeuron(axonCount: 0) };
            var parentA = CreateTwoRegionNbn(parentARegion0, Array.Empty<AxonRecord>(), region31, Array.Empty<AxonRecord>());
            var parentB = CreateTwoRegionNbn(parentBRegion0, Array.Empty<AxonRecord>(), region31, Array.Empty<AxonRecord>());

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
                    Seed = 99,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        ProbChooseParentA = 0f,
                        ProbChooseParentB = 0f,
                        ProbAverage = 1f,
                        ProbMutate = 0f,
                        ProbChooseFuncA = 1f,
                        ProbMutateFunc = 0f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
                    }
                });

            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.NotNull(response.ChildDef);

            var childBytes = await ReadArtifactBytesAsync(store, response.ChildDef!);
            var childHeader = NbnBinary.ReadNbnHeader(childBytes);
            var childSections = ReadSections(childBytes, childHeader);
            var region0 = childSections.Single(section => section.RegionId == 0);
            var neuron = region0.NeuronRecords[0];

            Assert.Equal((byte)30, neuron.ParamBCode);
            Assert.Equal((byte)20, neuron.ParamACode);
            Assert.Equal((byte)10, neuron.ActivationThresholdCode);
            Assert.Equal((byte)14, neuron.PreActivationThresholdCode);
            Assert.Equal((byte)1, neuron.ResetFunctionId);
            Assert.Equal((byte)2, neuron.ActivationFunctionId);
            Assert.Equal((byte)0, neuron.AccumulationFunctionId);

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
    public async Task ReproduceByArtifacts_StrengthTransformAverage_ChangesStrengthCode_AndSummary()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-strength-transform-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var region0 = new[] { CreateNeuron(axonCount: 1) };
            var region31 = new[] { CreateNeuron(axonCount: 0) };
            var parentAAxons = new[] { new AxonRecord(2, 0, 31) };
            var parentBAxons = new[] { new AxonRecord(28, 0, 31) };
            var parentA = CreateTwoRegionNbn(region0, parentAAxons, region31, Array.Empty<AxonRecord>());
            var parentB = CreateTwoRegionNbn(region0, parentBAxons, region31, Array.Empty<AxonRecord>());

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
                    Seed = 77,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        ProbChooseParentA = 1f,
                        ProbChooseParentB = 0f,
                        StrengthTransformEnabled = true,
                        ProbStrengthChooseA = 0f,
                        ProbStrengthChooseB = 0f,
                        ProbStrengthAverage = 1f,
                        ProbStrengthWeightedAverage = 0f,
                        ProbStrengthMutate = 0f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
                    }
                });

            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.NotNull(response.Summary);
            Assert.True(response.Summary.StrengthCodesChanged >= 1);
            Assert.NotNull(response.ChildDef);

            var childBytes = await ReadArtifactBytesAsync(store, response.ChildDef!);
            var expectedStrength = (byte)Math.Clamp(
                QuantizationSchemas.DefaultNbn.Strength.Encode(
                    (QuantizationSchemas.DefaultNbn.Strength.Decode(2, bits: 5)
                     + QuantizationSchemas.DefaultNbn.Strength.Decode(28, bits: 5)) * 0.5f,
                    bits: 5),
                0,
                31);
            Assert.Equal(expectedStrength, ReadAxonStrengthCode(childBytes, 0, 0, 0));

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
    public async Task ReproduceByArtifacts_FunctionMutation_BiasesTowardStableFamilies()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-function-bias-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var parentRegion0 = new[]
            {
                new NeuronRecord(
                    axonCount: 0,
                    paramBCode: 0,
                    paramACode: 0,
                    activationThresholdCode: 0,
                    preActivationThresholdCode: 0,
                    resetFunctionId: 4,
                    activationFunctionId: 13,
                    accumulationFunctionId: 1,
                    exists: true)
            };
            var region31 = new[] { CreateNeuron(axonCount: 0) };
            var parentA = CreateTwoRegionNbn(parentRegion0, Array.Empty<AxonRecord>(), region31, Array.Empty<AxonRecord>());
            var parentB = CreateTwoRegionNbn(parentRegion0, Array.Empty<AxonRecord>(), region31, Array.Empty<AxonRecord>());

            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var manifestA = await store.StoreAsync(new MemoryStream(parentA), "application/x-nbn");
            var manifestB = await store.StoreAsync(new MemoryStream(parentB), "application/x-nbn");
            var parentARef = manifestA.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestA.ByteLength, "application/x-nbn", artifactRoot);
            var parentBRef = manifestB.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestB.ByteLength, "application/x-nbn", artifactRoot);

            var system = new ActorSystem();
            var root = system.Root;
            var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor()));

            var preferredActivationIds = new HashSet<byte> { 1, 5, 6, 7, 8, 9, 11, 18, 28 };
            var preferredResetIds = new HashSet<byte> { 0, 1, 3, 17, 30, 43, 44, 45, 47, 48, 49, 58 };
            var preferredAccumIds = new HashSet<byte> { 0, 1, 2 };
            var preferredActivationCount = 0;
            var preferredResetCount = 0;
            var preferredAccumCount = 0;
            const int sampleCount = 20;

            for (ulong seed = 1; seed <= sampleCount; seed++)
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
                            ProbChooseParentA = 1f,
                            ProbChooseParentB = 0f,
                            ProbAverage = 0f,
                            ProbMutate = 0f,
                            ProbChooseFuncA = 1f,
                            ProbMutateFunc = 1f,
                            SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever
                        }
                    });

                Assert.NotNull(response.Report);
                Assert.True(response.Report.Compatible);
                Assert.NotNull(response.ChildDef);

                var childBytes = await ReadArtifactBytesAsync(store, response.ChildDef!);
                var childHeader = NbnBinary.ReadNbnHeader(childBytes);
                var childSections = ReadSections(childBytes, childHeader);
                var neuron = childSections.Single(section => section.RegionId == 0).NeuronRecords[0];

                if (preferredActivationIds.Contains(neuron.ActivationFunctionId))
                {
                    preferredActivationCount++;
                }

                if (preferredResetIds.Contains(neuron.ResetFunctionId))
                {
                    preferredResetCount++;
                }

                if (preferredAccumIds.Contains(neuron.AccumulationFunctionId))
                {
                    preferredAccumCount++;
                }
            }

            Assert.True(preferredActivationCount >= 12, $"Expected activation mutation bias toward preferred set; got {preferredActivationCount}/{sampleCount}.");
            Assert.True(preferredResetCount >= 10, $"Expected reset mutation bias toward preferred set; got {preferredResetCount}/{sampleCount}.");
            Assert.True(preferredAccumCount >= 16, $"Expected accumulation mutation bias toward preferred set; got {preferredAccumCount}/{sampleCount}.");

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
    public async Task ReproduceByArtifacts_DisableNeuron_ReroutesInbound_WhenConfigured()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-inbound-reroute-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var stride = (uint)NbnConstants.DefaultAxonStride;
            var sections = new List<NbnRegionSection>
            {
                CreateRegionSection(0, stride, new[] { CreateNeuron(axonCount: 0) }, Array.Empty<AxonRecord>()),
                CreateRegionSection(
                    1,
                    stride,
                    new[]
                    {
                        CreateNeuron(axonCount: 0),
                        CreateNeuron(axonCount: 0)
                    },
                    Array.Empty<AxonRecord>()),
                CreateRegionSection(
                    2,
                    stride,
                    new[] { CreateNeuron(axonCount: 1) },
                    new[] { new AxonRecord(10, 0, 1) }),
                CreateRegionSection(31, stride, new[] { CreateNeuron(axonCount: 0) }, Array.Empty<AxonRecord>())
            };

            var parentA = BuildNbn(sections, stride, NbnTestVectors.SampleBrainSeed);
            var parentB = BuildNbn(sections, stride, NbnTestVectors.SampleBrainSeed + 1);

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
                    Seed = 42,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        ProbChooseParentA = 1f,
                        ProbChooseParentB = 0f,
                        ProbDisableNeuron = 1f,
                        ProbReactivateNeuron = 0f,
                        ProbAddNeuronToEmptyRegion = 0f,
                        ProbRemoveLastNeuronFromRegion = 0f,
                        ProbAddAxon = 0f,
                        ProbRemoveAxon = 0f,
                        ProbRerouteAxon = 0f,
                        ProbRerouteInboundAxonOnDelete = 1f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever,
                        Limits = new Repro.ReproduceLimits
                        {
                            MaxNeuronsRemovedAbs = 1
                        }
                    }
                });

            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.NotNull(response.Summary);
            Assert.True(response.Summary.NeuronsRemoved >= 1);
            Assert.True(response.Summary.AxonsRerouted >= 1);
            Assert.NotNull(response.ChildDef);

            var childBytes = await ReadArtifactBytesAsync(store, response.ChildDef!);
            var childHeader = NbnBinary.ReadNbnHeader(childBytes);
            var childSections = ReadSections(childBytes, childHeader);
            var region1 = childSections.Single(section => section.RegionId == 1);
            var region2 = childSections.Single(section => section.RegionId == 2);

            Assert.False(region1.NeuronRecords[0].Exists);
            Assert.True(region1.NeuronRecords[1].Exists);
            Assert.Equal((byte)1, region2.AxonRecords[0].TargetRegionId);
            Assert.Equal(1, region2.AxonRecords[0].TargetNeuronId);

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
    public async Task ReproduceByArtifacts_DisableNeuron_RemovesInbound_WhenRerouteDistanceExceeded()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-inbound-reroute-maxdist-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var stride = (uint)NbnConstants.DefaultAxonStride;
            var sections = new List<NbnRegionSection>
            {
                CreateRegionSection(0, stride, [CreateNeuron(axonCount: 0)], Array.Empty<AxonRecord>()),
                CreateRegionSection(
                    1,
                    stride,
                    [
                        CreateNeuron(axonCount: 0),
                        CreateNeuron(axonCount: 0, exists: false),
                        CreateNeuron(axonCount: 0),
                        CreateNeuron(axonCount: 0, exists: false),
                        CreateNeuron(axonCount: 0, exists: false)
                    ],
                    Array.Empty<AxonRecord>()),
                CreateRegionSection(
                    2,
                    stride,
                    [CreateNeuron(axonCount: 1)],
                    [new AxonRecord(10, 0, 1)]),
                CreateRegionSection(31, stride, [CreateNeuron(axonCount: 0)], Array.Empty<AxonRecord>())
            };

            var parentA = BuildNbn(sections, stride, NbnTestVectors.SampleBrainSeed);
            var parentB = BuildNbn(sections, stride, NbnTestVectors.SampleBrainSeed + 1);

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
                    Seed = 84,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        ProbChooseParentA = 1f,
                        ProbChooseParentB = 0f,
                        ProbDisableNeuron = 1f,
                        ProbReactivateNeuron = 0f,
                        ProbAddNeuronToEmptyRegion = 0f,
                        ProbRemoveLastNeuronFromRegion = 0f,
                        ProbAddAxon = 0f,
                        ProbRemoveAxon = 0f,
                        ProbRerouteAxon = 0f,
                        ProbRerouteInboundAxonOnDelete = 1f,
                        InboundRerouteMaxRingDistance = 1,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever,
                        Limits = new Repro.ReproduceLimits
                        {
                            MaxNeuronsRemovedAbs = 1
                        }
                    }
                });

            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.NotNull(response.Summary);
            Assert.True(response.Summary.NeuronsRemoved >= 1);
            Assert.Equal((uint)0, response.Summary.AxonsRerouted);
            Assert.True(response.Summary.AxonsRemoved >= 1);
            Assert.NotNull(response.ChildDef);

            var childBytes = await ReadArtifactBytesAsync(store, response.ChildDef!);
            var childHeader = NbnBinary.ReadNbnHeader(childBytes);
            var childSections = ReadSections(childBytes, childHeader);
            var region1 = childSections.Single(section => section.RegionId == 1);
            var region2 = childSections.Single(section => section.RegionId == 2);

            Assert.False(region1.NeuronRecords[0].Exists);
            Assert.True(region1.NeuronRecords[2].Exists);
            Assert.Equal(0, region2.NeuronRecords[0].AxonCount);
            Assert.Empty(region2.AxonRecords);

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
    public async Task ReproduceByArtifacts_SuccessReport_UsesActualChildRegionCount()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-child-regions-{Guid.NewGuid():N}");
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
                    Seed = 77,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        ProbChooseParentA = 1f,
                        ProbChooseParentB = 0f,
                        ProbAddNeuronToEmptyRegion = 1f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever,
                        Limits = new Repro.ReproduceLimits
                        {
                            MaxNeuronsAddedAbs = 1,
                            MaxRegionsAddedAbs = 1
                        }
                    }
                });

            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.Equal((uint)2, response.Report.RegionsPresentA);
            Assert.Equal((uint)2, response.Report.RegionsPresentB);
            Assert.Equal((uint)3, response.Report.RegionsPresentChild);
            Assert.NotNull(response.ChildDef);

            var childBytes = await ReadArtifactBytesAsync(store, response.ChildDef!);
            var childHeader = NbnBinary.ReadNbnHeader(childBytes);
            var childSections = ReadSections(childBytes, childHeader);
            Assert.Equal(3, childSections.Count);

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
    public async Task ReproduceByArtifacts_ZeroRegionLimits_BlockRegionPresenceChanges()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-region-limits-{Guid.NewGuid():N}");
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
                    Seed = 88,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        ProbChooseParentA = 1f,
                        ProbChooseParentB = 0f,
                        ProbAddNeuronToEmptyRegion = 1f,
                        ProbRemoveLastNeuronFromRegion = 1f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever,
                        Limits = new Repro.ReproduceLimits
                        {
                            MaxNeuronsAddedAbs = 8,
                            MaxNeuronsRemovedAbs = 8,
                            MaxRegionsAddedAbs = 0,
                            MaxRegionsRemovedAbs = 0
                        }
                    }
                });

            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.Equal((uint)2, response.Report.RegionsPresentA);
            Assert.Equal((uint)2, response.Report.RegionsPresentB);
            Assert.Equal((uint)2, response.Report.RegionsPresentChild);
            Assert.NotNull(response.Summary);
            Assert.Equal((uint)0, response.Summary.NeuronsAdded);
            Assert.Equal((uint)0, response.Summary.NeuronsRemoved);

            Assert.NotNull(response.ChildDef);
            var childBytes = await ReadArtifactBytesAsync(store, response.ChildDef!);
            var childHeader = NbnBinary.ReadNbnHeader(childBytes);
            var childSections = ReadSections(childBytes, childHeader);
            Assert.Equal(2, childSections.Count);

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
    public async Task ReproduceByArtifacts_PerRegionOutDegreeCaps_PruneTargetRegionOnly()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-region-cap-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var stride = (uint)NbnConstants.DefaultAxonStride;
            var sections = new List<NbnRegionSection>
            {
                CreateRegionSection(0, stride, [CreateNeuron(axonCount: 0)], Array.Empty<AxonRecord>()),
                CreateRegionSection(
                    1,
                    stride,
                    [CreateNeuron(axonCount: 3), CreateNeuron(axonCount: 3)],
                    [
                        new AxonRecord(10, 0, 2),
                        new AxonRecord(10, 1, 2),
                        new AxonRecord(10, 0, 31),
                        new AxonRecord(11, 0, 2),
                        new AxonRecord(11, 2, 2),
                        new AxonRecord(11, 0, 31)
                    ]),
                CreateRegionSection(
                    2,
                    stride,
                    [CreateNeuron(axonCount: 1), CreateNeuron(axonCount: 1), CreateNeuron(axonCount: 0)],
                    [
                        new AxonRecord(7, 0, 31),
                        new AxonRecord(8, 0, 31)
                    ]),
                CreateRegionSection(31, stride, [CreateNeuron(axonCount: 0)], Array.Empty<AxonRecord>())
            };

            var parentA = BuildNbn(sections, stride, NbnTestVectors.SampleBrainSeed);
            var parentB = BuildNbn(sections, stride, NbnTestVectors.SampleBrainSeed + 1);

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
                    Seed = 424242,
                    Config = new Repro.ReproduceConfig
                    {
                        MaxFunctionHistDistance = 1f,
                        MaxConnectivityHistDistance = 1f,
                        ProbChooseParentA = 1f,
                        ProbChooseParentB = 0f,
                        MaxAvgOutDegreeBrain = 100f,
                        SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever,
                        PerRegionOutDegreeCaps =
                        {
                            new Repro.RegionOutDegreeCap
                            {
                                RegionId = 1,
                                MaxAvgOutDegree = 1f
                            }
                        }
                    }
                });

            Assert.NotNull(response.Report);
            Assert.True(response.Report.Compatible);
            Assert.NotNull(response.Summary);
            Assert.Equal((uint)4, response.Summary.AxonsRemoved);
            Assert.NotNull(response.ChildDef);

            var childBytes = await ReadArtifactBytesAsync(store, response.ChildDef!);
            var childHeader = NbnBinary.ReadNbnHeader(childBytes);
            var childSections = ReadSections(childBytes, childHeader);
            var region1 = childSections.Single(section => section.RegionId == 1);
            var region2 = childSections.Single(section => section.RegionId == 2);

            var region1ExistingNeurons = region1.NeuronRecords.Count(neuron => neuron.Exists);
            var region1TotalAxons = region1.NeuronRecords.Where(neuron => neuron.Exists).Sum(neuron => neuron.AxonCount);
            Assert.Equal(2, region1ExistingNeurons);
            Assert.Equal(2, region1TotalAxons);
            Assert.True(region1TotalAxons <= Math.Floor(1f * region1ExistingNeurons));

            var region2TotalAxons = region2.NeuronRecords.Where(neuron => neuron.Exists).Sum(neuron => neuron.AxonCount);
            Assert.Equal(2, region2TotalAxons);

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

    [Fact]
    public async Task ReproduceByArtifacts_WorkbenchDefaultLineage_PreservesExpectedCompatibilityBands()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-repro-lineage-defaults-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var parentA = DemoNbnBuilder.BuildSampleNbn();
            var parentB = DemoNbnBuilder.BuildSampleNbn();
            var unrelated = NbnTestVectors.CreateMinimalNbn();
            var manifestA = await store.StoreAsync(new MemoryStream(parentA), "application/x-nbn");
            var manifestB = await store.StoreAsync(new MemoryStream(parentB), "application/x-nbn");
            var unrelatedManifest = await store.StoreAsync(new MemoryStream(unrelated), "application/x-nbn");
            var parentARef = manifestA.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestA.ByteLength, "application/x-nbn", artifactRoot);
            var parentBRef = manifestB.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifestB.ByteLength, "application/x-nbn", artifactRoot);
            var unrelatedRef = unrelatedManifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)unrelatedManifest.ByteLength, "application/x-nbn", artifactRoot);

            var system = new ActorSystem();
            var root = system.Root;
            var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor()));

            var generationSuccesses = 0;
            var directParentChecks = 0;
            var directParentSuccesses = 0;
            var directParentScores = new List<float>();
            var unrelatedScores = new List<float>();
            var currentLeft = parentARef;
            var currentRight = parentBRef;

            for (var generation = 0; generation < 8; generation++)
            {
                var generationResult = await root.RequestAsync<Repro.ReproduceResult>(
                    manager,
                    new Repro.ReproduceByArtifactsRequest
                    {
                        ParentADef = currentLeft,
                        ParentBDef = currentRight,
                        Seed = 6100UL + (ulong)generation,
                        Config = CreateWorkbenchDefaultReproConfig()
                    });

                Assert.NotNull(generationResult.Report);
                if (!generationResult.Report.Compatible || generationResult.ChildDef is null)
                {
                    break;
                }

                generationSuccesses++;
                directParentScores.Add(generationResult.Report.SimilarityScore);
                var childRef = generationResult.ChildDef;

                foreach (var directParentRef in new[] { currentLeft, currentRight })
                {
                    var directCheck = await root.RequestAsync<Repro.ReproduceResult>(
                        manager,
                        new Repro.ReproduceByArtifactsRequest
                        {
                            ParentADef = childRef,
                            ParentBDef = directParentRef,
                            Seed = 7100UL + (ulong)(generation * 10) + (ulong)directParentChecks,
                            Config = CreateWorkbenchDefaultReproConfig()
                        });

                    Assert.NotNull(directCheck.Report);
                    directParentChecks++;
                    if (directCheck.Report.Compatible)
                    {
                        directParentSuccesses++;
                        directParentScores.Add(directCheck.Report.SimilarityScore);
                    }
                }

                var unrelatedCheck = await root.RequestAsync<Repro.ReproduceResult>(
                    manager,
                    new Repro.ReproduceByArtifactsRequest
                    {
                        ParentADef = childRef,
                        ParentBDef = unrelatedRef,
                        Seed = 9100UL + (ulong)generation,
                        Config = CreateWorkbenchDefaultReproConfig()
                    });

                Assert.NotNull(unrelatedCheck.Report);
                Assert.False(unrelatedCheck.Report.Compatible);
                Assert.Equal("repro_region_presence_mismatch", unrelatedCheck.Report.AbortReason);
                unrelatedScores.Add(unrelatedCheck.Report.SimilarityScore);

                currentLeft = childRef;
                currentRight = generation % 2 == 0 ? parentARef : parentBRef;
            }

            Assert.True(generationSuccesses >= 6, $"Expected at least 6 successful generations; got {generationSuccesses}.");
            Assert.True(directParentChecks >= generationSuccesses * 2);
            Assert.NotEmpty(directParentScores);
            Assert.NotEmpty(unrelatedScores);

            var directSuccessRate = directParentChecks == 0
                ? 0f
                : directParentSuccesses / (float)directParentChecks;
            Assert.True(directSuccessRate >= 0.75f, $"Expected direct parent compatibility >= 0.75, got {directSuccessRate:0.###}.");

            var directAverage = directParentScores.Average();
            var unrelatedAverage = unrelatedScores.Average();
            Assert.True(directAverage >= 0.55f, $"Expected direct lineage average similarity >= 0.55, got {directAverage:0.###}.");
            Assert.True(unrelatedAverage <= 0.25f, $"Expected unrelated average similarity <= 0.25, got {unrelatedAverage:0.###}.");
            Assert.True(directAverage > unrelatedAverage, $"Expected direct average similarity > unrelated average ({directAverage:0.###} vs {unrelatedAverage:0.###}).");

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

    private static Repro.ReproduceConfig CreateWorkbenchDefaultReproConfig()
        => new()
        {
            MaxRegionSpanDiffRatio = 0.15f,
            MaxFunctionHistDistance = 0.25f,
            MaxConnectivityHistDistance = 0.25f,
            ProbAddNeuronToEmptyRegion = 0f,
            ProbRemoveLastNeuronFromRegion = 0f,
            ProbDisableNeuron = 0.01f,
            ProbReactivateNeuron = 0.01f,
            ProbAddAxon = 0.05f,
            ProbRemoveAxon = 0.02f,
            ProbRerouteAxon = 0.02f,
            ProbRerouteInboundAxonOnDelete = 0.50f,
            InboundRerouteMaxRingDistance = 0,
            ProbChooseParentA = 0.45f,
            ProbChooseParentB = 0.45f,
            ProbAverage = 0.05f,
            ProbMutate = 0.05f,
            ProbChooseFuncA = 0.50f,
            ProbMutateFunc = 0.02f,
            MaxAvgOutDegreeBrain = 100f,
            PrunePolicy = Repro.PrunePolicy.PruneLowestAbsStrengthFirst,
            StrengthTransformEnabled = false,
            ProbStrengthChooseA = 0.35f,
            ProbStrengthChooseB = 0.35f,
            ProbStrengthAverage = 0.20f,
            ProbStrengthWeightedAverage = 0.05f,
            StrengthWeightA = 0.50f,
            StrengthWeightB = 0.50f,
            ProbStrengthMutate = 0.05f,
            SpawnChild = Repro.SpawnChildPolicy.SpawnChildNever,
            Limits = new Repro.ReproduceLimits
            {
                MaxNeuronsAddedAbs = 0,
                MaxNeuronsAddedPct = 0f,
                MaxNeuronsRemovedAbs = 0,
                MaxNeuronsRemovedPct = 0f,
                MaxAxonsAddedAbs = 0,
                MaxAxonsAddedPct = 0f,
                MaxAxonsRemovedAbs = 0,
                MaxAxonsRemovedPct = 0f,
                MaxRegionsAddedAbs = 0,
                MaxRegionsRemovedAbs = 0
            }
        };

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
        byte accumulationFunctionId = 0,
        bool exists = true)
        => new(
            axonCount: axonCount,
            paramBCode: 0,
            paramACode: 0,
            activationThresholdCode: 0,
            preActivationThresholdCode: 0,
            resetFunctionId: resetFunctionId,
            activationFunctionId: activationFunctionId,
            accumulationFunctionId: accumulationFunctionId,
            exists: exists);

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
