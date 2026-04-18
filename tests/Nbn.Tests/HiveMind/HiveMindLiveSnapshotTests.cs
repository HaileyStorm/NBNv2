using Microsoft.Data.Sqlite;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.HiveMind;
using Nbn.Runtime.IO;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Tests.Format;
using Nbn.Tests.TestSupport;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;
using SharedAddress32 = Nbn.Shared.Addressing.Address32;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Tests.HiveMind;

public class HiveMindLiveSnapshotTests
{
    [Fact]
    public async Task RequestSnapshot_Builds_And_Persists_Live_Nbs_With_Overlay_Deltas()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-hive-snapshot-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var richNbn = NbnTestVectors.CreateRichNbnVector();
            var baseManifest = await store.StoreAsync(new MemoryStream(richNbn.Bytes), "application/x-nbn");
            var baseRef = baseManifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)baseManifest.ByteLength, "application/x-nbn", artifactRoot);

            var system = new ActorSystem();
            var root = system.Root;
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));
            PrimeEligibleWorker(root, hiveMind);

            var brainId = Guid.NewGuid();
            var placement = await root.RequestAsync<PlacementAck>(
                hiveMind,
                new RequestPlacement
                {
                    BrainId = brainId.ToProtoUuid(),
                    BaseDef = baseRef,
                    InputWidth = 3,
                    OutputWidth = 2
                });
            Assert.True(placement.Accepted);

            var region0Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
                brainId,
                ShardId32.From(0, 0),
                neuronStart: 0,
                bufferCodes: new[] { 10, 11, 12 },
                enabledBitset: new byte[] { 0b0000_0111 },
                overlays: Array.Empty<SnapshotOverlayRecord>())));
            var region1Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
                brainId,
                ShardId32.From(1, 0),
                neuronStart: 0,
                bufferCodes: new[] { 20, 21, 22, 23 },
                enabledBitset: new byte[] { 0b0000_1101 },
                overlays: new[]
                {
                    new SnapshotOverlayRecord
                    {
                        FromAddress = SharedAddress32.From(1, 1).Value,
                        ToAddress = SharedAddress32.From(31, 1).Value,
                        StrengthCode = 22
                    }
                })));
            var region31Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
                brainId,
                ShardId32.From(31, 0),
                neuronStart: 0,
                bufferCodes: new[] { 30, 31 },
                enabledBitset: new byte[] { 0b0000_0001 },
                overlays: Array.Empty<SnapshotOverlayRecord>())));

            await root.RequestAsync<SendMessageAck>(region0Shard, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 0,
                ShardIndex = 0,
                ShardPid = PidLabel(region0Shard),
                NeuronStart = 0,
                NeuronCount = 3,
                PlacementEpoch = placement.PlacementEpoch
            }));
            await root.RequestAsync<SendMessageAck>(region1Shard, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 1,
                ShardIndex = 0,
                ShardPid = PidLabel(region1Shard),
                NeuronStart = 0,
                NeuronCount = 4,
                PlacementEpoch = placement.PlacementEpoch
            }));
            await root.RequestAsync<SendMessageAck>(region31Shard, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 31,
                ShardIndex = 0,
                ShardPid = PidLabel(region31Shard),
                NeuronStart = 0,
                NeuronCount = 2,
                PlacementEpoch = placement.PlacementEpoch
            }));

            root.Send(hiveMind, new ProtoSettings.SettingChanged
            {
                Key = CostEnergySettingsKeys.SystemEnabledKey,
                Value = "true",
                UpdatedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            });

            var ready = await root.RequestAsync<SnapshotReady>(
                hiveMind,
                new RequestSnapshot
                {
                    BrainId = brainId.ToProtoUuid(),
                    HasRuntimeState = true,
                    EnergyRemaining = 777,
                    CostEnabled = true,
                    EnergyEnabled = true,
                    PlasticityEnabled = true
                });

            Assert.NotNull(ready.Snapshot);
            Assert.True(ready.Snapshot.TryToSha256Bytes(out var snapshotHashBytes));

            var snapshotStream = await store.TryOpenArtifactAsync(new Sha256Hash(snapshotHashBytes));
            Assert.NotNull(snapshotStream);

            byte[] snapshotBytes;
            await using (snapshotStream!)
            using (var ms = new MemoryStream())
            {
                await snapshotStream.CopyToAsync(ms);
                snapshotBytes = ms.ToArray();
            }

            var nbsHeader = NbnBinary.ReadNbsHeader(snapshotBytes);
            Assert.Equal(brainId, nbsHeader.BrainId);
            Assert.Equal((long)777, nbsHeader.EnergyRemaining);
            Assert.True(nbsHeader.CostEnabled);
            Assert.True(nbsHeader.EnergyEnabled);
            Assert.True(nbsHeader.PlasticityEnabled);
            Assert.True(nbsHeader.EnabledBitsetIncluded);
            Assert.True(nbsHeader.AxonOverlayIncluded);

            var nbnHeader = NbnBinary.ReadNbnHeader(richNbn.Bytes);
            var offset = NbnBinary.NbsHeaderBytes;
            var regionSections = new Dictionary<int, NbsRegionSection>();
            for (var regionId = 0; regionId < nbnHeader.Regions.Length; regionId++)
            {
                if (nbnHeader.Regions[regionId].NeuronSpan == 0)
                {
                    continue;
                }

                var section = NbnBinary.ReadNbsRegionSection(snapshotBytes, offset, includeEnabledBitset: true);
                regionSections[regionId] = section;
                offset += section.ByteLength;
            }

            Assert.Equal(new short[] { 10, 11, 12 }, regionSections[0].BufferCodes);
            Assert.Equal(new byte[] { 0b0000_0111 }, regionSections[0].EnabledBitset);
            Assert.Equal(new short[] { 20, 21, 22, 23 }, regionSections[1].BufferCodes);
            Assert.Equal(new byte[] { 0b0000_1101 }, regionSections[1].EnabledBitset);
            Assert.Equal(new short[] { 30, 31 }, regionSections[31].BufferCodes);
            Assert.Equal(new byte[] { 0b0000_0001 }, regionSections[31].EnabledBitset);

            var overlays = NbnBinary.ReadNbsOverlaySection(snapshotBytes, offset);
            var overlay = Assert.Single(overlays.Records);
            Assert.Equal(SharedAddress32.From(1, 1).Value, overlay.FromAddress);
            Assert.Equal(SharedAddress32.From(31, 1).Value, overlay.ToAddress);
            Assert.Equal((byte)22, overlay.StrengthCode);

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
    public async Task RequestSnapshot_Persists_Homeostasis_Config_That_Reloads_On_Placement()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-hive-snapshot-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var richNbn = NbnTestVectors.CreateRichNbnVector();
            var baseManifest = await store.StoreAsync(new MemoryStream(richNbn.Bytes), "application/x-nbn");
            var baseRef = baseManifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)baseManifest.ByteLength, "application/x-nbn", artifactRoot);
            var brainId = Guid.NewGuid();
            var expectedHomeostasis = new NbsHomeostasisConfig(
                Enabled: false,
                TargetMode: HomeostasisTargetMode.HomeostasisTargetFixed,
                UpdateMode: HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
                BaseProbability: 0.35f,
                MinStepCodes: 3,
                EnergyCouplingEnabled: true,
                EnergyTargetScale: 0.75f,
                EnergyProbabilityScale: 1.5f);

            ArtifactRef snapshotRef;
            var system = new ActorSystem();
            var systemShutdown = false;
            try
            {
                var root = system.Root;
                var ioName = $"io-{Guid.NewGuid():N}";
                var ioPid = new PID(string.Empty, ioName);
                var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions(), ioPid: ioPid)));
                var gateway = root.SpawnNamed(
                    Props.FromProducer(() => new IoGatewayActor(CreateIoOptions(ioName), hiveMindPid: hiveMind)),
                    ioName);
                PrimeEligibleWorker(root, hiveMind);

                var placement = await root.RequestAsync<PlacementAck>(
                    hiveMind,
                    new RequestPlacement
                    {
                        BrainId = brainId.ToProtoUuid(),
                        BaseDef = baseRef,
                        InputWidth = 3,
                        OutputWidth = 2
                    });
                Assert.True(placement.Accepted);

                var region0Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
                    brainId,
                    ShardId32.From(0, 0),
                    neuronStart: 0,
                    bufferCodes: new[] { 10, 11, 12 },
                    enabledBitset: new byte[] { 0b0000_0111 },
                    overlays: Array.Empty<SnapshotOverlayRecord>())));
                var region1Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
                    brainId,
                    ShardId32.From(1, 0),
                    neuronStart: 0,
                    bufferCodes: new[] { 20, 21, 22, 23 },
                    enabledBitset: new byte[] { 0b0000_1101 },
                    overlays: new[]
                    {
                        new SnapshotOverlayRecord
                        {
                            FromAddress = SharedAddress32.From(1, 1).Value,
                            ToAddress = SharedAddress32.From(31, 1).Value,
                            StrengthCode = 22
                        }
                    })));
                var region31Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
                    brainId,
                    ShardId32.From(31, 0),
                    neuronStart: 0,
                    bufferCodes: new[] { 30, 31 },
                    enabledBitset: new byte[] { 0b0000_0001 },
                    overlays: Array.Empty<SnapshotOverlayRecord>())));

                await root.RequestAsync<SendMessageAck>(region0Shard, new SendMessage(hiveMind, new RegisterShard
                {
                    BrainId = brainId.ToProtoUuid(),
                    RegionId = 0,
                    ShardIndex = 0,
                    ShardPid = PidLabel(region0Shard),
                    NeuronStart = 0,
                    NeuronCount = 3,
                    PlacementEpoch = placement.PlacementEpoch
                }));
                await root.RequestAsync<SendMessageAck>(region1Shard, new SendMessage(hiveMind, new RegisterShard
                {
                    BrainId = brainId.ToProtoUuid(),
                    RegionId = 1,
                    ShardIndex = 0,
                    ShardPid = PidLabel(region1Shard),
                    NeuronStart = 0,
                    NeuronCount = 4,
                    PlacementEpoch = placement.PlacementEpoch
                }));
                await root.RequestAsync<SendMessageAck>(region31Shard, new SendMessage(hiveMind, new RegisterShard
                {
                    BrainId = brainId.ToProtoUuid(),
                    RegionId = 31,
                    ShardIndex = 0,
                    ShardPid = PidLabel(region31Shard),
                    NeuronStart = 0,
                    NeuronCount = 2,
                    PlacementEpoch = placement.PlacementEpoch
                }));

                _ = await WaitForBrainInfoAsync(
                    root,
                    gateway,
                    brainId,
                    info => info.InputWidth == 3 && info.OutputWidth == 2,
                    timeoutMs: 4_000);

                var homeostasisAck = await root.RequestAsync<IoCommandAck>(gateway, new SetHomeostasisEnabled
                {
                    BrainId = brainId.ToProtoUuid(),
                    HomeostasisEnabled = expectedHomeostasis.Enabled,
                    HomeostasisTargetMode = expectedHomeostasis.TargetMode,
                    HomeostasisUpdateMode = expectedHomeostasis.UpdateMode,
                    HomeostasisBaseProbability = expectedHomeostasis.BaseProbability,
                    HomeostasisMinStepCodes = expectedHomeostasis.MinStepCodes,
                    HomeostasisEnergyCouplingEnabled = expectedHomeostasis.EnergyCouplingEnabled,
                    HomeostasisEnergyTargetScale = expectedHomeostasis.EnergyTargetScale,
                    HomeostasisEnergyProbabilityScale = expectedHomeostasis.EnergyProbabilityScale
                });
                Assert.True(homeostasisAck.Success);

                var ready = await root.RequestAsync<SnapshotReady>(
                    hiveMind,
                    new RequestSnapshot
                    {
                        BrainId = brainId.ToProtoUuid(),
                        HasRuntimeState = true,
                        EnergyRemaining = 0,
                        CostEnabled = false,
                        EnergyEnabled = false,
                        PlasticityEnabled = true
                    });

                Assert.NotNull(ready.Snapshot);
                snapshotRef = ready.Snapshot;
                Assert.True(snapshotRef.TryToSha256Bytes(out var snapshotHashBytes));

                await using var snapshotStream = await store.TryOpenArtifactAsync(new Sha256Hash(snapshotHashBytes));
                Assert.NotNull(snapshotStream);

                byte[] snapshotBytes;
                await using (snapshotStream!)
                using (var ms = new MemoryStream())
                {
                    await snapshotStream.CopyToAsync(ms);
                    snapshotBytes = ms.ToArray();
                }

                var snapshotHeader = NbnBinary.ReadNbsHeader(snapshotBytes);
                Assert.NotNull(snapshotHeader.HomeostasisConfig);
                AssertHomeostasisConfig(expectedHomeostasis, snapshotHeader.HomeostasisConfig!);

                await system.ShutdownAsync();
                systemShutdown = true;
            }
            finally
            {
                if (!systemShutdown)
                {
                    await system.ShutdownAsync();
                }
            }

            var restoreSystem = new ActorSystem();
            var restoreSystemShutdown = false;
            try
            {
                var root = restoreSystem.Root;
                var ioName = $"io-{Guid.NewGuid():N}";
                var ioPid = new PID(string.Empty, ioName);
                var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions(), ioPid: ioPid)));
                var gateway = root.SpawnNamed(
                    Props.FromProducer(() => new IoGatewayActor(CreateIoOptions(ioName), hiveMindPid: hiveMind)),
                    ioName);
                PrimeEligibleWorker(root, hiveMind);

                var placement = await root.RequestAsync<PlacementAck>(
                    hiveMind,
                    new RequestPlacement
                    {
                        BrainId = brainId.ToProtoUuid(),
                        BaseDef = baseRef,
                        LastSnapshot = snapshotRef,
                        InputWidth = 3,
                        OutputWidth = 2
                    });
                Assert.True(placement.Accepted);

                var restoredInfo = await WaitForBrainInfoAsync(
                    root,
                    gateway,
                    brainId,
                    info => info.HomeostasisEnabled == expectedHomeostasis.Enabled
                            && info.HomeostasisTargetMode == expectedHomeostasis.TargetMode
                            && info.HomeostasisUpdateMode == expectedHomeostasis.UpdateMode
                            && Math.Abs(info.HomeostasisBaseProbability - expectedHomeostasis.BaseProbability) < 0.000001f
                            && info.HomeostasisMinStepCodes == expectedHomeostasis.MinStepCodes
                            && info.HomeostasisEnergyCouplingEnabled == expectedHomeostasis.EnergyCouplingEnabled
                            && Math.Abs(info.HomeostasisEnergyTargetScale - expectedHomeostasis.EnergyTargetScale) < 0.000001f
                            && Math.Abs(info.HomeostasisEnergyProbabilityScale - expectedHomeostasis.EnergyProbabilityScale) < 0.000001f,
                    timeoutMs: 4_000);

                AssertHomeostasisConfig(restoredInfo, expectedHomeostasis);

                await restoreSystem.ShutdownAsync();
                restoreSystemShutdown = true;
            }
            finally
            {
                if (!restoreSystemShutdown)
                {
                    await restoreSystem.ShutdownAsync();
                }
            }
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
    public async Task RequestPlacement_Failure_Restores_Previous_Homeostasis_After_Snapshot_Header_Read()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-hive-snapshot-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var richNbn = NbnTestVectors.CreateRichNbnVector();
            var richNbs = NbnTestVectors.CreateRichNbsVector(richNbn);
            var customHomeostasis = new NbsHomeostasisConfig(
                Enabled: false,
                TargetMode: HomeostasisTargetMode.HomeostasisTargetFixed,
                UpdateMode: HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
                BaseProbability: 0.4f,
                MinStepCodes: 5,
                EnergyCouplingEnabled: true,
                EnergyTargetScale: 0.5f,
                EnergyProbabilityScale: 1.75f);

            var snapshotBytes = richNbs.Bytes.ToArray();
            var snapshotHeader = NbnBinary.ReadNbsHeader(snapshotBytes);
            NbnBinary.WriteNbsHeader(
                snapshotBytes,
                new NbsHeaderV2(
                    snapshotHeader.Magic,
                    snapshotHeader.Version,
                    snapshotHeader.Endianness,
                    snapshotHeader.HeaderBytesPow2,
                    snapshotHeader.BrainId,
                    snapshotHeader.SnapshotTickId,
                    snapshotHeader.TimestampMs,
                    snapshotHeader.EnergyRemaining,
                    snapshotHeader.BaseNbnSha256,
                    snapshotHeader.Flags,
                    snapshotHeader.BufferMap,
                    customHomeostasis));

            var snapshotManifest = await store.StoreAsync(new MemoryStream(snapshotBytes), "application/x-nbs");
            var snapshotRef = snapshotManifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)snapshotManifest.ByteLength, "application/x-nbs", artifactRoot);

            var registerBrain = new TaskCompletionSource<Nbn.Proto.Io.RegisterBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
            var system = new ActorSystem();
            var systemShutdown = false;
            try
            {
                var root = system.Root;
                var ioName = $"io-{Guid.NewGuid():N}";
                var ioPid = new PID(string.Empty, ioName);
                root.SpawnNamed(Props.FromProducer(() => new RegisterBrainProbeActor(registerBrain)), ioName);
                var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions(), ioPid: ioPid)));

                var placement = await root.RequestAsync<PlacementAck>(
                    hiveMind,
                    new RequestPlacement
                    {
                        BrainId = Guid.NewGuid().ToProtoUuid(),
                        LastSnapshot = snapshotRef
                    });

                Assert.False(placement.Accepted);

                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                var register = await registerBrain.Task.WaitAsync(cts.Token);

                Assert.True(register.HomeostasisEnabled);
                Assert.Equal(HomeostasisTargetMode.HomeostasisTargetZero, register.HomeostasisTargetMode);
                Assert.Equal(HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep, register.HomeostasisUpdateMode);
                Assert.Equal(0.01f, register.HomeostasisBaseProbability);
                Assert.Equal((uint)1, register.HomeostasisMinStepCodes);
                Assert.False(register.HomeostasisEnergyCouplingEnabled);
                Assert.Equal(1f, register.HomeostasisEnergyTargetScale);
                Assert.Equal(1f, register.HomeostasisEnergyProbabilityScale);

                await system.ShutdownAsync();
                systemShutdown = true;
            }
            finally
            {
                if (!systemShutdown)
                {
                    await system.ShutdownAsync();
                }
            }
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
    public async Task RequestSnapshot_Builds_And_Persists_Live_Nbs_To_RegisteredNonFileStoreUri()
    {
        using var remoteScope = new RegisteredArtifactStoreScope();

        var richNbn = NbnTestVectors.CreateRichNbnVector();
        var baseManifest = await remoteScope.Store.StoreAsync(new MemoryStream(richNbn.Bytes), "application/x-nbn");
        var baseRef = baseManifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)baseManifest.ByteLength, "application/x-nbn", remoteScope.StoreUri);

        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));
        PrimeEligibleWorker(root, hiveMind);

        var brainId = Guid.NewGuid();
        var placement = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                BaseDef = baseRef,
                InputWidth = 3,
                OutputWidth = 2
            });
        Assert.True(placement.Accepted);

        var region0Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
            brainId,
            ShardId32.From(0, 0),
            neuronStart: 0,
            bufferCodes: new[] { 10, 11, 12 },
            enabledBitset: new byte[] { 0b0000_0111 },
            overlays: Array.Empty<SnapshotOverlayRecord>())));
        var region1Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
            brainId,
            ShardId32.From(1, 0),
            neuronStart: 0,
            bufferCodes: new[] { 20, 21, 22, 23 },
            enabledBitset: new byte[] { 0b0000_1101 },
            overlays: new[]
            {
                new SnapshotOverlayRecord
                {
                    FromAddress = SharedAddress32.From(1, 1).Value,
                    ToAddress = SharedAddress32.From(31, 1).Value,
                    StrengthCode = 22
                }
            })));
        var region31Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
            brainId,
            ShardId32.From(31, 0),
            neuronStart: 0,
            bufferCodes: new[] { 30, 31 },
            enabledBitset: new byte[] { 0b0000_0001 },
            overlays: Array.Empty<SnapshotOverlayRecord>())));

        await root.RequestAsync<SendMessageAck>(region0Shard, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = 0,
            ShardIndex = 0,
            ShardPid = PidLabel(region0Shard),
            NeuronStart = 0,
            NeuronCount = 3,
            PlacementEpoch = placement.PlacementEpoch
        }));
        await root.RequestAsync<SendMessageAck>(region1Shard, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = 1,
            ShardIndex = 0,
            ShardPid = PidLabel(region1Shard),
            NeuronStart = 0,
            NeuronCount = 4,
            PlacementEpoch = placement.PlacementEpoch
        }));
        await root.RequestAsync<SendMessageAck>(region31Shard, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = 31,
            ShardIndex = 0,
            ShardPid = PidLabel(region31Shard),
            NeuronStart = 0,
            NeuronCount = 2,
            PlacementEpoch = placement.PlacementEpoch
        }));

        var ready = await root.RequestAsync<SnapshotReady>(
            hiveMind,
            new RequestSnapshot
            {
                BrainId = brainId.ToProtoUuid(),
                HasRuntimeState = true,
                EnergyRemaining = 777,
                CostEnabled = true,
                EnergyEnabled = true,
                PlasticityEnabled = true
            });

        Assert.NotNull(ready.Snapshot);
        Assert.Equal(remoteScope.StoreUri, ready.Snapshot.StoreUri);
        Assert.True(ready.Snapshot.TryToSha256Bytes(out var snapshotHashBytes));

        await using (var snapshotStream = await remoteScope.Store.TryOpenArtifactAsync(new Sha256Hash(snapshotHashBytes)))
        {
            Assert.NotNull(snapshotStream);
        }

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RequestSnapshot_Builds_And_Persists_Live_Nbs_To_HttpArtifactStore()
    {
        await using var server = new HttpArtifactStoreTestServer();
        var remoteStore = new HttpArtifactStore(server.BaseUri);

        var richNbn = NbnTestVectors.CreateRichNbnVector();
        var baseManifest = await server.SeedAsync(richNbn.Bytes, "application/x-nbn");
        var baseRef = baseManifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)baseManifest.ByteLength, "application/x-nbn", server.BaseUri.AbsoluteUri);

        var system = new ActorSystem();
        var root = system.Root;
        var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));
        PrimeEligibleWorker(root, hiveMind);

        var brainId = Guid.NewGuid();
        var placement = await root.RequestAsync<PlacementAck>(
            hiveMind,
            new RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                BaseDef = baseRef,
                InputWidth = 3,
                OutputWidth = 2
            });
        Assert.True(placement.Accepted);

        var region0Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
            brainId,
            ShardId32.From(0, 0),
            neuronStart: 0,
            bufferCodes: new[] { 10, 11, 12 },
            enabledBitset: new byte[] { 0b0000_0111 },
            overlays: Array.Empty<SnapshotOverlayRecord>())));
        var region1Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
            brainId,
            ShardId32.From(1, 0),
            neuronStart: 0,
            bufferCodes: new[] { 20, 21, 22, 23 },
            enabledBitset: new byte[] { 0b0000_1101 },
            overlays: new[]
            {
                new SnapshotOverlayRecord
                {
                    FromAddress = SharedAddress32.From(1, 1).Value,
                    ToAddress = SharedAddress32.From(31, 1).Value,
                    StrengthCode = 22
                }
            })));
        var region31Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
            brainId,
            ShardId32.From(31, 0),
            neuronStart: 0,
            bufferCodes: new[] { 30, 31 },
            enabledBitset: new byte[] { 0b0000_0001 },
            overlays: Array.Empty<SnapshotOverlayRecord>())));

        await root.RequestAsync<SendMessageAck>(region0Shard, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = 0,
            ShardIndex = 0,
            ShardPid = PidLabel(region0Shard),
            NeuronStart = 0,
            NeuronCount = 3,
            PlacementEpoch = placement.PlacementEpoch
        }));
        await root.RequestAsync<SendMessageAck>(region1Shard, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = 1,
            ShardIndex = 0,
            ShardPid = PidLabel(region1Shard),
            NeuronStart = 0,
            NeuronCount = 4,
            PlacementEpoch = placement.PlacementEpoch
        }));
        await root.RequestAsync<SendMessageAck>(region31Shard, new SendMessage(hiveMind, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = 31,
            ShardIndex = 0,
            ShardPid = PidLabel(region31Shard),
            NeuronStart = 0,
            NeuronCount = 2,
            PlacementEpoch = placement.PlacementEpoch
        }));

        var ready = await root.RequestAsync<SnapshotReady>(
            hiveMind,
            new RequestSnapshot
            {
                BrainId = brainId.ToProtoUuid(),
                HasRuntimeState = true,
                EnergyRemaining = 777,
                CostEnabled = true,
                EnergyEnabled = true,
                PlasticityEnabled = true
            });

        Assert.NotNull(ready.Snapshot);
        Assert.Equal(server.BaseUri.AbsoluteUri, ready.Snapshot.StoreUri);
        Assert.True(ready.Snapshot.TryToSha256Bytes(out var snapshotHashBytes));

        await using (var snapshotStream = await remoteStore.TryOpenArtifactAsync(new Sha256Hash(snapshotHashBytes)))
        {
            Assert.NotNull(snapshotStream);
        }

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ExportBrainDefinition_RebaseOverlays_Builds_Rebased_Nbn_From_Live_Overlay_Strengths()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-hive-rebase-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var richNbn = NbnTestVectors.CreateRichNbnVector();
            var baseManifest = await store.StoreAsync(new MemoryStream(richNbn.Bytes), "application/x-nbn");
            var baseRef = baseManifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)baseManifest.ByteLength, "application/x-nbn", artifactRoot);

            var baseStrengthCode = FindAxonStrengthCode(richNbn.Bytes, fromRegionId: 1, fromNeuronId: 1, toRegionId: 31, toNeuronId: 1);
            var rebasedStrengthCode = baseStrengthCode == 22 ? (byte)21 : (byte)22;

            var system = new ActorSystem();
            var root = system.Root;
            var hiveMind = root.Spawn(Props.FromProducer(() => new HiveMindActor(CreateOptions())));
            PrimeEligibleWorker(root, hiveMind);

            var brainId = Guid.NewGuid();
            var placement = await root.RequestAsync<PlacementAck>(
                hiveMind,
                new RequestPlacement
                {
                    BrainId = brainId.ToProtoUuid(),
                    BaseDef = baseRef,
                    InputWidth = 3,
                    OutputWidth = 2
                });
            Assert.True(placement.Accepted);

            var region0Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
                brainId,
                ShardId32.From(0, 0),
                neuronStart: 0,
                bufferCodes: new[] { 10, 11, 12 },
                enabledBitset: new byte[] { 0b0000_0111 },
                overlays: Array.Empty<SnapshotOverlayRecord>())));
            var region1Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
                brainId,
                ShardId32.From(1, 0),
                neuronStart: 0,
                bufferCodes: new[] { 20, 21, 22, 23 },
                enabledBitset: new byte[] { 0b0000_1111 },
                overlays: new[]
                {
                    new SnapshotOverlayRecord
                    {
                        FromAddress = SharedAddress32.From(1, 1).Value,
                        ToAddress = SharedAddress32.From(31, 1).Value,
                        StrengthCode = rebasedStrengthCode
                    }
                })));
            var region31Shard = root.Spawn(Props.FromProducer(() => new SnapshotShardProbe(
                brainId,
                ShardId32.From(31, 0),
                neuronStart: 0,
                bufferCodes: new[] { 30, 31 },
                enabledBitset: new byte[] { 0b0000_0011 },
                overlays: Array.Empty<SnapshotOverlayRecord>())));

            await root.RequestAsync<SendMessageAck>(region0Shard, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 0,
                ShardIndex = 0,
                ShardPid = PidLabel(region0Shard),
                NeuronStart = 0,
                NeuronCount = 3,
                PlacementEpoch = placement.PlacementEpoch
            }));
            await root.RequestAsync<SendMessageAck>(region1Shard, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 1,
                ShardIndex = 0,
                ShardPid = PidLabel(region1Shard),
                NeuronStart = 0,
                NeuronCount = 4,
                PlacementEpoch = placement.PlacementEpoch
            }));
            await root.RequestAsync<SendMessageAck>(region31Shard, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 31,
                ShardIndex = 0,
                ShardPid = PidLabel(region31Shard),
                NeuronStart = 0,
                NeuronCount = 2,
                PlacementEpoch = placement.PlacementEpoch
            }));

            var rebasedReady = await root.RequestAsync<BrainDefinitionReady>(
                hiveMind,
                new ExportBrainDefinition
                {
                    BrainId = brainId.ToProtoUuid(),
                    RebaseOverlays = true
                });

            Assert.NotNull(rebasedReady.BrainDef);
            Assert.True(rebasedReady.BrainDef.TryToSha256Hex(out var rebasedSha));

            var rebasedReadyRepeat = await root.RequestAsync<BrainDefinitionReady>(
                hiveMind,
                new ExportBrainDefinition
                {
                    BrainId = brainId.ToProtoUuid(),
                    RebaseOverlays = true
                });

            Assert.NotNull(rebasedReadyRepeat.BrainDef);
            Assert.True(rebasedReadyRepeat.BrainDef.TryToSha256Hex(out var repeatedSha));
            Assert.Equal(rebasedSha, repeatedSha);

            Assert.True(rebasedReady.BrainDef.TryToSha256Bytes(out var rebasedHashBytes));
            var rebasedStream = await store.TryOpenArtifactAsync(new Sha256Hash(rebasedHashBytes));
            Assert.NotNull(rebasedStream);

            byte[] rebasedBytes;
            await using (rebasedStream!)
            using (var ms = new MemoryStream())
            {
                await rebasedStream.CopyToAsync(ms);
                rebasedBytes = ms.ToArray();
            }

            var exportedStrengthCode = FindAxonStrengthCode(rebasedBytes, fromRegionId: 1, fromNeuronId: 1, toRegionId: 31, toNeuronId: 1);
            Assert.Equal(rebasedStrengthCode, exportedStrengthCode);
            Assert.NotEqual(baseStrengthCode, exportedStrengthCode);

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

    private static async Task<BrainInfo> WaitForBrainInfoAsync(
        IRootContext root,
        PID gateway,
        Guid brainId,
        Func<BrainInfo, bool> predicate,
        int timeoutMs)
    {
        using var cts = new CancellationTokenSource(timeoutMs);
        BrainInfo? last = null;
        while (!cts.IsCancellationRequested)
        {
            last = await root.RequestAsync<BrainInfo>(
                gateway,
                new BrainInfoRequest
                {
                    BrainId = brainId.ToProtoUuid()
                });
            if (predicate(last))
            {
                return last;
            }

            await Task.Delay(20, cts.Token);
        }

        throw new TimeoutException($"Brain info did not reach the expected homeostasis state. Last probability={last?.HomeostasisBaseProbability ?? -1f}.");
    }

    private static void AssertHomeostasisConfig(NbsHomeostasisConfig expected, NbsHomeostasisConfig actual)
    {
        Assert.Equal(expected.Enabled, actual.Enabled);
        Assert.Equal(expected.TargetMode, actual.TargetMode);
        Assert.Equal(expected.UpdateMode, actual.UpdateMode);
        Assert.Equal(expected.BaseProbability, actual.BaseProbability);
        Assert.Equal(expected.MinStepCodes, actual.MinStepCodes);
        Assert.Equal(expected.EnergyCouplingEnabled, actual.EnergyCouplingEnabled);
        Assert.Equal(expected.EnergyTargetScale, actual.EnergyTargetScale);
        Assert.Equal(expected.EnergyProbabilityScale, actual.EnergyProbabilityScale);
    }

    private static void AssertHomeostasisConfig(BrainInfo actual, NbsHomeostasisConfig expected)
    {
        Assert.Equal(expected.Enabled, actual.HomeostasisEnabled);
        Assert.Equal(expected.TargetMode, actual.HomeostasisTargetMode);
        Assert.Equal(expected.UpdateMode, actual.HomeostasisUpdateMode);
        Assert.Equal(expected.BaseProbability, actual.HomeostasisBaseProbability);
        Assert.Equal(expected.MinStepCodes, actual.HomeostasisMinStepCodes);
        Assert.Equal(expected.EnergyCouplingEnabled, actual.HomeostasisEnergyCouplingEnabled);
        Assert.Equal(expected.EnergyTargetScale, actual.HomeostasisEnergyTargetScale);
        Assert.Equal(expected.EnergyProbabilityScale, actual.HomeostasisEnergyProbabilityScale);
    }

    private sealed class RegisterBrainProbeActor : IActor
    {
        private readonly TaskCompletionSource<Nbn.Proto.Io.RegisterBrain> _registerBrain;

        public RegisterBrainProbeActor(TaskCompletionSource<Nbn.Proto.Io.RegisterBrain> registerBrain)
        {
            _registerBrain = registerBrain;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is Nbn.Proto.Io.RegisterBrain registerBrain)
            {
                _registerBrain.TrySetResult(registerBrain.Clone());
            }

            return Task.CompletedTask;
        }
    }

    private sealed record SendMessage(PID Target, object Message);
    private sealed record SendMessageAck;

    private sealed class SnapshotShardProbe : IActor
    {
        private readonly Guid _brainId;
        private readonly ShardId32 _shardId;
        private readonly int _neuronStart;
        private readonly int[] _bufferCodes;
        private readonly byte[] _enabledBitset;
        private readonly IReadOnlyList<SnapshotOverlayRecord> _overlays;

        public SnapshotShardProbe(
            Guid brainId,
            ShardId32 shardId,
            int neuronStart,
            int[] bufferCodes,
            byte[] enabledBitset,
            IReadOnlyList<SnapshotOverlayRecord> overlays)
        {
            _brainId = brainId;
            _shardId = shardId;
            _neuronStart = neuronStart;
            _bufferCodes = bufferCodes;
            _enabledBitset = enabledBitset;
            _overlays = overlays;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is SendMessage send)
            {
                context.Request(send.Target, send.Message);
                context.Respond(new SendMessageAck());
                return Task.CompletedTask;
            }

            if (context.Message is not CaptureShardSnapshot capture)
            {
                return Task.CompletedTask;
            }

            var response = new CaptureShardSnapshotAck
            {
                BrainId = _brainId.ToProtoUuid(),
                RegionId = (uint)_shardId.RegionId,
                ShardIndex = (uint)_shardId.ShardIndex,
                NeuronStart = (uint)_neuronStart,
                NeuronCount = (uint)_bufferCodes.Length,
                Success = capture.BrainId is not null
                          && capture.BrainId.TryToGuid(out var guid)
                          && guid == _brainId
                          && capture.RegionId == (uint)_shardId.RegionId
                          && capture.ShardIndex == (uint)_shardId.ShardIndex
            };

            if (!response.Success)
            {
                response.Error = "mismatch";
                context.Respond(response);
                return Task.CompletedTask;
            }

            response.BufferCodes.AddRange(_bufferCodes);
            response.EnabledBitset = Google.Protobuf.ByteString.CopyFrom(_enabledBitset);
            response.Overlays.Add(_overlays);
            context.Respond(response);
            return Task.CompletedTask;
        }
    }

    private static byte FindAxonStrengthCode(
        byte[] nbnBytes,
        int fromRegionId,
        int fromNeuronId,
        int toRegionId,
        int toNeuronId)
    {
        var header = NbnBinary.ReadNbnHeader(nbnBytes);
        var entry = header.Regions[fromRegionId];
        if (entry.NeuronSpan == 0)
        {
            throw new InvalidOperationException($"Region {fromRegionId} is not present.");
        }

        var section = NbnBinary.ReadNbnRegionSection(nbnBytes, entry.Offset);
        if ((uint)fromNeuronId >= (uint)section.NeuronRecords.Length)
        {
            throw new InvalidOperationException($"Neuron {fromRegionId}:{fromNeuronId} is out of range.");
        }

        var axonStart = 0;
        for (var neuron = 0; neuron < fromNeuronId; neuron++)
        {
            axonStart += section.NeuronRecords[neuron].AxonCount;
        }

        var axonCount = section.NeuronRecords[fromNeuronId].AxonCount;
        for (var offset = 0; offset < axonCount; offset++)
        {
            var axon = section.AxonRecords[axonStart + offset];
            if (axon.TargetRegionId == toRegionId && axon.TargetNeuronId == toNeuronId)
            {
                return axon.StrengthCode;
            }
        }

        throw new InvalidOperationException($"Axon route {fromRegionId}:{fromNeuronId} -> {toRegionId}:{toNeuronId} not found.");
    }

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private static IoOptions CreateIoOptions(string gatewayName)
    {
        return new IoOptions(
            BindHost: "127.0.0.1",
            Port: 0,
            AdvertisedHost: null,
            AdvertisedPort: null,
            GatewayName: gatewayName,
            ServerName: "nbn.io.tests",
            SettingsHost: null,
            SettingsPort: 0,
            SettingsName: "SettingsMonitor",
            HiveMindAddress: null,
            HiveMindName: null,
            ReproAddress: null,
            ReproName: null);
    }

    private static void PrimeEligibleWorker(IRootContext root, PID hiveMind)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nowMs,
            Workers =
            {
                BuildWorker(
                    Guid.NewGuid(),
                    isAlive: true,
                    isReady: true,
                    lastSeenMs: nowMs,
                    capabilityTimeMs: nowMs,
                    address: string.Empty,
                    rootActorName: "region-host")
            }
        });
    }

    private static ProtoSettings.WorkerReadinessCapability BuildWorker(
        Guid nodeId,
        bool isAlive,
        bool isReady,
        long lastSeenMs,
        long capabilityTimeMs,
        string address,
        string rootActorName)
        => new()
        {
            NodeId = nodeId.ToProtoUuid(),
            Address = address,
            RootActorName = rootActorName,
            IsAlive = isAlive,
            IsReady = isReady,
            LastSeenMs = (ulong)lastSeenMs,
            HasCapabilities = true,
            CapabilityTimeMs = (ulong)capabilityTimeMs,
            Capabilities = new ProtoSettings.NodeCapabilities
            {
                CpuCores = 8,
                RamFreeBytes = 8UL * 1024 * 1024 * 1024,
                RamTotalBytes = 16UL * 1024 * 1024 * 1024,
                StorageFreeBytes = 64UL * 1024 * 1024 * 1024,
                StorageTotalBytes = 128UL * 1024 * 1024 * 1024,
                HasGpu = true,
                VramFreeBytes = 8UL * 1024 * 1024 * 1024,
                VramTotalBytes = 16UL * 1024 * 1024 * 1024,
                CpuScore = 40f,
                GpuScore = 80f,
                CpuLimitPercent = 100,
                RamLimitPercent = 100,
                StorageLimitPercent = 100,
                GpuComputeLimitPercent = 100,
                GpuVramLimitPercent = 100
            }
        };

    private static HiveMindOptions CreateOptions()
        => new(
            BindHost: "127.0.0.1",
            Port: 0,
            AdvertisedHost: null,
            AdvertisedPort: null,
            TargetTickHz: 50f,
            MinTickHz: 10f,
            ComputeTimeoutMs: 500,
            DeliverTimeoutMs: 500,
            BackpressureDecay: 0.9f,
            BackpressureRecovery: 1.1f,
            LateBackpressureThreshold: 2,
            TimeoutRescheduleThreshold: 3,
            TimeoutPauseThreshold: 6,
            RescheduleMinTicks: 10,
            RescheduleMinMinutes: 1,
            RescheduleQuietMs: 50,
            RescheduleSimulatedMs: 50,
            AutoStart: false,
            EnableOpenTelemetry: false,
            EnableOtelMetrics: false,
            EnableOtelTraces: false,
            EnableOtelConsoleExporter: false,
            OtlpEndpoint: null,
            ServiceName: "nbn.hivemind.tests",
            SettingsDbPath: null,
            SettingsHost: null,
            SettingsPort: 0,
            SettingsName: "SettingsMonitor",
            IoAddress: null,
            IoName: null);
}
