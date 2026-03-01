using Microsoft.Data.Sqlite;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Tests.Format;
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
                NeuronCount = 3
            }));
            await root.RequestAsync<SendMessageAck>(region1Shard, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 1,
                ShardIndex = 0,
                ShardPid = PidLabel(region1Shard),
                NeuronStart = 0,
                NeuronCount = 4
            }));
            await root.RequestAsync<SendMessageAck>(region31Shard, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 31,
                ShardIndex = 0,
                ShardPid = PidLabel(region31Shard),
                NeuronStart = 0,
                NeuronCount = 2
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
                NeuronCount = 3
            }));
            await root.RequestAsync<SendMessageAck>(region1Shard, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 1,
                ShardIndex = 0,
                ShardPid = PidLabel(region1Shard),
                NeuronStart = 0,
                NeuronCount = 4
            }));
            await root.RequestAsync<SendMessageAck>(region31Shard, new SendMessage(hiveMind, new RegisterShard
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = 31,
                ShardIndex = 0,
                ShardPid = PidLabel(region31Shard),
                NeuronStart = 0,
                NeuronCount = 2
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
                HasGpu = true,
                VramFreeBytes = 8UL * 1024 * 1024 * 1024,
                CpuScore = 40f,
                GpuScore = 80f
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
