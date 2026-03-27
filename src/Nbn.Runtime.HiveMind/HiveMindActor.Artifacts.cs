using Nbn.Runtime.Artifacts;
using Nbn.Runtime.IO;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Nbn.Shared.Format;
using Nbn.Shared.Validation;
using Proto;
using ProtoIo = Nbn.Proto.Io;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private bool HandleArtifactMessage(IContext context)
    {
        switch (context.Message)
        {
            case ProtoIo.ExportBrainDefinition message:
                HandleExportBrainDefinition(context, message);
                return true;
            case ProtoIo.RequestSnapshot message:
                HandleRequestSnapshot(context, message);
                return true;
            default:
                return false;
        }
    }

    private void HandleExportBrainDefinition(IContext context, ProtoIo.ExportBrainDefinition message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            context.Respond(new ProtoIo.BrainDefinitionReady());
            return;
        }

        if (!_brains.TryGetValue(brainId, out var brain) || brain.BaseDefinition is null)
        {
            context.Respond(new ProtoIo.BrainDefinitionReady
            {
                BrainId = brainId.ToProtoUuid()
            });
            return;
        }

        if (!message.RebaseOverlays || !HasArtifactRef(brain.BaseDefinition) || brain.Shards.Count == 0)
        {
            context.Respond(new ProtoIo.BrainDefinitionReady
            {
                BrainId = brainId.ToProtoUuid(),
                BrainDef = brain.BaseDefinition
            });
            return;
        }

        var fallbackDefinition = brain.BaseDefinition!;
        var storeRootPath = ResolveArtifactRoot(fallbackDefinition.StoreUri);
        var request = new RebasedDefinitionBuildRequest(
            brain.BrainId,
            fallbackDefinition,
            _lastCompletedTickId,
            new Dictionary<ShardId32, PID>(brain.Shards),
            storeRootPath,
            string.IsNullOrWhiteSpace(fallbackDefinition.StoreUri) ? storeRootPath : fallbackDefinition.StoreUri);

        var rebaseTask = BuildAndStoreRebasedDefinitionAsync(context.System, request);
        context.ReenterAfter(rebaseTask, task =>
        {
            if (task is { IsCompletedSuccessfully: true } && HasArtifactRef(task.Result))
            {
                context.Respond(new ProtoIo.BrainDefinitionReady
                {
                    BrainId = brainId.ToProtoUuid(),
                    BrainDef = task.Result
                });
                return Task.CompletedTask;
            }

            if (task.Exception is not null)
            {
                LogError($"Rebased export failed for brain {brainId}: {task.Exception.GetBaseException().Message}");
            }

            context.Respond(new ProtoIo.BrainDefinitionReady
            {
                BrainId = brainId.ToProtoUuid(),
                BrainDef = fallbackDefinition
            });
            return Task.CompletedTask;
        });
    }

    private void HandleRequestSnapshot(IContext context, ProtoIo.RequestSnapshot message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            context.Respond(new ProtoIo.SnapshotReady());
            return;
        }

        if (!_brains.TryGetValue(brainId, out var brain))
        {
            context.Respond(new ProtoIo.SnapshotReady
            {
                BrainId = brainId.ToProtoUuid()
            });
            return;
        }

        if (!HasArtifactRef(brain.BaseDefinition) || brain.Shards.Count == 0)
        {
            RespondSnapshot(context, brainId, brain.LastSnapshot);
            return;
        }

        var snapshotCostEnergyEnabled = ResolveSnapshotCostEnergyEnabled(brain, message);
        var snapshotPlasticityEnabled = ResolveSnapshotPlasticityEnabled(brain, message);
        var storeRootPath = ResolveArtifactRoot(brain.BaseDefinition!.StoreUri);
        var request = new SnapshotBuildRequest(
            brain.BrainId,
            brain.BaseDefinition!,
            _lastCompletedTickId,
            message.HasRuntimeState ? message.EnergyRemaining : 0L,
            snapshotCostEnergyEnabled,
            snapshotCostEnergyEnabled,
            snapshotPlasticityEnabled,
            brain.HomeostasisEnabled,
            brain.HomeostasisTargetMode,
            brain.HomeostasisUpdateMode,
            brain.HomeostasisBaseProbability,
            brain.HomeostasisMinStepCodes,
            brain.HomeostasisEnergyCouplingEnabled,
            brain.HomeostasisEnergyTargetScale,
            brain.HomeostasisEnergyProbabilityScale,
            new Dictionary<ShardId32, PID>(brain.Shards),
            storeRootPath,
            string.IsNullOrWhiteSpace(brain.BaseDefinition.StoreUri) ? storeRootPath : brain.BaseDefinition.StoreUri);

        var snapshotTask = BuildAndStoreSnapshotAsync(context.System, request);
        context.ReenterAfter(snapshotTask, task =>
        {
            if (task is { IsCompletedSuccessfully: true } && task.Result is not null)
            {
                var snapshot = task.Result;
                if (_brains.TryGetValue(brainId, out var liveBrain))
                {
                    liveBrain.LastSnapshot = snapshot;
                    if (_ioPid is not null)
                    {
                        var ioPid = ResolveSendTargetPid(context, _ioPid);
                        context.Send(ioPid, new UpdateBrainSnapshot(brainId, snapshot));
                    }

                    RegisterBrainWithIo(context, liveBrain, force: true);
                }

                context.Respond(new ProtoIo.SnapshotReady
                {
                    BrainId = brainId.ToProtoUuid(),
                    Snapshot = snapshot
                });
                return Task.CompletedTask;
            }

            if (task.Exception is not null)
            {
                LogError($"Live snapshot generation failed for brain {brainId}: {task.Exception.GetBaseException().Message}");
            }

            if (_brains.TryGetValue(brainId, out var liveFallback))
            {
                RespondSnapshot(context, brainId, liveFallback.LastSnapshot);
            }
            else
            {
                context.Respond(new ProtoIo.SnapshotReady
                {
                    BrainId = brainId.ToProtoUuid()
                });
            }

            return Task.CompletedTask;
        });
    }

    private static async Task<List<ProtoControl.CaptureShardSnapshotAck>> CaptureShardSnapshotsAsync(
        ActorSystem system,
        Guid brainId,
        ulong tickId,
        IReadOnlyDictionary<ShardId32, PID> shards)
    {
        var captures = new List<ProtoControl.CaptureShardSnapshotAck>(shards.Count);
        foreach (var entry in shards.OrderBy(static pair => pair.Key.RegionId).ThenBy(static pair => pair.Key.ShardIndex))
        {
            var capture = await system.Root.RequestAsync<ProtoControl.CaptureShardSnapshotAck>(
                    entry.Value,
                    new ProtoControl.CaptureShardSnapshot
                    {
                        BrainId = brainId.ToProtoUuid(),
                        RegionId = (uint)entry.Key.RegionId,
                        ShardIndex = (uint)entry.Key.ShardIndex,
                        TickId = tickId
                    },
                    SnapshotShardRequestTimeout)
                .ConfigureAwait(false);

            if (capture is null)
            {
                throw new InvalidOperationException($"Snapshot capture returned null for shard {entry.Key}.");
            }

            if (!capture.Success)
            {
                var error = string.IsNullOrWhiteSpace(capture.Error) ? "unknown" : capture.Error;
                throw new InvalidOperationException($"Snapshot capture failed for shard {entry.Key}: {error}");
            }

            captures.Add(capture);
        }

        return captures;
    }

    private static async Task<Nbn.Proto.ArtifactRef?> BuildAndStoreRebasedDefinitionAsync(ActorSystem system, RebasedDefinitionBuildRequest request)
    {
        var store = new ArtifactStoreResolver(new ArtifactStoreResolverOptions(request.StoreRootPath))
            .Resolve(request.StoreUri);
        if (!request.BaseDefinition.TryToSha256Bytes(out var baseHashBytes))
        {
            throw new InvalidOperationException("Base definition sha256 is required to build rebased exports.");
        }

        var baseHash = new Sha256Hash(baseHashBytes);
        var nbnStream = await store.TryOpenArtifactAsync(baseHash).ConfigureAwait(false);
        if (nbnStream is null)
        {
            throw new InvalidOperationException($"Base NBN artifact {baseHash.ToHex()} not found in store.");
        }

        byte[] nbnBytes;
        await using (nbnStream)
        using (var ms = new MemoryStream())
        {
            await nbnStream.CopyToAsync(ms).ConfigureAwait(false);
            nbnBytes = ms.ToArray();
        }

        var baseHeader = NbnBinary.ReadNbnHeader(nbnBytes);
        var captures = await CaptureShardSnapshotsAsync(system, request.BrainId, request.SnapshotTickId, request.Shards).ConfigureAwait(false);
        var (_, overlays) = BuildSnapshotSections(baseHeader, captures);
        HiveMindTelemetry.RecordRebaseOverlayRecords(request.BrainId, overlays.Count);
        if (overlays.Count == 0)
        {
            return request.BaseDefinition;
        }

        var rebasedSections = RebaseDefinitionWithOverlays(baseHeader, nbnBytes, overlays);
        var rebasedHeader = CloneHeader(baseHeader);
        var validation = NbnBinaryValidator.ValidateNbn(rebasedHeader, rebasedSections);
        if (!validation.IsValid)
        {
            var errors = string.Join("; ", validation.Issues.Select(static issue => issue.ToString()));
            throw new InvalidOperationException($"Generated rebased definition failed validation: {errors}");
        }

        var bytes = NbnBinary.WriteNbn(rebasedHeader, rebasedSections);
        await using var rebasedStream = new MemoryStream(bytes, writable: false);
        var manifest = await store.StoreAsync(rebasedStream, "application/x-nbn").ConfigureAwait(false);
        return manifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifest.ByteLength, "application/x-nbn", request.StoreUri);
    }

    private static List<NbnRegionSection> RebaseDefinitionWithOverlays(
        NbnHeaderV2 baseHeader,
        ReadOnlySpan<byte> nbnBytes,
        IReadOnlyList<NbsOverlayRecord> overlays)
    {
        var sectionMap = new Dictionary<int, NbnRegionSection>();
        for (var regionId = 0; regionId < baseHeader.Regions.Length; regionId++)
        {
            var entry = baseHeader.Regions[regionId];
            if (entry.NeuronSpan == 0)
            {
                continue;
            }

            var source = NbnBinary.ReadNbnRegionSection(nbnBytes, entry.Offset);
            sectionMap[regionId] = CloneRegionSection(source);
        }

        var routeMap = BuildAxonRouteIndex(sectionMap);
        foreach (var overlay in overlays)
        {
            var routeKey = (overlay.FromAddress, overlay.ToAddress);
            if (!routeMap.TryGetValue(routeKey, out var location))
            {
                throw new InvalidOperationException($"Overlay route {overlay.FromAddress}->{overlay.ToAddress} does not exist in the base definition.");
            }

            var section = sectionMap[location.RegionId];
            var axon = section.AxonRecords[location.AxonIndex];
            var strengthCode = (byte)Math.Clamp((int)overlay.StrengthCode, 0, 31);
            if (axon.StrengthCode == strengthCode)
            {
                continue;
            }

            section.AxonRecords[location.AxonIndex] = new Nbn.Shared.Packing.AxonRecord(strengthCode, axon.TargetNeuronId, axon.TargetRegionId);
        }

        return sectionMap
            .OrderBy(static pair => pair.Key)
            .Select(static pair => pair.Value)
            .ToList();
    }

    private static Dictionary<(uint From, uint To), RebasedAxonLocation> BuildAxonRouteIndex(IReadOnlyDictionary<int, NbnRegionSection> sections)
    {
        var routeMap = new Dictionary<(uint From, uint To), RebasedAxonLocation>();
        foreach (var pair in sections.OrderBy(static item => item.Key))
        {
            var section = pair.Value;
            var axonCursor = 0;
            for (var neuronId = 0; neuronId < section.NeuronRecords.Length; neuronId++)
            {
                var fromAddress = Nbn.Shared.Addressing.Address32.From(section.RegionId, neuronId).Value;
                var axonCount = section.NeuronRecords[neuronId].AxonCount;
                for (var axonOffset = 0; axonOffset < axonCount; axonOffset++)
                {
                    var axonIndex = axonCursor + axonOffset;
                    if ((uint)axonIndex >= (uint)section.AxonRecords.Length)
                    {
                        throw new InvalidOperationException($"Axon index {axonIndex} out of range for region {section.RegionId}.");
                    }

                    var axon = section.AxonRecords[axonIndex];
                    var toAddress = Nbn.Shared.Addressing.Address32.From(axon.TargetRegionId, axon.TargetNeuronId).Value;
                    if (!routeMap.TryAdd((fromAddress, toAddress), new RebasedAxonLocation(pair.Key, axonIndex)))
                    {
                        throw new InvalidOperationException($"Duplicate axon route {fromAddress}->{toAddress} in base definition.");
                    }
                }

                axonCursor += axonCount;
            }

            if (axonCursor != section.AxonRecords.Length)
            {
                throw new InvalidOperationException($"Region {section.RegionId} axon traversal mismatch.");
            }
        }

        return routeMap;
    }

    private static NbnRegionSection CloneRegionSection(NbnRegionSection section)
    {
        var checkpoints = (ulong[])section.Checkpoints.Clone();
        var neurons = (Nbn.Shared.Packing.NeuronRecord[])section.NeuronRecords.Clone();
        var axons = (Nbn.Shared.Packing.AxonRecord[])section.AxonRecords.Clone();
        return new NbnRegionSection(
            section.RegionId,
            section.NeuronSpan,
            section.TotalAxons,
            section.Stride,
            section.CheckpointCount,
            checkpoints,
            neurons,
            axons);
    }

    private static NbnHeaderV2 CloneHeader(NbnHeaderV2 header)
    {
        var regions = new NbnRegionDirectoryEntry[header.Regions.Length];
        Array.Copy(header.Regions, regions, header.Regions.Length);
        return new NbnHeaderV2(
            header.Magic,
            header.Version,
            header.Endianness,
            header.HeaderBytesPow2,
            header.BrainSeed,
            header.AxonStride,
            header.Flags,
            header.Quantization,
            regions);
    }

    private readonly record struct RebasedAxonLocation(int RegionId, int AxonIndex);

    private static async Task<Nbn.Proto.ArtifactRef?> BuildAndStoreSnapshotAsync(ActorSystem system, SnapshotBuildRequest request)
    {
        var store = new ArtifactStoreResolver(new ArtifactStoreResolverOptions(request.StoreRootPath))
            .Resolve(request.StoreUri);
        if (!request.BaseDefinition.TryToSha256Bytes(out var baseHashBytes))
        {
            throw new InvalidOperationException("Base definition sha256 is required to build snapshots.");
        }

        var baseHash = new Sha256Hash(baseHashBytes);
        var nbnStream = await store.TryOpenArtifactAsync(baseHash).ConfigureAwait(false);
        if (nbnStream is null)
        {
            throw new InvalidOperationException($"Base NBN artifact {baseHash.ToHex()} not found in store.");
        }

        byte[] nbnBytes;
        await using (nbnStream)
        using (var ms = new MemoryStream())
        {
            await nbnStream.CopyToAsync(ms).ConfigureAwait(false);
            nbnBytes = ms.ToArray();
        }

        var baseHeader = NbnBinary.ReadNbnHeader(nbnBytes);
        var captures = await CaptureShardSnapshotsAsync(system, request.BrainId, request.SnapshotTickId, request.Shards).ConfigureAwait(false);

        var (regions, overlays) = BuildSnapshotSections(baseHeader, captures);
        HiveMindTelemetry.RecordSnapshotOverlayRecords(request.BrainId, overlays.Count);
        var flags = 0x1u;
        if (overlays.Count > 0)
        {
            flags |= 0x2u;
        }

        if (request.CostEnabled)
        {
            flags |= 0x4u;
        }

        if (request.EnergyEnabled)
        {
            flags |= 0x8u;
        }

        if (request.PlasticityEnabled)
        {
            flags |= 0x10u;
        }

        var header = new NbsHeaderV2(
            magic: "NBS2",
            version: 2,
            endianness: 1,
            headerBytesPow2: 9,
            brainId: request.BrainId,
            snapshotTickId: request.SnapshotTickId,
            timestampMs: (ulong)NowMs(),
            energyRemaining: request.EnergyRemaining,
            baseNbnSha256: baseHashBytes,
            flags: flags,
            bufferMap: SnapshotBufferQuantization,
            homeostasisConfig: new NbsHomeostasisConfig(
                request.HomeostasisEnabled,
                request.HomeostasisTargetMode,
                request.HomeostasisUpdateMode,
                request.HomeostasisBaseProbability,
                request.HomeostasisMinStepCodes,
                request.HomeostasisEnergyCouplingEnabled,
                request.HomeostasisEnergyTargetScale,
                request.HomeostasisEnergyProbabilityScale));

        NbsOverlaySection? overlaySection = null;
        if (overlays.Count > 0)
        {
            overlaySection = new NbsOverlaySection(overlays.ToArray(), NbnBinary.GetNbsOverlaySectionSize(overlays.Count));
        }

        var validation = NbnBinaryValidator.ValidateNbs(header, regions, overlaySection);
        if (!validation.IsValid)
        {
            var errors = string.Join("; ", validation.Issues.Select(static issue => issue.ToString()));
            throw new InvalidOperationException($"Generated snapshot failed validation: {errors}");
        }

        var bytes = NbnBinary.WriteNbs(header, regions, overlays);
        await using var snapshotStream = new MemoryStream(bytes, writable: false);
        var manifest = await store.StoreAsync(snapshotStream, "application/x-nbs").ConfigureAwait(false);

        return manifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifest.ByteLength, "application/x-nbs", request.StoreUri);
    }

    private static (List<NbsRegionSection> Regions, List<NbsOverlayRecord> Overlays) BuildSnapshotSections(
        NbnHeaderV2 baseHeader,
        IReadOnlyList<ProtoControl.CaptureShardSnapshotAck> captures)
    {
        var regions = new Dictionary<int, SnapshotRegionBuffer>();
        for (var regionId = 0; regionId < baseHeader.Regions.Length; regionId++)
        {
            var span = (int)baseHeader.Regions[regionId].NeuronSpan;
            if (span == 0)
            {
                continue;
            }

            regions[regionId] = new SnapshotRegionBuffer(span);
        }

        var overlayMap = new Dictionary<(uint From, uint To), byte>();
        foreach (var capture in captures)
        {
            var regionId = (int)capture.RegionId;
            if (!regions.TryGetValue(regionId, out var region))
            {
                throw new InvalidOperationException($"Capture returned unknown region {regionId}.");
            }

            var neuronStart = checked((int)capture.NeuronStart);
            var neuronCount = checked((int)capture.NeuronCount);
            if (neuronCount != capture.BufferCodes.Count)
            {
                throw new InvalidOperationException($"Capture buffer count mismatch for region {regionId}: expected {neuronCount}, got {capture.BufferCodes.Count}.");
            }

            var enabledBytes = capture.EnabledBitset is null ? Array.Empty<byte>() : capture.EnabledBitset.ToByteArray();
            var expectedEnabledBytes = (neuronCount + 7) / 8;
            if (enabledBytes.Length != expectedEnabledBytes)
            {
                throw new InvalidOperationException($"Capture enabled bitset length mismatch for region {regionId}: expected {expectedEnabledBytes}, got {enabledBytes.Length}.");
            }

            for (var i = 0; i < neuronCount; i++)
            {
                var globalNeuron = neuronStart + i;
                if ((uint)globalNeuron >= (uint)region.BufferCodes.Length)
                {
                    throw new InvalidOperationException($"Capture neuron index {globalNeuron} is out of range for region {regionId}.");
                }

                if (region.Assigned[globalNeuron])
                {
                    throw new InvalidOperationException($"Capture overlap detected for region {regionId} neuron {globalNeuron}.");
                }

                region.Assigned[globalNeuron] = true;
                var code = capture.BufferCodes[i];
                code = Math.Clamp(code, 0, ushort.MaxValue);
                region.BufferCodes[globalNeuron] = unchecked((short)(ushort)code);

                if ((enabledBytes[i / 8] & (1 << (i % 8))) != 0)
                {
                    region.EnabledBitset[globalNeuron / 8] |= (byte)(1 << (globalNeuron % 8));
                }
            }

            foreach (var overlay in capture.Overlays)
            {
                var strengthCode = (byte)Math.Clamp((int)overlay.StrengthCode, 0, 31);
                overlayMap[(overlay.FromAddress, overlay.ToAddress)] = strengthCode;
            }
        }

        var regionSections = new List<NbsRegionSection>(regions.Count);
        foreach (var pair in regions.OrderBy(static item => item.Key))
        {
            var regionId = pair.Key;
            var region = pair.Value;
            if (region.Assigned.Any(static assigned => !assigned))
            {
                throw new InvalidOperationException($"Capture did not fully cover region {regionId}.");
            }

            regionSections.Add(new NbsRegionSection((byte)regionId, (uint)region.BufferCodes.Length, region.BufferCodes, region.EnabledBitset));
        }

        var overlayRecords = overlayMap
            .OrderBy(static item => item.Key.From)
            .ThenBy(static item => item.Key.To)
            .Select(static item => new NbsOverlayRecord(item.Key.From, item.Key.To, item.Value))
            .ToList();

        return (regionSections, overlayRecords);
    }

    private void RespondSnapshot(IContext context, Guid brainId, Nbn.Proto.ArtifactRef? snapshot)
    {
        if (HasArtifactRef(snapshot))
        {
            context.Respond(new ProtoIo.SnapshotReady
            {
                BrainId = brainId.ToProtoUuid(),
                Snapshot = snapshot
            });
            return;
        }

        context.Respond(new ProtoIo.SnapshotReady
        {
            BrainId = brainId.ToProtoUuid()
        });
    }

    private static bool HasArtifactRef(Nbn.Proto.ArtifactRef? reference)
        => reference is not null
           && reference.Sha256 is not null
           && reference.Sha256.Value is not null
           && reference.Sha256.Value.Length == 32;

    private static string ArtifactLabel(Nbn.Proto.ArtifactRef? reference)
    {
        if (!HasArtifactRef(reference))
        {
            return "missing";
        }

        return reference!.TryToSha256Hex(out var sha)
            ? sha[..Math.Min(12, sha.Length)]
            : "present";
    }

    private static string ResolveArtifactRoot(string? storeUri)
    {
        if (!string.IsNullOrWhiteSpace(storeUri))
        {
            if (Uri.TryCreate(storeUri, UriKind.Absolute, out var uri) && uri.IsFile)
            {
                return uri.LocalPath;
            }

            if (!storeUri.Contains("://", StringComparison.Ordinal))
            {
                return storeUri;
            }
        }

        var envRoot = Environment.GetEnvironmentVariable("NBN_ARTIFACT_ROOT");
        if (!string.IsNullOrWhiteSpace(envRoot))
        {
            return envRoot;
        }

        return ArtifactStoreResolverOptions.ResolveDefaultArtifactRootPath();
    }

    private static ArtifactStoreResolver CreateArtifactStoreResolver(string? storeUri)
        => new(new ArtifactStoreResolverOptions(ResolveArtifactRoot(storeUri)));
}
