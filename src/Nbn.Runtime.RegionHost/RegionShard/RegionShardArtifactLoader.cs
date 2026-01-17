using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
using Nbn.Proto;
using Proto;

namespace Nbn.Runtime.RegionHost;

public sealed record RegionShardLoadResult(
    RegionShardState State,
    NbnHeaderV2 Header,
    NbsHeaderV2? SnapshotHeader);

public static class RegionShardArtifactLoader
{
    public static async Task<RegionShardLoadResult> LoadAsync(
        IArtifactStore store,
        ArtifactRef nbnRef,
        ArtifactRef? nbsRef,
        int regionId,
        int neuronStart,
        int neuronCount,
        CancellationToken cancellationToken = default)
    {
        if (store is null)
        {
            throw new ArgumentNullException(nameof(store));
        }

        if (nbnRef is null)
        {
            throw new ArgumentNullException(nameof(nbnRef));
        }

        var nbnBytes = await ReadArtifactAsync(store, nbnRef, cancellationToken);
        var header = NbnBinary.ReadNbnHeader(nbnBytes);

        if (regionId < NbnConstants.RegionMinId || regionId > NbnConstants.RegionMaxId)
        {
            throw new ArgumentOutOfRangeException(nameof(regionId), regionId, "Region id must be between 0 and 31.");
        }

        var entry = header.Regions[regionId];
        if (entry.NeuronSpan == 0)
        {
            throw new InvalidOperationException($"Region {regionId} is not present in the NBN definition.");
        }

        var regionSection = NbnBinary.ReadNbnRegionSection(nbnBytes, entry.Offset);

        NbsHeaderV2? nbsHeader = null;
        NbsRegionSection? nbsRegion = null;
        NbsOverlaySection? nbsOverlays = null;

        if (nbsRef is not null)
        {
            var nbsBytes = await ReadArtifactAsync(store, nbsRef, cancellationToken);
            nbsHeader = NbnBinary.ReadNbsHeader(nbsBytes);
            var read = ReadNbsRegions(nbsBytes, header, nbsHeader);
            if (read.Regions.TryGetValue(regionId, out var region))
            {
                nbsRegion = region;
            }

            nbsOverlays = read.Overlays;
        }

        var state = BuildState(header, regionSection, nbsHeader, nbsRegion, nbsOverlays, regionId, neuronStart, neuronCount);
        return new RegionShardLoadResult(state, header, nbsHeader);
    }

    public static async Task<Props> CreatePropsAsync(
        IArtifactStore store,
        ArtifactRef nbnRef,
        ArtifactRef? nbsRef,
        int regionId,
        int neuronStart,
        int neuronCount,
        RegionShardActorConfig config,
        CancellationToken cancellationToken = default)
    {
        var load = await LoadAsync(store, nbnRef, nbsRef, regionId, neuronStart, neuronCount, cancellationToken);
        return Props.FromProducer(() => new RegionShardActor(load.State, config));
    }

    private static RegionShardState BuildState(
        NbnHeaderV2 header,
        NbnRegionSection region,
        NbsHeaderV2? nbsHeader,
        NbsRegionSection? nbsRegion,
        NbsOverlaySection? overlays,
        int regionId,
        int neuronStart,
        int neuronCount)
    {
        var regionSpan = (int)region.NeuronSpan;
        if (neuronStart < 0 || neuronStart >= regionSpan)
        {
            throw new ArgumentOutOfRangeException(nameof(neuronStart), neuronStart, "Neuron start must be within region span.");
        }

        if (neuronCount <= 0)
        {
            neuronCount = regionSpan - neuronStart;
        }

        if (neuronStart + neuronCount > regionSpan)
        {
            throw new ArgumentOutOfRangeException(nameof(neuronCount), neuronCount, "Neuron range exceeds region span.");
        }

        var totalAxons = 0;
        for (var i = neuronStart; i < neuronStart + neuronCount; i++)
        {
            totalAxons += region.NeuronRecords[i].AxonCount;
        }

        var buffer = new float[neuronCount];
        var enabled = new bool[neuronCount];
        var exists = new bool[neuronCount];
        var accum = new byte[neuronCount];
        var activation = new byte[neuronCount];
        var reset = new byte[neuronCount];
        var paramA = new float[neuronCount];
        var paramB = new float[neuronCount];
        var preThreshold = new float[neuronCount];
        var activationThreshold = new float[neuronCount];
        var axonCounts = new ushort[neuronCount];
        var axonStartOffsets = new int[neuronCount];

        var axonTargetRegions = new byte[totalAxons];
        var axonTargetNeurons = new int[totalAxons];
        var axonStrengths = new float[totalAxons];

        var overlayMap = BuildOverlayMap(overlays, regionId, neuronStart, neuronCount);

        var nbsBufferCodes = nbsRegion?.BufferCodes;
        var nbsEnabled = nbsRegion?.EnabledBitset;
        var bufferMap = nbsHeader?.BufferMap ?? QuantizationSchemas.DefaultBuffer;

        var regionAxonStarts = BuildAxonStarts(region.NeuronRecords);

        var axonCursor = 0;
        for (var local = 0; local < neuronCount; local++)
        {
            var global = neuronStart + local;
            var neuron = region.NeuronRecords[global];
            exists[local] = neuron.Exists;
            accum[local] = neuron.AccumulationFunctionId;
            activation[local] = neuron.ActivationFunctionId;
            reset[local] = neuron.ResetFunctionId;
            paramA[local] = header.Quantization.ParamA.Decode(neuron.ParamACode, 6);
            paramB[local] = header.Quantization.ParamB.Decode(neuron.ParamBCode, 6);
            preThreshold[local] = header.Quantization.PreActivationThreshold.Decode(neuron.PreActivationThresholdCode, 6);
            activationThreshold[local] = header.Quantization.ActivationThreshold.Decode(neuron.ActivationThresholdCode, 6);

            axonCounts[local] = neuron.AxonCount;
            axonStartOffsets[local] = axonCursor;

            if (nbsBufferCodes is not null && global < nbsBufferCodes.Length)
            {
                var code = unchecked((ushort)nbsBufferCodes[global]);
                buffer[local] = bufferMap.Decode(code, 16);
            }
            else
            {
                buffer[local] = 0f;
            }

            if (nbsEnabled is not null && global < regionSpan)
            {
                var byteIndex = global / 8;
                var bitIndex = global % 8;
                if (byteIndex < nbsEnabled.Length)
                {
                    enabled[local] = (nbsEnabled[byteIndex] & (1 << bitIndex)) != 0;
                }
            }
            else
            {
                enabled[local] = neuron.Exists;
            }

            if (!neuron.Exists)
            {
                enabled[local] = false;
            }

            var axonStart = regionAxonStarts[global];
            for (var a = 0; a < neuron.AxonCount; a++)
            {
                var axonRecord = region.AxonRecords[axonStart + a];
                var targetRegion = axonRecord.TargetRegionId;
                var targetNeuron = axonRecord.TargetNeuronId;
                var strengthCode = axonRecord.StrengthCode;

                if (overlayMap.TryGetValue((global, targetRegion, targetNeuron), out var overlayStrength))
                {
                    strengthCode = overlayStrength;
                }

                axonTargetRegions[axonCursor] = targetRegion;
                axonTargetNeurons[axonCursor] = targetNeuron;
                axonStrengths[axonCursor] = header.Quantization.Strength.Decode(strengthCode, 5);
                axonCursor++;
            }
        }

        var axons = new RegionShardAxons(axonTargetRegions, axonTargetNeurons, axonStrengths);
        return new RegionShardState(regionId, neuronStart, neuronCount, buffer, enabled, exists, accum, activation, reset,
            paramA, paramB, preThreshold, activationThreshold, axonCounts, axonStartOffsets, axons);
    }

    private static Dictionary<(int FromNeuron, byte ToRegion, int ToNeuron), byte> BuildOverlayMap(
        NbsOverlaySection? overlays,
        int regionId,
        int neuronStart,
        int neuronCount)
    {
        var map = new Dictionary<(int, byte, int), byte>();
        if (overlays is null || overlays.Records.Length == 0)
        {
            return map;
        }

        var neuronEnd = neuronStart + neuronCount;

        foreach (var record in overlays.Records)
        {
            DecodeAddress(record.FromAddress, out var fromRegion, out var fromNeuron);
            if (fromRegion != regionId)
            {
                continue;
            }

            if (fromNeuron < neuronStart || fromNeuron >= neuronEnd)
            {
                continue;
            }

            DecodeAddress(record.ToAddress, out var toRegion, out var toNeuron);
            map[(fromNeuron, (byte)toRegion, toNeuron)] = record.StrengthCode;
        }

        return map;
    }

    private static int[] BuildAxonStarts(IReadOnlyList<NeuronRecord> neurons)
    {
        var starts = new int[neurons.Count];
        var cursor = 0;
        for (var i = 0; i < neurons.Count; i++)
        {
            starts[i] = cursor;
            cursor += neurons[i].AxonCount;
        }

        return starts;
    }

    private static void DecodeAddress(uint address, out int regionId, out int neuronId)
    {
        regionId = (int)(address >> NbnConstants.AddressNeuronBits);
        neuronId = (int)(address & NbnConstants.AddressNeuronMask);
    }

    private static async Task<byte[]> ReadArtifactAsync(IArtifactStore store, ArtifactRef reference, CancellationToken cancellationToken)
    {
        if (reference.Sha256 is null)
        {
            throw new ArgumentException("ArtifactRef is missing sha256.", nameof(reference));
        }

        var bytes = reference.Sha256.ToByteArray();
        var hash = new Sha256Hash(bytes);
        var stream = await store.TryOpenArtifactAsync(hash, cancellationToken);
        if (stream is null)
        {
            throw new InvalidOperationException($"Artifact {hash} not found.");
        }

        await using (stream)
        {
            var capacity = reference.SizeBytes > 0 && reference.SizeBytes < int.MaxValue ? (int)reference.SizeBytes : 0;
            using var ms = capacity > 0 ? new MemoryStream(capacity) : new MemoryStream();
            await stream.CopyToAsync(ms, cancellationToken);
            return ms.ToArray();
        }
    }

    private static (Dictionary<int, NbsRegionSection> Regions, NbsOverlaySection? Overlays) ReadNbsRegions(
        byte[] nbsBytes,
        NbnHeaderV2 nbnHeader,
        NbsHeaderV2 nbsHeader)
    {
        var offset = NbnBinary.NbsHeaderBytes;
        var includeEnabled = nbsHeader.EnabledBitsetIncluded;
        var regions = new Dictionary<int, NbsRegionSection>();

        for (var regionId = 0; regionId < nbnHeader.Regions.Length; regionId++)
        {
            var entry = nbnHeader.Regions[regionId];
            if (entry.NeuronSpan == 0)
            {
                continue;
            }

            var section = NbnBinary.ReadNbsRegionSection(nbsBytes, offset, includeEnabled);
            regions[regionId] = section;
            offset += section.ByteLength;
        }

        NbsOverlaySection? overlays = null;
        if (nbsHeader.AxonOverlayIncluded)
        {
            overlays = NbnBinary.ReadNbsOverlaySection(nbsBytes, offset);
        }

        return (regions, overlays);
    }
}
