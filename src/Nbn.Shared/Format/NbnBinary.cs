using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;

namespace Nbn.Shared.Format;

public static class NbnBinary
{
    public const int NbnHeaderBytes = 1024;
    public const int NbsHeaderBytes = 512;

    private const int NbnRegionDirectoryOffset = 0x100;
    private const int NbnRegionDirectoryEntryBytes = 24;
    private const int NbnQuantizationOffset = 0x020;
    private const int NbnQuantizationFieldBytes = 16;

    public static NbnHeaderV2 ReadNbnHeader(ReadOnlySpan<byte> data)
    {
        if (data.Length < NbnHeaderBytes)
        {
            throw new ArgumentException("Data is smaller than the NBN header.", nameof(data));
        }

        var magic = Encoding.ASCII.GetString(data.Slice(0, 4));
        var version = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(0x004, 2));
        var endianness = data[0x006];
        var headerPow2 = data[0x007];
        var brainSeed = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(0x008, 8));
        var axonStride = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(0x010, 4));
        var flags = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(0x014, 4));

        var quantization = ReadNbnQuantization(data);
        var regions = ReadRegionDirectory(data);

        return new NbnHeaderV2(
            magic,
            version,
            endianness,
            headerPow2,
            brainSeed,
            axonStride,
            flags,
            quantization,
            regions);
    }

    public static void WriteNbnHeader(Span<byte> destination, NbnHeaderV2 header)
    {
        if (destination.Length < NbnHeaderBytes)
        {
            throw new ArgumentException("Destination span is too small for NBN header.", nameof(destination));
        }

        destination.Slice(0, NbnHeaderBytes).Clear();

        var magic = header.Magic.Length == 4 ? header.Magic : "NBN2";
        Encoding.ASCII.GetBytes(magic.AsSpan(0, 4), destination.Slice(0, 4));
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(0x004, 2), header.Version);
        destination[0x006] = header.Endianness;
        destination[0x007] = header.HeaderBytesPow2;
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(0x008, 8), header.BrainSeed);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0x010, 4), header.AxonStride);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0x014, 4), header.Flags);

        WriteNbnQuantization(destination.Slice(NbnQuantizationOffset, 80), header.Quantization);

        if (header.Regions.Length != NbnConstants.RegionCount)
        {
            throw new ArgumentException("Region directory must contain 32 entries.", nameof(header));
        }

        for (var i = 0; i < header.Regions.Length; i++)
        {
            var entry = header.Regions[i];
            var offset = NbnRegionDirectoryOffset + (i * NbnRegionDirectoryEntryBytes);
            var span = destination.Slice(offset, NbnRegionDirectoryEntryBytes);
            BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0, 4), entry.NeuronSpan);
            BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(4, 8), entry.TotalAxons);
            BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(12, 8), entry.Offset);
            BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(20, 4), entry.Flags);
        }
    }

    public static NbnRegionSection ReadNbnRegionSection(ReadOnlySpan<byte> data, ulong offset)
    {
        if (offset > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "Offset is too large for this parser.");
        }

        var start = (int)offset;
        if (data.Length < start + 24)
        {
            throw new ArgumentException("Data is too small for a region section header.", nameof(data));
        }

        var span = data.Slice(start);
        var regionId = span[0];
        var neuronSpan = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(4, 4));
        var totalAxons = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(8, 8));
        var stride = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(16, 4));
        var checkpointCount = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(20, 4));

        if (neuronSpan > int.MaxValue)
        {
            throw new InvalidOperationException("Neuron span is too large for this parser.");
        }

        if (totalAxons > int.MaxValue)
        {
            throw new InvalidOperationException("Total axons is too large for this parser.");
        }

        var checkpointOffset = 24;
        var checkpoints = new ulong[checkpointCount];
        for (var i = 0; i < checkpoints.Length; i++)
        {
            var checkpointSlice = span.Slice(checkpointOffset + (i * 8), 8);
            checkpoints[i] = BinaryPrimitives.ReadUInt64LittleEndian(checkpointSlice);
        }

        var neuronOffset = checkpointOffset + (checkpoints.Length * 8);
        var neuronRecords = new NeuronRecord[neuronSpan];
        for (var i = 0; i < neuronRecords.Length; i++)
        {
            var recordSpan = span.Slice(neuronOffset + (i * NbnConstants.NeuronRecordBytes), NbnConstants.NeuronRecordBytes);
            neuronRecords[i] = NeuronRecord.Read(recordSpan);
        }

        var axonOffset = neuronOffset + (neuronRecords.Length * NbnConstants.NeuronRecordBytes);
        var axonRecords = new AxonRecord[totalAxons];
        for (var i = 0; i < axonRecords.Length; i++)
        {
            var recordSpan = span.Slice(axonOffset + (i * NbnConstants.AxonRecordBytes), NbnConstants.AxonRecordBytes);
            axonRecords[i] = AxonRecord.Read(recordSpan);
        }

        return new NbnRegionSection(
            regionId,
            neuronSpan,
            totalAxons,
            stride,
            checkpointCount,
            checkpoints,
            neuronRecords,
            axonRecords);
    }

    public static void WriteNbnRegionSection(Span<byte> destination, NbnRegionSection section)
    {
        var requiredBytes = section.ByteLength;
        if (destination.Length < requiredBytes)
        {
            throw new ArgumentException("Destination span is too small for region section.", nameof(destination));
        }

        destination.Slice(0, requiredBytes).Clear();

        if (section.Checkpoints.Length != section.CheckpointCount)
        {
            throw new ArgumentException("Checkpoint count does not match checkpoints array length.", nameof(section));
        }

        var expectedCheckpointCount = GetCheckpointCount(section.NeuronSpan, section.Stride);
        if (section.CheckpointCount != expectedCheckpointCount)
        {
            throw new ArgumentException("Checkpoint count does not match stride rules.", nameof(section));
        }

        if ((ulong)section.AxonRecords.Length != section.TotalAxons)
        {
            throw new ArgumentException("Axon record count does not match total axons.", nameof(section));
        }

        var axonSum = 0UL;
        foreach (var neuron in section.NeuronRecords)
        {
            axonSum += neuron.AxonCount;
        }

        if (axonSum != section.TotalAxons)
        {
            throw new ArgumentException("Neuron axon counts do not match total axons.", nameof(section));
        }

        destination[0] = section.RegionId;
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(4, 4), section.NeuronSpan);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(8, 8), section.TotalAxons);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(16, 4), section.Stride);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(20, 4), section.CheckpointCount);

        var checkpointOffset = 24;
        for (var i = 0; i < section.Checkpoints.Length; i++)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(checkpointOffset + (i * 8), 8), section.Checkpoints[i]);
        }

        var neuronOffset = checkpointOffset + (section.Checkpoints.Length * 8);
        for (var i = 0; i < section.NeuronRecords.Length; i++)
        {
            section.NeuronRecords[i].WriteTo(destination.Slice(neuronOffset + (i * NbnConstants.NeuronRecordBytes), NbnConstants.NeuronRecordBytes));
        }

        var axonOffset = neuronOffset + (section.NeuronRecords.Length * NbnConstants.NeuronRecordBytes);
        for (var i = 0; i < section.AxonRecords.Length; i++)
        {
            section.AxonRecords[i].WriteTo(destination.Slice(axonOffset + (i * NbnConstants.AxonRecordBytes), NbnConstants.AxonRecordBytes));
        }
    }

    public static byte[] WriteNbn(NbnHeaderV2 header, IReadOnlyList<NbnRegionSection> sections)
    {
        if (header.Regions.Length != NbnConstants.RegionCount)
        {
            throw new ArgumentException("Region directory must contain 32 entries.", nameof(header));
        }

        var sectionMap = new Dictionary<byte, NbnRegionSection>();
        foreach (var section in sections)
        {
            if (!sectionMap.TryAdd(section.RegionId, section))
            {
                throw new ArgumentException("Duplicate region section provided.", nameof(sections));
            }
        }

        ulong totalSize = NbnHeaderBytes;
        for (var i = 0; i < header.Regions.Length; i++)
        {
            var entry = header.Regions[i];
            if (entry.NeuronSpan == 0)
            {
                continue;
            }

            if (!sectionMap.TryGetValue((byte)i, out var section))
            {
                throw new ArgumentException("Missing region section for directory entry.", nameof(sections));
            }

            if (section.NeuronSpan != entry.NeuronSpan)
            {
                throw new ArgumentException("Region neuron span does not match directory entry.", nameof(sections));
            }

            if (section.TotalAxons != entry.TotalAxons)
            {
                throw new ArgumentException("Region axon total does not match directory entry.", nameof(sections));
            }

            if (section.Stride != header.AxonStride)
            {
                throw new ArgumentException("Region stride does not match header stride.", nameof(sections));
            }

            var endOffset = entry.Offset + (ulong)section.ByteLength;
            if (endOffset > int.MaxValue)
            {
                throw new InvalidOperationException("Computed NBN size exceeds supported range.");
            }

            if (endOffset > totalSize)
            {
                totalSize = endOffset;
            }
        }

        var buffer = new byte[(int)totalSize];
        WriteNbnHeader(buffer, header);

        for (var i = 0; i < header.Regions.Length; i++)
        {
            var entry = header.Regions[i];
            if (entry.NeuronSpan == 0)
            {
                continue;
            }

            var section = sectionMap[(byte)i];
            if (entry.Offset > int.MaxValue)
            {
                throw new InvalidOperationException("Region offset exceeds supported range.");
            }

            WriteNbnRegionSection(buffer.AsSpan((int)entry.Offset), section);
        }

        return buffer;
    }

    public static NbsHeaderV2 ReadNbsHeader(ReadOnlySpan<byte> data)
    {
        if (data.Length < NbsHeaderBytes)
        {
            throw new ArgumentException("Data is smaller than the NBS header.", nameof(data));
        }

        var magic = Encoding.ASCII.GetString(data.Slice(0, 4));
        var version = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(0x004, 2));
        var endianness = data[0x006];
        var headerPow2 = data[0x007];
        var brainIdBytes = data.Slice(0x008, 16).ToArray();
        var brainId = new Guid(brainIdBytes);
        var snapshotTickId = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(0x018, 8));
        var timestampMs = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(0x020, 8));
        var energyRemaining = BinaryPrimitives.ReadInt64LittleEndian(data.Slice(0x028, 8));
        var baseHash = data.Slice(0x030, 32).ToArray();
        var flags = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(0x050, 4));
        var bufferMap = ReadQuantizationMap(data.Slice(0x054, NbnQuantizationFieldBytes));

        return new NbsHeaderV2(
            magic,
            version,
            endianness,
            headerPow2,
            brainId,
            snapshotTickId,
            timestampMs,
            energyRemaining,
            baseHash,
            flags,
            bufferMap);
    }

    public static void WriteNbsHeader(Span<byte> destination, NbsHeaderV2 header)
    {
        if (destination.Length < NbsHeaderBytes)
        {
            throw new ArgumentException("Destination span is too small for NBS header.", nameof(destination));
        }

        destination.Slice(0, NbsHeaderBytes).Clear();

        var magic = header.Magic.Length == 4 ? header.Magic : "NBS2";
        Encoding.ASCII.GetBytes(magic.AsSpan(0, 4), destination.Slice(0, 4));
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(0x004, 2), header.Version);
        destination[0x006] = header.Endianness;
        destination[0x007] = header.HeaderBytesPow2;
        header.BrainId.ToByteArray().CopyTo(destination.Slice(0x008, 16));
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(0x018, 8), header.SnapshotTickId);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(0x020, 8), header.TimestampMs);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(0x028, 8), header.EnergyRemaining);

        if (header.BaseNbnSha256.Length != 32)
        {
            throw new ArgumentException("Base NBN hash must be 32 bytes.", nameof(header));
        }

        header.BaseNbnSha256.CopyTo(destination.Slice(0x030, 32));
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0x050, 4), header.Flags);
        WriteQuantizationMap(destination.Slice(0x054, NbnQuantizationFieldBytes), header.BufferMap);
    }

    public static NbsRegionSection ReadNbsRegionSection(ReadOnlySpan<byte> data, int offset, bool includeEnabledBitset)
    {
        if (offset < 0 || offset >= data.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "Offset is out of range.");
        }

        var span = data.Slice(offset);
        if (span.Length < 8)
        {
            throw new ArgumentException("Data is too small for an NBS region header.", nameof(data));
        }

        var regionId = span[0];
        var neuronSpan = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(4, 4));
        if (neuronSpan > int.MaxValue)
        {
            throw new InvalidOperationException("Neuron span is too large for this parser.");
        }

        var bufferOffset = 8;
        var bufferCodes = new short[neuronSpan];
        for (var i = 0; i < bufferCodes.Length; i++)
        {
            bufferCodes[i] = BinaryPrimitives.ReadInt16LittleEndian(span.Slice(bufferOffset + (i * 2), 2));
        }

        var cursor = bufferOffset + (bufferCodes.Length * 2);
        byte[]? enabledBitset = null;
        if (includeEnabledBitset)
        {
            var enabledBytes = (int)((neuronSpan + 7) / 8);
            enabledBitset = span.Slice(cursor, enabledBytes).ToArray();
            cursor += enabledBytes;
        }

        return new NbsRegionSection(regionId, neuronSpan, bufferCodes, enabledBitset);
    }

    public static void WriteNbsRegionSection(Span<byte> destination, NbsRegionSection section, bool includeEnabledBitset)
    {
        var requiredBytes = GetNbsRegionSectionSize(section.NeuronSpan, includeEnabledBitset);
        if (destination.Length < requiredBytes)
        {
            throw new ArgumentException("Destination span is too small for NBS region section.", nameof(destination));
        }

        destination.Slice(0, requiredBytes).Clear();

        destination[0] = section.RegionId;
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(4, 4), section.NeuronSpan);

        var bufferOffset = 8;
        if (section.BufferCodes.Length != section.NeuronSpan)
        {
            throw new ArgumentException("Buffer code length does not match neuron span.", nameof(section));
        }

        for (var i = 0; i < section.BufferCodes.Length; i++)
        {
            BinaryPrimitives.WriteInt16LittleEndian(destination.Slice(bufferOffset + (i * 2), 2), section.BufferCodes[i]);
        }

        if (includeEnabledBitset)
        {
            if (section.EnabledBitset is null)
            {
                throw new ArgumentException("Enabled bitset is required but missing.", nameof(section));
            }

            var expectedBytes = (int)((section.NeuronSpan + 7) / 8);
            if (section.EnabledBitset.Length != expectedBytes)
            {
                throw new ArgumentException("Enabled bitset length does not match neuron span.", nameof(section));
            }

            section.EnabledBitset.CopyTo(destination.Slice(bufferOffset + (section.BufferCodes.Length * 2), expectedBytes));
        }
    }

    public static NbsOverlaySection ReadNbsOverlaySection(ReadOnlySpan<byte> data, int offset)
    {
        if (offset < 0 || offset >= data.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "Offset is out of range.");
        }

        if (data.Length - offset < 4)
        {
            throw new ArgumentException("Data is too small for NBS overlay section.", nameof(data));
        }

        var overlayCount = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(offset, 4));
        var expectedLength = GetNbsOverlaySectionSize((int)overlayCount);
        if (data.Length - offset < expectedLength)
        {
            throw new ArgumentException("Data is too small for NBS overlay records.", nameof(data));
        }

        var records = new NbsOverlayRecord[overlayCount];
        var recordOffset = offset + 4;
        for (var i = 0; i < records.Length; i++)
        {
            var fromAddress = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(recordOffset + (i * 12), 4));
            var toAddress = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(recordOffset + (i * 12) + 4, 4));
            var strengthCode = data[recordOffset + (i * 12) + 8];
            records[i] = new NbsOverlayRecord(fromAddress, toAddress, strengthCode);
        }

        return new NbsOverlaySection(records, expectedLength);
    }

    public static void WriteNbsOverlaySection(Span<byte> destination, IReadOnlyList<NbsOverlayRecord> overlays)
    {
        var requiredBytes = GetNbsOverlaySectionSize(overlays.Count);
        if (destination.Length < requiredBytes)
        {
            throw new ArgumentException("Destination span is too small for NBS overlay section.", nameof(destination));
        }

        destination.Slice(0, requiredBytes).Clear();
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0, 4), (uint)overlays.Count);

        var offset = 4;
        for (var i = 0; i < overlays.Count; i++)
        {
            var record = overlays[i];
            var recordOffset = offset + (i * 12);
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(recordOffset, 4), record.FromAddress);
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(recordOffset + 4, 4), record.ToAddress);
            destination[recordOffset + 8] = record.StrengthCode;
            destination[recordOffset + 9] = 0;
            BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(recordOffset + 10, 2), 0);
        }
    }

    public static byte[] WriteNbs(
        NbsHeaderV2 header,
        IReadOnlyList<NbsRegionSection> regions,
        IReadOnlyList<NbsOverlayRecord>? overlays = null)
    {
        var orderedRegions = new List<NbsRegionSection>(regions);
        orderedRegions.Sort((a, b) => a.RegionId.CompareTo(b.RegionId));

        var totalSize = NbsHeaderBytes;
        foreach (var region in orderedRegions)
        {
            totalSize += region.ByteLength;
        }

        if (header.AxonOverlayIncluded)
        {
            overlays ??= Array.Empty<NbsOverlayRecord>();
            totalSize += GetNbsOverlaySectionSize(overlays.Count);
        }
        else if (overlays is { Count: > 0 })
        {
            throw new ArgumentException("Overlays provided but header does not include overlay section.", nameof(overlays));
        }

        var buffer = new byte[totalSize];
        WriteNbsHeader(buffer, header);

        var offset = NbsHeaderBytes;
        foreach (var region in orderedRegions)
        {
            WriteNbsRegionSection(buffer.AsSpan(offset), region, header.EnabledBitsetIncluded);
            offset += region.ByteLength;
        }

        if (header.AxonOverlayIncluded)
        {
            overlays ??= Array.Empty<NbsOverlayRecord>();
            WriteNbsOverlaySection(buffer.AsSpan(offset), overlays);
        }

        return buffer;
    }

    public static int GetNbnRegionSectionSize(uint neuronSpan, ulong totalAxons, uint stride)
    {
        var checkpointCount = GetCheckpointCount(neuronSpan, stride);
        return checked(24 + ((int)checkpointCount * 8) + ((int)neuronSpan * NbnConstants.NeuronRecordBytes) + ((int)totalAxons * NbnConstants.AxonRecordBytes));
    }

    public static int GetNbsRegionSectionSize(uint neuronSpan, bool includeEnabledBitset)
    {
        var size = 8 + ((int)neuronSpan * 2);
        if (includeEnabledBitset)
        {
            size += (int)((neuronSpan + 7) / 8);
        }

        return size;
    }

    public static int GetNbsOverlaySectionSize(int overlayCount)
    {
        return checked(4 + (overlayCount * 12));
    }

    public static ulong[] BuildCheckpoints(IReadOnlyList<NeuronRecord> neurons, uint stride)
    {
        if (stride == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(stride), stride, "Stride must be greater than zero.");
        }

        if (neurons.Count == 0)
        {
            return new ulong[] { 0 };
        }

        var neuronSpan = (uint)neurons.Count;
        var checkpointCount = GetCheckpointCount(neuronSpan, stride);
        var checkpoints = new ulong[checkpointCount];
        var strideInt = checked((int)stride);
        ulong cumulative = 0;

        for (var i = 0; i < neurons.Count; i++)
        {
            if (i % strideInt == 0)
            {
                checkpoints[i / strideInt] = cumulative;
            }

            cumulative += neurons[i].AxonCount;
        }

        checkpoints[checkpoints.Length - 1] = cumulative;
        return checkpoints;
    }

    private static uint GetCheckpointCount(uint neuronSpan, uint stride)
    {
        if (stride == 0)
        {
            return 0;
        }

        return (uint)((neuronSpan + stride - 1) / stride + 1);
    }

    private static NbnQuantizationSchema ReadNbnQuantization(ReadOnlySpan<byte> data)
    {
        var offset = NbnQuantizationOffset;
        var strength = ReadQuantizationMap(data.Slice(offset, NbnQuantizationFieldBytes));
        offset += NbnQuantizationFieldBytes;
        var preActivation = ReadQuantizationMap(data.Slice(offset, NbnQuantizationFieldBytes));
        offset += NbnQuantizationFieldBytes;
        var activation = ReadQuantizationMap(data.Slice(offset, NbnQuantizationFieldBytes));
        offset += NbnQuantizationFieldBytes;
        var paramA = ReadQuantizationMap(data.Slice(offset, NbnQuantizationFieldBytes));
        offset += NbnQuantizationFieldBytes;
        var paramB = ReadQuantizationMap(data.Slice(offset, NbnQuantizationFieldBytes));

        return new NbnQuantizationSchema(strength, preActivation, activation, paramA, paramB);
    }

    private static void WriteNbnQuantization(Span<byte> span, NbnQuantizationSchema schema)
    {
        WriteQuantizationMap(span.Slice(0, 16), schema.Strength);
        WriteQuantizationMap(span.Slice(16, 16), schema.PreActivationThreshold);
        WriteQuantizationMap(span.Slice(32, 16), schema.ActivationThreshold);
        WriteQuantizationMap(span.Slice(48, 16), schema.ParamA);
        WriteQuantizationMap(span.Slice(64, 16), schema.ParamB);
    }

    private static QuantizationMap ReadQuantizationMap(ReadOnlySpan<byte> data)
    {
        var mapType = (QuantMapType)data[0];
        var min = BinaryPrimitives.ReadSingleLittleEndian(data.Slice(4, 4));
        var max = BinaryPrimitives.ReadSingleLittleEndian(data.Slice(8, 4));
        var gamma = BinaryPrimitives.ReadSingleLittleEndian(data.Slice(12, 4));
        return new QuantizationMap(mapType, min, max, gamma);
    }

    private static void WriteQuantizationMap(Span<byte> span, QuantizationMap map)
    {
        span[0] = (byte)map.MapType;
        span[1] = 0;
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(2, 2), 0);
        BinaryPrimitives.WriteSingleLittleEndian(span.Slice(4, 4), map.Min);
        BinaryPrimitives.WriteSingleLittleEndian(span.Slice(8, 4), map.Max);
        BinaryPrimitives.WriteSingleLittleEndian(span.Slice(12, 4), map.Gamma);
    }

    private static NbnRegionDirectoryEntry[] ReadRegionDirectory(ReadOnlySpan<byte> data)
    {
        var entries = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        for (var i = 0; i < entries.Length; i++)
        {
            var entryOffset = NbnRegionDirectoryOffset + (i * NbnRegionDirectoryEntryBytes);
            var entry = data.Slice(entryOffset, NbnRegionDirectoryEntryBytes);
            var neuronSpan = BinaryPrimitives.ReadUInt32LittleEndian(entry.Slice(0, 4));
            var totalAxons = BinaryPrimitives.ReadUInt64LittleEndian(entry.Slice(4, 8));
            var regionOffset = BinaryPrimitives.ReadUInt64LittleEndian(entry.Slice(12, 8));
            var flags = BinaryPrimitives.ReadUInt32LittleEndian(entry.Slice(20, 4));
            entries[i] = new NbnRegionDirectoryEntry(neuronSpan, totalAxons, regionOffset, flags);
        }

        return entries;
    }
}