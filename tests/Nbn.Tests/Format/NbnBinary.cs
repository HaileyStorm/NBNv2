using System;
using System.Buffers.Binary;
using System.Text;
using Nbn.Shared;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;

namespace Nbn.Tests.Format;

internal static class NbnBinary
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

    public static NbnRegionSection ReadRegionSection(ReadOnlySpan<byte> data, ulong offset)
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
            checkpoints[i] = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(checkpointOffset + (i * 8), 8));
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

        var byteLength = axonOffset + (axonRecords.Length * NbnConstants.AxonRecordBytes);

        return new NbnRegionSection(
            regionId,
            neuronSpan,
            totalAxons,
            stride,
            checkpointCount,
            checkpoints,
            neuronRecords,
            axonRecords,
            byteLength);
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
            if (span.Length < cursor + enabledBytes)
            {
                throw new ArgumentException("Data is too small for an NBS enabled bitset.", nameof(data));
            }

            enabledBitset = span.Slice(cursor, enabledBytes).ToArray();
            cursor += enabledBytes;
        }

        return new NbsRegionSection(regionId, neuronSpan, bufferCodes, enabledBitset, cursor);
    }

    public static NbsOverlaySection ReadNbsOverlaySection(ReadOnlySpan<byte> data, int offset)
    {
        if (offset < 0 || offset >= data.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "Offset is out of range.");
        }

        if (data.Length < offset + 4)
        {
            throw new ArgumentException("Data is too small for an NBS overlay header.", nameof(data));
        }

        var overlayCount = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(offset, 4));
        if (overlayCount > int.MaxValue)
        {
            throw new InvalidOperationException("Overlay count is too large for this parser.");
        }

        var overlays = new NbsOverlayRecord[overlayCount];
        var cursor = offset + 4;
        for (var i = 0; i < overlays.Length; i++)
        {
            if (data.Length < cursor + 12)
            {
                throw new ArgumentException("Data is too small for NBS overlay records.", nameof(data));
            }

            var fromAddress = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(cursor, 4));
            var toAddress = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(cursor + 4, 4));
            var strengthCode = data[cursor + 8];
            overlays[i] = new NbsOverlayRecord(fromAddress, toAddress, strengthCode);
            cursor += 12;
        }

        return new NbsOverlaySection(overlays, cursor - offset);
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

    private static QuantizationMap ReadQuantizationMap(ReadOnlySpan<byte> data)
    {
        var mapType = (QuantMapType)data[0];
        var min = BinaryPrimitives.ReadSingleLittleEndian(data.Slice(4, 4));
        var max = BinaryPrimitives.ReadSingleLittleEndian(data.Slice(8, 4));
        var gamma = BinaryPrimitives.ReadSingleLittleEndian(data.Slice(12, 4));
        return new QuantizationMap(mapType, min, max, gamma);
    }

    private static RegionDirectoryEntry[] ReadRegionDirectory(ReadOnlySpan<byte> data)
    {
        var entries = new RegionDirectoryEntry[NbnConstants.RegionCount];
        for (var i = 0; i < entries.Length; i++)
        {
            var entryOffset = NbnRegionDirectoryOffset + (i * NbnRegionDirectoryEntryBytes);
            var entry = data.Slice(entryOffset, NbnRegionDirectoryEntryBytes);
            var neuronSpan = BinaryPrimitives.ReadUInt32LittleEndian(entry.Slice(0, 4));
            var totalAxons = BinaryPrimitives.ReadUInt64LittleEndian(entry.Slice(4, 8));
            var regionOffset = BinaryPrimitives.ReadUInt64LittleEndian(entry.Slice(12, 8));
            var flags = BinaryPrimitives.ReadUInt32LittleEndian(entry.Slice(20, 4));
            entries[i] = new RegionDirectoryEntry(neuronSpan, totalAxons, regionOffset, flags);
        }

        return entries;
    }
}

internal sealed class NbnHeaderV2
{
    public NbnHeaderV2(
        string magic,
        ushort version,
        byte endianness,
        byte headerBytesPow2,
        ulong brainSeed,
        uint axonStride,
        uint flags,
        NbnQuantizationSchema quantization,
        RegionDirectoryEntry[] regions)
    {
        Magic = magic;
        Version = version;
        Endianness = endianness;
        HeaderBytesPow2 = headerBytesPow2;
        BrainSeed = brainSeed;
        AxonStride = axonStride;
        Flags = flags;
        Quantization = quantization;
        Regions = regions;
    }

    public string Magic { get; }
    public ushort Version { get; }
    public byte Endianness { get; }
    public byte HeaderBytesPow2 { get; }
    public ulong BrainSeed { get; }
    public uint AxonStride { get; }
    public uint Flags { get; }
    public NbnQuantizationSchema Quantization { get; }
    public RegionDirectoryEntry[] Regions { get; }
}

internal readonly struct RegionDirectoryEntry
{
    public RegionDirectoryEntry(uint neuronSpan, ulong totalAxons, ulong offset, uint flags)
    {
        NeuronSpan = neuronSpan;
        TotalAxons = totalAxons;
        Offset = offset;
        Flags = flags;
    }

    public uint NeuronSpan { get; }
    public ulong TotalAxons { get; }
    public ulong Offset { get; }
    public uint Flags { get; }
}

internal sealed class NbnRegionSection
{
    public NbnRegionSection(
        byte regionId,
        uint neuronSpan,
        ulong totalAxons,
        uint stride,
        uint checkpointCount,
        ulong[] checkpoints,
        NeuronRecord[] neuronRecords,
        AxonRecord[] axonRecords,
        int byteLength)
    {
        RegionId = regionId;
        NeuronSpan = neuronSpan;
        TotalAxons = totalAxons;
        Stride = stride;
        CheckpointCount = checkpointCount;
        Checkpoints = checkpoints;
        NeuronRecords = neuronRecords;
        AxonRecords = axonRecords;
        ByteLength = byteLength;
    }

    public byte RegionId { get; }
    public uint NeuronSpan { get; }
    public ulong TotalAxons { get; }
    public uint Stride { get; }
    public uint CheckpointCount { get; }
    public ulong[] Checkpoints { get; }
    public NeuronRecord[] NeuronRecords { get; }
    public AxonRecord[] AxonRecords { get; }
    public int ByteLength { get; }
}

internal sealed class NbsHeaderV2
{
    public NbsHeaderV2(
        string magic,
        ushort version,
        byte endianness,
        byte headerBytesPow2,
        Guid brainId,
        ulong snapshotTickId,
        ulong timestampMs,
        long energyRemaining,
        byte[] baseNbnSha256,
        uint flags,
        QuantizationMap bufferMap)
    {
        Magic = magic;
        Version = version;
        Endianness = endianness;
        HeaderBytesPow2 = headerBytesPow2;
        BrainId = brainId;
        SnapshotTickId = snapshotTickId;
        TimestampMs = timestampMs;
        EnergyRemaining = energyRemaining;
        BaseNbnSha256 = baseNbnSha256;
        Flags = flags;
        BufferMap = bufferMap;
    }

    public string Magic { get; }
    public ushort Version { get; }
    public byte Endianness { get; }
    public byte HeaderBytesPow2 { get; }
    public Guid BrainId { get; }
    public ulong SnapshotTickId { get; }
    public ulong TimestampMs { get; }
    public long EnergyRemaining { get; }
    public byte[] BaseNbnSha256 { get; }
    public uint Flags { get; }
    public QuantizationMap BufferMap { get; }

    public bool EnabledBitsetIncluded => (Flags & 0x1u) != 0;
    public bool AxonOverlayIncluded => (Flags & 0x2u) != 0;
    public bool CostEnabled => (Flags & 0x4u) != 0;
    public bool EnergyEnabled => (Flags & 0x8u) != 0;
    public bool PlasticityEnabled => (Flags & 0x10u) != 0;
}

internal sealed class NbsRegionSection
{
    public NbsRegionSection(byte regionId, uint neuronSpan, short[] bufferCodes, byte[]? enabledBitset, int byteLength)
    {
        RegionId = regionId;
        NeuronSpan = neuronSpan;
        BufferCodes = bufferCodes;
        EnabledBitset = enabledBitset;
        ByteLength = byteLength;
    }

    public byte RegionId { get; }
    public uint NeuronSpan { get; }
    public short[] BufferCodes { get; }
    public byte[]? EnabledBitset { get; }
    public int ByteLength { get; }
}

internal readonly struct NbsOverlayRecord
{
    public NbsOverlayRecord(uint fromAddress, uint toAddress, byte strengthCode)
    {
        FromAddress = fromAddress;
        ToAddress = toAddress;
        StrengthCode = strengthCode;
    }

    public uint FromAddress { get; }
    public uint ToAddress { get; }
    public byte StrengthCode { get; }
}

internal sealed class NbsOverlaySection
{
    public NbsOverlaySection(NbsOverlayRecord[] overlays, int byteLength)
    {
        Overlays = overlays;
        ByteLength = byteLength;
    }

    public NbsOverlayRecord[] Overlays { get; }
    public int ByteLength { get; }
}
