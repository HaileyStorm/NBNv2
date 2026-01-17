using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Nbn.Shared;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;

namespace Nbn.Tests.Format;

internal static class NbnTestVectors
{
    public const uint MinimalNeuronSpan = 2;
    public const ulong SampleBrainSeed = 0x0102030405060708;
    public const ulong SampleTickId = 42;
    public const ulong SampleTimestampMs = 1_700_000_000_000;
    public const long SampleEnergyRemaining = 123_456;

    public static readonly Guid SampleBrainId = Guid.Parse("8E8D0F2A-1DB7-4C7D-9D1F-05D5B8D8E2A1");

    public static byte[] CreateMinimalNbn()
    {
        var stride = (uint)NbnConstants.DefaultAxonStride;
        var regionSectionSize = ComputeRegionSectionSize(MinimalNeuronSpan, 0, stride);
        var region0Offset = NbnBinary.NbnHeaderBytes;
        var region31Offset = region0Offset + regionSectionSize;
        var totalSize = region31Offset + regionSectionSize;

        var buffer = new byte[totalSize];
        var span = buffer.AsSpan();

        Encoding.ASCII.GetBytes("NBN2").CopyTo(span.Slice(0, 4));
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x004, 2), 2);
        span[0x006] = 1;
        span[0x007] = 10;
        BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(0x008, 8), SampleBrainSeed);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x010, 4), stride);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x014, 4), 0);
        BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(0x018, 8), 0);

        WriteNbnQuantization(span.Slice(0x020, 80), QuantizationSchemas.DefaultNbn);

        WriteRegionDirectoryEntry(
            span.Slice(0x100 + (0 * 24), 24),
            MinimalNeuronSpan,
            0,
            (ulong)region0Offset,
            0);

        WriteRegionDirectoryEntry(
            span.Slice(0x100 + (31 * 24), 24),
            MinimalNeuronSpan,
            0,
            (ulong)region31Offset,
            0);

        WriteRegionSection(span.Slice(region0Offset, regionSectionSize), 0, MinimalNeuronSpan, 0, stride);
        WriteRegionSection(span.Slice(region31Offset, regionSectionSize), 31, MinimalNeuronSpan, 0, stride);

        return buffer;
    }

    public static byte[] CreateMinimalNbs(byte[] baseNbn)
    {
        var regionSectionSize = 8 + ((int)MinimalNeuronSpan * 2);
        var region0Offset = NbnBinary.NbsHeaderBytes;
        var region31Offset = region0Offset + regionSectionSize;
        var totalSize = region31Offset + regionSectionSize;

        var buffer = new byte[totalSize];
        var span = buffer.AsSpan();

        Encoding.ASCII.GetBytes("NBS2").CopyTo(span.Slice(0, 4));
        BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(0x004, 2), 2);
        span[0x006] = 1;
        span[0x007] = 9;
        SampleBrainId.ToByteArray().CopyTo(span.Slice(0x008, 16));
        BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(0x018, 8), SampleTickId);
        BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(0x020, 8), SampleTimestampMs);
        BinaryPrimitives.WriteInt64LittleEndian(span.Slice(0x028, 8), SampleEnergyRemaining);

        var hash = SHA256.HashData(baseNbn);
        hash.CopyTo(span.Slice(0x030, 32));

        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x050, 4), 0);
        WriteQuantizationMap(span.Slice(0x054, 16), QuantizationSchemas.DefaultBuffer);

        WriteNbsRegionSection(span.Slice(region0Offset, regionSectionSize), 0, MinimalNeuronSpan);
        WriteNbsRegionSection(span.Slice(region31Offset, regionSectionSize), 31, MinimalNeuronSpan);

        return buffer;
    }

    private static int ComputeRegionSectionSize(uint neuronSpan, ulong totalAxons, uint stride)
    {
        var checkpointCount = (uint)((neuronSpan + stride - 1) / stride + 1);
        return checked(24 + ((int)checkpointCount * 8) + ((int)neuronSpan * NbnConstants.NeuronRecordBytes) + ((int)totalAxons * NbnConstants.AxonRecordBytes));
    }

    private static void WriteRegionDirectoryEntry(Span<byte> span, uint neuronSpan, ulong totalAxons, ulong offset, uint flags)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0, 4), neuronSpan);
        BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(4, 8), totalAxons);
        BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(12, 8), offset);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(20, 4), flags);
    }

    private static void WriteRegionSection(Span<byte> span, byte regionId, uint neuronSpan, ulong totalAxons, uint stride)
    {
        span[0] = regionId;
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(4, 4), neuronSpan);
        BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(8, 8), totalAxons);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(16, 4), stride);

        var checkpointCount = (uint)((neuronSpan + stride - 1) / stride + 1);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(20, 4), checkpointCount);

        var checkpointsOffset = 24;
        for (var i = 0; i < checkpointCount; i++)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(checkpointsOffset + (i * 8), 8), 0);
        }

        var neuronOffset = checkpointsOffset + ((int)checkpointCount * 8);
        var record = new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true);
        for (var i = 0; i < neuronSpan; i++)
        {
            record.WriteTo(span.Slice(neuronOffset + (i * NbnConstants.NeuronRecordBytes), NbnConstants.NeuronRecordBytes));
        }
    }

    private static void WriteNbsRegionSection(Span<byte> span, byte regionId, uint neuronSpan)
    {
        span[0] = regionId;
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(4, 4), neuronSpan);
        var bufferOffset = 8;
        for (var i = 0; i < neuronSpan; i++)
        {
            BinaryPrimitives.WriteInt16LittleEndian(span.Slice(bufferOffset + (i * 2), 2), 0);
        }
    }

    private static void WriteNbnQuantization(Span<byte> span, NbnQuantizationSchema schema)
    {
        WriteQuantizationMap(span.Slice(0, 16), schema.Strength);
        WriteQuantizationMap(span.Slice(16, 16), schema.PreActivationThreshold);
        WriteQuantizationMap(span.Slice(32, 16), schema.ActivationThreshold);
        WriteQuantizationMap(span.Slice(48, 16), schema.ParamA);
        WriteQuantizationMap(span.Slice(64, 16), schema.ParamB);
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
}
