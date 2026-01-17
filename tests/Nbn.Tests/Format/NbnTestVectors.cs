using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;

namespace Nbn.Tests.Format;

internal static class NbnTestVectors
{
    public const uint MinimalNeuronSpan = 2;
    public const uint RichNbsFlags = 0x1F;
    public const uint OverlayOnlyFlags = 0x2;
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

        var neuronRecord = new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true);
        var minimalNeurons = CreateUniformNeurons(MinimalNeuronSpan, neuronRecord);
        WriteRegionSection(span.Slice(region0Offset, regionSectionSize), 0, minimalNeurons, Array.Empty<AxonRecord>(), stride);
        WriteRegionSection(span.Slice(region31Offset, regionSectionSize), 31, minimalNeurons, Array.Empty<AxonRecord>(), stride);

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

    public static byte[] CreateNbsWithEmptyOverlays(byte[] baseNbn)
    {
        var regionSectionSize = 8 + ((int)MinimalNeuronSpan * 2);
        var region0Offset = NbnBinary.NbsHeaderBytes;
        var region31Offset = region0Offset + regionSectionSize;
        var overlayOffset = region31Offset + regionSectionSize;
        var totalSize = overlayOffset + 4;

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

        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x050, 4), OverlayOnlyFlags);
        WriteQuantizationMap(span.Slice(0x054, 16), QuantizationSchemas.DefaultBuffer);

        WriteNbsRegionSection(span.Slice(region0Offset, regionSectionSize), 0, MinimalNeuronSpan);
        WriteNbsRegionSection(span.Slice(region31Offset, regionSectionSize), 31, MinimalNeuronSpan);

        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(overlayOffset, 4), 0);

        return buffer;
    }

    public static RichNbnVector CreateCheckpointNbnVector()
    {
        const uint stride = 4;

        var region0Neurons = new[]
        {
            new NeuronRecord(0, 1, 2, 3, 4, 5, 6, 1, true)
        };
        var region0Axons = Array.Empty<AxonRecord>();

        var region2Neurons = new[]
        {
            new NeuronRecord(1, 1, 2, 3, 4, 5, 6, 0, true),
            new NeuronRecord(0, 2, 3, 4, 5, 6, 7, 1, true),
            new NeuronRecord(2, 3, 4, 5, 6, 7, 8, 2, true),
            new NeuronRecord(1, 4, 5, 6, 7, 8, 9, 3, true),
            new NeuronRecord(0, 5, 6, 7, 8, 9, 10, 0, true),
            new NeuronRecord(1, 6, 7, 8, 9, 10, 11, 1, true),
            new NeuronRecord(0, 7, 8, 9, 10, 11, 12, 2, true),
            new NeuronRecord(2, 8, 9, 10, 11, 12, 13, 3, true),
            new NeuronRecord(1, 9, 10, 11, 12, 13, 14, 0, true),
            new NeuronRecord(0, 10, 11, 12, 13, 14, 15, 1, true)
        };
        var region2Axons = new[]
        {
            new AxonRecord(1, 5, 2),
            new AxonRecord(2, 1, 2),
            new AxonRecord(3, 0, 31),
            new AxonRecord(4, 0, 2),
            new AxonRecord(5, 0, 31),
            new AxonRecord(6, 2, 2),
            new AxonRecord(7, 3, 2),
            new AxonRecord(8, 9, 2)
        };

        var region31Neurons = new[]
        {
            new NeuronRecord(0, 2, 3, 4, 5, 6, 7, 0, true)
        };
        var region31Axons = Array.Empty<AxonRecord>();

        var regions = new[]
        {
            new RichRegionSpec(0, region0Neurons, region0Axons),
            new RichRegionSpec(2, region2Neurons, region2Axons),
            new RichRegionSpec(31, region31Neurons, region31Axons)
        };

        var region0Size = ComputeRegionSectionSize((uint)region0Neurons.Length, (ulong)region0Axons.Length, stride);
        var region2Size = ComputeRegionSectionSize((uint)region2Neurons.Length, (ulong)region2Axons.Length, stride);
        var region31Size = ComputeRegionSectionSize((uint)region31Neurons.Length, (ulong)region31Axons.Length, stride);

        var region0Offset = NbnBinary.NbnHeaderBytes;
        var region2Offset = region0Offset + region0Size;
        var region31Offset = region2Offset + region2Size;
        var totalSize = region31Offset + region31Size;

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
            (uint)region0Neurons.Length,
            (ulong)region0Axons.Length,
            (ulong)region0Offset,
            0);

        WriteRegionDirectoryEntry(
            span.Slice(0x100 + (2 * 24), 24),
            (uint)region2Neurons.Length,
            (ulong)region2Axons.Length,
            (ulong)region2Offset,
            0);

        WriteRegionDirectoryEntry(
            span.Slice(0x100 + (31 * 24), 24),
            (uint)region31Neurons.Length,
            (ulong)region31Axons.Length,
            (ulong)region31Offset,
            0);

        WriteRegionSection(span.Slice(region0Offset, region0Size), 0, region0Neurons, region0Axons, stride);
        WriteRegionSection(span.Slice(region2Offset, region2Size), 2, region2Neurons, region2Axons, stride);
        WriteRegionSection(span.Slice(region31Offset, region31Size), 31, region31Neurons, region31Axons, stride);

        return new RichNbnVector(buffer, stride, regions);
    }

    public static RichNbnVector CreateRichNbnVector()
    {
        var stride = (uint)NbnConstants.DefaultAxonStride;

        var region0Neurons = new[]
        {
            new NeuronRecord(2, 2, 1, 3, 4, 5, 6, 1, true),
            new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true),
            new NeuronRecord(1, 3, 4, 5, 6, 7, 8, 2, true)
        };
        var region0Axons = new[]
        {
            new AxonRecord(10, 1, 1),
            new AxonRecord(12, 0, 31),
            new AxonRecord(5, 3, 1)
        };

        var region1Neurons = new[]
        {
            new NeuronRecord(1, 4, 5, 6, 7, 8, 9, 0, true),
            new NeuronRecord(2, 5, 6, 7, 8, 9, 10, 1, true),
            new NeuronRecord(0, 6, 7, 8, 9, 10, 11, 2, true),
            new NeuronRecord(1, 7, 8, 9, 10, 11, 12, 3, true)
        };
        var region1Axons = new[]
        {
            new AxonRecord(7, 2, 1),
            new AxonRecord(6, 0, 1),
            new AxonRecord(11, 1, 31),
            new AxonRecord(9, 0, 31)
        };

        var region31Neurons = new[]
        {
            new NeuronRecord(1, 8, 9, 10, 11, 12, 13, 0, true),
            new NeuronRecord(0, 9, 10, 11, 12, 13, 14, 1, true)
        };
        var region31Axons = new[]
        {
            new AxonRecord(13, 1, 1)
        };

        var regions = new[]
        {
            new RichRegionSpec(0, region0Neurons, region0Axons),
            new RichRegionSpec(1, region1Neurons, region1Axons),
            new RichRegionSpec(31, region31Neurons, region31Axons)
        };

        var region0Size = ComputeRegionSectionSize((uint)region0Neurons.Length, (ulong)region0Axons.Length, stride);
        var region1Size = ComputeRegionSectionSize((uint)region1Neurons.Length, (ulong)region1Axons.Length, stride);
        var region31Size = ComputeRegionSectionSize((uint)region31Neurons.Length, (ulong)region31Axons.Length, stride);

        var region0Offset = NbnBinary.NbnHeaderBytes;
        var region1Offset = region0Offset + region0Size;
        var region31Offset = region1Offset + region1Size;
        var totalSize = region31Offset + region31Size;

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
            (uint)region0Neurons.Length,
            (ulong)region0Axons.Length,
            (ulong)region0Offset,
            0);

        WriteRegionDirectoryEntry(
            span.Slice(0x100 + (1 * 24), 24),
            (uint)region1Neurons.Length,
            (ulong)region1Axons.Length,
            (ulong)region1Offset,
            0);

        WriteRegionDirectoryEntry(
            span.Slice(0x100 + (31 * 24), 24),
            (uint)region31Neurons.Length,
            (ulong)region31Axons.Length,
            (ulong)region31Offset,
            0);

        WriteRegionSection(span.Slice(region0Offset, region0Size), 0, region0Neurons, region0Axons, stride);
        WriteRegionSection(span.Slice(region1Offset, region1Size), 1, region1Neurons, region1Axons, stride);
        WriteRegionSection(span.Slice(region31Offset, region31Size), 31, region31Neurons, region31Axons, stride);

        return new RichNbnVector(buffer, stride, regions);
    }

    public static RichNbsVector CreateRichNbsVector(RichNbnVector baseNbn)
    {
        var regionSpecs = new[]
        {
            new RichNbsRegionSpec(0, new short[] { 0, 256, -256 }, new byte[] { 0x05 }),
            new RichNbsRegionSpec(1, new short[] { 1000, -1000, 500, -500 }, new byte[] { 0x0B }),
            new RichNbsRegionSpec(31, new short[] { 42, -42 }, new byte[] { 0x02 })
        };

        var overlays = new[]
        {
            new NbsOverlaySpec(Address32.From(0, 0).Value, Address32.From(1, 1).Value, 19),
            new NbsOverlaySpec(Address32.From(1, 1).Value, Address32.From(31, 1).Value, 22)
        };

        var regionSectionSizes = 0;
        foreach (var region in regionSpecs)
        {
            regionSectionSizes += ComputeNbsRegionSectionSize(region.NeuronSpan, includeEnabledBitset: true);
        }

        var overlaySectionSize = 4 + (overlays.Length * 12);
        var totalSize = NbnBinary.NbsHeaderBytes + regionSectionSizes + overlaySectionSize;

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

        var hash = SHA256.HashData(baseNbn.Bytes);
        hash.CopyTo(span.Slice(0x030, 32));

        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0x050, 4), RichNbsFlags);
        WriteQuantizationMap(span.Slice(0x054, 16), QuantizationSchemas.DefaultBuffer);

        var offset = NbnBinary.NbsHeaderBytes;
        foreach (var region in regionSpecs)
        {
            var size = ComputeNbsRegionSectionSize(region.NeuronSpan, includeEnabledBitset: true);
            WriteNbsRegionSection(span.Slice(offset, size), region.RegionId, region.BufferCodes, region.EnabledBitset);
            offset += size;
        }

        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(offset, 4), (uint)overlays.Length);
        var overlayOffset = offset + 4;
        for (var i = 0; i < overlays.Length; i++)
        {
            var recordOffset = overlayOffset + (i * 12);
            BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(recordOffset, 4), overlays[i].FromAddress);
            BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(recordOffset + 4, 4), overlays[i].ToAddress);
            span[recordOffset + 8] = overlays[i].StrengthCode;
            span[recordOffset + 9] = 0;
            BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(recordOffset + 10, 2), 0);
        }

        return new RichNbsVector(buffer, regionSpecs, overlays, RichNbsFlags);
    }

    public static ulong[] BuildCheckpoints(NeuronRecord[] neurons, uint stride)
    {
        if (neurons.Length == 0)
        {
            return new ulong[] { 0 };
        }

        var neuronSpan = (uint)neurons.Length;
        var checkpointCount = (uint)((neuronSpan + stride - 1) / stride + 1);
        var checkpoints = new ulong[checkpointCount];
        var strideInt = checked((int)stride);
        ulong cumulative = 0;

        for (var i = 0; i < neurons.Length; i++)
        {
            if (i % strideInt == 0)
            {
                checkpoints[i / strideInt] = cumulative;
            }

            cumulative += neurons[i].AxonCount;
        }

        checkpoints[checkpointCount - 1] = cumulative;
        return checkpoints;
    }

    private static int ComputeRegionSectionSize(uint neuronSpan, ulong totalAxons, uint stride)
    {
        var checkpointCount = (uint)((neuronSpan + stride - 1) / stride + 1);
        return checked(24 + ((int)checkpointCount * 8) + ((int)neuronSpan * NbnConstants.NeuronRecordBytes) + ((int)totalAxons * NbnConstants.AxonRecordBytes));
    }

    private static int ComputeNbsRegionSectionSize(uint neuronSpan, bool includeEnabledBitset)
    {
        var size = 8 + ((int)neuronSpan * 2);
        if (includeEnabledBitset)
        {
            size += (int)((neuronSpan + 7) / 8);
        }

        return size;
    }

    private static void WriteRegionDirectoryEntry(Span<byte> span, uint neuronSpan, ulong totalAxons, ulong offset, uint flags)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(0, 4), neuronSpan);
        BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(4, 8), totalAxons);
        BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(12, 8), offset);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(20, 4), flags);
    }

    private static void WriteRegionSection(Span<byte> span, byte regionId, NeuronRecord[] neurons, AxonRecord[] axons, uint stride)
    {
        var neuronSpan = (uint)neurons.Length;
        var totalAxons = (ulong)axons.Length;
        var expectedAxonCount = 0UL;
        foreach (var neuron in neurons)
        {
            expectedAxonCount += neuron.AxonCount;
        }

        if (expectedAxonCount != totalAxons)
        {
            throw new InvalidOperationException("Axon records length does not match neuron axon counts.");
        }

        var expectedSize = ComputeRegionSectionSize(neuronSpan, totalAxons, stride);
        if (span.Length < expectedSize)
        {
            throw new ArgumentException("Destination span is too small for region section.", nameof(span));
        }

        span[0] = regionId;
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(4, 4), neuronSpan);
        BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(8, 8), totalAxons);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(16, 4), stride);

        var checkpoints = BuildCheckpoints(neurons, stride);
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(20, 4), (uint)checkpoints.Length);

        var checkpointsOffset = 24;
        for (var i = 0; i < checkpoints.Length; i++)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(checkpointsOffset + (i * 8), 8), checkpoints[i]);
        }

        var neuronOffset = checkpointsOffset + (checkpoints.Length * 8);
        for (var i = 0; i < neurons.Length; i++)
        {
            neurons[i].WriteTo(span.Slice(neuronOffset + (i * NbnConstants.NeuronRecordBytes), NbnConstants.NeuronRecordBytes));
        }

        var axonOffset = neuronOffset + (neurons.Length * NbnConstants.NeuronRecordBytes);
        for (var i = 0; i < axons.Length; i++)
        {
            axons[i].WriteTo(span.Slice(axonOffset + (i * NbnConstants.AxonRecordBytes), NbnConstants.AxonRecordBytes));
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

    private static void WriteNbsRegionSection(Span<byte> span, byte regionId, short[] bufferCodes, byte[] enabledBitset)
    {
        var neuronSpan = (uint)bufferCodes.Length;
        span[0] = regionId;
        BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(4, 4), neuronSpan);
        var bufferOffset = 8;
        for (var i = 0; i < bufferCodes.Length; i++)
        {
            BinaryPrimitives.WriteInt16LittleEndian(span.Slice(bufferOffset + (i * 2), 2), bufferCodes[i]);
        }

        var enabledOffset = bufferOffset + (bufferCodes.Length * 2);
        var expectedEnabledBytes = (int)((neuronSpan + 7) / 8);
        if (enabledBitset.Length != expectedEnabledBytes)
        {
            throw new ArgumentException("Enabled bitset length does not match neuron span.", nameof(enabledBitset));
        }

        enabledBitset.CopyTo(span.Slice(enabledOffset, enabledBitset.Length));
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

    private static NeuronRecord[] CreateUniformNeurons(uint neuronSpan, NeuronRecord record)
    {
        var neurons = new NeuronRecord[neuronSpan];
        for (var i = 0; i < neuronSpan; i++)
        {
            neurons[i] = record;
        }

        return neurons;
    }
}

internal sealed class RichNbnVector
{
    public RichNbnVector(byte[] bytes, uint stride, RichRegionSpec[] regions)
    {
        Bytes = bytes;
        Stride = stride;
        Regions = regions;
    }

    public byte[] Bytes { get; }
    public uint Stride { get; }
    public RichRegionSpec[] Regions { get; }
}

internal sealed class RichRegionSpec
{
    public RichRegionSpec(byte regionId, NeuronRecord[] neurons, AxonRecord[] axons)
    {
        RegionId = regionId;
        Neurons = neurons;
        Axons = axons;
    }

    public byte RegionId { get; }
    public NeuronRecord[] Neurons { get; }
    public AxonRecord[] Axons { get; }
    public uint NeuronSpan => (uint)Neurons.Length;
}

internal sealed class RichNbsVector
{
    public RichNbsVector(byte[] bytes, RichNbsRegionSpec[] regions, NbsOverlaySpec[] overlays, uint flags)
    {
        Bytes = bytes;
        Regions = regions;
        Overlays = overlays;
        Flags = flags;
    }

    public byte[] Bytes { get; }
    public RichNbsRegionSpec[] Regions { get; }
    public NbsOverlaySpec[] Overlays { get; }
    public uint Flags { get; }
}

internal sealed class RichNbsRegionSpec
{
    public RichNbsRegionSpec(byte regionId, short[] bufferCodes, byte[] enabledBitset)
    {
        RegionId = regionId;
        BufferCodes = bufferCodes;
        EnabledBitset = enabledBitset;
    }

    public byte RegionId { get; }
    public short[] BufferCodes { get; }
    public byte[] EnabledBitset { get; }
    public uint NeuronSpan => (uint)BufferCodes.Length;
}

internal readonly struct NbsOverlaySpec
{
    public NbsOverlaySpec(uint fromAddress, uint toAddress, byte strengthCode)
    {
        FromAddress = fromAddress;
        ToAddress = toAddress;
        StrengthCode = strengthCode;
    }

    public uint FromAddress { get; }
    public uint ToAddress { get; }
    public byte StrengthCode { get; }
}
