using Nbn.Shared.Quantization;

namespace Nbn.Shared.Format;

public sealed class NbnHeaderV2
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
        NbnRegionDirectoryEntry[] regions)
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
    public NbnRegionDirectoryEntry[] Regions { get; }

    public int HeaderByteCount => 1 << HeaderBytesPow2;
}