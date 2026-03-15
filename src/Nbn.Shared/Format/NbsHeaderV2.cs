using Nbn.Proto.Control;
using Nbn.Shared.Quantization;

namespace Nbn.Shared.Format;

public sealed class NbsHeaderV2
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
        QuantizationMap bufferMap,
        NbsHomeostasisConfig? homeostasisConfig = null)
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
        HomeostasisConfig = homeostasisConfig;
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
    public NbsHomeostasisConfig? HomeostasisConfig { get; }

    public int HeaderByteCount => 1 << HeaderBytesPow2;
    public bool HasHomeostasisConfig => HomeostasisConfig is not null;

    public bool EnabledBitsetIncluded => (Flags & 0x1u) != 0;
    public bool AxonOverlayIncluded => (Flags & 0x2u) != 0;
    public bool CostEnabled => (Flags & 0x4u) != 0;
    public bool EnergyEnabled => (Flags & 0x8u) != 0;
    public bool PlasticityEnabled => (Flags & 0x10u) != 0;
}

public sealed record NbsHomeostasisConfig(
    bool Enabled,
    HomeostasisTargetMode TargetMode,
    HomeostasisUpdateMode UpdateMode,
    float BaseProbability,
    uint MinStepCodes,
    bool EnergyCouplingEnabled,
    float EnergyTargetScale,
    float EnergyProbabilityScale);
