using System;
using System.Buffers.Binary;

namespace Nbn.Shared.Packing;

public readonly struct AxonRecord
{
    private const int StrengthMask = 0x1F;
    private const int TargetNeuronMask = 0x3FFFFF;
    private const int TargetNeuronShift = 5;
    private const int TargetRegionShift = 27;

    public AxonRecord(byte strengthCode, int targetNeuronId, byte targetRegionId)
    {
        if (strengthCode > StrengthMask)
        {
            throw new ArgumentOutOfRangeException(nameof(strengthCode), strengthCode, "Strength code must fit in 5 bits.");
        }

        if (targetNeuronId < 0 || targetNeuronId > NbnConstants.MaxAxonTargetNeuronId)
        {
            throw new ArgumentOutOfRangeException(nameof(targetNeuronId), targetNeuronId, "Target neuron id must fit in 22 bits.");
        }

        if (targetRegionId > NbnConstants.RegionMaxId)
        {
            throw new ArgumentOutOfRangeException(nameof(targetRegionId), targetRegionId, "Target region id must fit in 5 bits.");
        }

        StrengthCode = strengthCode;
        TargetNeuronId = targetNeuronId;
        TargetRegionId = targetRegionId;
    }

    public byte StrengthCode { get; }
    public int TargetNeuronId { get; }
    public byte TargetRegionId { get; }

    public uint Pack()
    {
        var packed = (uint)(StrengthCode & StrengthMask);
        packed |= (uint)(TargetNeuronId & TargetNeuronMask) << TargetNeuronShift;
        packed |= (uint)(TargetRegionId & NbnConstants.RegionMaxId) << TargetRegionShift;
        return packed;
    }

    public void WriteTo(Span<byte> destination)
    {
        if (destination.Length < NbnConstants.AxonRecordBytes)
        {
            throw new ArgumentException("Destination span is too small.", nameof(destination));
        }

        BinaryPrimitives.WriteUInt32LittleEndian(destination, Pack());
    }

    public static AxonRecord FromPacked(uint packed)
    {
        var strength = (byte)(packed & StrengthMask);
        var targetNeuronId = (int)((packed >> TargetNeuronShift) & TargetNeuronMask);
        var targetRegionId = (byte)((packed >> TargetRegionShift) & NbnConstants.RegionMaxId);

        return new AxonRecord(strength, targetNeuronId, targetRegionId);
    }

    public static AxonRecord Read(ReadOnlySpan<byte> source)
    {
        if (source.Length < NbnConstants.AxonRecordBytes)
        {
            throw new ArgumentException("Source span is too small.", nameof(source));
        }

        var packed = BinaryPrimitives.ReadUInt32LittleEndian(source);
        return FromPacked(packed);
    }
}