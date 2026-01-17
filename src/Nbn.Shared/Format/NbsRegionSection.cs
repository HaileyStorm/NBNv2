namespace Nbn.Shared.Format;

public sealed class NbsRegionSection
{
    public NbsRegionSection(byte regionId, uint neuronSpan, short[] bufferCodes, byte[]? enabledBitset)
    {
        RegionId = regionId;
        NeuronSpan = neuronSpan;
        BufferCodes = bufferCodes;
        EnabledBitset = enabledBitset;
        ByteLength = NbnBinary.GetNbsRegionSectionSize(neuronSpan, enabledBitset is not null);
    }

    public byte RegionId { get; }
    public uint NeuronSpan { get; }
    public short[] BufferCodes { get; }
    public byte[]? EnabledBitset { get; }
    public int ByteLength { get; }
}