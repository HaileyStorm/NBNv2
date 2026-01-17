using Nbn.Shared.Packing;

namespace Nbn.Shared.Format;

public sealed class NbnRegionSection
{
    public NbnRegionSection(
        byte regionId,
        uint neuronSpan,
        ulong totalAxons,
        uint stride,
        uint checkpointCount,
        ulong[] checkpoints,
        NeuronRecord[] neuronRecords,
        AxonRecord[] axonRecords)
    {
        RegionId = regionId;
        NeuronSpan = neuronSpan;
        TotalAxons = totalAxons;
        Stride = stride;
        CheckpointCount = checkpointCount;
        Checkpoints = checkpoints;
        NeuronRecords = neuronRecords;
        AxonRecords = axonRecords;
        ByteLength = NbnBinary.GetNbnRegionSectionSize(neuronSpan, totalAxons, stride);
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