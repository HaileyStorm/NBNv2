namespace Nbn.Shared.Format;

public readonly struct NbnRegionDirectoryEntry
{
    public NbnRegionDirectoryEntry(uint neuronSpan, ulong totalAxons, ulong offset, uint flags)
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