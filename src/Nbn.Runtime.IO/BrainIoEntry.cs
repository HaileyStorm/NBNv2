using Nbn.Proto;
using Proto;

namespace Nbn.Runtime.IO;

internal sealed class BrainIoEntry
{
    public BrainIoEntry(Guid brainId, PID inputPid, PID outputPid, uint inputWidth, uint outputWidth, BrainEnergyState energy)
    {
        BrainId = brainId;
        InputPid = inputPid;
        OutputPid = outputPid;
        InputWidth = inputWidth;
        OutputWidth = outputWidth;
        Energy = energy;
    }

    public Guid BrainId { get; }
    public PID InputPid { get; set; }
    public PID OutputPid { get; set; }
    public uint InputWidth { get; set; }
    public uint OutputWidth { get; set; }
    public BrainEnergyState Energy { get; }
    public ArtifactRef? BaseDefinition { get; set; }
    public ArtifactRef? LastSnapshot { get; set; }
    public bool EnergyDepletedSignaled { get; set; }
    public ulong? LastAppliedTickCostId { get; set; }
}
