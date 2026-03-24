using System.Collections.Generic;
using Nbn.Proto;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.IO;

internal sealed class BrainIoEntry
{
    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    public BrainIoEntry(
        Guid brainId,
        PID inputPid,
        PID outputPid,
        string outputActorReference,
        bool ownsInputCoordinator,
        bool ownsOutputCoordinator,
        uint inputWidth,
        uint outputWidth,
        BrainEnergyState energy,
        ProtoControl.InputCoordinatorMode inputCoordinatorMode,
        ProtoControl.OutputVectorSource outputVectorSource)
    {
        BrainId = brainId;
        InputPid = inputPid;
        OutputPid = outputPid;
        OutputActorReference = outputActorReference;
        OwnsInputCoordinator = ownsInputCoordinator;
        OwnsOutputCoordinator = ownsOutputCoordinator;
        InputWidth = inputWidth;
        OutputWidth = outputWidth;
        Energy = energy;
        InputCoordinatorMode = inputCoordinatorMode;
        OutputVectorSource = outputVectorSource;
        InputState = new InputCoordinatorShadowState(brainId, inputWidth, inputCoordinatorMode);
        RegisteredAtMs = NowMs();
    }

    public Guid BrainId { get; }
    public PID InputPid { get; set; }
    public PID OutputPid { get; set; }
    public string OutputActorReference { get; set; }
    public bool OwnsInputCoordinator { get; set; }
    public bool OwnsOutputCoordinator { get; set; }
    public uint InputWidth { get; set; }
    public uint OutputWidth { get; set; }
    public BrainEnergyState Energy { get; }
    public ProtoControl.InputCoordinatorMode InputCoordinatorMode { get; set; }
    public ProtoControl.OutputVectorSource OutputVectorSource { get; set; }
    public InputCoordinatorShadowState InputState { get; }
    public Dictionary<string, OutputSubscriberRegistration> OutputSubscribers { get; } = new(StringComparer.Ordinal);
    public Dictionary<string, OutputSubscriberRegistration> OutputVectorSubscribers { get; } = new(StringComparer.Ordinal);
    public ArtifactRef? BaseDefinition { get; set; }
    public ArtifactRef? LastSnapshot { get; set; }
    public bool EnergyDepletedSignaled { get; set; }
    public ulong? LastAppliedTickCostId { get; set; }
    public long RegisteredAtMs { get; set; }
}

internal sealed record OutputSubscriberRegistration(string ActorReference, PID ResolvedPid);
