using Google.Protobuf;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Shared;
using Proto;
using SharedShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Runtime.WorkerNode;

public sealed partial class WorkerNodeActor
{
    private sealed class BrainHostingState
    {
        public BrainHostingState(Guid brainId)
        {
            BrainId = brainId;
            BrainSeed = BitConverter.ToUInt64(brainId.ToByteArray(), 0);
        }

        public Guid BrainId { get; }
        public ulong BrainSeed { get; }
        public ulong PlacementEpoch { get; set; }
        public PID? BrainRootPid { get; set; }
        public PID? SignalRouterPid { get; set; }
        public PID? InputCoordinatorPid { get; set; }
        public PID? OutputCoordinatorPid { get; set; }
        public BrainRuntimeInfo? RuntimeInfo { get; set; }
        public Dictionary<SharedShardId32, HostedShard> RegionShards { get; } = new();
        public Dictionary<string, HostedAssignmentState> Assignments { get; } = new(StringComparer.Ordinal);
    }

    private sealed class BrainRuntimeInfo
    {
        public int InputWidth { get; set; }
        public int OutputWidth { get; set; }
        public ArtifactRef? BaseDefinition { get; set; }
        public ArtifactRef? LastSnapshot { get; set; }
        public bool HasIoMetadata { get; set; }
        public string LastIoError { get; set; } = string.Empty;
        public string LastArtifactLoadError { get; set; } = string.Empty;
    }

    private sealed class HostedAssignmentState
    {
        public HostedAssignmentState(PlacementAssignment assignment, PID? hostedPid)
        {
            Assignment = assignment;
            HostedPid = hostedPid;
        }

        public PlacementAssignment Assignment { get; }
        public PID? HostedPid { get; set; }
        public PlacementAssignmentState State { get; set; }
        public string Message { get; set; } = string.Empty;
    }

    private readonly record struct HostedShard(
        SharedShardId32 ShardId,
        int NeuronStart,
        int NeuronCount,
        PID Pid,
        string AssignmentId);

    private sealed class HostingResult
    {
        private HostingResult(bool success, PlacementAssignment? assignment, PID? hostedPid, PlacementAssignmentAck? failedAck)
        {
            Success = success;
            Assignment = assignment;
            HostedPid = hostedPid;
            FailedAck = failedAck;
        }

        public bool Success { get; }
        public PlacementAssignment? Assignment { get; }
        public PID? HostedPid { get; }
        public PlacementAssignmentAck? FailedAck { get; }

        public static HostingResult Succeeded(PlacementAssignment assignment, PID hostedPid)
            => new(true, assignment, hostedPid, null);

        public static HostingResult Failed(PlacementAssignmentAck failedAck)
            => new(false, null, null, failedAck);
    }
}

internal static class WorkerNodeUuidExtensions
{
    public static Guid ToGuidOrEmpty(this Uuid? value)
    {
        if (value is null || !value.TryToGuid(out var guid))
        {
            return Guid.Empty;
        }

        return guid;
    }
}
