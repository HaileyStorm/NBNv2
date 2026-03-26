using Nbn.Proto.Control;
using Nbn.Runtime.Brain;
using Nbn.Runtime.IO;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.WorkerNode;

public sealed partial class WorkerNodeActor
{
    private async Task<HostingResult> HostAssignmentAsync(
        IContext context,
        BrainHostingState brain,
        PlacementAssignment assignment)
    {
        try
        {
            return assignment.Target switch
            {
                PlacementAssignmentTarget.PlacementTargetBrainRoot
                    => HostBrainRoot(context, brain, assignment),
                PlacementAssignmentTarget.PlacementTargetSignalRouter
                    => HostSignalRouter(context, brain, assignment),
                PlacementAssignmentTarget.PlacementTargetInputCoordinator
                    => HostInputCoordinator(context, brain, assignment),
                PlacementAssignmentTarget.PlacementTargetOutputCoordinator
                    => HostOutputCoordinator(context, brain, assignment),
                PlacementAssignmentTarget.PlacementTargetRegionShard
                    => await HostRegionShardAsync(context, brain, assignment).ConfigureAwait(false),
                _ => HostingResult.Failed(FailedAck(
                    assignment.AssignmentId,
                    assignment.BrainId,
                    assignment.PlacementEpoch,
                    PlacementFailureReason.PlacementFailureInternalError,
                    $"unsupported placement target {(int)assignment.Target}"))
            };
        }
        catch (Exception ex)
        {
            var detail = string.IsNullOrWhiteSpace(ex.Message)
                ? ex.GetBaseException().Message
                : ex.Message;
            return HostingResult.Failed(FailedAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureInternalError,
                detail));
        }
    }

    private HostingResult HostBrainRoot(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        var actorName = ResolveActorName(assignment);
        var hiveMindPid = ResolveHiveMindPid(context);
        var pid = SpawnOrResolveNamed(
            context,
            actorName,
            Props.FromProducer(() => new BrainRootActor(brain.BrainId, hiveMindPid, autoSpawnSignalRouter: false)),
            brain.BrainRootPid);

        brain.BrainRootPid = pid;
        context.Watch(pid);

        if (brain.SignalRouterPid is not null)
        {
            context.Send(pid, new SetSignalRouter(brain.SignalRouterPid));
        }

        return HostingResult.Succeeded(assignment, pid);
    }

    private HostingResult HostSignalRouter(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        var actorName = ResolveActorName(assignment);
        var pid = SpawnOrResolveNamed(
            context,
            actorName,
            Props.FromProducer(() => new BrainSignalRouterActor(brain.BrainId)),
            brain.SignalRouterPid);

        brain.SignalRouterPid = pid;
        context.Watch(pid);

        if (brain.BrainRootPid is not null)
        {
            context.Send(brain.BrainRootPid, new SetSignalRouter(pid));
        }

        return HostingResult.Succeeded(assignment, pid);
    }

    private HostingResult HostInputCoordinator(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        var actorName = ResolveActorName(assignment);
        var inputWidth = ResolveInputWidth(brain);
        var pid = SpawnOrResolveNamed(
            context,
            actorName,
            Props.FromProducer(() => new InputCoordinatorActor(
                brain.BrainId,
                (uint)inputWidth,
                ProtoControl.InputCoordinatorMode.DirtyOnChange)),
            brain.InputCoordinatorPid);

        brain.InputCoordinatorPid = pid;
        context.Watch(pid);
        return HostingResult.Succeeded(assignment, pid);
    }

    private HostingResult HostOutputCoordinator(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        var actorName = ResolveActorName(assignment);
        var outputWidth = ResolveOutputWidth(brain);
        var pid = SpawnOrResolveNamed(
            context,
            actorName,
            Props.FromProducer(() => new OutputCoordinatorActor(brain.BrainId, (uint)outputWidth)),
            brain.OutputCoordinatorPid);

        brain.OutputCoordinatorPid = pid;
        context.Watch(pid);
        return HostingResult.Succeeded(assignment, pid);
    }

    private PID SpawnOrResolveNamed(IContext context, string actorName, Props props, PID? existingPid)
    {
        if (existingPid is not null && string.Equals(existingPid.Id, actorName, StringComparison.Ordinal))
        {
            return existingPid;
        }

        try
        {
            return context.SpawnNamed(props, actorName);
        }
        catch
        {
            return new PID(string.Empty, actorName);
        }
    }

    private static string ResolveActorName(PlacementAssignment assignment)
    {
        if (!string.IsNullOrWhiteSpace(assignment.ActorName))
        {
            var trimmed = assignment.ActorName.Trim();
            var slash = trimmed.LastIndexOf('/');
            if (slash >= 0 && slash < trimmed.Length - 1)
            {
                trimmed = trimmed[(slash + 1)..];
            }

            if (!string.IsNullOrWhiteSpace(trimmed))
            {
                return trimmed;
            }
        }

        return assignment.Target switch
        {
            PlacementAssignmentTarget.PlacementTargetBrainRoot => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-root",
            PlacementAssignmentTarget.PlacementTargetSignalRouter => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-router",
            PlacementAssignmentTarget.PlacementTargetInputCoordinator => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-input",
            PlacementAssignmentTarget.PlacementTargetOutputCoordinator => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-output",
            PlacementAssignmentTarget.PlacementTargetRegionShard => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-r{assignment.RegionId}-s{assignment.ShardIndex}",
            _ => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-assignment-{Math.Abs(assignment.AssignmentId.GetHashCode(StringComparison.Ordinal))}"
        };
    }
}
