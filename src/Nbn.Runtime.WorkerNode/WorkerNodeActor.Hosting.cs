using Proto;
using SharedShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Runtime.WorkerNode;

public sealed partial class WorkerNodeActor
{
    private BrainHostingState GetOrCreateBrainState(Guid brainId)
    {
        if (_brains.TryGetValue(brainId, out var brain))
        {
            return brain;
        }

        brain = new BrainHostingState(brainId);
        _brains[brainId] = brain;
        return brain;
    }

    private void ResetBrainState(IContext context, BrainHostingState brain)
    {
        var toStop = new Dictionary<string, PID>(StringComparer.Ordinal);
        AddPid(toStop, brain.BrainRootPid);
        AddPid(toStop, brain.SignalRouterPid);
        AddPid(toStop, brain.InputCoordinatorPid);
        AddPid(toStop, brain.OutputCoordinatorPid);

        foreach (var shard in brain.RegionShards.Values)
        {
            AddPid(toStop, shard.Pid);
        }

        foreach (var pid in toStop.Values)
        {
            context.Stop(pid);
        }

        foreach (var assignmentId in brain.Assignments.Keys.ToArray())
        {
            _assignments.Remove(assignmentId);
        }

        brain.Assignments.Clear();
        brain.RegionShards.Clear();
        brain.BrainRootPid = null;
        brain.SignalRouterPid = null;
        brain.InputCoordinatorPid = null;
        brain.OutputCoordinatorPid = null;
        brain.RuntimeInfo = null;
    }

    private void RemoveBrainStateIfEmpty(BrainHostingState brain)
    {
        if (brain.BrainRootPid is not null
            || brain.SignalRouterPid is not null
            || brain.InputCoordinatorPid is not null
            || brain.OutputCoordinatorPid is not null
            || brain.Assignments.Count > 0
            || brain.RegionShards.Count > 0)
        {
            return;
        }

        _brains.Remove(brain.BrainId);
    }

    private static void AddPid(Dictionary<string, PID> toStop, PID? pid)
    {
        if (pid is null)
        {
            return;
        }

        toStop[PidLabel(pid)] = pid;
    }
}
