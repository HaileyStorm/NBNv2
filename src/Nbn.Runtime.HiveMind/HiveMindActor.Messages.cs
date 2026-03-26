using Proto;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private static void ScheduleSelf(IContext context, TimeSpan delay, object message)
    {
        if (delay <= TimeSpan.Zero)
        {
            context.Send(context.Self, message);
            return;
        }

        context.ReenterAfter(Task.Delay(delay), _ =>
        {
            context.Send(context.Self, message);
            return Task.CompletedTask;
        });
    }

    private sealed record TickStart;
    private sealed record TickPhaseTimeout(ulong TickId, TickPhase Phase);
    private sealed record RefreshWorkerInventoryTick;
    private sealed record RefreshWorkerCapabilitiesTick;
    private sealed record SweepVisualizationSubscribers;
    private sealed record RescheduleNow(string Reason);
    private sealed record RescheduleCompleted(string Reason, bool Success);
    private sealed record RetryQueuedReschedule;
    private sealed record DispatchPlacementPlan(Guid BrainId, ulong PlacementEpoch);
    private sealed record RetryPlacementAssignment(Guid BrainId, ulong PlacementEpoch, string AssignmentId, int Attempt);
    private sealed record PlacementAssignmentTimeout(Guid BrainId, ulong PlacementEpoch, string AssignmentId, int Attempt);
    private sealed record PlacementReconcileTimeout(Guid BrainId, ulong PlacementEpoch);
    private sealed record SpawnCompletionTimeout(Guid BrainId, ulong PlacementEpoch);

    private enum TickPhase
    {
        Idle,
        Compute,
        Deliver
    }
}
