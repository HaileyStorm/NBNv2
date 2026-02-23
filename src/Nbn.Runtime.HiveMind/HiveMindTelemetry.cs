using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Collections.Generic;

namespace Nbn.Runtime.HiveMind;

public static class HiveMindTelemetry
{
    private const string MeterName = "Nbn.Runtime.HiveMind";

    private static readonly Meter Meter = new(MeterName);
    public static readonly ActivitySource ActivitySource = new(MeterName);
    public static string MeterNameValue => Meter.Name;

    private static readonly Counter<long> TickCompleted = Meter.CreateCounter<long>("nbn.hivemind.tick.completed");
    private static readonly Histogram<double> TickComputeMs = Meter.CreateHistogram<double>("nbn.hivemind.tick.compute.ms");
    private static readonly Histogram<double> TickDeliverMs = Meter.CreateHistogram<double>("nbn.hivemind.tick.deliver.ms");
    private static readonly Counter<long> TickComputeTimeouts = Meter.CreateCounter<long>("nbn.hivemind.tick.compute.timeouts");
    private static readonly Counter<long> TickDeliverTimeouts = Meter.CreateCounter<long>("nbn.hivemind.tick.deliver.timeouts");
    private static readonly Counter<long> LateCompute = Meter.CreateCounter<long>("nbn.hivemind.tick.compute.late");
    private static readonly Counter<long> LateDeliver = Meter.CreateCounter<long>("nbn.hivemind.tick.deliver.late");
    private static readonly Counter<long> LateComputeAfterCompletion = Meter.CreateCounter<long>("nbn.hivemind.tick.compute.late.after");
    private static readonly Counter<long> LateDeliverAfterCompletion = Meter.CreateCounter<long>("nbn.hivemind.tick.deliver.late.after");
    private static readonly Histogram<double> TargetTickHz = Meter.CreateHistogram<double>("nbn.hivemind.tick.target.hz");
    private static readonly Counter<long> RescheduleRequested = Meter.CreateCounter<long>("nbn.hivemind.reschedule.requested");
    private static readonly Counter<long> PauseRequested = Meter.CreateCounter<long>("nbn.hivemind.pause.requested");
    private static readonly Counter<long> BrainTickCostTotal = Meter.CreateCounter<long>("nbn.hivemind.brain.tick_cost.total");
    private static readonly Counter<long> BrainEnergyDepleted = Meter.CreateCounter<long>("nbn.hivemind.brain.energy.depleted");
    private static readonly Counter<long> SnapshotOverlayRecords = Meter.CreateCounter<long>("nbn.hivemind.snapshot.overlay.records");
    private static readonly Counter<long> RebaseOverlayRecords = Meter.CreateCounter<long>("nbn.hivemind.rebase.overlay.records");
    private static readonly Counter<long> PlacementRequestAccepted = Meter.CreateCounter<long>("nbn.hivemind.placement.request.accepted");
    private static readonly Counter<long> PlacementRequestRejected = Meter.CreateCounter<long>("nbn.hivemind.placement.request.rejected");
    private static readonly Counter<long> PlacementAssignmentDispatch = Meter.CreateCounter<long>("nbn.hivemind.placement.assignment.dispatch");
    private static readonly Counter<long> PlacementAssignmentDispatchFailed = Meter.CreateCounter<long>("nbn.hivemind.placement.assignment.dispatch.failed");
    private static readonly Counter<long> PlacementAssignmentAck = Meter.CreateCounter<long>("nbn.hivemind.placement.assignment.ack");
    private static readonly Counter<long> PlacementAssignmentRetry = Meter.CreateCounter<long>("nbn.hivemind.placement.assignment.retry");
    private static readonly Counter<long> PlacementAssignmentTimeout = Meter.CreateCounter<long>("nbn.hivemind.placement.assignment.timeout");
    private static readonly Counter<long> PlacementReconcileMatched = Meter.CreateCounter<long>("nbn.hivemind.placement.reconcile.matched");
    private static readonly Counter<long> PlacementReconcileFailed = Meter.CreateCounter<long>("nbn.hivemind.placement.reconcile.failed");
    private static readonly Counter<long> PlacementReconcileTimeout = Meter.CreateCounter<long>("nbn.hivemind.placement.reconcile.timeout");
    private static readonly Histogram<double> PlacementAssignmentAckLatencyMs = Meter.CreateHistogram<double>("nbn.hivemind.placement.assignment.ack_latency.ms");
    private static readonly Histogram<double> PlacementAssignmentReadyLatencyMs = Meter.CreateHistogram<double>("nbn.hivemind.placement.assignment.ready_latency.ms");

    public static void RecordTickOutcome(TickOutcome outcome, float targetTickHz)
    {
        TickCompleted.Add(1);
        TickComputeMs.Record(outcome.ComputeDuration.TotalMilliseconds);
        TickDeliverMs.Record(outcome.DeliverDuration.TotalMilliseconds);
        TargetTickHz.Record(targetTickHz);

        if (outcome.ComputeTimedOut)
        {
            TickComputeTimeouts.Add(1);
        }

        if (outcome.DeliverTimedOut)
        {
            TickDeliverTimeouts.Add(1);
        }

        if (outcome.LateComputeCount > 0)
        {
            LateCompute.Add(outcome.LateComputeCount);
        }

        if (outcome.LateDeliverCount > 0)
        {
            LateDeliver.Add(outcome.LateDeliverCount);
        }
    }

    public static void RecordReschedule(string? reason)
    {
        RescheduleRequested.Add(1);
        using var activity = ActivitySource.StartActivity("hivemind.reschedule");
        activity?.SetTag("reason", reason);
    }

    public static void RecordPause(string? reason)
    {
        PauseRequested.Add(1);
        using var activity = ActivitySource.StartActivity("hivemind.pause");
        activity?.SetTag("reason", reason);
    }

    public static void RecordLateComputeAfterCompletion(int count = 1)
    {
        if (count <= 0)
        {
            return;
        }

        LateComputeAfterCompletion.Add(count);
    }

    public static void RecordLateDeliverAfterCompletion(int count = 1)
    {
        if (count <= 0)
        {
            return;
        }

        LateDeliverAfterCompletion.Add(count);
    }

    public static void RecordBrainTickCost(Guid brainId, long tickCost)
    {
        if (tickCost <= 0)
        {
            return;
        }

        BrainTickCostTotal.Add(
            tickCost,
            new KeyValuePair<string, object?>("brain_id", brainId.ToString("D")));
    }

    public static void RecordEnergyDepleted(Guid brainId)
    {
        BrainEnergyDepleted.Add(
            1,
            new KeyValuePair<string, object?>("brain_id", brainId.ToString("D")));

        using var activity = ActivitySource.StartActivity("hivemind.energy.depleted");
        activity?.SetTag("brain.id", brainId.ToString("D"));
    }

    public static void RecordSnapshotOverlayRecords(Guid brainId, int overlayCount)
    {
        if (overlayCount <= 0)
        {
            return;
        }

        SnapshotOverlayRecords.Add(
            overlayCount,
            new KeyValuePair<string, object?>("brain_id", brainId.ToString("D")));
    }

    public static void RecordRebaseOverlayRecords(Guid brainId, int overlayCount)
    {
        if (overlayCount <= 0)
        {
            return;
        }

        RebaseOverlayRecords.Add(
            overlayCount,
            new KeyValuePair<string, object?>("brain_id", brainId.ToString("D")));
    }

    public static void RecordPlacementRequestAccepted(Guid brainId, ulong placementEpoch, int assignmentCount, int workerCount)
    {
        var brain = brainId.ToString("D");
        PlacementRequestAccepted.Add(
            1,
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("assignment_count", assignmentCount),
            new KeyValuePair<string, object?>("worker_count", workerCount));

        using var activity = ActivitySource.StartActivity("hivemind.placement.request.accepted");
        activity?.SetTag("brain.id", brain);
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.assignments", assignmentCount);
        activity?.SetTag("placement.workers", workerCount);
    }

    public static void RecordPlacementRequestRejected(Guid? brainId, ulong placementEpoch, string failureReason)
    {
        var reason = string.IsNullOrWhiteSpace(failureReason) ? "unknown" : failureReason;
        var brain = brainId.HasValue && brainId.Value != Guid.Empty
            ? brainId.Value.ToString("D")
            : string.Empty;

        PlacementRequestRejected.Add(
            1,
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("failure_reason", reason));

        using var activity = ActivitySource.StartActivity("hivemind.placement.request.rejected");
        activity?.SetTag("brain.id", brain);
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.failure_reason", reason);
    }

    public static void RecordPlacementAssignmentDispatch(Guid brainId, ulong placementEpoch, string target, int attempt)
    {
        var brain = brainId.ToString("D");
        var assignmentTarget = string.IsNullOrWhiteSpace(target) ? "unknown" : target;
        PlacementAssignmentDispatch.Add(
            1,
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("target", assignmentTarget),
            new KeyValuePair<string, object?>("attempt", attempt));

        using var activity = ActivitySource.StartActivity("hivemind.placement.assignment.dispatch");
        activity?.SetTag("brain.id", brain);
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.target", assignmentTarget);
        activity?.SetTag("placement.attempt", attempt);
    }

    public static void RecordPlacementAssignmentDispatchFailed(
        Guid brainId,
        ulong placementEpoch,
        string target,
        int attempt,
        string failureReason)
    {
        var brain = brainId.ToString("D");
        var assignmentTarget = string.IsNullOrWhiteSpace(target) ? "unknown" : target;
        var reason = string.IsNullOrWhiteSpace(failureReason) ? "unknown" : failureReason;
        PlacementAssignmentDispatchFailed.Add(
            1,
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("target", assignmentTarget),
            new KeyValuePair<string, object?>("attempt", attempt),
            new KeyValuePair<string, object?>("failure_reason", reason));

        using var activity = ActivitySource.StartActivity("hivemind.placement.assignment.dispatch_failed");
        activity?.SetTag("brain.id", brain);
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.target", assignmentTarget);
        activity?.SetTag("placement.attempt", attempt);
        activity?.SetTag("placement.failure_reason", reason);
    }

    public static void RecordPlacementAssignmentAck(
        Guid brainId,
        ulong placementEpoch,
        string target,
        string state,
        bool accepted,
        bool retryable,
        double latencyMs)
    {
        var brain = brainId.ToString("D");
        var assignmentTarget = string.IsNullOrWhiteSpace(target) ? "unknown" : target;
        var assignmentState = string.IsNullOrWhiteSpace(state) ? "unknown" : state;
        var normalizedLatencyMs = Math.Max(0, latencyMs);
        PlacementAssignmentAck.Add(
            1,
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("target", assignmentTarget),
            new KeyValuePair<string, object?>("state", assignmentState),
            new KeyValuePair<string, object?>("accepted", accepted),
            new KeyValuePair<string, object?>("retryable", retryable));

        PlacementAssignmentAckLatencyMs.Record(
            normalizedLatencyMs,
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("target", assignmentTarget),
            new KeyValuePair<string, object?>("state", assignmentState));

        using var activity = ActivitySource.StartActivity("hivemind.placement.assignment.ack");
        activity?.SetTag("brain.id", brain);
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.target", assignmentTarget);
        activity?.SetTag("placement.state", assignmentState);
        activity?.SetTag("placement.accepted", accepted);
        activity?.SetTag("placement.retryable", retryable);
        activity?.SetTag("placement.ack_latency_ms", normalizedLatencyMs);
    }

    public static void RecordPlacementAssignmentReadyLatency(Guid brainId, ulong placementEpoch, string target, double latencyMs)
    {
        if (latencyMs < 0)
        {
            return;
        }

        var brain = brainId.ToString("D");
        var assignmentTarget = string.IsNullOrWhiteSpace(target) ? "unknown" : target;
        PlacementAssignmentReadyLatencyMs.Record(
            latencyMs,
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("target", assignmentTarget));
    }

    public static void RecordPlacementAssignmentRetry(Guid brainId, ulong placementEpoch, string target, int nextAttempt, string reason)
    {
        var brain = brainId.ToString("D");
        var assignmentTarget = string.IsNullOrWhiteSpace(target) ? "unknown" : target;
        var retryReason = string.IsNullOrWhiteSpace(reason) ? "unknown" : reason;
        PlacementAssignmentRetry.Add(
            1,
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("target", assignmentTarget),
            new KeyValuePair<string, object?>("attempt", nextAttempt),
            new KeyValuePair<string, object?>("reason", retryReason));

        using var activity = ActivitySource.StartActivity("hivemind.placement.assignment.retry");
        activity?.SetTag("brain.id", brain);
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.target", assignmentTarget);
        activity?.SetTag("placement.attempt", nextAttempt);
        activity?.SetTag("placement.retry_reason", retryReason);
    }

    public static void RecordPlacementAssignmentTimeout(Guid brainId, ulong placementEpoch, string target, int attempt, bool willRetry)
    {
        var brain = brainId.ToString("D");
        var assignmentTarget = string.IsNullOrWhiteSpace(target) ? "unknown" : target;
        PlacementAssignmentTimeout.Add(
            1,
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("target", assignmentTarget),
            new KeyValuePair<string, object?>("attempt", attempt),
            new KeyValuePair<string, object?>("will_retry", willRetry));

        using var activity = ActivitySource.StartActivity("hivemind.placement.assignment.timeout");
        activity?.SetTag("brain.id", brain);
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.target", assignmentTarget);
        activity?.SetTag("placement.attempt", attempt);
        activity?.SetTag("placement.will_retry", willRetry);
    }

    public static void RecordPlacementReconcileMatched(Guid brainId, ulong placementEpoch, int observedAssignments)
    {
        var brain = brainId.ToString("D");
        PlacementReconcileMatched.Add(
            1,
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("observed_assignments", observedAssignments));

        using var activity = ActivitySource.StartActivity("hivemind.placement.reconcile.matched");
        activity?.SetTag("brain.id", brain);
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.observed_assignments", observedAssignments);
    }

    public static void RecordPlacementReconcileFailed(Guid brainId, ulong placementEpoch, string failureReason)
    {
        var brain = brainId.ToString("D");
        var reason = string.IsNullOrWhiteSpace(failureReason) ? "unknown" : failureReason;
        PlacementReconcileFailed.Add(
            1,
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("failure_reason", reason));

        using var activity = ActivitySource.StartActivity("hivemind.placement.reconcile.failed");
        activity?.SetTag("brain.id", brain);
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.failure_reason", reason);
    }

    public static void RecordPlacementReconcileTimeout(Guid brainId, ulong placementEpoch, int pendingWorkers)
    {
        PlacementReconcileTimeout.Add(
            1,
            new KeyValuePair<string, object?>("brain_id", brainId.ToString("D")),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("pending_workers", pendingWorkers));

        using var activity = ActivitySource.StartActivity("hivemind.placement.reconcile.timeout");
        activity?.SetTag("brain.id", brainId.ToString("D"));
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.pending_workers", pendingWorkers);
    }
}
