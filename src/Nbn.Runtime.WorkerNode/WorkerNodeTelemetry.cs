using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Nbn.Runtime.WorkerNode;

public static class WorkerNodeTelemetry
{
    private const string MeterName = "Nbn.Runtime.WorkerNode";

    private static readonly Meter Meter = new(MeterName);
    private static readonly ActivitySource ActivitySource = new(MeterName);

    public static string MeterNameValue => Meter.Name;

    private static readonly Counter<long> PlacementAssignmentHostedAccepted =
        Meter.CreateCounter<long>("nbn.workernode.placement.assignment.hosted.accepted");

    private static readonly Counter<long> PlacementAssignmentHostedFailed =
        Meter.CreateCounter<long>("nbn.workernode.placement.assignment.hosted.failed");

    private static readonly Histogram<double> PlacementAssignmentHostingMs =
        Meter.CreateHistogram<double>("nbn.workernode.placement.assignment.hosting.ms");

    public static void RecordPlacementAssignmentHostedAccepted(
        Guid workerNodeId,
        Guid brainId,
        ulong placementEpoch,
        string target,
        string outcome,
        double hostingMs)
    {
        var worker = workerNodeId.ToString("D");
        var brain = brainId == Guid.Empty ? string.Empty : brainId.ToString("D");
        var assignmentTarget = string.IsNullOrWhiteSpace(target) ? "unknown" : target;
        var acceptedOutcome = string.IsNullOrWhiteSpace(outcome) ? "ready" : outcome;
        PlacementAssignmentHostedAccepted.Add(
            1,
            new KeyValuePair<string, object?>("worker_node_id", worker),
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("target", assignmentTarget),
            new KeyValuePair<string, object?>("outcome", acceptedOutcome));

        PlacementAssignmentHostingMs.Record(
            Math.Max(0, hostingMs),
            new KeyValuePair<string, object?>("worker_node_id", worker),
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("target", assignmentTarget),
            new KeyValuePair<string, object?>("outcome", acceptedOutcome));

        using var activity = ActivitySource.StartActivity("workernode.placement.assignment.hosted.accepted");
        activity?.SetTag("worker_node.id", worker);
        activity?.SetTag("brain.id", brain);
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.target", assignmentTarget);
        activity?.SetTag("placement.outcome", acceptedOutcome);
        activity?.SetTag("placement.hosting_ms", Math.Max(0, hostingMs));
    }

    public static void RecordPlacementAssignmentHostedFailed(
        Guid workerNodeId,
        Guid? brainId,
        ulong placementEpoch,
        string target,
        string failureReason)
    {
        var worker = workerNodeId.ToString("D");
        var brain = brainId.HasValue && brainId.Value != Guid.Empty
            ? brainId.Value.ToString("D")
            : string.Empty;
        var assignmentTarget = string.IsNullOrWhiteSpace(target) ? "unknown" : target;
        var reason = string.IsNullOrWhiteSpace(failureReason) ? "unknown" : failureReason;
        PlacementAssignmentHostedFailed.Add(
            1,
            new KeyValuePair<string, object?>("worker_node_id", worker),
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("target", assignmentTarget),
            new KeyValuePair<string, object?>("failure_reason", reason));

        using var activity = ActivitySource.StartActivity("workernode.placement.assignment.hosted.failed");
        activity?.SetTag("worker_node.id", worker);
        activity?.SetTag("brain.id", brain);
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.target", assignmentTarget);
        activity?.SetTag("placement.failure_reason", reason);
    }
}
