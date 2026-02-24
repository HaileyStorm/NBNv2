using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Nbn.Runtime.WorkerNode;

public static class WorkerNodeTelemetry
{
    private const string MeterName = "Nbn.Runtime.WorkerNode";
    private const ulong DiscoveryPlacementEpoch = 0;
    private const string ReconcileTarget = "reconcile";
    private const string DiscoveryTarget = "discovery";

    private static readonly Meter Meter = new(MeterName);
    private static readonly ActivitySource ActivitySource = new(MeterName);

    public static string MeterNameValue => Meter.Name;

    private static readonly Counter<long> PlacementAssignmentHostedAccepted =
        Meter.CreateCounter<long>("nbn.workernode.placement.assignment.hosted.accepted");

    private static readonly Counter<long> PlacementAssignmentHostedFailed =
        Meter.CreateCounter<long>("nbn.workernode.placement.assignment.hosted.failed");

    private static readonly Histogram<double> PlacementAssignmentHostingMs =
        Meter.CreateHistogram<double>("nbn.workernode.placement.assignment.hosting.ms");

    private static readonly Counter<long> PlacementReconcileRequested =
        Meter.CreateCounter<long>("nbn.workernode.placement.reconcile.requested");

    private static readonly Counter<long> PlacementReconcileReported =
        Meter.CreateCounter<long>("nbn.workernode.placement.reconcile.reported");

    private static readonly Counter<long> DiscoveryEndpointObserved =
        Meter.CreateCounter<long>("nbn.workernode.discovery.endpoint.observed");

    private static readonly Counter<long> DiscoveryEndpointResolve =
        Meter.CreateCounter<long>("nbn.workernode.discovery.endpoint.resolve");

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
            new KeyValuePair<string, object?>("outcome", acceptedOutcome),
            new KeyValuePair<string, object?>("failure_reason", "none"));

        PlacementAssignmentHostingMs.Record(
            Math.Max(0, hostingMs),
            new KeyValuePair<string, object?>("worker_node_id", worker),
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("target", assignmentTarget),
            new KeyValuePair<string, object?>("outcome", acceptedOutcome),
            new KeyValuePair<string, object?>("failure_reason", "none"));

        using var activity = ActivitySource.StartActivity("workernode.placement.assignment.hosted.accepted");
        activity?.SetTag("worker_node.id", worker);
        activity?.SetTag("brain.id", brain);
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.target", assignmentTarget);
        activity?.SetTag("placement.outcome", acceptedOutcome);
        activity?.SetTag("placement.failure_reason", "none");
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

    public static void RecordPlacementReconcileRequested(Guid workerNodeId, Guid? brainId, ulong placementEpoch)
    {
        var worker = workerNodeId.ToString("D");
        var brain = FormatBrainId(brainId);
        PlacementReconcileRequested.Add(
            1,
            new KeyValuePair<string, object?>("worker_node_id", worker),
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("target", ReconcileTarget),
            new KeyValuePair<string, object?>("failure_reason", "none"));

        using var activity = ActivitySource.StartActivity("workernode.placement.reconcile.requested");
        activity?.SetTag("worker_node.id", worker);
        activity?.SetTag("brain.id", brain);
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.target", ReconcileTarget);
        activity?.SetTag("placement.failure_reason", "none");
    }

    public static void RecordPlacementReconcileReported(
        Guid workerNodeId,
        Guid? brainId,
        ulong placementEpoch,
        int assignmentCount,
        string outcome,
        string failureReason)
    {
        var worker = workerNodeId.ToString("D");
        var brain = FormatBrainId(brainId);
        var reconcileOutcome = string.IsNullOrWhiteSpace(outcome) ? "matched" : outcome;
        var reason = string.IsNullOrWhiteSpace(failureReason) ? "none" : failureReason;

        PlacementReconcileReported.Add(
            1,
            new KeyValuePair<string, object?>("worker_node_id", worker),
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("target", ReconcileTarget),
            new KeyValuePair<string, object?>("failure_reason", reason),
            new KeyValuePair<string, object?>("outcome", reconcileOutcome),
            new KeyValuePair<string, object?>("assignment_count", Math.Max(0, assignmentCount)));

        using var activity = ActivitySource.StartActivity("workernode.placement.reconcile.reported");
        activity?.SetTag("worker_node.id", worker);
        activity?.SetTag("brain.id", brain);
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.target", ReconcileTarget);
        activity?.SetTag("placement.failure_reason", reason);
        activity?.SetTag("placement.outcome", reconcileOutcome);
        activity?.SetTag("placement.assignment_count", Math.Max(0, assignmentCount));
    }

    public static void RecordDiscoveryEndpointObserved(
        Guid workerNodeId,
        string target,
        string outcome,
        string failureReason)
    {
        var worker = workerNodeId.ToString("D");
        var discoveryTarget = string.IsNullOrWhiteSpace(target) ? DiscoveryTarget : target;
        var observationOutcome = string.IsNullOrWhiteSpace(outcome) ? "updated" : outcome;
        var reason = string.IsNullOrWhiteSpace(failureReason) ? "none" : failureReason;

        DiscoveryEndpointObserved.Add(
            1,
            new KeyValuePair<string, object?>("worker_node_id", worker),
            new KeyValuePair<string, object?>("brain_id", string.Empty),
            new KeyValuePair<string, object?>("placement_epoch", (long)DiscoveryPlacementEpoch),
            new KeyValuePair<string, object?>("target", discoveryTarget),
            new KeyValuePair<string, object?>("failure_reason", reason),
            new KeyValuePair<string, object?>("outcome", observationOutcome));

        using var activity = ActivitySource.StartActivity("workernode.discovery.endpoint.observed");
        activity?.SetTag("worker_node.id", worker);
        activity?.SetTag("brain.id", string.Empty);
        activity?.SetTag("placement.epoch", (long)DiscoveryPlacementEpoch);
        activity?.SetTag("placement.target", discoveryTarget);
        activity?.SetTag("placement.failure_reason", reason);
        activity?.SetTag("discovery.outcome", observationOutcome);
    }

    public static void RecordDiscoveryEndpointResolve(
        Guid workerNodeId,
        Guid? brainId,
        ulong placementEpoch,
        string target,
        string outcome,
        string failureReason)
    {
        var worker = workerNodeId.ToString("D");
        var brain = FormatBrainId(brainId);
        var resolveTarget = string.IsNullOrWhiteSpace(target) ? DiscoveryTarget : target;
        var resolveOutcome = string.IsNullOrWhiteSpace(outcome) ? "resolved" : outcome;
        var reason = string.IsNullOrWhiteSpace(failureReason) ? "none" : failureReason;

        DiscoveryEndpointResolve.Add(
            1,
            new KeyValuePair<string, object?>("worker_node_id", worker),
            new KeyValuePair<string, object?>("brain_id", brain),
            new KeyValuePair<string, object?>("placement_epoch", (long)placementEpoch),
            new KeyValuePair<string, object?>("target", resolveTarget),
            new KeyValuePair<string, object?>("failure_reason", reason),
            new KeyValuePair<string, object?>("outcome", resolveOutcome));

        using var activity = ActivitySource.StartActivity("workernode.discovery.endpoint.resolve");
        activity?.SetTag("worker_node.id", worker);
        activity?.SetTag("brain.id", brain);
        activity?.SetTag("placement.epoch", (long)placementEpoch);
        activity?.SetTag("placement.target", resolveTarget);
        activity?.SetTag("placement.failure_reason", reason);
        activity?.SetTag("discovery.outcome", resolveOutcome);
    }

    private static string FormatBrainId(Guid? brainId)
        => brainId.HasValue && brainId.Value != Guid.Empty
            ? brainId.Value.ToString("D")
            : string.Empty;
}
