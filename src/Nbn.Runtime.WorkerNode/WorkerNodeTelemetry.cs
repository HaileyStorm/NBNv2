using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Nbn.Runtime.WorkerNode;

/// <summary>
/// Emits placement, discovery, and capability-adjacent telemetry for WorkerNode runtime flows.
/// </summary>
public static class WorkerNodeTelemetry
{
    private const string MeterName = "Nbn.Runtime.WorkerNode";
    private const ulong DiscoveryPlacementEpoch = 0;
    private const string ReconcileTarget = "reconcile";
    private const string DiscoveryTarget = "discovery";

    private static readonly Meter Meter = new(MeterName);
    private static readonly ActivitySource ActivitySource = new(MeterName);

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

    /// <summary>
    /// Gets the <see cref="Meter"/> name used by WorkerNode runtime metrics.
    /// </summary>
    public static string MeterNameValue => Meter.Name;

    /// <summary>
    /// Records a successful placement assignment and the time it took to host.
    /// </summary>
    public static void RecordPlacementAssignmentHostedAccepted(
        Guid workerNodeId,
        Guid brainId,
        ulong placementEpoch,
        string target,
        string outcome,
        double hostingMs)
    {
        var dimensions = CreateCommonDimensions(
            workerNodeId,
            brainId == Guid.Empty ? null : brainId,
            placementEpoch,
            NormalizeLabel(target, "unknown"));
        var acceptedOutcome = NormalizeLabel(outcome, "ready");
        var metricTags = BuildTags(
            dimensions,
            new KeyValuePair<string, object?>("outcome", acceptedOutcome),
            new KeyValuePair<string, object?>("failure_reason", "none"));

        PlacementAssignmentHostedAccepted.Add(1, metricTags);

        var normalizedHostingMs = Math.Max(0, hostingMs);
        PlacementAssignmentHostingMs.Record(normalizedHostingMs, metricTags);

        using var activity = ActivitySource.StartActivity("workernode.placement.assignment.hosted.accepted");
        ApplyPlacementTags(activity, dimensions);
        activity?.SetTag("placement.outcome", acceptedOutcome);
        activity?.SetTag("placement.failure_reason", "none");
        activity?.SetTag("placement.hosting_ms", normalizedHostingMs);
    }

    /// <summary>
    /// Records a failed placement assignment attempt.
    /// </summary>
    public static void RecordPlacementAssignmentHostedFailed(
        Guid workerNodeId,
        Guid? brainId,
        ulong placementEpoch,
        string target,
        string failureReason)
    {
        var dimensions = CreateCommonDimensions(
            workerNodeId,
            brainId,
            placementEpoch,
            NormalizeLabel(target, "unknown"));
        var reason = NormalizeLabel(failureReason, "unknown");

        PlacementAssignmentHostedFailed.Add(
            1,
            BuildTags(dimensions, new KeyValuePair<string, object?>("failure_reason", reason)));

        using var activity = ActivitySource.StartActivity("workernode.placement.assignment.hosted.failed");
        ApplyPlacementTags(activity, dimensions);
        activity?.SetTag("placement.failure_reason", reason);
    }

    /// <summary>
    /// Records a placement reconcile request received by the worker.
    /// </summary>
    public static void RecordPlacementReconcileRequested(Guid workerNodeId, Guid? brainId, ulong placementEpoch)
    {
        var dimensions = CreateCommonDimensions(workerNodeId, brainId, placementEpoch, ReconcileTarget);

        PlacementReconcileRequested.Add(
            1,
            BuildTags(dimensions, new KeyValuePair<string, object?>("failure_reason", "none")));

        using var activity = ActivitySource.StartActivity("workernode.placement.reconcile.requested");
        ApplyPlacementTags(activity, dimensions);
        activity?.SetTag("placement.failure_reason", "none");
    }

    /// <summary>
    /// Records the outcome of a placement reconcile report emitted by the worker.
    /// </summary>
    public static void RecordPlacementReconcileReported(
        Guid workerNodeId,
        Guid? brainId,
        ulong placementEpoch,
        int assignmentCount,
        string outcome,
        string failureReason)
    {
        var dimensions = CreateCommonDimensions(workerNodeId, brainId, placementEpoch, ReconcileTarget);
        var reconcileOutcome = NormalizeLabel(outcome, "matched");
        var reason = NormalizeLabel(failureReason, "none");
        var normalizedAssignmentCount = Math.Max(0, assignmentCount);

        PlacementReconcileReported.Add(
            1,
            BuildTags(
                dimensions,
                new KeyValuePair<string, object?>("failure_reason", reason),
                new KeyValuePair<string, object?>("outcome", reconcileOutcome),
                new KeyValuePair<string, object?>("assignment_count", normalizedAssignmentCount)));

        using var activity = ActivitySource.StartActivity("workernode.placement.reconcile.reported");
        ApplyPlacementTags(activity, dimensions);
        activity?.SetTag("placement.failure_reason", reason);
        activity?.SetTag("placement.outcome", reconcileOutcome);
        activity?.SetTag("placement.assignment_count", normalizedAssignmentCount);
    }

    /// <summary>
    /// Records a discovery endpoint registration, removal, or invalidation observation.
    /// </summary>
    public static void RecordDiscoveryEndpointObserved(
        Guid workerNodeId,
        string target,
        string outcome,
        string failureReason)
    {
        var dimensions = CreateCommonDimensions(
            workerNodeId,
            brainId: null,
            DiscoveryPlacementEpoch,
            NormalizeLabel(target, DiscoveryTarget));
        var observationOutcome = NormalizeLabel(outcome, "updated");
        var reason = NormalizeLabel(failureReason, "none");

        DiscoveryEndpointObserved.Add(
            1,
            BuildTags(
                dimensions,
                new KeyValuePair<string, object?>("failure_reason", reason),
                new KeyValuePair<string, object?>("outcome", observationOutcome)));

        using var activity = ActivitySource.StartActivity("workernode.discovery.endpoint.observed");
        ApplyPlacementTags(activity, dimensions);
        activity?.SetTag("placement.failure_reason", reason);
        activity?.SetTag("discovery.outcome", observationOutcome);
    }

    /// <summary>
    /// Records whether a discovery endpoint lookup resolved directly or fell back to a hint.
    /// </summary>
    public static void RecordDiscoveryEndpointResolve(
        Guid workerNodeId,
        Guid? brainId,
        ulong placementEpoch,
        string target,
        string outcome,
        string failureReason)
    {
        var dimensions = CreateCommonDimensions(
            workerNodeId,
            brainId,
            placementEpoch,
            NormalizeLabel(target, DiscoveryTarget));
        var resolveOutcome = NormalizeLabel(outcome, "resolved");
        var reason = NormalizeLabel(failureReason, "none");

        DiscoveryEndpointResolve.Add(
            1,
            BuildTags(
                dimensions,
                new KeyValuePair<string, object?>("failure_reason", reason),
                new KeyValuePair<string, object?>("outcome", resolveOutcome)));

        using var activity = ActivitySource.StartActivity("workernode.discovery.endpoint.resolve");
        ApplyPlacementTags(activity, dimensions);
        activity?.SetTag("placement.failure_reason", reason);
        activity?.SetTag("discovery.outcome", resolveOutcome);
    }

    private static CommonDimensions CreateCommonDimensions(
        Guid workerNodeId,
        Guid? brainId,
        ulong placementEpoch,
        string target)
        => new(
            workerNodeId.ToString("D"),
            FormatBrainId(brainId),
            (long)placementEpoch,
            target);

    private static KeyValuePair<string, object?>[] BuildTags(
        CommonDimensions dimensions,
        params KeyValuePair<string, object?>[] extraTags)
    {
        var tags = new KeyValuePair<string, object?>[4 + extraTags.Length];
        tags[0] = new KeyValuePair<string, object?>("worker_node_id", dimensions.WorkerNodeId);
        tags[1] = new KeyValuePair<string, object?>("brain_id", dimensions.BrainId);
        tags[2] = new KeyValuePair<string, object?>("placement_epoch", dimensions.PlacementEpoch);
        tags[3] = new KeyValuePair<string, object?>("target", dimensions.Target);
        for (var i = 0; i < extraTags.Length; i++)
        {
            tags[4 + i] = extraTags[i];
        }

        return tags;
    }

    private static void ApplyPlacementTags(Activity? activity, CommonDimensions dimensions)
    {
        activity?.SetTag("worker_node.id", dimensions.WorkerNodeId);
        activity?.SetTag("brain.id", dimensions.BrainId);
        activity?.SetTag("placement.epoch", dimensions.PlacementEpoch);
        activity?.SetTag("placement.target", dimensions.Target);
    }

    private static string NormalizeLabel(string value, string fallback)
        => string.IsNullOrWhiteSpace(value) ? fallback : value;

    private static string FormatBrainId(Guid? brainId)
        => brainId.HasValue && brainId.Value != Guid.Empty
            ? brainId.Value.ToString("D")
            : string.Empty;

    private readonly record struct CommonDimensions(
        string WorkerNodeId,
        string BrainId,
        long PlacementEpoch,
        string Target);
}
