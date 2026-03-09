using System.Diagnostics;
using System.Diagnostics.Metrics;
using ProtoSpec = Nbn.Proto.Speciation;

namespace Nbn.Runtime.Speciation;

public static class SpeciationTelemetry
{
    private const string MeterName = "Nbn.Runtime.Speciation";
    private static readonly Meter Meter = new(MeterName);

    public static readonly ActivitySource ActivitySource = new(MeterName);

    public static string MeterNameValue => Meter.Name;

    private static readonly Counter<long> StartupReconcileTotal =
        Meter.CreateCounter<long>("nbn.speciation.startup.reconcile.total");

    private static readonly Counter<long> StartupReconcileMembershipsAdded =
        Meter.CreateCounter<long>("nbn.speciation.startup.reconcile.memberships.added");

    private static readonly Counter<long> StartupReconcileMembershipsExisting =
        Meter.CreateCounter<long>("nbn.speciation.startup.reconcile.memberships.existing");

    private static readonly Counter<long> AssignmentDecisions =
        Meter.CreateCounter<long>("nbn.speciation.assignment.decisions");

    private static readonly Histogram<double> AssignmentDurationMs =
        Meter.CreateHistogram<double>("nbn.speciation.assignment.duration.ms");

    private static readonly Counter<long> EpochTransitionTotal =
        Meter.CreateCounter<long>("nbn.speciation.epoch.transition.total");

    private static readonly Histogram<long> StatusMembershipCount =
        Meter.CreateHistogram<long>("nbn.speciation.status.membership_count");

    private static readonly Histogram<long> StatusSpeciesCount =
        Meter.CreateHistogram<long>("nbn.speciation.status.species_count");

    private static readonly Histogram<long> StatusLineageEdgeCount =
        Meter.CreateHistogram<long>("nbn.speciation.status.lineage_edge_count");

    public static Activity? StartAssignmentActivity(
        string operation,
        long epochId,
        ProtoSpec.SpeciationApplyMode applyMode)
    {
        var normalizedOperation = NormalizeLabel(operation, "unknown");
        var activity = ActivitySource.StartActivity($"speciation.{normalizedOperation}", ActivityKind.Internal);
        activity?.SetTag("speciation.operation", normalizedOperation);
        activity?.SetTag("speciation.epoch_id", Math.Max(0, epochId));
        activity?.SetTag("speciation.apply_mode", FormatApplyMode(applyMode));
        return activity;
    }

    public static void CompleteAssignmentActivity(
        Activity? activity,
        long epochId,
        ProtoSpec.SpeciationDecision decision,
        double durationMs)
    {
        if (activity is null)
        {
            return;
        }

        activity.SetTag("speciation.epoch_id", Math.Max(0, epochId));
        activity.SetTag("speciation.candidate_mode", FormatCandidateMode(decision.CandidateMode));
        activity.SetTag("speciation.decision_reason", NormalizeDecisionReason(decision.DecisionReason));
        activity.SetTag("speciation.failure_reason", FormatFailureReason(decision.FailureReason));
        activity.SetTag("speciation.success", decision.Success);
        activity.SetTag("speciation.created", decision.Created);
        activity.SetTag("speciation.committed", decision.Committed);
        activity.SetTag("speciation.immutable_conflict", decision.ImmutableConflict);
        activity.SetTag("speciation.species_id", NormalizeLabel(decision.SpeciesId, string.Empty));
        activity.SetTag("speciation.species_display_name", NormalizeLabel(decision.SpeciesDisplayName, string.Empty));
        activity.SetTag("speciation.duration_ms", Math.Max(0d, durationMs));
    }

    public static void RecordAssignmentDecision(
        string operation,
        ProtoSpec.SpeciationDecision decision,
        double durationMs)
    {
        var tags = new TagList
        {
            { "operation", NormalizeLabel(operation, "unknown") },
            { "apply_mode", FormatApplyMode(decision.ApplyMode) },
            { "candidate_mode", FormatCandidateMode(decision.CandidateMode) },
            { "decision_reason", NormalizeDecisionReason(decision.DecisionReason) },
            { "failure_reason", FormatFailureReason(decision.FailureReason) },
            { "success", decision.Success ? "true" : "false" },
            { "created", decision.Created ? "true" : "false" },
            { "committed", decision.Committed ? "true" : "false" },
            { "immutable_conflict", decision.ImmutableConflict ? "true" : "false" }
        };

        AssignmentDecisions.Add(1, tags);
        AssignmentDurationMs.Record(Math.Max(0d, durationMs), tags);
    }

    public static Activity? StartStartupReconcileActivity(long epochId)
    {
        var activity = ActivitySource.StartActivity("speciation.startup.reconcile", ActivityKind.Internal);
        activity?.SetTag("speciation.epoch_id", Math.Max(0, epochId));
        return activity;
    }

    public static void CompleteStartupReconcileActivity(
        Activity? activity,
        long epochId,
        int knownBrains,
        SpeciationReconcileResult? result,
        string outcome,
        string failureReason)
    {
        if (activity is null)
        {
            return;
        }

        activity.SetTag("speciation.epoch_id", Math.Max(0, epochId));
        activity.SetTag("speciation.known_brains", Math.Max(0, knownBrains));
        activity.SetTag("speciation.outcome", NormalizeLabel(outcome, "unknown"));
        activity.SetTag("speciation.failure_reason", NormalizeLabel(failureReason, "none"));
        activity.SetTag("speciation.added_memberships", Math.Max(0, result?.AddedMemberships ?? 0));
        activity.SetTag("speciation.existing_memberships", Math.Max(0, result?.ExistingMemberships ?? 0));
    }

    public static void RecordStartupReconcile(
        int knownBrains,
        SpeciationReconcileResult? result,
        string outcome,
        string failureReason)
    {
        var tags = new TagList
        {
            { "outcome", NormalizeLabel(outcome, "unknown") },
            { "failure_reason", NormalizeLabel(failureReason, "none") }
        };

        StartupReconcileTotal.Add(1, tags);

        if (result is null)
        {
            return;
        }

        tags.Add("known_brains", Math.Max(0, knownBrains));
        StartupReconcileMembershipsAdded.Add(Math.Max(0, result.AddedMemberships), tags);
        StartupReconcileMembershipsExisting.Add(Math.Max(0, result.ExistingMemberships), tags);
    }

    public static Activity? StartEpochTransitionActivity(
        string transition,
        long previousEpochId)
    {
        var activity = ActivitySource.StartActivity("speciation.epoch.transition", ActivityKind.Internal);
        activity?.SetTag("speciation.transition", NormalizeLabel(transition, "unknown"));
        activity?.SetTag("speciation.previous_epoch_id", Math.Max(0, previousEpochId));
        return activity;
    }

    public static void CompleteEpochTransitionActivity(
        Activity? activity,
        string transition,
        string outcome,
        string failureReason,
        long previousEpochId,
        long currentEpochId,
        int deletedMembershipCount = 0,
        int deletedSpeciesCount = 0,
        int deletedDecisionCount = 0,
        int deletedLineageEdgeCount = 0,
        int deletedEpochCount = 0)
    {
        if (activity is null)
        {
            return;
        }

        activity.SetTag("speciation.transition", NormalizeLabel(transition, "unknown"));
        activity.SetTag("speciation.outcome", NormalizeLabel(outcome, "unknown"));
        activity.SetTag("speciation.failure_reason", NormalizeLabel(failureReason, "none"));
        activity.SetTag("speciation.previous_epoch_id", Math.Max(0, previousEpochId));
        activity.SetTag("speciation.current_epoch_id", Math.Max(0, currentEpochId));
        activity.SetTag("speciation.deleted_epoch_count", Math.Max(0, deletedEpochCount));
        activity.SetTag("speciation.deleted_membership_count", Math.Max(0, deletedMembershipCount));
        activity.SetTag("speciation.deleted_species_count", Math.Max(0, deletedSpeciesCount));
        activity.SetTag("speciation.deleted_decision_count", Math.Max(0, deletedDecisionCount));
        activity.SetTag("speciation.deleted_lineage_edge_count", Math.Max(0, deletedLineageEdgeCount));
    }

    public static void RecordEpochTransition(
        string transition,
        string outcome,
        string failureReason)
    {
        EpochTransitionTotal.Add(
            1,
            new TagList
            {
                { "transition", NormalizeLabel(transition, "unknown") },
                { "outcome", NormalizeLabel(outcome, "unknown") },
                { "failure_reason", NormalizeLabel(failureReason, "none") }
            });
    }

    public static void RecordStatusSnapshot(string source, SpeciationStatusSnapshot status)
    {
        var tags = new TagList
        {
            { "source", NormalizeLabel(source, "status") },
            { "failure_reason", "none" }
        };

        StatusMembershipCount.Record(Math.Max(0, status.MembershipCount), tags);
        StatusSpeciesCount.Record(Math.Max(0, status.SpeciesCount), tags);
        StatusLineageEdgeCount.Record(Math.Max(0, status.LineageEdgeCount), tags);
    }

    private static string NormalizeDecisionReason(string? decisionReason)
        => NormalizeLabel(decisionReason, "none");

    private static string FormatApplyMode(ProtoSpec.SpeciationApplyMode applyMode)
        => applyMode == ProtoSpec.SpeciationApplyMode.Commit
            ? "commit"
            : "dry_run";

    private static string FormatCandidateMode(ProtoSpec.SpeciationCandidateMode candidateMode)
    {
        return candidateMode switch
        {
            ProtoSpec.SpeciationCandidateMode.BrainId => "brain_id",
            ProtoSpec.SpeciationCandidateMode.ArtifactRef => "artifact_ref",
            ProtoSpec.SpeciationCandidateMode.ArtifactUri => "artifact_uri",
            _ => "unknown"
        };
    }

    private static string FormatFailureReason(ProtoSpec.SpeciationFailureReason failureReason)
    {
        return failureReason switch
        {
            ProtoSpec.SpeciationFailureReason.SpeciationFailureNone => "none",
            ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceUnavailable => "service_unavailable",
            ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceInitializing => "service_initializing",
            ProtoSpec.SpeciationFailureReason.SpeciationFailureStoreError => "store_error",
            ProtoSpec.SpeciationFailureReason.SpeciationFailureInvalidRequest => "invalid_request",
            ProtoSpec.SpeciationFailureReason.SpeciationFailureInvalidCandidate => "invalid_candidate",
            ProtoSpec.SpeciationFailureReason.SpeciationFailureMembershipImmutable => "membership_immutable",
            ProtoSpec.SpeciationFailureReason.SpeciationFailureEmptyResponse => "empty_response",
            ProtoSpec.SpeciationFailureReason.SpeciationFailureRequestFailed => "request_failed",
            _ => NormalizeLabel(failureReason.ToString(), "unknown")
        };
    }

    private static string NormalizeLabel(string? value, string fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        return value.Trim();
    }
}
