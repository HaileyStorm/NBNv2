namespace Nbn.Runtime.Speciation;

/// <summary>
/// Captures the persisted policy/config snapshot that drives speciation decisions for an epoch.
/// </summary>
public sealed record SpeciationRuntimeConfig(
    string PolicyVersion,
    string ConfigSnapshotJson,
    string DefaultSpeciesId,
    string DefaultSpeciesDisplayName,
    string StartupReconcileDecisionReason);

/// <summary>
/// Describes a persisted taxonomy epoch and its configuration snapshot.
/// </summary>
public sealed record SpeciationEpochInfo(
    long EpochId,
    long CreatedMs,
    string PolicyVersion,
    string ConfigSnapshotJson);

/// <summary>
/// Represents a single membership decision to persist for a brain.
/// </summary>
public sealed record SpeciationAssignment(
    Guid BrainId,
    string SpeciesId,
    string SpeciesDisplayName,
    string PolicyVersion,
    string DecisionReason,
    string DecisionMetadataJson,
    Guid? SourceBrainId = null,
    string? SourceArtifactRef = null);

/// <summary>
/// Renames an existing species display label while preserving its identity.
/// </summary>
public sealed record SpeciationSpeciesDisplayNameUpdate(
    string SpeciesId,
    string SpeciesDisplayName);

/// <summary>
/// Represents a persisted membership row joined with decision metadata.
/// </summary>
public sealed record SpeciationMembershipRecord(
    long EpochId,
    Guid BrainId,
    string SpeciesId,
    string SpeciesDisplayName,
    long AssignedMs,
    string PolicyVersion,
    string DecisionReason,
    string DecisionMetadataJson,
    Guid? SourceBrainId,
    string? SourceArtifactRef,
    long DecisionId);

/// <summary>
/// Represents a persisted lineage edge between parent and child brains.
/// </summary>
public sealed record SpeciationLineageEdgeRecord(
    long EpochId,
    Guid ParentBrainId,
    Guid ChildBrainId,
    string MetadataJson,
    long CreatedMs);

/// <summary>
/// Summarizes the current persisted counts for an epoch.
/// </summary>
public sealed record SpeciationStatusSnapshot(
    long EpochId,
    int MembershipCount,
    int SpeciesCount,
    int LineageEdgeCount);

/// <summary>
/// Reports the result of attempting to create an immutable membership.
/// </summary>
public sealed record SpeciationAssignOutcome(
    bool Created,
    bool ImmutableConflict,
    SpeciationMembershipRecord Membership);

/// <summary>
/// Reports the result of attempting to rewrite an existing membership.
/// </summary>
public sealed record SpeciationReassignOutcome(
    bool Reassigned,
    bool ImmutableConflict,
    SpeciationMembershipRecord? Membership);

/// <summary>
/// Reports how many memberships startup or manual reconciliation created.
/// </summary>
public sealed record SpeciationReconcileResult(
    long EpochId,
    int AddedMemberships,
    int ExistingMemberships,
    IReadOnlyList<Guid> AddedBrainIds);

/// <summary>
/// Reports the totals removed by a full history reset and the newly seeded epoch.
/// </summary>
public sealed record SpeciationResetAllResult(
    SpeciationEpochInfo CurrentEpoch,
    int DeletedEpochCount,
    int DeletedMembershipCount,
    int DeletedSpeciesCount,
    int DeletedDecisionCount,
    int DeletedLineageEdgeCount);

/// <summary>
/// Reports the totals removed when deleting a single historical epoch.
/// </summary>
public sealed record SpeciationDeleteEpochResult(
    long EpochId,
    bool Deleted,
    int DeletedMembershipCount,
    int DeletedSpeciesCount,
    int DeletedDecisionCount,
    int DeletedLineageEdgeCount);

/// <summary>
/// Requests the current aggregate status snapshot.
/// </summary>
public sealed record SpeciationStatusRequest;

/// <summary>
/// Returns the current aggregate status snapshot.
/// </summary>
public sealed record SpeciationStatusResponse(SpeciationStatusSnapshot Status);

/// <summary>
/// Requests the current epoch descriptor.
/// </summary>
public sealed record SpeciationGetCurrentEpochRequest;

/// <summary>
/// Returns the current epoch descriptor.
/// </summary>
public sealed record SpeciationGetCurrentEpochResponse(SpeciationEpochInfo Epoch);

/// <summary>
/// Requests an explicit membership assignment write for a brain.
/// </summary>
public sealed record SpeciationAssignMembershipRequest(
    SpeciationAssignment Assignment,
    long? DecisionTimeMs = null);

/// <summary>
/// Returns the outcome of an explicit membership assignment request.
/// </summary>
public sealed record SpeciationAssignMembershipResponse(
    bool Success,
    bool Created,
    bool ImmutableConflict,
    string FailureReason,
    SpeciationMembershipRecord? Membership);

/// <summary>
/// Requests creation of a new epoch with an optional replacement runtime config snapshot.
/// </summary>
public sealed record SpeciationResetEpochRequest(
    string? PolicyVersion = null,
    string? ConfigSnapshotJson = null,
    long? ResetTimeMs = null);

/// <summary>
/// Returns the previous and current epochs after a reset request.
/// </summary>
public sealed record SpeciationResetEpochResponse(
    SpeciationEpochInfo PreviousEpoch,
    SpeciationEpochInfo CurrentEpoch);

/// <summary>
/// Requests reconciliation for a set of known brains that may be missing memberships.
/// </summary>
public sealed record SpeciationReconcileKnownBrainsRequest(
    IReadOnlyList<Guid> BrainIds,
    string? SpeciesId = null,
    string? SpeciesDisplayName = null,
    string? DecisionReason = null,
    string? DecisionMetadataJson = null,
    string? PolicyVersion = null,
    long? DecisionTimeMs = null);

/// <summary>
/// Returns the result of a reconcile-known-brains request.
/// </summary>
public sealed record SpeciationReconcileKnownBrainsResponse(SpeciationReconcileResult Result);

/// <summary>
/// Requests memberships for the current epoch or a specified historical epoch.
/// </summary>
public sealed record SpeciationListMembershipsRequest(long? EpochId = null);

/// <summary>
/// Returns membership records for the requested epoch scope.
/// </summary>
public sealed record SpeciationListMembershipsResponse(IReadOnlyList<SpeciationMembershipRecord> Memberships);

/// <summary>
/// Requests an explicit lineage edge write without changing membership state.
/// </summary>
public sealed record SpeciationRecordLineageEdgeRequest(
    Guid ParentBrainId,
    Guid ChildBrainId,
    string MetadataJson,
    long? CreatedMs = null);

/// <summary>
/// Returns the outcome of an explicit lineage edge write.
/// </summary>
public sealed record SpeciationRecordLineageEdgeResponse(
    bool Success,
    string FailureReason);
