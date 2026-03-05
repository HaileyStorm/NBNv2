namespace Nbn.Runtime.Speciation;

public sealed record SpeciationRuntimeConfig(
    string PolicyVersion,
    string ConfigSnapshotJson,
    string DefaultSpeciesId,
    string DefaultSpeciesDisplayName,
    string StartupReconcileDecisionReason);

public sealed record SpeciationEpochInfo(
    long EpochId,
    long CreatedMs,
    string PolicyVersion,
    string ConfigSnapshotJson);

public sealed record SpeciationAssignment(
    Guid BrainId,
    string SpeciesId,
    string SpeciesDisplayName,
    string PolicyVersion,
    string DecisionReason,
    string DecisionMetadataJson,
    Guid? SourceBrainId = null,
    string? SourceArtifactRef = null);

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

public sealed record SpeciationLineageEdgeRecord(
    long EpochId,
    Guid ParentBrainId,
    Guid ChildBrainId,
    string MetadataJson,
    long CreatedMs);

public sealed record SpeciationStatusSnapshot(
    long EpochId,
    int MembershipCount,
    int SpeciesCount,
    int LineageEdgeCount);

public sealed record SpeciationAssignOutcome(
    bool Created,
    bool ImmutableConflict,
    SpeciationMembershipRecord Membership);

public sealed record SpeciationReassignOutcome(
    bool Reassigned,
    bool ImmutableConflict,
    SpeciationMembershipRecord? Membership);

public sealed record SpeciationReconcileResult(
    long EpochId,
    int AddedMemberships,
    int ExistingMemberships,
    IReadOnlyList<Guid> AddedBrainIds);

public sealed record SpeciationResetAllResult(
    SpeciationEpochInfo CurrentEpoch,
    int DeletedEpochCount,
    int DeletedMembershipCount,
    int DeletedSpeciesCount,
    int DeletedDecisionCount,
    int DeletedLineageEdgeCount);

public sealed record SpeciationDeleteEpochResult(
    long EpochId,
    bool Deleted,
    int DeletedMembershipCount,
    int DeletedSpeciesCount,
    int DeletedDecisionCount,
    int DeletedLineageEdgeCount);

public sealed record SpeciationStatusRequest;

public sealed record SpeciationStatusResponse(SpeciationStatusSnapshot Status);

public sealed record SpeciationGetCurrentEpochRequest;

public sealed record SpeciationGetCurrentEpochResponse(SpeciationEpochInfo Epoch);

public sealed record SpeciationAssignMembershipRequest(
    SpeciationAssignment Assignment,
    long? DecisionTimeMs = null);

public sealed record SpeciationAssignMembershipResponse(
    bool Success,
    bool Created,
    bool ImmutableConflict,
    string FailureReason,
    SpeciationMembershipRecord? Membership);

public sealed record SpeciationResetEpochRequest(
    string? PolicyVersion = null,
    string? ConfigSnapshotJson = null,
    long? ResetTimeMs = null);

public sealed record SpeciationResetEpochResponse(
    SpeciationEpochInfo PreviousEpoch,
    SpeciationEpochInfo CurrentEpoch);

public sealed record SpeciationReconcileKnownBrainsRequest(
    IReadOnlyList<Guid> BrainIds,
    string? SpeciesId = null,
    string? SpeciesDisplayName = null,
    string? DecisionReason = null,
    string? DecisionMetadataJson = null,
    string? PolicyVersion = null,
    long? DecisionTimeMs = null);

public sealed record SpeciationReconcileKnownBrainsResponse(SpeciationReconcileResult Result);

public sealed record SpeciationListMembershipsRequest(long? EpochId = null);

public sealed record SpeciationListMembershipsResponse(IReadOnlyList<SpeciationMembershipRecord> Memberships);

public sealed record SpeciationRecordLineageEdgeRequest(
    Guid ParentBrainId,
    Guid ChildBrainId,
    string MetadataJson,
    long? CreatedMs = null);

public sealed record SpeciationRecordLineageEdgeResponse(
    bool Success,
    string FailureReason);
