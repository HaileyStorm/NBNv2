using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Shared;
using Proto;
using ProtoIo = Nbn.Proto.Io;
using ProtoSpec = Nbn.Proto.Speciation;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace Nbn.Runtime.Speciation;

public sealed partial class SpeciationManagerActor
{
    private async Task<ProtoSpec.SpeciationBatchEvaluateApplyResponse> ProcessProtoBatchAsync(
        IContext context,
        SpeciationEpochInfo epoch,
        ProtoSpec.SpeciationApplyMode requestApplyMode,
        IEnumerable<ProtoSpec.SpeciationBatchItem> items)
    {
        var ordered = items?.ToArray() ?? Array.Empty<ProtoSpec.SpeciationBatchItem>();
        var response = new ProtoSpec.SpeciationBatchEvaluateApplyResponse
        {
            FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
            FailureDetail = string.Empty,
            ApplyMode = requestApplyMode,
            RequestedCount = (uint)ordered.Length
        };

        uint committed = 0;
        for (var index = 0; index < ordered.Length; index++)
        {
            var item = ordered[index] ?? new ProtoSpec.SpeciationBatchItem();
            var itemMode = requestApplyMode;
            if (item.HasApplyModeOverride)
            {
                itemMode = NormalizeApplyMode(item.ApplyModeOverride);
            }

            var stopwatch = Stopwatch.StartNew();
            var decision = await ProcessProtoDecisionAsync(
                context,
                epoch,
                itemMode,
                item.Candidate,
                item.Parents,
                item.SpeciesId,
                item.SpeciesDisplayName,
                item.PolicyVersion,
                item.DecisionReason,
                item.DecisionMetadataJson,
                item.HasDecisionTimeMs ? (long?)item.DecisionTimeMs : null,
                commit: itemMode == ProtoSpec.SpeciationApplyMode.Commit).ConfigureAwait(false);
            SpeciationTelemetry.RecordAssignmentDecision(
                "batch",
                decision,
                stopwatch.Elapsed.TotalMilliseconds);

            if (decision.Committed)
            {
                committed++;
            }

            response.Results.Add(new ProtoSpec.SpeciationBatchItemResult
            {
                ItemId = string.IsNullOrWhiteSpace(item.ItemId) ? $"item-{index}" : item.ItemId.Trim(),
                Decision = decision
            });
        }

        response.ProcessedCount = (uint)response.Results.Count;
        response.CommittedCount = committed;
        return response;
    }

    private async Task<ProtoSpec.SpeciationDecision> ProcessProtoDecisionAsync(
        IContext context,
        SpeciationEpochInfo epoch,
        ProtoSpec.SpeciationApplyMode applyMode,
        ProtoSpec.SpeciationCandidateRef? candidate,
        IEnumerable<ProtoSpec.SpeciationParentRef> parents,
        string? speciesId,
        string? speciesDisplayName,
        string? policyVersion,
        string? decisionReason,
        string? decisionMetadataJson,
        long? decisionTimeMs,
        bool commit)
    {
        // ReenterAfter allows assign requests to overlap, but this actor keeps
        // mutable in-memory lineage and root-naming state that must be observed
        // in commit order.
        await _decisionProcessingGate.WaitAsync().ConfigureAwait(false);
        try
        {
            return await ProcessProtoDecisionCoreAsync(
                context,
                epoch,
                applyMode,
                candidate,
                parents,
                speciesId,
                speciesDisplayName,
                policyVersion,
                decisionReason,
                decisionMetadataJson,
                decisionTimeMs,
                commit).ConfigureAwait(false);
        }
        finally
        {
            _decisionProcessingGate.Release();
        }
    }

    private async Task<ProtoSpec.SpeciationDecision> ProcessProtoDecisionCoreAsync(
        IContext context,
        SpeciationEpochInfo epoch,
        ProtoSpec.SpeciationApplyMode applyMode,
        ProtoSpec.SpeciationCandidateRef? candidate,
        IEnumerable<ProtoSpec.SpeciationParentRef> parents,
        string? speciesId,
        string? speciesDisplayName,
        string? policyVersion,
        string? decisionReason,
        string? decisionMetadataJson,
        long? decisionTimeMs,
        bool commit)
    {
        if (!TryResolveCandidate(candidate, out var resolved))
        {
            return CreateDecisionFailure(
                applyMode,
                ProtoSpec.SpeciationFailureReason.SpeciationFailureInvalidCandidate,
                "Speciation candidate must be brain_id, artifact_ref, or artifact_uri.");
        }

        resolved = await TryEnrichResolvedCandidateAsync(context, resolved, decisionMetadataJson).ConfigureAwait(false);

        var inputOrderedParentBrainIds = ExtractParentBrainIdsByInputOrder(parents);
        var orderedParentBrainIds = ExtractParentBrainIds(parents);
        var orderedParentArtifactRefs = ExtractParentArtifactLabels(parents);
        var parentMemberships = await LoadParentMembershipsAsync(epoch.EpochId, orderedParentBrainIds).ConfigureAwait(false);
        var hysteresisMembership = await ResolveHysteresisMembershipAsync(
            epoch.EpochId,
            orderedParentBrainIds).ConfigureAwait(false);
        var similarityEvidence = ExtractSimilarityEvidence(decisionMetadataJson);
        var lineageEvidence = BuildLineageEvidence(
            orderedParentBrainIds,
            orderedParentArtifactRefs,
            parentMemberships,
            hysteresisMembership);
        var bestParentSpeciesFit = TryResolveBestParentSpeciesPairwiseFit(
            parentMemberships,
            similarityEvidence,
            inputOrderedParentBrainIds,
            out var resolvedBestParentSpeciesFit)
            ? (ParentSpeciesPairwiseFit?)resolvedBestParentSpeciesFit
            : null;
        var isSeedFounderCandidate = IsSeedFounderCandidate(
            resolved.BrainId,
            inputOrderedParentBrainIds);
        var preliminarySourceSpeciesId = bestParentSpeciesFit?.SpeciesId
            ?? (lineageEvidence.DominantShare >= 0.999999d
                ? lineageEvidence.DominantSpeciesId
                : null);
        var sourceSpeciesSimilarityScore = bestParentSpeciesFit.HasValue
            ? bestParentSpeciesFit.Value.PairwiseSimilarity
            : ResolveSourceSpeciesSimilarityScore(
                preliminarySourceSpeciesId,
                parentMemberships,
                similarityEvidence,
                inputOrderedParentBrainIds);
        var sourceSpeciesFloor = ResolveSpeciesSimilarityFloor(
            preliminarySourceSpeciesId);
        var assignmentResolution = ResolveAssignment(
            epoch,
            speciesId,
            speciesDisplayName,
            lineageEvidence,
            sourceSpeciesFloor,
            sourceSpeciesSimilarityScore,
            bestParentSpeciesFit,
            isSeedFounderCandidate: isSeedFounderCandidate);
        var bootstrapRequirement = await TryResolveBootstrapAssignedSpeciesAdmissionRequirementAsync(
            epoch.EpochId,
            assignmentResolution).ConfigureAwait(false);
        AssignedSpeciesAdmissionAssessment? assignedSpeciesAdmissionAssessment = null;
        if (RequiresActualAssignedSpeciesAdmission(
                assignmentResolution,
                bootstrapRequirement.HasValue))
        {
            var admissionSourceSimilarityScore =
                bootstrapRequirement.HasValue
                && string.Equals(
                    assignmentResolution.SpeciesId,
                    assignmentResolution.SourceSpeciesId,
                    StringComparison.Ordinal)
                    ? null
                    : sourceSpeciesSimilarityScore;
            var evaluatedAdmission = await TryAssessActualAssignedSpeciesAdmissionAsync(
                context,
                epoch,
                resolved,
                assignmentResolution,
                bootstrapRequirement,
                admissionSourceSimilarityScore).ConfigureAwait(false);
            assignedSpeciesAdmissionAssessment = evaluatedAdmission;
            if (!evaluatedAdmission.Admitted)
            {
                if (bootstrapRequirement.HasValue)
                {
                    sourceSpeciesSimilarityScore = admissionSourceSimilarityScore;
                    sourceSpeciesFloor = ResolveSpeciesSimilarityFloor(
                        bootstrapRequirement.Value.SourceSpeciesId);
                    assignmentResolution = BuildBootstrapFallbackAssignmentResolution(
                        bootstrapRequirement.Value,
                        admissionSourceSimilarityScore);
                    bootstrapRequirement = await TryResolveBootstrapAssignedSpeciesAdmissionRequirementAsync(
                        epoch.EpochId,
                        assignmentResolution).ConfigureAwait(false);
                }
                else
                {
                    assignmentResolution = ResolveAssignment(
                        epoch,
                        speciesId,
                        speciesDisplayName,
                        lineageEvidence,
                        sourceSpeciesFloor,
                        sourceSpeciesSimilarityScore,
                        bestParentSpeciesFit,
                        isSeedFounderCandidate,
                        allowRecentSplitRealign: false,
                        allowRecentDerivedSpeciesReuse: false);
                }
            }
        }
        var allowSourceSimilarityCarryover = !bootstrapRequirement.HasValue;
        var intraSpeciesSimilaritySample = TryResolveIntraSpeciesSimilaritySample(
            assignmentResolution,
            parentMemberships,
            similarityEvidence,
            sourceSpeciesSimilarityScore,
            assignedSpeciesAdmissionAssessment.HasValue
            && assignedSpeciesAdmissionAssessment.Value.Admitted
                ? assignedSpeciesAdmissionAssessment.Value.SimilarityScore
                : null,
            inputOrderedParentBrainIds,
            allowSourceSimilarityCarryover,
            out var resolvedIntraSpeciesSample)
            ? (double?)resolvedIntraSpeciesSample
            : null;
        var assignedSpeciesSimilarityScore = ResolveAssignedSpeciesSimilarityScore(
            assignmentResolution,
            parentMemberships,
            similarityEvidence,
            inputOrderedParentBrainIds,
            sourceSpeciesSimilarityScore,
            assignedSpeciesAdmissionAssessment.HasValue
            && assignedSpeciesAdmissionAssessment.Value.Admitted
                ? assignedSpeciesAdmissionAssessment.Value.SimilarityScore
                : null,
            intraSpeciesSimilaritySample,
            allowSourceSimilarityCarryover);

        var resolvedPolicyVersion = NormalizeOrFallback(policyVersion, _runtimeConfig.PolicyVersion);
        var resolvedDecisionReason = assignmentResolution.ForceDecisionReason
            ? assignmentResolution.DecisionReason
            : NormalizeOrFallback(decisionReason, assignmentResolution.DecisionReason);
        var resolvedDecisionMetadata = BuildDecisionMetadataJson(
            decisionMetadataJson,
            resolvedPolicyVersion,
            resolved,
            assignmentResolution,
            lineageEvidence,
            similarityEvidence,
            sourceSpeciesFloor,
            sourceSpeciesSimilarityScore,
            assignedSpeciesSimilarityScore,
            intraSpeciesSimilaritySample,
            assignedSpeciesAdmissionAssessment);
        Guid? parentBrainId = orderedParentBrainIds.Count == 0
            ? null
            : orderedParentBrainIds[0];
        var parentArtifactRef = orderedParentArtifactRefs.Count == 0
            ? null
            : orderedParentArtifactRefs[0];

        var existingMembership = await _store.GetMembershipAsync(epoch.EpochId, resolved.BrainId).ConfigureAwait(false);
        if (!commit)
        {
            if (existingMembership is not null)
            {
                return CreateDecisionFromMembership(
                    applyMode,
                    resolved.CandidateMode,
                    existingMembership,
                    created: false,
                    immutableConflict: false,
                    committed: false,
                    failureReason: ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
                    failureDetail: string.Empty);
            }

            return new ProtoSpec.SpeciationDecision
            {
                ApplyMode = applyMode,
                CandidateMode = resolved.CandidateMode,
                Success = true,
                Created = false,
                ImmutableConflict = false,
                FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
                FailureDetail = string.Empty,
                SpeciesId = assignmentResolution.SpeciesId,
                SpeciesDisplayName = assignmentResolution.SpeciesDisplayName,
                DecisionReason = resolvedDecisionReason,
                DecisionMetadataJson = resolvedDecisionMetadata,
                Committed = false
            };
        }

        var assignment = new SpeciationAssignment(
            resolved.BrainId,
            assignmentResolution.SpeciesId,
            assignmentResolution.SpeciesDisplayName,
            resolvedPolicyVersion,
            resolvedDecisionReason,
            resolvedDecisionMetadata,
            parentBrainId,
            resolved.SourceArtifactRef ?? parentArtifactRef);
        var speciesDisplayNameUpdates =
            !string.IsNullOrWhiteSpace(assignmentResolution.DisplayNameRewriteSpeciesId)
            && !string.IsNullOrWhiteSpace(assignmentResolution.DisplayNameRewriteSpeciesDisplayName)
                ? new[]
                {
                    new SpeciationSpeciesDisplayNameUpdate(
                        assignmentResolution.DisplayNameRewriteSpeciesId!,
                        assignmentResolution.DisplayNameRewriteSpeciesDisplayName!)
                }
                : null;
        var outcome = await _store.TryAssignMembershipAsync(
            epoch.EpochId,
            assignment,
            decisionTimeMs,
            cancellationToken: StoreMutationCancellationToken,
            lineageParentBrainIds: orderedParentBrainIds,
            lineageMetadataJson: resolvedDecisionMetadata,
            speciesDisplayNameUpdates: speciesDisplayNameUpdates).ConfigureAwait(false);

        if (outcome.Created)
        {
            if (speciesDisplayNameUpdates is not null)
            {
                foreach (var speciesDisplayNameUpdate in speciesDisplayNameUpdates)
                {
                    RecordSpeciesDisplayName(
                        speciesDisplayNameUpdate.SpeciesId,
                        speciesDisplayNameUpdate.SpeciesDisplayName);
                }
            }

            RecordCommittedMembership(
                outcome.Membership,
                intraSpeciesSimilaritySample,
                assignedSpeciesAdmissionAssessment.HasValue
                && assignedSpeciesAdmissionAssessment.Value.Admitted);

            if (string.Equals(
                    assignmentResolution.DecisionReason,
                    "lineage_diverged_new_species",
                    StringComparison.Ordinal)
                && !string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
                && sourceSpeciesSimilarityScore.HasValue)
            {
                context.Send(
                    context.Self,
                    new ApplySplitHindsightReassignmentsRequest(
                        epoch,
                        outcome.Membership,
                        assignmentResolution,
                        sourceSpeciesSimilarityScore.Value,
                        resolvedPolicyVersion,
                        decisionTimeMs));
            }
        }

        var success = !outcome.ImmutableConflict;
        var reason = outcome.ImmutableConflict
            ? ProtoSpec.SpeciationFailureReason.SpeciationFailureMembershipImmutable
            : ProtoSpec.SpeciationFailureReason.SpeciationFailureNone;
        var detail = outcome.ImmutableConflict
            ? "Membership is immutable within the current epoch."
            : string.Empty;
        return CreateDecisionFromMembership(
            applyMode,
            resolved.CandidateMode,
            outcome.Membership,
            created: outcome.Created,
            immutableConflict: outcome.ImmutableConflict,
            committed: outcome.Created,
            failureReason: reason,
            failureDetail: detail,
            successOverride: success);
    }

    private static bool TryResolveCandidate(
        ProtoSpec.SpeciationCandidateRef? candidate,
        out ResolvedCandidate resolvedCandidate)
    {
        resolvedCandidate = default;
        if (candidate is null)
        {
            return false;
        }

        switch (candidate.CandidateCase)
        {
            case ProtoSpec.SpeciationCandidateRef.CandidateOneofCase.BrainId:
                if (candidate.BrainId is not null && candidate.BrainId.TryToGuid(out var brainId) && brainId != Guid.Empty)
                {
                    resolvedCandidate = new ResolvedCandidate(
                        ProtoSpec.SpeciationCandidateMode.BrainId,
                        brainId,
                        SourceArtifactRef: null,
                        CandidateArtifactRef: null,
                        CandidateArtifactUri: null);
                    return true;
                }
                return false;
            case ProtoSpec.SpeciationCandidateRef.CandidateOneofCase.ArtifactRef:
                if (HasUsableArtifactReference(candidate.ArtifactRef))
                {
                    var sourceArtifactRef = BuildArtifactLabel(candidate.ArtifactRef!);
                    var derivedBrainId = CreateDeterministicCandidateBrainId(
                        BuildArtifactIdentityKey(candidate.ArtifactRef!));
                    resolvedCandidate = new ResolvedCandidate(
                        ProtoSpec.SpeciationCandidateMode.ArtifactRef,
                        derivedBrainId,
                        sourceArtifactRef,
                        candidate.ArtifactRef!.Clone(),
                        CandidateArtifactUri: null);
                    return true;
                }
                return false;
            case ProtoSpec.SpeciationCandidateRef.CandidateOneofCase.ArtifactUri:
                if (!string.IsNullOrWhiteSpace(candidate.ArtifactUri))
                {
                    var normalizedUri = candidate.ArtifactUri.Trim();
                    var derivedBrainId = CreateDeterministicCandidateBrainId($"artifact_uri|{normalizedUri}");
                    resolvedCandidate = new ResolvedCandidate(
                        ProtoSpec.SpeciationCandidateMode.ArtifactUri,
                        derivedBrainId,
                        normalizedUri,
                        CandidateArtifactRef: null,
                        CandidateArtifactUri: normalizedUri);
                    return true;
                }
                return false;
            default:
                return false;
        }
    }

    private static string? BuildArtifactLabel(ArtifactRef artifactRef)
    {
        if (artifactRef.TryToSha256Hex(out var sha))
        {
            return $"sha256:{sha}";
        }

        return string.IsNullOrWhiteSpace(artifactRef.StoreUri)
            ? null
            : artifactRef.StoreUri.Trim();
    }

    private static string BuildArtifactIdentityKey(ArtifactRef artifactRef)
    {
        if (artifactRef.TryToSha256Hex(out var sha256Hex))
        {
            return $"artifact_ref|sha256={sha256Hex}";
        }

        var storeUri = string.IsNullOrWhiteSpace(artifactRef.StoreUri)
            ? string.Empty
            : artifactRef.StoreUri.Trim();
        var mediaType = string.IsNullOrWhiteSpace(artifactRef.MediaType)
            ? string.Empty
            : artifactRef.MediaType.Trim();
        return $"artifact_ref|size={artifactRef.SizeBytes}|media_type={mediaType}|store_uri={storeUri}";
    }

    private async Task<ResolvedCandidate> TryEnrichResolvedCandidateAsync(
        IContext context,
        ResolvedCandidate resolvedCandidate,
        string? decisionMetadataJson)
    {
        if (resolvedCandidate.CandidateMode != ProtoSpec.SpeciationCandidateMode.BrainId
            || resolvedCandidate.BrainId == Guid.Empty)
        {
            return resolvedCandidate;
        }

        var provenance = await TryResolveBrainArtifactProvenanceAsync(
            context,
            resolvedCandidate.BrainId).ConfigureAwait(false);
        if (!HasUsableArtifactReference(provenance.BaseArtifactRef))
        {
            if (!TryExtractStoredCandidateBrainArtifactRefs(
                    decisionMetadataJson,
                    out var storedBaseArtifactRef,
                    out var storedSnapshotArtifactRef))
            {
                return resolvedCandidate;
            }

            return resolvedCandidate with
            {
                SourceArtifactRef = resolvedCandidate.SourceArtifactRef ?? BuildArtifactLabel(storedBaseArtifactRef),
                CandidateBrainBaseArtifactRef = storedBaseArtifactRef.Clone(),
                CandidateBrainSnapshotArtifactRef = HasUsableArtifactReference(storedSnapshotArtifactRef)
                    ? storedSnapshotArtifactRef!.Clone()
                    : null
            };
        }

        return resolvedCandidate with
        {
            SourceArtifactRef = resolvedCandidate.SourceArtifactRef ?? BuildArtifactLabel(provenance.BaseArtifactRef!),
            CandidateBrainBaseArtifactRef = provenance.BaseArtifactRef!.Clone(),
            CandidateBrainSnapshotArtifactRef = HasUsableArtifactReference(provenance.SnapshotArtifactRef)
                ? provenance.SnapshotArtifactRef!.Clone()
                : null
        };
    }

    private async Task<BrainArtifactProvenance> TryResolveBrainArtifactProvenanceAsync(
        IContext context,
        Guid brainId)
    {
        if (_ioGatewayPid is null || brainId == Guid.Empty)
        {
            return default;
        }

        try
        {
            var info = await context.RequestAsync<ProtoIo.BrainInfo>(
                _ioGatewayPid,
                new ProtoIo.BrainInfoRequest
                {
                    BrainId = brainId.ToProtoUuid()
                },
                _compatibilityRequestTimeout).ConfigureAwait(false);
            if (info is null || !HasUsableArtifactReference(info.BaseDefinition))
            {
                return default;
            }

            return new BrainArtifactProvenance(
                info.BaseDefinition.Clone(),
                HasUsableArtifactReference(info.LastSnapshot)
                    ? info.LastSnapshot.Clone()
                    : null);
        }
        catch
        {
            return default;
        }
    }

    private static Guid CreateDeterministicCandidateBrainId(string identityKey)
    {
        if (string.IsNullOrWhiteSpace(identityKey))
        {
            return Guid.Empty;
        }

        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(identityKey.Trim()));
        Span<byte> guidBytes = stackalloc byte[16];
        bytes.AsSpan(0, 16).CopyTo(guidBytes);

        // Mark as RFC 4122 variant and version-5 style UUID for deterministic artifact identities.
        guidBytes[6] = (byte)((guidBytes[6] & 0x0F) | 0x50);
        guidBytes[8] = (byte)((guidBytes[8] & 0x3F) | 0x80);
        return new Guid(guidBytes);
    }

    private static bool HasUsableArtifactReference(ArtifactRef? artifactRef)
    {
        if (artifactRef is null)
        {
            return false;
        }

        var hasSha = artifactRef.Sha256 is not null
                     && artifactRef.Sha256.Value is not null
                     && artifactRef.Sha256.Value.Length == 32;
        var hasStoreUri = !string.IsNullOrWhiteSpace(artifactRef.StoreUri);
        return hasSha || hasStoreUri;
    }

    private static IReadOnlyList<Guid> ExtractParentBrainIds(IEnumerable<ProtoSpec.SpeciationParentRef> parents)
    {
        return ExtractParentBrainIdsByInputOrder(parents)
            .OrderBy(parentIdentity => parentIdentity)
            .ToArray();
    }

    private static IReadOnlyList<Guid> ExtractParentBrainIdsByInputOrder(IEnumerable<ProtoSpec.SpeciationParentRef> parents)
    {
        if (parents is null)
        {
            return Array.Empty<Guid>();
        }

        var identities = new List<Guid>();
        var seen = new HashSet<Guid>();
        foreach (var parent in parents)
        {
            if (TryResolveParentIdentity(parent, out var parentIdentity)
                && parentIdentity != Guid.Empty
                && seen.Add(parentIdentity))
            {
                identities.Add(parentIdentity);
            }
        }

        return identities;
    }

    private static bool TryResolveParentIdentity(ProtoSpec.SpeciationParentRef? parent, out Guid parentIdentity)
    {
        parentIdentity = Guid.Empty;
        if (parent is null)
        {
            return false;
        }

        switch (parent.ParentCase)
        {
            case ProtoSpec.SpeciationParentRef.ParentOneofCase.BrainId:
                if (parent.BrainId is not null
                    && parent.BrainId.TryToGuid(out var parentBrainId)
                    && parentBrainId != Guid.Empty)
                {
                    parentIdentity = parentBrainId;
                    return true;
                }

                return false;
            case ProtoSpec.SpeciationParentRef.ParentOneofCase.ArtifactRef:
                if (HasUsableArtifactReference(parent.ArtifactRef))
                {
                    parentIdentity = CreateDeterministicCandidateBrainId(
                        BuildArtifactIdentityKey(parent.ArtifactRef!));
                    return parentIdentity != Guid.Empty;
                }

                return false;
            case ProtoSpec.SpeciationParentRef.ParentOneofCase.ArtifactUri:
                if (!string.IsNullOrWhiteSpace(parent.ArtifactUri))
                {
                    parentIdentity = CreateDeterministicCandidateBrainId(
                        $"artifact_uri|{parent.ArtifactUri.Trim()}");
                    return parentIdentity != Guid.Empty;
                }

                return false;
            default:
                return false;
        }
    }

    private static IReadOnlyList<string> ExtractParentArtifactLabels(IEnumerable<ProtoSpec.SpeciationParentRef> parents)
    {
        if (parents is null)
        {
            return Array.Empty<string>();
        }

        var labels = new List<string>();
        foreach (var parent in parents)
        {
            if (parent is null)
            {
                continue;
            }

            if (parent.ParentCase == ProtoSpec.SpeciationParentRef.ParentOneofCase.ArtifactUri
                && !string.IsNullOrWhiteSpace(parent.ArtifactUri))
            {
                labels.Add(parent.ArtifactUri.Trim());
                continue;
            }

            if (parent.ParentCase != ProtoSpec.SpeciationParentRef.ParentOneofCase.ArtifactRef
                || parent.ArtifactRef is null)
            {
                continue;
            }

            if (parent.ArtifactRef.TryToSha256Hex(out var sha))
            {
                labels.Add($"sha256:{sha}");
                continue;
            }

            if (!string.IsNullOrWhiteSpace(parent.ArtifactRef.StoreUri))
            {
                labels.Add(parent.ArtifactRef.StoreUri.Trim());
            }
        }

        return labels
            .Where(label => !string.IsNullOrWhiteSpace(label))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(label => label, StringComparer.Ordinal)
            .ToArray();
    }

    private static Guid? ExtractFirstParentBrainId(IEnumerable<ProtoSpec.SpeciationParentRef> parents)
    {
        var parentBrainIds = ExtractParentBrainIds(parents);
        return parentBrainIds.Count == 0 ? null : parentBrainIds[0];
    }

    private static string? ExtractFirstParentArtifactLabel(IEnumerable<ProtoSpec.SpeciationParentRef> parents)
    {
        var parentArtifactLabels = ExtractParentArtifactLabels(parents);
        return parentArtifactLabels.Count == 0 ? null : parentArtifactLabels[0];
    }

    private async Task<IReadOnlyList<SpeciationMembershipRecord>> LoadParentMembershipsAsync(
        long epochId,
        IReadOnlyList<Guid> orderedParentBrainIds)
    {
        if (orderedParentBrainIds.Count == 0)
        {
            return Array.Empty<SpeciationMembershipRecord>();
        }

        var memberships = new List<SpeciationMembershipRecord>(orderedParentBrainIds.Count);
        foreach (var parentBrainId in orderedParentBrainIds)
        {
            var membership = await _store.GetMembershipAsync(epochId, parentBrainId).ConfigureAwait(false);
            if (membership is not null)
            {
                memberships.Add(membership);
            }
        }

        return memberships;
    }

    private async Task<SpeciationMembershipRecord?> ResolveHysteresisMembershipAsync(
        long epochId,
        IReadOnlyList<Guid> orderedParentBrainIds)
    {
        if (orderedParentBrainIds.Count == 0)
        {
            return null;
        }

        var hints = new List<SpeciationMembershipRecord>(orderedParentBrainIds.Count);
        foreach (var parentBrainId in orderedParentBrainIds)
        {
            var hint = await _store.GetLatestChildMembershipForParentAsync(
                epochId,
                parentBrainId).ConfigureAwait(false);
            if (hint is not null)
            {
                hints.Add(hint);
            }
        }

        if (hints.Count == 0)
        {
            return null;
        }

        return hints
            .GroupBy(item => item.SpeciesId, StringComparer.Ordinal)
            .Select(group =>
            {
                var latest = group
                    .OrderByDescending(item => item.AssignedMs)
                    .ThenBy(item => item.BrainId)
                    .First();
                return new
                {
                    SpeciesId = group.Key,
                    Count = group.Count(),
                    LatestAssignedMs = latest.AssignedMs,
                    Membership = latest
                };
            })
            .OrderByDescending(item => item.Count)
            .ThenByDescending(item => item.LatestAssignedMs)
            .ThenBy(item => item.SpeciesId, StringComparer.Ordinal)
            .Select(item => item.Membership)
            .FirstOrDefault();
    }

    private static LineageEvidence BuildLineageEvidence(
        IReadOnlyList<Guid> orderedParentBrainIds,
        IReadOnlyList<string> orderedParentArtifactRefs,
        IReadOnlyList<SpeciationMembershipRecord> parentMemberships,
        SpeciationMembershipRecord? hysteresisMembership)
    {
        var dominant = parentMemberships
            .GroupBy(item => item.SpeciesId, StringComparer.Ordinal)
            .Select(group =>
            {
                var preferred = group
                    .OrderByDescending(item => item.AssignedMs)
                    .ThenBy(item => item.BrainId)
                    .First();
                return new
                {
                    SpeciesId = group.Key,
                    SpeciesDisplayName = preferred.SpeciesDisplayName,
                    Count = group.Count(),
                    LatestAssignedMs = preferred.AssignedMs
                };
            })
            .OrderByDescending(item => item.Count)
            .ThenByDescending(item => item.LatestAssignedMs)
            .ThenBy(item => item.SpeciesId, StringComparer.Ordinal)
            .FirstOrDefault();

        var totalMemberships = parentMemberships.Count;
        var dominantShare = totalMemberships <= 0 || dominant is null
            ? 0d
            : (double)dominant.Count / totalMemberships;

        return new LineageEvidence(
            orderedParentBrainIds,
            orderedParentArtifactRefs,
            totalMemberships,
            dominant?.SpeciesId,
            dominant?.SpeciesDisplayName,
            dominantShare,
            BuildLineageKey(orderedParentBrainIds),
            hysteresisMembership?.SpeciesId,
            hysteresisMembership?.SpeciesDisplayName,
            hysteresisMembership?.DecisionReason);
    }
}
