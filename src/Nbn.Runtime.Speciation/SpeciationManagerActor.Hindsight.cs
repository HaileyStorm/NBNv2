using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Shared;
using Proto;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Nbn.Runtime.Speciation;

public sealed partial class SpeciationManagerActor
{
    private async Task<int> ApplySplitHindsightReassignmentsAsync(
        IContext context,
        SpeciationEpochInfo epoch,
        SpeciationMembershipRecord splitMembership,
        AssignmentResolution assignmentResolution,
        double? assignmentSimilarityScore,
        string policyVersion,
        long? decisionTimeMs)
    {
        if (_assignmentPolicy.HindsightReassignCommitWindow <= 0
            || !assignmentSimilarityScore.HasValue
            || string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
            || splitMembership.BrainId == Guid.Empty)
        {
            return 0;
        }

        var targetSpeciesFloor = ResolveSpeciesSimilarityFloor(assignmentResolution.SpeciesId);
        if (!targetSpeciesFloor.HasValue || targetSpeciesFloor.Value.MembershipCount != 1)
        {
            return 0;
        }

        var sourceSpeciesId = assignmentResolution.SourceSpeciesId.Trim();
        if (sourceSpeciesId.Length == 0
            || string.Equals(sourceSpeciesId, assignmentResolution.SpeciesId, StringComparison.Ordinal))
        {
            return 0;
        }

        var founderSimilarity = ClampScore(assignmentSimilarityScore.Value);
        var recent = await _store.ListRecentMembershipsForSpeciesAsync(
            epoch.EpochId,
            sourceSpeciesId,
            splitMembership.AssignedMs,
            _assignmentPolicy.HindsightReassignCommitWindow).ConfigureAwait(false);
        if (recent.Count == 0)
        {
            return 0;
        }

        var reassigned = 0;
        var similarityMargin = Math.Max(0d, _assignmentPolicy.HindsightReassignSimilarityMargin);
        var maxHindsightCandidates = Math.Max(1, DerivedSpeciesBootstrapActualSampleRequirement);
        var orderedCandidates = recent
            .Where(candidate => candidate.BrainId != splitMembership.BrainId)
            .OrderBy(candidate => candidate.AssignedMs)
            .ThenBy(candidate => candidate.BrainId)
            .ToArray();
        var eligibleCandidates = new List<(SpeciationMembershipRecord Candidate, double Similarity)>(
            Math.Min(maxHindsightCandidates, orderedCandidates.Length));
        for (var index = 0; index < orderedCandidates.Length; index++)
        {
            var candidate = orderedCandidates[index];
            if (!TryExtractSourceSpeciesSimilarity(
                    candidate.DecisionMetadataJson,
                    out var candidateSourceSpeciesId,
                    out var candidateSimilarity)
                || !string.Equals(candidateSourceSpeciesId, sourceSpeciesId, StringComparison.Ordinal))
            {
                continue;
            }

            if (candidateSimilarity >= _assignmentPolicy.LineageMatchThreshold)
            {
                continue;
            }

            var hindsightLowerSimilarity = Math.Max(0d, founderSimilarity - similarityMargin);
            var hindsightUpperSimilarity = Math.Min(1d, founderSimilarity + similarityMargin);
            if (candidateSimilarity < hindsightLowerSimilarity
                || candidateSimilarity > hindsightUpperSimilarity)
            {
                continue;
            }

            eligibleCandidates.Add((candidate, candidateSimilarity));
            if (eligibleCandidates.Count >= maxHindsightCandidates)
            {
                break;
            }
        }

        var bootstrapRequirement = await TryResolveBootstrapAssignedSpeciesAdmissionRequirementAsync(
            epoch.EpochId,
            assignmentResolution).ConfigureAwait(false);
        var bootstrapExemplars = await _store.ListEarliestMembershipsForSpeciesAsync(
            epoch.EpochId,
            assignmentResolution.SpeciesId,
            ExternalAdmissionExemplarLimit).ConfigureAwait(false);
        if (bootstrapExemplars.Count == 0)
        {
            return 0;
        }

        for (var index = 0; index < eligibleCandidates.Count; index++)
        {
            var (candidate, candidateSimilarity) = eligibleCandidates[index];
            if (!TryBuildCompatibilitySubjectOptions(
                    candidate,
                    out var candidateSubject,
                    out var candidateFallbackSubject))
            {
                continue;
            }

            var assignedSpeciesAdmissionAssessment = await TryAssessActualAssignedSpeciesAdmissionAsync(
                context,
                epoch,
                candidateSubject,
                candidateFallbackSubject,
                candidate.BrainId,
                assignmentResolution,
                bootstrapRequirement,
                candidateSimilarity,
                bootstrapExemplars).ConfigureAwait(false);
            if (!assignedSpeciesAdmissionAssessment.Admitted)
            {
                continue;
            }

            var reassignmentMetadataJson = BuildHindsightReassignDecisionMetadataJson(
                candidate,
                splitMembership,
                sourceSpeciesId,
                assignmentResolution.SourceSpeciesDisplayName,
                assignmentResolution,
                policyVersion,
                founderSimilarity,
                candidateSimilarity,
                assignedSpeciesAdmissionAssessment);
            var reassignmentTimeMs = Math.Max(
                splitMembership.AssignedMs + 1L + index,
                decisionTimeMs.HasValue
                    ? decisionTimeMs.Value + 1L + index
                    : splitMembership.AssignedMs + 1L + index);

            var reassignment = new SpeciationAssignment(
                BrainId: candidate.BrainId,
                SpeciesId: assignmentResolution.SpeciesId,
                SpeciesDisplayName: assignmentResolution.SpeciesDisplayName,
                PolicyVersion: policyVersion,
                DecisionReason: "lineage_hindsight_recent_reassign",
                DecisionMetadataJson: reassignmentMetadataJson,
                SourceBrainId: splitMembership.BrainId,
                SourceArtifactRef: splitMembership.SourceArtifactRef);
            var outcome = await _store.TryReassignMembershipAsync(
                epoch.EpochId,
                candidate.BrainId,
                expectedSpeciesId: sourceSpeciesId,
                assignment: reassignment,
                decisionTimeMs: reassignmentTimeMs,
                cancellationToken: StoreMutationCancellationToken).ConfigureAwait(false);
            if (outcome.Reassigned)
            {
                reassigned++;
            }
        }

        return reassigned;
    }

    private string BuildHindsightReassignDecisionMetadataJson(
        SpeciationMembershipRecord existingMembership,
        SpeciationMembershipRecord splitMembership,
        string sourceSpeciesId,
        string? sourceSpeciesDisplayName,
        AssignmentResolution assignmentResolution,
        string policyVersion,
        double founderSimilarity,
        double candidateSimilarity,
        AssignedSpeciesAdmissionAssessment assignedSpeciesAdmissionAssessment)
    {
        var metadata = ParseMetadataJson(existingMembership.DecisionMetadataJson);
        metadata["assignment_strategy"] = "lineage_hindsight_reassign";
        metadata["assignment_strategy_detail"] =
            "Recent source-species member reassigned to the new derived species within bounded hindsight window.";
        metadata["policy_version"] = policyVersion;

        var sourcePolicyEffectiveSplitThreshold = assignmentResolution.PolicyEffectiveSplitThreshold > 0d
            ? assignmentResolution.PolicyEffectiveSplitThreshold
            : Math.Max(
                0d,
                _assignmentPolicy.LineageSplitThreshold - _assignmentPolicy.LineageSplitGuardMargin);
        var sourceDynamicSplitThreshold = assignmentResolution.EffectiveSplitThreshold > 0d
            ? assignmentResolution.EffectiveSplitThreshold
            : sourcePolicyEffectiveSplitThreshold;
        var sourceSplitThresholdSource = sourceDynamicSplitThreshold > sourcePolicyEffectiveSplitThreshold
            ? "species_floor"
            : "policy";
        var assignedThresholdState = ResolveSplitThresholdState(
            ResolveSpeciesSimilarityFloor(assignmentResolution.SpeciesId));
        var assignedSplitThresholdSource = assignedThresholdState.UsesSpeciesFloor
            ? "species_floor"
            : "policy";
        var actualAssignedSimilarity = ClampScore(
            assignedSpeciesAdmissionAssessment.SimilarityScore ?? 0d);
        var normalizedCandidateSimilarity = ClampScore(candidateSimilarity);

        var assignmentPolicy = metadata["assignment_policy"] as JsonObject ?? new JsonObject();
        assignmentPolicy["lineage_hindsight_reassign_commit_window"] = _assignmentPolicy.HindsightReassignCommitWindow;
        assignmentPolicy["lineage_hindsight_similarity_margin"] = _assignmentPolicy.HindsightReassignSimilarityMargin;
        assignmentPolicy["lineage_policy_effective_split_threshold"] = assignedThresholdState.PolicyEffectiveSplitThreshold;
        assignmentPolicy["lineage_dynamic_split_threshold"] = assignedThresholdState.DynamicSplitThreshold;
        assignmentPolicy["lineage_split_threshold_source"] = assignedSplitThresholdSource;
        assignmentPolicy["lineage_source_policy_effective_split_threshold"] = sourcePolicyEffectiveSplitThreshold;
        assignmentPolicy["lineage_source_dynamic_split_threshold"] = sourceDynamicSplitThreshold;
        assignmentPolicy["lineage_source_split_threshold_source"] = sourceSplitThresholdSource;
        assignmentPolicy["lineage_assigned_policy_effective_split_threshold"] = assignedThresholdState.PolicyEffectiveSplitThreshold;
        assignmentPolicy["lineage_assigned_dynamic_split_threshold"] = assignedThresholdState.DynamicSplitThreshold;
        assignmentPolicy["lineage_assigned_split_threshold_source"] = assignedSplitThresholdSource;
        assignmentPolicy["lineage_source_species_similarity_score"] = normalizedCandidateSimilarity;
        assignmentPolicy["lineage_assignment_similarity_score"] = actualAssignedSimilarity;
        if (assignedThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            assignmentPolicy["lineage_species_floor_similarity_score"] =
                ClampScore(assignedThresholdState.SpeciesFloorSimilarityScore.Value);
            assignmentPolicy["lineage_species_floor_similarity_samples"] =
                assignedThresholdState.SpeciesFloorSampleCount;
            assignmentPolicy["lineage_species_floor_membership_count"] =
                assignedThresholdState.SpeciesFloorMembershipCount;
            assignmentPolicy["lineage_assigned_species_floor_similarity_score"] =
                ClampScore(assignedThresholdState.SpeciesFloorSimilarityScore.Value);
            assignmentPolicy["lineage_assigned_species_floor_similarity_samples"] =
                assignedThresholdState.SpeciesFloorSampleCount;
            assignmentPolicy["lineage_assigned_species_floor_membership_count"] =
                assignedThresholdState.SpeciesFloorMembershipCount;
        }
        metadata["assignment_policy"] = assignmentPolicy;

        var lineage = metadata["lineage"] as JsonObject ?? new JsonObject();
        lineage["source_species_id"] = sourceSpeciesId;
        lineage["source_species_display_name"] = sourceSpeciesDisplayName ?? string.Empty;
        lineage["dominant_species_id"] = sourceSpeciesId;
        lineage["dominant_species_display_name"] = sourceSpeciesDisplayName ?? string.Empty;
        lineage["source_species_similarity_score"] = normalizedCandidateSimilarity;
        lineage["dominant_species_similarity_score"] = normalizedCandidateSimilarity;
        lineage["lineage_assignment_similarity_score"] = actualAssignedSimilarity;
        lineage["intra_species_similarity_sample"] = actualAssignedSimilarity;
        lineage["intra_species_similarity_species_id"] = assignmentResolution.SpeciesId;
        ApplyAssignedSpeciesCompatibilityMetadata(lineage, assignedSpeciesAdmissionAssessment);
        lineage["lineage_policy_effective_split_threshold"] = assignedThresholdState.PolicyEffectiveSplitThreshold;
        lineage["lineage_dynamic_split_threshold"] = assignedThresholdState.DynamicSplitThreshold;
        lineage["lineage_split_threshold_source"] = assignedSplitThresholdSource;
        lineage["source_policy_effective_split_threshold"] = sourcePolicyEffectiveSplitThreshold;
        lineage["source_dynamic_split_threshold"] = sourceDynamicSplitThreshold;
        lineage["source_split_threshold_source"] = sourceSplitThresholdSource;
        lineage["assigned_policy_effective_split_threshold"] = assignedThresholdState.PolicyEffectiveSplitThreshold;
        lineage["assigned_dynamic_split_threshold"] = assignedThresholdState.DynamicSplitThreshold;
        lineage["assigned_split_threshold_source"] = assignedSplitThresholdSource;
        lineage["split_proximity_to_policy_threshold"] =
            actualAssignedSimilarity - assignedThresholdState.PolicyEffectiveSplitThreshold;
        lineage["split_proximity_to_dynamic_threshold"] =
            actualAssignedSimilarity - assignedThresholdState.DynamicSplitThreshold;
        lineage["assigned_split_proximity_to_policy_threshold"] =
            actualAssignedSimilarity - assignedThresholdState.PolicyEffectiveSplitThreshold;
        lineage["assigned_split_proximity_to_dynamic_threshold"] =
            actualAssignedSimilarity - assignedThresholdState.DynamicSplitThreshold;
        lineage["source_split_proximity_to_policy_threshold"] =
            normalizedCandidateSimilarity - sourcePolicyEffectiveSplitThreshold;
        lineage["source_split_proximity_to_dynamic_threshold"] =
            normalizedCandidateSimilarity - sourceDynamicSplitThreshold;
        if (assignedThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            lineage["species_floor_similarity_score"] =
                ClampScore(assignedThresholdState.SpeciesFloorSimilarityScore.Value);
            lineage["species_floor_similarity_samples"] = assignedThresholdState.SpeciesFloorSampleCount;
            lineage["species_floor_membership_count"] = assignedThresholdState.SpeciesFloorMembershipCount;
            lineage["assigned_species_floor_similarity_score"] =
                ClampScore(assignedThresholdState.SpeciesFloorSimilarityScore.Value);
            lineage["assigned_species_floor_similarity_samples"] = assignedThresholdState.SpeciesFloorSampleCount;
            lineage["assigned_species_floor_membership_count"] = assignedThresholdState.SpeciesFloorMembershipCount;
        }
        lineage["hindsight_source_species_id"] = sourceSpeciesId;
        lineage["hindsight_target_species_id"] = assignmentResolution.SpeciesId;
        lineage["hindsight_target_species_display_name"] = assignmentResolution.SpeciesDisplayName;
        lineage["hindsight_founder_brain_id"] = splitMembership.BrainId.ToString("D");
        lineage["hindsight_founder_similarity_score"] = ClampScore(founderSimilarity);
        lineage["hindsight_candidate_source_similarity_score"] = ClampScore(candidateSimilarity);
        lineage["hindsight_similarity_margin"] = _assignmentPolicy.HindsightReassignSimilarityMargin;
        lineage["hindsight_similarity_lower_bound"] = Math.Max(
            0d,
            ClampScore(founderSimilarity) - _assignmentPolicy.HindsightReassignSimilarityMargin);
        lineage["hindsight_similarity_upper_bound"] = Math.Min(
            1d,
            ClampScore(founderSimilarity) + _assignmentPolicy.HindsightReassignSimilarityMargin);
        lineage["hindsight_window_commits"] = _assignmentPolicy.HindsightReassignCommitWindow;
        metadata["lineage"] = lineage;

        return metadata.ToJsonString(MetadataJsonSerializerOptions);
    }

    private static bool TryExtractSourceSpeciesSimilarity(
        string? decisionMetadataJson,
        out string sourceSpeciesId,
        out double similarityScore)
    {
        sourceSpeciesId = string.Empty;
        similarityScore = 0d;
        if (string.IsNullOrWhiteSpace(decisionMetadataJson))
        {
            return false;
        }

        var similarityEvidence = ExtractSimilarityEvidence(decisionMetadataJson);
        var resolvedSimilarity = similarityEvidence.DominantSpeciesSimilarityScore
            ?? similarityEvidence.SimilarityScore;
        if (!resolvedSimilarity.HasValue)
        {
            return false;
        }

        JsonNode? node;
        try
        {
            node = JsonNode.Parse(decisionMetadataJson);
        }
        catch (JsonException)
        {
            return false;
        }

        JsonNode? lineageNode = null;
        if (node is JsonObject root)
        {
            root.TryGetPropertyValue("lineage", out lineageNode);
        }

        sourceSpeciesId = FindStringValue(
            lineageNode,
            "source_species_id",
            "sourceSpeciesId")
            ?? FindStringValue(
                lineageNode,
                "dominant_species_id",
                "dominantSpeciesId")
            ?? FindStringValue(
                node,
                "source_species_id",
                "sourceSpeciesId")
            ?? FindStringValue(
                node,
                "dominant_species_id",
                "dominantSpeciesId")
            ?? string.Empty;
        if (string.IsNullOrWhiteSpace(sourceSpeciesId))
        {
            return false;
        }

        sourceSpeciesId = sourceSpeciesId.Trim();
        var sourceSimilarity = FindNumericValue(
            lineageNode,
            "source_species_similarity_score",
            "sourceSpeciesSimilarityScore")
            ?? FindNumericValue(
                node,
                "source_species_similarity_score",
                "sourceSpeciesSimilarityScore");
        similarityScore = ClampScore((sourceSimilarity ?? resolvedSimilarity).Value);
        return true;
    }

    private static bool TryExtractRecentDerivedSpeciesHint(
        SpeciationMembershipRecord membership,
        out RecentDerivedSpeciesHint hint)
    {
        hint = default;
        if (membership is null
            || membership.AssignedMs <= 0
            || !string.Equals(
                membership.DecisionReason,
                "lineage_diverged_new_species",
                StringComparison.Ordinal)
            || string.IsNullOrWhiteSpace(membership.SpeciesId)
            || !TryExtractSourceSpeciesSimilarity(
                membership.DecisionMetadataJson,
                out var sourceSpeciesId,
                out var founderSimilarity))
        {
            return false;
        }

        var metadata = ParseMetadataJson(membership.DecisionMetadataJson);
        metadata.TryGetPropertyValue("lineage", out var lineageNode);
        var sourceSpeciesDisplayName = FindStringValue(
            lineageNode,
            "source_species_display_name",
            "sourceSpeciesDisplayName")
            ?? FindStringValue(
                lineageNode,
                "dominant_species_display_name",
                "dominantSpeciesDisplayName")
            ?? string.Empty;

        var targetSpeciesId = membership.SpeciesId.Trim();
        if (targetSpeciesId.Length == 0
            || string.Equals(targetSpeciesId, sourceSpeciesId, StringComparison.Ordinal))
        {
            return false;
        }

        hint = new RecentDerivedSpeciesHint(
            SourceSpeciesId: sourceSpeciesId,
            SourceSpeciesDisplayName: sourceSpeciesDisplayName,
            TargetSpeciesId: targetSpeciesId,
            TargetSpeciesDisplayName: membership.SpeciesDisplayName,
            FounderSimilarityScore: ClampScore(founderSimilarity),
            AssignedMs: membership.AssignedMs);
        return true;
    }

    private bool TryResolveRecentDerivedSpeciesReuse(
        string? sourceSpeciesId,
        double similarity,
        out RecentDerivedSpeciesHint hint)
    {
        hint = default;
        if (_assignmentPolicy.HindsightReassignCommitWindow <= 0
            || string.IsNullOrWhiteSpace(sourceSpeciesId)
            || !_recentDerivedSpeciesHintsBySourceSpecies.TryGetValue(
                sourceSpeciesId.Trim(),
                out var hints)
            || hints.Count == 0)
        {
            return false;
        }

        var margin = Math.Max(0d, _assignmentPolicy.HindsightReassignSimilarityMargin);
        var candidate = hints
            .Where(existing => IsWithinSimilarityBand(existing.FounderSimilarityScore, similarity, margin))
            .OrderBy(existing => Math.Abs(existing.FounderSimilarityScore - similarity))
            .ThenByDescending(existing => existing.AssignedMs)
            .ThenBy(existing => existing.TargetSpeciesId, StringComparer.Ordinal)
            .FirstOrDefault();
        if (string.IsNullOrWhiteSpace(candidate.TargetSpeciesId))
        {
            return false;
        }

        hint = candidate;
        return true;
    }

    private bool TryGetRecentDerivedSpeciesHintByTargetSpecies(
        string? speciesId,
        out RecentDerivedSpeciesHint hint)
    {
        hint = default;
        if (string.IsNullOrWhiteSpace(speciesId))
        {
            return false;
        }

        return _recentDerivedSpeciesHintsByTargetSpecies.TryGetValue(speciesId.Trim(), out hint);
    }

    private static bool IsWithinSimilarityBand(
        double centerSimilarity,
        double candidateSimilarity,
        double margin)
    {
        var normalizedMargin = Math.Max(0d, margin);
        var lowerBound = Math.Max(0d, ClampScore(centerSimilarity) - normalizedMargin);
        var upperBound = Math.Min(1d, ClampScore(centerSimilarity) + normalizedMargin);
        var normalizedCandidate = ClampScore(candidateSimilarity);
        return normalizedCandidate >= lowerBound && normalizedCandidate <= upperBound;
    }
}
