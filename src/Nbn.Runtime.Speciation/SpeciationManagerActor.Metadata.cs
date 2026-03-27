using System.Text.Json;
using System.Text.Json.Nodes;

namespace Nbn.Runtime.Speciation;

public sealed partial class SpeciationManagerActor
{
    private static void ApplyAssignedSpeciesCompatibilityMetadata(
        JsonObject lineage,
        AssignedSpeciesAdmissionAssessment assignedSpeciesAdmissionAssessment)
    {
        if (assignedSpeciesAdmissionAssessment.Admitted)
        {
            lineage["assigned_species_similarity_source"] = "compatibility_assessment";
        }

        lineage["assigned_species_compatibility_attempted"] =
            assignedSpeciesAdmissionAssessment.AssessmentAttempted;
        lineage["assigned_species_compatibility_admitted"] =
            assignedSpeciesAdmissionAssessment.Admitted;
        if (!string.IsNullOrWhiteSpace(assignedSpeciesAdmissionAssessment.AssessmentMode))
        {
            lineage["assigned_species_compatibility_mode"] =
                assignedSpeciesAdmissionAssessment.AssessmentMode;
        }

        if (assignedSpeciesAdmissionAssessment.SimilarityScore.HasValue)
        {
            lineage["assigned_species_compatibility_similarity_score"] = ClampScore(
                assignedSpeciesAdmissionAssessment.SimilarityScore.Value);
        }

        lineage["assigned_species_compatibility_report_compatible"] =
            assignedSpeciesAdmissionAssessment.Compatible;
        lineage["assigned_species_compatibility_abort_reason"] =
            assignedSpeciesAdmissionAssessment.AbortReason;
        lineage["assigned_species_compatibility_failure_reason"] =
            assignedSpeciesAdmissionAssessment.FailureReason;
        lineage["assigned_species_compatibility_elapsed_ms"] =
            assignedSpeciesAdmissionAssessment.ElapsedMs;
        lineage["assigned_species_compatibility_exemplar_count"] =
            assignedSpeciesAdmissionAssessment.ExemplarBrainIds.Length;
        var exemplarBrainIds = new JsonArray();
        foreach (var exemplarBrainId in assignedSpeciesAdmissionAssessment.ExemplarBrainIds)
        {
            exemplarBrainIds.Add(exemplarBrainId);
        }

        lineage["assigned_species_compatibility_exemplar_brain_ids"] = exemplarBrainIds;
    }

    private string BuildDecisionMetadataJson(
        string? sourceMetadataJson,
        string policyVersion,
        ResolvedCandidate resolvedCandidate,
        AssignmentResolution assignmentResolution,
        LineageEvidence lineageEvidence,
        SimilarityEvidence similarityEvidence,
        SpeciesSimilarityFloorState? sourceSpeciesFloor,
        double? sourceSpeciesSimilarityScore,
        double? assignedSpeciesSimilarityScore,
        double? intraSpeciesSimilaritySample,
        AssignedSpeciesAdmissionAssessment? assignedSpeciesAdmissionAssessment)
    {
        var metadata = ParseMetadataJson(sourceMetadataJson);
        metadata.TryGetPropertyValue("lineage", out var existingLineageNode);
        metadata["assignment_strategy"] = assignmentResolution.Strategy;
        metadata["assignment_strategy_detail"] = assignmentResolution.StrategyDetail;
        metadata["policy_version"] = policyVersion;

        var sourcePolicyEffectiveSplitThreshold = assignmentResolution.PolicyEffectiveSplitThreshold > 0d
            ? assignmentResolution.PolicyEffectiveSplitThreshold
            : Math.Max(
                0d,
                _assignmentPolicy.LineageSplitThreshold - _assignmentPolicy.LineageSplitGuardMargin);
        var sourceDynamicSplitThreshold = assignmentResolution.EffectiveSplitThreshold > 0d
            ? assignmentResolution.EffectiveSplitThreshold
            : sourcePolicyEffectiveSplitThreshold;
        var sourceSpeciesFloorSimilarityScore = assignmentResolution.SpeciesFloorSimilarityScore
            ?? sourceSpeciesFloor?.MinSimilarityScore;
        var sourceSpeciesFloorSampleCount = assignmentResolution.SpeciesFloorSampleCount > 0
            ? assignmentResolution.SpeciesFloorSampleCount
            : Math.Max(0, sourceSpeciesFloor?.SimilaritySampleCount ?? 0);
        var sourceSpeciesFloorMembershipCount = assignmentResolution.SpeciesFloorMembershipCount > 0
            ? assignmentResolution.SpeciesFloorMembershipCount
            : Math.Max(0, sourceSpeciesFloor?.MembershipCount ?? 0);
        var sourceThresholdState = new SplitThresholdState(
            PolicyEffectiveSplitThreshold: sourcePolicyEffectiveSplitThreshold,
            DynamicSplitThreshold: sourceDynamicSplitThreshold,
            UsesSpeciesFloor: sourceDynamicSplitThreshold > sourcePolicyEffectiveSplitThreshold,
            SpeciesFloorSimilarityScore: sourceSpeciesFloorSimilarityScore,
            SpeciesFloorSampleCount: sourceSpeciesFloorSampleCount,
            SpeciesFloorMembershipCount: sourceSpeciesFloorMembershipCount);
        var resolvedSourceSpeciesId = string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
            ? string.Empty
            : assignmentResolution.SourceSpeciesId;
        var resolvedSourceSpeciesDisplayName = string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesDisplayName)
            ? (string.IsNullOrWhiteSpace(resolvedSourceSpeciesId)
                ? string.Empty
                : ResolveSpeciesDisplayName(null, resolvedSourceSpeciesId))
            : assignmentResolution.SourceSpeciesDisplayName;
        var resolvedSourceConsensusShare = string.IsNullOrWhiteSpace(resolvedSourceSpeciesId)
            ? 0d
            : assignmentResolution.SourceConsensusShare;
        var assignedThresholdState = ResolveSplitThresholdState(
            ResolveSpeciesSimilarityFloor(assignmentResolution.SpeciesId));
        var sourceThresholdSource = sourceThresholdState.UsesSpeciesFloor
            ? "species_floor"
            : "policy";
        var assignedThresholdSource = assignedThresholdState.UsesSpeciesFloor
            ? "species_floor"
            : "policy";
        var useAssignedSplitPerspective = assignedSpeciesSimilarityScore.HasValue
            || string.IsNullOrWhiteSpace(resolvedSourceSpeciesId)
            || string.Equals(
                assignmentResolution.SpeciesId,
                resolvedSourceSpeciesId,
                StringComparison.Ordinal);
        var genericThresholdState = useAssignedSplitPerspective
            ? assignedThresholdState
            : sourceThresholdState;
        var genericThresholdSource = useAssignedSplitPerspective
            ? assignedThresholdSource
            : sourceThresholdSource;

        var assignmentPolicy = new JsonObject
        {
            ["lineage_match_threshold"] = _assignmentPolicy.LineageMatchThreshold,
            ["lineage_split_threshold"] = _assignmentPolicy.LineageSplitThreshold,
            ["lineage_effective_split_threshold"] = Math.Max(
                0d,
                _assignmentPolicy.LineageSplitThreshold - _assignmentPolicy.LineageSplitGuardMargin),
            ["lineage_split_guard_margin"] = _assignmentPolicy.LineageSplitGuardMargin,
            ["lineage_min_parent_memberships_before_split"] = _assignmentPolicy.MinParentMembershipsBeforeSplit,
            ["parent_consensus_threshold"] = _assignmentPolicy.ParentConsensusThreshold,
            ["hysteresis_margin"] = _assignmentPolicy.HysteresisMargin,
            ["lineage_realign_parent_membership_window"] = _assignmentPolicy.RecentSplitRealignParentMembershipWindow,
            ["lineage_realign_match_margin"] = _assignmentPolicy.RecentSplitRealignMatchMargin,
            ["lineage_hindsight_reassign_commit_window"] = _assignmentPolicy.HindsightReassignCommitWindow,
            ["lineage_hindsight_similarity_margin"] = _assignmentPolicy.HindsightReassignSimilarityMargin,
            ["create_derived_species_on_divergence"] = _assignmentPolicy.CreateDerivedSpeciesOnDivergence,
            ["derived_species_prefix"] = _assignmentPolicy.DerivedSpeciesPrefix,
            ["lineage_policy_effective_split_threshold"] = genericThresholdState.PolicyEffectiveSplitThreshold,
            ["lineage_dynamic_split_threshold"] = genericThresholdState.DynamicSplitThreshold,
            ["lineage_split_threshold_source"] = genericThresholdSource,
            ["lineage_source_policy_effective_split_threshold"] = sourceThresholdState.PolicyEffectiveSplitThreshold,
            ["lineage_source_dynamic_split_threshold"] = sourceThresholdState.DynamicSplitThreshold,
            ["lineage_source_split_threshold_source"] = sourceThresholdSource,
            ["lineage_assigned_policy_effective_split_threshold"] = assignedThresholdState.PolicyEffectiveSplitThreshold,
            ["lineage_assigned_dynamic_split_threshold"] = assignedThresholdState.DynamicSplitThreshold,
            ["lineage_assigned_split_threshold_source"] = assignedThresholdSource,
            ["lineage_species_floor_relaxation_margin"] = _assignmentPolicy.HysteresisMargin
        };
        if (sourceSpeciesSimilarityScore.HasValue)
        {
            assignmentPolicy["lineage_source_species_similarity_score"] =
                ClampScore(sourceSpeciesSimilarityScore.Value);
        }
        if (assignedSpeciesSimilarityScore.HasValue)
        {
            assignmentPolicy["lineage_assignment_similarity_score"] =
                ClampScore(assignedSpeciesSimilarityScore.Value);
        }
        if (genericThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            assignmentPolicy["lineage_species_floor_similarity_score"] =
                ClampScore(genericThresholdState.SpeciesFloorSimilarityScore.Value);
            assignmentPolicy["lineage_species_floor_similarity_samples"] =
                genericThresholdState.SpeciesFloorSampleCount;
            assignmentPolicy["lineage_species_floor_membership_count"] =
                genericThresholdState.SpeciesFloorMembershipCount;
        }
        if (sourceThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            assignmentPolicy["lineage_source_species_floor_similarity_score"] =
                ClampScore(sourceThresholdState.SpeciesFloorSimilarityScore.Value);
            assignmentPolicy["lineage_source_species_floor_similarity_samples"] =
                sourceThresholdState.SpeciesFloorSampleCount;
            assignmentPolicy["lineage_source_species_floor_membership_count"] =
                sourceThresholdState.SpeciesFloorMembershipCount;
        }
        if (assignedThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            assignmentPolicy["lineage_assigned_species_floor_similarity_score"] =
                ClampScore(assignedThresholdState.SpeciesFloorSimilarityScore.Value);
            assignmentPolicy["lineage_assigned_species_floor_similarity_samples"] =
                assignedThresholdState.SpeciesFloorSampleCount;
            assignmentPolicy["lineage_assigned_species_floor_membership_count"] =
                assignedThresholdState.SpeciesFloorMembershipCount;
        }
        metadata["assignment_policy"] = assignmentPolicy;

        var parentBrainIds = new JsonArray();
        foreach (var parentBrainId in lineageEvidence.ParentBrainIds)
        {
            parentBrainIds.Add(parentBrainId.ToString("D"));
        }

        var parentArtifactRefs = new JsonArray();
        foreach (var parentArtifactRef in lineageEvidence.ParentArtifactRefs)
        {
            parentArtifactRefs.Add(parentArtifactRef);
        }

        var lineage = new JsonObject
        {
            ["candidate_mode"] = resolvedCandidate.CandidateMode.ToString(),
            ["lineage_key"] = lineageEvidence.LineageKey,
            ["parent_membership_count"] = lineageEvidence.ParentMembershipCount,
            ["parent_brain_ids"] = parentBrainIds,
            ["parent_artifact_refs"] = parentArtifactRefs,
            ["source_species_id"] = resolvedSourceSpeciesId,
            ["source_species_display_name"] = resolvedSourceSpeciesDisplayName,
            ["source_species_share"] = resolvedSourceConsensusShare,
            ["dominant_species_id"] = resolvedSourceSpeciesId,
            ["dominant_species_display_name"] = resolvedSourceSpeciesDisplayName,
            ["dominant_species_share"] = resolvedSourceConsensusShare,
            ["hysteresis_species_id"] = lineageEvidence.HysteresisSpeciesId ?? string.Empty,
            ["hysteresis_species_display_name"] = lineageEvidence.HysteresisSpeciesDisplayName ?? string.Empty,
            ["lineage_policy_effective_split_threshold"] = genericThresholdState.PolicyEffectiveSplitThreshold,
            ["lineage_dynamic_split_threshold"] = genericThresholdState.DynamicSplitThreshold,
            ["lineage_split_threshold_source"] = genericThresholdSource,
            ["source_policy_effective_split_threshold"] = sourceThresholdState.PolicyEffectiveSplitThreshold,
            ["source_dynamic_split_threshold"] = sourceThresholdState.DynamicSplitThreshold,
            ["source_split_threshold_source"] = sourceThresholdSource,
            ["assigned_policy_effective_split_threshold"] = assignedThresholdState.PolicyEffectiveSplitThreshold,
            ["assigned_dynamic_split_threshold"] = assignedThresholdState.DynamicSplitThreshold,
            ["assigned_split_threshold_source"] = assignedThresholdSource
        };
        if (resolvedCandidate.CandidateArtifactRef is not null
            && HasUsableArtifactReference(resolvedCandidate.CandidateArtifactRef))
        {
            lineage["candidate_artifact_ref"] = BuildStoredArtifactRefNode(resolvedCandidate.CandidateArtifactRef);
        }
        if (resolvedCandidate.CandidateBrainBaseArtifactRef is not null
            && HasUsableArtifactReference(resolvedCandidate.CandidateBrainBaseArtifactRef))
        {
            lineage["candidate_brain_base_artifact_ref"] = BuildStoredArtifactRefNode(
                resolvedCandidate.CandidateBrainBaseArtifactRef);
        }
        if (resolvedCandidate.CandidateBrainSnapshotArtifactRef is not null
            && HasUsableArtifactReference(resolvedCandidate.CandidateBrainSnapshotArtifactRef))
        {
            lineage["candidate_brain_snapshot_artifact_ref"] = BuildStoredArtifactRefNode(
                resolvedCandidate.CandidateBrainSnapshotArtifactRef);
        }
        if (!string.IsNullOrWhiteSpace(resolvedCandidate.CandidateArtifactUri))
        {
            lineage["candidate_artifact_uri"] = resolvedCandidate.CandidateArtifactUri;
        }
        if (genericThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            lineage["species_floor_similarity_score"] =
                ClampScore(genericThresholdState.SpeciesFloorSimilarityScore.Value);
            lineage["species_floor_similarity_samples"] =
                genericThresholdState.SpeciesFloorSampleCount;
            lineage["species_floor_membership_count"] =
                genericThresholdState.SpeciesFloorMembershipCount;
        }
        if (sourceThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            lineage["source_species_floor_similarity_score"] =
                ClampScore(sourceThresholdState.SpeciesFloorSimilarityScore.Value);
            lineage["source_species_floor_similarity_samples"] =
                sourceThresholdState.SpeciesFloorSampleCount;
            lineage["source_species_floor_membership_count"] =
                sourceThresholdState.SpeciesFloorMembershipCount;
        }
        if (assignedThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            lineage["assigned_species_floor_similarity_score"] =
                ClampScore(assignedThresholdState.SpeciesFloorSimilarityScore.Value);
            lineage["assigned_species_floor_similarity_samples"] =
                assignedThresholdState.SpeciesFloorSampleCount;
            lineage["assigned_species_floor_membership_count"] =
                assignedThresholdState.SpeciesFloorMembershipCount;
        }
        if (sourceSpeciesSimilarityScore.HasValue)
        {
            lineage["source_species_similarity_score"] = ClampScore(sourceSpeciesSimilarityScore.Value);
        }
        if (assignedSpeciesSimilarityScore.HasValue)
        {
            lineage["lineage_assignment_similarity_score"] = ClampScore(assignedSpeciesSimilarityScore.Value);
        }
        if (!string.IsNullOrWhiteSpace(assignmentResolution.RecentDerivedSourceSpeciesId))
        {
            lineage["recent_derived_source_species_id"] = assignmentResolution.RecentDerivedSourceSpeciesId;
            lineage["recent_derived_source_species_display_name"] =
                assignmentResolution.RecentDerivedSourceSpeciesDisplayName ?? string.Empty;
        }
        if (assignmentResolution.RecentDerivedFounderSimilarityScore.HasValue)
        {
            lineage["recent_derived_founder_similarity_score"] = ClampScore(
                assignmentResolution.RecentDerivedFounderSimilarityScore.Value);
            lineage["recent_derived_similarity_margin"] = _assignmentPolicy.HindsightReassignSimilarityMargin;
        }
        if (intraSpeciesSimilaritySample.HasValue)
        {
            lineage["intra_species_similarity_sample"] = ClampScore(intraSpeciesSimilaritySample.Value);
            lineage["intra_species_similarity_species_id"] = assignmentResolution.SpeciesId;
        }
        if (assignedSpeciesAdmissionAssessment.HasValue)
        {
            ApplyAssignedSpeciesCompatibilityMetadata(
                lineage,
                assignedSpeciesAdmissionAssessment.Value);
        }
        AddLineageSimilarityScore(
            lineage,
            "lineage_similarity_score",
            similarityEvidence.SimilarityScore,
            existingLineageNode,
            "lineage_similarity_score",
            "lineageSimilarityScore",
            "similarity_score",
            "similarityScore");
        AddLineageSimilarityScore(
            lineage,
            "parent_a_similarity_score",
            preferredValue: similarityEvidence.ParentASimilarityScore,
            existingLineageNode,
            "parent_a_similarity_score",
            "parentASimilarityScore",
            "lineage_parent_a_similarity_score",
            "lineageParentASimilarityScore");
        AddLineageSimilarityScore(
            lineage,
            "parent_b_similarity_score",
            preferredValue: similarityEvidence.ParentBSimilarityScore,
            existingLineageNode,
            "parent_b_similarity_score",
            "parentBSimilarityScore",
            "lineage_parent_b_similarity_score",
            "lineageParentBSimilarityScore");
        AddLineageSimilarityScore(
            lineage,
            "dominant_species_similarity_score",
            sourceSpeciesSimilarityScore ?? similarityEvidence.DominantSpeciesSimilarityScore,
            existingLineageNode,
            "dominant_species_similarity_score",
            "dominantSpeciesSimilarityScore");
        AddLineageSimilarityScore(
            lineage,
            "source_species_similarity_score",
            sourceSpeciesSimilarityScore,
            existingLineageNode,
            "source_species_similarity_score",
            "sourceSpeciesSimilarityScore");
        var sourceSplitSimilarity = sourceSpeciesSimilarityScore ?? similarityEvidence.SimilarityScore;
        if (sourceSplitSimilarity.HasValue)
        {
            var normalizedSimilarity = ClampScore(sourceSplitSimilarity.Value);
            lineage["source_split_proximity_to_policy_threshold"] =
                normalizedSimilarity - sourceThresholdState.PolicyEffectiveSplitThreshold;
            lineage["source_split_proximity_to_dynamic_threshold"] =
                normalizedSimilarity - sourceThresholdState.DynamicSplitThreshold;
        }

        double? assignedSplitSimilarity = assignedSpeciesSimilarityScore;
        if (!assignedSplitSimilarity.HasValue
            && string.Equals(
                assignmentResolution.SpeciesId,
                resolvedSourceSpeciesId,
                StringComparison.Ordinal))
        {
            assignedSplitSimilarity = sourceSplitSimilarity;
        }

        if (assignedSplitSimilarity.HasValue)
        {
            var normalizedSimilarity = ClampScore(assignedSplitSimilarity.Value);
            lineage["assigned_split_proximity_to_policy_threshold"] =
                normalizedSimilarity - assignedThresholdState.PolicyEffectiveSplitThreshold;
            lineage["assigned_split_proximity_to_dynamic_threshold"] =
                normalizedSimilarity - assignedThresholdState.DynamicSplitThreshold;
        }

        var splitSimilarity = useAssignedSplitPerspective
            ? assignedSplitSimilarity ?? sourceSplitSimilarity
            : sourceSplitSimilarity;
        if (splitSimilarity.HasValue)
        {
            var normalizedSimilarity = ClampScore(splitSimilarity.Value);
            lineage["split_proximity_to_policy_threshold"] =
                normalizedSimilarity - genericThresholdState.PolicyEffectiveSplitThreshold;
            lineage["split_proximity_to_dynamic_threshold"] =
                normalizedSimilarity - genericThresholdState.DynamicSplitThreshold;
        }
        metadata["lineage"] = lineage;

        var scores = new JsonObject();
        if (similarityEvidence.SimilarityScore.HasValue)
        {
            scores["similarity_score"] = ClampScore(similarityEvidence.SimilarityScore.Value);
        }

        if (similarityEvidence.FunctionScore.HasValue)
        {
            scores["function_score"] = similarityEvidence.FunctionScore.Value;
        }

        if (similarityEvidence.ConnectivityScore.HasValue)
        {
            scores["connectivity_score"] = similarityEvidence.ConnectivityScore.Value;
        }

        if (similarityEvidence.RegionSpanScore.HasValue)
        {
            scores["region_span_score"] = similarityEvidence.RegionSpanScore.Value;
        }

        metadata["scores"] = scores;
        return metadata.ToJsonString(MetadataJsonSerializerOptions);
    }

    private static void AddLineageSimilarityScore(
        JsonObject target,
        string key,
        double? preferredValue,
        JsonNode? existingLineageNode,
        params string[] existingAliases)
    {
        var resolved = preferredValue ?? FindNumericValue(existingLineageNode, existingAliases);
        if (resolved.HasValue)
        {
            target[key] = ClampScore(resolved.Value);
        }
    }

    private static SimilarityEvidence ExtractSimilarityEvidence(string? decisionMetadataJson)
    {
        if (string.IsNullOrWhiteSpace(decisionMetadataJson))
        {
            return new SimilarityEvidence(
                SimilarityScore: null,
                DominantSpeciesSimilarityScore: null,
                ParentASimilarityScore: null,
                ParentBSimilarityScore: null,
                FunctionScore: null,
                ConnectivityScore: null,
                RegionSpanScore: null);
        }

        JsonNode? node;
        try
        {
            node = JsonNode.Parse(decisionMetadataJson);
        }
        catch (JsonException)
        {
            return new SimilarityEvidence(
                SimilarityScore: null,
                DominantSpeciesSimilarityScore: null,
                ParentASimilarityScore: null,
                ParentBSimilarityScore: null,
                FunctionScore: null,
                ConnectivityScore: null,
                RegionSpanScore: null);
        }

        JsonNode? lineageNode = null;
        if (node is JsonObject rootObject)
        {
            rootObject.TryGetPropertyValue("lineage", out lineageNode);
        }

        var similarityScore = FindNumericValue(
            lineageNode,
            "lineage_similarity_score",
            "lineageSimilarityScore")
            ?? FindNumericValue(
                lineageNode,
                "similarity_score",
                "similarityScore")
            ?? FindNumericValue(
                node,
                "lineage_similarity_score",
                "lineageSimilarityScore")
            ?? FindNumericValue(
                node,
                "similarity_score",
                "similarityScore");
        var dominantSpeciesSimilarityScore = FindNumericValue(
            lineageNode,
            "dominant_species_similarity_score",
            "dominantSpeciesSimilarityScore")
            ?? FindNumericValue(
                node,
                "dominant_species_similarity_score",
                "dominantSpeciesSimilarityScore");
        var parentASimilarityScore = FindNumericValue(
            lineageNode,
            "parent_a_similarity_score",
            "parentASimilarityScore",
            "lineage_parent_a_similarity_score",
            "lineageParentASimilarityScore")
            ?? FindNumericValue(
                node,
                "parent_a_similarity_score",
                "parentASimilarityScore",
                "lineage_parent_a_similarity_score",
                "lineageParentASimilarityScore");
        var parentBSimilarityScore = FindNumericValue(
            lineageNode,
            "parent_b_similarity_score",
            "parentBSimilarityScore",
            "lineage_parent_b_similarity_score",
            "lineageParentBSimilarityScore")
            ?? FindNumericValue(
                node,
                "parent_b_similarity_score",
                "parentBSimilarityScore",
                "lineage_parent_b_similarity_score",
                "lineageParentBSimilarityScore");
        var functionScore = FindNumericValue(
            node,
            "function_score",
            "functionScore");
        var connectivityScore = FindNumericValue(
            node,
            "connectivity_score",
            "connectivityScore");
        var regionSpanScore = FindNumericValue(
            node,
            "region_span_score",
            "regionSpanScore");
        return new SimilarityEvidence(
            SimilarityScore: similarityScore,
            DominantSpeciesSimilarityScore: dominantSpeciesSimilarityScore,
            ParentASimilarityScore: parentASimilarityScore,
            ParentBSimilarityScore: parentBSimilarityScore,
            FunctionScore: functionScore,
            ConnectivityScore: connectivityScore,
            RegionSpanScore: regionSpanScore);
    }

    private static double? FindNumericValue(JsonNode? node, params string[] aliases)
    {
        if (node is null || aliases.Length == 0)
        {
            return null;
        }

        var normalizedAliases = aliases
            .Select(NormalizeJsonKey)
            .Where(alias => alias.Length > 0)
            .ToHashSet(StringComparer.Ordinal);
        return normalizedAliases.Count == 0
            ? null
            : TryFindNumericValue(node, normalizedAliases, out var value)
                ? value
                : null;
    }

    private static string? FindStringValue(JsonNode? node, params string[] aliases)
    {
        if (node is null || aliases.Length == 0)
        {
            return null;
        }

        var normalizedAliases = aliases
            .Select(NormalizeJsonKey)
            .Where(alias => alias.Length > 0)
            .ToHashSet(StringComparer.Ordinal);
        if (normalizedAliases.Count == 0)
        {
            return null;
        }

        return TryFindStringValue(node, normalizedAliases, out var value)
            ? value
            : null;
    }

    private static bool TryFindNumericValue(
        JsonNode? node,
        HashSet<string> normalizedAliases,
        out double value)
    {
        if (node is JsonObject obj)
        {
            foreach (var property in obj)
            {
                if (normalizedAliases.Contains(NormalizeJsonKey(property.Key))
                    && TryReadDouble(property.Value, out value))
                {
                    return true;
                }

                if (TryFindNumericValue(property.Value, normalizedAliases, out value))
                {
                    return true;
                }
            }
        }
        else if (node is JsonArray array)
        {
            foreach (var item in array)
            {
                if (TryFindNumericValue(item, normalizedAliases, out value))
                {
                    return true;
                }
            }
        }

        value = default;
        return false;
    }

    private static bool TryFindStringValue(
        JsonNode? node,
        HashSet<string> normalizedAliases,
        out string value)
    {
        if (node is JsonObject obj)
        {
            foreach (var property in obj)
            {
                if (normalizedAliases.Contains(NormalizeJsonKey(property.Key))
                    && property.Value is JsonValue jsonValue
                    && jsonValue.TryGetValue<string>(out var textValue)
                    && !string.IsNullOrWhiteSpace(textValue))
                {
                    value = textValue.Trim();
                    return true;
                }

                if (TryFindStringValue(property.Value, normalizedAliases, out value))
                {
                    return true;
                }
            }
        }
        else if (node is JsonArray array)
        {
            foreach (var item in array)
            {
                if (TryFindStringValue(item, normalizedAliases, out value))
                {
                    return true;
                }
            }
        }

        value = string.Empty;
        return false;
    }

    private static JsonObject ParseMetadataJson(string? sourceMetadataJson)
    {
        if (string.IsNullOrWhiteSpace(sourceMetadataJson))
        {
            return new JsonObject();
        }

        var trimmed = sourceMetadataJson.Trim();
        try
        {
            var parsed = JsonNode.Parse(trimmed);
            if (parsed is JsonObject obj)
            {
                return (JsonObject)obj.DeepClone();
            }

            return new JsonObject
            {
                ["source_metadata"] = parsed?.DeepClone()
            };
        }
        catch (JsonException)
        {
            return new JsonObject
            {
                ["source_metadata_raw"] = trimmed
            };
        }
    }
}
