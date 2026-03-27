using Nbn.Proto;
using System.Globalization;

namespace Nbn.Runtime.Speciation;

public sealed partial class SpeciationManagerActor
{
    private AssignmentResolution ResolveAssignment(
        SpeciationEpochInfo epoch,
        string? requestedSpeciesId,
        string? requestedSpeciesDisplayName,
        LineageEvidence lineageEvidence,
        SpeciesSimilarityFloorState? sourceSpeciesFloor,
        double? sourceSpeciesSimilarityScore,
        ParentSpeciesPairwiseFit? bestParentSpeciesFit,
        bool isSeedFounderCandidate,
        bool allowRecentSplitRealign = true,
        bool allowRecentDerivedSpeciesReuse = true)
    {
        if (!string.IsNullOrWhiteSpace(requestedSpeciesId))
        {
            var speciesId = requestedSpeciesId.Trim();
            var speciesDisplayName = ResolveTrackedSpeciesDisplayName(speciesId, requestedSpeciesDisplayName);
            return new AssignmentResolution(
                speciesId,
                speciesDisplayName,
                DecisionReason: "explicit_species",
                Strategy: "explicit_species",
                StrategyDetail: "Request provided explicit species_id.",
                ForceDecisionReason: false);
        }

        if (lineageEvidence.ParentMembershipCount == 0)
        {
            return new AssignmentResolution(
                _runtimeConfig.DefaultSpeciesId,
                ResolveTrackedSpeciesDisplayName(
                    _runtimeConfig.DefaultSpeciesId,
                    requestedSpeciesDisplayName,
                    _runtimeConfig.DefaultSpeciesDisplayName),
                DecisionReason: "lineage_unavailable_default",
                Strategy: "default",
                StrategyDetail: "No parent membership evidence available in current epoch.",
                ForceDecisionReason: true);
        }

        if (string.IsNullOrWhiteSpace(lineageEvidence.DominantSpeciesId) && !bestParentSpeciesFit.HasValue)
        {
            return new AssignmentResolution(
                _runtimeConfig.DefaultSpeciesId,
                ResolveTrackedSpeciesDisplayName(
                    _runtimeConfig.DefaultSpeciesId,
                    fallbackDisplayName: _runtimeConfig.DefaultSpeciesDisplayName),
                DecisionReason: "lineage_source_unavailable_default",
                Strategy: "default",
                StrategyDetail: "Parent memberships did not resolve to a reusable source species.",
                ForceDecisionReason: true);
        }

        var sourceSpeciesId = bestParentSpeciesFit.HasValue
            ? bestParentSpeciesFit.Value.SpeciesId
            : (lineageEvidence.DominantShare >= 0.999999d
                ? lineageEvidence.DominantSpeciesId
                : null);
        if (string.IsNullOrWhiteSpace(sourceSpeciesId))
        {
            if (!string.IsNullOrWhiteSpace(lineageEvidence.HysteresisSpeciesId))
            {
                return new AssignmentResolution(
                    lineageEvidence.HysteresisSpeciesId!,
                    ResolveTrackedSpeciesDisplayName(
                        lineageEvidence.HysteresisSpeciesId!,
                        lineageEvidence.HysteresisSpeciesDisplayName),
                    DecisionReason: "lineage_hysteresis_low_consensus",
                    Strategy: "hysteresis_hold",
                    StrategyDetail: "Parent memberships did not resolve to a reusable source species; reused prior lineage species.",
                    ForceDecisionReason: true);
            }

            return new AssignmentResolution(
                _runtimeConfig.DefaultSpeciesId,
                ResolveTrackedSpeciesDisplayName(
                    _runtimeConfig.DefaultSpeciesId,
                    fallbackDisplayName: _runtimeConfig.DefaultSpeciesDisplayName),
                DecisionReason: "lineage_source_unavailable_default",
                Strategy: "default",
                StrategyDetail: "Parent memberships did not resolve to a reusable source species.",
                ForceDecisionReason: true);
        }

        sourceSpeciesId = sourceSpeciesId.Trim();
        var sourceSpeciesDisplayName = ResolveTrackedSpeciesDisplayName(
            sourceSpeciesId,
            bestParentSpeciesFit.HasValue
                ? bestParentSpeciesFit.Value.SpeciesDisplayName
                : lineageEvidence.DominantSpeciesDisplayName);
        var sourceConsensusShare = bestParentSpeciesFit.HasValue && lineageEvidence.ParentMembershipCount > 0
            ? (double)bestParentSpeciesFit.Value.SupportingParentCount / lineageEvidence.ParentMembershipCount
            : 1d;

        if (!sourceSpeciesSimilarityScore.HasValue)
        {
            return new AssignmentResolution(
                sourceSpeciesId,
                sourceSpeciesDisplayName,
                DecisionReason: "lineage_inherit_no_similarity",
                Strategy: "lineage_inherit",
                StrategyDetail: "No source-species similarity score supplied; inherited the resolved source species.",
                ForceDecisionReason: true,
                SourceSpeciesId: sourceSpeciesId,
                SourceSpeciesDisplayName: sourceSpeciesDisplayName,
                SourceConsensusShare: sourceConsensusShare);
        }

        var similarity = ClampScore(sourceSpeciesSimilarityScore.Value);
        var sourceThresholdState = ResolveSplitThresholdState(sourceSpeciesFloor);
        var policyEffectiveSplitThreshold = sourceThresholdState.PolicyEffectiveSplitThreshold;
        var effectiveSplitThreshold = sourceThresholdState.DynamicSplitThreshold;
        var speciesFloorSimilarityScore = sourceThresholdState.SpeciesFloorSimilarityScore;
        var speciesFloorSampleCount = sourceThresholdState.SpeciesFloorSampleCount;
        var speciesFloorMembershipCount = sourceThresholdState.SpeciesFloorMembershipCount;

        var splitTriggeredBySpeciesFloor =
            sourceThresholdState.UsesSpeciesFloor
            && similarity <= effectiveSplitThreshold
            && similarity > policyEffectiveSplitThreshold;

        AssignmentResolution BuildSimilarityResolution(
            string SpeciesId,
            string SpeciesDisplayName,
            string DecisionReason,
            string Strategy,
            string StrategyDetail,
            bool ForceDecisionReason,
            RecentDerivedSpeciesHint? recentDerivedHint = null)
        {
            return new AssignmentResolution(
                SpeciesId,
                SpeciesDisplayName,
                DecisionReason,
                Strategy,
                StrategyDetail,
                ForceDecisionReason,
                PolicyEffectiveSplitThreshold: policyEffectiveSplitThreshold,
                EffectiveSplitThreshold: effectiveSplitThreshold,
                SplitTriggeredBySpeciesFloor: splitTriggeredBySpeciesFloor,
                SourceSpeciesId: sourceSpeciesId,
                SourceSpeciesDisplayName: sourceSpeciesDisplayName,
                SourceSpeciesSimilarityScore: similarity,
                SourceConsensusShare: sourceConsensusShare,
                SpeciesFloorSimilarityScore: speciesFloorSimilarityScore,
                SpeciesFloorSampleCount: speciesFloorSampleCount,
                SpeciesFloorMembershipCount: speciesFloorMembershipCount,
                RecentDerivedSourceSpeciesId: recentDerivedHint?.SourceSpeciesId,
                RecentDerivedSourceSpeciesDisplayName: recentDerivedHint?.SourceSpeciesDisplayName,
                RecentDerivedFounderSimilarityScore: recentDerivedHint?.FounderSimilarityScore);
        }

        var hasSplitParentEvidence = lineageEvidence.ParentMembershipCount >= _assignmentPolicy.MinParentMembershipsBeforeSplit;
        var hasInSpeciesEvidence = sourceSpeciesFloor.HasValue
            && sourceSpeciesFloor.Value.SimilaritySampleCount > 0
            && sourceSpeciesFloor.Value.MinSimilarityScore.HasValue;
        var isRecentDerivedSourceSpecies = TryGetRecentDerivedSpeciesHintByTargetSpecies(
            sourceSpeciesId,
            out var recentDerivedSourceHint);
        var withinRecentSplitRealignWindow =
            _assignmentPolicy.RecentSplitRealignParentMembershipWindow > 0
            && lineageEvidence.ParentMembershipCount > 0
            && lineageEvidence.ParentMembershipCount <= _assignmentPolicy.RecentSplitRealignParentMembershipWindow
            && !string.IsNullOrWhiteSpace(lineageEvidence.HysteresisSpeciesId)
            && string.Equals(
                lineageEvidence.HysteresisDecisionReason,
                "lineage_diverged_new_species",
                StringComparison.Ordinal);

        if (allowRecentSplitRealign
            && withinRecentSplitRealignWindow
            && similarity <= Math.Min(
                1d,
                _assignmentPolicy.LineageMatchThreshold + _assignmentPolicy.RecentSplitRealignMatchMargin))
        {
            return BuildSimilarityResolution(
                lineageEvidence.HysteresisSpeciesId!,
                ResolveTrackedSpeciesDisplayName(
                    lineageEvidence.HysteresisSpeciesId!,
                    lineageEvidence.HysteresisSpeciesDisplayName),
                DecisionReason: "lineage_realign_recent_split",
                Strategy: "lineage_realign",
                StrategyDetail: "Recent derived split hint reused within bounded parent-membership realignment window.",
                ForceDecisionReason: true);
        }

        if (similarity >= _assignmentPolicy.LineageMatchThreshold)
        {
            return BuildSimilarityResolution(
                sourceSpeciesId,
                sourceSpeciesDisplayName,
                DecisionReason: "lineage_inherit_similarity_match",
                Strategy: "lineage_inherit",
                StrategyDetail: "Similarity met lineage match threshold.",
                ForceDecisionReason: true);
        }

        if (allowRecentDerivedSpeciesReuse
            && TryResolveRecentDerivedSpeciesReuse(sourceSpeciesId, similarity, out var recentDerivedHint))
        {
            return BuildSimilarityResolution(
                recentDerivedHint.TargetSpeciesId,
                ResolveTrackedSpeciesDisplayName(
                    recentDerivedHint.TargetSpeciesId,
                    recentDerivedHint.TargetSpeciesDisplayName),
                DecisionReason: "lineage_reuse_recent_derived_species",
                Strategy: "lineage_recent_derived_reuse",
                StrategyDetail: "Recent derived species from the same source lineage reused within bounded founder-similarity band.",
                ForceDecisionReason: true,
                recentDerivedHint: recentDerivedHint);
        }

        if (similarity <= effectiveSplitThreshold)
        {
            if (isSeedFounderCandidate
                && lineageEvidence.ParentMembershipCount == 1)
            {
                var founderRootNamingPlan = BuildFounderRootSpeciesNamingPlan(
                    sourceSpeciesId,
                    sourceSpeciesDisplayName);
                var founderRootSpeciesId = BuildFounderRootSpeciesId(
                    sourceSpeciesId,
                    lineageEvidence.LineageKey,
                    epoch.EpochId);
                return new AssignmentResolution(
                    founderRootSpeciesId,
                    founderRootNamingPlan.FounderSpeciesDisplayName,
                    DecisionReason: "lineage_diverged_founder_root_species",
                    Strategy: "lineage_founder_root",
                    StrategyDetail:
                        "Seed founder diverged before any reusable parent lineage existed; created an independent root species.",
                    ForceDecisionReason: true,
                    PolicyEffectiveSplitThreshold: policyEffectiveSplitThreshold,
                    EffectiveSplitThreshold: effectiveSplitThreshold,
                    SplitTriggeredBySpeciesFloor: splitTriggeredBySpeciesFloor,
                    SourceSpeciesSimilarityScore: similarity,
                    SourceConsensusShare: sourceConsensusShare,
                    SpeciesFloorSimilarityScore: speciesFloorSimilarityScore,
                    SpeciesFloorSampleCount: speciesFloorSampleCount,
                    SpeciesFloorMembershipCount: speciesFloorMembershipCount,
                    DisplayNameRewriteSpeciesId: founderRootNamingPlan.SourceSpeciesIdToRewrite,
                    DisplayNameRewriteSpeciesDisplayName: founderRootNamingPlan.SourceSpeciesDisplayNameRewrite);
            }

            if (isRecentDerivedSourceSpecies && !hasInSpeciesEvidence)
            {
                return BuildSimilarityResolution(
                    sourceSpeciesId,
                    sourceSpeciesDisplayName,
                    DecisionReason: "lineage_split_guarded_newborn_intraspecies_evidence",
                    Strategy: "lineage_inherit",
                    StrategyDetail: "Split threshold crossed but the newborn derived species has not yet recorded in-species similarity evidence.",
                    ForceDecisionReason: true,
                    recentDerivedHint: recentDerivedSourceHint);
            }

            if (!hasSplitParentEvidence)
            {
                if (!string.IsNullOrWhiteSpace(lineageEvidence.HysteresisSpeciesId))
                {
                    return BuildSimilarityResolution(
                        lineageEvidence.HysteresisSpeciesId!,
                        ResolveTrackedSpeciesDisplayName(
                            lineageEvidence.HysteresisSpeciesId!,
                            lineageEvidence.HysteresisSpeciesDisplayName),
                        DecisionReason: "lineage_split_guarded_hysteresis_hold",
                        Strategy: "hysteresis_hold",
                        StrategyDetail: "Split threshold crossed but minimum parent-membership evidence not met; reused prior lineage species.",
                        ForceDecisionReason: true);
                }

                return BuildSimilarityResolution(
                    sourceSpeciesId,
                    sourceSpeciesDisplayName,
                    DecisionReason: "lineage_split_guarded_parent_evidence",
                    Strategy: "lineage_inherit",
                    StrategyDetail: "Split threshold crossed but minimum parent-membership evidence not met.",
                    ForceDecisionReason: true);
            }

            if (_assignmentPolicy.CreateDerivedSpeciesOnDivergence)
            {
                var derivedSpeciesId = BuildDerivedSpeciesId(
                    sourceSpeciesId,
                    _assignmentPolicy.DerivedSpeciesPrefix,
                    lineageEvidence.LineageKey,
                    epoch.EpochId);
                return BuildSimilarityResolution(
                    derivedSpeciesId,
                    BuildDerivedSpeciesDisplayName(
                        sourceSpeciesDisplayName,
                        sourceSpeciesId,
                        derivedSpeciesId),
                    DecisionReason: "lineage_diverged_new_species",
                    Strategy: "lineage_diverged",
                    StrategyDetail: splitTriggeredBySpeciesFloor
                        ? "Similarity below dynamic species floor threshold; created deterministic derived species with lineage suffix."
                        : "Similarity below split threshold; created deterministic derived species with lineage suffix.",
                    ForceDecisionReason: true);
            }

            return BuildSimilarityResolution(
                _runtimeConfig.DefaultSpeciesId,
                ResolveTrackedSpeciesDisplayName(
                    _runtimeConfig.DefaultSpeciesId,
                    fallbackDisplayName: _runtimeConfig.DefaultSpeciesDisplayName),
                DecisionReason: "lineage_diverged_default",
                Strategy: "default",
                StrategyDetail: splitTriggeredBySpeciesFloor
                    ? "Similarity below dynamic species floor threshold with derived species disabled."
                    : "Similarity below split threshold with derived species disabled.",
                ForceDecisionReason: true);
        }

        if (_assignmentPolicy.LineageSplitGuardMargin > 0d
            && similarity <= _assignmentPolicy.LineageSplitThreshold)
        {
            if (!string.IsNullOrWhiteSpace(lineageEvidence.HysteresisSpeciesId))
            {
                return BuildSimilarityResolution(
                    lineageEvidence.HysteresisSpeciesId!,
                    ResolveSpeciesDisplayName(lineageEvidence.HysteresisSpeciesDisplayName, lineageEvidence.HysteresisSpeciesId!),
                    DecisionReason: "lineage_split_guard_band_hysteresis_hold",
                    Strategy: "hysteresis_hold",
                    StrategyDetail: "Similarity in split-guard band; reused prior lineage species to reduce fragmentation.",
                    ForceDecisionReason: true);
            }

            return BuildSimilarityResolution(
                sourceSpeciesId,
                sourceSpeciesDisplayName,
                DecisionReason: "lineage_split_guard_band_inherit",
                Strategy: "lineage_inherit",
                StrategyDetail: "Similarity in split-guard band; inherited the resolved source lineage species.",
                ForceDecisionReason: true);
        }

        if (!string.IsNullOrWhiteSpace(lineageEvidence.HysteresisSpeciesId))
        {
            return BuildSimilarityResolution(
                lineageEvidence.HysteresisSpeciesId!,
                ResolveSpeciesDisplayName(lineageEvidence.HysteresisSpeciesDisplayName, lineageEvidence.HysteresisSpeciesId!),
                DecisionReason: "lineage_hysteresis_hold",
                Strategy: "hysteresis_hold",
                StrategyDetail: "Similarity in hysteresis band; reused prior lineage species.",
                ForceDecisionReason: true);
        }

        return BuildSimilarityResolution(
            sourceSpeciesId,
            sourceSpeciesDisplayName,
            DecisionReason: "lineage_hysteresis_seed",
            Strategy: "lineage_inherit",
            StrategyDetail: "Similarity in hysteresis band without prior lineage hint; seeded with the resolved source species.",
            ForceDecisionReason: true);
    }
}
