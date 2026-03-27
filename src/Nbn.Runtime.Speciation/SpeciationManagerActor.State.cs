using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Shared;
using Proto;
using ProtoSpec = Nbn.Proto.Speciation;

namespace Nbn.Runtime.Speciation;

public sealed partial class SpeciationManagerActor
{
    private async Task PrimeSpeciesSimilarityFloorsAsync(long epochId)
    {
        ResetSpeciesSimilarityFloors();
        if (epochId <= 0)
        {
            return;
        }

        var memberships = await _store.ListMembershipsAsync(epochId).ConfigureAwait(false);
        foreach (var membership in memberships)
        {
            RecordCommittedMembership(membership);
        }
    }

    private void ResetSpeciesSimilarityFloors()
    {
        _speciesSimilarityFloors.Clear();
        _recentDerivedSpeciesHintsBySourceSpecies.Clear();
        _recentDerivedSpeciesHintsByTargetSpecies.Clear();
        _speciesDisplayNamesBySpeciesId.Clear();
        _rootSpeciesOrdinalsBySpeciesId.Clear();
        _maxRootSpeciesOrdinalByStem.Clear();
    }

    private SpeciesSimilarityFloorState? ResolveSpeciesSimilarityFloor(string? speciesId)
    {
        if (string.IsNullOrWhiteSpace(speciesId))
        {
            return null;
        }

        var normalizedSpeciesId = speciesId.Trim();
        return _speciesSimilarityFloors.TryGetValue(normalizedSpeciesId, out var state)
            ? state
            : null;
    }

    private void IncrementSpeciesMembershipCount(string speciesId, int incrementBy)
    {
        if (incrementBy <= 0 || string.IsNullOrWhiteSpace(speciesId))
        {
            return;
        }

        var normalizedSpeciesId = speciesId.Trim();
        _speciesSimilarityFloors.TryGetValue(normalizedSpeciesId, out var existing);
        _speciesSimilarityFloors[normalizedSpeciesId] = existing with
        {
            MembershipCount = Math.Max(0, existing.MembershipCount) + incrementBy
        };
    }

    private SplitThresholdState ResolveSplitThresholdState(SpeciesSimilarityFloorState? speciesFloor)
    {
        var policyEffectiveSplitThreshold = Math.Max(
            0d,
            _assignmentPolicy.LineageSplitThreshold - _assignmentPolicy.LineageSplitGuardMargin);
        var dynamicSplitThreshold = policyEffectiveSplitThreshold;
        double? speciesFloorSimilarityScore = null;
        var speciesFloorSampleCount = 0;
        var speciesFloorMembershipCount = 0;

        if (speciesFloor.HasValue
            && speciesFloor.Value.SimilaritySampleCount > 0
            && speciesFloor.Value.MinSimilarityScore.HasValue)
        {
            speciesFloorSimilarityScore = ClampScore(speciesFloor.Value.MinSimilarityScore.Value);
            speciesFloorSampleCount = Math.Max(0, speciesFloor.Value.SimilaritySampleCount);
            speciesFloorMembershipCount = Math.Max(0, speciesFloor.Value.MembershipCount);
            var relaxedSpeciesFloorThreshold = Math.Max(
                0d,
                speciesFloorSimilarityScore.Value - _assignmentPolicy.HysteresisMargin);
            dynamicSplitThreshold = Math.Max(dynamicSplitThreshold, relaxedSpeciesFloorThreshold);
        }

        return new SplitThresholdState(
            PolicyEffectiveSplitThreshold: policyEffectiveSplitThreshold,
            DynamicSplitThreshold: dynamicSplitThreshold,
            UsesSpeciesFloor: dynamicSplitThreshold > policyEffectiveSplitThreshold,
            SpeciesFloorSimilarityScore: speciesFloorSimilarityScore,
            SpeciesFloorSampleCount: speciesFloorSampleCount,
            SpeciesFloorMembershipCount: speciesFloorMembershipCount);
    }

    private static double? ResolveSourceSpeciesSimilarityScore(
        string? sourceSpeciesId,
        IReadOnlyList<SpeciationMembershipRecord> parentMemberships,
        SimilarityEvidence similarityEvidence,
        IReadOnlyList<Guid> inputOrderedParentBrainIds)
    {
        if (!string.IsNullOrWhiteSpace(sourceSpeciesId)
            && TryResolveSpeciesPairwiseSimilarityEstimate(
                sourceSpeciesId.Trim(),
                parentMemberships,
                similarityEvidence,
                inputOrderedParentBrainIds,
                out var sourcePairwiseSimilarity))
        {
            return sourcePairwiseSimilarity;
        }

        if (similarityEvidence.DominantSpeciesSimilarityScore.HasValue)
        {
            return ClampScore(similarityEvidence.DominantSpeciesSimilarityScore.Value);
        }

        return similarityEvidence.SimilarityScore.HasValue
            ? ClampScore(similarityEvidence.SimilarityScore.Value)
            : null;
    }

    private static double? ResolveAssignedSpeciesSimilarityScore(
        AssignmentResolution assignmentResolution,
        IReadOnlyList<SpeciationMembershipRecord> parentMemberships,
        SimilarityEvidence similarityEvidence,
        IReadOnlyList<Guid> inputOrderedParentBrainIds,
        double? sourceSpeciesSimilarityScore,
        double? actualAssignedSpeciesSimilarityScore,
        double? intraSpeciesSimilaritySample,
        bool allowSourceSimilarityCarryover)
    {
        if (string.IsNullOrWhiteSpace(assignmentResolution.SpeciesId)
            || string.Equals(assignmentResolution.Strategy, "explicit_species", StringComparison.Ordinal))
        {
            return null;
        }

        if (actualAssignedSpeciesSimilarityScore.HasValue)
        {
            return ClampScore(actualAssignedSpeciesSimilarityScore.Value);
        }

        if (intraSpeciesSimilaritySample.HasValue)
        {
            return ClampScore(intraSpeciesSimilaritySample.Value);
        }

        if (TryResolveSpeciesPairwiseSimilarityEstimate(
                assignmentResolution.SpeciesId,
                parentMemberships,
                similarityEvidence,
                inputOrderedParentBrainIds,
                out var pairwiseSimilarity))
        {
            return pairwiseSimilarity;
        }

        if (allowSourceSimilarityCarryover
            && ShouldCarrySourceSimilarityIntoAssignedSpecies(
                assignmentResolution,
                sourceSpeciesSimilarityScore))
        {
            return ClampScore(sourceSpeciesSimilarityScore!.Value);
        }

        return sourceSpeciesSimilarityScore.HasValue
            && !string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
            && string.Equals(
                assignmentResolution.SpeciesId,
                assignmentResolution.SourceSpeciesId,
                StringComparison.Ordinal)
            ? ClampScore(sourceSpeciesSimilarityScore.Value)
            : null;
    }

    private static bool ShouldCarrySourceSimilarityIntoAssignedSpecies(
        AssignmentResolution assignmentResolution,
        double? sourceSpeciesSimilarityScore)
    {
        return sourceSpeciesSimilarityScore.HasValue
            && !string.IsNullOrWhiteSpace(assignmentResolution.SpeciesId)
            && !string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
            && !string.Equals(
                assignmentResolution.SpeciesId,
                assignmentResolution.SourceSpeciesId,
                StringComparison.Ordinal)
            && !string.Equals(
                assignmentResolution.DecisionReason,
                "lineage_diverged_new_species",
                StringComparison.Ordinal)
            && !string.Equals(
                assignmentResolution.Strategy,
                "explicit_species",
                StringComparison.Ordinal);
    }

    private void RecordCommittedMembership(
        SpeciationMembershipRecord membership,
        double? similaritySample,
        bool actualSimilaritySample)
    {
        if (membership is null)
        {
            return;
        }

        RecordSpeciesDisplayName(membership.SpeciesId, membership.SpeciesDisplayName);
        UpdateSpeciesSimilarityFloor(
            membership.SpeciesId,
            NormalizeSpeciesFloorSimilaritySample(
                membership,
                similaritySample,
                actualSimilaritySample),
            actualSimilaritySample);
        UpdateRecentDerivedSpeciesHints(membership);
    }

    private void RecordCommittedMembership(SpeciationMembershipRecord membership)
    {
        if (membership is null)
        {
            return;
        }

        RecordSpeciesDisplayName(membership.SpeciesId, membership.SpeciesDisplayName);
        var actualSimilaritySample = HasActualAssignedSpeciesSimilaritySample(membership);
        var similaritySample = NormalizeSpeciesFloorSimilaritySample(
            membership,
            TryExtractIntraSpeciesSimilaritySample(membership),
            actualSimilaritySample);
        UpdateSpeciesSimilarityFloor(
            membership.SpeciesId,
            similaritySample,
            actualSimilaritySample);
        UpdateRecentDerivedSpeciesHints(membership);
    }

    private void RecordSpeciesDisplayName(string? speciesId, string? speciesDisplayName)
    {
        if (string.IsNullOrWhiteSpace(speciesId) || string.IsNullOrWhiteSpace(speciesDisplayName))
        {
            return;
        }

        var normalizedSpeciesId = speciesId.Trim();
        var normalizedDisplayName = speciesDisplayName.Trim();
        _speciesDisplayNamesBySpeciesId[normalizedSpeciesId] = normalizedDisplayName;

        var (stem, lineageCode) = ParseLineageDisplayName(normalizedDisplayName);
        if (lineageCode.Length == 0
            && TryParseNumberedRootSpeciesDisplayName(stem, out var rootStem, out var rootOrdinal))
        {
            _rootSpeciesOrdinalsBySpeciesId[normalizedSpeciesId] = rootOrdinal;
            _maxRootSpeciesOrdinalByStem[rootStem] = _maxRootSpeciesOrdinalByStem.TryGetValue(rootStem, out var existingOrdinal)
                ? Math.Max(existingOrdinal, rootOrdinal)
                : rootOrdinal;
            return;
        }

        _rootSpeciesOrdinalsBySpeciesId.Remove(normalizedSpeciesId);
    }

    private string ResolveTrackedSpeciesDisplayName(
        string speciesId,
        string? preferredDisplayName = null,
        string? fallbackDisplayName = null)
    {
        if (!string.IsNullOrWhiteSpace(preferredDisplayName))
        {
            return preferredDisplayName.Trim();
        }

        var normalizedSpeciesId = speciesId?.Trim() ?? string.Empty;
        if (normalizedSpeciesId.Length > 0
            && _speciesDisplayNamesBySpeciesId.TryGetValue(normalizedSpeciesId, out var trackedDisplayName)
            && !string.IsNullOrWhiteSpace(trackedDisplayName))
        {
            return trackedDisplayName;
        }

        if (!string.IsNullOrWhiteSpace(fallbackDisplayName))
        {
            return fallbackDisplayName.Trim();
        }

        return BuildDisplayNameFromSpeciesId(normalizedSpeciesId);
    }

    private FounderRootSpeciesNamingPlan BuildFounderRootSpeciesNamingPlan(
        string sourceSpeciesId,
        string? sourceSpeciesDisplayName)
    {
        var resolvedSourceDisplayName = ResolveTrackedSpeciesDisplayName(
            sourceSpeciesId,
            sourceSpeciesDisplayName);
        var (stem, _) = ParseLineageDisplayName(resolvedSourceDisplayName);
        var rootStem = stem.Trim().Length == 0 ? "Species" : stem.Trim();
        var sourceRootOrdinal = 0;
        if (TryParseNumberedRootSpeciesDisplayName(rootStem, out var parsedStem, out var parsedOrdinal))
        {
            rootStem = parsedStem;
            sourceRootOrdinal = parsedOrdinal;
        }
        else if (_rootSpeciesOrdinalsBySpeciesId.TryGetValue(sourceSpeciesId.Trim(), out var trackedOrdinal)
                 && trackedOrdinal > 0)
        {
            sourceRootOrdinal = trackedOrdinal;
        }

        var maxKnownOrdinal = _maxRootSpeciesOrdinalByStem.TryGetValue(rootStem, out var existingMaxOrdinal)
            ? existingMaxOrdinal
            : 0;
        string? sourceDisplayNameRewrite = null;
        if (sourceRootOrdinal <= 0)
        {
            sourceRootOrdinal = 1;
            sourceDisplayNameRewrite = BuildNumberedRootSpeciesDisplayName(rootStem, sourceRootOrdinal);
        }

        var founderRootOrdinal = Math.Max(sourceRootOrdinal, maxKnownOrdinal) + 1;
        return new FounderRootSpeciesNamingPlan(
            BuildNumberedRootSpeciesDisplayName(rootStem, founderRootOrdinal),
            sourceDisplayNameRewrite,
            sourceDisplayNameRewrite is null ? null : sourceSpeciesId.Trim());
    }

    private static double? NormalizeSpeciesFloorSimilaritySample(
        SpeciationMembershipRecord membership,
        double? similaritySample,
        bool actualSimilaritySample)
    {
        if (membership is null || !similaritySample.HasValue)
        {
            return null;
        }

        return IsSyntheticFounderSpeciesCreationDecision(membership.DecisionReason)
            && !actualSimilaritySample
            ? null
            : ClampScore(similaritySample.Value);
    }

    private static bool IsSyntheticFounderSpeciesCreationDecision(string? decisionReason)
    {
        return string.Equals(
                   decisionReason,
                   "lineage_diverged_new_species",
                   StringComparison.Ordinal)
               || string.Equals(
                   decisionReason,
                   "lineage_diverged_founder_root_species",
                   StringComparison.Ordinal);
    }

    private void UpdateRecentDerivedSpeciesHints(SpeciationMembershipRecord membership)
    {
        if (!TryExtractRecentDerivedSpeciesHint(membership, out var hint))
        {
            return;
        }

        StoreRecentDerivedSpeciesHint(hint);
    }

    private void StoreRecentDerivedSpeciesHint(RecentDerivedSpeciesHint hint)
    {
        if (!_recentDerivedSpeciesHintsBySourceSpecies.TryGetValue(hint.SourceSpeciesId, out var hints))
        {
            hints = new List<RecentDerivedSpeciesHint>();
            _recentDerivedSpeciesHintsBySourceSpecies[hint.SourceSpeciesId] = hints;
        }

        hints.RemoveAll(existing =>
            string.Equals(existing.TargetSpeciesId, hint.TargetSpeciesId, StringComparison.Ordinal));
        hints.Add(hint);
        _recentDerivedSpeciesHintsByTargetSpecies[hint.TargetSpeciesId] = hint;

        hints.Sort(static (left, right) =>
        {
            var assignedOrder = right.AssignedMs.CompareTo(left.AssignedMs);
            return assignedOrder != 0
                ? assignedOrder
                : string.Compare(left.TargetSpeciesId, right.TargetSpeciesId, StringComparison.Ordinal);
        });

        var hintLimit = Math.Max(
            1,
            Math.Max(
                _assignmentPolicy.HindsightReassignCommitWindow,
                _assignmentPolicy.RecentSplitRealignParentMembershipWindow));
        if (hints.Count > hintLimit)
        {
            hints.RemoveRange(hintLimit, hints.Count - hintLimit);
        }
    }

    private async Task<RecentDerivedSpeciesHint?> TryResolveDerivedSpeciesHintByTargetSpeciesAsync(
        long epochId,
        string? speciesId,
        CancellationToken cancellationToken = default)
    {
        if (TryGetRecentDerivedSpeciesHintByTargetSpecies(speciesId, out var hint))
        {
            return hint;
        }

        if (epochId <= 0 || string.IsNullOrWhiteSpace(speciesId))
        {
            return null;
        }

        var earliestMemberships = await _store.ListEarliestMembershipsForSpeciesAsync(
            epochId,
            speciesId.Trim(),
            limit: 1,
            cancellationToken).ConfigureAwait(false);
        if (earliestMemberships.Count == 0
            || !TryExtractRecentDerivedSpeciesHint(earliestMemberships[0], out hint))
        {
            return null;
        }

        StoreRecentDerivedSpeciesHint(hint);
        return hint;
    }

    private void UpdateSpeciesSimilarityFloor(
        string speciesId,
        double? similaritySample,
        bool actualSimilaritySample = false)
    {
        if (string.IsNullOrWhiteSpace(speciesId))
        {
            return;
        }

        var normalizedSpeciesId = speciesId.Trim();
        _speciesSimilarityFloors.TryGetValue(normalizedSpeciesId, out var existing);

        var membershipCount = Math.Max(0, existing.MembershipCount) + 1;
        var sampleCount = Math.Max(0, existing.SimilaritySampleCount);
        var actualSampleCount = Math.Max(0, existing.ActualSimilaritySampleCount);
        var minSimilarity = existing.MinSimilarityScore;

        if (similaritySample.HasValue)
        {
            var normalizedSample = ClampScore(similaritySample.Value);
            sampleCount++;
            if (actualSimilaritySample)
            {
                actualSampleCount++;
            }

            minSimilarity = minSimilarity.HasValue
                ? Math.Min(minSimilarity.Value, normalizedSample)
                : normalizedSample;
        }

        _speciesSimilarityFloors[normalizedSpeciesId] = new SpeciesSimilarityFloorState(
            MembershipCount: membershipCount,
            SimilaritySampleCount: sampleCount,
            ActualSimilaritySampleCount: actualSampleCount,
            MinSimilarityScore: minSimilarity);
    }

    private static bool TryResolveIntraSpeciesSimilaritySample(
        AssignmentResolution assignmentResolution,
        IReadOnlyList<SpeciationMembershipRecord> parentMemberships,
        SimilarityEvidence similarityEvidence,
        double? sourceSpeciesSimilarityScore,
        double? actualAssignedSpeciesSimilarityScore,
        IReadOnlyList<Guid> inputOrderedParentBrainIds,
        bool allowSourceSimilarityCarryover,
        out double similaritySample)
    {
        similaritySample = 0d;
        if (string.IsNullOrWhiteSpace(assignmentResolution.SpeciesId)
            || string.Equals(assignmentResolution.Strategy, "explicit_species", StringComparison.Ordinal))
        {
            return false;
        }

        if (actualAssignedSpeciesSimilarityScore.HasValue)
        {
            similaritySample = ClampScore(actualAssignedSpeciesSimilarityScore.Value);
            return true;
        }

        if (string.Equals(
                assignmentResolution.DecisionReason,
                "lineage_diverged_new_species",
                StringComparison.Ordinal))
        {
            similaritySample = 1d;
            return true;
        }

        if (TryResolveSpeciesPairwiseSimilarityEstimate(
                assignmentResolution.SpeciesId,
                parentMemberships,
                similarityEvidence,
                inputOrderedParentBrainIds,
                out var pairwiseSimilarity))
        {
            similaritySample = pairwiseSimilarity;
            return true;
        }

        if (sourceSpeciesSimilarityScore.HasValue
            && !string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
            && assignmentResolution.SourceConsensusShare >= 0.999999d
            && string.Equals(
                assignmentResolution.SpeciesId,
                assignmentResolution.SourceSpeciesId,
                StringComparison.Ordinal))
        {
            similaritySample = ClampScore(sourceSpeciesSimilarityScore.Value);
            return true;
        }

        if (allowSourceSimilarityCarryover
            && ShouldCarrySourceSimilarityIntoAssignedSpecies(
                assignmentResolution,
                sourceSpeciesSimilarityScore))
        {
            similaritySample = ClampScore(sourceSpeciesSimilarityScore!.Value);
            return true;
        }

        return false;
    }

    private static bool TryResolveBestParentSpeciesPairwiseFit(
        IReadOnlyList<SpeciationMembershipRecord> parentMemberships,
        SimilarityEvidence similarityEvidence,
        IReadOnlyList<Guid> inputOrderedParentBrainIds,
        out ParentSpeciesPairwiseFit bestFit)
    {
        bestFit = default;
        if (parentMemberships.Count == 0 || inputOrderedParentBrainIds.Count == 0)
        {
            return false;
        }

        var fits = parentMemberships
            .Where(parent => !string.IsNullOrWhiteSpace(parent.SpeciesId))
            .GroupBy(parent => parent.SpeciesId.Trim(), StringComparer.Ordinal)
            .Select(group =>
            {
                if (!TryResolveSpeciesPairwiseSimilarityEstimate(
                        group.Key,
                        parentMemberships,
                        similarityEvidence,
                        inputOrderedParentBrainIds,
                        out var pairwiseSimilarity))
                {
                    return (ParentSpeciesPairwiseFit?)null;
                }

                var preferred = group
                    .OrderByDescending(item => item.AssignedMs)
                    .ThenBy(item => item.BrainId)
                    .First();
                return new ParentSpeciesPairwiseFit(
                    group.Key,
                    ResolveSpeciesDisplayName(preferred.SpeciesDisplayName, group.Key),
                    pairwiseSimilarity,
                    group.Count(),
                    preferred.AssignedMs);
            })
            .Where(item => item.HasValue)
            .Select(item => item!.Value)
            .OrderByDescending(item => item.PairwiseSimilarity)
            .ThenByDescending(item => item.SupportingParentCount)
            .ThenByDescending(item => item.LatestAssignedMs)
            .ThenBy(item => item.SpeciesId, StringComparer.Ordinal)
            .ToArray();

        if (fits.Length == 0)
        {
            return false;
        }

        bestFit = fits[0];
        return true;
    }

    private static bool TryResolveSpeciesPairwiseSimilarityEstimate(
        string speciesId,
        IReadOnlyList<SpeciationMembershipRecord> parentMemberships,
        SimilarityEvidence similarityEvidence,
        IReadOnlyList<Guid> inputOrderedParentBrainIds,
        out double pairwiseSimilarity)
    {
        pairwiseSimilarity = 0d;
        if (string.IsNullOrWhiteSpace(speciesId) || inputOrderedParentBrainIds.Count == 0)
        {
            return false;
        }

        var normalizedSpeciesId = speciesId.Trim();
        var parentSpeciesByBrainId = parentMemberships
            .Where(parent => !string.IsNullOrWhiteSpace(parent.SpeciesId))
            .ToDictionary(
                static parent => parent.BrainId,
                static parent => parent.SpeciesId.Trim());

        var pairwiseScores = new List<double>(Math.Min(2, inputOrderedParentBrainIds.Count));
        for (var index = 0; index < inputOrderedParentBrainIds.Count; index++)
        {
            var parentBrainId = inputOrderedParentBrainIds[index];
            if (!parentSpeciesByBrainId.TryGetValue(parentBrainId, out var parentSpeciesId)
                || !string.Equals(parentSpeciesId, normalizedSpeciesId, StringComparison.Ordinal)
                || !TryResolveParentSimilarityAtIndex(index, similarityEvidence, out var score))
            {
                continue;
            }

            pairwiseScores.Add(score);
        }

        if (pairwiseScores.Count == 0)
        {
            return false;
        }

        pairwiseSimilarity = ClampScore(pairwiseScores.Min());
        return true;
    }
}
