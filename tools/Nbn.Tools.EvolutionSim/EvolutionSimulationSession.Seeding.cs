using Nbn.Proto;
using Nbn.Shared;

namespace Nbn.Tools.EvolutionSim;

public sealed partial class EvolutionSimulationSession
{
    private async Task SeedInitialParentsAsync(CancellationToken cancellationToken)
    {
        if (!_options.CommitToSpeciation)
        {
            return;
        }

        List<EvolutionParentRef> snapshot;
        lock (_gate)
        {
            if (_parentPool.Count == 0)
            {
                return;
            }

            snapshot = _parentPool.ToList();
        }

        var seededKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var seededParents = new List<EvolutionParentRef>(snapshot.Count);
        foreach (var parent in snapshot)
        {
            if (TryBuildParentKey(parent, out var seedKey)
                && !seededKeys.Add(seedKey))
            {
                continue;
            }

            var (hasSeedPartner, seedPartner, seedAssessment) = await ResolveSeedPartnerAsync(
                snapshot,
                seededParents,
                parent,
                cancellationToken).ConfigureAwait(false);
            if (!hasSeedPartner)
            {
                continue;
            }

            if (!TryBuildSeedCandidate(parent, seedAssessment, out var candidate))
            {
                continue;
            }

            if (!seedAssessment.Success
                && !string.IsNullOrWhiteSpace(seedAssessment.AbortReason))
            {
                SetLastFailure($"seed_parent_assess_failed:{seedAssessment.AbortReason}");
            }

            var commitOutcome = await _client.CommitSpeciationAsync(
                candidate,
                parent,
                seedPartner,
                cancellationToken).ConfigureAwait(false);

            if (commitOutcome.Success
                && !string.IsNullOrWhiteSpace(commitOutcome.SpeciesId)
                && TryBuildParentKey(parent, out var parentKey))
            {
                // Initial seed parents define founder roots for simulator lineage tracking.
                // Preserve the committed species id, but do not fold the founder into an
                // earlier seeded family just because runtime reported a source species.
                RecordParentSpecies(parentKey, commitOutcome.SpeciesId, sourceSpeciesId: string.Empty);
            }

            if (commitOutcome.Success)
            {
                seededParents.Add(parent);
            }

            if (!commitOutcome.Success
                && !commitOutcome.ExpectedNoOp
                && !string.IsNullOrWhiteSpace(commitOutcome.FailureDetail))
            {
                SetLastFailure($"seed_parent_commit_failed:{commitOutcome.FailureDetail}");
            }
        }

        NormalizeInitialSeedSelectionOrdinals(seededKeys);
    }

    private async Task<(bool HasPartner, EvolutionParentRef Partner, CompatibilityAssessment Assessment)> ResolveSeedPartnerAsync(
        IReadOnlyList<EvolutionParentRef> parentPoolSnapshot,
        IReadOnlyList<EvolutionParentRef> seededParents,
        EvolutionParentRef parentRef,
        CancellationToken cancellationToken)
    {
        if (seededParents.Count > 0)
        {
            var hasAssessment = false;
            var bestAssessment = default(CompatibilityAssessment);
            var bestPartner = default(EvolutionParentRef);
            foreach (var seededParent in seededParents)
            {
                if (TryBuildParentKey(parentRef, out var parentKey)
                    && TryBuildParentKey(seededParent, out var seededParentKey)
                    && string.Equals(parentKey, seededParentKey, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var assessment = await _client.AssessCompatibilityAsync(
                    parentRef,
                    seededParent,
                    NextSeed(),
                    _options.StrengthSource,
                    cancellationToken).ConfigureAwait(false);
                if (!assessment.Success)
                {
                    continue;
                }

                if (!hasAssessment
                    || assessment.SimilarityScore > bestAssessment.SimilarityScore)
                {
                    hasAssessment = true;
                    bestAssessment = assessment;
                    bestPartner = seededParent;
                }
            }

            if (hasAssessment)
            {
                return (true, bestPartner, bestAssessment);
            }
        }

        return TrySelectSeedPartner(parentPoolSnapshot, parentRef, out var fallbackPartner)
            ? (true, fallbackPartner, default)
            : (false, default, default);
    }

    private async Task ExecuteIterationAsync(EvolutionParentRef parentA, EvolutionParentRef parentB, CancellationToken cancellationToken)
    {
        var assessSeed = NextSeed();
        var assessment = await _client.AssessCompatibilityAsync(
            parentA,
            parentB,
            assessSeed,
            _options.StrengthSource,
            cancellationToken).ConfigureAwait(false);

        IncrementCompatibilityChecks();

        if (!assessment.Success)
        {
            SetLastFailure(string.IsNullOrWhiteSpace(assessment.AbortReason)
                ? "assess_request_failed"
                : assessment.AbortReason);
            return;
        }

        RecordAssessmentSimilarity(assessment.SimilarityScore);

        if (!assessment.Compatible)
        {
            if (!string.IsNullOrWhiteSpace(assessment.AbortReason))
            {
                SetLastFailure(assessment.AbortReason);
            }

            return;
        }

        IncrementCompatiblePairs();

        var adjustedSimilarity = ApplyRunPressureNudge(assessment.SimilarityScore);
        var runCount = _options.RunPolicy.ResolveRunCount(adjustedSimilarity);
        var reproduceSeed = NextSeed();

        IncrementReproductionCalls();
        var reproduction = await _client.ReproduceAsync(
            parentA,
            parentB,
            reproduceSeed,
            runCount,
            _options.SpawnChildren,
            _options.StrengthSource,
            cancellationToken).ConfigureAwait(false);
        RecordReproductionDiagnostics(reproduction.Diagnostics);

        if (!reproduction.Success || !reproduction.Compatible)
        {
            IncrementReproductionFailures();
            if (!string.IsNullOrWhiteSpace(reproduction.AbortReason))
            {
                SetLastFailure(reproduction.AbortReason);
            }

            return;
        }

        if (!_options.CommitToSpeciation || reproduction.CommitCandidates.Count == 0)
        {
            AddChildrenToPool(reproduction);
            return;
        }

        foreach (var candidate in reproduction.CommitCandidates)
        {
            var commitCandidate = candidate;
            if (!commitCandidate.SimilarityScore.HasValue
                && !float.IsNaN(assessment.SimilarityScore)
                && !float.IsInfinity(assessment.SimilarityScore))
            {
                commitCandidate = commitCandidate with
                {
                    SimilarityScore = Math.Clamp(assessment.SimilarityScore, 0f, 1f)
                };
            }

            IncrementSpeciationCommitAttempts();
            var commitOutcome = await _client.CommitSpeciationAsync(
                commitCandidate,
                parentA,
                parentB,
                cancellationToken).ConfigureAwait(false);

            if (commitOutcome.Success)
            {
                IncrementSpeciationCommitSuccesses();
                RecordSpeciationCommitSimilarity(
                    commitOutcome.SourceSpeciesSimilarityScore ?? commitCandidate.SimilarityScore);
                TryAddCommittedCandidateToPool(
                    commitCandidate,
                    commitOutcome.SpeciesId,
                    commitOutcome.SourceSpeciesId);
            }
            else if (!commitOutcome.ExpectedNoOp
                     && !string.IsNullOrWhiteSpace(commitOutcome.FailureDetail))
            {
                SetLastFailure(commitOutcome.FailureDetail);
            }
        }
    }

    private async Task DelayBetweenIterationsAsync(CancellationToken cancellationToken)
    {
        if (_options.Interval <= TimeSpan.Zero)
        {
            return;
        }

        await Task.Delay(_options.Interval, cancellationToken).ConfigureAwait(false);
    }

    private ulong NextSeed()
    {
        lock (_gate)
        {
            _lastSeed = _random.NextUInt64();
            return _lastSeed;
        }
    }
}
