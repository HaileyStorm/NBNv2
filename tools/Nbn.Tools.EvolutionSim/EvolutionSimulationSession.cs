using Nbn.Proto;
using Nbn.Shared;

namespace Nbn.Tools.EvolutionSim;

public sealed class EvolutionSimulationSession
{
    private const ulong CommitSimilarityPlateauWindowSamples = 64;
    private const float MaxRunPressureNudge = 0.35f;
    private const float RunPressureNudgeStep = 0.05f;
    private const float RunPressureRecoveryStep = 0.10f;
    private const double ParentSelectionBiasExponent = 1.8d;

    private readonly EvolutionSimulationOptions _options;
    private readonly IEvolutionSimulationClient _client;
    private readonly object _gate = new();
    private readonly List<EvolutionParentRef> _parentPool;
    private readonly List<ulong> _parentAddedOrdinals;
    private readonly Dictionary<string, string> _parentSpeciesByParentKey;
    private readonly Dictionary<string, string> _parentLineageFamilyByParentKey;
    private readonly Dictionary<string, string> _lineageFamilyBySpeciesId;
    private readonly Dictionary<string, ulong> _speciesFirstSeenOrdinals;
    private readonly Dictionary<string, ulong> _lineageFamilyFirstSeenOrdinals;
    private readonly HashSet<string> _parentPoolKeys;
    private readonly HashSet<string> _protectedParentPoolKeys;
    private readonly DeterministicRandom _random;

    private string _sessionId = "not-started";
    private bool _running;
    private ulong _iterations;
    private ulong _compatibilityChecks;
    private ulong _compatiblePairs;
    private ulong _reproductionCalls;
    private ulong _reproductionFailures;
    private ulong _reproductionRunsObserved;
    private ulong _reproductionRunsWithMutations;
    private ulong _reproductionMutationEvents;
    private ulong _similaritySamples;
    private float _minSimilarityObserved;
    private float _maxSimilarityObserved;
    private ulong _assessmentSimilaritySamples;
    private float _minAssessmentSimilarityObserved;
    private float _maxAssessmentSimilarityObserved;
    private ulong _reproductionSimilaritySamples;
    private float _minReproductionSimilarityObserved;
    private float _maxReproductionSimilarityObserved;
    private ulong _speciationCommitSimilaritySamples;
    private float _minSpeciationCommitSimilarityObserved;
    private float _maxSpeciationCommitSimilarityObserved;
    private ulong _childrenAddedToPool;
    private ulong _speciationCommitAttempts;
    private ulong _speciationCommitSuccesses;
    private ulong _speciationCommitSamplesSinceImprovement;
    private ulong _nextParentOrdinal;
    private ulong _nextSpeciesOrdinal;
    private ulong _nextLineageFamilyOrdinal;
    private float _runPressureNudge;
    private string _lastFailure = string.Empty;
    private ulong _lastSeed;

    public EvolutionSimulationSession(
        EvolutionSimulationOptions options,
        IReadOnlyList<EvolutionParentRef> initialParents,
        IEvolutionSimulationClient client)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _random = new DeterministicRandom(options.Seed);

        if (initialParents is null || initialParents.Count < 2)
        {
            throw new ArgumentException("At least two parent references are required.", nameof(initialParents));
        }

        _parentPool = new List<EvolutionParentRef>(initialParents.Count);
        _parentAddedOrdinals = new List<ulong>(initialParents.Count);
        _parentSpeciesByParentKey = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        _parentLineageFamilyByParentKey = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        _lineageFamilyBySpeciesId = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        _speciesFirstSeenOrdinals = new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase);
        _lineageFamilyFirstSeenOrdinals = new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase);
        _parentPoolKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        _protectedParentPoolKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        _nextParentOrdinal = 1;
        _nextSpeciesOrdinal = 1;
        _nextLineageFamilyOrdinal = 1;
        foreach (var parent in initialParents)
        {
            _parentPool.Add(parent);
            _parentAddedOrdinals.Add(_nextParentOrdinal++);
            if (TryBuildParentKey(parent, out var key))
            {
                _parentPoolKeys.Add(key);
                _protectedParentPoolKeys.Add(key);
            }
        }
    }

    public EvolutionSimulationStatus GetStatus()
    {
        lock (_gate)
        {
            return new EvolutionSimulationStatus(
                _sessionId,
                _running,
                _iterations,
                _parentPool.Count,
                _compatibilityChecks,
                _compatiblePairs,
                _reproductionCalls,
                _reproductionFailures,
                _reproductionRunsObserved,
                _reproductionRunsWithMutations,
                _reproductionMutationEvents,
                _similaritySamples,
                _similaritySamples == 0 ? 0f : _minSimilarityObserved,
                _similaritySamples == 0 ? 0f : _maxSimilarityObserved,
                _assessmentSimilaritySamples,
                _assessmentSimilaritySamples == 0 ? 0f : _minAssessmentSimilarityObserved,
                _assessmentSimilaritySamples == 0 ? 0f : _maxAssessmentSimilarityObserved,
                _reproductionSimilaritySamples,
                _reproductionSimilaritySamples == 0 ? 0f : _minReproductionSimilarityObserved,
                _reproductionSimilaritySamples == 0 ? 0f : _maxReproductionSimilarityObserved,
                _speciationCommitSimilaritySamples,
                _speciationCommitSimilaritySamples == 0 ? 0f : _minSpeciationCommitSimilarityObserved,
                _speciationCommitSimilaritySamples == 0 ? 0f : _maxSpeciationCommitSimilarityObserved,
                _childrenAddedToPool,
                _speciationCommitAttempts,
                _speciationCommitSuccesses,
                _lastFailure,
                _lastSeed);
        }
    }

    public async Task<EvolutionSimulationStatus> RunAsync(CancellationToken cancellationToken)
    {
        MarkRunStarted();
        try
        {
            await SeedInitialParentsAsync(cancellationToken).ConfigureAwait(false);

            while (!cancellationToken.IsCancellationRequested)
            {
                if (ReachedIterationLimit())
                {
                    break;
                }

                if (!TrySelectParents(out var parentA, out var parentB))
                {
                    await DelayBetweenIterationsAsync(cancellationToken).ConfigureAwait(false);
                    continue;
                }

                IncrementIterations();

                try
                {
                    await ExecuteIterationAsync(parentA, parentB, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    SetLastFailure($"iteration_failed:{ex.GetBaseException().Message}");
                }

                await DelayBetweenIterationsAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // normal stop path
        }
        finally
        {
            MarkRunStopped();
        }

        return GetStatus();
    }

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

    private bool ReachedIterationLimit()
    {
        if (_options.MaxIterations <= 0)
        {
            return false;
        }

        lock (_gate)
        {
            return _iterations >= (ulong)_options.MaxIterations;
        }
    }

    private bool TrySelectParents(out EvolutionParentRef parentA, out EvolutionParentRef parentB)
    {
        lock (_gate)
        {
            if (_parentPool.Count < 2)
            {
                parentA = default;
                parentB = default;
                return false;
            }

            var parentAIndex = SelectParentIndex(excludedIndex: -1);
            if (parentAIndex < 0)
            {
                parentA = default;
                parentB = default;
                return false;
            }

            var excludedParentKey = TryBuildParentKey(_parentPool[parentAIndex], out var parentAKey)
                ? parentAKey
                : string.Empty;
            var preferredLineageFamilyKey = ResolveTrackedLineageFamilyKey(_parentPool[parentAIndex]);
            var parentBIndex = SelectParentIndexForPair(
                excludedIndex: parentAIndex,
                excludedParentKey: excludedParentKey,
                preferredLineageFamilyKey: preferredLineageFamilyKey);
            if (parentBIndex < 0)
            {
                parentA = default;
                parentB = default;
                return false;
            }

            parentA = _parentPool[parentAIndex];
            parentB = _parentPool[parentBIndex];
            return true;
        }
    }

    // Caller must hold _gate.
    private int SelectParentIndex(int excludedIndex)
    {
        return SelectParentIndexCore(
            excludedIndex,
            excludedParentKey: null,
            preferredLineageFamilyKey: null);
    }

    // Caller must hold _gate.
    private int SelectParentIndexForPair(
        int excludedIndex,
        string? excludedParentKey,
        string? preferredLineageFamilyKey)
    {
        return SelectParentIndexCore(excludedIndex, excludedParentKey, preferredLineageFamilyKey);
    }

    // Caller must hold _gate.
    private int SelectParentIndexCore(
        int excludedIndex,
        string? excludedParentKey,
        string? preferredLineageFamilyKey)
    {
        if (_parentPool.Count == 0)
        {
            return -1;
        }

        var excludeParentKey = ShouldExcludeParentKey(excludedIndex, excludedParentKey);
        var preferLineageFamily = ShouldPreferLineageFamily(
            excludedIndex,
            excludedParentKey,
            preferredLineageFamilyKey);
        if (_options.ParentSelectionBias == EvolutionParentSelectionBias.Neutral)
        {
            return SelectUniformParentIndexCore(
                excludedIndex,
                excludedParentKey,
                excludeParentKey,
                preferredLineageFamilyKey,
                preferLineageFamily);
        }

        var speciesPopulationByKey = BuildSelectionSpeciesPopulationCounts(excludedIndex);
        var lineageFamilyPopulationByKey = BuildSelectionLineageFamilyPopulationCounts(excludedIndex);
        var useLineageFamilyAgeBias = lineageFamilyPopulationByKey.Count > 1;
        var flattenSingleFamilyDivergence =
            _options.ParentSelectionBias == EvolutionParentSelectionBias.Divergence
            && lineageFamilyPopulationByKey.Count == 1
            && speciesPopulationByKey.Count > 1;
        var flattenSpeciesAgeWithinFamily =
            _options.ParentSelectionBias == EvolutionParentSelectionBias.Divergence
            && speciesPopulationByKey.Count > 1;
        var useSpeciesAgeBias = !useLineageFamilyAgeBias && speciesPopulationByKey.Count > 1;

        var nowOrdinal = _nextParentOrdinal;
        var nowSpeciesOrdinal = _nextSpeciesOrdinal;
        var nowLineageFamilyOrdinal = _nextLineageFamilyOrdinal;
        var speciesAgeWeightByKey = useLineageFamilyAgeBias
            ? BuildSelectionSpeciesAgeWeights(
                speciesPopulationByKey.Keys,
                flattenSpeciesAgeWithinFamily,
                useFamilyRelativeSpeciesAge: true,
                nowSpeciesOrdinal)
            : null;
        var lineageFamilySpeciesWeightTotals = useLineageFamilyAgeBias
            ? BuildSelectionLineageFamilySpeciesWeightTotals(speciesAgeWeightByKey!)
            : null;
        double totalWeight = 0d;
        var weights = new double[_parentPool.Count];
        for (var i = 0; i < _parentPool.Count; i++)
        {
            if (i == excludedIndex)
            {
                continue;
            }

            if (excludeParentKey && IsExcludedParentKey(i, excludedParentKey))
            {
                continue;
            }

            if (preferLineageFamily && !MatchesPreferredLineageFamily(i, preferredLineageFamilyKey))
            {
                continue;
            }

            var speciesKey = ResolveTrackedSpeciesKey(_parentPool[i]);
            var speciesPopulation = ResolveSelectionSpeciesPopulation(i, speciesPopulationByKey);
            double weight;
            if (useLineageFamilyAgeBias)
            {
                var lineageFamilyKey = ResolveTrackedLineageFamilyKey(_parentPool[i]);
                var lineageFamilyAge = ResolveSelectionLineageFamilyAge(
                    lineageFamilyKey,
                    nowLineageFamilyOrdinal);
                var lineageFamilyAgeWeight = ResolveParentSelectionAgeWeight(lineageFamilyAge);
                var speciesAgeWeight = speciesAgeWeightByKey!.TryGetValue(speciesKey, out var trackedSpeciesAgeWeight)
                    && trackedSpeciesAgeWeight > 0d
                    ? trackedSpeciesAgeWeight
                    : 1d;
                var lineageFamilySpeciesWeightTotal =
                    lineageFamilySpeciesWeightTotals!.TryGetValue(
                        lineageFamilyKey,
                        out var trackedLineageFamilySpeciesWeightTotal)
                    && trackedLineageFamilySpeciesWeightTotal > 0d
                        ? trackedLineageFamilySpeciesWeightTotal
                        : speciesAgeWeight;
                weight = lineageFamilyAgeWeight
                    * (speciesAgeWeight / lineageFamilySpeciesWeightTotal)
                    / Math.Max(1d, speciesPopulation);
            }
            else
            {
                var age = flattenSingleFamilyDivergence
                    ? 1UL
                    : ResolveSelectionAgeForBias(
                        i,
                        useLineageFamilyAgeBias,
                        useSpeciesAgeBias,
                        nowLineageFamilyOrdinal,
                        nowSpeciesOrdinal,
                        nowOrdinal);
                weight = ResolveParentSelectionWeight(age, speciesPopulation);
            }

            if (!double.IsFinite(weight) || weight <= 0d)
            {
                continue;
            }

            weights[i] = weight;
            totalWeight += weight;
        }

        if (totalWeight <= 0d || !double.IsFinite(totalWeight))
        {
            return SelectUniformParentIndexCore(
                excludedIndex,
                excludedParentKey,
                excludeParentKey,
                preferredLineageFamilyKey,
                preferLineageFamily);
        }

        var sample = _random.NextUnitDouble() * totalWeight;
        var cumulative = 0d;
        for (var i = 0; i < weights.Length; i++)
        {
            var weight = weights[i];
            if (weight <= 0d)
            {
                continue;
            }

            cumulative += weight;
            if (sample <= cumulative)
            {
                return i;
            }
        }

        for (var i = weights.Length - 1; i >= 0; i--)
        {
            if (weights[i] > 0d)
            {
                return i;
            }
        }

        return -1;
    }

    // Caller must hold _gate.
    private int SelectUniformParentIndex(int excludedIndex)
    {
        return SelectUniformParentIndexCore(
            excludedIndex,
            excludedParentKey: null,
            excludeParentKey: false,
            preferredLineageFamilyKey: null,
            preferLineageFamily: false);
    }

    // Caller must hold _gate.
    private int SelectUniformParentIndexCore(
        int excludedIndex,
        string? excludedParentKey,
        bool excludeParentKey,
        string? preferredLineageFamilyKey,
        bool preferLineageFamily)
    {
        var selectedIndex = -1;
        var eligibleCount = 0;
        for (var i = 0; i < _parentPool.Count; i++)
        {
            if (i == excludedIndex)
            {
                continue;
            }

            if (excludeParentKey && IsExcludedParentKey(i, excludedParentKey))
            {
                continue;
            }

            if (preferLineageFamily && !MatchesPreferredLineageFamily(i, preferredLineageFamilyKey))
            {
                continue;
            }

            eligibleCount++;
            if (eligibleCount == 1 || _random.NextInt(eligibleCount) == 0)
            {
                selectedIndex = i;
            }
        }

        return selectedIndex;
    }

    // Caller must hold _gate.
    private bool ShouldExcludeParentKey(int excludedIndex, string? excludedParentKey)
    {
        if (string.IsNullOrWhiteSpace(excludedParentKey))
        {
            return false;
        }

        for (var i = 0; i < _parentPool.Count; i++)
        {
            if (i == excludedIndex)
            {
                continue;
            }

            if (!IsExcludedParentKey(i, excludedParentKey))
            {
                return true;
            }
        }

        return false;
    }

    // Caller must hold _gate.
    private bool ShouldPreferLineageFamily(
        int excludedIndex,
        string? excludedParentKey,
        string? preferredLineageFamilyKey)
    {
        var normalizedPreferredLineageFamilyKey = NormalizeSpeciesId(preferredLineageFamilyKey);
        if (normalizedPreferredLineageFamilyKey.Length == 0
            || string.Equals(normalizedPreferredLineageFamilyKey, "(unknown)", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        for (var i = 0; i < _parentPool.Count; i++)
        {
            if (i == excludedIndex)
            {
                continue;
            }

            if (IsExcludedParentKey(i, excludedParentKey))
            {
                continue;
            }

            if (MatchesPreferredLineageFamily(i, normalizedPreferredLineageFamilyKey))
            {
                return true;
            }
        }

        return false;
    }

    // Caller must hold _gate.
    private bool IsExcludedParentKey(int parentIndex, string? excludedParentKey)
    {
        if (parentIndex < 0
            || parentIndex >= _parentPool.Count
            || string.IsNullOrWhiteSpace(excludedParentKey))
        {
            return false;
        }

        return TryBuildParentKey(_parentPool[parentIndex], out var candidateKey)
            && string.Equals(candidateKey, excludedParentKey, StringComparison.OrdinalIgnoreCase);
    }

    // Caller must hold _gate.
    private bool MatchesPreferredLineageFamily(int parentIndex, string? preferredLineageFamilyKey)
    {
        if (parentIndex < 0
            || parentIndex >= _parentPool.Count
            || string.IsNullOrWhiteSpace(preferredLineageFamilyKey))
        {
            return false;
        }

        return string.Equals(
            ResolveTrackedLineageFamilyKey(_parentPool[parentIndex]),
            preferredLineageFamilyKey,
            StringComparison.OrdinalIgnoreCase);
    }

    // Caller must hold _gate.
    private Dictionary<string, int> BuildSelectionSpeciesPopulationCounts(int excludedIndex)
    {
        var speciesCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < _parentPool.Count; i++)
        {
            if (i == excludedIndex)
            {
                continue;
            }

            var speciesKey = ResolveTrackedSpeciesKey(_parentPool[i]);
            speciesCounts[speciesKey] = speciesCounts.TryGetValue(speciesKey, out var count)
                ? count + 1
                : 1;
        }

        return speciesCounts;
    }

    // Caller must hold _gate.
    private Dictionary<string, int> BuildSelectionLineageFamilyPopulationCounts(int excludedIndex)
    {
        var lineageFamilyCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < _parentPool.Count; i++)
        {
            if (i == excludedIndex)
            {
                continue;
            }

            var lineageFamilyKey = ResolveTrackedLineageFamilyKey(_parentPool[i]);
            lineageFamilyCounts[lineageFamilyKey] = lineageFamilyCounts.TryGetValue(lineageFamilyKey, out var count)
                ? count + 1
                : 1;
        }

        return lineageFamilyCounts;
    }

    // Caller must hold _gate.
    private Dictionary<string, double> BuildSelectionSpeciesAgeWeights(
        IEnumerable<string> speciesKeys,
        bool flattenSpeciesAge,
        bool useFamilyRelativeSpeciesAge,
        ulong nowSpeciesOrdinal)
    {
        var normalizedSpeciesKeys = speciesKeys
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var familyRelativeNowSpeciesOrdinals = useFamilyRelativeSpeciesAge && !flattenSpeciesAge
            ? BuildSelectionCurrentSpeciesOrdinalsByLineageFamily(normalizedSpeciesKeys)
            : null;
        var weights = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        foreach (var speciesKey in normalizedSpeciesKeys)
        {
            var effectiveNowSpeciesOrdinal = nowSpeciesOrdinal;
            if (familyRelativeNowSpeciesOrdinals is not null)
            {
                var lineageFamilyKey = ResolveTrackedLineageFamilyKey(speciesKey);
                if (familyRelativeNowSpeciesOrdinals.TryGetValue(lineageFamilyKey, out var trackedNowSpeciesOrdinal)
                    && trackedNowSpeciesOrdinal > 0)
                {
                    effectiveNowSpeciesOrdinal = trackedNowSpeciesOrdinal;
                }
            }

            var age = flattenSpeciesAge
                ? 1UL
                : ResolveSelectionSpeciesAge(speciesKey, effectiveNowSpeciesOrdinal);
            weights[speciesKey] = ResolveParentSelectionAgeWeight(age);
        }

        return weights;
    }

    // Caller must hold _gate.
    private Dictionary<string, ulong> BuildSelectionCurrentSpeciesOrdinalsByLineageFamily(
        IEnumerable<string> speciesKeys)
    {
        var ordinals = new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase);
        foreach (var speciesKey in speciesKeys)
        {
            var lineageFamilyKey = ResolveTrackedLineageFamilyKey(speciesKey);
            var speciesOrdinal = _speciesFirstSeenOrdinals.TryGetValue(speciesKey, out var trackedSpeciesOrdinal)
                ? trackedSpeciesOrdinal
                : 0UL;
            var currentNowOrdinal = Math.Max(1UL, speciesOrdinal + 1UL);
            ordinals[lineageFamilyKey] = ordinals.TryGetValue(lineageFamilyKey, out var existingNowOrdinal)
                ? Math.Max(existingNowOrdinal, currentNowOrdinal)
                : currentNowOrdinal;
        }

        return ordinals;
    }

    // Caller must hold _gate.
    private Dictionary<string, double> BuildSelectionLineageFamilySpeciesWeightTotals(
        IReadOnlyDictionary<string, double> speciesAgeWeightByKey)
    {
        var totals = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        foreach (var entry in speciesAgeWeightByKey)
        {
            var lineageFamilyKey = ResolveTrackedLineageFamilyKey(entry.Key);
            totals[lineageFamilyKey] = totals.TryGetValue(lineageFamilyKey, out var total)
                ? total + entry.Value
                : entry.Value;
        }

        return totals;
    }

    // Caller must hold _gate.
    private ulong ResolveSelectionAgeForBias(
        int parentIndex,
        bool useLineageFamilyAgeBias,
        bool useSpeciesAgeBias,
        ulong nowLineageFamilyOrdinal,
        ulong nowSpeciesOrdinal,
        ulong nowParentOrdinal)
    {
        if (parentIndex < 0 || parentIndex >= _parentPool.Count)
        {
            return 1UL;
        }

        if (useLineageFamilyAgeBias
            && TryBuildParentKey(_parentPool[parentIndex], out var parentKey)
            && _parentLineageFamilyByParentKey.TryGetValue(parentKey, out var lineageFamilyId)
            && !string.IsNullOrWhiteSpace(lineageFamilyId)
            && _lineageFamilyFirstSeenOrdinals.TryGetValue(lineageFamilyId, out var firstSeenLineageFamilyOrdinal))
        {
            return Math.Max(1UL, nowLineageFamilyOrdinal - firstSeenLineageFamilyOrdinal);
        }

        if (useSpeciesAgeBias
            && TryBuildParentKey(_parentPool[parentIndex], out var speciesParentKey)
            && _parentSpeciesByParentKey.TryGetValue(speciesParentKey, out var speciesId)
            && !string.IsNullOrWhiteSpace(speciesId)
            && _speciesFirstSeenOrdinals.TryGetValue(speciesId, out var firstSeenSpeciesOrdinal))
        {
            return Math.Max(1UL, nowSpeciesOrdinal - firstSeenSpeciesOrdinal);
        }

        var addedOrdinal = parentIndex < _parentAddedOrdinals.Count ? _parentAddedOrdinals[parentIndex] : 1UL;
        return Math.Max(1UL, nowParentOrdinal - addedOrdinal);
    }

    // Caller must hold _gate.
    private ulong ResolveSelectionSpeciesAge(string speciesId, ulong nowSpeciesOrdinal)
    {
        var normalizedSpeciesId = NormalizeSpeciesId(speciesId);
        if (normalizedSpeciesId.Length == 0
            || !_speciesFirstSeenOrdinals.TryGetValue(normalizedSpeciesId, out var firstSeenSpeciesOrdinal))
        {
            return 1UL;
        }

        return Math.Max(1UL, nowSpeciesOrdinal - firstSeenSpeciesOrdinal);
    }

    // Caller must hold _gate.
    private ulong ResolveSelectionLineageFamilyAge(string lineageFamilyId, ulong nowLineageFamilyOrdinal)
    {
        var normalizedLineageFamilyId = NormalizeSpeciesId(lineageFamilyId);
        if (normalizedLineageFamilyId.Length == 0
            || !_lineageFamilyFirstSeenOrdinals.TryGetValue(
                normalizedLineageFamilyId,
                out var firstSeenLineageFamilyOrdinal))
        {
            return 1UL;
        }

        return Math.Max(1UL, nowLineageFamilyOrdinal - firstSeenLineageFamilyOrdinal);
    }

    // Caller must hold _gate.
    private int ResolveSelectionSpeciesPopulation(
        int parentIndex,
        IReadOnlyDictionary<string, int> speciesPopulationByKey)
    {
        if (parentIndex < 0 || parentIndex >= _parentPool.Count)
        {
            return 1;
        }

        var speciesKey = ResolveTrackedSpeciesKey(_parentPool[parentIndex]);
        return speciesPopulationByKey.TryGetValue(speciesKey, out var speciesPopulation)
            ? Math.Max(1, speciesPopulation)
            : 1;
    }

    // Caller must hold _gate.
    private int ResolveSelectionLineageFamilyPopulation(
        int parentIndex,
        IReadOnlyDictionary<string, int> lineageFamilyPopulationByKey)
    {
        if (parentIndex < 0 || parentIndex >= _parentPool.Count)
        {
            return 1;
        }

        var lineageFamilyKey = ResolveTrackedLineageFamilyKey(_parentPool[parentIndex]);
        return lineageFamilyPopulationByKey.TryGetValue(lineageFamilyKey, out var lineageFamilyPopulation)
            ? Math.Max(1, lineageFamilyPopulation)
            : 1;
    }

    private double ResolveParentSelectionWeight(ulong age, int speciesPopulation)
    {
        var ageWeight = ResolveParentSelectionAgeWeight(age);
        var representationWeight = 1d / Math.Max(1d, speciesPopulation);
        return ageWeight * representationWeight;
    }

    private double ResolveParentSelectionAgeWeight(ulong age)
    {
        var normalizedAge = Math.Max(1d, age);
        var weightedAge = Math.Pow(normalizedAge, ParentSelectionBiasExponent);
        return _options.ParentSelectionBias switch
        {
            EvolutionParentSelectionBias.Divergence => 1d / weightedAge,
            EvolutionParentSelectionBias.Stability => weightedAge,
            _ => 1d
        };
    }

    private int AddChildrenToPool(ReproductionOutcome reproduction)
    {
        if (_options.ParentMode == EvolutionParentMode.BrainIds)
        {
            return AddSpawnedChildrenToBrainPool(reproduction.CommitCandidates);
        }

        return AddChildrenToArtifactPool(reproduction.ChildDefinitions);
    }

    private bool TryAddCommittedCandidateToPool(
        SpeciationCommitCandidate candidate,
        string? candidateSpeciesId,
        string? candidateSourceSpeciesId)
    {
        if (!TryBuildParentRefFromCandidate(candidate, _options.ParentMode, out var parentRef)
            || !TryBuildParentKey(parentRef, out var candidateKey))
        {
            return false;
        }

        var normalizedCandidateSpeciesId = NormalizeSpeciesId(candidateSpeciesId);
        lock (_gate)
        {
            if (_parentPoolKeys.Contains(candidateKey))
            {
                if (normalizedCandidateSpeciesId.Length > 0)
                {
                    RecordParentSpeciesLocked(
                        candidateKey,
                        normalizedCandidateSpeciesId,
                        candidateSourceSpeciesId);
                }

                return false;
            }

            if (!TryAddParentToPoolAtCapacity(
                    parentRef,
                    candidateKey,
                    normalizedCandidateSpeciesId,
                    candidateSourceSpeciesId))
            {
                return false;
            }

            if (normalizedCandidateSpeciesId.Length > 0)
            {
                RecordParentSpeciesLocked(
                    candidateKey,
                    normalizedCandidateSpeciesId,
                    candidateSourceSpeciesId);
            }

            _childrenAddedToPool++;
            return true;
        }
    }

    private int AddChildrenToArtifactPool(IReadOnlyList<ArtifactRef> children)
    {
        if (children.Count == 0)
        {
            return 0;
        }

        var addedCount = 0;
        lock (_gate)
        {
            foreach (var child in children)
            {
                var candidate = EvolutionParentRef.FromArtifactRef(child);
                if (!TryBuildParentKey(candidate, out var key))
                {
                    continue;
                }

                if (_parentPoolKeys.Contains(key))
                {
                    continue;
                }

                if (!TryAddParentToPoolAtCapacity(candidate, key))
                {
                    continue;
                }

                _childrenAddedToPool++;
                addedCount++;
            }
        }

        return addedCount;
    }

    private int AddSpawnedChildrenToBrainPool(IReadOnlyList<SpeciationCommitCandidate> candidates)
    {
        if (candidates.Count == 0)
        {
            return 0;
        }

        var addedCount = 0;
        lock (_gate)
        {
            foreach (var candidate in candidates)
            {
                if (candidate.ChildBrainId is not Guid childBrainId || childBrainId == Guid.Empty)
                {
                    continue;
                }

                var parentRef = EvolutionParentRef.FromBrainId(childBrainId);
                if (!TryBuildParentKey(parentRef, out var key))
                {
                    continue;
                }

                if (_parentPoolKeys.Contains(key))
                {
                    continue;
                }

                if (!TryAddParentToPoolAtCapacity(parentRef, key))
                {
                    continue;
                }

                _childrenAddedToPool++;
                addedCount++;
            }
        }

        return addedCount;
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

    private void MarkRunStarted()
    {
        lock (_gate)
        {
            _sessionId = Guid.NewGuid().ToString("N");
            _running = true;
            _iterations = 0;
            _compatibilityChecks = 0;
            _compatiblePairs = 0;
            _reproductionCalls = 0;
            _reproductionFailures = 0;
            _reproductionRunsObserved = 0;
            _reproductionRunsWithMutations = 0;
            _reproductionMutationEvents = 0;
            _similaritySamples = 0;
            _minSimilarityObserved = 0f;
            _maxSimilarityObserved = 0f;
            _assessmentSimilaritySamples = 0;
            _minAssessmentSimilarityObserved = 0f;
            _maxAssessmentSimilarityObserved = 0f;
            _reproductionSimilaritySamples = 0;
            _minReproductionSimilarityObserved = 0f;
            _maxReproductionSimilarityObserved = 0f;
            _speciationCommitSimilaritySamples = 0;
            _minSpeciationCommitSimilarityObserved = 0f;
            _maxSpeciationCommitSimilarityObserved = 0f;
            _childrenAddedToPool = 0;
            _speciationCommitAttempts = 0;
            _speciationCommitSuccesses = 0;
            _speciationCommitSamplesSinceImprovement = 0;
            _runPressureNudge = 0f;
            _lastFailure = string.Empty;
            _lastSeed = 0;
        }
    }

    private void RecordReproductionDiagnostics(ReproductionDiagnostics diagnostics)
    {
        lock (_gate)
        {
            _reproductionRunsObserved += diagnostics.RunCount;
            _reproductionRunsWithMutations += diagnostics.RunsWithMutations;
            _reproductionMutationEvents += diagnostics.MutationEvents;
        }

        if (diagnostics.SimilaritySamples > 0)
        {
            RecordOverallSimilarityRange(diagnostics.MinSimilarity, diagnostics.MaxSimilarity, diagnostics.SimilaritySamples);
            RecordReproductionSimilarityRange(diagnostics.MinSimilarity, diagnostics.MaxSimilarity, diagnostics.SimilaritySamples);
        }
    }

    private void RecordAssessmentSimilarity(float similarity)
    {
        if (float.IsNaN(similarity) || float.IsInfinity(similarity))
        {
            return;
        }

        var normalized = Math.Clamp(similarity, 0f, 1f);
        RecordOverallSimilarityRange(normalized, normalized, 1);
        RecordAssessmentSimilarityRange(normalized, normalized, 1);
    }

    private void RecordSpeciationCommitSimilarity(float? similarity)
    {
        if (!similarity.HasValue || float.IsNaN(similarity.Value) || float.IsInfinity(similarity.Value))
        {
            return;
        }

        var normalized = Math.Clamp(similarity.Value, 0f, 1f);
        RecordSpeciationCommitSimilarityRange(normalized, normalized, 1);
    }

    private void RecordOverallSimilarityRange(float minSimilarity, float maxSimilarity, ulong samples)
    {
        if (samples == 0)
        {
            return;
        }

        var normalizedMin = Math.Clamp(minSimilarity, 0f, 1f);
        var normalizedMax = Math.Clamp(maxSimilarity, 0f, 1f);
        if (normalizedMax < normalizedMin)
        {
            (normalizedMin, normalizedMax) = (normalizedMax, normalizedMin);
        }

        lock (_gate)
        {
            if (_similaritySamples == 0)
            {
                _minSimilarityObserved = normalizedMin;
                _maxSimilarityObserved = normalizedMax;
            }
            else
            {
                _minSimilarityObserved = Math.Min(_minSimilarityObserved, normalizedMin);
                _maxSimilarityObserved = Math.Max(_maxSimilarityObserved, normalizedMax);
            }

            _similaritySamples += samples;
        }
    }

    private void RecordAssessmentSimilarityRange(float minSimilarity, float maxSimilarity, ulong samples)
    {
        if (samples == 0)
        {
            return;
        }

        var normalizedMin = Math.Clamp(minSimilarity, 0f, 1f);
        var normalizedMax = Math.Clamp(maxSimilarity, 0f, 1f);
        if (normalizedMax < normalizedMin)
        {
            (normalizedMin, normalizedMax) = (normalizedMax, normalizedMin);
        }

        lock (_gate)
        {
            if (_assessmentSimilaritySamples == 0)
            {
                _minAssessmentSimilarityObserved = normalizedMin;
                _maxAssessmentSimilarityObserved = normalizedMax;
            }
            else
            {
                _minAssessmentSimilarityObserved = Math.Min(_minAssessmentSimilarityObserved, normalizedMin);
                _maxAssessmentSimilarityObserved = Math.Max(_maxAssessmentSimilarityObserved, normalizedMax);
            }

            _assessmentSimilaritySamples += samples;
        }
    }

    private void RecordReproductionSimilarityRange(float minSimilarity, float maxSimilarity, ulong samples)
    {
        if (samples == 0)
        {
            return;
        }

        var normalizedMin = Math.Clamp(minSimilarity, 0f, 1f);
        var normalizedMax = Math.Clamp(maxSimilarity, 0f, 1f);
        if (normalizedMax < normalizedMin)
        {
            (normalizedMin, normalizedMax) = (normalizedMax, normalizedMin);
        }

        lock (_gate)
        {
            if (_reproductionSimilaritySamples == 0)
            {
                _minReproductionSimilarityObserved = normalizedMin;
                _maxReproductionSimilarityObserved = normalizedMax;
            }
            else
            {
                _minReproductionSimilarityObserved = Math.Min(_minReproductionSimilarityObserved, normalizedMin);
                _maxReproductionSimilarityObserved = Math.Max(_maxReproductionSimilarityObserved, normalizedMax);
            }

            _reproductionSimilaritySamples += samples;
        }
    }

    private void RecordSpeciationCommitSimilarityRange(float minSimilarity, float maxSimilarity, ulong samples)
    {
        if (samples == 0)
        {
            return;
        }

        var normalizedMin = Math.Clamp(minSimilarity, 0f, 1f);
        var normalizedMax = Math.Clamp(maxSimilarity, 0f, 1f);
        if (normalizedMax < normalizedMin)
        {
            (normalizedMin, normalizedMax) = (normalizedMax, normalizedMin);
        }

        lock (_gate)
        {
            var improved = _speciationCommitSimilaritySamples == 0;
            if (_speciationCommitSimilaritySamples == 0)
            {
                _minSpeciationCommitSimilarityObserved = normalizedMin;
                _maxSpeciationCommitSimilarityObserved = normalizedMax;
            }
            else
            {
                improved = normalizedMin < _minSpeciationCommitSimilarityObserved;
                _minSpeciationCommitSimilarityObserved = Math.Min(_minSpeciationCommitSimilarityObserved, normalizedMin);
                _maxSpeciationCommitSimilarityObserved = Math.Max(_maxSpeciationCommitSimilarityObserved, normalizedMax);
            }

            _speciationCommitSimilaritySamples += samples;

            if (_options.RunPressureMode == EvolutionRunPressureMode.Neutral)
            {
                _speciationCommitSamplesSinceImprovement = 0;
                _runPressureNudge = 0f;
                return;
            }

            if (improved)
            {
                _speciationCommitSamplesSinceImprovement = 0;
                _runPressureNudge = Math.Max(0f, _runPressureNudge - RunPressureRecoveryStep);
                return;
            }

            _speciationCommitSamplesSinceImprovement += samples;
            while (_speciationCommitSamplesSinceImprovement >= CommitSimilarityPlateauWindowSamples)
            {
                _speciationCommitSamplesSinceImprovement -= CommitSimilarityPlateauWindowSamples;
                _runPressureNudge = Math.Min(MaxRunPressureNudge, _runPressureNudge + RunPressureNudgeStep);
                if (_runPressureNudge >= MaxRunPressureNudge)
                {
                    _speciationCommitSamplesSinceImprovement = 0;
                    break;
                }
            }
        }
    }

    private float ApplyRunPressureNudge(float similarity)
    {
        if (float.IsNaN(similarity) || float.IsInfinity(similarity))
        {
            return 0f;
        }

        var normalized = Math.Clamp(similarity, 0f, 1f);
        lock (_gate)
        {
            return _options.RunPressureMode switch
            {
                EvolutionRunPressureMode.Stability => Math.Clamp(normalized + _runPressureNudge, 0f, 1f),
                EvolutionRunPressureMode.Neutral => normalized,
                _ => Math.Clamp(normalized - _runPressureNudge, 0f, 1f)
            };
        }
    }

    private void MarkRunStopped()
    {
        lock (_gate)
        {
            _running = false;
        }
    }

    private void IncrementIterations()
    {
        lock (_gate)
        {
            _iterations++;
        }
    }

    private void IncrementCompatibilityChecks()
    {
        lock (_gate)
        {
            _compatibilityChecks++;
        }
    }

    private void IncrementCompatiblePairs()
    {
        lock (_gate)
        {
            _compatiblePairs++;
        }
    }

    private void IncrementReproductionCalls()
    {
        lock (_gate)
        {
            _reproductionCalls++;
        }
    }

    private void IncrementReproductionFailures()
    {
        lock (_gate)
        {
            _reproductionFailures++;
        }
    }

    private void IncrementSpeciationCommitAttempts()
    {
        lock (_gate)
        {
            _speciationCommitAttempts++;
        }
    }

    private void IncrementSpeciationCommitSuccesses()
    {
        lock (_gate)
        {
            _speciationCommitSuccesses++;
        }
    }

    private void SetLastFailure(string reason)
    {
        if (string.IsNullOrWhiteSpace(reason))
        {
            return;
        }

        lock (_gate)
        {
            _lastFailure = reason.Trim();
        }
    }

    private void RecordParentSpecies(
        string parentKey,
        string speciesId,
        string? sourceSpeciesId)
    {
        if (string.IsNullOrWhiteSpace(parentKey) || string.IsNullOrWhiteSpace(speciesId))
        {
            return;
        }

        lock (_gate)
        {
            RecordParentSpeciesLocked(parentKey, speciesId, sourceSpeciesId);
        }
    }

    // Caller must hold _gate.
    private void RecordParentSpeciesLocked(
        string parentKey,
        string speciesId,
        string? sourceSpeciesId)
    {
        var normalizedSpeciesId = speciesId.Trim();
        if (normalizedSpeciesId.Length == 0)
        {
            return;
        }

        _parentSpeciesByParentKey[parentKey] = normalizedSpeciesId;
        if (!_speciesFirstSeenOrdinals.ContainsKey(normalizedSpeciesId))
        {
            _speciesFirstSeenOrdinals[normalizedSpeciesId] = _nextSpeciesOrdinal++;
        }

        var lineageFamilyId = ResolveLineageFamilyKeyLocked(
            normalizedSpeciesId,
            sourceSpeciesId,
            registerIfMissing: true);
        if (lineageFamilyId.Length > 0)
        {
            _parentLineageFamilyByParentKey[parentKey] = lineageFamilyId;
        }
    }

    private void NormalizeInitialSeedSelectionOrdinals(IReadOnlyCollection<string> seededParentKeys)
    {
        if (seededParentKeys is null || seededParentKeys.Count < 2)
        {
            return;
        }

        lock (_gate)
        {
            var seededSpeciesIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var seededLineageFamilyIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var seededParentKey in seededParentKeys)
            {
                if (_parentSpeciesByParentKey.TryGetValue(seededParentKey, out var speciesId)
                    && NormalizeSpeciesId(speciesId).Length > 0)
                {
                    seededSpeciesIds.Add(NormalizeSpeciesId(speciesId));
                }

                if (_parentLineageFamilyByParentKey.TryGetValue(seededParentKey, out var lineageFamilyId)
                    && NormalizeSpeciesId(lineageFamilyId).Length > 0)
                {
                    seededLineageFamilyIds.Add(NormalizeSpeciesId(lineageFamilyId));
                }
            }

            if (seededSpeciesIds.Count > 1)
            {
                var sharedSpeciesOrdinal = seededSpeciesIds
                    .Select(speciesId => _speciesFirstSeenOrdinals.TryGetValue(speciesId, out var ordinal) ? ordinal : 0UL)
                    .Where(static ordinal => ordinal > 0)
                    .DefaultIfEmpty(0UL)
                    .Min();
                if (sharedSpeciesOrdinal > 0)
                {
                    foreach (var speciesId in seededSpeciesIds)
                    {
                        _speciesFirstSeenOrdinals[speciesId] = sharedSpeciesOrdinal;
                    }
                }
            }

            if (seededLineageFamilyIds.Count > 1)
            {
                var sharedLineageFamilyOrdinal = seededLineageFamilyIds
                    .Select(lineageFamilyId => _lineageFamilyFirstSeenOrdinals.TryGetValue(lineageFamilyId, out var ordinal) ? ordinal : 0UL)
                    .Where(static ordinal => ordinal > 0)
                    .DefaultIfEmpty(0UL)
                    .Min();
                if (sharedLineageFamilyOrdinal > 0)
                {
                    foreach (var lineageFamilyId in seededLineageFamilyIds)
                    {
                        _lineageFamilyFirstSeenOrdinals[lineageFamilyId] = sharedLineageFamilyOrdinal;
                    }
                }
            }
        }
    }

    // Caller must hold _gate.
    private string ResolveLineageFamilyKeyLocked(
        string? speciesId,
        string? sourceSpeciesId,
        bool registerIfMissing)
    {
        var normalizedSpeciesId = NormalizeSpeciesId(speciesId);
        if (normalizedSpeciesId.Length == 0)
        {
            return string.Empty;
        }

        if (_lineageFamilyBySpeciesId.TryGetValue(normalizedSpeciesId, out var existingLineageFamilyId)
            && NormalizeSpeciesId(existingLineageFamilyId).Length > 0)
        {
            return NormalizeSpeciesId(existingLineageFamilyId);
        }

        var normalizedSourceSpeciesId = NormalizeSpeciesId(sourceSpeciesId);
        var lineageFamilyId = normalizedSpeciesId;
        if (normalizedSourceSpeciesId.Length > 0
            && !string.Equals(normalizedSourceSpeciesId, normalizedSpeciesId, StringComparison.OrdinalIgnoreCase))
        {
            lineageFamilyId = _lineageFamilyBySpeciesId.TryGetValue(normalizedSourceSpeciesId, out var sourceLineageFamilyId)
                && NormalizeSpeciesId(sourceLineageFamilyId).Length > 0
                ? NormalizeSpeciesId(sourceLineageFamilyId)
                : normalizedSourceSpeciesId;
        }

        if (!registerIfMissing)
        {
            return lineageFamilyId;
        }

        if (normalizedSourceSpeciesId.Length > 0
            && !_lineageFamilyBySpeciesId.ContainsKey(normalizedSourceSpeciesId))
        {
            _lineageFamilyBySpeciesId[normalizedSourceSpeciesId] = lineageFamilyId;
        }

        _lineageFamilyBySpeciesId[normalizedSpeciesId] = lineageFamilyId;
        EnsureLineageFamilyOrdinalLocked(lineageFamilyId);
        return lineageFamilyId;
    }

    // Caller must hold _gate.
    private void EnsureLineageFamilyOrdinalLocked(string lineageFamilyId)
    {
        var normalizedLineageFamilyId = NormalizeSpeciesId(lineageFamilyId);
        if (normalizedLineageFamilyId.Length == 0)
        {
            return;
        }

        if (!_lineageFamilyFirstSeenOrdinals.ContainsKey(normalizedLineageFamilyId))
        {
            _lineageFamilyFirstSeenOrdinals[normalizedLineageFamilyId] = _nextLineageFamilyOrdinal++;
        }
    }

    // Caller must hold _gate.
    private bool TryAddParentToPoolAtCapacity(
        EvolutionParentRef candidate,
        string candidateKey,
        string? candidateSpeciesId = null,
        string? candidateSourceSpeciesId = null)
    {
        if (_parentPool.Count < _options.MaxParentPoolSize)
        {
            _parentPool.Add(candidate);
            _parentAddedOrdinals.Add(_nextParentOrdinal++);
            _parentPoolKeys.Add(candidateKey);
            return true;
        }

        var candidateLineageFamilyId = ResolveLineageFamilyKeyLocked(
            candidateSpeciesId,
            candidateSourceSpeciesId,
            registerIfMissing: false);
        if (!TrySelectEvictionIndex(
                candidateSpeciesId,
                candidateLineageFamilyId,
                out var evictionIndex))
        {
            return false;
        }

        var evicted = _parentPool[evictionIndex];
        if (TryBuildParentKey(evicted, out var evictedKey))
        {
            _parentPoolKeys.Remove(evictedKey);
            _parentSpeciesByParentKey.Remove(evictedKey);
            _parentLineageFamilyByParentKey.Remove(evictedKey);
        }

        _parentPool[evictionIndex] = candidate;
        if (evictionIndex < _parentAddedOrdinals.Count)
        {
            _parentAddedOrdinals[evictionIndex] = _nextParentOrdinal++;
        }
        else
        {
            _parentAddedOrdinals.Add(_nextParentOrdinal++);
        }
        _parentPoolKeys.Add(candidateKey);
        return true;
    }

    // Caller must hold _gate.
    private bool TrySelectEvictionIndex(
        string? candidateSpeciesId,
        string? candidateLineageFamilyId,
        out int evictionIndex)
    {
        var totalSpeciesCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var totalLineageFamilyCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < _parentPool.Count; i++)
        {
            var current = _parentPool[i];
            var speciesKey = ResolveTrackedSpeciesKey(current);
            if (totalSpeciesCounts.TryGetValue(speciesKey, out var currentCount))
            {
                totalSpeciesCounts[speciesKey] = currentCount + 1;
            }
            else
            {
                totalSpeciesCounts[speciesKey] = 1;
            }

            var lineageFamilyKey = ResolveTrackedLineageFamilyKey(current);
            if (totalLineageFamilyCounts.TryGetValue(lineageFamilyKey, out var currentLineageFamilyCount))
            {
                totalLineageFamilyCounts[lineageFamilyKey] = currentLineageFamilyCount + 1;
            }
            else
            {
                totalLineageFamilyCounts[lineageFamilyKey] = 1;
            }
        }

        var eligibleEntries = new List<(int Index, string SpeciesKey, int TotalSpeciesCount, string LineageFamilyKey, int TotalLineageFamilyCount)>();
        for (var i = 0; i < _parentPool.Count; i++)
        {
            var current = _parentPool[i];
            if (TryBuildParentKey(current, out var currentKey)
                && _protectedParentPoolKeys.Contains(currentKey))
            {
                continue;
            }

            var speciesKey = ResolveTrackedSpeciesKey(current);
            var totalSpeciesCount = totalSpeciesCounts.TryGetValue(speciesKey, out var count)
                ? count
                : 1;
            var lineageFamilyKey = ResolveTrackedLineageFamilyKey(current);
            var totalLineageFamilyCount = totalLineageFamilyCounts.TryGetValue(lineageFamilyKey, out var lineageCount)
                ? lineageCount
                : 1;
            eligibleEntries.Add((i, speciesKey, totalSpeciesCount, lineageFamilyKey, totalLineageFamilyCount));
        }

        if (eligibleEntries.Count == 0)
        {
            evictionIndex = -1;
            return false;
        }

        var normalizedCandidateLineageFamilyId = ResolveTrackedLineageFamilyKey(candidateLineageFamilyId);
        if (!string.Equals(normalizedCandidateLineageFamilyId, "(unknown)", StringComparison.Ordinal))
        {
            var candidateLineageFamilyCount = totalLineageFamilyCounts.TryGetValue(
                normalizedCandidateLineageFamilyId,
                out var lineageCount)
                ? lineageCount
                : 0;
            var moreRepresentedLineageFamilyEntries = eligibleEntries
                .Where(entry => entry.TotalLineageFamilyCount > candidateLineageFamilyCount)
                .ToList();
            if (moreRepresentedLineageFamilyEntries.Count > 0)
            {
                eligibleEntries = moreRepresentedLineageFamilyEntries;
            }
            else if (candidateLineageFamilyCount > 0)
            {
                var sameLineageFamilyEntries = eligibleEntries
                    .Where(entry => string.Equals(
                        entry.LineageFamilyKey,
                        normalizedCandidateLineageFamilyId,
                        StringComparison.OrdinalIgnoreCase))
                    .ToList();
                if (sameLineageFamilyEntries.Count == 0)
                {
                    evictionIndex = -1;
                    return false;
                }

                eligibleEntries = sameLineageFamilyEntries;
            }
        }

        var normalizedCandidateSpeciesId = NormalizeSpeciesId(candidateSpeciesId);
        if (normalizedCandidateSpeciesId.Length > 0)
        {
            var candidateSpeciesKey = ResolveTrackedSpeciesKey(normalizedCandidateSpeciesId);
            var candidateSpeciesCount = totalSpeciesCounts.TryGetValue(candidateSpeciesKey, out var count)
                ? count
                : 0;
            if (candidateSpeciesCount > 0)
            {
                var moreRepresentedEntries = eligibleEntries
                    .Where(entry => entry.TotalSpeciesCount > candidateSpeciesCount)
                    .ToList();
                if (moreRepresentedEntries.Count == 0)
                {
                    var sameSpeciesEntries = eligibleEntries
                        .Where(entry => string.Equals(
                            entry.SpeciesKey,
                            candidateSpeciesKey,
                            StringComparison.OrdinalIgnoreCase))
                        .ToList();
                    if (sameSpeciesEntries.Count == 0)
                    {
                        evictionIndex = -1;
                        return false;
                    }

                    eligibleEntries = sameSpeciesEntries;
                }
                else
                {
                    eligibleEntries = moreRepresentedEntries;
                }
            }
        }

        var maxLineageFamilyCount = eligibleEntries.Max(entry => entry.TotalLineageFamilyCount);
        var narrowedEntries = eligibleEntries
            .Where(entry => entry.TotalLineageFamilyCount == maxLineageFamilyCount)
            .ToList();
        var maxSpeciesCount = narrowedEntries.Max(entry => entry.TotalSpeciesCount);
        var selectedIndex = -1;
        var eligibleCount = 0;
        foreach (var entry in narrowedEntries)
        {
            if (entry.TotalSpeciesCount != maxSpeciesCount)
            {
                continue;
            }

            eligibleCount++;
            if (eligibleCount == 1 || _random.NextInt(eligibleCount) == 0)
            {
                selectedIndex = entry.Index;
            }
        }

        evictionIndex = selectedIndex;
        return selectedIndex >= 0;
    }

    private string ResolveTrackedLineageFamilyKey(EvolutionParentRef parentRef)
    {
        if (TryBuildParentKey(parentRef, out var parentKey))
        {
            return ResolveTrackedLineageFamilyKey(parentKey);
        }

        return "(unknown)";
    }

    private string ResolveTrackedLineageFamilyKey(string? lineageFamilyOrParentKey)
    {
        var normalized = NormalizeSpeciesId(lineageFamilyOrParentKey);
        if (normalized.Length == 0)
        {
            return "(unknown)";
        }

        return _parentLineageFamilyByParentKey.TryGetValue(normalized, out var lineageFamilyId)
            ? NormalizeSpeciesId(lineageFamilyId).Length > 0
                ? NormalizeSpeciesId(lineageFamilyId)
                : "(unknown)"
            : _lineageFamilyBySpeciesId.TryGetValue(normalized, out var speciesLineageFamilyId)
                ? NormalizeSpeciesId(speciesLineageFamilyId).Length > 0
                    ? NormalizeSpeciesId(speciesLineageFamilyId)
                    : "(unknown)"
                : normalized.StartsWith("artifact:", StringComparison.OrdinalIgnoreCase)
                  || normalized.StartsWith("brain:", StringComparison.OrdinalIgnoreCase)
                    ? "(unknown)"
                    : normalized;
    }

    private string ResolveTrackedSpeciesKey(EvolutionParentRef parentRef)
    {
        if (TryBuildParentKey(parentRef, out var parentKey))
        {
            return ResolveTrackedSpeciesKey(parentKey);
        }

        return "(unknown)";
    }

    private string ResolveTrackedSpeciesKey(string? speciesOrParentKey)
    {
        var normalized = NormalizeSpeciesId(speciesOrParentKey);
        if (normalized.Length == 0)
        {
            return "(unknown)";
        }

        return _parentSpeciesByParentKey.TryGetValue(normalized, out var speciesId)
            ? NormalizeSpeciesId(speciesId).Length > 0
                ? NormalizeSpeciesId(speciesId)
                : "(unknown)"
            : normalized.StartsWith("artifact:", StringComparison.OrdinalIgnoreCase)
                || normalized.StartsWith("brain:", StringComparison.OrdinalIgnoreCase)
                ? "(unknown)"
                : normalized;
    }

    private static string NormalizeSpeciesId(string? speciesId)
    {
        return string.IsNullOrWhiteSpace(speciesId)
            ? string.Empty
            : speciesId.Trim();
    }

    private static bool TryBuildParentRefFromCandidate(
        SpeciationCommitCandidate candidate,
        EvolutionParentMode parentMode,
        out EvolutionParentRef parentRef)
    {
        if (parentMode == EvolutionParentMode.BrainIds)
        {
            if (candidate.ChildBrainId is Guid childBrainId && childBrainId != Guid.Empty)
            {
                parentRef = EvolutionParentRef.FromBrainId(childBrainId);
                return true;
            }

            if (candidate.ChildDefinition is not null)
            {
                parentRef = EvolutionParentRef.FromArtifactRef(candidate.ChildDefinition);
                return true;
            }
        }
        else if (candidate.ChildDefinition is not null)
        {
            parentRef = EvolutionParentRef.FromArtifactRef(candidate.ChildDefinition);
            return true;
        }

        parentRef = default;
        return false;
    }

    private static bool TryBuildParentKey(EvolutionParentRef parentRef, out string key)
    {
        key = string.Empty;
        if (parentRef.BrainId is Guid brainId && brainId != Guid.Empty)
        {
            key = $"brain:{brainId:D}";
            return true;
        }

        if (parentRef.ArtifactRef is { } artifactRef)
        {
            if (artifactRef.TryToSha256Hex(out var sha))
            {
                key = $"artifact:{sha}";
                return true;
            }

            var storeUri = string.IsNullOrWhiteSpace(artifactRef.StoreUri)
                ? string.Empty
                : artifactRef.StoreUri.Trim();
            if (!string.IsNullOrWhiteSpace(storeUri))
            {
                var mediaType = string.IsNullOrWhiteSpace(artifactRef.MediaType)
                    ? string.Empty
                    : artifactRef.MediaType.Trim();
                key = $"artifact-uri:{storeUri}|{mediaType}|{artifactRef.SizeBytes}";
                return true;
            }
        }

        return false;
    }

    private static bool TryBuildSeedCandidate(
        EvolutionParentRef parentRef,
        CompatibilityAssessment seedAssessment,
        out SpeciationCommitCandidate candidate)
    {
        var hasAssessedSimilarity = TryResolveSeedSimilarity(seedAssessment, out var assessedSimilarity);
        if (parentRef.BrainId is Guid brainId && brainId != Guid.Empty)
        {
            candidate = new SpeciationCommitCandidate(
                ChildBrainId: brainId,
                ChildDefinition: null,
                SimilarityScore: hasAssessedSimilarity ? assessedSimilarity : null,
                LineageSimilarityScore: hasAssessedSimilarity ? assessedSimilarity : null,
                LineageParentASimilarityScore: hasAssessedSimilarity ? 1f : null,
                LineageParentBSimilarityScore: hasAssessedSimilarity ? assessedSimilarity : null);
            return true;
        }

        if (parentRef.ArtifactRef is { } artifactRef
            && artifactRef.TryToSha256Hex(out _))
        {
            candidate = new SpeciationCommitCandidate(
                ChildBrainId: null,
                ChildDefinition: artifactRef,
                SimilarityScore: hasAssessedSimilarity ? assessedSimilarity : null,
                LineageSimilarityScore: hasAssessedSimilarity ? assessedSimilarity : null,
                LineageParentASimilarityScore: hasAssessedSimilarity ? 1f : null,
                LineageParentBSimilarityScore: hasAssessedSimilarity ? assessedSimilarity : null);
            return true;
        }

        candidate = default;
        return false;
    }

    private static bool TryResolveSeedSimilarity(
        CompatibilityAssessment seedAssessment,
        out float similarity)
    {
        similarity = 0f;
        if (!seedAssessment.Success
            || float.IsNaN(seedAssessment.SimilarityScore)
            || float.IsInfinity(seedAssessment.SimilarityScore))
        {
            return false;
        }

        similarity = Math.Clamp(seedAssessment.SimilarityScore, 0f, 1f);
        return true;
    }

    private static bool TrySelectSeedPartner(
        IReadOnlyList<EvolutionParentRef> parentPoolSnapshot,
        EvolutionParentRef parentRef,
        out EvolutionParentRef partner)
    {
        if (parentPoolSnapshot.Count == 0)
        {
            partner = default;
            return false;
        }

        if (!TryBuildParentKey(parentRef, out var seedKey))
        {
            partner = parentPoolSnapshot[0];
            return true;
        }

        foreach (var candidate in parentPoolSnapshot)
        {
            if (!TryBuildParentKey(candidate, out var candidateKey))
            {
                continue;
            }

            if (!string.Equals(seedKey, candidateKey, StringComparison.OrdinalIgnoreCase))
            {
                partner = candidate;
                return true;
            }
        }

        partner = parentPoolSnapshot[0];
        return true;
    }
}
