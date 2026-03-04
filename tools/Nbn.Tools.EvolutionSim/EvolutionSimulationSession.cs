using Nbn.Proto;
using Nbn.Shared;

namespace Nbn.Tools.EvolutionSim;

public sealed class EvolutionSimulationSession
{
    private const ulong CommitSimilarityPlateauWindowSamples = 64;
    private const float MaxRunPressureNudge = 0.35f;
    private const float RunPressureNudgeStep = 0.05f;
    private const float RunPressureRecoveryStep = 0.10f;

    private readonly EvolutionSimulationOptions _options;
    private readonly IEvolutionSimulationClient _client;
    private readonly object _gate = new();
    private readonly List<EvolutionParentRef> _parentPool;
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
        _parentPoolKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        _protectedParentPoolKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var parent in initialParents)
        {
            _parentPool.Add(parent);
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
        foreach (var parent in snapshot)
        {
            if (!TryBuildSeedCandidate(parent, out var candidate))
            {
                continue;
            }

            if (TryBuildParentKey(parent, out var seedKey)
                && !seededKeys.Add(seedKey))
            {
                continue;
            }

            if (!TrySelectSeedPartner(snapshot, parent, out var seedPartner))
            {
                continue;
            }

            var commitOutcome = await _client.CommitSpeciationAsync(
                candidate,
                parent,
                seedPartner,
                cancellationToken).ConfigureAwait(false);

            if (!commitOutcome.Success
                && !commitOutcome.ExpectedNoOp
                && !string.IsNullOrWhiteSpace(commitOutcome.FailureDetail))
            {
                SetLastFailure($"seed_parent_commit_failed:{commitOutcome.FailureDetail}");
            }
        }
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

        AddChildrenToPool(reproduction);

        if (!_options.CommitToSpeciation || reproduction.CommitCandidates.Count == 0)
        {
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
                RecordSpeciationCommitSimilarity(commitCandidate.SimilarityScore);
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

            var parentAIndex = _random.NextInt(_parentPool.Count);
            var parentBIndex = _random.NextInt(_parentPool.Count - 1);
            if (parentBIndex >= parentAIndex)
            {
                parentBIndex++;
            }

            parentA = _parentPool[parentAIndex];
            parentB = _parentPool[parentBIndex];
            return true;
        }
    }

    private int AddChildrenToPool(ReproductionOutcome reproduction)
    {
        if (_options.ParentMode == EvolutionParentMode.BrainIds)
        {
            return AddSpawnedChildrenToBrainPool(reproduction.CommitCandidates);
        }

        return AddChildrenToArtifactPool(reproduction.ChildDefinitions);
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
            return Math.Clamp(normalized - _runPressureNudge, 0f, 1f);
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

    // Caller must hold _gate.
    private bool TryAddParentToPoolAtCapacity(EvolutionParentRef candidate, string candidateKey)
    {
        if (_parentPool.Count < _options.MaxParentPoolSize)
        {
            _parentPool.Add(candidate);
            _parentPoolKeys.Add(candidateKey);
            return true;
        }

        if (!TrySelectEvictionIndex(out var evictionIndex))
        {
            return false;
        }

        var evicted = _parentPool[evictionIndex];
        if (TryBuildParentKey(evicted, out var evictedKey))
        {
            _parentPoolKeys.Remove(evictedKey);
        }

        _parentPool[evictionIndex] = candidate;
        _parentPoolKeys.Add(candidateKey);
        return true;
    }

    // Caller must hold _gate.
    private bool TrySelectEvictionIndex(out int evictionIndex)
    {
        var selectedIndex = -1;
        var eligibleCount = 0;
        for (var i = 0; i < _parentPool.Count; i++)
        {
            var current = _parentPool[i];
            if (TryBuildParentKey(current, out var currentKey)
                && _protectedParentPoolKeys.Contains(currentKey))
            {
                continue;
            }

            eligibleCount++;
            if (eligibleCount == 1 || _random.NextInt(eligibleCount) == 0)
            {
                selectedIndex = i;
            }
        }

        evictionIndex = selectedIndex;
        return selectedIndex >= 0;
    }

    private static bool TryBuildParentKey(EvolutionParentRef parentRef, out string key)
    {
        key = string.Empty;
        if (parentRef.BrainId is Guid brainId && brainId != Guid.Empty)
        {
            key = $"brain:{brainId:D}";
            return true;
        }

        if (parentRef.ArtifactRef is not null && parentRef.ArtifactRef.TryToSha256Hex(out var sha))
        {
            key = $"artifact:{sha}|{parentRef.ArtifactRef.StoreUri}|{parentRef.ArtifactRef.MediaType}|{parentRef.ArtifactRef.SizeBytes}";
            return true;
        }

        return false;
    }

    private static bool TryBuildSeedCandidate(EvolutionParentRef parentRef, out SpeciationCommitCandidate candidate)
    {
        if (parentRef.BrainId is Guid brainId && brainId != Guid.Empty)
        {
            candidate = new SpeciationCommitCandidate(
                ChildBrainId: brainId,
                ChildDefinition: null,
                SimilarityScore: 1f,
                FunctionScore: 1f,
                ConnectivityScore: 1f,
                RegionSpanScore: 1f);
            return true;
        }

        if (parentRef.ArtifactRef is { } artifactRef
            && artifactRef.TryToSha256Hex(out _))
        {
            candidate = new SpeciationCommitCandidate(
                ChildBrainId: null,
                ChildDefinition: artifactRef,
                SimilarityScore: 1f,
                FunctionScore: 1f,
                ConnectivityScore: 1f,
                RegionSpanScore: 1f);
            return true;
        }

        candidate = default;
        return false;
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
