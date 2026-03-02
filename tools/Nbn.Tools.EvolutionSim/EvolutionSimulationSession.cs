using Nbn.Proto;
using Nbn.Shared;

namespace Nbn.Tools.EvolutionSim;

public sealed class EvolutionSimulationSession
{
    private readonly EvolutionSimulationOptions _options;
    private readonly IEvolutionSimulationClient _client;
    private readonly object _gate = new();
    private readonly List<ArtifactRef> _parentPool;
    private readonly HashSet<string> _parentPoolKeys;
    private readonly DeterministicRandom _random;

    private string _sessionId = "not-started";
    private bool _running;
    private ulong _iterations;
    private ulong _compatibilityChecks;
    private ulong _compatiblePairs;
    private ulong _reproductionCalls;
    private ulong _reproductionFailures;
    private ulong _childrenAddedToPool;
    private ulong _speciationCommitAttempts;
    private ulong _speciationCommitSuccesses;
    private string _lastFailure = string.Empty;
    private ulong _lastSeed;

    public EvolutionSimulationSession(
        EvolutionSimulationOptions options,
        IReadOnlyList<ArtifactRef> initialParents,
        IEvolutionSimulationClient client)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _random = new DeterministicRandom(options.Seed);

        if (initialParents is null || initialParents.Count < 2)
        {
            throw new ArgumentException("At least two parent artifact references are required.", nameof(initialParents));
        }

        _parentPool = new List<ArtifactRef>(initialParents.Count);
        _parentPoolKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var parent in initialParents)
        {
            _parentPool.Add(parent);
            if (TryBuildArtifactKey(parent, out var key))
            {
                _parentPoolKeys.Add(key);
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

    private async Task ExecuteIterationAsync(ArtifactRef parentA, ArtifactRef parentB, CancellationToken cancellationToken)
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

        if (!assessment.Compatible)
        {
            if (!string.IsNullOrWhiteSpace(assessment.AbortReason))
            {
                SetLastFailure(assessment.AbortReason);
            }

            return;
        }

        IncrementCompatiblePairs();

        var runCount = _options.RunPolicy.ResolveRunCount(assessment.SimilarityScore);
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

        if (!reproduction.Success || !reproduction.Compatible)
        {
            IncrementReproductionFailures();
            if (!string.IsNullOrWhiteSpace(reproduction.AbortReason))
            {
                SetLastFailure(reproduction.AbortReason);
            }

            return;
        }

        AddChildrenToPool(reproduction.ChildDefinitions);

        if (!_options.CommitToSpeciation || reproduction.CommitCandidates.Count == 0)
        {
            return;
        }

        foreach (var candidate in reproduction.CommitCandidates)
        {
            IncrementSpeciationCommitAttempts();
            var commitOutcome = await _client.CommitSpeciationAsync(
                candidate,
                parentA,
                parentB,
                cancellationToken).ConfigureAwait(false);

            if (commitOutcome.Success)
            {
                IncrementSpeciationCommitSuccesses();
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

    private bool TrySelectParents(out ArtifactRef parentA, out ArtifactRef parentB)
    {
        lock (_gate)
        {
            if (_parentPool.Count < 2)
            {
                parentA = new ArtifactRef();
                parentB = new ArtifactRef();
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

    private List<ArtifactRef> AddChildrenToPool(IReadOnlyList<ArtifactRef> children)
    {
        if (children.Count == 0)
        {
            return new List<ArtifactRef>();
        }

        var added = new List<ArtifactRef>(children.Count);
        lock (_gate)
        {
            foreach (var child in children)
            {
                if (!TryBuildArtifactKey(child, out var key))
                {
                    continue;
                }

                if (_parentPoolKeys.Contains(key))
                {
                    continue;
                }

                if (_parentPool.Count >= _options.MaxParentPoolSize)
                {
                    break;
                }

                _parentPool.Add(child);
                _parentPoolKeys.Add(key);
                _childrenAddedToPool++;
                added.Add(child);
            }
        }

        return added;
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
            _childrenAddedToPool = 0;
            _speciationCommitAttempts = 0;
            _speciationCommitSuccesses = 0;
            _lastFailure = string.Empty;
            _lastSeed = 0;
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

    private static bool TryBuildArtifactKey(ArtifactRef artifact, out string key)
    {
        key = string.Empty;
        if (!artifact.TryToSha256Hex(out var sha))
        {
            return false;
        }

        key = $"{sha}|{artifact.StoreUri}|{artifact.MediaType}|{artifact.SizeBytes}";
        return true;
    }
}
