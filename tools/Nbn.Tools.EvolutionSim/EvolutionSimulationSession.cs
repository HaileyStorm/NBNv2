using Nbn.Proto;
using Nbn.Shared;

namespace Nbn.Tools.EvolutionSim;

/// <summary>
/// Runs deterministic reproduction/speciation simulation loops against an evolution runtime client.
/// </summary>
public sealed partial class EvolutionSimulationSession
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

    /// <summary>
    /// Initializes a new simulation session with the provided options, initial parents, and runtime client.
    /// </summary>
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

    /// <summary>
    /// Returns the current observable simulation status snapshot.
    /// </summary>
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

    /// <summary>
    /// Executes the session until cancellation or the configured iteration limit is reached.
    /// </summary>
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
}
