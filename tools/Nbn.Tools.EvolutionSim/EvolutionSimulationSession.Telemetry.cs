using Nbn.Proto;
using Nbn.Shared;

namespace Nbn.Tools.EvolutionSim;

public sealed partial class EvolutionSimulationSession
{
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
}
