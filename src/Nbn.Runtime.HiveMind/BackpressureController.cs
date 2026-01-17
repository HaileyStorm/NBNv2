namespace Nbn.Runtime.HiveMind;

public sealed record TickOutcome(
    ulong TickId,
    TimeSpan ComputeDuration,
    TimeSpan DeliverDuration,
    bool ComputeTimedOut,
    bool DeliverTimedOut,
    int LateComputeCount,
    int LateDeliverCount,
    int ExpectedComputeCount,
    int CompletedComputeCount,
    int ExpectedDeliverCount,
    int CompletedDeliverCount);

public sealed record BackpressureDecision(
    float TargetTickHz,
    bool RequestReschedule,
    bool RequestPause,
    int TimeoutStreak,
    string Reason);

public sealed class BackpressureController
{
    private readonly HiveMindOptions _options;
    private float _targetTickHz;
    private int _timeoutStreak;

    public BackpressureController(HiveMindOptions options)
    {
        _options = options;
        _targetTickHz = options.TargetTickHz;
    }

    public float TargetTickHz => _targetTickHz;
    public int TimeoutStreak => _timeoutStreak;

    public BackpressureDecision Evaluate(TickOutcome outcome)
    {
        var timedOut = outcome.ComputeTimedOut || outcome.DeliverTimedOut;

        if (timedOut)
        {
            _timeoutStreak++;
            _targetTickHz = MathF.Max(_options.MinTickHz, _targetTickHz * _options.BackpressureDecay);
        }
        else
        {
            _timeoutStreak = 0;
            if (_targetTickHz < _options.TargetTickHz)
            {
                _targetTickHz = MathF.Min(_options.TargetTickHz, _targetTickHz * _options.BackpressureRecovery);
            }
        }

        var requestReschedule = timedOut && _timeoutStreak >= _options.TimeoutRescheduleThreshold;
        var requestPause = timedOut && _timeoutStreak >= _options.TimeoutPauseThreshold;

        var reason = timedOut
            ? $"tick {outcome.TickId} timed out"
            : $"tick {outcome.TickId} healthy";

        if (requestPause)
        {
            reason += $" (pause after {_timeoutStreak} timeouts)";
        }
        else if (requestReschedule)
        {
            reason += $" (reschedule after {_timeoutStreak} timeouts)";
        }

        return new BackpressureDecision(_targetTickHz, requestReschedule, requestPause, _timeoutStreak, reason);
    }
}
