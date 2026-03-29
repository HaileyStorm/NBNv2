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
    private const int RecentTickWindow = 64;

    private readonly HiveMindOptions _options;
    private readonly float _defaultTargetTickHz;
    private readonly float _defaultMinTickHz;
    private readonly Queue<RecentTickHealth> _recentTicks = new();
    private float _targetTickHz;
    private int _timeoutStreak;
    private int _lateStreak;
    private int _recentTimeoutTickCount;
    private int _recentLateTickCount;
    private float? _tickRateOverrideHz;

    public BackpressureController(HiveMindOptions options)
    {
        _options = options;
        _defaultTargetTickHz = options.TargetTickHz;
        _defaultMinTickHz = options.MinTickHz;
        _targetTickHz = options.TargetTickHz;
    }

    public float TargetTickHz => _targetTickHz;
    public float ConfiguredTargetTickHz => _defaultTargetTickHz;
    public int TimeoutStreak => _timeoutStreak;
    public bool HasTickRateOverride => _tickRateOverrideHz.HasValue;
    public float TickRateOverrideHz => _tickRateOverrideHz ?? 0f;
    public int RecentTickSampleCount => _recentTicks.Count;
    public int RecentTimeoutTickCount => _recentTimeoutTickCount;
    public int RecentLateTickCount => _recentLateTickCount;
    public bool AutomaticBackpressureActive => _targetTickHz + 0.001f < ControlTargetTickHz();

    public bool TrySetTickRateOverride(float? targetTickHz, out string message)
    {
        if (!targetTickHz.HasValue)
        {
            _tickRateOverrideHz = null;
            _targetTickHz = Math.Clamp(_targetTickHz, CurrentMinTickHz(), CurrentMaxTickHz());
            _timeoutStreak = 0;
            _lateStreak = 0;
            message = "Tick-rate override cleared.";
            return true;
        }

        var requested = targetTickHz.Value;
        if (!float.IsFinite(requested) || requested <= 0f)
        {
            message = "Tick-rate override must be a finite value greater than zero.";
            return false;
        }

        _tickRateOverrideHz = requested;
        _targetTickHz = requested;
        _timeoutStreak = 0;
        _lateStreak = 0;
        message = $"Tick-rate override set to {requested:0.###} Hz.";
        return true;
    }

    public BackpressureDecision Evaluate(TickOutcome outcome)
    {
        var minTickHz = CurrentMinTickHz();
        var maxTickHz = CurrentMaxTickHz();
        var timedOut = outcome.ComputeTimedOut || outcome.DeliverTimedOut;
        var lateDetected = outcome.LateComputeCount > 0 || outcome.LateDeliverCount > 0;

        RecordRecentTickHealth(
            timedOut
                ? RecentTickHealth.Timeout
                : lateDetected
                    ? RecentTickHealth.Late
                    : RecentTickHealth.Healthy);

        if (timedOut)
        {
            _timeoutStreak++;
            _lateStreak = 0;
            _targetTickHz = MathF.Max(minTickHz, _targetTickHz * _options.BackpressureDecay);
        }
        else if (lateDetected)
        {
            _timeoutStreak = 0;
            _lateStreak++;
            if (_lateStreak >= _options.LateBackpressureThreshold)
            {
                _targetTickHz = MathF.Max(minTickHz, _targetTickHz * _options.BackpressureDecay);
            }
        }
        else
        {
            _timeoutStreak = 0;
            _lateStreak = 0;
            if (_targetTickHz < maxTickHz)
            {
                _targetTickHz = MathF.Min(maxTickHz, _targetTickHz * _options.BackpressureRecovery);
            }
        }

        var requestReschedule = timedOut && _timeoutStreak >= _options.TimeoutRescheduleThreshold;
        var requestPause = timedOut && _timeoutStreak >= _options.TimeoutPauseThreshold;

        var reason = timedOut
            ? $"tick {outcome.TickId} timed out"
            : lateDetected
                ? $"tick {outcome.TickId} late arrivals (compute={outcome.LateComputeCount}, deliver={outcome.LateDeliverCount})"
                : $"tick {outcome.TickId} healthy";

        if (requestPause)
        {
            reason += $" (pause after {_timeoutStreak} timeouts)";
        }
        else if (requestReschedule)
        {
            reason += $" (reschedule after {_timeoutStreak} timeouts)";
        }
        else if (lateDetected && _lateStreak >= _options.LateBackpressureThreshold)
        {
            reason += $" (backpressure after {_lateStreak} late ticks)";
        }

        return new BackpressureDecision(_targetTickHz, requestReschedule, requestPause, _timeoutStreak, reason);
    }

    private float CurrentMinTickHz()
        => _tickRateOverrideHz.HasValue
            ? MathF.Min(_defaultMinTickHz, _tickRateOverrideHz.Value)
            : _defaultMinTickHz;

    private float ControlTargetTickHz()
        => _tickRateOverrideHz ?? _defaultTargetTickHz;

    private float CurrentMaxTickHz()
        => ControlTargetTickHz();

    private void RecordRecentTickHealth(RecentTickHealth health)
    {
        _recentTicks.Enqueue(health);
        switch (health)
        {
            case RecentTickHealth.Timeout:
                _recentTimeoutTickCount++;
                break;
            case RecentTickHealth.Late:
                _recentLateTickCount++;
                break;
        }

        while (_recentTicks.Count > RecentTickWindow)
        {
            switch (_recentTicks.Dequeue())
            {
                case RecentTickHealth.Timeout:
                    _recentTimeoutTickCount = Math.Max(0, _recentTimeoutTickCount - 1);
                    break;
                case RecentTickHealth.Late:
                    _recentLateTickCount = Math.Max(0, _recentLateTickCount - 1);
                    break;
            }
        }
    }

    private enum RecentTickHealth
    {
        Healthy = 0,
        Late = 1,
        Timeout = 2
    }
}
