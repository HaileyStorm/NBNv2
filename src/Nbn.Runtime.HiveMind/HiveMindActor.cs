using Nbn.Proto.Control;
using Nbn.Runtime.Brain;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Nbn.Shared.HiveMind;
using Proto;

namespace Nbn.Runtime.HiveMind;

public sealed class HiveMindActor : IActor
{
    private readonly HiveMindOptions _options;
    private readonly BackpressureController _backpressure;
    private readonly Dictionary<Guid, BrainState> _brains = new();
    private readonly HashSet<ShardKey> _pendingCompute = new();
    private readonly HashSet<Guid> _pendingDeliver = new();

    private TickState? _tick;
    private TickPhase _phase = TickPhase.Idle;
    private bool _tickLoopEnabled;
    private bool _rescheduleInProgress;
    private bool _rescheduleQueued;
    private ulong _lastRescheduleTick;
    private DateTime _lastRescheduleAt;
    private string? _queuedRescheduleReason;
    private ulong _lastCompletedTickId;

    public HiveMindActor(HiveMindOptions options)
    {
        _options = options;
        _backpressure = new BackpressureController(options);
        _tickLoopEnabled = options.AutoStart;
    }

    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
                if (_tickLoopEnabled)
                {
                    ScheduleNextTick(context, TimeSpan.Zero);
                }
                break;
            case StartTickLoop:
                _tickLoopEnabled = true;
                if (_phase == TickPhase.Idle && !_rescheduleInProgress)
                {
                    ScheduleNextTick(context, TimeSpan.Zero);
                }
                break;
            case StopTickLoop:
                _tickLoopEnabled = false;
                break;
            case TickStart:
                if (_tickLoopEnabled && !_rescheduleInProgress && _phase == TickPhase.Idle)
                {
                    StartTick(context);
                }
                break;
            case RegisterBrain message:
                RegisterBrain(context, message);
                break;
            case UpdateBrainSignalRouter message:
                UpdateBrainSignalRouter(context, message);
                break;
            case UnregisterBrain message:
                UnregisterBrain(context, message.BrainId);
                break;
            case RegisterShard message:
                RegisterShard(context, message);
                break;
            case UnregisterShard message:
                UnregisterShard(context, message);
                break;
            case PauseBrainRequest message:
                PauseBrain(context, message.BrainId, message.Reason);
                break;
            case ResumeBrainRequest message:
                ResumeBrain(message.BrainId);
                break;
            case PauseBrain message:
                if (message.BrainId.TryToGuid(out var pauseId))
                {
                    PauseBrain(context, pauseId, message.Reason);
                }
                break;
            case ResumeBrain message:
                if (message.BrainId.TryToGuid(out var resumeId))
                {
                    ResumeBrain(resumeId);
                }
                break;
            case TickComputeDone message:
                HandleTickComputeDone(context, message);
                break;
            case TickDeliverDone message:
                HandleTickDeliverDone(context, message);
                break;
            case TickPhaseTimeout message:
                HandleTickPhaseTimeout(context, message);
                break;
            case RescheduleNow message:
                BeginReschedule(context, message);
                break;
            case RescheduleCompleted message:
                CompleteReschedule(context, message);
                break;
            case GetHiveMindStatus:
                context.Respond(BuildStatus());
                break;
        }

        return Task.CompletedTask;
    }

    private void RegisterBrain(IContext context, RegisterBrain message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var brain))
        {
            brain = new BrainState(message.BrainId);
            _brains.Add(message.BrainId, brain);
        }

        if (message.BrainRootPid is not null)
        {
            brain.BrainRootPid = message.BrainRootPid;
        }

        if (message.SignalRouterPid is not null)
        {
            brain.SignalRouterPid = message.SignalRouterPid;
        }

        UpdateRoutingTable(context, brain);
    }

    private void UpdateBrainSignalRouter(IContext context, UpdateBrainSignalRouter message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var brain))
        {
            brain = new BrainState(message.BrainId);
            _brains.Add(message.BrainId, brain);
        }

        brain.SignalRouterPid = message.SignalRouterPid;
        UpdateRoutingTable(context, brain);
    }

    private void UnregisterBrain(IContext context, Guid brainId)
    {
        if (!_brains.Remove(brainId))
        {
            return;
        }

        if (_phase == TickPhase.Compute)
        {
            RemovePendingComputeForBrain(brainId);
        }

        if (_phase == TickPhase.Deliver)
        {
            if (_pendingDeliver.Remove(brainId))
            {
                MaybeCompleteDeliver(context);
            }
        }
    }

    private void RegisterShard(IContext context, RegisterShard message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var brain))
        {
            brain = new BrainState(message.BrainId);
            _brains.Add(message.BrainId, brain);
        }

        if (!ShardId32.TryFrom(message.RegionId, message.ShardIndex, out var shardId))
        {
            Log($"RegisterShard invalid shard index: brain {message.BrainId} region {message.RegionId} shardIndex {message.ShardIndex}.");
            return;
        }

        brain.Shards[shardId] = message.ShardPid;
        UpdateRoutingTable(context, brain);

        if (_phase == TickPhase.Compute && _tick is not null)
        {
            Log($"Shard registered mid-compute for brain {message.BrainId}; will start next tick.");
        }
    }

    private void UnregisterShard(IContext context, UnregisterShard message)
    {
        if (_brains.TryGetValue(message.BrainId, out var brain))
        {
            if (!ShardId32.TryFrom(message.RegionId, message.ShardIndex, out var shardId))
            {
                Log($"UnregisterShard invalid shard index: brain {message.BrainId} region {message.RegionId} shardIndex {message.ShardIndex}.");
                return;
            }

            brain.Shards.Remove(shardId);
            UpdateRoutingTable(context, brain);
        }

        if (_phase != TickPhase.Compute || _tick is null)
        {
            return;
        }

        if (ShardId32.TryFrom(message.RegionId, message.ShardIndex, out var pendingShardId)
            && _pendingCompute.Remove(new ShardKey(message.BrainId, pendingShardId)))
        {
            _tick.ExpectedComputeCount = Math.Max(_tick.CompletedComputeCount, _tick.ExpectedComputeCount - 1);
            MaybeCompleteCompute(context);
        }
    }

    private void PauseBrain(IContext context, Guid brainId, string? reason)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return;
        }

        brain.Paused = true;
        brain.PausedReason = reason;

        if (_phase == TickPhase.Compute)
        {
            RemovePendingComputeForBrain(brainId);
            MaybeCompleteCompute(context);
        }

        if (_phase == TickPhase.Deliver && _pendingDeliver.Remove(brainId))
        {
            MaybeCompleteDeliver(context);
        }
    }

    private void ResumeBrain(Guid brainId)
    {
        if (_brains.TryGetValue(brainId, out var brain))
        {
            brain.Paused = false;
            brain.PausedReason = null;
        }
    }

    private void StartTick(IContext context)
    {
        _tick = new TickState(_lastCompletedTickId + 1, DateTime.UtcNow);
        _phase = TickPhase.Compute;
        _pendingCompute.Clear();
        _pendingDeliver.Clear();

        _tick.ComputeStartedUtc = _tick.StartedUtc;

        foreach (var brain in _brains.Values)
        {
            if (brain.Paused || brain.Shards.Count == 0)
            {
                continue;
            }

            foreach (var (shardId, shardPid) in brain.Shards)
            {
                _pendingCompute.Add(new ShardKey(brain.BrainId, shardId));
                context.Send(
                    shardPid,
                    new TickCompute
                    {
                        TickId = _tick.TickId,
                        TargetTickHz = _backpressure.TargetTickHz
                    });
            }
        }

        _tick.ExpectedComputeCount = _pendingCompute.Count;

        if (_pendingCompute.Count == 0)
        {
            CompleteComputePhase(context);
            return;
        }

        SchedulePhaseTimeout(context, TickPhase.Compute, _tick.TickId, _options.ComputeTimeoutMs);
    }

    private void HandleTickComputeDone(IContext context, TickComputeDone message)
    {
        if (_tick is null || message.TickId != _tick.TickId || _phase != TickPhase.Compute)
        {
            if (_tick is not null && message.TickId <= _tick.TickId)
            {
                _tick.LateComputeCount++;
            }
            return;
        }

        if (!message.BrainId.TryToGuid(out var brainId) || message.ShardId is null)
        {
            return;
        }

        var shardId = message.ShardId.ToShardId32();
        var key = new ShardKey(brainId, shardId);

        if (!_pendingCompute.Remove(key))
        {
            _tick.LateComputeCount++;
            return;
        }

        _tick.CompletedComputeCount++;
        MaybeCompleteCompute(context);
    }

    private void HandleTickDeliverDone(IContext context, TickDeliverDone message)
    {
        if (_tick is null || message.TickId != _tick.TickId || _phase != TickPhase.Deliver)
        {
            if (_tick is not null && message.TickId <= _tick.TickId)
            {
                _tick.LateDeliverCount++;
            }
            return;
        }

        if (!message.BrainId.TryToGuid(out var brainId))
        {
            return;
        }

        if (!_pendingDeliver.Remove(brainId))
        {
            _tick.LateDeliverCount++;
            return;
        }

        _tick.CompletedDeliverCount++;
        MaybeCompleteDeliver(context);
    }

    private void HandleTickPhaseTimeout(IContext context, TickPhaseTimeout message)
    {
        if (_tick is null || message.TickId != _tick.TickId || _phase != message.Phase)
        {
            return;
        }

        switch (message.Phase)
        {
            case TickPhase.Compute:
                _tick.ComputeTimedOut = true;
                _pendingCompute.Clear();
                CompleteComputePhase(context);
                break;
            case TickPhase.Deliver:
                _tick.DeliverTimedOut = true;
                _pendingDeliver.Clear();
                CompleteTick(context);
                break;
        }
    }

    private void MaybeCompleteCompute(IContext context)
    {
        if (_pendingCompute.Count == 0)
        {
            CompleteComputePhase(context);
        }
    }

    private void CompleteComputePhase(IContext context)
    {
        if (_tick is null || _phase != TickPhase.Compute)
        {
            return;
        }

        _tick.ComputeCompletedUtc = DateTime.UtcNow;
        _phase = TickPhase.Deliver;
        _tick.DeliverStartedUtc = DateTime.UtcNow;

        foreach (var brain in _brains.Values)
        {
            if (brain.Paused || brain.Shards.Count == 0)
            {
                continue;
            }

            var deliverTarget = brain.BrainRootPid ?? brain.SignalRouterPid;
            if (deliverTarget is null)
            {
                continue;
            }

            _pendingDeliver.Add(brain.BrainId);
            context.Send(deliverTarget, new TickDeliver { TickId = _tick.TickId });
        }

        _tick.ExpectedDeliverCount = _pendingDeliver.Count;

        if (_pendingDeliver.Count == 0)
        {
            CompleteTick(context);
            return;
        }

        SchedulePhaseTimeout(context, TickPhase.Deliver, _tick.TickId, _options.DeliverTimeoutMs);
    }

    private void MaybeCompleteDeliver(IContext context)
    {
        if (_pendingDeliver.Count == 0)
        {
            CompleteTick(context);
        }
    }

    private void CompleteTick(IContext context)
    {
        if (_tick is null)
        {
            _phase = TickPhase.Idle;
            return;
        }

        _tick.DeliverCompletedUtc = DateTime.UtcNow;
        _phase = TickPhase.Idle;

        var outcome = new TickOutcome(
            _tick.TickId,
            SafeDuration(_tick.ComputeStartedUtc, _tick.ComputeCompletedUtc),
            SafeDuration(_tick.DeliverStartedUtc, _tick.DeliverCompletedUtc),
            _tick.ComputeTimedOut,
            _tick.DeliverTimedOut,
            _tick.LateComputeCount,
            _tick.LateDeliverCount,
            _tick.ExpectedComputeCount,
            _tick.CompletedComputeCount,
            _tick.ExpectedDeliverCount,
            _tick.CompletedDeliverCount);

        var elapsed = DateTime.UtcNow - _tick.StartedUtc;
        var completedTickId = _tick.TickId;
        _tick = null;
        _lastCompletedTickId = completedTickId;

        HiveMindTelemetry.RecordTickOutcome(outcome, _backpressure.TargetTickHz);

        var decision = _backpressure.Evaluate(outcome);

        if (decision.RequestReschedule)
        {
            RequestReschedule(context, decision.Reason);
            HiveMindTelemetry.RecordReschedule(decision.Reason);
        }

        if (decision.RequestPause)
        {
            PauseAllBrains(context, decision.Reason);
            HiveMindTelemetry.RecordPause(decision.Reason);
        }

        ScheduleNextTick(context, ComputeTickDelay(elapsed, decision.TargetTickHz));
    }

    private void ScheduleNextTick(IContext context, TimeSpan delay)
    {
        if (!_tickLoopEnabled || _rescheduleInProgress)
        {
            return;
        }

        ScheduleSelf(context, delay, new TickStart());
    }

    private void SchedulePhaseTimeout(IContext context, TickPhase phase, ulong tickId, int timeoutMs)
    {
        if (timeoutMs <= 0)
        {
            return;
        }

        ScheduleSelf(context, TimeSpan.FromMilliseconds(timeoutMs), new TickPhaseTimeout(tickId, phase));
    }

    private void RemovePendingComputeForBrain(Guid brainId)
    {
        if (_pendingCompute.Count == 0)
        {
            return;
        }

        var removeKeys = new List<ShardKey>();
        foreach (var key in _pendingCompute)
        {
            if (key.BrainId == brainId)
            {
                removeKeys.Add(key);
            }
        }

        if (_tick is null)
        {
            foreach (var key in removeKeys)
            {
                _pendingCompute.Remove(key);
            }

            return;
        }

        foreach (var key in removeKeys)
        {
            if (_pendingCompute.Remove(key))
            {
                _tick.ExpectedComputeCount = Math.Max(_tick.CompletedComputeCount, _tick.ExpectedComputeCount - 1);
            }
        }
    }

    private void PauseAllBrains(IContext context, string reason)
    {
        foreach (var brain in _brains.Values)
        {
            brain.Paused = true;
            brain.PausedReason = reason;
        }

        if (_phase == TickPhase.Compute)
        {
            _pendingCompute.Clear();
            MaybeCompleteCompute(context);
        }

        if (_phase == TickPhase.Deliver)
        {
            _pendingDeliver.Clear();
            MaybeCompleteDeliver(context);
        }

        Log($"Paused all brains: {reason}");
    }

    private void RequestReschedule(IContext context, string reason)
    {
        if (_rescheduleInProgress)
        {
            _rescheduleQueued = true;
            _queuedRescheduleReason ??= reason;
            return;
        }

        var now = DateTime.UtcNow;
        if (_lastRescheduleTick > 0 && (_lastCompletedTickId - _lastRescheduleTick) < (ulong)_options.RescheduleMinTicks)
        {
            _rescheduleQueued = true;
            _queuedRescheduleReason ??= reason;
            return;
        }

        if (_lastRescheduleAt != default && now - _lastRescheduleAt < TimeSpan.FromMinutes(_options.RescheduleMinMinutes))
        {
            _rescheduleQueued = true;
            _queuedRescheduleReason ??= reason;
            return;
        }

        _rescheduleInProgress = true;
        _lastRescheduleAt = now;
        _lastRescheduleTick = _lastCompletedTickId;

        ScheduleSelf(context, TimeSpan.FromMilliseconds(_options.RescheduleQuietMs), new RescheduleNow(reason));
    }

    private void BeginReschedule(IContext context, RescheduleNow message)
    {
        Log($"Reschedule started: {message.Reason}");
        ScheduleSelf(
            context,
            TimeSpan.FromMilliseconds(_options.RescheduleSimulatedMs),
            new RescheduleCompleted(message.Reason, true));
    }

    private void CompleteReschedule(IContext context, RescheduleCompleted message)
    {
        _rescheduleInProgress = false;
        Log($"Reschedule completed: {message.Reason} (success={message.Success})");

        if (_rescheduleQueued)
        {
            _rescheduleQueued = false;
            var queuedReason = _queuedRescheduleReason ?? "queued";
            _queuedRescheduleReason = null;
            RequestReschedule(context, queuedReason);
            return;
        }

        if (_tickLoopEnabled && _phase == TickPhase.Idle)
        {
            ScheduleNextTick(context, TimeSpan.Zero);
        }
    }

    private static TimeSpan SafeDuration(DateTime start, DateTime end)
    {
        if (start == default || end == default || end < start)
        {
            return TimeSpan.Zero;
        }

        return end - start;
    }

    private static TimeSpan ComputeTickDelay(TimeSpan elapsed, float targetTickHz)
    {
        if (targetTickHz <= 0)
        {
            return TimeSpan.Zero;
        }

        var period = TimeSpan.FromSeconds(1d / targetTickHz);
        var delay = period - elapsed;
        return delay <= TimeSpan.Zero ? TimeSpan.Zero : delay;
    }

    private static void Log(string message)
        => Console.WriteLine($"[{DateTime.UtcNow:O}] [HiveMind] {message}");

    private HiveMindStatus BuildStatus()
        => new(
            _lastCompletedTickId,
            _tickLoopEnabled,
            _backpressure.TargetTickHz,
            _pendingCompute.Count,
            _pendingDeliver.Count,
            _rescheduleInProgress,
            _brains.Count,
            _brains.Values.Sum(brain => brain.Shards.Count));

    private void UpdateRoutingTable(IContext? context, BrainState brain)
    {
        var snapshot = RoutingTableSnapshot.Empty;
        if (brain.Shards.Count > 0)
        {
            var routes = new List<ShardRoute>(brain.Shards.Count);
            foreach (var entry in brain.Shards)
            {
                routes.Add(new ShardRoute(entry.Key, entry.Value));
            }

            snapshot = new RoutingTableSnapshot(routes);
        }

        brain.RoutingSnapshot = snapshot;

        if (context is null)
        {
            return;
        }

        var target = brain.BrainRootPid ?? brain.SignalRouterPid;
        if (target is null)
        {
            return;
        }

        context.Send(target, new SetRoutingTable(brain.RoutingSnapshot));
    }

    private static void ScheduleSelf(IContext context, TimeSpan delay, object message)
    {
        if (delay <= TimeSpan.Zero)
        {
            context.Send(context.Self, message);
            return;
        }

        context.ReenterAfter(Task.Delay(delay), _ =>
        {
            context.Send(context.Self, message);
            return Task.CompletedTask;
        });
    }

    private sealed record TickStart;
    private sealed record TickPhaseTimeout(ulong TickId, TickPhase Phase);
    private sealed record RescheduleNow(string Reason);
    private sealed record RescheduleCompleted(string Reason, bool Success);

    private enum TickPhase
    {
        Idle,
        Compute,
        Deliver
    }

    private sealed class TickState
    {
        public TickState(ulong tickId, DateTime startedUtc)
        {
            TickId = tickId;
            StartedUtc = startedUtc;
        }

        public ulong TickId { get; }
        public DateTime StartedUtc { get; }
        public DateTime ComputeStartedUtc { get; set; }
        public DateTime ComputeCompletedUtc { get; set; }
        public DateTime DeliverStartedUtc { get; set; }
        public DateTime DeliverCompletedUtc { get; set; }
        public bool ComputeTimedOut { get; set; }
        public bool DeliverTimedOut { get; set; }
        public int ExpectedComputeCount { get; set; }
        public int CompletedComputeCount { get; set; }
        public int ExpectedDeliverCount { get; set; }
        public int CompletedDeliverCount { get; set; }
        public int LateComputeCount { get; set; }
        public int LateDeliverCount { get; set; }
    }

    private sealed class BrainState
    {
        public BrainState(Guid brainId)
        {
            BrainId = brainId;
        }

        public Guid BrainId { get; }
        public PID? BrainRootPid { get; set; }
        public PID? SignalRouterPid { get; set; }
        public bool Paused { get; set; }
        public string? PausedReason { get; set; }
        public Dictionary<ShardId32, PID> Shards { get; } = new();
        public RoutingTableSnapshot RoutingSnapshot { get; set; } = RoutingTableSnapshot.Empty;
    }

    private readonly record struct ShardKey(Guid BrainId, ShardId32 ShardId);
}
