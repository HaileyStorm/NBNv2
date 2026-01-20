using System.Security.Cryptography;
using System.Text;
using Nbn.Runtime.Brain;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Nbn.Shared.HiveMind;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed class HiveMindActor : IActor
{
    private readonly HiveMindOptions _options;
    private readonly BackpressureController _backpressure;
    private readonly PID? _settingsPid;
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
        _settingsPid = BuildSettingsPid(options);
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
            case ProtoControl.RegisterBrain message:
                HandleRegisterBrain(context, message);
                break;
            case ProtoControl.UpdateBrainSignalRouter message:
                HandleUpdateBrainSignalRouter(context, message);
                break;
            case ProtoControl.UnregisterBrain message:
                HandleUnregisterBrain(context, message);
                break;
            case ProtoControl.RegisterShard message:
                HandleRegisterShard(context, message);
                break;
            case ProtoControl.UnregisterShard message:
                HandleUnregisterShard(context, message);
                break;
            case ProtoControl.RegisterOutputSink message:
                HandleRegisterOutputSink(context, message);
                break;
            case ProtoControl.GetBrainIoInfo message:
                if (message.BrainId is not null && message.BrainId.TryToGuid(out var ioBrainId))
                {
                    context.Respond(BuildBrainIoInfo(ioBrainId));
                }
                else
                {
                    context.Respond(new ProtoControl.BrainIoInfo());
                }
                break;
            case PauseBrainRequest message:
                PauseBrain(context, message.BrainId, message.Reason);
                break;
            case ResumeBrainRequest message:
                ResumeBrain(context, message.BrainId);
                break;
            case ProtoControl.PauseBrain message:
                if (message.BrainId.TryToGuid(out var pauseId))
                {
                    PauseBrain(context, pauseId, message.Reason);
                }
                break;
            case ProtoControl.ResumeBrain message:
                if (message.BrainId.TryToGuid(out var resumeId))
                {
                    ResumeBrain(context, resumeId);
                }
                break;
            case ProtoControl.TickComputeDone message:
                HandleTickComputeDone(context, message);
                break;
            case ProtoControl.TickDeliverDone message:
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
            case ProtoControl.GetHiveMindStatus:
                context.Respond(BuildStatus());
                break;
            case GetBrainRouting message:
                context.Respond(BuildRoutingInfo(message.BrainId));
                break;
            case ProtoControl.GetBrainRouting message:
                if (message.BrainId is not null && message.BrainId.TryToGuid(out var routingBrainId))
                {
                    context.Respond(BuildRoutingInfoProto(routingBrainId));
                }
                else
                {
                    context.Respond(new ProtoControl.BrainRoutingInfo());
                }
                break;
        }

        return Task.CompletedTask;
    }

    private void RegisterBrainInternal(IContext context, Guid brainId, PID? brainRootPid, PID? routerPid)
    {
        var isNew = !_brains.TryGetValue(brainId, out var brain) || brain is null;
        if (isNew)
        {
            brain = new BrainState(brainId)
            {
                SpawnedMs = NowMs()
            };
            _brains[brainId] = brain;
        }

        brainRootPid = NormalizePid(context, brainRootPid);
        routerPid = NormalizePid(context, routerPid);

        if (routerPid is not null && string.IsNullOrWhiteSpace(routerPid.Address))
        {
            var fallbackAddress = brainRootPid?.Address;
            if (!string.IsNullOrWhiteSpace(fallbackAddress))
            {
                routerPid = new PID(fallbackAddress, routerPid.Id);
            }
        }

        if (brainRootPid is not null && string.IsNullOrWhiteSpace(brainRootPid.Address))
        {
            var fallbackAddress = routerPid?.Address;
            if (!string.IsNullOrWhiteSpace(fallbackAddress))
            {
                brainRootPid = new PID(fallbackAddress, brainRootPid.Id);
            }
        }

        var brainState = brain ?? throw new InvalidOperationException("Brain state was not initialized.");

        if (brainRootPid is not null)
        {
            brainState.BrainRootPid = brainRootPid;
        }

        if (routerPid is not null)
        {
            brainState.SignalRouterPid = routerPid;
        }

        UpdateRoutingTable(context, brainState);

        ReportBrainRegistration(context, brainState);
    }

    private void UpdateBrainSignalRouter(IContext context, Guid brainId, PID routerPid)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            brain = new BrainState(brainId);
            _brains.Add(brainId, brain);
        }

        routerPid = NormalizePid(context, routerPid) ?? routerPid;
        if (routerPid.Address.Length == 0 && brain.BrainRootPid is not null && brain.BrainRootPid.Address.Length > 0)
        {
            routerPid = new PID(brain.BrainRootPid.Address, routerPid.Id);
        }

        brain.SignalRouterPid = routerPid;
        UpdateRoutingTable(context, brain);
    }

    private void UnregisterBrain(IContext context, Guid brainId)
    {
        if (!_brains.Remove(brainId))
        {
            return;
        }

        ReportBrainUnregistered(context, brainId);

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

    private void RegisterShardInternal(IContext context, Guid brainId, int regionId, int shardIndex, PID shardPid, int neuronStart, int neuronCount)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            brain = new BrainState(brainId);
            _brains.Add(brainId, brain);
        }

        if (!ShardId32.TryFrom(regionId, shardIndex, out var shardId))
        {
            Log($"RegisterShard invalid shard index: brain {brainId} region {regionId} shardIndex {shardIndex}.");
            return;
        }

        var normalized = NormalizePid(context, shardPid) ?? shardPid;
        brain.Shards[shardId] = normalized;
        UpdateRoutingTable(context, brain);

        if (neuronCount > 0)
        {
            var span = neuronStart + neuronCount;
            if (regionId == NbnConstants.InputRegionId && span > brain.InputWidth)
            {
                brain.InputWidth = span;
            }

            if (regionId == NbnConstants.OutputRegionId && span > brain.OutputWidth)
            {
                brain.OutputWidth = span;
            }
        }

        if (regionId == NbnConstants.OutputRegionId && brain.OutputSinkPid is not null)
        {
            SendOutputSinkUpdate(context, brainId, shardId, normalized, brain.OutputSinkPid);
            Log($"Output shard registered; pushed sink for brain {brainId} shard {shardId}");
        }

        if (_phase == TickPhase.Compute && _tick is not null)
        {
            Log($"Shard registered mid-compute for brain {brainId}; will start next tick.");
        }
    }

    private void UnregisterShardInternal(IContext context, Guid brainId, int regionId, int shardIndex)
    {
        if (_brains.TryGetValue(brainId, out var brain))
        {
            if (!ShardId32.TryFrom(regionId, shardIndex, out var shardId))
            {
                Log($"UnregisterShard invalid shard index: brain {brainId} region {regionId} shardIndex {shardIndex}.");
                return;
            }

            brain.Shards.Remove(shardId);
            UpdateRoutingTable(context, brain);
        }

        if (_phase != TickPhase.Compute || _tick is null)
        {
            return;
        }

        if (ShardId32.TryFrom(regionId, shardIndex, out var pendingShardId)
            && _pendingCompute.Remove(new ShardKey(brainId, pendingShardId)))
        {
            _tick.ExpectedComputeCount = Math.Max(_tick.CompletedComputeCount, _tick.ExpectedComputeCount - 1);
            MaybeCompleteCompute(context);
        }
    }

    private void HandleRegisterBrain(IContext context, ProtoControl.RegisterBrain message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        var brainRootPid = ParsePid(message.BrainRootPid);
        var routerPid = ParsePid(message.SignalRouterPid);
        RegisterBrainInternal(context, brainId, brainRootPid, routerPid);
    }

    private void HandleUpdateBrainSignalRouter(IContext context, ProtoControl.UpdateBrainSignalRouter message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        var routerPid = ParsePid(message.SignalRouterPid);
        if (routerPid is null)
        {
            return;
        }

        UpdateBrainSignalRouter(context, brainId, routerPid);
    }

    private void HandleUnregisterBrain(IContext context, ProtoControl.UnregisterBrain message)
    {
        if (TryGetGuid(message.BrainId, out var brainId))
        {
            UnregisterBrain(context, brainId);
        }
    }

    private void HandleRegisterShard(IContext context, ProtoControl.RegisterShard message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        var shardPid = ParsePid(message.ShardPid);
        if (shardPid is null)
        {
            return;
        }

        RegisterShardInternal(
            context,
            brainId,
            (int)message.RegionId,
            (int)message.ShardIndex,
            shardPid,
            (int)message.NeuronStart,
            (int)message.NeuronCount);
    }

    private void HandleUnregisterShard(IContext context, ProtoControl.UnregisterShard message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        UnregisterShardInternal(context, brainId, (int)message.RegionId, (int)message.ShardIndex);
    }

    private void HandleRegisterOutputSink(IContext context, ProtoControl.RegisterOutputSink message)
    {
        if (!TryGetGuid(message.BrainId, out var brainId))
        {
            return;
        }

        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return;
        }

        if (!TryParsePid(message.OutputPid, out var outputPid))
        {
            return;
        }

        brain.OutputSinkPid = outputPid;
        UpdateOutputSinks(context, brain);
        Log($"Output sink registered for brain {brainId}: {PidLabel(outputPid)}");
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

        ReportBrainState(context, brainId, "Paused", reason);
    }

    private void ResumeBrain(IContext context, Guid brainId)
    {
        if (_brains.TryGetValue(brainId, out var brain))
        {
            brain.Paused = false;
            brain.PausedReason = null;
            ReportBrainState(context, brainId, "Active", null);
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

            if (brain.RoutingSnapshot.Count == 0)
            {
                LogError($"Routing snapshot missing for brain {brain.BrainId} with {brain.Shards.Count} shard(s).");
            }

            var computeTarget = brain.BrainRootPid ?? brain.SignalRouterPid;
            if (computeTarget is null)
            {
                LogError($"TickCompute skipped: missing BrainRoot/SignalRouter PID for brain {brain.BrainId}.");
                continue;
            }

            foreach (var shardId in brain.Shards.Keys)
            {
                _pendingCompute.Add(new ShardKey(brain.BrainId, shardId));
            }

            context.Send(
                computeTarget,
                new ProtoControl.TickCompute
                {
                    TickId = _tick.TickId,
                    TargetTickHz = _backpressure.TargetTickHz
                });
        }

        _tick.ExpectedComputeCount = _pendingCompute.Count;

        if (_pendingCompute.Count == 0)
        {
            CompleteComputePhase(context);
            return;
        }

        SchedulePhaseTimeout(context, TickPhase.Compute, _tick.TickId, _options.ComputeTimeoutMs);
    }

    private void HandleTickComputeDone(IContext context, ProtoControl.TickComputeDone message)
    {
        if (_tick is null)
        {
            if (message.TickId <= _lastCompletedTickId)
            {
                HiveMindTelemetry.RecordLateComputeAfterCompletion();
            }
            return;
        }

        if (message.TickId != _tick.TickId || _phase != TickPhase.Compute)
        {
            if (message.TickId <= _tick.TickId)
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

    private void HandleTickDeliverDone(IContext context, ProtoControl.TickDeliverDone message)
    {
        if (_tick is null)
        {
            if (message.TickId <= _lastCompletedTickId)
            {
                HiveMindTelemetry.RecordLateDeliverAfterCompletion();
            }
            return;
        }

        if (message.TickId != _tick.TickId || _phase != TickPhase.Deliver)
        {
            if (message.TickId <= _tick.TickId)
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
        ReportBrainTick(context, brainId, message.TickId);
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
                if (_pendingCompute.Count > 0)
                {
                    LogError($"TickCompute timeout: tick {_tick.TickId} pending={_pendingCompute.Count}");
                }
                _pendingCompute.Clear();
                CompleteComputePhase(context);
                break;
            case TickPhase.Deliver:
                _tick.DeliverTimedOut = true;
                if (_pendingDeliver.Count > 0)
                {
                    var pendingBrains = string.Join(",", _pendingDeliver);
                    LogError($"TickDeliver timeout: tick {_tick.TickId} pendingBrains={pendingBrains}");
                }
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

            if (brain.RoutingSnapshot.Count == 0)
            {
                LogError($"Routing snapshot missing for brain {brain.BrainId} with {brain.Shards.Count} shard(s).");
            }

            var deliverTarget = brain.BrainRootPid ?? brain.SignalRouterPid;
            if (deliverTarget is null)
            {
                LogError($"TickDeliver skipped: missing BrainRoot/SignalRouter PID for brain {brain.BrainId}.");
                continue;
            }
            _pendingDeliver.Add(brain.BrainId);
            context.Send(deliverTarget, new ProtoControl.TickDeliver { TickId = _tick.TickId });
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

        foreach (var brain in _brains.Values)
        {
            ReportBrainState(context, brain.BrainId, "Paused", reason);
        }
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

    private static void LogError(string message)
        => Console.WriteLine($"[{DateTime.UtcNow:O}] [HiveMind][ERROR] {message}");

    private static void SendRoutingTable(IContext context, PID pid, RoutingTableSnapshot snapshot, string label)
    {
        if (!string.IsNullOrWhiteSpace(pid.Address) && string.IsNullOrWhiteSpace(context.System.Address))
        {
            LogError($"Routing table not sent to {label} {PidLabel(pid)} because remoting is not configured.");
            return;
        }

        try
        {
            context.Send(pid, new SetRoutingTable(snapshot));
        }
        catch (Exception ex)
        {
            LogError($"Failed to send routing table to {label} {PidLabel(pid)}: {ex.Message}");
        }
    }

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private static PID? NormalizePid(IContext context, PID? pid)
    {
        if (pid is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(pid.Address))
        {
            return pid;
        }

        var senderAddress = context.Sender?.Address;
        if (!string.IsNullOrWhiteSpace(senderAddress))
        {
            return new PID(senderAddress, pid.Id);
        }

        return pid;
    }

    private static bool TryGetGuid(Nbn.Proto.Uuid? uuid, out Guid guid)
    {
        if (uuid is null)
        {
            guid = Guid.Empty;
            return false;
        }

        return uuid.TryToGuid(out guid);
    }

    private static PID? ParsePid(string? value)
        => TryParsePid(value, out var pid) ? pid : null;

    private static bool TryParsePid(string? value, out PID pid)
    {
        pid = new PID();
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        var slashIndex = trimmed.IndexOf('/');
        if (slashIndex <= 0)
        {
            pid.Id = trimmed;
            return true;
        }

        var address = trimmed[..slashIndex];
        var id = trimmed[(slashIndex + 1)..];

        if (string.IsNullOrWhiteSpace(id))
        {
            return false;
        }

        pid.Address = address;
        pid.Id = id;
        return true;
    }

    private ProtoControl.HiveMindStatus BuildStatus()
        => new()
        {
            LastCompletedTickId = _lastCompletedTickId,
            TickLoopEnabled = _tickLoopEnabled,
            TargetTickHz = _backpressure.TargetTickHz,
            PendingCompute = (uint)_pendingCompute.Count,
            PendingDeliver = (uint)_pendingDeliver.Count,
            RescheduleInProgress = _rescheduleInProgress,
            RegisteredBrains = (uint)_brains.Count,
            RegisteredShards = (uint)_brains.Values.Sum(brain => brain.Shards.Count)
        };

    private BrainRoutingInfo BuildRoutingInfo(Guid brainId)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return new BrainRoutingInfo(brainId, null, null, 0, 0);
        }

        return new BrainRoutingInfo(
            brain.BrainId,
            brain.BrainRootPid,
            brain.SignalRouterPid,
            brain.Shards.Count,
            brain.RoutingSnapshot.Count);
    }

    private ProtoControl.BrainRoutingInfo BuildRoutingInfoProto(Guid brainId)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return new ProtoControl.BrainRoutingInfo
            {
                BrainId = brainId.ToProtoUuid()
            };
        }

        return new ProtoControl.BrainRoutingInfo
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            BrainRootPid = brain.BrainRootPid is null ? string.Empty : PidLabel(brain.BrainRootPid),
            SignalRouterPid = brain.SignalRouterPid is null ? string.Empty : PidLabel(brain.SignalRouterPid),
            ShardCount = (uint)brain.Shards.Count,
            RoutingCount = (uint)brain.RoutingSnapshot.Count
        };
    }

    private ProtoControl.BrainIoInfo BuildBrainIoInfo(Guid brainId)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return new ProtoControl.BrainIoInfo
            {
                BrainId = brainId.ToProtoUuid()
            };
        }

        return new ProtoControl.BrainIoInfo
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            InputWidth = (uint)Math.Max(0, brain.InputWidth),
            OutputWidth = (uint)Math.Max(0, brain.OutputWidth)
        };
    }

    private void UpdateRoutingTable(IContext? context, BrainState brain)
    {
        var snapshot = RoutingTableSnapshot.Empty;
        if (brain.Shards.Count > 0)
        {
            var routes = new List<ShardRoute>(brain.Shards.Count);
            foreach (var entry in brain.Shards)
            {
                routes.Add(new ShardRoute(entry.Key.Value, entry.Value));
            }

            snapshot = new RoutingTableSnapshot(routes);
        }

        brain.RoutingSnapshot = snapshot;

        if (context is null)
        {
            return;
        }

        if (brain.SignalRouterPid is not null)
        {
            SendRoutingTable(context, brain.SignalRouterPid, brain.RoutingSnapshot, "SignalRouter");
        }

        if (brain.BrainRootPid is not null && brain.BrainRootPid != brain.SignalRouterPid)
        {
            SendRoutingTable(context, brain.BrainRootPid, brain.RoutingSnapshot, "BrainRoot");
        }
    }

    private void UpdateOutputSinks(IContext context, BrainState brain)
    {
        if (brain.OutputSinkPid is null)
        {
            Log($"Output sink missing for brain {brain.BrainId}; output shards will not emit until registered.");
            return;
        }

        foreach (var entry in brain.Shards)
        {
            if (entry.Key.RegionId != NbnConstants.OutputRegionId)
            {
                continue;
            }

            SendOutputSinkUpdate(context, brain.BrainId, entry.Key, entry.Value, brain.OutputSinkPid);
        }
    }

    private static void SendOutputSinkUpdate(IContext context, Guid brainId, ShardId32 shardId, PID shardPid, PID outputSink)
    {
        try
        {
            context.Send(shardPid, new ProtoControl.UpdateShardOutputSink
            {
                BrainId = brainId.ToProtoUuid(),
                RegionId = (uint)shardId.RegionId,
                ShardIndex = (uint)shardId.ShardIndex,
                OutputPid = PidLabel(outputSink)
            });
        }
        catch (Exception ex)
        {
            LogError($"Failed to update output sink for shard {shardId}: {ex.Message}");
        }
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
        public PID? OutputSinkPid { get; set; }
        public int InputWidth { get; set; }
        public int OutputWidth { get; set; }
        public bool Paused { get; set; }
        public string? PausedReason { get; set; }
        public long SpawnedMs { get; set; }
        public Dictionary<ShardId32, PID> Shards { get; } = new();
        public RoutingTableSnapshot RoutingSnapshot { get; set; } = RoutingTableSnapshot.Empty;
    }

    private readonly record struct ShardKey(Guid BrainId, ShardId32 ShardId);

    private void ReportBrainRegistration(IContext context, BrainState brain)
    {
        if (_settingsPid is null)
        {
            return;
        }

        var controllerPid = brain.BrainRootPid ?? brain.SignalRouterPid;
        var nodeAddress = controllerPid is null ? string.Empty : ResolveNodeAddress(context, controllerPid);
        var nodeId = string.IsNullOrWhiteSpace(nodeAddress) ? Guid.Empty : DeriveNodeId(nodeAddress);

        var message = new ProtoSettings.BrainRegistered
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            SpawnedMs = brain.SpawnedMs > 0 ? (ulong)brain.SpawnedMs : 0,
            LastTickId = _lastCompletedTickId,
            State = brain.Paused ? "Paused" : "Active",
            ControllerNodeAddress = nodeAddress,
            ControllerNodeLogicalName = nodeAddress,
            ControllerRootActorName = controllerPid?.Id ?? string.Empty,
            ControllerActorName = controllerPid is null ? string.Empty : PidLabel(controllerPid)
        };

        if (nodeId != Guid.Empty)
        {
            message.ControllerNodeId = nodeId.ToProtoUuid();
        }

        context.Send(_settingsPid, message);
    }

    private void ReportBrainUnregistered(IContext context, Guid brainId)
    {
        if (_settingsPid is null)
        {
            return;
        }

        context.Send(_settingsPid, new ProtoSettings.BrainUnregistered
        {
            BrainId = brainId.ToProtoUuid(),
            TimeMs = (ulong)NowMs()
        });
    }

    private void ReportBrainState(IContext context, Guid brainId, string state, string? notes)
    {
        if (_settingsPid is null)
        {
            return;
        }

        context.Send(_settingsPid, new ProtoSettings.BrainStateChanged
        {
            BrainId = brainId.ToProtoUuid(),
            State = state,
            Notes = notes ?? string.Empty
        });
    }

    private void ReportBrainTick(IContext context, Guid brainId, ulong tickId)
    {
        if (_settingsPid is null)
        {
            return;
        }

        context.Send(_settingsPid, new ProtoSettings.BrainTick
        {
            BrainId = brainId.ToProtoUuid(),
            LastTickId = tickId
        });

        context.Send(_settingsPid, new ProtoSettings.BrainControllerHeartbeat
        {
            BrainId = brainId.ToProtoUuid(),
            TimeMs = (ulong)NowMs()
        });
    }

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static string ResolveNodeAddress(IContext context, PID pid)
    {
        if (!string.IsNullOrWhiteSpace(pid.Address))
        {
            return pid.Address;
        }

        var systemAddress = context.System.Address;
        return string.IsNullOrWhiteSpace(systemAddress) ? "local" : systemAddress;
    }

    private static Guid DeriveNodeId(string address)
        => NodeIdentity.DeriveNodeId(address);

    private static PID? BuildSettingsPid(HiveMindOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.SettingsHost))
        {
            return null;
        }

        if (options.SettingsPort <= 0)
        {
            return null;
        }

        if (string.IsNullOrWhiteSpace(options.SettingsName))
        {
            return null;
        }

        return new PID($"{options.SettingsHost}:{options.SettingsPort}", options.SettingsName);
    }
}
