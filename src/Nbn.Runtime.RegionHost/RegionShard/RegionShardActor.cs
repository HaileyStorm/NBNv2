using System.Diagnostics;
using Google.Protobuf;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Proto.Viz;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Nbn.Shared.IO;
using Nbn.Shared.Quantization;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;

namespace Nbn.Runtime.RegionHost;

public sealed class RegionShardActor : IActor
{
    private static readonly bool LogDelivery = IsEnvTrue("NBN_REGIONHOST_LOG_DELIVERY");
    private static readonly bool LogOutput = IsEnvTrue("NBN_REGIONHOST_LOG_OUTPUT");
    private static readonly bool LogViz = IsEnvTrue("NBN_REGIONHOST_LOG_VIZ");
    private static readonly bool LogVizDiagnostics = IsEnvTrue("NBN_VIZ_DIAGNOSTICS_ENABLED");
    private static readonly bool LogInitDiagnostics = IsEnvTrue("NBN_REGIONSHARD_INIT_DIAGNOSTICS_ENABLED");
    private const int RecentComputeCacheSize = 2;
    private readonly RegionShardState _state;
    private readonly RegionShardCpuBackend _cpu;
    private readonly Guid _brainId;
    private readonly ShardId32 _shardId;
    private readonly Dictionary<ulong, TickComputeDone> _recentComputeDone = new();
    private RegionShardRoutingTable _routing;
    private PID? _router;
    private PID? _outputSink;
    private PID? _tickSink;
    private PID? _vizHub;
    private PID? _debugHub;
    private bool _debugEnabled;
    private ProtoSeverity _debugMinSeverity;
    private bool _vizEnabled;
    private uint? _vizFocusRegionId;
    private bool _costEnergyEnabled;
    private bool _remoteCostEnabled;
    private long _remoteCostPerBatch;
    private long _remoteCostPerContribution;
    private float _costTierAMultiplier = 1f;
    private float _costTierBMultiplier = 1f;
    private float _costTierCMultiplier = 1f;
    private bool _plasticityEnabled;
    private float _plasticityRate;
    private bool _plasticityProbabilisticUpdates;
    private float _plasticityDelta;
    private uint _plasticityRebaseThreshold;
    private float _plasticityRebaseThresholdPct;
    private bool _plasticityEnergyCostModulationEnabled = RegionShardPlasticityEnergyCostConfig.Default.Enabled;
    private long _plasticityEnergyCostReferenceTickCost = RegionShardPlasticityEnergyCostConfig.Default.ReferenceTickCost;
    private float _plasticityEnergyCostResponseStrength = RegionShardPlasticityEnergyCostConfig.Default.ResponseStrength;
    private float _plasticityEnergyCostMinScale = RegionShardPlasticityEnergyCostConfig.Default.MinScale;
    private float _plasticityEnergyCostMaxScale = RegionShardPlasticityEnergyCostConfig.Default.MaxScale;
    private bool _homeostasisEnabled = RegionShardHomeostasisConfig.Default.Enabled;
    private HomeostasisTargetMode _homeostasisTargetMode = RegionShardHomeostasisConfig.Default.TargetMode;
    private HomeostasisUpdateMode _homeostasisUpdateMode = RegionShardHomeostasisConfig.Default.UpdateMode;
    private float _homeostasisBaseProbability = RegionShardHomeostasisConfig.Default.BaseProbability;
    private uint _homeostasisMinStepCodes = RegionShardHomeostasisConfig.Default.MinStepCodes;
    private bool _homeostasisEnergyCouplingEnabled = RegionShardHomeostasisConfig.Default.EnergyCouplingEnabled;
    private float _homeostasisEnergyTargetScale = RegionShardHomeostasisConfig.Default.EnergyTargetScale;
    private float _homeostasisEnergyProbabilityScale = RegionShardHomeostasisConfig.Default.EnergyProbabilityScale;
    private OutputVectorSource _outputVectorSource = OutputVectorSource.Potential;
    private uint _vizStreamMinIntervalMs = 250;
    private bool _hasComputed;
    private ulong _lastComputeTickId;
    private long _lastTickCostTotal;
    private ulong _vizSequence;

    public RegionShardActor(RegionShardState state, RegionShardActorConfig config)
    {
        _state = state ?? throw new ArgumentNullException(nameof(state));
        _cpu = new RegionShardCpuBackend(_state);
        _brainId = config.BrainId;
        _shardId = config.ShardId;
        _router = config.Router;
        _outputSink = config.OutputSink;
        _tickSink = config.TickSink;
        _vizHub = config.VizHub;
        _debugHub = config.DebugHub;
        _debugEnabled = config.DebugEnabled;
        _debugMinSeverity = config.DebugMinSeverity;
        _vizEnabled = config.VizEnabled;
        _vizFocusRegionId = null;
        _routing = config.Routing ?? RegionShardRoutingTable.CreateSingleShard(_state.RegionId, _state.NeuronCount);
    }

    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
                EmitVizEvent(context, VizEventType.VizShardSpawned, tickId: 0, value: 0f);
                EmitDebug(context, ProtoSeverity.SevInfo, "shard.started", $"Shard {_shardId} for brain {_brainId} started.");
                if (LogInitDiagnostics)
                {
                    LogShardInitDiagnostics();
                }
                break;
            case RegionShardUpdateEndpoints endpoints:
                _router = endpoints.Router;
                _outputSink = endpoints.OutputSink;
                _tickSink = endpoints.TickSink;
                break;
            case RegionShardUpdateRouting routing:
                _routing = routing.Routing;
                break;
            case SignalBatch batch:
                HandleSignalBatch(context, batch);
                break;
            case TickCompute tick:
                HandleTickCompute(context, tick);
                break;
            case RegisterShard registerShard:
                ForwardRegisterShard(context, registerShard);
                break;
            case UnregisterShard unregisterShard:
                ForwardUnregisterShard(context, unregisterShard);
                break;
            case RuntimeNeuronPulse pulse:
                HandleRuntimeNeuronPulse(pulse);
                break;
            case RuntimeNeuronStateWrite stateWrite:
                HandleRuntimeNeuronStateWrite(stateWrite);
                break;
            case UpdateShardOutputSink message:
                HandleUpdateOutputSink(message);
                break;
            case UpdateShardVisualization message:
                HandleUpdateVisualization(message);
                break;
            case UpdateShardRuntimeConfig message:
                HandleUpdateRuntimeConfig(message);
                break;
            case CaptureShardSnapshot message:
                HandleCaptureShardSnapshot(context, message);
                break;
        }

        return Task.CompletedTask;
    }

    private void ForwardRegisterShard(IContext context, RegisterShard message)
    {
        if (_tickSink is not null)
        {
            context.Request(_tickSink, message);
        }
    }

    private void ForwardUnregisterShard(IContext context, UnregisterShard message)
    {
        if (_tickSink is not null)
        {
            context.Request(_tickSink, message);
        }
    }

    private void HandleUpdateOutputSink(UpdateShardOutputSink message)
    {
        if (message.BrainId is null || !message.BrainId.TryToGuid(out var guid) || guid != _brainId)
        {
            return;
        }

        if (message.RegionId != (uint)_state.RegionId || message.ShardIndex != (uint)_shardId.ShardIndex)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(message.OutputPid))
        {
            _outputSink = null;
            if (LogOutput && _state.IsOutputRegion)
            {
                Console.WriteLine($"[RegionShard] Output sink cleared for brain={_brainId} shard={_shardId}.");
            }
            return;
        }

        if (TryParsePid(message.OutputPid, out var pid))
        {
            _outputSink = pid;
            if (LogOutput && _state.IsOutputRegion)
            {
                Console.WriteLine($"[RegionShard] Output sink set for brain={_brainId} shard={_shardId} sink={message.OutputPid}.");
            }
        }
    }

    private void HandleUpdateVisualization(UpdateShardVisualization message)
    {
        if (message.BrainId is null || !message.BrainId.TryToGuid(out var guid) || guid != _brainId)
        {
            return;
        }

        if (message.RegionId != (uint)_state.RegionId || message.ShardIndex != (uint)_shardId.ShardIndex)
        {
            return;
        }

        _vizEnabled = message.Enabled;
        _vizFocusRegionId = message.Enabled && message.HasFocusRegion
            ? message.FocusRegionId
            : null;
        _vizStreamMinIntervalMs = NormalizeVisualizationMinIntervalMs(message.VizStreamMinIntervalMs);

        if (LogViz || LogVizDiagnostics)
        {
            var focusLabel = _vizFocusRegionId.HasValue ? _vizFocusRegionId.Value.ToString() : "all";
            var hubLabel = _vizHub is null
                ? "(null)"
                : (string.IsNullOrWhiteSpace(_vizHub.Address) ? _vizHub.Id : $"{_vizHub.Address}/{_vizHub.Id}");
            Console.WriteLine(
                $"[RegionShard] Viz config updated brain={_brainId} shard={_shardId} enabled={_vizEnabled} focus={focusLabel} streamMinIntervalMs={_vizStreamMinIntervalMs} hub={hubLabel}.");
        }
    }

    private void HandleUpdateRuntimeConfig(UpdateShardRuntimeConfig message)
    {
        if (message.BrainId is null || !message.BrainId.TryToGuid(out var guid) || guid != _brainId)
        {
            return;
        }

        if (message.RegionId != (uint)_state.RegionId || message.ShardIndex != (uint)_shardId.ShardIndex)
        {
            return;
        }

        _costEnergyEnabled = message.CostEnabled && message.EnergyEnabled;
        _remoteCostEnabled = message.RemoteCostEnabled;
        _remoteCostPerBatch = Math.Max(0L, message.RemoteCostPerBatch);
        _remoteCostPerContribution = Math.Max(0L, message.RemoteCostPerContribution);
        _costTierAMultiplier = NormalizeTierMultiplier(message.CostTierAMultiplier);
        _costTierBMultiplier = NormalizeTierMultiplier(message.CostTierBMultiplier);
        _costTierCMultiplier = NormalizeTierMultiplier(message.CostTierCMultiplier);
        _plasticityEnabled = message.PlasticityEnabled;
        _plasticityRate = message.PlasticityRate;
        _plasticityProbabilisticUpdates = message.ProbabilisticUpdates;
        _plasticityDelta = message.PlasticityDelta;
        _plasticityRebaseThreshold = message.PlasticityRebaseThreshold;
        _plasticityRebaseThresholdPct = message.PlasticityRebaseThresholdPct;
        _plasticityEnergyCostModulationEnabled = message.PlasticityEnergyCostModulationEnabled;
        _plasticityEnergyCostReferenceTickCost = Math.Max(1L, message.PlasticityEnergyCostReferenceTickCost);
        _plasticityEnergyCostResponseStrength = NormalizeFiniteInRange(message.PlasticityEnergyCostResponseStrength, 0f, 8f, RegionShardPlasticityEnergyCostConfig.Default.ResponseStrength);
        _plasticityEnergyCostMinScale = NormalizeFiniteInRange(message.PlasticityEnergyCostMinScale, 0f, 1f, RegionShardPlasticityEnergyCostConfig.Default.MinScale);
        _plasticityEnergyCostMaxScale = NormalizeFiniteInRange(message.PlasticityEnergyCostMaxScale, 0f, 1f, RegionShardPlasticityEnergyCostConfig.Default.MaxScale);
        if (_plasticityEnergyCostMaxScale < _plasticityEnergyCostMinScale)
        {
            _plasticityEnergyCostMaxScale = _plasticityEnergyCostMinScale;
        }
        _homeostasisEnabled = message.HomeostasisEnabled;
        _homeostasisTargetMode = message.HomeostasisTargetMode;
        _homeostasisUpdateMode = message.HomeostasisUpdateMode;
        _homeostasisBaseProbability = message.HomeostasisBaseProbability;
        _homeostasisMinStepCodes = message.HomeostasisMinStepCodes;
        _homeostasisEnergyCouplingEnabled = message.HomeostasisEnergyCouplingEnabled;
        _homeostasisEnergyTargetScale = message.HomeostasisEnergyTargetScale;
        _homeostasisEnergyProbabilityScale = message.HomeostasisEnergyProbabilityScale;
        _outputVectorSource = NormalizeOutputVectorSource(message.OutputVectorSource);
        _debugEnabled = message.DebugEnabled;
        _debugMinSeverity = message.DebugMinSeverity;
    }

    private void HandleCaptureShardSnapshot(IContext context, CaptureShardSnapshot message)
    {
        var response = new CaptureShardSnapshotAck
        {
            BrainId = _brainId.ToProtoUuid(),
            RegionId = (uint)_state.RegionId,
            ShardIndex = (uint)_shardId.ShardIndex,
            NeuronStart = (uint)_state.NeuronStart,
            NeuronCount = (uint)_state.NeuronCount,
            Success = false
        };

        if (message.BrainId is null || !message.BrainId.TryToGuid(out var brainId) || brainId != _brainId)
        {
            response.Error = "brain_id_mismatch";
            context.Respond(response);
            return;
        }

        if (message.RegionId != (uint)_state.RegionId || message.ShardIndex != (uint)_shardId.ShardIndex)
        {
            response.Error = "shard_id_mismatch";
            context.Respond(response);
            return;
        }

        var enabledBytes = new byte[(_state.NeuronCount + 7) / 8];
        for (var i = 0; i < _state.NeuronCount; i++)
        {
            var buffer = _state.Buffer[i];
            if (!float.IsFinite(buffer))
            {
                buffer = 0f;
            }

            var code = QuantizationSchemas.DefaultBuffer.Encode(buffer, bits: 16);
            response.BufferCodes.Add(code);

            if (_state.Enabled[i])
            {
                enabledBytes[i / 8] |= (byte)(1 << (i % 8));
            }
        }

        response.EnabledBitset = ByteString.CopyFrom(enabledBytes);

        for (var i = 0; i < _state.Axons.Count; i++)
        {
            var runtimeCode = _state.Axons.RuntimeStrengthCodes[i];
            if (!_state.Axons.HasRuntimeOverlay[i] || runtimeCode == _state.Axons.BaseStrengthCodes[i])
            {
                continue;
            }

            response.Overlays.Add(new SnapshotOverlayRecord
            {
                FromAddress = _state.Axons.FromAddress32[i],
                ToAddress = _state.Axons.ToAddress32[i],
                StrengthCode = runtimeCode
            });
        }

        response.Success = true;
        context.Respond(response);
    }

    private void HandleSignalBatch(IContext context, SignalBatch batch)
    {
        if (batch is null)
        {
            return;
        }

        if (!IsBatchForShard(batch, out var rejectReason))
        {
            RegionHostTelemetry.RecordSignalBatchRejected();
            EmitDebug(context, ProtoSeverity.SevWarn, "signal.rejected", $"Rejected SignalBatch tick={batch.TickId} reason={rejectReason}");
            if (LogDelivery)
            {
                Console.WriteLine($"[RegionShard] SignalBatch rejected. reason={rejectReason} tick={batch.TickId}");
            }
            SendSignalBatchAck(context, batch, preferBatchAddressing: true);
            return;
        }

        var isLateBatch = _hasComputed && batch.TickId < _lastComputeTickId;
        if (isLateBatch)
        {
            RegionHostTelemetry.RecordSignalBatchLate();
            EmitDebug(context, ProtoSeverity.SevWarn, "signal.late", $"Late SignalBatch tick={batch.TickId} lastCompute={_lastComputeTickId}");
            if (LogDelivery)
            {
                Console.WriteLine($"[RegionShard] SignalBatch late. tick={batch.TickId} lastCompute={_lastComputeTickId}");
            }
        }
        else
        {
            foreach (var contrib in batch.Contribs)
            {
                _state.ApplyContribution(contrib.TargetNeuronId, contrib.Value);
            }
        }

        SendSignalBatchAck(context, batch, preferBatchAddressing: false);
    }

    private void SendSignalBatchAck(IContext context, SignalBatch batch, bool preferBatchAddressing)
    {
        var ackRegionId = preferBatchAddressing ? batch.RegionId : (uint)_state.RegionId;
        var ackShardId = preferBatchAddressing && batch.ShardId is not null
            ? batch.ShardId.ToShardId32()
            : _shardId;
        var ack = new SignalBatchAck
        {
            BrainId = _brainId.ToProtoUuid(),
            RegionId = ackRegionId,
            ShardId = ackShardId.ToProtoShardId32(),
            TickId = batch.TickId
        };

        var target = context.Sender;
        if (target is null || string.IsNullOrWhiteSpace(target.Address))
        {
            if (LogDelivery)
            {
                var senderLabel = target is null ? "(null)" : $"{target.Id}";
                var routerLabel = _router is null ? "(null)" : $"{_router.Address}/{_router.Id}";
                Console.WriteLine($"[RegionShard] SignalBatch ack fallback to router. tick={batch.TickId} sender={senderLabel} router={routerLabel}");
            }

            target = _router;
        }

        if (target is null)
        {
            if (LogDelivery)
            {
                Console.WriteLine($"[RegionShard] SignalBatch ack dropped. tick={batch.TickId} reason=no_target");
            }

            return;
        }

        context.Request(target, ack);
    }

    private void HandleTickCompute(IContext context, TickCompute tick)
    {
        if (_recentComputeDone.TryGetValue(tick.TickId, out var cachedDone))
        {
            RegionHostTelemetry.RecordComputeDuplicate();
            EmitDebug(context, ProtoSeverity.SevDebug, "tick.duplicate", $"Duplicate TickCompute {tick.TickId}");
            if (LogDelivery)
            {
                Console.WriteLine($"[RegionShard] TickCompute duplicate. tick={tick.TickId}");
            }

            SendComputeDone(context, cachedDone);
            return;
        }

        if (_hasComputed && tick.TickId < _lastComputeTickId)
        {
            RegionHostTelemetry.RecordComputeOutOfOrder();
            EmitDebug(context, ProtoSeverity.SevWarn, "tick.out_of_order", $"Out-of-order TickCompute tick={tick.TickId} lastCompute={_lastComputeTickId}");
            if (LogDelivery)
            {
                Console.WriteLine($"[RegionShard] TickCompute out-of-order. tick={tick.TickId} lastCompute={_lastComputeTickId}");
            }
            return;
        }

        if (_hasComputed && tick.TickId > _lastComputeTickId + 1)
        {
            RegionHostTelemetry.RecordComputeJump();
            EmitDebug(context, ProtoSeverity.SevWarn, "tick.jump", $"TickCompute jump tick={tick.TickId} lastCompute={_lastComputeTickId}");
            if (LogDelivery)
            {
                Console.WriteLine($"[RegionShard] TickCompute jump. tick={tick.TickId} lastCompute={_lastComputeTickId}");
            }
        }

        var stopwatch = Stopwatch.StartNew();
        var collectVisualization = ShouldCollectVisualizationForTick(tick.TickId, tick.TargetTickHz);
        var vizScope = collectVisualization
            ? new RegionShardVisualizationComputeScope(true, _vizFocusRegionId)
            : RegionShardVisualizationComputeScope.Disabled;
        var result = _cpu.Compute(
            tick.TickId,
            _brainId,
            _shardId,
            _routing,
            vizScope,
            _plasticityEnabled,
            _plasticityRate,
            _plasticityProbabilisticUpdates,
            _plasticityDelta,
            _plasticityRebaseThreshold,
            _plasticityRebaseThresholdPct,
            new RegionShardPlasticityEnergyCostConfig(
                _plasticityEnergyCostModulationEnabled,
                _plasticityEnergyCostReferenceTickCost,
                _plasticityEnergyCostResponseStrength,
                _plasticityEnergyCostMinScale,
                _plasticityEnergyCostMaxScale),
            new RegionShardHomeostasisConfig(
                _homeostasisEnabled,
                _homeostasisTargetMode,
                _homeostasisUpdateMode,
                _homeostasisBaseProbability,
                _homeostasisMinStepCodes,
                _homeostasisEnergyCouplingEnabled,
                _homeostasisEnergyTargetScale,
                _homeostasisEnergyProbabilityScale),
            _costEnergyEnabled,
            _remoteCostEnabled,
            _remoteCostPerBatch,
            _remoteCostPerContribution,
            _costTierAMultiplier,
            _costTierBMultiplier,
            _costTierCMultiplier,
            _outputVectorSource,
            _lastTickCostTotal);
        stopwatch.Stop();

        if (LogViz || LogVizDiagnostics)
        {
            var focusLabel = _vizFocusRegionId.HasValue ? _vizFocusRegionId.Value.ToString() : "all";
            var hubLabel = _vizHub is null
                ? "(null)"
                : (string.IsNullOrWhiteSpace(_vizHub.Address) ? _vizHub.Id : $"{_vizHub.Address}/{_vizHub.Id}");
            var vizStride = ComputeVizStride(tick.TargetTickHz, _vizStreamMinIntervalMs);
            Console.WriteLine(
                $"[RegionShard] Viz compute tick={tick.TickId} shard={_shardId} enabled={_vizEnabled} focus={focusLabel} streamMinIntervalMs={_vizStreamMinIntervalMs} stride={vizStride} collect={collectVisualization} hub={hubLabel} axonEvents={result.AxonVizEvents.Count} bufferEvents={result.BufferNeuronEvents.Count} firedEvents={result.FiredNeuronEvents.Count}.");
        }

        if (result.PlasticityStrengthCodeChanges > 0)
        {
            RegionHostTelemetry.RecordPlasticityStrengthCodeChanges(result.PlasticityStrengthCodeChanges, _state.RegionId, _shardId.ShardIndex);
            EmitDebug(
                context,
                ProtoSeverity.SevDebug,
                "plasticity.mutation",
                $"TickCompute {tick.TickId} mutated {result.PlasticityStrengthCodeChanges} strength code(s) on shard {_shardId.Value}.");
        }

        var outboxTarget = _router ?? context.Sender;
        if (outboxTarget is not null)
        {
            var brainProto = _brainId.ToProtoUuid();
            foreach (var (destShard, contribs) in result.Outbox)
            {
                if (contribs.Count == 0)
                {
                    continue;
                }

                var batch = new OutboxBatch
                {
                    BrainId = brainProto,
                    TickId = tick.TickId,
                    DestRegionId = (uint)destShard.RegionId,
                    DestShardId = destShard.ToProtoShardId32()
                };
                batch.Contribs.AddRange(contribs);
                context.Send(outboxTarget, batch);
            }
        }

        foreach (var axonViz in result.AxonVizEvents)
        {
            EmitVizEvent(
                context,
                VizEventType.VizAxonSent,
                tick.TickId,
                axonViz.AverageSignedValue,
                source: new Address32(axonViz.SourceAddress),
                target: new Address32(axonViz.TargetAddress),
                strength: axonViz.AverageSignedStrength);
        }

        foreach (var buffer in result.BufferNeuronEvents)
        {
            EmitVizEvent(
                context,
                VizEventType.VizNeuronBuffer,
                buffer.TickId,
                buffer.Buffer,
                source: new Address32(buffer.SourceAddress));
        }

        foreach (var fired in result.FiredNeuronEvents)
        {
            EmitVizEvent(
                context,
                VizEventType.VizNeuronFired,
                fired.TickId,
                fired.Potential,
                source: new Address32(fired.SourceAddress));
        }

        if (_outputSink is not null)
        {
            if (result.OutputEvents.Count > 0)
            {
                foreach (var output in result.OutputEvents)
                {
                    context.Send(_outputSink, output);
                }
            }

            if (result.OutputVector.Count > 0)
            {
                context.Send(
                    _outputSink,
                    new OutputVectorSegment
                    {
                        BrainId = _brainId.ToProtoUuid(),
                        TickId = tick.TickId,
                        OutputIndexStart = (uint)_state.NeuronStart,
                        Values = { result.OutputVector }
                    });
            }
        }

        var done = new TickComputeDone
        {
            TickId = tick.TickId,
            BrainId = _brainId.ToProtoUuid(),
            RegionId = (uint)_state.RegionId,
            ShardId = _shardId.ToProtoShardId32(),
            ComputeMs = (ulong)Math.Round(stopwatch.Elapsed.TotalMilliseconds),
            TickCostTotal = result.Cost.Total,
            CostAccum = result.Cost.Accum,
            CostActivation = result.Cost.Activation,
            CostReset = result.Cost.Reset,
            CostDistance = result.Cost.Distance,
            CostRemote = result.Cost.Remote,
            FiredCount = result.FiredCount,
            OutBatches = (uint)result.Outbox.Count,
            OutContribs = result.OutContribs
        };

        if (LogOutput && _state.IsOutputRegion)
        {
            var sinkLabel = _outputSink is null
                ? "(null)"
                : (string.IsNullOrWhiteSpace(_outputSink.Address) ? _outputSink.Id : $"{_outputSink.Address}/{_outputSink.Id}");
            Console.WriteLine(
                $"[RegionShard] Output compute tick={tick.TickId} shard={_shardId} sink={sinkLabel} vectorCount={result.OutputVector.Count} singleCount={result.OutputEvents.Count}");
        }

        _hasComputed = true;
        _lastComputeTickId = tick.TickId;
        _lastTickCostTotal = result.Cost.Total;
        CacheComputeDone(done);
        SendComputeDone(context, done);
    }

    private void HandleRuntimeNeuronPulse(RuntimeNeuronPulse message)
    {
        if (message.BrainId is null
            || !message.BrainId.TryToGuid(out var guid)
            || guid != _brainId
            || message.TargetRegionId != (uint)_state.RegionId)
        {
            return;
        }

        _state.TryApplyRuntimePulse(message.TargetNeuronId, message.Value);
    }

    private void HandleRuntimeNeuronStateWrite(RuntimeNeuronStateWrite message)
    {
        if (message.BrainId is null
            || !message.BrainId.TryToGuid(out var guid)
            || guid != _brainId
            || message.TargetRegionId != (uint)_state.RegionId)
        {
            return;
        }

        _state.TrySetRuntimeNeuronState(
            message.TargetNeuronId,
            message.SetBuffer,
            message.BufferValue,
            message.SetAccumulator,
            message.AccumulatorValue);
    }

    private void CacheComputeDone(TickComputeDone done)
    {
        _recentComputeDone[done.TickId] = done;
        while (_recentComputeDone.Count > RecentComputeCacheSize)
        {
            var oldest = ulong.MaxValue;
            foreach (var key in _recentComputeDone.Keys)
            {
                if (key < oldest)
                {
                    oldest = key;
                }
            }

            if (oldest == ulong.MaxValue)
            {
                break;
            }

            _recentComputeDone.Remove(oldest);
        }
    }

    private void SendComputeDone(IContext context, TickComputeDone done)
    {
        var doneTarget = _tickSink ?? context.Sender ?? _router;
        if (doneTarget is not null)
        {
            context.Request(doneTarget, done);
        }

        if (LogVizDiagnostics)
        {
            var senderLabel = context.Sender is null
                ? "<missing>"
                : (string.IsNullOrWhiteSpace(context.Sender.Address) ? context.Sender.Id : $"{context.Sender.Address}/{context.Sender.Id}");
            var targetLabel = doneTarget is null
                ? "(null)"
                : (string.IsNullOrWhiteSpace(doneTarget.Address) ? doneTarget.Id : $"{doneTarget.Address}/{doneTarget.Id}");
            Console.WriteLine(
                $"[RegionShard] TickComputeDone sent tick={done.TickId} shard={_shardId} sender={senderLabel} target={targetLabel} vizEnabled={_vizEnabled} fired={done.FiredCount} outBatches={done.OutBatches}.");
        }
    }

    private bool IsBatchForShard(SignalBatch batch, out string reason)
    {
        reason = "unknown";

        if (batch.BrainId is null || !batch.BrainId.TryToGuid(out var batchBrain) || batchBrain != _brainId)
        {
            reason = "brain";
            return false;
        }

        if (batch.RegionId != (uint)_state.RegionId)
        {
            reason = "region";
            return false;
        }

        if (batch.ShardId is not null)
        {
            var shardId = batch.ShardId.ToShardId32();
            if (!shardId.Equals(_shardId))
            {
                reason = "shard";
                return false;
            }
        }

        reason = "ok";
        return true;
    }

    private void EmitVizEvent(
        IContext context,
        VizEventType type,
        ulong tickId,
        float value,
        Address32? source = null,
        Address32? target = null,
        float strength = 0f)
    {
        if (!_vizEnabled
            || _vizHub is null
            || !ObservabilityTargets.CanSend(context, _vizHub)
            || !TouchesFocusRegion(source, target))
        {
            return;
        }

        var evt = new VisualizationEvent
        {
            EventId = $"region-{++_vizSequence}",
            TimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Type = type,
            BrainId = _brainId.ToProtoUuid(),
            TickId = tickId,
            RegionId = (uint)_state.RegionId,
            ShardId = _shardId.ToProtoShardId32(),
            Value = value,
            Strength = strength
        };

        if (source.HasValue)
        {
            evt.Source = source.Value.ToProtoAddress32();
        }

        if (target.HasValue)
        {
            evt.Target = target.Value.ToProtoAddress32();
        }

        context.Send(_vizHub, evt);
    }

    private bool TouchesFocusRegion(Address32? source, Address32? target)
    {
        if (!_vizFocusRegionId.HasValue)
        {
            return true;
        }

        var focusRegionId = _vizFocusRegionId.Value;
        if ((uint)_state.RegionId == focusRegionId)
        {
            return true;
        }

        if (source.HasValue && (uint)source.Value.RegionId == focusRegionId)
        {
            return true;
        }

        if (target.HasValue && (uint)target.Value.RegionId == focusRegionId)
        {
            return true;
        }

        return false;
    }

    private bool ShouldCollectVisualizationForTick(ulong tickId, float targetTickHz)
    {
        if (!_vizEnabled || tickId == 0)
        {
            return false;
        }

        var stride = ComputeVizStride(targetTickHz, _vizStreamMinIntervalMs);
        if (stride <= 1u)
        {
            return true;
        }

        var strideAsUlong = (ulong)stride;
        var phase = ((ulong)(uint)_state.RegionId) % strideAsUlong;
        return tickId % strideAsUlong == phase;
    }

    private static uint NormalizeVisualizationMinIntervalMs(uint value)
        => Math.Min(value, 60_000u);

    private static uint ComputeVizStride(float targetTickHz, uint minIntervalMs)
    {
        if (minIntervalMs == 0u || !float.IsFinite(targetTickHz) || targetTickHz <= 0f)
        {
            return 1u;
        }

        var tickMs = 1000f / targetTickHz;
        if (!float.IsFinite(tickMs) || tickMs <= 0f || tickMs >= minIntervalMs)
        {
            return 1u;
        }

        var stride = (uint)Math.Ceiling(minIntervalMs / tickMs);
        return Math.Max(1u, stride);
    }

    private void EmitDebug(IContext context, ProtoSeverity severity, string category, string message)
    {
        if (!_debugEnabled || severity < _debugMinSeverity)
        {
            return;
        }

        if (_debugHub is null || !ObservabilityTargets.CanSend(context, _debugHub))
        {
            return;
        }

        context.Send(_debugHub, new DebugOutbound
        {
            Severity = severity,
            Context = $"region.{category}",
            Summary = category,
            Message = message,
            SenderActor = string.IsNullOrWhiteSpace(context.Self.Address) ? context.Self.Id : $"{context.Self.Address}/{context.Self.Id}",
            SenderNode = context.System.Address ?? string.Empty,
            TimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        });
    }

    private void LogShardInitDiagnostics()
    {
        var existsCount = 0;
        var enabledCount = 0;
        var totalAxons = 0;
        var sampleIndex = -1;
        var minPre = float.PositiveInfinity;
        var maxPre = float.NegativeInfinity;
        var minThr = float.PositiveInfinity;
        var maxThr = float.NegativeInfinity;

        for (var i = 0; i < _state.NeuronCount; i++)
        {
            if (_state.Exists[i])
            {
                existsCount++;
                if (sampleIndex < 0)
                {
                    sampleIndex = i;
                }
            }

            if (_state.Enabled[i])
            {
                enabledCount++;
            }

            var pre = _state.PreActivationThreshold[i];
            var thr = _state.ActivationThreshold[i];
            if (pre < minPre)
            {
                minPre = pre;
            }

            if (pre > maxPre)
            {
                maxPre = pre;
            }

            if (thr < minThr)
            {
                minThr = thr;
            }

            if (thr > maxThr)
            {
                maxThr = thr;
            }

            totalAxons += _state.AxonCounts[i];
        }

        var sampleLabel = "none";
        if (sampleIndex >= 0)
        {
            sampleLabel =
                $"idx={sampleIndex} neuron={_state.NeuronStart + sampleIndex} " +
                $"exists={_state.Exists[sampleIndex]} enabled={_state.Enabled[sampleIndex]} " +
                $"accum={_state.AccumulationFunctions[sampleIndex]} act={_state.ActivationFunctions[sampleIndex]} reset={_state.ResetFunctions[sampleIndex]} " +
                $"pre={_state.PreActivationThreshold[sampleIndex]:0.###} thr={_state.ActivationThreshold[sampleIndex]:0.###} " +
                $"paramA={_state.ParamA[sampleIndex]:0.###} paramB={_state.ParamB[sampleIndex]:0.###} axons={_state.AxonCounts[sampleIndex]}";
        }

        Console.WriteLine(
            $"[RegionShard] Init diagnostics brain={_brainId} shard={_shardId} region={_state.RegionId} neuronStart={_state.NeuronStart} neuronCount={_state.NeuronCount} exists={existsCount} enabled={enabledCount} totalAxons={totalAxons} preRange=[{minPre:0.###},{maxPre:0.###}] thrRange=[{minThr:0.###},{maxThr:0.###}] sample={sampleLabel}.");
    }

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Equals("1", StringComparison.OrdinalIgnoreCase)
               || value.Equals("true", StringComparison.OrdinalIgnoreCase)
               || value.Equals("yes", StringComparison.OrdinalIgnoreCase)
               || value.Equals("y", StringComparison.OrdinalIgnoreCase);
    }

    private static float NormalizeTierMultiplier(float value)
    {
        return float.IsFinite(value) && value > 0f
            ? value
            : 1f;
    }

    private static OutputVectorSource NormalizeOutputVectorSource(OutputVectorSource source)
    {
        return source switch
        {
            OutputVectorSource.Buffer => source,
            _ => OutputVectorSource.Potential
        };
    }

    private static float NormalizeFiniteInRange(float value, float min, float max, float fallback)
    {
        if (!float.IsFinite(value))
        {
            return fallback;
        }

        return Math.Clamp(value, min, max);
    }

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
}

