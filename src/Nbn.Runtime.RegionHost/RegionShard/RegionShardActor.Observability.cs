using Google.Protobuf;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Viz;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Nbn.Shared.Quantization;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;

namespace Nbn.Runtime.RegionHost;

public sealed partial class RegionShardActor
{
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
}
