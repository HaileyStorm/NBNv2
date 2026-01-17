using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Shared;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Runtime.RegionHost;

public sealed class RegionShardCpuBackend
{
    private readonly RegionShardState _state;

    public RegionShardCpuBackend(RegionShardState state)
    {
        _state = state ?? throw new ArgumentNullException(nameof(state));
    }

    public RegionShardComputeResult Compute(ulong tickId, Guid brainId, ShardId32 shardId, RegionShardRoutingTable routing)
    {
        routing ??= RegionShardRoutingTable.CreateSingleShard(_state.RegionId, _state.NeuronCount);

        var outbox = new Dictionary<ShardId32, List<Contribution>>();
        List<OutputEvent>? outputs = _state.IsOutputRegion ? new List<OutputEvent>() : null;
        var brainProto = brainId.ToProtoUuid();

        uint firedCount = 0;
        uint outContribs = 0;

        for (var i = 0; i < _state.NeuronCount; i++)
        {
            MergeInbox(i);

            if (!_state.Exists[i])
            {
                continue;
            }

            if (!_state.Enabled[i])
            {
                continue;
            }

            var buffer = _state.Buffer[i];
            if (buffer <= _state.PreActivationThreshold[i])
            {
                continue;
            }

            var potential = Activate((ActivationFunction)_state.ActivationFunctions[i], buffer, _state.ParamA[i], _state.ParamB[i]);
            _state.Buffer[i] = Reset((ResetFunction)_state.ResetFunctions[i], buffer, potential, _state.ActivationThreshold[i], _state.AxonCounts[i]);

            if (MathF.Abs(potential) <= _state.ActivationThreshold[i])
            {
                continue;
            }

            firedCount++;

            if (outputs is not null)
            {
                outputs.Add(new OutputEvent
                {
                    BrainId = brainProto,
                    OutputIndex = (uint)(_state.NeuronStart + i),
                    Value = potential,
                    TickId = tickId
                });
            }

            var axonCount = _state.AxonCounts[i];
            if (axonCount == 0)
            {
                continue;
            }

            var axonStart = _state.AxonStartOffsets[i];
            for (var a = 0; a < axonCount; a++)
            {
                var index = axonStart + a;
                var destRegion = _state.Axons.TargetRegionIds[index];
                var destNeuron = _state.Axons.TargetNeuronIds[index];
                var value = potential * _state.Axons.Strengths[index];

                if (!routing.TryGetShard(destRegion, destNeuron, out var destShard))
                {
                    destShard = ShardId32.From(destRegion, 0);
                }

                if (!outbox.TryGetValue(destShard, out var list))
                {
                    list = new List<Contribution>();
                    outbox[destShard] = list;
                }

                list.Add(new Contribution
                {
                    TargetNeuronId = (uint)destNeuron,
                    Value = value
                });
                outContribs++;
            }
        }

        IReadOnlyList<OutputEvent> outputList = outputs ?? new List<OutputEvent>();
        return new RegionShardComputeResult(outbox, outputList, firedCount, outContribs);
    }

    private void MergeInbox(int index)
    {
        var accum = (AccumulationFunction)_state.AccumulationFunctions[index];
        switch (accum)
        {
            case AccumulationFunction.AccumSum:
                _state.Buffer[index] += _state.Inbox[index];
                break;
            case AccumulationFunction.AccumProduct:
                if (_state.InboxHasInput[index])
                {
                    _state.Buffer[index] *= _state.Inbox[index];
                }
                break;
            case AccumulationFunction.AccumMax:
                if (_state.InboxHasInput[index])
                {
                    _state.Buffer[index] = MathF.Max(_state.Buffer[index], _state.Inbox[index]);
                }
                break;
            case AccumulationFunction.AccumNone:
                break;
            default:
                _state.Buffer[index] += _state.Inbox[index];
                break;
        }

        _state.Inbox[index] = 0f;
        _state.InboxHasInput[index] = false;
    }

    private static float Activate(ActivationFunction function, float buffer, float paramA, float paramB)
    {
        return function switch
        {
            ActivationFunction.ActNone => 0f,
            ActivationFunction.ActIdentity => buffer,
            ActivationFunction.ActStepUp => buffer <= 0f ? 0f : 1f,
            ActivationFunction.ActStepMid => buffer < 0f ? -1f : buffer == 0f ? 0f : 1f,
            ActivationFunction.ActStepDown => buffer < 0f ? -1f : 0f,
            ActivationFunction.ActAbs => MathF.Abs(buffer),
            ActivationFunction.ActClamp => Math.Clamp(buffer, -1f, 1f),
            ActivationFunction.ActRelu => MathF.Max(0f, buffer),
            ActivationFunction.ActNrelu => MathF.Min(buffer, 0f),
            ActivationFunction.ActSin => MathF.Sin(buffer),
            ActivationFunction.ActTan => Math.Clamp(MathF.Tan(buffer), -1f, 1f),
            ActivationFunction.ActTanh => MathF.Tanh(buffer),
            ActivationFunction.ActElu => buffer > 0f ? buffer : paramA * (MathF.Exp(buffer) - 1f),
            ActivationFunction.ActExp => MathF.Exp(buffer),
            ActivationFunction.ActPrelu => buffer >= 0f ? buffer : paramA * buffer,
            ActivationFunction.ActLog => buffer == 0f ? 0f : MathF.Log(buffer),
            ActivationFunction.ActMult => buffer * paramA,
            ActivationFunction.ActAdd => buffer + paramA,
            ActivationFunction.ActSig => 1f / (1f + MathF.Exp(-buffer)),
            ActivationFunction.ActSilu => buffer / (1f + MathF.Exp(-buffer)),
            ActivationFunction.ActPclamp => paramB <= paramA ? 0f : Math.Clamp(buffer, paramA, paramB),
            ActivationFunction.ActModl => paramA == 0f ? 0f : buffer % paramA,
            ActivationFunction.ActModr => buffer == 0f ? 0f : paramA % buffer,
            ActivationFunction.ActSoftp => MathF.Log(1f + MathF.Exp(buffer)),
            ActivationFunction.ActSelu => paramB * (buffer >= 0f ? buffer : paramA * (MathF.Exp(buffer) - 1f)),
            ActivationFunction.ActLin => paramA * buffer + paramB,
            ActivationFunction.ActLogb => paramA == 0f ? 0f : MathF.Log(buffer, paramA),
            ActivationFunction.ActPow => MathF.Pow(buffer, paramA),
            ActivationFunction.ActGauss => MathF.Exp(buffer * buffer),
            ActivationFunction.ActQuad => paramA * (buffer * buffer) + paramB * buffer,
            _ => 0f
        };
    }

    private static float Reset(ResetFunction function, float buffer, float potential, float threshold, int outDegree)
    {
        return function switch
        {
            ResetFunction.ResetZero => 0f,
            ResetFunction.ResetHold => Clamp(buffer, threshold),
            ResetFunction.ResetClampPotential => Clamp(buffer, MathF.Abs(potential)),
            ResetFunction.ResetClamp1 => Math.Clamp(buffer, -1f, 1f),
            ResetFunction.ResetPotentialClampBuffer => Clamp(potential, MathF.Abs(buffer)),
            ResetFunction.ResetNegPotentialClampBuffer => Clamp(-potential, MathF.Abs(buffer)),
            ResetFunction.ResetHundredthsPotentialClampBuffer => Clamp(0.01f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetTenthPotentialClampBuffer => Clamp(0.1f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetHalfPotentialClampBuffer => Clamp(0.5f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetDoublePotentialClampBuffer => Clamp(2f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetFivexPotentialClampBuffer => Clamp(5f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetNegHundredthsPotentialClampBuffer => Clamp(-0.01f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetNegTenthPotentialClampBuffer => Clamp(-0.1f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetNegHalfPotentialClampBuffer => Clamp(-0.5f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetNegDoublePotentialClampBuffer => Clamp(-2f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetNegFivexPotentialClampBuffer => Clamp(-5f * potential, MathF.Abs(buffer)),
            ResetFunction.ResetInversePotentialClampBuffer => Clamp(SafeInverse(potential), MathF.Abs(buffer)),

            ResetFunction.ResetPotentialClamp1 => Math.Clamp(potential, -1f, 1f),
            ResetFunction.ResetNegPotentialClamp1 => Math.Clamp(-potential, -1f, 1f),
            ResetFunction.ResetHundredthsPotentialClamp1 => Math.Clamp(0.01f * potential, -1f, 1f),
            ResetFunction.ResetTenthPotentialClamp1 => Math.Clamp(0.1f * potential, -1f, 1f),
            ResetFunction.ResetHalfPotentialClamp1 => Math.Clamp(0.5f * potential, -1f, 1f),
            ResetFunction.ResetDoublePotentialClamp1 => Math.Clamp(2f * potential, -1f, 1f),
            ResetFunction.ResetFivexPotentialClamp1 => Math.Clamp(5f * potential, -1f, 1f),
            ResetFunction.ResetNegHundredthsPotentialClamp1 => Math.Clamp(-0.01f * potential, -1f, 1f),
            ResetFunction.ResetNegTenthPotentialClamp1 => Math.Clamp(-0.1f * potential, -1f, 1f),
            ResetFunction.ResetNegHalfPotentialClamp1 => Math.Clamp(-0.5f * potential, -1f, 1f),
            ResetFunction.ResetNegDoublePotentialClamp1 => Math.Clamp(-2f * potential, -1f, 1f),
            ResetFunction.ResetNegFivexPotentialClamp1 => Math.Clamp(-5f * potential, -1f, 1f),
            ResetFunction.ResetInversePotentialClamp1 => Math.Clamp(SafeInverse(potential), -1f, 1f),

            ResetFunction.ResetPotential => Clamp(potential, threshold),
            ResetFunction.ResetNegPotential => Clamp(-potential, threshold),
            ResetFunction.ResetHundredthsPotential => Clamp(0.01f * potential, threshold),
            ResetFunction.ResetTenthPotential => Clamp(0.1f * potential, threshold),
            ResetFunction.ResetHalfPotential => Clamp(0.5f * potential, threshold),
            ResetFunction.ResetDoublePotential => Clamp(2f * potential, threshold),
            ResetFunction.ResetFivexPotential => Clamp(5f * potential, threshold),
            ResetFunction.ResetNegHundredthsPotential => Clamp(-0.01f * potential, threshold),
            ResetFunction.ResetNegTenthPotential => Clamp(-0.1f * potential, threshold),
            ResetFunction.ResetNegHalfPotential => Clamp(-0.5f * potential, threshold),
            ResetFunction.ResetNegDoublePotential => Clamp(-2f * potential, threshold),
            ResetFunction.ResetNegFivexPotential => Clamp(-5f * potential, threshold),
            ResetFunction.ResetInversePotential => Clamp(SafeInverse(potential), threshold),

            ResetFunction.ResetHalf => Clamp(0.5f * buffer, threshold),
            ResetFunction.ResetTenth => Clamp(0.1f * buffer, threshold),
            ResetFunction.ResetHundredth => Clamp(0.01f * buffer, threshold),
            ResetFunction.ResetNegative => Clamp(-buffer, threshold),
            ResetFunction.ResetNegHalf => Clamp(-0.5f * buffer, threshold),
            ResetFunction.ResetNegTenth => Clamp(-0.1f * buffer, threshold),
            ResetFunction.ResetNegHundredth => Clamp(-0.01f * buffer, threshold),

            ResetFunction.ResetDoubleClamp1 => Math.Clamp(2f * buffer, -1f, 1f),
            ResetFunction.ResetFivexClamp1 => Math.Clamp(5f * buffer, -1f, 1f),
            ResetFunction.ResetNegDoubleClamp1 => Math.Clamp(-2f * buffer, -1f, 1f),
            ResetFunction.ResetNegFivexClamp1 => Math.Clamp(-5f * buffer, -1f, 1f),

            ResetFunction.ResetDouble => Clamp(2f * buffer, threshold),
            ResetFunction.ResetFivex => Clamp(5f * buffer, threshold),
            ResetFunction.ResetNegDouble => Clamp(-2f * buffer, threshold),
            ResetFunction.ResetNegFivex => Clamp(-5f * buffer, threshold),

            ResetFunction.ResetDivideAxonCt => Clamp(buffer / Math.Max(1, outDegree), threshold),
            ResetFunction.ResetInverseClamp1 => Math.Clamp(-SafeInverse(buffer), -1f, 1f),
            ResetFunction.ResetInverse => Clamp(-SafeInverse(buffer), threshold),
            _ => buffer
        };
    }

    private static float Clamp(float value, float limit)
    {
        return Math.Clamp(value, -limit, limit);
    }

    private static float SafeInverse(float value)
    {
        return value == 0f ? 0f : 1f / value;
    }
}

public sealed record RegionShardComputeResult(
    Dictionary<ShardId32, List<Contribution>> Outbox,
    IReadOnlyList<OutputEvent> OutputEvents,
    uint FiredCount,
    uint OutContribs);
