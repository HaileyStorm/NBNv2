using System.Diagnostics;
using Nbn.Proto;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.Quantization;
using ProtoControl = Nbn.Proto.Control;
using SharedAddress32 = Nbn.Shared.Addressing.Address32;
using SharedShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Runtime.WorkerNode;

public sealed partial class WorkerNodeCapabilityProvider
{
    private static readonly Guid ScoreBenchmarkBrainId = Guid.Parse("4F20A095-A750-4AFE-99FD-3DE769078D0D");
    private static readonly TimeSpan ScoreBenchmarkMinimumDuration = TimeSpan.FromMilliseconds(250);
    private static readonly RegionShardHomeostasisConfig ScoreBenchmarkHomeostasisDisabled =
        RegionShardHomeostasisConfig.Default with { Enabled = false };

    private const int ScoreBenchmarkNeuronCount = 262_144;
    private const int ScoreBenchmarkFanOut = 2;
    private const int ScoreBenchmarkOutputWidth = 8;
    private const int ScoreBenchmarkWarmupIterations = 2;
    private const int ScoreBenchmarkMinimumIterations = 2;

    private static WorkerCapabilityScores BenchmarkScores(WorkerCapabilityBaseline baseline)
    {
        var cpuScore = BenchmarkCpuScore();
        var gpuScore = baseline.IlgpuCudaAvailable || baseline.IlgpuOpenclAvailable
            ? BenchmarkGpuScore()
            : 0f;
        return new WorkerCapabilityScores(cpuScore, gpuScore);
    }

    private static float BenchmarkCpuScore()
    {
        var state = BuildScoreBenchmarkState(ScoreBenchmarkNeuronCount, ScoreBenchmarkFanOut);
        var backend = new RegionShardCpuBackend(state);
        var shardId = SharedShardId32.From(state.RegionId, shardIndex: 0);
        var routing = RegionShardRoutingTable.CreateSingleShard(state.RegionId, state.NeuronCount);

        return RunScoreBenchmark(
            state.NeuronCount,
            tickId => backend.Compute(tickId, ScoreBenchmarkBrainId, shardId, routing));
    }

    private static float BenchmarkGpuScore()
    {
        try
        {
            var state = BuildScoreBenchmarkState(ScoreBenchmarkNeuronCount, ScoreBenchmarkFanOut);
            using var backend = RegionShardIlgpuBackend.TryCreate(state);
            if (backend is null)
            {
                return 0f;
            }

            var support = backend.GetSupport(
                RegionShardVisualizationComputeScope.Disabled,
                plasticityEnabled: false,
                probabilisticPlasticityUpdates: false,
                plasticityDelta: 0f,
                plasticityRebaseThreshold: 0,
                plasticityRebaseThresholdPct: 0f,
                homeostasisConfig: ScoreBenchmarkHomeostasisDisabled,
                costEnergyEnabled: false,
                outputVectorSource: ProtoControl.OutputVectorSource.Potential);
            if (!support.IsSupported)
            {
                return 0f;
            }

            var shardId = SharedShardId32.From(state.RegionId, shardIndex: 0);
            var routing = RegionShardRoutingTable.CreateSingleShard(state.RegionId, state.NeuronCount);

            return RunScoreBenchmark(
                state.NeuronCount,
                tickId => backend.Compute(
                    tickId,
                    ScoreBenchmarkBrainId,
                    shardId,
                    routing,
                    visualization: RegionShardVisualizationComputeScope.Disabled,
                    plasticityEnabled: false,
                    homeostasisConfig: ScoreBenchmarkHomeostasisDisabled));
        }
        catch
        {
            return 0f;
        }
    }

    private static float RunScoreBenchmark(int neuronCount, Action<ulong> computeTick)
    {
        var tickId = 1UL;
        for (var i = 0; i < ScoreBenchmarkWarmupIterations; i++)
        {
            computeTick(tickId++);
        }

        var iterations = 0;
        var stopwatch = Stopwatch.StartNew();
        while (iterations < ScoreBenchmarkMinimumIterations || stopwatch.Elapsed < ScoreBenchmarkMinimumDuration)
        {
            computeTick(tickId++);
            iterations++;
        }

        stopwatch.Stop();
        var weightedOperations = (double)iterations * neuronCount * (1 + ScoreBenchmarkFanOut);
        return NormalizeScore(weightedOperations / Math.Max(stopwatch.Elapsed.TotalSeconds, 0.001d) / 1_000_000d);
    }

    private static RegionShardState BuildScoreBenchmarkState(int neuronCount, int fanOut)
    {
        const int regionId = 1;
        var regionSpans = new int[NbnConstants.RegionCount];
        regionSpans[regionId] = neuronCount;
        regionSpans[NbnConstants.OutputRegionId] = ScoreBenchmarkOutputWidth;

        var totalAxons = neuronCount * fanOut;
        var targetRegionIds = new byte[totalAxons];
        var targetNeuronIds = new int[totalAxons];
        var strengths = new float[totalAxons];
        var baseStrengthCodes = new byte[totalAxons];
        var runtimeStrengthCodes = new byte[totalAxons];
        var hasRuntimeOverlay = new bool[totalAxons];
        var fromAddress32 = new uint[totalAxons];
        var toAddress32 = new uint[totalAxons];

        var axonStarts = new int[neuronCount];
        var axonCounts = new ushort[neuronCount];

        var axonIndex = 0;
        for (var neuronId = 0; neuronId < neuronCount; neuronId++)
        {
            axonStarts[neuronId] = axonIndex;
            axonCounts[neuronId] = (ushort)fanOut;
            var sourceAddress = SharedAddress32.From(regionId, neuronId).Value;

            for (var fanOutIndex = 0; fanOutIndex < fanOut; fanOutIndex++)
            {
                var targetRegionId = fanOutIndex == fanOut - 1
                    ? NbnConstants.OutputRegionId
                    : regionId;
                var targetNeuronId = targetRegionId == regionId
                    ? (neuronId + fanOutIndex + 1) % neuronCount
                    : neuronId % ScoreBenchmarkOutputWidth;
                var strength = 0.55f + (fanOutIndex * 0.1f);
                var strengthCode = (byte)QuantizationSchemas.DefaultNbn.Strength.Encode(strength, bits: 5);

                targetRegionIds[axonIndex] = (byte)targetRegionId;
                targetNeuronIds[axonIndex] = targetNeuronId;
                strengths[axonIndex] = strength;
                baseStrengthCodes[axonIndex] = strengthCode;
                runtimeStrengthCodes[axonIndex] = strengthCode;
                hasRuntimeOverlay[axonIndex] = false;
                fromAddress32[axonIndex] = sourceAddress;
                toAddress32[axonIndex] = SharedAddress32.From(targetRegionId, targetNeuronId).Value;
                axonIndex++;
            }
        }

        return new RegionShardState(
            regionId: regionId,
            neuronStart: 0,
            neuronCount: neuronCount,
            brainSeed: 0x0102030405060708UL,
            strengthQuantization: QuantizationSchemas.DefaultNbn.Strength,
            regionSpans: regionSpans,
            buffer: Enumerable.Repeat(0.75f, neuronCount).ToArray(),
            enabled: Enumerable.Repeat(true, neuronCount).ToArray(),
            exists: Enumerable.Repeat(true, neuronCount).ToArray(),
            accumulationFunctions: Enumerable.Repeat((byte)AccumulationFunction.AccumSum, neuronCount).ToArray(),
            activationFunctions: Enumerable.Repeat((byte)ActivationFunction.ActRelu, neuronCount).ToArray(),
            resetFunctions: Enumerable.Repeat((byte)ResetFunction.ResetHold, neuronCount).ToArray(),
            paramA: new float[neuronCount],
            paramB: new float[neuronCount],
            preActivationThreshold: Enumerable.Repeat(-1f, neuronCount).ToArray(),
            activationThreshold: Enumerable.Repeat(0.1f, neuronCount).ToArray(),
            axonCounts: axonCounts,
            axonStartOffsets: axonStarts,
            axons: new RegionShardAxons(
                targetRegionIds: targetRegionIds,
                targetNeuronIds: targetNeuronIds,
                strengths: strengths,
                baseStrengthCodes: baseStrengthCodes,
                runtimeStrengthCodes: runtimeStrengthCodes,
                hasRuntimeOverlay: hasRuntimeOverlay,
                fromAddress32: fromAddress32,
                toAddress32: toAddress32));
    }
}
