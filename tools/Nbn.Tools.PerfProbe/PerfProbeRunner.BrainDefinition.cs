using Nbn.Proto;
using Nbn.Runtime.HiveMind;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;

namespace Nbn.Tools.PerfProbe;

public static partial class PerfProbeRunner
{
    private static byte[] BuildPerformanceNbn(
        int hiddenNeuronCount,
        int inputWidth,
        int outputWidth,
        bool includeInputAssignments = true)
    {
        const uint stride = 1_024;
        var sections = new List<NbnRegionSection>();
        var directory = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        ulong offset = NbnBinary.NbnHeaderBytes;

        var inputAssignments = includeInputAssignments && inputWidth > 0
            ? Enumerable.Range(0, hiddenNeuronCount)
                .GroupBy(index => index % inputWidth)
                .ToDictionary(group => group.Key, group => group.ToArray())
            : new Dictionary<int, int[]>();
        offset = AddRegionSection(
            regionId: NbnConstants.InputRegionId,
            neuronSpan: (uint)inputWidth,
            stride: stride,
            directory: directory,
            sections: sections,
            offset: offset,
            neuronFactory: neuronId =>
            {
                inputAssignments.TryGetValue(neuronId, out var targets);
                return new NeuronRecord(
                    axonCount: (ushort)(targets?.Length ?? 0),
                    paramBCode: 0,
                    paramACode: 0,
                    activationThresholdCode: 0,
                    preActivationThresholdCode: 0,
                    resetFunctionId: (byte)ResetFunction.ResetHold,
                    activationFunctionId: (byte)ActivationFunction.ActIdentity,
                    accumulationFunctionId: (byte)AccumulationFunction.AccumSum,
                    exists: true);
            },
            axonsBuilder: () =>
            {
                var inputAxons = new List<AxonRecord>(hiddenNeuronCount);
                for (var neuronId = 0; neuronId < inputWidth; neuronId++)
                {
                    if (!inputAssignments.TryGetValue(neuronId, out var targets))
                    {
                        continue;
                    }

                    foreach (var targetNeuronId in targets)
                    {
                        inputAxons.Add(new AxonRecord(31, targetNeuronId, targetRegionId: 1));
                    }
                }

                return inputAxons;
            });

        offset = AddRegionSection(
            regionId: 1,
            neuronSpan: (uint)hiddenNeuronCount,
            stride: stride,
            directory: directory,
            sections: sections,
            offset: offset,
            neuronFactory: _ => new NeuronRecord(
                axonCount: (ushort)2,
                paramBCode: 0,
                paramACode: 0,
                activationThresholdCode: 0,
                preActivationThresholdCode: 0,
                resetFunctionId: (byte)ResetFunction.ResetHold,
                activationFunctionId: (byte)ActivationFunction.ActRelu,
                accumulationFunctionId: (byte)AccumulationFunction.AccumSum,
                exists: true),
            axonsBuilder: () =>
            {
                var hiddenAxons = new List<AxonRecord>(hiddenNeuronCount * 2);
                for (var neuronId = 0; neuronId < hiddenNeuronCount; neuronId++)
                {
                    hiddenAxons.Add(new AxonRecord(28, (neuronId + 1) % hiddenNeuronCount, targetRegionId: 1));
                    hiddenAxons.Add(new AxonRecord(24, neuronId % outputWidth, targetRegionId: (byte)NbnConstants.OutputRegionId));
                }

                return hiddenAxons;
            });

        offset = AddRegionSection(
            regionId: NbnConstants.OutputRegionId,
            neuronSpan: (uint)outputWidth,
            stride: stride,
            directory: directory,
            sections: sections,
            offset: offset,
            neuronFactory: _ => new NeuronRecord(
                axonCount: (ushort)0,
                paramBCode: 0,
                paramACode: 0,
                activationThresholdCode: 0,
                preActivationThresholdCode: 0,
                resetFunctionId: (byte)ResetFunction.ResetHold,
                activationFunctionId: (byte)ActivationFunction.ActIdentity,
                accumulationFunctionId: (byte)AccumulationFunction.AccumSum,
                exists: true),
            axonsBuilder: static () => Array.Empty<AxonRecord>());

        var header = new NbnHeaderV2(
            "NBN2",
            2,
            1,
            10,
            brainSeed: 1,
            axonStride: stride,
            flags: 0,
            quantization: QuantizationSchemas.DefaultNbn,
            regions: directory);

        return NbnBinary.WriteNbn(header, sections);
    }

    private static int ResolvePerformanceInputWidth(int hiddenNeuronCount)
    {
        if (hiddenNeuronCount <= 0)
        {
            return DefaultInputWidth;
        }

        var requiredWidth = (int)Math.Ceiling(hiddenNeuronCount / (double)MaxPackedAxonCount);
        return Math.Max(DefaultInputWidth, requiredWidth);
    }

    private static ulong AddRegionSection(
        int regionId,
        uint neuronSpan,
        uint stride,
        NbnRegionDirectoryEntry[] directory,
        List<NbnRegionSection> sections,
        ulong offset,
        Func<int, NeuronRecord> neuronFactory,
        Func<IReadOnlyList<AxonRecord>> axonsBuilder)
    {
        var neurons = new NeuronRecord[neuronSpan];
        for (var i = 0; i < neurons.Length; i++)
        {
            neurons[i] = neuronFactory(i);
        }

        var axons = axonsBuilder().ToArray();
        ulong totalAxons = 0;
        foreach (var neuron in neurons)
        {
            totalAxons += neuron.AxonCount;
        }

        if ((ulong)axons.Length != totalAxons)
        {
            throw new InvalidOperationException($"Region {regionId} axon count mismatch. Expected {totalAxons}, got {axons.Length}.");
        }

        var checkpointCount = (uint)((neuronSpan + stride - 1) / stride + 1);
        var checkpoints = new ulong[checkpointCount];
        checkpoints[0] = 0;
        var running = 0UL;
        var checkpointIndex = 1;
        uint nextBoundary = stride;
        for (var i = 0; i < neurons.Length; i++)
        {
            running += neurons[i].AxonCount;
            if ((uint)(i + 1) == nextBoundary && checkpointIndex < checkpointCount)
            {
                checkpoints[checkpointIndex++] = running;
                nextBoundary += stride;
            }
        }

        checkpoints[checkpointCount - 1] = running;
        var section = new NbnRegionSection(
            (byte)regionId,
            neuronSpan,
            totalAxons,
            stride,
            checkpointCount,
            checkpoints,
            neurons,
            axons);

        directory[regionId] = new NbnRegionDirectoryEntry(neuronSpan, totalAxons, offset, 0);
        sections.Add(section);
        return offset + (ulong)section.ByteLength;
    }
}
