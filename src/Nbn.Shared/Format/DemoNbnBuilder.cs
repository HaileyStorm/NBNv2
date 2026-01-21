using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;

namespace Nbn.Shared.Format;

public static class DemoNbnBuilder
{
    public static byte[] BuildSampleNbn()
    {
        var stride = 1024u;
        var sections = new List<NbnRegionSection>();
        var directory = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        ulong offset = NbnBinary.NbnHeaderBytes;

        var inputAxons = new[]
        {
            new AxonRecord(strengthCode: 31, targetNeuronId: 0, targetRegionId: 1)
        };

        offset = AddRegionSection(
            0,
            1,
            stride,
            ref directory,
            sections,
            offset,
            neuronFactory: _ => new NeuronRecord(
                axonCount: 1,
                paramBCode: 0,
                paramACode: 0,
                activationThresholdCode: 0,
                preActivationThresholdCode: 0,
                resetFunctionId: 0,
                activationFunctionId: 1,
                accumulationFunctionId: 0,
                exists: true),
            axons: inputAxons);

        var demoAxons = new[]
        {
            new AxonRecord(strengthCode: 31, targetNeuronId: 0, targetRegionId: 1),
            new AxonRecord(strengthCode: 31, targetNeuronId: 0, targetRegionId: NbnConstants.OutputRegionId)
        };

        offset = AddRegionSection(
            1,
            1,
            stride,
            ref directory,
            sections,
            offset,
            neuronFactory: _ => new NeuronRecord(
                axonCount: 2,
                paramBCode: 0,
                paramACode: 40,
                activationThresholdCode: 0,
                preActivationThresholdCode: 0,
                resetFunctionId: 0,
                activationFunctionId: 17,
                accumulationFunctionId: 0,
                exists: true),
            axons: demoAxons);

        offset = AddRegionSection(NbnConstants.OutputRegionId, 1, stride, ref directory, sections, offset);

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

    private static ulong AddRegionSection(
        int regionId,
        uint neuronSpan,
        uint stride,
        ref NbnRegionDirectoryEntry[] directory,
        List<NbnRegionSection> sections,
        ulong offset,
        Func<int, NeuronRecord>? neuronFactory = null,
        AxonRecord[]? axons = null)
    {
        var neurons = new NeuronRecord[neuronSpan];
        for (var i = 0; i < neurons.Length; i++)
        {
            neurons[i] = neuronFactory?.Invoke(i) ?? new NeuronRecord(
                axonCount: 0,
                paramBCode: 0,
                paramACode: 0,
                activationThresholdCode: 0,
                preActivationThresholdCode: 0,
                resetFunctionId: 0,
                activationFunctionId: 1,
                accumulationFunctionId: 0,
                exists: true);
        }

        ulong totalAxons = 0;
        for (var i = 0; i < neurons.Length; i++)
        {
            totalAxons += neurons[i].AxonCount;
        }

        axons ??= Array.Empty<AxonRecord>();
        if ((ulong)axons.Length != totalAxons)
        {
            throw new InvalidOperationException($"Region {regionId} axon count mismatch. Expected {totalAxons}, got {axons.Length}.");
        }

        var checkpointCount = (uint)((neuronSpan + stride - 1) / stride + 1);
        var checkpoints = new ulong[checkpointCount];
        var checkpointIndex = 1;
        var running = 0UL;
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

        checkpoints[0] = 0;
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
