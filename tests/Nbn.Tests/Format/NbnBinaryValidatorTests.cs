using System;
using System.Collections.Generic;
using System.Linq;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
using Nbn.Shared.Validation;
using Xunit;

namespace Nbn.Tests.Format;

public class NbnBinaryValidatorTests
{
    [Fact]
    public void ValidateNbn_MinimalVector_IsValid()
    {
        var bytes = NbnTestVectors.CreateMinimalNbn();
        var header = NbnBinary.ReadNbnHeader(bytes);
        var regions = ReadRegions(bytes, header);

        var result = NbnBinaryValidator.ValidateNbn(header, regions);

        Assert.True(result.IsValid, FormatIssues(result));
    }

    [Fact]
    public void ValidateNbn_DetectsUnsortedAxons()
    {
        var stride = (uint)NbnConstants.DefaultAxonStride;
        var neurons = new[]
        {
            new NeuronRecord(2, 0, 0, 0, 0, 0, 0, 0, true)
        };
        var axons = new[]
        {
            new AxonRecord(1, 5, 1),
            new AxonRecord(2, 3, 1)
        };
        var checkpoints = NbnBinary.BuildCheckpoints(neurons, stride);
        var region0 = new NbnRegionSection(0, (uint)neurons.Length, (ulong)axons.Length, stride, (uint)checkpoints.Length, checkpoints, neurons, axons);

        var outputNeurons = new[] { new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true) };
        var outputAxons = Array.Empty<AxonRecord>();
        var outputCheckpoints = NbnBinary.BuildCheckpoints(outputNeurons, stride);
        var region31 = new NbnRegionSection(31, 1, 0, stride, (uint)outputCheckpoints.Length, outputCheckpoints, outputNeurons, outputAxons);

        var regions = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        regions[0] = new NbnRegionDirectoryEntry(1, 2, (ulong)NbnBinary.NbnHeaderBytes, 0);
        regions[31] = new NbnRegionDirectoryEntry(1, 0, (ulong)NbnBinary.NbnHeaderBytes + 128, 0);

        var header = new NbnHeaderV2(
            "NBN2",
            2,
            1,
            10,
            0,
            stride,
            0,
            QuantizationSchemas.DefaultNbn,
            regions);

        var result = NbnBinaryValidator.ValidateNbn(header, new[] { region0, region31 });

        Assert.False(result.IsValid);
        Assert.Contains(result.Issues, issue => issue.Message.Contains("sorted", StringComparison.OrdinalIgnoreCase));
    }

    private static List<NbnRegionSection> ReadRegions(ReadOnlySpan<byte> data, NbnHeaderV2 header)
    {
        var regions = new List<NbnRegionSection>();
        for (var i = 0; i < header.Regions.Length; i++)
        {
            var entry = header.Regions[i];
            if (entry.NeuronSpan == 0)
            {
                continue;
            }

            regions.Add(NbnBinary.ReadNbnRegionSection(data, entry.Offset));
        }

        return regions;
    }

    private static string FormatIssues(NbnValidationResult result)
    {
        return string.Join(" | ", result.Issues.Select(issue => issue.ToString()));
    }
}
