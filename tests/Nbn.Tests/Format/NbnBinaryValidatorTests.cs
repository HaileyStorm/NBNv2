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

    [Fact]
    public void ValidateNbn_DetectsTargetNeuronBeyondDestinationSpan()
    {
        var stride = (uint)NbnConstants.DefaultAxonStride;
        var inputNeurons = new[]
        {
            new NeuronRecord(1, 0, 0, 0, 0, 0, 0, 0, true)
        };
        var inputAxons = new[]
        {
            new AxonRecord(1, 1, (byte)NbnConstants.OutputRegionId)
        };
        var inputCheckpoints = NbnBinary.BuildCheckpoints(inputNeurons, stride);
        var region0 = new NbnRegionSection(0, 1, 1, stride, (uint)inputCheckpoints.Length, inputCheckpoints, inputNeurons, inputAxons);

        var outputNeurons = new[] { new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true) };
        var outputCheckpoints = NbnBinary.BuildCheckpoints(outputNeurons, stride);
        var region31 = new NbnRegionSection(31, 1, 0, stride, (uint)outputCheckpoints.Length, outputCheckpoints, outputNeurons, Array.Empty<AxonRecord>());

        var regions = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        regions[0] = new NbnRegionDirectoryEntry(1, 1, (ulong)NbnBinary.NbnHeaderBytes, 0);
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
        Assert.Contains(result.Issues, issue => issue.Message.Contains("destination region span", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateNbn_DetectsAxonTargetRegionAbsentFromDirectory()
    {
        var stride = (uint)NbnConstants.DefaultAxonStride;
        var inputNeurons = new[]
        {
            new NeuronRecord(1, 0, 0, 0, 0, 0, 0, 0, true)
        };
        var inputAxons = new[]
        {
            new AxonRecord(1, 0, 1)
        };
        var inputCheckpoints = NbnBinary.BuildCheckpoints(inputNeurons, stride);
        var region0 = new NbnRegionSection(0, 1, 1, stride, (uint)inputCheckpoints.Length, inputCheckpoints, inputNeurons, inputAxons);

        var outputNeurons = new[] { new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true) };
        var outputCheckpoints = NbnBinary.BuildCheckpoints(outputNeurons, stride);
        var region31 = new NbnRegionSection(31, 1, 0, stride, (uint)outputCheckpoints.Length, outputCheckpoints, outputNeurons, Array.Empty<AxonRecord>());

        var regions = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        regions[0] = new NbnRegionDirectoryEntry(1, 1, (ulong)NbnBinary.NbnHeaderBytes, 0);
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
        Assert.Contains(result.Issues, issue => issue.Message.Contains("target region is absent from the region directory", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateNbn_ZeroStride_ReturnsValidationIssue()
    {
        var neurons = new[] { new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true) };
        var region0 = new NbnRegionSection(0, 1, 0, 0, 0, Array.Empty<ulong>(), neurons, Array.Empty<AxonRecord>());
        var region31 = new NbnRegionSection(31, 1, 0, 0, 0, Array.Empty<ulong>(), neurons, Array.Empty<AxonRecord>());

        var regions = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        regions[0] = new NbnRegionDirectoryEntry(1, 0, (ulong)NbnBinary.NbnHeaderBytes, 0);
        regions[31] = new NbnRegionDirectoryEntry(1, 0, (ulong)NbnBinary.NbnHeaderBytes + 128, 0);

        var header = new NbnHeaderV2(
            "NBN2",
            2,
            1,
            10,
            0,
            0,
            0,
            QuantizationSchemas.DefaultNbn,
            regions);

        NbnValidationResult? result = null;
        var ex = Record.Exception(() => result = NbnBinaryValidator.ValidateNbn(header, new[] { region0, region31 }));

        Assert.Null(ex);
        Assert.NotNull(result);
        Assert.False(result!.IsValid);
        Assert.Contains(result.Issues, issue => issue.Message.Contains("stride must be greater than zero", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateNbn_DetectsReservedFlags()
    {
        var stride = (uint)NbnConstants.DefaultAxonStride;
        var neurons = new[] { new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true) };
        var checkpoints = NbnBinary.BuildCheckpoints(neurons, stride);
        var region0 = new NbnRegionSection(0, 1, 0, stride, (uint)checkpoints.Length, checkpoints, neurons, Array.Empty<AxonRecord>());
        var region31 = new NbnRegionSection(31, 1, 0, stride, (uint)checkpoints.Length, checkpoints, neurons, Array.Empty<AxonRecord>());

        var regions = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        regions[0] = new NbnRegionDirectoryEntry(1, 0, (ulong)NbnBinary.NbnHeaderBytes, 1);
        regions[31] = new NbnRegionDirectoryEntry(1, 0, (ulong)NbnBinary.NbnHeaderBytes + 128, 0);
        regions[5] = new NbnRegionDirectoryEntry(0, 0, 0, 2);

        var header = new NbnHeaderV2(
            "NBN2",
            2,
            1,
            10,
            0,
            stride,
            1,
            QuantizationSchemas.DefaultNbn,
            regions);

        var result = NbnBinaryValidator.ValidateNbn(header, new[] { region0, region31 });

        Assert.False(result.IsValid);
        Assert.Contains(result.Issues, issue => issue.Message.Contains("header flags", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(result.Issues, issue => issue.Message.Contains("directory flags", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateNbn_DetectsMissingRequiredOutputRegion()
    {
        var stride = (uint)NbnConstants.DefaultAxonStride;
        var neurons = new[] { new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true) };
        var checkpoints = NbnBinary.BuildCheckpoints(neurons, stride);
        var region0 = new NbnRegionSection(0, 1, 0, stride, (uint)checkpoints.Length, checkpoints, neurons, Array.Empty<AxonRecord>());

        var regions = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        regions[0] = new NbnRegionDirectoryEntry(1, 0, (ulong)NbnBinary.NbnHeaderBytes, 0);

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

        var result = NbnBinaryValidator.ValidateNbn(header, new[] { region0 });

        Assert.False(result.IsValid);
        Assert.Contains(result.Issues, issue => issue.Message.Contains("output region must be present", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateNbs_DetectsReservedFlagsAndUnorderedRegions()
    {
        var header = new NbsHeaderV2(
            "NBS2",
            2,
            1,
            9,
            Guid.NewGuid(),
            0,
            0,
            0,
            new byte[32],
            0x20,
            QuantizationSchemas.DefaultBuffer);

        var region31 = new NbsRegionSection(31, 1, new short[] { 0 }, enabledBitset: null);
        var region0 = new NbsRegionSection(0, 1, new short[] { 0 }, enabledBitset: null);

        var result = NbnBinaryValidator.ValidateNbs(header, new[] { region31, region0 }, overlays: null);

        Assert.False(result.IsValid);
        Assert.Contains(result.Issues, issue => issue.Message.Contains("reserved bits", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(result.Issues, issue => issue.Message.Contains("sorted by ascending region id", StringComparison.OrdinalIgnoreCase));
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
