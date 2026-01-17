using System;
using System.Security.Cryptography;
using Nbn.Shared;
using Nbn.Shared.Quantization;
using Xunit;

namespace Nbn.Tests.Format;

public class NbnFormatTests
{
    [Fact]
    public void NbnHeader_ConformsToSpec()
    {
        var nbn = NbnTestVectors.CreateMinimalNbn();
        var header = NbnBinary.ReadNbnHeader(nbn);

        Assert.Equal("NBN2", header.Magic);
        Assert.Equal((ushort)2, header.Version);
        Assert.Equal((byte)1, header.Endianness);
        Assert.Equal(NbnBinary.NbnHeaderBytes, 1 << header.HeaderBytesPow2);
        Assert.Equal(NbnTestVectors.SampleBrainSeed, header.BrainSeed);
        Assert.Equal((uint)NbnConstants.DefaultAxonStride, header.AxonStride);
        Assert.Equal(0u, header.Flags);

        AssertQuantMap(QuantizationSchemas.DefaultNbn.Strength, header.Quantization.Strength);
        AssertQuantMap(QuantizationSchemas.DefaultNbn.PreActivationThreshold, header.Quantization.PreActivationThreshold);
        AssertQuantMap(QuantizationSchemas.DefaultNbn.ActivationThreshold, header.Quantization.ActivationThreshold);
        AssertQuantMap(QuantizationSchemas.DefaultNbn.ParamA, header.Quantization.ParamA);
        AssertQuantMap(QuantizationSchemas.DefaultNbn.ParamB, header.Quantization.ParamB);

        var region0 = header.Regions[0];
        Assert.Equal(NbnTestVectors.MinimalNeuronSpan, region0.NeuronSpan);
        Assert.Equal(0ul, region0.TotalAxons);
        Assert.Equal((ulong)NbnBinary.NbnHeaderBytes, region0.Offset);
        Assert.Equal(0u, region0.Flags);

        var region31 = header.Regions[NbnConstants.OutputRegionId];
        Assert.Equal(NbnTestVectors.MinimalNeuronSpan, region31.NeuronSpan);
        Assert.Equal(0ul, region31.TotalAxons);
        Assert.True(region31.Offset > region0.Offset);

        var region1 = header.Regions[1];
        Assert.Equal(0u, region1.NeuronSpan);
        Assert.Equal(0ul, region1.TotalAxons);
        Assert.Equal(0ul, region1.Offset);
        Assert.Equal(0u, region1.Flags);
    }

    [Fact]
    public void NbnRegionSections_AreReadable()
    {
        var nbn = NbnTestVectors.CreateMinimalNbn();
        var header = NbnBinary.ReadNbnHeader(nbn);

        var region0 = NbnBinary.ReadRegionSection(nbn, header.Regions[0].Offset);
        Assert.Equal(0, region0.RegionId);
        Assert.Equal(header.Regions[0].NeuronSpan, region0.NeuronSpan);
        Assert.Equal((uint)NbnConstants.DefaultAxonStride, region0.Stride);
        Assert.Equal(2u, region0.CheckpointCount);
        Assert.All(region0.Checkpoints, checkpoint => Assert.Equal(0ul, checkpoint));
        Assert.All(region0.NeuronRecords, record =>
        {
            Assert.True(record.Exists);
            Assert.Equal(0, record.AxonCount);
        });
        Assert.Empty(region0.AxonRecords);

        var region31 = NbnBinary.ReadRegionSection(nbn, header.Regions[NbnConstants.OutputRegionId].Offset);
        Assert.Equal(NbnConstants.OutputRegionId, region31.RegionId);
        Assert.Equal(header.Regions[NbnConstants.OutputRegionId].NeuronSpan, region31.NeuronSpan);
        Assert.Empty(region31.AxonRecords);
    }

    [Fact]
    public void NbsHeader_ConformsToSpec()
    {
        var nbn = NbnTestVectors.CreateMinimalNbn();
        var nbs = NbnTestVectors.CreateMinimalNbs(nbn);
        var header = NbnBinary.ReadNbsHeader(nbs);

        Assert.Equal("NBS2", header.Magic);
        Assert.Equal((ushort)2, header.Version);
        Assert.Equal((byte)1, header.Endianness);
        Assert.Equal(NbnBinary.NbsHeaderBytes, 1 << header.HeaderBytesPow2);
        Assert.Equal(NbnTestVectors.SampleBrainId, header.BrainId);
        Assert.Equal(NbnTestVectors.SampleTickId, header.SnapshotTickId);
        Assert.Equal(NbnTestVectors.SampleTimestampMs, header.TimestampMs);
        Assert.Equal(NbnTestVectors.SampleEnergyRemaining, header.EnergyRemaining);

        var expectedHash = SHA256.HashData(nbn);
        Assert.Equal(expectedHash, header.BaseNbnSha256);

        Assert.False(header.EnabledBitsetIncluded);
        Assert.False(header.AxonOverlayIncluded);
        Assert.False(header.CostEnabled);
        Assert.False(header.EnergyEnabled);
        Assert.False(header.PlasticityEnabled);

        AssertQuantMap(QuantizationSchemas.DefaultBuffer, header.BufferMap);
    }

    [Fact]
    public void NbsRegionSections_AreOrderedAndSized()
    {
        var nbn = NbnTestVectors.CreateMinimalNbn();
        var nbs = NbnTestVectors.CreateMinimalNbs(nbn);
        var header = NbnBinary.ReadNbsHeader(nbs);

        var offset = NbnBinary.NbsHeaderBytes;
        var region0 = NbnBinary.ReadNbsRegionSection(nbs, offset, header.EnabledBitsetIncluded);
        Assert.Equal(0, region0.RegionId);
        Assert.Equal(NbnTestVectors.MinimalNeuronSpan, region0.NeuronSpan);
        Assert.All(region0.BufferCodes, code => Assert.Equal((short)0, code));
        Assert.Null(region0.EnabledBitset);
        offset += region0.ByteLength;

        var region31 = NbnBinary.ReadNbsRegionSection(nbs, offset, header.EnabledBitsetIncluded);
        Assert.Equal(NbnConstants.OutputRegionId, region31.RegionId);
        Assert.Equal(NbnTestVectors.MinimalNeuronSpan, region31.NeuronSpan);
        Assert.All(region31.BufferCodes, code => Assert.Equal((short)0, code));
        Assert.Null(region31.EnabledBitset);
    }

    private static void AssertQuantMap(QuantizationMap expected, QuantizationMap actual)
    {
        Assert.Equal(expected.MapType, actual.MapType);
        Assert.Equal(expected.Min, actual.Min, 6);
        Assert.Equal(expected.Max, actual.Max, 6);
        Assert.Equal(expected.Gamma, actual.Gamma, 6);
    }
}
