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
    public void NbnRegionSections_WithAxons_ConformToSpec()
    {
        var vector = NbnTestVectors.CreateRichNbnVector();
        var header = NbnBinary.ReadNbnHeader(vector.Bytes);

        foreach (var expected in vector.Regions)
        {
            var entry = header.Regions[expected.RegionId];
            Assert.Equal(expected.NeuronSpan, entry.NeuronSpan);
            Assert.Equal((ulong)expected.Axons.Length, entry.TotalAxons);
            Assert.True(entry.Offset > 0);

            var section = NbnBinary.ReadRegionSection(vector.Bytes, entry.Offset);
            Assert.Equal(expected.RegionId, section.RegionId);
            Assert.Equal(expected.NeuronSpan, section.NeuronSpan);
            Assert.Equal((ulong)expected.Axons.Length, section.TotalAxons);
            Assert.Equal(vector.Stride, section.Stride);

            var expectedCheckpoints = NbnTestVectors.BuildCheckpoints(expected.Neurons, vector.Stride);
            Assert.Equal(expectedCheckpoints.Length, section.Checkpoints.Length);
            for (var i = 0; i < expectedCheckpoints.Length; i++)
            {
                Assert.Equal(expectedCheckpoints[i], section.Checkpoints[i]);
            }

            Assert.Equal(expected.Neurons.Length, section.NeuronRecords.Length);
            for (var i = 0; i < expected.Neurons.Length; i++)
            {
                AssertNeuronRecord(expected.Neurons[i], section.NeuronRecords[i]);
            }

            Assert.Equal(expected.Axons.Length, section.AxonRecords.Length);
            for (var i = 0; i < expected.Axons.Length; i++)
            {
                AssertAxonRecord(expected.Axons[i], section.AxonRecords[i]);
            }

            AssertAxonOrdering(section);
        }
    }

    [Fact]
    public void NbnCheckpoints_CrossStrideBoundaries()
    {
        var vector = NbnTestVectors.CreateCheckpointNbnVector();
        var header = NbnBinary.ReadNbnHeader(vector.Bytes);

        Assert.Equal(4u, header.AxonStride);

        var entry = header.Regions[2];
        var section = NbnBinary.ReadRegionSection(vector.Bytes, entry.Offset);
        Assert.Equal(2, section.RegionId);
        Assert.Equal(4u, section.Stride);
        Assert.Equal(10u, section.NeuronSpan);
        Assert.Equal(8ul, section.TotalAxons);
        Assert.Equal(4u, section.CheckpointCount);
        Assert.Equal(new ulong[] { 0, 4, 7, 8 }, section.Checkpoints);

        AssertAxonOrdering(section);
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

    [Fact]
    public void NbsWithOverlays_ConformsToSpec()
    {
        var richNbn = NbnTestVectors.CreateRichNbnVector();
        var richNbs = NbnTestVectors.CreateRichNbsVector(richNbn);
        var header = NbnBinary.ReadNbsHeader(richNbs.Bytes);

        Assert.Equal(richNbs.Flags, header.Flags);
        Assert.True(header.EnabledBitsetIncluded);
        Assert.True(header.AxonOverlayIncluded);
        Assert.True(header.CostEnabled);
        Assert.True(header.EnergyEnabled);
        Assert.True(header.PlasticityEnabled);

        var expectedHash = SHA256.HashData(richNbn.Bytes);
        Assert.Equal(expectedHash, header.BaseNbnSha256);
        AssertQuantMap(QuantizationSchemas.DefaultBuffer, header.BufferMap);

        var offset = NbnBinary.NbsHeaderBytes;
        foreach (var region in richNbs.Regions)
        {
            var section = NbnBinary.ReadNbsRegionSection(richNbs.Bytes, offset, header.EnabledBitsetIncluded);
            Assert.Equal(region.RegionId, section.RegionId);
            Assert.Equal(region.NeuronSpan, section.NeuronSpan);
            Assert.Equal(region.BufferCodes, section.BufferCodes);
            Assert.NotNull(section.EnabledBitset);
            Assert.Equal(region.EnabledBitset, section.EnabledBitset!);
            offset += section.ByteLength;
        }

        var overlaySection = NbnBinary.ReadNbsOverlaySection(richNbs.Bytes, offset);
        Assert.Equal(richNbs.Overlays.Length, overlaySection.Overlays.Length);
        for (var i = 0; i < richNbs.Overlays.Length; i++)
        {
            Assert.Equal(richNbs.Overlays[i].FromAddress, overlaySection.Overlays[i].FromAddress);
            Assert.Equal(richNbs.Overlays[i].ToAddress, overlaySection.Overlays[i].ToAddress);
            Assert.Equal(richNbs.Overlays[i].StrengthCode, overlaySection.Overlays[i].StrengthCode);
        }

        Assert.Equal(4 + (richNbs.Overlays.Length * 12), overlaySection.ByteLength);
        Assert.Equal(offset + overlaySection.ByteLength, richNbs.Bytes.Length);
    }

    [Fact]
    public void NbsOverlaySection_AllowsZeroOverlays()
    {
        var nbn = NbnTestVectors.CreateMinimalNbn();
        var nbs = NbnTestVectors.CreateNbsWithEmptyOverlays(nbn);
        var header = NbnBinary.ReadNbsHeader(nbs);

        Assert.Equal(NbnTestVectors.OverlayOnlyFlags, header.Flags);
        Assert.False(header.EnabledBitsetIncluded);
        Assert.True(header.AxonOverlayIncluded);
        Assert.False(header.CostEnabled);
        Assert.False(header.EnergyEnabled);
        Assert.False(header.PlasticityEnabled);

        var offset = NbnBinary.NbsHeaderBytes;
        var region0 = NbnBinary.ReadNbsRegionSection(nbs, offset, header.EnabledBitsetIncluded);
        offset += region0.ByteLength;
        var region31 = NbnBinary.ReadNbsRegionSection(nbs, offset, header.EnabledBitsetIncluded);
        offset += region31.ByteLength;

        var overlaySection = NbnBinary.ReadNbsOverlaySection(nbs, offset);
        Assert.Empty(overlaySection.Overlays);
        Assert.Equal(4, overlaySection.ByteLength);
        Assert.Equal(offset + overlaySection.ByteLength, nbs.Length);
        Assert.Equal(NbnConstants.OutputRegionId, region31.RegionId);
    }

    private static void AssertQuantMap(QuantizationMap expected, QuantizationMap actual)
    {
        Assert.Equal(expected.MapType, actual.MapType);
        Assert.Equal(expected.Min, actual.Min, 6);
        Assert.Equal(expected.Max, actual.Max, 6);
        Assert.Equal(expected.Gamma, actual.Gamma, 6);
    }

    private static void AssertNeuronRecord(Nbn.Shared.Packing.NeuronRecord expected, Nbn.Shared.Packing.NeuronRecord actual)
    {
        Assert.Equal(expected.AxonCount, actual.AxonCount);
        Assert.Equal(expected.ParamBCode, actual.ParamBCode);
        Assert.Equal(expected.ParamACode, actual.ParamACode);
        Assert.Equal(expected.ActivationThresholdCode, actual.ActivationThresholdCode);
        Assert.Equal(expected.PreActivationThresholdCode, actual.PreActivationThresholdCode);
        Assert.Equal(expected.ResetFunctionId, actual.ResetFunctionId);
        Assert.Equal(expected.ActivationFunctionId, actual.ActivationFunctionId);
        Assert.Equal(expected.AccumulationFunctionId, actual.AccumulationFunctionId);
        Assert.Equal(expected.Exists, actual.Exists);
    }

    private static void AssertAxonRecord(Nbn.Shared.Packing.AxonRecord expected, Nbn.Shared.Packing.AxonRecord actual)
    {
        Assert.Equal(expected.StrengthCode, actual.StrengthCode);
        Assert.Equal(expected.TargetNeuronId, actual.TargetNeuronId);
        Assert.Equal(expected.TargetRegionId, actual.TargetRegionId);
    }

    private static void AssertAxonOrdering(NbnRegionSection section)
    {
        var cursor = 0;
        for (var i = 0; i < section.NeuronRecords.Length; i++)
        {
            var axonCount = section.NeuronRecords[i].AxonCount;
            for (var j = 0; j < axonCount; j++)
            {
                var axon = section.AxonRecords[cursor + j];
                Assert.NotEqual(NbnConstants.InputRegionId, axon.TargetRegionId);

                if (section.RegionId == NbnConstants.OutputRegionId)
                {
                    Assert.NotEqual(NbnConstants.OutputRegionId, axon.TargetRegionId);
                }

                if (j > 0)
                {
                    var previous = section.AxonRecords[cursor + j - 1];
                    var ordered = previous.TargetRegionId < axon.TargetRegionId
                                  || (previous.TargetRegionId == axon.TargetRegionId && previous.TargetNeuronId < axon.TargetNeuronId);
                    Assert.True(ordered);
                }
            }

            cursor += axonCount;
        }

        Assert.Equal(section.AxonRecords.Length, cursor);
    }
}
