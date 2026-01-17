using Nbn.Shared.Addressing;
using Nbn.Shared.Packing;
using Xunit;

namespace Nbn.Tests.Format;

public class PackingConformanceTests
{
    [Fact]
    public void NeuronRecord_PacksBitfields()
    {
        var record = new NeuronRecord(
            axonCount: 26,
            paramBCode: 42,
            paramACode: 21,
            activationThresholdCode: 47,
            preActivationThresholdCode: 4,
            resetFunctionId: 51,
            activationFunctionId: 28,
            accumulationFunctionId: 2,
            exists: true);

        var packed = record.Pack();

        ulong expected = 0;
        expected |= 26UL;
        expected |= 42UL << 9;
        expected |= 21UL << 15;
        expected |= 47UL << 21;
        expected |= 4UL << 27;
        expected |= 51UL << 33;
        expected |= 28UL << 39;
        expected |= 2UL << 45;
        expected |= 1UL << 47;

        Assert.Equal(expected, packed);

        var roundTrip = NeuronRecord.FromPacked(packed);
        Assert.Equal(record.AxonCount, roundTrip.AxonCount);
        Assert.Equal(record.ParamBCode, roundTrip.ParamBCode);
        Assert.Equal(record.ParamACode, roundTrip.ParamACode);
        Assert.Equal(record.ActivationThresholdCode, roundTrip.ActivationThresholdCode);
        Assert.Equal(record.PreActivationThresholdCode, roundTrip.PreActivationThresholdCode);
        Assert.Equal(record.ResetFunctionId, roundTrip.ResetFunctionId);
        Assert.Equal(record.ActivationFunctionId, roundTrip.ActivationFunctionId);
        Assert.Equal(record.AccumulationFunctionId, roundTrip.AccumulationFunctionId);
        Assert.Equal(record.Exists, roundTrip.Exists);
    }

    [Fact]
    public void AxonRecord_PacksBitfields()
    {
        const byte strengthCode = 27;
        const int targetNeuronId = 0x15555;
        const byte targetRegionId = 27;

        var record = new AxonRecord(strengthCode, targetNeuronId, targetRegionId);
        var packed = record.Pack();

        uint expected = 0;
        expected |= strengthCode;
        expected |= (uint)targetNeuronId << 5;
        expected |= (uint)targetRegionId << 27;

        Assert.Equal(expected, packed);

        var roundTrip = AxonRecord.FromPacked(packed);
        Assert.Equal(strengthCode, roundTrip.StrengthCode);
        Assert.Equal(targetNeuronId, roundTrip.TargetNeuronId);
        Assert.Equal(targetRegionId, roundTrip.TargetRegionId);
    }

    [Fact]
    public void AddressAndShardIds_PackExpectedBits()
    {
        const int regionId = 12;
        const int neuronId = 12345;
        var address = Address32.From(regionId, neuronId);
        var expectedAddressValue = ((uint)regionId << 27) | (uint)neuronId;
        Assert.Equal(expectedAddressValue, address.Value);
        Assert.Equal(regionId, address.RegionId);
        Assert.Equal(neuronId, address.NeuronId);

        const int shardRegionId = 5;
        const int shardIndex = 0xBEEF;
        var shardId = ShardId32.From(shardRegionId, shardIndex);
        var expectedShardValue = ((uint)shardRegionId << 16) | (uint)shardIndex;
        Assert.Equal(expectedShardValue, shardId.Value);
        Assert.Equal(shardRegionId, shardId.RegionId);
        Assert.Equal(shardIndex, shardId.ShardIndex);
    }
}
