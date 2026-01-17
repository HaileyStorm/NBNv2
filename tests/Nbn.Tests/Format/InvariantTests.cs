using Nbn.Shared;
using Nbn.Shared.Addressing;
using Nbn.Shared.Packing;
using Nbn.Shared.Validation;
using Xunit;

namespace Nbn.Tests.Format;

public class InvariantTests
{
    [Fact]
    public void NeuronRecord_InputRegionCannotBeDeleted()
    {
        var record = new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, false);
        Assert.False(NbnInvariants.TryValidateNeuronRecord(record, NbnConstants.InputRegionId, out _));
    }

    [Fact]
    public void AxonRecord_CannotTargetInputRegion()
    {
        var record = new AxonRecord(1, 0, (byte)NbnConstants.InputRegionId);
        Assert.False(NbnInvariants.TryValidateAxonRecord(record, 1, 10, out _));
    }

    [Fact]
    public void AxonRecord_OutputToOutputIsInvalid()
    {
        var record = new AxonRecord(1, 0, (byte)NbnConstants.OutputRegionId);
        Assert.False(NbnInvariants.TryValidateAxonRecord(record, NbnConstants.OutputRegionId, 10, out _));
    }

    [Fact]
    public void AxonList_DetectsDuplicateTargets()
    {
        var axons = new[]
        {
            new AxonRecord(1, 2, 3),
            new AxonRecord(2, 2, 3)
        };

        Assert.False(NbnInvariants.TryValidateAxonList(axons, 1, 10, out _));
    }

    [Fact]
    public void AxonList_DetectsUnsortedTargets()
    {
        var axons = new[]
        {
            new AxonRecord(1, 5, 2),
            new AxonRecord(2, 3, 2)
        };

        Assert.False(NbnInvariants.TryValidateAxonList(axons, 1, 10, out _));
    }

    [Fact]
    public void AxonRecord_ValidatesTargetSpan()
    {
        var record = new AxonRecord(1, 5, 2);
        Assert.False(NbnInvariants.TryValidateAxonRecord(record, 1, 5, out _));
        Assert.True(NbnInvariants.TryValidateAxonRecord(record, 1, 6, out _));
    }

    [Fact]
    public void AddressAndShard_TryFromRejectsOutOfRange()
    {
        Assert.False(Address32.TryFrom(32, 0, out _));
        Assert.False(Address32.TryFrom(0, NbnConstants.MaxAddressNeuronId + 1, out _));
        Assert.False(ShardId32.TryFrom(32, 0, out _));
        Assert.False(ShardId32.TryFrom(0, 1 << 16, out _));
    }
}