using Nbn.Shared;
using Nbn.Shared.Addressing;
using Xunit;

namespace Nbn.Tests.Format;

public class ProtoAddressExtensionsTests
{
    [Fact]
    public void Address32_RoundTrips()
    {
        var address = Address32.From(5, 1234);
        var proto = address.ToProtoAddress32();

        Assert.Equal(address.Value, proto.Value);
        Assert.Equal(address.Value, proto.ToAddress32().Value);
    }

    [Fact]
    public void ShardId32_RoundTrips()
    {
        var shardId = ShardId32.From(7, 42);
        var proto = shardId.ToProtoShardId32();

        Assert.Equal(shardId.Value, proto.Value);
        Assert.Equal(shardId.Value, proto.ToShardId32().Value);
    }
}
