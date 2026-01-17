using System;
using Google.Protobuf;
using Nbn.Proto;
using Nbn.Shared;
using Xunit;

namespace Nbn.Tests.Format;

public class ProtoUuidExtensionsTests
{
    [Fact]
    public void ToGuid_UsesRfc4122Order()
    {
        var bytes = new byte[]
        {
            0x8E, 0x8D, 0x0F, 0x2A,
            0x1D, 0xB7,
            0x4C, 0x7D,
            0x9D, 0x1F, 0x05, 0xD5, 0xB8, 0xD8, 0xE2, 0xA1
        };

        var uuid = new Uuid { Value = ByteString.CopyFrom(bytes) };

        var guid = uuid.ToGuid();

        Assert.Equal(Guid.Parse("8E8D0F2A-1DB7-4C7D-9D1F-05D5B8D8E2A1"), guid);
    }

    [Fact]
    public void ToProtoUuid_RoundTrips()
    {
        var guid = Guid.Parse("8E8D0F2A-1DB7-4C7D-9D1F-05D5B8D8E2A1");
        var uuid = guid.ToProtoUuid();

        Assert.Equal(Guid.Parse("8E8D0F2A-1DB7-4C7D-9D1F-05D5B8D8E2A1"), uuid.ToGuid());
    }

    [Fact]
    public void TryToGuid_FailsOnInvalidLength()
    {
        var uuid = new Uuid { Value = ByteString.CopyFrom(new byte[] { 0x01, 0x02 }) };

        Assert.False(uuid.TryToGuid(out var guid));
        Assert.Equal(Guid.Empty, guid);
    }
}
