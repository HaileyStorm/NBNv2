using System;
using Nbn.Shared;
using Xunit;

namespace Nbn.Tests.Format;

public class UuidEncodingTests
{
    [Fact]
    public void Rfc4122Bytes_RoundTrip()
    {
        var guid = Guid.Parse("8E8D0F2A-1DB7-4C7D-9D1F-05D5B8D8E2A1");
        var expected = new byte[]
        {
            0x8E, 0x8D, 0x0F, 0x2A,
            0x1D, 0xB7,
            0x4C, 0x7D,
            0x9D, 0x1F, 0x05, 0xD5, 0xB8, 0xD8, 0xE2, 0xA1
        };

        var bytes = UuidEncoding.ToRfc4122Bytes(guid);

        Assert.Equal(expected, bytes);
        Assert.Equal(guid, UuidEncoding.FromRfc4122Bytes(bytes));
    }
}
