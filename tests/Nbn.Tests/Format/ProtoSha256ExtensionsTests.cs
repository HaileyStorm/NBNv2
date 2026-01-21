using System.Linq;
using Nbn.Shared;
using Xunit;

namespace Nbn.Tests.Format;

public class ProtoSha256ExtensionsTests
{
    [Fact]
    public void Sha256_RoundTrips()
    {
        var bytes = Enumerable.Range(0, ProtoSha256Extensions.Sha256Length).Select(i => (byte)i).ToArray();
        var proto = bytes.ToProtoSha256();

        var roundTrip = proto.ToByteArray();

        Assert.Equal(bytes, roundTrip);
    }

    [Fact]
    public void TryToByteArray_FailsOnInvalidLength()
    {
        var proto = new Nbn.Proto.Sha256 { Value = Google.Protobuf.ByteString.CopyFrom(new byte[] { 0x01 }) };

        Assert.False(proto.TryToByteArray(out var bytes));
        Assert.Empty(bytes);
    }

    [Fact]
    public void Sha256_HexRoundTrips()
    {
        var bytes = Enumerable.Range(0, ProtoSha256Extensions.Sha256Length).Select(i => (byte)(255 - i)).ToArray();
        var proto = bytes.ToProtoSha256();

        var hex = proto.ToHex();
        var roundTrip = ProtoSha256Extensions.FromHex(hex).ToByteArray();

        Assert.Equal(bytes, roundTrip);
    }

    [Fact]
    public void TryFromHex_FailsOnInvalidInput()
    {
        Assert.False(ProtoSha256Extensions.TryFromHex("not-hex", out _));
        Assert.False(ProtoSha256Extensions.TryFromHex("0x1234", out _));
    }
}
