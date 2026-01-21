using System.Linq;
using Nbn.Proto;
using Nbn.Shared;
using Xunit;

namespace Nbn.Tests.Format;

public class ProtoArtifactExtensionsTests
{
    [Fact]
    public void ArtifactRef_RoundTripsSha256Bytes()
    {
        var bytes = Enumerable.Range(0, ProtoSha256Extensions.Sha256Length).Select(i => (byte)i).ToArray();
        var reference = bytes.ToArtifactRef(sizeBytes: 123, mediaType: "application/x-nbn", storeUri: "file:///tmp/nbn");

        var roundTrip = reference.ToSha256Bytes();

        Assert.Equal(bytes, roundTrip);
        Assert.Equal((ulong)123, reference.SizeBytes);
        Assert.Equal("application/x-nbn", reference.MediaType);
        Assert.Equal("file:///tmp/nbn", reference.StoreUri);
    }

    [Fact]
    public void ArtifactRef_HexHelpers_Work()
    {
        var bytes = Enumerable.Range(0, ProtoSha256Extensions.Sha256Length).Select(i => (byte)(i * 3 % 255)).ToArray();
        var reference = bytes.ToArtifactRef();

        var hex = reference.ToSha256Hex();
        var roundTrip = ProtoSha256Extensions.FromHex(hex).ToByteArray();

        Assert.Equal(bytes, roundTrip);
    }

    [Fact]
    public void ArtifactRef_TryHelpers_FailWhenMissingSha()
    {
        var reference = new ArtifactRef();

        Assert.False(reference.TryToSha256Bytes(out var bytes));
        Assert.Empty(bytes);

        Assert.False(reference.TryToSha256Hex(out var hex));
        Assert.Equal(string.Empty, hex);
    }
}
