using Nbn.Shared.Quantization;
using Xunit;

namespace Nbn.Tests.Format;

public class QuantizationTests
{
    [Fact]
    public void LinearUnsigned_EncodesAndDecodesBounds()
    {
        var map = new QuantizationMap(QuantMapType.LinearUnsigned, 0f, 1f, 1f);
        var maxCode = QuantizationMap.MaxCode(6);

        Assert.Equal(0, map.Encode(0f, 6));
        Assert.Equal(maxCode, map.Encode(1f, 6));
        Assert.Equal(0, map.Encode(-1f, 6));
        Assert.Equal(maxCode, map.Encode(2f, 6));

        Assert.Equal(0f, map.Decode(0, 6), 6);
        Assert.Equal(1f, map.Decode(maxCode, 6), 6);
    }

    [Fact]
    public void SignedCentered_UsesCenterCodeForZero()
    {
        var map = new QuantizationMap(QuantMapType.LinearSignedCentered, -1f, 1f, 1f);
        var maxCode = QuantizationMap.MaxCode(6);
        var centerHi = (maxCode + 1) / 2;
        var centerLo = centerHi - 1;

        Assert.Equal(centerHi, map.Encode(0f, 6));
        Assert.Equal(0f, map.Decode(centerHi, 6), 6);
        Assert.Equal(0f, map.Decode(centerLo, 6), 6);
    }

    [Fact]
    public void GammaSignedCentered_EncodesWithinRange()
    {
        var map = new QuantizationMap(QuantMapType.GammaSignedCentered, -1f, 1f, 2f);
        var code = map.Encode(0.5f, 5);
        var decoded = map.Decode(code, 5);

        Assert.InRange(decoded, -1f, 1f);
    }

    [Fact]
    public void MaxCode_RejectsInvalidBits()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => QuantizationMap.MaxCode(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => QuantizationMap.MaxCode(31));
    }
}