using System;

namespace Nbn.Shared.Quantization;

public readonly struct QuantizationMap
{
    public QuantizationMap(QuantMapType mapType, float min, float max, float gamma)
    {
        MapType = mapType;
        Min = min;
        Max = max;
        Gamma = gamma <= 0f ? 1f : gamma;
    }

    public QuantMapType MapType { get; }
    public float Min { get; }
    public float Max { get; }
    public float Gamma { get; }

    public float Decode(int code, int bits)
    {
        var maxCode = MaxCode(bits);
        if (code < 0)
        {
            code = 0;
        }
        else if (code > maxCode)
        {
            code = maxCode;
        }

        return MapType switch
        {
            QuantMapType.LinearUnsigned => DecodeUnsigned(code, maxCode, false),
            QuantMapType.GammaUnsigned => DecodeUnsigned(code, maxCode, true),
            QuantMapType.LinearSignedCentered => DecodeSignedCentered(code, maxCode, false),
            QuantMapType.GammaSignedCentered => DecodeSignedCentered(code, maxCode, true),
            _ => throw new ArgumentOutOfRangeException(nameof(MapType), MapType, "Unknown quantization map type.")
        };
    }

    public int Encode(float value, int bits)
    {
        var maxCode = MaxCode(bits);
        if (maxCode == 0)
        {
            return 0;
        }

        return MapType switch
        {
            QuantMapType.LinearUnsigned => EncodeUnsigned(value, maxCode, false),
            QuantMapType.GammaUnsigned => EncodeUnsigned(value, maxCode, true),
            QuantMapType.LinearSignedCentered => EncodeSignedCentered(value, maxCode, false),
            QuantMapType.GammaSignedCentered => EncodeSignedCentered(value, maxCode, true),
            _ => throw new ArgumentOutOfRangeException(nameof(MapType), MapType, "Unknown quantization map type.")
        };
    }

    public static int MaxCode(int bits)
    {
        if (bits <= 0 || bits > 30)
        {
            throw new ArgumentOutOfRangeException(nameof(bits), bits, "Bit width must be between 1 and 30.");
        }

        return (1 << bits) - 1;
    }

    private float DecodeUnsigned(int code, int maxCode, bool useGamma)
    {
        var range = Max - Min;
        if (range == 0f)
        {
            return Min;
        }

        var u = code / (float)maxCode;
        if (useGamma)
        {
            u = MathF.Pow(u, Gamma);
        }

        return Min + (u * range);
    }

    private float DecodeSignedCentered(int code, int maxCode, bool useGamma)
    {
        var maxAbs = MathF.Max(MathF.Abs(Min), MathF.Abs(Max));
        if (maxAbs == 0f)
        {
            return 0f;
        }

        var t = DecodeSignedCenteredUnit(code, maxCode);
        if (useGamma)
        {
            t = MathF.Sign(t) * MathF.Pow(MathF.Abs(t), Gamma);
        }

        var value = t * maxAbs;
        return Math.Clamp(value, Min, Max);
    }

    private int EncodeUnsigned(float value, int maxCode, bool useGamma)
    {
        var range = Max - Min;
        if (range == 0f)
        {
            return 0;
        }

        var clamped = Math.Clamp(value, Min, Max);
        var u = (clamped - Min) / range;
        if (useGamma)
        {
            u = MathF.Pow(u, 1f / Gamma);
        }

        var code = (int)MathF.Round(u * maxCode);
        return Math.Clamp(code, 0, maxCode);
    }

    private int EncodeSignedCentered(float value, int maxCode, bool useGamma)
    {
        var maxAbs = MathF.Max(MathF.Abs(Min), MathF.Abs(Max));
        var centerHi = (maxCode + 1) / 2;
        var centerLo = centerHi - 1;

        if (maxAbs == 0f || centerLo <= 0)
        {
            return Math.Clamp(centerHi, 0, maxCode);
        }

        var clamped = Math.Clamp(value, Min, Max);
        var t = clamped / maxAbs;
        t = Math.Clamp(t, -1f, 1f);
        if (useGamma)
        {
            t = MathF.Sign(t) * MathF.Pow(MathF.Abs(t), 1f / Gamma);
        }

        int code;
        if (t >= 0f)
        {
            code = centerHi + (int)MathF.Round(t * centerLo);
        }
        else
        {
            code = centerLo + (int)MathF.Round(t * centerLo);
        }

        return Math.Clamp(code, 0, maxCode);
    }

    private static float DecodeSignedCenteredUnit(int code, int maxCode)
    {
        var centerHi = (maxCode + 1) / 2;
        var centerLo = centerHi - 1;
        if (centerLo <= 0)
        {
            return 0f;
        }

        if (code == centerLo || code == centerHi)
        {
            return 0f;
        }

        if (code < centerLo)
        {
            var k = code - centerLo;
            return k / (float)centerLo;
        }

        var kPos = code - centerHi;
        return kPos / (float)centerLo;
    }
}