namespace Nbn.Shared.Quantization;

public readonly struct NbnQuantizationSchema
{
    public NbnQuantizationSchema(
        QuantizationMap strength,
        QuantizationMap preActivationThreshold,
        QuantizationMap activationThreshold,
        QuantizationMap paramA,
        QuantizationMap paramB)
    {
        Strength = strength;
        PreActivationThreshold = preActivationThreshold;
        ActivationThreshold = activationThreshold;
        ParamA = paramA;
        ParamB = paramB;
    }

    public QuantizationMap Strength { get; }
    public QuantizationMap PreActivationThreshold { get; }
    public QuantizationMap ActivationThreshold { get; }
    public QuantizationMap ParamA { get; }
    public QuantizationMap ParamB { get; }
}

public static class QuantizationSchemas
{
    public static readonly NbnQuantizationSchema DefaultNbn = new(
        new QuantizationMap(QuantMapType.GammaSignedCentered, -1f, 1f, 2f),
        new QuantizationMap(QuantMapType.GammaSignedCentered, -1f, 1f, 2f),
        new QuantizationMap(QuantMapType.GammaUnsigned, 0f, 1f, 2f),
        new QuantizationMap(QuantMapType.GammaSignedCentered, -3f, 3f, 2f),
        new QuantizationMap(QuantMapType.GammaSignedCentered, -3f, 3f, 2f));

    public static readonly QuantizationMap DefaultBuffer =
        new(QuantMapType.GammaSignedCentered, -4f, 4f, 2f);
}