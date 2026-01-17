namespace Nbn.Shared.Quantization;

public enum QuantMapType : byte
{
    LinearSignedCentered = 0,
    LinearUnsigned = 1,
    GammaSignedCentered = 2,
    GammaUnsigned = 3
}