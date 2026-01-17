namespace Nbn.Shared.Format;

public readonly struct NbsOverlayRecord
{
    public NbsOverlayRecord(uint fromAddress, uint toAddress, byte strengthCode)
    {
        FromAddress = fromAddress;
        ToAddress = toAddress;
        StrengthCode = strengthCode;
    }

    public uint FromAddress { get; }
    public uint ToAddress { get; }
    public byte StrengthCode { get; }
}