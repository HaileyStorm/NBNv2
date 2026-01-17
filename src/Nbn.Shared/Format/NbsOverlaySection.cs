namespace Nbn.Shared.Format;

public sealed class NbsOverlaySection
{
    public NbsOverlaySection(NbsOverlayRecord[] records, int byteLength)
    {
        Records = records;
        ByteLength = byteLength;
    }

    public NbsOverlayRecord[] Records { get; }
    public int ByteLength { get; }
}