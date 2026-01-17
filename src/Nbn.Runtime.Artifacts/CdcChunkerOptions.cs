namespace Nbn.Runtime.Artifacts;

public sealed class CdcChunkerOptions
{
    public int MinChunkSize { get; init; } = 512 * 1024;
    public int AvgChunkSize { get; init; } = 2 * 1024 * 1024;
    public int MaxChunkSize { get; init; } = 8 * 1024 * 1024;
    public int ReadBufferSize { get; init; } = 256 * 1024;

    public void Validate()
    {
        if (MinChunkSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(MinChunkSize));
        }

        if (AvgChunkSize < MinChunkSize)
        {
            throw new ArgumentOutOfRangeException(nameof(AvgChunkSize));
        }

        if (MaxChunkSize < AvgChunkSize)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxChunkSize));
        }

        if (ReadBufferSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(ReadBufferSize));
        }
    }
}
