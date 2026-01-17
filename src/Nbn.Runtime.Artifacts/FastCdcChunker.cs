using System.Buffers;

namespace Nbn.Runtime.Artifacts;

public sealed class FastCdcChunker
{
    private readonly CdcChunkerOptions _options;
    private readonly ulong _maskSmall;
    private readonly ulong _maskLarge;

    public FastCdcChunker(CdcChunkerOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _options.Validate();

        var avgBits = Log2(_options.AvgChunkSize);
        if (avgBits < 2 || avgBits > 62)
        {
            throw new ArgumentOutOfRangeException(nameof(options.AvgChunkSize), "Average chunk size is out of range.");
        }

        _maskSmall = (1UL << (avgBits - 1)) - 1;
        _maskLarge = (1UL << (avgBits + 1)) - 1;
    }

    public async Task ChunkAsync(Stream input, Func<ReadOnlyMemory<byte>, ValueTask> onChunk, CancellationToken cancellationToken = default)
    {
        if (input is null)
        {
            throw new ArgumentNullException(nameof(input));
        }

        if (onChunk is null)
        {
            throw new ArgumentNullException(nameof(onChunk));
        }

        var readBufferSize = Math.Clamp(_options.ReadBufferSize, 4096, _options.MaxChunkSize);
        var readBuffer = ArrayPool<byte>.Shared.Rent(readBufferSize);
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_options.MaxChunkSize);

        var chunkSize = 0;
        var rolling = 0UL;

        try
        {
            while (true)
            {
                var read = await input.ReadAsync(readBuffer.AsMemory(0, readBufferSize), cancellationToken);
                if (read == 0)
                {
                    break;
                }

                for (var i = 0; i < read; i++)
                {
                    var value = readBuffer[i];
                    chunkBuffer[chunkSize++] = value;
                    rolling = (rolling >> 1) + GearTable[value];

                    if (chunkSize < _options.MinChunkSize)
                    {
                        continue;
                    }

                    var cut = chunkSize >= _options.MaxChunkSize;
                    if (!cut)
                    {
                        if (chunkSize <= _options.AvgChunkSize)
                        {
                            cut = (rolling & _maskSmall) == 0;
                        }
                        else
                        {
                            cut = (rolling & _maskLarge) == 0;
                        }
                    }

                    if (cut)
                    {
                        await onChunk(chunkBuffer.AsMemory(0, chunkSize));
                        chunkSize = 0;
                        rolling = 0;
                    }
                }
            }

            if (chunkSize > 0)
            {
                await onChunk(chunkBuffer.AsMemory(0, chunkSize));
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(readBuffer);
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
    }

    private static int Log2(int value)
    {
        var bits = 0;
        while (value > 1)
        {
            value >>= 1;
            bits++;
        }

        return bits;
    }

    private static readonly ulong[] GearTable = BuildGearTable();

    private static ulong[] BuildGearTable()
    {
        var table = new ulong[256];
        var seed = 0x9e3779b97f4a7c15UL;
        for (var i = 0; i < table.Length; i++)
        {
            seed ^= seed << 7;
            seed ^= seed >> 9;
            seed ^= seed << 8;
            table[i] = seed;
        }

        return table;
    }
}
