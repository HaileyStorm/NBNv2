using ZstdSharp;

namespace Nbn.Runtime.Artifacts;

internal static class ChunkCompression
{
    public const string NoneLabel = "none";
    public const string ZstdLabel = "zstd";

    public static string ToLabel(ChunkCompressionKind kind) => kind switch
    {
        ChunkCompressionKind.Zstd => ZstdLabel,
        _ => NoneLabel
    };

    public static ChunkCompressionKind FromLabel(string? label)
    {
        if (string.IsNullOrWhiteSpace(label))
        {
            return ChunkCompressionKind.None;
        }

        return label.Trim().ToLowerInvariant() switch
        {
            ZstdLabel => ChunkCompressionKind.Zstd,
            _ => ChunkCompressionKind.None
        };
    }

    public static byte[] CompressZstd(ReadOnlySpan<byte> data, int level)
    {
        using var compressor = new Compressor(level);
        return compressor.Wrap(data.ToArray()).ToArray();
    }

    public static byte[] DecompressZstd(ReadOnlySpan<byte> data, int expectedLength)
    {
        using var decompressor = new Decompressor();
        var result = decompressor.Unwrap(data.ToArray()).ToArray();
        if (expectedLength > 0 && result.Length != expectedLength)
        {
            throw new InvalidOperationException($"Decompressed length {result.Length} did not match expected {expectedLength}.");
        }

        return result;
    }
}
