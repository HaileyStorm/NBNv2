namespace Nbn.Runtime.Artifacts;

internal sealed class ChunkStore
{
    private readonly string _rootPath;

    public ChunkStore(string rootPath)
    {
        if (string.IsNullOrWhiteSpace(rootPath))
        {
            throw new ArgumentException("Root path is required.", nameof(rootPath));
        }

        _rootPath = rootPath;
    }

    public string GetChunkPath(Sha256Hash hash)
    {
        var hex = hash.ToHex();
        if (string.IsNullOrEmpty(hex) || hex.Length < 2)
        {
            throw new ArgumentException("Invalid chunk hash.", nameof(hash));
        }

        var prefix = hex[..2];
        return Path.Combine(_rootPath, prefix, hex);
    }

    public async Task<bool> TryWriteChunkAsync(Sha256Hash hash, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        var path = GetChunkPath(hash);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        try
        {
            await using var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.Read, 64 * 1024, useAsync: true);
            await stream.WriteAsync(data, cancellationToken);
            await stream.FlushAsync(cancellationToken);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
    }

    public Stream OpenRead(Sha256Hash hash)
    {
        var path = GetChunkPath(hash);
        return new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 64 * 1024, useAsync: true);
    }
}
