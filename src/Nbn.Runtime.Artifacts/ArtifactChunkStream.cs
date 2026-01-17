namespace Nbn.Runtime.Artifacts;

internal sealed class ArtifactChunkStream : Stream
{
    private readonly ChunkStore _chunkStore;
    private readonly IReadOnlyList<ArtifactChunkInfo> _chunks;
    private readonly long _length;
    private int _chunkIndex;
    private Stream? _current;
    private long _position;

    public ArtifactChunkStream(ChunkStore chunkStore, IReadOnlyList<ArtifactChunkInfo> chunks)
    {
        _chunkStore = chunkStore ?? throw new ArgumentNullException(nameof(chunkStore));
        _chunks = chunks ?? throw new ArgumentNullException(nameof(chunks));

        var total = 0L;
        foreach (var chunk in _chunks)
        {
            total += chunk.UncompressedLength;
        }

        _length = total;
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _length;

    public override long Position
    {
        get => _position;
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return ReadAsync(buffer.AsMemory(offset, count)).GetAwaiter().GetResult();
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (buffer.Length == 0)
        {
            return 0;
        }

        var totalRead = 0;
        while (totalRead < buffer.Length)
        {
            if (_current is null)
            {
                if (!TryOpenNextChunk())
                {
                    break;
                }
            }

            var current = _current;
            if (current is null)
            {
                break;
            }

            var read = await current.ReadAsync(buffer.Slice(totalRead), cancellationToken);
            if (read == 0)
            {
                current.Dispose();
                _current = null;
                _chunkIndex++;
                continue;
            }

            totalRead += read;
            _position += read;
        }

        return totalRead;
    }

    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public override void Flush()
    {
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _current?.Dispose();
            _current = null;
        }

        base.Dispose(disposing);
    }

    private bool TryOpenNextChunk()
    {
        if (_chunkIndex >= _chunks.Count)
        {
            return false;
        }

        _current = _chunkStore.OpenRead(_chunks[_chunkIndex]);
        return true;
    }
}
