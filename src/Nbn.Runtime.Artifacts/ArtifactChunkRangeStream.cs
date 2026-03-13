namespace Nbn.Runtime.Artifacts;

internal sealed class ArtifactChunkRangeStream : Stream
{
    private readonly ChunkStore _chunkStore;
    private readonly IReadOnlyList<ArtifactChunkInfo> _chunks;
    private readonly long _length;
    private readonly int _startChunkOffset;
    private int _chunkIndex;
    private Stream? _current;
    private bool _startChunkOpened;
    private long _position;

    public ArtifactChunkRangeStream(ChunkStore chunkStore, IReadOnlyList<ArtifactChunkInfo> chunks, long offset, long length)
    {
        ArtifactRangeSupport.ValidateRange(offset, length);
        _chunkStore = chunkStore ?? throw new ArgumentNullException(nameof(chunkStore));
        _chunks = chunks ?? throw new ArgumentNullException(nameof(chunks));
        _length = length;

        var totalLength = 0L;
        foreach (var chunk in _chunks)
        {
            totalLength += chunk.UncompressedLength;
        }

        if (offset > totalLength || offset + length > totalLength)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "Requested range exceeds artifact length.");
        }

        if (length == 0)
        {
            _chunkIndex = _chunks.Count;
            _startChunkOffset = 0;
            return;
        }

        var cursor = 0L;
        for (var i = 0; i < _chunks.Count; i++)
        {
            var nextCursor = cursor + _chunks[i].UncompressedLength;
            if (offset < nextCursor)
            {
                _chunkIndex = i;
                _startChunkOffset = checked((int)(offset - cursor));
                return;
            }

            cursor = nextCursor;
        }

        _chunkIndex = _chunks.Count;
        _startChunkOffset = 0;
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
        => ReadAsync(buffer.AsMemory(offset, count)).GetAwaiter().GetResult();

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (buffer.Length == 0 || _position >= _length)
        {
            return 0;
        }

        var totalRead = 0;
        while (totalRead < buffer.Length && _position < _length)
        {
            if (_current is null && !TryOpenNextChunk())
            {
                break;
            }

            var current = _current;
            if (current is null)
            {
                break;
            }

            var remaining = _length - _position;
            var toRead = (int)Math.Min(buffer.Length - totalRead, remaining);
            var read = await current.ReadAsync(buffer.Slice(totalRead, toRead), cancellationToken).ConfigureAwait(false);
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
        if (!_startChunkOpened && _startChunkOffset > 0)
        {
            _current.Seek(_startChunkOffset, SeekOrigin.Begin);
        }

        _startChunkOpened = true;
        return true;
    }
}
