using System.Buffers;

namespace Nbn.Runtime.Artifacts;

internal sealed class ArtifactRangeStream : Stream
{
    private readonly Stream _inner;
    private readonly long _offset;
    private readonly long _length;
    private bool _initialized;
    private long _position;

    public ArtifactRangeStream(Stream inner, long offset, long length)
    {
        ArtifactRangeSupport.ValidateRange(offset, length);
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _offset = offset;
        _length = length;
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

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var remaining = _length - _position;
        var slice = buffer.Length > remaining
            ? buffer[..checked((int)remaining)]
            : buffer;

        var read = await _inner.ReadAsync(slice, cancellationToken).ConfigureAwait(false);
        _position += read;
        return read;
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
            _inner.Dispose();
        }

        base.Dispose(disposing);
    }

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
        {
            return;
        }

        _initialized = true;
        if (_offset == 0)
        {
            return;
        }

        if (_inner.CanSeek)
        {
            _inner.Seek(_offset, SeekOrigin.Begin);
            return;
        }

        var remaining = _offset;
        var buffer = ArrayPool<byte>.Shared.Rent((int)Math.Min(81920, remaining));
        try
        {
            while (remaining > 0)
            {
                var toRead = (int)Math.Min(buffer.Length, remaining);
                var read = await _inner.ReadAsync(buffer.AsMemory(0, toRead), cancellationToken).ConfigureAwait(false);
                if (read == 0)
                {
                    throw new EndOfStreamException($"Unable to skip {_offset} bytes in artifact stream.");
                }

                remaining -= read;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
