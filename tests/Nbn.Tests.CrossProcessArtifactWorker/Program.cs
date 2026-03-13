using Nbn.Runtime.Artifacts;
using Microsoft.Data.Sqlite;

return await ProgramEntry.RunAsync(args);

internal static class ProgramEntry
{
    private const int ChunkSize = 4096;

    public static async Task<int> RunAsync(string[] args)
    {
        try
        {
            var options = WorkerOptions.Parse(args);
            var store = CreateStore(options.RootPath);
            var payload = await File.ReadAllBytesAsync(options.PayloadPath);

            Stream stream = options.Mode switch
            {
                WorkerMode.BlockingStore => new BlockingAfterFirstReadStream(payload, ChunkSize, options.ReadySignalPath!, options.ReleaseSignalPath!),
                WorkerMode.NonSeekableStore => new NonSeekableReadStream(payload),
                WorkerMode.HoldDbWriteLock => Stream.Null,
                _ => new MemoryStream(payload, writable: false)
            };

            if (options.Mode == WorkerMode.HoldDbWriteLock)
            {
                await HoldDbWriteLockAsync(options);
            }
            else
            {
                await using (stream.ConfigureAwait(false))
                {
                    await store.StoreAsync(stream, options.MediaType);
                }
            }

            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex);
            return 1;
        }
    }

    private static LocalArtifactStore CreateStore(string artifactRoot)
        => new(new ArtifactStoreOptions(artifactRoot)
        {
            Chunking = new CdcChunkerOptions
            {
                MinChunkSize = ChunkSize,
                AvgChunkSize = ChunkSize,
                MaxChunkSize = ChunkSize,
                ReadBufferSize = ChunkSize
            }
        });

    private sealed class WorkerOptions
    {
        public required WorkerMode Mode { get; init; }
        public required string RootPath { get; init; }
        public required string PayloadPath { get; init; }
        public required string MediaType { get; init; }
        public string? ReadySignalPath { get; init; }
        public string? ReleaseSignalPath { get; init; }

        public static WorkerOptions Parse(string[] args)
        {
            if (args.Length < 4)
            {
                throw new ArgumentException("Expected arguments: <mode> <rootPath> <payloadPath> <mediaType> [readySignalPath] [releaseSignalPath].");
            }

            var mode = args[0] switch
            {
                "store" => WorkerMode.Store,
                "blocking-store" => WorkerMode.BlockingStore,
                "nonseekable-store" => WorkerMode.NonSeekableStore,
                "hold-db-write-lock" => WorkerMode.HoldDbWriteLock,
                _ => throw new ArgumentOutOfRangeException(nameof(args), args[0], "Unsupported worker mode.")
            };

            return new WorkerOptions
            {
                Mode = mode,
                RootPath = args[1],
                PayloadPath = args[2],
                MediaType = args[3],
                ReadySignalPath = args.Length > 4 && !string.Equals(args[4], "-", StringComparison.Ordinal) ? args[4] : null,
                ReleaseSignalPath = args.Length > 5 && !string.Equals(args[5], "-", StringComparison.Ordinal) ? args[5] : null
            };
        }
    }

    private enum WorkerMode
    {
        Store,
        BlockingStore,
        NonSeekableStore,
        HoldDbWriteLock
    }

    private static async Task HoldDbWriteLockAsync(WorkerOptions options)
    {
        var store = CreateStore(options.RootPath);
        await store.ContainsAsync(new Sha256Hash(new byte[32]));

        var databasePath = Path.Combine(options.RootPath, "artifacts.db");
        await using var connection = new SqliteConnection($"Data Source={databasePath}");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = @"
PRAGMA busy_timeout=30000;
CREATE TABLE IF NOT EXISTS lock_probe (id INTEGER PRIMARY KEY);
BEGIN IMMEDIATE;
INSERT INTO lock_probe DEFAULT VALUES;";
        await command.ExecuteNonQueryAsync();

        if (options.ReadySignalPath is not null)
        {
            await File.WriteAllTextAsync(options.ReadySignalPath, "ready");
        }

        if (options.ReleaseSignalPath is not null)
        {
            while (!File.Exists(options.ReleaseSignalPath))
            {
                await Task.Delay(25);
            }
        }

        await using var releaseCommand = connection.CreateCommand();
        releaseCommand.CommandText = "ROLLBACK;";
        await releaseCommand.ExecuteNonQueryAsync();
    }

    private sealed class NonSeekableReadStream : Stream
    {
        private readonly byte[] _data;
        private int _position;

        public NonSeekableReadStream(byte[] data)
        {
            _data = data;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => _position;
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var bytesToCopy = Math.Min(count, _data.Length - _position);
            if (bytesToCopy <= 0)
            {
                return 0;
            }

            Buffer.BlockCopy(_data, _position, buffer, offset, bytesToCopy);
            _position += bytesToCopy;
            return bytesToCopy;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var bytesToCopy = Math.Min(buffer.Length, _data.Length - _position);
            if (bytesToCopy <= 0)
            {
                return ValueTask.FromResult(0);
            }

            _data.AsMemory(_position, bytesToCopy).CopyTo(buffer);
            _position += bytesToCopy;
            return ValueTask.FromResult(bytesToCopy);
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class BlockingAfterFirstReadStream : Stream
    {
        private readonly byte[] _data;
        private readonly int _firstReadBytes;
        private readonly string _readySignalPath;
        private readonly string _releaseSignalPath;
        private int _position;
        private int _readCount;

        public BlockingAfterFirstReadStream(byte[] data, int firstReadBytes, string readySignalPath, string releaseSignalPath)
        {
            _data = data;
            _firstReadBytes = firstReadBytes;
            _readySignalPath = readySignalPath;
            _releaseSignalPath = releaseSignalPath;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => _position;
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _readCount++;
            if (_readCount == 2)
            {
                await File.WriteAllTextAsync(_readySignalPath, "ready", cancellationToken);
                while (!File.Exists(_releaseSignalPath))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await Task.Delay(25, cancellationToken);
                }
            }

            var remaining = _data.Length - _position;
            if (remaining <= 0)
            {
                return 0;
            }

            var bytesToCopy = Math.Min(buffer.Length, remaining);
            if (_readCount == 1)
            {
                bytesToCopy = Math.Min(bytesToCopy, _firstReadBytes);
            }

            _data.AsMemory(_position, bytesToCopy).CopyTo(buffer);
            _position += bytesToCopy;
            return bytesToCopy;
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
