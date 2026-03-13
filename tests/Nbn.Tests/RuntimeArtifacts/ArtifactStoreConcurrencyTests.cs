using Microsoft.Data.Sqlite;
using Nbn.Runtime.Artifacts;
using System.Threading.Tasks;

namespace Nbn.Tests.Artifacts;

public sealed class ArtifactStoreConcurrencyTests
{
    [Fact]
    public async Task StoreAsync_ConcurrentOverlappingArtifacts_WaitsForCommittedChunkMetadata()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-concurrency-overlap-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var storeA = CreateStore(artifactRoot);
            var storeB = CreateStore(artifactRoot);
            var sharedChunk = CreateChunk(0x41);
            var artifactABytes = JoinChunks(sharedChunk, CreateChunk(0x99));
            var artifactBBytes = sharedChunk;

            using var blockingStream = new BlockingAfterFirstReadStream(artifactABytes, ChunkSize);
            var storeATask = storeA.StoreAsync(blockingStream, "application/x-nbs");

            await blockingStream.WaitUntilBlockedAsync();

            var storeBTask = storeB.StoreAsync(new MemoryStream(artifactBBytes), "application/x-nbs");
            await Task.Delay(TimeSpan.FromMilliseconds(5500));
            blockingStream.Release();

            var manifestA = await storeATask.WaitAsync(TimeSpan.FromSeconds(5));
            var manifestB = await storeBTask.WaitAsync(TimeSpan.FromSeconds(5));
            var chunkRows = await ReadChunkRowsAsync(Path.Combine(artifactRoot, "artifacts.db"));

            Assert.Equal(2, chunkRows.Count);
            var sharedRow = Assert.Single(chunkRows, row => row.ChunkHex == manifestB.Chunks.Single().Hash.ToHex());
            Assert.Equal(2, sharedRow.RefCount);
            Assert.Equal(artifactABytes, await ReadArtifactBytesAsync(storeA, manifestA.ArtifactId));
            Assert.Equal(artifactBBytes, await ReadArtifactBytesAsync(storeB, manifestB.ArtifactId));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task StoreAsync_ConcurrentDuplicateArtifacts_ReusesSingleArtifactRow_WithoutThrowing()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-concurrency-duplicate-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var storeA = CreateStore(artifactRoot);
            var storeB = CreateStore(artifactRoot);
            var payload = JoinChunks(
                CreateChunk(0x51),
                CreateChunk(0x52));

            using var blockingStream = new BlockingAfterFirstReadStream(payload, ChunkSize);
            var storeATask = storeA.StoreAsync(blockingStream, "application/x-nbs");

            await blockingStream.WaitUntilBlockedAsync();

            var storeBTask = storeB.StoreAsync(new MemoryStream(payload), "application/x-nbs");
            await Task.Delay(100);
            blockingStream.Release();

            var manifestA = await storeATask.WaitAsync(TimeSpan.FromSeconds(5));
            var manifestB = await storeBTask.WaitAsync(TimeSpan.FromSeconds(5));
            var artifactRows = await ReadArtifactRowsAsync(Path.Combine(artifactRoot, "artifacts.db"));
            var chunkRows = await ReadChunkRowsAsync(Path.Combine(artifactRoot, "artifacts.db"));

            Assert.Equal(manifestA.ArtifactId, manifestB.ArtifactId);
            Assert.Single(artifactRows);
            Assert.All(chunkRows, row => Assert.Equal(1, row.RefCount));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
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

    private static byte[] CreateChunk(byte value)
    {
        var buffer = new byte[ChunkSize];
        Array.Fill(buffer, value);
        return buffer;
    }

    private static byte[] JoinChunks(params byte[][] chunks)
    {
        var payload = new byte[chunks.Sum(chunk => chunk.Length)];
        var offset = 0;
        foreach (var chunk in chunks)
        {
            Buffer.BlockCopy(chunk, 0, payload, offset, chunk.Length);
            offset += chunk.Length;
        }

        return payload;
    }

    private static async Task<byte[]> ReadArtifactBytesAsync(LocalArtifactStore store, Sha256Hash artifactId)
    {
        await using var stream = await store.TryOpenArtifactAsync(artifactId);
        Assert.NotNull(stream);

        using var buffer = new MemoryStream();
        await stream!.CopyToAsync(buffer);
        return buffer.ToArray();
    }

    private static async Task<List<ChunkRow>> ReadChunkRowsAsync(string databasePath)
    {
        await using var connection = new SqliteConnection($"Data Source={databasePath}");
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT lower(hex(chunk_sha256)),
       ref_count
FROM chunks
ORDER BY lower(hex(chunk_sha256));";

        var rows = new List<ChunkRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new ChunkRow(
                reader.GetString(0),
                reader.GetInt64(1)));
        }

        return rows;
    }

    private static async Task<List<ArtifactRow>> ReadArtifactRowsAsync(string databasePath)
    {
        await using var connection = new SqliteConnection($"Data Source={databasePath}");
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT lower(hex(artifact_sha256)),
       ref_count
FROM artifacts
ORDER BY lower(hex(artifact_sha256));";

        var rows = new List<ArtifactRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new ArtifactRow(
                reader.GetString(0),
                reader.GetInt64(1)));
        }

        return rows;
    }

    private const int ChunkSize = 4096;

    private sealed record ChunkRow(string ChunkHex, long RefCount);

    private sealed record ArtifactRow(string ArtifactHex, long RefCount);

    private sealed class BlockingAfterFirstReadStream : Stream
    {
        private readonly byte[] _data;
        private readonly int _firstReadBytes;
        private readonly TaskCompletionSource _blockedOnSecondRead = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _resumeSecondRead = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _position;
        private int _readCount;

        public BlockingAfterFirstReadStream(byte[] data, int firstReadBytes)
        {
            _data = data;
            _firstReadBytes = firstReadBytes;
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

        public Task WaitUntilBlockedAsync() => _blockedOnSecondRead.Task;

        public void Release() => _resumeSecondRead.TrySetResult();

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
            => throw new NotSupportedException();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _readCount++;
            if (_readCount == 2)
            {
                _blockedOnSecondRead.TrySetResult();
                await _resumeSecondRead.Task.WaitAsync(cancellationToken);
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
