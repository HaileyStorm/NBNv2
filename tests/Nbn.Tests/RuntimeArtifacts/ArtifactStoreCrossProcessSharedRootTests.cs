using Microsoft.Data.Sqlite;
using Nbn.Runtime.Artifacts;
using Nbn.Tests.Format;
using Nbn.Tests.TestSupport;

namespace Nbn.Tests.Artifacts;

[Collection("ArtifactEnvSerial")]
public sealed class ArtifactStoreCrossProcessSharedRootTests
{
    [Fact]
    public async Task SharedRoot_ConcurrentOverlappingStores_AcrossProcesses_ReusesSharedChunkMetadata()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-crossproc-overlap-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var parentStore = CreateStore(artifactRoot);
            using var harness = new CrossProcessArtifactStoreHarness();
            var sharedChunk = CreateChunk(0x41);
            var childPayload = JoinChunks(sharedChunk, CreateChunk(0x77));
            var parentPayload = JoinChunks(sharedChunk, CreateChunk(0x99));
            var childPayloadPath = CrossProcessArtifactStoreHarness.GetWorkerPayloadPath(artifactRoot, "child-overlap");
            var readySignalPath = Path.Combine(artifactRoot, "child-ready.signal");
            var releaseSignalPath = Path.Combine(artifactRoot, "child-release.signal");

            await File.WriteAllBytesAsync(childPayloadPath, childPayload);

            using var worker = harness.StartWorker(
                "blocking-store",
                artifactRoot,
                childPayloadPath,
                "application/x-nbs",
                readySignalPath,
                releaseSignalPath);

            await CrossProcessArtifactStoreHarness.WaitForFileAsync(readySignalPath, TimeSpan.FromSeconds(30));

            var parentStoreTask = parentStore.StoreAsync(new MemoryStream(parentPayload), "application/x-nbs");
            await Task.Delay(100);
            await File.WriteAllTextAsync(releaseSignalPath, "go");

            var parentManifest = await parentStoreTask;
            await worker.WaitForSuccessAsync(TimeSpan.FromSeconds(30));

            var chunkRows = await ReadChunkRowsAsync(Path.Combine(artifactRoot, "artifacts.db"));
            var artifactRows = await ReadArtifactRowsAsync(Path.Combine(artifactRoot, "artifacts.db"));

            Assert.Equal(2, artifactRows.Count);
            Assert.Equal(3, chunkRows.Count);
            var sharedRow = Assert.Single(chunkRows, row => row.ChunkHex == parentManifest.Chunks[0].Hash.ToHex());
            Assert.Equal(2, sharedRow.RefCount);
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
    public async Task SharedRoot_DuplicateArtifactRegionIndexEnrichment_AcrossProcesses_PersistsMergedManifest()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-crossproc-merge-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var parentStore = CreateStore(artifactRoot);
            using var harness = new CrossProcessArtifactStoreHarness();
            var vector = NbnTestVectors.CreateRichNbnVector();
            var payloadPath = CrossProcessArtifactStoreHarness.GetWorkerPayloadPath(artifactRoot, "child-nonseekable");

            await File.WriteAllBytesAsync(payloadPath, vector.Bytes);

            using var worker = harness.StartWorker(
                "nonseekable-store",
                artifactRoot,
                payloadPath,
                "application/x-nbn");

            await worker.WaitForSuccessAsync(TimeSpan.FromSeconds(30));

            var manifest = await parentStore.StoreAsync(new MemoryStream(vector.Bytes), "application/x-nbn");
            var persistedManifest = await parentStore.TryGetManifestAsync(manifest.ArtifactId);

            Assert.NotNull(persistedManifest);
            Assert.NotEmpty(manifest.RegionIndex);
            Assert.Equal(manifest.RegionIndex, persistedManifest!.RegionIndex);
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
    public async Task SharedRoot_CrossProcessDatabaseWriteLock_WaitsInsteadOfFailingBusy()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-artifact-crossproc-db-lock-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var parentStore = CreateStore(artifactRoot);
            using var harness = new CrossProcessArtifactStoreHarness();
            var readySignalPath = Path.Combine(artifactRoot, "db-lock-ready.signal");
            var releaseSignalPath = Path.Combine(artifactRoot, "db-lock-release.signal");
            var payload = JoinChunks(
                CreateChunk(0x11),
                CreateChunk(0x22));
            var payloadPath = CrossProcessArtifactStoreHarness.GetWorkerPayloadPath(artifactRoot, "db-lock-payload");

            await File.WriteAllBytesAsync(payloadPath, payload);

            using var worker = harness.StartWorker(
                "hold-db-write-lock",
                artifactRoot,
                payloadPath,
                "application/x-nbs",
                readySignalPath,
                releaseSignalPath);

            await CrossProcessArtifactStoreHarness.WaitForFileAsync(readySignalPath, TimeSpan.FromSeconds(30));

            var parentStoreTask = parentStore.StoreAsync(new MemoryStream(payload), "application/x-nbs");
            await Task.Delay(200);
            Assert.False(parentStoreTask.IsCompleted);

            await File.WriteAllTextAsync(releaseSignalPath, "release");

            var manifest = await parentStoreTask.WaitAsync(TimeSpan.FromSeconds(30));
            await worker.WaitForSuccessAsync(TimeSpan.FromSeconds(30));

            Assert.NotEqual(default, manifest.ArtifactId);
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
}
