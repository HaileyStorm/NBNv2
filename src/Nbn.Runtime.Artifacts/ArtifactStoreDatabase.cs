using Dapper;
using Microsoft.Data.Sqlite;

namespace Nbn.Runtime.Artifacts;

internal sealed class ArtifactStoreDatabase
{
    private readonly string _connectionString;

    public ArtifactStoreDatabase(string databasePath)
    {
        if (string.IsNullOrWhiteSpace(databasePath))
        {
            throw new ArgumentException("Database path is required.", nameof(databasePath));
        }

        _connectionString = $"Data Source={databasePath}";
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await connection.ExecuteAsync(
            "PRAGMA journal_mode=WAL;" +
            "PRAGMA synchronous=NORMAL;" +
            "PRAGMA foreign_keys=ON;");

        await connection.ExecuteAsync(@"
CREATE TABLE IF NOT EXISTS artifacts (
    artifact_sha256 BLOB(32) PRIMARY KEY,
    media_type TEXT NOT NULL,
    byte_length INTEGER NOT NULL,
    created_ms INTEGER NOT NULL,
    manifest_sha256 BLOB(32) NOT NULL,
    ref_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_sha256 BLOB(32) PRIMARY KEY,
    byte_length INTEGER NOT NULL,
    stored_length INTEGER NOT NULL,
    compression TEXT NOT NULL,
    ref_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS artifact_chunks (
    artifact_sha256 BLOB(32) NOT NULL,
    seq INTEGER NOT NULL,
    chunk_sha256 BLOB(32) NOT NULL,
    chunk_uncompressed_length INTEGER NOT NULL,
    PRIMARY KEY (artifact_sha256, seq)
);

CREATE TABLE IF NOT EXISTS artifact_region_index (
    artifact_sha256 BLOB(32) NOT NULL,
    region_id INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    length INTEGER NOT NULL,
    PRIMARY KEY (artifact_sha256, region_id)
);
");
    }

    public async Task<bool> ArtifactExistsAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var exists = await connection.ExecuteScalarAsync<long?>(
            "SELECT 1 FROM artifacts WHERE artifact_sha256 = @Id LIMIT 1;",
            new { Id = artifactId.Bytes.ToArray() });

        return exists.HasValue;
    }

    public async Task<ArtifactManifest?> TryGetManifestAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var artifactRow = await connection.QuerySingleOrDefaultAsync<ArtifactRow>(
            "SELECT artifact_sha256 AS ArtifactSha256, media_type AS MediaType, byte_length AS ByteLength FROM artifacts WHERE artifact_sha256 = @Id;",
            new { Id = artifactId.Bytes.ToArray() });

        if (artifactRow is null)
        {
            return null;
        }

        var chunkRows = (await connection.QueryAsync<ArtifactChunkRow>(
            @"SELECT ac.seq AS Seq,
                     ac.chunk_sha256 AS ChunkSha256,
                     ac.chunk_uncompressed_length AS ChunkUncompressedLength,
                     c.stored_length AS StoredLength,
                     c.compression AS Compression
              FROM artifact_chunks ac
              JOIN chunks c ON ac.chunk_sha256 = c.chunk_sha256
              WHERE ac.artifact_sha256 = @Id
              ORDER BY ac.seq;",
            new { Id = artifactId.Bytes.ToArray() })).ToList();

        var chunks = new List<ArtifactChunkInfo>(chunkRows.Count);
        foreach (var row in chunkRows)
        {
            var compression = ChunkCompression.FromLabel(row.Compression);
            chunks.Add(new ArtifactChunkInfo(new Sha256Hash(row.ChunkSha256), row.ChunkUncompressedLength, row.StoredLength, compression));
        }

        var regionRows = (await connection.QueryAsync<ArtifactRegionRow>(
            "SELECT region_id AS RegionId, offset AS Offset, length AS Length FROM artifact_region_index WHERE artifact_sha256 = @Id ORDER BY region_id;",
            new { Id = artifactId.Bytes.ToArray() })).ToList();

        IReadOnlyList<ArtifactRegionIndexEntry> regionIndex = Array.Empty<ArtifactRegionIndexEntry>();
        if (regionRows.Count > 0)
        {
            var regionList = new List<ArtifactRegionIndexEntry>(regionRows.Count);
            foreach (var row in regionRows)
            {
                regionList.Add(new ArtifactRegionIndexEntry(row.RegionId, row.Offset, row.Length));
            }

            regionIndex = regionList;
        }

        return new ArtifactManifest(new Sha256Hash(artifactRow.ArtifactSha256), artifactRow.MediaType, artifactRow.ByteLength, chunks, regionIndex);
    }

    public async Task<bool> TryInsertArtifactAsync(
        ArtifactManifest manifest,
        Sha256Hash manifestHash,
        DateTimeOffset createdAt,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var exists = await connection.ExecuteScalarAsync<long?>(
            "SELECT 1 FROM artifacts WHERE artifact_sha256 = @Id LIMIT 1;",
            new { Id = manifest.ArtifactId.Bytes.ToArray() },
            transaction);

        if (exists.HasValue)
        {
            await transaction.RollbackAsync(cancellationToken);
            return false;
        }

        var chunkUpserts = new List<ChunkUpsertRow>(manifest.Chunks.Count);
        foreach (var chunk in manifest.Chunks)
        {
            chunkUpserts.Add(new ChunkUpsertRow(
                chunk.Hash.Bytes.ToArray(),
                chunk.UncompressedLength,
                chunk.StoredLength,
                ChunkCompression.ToLabel(chunk.Compression)));
        }

        const string chunkUpsertSql = @"
INSERT INTO chunks (chunk_sha256, byte_length, stored_length, compression, ref_count)
VALUES (@ChunkSha256, @ByteLength, @StoredLength, @Compression, 1)
ON CONFLICT(chunk_sha256) DO UPDATE SET ref_count = ref_count + 1;";

        try
        {
            if (chunkUpserts.Count > 0)
            {
                await connection.ExecuteAsync(chunkUpsertSql, chunkUpserts, transaction);
            }

            const string artifactInsertSql = @"
INSERT INTO artifacts (artifact_sha256, media_type, byte_length, created_ms, manifest_sha256, ref_count)
VALUES (@ArtifactSha256, @MediaType, @ByteLength, @CreatedMs, @ManifestSha256, 1);";

            await connection.ExecuteAsync(
                artifactInsertSql,
                new
                {
                    ArtifactSha256 = manifest.ArtifactId.Bytes.ToArray(),
                    MediaType = manifest.MediaType,
                    ByteLength = manifest.ByteLength,
                    CreatedMs = createdAt.ToUnixTimeMilliseconds(),
                    ManifestSha256 = manifestHash.Bytes.ToArray()
                },
                transaction);

        var chunkRows = new List<ArtifactChunkInsertRow>(manifest.Chunks.Count);
        for (var i = 0; i < manifest.Chunks.Count; i++)
        {
            var chunk = manifest.Chunks[i];
            chunkRows.Add(new ArtifactChunkInsertRow(manifest.ArtifactId.Bytes.ToArray(), i, chunk.Hash.Bytes.ToArray(), chunk.UncompressedLength));
        }

        if (chunkRows.Count > 0)
        {
            await connection.ExecuteAsync(
                "INSERT INTO artifact_chunks (artifact_sha256, seq, chunk_sha256, chunk_uncompressed_length) VALUES (@ArtifactSha256, @Seq, @ChunkSha256, @ChunkUncompressedLength);",
                chunkRows,
                transaction);
        }

        if (manifest.RegionIndex.Count > 0)
        {
            var regionRows = new List<ArtifactRegionInsertRow>(manifest.RegionIndex.Count);
            foreach (var entry in manifest.RegionIndex)
            {
                regionRows.Add(new ArtifactRegionInsertRow(manifest.ArtifactId.Bytes.ToArray(), entry.RegionId, entry.Offset, entry.Length));
            }

            await connection.ExecuteAsync(
                "INSERT INTO artifact_region_index (artifact_sha256, region_id, offset, length) VALUES (@ArtifactSha256, @RegionId, @Offset, @Length);",
                regionRows,
                transaction);
        }

            await transaction.CommitAsync(cancellationToken);
            return true;
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19)
        {
            await transaction.RollbackAsync(cancellationToken);
            return false;
        }
    }

    private sealed record ArtifactRow(byte[] ArtifactSha256, string MediaType, long ByteLength);

    private sealed record ArtifactChunkRow(int Seq, byte[] ChunkSha256, int ChunkUncompressedLength, int StoredLength, string Compression);

    private sealed record ArtifactRegionRow(int RegionId, long Offset, long Length);

    private sealed record ChunkUpsertRow(byte[] ChunkSha256, int ByteLength, int StoredLength, string Compression);

    private sealed record ArtifactChunkInsertRow(byte[] ArtifactSha256, int Seq, byte[] ChunkSha256, int ChunkUncompressedLength);

    private sealed record ArtifactRegionInsertRow(byte[] ArtifactSha256, int RegionId, long Offset, long Length);

    public async Task<ChunkMetadata?> TryGetChunkMetadataAsync(Sha256Hash chunkHash, CancellationToken cancellationToken = default)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var row = await connection.QuerySingleOrDefaultAsync<ChunkMetadata>(
            "SELECT byte_length AS ByteLength, stored_length AS StoredLength, compression AS Compression FROM chunks WHERE chunk_sha256 = @Id;",
            new { Id = chunkHash.Bytes.ToArray() });

        return row;
    }

    internal sealed record ChunkMetadata(int ByteLength, int StoredLength, string Compression);
}
