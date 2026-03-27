using System.Net.Http;
using System.Text;
using System.Text.Json;

namespace Nbn.Runtime.Artifacts;

internal static class ArtifactStoreHttpPayloads
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static void AddRegionIndexHeader(HttpRequestMessage request, ArtifactStoreWriteOptions? options)
    {
        ArgumentNullException.ThrowIfNull(request);

        var encodedRegionIndex = EncodeRegionIndex(options?.RegionIndex);
        if (encodedRegionIndex.Length == 0)
        {
            return;
        }

        request.Headers.TryAddWithoutValidation(HttpArtifactStoreHeaderNames.RegionIndex, encodedRegionIndex);
    }

    public static ArtifactStoreWriteOptions? ParseWriteOptions(string? encodedRegionIndex)
    {
        if (string.IsNullOrWhiteSpace(encodedRegionIndex))
        {
            return null;
        }

        var json = Encoding.UTF8.GetString(Convert.FromBase64String(encodedRegionIndex.Trim()));
        var entries = JsonSerializer.Deserialize<List<ArtifactRegionIndexEntry>>(json, JsonOptions);
        return entries is { Count: > 0 }
            ? new ArtifactStoreWriteOptions { RegionIndex = entries }
            : null;
    }

    public static byte[] SerializeManifest(ArtifactManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        return JsonSerializer.SerializeToUtf8Bytes(ArtifactManifestDto.FromManifest(manifest), JsonOptions);
    }

    public static async Task<ArtifactManifest> DeserializeManifestAsync(HttpContent content, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(content);

        await using var stream = await content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        var dto = await JsonSerializer.DeserializeAsync<ArtifactManifestDto>(stream, JsonOptions, cancellationToken).ConfigureAwait(false);
        if (dto is null)
        {
            throw new InvalidOperationException("Artifact store returned an empty manifest payload.");
        }

        return dto.ToManifest();
    }

    private static string EncodeRegionIndex(IReadOnlyList<ArtifactRegionIndexEntry>? regionIndex)
    {
        if (regionIndex is not { Count: > 0 })
        {
            return string.Empty;
        }

        var raw = JsonSerializer.Serialize(regionIndex, JsonOptions);
        return Convert.ToBase64String(Encoding.UTF8.GetBytes(raw));
    }

    private sealed class ArtifactManifestDto
    {
        public string ArtifactId { get; set; } = string.Empty;
        public string MediaType { get; set; } = string.Empty;
        public long ByteLength { get; set; }
        public List<ChunkDto> Chunks { get; set; } = [];
        public List<RegionIndexDto> RegionIndex { get; set; } = [];

        public static ArtifactManifestDto FromManifest(ArtifactManifest manifest)
            => new()
            {
                ArtifactId = manifest.ArtifactId.ToHex(),
                MediaType = manifest.MediaType,
                ByteLength = manifest.ByteLength,
                Chunks = manifest.Chunks
                    .Select(static chunk => new ChunkDto
                    {
                        Hash = chunk.Hash.ToHex(),
                        UncompressedLength = chunk.UncompressedLength,
                        StoredLength = chunk.StoredLength,
                        Compression = ChunkCompression.ToLabel(chunk.Compression)
                    })
                    .ToList(),
                RegionIndex = manifest.RegionIndex
                    .Select(static entry => new RegionIndexDto
                    {
                        RegionId = entry.RegionId,
                        Offset = entry.Offset,
                        Length = entry.Length
                    })
                    .ToList()
            };

        public ArtifactManifest ToManifest()
        {
            if (!Sha256Hash.TryParseHex(ArtifactId, out var artifactId))
            {
                throw new InvalidOperationException($"Artifact manifest contained invalid artifact id '{ArtifactId}'.");
            }

            var chunks = new List<ArtifactChunkInfo>(Chunks.Count);
            foreach (var chunk in Chunks)
            {
                if (!Sha256Hash.TryParseHex(chunk.Hash, out var chunkHash))
                {
                    throw new InvalidOperationException($"Artifact manifest contained invalid chunk hash '{chunk.Hash}'.");
                }

                chunks.Add(new ArtifactChunkInfo(
                    chunkHash,
                    chunk.UncompressedLength,
                    chunk.StoredLength,
                    ChunkCompression.FromLabel(chunk.Compression)));
            }

            var regionIndex = RegionIndex
                .Select(static entry => new ArtifactRegionIndexEntry(entry.RegionId, entry.Offset, entry.Length))
                .ToList();
            return new ArtifactManifest(artifactId, MediaType, ByteLength, chunks, regionIndex);
        }
    }

    private sealed class ChunkDto
    {
        public string Hash { get; set; } = string.Empty;
        public int UncompressedLength { get; set; }
        public int StoredLength { get; set; }
        public string Compression { get; set; } = ChunkCompression.NoneLabel;
    }

    private sealed class RegionIndexDto
    {
        public int RegionId { get; set; }
        public long Offset { get; set; }
        public long Length { get; set; }
    }
}
