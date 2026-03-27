using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Nbn.Runtime.Artifacts;

namespace Nbn.Tests.TestSupport;

public sealed class HttpArtifactStoreTestServer : IAsyncDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly TempDirectoryScope _rootPath;
    private readonly LocalArtifactStore _store;
    private readonly HttpListener _listener;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loopTask;

    public HttpArtifactStoreTestServer(bool supportsRangeRequests = true)
    {
        SupportsRangeRequests = supportsRangeRequests;
        _rootPath = TempDirectoryScope.Create("nbn-http-artifact-store", clearSqlitePools: true);
        _store = new LocalArtifactStore(new ArtifactStoreOptions(_rootPath));

        var port = ReservePort();
        BaseUri = new Uri($"http://127.0.0.1:{port}/", UriKind.Absolute);

        _listener = new HttpListener();
        _listener.Prefixes.Add(BaseUri.AbsoluteUri);
        _listener.Start();
        _loopTask = Task.Run(() => RunAsync(_cts.Token));
    }

    public Uri BaseUri { get; }
    public bool SupportsRangeRequests { get; }
    public int ManifestRequests { get; private set; }
    public int ArtifactRequests { get; private set; }
    public int RangeRequests { get; private set; }
    public int StoreRequests { get; private set; }

    public Task<ArtifactManifest> SeedAsync(byte[] payload, string mediaType, ArtifactStoreWriteOptions? options = null)
        => _store.StoreAsync(new MemoryStream(payload, writable: false), mediaType, options);

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _listener.Close();

        try
        {
            await _loopTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
        finally
        {
            _cts.Dispose();
        }
        _rootPath.Dispose();
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            HttpListenerContext context;
            try
            {
                context = await _listener.GetContextAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (HttpListenerException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }

            try
            {
                await HandleAsync(context, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await WriteErrorAsync(context.Response, HttpStatusCode.InternalServerError, ex.GetBaseException().Message, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                context.Response.Close();
            }
        }
    }

    private async Task HandleAsync(HttpListenerContext context, CancellationToken cancellationToken)
    {
        var request = context.Request;
        var response = context.Response;
        var segments = request.Url?.AbsolutePath
            .Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            ?? [];

        if (request.HttpMethod == HttpMethod.Post.Method
            && segments.Length == 2
            && string.Equals(segments[0], "v1", StringComparison.OrdinalIgnoreCase)
            && string.Equals(segments[1], "artifacts", StringComparison.OrdinalIgnoreCase))
        {
            await HandleStoreAsync(request, response, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (request.HttpMethod == HttpMethod.Get.Method
            && segments.Length == 3
            && string.Equals(segments[0], "v1", StringComparison.OrdinalIgnoreCase)
            && string.Equals(segments[1], "manifests", StringComparison.OrdinalIgnoreCase))
        {
            await HandleManifestAsync(segments[2], response, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (request.HttpMethod == HttpMethod.Get.Method
            && segments.Length == 3
            && string.Equals(segments[0], "v1", StringComparison.OrdinalIgnoreCase)
            && string.Equals(segments[1], "artifacts", StringComparison.OrdinalIgnoreCase))
        {
            await HandleArtifactAsync(request, segments[2], response, cancellationToken).ConfigureAwait(false);
            return;
        }

        await WriteErrorAsync(response, HttpStatusCode.NotFound, "route_not_found", cancellationToken).ConfigureAwait(false);
    }

    private async Task HandleStoreAsync(HttpListenerRequest request, HttpListenerResponse response, CancellationToken cancellationToken)
    {
        StoreRequests++;
        var mediaType = request.ContentType;
        if (string.IsNullOrWhiteSpace(mediaType))
        {
            await WriteErrorAsync(response, HttpStatusCode.BadRequest, "content_type_required", cancellationToken).ConfigureAwait(false);
            return;
        }

        mediaType = mediaType.Split(';', 2)[0].Trim();
        var options = TryParseWriteOptions(request.Headers[HttpArtifactStoreHeaderNames.RegionIndex]);
        var manifest = await _store.StoreAsync(request.InputStream, mediaType, options, cancellationToken).ConfigureAwait(false);
        response.StatusCode = (int)HttpStatusCode.Created;
        response.ContentType = "application/json";
        await WriteJsonAsync(response, ManifestDto.FromManifest(manifest), cancellationToken).ConfigureAwait(false);
    }

    private async Task HandleManifestAsync(string artifactIdHex, HttpListenerResponse response, CancellationToken cancellationToken)
    {
        ManifestRequests++;
        if (!Sha256Hash.TryParseHex(artifactIdHex, out var artifactId))
        {
            await WriteErrorAsync(response, HttpStatusCode.BadRequest, "invalid_sha256", cancellationToken).ConfigureAwait(false);
            return;
        }

        var manifest = await _store.TryGetManifestAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (manifest is null)
        {
            await WriteErrorAsync(response, HttpStatusCode.NotFound, "artifact_not_found", cancellationToken).ConfigureAwait(false);
            return;
        }

        response.StatusCode = (int)HttpStatusCode.OK;
        response.ContentType = "application/json";
        await WriteJsonAsync(response, ManifestDto.FromManifest(manifest), cancellationToken).ConfigureAwait(false);
    }

    private async Task HandleArtifactAsync(
        HttpListenerRequest request,
        string artifactIdHex,
        HttpListenerResponse response,
        CancellationToken cancellationToken)
    {
        ArtifactRequests++;
        if (!Sha256Hash.TryParseHex(artifactIdHex, out var artifactId))
        {
            await WriteErrorAsync(response, HttpStatusCode.BadRequest, "invalid_sha256", cancellationToken).ConfigureAwait(false);
            return;
        }

        var manifest = await _store.TryGetManifestAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (manifest is null)
        {
            await WriteErrorAsync(response, HttpStatusCode.NotFound, "artifact_not_found", cancellationToken).ConfigureAwait(false);
            return;
        }

        var rangeHeader = request.Headers["Range"];
        if (string.IsNullOrWhiteSpace(rangeHeader))
        {
            await using var stream = await _store.TryOpenArtifactAsync(artifactId, cancellationToken).ConfigureAwait(false);
            if (stream is null)
            {
                await WriteErrorAsync(response, HttpStatusCode.NotFound, "artifact_not_found", cancellationToken).ConfigureAwait(false);
                return;
            }

            response.StatusCode = (int)HttpStatusCode.OK;
            response.ContentType = manifest.MediaType;
            response.ContentLength64 = manifest.ByteLength;
            response.Headers[HttpResponseHeader.AcceptRanges] = "bytes";
            await stream.CopyToAsync(response.OutputStream, cancellationToken).ConfigureAwait(false);
            return;
        }

        RangeRequests++;
        if (!SupportsRangeRequests)
        {
            await WriteErrorAsync(response, HttpStatusCode.NotImplemented, "range_reads_not_supported", cancellationToken).ConfigureAwait(false);
            return;
        }

        if (!RangeHeaderValue.TryParse(rangeHeader, out var parsed)
            || !string.Equals(parsed.Unit, "bytes", StringComparison.OrdinalIgnoreCase)
            || parsed.Ranges.Count != 1)
        {
            await WriteErrorAsync(response, HttpStatusCode.RequestedRangeNotSatisfiable, "invalid_range_header", cancellationToken).ConfigureAwait(false);
            return;
        }

        var range = parsed.Ranges.Single();
        if (!range.From.HasValue || !range.To.HasValue)
        {
            await WriteErrorAsync(response, HttpStatusCode.RequestedRangeNotSatisfiable, "range_bounds_required", cancellationToken).ConfigureAwait(false);
            return;
        }

        var offset = range.From.Value;
        var length = checked(range.To.Value - offset + 1);
        if (offset < 0 || length < 0 || offset + length > manifest.ByteLength)
        {
            await WriteErrorAsync(response, HttpStatusCode.RequestedRangeNotSatisfiable, "range_out_of_bounds", cancellationToken).ConfigureAwait(false);
            return;
        }

        await using var rangeStream = await _store.TryOpenArtifactRangeAsync(artifactId, offset, length, cancellationToken).ConfigureAwait(false);
        if (rangeStream is null)
        {
            await WriteErrorAsync(response, HttpStatusCode.NotFound, "artifact_not_found", cancellationToken).ConfigureAwait(false);
            return;
        }

        response.StatusCode = (int)HttpStatusCode.PartialContent;
        response.ContentType = manifest.MediaType;
        response.ContentLength64 = length;
        response.Headers[HttpResponseHeader.AcceptRanges] = "bytes";
        response.Headers[HttpResponseHeader.ContentRange] = $"bytes {offset}-{offset + length - 1}/{manifest.ByteLength}";
        await rangeStream.CopyToAsync(response.OutputStream, cancellationToken).ConfigureAwait(false);
    }

    private static ArtifactStoreWriteOptions? TryParseWriteOptions(string? encodedRegionIndex)
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

    private static async Task WriteJsonAsync(HttpListenerResponse response, object value, CancellationToken cancellationToken)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(value, JsonOptions);
        response.ContentLength64 = bytes.Length;
        await response.OutputStream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
    }

    private static async Task WriteErrorAsync(
        HttpListenerResponse response,
        HttpStatusCode statusCode,
        string message,
        CancellationToken cancellationToken)
    {
        response.StatusCode = (int)statusCode;
        response.ContentType = "text/plain";
        var bytes = Encoding.UTF8.GetBytes(message);
        response.ContentLength64 = bytes.Length;
        await response.OutputStream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
    }

    private static int ReservePort()
    {
        var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    private sealed class ManifestDto
    {
        public string ArtifactId { get; set; } = string.Empty;
        public string MediaType { get; set; } = string.Empty;
        public long ByteLength { get; set; }
        public List<ChunkDto> Chunks { get; set; } = [];
        public List<RegionIndexDto> RegionIndex { get; set; } = [];

        public static ManifestDto FromManifest(ArtifactManifest manifest)
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
                        Compression = ToCompressionLabel(chunk.Compression)
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

        private static string ToCompressionLabel(ChunkCompressionKind compression)
            => compression switch
            {
                ChunkCompressionKind.Zstd => "zstd",
                _ => "none"
            };
    }

    private sealed class ChunkDto
    {
        public string Hash { get; set; } = string.Empty;
        public int UncompressedLength { get; set; }
        public int StoredLength { get; set; }
        public string Compression { get; set; } = "none";
    }

    private sealed class RegionIndexDto
    {
        public int RegionId { get; set; }
        public long Offset { get; set; }
        public long Length { get; set; }
    }
}
