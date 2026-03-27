using System.Net;
using System.Net.Http.Headers;

namespace Nbn.Runtime.Artifacts;

/// <summary>
/// Talks to the built-in HTTP artifact-service contract for manifest, full-read, and range-read operations.
/// </summary>
public sealed class HttpArtifactStore : IArtifactStore
{
    private static readonly HttpClient SharedHttpClient = CreateSharedHttpClient();
    private readonly HttpClient _httpClient;

    /// <summary>
    /// Initializes an HTTP artifact store from an absolute base URI string.
    /// </summary>
    public HttpArtifactStore(string baseUri, HttpClient? httpClient = null)
        : this(CreateBaseUri(baseUri), httpClient)
    {
    }

    /// <summary>
    /// Initializes an HTTP artifact store from an absolute HTTP(S) base URI.
    /// </summary>
    public HttpArtifactStore(Uri baseUri, HttpClient? httpClient = null)
    {
        if (baseUri is null)
        {
            throw new ArgumentNullException(nameof(baseUri));
        }

        if (!baseUri.IsAbsoluteUri || baseUri.IsFile)
        {
            throw new ArgumentException("HTTP artifact store base URI must be a non-file absolute URI.", nameof(baseUri));
        }

        if (!string.Equals(baseUri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(baseUri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException("HTTP artifact store base URI must use http or https.", nameof(baseUri));
        }

        BaseUri = EnsureTrailingSlash(baseUri);
        _httpClient = httpClient ?? SharedHttpClient;
    }

    /// <summary>
    /// Gets the normalized artifact-service base URI.
    /// </summary>
    public Uri BaseUri { get; }

    /// <inheritdoc />
    public async Task<ArtifactManifest> StoreAsync(
        Stream content,
        string mediaType,
        ArtifactStoreWriteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (content is null)
        {
            throw new ArgumentNullException(nameof(content));
        }

        if (!content.CanRead)
        {
            throw new ArgumentException("Content stream must be readable.", nameof(content));
        }

        if (string.IsNullOrWhiteSpace(mediaType))
        {
            throw new ArgumentException("Media type is required.", nameof(mediaType));
        }

        options = ArtifactRegionIndexBuilder.PopulateIfMissing(content, mediaType, options);

        using var request = new HttpRequestMessage(HttpMethod.Post, BuildRelativeUri("v1/artifacts"))
        {
            Content = new StreamContent(content)
        };
        request.Content.Headers.ContentType = MediaTypeHeaderValue.Parse(mediaType);
        ArtifactStoreHttpPayloads.AddRegionIndexHeader(request, options);

        using var response = await _httpClient.SendAsync(
                request,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken)
            .ConfigureAwait(false);

        await EnsureSuccessStatusCodeAsync(response, cancellationToken).ConfigureAwait(false);
        return await ArtifactStoreHttpPayloads.DeserializeManifestAsync(response.Content, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<ArtifactManifest?> TryGetManifestAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, BuildRelativeUri($"v1/manifests/{artifactId.ToHex()}"));
        using var response = await _httpClient.SendAsync(
                request,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken)
            .ConfigureAwait(false);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }

        await EnsureSuccessStatusCodeAsync(response, cancellationToken).ConfigureAwait(false);
        return await ArtifactStoreHttpPayloads.DeserializeManifestAsync(response.Content, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> ContainsAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
        => await TryGetManifestAsync(artifactId, cancellationToken).ConfigureAwait(false) is not null;

    /// <inheritdoc />
    public async Task<Stream?> TryOpenArtifactAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, BuildRelativeUri($"v1/artifacts/{artifactId.ToHex()}"));
        var response = await _httpClient.SendAsync(
                request,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken)
            .ConfigureAwait(false);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            response.Dispose();
            return null;
        }

        try
        {
            await EnsureSuccessStatusCodeAsync(response, cancellationToken).ConfigureAwait(false);
            var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            return new ResponseStream(stream, response);
        }
        catch
        {
            response.Dispose();
            throw;
        }
    }

    /// <inheritdoc />
    public async Task<Stream?> TryOpenArtifactRangeAsync(
        Sha256Hash artifactId,
        long offset,
        long length,
        CancellationToken cancellationToken = default)
    {
        ArtifactRangeSupport.ValidateRange(offset, length);
        if (length == 0)
        {
            return new MemoryStream(Array.Empty<byte>(), writable: false);
        }

        using var request = new HttpRequestMessage(HttpMethod.Get, BuildRelativeUri($"v1/artifacts/{artifactId.ToHex()}"));
        request.Headers.Range = new RangeHeaderValue(offset, offset + length - 1);

        var response = await _httpClient.SendAsync(
                request,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken)
            .ConfigureAwait(false);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            response.Dispose();
            return null;
        }

        if (response.StatusCode == HttpStatusCode.MethodNotAllowed
            || response.StatusCode == HttpStatusCode.NotImplemented)
        {
            response.Dispose();
            return await ArtifactRangeSupport.TryOpenFallbackAsync(this, artifactId, offset, length, cancellationToken).ConfigureAwait(false);
        }

        if (response.StatusCode == HttpStatusCode.OK)
        {
            response.Dispose();
            return await ArtifactRangeSupport.TryOpenFallbackAsync(this, artifactId, offset, length, cancellationToken).ConfigureAwait(false);
        }

        try
        {
            await EnsureSuccessStatusCodeAsync(response, cancellationToken).ConfigureAwait(false);
            var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            return new ResponseStream(stream, response);
        }
        catch
        {
            response.Dispose();
            throw;
        }
    }

    private Uri BuildRelativeUri(string relative)
        => new(BaseUri, relative);

    private static async Task EnsureSuccessStatusCodeAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }

        var body = response.Content is null
            ? string.Empty
            : await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        var detail = string.IsNullOrWhiteSpace(body)
            ? string.Empty
            : $" Detail: {body.Trim()}";
        throw new InvalidOperationException(
            $"Artifact store request to '{response.RequestMessage?.RequestUri}' failed with {(int)response.StatusCode} {response.ReasonPhrase}.{detail}");
    }

    private static Uri CreateBaseUri(string baseUri)
    {
        if (string.IsNullOrWhiteSpace(baseUri))
        {
            throw new ArgumentException("HTTP artifact store base URI is required.", nameof(baseUri));
        }

        if (!Uri.TryCreate(baseUri.Trim(), UriKind.Absolute, out var parsed))
        {
            throw new ArgumentException("HTTP artifact store base URI must be an absolute URI.", nameof(baseUri));
        }

        return parsed;
    }

    private static Uri EnsureTrailingSlash(Uri baseUri)
    {
        if (baseUri.AbsolutePath.EndsWith("/", StringComparison.Ordinal))
        {
            return baseUri;
        }

        var builder = new UriBuilder(baseUri)
        {
            Path = baseUri.AbsolutePath + "/"
        };
        return builder.Uri;
    }

    private static HttpClient CreateSharedHttpClient()
    {
        var client = new HttpClient();
        client.DefaultRequestVersion = HttpVersion.Version11;
        client.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrLower;
        return client;
    }

    private sealed class ResponseStream : Stream
    {
        private readonly Stream _inner;
        private readonly HttpResponseMessage _response;
        private bool _disposed;

        public ResponseStream(Stream inner, HttpResponseMessage response)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _response = response ?? throw new ArgumentNullException(nameof(response));
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => _inner.Length;
        public override long Position
        {
            get => _inner.Position;
            set => _inner.Position = value;
        }

        public override void Flush() => _inner.Flush();

        public override Task FlushAsync(CancellationToken cancellationToken)
            => _inner.FlushAsync(cancellationToken);

        public override int Read(byte[] buffer, int offset, int count)
            => _inner.Read(buffer, offset, count);

        public override int Read(Span<byte> buffer)
            => _inner.Read(buffer);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => _inner.ReadAsync(buffer, cancellationToken);

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => _inner.ReadAsync(buffer, offset, count, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin)
            => _inner.Seek(offset, origin);

        public override void SetLength(long value)
            => _inner.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count)
            => _inner.Write(buffer, offset, count);

        public override void Write(ReadOnlySpan<byte> buffer)
            => _inner.Write(buffer);

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => _inner.WriteAsync(buffer, cancellationToken);

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => _inner.WriteAsync(buffer, offset, count, cancellationToken);

        protected override void Dispose(bool disposing)
        {
            if (!disposing || _disposed)
            {
                return;
            }

            _disposed = true;
            _inner.Dispose();
            _response.Dispose();
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            await _inner.DisposeAsync().ConfigureAwait(false);
            _response.Dispose();
            await base.DisposeAsync().ConfigureAwait(false);
        }
    }
}
