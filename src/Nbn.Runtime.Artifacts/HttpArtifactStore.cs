using System.Buffers;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;

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
        var manifest = await ArtifactStoreHttpPayloads.DeserializeManifestAsync(response.Content, cancellationToken).ConfigureAwait(false);
        if (manifest.ArtifactId != artifactId)
        {
            throw new InvalidDataException(
                $"Artifact store returned manifest {manifest.ArtifactId} for requested artifact {artifactId}.");
        }

        return manifest;
    }

    /// <inheritdoc />
    public async Task<bool> ContainsAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
        => await TryGetManifestAsync(artifactId, cancellationToken).ConfigureAwait(false) is not null;

    /// <inheritdoc />
    public async Task<Stream?> TryOpenArtifactAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        var manifest = await TryGetManifestAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (manifest is null)
        {
            return null;
        }

        return await OpenVerifiedArtifactAsync(artifactId, manifest.ByteLength, cancellationToken).ConfigureAwait(false);
    }

    private async Task<Stream?> OpenVerifiedArtifactAsync(
        Sha256Hash artifactId,
        long expectedLength,
        CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, BuildRelativeUri($"v1/artifacts/{artifactId.ToHex()}"));
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
        return await StageVerifiedPayloadAsync(response.Content, artifactId, expectedLength, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<Stream?> TryOpenArtifactRangeAsync(
        Sha256Hash artifactId,
        long offset,
        long length,
        CancellationToken cancellationToken = default)
    {
        ArtifactRangeSupport.ValidateRange(offset, length);
        var manifest = await TryGetManifestAsync(artifactId, cancellationToken).ConfigureAwait(false);
        if (manifest is null)
        {
            return null;
        }

        ArtifactRangeSupport.ValidateRangeWithin(manifest.ByteLength, offset, length);
        if (length == 0)
        {
            return new MemoryStream(Array.Empty<byte>(), writable: false);
        }

        var verified = await OpenVerifiedArtifactAsync(artifactId, manifest.ByteLength, cancellationToken).ConfigureAwait(false);
        if (verified is null)
        {
            return null;
        }

        return new ArtifactRangeStream(verified, offset, length);
    }

    private static async Task<Stream> StageVerifiedPayloadAsync(
        HttpContent content,
        Sha256Hash artifactId,
        long expectedLength,
        CancellationToken cancellationToken)
    {
        if (content.Headers.ContentLength is { } contentLength && contentLength != expectedLength)
        {
            throw new InvalidDataException(
                $"Artifact {artifactId} declared {contentLength} response bytes; expected {expectedLength}.");
        }

        var tempPath = Path.Combine(Path.GetTempPath(), $"nbn-http-artifact-{Guid.NewGuid():N}.tmp");
        FileStream? staged = null;
        var completed = false;
        try
        {
            var fileOptions = new FileStreamOptions
            {
                Mode = FileMode.CreateNew,
                Access = FileAccess.ReadWrite,
                Share = FileShare.None,
                BufferSize = 64 * 1024,
                Options = FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.DeleteOnClose
            };
            if (!OperatingSystem.IsWindows())
            {
                fileOptions.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
            }

            staged = new FileStream(tempPath, fileOptions);

            await using var source = await content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            var buffer = ArrayPool<byte>.Shared.Rent(81920);
            var actualLength = 0L;
            try
            {
                while (true)
                {
                    var read = await source.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken).ConfigureAwait(false);
                    if (read == 0)
                    {
                        break;
                    }

                    actualLength = checked(actualLength + read);
                    if (actualLength > expectedLength)
                    {
                        throw new InvalidDataException(
                            $"Artifact {artifactId} exceeded its expected length of {expectedLength} bytes.");
                    }

                    hasher.AppendData(buffer, 0, read);
                    await staged.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }

            if (actualLength != expectedLength)
            {
                throw new InvalidDataException(
                    $"Artifact {artifactId} contained {actualLength} bytes; expected {expectedLength}.");
            }

            var actualId = new Sha256Hash(hasher.GetHashAndReset());
            if (actualId != artifactId)
            {
                throw new InvalidDataException(
                    $"Artifact payload SHA-256 {actualId} does not match requested identity {artifactId}.");
            }

            await staged.FlushAsync(cancellationToken).ConfigureAwait(false);
            staged.Position = 0;
            completed = true;
            return staged;
        }
        finally
        {
            if (!completed)
            {
                staged?.Dispose();
                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }
            }
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

}
