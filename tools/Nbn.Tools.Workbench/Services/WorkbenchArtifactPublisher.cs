using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Nbn.Proto;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;

namespace Nbn.Tools.Workbench.Services;

public interface IWorkbenchArtifactPublisher : IAsyncDisposable
{
    Task<PublishedArtifact> PublishAsync(
        byte[] bytes,
        string mediaType,
        string backingStoreRoot,
        string bindHost,
        string? advertisedHost = null,
        string? label = null,
        CancellationToken cancellationToken = default);
}

public sealed record PublishedArtifact(ArtifactRef ArtifactRef, string? AttentionMessage);

public sealed class WorkbenchArtifactPublisher : IWorkbenchArtifactPublisher
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly Dictionary<string, HostedStore> _stores = new(StringComparer.OrdinalIgnoreCase);
    private readonly ILocalFirewallManager _firewallManager;
    private readonly Action<string>? _logInfo;
    private readonly Action<string>? _logWarn;

    public WorkbenchArtifactPublisher(
        ILocalFirewallManager? firewallManager = null,
        Action<string>? logInfo = null,
        Action<string>? logWarn = null)
    {
        _firewallManager = firewallManager ?? new LocalFirewallManager();
        _logInfo = logInfo;
        _logWarn = logWarn;
    }

    public async Task<PublishedArtifact> PublishAsync(
        byte[] bytes,
        string mediaType,
        string backingStoreRoot,
        string bindHost,
        string? advertisedHost = null,
        string? label = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(bytes);
        if (string.IsNullOrWhiteSpace(mediaType))
        {
            throw new ArgumentException("Media type is required.", nameof(mediaType));
        }

        var normalizedRoot = NormalizeRoot(backingStoreRoot);
        var normalizedBindHost = NormalizeBindHost(bindHost);
        var normalizedAdvertisedHost = NetworkAddressDefaults.ResolveAdvertisedHost(normalizedBindHost, advertisedHost);
        var key = BuildStoreKey(normalizedRoot, normalizedBindHost, normalizedAdvertisedHost);

        HostedStore hosted;
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!_stores.TryGetValue(key, out hosted!))
            {
                hosted = await StartHostedStoreAsync(
                        normalizedRoot,
                        normalizedBindHost,
                        normalizedAdvertisedHost,
                        label,
                        cancellationToken)
                    .ConfigureAwait(false);
                _stores[key] = hosted;
            }
        }
        finally
        {
            _gate.Release();
        }

        var manifest = await hosted.Store
            .StoreAsync(new MemoryStream(bytes, writable: false), mediaType, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        var artifactRef = manifest.ArtifactId.ToHex()
            .ToArtifactRef((ulong)Math.Max(0L, manifest.ByteLength), mediaType, hosted.BaseUri.AbsoluteUri);
        return new PublishedArtifact(artifactRef, hosted.AttentionMessage);
    }

    public async ValueTask DisposeAsync()
    {
        HostedStore[] hostedStores;
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            hostedStores = _stores.Values.ToArray();
            _stores.Clear();
        }
        finally
        {
            _gate.Release();
        }

        foreach (var hosted in hostedStores)
        {
            await hosted.App.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task<HostedStore> StartHostedStoreAsync(
        string rootPath,
        string bindHost,
        string advertisedHost,
        string? label,
        CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(rootPath);
        var store = new LocalArtifactStore(new ArtifactStoreOptions(rootPath));
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
        {
            Args = Array.Empty<string>(),
            ApplicationName = typeof(WorkbenchArtifactPublisher).Assembly.GetName().Name,
            EnvironmentName = Environments.Production
        });
        builder.WebHost.UseKestrelCore();
        builder.WebHost.ConfigureKestrel(options => ConfigureListen(options, bindHost));

        var app = builder.Build();
        MapRoutes(app, store);
        await app.StartAsync(cancellationToken).ConfigureAwait(false);

        var address = ResolveListeningAddress(app);
        var baseUri = new UriBuilder(Uri.UriSchemeHttp, advertisedHost, address.Port).Uri;
        var firewall = await _firewallManager
            .EnsureInboundTcpAccessAsync(string.IsNullOrWhiteSpace(label) ? "WorkbenchArtifactStore" : label!, bindHost, address.Port)
            .ConfigureAwait(false);
        if (!string.IsNullOrWhiteSpace(firewall.Message))
        {
            var logMessage = $"Workbench artifact store firewall: {firewall.Message}";
            if (firewall.RequiresAttention)
            {
                _logWarn?.Invoke(logMessage);
            }
            else
            {
                _logInfo?.Invoke(logMessage);
            }
        }

        return new HostedStore(
            app,
            store,
            baseUri,
            firewall.RequiresAttention ? firewall.Message : null);
    }

    private static void ConfigureListen(KestrelServerOptions options, string bindHost)
    {
        if (NetworkAddressDefaults.IsAllInterfaces(bindHost))
        {
            options.ListenAnyIP(0);
            return;
        }

        if (NetworkAddressDefaults.IsLoopbackHost(bindHost))
        {
            options.Listen(IPAddress.Loopback, 0);
            return;
        }

        if (IPAddress.TryParse(bindHost, out var ip))
        {
            options.Listen(ip, 0);
            return;
        }

        options.ListenAnyIP(0);
    }

    private static void MapRoutes(WebApplication app, LocalArtifactStore store)
    {
        app.MapPost(
            "/v1/artifacts",
            async context =>
            {
                var mediaType = context.Request.ContentType;
                if (string.IsNullOrWhiteSpace(mediaType))
                {
                    await WriteErrorAsync(
                        context.Response,
                        StatusCodes.Status400BadRequest,
                        "content_type_required",
                        context.RequestAborted).ConfigureAwait(false);
                    return;
                }

                mediaType = mediaType.Split(';', 2)[0].Trim();
                var options = TryParseWriteOptions(context.Request.Headers[HttpArtifactStoreHeaderNames.RegionIndex]);
                var manifest = await store.StoreAsync(
                        context.Request.Body,
                        mediaType,
                        options,
                        context.RequestAborted)
                    .ConfigureAwait(false);
                context.Response.StatusCode = StatusCodes.Status201Created;
                context.Response.ContentType = "application/json";
                await WriteJsonAsync(context.Response, ManifestDto.FromManifest(manifest), context.RequestAborted).ConfigureAwait(false);
            });

        app.MapGet(
            "/v1/manifests/{artifactIdHex}",
            async (HttpContext context, string artifactIdHex) =>
            {
                if (!Sha256Hash.TryParseHex(artifactIdHex, out var artifactId))
                {
                    await WriteErrorAsync(
                        context.Response,
                        StatusCodes.Status400BadRequest,
                        "invalid_sha256",
                        context.RequestAborted).ConfigureAwait(false);
                    return;
                }

                var manifest = await store.TryGetManifestAsync(artifactId, context.RequestAborted).ConfigureAwait(false);
                if (manifest is null)
                {
                    await WriteErrorAsync(
                        context.Response,
                        StatusCodes.Status404NotFound,
                        "artifact_not_found",
                        context.RequestAborted).ConfigureAwait(false);
                    return;
                }

                context.Response.StatusCode = StatusCodes.Status200OK;
                context.Response.ContentType = "application/json";
                await WriteJsonAsync(context.Response, ManifestDto.FromManifest(manifest), context.RequestAborted).ConfigureAwait(false);
            });

        app.MapGet(
            "/v1/artifacts/{artifactIdHex}",
            async (HttpContext context, string artifactIdHex) =>
            {
                if (!Sha256Hash.TryParseHex(artifactIdHex, out var artifactId))
                {
                    await WriteErrorAsync(
                        context.Response,
                        StatusCodes.Status400BadRequest,
                        "invalid_sha256",
                        context.RequestAborted).ConfigureAwait(false);
                    return;
                }

                var manifest = await store.TryGetManifestAsync(artifactId, context.RequestAborted).ConfigureAwait(false);
                if (manifest is null)
                {
                    await WriteErrorAsync(
                        context.Response,
                        StatusCodes.Status404NotFound,
                        "artifact_not_found",
                        context.RequestAborted).ConfigureAwait(false);
                    return;
                }

                var rangeHeader = context.Request.Headers.Range.ToString();
                if (string.IsNullOrWhiteSpace(rangeHeader))
                {
                    await using var stream = await store.TryOpenArtifactAsync(artifactId, context.RequestAborted).ConfigureAwait(false);
                    if (stream is null)
                    {
                        await WriteErrorAsync(
                            context.Response,
                            StatusCodes.Status404NotFound,
                            "artifact_not_found",
                            context.RequestAborted).ConfigureAwait(false);
                        return;
                    }

                    context.Response.StatusCode = StatusCodes.Status200OK;
                    context.Response.ContentType = manifest.MediaType;
                    context.Response.ContentLength = manifest.ByteLength;
                    context.Response.Headers.AcceptRanges = "bytes";
                    await stream.CopyToAsync(context.Response.Body, context.RequestAborted).ConfigureAwait(false);
                    return;
                }

                if (!RangeHeaderValue.TryParse(rangeHeader, out var parsed)
                    || !string.Equals(parsed.Unit, "bytes", StringComparison.OrdinalIgnoreCase)
                    || parsed.Ranges.Count != 1)
                {
                    await WriteErrorAsync(
                        context.Response,
                        StatusCodes.Status416RangeNotSatisfiable,
                        "invalid_range_header",
                        context.RequestAborted).ConfigureAwait(false);
                    return;
                }

                var range = parsed.Ranges.Single();
                if (!range.From.HasValue || !range.To.HasValue)
                {
                    await WriteErrorAsync(
                        context.Response,
                        StatusCodes.Status416RangeNotSatisfiable,
                        "range_bounds_required",
                        context.RequestAborted).ConfigureAwait(false);
                    return;
                }

                var offset = range.From.Value;
                var length = checked(range.To.Value - offset + 1);
                if (offset < 0 || length < 0 || offset + length > manifest.ByteLength)
                {
                    await WriteErrorAsync(
                        context.Response,
                        StatusCodes.Status416RangeNotSatisfiable,
                        "range_out_of_bounds",
                        context.RequestAborted).ConfigureAwait(false);
                    return;
                }

                await using var rangeStream = await store
                    .TryOpenArtifactRangeAsync(artifactId, offset, length, context.RequestAborted)
                    .ConfigureAwait(false);
                if (rangeStream is null)
                {
                    await WriteErrorAsync(
                        context.Response,
                        StatusCodes.Status404NotFound,
                        "artifact_not_found",
                        context.RequestAborted).ConfigureAwait(false);
                    return;
                }

                context.Response.StatusCode = StatusCodes.Status206PartialContent;
                context.Response.ContentType = manifest.MediaType;
                context.Response.ContentLength = length;
                context.Response.Headers.AcceptRanges = "bytes";
                context.Response.Headers.ContentRange = $"bytes {offset}-{offset + length - 1}/{manifest.ByteLength}";
                await rangeStream.CopyToAsync(context.Response.Body, context.RequestAborted).ConfigureAwait(false);
            });
    }

    private static Uri ResolveListeningAddress(WebApplication app)
    {
        var addresses = app.Services
            .GetRequiredService<IServer>()
            .Features
            .Get<IServerAddressesFeature>()?
            .Addresses;
        var address = addresses?.FirstOrDefault();
        if (string.IsNullOrWhiteSpace(address))
        {
            throw new InvalidOperationException("Workbench artifact store did not publish a listening address.");
        }

        return new Uri(address, UriKind.Absolute);
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

    private static async Task WriteJsonAsync(HttpResponse response, object value, CancellationToken cancellationToken)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(value, JsonOptions);
        response.ContentLength = bytes.Length;
        await response.Body.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
    }

    private static async Task WriteErrorAsync(
        HttpResponse response,
        int statusCode,
        string message,
        CancellationToken cancellationToken)
    {
        response.StatusCode = statusCode;
        response.ContentType = "text/plain";
        var bytes = Encoding.UTF8.GetBytes(message);
        response.ContentLength = bytes.Length;
        await response.Body.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
    }

    private static string BuildStoreKey(string rootPath, string bindHost, string advertisedHost)
        => $"{rootPath}|{bindHost}|{advertisedHost}";

    private static string NormalizeRoot(string rootPath)
    {
        if (string.IsNullOrWhiteSpace(rootPath))
        {
            rootPath = Path.Combine(Environment.CurrentDirectory, "artifacts");
        }

        return Path.GetFullPath(rootPath.Trim());
    }

    private static string NormalizeBindHost(string bindHost)
        => string.IsNullOrWhiteSpace(bindHost) ? NetworkAddressDefaults.LoopbackHost : bindHost.Trim();

    private sealed record HostedStore(
        WebApplication App,
        LocalArtifactStore Store,
        Uri BaseUri,
        string? AttentionMessage);

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
                        Compression = chunk.Compression switch
                        {
                            ChunkCompressionKind.Zstd => "zstd",
                            _ => "none"
                        }
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
    }

    private sealed class ChunkDto
    {
        public string Hash { get; set; } = string.Empty;
        public int UncompressedLength { get; set; }
        public int StoredLength { get; set; }
        public string Compression { get; set; } = string.Empty;
    }

    private sealed class RegionIndexDto
    {
        public int RegionId { get; set; }
        public long Offset { get; set; }
        public long Length { get; set; }
    }
}
