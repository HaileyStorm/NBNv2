using Proto;
using System.Net.Sockets;

namespace Nbn.Shared;

public static class RoutablePidReference
{
    public const string Prefix = "nbn+pidset:";

    public static string Encode(ServiceEndpointSet endpointSet)
        => Prefix + ServiceEndpointSettings.EncodeSetValue(endpointSet.ActorName, endpointSet.Candidates);

    public static bool TryDecode(string? value, out ServiceEndpointSet endpointSet)
    {
        endpointSet = default;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        if (!trimmed.StartsWith(Prefix, StringComparison.Ordinal))
        {
            return false;
        }

        return ServiceEndpointSettings.TryParseSetValue(trimmed.Substring(Prefix.Length), out endpointSet);
    }

    public static bool TryParsePlainPid(string? value, out PID pid)
    {
        pid = new PID();
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        if (trimmed.StartsWith(Prefix, StringComparison.Ordinal))
        {
            return false;
        }

        var slashIndex = trimmed.IndexOf('/');
        if (slashIndex <= 0)
        {
            pid.Id = trimmed;
            return true;
        }

        var address = trimmed[..slashIndex];
        var id = trimmed[(slashIndex + 1)..];
        if (string.IsNullOrWhiteSpace(id))
        {
            return false;
        }

        pid.Address = address;
        pid.Id = id;
        return true;
    }

    public static async Task<PID?> ResolveAsync(
        string? value,
        Func<ServiceEndpointCandidate, CancellationToken, Task<bool>>? probeAsync = null,
        CancellationToken cancellationToken = default)
    {
        if (TryDecode(value, out var endpointSet))
        {
            var endpoint = await ServiceEndpointResolver.ResolveAsync(
                endpointSet,
                probeAsync ?? ProbeCandidateAsync,
                cancellationToken).ConfigureAwait(false);
            return endpoint.ToPid();
        }

        return TryParsePlainPid(value, out var pid) ? pid : null;
    }

    private static async Task<bool> ProbeCandidateAsync(ServiceEndpointCandidate candidate, CancellationToken cancellationToken)
    {
        var hostPort = candidate.HostPort;
        var colonIndex = hostPort.LastIndexOf(':');
        if (colonIndex <= 0 || colonIndex >= hostPort.Length - 1)
        {
            return false;
        }

        var host = hostPort[..colonIndex];
        var portToken = hostPort[(colonIndex + 1)..];
        if (!int.TryParse(portToken, out var port) || port <= 0 || port >= 65536)
        {
            return false;
        }

        if (host.StartsWith("[", StringComparison.Ordinal) && host.EndsWith("]", StringComparison.Ordinal))
        {
            host = host[1..^1];
        }

        try
        {
            using var client = new TcpClient();
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(2));
            await client.ConnectAsync(host.Trim(), port, timeoutCts.Token).ConfigureAwait(false);
            return client.Connected;
        }
        catch
        {
            return false;
        }
    }
}
