using System.Net;
using System.Net.Sockets;

namespace Nbn.Tools.Workbench.Services;

public static class LocalPortAllocator
{
    private const int MinPort = 1;
    private const int MaxPort = 65535;

    public static bool TryFindAvailablePort(
        string bindHost,
        int preferredStartPort,
        ISet<int>? reservedPorts,
        out int port,
        out string? error)
    {
        port = 0;
        error = null;

        var start = Math.Clamp(preferredStartPort, MinPort, MaxPort);
        for (var candidate = start; candidate <= MaxPort; candidate++)
        {
            if (reservedPorts is not null && reservedPorts.Contains(candidate))
            {
                continue;
            }

            if (IsPortAvailable(bindHost, candidate))
            {
                port = candidate;
                return true;
            }
        }

        error = $"No available port found for host '{bindHost}' starting at {start}.";
        return false;
    }

    private static bool IsPortAvailable(string bindHost, int port)
    {
        foreach (var address in ResolveAddresses(bindHost))
        {
            if (!CanBind(address, port))
            {
                return false;
            }
        }

        return true;
    }

    private static IReadOnlyList<IPAddress> ResolveAddresses(string bindHost)
    {
        if (string.IsNullOrWhiteSpace(bindHost))
        {
            return new[] { IPAddress.Loopback };
        }

        var host = bindHost.Trim();
        if (host.Equals("localhost", StringComparison.OrdinalIgnoreCase))
        {
            return new[] { IPAddress.Loopback, IPAddress.IPv6Loopback };
        }

        if (host.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase) || host.Equals("*", StringComparison.OrdinalIgnoreCase))
        {
            return new[] { IPAddress.Any };
        }

        if (host.Equals("::", StringComparison.OrdinalIgnoreCase))
        {
            return new[] { IPAddress.IPv6Any };
        }

        if (IPAddress.TryParse(host, out var parsed))
        {
            return new[] { parsed };
        }

        try
        {
            var resolved = Dns.GetHostAddresses(host);
            return resolved.Length == 0 ? new[] { IPAddress.Loopback } : resolved;
        }
        catch
        {
            return new[] { IPAddress.Loopback };
        }
    }

    private static bool CanBind(IPAddress address, int port)
    {
        TcpListener? listener = null;
        try
        {
            listener = new TcpListener(address, port);
            listener.Server.ExclusiveAddressUse = true;
            listener.Start();
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
        finally
        {
            try
            {
                listener?.Stop();
            }
            catch
            {
            }
        }
    }
}
