using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace Nbn.Shared;

public static class NetworkAddressDefaults
{
    public const string LoopbackHost = "127.0.0.1";
    public const string DefaultBindHost = "0.0.0.0";

    public static string ResolveDefaultAdvertisedHost()
    {
        if (TryResolveDefaultAdvertisedHost(out var host))
        {
            return host;
        }

        return LoopbackHost;
    }

    public static string ResolveAdvertisedHost(string bindHost, string? advertisedHost)
    {
        if (!string.IsNullOrWhiteSpace(advertisedHost))
        {
            return advertisedHost.Trim();
        }

        if (!IsAllInterfaces(bindHost))
        {
            return string.IsNullOrWhiteSpace(bindHost) ? LoopbackHost : bindHost.Trim();
        }

        return ResolveDefaultAdvertisedHost();
    }

    public static bool IsLoopbackHost(string? host)
    {
        if (string.IsNullOrWhiteSpace(host))
        {
            return false;
        }

        var trimmed = host.Trim();
        if (string.Equals(trimmed, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return IPAddress.TryParse(trimmed, out var address) && IPAddress.IsLoopback(address);
    }

    public static bool IsAllInterfaces(string? host)
    {
        if (string.IsNullOrWhiteSpace(host))
        {
            return false;
        }

        var trimmed = host.Trim();
        return trimmed.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase)
               || trimmed.Equals("::", StringComparison.OrdinalIgnoreCase)
               || trimmed.Equals("*", StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryResolveDefaultAdvertisedHost(out string host)
    {
        host = string.Empty;

        var configured = Environment.GetEnvironmentVariable("NBN_DEFAULT_ADVERTISE_HOST");
        if (TryNormalizeConfiguredHost(configured, out host))
        {
            return true;
        }

        foreach (var address in EnumerateCandidateAddresses())
        {
            if (address.AddressFamily == AddressFamily.InterNetwork
                && !IPAddress.IsLoopback(address)
                && !IsAutomaticPrivateAddress(address))
            {
                host = address.ToString();
                return true;
            }
        }

        foreach (var address in EnumerateCandidateAddresses())
        {
            if (address.AddressFamily == AddressFamily.InterNetworkV6
                && !IPAddress.IsLoopback(address)
                && !address.IsIPv6LinkLocal)
            {
                host = address.ToString();
                return true;
            }
        }

        return false;
    }

    private static IEnumerable<IPAddress> EnumerateCandidateAddresses()
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        NetworkInterface[] interfaces;
        try
        {
            interfaces = NetworkInterface.GetAllNetworkInterfaces();
        }
        catch
        {
            yield break;
        }

        foreach (var nic in interfaces)
        {
            if (nic.OperationalStatus != OperationalStatus.Up
                || nic.NetworkInterfaceType == NetworkInterfaceType.Loopback
                || nic.NetworkInterfaceType == NetworkInterfaceType.Tunnel)
            {
                continue;
            }

            IPInterfaceProperties properties;
            try
            {
                properties = nic.GetIPProperties();
            }
            catch
            {
                continue;
            }

            foreach (var unicast in properties.UnicastAddresses)
            {
                var address = unicast.Address;
                if (IPAddress.IsLoopback(address))
                {
                    continue;
                }

                var key = address.ToString();
                if (seen.Add(key))
                {
                    yield return address;
                }
            }
        }
    }

    private static bool TryNormalizeConfiguredHost(string? host, out string normalizedHost)
    {
        normalizedHost = string.Empty;
        if (string.IsNullOrWhiteSpace(host))
        {
            return false;
        }

        var trimmed = host.Trim();
        if (IsAllInterfaces(trimmed) || IsLoopbackHost(trimmed))
        {
            return false;
        }

        normalizedHost = trimmed;
        return true;
    }

    private static bool IsAutomaticPrivateAddress(IPAddress address)
    {
        if (address.AddressFamily != AddressFamily.InterNetwork)
        {
            return false;
        }

        var bytes = address.GetAddressBytes();
        return bytes[0] == 169 && bytes[1] == 254;
    }
}
