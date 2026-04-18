using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Linq;

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

    public static bool IsLocalHost(string? host)
    {
        if (string.IsNullOrWhiteSpace(host))
        {
            return false;
        }

        var trimmed = host.Trim();
        if (IsLoopbackHost(trimmed) || IsAllInterfaces(trimmed))
        {
            return true;
        }

        if (TryNormalizeConfiguredHost(Environment.GetEnvironmentVariable("NBN_DEFAULT_ADVERTISE_HOST"), out var configured)
            && string.Equals(trimmed, configured, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (IsKnownLocalHostName(trimmed))
        {
            return true;
        }

        var localAddresses = EnumerateCandidateAddresses().ToArray();
        if (IPAddress.TryParse(trimmed, out var parsedAddress))
        {
            return localAddresses.Any(address => address.Equals(parsedAddress));
        }

        try
        {
            foreach (var resolved in Dns.GetHostAddresses(trimmed))
            {
                if (localAddresses.Any(address => address.Equals(resolved)))
                {
                    return true;
                }
            }
        }
        catch
        {
        }

        return false;
    }

    private static bool TryResolveDefaultAdvertisedHost(out string host)
    {
        host = string.Empty;

        var configured = Environment.GetEnvironmentVariable("NBN_DEFAULT_ADVERTISE_HOST");
        if (TryNormalizeConfiguredHost(configured, out host))
        {
            return true;
        }

        return TryChooseDefaultAdvertisedHost(EnumerateCandidateAddresses(), out host);
    }

    internal static bool TryChooseDefaultAdvertisedHost(IEnumerable<IPAddress> addresses, out string host)
    {
        var candidates = addresses.ToArray();
        if (TryChooseAddress(
                candidates,
                address => IsUsableIpv4Address(address) && IsPrivateLanIpv4Address(address),
                out host))
        {
            return true;
        }

        if (TryChooseAddress(
                candidates,
                address => IsUsableIpv4Address(address) && !IsCarrierGradeNatIpv4Address(address),
                out host))
        {
            return true;
        }

        if (TryChooseAddress(candidates, IsUsableIpv4Address, out host))
        {
            return true;
        }

        return TryChooseAddress(candidates, IsUsableIpv6Address, out host);
    }

    private static bool TryChooseAddress(
        IEnumerable<IPAddress> addresses,
        Func<IPAddress, bool> predicate,
        out string host)
    {
        foreach (var address in addresses)
        {
            if (predicate(address))
            {
                host = address.ToString();
                return true;
            }
        }

        host = string.Empty;
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

    private static bool IsKnownLocalHostName(string host)
    {
        if (string.IsNullOrWhiteSpace(host))
        {
            return false;
        }

        if (string.Equals(host, Dns.GetHostName(), StringComparison.OrdinalIgnoreCase)
            || string.Equals(host, Environment.MachineName, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        try
        {
            var properties = IPGlobalProperties.GetIPGlobalProperties();
            if (string.Equals(host, properties.HostName, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (!string.IsNullOrWhiteSpace(properties.DomainName))
            {
                var fqdn = $"{properties.HostName}.{properties.DomainName}";
                if (string.Equals(host, fqdn, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
        }
        catch
        {
        }

        return false;
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

    private static bool IsUsableIpv4Address(IPAddress address)
        => address.AddressFamily == AddressFamily.InterNetwork
           && !IPAddress.IsLoopback(address)
           && !IsAutomaticPrivateAddress(address);

    private static bool IsUsableIpv6Address(IPAddress address)
        => address.AddressFamily == AddressFamily.InterNetworkV6
           && !IPAddress.IsLoopback(address)
           && !address.IsIPv6LinkLocal;

    private static bool IsPrivateLanIpv4Address(IPAddress address)
    {
        if (address.AddressFamily != AddressFamily.InterNetwork)
        {
            return false;
        }

        var bytes = address.GetAddressBytes();
        return bytes[0] == 10
               || (bytes[0] == 172 && bytes[1] >= 16 && bytes[1] <= 31)
               || (bytes[0] == 192 && bytes[1] == 168);
    }

    private static bool IsCarrierGradeNatIpv4Address(IPAddress address)
    {
        if (address.AddressFamily != AddressFamily.InterNetwork)
        {
            return false;
        }

        var bytes = address.GetAddressBytes();
        return bytes[0] == 100 && bytes[1] >= 64 && bytes[1] <= 127;
    }
}
