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

    public static ServiceEndpointSet BuildEndpointSet(string bindHost, string? advertisedHost, int port, string actorName)
    {
        if (port <= 0 || port >= 65536)
        {
            throw new ArgumentOutOfRangeException(nameof(port), "Port must be between 1 and 65535.");
        }

        if (string.IsNullOrWhiteSpace(actorName))
        {
            throw new ArgumentException("Actor name is required.", nameof(actorName));
        }

        return new ServiceEndpointSet(actorName.Trim(), BuildEndpointCandidates(bindHost, advertisedHost, port, actorName));
    }

    public static IReadOnlyList<ServiceEndpointCandidate> BuildEndpointCandidates(
        string bindHost,
        string? advertisedHost,
        int port,
        string actorName)
    {
        if (port <= 0 || port >= 65536)
        {
            throw new ArgumentOutOfRangeException(nameof(port), "Port must be between 1 and 65535.");
        }

        if (string.IsNullOrWhiteSpace(actorName))
        {
            throw new ArgumentException("Actor name is required.", nameof(actorName));
        }

        var normalizedActorName = actorName.Trim();
        var candidates = new List<ServiceEndpointCandidate>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var defaultHost = ResolveAdvertisedHost(bindHost, advertisedHost);

        void AddCandidate(string? host, ServiceEndpointCandidateKind kind, int priority, string label, bool isDefault)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                return;
            }

            var trimmedHost = host.Trim();
            if (!seen.Add(trimmedHost))
            {
                return;
            }

            candidates.Add(new ServiceEndpointCandidate(
                $"{trimmedHost}:{port}",
                normalizedActorName,
                kind,
                priority,
                label,
                isDefault));
        }

        AddCandidate(
            defaultHost,
            ClassifyHost(defaultHost, interfaceType: null, interfaceName: null),
            priority: 1000,
            label: "default_advertised",
            isDefault: true);

        if (!string.IsNullOrWhiteSpace(advertisedHost))
        {
            AddCandidate(
                advertisedHost,
                ClassifyHost(advertisedHost, interfaceType: null, interfaceName: null),
                priority: 950,
                label: "explicit_advertise_host",
                isDefault: false);
        }

        if (!IsAllInterfaces(bindHost))
        {
            AddCandidate(
                bindHost,
                ClassifyHost(bindHost, interfaceType: null, interfaceName: null),
                priority: 850,
                label: "bind_host",
                isDefault: false);

            return candidates;
        }

        foreach (var candidate in EnumerateEndpointHostCandidates(includeTunnelInterfaces: true))
        {
            AddCandidate(candidate.Host, candidate.Kind, candidate.Priority, candidate.Label, isDefault: false);
        }

        return candidates;
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

        var preferredCandidate = EnumerateEndpointHostCandidates(includeTunnelInterfaces: true)
            .OrderBy(candidate => DefaultAdvertisedHostRank(candidate.Kind))
            .ThenBy(candidate => candidate.Host.Contains(':', StringComparison.Ordinal) ? 1 : 0)
            .ThenByDescending(candidate => candidate.Priority)
            .FirstOrDefault();
        if (!string.IsNullOrWhiteSpace(preferredCandidate.Host))
        {
            host = preferredCandidate.Host;
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

    private static IEnumerable<EndpointHostCandidate> EnumerateEndpointHostCandidates(bool includeTunnelInterfaces)
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
                || (!includeTunnelInterfaces && nic.NetworkInterfaceType == NetworkInterfaceType.Tunnel)
                || IsIgnoredInterfaceName(nic.Name))
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
                if (IPAddress.IsLoopback(address)
                    || IsAutomaticPrivateAddress(address)
                    || (address.AddressFamily == AddressFamily.InterNetworkV6 && address.IsIPv6LinkLocal))
                {
                    continue;
                }

                var host = address.ToString();
                if (!seen.Add(host))
                {
                    continue;
                }

                var kind = ClassifyHost(host, nic.NetworkInterfaceType, nic.Name);
                yield return new EndpointHostCandidate(host, kind, CandidatePriority(kind), $"interface:{nic.Name}");
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

    private static bool IsIgnoredInterfaceName(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return false;
        }

        var normalized = name.Trim().ToLowerInvariant();
        return normalized.StartsWith("docker", StringComparison.Ordinal)
               || normalized.StartsWith("br-", StringComparison.Ordinal)
               || normalized.StartsWith("veth", StringComparison.Ordinal)
               || normalized.StartsWith("virbr", StringComparison.Ordinal)
               || normalized.StartsWith("cni", StringComparison.Ordinal)
               || normalized.StartsWith("podman", StringComparison.Ordinal);
    }

    private static ServiceEndpointCandidateKind ClassifyHost(
        string host,
        NetworkInterfaceType? interfaceType,
        string? interfaceName)
    {
        if (IsLoopbackHost(host))
        {
            return ServiceEndpointCandidateKind.Loopback;
        }

        if (interfaceType == NetworkInterfaceType.Tunnel)
        {
            return string.IsNullOrWhiteSpace(interfaceName) || !interfaceName.Contains("tail", StringComparison.OrdinalIgnoreCase)
                ? ServiceEndpointCandidateKind.Custom
                : ServiceEndpointCandidateKind.Tailnet;
        }

        if (!IPAddress.TryParse(host, out var address))
        {
            return ServiceEndpointCandidateKind.Custom;
        }

        if (IsTailnetAddress(address))
        {
            return ServiceEndpointCandidateKind.Tailnet;
        }

        return IsPrivateAddress(address)
            ? ServiceEndpointCandidateKind.Lan
            : ServiceEndpointCandidateKind.Public;
    }

    private static int CandidatePriority(ServiceEndpointCandidateKind kind)
    {
        return kind switch
        {
            ServiceEndpointCandidateKind.Public => 700,
            ServiceEndpointCandidateKind.Tailnet => 650,
            ServiceEndpointCandidateKind.Lan => 600,
            ServiceEndpointCandidateKind.Custom => 500,
            ServiceEndpointCandidateKind.Loopback => 100,
            _ => 400
        };
    }

    private static bool IsPrivateAddress(IPAddress address)
    {
        if (address.AddressFamily == AddressFamily.InterNetwork)
        {
            var bytes = address.GetAddressBytes();
            return bytes[0] == 10
                   || (bytes[0] == 172 && bytes[1] >= 16 && bytes[1] <= 31)
                   || (bytes[0] == 192 && bytes[1] == 168)
                   || (bytes[0] == 169 && bytes[1] == 254);
        }

        if (address.AddressFamily == AddressFamily.InterNetworkV6)
        {
            return address.IsIPv6LinkLocal
                   || address.IsIPv6SiteLocal
                   || address.ToString().StartsWith("fd", StringComparison.OrdinalIgnoreCase)
                   || address.ToString().StartsWith("fc", StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    private static bool IsTailnetAddress(IPAddress address)
    {
        if (address.AddressFamily == AddressFamily.InterNetwork)
        {
            var bytes = address.GetAddressBytes();
            return bytes[0] == 100 && bytes[1] >= 64 && bytes[1] <= 127;
        }

        if (address.AddressFamily == AddressFamily.InterNetworkV6)
        {
            var normalized = address.ToString();
            return normalized.StartsWith("fd7a:115c:a1e0:", StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    private static int DefaultAdvertisedHostRank(ServiceEndpointCandidateKind kind)
    {
        return kind switch
        {
            ServiceEndpointCandidateKind.Lan => 0,
            ServiceEndpointCandidateKind.Custom => 1,
            ServiceEndpointCandidateKind.Tailnet => 2,
            ServiceEndpointCandidateKind.Public => 3,
            ServiceEndpointCandidateKind.Loopback => 4,
            _ => 5
        };
    }

    private readonly record struct EndpointHostCandidate(
        string Host,
        ServiceEndpointCandidateKind Kind,
        int Priority,
        string Label);
}
