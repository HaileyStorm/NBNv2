using System.Globalization;
using System.Net;
using Proto;

namespace Nbn.Runtime.Brain;

public sealed partial class BrainSignalRouterActor
{
    private static bool TryParsePid(string? value, out PID pid)
    {
        pid = new PID();
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
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

    private static bool SenderMatchesPid(PID? sender, PID expected)
    {
        if (sender is null)
        {
            return false;
        }

        return expected.Equals(sender)
               || PidEquals(sender, expected)
               || PidHasEquivalentEndpoint(sender, expected);
    }

    private static bool PidEquals(PID? left, PID right)
        => left is not null
           && string.Equals(left.Address ?? string.Empty, right.Address ?? string.Empty, StringComparison.OrdinalIgnoreCase)
           && string.Equals(left.Id ?? string.Empty, right.Id ?? string.Empty, StringComparison.Ordinal);

    private static bool PidHasEquivalentEndpoint(PID sender, PID expected)
    {
        if (!string.Equals(sender.Id ?? string.Empty, expected.Id ?? string.Empty, StringComparison.Ordinal))
        {
            return false;
        }

        if (!TryParseEndpoint(sender.Address, out var senderHost, out var senderPort)
            || !TryParseEndpoint(expected.Address, out var expectedHost, out var expectedPort))
        {
            return false;
        }

        if (senderPort != expectedPort)
        {
            return false;
        }

        if (string.Equals(senderHost, expectedHost, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var senderClass = ClassifyEndpointHost(senderHost);
        var expectedClass = ClassifyEndpointHost(expectedHost);
        if (senderClass == EndpointHostClass.Loopback && expectedClass == EndpointHostClass.Loopback)
        {
            return true;
        }

        if ((senderClass == EndpointHostClass.Wildcard && expectedClass == EndpointHostClass.Loopback)
            || (senderClass == EndpointHostClass.Loopback && expectedClass == EndpointHostClass.Wildcard))
        {
            return true;
        }

        return HostsResolveToSameAddress(senderHost, expectedHost);
    }

    private static bool TryParseEndpoint(string? address, out string host, out int port)
    {
        host = string.Empty;
        port = 0;
        if (string.IsNullOrWhiteSpace(address))
        {
            return false;
        }

        var trimmed = address.Trim();
        if (trimmed.Length == 0)
        {
            return false;
        }

        if (trimmed[0] == '[')
        {
            var closingBracket = trimmed.IndexOf(']');
            if (closingBracket <= 1 || closingBracket >= trimmed.Length - 1 || trimmed[closingBracket + 1] != ':')
            {
                return false;
            }

            var bracketHost = trimmed[1..closingBracket];
            var bracketPort = trimmed[(closingBracket + 2)..];
            if (!int.TryParse(bracketPort, NumberStyles.None, CultureInfo.InvariantCulture, out port) || port <= 0)
            {
                return false;
            }

            host = bracketHost;
            return !string.IsNullOrWhiteSpace(host);
        }

        var separator = trimmed.LastIndexOf(':');
        if (separator <= 0 || separator >= trimmed.Length - 1)
        {
            return false;
        }

        var hostToken = trimmed[..separator];
        var portToken = trimmed[(separator + 1)..];
        if (!int.TryParse(portToken, NumberStyles.None, CultureInfo.InvariantCulture, out port) || port <= 0)
        {
            return false;
        }

        host = hostToken;
        return !string.IsNullOrWhiteSpace(host);
    }

    private static EndpointHostClass ClassifyEndpointHost(string host)
    {
        var normalized = host.Trim();
        if (normalized.StartsWith("[", StringComparison.Ordinal) && normalized.EndsWith("]", StringComparison.Ordinal))
        {
            normalized = normalized[1..^1];
        }

        if (normalized.Equals("localhost", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("::1", StringComparison.OrdinalIgnoreCase))
        {
            return EndpointHostClass.Loopback;
        }

        if (normalized.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("::", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("*", StringComparison.Ordinal)
            || normalized.Equals("+", StringComparison.Ordinal))
        {
            return EndpointHostClass.Wildcard;
        }

        return EndpointHostClass.Other;
    }

    private static bool HostsResolveToSameAddress(string senderHost, string expectedHost)
    {
        var senderAddresses = ResolveEndpointHostAddresses(senderHost);
        if (senderAddresses.Count == 0)
        {
            return false;
        }

        var expectedAddresses = ResolveEndpointHostAddresses(expectedHost);
        if (expectedAddresses.Count == 0)
        {
            return false;
        }

        foreach (var senderAddress in senderAddresses)
        {
            if (expectedAddresses.Contains(senderAddress, StringComparer.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static IReadOnlyList<string> ResolveEndpointHostAddresses(string host)
    {
        var normalized = host.Trim();
        if (normalized.StartsWith("[", StringComparison.Ordinal) && normalized.EndsWith("]", StringComparison.Ordinal))
        {
            normalized = normalized[1..^1];
        }

        if (normalized.Length == 0)
        {
            return Array.Empty<string>();
        }

        if (IPAddress.TryParse(normalized, out var parsed))
        {
            return [NormalizeComparableAddress(parsed)];
        }

        try
        {
            return Dns.GetHostAddresses(normalized)
                .Select(NormalizeComparableAddress)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
        catch
        {
            return Array.Empty<string>();
        }
    }

    private static string NormalizeComparableAddress(IPAddress address)
    {
        if (address.IsIPv4MappedToIPv6)
        {
            address = address.MapToIPv4();
        }

        return address.ToString();
    }

    private static string PidLabel(PID? pid)
        => pid is null
            ? "<missing>"
            : string.IsNullOrWhiteSpace(pid.Address)
                ? pid.Id
                : $"{pid.Address}/{pid.Id}";

    private enum EndpointHostClass
    {
        Loopback,
        Wildcard,
        Other
    }
}
