using Proto;

namespace Nbn.Shared;

public readonly record struct ServiceEndpoint(string HostPort, string ActorName)
{
    public string ToSettingValue() => ServiceEndpointSettings.EncodeValue(HostPort, ActorName);

    public PID ToPid() => new(HostPort, ActorName);

    public override string ToString() => ToSettingValue();
}

public readonly record struct ServiceEndpointRegistration(
    string Key,
    ServiceEndpoint Endpoint,
    long UpdatedMs);

public enum ServiceEndpointObservationKind
{
    Upserted = 0,
    Removed = 1,
    Invalid = 2
}

public readonly record struct ServiceEndpointObservation(
    string Key,
    ServiceEndpointObservationKind Kind,
    ServiceEndpointRegistration? Registration,
    string FailureReason,
    ulong UpdatedMs);

public static class ServiceEndpointSettings
{
    public const string EndpointPrefix = "service.endpoint.";
    public const string HiveMindKey = EndpointPrefix + "hivemind";
    public const string IoGatewayKey = EndpointPrefix + "io_gateway";

    private static readonly string[] KnownEndpointKeys =
    [
        HiveMindKey,
        IoGatewayKey
    ];

    public static IReadOnlyList<string> AllKeys => KnownEndpointKeys;

    public static bool IsKnownKey(string? key)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        return Array.Exists(
            KnownEndpointKeys,
            candidate => string.Equals(candidate, key, StringComparison.Ordinal));
    }

    public static string EncodeValue(string hostPort, string actorName)
    {
        if (string.IsNullOrWhiteSpace(hostPort))
        {
            throw new ArgumentException("Endpoint host:port is required.", nameof(hostPort));
        }

        if (string.IsNullOrWhiteSpace(actorName))
        {
            throw new ArgumentException("Actor name is required.", nameof(actorName));
        }

        return $"{hostPort.Trim()}/{actorName.Trim()}";
    }

    public static bool TryParseValue(string? value, out ServiceEndpoint endpoint)
    {
        endpoint = default;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        var slashIndex = trimmed.IndexOf('/');
        if (slashIndex <= 0 || slashIndex >= trimmed.Length - 1)
        {
            return false;
        }

        var hostPort = trimmed[..slashIndex].Trim();
        var actorName = trimmed[(slashIndex + 1)..].Trim();
        if (hostPort.Length == 0 || actorName.Length == 0)
        {
            return false;
        }

        endpoint = new ServiceEndpoint(hostPort, actorName);
        return true;
    }

    public static bool TryParseSetting(
        string? key,
        string? value,
        ulong updatedMs,
        out ServiceEndpointRegistration registration)
    {
        registration = default;
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        if (!TryParseValue(value, out var endpoint))
        {
            return false;
        }

        registration = new ServiceEndpointRegistration(
            key.Trim(),
            endpoint,
            ToSignedMs(updatedMs));

        return true;
    }

    private static long ToSignedMs(ulong updatedMs)
        => updatedMs > long.MaxValue ? long.MaxValue : (long)updatedMs;
}
