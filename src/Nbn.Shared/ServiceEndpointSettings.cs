using Proto;

namespace Nbn.Shared;

/// <summary>
/// Identifies a remotely reachable service root by host:port and actor name.
/// </summary>
/// <param name="HostPort">The advertised remoting host and port.</param>
/// <param name="ActorName">The root actor name at that address.</param>
public readonly record struct ServiceEndpoint(string HostPort, string ActorName)
{
    /// <summary>
    /// Encodes the endpoint as the canonical settings value.
    /// </summary>
    public string ToSettingValue() => ServiceEndpointSettings.EncodeValue(HostPort, ActorName);

    /// <summary>
    /// Converts the endpoint to a Proto.Actor PID.
    /// </summary>
    public PID ToPid() => new(HostPort, ActorName);

    public override string ToString() => ToSettingValue();
}

/// <summary>
/// Represents a resolved service endpoint setting and its last update time.
/// </summary>
/// <param name="Key">The canonical settings key.</param>
/// <param name="Endpoint">The parsed service endpoint.</param>
/// <param name="UpdatedMs">The last update timestamp in milliseconds.</param>
public readonly record struct ServiceEndpointRegistration(
    string Key,
    ServiceEndpoint Endpoint,
    long UpdatedMs);

/// <summary>
/// Describes the outcome of observing a service endpoint setting change.
/// </summary>
public enum ServiceEndpointObservationKind
{
    /// <summary>
    /// The setting contains a valid endpoint registration.
    /// </summary>
    Upserted = 0,

    /// <summary>
    /// The setting was removed or cleared.
    /// </summary>
    Removed = 1,

    /// <summary>
    /// The setting update could not be parsed into a valid endpoint.
    /// </summary>
    Invalid = 2
}

/// <summary>
/// Captures the normalized outcome of a watched service endpoint setting update.
/// </summary>
/// <param name="Key">The settings key that changed.</param>
/// <param name="Kind">The normalized observation result.</param>
/// <param name="Registration">The parsed registration when the update is valid.</param>
/// <param name="FailureReason">The normalized failure reason when parsing failed.</param>
/// <param name="UpdatedMs">The observed update timestamp in milliseconds.</param>
public readonly record struct ServiceEndpointObservation(
    string Key,
    ServiceEndpointObservationKind Kind,
    ServiceEndpointRegistration? Registration,
    string FailureReason,
    ulong UpdatedMs);

/// <summary>
/// Defines the canonical settings keys and value encoding used for service endpoint discovery.
/// </summary>
public static class ServiceEndpointSettings
{
    /// <summary>
    /// Prefix applied to all service endpoint settings keys.
    /// </summary>
    public const string EndpointPrefix = "service.endpoint.";

    /// <summary>
    /// Settings key for the HiveMind endpoint.
    /// </summary>
    public const string HiveMindKey = EndpointPrefix + "hivemind";

    /// <summary>
    /// Settings key for the IO Gateway endpoint.
    /// </summary>
    public const string IoGatewayKey = EndpointPrefix + "io_gateway";

    /// <summary>
    /// Settings key for the reproduction manager endpoint.
    /// </summary>
    public const string ReproductionManagerKey = EndpointPrefix + "reproduction_manager";

    /// <summary>
    /// Settings key for the speciation manager endpoint.
    /// </summary>
    public const string SpeciationManagerKey = EndpointPrefix + "speciation_manager";

    /// <summary>
    /// Settings key for the worker node endpoint.
    /// </summary>
    public const string WorkerNodeKey = EndpointPrefix + "worker_node";

    /// <summary>
    /// Settings key for the observability endpoint.
    /// </summary>
    public const string ObservabilityKey = EndpointPrefix + "observability";

    private static readonly string[] KnownEndpointKeys =
    [
        HiveMindKey,
        IoGatewayKey,
        ReproductionManagerKey,
        SpeciationManagerKey,
        WorkerNodeKey,
        ObservabilityKey
    ];

    /// <summary>
    /// Gets the complete set of canonical service endpoint keys.
    /// </summary>
    public static IReadOnlyList<string> AllKeys => KnownEndpointKeys;

    /// <summary>
    /// Determines whether the supplied key is one of the canonical service endpoint keys.
    /// </summary>
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

    /// <summary>
    /// Encodes a service endpoint into the canonical <c>host:port/actor</c> settings form.
    /// </summary>
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

    /// <summary>
    /// Attempts to parse a canonical endpoint settings value.
    /// </summary>
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

    /// <summary>
    /// Attempts to parse a service endpoint setting into a registration record.
    /// </summary>
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
