using Proto;
using System.Text.Json;
using System.Text.Json.Serialization;

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

public enum ServiceEndpointCandidateKind
{
    Unspecified = 0,
    Lan = 1,
    Public = 2,
    Tailnet = 3,
    Loopback = 4,
    Custom = 5
}

public readonly record struct ServiceEndpointCandidate(
    string HostPort,
    string ActorName,
    ServiceEndpointCandidateKind Kind = ServiceEndpointCandidateKind.Unspecified,
    int Priority = 0,
    string Label = "",
    bool IsDefault = false)
{
    public ServiceEndpoint ToEndpoint() => new(HostPort, ActorName);
}

public readonly record struct ServiceEndpointSet(
    string ActorName,
    IReadOnlyList<ServiceEndpointCandidate> Candidates)
{
    public ServiceEndpointCandidate GetPreferredCandidate()
    {
        if (Candidates.Count == 0)
        {
            throw new InvalidOperationException("Endpoint set does not contain any candidates.");
        }

        return Candidates
            .OrderByDescending(candidate => candidate.IsDefault)
            .ThenByDescending(candidate => candidate.Priority)
            .ThenBy(candidate => candidate.HostPort, StringComparer.OrdinalIgnoreCase)
            .First();
    }
}

public readonly record struct ServiceEndpointSetRegistration(
    string Key,
    ServiceEndpointSet EndpointSet,
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
    public const string EndpointSetPrefix = "service.endpoint_set.";
    public const string HiveMindKey = EndpointPrefix + "hivemind";
    public const string IoGatewayKey = EndpointPrefix + "io_gateway";
    public const string ReproductionManagerKey = EndpointPrefix + "reproduction_manager";
    public const string SpeciationManagerKey = EndpointPrefix + "speciation_manager";
    public const string WorkerNodeKey = EndpointPrefix + "worker_node";
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

    public static IReadOnlyList<string> AllKeys => KnownEndpointKeys;

    public static string ToEndpointSetKey(string endpointKey)
    {
        if (string.IsNullOrWhiteSpace(endpointKey))
        {
            throw new ArgumentException("Endpoint key is required.", nameof(endpointKey));
        }

        var trimmed = endpointKey.Trim();
        if (!trimmed.StartsWith(EndpointPrefix, StringComparison.Ordinal))
        {
            throw new ArgumentException($"Endpoint key '{trimmed}' is not a known service endpoint key.", nameof(endpointKey));
        }

        return EndpointSetPrefix + trimmed.Substring(EndpointPrefix.Length);
    }

    public static bool IsKnownSetKey(string? key)
        => TryGetEndpointKeyFromSetKey(key, out _);

    public static bool TryGetEndpointKeyFromSetKey(string? key, out string endpointKey)
    {
        endpointKey = string.Empty;
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        var trimmed = key.Trim();
        if (!trimmed.StartsWith(EndpointSetPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        endpointKey = EndpointPrefix + trimmed.Substring(EndpointSetPrefix.Length);
        return IsKnownKey(endpointKey);
    }

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

    public static string EncodeSetValue(string actorName, IEnumerable<ServiceEndpointCandidate> candidates)
    {
        if (string.IsNullOrWhiteSpace(actorName))
        {
            throw new ArgumentException("Actor name is required.", nameof(actorName));
        }

        if (candidates is null)
        {
            throw new ArgumentNullException(nameof(candidates));
        }

        var normalized = NormalizeCandidates(actorName, candidates);
        if (normalized.Count == 0)
        {
            throw new ArgumentException("At least one endpoint candidate is required.", nameof(candidates));
        }

        var payload = new ServiceEndpointSetPayload(
            actorName.Trim(),
            normalized
                .Select(candidate => new ServiceEndpointCandidatePayload(
                    candidate.HostPort,
                    candidate.ActorName,
                    candidate.Kind.ToString().ToLowerInvariant(),
                    candidate.Priority,
                    candidate.Label,
                    candidate.IsDefault))
                .ToArray());

        return JsonSerializer.Serialize(payload, SerializerOptions);
    }

    public static bool TryParseSetValue(string? value, out ServiceEndpointSet endpointSet)
    {
        endpointSet = default;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        try
        {
            var payload = JsonSerializer.Deserialize<ServiceEndpointSetPayload>(value.Trim(), SerializerOptions);
            if (payload is null || string.IsNullOrWhiteSpace(payload.ActorName) || payload.Candidates is null || payload.Candidates.Count == 0)
            {
                return false;
            }

            var normalized = new List<ServiceEndpointCandidate>(payload.Candidates.Count);
            foreach (var candidatePayload in payload.Candidates)
            {
                if (candidatePayload is null
                    || string.IsNullOrWhiteSpace(candidatePayload.HostPort)
                    || string.IsNullOrWhiteSpace(candidatePayload.ActorName))
                {
                    return false;
                }

                var actorName = candidatePayload.ActorName.Trim();
                if (!string.Equals(actorName, payload.ActorName.Trim(), StringComparison.Ordinal))
                {
                    return false;
                }

                if (!TryParseCandidateKind(candidatePayload.Kind, out var kind))
                {
                    return false;
                }

                normalized.Add(new ServiceEndpointCandidate(
                    candidatePayload.HostPort.Trim(),
                    actorName,
                    kind,
                    candidatePayload.Priority,
                    candidatePayload.Label?.Trim() ?? string.Empty,
                    candidatePayload.IsDefault));
            }

            endpointSet = new ServiceEndpointSet(payload.ActorName.Trim(), normalized);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
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

    public static bool TryParseSetSetting(
        string? key,
        string? value,
        ulong updatedMs,
        out ServiceEndpointSetRegistration registration)
    {
        registration = default;
        if (string.IsNullOrWhiteSpace(key) || !IsKnownSetKey(key))
        {
            return false;
        }

        if (!TryParseSetValue(value, out var endpointSet))
        {
            return false;
        }

        registration = new ServiceEndpointSetRegistration(
            key.Trim(),
            endpointSet,
            ToSignedMs(updatedMs));
        return true;
    }

    public static ServiceEndpointSet CreateLegacyFallbackSet(ServiceEndpoint endpoint)
    {
        var normalizedActorName = endpoint.ActorName?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(normalizedActorName))
        {
            throw new ArgumentException("Actor name is required.", nameof(endpoint));
        }

        return new ServiceEndpointSet(
            normalizedActorName,
            [
                new ServiceEndpointCandidate(
                    endpoint.HostPort.Trim(),
                    normalizedActorName,
                    ServiceEndpointCandidateKind.Unspecified,
                    Priority: 0,
                    Label: "legacy_single_endpoint",
                    IsDefault: true)
            ]);
    }

    private static IReadOnlyList<ServiceEndpointCandidate> NormalizeCandidates(
        string actorName,
        IEnumerable<ServiceEndpointCandidate> candidates)
    {
        var normalizedActorName = actorName.Trim();
        var normalized = new List<ServiceEndpointCandidate>();
        foreach (var candidate in candidates)
        {
            var hostPort = candidate.HostPort?.Trim() ?? string.Empty;
            var candidateActorName = candidate.ActorName?.Trim() ?? string.Empty;
            if (string.IsNullOrWhiteSpace(hostPort))
            {
                throw new ArgumentException("Endpoint candidate host:port is required.", nameof(candidates));
            }

            if (string.IsNullOrWhiteSpace(candidateActorName))
            {
                throw new ArgumentException("Endpoint candidate actor name is required.", nameof(candidates));
            }

            if (!string.Equals(candidateActorName, normalizedActorName, StringComparison.Ordinal))
            {
                throw new ArgumentException("All endpoint candidates in a set must use the same actor name.", nameof(candidates));
            }

            normalized.Add(candidate with
            {
                HostPort = hostPort,
                ActorName = candidateActorName,
                Label = candidate.Label?.Trim() ?? string.Empty
            });
        }

        return normalized;
    }

    private static bool TryParseCandidateKind(string? value, out ServiceEndpointCandidateKind kind)
    {
        kind = ServiceEndpointCandidateKind.Unspecified;
        if (string.IsNullOrWhiteSpace(value))
        {
            return true;
        }

        return Enum.TryParse(value.Trim(), ignoreCase: true, out kind);
    }

    private static long ToSignedMs(ulong updatedMs)
        => updatedMs > long.MaxValue ? long.MaxValue : (long)updatedMs;

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault
    };

    private sealed record ServiceEndpointSetPayload(
        string ActorName,
        IReadOnlyList<ServiceEndpointCandidatePayload> Candidates);

    private sealed record ServiceEndpointCandidatePayload(
        string HostPort,
        string ActorName,
        string Kind,
        int Priority,
        string Label,
        bool IsDefault);
}
