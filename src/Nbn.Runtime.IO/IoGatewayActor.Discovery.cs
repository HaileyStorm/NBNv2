using Nbn.Proto;
using Nbn.Shared;
using Proto;

namespace Nbn.Runtime.IO;

public sealed partial class IoGatewayActor
{
    /// <summary>
    /// Applies a full discovery snapshot for runtime service endpoints.
    /// </summary>
    public sealed record DiscoverySnapshotApplied(IReadOnlyDictionary<string, ServiceEndpointRegistration> Registrations);

    /// <summary>
    /// Applies an incremental discovery observation for a runtime service endpoint.
    /// </summary>
    public sealed record EndpointStateObserved(ServiceEndpointObservation Observation);

    private void ApplyDiscoverySnapshot(DiscoverySnapshotApplied snapshot)
    {
        if (snapshot.Registrations is null)
        {
            return;
        }

        var hiveSeen = false;
        var reproSeen = false;
        var speciationSeen = false;
        foreach (var entry in snapshot.Registrations)
        {
            if (!TryMarkDiscoveryKey(entry.Key, ref hiveSeen, ref reproSeen, ref speciationSeen))
            {
                continue;
            }

            ApplyEndpoint(entry.Value);
        }

        if (!hiveSeen)
        {
            _hiveMindPid = _configuredHiveMindPid;
        }

        if (!reproSeen)
        {
            _reproPid = _configuredReproPid;
        }

        if (!speciationSeen)
        {
            _speciationPid = _configuredSpeciationPid;
        }
    }

    private void ApplyObservedEndpoint(ServiceEndpointObservation observation, string source)
    {
        if (!IsDiscoveryKey(observation.Key))
        {
            return;
        }

        if (observation.Kind == ServiceEndpointObservationKind.Upserted)
        {
            if (observation.Registration is ServiceEndpointRegistration registration)
            {
                ApplyEndpoint(registration);
                return;
            }

            ApplyObservationRemoval(observation.Key, source, "registration_missing");
            return;
        }

        if (observation.Kind == ServiceEndpointObservationKind.Removed
            || observation.Kind == ServiceEndpointObservationKind.Invalid)
        {
            var fallbackReason = observation.Kind == ServiceEndpointObservationKind.Removed
                ? "endpoint_removed"
                : "endpoint_parse_failed";
            ApplyObservationRemoval(observation.Key, source, NormalizeFailureReason(observation.FailureReason, fallbackReason));
        }
    }

    private void ApplyEndpoint(ServiceEndpointRegistration registration)
    {
        if (string.Equals(registration.Key, ServiceEndpointSettings.HiveMindKey, StringComparison.Ordinal))
        {
            _hiveMindPid = registration.Endpoint.ToPid();
            return;
        }

        if (string.Equals(registration.Key, ServiceEndpointSettings.ReproductionManagerKey, StringComparison.Ordinal))
        {
            _reproPid = registration.Endpoint.ToPid();
            return;
        }

        if (string.Equals(registration.Key, ServiceEndpointSettings.SpeciationManagerKey, StringComparison.Ordinal))
        {
            _speciationPid = registration.Endpoint.ToPid();
        }
    }

    private async Task<TResponse?> RequestHiveMindAsync<TResponse>(
        IContext context,
        object request,
        TimeSpan timeout,
        string operation)
        where TResponse : class
    {
        var initialPid = _hiveMindPid;
        if (initialPid is null)
        {
            return null;
        }

        try
        {
            return await context.RequestAsync<TResponse>(initialPid, request, timeout).ConfigureAwait(false);
        }
        catch (Exception ex) when (IsStaleServiceEndpointFailure(ex))
        {
            var refreshed = await TryRefreshHiveMindEndpointAsync(context, operation, ex).ConfigureAwait(false);
            var retryPid = _hiveMindPid;
            if (!refreshed || retryPid is null)
            {
                throw;
            }

            Console.WriteLine(
                $"[WARN] IO retrying {operation} after HiveMind endpoint refresh: previous={PidLabel(initialPid)} current={PidLabel(retryPid)} failure={ex.GetBaseException().Message}");
            return await context.RequestAsync<TResponse>(retryPid, request, timeout).ConfigureAwait(false);
        }
    }

    private async Task<bool> TryRefreshHiveMindEndpointAsync(IContext context, string operation, Exception failure)
    {
        var client = ServiceEndpointDiscoveryClient.Create(
            context.System,
            _options.SettingsHost,
            _options.SettingsPort,
            _options.SettingsName);
        if (client is null)
        {
            return false;
        }

        await using (client.ConfigureAwait(false))
        {
            try
            {
                var registration = await client.ResolveAsync(ServiceEndpointSettings.HiveMindKey).ConfigureAwait(false);
                if (registration is not null)
                {
                    ApplyEndpoint(registration.Value);
                    return _hiveMindPid is not null;
                }

                _hiveMindPid = _configuredHiveMindPid;
                return _hiveMindPid is not null;
            }
            catch (Exception refreshEx)
            {
                Console.WriteLine(
                    $"[WARN] IO failed to refresh HiveMind endpoint after {operation} failure ({failure.GetBaseException().Message}): {refreshEx.GetBaseException().Message}");
                return false;
            }
        }
    }

    private void ApplyObservationRemoval(string key, string source, string reason)
    {
        if (string.Equals(key, ServiceEndpointSettings.HiveMindKey, StringComparison.Ordinal))
        {
            _hiveMindPid = _configuredHiveMindPid;
            Console.WriteLine($"[WARN] IO discovery removed hive endpoint (source={source}, reason={reason}).");
            return;
        }

        if (string.Equals(key, ServiceEndpointSettings.ReproductionManagerKey, StringComparison.Ordinal))
        {
            _reproPid = _configuredReproPid;
            Console.WriteLine($"[WARN] IO discovery removed repro endpoint (source={source}, reason={reason}).");
            return;
        }

        if (string.Equals(key, ServiceEndpointSettings.SpeciationManagerKey, StringComparison.Ordinal))
        {
            _speciationPid = _configuredSpeciationPid;
            Console.WriteLine($"[WARN] IO discovery removed speciation endpoint (source={source}, reason={reason}).");
        }
    }

    private static bool IsDiscoveryKey(string? key)
        => string.Equals(key, ServiceEndpointSettings.HiveMindKey, StringComparison.Ordinal)
           || string.Equals(key, ServiceEndpointSettings.ReproductionManagerKey, StringComparison.Ordinal)
           || string.Equals(key, ServiceEndpointSettings.SpeciationManagerKey, StringComparison.Ordinal);

    private static bool TryMarkDiscoveryKey(
        string? key,
        ref bool hiveSeen,
        ref bool reproSeen,
        ref bool speciationSeen)
    {
        if (string.Equals(key, ServiceEndpointSettings.HiveMindKey, StringComparison.Ordinal))
        {
            hiveSeen = true;
            return true;
        }

        if (string.Equals(key, ServiceEndpointSettings.ReproductionManagerKey, StringComparison.Ordinal))
        {
            reproSeen = true;
            return true;
        }

        if (string.Equals(key, ServiceEndpointSettings.SpeciationManagerKey, StringComparison.Ordinal))
        {
            speciationSeen = true;
            return true;
        }

        return false;
    }

    private static string NormalizeFailureReason(string? value, string fallback)
        => string.IsNullOrWhiteSpace(value) ? fallback : value.Trim();

    private static bool IsStaleServiceEndpointFailure(Exception ex)
    {
        var failure = ex.GetBaseException();
        if (failure is TimeoutException)
        {
            return false;
        }

        var detail = failure.Message;
        return !string.IsNullOrWhiteSpace(detail)
               && (detail.Contains("no longer exists", StringComparison.OrdinalIgnoreCase)
                   || detail.Contains("unknown actor", StringComparison.OrdinalIgnoreCase));
    }
}
