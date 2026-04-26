using Nbn.Shared;

namespace Nbn.Runtime.Ppo;

public sealed partial class PpoManagerActor
{
    public sealed record DiscoverySnapshotApplied(IReadOnlyDictionary<string, ServiceEndpointRegistration> Registrations);

    public sealed record EndpointStateObserved(ServiceEndpointObservation Observation);

    private void ApplyDiscoverySnapshot(DiscoverySnapshotApplied snapshot)
    {
        if (snapshot.Registrations is null)
        {
            return;
        }

        _ioPid = snapshot.Registrations.TryGetValue(ServiceEndpointSettings.IoGatewayKey, out var io)
            ? io.Endpoint.ToPid()
            : _configuredIoPid;

        _reproductionPid = snapshot.Registrations.TryGetValue(ServiceEndpointSettings.ReproductionManagerKey, out var reproduction)
            ? reproduction.Endpoint.ToPid()
            : _configuredReproductionPid;

        _speciationPid = snapshot.Registrations.TryGetValue(ServiceEndpointSettings.SpeciationManagerKey, out var speciation)
            ? speciation.Endpoint.ToPid()
            : _configuredSpeciationPid;
    }

    private void ApplyObservedEndpoint(ServiceEndpointObservation observation)
    {
        if (string.Equals(observation.Key, ServiceEndpointSettings.ReproductionManagerKey, StringComparison.Ordinal))
        {
            _reproductionPid = observation.Kind == ServiceEndpointObservationKind.Upserted
                && observation.Registration is ServiceEndpointRegistration reproduction
                    ? reproduction.Endpoint.ToPid()
                    : _configuredReproductionPid;
            return;
        }

        if (string.Equals(observation.Key, ServiceEndpointSettings.SpeciationManagerKey, StringComparison.Ordinal))
        {
            _speciationPid = observation.Kind == ServiceEndpointObservationKind.Upserted
                && observation.Registration is ServiceEndpointRegistration speciation
                    ? speciation.Endpoint.ToPid()
                    : _configuredSpeciationPid;
            return;
        }

        if (!string.Equals(observation.Key, ServiceEndpointSettings.IoGatewayKey, StringComparison.Ordinal))
        {
            return;
        }

        _ioPid = observation.Kind == ServiceEndpointObservationKind.Upserted
            && observation.Registration is ServiceEndpointRegistration io
                ? io.Endpoint.ToPid()
                : _configuredIoPid;
    }
}
