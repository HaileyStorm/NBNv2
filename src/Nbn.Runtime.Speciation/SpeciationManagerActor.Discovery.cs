using Nbn.Proto;
using Nbn.Shared;

namespace Nbn.Runtime.Speciation;

public sealed partial class SpeciationManagerActor
{
    /// <summary>
    /// Applies a full discovery snapshot to speciation's reproduction and IO endpoint hints.
    /// </summary>
    public sealed record DiscoverySnapshotApplied(IReadOnlyDictionary<string, ServiceEndpointRegistration> Registrations);

    /// <summary>
    /// Applies a single discovery observation to speciation's reproduction and IO endpoint hints.
    /// </summary>
    public sealed record EndpointStateObserved(ServiceEndpointObservation Observation);

    private void ApplyDiscoverySnapshot(DiscoverySnapshotApplied snapshot)
    {
        if (snapshot.Registrations is null)
        {
            return;
        }

        if (snapshot.Registrations.TryGetValue(ServiceEndpointSettings.ReproductionManagerKey, out var reproductionRegistration))
        {
            _reproductionManagerPid = reproductionRegistration.Endpoint.ToPid();
        }
        else
        {
            _reproductionManagerPid = _configuredReproductionManagerPid;
        }

        if (snapshot.Registrations.TryGetValue(ServiceEndpointSettings.IoGatewayKey, out var ioRegistration))
        {
            _ioGatewayPid = ioRegistration.Endpoint.ToPid();
        }
        else
        {
            _ioGatewayPid = _configuredIoGatewayPid;
        }
    }

    private void ApplyObservedEndpoint(ServiceEndpointObservation observation)
    {
        if (string.Equals(observation.Key, ServiceEndpointSettings.ReproductionManagerKey, StringComparison.Ordinal))
        {
            if (observation.Kind == ServiceEndpointObservationKind.Upserted
                && observation.Registration is ServiceEndpointRegistration reproductionRegistration)
            {
                _reproductionManagerPid = reproductionRegistration.Endpoint.ToPid();
                return;
            }

            if (observation.Kind == ServiceEndpointObservationKind.Removed
                || observation.Kind == ServiceEndpointObservationKind.Invalid
                || observation.Kind == ServiceEndpointObservationKind.Upserted)
            {
                _reproductionManagerPid = _configuredReproductionManagerPid;
            }

            return;
        }

        if (!string.Equals(observation.Key, ServiceEndpointSettings.IoGatewayKey, StringComparison.Ordinal))
        {
            return;
        }

        if (observation.Kind == ServiceEndpointObservationKind.Upserted
            && observation.Registration is ServiceEndpointRegistration ioRegistration)
        {
            _ioGatewayPid = ioRegistration.Endpoint.ToPid();
            return;
        }

        if (observation.Kind == ServiceEndpointObservationKind.Removed
            || observation.Kind == ServiceEndpointObservationKind.Invalid
            || observation.Kind == ServiceEndpointObservationKind.Upserted)
        {
            _ioGatewayPid = _configuredIoGatewayPid;
        }
    }
}
