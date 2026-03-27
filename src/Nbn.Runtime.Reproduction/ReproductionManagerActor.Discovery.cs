using Nbn.Proto;
using Nbn.Shared;
using Proto;

namespace Nbn.Runtime.Reproduction;

public sealed partial class ReproductionManagerActor
{
    /// <summary>
    /// Applies a full discovery snapshot so the manager can refresh its IO gateway endpoint.
    /// </summary>
    public sealed record DiscoverySnapshotApplied(IReadOnlyDictionary<string, ServiceEndpointRegistration> Registrations);

    /// <summary>
    /// Applies a single endpoint observation so the manager can react to IO gateway upserts and removals.
    /// </summary>
    public sealed record EndpointStateObserved(ServiceEndpointObservation Observation);

    private void ApplyDiscoverySnapshot(DiscoverySnapshotApplied snapshot)
    {
        if (snapshot.Registrations is null)
        {
            return;
        }

        if (snapshot.Registrations.TryGetValue(ServiceEndpointSettings.IoGatewayKey, out var registration))
        {
            SetIoGatewayPid(registration.Endpoint.ToPid());
            return;
        }

        ResetIoGatewayPid();
    }

    private void ApplyObservedEndpoint(ServiceEndpointObservation observation)
    {
        if (!string.Equals(observation.Key, ServiceEndpointSettings.IoGatewayKey, StringComparison.Ordinal))
        {
            return;
        }

        if (observation.Kind == ServiceEndpointObservationKind.Upserted
            && observation.Registration is ServiceEndpointRegistration registration)
        {
            SetIoGatewayPid(registration.Endpoint.ToPid());
            return;
        }

        if (observation.Kind == ServiceEndpointObservationKind.Removed
            || observation.Kind == ServiceEndpointObservationKind.Invalid
            || observation.Kind == ServiceEndpointObservationKind.Upserted)
        {
            ResetIoGatewayPid();
        }
    }

    private void ResetIoGatewayPid()
        => _ioGatewayPid = _configuredIoGatewayPid;

    private void SetIoGatewayPid(PID pid)
        => _ioGatewayPid = pid;
}
