using Nbn.Proto;
using Nbn.Shared;

namespace Nbn.Runtime.WorkerNode;

public sealed partial class WorkerNodeActor
{
    private void ApplyDiscoverySnapshot(DiscoverySnapshotApplied snapshot)
    {
        if (snapshot.Registrations is null)
        {
            WorkerNodeTelemetry.RecordDiscoveryEndpointObserved(
                _workerNodeId,
                DiscoveryTargetLabel,
                "snapshot_ignored",
                "snapshot_missing");
            return;
        }

        if (snapshot.Registrations.Count == 0)
        {
            WorkerNodeTelemetry.RecordDiscoveryEndpointObserved(
                _workerNodeId,
                DiscoveryTargetLabel,
                "snapshot_empty",
                "none");
        }

        var seenKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var entry in snapshot.Registrations)
        {
            ApplyEndpoint(entry.Value, source: "snapshot");
            if (ServiceEndpointSettings.IsKnownKey(entry.Value.Key))
            {
                seenKeys.Add(entry.Value.Key);
            }
        }

        foreach (var knownKey in ServiceEndpointSettings.AllKeys)
        {
            if (!seenKeys.Contains(knownKey))
            {
                RemoveEndpoint(
                    knownKey,
                    source: "snapshot",
                    outcome: "missing",
                    failureReason: "endpoint_missing");
            }
        }
    }

    private void ApplyObservedEndpoint(ServiceEndpointObservation observation, string source)
    {
        if (!ServiceEndpointSettings.IsKnownKey(observation.Key))
        {
            WorkerNodeTelemetry.RecordDiscoveryEndpointObserved(
                _workerNodeId,
                string.IsNullOrWhiteSpace(observation.Key) ? DiscoveryTargetLabel : observation.Key,
                $"{NormalizeSource(source)}_ignored",
                "unknown_key");
            return;
        }

        switch (observation.Kind)
        {
            case ServiceEndpointObservationKind.Upserted:
                if (observation.Registration is ServiceEndpointRegistration registration)
                {
                    ApplyEndpoint(registration, source);
                    return;
                }

                WorkerNodeTelemetry.RecordDiscoveryEndpointObserved(
                    _workerNodeId,
                    observation.Key,
                    $"{NormalizeSource(source)}_ignored",
                    "registration_missing");
                return;
            case ServiceEndpointObservationKind.Removed:
                RemoveEndpoint(
                    observation.Key,
                    source,
                    outcome: "removed",
                    failureReason: NormalizeFailureReason(observation.FailureReason, "endpoint_removed"));
                return;
            case ServiceEndpointObservationKind.Invalid:
                RemoveEndpoint(
                    observation.Key,
                    source,
                    outcome: "invalidated",
                    failureReason: NormalizeFailureReason(observation.FailureReason, "endpoint_parse_failed"));
                return;
            default:
                WorkerNodeTelemetry.RecordDiscoveryEndpointObserved(
                    _workerNodeId,
                    observation.Key,
                    $"{NormalizeSource(source)}_ignored",
                    "unknown_update_kind");
                return;
        }
    }

    private void ApplyEndpoint(ServiceEndpointRegistration registration, string source)
    {
        if (!ServiceEndpointSettings.IsKnownKey(registration.Key))
        {
            WorkerNodeTelemetry.RecordDiscoveryEndpointObserved(
                _workerNodeId,
                string.IsNullOrWhiteSpace(registration.Key) ? DiscoveryTargetLabel : registration.Key,
                $"{NormalizeSource(source)}_ignored",
                "unknown_key");
            return;
        }

        var existed = _endpoints.ContainsKey(registration.Key);
        _endpoints[registration.Key] = registration;
        WorkerNodeTelemetry.RecordDiscoveryEndpointObserved(
            _workerNodeId,
            registration.Key,
            existed ? $"{NormalizeSource(source)}_updated" : $"{NormalizeSource(source)}_registered",
            "none");
    }

    private void RemoveEndpoint(string key, string source, string outcome, string failureReason)
    {
        if (!ServiceEndpointSettings.IsKnownKey(key))
        {
            WorkerNodeTelemetry.RecordDiscoveryEndpointObserved(
                _workerNodeId,
                string.IsNullOrWhiteSpace(key) ? DiscoveryTargetLabel : key,
                $"{NormalizeSource(source)}_ignored",
                "unknown_key");
            return;
        }

        _endpoints.Remove(key);
        WorkerNodeTelemetry.RecordDiscoveryEndpointObserved(
            _workerNodeId,
            key,
            $"{NormalizeSource(source)}_{outcome}",
            NormalizeFailureReason(failureReason, "none"));
    }
}
