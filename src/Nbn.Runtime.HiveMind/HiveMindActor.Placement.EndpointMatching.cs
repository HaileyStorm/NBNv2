using System.Globalization;
using System.Net;
using System.Reflection;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private async Task ApplyObservedControlAssignmentsAsync(IContext context, BrainState brain, PlacementExecutionState execution)
    {
        var routingUpdated = false;
        var ioUpdated = false;
        if (TryGetObservedControlAssignment(execution, ProtoControl.PlacementAssignmentTarget.PlacementTargetBrainRoot, out var observedRootReference))
        {
            var observedRoot = await ResolvePidAsync(observedRootReference).ConfigureAwait(false);
            if (observedRoot is not null
                && (!PidEquals(brain.BrainRootPid, observedRoot)
                    || !string.Equals(brain.BrainRootActorReference, observedRootReference, StringComparison.Ordinal)))
            {
                brain.BrainRootPid = NormalizePid(context, observedRoot) ?? observedRoot;
                brain.BrainRootActorReference = ResolveActorReference(observedRootReference, brain.BrainRootPid);
                routingUpdated = true;
            }
        }

        if (TryGetObservedControlAssignment(execution, ProtoControl.PlacementAssignmentTarget.PlacementTargetSignalRouter, out var observedRouterReference))
        {
            var observedRouter = await ResolvePidAsync(observedRouterReference).ConfigureAwait(false);
            if (observedRouter is not null
                && (!PidEquals(brain.SignalRouterPid, observedRouter)
                    || !string.Equals(brain.SignalRouterActorReference, observedRouterReference, StringComparison.Ordinal)))
            {
                brain.SignalRouterPid = NormalizePid(context, observedRouter) ?? observedRouter;
                brain.SignalRouterActorReference = ResolveActorReference(observedRouterReference, brain.SignalRouterPid);
                routingUpdated = true;
            }
        }

        if (TryGetObservedControlAssignment(execution, ProtoControl.PlacementAssignmentTarget.PlacementTargetInputCoordinator, out var observedInputCoordinatorReference))
        {
            var observedInputCoordinator = await ResolvePidAsync(observedInputCoordinatorReference).ConfigureAwait(false);
            if (observedInputCoordinator is not null
                && (!PidEquals(brain.InputCoordinatorPid, observedInputCoordinator)
                    || !string.Equals(brain.InputCoordinatorActorReference, observedInputCoordinatorReference, StringComparison.Ordinal)))
            {
                brain.InputCoordinatorPid = NormalizePid(context, observedInputCoordinator) ?? observedInputCoordinator;
                brain.InputCoordinatorActorReference = ResolveActorReference(observedInputCoordinatorReference, brain.InputCoordinatorPid);
                ioUpdated = true;
            }
        }

        if (TryGetObservedControlAssignment(execution, ProtoControl.PlacementAssignmentTarget.PlacementTargetOutputCoordinator, out var observedOutputCoordinatorReference))
        {
            var observedOutputCoordinator = await ResolvePidAsync(observedOutputCoordinatorReference).ConfigureAwait(false);
            if (observedOutputCoordinator is not null
                && (!PidEquals(brain.OutputCoordinatorPid, observedOutputCoordinator)
                    || !string.Equals(brain.OutputCoordinatorActorReference, observedOutputCoordinatorReference, StringComparison.Ordinal)))
            {
                brain.OutputCoordinatorPid = NormalizePid(context, observedOutputCoordinator) ?? observedOutputCoordinator;
                brain.OutputCoordinatorActorReference = ResolveActorReference(observedOutputCoordinatorReference, brain.OutputCoordinatorPid);
                ioUpdated = true;
            }
        }

        if (brain.SignalRouterPid is not null && string.IsNullOrWhiteSpace(brain.SignalRouterPid.Address))
        {
            var fallbackAddress = brain.BrainRootPid?.Address;
            if (!string.IsNullOrWhiteSpace(fallbackAddress))
            {
                brain.SignalRouterPid = new PID(fallbackAddress, brain.SignalRouterPid.Id);
                routingUpdated = true;
            }
        }

        if (brain.BrainRootPid is not null && string.IsNullOrWhiteSpace(brain.BrainRootPid.Address))
        {
            var fallbackAddress = brain.SignalRouterPid?.Address;
            if (!string.IsNullOrWhiteSpace(fallbackAddress))
            {
                brain.BrainRootPid = new PID(fallbackAddress, brain.BrainRootPid.Id);
                routingUpdated = true;
            }
        }

        var coordinatorFallbackAddress = brain.SignalRouterPid?.Address;
        if (string.IsNullOrWhiteSpace(coordinatorFallbackAddress))
        {
            coordinatorFallbackAddress = brain.BrainRootPid?.Address;
        }

        if (brain.InputCoordinatorPid is not null
            && string.IsNullOrWhiteSpace(brain.InputCoordinatorPid.Address)
            && !string.IsNullOrWhiteSpace(coordinatorFallbackAddress))
        {
            brain.InputCoordinatorPid = new PID(coordinatorFallbackAddress, brain.InputCoordinatorPid.Id);
            ioUpdated = true;
        }

        if (brain.OutputCoordinatorPid is not null
            && string.IsNullOrWhiteSpace(brain.OutputCoordinatorPid.Address)
            && !string.IsNullOrWhiteSpace(coordinatorFallbackAddress))
        {
            brain.OutputCoordinatorPid = new PID(coordinatorFallbackAddress, brain.OutputCoordinatorPid.Id);
            ioUpdated = true;
        }

        if (!routingUpdated && !ioUpdated)
        {
            return;
        }

        if (routingUpdated)
        {
            UpdateRoutingTable(context, brain);
            ReportBrainRegistration(context, brain);
        }

        if (ioUpdated)
        {
            RegisterBrainWithIo(context, brain, force: true);
        }
    }

    private static bool TryGetObservedControlAssignment(
        PlacementExecutionState execution,
        ProtoControl.PlacementAssignmentTarget target,
        out string actorReference)
    {
        foreach (var observed in execution.ObservedAssignments.Values.OrderBy(static value => value.AssignmentId, StringComparer.Ordinal))
        {
            if (observed.Target != target || string.IsNullOrWhiteSpace(observed.ActorPid))
            {
                continue;
            }

            actorReference = observed.ActorPid;
            return true;
        }

        actorReference = string.Empty;
        return false;
    }

    private static bool PidEquals(PID? left, PID right)
        => left is not null
           && string.Equals(left.Address ?? string.Empty, right.Address ?? string.Empty, StringComparison.OrdinalIgnoreCase)
           && string.Equals(left.Id ?? string.Empty, right.Id ?? string.Empty, StringComparison.Ordinal);

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

    private static bool SenderMatchesActorReference(PID? sender, string? actorReference)
    {
        if (sender is null || string.IsNullOrWhiteSpace(actorReference))
        {
            return false;
        }

        if (RoutablePidReference.TryDecode(actorReference, out var endpointSet))
        {
            foreach (var candidate in endpointSet.Candidates)
            {
                if (SenderMatchesPid(sender, candidate.ToEndpoint().ToPid()))
                {
                    return true;
                }
            }

            return false;
        }

        return RoutablePidReference.TryParsePlainPid(actorReference, out var parsed)
               && SenderMatchesPid(sender, parsed);
    }

    private static bool SenderMatchesActorReferenceOrPid(PID? sender, string? actorReference, PID? pid)
    {
        if (pid is not null && SenderMatchesPid(sender, pid))
        {
            return true;
        }

        return SenderMatchesActorReference(sender, actorReference);
    }

    private static bool PidHasEquivalentEndpoint(PID sender, PID expected)
    {
        if (!ActorIdsEquivalent(sender.Id, expected.Id))
        {
            return false;
        }

        if (HasEquivalentProcessLocalEndpoint(sender.Address, expected.Address)
            || HasEquivalentProcessLocalEndpoint(expected.Address, sender.Address))
        {
            return true;
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

    private static bool HasEquivalentProcessLocalEndpoint(string? address, string? otherAddress)
    {
        if (!string.Equals(address, "nonhost", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return TryParseEndpoint(otherAddress, out var otherHost, out _)
               && NetworkAddressDefaults.IsLocalHost(otherHost);
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

    private static string NormalizeEndpointAddress(string? address)
    {
        if (string.IsNullOrWhiteSpace(address))
        {
            return string.Empty;
        }

        if (TryParseEndpoint(address, out var host, out var port))
        {
            return $"{host.Trim().ToLowerInvariant()}:{port.ToString(CultureInfo.InvariantCulture)}";
        }

        return address.Trim().ToLowerInvariant();
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

    private static bool IsLikelyLocalSubscriberPid(ActorSystem system, PID? pid)
    {
        if (pid is null)
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(pid.Address))
        {
            return true;
        }

        if (string.IsNullOrWhiteSpace(system.Address))
        {
            return false;
        }

        if (string.Equals(pid.Address, system.Address, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var probeSender = new PID(pid.Address, "probe");
        var probeExpected = new PID(system.Address, "probe");
        return PidHasEquivalentEndpoint(probeSender, probeExpected);
    }

    private static bool TryLookupProcessInRegistry(ActorSystem system, PID pid, out object? process)
    {
        process = null;
        if (ProcessRegistryProperty is null || ProcessRegistryLookupMethod is null)
        {
            return false;
        }

        try
        {
            var registry = ProcessRegistryProperty.GetValue(system);
            if (registry is null)
            {
                return false;
            }

            process = ProcessRegistryLookupMethod.Invoke(registry, new object?[] { pid });
            return true;
        }
        catch
        {
            process = null;
            return false;
        }
    }

    private static bool ActorIdsEquivalent(string? left, string? right)
    {
        var normalizedLeft = NormalizeActorId(left);
        var normalizedRight = NormalizeActorId(right);
        if (string.Equals(normalizedLeft, normalizedRight, StringComparison.Ordinal))
        {
            return true;
        }

        if (normalizedLeft.Length == 0 || normalizedRight.Length == 0)
        {
            return false;
        }

        return normalizedLeft.EndsWith("/" + normalizedRight, StringComparison.Ordinal)
               || normalizedRight.EndsWith("/" + normalizedLeft, StringComparison.Ordinal);
    }

    private static string NormalizeActorId(string? actorId)
    {
        if (string.IsNullOrWhiteSpace(actorId))
        {
            return string.Empty;
        }

        return string.Join(
            "/",
            actorId
                .Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Where(static segment => !segment.StartsWith("$", StringComparison.Ordinal)));
    }

    private static MethodInfo? ResolveProcessRegistryLookupMethod()
    {
        var registryType = ProcessRegistryProperty?.PropertyType;
        if (registryType is null)
        {
            return null;
        }

        return registryType
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .FirstOrDefault(static method =>
            {
                var parameters = method.GetParameters();
                return parameters.Length == 1
                       && parameters[0].ParameterType == typeof(PID)
                       && method.ReturnType != typeof(void)
                       && method.ReturnType != typeof(bool)
                       && !method.ReturnType.IsByRef;
            });
    }

    private static string ToPlacementTargetLabel(ProtoControl.PlacementAssignmentTarget target)
        => target switch
        {
            ProtoControl.PlacementAssignmentTarget.PlacementTargetBrainRoot => "brain_root",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetSignalRouter => "signal_router",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetInputCoordinator => "input_coordinator",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetOutputCoordinator => "output_coordinator",
            ProtoControl.PlacementAssignmentTarget.PlacementTargetRegionShard => "region_shard",
            _ => "unknown"
        };

    private static string ToAssignmentStateLabel(ProtoControl.PlacementAssignmentState state)
        => state switch
        {
            ProtoControl.PlacementAssignmentState.PlacementAssignmentPending => "pending",
            ProtoControl.PlacementAssignmentState.PlacementAssignmentAccepted => "accepted",
            ProtoControl.PlacementAssignmentState.PlacementAssignmentReady => "ready",
            ProtoControl.PlacementAssignmentState.PlacementAssignmentDraining => "draining",
            ProtoControl.PlacementAssignmentState.PlacementAssignmentFailed => "failed",
            _ => "unknown"
        };

    private static string ToFailureReasonLabel(ProtoControl.PlacementFailureReason reason)
        => reason switch
        {
            ProtoControl.PlacementFailureReason.PlacementFailureNone => "none",
            ProtoControl.PlacementFailureReason.PlacementFailureInvalidBrain => "invalid_brain",
            ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable => "worker_unavailable",
            ProtoControl.PlacementFailureReason.PlacementFailureAssignmentRejected => "assignment_rejected",
            ProtoControl.PlacementFailureReason.PlacementFailureAssignmentTimeout => "assignment_timeout",
            ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch => "reconcile_mismatch",
            ProtoControl.PlacementFailureReason.PlacementFailureInternalError => "internal_error",
            _ => "unknown"
        };

    private static string ToSpawnFailureReasonCode(ProtoControl.PlacementFailureReason reason)
        => reason switch
        {
            ProtoControl.PlacementFailureReason.PlacementFailureWorkerUnavailable => "spawn_worker_unavailable",
            ProtoControl.PlacementFailureReason.PlacementFailureAssignmentRejected => "spawn_assignment_rejected",
            ProtoControl.PlacementFailureReason.PlacementFailureAssignmentTimeout => "spawn_assignment_timeout",
            ProtoControl.PlacementFailureReason.PlacementFailureReconcileMismatch => "spawn_reconcile_mismatch",
            ProtoControl.PlacementFailureReason.PlacementFailureInternalError => "spawn_internal_error",
            ProtoControl.PlacementFailureReason.PlacementFailureInvalidBrain => "spawn_invalid_request",
            _ => "spawn_failed"
        };

    private static string BuildSpawnFailureMessage(
        ProtoControl.PlacementFailureReason reason,
        string? detail,
        string? fallbackReasonCode = null)
    {
        if (!string.IsNullOrWhiteSpace(detail))
        {
            return detail.Trim();
        }

        var reasonCode = string.IsNullOrWhiteSpace(fallbackReasonCode)
            ? ToSpawnFailureReasonCode(reason)
            : fallbackReasonCode.Trim();

        return reasonCode switch
        {
            "spawn_invalid_request" => "Spawn request rejected: invalid brain definition request.",
            "spawn_timeout" => "Spawn timed out while waiting for placement completion.",
            "spawn_worker_unavailable" => "Spawn failed: no eligible worker was available for the placement plan.",
            "spawn_assignment_rejected" => "Spawn failed: a worker rejected one or more placement assignments.",
            "spawn_assignment_timeout" => "Spawn failed: placement assignment acknowledgements timed out and retry budget was exhausted.",
            "spawn_reconcile_timeout" => "Spawn failed: placement reconcile timed out before workers reported final assignments.",
            "spawn_reconcile_mismatch" => "Spawn failed: reconcile results did not match planned assignments.",
            "spawn_internal_error" => "Spawn failed: an internal placement error occurred.",
            _ => "Spawn failed before placement completed."
        };
    }

    private static void SetSpawnFailureDetails(BrainState brain, string reasonCode, string failureMessage)
    {
        var normalizedReasonCode = string.IsNullOrWhiteSpace(reasonCode)
            ? "spawn_failed"
            : reasonCode.Trim();
        var normalizedFailureMessage = string.IsNullOrWhiteSpace(failureMessage)
            ? BuildSpawnFailureMessage(
                ProtoControl.PlacementFailureReason.PlacementFailureNone,
                detail: null,
                fallbackReasonCode: normalizedReasonCode)
            : failureMessage.Trim();
        brain.SpawnFailureReasonCode = normalizedReasonCode;
        brain.SpawnFailureMessage = normalizedFailureMessage;
    }

    private static ProtoControl.SpawnBrainAck BuildSpawnFailureAck(string? reasonCode, string? failureMessage)
    {
        var normalizedReasonCode = string.IsNullOrWhiteSpace(reasonCode)
            ? "spawn_failed"
            : reasonCode.Trim();
        var normalizedFailureMessage = string.IsNullOrWhiteSpace(failureMessage)
            ? BuildSpawnFailureMessage(
                ProtoControl.PlacementFailureReason.PlacementFailureNone,
                detail: null,
                fallbackReasonCode: normalizedReasonCode)
            : failureMessage.Trim();
        return new ProtoControl.SpawnBrainAck
        {
            BrainId = Guid.Empty.ToProtoUuid(),
            FailureReasonCode = normalizedReasonCode,
            FailureMessage = normalizedFailureMessage
        };
    }
}
