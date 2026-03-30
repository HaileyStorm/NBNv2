using System.Globalization;
using System.Net;
using System.Reflection;
using System.Text;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private void ApplyObservedControlAssignments(IContext context, BrainState brain, PlacementExecutionState execution)
    {
        var routingUpdated = false;
        var ioUpdated = false;
        if (TryGetObservedControlPid(execution, ProtoControl.PlacementAssignmentTarget.PlacementTargetBrainRoot, out var observedRoot)
            && !PidEquals(brain.BrainRootPid, observedRoot))
        {
            brain.BrainRootPid = NormalizePid(context, observedRoot) ?? observedRoot;
            routingUpdated = true;
        }

        if (TryGetObservedControlPid(execution, ProtoControl.PlacementAssignmentTarget.PlacementTargetSignalRouter, out var observedRouter)
            && !PidEquals(brain.SignalRouterPid, observedRouter))
        {
            brain.SignalRouterPid = NormalizePid(context, observedRouter) ?? observedRouter;
            routingUpdated = true;
        }

        if (TryGetObservedControlPid(execution, ProtoControl.PlacementAssignmentTarget.PlacementTargetInputCoordinator, out var observedInputCoordinator)
            && !PidEquals(brain.InputCoordinatorPid, observedInputCoordinator))
        {
            brain.InputCoordinatorPid = NormalizePid(context, observedInputCoordinator) ?? observedInputCoordinator;
            ioUpdated = true;
        }

        if (TryGetObservedControlPid(execution, ProtoControl.PlacementAssignmentTarget.PlacementTargetOutputCoordinator, out var observedOutputCoordinator)
            && !PidEquals(brain.OutputCoordinatorPid, observedOutputCoordinator))
        {
            brain.OutputCoordinatorPid = NormalizePid(context, observedOutputCoordinator) ?? observedOutputCoordinator;
            ioUpdated = true;
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

    private static bool TryGetObservedControlPid(
        PlacementExecutionState execution,
        ProtoControl.PlacementAssignmentTarget target,
        out PID pid)
    {
        foreach (var observed in execution.ObservedAssignments.Values.OrderBy(static value => value.AssignmentId, StringComparer.Ordinal))
        {
            if (observed.Target != target || !TryParsePid(observed.ActorPid, out var observedPid))
            {
                continue;
            }

            pid = observedPid;
            return true;
        }

        pid = new PID();
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
               || PidHasEquivalentActorIdentity(sender, expected)
               || PidHasEquivalentEndpoint(sender, expected);
    }

    private static bool PidHasEquivalentActorIdentity(PID sender, PID expected)
    {
        if (!string.Equals(sender.Id ?? string.Empty, expected.Id ?? string.Empty, StringComparison.Ordinal))
        {
            return false;
        }

        return IsUnspecifiedActorAddress(sender.Address)
               || IsUnspecifiedActorAddress(expected.Address);
    }

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

    private static bool IsUnspecifiedActorAddress(string? address)
        => string.IsNullOrWhiteSpace(address)
           || string.Equals(address.Trim(), "nonhost", StringComparison.OrdinalIgnoreCase);

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

    private static ProtoControl.SpawnBrainAck BuildSpawnAck(
        Guid brainId,
        ulong placementEpoch,
        bool acceptedForPlacement,
        bool placementReady,
        ProtoControl.PlacementLifecycleState lifecycleState,
        ProtoControl.PlacementReconcileState reconcileState,
        string? reasonCode = null,
        string? failureMessage = null)
    {
        var normalizedReasonCode = string.IsNullOrWhiteSpace(reasonCode) ? string.Empty : reasonCode.Trim();
        var normalizedFailureMessage = string.IsNullOrWhiteSpace(normalizedReasonCode)
            ? string.Empty
            : string.IsNullOrWhiteSpace(failureMessage)
                ? BuildSpawnFailureMessage(
                    ProtoControl.PlacementFailureReason.PlacementFailureNone,
                    detail: null,
                    fallbackReasonCode: normalizedReasonCode)
                : failureMessage.Trim();
        return new ProtoControl.SpawnBrainAck
        {
            BrainId = brainId.ToProtoUuid(),
            FailureReasonCode = normalizedReasonCode,
            FailureMessage = normalizedFailureMessage,
            AcceptedForPlacement = acceptedForPlacement,
            PlacementReady = placementReady,
            PlacementEpoch = placementEpoch,
            LifecycleState = lifecycleState,
            ReconcileState = reconcileState
        };
    }

    private static ProtoControl.SpawnBrainAck BuildSpawnQueuedAck(BrainState brain)
        => BuildSpawnAck(
            brain.BrainId,
            brain.PlacementEpoch,
            acceptedForPlacement: true,
            placementReady: false,
            brain.PlacementLifecycleState,
            brain.PlacementReconcileState);

    private ProtoControl.SpawnBrainAck BuildCurrentSpawnAck(Guid brainId)
    {
        if (_completedSpawns.TryGetValue(brainId, out var completed))
        {
            _completedSpawns.Remove(brainId);
            return string.IsNullOrWhiteSpace(completed.FailureReasonCode)
                ? BuildSpawnAck(
                    completed.BrainId,
                    completed.PlacementEpoch,
                    completed.AcceptedForPlacement,
                    completed.PlacementReady,
                    completed.LifecycleState,
                    completed.ReconcileState)
                : BuildSpawnFailureAck(
                    completed.BrainId,
                    completed.PlacementEpoch,
                    completed.AcceptedForPlacement,
                    completed.LifecycleState,
                    completed.ReconcileState,
                    completed.FailureReasonCode,
                    completed.FailureMessage);
        }

        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return BuildSpawnFailureAck(
                reasonCode: "spawn_not_found",
                failureMessage: $"Spawn wait failed: brain {brainId} is no longer tracked.");
        }

        var executionCompleted = brain.PlacementExecution is null || brain.PlacementExecution.Completed;
        var placementReady = executionCompleted
                             && (brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned
                                 || brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning);
        if (placementReady)
        {
            return BuildSpawnAck(
                brain.BrainId,
                brain.PlacementEpoch,
                acceptedForPlacement: true,
                placementReady: true,
                brain.PlacementLifecycleState,
                brain.PlacementReconcileState);
        }

        if (brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed
            || brain.PlacementLifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleTerminated)
        {
            return BuildSpawnFailureAck(
                brain.BrainId,
                brain.PlacementEpoch,
                acceptedForPlacement: true,
                brain.PlacementLifecycleState,
                brain.PlacementReconcileState,
                string.IsNullOrWhiteSpace(brain.SpawnFailureReasonCode)
                    ? ToSpawnFailureReasonCode(brain.PlacementFailureReason)
                    : brain.SpawnFailureReasonCode,
                string.IsNullOrWhiteSpace(brain.SpawnFailureMessage)
                    ? BuildSpawnFailureMessage(brain.PlacementFailureReason, detail: null)
                    : brain.SpawnFailureMessage);
        }

        return BuildSpawnAck(
            brain.BrainId,
            brain.PlacementEpoch,
            acceptedForPlacement: true,
            placementReady: false,
            brain.PlacementLifecycleState,
            brain.PlacementReconcileState);
    }

    private ProtoControl.SpawnBrainAck BuildAwaitedSpawnAck(Guid brainId, PendingSpawnState pending, PendingSpawnAwaitResult result)
    {
        if (result.TimedOut)
        {
            if (_brains.TryGetValue(brainId, out var inFlightBrain))
            {
                LogError($"Spawn wait timeout for brain {brainId} epoch={pending.PlacementEpoch}: {DescribeSpawnWaitTimeoutState(inFlightBrain, pending)}");
                return BuildSpawnFailureAck(
                    brainId,
                    pending.PlacementEpoch,
                    acceptedForPlacement: true,
                    inFlightBrain.PlacementLifecycleState,
                    inFlightBrain.PlacementReconcileState,
                    reasonCode: "spawn_wait_timeout",
                    failureMessage: "Spawn wait timed out before placement became ready.");
            }

            LogError($"Spawn wait timeout for brain {brainId} epoch={pending.PlacementEpoch}: brain_not_tracked pendingFailure={pending.FailureReasonCode}");
            return BuildSpawnFailureAck(
                brainId,
                pending.PlacementEpoch,
                acceptedForPlacement: true,
                lifecycleState: ProtoControl.PlacementLifecycleState.PlacementLifecycleUnknown,
                reconcileState: ProtoControl.PlacementReconcileState.PlacementReconcileUnknown,
                reasonCode: "spawn_wait_timeout",
                failureMessage: "Spawn wait timed out before placement became ready.");
        }

        if (result.Completed)
        {
            return BuildCurrentSpawnAck(brainId);
        }

        if (_brains.TryGetValue(brainId, out var failedBrain))
        {
            return BuildSpawnFailureAck(
                brainId,
                pending.PlacementEpoch,
                acceptedForPlacement: true,
                failedBrain.PlacementLifecycleState,
                failedBrain.PlacementReconcileState,
                pending.FailureReasonCode,
                pending.FailureMessage);
        }

        return BuildSpawnFailureAck(
            brainId,
            pending.PlacementEpoch,
            acceptedForPlacement: true,
            lifecycleState: ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
            reconcileState: ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
            reasonCode: pending.FailureReasonCode,
            failureMessage: pending.FailureMessage);
    }

    private string DescribeSpawnWaitTimeoutState(BrainState brain, PendingSpawnState pending)
    {
        var builder = new StringBuilder();
        builder.Append($"lifecycle={brain.PlacementLifecycleState} reconcile={brain.PlacementReconcileState}");
        builder.Append($" placementEpoch={brain.PlacementEpoch} pendingEpoch={pending.PlacementEpoch}");
        builder.Append($" registeredShards={brain.Shards.Count}");
        builder.Append($" failure={pending.FailureReasonCode}");

        var execution = brain.PlacementExecution;
        if (execution is null)
        {
            builder.Append(" execution=none");
            return builder.ToString();
        }

        var readyAssignments = 0;
        var awaitingAssignments = 0;
        var failedAssignments = 0;
        var acceptedAssignments = 0;
        foreach (var assignment in execution.Assignments.Values)
        {
            if (assignment.Ready)
            {
                readyAssignments++;
            }

            if (assignment.AwaitingAck)
            {
                awaitingAssignments++;
            }

            if (assignment.Failed)
            {
                failedAssignments++;
            }

            if (assignment.Accepted)
            {
                acceptedAssignments++;
            }
        }

        builder.Append(
            $" executionCompleted={execution.Completed} assignments={execution.Assignments.Count} ready={readyAssignments} accepted={acceptedAssignments} awaitingAck={awaitingAssignments} failed={failedAssignments} reconcileRequested={execution.ReconcileRequested} pendingReconcileWorkers={execution.PendingReconcileWorkers.Count} observedAssignments={execution.ObservedAssignments.Count}");

        if (execution.WorkerTargets.Count == 0)
        {
            builder.Append(" workerDispatch=none");
            return builder.ToString();
        }

        builder.Append(" workerDispatch=[");
        var first = true;
        foreach (var workerTarget in execution.WorkerTargets.OrderBy(static entry => entry.Key))
        {
            if (!first)
            {
                builder.Append("; ");
            }

            first = false;
            builder.Append(workerTarget.Key.ToString("D"));
            builder.Append(":");
            builder.Append(_workerPlacementDispatches.TryGetValue(workerTarget.Key, out var dispatch)
                ? DescribeWorkerPlacementDispatchState(dispatch)
                : "idle");
        }

        builder.Append(']');
        return builder.ToString();
    }

    private static string DescribeWorkerPlacementDispatchState(WorkerPlacementDispatchState dispatch)
    {
        if (dispatch.Active is null && dispatch.Pending.Count == 0)
        {
            return "idle";
        }

        var builder = new StringBuilder();
        if (dispatch.Active is { } active)
        {
            builder.Append($"active={active.BrainId:D}@{active.PlacementEpoch}/remaining={active.RemainingAssignmentCount}");
        }
        else
        {
            builder.Append("active=none");
        }

        builder.Append($",pendingBatches={dispatch.Pending.Count}");
        if (dispatch.Pending.Count > 0)
        {
            var next = dispatch.Pending.Peek();
            builder.Append($",next={next.BrainId:D}@{next.PlacementEpoch}/assignments={next.Assignments.Count}");
        }

        return builder.ToString();
    }

    private void RememberCompletedSpawn(CompletedSpawnState completed)
    {
        if (!_completedSpawns.ContainsKey(completed.BrainId))
        {
            _completedSpawnOrder.Enqueue(completed.BrainId);
        }

        _completedSpawns[completed.BrainId] = completed;

        while (_completedSpawnOrder.Count > MaxCompletedSpawnResults)
        {
            var evictedBrainId = _completedSpawnOrder.Dequeue();
            _completedSpawns.Remove(evictedBrainId);
        }
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
        => BuildSpawnFailureAck(
            Guid.Empty,
            placementEpoch: 0,
            acceptedForPlacement: false,
            lifecycleState: ProtoControl.PlacementLifecycleState.PlacementLifecycleFailed,
            reconcileState: ProtoControl.PlacementReconcileState.PlacementReconcileFailed,
            reasonCode,
            failureMessage);

    private static ProtoControl.SpawnBrainAck BuildSpawnFailureAck(
        Guid brainId,
        ulong placementEpoch,
        bool acceptedForPlacement,
        ProtoControl.PlacementLifecycleState lifecycleState,
        ProtoControl.PlacementReconcileState reconcileState,
        string? reasonCode,
        string? failureMessage)
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
        return BuildSpawnAck(
            brainId,
            placementEpoch,
            acceptedForPlacement,
            placementReady: false,
            lifecycleState,
            reconcileState,
            normalizedReasonCode,
            normalizedFailureMessage);
    }
}
