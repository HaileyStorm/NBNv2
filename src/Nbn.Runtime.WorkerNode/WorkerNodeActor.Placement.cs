using System.Diagnostics;
using Google.Protobuf;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Shared;
using Proto;
using SharedShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Runtime.WorkerNode;

public sealed partial class WorkerNodeActor
{
    private async Task HandlePlacementAssignmentAsync(IContext context, PlacementAssignmentRequest request)
    {
        if (!TryValidatePlacementRequest(request.Assignment, out var validated, out var validationFailure))
        {
            RespondFailedPlacement(
                context,
                FailedAck(
                    validationFailure.AssignmentId,
                    validationFailure.BrainId,
                    validationFailure.PlacementEpoch,
                    validationFailure.FailureReason,
                    validationFailure.Message),
                validationFailure.Target);
            return;
        }

        var assignment = validated.Assignment;
        if (!TryValidateAssignmentTargetRole(context, assignment))
        {
            return;
        }

        MaybeCaptureHiveMindHint(context.Sender);

        var brain = GetOrCreateBrainState(validated.BrainId);
        if (!TryPrepareBrainForAssignment(context, brain, assignment)
            || !TryReuseOrRejectExistingAssignment(context, assignment)
            || !await EnsureAssignmentMetadataAsync(context, brain, assignment).ConfigureAwait(false))
        {
            return;
        }

        var hostingStarted = Stopwatch.GetTimestamp();
        var hosted = await HostAssignmentAsync(context, brain, assignment.Clone()).ConfigureAwait(false);
        var hostingMs = Stopwatch.GetElapsedTime(hostingStarted).TotalMilliseconds;
        if (!hosted.Success)
        {
            RespondFailedPlacement(context, hosted.FailedAck!, assignment.Target);
            return;
        }

        RegisterHostedAssignment(context, brain, hosted);
        RespondReadyPlacement(context, hosted.Assignment!, "ready", hostingMs);
    }

    private void HandlePlacementUnassignment(IContext context, PlacementUnassignmentRequest request)
    {
        if (!TryValidatePlacementRequest(request.Assignment, out var validated, out var validationFailure))
        {
            ReplyToSender(
                context,
                FailedUnassignmentAck(
                    validationFailure.AssignmentId,
                    validationFailure.BrainId,
                    validationFailure.PlacementEpoch,
                    validationFailure.FailureReason,
                    validationFailure.Message));
            return;
        }

        var assignment = validated.Assignment;

        MaybeCaptureHiveMindHint(context.Sender);

        if (!_assignments.TryGetValue(assignment.AssignmentId, out var hostedState))
        {
            ReplyToSender(context, BuildUnassignmentAck(assignment, accepted: true, "already_unassigned"));
            return;
        }

        if (!AssignmentSemanticallyMatches(hostedState.Assignment, assignment))
        {
            ReplyToSender(context, FailedUnassignmentAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureAssignmentRejected,
                "assignment_id conflicts with an existing hosted assignment"));
            return;
        }

        if (!_brains.TryGetValue(validated.BrainId, out var brain))
        {
            _assignments.Remove(assignment.AssignmentId);
            ReplyToSender(context, BuildUnassignmentAck(assignment, accepted: true, "already_unassigned"));
            return;
        }

        var outcome = UnassignHostedAssignment(context, brain, hostedState, notifyHiveMind: true)
            ? "unassigned"
            : "already_unassigned";
        ReplyToSender(context, BuildUnassignmentAck(assignment, accepted: true, outcome));
    }

    private bool UnassignHostedAssignment(
        IContext context,
        BrainHostingState brain,
        HostedAssignmentState hosted,
        bool notifyHiveMind)
    {
        var assignment = hosted.Assignment;
        var removed = _assignments.Remove(assignment.AssignmentId);
        removed |= brain.Assignments.Remove(assignment.AssignmentId);

        var hostedPid = hosted.HostedPid;
        if (hostedPid is not null)
        {
            context.Stop(hostedPid);
        }

        if (assignment.Target == PlacementAssignmentTarget.PlacementTargetBrainRoot
            && hostedPid is not null
            && PidEquals(brain.BrainRootPid, hostedPid))
        {
            brain.BrainRootPid = null;
            removed = true;
        }

        if (assignment.Target == PlacementAssignmentTarget.PlacementTargetSignalRouter
            && hostedPid is not null
            && PidEquals(brain.SignalRouterPid, hostedPid))
        {
            brain.SignalRouterPid = null;
            removed = true;
        }

        if (assignment.Target == PlacementAssignmentTarget.PlacementTargetInputCoordinator
            && hostedPid is not null
            && PidEquals(brain.InputCoordinatorPid, hostedPid))
        {
            brain.InputCoordinatorPid = null;
            removed = true;
        }

        if (assignment.Target == PlacementAssignmentTarget.PlacementTargetOutputCoordinator
            && hostedPid is not null
            && PidEquals(brain.OutputCoordinatorPid, hostedPid))
        {
            brain.OutputCoordinatorPid = null;
            removed = true;
        }

        if (assignment.Target == PlacementAssignmentTarget.PlacementTargetRegionShard
            && SharedShardId32.TryFrom(checked((int)assignment.RegionId), checked((int)assignment.ShardIndex), out var shardId))
        {
            if (brain.RegionShards.TryGetValue(shardId, out var shard)
                && (string.Equals(shard.AssignmentId, assignment.AssignmentId, StringComparison.Ordinal)
                    || (hostedPid is not null && PidEquals(shard.Pid, hostedPid))))
            {
                brain.RegionShards.Remove(shardId);
                removed = true;
            }

            if (notifyHiveMind)
            {
                UnregisterShard(context, brain, shardId);
            }
        }

        PublishHostedBrainState(context, brain, refreshCoordinatorWidths: false, allowClearOutputSink: true);
        return removed;
    }

    private bool TryValidatePlacementRequest(
        PlacementAssignment? assignment,
        out ValidatedPlacementRequest validated,
        out PlacementValidationFailure failure)
    {
        if (assignment is null)
        {
            validated = default;
            failure = new PlacementValidationFailure(
                AssignmentId: string.Empty,
                BrainId: null,
                PlacementEpoch: 0,
                FailureReason: PlacementFailureReason.PlacementFailureInternalError,
                Message: "assignment payload was empty",
                Target: PlacementAssignmentTarget.PlacementTargetUnknown);
            return false;
        }

        if (string.IsNullOrWhiteSpace(assignment.AssignmentId))
        {
            validated = default;
            failure = new PlacementValidationFailure(
                AssignmentId: string.Empty,
                BrainId: assignment.BrainId,
                PlacementEpoch: assignment.PlacementEpoch,
                FailureReason: PlacementFailureReason.PlacementFailureInternalError,
                Message: "assignment_id is required",
                Target: assignment.Target);
            return false;
        }

        if (assignment.BrainId is null || !assignment.BrainId.TryToGuid(out var brainId))
        {
            validated = default;
            failure = new PlacementValidationFailure(
                AssignmentId: assignment.AssignmentId,
                BrainId: assignment.BrainId,
                PlacementEpoch: assignment.PlacementEpoch,
                FailureReason: PlacementFailureReason.PlacementFailureInternalError,
                Message: "brain_id was invalid",
                Target: assignment.Target);
            return false;
        }

        if (!assignment.WorkerNodeId.TryToGuid(out var targetWorkerNodeId))
        {
            validated = default;
            failure = new PlacementValidationFailure(
                AssignmentId: assignment.AssignmentId,
                BrainId: assignment.BrainId,
                PlacementEpoch: assignment.PlacementEpoch,
                FailureReason: PlacementFailureReason.PlacementFailureInternalError,
                Message: "worker_node_id was invalid",
                Target: assignment.Target);
            return false;
        }

        if (targetWorkerNodeId != _workerNodeId)
        {
            validated = default;
            failure = new PlacementValidationFailure(
                AssignmentId: assignment.AssignmentId,
                BrainId: assignment.BrainId,
                PlacementEpoch: assignment.PlacementEpoch,
                FailureReason: PlacementFailureReason.PlacementFailureWorkerUnavailable,
                Message: $"assignment is for worker {targetWorkerNodeId}, not {_workerNodeId}",
                Target: assignment.Target);
            return false;
        }

        validated = new ValidatedPlacementRequest(assignment, brainId);
        failure = default;
        return true;
    }

    private bool TryValidateAssignmentTargetRole(IContext context, PlacementAssignment assignment)
    {
        if (IsTargetRoleEnabled(assignment.Target, out var requiredRole))
        {
            return true;
        }

        var roleToken = WorkerServiceRoles.ToRoleToken(requiredRole);
        var targetToken = ToPlacementTargetLabel(assignment.Target);
        RespondFailedPlacement(
            context,
            FailedAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureAssignmentRejected,
                $"assignment target '{targetToken}' requires service role '{roleToken}', but that role is disabled on worker {_workerNodeId}. Enable it with --service-role {roleToken} or --service-roles all."),
            assignment.Target);
        return false;
    }

    private bool TryPrepareBrainForAssignment(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        if (assignment.PlacementEpoch < brain.PlacementEpoch)
        {
            RespondFailedPlacement(
                context,
                FailedAck(
                    assignment.AssignmentId,
                    assignment.BrainId,
                    assignment.PlacementEpoch,
                    PlacementFailureReason.PlacementFailureInternalError,
                    $"assignment epoch {assignment.PlacementEpoch} is older than hosted epoch {brain.PlacementEpoch}"),
                assignment.Target);
            return false;
        }

        if (assignment.PlacementEpoch > brain.PlacementEpoch)
        {
            ResetBrainState(context, brain);
            brain.PlacementEpoch = assignment.PlacementEpoch;
        }
        else if (brain.PlacementEpoch == 0)
        {
            brain.PlacementEpoch = assignment.PlacementEpoch;
        }

        return true;
    }

    private bool TryReuseOrRejectExistingAssignment(IContext context, PlacementAssignment assignment)
    {
        if (!_assignments.TryGetValue(assignment.AssignmentId, out var existing))
        {
            return true;
        }

        var existingAssignment = existing.Assignment;
        if (existing.State == PlacementAssignmentState.PlacementAssignmentReady
            && AssignmentSemanticallyMatches(existingAssignment, assignment))
        {
            RespondReadyPlacement(context, existing.Assignment, "already_ready", hostingMs: 0);
            return false;
        }

        if (existing.State == PlacementAssignmentState.PlacementAssignmentReady)
        {
            RespondFailedPlacement(
                context,
                FailedAck(
                    assignment.AssignmentId,
                    assignment.BrainId,
                    assignment.PlacementEpoch,
                    PlacementFailureReason.PlacementFailureAssignmentRejected,
                    "assignment_id conflicts with an existing ready assignment"),
                assignment.Target);
            return false;
        }

        return true;
    }

    private async Task<bool> EnsureAssignmentMetadataAsync(
        IContext context,
        BrainHostingState brain,
        PlacementAssignment assignment)
    {
        if (assignment.Target != PlacementAssignmentTarget.PlacementTargetRegionShard)
        {
            return true;
        }

        var metadataReady = await EnsureRuntimeInfoAsync(context, brain, requireArtifacts: true).ConfigureAwait(false);
        var requesterIsEphemeral = IsEphemeralRequestSender(context.Sender);
        var shouldRequireArtifacts = HasKnownIoGatewayEndpoint() || HasHiveMindHint() || !requesterIsEphemeral;
        if (metadataReady || !shouldRequireArtifacts)
        {
            return true;
        }

        RespondArtifactMetadataUnavailable(context, brain, assignment);
        return false;
    }

    private void RespondArtifactMetadataUnavailable(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        var ioError = string.IsNullOrWhiteSpace(brain.RuntimeInfo?.LastIoError)
            ? "metadata_unavailable"
            : brain.RuntimeInfo!.LastIoError;

        if (LogRuntimeMetadataDiagnostics)
        {
            Console.WriteLine(
                $"[WorkerNode] Region shard placement deferred pending artifact metadata. brain={brain.BrainId} assignment={assignment.AssignmentId} region={assignment.RegionId} shard={assignment.ShardIndex} hasIoEndpoint={HasKnownIoGatewayEndpoint()} hasHiveHint={HasHiveMindHint()} ioError={ioError}");
        }

        RespondFailedPlacement(
            context,
            FailedAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureWorkerUnavailable,
                $"artifact metadata unavailable for brain {brain.BrainId}; retry when IO/Hive metadata is ready (detail={ioError})",
                retryable: true,
                retryAfterMs: (ulong)Math.Max(50, RuntimeMetadataRetryDelay.TotalMilliseconds)),
            assignment.Target);
    }

    private void RegisterHostedAssignment(IContext context, BrainHostingState brain, HostingResult hosted)
    {
        var assignment = hosted.Assignment!;
        var hostedState = new HostedAssignmentState(assignment, hosted.HostedPid)
        {
            State = PlacementAssignmentState.PlacementAssignmentReady
        };

        _assignments[assignment.AssignmentId] = hostedState;
        brain.Assignments[assignment.AssignmentId] = hostedState;
        PublishHostedBrainState(context, brain, refreshCoordinatorWidths: true, allowClearOutputSink: false);
    }

    private void PublishHostedBrainState(
        IContext context,
        BrainHostingState brain,
        bool refreshCoordinatorWidths,
        bool allowClearOutputSink)
    {
        UpdateRuntimeWidthsFromShards(brain);
        if (refreshCoordinatorWidths)
        {
            UpdateInputCoordinatorWidth(context, brain);
            UpdateOutputCoordinatorWidth(context, brain);
        }

        PushRouting(context, brain);
        PushShardEndpoints(context, brain);
        PushIoGatewayRegistration(context, brain);
        RegisterOutputSink(context, brain, allowClear: allowClearOutputSink);
    }

    private void HandlePlacementReconcile(IContext context, PlacementReconcileRequest request)
    {
        var requestHasBrainId = request.BrainId.TryToGuid(out var requestBrainId);
        WorkerNodeTelemetry.RecordPlacementReconcileRequested(
            _workerNodeId,
            requestHasBrainId ? requestBrainId : null,
            request.PlacementEpoch);

        var report = new PlacementReconcileReport
        {
            BrainId = request.BrainId,
            PlacementEpoch = request.PlacementEpoch,
            ReconcileState = PlacementReconcileState.PlacementReconcileMatched,
            FailureReason = PlacementFailureReason.PlacementFailureNone,
            Message = "matched"
        };

        foreach (var assignment in _assignments.Values
                     .Where(value => MatchesReconcileRequest(value, request))
                     .OrderBy(value => value.Assignment.AssignmentId, StringComparer.Ordinal))
        {
            report.Assignments.Add(new PlacementObservedAssignment
            {
                AssignmentId = assignment.Assignment.AssignmentId,
                Target = assignment.Assignment.Target,
                WorkerNodeId = assignment.Assignment.WorkerNodeId ?? _workerNodeId.ToProtoUuid(),
                RegionId = assignment.Assignment.RegionId,
                ShardIndex = assignment.Assignment.ShardIndex,
                ActorPid = BuildObservedActorPid(context, assignment)
            });
        }

        var failureReason = ResolveReconcileFailureReason(
            requestHasBrainId,
            requestHasBrainId ? requestBrainId : Guid.Empty,
            request.PlacementEpoch,
            report.Assignments.Count);
        WorkerNodeTelemetry.RecordPlacementReconcileReported(
            _workerNodeId,
            requestHasBrainId ? requestBrainId : null,
            request.PlacementEpoch,
            report.Assignments.Count,
            report.Assignments.Count > 0 ? "matched" : "empty",
            failureReason);

        ReplyToSender(context, report);
    }

    private void HandleWorkerCapabilityRefresh(IContext context, WorkerCapabilityRefreshRequest request)
    {
        _capabilityProfileChanged?.Invoke();
        ReplyToSender(context, new WorkerCapabilityRefreshAck
        {
            Accepted = true,
            RequestedMs = request.RequestedMs,
            Message = "scheduled_for_next_heartbeat"
        });
    }

    private async Task HandlePlacementPeerLatencyAsync(IContext context, PlacementPeerLatencyRequest request)
    {
        var response = new PlacementPeerLatencyResponse
        {
            WorkerNodeId = _workerNodeId.ToProtoUuid()
        };

        var peers = request.Peers
            .Where(static peer => peer is not null && !string.IsNullOrWhiteSpace(peer.WorkerRootActorName))
            .OrderBy(static peer => peer.WorkerAddress ?? string.Empty, StringComparer.Ordinal)
            .ThenBy(static peer => peer.WorkerRootActorName ?? string.Empty, StringComparer.Ordinal)
            .ToArray();
        if (peers.Length == 0)
        {
            ReplyToSender(context, response);
            return;
        }

        var timeoutMs = request.TimeoutMs > 0 ? (int)request.TimeoutMs : 250;
        double totalLatencyMs = 0;
        var sampleCount = 0;

        foreach (var peer in peers)
        {
            if (peer.WorkerNodeId is not null
                && peer.WorkerNodeId.TryToGuid(out var peerNodeId)
                && peerNodeId == _workerNodeId)
            {
                continue;
            }

            var target = ResolvePeerLatencyProbePid(peer);
            if (target is null)
            {
                continue;
            }

            try
            {
                var started = Stopwatch.GetTimestamp();
                var ack = await context.RequestAsync<PlacementLatencyEchoAck>(
                        target,
                        new PlacementLatencyEchoRequest(),
                        TimeSpan.FromMilliseconds(Math.Max(50, timeoutMs)))
                    .ConfigureAwait(false);
                if (ack is null)
                {
                    continue;
                }

                totalLatencyMs += Stopwatch.GetElapsedTime(started).TotalMilliseconds;
                sampleCount++;
            }
            catch
            {
            }
        }

        if (sampleCount > 0)
        {
            response.AveragePeerLatencyMs = (float)(totalLatencyMs / sampleCount);
            response.SampleCount = (uint)sampleCount;
        }

        ReplyToSender(context, response);
    }

    private WorkerNodeSnapshot BuildSnapshot()
    {
        var hiveMindEndpoint = _endpoints.TryGetValue(ServiceEndpointSettings.HiveMindKey, out var hiveValue)
            ? hiveValue
            : (ServiceEndpointRegistration?)null;
        var ioEndpoint = _endpoints.TryGetValue(ServiceEndpointSettings.IoGatewayKey, out var ioValue)
            ? ioValue
            : (ServiceEndpointRegistration?)null;
        return new WorkerNodeSnapshot(
            _workerNodeId,
            _workerAddress,
            hiveMindEndpoint,
            ioEndpoint,
            _enabledRoles,
            _assignments.Count,
            _resourceAvailability);
    }

    private void HandleTerminated(Terminated terminated)
    {
        foreach (var brain in _brains.Values)
        {
            if (brain.BrainRootPid is not null && terminated.Who.Equals(brain.BrainRootPid))
            {
                brain.BrainRootPid = null;
            }

            if (brain.SignalRouterPid is not null && terminated.Who.Equals(brain.SignalRouterPid))
            {
                brain.SignalRouterPid = null;
            }

            if (brain.InputCoordinatorPid is not null && terminated.Who.Equals(brain.InputCoordinatorPid))
            {
                brain.InputCoordinatorPid = null;
            }

            if (brain.OutputCoordinatorPid is not null && terminated.Who.Equals(brain.OutputCoordinatorPid))
            {
                brain.OutputCoordinatorPid = null;
            }

            foreach (var shard in brain.RegionShards.Where(entry => entry.Value.Pid.Equals(terminated.Who)).Select(entry => entry.Key).ToArray())
            {
                brain.RegionShards.Remove(shard);
            }

            foreach (var assignment in brain.Assignments.Values)
            {
                if (assignment.HostedPid is not null && terminated.Who.Equals(assignment.HostedPid))
                {
                    assignment.HostedPid = null;
                    assignment.State = PlacementAssignmentState.PlacementAssignmentFailed;
                }
            }
        }
    }

    private void RespondFailedPlacement(
        IContext context,
        PlacementAssignmentAck ack,
        PlacementAssignmentTarget target)
    {
        WorkerNodeTelemetry.RecordPlacementAssignmentHostedFailed(
            _workerNodeId,
            ack.BrainId.ToGuidOrEmpty(),
            ack.PlacementEpoch,
            ToPlacementTargetLabel(target),
            ToFailureReasonLabel(ack.FailureReason));
        ReplyToSender(context, ack);
    }

    private void RespondReadyPlacement(
        IContext context,
        PlacementAssignment assignment,
        string message,
        double hostingMs)
    {
        WorkerNodeTelemetry.RecordPlacementAssignmentHostedAccepted(
            _workerNodeId,
            assignment.BrainId.ToGuidOrEmpty(),
            assignment.PlacementEpoch,
            ToPlacementTargetLabel(assignment.Target),
            string.IsNullOrWhiteSpace(message) ? "ready" : message,
            hostingMs);
        ReplyToSender(context, BuildReadyAck(assignment, message));
    }

    private void ReplyToSender(IContext context, object message)
    {
        if (context.Sender is null)
        {
            return;
        }

        TryRequest(context, context.Sender, message);
    }

    private static void TryRequest(IContext context, PID target, object message)
    {
        if (string.IsNullOrWhiteSpace(target.Address))
        {
            var systemAddress = context.System.Address;
            if (!string.IsNullOrWhiteSpace(systemAddress))
            {
                try
                {
                    context.Request(new PID(systemAddress, target.Id), message);
                    return;
                }
                catch
                {
                }
            }
        }

        try
        {
            context.Request(target, message);
        }
        catch
        {
        }
    }

    private static string ToPlacementTargetLabel(PlacementAssignmentTarget target)
        => target switch
        {
            PlacementAssignmentTarget.PlacementTargetBrainRoot => "brain_root",
            PlacementAssignmentTarget.PlacementTargetSignalRouter => "signal_router",
            PlacementAssignmentTarget.PlacementTargetInputCoordinator => "input_coordinator",
            PlacementAssignmentTarget.PlacementTargetOutputCoordinator => "output_coordinator",
            PlacementAssignmentTarget.PlacementTargetRegionShard => "region_shard",
            _ => "unknown"
        };

    private bool IsTargetRoleEnabled(PlacementAssignmentTarget target, out WorkerServiceRole requiredRole)
    {
        if (!WorkerServiceRoles.TryMapAssignmentTarget(target, out requiredRole))
        {
            requiredRole = WorkerServiceRole.None;
            return true;
        }

        return WorkerServiceRoles.IsEnabled(_enabledRoles, requiredRole);
    }

    private static string ToFailureReasonLabel(PlacementFailureReason reason)
        => reason switch
        {
            PlacementFailureReason.PlacementFailureNone => "none",
            PlacementFailureReason.PlacementFailureInvalidBrain => "invalid_brain",
            PlacementFailureReason.PlacementFailureWorkerUnavailable => "worker_unavailable",
            PlacementFailureReason.PlacementFailureAssignmentRejected => "assignment_rejected",
            PlacementFailureReason.PlacementFailureAssignmentTimeout => "assignment_timeout",
            PlacementFailureReason.PlacementFailureReconcileMismatch => "reconcile_mismatch",
            PlacementFailureReason.PlacementFailureInternalError => "internal_error",
            _ => "unknown"
        };

    private static bool AssignmentSemanticallyMatches(PlacementAssignment existing, PlacementAssignment incoming)
    {
        if (!UuidEqual(existing.BrainId, incoming.BrainId)
            || !UuidEqual(existing.WorkerNodeId, incoming.WorkerNodeId)
            || existing.PlacementEpoch != incoming.PlacementEpoch
            || existing.Target != incoming.Target
            || existing.RegionId != incoming.RegionId
            || existing.ShardIndex != incoming.ShardIndex
            || existing.NeuronStart != incoming.NeuronStart
            || existing.NeuronCount != incoming.NeuronCount)
        {
            return false;
        }

        var existingName = existing.ActorName ?? string.Empty;
        var incomingName = incoming.ActorName ?? string.Empty;
        return string.Equals(existingName, incomingName, StringComparison.Ordinal);
    }

    private static bool UuidEqual(Uuid? left, Uuid? right)
    {
        var leftValue = left?.Value ?? ByteString.Empty;
        var rightValue = right?.Value ?? ByteString.Empty;
        return leftValue.Span.SequenceEqual(rightValue.Span);
    }

    private static bool PidEquals(PID? left, PID right)
        => left is not null
           && string.Equals(left.Address ?? string.Empty, right.Address ?? string.Empty, StringComparison.Ordinal)
           && string.Equals(left.Id ?? string.Empty, right.Id ?? string.Empty, StringComparison.Ordinal);

    private static PlacementAssignmentAck FailedAck(
        string assignmentId,
        Uuid? brainId,
        ulong placementEpoch,
        PlacementFailureReason reason,
        string message,
        bool retryable = false,
        ulong retryAfterMs = 0)
        => new()
        {
            AssignmentId = assignmentId ?? string.Empty,
            BrainId = brainId ?? new Uuid(),
            PlacementEpoch = placementEpoch,
            State = PlacementAssignmentState.PlacementAssignmentFailed,
            Accepted = false,
            Retryable = retryable,
            FailureReason = reason,
            Message = message ?? string.Empty,
            RetryAfterMs = retryable ? retryAfterMs : 0
        };

    private static PlacementUnassignmentAck FailedUnassignmentAck(
        string assignmentId,
        Uuid? brainId,
        ulong placementEpoch,
        PlacementFailureReason reason,
        string message)
        => new()
        {
            AssignmentId = assignmentId ?? string.Empty,
            BrainId = brainId ?? new Uuid(),
            PlacementEpoch = placementEpoch,
            Accepted = false,
            Retryable = false,
            FailureReason = reason,
            Message = message ?? string.Empty,
            RetryAfterMs = 0
        };

    private static PlacementUnassignmentAck BuildUnassignmentAck(
        PlacementAssignment assignment,
        bool accepted,
        string message)
        => new()
        {
            AssignmentId = assignment.AssignmentId ?? string.Empty,
            BrainId = assignment.BrainId ?? new Uuid(),
            PlacementEpoch = assignment.PlacementEpoch,
            Accepted = accepted,
            Retryable = false,
            FailureReason = accepted
                ? PlacementFailureReason.PlacementFailureNone
                : PlacementFailureReason.PlacementFailureInternalError,
            Message = message ?? string.Empty,
            RetryAfterMs = 0
        };

    private PlacementAssignmentAck BuildReadyAck(PlacementAssignment assignment, string message)
        => new()
        {
            AssignmentId = assignment.AssignmentId ?? string.Empty,
            BrainId = assignment.BrainId ?? new Uuid(),
            PlacementEpoch = assignment.PlacementEpoch,
            State = PlacementAssignmentState.PlacementAssignmentReady,
            Accepted = true,
            Retryable = false,
            FailureReason = PlacementFailureReason.PlacementFailureNone,
            Message = message
        };

    private bool MatchesReconcileRequest(HostedAssignmentState assignment, PlacementReconcileRequest request)
    {
        if (!assignment.Assignment.WorkerNodeId.TryToGuid(out var workerNodeId) || workerNodeId != _workerNodeId)
        {
            return false;
        }

        if (assignment.Assignment.PlacementEpoch != request.PlacementEpoch)
        {
            return false;
        }

        if (!assignment.Assignment.BrainId.TryToGuid(out var assignmentBrainId)
            || !request.BrainId.TryToGuid(out var requestBrainId))
        {
            return false;
        }

        return assignmentBrainId == requestBrainId;
    }

    private string BuildObservedActorPid(IContext context, HostedAssignmentState assignment)
    {
        if (assignment.HostedPid is not null)
        {
            return PidLabel(ToObservedRemotePid(context, assignment.HostedPid));
        }

        var actorId = assignment.Assignment.ActorName?.Trim();
        if (string.IsNullOrWhiteSpace(actorId))
        {
            actorId = context.Self.Id;
        }

        actorId = NormalizeObservedActorId(actorId);
        if (!actorId.Contains('/', StringComparison.Ordinal))
        {
            var workerRootActorId = NormalizeObservedActorId(context.Self.Id);
            if (!string.IsNullOrWhiteSpace(workerRootActorId)
                && !string.Equals(actorId, workerRootActorId, StringComparison.Ordinal))
            {
                actorId = $"{workerRootActorId}/{actorId}";
            }
        }

        return string.IsNullOrWhiteSpace(_workerAddress) ? actorId : $"{_workerAddress}/{actorId}";
    }

    private PID ToObservedRemotePid(IContext context, PID pid)
        => ToRemotePid(context, pid);

    private readonly record struct ValidatedPlacementRequest(PlacementAssignment Assignment, Guid BrainId);

    private readonly record struct PlacementValidationFailure(
        string AssignmentId,
        Uuid? BrainId,
        ulong PlacementEpoch,
        PlacementFailureReason FailureReason,
        string Message,
        PlacementAssignmentTarget Target);

    private static PID? ResolvePeerLatencyProbePid(PlacementPeerTarget peer)
    {
        if (peer is null || string.IsNullOrWhiteSpace(peer.WorkerRootActorName))
        {
            return null;
        }

        return new PID(peer.WorkerAddress ?? string.Empty, peer.WorkerRootActorName);
    }
}
