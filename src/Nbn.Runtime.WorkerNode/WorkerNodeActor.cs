
using System.Diagnostics;
using Google.Protobuf;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.Brain;
using Nbn.Runtime.IO;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.HiveMind;
using Nbn.Shared.Quantization;
using Proto;
using ProtoControl = Nbn.Proto.Control;
using SharedShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Runtime.WorkerNode;

public sealed class WorkerNodeActor : IActor
{
    private static readonly TimeSpan BrainInfoTimeout = TimeSpan.FromMilliseconds(500);

    private readonly Guid _workerNodeId;
    private readonly string _workerAddress;
    private readonly IArtifactStore _artifactStore;
    private readonly Dictionary<string, ServiceEndpointRegistration> _endpoints = new(StringComparer.Ordinal);
    private readonly Dictionary<string, HostedAssignmentState> _assignments = new(StringComparer.Ordinal);
    private readonly Dictionary<Guid, BrainHostingState> _brains = new();
    private PID? _hiveMindHintPid;
    private readonly WorkerServiceRole _enabledRoles;
    private readonly WorkerResourceAvailability _resourceAvailability;

    public WorkerNodeActor(
        Guid workerNodeId,
        string workerAddress,
        string? artifactRootPath = null,
        IArtifactStore? artifactStore = null,
        WorkerServiceRole enabledRoles = WorkerServiceRole.All,
        WorkerResourceAvailability? resourceAvailability = null)
    {
        if (workerNodeId == Guid.Empty)
        {
            throw new ArgumentException("Worker node id is required.", nameof(workerNodeId));
        }

        _workerNodeId = workerNodeId;
        _workerAddress = workerAddress ?? string.Empty;
        _artifactStore = artifactStore ?? new LocalArtifactStore(new ArtifactStoreOptions(ResolveArtifactRoot(artifactRootPath)));
        _enabledRoles = WorkerServiceRoles.Sanitize(enabledRoles);
        _resourceAvailability = resourceAvailability ?? WorkerResourceAvailability.Default;
    }

    public async Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case DiscoverySnapshotApplied snapshot:
                ApplyDiscoverySnapshot(snapshot);
                break;
            case EndpointStateObserved endpointState:
                ApplyObservedEndpoint(endpointState.Observation, source: "update");
                break;
            case EndpointRegistrationObserved endpoint:
                ApplyEndpoint(endpoint.Registration, source: "update");
                break;
            case PlacementAssignmentRequest request:
                await HandlePlacementAssignmentAsync(context, request).ConfigureAwait(false);
                break;
            case PlacementUnassignmentRequest request:
                HandlePlacementUnassignment(context, request);
                break;
            case PlacementReconcileRequest request:
                HandlePlacementReconcile(context, request);
                break;
            case GetWorkerNodeSnapshot:
                context.Respond(BuildSnapshot());
                break;
            case Terminated terminated:
                HandleTerminated(terminated);
                break;
        }
    }

    public sealed record DiscoverySnapshotApplied(IReadOnlyDictionary<string, ServiceEndpointRegistration> Registrations);

    public sealed record EndpointStateObserved(ServiceEndpointObservation Observation);

    public sealed record EndpointRegistrationObserved(ServiceEndpointRegistration Registration);

    public sealed record GetWorkerNodeSnapshot;

    public sealed record WorkerNodeSnapshot(
        Guid WorkerNodeId,
        string WorkerAddress,
        ServiceEndpointRegistration? HiveMindEndpoint,
        ServiceEndpointRegistration? IoGatewayEndpoint,
        WorkerServiceRole EnabledRoles,
        int TrackedAssignmentCount,
        WorkerResourceAvailability ResourceAvailability);

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
    private async Task HandlePlacementAssignmentAsync(IContext context, PlacementAssignmentRequest request)
    {
        var assignment = request.Assignment;
        if (assignment is null)
        {
            RespondFailedPlacement(context, FailedAck(
                assignmentId: string.Empty,
                brainId: null,
                placementEpoch: 0,
                PlacementFailureReason.PlacementFailureInternalError,
                "assignment payload was empty"), PlacementAssignmentTarget.PlacementTargetUnknown);
            return;
        }

        if (string.IsNullOrWhiteSpace(assignment.AssignmentId))
        {
            RespondFailedPlacement(context, FailedAck(
                assignmentId: string.Empty,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureInternalError,
                "assignment_id is required"), assignment.Target);
            return;
        }

        if (assignment.BrainId is null || !assignment.BrainId.TryToGuid(out var brainId))
        {
            RespondFailedPlacement(context, FailedAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureInternalError,
                "brain_id was invalid"), assignment.Target);
            return;
        }

        if (!assignment.WorkerNodeId.TryToGuid(out var targetWorkerNodeId))
        {
            RespondFailedPlacement(context, FailedAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureInternalError,
                "worker_node_id was invalid"), assignment.Target);
            return;
        }

        if (targetWorkerNodeId != _workerNodeId)
        {
            RespondFailedPlacement(context, FailedAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureWorkerUnavailable,
                $"assignment is for worker {targetWorkerNodeId}, not {_workerNodeId}"), assignment.Target);
            return;
        }

        if (!IsTargetRoleEnabled(assignment.Target, out var requiredRole))
        {
            var roleToken = WorkerServiceRoles.ToRoleToken(requiredRole);
            var targetToken = ToPlacementTargetLabel(assignment.Target);
            RespondFailedPlacement(context, FailedAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureAssignmentRejected,
                $"assignment target '{targetToken}' requires service role '{roleToken}', but that role is disabled on worker {_workerNodeId}. Enable it with --service-role {roleToken} or --service-roles all."), assignment.Target);
            return;
        }

        MaybeCaptureHiveMindHint(context.Sender);

        var brain = GetOrCreateBrainState(brainId);

        if (assignment.PlacementEpoch < brain.PlacementEpoch)
        {
            RespondFailedPlacement(context, FailedAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureInternalError,
                $"assignment epoch {assignment.PlacementEpoch} is older than hosted epoch {brain.PlacementEpoch}"), assignment.Target);
            return;
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

        if (_assignments.TryGetValue(assignment.AssignmentId, out var existing))
        {
            var existingAssignment = existing.Assignment;
            if (existing.State == PlacementAssignmentState.PlacementAssignmentReady
                && AssignmentSemanticallyMatches(existingAssignment, assignment))
            {
                RespondReadyPlacement(context, existing.Assignment, "already_ready", hostingMs: 0);
                return;
            }

            if (existing.State == PlacementAssignmentState.PlacementAssignmentReady)
            {
                RespondFailedPlacement(context, FailedAck(
                    assignment.AssignmentId,
                    assignment.BrainId,
                    assignment.PlacementEpoch,
                    PlacementFailureReason.PlacementFailureAssignmentRejected,
                    "assignment_id conflicts with an existing ready assignment"), assignment.Target);
                return;
            }
        }

        if (assignment.Target == PlacementAssignmentTarget.PlacementTargetRegionShard)
        {
            await EnsureRuntimeInfoAsync(context, brain).ConfigureAwait(false);
        }

        var hostingStarted = Stopwatch.GetTimestamp();
        var hosted = await HostAssignmentAsync(context, brain, assignment.Clone()).ConfigureAwait(false);
        var hostingMs = Stopwatch.GetElapsedTime(hostingStarted).TotalMilliseconds;
        if (!hosted.Success)
        {
            RespondFailedPlacement(context, hosted.FailedAck!, assignment.Target);
            return;
        }

        var hostedState = new HostedAssignmentState(hosted.Assignment!, hosted.HostedPid)
        {
            State = PlacementAssignmentState.PlacementAssignmentReady,
            Message = "ready"
        };

        _assignments[assignment.AssignmentId] = hostedState;
        brain.Assignments[assignment.AssignmentId] = hostedState;

        UpdateRuntimeWidthsFromShards(brain);
        PushRouting(context, brain);
        PushShardEndpoints(context, brain);
        PushIoGatewayRegistration(context, brain);
        RegisterOutputSink(context, brain);

        RespondReadyPlacement(context, hosted.Assignment!, "ready", hostingMs);
    }

    private void HandlePlacementUnassignment(IContext context, PlacementUnassignmentRequest request)
    {
        var assignment = request.Assignment;
        if (assignment is null)
        {
            ReplyToSender(context, FailedUnassignmentAck(
                assignmentId: string.Empty,
                brainId: null,
                placementEpoch: 0,
                PlacementFailureReason.PlacementFailureInternalError,
                "assignment payload was empty"));
            return;
        }

        if (string.IsNullOrWhiteSpace(assignment.AssignmentId))
        {
            ReplyToSender(context, FailedUnassignmentAck(
                assignmentId: string.Empty,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureInternalError,
                "assignment_id is required"));
            return;
        }

        if (assignment.BrainId is null || !assignment.BrainId.TryToGuid(out var brainId))
        {
            ReplyToSender(context, FailedUnassignmentAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureInternalError,
                "brain_id was invalid"));
            return;
        }

        if (!assignment.WorkerNodeId.TryToGuid(out var targetWorkerNodeId))
        {
            ReplyToSender(context, FailedUnassignmentAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureInternalError,
                "worker_node_id was invalid"));
            return;
        }

        if (targetWorkerNodeId != _workerNodeId)
        {
            ReplyToSender(context, FailedUnassignmentAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureWorkerUnavailable,
                $"assignment is for worker {targetWorkerNodeId}, not {_workerNodeId}"));
            return;
        }

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

        if (!_brains.TryGetValue(brainId, out var brain))
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

        UpdateRuntimeWidthsFromShards(brain);
        PushRouting(context, brain);
        PushShardEndpoints(context, brain);
        PushIoGatewayRegistration(context, brain);
        RegisterOutputSink(context, brain, allowClear: true);

        return removed;
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
    private async Task<HostingResult> HostAssignmentAsync(
        IContext context,
        BrainHostingState brain,
        PlacementAssignment assignment)
    {
        try
        {
            return assignment.Target switch
            {
                PlacementAssignmentTarget.PlacementTargetBrainRoot
                    => HostBrainRoot(context, brain, assignment),
                PlacementAssignmentTarget.PlacementTargetSignalRouter
                    => HostSignalRouter(context, brain, assignment),
                PlacementAssignmentTarget.PlacementTargetInputCoordinator
                    => HostInputCoordinator(context, brain, assignment),
                PlacementAssignmentTarget.PlacementTargetOutputCoordinator
                    => HostOutputCoordinator(context, brain, assignment),
                PlacementAssignmentTarget.PlacementTargetRegionShard
                    => await HostRegionShardAsync(context, brain, assignment).ConfigureAwait(false),
                _ => HostingResult.Failed(FailedAck(
                    assignment.AssignmentId,
                    assignment.BrainId,
                    assignment.PlacementEpoch,
                    PlacementFailureReason.PlacementFailureInternalError,
                    $"unsupported placement target {(int)assignment.Target}"))
            };
        }
        catch (Exception ex)
        {
            var detail = string.IsNullOrWhiteSpace(ex.Message)
                ? ex.GetBaseException().Message
                : ex.Message;
            return HostingResult.Failed(FailedAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureInternalError,
                detail));
        }
    }

    private HostingResult HostBrainRoot(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        var actorName = ResolveActorName(assignment);
        var hiveMindPid = ResolveHiveMindPid(context);
        var pid = SpawnOrResolveNamed(
            context,
            actorName,
            Props.FromProducer(() => new BrainRootActor(brain.BrainId, hiveMindPid, autoSpawnSignalRouter: false)),
            brain.BrainRootPid);

        brain.BrainRootPid = pid;
        context.Watch(pid);

        if (brain.SignalRouterPid is not null)
        {
            context.Send(pid, new SetSignalRouter(brain.SignalRouterPid));
        }

        return HostingResult.Succeeded(assignment, pid);
    }

    private HostingResult HostSignalRouter(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        var actorName = ResolveActorName(assignment);
        var pid = SpawnOrResolveNamed(
            context,
            actorName,
            Props.FromProducer(() => new BrainSignalRouterActor(brain.BrainId)),
            brain.SignalRouterPid);

        brain.SignalRouterPid = pid;
        context.Watch(pid);

        if (brain.BrainRootPid is not null)
        {
            context.Send(brain.BrainRootPid, new SetSignalRouter(pid));
        }

        return HostingResult.Succeeded(assignment, pid);
    }

    private HostingResult HostInputCoordinator(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        var actorName = ResolveActorName(assignment);
        var inputWidth = ResolveInputWidth(brain);
        var pid = SpawnOrResolveNamed(
            context,
            actorName,
            Props.FromProducer(() => new InputCoordinatorActor(brain.BrainId, (uint)inputWidth)),
            brain.InputCoordinatorPid);

        brain.InputCoordinatorPid = pid;
        context.Watch(pid);
        return HostingResult.Succeeded(assignment, pid);
    }

    private HostingResult HostOutputCoordinator(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        var actorName = ResolveActorName(assignment);
        var outputWidth = ResolveOutputWidth(brain);
        var pid = SpawnOrResolveNamed(
            context,
            actorName,
            Props.FromProducer(() => new OutputCoordinatorActor(brain.BrainId, (uint)outputWidth)),
            brain.OutputCoordinatorPid);

        brain.OutputCoordinatorPid = pid;
        context.Watch(pid);
        return HostingResult.Succeeded(assignment, pid);
    }

    private async Task<HostingResult> HostRegionShardAsync(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        var regionId = checked((int)assignment.RegionId);
        var shardIndex = checked((int)assignment.ShardIndex);
        if (!SharedShardId32.TryFrom(regionId, shardIndex, out var shardId))
        {
            return HostingResult.Failed(FailedAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureInternalError,
                $"invalid shard target r{regionId}:s{shardIndex}"));
        }

        var neuronStart = checked((int)assignment.NeuronStart);
        var requestedNeuronCount = checked((int)assignment.NeuronCount);
        var neuronCount = Math.Max(1, requestedNeuronCount);
        var actorName = ResolveActorName(assignment);
        var routing = BuildShardRouting(brain, (shardId, neuronStart, neuronCount));

        var config = new RegionShardActorConfig(
            brain.BrainId,
            shardId,
            brain.SignalRouterPid,
            regionId == NbnConstants.OutputRegionId ? brain.OutputCoordinatorPid : null,
            ResolveHiveMindPid(context),
            routing);

        var props = await BuildRegionShardPropsAsync(brain, assignment, neuronStart, neuronCount, config).ConfigureAwait(false);
        var existingPid = brain.RegionShards.TryGetValue(shardId, out var existing) ? existing.Pid : null;
        var pid = SpawnOrResolveNamed(context, actorName, props, existingPid);

        brain.RegionShards[shardId] = new HostedShard(shardId, neuronStart, neuronCount, pid, assignment.AssignmentId);
        context.Watch(pid);
        RegisterShard(context, brain, shardId, neuronStart, neuronCount, pid);

        return HostingResult.Succeeded(assignment, pid);
    }
    private async Task<Props> BuildRegionShardPropsAsync(
        BrainHostingState brain,
        PlacementAssignment assignment,
        int neuronStart,
        int neuronCount,
        RegionShardActorConfig config)
    {
        var runtime = brain.RuntimeInfo;
        if (runtime is not null && HasArtifactRef(runtime.BaseDefinition))
        {
            var nbnRef = runtime.BaseDefinition!.Clone();
            var nbsRef = HasArtifactRef(runtime.LastSnapshot) ? runtime.LastSnapshot!.Clone() : null;
            try
            {
                return await RegionShardArtifactLoader.CreatePropsAsync(
                    _artifactStore,
                    nbnRef,
                    nbsRef,
                    checked((int)assignment.RegionId),
                    neuronStart,
                    neuronCount,
                    config,
                    brain.BrainId).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var detail = ex.GetBaseException().Message;
                runtime.LastArtifactLoadError = detail;
                throw new InvalidOperationException(
                    $"Artifact-backed shard load failed for brain {brain.BrainId} region {assignment.RegionId} shard {assignment.ShardIndex}: {detail}",
                    ex);
            }
        }

        var state = BuildSyntheticRegionState(brain, assignment, neuronStart, neuronCount);
        return Props.FromProducer(() => new RegionShardActor(state, config));
    }

    private RegionShardState BuildSyntheticRegionState(
        BrainHostingState brain,
        PlacementAssignment assignment,
        int neuronStart,
        int neuronCount)
    {
        var regionId = checked((int)assignment.RegionId);
        var regionSpans = new int[NbnConstants.RegionCount];
        regionSpans[regionId] = neuronStart + neuronCount;
        regionSpans[NbnConstants.InputRegionId] = ResolveInputWidth(brain);
        regionSpans[NbnConstants.OutputRegionId] = ResolveOutputWidth(brain);

        var buffer = new float[neuronCount];
        var enabled = Enumerable.Repeat(true, neuronCount).ToArray();
        var exists = Enumerable.Repeat(true, neuronCount).ToArray();
        var accum = Enumerable.Repeat((byte)AccumulationFunction.AccumSum, neuronCount).ToArray();
        var activation = Enumerable.Repeat((byte)ActivationFunction.ActNone, neuronCount).ToArray();
        var reset = Enumerable.Repeat((byte)ResetFunction.ResetHold, neuronCount).ToArray();
        var paramA = new float[neuronCount];
        var paramB = new float[neuronCount];
        var pre = Enumerable.Repeat(-1f, neuronCount).ToArray();
        var threshold = Enumerable.Repeat(1f, neuronCount).ToArray();
        var axonCounts = new ushort[neuronCount];
        var axonStarts = new int[neuronCount];
        var axons = new RegionShardAxons(
            Array.Empty<byte>(),
            Array.Empty<int>(),
            Array.Empty<float>(),
            Array.Empty<byte>(),
            Array.Empty<byte>(),
            Array.Empty<bool>(),
            Array.Empty<uint>(),
            Array.Empty<uint>());

        return new RegionShardState(
            regionId,
            neuronStart,
            neuronCount,
            brain.BrainSeed,
            QuantizationSchemas.DefaultNbn.Strength,
            regionSpans,
            buffer,
            enabled,
            exists,
            accum,
            activation,
            reset,
            paramA,
            paramB,
            pre,
            threshold,
            axonCounts,
            axonStarts,
            axons);
    }

    private void RegisterShard(
        IContext context,
        BrainHostingState brain,
        SharedShardId32 shardId,
        int neuronStart,
        int neuronCount,
        PID shardPid)
    {
        var hiveMindPid = ResolveHiveMindPid(context);
        if (hiveMindPid is null)
        {
            return;
        }

        var remoteShardPid = ToRemotePid(context, shardPid);
        TryRequest(context, hiveMindPid, new ProtoControl.RegisterShard
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(remoteShardPid),
            NeuronStart = (uint)Math.Max(0, neuronStart),
            NeuronCount = (uint)Math.Max(0, neuronCount)
        });
    }

    private void RegisterOutputSink(IContext context, BrainHostingState brain)
        => RegisterOutputSink(context, brain, allowClear: false);

    private void RegisterOutputSink(IContext context, BrainHostingState brain, bool allowClear)
    {
        if (brain.OutputCoordinatorPid is null && !allowClear)
        {
            return;
        }

        var hiveMindPid = ResolveHiveMindPid(context);
        if (hiveMindPid is null)
        {
            return;
        }

        TryRequest(context, hiveMindPid, new ProtoControl.RegisterOutputSink
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            OutputPid = brain.OutputCoordinatorPid is null
                ? string.Empty
                : PidLabel(ToRemotePid(context, brain.OutputCoordinatorPid))
        });
    }

    private void UnregisterShard(
        IContext context,
        BrainHostingState brain,
        SharedShardId32 shardId)
    {
        var hiveMindPid = ResolveHiveMindPid(context);
        if (hiveMindPid is null)
        {
            return;
        }

        TryRequest(context, hiveMindPid, new ProtoControl.UnregisterShard
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex
        });
    }

    private void PushRouting(IContext context, BrainHostingState brain)
    {
        var routes = brain.RegionShards.Values
            .OrderBy(static entry => entry.ShardId.RegionId)
            .ThenBy(static entry => entry.ShardId.ShardIndex)
            .Select(entry => new ShardRoute(entry.ShardId.Value, ToRemotePid(context, entry.Pid)))
            .ToArray();

        var snapshot = routes.Length == 0 ? RoutingTableSnapshot.Empty : new RoutingTableSnapshot(routes);
        if (brain.BrainRootPid is not null)
        {
            context.Send(brain.BrainRootPid, new SetRoutingTable(snapshot));
        }

        if (brain.SignalRouterPid is not null)
        {
            context.Send(brain.SignalRouterPid, new SetRoutingTable(snapshot));
        }

        if (brain.RegionShards.Count == 0)
        {
            return;
        }

        var shardRouting = BuildShardRouting(brain);
        foreach (var shard in brain.RegionShards.Values)
        {
            context.Send(shard.Pid, new RegionShardUpdateRouting(shardRouting));
        }
    }

    private void PushShardEndpoints(IContext context, BrainHostingState brain)
    {
        if (brain.RegionShards.Count == 0)
        {
            return;
        }

        var tickSink = ResolveHiveMindPid(context);
        foreach (var shard in brain.RegionShards.Values)
        {
            var outputSink = shard.ShardId.RegionId == NbnConstants.OutputRegionId
                ? brain.OutputCoordinatorPid
                : null;
            context.Send(
                shard.Pid,
                new RegionShardUpdateEndpoints(brain.SignalRouterPid, outputSink, tickSink));
        }
    }

    private void PushIoGatewayRegistration(IContext context, BrainHostingState brain)
    {
        if (!TryResolveEndpointPid(ServiceEndpointSettings.IoGatewayKey, out var ioPid))
        {
            return;
        }

        var target = brain.BrainRootPid ?? brain.SignalRouterPid;
        if (target is null)
        {
            return;
        }

        context.Send(target, new RegisterIoGateway
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            IoGatewayPid = PidLabel(ioPid)
        });
    }

    private async Task EnsureRuntimeInfoAsync(IContext context, BrainHostingState brain)
    {
        brain.RuntimeInfo ??= new BrainRuntimeInfo();
        if (brain.RuntimeInfo.HasIoMetadata)
        {
            return;
        }

        if (!TryResolveEndpointPid(ServiceEndpointSettings.IoGatewayKey, out var ioPid))
        {
            brain.RuntimeInfo.LastIoError = "io_gateway_unavailable";
            return;
        }

        var candidatePids = new List<PID>(2);
        var systemAddress = context.System.Address;
        if (string.IsNullOrWhiteSpace(ioPid.Address) && !string.IsNullOrWhiteSpace(systemAddress))
        {
            candidatePids.Add(new PID(systemAddress, ioPid.Id));
        }

        candidatePids.Add(ioPid);

        Exception? lastRequestException = null;
        BrainInfo? info = null;

        try
        {
            foreach (var candidate in candidatePids)
            {
                try
                {
                    info = await context.RequestAsync<BrainInfo>(
                        candidate,
                        new BrainInfoRequest
                        {
                            BrainId = brain.BrainId.ToProtoUuid()
                        },
                        BrainInfoTimeout).ConfigureAwait(false);

                    if (info is not null)
                    {
                        break;
                    }
                }
                catch (Exception ex)
                {
                    lastRequestException = ex;
                }
            }

            if (info is null)
            {
                if (lastRequestException is not null)
                {
                    throw lastRequestException;
                }

                return;
            }

            var hasMetadata = false;
            if (info.InputWidth > 0)
            {
                brain.RuntimeInfo.InputWidth = Math.Max(brain.RuntimeInfo.InputWidth, checked((int)info.InputWidth));
                hasMetadata = true;
            }

            if (info.OutputWidth > 0)
            {
                brain.RuntimeInfo.OutputWidth = Math.Max(brain.RuntimeInfo.OutputWidth, checked((int)info.OutputWidth));
                hasMetadata = true;
            }

            if (HasArtifactRef(info.BaseDefinition))
            {
                brain.RuntimeInfo.BaseDefinition = info.BaseDefinition.Clone();
                hasMetadata = true;
            }

            if (HasArtifactRef(info.LastSnapshot))
            {
                brain.RuntimeInfo.LastSnapshot = info.LastSnapshot.Clone();
                hasMetadata = true;
            }

            if (hasMetadata)
            {
                brain.RuntimeInfo.HasIoMetadata = true;
                brain.RuntimeInfo.LastIoError = string.Empty;
            }

            UpdateRuntimeWidthsFromShards(brain);
        }
        catch (Exception ex)
        {
            brain.RuntimeInfo.LastIoError = ex.GetBaseException().Message;
        }
    }

    private void UpdateRuntimeWidthsFromShards(BrainHostingState brain)
    {
        brain.RuntimeInfo ??= new BrainRuntimeInfo();
        foreach (var shard in brain.RegionShards.Values)
        {
            var span = shard.NeuronStart + shard.NeuronCount;
            if (shard.ShardId.RegionId == NbnConstants.InputRegionId)
            {
                brain.RuntimeInfo.InputWidth = Math.Max(brain.RuntimeInfo.InputWidth, span);
            }

            if (shard.ShardId.RegionId == NbnConstants.OutputRegionId)
            {
                brain.RuntimeInfo.OutputWidth = Math.Max(brain.RuntimeInfo.OutputWidth, span);
            }
        }
    }

    private int ResolveInputWidth(BrainHostingState brain)
    {
        brain.RuntimeInfo ??= new BrainRuntimeInfo();
        UpdateRuntimeWidthsFromShards(brain);
        return Math.Max(1, brain.RuntimeInfo.InputWidth);
    }

    private int ResolveOutputWidth(BrainHostingState brain)
    {
        brain.RuntimeInfo ??= new BrainRuntimeInfo();
        UpdateRuntimeWidthsFromShards(brain);
        return Math.Max(1, brain.RuntimeInfo.OutputWidth);
    }
    private RegionShardRoutingTable BuildShardRouting(
        BrainHostingState brain,
        (SharedShardId32 ShardId, int NeuronStart, int NeuronCount)? includeShard = null)
    {
        var map = new Dictionary<int, List<ShardSpan>>();
        foreach (var shard in brain.RegionShards.Values)
        {
            if (!map.TryGetValue(shard.ShardId.RegionId, out var spans))
            {
                spans = new List<ShardSpan>();
                map[shard.ShardId.RegionId] = spans;
            }

            spans.Add(new ShardSpan(shard.NeuronStart, Math.Max(1, shard.NeuronCount), shard.ShardId));
        }

        if (includeShard.HasValue)
        {
            var include = includeShard.Value;
            if (!map.TryGetValue(include.ShardId.RegionId, out var spans))
            {
                spans = new List<ShardSpan>();
                map[include.ShardId.RegionId] = spans;
            }

            spans.RemoveAll(span => span.ShardId.Equals(include.ShardId));
            spans.Add(new ShardSpan(include.NeuronStart, Math.Max(1, include.NeuronCount), include.ShardId));
        }

        var compact = new Dictionary<int, ShardSpan[]>();
        foreach (var entry in map)
        {
            var spans = entry.Value
                .OrderBy(static span => span.Start)
                .ToArray();
            compact[entry.Key] = spans;
        }

        return new RegionShardRoutingTable(compact);
    }

    private PID SpawnOrResolveNamed(IContext context, string actorName, Props props, PID? existingPid)
    {
        if (existingPid is not null && string.Equals(existingPid.Id, actorName, StringComparison.Ordinal))
        {
            return existingPid;
        }

        try
        {
            return context.SpawnNamed(props, actorName);
        }
        catch
        {
            return new PID(string.Empty, actorName);
        }
    }

    private static string ResolveActorName(PlacementAssignment assignment)
    {
        if (!string.IsNullOrWhiteSpace(assignment.ActorName))
        {
            var trimmed = assignment.ActorName.Trim();
            var slash = trimmed.LastIndexOf('/');
            if (slash >= 0 && slash < trimmed.Length - 1)
            {
                trimmed = trimmed[(slash + 1)..];
            }

            if (!string.IsNullOrWhiteSpace(trimmed))
            {
                return trimmed;
            }
        }

        return assignment.Target switch
        {
            PlacementAssignmentTarget.PlacementTargetBrainRoot => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-root",
            PlacementAssignmentTarget.PlacementTargetSignalRouter => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-router",
            PlacementAssignmentTarget.PlacementTargetInputCoordinator => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-input",
            PlacementAssignmentTarget.PlacementTargetOutputCoordinator => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-output",
            PlacementAssignmentTarget.PlacementTargetRegionShard => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-r{assignment.RegionId}-s{assignment.ShardIndex}",
            _ => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-assignment-{Math.Abs(assignment.AssignmentId.GetHashCode(StringComparison.Ordinal))}"
        };
    }

    private BrainHostingState GetOrCreateBrainState(Guid brainId)
    {
        if (_brains.TryGetValue(brainId, out var brain))
        {
            return brain;
        }

        brain = new BrainHostingState(brainId);
        _brains[brainId] = brain;
        return brain;
    }

    private void ResetBrainState(IContext context, BrainHostingState brain)
    {
        var toStop = new Dictionary<string, PID>(StringComparer.Ordinal);
        AddPid(toStop, brain.BrainRootPid);
        AddPid(toStop, brain.SignalRouterPid);
        AddPid(toStop, brain.InputCoordinatorPid);
        AddPid(toStop, brain.OutputCoordinatorPid);

        foreach (var shard in brain.RegionShards.Values)
        {
            AddPid(toStop, shard.Pid);
        }

        foreach (var pid in toStop.Values)
        {
            context.Stop(pid);
        }

        foreach (var assignmentId in brain.Assignments.Keys.ToArray())
        {
            _assignments.Remove(assignmentId);
        }

        brain.Assignments.Clear();
        brain.RegionShards.Clear();
        brain.BrainRootPid = null;
        brain.SignalRouterPid = null;
        brain.InputCoordinatorPid = null;
        brain.OutputCoordinatorPid = null;
        brain.RuntimeInfo = null;
    }

    private static void AddPid(Dictionary<string, PID> toStop, PID? pid)
    {
        if (pid is null)
        {
            return;
        }

        toStop[PidLabel(pid)] = pid;
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
                    assignment.Message = "terminated";
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
        try
        {
            context.Request(target, message);
            return;
        }
        catch
        {
            // Retry with local system address for address-less local endpoints.
        }

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
                    // Best-effort control-plane notifications should not crash placement hosting.
                }
            }
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
        string message)
        => new()
        {
            AssignmentId = assignmentId ?? string.Empty,
            BrainId = brainId ?? new Uuid(),
            PlacementEpoch = placementEpoch,
            State = PlacementAssignmentState.PlacementAssignmentFailed,
            Accepted = false,
            Retryable = false,
            FailureReason = reason,
            Message = message ?? string.Empty,
            RetryAfterMs = 0
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
        var configuredActorName = assignment.Assignment.ActorName?.Trim();
        if (!string.IsNullOrWhiteSpace(configuredActorName))
        {
            var slash = configuredActorName.LastIndexOf('/');
            if (slash >= 0 && slash < configuredActorName.Length - 1)
            {
                configuredActorName = configuredActorName[(slash + 1)..];
            }

            if (!string.IsNullOrWhiteSpace(configuredActorName))
            {
                return string.IsNullOrWhiteSpace(_workerAddress)
                    ? configuredActorName
                    : $"{_workerAddress}/{configuredActorName}";
            }
        }

        if (assignment.HostedPid is not null)
        {
            return PidLabel(ToRemotePid(context, assignment.HostedPid));
        }

        var actor = string.IsNullOrWhiteSpace(assignment.Assignment.ActorName)
            ? context.Self.Id
            : assignment.Assignment.ActorName.Trim();
        if (actor.Contains('/', StringComparison.Ordinal))
        {
            return actor;
        }

        return string.IsNullOrWhiteSpace(_workerAddress) ? actor : $"{_workerAddress}/{actor}";
    }

    private void MaybeCaptureHiveMindHint(PID? sender)
    {
        if (sender is null
            || string.IsNullOrWhiteSpace(sender.Id)
            || sender.Id.StartsWith("$", StringComparison.Ordinal))
        {
            return;
        }

        _hiveMindHintPid = sender;
    }

    private PID? ResolveHiveMindPid(IContext context)
    {
        if (TryResolveEndpointPid(ServiceEndpointSettings.HiveMindKey, out var endpointPid))
        {
            if (!string.IsNullOrWhiteSpace(endpointPid.Address))
            {
                return endpointPid;
            }

            if (_hiveMindHintPid is not null
                && !string.IsNullOrWhiteSpace(_hiveMindHintPid.Address)
                && string.Equals(_hiveMindHintPid.Id, endpointPid.Id, StringComparison.Ordinal))
            {
                return _hiveMindHintPid;
            }

            return endpointPid;
        }

        if (_hiveMindHintPid is not null)
        {
            WorkerNodeTelemetry.RecordDiscoveryEndpointResolve(
                _workerNodeId,
                brainId: null,
                placementEpoch: 0,
                target: ServiceEndpointSettings.HiveMindKey,
                outcome: "resolved_hint",
                failureReason: "endpoint_missing");
            return _hiveMindHintPid;
        }

        return null;
    }

    private bool TryResolveEndpointPid(string key, out PID pid)
    {
        if (_endpoints.TryGetValue(key, out var registration))
        {
            pid = registration.Endpoint.ToPid();
            WorkerNodeTelemetry.RecordDiscoveryEndpointResolve(
                _workerNodeId,
                brainId: null,
                placementEpoch: 0,
                target: key,
                outcome: "resolved",
                failureReason: "none");
            return true;
        }

        WorkerNodeTelemetry.RecordDiscoveryEndpointResolve(
            _workerNodeId,
            brainId: null,
            placementEpoch: 0,
            target: key,
            outcome: "missing",
            failureReason: "endpoint_missing");
        pid = new PID();
        return false;
    }

    private string ResolveReconcileFailureReason(
        bool requestHasBrainId,
        Guid requestBrainId,
        ulong placementEpoch,
        int assignmentCount)
    {
        if (!requestHasBrainId)
        {
            return "brain_id_invalid";
        }

        if (!_brains.TryGetValue(requestBrainId, out var brain))
        {
            return assignmentCount > 0 ? "none" : "brain_not_hosted";
        }

        if (brain.PlacementEpoch != placementEpoch)
        {
            return "placement_epoch_mismatch";
        }

        return assignmentCount > 0 ? "none" : "no_assignments";
    }

    private const string DiscoveryTargetLabel = "discovery";

    private static string NormalizeSource(string source)
        => string.IsNullOrWhiteSpace(source) ? "update" : source.Trim().ToLowerInvariant();

    private static string NormalizeFailureReason(string failureReason, string fallback)
        => string.IsNullOrWhiteSpace(failureReason) ? fallback : failureReason.Trim().ToLowerInvariant();

    private PID ToRemotePid(IContext context, PID pid)
    {
        var systemAddress = context.System.Address;

        if (!string.IsNullOrWhiteSpace(_workerAddress))
        {
            if (string.IsNullOrWhiteSpace(pid.Address)
                || string.Equals(pid.Address, systemAddress, StringComparison.Ordinal)
                || string.Equals(pid.Address, "nonhost", StringComparison.OrdinalIgnoreCase))
            {
                return new PID(_workerAddress, pid.Id);
            }
        }

        if (!string.IsNullOrWhiteSpace(pid.Address))
        {
            return pid;
        }

        if (string.IsNullOrWhiteSpace(systemAddress))
        {
            return pid;
        }

        return new PID(systemAddress, pid.Id);
    }

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private static bool HasArtifactRef(ArtifactRef? reference)
        => reference is not null
           && reference.Sha256 is not null
           && reference.Sha256.Value is not null
           && reference.Sha256.Value.Length == 32;

    private static string ResolveArtifactRoot(string? overridePath)
    {
        if (!string.IsNullOrWhiteSpace(overridePath))
        {
            return overridePath.Trim();
        }

        var envRoot = Environment.GetEnvironmentVariable("NBN_ARTIFACT_ROOT");
        if (!string.IsNullOrWhiteSpace(envRoot))
        {
            return envRoot.Trim();
        }

        return Path.Combine(Environment.CurrentDirectory, "artifacts");
    }

    private sealed class BrainHostingState
    {
        public BrainHostingState(Guid brainId)
        {
            BrainId = brainId;
            BrainSeed = BitConverter.ToUInt64(brainId.ToByteArray(), 0);
        }

        public Guid BrainId { get; }
        public ulong BrainSeed { get; }
        public ulong PlacementEpoch { get; set; }
        public PID? BrainRootPid { get; set; }
        public PID? SignalRouterPid { get; set; }
        public PID? InputCoordinatorPid { get; set; }
        public PID? OutputCoordinatorPid { get; set; }
        public BrainRuntimeInfo? RuntimeInfo { get; set; }
        public Dictionary<SharedShardId32, HostedShard> RegionShards { get; } = new();
        public Dictionary<string, HostedAssignmentState> Assignments { get; } = new(StringComparer.Ordinal);
    }

    private sealed class BrainRuntimeInfo
    {
        public int InputWidth { get; set; }
        public int OutputWidth { get; set; }
        public ArtifactRef? BaseDefinition { get; set; }
        public ArtifactRef? LastSnapshot { get; set; }
        public bool HasIoMetadata { get; set; }
        public string LastIoError { get; set; } = string.Empty;
        public string LastArtifactLoadError { get; set; } = string.Empty;
    }

    private sealed class HostedAssignmentState
    {
        public HostedAssignmentState(PlacementAssignment assignment, PID? hostedPid)
        {
            Assignment = assignment;
            HostedPid = hostedPid;
        }

        public PlacementAssignment Assignment { get; }
        public PID? HostedPid { get; set; }
        public PlacementAssignmentState State { get; set; }
        public string Message { get; set; } = string.Empty;
    }

    private readonly record struct HostedShard(
        SharedShardId32 ShardId,
        int NeuronStart,
        int NeuronCount,
        PID Pid,
        string AssignmentId);

    private sealed class HostingResult
    {
        private HostingResult(bool success, PlacementAssignment? assignment, PID? hostedPid, PlacementAssignmentAck? failedAck)
        {
            Success = success;
            Assignment = assignment;
            HostedPid = hostedPid;
            FailedAck = failedAck;
        }

        public bool Success { get; }
        public PlacementAssignment? Assignment { get; }
        public PID? HostedPid { get; }
        public PlacementAssignmentAck? FailedAck { get; }

        public static HostingResult Succeeded(PlacementAssignment assignment, PID hostedPid)
            => new(true, assignment, hostedPid, null);

        public static HostingResult Failed(PlacementAssignmentAck failedAck)
            => new(false, null, null, failedAck);
    }
}

internal static class WorkerNodeUuidExtensions
{
    public static Guid ToGuidOrEmpty(this Uuid? value)
    {
        if (value is null || !value.TryToGuid(out var guid))
        {
            return Guid.Empty;
        }

        return guid;
    }
}
