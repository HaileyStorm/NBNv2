
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
    private static readonly TimeSpan BrainInfoTimeout = TimeSpan.FromSeconds(5);

    private readonly Guid _workerNodeId;
    private readonly string _workerAddress;
    private readonly IArtifactStore _artifactStore;
    private readonly Dictionary<string, ServiceEndpointRegistration> _endpoints = new(StringComparer.Ordinal);
    private readonly Dictionary<string, HostedAssignmentState> _assignments = new(StringComparer.Ordinal);
    private readonly Dictionary<Guid, BrainHostingState> _brains = new();
    private PID? _hiveMindHintPid;

    public WorkerNodeActor(
        Guid workerNodeId,
        string workerAddress,
        string? artifactRootPath = null,
        IArtifactStore? artifactStore = null)
    {
        if (workerNodeId == Guid.Empty)
        {
            throw new ArgumentException("Worker node id is required.", nameof(workerNodeId));
        }

        _workerNodeId = workerNodeId;
        _workerAddress = workerAddress ?? string.Empty;
        _artifactStore = artifactStore ?? new LocalArtifactStore(new ArtifactStoreOptions(ResolveArtifactRoot(artifactRootPath)));
    }

    public async Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case DiscoverySnapshotApplied snapshot:
                ApplyDiscoverySnapshot(snapshot);
                break;
            case EndpointRegistrationObserved endpoint:
                ApplyEndpoint(endpoint.Registration);
                break;
            case PlacementAssignmentRequest request:
                await HandlePlacementAssignmentAsync(context, request).ConfigureAwait(false);
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

    public sealed record EndpointRegistrationObserved(ServiceEndpointRegistration Registration);

    public sealed record GetWorkerNodeSnapshot;

    public sealed record WorkerNodeSnapshot(
        Guid WorkerNodeId,
        string WorkerAddress,
        ServiceEndpointRegistration? HiveMindEndpoint,
        ServiceEndpointRegistration? IoGatewayEndpoint,
        int TrackedAssignmentCount);

    private void ApplyDiscoverySnapshot(DiscoverySnapshotApplied snapshot)
    {
        if (snapshot.Registrations is null)
        {
            return;
        }

        foreach (var entry in snapshot.Registrations)
        {
            ApplyEndpoint(entry.Value);
        }
    }

    private void ApplyEndpoint(ServiceEndpointRegistration registration)
    {
        if (!ServiceEndpointSettings.IsKnownKey(registration.Key))
        {
            return;
        }

        _endpoints[registration.Key] = registration;
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

        if (context.Sender is not null
            && !string.IsNullOrWhiteSpace(context.Sender.Id)
            && !context.Sender.Id.StartsWith("$", StringComparison.Ordinal))
        {
            _hiveMindHintPid = context.Sender;
        }

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

        await EnsureRuntimeInfoAsync(context, brain).ConfigureAwait(false);

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

    private void HandlePlacementReconcile(IContext context, PlacementReconcileRequest request)
    {
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

        context.Respond(report);
    }

    private WorkerNodeSnapshot BuildSnapshot()
    {
        _endpoints.TryGetValue(ServiceEndpointSettings.HiveMindKey, out var hiveMindEndpoint);
        _endpoints.TryGetValue(ServiceEndpointSettings.IoGatewayKey, out var ioEndpoint);
        return new WorkerNodeSnapshot(
            _workerNodeId,
            _workerAddress,
            hiveMindEndpoint,
            ioEndpoint,
            _assignments.Count);
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
            return HostingResult.Failed(FailedAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureInternalError,
                ex.GetBaseException().Message));
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
            try
            {
                var nbnRef = runtime.BaseDefinition!.Clone();
                var nbsRef = HasArtifactRef(runtime.LastSnapshot) ? runtime.LastSnapshot!.Clone() : null;
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
                runtime.LastArtifactLoadError = ex.GetBaseException().Message;
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
        context.Request(hiveMindPid, new ProtoControl.RegisterShard
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
    {
        if (brain.OutputCoordinatorPid is null)
        {
            return;
        }

        var hiveMindPid = ResolveHiveMindPid(context);
        if (hiveMindPid is null)
        {
            return;
        }

        context.Request(hiveMindPid, new ProtoControl.RegisterOutputSink
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            OutputPid = PidLabel(ToRemotePid(context, brain.OutputCoordinatorPid))
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
            return;
        }

        try
        {
            var info = await context.RequestAsync<BrainInfo>(
                ioPid,
                new BrainInfoRequest
                {
                    BrainId = brain.BrainId.ToProtoUuid()
                },
                BrainInfoTimeout).ConfigureAwait(false);
            if (info is null)
            {
                return;
            }

            if (info.InputWidth > 0)
            {
                brain.RuntimeInfo.InputWidth = Math.Max(brain.RuntimeInfo.InputWidth, checked((int)info.InputWidth));
            }

            if (info.OutputWidth > 0)
            {
                brain.RuntimeInfo.OutputWidth = Math.Max(brain.RuntimeInfo.OutputWidth, checked((int)info.OutputWidth));
            }

            if (HasArtifactRef(info.BaseDefinition))
            {
                brain.RuntimeInfo.BaseDefinition = info.BaseDefinition.Clone();
            }

            if (HasArtifactRef(info.LastSnapshot))
            {
                brain.RuntimeInfo.LastSnapshot = info.LastSnapshot.Clone();
            }

            brain.RuntimeInfo.HasIoMetadata = true;
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
        context.Respond(ack);
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
        context.Respond(BuildReadyAck(assignment, message));
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

    private PID? ResolveHiveMindPid(IContext context)
    {
        if (TryResolveEndpointPid(ServiceEndpointSettings.HiveMindKey, out var endpointPid))
        {
            return endpointPid;
        }

        if (_hiveMindHintPid is not null)
        {
            return _hiveMindHintPid;
        }

        return null;
    }

    private bool TryResolveEndpointPid(string key, out PID pid)
    {
        if (_endpoints.TryGetValue(key, out var registration))
        {
            pid = registration.Endpoint.ToPid();
            return true;
        }

        pid = new PID();
        return false;
    }

    private PID ToRemotePid(IContext context, PID pid)
    {
        if (!string.IsNullOrWhiteSpace(pid.Address))
        {
            return pid;
        }

        if (!string.IsNullOrWhiteSpace(_workerAddress))
        {
            return new PID(_workerAddress, pid.Id);
        }

        var systemAddress = context.System.Address;
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
