using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.HiveMind;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Runtime.WorkerNode;

/// <summary>
/// Hosts region shards and related per-brain runtime actors on a worker node.
/// </summary>
public sealed partial class WorkerNodeActor : IActor
{
    private static readonly TimeSpan BrainInfoTimeout = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan BrainDefinitionTimeout = TimeSpan.FromMilliseconds(750);
    private static readonly bool LogRuntimeMetadataDiagnostics = IsEnvTrue("NBN_RUNTIME_METADATA_DIAGNOSTICS_ENABLED");
    private static readonly int RuntimeMetadataMaxAttempts = 6;
    private static readonly TimeSpan RuntimeMetadataRetryDelay = TimeSpan.FromMilliseconds(150);
    private const string DiscoveryTargetLabel = "discovery";

    private readonly Guid _workerNodeId;
    private readonly string _workerAddress;
    private readonly IArtifactStore _artifactStore;
    private readonly string _defaultArtifactRootPath;
    private readonly ArtifactStoreResolver _artifactStoreResolver;
    private readonly Dictionary<string, ServiceEndpointRegistration> _endpoints = new(StringComparer.Ordinal);
    private readonly Dictionary<string, HostedAssignmentState> _assignments = new(StringComparer.Ordinal);
    private readonly Dictionary<Guid, BrainHostingState> _brains = new();
    private readonly WorkerServiceRole _enabledRoles;
    private readonly Action? _capabilityProfileChanged;
    private readonly Func<ProtoSettings.NodeCapabilities>? _capabilitySnapshotProvider;
    private readonly WorkerResourceAvailability _resourceAvailability;
    private readonly string? _observabilityDefaultHost;
    private readonly Severity _debugMinSeverityDefault;
    private readonly bool _debugStreamEnabledDefault;

    private PID? _hiveMindHintPid;

    /// <summary>
    /// Initializes a worker node actor with the local hosting, artifact, and observability dependencies.
    /// </summary>
    public WorkerNodeActor(
        Guid workerNodeId,
        string workerAddress,
        string? artifactRootPath = null,
        IArtifactStore? artifactStore = null,
        WorkerServiceRole enabledRoles = WorkerServiceRole.All,
        Action? capabilityProfileChanged = null,
        Func<ProtoSettings.NodeCapabilities>? capabilitySnapshotProvider = null,
        WorkerResourceAvailability? resourceAvailability = null,
        string? observabilityDefaultHost = null)
    {
        if (workerNodeId == Guid.Empty)
        {
            throw new ArgumentException("Worker node id is required.", nameof(workerNodeId));
        }

        _workerNodeId = workerNodeId;
        _workerAddress = workerAddress ?? string.Empty;
        _defaultArtifactRootPath = ResolveArtifactRoot(artifactRootPath);
        _artifactStore = artifactStore ?? new LocalArtifactStore(new ArtifactStoreOptions(_defaultArtifactRootPath));
        _artifactStoreResolver = new ArtifactStoreResolver(new ArtifactStoreResolverOptions(_defaultArtifactRootPath));
        _enabledRoles = WorkerServiceRoles.Sanitize(enabledRoles);
        _capabilityProfileChanged = capabilityProfileChanged;
        _capabilitySnapshotProvider = capabilitySnapshotProvider;
        _resourceAvailability = resourceAvailability ?? WorkerResourceAvailability.Default;
        _observabilityDefaultHost = string.IsNullOrWhiteSpace(observabilityDefaultHost)
            ? null
            : observabilityDefaultHost.Trim();
        _debugStreamEnabledDefault = ResolveDebugStreamEnabled(defaultValue: false);
        _debugMinSeverityDefault = ResolveDebugMinSeverity(Severity.SevDebug);
    }

    /// <summary>
    /// Processes worker-node control, discovery, and debug messages.
    /// </summary>
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
            case PlacementPeerLatencyRequest request:
                await HandlePlacementPeerLatencyAsync(context, request).ConfigureAwait(false);
                break;
            case PlacementLatencyEchoRequest:
                context.Respond(new PlacementLatencyEchoAck());
                break;
            case WorkerCapabilityRefreshRequest request:
                HandleWorkerCapabilityRefresh(context, request);
                break;
            case TickComputeDone computeDone:
                ForwardTickCompletion(context, computeDone);
                break;
            case TickDeliverDone deliverDone:
                ForwardTickCompletion(context, deliverDone);
                break;
            case GetWorkerNodeSnapshot:
                context.Respond(BuildSnapshot());
                break;
            case GetHostedBrainSnapshot request:
                context.Respond(BuildHostedBrainSnapshot(request.BrainId));
                break;
            case GetHostedRegionShardBackendExecutionInfo request:
                await HandleGetHostedRegionShardBackendExecutionInfoAsync(context, request).ConfigureAwait(false);
                break;
            case Terminated terminated:
                HandleTerminated(terminated);
                break;
        }
    }

    private void ForwardTickCompletion(IContext context, object completion)
    {
        var hiveMindPid = ResolveHiveMindPid(context);
        if (hiveMindPid is null)
        {
            return;
        }

        TryRequest(context, hiveMindPid, completion);
    }
}
