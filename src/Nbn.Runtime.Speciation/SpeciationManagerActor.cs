using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Shared;
using Proto;
using ProtoIo = Nbn.Proto.Io;
using ProtoRepro = Nbn.Proto.Repro;
using ProtoSettings = Nbn.Proto.Settings;
using ProtoSpec = Nbn.Proto.Speciation;
using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Nbn.Runtime.Speciation;

/// <summary>
/// Owns speciation epoch state, membership assignment, and lineage reconciliation for running brains.
/// </summary>
public sealed partial class SpeciationManagerActor : IActor
{
    private static readonly TimeSpan DefaultSettingsRequestTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan DefaultCompatibilityRequestTimeout = TimeSpan.FromSeconds(5);
    // Once a mutation enters actor-ordered commit flow, let the store finish or fail atomically.
    private static readonly CancellationToken StoreMutationCancellationToken = CancellationToken.None;
    private static readonly JsonSerializerOptions MetadataJsonSerializerOptions = new()
    {
        WriteIndented = false
    };
    private const int ExternalAdmissionExemplarLimit = 3;
    private const int DerivedSpeciesBootstrapActualSampleRequirement = 3;
    private const int DerivedSpeciesBootstrapMembershipLimit = DerivedSpeciesBootstrapActualSampleRequirement;

    private readonly SpeciationStore _store;
    private SpeciationRuntimeConfig _runtimeConfig;
    private SpeciationAssignmentPolicy _assignmentPolicy;
    private readonly PID? _settingsPid;
    private readonly TimeSpan _settingsRequestTimeout;
    private readonly PID? _configuredReproductionManagerPid;
    private PID? _reproductionManagerPid;
    private readonly PID? _configuredIoGatewayPid;
    private PID? _ioGatewayPid;
    private readonly TimeSpan _compatibilityRequestTimeout;
    private ProtoRepro.ReproduceConfig _compatibilityAssessmentConfig;
    private readonly Dictionary<string, SpeciesSimilarityFloorState> _speciesSimilarityFloors = new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<RecentDerivedSpeciesHint>> _recentDerivedSpeciesHintsBySourceSpecies = new(StringComparer.Ordinal);
    private readonly Dictionary<string, RecentDerivedSpeciesHint> _recentDerivedSpeciesHintsByTargetSpecies = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _speciesDisplayNamesBySpeciesId = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _rootSpeciesOrdinalsBySpeciesId = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _maxRootSpeciesOrdinalByStem = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _decisionProcessingGate = new(1, 1);

    private bool _initializing;
    private bool _initialized;
    private SpeciationEpochInfo? _currentEpoch;

    /// <summary>
    /// Initializes the single-writer speciation actor with persistence and optional runtime service endpoint hints.
    /// </summary>
    public SpeciationManagerActor(
        SpeciationStore store,
        SpeciationRuntimeConfig runtimeConfig,
        PID? settingsPid,
        TimeSpan? settingsRequestTimeout = null,
        PID? reproductionManagerPid = null,
        PID? ioGatewayPid = null,
        TimeSpan? compatibilityRequestTimeout = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _runtimeConfig = runtimeConfig ?? throw new ArgumentNullException(nameof(runtimeConfig));
        _assignmentPolicy = BuildAssignmentPolicy(runtimeConfig);
        _settingsPid = settingsPid;
        _settingsRequestTimeout = settingsRequestTimeout ?? DefaultSettingsRequestTimeout;
        _configuredReproductionManagerPid = reproductionManagerPid;
        _reproductionManagerPid = reproductionManagerPid;
        _configuredIoGatewayPid = ioGatewayPid;
        _ioGatewayPid = ioGatewayPid;
        _compatibilityRequestTimeout = compatibilityRequestTimeout ?? DefaultCompatibilityRequestTimeout;
        _compatibilityAssessmentConfig = ReproductionSettings.CreateDefaultConfig(
            ProtoRepro.SpawnChildPolicy.SpawnChildNever);
    }

    private sealed record ApplySplitHindsightReassignmentsRequest(
        SpeciationEpochInfo Epoch,
        SpeciationMembershipRecord SplitMembership,
        AssignmentResolution AssignmentResolution,
        double AssignmentSimilarityScore,
        string PolicyVersion,
        long? DecisionTimeMs);

    /// <summary>
    /// Dispatches speciation control-plane, protobuf contract, startup, and discovery messages.
    /// </summary>
    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
                HandleStarted(context);
                break;
            case SpeciationStatusRequest:
                HandleStatusRequest(context);
                break;
            case SpeciationGetCurrentEpochRequest:
                HandleCurrentEpochRequest(context);
                break;
            case SpeciationAssignMembershipRequest message:
                HandleAssignMembership(context, message);
                break;
            case SpeciationResetEpochRequest message:
                HandleResetEpoch(context, message);
                break;
            case SpeciationReconcileKnownBrainsRequest message:
                HandleReconcileKnownBrains(context, message);
                break;
            case SpeciationListMembershipsRequest message:
                HandleListMemberships(context, message);
                break;
            case SpeciationRecordLineageEdgeRequest message:
                HandleRecordLineageEdge(context, message);
                break;
            case ProtoSpec.SpeciationStatusRequest message:
                HandleProtoStatus(context, message);
                break;
            case ProtoSpec.SpeciationGetConfigRequest message:
                HandleProtoGetConfig(context, message);
                break;
            case ProtoSpec.SpeciationSetConfigRequest message:
                HandleProtoSetConfig(context, message);
                break;
            case ProtoSpec.SpeciationResetAllRequest message:
                HandleProtoResetAll(context, message);
                break;
            case ProtoSpec.SpeciationDeleteEpochRequest message:
                HandleProtoDeleteEpoch(context, message);
                break;
            case ProtoSpec.SpeciationEvaluateRequest message:
                HandleProtoEvaluate(context, message);
                break;
            case ProtoSpec.SpeciationAssignRequest message:
                HandleProtoAssign(context, message);
                break;
            case ProtoSpec.SpeciationBatchEvaluateApplyRequest message:
                HandleProtoBatchEvaluateApply(context, message);
                break;
            case ProtoSpec.SpeciationListMembershipsRequest message:
                HandleProtoListMemberships(context, message);
                break;
            case ProtoSpec.SpeciationQueryMembershipRequest message:
                HandleProtoQueryMembership(context, message);
                break;
            case ProtoSpec.SpeciationListHistoryRequest message:
                HandleProtoListHistory(context, message);
                break;
            case ApplySplitHindsightReassignmentsRequest message:
                HandleApplySplitHindsightReassignments(context, message);
                break;
            case DiscoverySnapshotApplied snapshot:
                ApplyDiscoverySnapshot(snapshot);
                break;
            case EndpointStateObserved observed:
                ApplyObservedEndpoint(observed.Observation);
                break;
        }

        return Task.CompletedTask;
    }
}
