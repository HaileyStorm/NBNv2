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

public sealed class SpeciationManagerActor : IActor
{
    private static readonly TimeSpan DefaultSettingsRequestTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan DefaultCompatibilityRequestTimeout = TimeSpan.FromSeconds(5);
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

    private bool _initializing;
    private bool _initialized;
    private SpeciationEpochInfo? _currentEpoch;

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

    public sealed record DiscoverySnapshotApplied(IReadOnlyDictionary<string, ServiceEndpointRegistration> Registrations);

    public sealed record EndpointStateObserved(ServiceEndpointObservation Observation);

    private sealed record ApplySplitHindsightReassignmentsRequest(
        SpeciationEpochInfo Epoch,
        SpeciationMembershipRecord SplitMembership,
        AssignmentResolution AssignmentResolution,
        double AssignmentSimilarityScore,
        string PolicyVersion,
        long? DecisionTimeMs);

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

    private void HandleStarted(IContext context)
    {
        if (_initializing || _initialized)
        {
            return;
        }

        _initializing = true;
        var activity = SpeciationTelemetry.StartEpochTransitionActivity("initialize", previousEpochId: 0);
        var initializeTask = InitializeStoreAsync(context);
        context.ReenterAfter(initializeTask, completed =>
        {
            using (activity)
            {
                RecordEpochTransitionTelemetry(
                    activity,
                    "initialize",
                    completed.IsFaulted ? "failed" : "completed",
                    completed.IsFaulted ? "store_error" : "none",
                    previousEpochId: 0,
                    currentEpochId: completed.IsFaulted ? 0 : completed.Result.EpochId);
            }

            _initializing = false;
            if (completed.IsFaulted)
            {
                LogError($"Speciation startup initialize failed: {completed.Exception?.GetBaseException().Message}");
                return Task.CompletedTask;
            }

            _currentEpoch = completed.Result;
            _initialized = true;
            StartStartupReconciliation(context);
            return Task.CompletedTask;
        });
    }

    private async Task<SpeciationEpochInfo> InitializeStoreAsync(IContext context)
    {
        await _store.InitializeAsync().ConfigureAwait(false);
        _runtimeConfig = await ResolveRuntimeConfigFromSettingsAsync(context, _runtimeConfig).ConfigureAwait(false);
        _assignmentPolicy = BuildAssignmentPolicy(_runtimeConfig);
        _compatibilityAssessmentConfig = await ResolveCompatibilityAssessmentConfigFromSettingsAsync(
            context,
            _compatibilityAssessmentConfig).ConfigureAwait(false);
        var epoch = await _store.EnsureCurrentEpochAsync(_runtimeConfig).ConfigureAwait(false);
        await PrimeSpeciesSimilarityFloorsAsync(epoch.EpochId).ConfigureAwait(false);
        return epoch;
    }

    private async Task<SpeciationRuntimeConfig> ResolveRuntimeConfigFromSettingsAsync(
        IContext context,
        SpeciationRuntimeConfig fallback)
    {
        if (_settingsPid is null)
        {
            return fallback;
        }

        var settingValues = await ReadSettingValuesAsync(
            context,
            SpeciationSettingsKeys.AllKeys,
            "Speciation startup").ConfigureAwait(false);

        return settingValues.Count == 0
            ? fallback
            : BuildRuntimeConfigFromSettings(settingValues, fallback);
    }

    private async Task<ProtoRepro.ReproduceConfig> ResolveCompatibilityAssessmentConfigFromSettingsAsync(
        IContext context,
        ProtoRepro.ReproduceConfig fallback)
    {
        if (_settingsPid is null)
        {
            return fallback.Clone();
        }

        var settingValues = await ReadSettingValuesAsync(
            context,
            ReproductionSettingsKeys.AllKeys,
            "Speciation compatibility config").ConfigureAwait(false);
        return settingValues.Count == 0
            ? fallback.Clone()
            : ReproductionSettings.CreateConfigFromSettings(
                settingValues.ToDictionary(
                    static pair => pair.Key,
                    static pair => (string?)pair.Value,
                    StringComparer.OrdinalIgnoreCase),
                ProtoRepro.SpawnChildPolicy.SpawnChildNever);
    }

    private async Task<Dictionary<string, string>> ReadSettingValuesAsync(
        IContext context,
        IReadOnlyList<string> keys,
        string logContext)
    {
        var settingValues = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (_settingsPid is null)
        {
            return settingValues;
        }

        foreach (var key in keys)
        {
            try
            {
                var setting = await context.RequestAsync<ProtoSettings.SettingValue>(
                    _settingsPid,
                    new ProtoSettings.SettingGet
                    {
                        Key = key
                    },
                    _settingsRequestTimeout).ConfigureAwait(false);
                if (!string.IsNullOrWhiteSpace(setting?.Value))
                {
                    settingValues[key] = setting.Value.Trim();
                }
            }
            catch (Exception ex)
            {
                LogError($"{logContext} settings read failed for '{key}': {ex.GetBaseException().Message}");
            }
        }

        return settingValues;
    }

    private void StartStartupReconciliation(IContext context)
    {
        if (_settingsPid is null || !_initialized || _currentEpoch is null)
        {
            return;
        }

        var epochId = _currentEpoch.EpochId;
        var activity = SpeciationTelemetry.StartStartupReconcileActivity(epochId);
        var brainListTask = context.RequestAsync<ProtoSettings.BrainListResponse>(
            _settingsPid,
            new ProtoSettings.BrainListRequest(),
            _settingsRequestTimeout);

        context.ReenterAfter(brainListTask, completed =>
        {
            if (completed.IsFaulted)
            {
                using (activity)
                {
                    RecordStartupReconcileTelemetry(
                        activity,
                        epochId,
                        knownBrains: 0,
                        result: null,
                        outcome: "failed",
                        failureReason: "settings_request_failed");
                }

                LogError($"Speciation startup reconcile skipped: failed to fetch BrainList from SettingsMonitor: {completed.Exception?.GetBaseException().Message}");
                return Task.CompletedTask;
            }

            var knownBrains = ParseKnownBrainIds(completed.Result);
            if (knownBrains.Count == 0 || _currentEpoch is null)
            {
                using (activity)
                {
                    RecordStartupReconcileTelemetry(
                        activity,
                        epochId,
                        knownBrains.Count,
                        new SpeciationReconcileResult(epochId, 0, 0, Array.Empty<Guid>()),
                        outcome: "completed",
                        failureReason: "none");
                }

                return Task.CompletedTask;
            }

            var reconcileTask = _store.ReconcileMissingMembershipsAsync(
                _currentEpoch.EpochId,
                knownBrains,
                _runtimeConfig,
                decisionMetadataJson: "{\"source\":\"startup_reconcile\"}");

            context.ReenterAfter(reconcileTask, reconcileCompleted =>
            {
                using (activity)
                {
                    if (reconcileCompleted.IsFaulted)
                    {
                        RecordStartupReconcileTelemetry(
                            activity,
                            epochId,
                            knownBrains.Count,
                            result: null,
                            outcome: "failed",
                            failureReason: "store_error");
                    }
                    else
                    {
                        RecordStartupReconcileTelemetry(
                            activity,
                            epochId,
                            knownBrains.Count,
                            reconcileCompleted.Result,
                            outcome: "completed",
                            failureReason: "none");
                    }
                }

                if (reconcileCompleted.IsFaulted)
                {
                    LogError($"Speciation startup reconcile failed: {reconcileCompleted.Exception?.GetBaseException().Message}");
                    return Task.CompletedTask;
                }

                var reconcileResult = reconcileCompleted.Result;
                if (reconcileResult.AddedMemberships > 0)
                {
                    IncrementSpeciesMembershipCount(_runtimeConfig.DefaultSpeciesId, reconcileResult.AddedMemberships);
                }

                return Task.CompletedTask;
            });

            return Task.CompletedTask;
        });
    }

    private static IReadOnlyList<Guid> ParseKnownBrainIds(ProtoSettings.BrainListResponse response)
    {
        if (response is null || response.Brains.Count == 0)
        {
            return Array.Empty<Guid>();
        }

        return response.Brains
            .Where(static brain => brain.BrainId is not null && brain.BrainId.TryToGuid(out _))
            .Select(static brain => brain.BrainId.ToGuid())
            .Where(static brainId => brainId != Guid.Empty)
            .Distinct()
            .OrderBy(static brainId => brainId)
            .ToArray();
    }

    private void HandleStatusRequest(IContext context)
    {
        if (!_initialized || _currentEpoch is null)
        {
            context.Respond(new SpeciationStatusResponse(new SpeciationStatusSnapshot(0, 0, 0, 0)));
            return;
        }

        var statusTask = _store.GetStatusAsync(_currentEpoch.EpochId);
        context.ReenterAfter(statusTask, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"Speciation status request failed: {completed.Exception?.GetBaseException().Message}");
                context.Respond(new SpeciationStatusResponse(new SpeciationStatusSnapshot(_currentEpoch.EpochId, 0, 0, 0)));
                return Task.CompletedTask;
            }

            context.Respond(new SpeciationStatusResponse(completed.Result));
            return Task.CompletedTask;
        });
    }

    private void HandleCurrentEpochRequest(IContext context)
    {
        context.Respond(new SpeciationGetCurrentEpochResponse(_currentEpoch ?? CreateFallbackEpoch()));
    }

    private void HandleAssignMembership(IContext context, SpeciationAssignMembershipRequest message)
    {
        if (!TryGetCurrentEpoch(out var epoch))
        {
            context.Respond(new SpeciationAssignMembershipResponse(false, false, false, "service_initializing", null));
            return;
        }

        var assignTask = _store.TryAssignMembershipAsync(
            epoch.EpochId,
            message.Assignment,
            message.DecisionTimeMs);

        context.ReenterAfter(assignTask, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"Speciation assign membership failed: {completed.Exception?.GetBaseException().Message}");
                context.Respond(new SpeciationAssignMembershipResponse(false, false, false, "store_error", null));
                return Task.CompletedTask;
            }

            var outcome = completed.Result;
            if (outcome.Created)
            {
                RecordCommittedMembership(outcome.Membership);
            }
            var success = !outcome.ImmutableConflict;
            var failureReason = outcome.ImmutableConflict ? "membership_immutable" : "none";
            context.Respond(new SpeciationAssignMembershipResponse(
                success,
                outcome.Created,
                outcome.ImmutableConflict,
                failureReason,
                outcome.Membership));
            return Task.CompletedTask;
        });
    }

    private void HandleResetEpoch(IContext context, SpeciationResetEpochRequest message)
    {
        var previousEpoch = _currentEpoch ?? CreateFallbackEpoch();
        if (!_initialized)
        {
            context.Respond(new SpeciationResetEpochResponse(previousEpoch, previousEpoch));
            return;
        }

        var nextConfig = BuildResetRuntimeConfig(message);
        var resetTask = _store.ResetEpochAsync(nextConfig, message.ResetTimeMs);
        context.ReenterAfter(resetTask, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"Speciation reset epoch failed: {completed.Exception?.GetBaseException().Message}");
                context.Respond(new SpeciationResetEpochResponse(previousEpoch, previousEpoch));
                return Task.CompletedTask;
            }

            _runtimeConfig = nextConfig;
            _assignmentPolicy = BuildAssignmentPolicy(nextConfig);
            _currentEpoch = completed.Result;
            ResetSpeciesSimilarityFloors();
            context.Respond(new SpeciationResetEpochResponse(previousEpoch, completed.Result));
            return Task.CompletedTask;
        });
    }

    private void HandleReconcileKnownBrains(IContext context, SpeciationReconcileKnownBrainsRequest message)
    {
        if (!TryGetCurrentEpoch(out var epoch))
        {
            context.Respond(new SpeciationReconcileKnownBrainsResponse(new SpeciationReconcileResult(0, 0, 0, Array.Empty<Guid>())));
            return;
        }

        var runtimeConfig = BuildReconcileRuntimeConfig(message);
        var metadataJson = string.IsNullOrWhiteSpace(message.DecisionMetadataJson)
            ? "{\"source\":\"manual_reconcile\"}"
            : message.DecisionMetadataJson;

        var reconcileTask = _store.ReconcileMissingMembershipsAsync(
            epoch.EpochId,
            message.BrainIds ?? Array.Empty<Guid>(),
            runtimeConfig,
            metadataJson,
            message.DecisionTimeMs);

        context.ReenterAfter(reconcileTask, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"Speciation reconcile known brains failed: {completed.Exception?.GetBaseException().Message}");
                context.Respond(new SpeciationReconcileKnownBrainsResponse(new SpeciationReconcileResult(epoch.EpochId, 0, 0, Array.Empty<Guid>())));
                return Task.CompletedTask;
            }

            var result = completed.Result;
            if (result.AddedMemberships > 0)
            {
                IncrementSpeciesMembershipCount(runtimeConfig.DefaultSpeciesId, result.AddedMemberships);
            }

            context.Respond(new SpeciationReconcileKnownBrainsResponse(result));
            return Task.CompletedTask;
        });
    }

    private void HandleListMemberships(IContext context, SpeciationListMembershipsRequest message)
    {
        if (!_initialized)
        {
            context.Respond(new SpeciationListMembershipsResponse(Array.Empty<SpeciationMembershipRecord>()));
            return;
        }

        var listTask = _store.ListMembershipsAsync(message.EpochId);
        context.ReenterAfter(listTask, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"Speciation list memberships failed: {completed.Exception?.GetBaseException().Message}");
                context.Respond(new SpeciationListMembershipsResponse(Array.Empty<SpeciationMembershipRecord>()));
                return Task.CompletedTask;
            }

            context.Respond(new SpeciationListMembershipsResponse(completed.Result));
            return Task.CompletedTask;
        });
    }

    private void HandleRecordLineageEdge(IContext context, SpeciationRecordLineageEdgeRequest message)
    {
        if (!TryGetCurrentEpoch(out var epoch))
        {
            context.Respond(new SpeciationRecordLineageEdgeResponse(false, "service_initializing"));
            return;
        }

        if (message.ParentBrainId == Guid.Empty || message.ChildBrainId == Guid.Empty)
        {
            context.Respond(new SpeciationRecordLineageEdgeResponse(false, "invalid_brain_id"));
            return;
        }

        if (message.ParentBrainId == message.ChildBrainId)
        {
            context.Respond(new SpeciationRecordLineageEdgeResponse(false, "self_edge_disallowed"));
            return;
        }

        var recordTask = _store.RecordLineageEdgeAsync(
            epoch.EpochId,
            message.ParentBrainId,
            message.ChildBrainId,
            message.MetadataJson,
            message.CreatedMs);

        context.ReenterAfter(recordTask, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"Speciation record lineage edge failed: {completed.Exception?.GetBaseException().Message}");
                context.Respond(new SpeciationRecordLineageEdgeResponse(false, "store_error"));
                return Task.CompletedTask;
            }

            context.Respond(new SpeciationRecordLineageEdgeResponse(true, "none"));
            return Task.CompletedTask;
        });
    }

    private void HandleProtoStatus(IContext context, ProtoSpec.SpeciationStatusRequest _)
    {
        var epoch = _currentEpoch ?? CreateFallbackEpoch();
        var config = ToProtoRuntimeConfig(_runtimeConfig);

        if (!_initialized || _currentEpoch is null)
        {
            context.Respond(new ProtoSpec.SpeciationStatusResponse
            {
                FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceInitializing,
                FailureDetail = "Speciation service is still initializing.",
                Status = new ProtoSpec.SpeciationStatusSnapshot(),
                CurrentEpoch = ToProtoEpochInfo(epoch),
                Config = config
            });
            return;
        }

        var statusTask = _store.GetStatusAsync(epoch.EpochId);
        context.ReenterAfter(statusTask, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"Speciation proto status request failed: {completed.Exception?.GetBaseException().Message}");
                context.Respond(new ProtoSpec.SpeciationStatusResponse
                {
                    FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureStoreError,
                    FailureDetail = "Failed to load status from speciation store.",
                    Status = new ProtoSpec.SpeciationStatusSnapshot
                    {
                        EpochId = (ulong)epoch.EpochId
                    },
                    CurrentEpoch = ToProtoEpochInfo(epoch),
                    Config = config
                });
                return Task.CompletedTask;
            }

            context.Respond(new ProtoSpec.SpeciationStatusResponse
            {
                FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
                FailureDetail = string.Empty,
                Status = ToProtoStatusSnapshot(completed.Result),
                CurrentEpoch = ToProtoEpochInfo(epoch),
                Config = config
            });
            SpeciationTelemetry.RecordStatusSnapshot("status", completed.Result);
            return Task.CompletedTask;
        });
    }

    private void HandleProtoGetConfig(IContext context, ProtoSpec.SpeciationGetConfigRequest _)
    {
        var epoch = _currentEpoch ?? CreateFallbackEpoch();
        var failureReason = _initialized
            ? ProtoSpec.SpeciationFailureReason.SpeciationFailureNone
            : ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceInitializing;

        context.Respond(new ProtoSpec.SpeciationGetConfigResponse
        {
            FailureReason = failureReason,
            FailureDetail = failureReason == ProtoSpec.SpeciationFailureReason.SpeciationFailureNone
                ? string.Empty
                : "Speciation service is still initializing.",
            Config = ToProtoRuntimeConfig(_runtimeConfig),
            CurrentEpoch = ToProtoEpochInfo(epoch)
        });
    }

    private void HandleProtoSetConfig(IContext context, ProtoSpec.SpeciationSetConfigRequest message)
    {
        var nextConfig = FromProtoRuntimeConfig(message.Config, _runtimeConfig);
        var previousEpoch = _currentEpoch ?? CreateFallbackEpoch();

        if (message.StartNewEpoch)
        {
            if (!_initialized)
            {
                context.Respond(CreateProtoSetConfigResponse(
                    ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceInitializing,
                    "Speciation service is still initializing.",
                    previousEpoch,
                    previousEpoch,
                    _runtimeConfig));
                RecordEpochTransitionTelemetry(
                    activity: null,
                    transition: "start_new_epoch",
                    outcome: "rejected",
                    failureReason: "service_initializing",
                    previousEpochId: previousEpoch.EpochId,
                    currentEpochId: previousEpoch.EpochId);
                return;
            }

            var activity = SpeciationTelemetry.StartEpochTransitionActivity("start_new_epoch", previousEpoch.EpochId);
            var applyTime = message.HasApplyTimeMs ? (long?)message.ApplyTimeMs : null;
            var resetTask = _store.ResetEpochAsync(nextConfig, applyTime);
            context.ReenterAfter(resetTask, completed =>
            {
                using (activity)
                {
                    RecordEpochTransitionTelemetry(
                        activity,
                        "start_new_epoch",
                        completed.IsFaulted ? "failed" : "completed",
                        completed.IsFaulted ? "store_error" : "none",
                        previousEpoch.EpochId,
                        completed.IsFaulted ? previousEpoch.EpochId : completed.Result.EpochId);
                }

                if (completed.IsFaulted)
                {
                    LogError($"Speciation proto set config reset failed: {completed.Exception?.GetBaseException().Message}");
                    context.Respond(CreateProtoSetConfigResponse(
                        ProtoSpec.SpeciationFailureReason.SpeciationFailureStoreError,
                        "Failed to persist config as a new epoch.",
                        previousEpoch,
                        previousEpoch,
                        _runtimeConfig));
                    return Task.CompletedTask;
                }

                _runtimeConfig = nextConfig;
                _assignmentPolicy = BuildAssignmentPolicy(nextConfig);
                _currentEpoch = completed.Result;
                ResetSpeciesSimilarityFloors();
                SpeciationTelemetry.RecordStatusSnapshot(
                    "start_new_epoch",
                    new SpeciationStatusSnapshot(completed.Result.EpochId, 0, 0, 0));
                context.Respond(CreateProtoSetConfigResponse(
                    ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
                    string.Empty,
                    previousEpoch,
                    completed.Result,
                    _runtimeConfig));
                return Task.CompletedTask;
            });
            return;
        }

        _runtimeConfig = nextConfig;
        _assignmentPolicy = BuildAssignmentPolicy(nextConfig);
        _currentEpoch = (_currentEpoch ?? previousEpoch) with
        {
            PolicyVersion = nextConfig.PolicyVersion,
            ConfigSnapshotJson = nextConfig.ConfigSnapshotJson
        };

        context.Respond(CreateProtoSetConfigResponse(
            ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
            string.Empty,
            previousEpoch,
            _currentEpoch,
            _runtimeConfig));
    }

    private void HandleProtoResetAll(IContext context, ProtoSpec.SpeciationResetAllRequest message)
    {
        var previousEpoch = _currentEpoch ?? CreateFallbackEpoch();
        var activity = SpeciationTelemetry.StartEpochTransitionActivity("reset_all", previousEpoch.EpochId);
        if (!_initialized || _currentEpoch is null)
        {
            using (activity)
            {
                RecordEpochTransitionTelemetry(
                    activity,
                    "reset_all",
                    "rejected",
                    "service_initializing",
                    previousEpoch.EpochId,
                    previousEpoch.EpochId);
            }

            context.Respond(CreateProtoResetAllResponse(
                ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceInitializing,
                "Speciation service is still initializing.",
                previousEpoch,
                previousEpoch,
                _runtimeConfig,
                deletedEpochCount: 0,
                deletedMembershipCount: 0,
                deletedSpeciesCount: 0,
                deletedDecisionCount: 0,
                deletedLineageEdgeCount: 0));
            return;
        }

        var applyTime = message.HasApplyTimeMs ? (long?)message.ApplyTimeMs : null;
        var resetTask = _store.ResetAllAsync(_runtimeConfig, applyTime);
        context.ReenterAfter(resetTask, completed =>
        {
            using (activity)
            {
                if (completed.IsFaulted)
                {
                    RecordEpochTransitionTelemetry(
                        activity,
                        "reset_all",
                        "failed",
                        "store_error",
                        previousEpoch.EpochId,
                        previousEpoch.EpochId);
                }
                else
                {
                    RecordEpochTransitionTelemetry(
                        activity,
                        "reset_all",
                        "completed",
                        "none",
                        previousEpoch.EpochId,
                        completed.Result.CurrentEpoch.EpochId,
                        completed.Result.DeletedMembershipCount,
                        completed.Result.DeletedSpeciesCount,
                        completed.Result.DeletedDecisionCount,
                        completed.Result.DeletedLineageEdgeCount,
                        completed.Result.DeletedEpochCount);
                }
            }

            if (completed.IsFaulted)
            {
                LogError($"Speciation proto reset-all failed: {completed.Exception?.GetBaseException().Message}");
                context.Respond(CreateProtoResetAllResponse(
                    ProtoSpec.SpeciationFailureReason.SpeciationFailureStoreError,
                    "Failed to clear speciation history.",
                    previousEpoch,
                    previousEpoch,
                    _runtimeConfig,
                    deletedEpochCount: 0,
                    deletedMembershipCount: 0,
                    deletedSpeciesCount: 0,
                    deletedDecisionCount: 0,
                    deletedLineageEdgeCount: 0));
                return Task.CompletedTask;
            }

            var outcome = completed.Result;
            _currentEpoch = outcome.CurrentEpoch;
            ResetSpeciesSimilarityFloors();
            SpeciationTelemetry.RecordStatusSnapshot(
                "reset_all",
                new SpeciationStatusSnapshot(outcome.CurrentEpoch.EpochId, 0, 0, 0));
            context.Respond(CreateProtoResetAllResponse(
                ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
                string.Empty,
                previousEpoch,
                outcome.CurrentEpoch,
                _runtimeConfig,
                deletedEpochCount: outcome.DeletedEpochCount,
                deletedMembershipCount: outcome.DeletedMembershipCount,
                deletedSpeciesCount: outcome.DeletedSpeciesCount,
                deletedDecisionCount: outcome.DeletedDecisionCount,
                deletedLineageEdgeCount: outcome.DeletedLineageEdgeCount));
            return Task.CompletedTask;
        });
    }

    private void HandleProtoDeleteEpoch(IContext context, ProtoSpec.SpeciationDeleteEpochRequest message)
    {
        var currentEpoch = _currentEpoch ?? CreateFallbackEpoch();
        var activity = SpeciationTelemetry.StartEpochTransitionActivity("delete_epoch", currentEpoch.EpochId);
        if (!_initialized || _currentEpoch is null)
        {
            using (activity)
            {
                RecordEpochTransitionTelemetry(
                    activity,
                    "delete_epoch",
                    "rejected",
                    "service_initializing",
                    currentEpoch.EpochId,
                    currentEpoch.EpochId);
            }

            context.Respond(CreateProtoDeleteEpochResponse(
                ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceInitializing,
                "Speciation service is still initializing.",
                epochId: 0,
                deleted: false,
                deletedMembershipCount: 0,
                deletedSpeciesCount: 0,
                deletedDecisionCount: 0,
                deletedLineageEdgeCount: 0,
                currentEpoch));
            return;
        }

        var epochId = (long)message.EpochId;
        if (epochId <= 0)
        {
            using (activity)
            {
                RecordEpochTransitionTelemetry(
                    activity,
                    "delete_epoch",
                    "rejected",
                    "invalid_request",
                    currentEpoch.EpochId,
                    currentEpoch.EpochId);
            }

            context.Respond(CreateProtoDeleteEpochResponse(
                ProtoSpec.SpeciationFailureReason.SpeciationFailureInvalidRequest,
                "Delete epoch requires a positive epoch_id.",
                epochId: 0,
                deleted: false,
                deletedMembershipCount: 0,
                deletedSpeciesCount: 0,
                deletedDecisionCount: 0,
                deletedLineageEdgeCount: 0,
                currentEpoch));
            return;
        }

        if (epochId == _currentEpoch.EpochId)
        {
            using (activity)
            {
                RecordEpochTransitionTelemetry(
                    activity,
                    "delete_epoch",
                    "rejected",
                    "invalid_request",
                    currentEpoch.EpochId,
                    currentEpoch.EpochId);
            }

            context.Respond(CreateProtoDeleteEpochResponse(
                ProtoSpec.SpeciationFailureReason.SpeciationFailureInvalidRequest,
                "Current epoch cannot be deleted.",
                epochId,
                deleted: false,
                deletedMembershipCount: 0,
                deletedSpeciesCount: 0,
                deletedDecisionCount: 0,
                deletedLineageEdgeCount: 0,
                currentEpoch));
            return;
        }

        var deleteTask = _store.DeleteEpochAsync(epochId);
        context.ReenterAfter(deleteTask, completed =>
        {
            if (completed.IsFaulted)
            {
                using (activity)
                {
                    RecordEpochTransitionTelemetry(
                        activity,
                        "delete_epoch",
                        "failed",
                        "store_error",
                        currentEpoch.EpochId,
                        (_currentEpoch ?? currentEpoch).EpochId);
                }

                LogError($"Speciation proto delete epoch failed: {completed.Exception?.GetBaseException().Message}");
                context.Respond(CreateProtoDeleteEpochResponse(
                    ProtoSpec.SpeciationFailureReason.SpeciationFailureStoreError,
                    "Failed to delete requested epoch.",
                    epochId,
                    deleted: false,
                    deletedMembershipCount: 0,
                    deletedSpeciesCount: 0,
                    deletedDecisionCount: 0,
                    deletedLineageEdgeCount: 0,
                    _currentEpoch ?? currentEpoch));
                return Task.CompletedTask;
            }

            var result = completed.Result;
            if (!result.Deleted)
            {
                using (activity)
                {
                    RecordEpochTransitionTelemetry(
                        activity,
                        "delete_epoch",
                        "not_found",
                        "invalid_request",
                        currentEpoch.EpochId,
                        (_currentEpoch ?? currentEpoch).EpochId);
                }

                context.Respond(CreateProtoDeleteEpochResponse(
                    ProtoSpec.SpeciationFailureReason.SpeciationFailureInvalidRequest,
                    $"Epoch {epochId} does not exist.",
                    epochId,
                    deleted: false,
                    deletedMembershipCount: 0,
                    deletedSpeciesCount: 0,
                    deletedDecisionCount: 0,
                    deletedLineageEdgeCount: 0,
                    _currentEpoch ?? currentEpoch));
                return Task.CompletedTask;
            }

            using (activity)
            {
                RecordEpochTransitionTelemetry(
                    activity,
                    "delete_epoch",
                    "completed",
                    "none",
                    currentEpoch.EpochId,
                    (_currentEpoch ?? currentEpoch).EpochId,
                    result.DeletedMembershipCount,
                    result.DeletedSpeciesCount,
                    result.DeletedDecisionCount,
                    result.DeletedLineageEdgeCount);
            }

            context.Respond(CreateProtoDeleteEpochResponse(
                ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
                string.Empty,
                epochId,
                deleted: true,
                deletedMembershipCount: result.DeletedMembershipCount,
                deletedSpeciesCount: result.DeletedSpeciesCount,
                deletedDecisionCount: result.DeletedDecisionCount,
                deletedLineageEdgeCount: result.DeletedLineageEdgeCount,
                _currentEpoch ?? currentEpoch));
            return Task.CompletedTask;
        });
    }

    private void HandleProtoEvaluate(IContext context, ProtoSpec.SpeciationEvaluateRequest message)
    {
        if (!TryGetCurrentEpoch(out var epoch))
        {
            using var initializingActivity = SpeciationTelemetry.StartAssignmentActivity(
                "evaluate",
                epochId: 0,
                ProtoSpec.SpeciationApplyMode.DryRun);
            var decision = CreateDecisionFailure(
                ProtoSpec.SpeciationApplyMode.DryRun,
                ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceInitializing,
                "Speciation service is still initializing.");
            RecordDecisionTelemetry(
                "evaluate",
                epochId: 0,
                durationMs: 0d,
                initializingActivity,
                decision);
            context.Respond(new ProtoSpec.SpeciationEvaluateResponse
            {
                Decision = decision
            });
            return;
        }

        var decisionActivity = SpeciationTelemetry.StartAssignmentActivity(
            "evaluate",
            epoch.EpochId,
            ProtoSpec.SpeciationApplyMode.DryRun);
        var stopwatch = Stopwatch.StartNew();
        var evaluateTask = ProcessProtoDecisionAsync(
            context,
            epoch,
            ProtoSpec.SpeciationApplyMode.DryRun,
            message.Candidate,
            message.Parents,
            message.SpeciesId,
            message.SpeciesDisplayName,
            message.PolicyVersion,
            message.DecisionReason,
            message.DecisionMetadataJson,
            message.HasDecisionTimeMs ? (long?)message.DecisionTimeMs : null,
            commit: false);
        context.ReenterAfter(evaluateTask, completed =>
        {
            ProtoSpec.SpeciationDecision decision;
            if (completed.IsFaulted)
            {
                LogError($"Speciation proto evaluate failed: {completed.Exception?.GetBaseException().Message}");
                decision = CreateDecisionFailure(
                    ProtoSpec.SpeciationApplyMode.DryRun,
                    ProtoSpec.SpeciationFailureReason.SpeciationFailureStoreError,
                    "Failed to evaluate speciation decision.");
                using (decisionActivity)
                {
                    RecordDecisionTelemetry(
                        "evaluate",
                        epoch.EpochId,
                        stopwatch.Elapsed.TotalMilliseconds,
                        decisionActivity,
                        decision);
                }

                context.Respond(new ProtoSpec.SpeciationEvaluateResponse
                {
                    Decision = decision
                });
                return Task.CompletedTask;
            }

            decision = completed.Result;
            using (decisionActivity)
            {
                RecordDecisionTelemetry(
                    "evaluate",
                    epoch.EpochId,
                    stopwatch.Elapsed.TotalMilliseconds,
                    decisionActivity,
                    decision);
            }

            context.Respond(new ProtoSpec.SpeciationEvaluateResponse
            {
                Decision = decision
            });
            return Task.CompletedTask;
        });
    }

    private void HandleProtoAssign(IContext context, ProtoSpec.SpeciationAssignRequest message)
    {
        var applyMode = NormalizeApplyMode(message.ApplyMode);
        if (!TryGetCurrentEpoch(out var epoch))
        {
            using var initializingActivity = SpeciationTelemetry.StartAssignmentActivity("assign", epochId: 0, applyMode);
            var decision = CreateDecisionFailure(
                applyMode,
                ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceInitializing,
                "Speciation service is still initializing.");
            RecordDecisionTelemetry(
                "assign",
                epochId: 0,
                durationMs: 0d,
                initializingActivity,
                decision);
            context.Respond(new ProtoSpec.SpeciationAssignResponse
            {
                Decision = decision
            });
            return;
        }

        var decisionActivity = SpeciationTelemetry.StartAssignmentActivity("assign", epoch.EpochId, applyMode);
        var stopwatch = Stopwatch.StartNew();
        var assignTask = ProcessProtoDecisionAsync(
            context,
            epoch,
            applyMode,
            message.Candidate,
            message.Parents,
            message.SpeciesId,
            message.SpeciesDisplayName,
            message.PolicyVersion,
            message.DecisionReason,
            message.DecisionMetadataJson,
            message.HasDecisionTimeMs ? (long?)message.DecisionTimeMs : null,
            commit: applyMode == ProtoSpec.SpeciationApplyMode.Commit);
        context.ReenterAfter(assignTask, completed =>
        {
            ProtoSpec.SpeciationDecision decision;
            if (completed.IsFaulted)
            {
                LogError($"Speciation proto assign failed: {completed.Exception?.GetBaseException().Message}");
                decision = CreateDecisionFailure(
                    applyMode,
                    ProtoSpec.SpeciationFailureReason.SpeciationFailureStoreError,
                    "Failed to process speciation assignment.");
                using (decisionActivity)
                {
                    RecordDecisionTelemetry(
                        "assign",
                        epoch.EpochId,
                        stopwatch.Elapsed.TotalMilliseconds,
                        decisionActivity,
                        decision);
                }

                context.Respond(new ProtoSpec.SpeciationAssignResponse
                {
                    Decision = decision
                });
                return Task.CompletedTask;
            }

            decision = completed.Result;
            using (decisionActivity)
            {
                RecordDecisionTelemetry(
                    "assign",
                    epoch.EpochId,
                    stopwatch.Elapsed.TotalMilliseconds,
                    decisionActivity,
                    decision);
            }

            context.Respond(new ProtoSpec.SpeciationAssignResponse
            {
                Decision = decision
            });
            return Task.CompletedTask;
        });
    }

    private void HandleApplySplitHindsightReassignments(
        IContext context,
        ApplySplitHindsightReassignmentsRequest message)
    {
        var hindsightTask = ApplySplitHindsightReassignmentsAsync(
            context,
            message.Epoch,
            message.SplitMembership,
            message.AssignmentResolution,
            message.AssignmentSimilarityScore,
            message.PolicyVersion,
            message.DecisionTimeMs);
        context.ReenterAfter(hindsightTask, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError(
                    $"Speciation hindsight reassign failed for epoch={message.Epoch.EpochId} brain={message.SplitMembership.BrainId:D}: {completed.Exception?.GetBaseException().Message}");
                return Task.CompletedTask;
            }

            if (completed.Result <= 0)
            {
                return Task.CompletedTask;
            }

            context.ReenterAfter(
                PrimeSpeciesSimilarityFloorsAsync(message.Epoch.EpochId),
                primed =>
                {
                    if (primed.IsFaulted)
                    {
                        LogError(
                            $"Speciation hindsight floor prime failed for epoch={message.Epoch.EpochId}: {primed.Exception?.GetBaseException().Message}");
                    }

                    return Task.CompletedTask;
                });
            return Task.CompletedTask;
        });
    }

    private void HandleProtoBatchEvaluateApply(IContext context, ProtoSpec.SpeciationBatchEvaluateApplyRequest message)
    {
        var applyMode = NormalizeApplyMode(message.ApplyMode);
        if (!TryGetCurrentEpoch(out var epoch))
        {
            SpeciationTelemetry.RecordAssignmentDecision(
                "batch",
                CreateDecisionFailure(
                    applyMode,
                    ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceInitializing,
                    "Speciation service is still initializing."),
                durationMs: 0d);
            context.Respond(new ProtoSpec.SpeciationBatchEvaluateApplyResponse
            {
                FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceInitializing,
                FailureDetail = "Speciation service is still initializing.",
                ApplyMode = applyMode,
                RequestedCount = (uint)message.Items.Count,
                ProcessedCount = 0,
                CommittedCount = 0
            });
            return;
        }

        var batchTask = ProcessProtoBatchAsync(context, epoch, applyMode, message.Items);
        context.ReenterAfter(batchTask, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"Speciation proto batch evaluate/apply failed: {completed.Exception?.GetBaseException().Message}");
                SpeciationTelemetry.RecordAssignmentDecision(
                    "batch",
                    CreateDecisionFailure(
                        applyMode,
                        ProtoSpec.SpeciationFailureReason.SpeciationFailureStoreError,
                        "Failed to process speciation batch request."),
                    durationMs: 0d);
                context.Respond(new ProtoSpec.SpeciationBatchEvaluateApplyResponse
                {
                    FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureStoreError,
                    FailureDetail = "Failed to process speciation batch request.",
                    ApplyMode = applyMode,
                    RequestedCount = (uint)message.Items.Count,
                    ProcessedCount = 0,
                    CommittedCount = 0
                });
                return Task.CompletedTask;
            }

            context.Respond(completed.Result);
            return Task.CompletedTask;
        });
    }

    private void HandleProtoListMemberships(IContext context, ProtoSpec.SpeciationListMembershipsRequest message)
    {
        if (!_initialized)
        {
            context.Respond(new ProtoSpec.SpeciationListMembershipsResponse
            {
                FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceInitializing,
                FailureDetail = "Speciation service is still initializing."
            });
            return;
        }

        var epochId = message.HasEpochId ? (long?)message.EpochId : null;
        var listTask = _store.ListMembershipsAsync(epochId);
        context.ReenterAfter(listTask, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"Speciation proto list memberships failed: {completed.Exception?.GetBaseException().Message}");
                context.Respond(new ProtoSpec.SpeciationListMembershipsResponse
                {
                    FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureStoreError,
                    FailureDetail = "Failed to list speciation memberships."
                });
                return Task.CompletedTask;
            }

            var response = new ProtoSpec.SpeciationListMembershipsResponse
            {
                FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
                FailureDetail = string.Empty
            };
            response.Memberships.AddRange(completed.Result.Select(ToProtoMembershipRecord));
            context.Respond(response);
            return Task.CompletedTask;
        });
    }

    private void HandleProtoQueryMembership(IContext context, ProtoSpec.SpeciationQueryMembershipRequest message)
    {
        if (!_initialized || _currentEpoch is null)
        {
            context.Respond(new ProtoSpec.SpeciationQueryMembershipResponse
            {
                FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceInitializing,
                FailureDetail = "Speciation service is still initializing.",
                Found = false
            });
            return;
        }

        if (message.BrainId is null || !message.BrainId.TryToGuid(out var brainId) || brainId == Guid.Empty)
        {
            context.Respond(new ProtoSpec.SpeciationQueryMembershipResponse
            {
                FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureInvalidCandidate,
                FailureDetail = "Query requires a non-empty brain_id candidate.",
                Found = false
            });
            return;
        }

        var epochId = message.HasEpochId ? (long)message.EpochId : _currentEpoch.EpochId;
        var queryTask = _store.GetMembershipAsync(epochId, brainId);
        context.ReenterAfter(queryTask, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"Speciation proto query membership failed: {completed.Exception?.GetBaseException().Message}");
                context.Respond(new ProtoSpec.SpeciationQueryMembershipResponse
                {
                    FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureStoreError,
                    FailureDetail = "Failed to query speciation membership.",
                    Found = false
                });
                return Task.CompletedTask;
            }

            var membership = completed.Result;
            var response = new ProtoSpec.SpeciationQueryMembershipResponse
            {
                FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
                FailureDetail = string.Empty,
                Found = membership is not null
            };
            if (membership is not null)
            {
                response.Membership = ToProtoMembershipRecord(membership);
            }

            context.Respond(response);
            return Task.CompletedTask;
        });
    }

    private void HandleProtoListHistory(IContext context, ProtoSpec.SpeciationListHistoryRequest message)
    {
        if (!_initialized)
        {
            context.Respond(new ProtoSpec.SpeciationListHistoryResponse
            {
                FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceInitializing,
                FailureDetail = "Speciation service is still initializing.",
                TotalRecords = 0
            });
            return;
        }

        var epochId = message.HasEpochId ? (long?)message.EpochId : null;
        var filterBrainId = message.HasBrainId
                            && message.BrainId is not null
                            && message.BrainId.TryToGuid(out var parsedBrainId)
                            && parsedBrainId != Guid.Empty
            ? parsedBrainId
            : (Guid?)null;
        var limit = (int)Math.Min(int.MaxValue, Math.Max(1u, message.Limit));
        var offset = (int)Math.Min(int.MaxValue, message.Offset);
        var listTask = _store.ListHistoryPageAsync(
            epochId,
            filterBrainId,
            limit,
            offset);
        context.ReenterAfter(listTask, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"Speciation proto list history failed: {completed.Exception?.GetBaseException().Message}");
                context.Respond(new ProtoSpec.SpeciationListHistoryResponse
                {
                    FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureStoreError,
                    FailureDetail = "Failed to load speciation history.",
                    TotalRecords = 0
                });
                return Task.CompletedTask;
            }

            var page = completed.Result;

            var response = new ProtoSpec.SpeciationListHistoryResponse
            {
                FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
                FailureDetail = string.Empty,
                TotalRecords = (uint)Math.Max(0, page.TotalRecords)
            };
            response.History.AddRange(page.Records.Select(ToProtoMembershipRecord));
            context.Respond(response);
            return Task.CompletedTask;
        });
    }

    private async Task<ProtoSpec.SpeciationBatchEvaluateApplyResponse> ProcessProtoBatchAsync(
        IContext context,
        SpeciationEpochInfo epoch,
        ProtoSpec.SpeciationApplyMode requestApplyMode,
        IEnumerable<ProtoSpec.SpeciationBatchItem> items)
    {
        var ordered = items?.ToArray() ?? Array.Empty<ProtoSpec.SpeciationBatchItem>();
        var response = new ProtoSpec.SpeciationBatchEvaluateApplyResponse
        {
            FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
            FailureDetail = string.Empty,
            ApplyMode = requestApplyMode,
            RequestedCount = (uint)ordered.Length
        };

        uint committed = 0;
        for (var index = 0; index < ordered.Length; index++)
        {
            var item = ordered[index] ?? new ProtoSpec.SpeciationBatchItem();
            var itemMode = requestApplyMode;
            if (item.HasApplyModeOverride)
            {
                itemMode = NormalizeApplyMode(item.ApplyModeOverride);
            }

            var stopwatch = Stopwatch.StartNew();
            var decision = await ProcessProtoDecisionAsync(
                context,
                epoch,
                itemMode,
                item.Candidate,
                item.Parents,
                item.SpeciesId,
                item.SpeciesDisplayName,
                item.PolicyVersion,
                item.DecisionReason,
                item.DecisionMetadataJson,
                item.HasDecisionTimeMs ? (long?)item.DecisionTimeMs : null,
                commit: itemMode == ProtoSpec.SpeciationApplyMode.Commit).ConfigureAwait(false);
            SpeciationTelemetry.RecordAssignmentDecision(
                "batch",
                decision,
                stopwatch.Elapsed.TotalMilliseconds);

            if (decision.Committed)
            {
                committed++;
            }

            response.Results.Add(new ProtoSpec.SpeciationBatchItemResult
            {
                ItemId = string.IsNullOrWhiteSpace(item.ItemId) ? $"item-{index}" : item.ItemId.Trim(),
                Decision = decision
            });
        }

        response.ProcessedCount = (uint)response.Results.Count;
        response.CommittedCount = committed;
        return response;
    }

    private async Task<ProtoSpec.SpeciationDecision> ProcessProtoDecisionAsync(
        IContext context,
        SpeciationEpochInfo epoch,
        ProtoSpec.SpeciationApplyMode applyMode,
        ProtoSpec.SpeciationCandidateRef? candidate,
        IEnumerable<ProtoSpec.SpeciationParentRef> parents,
        string? speciesId,
        string? speciesDisplayName,
        string? policyVersion,
        string? decisionReason,
        string? decisionMetadataJson,
        long? decisionTimeMs,
        bool commit)
    {
        if (!TryResolveCandidate(candidate, out var resolved))
        {
            return CreateDecisionFailure(
                applyMode,
                ProtoSpec.SpeciationFailureReason.SpeciationFailureInvalidCandidate,
                "Speciation candidate must be brain_id, artifact_ref, or artifact_uri.");
        }

        resolved = await TryEnrichResolvedCandidateAsync(context, resolved).ConfigureAwait(false);

        var inputOrderedParentBrainIds = ExtractParentBrainIdsByInputOrder(parents);
        var orderedParentBrainIds = ExtractParentBrainIds(parents);
        var orderedParentArtifactRefs = ExtractParentArtifactLabels(parents);
        var parentMemberships = await LoadParentMembershipsAsync(epoch.EpochId, orderedParentBrainIds).ConfigureAwait(false);
        var hysteresisMembership = await ResolveHysteresisMembershipAsync(
            epoch.EpochId,
            orderedParentBrainIds).ConfigureAwait(false);
        var similarityEvidence = ExtractSimilarityEvidence(decisionMetadataJson);
        var lineageEvidence = BuildLineageEvidence(
            orderedParentBrainIds,
            orderedParentArtifactRefs,
            parentMemberships,
            hysteresisMembership);
        var bestParentSpeciesFit = TryResolveBestParentSpeciesPairwiseFit(
            parentMemberships,
            similarityEvidence,
            inputOrderedParentBrainIds,
            out var resolvedBestParentSpeciesFit)
            ? (ParentSpeciesPairwiseFit?)resolvedBestParentSpeciesFit
            : null;
        var isSeedFounderCandidate = IsSeedFounderCandidate(
            resolved.BrainId,
            inputOrderedParentBrainIds);
        var preliminarySourceSpeciesId = bestParentSpeciesFit?.SpeciesId
            ?? (lineageEvidence.DominantShare >= 0.999999d
                ? lineageEvidence.DominantSpeciesId
                : null);
        var sourceSpeciesSimilarityScore = bestParentSpeciesFit.HasValue
            ? bestParentSpeciesFit.Value.PairwiseSimilarity
            : ResolveSourceSpeciesSimilarityScore(
                preliminarySourceSpeciesId,
                parentMemberships,
                similarityEvidence,
                inputOrderedParentBrainIds);
        var sourceSpeciesFloor = ResolveSpeciesSimilarityFloor(
            preliminarySourceSpeciesId);
        var assignmentResolution = ResolveAssignment(
            epoch,
            speciesId,
            speciesDisplayName,
            lineageEvidence,
            sourceSpeciesFloor,
            sourceSpeciesSimilarityScore,
            bestParentSpeciesFit,
            isSeedFounderCandidate: isSeedFounderCandidate);
        var bootstrapRequirement = await TryResolveBootstrapAssignedSpeciesAdmissionRequirementAsync(
            epoch.EpochId,
            assignmentResolution).ConfigureAwait(false);
        AssignedSpeciesAdmissionAssessment? assignedSpeciesAdmissionAssessment = null;
        if (RequiresActualAssignedSpeciesAdmission(
                assignmentResolution,
                bootstrapRequirement.HasValue))
        {
            var admissionSourceSimilarityScore =
                bootstrapRequirement.HasValue
                && string.Equals(
                    assignmentResolution.SpeciesId,
                    assignmentResolution.SourceSpeciesId,
                    StringComparison.Ordinal)
                    ? null
                    : sourceSpeciesSimilarityScore;
            var evaluatedAdmission = await TryAssessActualAssignedSpeciesAdmissionAsync(
                context,
                epoch,
                resolved,
                assignmentResolution,
                bootstrapRequirement,
                admissionSourceSimilarityScore).ConfigureAwait(false);
            assignedSpeciesAdmissionAssessment = evaluatedAdmission;
            if (evaluatedAdmission.Admitted)
            {
            }
            else if (bootstrapRequirement.HasValue)
            {
                sourceSpeciesSimilarityScore = admissionSourceSimilarityScore;
                sourceSpeciesFloor = ResolveSpeciesSimilarityFloor(
                    bootstrapRequirement.Value.SourceSpeciesId);
                assignmentResolution = BuildBootstrapFallbackAssignmentResolution(
                    bootstrapRequirement.Value,
                    admissionSourceSimilarityScore);
                bootstrapRequirement = await TryResolveBootstrapAssignedSpeciesAdmissionRequirementAsync(
                    epoch.EpochId,
                    assignmentResolution).ConfigureAwait(false);
            }
            else
            {
                assignmentResolution = ResolveAssignment(
                    epoch,
                    speciesId,
                    speciesDisplayName,
                    lineageEvidence,
                    sourceSpeciesFloor,
                    sourceSpeciesSimilarityScore,
                    bestParentSpeciesFit,
                    isSeedFounderCandidate,
                    allowRecentSplitRealign: false,
                    allowRecentDerivedSpeciesReuse: false);
            }
        }
        var allowSourceSimilarityCarryover = !bootstrapRequirement.HasValue;
        var intraSpeciesSimilaritySample = TryResolveIntraSpeciesSimilaritySample(
            assignmentResolution,
            parentMemberships,
            similarityEvidence,
            sourceSpeciesSimilarityScore,
            assignedSpeciesAdmissionAssessment.HasValue
            && assignedSpeciesAdmissionAssessment.Value.Admitted
                ? assignedSpeciesAdmissionAssessment.Value.SimilarityScore
                : null,
            inputOrderedParentBrainIds,
            allowSourceSimilarityCarryover,
            out var resolvedIntraSpeciesSample)
            ? (double?)resolvedIntraSpeciesSample
            : null;
        var assignedSpeciesSimilarityScore = ResolveAssignedSpeciesSimilarityScore(
            assignmentResolution,
            parentMemberships,
            similarityEvidence,
            inputOrderedParentBrainIds,
            sourceSpeciesSimilarityScore,
            assignedSpeciesAdmissionAssessment.HasValue
            && assignedSpeciesAdmissionAssessment.Value.Admitted
                ? assignedSpeciesAdmissionAssessment.Value.SimilarityScore
                : null,
            intraSpeciesSimilaritySample,
            allowSourceSimilarityCarryover);

        var resolvedPolicyVersion = NormalizeOrFallback(policyVersion, _runtimeConfig.PolicyVersion);
        var resolvedDecisionReason = assignmentResolution.ForceDecisionReason
            ? assignmentResolution.DecisionReason
            : NormalizeOrFallback(decisionReason, assignmentResolution.DecisionReason);
        var resolvedDecisionMetadata = BuildDecisionMetadataJson(
            decisionMetadataJson,
            resolvedPolicyVersion,
            resolved,
            assignmentResolution,
            lineageEvidence,
            similarityEvidence,
            sourceSpeciesFloor,
            sourceSpeciesSimilarityScore,
            assignedSpeciesSimilarityScore,
            intraSpeciesSimilaritySample,
            assignedSpeciesAdmissionAssessment);
        Guid? parentBrainId = orderedParentBrainIds.Count == 0
            ? null
            : orderedParentBrainIds[0];
        var parentArtifactRef = orderedParentArtifactRefs.Count == 0
            ? null
            : orderedParentArtifactRefs[0];

        var existingMembership = await _store.GetMembershipAsync(epoch.EpochId, resolved.BrainId).ConfigureAwait(false);
        if (!commit)
        {
            if (existingMembership is not null)
            {
                return CreateDecisionFromMembership(
                    applyMode,
                    resolved.CandidateMode,
                    existingMembership,
                    created: false,
                    immutableConflict: false,
                    committed: false,
                    failureReason: ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
                    failureDetail: string.Empty);
            }

            return new ProtoSpec.SpeciationDecision
            {
                ApplyMode = applyMode,
                CandidateMode = resolved.CandidateMode,
                Success = true,
                Created = false,
                ImmutableConflict = false,
                FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
                FailureDetail = string.Empty,
                SpeciesId = assignmentResolution.SpeciesId,
                SpeciesDisplayName = assignmentResolution.SpeciesDisplayName,
                DecisionReason = resolvedDecisionReason,
                DecisionMetadataJson = resolvedDecisionMetadata,
                Committed = false
            };
        }

        var assignment = new SpeciationAssignment(
            resolved.BrainId,
            assignmentResolution.SpeciesId,
            assignmentResolution.SpeciesDisplayName,
            resolvedPolicyVersion,
            resolvedDecisionReason,
            resolvedDecisionMetadata,
            parentBrainId,
            resolved.SourceArtifactRef ?? parentArtifactRef);
        var speciesDisplayNameUpdates =
            !string.IsNullOrWhiteSpace(assignmentResolution.DisplayNameRewriteSpeciesId)
            && !string.IsNullOrWhiteSpace(assignmentResolution.DisplayNameRewriteSpeciesDisplayName)
                ? new[]
                {
                    new SpeciationSpeciesDisplayNameUpdate(
                        assignmentResolution.DisplayNameRewriteSpeciesId!,
                        assignmentResolution.DisplayNameRewriteSpeciesDisplayName!)
                }
                : null;
        var outcome = await _store.TryAssignMembershipAsync(
            epoch.EpochId,
            assignment,
            decisionTimeMs,
            cancellationToken: default,
            lineageParentBrainIds: orderedParentBrainIds,
            lineageMetadataJson: resolvedDecisionMetadata,
            speciesDisplayNameUpdates: speciesDisplayNameUpdates).ConfigureAwait(false);

        if (outcome.Created)
        {
            if (speciesDisplayNameUpdates is not null)
            {
                foreach (var speciesDisplayNameUpdate in speciesDisplayNameUpdates)
                {
                    RecordSpeciesDisplayName(
                        speciesDisplayNameUpdate.SpeciesId,
                        speciesDisplayNameUpdate.SpeciesDisplayName);
                }
            }

            RecordCommittedMembership(
                outcome.Membership,
                intraSpeciesSimilaritySample,
                assignedSpeciesAdmissionAssessment.HasValue
                && assignedSpeciesAdmissionAssessment.Value.Admitted);

            if (string.Equals(
                    assignmentResolution.DecisionReason,
                    "lineage_diverged_new_species",
                    StringComparison.Ordinal)
                && !string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
                && sourceSpeciesSimilarityScore.HasValue)
            {
                context.Send(
                    context.Self,
                    new ApplySplitHindsightReassignmentsRequest(
                        epoch,
                        outcome.Membership,
                        assignmentResolution,
                        sourceSpeciesSimilarityScore.Value,
                        resolvedPolicyVersion,
                        decisionTimeMs));
            }
        }

        var success = !outcome.ImmutableConflict;
        var reason = outcome.ImmutableConflict
            ? ProtoSpec.SpeciationFailureReason.SpeciationFailureMembershipImmutable
            : ProtoSpec.SpeciationFailureReason.SpeciationFailureNone;
        var detail = outcome.ImmutableConflict
            ? "Membership is immutable within the current epoch."
            : string.Empty;
        return CreateDecisionFromMembership(
            applyMode,
            resolved.CandidateMode,
            outcome.Membership,
            created: outcome.Created,
            immutableConflict: outcome.ImmutableConflict,
            committed: outcome.Created,
            failureReason: reason,
            failureDetail: detail,
            successOverride: success);
    }

    private static bool TryResolveCandidate(
        ProtoSpec.SpeciationCandidateRef? candidate,
        out ResolvedCandidate resolvedCandidate)
    {
        resolvedCandidate = default;
        if (candidate is null)
        {
            return false;
        }

        switch (candidate.CandidateCase)
        {
            case ProtoSpec.SpeciationCandidateRef.CandidateOneofCase.BrainId:
                if (candidate.BrainId is not null && candidate.BrainId.TryToGuid(out var brainId) && brainId != Guid.Empty)
                {
                    resolvedCandidate = new ResolvedCandidate(
                        ProtoSpec.SpeciationCandidateMode.BrainId,
                        brainId,
                        SourceArtifactRef: null,
                        CandidateArtifactRef: null,
                        CandidateArtifactUri: null);
                    return true;
                }
                return false;
            case ProtoSpec.SpeciationCandidateRef.CandidateOneofCase.ArtifactRef:
                if (HasUsableArtifactReference(candidate.ArtifactRef))
                {
                    var sourceArtifactRef = BuildArtifactLabel(candidate.ArtifactRef!);
                    var derivedBrainId = CreateDeterministicCandidateBrainId(
                        BuildArtifactIdentityKey(candidate.ArtifactRef!));
                    resolvedCandidate = new ResolvedCandidate(
                        ProtoSpec.SpeciationCandidateMode.ArtifactRef,
                        derivedBrainId,
                        sourceArtifactRef,
                        candidate.ArtifactRef!.Clone(),
                        CandidateArtifactUri: null);
                    return true;
                }
                return false;
            case ProtoSpec.SpeciationCandidateRef.CandidateOneofCase.ArtifactUri:
                if (!string.IsNullOrWhiteSpace(candidate.ArtifactUri))
                {
                    var normalizedUri = candidate.ArtifactUri.Trim();
                    var derivedBrainId = CreateDeterministicCandidateBrainId($"artifact_uri|{normalizedUri}");
                    resolvedCandidate = new ResolvedCandidate(
                        ProtoSpec.SpeciationCandidateMode.ArtifactUri,
                        derivedBrainId,
                        normalizedUri,
                        CandidateArtifactRef: null,
                        CandidateArtifactUri: normalizedUri);
                    return true;
                }
                return false;
            default:
                return false;
        }
    }

    private static string? BuildArtifactLabel(ArtifactRef artifactRef)
    {
        if (artifactRef.TryToSha256Hex(out var sha))
        {
            return $"sha256:{sha}";
        }

        return string.IsNullOrWhiteSpace(artifactRef.StoreUri)
            ? null
            : artifactRef.StoreUri.Trim();
    }

    private static string BuildArtifactIdentityKey(ArtifactRef artifactRef)
    {
        if (artifactRef.TryToSha256Hex(out var sha256Hex))
        {
            return $"artifact_ref|sha256={sha256Hex}";
        }

        var storeUri = string.IsNullOrWhiteSpace(artifactRef.StoreUri)
            ? string.Empty
            : artifactRef.StoreUri.Trim();
        var mediaType = string.IsNullOrWhiteSpace(artifactRef.MediaType)
            ? string.Empty
            : artifactRef.MediaType.Trim();
        return $"artifact_ref|size={artifactRef.SizeBytes}|media_type={mediaType}|store_uri={storeUri}";
    }

    private async Task<ResolvedCandidate> TryEnrichResolvedCandidateAsync(
        IContext context,
        ResolvedCandidate resolvedCandidate)
    {
        if (resolvedCandidate.CandidateMode != ProtoSpec.SpeciationCandidateMode.BrainId
            || resolvedCandidate.BrainId == Guid.Empty)
        {
            return resolvedCandidate;
        }

        var provenance = await TryResolveBrainArtifactProvenanceAsync(
            context,
            resolvedCandidate.BrainId).ConfigureAwait(false);
        if (!HasUsableArtifactReference(provenance.BaseArtifactRef))
        {
            return resolvedCandidate;
        }

        return resolvedCandidate with
        {
            SourceArtifactRef = resolvedCandidate.SourceArtifactRef ?? BuildArtifactLabel(provenance.BaseArtifactRef!),
            CandidateBrainBaseArtifactRef = provenance.BaseArtifactRef!.Clone(),
            CandidateBrainSnapshotArtifactRef = HasUsableArtifactReference(provenance.SnapshotArtifactRef)
                ? provenance.SnapshotArtifactRef!.Clone()
                : null
        };
    }

    private async Task<BrainArtifactProvenance> TryResolveBrainArtifactProvenanceAsync(
        IContext context,
        Guid brainId)
    {
        if (_ioGatewayPid is null || brainId == Guid.Empty)
        {
            return default;
        }

        try
        {
            var info = await context.RequestAsync<ProtoIo.BrainInfo>(
                _ioGatewayPid,
                new ProtoIo.BrainInfoRequest
                {
                    BrainId = brainId.ToProtoUuid()
                },
                _compatibilityRequestTimeout).ConfigureAwait(false);
            if (info is null || !HasUsableArtifactReference(info.BaseDefinition))
            {
                return default;
            }

            return new BrainArtifactProvenance(
                info.BaseDefinition.Clone(),
                HasUsableArtifactReference(info.LastSnapshot)
                    ? info.LastSnapshot.Clone()
                    : null);
        }
        catch
        {
            return default;
        }
    }

    private static Guid CreateDeterministicCandidateBrainId(string identityKey)
    {
        if (string.IsNullOrWhiteSpace(identityKey))
        {
            return Guid.Empty;
        }

        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(identityKey.Trim()));
        Span<byte> guidBytes = stackalloc byte[16];
        bytes.AsSpan(0, 16).CopyTo(guidBytes);

        // Mark as RFC 4122 variant and version-5 style UUID for deterministic artifact identities.
        guidBytes[6] = (byte)((guidBytes[6] & 0x0F) | 0x50);
        guidBytes[8] = (byte)((guidBytes[8] & 0x3F) | 0x80);
        return new Guid(guidBytes);
    }

    private static bool HasUsableArtifactReference(ArtifactRef? artifactRef)
    {
        if (artifactRef is null)
        {
            return false;
        }

        var hasSha = artifactRef.Sha256 is not null
                     && artifactRef.Sha256.Value is not null
                     && artifactRef.Sha256.Value.Length == 32;
        var hasStoreUri = !string.IsNullOrWhiteSpace(artifactRef.StoreUri);
        return hasSha || hasStoreUri;
    }

    private static IReadOnlyList<Guid> ExtractParentBrainIds(IEnumerable<ProtoSpec.SpeciationParentRef> parents)
    {
        return ExtractParentBrainIdsByInputOrder(parents)
            .OrderBy(parentIdentity => parentIdentity)
            .ToArray();
    }

    private static IReadOnlyList<Guid> ExtractParentBrainIdsByInputOrder(IEnumerable<ProtoSpec.SpeciationParentRef> parents)
    {
        if (parents is null)
        {
            return Array.Empty<Guid>();
        }

        var identities = new List<Guid>();
        var seen = new HashSet<Guid>();
        foreach (var parent in parents)
        {
            if (TryResolveParentIdentity(parent, out var parentIdentity)
                && parentIdentity != Guid.Empty
                && seen.Add(parentIdentity))
            {
                identities.Add(parentIdentity);
            }
        }

        return identities;
    }

    private static bool TryResolveParentIdentity(ProtoSpec.SpeciationParentRef? parent, out Guid parentIdentity)
    {
        parentIdentity = Guid.Empty;
        if (parent is null)
        {
            return false;
        }

        switch (parent.ParentCase)
        {
            case ProtoSpec.SpeciationParentRef.ParentOneofCase.BrainId:
                if (parent.BrainId is not null
                    && parent.BrainId.TryToGuid(out var parentBrainId)
                    && parentBrainId != Guid.Empty)
                {
                    parentIdentity = parentBrainId;
                    return true;
                }

                return false;
            case ProtoSpec.SpeciationParentRef.ParentOneofCase.ArtifactRef:
                if (HasUsableArtifactReference(parent.ArtifactRef))
                {
                    parentIdentity = CreateDeterministicCandidateBrainId(
                        BuildArtifactIdentityKey(parent.ArtifactRef!));
                    return parentIdentity != Guid.Empty;
                }

                return false;
            case ProtoSpec.SpeciationParentRef.ParentOneofCase.ArtifactUri:
                if (!string.IsNullOrWhiteSpace(parent.ArtifactUri))
                {
                    parentIdentity = CreateDeterministicCandidateBrainId(
                        $"artifact_uri|{parent.ArtifactUri.Trim()}");
                    return parentIdentity != Guid.Empty;
                }

                return false;
            default:
                return false;
        }
    }

    private static IReadOnlyList<string> ExtractParentArtifactLabels(IEnumerable<ProtoSpec.SpeciationParentRef> parents)
    {
        if (parents is null)
        {
            return Array.Empty<string>();
        }

        var labels = new List<string>();
        foreach (var parent in parents)
        {
            if (parent is null)
            {
                continue;
            }

            if (parent.ParentCase == ProtoSpec.SpeciationParentRef.ParentOneofCase.ArtifactUri
                && !string.IsNullOrWhiteSpace(parent.ArtifactUri))
            {
                labels.Add(parent.ArtifactUri.Trim());
                continue;
            }

            if (parent.ParentCase != ProtoSpec.SpeciationParentRef.ParentOneofCase.ArtifactRef
                || parent.ArtifactRef is null)
            {
                continue;
            }

            if (parent.ArtifactRef.TryToSha256Hex(out var sha))
            {
                labels.Add($"sha256:{sha}");
                continue;
            }

            if (!string.IsNullOrWhiteSpace(parent.ArtifactRef.StoreUri))
            {
                labels.Add(parent.ArtifactRef.StoreUri.Trim());
            }
        }

        return labels
            .Where(label => !string.IsNullOrWhiteSpace(label))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(label => label, StringComparer.Ordinal)
            .ToArray();
    }

    private static Guid? ExtractFirstParentBrainId(IEnumerable<ProtoSpec.SpeciationParentRef> parents)
    {
        var parentBrainIds = ExtractParentBrainIds(parents);
        return parentBrainIds.Count == 0 ? null : parentBrainIds[0];
    }

    private static string? ExtractFirstParentArtifactLabel(IEnumerable<ProtoSpec.SpeciationParentRef> parents)
    {
        var parentArtifactLabels = ExtractParentArtifactLabels(parents);
        return parentArtifactLabels.Count == 0 ? null : parentArtifactLabels[0];
    }

    private async Task<IReadOnlyList<SpeciationMembershipRecord>> LoadParentMembershipsAsync(
        long epochId,
        IReadOnlyList<Guid> orderedParentBrainIds)
    {
        if (orderedParentBrainIds.Count == 0)
        {
            return Array.Empty<SpeciationMembershipRecord>();
        }

        var memberships = new List<SpeciationMembershipRecord>(orderedParentBrainIds.Count);
        foreach (var parentBrainId in orderedParentBrainIds)
        {
            var membership = await _store.GetMembershipAsync(epochId, parentBrainId).ConfigureAwait(false);
            if (membership is not null)
            {
                memberships.Add(membership);
            }
        }

        return memberships;
    }

    private async Task<SpeciationMembershipRecord?> ResolveHysteresisMembershipAsync(
        long epochId,
        IReadOnlyList<Guid> orderedParentBrainIds)
    {
        if (orderedParentBrainIds.Count == 0)
        {
            return null;
        }

        var hints = new List<SpeciationMembershipRecord>(orderedParentBrainIds.Count);
        foreach (var parentBrainId in orderedParentBrainIds)
        {
            var hint = await _store.GetLatestChildMembershipForParentAsync(
                epochId,
                parentBrainId).ConfigureAwait(false);
            if (hint is not null)
            {
                hints.Add(hint);
            }
        }

        if (hints.Count == 0)
        {
            return null;
        }

        return hints
            .GroupBy(item => item.SpeciesId, StringComparer.Ordinal)
            .Select(group =>
            {
                var latest = group
                    .OrderByDescending(item => item.AssignedMs)
                    .ThenBy(item => item.BrainId)
                    .First();
                return new
                {
                    SpeciesId = group.Key,
                    Count = group.Count(),
                    LatestAssignedMs = latest.AssignedMs,
                    Membership = latest
                };
            })
            .OrderByDescending(item => item.Count)
            .ThenByDescending(item => item.LatestAssignedMs)
            .ThenBy(item => item.SpeciesId, StringComparer.Ordinal)
            .Select(item => item.Membership)
            .FirstOrDefault();
    }

    private static LineageEvidence BuildLineageEvidence(
        IReadOnlyList<Guid> orderedParentBrainIds,
        IReadOnlyList<string> orderedParentArtifactRefs,
        IReadOnlyList<SpeciationMembershipRecord> parentMemberships,
        SpeciationMembershipRecord? hysteresisMembership)
    {
        var dominant = parentMemberships
            .GroupBy(item => item.SpeciesId, StringComparer.Ordinal)
            .Select(group =>
            {
                var preferred = group
                    .OrderByDescending(item => item.AssignedMs)
                    .ThenBy(item => item.BrainId)
                    .First();
                return new
                {
                    SpeciesId = group.Key,
                    SpeciesDisplayName = preferred.SpeciesDisplayName,
                    Count = group.Count(),
                    LatestAssignedMs = preferred.AssignedMs
                };
            })
            .OrderByDescending(item => item.Count)
            .ThenByDescending(item => item.LatestAssignedMs)
            .ThenBy(item => item.SpeciesId, StringComparer.Ordinal)
            .FirstOrDefault();

        var totalMemberships = parentMemberships.Count;
        var dominantShare = totalMemberships <= 0 || dominant is null
            ? 0d
            : (double)dominant.Count / totalMemberships;

        return new LineageEvidence(
            orderedParentBrainIds,
            orderedParentArtifactRefs,
            totalMemberships,
            dominant?.SpeciesId,
            dominant?.SpeciesDisplayName,
            dominantShare,
            BuildLineageKey(orderedParentBrainIds),
            hysteresisMembership?.SpeciesId,
            hysteresisMembership?.SpeciesDisplayName,
            hysteresisMembership?.DecisionReason);
    }

    private async Task PrimeSpeciesSimilarityFloorsAsync(long epochId)
    {
        ResetSpeciesSimilarityFloors();
        if (epochId <= 0)
        {
            return;
        }

        var memberships = await _store.ListMembershipsAsync(epochId).ConfigureAwait(false);
        foreach (var membership in memberships)
        {
            RecordCommittedMembership(membership);
        }
    }

    private void ResetSpeciesSimilarityFloors()
    {
        _speciesSimilarityFloors.Clear();
        _recentDerivedSpeciesHintsBySourceSpecies.Clear();
        _recentDerivedSpeciesHintsByTargetSpecies.Clear();
        _speciesDisplayNamesBySpeciesId.Clear();
        _rootSpeciesOrdinalsBySpeciesId.Clear();
        _maxRootSpeciesOrdinalByStem.Clear();
    }

    private SpeciesSimilarityFloorState? ResolveSpeciesSimilarityFloor(string? speciesId)
    {
        if (string.IsNullOrWhiteSpace(speciesId))
        {
            return null;
        }

        var normalizedSpeciesId = speciesId.Trim();
        return _speciesSimilarityFloors.TryGetValue(normalizedSpeciesId, out var state)
            ? state
            : null;
    }

    private void IncrementSpeciesMembershipCount(string speciesId, int incrementBy)
    {
        if (incrementBy <= 0 || string.IsNullOrWhiteSpace(speciesId))
        {
            return;
        }

        var normalizedSpeciesId = speciesId.Trim();
        _speciesSimilarityFloors.TryGetValue(normalizedSpeciesId, out var existing);
        _speciesSimilarityFloors[normalizedSpeciesId] = existing with
        {
            MembershipCount = Math.Max(0, existing.MembershipCount) + incrementBy
        };
    }

    private SplitThresholdState ResolveSplitThresholdState(SpeciesSimilarityFloorState? speciesFloor)
    {
        var policyEffectiveSplitThreshold = Math.Max(
            0d,
            _assignmentPolicy.LineageSplitThreshold - _assignmentPolicy.LineageSplitGuardMargin);
        var dynamicSplitThreshold = policyEffectiveSplitThreshold;
        double? speciesFloorSimilarityScore = null;
        var speciesFloorSampleCount = 0;
        var speciesFloorMembershipCount = 0;

        if (speciesFloor.HasValue
            && speciesFloor.Value.SimilaritySampleCount > 0
            && speciesFloor.Value.MinSimilarityScore.HasValue)
        {
            speciesFloorSimilarityScore = ClampScore(speciesFloor.Value.MinSimilarityScore.Value);
            speciesFloorSampleCount = Math.Max(0, speciesFloor.Value.SimilaritySampleCount);
            speciesFloorMembershipCount = Math.Max(0, speciesFloor.Value.MembershipCount);
            var relaxedSpeciesFloorThreshold = Math.Max(
                0d,
                speciesFloorSimilarityScore.Value - _assignmentPolicy.HysteresisMargin);
            dynamicSplitThreshold = Math.Max(dynamicSplitThreshold, relaxedSpeciesFloorThreshold);
        }

        return new SplitThresholdState(
            PolicyEffectiveSplitThreshold: policyEffectiveSplitThreshold,
            DynamicSplitThreshold: dynamicSplitThreshold,
            UsesSpeciesFloor: dynamicSplitThreshold > policyEffectiveSplitThreshold,
            SpeciesFloorSimilarityScore: speciesFloorSimilarityScore,
            SpeciesFloorSampleCount: speciesFloorSampleCount,
            SpeciesFloorMembershipCount: speciesFloorMembershipCount);
    }

    private static double? ResolveSourceSpeciesSimilarityScore(
        string? sourceSpeciesId,
        IReadOnlyList<SpeciationMembershipRecord> parentMemberships,
        SimilarityEvidence similarityEvidence,
        IReadOnlyList<Guid> inputOrderedParentBrainIds)
    {
        if (!string.IsNullOrWhiteSpace(sourceSpeciesId)
            && TryResolveSpeciesPairwiseSimilarityEstimate(
                sourceSpeciesId.Trim(),
                parentMemberships,
                similarityEvidence,
                inputOrderedParentBrainIds,
                out var sourcePairwiseSimilarity))
        {
            return sourcePairwiseSimilarity;
        }

        if (similarityEvidence.DominantSpeciesSimilarityScore.HasValue)
        {
            return ClampScore(similarityEvidence.DominantSpeciesSimilarityScore.Value);
        }

        return similarityEvidence.SimilarityScore.HasValue
            ? ClampScore(similarityEvidence.SimilarityScore.Value)
            : null;
    }

    private static double? ResolveAssignedSpeciesSimilarityScore(
        AssignmentResolution assignmentResolution,
        IReadOnlyList<SpeciationMembershipRecord> parentMemberships,
        SimilarityEvidence similarityEvidence,
        IReadOnlyList<Guid> inputOrderedParentBrainIds,
        double? sourceSpeciesSimilarityScore,
        double? actualAssignedSpeciesSimilarityScore,
        double? intraSpeciesSimilaritySample,
        bool allowSourceSimilarityCarryover)
    {
        if (string.IsNullOrWhiteSpace(assignmentResolution.SpeciesId)
            || string.Equals(assignmentResolution.Strategy, "explicit_species", StringComparison.Ordinal))
        {
            return null;
        }

        if (actualAssignedSpeciesSimilarityScore.HasValue)
        {
            return ClampScore(actualAssignedSpeciesSimilarityScore.Value);
        }

        if (intraSpeciesSimilaritySample.HasValue)
        {
            return ClampScore(intraSpeciesSimilaritySample.Value);
        }

        if (TryResolveSpeciesPairwiseSimilarityEstimate(
                assignmentResolution.SpeciesId,
                parentMemberships,
                similarityEvidence,
                inputOrderedParentBrainIds,
                out var pairwiseSimilarity))
        {
            return pairwiseSimilarity;
        }

        if (allowSourceSimilarityCarryover
            && ShouldCarrySourceSimilarityIntoAssignedSpecies(
                assignmentResolution,
                sourceSpeciesSimilarityScore))
        {
            return ClampScore(sourceSpeciesSimilarityScore!.Value);
        }

        return sourceSpeciesSimilarityScore.HasValue
            && !string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
            && string.Equals(
                assignmentResolution.SpeciesId,
                assignmentResolution.SourceSpeciesId,
                StringComparison.Ordinal)
            ? ClampScore(sourceSpeciesSimilarityScore.Value)
            : null;
    }

    private static bool ShouldCarrySourceSimilarityIntoAssignedSpecies(
        AssignmentResolution assignmentResolution,
        double? sourceSpeciesSimilarityScore)
    {
        return sourceSpeciesSimilarityScore.HasValue
            && !string.IsNullOrWhiteSpace(assignmentResolution.SpeciesId)
            && !string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
            && !string.Equals(
                assignmentResolution.SpeciesId,
                assignmentResolution.SourceSpeciesId,
                StringComparison.Ordinal)
            && !string.Equals(
                assignmentResolution.DecisionReason,
                "lineage_diverged_new_species",
                StringComparison.Ordinal)
            && !string.Equals(
                assignmentResolution.Strategy,
                "explicit_species",
                StringComparison.Ordinal);
    }

    private void RecordCommittedMembership(
        SpeciationMembershipRecord membership,
        double? similaritySample,
        bool actualSimilaritySample)
    {
        if (membership is null)
        {
            return;
        }

        UpdateSpeciesSimilarityFloor(
            membership.SpeciesId,
            NormalizeSpeciesFloorSimilaritySample(
                membership,
                similaritySample,
                actualSimilaritySample),
            actualSimilaritySample);
        UpdateRecentDerivedSpeciesHints(membership);
    }

    private void RecordCommittedMembership(SpeciationMembershipRecord membership)
    {
        if (membership is null)
        {
            return;
        }

        RecordSpeciesDisplayName(membership.SpeciesId, membership.SpeciesDisplayName);
        var actualSimilaritySample = HasActualAssignedSpeciesSimilaritySample(membership);
        var similaritySample = NormalizeSpeciesFloorSimilaritySample(
            membership,
            TryExtractIntraSpeciesSimilaritySample(membership),
            actualSimilaritySample);
        UpdateSpeciesSimilarityFloor(
            membership.SpeciesId,
            similaritySample,
            actualSimilaritySample);
        UpdateRecentDerivedSpeciesHints(membership);
    }

    private void RecordSpeciesDisplayName(string? speciesId, string? speciesDisplayName)
    {
        if (string.IsNullOrWhiteSpace(speciesId) || string.IsNullOrWhiteSpace(speciesDisplayName))
        {
            return;
        }

        var normalizedSpeciesId = speciesId.Trim();
        var normalizedDisplayName = speciesDisplayName.Trim();
        _speciesDisplayNamesBySpeciesId[normalizedSpeciesId] = normalizedDisplayName;

        var (stem, lineageCode) = ParseLineageDisplayName(normalizedDisplayName);
        if (lineageCode.Length == 0
            && TryParseNumberedRootSpeciesDisplayName(stem, out var rootStem, out var rootOrdinal))
        {
            _rootSpeciesOrdinalsBySpeciesId[normalizedSpeciesId] = rootOrdinal;
            _maxRootSpeciesOrdinalByStem[rootStem] = _maxRootSpeciesOrdinalByStem.TryGetValue(rootStem, out var existingOrdinal)
                ? Math.Max(existingOrdinal, rootOrdinal)
                : rootOrdinal;
            return;
        }

        _rootSpeciesOrdinalsBySpeciesId.Remove(normalizedSpeciesId);
    }

    private string ResolveTrackedSpeciesDisplayName(
        string speciesId,
        string? preferredDisplayName = null,
        string? fallbackDisplayName = null)
    {
        if (!string.IsNullOrWhiteSpace(preferredDisplayName))
        {
            return preferredDisplayName.Trim();
        }

        var normalizedSpeciesId = speciesId?.Trim() ?? string.Empty;
        if (normalizedSpeciesId.Length > 0
            && _speciesDisplayNamesBySpeciesId.TryGetValue(normalizedSpeciesId, out var trackedDisplayName)
            && !string.IsNullOrWhiteSpace(trackedDisplayName))
        {
            return trackedDisplayName;
        }

        if (!string.IsNullOrWhiteSpace(fallbackDisplayName))
        {
            return fallbackDisplayName.Trim();
        }

        return BuildDisplayNameFromSpeciesId(normalizedSpeciesId);
    }

    private FounderRootSpeciesNamingPlan BuildFounderRootSpeciesNamingPlan(
        string sourceSpeciesId,
        string? sourceSpeciesDisplayName)
    {
        var resolvedSourceDisplayName = ResolveTrackedSpeciesDisplayName(
            sourceSpeciesId,
            sourceSpeciesDisplayName);
        var (stem, _) = ParseLineageDisplayName(resolvedSourceDisplayName);
        var rootStem = stem.Trim().Length == 0 ? "Species" : stem.Trim();
        var sourceRootOrdinal = 0;
        if (TryParseNumberedRootSpeciesDisplayName(rootStem, out var parsedStem, out var parsedOrdinal))
        {
            rootStem = parsedStem;
            sourceRootOrdinal = parsedOrdinal;
        }
        else if (_rootSpeciesOrdinalsBySpeciesId.TryGetValue(sourceSpeciesId.Trim(), out var trackedOrdinal)
                 && trackedOrdinal > 0)
        {
            sourceRootOrdinal = trackedOrdinal;
        }

        var maxKnownOrdinal = _maxRootSpeciesOrdinalByStem.TryGetValue(rootStem, out var existingMaxOrdinal)
            ? existingMaxOrdinal
            : 0;
        string? sourceDisplayNameRewrite = null;
        if (sourceRootOrdinal <= 0)
        {
            sourceRootOrdinal = 1;
            sourceDisplayNameRewrite = BuildNumberedRootSpeciesDisplayName(rootStem, sourceRootOrdinal);
        }

        var founderRootOrdinal = Math.Max(sourceRootOrdinal, maxKnownOrdinal) + 1;
        return new FounderRootSpeciesNamingPlan(
            BuildNumberedRootSpeciesDisplayName(rootStem, founderRootOrdinal),
            sourceDisplayNameRewrite,
            sourceDisplayNameRewrite is null ? null : sourceSpeciesId.Trim());
    }

    private static double? NormalizeSpeciesFloorSimilaritySample(
        SpeciationMembershipRecord membership,
        double? similaritySample,
        bool actualSimilaritySample)
    {
        if (membership is null || !similaritySample.HasValue)
        {
            return null;
        }

        return IsSyntheticFounderSpeciesCreationDecision(membership.DecisionReason)
            && !actualSimilaritySample
            ? null
            : ClampScore(similaritySample.Value);
    }

    private static bool IsSyntheticFounderSpeciesCreationDecision(string? decisionReason)
    {
        return string.Equals(
                   decisionReason,
                   "lineage_diverged_new_species",
                   StringComparison.Ordinal)
               || string.Equals(
                   decisionReason,
                   "lineage_diverged_founder_root_species",
                   StringComparison.Ordinal);
    }

    private void UpdateRecentDerivedSpeciesHints(SpeciationMembershipRecord membership)
    {
        if (!TryExtractRecentDerivedSpeciesHint(membership, out var hint))
        {
            return;
        }

        StoreRecentDerivedSpeciesHint(hint);
    }

    private void StoreRecentDerivedSpeciesHint(RecentDerivedSpeciesHint hint)
    {
        if (!_recentDerivedSpeciesHintsBySourceSpecies.TryGetValue(hint.SourceSpeciesId, out var hints))
        {
            hints = new List<RecentDerivedSpeciesHint>();
            _recentDerivedSpeciesHintsBySourceSpecies[hint.SourceSpeciesId] = hints;
        }

        hints.RemoveAll(existing =>
            string.Equals(existing.TargetSpeciesId, hint.TargetSpeciesId, StringComparison.Ordinal));
        hints.Add(hint);
        _recentDerivedSpeciesHintsByTargetSpecies[hint.TargetSpeciesId] = hint;

        hints.Sort(static (left, right) =>
        {
            var assignedOrder = right.AssignedMs.CompareTo(left.AssignedMs);
            return assignedOrder != 0
                ? assignedOrder
                : string.Compare(left.TargetSpeciesId, right.TargetSpeciesId, StringComparison.Ordinal);
        });

        var hintLimit = Math.Max(
            1,
            Math.Max(
                _assignmentPolicy.HindsightReassignCommitWindow,
                _assignmentPolicy.RecentSplitRealignParentMembershipWindow));
        if (hints.Count > hintLimit)
        {
            hints.RemoveRange(hintLimit, hints.Count - hintLimit);
        }
    }

    private async Task<RecentDerivedSpeciesHint?> TryResolveDerivedSpeciesHintByTargetSpeciesAsync(
        long epochId,
        string? speciesId,
        CancellationToken cancellationToken = default)
    {
        if (TryGetRecentDerivedSpeciesHintByTargetSpecies(speciesId, out var hint))
        {
            return hint;
        }

        if (epochId <= 0 || string.IsNullOrWhiteSpace(speciesId))
        {
            return null;
        }

        var earliestMemberships = await _store.ListEarliestMembershipsForSpeciesAsync(
            epochId,
            speciesId.Trim(),
            limit: 1,
            cancellationToken).ConfigureAwait(false);
        if (earliestMemberships.Count == 0
            || !TryExtractRecentDerivedSpeciesHint(earliestMemberships[0], out hint))
        {
            return null;
        }

        StoreRecentDerivedSpeciesHint(hint);
        return hint;
    }

    private void UpdateSpeciesSimilarityFloor(
        string speciesId,
        double? similaritySample,
        bool actualSimilaritySample = false)
    {
        if (string.IsNullOrWhiteSpace(speciesId))
        {
            return;
        }

        var normalizedSpeciesId = speciesId.Trim();
        _speciesSimilarityFloors.TryGetValue(normalizedSpeciesId, out var existing);

        var membershipCount = Math.Max(0, existing.MembershipCount) + 1;
        var sampleCount = Math.Max(0, existing.SimilaritySampleCount);
        var actualSampleCount = Math.Max(0, existing.ActualSimilaritySampleCount);
        var minSimilarity = existing.MinSimilarityScore;

        if (similaritySample.HasValue)
        {
            var normalizedSample = ClampScore(similaritySample.Value);
            sampleCount++;
            if (actualSimilaritySample)
            {
                actualSampleCount++;
            }

            minSimilarity = minSimilarity.HasValue
                ? Math.Min(minSimilarity.Value, normalizedSample)
                : normalizedSample;
        }

        _speciesSimilarityFloors[normalizedSpeciesId] = new SpeciesSimilarityFloorState(
            MembershipCount: membershipCount,
            SimilaritySampleCount: sampleCount,
            ActualSimilaritySampleCount: actualSampleCount,
            MinSimilarityScore: minSimilarity);
    }

    private static bool TryResolveIntraSpeciesSimilaritySample(
        AssignmentResolution assignmentResolution,
        IReadOnlyList<SpeciationMembershipRecord> parentMemberships,
        SimilarityEvidence similarityEvidence,
        double? sourceSpeciesSimilarityScore,
        double? actualAssignedSpeciesSimilarityScore,
        IReadOnlyList<Guid> inputOrderedParentBrainIds,
        bool allowSourceSimilarityCarryover,
        out double similaritySample)
    {
        similaritySample = 0d;
        if (string.IsNullOrWhiteSpace(assignmentResolution.SpeciesId)
            || string.Equals(assignmentResolution.Strategy, "explicit_species", StringComparison.Ordinal))
        {
            return false;
        }

        if (actualAssignedSpeciesSimilarityScore.HasValue)
        {
            similaritySample = ClampScore(actualAssignedSpeciesSimilarityScore.Value);
            return true;
        }

        if (string.Equals(
                assignmentResolution.DecisionReason,
                "lineage_diverged_new_species",
                StringComparison.Ordinal))
        {
            similaritySample = 1d;
            return true;
        }

        if (TryResolveSpeciesPairwiseSimilarityEstimate(
                assignmentResolution.SpeciesId,
                parentMemberships,
                similarityEvidence,
                inputOrderedParentBrainIds,
                out var pairwiseSimilarity))
        {
            similaritySample = pairwiseSimilarity;
            return true;
        }

        if (sourceSpeciesSimilarityScore.HasValue
            && !string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
            && assignmentResolution.SourceConsensusShare >= 0.999999d
            && string.Equals(
                assignmentResolution.SpeciesId,
                assignmentResolution.SourceSpeciesId,
                StringComparison.Ordinal))
        {
            similaritySample = ClampScore(sourceSpeciesSimilarityScore.Value);
            return true;
        }

        if (allowSourceSimilarityCarryover
            && ShouldCarrySourceSimilarityIntoAssignedSpecies(
                assignmentResolution,
                sourceSpeciesSimilarityScore))
        {
            similaritySample = ClampScore(sourceSpeciesSimilarityScore!.Value);
            return true;
        }

        return false;
    }

    private static bool TryResolveBestParentSpeciesPairwiseFit(
        IReadOnlyList<SpeciationMembershipRecord> parentMemberships,
        SimilarityEvidence similarityEvidence,
        IReadOnlyList<Guid> inputOrderedParentBrainIds,
        out ParentSpeciesPairwiseFit bestFit)
    {
        bestFit = default;
        if (parentMemberships.Count == 0 || inputOrderedParentBrainIds.Count == 0)
        {
            return false;
        }

        var fits = parentMemberships
            .Where(parent => !string.IsNullOrWhiteSpace(parent.SpeciesId))
            .GroupBy(parent => parent.SpeciesId.Trim(), StringComparer.Ordinal)
            .Select(group =>
            {
                if (!TryResolveSpeciesPairwiseSimilarityEstimate(
                        group.Key,
                        parentMemberships,
                        similarityEvidence,
                        inputOrderedParentBrainIds,
                        out var pairwiseSimilarity))
                {
                    return (ParentSpeciesPairwiseFit?)null;
                }

                var preferred = group
                    .OrderByDescending(item => item.AssignedMs)
                    .ThenBy(item => item.BrainId)
                    .First();
                return new ParentSpeciesPairwiseFit(
                    group.Key,
                    ResolveSpeciesDisplayName(preferred.SpeciesDisplayName, group.Key),
                    pairwiseSimilarity,
                    group.Count(),
                    preferred.AssignedMs);
            })
            .Where(item => item.HasValue)
            .Select(item => item!.Value)
            .OrderByDescending(item => item.PairwiseSimilarity)
            .ThenByDescending(item => item.SupportingParentCount)
            .ThenByDescending(item => item.LatestAssignedMs)
            .ThenBy(item => item.SpeciesId, StringComparer.Ordinal)
            .ToArray();

        if (fits.Length == 0)
        {
            return false;
        }

        bestFit = fits[0];
        return true;
    }

    private static bool TryResolveSpeciesPairwiseSimilarityEstimate(
        string speciesId,
        IReadOnlyList<SpeciationMembershipRecord> parentMemberships,
        SimilarityEvidence similarityEvidence,
        IReadOnlyList<Guid> inputOrderedParentBrainIds,
        out double pairwiseSimilarity)
    {
        pairwiseSimilarity = 0d;
        if (string.IsNullOrWhiteSpace(speciesId) || inputOrderedParentBrainIds.Count == 0)
        {
            return false;
        }

        var normalizedSpeciesId = speciesId.Trim();
        var parentSpeciesByBrainId = parentMemberships
            .Where(parent => !string.IsNullOrWhiteSpace(parent.SpeciesId))
            .ToDictionary(
                static parent => parent.BrainId,
                static parent => parent.SpeciesId.Trim());

        var pairwiseScores = new List<double>(Math.Min(2, inputOrderedParentBrainIds.Count));
        for (var index = 0; index < inputOrderedParentBrainIds.Count; index++)
        {
            var parentBrainId = inputOrderedParentBrainIds[index];
            if (!parentSpeciesByBrainId.TryGetValue(parentBrainId, out var parentSpeciesId)
                || !string.Equals(parentSpeciesId, normalizedSpeciesId, StringComparison.Ordinal)
                || !TryResolveParentSimilarityAtIndex(index, similarityEvidence, out var score))
            {
                continue;
            }

            pairwiseScores.Add(score);
        }

        if (pairwiseScores.Count == 0)
        {
            return false;
        }

        pairwiseSimilarity = ClampScore(pairwiseScores.Min());
        return true;
    }

    private async Task<BootstrapAssignedSpeciesAdmissionRequirement?> TryResolveBootstrapAssignedSpeciesAdmissionRequirementAsync(
        long epochId,
        AssignmentResolution assignmentResolution,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(assignmentResolution.SpeciesId)
            || string.Equals(assignmentResolution.Strategy, "explicit_species", StringComparison.Ordinal))
        {
            return null;
        }

        var targetSpeciesId = assignmentResolution.SpeciesId.Trim();
        if (targetSpeciesId.Length == 0)
        {
            return null;
        }

        var targetFloor = ResolveSpeciesSimilarityFloor(targetSpeciesId);
        if (!targetFloor.HasValue
            || targetFloor.Value.MembershipCount <= 0
            || targetFloor.Value.MembershipCount > DerivedSpeciesBootstrapMembershipLimit
            || targetFloor.Value.ActualSimilaritySampleCount >= DerivedSpeciesBootstrapActualSampleRequirement)
        {
            return null;
        }

        string? bootstrapSourceSpeciesId = null;
        string? bootstrapSourceSpeciesDisplayName = null;
        var targetHint = await TryResolveDerivedSpeciesHintByTargetSpeciesAsync(
            epochId,
            targetSpeciesId,
            cancellationToken).ConfigureAwait(false);
        if (targetHint.HasValue)
        {
            bootstrapSourceSpeciesId = targetHint.Value.SourceSpeciesId;
            bootstrapSourceSpeciesDisplayName = targetHint.Value.SourceSpeciesDisplayName;
        }
        else if (!string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
            && !string.Equals(
                targetSpeciesId,
                assignmentResolution.SourceSpeciesId.Trim(),
                StringComparison.Ordinal))
        {
            bootstrapSourceSpeciesId = assignmentResolution.SourceSpeciesId.Trim();
            bootstrapSourceSpeciesDisplayName = assignmentResolution.SourceSpeciesDisplayName;
        }

        if (string.IsNullOrWhiteSpace(bootstrapSourceSpeciesId)
            || string.Equals(targetSpeciesId, bootstrapSourceSpeciesId.Trim(), StringComparison.Ordinal))
        {
            return null;
        }

        return new BootstrapAssignedSpeciesAdmissionRequirement(
            targetSpeciesId,
            ResolveSpeciesDisplayName(assignmentResolution.SpeciesDisplayName, targetSpeciesId),
            bootstrapSourceSpeciesId.Trim(),
            ResolveSpeciesDisplayName(
                bootstrapSourceSpeciesDisplayName,
                bootstrapSourceSpeciesId.Trim()),
            targetFloor.Value.MembershipCount,
            targetFloor.Value.ActualSimilaritySampleCount);
    }

    private static bool RequiresActualAssignedSpeciesAdmission(
        AssignmentResolution assignmentResolution,
        bool bootstrapRequired)
    {
        return bootstrapRequired
            || (!string.IsNullOrWhiteSpace(assignmentResolution.SpeciesId)
            && !string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
            && !string.Equals(
                assignmentResolution.SpeciesId,
                assignmentResolution.SourceSpeciesId,
                StringComparison.Ordinal)
            && (string.Equals(
                    assignmentResolution.DecisionReason,
                    "lineage_realign_recent_split",
                    StringComparison.Ordinal)
                || string.Equals(
                    assignmentResolution.DecisionReason,
                    "lineage_reuse_recent_derived_species",
                    StringComparison.Ordinal)));
    }

    private AssignmentResolution BuildBootstrapFallbackAssignmentResolution(
        BootstrapAssignedSpeciesAdmissionRequirement requirement,
        double? fallbackSourceSpeciesSimilarityScore)
    {
        var fallbackThresholdState = ResolveSplitThresholdState(
            ResolveSpeciesSimilarityFloor(requirement.SourceSpeciesId));
        var splitTriggeredBySpeciesFloor = fallbackSourceSpeciesSimilarityScore.HasValue
            && fallbackThresholdState.UsesSpeciesFloor
            && ClampScore(fallbackSourceSpeciesSimilarityScore.Value) <= fallbackThresholdState.DynamicSplitThreshold
            && ClampScore(fallbackSourceSpeciesSimilarityScore.Value) > fallbackThresholdState.PolicyEffectiveSplitThreshold;

        return new AssignmentResolution(
            requirement.SourceSpeciesId,
            requirement.SourceSpeciesDisplayName,
            DecisionReason: "lineage_bootstrap_compatibility_required",
            Strategy: "lineage_bootstrap_fallback",
            StrategyDetail:
                "Derived species bootstrap required actual compatibility evidence; retained the pre-split source species.",
            ForceDecisionReason: true,
            PolicyEffectiveSplitThreshold: fallbackThresholdState.PolicyEffectiveSplitThreshold,
            EffectiveSplitThreshold: fallbackThresholdState.DynamicSplitThreshold,
            SplitTriggeredBySpeciesFloor: splitTriggeredBySpeciesFloor,
            SourceSpeciesId: requirement.SourceSpeciesId,
            SourceSpeciesDisplayName: requirement.SourceSpeciesDisplayName,
            SourceSpeciesSimilarityScore: fallbackSourceSpeciesSimilarityScore,
            SourceConsensusShare: 0d,
            SpeciesFloorSimilarityScore: fallbackThresholdState.SpeciesFloorSimilarityScore,
            SpeciesFloorSampleCount: fallbackThresholdState.SpeciesFloorSampleCount,
            SpeciesFloorMembershipCount: fallbackThresholdState.SpeciesFloorMembershipCount);
    }

    private async Task<AssignedSpeciesAdmissionAssessment> TryAssessActualAssignedSpeciesAdmissionAsync(
        IContext context,
        SpeciationEpochInfo epoch,
        ResolvedCandidate resolvedCandidate,
        AssignmentResolution assignmentResolution,
        BootstrapAssignedSpeciesAdmissionRequirement? bootstrapRequirement,
        double? sourceSpeciesSimilarityScore,
        IReadOnlyList<SpeciationMembershipRecord>? exemplarMemberships = null)
    {
        var assessmentMode = ResolveCompatibilityAssessmentMode(resolvedCandidate.CandidateMode);
        if (!TryBuildCompatibilitySubjectOptions(
                resolvedCandidate,
                out var candidateSubject,
                out var candidateFallbackSubject))
        {
            return CreateAssignedSpeciesAdmissionAssessment(
                assessmentAttempted: false,
                admitted: false,
                similarityScore: null,
                assessmentMode: assessmentMode,
                exemplarBrainIds: Array.Empty<string>(),
                compatible: false,
                abortReason: string.Empty,
                failureReason: "candidate_subject_unavailable",
                elapsedMs: 0L);
        }

        return await TryAssessActualAssignedSpeciesAdmissionAsync(
            context,
            epoch,
            candidateSubject,
            candidateFallbackSubject,
            resolvedCandidate.BrainId,
            assignmentResolution,
            bootstrapRequirement,
            sourceSpeciesSimilarityScore,
            exemplarMemberships).ConfigureAwait(false);
    }

    private async Task<AssignedSpeciesAdmissionAssessment> TryAssessActualAssignedSpeciesAdmissionAsync(
        IContext context,
        SpeciationEpochInfo epoch,
        CompatibilitySubject candidateSubject,
        CompatibilitySubject candidateFallbackSubject,
        Guid candidateBrainId,
        AssignmentResolution assignmentResolution,
        BootstrapAssignedSpeciesAdmissionRequirement? bootstrapRequirement,
        double? sourceSpeciesSimilarityScore,
        IReadOnlyList<SpeciationMembershipRecord>? exemplarMemberships = null)
    {
        var stopwatch = Stopwatch.StartNew();
        var assessmentMode = ResolveCompatibilityAssessmentMode(candidateSubject.Kind);
        AssignedSpeciesAdmissionAssessment BuildAssessment(
            bool assessmentAttempted,
            bool admitted,
            double? similarityScore,
            IReadOnlyList<string> exemplarBrainIds,
            bool compatible,
            string abortReason,
            string failureReason)
            => CreateAssignedSpeciesAdmissionAssessment(
                assessmentAttempted,
                admitted,
                similarityScore,
                assessmentMode,
                exemplarBrainIds,
                compatible,
                abortReason,
                failureReason,
                ResolveAssignedSpeciesAdmissionElapsedMs(stopwatch, failureReason));

        if (_reproductionManagerPid is null
            || epoch.EpochId <= 0
            || string.IsNullOrWhiteSpace(assignmentResolution.SpeciesId))
        {
            var failureReason = _reproductionManagerPid is null
                ? "reproduction_unavailable"
                : epoch.EpochId <= 0
                    ? "epoch_unavailable"
                    : "assigned_species_unavailable";
            return BuildAssessment(
                assessmentAttempted: false,
                admitted: false,
                similarityScore: null,
                exemplarBrainIds: Array.Empty<string>(),
                compatible: false,
                abortReason: string.Empty,
                failureReason: failureReason);
        }

        var exemplars = exemplarMemberships;
        if (exemplars is null)
        {
            exemplars = await _store.ListEarliestMembershipsForSpeciesAsync(
                epoch.EpochId,
                assignmentResolution.SpeciesId,
                ExternalAdmissionExemplarLimit).ConfigureAwait(false);
        }

        if (exemplars.Count == 0)
        {
            return BuildAssessment(
                assessmentAttempted: false,
                admitted: false,
                similarityScore: null,
                exemplarBrainIds: Array.Empty<string>(),
                compatible: false,
                abortReason: string.Empty,
                failureReason: "assigned_species_exemplars_unavailable");
        }

        if (bootstrapRequirement.HasValue && exemplars.Count > 1)
        {
            exemplars = exemplars
                .OrderBy(record => record.AssignedMs)
                .ThenBy(record => record.BrainId)
                .Take(1)
                .ToArray();
        }

        var attemptedExemplarBrainIds = new List<string>(exemplars.Count);
        var exemplarSimilarities = new List<double>(exemplars.Count);
        var assessmentAttempted = false;
        var allAssessmentsCompatible = true;
        string? firstAssessmentAbortReason = null;
        foreach (var exemplar in exemplars)
        {
            if (candidateBrainId != Guid.Empty
                && exemplar.BrainId == candidateBrainId)
            {
                continue;
            }

            attemptedExemplarBrainIds.Add(exemplar.BrainId.ToString("D"));
            if (!TryBuildCompatibilitySubjectOptions(
                    exemplar,
                    out var exemplarSubject,
                    out var exemplarFallbackSubject)
                || !TrySelectCompatibleSubjects(
                    candidateSubject,
                    candidateFallbackSubject,
                    exemplarSubject,
                    exemplarFallbackSubject,
                    out var selectedCandidateSubject,
                    out var selectedExemplarSubject))
            {
                return BuildAssessment(
                    assessmentAttempted: assessmentAttempted,
                    admitted: false,
                    similarityScore: null,
                    exemplarBrainIds: attemptedExemplarBrainIds,
                    compatible: assessmentAttempted && allAssessmentsCompatible,
                    abortReason: firstAssessmentAbortReason ?? string.Empty,
                    failureReason: "assigned_species_exemplar_subject_unavailable");
            }

            var assessment = await TryAssessCompatibilitySimilarityAsync(
                context,
                selectedCandidateSubject,
                selectedExemplarSubject).ConfigureAwait(false);
            assessmentAttempted |= assessment.RequestAttempted;
            allAssessmentsCompatible &= assessment.Compatible;
            if (!string.IsNullOrWhiteSpace(assessment.AssessmentMode))
            {
                assessmentMode = assessment.AssessmentMode;
            }
            if (string.IsNullOrWhiteSpace(firstAssessmentAbortReason)
                && !string.IsNullOrWhiteSpace(assessment.AbortReason))
            {
                firstAssessmentAbortReason = assessment.AbortReason;
            }

            if (!assessment.SimilarityScore.HasValue)
            {
                return BuildAssessment(
                    assessmentAttempted: assessmentAttempted,
                    admitted: false,
                    similarityScore: null,
                    exemplarBrainIds: attemptedExemplarBrainIds,
                    compatible: assessmentAttempted && allAssessmentsCompatible,
                    abortReason: firstAssessmentAbortReason ?? string.Empty,
                    failureReason: string.IsNullOrWhiteSpace(assessment.FailureReason)
                        ? "compatibility_similarity_unavailable"
                        : assessment.FailureReason);
            }

            exemplarSimilarities.Add(assessment.SimilarityScore.Value);
        }

        if (exemplarSimilarities.Count == 0)
        {
            return BuildAssessment(
                assessmentAttempted: assessmentAttempted,
                admitted: false,
                similarityScore: null,
                exemplarBrainIds: attemptedExemplarBrainIds,
                compatible: assessmentAttempted && allAssessmentsCompatible,
                abortReason: firstAssessmentAbortReason ?? string.Empty,
                failureReason: "assigned_species_exemplars_unavailable");
        }

        var assignedSimilarity = ClampScore(exemplarSimilarities.Min());
        var targetThresholdState = ResolveSplitThresholdState(
            ResolveSpeciesSimilarityFloor(assignmentResolution.SpeciesId));
        var minimumAssignedSimilarity = targetThresholdState.DynamicSplitThreshold;
        var sourceComparisonMargin = bootstrapRequirement.HasValue
            ? Math.Max(0d, _assignmentPolicy.LineageSplitGuardMargin)
            : 0d;

        if (assignedSimilarity <= 0d
            || assignedSimilarity < minimumAssignedSimilarity)
        {
            return BuildAssessment(
                assessmentAttempted: assessmentAttempted,
                admitted: false,
                similarityScore: assignedSimilarity,
                exemplarBrainIds: attemptedExemplarBrainIds,
                compatible: allAssessmentsCompatible,
                abortReason: firstAssessmentAbortReason ?? string.Empty,
                failureReason: "compatibility_similarity_below_threshold");
        }

        if (sourceSpeciesSimilarityScore.HasValue
            && assignedSimilarity + sourceComparisonMargin <= ClampScore(sourceSpeciesSimilarityScore.Value))
        {
            return BuildAssessment(
                assessmentAttempted: assessmentAttempted,
                admitted: false,
                similarityScore: assignedSimilarity,
                exemplarBrainIds: attemptedExemplarBrainIds,
                compatible: allAssessmentsCompatible,
                abortReason: firstAssessmentAbortReason ?? string.Empty,
                failureReason: "compatibility_similarity_not_better_than_source");
        }

        return BuildAssessment(
            assessmentAttempted: assessmentAttempted,
            admitted: true,
            similarityScore: assignedSimilarity,
            exemplarBrainIds: attemptedExemplarBrainIds,
            compatible: allAssessmentsCompatible,
            abortReason: firstAssessmentAbortReason ?? string.Empty,
            failureReason: string.Empty);
    }

    private async Task<CompatibilitySimilarityAssessment> TryAssessCompatibilitySimilarityAsync(
        IContext context,
        CompatibilitySubject candidateSubject,
        CompatibilitySubject exemplarSubject)
    {
        if (_reproductionManagerPid is null
            || candidateSubject.Kind != exemplarSubject.Kind)
        {
            return new CompatibilitySimilarityAssessment(
                RequestAttempted: false,
                SimilarityScore: null,
                Compatible: false,
                AbortReason: string.Empty,
                FailureReason: _reproductionManagerPid is null
                    ? "reproduction_unavailable"
                    : "compatibility_subject_kind_mismatch",
                AssessmentMode: string.Empty);
        }

        try
        {
            ProtoRepro.ReproduceResult response;
            if (candidateSubject.Kind == CompatibilitySubjectKind.BrainId)
            {
                response = await context.RequestAsync<ProtoRepro.ReproduceResult>(
                    _reproductionManagerPid,
                    new ProtoRepro.AssessCompatibilityByBrainIdsRequest
                    {
                        ParentA = candidateSubject.BrainId.ToProtoUuid(),
                        ParentB = exemplarSubject.BrainId.ToProtoUuid(),
                        Config = CreateCompatibilityAssessmentConfig(),
                        RunCount = 1
                    },
                    _compatibilityRequestTimeout).ConfigureAwait(false);
            }
            else
            {
                var request = new ProtoRepro.AssessCompatibilityByArtifactsRequest
                {
                    ParentADef = candidateSubject.ArtifactDefRef!.Clone(),
                    ParentBDef = exemplarSubject.ArtifactDefRef!.Clone(),
                    Config = CreateCompatibilityAssessmentConfig(),
                    RunCount = 1
                };
                if (HasUsableArtifactReference(candidateSubject.ArtifactStateRef))
                {
                    request.ParentAState = candidateSubject.ArtifactStateRef!.Clone();
                }

                if (HasUsableArtifactReference(exemplarSubject.ArtifactStateRef))
                {
                    request.ParentBState = exemplarSubject.ArtifactStateRef!.Clone();
                }

                response = await context.RequestAsync<ProtoRepro.ReproduceResult>(
                    _reproductionManagerPid,
                    request,
                    _compatibilityRequestTimeout).ConfigureAwait(false);
            }

            if (response?.Report is null
                || float.IsNaN(response.Report.SimilarityScore)
                || float.IsInfinity(response.Report.SimilarityScore))
            {
                return new CompatibilitySimilarityAssessment(
                    RequestAttempted: true,
                    SimilarityScore: null,
                    Compatible: false,
                    AbortReason: string.Empty,
                    FailureReason: "compatibility_response_invalid",
                    AssessmentMode: ResolveCompatibilityAssessmentMode(candidateSubject.Kind));
            }

            var abortReason = NormalizeCompatibilityAbortReason(response.Report.AbortReason);
            var similarityScore = ClampScore(response.Report.SimilarityScore);
            if (!response.Report.Compatible
                && !ShouldUseCompatibilitySimilarityScore(abortReason))
            {
                return new CompatibilitySimilarityAssessment(
                    RequestAttempted: true,
                    SimilarityScore: null,
                    Compatible: false,
                    AbortReason: abortReason,
                    FailureReason: "compatibility_infrastructure_failure",
                    AssessmentMode: ResolveCompatibilityAssessmentMode(candidateSubject.Kind));
            }

            return new CompatibilitySimilarityAssessment(
                RequestAttempted: true,
                SimilarityScore: similarityScore,
                Compatible: response.Report.Compatible,
                AbortReason: abortReason,
                FailureReason: string.Empty,
                AssessmentMode: ResolveCompatibilityAssessmentMode(candidateSubject.Kind));
        }
        catch (Exception ex) when (ex is TimeoutException or TaskCanceledException or OperationCanceledException)
        {
            return new CompatibilitySimilarityAssessment(
                RequestAttempted: true,
                SimilarityScore: null,
                Compatible: false,
                AbortReason: "repro_request_timeout",
                FailureReason: "compatibility_request_timeout",
                AssessmentMode: ResolveCompatibilityAssessmentMode(candidateSubject.Kind));
        }
        catch
        {
            return new CompatibilitySimilarityAssessment(
                RequestAttempted: true,
                SimilarityScore: null,
                Compatible: false,
                AbortReason: "repro_request_failed",
                FailureReason: "compatibility_request_failed",
                AssessmentMode: ResolveCompatibilityAssessmentMode(candidateSubject.Kind));
        }
    }

    private static string ResolveCompatibilityAssessmentMode(ProtoSpec.SpeciationCandidateMode candidateMode)
        => candidateMode switch
        {
            ProtoSpec.SpeciationCandidateMode.BrainId => "brain_ids",
            ProtoSpec.SpeciationCandidateMode.ArtifactRef => "artifacts",
            _ => string.Empty
        };

    private static string ResolveCompatibilityAssessmentMode(CompatibilitySubjectKind kind)
        => kind == CompatibilitySubjectKind.BrainId
            ? "brain_ids"
            : kind == CompatibilitySubjectKind.ArtifactRef
                ? "artifacts"
                : string.Empty;

    private static AssignedSpeciesAdmissionAssessment CreateAssignedSpeciesAdmissionAssessment(
        bool assessmentAttempted,
        bool admitted,
        double? similarityScore,
        string assessmentMode,
        IReadOnlyList<string> exemplarBrainIds,
        bool compatible,
        string abortReason,
        string failureReason,
        long elapsedMs)
        => new(
            AssessmentAttempted: assessmentAttempted,
            Admitted: admitted,
            SimilarityScore: similarityScore,
            AssessmentMode: assessmentMode,
            ExemplarBrainIds: exemplarBrainIds.Count == 0 ? Array.Empty<string>() : exemplarBrainIds.ToArray(),
            Compatible: compatible,
            AbortReason: abortReason,
            FailureReason: failureReason,
            ElapsedMs: elapsedMs);

    private long ResolveAssignedSpeciesAdmissionElapsedMs(Stopwatch stopwatch, string failureReason)
    {
        ArgumentNullException.ThrowIfNull(stopwatch);

        var elapsedMs = (long)Math.Ceiling(stopwatch.Elapsed.TotalMilliseconds);
        if (elapsedMs < 0)
        {
            elapsedMs = 0;
        }

        if (!string.Equals(failureReason, "compatibility_request_timeout", StringComparison.Ordinal))
        {
            return elapsedMs;
        }

        var timeoutFloorMs = GetPositiveCeilingMilliseconds(_compatibilityRequestTimeout);
        return timeoutFloorMs > 0
            ? Math.Max(elapsedMs, timeoutFloorMs)
            : elapsedMs;
    }

    private static long GetPositiveCeilingMilliseconds(TimeSpan value)
    {
        if (value <= TimeSpan.Zero)
        {
            return 0;
        }

        var totalMilliseconds = value.TotalMilliseconds;
        return double.IsNaN(totalMilliseconds) || double.IsInfinity(totalMilliseconds) || totalMilliseconds <= 0d
            ? 0
            : (long)Math.Ceiling(totalMilliseconds);
    }

    private static string NormalizeCompatibilityAbortReason(string? abortReason)
        => string.IsNullOrWhiteSpace(abortReason)
            ? string.Empty
            : abortReason.Trim();

    private static bool ShouldUseCompatibilitySimilarityScore(string abortReason)
    {
        if (string.IsNullOrWhiteSpace(abortReason))
        {
            return true;
        }

        return !IsCompatibilityInfrastructureFailure(abortReason);
    }

    private static bool IsCompatibilityInfrastructureFailure(string abortReason)
    {
        return abortReason switch
        {
            "repro_internal_error" => true,
            "repro_parent_resolution_unavailable" => true,
            "repro_run_count_out_of_range" => true,
            "repro_missing_parent_brain_ids" => true,
            "repro_parent_a_brain_id_invalid" => true,
            "repro_parent_b_brain_id_invalid" => true,
            "repro_missing_parent_a_def" => true,
            "repro_missing_parent_b_def" => true,
            _ when abortReason.EndsWith("_lookup_failed", StringComparison.Ordinal)
                || abortReason.EndsWith("_brain_not_found", StringComparison.Ordinal)
                || abortReason.EndsWith("_base_def_missing", StringComparison.Ordinal)
                || abortReason.EndsWith("_media_type_invalid", StringComparison.Ordinal)
                || abortReason.EndsWith("_sha256_invalid", StringComparison.Ordinal)
                || abortReason.EndsWith("_artifact_not_found", StringComparison.Ordinal)
                || abortReason.EndsWith("_parse_failed", StringComparison.Ordinal)
                || abortReason.EndsWith("_validation_failed", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_ref_missing", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_media_type_invalid", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_sha256_invalid", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_artifact_not_found", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_parse_failed", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_incompatible_with_base", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_overlay_missing", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_overlay_empty", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_overlay_no_matching_routes", StringComparison.Ordinal) => true,
            _ => false
        };
    }

    private ProtoRepro.ReproduceConfig CreateCompatibilityAssessmentConfig()
        => _compatibilityAssessmentConfig.Clone();

    private static bool TryBuildCompatibilitySubjectOptions(
        ResolvedCandidate resolvedCandidate,
        out CompatibilitySubject preferredSubject,
        out CompatibilitySubject fallbackSubject)
    {
        preferredSubject = default;
        fallbackSubject = default;
        if (!TryBuildCompatibilitySubject(
                resolvedCandidate,
                preferArtifacts: true,
                out preferredSubject))
        {
            return false;
        }

        if (!TryBuildCompatibilitySubject(
                resolvedCandidate,
                preferArtifacts: false,
                out fallbackSubject))
        {
            fallbackSubject = preferredSubject;
        }

        return true;
    }

    private static bool TryBuildCompatibilitySubject(
        ResolvedCandidate resolvedCandidate,
        bool preferArtifacts,
        out CompatibilitySubject subject)
    {
        subject = default;
        switch (resolvedCandidate.CandidateMode)
        {
            case ProtoSpec.SpeciationCandidateMode.BrainId when resolvedCandidate.BrainId != Guid.Empty:
                if (preferArtifacts
                    && CanAssessArtifactReference(resolvedCandidate.CandidateBrainBaseArtifactRef))
                {
                    subject = new CompatibilitySubject(
                        CompatibilitySubjectKind.ArtifactRef,
                        Guid.Empty,
                        resolvedCandidate.CandidateBrainBaseArtifactRef!.Clone(),
                        HasUsableArtifactReference(resolvedCandidate.CandidateBrainSnapshotArtifactRef)
                            ? resolvedCandidate.CandidateBrainSnapshotArtifactRef!.Clone()
                            : null);
                    return true;
                }

                subject = new CompatibilitySubject(
                    CompatibilitySubjectKind.BrainId,
                    resolvedCandidate.BrainId,
                    ArtifactDefRef: null,
                    ArtifactStateRef: null);
                return true;
            case ProtoSpec.SpeciationCandidateMode.ArtifactRef
                when CanAssessArtifactReference(resolvedCandidate.CandidateArtifactRef):
                subject = new CompatibilitySubject(
                    CompatibilitySubjectKind.ArtifactRef,
                    Guid.Empty,
                    resolvedCandidate.CandidateArtifactRef!.Clone(),
                    ArtifactStateRef: null);
                return true;
            default:
                return false;
        }
    }

    private static bool TryBuildCompatibilitySubjectOptions(
        SpeciationMembershipRecord membership,
        out CompatibilitySubject preferredSubject,
        out CompatibilitySubject fallbackSubject)
    {
        preferredSubject = default;
        fallbackSubject = default;
        if (!TryBuildCompatibilitySubject(
                membership,
                preferArtifacts: true,
                out preferredSubject))
        {
            return false;
        }

        if (!TryBuildCompatibilitySubject(
                membership,
                preferArtifacts: false,
                out fallbackSubject))
        {
            fallbackSubject = preferredSubject;
        }

        return true;
    }

    private static bool TryBuildCompatibilitySubject(
        SpeciationMembershipRecord membership,
        bool preferArtifacts,
        out CompatibilitySubject subject)
    {
        subject = default;
        if (membership.BrainId == Guid.Empty)
        {
            return false;
        }

        if (!TryExtractCandidateMode(membership.DecisionMetadataJson, out var candidateMode))
        {
            if (preferArtifacts
                && TryExtractStoredCandidateBrainArtifactRefs(
                    membership.DecisionMetadataJson,
                    out var storedBaseArtifactRef,
                    out var storedSnapshotArtifactRef))
            {
                subject = new CompatibilitySubject(
                    CompatibilitySubjectKind.ArtifactRef,
                    Guid.Empty,
                    storedBaseArtifactRef,
                    storedSnapshotArtifactRef);
                return true;
            }

            subject = new CompatibilitySubject(
                CompatibilitySubjectKind.BrainId,
                membership.BrainId,
                ArtifactDefRef: null,
                ArtifactStateRef: null);
            return true;
        }

        switch (candidateMode)
        {
            case ProtoSpec.SpeciationCandidateMode.BrainId:
                if (preferArtifacts
                    && TryExtractStoredCandidateBrainArtifactRefs(
                        membership.DecisionMetadataJson,
                        out var storedBaseArtifactRef,
                        out var storedSnapshotArtifactRef))
                {
                    subject = new CompatibilitySubject(
                        CompatibilitySubjectKind.ArtifactRef,
                        Guid.Empty,
                        storedBaseArtifactRef,
                        storedSnapshotArtifactRef);
                    return true;
                }

                subject = new CompatibilitySubject(
                    CompatibilitySubjectKind.BrainId,
                    membership.BrainId,
                    ArtifactDefRef: null,
                    ArtifactStateRef: null);
                return true;
            case ProtoSpec.SpeciationCandidateMode.ArtifactRef when TryExtractStoredCandidateArtifactRef(
                membership.DecisionMetadataJson,
                out var artifactRef)
                && CanAssessArtifactReference(artifactRef):
                subject = new CompatibilitySubject(
                    CompatibilitySubjectKind.ArtifactRef,
                    Guid.Empty,
                    artifactRef,
                    ArtifactStateRef: null);
                return true;
            default:
                return false;
        }
    }

    private static bool TrySelectCompatibleSubjects(
        CompatibilitySubject preferredCandidateSubject,
        CompatibilitySubject fallbackCandidateSubject,
        CompatibilitySubject preferredExemplarSubject,
        CompatibilitySubject fallbackExemplarSubject,
        out CompatibilitySubject selectedCandidateSubject,
        out CompatibilitySubject selectedExemplarSubject)
    {
        selectedCandidateSubject = default;
        selectedExemplarSubject = default;

        CompatibilitySubject[] candidateOptions = [preferredCandidateSubject, fallbackCandidateSubject];
        CompatibilitySubject[] exemplarOptions = [preferredExemplarSubject, fallbackExemplarSubject];
        foreach (var candidateOption in candidateOptions)
        {
            if (candidateOption.Kind == CompatibilitySubjectKind.None)
            {
                continue;
            }

            foreach (var exemplarOption in exemplarOptions)
            {
                if (candidateOption.Kind != exemplarOption.Kind
                    || exemplarOption.Kind == CompatibilitySubjectKind.None)
                {
                    continue;
                }

                selectedCandidateSubject = candidateOption;
                selectedExemplarSubject = exemplarOption;
                return true;
            }
        }

        return false;
    }

    private static bool CanAssessArtifactReference(ArtifactRef? artifactRef)
    {
        return artifactRef is not null
            && artifactRef.TryToSha256Hex(out _);
    }

    private static JsonObject BuildStoredArtifactRefNode(ArtifactRef artifactRef)
    {
        var node = new JsonObject
        {
            ["size_bytes"] = artifactRef.SizeBytes
        };
        if (artifactRef.TryToSha256Hex(out var sha))
        {
            node["sha256_hex"] = sha;
        }

        if (!string.IsNullOrWhiteSpace(artifactRef.MediaType))
        {
            node["media_type"] = artifactRef.MediaType.Trim();
        }

        if (!string.IsNullOrWhiteSpace(artifactRef.StoreUri))
        {
            node["store_uri"] = artifactRef.StoreUri.Trim();
        }

        return node;
    }

    private static bool TryExtractCandidateMode(
        string? decisionMetadataJson,
        out ProtoSpec.SpeciationCandidateMode candidateMode)
    {
        candidateMode = ProtoSpec.SpeciationCandidateMode.Unknown;
        if (string.IsNullOrWhiteSpace(decisionMetadataJson))
        {
            return false;
        }

        JsonNode? node;
        try
        {
            node = JsonNode.Parse(decisionMetadataJson);
        }
        catch (JsonException)
        {
            return false;
        }

        JsonNode? lineageNode = null;
        if (node is JsonObject root)
        {
            root.TryGetPropertyValue("lineage", out lineageNode);
        }

        var candidateModeText = FindStringValue(
            lineageNode,
            "candidate_mode",
            "candidateMode")
            ?? FindStringValue(
                node,
                "candidate_mode",
                "candidateMode");
        return !string.IsNullOrWhiteSpace(candidateModeText)
            && Enum.TryParse(candidateModeText.Trim(), ignoreCase: true, out candidateMode)
            && candidateMode != ProtoSpec.SpeciationCandidateMode.Unknown;
    }

    private static bool TryExtractStoredCandidateArtifactRef(
        string? decisionMetadataJson,
        out ArtifactRef artifactRef)
        => TryExtractStoredArtifactRef(
            decisionMetadataJson,
            "candidate_artifact_ref",
            out artifactRef);

    private static bool TryExtractStoredCandidateBrainArtifactRefs(
        string? decisionMetadataJson,
        out ArtifactRef baseArtifactRef,
        out ArtifactRef? snapshotArtifactRef)
    {
        snapshotArtifactRef = null;
        if (!TryExtractStoredArtifactRef(
                decisionMetadataJson,
                "candidate_brain_base_artifact_ref",
                out baseArtifactRef))
        {
            return false;
        }

        if (TryExtractStoredArtifactRef(
                decisionMetadataJson,
                "candidate_brain_snapshot_artifact_ref",
                out var storedSnapshotArtifactRef))
        {
            snapshotArtifactRef = storedSnapshotArtifactRef;
        }

        return true;
    }

    private static bool TryExtractStoredArtifactRef(
        string? decisionMetadataJson,
        string propertyName,
        out ArtifactRef artifactRef)
    {
        artifactRef = new ArtifactRef();
        if (string.IsNullOrWhiteSpace(decisionMetadataJson))
        {
            return false;
        }

        JsonNode? node;
        try
        {
            node = JsonNode.Parse(decisionMetadataJson);
        }
        catch (JsonException)
        {
            return false;
        }

        JsonNode? lineageNode = null;
        if (node is JsonObject root)
        {
            root.TryGetPropertyValue("lineage", out lineageNode);
        }

        if (lineageNode is not JsonObject lineage
            || !lineage.TryGetPropertyValue(propertyName, out var artifactNode)
            || artifactNode is not JsonObject artifactObject)
        {
            return false;
        }

        var sha256Hex = FindStringValue(
            artifactObject,
            "sha256_hex",
            "sha256Hex");
        var mediaType = FindStringValue(
            artifactObject,
            "media_type",
            "mediaType");
        var storeUri = FindStringValue(
            artifactObject,
            "store_uri",
            "storeUri");
        var sizeBytes = FindNumericValue(
            artifactObject,
            "size_bytes",
            "sizeBytes");

        if (!string.IsNullOrWhiteSpace(sha256Hex))
        {
            artifactRef = sha256Hex.Trim().ToArtifactRef(
                sizeBytes.HasValue ? (ulong)Math.Max(0d, sizeBytes.Value) : 0UL,
                mediaType,
                storeUri);
            return true;
        }

        if (string.IsNullOrWhiteSpace(storeUri))
        {
            return false;
        }

        artifactRef = new ArtifactRef
        {
            SizeBytes = sizeBytes.HasValue ? (ulong)Math.Max(0d, sizeBytes.Value) : 0UL,
            MediaType = mediaType ?? string.Empty,
            StoreUri = storeUri.Trim()
        };
        return HasUsableArtifactReference(artifactRef);
    }

    private static bool TryResolveParentSimilarityAtIndex(
        int parentIndex,
        SimilarityEvidence similarityEvidence,
        out double similarityScore)
    {
        similarityScore = 0d;
        return parentIndex switch
        {
            0 when similarityEvidence.ParentASimilarityScore.HasValue =>
                TryNormalizeSimilarity(similarityEvidence.ParentASimilarityScore.Value, out similarityScore),
            1 when similarityEvidence.ParentBSimilarityScore.HasValue =>
                TryNormalizeSimilarity(similarityEvidence.ParentBSimilarityScore.Value, out similarityScore),
            _ => false
        };
    }

    private static bool TryNormalizeSimilarity(double rawScore, out double similarityScore)
    {
        similarityScore = ClampScore(rawScore);
        return double.IsFinite(similarityScore);
    }

    private static double? TryExtractIntraSpeciesSimilaritySample(SpeciationMembershipRecord membership)
    {
        if (string.Equals(membership.DecisionReason, "explicit_species", StringComparison.Ordinal))
        {
            return null;
        }

        if (string.IsNullOrWhiteSpace(membership.SpeciesId)
            || string.IsNullOrWhiteSpace(membership.DecisionMetadataJson))
        {
            return null;
        }

        JsonNode? node;
        try
        {
            node = JsonNode.Parse(membership.DecisionMetadataJson);
        }
        catch (JsonException)
        {
            return null;
        }

        if (node is not JsonObject root)
        {
            return null;
        }

        root.TryGetPropertyValue("lineage", out var lineageNode);
        var explicitIntraSpeciesSample = FindNumericValue(
            lineageNode,
            "intra_species_similarity_sample",
            "intraSpeciesSimilaritySample");
        if (explicitIntraSpeciesSample.HasValue)
        {
            var explicitSampleSpeciesId = FindStringValue(
                lineageNode,
                "intra_species_similarity_species_id",
                "intraSpeciesSimilaritySpeciesId");
            if (string.IsNullOrWhiteSpace(explicitSampleSpeciesId)
                || string.Equals(
                    explicitSampleSpeciesId.Trim(),
                    membership.SpeciesId.Trim(),
                    StringComparison.Ordinal))
            {
                return ClampScore(explicitIntraSpeciesSample.Value);
            }
        }

        if (string.Equals(membership.DecisionReason, "lineage_diverged_new_species", StringComparison.Ordinal))
        {
            return 1d;
        }

        var sourceSpeciesId = FindStringValue(
            lineageNode,
            "source_species_id",
            "sourceSpeciesId")
            ?? FindStringValue(
            lineageNode,
            "dominant_species_id",
            "dominantSpeciesId");
        var sourceShare = FindNumericValue(
            lineageNode,
            "source_species_share",
            "sourceSpeciesShare")
            ?? FindNumericValue(
            lineageNode,
            "dominant_species_share",
            "dominantSpeciesShare");
        var normalizedSourceSpeciesId = string.IsNullOrWhiteSpace(sourceSpeciesId)
            ? string.Empty
            : sourceSpeciesId.Trim();
        if (string.IsNullOrWhiteSpace(sourceSpeciesId)
            || !sourceShare.HasValue
            || sourceShare.Value < 0.999999d
            || !string.Equals(
                membership.SpeciesId.Trim(),
                normalizedSourceSpeciesId,
                StringComparison.Ordinal))
        {
            return null;
        }

        var sourceSimilarityScore = FindNumericValue(
            lineageNode,
            "source_species_similarity_score",
            "sourceSpeciesSimilarityScore");
        if (sourceSimilarityScore.HasValue)
        {
            return ClampScore(sourceSimilarityScore.Value);
        }

        var similarityEvidence = ExtractSimilarityEvidence(membership.DecisionMetadataJson);
        if (similarityEvidence.DominantSpeciesSimilarityScore.HasValue)
        {
            return ClampScore(similarityEvidence.DominantSpeciesSimilarityScore.Value);
        }

        return similarityEvidence.SimilarityScore.HasValue
            ? ClampScore(similarityEvidence.SimilarityScore.Value)
            : null;
    }

    private static bool HasActualAssignedSpeciesSimilaritySample(SpeciationMembershipRecord membership)
    {
        if (membership is null
            || string.IsNullOrWhiteSpace(membership.SpeciesId)
            || string.IsNullOrWhiteSpace(membership.DecisionMetadataJson))
        {
            return false;
        }

        JsonNode? node;
        try
        {
            node = JsonNode.Parse(membership.DecisionMetadataJson);
        }
        catch (JsonException)
        {
            return false;
        }

        if (node is not JsonObject root)
        {
            return false;
        }

        root.TryGetPropertyValue("lineage", out var lineageNode);
        var assignedSimilaritySource = FindStringValue(
            lineageNode,
            "assigned_species_similarity_source",
            "assignedSpeciesSimilaritySource");
        if (!string.Equals(
                assignedSimilaritySource,
                "compatibility_assessment",
                StringComparison.Ordinal))
        {
            return false;
        }

        var sampleSpeciesId = FindStringValue(
            lineageNode,
            "intra_species_similarity_species_id",
            "intraSpeciesSimilaritySpeciesId");
        return string.IsNullOrWhiteSpace(sampleSpeciesId)
            || string.Equals(
                sampleSpeciesId.Trim(),
                membership.SpeciesId.Trim(),
                StringComparison.Ordinal);
    }

    private async Task<int> ApplySplitHindsightReassignmentsAsync(
        IContext context,
        SpeciationEpochInfo epoch,
        SpeciationMembershipRecord splitMembership,
        AssignmentResolution assignmentResolution,
        double? assignmentSimilarityScore,
        string policyVersion,
        long? decisionTimeMs)
    {
        if (_assignmentPolicy.HindsightReassignCommitWindow <= 0
            || !assignmentSimilarityScore.HasValue
            || string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
            || splitMembership.BrainId == Guid.Empty)
        {
            return 0;
        }

        var targetSpeciesFloor = ResolveSpeciesSimilarityFloor(assignmentResolution.SpeciesId);
        if (!targetSpeciesFloor.HasValue || targetSpeciesFloor.Value.MembershipCount != 1)
        {
            return 0;
        }

        var sourceSpeciesId = assignmentResolution.SourceSpeciesId.Trim();
        if (sourceSpeciesId.Length == 0
            || string.Equals(sourceSpeciesId, assignmentResolution.SpeciesId, StringComparison.Ordinal))
        {
            return 0;
        }

        var founderSimilarity = ClampScore(assignmentSimilarityScore.Value);
        var recent = await _store.ListRecentMembershipsForSpeciesAsync(
            epoch.EpochId,
            sourceSpeciesId,
            splitMembership.AssignedMs,
            _assignmentPolicy.HindsightReassignCommitWindow).ConfigureAwait(false);
        if (recent.Count == 0)
        {
            return 0;
        }

        var reassigned = 0;
        var similarityMargin = Math.Max(0d, _assignmentPolicy.HindsightReassignSimilarityMargin);
        var maxHindsightCandidates = Math.Max(1, DerivedSpeciesBootstrapActualSampleRequirement);
        var orderedCandidates = recent
            .Where(candidate => candidate.BrainId != splitMembership.BrainId)
            .OrderBy(candidate => candidate.AssignedMs)
            .ThenBy(candidate => candidate.BrainId)
            .ToArray();
        var eligibleCandidates = new List<(SpeciationMembershipRecord Candidate, double Similarity)>(
            Math.Min(maxHindsightCandidates, orderedCandidates.Length));
        for (var index = 0; index < orderedCandidates.Length; index++)
        {
            var candidate = orderedCandidates[index];
            if (!TryExtractSourceSpeciesSimilarity(
                    candidate.DecisionMetadataJson,
                    out var candidateSourceSpeciesId,
                    out var candidateSimilarity)
                || !string.Equals(candidateSourceSpeciesId, sourceSpeciesId, StringComparison.Ordinal))
            {
                continue;
            }

            if (candidateSimilarity >= _assignmentPolicy.LineageMatchThreshold)
            {
                continue;
            }

            var hindsightLowerSimilarity = Math.Max(0d, founderSimilarity - similarityMargin);
            var hindsightUpperSimilarity = Math.Min(1d, founderSimilarity + similarityMargin);
            if (candidateSimilarity < hindsightLowerSimilarity
                || candidateSimilarity > hindsightUpperSimilarity)
            {
                continue;
            }

            eligibleCandidates.Add((candidate, candidateSimilarity));
            if (eligibleCandidates.Count >= maxHindsightCandidates)
            {
                break;
            }
        }

        var bootstrapRequirement = await TryResolveBootstrapAssignedSpeciesAdmissionRequirementAsync(
            epoch.EpochId,
            assignmentResolution).ConfigureAwait(false);
        var bootstrapExemplars = await _store.ListEarliestMembershipsForSpeciesAsync(
            epoch.EpochId,
            assignmentResolution.SpeciesId,
            ExternalAdmissionExemplarLimit).ConfigureAwait(false);
        if (bootstrapExemplars.Count == 0)
        {
            return 0;
        }

        for (var index = 0; index < eligibleCandidates.Count; index++)
        {
            var (candidate, candidateSimilarity) = eligibleCandidates[index];
            if (!TryBuildCompatibilitySubjectOptions(
                    candidate,
                    out var candidateSubject,
                    out var candidateFallbackSubject))
            {
                continue;
            }

            var assignedSpeciesAdmissionAssessment = await TryAssessActualAssignedSpeciesAdmissionAsync(
                context,
                epoch,
                candidateSubject,
                candidateFallbackSubject,
                candidate.BrainId,
                assignmentResolution,
                bootstrapRequirement,
                candidateSimilarity,
                bootstrapExemplars).ConfigureAwait(false);
            if (!assignedSpeciesAdmissionAssessment.Admitted)
            {
                continue;
            }

            var reassignmentMetadataJson = BuildHindsightReassignDecisionMetadataJson(
                candidate,
                splitMembership,
                sourceSpeciesId,
                assignmentResolution.SourceSpeciesDisplayName,
                assignmentResolution,
                policyVersion,
                founderSimilarity,
                candidateSimilarity,
                assignedSpeciesAdmissionAssessment);
            var reassignmentTimeMs = Math.Max(
                splitMembership.AssignedMs + 1L + index,
                decisionTimeMs.HasValue
                    ? decisionTimeMs.Value + 1L + index
                    : splitMembership.AssignedMs + 1L + index);

            var reassignment = new SpeciationAssignment(
                BrainId: candidate.BrainId,
                SpeciesId: assignmentResolution.SpeciesId,
                SpeciesDisplayName: assignmentResolution.SpeciesDisplayName,
                PolicyVersion: policyVersion,
                DecisionReason: "lineage_hindsight_recent_reassign",
                DecisionMetadataJson: reassignmentMetadataJson,
                SourceBrainId: splitMembership.BrainId,
                SourceArtifactRef: splitMembership.SourceArtifactRef);
            var outcome = await _store.TryReassignMembershipAsync(
                epoch.EpochId,
                candidate.BrainId,
                expectedSpeciesId: sourceSpeciesId,
                assignment: reassignment,
                decisionTimeMs: reassignmentTimeMs).ConfigureAwait(false);
            if (outcome.Reassigned)
            {
                reassigned++;
            }
        }

        return reassigned;
    }

    private string BuildHindsightReassignDecisionMetadataJson(
        SpeciationMembershipRecord existingMembership,
        SpeciationMembershipRecord splitMembership,
        string sourceSpeciesId,
        string? sourceSpeciesDisplayName,
        AssignmentResolution assignmentResolution,
        string policyVersion,
        double founderSimilarity,
        double candidateSimilarity,
        AssignedSpeciesAdmissionAssessment assignedSpeciesAdmissionAssessment)
    {
        var metadata = ParseMetadataJson(existingMembership.DecisionMetadataJson);
        metadata["assignment_strategy"] = "lineage_hindsight_reassign";
        metadata["assignment_strategy_detail"] =
            "Recent source-species member reassigned to the new derived species within bounded hindsight window.";
        metadata["policy_version"] = policyVersion;

        var sourcePolicyEffectiveSplitThreshold = assignmentResolution.PolicyEffectiveSplitThreshold > 0d
            ? assignmentResolution.PolicyEffectiveSplitThreshold
            : Math.Max(
                0d,
                _assignmentPolicy.LineageSplitThreshold - _assignmentPolicy.LineageSplitGuardMargin);
        var sourceDynamicSplitThreshold = assignmentResolution.EffectiveSplitThreshold > 0d
            ? assignmentResolution.EffectiveSplitThreshold
            : sourcePolicyEffectiveSplitThreshold;
        var sourceSplitThresholdSource = sourceDynamicSplitThreshold > sourcePolicyEffectiveSplitThreshold
            ? "species_floor"
            : "policy";
        var assignedThresholdState = ResolveSplitThresholdState(
            ResolveSpeciesSimilarityFloor(assignmentResolution.SpeciesId));
        var assignedSplitThresholdSource = assignedThresholdState.UsesSpeciesFloor
            ? "species_floor"
            : "policy";
        var actualAssignedSimilarity = ClampScore(
            assignedSpeciesAdmissionAssessment.SimilarityScore ?? 0d);
        var normalizedCandidateSimilarity = ClampScore(candidateSimilarity);

        var assignmentPolicy = metadata["assignment_policy"] as JsonObject ?? new JsonObject();
        assignmentPolicy["lineage_hindsight_reassign_commit_window"] = _assignmentPolicy.HindsightReassignCommitWindow;
        assignmentPolicy["lineage_hindsight_similarity_margin"] = _assignmentPolicy.HindsightReassignSimilarityMargin;
        assignmentPolicy["lineage_policy_effective_split_threshold"] = assignedThresholdState.PolicyEffectiveSplitThreshold;
        assignmentPolicy["lineage_dynamic_split_threshold"] = assignedThresholdState.DynamicSplitThreshold;
        assignmentPolicy["lineage_split_threshold_source"] = assignedSplitThresholdSource;
        assignmentPolicy["lineage_source_policy_effective_split_threshold"] = sourcePolicyEffectiveSplitThreshold;
        assignmentPolicy["lineage_source_dynamic_split_threshold"] = sourceDynamicSplitThreshold;
        assignmentPolicy["lineage_source_split_threshold_source"] = sourceSplitThresholdSource;
        assignmentPolicy["lineage_assigned_policy_effective_split_threshold"] = assignedThresholdState.PolicyEffectiveSplitThreshold;
        assignmentPolicy["lineage_assigned_dynamic_split_threshold"] = assignedThresholdState.DynamicSplitThreshold;
        assignmentPolicy["lineage_assigned_split_threshold_source"] = assignedSplitThresholdSource;
        assignmentPolicy["lineage_source_species_similarity_score"] = normalizedCandidateSimilarity;
        assignmentPolicy["lineage_assignment_similarity_score"] = actualAssignedSimilarity;
        if (assignedThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            assignmentPolicy["lineage_species_floor_similarity_score"] =
                ClampScore(assignedThresholdState.SpeciesFloorSimilarityScore.Value);
            assignmentPolicy["lineage_species_floor_similarity_samples"] =
                assignedThresholdState.SpeciesFloorSampleCount;
            assignmentPolicy["lineage_species_floor_membership_count"] =
                assignedThresholdState.SpeciesFloorMembershipCount;
            assignmentPolicy["lineage_assigned_species_floor_similarity_score"] =
                ClampScore(assignedThresholdState.SpeciesFloorSimilarityScore.Value);
            assignmentPolicy["lineage_assigned_species_floor_similarity_samples"] =
                assignedThresholdState.SpeciesFloorSampleCount;
            assignmentPolicy["lineage_assigned_species_floor_membership_count"] =
                assignedThresholdState.SpeciesFloorMembershipCount;
        }
        metadata["assignment_policy"] = assignmentPolicy;

        var lineage = metadata["lineage"] as JsonObject ?? new JsonObject();
        lineage["source_species_id"] = sourceSpeciesId;
        lineage["source_species_display_name"] = sourceSpeciesDisplayName ?? string.Empty;
        lineage["dominant_species_id"] = sourceSpeciesId;
        lineage["dominant_species_display_name"] = sourceSpeciesDisplayName ?? string.Empty;
        lineage["source_species_similarity_score"] = normalizedCandidateSimilarity;
        lineage["dominant_species_similarity_score"] = normalizedCandidateSimilarity;
        lineage["lineage_assignment_similarity_score"] = actualAssignedSimilarity;
        lineage["intra_species_similarity_sample"] = actualAssignedSimilarity;
        lineage["intra_species_similarity_species_id"] = assignmentResolution.SpeciesId;
        ApplyAssignedSpeciesCompatibilityMetadata(lineage, assignedSpeciesAdmissionAssessment);
        lineage["lineage_policy_effective_split_threshold"] = assignedThresholdState.PolicyEffectiveSplitThreshold;
        lineage["lineage_dynamic_split_threshold"] = assignedThresholdState.DynamicSplitThreshold;
        lineage["lineage_split_threshold_source"] = assignedSplitThresholdSource;
        lineage["source_policy_effective_split_threshold"] = sourcePolicyEffectiveSplitThreshold;
        lineage["source_dynamic_split_threshold"] = sourceDynamicSplitThreshold;
        lineage["source_split_threshold_source"] = sourceSplitThresholdSource;
        lineage["assigned_policy_effective_split_threshold"] = assignedThresholdState.PolicyEffectiveSplitThreshold;
        lineage["assigned_dynamic_split_threshold"] = assignedThresholdState.DynamicSplitThreshold;
        lineage["assigned_split_threshold_source"] = assignedSplitThresholdSource;
        lineage["split_proximity_to_policy_threshold"] =
            actualAssignedSimilarity - assignedThresholdState.PolicyEffectiveSplitThreshold;
        lineage["split_proximity_to_dynamic_threshold"] =
            actualAssignedSimilarity - assignedThresholdState.DynamicSplitThreshold;
        lineage["assigned_split_proximity_to_policy_threshold"] =
            actualAssignedSimilarity - assignedThresholdState.PolicyEffectiveSplitThreshold;
        lineage["assigned_split_proximity_to_dynamic_threshold"] =
            actualAssignedSimilarity - assignedThresholdState.DynamicSplitThreshold;
        lineage["source_split_proximity_to_policy_threshold"] =
            normalizedCandidateSimilarity - sourcePolicyEffectiveSplitThreshold;
        lineage["source_split_proximity_to_dynamic_threshold"] =
            normalizedCandidateSimilarity - sourceDynamicSplitThreshold;
        if (assignedThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            lineage["species_floor_similarity_score"] =
                ClampScore(assignedThresholdState.SpeciesFloorSimilarityScore.Value);
            lineage["species_floor_similarity_samples"] = assignedThresholdState.SpeciesFloorSampleCount;
            lineage["species_floor_membership_count"] = assignedThresholdState.SpeciesFloorMembershipCount;
            lineage["assigned_species_floor_similarity_score"] =
                ClampScore(assignedThresholdState.SpeciesFloorSimilarityScore.Value);
            lineage["assigned_species_floor_similarity_samples"] = assignedThresholdState.SpeciesFloorSampleCount;
            lineage["assigned_species_floor_membership_count"] = assignedThresholdState.SpeciesFloorMembershipCount;
        }
        lineage["hindsight_source_species_id"] = sourceSpeciesId;
        lineage["hindsight_target_species_id"] = assignmentResolution.SpeciesId;
        lineage["hindsight_target_species_display_name"] = assignmentResolution.SpeciesDisplayName;
        lineage["hindsight_founder_brain_id"] = splitMembership.BrainId.ToString("D");
        lineage["hindsight_founder_similarity_score"] = ClampScore(founderSimilarity);
        lineage["hindsight_candidate_source_similarity_score"] = ClampScore(candidateSimilarity);
        lineage["hindsight_similarity_margin"] = _assignmentPolicy.HindsightReassignSimilarityMargin;
        lineage["hindsight_similarity_lower_bound"] = Math.Max(
            0d,
            ClampScore(founderSimilarity) - _assignmentPolicy.HindsightReassignSimilarityMargin);
        lineage["hindsight_similarity_upper_bound"] = Math.Min(
            1d,
            ClampScore(founderSimilarity) + _assignmentPolicy.HindsightReassignSimilarityMargin);
        lineage["hindsight_window_commits"] = _assignmentPolicy.HindsightReassignCommitWindow;
        metadata["lineage"] = lineage;

        return metadata.ToJsonString(MetadataJsonSerializerOptions);
    }

    private static bool TryExtractSourceSpeciesSimilarity(
        string? decisionMetadataJson,
        out string sourceSpeciesId,
        out double similarityScore)
    {
        sourceSpeciesId = string.Empty;
        similarityScore = 0d;
        if (string.IsNullOrWhiteSpace(decisionMetadataJson))
        {
            return false;
        }

        var similarityEvidence = ExtractSimilarityEvidence(decisionMetadataJson);
        var resolvedSimilarity = similarityEvidence.DominantSpeciesSimilarityScore
            ?? similarityEvidence.SimilarityScore;
        if (!resolvedSimilarity.HasValue)
        {
            return false;
        }

        JsonNode? node;
        try
        {
            node = JsonNode.Parse(decisionMetadataJson);
        }
        catch (JsonException)
        {
            return false;
        }

        JsonNode? lineageNode = null;
        if (node is JsonObject root)
        {
            root.TryGetPropertyValue("lineage", out lineageNode);
        }

        sourceSpeciesId = FindStringValue(
            lineageNode,
            "source_species_id",
            "sourceSpeciesId")
            ?? FindStringValue(
            lineageNode,
            "dominant_species_id",
            "dominantSpeciesId")
            ?? FindStringValue(
                node,
                "source_species_id",
                "sourceSpeciesId")
            ?? FindStringValue(
                node,
                "dominant_species_id",
                "dominantSpeciesId")
            ?? string.Empty;
        if (string.IsNullOrWhiteSpace(sourceSpeciesId))
        {
            return false;
        }

        sourceSpeciesId = sourceSpeciesId.Trim();
        var sourceSimilarity = FindNumericValue(
            lineageNode,
            "source_species_similarity_score",
            "sourceSpeciesSimilarityScore")
            ?? FindNumericValue(
                node,
                "source_species_similarity_score",
                "sourceSpeciesSimilarityScore");
        similarityScore = ClampScore((sourceSimilarity ?? resolvedSimilarity).Value);
        return true;
    }

    private static bool TryExtractRecentDerivedSpeciesHint(
        SpeciationMembershipRecord membership,
        out RecentDerivedSpeciesHint hint)
    {
        hint = default;
        if (membership is null
            || membership.AssignedMs <= 0
            || !string.Equals(
                membership.DecisionReason,
                "lineage_diverged_new_species",
                StringComparison.Ordinal)
            || string.IsNullOrWhiteSpace(membership.SpeciesId)
            || !TryExtractSourceSpeciesSimilarity(
                membership.DecisionMetadataJson,
                out var sourceSpeciesId,
                out var founderSimilarity))
        {
            return false;
        }

        var metadata = ParseMetadataJson(membership.DecisionMetadataJson);
        metadata.TryGetPropertyValue("lineage", out var lineageNode);
        var sourceSpeciesDisplayName = FindStringValue(
            lineageNode,
            "source_species_display_name",
            "sourceSpeciesDisplayName")
            ?? FindStringValue(
            lineageNode,
            "dominant_species_display_name",
            "dominantSpeciesDisplayName")
            ?? string.Empty;

        var targetSpeciesId = membership.SpeciesId.Trim();
        if (targetSpeciesId.Length == 0
            || string.Equals(targetSpeciesId, sourceSpeciesId, StringComparison.Ordinal))
        {
            return false;
        }

        hint = new RecentDerivedSpeciesHint(
            SourceSpeciesId: sourceSpeciesId,
            SourceSpeciesDisplayName: sourceSpeciesDisplayName,
            TargetSpeciesId: targetSpeciesId,
            TargetSpeciesDisplayName: membership.SpeciesDisplayName,
            FounderSimilarityScore: ClampScore(founderSimilarity),
            AssignedMs: membership.AssignedMs);
        return true;
    }

    private bool TryResolveRecentDerivedSpeciesReuse(
        string? sourceSpeciesId,
        double similarity,
        out RecentDerivedSpeciesHint hint)
    {
        hint = default;
        if (_assignmentPolicy.HindsightReassignCommitWindow <= 0
            || string.IsNullOrWhiteSpace(sourceSpeciesId)
            || !_recentDerivedSpeciesHintsBySourceSpecies.TryGetValue(
                sourceSpeciesId.Trim(),
                out var hints)
            || hints.Count == 0)
        {
            return false;
        }

        var margin = Math.Max(0d, _assignmentPolicy.HindsightReassignSimilarityMargin);
        var candidate = hints
            .Where(existing => IsWithinSimilarityBand(existing.FounderSimilarityScore, similarity, margin))
            .OrderBy(existing => Math.Abs(existing.FounderSimilarityScore - similarity))
            .ThenByDescending(existing => existing.AssignedMs)
            .ThenBy(existing => existing.TargetSpeciesId, StringComparer.Ordinal)
            .FirstOrDefault();
        if (string.IsNullOrWhiteSpace(candidate.TargetSpeciesId))
        {
            return false;
        }

        hint = candidate;
        return true;
    }

    private bool TryGetRecentDerivedSpeciesHintByTargetSpecies(
        string? speciesId,
        out RecentDerivedSpeciesHint hint)
    {
        hint = default;
        if (string.IsNullOrWhiteSpace(speciesId))
        {
            return false;
        }

        return _recentDerivedSpeciesHintsByTargetSpecies.TryGetValue(speciesId.Trim(), out hint);
    }

    private static bool IsWithinSimilarityBand(
        double centerSimilarity,
        double candidateSimilarity,
        double margin)
    {
        var normalizedMargin = Math.Max(0d, margin);
        var lowerBound = Math.Max(0d, ClampScore(centerSimilarity) - normalizedMargin);
        var upperBound = Math.Min(1d, ClampScore(centerSimilarity) + normalizedMargin);
        var normalizedCandidate = ClampScore(candidateSimilarity);
        return normalizedCandidate >= lowerBound && normalizedCandidate <= upperBound;
    }

    private AssignmentResolution ResolveAssignment(
        SpeciationEpochInfo epoch,
        string? requestedSpeciesId,
        string? requestedSpeciesDisplayName,
        LineageEvidence lineageEvidence,
        SpeciesSimilarityFloorState? sourceSpeciesFloor,
        double? sourceSpeciesSimilarityScore,
        ParentSpeciesPairwiseFit? bestParentSpeciesFit,
        bool isSeedFounderCandidate,
        bool allowRecentSplitRealign = true,
        bool allowRecentDerivedSpeciesReuse = true)
    {
        if (!string.IsNullOrWhiteSpace(requestedSpeciesId))
        {
            var speciesId = requestedSpeciesId.Trim();
            var speciesDisplayName = ResolveTrackedSpeciesDisplayName(speciesId, requestedSpeciesDisplayName);
            return new AssignmentResolution(
                speciesId,
                speciesDisplayName,
                DecisionReason: "explicit_species",
                Strategy: "explicit_species",
                StrategyDetail: "Request provided explicit species_id.",
                ForceDecisionReason: false);
        }

        if (lineageEvidence.ParentMembershipCount == 0)
        {
            return new AssignmentResolution(
                _runtimeConfig.DefaultSpeciesId,
                ResolveTrackedSpeciesDisplayName(
                    _runtimeConfig.DefaultSpeciesId,
                    requestedSpeciesDisplayName,
                    _runtimeConfig.DefaultSpeciesDisplayName),
                DecisionReason: "lineage_unavailable_default",
                Strategy: "default",
                StrategyDetail: "No parent membership evidence available in current epoch.",
                ForceDecisionReason: true);
        }

        if (string.IsNullOrWhiteSpace(lineageEvidence.DominantSpeciesId) && !bestParentSpeciesFit.HasValue)
        {
            return new AssignmentResolution(
                _runtimeConfig.DefaultSpeciesId,
                ResolveTrackedSpeciesDisplayName(
                    _runtimeConfig.DefaultSpeciesId,
                    fallbackDisplayName: _runtimeConfig.DefaultSpeciesDisplayName),
                DecisionReason: "lineage_source_unavailable_default",
                Strategy: "default",
                StrategyDetail: "Parent memberships did not resolve to a reusable source species.",
                ForceDecisionReason: true);
        }

        var sourceSpeciesId = bestParentSpeciesFit.HasValue
            ? bestParentSpeciesFit.Value.SpeciesId
            : (lineageEvidence.DominantShare >= 0.999999d
                ? lineageEvidence.DominantSpeciesId
                : null);
        if (string.IsNullOrWhiteSpace(sourceSpeciesId))
        {
            if (!string.IsNullOrWhiteSpace(lineageEvidence.HysteresisSpeciesId))
            {
                return new AssignmentResolution(
                    lineageEvidence.HysteresisSpeciesId!,
                    ResolveTrackedSpeciesDisplayName(
                        lineageEvidence.HysteresisSpeciesId!,
                        lineageEvidence.HysteresisSpeciesDisplayName),
                    DecisionReason: "lineage_hysteresis_low_consensus",
                    Strategy: "hysteresis_hold",
                    StrategyDetail: "Parent memberships did not resolve to a reusable source species; reused prior lineage species.",
                    ForceDecisionReason: true);
            }

            return new AssignmentResolution(
                _runtimeConfig.DefaultSpeciesId,
                ResolveTrackedSpeciesDisplayName(
                    _runtimeConfig.DefaultSpeciesId,
                    fallbackDisplayName: _runtimeConfig.DefaultSpeciesDisplayName),
                DecisionReason: "lineage_source_unavailable_default",
                Strategy: "default",
                StrategyDetail: "Parent memberships did not resolve to a reusable source species.",
                ForceDecisionReason: true);
        }

        sourceSpeciesId = sourceSpeciesId.Trim();
        var sourceSpeciesDisplayName = ResolveTrackedSpeciesDisplayName(
            sourceSpeciesId,
            bestParentSpeciesFit.HasValue
                ? bestParentSpeciesFit.Value.SpeciesDisplayName
                : lineageEvidence.DominantSpeciesDisplayName);
        var sourceConsensusShare = bestParentSpeciesFit.HasValue && lineageEvidence.ParentMembershipCount > 0
            ? (double)bestParentSpeciesFit.Value.SupportingParentCount / lineageEvidence.ParentMembershipCount
            : 1d;

        if (!sourceSpeciesSimilarityScore.HasValue)
        {
            return new AssignmentResolution(
                sourceSpeciesId,
                sourceSpeciesDisplayName,
                DecisionReason: "lineage_inherit_no_similarity",
                Strategy: "lineage_inherit",
                StrategyDetail: "No source-species similarity score supplied; inherited the resolved source species.",
                ForceDecisionReason: true,
                SourceSpeciesId: sourceSpeciesId,
                SourceSpeciesDisplayName: sourceSpeciesDisplayName,
                SourceConsensusShare: sourceConsensusShare);
        }

        var similarity = ClampScore(sourceSpeciesSimilarityScore.Value);
        var sourceThresholdState = ResolveSplitThresholdState(sourceSpeciesFloor);
        var policyEffectiveSplitThreshold = sourceThresholdState.PolicyEffectiveSplitThreshold;
        var effectiveSplitThreshold = sourceThresholdState.DynamicSplitThreshold;
        var speciesFloorSimilarityScore = sourceThresholdState.SpeciesFloorSimilarityScore;
        var speciesFloorSampleCount = sourceThresholdState.SpeciesFloorSampleCount;
        var speciesFloorMembershipCount = sourceThresholdState.SpeciesFloorMembershipCount;

        var splitTriggeredBySpeciesFloor =
            sourceThresholdState.UsesSpeciesFloor
            && similarity <= effectiveSplitThreshold
            && similarity > policyEffectiveSplitThreshold;

        AssignmentResolution BuildSimilarityResolution(
            string SpeciesId,
            string SpeciesDisplayName,
            string DecisionReason,
            string Strategy,
            string StrategyDetail,
            bool ForceDecisionReason,
            RecentDerivedSpeciesHint? recentDerivedHint = null)
        {
            return new AssignmentResolution(
                SpeciesId,
                SpeciesDisplayName,
                DecisionReason,
                Strategy,
                StrategyDetail,
                ForceDecisionReason,
                PolicyEffectiveSplitThreshold: policyEffectiveSplitThreshold,
                EffectiveSplitThreshold: effectiveSplitThreshold,
                SplitTriggeredBySpeciesFloor: splitTriggeredBySpeciesFloor,
                SourceSpeciesId: sourceSpeciesId,
                SourceSpeciesDisplayName: sourceSpeciesDisplayName,
                SourceSpeciesSimilarityScore: similarity,
                SourceConsensusShare: sourceConsensusShare,
                SpeciesFloorSimilarityScore: speciesFloorSimilarityScore,
                SpeciesFloorSampleCount: speciesFloorSampleCount,
                SpeciesFloorMembershipCount: speciesFloorMembershipCount,
                RecentDerivedSourceSpeciesId: recentDerivedHint?.SourceSpeciesId,
                RecentDerivedSourceSpeciesDisplayName: recentDerivedHint?.SourceSpeciesDisplayName,
                RecentDerivedFounderSimilarityScore: recentDerivedHint?.FounderSimilarityScore);
        }

        var hasSplitParentEvidence = lineageEvidence.ParentMembershipCount >= _assignmentPolicy.MinParentMembershipsBeforeSplit;
        var hasInSpeciesEvidence = sourceSpeciesFloor.HasValue
            && sourceSpeciesFloor.Value.SimilaritySampleCount > 0
            && sourceSpeciesFloor.Value.MinSimilarityScore.HasValue;
        var isRecentDerivedSourceSpecies = TryGetRecentDerivedSpeciesHintByTargetSpecies(
            sourceSpeciesId,
            out var recentDerivedSourceHint);
        var withinRecentSplitRealignWindow =
            _assignmentPolicy.RecentSplitRealignParentMembershipWindow > 0
            && lineageEvidence.ParentMembershipCount > 0
            && lineageEvidence.ParentMembershipCount <= _assignmentPolicy.RecentSplitRealignParentMembershipWindow
            && !string.IsNullOrWhiteSpace(lineageEvidence.HysteresisSpeciesId)
            && string.Equals(
                lineageEvidence.HysteresisDecisionReason,
                "lineage_diverged_new_species",
                StringComparison.Ordinal);

        if (allowRecentSplitRealign
            && withinRecentSplitRealignWindow
            && similarity <= Math.Min(
                1d,
                _assignmentPolicy.LineageMatchThreshold + _assignmentPolicy.RecentSplitRealignMatchMargin))
        {
            return BuildSimilarityResolution(
                lineageEvidence.HysteresisSpeciesId!,
                ResolveTrackedSpeciesDisplayName(
                    lineageEvidence.HysteresisSpeciesId!,
                    lineageEvidence.HysteresisSpeciesDisplayName),
                DecisionReason: "lineage_realign_recent_split",
                Strategy: "lineage_realign",
                StrategyDetail: "Recent derived split hint reused within bounded parent-membership realignment window.",
                ForceDecisionReason: true);
        }

        if (similarity >= _assignmentPolicy.LineageMatchThreshold)
        {
            return BuildSimilarityResolution(
                sourceSpeciesId,
                sourceSpeciesDisplayName,
                DecisionReason: "lineage_inherit_similarity_match",
                Strategy: "lineage_inherit",
                StrategyDetail: "Similarity met lineage match threshold.",
                ForceDecisionReason: true);
        }

        if (allowRecentDerivedSpeciesReuse
            && TryResolveRecentDerivedSpeciesReuse(sourceSpeciesId, similarity, out var recentDerivedHint))
        {
            return BuildSimilarityResolution(
                recentDerivedHint.TargetSpeciesId,
                ResolveTrackedSpeciesDisplayName(
                    recentDerivedHint.TargetSpeciesId,
                    recentDerivedHint.TargetSpeciesDisplayName),
                DecisionReason: "lineage_reuse_recent_derived_species",
                Strategy: "lineage_recent_derived_reuse",
                StrategyDetail: "Recent derived species from the same source lineage reused within bounded founder-similarity band.",
                ForceDecisionReason: true,
                recentDerivedHint: recentDerivedHint);
        }

        if (similarity <= effectiveSplitThreshold)
        {
            if (isSeedFounderCandidate
                && lineageEvidence.ParentMembershipCount == 1)
            {
                var founderRootNamingPlan = BuildFounderRootSpeciesNamingPlan(
                    sourceSpeciesId,
                    sourceSpeciesDisplayName);
                var founderRootSpeciesId = BuildFounderRootSpeciesId(
                    sourceSpeciesId,
                    lineageEvidence.LineageKey,
                    epoch.EpochId);
                return new AssignmentResolution(
                    founderRootSpeciesId,
                    founderRootNamingPlan.FounderSpeciesDisplayName,
                    DecisionReason: "lineage_diverged_founder_root_species",
                    Strategy: "lineage_founder_root",
                    StrategyDetail:
                        "Seed founder diverged before any reusable parent lineage existed; created an independent root species.",
                    ForceDecisionReason: true,
                    PolicyEffectiveSplitThreshold: policyEffectiveSplitThreshold,
                    EffectiveSplitThreshold: effectiveSplitThreshold,
                    SplitTriggeredBySpeciesFloor: splitTriggeredBySpeciesFloor,
                    SourceSpeciesSimilarityScore: similarity,
                    SourceConsensusShare: sourceConsensusShare,
                    SpeciesFloorSimilarityScore: speciesFloorSimilarityScore,
                    SpeciesFloorSampleCount: speciesFloorSampleCount,
                    SpeciesFloorMembershipCount: speciesFloorMembershipCount,
                    DisplayNameRewriteSpeciesId: founderRootNamingPlan.SourceSpeciesIdToRewrite,
                    DisplayNameRewriteSpeciesDisplayName: founderRootNamingPlan.SourceSpeciesDisplayNameRewrite);
            }

            if (isRecentDerivedSourceSpecies && !hasInSpeciesEvidence)
            {
                return BuildSimilarityResolution(
                    sourceSpeciesId,
                    sourceSpeciesDisplayName,
                    DecisionReason: "lineage_split_guarded_newborn_intraspecies_evidence",
                    Strategy: "lineage_inherit",
                    StrategyDetail: "Split threshold crossed but the newborn derived species has not yet recorded in-species similarity evidence.",
                    ForceDecisionReason: true,
                    recentDerivedHint: recentDerivedSourceHint);
            }

            if (!hasSplitParentEvidence)
            {
                if (!string.IsNullOrWhiteSpace(lineageEvidence.HysteresisSpeciesId))
                {
                    return BuildSimilarityResolution(
                        lineageEvidence.HysteresisSpeciesId!,
                        ResolveTrackedSpeciesDisplayName(
                            lineageEvidence.HysteresisSpeciesId!,
                            lineageEvidence.HysteresisSpeciesDisplayName),
                        DecisionReason: "lineage_split_guarded_hysteresis_hold",
                        Strategy: "hysteresis_hold",
                        StrategyDetail: "Split threshold crossed but minimum parent-membership evidence not met; reused prior lineage species.",
                        ForceDecisionReason: true);
                }

                return BuildSimilarityResolution(
                    sourceSpeciesId,
                    sourceSpeciesDisplayName,
                    DecisionReason: "lineage_split_guarded_parent_evidence",
                    Strategy: "lineage_inherit",
                    StrategyDetail: "Split threshold crossed but minimum parent-membership evidence not met.",
                    ForceDecisionReason: true);
            }

            if (_assignmentPolicy.CreateDerivedSpeciesOnDivergence)
            {
                var derivedSpeciesId = BuildDerivedSpeciesId(
                    sourceSpeciesId,
                    _assignmentPolicy.DerivedSpeciesPrefix,
                    lineageEvidence.LineageKey,
                    epoch.EpochId);
                return BuildSimilarityResolution(
                    derivedSpeciesId,
                    BuildDerivedSpeciesDisplayName(
                        sourceSpeciesDisplayName,
                        sourceSpeciesId,
                        derivedSpeciesId),
                    DecisionReason: "lineage_diverged_new_species",
                    Strategy: "lineage_diverged",
                    StrategyDetail: splitTriggeredBySpeciesFloor
                        ? "Similarity below dynamic species floor threshold; created deterministic derived species with lineage suffix."
                        : "Similarity below split threshold; created deterministic derived species with lineage suffix.",
                    ForceDecisionReason: true);
            }

            return BuildSimilarityResolution(
                _runtimeConfig.DefaultSpeciesId,
                ResolveTrackedSpeciesDisplayName(
                    _runtimeConfig.DefaultSpeciesId,
                    fallbackDisplayName: _runtimeConfig.DefaultSpeciesDisplayName),
                DecisionReason: "lineage_diverged_default",
                Strategy: "default",
                StrategyDetail: splitTriggeredBySpeciesFloor
                    ? "Similarity below dynamic species floor threshold with derived species disabled."
                    : "Similarity below split threshold with derived species disabled.",
                ForceDecisionReason: true);
        }

        if (_assignmentPolicy.LineageSplitGuardMargin > 0d
            && similarity <= _assignmentPolicy.LineageSplitThreshold)
        {
            if (!string.IsNullOrWhiteSpace(lineageEvidence.HysteresisSpeciesId))
            {
                return BuildSimilarityResolution(
                    lineageEvidence.HysteresisSpeciesId!,
                    ResolveSpeciesDisplayName(lineageEvidence.HysteresisSpeciesDisplayName, lineageEvidence.HysteresisSpeciesId!),
                    DecisionReason: "lineage_split_guard_band_hysteresis_hold",
                    Strategy: "hysteresis_hold",
                    StrategyDetail: "Similarity in split-guard band; reused prior lineage species to reduce fragmentation.",
                    ForceDecisionReason: true);
            }

            return BuildSimilarityResolution(
                sourceSpeciesId,
                sourceSpeciesDisplayName,
                DecisionReason: "lineage_split_guard_band_inherit",
                Strategy: "lineage_inherit",
                StrategyDetail: "Similarity in split-guard band; inherited the resolved source lineage species.",
                ForceDecisionReason: true);
        }

        if (!string.IsNullOrWhiteSpace(lineageEvidence.HysteresisSpeciesId))
        {
            return BuildSimilarityResolution(
                lineageEvidence.HysteresisSpeciesId!,
                ResolveSpeciesDisplayName(lineageEvidence.HysteresisSpeciesDisplayName, lineageEvidence.HysteresisSpeciesId!),
                DecisionReason: "lineage_hysteresis_hold",
                Strategy: "hysteresis_hold",
                StrategyDetail: "Similarity in hysteresis band; reused prior lineage species.",
                ForceDecisionReason: true);
        }

        return BuildSimilarityResolution(
            sourceSpeciesId,
            sourceSpeciesDisplayName,
            DecisionReason: "lineage_hysteresis_seed",
            Strategy: "lineage_inherit",
            StrategyDetail: "Similarity in hysteresis band without prior lineage hint; seeded with the resolved source species.",
            ForceDecisionReason: true);
    }
    private static void ApplyAssignedSpeciesCompatibilityMetadata(
        JsonObject lineage,
        AssignedSpeciesAdmissionAssessment assignedSpeciesAdmissionAssessment)
    {
        if (assignedSpeciesAdmissionAssessment.Admitted)
        {
            lineage["assigned_species_similarity_source"] = "compatibility_assessment";
        }

        lineage["assigned_species_compatibility_attempted"] =
            assignedSpeciesAdmissionAssessment.AssessmentAttempted;
        lineage["assigned_species_compatibility_admitted"] =
            assignedSpeciesAdmissionAssessment.Admitted;
        if (!string.IsNullOrWhiteSpace(assignedSpeciesAdmissionAssessment.AssessmentMode))
        {
            lineage["assigned_species_compatibility_mode"] =
                assignedSpeciesAdmissionAssessment.AssessmentMode;
        }

        if (assignedSpeciesAdmissionAssessment.SimilarityScore.HasValue)
        {
            lineage["assigned_species_compatibility_similarity_score"] = ClampScore(
                assignedSpeciesAdmissionAssessment.SimilarityScore.Value);
        }

        lineage["assigned_species_compatibility_report_compatible"] =
            assignedSpeciesAdmissionAssessment.Compatible;
        lineage["assigned_species_compatibility_abort_reason"] =
            assignedSpeciesAdmissionAssessment.AbortReason;
        lineage["assigned_species_compatibility_failure_reason"] =
            assignedSpeciesAdmissionAssessment.FailureReason;
        lineage["assigned_species_compatibility_elapsed_ms"] =
            assignedSpeciesAdmissionAssessment.ElapsedMs;
        lineage["assigned_species_compatibility_exemplar_count"] =
            assignedSpeciesAdmissionAssessment.ExemplarBrainIds.Length;
        var exemplarBrainIds = new JsonArray();
        foreach (var exemplarBrainId in assignedSpeciesAdmissionAssessment.ExemplarBrainIds)
        {
            exemplarBrainIds.Add(exemplarBrainId);
        }

        lineage["assigned_species_compatibility_exemplar_brain_ids"] = exemplarBrainIds;
    }

    private string BuildDecisionMetadataJson(
        string? sourceMetadataJson,
        string policyVersion,
        ResolvedCandidate resolvedCandidate,
        AssignmentResolution assignmentResolution,
        LineageEvidence lineageEvidence,
        SimilarityEvidence similarityEvidence,
        SpeciesSimilarityFloorState? sourceSpeciesFloor,
        double? sourceSpeciesSimilarityScore,
        double? assignedSpeciesSimilarityScore,
        double? intraSpeciesSimilaritySample,
        AssignedSpeciesAdmissionAssessment? assignedSpeciesAdmissionAssessment)
    {
        var metadata = ParseMetadataJson(sourceMetadataJson);
        metadata.TryGetPropertyValue("lineage", out var existingLineageNode);
        metadata["assignment_strategy"] = assignmentResolution.Strategy;
        metadata["assignment_strategy_detail"] = assignmentResolution.StrategyDetail;
        metadata["policy_version"] = policyVersion;

        var sourcePolicyEffectiveSplitThreshold = assignmentResolution.PolicyEffectiveSplitThreshold > 0d
            ? assignmentResolution.PolicyEffectiveSplitThreshold
            : Math.Max(
                0d,
                _assignmentPolicy.LineageSplitThreshold - _assignmentPolicy.LineageSplitGuardMargin);
        var sourceDynamicSplitThreshold = assignmentResolution.EffectiveSplitThreshold > 0d
            ? assignmentResolution.EffectiveSplitThreshold
            : sourcePolicyEffectiveSplitThreshold;
        var sourceSpeciesFloorSimilarityScore = assignmentResolution.SpeciesFloorSimilarityScore
            ?? sourceSpeciesFloor?.MinSimilarityScore;
        var sourceSpeciesFloorSampleCount = assignmentResolution.SpeciesFloorSampleCount > 0
            ? assignmentResolution.SpeciesFloorSampleCount
            : Math.Max(0, sourceSpeciesFloor?.SimilaritySampleCount ?? 0);
        var sourceSpeciesFloorMembershipCount = assignmentResolution.SpeciesFloorMembershipCount > 0
            ? assignmentResolution.SpeciesFloorMembershipCount
            : Math.Max(0, sourceSpeciesFloor?.MembershipCount ?? 0);
        var sourceThresholdState = new SplitThresholdState(
            PolicyEffectiveSplitThreshold: sourcePolicyEffectiveSplitThreshold,
            DynamicSplitThreshold: sourceDynamicSplitThreshold,
            UsesSpeciesFloor: sourceDynamicSplitThreshold > sourcePolicyEffectiveSplitThreshold,
            SpeciesFloorSimilarityScore: sourceSpeciesFloorSimilarityScore,
            SpeciesFloorSampleCount: sourceSpeciesFloorSampleCount,
            SpeciesFloorMembershipCount: sourceSpeciesFloorMembershipCount);
        var resolvedSourceSpeciesId = string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
            ? string.Empty
            : assignmentResolution.SourceSpeciesId;
        var resolvedSourceSpeciesDisplayName = string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesDisplayName)
            ? (string.IsNullOrWhiteSpace(resolvedSourceSpeciesId)
                ? string.Empty
                : ResolveSpeciesDisplayName(null, resolvedSourceSpeciesId))
            : assignmentResolution.SourceSpeciesDisplayName;
        var resolvedSourceConsensusShare = string.IsNullOrWhiteSpace(resolvedSourceSpeciesId)
            ? 0d
            : assignmentResolution.SourceConsensusShare;
        var assignedThresholdState = ResolveSplitThresholdState(
            ResolveSpeciesSimilarityFloor(assignmentResolution.SpeciesId));
        var sourceThresholdSource = sourceThresholdState.UsesSpeciesFloor
            ? "species_floor"
            : "policy";
        var assignedThresholdSource = assignedThresholdState.UsesSpeciesFloor
            ? "species_floor"
            : "policy";
        var useAssignedSplitPerspective = assignedSpeciesSimilarityScore.HasValue
            || string.IsNullOrWhiteSpace(resolvedSourceSpeciesId)
            || string.Equals(
                assignmentResolution.SpeciesId,
                resolvedSourceSpeciesId,
                StringComparison.Ordinal);
        var genericThresholdState = useAssignedSplitPerspective
            ? assignedThresholdState
            : sourceThresholdState;
        var genericThresholdSource = useAssignedSplitPerspective
            ? assignedThresholdSource
            : sourceThresholdSource;

        var assignmentPolicy = new JsonObject
        {
            ["lineage_match_threshold"] = _assignmentPolicy.LineageMatchThreshold,
            ["lineage_split_threshold"] = _assignmentPolicy.LineageSplitThreshold,
            ["lineage_effective_split_threshold"] = Math.Max(
                0d,
                _assignmentPolicy.LineageSplitThreshold - _assignmentPolicy.LineageSplitGuardMargin),
            ["lineage_split_guard_margin"] = _assignmentPolicy.LineageSplitGuardMargin,
            ["lineage_min_parent_memberships_before_split"] = _assignmentPolicy.MinParentMembershipsBeforeSplit,
            ["parent_consensus_threshold"] = _assignmentPolicy.ParentConsensusThreshold,
            ["hysteresis_margin"] = _assignmentPolicy.HysteresisMargin,
            ["lineage_realign_parent_membership_window"] = _assignmentPolicy.RecentSplitRealignParentMembershipWindow,
            ["lineage_realign_match_margin"] = _assignmentPolicy.RecentSplitRealignMatchMargin,
            ["lineage_hindsight_reassign_commit_window"] = _assignmentPolicy.HindsightReassignCommitWindow,
            ["lineage_hindsight_similarity_margin"] = _assignmentPolicy.HindsightReassignSimilarityMargin,
            ["create_derived_species_on_divergence"] = _assignmentPolicy.CreateDerivedSpeciesOnDivergence,
            ["derived_species_prefix"] = _assignmentPolicy.DerivedSpeciesPrefix,
            ["lineage_policy_effective_split_threshold"] = genericThresholdState.PolicyEffectiveSplitThreshold,
            ["lineage_dynamic_split_threshold"] = genericThresholdState.DynamicSplitThreshold,
            ["lineage_split_threshold_source"] = genericThresholdSource,
            ["lineage_source_policy_effective_split_threshold"] = sourceThresholdState.PolicyEffectiveSplitThreshold,
            ["lineage_source_dynamic_split_threshold"] = sourceThresholdState.DynamicSplitThreshold,
            ["lineage_source_split_threshold_source"] = sourceThresholdSource,
            ["lineage_assigned_policy_effective_split_threshold"] = assignedThresholdState.PolicyEffectiveSplitThreshold,
            ["lineage_assigned_dynamic_split_threshold"] = assignedThresholdState.DynamicSplitThreshold,
            ["lineage_assigned_split_threshold_source"] = assignedThresholdSource,
            ["lineage_species_floor_relaxation_margin"] = _assignmentPolicy.HysteresisMargin
        };
        if (sourceSpeciesSimilarityScore.HasValue)
        {
            assignmentPolicy["lineage_source_species_similarity_score"] =
                ClampScore(sourceSpeciesSimilarityScore.Value);
        }
        if (assignedSpeciesSimilarityScore.HasValue)
        {
            assignmentPolicy["lineage_assignment_similarity_score"] =
                ClampScore(assignedSpeciesSimilarityScore.Value);
        }
        if (genericThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            assignmentPolicy["lineage_species_floor_similarity_score"] =
                ClampScore(genericThresholdState.SpeciesFloorSimilarityScore.Value);
            assignmentPolicy["lineage_species_floor_similarity_samples"] =
                genericThresholdState.SpeciesFloorSampleCount;
            assignmentPolicy["lineage_species_floor_membership_count"] =
                genericThresholdState.SpeciesFloorMembershipCount;
        }
        if (sourceThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            assignmentPolicy["lineage_source_species_floor_similarity_score"] =
                ClampScore(sourceThresholdState.SpeciesFloorSimilarityScore.Value);
            assignmentPolicy["lineage_source_species_floor_similarity_samples"] =
                sourceThresholdState.SpeciesFloorSampleCount;
            assignmentPolicy["lineage_source_species_floor_membership_count"] =
                sourceThresholdState.SpeciesFloorMembershipCount;
        }
        if (assignedThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            assignmentPolicy["lineage_assigned_species_floor_similarity_score"] =
                ClampScore(assignedThresholdState.SpeciesFloorSimilarityScore.Value);
            assignmentPolicy["lineage_assigned_species_floor_similarity_samples"] =
                assignedThresholdState.SpeciesFloorSampleCount;
            assignmentPolicy["lineage_assigned_species_floor_membership_count"] =
                assignedThresholdState.SpeciesFloorMembershipCount;
        }
        metadata["assignment_policy"] = assignmentPolicy;

        var parentBrainIds = new JsonArray();
        foreach (var parentBrainId in lineageEvidence.ParentBrainIds)
        {
            parentBrainIds.Add(parentBrainId.ToString("D"));
        }

        var parentArtifactRefs = new JsonArray();
        foreach (var parentArtifactRef in lineageEvidence.ParentArtifactRefs)
        {
            parentArtifactRefs.Add(parentArtifactRef);
        }

        var lineage = new JsonObject
        {
            ["candidate_mode"] = resolvedCandidate.CandidateMode.ToString(),
            ["lineage_key"] = lineageEvidence.LineageKey,
            ["parent_membership_count"] = lineageEvidence.ParentMembershipCount,
            ["parent_brain_ids"] = parentBrainIds,
            ["parent_artifact_refs"] = parentArtifactRefs,
            ["source_species_id"] = resolvedSourceSpeciesId,
            ["source_species_display_name"] = resolvedSourceSpeciesDisplayName,
            ["source_species_share"] = resolvedSourceConsensusShare,
            ["dominant_species_id"] = resolvedSourceSpeciesId,
            ["dominant_species_display_name"] = resolvedSourceSpeciesDisplayName,
            ["dominant_species_share"] = resolvedSourceConsensusShare,
            ["hysteresis_species_id"] = lineageEvidence.HysteresisSpeciesId ?? string.Empty,
            ["hysteresis_species_display_name"] = lineageEvidence.HysteresisSpeciesDisplayName ?? string.Empty,
            ["lineage_policy_effective_split_threshold"] = genericThresholdState.PolicyEffectiveSplitThreshold,
            ["lineage_dynamic_split_threshold"] = genericThresholdState.DynamicSplitThreshold,
            ["lineage_split_threshold_source"] = genericThresholdSource,
            ["source_policy_effective_split_threshold"] = sourceThresholdState.PolicyEffectiveSplitThreshold,
            ["source_dynamic_split_threshold"] = sourceThresholdState.DynamicSplitThreshold,
            ["source_split_threshold_source"] = sourceThresholdSource,
            ["assigned_policy_effective_split_threshold"] = assignedThresholdState.PolicyEffectiveSplitThreshold,
            ["assigned_dynamic_split_threshold"] = assignedThresholdState.DynamicSplitThreshold,
            ["assigned_split_threshold_source"] = assignedThresholdSource
        };
        if (resolvedCandidate.CandidateArtifactRef is not null
            && HasUsableArtifactReference(resolvedCandidate.CandidateArtifactRef))
        {
            lineage["candidate_artifact_ref"] = BuildStoredArtifactRefNode(resolvedCandidate.CandidateArtifactRef);
        }
        if (resolvedCandidate.CandidateBrainBaseArtifactRef is not null
            && HasUsableArtifactReference(resolvedCandidate.CandidateBrainBaseArtifactRef))
        {
            lineage["candidate_brain_base_artifact_ref"] = BuildStoredArtifactRefNode(
                resolvedCandidate.CandidateBrainBaseArtifactRef);
        }
        if (resolvedCandidate.CandidateBrainSnapshotArtifactRef is not null
            && HasUsableArtifactReference(resolvedCandidate.CandidateBrainSnapshotArtifactRef))
        {
            lineage["candidate_brain_snapshot_artifact_ref"] = BuildStoredArtifactRefNode(
                resolvedCandidate.CandidateBrainSnapshotArtifactRef);
        }
        if (!string.IsNullOrWhiteSpace(resolvedCandidate.CandidateArtifactUri))
        {
            lineage["candidate_artifact_uri"] = resolvedCandidate.CandidateArtifactUri;
        }
        if (genericThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            lineage["species_floor_similarity_score"] =
                ClampScore(genericThresholdState.SpeciesFloorSimilarityScore.Value);
            lineage["species_floor_similarity_samples"] =
                genericThresholdState.SpeciesFloorSampleCount;
            lineage["species_floor_membership_count"] =
                genericThresholdState.SpeciesFloorMembershipCount;
        }
        if (sourceThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            lineage["source_species_floor_similarity_score"] =
                ClampScore(sourceThresholdState.SpeciesFloorSimilarityScore.Value);
            lineage["source_species_floor_similarity_samples"] =
                sourceThresholdState.SpeciesFloorSampleCount;
            lineage["source_species_floor_membership_count"] =
                sourceThresholdState.SpeciesFloorMembershipCount;
        }
        if (assignedThresholdState.SpeciesFloorSimilarityScore.HasValue)
        {
            lineage["assigned_species_floor_similarity_score"] =
                ClampScore(assignedThresholdState.SpeciesFloorSimilarityScore.Value);
            lineage["assigned_species_floor_similarity_samples"] =
                assignedThresholdState.SpeciesFloorSampleCount;
            lineage["assigned_species_floor_membership_count"] =
                assignedThresholdState.SpeciesFloorMembershipCount;
        }
        if (sourceSpeciesSimilarityScore.HasValue)
        {
            lineage["source_species_similarity_score"] = ClampScore(sourceSpeciesSimilarityScore.Value);
        }
        if (assignedSpeciesSimilarityScore.HasValue)
        {
            lineage["lineage_assignment_similarity_score"] = ClampScore(assignedSpeciesSimilarityScore.Value);
        }
        if (!string.IsNullOrWhiteSpace(assignmentResolution.RecentDerivedSourceSpeciesId))
        {
            lineage["recent_derived_source_species_id"] = assignmentResolution.RecentDerivedSourceSpeciesId;
            lineage["recent_derived_source_species_display_name"] =
                assignmentResolution.RecentDerivedSourceSpeciesDisplayName ?? string.Empty;
        }
        if (assignmentResolution.RecentDerivedFounderSimilarityScore.HasValue)
        {
            lineage["recent_derived_founder_similarity_score"] = ClampScore(
                assignmentResolution.RecentDerivedFounderSimilarityScore.Value);
            lineage["recent_derived_similarity_margin"] = _assignmentPolicy.HindsightReassignSimilarityMargin;
        }
        if (intraSpeciesSimilaritySample.HasValue)
        {
            lineage["intra_species_similarity_sample"] = ClampScore(intraSpeciesSimilaritySample.Value);
            lineage["intra_species_similarity_species_id"] = assignmentResolution.SpeciesId;
        }
        if (assignedSpeciesAdmissionAssessment.HasValue)
        {
            ApplyAssignedSpeciesCompatibilityMetadata(
                lineage,
                assignedSpeciesAdmissionAssessment.Value);
        }
        AddLineageSimilarityScore(
            lineage,
            "lineage_similarity_score",
            similarityEvidence.SimilarityScore,
            existingLineageNode,
            "lineage_similarity_score",
            "lineageSimilarityScore",
            "similarity_score",
            "similarityScore");
        AddLineageSimilarityScore(
            lineage,
            "parent_a_similarity_score",
            preferredValue: similarityEvidence.ParentASimilarityScore,
            existingLineageNode,
            "parent_a_similarity_score",
            "parentASimilarityScore",
            "lineage_parent_a_similarity_score",
            "lineageParentASimilarityScore");
        AddLineageSimilarityScore(
            lineage,
            "parent_b_similarity_score",
            preferredValue: similarityEvidence.ParentBSimilarityScore,
            existingLineageNode,
            "parent_b_similarity_score",
            "parentBSimilarityScore",
            "lineage_parent_b_similarity_score",
            "lineageParentBSimilarityScore");
        AddLineageSimilarityScore(
            lineage,
            "dominant_species_similarity_score",
            sourceSpeciesSimilarityScore ?? similarityEvidence.DominantSpeciesSimilarityScore,
            existingLineageNode,
            "dominant_species_similarity_score",
            "dominantSpeciesSimilarityScore");
        AddLineageSimilarityScore(
            lineage,
            "source_species_similarity_score",
            sourceSpeciesSimilarityScore,
            existingLineageNode,
            "source_species_similarity_score",
            "sourceSpeciesSimilarityScore");
        var sourceSplitSimilarity = sourceSpeciesSimilarityScore ?? similarityEvidence.SimilarityScore;
        if (sourceSplitSimilarity.HasValue)
        {
            var normalizedSimilarity = ClampScore(sourceSplitSimilarity.Value);
            lineage["source_split_proximity_to_policy_threshold"] =
                normalizedSimilarity - sourceThresholdState.PolicyEffectiveSplitThreshold;
            lineage["source_split_proximity_to_dynamic_threshold"] =
                normalizedSimilarity - sourceThresholdState.DynamicSplitThreshold;
        }

        double? assignedSplitSimilarity = assignedSpeciesSimilarityScore;
        if (!assignedSplitSimilarity.HasValue
            && string.Equals(
                assignmentResolution.SpeciesId,
                resolvedSourceSpeciesId,
                StringComparison.Ordinal))
        {
            assignedSplitSimilarity = sourceSplitSimilarity;
        }

        if (assignedSplitSimilarity.HasValue)
        {
            var normalizedSimilarity = ClampScore(assignedSplitSimilarity.Value);
            lineage["assigned_split_proximity_to_policy_threshold"] =
                normalizedSimilarity - assignedThresholdState.PolicyEffectiveSplitThreshold;
            lineage["assigned_split_proximity_to_dynamic_threshold"] =
                normalizedSimilarity - assignedThresholdState.DynamicSplitThreshold;
        }

        var splitSimilarity = useAssignedSplitPerspective
            ? assignedSplitSimilarity ?? sourceSplitSimilarity
            : sourceSplitSimilarity;
        if (splitSimilarity.HasValue)
        {
            var normalizedSimilarity = ClampScore(splitSimilarity.Value);
            lineage["split_proximity_to_policy_threshold"] =
                normalizedSimilarity - genericThresholdState.PolicyEffectiveSplitThreshold;
            lineage["split_proximity_to_dynamic_threshold"] =
                normalizedSimilarity - genericThresholdState.DynamicSplitThreshold;
        }
        metadata["lineage"] = lineage;

        var scores = new JsonObject();
        if (similarityEvidence.SimilarityScore.HasValue)
        {
            scores["similarity_score"] = ClampScore(similarityEvidence.SimilarityScore.Value);
        }

        if (similarityEvidence.FunctionScore.HasValue)
        {
            scores["function_score"] = similarityEvidence.FunctionScore.Value;
        }

        if (similarityEvidence.ConnectivityScore.HasValue)
        {
            scores["connectivity_score"] = similarityEvidence.ConnectivityScore.Value;
        }

        if (similarityEvidence.RegionSpanScore.HasValue)
        {
            scores["region_span_score"] = similarityEvidence.RegionSpanScore.Value;
        }

        metadata["scores"] = scores;
        return metadata.ToJsonString(MetadataJsonSerializerOptions);
    }

    private static void AddLineageSimilarityScore(
        JsonObject target,
        string key,
        double? preferredValue,
        JsonNode? existingLineageNode,
        params string[] existingAliases)
    {
        var resolved = preferredValue ?? FindNumericValue(existingLineageNode, existingAliases);
        if (resolved.HasValue)
        {
            target[key] = ClampScore(resolved.Value);
        }
    }

    private static SimilarityEvidence ExtractSimilarityEvidence(string? decisionMetadataJson)
    {
        if (string.IsNullOrWhiteSpace(decisionMetadataJson))
        {
            return new SimilarityEvidence(
                SimilarityScore: null,
                DominantSpeciesSimilarityScore: null,
                ParentASimilarityScore: null,
                ParentBSimilarityScore: null,
                FunctionScore: null,
                ConnectivityScore: null,
                RegionSpanScore: null);
        }

        JsonNode? node;
        try
        {
            node = JsonNode.Parse(decisionMetadataJson);
        }
        catch (JsonException)
        {
            return new SimilarityEvidence(
                SimilarityScore: null,
                DominantSpeciesSimilarityScore: null,
                ParentASimilarityScore: null,
                ParentBSimilarityScore: null,
                FunctionScore: null,
                ConnectivityScore: null,
                RegionSpanScore: null);
        }

        JsonNode? lineageNode = null;
        if (node is JsonObject rootObject)
        {
            rootObject.TryGetPropertyValue("lineage", out lineageNode);
        }

        var similarityScore = FindNumericValue(
            lineageNode,
            "lineage_similarity_score",
            "lineageSimilarityScore")
            ?? FindNumericValue(
                lineageNode,
                "similarity_score",
                "similarityScore")
            ?? FindNumericValue(
                node,
                "lineage_similarity_score",
                "lineageSimilarityScore")
            ?? FindNumericValue(
                node,
                "similarity_score",
                "similarityScore");
        var dominantSpeciesSimilarityScore = FindNumericValue(
            lineageNode,
            "dominant_species_similarity_score",
            "dominantSpeciesSimilarityScore")
            ?? FindNumericValue(
                node,
                "dominant_species_similarity_score",
                "dominantSpeciesSimilarityScore");
        var parentASimilarityScore = FindNumericValue(
            lineageNode,
            "parent_a_similarity_score",
            "parentASimilarityScore",
            "lineage_parent_a_similarity_score",
            "lineageParentASimilarityScore")
            ?? FindNumericValue(
                node,
                "parent_a_similarity_score",
                "parentASimilarityScore",
                "lineage_parent_a_similarity_score",
                "lineageParentASimilarityScore");
        var parentBSimilarityScore = FindNumericValue(
            lineageNode,
            "parent_b_similarity_score",
            "parentBSimilarityScore",
            "lineage_parent_b_similarity_score",
            "lineageParentBSimilarityScore")
            ?? FindNumericValue(
                node,
                "parent_b_similarity_score",
                "parentBSimilarityScore",
                "lineage_parent_b_similarity_score",
                "lineageParentBSimilarityScore");
        var functionScore = FindNumericValue(
            node,
            "function_score",
            "functionScore");
        var connectivityScore = FindNumericValue(
            node,
            "connectivity_score",
            "connectivityScore");
        var regionSpanScore = FindNumericValue(
            node,
            "region_span_score",
            "regionSpanScore");
        return new SimilarityEvidence(
            SimilarityScore: similarityScore,
            DominantSpeciesSimilarityScore: dominantSpeciesSimilarityScore,
            ParentASimilarityScore: parentASimilarityScore,
            ParentBSimilarityScore: parentBSimilarityScore,
            FunctionScore: functionScore,
            ConnectivityScore: connectivityScore,
            RegionSpanScore: regionSpanScore);
    }

    private static double? FindNumericValue(JsonNode? node, params string[] aliases)
    {
        if (node is null || aliases.Length == 0)
        {
            return null;
        }

        var normalizedAliases = aliases
            .Select(NormalizeJsonKey)
            .Where(alias => alias.Length > 0)
            .ToHashSet(StringComparer.Ordinal);
        return normalizedAliases.Count == 0
            ? null
            : TryFindNumericValue(node, normalizedAliases, out var value)
                ? value
                : null;
    }

    private static string? FindStringValue(JsonNode? node, params string[] aliases)
    {
        if (node is null || aliases.Length == 0)
        {
            return null;
        }

        var normalizedAliases = aliases
            .Select(NormalizeJsonKey)
            .Where(alias => alias.Length > 0)
            .ToHashSet(StringComparer.Ordinal);
        if (normalizedAliases.Count == 0)
        {
            return null;
        }

        return TryFindStringValue(node, normalizedAliases, out var value)
            ? value
            : null;
    }

    private static bool TryFindNumericValue(
        JsonNode? node,
        HashSet<string> normalizedAliases,
        out double value)
    {
        if (node is JsonObject obj)
        {
            foreach (var property in obj)
            {
                if (normalizedAliases.Contains(NormalizeJsonKey(property.Key))
                    && TryReadDouble(property.Value, out value))
                {
                    return true;
                }

                if (TryFindNumericValue(property.Value, normalizedAliases, out value))
                {
                    return true;
                }
            }
        }
        else if (node is JsonArray array)
        {
            foreach (var item in array)
            {
                if (TryFindNumericValue(item, normalizedAliases, out value))
                {
                    return true;
                }
            }
        }

        value = default;
        return false;
    }

    private static bool TryFindStringValue(
        JsonNode? node,
        HashSet<string> normalizedAliases,
        out string value)
    {
        if (node is JsonObject obj)
        {
            foreach (var property in obj)
            {
                if (normalizedAliases.Contains(NormalizeJsonKey(property.Key))
                    && property.Value is JsonValue jsonValue
                    && jsonValue.TryGetValue<string>(out var textValue)
                    && !string.IsNullOrWhiteSpace(textValue))
                {
                    value = textValue.Trim();
                    return true;
                }

                if (TryFindStringValue(property.Value, normalizedAliases, out value))
                {
                    return true;
                }
            }
        }
        else if (node is JsonArray array)
        {
            foreach (var item in array)
            {
                if (TryFindStringValue(item, normalizedAliases, out value))
                {
                    return true;
                }
            }
        }

        value = string.Empty;
        return false;
    }

    private static JsonObject ParseMetadataJson(string? sourceMetadataJson)
    {
        if (string.IsNullOrWhiteSpace(sourceMetadataJson))
        {
            return new JsonObject();
        }

        var trimmed = sourceMetadataJson.Trim();
        try
        {
            var parsed = JsonNode.Parse(trimmed);
            if (parsed is JsonObject obj)
            {
                return (JsonObject)obj.DeepClone();
            }

            return new JsonObject
            {
                ["source_metadata"] = parsed?.DeepClone()
            };
        }
        catch (JsonException)
        {
            return new JsonObject
            {
                ["source_metadata_raw"] = trimmed
            };
        }
    }

    private static SpeciationAssignmentPolicy BuildAssignmentPolicy(SpeciationRuntimeConfig runtimeConfig)
    {
        var policyNode = TryResolvePolicyNode(runtimeConfig.ConfigSnapshotJson);
        var matchThreshold = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.92d,
            "lineage_match_threshold",
            "lineageMatchThreshold"));
        var hysteresisMargin = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.04d,
            "lineage_hysteresis_margin",
            "lineageHysteresisMargin"));
        var resolvedSplitThreshold = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.88d,
            "lineage_split_threshold",
            "lineageSplitThreshold"));
        if (resolvedSplitThreshold > matchThreshold)
        {
            resolvedSplitThreshold = matchThreshold;
        }

        var parentConsensusThreshold = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.70d,
            "parent_consensus_threshold",
            "parentConsensusThreshold"));
        var splitGuardMargin = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.02d,
            "lineage_split_guard_margin",
            "lineageSplitGuardMargin"));
        var minParentMembershipsBeforeSplit = ReadPolicyInt(
            policyNode,
            defaultValue: 1,
            "lineage_min_parent_memberships_before_split",
            "lineageMinParentMembershipsBeforeSplit");
        var recentSplitRealignParentMembershipWindow = ReadPolicyInt(
            policyNode,
            defaultValue: 3,
            "lineage_realign_parent_membership_window",
            "lineageRealignParentMembershipWindow");
        var recentSplitRealignMatchMargin = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.05d,
            "lineage_realign_match_margin",
            "lineageRealignMatchMargin"));
        var hindsightReassignCommitWindow = ReadPolicyInt(
            policyNode,
            defaultValue: 6,
            "lineage_hindsight_reassign_commit_window",
            "lineageHindsightReassignCommitWindow");
        var hindsightReassignSimilarityMargin = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.015d,
            "lineage_hindsight_similarity_margin",
            "lineageHindsightSimilarityMargin"));
        var createDerivedSpecies = ReadPolicyBool(
            policyNode,
            defaultValue: true,
            "create_derived_species_on_divergence",
            "createDerivedSpeciesOnDivergence");
        var derivedSpeciesPrefix = NormalizeToken(ReadPolicyString(
            policyNode,
            defaultValue: "branch",
            "derived_species_prefix",
            "derivedSpeciesPrefix"), "branch");

        return new SpeciationAssignmentPolicy(
            LineageMatchThreshold: matchThreshold,
            LineageSplitThreshold: resolvedSplitThreshold,
            ParentConsensusThreshold: parentConsensusThreshold,
            HysteresisMargin: hysteresisMargin,
            LineageSplitGuardMargin: splitGuardMargin,
            MinParentMembershipsBeforeSplit: Math.Max(1, minParentMembershipsBeforeSplit),
            RecentSplitRealignParentMembershipWindow: Math.Max(0, recentSplitRealignParentMembershipWindow),
            RecentSplitRealignMatchMargin: recentSplitRealignMatchMargin,
            HindsightReassignCommitWindow: Math.Max(0, hindsightReassignCommitWindow),
            HindsightReassignSimilarityMargin: hindsightReassignSimilarityMargin,
            CreateDerivedSpeciesOnDivergence: createDerivedSpecies,
            DerivedSpeciesPrefix: derivedSpeciesPrefix);
    }

    private static SpeciationRuntimeConfig BuildRuntimeConfigFromSettings(
        IReadOnlyDictionary<string, string> settings,
        SpeciationRuntimeConfig fallback)
    {
        var policyVersion = ReadSettingValue(
            settings,
            SpeciationSettingsKeys.PolicyVersionKey,
            fallback.PolicyVersion,
            SpeciationOptions.DefaultPolicyVersion);
        var defaultSpeciesId = ReadSettingValue(
            settings,
            SpeciationSettingsKeys.DefaultSpeciesIdKey,
            fallback.DefaultSpeciesId,
            SpeciationOptions.DefaultSpeciesId);
        var defaultSpeciesDisplayName = ReadSettingValue(
            settings,
            SpeciationSettingsKeys.DefaultSpeciesDisplayNameKey,
            fallback.DefaultSpeciesDisplayName,
            SpeciationOptions.DefaultSpeciesDisplayName);
        var startupReconcileReason = ReadSettingValue(
            settings,
            SpeciationSettingsKeys.StartupReconcileReasonKey,
            fallback.StartupReconcileDecisionReason,
            SpeciationOptions.DefaultStartupReconcileDecisionReason);

        var enabled = ParseBoolSetting(
            ReadSettingValue(settings, SpeciationSettingsKeys.ConfigEnabledKey, fallbackValue: "true", defaultValue: "true"),
            defaultValue: true);
        var matchThreshold = ClampScore(ParseDoubleSetting(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.LineageMatchThresholdKey,
                fallbackValue: null,
                defaultValue: "0.92"),
            defaultValue: 0.92d));
        var splitThreshold = ClampScore(ParseDoubleSetting(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.LineageSplitThresholdKey,
                fallbackValue: null,
                defaultValue: "0.88"),
            defaultValue: 0.88d));
        if (splitThreshold > matchThreshold)
        {
            splitThreshold = matchThreshold;
        }

        var parentConsensusThreshold = ClampScore(ParseDoubleSetting(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.ParentConsensusThresholdKey,
                fallbackValue: null,
                defaultValue: "0.70"),
            defaultValue: 0.70d));
        var hysteresisMargin = Math.Max(
            0d,
            ParseDoubleSetting(
                ReadSettingValue(
                    settings,
                    SpeciationSettingsKeys.LineageHysteresisMarginKey,
                    fallbackValue: null,
                    defaultValue: "0.04"),
                defaultValue: 0.04d));
        var splitGuardMargin = ClampScore(ParseDoubleSetting(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.LineageSplitGuardMarginKey,
                fallbackValue: null,
                defaultValue: "0.02"),
            defaultValue: 0.02d));
        var minParentMembershipsBeforeSplit = Math.Max(
            1,
            ParseIntSetting(
                ReadSettingValue(
                    settings,
                    SpeciationSettingsKeys.LineageMinParentMembershipsBeforeSplitKey,
                    fallbackValue: null,
                    defaultValue: "1"),
                defaultValue: 1));
        var realignParentMembershipWindow = Math.Max(
            0,
            ParseIntSetting(
                ReadSettingValue(
                    settings,
                    SpeciationSettingsKeys.LineageRealignParentMembershipWindowKey,
                    fallbackValue: null,
                    defaultValue: "3"),
                defaultValue: 3));
        var realignMatchMargin = ClampScore(ParseDoubleSetting(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.LineageRealignMatchMarginKey,
                fallbackValue: null,
                defaultValue: "0.05"),
            defaultValue: 0.05d));
        var hindsightReassignCommitWindow = Math.Max(
            0,
            ParseIntSetting(
                ReadSettingValue(
                    settings,
                    SpeciationSettingsKeys.LineageHindsightReassignCommitWindowKey,
                    fallbackValue: null,
                    defaultValue: "6"),
                defaultValue: 6));
        var hindsightReassignSimilarityMargin = ClampScore(ParseDoubleSetting(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.LineageHindsightSimilarityMarginKey,
                fallbackValue: null,
                defaultValue: "0.015"),
            defaultValue: 0.015d));
        var createDerivedSpecies = ParseBoolSetting(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.CreateDerivedSpeciesOnDivergenceKey,
                fallbackValue: null,
                defaultValue: "true"),
            defaultValue: true);
        var derivedSpeciesPrefix = NormalizeToken(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.DerivedSpeciesPrefixKey,
                fallbackValue: null,
                defaultValue: "branch"),
            "branch");

        var snapshot = new JsonObject
        {
            ["enabled"] = enabled,
            ["assignment_policy"] = new JsonObject
            {
                ["lineage_match_threshold"] = matchThreshold,
                ["lineage_split_threshold"] = splitThreshold,
                ["parent_consensus_threshold"] = parentConsensusThreshold,
                ["lineage_hysteresis_margin"] = hysteresisMargin,
                ["lineage_split_guard_margin"] = splitGuardMargin,
                ["lineage_min_parent_memberships_before_split"] = minParentMembershipsBeforeSplit,
                ["lineage_realign_parent_membership_window"] = realignParentMembershipWindow,
                ["lineage_realign_match_margin"] = realignMatchMargin,
                ["lineage_hindsight_reassign_commit_window"] = hindsightReassignCommitWindow,
                ["lineage_hindsight_similarity_margin"] = hindsightReassignSimilarityMargin,
                ["create_derived_species_on_divergence"] = createDerivedSpecies,
                ["derived_species_prefix"] = derivedSpeciesPrefix
            }
        };

        return new SpeciationRuntimeConfig(
            PolicyVersion: policyVersion,
            ConfigSnapshotJson: snapshot.ToJsonString(),
            DefaultSpeciesId: defaultSpeciesId,
            DefaultSpeciesDisplayName: defaultSpeciesDisplayName,
            StartupReconcileDecisionReason: startupReconcileReason);
    }

    private static string ReadSettingValue(
        IReadOnlyDictionary<string, string> settings,
        string key,
        string? fallbackValue,
        string defaultValue)
    {
        if (settings.TryGetValue(key, out var configured) && !string.IsNullOrWhiteSpace(configured))
        {
            return configured.Trim();
        }

        if (!string.IsNullOrWhiteSpace(fallbackValue))
        {
            return fallbackValue.Trim();
        }

        return defaultValue;
    }

    private static bool ParseBoolSetting(string? rawValue, bool defaultValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            return defaultValue;
        }

        return bool.TryParse(rawValue.Trim(), out var parsed) ? parsed : defaultValue;
    }

    private static double ParseDoubleSetting(string? rawValue, double defaultValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            return defaultValue;
        }

        return double.TryParse(rawValue.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : defaultValue;
    }

    private static int ParseIntSetting(string? rawValue, int defaultValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            return defaultValue;
        }

        return int.TryParse(rawValue.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : defaultValue;
    }

    private static JsonObject? TryResolvePolicyNode(string configSnapshotJson)
    {
        if (string.IsNullOrWhiteSpace(configSnapshotJson))
        {
            return null;
        }

        try
        {
            var node = JsonNode.Parse(configSnapshotJson);
            if (node is not JsonObject root)
            {
                return null;
            }

            if (root["assignment_policy"] is JsonObject assignmentPolicy)
            {
                return assignmentPolicy;
            }

            if (root["assignmentPolicy"] is JsonObject assignmentPolicyCamel)
            {
                return assignmentPolicyCamel;
            }

            return root;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static double? ReadOptionalPolicyDouble(JsonObject? policyNode, params string[] aliases)
    {
        return TryReadPolicyValue(policyNode, out double value, aliases) ? value : null;
    }

    private static double ReadPolicyDouble(JsonObject? policyNode, double defaultValue, params string[] aliases)
    {
        return TryReadPolicyValue(policyNode, out double value, aliases)
            ? value
            : defaultValue;
    }

    private static int ReadPolicyInt(JsonObject? policyNode, int defaultValue, params string[] aliases)
    {
        if (!TryReadPolicyValue(policyNode, out double value, aliases))
        {
            return defaultValue;
        }

        if (double.IsNaN(value) || double.IsInfinity(value))
        {
            return defaultValue;
        }

        return (int)Math.Max(0d, Math.Round(value, MidpointRounding.AwayFromZero));
    }

    private static bool ReadPolicyBool(JsonObject? policyNode, bool defaultValue, params string[] aliases)
    {
        if (!TryGetPolicyNode(policyNode, aliases, out var node) || node is null)
        {
            return defaultValue;
        }

        return node switch
        {
            JsonValue value when value.TryGetValue<bool>(out var asBool) => asBool,
            JsonValue value when value.TryGetValue<string>(out var asString)
                && bool.TryParse(asString, out var parsedBool) => parsedBool,
            _ => defaultValue
        };
    }

    private static string ReadPolicyString(JsonObject? policyNode, string defaultValue, params string[] aliases)
    {
        if (!TryGetPolicyNode(policyNode, aliases, out var node) || node is null)
        {
            return defaultValue;
        }

        return node is JsonValue value && value.TryGetValue<string>(out var asString) && !string.IsNullOrWhiteSpace(asString)
            ? asString.Trim()
            : defaultValue;
    }

    private static bool TryReadPolicyValue(
        JsonObject? policyNode,
        out double value,
        params string[] aliases)
    {
        if (!TryGetPolicyNode(policyNode, aliases, out var node) || !TryReadDouble(node, out value))
        {
            value = default;
            return false;
        }

        return true;
    }

    private static bool TryGetPolicyNode(
        JsonObject? policyNode,
        IReadOnlyList<string> aliases,
        out JsonNode? value)
    {
        if (policyNode is null || aliases.Count == 0)
        {
            value = null;
            return false;
        }

        var normalizedAliases = aliases
            .Select(NormalizeJsonKey)
            .Where(alias => alias.Length > 0)
            .ToHashSet(StringComparer.Ordinal);
        if (normalizedAliases.Count == 0)
        {
            value = null;
            return false;
        }

        foreach (var property in policyNode)
        {
            if (normalizedAliases.Contains(NormalizeJsonKey(property.Key)))
            {
                value = property.Value;
                return true;
            }
        }

        value = null;
        return false;
    }

    private static bool TryReadDouble(JsonNode? node, out double value)
    {
        switch (node)
        {
            case JsonValue valueNode when valueNode.TryGetValue<double>(out value):
                return true;
            case JsonValue valueNode when valueNode.TryGetValue<float>(out var asFloat):
                value = asFloat;
                return true;
            case JsonValue valueNode when valueNode.TryGetValue<decimal>(out var asDecimal):
                value = (double)asDecimal;
                return true;
            case JsonValue valueNode when valueNode.TryGetValue<long>(out var asLong):
                value = asLong;
                return true;
            case JsonValue valueNode when valueNode.TryGetValue<int>(out var asInt):
                value = asInt;
                return true;
            case JsonValue valueNode when valueNode.TryGetValue<string>(out var asString)
                && double.TryParse(asString, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed):
                value = parsed;
                return true;
            default:
                value = default;
                return false;
        }
    }

    private static double ClampScore(double value)
    {
        if (double.IsNaN(value) || double.IsInfinity(value))
        {
            return 0d;
        }

        if (value < 0d)
        {
            return 0d;
        }

        return value > 1d ? 1d : value;
    }

    private static string BuildLineageKey(IReadOnlyList<Guid> orderedParentBrainIds)
    {
        if (orderedParentBrainIds.Count == 0)
        {
            return string.Empty;
        }

        return string.Join("|", orderedParentBrainIds.Select(parentBrainId => parentBrainId.ToString("D")));
    }

    private static string BuildDerivedSpeciesId(
        string dominantSpeciesId,
        string derivedSpeciesPrefix,
        string lineageKey,
        long epochId)
    {
        var normalizedDominant = NormalizeToken(dominantSpeciesId, "species");
        var normalizedPrefix = NormalizeToken(derivedSpeciesPrefix, "branch");
        var hashInput = $"{epochId}:{lineageKey}:{normalizedDominant}:{normalizedPrefix}";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(hashInput));
        var suffix = Convert.ToHexString(hash.AsSpan(0, 6)).ToLowerInvariant();
        var maxSpeciesIdLength = 96;
        var prefixBudget = Math.Max(1, maxSpeciesIdLength - suffix.Length - 2);
        var truncatedPrefix = normalizedPrefix.Length <= prefixBudget
            ? normalizedPrefix
            : normalizedPrefix[..prefixBudget];
        var dominantBudget = Math.Max(1, maxSpeciesIdLength - truncatedPrefix.Length - suffix.Length - 2);
        var truncatedDominant = normalizedDominant.Length <= dominantBudget
            ? normalizedDominant
            : normalizedDominant[..dominantBudget];
        return $"{truncatedDominant}-{truncatedPrefix}-{suffix}";
    }

    private static string BuildFounderRootSpeciesId(
        string baselineSpeciesId,
        string lineageKey,
        long epochId)
    {
        var normalizedBaseline = NormalizeToken(baselineSpeciesId, "species");
        const string founderPrefix = "founder";
        var hashInput = $"{epochId}:{lineageKey}:{normalizedBaseline}:{founderPrefix}";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(hashInput));
        var suffix = Convert.ToHexString(hash.AsSpan(0, 6)).ToLowerInvariant();
        var maxSpeciesIdLength = 96;
        var prefixBudget = Math.Max(1, maxSpeciesIdLength - suffix.Length - 2);
        var truncatedPrefix = founderPrefix.Length <= prefixBudget
            ? founderPrefix
            : founderPrefix[..prefixBudget];
        var baselineBudget = Math.Max(1, maxSpeciesIdLength - truncatedPrefix.Length - suffix.Length - 2);
        var truncatedBaseline = normalizedBaseline.Length <= baselineBudget
            ? normalizedBaseline
            : normalizedBaseline[..baselineBudget];
        return $"{truncatedBaseline}-{truncatedPrefix}-{suffix}";
    }

    private static string BuildDerivedSpeciesDisplayName(
        string? dominantSpeciesDisplayName,
        string dominantSpeciesId,
        string derivedSpeciesId)
    {
        var parentDisplayName = ResolveSpeciesDisplayName(dominantSpeciesDisplayName, dominantSpeciesId);
        var (stem, lineageCode) = ParseLineageDisplayName(parentDisplayName);
        if (lineageCode.Length == 0
            && TryParseNumberedRootSpeciesDisplayName(stem, out _, out var rootOrdinal))
        {
            lineageCode = BuildRootSpeciesLineagePrefix(rootOrdinal);
        }

        var nextLetter = ComputeLineageLetter(derivedSpeciesId);
        var nextCode = lineageCode + nextLetter;
        return $"{stem} [{nextCode}]";
    }

    private static string ResolveSpeciesDisplayName(string? preferredDisplayName, string speciesId)
    {
        if (!string.IsNullOrWhiteSpace(preferredDisplayName))
        {
            return preferredDisplayName.Trim();
        }

        return BuildDisplayNameFromSpeciesId(speciesId);
    }

    private static (string Stem, string LineageCode) ParseLineageDisplayName(string displayName)
    {
        if (string.IsNullOrWhiteSpace(displayName))
        {
            return ("Species", string.Empty);
        }

        var trimmed = displayName.Trim();
        var openIndex = trimmed.LastIndexOf('[');
        if (openIndex >= 0 && trimmed.EndsWith(']') && openIndex < trimmed.Length - 2)
        {
            var candidateCode = trimmed[(openIndex + 1)..^1].Trim();
            if (candidateCode.Length > 0 && candidateCode.All(ch => ch is >= 'A' and <= 'Z'))
            {
                var stem = trimmed[..openIndex].TrimEnd();
                return (stem.Length == 0 ? "Species" : stem, candidateCode);
            }
        }

        return (trimmed, string.Empty);
    }

    private static string ComputeLineageLetter(string seed)
    {
        if (string.IsNullOrWhiteSpace(seed))
        {
            return "A";
        }

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(seed.Trim()));
        var letter = (char)('A' + (hash[0] % 26));
        return letter.ToString(CultureInfo.InvariantCulture);
    }

    private static bool TryParseNumberedRootSpeciesDisplayName(
        string? speciesDisplayStem,
        out string rootStem,
        out int rootOrdinal)
    {
        rootStem = string.IsNullOrWhiteSpace(speciesDisplayStem)
            ? "Species"
            : speciesDisplayStem.Trim();
        rootOrdinal = 0;

        var separatorIndex = rootStem.LastIndexOf('-');
        if (separatorIndex <= 0 || separatorIndex >= rootStem.Length - 1)
        {
            return false;
        }

        var ordinalToken = rootStem[(separatorIndex + 1)..].Trim();
        if (!int.TryParse(
                ordinalToken,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out rootOrdinal)
            || rootOrdinal <= 0)
        {
            rootOrdinal = 0;
            return false;
        }

        rootStem = rootStem[..separatorIndex].TrimEnd();
        if (rootStem.Length == 0)
        {
            rootStem = "Species";
        }

        return true;
    }

    private static string BuildNumberedRootSpeciesDisplayName(string rootStem, int rootOrdinal)
    {
        var normalizedStem = string.IsNullOrWhiteSpace(rootStem)
            ? "Species"
            : rootStem.Trim();
        return rootOrdinal > 0
            ? $"{normalizedStem}-{rootOrdinal.ToString(CultureInfo.InvariantCulture)}"
            : normalizedStem;
    }

    private static string BuildRootSpeciesLineagePrefix(int rootOrdinal)
    {
        var normalizedOrdinal = Math.Max(1, rootOrdinal);
        var builder = new StringBuilder();
        while (normalizedOrdinal > 0)
        {
            normalizedOrdinal--;
            builder.Insert(0, (char)('A' + (normalizedOrdinal % 26)));
            normalizedOrdinal /= 26;
        }

        return builder.ToString();
    }

    private static bool IsSeedFounderCandidate(
        Guid candidateBrainId,
        IReadOnlyList<Guid> inputOrderedParentBrainIds)
    {
        if (candidateBrainId == Guid.Empty || inputOrderedParentBrainIds.Count == 0)
        {
            return false;
        }

        var containsSelf = false;
        var containsDistinctPeer = false;
        foreach (var parentBrainId in inputOrderedParentBrainIds)
        {
            if (parentBrainId == Guid.Empty)
            {
                continue;
            }

            if (parentBrainId == candidateBrainId)
            {
                containsSelf = true;
                continue;
            }

            containsDistinctPeer = true;
        }

        return containsSelf && containsDistinctPeer;
    }

    private static string BuildDisplayNameFromSpeciesId(string speciesId)
    {
        if (string.IsNullOrWhiteSpace(speciesId))
        {
            return "Species";
        }

        var tokens = speciesId
            .Trim()
            .Split(['.', '-', '_', '/', ':'], StringSplitOptions.RemoveEmptyEntries);
        if (tokens.Length == 0)
        {
            return speciesId.Trim();
        }

        var parts = tokens
            .Select(FormatDisplayToken)
            .Where(part => part.Length > 0)
            .ToArray();
        if (parts.Length == 0)
        {
            return speciesId.Trim();
        }

        return string.Join(' ', parts);
    }

    private static string FormatDisplayToken(string token)
    {
        if (string.IsNullOrWhiteSpace(token))
        {
            return string.Empty;
        }

        var trimmed = token.Trim();
        if (trimmed.Length == 0)
        {
            return string.Empty;
        }

        if (trimmed.Length == 1)
        {
            return char.ToUpperInvariant(trimmed[0]).ToString(CultureInfo.InvariantCulture);
        }

        return char.ToUpperInvariant(trimmed[0]) + trimmed[1..].ToLowerInvariant();
    }

    private static string NormalizeToken(string? value, string fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        var builder = new StringBuilder(value.Length);
        foreach (var ch in value.Trim().ToLowerInvariant())
        {
            if ((ch >= 'a' && ch <= 'z')
                || (ch >= '0' && ch <= '9')
                || ch == '-'
                || ch == '_')
            {
                builder.Append(ch);
            }
            else if (builder.Length == 0 || builder[^1] != '-')
            {
                builder.Append('-');
            }
        }

        var normalized = builder.ToString().Trim('-');
        return normalized.Length == 0 ? fallback : normalized;
    }

    private static string NormalizeJsonKey(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return string.Empty;
        }

        var builder = new StringBuilder(key.Length);
        foreach (var ch in key)
        {
            if (char.IsLetterOrDigit(ch))
            {
                builder.Append(char.ToLowerInvariant(ch));
            }
        }

        return builder.ToString();
    }

    private static ProtoSpec.SpeciationApplyMode NormalizeApplyMode(ProtoSpec.SpeciationApplyMode applyMode)
    {
        return applyMode == ProtoSpec.SpeciationApplyMode.Commit
            ? ProtoSpec.SpeciationApplyMode.Commit
            : ProtoSpec.SpeciationApplyMode.DryRun;
    }

    private static ProtoSpec.SpeciationSetConfigResponse CreateProtoSetConfigResponse(
        ProtoSpec.SpeciationFailureReason failureReason,
        string failureDetail,
        SpeciationEpochInfo previousEpoch,
        SpeciationEpochInfo currentEpoch,
        SpeciationRuntimeConfig runtimeConfig)
    {
        return new ProtoSpec.SpeciationSetConfigResponse
        {
            FailureReason = failureReason,
            FailureDetail = failureDetail,
            PreviousEpoch = ToProtoEpochInfo(previousEpoch),
            CurrentEpoch = ToProtoEpochInfo(currentEpoch),
            Config = ToProtoRuntimeConfig(runtimeConfig)
        };
    }

    private static ProtoSpec.SpeciationResetAllResponse CreateProtoResetAllResponse(
        ProtoSpec.SpeciationFailureReason failureReason,
        string failureDetail,
        SpeciationEpochInfo previousEpoch,
        SpeciationEpochInfo currentEpoch,
        SpeciationRuntimeConfig runtimeConfig,
        int deletedEpochCount,
        int deletedMembershipCount,
        int deletedSpeciesCount,
        int deletedDecisionCount,
        int deletedLineageEdgeCount)
    {
        return new ProtoSpec.SpeciationResetAllResponse
        {
            FailureReason = failureReason,
            FailureDetail = failureDetail,
            PreviousEpoch = ToProtoEpochInfo(previousEpoch),
            CurrentEpoch = ToProtoEpochInfo(currentEpoch),
            Config = ToProtoRuntimeConfig(runtimeConfig),
            DeletedEpochCount = (uint)Math.Max(0, deletedEpochCount),
            DeletedMembershipCount = (uint)Math.Max(0, deletedMembershipCount),
            DeletedSpeciesCount = (uint)Math.Max(0, deletedSpeciesCount),
            DeletedDecisionCount = (uint)Math.Max(0, deletedDecisionCount),
            DeletedLineageEdgeCount = (uint)Math.Max(0, deletedLineageEdgeCount)
        };
    }

    private static ProtoSpec.SpeciationDeleteEpochResponse CreateProtoDeleteEpochResponse(
        ProtoSpec.SpeciationFailureReason failureReason,
        string failureDetail,
        long epochId,
        bool deleted,
        int deletedMembershipCount,
        int deletedSpeciesCount,
        int deletedDecisionCount,
        int deletedLineageEdgeCount,
        SpeciationEpochInfo currentEpoch)
    {
        return new ProtoSpec.SpeciationDeleteEpochResponse
        {
            FailureReason = failureReason,
            FailureDetail = failureDetail,
            EpochId = (ulong)Math.Max(0, epochId),
            Deleted = deleted,
            DeletedMembershipCount = (uint)Math.Max(0, deletedMembershipCount),
            DeletedSpeciesCount = (uint)Math.Max(0, deletedSpeciesCount),
            DeletedDecisionCount = (uint)Math.Max(0, deletedDecisionCount),
            DeletedLineageEdgeCount = (uint)Math.Max(0, deletedLineageEdgeCount),
            CurrentEpoch = ToProtoEpochInfo(currentEpoch)
        };
    }

    private static ProtoSpec.SpeciationDecision CreateDecisionFailure(
        ProtoSpec.SpeciationApplyMode applyMode,
        ProtoSpec.SpeciationFailureReason failureReason,
        string failureDetail)
    {
        return new ProtoSpec.SpeciationDecision
        {
            ApplyMode = applyMode,
            CandidateMode = ProtoSpec.SpeciationCandidateMode.Unknown,
            Success = false,
            Created = false,
            ImmutableConflict = false,
            FailureReason = failureReason,
            FailureDetail = failureDetail,
            SpeciesId = string.Empty,
            SpeciesDisplayName = string.Empty,
            DecisionReason = string.Empty,
            DecisionMetadataJson = "{}",
            Committed = false
        };
    }

    private static void RecordDecisionTelemetry(
        string operation,
        long epochId,
        double durationMs,
        Activity? activity,
        ProtoSpec.SpeciationDecision decision)
    {
        SpeciationTelemetry.RecordAssignmentDecision(operation, decision, durationMs);
        SpeciationTelemetry.CompleteAssignmentActivity(activity, epochId, decision, durationMs);
    }

    private static void RecordStartupReconcileTelemetry(
        Activity? activity,
        long epochId,
        int knownBrains,
        SpeciationReconcileResult? result,
        string outcome,
        string failureReason)
    {
        SpeciationTelemetry.RecordStartupReconcile(knownBrains, result, outcome, failureReason);
        SpeciationTelemetry.CompleteStartupReconcileActivity(
            activity,
            epochId,
            knownBrains,
            result,
            outcome,
            failureReason);
    }

    private static void RecordEpochTransitionTelemetry(
        Activity? activity,
        string transition,
        string outcome,
        string failureReason,
        long previousEpochId,
        long currentEpochId,
        int deletedMembershipCount = 0,
        int deletedSpeciesCount = 0,
        int deletedDecisionCount = 0,
        int deletedLineageEdgeCount = 0,
        int deletedEpochCount = 0)
    {
        SpeciationTelemetry.RecordEpochTransition(transition, outcome, failureReason);
        SpeciationTelemetry.CompleteEpochTransitionActivity(
            activity,
            transition,
            outcome,
            failureReason,
            previousEpochId,
            currentEpochId,
            deletedMembershipCount,
            deletedSpeciesCount,
            deletedDecisionCount,
            deletedLineageEdgeCount,
            deletedEpochCount);
    }

    private static ProtoSpec.SpeciationDecision CreateDecisionFromMembership(
        ProtoSpec.SpeciationApplyMode applyMode,
        ProtoSpec.SpeciationCandidateMode candidateMode,
        SpeciationMembershipRecord membership,
        bool created,
        bool immutableConflict,
        bool committed,
        ProtoSpec.SpeciationFailureReason failureReason,
        string failureDetail,
        bool? successOverride = null)
    {
        var success = successOverride ?? (failureReason == ProtoSpec.SpeciationFailureReason.SpeciationFailureNone);
        return new ProtoSpec.SpeciationDecision
        {
            ApplyMode = applyMode,
            CandidateMode = candidateMode,
            Success = success,
            Created = created,
            ImmutableConflict = immutableConflict,
            FailureReason = failureReason,
            FailureDetail = failureDetail,
            SpeciesId = membership.SpeciesId,
            SpeciesDisplayName = membership.SpeciesDisplayName,
            DecisionReason = membership.DecisionReason,
            DecisionMetadataJson = membership.DecisionMetadataJson,
            Committed = committed,
            Membership = ToProtoMembershipRecord(membership)
        };
    }

    private static ProtoSpec.SpeciationRuntimeConfig ToProtoRuntimeConfig(SpeciationRuntimeConfig config)
    {
        return new ProtoSpec.SpeciationRuntimeConfig
        {
            PolicyVersion = config.PolicyVersion,
            ConfigSnapshotJson = config.ConfigSnapshotJson,
            DefaultSpeciesId = config.DefaultSpeciesId,
            DefaultSpeciesDisplayName = config.DefaultSpeciesDisplayName,
            StartupReconcileDecisionReason = config.StartupReconcileDecisionReason
        };
    }

    private static ProtoSpec.SpeciationEpochInfo ToProtoEpochInfo(SpeciationEpochInfo epoch)
    {
        return new ProtoSpec.SpeciationEpochInfo
        {
            EpochId = (ulong)Math.Max(0, epoch.EpochId),
            CreatedMs = (ulong)Math.Max(0, epoch.CreatedMs),
            PolicyVersion = epoch.PolicyVersion ?? "unknown",
            ConfigSnapshotJson = epoch.ConfigSnapshotJson ?? "{}"
        };
    }

    private static ProtoSpec.SpeciationStatusSnapshot ToProtoStatusSnapshot(SpeciationStatusSnapshot status)
    {
        return new ProtoSpec.SpeciationStatusSnapshot
        {
            EpochId = (ulong)Math.Max(0, status.EpochId),
            MembershipCount = (uint)Math.Max(0, status.MembershipCount),
            SpeciesCount = (uint)Math.Max(0, status.SpeciesCount),
            LineageEdgeCount = (uint)Math.Max(0, status.LineageEdgeCount)
        };
    }

    private static ProtoSpec.SpeciationMembershipRecord ToProtoMembershipRecord(SpeciationMembershipRecord membership)
    {
        var proto = new ProtoSpec.SpeciationMembershipRecord
        {
            EpochId = (ulong)Math.Max(0, membership.EpochId),
            BrainId = membership.BrainId.ToProtoUuid(),
            SpeciesId = membership.SpeciesId,
            SpeciesDisplayName = membership.SpeciesDisplayName,
            AssignedMs = (ulong)Math.Max(0, membership.AssignedMs),
            PolicyVersion = membership.PolicyVersion,
            DecisionReason = membership.DecisionReason,
            DecisionMetadataJson = membership.DecisionMetadataJson,
            SourceArtifactRef = membership.SourceArtifactRef ?? string.Empty,
            DecisionId = (ulong)Math.Max(0, membership.DecisionId),
            HasSourceBrainId = membership.SourceBrainId.HasValue
        };
        if (membership.SourceBrainId.HasValue)
        {
            proto.SourceBrainId = membership.SourceBrainId.Value.ToProtoUuid();
        }

        return proto;
    }

    private static SpeciationRuntimeConfig FromProtoRuntimeConfig(
        ProtoSpec.SpeciationRuntimeConfig? request,
        SpeciationRuntimeConfig fallback)
    {
        if (request is null)
        {
            return fallback;
        }

        return new SpeciationRuntimeConfig(
            PolicyVersion: NormalizeOrFallback(request.PolicyVersion, fallback.PolicyVersion),
            ConfigSnapshotJson: NormalizeJsonOrFallback(request.ConfigSnapshotJson, fallback.ConfigSnapshotJson),
            DefaultSpeciesId: NormalizeOrFallback(request.DefaultSpeciesId, fallback.DefaultSpeciesId),
            DefaultSpeciesDisplayName: NormalizeOrFallback(request.DefaultSpeciesDisplayName, fallback.DefaultSpeciesDisplayName),
            StartupReconcileDecisionReason: NormalizeOrFallback(request.StartupReconcileDecisionReason, fallback.StartupReconcileDecisionReason));
    }

    private readonly record struct SpeciationAssignmentPolicy(
        double LineageMatchThreshold,
        double LineageSplitThreshold,
        double ParentConsensusThreshold,
        double HysteresisMargin,
        double LineageSplitGuardMargin,
        int MinParentMembershipsBeforeSplit,
        int RecentSplitRealignParentMembershipWindow,
        double RecentSplitRealignMatchMargin,
        int HindsightReassignCommitWindow,
        double HindsightReassignSimilarityMargin,
        bool CreateDerivedSpeciesOnDivergence,
        string DerivedSpeciesPrefix);

    private readonly record struct SimilarityEvidence(
        double? SimilarityScore,
        double? DominantSpeciesSimilarityScore,
        double? ParentASimilarityScore,
        double? ParentBSimilarityScore,
        double? FunctionScore,
        double? ConnectivityScore,
        double? RegionSpanScore);

    private readonly record struct LineageEvidence(
        IReadOnlyList<Guid> ParentBrainIds,
        IReadOnlyList<string> ParentArtifactRefs,
        int ParentMembershipCount,
        string? DominantSpeciesId,
        string? DominantSpeciesDisplayName,
        double DominantShare,
        string LineageKey,
        string? HysteresisSpeciesId,
        string? HysteresisSpeciesDisplayName,
        string? HysteresisDecisionReason);

    private readonly record struct AssignmentResolution(
        string SpeciesId,
        string SpeciesDisplayName,
        string DecisionReason,
        string Strategy,
        string StrategyDetail,
        bool ForceDecisionReason,
        double PolicyEffectiveSplitThreshold = 0d,
        double EffectiveSplitThreshold = 0d,
        bool SplitTriggeredBySpeciesFloor = false,
        string? SourceSpeciesId = null,
        string? SourceSpeciesDisplayName = null,
        double? SourceSpeciesSimilarityScore = null,
        double SourceConsensusShare = 0d,
        double? SpeciesFloorSimilarityScore = null,
        int SpeciesFloorSampleCount = 0,
        int SpeciesFloorMembershipCount = 0,
        string? RecentDerivedSourceSpeciesId = null,
        string? RecentDerivedSourceSpeciesDisplayName = null,
        double? RecentDerivedFounderSimilarityScore = null,
        string? DisplayNameRewriteSpeciesId = null,
        string? DisplayNameRewriteSpeciesDisplayName = null);

    private readonly record struct FounderRootSpeciesNamingPlan(
        string FounderSpeciesDisplayName,
        string? SourceSpeciesDisplayNameRewrite,
        string? SourceSpeciesIdToRewrite);

    private readonly record struct SpeciesSimilarityFloorState(
        int MembershipCount,
        int SimilaritySampleCount,
        int ActualSimilaritySampleCount,
        double? MinSimilarityScore);

    private readonly record struct SplitThresholdState(
        double PolicyEffectiveSplitThreshold,
        double DynamicSplitThreshold,
        bool UsesSpeciesFloor,
        double? SpeciesFloorSimilarityScore,
        int SpeciesFloorSampleCount,
        int SpeciesFloorMembershipCount);

    private readonly record struct ParentSpeciesPairwiseFit(
        string SpeciesId,
        string SpeciesDisplayName,
        double PairwiseSimilarity,
        int SupportingParentCount,
        long LatestAssignedMs);

    private readonly record struct RecentDerivedSpeciesHint(
        string SourceSpeciesId,
        string SourceSpeciesDisplayName,
        string TargetSpeciesId,
        string TargetSpeciesDisplayName,
        double FounderSimilarityScore,
        long AssignedMs);

    private readonly record struct BootstrapAssignedSpeciesAdmissionRequirement(
        string TargetSpeciesId,
        string TargetSpeciesDisplayName,
        string SourceSpeciesId,
        string SourceSpeciesDisplayName,
        int MembershipCount,
        int ActualSimilaritySampleCount);

    private readonly record struct AssignedSpeciesAdmissionAssessment(
        bool AssessmentAttempted,
        bool Admitted,
        double? SimilarityScore,
        string AssessmentMode,
        string[] ExemplarBrainIds,
        bool Compatible,
        string AbortReason,
        string FailureReason,
        long ElapsedMs);

    private readonly record struct CompatibilitySimilarityAssessment(
        bool RequestAttempted,
        double? SimilarityScore,
        bool Compatible,
        string AbortReason,
        string FailureReason,
        string AssessmentMode);

    private enum CompatibilitySubjectKind
    {
        None = 0,
        BrainId = 1,
        ArtifactRef = 2
    }

    private readonly record struct CompatibilitySubject(
        CompatibilitySubjectKind Kind,
        Guid BrainId,
        ArtifactRef? ArtifactDefRef,
        ArtifactRef? ArtifactStateRef);

    private readonly record struct ResolvedCandidate(
        ProtoSpec.SpeciationCandidateMode CandidateMode,
        Guid BrainId,
        string? SourceArtifactRef,
        ArtifactRef? CandidateArtifactRef,
        string? CandidateArtifactUri,
        ArtifactRef? CandidateBrainBaseArtifactRef = null,
        ArtifactRef? CandidateBrainSnapshotArtifactRef = null);

    private readonly record struct BrainArtifactProvenance(
        ArtifactRef? BaseArtifactRef,
        ArtifactRef? SnapshotArtifactRef);

    private bool TryGetCurrentEpoch(out SpeciationEpochInfo epoch)
    {
        if (_initialized && _currentEpoch is not null)
        {
            epoch = _currentEpoch;
            return true;
        }

        epoch = CreateFallbackEpoch();
        return false;
    }

    private SpeciationEpochInfo CreateFallbackEpoch()
    {
        return new SpeciationEpochInfo(
            EpochId: 0,
            CreatedMs: 0,
            PolicyVersion: _runtimeConfig.PolicyVersion,
            ConfigSnapshotJson: _runtimeConfig.ConfigSnapshotJson);
    }

    private SpeciationRuntimeConfig BuildResetRuntimeConfig(SpeciationResetEpochRequest request)
    {
        return new SpeciationRuntimeConfig(
            PolicyVersion: NormalizeOrFallback(request.PolicyVersion, _runtimeConfig.PolicyVersion),
            ConfigSnapshotJson: NormalizeJsonOrFallback(request.ConfigSnapshotJson, _runtimeConfig.ConfigSnapshotJson),
            DefaultSpeciesId: _runtimeConfig.DefaultSpeciesId,
            DefaultSpeciesDisplayName: _runtimeConfig.DefaultSpeciesDisplayName,
            StartupReconcileDecisionReason: _runtimeConfig.StartupReconcileDecisionReason);
    }

    private SpeciationRuntimeConfig BuildReconcileRuntimeConfig(SpeciationReconcileKnownBrainsRequest request)
    {
        return new SpeciationRuntimeConfig(
            PolicyVersion: NormalizeOrFallback(request.PolicyVersion, _runtimeConfig.PolicyVersion),
            ConfigSnapshotJson: _runtimeConfig.ConfigSnapshotJson,
            DefaultSpeciesId: NormalizeOrFallback(request.SpeciesId, _runtimeConfig.DefaultSpeciesId),
            DefaultSpeciesDisplayName: NormalizeOrFallback(request.SpeciesDisplayName, _runtimeConfig.DefaultSpeciesDisplayName),
            StartupReconcileDecisionReason: NormalizeOrFallback(request.DecisionReason, _runtimeConfig.StartupReconcileDecisionReason));
    }

    private static string NormalizeOrFallback(string? value, string fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        return value.Trim();
    }

    private static string NormalizeJsonOrFallback(string? value, string fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        var trimmed = value.Trim();
        return trimmed.Length == 0 ? "{}" : trimmed;
    }

    private static void LogError(string message)
    {
        Console.WriteLine($"[SpeciationManager] {message}");
    }
}

