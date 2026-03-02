using Nbn.Proto;
using Nbn.Proto.Settings;
using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;
using ProtoSpec = Nbn.Proto.Speciation;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Nbn.Runtime.Speciation;

public sealed class SpeciationManagerActor : IActor
{
    private static readonly TimeSpan DefaultSettingsRequestTimeout = TimeSpan.FromSeconds(5);
    private static readonly JsonSerializerOptions MetadataJsonSerializerOptions = new()
    {
        WriteIndented = false
    };

    private readonly SpeciationStore _store;
    private SpeciationRuntimeConfig _runtimeConfig;
    private SpeciationAssignmentPolicy _assignmentPolicy;
    private readonly PID? _settingsPid;
    private readonly TimeSpan _settingsRequestTimeout;

    private bool _initializing;
    private bool _initialized;
    private SpeciationEpochInfo? _currentEpoch;

    public SpeciationManagerActor(
        SpeciationStore store,
        SpeciationRuntimeConfig runtimeConfig,
        PID? settingsPid,
        TimeSpan? settingsRequestTimeout = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _runtimeConfig = runtimeConfig ?? throw new ArgumentNullException(nameof(runtimeConfig));
        _assignmentPolicy = BuildAssignmentPolicy(runtimeConfig);
        _settingsPid = settingsPid;
        _settingsRequestTimeout = settingsRequestTimeout ?? DefaultSettingsRequestTimeout;
    }

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
        }

        return Task.CompletedTask;
    }

    private void HandleStarted(IContext context)
    {
        if (_initializing || _initialized)
        {
            return;
        }

        _initializing = true;
        var initializeTask = InitializeStoreAsync();
        context.ReenterAfter(initializeTask, completed =>
        {
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

    private async Task<SpeciationEpochInfo> InitializeStoreAsync()
    {
        await _store.InitializeAsync().ConfigureAwait(false);
        return await _store.EnsureCurrentEpochAsync(_runtimeConfig).ConfigureAwait(false);
    }

    private void StartStartupReconciliation(IContext context)
    {
        if (_settingsPid is null || !_initialized || _currentEpoch is null)
        {
            return;
        }

        var brainListTask = context.RequestAsync<ProtoSettings.BrainListResponse>(
            _settingsPid,
            new ProtoSettings.BrainListRequest(),
            _settingsRequestTimeout);

        context.ReenterAfter(brainListTask, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"Speciation startup reconcile skipped: failed to fetch BrainList from SettingsMonitor: {completed.Exception?.GetBaseException().Message}");
                return Task.CompletedTask;
            }

            var knownBrains = ParseKnownBrainIds(completed.Result);
            if (knownBrains.Count == 0 || _currentEpoch is null)
            {
                return Task.CompletedTask;
            }

            var reconcileTask = _store.ReconcileMissingMembershipsAsync(
                _currentEpoch.EpochId,
                knownBrains,
                _runtimeConfig,
                decisionMetadataJson: "{\"source\":\"startup_reconcile\"}");

            context.ReenterAfter(reconcileTask, reconcileCompleted =>
            {
                if (reconcileCompleted.IsFaulted)
                {
                    LogError($"Speciation startup reconcile failed: {reconcileCompleted.Exception?.GetBaseException().Message}");
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

            context.Respond(new SpeciationReconcileKnownBrainsResponse(completed.Result));
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
                return;
            }

            var applyTime = message.HasApplyTimeMs ? (long?)message.ApplyTimeMs : null;
            var resetTask = _store.ResetEpochAsync(nextConfig, applyTime);
            context.ReenterAfter(resetTask, completed =>
            {
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

    private void HandleProtoEvaluate(IContext context, ProtoSpec.SpeciationEvaluateRequest message)
    {
        if (!TryGetCurrentEpoch(out var epoch))
        {
            context.Respond(new ProtoSpec.SpeciationEvaluateResponse
            {
                Decision = CreateDecisionFailure(
                    ProtoSpec.SpeciationApplyMode.DryRun,
                    ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceInitializing,
                    "Speciation service is still initializing.")
            });
            return;
        }

        var evaluateTask = ProcessProtoDecisionAsync(
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
            if (completed.IsFaulted)
            {
                LogError($"Speciation proto evaluate failed: {completed.Exception?.GetBaseException().Message}");
                context.Respond(new ProtoSpec.SpeciationEvaluateResponse
                {
                    Decision = CreateDecisionFailure(
                        ProtoSpec.SpeciationApplyMode.DryRun,
                        ProtoSpec.SpeciationFailureReason.SpeciationFailureStoreError,
                        "Failed to evaluate speciation decision.")
                });
                return Task.CompletedTask;
            }

            context.Respond(new ProtoSpec.SpeciationEvaluateResponse
            {
                Decision = completed.Result
            });
            return Task.CompletedTask;
        });
    }

    private void HandleProtoAssign(IContext context, ProtoSpec.SpeciationAssignRequest message)
    {
        var applyMode = NormalizeApplyMode(message.ApplyMode);
        if (!TryGetCurrentEpoch(out var epoch))
        {
            context.Respond(new ProtoSpec.SpeciationAssignResponse
            {
                Decision = CreateDecisionFailure(
                    applyMode,
                    ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceInitializing,
                    "Speciation service is still initializing.")
            });
            return;
        }

        var assignTask = ProcessProtoDecisionAsync(
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
            if (completed.IsFaulted)
            {
                LogError($"Speciation proto assign failed: {completed.Exception?.GetBaseException().Message}");
                context.Respond(new ProtoSpec.SpeciationAssignResponse
                {
                    Decision = CreateDecisionFailure(
                        applyMode,
                        ProtoSpec.SpeciationFailureReason.SpeciationFailureStoreError,
                        "Failed to process speciation assignment.")
                });
                return Task.CompletedTask;
            }

            context.Respond(new ProtoSpec.SpeciationAssignResponse
            {
                Decision = completed.Result
            });
            return Task.CompletedTask;
        });
    }

    private void HandleProtoBatchEvaluateApply(IContext context, ProtoSpec.SpeciationBatchEvaluateApplyRequest message)
    {
        var applyMode = NormalizeApplyMode(message.ApplyMode);
        if (!TryGetCurrentEpoch(out var epoch))
        {
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

        var batchTask = ProcessProtoBatchAsync(epoch, applyMode, message.Items);
        context.ReenterAfter(batchTask, completed =>
        {
            if (completed.IsFaulted)
            {
                LogError($"Speciation proto batch evaluate/apply failed: {completed.Exception?.GetBaseException().Message}");
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
        var listTask = _store.ListMembershipsAsync(epochId);
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

            IEnumerable<SpeciationMembershipRecord> history = completed.Result;
            if (message.HasBrainId && message.BrainId is not null && message.BrainId.TryToGuid(out var filterBrainId) && filterBrainId != Guid.Empty)
            {
                history = history.Where(record => record.BrainId == filterBrainId);
            }

            var ordered = history
                .OrderBy(record => record.EpochId)
                .ThenBy(record => record.AssignedMs)
                .ThenBy(record => record.BrainId)
                .ToList();
            var total = ordered.Count;
            if (message.Limit > 0 && ordered.Count > message.Limit)
            {
                ordered = ordered.Take((int)message.Limit).ToList();
            }

            var response = new ProtoSpec.SpeciationListHistoryResponse
            {
                FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
                FailureDetail = string.Empty,
                TotalRecords = (uint)total
            };
            response.History.AddRange(ordered.Select(ToProtoMembershipRecord));
            context.Respond(response);
            return Task.CompletedTask;
        });
    }

    private async Task<ProtoSpec.SpeciationBatchEvaluateApplyResponse> ProcessProtoBatchAsync(
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

            var decision = await ProcessProtoDecisionAsync(
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
        var assignmentResolution = ResolveAssignment(
            epoch,
            speciesId,
            speciesDisplayName,
            lineageEvidence,
            similarityEvidence);

        var resolvedPolicyVersion = NormalizeOrFallback(policyVersion, _runtimeConfig.PolicyVersion);
        var resolvedDecisionReason = assignmentResolution.ForceDecisionReason
            ? assignmentResolution.DecisionReason
            : NormalizeOrFallback(decisionReason, assignmentResolution.DecisionReason);
        var resolvedDecisionMetadata = BuildDecisionMetadataJson(
            decisionMetadataJson,
            resolvedPolicyVersion,
            resolved.CandidateMode,
            assignmentResolution,
            lineageEvidence,
            similarityEvidence);
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
        var outcome = await _store.TryAssignMembershipAsync(
            epoch.EpochId,
            assignment,
            decisionTimeMs,
            cancellationToken: default,
            lineageParentBrainIds: orderedParentBrainIds,
            lineageMetadataJson: resolvedDecisionMetadata).ConfigureAwait(false);

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
                        SourceArtifactRef: null);
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
                        sourceArtifactRef);
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
                        normalizedUri);
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
        var sha = artifactRef.TryToSha256Hex(out var sha256Hex)
            ? sha256Hex
            : string.Empty;
        var storeUri = string.IsNullOrWhiteSpace(artifactRef.StoreUri)
            ? string.Empty
            : artifactRef.StoreUri.Trim();
        var mediaType = string.IsNullOrWhiteSpace(artifactRef.MediaType)
            ? string.Empty
            : artifactRef.MediaType.Trim();
        return $"artifact_ref|sha256={sha}|size={artifactRef.SizeBytes}|media_type={mediaType}|store_uri={storeUri}";
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
        if (parents is null)
        {
            return Array.Empty<Guid>();
        }

        return parents
            .Where(parent => parent is not null && parent.ParentCase == ProtoSpec.SpeciationParentRef.ParentOneofCase.BrainId)
            .Select(parent => parent.BrainId)
            .Where(brainId => brainId is not null && brainId.TryToGuid(out _))
            .Select(brainId => brainId!.ToGuid())
            .Where(brainId => brainId != Guid.Empty)
            .Distinct()
            .OrderBy(brainId => brainId)
            .ToArray();
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
            hysteresisMembership?.SpeciesDisplayName);
    }

    private AssignmentResolution ResolveAssignment(
        SpeciationEpochInfo epoch,
        string? requestedSpeciesId,
        string? requestedSpeciesDisplayName,
        LineageEvidence lineageEvidence,
        SimilarityEvidence similarityEvidence)
    {
        if (!string.IsNullOrWhiteSpace(requestedSpeciesId))
        {
            var speciesId = requestedSpeciesId.Trim();
            var speciesDisplayName = NormalizeOrFallback(requestedSpeciesDisplayName, speciesId);
            return new AssignmentResolution(
                speciesId,
                speciesDisplayName,
                DecisionReason: "explicit_species",
                Strategy: "explicit_species",
                StrategyDetail: "Request provided explicit species_id.",
                ForceDecisionReason: false);
        }

        if (lineageEvidence.ParentMembershipCount == 0
            || string.IsNullOrWhiteSpace(lineageEvidence.DominantSpeciesId))
        {
            return new AssignmentResolution(
                _runtimeConfig.DefaultSpeciesId,
                _runtimeConfig.DefaultSpeciesDisplayName,
                DecisionReason: "lineage_unavailable_default",
                Strategy: "default",
                StrategyDetail: "No parent membership evidence available in current epoch.",
                ForceDecisionReason: true);
        }

        var dominantSpeciesId = lineageEvidence.DominantSpeciesId!;
        var dominantSpeciesDisplayName = string.IsNullOrWhiteSpace(lineageEvidence.DominantSpeciesDisplayName)
            ? dominantSpeciesId
            : lineageEvidence.DominantSpeciesDisplayName!;

        if (lineageEvidence.DominantShare < _assignmentPolicy.ParentConsensusThreshold)
        {
            if (!string.IsNullOrWhiteSpace(lineageEvidence.HysteresisSpeciesId))
            {
                return new AssignmentResolution(
                    lineageEvidence.HysteresisSpeciesId!,
                    NormalizeOrFallback(lineageEvidence.HysteresisSpeciesDisplayName, lineageEvidence.HysteresisSpeciesId!),
                    DecisionReason: "lineage_hysteresis_low_consensus",
                    Strategy: "hysteresis_hold",
                    StrategyDetail: "Parent consensus below threshold; reused prior lineage species.",
                    ForceDecisionReason: true);
            }

            return new AssignmentResolution(
                _runtimeConfig.DefaultSpeciesId,
                _runtimeConfig.DefaultSpeciesDisplayName,
                DecisionReason: "lineage_low_consensus_default",
                Strategy: "default",
                StrategyDetail: "Parent consensus below configured threshold.",
                ForceDecisionReason: true);
        }

        if (!similarityEvidence.SimilarityScore.HasValue)
        {
            return new AssignmentResolution(
                dominantSpeciesId,
                dominantSpeciesDisplayName,
                DecisionReason: "lineage_inherit_no_similarity",
                Strategy: "lineage_inherit",
                StrategyDetail: "No similarity score supplied; inherited dominant parent species.",
                ForceDecisionReason: true);
        }

        var similarity = ClampScore(similarityEvidence.SimilarityScore.Value);
        if (similarity >= _assignmentPolicy.LineageMatchThreshold)
        {
            return new AssignmentResolution(
                dominantSpeciesId,
                dominantSpeciesDisplayName,
                DecisionReason: "lineage_inherit_similarity_match",
                Strategy: "lineage_inherit",
                StrategyDetail: "Similarity met lineage match threshold.",
                ForceDecisionReason: true);
        }

        if (similarity <= _assignmentPolicy.LineageSplitThreshold)
        {
            if (_assignmentPolicy.CreateDerivedSpeciesOnDivergence)
            {
                var derivedSpeciesId = BuildDerivedSpeciesId(
                    dominantSpeciesId,
                    _assignmentPolicy.DerivedSpeciesPrefix,
                    lineageEvidence.LineageKey,
                    epoch.EpochId);
                return new AssignmentResolution(
                    derivedSpeciesId,
                    $"Derived {dominantSpeciesDisplayName}",
                    DecisionReason: "lineage_diverged_new_species",
                    Strategy: "lineage_diverged",
                    StrategyDetail: "Similarity below split threshold; created deterministic derived species.",
                    ForceDecisionReason: true);
            }

            return new AssignmentResolution(
                _runtimeConfig.DefaultSpeciesId,
                _runtimeConfig.DefaultSpeciesDisplayName,
                DecisionReason: "lineage_diverged_default",
                Strategy: "default",
                StrategyDetail: "Similarity below split threshold with derived species disabled.",
                ForceDecisionReason: true);
        }

        if (!string.IsNullOrWhiteSpace(lineageEvidence.HysteresisSpeciesId))
        {
            return new AssignmentResolution(
                lineageEvidence.HysteresisSpeciesId!,
                NormalizeOrFallback(lineageEvidence.HysteresisSpeciesDisplayName, lineageEvidence.HysteresisSpeciesId!),
                DecisionReason: "lineage_hysteresis_hold",
                Strategy: "hysteresis_hold",
                StrategyDetail: "Similarity in hysteresis band; reused prior lineage species.",
                ForceDecisionReason: true);
        }

        return new AssignmentResolution(
            dominantSpeciesId,
            dominantSpeciesDisplayName,
            DecisionReason: "lineage_hysteresis_seed",
            Strategy: "lineage_inherit",
            StrategyDetail: "Similarity in hysteresis band without prior lineage hint; seeded with dominant species.",
            ForceDecisionReason: true);
    }

    private string BuildDecisionMetadataJson(
        string? sourceMetadataJson,
        string policyVersion,
        ProtoSpec.SpeciationCandidateMode candidateMode,
        AssignmentResolution assignmentResolution,
        LineageEvidence lineageEvidence,
        SimilarityEvidence similarityEvidence)
    {
        var metadata = ParseMetadataJson(sourceMetadataJson);
        metadata["assignment_strategy"] = assignmentResolution.Strategy;
        metadata["assignment_strategy_detail"] = assignmentResolution.StrategyDetail;
        metadata["policy_version"] = policyVersion;

        metadata["assignment_policy"] = new JsonObject
        {
            ["lineage_match_threshold"] = _assignmentPolicy.LineageMatchThreshold,
            ["lineage_split_threshold"] = _assignmentPolicy.LineageSplitThreshold,
            ["parent_consensus_threshold"] = _assignmentPolicy.ParentConsensusThreshold,
            ["hysteresis_margin"] = _assignmentPolicy.HysteresisMargin,
            ["create_derived_species_on_divergence"] = _assignmentPolicy.CreateDerivedSpeciesOnDivergence,
            ["derived_species_prefix"] = _assignmentPolicy.DerivedSpeciesPrefix
        };

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

        metadata["lineage"] = new JsonObject
        {
            ["candidate_mode"] = candidateMode.ToString(),
            ["lineage_key"] = lineageEvidence.LineageKey,
            ["parent_membership_count"] = lineageEvidence.ParentMembershipCount,
            ["parent_brain_ids"] = parentBrainIds,
            ["parent_artifact_refs"] = parentArtifactRefs,
            ["dominant_species_id"] = lineageEvidence.DominantSpeciesId ?? string.Empty,
            ["dominant_species_display_name"] = lineageEvidence.DominantSpeciesDisplayName ?? string.Empty,
            ["dominant_species_share"] = lineageEvidence.DominantShare,
            ["hysteresis_species_id"] = lineageEvidence.HysteresisSpeciesId ?? string.Empty,
            ["hysteresis_species_display_name"] = lineageEvidence.HysteresisSpeciesDisplayName ?? string.Empty
        };

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

    private static SimilarityEvidence ExtractSimilarityEvidence(string? decisionMetadataJson)
    {
        if (string.IsNullOrWhiteSpace(decisionMetadataJson))
        {
            return new SimilarityEvidence(null, null, null, null);
        }

        JsonNode? node;
        try
        {
            node = JsonNode.Parse(decisionMetadataJson);
        }
        catch (JsonException)
        {
            return new SimilarityEvidence(null, null, null, null);
        }

        var similarityScore = FindNumericValue(
            node,
            "similarity_score",
            "similarityScore");
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
        return new SimilarityEvidence(similarityScore, functionScore, connectivityScore, regionSpanScore);
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
            defaultValue: 0.70d,
            "lineage_match_threshold",
            "lineageMatchThreshold"));
        var splitThreshold = ReadOptionalPolicyDouble(
            policyNode,
            "lineage_split_threshold",
            "lineageSplitThreshold");
        var hysteresisMargin = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.10d,
            "lineage_hysteresis_margin",
            "lineageHysteresisMargin"));
        var resolvedSplitThreshold = ClampScore(splitThreshold ?? (matchThreshold - hysteresisMargin));
        if (resolvedSplitThreshold > matchThreshold)
        {
            (resolvedSplitThreshold, matchThreshold) = (matchThreshold, resolvedSplitThreshold);
        }

        var parentConsensusThreshold = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.50d,
            "parent_consensus_threshold",
            "parentConsensusThreshold"));
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
            HysteresisMargin: Math.Max(0d, matchThreshold - resolvedSplitThreshold),
            CreateDerivedSpeciesOnDivergence: createDerivedSpecies,
            DerivedSpeciesPrefix: derivedSpeciesPrefix);
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
        var speciesId = $"{normalizedDominant}-{normalizedPrefix}-{suffix}";
        return speciesId.Length <= 96
            ? speciesId
            : speciesId[..96];
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
        bool CreateDerivedSpeciesOnDivergence,
        string DerivedSpeciesPrefix);

    private readonly record struct SimilarityEvidence(
        double? SimilarityScore,
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
        string? HysteresisSpeciesDisplayName);

    private readonly record struct AssignmentResolution(
        string SpeciesId,
        string SpeciesDisplayName,
        string DecisionReason,
        string Strategy,
        string StrategyDetail,
        bool ForceDecisionReason);

    private readonly record struct ResolvedCandidate(
        ProtoSpec.SpeciationCandidateMode CandidateMode,
        Guid BrainId,
        string? SourceArtifactRef);

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

