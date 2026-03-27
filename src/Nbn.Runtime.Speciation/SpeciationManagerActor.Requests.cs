using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Shared;
using Proto;
using ProtoSpec = Nbn.Proto.Speciation;
using System.Diagnostics;

namespace Nbn.Runtime.Speciation;

public sealed partial class SpeciationManagerActor
{
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
}
