using Nbn.Proto.Settings;
using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Runtime.Speciation;

public sealed class SpeciationManagerActor : IActor
{
    private static readonly TimeSpan DefaultSettingsRequestTimeout = TimeSpan.FromSeconds(5);

    private readonly SpeciationStore _store;
    private SpeciationRuntimeConfig _runtimeConfig;
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
