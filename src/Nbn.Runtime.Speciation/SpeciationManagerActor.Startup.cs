using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Shared;
using Proto;
using ProtoRepro = Nbn.Proto.Repro;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Runtime.Speciation;

public sealed partial class SpeciationManagerActor
{
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
}
