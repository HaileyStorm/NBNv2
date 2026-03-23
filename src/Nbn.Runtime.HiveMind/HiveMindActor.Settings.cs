using System.Globalization;
using Nbn.Shared;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoSettings = Nbn.Proto.Settings;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private void EnsureDebugSettingsSubscription(IContext context)
    {
        if (_settingsPid is null || _debugSettingsSubscribed)
        {
            return;
        }

        context.Send(_settingsPid, new ProtoSettings.SettingSubscribe
        {
            SubscriberActor = BuildLocalActorReference(context.Self)
        });
        _debugSettingsSubscribed = true;
    }

    private void RefreshDebugSettings(IContext context)
    {
        if (_settingsPid is null)
        {
            return;
        }

        context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = DebugSettingsKeys.EnabledKey });
        context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = DebugSettingsKeys.MinSeverityKey });
        foreach (var key in CostEnergySettingsKeys.AllKeys)
        {
            context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = key });
        }
        context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = PlasticitySettingsKeys.SystemEnabledKey });
        foreach (var key in IoCoordinatorSettingsKeys.AllKeys)
        {
            context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = key });
        }
        foreach (var key in TickSettingsKeys.AllKeys)
        {
            context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = key });
        }
        foreach (var key in VisualizationSettingsKeys.AllKeys)
        {
            context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = key });
        }
        foreach (var key in WorkerCapabilitySettingsKeys.AllKeys)
        {
            context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = key });
        }

        context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = ServiceEndpointSettings.IoGatewayKey });
        context.Request(_settingsPid, new ProtoSettings.SettingGet { Key = ServiceEndpointSettings.WorkerNodeKey });
    }

    private void HandleSettingValue(IContext context, ProtoSettings.SettingValue message)
    {
        if (message is null)
        {
            return;
        }

        if (TryApplyDebugSetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
        }

        if (TryApplyIoEndpointSetting(message.Key, message.Value))
        {
            RegisterAllBrainsWithIo(context);
        }

        TryApplyWorkerEndpointSetting(message.Key, message.Value);
        TryApplyNodeEndpointSetSetting(message.Key, message.Value);

        if (TryApplySystemCostEnergySetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplySystemPlasticitySetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplyInputCoordinatorModeSetting(message.Key, message.Value))
        {
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplyOutputVectorSourceSetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplyTickRateOverrideSetting(context, message.Key, message.Value))
        {
        }

        if (TryApplyVisualizationTickMinIntervalSetting(message.Key, message.Value))
        {
        }

        if (TryApplyVisualizationStreamMinIntervalSetting(message.Key, message.Value))
        {
            UpdateAllShardVisualizationConfig(context);
        }

        TryApplyWorkerCapabilitySetting(context, message.Key, message.Value);
    }

    private void HandleSettingChanged(IContext context, ProtoSettings.SettingChanged message)
    {
        if (message is null)
        {
            return;
        }

        if (TryApplyDebugSetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
        }

        if (TryApplyIoEndpointSetting(message.Key, message.Value))
        {
            RegisterAllBrainsWithIo(context);
        }

        TryApplyWorkerEndpointSetting(message.Key, message.Value);
        TryApplyNodeEndpointSetSetting(message.Key, message.Value);

        if (TryApplySystemCostEnergySetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplySystemPlasticitySetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplyInputCoordinatorModeSetting(message.Key, message.Value))
        {
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplyOutputVectorSourceSetting(message.Key, message.Value))
        {
            UpdateAllShardRuntimeConfig(context);
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplyTickRateOverrideSetting(context, message.Key, message.Value))
        {
        }

        if (TryApplyVisualizationTickMinIntervalSetting(message.Key, message.Value))
        {
        }

        if (TryApplyVisualizationStreamMinIntervalSetting(message.Key, message.Value))
        {
            UpdateAllShardVisualizationConfig(context);
        }

        TryApplyWorkerCapabilitySetting(context, message.Key, message.Value);
    }

    private bool TryApplyIoEndpointSetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, ServiceEndpointSettings.IoGatewayKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var nextPid = _configuredIoPid;
        if (ServiceEndpointSettings.TryParseValue(value, out var endpoint))
        {
            nextPid = endpoint.ToPid();
        }

        if (SamePid(_ioPid, nextPid))
        {
            return false;
        }

        _ioPid = nextPid;
        return true;
    }

    private bool TryApplyWorkerEndpointSetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, ServiceEndpointSettings.WorkerNodeKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var nextRootActorName = string.Empty;
        if (ServiceEndpointSettings.TryParseValue(value, out var endpoint))
        {
            nextRootActorName = endpoint.ActorName.Trim();
        }

        if (string.Equals(
                _configuredWorkerRootActorName,
                nextRootActorName,
                StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        _configuredWorkerRootActorName = nextRootActorName;
        return true;
    }

    private bool TryApplyNodeEndpointSetSetting(string? key, string? value)
    {
        if (!NodeEndpointSetSettings.TryParseKey(key, out var nodeId))
        {
            return false;
        }

        if (ServiceEndpointSettings.TryParseSetValue(value, out var endpointSet))
        {
            _nodeEndpointSets[nodeId] = endpointSet;
        }
        else
        {
            _nodeEndpointSets.Remove(nodeId);
        }

        if (!_workerCatalog.TryGetValue(nodeId, out var worker))
        {
            return true;
        }

        worker.WorkerActorReference = BuildWorkerActorReference(nodeId, worker.WorkerAddress, worker.WorkerRootActorName);
        return true;
    }

    private string BuildLocalActorReference(PID pid)
    {
        if (_localEndpointCandidates is not null && _localEndpointCandidates.Count > 0)
        {
            return RoutablePidReference.Encode(pid, _localEndpointCandidates);
        }

        return PidLabel(pid);
    }

    private static bool SamePid(PID? left, PID? right)
    {
        if (left is null && right is null)
        {
            return true;
        }

        if (left is null || right is null)
        {
            return false;
        }

        return string.Equals(left.Address, right.Address, StringComparison.Ordinal)
               && string.Equals(left.Id, right.Id, StringComparison.Ordinal);
    }

    private bool TryApplyDebugSetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        if (string.Equals(key, DebugSettingsKeys.EnabledKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseDebugEnabledSetting(value, _debugStreamEnabled);
            if (parsed == _debugStreamEnabled)
            {
                return false;
            }

            _debugStreamEnabled = parsed;
            return true;
        }

        if (string.Equals(key, DebugSettingsKeys.MinSeverityKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseDebugSeveritySetting(value, _debugMinSeverity);
            if (parsed == _debugMinSeverity)
            {
                return false;
            }

            _debugMinSeverity = parsed;
            return true;
        }

        return false;
    }

    private bool TryApplySystemCostEnergySetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        if (string.Equals(key, CostEnergySettingsKeys.SystemEnabledKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseBooleanSetting(value, _systemCostEnergyEnabled);
            if (parsed == _systemCostEnergyEnabled)
            {
                return false;
            }

            _systemCostEnergyEnabled = parsed;
            return true;
        }

        if (string.Equals(key, CostEnergySettingsKeys.RemoteCostEnabledKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseBooleanSetting(value, _remoteCostEnabled);
            if (parsed == _remoteCostEnabled)
            {
                return false;
            }

            _remoteCostEnabled = parsed;
            return true;
        }

        if (string.Equals(key, CostEnergySettingsKeys.RemoteCostPerBatchKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseNonNegativeInt64Setting(value, _remoteCostPerBatch);
            if (parsed == _remoteCostPerBatch)
            {
                return false;
            }

            _remoteCostPerBatch = parsed;
            return true;
        }

        if (string.Equals(key, CostEnergySettingsKeys.RemoteCostPerContributionKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseNonNegativeInt64Setting(value, _remoteCostPerContribution);
            if (parsed == _remoteCostPerContribution)
            {
                return false;
            }

            _remoteCostPerContribution = parsed;
            return true;
        }

        if (string.Equals(key, CostEnergySettingsKeys.TierAMultiplierKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParsePositiveFiniteFloatSetting(value, _costTierAMultiplier);
            if (Math.Abs(parsed - _costTierAMultiplier) < 0.000001f)
            {
                return false;
            }

            _costTierAMultiplier = parsed;
            return true;
        }

        if (string.Equals(key, CostEnergySettingsKeys.TierBMultiplierKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParsePositiveFiniteFloatSetting(value, _costTierBMultiplier);
            if (Math.Abs(parsed - _costTierBMultiplier) < 0.000001f)
            {
                return false;
            }

            _costTierBMultiplier = parsed;
            return true;
        }

        if (string.Equals(key, CostEnergySettingsKeys.TierCMultiplierKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParsePositiveFiniteFloatSetting(value, _costTierCMultiplier);
            if (Math.Abs(parsed - _costTierCMultiplier) < 0.000001f)
            {
                return false;
            }

            _costTierCMultiplier = parsed;
            return true;
        }

        return false;
    }

    private bool TryApplySystemPlasticitySetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, PlasticitySettingsKeys.SystemEnabledKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var parsed = ParseBooleanSetting(value, _systemPlasticityEnabled);
        if (parsed == _systemPlasticityEnabled)
        {
            return false;
        }

        _systemPlasticityEnabled = parsed;
        return true;
    }

    private bool TryApplyInputCoordinatorModeSetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, IoCoordinatorSettingsKeys.InputCoordinatorModeKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var parsed = ParseInputCoordinatorModeSetting(value, _inputCoordinatorMode);
        if (parsed == _inputCoordinatorMode)
        {
            return false;
        }

        _inputCoordinatorMode = parsed;
        return true;
    }

    private bool TryApplyOutputVectorSourceSetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, IoCoordinatorSettingsKeys.OutputVectorSourceKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var parsed = ParseOutputVectorSourceSetting(value, _outputVectorSource);
        if (parsed == _outputVectorSource)
        {
            return false;
        }

        _outputVectorSource = parsed;
        return true;
    }

    private bool TryApplyTickRateOverrideSetting(IContext context, string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, TickSettingsKeys.CadenceHzKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (!TryParseTickRateOverrideSetting(value, out var requestedOverride))
        {
            EmitDebug(
                context,
                ProtoSeverity.SevWarn,
                "tick.override.setting.invalid",
                $"Ignoring invalid tick cadence setting '{TickSettingsKeys.CadenceHzKey}'='{value ?? string.Empty}'.");
            return false;
        }

        if (HasEquivalentTickRateOverride(requestedOverride))
        {
            return false;
        }

        var accepted = _backpressure.TrySetTickRateOverride(requestedOverride, out var summary);
        if (accepted)
        {
            EmitDebug(context, ProtoSeverity.SevInfo, "tick.override.setting", summary);
            return true;
        }

        EmitDebug(context, ProtoSeverity.SevWarn, "tick.override.setting.invalid", summary);
        return false;
    }

    private bool HasEquivalentTickRateOverride(float? requestedOverride)
    {
        if (!requestedOverride.HasValue)
        {
            return !_backpressure.HasTickRateOverride;
        }

        if (!_backpressure.HasTickRateOverride)
        {
            return false;
        }

        return MathF.Abs(requestedOverride.Value - _backpressure.TickRateOverrideHz) <= 1e-3f;
    }

    private bool TryApplyVisualizationTickMinIntervalSetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, VisualizationSettingsKeys.TickMinIntervalMsKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var parsed = ParseVisualizationMinIntervalSetting(value, _vizTickMinIntervalMs);
        if (parsed == _vizTickMinIntervalMs)
        {
            return false;
        }

        _vizTickMinIntervalMs = parsed;
        return true;
    }

    private bool TryApplyVisualizationStreamMinIntervalSetting(string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key)
            || !string.Equals(key, VisualizationSettingsKeys.StreamMinIntervalMsKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var parsed = ParseVisualizationMinIntervalSetting(value, _vizStreamMinIntervalMs);
        if (parsed == _vizStreamMinIntervalMs)
        {
            return false;
        }

        _vizStreamMinIntervalMs = parsed;
        return true;
    }

    private bool TryApplyWorkerCapabilitySetting(IContext context, string? key, string? value)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        if (string.Equals(key, WorkerCapabilitySettingsKeys.BenchmarkRefreshSecondsKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseWorkerCapabilityRefreshSeconds(value, _workerCapabilityBenchmarkRefreshSeconds);
            if (parsed == _workerCapabilityBenchmarkRefreshSeconds)
            {
                return false;
            }

            _workerCapabilityBenchmarkRefreshSeconds = parsed;
            ScheduleSelf(context, TimeSpan.Zero, new RefreshWorkerCapabilitiesTick());
            return true;
        }

        if (string.Equals(key, WorkerCapabilitySettingsKeys.PressureRebalanceWindowKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseWorkerPressureWindow(value, _workerPressureRebalanceWindow);
            if (parsed == _workerPressureRebalanceWindow)
            {
                return false;
            }

            _workerPressureRebalanceWindow = parsed;
            return true;
        }

        if (string.Equals(key, WorkerCapabilitySettingsKeys.PressureViolationRatioKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseWorkerPressureRatio(value, _workerPressureViolationRatio);
            if (Math.Abs(parsed - _workerPressureViolationRatio) <= 0.0001d)
            {
                return false;
            }

            _workerPressureViolationRatio = parsed;
            return true;
        }

        if (string.Equals(key, WorkerCapabilitySettingsKeys.PressureLimitTolerancePercentKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseWorkerPressureTolerance(value, _workerPressureLimitTolerancePercent);
            if (Math.Abs(parsed - _workerPressureLimitTolerancePercent) <= 0.0001f)
            {
                return false;
            }

            _workerPressureLimitTolerancePercent = parsed;
            return true;
        }

        return false;
    }

    private void UpdateAllShardRuntimeConfig(IContext context)
    {
        foreach (var brain in _brains.Values)
        {
            UpdateShardRuntimeConfig(context, brain);
        }
    }

    private void UpdateAllShardVisualizationConfig(IContext context)
    {
        foreach (var brain in _brains.Values)
        {
            foreach (var entry in brain.Shards)
            {
                SendShardVisualizationUpdate(
                    context,
                    brain.BrainId,
                    entry.Key,
                    entry.Value,
                    brain.VisualizationEnabled,
                    brain.VisualizationFocusRegionId,
                    _vizStreamMinIntervalMs);
            }
        }
    }

    private void RegisterAllBrainsWithIo(IContext context)
    {
        foreach (var brain in _brains.Values)
        {
            RegisterBrainWithIo(context, brain, force: true);
        }
    }

    private static bool ParseDebugEnabledSetting(string? value, bool fallback)
        => ParseBooleanSetting(value, fallback);

    private static ProtoControl.InputCoordinatorMode ParseInputCoordinatorModeSetting(
        string? value,
        ProtoControl.InputCoordinatorMode fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        var normalized = value.Trim().ToLowerInvariant().Replace('-', '_');
        return normalized switch
        {
            "0" or "dirty" or "dirty_on_change" => ProtoControl.InputCoordinatorMode.DirtyOnChange,
            "1" or "replay" or "replay_latest_vector" => ProtoControl.InputCoordinatorMode.ReplayLatestVector,
            _ => fallback
        };
    }

    private static ProtoControl.OutputVectorSource ParseOutputVectorSourceSetting(
        string? value,
        ProtoControl.OutputVectorSource fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        var normalized = value.Trim().ToLowerInvariant().Replace('-', '_');
        return normalized switch
        {
            "0" or "potential" => ProtoControl.OutputVectorSource.Potential,
            "1" or "buffer" => ProtoControl.OutputVectorSource.Buffer,
            _ => fallback
        };
    }

    private static bool TryParseTickRateOverrideSetting(string? value, out float? overrideHz)
    {
        overrideHz = null;
        if (string.IsNullOrWhiteSpace(value))
        {
            return true;
        }

        var trimmed = value.Trim();
        var normalized = trimmed.ToLowerInvariant();
        if (normalized is "0" or "off" or "none" or "clear" or "default")
        {
            return true;
        }

        if (normalized.EndsWith("ms", StringComparison.Ordinal))
        {
            var numeric = trimmed[..^2].Trim();
            if (!float.TryParse(numeric, NumberStyles.Float, CultureInfo.InvariantCulture, out var ms)
                || !float.IsFinite(ms)
                || ms <= 0f)
            {
                return false;
            }

            overrideHz = 1000f / ms;
            return float.IsFinite(overrideHz.Value) && overrideHz.Value > 0f;
        }

        if (normalized.EndsWith("hz", StringComparison.Ordinal))
        {
            trimmed = trimmed[..^2].Trim();
        }

        if (!float.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out var hz)
            || !float.IsFinite(hz)
            || hz <= 0f)
        {
            return false;
        }

        overrideHz = hz;
        return true;
    }

    private static uint ParseVisualizationMinIntervalSetting(string? value, uint fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !uint.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return fallback;
        }

        return Math.Min(parsed, 60_000u);
    }

    private static uint ComputeVizStride(float targetTickHz, uint minIntervalMs)
    {
        if (minIntervalMs == 0u || !float.IsFinite(targetTickHz) || targetTickHz <= 0f)
        {
            return 1u;
        }

        var tickMs = 1000f / targetTickHz;
        if (!float.IsFinite(tickMs) || tickMs <= 0f || tickMs >= minIntervalMs)
        {
            return 1u;
        }

        var stride = (uint)Math.Ceiling(minIntervalMs / tickMs);
        return Math.Max(1u, stride);
    }

    private static bool ParseBooleanSetting(string? value, bool fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "1" or "true" or "yes" or "on" => true,
            "0" or "false" or "no" or "off" => false,
            _ => fallback
        };
    }

    private static long ParseNonNegativeInt64Setting(string? value, long fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !long.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return fallback;
        }

        return Math.Max(0L, parsed);
    }

    private static int ParseWorkerCapabilityRefreshSeconds(string? value, int fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !int.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return fallback;
        }

        return Math.Max(0, parsed);
    }

    private static int ParseWorkerPressureWindow(string? value, int fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !int.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return fallback;
        }

        return Math.Max(1, parsed);
    }

    private static double ParseWorkerPressureRatio(string? value, double fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !double.TryParse(value.Trim(), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsed)
            || !double.IsFinite(parsed))
        {
            return fallback;
        }

        return Math.Clamp(parsed, 0d, 1d);
    }

    private static float ParseWorkerPressureTolerance(string? value, float fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !float.TryParse(value.Trim(), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsed)
            || !float.IsFinite(parsed))
        {
            return fallback;
        }

        return Math.Max(0f, parsed);
    }

    private static float ParsePositiveFiniteFloatSetting(string? value, float fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !float.TryParse(value.Trim(), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsed)
            || !float.IsFinite(parsed)
            || parsed <= 0f)
        {
            return fallback;
        }

        return parsed;
    }

    private static ProtoSeverity ParseDebugSeveritySetting(string? value, ProtoSeverity fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        if (Enum.TryParse<ProtoSeverity>(value, ignoreCase: true, out var direct))
        {
            return NormalizeDebugSeverity(direct);
        }

        var normalized = value.Trim().ToLowerInvariant();
        return normalized switch
        {
            "trace" or "sev_trace" => ProtoSeverity.SevTrace,
            "debug" or "sev_debug" => ProtoSeverity.SevDebug,
            "info" or "sev_info" => ProtoSeverity.SevInfo,
            "warn" or "warning" or "sev_warn" => ProtoSeverity.SevWarn,
            "error" or "sev_error" => ProtoSeverity.SevError,
            "fatal" or "sev_fatal" => ProtoSeverity.SevFatal,
            _ => fallback
        };
    }

    private static ProtoSeverity NormalizeDebugSeverity(ProtoSeverity severity)
    {
        return severity switch
        {
            ProtoSeverity.SevTrace or ProtoSeverity.SevDebug or ProtoSeverity.SevInfo or ProtoSeverity.SevWarn or ProtoSeverity.SevError or ProtoSeverity.SevFatal => severity,
            _ => ProtoSeverity.SevDebug
        };
    }

    private ProtoControl.HiveMindStatus BuildStatus()
        => new()
        {
            LastCompletedTickId = _lastCompletedTickId,
            TickLoopEnabled = _tickLoopEnabled,
            TargetTickHz = _backpressure.TargetTickHz,
            PendingCompute = (uint)_pendingCompute.Count,
            PendingDeliver = (uint)_pendingDeliver.Count,
            RescheduleInProgress = _rescheduleInProgress,
            RegisteredBrains = (uint)_brains.Count,
            RegisteredShards = (uint)_brains.Values.Sum(brain => brain.Shards.Count),
            HasTickRateOverride = _backpressure.HasTickRateOverride,
            TickRateOverrideHz = _backpressure.TickRateOverrideHz
        };

    private void HandleSetTickRateOverride(IContext context, ProtoControl.SetTickRateOverride message)
    {
        float? requestedOverride = message.ClearOverride ? null : message.TargetTickHz;
        var accepted = _backpressure.TrySetTickRateOverride(requestedOverride, out var summary);
        if (accepted)
        {
            EmitDebug(context, ProtoSeverity.SevInfo, "tick.override", summary);
            PersistTickRateOverrideSetting(context);
        }
        else
        {
            EmitDebug(context, ProtoSeverity.SevWarn, "tick.override.invalid", summary);
        }

        context.Respond(new ProtoControl.SetTickRateOverrideAck
        {
            Accepted = accepted,
            Message = summary,
            TargetTickHz = _backpressure.TargetTickHz,
            HasOverride = _backpressure.HasTickRateOverride,
            OverrideTickHz = _backpressure.TickRateOverrideHz
        });
    }

    private void PersistTickRateOverrideSetting(IContext context)
    {
        if (_settingsPid is null)
        {
            return;
        }

        var value = _backpressure.HasTickRateOverride
            ? _backpressure.TickRateOverrideHz.ToString("0.###", CultureInfo.InvariantCulture)
            : string.Empty;

        context.Send(_settingsPid, new ProtoSettings.SettingSet
        {
            Key = TickSettingsKeys.CadenceHzKey,
            Value = value
        });
    }
}
