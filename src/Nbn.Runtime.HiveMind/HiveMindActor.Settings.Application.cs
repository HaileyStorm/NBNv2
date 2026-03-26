using Nbn.Shared;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
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
}
