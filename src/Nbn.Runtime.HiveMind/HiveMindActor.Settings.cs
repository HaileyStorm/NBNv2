using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private bool HandleSettingsMessage(IContext context)
    {
        switch (context.Message)
        {
            case ProtoSettings.SettingValue message:
                HandleSettingValue(context, message);
                return true;
            case ProtoSettings.SettingChanged message:
                HandleSettingChanged(context, message);
                return true;
            case ProtoControl.GetHiveMindStatus:
                context.Respond(BuildStatus());
                return true;
            case ProtoControl.SetTickRateOverride message:
                HandleSetTickRateOverride(context, message);
                return true;
            default:
                return false;
        }
    }

    private void EnsureDebugSettingsSubscription(IContext context)
    {
        if (_settingsPid is null || _debugSettingsSubscribed)
        {
            return;
        }

        context.Send(_settingsPid, new ProtoSettings.SettingSubscribe
        {
            SubscriberActor = PidLabel(context.Self)
        });
        _debugSettingsSubscribed = true;
    }

    private void RefreshDebugSettings(IContext context)
    {
        if (_settingsPid is null)
        {
            return;
        }

        RequestSetting(context, _settingsPid, DebugSettingsKeys.EnabledKey);
        RequestSetting(context, _settingsPid, DebugSettingsKeys.MinSeverityKey);
        RequestSettings(context, _settingsPid, CostEnergySettingsKeys.AllKeys);
        RequestSetting(context, _settingsPid, PlasticitySettingsKeys.SystemEnabledKey);
        RequestSettings(context, _settingsPid, IoCoordinatorSettingsKeys.AllKeys);
        RequestSettings(context, _settingsPid, TickSettingsKeys.AllKeys);
        RequestSettings(context, _settingsPid, VisualizationSettingsKeys.AllKeys);
        RequestSettings(context, _settingsPid, WorkerCapabilitySettingsKeys.AllKeys);
        RequestSetting(context, _settingsPid, ServiceEndpointSettings.IoGatewayKey);
        RequestSetting(context, _settingsPid, ServiceEndpointSettings.WorkerNodeKey);
    }

    private void HandleSettingValue(IContext context, ProtoSettings.SettingValue message)
        => ApplySettingChange(context, message.Key, message.Value);

    private void HandleSettingChanged(IContext context, ProtoSettings.SettingChanged message)
        => ApplySettingChange(context, message.Key, message.Value);

    private static void RequestSetting(IContext context, PID settingsPid, string key)
        => context.Request(settingsPid, new ProtoSettings.SettingGet { Key = key });

    private static void RequestSettings(IContext context, PID settingsPid, IEnumerable<string> keys)
    {
        foreach (var key in keys)
        {
            RequestSetting(context, settingsPid, key);
        }
    }

    private void ApplySettingChange(IContext context, string? key, string? value)
    {
        if (TryApplyDebugSetting(key, value))
        {
            UpdateAllShardRuntimeConfig(context);
        }

        if (TryApplyIoEndpointSetting(key, value))
        {
            RegisterAllBrainsWithIo(context);
        }

        TryApplyWorkerEndpointSetting(key, value);

        if (TryApplySystemCostEnergySetting(key, value))
        {
            UpdateAllShardRuntimeConfig(context);
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplySystemPlasticitySetting(key, value))
        {
            UpdateAllShardRuntimeConfig(context);
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplyInputCoordinatorModeSetting(key, value))
        {
            RegisterAllBrainsWithIo(context);
        }

        if (TryApplyOutputVectorSourceSetting(key, value))
        {
            UpdateAllShardRuntimeConfig(context);
            RegisterAllBrainsWithIo(context);
        }

        TryApplyTickRateOverrideSetting(context, key, value);
        TryApplyVisualizationTickMinIntervalSetting(key, value);

        if (TryApplyVisualizationStreamMinIntervalSetting(key, value))
        {
            UpdateAllShardVisualizationConfig(context);
        }

        TryApplyWorkerCapabilitySetting(context, key, value);
    }
}
