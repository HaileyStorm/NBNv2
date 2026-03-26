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

        ApplySettingChange(context, message.Key, message.Value);
    }

    private void HandleSettingChanged(IContext context, ProtoSettings.SettingChanged message)
    {
        if (message is null)
        {
            return;
        }

        ApplySettingChange(context, message.Key, message.Value);
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
