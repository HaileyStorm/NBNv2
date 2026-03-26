using System.Globalization;
using Nbn.Shared;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoSettings = Nbn.Proto.Settings;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
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
