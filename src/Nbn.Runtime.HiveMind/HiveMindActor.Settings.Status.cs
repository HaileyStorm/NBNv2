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

    private void HandleSetOutputVectorSource(IContext context, ProtoControl.SetOutputVectorSource message)
    {
        var normalized = NormalizeOutputVectorSource(message.OutputVectorSource);
        if (message.BrainId is not null)
        {
            if (!TryGetGuid(message.BrainId, out var brainId))
            {
                context.Respond(new ProtoControl.SetOutputVectorSourceAck
                {
                    Accepted = false,
                    Message = "invalid_brain_id",
                    OutputVectorSource = normalized,
                    BrainId = message.BrainId.Clone()
                });
                return;
            }

            if (!_brains.TryGetValue(brainId, out var brain))
            {
                context.Respond(new ProtoControl.SetOutputVectorSourceAck
                {
                    Accepted = false,
                    Message = "brain_not_registered",
                    OutputVectorSource = normalized,
                    BrainId = brainId.ToProtoUuid()
                });
                return;
            }

            var changed = brain.OutputVectorSource != normalized || !brain.HasExplicitOutputVectorSource;
            brain.OutputVectorSource = normalized;
            brain.HasExplicitOutputVectorSource = true;
            if (changed)
            {
                UpdateShardRuntimeConfig(context, brain);
                RegisterBrainWithIo(context, brain, force: true);
                EmitDebug(
                    context,
                    ProtoSeverity.SevInfo,
                    "io.output_vector_source.brain",
                    $"Output vector source for brain {brain.BrainId} set to {FormatOutputVectorSource(brain.OutputVectorSource)}.");
            }

            context.Respond(new ProtoControl.SetOutputVectorSourceAck
            {
                Accepted = true,
                Message = changed ? "applied" : "unchanged",
                OutputVectorSource = brain.OutputVectorSource,
                BrainId = brain.BrainId.ToProtoUuid()
            });
            return;
        }

        var defaultChanged = _outputVectorSource != normalized;
        _outputVectorSource = normalized;
        if (defaultChanged)
        {
            ApplyDefaultOutputVectorSourceToUnpinnedBrains(context);
            PersistOutputVectorSourceSetting(context);
            EmitDebug(
                context,
                ProtoSeverity.SevInfo,
                "io.output_vector_source",
                $"Default output vector source set to {FormatOutputVectorSource(_outputVectorSource)}.");
        }

        context.Respond(new ProtoControl.SetOutputVectorSourceAck
        {
            Accepted = true,
            Message = defaultChanged ? "applied" : "unchanged",
            OutputVectorSource = _outputVectorSource
        });
    }

    private void ApplyDefaultOutputVectorSourceToUnpinnedBrains(IContext context)
    {
        foreach (var brain in _brains.Values)
        {
            if (brain.HasExplicitOutputVectorSource || brain.OutputVectorSource == _outputVectorSource)
            {
                continue;
            }

            brain.OutputVectorSource = _outputVectorSource;
            UpdateShardRuntimeConfig(context, brain);
            RegisterBrainWithIo(context, brain, force: true);
        }
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

    private void PersistOutputVectorSourceSetting(IContext context)
    {
        if (_settingsPid is null)
        {
            return;
        }

        context.Send(_settingsPid, new ProtoSettings.SettingSet
        {
            Key = IoCoordinatorSettingsKeys.OutputVectorSourceKey,
            Value = FormatOutputVectorSource(_outputVectorSource)
        });
    }

    private static string FormatOutputVectorSource(ProtoControl.OutputVectorSource source)
    {
        return source switch
        {
            ProtoControl.OutputVectorSource.Buffer => "buffer",
            _ => "potential"
        };
    }

    private static ProtoControl.OutputVectorSource NormalizeOutputVectorSource(ProtoControl.OutputVectorSource source)
    {
        return source switch
        {
            ProtoControl.OutputVectorSource.Buffer => ProtoControl.OutputVectorSource.Buffer,
            _ => ProtoControl.OutputVectorSource.Potential
        };
    }
}
