using System;
using System.Collections.Generic;
using System.Linq;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Proto.Viz;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Proto;

namespace Nbn.Tools.Workbench.Services;

public sealed class WorkbenchReceiverActor : IActor
{
    private static readonly TimeSpan CommandAckTimeout = TimeSpan.FromSeconds(10);
    private readonly IWorkbenchEventSink _sink;
    private PID? _ioGateway;

    public WorkbenchReceiverActor(IWorkbenchEventSink sink)
    {
        _sink = sink;
    }

    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case SetIoGatewayPid setIo:
                _ioGateway = setIo.Pid;
                return Task.CompletedTask;
            case SubscribeOutputsCommand subscribe:
                SendToIoRequest(context, new SubscribeOutputs
                {
                    BrainId = subscribe.BrainId.ToProtoUuid(),
                    SubscriberActor = PidLabel(context.Self, context.System.Address)
                });
                return Task.CompletedTask;
            case UnsubscribeOutputsCommand unsubscribe:
                SendToIoRequest(context, new UnsubscribeOutputs
                {
                    BrainId = unsubscribe.BrainId.ToProtoUuid(),
                    SubscriberActor = PidLabel(context.Self, context.System.Address)
                });
                return Task.CompletedTask;
            case SubscribeOutputsVectorCommand subscribeVector:
                SendToIoRequest(context, new SubscribeOutputsVector
                {
                    BrainId = subscribeVector.BrainId.ToProtoUuid(),
                    SubscriberActor = PidLabel(context.Self, context.System.Address)
                });
                return Task.CompletedTask;
            case UnsubscribeOutputsVectorCommand unsubscribeVector:
                SendToIoRequest(context, new UnsubscribeOutputsVector
                {
                    BrainId = unsubscribeVector.BrainId.ToProtoUuid(),
                    SubscriberActor = PidLabel(context.Self, context.System.Address)
                });
                return Task.CompletedTask;
            case InputWriteCommand input:
                SendToIo(context, new InputWrite
                {
                    BrainId = input.BrainId.ToProtoUuid(),
                    InputIndex = input.InputIndex,
                    Value = input.Value
                });
                return Task.CompletedTask;
            case InputVectorCommand vector:
                var message = new InputVector { BrainId = vector.BrainId.ToProtoUuid() };
                message.Values.Add(vector.Values);
                SendToIo(context, message);
                return Task.CompletedTask;
            case RuntimeNeuronPulseCommand pulse:
                SendToIo(context, new RuntimeNeuronPulse
                {
                    BrainId = pulse.BrainId.ToProtoUuid(),
                    TargetRegionId = pulse.TargetRegionId,
                    TargetNeuronId = pulse.TargetNeuronId,
                    Value = pulse.Value
                });
                return Task.CompletedTask;
            case RuntimeNeuronStateWriteCommand stateWrite:
                SendToIo(context, new RuntimeNeuronStateWrite
                {
                    BrainId = stateWrite.BrainId.ToProtoUuid(),
                    TargetRegionId = stateWrite.TargetRegionId,
                    TargetNeuronId = stateWrite.TargetNeuronId,
                    SetBuffer = stateWrite.SetBuffer,
                    BufferValue = stateWrite.BufferValue,
                    SetAccumulator = stateWrite.SetAccumulator,
                    AccumulatorValue = stateWrite.AccumulatorValue
                });
                return Task.CompletedTask;
            case EnergyCreditCommand credit:
                return ForwardEnergyCreditAsync(context, credit);
            case EnergyRateCommand rate:
                return ForwardEnergyRateAsync(context, rate);
            case SetCostEnergyCommand flags:
                return ForwardSetCostEnergyAsync(context, flags);
            case SetPlasticityCommand plasticity:
                return ForwardSetPlasticityAsync(context, plasticity);
            case OutputEvent output:
                HandleOutput(output);
                return Task.CompletedTask;
            case OutputVectorEvent outputVector:
                HandleVector(outputVector);
                return Task.CompletedTask;
            case DebugInbound debug:
                HandleDebug(debug);
                return Task.CompletedTask;
            case VisualizationEvent viz:
                HandleViz(viz);
                return Task.CompletedTask;
            case BrainTerminated terminated:
                HandleTermination(terminated);
                return Task.CompletedTask;
            case SettingChanged settingChanged:
                HandleSettingChanged(settingChanged);
                return Task.CompletedTask;
        }

        return Task.CompletedTask;
    }

    private async Task ForwardEnergyCreditAsync(IContext context, EnergyCreditCommand credit)
    {
        var message = new EnergyCredit
        {
            BrainId = credit.BrainId.ToProtoUuid(),
            Amount = credit.Amount
        };

        var result = await SendCommandAsync(context, message, credit.BrainId, "energy_credit").ConfigureAwait(false);
        if (context.Sender is not null)
        {
            context.Respond(result);
        }
    }

    private async Task ForwardEnergyRateAsync(IContext context, EnergyRateCommand rate)
    {
        var message = new EnergyRate
        {
            BrainId = rate.BrainId.ToProtoUuid(),
            UnitsPerSecond = rate.UnitsPerSecond
        };

        var result = await SendCommandAsync(context, message, rate.BrainId, "energy_rate").ConfigureAwait(false);
        if (context.Sender is not null)
        {
            context.Respond(result);
        }
    }

    private async Task ForwardSetCostEnergyAsync(IContext context, SetCostEnergyCommand flags)
    {
        var message = new SetCostEnergyEnabled
        {
            BrainId = flags.BrainId.ToProtoUuid(),
            CostEnabled = flags.CostEnabled,
            EnergyEnabled = flags.EnergyEnabled
        };

        var result = await SendCommandAsync(context, message, flags.BrainId, "set_cost_energy").ConfigureAwait(false);
        if (context.Sender is not null)
        {
            context.Respond(result);
        }
    }

    private async Task ForwardSetPlasticityAsync(IContext context, SetPlasticityCommand plasticity)
    {
        var message = new SetPlasticityEnabled
        {
            BrainId = plasticity.BrainId.ToProtoUuid(),
            PlasticityEnabled = plasticity.PlasticityEnabled,
            PlasticityRate = plasticity.PlasticityRate,
            ProbabilisticUpdates = plasticity.ProbabilisticUpdates
        };

        var result = await SendCommandAsync(context, message, plasticity.BrainId, "set_plasticity").ConfigureAwait(false);
        if (context.Sender is not null)
        {
            context.Respond(result);
        }
    }

    private async Task<IoCommandResult> SendCommandAsync(IContext context, object message, Guid brainId, string fallbackCommand)
    {
        if (_ioGateway is null)
        {
            return new IoCommandResult(brainId, fallbackCommand, false, "io_gateway_unavailable");
        }

        if (context.Sender is null)
        {
            context.Send(_ioGateway, message);
            return new IoCommandResult(brainId, fallbackCommand, true, "sent_fire_and_forget");
        }

        try
        {
            var ack = await context.RequestAsync<IoCommandAck>(_ioGateway, message, CommandAckTimeout).ConfigureAwait(false);
            var resolvedBrainId = ack.BrainId?.TryToGuid(out var parsed) == true ? parsed : brainId;
            var command = string.IsNullOrWhiteSpace(ack.Command) ? fallbackCommand : ack.Command;
            var resultMessage = string.IsNullOrWhiteSpace(ack.Message) ? "ok" : ack.Message;
            var energyState = ack.HasEnergyState ? ack.EnergyState : null;
            return new IoCommandResult(resolvedBrainId, command, ack.Success, resultMessage, energyState);
        }
        catch (Exception ex)
        {
            return new IoCommandResult(brainId, fallbackCommand, false, $"request_failed:{ex.Message}");
        }
    }

    private void SendToIo(IContext context, object message)
    {
        if (_ioGateway is null)
        {
            return;
        }

        context.Send(_ioGateway, message);
    }

    private void SendToIoRequest(IContext context, object message)
    {
        if (_ioGateway is null)
        {
            return;
        }

        context.Send(_ioGateway, message);
    }

    private void HandleOutput(OutputEvent output)
    {
        var brainId = output.BrainId?.TryToGuid(out var guid) == true ? guid.ToString("D") : "unknown";
        var now = DateTimeOffset.Now;
        _sink.OnOutputEvent(new OutputEventItem(
            now,
            now.ToString("g"),
            brainId,
            output.OutputIndex,
            output.Value,
            output.TickId));
    }

    private void HandleVector(OutputVectorEvent output)
    {
        var brainId = output.BrainId?.TryToGuid(out var guid) == true ? guid.ToString("D") : "unknown";
        var allZero = true;
        foreach (var value in output.Values)
        {
            if (Math.Abs(value) > 1e-6f)
            {
                allZero = false;
                break;
            }
        }

        var now = DateTimeOffset.Now;
        _sink.OnOutputVectorEvent(new OutputVectorEventItem(
            now,
            now.ToString("g"),
            brainId,
            PreviewValues(output.Values),
            allZero,
            output.TickId));
    }

    private void HandleDebug(DebugInbound inbound)
    {
        var outbound = inbound.Outbound;
        if (outbound is null)
        {
            return;
        }

        _sink.OnDebugEvent(new DebugEventItem(
            DateTimeOffset.UtcNow,
            outbound.Severity.ToString(),
            outbound.Context ?? string.Empty,
            outbound.Summary ?? string.Empty,
            outbound.Message ?? string.Empty,
            outbound.SenderActor ?? string.Empty,
            outbound.SenderNode ?? string.Empty));
    }

    private void HandleViz(VisualizationEvent viz)
    {
        var brainId = viz.BrainId?.TryToGuid(out var guid) == true ? guid.ToString("D") : "unknown";
        _sink.OnVizEvent(new VizEventItem(
            DateTimeOffset.UtcNow,
            viz.Type.ToString(),
            brainId,
            viz.TickId,
            viz.RegionId.ToString(),
            viz.Source?.Value.ToString() ?? string.Empty,
            viz.Target?.Value.ToString() ?? string.Empty,
            viz.Value,
            viz.Strength,
            viz.EventId ?? string.Empty));
    }

    private void HandleTermination(BrainTerminated terminated)
    {
        var brainId = terminated.BrainId?.TryToGuid(out var guid) == true ? guid.ToString("D") : "unknown";
        _sink.OnBrainTerminated(new BrainTerminatedItem(
            DateTimeOffset.UtcNow,
            brainId,
            terminated.Reason ?? string.Empty,
            terminated.LastEnergyRemaining,
            terminated.LastTickCost));
    }

    private void HandleSettingChanged(SettingChanged changed)
    {
        _sink.OnSettingChanged(new SettingItem(
            changed.Key ?? string.Empty,
            changed.Value ?? string.Empty,
            changed.UpdatedMs.ToString()));
    }

    private static string PreviewValues(IReadOnlyList<float> values)
    {
        if (values.Count == 0)
        {
            return "(empty)";
        }

        var take = Math.Min(values.Count, 18);
        var preview = string.Join(", ", values.Take(take).Select(v => v.ToString("0.###")));
        if (values.Count > take)
        {
            preview += $" ... ({values.Count})";
        }

        return preview;
    }

    private static string PidLabel(PID pid, string? fallbackAddress = null)
    {
        var address = string.IsNullOrWhiteSpace(pid.Address) ? fallbackAddress : pid.Address;
        return string.IsNullOrWhiteSpace(address) ? pid.Id : $"{address}/{pid.Id}";
    }
}

public sealed record SetIoGatewayPid(PID? Pid);

public sealed record SubscribeOutputsCommand(Guid BrainId);
public sealed record UnsubscribeOutputsCommand(Guid BrainId);
public sealed record SubscribeOutputsVectorCommand(Guid BrainId);
public sealed record UnsubscribeOutputsVectorCommand(Guid BrainId);

public sealed record InputWriteCommand(Guid BrainId, uint InputIndex, float Value);
public sealed record InputVectorCommand(Guid BrainId, IReadOnlyList<float> Values);
public sealed record RuntimeNeuronPulseCommand(Guid BrainId, uint TargetRegionId, uint TargetNeuronId, float Value);
public sealed record RuntimeNeuronStateWriteCommand(
    Guid BrainId,
    uint TargetRegionId,
    uint TargetNeuronId,
    bool SetBuffer,
    float BufferValue,
    bool SetAccumulator,
    float AccumulatorValue);

public sealed record EnergyCreditCommand(Guid BrainId, long Amount);
public sealed record EnergyRateCommand(Guid BrainId, long UnitsPerSecond);
public sealed record SetCostEnergyCommand(Guid BrainId, bool CostEnabled, bool EnergyEnabled);
public sealed record SetPlasticityCommand(Guid BrainId, bool PlasticityEnabled, float PlasticityRate, bool ProbabilisticUpdates);
public sealed record IoCommandResult(
    Guid BrainId,
    string Command,
    bool Success,
    string Message,
    BrainEnergyState? EnergyState = null);
