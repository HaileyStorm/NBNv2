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
                SendToIoRequest(context, new SubscribeOutputs { BrainId = subscribe.BrainId.ToProtoUuid() });
                return Task.CompletedTask;
            case UnsubscribeOutputsCommand unsubscribe:
                SendToIoRequest(context, new UnsubscribeOutputs { BrainId = unsubscribe.BrainId.ToProtoUuid() });
                return Task.CompletedTask;
            case SubscribeOutputsVectorCommand subscribeVector:
                SendToIoRequest(context, new SubscribeOutputsVector { BrainId = subscribeVector.BrainId.ToProtoUuid() });
                return Task.CompletedTask;
            case UnsubscribeOutputsVectorCommand unsubscribeVector:
                SendToIoRequest(context, new UnsubscribeOutputsVector { BrainId = unsubscribeVector.BrainId.ToProtoUuid() });
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
            case EnergyCreditCommand credit:
                SendToIo(context, new EnergyCredit
                {
                    BrainId = credit.BrainId.ToProtoUuid(),
                    Amount = credit.Amount
                });
                return Task.CompletedTask;
            case EnergyRateCommand rate:
                SendToIo(context, new EnergyRate
                {
                    BrainId = rate.BrainId.ToProtoUuid(),
                    UnitsPerSecond = rate.UnitsPerSecond
                });
                return Task.CompletedTask;
            case SetCostEnergyCommand flags:
                SendToIo(context, new SetCostEnergyEnabled
                {
                    BrainId = flags.BrainId.ToProtoUuid(),
                    CostEnabled = flags.CostEnabled,
                    EnergyEnabled = flags.EnergyEnabled
                });
                return Task.CompletedTask;
            case SetPlasticityCommand plasticity:
                SendToIo(context, new SetPlasticityEnabled
                {
                    BrainId = plasticity.BrainId.ToProtoUuid(),
                    PlasticityEnabled = plasticity.PlasticityEnabled,
                    PlasticityRate = plasticity.PlasticityRate,
                    ProbabilisticUpdates = plasticity.ProbabilisticUpdates
                });
                return Task.CompletedTask;
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

        // Use Request so the IO gateway sees the sender and can register subscriptions.
        context.Request(_ioGateway, message);
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

        var take = Math.Min(values.Count, 6);
        var preview = string.Join(", ", values.Take(take).Select(v => v.ToString("0.###")));
        if (values.Count > take)
        {
            preview += $" ... ({values.Count})";
        }

        return preview;
    }
}

public sealed record SetIoGatewayPid(PID? Pid);

public sealed record SubscribeOutputsCommand(Guid BrainId);
public sealed record UnsubscribeOutputsCommand(Guid BrainId);
public sealed record SubscribeOutputsVectorCommand(Guid BrainId);
public sealed record UnsubscribeOutputsVectorCommand(Guid BrainId);

public sealed record InputWriteCommand(Guid BrainId, uint InputIndex, float Value);
public sealed record InputVectorCommand(Guid BrainId, IReadOnlyList<float> Values);

public sealed record EnergyCreditCommand(Guid BrainId, long Amount);
public sealed record EnergyRateCommand(Guid BrainId, long UnitsPerSecond);
public sealed record SetCostEnergyCommand(Guid BrainId, bool CostEnabled, bool EnergyEnabled);
public sealed record SetPlasticityCommand(Guid BrainId, bool PlasticityEnabled, float PlasticityRate, bool ProbabilisticUpdates);
