using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Shared;
using Proto;

namespace Nbn.Runtime.IO;

public sealed class IoGatewayActor : IActor
{
    private static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(15);

    private readonly IoOptions _options;
    private readonly Dictionary<Guid, BrainIoEntry> _brains = new();
    private readonly Dictionary<string, ClientInfo> _clients = new(StringComparer.Ordinal);
    private readonly PID? _hiveMindPid;
    private readonly PID? _reproPid;

    public IoGatewayActor(IoOptions options)
    {
        _options = options;
        _hiveMindPid = TryCreatePid(options.HiveMindAddress, options.HiveMindName);
        _reproPid = TryCreatePid(options.ReproAddress, options.ReproName);
    }

    public async Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
                Console.WriteLine($"IO Gateway actor online: {PidLabel(context.Self)}");
                break;
            case Connect message:
                HandleConnect(context, message);
                break;
            case SpawnBrainViaIO message:
                await HandleSpawnBrain(context, message);
                break;
            case BrainInfoRequest message:
                HandleBrainInfo(context, message);
                break;
            case InputWrite message:
                ForwardInput(context, message);
                break;
            case InputVector message:
                ForwardInput(context, message);
                break;
            case SubscribeOutputs message:
                ForwardOutput(context, message);
                break;
            case UnsubscribeOutputs message:
                ForwardOutput(context, message);
                break;
            case SubscribeOutputsVector message:
                ForwardOutput(context, message);
                break;
            case UnsubscribeOutputsVector message:
                ForwardOutput(context, message);
                break;
            case EnergyCredit message:
                ApplyEnergyCredit(message);
                break;
            case EnergyRate message:
                ApplyEnergyRate(message);
                break;
            case SetCostEnergyEnabled message:
                ApplyCostEnergyFlags(message);
                break;
            case SetPlasticityEnabled message:
                ApplyPlasticityFlags(message);
                break;
            case ApplyTickCost message:
                ApplyTickCost(context, message);
                break;
            case RegisterBrain message:
                RegisterBrain(context, message);
                break;
            case UnregisterBrain message:
                UnregisterBrain(context, message);
                break;
            case UpdateBrainSnapshot message:
                UpdateSnapshot(message);
                break;
            case BrainTerminated message:
                HandleBrainTerminated(context, message);
                break;
            case RequestSnapshot:
            case ExportBrainDefinition:
                ForwardToHiveMind(context);
                break;
            case ReproduceByBrainIds message:
                await HandleReproduceByBrainIds(context, message);
                break;
            case ReproduceByArtifacts message:
                await HandleReproduceByArtifacts(context, message);
                break;
        }
    }

    private void HandleConnect(IContext context, Connect message)
    {
        if (context.Sender is null)
        {
            return;
        }

        var key = PidKey(context.Sender);
        _clients[key] = new ClientInfo(context.Sender, message.ClientName ?? string.Empty);

        context.Respond(new ConnectAck
        {
            ServerName = _options.ServerName,
            ServerTimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        });
    }

    private async Task HandleSpawnBrain(IContext context, SpawnBrainViaIO message)
    {
        if (_hiveMindPid is null)
        {
            context.Respond(new SpawnBrainViaIOAck
            {
                Ack = new SpawnBrainAck { BrainId = Guid.Empty.ToProtoUuid() }
            });
            return;
        }

        try
        {
            var ack = await context.RequestAsync<SpawnBrainAck>(_hiveMindPid, message.Request, DefaultRequestTimeout);
            context.Respond(new SpawnBrainViaIOAck { Ack = ack });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"SpawnBrainViaIO failed: {ex.Message}");
            context.Respond(new SpawnBrainViaIOAck
            {
                Ack = new SpawnBrainAck { BrainId = Guid.Empty.ToProtoUuid() }
            });
        }
    }

    private void HandleBrainInfo(IContext context, BrainInfoRequest message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            return;
        }

        if (!_brains.TryGetValue(brainId, out var entry))
        {
            context.Respond(new BrainInfo
            {
                BrainId = message.BrainId,
                InputWidth = 0,
                OutputWidth = 0,
                CostEnabled = false,
                EnergyEnabled = false,
                EnergyRemaining = 0,
                PlasticityEnabled = false
            });
            return;
        }

        entry.Energy.Accrue();
        context.Respond(new BrainInfo
        {
            BrainId = message.BrainId,
            InputWidth = entry.InputWidth,
            OutputWidth = entry.OutputWidth,
            CostEnabled = entry.Energy.CostEnabled,
            EnergyEnabled = entry.Energy.EnergyEnabled,
            EnergyRemaining = entry.Energy.EnergyRemaining,
            PlasticityEnabled = entry.Energy.PlasticityEnabled
        });
    }

    private void ForwardInput(IContext context, object message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            return;
        }

        context.Send(entry.InputPid, message);
    }

    private void ForwardOutput(IContext context, object message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            return;
        }

        context.Forward(entry.OutputPid);
    }

    private void ApplyEnergyCredit(EnergyCredit message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            return;
        }

        entry.Energy.ApplyCredit(message.Amount);
        if (entry.Energy.EnergyRemaining >= 0)
        {
            entry.EnergyDepletedSignaled = false;
        }
    }

    private void ApplyEnergyRate(EnergyRate message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            return;
        }

        entry.Energy.SetEnergyRate(message.UnitsPerSecond);
    }

    private void ApplyCostEnergyFlags(SetCostEnergyEnabled message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            return;
        }

        entry.Energy.SetCostEnergyEnabled(message.CostEnabled, message.EnergyEnabled);
    }

    private void ApplyPlasticityFlags(SetPlasticityEnabled message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            return;
        }

        entry.Energy.SetPlasticity(message.PlasticityEnabled, message.PlasticityRate, message.ProbabilisticUpdates);
    }

    private void ApplyTickCost(IContext context, ApplyTickCost message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var entry))
        {
            return;
        }

        if (!entry.Energy.CostEnabled)
        {
            return;
        }

        entry.Energy.ApplyTickCost(message.TickCost);
        if (!entry.Energy.EnergyEnabled || entry.Energy.EnergyRemaining >= 0)
        {
            return;
        }

        if (entry.EnergyDepletedSignaled)
        {
            return;
        }

        entry.EnergyDepletedSignaled = true;

        if (_hiveMindPid is null)
        {
            var terminated = BuildEnergyTerminated(entry, message.TickCost);
            BroadcastToClients(context, terminated);
            StopAndRemoveBrain(context, entry);
            return;
        }

        context.Send(_hiveMindPid, new KillBrain
        {
            BrainId = entry.BrainId.ToProtoUuid(),
            Reason = "energy_exhausted"
        });
    }

    private void RegisterBrain(IContext context, RegisterBrain message)
    {
        if (_brains.TryGetValue(message.BrainId, out var existing))
        {
            if (existing.InputWidth != message.InputWidth || existing.OutputWidth != message.OutputWidth)
            {
                Console.WriteLine($"RegisterBrain width mismatch for {message.BrainId}. Keeping existing widths.");
            }

            if (message.BaseDefinition is not null)
            {
                existing.BaseDefinition = message.BaseDefinition;
            }

            if (message.LastSnapshot is not null)
            {
                existing.LastSnapshot = message.LastSnapshot;
            }

            if (message.EnergyState is not null)
            {
                existing.Energy.ResetFrom(message.EnergyState);
            }

            return;
        }

        var inputPid = context.Spawn(Props.FromProducer(() => new InputCoordinatorActor(message.BrainId, message.InputWidth)));
        var outputPid = context.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(message.BrainId, message.OutputWidth)));

        var energy = message.EnergyState ?? new BrainEnergyState();
        var entry = new BrainIoEntry(message.BrainId, inputPid, outputPid, message.InputWidth, message.OutputWidth, energy)
        {
            BaseDefinition = message.BaseDefinition,
            LastSnapshot = message.LastSnapshot
        };

        _brains.Add(message.BrainId, entry);
    }

    private void UnregisterBrain(IContext context, UnregisterBrain message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var entry))
        {
            return;
        }

        StopAndRemoveBrain(context, entry);
    }

    private void UpdateSnapshot(UpdateBrainSnapshot message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var entry))
        {
            return;
        }

        entry.LastSnapshot = message.Snapshot;
    }

    private void HandleBrainTerminated(IContext context, BrainTerminated message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            return;
        }

        if (_brains.TryGetValue(brainId, out var entry))
        {
            entry.BaseDefinition = message.BaseDef;
            entry.LastSnapshot = message.LastSnapshot;
            StopAndRemoveBrain(context, entry);
        }

        BroadcastToClients(context, message);
    }

    private void ForwardToHiveMind(IContext context)
    {
        if (_hiveMindPid is null)
        {
            return;
        }

        context.Forward(_hiveMindPid);
    }

    private async Task HandleReproduceByBrainIds(IContext context, ReproduceByBrainIds message)
    {
        if (_reproPid is null)
        {
            context.Respond(new Nbn.Proto.Io.ReproduceResult());
            return;
        }

        try
        {
            var result = await context.RequestAsync<Nbn.Proto.Repro.ReproduceResult>(_reproPid, message.Request, DefaultRequestTimeout);
            context.Respond(new Nbn.Proto.Io.ReproduceResult { Result = result });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"ReproduceByBrainIds failed: {ex.Message}");
            context.Respond(new Nbn.Proto.Io.ReproduceResult());
        }
    }

    private async Task HandleReproduceByArtifacts(IContext context, ReproduceByArtifacts message)
    {
        if (_reproPid is null)
        {
            context.Respond(new Nbn.Proto.Io.ReproduceResult());
            return;
        }

        try
        {
            var result = await context.RequestAsync<Nbn.Proto.Repro.ReproduceResult>(_reproPid, message.Request, DefaultRequestTimeout);
            context.Respond(new Nbn.Proto.Io.ReproduceResult { Result = result });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"ReproduceByArtifacts failed: {ex.Message}");
            context.Respond(new Nbn.Proto.Io.ReproduceResult());
        }
    }

    private bool TryGetBrainEntry(object message, out BrainIoEntry entry)
    {
        entry = null!;

        switch (message)
        {
            case InputWrite inputWrite when TryGetBrainId(inputWrite.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case InputVector inputVector when TryGetBrainId(inputVector.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case SubscribeOutputs subscribe when TryGetBrainId(subscribe.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case UnsubscribeOutputs unsubscribe when TryGetBrainId(unsubscribe.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case SubscribeOutputsVector subscribeVector when TryGetBrainId(subscribeVector.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case UnsubscribeOutputsVector unsubscribeVector when TryGetBrainId(unsubscribeVector.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case EnergyCredit energyCredit when TryGetBrainId(energyCredit.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case EnergyRate energyRate when TryGetBrainId(energyRate.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case SetCostEnergyEnabled costEnergy when TryGetBrainId(costEnergy.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
            case SetPlasticityEnabled plasticity when TryGetBrainId(plasticity.BrainId, out var brainId):
                return _brains.TryGetValue(brainId, out entry!);
        }

        return false;
    }

    private static bool TryGetBrainId(Uuid? brainId, out Guid guid)
    {
        if (brainId is null)
        {
            guid = Guid.Empty;
            return false;
        }

        return brainId.TryToGuid(out guid);
    }

    private BrainTerminated BuildEnergyTerminated(BrainIoEntry entry, long lastTickCost)
    {
        return new BrainTerminated
        {
            BrainId = entry.BrainId.ToProtoUuid(),
            Reason = "energy_exhausted",
            BaseDef = entry.BaseDefinition ?? new ArtifactRef(),
            LastSnapshot = entry.LastSnapshot ?? new ArtifactRef(),
            LastEnergyRemaining = entry.Energy.EnergyRemaining,
            LastTickCost = lastTickCost,
            TimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };
    }

    private void BroadcastToClients(IContext context, object message)
    {
        if (_clients.Count == 0)
        {
            return;
        }

        foreach (var client in _clients.Values)
        {
            context.Send(client.Pid, message);
        }
    }

    private void StopAndRemoveBrain(IContext context, BrainIoEntry entry)
    {
        context.Stop(entry.InputPid);
        context.Stop(entry.OutputPid);
        _brains.Remove(entry.BrainId);
    }

    private static PID? TryCreatePid(string? address, string? name)
    {
        if (string.IsNullOrWhiteSpace(address) || string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        return new PID(address, name);
    }

    private static string PidKey(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private static string PidLabel(PID? pid)
        => pid is null ? "unknown" : PidKey(pid);

    private sealed record ClientInfo(PID Pid, string Name);
}
