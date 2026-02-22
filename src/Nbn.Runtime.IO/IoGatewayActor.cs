using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.IO;

public sealed class IoGatewayActor : IActor
{
    private static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(15);

    private readonly IoOptions _options;
    private readonly Dictionary<Guid, BrainIoEntry> _brains = new();
    private readonly Dictionary<string, ClientInfo> _clients = new(StringComparer.Ordinal);
    private readonly Dictionary<Guid, PID> _routerCache = new();
    private readonly Dictionary<Guid, string> _routerRegistration = new();
    private readonly PID? _hiveMindPid;
    private readonly PID? _reproPid;

    public IoGatewayActor(IoOptions options, PID? hiveMindPid = null, PID? reproPid = null)
    {
        _options = options;
        _hiveMindPid = hiveMindPid ?? TryCreatePid(options.HiveMindAddress, options.HiveMindName);
        _reproPid = reproPid ?? TryCreatePid(options.ReproAddress, options.ReproName);
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
                await ForwardInputAsync(context, message);
                break;
            case InputVector message:
                await ForwardInputAsync(context, message);
                break;
            case RuntimeNeuronPulse message:
                await ForwardRuntimeNeuronAsync(context, message);
                break;
            case RuntimeNeuronStateWrite message:
                await ForwardRuntimeNeuronAsync(context, message);
                break;
            case SubscribeOutputs message:
                await ForwardOutputAsync(context, message);
                break;
            case UnsubscribeOutputs message:
                await ForwardOutputAsync(context, message);
                break;
            case SubscribeOutputsVector message:
                await ForwardOutputAsync(context, message);
                break;
            case UnsubscribeOutputsVector message:
                await ForwardOutputAsync(context, message);
                break;
            case EnergyCredit message:
                ApplyEnergyCredit(message);
                break;
            case EnergyRate message:
                ApplyEnergyRate(message);
                break;
            case SetCostEnergyEnabled message:
                ApplyCostEnergyFlags(context, message);
                break;
            case SetPlasticityEnabled message:
                ApplyPlasticityFlags(context, message);
                break;
            case ApplyTickCost message:
                ApplyTickCost(context, message);
                break;
            case RegisterBrain message:
                await RegisterBrainAsync(context, message);
                break;
            case UnregisterBrain message:
                UnregisterBrain(context, message);
                break;
            case DrainInputs message:
                await HandleDrainInputsAsync(context, message);
                break;
            case UpdateBrainSnapshot message:
                UpdateSnapshot(message);
                break;
            case ProtoControl.BrainTerminated message:
                HandleBrainTerminated(context, message);
                break;
            case RequestSnapshot message:
                await HandleRequestSnapshotAsync(context, message);
                break;
            case ExportBrainDefinition message:
                await HandleExportBrainDefinitionAsync(context, message);
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
                Ack = new ProtoControl.SpawnBrainAck { BrainId = Guid.Empty.ToProtoUuid() }
            });
            return;
        }

        try
        {
            var ack = await context.RequestAsync<ProtoControl.SpawnBrainAck>(_hiveMindPid, message.Request, DefaultRequestTimeout);
            context.Respond(new SpawnBrainViaIOAck { Ack = ack });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"SpawnBrainViaIO failed: {ex.Message}");
            context.Respond(new SpawnBrainViaIOAck
            {
                Ack = new ProtoControl.SpawnBrainAck { BrainId = Guid.Empty.ToProtoUuid() }
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
                PlasticityEnabled = false,
                EnergyRateUnitsPerSecond = 0,
                PlasticityRate = 0,
                PlasticityProbabilisticUpdates = false,
                LastTickCost = 0,
                BaseDefinition = new ArtifactRef(),
                LastSnapshot = new ArtifactRef()
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
            PlasticityEnabled = entry.Energy.PlasticityEnabled,
            EnergyRateUnitsPerSecond = entry.Energy.EnergyRateUnitsPerSecond,
            PlasticityRate = entry.Energy.PlasticityRate,
            PlasticityProbabilisticUpdates = entry.Energy.PlasticityProbabilisticUpdates,
            LastTickCost = entry.Energy.LastTickCost,
            BaseDefinition = entry.BaseDefinition ?? new ArtifactRef(),
            LastSnapshot = entry.LastSnapshot ?? new ArtifactRef()
        });
    }

    private async Task ForwardInputAsync(IContext context, object message)
    {
        if (!TryGetBrainId(message, out var brainId))
        {
            return;
        }

        if (!TryGetBrainEntry(message, out var entry))
        {
            entry = await EnsureBrainEntryAsync(context, brainId).ConfigureAwait(false);
        }

        if (entry is not null)
        {
            context.Send(entry.InputPid, message);
        }

        if (_hiveMindPid is null)
        {
            return;
        }

        var routerPid = await ResolveRouterPidAsync(context, brainId).ConfigureAwait(false);
        if (routerPid is null)
        {
            return;
        }

        context.Send(routerPid, message);
    }

    private async Task ForwardRuntimeNeuronAsync(IContext context, object message)
    {
        if (!TryGetBrainId(message, out var brainId))
        {
            return;
        }

        var routerPid = await ResolveRouterPidAsync(context, brainId).ConfigureAwait(false);
        if (routerPid is null)
        {
            return;
        }

        context.Send(routerPid, message);
    }

    private async Task ForwardOutputAsync(IContext context, object message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            if (TryGetBrainId(message, out var brainId))
            {
                entry = await EnsureBrainEntryAsync(context, brainId).ConfigureAwait(false);
            }
        }

        if (entry is null)
        {
            return;
        }

        context.Forward(entry.OutputPid);
    }

    private async Task HandleDrainInputsAsync(IContext context, DrainInputs message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            context.Respond(new InputDrain
            {
                BrainId = message.BrainId,
                TickId = message.TickId
            });
            return;
        }

        if (!_brains.TryGetValue(brainId, out var entry))
        {
            entry = await EnsureBrainEntryAsync(context, brainId).ConfigureAwait(false);
            if (entry is null)
            {
                context.Respond(new InputDrain
                {
                    BrainId = message.BrainId,
                    TickId = message.TickId
                });
                return;
            }
        }

        try
        {
            var drain = await context.RequestAsync<InputDrain>(entry.InputPid, message, DefaultRequestTimeout);
            context.Respond(drain);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"DrainInputs failed for {brainId}: {ex.Message}");
            context.Respond(new InputDrain
            {
                BrainId = message.BrainId,
                TickId = message.TickId
            });
        }
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

    private void ApplyCostEnergyFlags(IContext context, SetCostEnergyEnabled message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            return;
        }

        entry.Energy.SetCostEnergyEnabled(message.CostEnabled, message.EnergyEnabled);

        if (_hiveMindPid is not null)
        {
            context.Send(_hiveMindPid, new ProtoControl.SetBrainCostEnergy
            {
                BrainId = message.BrainId,
                CostEnabled = message.CostEnabled,
                EnergyEnabled = message.EnergyEnabled
            });
        }
    }

    private void ApplyPlasticityFlags(IContext context, SetPlasticityEnabled message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            return;
        }

        entry.Energy.SetPlasticity(message.PlasticityEnabled, message.PlasticityRate, message.ProbabilisticUpdates);

        if (_hiveMindPid is not null)
        {
            context.Send(_hiveMindPid, new ProtoControl.SetBrainPlasticity
            {
                BrainId = message.BrainId,
                PlasticityEnabled = message.PlasticityEnabled,
                PlasticityRate = message.PlasticityRate,
                ProbabilisticUpdates = message.ProbabilisticUpdates
            });
        }
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

        context.Send(_hiveMindPid, new ProtoControl.KillBrain
        {
            BrainId = entry.BrainId.ToProtoUuid(),
            Reason = "energy_exhausted"
        });
    }

    private async Task RegisterBrainAsync(IContext context, RegisterBrain message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            return;
        }

        if (_brains.TryGetValue(brainId, out var existing))
        {
            if (existing.InputWidth != message.InputWidth || existing.OutputWidth != message.OutputWidth)
            {
                if (message.OutputWidth > existing.OutputWidth && message.OutputWidth > 0)
                {
                    existing.OutputWidth = message.OutputWidth;
                }
                else
                {
                    Console.WriteLine($"RegisterBrain width mismatch for {brainId}. Keeping existing widths.");
                }
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

            if (message.HasRuntimeConfig)
            {
                existing.Energy.SetRuntimeConfig(
                    message.CostEnabled,
                    message.EnergyEnabled,
                    message.PlasticityEnabled,
                    message.PlasticityRate,
                    message.PlasticityProbabilisticUpdates,
                    message.LastTickCost);
            }

            await EnsureIoGatewayRegisteredAsync(context, brainId);
            await EnsureOutputSinkRegisteredAsync(context, brainId, existing.OutputPid);
            return;
        }

        var inputName = IoNames.InputCoordinatorPrefix + brainId.ToString("N");
        var outputName = IoNames.OutputCoordinatorPrefix + brainId.ToString("N");

        PID inputPid;
        PID outputPid;
        try
        {
            inputPid = context.SpawnNamed(Props.FromProducer(() => new InputCoordinatorActor(brainId, message.InputWidth)), inputName);
        }
        catch
        {
            inputPid = context.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, message.InputWidth)));
        }

        try
        {
            outputPid = context.SpawnNamed(Props.FromProducer(() => new OutputCoordinatorActor(brainId, message.OutputWidth)), outputName);
        }
        catch
        {
            outputPid = context.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, message.OutputWidth)));
        }

        var energy = new BrainEnergyState();
        if (message.EnergyState is not null)
        {
            energy.ResetFrom(message.EnergyState);
        }

        if (message.HasRuntimeConfig)
        {
            energy.SetRuntimeConfig(
                message.CostEnabled,
                message.EnergyEnabled,
                message.PlasticityEnabled,
                message.PlasticityRate,
                message.PlasticityProbabilisticUpdates,
                message.LastTickCost);
        }

        var entry = new BrainIoEntry(brainId, inputPid, outputPid, message.InputWidth, message.OutputWidth, energy)
        {
            BaseDefinition = message.BaseDefinition,
            LastSnapshot = message.LastSnapshot
        };

        _brains.Add(brainId, entry);

        await EnsureIoGatewayRegisteredAsync(context, brainId);
        await EnsureOutputSinkRegisteredAsync(context, brainId, outputPid);
    }

    private void UnregisterBrain(IContext context, UnregisterBrain message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            return;
        }

        if (!_brains.TryGetValue(brainId, out var entry))
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

    private void HandleBrainTerminated(IContext context, ProtoControl.BrainTerminated message)
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

    private async Task HandleExportBrainDefinitionAsync(IContext context, ExportBrainDefinition message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            context.Respond(new BrainDefinitionReady());
            return;
        }

        if (_brains.TryGetValue(brainId, out var entry) && HasArtifactRef(entry.BaseDefinition))
        {
            context.Respond(new BrainDefinitionReady
            {
                BrainId = message.BrainId,
                BrainDef = entry.BaseDefinition
            });
            return;
        }

        if (_hiveMindPid is null)
        {
            context.Respond(new BrainDefinitionReady { BrainId = message.BrainId });
            return;
        }

        try
        {
            var ready = await context.RequestAsync<BrainDefinitionReady>(_hiveMindPid, message, DefaultRequestTimeout).ConfigureAwait(false);
            if (ready is not null
                && HasArtifactRef(ready.BrainDef)
                && _brains.TryGetValue(brainId, out var existing))
            {
                existing.BaseDefinition = ready.BrainDef;
            }

            context.Respond(ready ?? new BrainDefinitionReady { BrainId = message.BrainId });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"ExportBrainDefinition failed for {brainId}: {ex.Message}");
            context.Respond(new BrainDefinitionReady { BrainId = message.BrainId });
        }
    }

    private async Task HandleRequestSnapshotAsync(IContext context, RequestSnapshot message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            context.Respond(new SnapshotReady());
            return;
        }

        if (_brains.TryGetValue(brainId, out var entry) && HasArtifactRef(entry.LastSnapshot))
        {
            context.Respond(new SnapshotReady
            {
                BrainId = message.BrainId,
                Snapshot = entry.LastSnapshot
            });
            return;
        }

        if (_hiveMindPid is null)
        {
            context.Respond(new SnapshotReady { BrainId = message.BrainId });
            return;
        }

        try
        {
            var ready = await context.RequestAsync<SnapshotReady>(_hiveMindPid, message, DefaultRequestTimeout).ConfigureAwait(false);
            if (ready is not null
                && HasArtifactRef(ready.Snapshot)
                && _brains.TryGetValue(brainId, out var existing))
            {
                existing.LastSnapshot = ready.Snapshot;
            }

            context.Respond(ready ?? new SnapshotReady { BrainId = message.BrainId });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"RequestSnapshot failed for {brainId}: {ex.Message}");
            context.Respond(new SnapshotReady { BrainId = message.BrainId });
        }
    }

    private static bool HasArtifactRef(ArtifactRef? reference)
        => reference is not null
           && reference.Sha256 is not null
           && reference.Sha256.Value is not null
           && reference.Sha256.Value.Length == 32;

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
            EnsureSimilarityScore(result);
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
            EnsureSimilarityScore(result);
            context.Respond(new Nbn.Proto.Io.ReproduceResult { Result = result });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"ReproduceByArtifacts failed: {ex.Message}");
            context.Respond(new Nbn.Proto.Io.ReproduceResult());
        }
    }

    private static void EnsureSimilarityScore(Nbn.Proto.Repro.ReproduceResult? result)
    {
        var report = result?.Report;
        if (report is null || report.SimilarityScore > 0f)
        {
            return;
        }

        var total = 0f;
        var count = 0;

        if (report.RegionSpanScore > 0f)
        {
            total += report.RegionSpanScore;
            count++;
        }

        if (report.FunctionScore > 0f)
        {
            total += report.FunctionScore;
            count++;
        }

        if (report.ConnectivityScore > 0f)
        {
            total += report.ConnectivityScore;
            count++;
        }

        if (count > 0)
        {
            report.SimilarityScore = Clamp01(total / count);
            return;
        }

        report.SimilarityScore = report.Compatible ? 1f : 0f;
    }

    private static float Clamp01(float value)
    {
        if (value < 0f)
        {
            return 0f;
        }

        if (value > 1f)
        {
            return 1f;
        }

        return value;
    }

    private async Task EnsureIoGatewayRegisteredAsync(IContext context, Guid brainId)
    {
        if (_hiveMindPid is null)
        {
            return;
        }

        try
        {
            await ResolveRouterPidAsync(context, brainId).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"RegisterIoGateway failed for {brainId}: {ex.Message}");
        }
    }

    private async Task<BrainIoEntry?> EnsureBrainEntryAsync(IContext context, Guid brainId)
    {
        if (_brains.TryGetValue(brainId, out var existing))
        {
            return existing;
        }

        if (_hiveMindPid is null)
        {
            return null;
        }

        try
        {
            var info = await context.RequestAsync<ProtoControl.BrainIoInfo>(
                    _hiveMindPid,
                    new ProtoControl.GetBrainIoInfo { BrainId = brainId.ToProtoUuid() },
                    DefaultRequestTimeout)
                .ConfigureAwait(false);

            if (info is null || info.InputWidth == 0)
            {
                return null;
            }

            var register = new RegisterBrain
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = info.InputWidth,
                OutputWidth = info.OutputWidth
            };
            await RegisterBrainAsync(context, register);

            if (_brains.TryGetValue(brainId, out var entry))
            {
                return entry;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"EnsureBrainEntry failed for {brainId}: {ex.Message}");
        }

        return null;
    }

    private async Task EnsureOutputSinkRegisteredAsync(IContext context, Guid brainId, PID outputPid)
    {
        if (_hiveMindPid is null)
        {
            Console.WriteLine($"RegisterOutputSink skipped (no HiveMind PID) for {brainId}");
            return;
        }

        try
        {
            var outputLabel = PidLabel(ToRemotePid(context, outputPid));
            context.Send(_hiveMindPid, new ProtoControl.RegisterOutputSink
            {
                BrainId = brainId.ToProtoUuid(),
                OutputPid = outputLabel
            });

            Console.WriteLine($"RegisterOutputSink sent for {brainId} -> {outputLabel}");
            await Task.CompletedTask;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"RegisterOutputSink failed for {brainId}: {ex.Message}");
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

    private static bool TryGetBrainId(object message, out Guid guid)
    {
        guid = Guid.Empty;

        switch (message)
        {
            case InputWrite inputWrite:
                return TryGetBrainId(inputWrite.BrainId, out guid);
            case InputVector inputVector:
                return TryGetBrainId(inputVector.BrainId, out guid);
            case SubscribeOutputs subscribe:
                return TryGetBrainId(subscribe.BrainId, out guid);
            case UnsubscribeOutputs unsubscribe:
                return TryGetBrainId(unsubscribe.BrainId, out guid);
            case SubscribeOutputsVector subscribeVector:
                return TryGetBrainId(subscribeVector.BrainId, out guid);
            case UnsubscribeOutputsVector unsubscribeVector:
                return TryGetBrainId(unsubscribeVector.BrainId, out guid);
            case EnergyCredit energyCredit:
                return TryGetBrainId(energyCredit.BrainId, out guid);
            case EnergyRate energyRate:
                return TryGetBrainId(energyRate.BrainId, out guid);
            case SetCostEnergyEnabled costEnergy:
                return TryGetBrainId(costEnergy.BrainId, out guid);
            case SetPlasticityEnabled plasticity:
                return TryGetBrainId(plasticity.BrainId, out guid);
            case RuntimeNeuronPulse pulse:
                return TryGetBrainId(pulse.BrainId, out guid);
            case RuntimeNeuronStateWrite stateWrite:
                return TryGetBrainId(stateWrite.BrainId, out guid);
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

    private ProtoControl.BrainTerminated BuildEnergyTerminated(BrainIoEntry entry, long lastTickCost)
    {
        return new ProtoControl.BrainTerminated
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
        _routerCache.Remove(entry.BrainId);
        _routerRegistration.Remove(entry.BrainId);
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

    private async Task<PID?> ResolveRouterPidAsync(IContext context, Guid brainId)
    {
        if (_routerCache.TryGetValue(brainId, out var cached))
        {
            return cached;
        }

        if (_hiveMindPid is null)
        {
            return null;
        }

        try
        {
            var info = await context.RequestAsync<ProtoControl.BrainRoutingInfo>(
                    _hiveMindPid,
                    new ProtoControl.GetBrainRouting { BrainId = brainId.ToProtoUuid() },
                    DefaultRequestTimeout)
                .ConfigureAwait(false);

            if (info is null)
            {
                return null;
            }

            if (TryParsePid(info.SignalRouterPid, out var routerPid) && routerPid is not null)
            {
                _routerCache[brainId] = routerPid;
                RegisterIoGatewayPid(context, brainId, routerPid);
                return routerPid;
            }

            if (TryParsePid(info.BrainRootPid, out var rootPid) && rootPid is not null)
            {
                _routerCache[brainId] = rootPid;
                RegisterIoGatewayPid(context, brainId, rootPid);
                return rootPid;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Resolve router failed: {ex.Message}");
        }

        return null;
    }

    private static bool TryParsePid(string? value, out PID? pid)
    {
        pid = null;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        var slashIndex = trimmed.IndexOf('/');
        if (slashIndex <= 0)
        {
            pid = new PID(string.Empty, trimmed);
            return true;
        }

        var address = trimmed[..slashIndex];
        var id = trimmed[(slashIndex + 1)..];
        if (string.IsNullOrWhiteSpace(id))
        {
            return false;
        }

        pid = new PID(address, id);
        return true;
    }

    private void RegisterIoGatewayPid(IContext context, Guid brainId, PID routerPid)
    {
        var routerLabel = PidLabel(routerPid);
        if (_routerRegistration.TryGetValue(brainId, out var registered) && registered == routerLabel)
        {
            return;
        }

        var selfPid = ToRemotePid(context, context.Self);
        context.Send(routerPid, new RegisterIoGateway
        {
            BrainId = brainId.ToProtoUuid(),
            IoGatewayPid = PidLabel(selfPid)
        });

        _routerRegistration[brainId] = routerLabel;
    }

    private static PID ToRemotePid(IContext context, PID pid)
    {
        if (!string.IsNullOrWhiteSpace(pid.Address))
        {
            return pid;
        }

        var address = context.System.Address;
        if (string.IsNullOrWhiteSpace(address))
        {
            return pid;
        }

        return new PID(address, pid.Id);
    }

    private sealed record ClientInfo(PID Pid, string Name);
}
