using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;
using ProtoSpec = Nbn.Proto.Speciation;

namespace Nbn.Runtime.IO;

public sealed class IoGatewayActor : IActor
{
    private static readonly bool LogOutput = IsEnvTrue("NBN_IO_LOG_OUTPUT");
    private static readonly bool LogInputDiagnostics =
        IsEnvTrue("NBN_VIZ_DIAGNOSTICS_ENABLED") || IsEnvTrue("NBN_INPUT_DIAGNOSTICS_ENABLED");
    private static readonly bool LogMetadataDiagnostics =
        IsEnvTrue("NBN_METADATA_DIAGNOSTICS_ENABLED") || IsEnvTrue("NBN_IO_METADATA_DIAGNOSTICS_ENABLED");
    private static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan BrainInfoResolveTimeout = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan SpawnRequestTimeout = TimeSpan.FromSeconds(70);
    private static readonly TimeSpan ReproRequestTimeout = TimeSpan.FromSeconds(45);
    private static readonly TimeSpan SpeciationRequestTimeout = TimeSpan.FromSeconds(45);
    private static readonly TimeSpan ExportRequestTimeout = TimeSpan.FromSeconds(45);
    private const float DefaultPlasticityRate = 0.001f;
    private const float DefaultPlasticityDelta = DefaultPlasticityRate;
    private const long DefaultPlasticityEnergyCostReferenceTickCost = 100;
    private const float DefaultPlasticityEnergyCostResponseStrength = 1f;
    private const float DefaultPlasticityEnergyCostMinScale = 0.1f;
    private const float DefaultPlasticityEnergyCostMaxScale = 1f;
    private const ProtoControl.InputCoordinatorMode DefaultInputCoordinatorMode =
        ProtoControl.InputCoordinatorMode.DirtyOnChange;
    private const ProtoControl.OutputVectorSource DefaultOutputVectorSource =
        ProtoControl.OutputVectorSource.Potential;
    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private readonly IoOptions _options;
    private readonly Dictionary<Guid, BrainIoEntry> _brains = new();
    private readonly Dictionary<string, ClientInfo> _clients = new(StringComparer.Ordinal);
    private readonly Dictionary<Guid, PID> _routerCache = new();
    private readonly Dictionary<Guid, string> _routerRegistration = new();
    private readonly Dictionary<Guid, Dictionary<string, PID>> _pendingOutputSubscribers = new();
    private readonly Dictionary<Guid, Dictionary<string, PID>> _pendingOutputVectorSubscribers = new();
    private readonly PID? _configuredHiveMindPid;
    private readonly PID? _configuredReproPid;
    private readonly PID? _configuredSpeciationPid;
    private PID? _hiveMindPid;
    private PID? _reproPid;
    private PID? _speciationPid;

    public IoGatewayActor(IoOptions options, PID? hiveMindPid = null, PID? reproPid = null, PID? speciationPid = null)
    {
        _options = options;
        _configuredHiveMindPid = hiveMindPid ?? TryCreatePid(options.HiveMindAddress, options.HiveMindName);
        _configuredReproPid = reproPid ?? TryCreatePid(options.ReproAddress, options.ReproName);
        _configuredSpeciationPid = speciationPid ?? TryCreatePid(options.SpeciationAddress, options.SpeciationName);
        _hiveMindPid = _configuredHiveMindPid;
        _reproPid = _configuredReproPid;
        _speciationPid = _configuredSpeciationPid;
    }

    public sealed record DiscoverySnapshotApplied(IReadOnlyDictionary<string, ServiceEndpointRegistration> Registrations);

    public sealed record EndpointStateObserved(ServiceEndpointObservation Observation);

    public async Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
                Console.WriteLine($"IO Gateway actor online: {PidLabel(context.Self)}");
                break;
            case Terminated terminated:
                HandleClientTerminated(context, terminated);
                break;
            case Connect message:
                HandleConnect(context, message);
                break;
            case SpawnBrainViaIO message:
                await HandleSpawnBrain(context, message);
                break;
            case BrainInfoRequest message:
                await HandleBrainInfoAsync(context, message);
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
                ApplyEnergyCredit(context, message);
                break;
            case EnergyRate message:
                ApplyEnergyRate(context, message);
                break;
            case SetCostEnergyEnabled message:
                ApplyCostEnergyFlags(context, message);
                break;
            case SetPlasticityEnabled message:
                ApplyPlasticityFlags(context, message);
                break;
            case SetHomeostasisEnabled message:
                ApplyHomeostasisFlags(context, message);
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
                HandleExportBrainDefinition(context, message);
                break;
            case ReproduceByBrainIds message:
                HandleReproduceByBrainIds(context, message);
                break;
            case ReproduceByArtifacts message:
                HandleReproduceByArtifacts(context, message);
                break;
            case AssessCompatibilityByBrainIds message:
                HandleAssessCompatibilityByBrainIds(context, message);
                break;
            case AssessCompatibilityByArtifacts message:
                HandleAssessCompatibilityByArtifacts(context, message);
                break;
            case SpeciationStatus message:
                HandleSpeciationStatus(context, message);
                break;
            case SpeciationGetConfig message:
                HandleSpeciationGetConfig(context, message);
                break;
            case SpeciationSetConfig message:
                HandleSpeciationSetConfig(context, message);
                break;
            case SpeciationResetAll message:
                HandleSpeciationResetAll(context, message);
                break;
            case SpeciationDeleteEpoch message:
                HandleSpeciationDeleteEpoch(context, message);
                break;
            case SpeciationEvaluate message:
                HandleSpeciationEvaluate(context, message);
                break;
            case SpeciationAssign message:
                HandleSpeciationAssign(context, message);
                break;
            case SpeciationBatchEvaluateApply message:
                HandleSpeciationBatchEvaluateApply(context, message);
                break;
            case SpeciationListMemberships message:
                HandleSpeciationListMemberships(context, message);
                break;
            case SpeciationQueryMembership message:
                HandleSpeciationQueryMembership(context, message);
                break;
            case SpeciationListHistory message:
                HandleSpeciationListHistory(context, message);
                break;
            case DiscoverySnapshotApplied snapshot:
                ApplyDiscoverySnapshot(snapshot);
                break;
            case EndpointStateObserved observed:
                ApplyObservedEndpoint(observed.Observation, source: "update");
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
        if (!_clients.ContainsKey(key))
        {
            context.Watch(context.Sender);
        }

        _clients[key] = new ClientInfo(context.Sender, message.ClientName ?? string.Empty);

        context.Respond(new ConnectAck
        {
            ServerName = _options.ServerName,
            ServerTimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        });
    }

    private void HandleClientTerminated(IContext context, Terminated terminated)
    {
        if (terminated.Who is null)
        {
            return;
        }

        var key = PidKey(terminated.Who);
        if (_clients.Remove(key))
        {
            context.Unwatch(terminated.Who);
        }
    }

    private void ApplyDiscoverySnapshot(DiscoverySnapshotApplied snapshot)
    {
        if (snapshot.Registrations is null)
        {
            return;
        }

        var hiveSeen = false;
        var reproSeen = false;
        var speciationSeen = false;
        foreach (var entry in snapshot.Registrations)
        {
            if (!TryMarkDiscoveryKey(entry.Key, ref hiveSeen, ref reproSeen, ref speciationSeen))
            {
                continue;
            }

            ApplyEndpoint(entry.Value);
        }

        if (!hiveSeen)
        {
            _hiveMindPid = _configuredHiveMindPid;
        }

        if (!reproSeen)
        {
            _reproPid = _configuredReproPid;
        }

        if (!speciationSeen)
        {
            _speciationPid = _configuredSpeciationPid;
        }
    }

    private void ApplyObservedEndpoint(ServiceEndpointObservation observation, string source)
    {
        if (!IsDiscoveryKey(observation.Key))
        {
            return;
        }

        if (observation.Kind == ServiceEndpointObservationKind.Upserted)
        {
            if (observation.Registration is ServiceEndpointRegistration registration)
            {
                ApplyEndpoint(registration);
                return;
            }

            ApplyObservationRemoval(observation.Key, source, "registration_missing");
            return;
        }

        if (observation.Kind == ServiceEndpointObservationKind.Removed
            || observation.Kind == ServiceEndpointObservationKind.Invalid)
        {
            var fallbackReason = observation.Kind == ServiceEndpointObservationKind.Removed
                ? "endpoint_removed"
                : "endpoint_parse_failed";
            ApplyObservationRemoval(observation.Key, source, NormalizeFailureReason(observation.FailureReason, fallbackReason));
        }
    }

    private void ApplyEndpoint(ServiceEndpointRegistration registration)
    {
        if (string.Equals(registration.Key, ServiceEndpointSettings.HiveMindKey, StringComparison.Ordinal))
        {
            _hiveMindPid = registration.Endpoint.ToPid();
            return;
        }

        if (string.Equals(registration.Key, ServiceEndpointSettings.ReproductionManagerKey, StringComparison.Ordinal))
        {
            _reproPid = registration.Endpoint.ToPid();
            return;
        }

        if (string.Equals(registration.Key, ServiceEndpointSettings.SpeciationManagerKey, StringComparison.Ordinal))
        {
            _speciationPid = registration.Endpoint.ToPid();
        }
    }

    private void ApplyObservationRemoval(string key, string source, string reason)
    {
        if (string.Equals(key, ServiceEndpointSettings.HiveMindKey, StringComparison.Ordinal))
        {
            _hiveMindPid = _configuredHiveMindPid;
            Console.WriteLine($"[WARN] IO discovery removed hive endpoint (source={source}, reason={reason}).");
            return;
        }

        if (string.Equals(key, ServiceEndpointSettings.ReproductionManagerKey, StringComparison.Ordinal))
        {
            _reproPid = _configuredReproPid;
            Console.WriteLine($"[WARN] IO discovery removed repro endpoint (source={source}, reason={reason}).");
            return;
        }

        if (string.Equals(key, ServiceEndpointSettings.SpeciationManagerKey, StringComparison.Ordinal))
        {
            _speciationPid = _configuredSpeciationPid;
            Console.WriteLine($"[WARN] IO discovery removed speciation endpoint (source={source}, reason={reason}).");
        }
    }

    private static bool IsDiscoveryKey(string? key)
        => string.Equals(key, ServiceEndpointSettings.HiveMindKey, StringComparison.Ordinal)
           || string.Equals(key, ServiceEndpointSettings.ReproductionManagerKey, StringComparison.Ordinal)
           || string.Equals(key, ServiceEndpointSettings.SpeciationManagerKey, StringComparison.Ordinal);

    private static bool TryMarkDiscoveryKey(
        string? key,
        ref bool hiveSeen,
        ref bool reproSeen,
        ref bool speciationSeen)
    {
        if (string.Equals(key, ServiceEndpointSettings.HiveMindKey, StringComparison.Ordinal))
        {
            hiveSeen = true;
            return true;
        }

        if (string.Equals(key, ServiceEndpointSettings.ReproductionManagerKey, StringComparison.Ordinal))
        {
            reproSeen = true;
            return true;
        }

        if (string.Equals(key, ServiceEndpointSettings.SpeciationManagerKey, StringComparison.Ordinal))
        {
            speciationSeen = true;
            return true;
        }

        return false;
    }

    private static string NormalizeFailureReason(string? value, string fallback)
        => string.IsNullOrWhiteSpace(value) ? fallback : value.Trim();

    private async Task HandleSpawnBrain(IContext context, SpawnBrainViaIO message)
    {
        if (_hiveMindPid is null)
        {
            var ack = BuildSpawnFailureAck(
                reasonCode: "spawn_unavailable",
                failureMessage: "Spawn failed: HiveMind endpoint is not configured.");
            context.Respond(new SpawnBrainViaIOAck
            {
                Ack = ack,
                FailureReasonCode = ack.FailureReasonCode,
                FailureMessage = ack.FailureMessage
            });
            return;
        }

        try
        {
            var ack = await context.RequestAsync<ProtoControl.SpawnBrainAck>(_hiveMindPid, message.Request, SpawnRequestTimeout);
            if (ack is null)
            {
                ack = BuildSpawnFailureAck(
                    reasonCode: "spawn_empty_response",
                    failureMessage: "Spawn failed: HiveMind returned an empty spawn acknowledgment.");
            }

            context.Respond(new SpawnBrainViaIOAck
            {
                Ack = ack,
                FailureReasonCode = ack.FailureReasonCode,
                FailureMessage = ack.FailureMessage
            });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"SpawnBrainViaIO failed: {ex.Message}");
            var ack = BuildSpawnFailureAck(
                reasonCode: "spawn_request_failed",
                failureMessage: $"Spawn failed: request forwarding to HiveMind failed ({ex.GetBaseException().Message}).");
            context.Respond(new SpawnBrainViaIOAck
            {
                Ack = ack,
                FailureReasonCode = ack.FailureReasonCode,
                FailureMessage = ack.FailureMessage
            });
        }
    }

    private async Task HandleBrainInfoAsync(IContext context, BrainInfoRequest message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            return;
        }

        if (!_brains.TryGetValue(brainId, out var entry))
        {
            entry = await EnsureBrainEntryAsync(context, brainId, BrainInfoResolveTimeout).ConfigureAwait(false);
        }

        if (entry is null)
        {
            var missing = new BrainInfo
            {
                BrainId = message.BrainId,
                InputWidth = 0,
                OutputWidth = 0,
                CostEnabled = false,
                EnergyEnabled = false,
                EnergyRemaining = 0,
                PlasticityEnabled = true,
                EnergyRateUnitsPerSecond = 0,
                PlasticityRate = DefaultPlasticityRate,
                PlasticityProbabilisticUpdates = true,
                HomeostasisEnabled = true,
                HomeostasisTargetMode = ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero,
                HomeostasisUpdateMode = ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
                HomeostasisBaseProbability = 0.01f,
                HomeostasisMinStepCodes = 1,
                HomeostasisEnergyCouplingEnabled = false,
                HomeostasisEnergyTargetScale = 1f,
                HomeostasisEnergyProbabilityScale = 1f,
                PlasticityDelta = DefaultPlasticityDelta,
                PlasticityRebaseThreshold = 0,
                PlasticityRebaseThresholdPct = 0f,
                PlasticityEnergyCostModulationEnabled = false,
                PlasticityEnergyCostReferenceTickCost = DefaultPlasticityEnergyCostReferenceTickCost,
                PlasticityEnergyCostResponseStrength = DefaultPlasticityEnergyCostResponseStrength,
                PlasticityEnergyCostMinScale = DefaultPlasticityEnergyCostMinScale,
                PlasticityEnergyCostMaxScale = DefaultPlasticityEnergyCostMaxScale,
                InputCoordinatorMode = DefaultInputCoordinatorMode,
                OutputVectorSource = DefaultOutputVectorSource,
                LastTickCost = 0,
                BaseDefinition = new ArtifactRef(),
                LastSnapshot = new ArtifactRef()
            };
            context.Respond(missing);

            if (LogMetadataDiagnostics)
            {
                Console.WriteLine(
                    $"[IoGatewayMeta] BrainInfo missing brain={brainId} requester={PidLabel(context.Sender)}");
            }
            return;
        }

        entry.Energy.Accrue();
        var response = new BrainInfo
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
            PlasticityDelta = entry.Energy.PlasticityDelta,
            PlasticityRebaseThreshold = entry.Energy.PlasticityRebaseThreshold,
            PlasticityRebaseThresholdPct = entry.Energy.PlasticityRebaseThresholdPct,
            PlasticityEnergyCostModulationEnabled = entry.Energy.PlasticityEnergyCostModulationEnabled,
            PlasticityEnergyCostReferenceTickCost = entry.Energy.PlasticityEnergyCostReferenceTickCost,
            PlasticityEnergyCostResponseStrength = entry.Energy.PlasticityEnergyCostResponseStrength,
            PlasticityEnergyCostMinScale = entry.Energy.PlasticityEnergyCostMinScale,
            PlasticityEnergyCostMaxScale = entry.Energy.PlasticityEnergyCostMaxScale,
            HomeostasisEnabled = entry.Energy.HomeostasisEnabled,
            HomeostasisTargetMode = entry.Energy.HomeostasisTargetMode,
            HomeostasisUpdateMode = entry.Energy.HomeostasisUpdateMode,
            HomeostasisBaseProbability = entry.Energy.HomeostasisBaseProbability,
            HomeostasisMinStepCodes = entry.Energy.HomeostasisMinStepCodes,
            HomeostasisEnergyCouplingEnabled = entry.Energy.HomeostasisEnergyCouplingEnabled,
            HomeostasisEnergyTargetScale = entry.Energy.HomeostasisEnergyTargetScale,
            HomeostasisEnergyProbabilityScale = entry.Energy.HomeostasisEnergyProbabilityScale,
            LastTickCost = entry.Energy.LastTickCost,
            InputCoordinatorMode = entry.InputCoordinatorMode,
            OutputVectorSource = entry.OutputVectorSource,
            BaseDefinition = entry.BaseDefinition ?? new ArtifactRef(),
            LastSnapshot = entry.LastSnapshot ?? new ArtifactRef()
        };
        context.Respond(response);

        if (LogMetadataDiagnostics && !HasArtifactRef(response.BaseDefinition))
        {
            Console.WriteLine(
                $"[IoGatewayMeta] BrainInfo no-base brain={brainId} requester={PidLabel(context.Sender)} input={response.InputWidth} output={response.OutputWidth} snapshot={ArtifactLabel(response.LastSnapshot)}");
        }
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
            TrackInputState(entry, message);
            await DispatchCoordinatorMessageAsync(context, entry.InputPid, message).ConfigureAwait(false);
        }

        if (_hiveMindPid is null)
        {
            return;
        }

        var routerPid = await ResolveRouterPidAsync(context, brainId, allowCached: false).ConfigureAwait(false);
        if (LogInputDiagnostics)
        {
            Console.WriteLine(
                $"[IoGatewayInput] forward type={message.GetType().Name} brain={brainId} input={PidLabel(entry?.InputPid)} router={PidLabel(routerPid)} payload={DescribeInputPayload(message)}");
        }

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

        var routerPid = await ResolveRouterPidAsync(context, brainId, allowCached: false).ConfigureAwait(false);
        if (routerPid is null)
        {
            return;
        }

        context.Send(routerPid, message);
    }

    private async Task ForwardOutputAsync(IContext context, object message)
    {
        if (!TryGetBrainId(message, out var brainId))
        {
            RespondOutputCommandAck(context, message, Guid.Empty.ToProtoUuid(), success: false, "brain_id_invalid");
            return;
        }

        if (!TryGetBrainEntry(message, out var entry))
        {
            TrackPendingOutputSubscription(context, brainId, message);
            entry = await EnsureBrainEntryAsync(context, brainId).ConfigureAwait(false);
        }

        if (entry is null)
        {
            if (LogOutput)
            {
                Console.WriteLine($"IoGateway output forward dropped type={message.GetType().Name} sender={PidLabel(context.Sender)} reason=brain_entry_missing.");
            }

            RespondOutputCommandAck(context, message, brainId.ToProtoUuid(), success: true, "queued");
            return;
        }

        var replayedPendingSubscriptions = MergePendingOutputSubscriptions(entry);
        TrackOutputSubscription(context, entry, message);

        if (LogOutput)
        {
            var subscriberActor = message switch
            {
                SubscribeOutputs subscribe => subscribe.SubscriberActor,
                UnsubscribeOutputs unsubscribe => unsubscribe.SubscriberActor,
                SubscribeOutputsVector subscribeVector => subscribeVector.SubscriberActor,
                UnsubscribeOutputsVector unsubscribeVector => unsubscribeVector.SubscriberActor,
                _ => string.Empty
            };
            Console.WriteLine(
                $"IoGateway output forward type={message.GetType().Name} sender={PidLabel(context.Sender)} subscriber={subscriberActor} target={PidLabel(entry.OutputPid)}.");
        }

        try
        {
            if (replayedPendingSubscriptions)
            {
                await ReplayOutputSubscriptionsAsync(context, entry).ConfigureAwait(false);
            }

            var ack = await DispatchCoordinatorMessageAsync(
                    context,
                    entry.OutputPid,
                    NormalizeOutputSubscriptionMessage(context, message))
                .ConfigureAwait(false);
            if (context.Sender is not null)
            {
                context.Respond(ack);
            }
        }
        catch (Exception ex)
        {
            if (LogOutput)
            {
                Console.WriteLine(
                    $"IoGateway output forward failed type={message.GetType().Name} brain={brainId} sender={PidLabel(context.Sender)} error={ex.GetBaseException().Message}.");
            }

            RespondOutputCommandAck(context, message, brainId.ToProtoUuid(), success: false, ex.GetBaseException().Message);
        }
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
            if (LogInputDiagnostics)
            {
                Console.WriteLine(
                    $"[IoGatewayInput] drain request brain={brainId} tick={message.TickId} input={PidLabel(entry.InputPid)}");
            }

            var drain = await context.RequestAsync<InputDrain>(entry.InputPid, message, DefaultRequestTimeout);
            entry.InputState.ApplyDrain(drain);
            if (LogInputDiagnostics)
            {
                Console.WriteLine(
                    $"[IoGatewayInput] drain response brain={brainId} tick={message.TickId} contribs={drain.Contribs.Count} input={PidLabel(entry.InputPid)}");
            }

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

    private void ApplyEnergyCredit(IContext context, EnergyCredit message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            RespondCommandAck(context, message.BrainId, "energy_credit", success: false, "brain_not_found");
            return;
        }

        entry.Energy.ApplyCredit(message.Amount);
        if (entry.Energy.EnergyRemaining >= 0)
        {
            entry.EnergyDepletedSignaled = false;
        }

        RespondCommandAck(context, message.BrainId, "energy_credit", success: true, "applied", entry);
    }

    private void ApplyEnergyRate(IContext context, EnergyRate message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            RespondCommandAck(context, message.BrainId, "energy_rate", success: false, "brain_not_found");
            return;
        }

        entry.Energy.SetEnergyRate(message.UnitsPerSecond);
        RespondCommandAck(context, message.BrainId, "energy_rate", success: true, "applied", entry);
    }

    private void ApplyCostEnergyFlags(IContext context, SetCostEnergyEnabled message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            RespondCommandAck(context, message.BrainId, "set_cost_energy", success: false, "brain_not_found");
            return;
        }

        var enabled = message.CostEnabled && message.EnergyEnabled;
        entry.Energy.SetCostEnergyEnabled(enabled, enabled);
        Console.WriteLine(
            $"Cost/Energy override applied for brain {entry.BrainId}: enabled={enabled} remaining={entry.Energy.EnergyRemaining} rate={entry.Energy.EnergyRateUnitsPerSecond}/s");

        var ackMessage = "applied";
        if (_hiveMindPid is not null)
        {
            context.Request(_hiveMindPid, new ProtoControl.SetBrainCostEnergy
            {
                BrainId = message.BrainId,
                CostEnabled = enabled,
                EnergyEnabled = enabled
            });
        }
        else
        {
            ackMessage = "applied_local_only_hivemind_unavailable";
        }

        RespondCommandAck(context, message.BrainId, "set_cost_energy", success: true, ackMessage, entry);
    }

    private void ApplyPlasticityFlags(IContext context, SetPlasticityEnabled message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            RespondCommandAck(context, message.BrainId, "set_plasticity", success: false, "brain_not_found");
            return;
        }

        if (!float.IsFinite(message.PlasticityRate) || message.PlasticityRate < 0f)
        {
            RespondCommandAck(context, message.BrainId, "set_plasticity", success: false, "plasticity_rate_invalid", entry);
            return;
        }

        if (!float.IsFinite(message.PlasticityDelta) || message.PlasticityDelta < 0f)
        {
            RespondCommandAck(context, message.BrainId, "set_plasticity", success: false, "plasticity_delta_invalid", entry);
            return;
        }

        if (!IsFiniteInRange(message.PlasticityRebaseThresholdPct, 0f, 1f))
        {
            RespondCommandAck(context, message.BrainId, "set_plasticity", success: false, "plasticity_rebase_threshold_pct_invalid", entry);
            return;
        }

        if (!TryNormalizePlasticityEnergyCostModulation(
                message.PlasticityEnergyCostModulationEnabled,
                message.PlasticityEnergyCostReferenceTickCost,
                message.PlasticityEnergyCostResponseStrength,
                message.PlasticityEnergyCostMinScale,
                message.PlasticityEnergyCostMaxScale,
                out var modulationReferenceTickCost,
                out var modulationResponseStrength,
                out var modulationMinScale,
                out var modulationMaxScale))
        {
            RespondCommandAck(context, message.BrainId, "set_plasticity", success: false, "plasticity_energy_cost_modulation_invalid", entry);
            return;
        }

        var effectiveDelta = ResolvePlasticityDelta(message.PlasticityRate, message.PlasticityDelta);
        var configuredPlasticityEnabled = message.PlasticityEnabled;
        var effectivePlasticityEnabled = entry.Energy.PlasticityEnabled;
        var ackMessage = "accepted_pending_authoritative_state";
        if (_hiveMindPid is not null)
        {
            context.Request(_hiveMindPid, new ProtoControl.SetBrainPlasticity
            {
                BrainId = message.BrainId,
                PlasticityEnabled = message.PlasticityEnabled,
                PlasticityRate = message.PlasticityRate,
                ProbabilisticUpdates = message.ProbabilisticUpdates,
                PlasticityDelta = effectiveDelta,
                PlasticityRebaseThreshold = message.PlasticityRebaseThreshold,
                PlasticityRebaseThresholdPct = message.PlasticityRebaseThresholdPct,
                PlasticityEnergyCostModulationEnabled = message.PlasticityEnergyCostModulationEnabled,
                PlasticityEnergyCostReferenceTickCost = modulationReferenceTickCost,
                PlasticityEnergyCostResponseStrength = modulationResponseStrength,
                PlasticityEnergyCostMinScale = modulationMinScale,
                PlasticityEnergyCostMaxScale = modulationMaxScale
            });
        }
        else
        {
            entry.Energy.SetPlasticity(
                message.PlasticityEnabled,
                message.PlasticityRate,
                message.ProbabilisticUpdates,
                effectiveDelta,
                message.PlasticityRebaseThreshold,
                message.PlasticityRebaseThresholdPct,
                message.PlasticityEnergyCostModulationEnabled,
                modulationReferenceTickCost,
                modulationResponseStrength,
                modulationMinScale,
                modulationMaxScale);
            ackMessage = "applied_local_only_hivemind_unavailable";
            effectivePlasticityEnabled = entry.Energy.PlasticityEnabled;
        }

        RespondCommandAck(
            context,
            message.BrainId,
            "set_plasticity",
            success: true,
            ackMessage,
            entry,
            configuredPlasticityEnabled,
            effectivePlasticityEnabled);
    }

    private void ApplyHomeostasisFlags(IContext context, SetHomeostasisEnabled message)
    {
        if (!TryGetBrainEntry(message, out var entry))
        {
            RespondCommandAck(context, message.BrainId, "set_homeostasis", success: false, "brain_not_found");
            return;
        }

        if (!float.IsFinite(message.HomeostasisBaseProbability)
            || message.HomeostasisBaseProbability < 0f
            || message.HomeostasisBaseProbability > 1f)
        {
            RespondCommandAck(context, message.BrainId, "set_homeostasis", success: false, "homeostasis_probability_invalid", entry);
            return;
        }

        if (!IsSupportedHomeostasisTargetMode(message.HomeostasisTargetMode))
        {
            RespondCommandAck(context, message.BrainId, "set_homeostasis", success: false, "homeostasis_target_mode_invalid", entry);
            return;
        }

        if (message.HomeostasisUpdateMode != ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep)
        {
            RespondCommandAck(context, message.BrainId, "set_homeostasis", success: false, "homeostasis_update_mode_invalid", entry);
            return;
        }

        if (message.HomeostasisMinStepCodes == 0)
        {
            RespondCommandAck(context, message.BrainId, "set_homeostasis", success: false, "homeostasis_min_step_codes_invalid", entry);
            return;
        }

        if (!IsFiniteInRange(message.HomeostasisEnergyTargetScale, 0f, 4f))
        {
            RespondCommandAck(context, message.BrainId, "set_homeostasis", success: false, "homeostasis_energy_target_scale_invalid", entry);
            return;
        }

        if (!IsFiniteInRange(message.HomeostasisEnergyProbabilityScale, 0f, 4f))
        {
            RespondCommandAck(context, message.BrainId, "set_homeostasis", success: false, "homeostasis_energy_probability_scale_invalid", entry);
            return;
        }

        entry.Energy.SetHomeostasis(
            message.HomeostasisEnabled,
            message.HomeostasisTargetMode,
            message.HomeostasisUpdateMode,
            message.HomeostasisBaseProbability,
            message.HomeostasisMinStepCodes,
            message.HomeostasisEnergyCouplingEnabled,
            message.HomeostasisEnergyTargetScale,
            message.HomeostasisEnergyProbabilityScale);

        var ackMessage = "applied";
        if (_hiveMindPid is not null)
        {
            context.Request(_hiveMindPid, new ProtoControl.SetBrainHomeostasis
            {
                BrainId = message.BrainId,
                HomeostasisEnabled = message.HomeostasisEnabled,
                HomeostasisTargetMode = message.HomeostasisTargetMode,
                HomeostasisUpdateMode = message.HomeostasisUpdateMode,
                HomeostasisBaseProbability = message.HomeostasisBaseProbability,
                HomeostasisMinStepCodes = message.HomeostasisMinStepCodes,
                HomeostasisEnergyCouplingEnabled = message.HomeostasisEnergyCouplingEnabled,
                HomeostasisEnergyTargetScale = message.HomeostasisEnergyTargetScale,
                HomeostasisEnergyProbabilityScale = message.HomeostasisEnergyProbabilityScale
            });
        }
        else
        {
            ackMessage = "applied_local_only_hivemind_unavailable";
        }

        RespondCommandAck(context, message.BrainId, "set_homeostasis", success: true, ackMessage, entry);
    }

    private void ApplyTickCost(IContext context, ApplyTickCost message)
    {
        if (!_brains.TryGetValue(message.BrainId, out var entry))
        {
            return;
        }

        if (entry.LastAppliedTickCostId is ulong lastAppliedTickCostId && message.TickId <= lastAppliedTickCostId)
        {
            return;
        }

        entry.LastAppliedTickCostId = message.TickId;

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
        Console.WriteLine(
            $"Energy depleted for brain {entry.BrainId}: tick={message.TickId} remaining={entry.Energy.EnergyRemaining} last_tick_cost={entry.Energy.LastTickCost} rate={entry.Energy.EnergyRateUnitsPerSecond}/s");

        if (_hiveMindPid is null)
        {
            var terminated = BuildEnergyTerminated(entry, message.TickCost);
            BroadcastToClients(context, terminated);
            StopAndRemoveBrain(context, entry);
            return;
        }

        context.Request(_hiveMindPid, new ProtoControl.KillBrain
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

        var inputCoordinatorMode = NormalizeInputCoordinatorMode(message.InputCoordinatorMode);
        var outputVectorSource = NormalizeOutputVectorSource(message.OutputVectorSource);
        var hasRegisteredInputPid = TryParsePid(message.InputCoordinatorPid, out var registeredInputPid)
                                    && registeredInputPid is not null;
        var hasRegisteredOutputPid = TryParsePid(message.OutputCoordinatorPid, out var registeredOutputPid)
                                     && registeredOutputPid is not null;
        var registeredInputOwned = hasRegisteredInputPid && message.IoGatewayOwnsInputCoordinator;
        var registeredOutputOwned = hasRegisteredOutputPid && message.IoGatewayOwnsOutputCoordinator;
        var shouldRegisterOutputSink = hasRegisteredOutputPid || message.IoGatewayOwnsOutputCoordinator;

        if (_brains.TryGetValue(brainId, out var existing))
        {
            var inputWidthChanged = false;
            var outputWidthChanged = false;
            if (existing.InputWidth != message.InputWidth || existing.OutputWidth != message.OutputWidth)
            {
                if (message.InputWidth > existing.InputWidth && message.InputWidth > 0)
                {
                    existing.InputWidth = message.InputWidth;
                    inputWidthChanged = true;
                }

                if (message.OutputWidth > existing.OutputWidth && message.OutputWidth > 0)
                {
                    existing.OutputWidth = message.OutputWidth;
                    outputWidthChanged = true;
                }

                if (existing.InputWidth != message.InputWidth && existing.OutputWidth != message.OutputWidth)
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
                existing.EnergyDepletedSignaled = false;
                existing.LastAppliedTickCostId = null;
                existing.RegisteredAtMs = NowMs();
            }

            if (message.HasRuntimeConfig)
            {
                var effectiveDelta = ResolvePlasticityDelta(message.PlasticityRate, message.PlasticityDelta);
                existing.Energy.SetRuntimeConfig(
                    message.CostEnabled,
                    message.EnergyEnabled,
                    message.PlasticityEnabled,
                    message.PlasticityRate,
                    message.PlasticityProbabilisticUpdates,
                    effectiveDelta,
                    message.PlasticityRebaseThreshold,
                    message.PlasticityRebaseThresholdPct,
                    message.PlasticityEnergyCostModulationEnabled,
                    message.PlasticityEnergyCostReferenceTickCost,
                    message.PlasticityEnergyCostResponseStrength,
                    message.PlasticityEnergyCostMinScale,
                    message.PlasticityEnergyCostMaxScale,
                    message.HomeostasisEnabled,
                    message.HomeostasisTargetMode,
                    message.HomeostasisUpdateMode,
                    message.HomeostasisBaseProbability,
                    message.HomeostasisMinStepCodes,
                    message.HomeostasisEnergyCouplingEnabled,
                    message.HomeostasisEnergyTargetScale,
                    message.HomeostasisEnergyProbabilityScale,
                    message.LastTickCost);
            }

            var inputModeChanged = existing.InputCoordinatorMode != inputCoordinatorMode;
            existing.InputCoordinatorMode = inputCoordinatorMode;
            existing.OutputVectorSource = outputVectorSource;
            existing.InputState.UpdateConfiguration(existing.InputWidth, inputCoordinatorMode);

            var inputCoordinatorChanged = false;
            if (hasRegisteredInputPid && registeredInputPid is not null)
            {
                inputCoordinatorChanged = UpdateCoordinatorReference(
                    context,
                    existing.InputPid,
                    existing.OwnsInputCoordinator,
                    registeredInputPid,
                    registeredInputOwned,
                    out var updatedInputPid,
                    out var updatedOwnsInputCoordinator);
                existing.InputPid = updatedInputPid;
                existing.OwnsInputCoordinator = updatedOwnsInputCoordinator;
            }

            var outputCoordinatorChanged = false;
            if (hasRegisteredOutputPid && registeredOutputPid is not null)
            {
                outputCoordinatorChanged = UpdateCoordinatorReference(
                    context,
                    existing.OutputPid,
                    existing.OwnsOutputCoordinator,
                    registeredOutputPid,
                    registeredOutputOwned,
                    out var updatedOutputPid,
                    out var updatedOwnsOutputCoordinator);
                existing.OutputPid = updatedOutputPid;
                existing.OwnsOutputCoordinator = updatedOwnsOutputCoordinator;
            }

            if (inputWidthChanged || inputCoordinatorChanged)
            {
                await DispatchCoordinatorMessageAsync(
                        context,
                        existing.InputPid,
                        new UpdateInputWidth(existing.InputWidth))
                    .ConfigureAwait(false);
            }

            if (inputModeChanged || inputCoordinatorChanged)
            {
                await DispatchCoordinatorMessageAsync(
                        context,
                        existing.InputPid,
                        new UpdateInputCoordinatorMode(inputCoordinatorMode))
                    .ConfigureAwait(false);
            }

            if (outputWidthChanged || outputCoordinatorChanged)
            {
                await DispatchCoordinatorMessageAsync(
                        context,
                        existing.OutputPid,
                        new UpdateOutputWidth(existing.OutputWidth))
                    .ConfigureAwait(false);
            }

            if (inputCoordinatorChanged)
            {
                await ReplayInputStateAsync(context, existing).ConfigureAwait(false);
            }

            if (outputCoordinatorChanged)
            {
                await ReplayOutputSubscriptionsAsync(context, existing).ConfigureAwait(false);
            }

            if (MergePendingOutputSubscriptions(existing))
            {
                await ReplayOutputSubscriptionsAsync(context, existing).ConfigureAwait(false);
            }

            if (LogMetadataDiagnostics)
            {
                Console.WriteLine(
                    $"[IoGatewayMeta] RegisterBrain update brain={brainId} input={existing.InputWidth} output={existing.OutputWidth} base={ArtifactLabel(existing.BaseDefinition)} snapshot={ArtifactLabel(existing.LastSnapshot)} runtimeConfig={message.HasRuntimeConfig}");
            }

            await EnsureIoGatewayRegisteredAsync(context, brainId);
            await EnsureOutputSinkRegisteredAsync(context, brainId, existing.OutputPid, shouldRegisterOutputSink);
            return;
        }

        var ownsInputCoordinator = !hasRegisteredInputPid || registeredInputOwned;
        var ownsOutputCoordinator = !hasRegisteredOutputPid || registeredOutputOwned;
        var inputPid = hasRegisteredInputPid && registeredInputPid is not null
            ? registeredInputPid
            : SpawnOwnedInputCoordinator(context, brainId, message.InputWidth, inputCoordinatorMode);
        var outputPid = hasRegisteredOutputPid && registeredOutputPid is not null
            ? registeredOutputPid
            : SpawnOwnedOutputCoordinator(context, brainId, message.OutputWidth);

        var energy = new BrainEnergyState();
        if (message.EnergyState is not null)
        {
            energy.ResetFrom(message.EnergyState);
        }

        if (message.HasRuntimeConfig)
        {
            var effectiveDelta = ResolvePlasticityDelta(message.PlasticityRate, message.PlasticityDelta);
            energy.SetRuntimeConfig(
                message.CostEnabled,
                message.EnergyEnabled,
                message.PlasticityEnabled,
                message.PlasticityRate,
                message.PlasticityProbabilisticUpdates,
                effectiveDelta,
                message.PlasticityRebaseThreshold,
                message.PlasticityRebaseThresholdPct,
                message.PlasticityEnergyCostModulationEnabled,
                message.PlasticityEnergyCostReferenceTickCost,
                message.PlasticityEnergyCostResponseStrength,
                message.PlasticityEnergyCostMinScale,
                message.PlasticityEnergyCostMaxScale,
                message.HomeostasisEnabled,
                message.HomeostasisTargetMode,
                message.HomeostasisUpdateMode,
                message.HomeostasisBaseProbability,
                message.HomeostasisMinStepCodes,
                message.HomeostasisEnergyCouplingEnabled,
                message.HomeostasisEnergyTargetScale,
                message.HomeostasisEnergyProbabilityScale,
                message.LastTickCost);
        }

        var entry = new BrainIoEntry(
            brainId,
            inputPid,
            outputPid,
            ownsInputCoordinator,
            ownsOutputCoordinator,
            message.InputWidth,
            message.OutputWidth,
            energy,
            inputCoordinatorMode,
            outputVectorSource)
        {
            BaseDefinition = message.BaseDefinition,
            LastSnapshot = message.LastSnapshot
        };

        _brains.Add(brainId, entry);

        await DispatchCoordinatorMessageAsync(
                context,
                entry.InputPid,
                new UpdateInputWidth(entry.InputWidth))
            .ConfigureAwait(false);
        await DispatchCoordinatorMessageAsync(
                context,
                entry.InputPid,
                new UpdateInputCoordinatorMode(inputCoordinatorMode))
            .ConfigureAwait(false);
        await DispatchCoordinatorMessageAsync(
                context,
                entry.OutputPid,
                new UpdateOutputWidth(entry.OutputWidth))
            .ConfigureAwait(false);

        if (MergePendingOutputSubscriptions(entry))
        {
            await ReplayOutputSubscriptionsAsync(context, entry).ConfigureAwait(false);
        }

        if (LogMetadataDiagnostics)
        {
            Console.WriteLine(
                $"[IoGatewayMeta] RegisterBrain add brain={brainId} input={entry.InputWidth} output={entry.OutputWidth} base={ArtifactLabel(entry.BaseDefinition)} snapshot={ArtifactLabel(entry.LastSnapshot)} runtimeConfig={message.HasRuntimeConfig}");
        }

        await EnsureIoGatewayRegisteredAsync(context, brainId);
        await EnsureOutputSinkRegisteredAsync(context, brainId, outputPid, shouldRegisterOutputSink);
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

        var outbound = message;
        if (_brains.TryGetValue(brainId, out var entry))
        {
            if (message.TimeMs > 0 && message.TimeMs < (ulong)entry.RegisteredAtMs)
            {
                return;
            }

            if (HasArtifactRef(message.BaseDef))
            {
                entry.BaseDefinition = message.BaseDef;
            }

            if (HasArtifactRef(message.LastSnapshot))
            {
                entry.LastSnapshot = message.LastSnapshot;
            }

            outbound = BuildTerminatedFromEntry(message, entry);
            StopAndRemoveBrain(context, entry);
        }

        BroadcastToClients(context, outbound);
    }

    private void HandleExportBrainDefinition(IContext context, ExportBrainDefinition message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            context.Respond(new BrainDefinitionReady());
            if (LogMetadataDiagnostics)
            {
                Console.WriteLine("[IoGatewayMeta] ExportBrainDefinition invalid-brain-id");
            }
            return;
        }

        _brains.TryGetValue(brainId, out var entry);
        if (!message.RebaseOverlays && entry is not null && HasArtifactRef(entry.BaseDefinition))
        {
            if (LogMetadataDiagnostics)
            {
                Console.WriteLine(
                    $"[IoGatewayMeta] ExportBrainDefinition local-hit brain={brainId} base={ArtifactLabel(entry.BaseDefinition)} rebase={message.RebaseOverlays}");
            }

            context.Respond(new BrainDefinitionReady
            {
                BrainId = message.BrainId,
                BrainDef = entry.BaseDefinition
            });
            return;
        }

        if (_hiveMindPid is null)
        {
            if (entry is not null && HasArtifactRef(entry.BaseDefinition))
            {
                if (LogMetadataDiagnostics)
                {
                    Console.WriteLine(
                        $"[IoGatewayMeta] ExportBrainDefinition hive-missing local-fallback brain={brainId} base={ArtifactLabel(entry.BaseDefinition)}");
                }

                context.Respond(new BrainDefinitionReady
                {
                    BrainId = message.BrainId,
                    BrainDef = entry.BaseDefinition
                });
            }
            else
            {
                if (LogMetadataDiagnostics)
                {
                    Console.WriteLine(
                        $"[IoGatewayMeta] ExportBrainDefinition empty brain={brainId} reason=hive-missing-local-missing");
                }

                context.Respond(new BrainDefinitionReady { BrainId = message.BrainId });
            }
            return;
        }

        var exportTask = context.RequestAsync<BrainDefinitionReady>(_hiveMindPid, message, ExportRequestTimeout);
        context.ReenterAfter(exportTask, completed =>
        {
            if (completed.IsCompletedSuccessfully)
            {
                var ready = completed.Result;
                if (ready is not null
                    && HasArtifactRef(ready.BrainDef)
                    && !message.RebaseOverlays
                    && _brains.TryGetValue(brainId, out var existing))
                {
                    existing.BaseDefinition = ready.BrainDef;
                }

                if (ready is not null)
                {
                    if (LogMetadataDiagnostics)
                    {
                        Console.WriteLine(
                            $"[IoGatewayMeta] ExportBrainDefinition hive-response brain={brainId} base={ArtifactLabel(ready.BrainDef)}");
                    }

                    context.Respond(ready);
                    return Task.CompletedTask;
                }
            }

            if (completed.IsFaulted || completed.IsCanceled)
            {
                var detail = completed.Exception?.GetBaseException().Message ?? "request canceled";
                Console.WriteLine($"ExportBrainDefinition failed for {brainId}: {detail}");
            }

            if (entry is not null && HasArtifactRef(entry.BaseDefinition))
            {
                if (LogMetadataDiagnostics)
                {
                    Console.WriteLine(
                        $"[IoGatewayMeta] ExportBrainDefinition fallback-local brain={brainId} base={ArtifactLabel(entry.BaseDefinition)}");
                }

                context.Respond(new BrainDefinitionReady
                {
                    BrainId = message.BrainId,
                    BrainDef = entry.BaseDefinition
                });
                return Task.CompletedTask;
            }

            if (LogMetadataDiagnostics)
            {
                Console.WriteLine(
                    $"[IoGatewayMeta] ExportBrainDefinition empty brain={brainId} reason=hive-failed-local-missing");
            }

            context.Respond(new BrainDefinitionReady { BrainId = message.BrainId });
            return Task.CompletedTask;
        });
    }

    private async Task HandleRequestSnapshotAsync(IContext context, RequestSnapshot message)
    {
        if (!TryGetBrainId(message.BrainId, out var brainId))
        {
            context.Respond(new SnapshotReady());
            return;
        }

        _brains.TryGetValue(brainId, out var entry);

        if (_hiveMindPid is null)
        {
            if (entry is not null && HasArtifactRef(entry.LastSnapshot))
            {
                context.Respond(new SnapshotReady
                {
                    BrainId = message.BrainId,
                    Snapshot = entry.LastSnapshot
                });
                return;
            }

            context.Respond(new SnapshotReady { BrainId = message.BrainId });
            return;
        }

        try
        {
            var request = new RequestSnapshot
            {
                BrainId = message.BrainId
            };

            if (entry is not null)
            {
                request.HasRuntimeState = true;
                request.EnergyRemaining = entry.Energy.EnergyRemaining;
                request.CostEnabled = entry.Energy.CostEnabled;
                request.EnergyEnabled = entry.Energy.EnergyEnabled;
                request.PlasticityEnabled = entry.Energy.PlasticityEnabled;
            }

            var ready = await context.RequestAsync<SnapshotReady>(_hiveMindPid, request, DefaultRequestTimeout).ConfigureAwait(false);
            if (ready is not null
                && HasArtifactRef(ready.Snapshot)
                && _brains.TryGetValue(brainId, out var existing))
            {
                existing.LastSnapshot = ready.Snapshot;
            }

            if (ready is not null && HasArtifactRef(ready.Snapshot))
            {
                context.Respond(ready);
                return;
            }

            if (entry is not null && HasArtifactRef(entry.LastSnapshot))
            {
                context.Respond(new SnapshotReady
                {
                    BrainId = message.BrainId,
                    Snapshot = entry.LastSnapshot
                });
                return;
            }

            context.Respond(new SnapshotReady { BrainId = message.BrainId });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"RequestSnapshot failed for {brainId}: {ex.Message}");
            if (entry is not null && HasArtifactRef(entry.LastSnapshot))
            {
                context.Respond(new SnapshotReady
                {
                    BrainId = message.BrainId,
                    Snapshot = entry.LastSnapshot
                });
                return;
            }

            context.Respond(new SnapshotReady { BrainId = message.BrainId });
        }
    }

    private static ProtoControl.SpawnBrainAck BuildSpawnFailureAck(string reasonCode, string failureMessage)
    {
        var normalizedReasonCode = string.IsNullOrWhiteSpace(reasonCode)
            ? "spawn_failed"
            : reasonCode.Trim();
        var normalizedFailureMessage = string.IsNullOrWhiteSpace(failureMessage)
            ? "Spawn failed."
            : failureMessage.Trim();
        return new ProtoControl.SpawnBrainAck
        {
            BrainId = Guid.Empty.ToProtoUuid(),
            FailureReasonCode = normalizedReasonCode,
            FailureMessage = normalizedFailureMessage
        };
    }

    private static bool HasArtifactRef(ArtifactRef? reference)
        => reference is not null
           && reference.Sha256 is not null
           && reference.Sha256.Value is not null
           && reference.Sha256.Value.Length == 32;

    private static string ArtifactLabel(ArtifactRef? reference)
    {
        if (!HasArtifactRef(reference))
        {
            return "missing";
        }

        return reference!.TryToSha256Hex(out var sha)
            ? sha[..Math.Min(12, sha.Length)]
            : "present";
    }

    private void HandleReproduceByBrainIds(IContext context, ReproduceByBrainIds message)
    {
        if (_reproPid is null)
        {
            context.Respond(CreateReproFailure("repro_unavailable"));
            return;
        }

        var reproTask = context.RequestAsync<Nbn.Proto.Repro.ReproduceResult>(_reproPid, message.Request, ReproRequestTimeout);
        context.ReenterAfter(reproTask, completed =>
        {
            if (completed.IsCompletedSuccessfully)
            {
                var result = completed.Result;
                if (result is null)
                {
                    context.Respond(CreateReproFailure("repro_empty_response"));
                    return Task.CompletedTask;
                }

                result.Report ??= CreateAbortReport("repro_missing_report");
                EnsureSimilarityScore(result);
                context.Respond(new Nbn.Proto.Io.ReproduceResult { Result = result });
                return Task.CompletedTask;
            }

            var detail = completed.Exception?.GetBaseException().Message ?? "request canceled";
            Console.WriteLine($"ReproduceByBrainIds failed: {detail}");
            context.Respond(CreateReproFailure("repro_request_failed"));
            return Task.CompletedTask;
        });
    }

    private void HandleReproduceByArtifacts(IContext context, ReproduceByArtifacts message)
    {
        if (_reproPid is null)
        {
            context.Respond(CreateReproFailure("repro_unavailable"));
            return;
        }

        var reproTask = context.RequestAsync<Nbn.Proto.Repro.ReproduceResult>(_reproPid, message.Request, ReproRequestTimeout);
        context.ReenterAfter(reproTask, completed =>
        {
            if (completed.IsCompletedSuccessfully)
            {
                var result = completed.Result;
                if (result is null)
                {
                    context.Respond(CreateReproFailure("repro_empty_response"));
                    return Task.CompletedTask;
                }

                result.Report ??= CreateAbortReport("repro_missing_report");
                EnsureSimilarityScore(result);
                context.Respond(new Nbn.Proto.Io.ReproduceResult { Result = result });
                return Task.CompletedTask;
            }

            var detail = completed.Exception?.GetBaseException().Message ?? "request canceled";
            Console.WriteLine($"ReproduceByArtifacts failed: {detail}");
            context.Respond(CreateReproFailure("repro_request_failed"));
            return Task.CompletedTask;
        });
    }

    private void HandleAssessCompatibilityByBrainIds(IContext context, AssessCompatibilityByBrainIds message)
    {
        if (_reproPid is null)
        {
            context.Respond(CreateAssessmentFailure("repro_unavailable"));
            return;
        }

        var reproTask = context.RequestAsync<Nbn.Proto.Repro.ReproduceResult>(_reproPid, message.Request, ReproRequestTimeout);
        context.ReenterAfter(reproTask, completed =>
        {
            if (completed.IsCompletedSuccessfully)
            {
                var result = completed.Result;
                if (result is null)
                {
                    context.Respond(CreateAssessmentFailure("repro_empty_response"));
                    return Task.CompletedTask;
                }

                result.Report ??= CreateAbortReport("repro_missing_report");
                EnsureSimilarityScore(result);
                context.Respond(new AssessCompatibilityResult { Result = result });
                return Task.CompletedTask;
            }

            var detail = completed.Exception?.GetBaseException().Message ?? "request canceled";
            Console.WriteLine($"AssessCompatibilityByBrainIds failed: {detail}");
            context.Respond(CreateAssessmentFailure("repro_request_failed"));
            return Task.CompletedTask;
        });
    }

    private void HandleAssessCompatibilityByArtifacts(IContext context, AssessCompatibilityByArtifacts message)
    {
        if (_reproPid is null)
        {
            context.Respond(CreateAssessmentFailure("repro_unavailable"));
            return;
        }

        var reproTask = context.RequestAsync<Nbn.Proto.Repro.ReproduceResult>(_reproPid, message.Request, ReproRequestTimeout);
        context.ReenterAfter(reproTask, completed =>
        {
            if (completed.IsCompletedSuccessfully)
            {
                var result = completed.Result;
                if (result is null)
                {
                    context.Respond(CreateAssessmentFailure("repro_empty_response"));
                    return Task.CompletedTask;
                }

                result.Report ??= CreateAbortReport("repro_missing_report");
                EnsureSimilarityScore(result);
                context.Respond(new AssessCompatibilityResult { Result = result });
                return Task.CompletedTask;
            }

            var detail = completed.Exception?.GetBaseException().Message ?? "request canceled";
            Console.WriteLine($"AssessCompatibilityByArtifacts failed: {detail}");
            context.Respond(CreateAssessmentFailure("repro_request_failed"));
            return Task.CompletedTask;
        });
    }

    private void HandleSpeciationStatus(IContext context, SpeciationStatus message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationStatusRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationStatusResult { Response = response },
            static (reason, detail) => CreateSpeciationStatusFailure(reason, detail),
            operationName: nameof(SpeciationStatus));
    }

    private void HandleSpeciationGetConfig(IContext context, SpeciationGetConfig message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationGetConfigRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationGetConfigResult { Response = response },
            static (reason, detail) => CreateSpeciationGetConfigFailure(reason, detail),
            operationName: nameof(SpeciationGetConfig));
    }

    private void HandleSpeciationSetConfig(IContext context, SpeciationSetConfig message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationSetConfigRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationSetConfigResult { Response = response },
            static (reason, detail) => CreateSpeciationSetConfigFailure(reason, detail),
            operationName: nameof(SpeciationSetConfig));
    }

    private void HandleSpeciationResetAll(IContext context, SpeciationResetAll message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationResetAllRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationResetAllResult { Response = response },
            static (reason, detail) => new ProtoSpec.SpeciationResetAllResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                PreviousEpoch = new ProtoSpec.SpeciationEpochInfo(),
                CurrentEpoch = new ProtoSpec.SpeciationEpochInfo(),
                Config = CreateDefaultSpeciationConfig(),
                DeletedEpochCount = 0,
                DeletedMembershipCount = 0,
                DeletedSpeciesCount = 0,
                DeletedDecisionCount = 0,
                DeletedLineageEdgeCount = 0
            },
            operationName: nameof(SpeciationResetAll));
    }

    private void HandleSpeciationDeleteEpoch(IContext context, SpeciationDeleteEpoch message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationDeleteEpochRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationDeleteEpochResult { Response = response },
            static (reason, detail) => new ProtoSpec.SpeciationDeleteEpochResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                EpochId = 0,
                Deleted = false,
                DeletedMembershipCount = 0,
                DeletedSpeciesCount = 0,
                DeletedDecisionCount = 0,
                DeletedLineageEdgeCount = 0,
                CurrentEpoch = new ProtoSpec.SpeciationEpochInfo()
            },
            operationName: nameof(SpeciationDeleteEpoch));
    }

    private void HandleSpeciationEvaluate(IContext context, SpeciationEvaluate message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationEvaluateRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationEvaluateResult { Response = response },
            static (reason, detail) => new ProtoSpec.SpeciationEvaluateResponse
            {
                Decision = CreateSpeciationDecisionFailure(
                    ProtoSpec.SpeciationApplyMode.DryRun,
                    reason,
                    detail)
            },
            operationName: nameof(SpeciationEvaluate));
    }

    private void HandleSpeciationAssign(IContext context, SpeciationAssign message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationAssignRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationAssignResult { Response = response },
            (reason, detail) => new ProtoSpec.SpeciationAssignResponse
            {
                Decision = CreateSpeciationDecisionFailure(
                    NormalizeSpeciationApplyMode(request.ApplyMode),
                    reason,
                    detail)
            },
            operationName: nameof(SpeciationAssign));
    }

    private void HandleSpeciationBatchEvaluateApply(IContext context, SpeciationBatchEvaluateApply message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationBatchEvaluateApplyRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationBatchEvaluateApplyResult { Response = response },
            (reason, detail) => new ProtoSpec.SpeciationBatchEvaluateApplyResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                ApplyMode = NormalizeSpeciationApplyMode(request.ApplyMode),
                RequestedCount = (uint)request.Items.Count,
                ProcessedCount = 0,
                CommittedCount = 0
            },
            operationName: nameof(SpeciationBatchEvaluateApply));
    }

    private void HandleSpeciationListMemberships(IContext context, SpeciationListMemberships message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationListMembershipsRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationListMembershipsResult { Response = response },
            static (reason, detail) => new ProtoSpec.SpeciationListMembershipsResponse
            {
                FailureReason = reason,
                FailureDetail = detail
            },
            operationName: nameof(SpeciationListMemberships));
    }

    private void HandleSpeciationQueryMembership(IContext context, SpeciationQueryMembership message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationQueryMembershipRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationQueryMembershipResult { Response = response },
            static (reason, detail) => new ProtoSpec.SpeciationQueryMembershipResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                Found = false
            },
            operationName: nameof(SpeciationQueryMembership));
    }

    private void HandleSpeciationListHistory(IContext context, SpeciationListHistory message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationListHistoryRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationListHistoryResult { Response = response },
            static (reason, detail) => new ProtoSpec.SpeciationListHistoryResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                TotalRecords = 0
            },
            operationName: nameof(SpeciationListHistory));
    }

    private void ForwardSpeciationRequest<TRequest, TResponse, TResult>(
        IContext context,
        TRequest request,
        Func<TResponse, TResult> wrapResponse,
        Func<ProtoSpec.SpeciationFailureReason, string, TResponse> createFailureResponse,
        string operationName)
        where TRequest : class
        where TResponse : class
        where TResult : class
    {
        if (_speciationPid is null)
        {
            context.Respond(
                wrapResponse(
                    createFailureResponse(
                        ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceUnavailable,
                        $"{operationName} failed: speciation manager endpoint is not configured.")));
            return;
        }

        var requestTask = context.RequestAsync<TResponse>(_speciationPid, request, SpeciationRequestTimeout);
        context.ReenterAfter(requestTask, completed =>
        {
            if (completed.IsCompletedSuccessfully)
            {
                var response = completed.Result;
                if (response is null)
                {
                    context.Respond(
                        wrapResponse(
                            createFailureResponse(
                                ProtoSpec.SpeciationFailureReason.SpeciationFailureEmptyResponse,
                                $"{operationName} failed: speciation manager returned an empty response.")));
                    return Task.CompletedTask;
                }

                context.Respond(wrapResponse(response));
                return Task.CompletedTask;
            }

            var detail = completed.Exception?.GetBaseException().Message ?? "request canceled";
            Console.WriteLine($"{operationName} failed: {detail}");
            context.Respond(
                wrapResponse(
                    createFailureResponse(
                        ProtoSpec.SpeciationFailureReason.SpeciationFailureRequestFailed,
                        $"{operationName} failed: forwarding request to speciation manager failed ({detail}).")));
            return Task.CompletedTask;
        });
    }

    private static ProtoSpec.SpeciationStatusResponse CreateSpeciationStatusFailure(
        ProtoSpec.SpeciationFailureReason reason,
        string detail)
    {
        return new ProtoSpec.SpeciationStatusResponse
        {
            FailureReason = reason,
            FailureDetail = detail,
            Status = new ProtoSpec.SpeciationStatusSnapshot(),
            CurrentEpoch = new ProtoSpec.SpeciationEpochInfo(),
            Config = CreateDefaultSpeciationConfig()
        };
    }

    private static ProtoSpec.SpeciationGetConfigResponse CreateSpeciationGetConfigFailure(
        ProtoSpec.SpeciationFailureReason reason,
        string detail)
    {
        return new ProtoSpec.SpeciationGetConfigResponse
        {
            FailureReason = reason,
            FailureDetail = detail,
            Config = CreateDefaultSpeciationConfig(),
            CurrentEpoch = new ProtoSpec.SpeciationEpochInfo()
        };
    }

    private static ProtoSpec.SpeciationSetConfigResponse CreateSpeciationSetConfigFailure(
        ProtoSpec.SpeciationFailureReason reason,
        string detail)
    {
        return new ProtoSpec.SpeciationSetConfigResponse
        {
            FailureReason = reason,
            FailureDetail = detail,
            Config = CreateDefaultSpeciationConfig(),
            PreviousEpoch = new ProtoSpec.SpeciationEpochInfo(),
            CurrentEpoch = new ProtoSpec.SpeciationEpochInfo()
        };
    }

    private static ProtoSpec.SpeciationRuntimeConfig CreateDefaultSpeciationConfig()
    {
        return new ProtoSpec.SpeciationRuntimeConfig
        {
            PolicyVersion = "unknown",
            ConfigSnapshotJson = "{}",
            DefaultSpeciesId = "default",
            DefaultSpeciesDisplayName = "Default",
            StartupReconcileDecisionReason = "startup_reconcile"
        };
    }

    private static ProtoSpec.SpeciationDecision CreateSpeciationDecisionFailure(
        ProtoSpec.SpeciationApplyMode applyMode,
        ProtoSpec.SpeciationFailureReason failureReason,
        string failureDetail)
    {
        return new ProtoSpec.SpeciationDecision
        {
            ApplyMode = applyMode,
            CandidateMode = ProtoSpec.SpeciationCandidateMode.Unknown,
            Success = false,
            Created = false,
            ImmutableConflict = false,
            FailureReason = failureReason,
            FailureDetail = failureDetail,
            SpeciesId = string.Empty,
            SpeciesDisplayName = string.Empty,
            DecisionReason = string.Empty,
            DecisionMetadataJson = "{}",
            Committed = false
        };
    }

    private static ProtoSpec.SpeciationApplyMode NormalizeSpeciationApplyMode(ProtoSpec.SpeciationApplyMode applyMode)
    {
        return applyMode == ProtoSpec.SpeciationApplyMode.Commit
            ? ProtoSpec.SpeciationApplyMode.Commit
            : ProtoSpec.SpeciationApplyMode.DryRun;
    }

    private static Nbn.Proto.Io.ReproduceResult CreateReproFailure(string reason)
        => new()
        {
            Result = new Nbn.Proto.Repro.ReproduceResult
            {
                Report = CreateAbortReport(reason),
                Summary = new MutationSummary(),
                Spawned = false
            }
        };

    private static AssessCompatibilityResult CreateAssessmentFailure(string reason)
        => new()
        {
            Result = new Nbn.Proto.Repro.ReproduceResult
            {
                Report = CreateAbortReport(reason),
                Summary = new MutationSummary(),
                Spawned = false
            }
        };

    private static SimilarityReport CreateAbortReport(string reason)
        => new()
        {
            Compatible = false,
            AbortReason = reason,
            SimilarityScore = 0f,
            RegionSpanScore = 0f,
            FunctionScore = 0f,
            ConnectivityScore = 0f
        };

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
            await ResolveRouterPidAsync(context, brainId, allowCached: false).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"RegisterIoGateway failed for {brainId}: {ex.Message}");
        }
    }

    private async Task<BrainIoEntry?> EnsureBrainEntryAsync(IContext context, Guid brainId, TimeSpan? requestTimeout = null)
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
                    requestTimeout ?? DefaultRequestTimeout)
                .ConfigureAwait(false);

            if (info is null || info.InputWidth == 0)
            {
                return null;
            }

            var register = new RegisterBrain
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = info.InputWidth,
                OutputWidth = info.OutputWidth,
                InputCoordinatorMode = info.InputCoordinatorMode,
                OutputVectorSource = info.OutputVectorSource,
                InputCoordinatorPid = info.InputCoordinatorPid,
                OutputCoordinatorPid = info.OutputCoordinatorPid,
                IoGatewayOwnsInputCoordinator = info.IoGatewayOwnsInputCoordinator,
                IoGatewayOwnsOutputCoordinator = info.IoGatewayOwnsOutputCoordinator
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

    private async Task EnsureOutputSinkRegisteredAsync(
        IContext context,
        Guid brainId,
        PID outputPid,
        bool shouldRegisterOutputSink)
    {
        if (!shouldRegisterOutputSink)
        {
            return;
        }

        if (_hiveMindPid is null)
        {
            Console.WriteLine($"RegisterOutputSink skipped (no HiveMind PID) for {brainId}");
            return;
        }

        try
        {
            var outputLabel = PidLabel(ToRemotePid(context, outputPid));
            context.Request(_hiveMindPid, new ProtoControl.RegisterOutputSink
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
            case SetHomeostasisEnabled homeostasis when TryGetBrainId(homeostasis.BrainId, out var brainId):
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
            case SetHomeostasisEnabled homeostasis:
                return TryGetBrainId(homeostasis.BrainId, out guid);
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

    private static ProtoControl.InputCoordinatorMode NormalizeInputCoordinatorMode(ProtoControl.InputCoordinatorMode mode)
    {
        return mode switch
        {
            ProtoControl.InputCoordinatorMode.ReplayLatestVector => mode,
            _ => DefaultInputCoordinatorMode
        };
    }

    private static ProtoControl.OutputVectorSource NormalizeOutputVectorSource(ProtoControl.OutputVectorSource source)
    {
        return source switch
        {
            ProtoControl.OutputVectorSource.Buffer => source,
            _ => DefaultOutputVectorSource
        };
    }

    private static bool IsSupportedHomeostasisTargetMode(ProtoControl.HomeostasisTargetMode mode)
    {
        return mode == ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero
               || mode == ProtoControl.HomeostasisTargetMode.HomeostasisTargetFixed;
    }

    private static bool IsFiniteInRange(float value, float min, float max)
    {
        return float.IsFinite(value) && value >= min && value <= max;
    }

    private static bool TryNormalizePlasticityEnergyCostModulation(
        bool enabled,
        long referenceTickCost,
        float responseStrength,
        float minScale,
        float maxScale,
        out long normalizedReferenceTickCost,
        out float normalizedResponseStrength,
        out float normalizedMinScale,
        out float normalizedMaxScale)
    {
        normalizedReferenceTickCost = DefaultPlasticityEnergyCostReferenceTickCost;
        normalizedResponseStrength = DefaultPlasticityEnergyCostResponseStrength;
        normalizedMinScale = DefaultPlasticityEnergyCostMinScale;
        normalizedMaxScale = DefaultPlasticityEnergyCostMaxScale;

        if (!enabled)
        {
            var hasExplicitConfiguration = referenceTickCost > 0
                                           || (float.IsFinite(responseStrength) && responseStrength > 0f)
                                           || (float.IsFinite(minScale) && minScale > 0f)
                                           || (float.IsFinite(maxScale) && maxScale > 0f);
            if (!hasExplicitConfiguration)
            {
                return true;
            }

            if (referenceTickCost > 0)
            {
                normalizedReferenceTickCost = referenceTickCost;
            }

            if (float.IsFinite(responseStrength) && responseStrength >= 0f)
            {
                normalizedResponseStrength = Math.Clamp(responseStrength, 0f, 8f);
            }

            var hasExplicitScale = float.IsFinite(minScale)
                                   && float.IsFinite(maxScale)
                                   && (minScale > 0f || maxScale > 0f);
            if (hasExplicitScale)
            {
                normalizedMinScale = Math.Clamp(minScale, 0f, 1f);
                normalizedMaxScale = Math.Clamp(maxScale, 0f, 1f);
                if (normalizedMaxScale < normalizedMinScale)
                {
                    normalizedMaxScale = normalizedMinScale;
                }
            }

            return true;
        }

        if (referenceTickCost <= 0
            || !IsFiniteInRange(responseStrength, 0f, 8f)
            || !IsFiniteInRange(minScale, 0f, 1f)
            || !IsFiniteInRange(maxScale, 0f, 1f)
            || maxScale < minScale)
        {
            return false;
        }

        normalizedReferenceTickCost = referenceTickCost;
        normalizedResponseStrength = responseStrength;
        normalizedMinScale = minScale;
        normalizedMaxScale = maxScale;
        return true;
    }

    private static float ResolvePlasticityDelta(float plasticityRate, float plasticityDelta)
    {
        if (plasticityDelta > 0f)
        {
            return plasticityDelta;
        }

        return plasticityRate > 0f ? plasticityRate : 0f;
    }

    private static Nbn.Proto.Io.BrainEnergyState BuildCommandEnergyState(BrainEnergyState energy)
    {
        return new Nbn.Proto.Io.BrainEnergyState
        {
            EnergyRemaining = energy.EnergyRemaining,
            EnergyRateUnitsPerSecond = energy.EnergyRateUnitsPerSecond,
            CostEnabled = energy.CostEnabled,
            EnergyEnabled = energy.EnergyEnabled,
            PlasticityEnabled = energy.PlasticityEnabled,
            PlasticityRate = energy.PlasticityRate,
            PlasticityProbabilisticUpdates = energy.PlasticityProbabilisticUpdates,
            PlasticityDelta = energy.PlasticityDelta,
            PlasticityRebaseThreshold = energy.PlasticityRebaseThreshold,
            PlasticityRebaseThresholdPct = energy.PlasticityRebaseThresholdPct,
            PlasticityEnergyCostModulationEnabled = energy.PlasticityEnergyCostModulationEnabled,
            PlasticityEnergyCostReferenceTickCost = energy.PlasticityEnergyCostReferenceTickCost,
            PlasticityEnergyCostResponseStrength = energy.PlasticityEnergyCostResponseStrength,
            PlasticityEnergyCostMinScale = energy.PlasticityEnergyCostMinScale,
            PlasticityEnergyCostMaxScale = energy.PlasticityEnergyCostMaxScale,
            HomeostasisEnabled = energy.HomeostasisEnabled,
            HomeostasisTargetMode = energy.HomeostasisTargetMode,
            HomeostasisUpdateMode = energy.HomeostasisUpdateMode,
            HomeostasisBaseProbability = energy.HomeostasisBaseProbability,
            HomeostasisMinStepCodes = energy.HomeostasisMinStepCodes,
            HomeostasisEnergyCouplingEnabled = energy.HomeostasisEnergyCouplingEnabled,
            HomeostasisEnergyTargetScale = energy.HomeostasisEnergyTargetScale,
            HomeostasisEnergyProbabilityScale = energy.HomeostasisEnergyProbabilityScale,
            LastTickCost = energy.LastTickCost
        };
    }

    private static void RespondCommandAck(
        IContext context,
        Uuid? brainId,
        string command,
        bool success,
        string message,
        BrainIoEntry? entry = null,
        bool? configuredPlasticityEnabled = null,
        bool? effectivePlasticityEnabled = null)
    {
        if (context.Sender is null)
        {
            return;
        }

        var ack = new IoCommandAck
        {
            BrainId = brainId ?? Guid.Empty.ToProtoUuid(),
            Command = command,
            Success = success,
            Message = message ?? string.Empty,
            HasEnergyState = entry is not null,
            HasConfiguredPlasticityEnabled = configuredPlasticityEnabled.HasValue,
            ConfiguredPlasticityEnabled = configuredPlasticityEnabled.GetValueOrDefault(),
            HasEffectivePlasticityEnabled = effectivePlasticityEnabled.HasValue,
            EffectivePlasticityEnabled = effectivePlasticityEnabled.GetValueOrDefault()
        };

        if (entry is not null)
        {
            ack.EnergyState = BuildCommandEnergyState(entry.Energy);
        }

        context.Respond(ack);
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
            TimeMs = (ulong)NowMs()
        };
    }

    private ProtoControl.BrainTerminated BuildTerminatedFromEntry(ProtoControl.BrainTerminated message, BrainIoEntry entry)
    {
        var baseDef = HasArtifactRef(message.BaseDef) ? message.BaseDef : entry.BaseDefinition ?? new ArtifactRef();
        var lastSnapshot = HasArtifactRef(message.LastSnapshot) ? message.LastSnapshot : entry.LastSnapshot ?? new ArtifactRef();
        var lastEnergyRemaining = entry.Energy.EnergyRemaining;
        var lastTickCost = entry.Energy.LastTickCost;
        var timeMs = message.TimeMs == 0 ? (ulong)NowMs() : message.TimeMs;

        return new ProtoControl.BrainTerminated
        {
            BrainId = message.BrainId,
            Reason = message.Reason,
            BaseDef = baseDef,
            LastSnapshot = lastSnapshot,
            LastEnergyRemaining = lastEnergyRemaining,
            LastTickCost = lastTickCost,
            TimeMs = timeMs
        };
    }

    private void BroadcastToClients(IContext context, object message)
    {
        if (_clients.Count == 0)
        {
            return;
        }

        foreach (var client in _clients.ToArray())
        {
            try
            {
                context.Send(client.Value.Pid, message);
            }
            catch
            {
                _clients.Remove(client.Key);
                context.Unwatch(client.Value.Pid);
            }
        }
    }

    private void StopAndRemoveBrain(IContext context, BrainIoEntry entry)
    {
        if (entry.OwnsInputCoordinator)
        {
            context.Stop(entry.InputPid);
        }

        if (entry.OwnsOutputCoordinator)
        {
            context.Stop(entry.OutputPid);
        }

        _brains.Remove(entry.BrainId);
        _routerCache.Remove(entry.BrainId);
        _routerRegistration.Remove(entry.BrainId);
    }

    private static PID SpawnOwnedInputCoordinator(
        IContext context,
        Guid brainId,
        uint inputWidth,
        ProtoControl.InputCoordinatorMode inputCoordinatorMode)
    {
        var inputName = IoNames.InputCoordinatorPrefix + brainId.ToString("N");
        try
        {
            return context.SpawnNamed(
                Props.FromProducer(() => new InputCoordinatorActor(brainId, inputWidth, inputCoordinatorMode)),
                inputName);
        }
        catch
        {
            return context.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, inputWidth, inputCoordinatorMode)));
        }
    }

    private static PID SpawnOwnedOutputCoordinator(IContext context, Guid brainId, uint outputWidth)
    {
        var outputName = IoNames.OutputCoordinatorPrefix + brainId.ToString("N");
        try
        {
            return context.SpawnNamed(
                Props.FromProducer(() => new OutputCoordinatorActor(brainId, outputWidth)),
                outputName);
        }
        catch
        {
            return context.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, outputWidth)));
        }
    }

    private static void TrackInputState(BrainIoEntry entry, object message)
    {
        switch (message)
        {
            case InputWrite inputWrite:
                entry.InputState.Apply(inputWrite);
                break;
            case InputVector inputVector:
                entry.InputState.Apply(inputVector);
                break;
        }
    }

    private static void TrackOutputSubscription(IContext context, BrainIoEntry entry, object message)
    {
        switch (message)
        {
            case SubscribeOutputs subscribe:
                AddSubscriber(context, subscribe.SubscriberActor, entry.OutputSubscribers);
                break;
            case UnsubscribeOutputs unsubscribe:
                RemoveSubscriber(context, unsubscribe.SubscriberActor, entry.OutputSubscribers);
                break;
            case SubscribeOutputsVector subscribeVector:
                AddSubscriber(context, subscribeVector.SubscriberActor, entry.OutputVectorSubscribers);
                break;
            case UnsubscribeOutputsVector unsubscribeVector:
                RemoveSubscriber(context, unsubscribeVector.SubscriberActor, entry.OutputVectorSubscribers);
                break;
        }
    }

    private void TrackPendingOutputSubscription(IContext context, Guid brainId, object message)
    {
        switch (message)
        {
            case SubscribeOutputs subscribe:
                AddSubscriber(context, subscribe.SubscriberActor, GetPendingSubscriberSet(brainId, vector: false));
                break;
            case UnsubscribeOutputs unsubscribe:
                RemovePendingSubscriber(context, unsubscribe.SubscriberActor, brainId, vector: false);
                break;
            case SubscribeOutputsVector subscribeVector:
                AddSubscriber(context, subscribeVector.SubscriberActor, GetPendingSubscriberSet(brainId, vector: true));
                break;
            case UnsubscribeOutputsVector unsubscribeVector:
                RemovePendingSubscriber(context, unsubscribeVector.SubscriberActor, brainId, vector: true);
                break;
        }
    }

    private bool MergePendingOutputSubscriptions(BrainIoEntry entry)
    {
        var merged = false;

        if (_pendingOutputSubscribers.Remove(entry.BrainId, out var pendingSingles))
        {
            foreach (var subscriber in pendingSingles)
            {
                entry.OutputSubscribers[subscriber.Key] = subscriber.Value;
            }

            merged = pendingSingles.Count > 0;
        }

        if (_pendingOutputVectorSubscribers.Remove(entry.BrainId, out var pendingVectors))
        {
            foreach (var subscriber in pendingVectors)
            {
                entry.OutputVectorSubscribers[subscriber.Key] = subscriber.Value;
            }

            merged = merged || pendingVectors.Count > 0;
        }

        return merged;
    }

    private static void AddSubscriber(IContext context, string? subscriberActor, Dictionary<string, PID> set)
    {
        var subscriber = ResolveSubscriberPid(context, subscriberActor);
        if (subscriber is null)
        {
            return;
        }

        set[PidKey(subscriber)] = subscriber;
    }

    private static void RemoveSubscriber(IContext context, string? subscriberActor, Dictionary<string, PID> set)
    {
        var subscriber = ResolveSubscriberPid(context, subscriberActor);
        if (subscriber is null)
        {
            return;
        }

        set.Remove(PidKey(subscriber));
    }

    private Dictionary<string, PID> GetPendingSubscriberSet(Guid brainId, bool vector)
    {
        var source = vector ? _pendingOutputVectorSubscribers : _pendingOutputSubscribers;
        if (!source.TryGetValue(brainId, out var set))
        {
            set = new Dictionary<string, PID>(StringComparer.Ordinal);
            source.Add(brainId, set);
        }

        return set;
    }

    private void RemovePendingSubscriber(IContext context, string? subscriberActor, Guid brainId, bool vector)
    {
        var source = vector ? _pendingOutputVectorSubscribers : _pendingOutputSubscribers;
        if (!source.TryGetValue(brainId, out var set))
        {
            return;
        }

        RemoveSubscriber(context, subscriberActor, set);
        if (set.Count == 0)
        {
            source.Remove(brainId);
        }
    }

    private static PID? ResolveSubscriberPid(IContext context, string? subscriberActor)
    {
        if (TryParsePid(subscriberActor, out var parsed) && parsed is not null)
        {
            return ToRemotePid(context, parsed);
        }

        return context.Sender is null ? null : ToRemotePid(context, context.Sender);
    }

    private static object NormalizeOutputSubscriptionMessage(IContext context, object message)
    {
        return message switch
        {
            SubscribeOutputs subscribe => new SubscribeOutputs
            {
                BrainId = subscribe.BrainId,
                SubscriberActor = ResolveSubscriberActorLabel(context, subscribe.SubscriberActor)
            },
            UnsubscribeOutputs unsubscribe => new UnsubscribeOutputs
            {
                BrainId = unsubscribe.BrainId,
                SubscriberActor = ResolveSubscriberActorLabel(context, unsubscribe.SubscriberActor)
            },
            SubscribeOutputsVector subscribeVector => new SubscribeOutputsVector
            {
                BrainId = subscribeVector.BrainId,
                SubscriberActor = ResolveSubscriberActorLabel(context, subscribeVector.SubscriberActor)
            },
            UnsubscribeOutputsVector unsubscribeVector => new UnsubscribeOutputsVector
            {
                BrainId = unsubscribeVector.BrainId,
                SubscriberActor = ResolveSubscriberActorLabel(context, unsubscribeVector.SubscriberActor)
            },
            _ => message
        };
    }

    private static string ResolveSubscriberActorLabel(IContext context, string? subscriberActor)
    {
        if (!string.IsNullOrWhiteSpace(subscriberActor))
        {
            return subscriberActor;
        }

        var subscriberPid = ResolveSubscriberPid(context, subscriberActor);
        return subscriberPid is null ? string.Empty : PidLabel(subscriberPid);
    }

    private static bool UpdateCoordinatorReference(
        IContext context,
        PID currentPid,
        bool currentlyOwned,
        PID requestedPid,
        bool requestedOwned,
        out PID updatedPid,
        out bool updatedOwned)
    {
        updatedPid = currentPid;
        updatedOwned = currentlyOwned;
        if (PidEquals(currentPid, requestedPid) && currentlyOwned == requestedOwned)
        {
            return false;
        }

        if (currentlyOwned && !PidEquals(currentPid, requestedPid))
        {
            context.Stop(currentPid);
        }

        updatedPid = requestedPid;
        updatedOwned = requestedOwned;
        return true;
    }

    private static async Task ReplayInputStateAsync(IContext context, BrainIoEntry entry)
    {
        await DispatchCoordinatorMessageAsync(
                context,
                entry.InputPid,
                new UpdateInputCoordinatorMode(entry.InputCoordinatorMode))
            .ConfigureAwait(false);
        await entry.InputState
            .ReplayToAsync(message => DispatchCoordinatorMessageAsync(context, entry.InputPid, message))
            .ConfigureAwait(false);
    }

    private static async Task ReplayOutputSubscriptionsAsync(IContext context, BrainIoEntry entry)
    {
        foreach (var subscriber in entry.OutputSubscribers.Values)
        {
            await DispatchCoordinatorMessageAsync(context, entry.OutputPid, new SubscribeOutputs
            {
                BrainId = entry.BrainId.ToProtoUuid(),
                SubscriberActor = PidLabel(subscriber)
            }).ConfigureAwait(false);
        }

        foreach (var subscriber in entry.OutputVectorSubscribers.Values)
        {
            await DispatchCoordinatorMessageAsync(context, entry.OutputPid, new SubscribeOutputsVector
            {
                BrainId = entry.BrainId.ToProtoUuid(),
                SubscriberActor = PidLabel(subscriber)
            }).ConfigureAwait(false);
        }
    }

    private static async Task<IoCommandAck> DispatchCoordinatorMessageAsync(IContext context, PID target, object message)
    {
        return await context.RequestAsync<IoCommandAck>(target, message, DefaultRequestTimeout).ConfigureAwait(false);
    }

    private static void RespondOutputCommandAck(IContext context, object message, Uuid? brainId, bool success, string ackMessage)
    {
        var command = message switch
        {
            SubscribeOutputs => "subscribe_outputs",
            UnsubscribeOutputs => "unsubscribe_outputs",
            SubscribeOutputsVector => "subscribe_outputs_vector",
            UnsubscribeOutputsVector => "unsubscribe_outputs_vector",
            _ => "output_command"
        };

        RespondCommandAck(context, brainId, command, success, ackMessage);
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

    private static bool PidEquals(PID? left, PID right)
    {
        return left is not null
               && string.Equals(left.Id, right.Id, StringComparison.Ordinal)
               && string.Equals(left.Address, right.Address, StringComparison.Ordinal);
    }

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "1" or "true" or "yes" or "on" => true,
            _ => false
        };
    }

    private async Task<PID?> ResolveRouterPidAsync(IContext context, Guid brainId, bool allowCached = true)
    {
        _routerCache.TryGetValue(brainId, out var cached);
        if (allowCached && cached is not null)
        {
            if (LogInputDiagnostics)
            {
                Console.WriteLine($"[IoGatewayInput] router cached brain={brainId} router={PidLabel(cached)}");
            }

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
                return cached;
            }

            if (TryParsePid(info.SignalRouterPid, out var routerPid) && routerPid is not null)
            {
                if (LogInputDiagnostics)
                {
                    Console.WriteLine(
                        $"[IoGatewayInput] router resolved brain={brainId} target=signal_router previous={PidLabel(cached)} current={PidLabel(routerPid)}");
                }

                if (!PidEquals(cached, routerPid))
                {
                    _routerRegistration.Remove(brainId);
                }

                _routerCache[brainId] = routerPid;
                RegisterIoGatewayPid(context, brainId, routerPid);
                return routerPid;
            }

            if (TryParsePid(info.BrainRootPid, out var rootPid) && rootPid is not null)
            {
                if (LogInputDiagnostics)
                {
                    Console.WriteLine(
                        $"[IoGatewayInput] router resolved brain={brainId} target=brain_root previous={PidLabel(cached)} current={PidLabel(rootPid)}");
                }

                if (!PidEquals(cached, rootPid))
                {
                    _routerRegistration.Remove(brainId);
                }

                _routerCache[brainId] = rootPid;
                RegisterIoGatewayPid(context, brainId, rootPid);
                return rootPid;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Resolve router failed: {ex.Message}");
        }

        return cached;
    }

    private static string DescribeInputPayload(object message)
    {
        return message switch
        {
            InputWrite inputWrite => $"index={inputWrite.InputIndex} value={inputWrite.Value:0.###}",
            InputVector inputVector => $"width={inputVector.Values.Count}",
            RuntimeNeuronPulse pulse => $"region={pulse.TargetRegionId} neuron={pulse.TargetNeuronId} value={pulse.Value:0.###}",
            RuntimeNeuronStateWrite stateWrite => $"region={stateWrite.TargetRegionId} neuron={stateWrite.TargetNeuronId} buffer={stateWrite.SetBuffer} accumulator={stateWrite.SetAccumulator}",
            _ => string.Empty
        };
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


