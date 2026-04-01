using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Shared;
using Nbn.Shared.HiveMind;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.IO;

/// <summary>
/// Bridges external IO clients to runtime services and per-brain coordinators.
/// </summary>
public sealed partial class IoGatewayActor : IActor
{
    private static readonly bool LogOutput = IsEnvTrue("NBN_IO_LOG_OUTPUT");
    private static readonly bool LogInputDiagnostics =
        IsEnvTrue("NBN_VIZ_DIAGNOSTICS_ENABLED") || IsEnvTrue("NBN_INPUT_DIAGNOSTICS_ENABLED");
    private static readonly bool LogInputTraceDiagnostics = IsEnvTrue("NBN_INPUT_TRACE_DIAGNOSTICS_ENABLED");
    private static readonly bool LogMetadataDiagnostics =
        IsEnvTrue("NBN_METADATA_DIAGNOSTICS_ENABLED") || IsEnvTrue("NBN_IO_METADATA_DIAGNOSTICS_ENABLED");
    private static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan BrainInfoResolveTimeout = TimeSpan.FromSeconds(3);
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

    /// <summary>
    /// Initializes an IO gateway actor with optional preconfigured service endpoints.
    /// </summary>
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

    /// <summary>
    /// Processes gateway client, discovery, coordination, and service-forwarding messages.
    /// </summary>
    public async Task ReceiveAsync(IContext context)
    {
        try
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
                case GetPlacementWorkerInventory:
                    await HandlePlacementWorkerInventoryAsync(context);
                    break;
                case SpawnBrainViaIO message:
                    await HandleSpawnBrain(context, message);
                    break;
                case AwaitSpawnPlacementViaIO message:
                    await HandleAwaitSpawnPlacementAsync(context, message);
                    break;
                case KillBrainViaIO message:
                    HandleKillBrain(context, message);
                    break;
                case ProtoControl.PauseBrain message:
                    HandlePauseBrain(context, message);
                    break;
                case ProtoControl.ResumeBrain message:
                    HandleResumeBrain(context, message);
                    break;
                case SetOutputVectorSource message:
                    await HandleSetOutputVectorSourceAsync(context, message);
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
                case ResetBrainRuntimeState message:
                    await ForwardRuntimeStateResetAsync(context, message);
                    break;
                case ApplyBrainRuntimeResetAtBarrier message:
                    await ApplyRuntimeStateResetAtBarrierAsync(context, message);
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
                case Nbn.Proto.Io.RegisterBrain message:
                    await RegisterBrainAsync(context, message);
                    break;
                case Nbn.Proto.Io.UnregisterBrain message:
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
        catch (Exception ex)
        {
            HandleReceiveFailure(context, context.Message, ex);
        }
    }

    private void HandleReceiveFailure(IContext context, object? message, Exception ex)
    {
        var detail = ex.GetBaseException().Message;
        Console.WriteLine($"[ERROR] IO Gateway handler {message?.GetType().Name ?? "unknown"} failed: {detail}");

        switch (message)
        {
            case GetPlacementWorkerInventory:
                context.Respond(BuildPlacementWorkerInventoryFailure(
                    reasonCode: "io_gateway_internal_error",
                    failureMessage: $"Worker capacity query failed: IO Gateway internal error ({detail})."));
                break;
            case SpawnBrainViaIO:
                var spawnAck = BuildSpawnFailureAck(
                    reasonCode: "spawn_request_failed",
                    failureMessage: $"Spawn failed: IO Gateway internal error ({detail}).");
                context.Respond(new SpawnBrainViaIOAck
                {
                    Ack = spawnAck,
                    FailureReasonCode = spawnAck.FailureReasonCode,
                    FailureMessage = spawnAck.FailureMessage
                });
                break;
            case AwaitSpawnPlacementViaIO:
                var placementAck = BuildSpawnFailureAck(
                    reasonCode: "spawn_request_failed",
                    failureMessage: $"Spawn wait failed: IO Gateway internal error ({detail}).");
                context.Respond(new AwaitSpawnPlacementViaIOAck
                {
                    Ack = placementAck,
                    FailureReasonCode = placementAck.FailureReasonCode,
                    FailureMessage = placementAck.FailureMessage
                });
                break;
            case KillBrainViaIO:
                context.Respond(new KillBrainViaIOAck
                {
                    Accepted = false,
                    FailureReasonCode = "kill_request_failed",
                    FailureMessage = $"Kill failed: IO Gateway internal error ({detail})."
                });
                break;
            case ProtoControl.PauseBrain pause:
                RespondCommandAck(
                    context,
                    pause.BrainId,
                    "pause_brain",
                    success: false,
                    $"io_gateway_internal_error:{detail}");
                break;
            case ProtoControl.ResumeBrain resume:
                RespondCommandAck(
                    context,
                    resume.BrainId,
                    "resume_brain",
                    success: false,
                    $"io_gateway_internal_error:{detail}");
                break;
            case SetOutputVectorSource outputSourceMessage:
                context.Respond(new SetOutputVectorSourceAck
                {
                    Success = false,
                    FailureReasonCode = "output_vector_source_internal_error",
                    FailureMessage = $"Output vector source update failed: IO Gateway internal error ({detail}).",
                    OutputVectorSource = DefaultOutputVectorSource,
                    BrainId = outputSourceMessage.BrainId?.Clone()
                });
                break;
            case BrainInfoRequest brainInfo:
                context.Respond(new BrainInfo
                {
                    BrainId = brainInfo.BrainId,
                    BaseDefinition = new ArtifactRef(),
                    LastSnapshot = new ArtifactRef()
                });
                break;
            case ResetBrainRuntimeState reset:
                RespondCommandAck(
                    context,
                    reset.BrainId,
                    "reset_brain_runtime_state",
                    success: false,
                    $"io_gateway_internal_error:{detail}");
                break;
            case SubscribeOutputs or UnsubscribeOutputs or SubscribeOutputsVector or UnsubscribeOutputsVector:
                if (TryGetBrainId(message, out var outputBrainId))
                {
                    RespondOutputCommandAck(context, message, outputBrainId.ToProtoUuid(), success: false, $"io_gateway_internal_error:{detail}");
                }
                else
                {
                    RespondOutputCommandAck(context, message, Guid.Empty.ToProtoUuid(), success: false, $"io_gateway_internal_error:{detail}");
                }
                break;
            case DrainInputs drain:
                context.Respond(new InputDrain
                {
                    BrainId = drain.BrainId,
                    TickId = drain.TickId
                });
                break;
            case RequestSnapshot snapshot:
                context.Respond(new SnapshotReady { BrainId = snapshot.BrainId });
                break;
            case ExportBrainDefinition export:
                context.Respond(new BrainDefinitionReady { BrainId = export.BrainId });
                break;
        }
    }
}
