using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Proto.Viz;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;

namespace Nbn.Runtime.RegionHost;

/// <summary>
/// Hosts a single region shard actor and coordinates signal ingress, compute execution, and runtime control updates.
/// </summary>
public sealed partial class RegionShardActor : IActor
{
    private static readonly bool LogDelivery = IsEnvTrue("NBN_REGIONHOST_LOG_DELIVERY");
    private static readonly bool LogOutput = IsEnvTrue("NBN_REGIONHOST_LOG_OUTPUT");
    private static readonly bool LogViz = IsEnvTrue("NBN_REGIONHOST_LOG_VIZ");
    private static readonly bool LogVizDiagnostics = IsEnvTrue("NBN_VIZ_DIAGNOSTICS_ENABLED");
    private static readonly bool LogInitDiagnostics = IsEnvTrue("NBN_REGIONSHARD_INIT_DIAGNOSTICS_ENABLED");

    private const int RecentComputeCacheSize = 2;

    private readonly RegionShardState _state;
    private readonly RegionShardComputeBackendDispatcher _computeBackend;
    private readonly Guid _brainId;
    private readonly ShardId32 _shardId;
    private readonly Dictionary<ulong, TickComputeDone> _recentComputeDone = new();

    private RegionShardRoutingTable _routing;
    private PID? _router;
    private PID? _outputSink;
    private PID? _tickSink;
    private PID? _vizHub;
    private PID? _debugHub;
    private bool _debugEnabled;
    private ProtoSeverity _debugMinSeverity;
    private bool _vizEnabled;
    private uint? _vizFocusRegionId;
    private bool _costEnergyEnabled;
    private bool _remoteCostEnabled;
    private long _remoteCostPerBatch;
    private long _remoteCostPerContribution;
    private float _costTierAMultiplier = 1f;
    private float _costTierBMultiplier = 1f;
    private float _costTierCMultiplier = 1f;
    private bool _plasticityEnabled;
    private float _plasticityRate;
    private bool _plasticityProbabilisticUpdates;
    private float _plasticityDelta;
    private uint _plasticityRebaseThreshold;
    private float _plasticityRebaseThresholdPct;
    private bool _plasticityEnergyCostModulationEnabled = RegionShardPlasticityEnergyCostConfig.Default.Enabled;
    private long _plasticityEnergyCostReferenceTickCost = RegionShardPlasticityEnergyCostConfig.Default.ReferenceTickCost;
    private float _plasticityEnergyCostResponseStrength = RegionShardPlasticityEnergyCostConfig.Default.ResponseStrength;
    private float _plasticityEnergyCostMinScale = RegionShardPlasticityEnergyCostConfig.Default.MinScale;
    private float _plasticityEnergyCostMaxScale = RegionShardPlasticityEnergyCostConfig.Default.MaxScale;
    private bool _homeostasisEnabled = RegionShardHomeostasisConfig.Default.Enabled;
    private HomeostasisTargetMode _homeostasisTargetMode = RegionShardHomeostasisConfig.Default.TargetMode;
    private HomeostasisUpdateMode _homeostasisUpdateMode = RegionShardHomeostasisConfig.Default.UpdateMode;
    private float _homeostasisBaseProbability = RegionShardHomeostasisConfig.Default.BaseProbability;
    private uint _homeostasisMinStepCodes = RegionShardHomeostasisConfig.Default.MinStepCodes;
    private bool _homeostasisEnergyCouplingEnabled = RegionShardHomeostasisConfig.Default.EnergyCouplingEnabled;
    private float _homeostasisEnergyTargetScale = RegionShardHomeostasisConfig.Default.EnergyTargetScale;
    private float _homeostasisEnergyProbabilityScale = RegionShardHomeostasisConfig.Default.EnergyProbabilityScale;
    private OutputVectorSource _outputVectorSource = OutputVectorSource.Potential;
    private uint _vizStreamMinIntervalMs = 250;
    private bool _hasComputed;
    private ulong _lastComputeTickId;
    private long _lastTickCostTotal;
    private ulong _vizSequence;

    /// <summary>
    /// Creates a region shard actor with the supplied state and runtime surfaces.
    /// </summary>
    /// <param name="state">Shard-local neuron and axon state.</param>
    /// <param name="config">Actor wiring, routing, and initial observability/runtime configuration.</param>
    public RegionShardActor(RegionShardState state, RegionShardActorConfig config)
    {
        _state = state ?? throw new ArgumentNullException(nameof(state));
        _computeBackend = new RegionShardComputeBackendDispatcher(_state, config.ComputeBackendPreference);
        _brainId = config.BrainId;
        _shardId = config.ShardId;
        _router = config.Router;
        _outputSink = config.OutputSink;
        _tickSink = config.TickSink;
        _vizHub = config.VizHub;
        _debugHub = config.DebugHub;
        _debugEnabled = config.DebugEnabled;
        _debugMinSeverity = config.DebugMinSeverity;
        _vizEnabled = config.VizEnabled;
        _vizFocusRegionId = null;
        _routing = config.Routing ?? RegionShardRoutingTable.CreateSingleShard(_state.RegionId, _state.NeuronCount);
    }

    /// <summary>
    /// Handles lifecycle, compute, signal, snapshot, and runtime-configuration messages for the shard.
    /// </summary>
    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
                HandleStarted(context);
                break;
            case Stopping:
                HandleStopping();
                break;
            case RegionShardUpdateEndpoints endpoints:
                _router = endpoints.Router;
                _outputSink = endpoints.OutputSink;
                _tickSink = endpoints.TickSink;
                break;
            case RegionShardUpdateRouting routing:
                _routing = routing.Routing;
                break;
            case GetRegionShardBackendExecutionInfo:
                context.Respond(_computeBackend.LastExecution);
                break;
            case SignalBatch batch:
                HandleSignalBatch(context, batch);
                break;
            case TickCompute tick:
                HandleTickCompute(context, tick);
                break;
            case RegisterShard registerShard:
                ForwardRegisterShard(context, registerShard);
                break;
            case UnregisterShard unregisterShard:
                ForwardUnregisterShard(context, unregisterShard);
                break;
            case RuntimeNeuronPulse pulse:
                HandleRuntimeNeuronPulse(pulse);
                break;
            case RuntimeNeuronStateWrite stateWrite:
                HandleRuntimeNeuronStateWrite(stateWrite);
                break;
            case UpdateShardOutputSink message:
                HandleUpdateOutputSink(message);
                break;
            case UpdateShardVisualization message:
                HandleUpdateVisualization(message);
                break;
            case UpdateShardRuntimeConfig message:
                HandleUpdateRuntimeConfig(message);
                break;
            case CaptureShardSnapshot message:
                HandleCaptureShardSnapshot(context, message);
                break;
        }

        return Task.CompletedTask;
    }

    private void HandleStarted(IContext context)
    {
        EmitVizEvent(context, VizEventType.VizShardSpawned, tickId: 0, value: 0f);
        EmitDebug(context, ProtoSeverity.SevInfo, "shard.started", $"Shard {_shardId} for brain {_brainId} started.");
        if (LogInitDiagnostics)
        {
            LogShardInitDiagnostics();
        }
    }

    private void HandleStopping()
    {
        _computeBackend.Dispose();
    }

    private void ForwardRegisterShard(IContext context, RegisterShard message)
    {
        if (_tickSink is not null)
        {
            context.Request(_tickSink, message);
        }
    }

    private void ForwardUnregisterShard(IContext context, UnregisterShard message)
    {
        if (_tickSink is not null)
        {
            context.Request(_tickSink, message);
        }
    }
}
