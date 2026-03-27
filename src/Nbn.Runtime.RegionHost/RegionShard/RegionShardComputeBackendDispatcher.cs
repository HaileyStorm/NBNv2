using Nbn.Proto.Control;
using Nbn.Shared;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Runtime.RegionHost;

/// <summary>
/// Reports which compute backend executed the last RegionShard tick and why a fallback occurred.
/// </summary>
public readonly record struct RegionShardBackendExecutionInfo(
    RegionShardComputeBackendPreference RequestedPreference,
    string BackendName,
    bool UsedGpu,
    string FallbackReason,
    bool HasExecuted);

/// <summary>
/// Routes shard compute requests to the preferred backend while preserving CPU fallback behavior and execution metadata.
/// </summary>
public sealed class RegionShardComputeBackendDispatcher : IDisposable
{
    private const string CpuBackendName = "cpu";

    private readonly RegionShardState _state;
    private readonly RegionShardCpuBackend _cpu;
    private readonly Func<RegionShardState, RegionShardIlgpuBackend?> _gpuFactory;
    private readonly RegionShardComputeBackendPreference _preference;
    private RegionShardIlgpuBackend? _gpu;
    private bool _gpuInitialized;
    private string _gpuInitializationReason;

    /// <summary>
    /// Creates a backend dispatcher for a single shard state instance.
    /// </summary>
    /// <param name="state">Mutable shard state shared by the compute backends.</param>
    /// <param name="preference">Requested backend preference for future compute calls.</param>
    /// <param name="gpuFactory">Optional GPU backend factory used for tests or specialized bootstrapping.</param>
    public RegionShardComputeBackendDispatcher(
        RegionShardState state,
        RegionShardComputeBackendPreference preference,
        Func<RegionShardState, RegionShardIlgpuBackend?>? gpuFactory = null)
    {
        _state = state ?? throw new ArgumentNullException(nameof(state));
        _cpu = new RegionShardCpuBackend(state);
        _preference = preference;
        _gpuFactory = gpuFactory ?? RegionShardIlgpuBackend.TryCreate;
        _gpuInitializationReason = string.Empty;

        LastExecution = CreateExecutionInfo(CpuBackendName, usedGpu: false, fallbackReason: string.Empty, hasExecuted: false);
    }

    /// <summary>
    /// Gets metadata for the last attempted compute call.
    /// </summary>
    public RegionShardBackendExecutionInfo LastExecution { get; private set; }

    /// <summary>
    /// Executes shard compute on the preferred backend, falling back to CPU when GPU use is disabled or unsupported.
    /// </summary>
    /// <returns>The compute result produced by the executing backend.</returns>
    public RegionShardComputeResult Compute(
        ulong tickId,
        Guid brainId,
        ShardId32 shardId,
        RegionShardRoutingTable routing,
        RegionShardVisualizationComputeScope? visualization = null,
        bool plasticityEnabled = false,
        float plasticityRate = 0f,
        bool probabilisticPlasticityUpdates = false,
        float plasticityDelta = 0f,
        uint plasticityRebaseThreshold = 0,
        float plasticityRebaseThresholdPct = 0f,
        RegionShardPlasticityEnergyCostConfig? plasticityEnergyCostConfig = null,
        RegionShardHomeostasisConfig? homeostasisConfig = null,
        bool costEnergyEnabled = false,
        bool? remoteCostEnabled = null,
        long? remoteCostPerBatch = null,
        long? remoteCostPerContribution = null,
        float? costTierAMultiplier = null,
        float? costTierBMultiplier = null,
        float? costTierCMultiplier = null,
        OutputVectorSource outputVectorSource = OutputVectorSource.Potential,
        long previousTickCostTotal = 0)
    {
        EnsureGpuBackend();

        if (TryComputeWithGpu(
                tickId,
                brainId,
                shardId,
                routing,
                visualization,
                plasticityEnabled,
                plasticityRate,
                probabilisticPlasticityUpdates,
                plasticityDelta,
                plasticityRebaseThreshold,
                plasticityRebaseThresholdPct,
                plasticityEnergyCostConfig,
                homeostasisConfig,
                costEnergyEnabled,
                remoteCostEnabled,
                remoteCostPerBatch,
                remoteCostPerContribution,
                costTierAMultiplier,
                costTierBMultiplier,
                costTierCMultiplier,
                outputVectorSource,
                previousTickCostTotal,
                out var gpuResult))
        {
            return gpuResult;
        }

        var cpuResult = _cpu.Compute(
            tickId,
            brainId,
            shardId,
            routing,
            visualization,
            plasticityEnabled,
            plasticityRate,
            probabilisticPlasticityUpdates,
            plasticityDelta,
            plasticityRebaseThreshold,
            plasticityRebaseThresholdPct,
            plasticityEnergyCostConfig,
            homeostasisConfig,
            costEnergyEnabled,
            remoteCostEnabled,
            remoteCostPerBatch,
            remoteCostPerContribution,
            costTierAMultiplier,
            costTierBMultiplier,
            costTierCMultiplier,
            outputVectorSource,
            previousTickCostTotal);

        RecordCpuExecutionIfUnset();
        return cpuResult;
    }

    private void EnsureGpuBackend()
    {
        if (_gpuInitialized || !RegionShardComputeBackendPreferenceResolver.IsGpuExecutionEnabled(_preference))
        {
            return;
        }

        _gpuInitialized = true;
        try
        {
            _gpu = _gpuFactory(_state);
            _gpuInitializationReason = _gpu is null && _preference == RegionShardComputeBackendPreference.Gpu
                ? ResolveGpuUnavailableReason()
                : string.Empty;
        }
        catch (Exception ex)
        {
            _gpuInitializationReason = $"gpu_init_failed:{ex.GetBaseException().Message}";
        }
    }

    private static string ResolveGpuUnavailableReason()
    {
        var availability = RegionShardGpuRuntime.ProbeAvailability();
        return string.IsNullOrWhiteSpace(availability.FailureReason)
            ? "gpu_backend_unavailable"
            : availability.FailureReason;
    }

    /// <summary>
    /// Releases any lazily-created GPU backend resources.
    /// </summary>
    public void Dispose()
    {
        _gpu?.Dispose();
    }

    private bool TryComputeWithGpu(
        ulong tickId,
        Guid brainId,
        ShardId32 shardId,
        RegionShardRoutingTable routing,
        RegionShardVisualizationComputeScope? visualization,
        bool plasticityEnabled,
        float plasticityRate,
        bool probabilisticPlasticityUpdates,
        float plasticityDelta,
        uint plasticityRebaseThreshold,
        float plasticityRebaseThresholdPct,
        RegionShardPlasticityEnergyCostConfig? plasticityEnergyCostConfig,
        RegionShardHomeostasisConfig? homeostasisConfig,
        bool costEnergyEnabled,
        bool? remoteCostEnabled,
        long? remoteCostPerBatch,
        long? remoteCostPerContribution,
        float? costTierAMultiplier,
        float? costTierBMultiplier,
        float? costTierCMultiplier,
        OutputVectorSource outputVectorSource,
        long previousTickCostTotal,
        out RegionShardComputeResult result)
    {
        result = default!;
        if (_gpu is null)
        {
            return false;
        }

        var support = _gpu.GetSupport(
            visualization,
            plasticityEnabled,
            probabilisticPlasticityUpdates,
            plasticityDelta,
            plasticityRebaseThreshold,
            plasticityRebaseThresholdPct,
            homeostasisConfig,
            costEnergyEnabled,
            outputVectorSource);
        if (!support.IsSupported)
        {
            if (_preference != RegionShardComputeBackendPreference.Cpu)
            {
                RecordCpuExecution(support.Reason);
            }

            return false;
        }

        try
        {
            result = _gpu.Compute(
                tickId,
                brainId,
                shardId,
                routing,
                visualization,
                plasticityEnabled,
                plasticityRate,
                probabilisticPlasticityUpdates,
                plasticityDelta,
                plasticityRebaseThreshold,
                plasticityRebaseThresholdPct,
                plasticityEnergyCostConfig,
                homeostasisConfig,
                costEnergyEnabled,
                remoteCostEnabled,
                remoteCostPerBatch,
                remoteCostPerContribution,
                costTierAMultiplier,
                costTierBMultiplier,
                costTierCMultiplier,
                outputVectorSource,
                previousTickCostTotal);
            LastExecution = CreateExecutionInfo(_gpu.BackendName, usedGpu: true, fallbackReason: string.Empty, hasExecuted: true);
            return true;
        }
        catch (Exception ex)
        {
            RecordCpuExecution($"gpu_compute_failed:{ex}");
            return false;
        }
    }

    private void RecordCpuExecutionIfUnset()
    {
        if (string.IsNullOrWhiteSpace(LastExecution.FallbackReason))
        {
            RecordCpuExecution(_gpuInitializationReason);
        }
    }

    private void RecordCpuExecution(string fallbackReason)
    {
        LastExecution = CreateExecutionInfo(CpuBackendName, usedGpu: false, fallbackReason, hasExecuted: true);
    }

    private RegionShardBackendExecutionInfo CreateExecutionInfo(string backendName, bool usedGpu, string fallbackReason, bool hasExecuted)
    {
        return new RegionShardBackendExecutionInfo(
            _preference,
            backendName,
            usedGpu,
            fallbackReason,
            hasExecuted);
    }
}
