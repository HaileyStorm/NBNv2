using Nbn.Proto.Control;
using Nbn.Shared;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Runtime.RegionHost;

public readonly record struct RegionShardBackendExecutionInfo(
    RegionShardComputeBackendPreference RequestedPreference,
    string BackendName,
    bool UsedGpu,
    string FallbackReason,
    bool HasExecuted);

public sealed class RegionShardComputeBackendDispatcher : IDisposable
{
    private readonly RegionShardState _state;
    private readonly RegionShardCpuBackend _cpu;
    private readonly Func<RegionShardState, RegionShardIlgpuBackend?> _gpuFactory;
    private readonly RegionShardComputeBackendPreference _preference;
    private RegionShardIlgpuBackend? _gpu;
    private bool _gpuInitialized;
    private string _gpuInitializationReason;

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

        LastExecution = new RegionShardBackendExecutionInfo(
            preference,
            BackendName: "cpu",
            UsedGpu: false,
            FallbackReason: string.Empty,
            HasExecuted: false);
    }

    public RegionShardBackendExecutionInfo LastExecution { get; private set; }

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

        if (_gpu is not null)
        {
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
            if (support.IsSupported)
            {
                try
                {
                    var result = _gpu.Compute(
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
                    LastExecution = new RegionShardBackendExecutionInfo(
                        _preference,
                        _gpu.BackendName,
                        UsedGpu: true,
                        FallbackReason: string.Empty,
                        HasExecuted: true);
                    return result;
                }
                catch (Exception ex)
                {
                    LastExecution = new RegionShardBackendExecutionInfo(
                        _preference,
                        BackendName: "cpu",
                        UsedGpu: false,
                        FallbackReason: $"gpu_compute_failed:{ex}",
                        HasExecuted: true);
                }
            }
            else if (_preference != RegionShardComputeBackendPreference.Cpu)
            {
                LastExecution = new RegionShardBackendExecutionInfo(
                    _preference,
                    BackendName: "cpu",
                    UsedGpu: false,
                    FallbackReason: support.Reason,
                    HasExecuted: true);
            }
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

        if (string.IsNullOrWhiteSpace(LastExecution.FallbackReason))
        {
            LastExecution = new RegionShardBackendExecutionInfo(
                _preference,
                BackendName: "cpu",
                UsedGpu: false,
                FallbackReason: _gpuInitializationReason,
                HasExecuted: true);
        }

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

    public void Dispose()
    {
        _gpu?.Dispose();
    }
}
