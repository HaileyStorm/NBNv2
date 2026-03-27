using ILGPU;
using ILGPU.Runtime;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Shared;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Runtime.RegionHost;

/// <summary>
/// Reports whether the ILGPU backend can execute a given shard compute request.
/// </summary>
public readonly record struct RegionShardGpuSupport(bool IsSupported, string Reason);

/// <summary>
/// Optional ILGPU-backed RegionShard compute implementation used when runtime and feature support allow it.
/// </summary>
public sealed class RegionShardIlgpuBackend : IDisposable
{
    private const float RuntimeSignalLimit = 1f;
    private const float DefaultBufferMin = -4f;
    private const float DefaultBufferMax = 4f;
    private const float DefaultBufferGamma = 2f;
    private const int DefaultBufferBits = 16;
    private const int DefaultBufferMaxCode = (1 << DefaultBufferBits) - 1;
    private static readonly double[] AccumulationBaseCosts = { 1.0, 1.2, 1.0, 0.1 };
    private static readonly CostTier[] AccumulationTiers =
    {
        CostTier.A,
        CostTier.A,
        CostTier.A,
        CostTier.A
    };
    private static readonly double[] ActivationBaseCosts = BuildActivationBaseCosts();
    private static readonly CostTier[] ActivationTiers = BuildActivationTiers();
    private static readonly double[] ResetBaseCosts = BuildResetBaseCosts();
    private static readonly CostTier[] ResetTiers = BuildResetTiers();

    private readonly RegionShardState _state;
    private readonly RegionShardCostConfig _costConfig;
    private readonly RegionShardGpuAcceleratorLease _lease;
    private readonly string _backendName;
    private readonly string? _unsupportedStateReason;
    private readonly Dictionary<byte, NeuronIndexGroup> _activationGroups;
    private readonly Dictionary<byte, NeuronIndexGroup> _resetGroups;
    private readonly byte[] _inboxHasInputScratch;
    private readonly byte[] _activationRanScratch;
    private readonly byte[] _firedScratch;
    private readonly float[] _potentialScratch;
    private readonly float[]? _preResetBufferScratch;
    private readonly byte[] _existsBytes;
    private readonly byte[] _enabledBytes;

    private readonly MemoryBuffer1D<byte, Stride1D.Dense> _accumulationFunctionsBuffer;
    private readonly MemoryBuffer1D<byte, Stride1D.Dense> _activationFunctionsBuffer;
    private readonly MemoryBuffer1D<byte, Stride1D.Dense> _resetFunctionsBuffer;
    private readonly MemoryBuffer1D<byte, Stride1D.Dense> _existsBuffer;
    private readonly MemoryBuffer1D<byte, Stride1D.Dense> _enabledBuffer;
    private readonly MemoryBuffer1D<float, Stride1D.Dense> _paramABuffer;
    private readonly MemoryBuffer1D<float, Stride1D.Dense> _paramBBuffer;
    private readonly MemoryBuffer1D<float, Stride1D.Dense> _preActivationThresholdBuffer;
    private readonly MemoryBuffer1D<float, Stride1D.Dense> _activationThresholdBuffer;
    private readonly MemoryBuffer1D<ushort, Stride1D.Dense> _axonCountsBuffer;
    private readonly MemoryBuffer1D<uint, Stride1D.Dense> _neuronAddressBuffer;

    private readonly MemoryBuffer1D<float, Stride1D.Dense> _bufferBuffer;
    private readonly MemoryBuffer1D<float, Stride1D.Dense> _preResetBuffer;
    private readonly MemoryBuffer1D<float, Stride1D.Dense> _inboxBuffer;
    private readonly MemoryBuffer1D<byte, Stride1D.Dense> _inboxHasInputBuffer;
    private readonly MemoryBuffer1D<float, Stride1D.Dense> _potentialBuffer;
    private readonly MemoryBuffer1D<byte, Stride1D.Dense> _activationRanBuffer;
    private readonly MemoryBuffer1D<byte, Stride1D.Dense> _firedBuffer;

    private enum CostTier
    {
        A = 0,
        B = 1,
        C = 2
    }

    /// <summary>
    /// Captures per-tick state passed into the GPU prepare kernel.
    /// </summary>
    public readonly record struct PrepareKernelConfig(
        ulong TickId,
        ulong BrainSeed,
        byte HomeostasisEnabled,
        int TargetMode,
        int UpdateMode,
        float BaseProbability,
        uint MinStepCodes,
        byte EnergyCouplingEnabled,
        float EnergyTargetScale,
        float EnergyProbabilityScale,
        byte CostEnergyEnabled);

    private Action<
        Index1D,
        ArrayView1D<byte, Stride1D.Dense>,
        ArrayView1D<byte, Stride1D.Dense>,
        ArrayView1D<byte, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<byte, Stride1D.Dense>,
        ArrayView1D<uint, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<byte, Stride1D.Dense>,
        ArrayView1D<byte, Stride1D.Dense>,
        PrepareKernelConfig>? _prepareKernel;

    private readonly Dictionary<byte, Action<
        Index1D,
        ArrayView1D<int, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<byte, Stride1D.Dense>,
        ArrayView1D<byte, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<byte, Stride1D.Dense>>> _activationKernels = new();

    private readonly Dictionary<byte, Action<
        Index1D,
        ArrayView1D<int, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<ushort, Stride1D.Dense>,
        ArrayView1D<byte, Stride1D.Dense>,
        ArrayView1D<byte, Stride1D.Dense>>> _resetKernels = new();

    private RegionShardIlgpuBackend(
        RegionShardState state,
        RegionShardGpuAcceleratorLease lease,
        RegionShardCostConfig? costConfig = null)
    {
        _state = state ?? throw new ArgumentNullException(nameof(state));
        _lease = lease;
        _costConfig = costConfig ?? RegionShardCostConfig.Default;
        _backendName = lease.Device.AcceleratorType == AcceleratorType.Cuda ? "gpu-cuda" : "gpu-opencl";
        _unsupportedStateReason = ResolveUnsupportedStateReason(state);
        _inboxHasInputScratch = new byte[state.NeuronCount];
        _activationRanScratch = new byte[state.NeuronCount];
        _firedScratch = new byte[state.NeuronCount];
        _potentialScratch = new float[state.NeuronCount];
        _preResetBufferScratch = state.IsOutputRegion ? new float[state.NeuronCount] : null;
        _existsBytes = ToByteFlags(state.Exists);
        _enabledBytes = ToByteFlags(state.Enabled);

        _accumulationFunctionsBuffer = lease.Accelerator.Allocate1D<byte>(state.AccumulationFunctions.Length);
        _activationFunctionsBuffer = lease.Accelerator.Allocate1D<byte>(state.ActivationFunctions.Length);
        _resetFunctionsBuffer = lease.Accelerator.Allocate1D<byte>(state.ResetFunctions.Length);
        _existsBuffer = lease.Accelerator.Allocate1D<byte>(_existsBytes.Length);
        _enabledBuffer = lease.Accelerator.Allocate1D<byte>(_enabledBytes.Length);
        _paramABuffer = lease.Accelerator.Allocate1D<float>(state.ParamA.Length);
        _paramBBuffer = lease.Accelerator.Allocate1D<float>(state.ParamB.Length);
        _preActivationThresholdBuffer = lease.Accelerator.Allocate1D<float>(state.PreActivationThreshold.Length);
        _activationThresholdBuffer = lease.Accelerator.Allocate1D<float>(state.ActivationThreshold.Length);
        _axonCountsBuffer = lease.Accelerator.Allocate1D<ushort>(state.AxonCounts.Length);
        _neuronAddressBuffer = lease.Accelerator.Allocate1D<uint>(state.NeuronCount);

        _bufferBuffer = lease.Accelerator.Allocate1D<float>(state.NeuronCount);
        _preResetBuffer = lease.Accelerator.Allocate1D<float>(state.NeuronCount);
        _inboxBuffer = lease.Accelerator.Allocate1D<float>(state.NeuronCount);
        _inboxHasInputBuffer = lease.Accelerator.Allocate1D<byte>(state.NeuronCount);
        _potentialBuffer = lease.Accelerator.Allocate1D<float>(state.NeuronCount);
        _activationRanBuffer = lease.Accelerator.Allocate1D<byte>(state.NeuronCount);
        _firedBuffer = lease.Accelerator.Allocate1D<byte>(state.NeuronCount);

        _accumulationFunctionsBuffer.CopyFromCPU(state.AccumulationFunctions);
        _activationFunctionsBuffer.CopyFromCPU(state.ActivationFunctions);
        _resetFunctionsBuffer.CopyFromCPU(state.ResetFunctions);
        _existsBuffer.CopyFromCPU(_existsBytes);
        _enabledBuffer.CopyFromCPU(_enabledBytes);
        _paramABuffer.CopyFromCPU(state.ParamA);
        _paramBBuffer.CopyFromCPU(state.ParamB);
        _preActivationThresholdBuffer.CopyFromCPU(state.PreActivationThreshold);
        _activationThresholdBuffer.CopyFromCPU(state.ActivationThreshold);
        _axonCountsBuffer.CopyFromCPU(state.AxonCounts);
        _neuronAddressBuffer.CopyFromCPU(BuildNeuronAddresses(state));

        _activationGroups = BuildGroups(state.ActivationFunctions, lease.Accelerator);
        _resetGroups = BuildGroups(state.ResetFunctions, lease.Accelerator);
    }

    /// <summary>
    /// Gets the backend label reported in execution metadata.
    /// </summary>
    public string BackendName => _backendName;

    /// <summary>
    /// Attempts to create an ILGPU backend using the preferred compatible accelerator.
    /// </summary>
    /// <param name="state">Shard state that the backend will execute against.</param>
    /// <returns>The initialized backend, or <see langword="null"/> when no compatible accelerator is available.</returns>
    public static RegionShardIlgpuBackend? TryCreate(RegionShardState state)
    {
        if (!RegionShardGpuRuntime.TryCreatePreferredAccelerator(out var lease, out _)
            || lease is null)
        {
            return null;
        }

        return new RegionShardIlgpuBackend(state, lease);
    }

    /// <summary>
    /// Checks whether the current shard state and compute options can run on the GPU backend.
    /// </summary>
    public RegionShardGpuSupport GetSupport(
        RegionShardVisualizationComputeScope? visualization,
        bool plasticityEnabled,
        bool probabilisticPlasticityUpdates,
        float plasticityDelta,
        uint plasticityRebaseThreshold,
        float plasticityRebaseThresholdPct,
        RegionShardHomeostasisConfig? homeostasisConfig,
        bool costEnergyEnabled,
        OutputVectorSource outputVectorSource)
    {
        if (!string.IsNullOrWhiteSpace(_unsupportedStateReason))
        {
            return new RegionShardGpuSupport(false, _unsupportedStateReason);
        }

        var vizScope = visualization ?? RegionShardVisualizationComputeScope.EnabledAll;
        if (vizScope.Enabled)
        {
            return new RegionShardGpuSupport(false, "visualization_not_supported_by_gpu_backend");
        }

        if (plasticityEnabled
            || probabilisticPlasticityUpdates
            || plasticityDelta > 0f
            || plasticityRebaseThreshold > 0
            || plasticityRebaseThresholdPct > 0f)
        {
            return new RegionShardGpuSupport(false, "plasticity_not_supported_by_gpu_backend");
        }

        var homeostasis = homeostasisConfig ?? RegionShardHomeostasisConfig.Default;
        if (homeostasis.Enabled
            && homeostasis.UpdateMode != HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep)
        {
            return new RegionShardGpuSupport(false, "homeostasis_update_mode_not_supported_by_gpu_backend");
        }

        if (outputVectorSource is not OutputVectorSource.Potential and not OutputVectorSource.Buffer)
        {
            return new RegionShardGpuSupport(false, "output_vector_source_not_supported_by_gpu_backend");
        }

        return new RegionShardGpuSupport(true, string.Empty);
    }

    /// <summary>
    /// Executes one shard compute step on the ILGPU backend.
    /// </summary>
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
        var support = GetSupport(
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
            throw new InvalidOperationException(support.Reason);
        }

        EnsurePrepareKernel();

        _bufferBuffer.CopyFromCPU(_state.Buffer);
        _inboxBuffer.CopyFromCPU(_state.Inbox);
        CopyInboxHasInputToScratch();
        _inboxHasInputBuffer.CopyFromCPU(_inboxHasInputScratch);

        var homeostasis = homeostasisConfig ?? RegionShardHomeostasisConfig.Default;
        var prepareConfig = new PrepareKernelConfig(
            tickId,
            _state.BrainSeed,
            homeostasis.Enabled ? (byte)1 : (byte)0,
            (int)homeostasis.TargetMode,
            (int)homeostasis.UpdateMode,
            homeostasis.BaseProbability,
            homeostasis.MinStepCodes,
            homeostasis.EnergyCouplingEnabled ? (byte)1 : (byte)0,
            homeostasis.EnergyTargetScale,
            homeostasis.EnergyProbabilityScale,
            costEnergyEnabled ? (byte)1 : (byte)0);
        try
        {
            _prepareKernel!(
                _state.NeuronCount,
                _accumulationFunctionsBuffer.View,
                _existsBuffer.View,
                _enabledBuffer.View,
                _bufferBuffer.View,
                _inboxBuffer.View,
                _inboxHasInputBuffer.View,
                _neuronAddressBuffer.View,
                _preResetBuffer.View,
                _potentialBuffer.View,
                _activationRanBuffer.View,
                _firedBuffer.View,
                prepareConfig);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"gpu_prepare_failed:{ex.GetBaseException().Message}", ex);
        }

        foreach (var group in _activationGroups)
        {
            try
            {
                var kernel = GetActivationKernel(group.Key);
                kernel(
                    group.Value.Count,
                    group.Value.Indices.View,
                    _preResetBuffer.View,
                    _existsBuffer.View,
                    _enabledBuffer.View,
                    _preActivationThresholdBuffer.View,
                    _paramABuffer.View,
                    _paramBBuffer.View,
                    _potentialBuffer.View,
                    _activationRanBuffer.View);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"gpu_activation_failed:function={group.Key}:{ex.GetBaseException().Message}", ex);
            }
        }

        foreach (var group in _resetGroups)
        {
            try
            {
                var kernel = GetResetKernel(group.Key);
                kernel(
                    group.Value.Count,
                    group.Value.Indices.View,
                    _bufferBuffer.View,
                    _potentialBuffer.View,
                    _activationThresholdBuffer.View,
                    _axonCountsBuffer.View,
                    _activationRanBuffer.View,
                    _firedBuffer.View);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"gpu_reset_failed:function={group.Key}:{ex.GetBaseException().Message}", ex);
            }
        }

        _lease.Accelerator.Synchronize();

        _bufferBuffer.CopyToCPU(_state.Buffer);
        _potentialBuffer.CopyToCPU(_potentialScratch);
        _activationRanBuffer.CopyToCPU(_activationRanScratch);
        _firedBuffer.CopyToCPU(_firedScratch);
        if (_preResetBufferScratch is not null)
        {
            _preResetBuffer.CopyToCPU(_preResetBufferScratch);
        }

        Array.Clear(_state.Inbox, 0, _state.Inbox.Length);
        Array.Clear(_state.InboxHasInput, 0, _state.InboxHasInput.Length);

        return BuildHostResult(
            tickId,
            brainId,
            shardId,
            routing,
            costEnergyEnabled,
            remoteCostEnabled,
            remoteCostPerBatch,
            remoteCostPerContribution,
            costTierAMultiplier,
            costTierBMultiplier,
            costTierCMultiplier,
            outputVectorSource);
    }

    /// <summary>
    /// Releases accelerator buffers and kernel grouping resources.
    /// </summary>
    public void Dispose()
    {
        foreach (var group in _activationGroups.Values)
        {
            group.Dispose();
        }

        foreach (var group in _resetGroups.Values)
        {
            group.Dispose();
        }

        _accumulationFunctionsBuffer.Dispose();
        _activationFunctionsBuffer.Dispose();
        _resetFunctionsBuffer.Dispose();
        _existsBuffer.Dispose();
        _enabledBuffer.Dispose();
        _paramABuffer.Dispose();
        _paramBBuffer.Dispose();
        _preActivationThresholdBuffer.Dispose();
        _activationThresholdBuffer.Dispose();
        _axonCountsBuffer.Dispose();
        _neuronAddressBuffer.Dispose();
        _bufferBuffer.Dispose();
        _preResetBuffer.Dispose();
        _inboxBuffer.Dispose();
        _inboxHasInputBuffer.Dispose();
        _potentialBuffer.Dispose();
        _activationRanBuffer.Dispose();
        _firedBuffer.Dispose();
        _lease.Dispose();
    }

    private RegionShardComputeResult BuildHostResult(
        ulong tickId,
        Guid brainId,
        ShardId32 shardId,
        RegionShardRoutingTable routing,
        bool costEnergyEnabled,
        bool? remoteCostEnabled,
        long? remoteCostPerBatch,
        long? remoteCostPerContribution,
        float? costTierAMultiplier,
        float? costTierBMultiplier,
        float? costTierCMultiplier,
        OutputVectorSource outputVectorSource)
    {
        routing ??= RegionShardRoutingTable.CreateSingleShard(_state.RegionId, _state.NeuronCount);
        var effectiveRemoteCostEnabled = remoteCostEnabled ?? _costConfig.RemoteCostEnabled;
        var effectiveRemoteCostPerBatch = Math.Max(0L, remoteCostPerBatch ?? _costConfig.RemoteCostPerBatch);
        var effectiveRemoteCostPerContribution = Math.Max(0L, remoteCostPerContribution ?? _costConfig.RemoteCostPerContribution);
        var tierAMultiplier = NormalizeTierMultiplier(costTierAMultiplier ?? _costConfig.TierAMultiplier);
        var tierBMultiplier = NormalizeTierMultiplier(costTierBMultiplier ?? _costConfig.TierBMultiplier);
        var tierCMultiplier = NormalizeTierMultiplier(costTierCMultiplier ?? _costConfig.TierCMultiplier);
        var accumCostLookup = costEnergyEnabled
            ? BuildWeightedLookup(AccumulationBaseCosts, AccumulationTiers, tierAMultiplier, tierBMultiplier, tierCMultiplier)
            : null;
        var activationCostLookup = costEnergyEnabled
            ? BuildWeightedLookup(ActivationBaseCosts, ActivationTiers, tierAMultiplier, tierBMultiplier, tierCMultiplier)
            : null;
        var resetCostLookup = costEnergyEnabled
            ? BuildWeightedLookup(ResetBaseCosts, ResetTiers, tierAMultiplier, tierBMultiplier, tierCMultiplier)
            : null;

        var outbox = new Dictionary<ShardId32, List<Contribution>>();
        var outputs = _state.IsOutputRegion ? new List<OutputEvent>() : null;
        float[]? outputVector = _state.IsOutputRegion ? new float[_state.NeuronCount] : null;
        var outputVectorFromBuffer = outputVector is not null && outputVectorSource == OutputVectorSource.Buffer;
        var brainProto = brainId.ToProtoUuid();
        var regionDistanceCache = new int?[NbnConstants.RegionCount];
        var sourceRegionZ = RegionZ(_state.RegionId);

        double costAccum = 0d;
        double costActivation = 0d;
        double costReset = 0d;
        long costDistance = 0L;
        long costRemote = 0L;
        uint firedCount = 0;
        uint outContribs = 0;

        for (var i = 0; i < _state.NeuronCount; i++)
        {
            if (costEnergyEnabled)
            {
                costAccum += ResolveFunctionCost(accumCostLookup!, _state.AccumulationFunctions[i]);
            }

            if (!_state.Exists[i] || !_state.Enabled[i])
            {
                continue;
            }

            var sourceBuffer = outputVectorFromBuffer && _preResetBufferScratch is not null
                ? _preResetBufferScratch[i]
                : _state.Buffer[i];
            if (outputVectorFromBuffer)
            {
                outputVector![i] = sourceBuffer;
            }

            if (_activationRanScratch[i] == 0)
            {
                if (outputVector is not null && !outputVectorFromBuffer)
                {
                    outputVector[i] = 0f;
                }

                continue;
            }

            if (costEnergyEnabled)
            {
                costActivation += ResolveFunctionCost(activationCostLookup!, _state.ActivationFunctions[i]);
                costReset += ResolveFunctionCost(resetCostLookup!, _state.ResetFunctions[i]);
            }

            var potential = ClampSignal(_potentialScratch[i]);
            if (outputVector is not null && !outputVectorFromBuffer)
            {
                outputVector[i] = potential;
            }

            var activationThreshold = NormalizeActivationThreshold(_state.ActivationThreshold[i]);
            if (_firedScratch[i] == 0 || MathF.Abs(potential) <= activationThreshold)
            {
                continue;
            }

            firedCount++;
            if (outputs is not null)
            {
                outputs.Add(new OutputEvent
                {
                    BrainId = brainProto,
                    OutputIndex = (uint)(_state.NeuronStart + i),
                    Value = potential,
                    TickId = tickId
                });
            }

            var axonCount = _state.AxonCounts[i];
            if (axonCount == 0)
            {
                continue;
            }

            var axonStart = _state.AxonStartOffsets[i];
            var sourceNeuronId = _state.NeuronStart + i;
            for (var a = 0; a < axonCount; a++)
            {
                var axonIndex = axonStart + a;
                var destRegion = _state.Axons.TargetRegionIds[axonIndex];
                var destNeuron = _state.Axons.TargetNeuronIds[axonIndex];
                var strength = NormalizeAxonStrength(axonIndex);
                var value = ClampSignal(potential * strength);

                if (!routing.TryGetShard(destRegion, destNeuron, out var destShard))
                {
                    destShard = ShardId32.From(destRegion, 0);
                }

                if (!outbox.TryGetValue(destShard, out var list))
                {
                    list = new List<Contribution>();
                    outbox[destShard] = list;
                }

                list.Add(new Contribution
                {
                    TargetNeuronId = (uint)destNeuron,
                    Value = value
                });
                outContribs++;

                if (costEnergyEnabled)
                {
                    var distanceUnits = ComputeDistanceUnits(sourceNeuronId, destRegion, destNeuron, sourceRegionZ, regionDistanceCache);
                    costDistance += _costConfig.AxonBaseCost + (_costConfig.AxonUnitCost * distanceUnits);
                }
            }
        }

        if (costEnergyEnabled
            && effectiveRemoteCostEnabled
            && (effectiveRemoteCostPerBatch != 0 || effectiveRemoteCostPerContribution != 0))
        {
            long remoteBatchCount = 0;
            long remoteContributionCount = 0;
            foreach (var (destinationShard, contribs) in outbox)
            {
                if (destinationShard.Equals(shardId))
                {
                    continue;
                }

                remoteBatchCount++;
                remoteContributionCount += contribs.Count;
            }

            costRemote = checked((remoteBatchCount * effectiveRemoteCostPerBatch) + (remoteContributionCount * effectiveRemoteCostPerContribution));
        }

        var costAccumUnits = costEnergyEnabled ? RoundToCostUnits(costAccum) : 0L;
        var costActivationUnits = costEnergyEnabled ? RoundToCostUnits(costActivation) : 0L;
        var costResetUnits = costEnergyEnabled ? RoundToCostUnits(costReset) : 0L;
        var tickCostTotal = checked(costAccumUnits + costActivationUnits + costResetUnits + costDistance + costRemote);

        return new RegionShardComputeResult(
            outbox,
            outputs ?? (IReadOnlyList<OutputEvent>)Array.Empty<OutputEvent>(),
            outputVector ?? Array.Empty<float>(),
            firedCount,
            outContribs,
            0,
            new RegionShardCostSummary(tickCostTotal, costAccumUnits, costActivationUnits, costResetUnits, costDistance, costRemote),
            Array.Empty<RegionShardAxonVizEvent>(),
            Array.Empty<RegionShardNeuronBufferVizEvent>(),
            Array.Empty<RegionShardNeuronVizEvent>());
    }

    private static byte[] ToByteFlags(bool[] source)
    {
        var flags = new byte[source.Length];
        for (var i = 0; i < source.Length; i++)
        {
            flags[i] = source[i] ? (byte)1 : (byte)0;
        }

        return flags;
    }

    private static uint[] BuildNeuronAddresses(RegionShardState state)
    {
        var addresses = new uint[state.NeuronCount];
        for (var i = 0; i < state.NeuronCount; i++)
        {
            addresses[i] = ComposeAddress(state.RegionId, state.NeuronStart + i);
        }

        return addresses;
    }

    private static Dictionary<byte, NeuronIndexGroup> BuildGroups(byte[] functions, Accelerator accelerator)
    {
        var groups = functions
            .Select((functionId, index) => (functionId, index))
            .GroupBy(static entry => entry.functionId, static entry => entry.index)
            .ToDictionary(
                static group => group.Key,
                group => new NeuronIndexGroup(group.Key, group.ToArray(), accelerator));
        return groups;
    }

    private void CopyInboxHasInputToScratch()
    {
        for (var i = 0; i < _state.NeuronCount; i++)
        {
            _inboxHasInputScratch[i] = _state.InboxHasInput[i] ? (byte)1 : (byte)0;
        }
    }

    private void EnsurePrepareKernel()
    {
        _prepareKernel ??= _lease.Accelerator.LoadAutoGroupedStreamKernel<
            Index1D,
            ArrayView1D<byte, Stride1D.Dense>,
            ArrayView1D<byte, Stride1D.Dense>,
            ArrayView1D<byte, Stride1D.Dense>,
            ArrayView1D<float, Stride1D.Dense>,
            ArrayView1D<float, Stride1D.Dense>,
            ArrayView1D<byte, Stride1D.Dense>,
            ArrayView1D<uint, Stride1D.Dense>,
            ArrayView1D<float, Stride1D.Dense>,
            ArrayView1D<float, Stride1D.Dense>,
            ArrayView1D<byte, Stride1D.Dense>,
            ArrayView1D<byte, Stride1D.Dense>,
            PrepareKernelConfig>(PrepareKernel);
    }

    private Action<
        Index1D,
        ArrayView1D<int, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<byte, Stride1D.Dense>,
        ArrayView1D<byte, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<byte, Stride1D.Dense>> GetActivationKernel(byte functionId)
    {
        if (_activationKernels.TryGetValue(functionId, out var kernel))
        {
            return kernel;
        }

        kernel = functionId switch
        {
            (byte)ActivationFunction.ActNone => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ActivationNoneKernel),
            (byte)ActivationFunction.ActIdentity => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ActivationIdentityKernel),
            (byte)ActivationFunction.ActClamp => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ActivationClampKernel),
            (byte)ActivationFunction.ActRelu => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ActivationReluKernel),
            (byte)ActivationFunction.ActNrelu => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ActivationNreluKernel),
            (byte)ActivationFunction.ActTanh => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ActivationTanhKernel),
            (byte)ActivationFunction.ActExp => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ActivationExpKernel),
            (byte)ActivationFunction.ActPrelu => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ActivationPreluKernel),
            (byte)ActivationFunction.ActMult => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ActivationMultKernel),
            (byte)ActivationFunction.ActAdd => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ActivationAddKernel),
            (byte)ActivationFunction.ActSig => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ActivationSigKernel),
            (byte)ActivationFunction.ActSilu => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ActivationSiluKernel),
            (byte)ActivationFunction.ActPclamp => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ActivationPclampKernel),
            (byte)ActivationFunction.ActLin => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ActivationLinKernel),
            _ => throw new InvalidOperationException($"Unsupported GPU activation function id {functionId}.")
        };

        _activationKernels[functionId] = kernel;
        return kernel;
    }

    private Action<
        Index1D,
        ArrayView1D<int, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<float, Stride1D.Dense>,
        ArrayView1D<ushort, Stride1D.Dense>,
        ArrayView1D<byte, Stride1D.Dense>,
        ArrayView1D<byte, Stride1D.Dense>> GetResetKernel(byte functionId)
    {
        if (_resetKernels.TryGetValue(functionId, out var kernel))
        {
            return kernel;
        }

        kernel = functionId switch
        {
            (byte)ResetFunction.ResetZero => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<ushort, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ResetZeroKernel),
            (byte)ResetFunction.ResetHold => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<ushort, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ResetHoldKernel),
            (byte)ResetFunction.ResetClamp1 => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<ushort, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ResetClamp1Kernel),
            (byte)ResetFunction.ResetPotential => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<ushort, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ResetPotentialKernel),
            (byte)ResetFunction.ResetDivideAxonCt => _lease.Accelerator.LoadAutoGroupedStreamKernel<
                Index1D, ArrayView1D<int, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<float, Stride1D.Dense>, ArrayView1D<ushort, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>, ArrayView1D<byte, Stride1D.Dense>>(ResetDivideAxonCountKernel),
            _ => throw new InvalidOperationException($"Unsupported GPU reset function id {functionId}.")
        };

        _resetKernels[functionId] = kernel;
        return kernel;
    }

    private static string? ResolveUnsupportedStateReason(RegionShardState state)
    {
        foreach (var functionId in state.ActivationFunctions.Distinct())
        {
            if (!IsSupportedActivation(functionId))
            {
                return $"activation_function_not_supported_by_gpu_backend:{functionId}";
            }
        }

        foreach (var functionId in state.ResetFunctions.Distinct())
        {
            if (!IsSupportedReset(functionId))
            {
                return $"reset_function_not_supported_by_gpu_backend:{functionId}";
            }
        }

        return null;
    }

    private static bool IsSupportedActivation(byte functionId)
        => functionId is
            (byte)ActivationFunction.ActNone or
            (byte)ActivationFunction.ActIdentity or
            (byte)ActivationFunction.ActClamp or
            (byte)ActivationFunction.ActRelu or
            (byte)ActivationFunction.ActNrelu or
            (byte)ActivationFunction.ActPrelu or
            (byte)ActivationFunction.ActMult or
            (byte)ActivationFunction.ActAdd or
            (byte)ActivationFunction.ActPclamp or
            (byte)ActivationFunction.ActLin;

    private static bool IsSupportedReset(byte functionId)
        => functionId is
            (byte)ResetFunction.ResetZero or
            (byte)ResetFunction.ResetHold or
            (byte)ResetFunction.ResetClamp1 or
            (byte)ResetFunction.ResetPotential or
            (byte)ResetFunction.ResetDivideAxonCt;

    private static void PrepareKernel(
        Index1D index,
        ArrayView1D<byte, Stride1D.Dense> accumulationFunctions,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<float, Stride1D.Dense> inbox,
        ArrayView1D<byte, Stride1D.Dense> inboxHasInput,
        ArrayView1D<uint, Stride1D.Dense> addresses,
        ArrayView1D<float, Stride1D.Dense> preResetBuffer,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan,
        ArrayView1D<byte, Stride1D.Dense> fired,
        PrepareKernelConfig config)
    {
        var bufferValue = buffer[index];
        var inboxValue = inbox[index];
        var hasInput = inboxHasInput[index] != 0;
        switch ((AccumulationFunction)accumulationFunctions[index])
        {
            case AccumulationFunction.AccumProduct:
                if (hasInput)
                {
                    bufferValue *= inboxValue;
                }
                break;
            case AccumulationFunction.AccumMax:
                if (hasInput)
                {
                    bufferValue = Max(bufferValue, inboxValue);
                }
                break;
            case AccumulationFunction.AccumNone:
                break;
            default:
                bufferValue += inboxValue;
                break;
        }

        bufferValue = ClampSignal(bufferValue);

        if (exists[index] != 0
            && enabled[index] != 0
            && config.HomeostasisEnabled != 0
            && config.UpdateMode == (int)HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep)
        {
            var probability = ClampFinite(config.BaseProbability, 0f, 1f, 0f);
            if (probability > 0f)
            {
                if (config.EnergyCouplingEnabled != 0 && config.CostEnergyEnabled != 0)
                {
                    var probabilityScale = ClampFinite(config.EnergyProbabilityScale, 0f, 4f, 1f);
                    probability = Clamp(probability * probabilityScale, 0f, 1f);
                }

                if (probability > 0f)
                {
                    var seed = MixToU64(config.BrainSeed, config.TickId, addresses[index], addresses[index]);
                    if (UnitIntervalFromSeed(seed) < probability)
                    {
                        var target = config.TargetMode == (int)HomeostasisTargetMode.HomeostasisTargetZero
                            || config.TargetMode == (int)HomeostasisTargetMode.HomeostasisTargetFixed
                            ? 0f
                            : 0f;
                        if (config.EnergyCouplingEnabled != 0 && config.CostEnergyEnabled != 0)
                        {
                            var targetScale = ClampFinite(config.EnergyTargetScale, 0f, 4f, 1f);
                            target *= targetScale;
                        }

                        var currentCode = EncodeBuffer(bufferValue);
                        var targetCode = EncodeBuffer(target);
                        if (currentCode != targetCode)
                        {
                            var maxStep = DefaultBufferMaxCode;
                            var requestedStep = config.MinStepCodes == 0 ? 1 : Min((int)config.MinStepCodes, maxStep);
                            var stepCodes = Max(1, Min(requestedStep, maxStep));
                            var nextCode = currentCode;
                            if (targetCode > currentCode)
                            {
                                nextCode = Min(currentCode + stepCodes, targetCode);
                            }
                            else
                            {
                                nextCode = Max(currentCode - stepCodes, targetCode);
                            }

                            if (nextCode != currentCode)
                            {
                                bufferValue = DecodeBuffer(nextCode);
                            }
                        }
                    }
                }
            }
        }

        bufferValue = ClampSignal(bufferValue);
        buffer[index] = bufferValue;
        preResetBuffer[index] = bufferValue;
        inbox[index] = 0f;
        inboxHasInput[index] = 0;
        potentials[index] = 0f;
        activationRan[index] = 0;
        fired[index] = 0;
    }

    private static void ActivationNoneKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> preActivationThreshold,
        ArrayView1D<float, Stride1D.Dense> paramA,
        ArrayView1D<float, Stride1D.Dense> paramB,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan)
    {
        if (!TryBeginActivation(index, indices, buffer, exists, enabled, preActivationThreshold, potentials, activationRan, out var neuronIndex, out var bufferValue))
        {
            return;
        }

        FinishActivation(neuronIndex, 0f, potentials, activationRan);
    }

    private static void ActivationIdentityKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> preActivationThreshold,
        ArrayView1D<float, Stride1D.Dense> paramA,
        ArrayView1D<float, Stride1D.Dense> paramB,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan)
    {
        if (!TryBeginActivation(index, indices, buffer, exists, enabled, preActivationThreshold, potentials, activationRan, out var neuronIndex, out var bufferValue))
        {
            return;
        }

        FinishActivation(neuronIndex, bufferValue, potentials, activationRan);
    }

    private static void ActivationClampKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> preActivationThreshold,
        ArrayView1D<float, Stride1D.Dense> paramA,
        ArrayView1D<float, Stride1D.Dense> paramB,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan)
    {
        if (!TryBeginActivation(index, indices, buffer, exists, enabled, preActivationThreshold, potentials, activationRan, out var neuronIndex, out var bufferValue))
        {
            return;
        }

        FinishActivation(neuronIndex, Clamp(bufferValue, -1f, 1f), potentials, activationRan);
    }

    private static void ActivationReluKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> preActivationThreshold,
        ArrayView1D<float, Stride1D.Dense> paramA,
        ArrayView1D<float, Stride1D.Dense> paramB,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan)
    {
        if (!TryBeginActivation(index, indices, buffer, exists, enabled, preActivationThreshold, potentials, activationRan, out var neuronIndex, out var bufferValue))
        {
            return;
        }

        FinishActivation(neuronIndex, Max(0f, bufferValue), potentials, activationRan);
    }

    private static void ActivationNreluKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> preActivationThreshold,
        ArrayView1D<float, Stride1D.Dense> paramA,
        ArrayView1D<float, Stride1D.Dense> paramB,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan)
    {
        if (!TryBeginActivation(index, indices, buffer, exists, enabled, preActivationThreshold, potentials, activationRan, out var neuronIndex, out var bufferValue))
        {
            return;
        }

        FinishActivation(neuronIndex, Min(bufferValue, 0f), potentials, activationRan);
    }

    private static void ActivationTanhKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> preActivationThreshold,
        ArrayView1D<float, Stride1D.Dense> paramA,
        ArrayView1D<float, Stride1D.Dense> paramB,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan)
    {
        if (!TryBeginActivation(index, indices, buffer, exists, enabled, preActivationThreshold, potentials, activationRan, out var neuronIndex, out var bufferValue))
        {
            return;
        }

        FinishActivation(neuronIndex, MathF.Tanh(bufferValue), potentials, activationRan);
    }

    private static void ActivationExpKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> preActivationThreshold,
        ArrayView1D<float, Stride1D.Dense> paramA,
        ArrayView1D<float, Stride1D.Dense> paramB,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan)
    {
        if (!TryBeginActivation(index, indices, buffer, exists, enabled, preActivationThreshold, potentials, activationRan, out var neuronIndex, out var bufferValue))
        {
            return;
        }

        FinishActivation(neuronIndex, MathF.Exp(bufferValue), potentials, activationRan);
    }

    private static void ActivationPreluKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> preActivationThreshold,
        ArrayView1D<float, Stride1D.Dense> paramA,
        ArrayView1D<float, Stride1D.Dense> paramB,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan)
    {
        if (!TryBeginActivation(index, indices, buffer, exists, enabled, preActivationThreshold, potentials, activationRan, out var neuronIndex, out var bufferValue))
        {
            return;
        }

        var a = paramA[neuronIndex];
        FinishActivation(neuronIndex, bufferValue >= 0f ? bufferValue : a * bufferValue, potentials, activationRan);
    }

    private static void ActivationMultKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> preActivationThreshold,
        ArrayView1D<float, Stride1D.Dense> paramA,
        ArrayView1D<float, Stride1D.Dense> paramB,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan)
    {
        if (!TryBeginActivation(index, indices, buffer, exists, enabled, preActivationThreshold, potentials, activationRan, out var neuronIndex, out var bufferValue))
        {
            return;
        }

        FinishActivation(neuronIndex, bufferValue * paramA[neuronIndex], potentials, activationRan);
    }

    private static void ActivationAddKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> preActivationThreshold,
        ArrayView1D<float, Stride1D.Dense> paramA,
        ArrayView1D<float, Stride1D.Dense> paramB,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan)
    {
        if (!TryBeginActivation(index, indices, buffer, exists, enabled, preActivationThreshold, potentials, activationRan, out var neuronIndex, out var bufferValue))
        {
            return;
        }

        FinishActivation(neuronIndex, bufferValue + paramA[neuronIndex], potentials, activationRan);
    }

    private static void ActivationSigKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> preActivationThreshold,
        ArrayView1D<float, Stride1D.Dense> paramA,
        ArrayView1D<float, Stride1D.Dense> paramB,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan)
    {
        if (!TryBeginActivation(index, indices, buffer, exists, enabled, preActivationThreshold, potentials, activationRan, out var neuronIndex, out var bufferValue))
        {
            return;
        }

        FinishActivation(neuronIndex, 1f / (1f + MathF.Exp(-bufferValue)), potentials, activationRan);
    }

    private static void ActivationSiluKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> preActivationThreshold,
        ArrayView1D<float, Stride1D.Dense> paramA,
        ArrayView1D<float, Stride1D.Dense> paramB,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan)
    {
        if (!TryBeginActivation(index, indices, buffer, exists, enabled, preActivationThreshold, potentials, activationRan, out var neuronIndex, out var bufferValue))
        {
            return;
        }

        FinishActivation(neuronIndex, bufferValue / (1f + MathF.Exp(-bufferValue)), potentials, activationRan);
    }

    private static void ActivationPclampKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> preActivationThreshold,
        ArrayView1D<float, Stride1D.Dense> paramA,
        ArrayView1D<float, Stride1D.Dense> paramB,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan)
    {
        if (!TryBeginActivation(index, indices, buffer, exists, enabled, preActivationThreshold, potentials, activationRan, out var neuronIndex, out var bufferValue))
        {
            return;
        }

        var a = paramA[neuronIndex];
        var b = paramB[neuronIndex];
        FinishActivation(neuronIndex, b <= a ? 0f : Clamp(bufferValue, a, b), potentials, activationRan);
    }

    private static void ActivationLinKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> preActivationThreshold,
        ArrayView1D<float, Stride1D.Dense> paramA,
        ArrayView1D<float, Stride1D.Dense> paramB,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan)
    {
        if (!TryBeginActivation(index, indices, buffer, exists, enabled, preActivationThreshold, potentials, activationRan, out var neuronIndex, out var bufferValue))
        {
            return;
        }

        FinishActivation(neuronIndex, (paramA[neuronIndex] * bufferValue) + paramB[neuronIndex], potentials, activationRan);
    }

    private static void ResetZeroKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<float, Stride1D.Dense> activationThreshold,
        ArrayView1D<ushort, Stride1D.Dense> axonCounts,
        ArrayView1D<byte, Stride1D.Dense> activationRan,
        ArrayView1D<byte, Stride1D.Dense> fired)
    {
        if (!TryBeginReset(index, indices, buffer, potentials, activationThreshold, axonCounts, activationRan, fired, out var neuronIndex, out _, out _, out _, out _))
        {
            return;
        }

        FinishReset(neuronIndex, 0f, buffer, fired);
    }

    private static void ResetHoldKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<float, Stride1D.Dense> activationThreshold,
        ArrayView1D<ushort, Stride1D.Dense> axonCounts,
        ArrayView1D<byte, Stride1D.Dense> activationRan,
        ArrayView1D<byte, Stride1D.Dense> fired)
    {
        if (!TryBeginReset(index, indices, buffer, potentials, activationThreshold, axonCounts, activationRan, fired, out var neuronIndex, out _, out var threshold, out var bufferValue, out _))
        {
            return;
        }

        FinishReset(neuronIndex, Clamp(bufferValue, -threshold, threshold), buffer, fired);
    }

    private static void ResetClamp1Kernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<float, Stride1D.Dense> activationThreshold,
        ArrayView1D<ushort, Stride1D.Dense> axonCounts,
        ArrayView1D<byte, Stride1D.Dense> activationRan,
        ArrayView1D<byte, Stride1D.Dense> fired)
    {
        if (!TryBeginReset(index, indices, buffer, potentials, activationThreshold, axonCounts, activationRan, fired, out var neuronIndex, out _, out _, out var bufferValue, out _))
        {
            return;
        }

        FinishReset(neuronIndex, Clamp(bufferValue, -1f, 1f), buffer, fired);
    }

    private static void ResetPotentialKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<float, Stride1D.Dense> activationThreshold,
        ArrayView1D<ushort, Stride1D.Dense> axonCounts,
        ArrayView1D<byte, Stride1D.Dense> activationRan,
        ArrayView1D<byte, Stride1D.Dense> fired)
    {
        if (!TryBeginReset(index, indices, buffer, potentials, activationThreshold, axonCounts, activationRan, fired, out var neuronIndex, out var potential, out var threshold, out _, out _))
        {
            return;
        }

        FinishReset(neuronIndex, Clamp(potential, -threshold, threshold), buffer, fired);
    }

    private static void ResetDivideAxonCountKernel(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<float, Stride1D.Dense> activationThreshold,
        ArrayView1D<ushort, Stride1D.Dense> axonCounts,
        ArrayView1D<byte, Stride1D.Dense> activationRan,
        ArrayView1D<byte, Stride1D.Dense> fired)
    {
        if (!TryBeginReset(index, indices, buffer, potentials, activationThreshold, axonCounts, activationRan, fired, out var neuronIndex, out _, out var threshold, out var bufferValue, out var outDegree))
        {
            return;
        }

        FinishReset(neuronIndex, Clamp(bufferValue / Max(1f, outDegree), -threshold, threshold), buffer, fired);
    }

    private static bool TryBeginActivation(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> exists,
        ArrayView1D<byte, Stride1D.Dense> enabled,
        ArrayView1D<float, Stride1D.Dense> preActivationThreshold,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan,
        out int neuronIndex,
        out float bufferValue)
    {
        neuronIndex = indices[index];
        bufferValue = buffer[neuronIndex];
        if (exists[neuronIndex] == 0 || enabled[neuronIndex] == 0)
        {
            activationRan[neuronIndex] = 0;
            potentials[neuronIndex] = 0f;
            return false;
        }

        var threshold = NormalizePreActivationThreshold(preActivationThreshold[neuronIndex]);
        if (bufferValue <= threshold)
        {
            activationRan[neuronIndex] = 0;
            potentials[neuronIndex] = 0f;
            return false;
        }

        return true;
    }

    private static void FinishActivation(
        int neuronIndex,
        float potential,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<byte, Stride1D.Dense> activationRan)
    {
        potentials[neuronIndex] = ClampSignal(potential);
        activationRan[neuronIndex] = 1;
    }

    private static bool TryBeginReset(
        Index1D index,
        ArrayView1D<int, Stride1D.Dense> indices,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<float, Stride1D.Dense> potentials,
        ArrayView1D<float, Stride1D.Dense> activationThreshold,
        ArrayView1D<ushort, Stride1D.Dense> axonCounts,
        ArrayView1D<byte, Stride1D.Dense> activationRan,
        ArrayView1D<byte, Stride1D.Dense> fired,
        out int neuronIndex,
        out float potential,
        out float threshold,
        out float bufferValue,
        out float outDegree)
    {
        neuronIndex = indices[index];
        potential = 0f;
        threshold = 0f;
        bufferValue = 0f;
        outDegree = 0f;
        if (activationRan[neuronIndex] == 0)
        {
            fired[neuronIndex] = 0;
            return false;
        }

        potential = ClampSignal(potentials[neuronIndex]);
        threshold = NormalizeActivationThreshold(activationThreshold[neuronIndex]);
        bufferValue = buffer[neuronIndex];
        outDegree = axonCounts[neuronIndex];
        fired[neuronIndex] = MathF.Abs(potential) > threshold ? (byte)1 : (byte)0;
        return true;
    }

    private static void FinishReset(
        int neuronIndex,
        float resetValue,
        ArrayView1D<float, Stride1D.Dense> buffer,
        ArrayView1D<byte, Stride1D.Dense> fired)
    {
        buffer[neuronIndex] = ClampSignal(resetValue);
    }

    private static double[] BuildWeightedLookup(
        double[] baseCosts,
        CostTier[] tiers,
        float tierAMultiplier,
        float tierBMultiplier,
        float tierCMultiplier)
    {
        var weighted = new double[baseCosts.Length];
        for (var i = 0; i < baseCosts.Length; i++)
        {
            weighted[i] = baseCosts[i] * ResolveTierMultiplier(tiers[i], tierAMultiplier, tierBMultiplier, tierCMultiplier);
        }

        return weighted;
    }

    private static long ComputeDistanceUnits(int sourceNeuronId, byte destRegionId, int destNeuronId, int sourceRegionZ, int?[] regionDistanceCache, RegionShardCostConfig costConfig)
    {
        var destRegion = (int)destRegionId;
        var regionDist = regionDistanceCache[destRegion] ??= ComputeRegionDistance(sourceRegionZ, destRegion, costConfig);
        var span = destRegion >= 0 && destRegion < NbnConstants.RegionCount ? 0 : 0;
        span = destRegion >= 0 && destRegion < NbnConstants.RegionCount ? span : 0;
        var regionSpan = span;
        if (destRegion >= 0 && destRegion < NbnConstants.RegionCount)
        {
            regionSpan = 0;
        }

        var d = Math.Abs(sourceNeuronId - destNeuronId);
        var wrap = regionSpan > 0 && d < regionSpan ? Math.Min(d, regionSpan - d) : d;
        var neuronUnits = costConfig.NeuronDistShift > 0 ? wrap >> costConfig.NeuronDistShift : wrap;
        return (costConfig.RegionWeight * regionDist) + neuronUnits;
    }

    private long ComputeDistanceUnits(int sourceNeuronId, byte destRegionId, int destNeuronId, int sourceRegionZ, int?[] regionDistanceCache)
    {
        var destRegion = (int)destRegionId;
        var regionDist = regionDistanceCache[destRegion] ??= ComputeRegionDistance(sourceRegionZ, destRegion, _costConfig);
        var span = destRegion >= 0 && destRegion < _state.RegionSpans.Length ? _state.RegionSpans[destRegion] : 0;
        var d = Math.Abs(sourceNeuronId - destNeuronId);
        var wrap = span > 0 && d < span ? Math.Min(d, span - d) : d;
        var neuronUnits = _costConfig.NeuronDistShift > 0 ? wrap >> _costConfig.NeuronDistShift : wrap;
        return (_costConfig.RegionWeight * regionDist) + neuronUnits;
    }

    private static int ComputeRegionDistance(int sourceRegionZ, int destRegionId, RegionShardCostConfig costConfig)
    {
        if (destRegionId == 0)
        {
            if (sourceRegionZ == -3)
            {
                return 0;
            }
        }

        var destZ = RegionZ(destRegionId);
        if (destZ == sourceRegionZ)
        {
            return costConfig.RegionIntrasliceUnit;
        }

        return costConfig.RegionAxialUnit * Math.Abs(destZ - sourceRegionZ);
    }

    private static int RegionZ(int regionId)
    {
        if (regionId == 0)
        {
            return -3;
        }

        if (regionId <= 3)
        {
            return -2;
        }

        if (regionId <= 8)
        {
            return -1;
        }

        if (regionId <= 22)
        {
            return 0;
        }

        if (regionId <= 27)
        {
            return 1;
        }

        if (regionId <= 30)
        {
            return 2;
        }

        return 3;
    }

    private float NormalizeAxonStrength(int axonIndex)
    {
        var strength = _state.Axons.Strengths[axonIndex];
        if (!float.IsFinite(strength))
        {
            strength = 0f;
        }

        var min = MathF.Min(_state.StrengthQuantization.Min, _state.StrengthQuantization.Max);
        var max = MathF.Max(_state.StrengthQuantization.Min, _state.StrengthQuantization.Max);
        return Math.Clamp(strength, min, max);
    }

    private static double ResolveFunctionCost(double[] weightedLookup, byte functionId)
    {
        var index = (int)functionId;
        if ((uint)index >= (uint)weightedLookup.Length)
        {
            return weightedLookup[0];
        }

        return weightedLookup[index];
    }

    private static long RoundToCostUnits(double value)
        => (long)Math.Round(value, MidpointRounding.AwayFromZero);

    private static float NormalizeTierMultiplier(float value)
        => float.IsFinite(value) && value > 0f ? value : 1f;

    private static float ResolveTierMultiplier(CostTier tier, float tierAMultiplier, float tierBMultiplier, float tierCMultiplier)
        => tier switch
        {
            CostTier.B => tierBMultiplier,
            CostTier.C => tierCMultiplier,
            _ => tierAMultiplier
        };

    private static double[] BuildActivationBaseCosts()
    {
        var costs = new double[64];
        for (var i = 0; i < costs.Length; i++)
        {
            costs[i] = 1.0;
        }

        costs[(int)ActivationFunction.ActNone] = 0.0;
        costs[(int)ActivationFunction.ActIdentity] = 1.0;
        costs[(int)ActivationFunction.ActClamp] = 1.1;
        costs[(int)ActivationFunction.ActRelu] = 1.1;
        costs[(int)ActivationFunction.ActNrelu] = 1.1;
        costs[(int)ActivationFunction.ActTanh] = 1.6;
        costs[(int)ActivationFunction.ActExp] = 1.8;
        costs[(int)ActivationFunction.ActPrelu] = 1.4;
        costs[(int)ActivationFunction.ActMult] = 1.2;
        costs[(int)ActivationFunction.ActAdd] = 1.2;
        costs[(int)ActivationFunction.ActSig] = 2.0;
        costs[(int)ActivationFunction.ActSilu] = 2.0;
        costs[(int)ActivationFunction.ActPclamp] = 1.3;
        costs[(int)ActivationFunction.ActLin] = 1.4;
        return costs;
    }

    private static CostTier[] BuildActivationTiers()
    {
        var tiers = new CostTier[64];
        for (var i = 0; i < tiers.Length; i++)
        {
            tiers[i] = CostTier.A;
        }

        tiers[(int)ActivationFunction.ActTanh] = CostTier.B;
        tiers[(int)ActivationFunction.ActExp] = CostTier.B;
        tiers[(int)ActivationFunction.ActPrelu] = CostTier.B;
        tiers[(int)ActivationFunction.ActSig] = CostTier.B;
        tiers[(int)ActivationFunction.ActSilu] = CostTier.B;
        return tiers;
    }

    private static double[] BuildResetBaseCosts()
    {
        var costs = new double[64];
        for (var i = 0; i < costs.Length; i++)
        {
            costs[i] = 1.0;
        }

        costs[(int)ResetFunction.ResetZero] = 0.2;
        costs[(int)ResetFunction.ResetHold] = 1.0;
        costs[(int)ResetFunction.ResetClamp1] = 1.0;
        costs[(int)ResetFunction.ResetPotential] = 1.0;
        costs[(int)ResetFunction.ResetDivideAxonCt] = 1.1;
        return costs;
    }

    private static CostTier[] BuildResetTiers()
    {
        var tiers = new CostTier[64];
        for (var i = 0; i < tiers.Length; i++)
        {
            tiers[i] = CostTier.A;
        }

        return tiers;
    }

    private static uint ComposeAddress(int regionId, int neuronId)
        => ((uint)regionId << NbnConstants.AddressNeuronBits) | ((uint)neuronId & NbnConstants.AddressNeuronMask);

    private static int EncodeBuffer(float value)
    {
        var centerHi = (DefaultBufferMaxCode + 1) / 2;
        var centerLo = centerHi - 1;
        var clamped = Clamp(value, DefaultBufferMin, DefaultBufferMax);
        var t = Clamp(clamped / Max(MathF.Abs(DefaultBufferMin), MathF.Abs(DefaultBufferMax)), -1f, 1f);
        t = ApplySignedPow(t, 1f / DefaultBufferGamma);
        int code;
        if (t >= 0f)
        {
            code = centerHi + RoundToInt(t * centerLo);
        }
        else
        {
            code = centerLo + RoundToInt(t * centerLo);
        }

        return Max(0, Min(code, DefaultBufferMaxCode));
    }

    private static float DecodeBuffer(int code)
    {
        code = Max(0, Min(code, DefaultBufferMaxCode));
        var centerHi = (DefaultBufferMaxCode + 1) / 2;
        var centerLo = centerHi - 1;
        float t;
        if (centerLo <= 0 || code == centerLo || code == centerHi)
        {
            t = 0f;
        }
        else if (code < centerLo)
        {
            t = (code - centerLo) / (float)centerLo;
        }
        else
        {
            t = (code - centerHi) / (float)centerLo;
        }

        t = ApplySignedPow(t, DefaultBufferGamma);
        var value = t * Max(MathF.Abs(DefaultBufferMin), MathF.Abs(DefaultBufferMax));
        return Clamp(value, DefaultBufferMin, DefaultBufferMax);
    }

    private static float ApplySignedPow(float value, float exponent)
    {
        if (value == 0f)
        {
            return 0f;
        }

        var absValue = MathF.Abs(value);
        float magnitude;
        if (MathF.Abs(exponent - 0.5f) < 1e-6f)
        {
            magnitude = MathF.Sqrt(absValue);
        }
        else if (MathF.Abs(exponent - 2f) < 1e-6f)
        {
            magnitude = absValue * absValue;
        }
        else
        {
            magnitude = absValue;
        }

        return value < 0f ? -magnitude : magnitude;
    }

    private static int RoundToInt(float value)
        => value >= 0f
            ? (int)(value + 0.5f)
            : (int)(value - 0.5f);

    private static ulong MixToU64(ulong brainSeed, ulong tickId, uint fromAddress32, uint toAddress32)
    {
        var mixed = brainSeed;
        mixed = SplitMixStep(mixed ^ tickId);
        mixed = SplitMixStep(mixed ^ fromAddress32);
        mixed = SplitMixStep(mixed ^ toAddress32);
        return SplitMixStep(mixed);
    }

    private static ulong SplitMixStep(ulong value)
    {
        value ^= value >> 30;
        value *= 0xbf58476d1ce4e5b9UL;
        value ^= value >> 27;
        value *= 0x94d049bb133111ebUL;
        value ^= value >> 31;
        return value;
    }

    private static float UnitIntervalFromSeed(ulong seed)
    {
        const double scale = 1d / (1UL << 53);
        var bits = seed >> 11;
        return (float)(bits * scale);
    }

    private static float NormalizePreActivationThreshold(float threshold)
    {
        if (!float.IsFinite(threshold))
        {
            return 0f;
        }

        return Clamp(threshold, -RuntimeSignalLimit, RuntimeSignalLimit);
    }

    private static float NormalizeActivationThreshold(float threshold)
    {
        if (!float.IsFinite(threshold))
        {
            return 0f;
        }

        return Clamp(threshold, 0f, RuntimeSignalLimit);
    }

    private static float ClampSignal(float value)
    {
        if (!float.IsFinite(value))
        {
            return 0f;
        }

        return Clamp(value, -RuntimeSignalLimit, RuntimeSignalLimit);
    }

    private static float ClampFinite(float value, float min, float max, float fallback)
    {
        if (!float.IsFinite(value))
        {
            return fallback;
        }

        return Clamp(value, min, max);
    }

    private static float Clamp(float value, float min, float max)
        => value < min ? min : (value > max ? max : value);

    private static float Max(float left, float right)
        => left > right ? left : right;

    private static float Min(float left, float right)
        => left < right ? left : right;

    private static int Max(int left, int right)
        => left > right ? left : right;

    private static int Min(int left, int right)
        => left < right ? left : right;

    private sealed class NeuronIndexGroup : IDisposable
    {
        public NeuronIndexGroup(byte functionId, int[] indices, Accelerator accelerator)
        {
            FunctionId = functionId;
            Count = indices.Length;
            Indices = accelerator.Allocate1D<int>(indices.Length);
            Indices.CopyFromCPU(indices);
        }

        public byte FunctionId { get; }

        public int Count { get; }

        public MemoryBuffer1D<int, Stride1D.Dense> Indices { get; }

        public void Dispose()
        {
            Indices.Dispose();
        }
    }
}
