using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.Quantization;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;
using SharedShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Runtime.WorkerNode;

public sealed partial class WorkerNodeActor
{
    private async Task<HostingResult> HostRegionShardAsync(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        var regionId = checked((int)assignment.RegionId);
        var shardIndex = checked((int)assignment.ShardIndex);
        if (!SharedShardId32.TryFrom(regionId, shardIndex, out var shardId))
        {
            return HostingResult.Failed(FailedAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureInternalError,
                $"invalid shard target r{regionId}:s{shardIndex}"));
        }

        var neuronStart = checked((int)assignment.NeuronStart);
        var requestedNeuronCount = checked((int)assignment.NeuronCount);
        var neuronCount = Math.Max(1, requestedNeuronCount);
        var shouldRequireArtifacts = HasKnownIoGatewayEndpoint() || HasHiveMindHint() || !IsEphemeralRequestSender(context.Sender);
        if (!HasArtifactRef(brain.RuntimeInfo?.BaseDefinition)
            && shouldRequireArtifacts)
        {
            var detail = string.IsNullOrWhiteSpace(brain.RuntimeInfo?.LastIoError)
                ? "metadata_unavailable"
                : brain.RuntimeInfo!.LastIoError;
            return HostingResult.Failed(FailedAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureWorkerUnavailable,
                $"artifact metadata unavailable for brain {brain.BrainId}; retry when IO/Hive metadata is ready (detail={detail})",
                retryable: true,
                retryAfterMs: (ulong)Math.Max(50, RuntimeMetadataRetryDelay.TotalMilliseconds)));
        }

        var actorName = ResolveActorName(assignment);
        var routing = BuildShardRouting(brain, (shardId, neuronStart, neuronCount));
        var observabilityTargets = ObservabilityTargets.Resolve(_observabilityDefaultHost);

        var config = new RegionShardActorConfig(
            brain.BrainId,
            shardId,
            brain.SignalRouterPid,
            regionId == NbnConstants.OutputRegionId ? brain.OutputCoordinatorPid : null,
            ResolveHiveMindPid(context),
            routing,
            VizHub: observabilityTargets.VizHub,
            DebugHub: observabilityTargets.DebugHub,
            DebugEnabled: _debugStreamEnabledDefault,
            DebugMinSeverity: _debugMinSeverityDefault,
            ComputeBackendPreference: ResolveComputeBackendPreference(assignment));

        var props = await BuildRegionShardPropsAsync(brain, assignment, neuronStart, neuronCount, config).ConfigureAwait(false);
        var existingPid = brain.RegionShards.TryGetValue(shardId, out var existing) ? existing.Pid : null;
        var pid = SpawnOrResolveNamed(context, actorName, props, existingPid);

        brain.RegionShards[shardId] = new HostedShard(shardId, neuronStart, neuronCount, pid, assignment.AssignmentId);
        context.Watch(pid);
        RegisterShard(context, brain, shardId, neuronStart, neuronCount, pid);

        return HostingResult.Succeeded(assignment, pid);
    }

    private async Task<Props> BuildRegionShardPropsAsync(
        BrainHostingState brain,
        PlacementAssignment assignment,
        int neuronStart,
        int neuronCount,
        RegionShardActorConfig config)
    {
        var runtime = brain.RuntimeInfo;
        if (runtime is not null && HasArtifactRef(runtime.BaseDefinition))
        {
            var nbnRef = runtime.BaseDefinition!.Clone();
            var nbsRef = HasArtifactRef(runtime.LastSnapshot) ? runtime.LastSnapshot!.Clone() : null;
            var artifactStore = ResolveArtifactStore(nbnRef);
            if (LogRuntimeMetadataDiagnostics)
            {
                Console.WriteLine(
                    $"[WorkerNode] Region shard using artifact state. brain={brain.BrainId} assignment={assignment.AssignmentId} region={assignment.RegionId} shard={assignment.ShardIndex} base={ArtifactLabel(nbnRef)} snapshot={ArtifactLabel(nbsRef)} store={ResolveArtifactStoreRootLabel(nbnRef)}");
            }
            try
            {
                return await RegionShardArtifactLoader.CreatePropsAsync(
                    artifactStore,
                    nbnRef,
                    nbsRef,
                    checked((int)assignment.RegionId),
                    neuronStart,
                    neuronCount,
                    config,
                    brain.BrainId).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var detail = ex.GetBaseException().Message;
                runtime.LastArtifactLoadError = detail;
                throw new InvalidOperationException(
                    $"Artifact-backed shard load failed for brain {brain.BrainId} region {assignment.RegionId} shard {assignment.ShardIndex}: {detail}",
                    ex);
            }
        }

        if (LogRuntimeMetadataDiagnostics)
        {
            Console.WriteLine(
                $"[WorkerNode] Falling back to synthetic shard state. brain={brain.BrainId} region={assignment.RegionId} shard={assignment.ShardIndex} hasIoMetadata={runtime?.HasIoMetadata ?? false} baseDefPresent={HasArtifactRef(runtime?.BaseDefinition)} ioError={runtime?.LastIoError}");
        }

        var state = BuildSyntheticRegionState(brain, assignment, neuronStart, neuronCount);
        return Props.FromProducer(() => new RegionShardActor(state, config));
    }

    private RegionShardComputeBackendPreference ResolveComputeBackendPreference(PlacementAssignment assignment)
    {
        if (assignment.Target != PlacementAssignmentTarget.PlacementTargetRegionShard)
        {
            return RegionShardComputeBackendPreference.Cpu;
        }

        var regionId = checked((int)assignment.RegionId);
        if (regionId == NbnConstants.InputRegionId || regionId == NbnConstants.OutputRegionId)
        {
            return RegionShardComputeBackendPreference.Cpu;
        }

        var configuredPreference = RegionShardComputeBackendPreferenceResolver.Resolve();
        if (configuredPreference != RegionShardComputeBackendPreference.Auto)
        {
            return configuredPreference;
        }

        var neuronCount = checked((int)assignment.NeuronCount);
        var gpuThreshold = Math.Max(4096, NbnConstants.DefaultAxonStride * 2);
        if (neuronCount < gpuThreshold)
        {
            return RegionShardComputeBackendPreference.Cpu;
        }

        var capabilities = _capabilitySnapshotProvider?.Invoke() ?? new ProtoSettings.NodeCapabilities();
        if (!capabilities.HasGpu || (!capabilities.IlgpuCudaAvailable && !capabilities.IlgpuOpenclAvailable))
        {
            return RegionShardComputeBackendPreference.Cpu;
        }

        var effectiveGpuScore = WorkerCapabilityMath.EffectiveGpuScore(capabilities.GpuScore, capabilities.GpuComputeLimitPercent);
        var effectiveVramFreeBytes = WorkerCapabilityMath.EffectiveVramFreeBytes(
            capabilities.VramFreeBytes,
            capabilities.VramTotalBytes,
            capabilities.GpuVramLimitPercent);
        if (effectiveGpuScore <= 0f || effectiveVramFreeBytes == 0)
        {
            return RegionShardComputeBackendPreference.Cpu;
        }

        var effectiveCpuScore = WorkerCapabilityMath.EffectiveCpuScore(capabilities.CpuScore, capabilities.CpuLimitPercent);
        if (effectiveCpuScore <= 0f)
        {
            var effectiveCpuCores = WorkerCapabilityMath.EffectiveCpuCores(capabilities.CpuCores, capabilities.CpuLimitPercent);
            effectiveCpuScore = effectiveCpuCores > 0 ? effectiveCpuCores / 1000f : 0f;
        }

        return effectiveGpuScore > effectiveCpuScore
            ? RegionShardComputeBackendPreference.Gpu
            : RegionShardComputeBackendPreference.Cpu;
    }

    private RegionShardState BuildSyntheticRegionState(
        BrainHostingState brain,
        PlacementAssignment assignment,
        int neuronStart,
        int neuronCount)
    {
        var regionId = checked((int)assignment.RegionId);
        var regionSpans = new int[NbnConstants.RegionCount];
        regionSpans[regionId] = neuronStart + neuronCount;
        regionSpans[NbnConstants.InputRegionId] = ResolveInputWidth(brain);
        regionSpans[NbnConstants.OutputRegionId] = ResolveOutputWidth(brain);

        var buffer = new float[neuronCount];
        var enabled = Enumerable.Repeat(true, neuronCount).ToArray();
        var exists = Enumerable.Repeat(true, neuronCount).ToArray();
        var accum = Enumerable.Repeat((byte)AccumulationFunction.AccumSum, neuronCount).ToArray();
        var activation = Enumerable.Repeat((byte)ActivationFunction.ActNone, neuronCount).ToArray();
        var reset = Enumerable.Repeat((byte)ResetFunction.ResetHold, neuronCount).ToArray();
        var paramA = new float[neuronCount];
        var paramB = new float[neuronCount];
        var pre = Enumerable.Repeat(-1f, neuronCount).ToArray();
        var threshold = Enumerable.Repeat(1f, neuronCount).ToArray();
        var axonCounts = new ushort[neuronCount];
        var axonStarts = new int[neuronCount];
        var axons = new RegionShardAxons(
            Array.Empty<byte>(),
            Array.Empty<int>(),
            Array.Empty<float>(),
            Array.Empty<byte>(),
            Array.Empty<byte>(),
            Array.Empty<bool>(),
            Array.Empty<uint>(),
            Array.Empty<uint>());

        return new RegionShardState(
            regionId,
            neuronStart,
            neuronCount,
            brain.BrainSeed,
            QuantizationSchemas.DefaultNbn.Strength,
            regionSpans,
            buffer,
            enabled,
            exists,
            accum,
            activation,
            reset,
            paramA,
            paramB,
            pre,
            threshold,
            axonCounts,
            axonStarts,
            axons);
    }

    private IArtifactStore ResolveArtifactStore(ArtifactRef reference)
    {
        if (_artifactStore is not LocalArtifactStore)
        {
            return _artifactStore;
        }

        if (ArtifactStoreResolver.TryGetLocalStoreRoot(reference.StoreUri, _defaultArtifactRootPath, out var storeRoot)
            && ArePathsEquivalent(storeRoot, _defaultArtifactRootPath))
        {
            return _artifactStore;
        }

        return _artifactStoreResolver.Resolve(reference.StoreUri);
    }

    private string ResolveArtifactStoreRootLabel(ArtifactRef? reference)
    {
        if (reference is null)
        {
            return _defaultArtifactRootPath;
        }

        if (ArtifactStoreResolver.TryGetLocalStoreRoot(reference.StoreUri, _defaultArtifactRootPath, out var storeRoot))
        {
            return storeRoot;
        }

        return reference.StoreUri;
    }

    private static bool ArePathsEquivalent(string left, string right)
    {
        try
        {
            var leftFull = Path.GetFullPath(left);
            var rightFull = Path.GetFullPath(right);
            return string.Equals(
                leftFull,
                rightFull,
                OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
        }
        catch
        {
            return string.Equals(left, right, StringComparison.OrdinalIgnoreCase);
        }
    }
}
