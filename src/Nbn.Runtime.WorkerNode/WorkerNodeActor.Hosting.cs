using System.Diagnostics;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.Brain;
using Nbn.Runtime.IO;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.HiveMind;
using Nbn.Shared.Quantization;
using Proto;
using ProtoControl = Nbn.Proto.Control;
using ProtoSettings = Nbn.Proto.Settings;
using SharedShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Runtime.WorkerNode;

public sealed partial class WorkerNodeActor
{
    private async Task<HostingResult> HostAssignmentAsync(
        IContext context,
        BrainHostingState brain,
        PlacementAssignment assignment)
    {
        try
        {
            return assignment.Target switch
            {
                PlacementAssignmentTarget.PlacementTargetBrainRoot
                    => HostBrainRoot(context, brain, assignment),
                PlacementAssignmentTarget.PlacementTargetSignalRouter
                    => HostSignalRouter(context, brain, assignment),
                PlacementAssignmentTarget.PlacementTargetInputCoordinator
                    => HostInputCoordinator(context, brain, assignment),
                PlacementAssignmentTarget.PlacementTargetOutputCoordinator
                    => HostOutputCoordinator(context, brain, assignment),
                PlacementAssignmentTarget.PlacementTargetRegionShard
                    => await HostRegionShardAsync(context, brain, assignment).ConfigureAwait(false),
                _ => HostingResult.Failed(FailedAck(
                    assignment.AssignmentId,
                    assignment.BrainId,
                    assignment.PlacementEpoch,
                    PlacementFailureReason.PlacementFailureInternalError,
                    $"unsupported placement target {(int)assignment.Target}"))
            };
        }
        catch (Exception ex)
        {
            var detail = string.IsNullOrWhiteSpace(ex.Message)
                ? ex.GetBaseException().Message
                : ex.Message;
            return HostingResult.Failed(FailedAck(
                assignment.AssignmentId,
                assignment.BrainId,
                assignment.PlacementEpoch,
                PlacementFailureReason.PlacementFailureInternalError,
                detail));
        }
    }

    private HostingResult HostBrainRoot(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        var actorName = ResolveActorName(assignment);
        var hiveMindPid = ResolveHiveMindPid(context);
        var pid = SpawnOrResolveNamed(
            context,
            actorName,
            Props.FromProducer(() => new BrainRootActor(brain.BrainId, hiveMindPid, autoSpawnSignalRouter: false)),
            brain.BrainRootPid);

        brain.BrainRootPid = pid;
        context.Watch(pid);

        if (brain.SignalRouterPid is not null)
        {
            context.Send(pid, new SetSignalRouter(brain.SignalRouterPid));
        }

        return HostingResult.Succeeded(assignment, pid);
    }

    private HostingResult HostSignalRouter(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        var actorName = ResolveActorName(assignment);
        var pid = SpawnOrResolveNamed(
            context,
            actorName,
            Props.FromProducer(() => new BrainSignalRouterActor(brain.BrainId)),
            brain.SignalRouterPid);

        brain.SignalRouterPid = pid;
        context.Watch(pid);

        if (brain.BrainRootPid is not null)
        {
            context.Send(brain.BrainRootPid, new SetSignalRouter(pid));
        }

        return HostingResult.Succeeded(assignment, pid);
    }

    private HostingResult HostInputCoordinator(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        var actorName = ResolveActorName(assignment);
        var inputWidth = ResolveInputWidth(brain);
        var pid = SpawnOrResolveNamed(
            context,
            actorName,
            Props.FromProducer(() => new InputCoordinatorActor(
                brain.BrainId,
                (uint)inputWidth,
                ProtoControl.InputCoordinatorMode.DirtyOnChange)),
            brain.InputCoordinatorPid);

        brain.InputCoordinatorPid = pid;
        context.Watch(pid);
        return HostingResult.Succeeded(assignment, pid);
    }

    private HostingResult HostOutputCoordinator(IContext context, BrainHostingState brain, PlacementAssignment assignment)
    {
        var actorName = ResolveActorName(assignment);
        var outputWidth = ResolveOutputWidth(brain);
        var pid = SpawnOrResolveNamed(
            context,
            actorName,
            Props.FromProducer(() => new OutputCoordinatorActor(brain.BrainId, (uint)outputWidth)),
            brain.OutputCoordinatorPid);

        brain.OutputCoordinatorPid = pid;
        context.Watch(pid);
        return HostingResult.Succeeded(assignment, pid);
    }

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

    private void RegisterShard(
        IContext context,
        BrainHostingState brain,
        SharedShardId32 shardId,
        int neuronStart,
        int neuronCount,
        PID shardPid)
    {
        var hiveMindPid = ResolveHiveMindPid(context);
        if (hiveMindPid is null)
        {
            return;
        }

        var remoteShardPid = ToObservedRemotePid(context, shardPid);
        TryRequest(context, hiveMindPid, new ProtoControl.RegisterShard
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(remoteShardPid),
            NeuronStart = (uint)Math.Max(0, neuronStart),
            NeuronCount = (uint)Math.Max(0, neuronCount)
        });
    }

    private void RegisterOutputSink(IContext context, BrainHostingState brain)
        => RegisterOutputSink(context, brain, allowClear: false);

    private void UpdateInputCoordinatorWidth(IContext context, BrainHostingState brain)
    {
        if (brain.InputCoordinatorPid is null)
        {
            return;
        }

        var inputWidth = ResolveInputWidth(brain);
        context.Send(brain.InputCoordinatorPid, new UpdateInputWidth((uint)Math.Max(1, inputWidth)));
    }

    private void UpdateOutputCoordinatorWidth(IContext context, BrainHostingState brain)
    {
        if (brain.OutputCoordinatorPid is null)
        {
            return;
        }

        var outputWidth = ResolveOutputWidth(brain);
        context.Send(brain.OutputCoordinatorPid, new UpdateOutputWidth((uint)Math.Max(1, outputWidth)));
    }

    private void RegisterOutputSink(IContext context, BrainHostingState brain, bool allowClear)
    {
        if (brain.OutputCoordinatorPid is null && !allowClear)
        {
            return;
        }

        var hiveMindPid = ResolveHiveMindPid(context);
        if (hiveMindPid is null)
        {
            return;
        }

        TryRequest(context, hiveMindPid, new ProtoControl.RegisterOutputSink
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            OutputPid = brain.OutputCoordinatorPid is null
                ? string.Empty
                : PidLabel(ToRemotePid(context, brain.OutputCoordinatorPid))
        });
    }

    private void UnregisterShard(
        IContext context,
        BrainHostingState brain,
        SharedShardId32 shardId)
    {
        var hiveMindPid = ResolveHiveMindPid(context);
        if (hiveMindPid is null)
        {
            return;
        }

        TryRequest(context, hiveMindPid, new ProtoControl.UnregisterShard
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex
        });
    }

    private void PushRouting(IContext context, BrainHostingState brain)
    {
        var routes = brain.RegionShards.Values
            .OrderBy(static entry => entry.ShardId.RegionId)
            .ThenBy(static entry => entry.ShardId.ShardIndex)
            .Select(entry => new ShardRoute(entry.ShardId.Value, entry.Pid))
            .ToArray();

        var snapshot = routes.Length == 0 ? RoutingTableSnapshot.Empty : new RoutingTableSnapshot(routes);
        if (brain.BrainRootPid is not null)
        {
            context.Send(brain.BrainRootPid, new SetRoutingTable(snapshot));
        }

        if (brain.SignalRouterPid is not null)
        {
            context.Send(brain.SignalRouterPid, new SetRoutingTable(snapshot));
        }

        if (brain.RegionShards.Count == 0)
        {
            return;
        }

        var shardRouting = BuildShardRouting(brain);
        foreach (var shard in brain.RegionShards.Values)
        {
            context.Send(shard.Pid, new RegionShardUpdateRouting(shardRouting));
        }
    }

    private void PushShardEndpoints(IContext context, BrainHostingState brain)
    {
        if (brain.RegionShards.Count == 0)
        {
            return;
        }

        var tickSink = ResolveHiveMindPid(context);
        foreach (var shard in brain.RegionShards.Values)
        {
            var outputSink = shard.ShardId.RegionId == NbnConstants.OutputRegionId
                ? brain.OutputCoordinatorPid
                : null;
            context.Send(
                shard.Pid,
                new RegionShardUpdateEndpoints(brain.SignalRouterPid, outputSink, tickSink));
        }
    }

    private void PushIoGatewayRegistration(IContext context, BrainHostingState brain)
    {
        if (!TryResolveEndpointPid(ServiceEndpointSettings.IoGatewayKey, out var ioPid))
        {
            return;
        }

        var target = brain.BrainRootPid ?? brain.SignalRouterPid;
        if (target is null)
        {
            return;
        }

        context.Send(target, new RegisterIoGateway
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            IoGatewayPid = PidLabel(ioPid)
        });
    }

    private async Task<bool> EnsureRuntimeInfoAsync(IContext context, BrainHostingState brain, bool requireArtifacts = false)
    {
        brain.RuntimeInfo ??= new BrainRuntimeInfo();
        if (brain.RuntimeInfo.HasIoMetadata
            && (!requireArtifacts || HasArtifactRef(brain.RuntimeInfo.BaseDefinition)))
        {
            return true;
        }

        if (LogRuntimeMetadataDiagnostics)
        {
            LogRuntimeMetadata(brain.BrainId, "begin", brain.RuntimeInfo, requireArtifacts);
        }

        var maxAttempts = requireArtifacts ? RuntimeMetadataMaxAttempts : 1;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            var complete = await TryRefreshRuntimeInfoAsync(context, brain, requireArtifacts).ConfigureAwait(false);
            if (complete)
            {
                break;
            }

            if (attempt >= maxAttempts)
            {
                break;
            }

            if (LogRuntimeMetadataDiagnostics)
            {
                Console.WriteLine(
                    $"[WorkerNode] Runtime metadata retry pending. brain={brain.BrainId} attempt={attempt}/{maxAttempts} ioError={brain.RuntimeInfo.LastIoError}");
            }

            if (RuntimeMetadataRetryDelay > TimeSpan.Zero)
            {
                await Task.Delay(RuntimeMetadataRetryDelay).ConfigureAwait(false);
            }
        }

        UpdateRuntimeWidthsFromShards(brain);

        if (LogRuntimeMetadataDiagnostics)
        {
            LogRuntimeMetadata(brain.BrainId, "end", brain.RuntimeInfo, requireArtifacts);
        }

        return brain.RuntimeInfo.HasIoMetadata
               && (!requireArtifacts || HasArtifactRef(brain.RuntimeInfo.BaseDefinition));
    }

    private async Task<bool> TryRefreshRuntimeInfoAsync(IContext context, BrainHostingState brain, bool requireArtifacts)
    {
        brain.RuntimeInfo ??= new BrainRuntimeInfo();
        var runtime = brain.RuntimeInfo;

        if (!TryResolveEndpointPid(ServiceEndpointSettings.IoGatewayKey, out var resolvedIoPid))
        {
            runtime.LastIoError = "io_gateway_unavailable";
            runtime.HasIoMetadata = false;

            if (requireArtifacts)
            {
                await TryPopulateArtifactRefsAsync(context, brain, ioPid: null).ConfigureAwait(false);
                if (HasArtifactRef(runtime.BaseDefinition))
                {
                    runtime.HasIoMetadata = true;
                    runtime.LastIoError = string.Empty;
                    return true;
                }
            }

            return false;
        }

        Exception? lastRequestException = null;
        BrainInfo? info = null;
        foreach (var candidate in BuildCandidatePids(context, resolvedIoPid))
        {
            try
            {
                info = await context.RequestAsync<BrainInfo>(
                    candidate,
                    new BrainInfoRequest
                    {
                        BrainId = brain.BrainId.ToProtoUuid()
                    },
                    BrainInfoTimeout).ConfigureAwait(false);

                if (info is not null)
                {
                    break;
                }
            }
            catch (Exception ex)
            {
                lastRequestException = ex;
            }
        }

        var hasMetadata = false;
        try
        {
            if (info is not null)
            {
                if (info.InputWidth > 0)
                {
                    runtime.InputWidth = Math.Max(runtime.InputWidth, checked((int)info.InputWidth));
                    hasMetadata = true;
                }

                if (info.OutputWidth > 0)
                {
                    runtime.OutputWidth = Math.Max(runtime.OutputWidth, checked((int)info.OutputWidth));
                    hasMetadata = true;
                }

                if (HasArtifactRef(info.BaseDefinition))
                {
                    runtime.BaseDefinition = info.BaseDefinition.Clone();
                    hasMetadata = true;
                }

                if (HasArtifactRef(info.LastSnapshot))
                {
                    runtime.LastSnapshot = info.LastSnapshot.Clone();
                    hasMetadata = true;
                }
            }

            if (requireArtifacts && !HasArtifactRef(runtime.BaseDefinition))
            {
                await TryPopulateArtifactRefsAsync(context, brain, resolvedIoPid).ConfigureAwait(false);
            }

            var hasArtifacts = HasArtifactRef(runtime.BaseDefinition);
            var complete = requireArtifacts ? hasArtifacts : hasMetadata;
            runtime.HasIoMetadata = complete;
            if (complete)
            {
                runtime.LastIoError = string.Empty;
                return true;
            }

            if (info is null && lastRequestException is not null)
            {
                runtime.LastIoError = lastRequestException.GetBaseException().Message;
            }
            else if (requireArtifacts && string.IsNullOrWhiteSpace(runtime.LastIoError))
            {
                runtime.LastIoError = "missing_artifact_metadata";
            }

            return false;
        }
        catch (Exception ex)
        {
            runtime.HasIoMetadata = false;
            runtime.LastIoError = ex.GetBaseException().Message;
            return false;
        }
    }

    private async Task TryPopulateArtifactRefsAsync(IContext context, BrainHostingState brain, PID? ioPid)
    {
        brain.RuntimeInfo ??= new BrainRuntimeInfo();
        if (HasArtifactRef(brain.RuntimeInfo.BaseDefinition))
        {
            return;
        }

        async Task<bool> TryExportBaseDefinitionAsync(PID endpointPid)
        {
            Exception? exportException = null;
            foreach (var candidate in BuildCandidatePids(context, endpointPid))
            {
                try
                {
                    var ready = await context.RequestAsync<BrainDefinitionReady>(
                        candidate,
                        new ExportBrainDefinition
                        {
                            BrainId = brain.BrainId.ToProtoUuid(),
                            RebaseOverlays = false
                        },
                        BrainDefinitionTimeout).ConfigureAwait(false);

                    if (ready is not null && HasArtifactRef(ready.BrainDef))
                    {
                        brain.RuntimeInfo.BaseDefinition = ready.BrainDef.Clone();
                        return true;
                    }
                }
                catch (Exception ex)
                {
                    exportException = ex;
                }
            }

            if (exportException is not null)
            {
                brain.RuntimeInfo.LastIoError = exportException.GetBaseException().Message;
            }

            return false;
        }

        var exportedFromIo = false;
        if (ioPid is not null)
        {
            exportedFromIo = await TryExportBaseDefinitionAsync(ioPid).ConfigureAwait(false);
        }

        if (!exportedFromIo)
        {
            var hiveMindPid = ResolveHiveMindPid(context);
            if (hiveMindPid is not null)
            {
                await TryExportBaseDefinitionAsync(hiveMindPid).ConfigureAwait(false);
            }
        }
    }

    private static IReadOnlyList<PID> BuildCandidatePids(IContext context, PID endpointPid)
    {
        var candidates = new List<PID>(2);
        var systemAddress = context.System.Address;
        if (string.IsNullOrWhiteSpace(endpointPid.Address) && !string.IsNullOrWhiteSpace(systemAddress))
        {
            candidates.Add(new PID(systemAddress, endpointPid.Id));
        }

        candidates.Add(endpointPid);

        if (candidates.Count <= 1)
        {
            return candidates;
        }

        var deduped = new List<PID>(candidates.Count);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var candidate in candidates)
        {
            var key = $"{candidate.Address}\u001f{candidate.Id}";
            if (seen.Add(key))
            {
                deduped.Add(candidate);
            }
        }

        return deduped;
    }

    private static void LogRuntimeMetadata(Guid brainId, string stage, BrainRuntimeInfo info, bool requireArtifacts)
        => Console.WriteLine(
            $"[WorkerNode] Runtime metadata {stage}. brain={brainId} requireArtifacts={requireArtifacts} ioMetadata={info.HasIoMetadata} input={info.InputWidth} output={info.OutputWidth} base={ArtifactLabel(info.BaseDefinition)} snapshot={ArtifactLabel(info.LastSnapshot)} ioError={info.LastIoError}");

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

    private void UpdateRuntimeWidthsFromShards(BrainHostingState brain)
    {
        brain.RuntimeInfo ??= new BrainRuntimeInfo();
        foreach (var shard in brain.RegionShards.Values)
        {
            var span = shard.NeuronStart + shard.NeuronCount;
            if (shard.ShardId.RegionId == NbnConstants.InputRegionId)
            {
                brain.RuntimeInfo.InputWidth = Math.Max(brain.RuntimeInfo.InputWidth, span);
            }

            if (shard.ShardId.RegionId == NbnConstants.OutputRegionId)
            {
                brain.RuntimeInfo.OutputWidth = Math.Max(brain.RuntimeInfo.OutputWidth, span);
            }
        }
    }

    private int ResolveInputWidth(BrainHostingState brain)
    {
        brain.RuntimeInfo ??= new BrainRuntimeInfo();
        UpdateRuntimeWidthsFromShards(brain);
        return Math.Max(1, brain.RuntimeInfo.InputWidth);
    }

    private int ResolveOutputWidth(BrainHostingState brain)
    {
        brain.RuntimeInfo ??= new BrainRuntimeInfo();
        UpdateRuntimeWidthsFromShards(brain);
        return Math.Max(1, brain.RuntimeInfo.OutputWidth);
    }

    private RegionShardRoutingTable BuildShardRouting(
        BrainHostingState brain,
        (SharedShardId32 ShardId, int NeuronStart, int NeuronCount)? includeShard = null)
    {
        var map = new Dictionary<int, List<ShardSpan>>();
        foreach (var shard in brain.RegionShards.Values)
        {
            if (!map.TryGetValue(shard.ShardId.RegionId, out var spans))
            {
                spans = new List<ShardSpan>();
                map[shard.ShardId.RegionId] = spans;
            }

            spans.Add(new ShardSpan(shard.NeuronStart, Math.Max(1, shard.NeuronCount), shard.ShardId));
        }

        if (includeShard.HasValue)
        {
            var include = includeShard.Value;
            if (!map.TryGetValue(include.ShardId.RegionId, out var spans))
            {
                spans = new List<ShardSpan>();
                map[include.ShardId.RegionId] = spans;
            }

            spans.RemoveAll(span => span.ShardId.Equals(include.ShardId));
            spans.Add(new ShardSpan(include.NeuronStart, Math.Max(1, include.NeuronCount), include.ShardId));
        }

        var compact = new Dictionary<int, ShardSpan[]>();
        foreach (var entry in map)
        {
            var spans = entry.Value
                .OrderBy(static span => span.Start)
                .ToArray();
            compact[entry.Key] = spans;
        }

        return new RegionShardRoutingTable(compact);
    }

    private PID SpawnOrResolveNamed(IContext context, string actorName, Props props, PID? existingPid)
    {
        if (existingPid is not null && string.Equals(existingPid.Id, actorName, StringComparison.Ordinal))
        {
            return existingPid;
        }

        try
        {
            return context.SpawnNamed(props, actorName);
        }
        catch
        {
            return new PID(string.Empty, actorName);
        }
    }

    private static string ResolveActorName(PlacementAssignment assignment)
    {
        if (!string.IsNullOrWhiteSpace(assignment.ActorName))
        {
            var trimmed = assignment.ActorName.Trim();
            var slash = trimmed.LastIndexOf('/');
            if (slash >= 0 && slash < trimmed.Length - 1)
            {
                trimmed = trimmed[(slash + 1)..];
            }

            if (!string.IsNullOrWhiteSpace(trimmed))
            {
                return trimmed;
            }
        }

        return assignment.Target switch
        {
            PlacementAssignmentTarget.PlacementTargetBrainRoot => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-root",
            PlacementAssignmentTarget.PlacementTargetSignalRouter => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-router",
            PlacementAssignmentTarget.PlacementTargetInputCoordinator => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-input",
            PlacementAssignmentTarget.PlacementTargetOutputCoordinator => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-output",
            PlacementAssignmentTarget.PlacementTargetRegionShard => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-r{assignment.RegionId}-s{assignment.ShardIndex}",
            _ => $"brain-{assignment.BrainId?.ToGuidOrEmpty():N}-assignment-{Math.Abs(assignment.AssignmentId.GetHashCode(StringComparison.Ordinal))}"
        };
    }

    private BrainHostingState GetOrCreateBrainState(Guid brainId)
    {
        if (_brains.TryGetValue(brainId, out var brain))
        {
            return brain;
        }

        brain = new BrainHostingState(brainId);
        _brains[brainId] = brain;
        return brain;
    }

    private void ResetBrainState(IContext context, BrainHostingState brain)
    {
        var toStop = new Dictionary<string, PID>(StringComparer.Ordinal);
        AddPid(toStop, brain.BrainRootPid);
        AddPid(toStop, brain.SignalRouterPid);
        AddPid(toStop, brain.InputCoordinatorPid);
        AddPid(toStop, brain.OutputCoordinatorPid);

        foreach (var shard in brain.RegionShards.Values)
        {
            AddPid(toStop, shard.Pid);
        }

        foreach (var pid in toStop.Values)
        {
            context.Stop(pid);
        }

        foreach (var assignmentId in brain.Assignments.Keys.ToArray())
        {
            _assignments.Remove(assignmentId);
        }

        brain.Assignments.Clear();
        brain.RegionShards.Clear();
        brain.BrainRootPid = null;
        brain.SignalRouterPid = null;
        brain.InputCoordinatorPid = null;
        brain.OutputCoordinatorPid = null;
        brain.RuntimeInfo = null;
    }

    private static void AddPid(Dictionary<string, PID> toStop, PID? pid)
    {
        if (pid is null)
        {
            return;
        }

        toStop[PidLabel(pid)] = pid;
    }
}
