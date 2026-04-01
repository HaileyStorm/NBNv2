using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Proto;

namespace Nbn.Runtime.WorkerNode;

public sealed partial class WorkerNodeActor
{
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

                runtime.InputCoordinatorMode = info.InputCoordinatorMode;
                hasMetadata = true;

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
}
