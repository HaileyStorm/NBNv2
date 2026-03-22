using Nbn.Proto;
using Nbn.Proto.Repro;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;
using ProtoIo = Nbn.Proto.Io;

namespace Nbn.Runtime.Reproduction;

public sealed partial class ReproductionManagerActor
{
    private Task<ReproduceResult> HandleAssessCompatibilityByBrainIdsAsync(
        IContext context,
        AssessCompatibilityByBrainIdsRequest request)
        => HandleReproduceByBrainIdsAsync(
            context,
            new ReproduceByBrainIdsRequest
            {
                ParentA = request.ParentA,
                ParentB = request.ParentB,
                StrengthSource = request.StrengthSource,
                Config = request.Config,
                Seed = request.Seed,
                ManualIoNeuronAdds = { request.ManualIoNeuronAdds },
                ManualIoNeuronRemoves = { request.ManualIoNeuronRemoves },
                RunCount = request.RunCount
            },
            assessmentOnly: true);

    private Task<ReproduceResult> HandleAssessCompatibilityByArtifactsAsync(
        IContext context,
        AssessCompatibilityByArtifactsRequest request)
        => HandleReproduceByArtifactsAsync(
            context,
            new ReproduceByArtifactsRequest
            {
                ParentADef = request.ParentADef,
                ParentAState = request.ParentAState,
                ParentBDef = request.ParentBDef,
                ParentBState = request.ParentBState,
                StrengthSource = request.StrengthSource,
                Config = request.Config,
                Seed = request.Seed,
                ManualIoNeuronAdds = { request.ManualIoNeuronAdds },
                ManualIoNeuronRemoves = { request.ManualIoNeuronRemoves },
                RunCount = request.RunCount
            },
            assessmentOnly: true);

    private async Task<ReproduceResult> HandleReproduceByBrainIdsAsync(
        IContext context,
        ReproduceByBrainIdsRequest request,
        bool assessmentOnly = false)
    {
        if (!TryResolveRunCount(request.RunCount, out _))
        {
            return CreateRunCountOutOfRangeResult(request.RunCount);
        }

        if (request.ParentA is null || request.ParentB is null)
        {
            return CreateAbortResult("repro_missing_parent_brain_ids");
        }

        if (!request.ParentA.TryToGuid(out var parentAId) || parentAId == Guid.Empty)
        {
            return CreateAbortResult("repro_parent_a_brain_id_invalid");
        }

        if (!request.ParentB.TryToGuid(out var parentBId) || parentBId == Guid.Empty)
        {
            return CreateAbortResult("repro_parent_b_brain_id_invalid");
        }

        if (_ioGatewayPid is null)
        {
            return CreateAbortResult("repro_parent_resolution_unavailable");
        }

        var parentAResolution = await ResolveParentArtifactAsync(context, request.ParentA, "a").ConfigureAwait(false);
        if (parentAResolution.AbortReason is not null)
        {
            return CreateAbortResult(parentAResolution.AbortReason);
        }

        var parentBResolution = await ResolveParentArtifactAsync(context, request.ParentB, "b").ConfigureAwait(false);
        if (parentBResolution.AbortReason is not null)
        {
            return CreateAbortResult(parentBResolution.AbortReason);
        }

        return await HandleReproduceByArtifactsAsync(
                context,
                new ReproduceByArtifactsRequest
                {
                    ParentADef = parentAResolution.ParentDef!,
                    ParentBDef = parentBResolution.ParentDef!,
                    ParentAState = parentAResolution.ParentState,
                    ParentBState = parentBResolution.ParentState,
                    StrengthSource = request.StrengthSource,
                    Config = request.Config,
                    Seed = request.Seed,
                    ManualIoNeuronAdds = { request.ManualIoNeuronAdds },
                    ManualIoNeuronRemoves = { request.ManualIoNeuronRemoves },
                    RunCount = request.RunCount
                },
                assessmentOnly)
            .ConfigureAwait(false);
    }

    private async Task<ReproduceResult> HandleReproduceByArtifactsAsync(
        IContext context,
        ReproduceByArtifactsRequest request,
        bool assessmentOnly = false)
    {
        try
        {
            if (!TryResolveRunCount(request.RunCount, out var runCount))
            {
                return CreateRunCountOutOfRangeResult(request.RunCount);
            }

            if (request.ParentADef is null)
            {
                return CreateAbortResult("repro_missing_parent_a_def");
            }

            if (request.ParentBDef is null)
            {
                return CreateAbortResult("repro_missing_parent_b_def");
            }

            var parentA = await TryLoadParentAsync(request.ParentADef, "a").ConfigureAwait(false);
            if (parentA.AbortReason is not null)
            {
                return CreateAbortResult(parentA.AbortReason);
            }

            var parentB = await TryLoadParentAsync(request.ParentBDef, "b").ConfigureAwait(false);
            if (parentB.AbortReason is not null)
            {
                return CreateAbortResult(parentB.AbortReason);
            }

            var transformParentA = await ResolveTransformParentAsync(
                    parentA.Parsed!,
                    request.ParentADef,
                    request.ParentAState,
                    request.StrengthSource,
                    "a")
                .ConfigureAwait(false);

            var transformParentB = await ResolveTransformParentAsync(
                    parentB.Parsed!,
                    request.ParentBDef,
                    request.ParentBState,
                    request.StrengthSource,
                    "b")
                .ConfigureAwait(false);

            return await ExecuteRunsAsync(
                    context,
                    runCount,
                    assessmentOnly,
                    parentA.Parsed!,
                    parentB.Parsed!,
                    transformParentA.Parsed,
                    transformParentB.Parsed,
                    request.ParentADef,
                    request.ParentBDef,
                    request.Config,
                    request.Seed,
                    request.ManualIoNeuronAdds,
                    request.ManualIoNeuronRemoves)
                .ConfigureAwait(false);
        }
        catch
        {
            return CreateAbortResult("repro_internal_error");
        }
    }

    private async Task<ReproduceResult> ExecuteRunsAsync(
        IContext context,
        uint runCount,
        bool assessmentOnly,
        ParsedParent gateParentA,
        ParsedParent gateParentB,
        ParsedParent transformParentA,
        ParsedParent transformParentB,
        ArtifactRef parentARef,
        ArtifactRef parentBRef,
        ReproduceConfig? config,
        ulong seed,
        IReadOnlyList<ManualIoNeuronEdit> manualIoNeuronAdds,
        IReadOnlyList<ManualIoNeuronEdit> manualIoNeuronRemoves)
    {
        ReproduceResult? firstRun = null;
        var outcomes = new List<ReproduceRunOutcome>((int)runCount);

        for (uint runIndex = 0; runIndex < runCount; runIndex++)
        {
            var runSeed = DeriveRunSeed(seed, runIndex);
            var runResult = await EvaluateSimilarityGatesAndBuildChildAsync(
                    context,
                    gateParentA,
                    gateParentB,
                    transformParentA,
                    transformParentB,
                    parentARef,
                    parentBRef,
                    config,
                    runSeed,
                    manualIoNeuronAdds,
                    manualIoNeuronRemoves,
                    assessmentOnly)
                .ConfigureAwait(false);

            outcomes.Add(CreateRunOutcome(runIndex, runSeed, runResult));
            firstRun ??= runResult;
        }

        var response = firstRun ?? CreateAbortResult("repro_internal_error");
        response.RequestedRunCount = runCount;
        response.Runs.Clear();
        foreach (var outcome in outcomes)
        {
            response.Runs.Add(outcome);
        }

        return response;
    }

    private async Task<ReproduceResult> ApplySpawnPolicyAsync(IContext context, ReproduceResult result, ReproduceConfig? config)
    {
        var policy = config?.SpawnChild ?? SpawnChildPolicy.SpawnChildDefaultOn;
        if (policy == SpawnChildPolicy.SpawnChildNever)
        {
            return result;
        }

        if (_ioGatewayPid is null)
        {
            return CreateSpawnFailureResult(result, "repro_spawn_unavailable");
        }

        if (!HasArtifactRef(result.ChildDef))
        {
            return CreateSpawnFailureResult(result, "repro_child_artifact_missing");
        }

        try
        {
            var response = await context
                .RequestAsync<ProtoIo.SpawnBrainViaIOAck>(
                    _ioGatewayPid,
                    new ProtoIo.SpawnBrainViaIO
                    {
                        Request = new ProtoControl.SpawnBrain
                        {
                            BrainDef = result.ChildDef
                        }
                    },
                    DefaultRequestTimeout)
                .ConfigureAwait(false);

            if (response?.Ack?.BrainId is not null
                && response.Ack.BrainId.TryToGuid(out var childBrainId)
                && childBrainId != Guid.Empty)
            {
                result.Spawned = true;
                result.ChildBrainId = response.Ack.BrainId;
                return result;
            }

            return CreateSpawnFailureResult(result, "repro_spawn_failed");
        }
        catch
        {
            return CreateSpawnFailureResult(result, "repro_spawn_request_failed");
        }
    }

    private static ReproduceResult CreateSpawnFailureResult(ReproduceResult result, string reason)
    {
        result.Report ??= new SimilarityReport();
        result.Report.Compatible = false;
        result.Report.AbortReason = reason;
        result.Spawned = false;
        result.ChildBrainId = null;
        return result;
    }

    private static bool TryResolveRunCount(uint requestedRunCount, out uint normalizedRunCount)
    {
        normalizedRunCount = requestedRunCount == 0 ? DefaultRunCount : requestedRunCount;
        return normalizedRunCount >= DefaultRunCount && normalizedRunCount <= MaxRunCount;
    }

    private static ReproduceResult CreateRunCountOutOfRangeResult(uint requestedRunCount)
    {
        var result = CreateAbortResult("repro_run_count_out_of_range");
        result.RequestedRunCount = requestedRunCount;
        return result;
    }

    private static ulong DeriveRunSeed(ulong requestSeed, uint runIndex)
    {
        if (runIndex == 0)
        {
            return requestSeed;
        }

        var state = requestSeed + (0x9E3779B97F4A7C15UL * runIndex);
        return NextRandom(ref state);
    }

    private static ReproduceRunOutcome CreateRunOutcome(uint runIndex, ulong seed, ReproduceResult source)
    {
        var outcome = new ReproduceRunOutcome
        {
            RunIndex = runIndex,
            Seed = seed,
            Spawned = source.Spawned
        };

        if (source.Report is not null)
        {
            outcome.Report = source.Report.Clone();
        }

        if (source.Summary is not null)
        {
            outcome.Summary = source.Summary.Clone();
        }

        if (source.ChildDef is not null)
        {
            outcome.ChildDef = source.ChildDef.Clone();
        }

        if (source.ChildBrainId is not null)
        {
            outcome.ChildBrainId = source.ChildBrainId.Clone();
        }

        return outcome;
    }

    private static bool HasArtifactRef(ArtifactRef? reference)
        => reference is not null
           && reference.Sha256 is not null
           && reference.Sha256.Value is not null
           && reference.Sha256.Value.Length == Sha256Hash.Length;

    private static ReproduceResult CreateAssessmentResult(
        float regionSpanScore,
        float functionScore,
        float connectivityScore,
        float similarityScore,
        int regionsPresentA,
        int regionsPresentB)
        => new()
        {
            Report = new SimilarityReport
            {
                Compatible = true,
                AbortReason = string.Empty,
                RegionSpanScore = Math.Clamp(regionSpanScore, 0f, 1f),
                FunctionScore = Math.Clamp(functionScore, 0f, 1f),
                ConnectivityScore = Math.Clamp(connectivityScore, 0f, 1f),
                SimilarityScore = Math.Clamp(similarityScore, 0f, 1f),
                RegionsPresentA = (uint)Math.Max(regionsPresentA, 0),
                RegionsPresentB = (uint)Math.Max(regionsPresentB, 0),
                RegionsPresentChild = 0
            },
            Summary = new MutationSummary(),
            Spawned = false
        };

    private static ReproduceResult CreateSuccessResult(
        ArtifactRef childDef,
        MutationSummary summary,
        float regionSpanScore,
        float functionScore,
        float connectivityScore,
        float similarityScore,
        float lineageSimilarityScore,
        float lineageParentASimilarityScore,
        float lineageParentBSimilarityScore,
        int regionsPresentA,
        int regionsPresentB,
        int regionsPresentChild)
        => new()
        {
            Report = new SimilarityReport
            {
                Compatible = true,
                AbortReason = string.Empty,
                RegionSpanScore = Math.Clamp(regionSpanScore, 0f, 1f),
                FunctionScore = Math.Clamp(functionScore, 0f, 1f),
                ConnectivityScore = Math.Clamp(connectivityScore, 0f, 1f),
                SimilarityScore = Math.Clamp(similarityScore, 0f, 1f),
                LineageSimilarityScore = Math.Clamp(lineageSimilarityScore, 0f, 1f),
                LineageParentASimilarityScore = Math.Clamp(lineageParentASimilarityScore, 0f, 1f),
                LineageParentBSimilarityScore = Math.Clamp(lineageParentBSimilarityScore, 0f, 1f),
                RegionsPresentA = (uint)Math.Max(regionsPresentA, 0),
                RegionsPresentB = (uint)Math.Max(regionsPresentB, 0),
                RegionsPresentChild = (uint)Math.Max(regionsPresentChild, 0)
            },
            Summary = summary,
            ChildDef = childDef,
            Spawned = false
        };

    private static ReproduceResult CreateAbortResult(
        string reason,
        float regionSpanScore = 0f,
        float functionScore = 0f,
        float connectivityScore = 0f,
        float similarityScore = 0f,
        int regionsPresentA = 0,
        int regionsPresentB = 0,
        int regionsPresentChild = 0)
        => new()
        {
            Report = new SimilarityReport
            {
                Compatible = false,
                AbortReason = reason,
                RegionSpanScore = Math.Clamp(regionSpanScore, 0f, 1f),
                FunctionScore = Math.Clamp(functionScore, 0f, 1f),
                ConnectivityScore = Math.Clamp(connectivityScore, 0f, 1f),
                SimilarityScore = Math.Clamp(similarityScore, 0f, 1f),
                RegionsPresentA = (uint)Math.Max(regionsPresentA, 0),
                RegionsPresentB = (uint)Math.Max(regionsPresentB, 0),
                RegionsPresentChild = (uint)Math.Max(regionsPresentChild, 0)
            },
            Summary = new MutationSummary(),
            Spawned = false
        };
}
