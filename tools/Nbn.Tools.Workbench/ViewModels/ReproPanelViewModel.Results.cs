using System;
using System.Threading.Tasks;
using Nbn.Proto.Repro;
using Nbn.Shared;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class ReproPanelViewModel
{
    private async Task RunAsync()
    {
        if (_connections is not null && !_connections.HasReproductionServiceReadiness())
        {
            Status = "Connect Settings, IO, and Reproduction first.";
            return;
        }

        var config = BuildConfig();
        var seed = ParseUlong(SeedText, (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        Status = "Submitting request...";

        ReproduceResult? result;
        if (ShouldUseArtifactParents())
        {
            result = await RunByArtifactsAsync(config, seed);
        }
        else
        {
            result = await RunByBrainIdsAsync(config, seed);
        }

        if (result is null)
        {
            Status = "Repro request failed.";
            return;
        }

        var report = result.Report;
        var summary = result.Summary;
        var score = ComputeSimilarityScore(report);
        var compatibleText = report is null ? "Unknown" : report.Compatible ? "True" : "False";
        var abortReason = ResolveAbortReason(report);
        var regionSpanScore = report?.RegionSpanScore ?? 0f;
        var functionScore = report?.FunctionScore ?? 0f;
        var connectivityScore = report?.ConnectivityScore ?? 0f;
        var parentARegionCount = report?.RegionsPresentA ?? 0u;
        var parentBRegionCount = report?.RegionsPresentB ?? 0u;
        var childRegionCount = report?.RegionsPresentChild ?? 0u;

        SimilaritySummary = $"Score: {score:0.000} | Compatible: {compatibleText} | Abort: {abortReason} | Region span: {regionSpanScore:0.000} | Function: {functionScore:0.000} | Connectivity: {connectivityScore:0.000} | Regions A/B/Child: {parentARegionCount}/{parentBRegionCount}/{childRegionCount}";
        MutationSummary = $"+N{summary?.NeuronsAdded ?? 0u} -N{summary?.NeuronsRemoved ?? 0u} +A{summary?.AxonsAdded ?? 0u} -A{summary?.AxonsRemoved ?? 0u} reroute={summary?.AxonsRerouted ?? 0u} func={summary?.FunctionsMutated ?? 0u} strength={summary?.StrengthCodesChanged ?? 0u}";

        var childLabel = result.ChildBrainId is not null && result.ChildBrainId.TryToGuid(out var childGuid)
            ? childGuid.ToString("D")
            : "unknown";
        if (result.Spawned)
        {
            Status = report is not null && !report.Compatible
                ? $"Spawned child {childLabel} (abort: {abortReason})"
                : $"Spawned child {childLabel}";
            return;
        }

        Status = report is not null && !report.Compatible
            ? $"Aborted: {abortReason} (not spawned)."
            : "Completed (not spawned).";
    }

    private async Task<ReproduceResult?> RunByBrainIdsAsync(ReproduceConfig config, ulong seed)
    {
        if (!TryResolveParentId(SelectedParentABrain, ParentAGuidText, out var parentA)
            || !TryResolveParentId(SelectedParentBBrain, ParentBGuidText, out var parentB))
        {
            Status = "Select active parents or provide valid parent GUIDs.";
            return null;
        }

        if (parentA == parentB)
        {
            Status = "Parent A and Parent B must be different.";
            return null;
        }

        var request = new ReproduceByBrainIdsRequest
        {
            ParentA = parentA.ToProtoUuid(),
            ParentB = parentB.ToProtoUuid(),
            StrengthSource = SelectedStrengthSource.Value,
            Config = config,
            Seed = seed
        };

        return await _client.ReproduceByBrainIdsAsync(request);
    }

    private static float ComputeSimilarityScore(SimilarityReport? report)
    {
        if (report is null)
        {
            return 0f;
        }

        if (report.SimilarityScore > 0f)
        {
            return Clamp01(report.SimilarityScore);
        }

        var hasAny = false;
        var total = 0f;
        var count = 0;

        if (report.RegionSpanScore > 0f)
        {
            total += report.RegionSpanScore;
            count++;
            hasAny = true;
        }

        if (report.FunctionScore > 0f)
        {
            total += report.FunctionScore;
            count++;
            hasAny = true;
        }

        if (report.ConnectivityScore > 0f)
        {
            total += report.ConnectivityScore;
            count++;
            hasAny = true;
        }

        if (hasAny && count > 0)
        {
            return Clamp01(total / count);
        }

        return report.Compatible ? 1f : 0f;
    }

    private static string ResolveAbortReason(SimilarityReport? report)
    {
        var abortReason = report?.AbortReason;
        return string.IsNullOrWhiteSpace(abortReason) ? "none" : abortReason.Trim();
    }

    private static bool TryResolveParentId(ReproBrainOption? selected, string rawText, out Guid brainId)
    {
        if (selected is not null)
        {
            brainId = selected.BrainId;
            return true;
        }

        if (Guid.TryParse(rawText, out brainId))
        {
            return true;
        }

        brainId = Guid.Empty;
        return false;
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
}
