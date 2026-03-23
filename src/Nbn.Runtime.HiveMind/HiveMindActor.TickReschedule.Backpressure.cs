using Proto;
using ProtoSeverity = Nbn.Proto.Severity;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private bool TryApplyBackpressurePause(IContext context, string reason, TimeSpan nextTickDelay)
    {
        var candidates = BuildBackpressurePauseCandidates();
        if (candidates.Count == 0)
        {
            EmitDebug(context, ProtoSeverity.SevWarn, "backpressure.pause.skipped", $"Backpressure pause skipped: no eligible brains. reason={reason}");
            return false;
        }

        if (_options.BackpressurePauseStrategy == BackpressurePauseStrategy.LowestEnergy)
        {
            var ioPid = _ioPid is null ? null : ResolveSendTargetPid(context, _ioPid);
            var resolveTask = ResolveBackpressurePauseCandidatesAsync(context.System, ioPid, candidates);
            context.ReenterAfter(
                resolveTask,
                task =>
                {
                    var resolvedCandidates = task.IsCompletedSuccessfully ? task.Result : candidates;
                    ApplyBackpressurePauseSelection(context, resolvedCandidates, reason);
                    ScheduleNextTick(context, nextTickDelay);
                    return Task.CompletedTask;
                });
            return true;
        }

        ApplyBackpressurePauseSelection(context, candidates, reason);
        return false;
    }

    private List<BackpressurePauseCandidate> BuildBackpressurePauseCandidates()
    {
        var candidates = new List<BackpressurePauseCandidate>();
        foreach (var brain in _brains.Values)
        {
            if (!CanDispatchTickToBrain(brain))
            {
                continue;
            }

            candidates.Add(new BackpressurePauseCandidate(brain.BrainId, brain.SpawnedMs, brain.PausePriority, 0L));
        }

        return candidates;
    }

    private void ApplyBackpressurePauseSelection(
        IContext context,
        IReadOnlyList<BackpressurePauseCandidate> candidates,
        string reason)
    {
        foreach (var candidate in OrderBackpressurePauseCandidates(candidates))
        {
            if (!_brains.TryGetValue(candidate.BrainId, out var brain) || !CanDispatchTickToBrain(brain))
            {
                continue;
            }

            PauseBrain(
                context,
                candidate.BrainId,
                $"{reason}; strategy={FormatBackpressurePauseStrategy(_options.BackpressurePauseStrategy)}");
            HiveMindTelemetry.RecordPause(reason);
            return;
        }

        EmitDebug(
            context,
            ProtoSeverity.SevWarn,
            "backpressure.pause.skipped",
            $"Backpressure pause skipped: no matching brain remained eligible. reason={reason}");
    }

    private IEnumerable<BackpressurePauseCandidate> OrderBackpressurePauseCandidates(IReadOnlyList<BackpressurePauseCandidate> candidates)
    {
        return _options.BackpressurePauseStrategy switch
        {
            BackpressurePauseStrategy.NewestFirst => candidates
                .OrderByDescending(static candidate => candidate.SpawnedMs)
                .ThenBy(static candidate => candidate.BrainId),
            BackpressurePauseStrategy.LowestEnergy => candidates
                .OrderBy(static candidate => candidate.EnergyRemaining)
                .ThenBy(static candidate => candidate.SpawnedMs)
                .ThenBy(static candidate => candidate.BrainId),
            BackpressurePauseStrategy.LowestPriority => candidates
                .OrderBy(static candidate => candidate.PausePriority)
                .ThenBy(static candidate => candidate.SpawnedMs)
                .ThenBy(static candidate => candidate.BrainId),
            BackpressurePauseStrategy.ExternalOrder => OrderBackpressurePauseCandidatesByExternalOrder(candidates),
            _ => candidates
                .OrderBy(static candidate => candidate.SpawnedMs)
                .ThenBy(static candidate => candidate.BrainId)
        };
    }

    private IEnumerable<BackpressurePauseCandidate> OrderBackpressurePauseCandidatesByExternalOrder(IReadOnlyList<BackpressurePauseCandidate> candidates)
    {
        if (_options.BackpressurePauseExternalOrder is not { Length: > 0 } externalOrder)
        {
            return Array.Empty<BackpressurePauseCandidate>();
        }

        var byId = candidates.ToDictionary(static candidate => candidate.BrainId);
        var ordered = new List<BackpressurePauseCandidate>(Math.Min(externalOrder.Length, candidates.Count));
        foreach (var brainId in externalOrder)
        {
            if (byId.TryGetValue(brainId, out var candidate))
            {
                ordered.Add(candidate);
            }
        }

        return ordered;
    }

    private static async Task<List<BackpressurePauseCandidate>> ResolveBackpressurePauseCandidatesAsync(
        ActorSystem system,
        PID? ioPid,
        IReadOnlyList<BackpressurePauseCandidate> candidates)
    {
        if (candidates.Count == 0 || ioPid is null)
        {
            return new List<BackpressurePauseCandidate>(candidates);
        }

        var resolved = await Task.WhenAll(
                candidates.Select(
                    async candidate => candidate with
                    {
                        EnergyRemaining = await TryReadBrainEnergyRemainingAsync(system, ioPid, candidate.BrainId).ConfigureAwait(false)
                    }))
            .ConfigureAwait(false);
        return resolved.ToList();
    }

    private static string FormatBackpressurePauseStrategy(BackpressurePauseStrategy strategy)
    {
        return strategy switch
        {
            BackpressurePauseStrategy.NewestFirst => "newest-first",
            BackpressurePauseStrategy.LowestEnergy => "lowest-energy",
            BackpressurePauseStrategy.LowestPriority => "lowest-priority",
            BackpressurePauseStrategy.ExternalOrder => "external-order",
            _ => "oldest-first"
        };
    }
}
