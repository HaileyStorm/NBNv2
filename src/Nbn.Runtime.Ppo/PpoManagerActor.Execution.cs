using System.Text.Json;
using Nbn.Proto;
using Nbn.Proto.Ppo;
using Nbn.Shared;
using Proto;
using ProtoIo = Nbn.Proto.Io;
using ProtoRepro = Nbn.Proto.Repro;
using ProtoSpec = Nbn.Proto.Speciation;

namespace Nbn.Runtime.Ppo;

public sealed partial class PpoManagerActor
{
    private static readonly TimeSpan RolloutRequestTimeout = TimeSpan.FromSeconds(45);

    private async Task<PpoRunDescriptor> ExecuteRunAsync(
        IContext context,
        PpoRunDescriptor run,
        PpoStartRunRequest request,
        PID? ioPid,
        PID? reproductionPid,
        PID? speciationPid,
        RunExecutionState executionState,
        CancellationToken cancellationToken)
    {
        if (ioPid is null)
        {
            return CreateTerminalRun(run, PpoRunState.Failed, "ppo_io_unavailable");
        }

        if (reproductionPid is null)
        {
            return CreateTerminalRun(run, PpoRunState.Failed, "ppo_reproduction_unavailable");
        }

        if (speciationPid is null)
        {
            return CreateTerminalRun(run, PpoRunState.Failed, "ppo_speciation_unavailable");
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            var observedParents = await ObserveParentsAsync(context, ioPid, request, cancellationToken)
                .ConfigureAwait(false);

            cancellationToken.ThrowIfCancellationRequested();
            var reproductionRequest = CreateReproductionRequest(request, observedParents);
            var reproduction = await context
                .RequestAsync<ProtoRepro.ReproduceResult>(
                    reproductionPid,
                    reproductionRequest,
                    RolloutRequestTimeout)
                .ConfigureAwait(false);

            if (reproduction is null)
            {
                return CreateTerminalRun(run, PpoRunState.Failed, "ppo_reproduction_empty_response");
            }

            var candidates = ResolveCandidateArtifacts(reproduction);
            if (candidates.Count == 0)
            {
                return CreateTerminalRun(
                    run,
                    PpoRunState.Failed,
                    string.IsNullOrWhiteSpace(reproduction.Report?.AbortReason)
                        ? "ppo_reproduction_candidate_missing"
                        : reproduction.Report.AbortReason);
            }

            cancellationToken.ThrowIfCancellationRequested();
            if (!executionState.TryMarkCommitDispatched())
            {
                return CreateTerminalRun(run, PpoRunState.Cancelled, "ppo_run_cancelled");
            }

            var speciationRequest = CreateSpeciationRequest(run, request, observedParents, candidates);
            var speciation = await context
                .RequestAsync<ProtoSpec.SpeciationBatchEvaluateApplyResponse>(
                    speciationPid,
                    speciationRequest,
                    RolloutRequestTimeout)
                .ConfigureAwait(false);

            if (speciation is null)
            {
                return CreateTerminalRun(run, PpoRunState.Failed, "ppo_speciation_empty_response");
            }

            var report = CreateExecutionReport(run, request, observedParents, reproduction, speciation, candidates);
            var success = speciation.FailureReason == ProtoSpec.SpeciationFailureReason.SpeciationFailureNone
                          && speciation.ProcessedCount == candidates.Count
                          && speciation.CommittedCount == candidates.Count;
            var detail = success
                ? "completed"
                : string.IsNullOrWhiteSpace(speciation.FailureDetail)
                    ? "ppo_speciation_commit_failed"
                    : speciation.FailureDetail;

            var completed = CreateTerminalRun(run, success ? PpoRunState.Completed : PpoRunState.Failed, detail);
            completed.ExecutionReport = report;
            return completed;
        }
        catch (OperationCanceledException)
        {
            return CreateTerminalRun(run, PpoRunState.Cancelled, "ppo_run_cancelled");
        }
        catch (Exception ex)
        {
            return CreateTerminalRun(run, PpoRunState.Failed, ex.GetBaseException().Message);
        }
    }

    private static async Task<IReadOnlyList<PpoObservedParent>> ObserveParentsAsync(
        IContext context,
        PID ioPid,
        PpoStartRunRequest request,
        CancellationToken cancellationToken)
    {
        var observed = new List<PpoObservedParent>(request.ParentBrainIds.Count);
        foreach (var parentBrainId in request.ParentBrainIds)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var info = await context
                .RequestAsync<ProtoIo.BrainInfo>(
                    ioPid,
                    new ProtoIo.BrainInfoRequest { BrainId = parentBrainId.Clone() },
                    RolloutRequestTimeout)
                .ConfigureAwait(false);

            if (info is null || !HasArtifactRef(info.BaseDefinition))
            {
                throw new InvalidOperationException("ppo_parent_brain_definition_unavailable");
            }

            cancellationToken.ThrowIfCancellationRequested();
            var snapshot = await context
                .RequestAsync<ProtoIo.SnapshotReady>(
                    ioPid,
                    new ProtoIo.RequestSnapshot
                    {
                        BrainId = parentBrainId.Clone(),
                        HasRuntimeState = true
                    },
                    RolloutRequestTimeout)
                .ConfigureAwait(false);

            if (snapshot is null || !HasArtifactRef(snapshot.Snapshot))
            {
                throw new InvalidOperationException("ppo_parent_snapshot_unavailable");
            }

            if (!snapshot.GeneratedFromLiveState)
            {
                throw new InvalidOperationException("ppo_parent_live_snapshot_unavailable");
            }

            observed.Add(new PpoObservedParent
            {
                BrainId = parentBrainId.Clone(),
                BrainDef = info.BaseDefinition.Clone(),
                Snapshot = snapshot.Snapshot.Clone(),
                ObservedMs = CurrentUnixTimeMs(),
                SnapshotTickId = snapshot.SnapshotTickId,
                SnapshotSource = string.IsNullOrWhiteSpace(snapshot.SnapshotSource)
                    ? "live_tick_boundary"
                    : snapshot.SnapshotSource.Trim()
            });
        }

        return observed;
    }

    private static ProtoRepro.ReproduceByArtifactsRequest CreateReproductionRequest(
        PpoStartRunRequest request,
        IReadOnlyList<PpoObservedParent> parents)
    {
        var config = request.ReproduceConfig?.Clone() ?? new ProtoRepro.ReproduceConfig();
        config.SpawnChild = ProtoRepro.SpawnChildPolicy.SpawnChildNever;
        config.ProtectIoRegionNeuronCounts = true;

        return new ProtoRepro.ReproduceByArtifactsRequest
        {
            ParentADef = parents[0].BrainDef.Clone(),
            ParentAState = parents[0].Snapshot.Clone(),
            ParentBDef = parents[1].BrainDef.Clone(),
            ParentBState = parents[1].Snapshot.Clone(),
            StrengthSource = request.StrengthSource,
            Config = config,
            Seed = request.Hyperparameters?.Seed ?? 0,
            RunCount = (uint)Math.Max(1UL, request.Hyperparameters?.RolloutBatchCount ?? 1UL)
        };
    }

    private static IReadOnlyList<CandidateArtifact> ResolveCandidateArtifacts(ProtoRepro.ReproduceResult reproduction)
    {
        var candidates = new List<CandidateArtifact>();
        foreach (var run in reproduction.Runs)
        {
            if (!HasArtifactRef(run.ChildDef))
            {
                continue;
            }

            candidates.Add(new CandidateArtifact(
                run.RunIndex,
                run.Seed,
                run.ChildDef.Clone(),
                run.Report?.Clone(),
                run.Summary?.Clone()));
        }

        if (candidates.Count == 0 && HasArtifactRef(reproduction.ChildDef))
        {
            candidates.Add(new CandidateArtifact(
                0,
                reproduction.Runs.Count > 0 ? reproduction.Runs[0].Seed : 0,
                reproduction.ChildDef.Clone(),
                reproduction.Report?.Clone(),
                reproduction.Summary?.Clone()));
        }

        return candidates;
    }

    private static ProtoSpec.SpeciationBatchEvaluateApplyRequest CreateSpeciationRequest(
        PpoRunDescriptor run,
        PpoStartRunRequest request,
        IReadOnlyList<PpoObservedParent> parents,
        IReadOnlyList<CandidateArtifact> candidates)
    {
        var speciation = new ProtoSpec.SpeciationBatchEvaluateApplyRequest
        {
            ApplyMode = ProtoSpec.SpeciationApplyMode.Commit
        };

        foreach (var candidate in candidates)
        {
            var item = new ProtoSpec.SpeciationBatchItem
            {
                ItemId = $"{run.RunId}:{candidate.RunIndex}",
                Candidate = new ProtoSpec.SpeciationCandidateRef
                {
                    ArtifactRef = candidate.ChildDef.Clone()
                },
                DecisionReason = "ppo_rollout_candidate",
                DecisionMetadataJson = CreateCandidateMetadataJson(run, request, parents, candidate),
                DecisionTimeMs = CurrentUnixTimeMs(),
                HasDecisionTimeMs = true
            };

            foreach (var parent in parents)
            {
                item.Parents.Add(new ProtoSpec.SpeciationParentRef
                {
                    ArtifactRef = parent.BrainDef.Clone()
                });
            }

            speciation.Items.Add(item);
        }

        return speciation;
    }

    private static PpoRolloutExecutionReport CreateExecutionReport(
        PpoRunDescriptor run,
        PpoStartRunRequest request,
        IReadOnlyList<PpoObservedParent> parents,
        ProtoRepro.ReproduceResult reproduction,
        ProtoSpec.SpeciationBatchEvaluateApplyResponse speciation,
        IReadOnlyList<CandidateArtifact> candidates)
    {
        var report = new PpoRolloutExecutionReport
        {
            ReproductionResult = reproduction.Clone(),
            SpeciationResult = speciation.Clone(),
            ProvenanceJson = CreateRunProvenanceJson(run, request, parents, candidates)
        };
        report.ObservedParents.AddRange(parents.Select(parent => parent.Clone()));

        foreach (var candidate in candidates)
        {
            var itemId = $"{run.RunId}:{candidate.RunIndex}";
            var decision = speciation.Results
                .FirstOrDefault(result => string.Equals(result.ItemId, itemId, StringComparison.Ordinal))
                ?.Decision;

            report.Candidates.Add(new PpoCandidateResult
            {
                RunIndex = candidate.RunIndex,
                Seed = candidate.Seed,
                ChildDef = candidate.ChildDef.Clone(),
                ReproductionReport = candidate.Report?.Clone(),
                MutationSummary = candidate.Summary?.Clone(),
                SpeciationDecision = decision?.Clone()
            });
        }

        return report;
    }

    private static string CreateCandidateMetadataJson(
        PpoRunDescriptor run,
        PpoStartRunRequest request,
        IReadOnlyList<PpoObservedParent> parents,
        CandidateArtifact candidate)
        => JsonSerializer.Serialize(new
        {
            source = "ppo",
            ppo_run_id = run.RunId,
            objective_name = run.ObjectiveName,
            reward_signal = request.Hyperparameters?.RewardSignal ?? string.Empty,
            rollout_tick_count = request.Hyperparameters?.RolloutTickCount ?? 0,
            rollout_batch_count = request.Hyperparameters?.RolloutBatchCount ?? 0,
            candidate_run_index = candidate.RunIndex,
            candidate_seed = candidate.Seed,
            candidate_artifact = ArtifactMetadata(candidate.ChildDef),
            observation_source = "io_request_snapshot",
            post_deliver_output_fence = false,
            observed_parents = parents.Select(ParentMetadata).ToArray(),
            request_metadata_json = request.MetadataJson ?? string.Empty
        });

    private static string CreateRunProvenanceJson(
        PpoRunDescriptor run,
        PpoStartRunRequest request,
        IReadOnlyList<PpoObservedParent> parents,
        IReadOnlyList<CandidateArtifact> candidates)
        => JsonSerializer.Serialize(new
        {
            source = "ppo",
            ppo_run_id = run.RunId,
            objective_name = run.ObjectiveName,
            reward_signal = request.Hyperparameters?.RewardSignal ?? string.Empty,
            observation_source = "io_request_snapshot",
            post_deliver_output_fence = false,
            reproduction_spawn_child = "spawn_child_never",
            speciation_apply_mode = "commit",
            parent_count = parents.Count,
            candidate_count = candidates.Count,
            observed_parents = parents.Select(ParentMetadata).ToArray()
        });

    private static object ParentMetadata(PpoObservedParent parent)
        => new
        {
            brain_id = parent.BrainId.TryToGuid(out var brainId) ? brainId.ToString("D") : string.Empty,
            observed_ms = parent.ObservedMs,
            snapshot_tick_id = parent.SnapshotTickId,
            snapshot_source = parent.SnapshotSource,
            brain_def = ArtifactMetadata(parent.BrainDef),
            snapshot = ArtifactMetadata(parent.Snapshot)
        };

    private static object ArtifactMetadata(ArtifactRef? artifact)
        => new
        {
            sha256 = TryArtifactSha(artifact),
            media_type = artifact?.MediaType ?? string.Empty,
            size_bytes = artifact?.SizeBytes ?? 0,
            store_uri = artifact?.StoreUri ?? string.Empty
        };

    private static string TryArtifactSha(ArtifactRef? artifact)
        => artifact is not null && artifact.TryToSha256Hex(out var sha)
            ? sha
            : string.Empty;

    private static PpoRunDescriptor CreateTerminalRun(PpoRunDescriptor run, PpoRunState state, string detail)
    {
        var terminal = run.Clone();
        terminal.State = state;
        terminal.CompletedMs = CurrentUnixTimeMs();
        terminal.StatusDetail = string.IsNullOrWhiteSpace(detail) ? state.ToString() : detail.Trim();
        return terminal;
    }

    private static bool HasArtifactRef(ArtifactRef? artifact)
        => artifact?.Sha256?.Value is { Length: 32 };

    private sealed record CandidateArtifact(
        uint RunIndex,
        ulong Seed,
        ArtifactRef ChildDef,
        ProtoRepro.SimilarityReport? Report,
        ProtoRepro.MutationSummary? Summary);
}
