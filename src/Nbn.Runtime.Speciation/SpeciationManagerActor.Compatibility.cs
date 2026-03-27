using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Shared;
using Proto;
using ProtoRepro = Nbn.Proto.Repro;
using ProtoSpec = Nbn.Proto.Speciation;
using System.Diagnostics;

namespace Nbn.Runtime.Speciation;

public sealed partial class SpeciationManagerActor
{
    private async Task<BootstrapAssignedSpeciesAdmissionRequirement?> TryResolveBootstrapAssignedSpeciesAdmissionRequirementAsync(
        long epochId,
        AssignmentResolution assignmentResolution,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(assignmentResolution.SpeciesId)
            || string.Equals(assignmentResolution.Strategy, "explicit_species", StringComparison.Ordinal))
        {
            return null;
        }

        var targetSpeciesId = assignmentResolution.SpeciesId.Trim();
        if (targetSpeciesId.Length == 0)
        {
            return null;
        }

        var targetFloor = ResolveSpeciesSimilarityFloor(targetSpeciesId);
        if (!targetFloor.HasValue
            || targetFloor.Value.MembershipCount <= 0
            || targetFloor.Value.MembershipCount > DerivedSpeciesBootstrapMembershipLimit
            || targetFloor.Value.ActualSimilaritySampleCount >= DerivedSpeciesBootstrapActualSampleRequirement)
        {
            return null;
        }

        string? bootstrapSourceSpeciesId = null;
        string? bootstrapSourceSpeciesDisplayName = null;
        var targetHint = await TryResolveDerivedSpeciesHintByTargetSpeciesAsync(
            epochId,
            targetSpeciesId,
            cancellationToken).ConfigureAwait(false);
        if (targetHint.HasValue)
        {
            bootstrapSourceSpeciesId = targetHint.Value.SourceSpeciesId;
            bootstrapSourceSpeciesDisplayName = targetHint.Value.SourceSpeciesDisplayName;
        }
        else if (!string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
            && !string.Equals(
                targetSpeciesId,
                assignmentResolution.SourceSpeciesId.Trim(),
                StringComparison.Ordinal))
        {
            bootstrapSourceSpeciesId = assignmentResolution.SourceSpeciesId.Trim();
            bootstrapSourceSpeciesDisplayName = assignmentResolution.SourceSpeciesDisplayName;
        }

        if (string.IsNullOrWhiteSpace(bootstrapSourceSpeciesId)
            || string.Equals(targetSpeciesId, bootstrapSourceSpeciesId.Trim(), StringComparison.Ordinal))
        {
            return null;
        }

        return new BootstrapAssignedSpeciesAdmissionRequirement(
            targetSpeciesId,
            ResolveSpeciesDisplayName(assignmentResolution.SpeciesDisplayName, targetSpeciesId),
            bootstrapSourceSpeciesId.Trim(),
            ResolveSpeciesDisplayName(
                bootstrapSourceSpeciesDisplayName,
                bootstrapSourceSpeciesId.Trim()),
            targetFloor.Value.MembershipCount,
            targetFloor.Value.ActualSimilaritySampleCount);
    }

    private static bool RequiresActualAssignedSpeciesAdmission(
        AssignmentResolution assignmentResolution,
        bool bootstrapRequired)
    {
        return bootstrapRequired
            || (!string.IsNullOrWhiteSpace(assignmentResolution.SpeciesId)
            && !string.IsNullOrWhiteSpace(assignmentResolution.SourceSpeciesId)
            && !string.Equals(
                assignmentResolution.SpeciesId,
                assignmentResolution.SourceSpeciesId,
                StringComparison.Ordinal)
            && (string.Equals(
                    assignmentResolution.DecisionReason,
                    "lineage_realign_recent_split",
                    StringComparison.Ordinal)
                || string.Equals(
                    assignmentResolution.DecisionReason,
                    "lineage_reuse_recent_derived_species",
                    StringComparison.Ordinal)));
    }

    private AssignmentResolution BuildBootstrapFallbackAssignmentResolution(
        BootstrapAssignedSpeciesAdmissionRequirement requirement,
        double? fallbackSourceSpeciesSimilarityScore)
    {
        var fallbackThresholdState = ResolveSplitThresholdState(
            ResolveSpeciesSimilarityFloor(requirement.SourceSpeciesId));
        var splitTriggeredBySpeciesFloor = fallbackSourceSpeciesSimilarityScore.HasValue
            && fallbackThresholdState.UsesSpeciesFloor
            && ClampScore(fallbackSourceSpeciesSimilarityScore.Value) <= fallbackThresholdState.DynamicSplitThreshold
            && ClampScore(fallbackSourceSpeciesSimilarityScore.Value) > fallbackThresholdState.PolicyEffectiveSplitThreshold;

        return new AssignmentResolution(
            requirement.SourceSpeciesId,
            requirement.SourceSpeciesDisplayName,
            DecisionReason: "lineage_bootstrap_compatibility_required",
            Strategy: "lineage_bootstrap_fallback",
            StrategyDetail:
                "Derived species bootstrap required actual compatibility evidence; retained the pre-split source species.",
            ForceDecisionReason: true,
            PolicyEffectiveSplitThreshold: fallbackThresholdState.PolicyEffectiveSplitThreshold,
            EffectiveSplitThreshold: fallbackThresholdState.DynamicSplitThreshold,
            SplitTriggeredBySpeciesFloor: splitTriggeredBySpeciesFloor,
            SourceSpeciesId: requirement.SourceSpeciesId,
            SourceSpeciesDisplayName: requirement.SourceSpeciesDisplayName,
            SourceSpeciesSimilarityScore: fallbackSourceSpeciesSimilarityScore,
            SourceConsensusShare: 0d,
            SpeciesFloorSimilarityScore: fallbackThresholdState.SpeciesFloorSimilarityScore,
            SpeciesFloorSampleCount: fallbackThresholdState.SpeciesFloorSampleCount,
            SpeciesFloorMembershipCount: fallbackThresholdState.SpeciesFloorMembershipCount);
    }

    private async Task<AssignedSpeciesAdmissionAssessment> TryAssessActualAssignedSpeciesAdmissionAsync(
        IContext context,
        SpeciationEpochInfo epoch,
        ResolvedCandidate resolvedCandidate,
        AssignmentResolution assignmentResolution,
        BootstrapAssignedSpeciesAdmissionRequirement? bootstrapRequirement,
        double? sourceSpeciesSimilarityScore,
        IReadOnlyList<SpeciationMembershipRecord>? exemplarMemberships = null)
    {
        var assessmentMode = ResolveCompatibilityAssessmentMode(resolvedCandidate.CandidateMode);
        if (!TryBuildCompatibilitySubjectOptions(
                resolvedCandidate,
                out var candidateSubject,
                out var candidateFallbackSubject))
        {
            return CreateAssignedSpeciesAdmissionAssessment(
                assessmentAttempted: false,
                admitted: false,
                similarityScore: null,
                assessmentMode: assessmentMode,
                exemplarBrainIds: Array.Empty<string>(),
                compatible: false,
                abortReason: string.Empty,
                failureReason: "candidate_subject_unavailable",
                elapsedMs: 0L);
        }

        return await TryAssessActualAssignedSpeciesAdmissionAsync(
            context,
            epoch,
            candidateSubject,
            candidateFallbackSubject,
            resolvedCandidate.BrainId,
            assignmentResolution,
            bootstrapRequirement,
            sourceSpeciesSimilarityScore,
            exemplarMemberships).ConfigureAwait(false);
    }

    private async Task<AssignedSpeciesAdmissionAssessment> TryAssessActualAssignedSpeciesAdmissionAsync(
        IContext context,
        SpeciationEpochInfo epoch,
        CompatibilitySubject candidateSubject,
        CompatibilitySubject candidateFallbackSubject,
        Guid candidateBrainId,
        AssignmentResolution assignmentResolution,
        BootstrapAssignedSpeciesAdmissionRequirement? bootstrapRequirement,
        double? sourceSpeciesSimilarityScore,
        IReadOnlyList<SpeciationMembershipRecord>? exemplarMemberships = null)
    {
        var stopwatch = Stopwatch.StartNew();
        var assessmentMode = ResolveCompatibilityAssessmentMode(candidateSubject.Kind);
        AssignedSpeciesAdmissionAssessment BuildAssessment(
            bool assessmentAttempted,
            bool admitted,
            double? similarityScore,
            IReadOnlyList<string> exemplarBrainIds,
            bool compatible,
            string abortReason,
            string failureReason)
            => CreateAssignedSpeciesAdmissionAssessment(
                assessmentAttempted,
                admitted,
                similarityScore,
                assessmentMode,
                exemplarBrainIds,
                compatible,
                abortReason,
                failureReason,
                ResolveAssignedSpeciesAdmissionElapsedMs(stopwatch, failureReason));

        if (_reproductionManagerPid is null
            || epoch.EpochId <= 0
            || string.IsNullOrWhiteSpace(assignmentResolution.SpeciesId))
        {
            var failureReason = _reproductionManagerPid is null
                ? "reproduction_unavailable"
                : epoch.EpochId <= 0
                    ? "epoch_unavailable"
                    : "assigned_species_unavailable";
            return BuildAssessment(
                assessmentAttempted: false,
                admitted: false,
                similarityScore: null,
                exemplarBrainIds: Array.Empty<string>(),
                compatible: false,
                abortReason: string.Empty,
                failureReason: failureReason);
        }

        var exemplars = exemplarMemberships;
        if (exemplars is null)
        {
            exemplars = await _store.ListEarliestMembershipsForSpeciesAsync(
                epoch.EpochId,
                assignmentResolution.SpeciesId,
                ExternalAdmissionExemplarLimit).ConfigureAwait(false);
        }

        if (exemplars.Count == 0)
        {
            return BuildAssessment(
                assessmentAttempted: false,
                admitted: false,
                similarityScore: null,
                exemplarBrainIds: Array.Empty<string>(),
                compatible: false,
                abortReason: string.Empty,
                failureReason: "assigned_species_exemplars_unavailable");
        }

        if (bootstrapRequirement.HasValue && exemplars.Count > 1)
        {
            exemplars = exemplars
                .OrderBy(record => record.AssignedMs)
                .ThenBy(record => record.BrainId)
                .Take(1)
                .ToArray();
        }

        var attemptedExemplarBrainIds = new List<string>(exemplars.Count);
        var exemplarSimilarities = new List<double>(exemplars.Count);
        var assessmentAttempted = false;
        var allAssessmentsCompatible = true;
        string? firstAssessmentAbortReason = null;
        foreach (var exemplar in exemplars)
        {
            if (candidateBrainId != Guid.Empty
                && exemplar.BrainId == candidateBrainId)
            {
                continue;
            }

            attemptedExemplarBrainIds.Add(exemplar.BrainId.ToString("D"));
            if (!TryBuildCompatibilitySubjectOptions(
                    exemplar,
                    out var exemplarSubject,
                    out var exemplarFallbackSubject)
                || !TrySelectCompatibleSubjects(
                    candidateSubject,
                    candidateFallbackSubject,
                    exemplarSubject,
                    exemplarFallbackSubject,
                    out var selectedCandidateSubject,
                    out var selectedExemplarSubject))
            {
                return BuildAssessment(
                    assessmentAttempted: assessmentAttempted,
                    admitted: false,
                    similarityScore: null,
                    exemplarBrainIds: attemptedExemplarBrainIds,
                    compatible: assessmentAttempted && allAssessmentsCompatible,
                    abortReason: firstAssessmentAbortReason ?? string.Empty,
                    failureReason: "assigned_species_exemplar_subject_unavailable");
            }

            var assessment = await TryAssessCompatibilitySimilarityAsync(
                context,
                selectedCandidateSubject,
                selectedExemplarSubject).ConfigureAwait(false);
            assessmentAttempted |= assessment.RequestAttempted;
            allAssessmentsCompatible &= assessment.Compatible;
            if (!string.IsNullOrWhiteSpace(assessment.AssessmentMode))
            {
                assessmentMode = assessment.AssessmentMode;
            }
            if (string.IsNullOrWhiteSpace(firstAssessmentAbortReason)
                && !string.IsNullOrWhiteSpace(assessment.AbortReason))
            {
                firstAssessmentAbortReason = assessment.AbortReason;
            }

            if (!assessment.SimilarityScore.HasValue)
            {
                return BuildAssessment(
                    assessmentAttempted: assessmentAttempted,
                    admitted: false,
                    similarityScore: null,
                    exemplarBrainIds: attemptedExemplarBrainIds,
                    compatible: assessmentAttempted && allAssessmentsCompatible,
                    abortReason: firstAssessmentAbortReason ?? string.Empty,
                    failureReason: string.IsNullOrWhiteSpace(assessment.FailureReason)
                        ? "compatibility_similarity_unavailable"
                        : assessment.FailureReason);
            }

            exemplarSimilarities.Add(assessment.SimilarityScore.Value);
        }

        if (exemplarSimilarities.Count == 0)
        {
            return BuildAssessment(
                assessmentAttempted: assessmentAttempted,
                admitted: false,
                similarityScore: null,
                exemplarBrainIds: attemptedExemplarBrainIds,
                compatible: assessmentAttempted && allAssessmentsCompatible,
                abortReason: firstAssessmentAbortReason ?? string.Empty,
                failureReason: "assigned_species_exemplars_unavailable");
        }

        var assignedSimilarity = ClampScore(exemplarSimilarities.Min());
        var targetThresholdState = ResolveSplitThresholdState(
            ResolveSpeciesSimilarityFloor(assignmentResolution.SpeciesId));
        var minimumAssignedSimilarity = targetThresholdState.DynamicSplitThreshold;
        var sourceComparisonMargin = bootstrapRequirement.HasValue
            ? Math.Max(0d, _assignmentPolicy.LineageSplitGuardMargin)
            : 0d;

        if (assignedSimilarity <= 0d
            || assignedSimilarity < minimumAssignedSimilarity)
        {
            return BuildAssessment(
                assessmentAttempted: assessmentAttempted,
                admitted: false,
                similarityScore: assignedSimilarity,
                exemplarBrainIds: attemptedExemplarBrainIds,
                compatible: allAssessmentsCompatible,
                abortReason: firstAssessmentAbortReason ?? string.Empty,
                failureReason: "compatibility_similarity_below_threshold");
        }

        if (sourceSpeciesSimilarityScore.HasValue
            && assignedSimilarity + sourceComparisonMargin <= ClampScore(sourceSpeciesSimilarityScore.Value))
        {
            return BuildAssessment(
                assessmentAttempted: assessmentAttempted,
                admitted: false,
                similarityScore: assignedSimilarity,
                exemplarBrainIds: attemptedExemplarBrainIds,
                compatible: allAssessmentsCompatible,
                abortReason: firstAssessmentAbortReason ?? string.Empty,
                failureReason: "compatibility_similarity_not_better_than_source");
        }

        return BuildAssessment(
            assessmentAttempted: assessmentAttempted,
            admitted: true,
            similarityScore: assignedSimilarity,
            exemplarBrainIds: attemptedExemplarBrainIds,
            compatible: allAssessmentsCompatible,
            abortReason: firstAssessmentAbortReason ?? string.Empty,
            failureReason: string.Empty);
    }

    private async Task<CompatibilitySimilarityAssessment> TryAssessCompatibilitySimilarityAsync(
        IContext context,
        CompatibilitySubject candidateSubject,
        CompatibilitySubject exemplarSubject)
    {
        if (_reproductionManagerPid is null
            || candidateSubject.Kind != exemplarSubject.Kind)
        {
            return new CompatibilitySimilarityAssessment(
                RequestAttempted: false,
                SimilarityScore: null,
                Compatible: false,
                AbortReason: string.Empty,
                FailureReason: _reproductionManagerPid is null
                    ? "reproduction_unavailable"
                    : "compatibility_subject_kind_mismatch",
                AssessmentMode: string.Empty);
        }

        try
        {
            ProtoRepro.ReproduceResult response;
            if (candidateSubject.Kind == CompatibilitySubjectKind.BrainId)
            {
                response = await context.RequestAsync<ProtoRepro.ReproduceResult>(
                    _reproductionManagerPid,
                    new ProtoRepro.AssessCompatibilityByBrainIdsRequest
                    {
                        ParentA = candidateSubject.BrainId.ToProtoUuid(),
                        ParentB = exemplarSubject.BrainId.ToProtoUuid(),
                        Config = CreateCompatibilityAssessmentConfig(),
                        RunCount = 1
                    },
                    _compatibilityRequestTimeout).ConfigureAwait(false);
            }
            else
            {
                var request = new ProtoRepro.AssessCompatibilityByArtifactsRequest
                {
                    ParentADef = candidateSubject.ArtifactDefRef!.Clone(),
                    ParentBDef = exemplarSubject.ArtifactDefRef!.Clone(),
                    Config = CreateCompatibilityAssessmentConfig(),
                    RunCount = 1
                };
                if (HasUsableArtifactReference(candidateSubject.ArtifactStateRef))
                {
                    request.ParentAState = candidateSubject.ArtifactStateRef!.Clone();
                }

                if (HasUsableArtifactReference(exemplarSubject.ArtifactStateRef))
                {
                    request.ParentBState = exemplarSubject.ArtifactStateRef!.Clone();
                }

                response = await context.RequestAsync<ProtoRepro.ReproduceResult>(
                    _reproductionManagerPid,
                    request,
                    _compatibilityRequestTimeout).ConfigureAwait(false);
            }

            if (response?.Report is null
                || float.IsNaN(response.Report.SimilarityScore)
                || float.IsInfinity(response.Report.SimilarityScore))
            {
                return new CompatibilitySimilarityAssessment(
                    RequestAttempted: true,
                    SimilarityScore: null,
                    Compatible: false,
                    AbortReason: string.Empty,
                    FailureReason: "compatibility_response_invalid",
                    AssessmentMode: ResolveCompatibilityAssessmentMode(candidateSubject.Kind));
            }

            var abortReason = NormalizeCompatibilityAbortReason(response.Report.AbortReason);
            var similarityScore = ClampScore(response.Report.SimilarityScore);
            if (!response.Report.Compatible
                && !ShouldUseCompatibilitySimilarityScore(abortReason))
            {
                return new CompatibilitySimilarityAssessment(
                    RequestAttempted: true,
                    SimilarityScore: null,
                    Compatible: false,
                    AbortReason: abortReason,
                    FailureReason: "compatibility_infrastructure_failure",
                    AssessmentMode: ResolveCompatibilityAssessmentMode(candidateSubject.Kind));
            }

            return new CompatibilitySimilarityAssessment(
                RequestAttempted: true,
                SimilarityScore: similarityScore,
                Compatible: response.Report.Compatible,
                AbortReason: abortReason,
                FailureReason: string.Empty,
                AssessmentMode: ResolveCompatibilityAssessmentMode(candidateSubject.Kind));
        }
        catch (Exception ex) when (ex is TimeoutException or TaskCanceledException or OperationCanceledException)
        {
            return new CompatibilitySimilarityAssessment(
                RequestAttempted: true,
                SimilarityScore: null,
                Compatible: false,
                AbortReason: "repro_request_timeout",
                FailureReason: "compatibility_request_timeout",
                AssessmentMode: ResolveCompatibilityAssessmentMode(candidateSubject.Kind));
        }
        catch
        {
            return new CompatibilitySimilarityAssessment(
                RequestAttempted: true,
                SimilarityScore: null,
                Compatible: false,
                AbortReason: "repro_request_failed",
                FailureReason: "compatibility_request_failed",
                AssessmentMode: ResolveCompatibilityAssessmentMode(candidateSubject.Kind));
        }
    }

    private static string ResolveCompatibilityAssessmentMode(ProtoSpec.SpeciationCandidateMode candidateMode)
        => candidateMode switch
        {
            ProtoSpec.SpeciationCandidateMode.BrainId => "brain_ids",
            ProtoSpec.SpeciationCandidateMode.ArtifactRef => "artifacts",
            _ => string.Empty
        };

    private static string ResolveCompatibilityAssessmentMode(CompatibilitySubjectKind kind)
        => kind == CompatibilitySubjectKind.BrainId
            ? "brain_ids"
            : kind == CompatibilitySubjectKind.ArtifactRef
                ? "artifacts"
                : string.Empty;

    private static AssignedSpeciesAdmissionAssessment CreateAssignedSpeciesAdmissionAssessment(
        bool assessmentAttempted,
        bool admitted,
        double? similarityScore,
        string assessmentMode,
        IReadOnlyList<string> exemplarBrainIds,
        bool compatible,
        string abortReason,
        string failureReason,
        long elapsedMs)
        => new(
            AssessmentAttempted: assessmentAttempted,
            Admitted: admitted,
            SimilarityScore: similarityScore,
            AssessmentMode: assessmentMode,
            ExemplarBrainIds: exemplarBrainIds.Count == 0 ? Array.Empty<string>() : exemplarBrainIds.ToArray(),
            Compatible: compatible,
            AbortReason: abortReason,
            FailureReason: failureReason,
            ElapsedMs: elapsedMs);

    private long ResolveAssignedSpeciesAdmissionElapsedMs(Stopwatch stopwatch, string failureReason)
    {
        ArgumentNullException.ThrowIfNull(stopwatch);

        var elapsedMs = (long)Math.Ceiling(stopwatch.Elapsed.TotalMilliseconds);
        if (elapsedMs < 0)
        {
            elapsedMs = 0;
        }

        if (!string.Equals(failureReason, "compatibility_request_timeout", StringComparison.Ordinal))
        {
            return elapsedMs;
        }

        var timeoutFloorMs = GetPositiveCeilingMilliseconds(_compatibilityRequestTimeout);
        return timeoutFloorMs > 0
            ? Math.Max(elapsedMs, timeoutFloorMs)
            : elapsedMs;
    }

    private static long GetPositiveCeilingMilliseconds(TimeSpan value)
    {
        if (value <= TimeSpan.Zero)
        {
            return 0;
        }

        var totalMilliseconds = value.TotalMilliseconds;
        return double.IsNaN(totalMilliseconds) || double.IsInfinity(totalMilliseconds) || totalMilliseconds <= 0d
            ? 0
            : (long)Math.Ceiling(totalMilliseconds);
    }

    private static string NormalizeCompatibilityAbortReason(string? abortReason)
        => string.IsNullOrWhiteSpace(abortReason)
            ? string.Empty
            : abortReason.Trim();

    private static bool ShouldUseCompatibilitySimilarityScore(string abortReason)
    {
        if (string.IsNullOrWhiteSpace(abortReason))
        {
            return true;
        }

        return !IsCompatibilityInfrastructureFailure(abortReason);
    }

    private static bool IsCompatibilityInfrastructureFailure(string abortReason)
    {
        return abortReason switch
        {
            "repro_internal_error" => true,
            "repro_parent_resolution_unavailable" => true,
            "repro_run_count_out_of_range" => true,
            "repro_missing_parent_brain_ids" => true,
            "repro_parent_a_brain_id_invalid" => true,
            "repro_parent_b_brain_id_invalid" => true,
            "repro_missing_parent_a_def" => true,
            "repro_missing_parent_b_def" => true,
            _ when abortReason.EndsWith("_lookup_failed", StringComparison.Ordinal)
                || abortReason.EndsWith("_brain_not_found", StringComparison.Ordinal)
                || abortReason.EndsWith("_base_def_missing", StringComparison.Ordinal)
                || abortReason.EndsWith("_media_type_invalid", StringComparison.Ordinal)
                || abortReason.EndsWith("_sha256_invalid", StringComparison.Ordinal)
                || abortReason.EndsWith("_artifact_not_found", StringComparison.Ordinal)
                || abortReason.EndsWith("_parse_failed", StringComparison.Ordinal)
                || abortReason.EndsWith("_validation_failed", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_ref_missing", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_media_type_invalid", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_sha256_invalid", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_artifact_not_found", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_parse_failed", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_incompatible_with_base", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_overlay_missing", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_overlay_empty", StringComparison.Ordinal)
                || abortReason.EndsWith("_state_overlay_no_matching_routes", StringComparison.Ordinal) => true,
            _ => false
        };
    }

    private ProtoRepro.ReproduceConfig CreateCompatibilityAssessmentConfig()
        => _compatibilityAssessmentConfig.Clone();
}
