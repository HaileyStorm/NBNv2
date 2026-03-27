using Nbn.Proto;
using Nbn.Shared;
using ProtoSpec = Nbn.Proto.Speciation;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Nbn.Runtime.Speciation;

public sealed partial class SpeciationManagerActor
{
    private static bool TryBuildCompatibilitySubjectOptions(
        ResolvedCandidate resolvedCandidate,
        out CompatibilitySubject preferredSubject,
        out CompatibilitySubject fallbackSubject)
    {
        preferredSubject = default;
        fallbackSubject = default;
        if (!TryBuildCompatibilitySubject(
                resolvedCandidate,
                preferArtifacts: true,
                out preferredSubject))
        {
            return false;
        }

        if (!TryBuildCompatibilitySubject(
                resolvedCandidate,
                preferArtifacts: false,
                out fallbackSubject))
        {
            fallbackSubject = preferredSubject;
        }

        return true;
    }

    private static bool TryBuildCompatibilitySubject(
        ResolvedCandidate resolvedCandidate,
        bool preferArtifacts,
        out CompatibilitySubject subject)
    {
        subject = default;
        switch (resolvedCandidate.CandidateMode)
        {
            case ProtoSpec.SpeciationCandidateMode.BrainId when resolvedCandidate.BrainId != Guid.Empty:
                if (preferArtifacts
                    && CanAssessArtifactReference(resolvedCandidate.CandidateBrainBaseArtifactRef))
                {
                    subject = new CompatibilitySubject(
                        CompatibilitySubjectKind.ArtifactRef,
                        Guid.Empty,
                        resolvedCandidate.CandidateBrainBaseArtifactRef!.Clone(),
                        HasUsableArtifactReference(resolvedCandidate.CandidateBrainSnapshotArtifactRef)
                            ? resolvedCandidate.CandidateBrainSnapshotArtifactRef!.Clone()
                            : null);
                    return true;
                }

                subject = new CompatibilitySubject(
                    CompatibilitySubjectKind.BrainId,
                    resolvedCandidate.BrainId,
                    ArtifactDefRef: null,
                    ArtifactStateRef: null);
                return true;
            case ProtoSpec.SpeciationCandidateMode.ArtifactRef
                when CanAssessArtifactReference(resolvedCandidate.CandidateArtifactRef):
                subject = new CompatibilitySubject(
                    CompatibilitySubjectKind.ArtifactRef,
                    Guid.Empty,
                    resolvedCandidate.CandidateArtifactRef!.Clone(),
                    ArtifactStateRef: null);
                return true;
            default:
                return false;
        }
    }

    private static bool TryBuildCompatibilitySubjectOptions(
        SpeciationMembershipRecord membership,
        out CompatibilitySubject preferredSubject,
        out CompatibilitySubject fallbackSubject)
    {
        preferredSubject = default;
        fallbackSubject = default;
        if (!TryBuildCompatibilitySubject(
                membership,
                preferArtifacts: true,
                out preferredSubject))
        {
            return false;
        }

        if (!TryBuildCompatibilitySubject(
                membership,
                preferArtifacts: false,
                out fallbackSubject))
        {
            fallbackSubject = preferredSubject;
        }

        return true;
    }

    private static bool TryBuildCompatibilitySubject(
        SpeciationMembershipRecord membership,
        bool preferArtifacts,
        out CompatibilitySubject subject)
    {
        subject = default;
        if (membership.BrainId == Guid.Empty)
        {
            return false;
        }

        if (!TryExtractCandidateMode(membership.DecisionMetadataJson, out var candidateMode))
        {
            if (preferArtifacts
                && TryExtractStoredCandidateBrainArtifactRefs(
                    membership.DecisionMetadataJson,
                    out var storedBaseArtifactRef,
                    out var storedSnapshotArtifactRef))
            {
                subject = new CompatibilitySubject(
                    CompatibilitySubjectKind.ArtifactRef,
                    Guid.Empty,
                    storedBaseArtifactRef,
                    storedSnapshotArtifactRef);
                return true;
            }

            subject = new CompatibilitySubject(
                CompatibilitySubjectKind.BrainId,
                membership.BrainId,
                ArtifactDefRef: null,
                ArtifactStateRef: null);
            return true;
        }

        switch (candidateMode)
        {
            case ProtoSpec.SpeciationCandidateMode.BrainId:
                if (preferArtifacts
                    && TryExtractStoredCandidateBrainArtifactRefs(
                        membership.DecisionMetadataJson,
                        out var storedBaseArtifactRef,
                        out var storedSnapshotArtifactRef))
                {
                    subject = new CompatibilitySubject(
                        CompatibilitySubjectKind.ArtifactRef,
                        Guid.Empty,
                        storedBaseArtifactRef,
                        storedSnapshotArtifactRef);
                    return true;
                }

                subject = new CompatibilitySubject(
                    CompatibilitySubjectKind.BrainId,
                    membership.BrainId,
                    ArtifactDefRef: null,
                    ArtifactStateRef: null);
                return true;
            case ProtoSpec.SpeciationCandidateMode.ArtifactRef when TryExtractStoredCandidateArtifactRef(
                membership.DecisionMetadataJson,
                out var artifactRef)
                && CanAssessArtifactReference(artifactRef):
                subject = new CompatibilitySubject(
                    CompatibilitySubjectKind.ArtifactRef,
                    Guid.Empty,
                    artifactRef,
                    ArtifactStateRef: null);
                return true;
            default:
                return false;
        }
    }

    private static bool TrySelectCompatibleSubjects(
        CompatibilitySubject preferredCandidateSubject,
        CompatibilitySubject fallbackCandidateSubject,
        CompatibilitySubject preferredExemplarSubject,
        CompatibilitySubject fallbackExemplarSubject,
        out CompatibilitySubject selectedCandidateSubject,
        out CompatibilitySubject selectedExemplarSubject)
    {
        selectedCandidateSubject = default;
        selectedExemplarSubject = default;

        CompatibilitySubject[] candidateOptions = [preferredCandidateSubject, fallbackCandidateSubject];
        CompatibilitySubject[] exemplarOptions = [preferredExemplarSubject, fallbackExemplarSubject];
        foreach (var candidateOption in candidateOptions)
        {
            if (candidateOption.Kind == CompatibilitySubjectKind.None)
            {
                continue;
            }

            foreach (var exemplarOption in exemplarOptions)
            {
                if (candidateOption.Kind != exemplarOption.Kind
                    || exemplarOption.Kind == CompatibilitySubjectKind.None)
                {
                    continue;
                }

                selectedCandidateSubject = candidateOption;
                selectedExemplarSubject = exemplarOption;
                return true;
            }
        }

        return false;
    }

    private static bool CanAssessArtifactReference(ArtifactRef? artifactRef)
    {
        return artifactRef is not null
            && artifactRef.TryToSha256Hex(out _);
    }

    private static JsonObject BuildStoredArtifactRefNode(ArtifactRef artifactRef)
    {
        var node = new JsonObject
        {
            ["size_bytes"] = artifactRef.SizeBytes
        };
        if (artifactRef.TryToSha256Hex(out var sha))
        {
            node["sha256_hex"] = sha;
        }

        if (!string.IsNullOrWhiteSpace(artifactRef.MediaType))
        {
            node["media_type"] = artifactRef.MediaType.Trim();
        }

        if (!string.IsNullOrWhiteSpace(artifactRef.StoreUri))
        {
            node["store_uri"] = artifactRef.StoreUri.Trim();
        }

        return node;
    }

    private static bool TryExtractCandidateMode(
        string? decisionMetadataJson,
        out ProtoSpec.SpeciationCandidateMode candidateMode)
    {
        candidateMode = ProtoSpec.SpeciationCandidateMode.Unknown;
        if (string.IsNullOrWhiteSpace(decisionMetadataJson))
        {
            return false;
        }

        JsonNode? node;
        try
        {
            node = JsonNode.Parse(decisionMetadataJson);
        }
        catch (JsonException)
        {
            return false;
        }

        JsonNode? lineageNode = null;
        if (node is JsonObject root)
        {
            root.TryGetPropertyValue("lineage", out lineageNode);
        }

        var candidateModeText = FindStringValue(
            lineageNode,
            "candidate_mode",
            "candidateMode")
            ?? FindStringValue(
                node,
                "candidate_mode",
                "candidateMode");
        return !string.IsNullOrWhiteSpace(candidateModeText)
            && Enum.TryParse(candidateModeText.Trim(), ignoreCase: true, out candidateMode)
            && candidateMode != ProtoSpec.SpeciationCandidateMode.Unknown;
    }

    private static bool TryExtractStoredCandidateArtifactRef(
        string? decisionMetadataJson,
        out ArtifactRef artifactRef)
        => TryExtractStoredArtifactRef(
            decisionMetadataJson,
            "candidate_artifact_ref",
            out artifactRef);

    private static bool TryExtractStoredCandidateBrainArtifactRefs(
        string? decisionMetadataJson,
        out ArtifactRef baseArtifactRef,
        out ArtifactRef? snapshotArtifactRef)
    {
        snapshotArtifactRef = null;
        if (!TryExtractStoredArtifactRef(
                decisionMetadataJson,
                "candidate_brain_base_artifact_ref",
                out baseArtifactRef))
        {
            return false;
        }

        if (TryExtractStoredArtifactRef(
                decisionMetadataJson,
                "candidate_brain_snapshot_artifact_ref",
                out var storedSnapshotArtifactRef))
        {
            snapshotArtifactRef = storedSnapshotArtifactRef;
        }

        return true;
    }

    private static bool TryExtractStoredArtifactRef(
        string? decisionMetadataJson,
        string propertyName,
        out ArtifactRef artifactRef)
    {
        artifactRef = new ArtifactRef();
        if (string.IsNullOrWhiteSpace(decisionMetadataJson))
        {
            return false;
        }

        JsonNode? node;
        try
        {
            node = JsonNode.Parse(decisionMetadataJson);
        }
        catch (JsonException)
        {
            return false;
        }

        JsonNode? lineageNode = null;
        if (node is JsonObject root)
        {
            root.TryGetPropertyValue("lineage", out lineageNode);
        }

        if (lineageNode is not JsonObject lineage
            || !lineage.TryGetPropertyValue(propertyName, out var artifactNode)
            || artifactNode is not JsonObject artifactObject)
        {
            return false;
        }

        var sha256Hex = FindStringValue(
            artifactObject,
            "sha256_hex",
            "sha256Hex");
        var mediaType = FindStringValue(
            artifactObject,
            "media_type",
            "mediaType");
        var storeUri = FindStringValue(
            artifactObject,
            "store_uri",
            "storeUri");
        var sizeBytes = FindNumericValue(
            artifactObject,
            "size_bytes",
            "sizeBytes");

        if (!string.IsNullOrWhiteSpace(sha256Hex))
        {
            artifactRef = sha256Hex.Trim().ToArtifactRef(
                sizeBytes.HasValue ? (ulong)Math.Max(0d, sizeBytes.Value) : 0UL,
                mediaType,
                storeUri);
            return true;
        }

        if (string.IsNullOrWhiteSpace(storeUri))
        {
            return false;
        }

        artifactRef = new ArtifactRef
        {
            SizeBytes = sizeBytes.HasValue ? (ulong)Math.Max(0d, sizeBytes.Value) : 0UL,
            MediaType = mediaType ?? string.Empty,
            StoreUri = storeUri.Trim()
        };
        return HasUsableArtifactReference(artifactRef);
    }

    private static bool TryResolveParentSimilarityAtIndex(
        int parentIndex,
        SimilarityEvidence similarityEvidence,
        out double similarityScore)
    {
        similarityScore = 0d;
        return parentIndex switch
        {
            0 when similarityEvidence.ParentASimilarityScore.HasValue =>
                TryNormalizeSimilarity(similarityEvidence.ParentASimilarityScore.Value, out similarityScore),
            1 when similarityEvidence.ParentBSimilarityScore.HasValue =>
                TryNormalizeSimilarity(similarityEvidence.ParentBSimilarityScore.Value, out similarityScore),
            _ => false
        };
    }

    private static bool TryNormalizeSimilarity(double rawScore, out double similarityScore)
    {
        similarityScore = ClampScore(rawScore);
        return double.IsFinite(similarityScore);
    }

    private static double? TryExtractIntraSpeciesSimilaritySample(SpeciationMembershipRecord membership)
    {
        if (string.Equals(membership.DecisionReason, "explicit_species", StringComparison.Ordinal))
        {
            return null;
        }

        if (string.IsNullOrWhiteSpace(membership.SpeciesId)
            || string.IsNullOrWhiteSpace(membership.DecisionMetadataJson))
        {
            return null;
        }

        JsonNode? node;
        try
        {
            node = JsonNode.Parse(membership.DecisionMetadataJson);
        }
        catch (JsonException)
        {
            return null;
        }

        if (node is not JsonObject root)
        {
            return null;
        }

        root.TryGetPropertyValue("lineage", out var lineageNode);
        var explicitIntraSpeciesSample = FindNumericValue(
            lineageNode,
            "intra_species_similarity_sample",
            "intraSpeciesSimilaritySample");
        if (explicitIntraSpeciesSample.HasValue)
        {
            var explicitSampleSpeciesId = FindStringValue(
                lineageNode,
                "intra_species_similarity_species_id",
                "intraSpeciesSimilaritySpeciesId");
            if (string.IsNullOrWhiteSpace(explicitSampleSpeciesId)
                || string.Equals(
                    explicitSampleSpeciesId.Trim(),
                    membership.SpeciesId.Trim(),
                    StringComparison.Ordinal))
            {
                return ClampScore(explicitIntraSpeciesSample.Value);
            }
        }

        if (string.Equals(membership.DecisionReason, "lineage_diverged_new_species", StringComparison.Ordinal))
        {
            return 1d;
        }

        var sourceSpeciesId = FindStringValue(
            lineageNode,
            "source_species_id",
            "sourceSpeciesId")
            ?? FindStringValue(
                lineageNode,
                "dominant_species_id",
                "dominantSpeciesId");
        var sourceShare = FindNumericValue(
            lineageNode,
            "source_species_share",
            "sourceSpeciesShare")
            ?? FindNumericValue(
                lineageNode,
                "dominant_species_share",
                "dominantSpeciesShare");
        var normalizedSourceSpeciesId = string.IsNullOrWhiteSpace(sourceSpeciesId)
            ? string.Empty
            : sourceSpeciesId.Trim();
        if (string.IsNullOrWhiteSpace(sourceSpeciesId)
            || !sourceShare.HasValue
            || sourceShare.Value < 0.999999d
            || !string.Equals(
                membership.SpeciesId.Trim(),
                normalizedSourceSpeciesId,
                StringComparison.Ordinal))
        {
            return null;
        }

        var sourceSimilarityScore = FindNumericValue(
            lineageNode,
            "source_species_similarity_score",
            "sourceSpeciesSimilarityScore");
        if (sourceSimilarityScore.HasValue)
        {
            return ClampScore(sourceSimilarityScore.Value);
        }

        var similarityEvidence = ExtractSimilarityEvidence(membership.DecisionMetadataJson);
        if (similarityEvidence.DominantSpeciesSimilarityScore.HasValue)
        {
            return ClampScore(similarityEvidence.DominantSpeciesSimilarityScore.Value);
        }

        return similarityEvidence.SimilarityScore.HasValue
            ? ClampScore(similarityEvidence.SimilarityScore.Value)
            : null;
    }

    private static bool HasActualAssignedSpeciesSimilaritySample(SpeciationMembershipRecord membership)
    {
        if (membership is null
            || string.IsNullOrWhiteSpace(membership.SpeciesId)
            || string.IsNullOrWhiteSpace(membership.DecisionMetadataJson))
        {
            return false;
        }

        JsonNode? node;
        try
        {
            node = JsonNode.Parse(membership.DecisionMetadataJson);
        }
        catch (JsonException)
        {
            return false;
        }

        if (node is not JsonObject root)
        {
            return false;
        }

        root.TryGetPropertyValue("lineage", out var lineageNode);
        var assignedSimilaritySource = FindStringValue(
            lineageNode,
            "assigned_species_similarity_source",
            "assignedSpeciesSimilaritySource");
        if (!string.Equals(
                assignedSimilaritySource,
                "compatibility_assessment",
                StringComparison.Ordinal))
        {
            return false;
        }

        var sampleSpeciesId = FindStringValue(
            lineageNode,
            "intra_species_similarity_species_id",
            "intraSpeciesSimilaritySpeciesId");
        return string.IsNullOrWhiteSpace(sampleSpeciesId)
            || string.Equals(
                sampleSpeciesId.Trim(),
                membership.SpeciesId.Trim(),
                StringComparison.Ordinal);
    }
}
