using Nbn.Proto;
using Nbn.Shared;

namespace Nbn.Tools.EvolutionSim;

public sealed partial class EvolutionSimulationSession
{
    private static bool TryBuildParentRefFromCandidate(
        SpeciationCommitCandidate candidate,
        EvolutionParentMode parentMode,
        out EvolutionParentRef parentRef)
    {
        if (parentMode == EvolutionParentMode.BrainIds)
        {
            if (candidate.ChildBrainId is Guid childBrainId && childBrainId != Guid.Empty)
            {
                parentRef = EvolutionParentRef.FromBrainId(childBrainId);
                return true;
            }

            if (candidate.ChildDefinition is not null)
            {
                parentRef = EvolutionParentRef.FromArtifactRef(candidate.ChildDefinition);
                return true;
            }
        }
        else if (candidate.ChildDefinition is not null)
        {
            parentRef = EvolutionParentRef.FromArtifactRef(candidate.ChildDefinition);
            return true;
        }

        parentRef = default;
        return false;
    }

    private static bool TryBuildParentKey(EvolutionParentRef parentRef, out string key)
    {
        key = string.Empty;
        if (parentRef.BrainId is Guid brainId && brainId != Guid.Empty)
        {
            key = $"{BrainParentKeyPrefix}{brainId:D}";
            return true;
        }

        if (parentRef.ArtifactRef is { } artifactRef)
        {
            if (artifactRef.TryToSha256Hex(out var sha))
            {
                key = $"{ArtifactParentKeyPrefix}{sha}";
                return true;
            }

            var storeUri = string.IsNullOrWhiteSpace(artifactRef.StoreUri)
                ? string.Empty
                : artifactRef.StoreUri.Trim();
            if (!string.IsNullOrWhiteSpace(storeUri))
            {
                var mediaType = string.IsNullOrWhiteSpace(artifactRef.MediaType)
                    ? string.Empty
                    : artifactRef.MediaType.Trim();
                key = $"{ArtifactUriParentKeyPrefix}{storeUri}|{mediaType}|{artifactRef.SizeBytes}";
                return true;
            }
        }

        return false;
    }

    private static bool IsOpaqueParentIdentityKey(string normalizedKey)
    {
        return normalizedKey.StartsWith(BrainParentKeyPrefix, StringComparison.OrdinalIgnoreCase)
               || normalizedKey.StartsWith(ArtifactParentKeyPrefix, StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryBuildSeedCandidate(
        EvolutionParentRef parentRef,
        CompatibilityAssessment seedAssessment,
        out SpeciationCommitCandidate candidate)
    {
        var hasAssessedSimilarity = TryResolveSeedSimilarity(seedAssessment, out var assessedSimilarity);
        if (parentRef.BrainId is Guid brainId && brainId != Guid.Empty)
        {
            candidate = new SpeciationCommitCandidate(
                ChildBrainId: brainId,
                ChildDefinition: null,
                SimilarityScore: hasAssessedSimilarity ? assessedSimilarity : null,
                LineageSimilarityScore: hasAssessedSimilarity ? assessedSimilarity : null,
                LineageParentASimilarityScore: hasAssessedSimilarity ? 1f : null,
                LineageParentBSimilarityScore: hasAssessedSimilarity ? assessedSimilarity : null);
            return true;
        }

        if (parentRef.ArtifactRef is { } artifactRef
            && artifactRef.TryToSha256Hex(out _))
        {
            candidate = new SpeciationCommitCandidate(
                ChildBrainId: null,
                ChildDefinition: artifactRef,
                SimilarityScore: hasAssessedSimilarity ? assessedSimilarity : null,
                LineageSimilarityScore: hasAssessedSimilarity ? assessedSimilarity : null,
                LineageParentASimilarityScore: hasAssessedSimilarity ? 1f : null,
                LineageParentBSimilarityScore: hasAssessedSimilarity ? assessedSimilarity : null);
            return true;
        }

        candidate = default;
        return false;
    }

    private static bool TryResolveSeedSimilarity(
        CompatibilityAssessment seedAssessment,
        out float similarity)
    {
        similarity = 0f;
        if (!seedAssessment.Success
            || float.IsNaN(seedAssessment.SimilarityScore)
            || float.IsInfinity(seedAssessment.SimilarityScore))
        {
            return false;
        }

        similarity = Math.Clamp(seedAssessment.SimilarityScore, 0f, 1f);
        return true;
    }

    private static bool TrySelectSeedPartner(
        IReadOnlyList<EvolutionParentRef> parentPoolSnapshot,
        EvolutionParentRef parentRef,
        out EvolutionParentRef partner)
    {
        if (parentPoolSnapshot.Count == 0)
        {
            partner = default;
            return false;
        }

        if (!TryBuildParentKey(parentRef, out var seedKey))
        {
            partner = parentPoolSnapshot[0];
            return true;
        }

        foreach (var candidate in parentPoolSnapshot)
        {
            if (!TryBuildParentKey(candidate, out var candidateKey))
            {
                continue;
            }

            if (!string.Equals(seedKey, candidateKey, StringComparison.OrdinalIgnoreCase))
            {
                partner = candidate;
                return true;
            }
        }

        partner = parentPoolSnapshot[0];
        return true;
    }
}
