using Nbn.Proto;
using Nbn.Shared;

namespace Nbn.Tools.EvolutionSim;

public sealed partial class EvolutionSimulationSession
{
    private int AddChildrenToPool(ReproductionOutcome reproduction)
    {
        if (_options.ParentMode == EvolutionParentMode.BrainIds)
        {
            return AddSpawnedChildrenToBrainPool(reproduction.CommitCandidates);
        }

        return AddChildrenToArtifactPool(reproduction.ChildDefinitions);
    }

    private bool TryAddCommittedCandidateToPool(
        SpeciationCommitCandidate candidate,
        string? candidateSpeciesId,
        string? candidateSourceSpeciesId)
    {
        if (!TryBuildParentRefFromCandidate(candidate, _options.ParentMode, out var parentRef)
            || !TryBuildParentKey(parentRef, out var candidateKey))
        {
            return false;
        }

        var normalizedCandidateSpeciesId = NormalizeSpeciesId(candidateSpeciesId);
        lock (_gate)
        {
            if (_parentPoolKeys.Contains(candidateKey))
            {
                if (normalizedCandidateSpeciesId.Length > 0)
                {
                    RecordParentSpeciesLocked(
                        candidateKey,
                        normalizedCandidateSpeciesId,
                        candidateSourceSpeciesId);
                }

                return false;
            }

            if (!TryAddParentToPoolAtCapacity(
                    parentRef,
                    candidateKey,
                    normalizedCandidateSpeciesId,
                    candidateSourceSpeciesId))
            {
                return false;
            }

            if (normalizedCandidateSpeciesId.Length > 0)
            {
                RecordParentSpeciesLocked(
                    candidateKey,
                    normalizedCandidateSpeciesId,
                    candidateSourceSpeciesId);
            }

            _childrenAddedToPool++;
            return true;
        }
    }

    private int AddChildrenToArtifactPool(IReadOnlyList<ArtifactRef> children)
    {
        if (children.Count == 0)
        {
            return 0;
        }

        var addedCount = 0;
        lock (_gate)
        {
            foreach (var child in children)
            {
                var candidate = EvolutionParentRef.FromArtifactRef(child);
                if (!TryBuildParentKey(candidate, out var key))
                {
                    continue;
                }

                if (_parentPoolKeys.Contains(key))
                {
                    continue;
                }

                if (!TryAddParentToPoolAtCapacity(candidate, key))
                {
                    continue;
                }

                _childrenAddedToPool++;
                addedCount++;
            }
        }

        return addedCount;
    }

    private int AddSpawnedChildrenToBrainPool(IReadOnlyList<SpeciationCommitCandidate> candidates)
    {
        if (candidates.Count == 0)
        {
            return 0;
        }

        var addedCount = 0;
        lock (_gate)
        {
            foreach (var candidate in candidates)
            {
                if (candidate.ChildBrainId is not Guid childBrainId || childBrainId == Guid.Empty)
                {
                    continue;
                }

                var parentRef = EvolutionParentRef.FromBrainId(childBrainId);
                if (!TryBuildParentKey(parentRef, out var key))
                {
                    continue;
                }

                if (_parentPoolKeys.Contains(key))
                {
                    continue;
                }

                if (!TryAddParentToPoolAtCapacity(parentRef, key))
                {
                    continue;
                }

                _childrenAddedToPool++;
                addedCount++;
            }
        }

        return addedCount;
    }

    private void RecordParentSpecies(
        string parentKey,
        string speciesId,
        string? sourceSpeciesId)
    {
        if (string.IsNullOrWhiteSpace(parentKey) || string.IsNullOrWhiteSpace(speciesId))
        {
            return;
        }

        lock (_gate)
        {
            RecordParentSpeciesLocked(parentKey, speciesId, sourceSpeciesId);
        }
    }

    // Caller must hold _gate.
    private void RecordParentSpeciesLocked(
        string parentKey,
        string speciesId,
        string? sourceSpeciesId)
    {
        var normalizedSpeciesId = speciesId.Trim();
        if (normalizedSpeciesId.Length == 0)
        {
            return;
        }

        _parentSpeciesByParentKey[parentKey] = normalizedSpeciesId;
        if (!_speciesFirstSeenOrdinals.ContainsKey(normalizedSpeciesId))
        {
            _speciesFirstSeenOrdinals[normalizedSpeciesId] = _nextSpeciesOrdinal++;
        }

        var lineageFamilyId = ResolveLineageFamilyKeyLocked(
            normalizedSpeciesId,
            sourceSpeciesId,
            registerIfMissing: true);
        if (lineageFamilyId.Length > 0)
        {
            _parentLineageFamilyByParentKey[parentKey] = lineageFamilyId;
        }
    }

    private void NormalizeInitialSeedSelectionOrdinals(IReadOnlyCollection<string> seededParentKeys)
    {
        if (seededParentKeys is null || seededParentKeys.Count < 2)
        {
            return;
        }

        lock (_gate)
        {
            var seededSpeciesIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var seededLineageFamilyIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var seededParentKey in seededParentKeys)
            {
                if (_parentSpeciesByParentKey.TryGetValue(seededParentKey, out var speciesId)
                    && NormalizeSpeciesId(speciesId).Length > 0)
                {
                    seededSpeciesIds.Add(NormalizeSpeciesId(speciesId));
                }

                if (_parentLineageFamilyByParentKey.TryGetValue(seededParentKey, out var lineageFamilyId)
                    && NormalizeSpeciesId(lineageFamilyId).Length > 0)
                {
                    seededLineageFamilyIds.Add(NormalizeSpeciesId(lineageFamilyId));
                }
            }

            if (seededSpeciesIds.Count > 1)
            {
                var sharedSpeciesOrdinal = seededSpeciesIds
                    .Select(speciesId => _speciesFirstSeenOrdinals.TryGetValue(speciesId, out var ordinal) ? ordinal : 0UL)
                    .Where(static ordinal => ordinal > 0)
                    .DefaultIfEmpty(0UL)
                    .Min();
                if (sharedSpeciesOrdinal > 0)
                {
                    foreach (var speciesId in seededSpeciesIds)
                    {
                        _speciesFirstSeenOrdinals[speciesId] = sharedSpeciesOrdinal;
                    }
                }
            }

            if (seededLineageFamilyIds.Count > 1)
            {
                var sharedLineageFamilyOrdinal = seededLineageFamilyIds
                    .Select(lineageFamilyId => _lineageFamilyFirstSeenOrdinals.TryGetValue(lineageFamilyId, out var ordinal) ? ordinal : 0UL)
                    .Where(static ordinal => ordinal > 0)
                    .DefaultIfEmpty(0UL)
                    .Min();
                if (sharedLineageFamilyOrdinal > 0)
                {
                    foreach (var lineageFamilyId in seededLineageFamilyIds)
                    {
                        _lineageFamilyFirstSeenOrdinals[lineageFamilyId] = sharedLineageFamilyOrdinal;
                    }
                }
            }
        }
    }

    // Caller must hold _gate.
    private string ResolveLineageFamilyKeyLocked(
        string? speciesId,
        string? sourceSpeciesId,
        bool registerIfMissing)
    {
        var normalizedSpeciesId = NormalizeSpeciesId(speciesId);
        if (normalizedSpeciesId.Length == 0)
        {
            return string.Empty;
        }

        if (_lineageFamilyBySpeciesId.TryGetValue(normalizedSpeciesId, out var existingLineageFamilyId)
            && NormalizeSpeciesId(existingLineageFamilyId).Length > 0)
        {
            return NormalizeSpeciesId(existingLineageFamilyId);
        }

        var normalizedSourceSpeciesId = NormalizeSpeciesId(sourceSpeciesId);
        var lineageFamilyId = normalizedSpeciesId;
        if (normalizedSourceSpeciesId.Length > 0
            && !string.Equals(normalizedSourceSpeciesId, normalizedSpeciesId, StringComparison.OrdinalIgnoreCase))
        {
            lineageFamilyId = _lineageFamilyBySpeciesId.TryGetValue(normalizedSourceSpeciesId, out var sourceLineageFamilyId)
                && NormalizeSpeciesId(sourceLineageFamilyId).Length > 0
                ? NormalizeSpeciesId(sourceLineageFamilyId)
                : normalizedSourceSpeciesId;
        }

        if (!registerIfMissing)
        {
            return lineageFamilyId;
        }

        if (normalizedSourceSpeciesId.Length > 0
            && !_lineageFamilyBySpeciesId.ContainsKey(normalizedSourceSpeciesId))
        {
            _lineageFamilyBySpeciesId[normalizedSourceSpeciesId] = lineageFamilyId;
        }

        _lineageFamilyBySpeciesId[normalizedSpeciesId] = lineageFamilyId;
        EnsureLineageFamilyOrdinalLocked(lineageFamilyId);
        return lineageFamilyId;
    }

    // Caller must hold _gate.
    private void EnsureLineageFamilyOrdinalLocked(string lineageFamilyId)
    {
        var normalizedLineageFamilyId = NormalizeSpeciesId(lineageFamilyId);
        if (normalizedLineageFamilyId.Length == 0)
        {
            return;
        }

        if (!_lineageFamilyFirstSeenOrdinals.ContainsKey(normalizedLineageFamilyId))
        {
            _lineageFamilyFirstSeenOrdinals[normalizedLineageFamilyId] = _nextLineageFamilyOrdinal++;
        }
    }

    // Caller must hold _gate.
    private bool TryAddParentToPoolAtCapacity(
        EvolutionParentRef candidate,
        string candidateKey,
        string? candidateSpeciesId = null,
        string? candidateSourceSpeciesId = null)
    {
        if (_parentPool.Count < _options.MaxParentPoolSize)
        {
            _parentPool.Add(candidate);
            _parentAddedOrdinals.Add(_nextParentOrdinal++);
            _parentPoolKeys.Add(candidateKey);
            return true;
        }

        var candidateLineageFamilyId = ResolveLineageFamilyKeyLocked(
            candidateSpeciesId,
            candidateSourceSpeciesId,
            registerIfMissing: false);
        if (!TrySelectEvictionIndex(
                candidateSpeciesId,
                candidateLineageFamilyId,
                out var evictionIndex))
        {
            return false;
        }

        var evicted = _parentPool[evictionIndex];
        if (TryBuildParentKey(evicted, out var evictedKey))
        {
            _parentPoolKeys.Remove(evictedKey);
            _parentSpeciesByParentKey.Remove(evictedKey);
            _parentLineageFamilyByParentKey.Remove(evictedKey);
        }

        _parentPool[evictionIndex] = candidate;
        if (evictionIndex < _parentAddedOrdinals.Count)
        {
            _parentAddedOrdinals[evictionIndex] = _nextParentOrdinal++;
        }
        else
        {
            _parentAddedOrdinals.Add(_nextParentOrdinal++);
        }
        _parentPoolKeys.Add(candidateKey);
        return true;
    }

    // Caller must hold _gate.
    private bool TrySelectEvictionIndex(
        string? candidateSpeciesId,
        string? candidateLineageFamilyId,
        out int evictionIndex)
    {
        var totalSpeciesCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var totalLineageFamilyCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < _parentPool.Count; i++)
        {
            var current = _parentPool[i];
            var speciesKey = ResolveTrackedSpeciesKey(current);
            if (totalSpeciesCounts.TryGetValue(speciesKey, out var currentCount))
            {
                totalSpeciesCounts[speciesKey] = currentCount + 1;
            }
            else
            {
                totalSpeciesCounts[speciesKey] = 1;
            }

            var lineageFamilyKey = ResolveTrackedLineageFamilyKey(current);
            if (totalLineageFamilyCounts.TryGetValue(lineageFamilyKey, out var currentLineageFamilyCount))
            {
                totalLineageFamilyCounts[lineageFamilyKey] = currentLineageFamilyCount + 1;
            }
            else
            {
                totalLineageFamilyCounts[lineageFamilyKey] = 1;
            }
        }

        var eligibleEntries = new List<(int Index, string SpeciesKey, int TotalSpeciesCount, string LineageFamilyKey, int TotalLineageFamilyCount)>();
        for (var i = 0; i < _parentPool.Count; i++)
        {
            var current = _parentPool[i];
            if (TryBuildParentKey(current, out var currentKey)
                && _protectedParentPoolKeys.Contains(currentKey))
            {
                continue;
            }

            var speciesKey = ResolveTrackedSpeciesKey(current);
            var totalSpeciesCount = totalSpeciesCounts.TryGetValue(speciesKey, out var count)
                ? count
                : 1;
            var lineageFamilyKey = ResolveTrackedLineageFamilyKey(current);
            var totalLineageFamilyCount = totalLineageFamilyCounts.TryGetValue(lineageFamilyKey, out var lineageCount)
                ? lineageCount
                : 1;
            eligibleEntries.Add((i, speciesKey, totalSpeciesCount, lineageFamilyKey, totalLineageFamilyCount));
        }

        if (eligibleEntries.Count == 0)
        {
            evictionIndex = -1;
            return false;
        }

        var normalizedCandidateLineageFamilyId = ResolveTrackedLineageFamilyKey(candidateLineageFamilyId);
        if (!string.Equals(normalizedCandidateLineageFamilyId, "(unknown)", StringComparison.Ordinal))
        {
            var candidateLineageFamilyCount = totalLineageFamilyCounts.TryGetValue(
                normalizedCandidateLineageFamilyId,
                out var lineageCount)
                ? lineageCount
                : 0;
            var moreRepresentedLineageFamilyEntries = eligibleEntries
                .Where(entry => entry.TotalLineageFamilyCount > candidateLineageFamilyCount)
                .ToList();
            if (moreRepresentedLineageFamilyEntries.Count > 0)
            {
                eligibleEntries = moreRepresentedLineageFamilyEntries;
            }
            else if (candidateLineageFamilyCount > 0)
            {
                var sameLineageFamilyEntries = eligibleEntries
                    .Where(entry => string.Equals(
                        entry.LineageFamilyKey,
                        normalizedCandidateLineageFamilyId,
                        StringComparison.OrdinalIgnoreCase))
                    .ToList();
                if (sameLineageFamilyEntries.Count == 0)
                {
                    evictionIndex = -1;
                    return false;
                }

                eligibleEntries = sameLineageFamilyEntries;
            }
        }

        var normalizedCandidateSpeciesId = NormalizeSpeciesId(candidateSpeciesId);
        if (normalizedCandidateSpeciesId.Length > 0)
        {
            var candidateSpeciesKey = ResolveTrackedSpeciesKey(normalizedCandidateSpeciesId);
            var candidateSpeciesCount = totalSpeciesCounts.TryGetValue(candidateSpeciesKey, out var count)
                ? count
                : 0;
            if (candidateSpeciesCount > 0)
            {
                var moreRepresentedEntries = eligibleEntries
                    .Where(entry => entry.TotalSpeciesCount > candidateSpeciesCount)
                    .ToList();
                if (moreRepresentedEntries.Count == 0)
                {
                    var sameSpeciesEntries = eligibleEntries
                        .Where(entry => string.Equals(
                            entry.SpeciesKey,
                            candidateSpeciesKey,
                            StringComparison.OrdinalIgnoreCase))
                        .ToList();
                    if (sameSpeciesEntries.Count == 0)
                    {
                        evictionIndex = -1;
                        return false;
                    }

                    eligibleEntries = sameSpeciesEntries;
                }
                else
                {
                    eligibleEntries = moreRepresentedEntries;
                }
            }
        }

        var maxLineageFamilyCount = eligibleEntries.Max(entry => entry.TotalLineageFamilyCount);
        var narrowedEntries = eligibleEntries
            .Where(entry => entry.TotalLineageFamilyCount == maxLineageFamilyCount)
            .ToList();
        var maxSpeciesCount = narrowedEntries.Max(entry => entry.TotalSpeciesCount);
        var selectedIndex = -1;
        var eligibleCount = 0;
        foreach (var entry in narrowedEntries)
        {
            if (entry.TotalSpeciesCount != maxSpeciesCount)
            {
                continue;
            }

            eligibleCount++;
            if (eligibleCount == 1 || _random.NextInt(eligibleCount) == 0)
            {
                selectedIndex = entry.Index;
            }
        }

        evictionIndex = selectedIndex;
        return selectedIndex >= 0;
    }

    private string ResolveTrackedLineageFamilyKey(EvolutionParentRef parentRef)
    {
        if (TryBuildParentKey(parentRef, out var parentKey))
        {
            return ResolveTrackedLineageFamilyKey(parentKey);
        }

        return "(unknown)";
    }

    private string ResolveTrackedLineageFamilyKey(string? lineageFamilyOrParentKey)
    {
        var normalized = NormalizeSpeciesId(lineageFamilyOrParentKey);
        if (normalized.Length == 0)
        {
            return "(unknown)";
        }

        return _parentLineageFamilyByParentKey.TryGetValue(normalized, out var lineageFamilyId)
            ? NormalizeSpeciesId(lineageFamilyId).Length > 0
                ? NormalizeSpeciesId(lineageFamilyId)
                : "(unknown)"
            : _lineageFamilyBySpeciesId.TryGetValue(normalized, out var speciesLineageFamilyId)
                ? NormalizeSpeciesId(speciesLineageFamilyId).Length > 0
                    ? NormalizeSpeciesId(speciesLineageFamilyId)
                    : "(unknown)"
                : normalized.StartsWith("artifact:", StringComparison.OrdinalIgnoreCase)
                  || normalized.StartsWith("brain:", StringComparison.OrdinalIgnoreCase)
                    ? "(unknown)"
                    : normalized;
    }

    private string ResolveTrackedSpeciesKey(EvolutionParentRef parentRef)
    {
        if (TryBuildParentKey(parentRef, out var parentKey))
        {
            return ResolveTrackedSpeciesKey(parentKey);
        }

        return "(unknown)";
    }

    private string ResolveTrackedSpeciesKey(string? speciesOrParentKey)
    {
        var normalized = NormalizeSpeciesId(speciesOrParentKey);
        if (normalized.Length == 0)
        {
            return "(unknown)";
        }

        return _parentSpeciesByParentKey.TryGetValue(normalized, out var speciesId)
            ? NormalizeSpeciesId(speciesId).Length > 0
                ? NormalizeSpeciesId(speciesId)
                : "(unknown)"
            : normalized.StartsWith("artifact:", StringComparison.OrdinalIgnoreCase)
                || normalized.StartsWith("brain:", StringComparison.OrdinalIgnoreCase)
                ? "(unknown)"
                : normalized;
    }

    private static string NormalizeSpeciesId(string? speciesId)
    {
        return string.IsNullOrWhiteSpace(speciesId)
            ? string.Empty
            : speciesId.Trim();
    }
}
