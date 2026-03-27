using Nbn.Proto;
using Nbn.Shared;

namespace Nbn.Tools.EvolutionSim;

public sealed partial class EvolutionSimulationSession
{
    private bool ReachedIterationLimit()
    {
        if (_options.MaxIterations <= 0)
        {
            return false;
        }

        lock (_gate)
        {
            return _iterations >= (ulong)_options.MaxIterations;
        }
    }

    private bool TrySelectParents(out EvolutionParentRef parentA, out EvolutionParentRef parentB)
    {
        lock (_gate)
        {
            if (_parentPool.Count < 2)
            {
                parentA = default;
                parentB = default;
                return false;
            }

            var parentAIndex = SelectParentIndex(excludedIndex: -1);
            if (parentAIndex < 0)
            {
                parentA = default;
                parentB = default;
                return false;
            }

            var excludedParentKey = TryBuildParentKey(_parentPool[parentAIndex], out var parentAKey)
                ? parentAKey
                : string.Empty;
            var preferredLineageFamilyKey = ResolveTrackedLineageFamilyKey(_parentPool[parentAIndex]);
            var parentBIndex = SelectParentIndexForPair(
                excludedIndex: parentAIndex,
                excludedParentKey: excludedParentKey,
                preferredLineageFamilyKey: preferredLineageFamilyKey);
            if (parentBIndex < 0)
            {
                parentA = default;
                parentB = default;
                return false;
            }

            parentA = _parentPool[parentAIndex];
            parentB = _parentPool[parentBIndex];
            return true;
        }
    }

    // Caller must hold _gate.
    private int SelectParentIndex(int excludedIndex)
    {
        return SelectParentIndexCore(
            excludedIndex,
            excludedParentKey: null,
            preferredLineageFamilyKey: null);
    }

    // Caller must hold _gate.
    private int SelectParentIndexForPair(
        int excludedIndex,
        string? excludedParentKey,
        string? preferredLineageFamilyKey)
    {
        return SelectParentIndexCore(excludedIndex, excludedParentKey, preferredLineageFamilyKey);
    }

    // Caller must hold _gate.
    private int SelectParentIndexCore(
        int excludedIndex,
        string? excludedParentKey,
        string? preferredLineageFamilyKey)
    {
        if (_parentPool.Count == 0)
        {
            return -1;
        }

        var excludeParentKey = ShouldExcludeParentKey(excludedIndex, excludedParentKey);
        var preferLineageFamily = ShouldPreferLineageFamily(
            excludedIndex,
            excludedParentKey,
            preferredLineageFamilyKey);
        if (_options.ParentSelectionBias == EvolutionParentSelectionBias.Neutral)
        {
            return SelectUniformParentIndexCore(
                excludedIndex,
                excludedParentKey,
                excludeParentKey,
                preferredLineageFamilyKey,
                preferLineageFamily);
        }

        var speciesPopulationByKey = BuildSelectionSpeciesPopulationCounts(excludedIndex);
        var lineageFamilyPopulationByKey = BuildSelectionLineageFamilyPopulationCounts(excludedIndex);
        var useLineageFamilyAgeBias = lineageFamilyPopulationByKey.Count > 1;
        var flattenSingleFamilyDivergence =
            _options.ParentSelectionBias == EvolutionParentSelectionBias.Divergence
            && lineageFamilyPopulationByKey.Count == 1
            && speciesPopulationByKey.Count > 1;
        var flattenSpeciesAgeWithinFamily =
            _options.ParentSelectionBias == EvolutionParentSelectionBias.Divergence
            && speciesPopulationByKey.Count > 1;
        var useSpeciesAgeBias = !useLineageFamilyAgeBias && speciesPopulationByKey.Count > 1;

        var nowOrdinal = _nextParentOrdinal;
        var nowSpeciesOrdinal = _nextSpeciesOrdinal;
        var nowLineageFamilyOrdinal = _nextLineageFamilyOrdinal;
        var speciesAgeWeightByKey = useLineageFamilyAgeBias
            ? BuildSelectionSpeciesAgeWeights(
                speciesPopulationByKey.Keys,
                flattenSpeciesAgeWithinFamily,
                useFamilyRelativeSpeciesAge: true,
                nowSpeciesOrdinal)
            : null;
        var lineageFamilySpeciesWeightTotals = useLineageFamilyAgeBias
            ? BuildSelectionLineageFamilySpeciesWeightTotals(speciesAgeWeightByKey!)
            : null;
        double totalWeight = 0d;
        var weights = new double[_parentPool.Count];
        for (var i = 0; i < _parentPool.Count; i++)
        {
            if (i == excludedIndex)
            {
                continue;
            }

            if (excludeParentKey && IsExcludedParentKey(i, excludedParentKey))
            {
                continue;
            }

            if (preferLineageFamily && !MatchesPreferredLineageFamily(i, preferredLineageFamilyKey))
            {
                continue;
            }

            var speciesKey = ResolveTrackedSpeciesKey(_parentPool[i]);
            var speciesPopulation = ResolveSelectionSpeciesPopulation(i, speciesPopulationByKey);
            double weight;
            if (useLineageFamilyAgeBias)
            {
                var lineageFamilyKey = ResolveTrackedLineageFamilyKey(_parentPool[i]);
                var lineageFamilyAge = ResolveSelectionLineageFamilyAge(
                    lineageFamilyKey,
                    nowLineageFamilyOrdinal);
                var lineageFamilyAgeWeight = ResolveParentSelectionAgeWeight(lineageFamilyAge);
                var speciesAgeWeight = speciesAgeWeightByKey!.TryGetValue(speciesKey, out var trackedSpeciesAgeWeight)
                    && trackedSpeciesAgeWeight > 0d
                    ? trackedSpeciesAgeWeight
                    : 1d;
                var lineageFamilySpeciesWeightTotal =
                    lineageFamilySpeciesWeightTotals!.TryGetValue(
                        lineageFamilyKey,
                        out var trackedLineageFamilySpeciesWeightTotal)
                    && trackedLineageFamilySpeciesWeightTotal > 0d
                        ? trackedLineageFamilySpeciesWeightTotal
                        : speciesAgeWeight;
                weight = lineageFamilyAgeWeight
                    * (speciesAgeWeight / lineageFamilySpeciesWeightTotal)
                    / Math.Max(1d, speciesPopulation);
            }
            else
            {
                var age = flattenSingleFamilyDivergence
                    ? 1UL
                    : ResolveSelectionAgeForBias(
                        i,
                        useLineageFamilyAgeBias,
                        useSpeciesAgeBias,
                        nowLineageFamilyOrdinal,
                        nowSpeciesOrdinal,
                        nowOrdinal);
                weight = ResolveParentSelectionWeight(age, speciesPopulation);
            }

            if (!double.IsFinite(weight) || weight <= 0d)
            {
                continue;
            }

            weights[i] = weight;
            totalWeight += weight;
        }

        if (totalWeight <= 0d || !double.IsFinite(totalWeight))
        {
            return SelectUniformParentIndexCore(
                excludedIndex,
                excludedParentKey,
                excludeParentKey,
                preferredLineageFamilyKey,
                preferLineageFamily);
        }

        var sample = _random.NextUnitDouble() * totalWeight;
        var cumulative = 0d;
        for (var i = 0; i < weights.Length; i++)
        {
            var weight = weights[i];
            if (weight <= 0d)
            {
                continue;
            }

            cumulative += weight;
            if (sample <= cumulative)
            {
                return i;
            }
        }

        for (var i = weights.Length - 1; i >= 0; i--)
        {
            if (weights[i] > 0d)
            {
                return i;
            }
        }

        return -1;
    }

    // Caller must hold _gate.
    private int SelectUniformParentIndexCore(
        int excludedIndex,
        string? excludedParentKey,
        bool excludeParentKey,
        string? preferredLineageFamilyKey,
        bool preferLineageFamily)
    {
        var selectedIndex = -1;
        var eligibleCount = 0;
        for (var i = 0; i < _parentPool.Count; i++)
        {
            if (i == excludedIndex)
            {
                continue;
            }

            if (excludeParentKey && IsExcludedParentKey(i, excludedParentKey))
            {
                continue;
            }

            if (preferLineageFamily && !MatchesPreferredLineageFamily(i, preferredLineageFamilyKey))
            {
                continue;
            }

            eligibleCount++;
            if (eligibleCount == 1 || _random.NextInt(eligibleCount) == 0)
            {
                selectedIndex = i;
            }
        }

        return selectedIndex;
    }

    // Caller must hold _gate.
    private bool ShouldExcludeParentKey(int excludedIndex, string? excludedParentKey)
    {
        if (string.IsNullOrWhiteSpace(excludedParentKey))
        {
            return false;
        }

        for (var i = 0; i < _parentPool.Count; i++)
        {
            if (i == excludedIndex)
            {
                continue;
            }

            if (!IsExcludedParentKey(i, excludedParentKey))
            {
                return true;
            }
        }

        return false;
    }

    // Caller must hold _gate.
    private bool ShouldPreferLineageFamily(
        int excludedIndex,
        string? excludedParentKey,
        string? preferredLineageFamilyKey)
    {
        var normalizedPreferredLineageFamilyKey = NormalizeSpeciesId(preferredLineageFamilyKey);
        if (normalizedPreferredLineageFamilyKey.Length == 0
            || string.Equals(normalizedPreferredLineageFamilyKey, UnknownTrackedKey, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        for (var i = 0; i < _parentPool.Count; i++)
        {
            if (i == excludedIndex)
            {
                continue;
            }

            if (IsExcludedParentKey(i, excludedParentKey))
            {
                continue;
            }

            if (MatchesPreferredLineageFamily(i, normalizedPreferredLineageFamilyKey))
            {
                return true;
            }
        }

        return false;
    }

    // Caller must hold _gate.
    private bool IsExcludedParentKey(int parentIndex, string? excludedParentKey)
    {
        if (parentIndex < 0
            || parentIndex >= _parentPool.Count
            || string.IsNullOrWhiteSpace(excludedParentKey))
        {
            return false;
        }

        return TryBuildParentKey(_parentPool[parentIndex], out var candidateKey)
            && string.Equals(candidateKey, excludedParentKey, StringComparison.OrdinalIgnoreCase);
    }

    // Caller must hold _gate.
    private bool MatchesPreferredLineageFamily(int parentIndex, string? preferredLineageFamilyKey)
    {
        if (parentIndex < 0
            || parentIndex >= _parentPool.Count
            || string.IsNullOrWhiteSpace(preferredLineageFamilyKey))
        {
            return false;
        }

        return string.Equals(
            ResolveTrackedLineageFamilyKey(_parentPool[parentIndex]),
            preferredLineageFamilyKey,
            StringComparison.OrdinalIgnoreCase);
    }

    // Caller must hold _gate.
    private Dictionary<string, int> BuildSelectionSpeciesPopulationCounts(int excludedIndex)
    {
        var speciesCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < _parentPool.Count; i++)
        {
            if (i == excludedIndex)
            {
                continue;
            }

            var speciesKey = ResolveTrackedSpeciesKey(_parentPool[i]);
            speciesCounts[speciesKey] = speciesCounts.TryGetValue(speciesKey, out var count)
                ? count + 1
                : 1;
        }

        return speciesCounts;
    }

    // Caller must hold _gate.
    private Dictionary<string, int> BuildSelectionLineageFamilyPopulationCounts(int excludedIndex)
    {
        var lineageFamilyCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < _parentPool.Count; i++)
        {
            if (i == excludedIndex)
            {
                continue;
            }

            var lineageFamilyKey = ResolveTrackedLineageFamilyKey(_parentPool[i]);
            lineageFamilyCounts[lineageFamilyKey] = lineageFamilyCounts.TryGetValue(lineageFamilyKey, out var count)
                ? count + 1
                : 1;
        }

        return lineageFamilyCounts;
    }

    // Caller must hold _gate.
    private Dictionary<string, double> BuildSelectionSpeciesAgeWeights(
        IEnumerable<string> speciesKeys,
        bool flattenSpeciesAge,
        bool useFamilyRelativeSpeciesAge,
        ulong nowSpeciesOrdinal)
    {
        var normalizedSpeciesKeys = speciesKeys
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var familyRelativeNowSpeciesOrdinals = useFamilyRelativeSpeciesAge && !flattenSpeciesAge
            ? BuildSelectionCurrentSpeciesOrdinalsByLineageFamily(normalizedSpeciesKeys)
            : null;
        var weights = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        foreach (var speciesKey in normalizedSpeciesKeys)
        {
            var effectiveNowSpeciesOrdinal = nowSpeciesOrdinal;
            if (familyRelativeNowSpeciesOrdinals is not null)
            {
                var lineageFamilyKey = ResolveTrackedLineageFamilyKey(speciesKey);
                if (familyRelativeNowSpeciesOrdinals.TryGetValue(lineageFamilyKey, out var trackedNowSpeciesOrdinal)
                    && trackedNowSpeciesOrdinal > 0)
                {
                    effectiveNowSpeciesOrdinal = trackedNowSpeciesOrdinal;
                }
            }

            var age = flattenSpeciesAge
                ? 1UL
                : ResolveSelectionSpeciesAge(speciesKey, effectiveNowSpeciesOrdinal);
            weights[speciesKey] = ResolveParentSelectionAgeWeight(age);
        }

        return weights;
    }

    // Caller must hold _gate.
    private Dictionary<string, ulong> BuildSelectionCurrentSpeciesOrdinalsByLineageFamily(
        IEnumerable<string> speciesKeys)
    {
        var ordinals = new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase);
        foreach (var speciesKey in speciesKeys)
        {
            var lineageFamilyKey = ResolveTrackedLineageFamilyKey(speciesKey);
            var speciesOrdinal = _speciesFirstSeenOrdinals.TryGetValue(speciesKey, out var trackedSpeciesOrdinal)
                ? trackedSpeciesOrdinal
                : 0UL;
            var currentNowOrdinal = Math.Max(1UL, speciesOrdinal + 1UL);
            ordinals[lineageFamilyKey] = ordinals.TryGetValue(lineageFamilyKey, out var existingNowOrdinal)
                ? Math.Max(existingNowOrdinal, currentNowOrdinal)
                : currentNowOrdinal;
        }

        return ordinals;
    }

    // Caller must hold _gate.
    private Dictionary<string, double> BuildSelectionLineageFamilySpeciesWeightTotals(
        IReadOnlyDictionary<string, double> speciesAgeWeightByKey)
    {
        var totals = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        foreach (var entry in speciesAgeWeightByKey)
        {
            var lineageFamilyKey = ResolveTrackedLineageFamilyKey(entry.Key);
            totals[lineageFamilyKey] = totals.TryGetValue(lineageFamilyKey, out var total)
                ? total + entry.Value
                : entry.Value;
        }

        return totals;
    }

    // Caller must hold _gate.
    private ulong ResolveSelectionAgeForBias(
        int parentIndex,
        bool useLineageFamilyAgeBias,
        bool useSpeciesAgeBias,
        ulong nowLineageFamilyOrdinal,
        ulong nowSpeciesOrdinal,
        ulong nowParentOrdinal)
    {
        if (parentIndex < 0 || parentIndex >= _parentPool.Count)
        {
            return 1UL;
        }

        if (useLineageFamilyAgeBias
            && TryBuildParentKey(_parentPool[parentIndex], out var parentKey)
            && _parentLineageFamilyByParentKey.TryGetValue(parentKey, out var lineageFamilyId)
            && !string.IsNullOrWhiteSpace(lineageFamilyId)
            && _lineageFamilyFirstSeenOrdinals.TryGetValue(lineageFamilyId, out var firstSeenLineageFamilyOrdinal))
        {
            return Math.Max(1UL, nowLineageFamilyOrdinal - firstSeenLineageFamilyOrdinal);
        }

        if (useSpeciesAgeBias
            && TryBuildParentKey(_parentPool[parentIndex], out var speciesParentKey)
            && _parentSpeciesByParentKey.TryGetValue(speciesParentKey, out var speciesId)
            && !string.IsNullOrWhiteSpace(speciesId)
            && _speciesFirstSeenOrdinals.TryGetValue(speciesId, out var firstSeenSpeciesOrdinal))
        {
            return Math.Max(1UL, nowSpeciesOrdinal - firstSeenSpeciesOrdinal);
        }

        var addedOrdinal = parentIndex < _parentAddedOrdinals.Count ? _parentAddedOrdinals[parentIndex] : 1UL;
        return Math.Max(1UL, nowParentOrdinal - addedOrdinal);
    }

    // Caller must hold _gate.
    private ulong ResolveSelectionSpeciesAge(string speciesId, ulong nowSpeciesOrdinal)
    {
        var normalizedSpeciesId = NormalizeSpeciesId(speciesId);
        if (normalizedSpeciesId.Length == 0
            || !_speciesFirstSeenOrdinals.TryGetValue(normalizedSpeciesId, out var firstSeenSpeciesOrdinal))
        {
            return 1UL;
        }

        return Math.Max(1UL, nowSpeciesOrdinal - firstSeenSpeciesOrdinal);
    }

    // Caller must hold _gate.
    private ulong ResolveSelectionLineageFamilyAge(string lineageFamilyId, ulong nowLineageFamilyOrdinal)
    {
        var normalizedLineageFamilyId = NormalizeSpeciesId(lineageFamilyId);
        if (normalizedLineageFamilyId.Length == 0
            || !_lineageFamilyFirstSeenOrdinals.TryGetValue(
                normalizedLineageFamilyId,
                out var firstSeenLineageFamilyOrdinal))
        {
            return 1UL;
        }

        return Math.Max(1UL, nowLineageFamilyOrdinal - firstSeenLineageFamilyOrdinal);
    }

    // Caller must hold _gate.
    private int ResolveSelectionSpeciesPopulation(
        int parentIndex,
        IReadOnlyDictionary<string, int> speciesPopulationByKey)
    {
        if (parentIndex < 0 || parentIndex >= _parentPool.Count)
        {
            return 1;
        }

        var speciesKey = ResolveTrackedSpeciesKey(_parentPool[parentIndex]);
        return speciesPopulationByKey.TryGetValue(speciesKey, out var speciesPopulation)
            ? Math.Max(1, speciesPopulation)
            : 1;
    }

    private double ResolveParentSelectionWeight(ulong age, int speciesPopulation)
    {
        var ageWeight = ResolveParentSelectionAgeWeight(age);
        var representationWeight = 1d / Math.Max(1d, speciesPopulation);
        return ageWeight * representationWeight;
    }

    private double ResolveParentSelectionAgeWeight(ulong age)
    {
        var normalizedAge = Math.Max(1d, age);
        var weightedAge = Math.Pow(normalizedAge, ParentSelectionBiasExponent);
        return _options.ParentSelectionBias switch
        {
            EvolutionParentSelectionBias.Divergence => 1d / weightedAge,
            EvolutionParentSelectionBias.Stability => weightedAge,
            _ => 1d
        };
    }
}
