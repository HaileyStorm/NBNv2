using Nbn.Proto;
using Nbn.Proto.Repro;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Validation;

namespace Nbn.Runtime.Reproduction;

public sealed partial class ReproductionManagerActor
{
    private static void ApplyStructuralMutations(
        Dictionary<int, MutableRegion> regions,
        ReproduceConfig? config,
        ref ulong state,
        MutationBudgets budgets)
    {
        if (config is null)
        {
            return;
        }

        var addNeuronToEmptyProbability = ClampProbability(config.ProbAddNeuronToEmptyRegion);
        var removeLastNeuronProbability = ClampProbability(config.ProbRemoveLastNeuronFromRegion);
        var disableNeuronProbability = ClampProbability(config.ProbDisableNeuron);
        var reactivateNeuronProbability = ClampProbability(config.ProbReactivateNeuron);
        var addAxonProbability = ClampProbability(config.ProbAddAxon);
        var removeAxonProbability = ClampProbability(config.ProbRemoveAxon);
        var rerouteAxonProbability = ClampProbability(config.ProbRerouteAxon);
        var rerouteInboundOnDeleteProbability = ClampProbability(config.ProbRerouteInboundAxonOnDelete);
        var inboundRerouteMaxRingDistance = config.InboundRerouteMaxRingDistance > 0
            ? (int)Math.Min(config.InboundRerouteMaxRingDistance, int.MaxValue)
            : 0;

        for (var regionId = 1; regionId < NbnConstants.OutputRegionId; regionId++)
        {
            if (regions.ContainsKey(regionId))
            {
                continue;
            }

            if (!budgets.CanAddNeuron || !budgets.CanAddRegion || !ShouldMutate(addNeuronToEmptyProbability, ref state))
            {
                continue;
            }

            regions[regionId] = new MutableRegion(
                regionId,
                new List<MutableNeuron> { CreateDefaultMutableNeuron() });
            budgets.ConsumeNeuronAdded();
            budgets.ConsumeRegionAdded();
        }

        foreach (var regionId in regions.Keys.OrderBy(static id => id).ToArray())
        {
            if (regionId <= NbnConstants.InputRegionId || regionId >= NbnConstants.OutputRegionId)
            {
                continue;
            }

            if (!regions.TryGetValue(regionId, out var region))
            {
                continue;
            }

            if (CountExistingNeurons(region) != 1 || !budgets.CanRemoveNeuron || !budgets.CanRemoveRegion)
            {
                continue;
            }

            var existingNeuronId = FindSingleExistingNeuron(region);
            if (existingNeuronId < 0)
            {
                continue;
            }

            if (!ShouldMutate(removeLastNeuronProbability, ref state))
            {
                continue;
            }

            if (!HandleNeuronDeletionSideEffects(
                    regions,
                    regionId,
                    existingNeuronId,
                    rerouteInboundOnDeleteProbability,
                    inboundRerouteMaxRingDistance,
                    ref state,
                    budgets))
            {
                continue;
            }

            budgets.ConsumeNeuronRemoved();
            budgets.ConsumeRegionRemoved();
            regions.Remove(regionId);
        }

        foreach (var regionId in regions.Keys.OrderBy(static id => id).ToArray())
        {
            if (regionId <= NbnConstants.InputRegionId || regionId >= NbnConstants.OutputRegionId)
            {
                continue;
            }

            if (!regions.TryGetValue(regionId, out var region))
            {
                continue;
            }

            var existingCount = CountExistingNeurons(region);
            for (var neuronId = 0; neuronId < region.Neurons.Count; neuronId++)
            {
                var neuron = region.Neurons[neuronId];
                if (neuron.Exists)
                {
                    if (!budgets.CanRemoveNeuron || !ShouldMutate(disableNeuronProbability, ref state))
                    {
                        continue;
                    }

                    if (existingCount == 1 && !budgets.CanRemoveRegion)
                    {
                        continue;
                    }

                    if (!HandleNeuronDeletionSideEffects(
                            regions,
                            regionId,
                            neuronId,
                            rerouteInboundOnDeleteProbability,
                            inboundRerouteMaxRingDistance,
                            ref state,
                            budgets))
                    {
                        continue;
                    }

                    if (existingCount == 1)
                    {
                        if (!budgets.CanRemoveRegion)
                        {
                            continue;
                        }

                        budgets.ConsumeNeuronRemoved();
                        budgets.ConsumeRegionRemoved();
                        regions.Remove(regionId);
                        break;
                    }

                    neuron.Exists = false;
                    neuron.Axons.Clear();
                    budgets.ConsumeNeuronRemoved();
                    existingCount--;
                    continue;
                }

                if (!budgets.CanAddNeuron || !ShouldMutate(reactivateNeuronProbability, ref state))
                {
                    continue;
                }

                neuron.Exists = true;
                budgets.ConsumeNeuronAdded();
                existingCount++;
            }
        }

        var addedConnections = new HashSet<ulong>();
        foreach (var regionId in regions.Keys.OrderBy(static id => id))
        {
            if (!regions.TryGetValue(regionId, out var region))
            {
                continue;
            }

            for (var neuronId = 0; neuronId < region.Neurons.Count; neuronId++)
            {
                var neuron = region.Neurons[neuronId];
                if (!neuron.Exists)
                {
                    neuron.Axons.Clear();
                    continue;
                }

                var existingTargets = new HashSet<uint>();
                for (var i = 0; i < neuron.Axons.Count; i++)
                {
                    existingTargets.Add(BuildTargetKey(neuron.Axons[i].TargetRegionId, neuron.Axons[i].TargetNeuronId));
                }

                for (var axonIndex = neuron.Axons.Count - 1; axonIndex >= 0; axonIndex--)
                {
                    if (!budgets.CanRemoveAxon || !ShouldMutate(removeAxonProbability, ref state))
                    {
                        continue;
                    }

                    var axon = neuron.Axons[axonIndex];
                    neuron.Axons.RemoveAt(axonIndex);
                    existingTargets.Remove(BuildTargetKey(axon.TargetRegionId, axon.TargetNeuronId));
                    budgets.ConsumeAxonRemoved();
                }

                for (var axonIndex = 0; axonIndex < neuron.Axons.Count; axonIndex++)
                {
                    if (!ShouldMutate(rerouteAxonProbability, ref state))
                    {
                        continue;
                    }

                    var current = neuron.Axons[axonIndex];
                    var currentTarget = BuildTargetKey(current.TargetRegionId, current.TargetNeuronId);
                    if (!TrySelectTarget(
                            regions,
                            regionId,
                            existingTargets,
                            currentTarget,
                            ref state,
                            out var newTarget))
                    {
                        continue;
                    }

                    neuron.Axons[axonIndex] = new AxonRecord(current.StrengthCode, newTarget.NeuronId, (byte)newTarget.RegionId);
                    existingTargets.Remove(currentTarget);
                    existingTargets.Add(BuildTargetKey(newTarget.RegionId, newTarget.NeuronId));
                    budgets.AxonsRerouted++;
                }

                if (budgets.CanAddAxon
                    && neuron.Axons.Count < NbnConstants.MaxAxonsPerNeuron
                    && ShouldMutate(addAxonProbability, ref state)
                    && TrySelectTarget(regions, regionId, existingTargets, null, ref state, out var addedTarget))
                {
                    var strengthCode = (byte)(NextRandom(ref state) & 0x1F);
                    neuron.Axons.Add(new AxonRecord(strengthCode, addedTarget.NeuronId, (byte)addedTarget.RegionId));
                    existingTargets.Add(BuildTargetKey(addedTarget.RegionId, addedTarget.NeuronId));
                    budgets.ConsumeAxonAdded();
                    addedConnections.Add(BuildConnectionKey(regionId, neuronId, addedTarget.RegionId, addedTarget.NeuronId));
                }
            }
        }

        EnforceAverageOutDegree(regions, config, ref state, budgets, addedConnections);
    }

    private static MutableNeuron CreateDefaultMutableNeuron()
    {
        var defaultNeuron = new NeuronRecord(
            axonCount: 0,
            paramBCode: 0,
            paramACode: 0,
            activationThresholdCode: 0,
            preActivationThresholdCode: 0,
            resetFunctionId: 0,
            activationFunctionId: 1,
            accumulationFunctionId: 0,
            exists: true);
        return new MutableNeuron(defaultNeuron, true, new List<AxonRecord>());
    }

    private static bool TryApplyManualIoNeuronOperations(
        Dictionary<int, MutableRegion> regions,
        ReproduceConfig? config,
        IReadOnlyList<ManualIoNeuronEdit> manualIoNeuronAdds,
        IReadOnlyList<ManualIoNeuronEdit> manualIoNeuronRemoves,
        MutationBudgets budgets,
        out string? abortReason)
    {
        abortReason = null;
        if (manualIoNeuronAdds.Count == 0 && manualIoNeuronRemoves.Count == 0)
        {
            return true;
        }

        if (ResolveProtectIoRegionNeuronCounts(config))
        {
            abortReason = "repro_io_neuron_count_protected";
            return false;
        }

        if (!TryNormalizeManualIoNeuronEdits(manualIoNeuronAdds, out var normalizedAdds)
            || !TryNormalizeManualIoNeuronEdits(manualIoNeuronRemoves, out var normalizedRemoves)
            || !ApplyManualIoNeuronRemovals(regions, normalizedRemoves, budgets)
            || !ApplyManualIoNeuronAdds(regions, normalizedAdds, budgets))
        {
            abortReason = "repro_manual_io_neuron_ops_invalid";
            return false;
        }

        return true;
    }

    private static bool ResolveProtectIoRegionNeuronCounts(ReproduceConfig? config)
    {
        if (config is null)
        {
            return true;
        }

        return !config.HasProtectIoRegionNeuronCounts || config.ProtectIoRegionNeuronCounts;
    }

    private static bool TryNormalizeManualIoNeuronEdits(
        IReadOnlyList<ManualIoNeuronEdit> edits,
        out List<(int RegionId, int NeuronId)> normalized)
    {
        normalized = new List<(int RegionId, int NeuronId)>(edits.Count);
        for (var i = 0; i < edits.Count; i++)
        {
            var edit = edits[i];
            if (edit.RegionId > int.MaxValue || edit.NeuronId > int.MaxValue)
            {
                return false;
            }

            var regionId = (int)edit.RegionId;
            var neuronId = (int)edit.NeuronId;
            if ((regionId != NbnConstants.InputRegionId && regionId != NbnConstants.OutputRegionId)
                || !NbnInvariants.IsValidAxonTargetNeuronId(neuronId))
            {
                return false;
            }

            normalized.Add((regionId, neuronId));
        }

        return true;
    }

    private static bool ApplyManualIoNeuronRemovals(
        Dictionary<int, MutableRegion> regions,
        IReadOnlyList<(int RegionId, int NeuronId)> removals,
        MutationBudgets budgets)
    {
        ulong state = 0;
        foreach (var removal in removals
                     .OrderBy(static op => op.RegionId)
                     .ThenByDescending(static op => op.NeuronId))
        {
            if (!regions.TryGetValue(removal.RegionId, out var region)
                || removal.NeuronId < 0
                || removal.NeuronId >= region.Neurons.Count
                || !region.Neurons[removal.NeuronId].Exists
                || CountExistingNeurons(region) <= 1
                || !budgets.CanRemoveNeuron)
            {
                return false;
            }

            if (!HandleNeuronDeletionSideEffects(
                    regions,
                    removal.RegionId,
                    removal.NeuronId,
                    rerouteInboundProbability: 0f,
                    inboundRerouteMaxRingDistance: 0,
                    ref state,
                    budgets))
            {
                return false;
            }

            region.Neurons.RemoveAt(removal.NeuronId);
            ReindexTargetNeuronIdsAfterRemoval(regions, removal.RegionId, removal.NeuronId);
            budgets.ConsumeNeuronRemoved();
        }

        return true;
    }

    private static bool ApplyManualIoNeuronAdds(
        Dictionary<int, MutableRegion> regions,
        IReadOnlyList<(int RegionId, int NeuronId)> additions,
        MutationBudgets budgets)
    {
        foreach (var add in additions
                     .OrderBy(static op => op.RegionId)
                     .ThenBy(static op => op.NeuronId))
        {
            if (!regions.TryGetValue(add.RegionId, out var region)
                || add.NeuronId != region.Neurons.Count
                || !budgets.CanAddNeuron)
            {
                return false;
            }

            var nextSpan = region.Neurons.Count + 1;
            if (!NbnInvariants.IsValidRegionSpan(nextSpan))
            {
                return false;
            }

            region.Neurons.Add(CreateDefaultMutableNeuron());
            budgets.ConsumeNeuronAdded();
        }

        return true;
    }

    private static void ReindexTargetNeuronIdsAfterRemoval(
        IReadOnlyDictionary<int, MutableRegion> regions,
        int targetRegionId,
        int removedNeuronId)
    {
        foreach (var pair in regions)
        {
            var sourceRegion = pair.Value;
            for (var sourceNeuronId = 0; sourceNeuronId < sourceRegion.Neurons.Count; sourceNeuronId++)
            {
                var sourceNeuron = sourceRegion.Neurons[sourceNeuronId];
                if (!sourceNeuron.Exists || sourceNeuron.Axons.Count == 0)
                {
                    continue;
                }

                for (var axonIndex = 0; axonIndex < sourceNeuron.Axons.Count; axonIndex++)
                {
                    var axon = sourceNeuron.Axons[axonIndex];
                    if (axon.TargetRegionId == targetRegionId && axon.TargetNeuronId > removedNeuronId)
                    {
                        sourceNeuron.Axons[axonIndex] = new AxonRecord(
                            axon.StrengthCode,
                            axon.TargetNeuronId - 1,
                            axon.TargetRegionId);
                    }
                }
            }
        }
    }

    private static void EnforceAverageOutDegree(
        Dictionary<int, MutableRegion> regions,
        ReproduceConfig config,
        ref ulong state,
        MutationBudgets budgets,
        HashSet<ulong> addedConnections)
    {
        var normalizedGlobalCap = NormalizeOutDegreeCap(config.MaxAvgOutDegreeBrain);
        var perRegionCaps = ResolvePerRegionOutDegreeCaps(config);
        if (!normalizedGlobalCap.HasValue && perRegionCaps.Count == 0)
        {
            return;
        }

        if (normalizedGlobalCap.HasValue)
        {
            EnforceAverageOutDegreeLimit(
                regions,
                normalizedGlobalCap.Value,
                sourceRegionIdFilter: null,
                config.PrunePolicy,
                ref state,
                budgets,
                addedConnections);
        }

        if (perRegionCaps.Count == 0)
        {
            return;
        }

        foreach (var regionCap in perRegionCaps.OrderBy(static pair => pair.Key))
        {
            EnforceAverageOutDegreeLimit(
                regions,
                regionCap.Value,
                regionCap.Key,
                config.PrunePolicy,
                ref state,
                budgets,
                addedConnections);
        }
    }

    private static void EnforceAverageOutDegreeLimit(
        Dictionary<int, MutableRegion> regions,
        float maxAvgOutDegree,
        int? sourceRegionIdFilter,
        PrunePolicy prunePolicy,
        ref ulong state,
        MutationBudgets budgets,
        HashSet<ulong> addedConnections)
    {
        if (!TryGetOutDegreeTotals(regions, sourceRegionIdFilter, out var existingNeurons, out var totalAxons))
        {
            return;
        }

        var allowedAxons = ResolveAllowedAxons(maxAvgOutDegree, existingNeurons);
        while (totalAxons > allowedAxons)
        {
            var candidates = CollectPruneCandidates(regions, addedConnections, sourceRegionIdFilter);
            if (candidates.Count == 0)
            {
                break;
            }

            var selected = SelectPruneCandidate(candidates, prunePolicy, ref state);
            if (!regions.TryGetValue(selected.SourceRegionId, out var region))
            {
                break;
            }

            var neuron = region.Neurons[selected.SourceNeuronId];
            if (selected.AxonIndex < 0 || selected.AxonIndex >= neuron.Axons.Count)
            {
                break;
            }

            neuron.Axons.RemoveAt(selected.AxonIndex);
            totalAxons--;
            budgets.ConsumeAxonRemoved();
        }
    }

    private static int ResolveAllowedAxons(float maxAvgOutDegree, int existingNeurons)
    {
        var allowedAxons = (int)Math.Floor(maxAvgOutDegree * existingNeurons);
        if (allowedAxons < 0)
        {
            allowedAxons = 0;
        }

        return allowedAxons;
    }

    private static bool TryGetOutDegreeTotals(
        IReadOnlyDictionary<int, MutableRegion> regions,
        int? sourceRegionIdFilter,
        out int existingNeurons,
        out int totalAxons)
    {
        existingNeurons = 0;
        totalAxons = 0;
        foreach (var pair in regions)
        {
            if (sourceRegionIdFilter.HasValue && pair.Key != sourceRegionIdFilter.Value)
            {
                continue;
            }

            var region = pair.Value;
            for (var neuronId = 0; neuronId < region.Neurons.Count; neuronId++)
            {
                var neuron = region.Neurons[neuronId];
                if (!neuron.Exists)
                {
                    continue;
                }

                existingNeurons++;
                totalAxons += neuron.Axons.Count;
            }
        }

        return existingNeurons > 0;
    }

    private static float? NormalizeOutDegreeCap(float cap)
    {
        if (cap <= 0f || float.IsNaN(cap) || float.IsInfinity(cap))
        {
            return null;
        }

        return cap;
    }

    private static Dictionary<int, float> ResolvePerRegionOutDegreeCaps(ReproduceConfig config)
    {
        var caps = new Dictionary<int, float>();
        foreach (var cap in config.PerRegionOutDegreeCaps)
        {
            if (cap.RegionId > NbnConstants.RegionMaxId)
            {
                continue;
            }

            var normalized = NormalizeOutDegreeCap(cap.MaxAvgOutDegree);
            if (!normalized.HasValue)
            {
                continue;
            }

            var regionId = (int)cap.RegionId;
            if (caps.TryGetValue(regionId, out var existing))
            {
                caps[regionId] = Math.Min(existing, normalized.Value);
                continue;
            }

            caps.Add(regionId, normalized.Value);
        }

        return caps;
    }

    private static List<PruneCandidate> CollectPruneCandidates(
        Dictionary<int, MutableRegion> regions,
        HashSet<ulong> addedConnections,
        int? sourceRegionIdFilter = null)
    {
        var candidates = new List<PruneCandidate>();
        foreach (var pair in regions)
        {
            var regionId = pair.Key;
            if (sourceRegionIdFilter.HasValue && regionId != sourceRegionIdFilter.Value)
            {
                continue;
            }

            var region = pair.Value;
            for (var neuronId = 0; neuronId < region.Neurons.Count; neuronId++)
            {
                var neuron = region.Neurons[neuronId];
                if (!neuron.Exists)
                {
                    continue;
                }

                for (var axonIndex = 0; axonIndex < neuron.Axons.Count; axonIndex++)
                {
                    var axon = neuron.Axons[axonIndex];
                    var key = BuildConnectionKey(regionId, neuronId, axon.TargetRegionId, axon.TargetNeuronId);
                    candidates.Add(new PruneCandidate(
                        regionId,
                        neuronId,
                        axonIndex,
                        MathF.Abs(axon.StrengthCode - 15.5f),
                        addedConnections.Contains(key)));
                }
            }
        }

        return candidates;
    }

    private static PruneCandidate SelectPruneCandidate(
        List<PruneCandidate> candidates,
        PrunePolicy policy,
        ref ulong state)
    {
        switch (policy)
        {
            case PrunePolicy.PruneNewConnectionsFirst:
            {
                var newest = candidates.FirstOrDefault(static candidate => candidate.IsNewConnection);
                if (newest.IsNewConnection)
                {
                    return newest;
                }

                break;
            }
            case PrunePolicy.PruneRandom:
            {
                var index = (int)(NextRandom(ref state) % (ulong)candidates.Count);
                return candidates[index];
            }
        }

        return candidates
            .OrderBy(static candidate => candidate.StrengthDistance)
            .ThenBy(static candidate => candidate.SourceRegionId)
            .ThenBy(static candidate => candidate.SourceNeuronId)
            .ThenBy(static candidate => candidate.AxonIndex)
            .First();
    }

    private static List<NbnRegionSection> BuildSectionsFromMutableRegions(
        Dictionary<int, MutableRegion> regions,
        uint stride,
        MutationBudgets budgets)
    {
        var sections = new List<NbnRegionSection>(regions.Count);
        foreach (var regionId in regions.Keys.OrderBy(static id => id))
        {
            if (!regions.TryGetValue(regionId, out var region))
            {
                continue;
            }

            if (regionId != NbnConstants.InputRegionId
                && regionId != NbnConstants.OutputRegionId
                && CountExistingNeurons(region) == 0)
            {
                continue;
            }

            var neurons = new NeuronRecord[region.Neurons.Count];
            var axons = new List<AxonRecord>();
            for (var neuronId = 0; neuronId < region.Neurons.Count; neuronId++)
            {
                var mutable = region.Neurons[neuronId];
                var exists = mutable.Exists;
                if ((regionId == NbnConstants.InputRegionId || regionId == NbnConstants.OutputRegionId) && !exists)
                {
                    exists = true;
                }

                var removedAxons = 0;
                var normalizedAxons = exists
                    ? NormalizeAxons(regionId, mutable.Axons, regions, out removedAxons)
                    : new List<AxonRecord>();
                if (removedAxons > 0)
                {
                    budgets.ConsumeAxonsRemoved(removedAxons);
                }

                if (normalizedAxons.Count > NbnConstants.MaxAxonsPerNeuron)
                {
                    var trimCount = normalizedAxons.Count - NbnConstants.MaxAxonsPerNeuron;
                    normalizedAxons.RemoveRange(NbnConstants.MaxAxonsPerNeuron, trimCount);
                    budgets.ConsumeAxonsRemoved(trimCount);
                }

                neurons[neuronId] = new NeuronRecord(
                    axonCount: (ushort)normalizedAxons.Count,
                    paramBCode: mutable.Template.ParamBCode,
                    paramACode: mutable.Template.ParamACode,
                    activationThresholdCode: mutable.Template.ActivationThresholdCode,
                    preActivationThresholdCode: mutable.Template.PreActivationThresholdCode,
                    resetFunctionId: mutable.Template.ResetFunctionId,
                    activationFunctionId: mutable.Template.ActivationFunctionId,
                    accumulationFunctionId: mutable.Template.AccumulationFunctionId,
                    exists: exists);
                axons.AddRange(normalizedAxons);
            }

            var checkpoints = NbnBinary.BuildCheckpoints(neurons, stride);
            sections.Add(new NbnRegionSection(
                (byte)regionId,
                (uint)neurons.Length,
                (ulong)axons.Count,
                stride,
                (uint)checkpoints.Length,
                checkpoints,
                neurons,
                axons.ToArray()));
        }

        return sections;
    }

    private static List<AxonRecord> NormalizeAxons(
        int sourceRegionId,
        List<AxonRecord> axons,
        IReadOnlyDictionary<int, MutableRegion> regions,
        out int removedAxons)
    {
        var normalized = new List<AxonRecord>(axons.Count);
        var seenTargets = new HashSet<uint>();
        removedAxons = 0;

        for (var i = 0; i < axons.Count; i++)
        {
            var axon = axons[i];
            if (!IsValidTarget(sourceRegionId, axon, regions))
            {
                removedAxons++;
                continue;
            }

            var targetKey = BuildTargetKey(axon.TargetRegionId, axon.TargetNeuronId);
            if (!seenTargets.Add(targetKey))
            {
                removedAxons++;
                continue;
            }

            normalized.Add(axon);
        }

        normalized.Sort(static (left, right) =>
        {
            var regionComparison = left.TargetRegionId.CompareTo(right.TargetRegionId);
            return regionComparison != 0
                ? regionComparison
                : left.TargetNeuronId.CompareTo(right.TargetNeuronId);
        });
        return normalized;
    }

    private static bool IsValidTarget(
        int sourceRegionId,
        AxonRecord axon,
        IReadOnlyDictionary<int, MutableRegion> regions)
    {
        if (!IsValidTargetRegion(sourceRegionId, axon.TargetRegionId))
        {
            return false;
        }

        if (!regions.TryGetValue(axon.TargetRegionId, out var targetRegion))
        {
            return false;
        }

        if (axon.TargetNeuronId < 0 || axon.TargetNeuronId >= targetRegion.Neurons.Count)
        {
            return false;
        }

        return targetRegion.Neurons[axon.TargetNeuronId].Exists;
    }

    private static bool IsValidTargetRegion(int sourceRegionId, int targetRegionId)
    {
        if (targetRegionId == NbnConstants.InputRegionId)
        {
            return false;
        }

        if (sourceRegionId == NbnConstants.OutputRegionId && targetRegionId == NbnConstants.OutputRegionId)
        {
            return false;
        }

        return true;
    }

    private static bool TrySelectTarget(
        IReadOnlyDictionary<int, MutableRegion> regions,
        int sourceRegionId,
        HashSet<uint> existingTargets,
        uint? excludedTarget,
        ref ulong state,
        out TargetLocus target)
    {
        var candidates = new List<TargetLocus>();
        foreach (var regionId in regions.Keys.OrderBy(static id => id))
        {
            if (!regions.TryGetValue(regionId, out var region) || !IsValidTargetRegion(sourceRegionId, regionId))
            {
                continue;
            }

            for (var neuronId = 0; neuronId < region.Neurons.Count; neuronId++)
            {
                if (!region.Neurons[neuronId].Exists)
                {
                    continue;
                }

                var targetKey = BuildTargetKey(regionId, neuronId);
                if (existingTargets.Contains(targetKey) || (excludedTarget.HasValue && excludedTarget.Value == targetKey))
                {
                    continue;
                }

                candidates.Add(new TargetLocus(regionId, neuronId));
            }
        }

        if (candidates.Count == 0)
        {
            target = default;
            return false;
        }

        var index = (int)(NextRandom(ref state) % (ulong)candidates.Count);
        target = candidates[index];
        return true;
    }

    private static bool HandleNeuronDeletionSideEffects(
        Dictionary<int, MutableRegion> regions,
        int targetRegionId,
        int targetNeuronId,
        float rerouteInboundProbability,
        int inboundRerouteMaxRingDistance,
        ref ulong state,
        MutationBudgets budgets)
    {
        if (!regions.TryGetValue(targetRegionId, out var targetRegion))
        {
            return false;
        }

        if (targetNeuronId < 0 || targetNeuronId >= targetRegion.Neurons.Count)
        {
            return false;
        }

        RerouteOrRemoveInboundAxons(
            regions,
            targetRegionId,
            targetNeuronId,
            rerouteInboundProbability,
            inboundRerouteMaxRingDistance,
            ref state,
            budgets);

        var targetNeuron = targetRegion.Neurons[targetNeuronId];
        if (targetNeuron.Axons.Count > 0)
        {
            budgets.ConsumeAxonsRemoved(targetNeuron.Axons.Count);
            targetNeuron.Axons.Clear();
        }

        return true;
    }

    private static void RerouteOrRemoveInboundAxons(
        Dictionary<int, MutableRegion> regions,
        int targetRegionId,
        int targetNeuronId,
        float rerouteInboundProbability,
        int inboundRerouteMaxRingDistance,
        ref ulong state,
        MutationBudgets budgets)
    {
        foreach (var pair in regions)
        {
            var sourceRegionId = pair.Key;
            var sourceRegion = pair.Value;

            for (var sourceNeuronId = 0; sourceNeuronId < sourceRegion.Neurons.Count; sourceNeuronId++)
            {
                if (sourceRegionId == targetRegionId && sourceNeuronId == targetNeuronId)
                {
                    continue;
                }

                var sourceNeuron = sourceRegion.Neurons[sourceNeuronId];
                if (!sourceNeuron.Exists || sourceNeuron.Axons.Count == 0)
                {
                    continue;
                }

                var existingTargets = new HashSet<uint>(sourceNeuron.Axons.Count);
                for (var i = 0; i < sourceNeuron.Axons.Count; i++)
                {
                    existingTargets.Add(BuildTargetKey(sourceNeuron.Axons[i].TargetRegionId, sourceNeuron.Axons[i].TargetNeuronId));
                }

                for (var axonIndex = sourceNeuron.Axons.Count - 1; axonIndex >= 0; axonIndex--)
                {
                    var axon = sourceNeuron.Axons[axonIndex];
                    if (axon.TargetRegionId != targetRegionId || axon.TargetNeuronId != targetNeuronId)
                    {
                        continue;
                    }

                    var currentTargetKey = BuildTargetKey(axon.TargetRegionId, axon.TargetNeuronId);
                    existingTargets.Remove(currentTargetKey);

                    if (ShouldMutate(rerouteInboundProbability, ref state)
                        && TrySelectInboundRerouteTarget(
                            regions,
                            sourceRegionId,
                            targetRegionId,
                            targetNeuronId,
                            inboundRerouteMaxRingDistance,
                            existingTargets,
                            ref state,
                            out var rerouteTarget))
                    {
                        sourceNeuron.Axons[axonIndex] = new AxonRecord(
                            axon.StrengthCode,
                            rerouteTarget.NeuronId,
                            (byte)rerouteTarget.RegionId);
                        existingTargets.Add(BuildTargetKey(rerouteTarget.RegionId, rerouteTarget.NeuronId));
                        budgets.AxonsRerouted++;
                        continue;
                    }

                    sourceNeuron.Axons.RemoveAt(axonIndex);
                    budgets.ConsumeAxonRemoved();
                }
            }
        }
    }

    private static bool TrySelectInboundRerouteTarget(
        IReadOnlyDictionary<int, MutableRegion> regions,
        int sourceRegionId,
        int targetRegionId,
        int deletedNeuronId,
        int inboundRerouteMaxRingDistance,
        HashSet<uint> existingTargets,
        ref ulong state,
        out TargetLocus target)
    {
        target = default;
        if (!IsValidTargetRegion(sourceRegionId, targetRegionId))
        {
            return false;
        }

        if (!regions.TryGetValue(targetRegionId, out var targetRegion))
        {
            return false;
        }

        var span = targetRegion.Neurons.Count;
        if (span <= 1)
        {
            return false;
        }

        var candidates = new List<WeightedTargetCandidate>(span);
        for (var neuronId = 0; neuronId < span; neuronId++)
        {
            if (neuronId == deletedNeuronId || !targetRegion.Neurons[neuronId].Exists)
            {
                continue;
            }

            var targetKey = BuildTargetKey(targetRegionId, neuronId);
            if (existingTargets.Contains(targetKey))
            {
                continue;
            }

            var delta = Math.Abs(neuronId - deletedNeuronId);
            var ringDistance = Math.Min(delta, span - delta);
            if (inboundRerouteMaxRingDistance > 0 && ringDistance > inboundRerouteMaxRingDistance)
            {
                continue;
            }

            var weight = 1f / (1f + ringDistance);
            candidates.Add(new WeightedTargetCandidate(new TargetLocus(targetRegionId, neuronId), weight));
        }

        if (candidates.Count == 0)
        {
            return false;
        }

        var totalWeight = 0f;
        for (var i = 0; i < candidates.Count; i++)
        {
            totalWeight += candidates[i].Weight;
        }

        if (totalWeight <= 0f)
        {
            return false;
        }

        var draw = NextUnitFloat(ref state) * totalWeight;
        for (var i = 0; i < candidates.Count; i++)
        {
            var candidate = candidates[i];
            if (draw <= candidate.Weight)
            {
                target = candidate.Target;
                return true;
            }

            draw -= candidate.Weight;
        }

        target = candidates[candidates.Count - 1].Target;
        return true;
    }

    private static int CountExistingNeurons(MutableRegion region)
    {
        var count = 0;
        for (var i = 0; i < region.Neurons.Count; i++)
        {
            if (region.Neurons[i].Exists)
            {
                count++;
            }
        }

        return count;
    }

    private static int FindSingleExistingNeuron(MutableRegion region)
    {
        for (var i = 0; i < region.Neurons.Count; i++)
        {
            if (region.Neurons[i].Exists)
            {
                return i;
            }
        }

        return -1;
    }

    private static bool ShouldMutate(float probability, ref ulong state)
        => probability > 0f && NextUnitFloat(ref state) < probability;

    private static float ClampProbability(float probability)
        => float.IsNaN(probability) ? 0f : Math.Clamp(probability, 0f, 1f);

    private static uint BuildTargetKey(int targetRegionId, int targetNeuronId)
        => ((uint)targetRegionId << NbnConstants.AddressNeuronBits) | (uint)targetNeuronId;

    private static ulong BuildConnectionKey(int sourceRegionId, int sourceNeuronId, int targetRegionId, int targetNeuronId)
        => ((ulong)(sourceRegionId & NbnConstants.RegionMaxId) << 59)
           | (((ulong)sourceNeuronId & 0x3FFFFFUL) << 37)
           | ((ulong)(targetRegionId & NbnConstants.RegionMaxId) << 32)
           | ((uint)targetNeuronId & 0x3FFFFFUL);

    private static bool ChooseParentA(float probability, ref ulong state)
    {
        if (probability <= 0f)
        {
            return false;
        }

        if (probability >= 1f)
        {
            return true;
        }

        return NextUnitFloat(ref state) < probability;
    }

    private static bool NeuronFunctionsDiffer(NeuronRecord left, NeuronRecord right)
        => left.ActivationFunctionId != right.ActivationFunctionId
           || left.ResetFunctionId != right.ResetFunctionId
           || left.AccumulationFunctionId != right.AccumulationFunctionId;

    private static float ResolveSelectionProbability(float chooseA, float chooseB)
    {
        var a = Math.Max(chooseA, 0f);
        var b = Math.Max(chooseB, 0f);
        if (a <= 0f && b <= 0f)
        {
            return 1f;
        }

        var total = a + b;
        if (total <= 0f)
        {
            return 1f;
        }

        return Math.Clamp(a / total, 0f, 1f);
    }

}
