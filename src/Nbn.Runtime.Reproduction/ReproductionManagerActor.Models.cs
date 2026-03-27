using Nbn.Proto;
using Nbn.Proto.Repro;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;

namespace Nbn.Runtime.Reproduction;

public sealed partial class ReproductionManagerActor
{
    private sealed record ParsedParent(
        NbnHeaderV2 Header,
        IReadOnlyList<NbnRegionSection> Regions);

    private sealed record LoadParentResult(
        ParsedParent? Parsed,
        string? AbortReason);

    private sealed record TransformParentResult(
        ParsedParent Parsed);

    private sealed record OverlayApplyResult(
        ParsedParent Parent,
        int MatchedRoutes,
        int IgnoredRoutes);

    private sealed record ResolvedParentArtifact(
        ArtifactRef? ParentDef,
        ArtifactRef? ParentState,
        string? AbortReason);

    private sealed record ChildBuildResult(
        ArtifactRef? ChildDef,
        MutationSummary? Summary,
        string? AbortReason,
        int RegionsPresentChild = 0,
        float LineageSimilarityScore = 0f,
        float LineageParentASimilarityScore = 0f,
        float LineageParentBSimilarityScore = 0f);

    private sealed record PreparedReproductionRun(
        uint RunCount,
        bool AssessmentOnly,
        ParsedParent GateParentA,
        ParsedParent GateParentB,
        ParsedParent TransformParentA,
        ParsedParent TransformParentB,
        ArtifactRef ParentARef,
        ArtifactRef ParentBRef,
        ReproduceConfig? Config,
        ulong Seed,
        IReadOnlyList<ManualIoNeuronEdit> ManualIoNeuronAdds,
        IReadOnlyList<ManualIoNeuronEdit> ManualIoNeuronRemoves);

    private readonly record struct LineageSimilarityScores(
        float LineageSimilarityScore,
        float ParentASimilarityScore,
        float ParentBSimilarityScore);

    private sealed record ConnectivityHistogram(
        int[] OutDegreeCounts,
        int OutDegreeTotal,
        int[] TargetRegionCounts,
        int TargetRegionTotal);

    private sealed class MutableRegion
    {
        public MutableRegion(int regionId, List<MutableNeuron> neurons)
        {
            RegionId = regionId;
            Neurons = neurons;
        }

        public int RegionId { get; }

        public List<MutableNeuron> Neurons { get; }
    }

    private sealed class MutableNeuron
    {
        public MutableNeuron(NeuronRecord template, bool exists, List<AxonRecord> axons)
        {
            Template = template;
            Exists = exists;
            Axons = axons;
        }

        public NeuronRecord Template { get; }

        public bool Exists { get; set; }

        public List<AxonRecord> Axons { get; }
    }

    private sealed class MutationBudgets
    {
        public MutationBudgets(
            int maxNeuronsAdded,
            int maxNeuronsRemoved,
            int maxAxonsAdded,
            int maxAxonsRemoved,
            int maxRegionsAdded,
            int maxRegionsRemoved)
        {
            MaxNeuronsAdded = maxNeuronsAdded;
            MaxNeuronsRemoved = maxNeuronsRemoved;
            MaxAxonsAdded = maxAxonsAdded;
            MaxAxonsRemoved = maxAxonsRemoved;
            MaxRegionsAdded = maxRegionsAdded;
            MaxRegionsRemoved = maxRegionsRemoved;
        }

        public int MaxNeuronsAdded { get; }

        public int MaxNeuronsRemoved { get; }

        public int MaxAxonsAdded { get; }

        public int MaxAxonsRemoved { get; }

        public int MaxRegionsAdded { get; }

        public int MaxRegionsRemoved { get; }

        public uint NeuronsAdded { get; private set; }

        public uint NeuronsRemoved { get; private set; }

        public uint AxonsAdded { get; private set; }

        public uint AxonsRemoved { get; private set; }

        public uint RegionsAdded { get; private set; }

        public uint RegionsRemoved { get; private set; }

        public uint AxonsRerouted { get; set; }

        public uint FunctionsMutated { get; set; }

        public uint StrengthCodesChanged { get; private set; }

        public bool CanAddNeuron => NeuronsAdded < (uint)MaxNeuronsAdded;

        public bool CanRemoveNeuron => NeuronsRemoved < (uint)MaxNeuronsRemoved;

        public bool CanAddAxon => AxonsAdded < (uint)MaxAxonsAdded;

        public bool CanRemoveAxon => AxonsRemoved < (uint)MaxAxonsRemoved;

        public bool CanAddRegion => RegionsAdded < (uint)MaxRegionsAdded;

        public bool CanRemoveRegion => RegionsRemoved < (uint)MaxRegionsRemoved;

        public void ConsumeNeuronAdded()
        {
            if (CanAddNeuron)
            {
                NeuronsAdded++;
            }
        }

        public void ConsumeNeuronRemoved()
        {
            if (CanRemoveNeuron)
            {
                NeuronsRemoved++;
            }
        }

        public void ConsumeAxonAdded()
        {
            if (CanAddAxon)
            {
                AxonsAdded++;
            }
        }

        public void ConsumeAxonRemoved()
        {
            if (CanRemoveAxon)
            {
                AxonsRemoved++;
            }
        }

        public void ConsumeAxonsRemoved(int count)
        {
            if (count <= 0)
            {
                return;
            }

            for (var i = 0; i < count && CanRemoveAxon; i++)
            {
                AxonsRemoved++;
            }
        }

        public void ConsumeRegionAdded()
        {
            if (CanAddRegion)
            {
                RegionsAdded++;
            }
        }

        public void ConsumeRegionRemoved()
        {
            if (CanRemoveRegion)
            {
                RegionsRemoved++;
            }
        }

        public void RecordStrengthCodeChanged()
        {
            StrengthCodesChanged++;
        }
    }

    private enum ValueSelectionMode : byte
    {
        ParentA = 0,
        ParentB = 1,
        Average = 2,
        Mutate = 3
    }

    private readonly record struct TargetLocus(int RegionId, int NeuronId);

    private readonly record struct WeightedTargetCandidate(TargetLocus Target, float Weight);

    private readonly record struct PruneCandidate(
        int SourceRegionId,
        int SourceNeuronId,
        int AxonIndex,
        float StrengthDistance,
        bool IsNewConnection);

    private sealed record NeuronLocus(int RegionId, int NeuronId);
}
