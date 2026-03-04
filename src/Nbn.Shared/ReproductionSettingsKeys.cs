namespace Nbn.Shared;

public static class ReproductionSettingsKeys
{
    public const string StrengthSourceKey = "repro.config.strength_source";

    public const string MaxRegionSpanDiffRatioKey = "repro.config.max_region_span_diff_ratio";
    public const string MaxFunctionHistDistanceKey = "repro.config.max_function_hist_distance";
    public const string MaxConnectivityHistDistanceKey = "repro.config.max_connectivity_hist_distance";

    public const string ProbAddNeuronToEmptyRegionKey = "repro.config.prob_add_neuron_to_empty_region";
    public const string ProbRemoveLastNeuronFromRegionKey = "repro.config.prob_remove_last_neuron_from_region";
    public const string ProbDisableNeuronKey = "repro.config.prob_disable_neuron";
    public const string ProbReactivateNeuronKey = "repro.config.prob_reactivate_neuron";

    public const string ProbAddAxonKey = "repro.config.prob_add_axon";
    public const string ProbRemoveAxonKey = "repro.config.prob_remove_axon";
    public const string ProbRerouteAxonKey = "repro.config.prob_reroute_axon";
    public const string ProbRerouteInboundAxonOnDeleteKey = "repro.config.prob_reroute_inbound_axon_on_delete";
    public const string InboundRerouteMaxRingDistanceKey = "repro.config.inbound_reroute_max_ring_distance";

    public const string ProbChooseParentAKey = "repro.config.prob_choose_parent_a";
    public const string ProbChooseParentBKey = "repro.config.prob_choose_parent_b";
    public const string ProbAverageKey = "repro.config.prob_average";
    public const string ProbMutateKey = "repro.config.prob_mutate";
    public const string ProbChooseFuncAKey = "repro.config.prob_choose_func_a";
    public const string ProbMutateFuncKey = "repro.config.prob_mutate_func";

    public const string MaxAvgOutDegreeBrainKey = "repro.config.max_avg_out_degree_brain";
    public const string PrunePolicyKey = "repro.config.prune_policy";
    public const string PerRegionOutDegreeCapsKey = "repro.config.per_region_out_degree_caps";

    public const string StrengthTransformEnabledKey = "repro.config.strength_transform_enabled";
    public const string ProbStrengthChooseAKey = "repro.config.prob_strength_choose_a";
    public const string ProbStrengthChooseBKey = "repro.config.prob_strength_choose_b";
    public const string ProbStrengthAverageKey = "repro.config.prob_strength_average";
    public const string ProbStrengthWeightedAverageKey = "repro.config.prob_strength_weighted_average";
    public const string StrengthWeightAKey = "repro.config.strength_weight_a";
    public const string StrengthWeightBKey = "repro.config.strength_weight_b";
    public const string ProbStrengthMutateKey = "repro.config.prob_strength_mutate";

    public const string MaxNeuronsAddedAbsKey = "repro.config.max_neurons_added_abs";
    public const string MaxNeuronsAddedPctKey = "repro.config.max_neurons_added_pct";
    public const string MaxNeuronsRemovedAbsKey = "repro.config.max_neurons_removed_abs";
    public const string MaxNeuronsRemovedPctKey = "repro.config.max_neurons_removed_pct";
    public const string MaxAxonsAddedAbsKey = "repro.config.max_axons_added_abs";
    public const string MaxAxonsAddedPctKey = "repro.config.max_axons_added_pct";
    public const string MaxAxonsRemovedAbsKey = "repro.config.max_axons_removed_abs";
    public const string MaxAxonsRemovedPctKey = "repro.config.max_axons_removed_pct";
    public const string MaxRegionsAddedAbsKey = "repro.config.max_regions_added_abs";
    public const string MaxRegionsRemovedAbsKey = "repro.config.max_regions_removed_abs";

    public const string SpawnChildKey = "repro.config.spawn_child";

    public static readonly IReadOnlyList<string> AllKeys = new[]
    {
        StrengthSourceKey,
        MaxRegionSpanDiffRatioKey,
        MaxFunctionHistDistanceKey,
        MaxConnectivityHistDistanceKey,
        ProbAddNeuronToEmptyRegionKey,
        ProbRemoveLastNeuronFromRegionKey,
        ProbDisableNeuronKey,
        ProbReactivateNeuronKey,
        ProbAddAxonKey,
        ProbRemoveAxonKey,
        ProbRerouteAxonKey,
        ProbRerouteInboundAxonOnDeleteKey,
        InboundRerouteMaxRingDistanceKey,
        ProbChooseParentAKey,
        ProbChooseParentBKey,
        ProbAverageKey,
        ProbMutateKey,
        ProbChooseFuncAKey,
        ProbMutateFuncKey,
        MaxAvgOutDegreeBrainKey,
        PrunePolicyKey,
        PerRegionOutDegreeCapsKey,
        StrengthTransformEnabledKey,
        ProbStrengthChooseAKey,
        ProbStrengthChooseBKey,
        ProbStrengthAverageKey,
        ProbStrengthWeightedAverageKey,
        StrengthWeightAKey,
        StrengthWeightBKey,
        ProbStrengthMutateKey,
        MaxNeuronsAddedAbsKey,
        MaxNeuronsAddedPctKey,
        MaxNeuronsRemovedAbsKey,
        MaxNeuronsRemovedPctKey,
        MaxAxonsAddedAbsKey,
        MaxAxonsAddedPctKey,
        MaxAxonsRemovedAbsKey,
        MaxAxonsRemovedPctKey,
        MaxRegionsAddedAbsKey,
        MaxRegionsRemovedAbsKey,
        SpawnChildKey
    };
}
