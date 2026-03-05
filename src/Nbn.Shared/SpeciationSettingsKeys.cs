namespace Nbn.Shared;

public static class SpeciationSettingsKeys
{
    private const string Prefix = "workbench.speciation";

    public const string ConfigEnabledKey = Prefix + ".config_enabled";
    public const string PolicyVersionKey = Prefix + ".policy_version";
    public const string DefaultSpeciesIdKey = Prefix + ".default_species_id";
    public const string DefaultSpeciesDisplayNameKey = Prefix + ".default_species_display_name";
    public const string StartupReconcileReasonKey = Prefix + ".startup_reconcile_reason";
    public const string LineageMatchThresholdKey = Prefix + ".lineage_match_threshold";
    public const string LineageSplitThresholdKey = Prefix + ".lineage_split_threshold";
    public const string ParentConsensusThresholdKey = Prefix + ".parent_consensus_threshold";
    public const string LineageHysteresisMarginKey = Prefix + ".lineage_hysteresis_margin";
    public const string LineageSplitGuardMarginKey = Prefix + ".lineage_split_guard_margin";
    public const string LineageMinParentMembershipsBeforeSplitKey = Prefix + ".lineage_min_parent_memberships_before_split";
    public const string LineageRealignParentMembershipWindowKey = Prefix + ".lineage_realign_parent_membership_window";
    public const string LineageRealignMatchMarginKey = Prefix + ".lineage_realign_match_margin";
    public const string CreateDerivedSpeciesOnDivergenceKey = Prefix + ".create_derived_species_on_divergence";
    public const string DerivedSpeciesPrefixKey = Prefix + ".derived_species_prefix";
    public const string HistoryLimitKey = Prefix + ".history_limit";

    public static readonly IReadOnlyList<string> AllKeys = new[]
    {
        ConfigEnabledKey,
        PolicyVersionKey,
        DefaultSpeciesIdKey,
        DefaultSpeciesDisplayNameKey,
        StartupReconcileReasonKey,
        LineageMatchThresholdKey,
        LineageSplitThresholdKey,
        ParentConsensusThresholdKey,
        LineageHysteresisMarginKey,
        LineageSplitGuardMarginKey,
        LineageMinParentMembershipsBeforeSplitKey,
        LineageRealignParentMembershipWindowKey,
        LineageRealignMatchMarginKey,
        CreateDerivedSpeciesOnDivergenceKey,
        DerivedSpeciesPrefixKey,
        HistoryLimitKey
    };
}
