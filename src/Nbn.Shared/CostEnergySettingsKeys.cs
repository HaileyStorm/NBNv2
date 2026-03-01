namespace Nbn.Shared;

public static class CostEnergySettingsKeys
{
    public const string SystemEnabledKey = "cost_energy.system.enabled";
    public const string RemoteCostEnabledKey = "cost_energy.remote_cost.enabled";
    public const string RemoteCostPerBatchKey = "cost_energy.remote_cost.per_batch";
    public const string RemoteCostPerContributionKey = "cost_energy.remote_cost.per_contribution";
    public const string TierAMultiplierKey = "cost_energy.tier_a.multiplier";
    public const string TierBMultiplierKey = "cost_energy.tier_b.multiplier";
    public const string TierCMultiplierKey = "cost_energy.tier_c.multiplier";

    public static readonly IReadOnlyList<string> AllKeys = new[]
    {
        SystemEnabledKey,
        RemoteCostEnabledKey,
        RemoteCostPerBatchKey,
        RemoteCostPerContributionKey,
        TierAMultiplierKey,
        TierBMultiplierKey,
        TierCMultiplierKey
    };
}
