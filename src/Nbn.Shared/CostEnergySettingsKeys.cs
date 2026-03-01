namespace Nbn.Shared;

public static class CostEnergySettingsKeys
{
    public const string SystemEnabledKey = "cost_energy.system.enabled";

    public static readonly IReadOnlyList<string> AllKeys = new[]
    {
        SystemEnabledKey
    };
}
