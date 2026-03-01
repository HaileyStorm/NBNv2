namespace Nbn.Shared;

public static class PlasticitySettingsKeys
{
    public const string SystemEnabledKey = "plasticity.system.enabled";
    public const string SystemRateKey = "plasticity.system.rate";
    public const string SystemProbabilisticUpdatesKey = "plasticity.system.probabilistic_updates";

    public static readonly IReadOnlyList<string> AllKeys = new[]
    {
        SystemEnabledKey,
        SystemRateKey,
        SystemProbabilisticUpdatesKey
    };
}
