namespace Nbn.Shared;

public static class PlasticitySettingsKeys
{
    public const string SystemEnabledKey = "plasticity.system.enabled";

    public static readonly IReadOnlyList<string> AllKeys = new[]
    {
        SystemEnabledKey
    };
}
