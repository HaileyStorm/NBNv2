namespace Nbn.Shared;

public static class TickSettingsKeys
{
    public const string OverrideHzKey = "tick.override.hz";

    public static readonly IReadOnlyList<string> AllKeys = new[]
    {
        OverrideHzKey
    };
}
