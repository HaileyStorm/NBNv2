namespace Nbn.Shared;

public static class TickSettingsKeys
{
    public const string CadenceHzKey = "tick.cadence.hz";

    public static readonly IReadOnlyList<string> AllKeys = new[]
    {
        CadenceHzKey
    };
}
