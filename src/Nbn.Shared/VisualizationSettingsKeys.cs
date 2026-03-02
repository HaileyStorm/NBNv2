namespace Nbn.Shared;

public static class VisualizationSettingsKeys
{
    public const string TickMinIntervalMsKey = "viz.tick.min_interval_ms";
    public const string StreamMinIntervalMsKey = "viz.stream.min_interval_ms";

    public static readonly IReadOnlyList<string> AllKeys = new[]
    {
        TickMinIntervalMsKey,
        StreamMinIntervalMsKey
    };
}
