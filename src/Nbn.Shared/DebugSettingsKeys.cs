namespace Nbn.Shared;

public static class DebugSettingsKeys
{
    public const string EnabledKey = "debug.stream.enabled";
    public const string MinSeverityKey = "debug.stream.min_severity";
    public const string ContextRegexKey = "debug.stream.context_regex";
    public const string IncludeContextPrefixesKey = "debug.stream.include_context_prefixes";
    public const string ExcludeContextPrefixesKey = "debug.stream.exclude_context_prefixes";
    public const string IncludeSummaryPrefixesKey = "debug.stream.include_summary_prefixes";
    public const string ExcludeSummaryPrefixesKey = "debug.stream.exclude_summary_prefixes";

    public static readonly IReadOnlyList<string> AllKeys = new[]
    {
        EnabledKey,
        MinSeverityKey,
        ContextRegexKey,
        IncludeContextPrefixesKey,
        ExcludeContextPrefixesKey,
        IncludeSummaryPrefixesKey,
        ExcludeSummaryPrefixesKey
    };
}
