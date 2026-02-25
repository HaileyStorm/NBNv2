using Nbn.Proto;

namespace Nbn.Tools.Workbench.Models;

public sealed record DebugSubscriptionFilter(
    bool StreamEnabled,
    Severity MinSeverity,
    string ContextRegex,
    IReadOnlyList<string> IncludeContextPrefixes,
    IReadOnlyList<string> ExcludeContextPrefixes,
    IReadOnlyList<string> IncludeSummaryPrefixes,
    IReadOnlyList<string> ExcludeSummaryPrefixes)
{
    public static DebugSubscriptionFilter Default { get; } = new(
        StreamEnabled: false,
        MinSeverity: Severity.SevInfo,
        ContextRegex: string.Empty,
        IncludeContextPrefixes: Array.Empty<string>(),
        ExcludeContextPrefixes: Array.Empty<string>(),
        IncludeSummaryPrefixes: Array.Empty<string>(),
        ExcludeSummaryPrefixes: Array.Empty<string>());
}
