namespace Nbn.Tools.PerfProbe;

internal static class PerfProbePaths
{
    public static string ResolveOutputDirectory(
        string? configuredOutputDirectory,
        string currentDirectory,
        DateTimeOffset timestampUtc)
    {
        return string.IsNullOrWhiteSpace(configuredOutputDirectory)
            ? Path.GetFullPath(
                Path.Combine("perf-probe", $"perf-probe-{timestampUtc:yyyyMMdd-HHmmss}"),
                currentDirectory)
            : Path.GetFullPath(configuredOutputDirectory, currentDirectory);
    }
}
