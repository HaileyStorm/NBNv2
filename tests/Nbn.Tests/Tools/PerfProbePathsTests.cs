using Nbn.Tools.PerfProbe;

namespace Nbn.Tests.Tools;

public sealed class PerfProbePathsTests
{
    [Fact]
    public void ResolveOutputDirectory_NestsDefaultReportsUnderPerfProbeRoot()
    {
        var currentDirectory = Path.Combine(Path.DirectorySeparatorChar.ToString(), "repo-root");
        var expectedCurrentDirectory = Path.GetFullPath(currentDirectory);
        var timestamp = new DateTimeOffset(2026, 03, 21, 14, 05, 06, TimeSpan.Zero);

        var outputDirectory = PerfProbePaths.ResolveOutputDirectory(
            configuredOutputDirectory: null,
            currentDirectory,
            timestamp);

        Assert.Equal(
            Path.Combine(expectedCurrentDirectory, "perf-probe", "perf-probe-20260321-140506"),
            outputDirectory);
    }

    [Fact]
    public void ResolveOutputDirectory_ResolvesExplicitRelativeOutputPathAgainstCurrentDirectory()
    {
        var currentDirectory = Path.Combine(Path.DirectorySeparatorChar.ToString(), "repo-root");
        var expectedCurrentDirectory = Path.GetFullPath(currentDirectory);
        var timestamp = new DateTimeOffset(2026, 03, 21, 14, 05, 06, TimeSpan.Zero);

        var outputDirectory = PerfProbePaths.ResolveOutputDirectory(
            configuredOutputDirectory: Path.Combine("custom", "reports"),
            currentDirectory,
            timestamp);

        Assert.Equal(
            Path.Combine(expectedCurrentDirectory, "custom", "reports"),
            outputDirectory);
    }
}
