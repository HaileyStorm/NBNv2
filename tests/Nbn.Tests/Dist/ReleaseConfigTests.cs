using System.Text.Json;
using System.Xml.Linq;

namespace Nbn.Tests.Dist;

public class ReleaseConfigTests
{
    [Fact]
    public void ReleaseConfig_CoversEveryExecutableProject()
    {
        var repoRoot = FindRepoRoot();
        var configProjects = ReadConfiguredProjects(repoRoot);
        var executableProjects = Directory
            .EnumerateFiles(Path.Combine(repoRoot, "src"), "*.csproj", SearchOption.AllDirectories)
            .Concat(Directory.EnumerateFiles(Path.Combine(repoRoot, "tools"), "*.csproj", SearchOption.AllDirectories))
            .Where(IsExecutableProject)
            .Select(path => Path.GetRelativePath(repoRoot, path).Replace('\\', '/'))
            .OrderBy(path => path, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(executableProjects, configProjects);
    }

    [Fact]
    public void ReleaseConfig_DeclaresExpectedSuiteAndWorkerAliases()
    {
        var repoRoot = FindRepoRoot();
        using var document = JsonDocument.Parse(File.ReadAllText(Path.Combine(repoRoot, "tools", "dist", "release-config.json")));
        var applications = document.RootElement.GetProperty("applications").EnumerateArray().ToArray();

        var suiteAliases = applications
            .Where(app => app.GetProperty("bundles").EnumerateArray().Any(bundle => bundle.GetString() == "suite"))
            .Select(app => app.GetProperty("alias").GetString())
            .OrderBy(alias => alias, StringComparer.Ordinal)
            .ToArray();
        var workerAliases = applications
            .Where(app => app.GetProperty("bundles").EnumerateArray().Any(bundle => bundle.GetString() == "worker"))
            .Select(app => app.GetProperty("alias").GetString())
            .OrderBy(alias => alias, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
            new[]
            {
                "nbn-brainhost",
                "nbn-evolution-sim",
                "nbn-hivemind",
                "nbn-io",
                "nbn-observability",
                "nbn-perf-probe",
                "nbn-ppo",
                "nbn-regionhost",
                "nbn-repro",
                "nbn-settings",
                "nbn-speciation",
                "nbn-workbench",
                "nbn-worker"
            },
            suiteAliases);
        Assert.Equal(new[] { "nbn-worker" }, workerAliases);
    }

    [Fact]
    public void ReleaseVersionFile_UsesFixedMajorLine()
    {
        var repoRoot = FindRepoRoot();
        using var document = JsonDocument.Parse(File.ReadAllText(Path.Combine(repoRoot, "release", "version.json")));
        Assert.Equal(2, document.RootElement.GetProperty("major").GetInt32());
        Assert.True(document.RootElement.GetProperty("minor").GetInt32() >= 0);
    }

    private static string[] ReadConfiguredProjects(string repoRoot)
    {
        using var document = JsonDocument.Parse(File.ReadAllText(Path.Combine(repoRoot, "tools", "dist", "release-config.json")));
        return document.RootElement.GetProperty("applications")
            .EnumerateArray()
            .Select(app => app.GetProperty("project").GetString())
            .Where(path => !string.IsNullOrWhiteSpace(path))
            .Select(path => path!.Replace('\\', '/'))
            .OrderBy(path => path, StringComparer.Ordinal)
            .ToArray();
    }

    private static bool IsExecutableProject(string projectPath)
    {
        var document = XDocument.Load(projectPath);
        return document
            .Descendants()
            .Any(element => element.Name.LocalName == "OutputType"
                            && (string.Equals(element.Value.Trim(), "Exe", StringComparison.Ordinal)
                                || string.Equals(element.Value.Trim(), "WinExe", StringComparison.Ordinal)));
    }

    private static string FindRepoRoot()
    {
        var current = new DirectoryInfo(AppContext.BaseDirectory);
        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "NBNv2.sln"))
                && File.Exists(Path.Combine(current.FullName, "Directory.Build.props")))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        throw new InvalidOperationException("Unable to locate repo root.");
    }
}
