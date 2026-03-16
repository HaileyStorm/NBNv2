using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Nbn.Tools.Workbench.Services;
using Xunit;

namespace Nbn.Tests.Workbench;

public class LocalProjectLaunchPreparerTests
{
    [Fact]
    public async Task PrepareAsync_UsesBuiltExecutable_WhenAvailable()
    {
        var projectRoot = CreateProjectLayout("Nbn.Runtime.Speciation", createExecutable: true);
        ProcessStartInfo? capturedBuildStartInfo = null;

        try
        {
            var preparer = new LocalProjectLaunchPreparer(
                buildRunner: startInfo =>
                {
                    capturedBuildStartInfo = startInfo;
                    return Task.FromResult(new LocalProjectBuildResult(true, "Build succeeded."));
                },
                isLocalSourceCheckout: () => true);

            var result = await preparer.PrepareAsync(
                projectRoot,
                "Nbn.Runtime.Speciation",
                "--bind-host 127.0.0.1 --port 12080",
                "Speciation");

            Assert.True(result.Success);
            Assert.NotNull(result.StartInfo);
            Assert.Equal(ExpectedExecutablePath(projectRoot, "Nbn.Runtime.Speciation"), result.StartInfo!.FileName);
            Assert.Equal("--bind-host 127.0.0.1 --port 12080", result.StartInfo.Arguments);
            Assert.NotNull(capturedBuildStartInfo);
            Assert.Equal("dotnet", capturedBuildStartInfo!.FileName);
            Assert.Contains("build", capturedBuildStartInfo.Arguments, StringComparison.Ordinal);
            Assert.Contains("--disable-build-servers", capturedBuildStartInfo.Arguments, StringComparison.Ordinal);
        }
        finally
        {
            DeleteDirectory(projectRoot);
        }
    }

    [Fact]
    public async Task PrepareAsync_FallsBackToDotnetRun_WhenExecutableMissing()
    {
        var projectRoot = CreateProjectLayout("Nbn.Tools.EvolutionSim", createExecutable: false);

        try
        {
            var preparer = new LocalProjectLaunchPreparer(
                buildRunner: _ => Task.FromResult(new LocalProjectBuildResult(true, "Build succeeded.")),
                isLocalSourceCheckout: () => true);

            var result = await preparer.PrepareAsync(
                projectRoot,
                "Nbn.Tools.EvolutionSim",
                "--io-port 12050 --port 12074",
                "EvolutionSim");

            Assert.True(result.Success);
            Assert.NotNull(result.StartInfo);
            Assert.Equal("dotnet", result.StartInfo!.FileName);
            Assert.Contains($"run --project \"{projectRoot}\" -c Release --no-build -- --io-port 12050 --port 12074", result.StartInfo.Arguments, StringComparison.Ordinal);
        }
        finally
        {
            DeleteDirectory(projectRoot);
        }
    }

    [Fact]
    public async Task PrepareAsync_ReturnsFailure_WhenBuildFails()
    {
        var projectRoot = CreateProjectLayout("Nbn.Runtime.Speciation", createExecutable: true);

        try
        {
            var preparer = new LocalProjectLaunchPreparer(
                buildRunner: _ => Task.FromResult(new LocalProjectBuildResult(false, "Build failed (code 1). CS1000")),
                isLocalSourceCheckout: () => true);

            var result = await preparer.PrepareAsync(
                projectRoot,
                "Nbn.Runtime.Speciation",
                "--bind-host 127.0.0.1 --port 12080",
                "Speciation");

            Assert.False(result.Success);
            Assert.Null(result.StartInfo);
            Assert.Equal("Build failed (code 1). CS1000", result.Message);
        }
        finally
        {
            DeleteDirectory(projectRoot);
        }
    }

    [Fact]
    public async Task PrepareAsync_InstalledMode_UsesManifestCommand_WhenAvailable()
    {
        var installRoot = CreateInstalledLayout();
        var workbenchBaseDir = Path.Combine(installRoot, "apps", "workbench");
        Directory.CreateDirectory(workbenchBaseDir);
        var commandPath = CreateInstalledCommandFile(
            Path.Combine(installRoot, "services", "worker"),
            OperatingSystem.IsWindows() ? "nbn-worker.exe" : "nbn-worker");
        var relativeCommandPath = Path.GetRelativePath(installRoot, commandPath);
        await File.WriteAllTextAsync(
            Path.Combine(installRoot, "runtime-manifest.json"),
            $$"""
              {
                "commands": {
                  "nbn-worker": {
                    "path": "{{relativeCommandPath.Replace('\\', '/')}}"
                  }
                }
              }
              """);

        try
        {
            var buildCalled = false;
            var preparer = new LocalProjectLaunchPreparer(
                buildRunner: _ =>
                {
                    buildCalled = true;
                    return Task.FromResult(new LocalProjectBuildResult(true, "Build succeeded."));
                },
                isLocalSourceCheckout: () => false,
                baseDirectoryProvider: () => workbenchBaseDir);

            var result = await preparer.PrepareAsync(
                projectPath: null,
                "Nbn.Runtime.WorkerNode",
                "--port 12041",
                "WorkerNode");

            Assert.True(result.Success);
            Assert.NotNull(result.StartInfo);
            Assert.False(buildCalled);
            Assert.Equal(commandPath, result.StartInfo!.FileName);
            Assert.Equal("--port 12041", result.StartInfo.Arguments);
        }
        finally
        {
            DeleteDirectory(installRoot);
        }
    }

    [Fact]
    public async Task PrepareAsync_InstalledMode_FallsBackToPath_WhenManifestMissing()
    {
        var installRoot = CreateInstalledLayout();
        var workbenchBaseDir = Path.Combine(installRoot, "apps", "workbench");
        Directory.CreateDirectory(workbenchBaseDir);
        var pathDirectory = Path.Combine(installRoot, "bin");
        Directory.CreateDirectory(pathDirectory);
        var commandFileName = OperatingSystem.IsWindows() ? "nbn-evolution-sim.exe" : "nbn-evolution-sim";
        var commandPath = CreateInstalledCommandFile(pathDirectory, commandFileName);

        try
        {
            var buildCalled = false;
            var preparer = new LocalProjectLaunchPreparer(
                buildRunner: _ =>
                {
                    buildCalled = true;
                    return Task.FromResult(new LocalProjectBuildResult(true, "Build succeeded."));
                },
                isLocalSourceCheckout: () => false,
                baseDirectoryProvider: () => workbenchBaseDir,
                pathProvider: () => pathDirectory,
                pathExtProvider: () => OperatingSystem.IsWindows() ? ".EXE;.CMD;.BAT" : string.Empty);

            var result = await preparer.PrepareAsync(
                projectPath: null,
                "Nbn.Tools.EvolutionSim",
                "--io-port 12050 --port 12074",
                "EvolutionSim");

            Assert.True(result.Success);
            Assert.NotNull(result.StartInfo);
            Assert.False(buildCalled);
            Assert.Equal(commandPath, result.StartInfo!.FileName, ignoreCase: OperatingSystem.IsWindows());
            Assert.Equal("--io-port 12050 --port 12074", result.StartInfo.Arguments);
        }
        finally
        {
            DeleteDirectory(installRoot);
        }
    }

    [Fact]
    public async Task PrepareAsync_InstalledMode_ReturnsFailure_WhenInstalledCommandMissing()
    {
        var workbenchBaseDir = CreateInstalledLayout();
        Directory.CreateDirectory(Path.Combine(workbenchBaseDir, "apps", "workbench"));

        try
        {
            var preparer = new LocalProjectLaunchPreparer(
                isLocalSourceCheckout: () => false,
                baseDirectoryProvider: () => Path.Combine(workbenchBaseDir, "apps", "workbench"),
                pathProvider: () => string.Empty);

            var result = await preparer.PrepareAsync(
                projectPath: null,
                "Nbn.Runtime.SettingsMonitor",
                "--port 12010",
                "SettingsMonitor");

            Assert.False(result.Success);
            Assert.Null(result.StartInfo);
            Assert.Equal("SettingsMonitor installed command not found (checked runtime-manifest.json and PATH).", result.Message);
        }
        finally
        {
            DeleteDirectory(workbenchBaseDir);
        }
    }

    private static string CreateProjectLayout(string exeName, bool createExecutable)
    {
        var root = Path.Combine(Path.GetTempPath(), "nbn-launch-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        File.WriteAllText(Path.Combine(root, exeName + ".csproj"), "<Project Sdk=\"Microsoft.NET.Sdk\"></Project>");
        if (createExecutable)
        {
            var exePath = ExpectedExecutablePath(root, exeName);
            Directory.CreateDirectory(Path.GetDirectoryName(exePath)!);
            File.WriteAllText(exePath, string.Empty);
        }

        return root;
    }

    private static string CreateInstalledLayout()
    {
        var root = Path.Combine(Path.GetTempPath(), "nbn-installed-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        return root;
    }

    private static string CreateInstalledCommandFile(string directory, string fileName)
    {
        var path = Path.Combine(directory, fileName);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, string.Empty);
        return path;
    }

    private static string ExpectedExecutablePath(string projectRoot, string exeName)
    {
        var basePath = Path.Combine(projectRoot, "bin", "Release", "net8.0");
        return OperatingSystem.IsWindows()
            ? Path.Combine(basePath, exeName + ".exe")
            : Path.Combine(basePath, exeName);
    }

    private static void DeleteDirectory(string path)
    {
        if (!Directory.Exists(path))
        {
            return;
        }

        try
        {
            Directory.Delete(path, recursive: true);
        }
        catch
        {
        }
    }
}
