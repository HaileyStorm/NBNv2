using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nbn.Tools.Workbench.Services;

public interface ILocalProjectLaunchPreparer
{
    Task<LocalProjectLaunchPreparation> PrepareAsync(string projectPath, string exeName, string runtimeArgs, string label);
}

public sealed class LocalProjectLaunchPreparer : ILocalProjectLaunchPreparer
{
    private readonly Func<ProcessStartInfo, Task<LocalProjectBuildResult>> _buildRunner;
    private readonly Func<string, string, string?> _executableResolver;

    public LocalProjectLaunchPreparer(
        Func<ProcessStartInfo, Task<LocalProjectBuildResult>>? buildRunner = null,
        Func<string, string, string?>? executableResolver = null)
    {
        _buildRunner = buildRunner ?? RunBuildAsync;
        _executableResolver = executableResolver ?? ResolveExecutable;
    }

    public async Task<LocalProjectLaunchPreparation> PrepareAsync(
        string projectPath,
        string exeName,
        string runtimeArgs,
        string label)
    {
        var projectFile = ResolveProjectFile(projectPath, exeName);
        if (string.IsNullOrWhiteSpace(projectFile))
        {
            return new LocalProjectLaunchPreparation(false, null, $"{label} project file not found.");
        }

        var buildResult = await _buildRunner(BuildBuildStartInfo(projectFile)).ConfigureAwait(false);
        if (!buildResult.Success)
        {
            WorkbenchLog.Warn($"Local launch build failed: {label}. {buildResult.Message}");
            return new LocalProjectLaunchPreparation(false, null, buildResult.Message);
        }

        var exePath = _executableResolver(projectPath, exeName);
        if (!string.IsNullOrWhiteSpace(exePath) && File.Exists(exePath))
        {
            return new LocalProjectLaunchPreparation(
                true,
                new ProcessStartInfo
                {
                    FileName = exePath,
                    Arguments = runtimeArgs,
                    UseShellExecute = false,
                    CreateNoWindow = true
                },
                "Prepared.");
        }

        return new LocalProjectLaunchPreparation(
            true,
            BuildDotnetRunStartInfo(projectPath, runtimeArgs),
            "Prepared.");
    }

    internal static ProcessStartInfo BuildBuildStartInfo(string projectFile)
    {
        return new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = $"build \"{projectFile}\" -c Release --disable-build-servers",
            WorkingDirectory = Path.GetDirectoryName(projectFile) ?? Environment.CurrentDirectory,
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8
        };
    }

    internal static ProcessStartInfo BuildDotnetRunStartInfo(string projectPath, string runtimeArgs)
    {
        return new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = $"run --project \"{projectPath}\" -c Release --no-build -- {runtimeArgs}",
            WorkingDirectory = projectPath,
            UseShellExecute = false,
            CreateNoWindow = true
        };
    }

    internal static string? ResolveProjectFile(string projectPath, string exeName)
    {
        if (string.IsNullOrWhiteSpace(projectPath) || !Directory.Exists(projectPath))
        {
            return null;
        }

        var exactMatch = Path.Combine(projectPath, exeName + ".csproj");
        if (File.Exists(exactMatch))
        {
            return exactMatch;
        }

        var projects = Directory.GetFiles(projectPath, "*.csproj", SearchOption.TopDirectoryOnly)
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        return projects.Length == 1 ? projects[0] : null;
    }

    internal static string? ResolveExecutable(string projectPath, string exeName)
    {
        if (string.IsNullOrWhiteSpace(projectPath))
        {
            return null;
        }

        var output = Path.Combine(projectPath, "bin", "Release", "net8.0");
        return OperatingSystem.IsWindows()
            ? Path.Combine(output, exeName + ".exe")
            : Path.Combine(output, exeName);
    }

    private static async Task<LocalProjectBuildResult> RunBuildAsync(ProcessStartInfo startInfo)
    {
        var stdout = new StringBuilder();
        var stderr = new StringBuilder();
        using var process = new Process { StartInfo = startInfo, EnableRaisingEvents = true };
        process.OutputDataReceived += (_, args) =>
        {
            if (!string.IsNullOrWhiteSpace(args.Data))
            {
                stdout.AppendLine(args.Data);
            }
        };
        process.ErrorDataReceived += (_, args) =>
        {
            if (!string.IsNullOrWhiteSpace(args.Data))
            {
                stderr.AppendLine(args.Data);
            }
        };

        try
        {
            if (!process.Start())
            {
                return new LocalProjectBuildResult(false, "Build failed to start.");
            }
        }
        catch (Exception ex)
        {
            return new LocalProjectBuildResult(false, $"Build failed to start. {ex.Message}");
        }

        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        await process.WaitForExitAsync().ConfigureAwait(false);

        if (process.ExitCode == 0)
        {
            return new LocalProjectBuildResult(true, "Build succeeded.");
        }

        var detail = TryGetLastNonEmptyLine(stderr.ToString()) ?? TryGetLastNonEmptyLine(stdout.ToString());
        var message = detail is null
            ? $"Build failed (code {process.ExitCode})."
            : $"Build failed (code {process.ExitCode}). {detail}";
        return new LocalProjectBuildResult(false, message);
    }

    private static string? TryGetLastNonEmptyLine(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return null;
        }

        var lines = text.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return lines.Length == 0 ? null : lines[^1];
    }
}

public sealed record LocalProjectLaunchPreparation(bool Success, ProcessStartInfo? StartInfo, string Message);

public sealed record LocalProjectBuildResult(bool Success, string Message);
