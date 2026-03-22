using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Nbn.Tools.Workbench.Services;

public interface ILocalProjectLaunchPreparer
{
    Task<LocalProjectLaunchPreparation> PrepareAsync(string? projectPath, string exeName, string runtimeArgs, string label);
}

public sealed class LocalProjectLaunchPreparer : ILocalProjectLaunchPreparer
{
    private readonly Func<ProcessStartInfo, Task<LocalProjectBuildResult>> _buildRunner;
    private readonly Func<string, string, string?> _executableResolver;
    private readonly Func<bool> _isLocalSourceCheckout;
    private readonly Func<string> _baseDirectoryProvider;
    private readonly Func<string, bool> _fileExists;
    private readonly Func<string, string> _readAllText;
    private readonly Func<string?> _pathProvider;
    private readonly Func<string?> _pathExtProvider;

    public LocalProjectLaunchPreparer(
        Func<ProcessStartInfo, Task<LocalProjectBuildResult>>? buildRunner = null,
        Func<string, string, string?>? executableResolver = null,
        Func<bool>? isLocalSourceCheckout = null,
        Func<string>? baseDirectoryProvider = null,
        Func<string, bool>? fileExists = null,
        Func<string, string>? readAllText = null,
        Func<string?>? pathProvider = null,
        Func<string?>? pathExtProvider = null)
    {
        _buildRunner = buildRunner ?? RunBuildAsync;
        _executableResolver = executableResolver ?? ResolveExecutable;
        _isLocalSourceCheckout = isLocalSourceCheckout ?? (() => RepoLocator.IsLocalSourceCheckout());
        _baseDirectoryProvider = baseDirectoryProvider ?? (() => AppContext.BaseDirectory);
        _fileExists = fileExists ?? File.Exists;
        _readAllText = readAllText ?? File.ReadAllText;
        _pathProvider = pathProvider ?? (() => Environment.GetEnvironmentVariable("PATH"));
        _pathExtProvider = pathExtProvider ?? (() => Environment.GetEnvironmentVariable("PATHEXT"));
    }

    public async Task<LocalProjectLaunchPreparation> PrepareAsync(
        string? projectPath,
        string exeName,
        string runtimeArgs,
        string label)
    {
        if (_isLocalSourceCheckout())
        {
            return await PrepareLocalAsync(projectPath, exeName, runtimeArgs, label).ConfigureAwait(false);
        }

        return PrepareInstalled(exeName, runtimeArgs, label);
    }

    private async Task<LocalProjectLaunchPreparation> PrepareLocalAsync(
        string? projectPath,
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

        var exePath = _executableResolver(projectPath ?? string.Empty, exeName);
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

    private LocalProjectLaunchPreparation PrepareInstalled(string exeName, string runtimeArgs, string label)
    {
        var alias = ResolveInstalledCommandAlias(exeName);
        if (TryResolveInstalledCommand(alias, out var commandPath))
        {
            return new LocalProjectLaunchPreparation(true, BuildInstalledStartInfo(commandPath, runtimeArgs), "Prepared.");
        }

        if (!string.Equals(alias, exeName, StringComparison.OrdinalIgnoreCase)
            && TryResolveInstalledCommand(exeName, out commandPath))
        {
            return new LocalProjectLaunchPreparation(true, BuildInstalledStartInfo(commandPath, runtimeArgs), "Prepared.");
        }

        return new LocalProjectLaunchPreparation(
            false,
            null,
            $"{label} installed command not found (checked runtime-manifest.json and PATH).");
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

    internal static ProcessStartInfo BuildDotnetRunStartInfo(string? projectPath, string runtimeArgs)
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

    internal static string? ResolveProjectFile(string? projectPath, string exeName)
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

    internal static string? ResolveExecutable(string? projectPath, string exeName)
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

    internal static string ResolveInstalledCommandAlias(string exeName)
    {
        return exeName switch
        {
            "Nbn.Runtime.SettingsMonitor" => "nbn-settings",
            "Nbn.Runtime.HiveMind" => "nbn-hivemind",
            "Nbn.Runtime.IO" => "nbn-io",
            "Nbn.Runtime.Reproduction" => "nbn-repro",
            "Nbn.Runtime.Speciation" => "nbn-speciation",
            "Nbn.Runtime.Observability" => "nbn-observability",
            "Nbn.Runtime.WorkerNode" => "nbn-worker",
            "Nbn.Runtime.BrainHost" => "nbn-brainhost",
            "Nbn.Runtime.RegionHost" => "nbn-regionhost",
            "Nbn.Tools.EvolutionSim" => "nbn-evolution-sim",
            "Nbn.Tools.PerfProbe" => "nbn-perf-probe",
            _ => exeName
        };
    }

    internal ProcessStartInfo BuildInstalledStartInfo(string commandPath, string runtimeArgs)
    {
        var workingDirectory = Path.GetDirectoryName(commandPath) ?? _baseDirectoryProvider();
        if (OperatingSystem.IsWindows()
            && (commandPath.EndsWith(".cmd", StringComparison.OrdinalIgnoreCase)
                || commandPath.EndsWith(".bat", StringComparison.OrdinalIgnoreCase)))
        {
            return new ProcessStartInfo
            {
                FileName = Environment.GetEnvironmentVariable("ComSpec") ?? "cmd.exe",
                Arguments = BuildCommandShellArguments(commandPath, runtimeArgs),
                WorkingDirectory = workingDirectory,
                UseShellExecute = false,
                CreateNoWindow = true
            };
        }

        return new ProcessStartInfo
        {
            FileName = commandPath,
            Arguments = runtimeArgs,
            WorkingDirectory = workingDirectory,
            UseShellExecute = false,
            CreateNoWindow = true
        };
    }

    internal static string BuildCommandShellArguments(string commandPath, string runtimeArgs)
    {
        var quotedCommand = $"\"{commandPath}\"";
        return string.IsNullOrWhiteSpace(runtimeArgs)
            ? $"/c {quotedCommand}"
            : $"/c {quotedCommand} {runtimeArgs}";
    }

    private bool TryResolveInstalledCommand(string commandName, out string commandPath)
    {
        commandPath = string.Empty;

        if (TryResolveInstalledCommandFromManifest(commandName, out commandPath))
        {
            return true;
        }

        return TryResolveInstalledCommandFromPath(commandName, out commandPath);
    }

    private bool TryResolveInstalledCommandFromManifest(string commandName, out string commandPath)
    {
        commandPath = string.Empty;
        var manifest = RepoLocator.FindRuntimeManifest(_baseDirectoryProvider());
        if (manifest is null)
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(_readAllText(manifest.FullName));
            if (!document.RootElement.TryGetProperty("commands", out var commands)
                || commands.ValueKind != JsonValueKind.Object
                || !commands.TryGetProperty(commandName, out var commandElement))
            {
                return false;
            }

            string? rawPath = commandElement.ValueKind switch
            {
                JsonValueKind.String => commandElement.GetString(),
                JsonValueKind.Object when commandElement.TryGetProperty("path", out var pathElement)
                    && pathElement.ValueKind == JsonValueKind.String => pathElement.GetString(),
                _ => null
            };

            if (string.IsNullOrWhiteSpace(rawPath))
            {
                return false;
            }

            var resolvedPath = Path.IsPathRooted(rawPath)
                ? rawPath
                : Path.GetFullPath(Path.Combine(manifest.DirectoryName ?? _baseDirectoryProvider(), rawPath));
            if (!_fileExists(resolvedPath))
            {
                return false;
            }

            commandPath = resolvedPath;
            return true;
        }
        catch
        {
            return false;
        }
    }

    private bool TryResolveInstalledCommandFromPath(string commandName, out string commandPath)
    {
        commandPath = string.Empty;
        var path = _pathProvider();
        if (string.IsNullOrWhiteSpace(path))
        {
            return false;
        }

        foreach (var directory in path.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            foreach (var candidate in ExpandCommandCandidates(commandName))
            {
                var resolved = Path.Combine(directory, candidate);
                if (_fileExists(resolved))
                {
                    commandPath = resolved;
                    return true;
                }
            }
        }

        return false;
    }

    private IEnumerable<string> ExpandCommandCandidates(string commandName)
    {
        yield return commandName;

        if (!OperatingSystem.IsWindows() || Path.HasExtension(commandName))
        {
            yield break;
        }

        var pathExt = _pathExtProvider();
        var extensions = string.IsNullOrWhiteSpace(pathExt)
            ? [".exe", ".cmd", ".bat", ".com"]
            : pathExt.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        foreach (var extension in extensions)
        {
            yield return commandName + extension;
        }
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
