using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace Nbn.Tools.Workbench.Services;

public sealed class LocalDemoRunner
{
    private readonly object _gate = new();
    private Process? _process;

    public bool IsRunning => _process is { HasExited: false };

    public Task<DemoStartResult> StartAsync(DemoLaunchOptions options)
    {
        lock (_gate)
        {
            if (IsRunning)
            {
                return Task.FromResult(new DemoStartResult(false, "Demo already running."));
            }
        }

        var scriptPath = ResolveDemoScript();
        if (string.IsNullOrWhiteSpace(scriptPath))
        {
            return Task.FromResult(new DemoStartResult(false, "Demo script not found."));
        }

        var repoRoot = RepoLocator.FindRepoRoot()?.FullName
            ?? Path.GetFullPath(Path.Combine(Path.GetDirectoryName(scriptPath)!, "..", ".."));
        var args = BuildArgs(scriptPath, options);

        var startInfo = new ProcessStartInfo
        {
            FileName = "powershell",
            Arguments = args,
            WorkingDirectory = repoRoot,
            UseShellExecute = false,
            RedirectStandardInput = true,
            CreateNoWindow = true
        };

        var process = new Process { StartInfo = startInfo, EnableRaisingEvents = true };
        if (!process.Start())
        {
            return Task.FromResult(new DemoStartResult(false, "Failed to start demo process."));
        }

        lock (_gate)
        {
            _process = process;
        }

        return Task.FromResult(new DemoStartResult(true, $"Demo running (pid {process.Id})."));
    }

    public async Task<string> StopAsync()
    {
        Process? process;
        lock (_gate)
        {
            process = _process;
        }

        if (process is null || process.HasExited)
        {
            _process = null;
            return "Demo not running.";
        }

        try
        {
            await process.StandardInput.WriteLineAsync().ConfigureAwait(false);
            await process.StandardInput.FlushAsync().ConfigureAwait(false);
        }
        catch
        {
        }

        try
        {
            if (!process.WaitForExit(5000))
            {
                process.Kill(true);
            }
        }
        catch
        {
        }

        lock (_gate)
        {
            _process = null;
        }

        return "Demo stopped.";
    }

    private static string BuildArgs(string scriptPath, DemoLaunchOptions options)
    {
        return $"-NoProfile -ExecutionPolicy Bypass -File \"{scriptPath}\" "
             + $"-DemoRoot \"{options.DemoRoot}\" "
             + $"-BindHost \"{options.BindHost}\" "
             + $"-HiveMindPort {options.HiveMindPort} "
             + $"-BrainHostPort {options.BrainHostPort} "
             + $"-RegionHostPort {options.RegionHostPort} "
             + $"-IoPort {options.IoPort} "
             + $"-ObsPort {options.ObsPort} "
             + $"-SettingsPort {options.SettingsPort}";
    }

    private static string? ResolveDemoScript()
    {
        var repoRoot = RepoLocator.FindRepoRoot();
        if (repoRoot is not null)
        {
            var candidate = Path.Combine(repoRoot.FullName, "tools", "demo", "run_local_hivemind_demo.ps1");
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        var current = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 8 && current is not null; i++)
        {
            var candidate = Path.Combine(current.FullName, "tools", "demo", "run_local_hivemind_demo.ps1");
            if (File.Exists(candidate))
            {
                return candidate;
            }

            current = current.Parent;
        }

        return null;
    }

}

public sealed record DemoLaunchOptions(
    string DemoRoot,
    string BindHost,
    int HiveMindPort,
    int BrainHostPort,
    int RegionHostPort,
    int IoPort,
    int ObsPort,
    int SettingsPort);

public sealed record DemoStartResult(bool Success, string Message);
