using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using System.Text;

namespace Nbn.Tools.Workbench.Services;

public sealed class LocalServiceRunner
{
    private const int StartupProbeDelayMs = 400;
    private const int StartupExitDrainMs = 250;
    private readonly object _gate = new();
    private Process? _process;

    public bool IsRunning => _process is { HasExited: false };

    public async Task<ServiceStartResult> StartAsync(ProcessStartInfo startInfo, bool waitForExit, string? label = null)
    {
        lock (_gate)
        {
            if (IsRunning)
            {
                return new ServiceStartResult(false, "Already running.");
            }
        }

        var processLabel = label ?? startInfo.FileName ?? "process";
        var logFiles = WorkbenchLog.GetProcessLogFiles(processLabel);
        if (logFiles is not null)
        {
            startInfo.RedirectStandardOutput = true;
            startInfo.RedirectStandardError = true;
            startInfo.StandardOutputEncoding = Encoding.UTF8;
            startInfo.StandardErrorEncoding = Encoding.UTF8;
        }

        var process = new Process { StartInfo = startInfo, EnableRaisingEvents = true };
        if (!process.Start())
        {
            return new ServiceStartResult(false, "Failed to start process.");
        }

        if (logFiles is not null)
        {
            process.OutputDataReceived += (_, args) => WorkbenchLog.AppendLine(logFiles.StdoutPath, args.Data);
            process.ErrorDataReceived += (_, args) => WorkbenchLog.AppendLine(logFiles.StderrPath, args.Data);
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            WorkbenchLog.Info($"Local launch started: {processLabel} (pid {process.Id})");
        }

        if (waitForExit)
        {
            await process.WaitForExitAsync().ConfigureAwait(false);
            return new ServiceStartResult(true, "Completed.");
        }

        await Task.Delay(StartupProbeDelayMs).ConfigureAwait(false);
        if (TryBuildStartupExitResult(process, processLabel, logFiles, removeFromRegistry: false, out var startupFailure))
        {
            return startupFailure;
        }

        WorkbenchProcessRegistry.Default.Record(process, processLabel);
        if (TryBuildStartupExitResult(process, processLabel, logFiles, removeFromRegistry: true, out startupFailure))
        {
            return startupFailure;
        }

        lock (_gate)
        {
            _process = process;
        }

        var message = logFiles is null
            ? $"Running (pid {process.Id})."
            : $"Running (pid {process.Id}). Logs: {logFiles.StdoutPath}";
        return new ServiceStartResult(true, message);
    }

    private static bool TryBuildStartupExitResult(
        Process process,
        string processLabel,
        ProcessLogFiles? logFiles,
        bool removeFromRegistry,
        out ServiceStartResult result)
    {
        if (!HasProcessExited(process))
        {
            result = new ServiceStartResult(false, string.Empty);
            return false;
        }

        try
        {
            process.WaitForExit(StartupExitDrainMs);
        }
        catch
        {
        }

        if (removeFromRegistry)
        {
            WorkbenchProcessRegistry.Default.Remove(process.Id);
        }

        var failureMessage = BuildStartupFailureMessage(process, logFiles);
        WorkbenchLog.Warn($"Local launch exited during startup: {processLabel} (pid {process.Id}). {failureMessage}");
        result = new ServiceStartResult(false, failureMessage);
        return true;
    }

    private static string BuildStartupFailureMessage(Process process, ProcessLogFiles? logFiles)
    {
        var exitCode = TryGetExitCode(process);
        var message = exitCode.HasValue
            ? $"Process exited during startup (code {exitCode.Value})."
            : "Process exited during startup.";
        var stderrTail = TryReadLastLogLine(logFiles?.StderrPath);
        if (!string.IsNullOrWhiteSpace(stderrTail))
        {
            message = $"{message} {stderrTail}";
        }

        return message;
    }

    private static bool HasProcessExited(Process process)
    {
        try
        {
            return process.HasExited;
        }
        catch
        {
            return true;
        }
    }

    private static int? TryGetExitCode(Process process)
    {
        try
        {
            return process.ExitCode;
        }
        catch
        {
            return null;
        }
    }

    private static string? TryReadLastLogLine(string? path)
    {
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
        {
            return null;
        }

        try
        {
            var lines = File.ReadAllLines(path);
            for (var i = lines.Length - 1; i >= 0; i--)
            {
                if (!string.IsNullOrWhiteSpace(lines[i]))
                {
                    return lines[i].Trim();
                }
            }
        }
        catch
        {
        }

        return null;
    }

    public Task<string> StopAsync()
    {
        Process? process;
        lock (_gate)
        {
            process = _process;
        }

        if (process is null || process.HasExited)
        {
            _process = null;
            return Task.FromResult("Not running.");
        }

        try
        {
            if (!process.HasExited)
            {
                process.CloseMainWindow();
            }
        }
        catch
        {
        }

        try
        {
            if (!process.HasExited)
            {
                process.Kill(true);
            }
        }
        catch
        {
        }

        var exited = false;
        try
        {
            exited = process.WaitForExit(2000);
        }
        catch
        {
        }

        if (!exited && !process.HasExited)
        {
            WorkbenchLog.Warn($"Local launch stop requested but process still running: {process.ProcessName} (pid {process.Id})");
            return Task.FromResult("Stop requested; process still running.");
        }

        _process = null;
        WorkbenchProcessRegistry.Default.Remove(process.Id);
        WorkbenchLog.Info($"Local launch stopped: {process.ProcessName} (pid {process.Id})");
        return Task.FromResult("Stopped.");
    }
}

public sealed record ServiceStartResult(bool Success, string Message);
