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
        if (process.HasExited)
        {
            try
            {
                process.WaitForExit(StartupExitDrainMs);
            }
            catch
            {
            }

            var failureMessage = BuildStartupFailureMessage(process, logFiles);
            WorkbenchLog.Warn($"Local launch exited during startup: {processLabel} (pid {process.Id}). {failureMessage}");
            return new ServiceStartResult(false, failureMessage);
        }

        lock (_gate)
        {
            _process = process;
        }

        WorkbenchProcessRegistry.Default.Record(process, processLabel);
        var message = logFiles is null
            ? $"Running (pid {process.Id})."
            : $"Running (pid {process.Id}). Logs: {logFiles.StdoutPath}";
        return new ServiceStartResult(true, message);
    }

    private static string BuildStartupFailureMessage(Process process, ProcessLogFiles? logFiles)
    {
        var message = $"Process exited during startup (code {process.ExitCode}).";
        var stderrTail = TryReadLastLogLine(logFiles?.StderrPath);
        if (!string.IsNullOrWhiteSpace(stderrTail))
        {
            message = $"{message} {stderrTail}";
        }

        return message;
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
