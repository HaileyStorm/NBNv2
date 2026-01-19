using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Nbn.Tools.Workbench.Services;

public sealed class LocalServiceRunner
{
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

        var process = new Process { StartInfo = startInfo, EnableRaisingEvents = true };
        if (!process.Start())
        {
            return new ServiceStartResult(false, "Failed to start process.");
        }

        if (waitForExit)
        {
            await process.WaitForExitAsync().ConfigureAwait(false);
            return new ServiceStartResult(true, "Completed.");
        }

        lock (_gate)
        {
            _process = process;
        }

        WorkbenchProcessRegistry.Default.Record(process, label ?? startInfo.FileName ?? "process");
        return new ServiceStartResult(true, $"Running (pid {process.Id}).");
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
            process.Kill(true);
        }
        catch
        {
        }

        _process = null;
        WorkbenchProcessRegistry.Default.Remove(process.Id);
        return Task.FromResult("Stopped.");
    }
}

public sealed record ServiceStartResult(bool Success, string Message);
