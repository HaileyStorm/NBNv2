using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace Nbn.Tools.Workbench.Services;

public sealed class WorkbenchProcessRegistry
{
    private static readonly Lazy<WorkbenchProcessRegistry> _default = new(() => new WorkbenchProcessRegistry(GetDefaultPath()));
    private readonly object _gate = new();
    private readonly string _path;
    private List<ProcessEntry> _entries = new();

    private WorkbenchProcessRegistry(string path)
    {
        _path = path;
        Load();
    }

    public static WorkbenchProcessRegistry Default => _default.Value;

    public void CleanupStale()
    {
        lock (_gate)
        {
            Load();
            var remaining = new List<ProcessEntry>();

            foreach (var entry in _entries)
            {
                if (!TryGetProcess(entry.Pid, out var process))
                {
                    continue;
                }

                if (!Matches(process, entry))
                {
                    remaining.Add(entry);
                    continue;
                }

                TryKill(process);
            }

            _entries = remaining;
            Save();
        }
    }

    public void Record(Process process, string label)
    {
        if (process is null)
        {
            return;
        }

        lock (_gate)
        {
            Load();
            var pid = SafeProcessId(process);
            if (!pid.HasValue)
            {
                return;
            }

            var processName = SafeProcessName(process) ?? string.Empty;
            var startUtc = SafeStartTimeUtc(process);
            var entry = new ProcessEntry(pid.Value, processName, startUtc?.Ticks ?? 0, label ?? string.Empty);
            _entries.RemoveAll(item => item.Pid == entry.Pid);
            _entries.Add(entry);
            Save();
        }
    }

    public void Remove(int pid)
    {
        lock (_gate)
        {
            Load();
            _entries.RemoveAll(item => item.Pid == pid);
            Save();
        }
    }

    private void Load()
    {
        if (!File.Exists(_path))
        {
            _entries = new List<ProcessEntry>();
            return;
        }

        try
        {
            var json = File.ReadAllText(_path);
            _entries = JsonSerializer.Deserialize<List<ProcessEntry>>(json) ?? new List<ProcessEntry>();
        }
        catch
        {
            _entries = new List<ProcessEntry>();
        }
    }

    private void Save()
    {
        try
        {
            var directory = Path.GetDirectoryName(_path);
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var json = JsonSerializer.Serialize(_entries);
            File.WriteAllText(_path, json);
        }
        catch
        {
        }
    }

    private static bool Matches(Process process, ProcessEntry entry)
    {
        if (!string.IsNullOrWhiteSpace(entry.ProcessName))
        {
            var processName = SafeProcessName(process);
            if (string.IsNullOrWhiteSpace(processName)
                || !string.Equals(processName, entry.ProcessName, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        if (entry.StartTicksUtc == 0)
        {
            return true;
        }

        var startUtc = SafeStartTimeUtc(process);
        if (startUtc is null)
        {
            return false;
        }

        var delta = Math.Abs((startUtc.Value - new DateTime(entry.StartTicksUtc, DateTimeKind.Utc)).TotalSeconds);
        return delta <= 2;
    }

    private static DateTime? SafeStartTimeUtc(Process process)
    {
        try
        {
            return process.StartTime.ToUniversalTime();
        }
        catch
        {
            return null;
        }
    }

    private static int? SafeProcessId(Process process)
    {
        try
        {
            return process.Id;
        }
        catch
        {
            return null;
        }
    }

    private static string? SafeProcessName(Process process)
    {
        try
        {
            return process.ProcessName;
        }
        catch
        {
            return null;
        }
    }

    private static bool TryGetProcess(int pid, out Process process)
    {
        try
        {
            process = Process.GetProcessById(pid);
            return true;
        }
        catch
        {
            process = null!;
            return false;
        }
    }

    private static void TryKill(Process process)
    {
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
    }

    private static string GetDefaultPath()
    {
        var baseDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "Nbn.Workbench");
        return Path.Combine(baseDir, "processes.json");
    }

    private sealed record ProcessEntry(int Pid, string ProcessName, long StartTicksUtc, string Label);
}
