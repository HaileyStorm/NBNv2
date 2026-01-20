using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;

namespace Nbn.Tools.Workbench.Services;

public static class WorkbenchLog
{
    private static readonly object Gate = new();
    private static readonly ConcurrentDictionary<string, object> FileLocks = new();
    private static bool _enabled;
    private static string? _sessionDirectory;

    public static bool Enabled => _enabled;

    public static string SessionDirectory => _sessionDirectory ?? string.Empty;

    public static void SetEnabled(bool enabled)
    {
        lock (Gate)
        {
            _enabled = enabled;
            if (!_enabled)
            {
                return;
            }

            if (string.IsNullOrWhiteSpace(_sessionDirectory))
            {
                _sessionDirectory = CreateSessionDirectory();
            }
            else
            {
                EnsureDirectory(_sessionDirectory);
            }

            ClearLogs(_sessionDirectory);

            WriteLine("workbench.log", "Workbench logging enabled.");
        }
    }

    public static void Info(string message)
        => WriteLine("workbench.log", message);

    public static void Warn(string message)
        => WriteLine("workbench.log", "WARN: " + message);

    public static void Error(string message)
        => WriteLine("workbench.log", "ERROR: " + message);

    public static ProcessLogFiles? GetProcessLogFiles(string label)
    {
        if (!_enabled)
        {
            return null;
        }

        var safeLabel = Sanitize(label);
        var stdout = Path.Combine(SessionDirectory, $"{safeLabel}.out.log");
        var stderr = Path.Combine(SessionDirectory, $"{safeLabel}.err.log");
        return new ProcessLogFiles(stdout, stderr);
    }

    public static void AppendLine(string path, string? line)
    {
        if (!_enabled || string.IsNullOrWhiteSpace(path) || line is null)
        {
            return;
        }

        var fileLock = FileLocks.GetOrAdd(path, _ => new object());
        lock (fileLock)
        {
            try
            {
                File.AppendAllText(path, $"{DateTimeOffset.UtcNow:O} {line}{Environment.NewLine}", Encoding.UTF8);
            }
            catch
            {
            }
        }
    }

    private static void WriteLine(string fileName, string message)
    {
        if (!_enabled)
        {
            return;
        }

        try
        {
            var path = Path.Combine(SessionDirectory, fileName);
            File.AppendAllText(path, $"{DateTimeOffset.UtcNow:O} {message}{Environment.NewLine}", Encoding.UTF8);
        }
        catch
        {
        }
    }

    private static string CreateSessionDirectory()
    {
        var baseDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "Nbn.Workbench",
            "logs");
        Directory.CreateDirectory(baseDir);
        return baseDir;
    }

    private static void EnsureDirectory(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return;
        }

        try
        {
            Directory.CreateDirectory(path);
        }
        catch
        {
        }
    }

    private static void ClearLogs(string? directory)
    {
        if (string.IsNullOrWhiteSpace(directory) || !Directory.Exists(directory))
        {
            return;
        }

        try
        {
            foreach (var file in Directory.EnumerateFiles(directory, "*.log"))
            {
                File.Delete(file);
            }
        }
        catch
        {
        }
    }

    private static string Sanitize(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "process";
        }

        foreach (var ch in Path.GetInvalidFileNameChars())
        {
            value = value.Replace(ch, '_');
        }

        return value.Replace(' ', '_');
    }
}

public sealed record ProcessLogFiles(string StdoutPath, string StderrPath);
