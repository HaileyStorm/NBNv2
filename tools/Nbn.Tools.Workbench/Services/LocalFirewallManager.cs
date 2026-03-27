using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Principal;
using System.Text;
using System.Threading.Tasks;

namespace Nbn.Tools.Workbench.Services;

/// <summary>
/// Best-effort helper for opening local inbound firewall access for explicitly launched runtime ports.
/// </summary>
public interface ILocalFirewallManager
{
    /// <summary>
    /// Ensures the local machine can accept inbound TCP traffic for the supplied bind host and port.
    /// </summary>
    Task<FirewallAccessResult> EnsureInboundTcpAccessAsync(string label, string bindHost, int port);
}

/// <summary>
/// Describes the outcome of Workbench firewall access checks for a launched service port.
/// </summary>
public enum FirewallAccessStatus
{
    NotNeeded = 0,
    AlreadyAllowed = 1,
    Allowed = 2,
    PermissionRequired = 3,
    Failed = 4,
    Unsupported = 5
}

/// <summary>
/// Describes whether firewall work succeeded and whether the operator still needs to take action.
/// </summary>
public sealed record FirewallAccessResult(FirewallAccessStatus Status, string Message)
{
    public bool RequiresAttention
        => Status is FirewallAccessStatus.PermissionRequired or FirewallAccessStatus.Failed;
}

/// <summary>
/// Captures stdout/stderr from an OS firewall command invocation.
/// </summary>
public sealed record FirewallCommandResult(int ExitCode, string Stdout, string Stderr);

/// <summary>
/// Implements best-effort Windows and Linux firewall rule handling for Workbench local launch.
/// </summary>
public sealed class LocalFirewallManager : ILocalFirewallManager
{
    private readonly Func<ProcessStartInfo, Task<FirewallCommandResult>> _commandRunner;
    private readonly Func<bool> _isWindows;
    private readonly Func<bool> _isLinux;
    private readonly Func<bool> _isElevated;
    private readonly Func<string, bool> _commandExists;

    public LocalFirewallManager(
        Func<ProcessStartInfo, Task<FirewallCommandResult>>? commandRunner = null,
        Func<bool>? isWindows = null,
        Func<bool>? isLinux = null,
        Func<bool>? isElevated = null,
        Func<string, bool>? commandExists = null)
    {
        _commandRunner = commandRunner ?? RunCommandAsync;
        _isWindows = isWindows ?? OperatingSystem.IsWindows;
        _isLinux = isLinux ?? OperatingSystem.IsLinux;
        _isElevated = isElevated ?? IsProcessElevated;
        _commandExists = commandExists ?? CommandExists;
    }

    /// <inheritdoc />
    public async Task<FirewallAccessResult> EnsureInboundTcpAccessAsync(string label, string bindHost, int port)
    {
        if (port <= 0 || port >= 65536)
        {
            return new FirewallAccessResult(FirewallAccessStatus.Failed, $"Port {port} is invalid.");
        }

        if (IsLoopbackHost(bindHost))
        {
            return new FirewallAccessResult(FirewallAccessStatus.NotNeeded, "Loopback bind does not require an inbound firewall rule.");
        }

        if (_isWindows())
        {
            return await EnsureWindowsInboundRuleAsync(label, port).ConfigureAwait(false);
        }

        if (_isLinux())
        {
            return await EnsureLinuxInboundRuleAsync(label, port).ConfigureAwait(false);
        }

        return new FirewallAccessResult(FirewallAccessStatus.Unsupported, "Automatic firewall handling is not supported on this OS.");
    }

    internal static bool IsLoopbackHost(string? host)
    {
        if (string.IsNullOrWhiteSpace(host))
        {
            return false;
        }

        var trimmed = host.Trim();
        if (string.Equals(trimmed, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return IPAddress.TryParse(trimmed, out var address) && IPAddress.IsLoopback(address);
    }

    internal static string BuildWindowsRuleName(string label, int port)
        => $"NBN {SanitizeLabel(label)} TCP {port}";

    internal static string SanitizeLabel(string? label)
    {
        if (string.IsNullOrWhiteSpace(label))
        {
            return "Service";
        }

        var sanitized = new string(label
            .Trim()
            .Select(ch => char.IsLetterOrDigit(ch) ? ch : '-')
            .ToArray());
        return string.IsNullOrWhiteSpace(sanitized) ? "Service" : sanitized;
    }

    private async Task<FirewallAccessResult> EnsureWindowsInboundRuleAsync(string label, int port)
    {
        if (!_commandExists("netsh"))
        {
            return new FirewallAccessResult(FirewallAccessStatus.Unsupported, "Windows firewall tooling (`netsh`) is unavailable.");
        }

        var ruleName = BuildWindowsRuleName(label, port);
        var showResult = await _commandRunner(BuildWindowsShowRuleStartInfo(ruleName)).ConfigureAwait(false);
        if (showResult.ExitCode == 0)
        {
            return new FirewallAccessResult(FirewallAccessStatus.AlreadyAllowed, $"Windows Firewall already allows TCP {port}.");
        }

        var addResult = await _commandRunner(BuildWindowsAddRuleStartInfo(ruleName, port)).ConfigureAwait(false);
        if (addResult.ExitCode == 0)
        {
            return new FirewallAccessResult(FirewallAccessStatus.Allowed, $"Opened Windows Firewall inbound TCP {port}.");
        }

        return new FirewallAccessResult(
            _isElevated() ? FirewallAccessStatus.Failed : FirewallAccessStatus.PermissionRequired,
            _isElevated()
                ? $"Failed to open Windows Firewall inbound TCP {port}: {SummarizeCommandFailure(addResult)}"
                : $"Windows Firewall was not updated for TCP {port}; run Workbench elevated to add the inbound rule automatically.");
    }

    private async Task<FirewallAccessResult> EnsureLinuxInboundRuleAsync(string label, int port)
    {
        if (_commandExists("ufw"))
        {
            var statusResult = await _commandRunner(BuildLinuxCommandStartInfo("ufw", "status")).ConfigureAwait(false);
            if (statusResult.ExitCode == 0
                && statusResult.Stdout.Contains("Status: active", StringComparison.OrdinalIgnoreCase))
            {
                if (!_isElevated())
                {
                    return new FirewallAccessResult(
                        FirewallAccessStatus.PermissionRequired,
                        $"Linux firewall appears active (`ufw`), but TCP {port} was not opened automatically because root privileges are required.");
                }

                var allowResult = await _commandRunner(
                    BuildLinuxCommandStartInfo("ufw", $"allow {port}/tcp comment \"NBN {SanitizeLabel(label)}\"")).ConfigureAwait(false);
                return allowResult.ExitCode == 0
                    ? new FirewallAccessResult(FirewallAccessStatus.Allowed, $"Opened `ufw` inbound TCP {port}.")
                    : new FirewallAccessResult(FirewallAccessStatus.Failed, $"Failed to open `ufw` inbound TCP {port}: {SummarizeCommandFailure(allowResult)}");
            }
        }

        if (_commandExists("firewall-cmd"))
        {
            var stateResult = await _commandRunner(BuildLinuxCommandStartInfo("firewall-cmd", "--state")).ConfigureAwait(false);
            if (stateResult.ExitCode == 0
                && stateResult.Stdout.Contains("running", StringComparison.OrdinalIgnoreCase))
            {
                if (!_isElevated())
                {
                    return new FirewallAccessResult(
                        FirewallAccessStatus.PermissionRequired,
                        $"Linux firewall appears active (`firewalld`), but TCP {port} was not opened automatically because root privileges are required.");
                }

                var addResult = await _commandRunner(
                    BuildLinuxCommandStartInfo("firewall-cmd", $"--quiet --add-port={port}/tcp")).ConfigureAwait(false);
                return addResult.ExitCode == 0
                    ? new FirewallAccessResult(FirewallAccessStatus.Allowed, $"Opened `firewalld` inbound TCP {port}.")
                    : new FirewallAccessResult(FirewallAccessStatus.Failed, $"Failed to open `firewalld` inbound TCP {port}: {SummarizeCommandFailure(addResult)}");
            }
        }

        return new FirewallAccessResult(FirewallAccessStatus.Unsupported, "No supported active Linux firewall manager was detected.");
    }

    internal static ProcessStartInfo BuildWindowsShowRuleStartInfo(string ruleName)
        => new()
        {
            FileName = "netsh",
            Arguments = $"advfirewall firewall show rule name=\"{ruleName}\"",
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8
        };

    internal static ProcessStartInfo BuildWindowsAddRuleStartInfo(string ruleName, int port)
        => new()
        {
            FileName = "netsh",
            Arguments = $"advfirewall firewall add rule name=\"{ruleName}\" dir=in action=allow protocol=TCP localport={port}",
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8
        };

    internal static ProcessStartInfo BuildLinuxCommandStartInfo(string command, string arguments)
        => new()
        {
            FileName = command,
            Arguments = arguments,
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8
        };

    private static bool IsProcessElevated()
    {
        if (OperatingSystem.IsWindows())
        {
            using var identity = WindowsIdentity.GetCurrent();
            var principal = new WindowsPrincipal(identity);
            return principal.IsInRole(WindowsBuiltInRole.Administrator);
        }

        return string.Equals(Environment.UserName, "root", StringComparison.Ordinal);
    }

    private static bool CommandExists(string command)
    {
        var path = Environment.GetEnvironmentVariable("PATH");
        if (string.IsNullOrWhiteSpace(path))
        {
            return false;
        }

        foreach (var directory in path.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var candidate = Path.Combine(directory, command);
            if (File.Exists(candidate))
            {
                return true;
            }

            if (OperatingSystem.IsWindows())
            {
                foreach (var extension in new[] { ".exe", ".cmd", ".bat" })
                {
                    if (File.Exists(candidate + extension))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private static async Task<FirewallCommandResult> RunCommandAsync(ProcessStartInfo startInfo)
    {
        using var process = new Process { StartInfo = startInfo };
        var stdout = new StringBuilder();
        var stderr = new StringBuilder();
        process.OutputDataReceived += (_, args) =>
        {
            if (args.Data is not null)
            {
                stdout.AppendLine(args.Data);
            }
        };
        process.ErrorDataReceived += (_, args) =>
        {
            if (args.Data is not null)
            {
                stderr.AppendLine(args.Data);
            }
        };

        if (!process.Start())
        {
            return new FirewallCommandResult(-1, string.Empty, "Failed to start process.");
        }

        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        await process.WaitForExitAsync().ConfigureAwait(false);
        return new FirewallCommandResult(process.ExitCode, stdout.ToString(), stderr.ToString());
    }

    private static string SummarizeCommandFailure(FirewallCommandResult result)
    {
        var detail = LastNonEmptyLine(result.Stderr) ?? LastNonEmptyLine(result.Stdout);
        return string.IsNullOrWhiteSpace(detail)
            ? $"exit code {result.ExitCode}"
            : detail;
    }

    private static string? LastNonEmptyLine(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return null;
        }

        var lines = text.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return lines.Length == 0 ? null : lines[^1];
    }
}
