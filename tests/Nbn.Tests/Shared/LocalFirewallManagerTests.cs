using System.Diagnostics;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tests.Shared;

public sealed class LocalFirewallManagerTests
{
    [Fact]
    public async Task EnsureInboundTcpAccessAsync_LoopbackBind_IsNotNeeded()
    {
        var manager = new LocalFirewallManager(
            commandRunner: _ => throw new InvalidOperationException("command runner should not be used"),
            isWindows: () => false,
            isLinux: () => true,
            isElevated: () => false,
            commandExists: _ => true);

        var result = await manager.EnsureInboundTcpAccessAsync("SettingsMonitor", "127.0.0.1", 12010);

        Assert.Equal(FirewallAccessStatus.NotNeeded, result.Status);
        Assert.Contains("Loopback bind", result.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EnsureInboundTcpAccessAsync_WindowsExistingRule_ReportsAlreadyAllowed()
    {
        var seen = new List<ProcessStartInfo>();
        var manager = new LocalFirewallManager(
            commandRunner: startInfo =>
            {
                seen.Add(startInfo);
                return Task.FromResult(new FirewallCommandResult(0, string.Empty, string.Empty));
            },
            isWindows: () => true,
            isLinux: () => false,
            isElevated: () => false,
            commandExists: _ => true);

        var result = await manager.EnsureInboundTcpAccessAsync("Settings Monitor", "0.0.0.0", 12010);

        Assert.Equal(FirewallAccessStatus.AlreadyAllowed, result.Status);
        var showRule = Assert.Single(seen);
        Assert.Equal("netsh", showRule.FileName);
        Assert.Contains("show rule", showRule.Arguments, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EnsureInboundTcpAccessAsync_LinuxActiveUfwWithoutRoot_ReportsPermissionRequired()
    {
        var seen = new List<ProcessStartInfo>();
        var manager = new LocalFirewallManager(
            commandRunner: startInfo =>
            {
                seen.Add(startInfo);
                return Task.FromResult(
                    startInfo.FileName == "ufw" && string.Equals(startInfo.Arguments, "status", StringComparison.Ordinal)
                        ? new FirewallCommandResult(0, "Status: active", string.Empty)
                        : new FirewallCommandResult(1, string.Empty, string.Empty));
            },
            isWindows: () => false,
            isLinux: () => true,
            isElevated: () => false,
            commandExists: command => string.Equals(command, "ufw", StringComparison.Ordinal));

        var result = await manager.EnsureInboundTcpAccessAsync("IoGateway", "0.0.0.0", 12050);

        Assert.Equal(FirewallAccessStatus.PermissionRequired, result.Status);
        var statusCall = Assert.Single(seen);
        Assert.Equal("ufw", statusCall.FileName);
        Assert.Equal("status", statusCall.Arguments);
        Assert.Contains("root privileges", result.Message, StringComparison.OrdinalIgnoreCase);
    }
}
