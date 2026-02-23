using System;
using System.Diagnostics;
using Nbn.Tools.Workbench.Services;
using Xunit;

namespace Nbn.Tests.Workbench;

public class LocalServiceRunnerTests
{
    [Fact]
    public async Task StartAsync_ReturnsFailure_WhenProcessExitsDuringStartup()
    {
        var runner = new LocalServiceRunner();
        for (var attempt = 0; attempt < 8; attempt++)
        {
            var result = await runner.StartAsync(
                CreateImmediateExitStartInfo(),
                waitForExit: false,
                label: $"test-quick-exit-{attempt}");

            Assert.False(result.Success);
            Assert.Contains("exited during startup", result.Message, StringComparison.OrdinalIgnoreCase);
            Assert.False(runner.IsRunning);
        }
    }

    private static ProcessStartInfo CreateImmediateExitStartInfo()
    {
        if (OperatingSystem.IsWindows())
        {
            return new ProcessStartInfo
            {
                FileName = "cmd.exe",
                Arguments = "/d /c exit 0",
                UseShellExecute = false,
                CreateNoWindow = true
            };
        }

        return new ProcessStartInfo
        {
            FileName = "/bin/sh",
            Arguments = "-c \"exit 0\"",
            UseShellExecute = false,
            CreateNoWindow = true
        };
    }
}
