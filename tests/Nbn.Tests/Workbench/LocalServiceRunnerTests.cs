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
        var startInfo = new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = "--version",
            UseShellExecute = false,
            CreateNoWindow = true
        };

        var result = await runner.StartAsync(startInfo, waitForExit: false, label: "test-quick-exit");

        Assert.False(result.Success);
        Assert.Contains("exited during startup", result.Message, StringComparison.OrdinalIgnoreCase);
        Assert.False(runner.IsRunning);
    }
}
