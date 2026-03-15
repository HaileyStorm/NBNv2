using Nbn.Tools.PerfProbe;

namespace Nbn.Tests.Tools;

public sealed class PerfReportWriterTests
{
    [Fact]
    public void ReportArtifacts_ContainTablesAndCharts()
    {
        var report = new PerfReport(
            ToolName: "Nbn.Tools.PerfProbe",
            GeneratedAtUtc: DateTimeOffset.Parse("2026-03-15T12:00:00Z"),
            Environment: new Dictionary<string, string> { ["machine_name"] = "test-host" },
            Scenarios:
            [
                new PerfScenarioResult(
                    Suite: "worker_profile",
                    Scenario: "placement_planner_profile",
                    Backend: "cpu",
                    Status: PerfScenarioStatus.Passed,
                    Summary: "Planner throughput captured.",
                    Parameters: new Dictionary<string, string> { ["planner_iterations"] = "100" },
                    Metrics: new Dictionary<string, double> { ["plans_per_second"] = 1234.5 }),
                new PerfScenarioResult(
                    Suite: "localhost_stress",
                    Scenario: "brain_size_limit_10hz",
                    Backend: "gpu",
                    Status: PerfScenarioStatus.Skipped,
                    Summary: "GPU runtime unavailable.",
                    Parameters: new Dictionary<string, string>(),
                    Metrics: new Dictionary<string, double>(),
                    SkipReason: "regionshard_gpu_backend_not_available")
            ]);

        var markdown = PerfReportWriter.BuildMarkdown(report);
        var html = PerfReportWriter.BuildHtml(report);
        var csv = PerfReportWriter.BuildCsv(report);

        Assert.Contains("| Suite | Scenario | Backend | Status | Primary Metric | Summary |", markdown, StringComparison.Ordinal);
        Assert.Contains("<svg", html, StringComparison.Ordinal);
        Assert.Contains("<table>", html, StringComparison.Ordinal);
        Assert.Contains("placement_planner_profile", csv, StringComparison.Ordinal);
        Assert.Contains("regionshard_gpu_backend_not_available", csv, StringComparison.Ordinal);
    }
}
