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
                    Metrics: new Dictionary<string, double> { ["plans_per_second"] = 1234.5 })
                {
                    DurationMs = 12.5
                },
                new PerfScenarioResult(
                    Suite: "localhost_stress",
                    Scenario: "brain_size_limit_10hz",
                    Backend: "gpu",
                    Status: PerfScenarioStatus.Skipped,
                    Summary: "GPU runtime unavailable.",
                    Parameters: new Dictionary<string, string>(),
                    Metrics: new Dictionary<string, double>(),
                    SkipReason: "regionshard_gpu_backend_not_available")
                {
                    DurationMs = 1.25
                }
            ])
        {
            TotalDurationMs = 13.75
        };

        var markdown = PerfReportWriter.BuildMarkdown(report);
        var html = PerfReportWriter.BuildHtml(report);
        var csv = PerfReportWriter.BuildCsv(report);

        Assert.Contains("| Suite | Scenario | Backend | Status | Duration (ms) | Primary Metric | Metrics | Summary |", markdown, StringComparison.Ordinal);
        Assert.Contains("Total duration: 13.75 ms", markdown, StringComparison.Ordinal);
        Assert.Contains("<svg", html, StringComparison.Ordinal);
        Assert.Contains("<table>", html, StringComparison.Ordinal);
        Assert.Contains("Total duration 13.75 ms", html, StringComparison.Ordinal);
        Assert.Contains("plans_per_second=1234.5", html, StringComparison.Ordinal);
        Assert.Contains("\"12.5\"", csv, StringComparison.Ordinal);
        Assert.Contains("placement_planner_profile", csv, StringComparison.Ordinal);
        Assert.Contains("regionshard_gpu_backend_not_available", csv, StringComparison.Ordinal);
    }

    [Fact]
    public void CurrentSystemCharts_UseCurrentSystemMetrics_AndRenderFullScenarioLabels()
    {
        var report = new PerfReport(
            ToolName: "Nbn.Tools.PerfProbe",
            GeneratedAtUtc: DateTimeOffset.Parse("2026-03-15T12:00:00Z"),
            Environment: new Dictionary<string, string> { ["machine_name"] = "test-host" },
            Scenarios:
            [
                new PerfScenarioResult(
                    Suite: "current_system",
                    Scenario: "service_discovery_snapshot",
                    Backend: "cpu",
                    Status: PerfScenarioStatus.Passed,
                    Summary: "SettingsMonitor discovery snapshot captured.",
                    Parameters: new Dictionary<string, string>(),
                    Metrics: new Dictionary<string, double>
                    {
                        ["discovered_endpoint_count"] = 6
                    }),
                new PerfScenarioResult(
                    Suite: "current_system",
                    Scenario: "placement_inventory_snapshot",
                    Backend: "cpu",
                    Status: PerfScenarioStatus.Passed,
                    Summary: "HiveMind placement inventory captured.",
                    Parameters: new Dictionary<string, string>(),
                    Metrics: new Dictionary<string, double>
                    {
                        ["eligible_workers"] = 2,
                        ["max_cpu_score"] = 72
                    })
            ]);

        var html = PerfReportWriter.BuildHtml(report);

        Assert.Contains("discovered_endpoint_count=6", html, StringComparison.Ordinal);
        Assert.Contains("eligible_workers=2", html, StringComparison.Ordinal);
        Assert.Contains("current_system/service_discovery_snapshot (cpu)", html, StringComparison.Ordinal);
        Assert.Contains("current_system/placement_inventory_snapshot (cpu)", html, StringComparison.Ordinal);
        Assert.Contains("<rect x=\"360\" y=\"28\" width=\"880\"", html, StringComparison.Ordinal);
    }
}
