using System.Globalization;
using System.Text;
using System.Text.Json;

namespace Nbn.Tools.PerfProbe;

public static class PerfReportWriter
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    public static async Task WriteAsync(
        PerfReport report,
        string outputDirectory,
        CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(outputDirectory);

        var jsonPath = Path.Combine(outputDirectory, "perf-report.json");
        var csvPath = Path.Combine(outputDirectory, "perf-report.csv");
        var markdownPath = Path.Combine(outputDirectory, "perf-report.md");
        var htmlPath = Path.Combine(outputDirectory, "perf-report.html");

        await File.WriteAllTextAsync(jsonPath, JsonSerializer.Serialize(report, JsonOptions), cancellationToken);
        await File.WriteAllTextAsync(csvPath, BuildCsv(report), cancellationToken);
        await File.WriteAllTextAsync(markdownPath, BuildMarkdown(report), cancellationToken);
        await File.WriteAllTextAsync(htmlPath, BuildHtml(report), cancellationToken);
    }

    public static string BuildCsv(PerfReport report)
    {
        var builder = new StringBuilder();
        builder.AppendLine("suite,scenario,backend,status,summary,skip_reason,failure,primary_metric_label,primary_metric_value,parameters_json,metrics_json");
        foreach (var scenario in report.Scenarios)
        {
            var primaryValue = scenario.TryResolvePrimaryMetric(out var value)
                ? value.ToString("0.###", CultureInfo.InvariantCulture)
                : string.Empty;
            builder.AppendLine(string.Join(",",
                EscapeCsv(scenario.Suite),
                EscapeCsv(scenario.Scenario),
                EscapeCsv(scenario.Backend),
                EscapeCsv(scenario.Status.ToString()),
                EscapeCsv(scenario.Summary),
                EscapeCsv(scenario.SkipReason ?? string.Empty),
                EscapeCsv(scenario.Failure ?? string.Empty),
                EscapeCsv(scenario.PrimaryMetricLabel),
                EscapeCsv(primaryValue),
                EscapeCsv(JsonSerializer.Serialize(scenario.Parameters, JsonOptions)),
                EscapeCsv(JsonSerializer.Serialize(scenario.Metrics, JsonOptions))));
        }

        return builder.ToString();
    }

    public static string BuildMarkdown(PerfReport report)
    {
        var builder = new StringBuilder();
        builder.AppendLine("# NBN Performance Probe");
        builder.AppendLine();
        builder.AppendLine($"Generated: {report.GeneratedAtUtc:O}");
        builder.AppendLine();
        builder.AppendLine("| Suite | Scenario | Backend | Status | Primary Metric | Metrics | Summary |");
        builder.AppendLine("| --- | --- | --- | --- | --- | --- | --- |");

        foreach (var scenario in report.Scenarios)
        {
            var primaryMetric = scenario.TryResolvePrimaryMetric(out var value)
                ? $"{scenario.PrimaryMetricLabel}={value:0.###}"
                : "(none)";
            builder.AppendLine(
                $"| {EscapeMarkdown(scenario.Suite)} | {EscapeMarkdown(scenario.Scenario)} | {EscapeMarkdown(scenario.Backend)} | {scenario.Status} | {EscapeMarkdown(primaryMetric)} | {EscapeMarkdown(FormatKeyValueSummary(scenario.Metrics))} | {EscapeMarkdown(scenario.Summary)} |");
        }

        return builder.ToString();
    }

    public static string BuildHtml(PerfReport report)
    {
        const double svgWidth = 1100d;
        const double chartLeft = 200d;
        const double chartWidth = 820d;
        var maxMetric = report.Scenarios
            .Select(static scenario => scenario.PrimaryMetricValue)
            .DefaultIfEmpty(0d)
            .Max();
        var chartRows = report.Scenarios
            .Select((scenario, index) => BuildChartBar(scenario, index, maxMetric, chartLeft, chartWidth, svgWidth))
            .ToArray();
        var tableRows = report.Scenarios
            .Select(BuildTableRow)
            .ToArray();

        return $$"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>NBN Performance Probe</title>
  <style>
    :root { color-scheme: light; --ink: #1f2a3d; --grid: #d5dce8; --accent: #1d6fd6; --accent-soft: #9dc4f7; --bg: #f7f9fc; }
    body { font-family: "Segoe UI", sans-serif; margin: 24px; color: var(--ink); background: var(--bg); }
    h1 { margin-bottom: 4px; }
    .meta { margin-bottom: 24px; color: #4f5d75; }
    .card { background: white; border: 1px solid var(--grid); border-radius: 14px; padding: 18px; margin-bottom: 20px; box-shadow: 0 6px 20px rgba(29, 54, 96, 0.06); }
    table { width: 100%; border-collapse: collapse; }
    th, td { border-bottom: 1px solid var(--grid); text-align: left; padding: 10px 8px; vertical-align: top; }
    th { font-size: 0.92rem; color: #4f5d75; }
    .status-passed { color: #0c7b32; font-weight: 600; }
    .status-skipped { color: #9a6700; font-weight: 600; }
    .status-failed { color: #b42318; font-weight: 600; }
    .note { color: #4f5d75; font-size: 0.92rem; }
    svg text { font-size: 11px; fill: var(--ink); }
  </style>
</head>
<body>
  <h1>NBN Performance Probe</h1>
  <div class="meta">Generated {{report.GeneratedAtUtc:O}}</div>
  <div class="card">
    <h2>Charts</h2>
    <p class="note">Bar chart uses each scenario's primary metric when available. GPU runtime scenarios may appear as skips until the RegionShard GPU backend lands.</p>
    <svg width="1100" height="{{Math.Max(220, 60 + (report.Scenarios.Count * 36))}}" viewBox="0 0 1100 {{Math.Max(220, 60 + (report.Scenarios.Count * 36))}}" role="img" aria-label="Performance scenario bar chart">
      <line x1="190" y1="20" x2="190" y2="{{40 + (report.Scenarios.Count * 36)}}" stroke="#d5dce8" />
      {{string.Join(Environment.NewLine, chartRows)}}
    </svg>
  </div>
  <div class="card">
    <h2>Table</h2>
    <table>
      <thead>
        <tr>
          <th>Suite</th>
          <th>Scenario</th>
          <th>Backend</th>
          <th>Status</th>
          <th>Primary Metric</th>
          <th>Metrics</th>
          <th>Summary</th>
        </tr>
      </thead>
      <tbody>
        {{string.Join(Environment.NewLine, tableRows)}}
      </tbody>
    </table>
  </div>
</body>
</html>
""";
    }

    private static string BuildChartBar(
        PerfScenarioResult scenario,
        int index,
        double maxMetric,
        double chartLeft,
        double chartWidth,
        double svgWidth)
    {
        const double rowHeight = 36d;
        const double barHeight = 20d;
        var top = 28d + (index * rowHeight);
        var width = maxMetric <= 0d || scenario.PrimaryMetricValue <= 0d
            ? 0d
            : Math.Clamp((scenario.PrimaryMetricValue / maxMetric) * chartWidth, 0d, chartWidth);
        var statusColor = scenario.Status switch
        {
            PerfScenarioStatus.Passed => "#1d6fd6",
            PerfScenarioStatus.Skipped => "#f1a208",
            PerfScenarioStatus.Failed => "#d64545",
            _ => "#9dc4f7"
        };
        var label = scenario.TryResolvePrimaryMetric(out var value)
            ? $"{scenario.PrimaryMetricLabel}={value:0.###}"
            : scenario.Status.ToString();
        var barRight = chartLeft + width;
        var labelInside = barRight >= chartLeft + (chartWidth * 0.82d);
        var labelX = labelInside
            ? chartLeft + chartWidth - 8d
            : Math.Min(barRight + 10d, svgWidth - 12d);
        var anchor = labelInside ? "end" : "start";
        var title = TruncateText($"{scenario.Suite}/{scenario.Scenario} ({scenario.Backend})", 28);

        return $$"""
  <text x="16" y="{{top + 14}}">{{EscapeHtml(title)}}</text>
  <rect x="{{chartLeft}}" y="{{top}}" width="{{width.ToString("0.###", CultureInfo.InvariantCulture)}}" height="{{barHeight}}" rx="6" fill="{{statusColor}}" />
  <text x="{{labelX.ToString("0.###", CultureInfo.InvariantCulture)}}" y="{{top + 14}}" text-anchor="{{anchor}}">{{EscapeHtml(label)}}</text>
""";
    }

    private static string BuildTableRow(PerfScenarioResult scenario)
    {
        var statusClass = scenario.Status switch
        {
            PerfScenarioStatus.Passed => "status-passed",
            PerfScenarioStatus.Skipped => "status-skipped",
            PerfScenarioStatus.Failed => "status-failed",
            _ => string.Empty
        };
        var primaryMetric = scenario.TryResolvePrimaryMetric(out var value)
            ? $"{scenario.PrimaryMetricLabel}={value:0.###}"
            : "(none)";
        var summary = scenario.Status switch
        {
            PerfScenarioStatus.Skipped when !string.IsNullOrWhiteSpace(scenario.SkipReason) => $"{scenario.Summary} [{scenario.SkipReason}]",
            PerfScenarioStatus.Failed when !string.IsNullOrWhiteSpace(scenario.Failure) => $"{scenario.Summary} [{scenario.Failure}]",
            _ => scenario.Summary
        };

        return $$"""
        <tr>
          <td>{{EscapeHtml(scenario.Suite)}}</td>
          <td>{{EscapeHtml(scenario.Scenario)}}</td>
          <td>{{EscapeHtml(scenario.Backend)}}</td>
          <td class="{{statusClass}}">{{scenario.Status}}</td>
          <td>{{EscapeHtml(primaryMetric)}}</td>
          <td>{{EscapeHtml(FormatKeyValueSummary(scenario.Metrics))}}</td>
          <td>{{EscapeHtml(summary)}}</td>
        </tr>
""";
    }

    private static string FormatKeyValueSummary(IReadOnlyDictionary<string, double> values)
    {
        if (values.Count == 0)
        {
            return "(none)";
        }

        return string.Join(
            "; ",
            values
                .OrderBy(static pair => pair.Key, StringComparer.Ordinal)
                .Select(pair => $"{pair.Key}={pair.Value:0.###}"));
    }

    private static string TruncateText(string value, int maxLength)
    {
        if (value.Length <= maxLength)
        {
            return value;
        }

        return value[..Math.Max(0, maxLength - 3)] + "...";
    }

    private static string EscapeCsv(string value)
        => $"\"{value.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";

    private static string EscapeMarkdown(string value)
        => value.Replace("|", "\\|", StringComparison.Ordinal);

    private static string EscapeHtml(string value)
        => value
            .Replace("&", "&amp;", StringComparison.Ordinal)
            .Replace("<", "&lt;", StringComparison.Ordinal)
            .Replace(">", "&gt;", StringComparison.Ordinal)
            .Replace("\"", "&quot;", StringComparison.Ordinal);
}
