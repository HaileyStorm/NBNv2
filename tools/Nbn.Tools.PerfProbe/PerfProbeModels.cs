using System.Text.Json.Serialization;

namespace Nbn.Tools.PerfProbe;

public sealed record PerfProbeConfig(
    string OutputDirectory,
    WorkerProfileConfig WorkerProfile,
    LocalhostStressConfig LocalhostStress);

public sealed record CurrentSystemProfileConfig(
    string SettingsHost,
    int SettingsPort,
    string SettingsName,
    string BindHost,
    int BindPort);

public sealed record WorkerProfileConfig(
    int PlannerWorkerCount,
    int PlannerIterations,
    int HiddenRegionNeurons);

public sealed record LocalhostStressConfig(
    IReadOnlyList<float> TargetTickRates,
    IReadOnlyList<int> BrainSizes,
    IReadOnlyList<int> BrainCounts,
    IReadOnlyList<float> SustainableTickSweep,
    int SustainableWorkloadNeurons,
    int SustainableBrainCount,
    int SpawnChurnBrainCount,
    int RunSeconds);

public sealed record PerfReport(
    string ToolName,
    DateTimeOffset GeneratedAtUtc,
    IReadOnlyDictionary<string, string> Environment,
    IReadOnlyList<PerfScenarioResult> Scenarios);

public sealed record PerfScenarioResult(
    string Suite,
    string Scenario,
    string Backend,
    PerfScenarioStatus Status,
    string Summary,
    IReadOnlyDictionary<string, string> Parameters,
    IReadOnlyDictionary<string, double> Metrics,
    string? SkipReason = null,
    string? Failure = null)
{
    [JsonIgnore]
    public double PrimaryMetricValue => TryResolvePrimaryMetric(out var value) ? value : 0d;

    [JsonIgnore]
    public string PrimaryMetricLabel => ResolvePrimaryMetricLabel();

    public bool TryResolvePrimaryMetric(out double value)
    {
        foreach (var key in PreferredMetricOrder)
        {
            if (Metrics.TryGetValue(key, out value))
            {
                return true;
            }
        }

        value = 0d;
        return false;
    }

    private string ResolvePrimaryMetricLabel()
    {
        foreach (var key in PreferredMetricOrder)
        {
            if (Metrics.ContainsKey(key))
            {
                return key;
            }
        }

        return "metric";
    }

    private static readonly string[] PreferredMetricOrder =
    [
        "cpu_score",
        "gpu_score",
        "plans_per_second",
        "max_supported_hidden_neurons",
        "max_supported_brain_count",
        "max_supported_tick_hz",
        "median_spawn_latency_ms",
        "observed_tick_hz"
    ];
}

public enum PerfScenarioStatus
{
    Passed = 0,
    Skipped = 1,
    Failed = 2
}

public static class PerfProbeDefaults
{
    public static PerfProbeConfig Create(string outputDirectory)
        => new(
            OutputDirectory: outputDirectory,
            WorkerProfile: new WorkerProfileConfig(
                PlannerWorkerCount: 8,
                PlannerIterations: 2_000,
                HiddenRegionNeurons: 8_192),
            LocalhostStress: new LocalhostStressConfig(
                TargetTickRates: [5f, 10f, 20f],
                BrainSizes: [512, 2_048, 8_192],
                BrainCounts: [1, 2, 4, 8],
                SustainableTickSweep: [5f, 10f, 15f, 20f, 30f],
                SustainableWorkloadNeurons: 2_048,
                SustainableBrainCount: 2,
                SpawnChurnBrainCount: 4,
                RunSeconds: 4));
}
