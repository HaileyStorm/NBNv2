using System.Text.Json.Serialization;

namespace Nbn.Tools.PerfProbe;

/// <summary>
/// Captures the complete configuration for a probe run that can execute multiple suites.
/// </summary>
public sealed record PerfProbeConfig(
    string OutputDirectory,
    WorkerProfileConfig WorkerProfile,
    LocalhostStressConfig LocalhostStress);

/// <summary>
/// Describes how the current-system snapshot mode should attach to a running runtime.
/// </summary>
public sealed record CurrentSystemProfileConfig(
    string SettingsHost,
    int SettingsPort,
    string SettingsName,
    string BindHost,
    int BindPort);

/// <summary>
/// Configures placement-planner and capability sampling for the worker-profile suite.
/// </summary>
public sealed record WorkerProfileConfig(
    int PlannerWorkerCount,
    int PlannerIterations,
    int HiddenRegionNeurons);

/// <summary>
/// Configures localhost runtime stress scenarios and their shared workload knobs.
/// </summary>
public sealed record LocalhostStressConfig(
    IReadOnlyList<float> TargetTickRates,
    IReadOnlyList<int> BrainSizes,
    IReadOnlyList<int> BrainCounts,
    IReadOnlyList<float> SustainableTickSweep,
    int SustainableWorkloadNeurons,
    int SustainableBrainCount,
    int SpawnChurnBrainCount,
    int RunSeconds,
    ComputeDominantStressConfig ComputeDominantWorkload);

/// <summary>
/// Configures the recurrent workload used for compute-dominant localhost measurements.
/// </summary>
public sealed record ComputeDominantStressConfig(
    float TargetTickHz,
    int HiddenNeurons,
    int BrainCount,
    int RunSeconds);

/// <summary>
/// Represents a complete PerfProbe run and the scenario rows emitted for that run.
/// </summary>
public sealed record PerfReport(
    string ToolName,
    DateTimeOffset GeneratedAtUtc,
    IReadOnlyDictionary<string, string> Environment,
    IReadOnlyList<PerfScenarioResult> Scenarios)
{
    /// <summary>
    /// Gets the wall-clock duration for the overall probe run in milliseconds.
    /// </summary>
    public double TotalDurationMs { get; init; }
}

/// <summary>
/// Represents one stable report row emitted by a PerfProbe scenario.
/// </summary>
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
    public double DurationMs { get; init; }

    [JsonIgnore]
    public double PrimaryMetricValue => TryResolvePrimaryMetric(out var value) ? value : 0d;

    [JsonIgnore]
    public string PrimaryMetricLabel => ResolvePrimaryMetricLabel();

    /// <summary>
    /// Resolves the primary metric value that should drive summaries and chart ordering.
    /// </summary>
    public bool TryResolvePrimaryMetric(out double value)
        => TryResolvePrimaryMetricCore(out _, out value);

    private string ResolvePrimaryMetricLabel()
        => TryResolvePrimaryMetricCore(out var label, out _)
            ? label
            : "metric";

    private bool TryResolvePrimaryMetricCore(out string label, out double value)
    {
        foreach (var key in GetPreferredMetricOrder())
        {
            if (Metrics.TryGetValue(key, out value)
                && double.IsFinite(value)
                && value > 0d)
            {
                label = key;
                return true;
            }
        }

        foreach (var pair in Metrics)
        {
            if (double.IsFinite(pair.Value) && pair.Value > 0d)
            {
                label = pair.Key;
                value = pair.Value;
                return true;
            }
        }

        foreach (var key in GetPreferredMetricOrder())
        {
            if (Metrics.TryGetValue(key, out value) && double.IsFinite(value))
            {
                label = key;
                return true;
            }
        }

        foreach (var pair in Metrics)
        {
            if (double.IsFinite(pair.Value))
            {
                label = pair.Key;
                value = pair.Value;
                return true;
            }
        }

        label = string.Empty;
        value = 0d;
        return false;
    }

    private IEnumerable<string> GetPreferredMetricOrder()
    {
        if (ScenarioMetricOrder.TryGetValue((Suite, Scenario), out var preferredKeys))
        {
            foreach (var key in preferredKeys)
            {
                yield return key;
            }
        }

        foreach (var key in DefaultPreferredMetricOrder)
        {
            yield return key;
        }
    }

    private static readonly IReadOnlyDictionary<(string Suite, string Scenario), string[]> ScenarioMetricOrder =
        new Dictionary<(string Suite, string Scenario), string[]>
        {
            [("current_system", "service_discovery_snapshot")] =
            [
                "discovered_endpoint_count"
            ],
            [("current_system", "worker_inventory_snapshot")] =
            [
                "ready_workers",
                "active_workers",
                "gpu_runtime_ready_workers",
                "gpu_ready_workers",
                "max_cpu_score",
                "max_gpu_score"
            ],
            [("current_system", "hivemind_status_snapshot")] =
            [
                "target_tick_hz",
                "registered_brains",
                "registered_shards",
                "last_completed_tick_id"
            ],
            [("current_system", "placement_inventory_snapshot")] =
            [
                "eligible_workers",
                "gpu_capable_workers",
                "max_cpu_score",
                "max_gpu_score"
            ]
        };

    private static readonly string[] DefaultPreferredMetricOrder =
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

/// <summary>
/// Describes the stable outcome for an individual PerfProbe scenario row.
/// </summary>
public enum PerfScenarioStatus
{
    Passed = 0,
    Skipped = 1,
    Failed = 2
}

/// <summary>
/// Provides the default configuration used by the PerfProbe CLI.
/// </summary>
public static class PerfProbeDefaults
{
    /// <summary>
    /// Creates the default probe configuration rooted at the resolved output directory.
    /// </summary>
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
                RunSeconds: 4,
                ComputeDominantWorkload: new ComputeDominantStressConfig(
                    TargetTickHz: 120f,
                    HiddenNeurons: 262_144,
                    BrainCount: 1,
                    RunSeconds: 4)));
}
