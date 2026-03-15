namespace Nbn.Shared;

public static class WorkerCapabilitySettingsKeys
{
    public const int DefaultBenchmarkRefreshSeconds = 3600;
    public const int DefaultPressureRebalanceWindow = 6;
    public const double DefaultPressureViolationRatio = 0.5d;
    public const double DefaultPressureLimitTolerancePercent = 2.5d;

    public const string BenchmarkRefreshSecondsKey = "worker.capability.benchmark_refresh_seconds";
    public const string PressureRebalanceWindowKey = "worker.pressure.rebalance.window";
    public const string PressureViolationRatioKey = "worker.pressure.rebalance.violation_ratio";
    public const string PressureLimitTolerancePercentKey = "worker.pressure.limit_tolerance_percent";

    public static IReadOnlyList<string> AllKeys { get; } =
    [
        BenchmarkRefreshSecondsKey,
        PressureRebalanceWindowKey,
        PressureViolationRatioKey,
        PressureLimitTolerancePercentKey
    ];
}
