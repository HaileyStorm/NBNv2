using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Nbn.Shared;
using ProtoControl = Nbn.Proto.Control;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Tools.Workbench.ViewModels;

internal sealed record WorkbenchSystemLoadSummary(
    string ResourceSummary,
    string PressureSummary,
    string TickSummary,
    string HealthSummary,
    string SparklinePathData,
    string SparklineStroke);

internal sealed record WorkbenchSystemLoadSnapshot(
    long SampleTimeMs,
    int WorkerCount,
    int CurrentPressureWorkerCount,
    int RecentPressureWorkerCount,
    uint WorkerPressureWindow,
    uint TickSampleCount,
    double TimeoutRate,
    double LateRate,
    float TargetTickHz,
    float RequestedTargetTickHz,
    double CadenceRatio,
    bool AutomaticBackpressureActive,
    bool RescheduleInProgress,
    double HealthScore);

internal sealed class WorkbenchSystemLoadHistoryTracker
{
    internal const int MaxSnapshots = 120;

    private readonly Queue<WorkbenchSystemLoadSnapshot> _samples = new();

    internal IReadOnlyList<WorkbenchSystemLoadSnapshot> Record(
        IReadOnlyList<ProtoSettings.WorkerReadinessCapability> workers,
        ProtoControl.HiveMindStatus? hiveMindStatus,
        long sampleTimeMs)
    {
        _samples.Enqueue(WorkbenchSystemLoadSummaryBuilder.CreateSnapshot(workers, hiveMindStatus, sampleTimeMs));
        while (_samples.Count > MaxSnapshots)
        {
            _samples.Dequeue();
        }

        return _samples.ToArray();
    }

    internal void Clear() => _samples.Clear();
}

internal static class WorkbenchSystemLoadSummaryBuilder
{
    internal const long StaleWorkerTelemetryMs = 15_000;
    internal const string EmptySparklinePathData = "M 0 16 L 180 16";
    internal const string NeutralSparklineStroke = "#7A8796";
    private const double SparklineWidth = 180d;
    private const double SparklineHeight = 32d;
    private const double SparklinePadding = 3d;

    internal static IReadOnlyList<ProtoSettings.WorkerReadinessCapability> FilterWorkers(
        IReadOnlyList<ProtoSettings.WorkerReadinessCapability> inventory,
        long nowMs,
        Func<ProtoSettings.WorkerReadinessCapability, bool>? extraFilter = null)
    {
        return inventory
            .Where(worker =>
                worker.IsAlive
                && worker.IsReady
                && worker.HasCapabilities
                && worker.Capabilities is not null
                && IsFresh(worker.LastSeenMs, nowMs)
                && (extraFilter is null || extraFilter(worker)))
            .ToArray();
    }

    internal static WorkbenchSystemLoadSnapshot CreateSnapshot(
        IReadOnlyList<ProtoSettings.WorkerReadinessCapability> workers,
        ProtoControl.HiveMindStatus? hiveMindStatus,
        long sampleTimeMs)
    {
        var workerCount = Math.Max(0, workers.Count);
        var currentPressureWorkerCount = (int)Math.Max(0u, hiveMindStatus?.CurrentPressureWorkerCount ?? 0u);
        var recentPressureWorkerCount = (int)Math.Max(0u, hiveMindStatus?.RecentPressureWorkerCount ?? 0u);
        var workerPressureWindow = Math.Max(1u, hiveMindStatus?.WorkerPressureWindow ?? 1u);
        var tickSampleCount = Math.Max(0u, hiveMindStatus?.RecentTickSampleCount ?? 0u);
        var timeoutRate = tickSampleCount == 0
            ? 0d
            : (double)(hiveMindStatus?.RecentTimeoutTickCount ?? 0u) / tickSampleCount;
        var lateRate = tickSampleCount == 0
            ? 0d
            : (double)(hiveMindStatus?.RecentLateTickCount ?? 0u) / tickSampleCount;
        var requestedTargetTickHz = ResolveRequestedTargetTickHz(hiveMindStatus);
        var targetTickHz = hiveMindStatus?.TargetTickHz ?? 0f;
        var cadenceRatio = hiveMindStatus is null
            ? 1d
            : ResolveCadenceRatio(hiveMindStatus.AutomaticBackpressureActive, targetTickHz, requestedTargetTickHz);
        return new WorkbenchSystemLoadSnapshot(
            SampleTimeMs: sampleTimeMs,
            WorkerCount: workerCount,
            CurrentPressureWorkerCount: currentPressureWorkerCount,
            RecentPressureWorkerCount: recentPressureWorkerCount,
            WorkerPressureWindow: workerPressureWindow,
            TickSampleCount: tickSampleCount,
            TimeoutRate: timeoutRate,
            LateRate: lateRate,
            TargetTickHz: targetTickHz,
            RequestedTargetTickHz: requestedTargetTickHz,
            CadenceRatio: cadenceRatio,
            AutomaticBackpressureActive: hiveMindStatus?.AutomaticBackpressureActive ?? false,
            RescheduleInProgress: hiveMindStatus?.RescheduleInProgress ?? false,
            HealthScore: ComputeHealthScore(
                workerCount,
                currentPressureWorkerCount,
                recentPressureWorkerCount,
                timeoutRate,
                lateRate,
                cadenceRatio,
                hiveMindStatus?.RescheduleInProgress ?? false));
    }

    internal static WorkbenchSystemLoadSummary Build(
        IReadOnlyList<ProtoSettings.WorkerReadinessCapability> workers,
        ProtoControl.HiveMindStatus? hiveMindStatus,
        IReadOnlyList<WorkbenchSystemLoadSnapshot>? history = null)
    {
        var snapshots = history is { Count: > 0 }
            ? history
            : new[] { CreateSnapshot(workers, hiveMindStatus, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) };
        var latest = snapshots[^1];

        if (workers.Count == 0)
        {
            return new WorkbenchSystemLoadSummary(
                "Resource usage: awaiting worker telemetry.",
                BuildPressureSummary(workerCount: 0, hiveMindStatus, snapshots),
                BuildTickHealthSummary(hiveMindStatus, snapshots),
                BuildHealthSummary(hiveMindStatus, snapshots),
                BuildSparklinePathData(snapshots),
                ResolveSparklineStroke(latest.HealthScore));
        }

        double cpuUsedCores = 0d;
        double cpuQuotaCores = 0d;
        ulong ramUsedBytes = 0UL;
        ulong ramQuotaBytes = 0UL;
        ulong storageUsedBytes = 0UL;
        ulong storageQuotaBytes = 0UL;
        ulong vramUsedBytes = 0UL;
        ulong vramQuotaBytes = 0UL;
        var gpuWorkerCount = 0;

        foreach (var worker in workers)
        {
            var caps = worker.Capabilities!;
            cpuUsedCores += Math.Max(0d, caps.CpuCores * (caps.ProcessCpuLoadPercent / 100d));
            cpuQuotaCores += WorkerCapabilityMath.EffectiveCpuCores(caps.CpuCores, caps.CpuLimitPercent);

            ramUsedBytes += ToUnsigned(caps.ProcessRamUsedBytes);
            ramQuotaBytes += WorkerCapabilityMath.LimitBytes(ToUnsigned(caps.RamTotalBytes), caps.RamLimitPercent);

            storageUsedBytes += CalculateUsedBytes(caps.StorageFreeBytes, caps.StorageTotalBytes);
            storageQuotaBytes += WorkerCapabilityMath.LimitBytes(ToUnsigned(caps.StorageTotalBytes), caps.StorageLimitPercent);

            if (caps.HasGpu && caps.VramTotalBytes > 0)
            {
                gpuWorkerCount++;
                vramUsedBytes += CalculateUsedBytes(caps.VramFreeBytes, caps.VramTotalBytes);
                vramQuotaBytes += WorkerCapabilityMath.LimitBytes(ToUnsigned(caps.VramTotalBytes), caps.GpuVramLimitPercent);
            }
        }

        return new WorkbenchSystemLoadSummary(
            BuildResourceSummary(
                workers.Count,
                gpuWorkerCount,
                cpuUsedCores,
                cpuQuotaCores,
                ramUsedBytes,
                ramQuotaBytes,
                storageUsedBytes,
                storageQuotaBytes,
                vramUsedBytes,
                vramQuotaBytes),
            BuildPressureSummary(workers.Count, hiveMindStatus, snapshots),
            BuildTickHealthSummary(hiveMindStatus, snapshots),
            BuildHealthSummary(hiveMindStatus, snapshots),
            BuildSparklinePathData(snapshots),
            ResolveSparklineStroke(latest.HealthScore));
    }

    private static bool IsFresh(ulong lastSeenMs, long nowMs)
    {
        if (lastSeenMs == 0)
        {
            return false;
        }

        var delta = nowMs - (long)lastSeenMs;
        return delta >= 0 && delta <= StaleWorkerTelemetryMs;
    }

    private static string BuildResourceSummary(
        int workerCount,
        int gpuWorkerCount,
        double cpuUsedCores,
        double cpuQuotaCores,
        ulong ramUsedBytes,
        ulong ramQuotaBytes,
        ulong storageUsedBytes,
        ulong storageQuotaBytes,
        ulong vramUsedBytes,
        ulong vramQuotaBytes)
    {
        var summary =
            $"Resource usage: CPU {FormatCoreCount(cpuUsedCores)}/{FormatCoreCount(cpuQuotaCores)} cores, " +
            $"RAM {FormatBytes(ramUsedBytes)}/{FormatBytes(ramQuotaBytes)}, " +
            $"storage {FormatBytes(storageUsedBytes)}/{FormatBytes(storageQuotaBytes)} across {workerCount} worker{(workerCount == 1 ? string.Empty : "s")}.";

        if (gpuWorkerCount > 0 && vramQuotaBytes > 0)
        {
            summary += $" VRAM {FormatBytes(vramUsedBytes)}/{FormatBytes(vramQuotaBytes)} across {gpuWorkerCount} GPU worker{(gpuWorkerCount == 1 ? string.Empty : "s")}.";
        }

        return summary;
    }

    private static string BuildPressureSummary(
        int workerCount,
        ProtoControl.HiveMindStatus? hiveMindStatus,
        IReadOnlyList<WorkbenchSystemLoadSnapshot> history)
    {
        if (workerCount <= 0)
        {
            if (hiveMindStatus is null)
            {
                return "Pressure: no ready workers reported load telemetry. Connect HiveMind to view pressure state.";
            }

            return $"Pressure: no ready workers reported load telemetry. {hiveMindStatus.RecentPressureWorkerCount} worker{(hiveMindStatus.RecentPressureWorkerCount == 1 ? string.Empty : "s")} reported pressure in the last {Math.Max(1u, hiveMindStatus.WorkerPressureWindow)} snapshot{(Math.Max(1u, hiveMindStatus.WorkerPressureWindow) == 1 ? string.Empty : "s")}.";
        }

        if (hiveMindStatus is null)
        {
            return "Pressure: connect HiveMind to view current and recent worker pressure.";
        }

        var currentPressureWorkerCount = hiveMindStatus.CurrentPressureWorkerCount;
        var currentPart =
            $"Pressure: {currentPressureWorkerCount}/{workerCount} worker{(workerCount == 1 ? string.Empty : "s")} over quota now.";
        if (history.Count <= 1)
        {
            var window = Math.Max(1u, hiveMindStatus.WorkerPressureWindow);
            return
                $"{currentPart} {hiveMindStatus.RecentPressureWorkerCount} worker{(hiveMindStatus.RecentPressureWorkerCount == 1 ? string.Empty : "s")} reported pressure in the last {window} snapshot{(window == 1 ? string.Empty : "s")}.";
        }

        var windowLabel = FormatHistoryWindowLabel(history);
        var avgCurrentPressure = history.Average(static sample => sample.CurrentPressureWorkerCount);
        var peakCurrentPressure = history.Max(static sample => sample.CurrentPressureWorkerCount);
        var snapshotsWithRecentPressure = history.Count(static sample => sample.RecentPressureWorkerCount > 0);
        return
            $"{currentPart} Workbench window {windowLabel}: avg {avgCurrentPressure:0.##} pressured now, peak {peakCurrentPressure}, recent pressure seen in {snapshotsWithRecentPressure}/{history.Count} snapshots.";
    }

    private static string BuildTickHealthSummary(
        ProtoControl.HiveMindStatus? hiveMindStatus,
        IReadOnlyList<WorkbenchSystemLoadSnapshot> history)
    {
        if (hiveMindStatus is null)
        {
            return "Tick health: connect HiveMind to view recent timeout and cadence pressure.";
        }

        var latest = history[^1];
        var cadencePart = latest.AutomaticBackpressureActive && latest.RequestedTargetTickHz > 0f
            ? $"Cadence {FormatHz(latest.TargetTickHz)} vs requested {FormatHz(latest.RequestedTargetTickHz)}."
            : latest.TargetTickHz > 0f
                ? $"Cadence target {FormatHz(latest.TargetTickHz)}."
                : "Cadence target unavailable.";

        if (history.All(static sample => sample.TickSampleCount == 0))
        {
            var waitingReschedulePart = latest.RescheduleInProgress ? " Reschedule in progress." : string.Empty;
            return $"Tick health: waiting for completed ticks. {cadencePart}{waitingReschedulePart}";
        }

        if (history.Count <= 1)
        {
            var timedOutPct = 100d * latest.TimeoutRate;
            var latePct = 100d * latest.LateRate;
            var singleWindowTickPart = $"Tick health: {timedOutPct:0.#}% recent ticks timed out and {latePct:0.#}% had late arrivals ({latest.TickSampleCount} sample{(latest.TickSampleCount == 1 ? string.Empty : "s")}).";
            var singleWindowReschedulePart = latest.RescheduleInProgress ? " Reschedule in progress." : string.Empty;
            return $"{singleWindowTickPart} {cadencePart}{singleWindowReschedulePart}";
        }

        var workbenchWindow = FormatHistoryWindowLabel(history);
        var effectiveSamples = history.Count(static sample => sample.TickSampleCount > 0);
        var avgTimedOutPct = 100d * history.Where(static sample => sample.TickSampleCount > 0).Average(static sample => sample.TimeoutRate);
        var avgLatePct = 100d * history.Where(static sample => sample.TickSampleCount > 0).Average(static sample => sample.LateRate);
        var cadenceFloorPct = 100d * history.Min(static sample => sample.CadenceRatio);
        var tickPart =
            $"Tick health: avg {avgTimedOutPct:0.#}% timeouts and {avgLatePct:0.#}% late arrivals across {effectiveSamples} Workbench snapshot{(effectiveSamples == 1 ? string.Empty : "s")} ({workbenchWindow}).";
        var cadenceWindowPart = latest.AutomaticBackpressureActive && latest.RequestedTargetTickHz > 0f
            ? $" Cadence floor {cadenceFloorPct:0.#}% of requested."
            : string.Empty;
        var reschedulePart = latest.RescheduleInProgress ? " Reschedule in progress." : string.Empty;
        return $"{tickPart} {cadencePart}{cadenceWindowPart}{reschedulePart}";
    }

    private static string BuildHealthSummary(
        ProtoControl.HiveMindStatus? hiveMindStatus,
        IReadOnlyList<WorkbenchSystemLoadSnapshot> history)
    {
        if (hiveMindStatus is null)
        {
            return "Health: connect HiveMind to view long-window trend and early warning signals.";
        }

        var latest = history[^1];
        if (history.Count == 1)
        {
            return $"Health: {latest.HealthScore:0}/100. Early warning blends cadence reduction, late arrivals, timeout rate, and worker pressure.";
        }

        var baselineCount = Math.Max(1, history.Count / 3);
        var baselineScore = history.Take(baselineCount).Average(static sample => sample.HealthScore);
        var delta = latest.HealthScore - baselineScore;
        var trend = delta switch
        {
            > 6d => "improving",
            < -6d => "softening",
            _ => "steady"
        };
        var avgLatePct = 100d * history.Where(static sample => sample.TickSampleCount > 0).DefaultIfEmpty(latest).Average(static sample => sample.LateRate);
        var cadenceFloorPct = 100d * history.Min(static sample => sample.CadenceRatio);
        var peakPressureWorkers = history.Max(static sample => sample.CurrentPressureWorkerCount);
        var workbenchWindow = FormatHistoryWindowLabel(history);
        return
            $"Health: {latest.HealthScore:0}/100 {trend} over {workbenchWindow}. Cadence floor {cadenceFloorPct:0.#}% of requested, avg late {avgLatePct:0.#}%, peak pressure {peakPressureWorkers}/{Math.Max(1, latest.WorkerCount)} worker{(Math.Max(1, latest.WorkerCount) == 1 ? string.Empty : "s")}.";
    }

    private static float ResolveRequestedTargetTickHz(ProtoControl.HiveMindStatus? hiveMindStatus)
    {
        if (hiveMindStatus is null)
        {
            return 0f;
        }

        return hiveMindStatus.HasTickRateOverride && hiveMindStatus.TickRateOverrideHz > 0f
            ? hiveMindStatus.TickRateOverrideHz
            : hiveMindStatus.ConfiguredTargetTickHz;
    }

    private static double ResolveCadenceRatio(
        bool automaticBackpressureActive,
        float targetTickHz,
        float requestedTargetTickHz)
    {
        if (!automaticBackpressureActive || requestedTargetTickHz <= 0f || targetTickHz <= 0f)
        {
            return 1d;
        }

        return Math.Clamp(targetTickHz / requestedTargetTickHz, 0d, 1d);
    }

    private static double ComputeHealthScore(
        int workerCount,
        int currentPressureWorkerCount,
        int recentPressureWorkerCount,
        double timeoutRate,
        double lateRate,
        double cadenceRatio,
        bool rescheduleInProgress)
    {
        var safeWorkerCount = Math.Max(1, workerCount);
        var currentPressureRatio = (double)currentPressureWorkerCount / safeWorkerCount;
        var recentPressureRatio = (double)recentPressureWorkerCount / safeWorkerCount;

        var score = 100d;
        score -= Math.Clamp(timeoutRate, 0d, 1d) * 140d;
        score -= Math.Clamp(lateRate, 0d, 1d) * 45d;
        score -= Math.Clamp(1d - cadenceRatio, 0d, 1d) * 45d;
        score -= Math.Clamp(currentPressureRatio, 0d, 1d) * 20d;
        score -= Math.Clamp(recentPressureRatio, 0d, 1d) * 10d;
        if (rescheduleInProgress)
        {
            score -= 5d;
        }

        return Math.Clamp(score, 0d, 100d);
    }

    private static string BuildSparklinePathData(IReadOnlyList<WorkbenchSystemLoadSnapshot> history)
    {
        if (history.Count <= 1)
        {
            return EmptySparklinePathData;
        }

        var builder = new StringBuilder();
        var drawableHeight = SparklineHeight - (SparklinePadding * 2d);
        for (var i = 0; i < history.Count; i++)
        {
            var x = history.Count == 1
                ? 0d
                : (SparklineWidth * i) / (history.Count - 1d);
            var y = SparklinePadding + ((100d - history[i].HealthScore) / 100d) * drawableHeight;
            builder.Append(i == 0 ? "M " : " L ");
            builder.Append(x.ToString("0.##", CultureInfo.InvariantCulture));
            builder.Append(' ');
            builder.Append(y.ToString("0.##", CultureInfo.InvariantCulture));
        }

        return builder.ToString();
    }

    private static string ResolveSparklineStroke(double healthScore)
        => healthScore switch
        {
            >= 85d => "#5FA46A",
            >= 70d => "#C5A242",
            >= 50d => "#D07A43",
            _ => "#C84A4A"
        };

    private static string FormatHistoryWindowLabel(IReadOnlyList<WorkbenchSystemLoadSnapshot> history)
    {
        if (history.Count <= 1)
        {
            return "one snapshot";
        }

        var elapsedMs = Math.Max(0L, history[^1].SampleTimeMs - history[0].SampleTimeMs);
        if (elapsedMs <= 0)
        {
            return $"{history.Count} snapshots";
        }

        var elapsed = TimeSpan.FromMilliseconds(elapsedMs);
        if (elapsed.TotalMinutes >= 1d)
        {
            return elapsed.TotalMinutes >= 10d
                ? $"{elapsed.TotalMinutes:0}m"
                : $"{elapsed.TotalMinutes:0.#}m";
        }

        return elapsed.TotalSeconds >= 10d
            ? $"{elapsed.TotalSeconds:0}s"
            : $"{elapsed.TotalSeconds:0.#}s";
    }

    private static string FormatHz(float value)
        => !float.IsFinite(value) || value <= 0f
            ? "n/a"
            : $"{value:0.###} Hz";

    private static string FormatCoreCount(double value)
    {
        if (!double.IsFinite(value) || value <= 0d)
        {
            return "0";
        }

        return value >= 10d
            ? value.ToString("0.#", CultureInfo.InvariantCulture)
            : value.ToString("0.##", CultureInfo.InvariantCulture);
    }

    private static string FormatBytes(ulong bytes)
    {
        if (bytes == 0UL)
        {
            return "0 B";
        }

        var units = new[] { "B", "KiB", "MiB", "GiB", "TiB", "PiB" };
        var value = (double)bytes;
        var unitIndex = 0;
        while (value >= 1024d && unitIndex < units.Length - 1)
        {
            value /= 1024d;
            unitIndex++;
        }

        var format = value >= 10d ? "0.#" : "0.##";
        return $"{value.ToString(format, CultureInfo.InvariantCulture)} {units[unitIndex]}";
    }

    private static ulong CalculateUsedBytes(ulong freeBytes, ulong totalBytes)
        => freeBytes >= totalBytes ? 0UL : totalBytes - freeBytes;

    private static ulong ToUnsigned(ulong value) => value;

    private static ulong ToUnsigned(long value)
        => value <= 0 ? 0UL : (ulong)value;
}
