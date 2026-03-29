using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Nbn.Shared;
using ProtoControl = Nbn.Proto.Control;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Tools.Workbench.ViewModels;

internal sealed record WorkbenchSystemLoadSummary(
    string ResourceSummary,
    string PressureSummary,
    string TickSummary);

internal static class WorkbenchSystemLoadSummaryBuilder
{
    internal const long StaleWorkerTelemetryMs = 15_000;

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

    internal static WorkbenchSystemLoadSummary Build(
        IReadOnlyList<ProtoSettings.WorkerReadinessCapability> workers,
        ProtoControl.HiveMindStatus? hiveMindStatus)
    {
        if (workers.Count == 0)
        {
            return new WorkbenchSystemLoadSummary(
                "Resource usage: awaiting worker telemetry.",
                BuildPressureSummary(workerCount: 0, hiveMindStatus),
                BuildTickHealthSummary(hiveMindStatus));
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
            BuildPressureSummary(workers.Count, hiveMindStatus),
            BuildTickHealthSummary(hiveMindStatus));
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
        ProtoControl.HiveMindStatus? hiveMindStatus)
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
        var window = Math.Max(1u, hiveMindStatus.WorkerPressureWindow);
        return
            $"{currentPart} {hiveMindStatus.RecentPressureWorkerCount} worker{(hiveMindStatus.RecentPressureWorkerCount == 1 ? string.Empty : "s")} reported pressure in the last {window} snapshot{(window == 1 ? string.Empty : "s")}.";
    }

    private static string BuildTickHealthSummary(ProtoControl.HiveMindStatus? hiveMindStatus)
    {
        if (hiveMindStatus is null)
        {
            return "Tick health: connect HiveMind to view recent timeout and cadence pressure.";
        }

        var sampleCount = Math.Max(0u, hiveMindStatus.RecentTickSampleCount);
        var timedOutPct = sampleCount == 0
            ? 0d
            : (100d * hiveMindStatus.RecentTimeoutTickCount) / sampleCount;
        var latePct = sampleCount == 0
            ? 0d
            : (100d * hiveMindStatus.RecentLateTickCount) / sampleCount;
        var requestedTargetHz = hiveMindStatus.HasTickRateOverride && hiveMindStatus.TickRateOverrideHz > 0f
            ? hiveMindStatus.TickRateOverrideHz
            : hiveMindStatus.ConfiguredTargetTickHz;
        var cadencePart = hiveMindStatus.AutomaticBackpressureActive && requestedTargetHz > 0f
            ? $"Cadence auto-reduced to {FormatHz(hiveMindStatus.TargetTickHz)} from {FormatHz(requestedTargetHz)}."
            : hiveMindStatus.TargetTickHz > 0f
                ? $"Cadence target {FormatHz(hiveMindStatus.TargetTickHz)}."
                : "Cadence target unavailable.";
        var tickPart = sampleCount == 0
            ? "Tick health: waiting for completed ticks."
            : $"Tick health: {timedOutPct:0.#}% recent ticks timed out and {latePct:0.#}% had late arrivals ({sampleCount} sample{(sampleCount == 1 ? string.Empty : "s")}).";
        var reschedulePart = hiveMindStatus.RescheduleInProgress ? " Reschedule in progress." : string.Empty;
        return $"{tickPart} {cadencePart}{reschedulePart}";
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
