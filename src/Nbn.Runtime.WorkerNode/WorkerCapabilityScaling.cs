using Nbn.Proto.Settings;
using Nbn.Shared;

namespace Nbn.Runtime.WorkerNode;

public static class WorkerCapabilityScaling
{
    public static NodeCapabilities BuildScaledCapabilities(WorkerResourceAvailability? availability = null)
        => ApplyScale(SettingsMonitorReporter.BuildDefaultCapabilities(), availability);

    public static NodeCapabilities ApplyScale(NodeCapabilities baseline, WorkerResourceAvailability? availability = null)
    {
        ArgumentNullException.ThrowIfNull(baseline);

        var effectiveAvailability = availability ?? WorkerResourceAvailability.Default;
        var scaled = baseline.Clone();

        scaled.CpuCores = (uint)ScaleUnsigned(
            baseline.CpuCores,
            effectiveAvailability.CpuPercent,
            ensureMinimumOne: baseline.CpuCores > 0 && effectiveAvailability.CpuPercent > 0);
        scaled.CpuScore = ScaleFloat(baseline.CpuScore, effectiveAvailability.CpuPercent);
        scaled.RamFreeBytes = ScaleUnsigned(baseline.RamFreeBytes, effectiveAvailability.RamPercent);
        scaled.StorageFreeBytes = ScaleUnsigned(baseline.StorageFreeBytes, effectiveAvailability.StoragePercent);

        if (!baseline.HasGpu || effectiveAvailability.GpuPercent <= 0)
        {
            scaled.HasGpu = false;
            scaled.GpuName = string.Empty;
            scaled.VramFreeBytes = 0;
            scaled.GpuScore = 0f;
            scaled.IlgpuCudaAvailable = false;
            scaled.IlgpuOpenclAvailable = false;
        }
        else
        {
            scaled.HasGpu = true;
            scaled.VramFreeBytes = ScaleUnsigned(baseline.VramFreeBytes, effectiveAvailability.GpuPercent);
            scaled.GpuScore = ScaleFloat(baseline.GpuScore, effectiveAvailability.GpuPercent);
        }

        return scaled;
    }

    private static ulong ScaleUnsigned(ulong value, int percent, bool ensureMinimumOne = false)
    {
        if (value == 0 || percent <= 0)
        {
            return 0;
        }

        if (percent >= 100)
        {
            return value;
        }

        var scaled = (ulong)Math.Floor(value * (percent / 100d));
        if (ensureMinimumOne && scaled == 0)
        {
            return 1;
        }

        return scaled;
    }

    private static float ScaleFloat(float value, int percent)
    {
        if (percent <= 0)
        {
            return 0f;
        }

        if (percent >= 100)
        {
            return value;
        }

        return value * (percent / 100f);
    }
}
