using System.Globalization;

namespace Nbn.Shared;

public static class WorkerCapabilityMath
{
    public static uint ClampPercent(uint value) => Math.Min(value, 100u);

    public static uint EffectiveCpuCores(uint cpuCores, uint limitPercent)
        => (uint)ScaleUnsigned(cpuCores, ClampPercent(limitPercent), ensureMinimumOne: cpuCores > 0 && limitPercent > 0);

    public static float EffectiveCpuScore(float cpuScore, uint limitPercent)
        => ScaleFloat(cpuScore, ClampPercent(limitPercent));

    public static float EffectiveCpuPlacementScore(float cpuScore, uint cpuCores, uint limitPercent)
    {
        var effectiveScore = EffectiveCpuScore(cpuScore, limitPercent);
        if (effectiveScore > 0f)
        {
            return effectiveScore;
        }

        var effectiveCores = EffectiveCpuCores(cpuCores, limitPercent);
        return effectiveCores > 0 ? effectiveCores / 1000f : 0f;
    }

    public static bool HasEffectiveCpuPlacementCapacity(float cpuScore, uint cpuCores, uint limitPercent)
        => EffectiveCpuPlacementScore(cpuScore, cpuCores, limitPercent) > 0f;

    public static float EffectiveGpuScore(float gpuScore, uint limitPercent)
        => ScaleFloat(gpuScore, ClampPercent(limitPercent));

    public static ulong EffectiveRamFreeBytes(
        ulong rawFreeBytes,
        ulong totalBytes,
        ulong processUsedBytes,
        uint limitPercent)
    {
        var limitBytes = LimitBytes(totalBytes, limitPercent);
        if (limitBytes == 0)
        {
            return 0;
        }

        var remainingByLimit = processUsedBytes >= limitBytes ? 0UL : limitBytes - processUsedBytes;
        return Math.Min(rawFreeBytes, remainingByLimit);
    }

    public static ulong EffectiveStorageFreeBytes(
        ulong rawFreeBytes,
        ulong totalBytes,
        uint limitPercent)
    {
        var limitBytes = LimitBytes(totalBytes, limitPercent);
        if (limitBytes == 0)
        {
            return 0;
        }

        return Math.Min(rawFreeBytes, limitBytes);
    }

    public static ulong EffectiveVramFreeBytes(
        ulong rawFreeBytes,
        ulong totalBytes,
        uint limitPercent)
    {
        var limitBytes = LimitBytes(totalBytes, limitPercent);
        if (limitBytes == 0)
        {
            return 0;
        }

        return Math.Min(rawFreeBytes, limitBytes);
    }

    public static ulong LimitBytes(ulong totalBytes, uint limitPercent)
    {
        if (totalBytes == 0)
        {
            return 0;
        }

        var clamped = ClampPercent(limitPercent);
        if (clamped == 0)
        {
            return 0;
        }

        if (clamped >= 100)
        {
            return totalBytes;
        }

        return ScaleUnsigned(totalBytes, clamped);
    }

    public static float ComputeUsedPercent(ulong freeBytes, ulong totalBytes)
    {
        if (totalBytes == 0)
        {
            return 0f;
        }

        var usedBytes = freeBytes >= totalBytes ? 0UL : totalBytes - freeBytes;
        return NormalizePercent((double)usedBytes * 100d / totalBytes);
    }

    public static bool IsCpuOverLimit(float processCpuLoadPercent, uint limitPercent, float tolerancePercent)
    {
        var clampedLimit = ClampPercent(limitPercent);
        if (clampedLimit >= 100)
        {
            return false;
        }

        return processCpuLoadPercent > clampedLimit + Math.Max(0f, tolerancePercent);
    }

    public static bool IsRamOverLimit(
        ulong processRamUsedBytes,
        ulong totalRamBytes,
        uint limitPercent,
        float tolerancePercent)
    {
        var limitBytes = LimitBytes(totalRamBytes, limitPercent);
        if (limitBytes == 0)
        {
            return processRamUsedBytes > 0;
        }

        return processRamUsedBytes > limitBytes + ToleranceBytes(totalRamBytes, tolerancePercent);
    }

    public static bool IsStorageOverLimit(
        ulong storageFreeBytes,
        ulong storageTotalBytes,
        uint limitPercent,
        float tolerancePercent)
    {
        var clampedLimit = ClampPercent(limitPercent);
        if (clampedLimit >= 100)
        {
            return false;
        }

        return ComputeUsedPercent(storageFreeBytes, storageTotalBytes) > clampedLimit + Math.Max(0f, tolerancePercent);
    }

    public static bool IsVramOverLimit(
        ulong vramFreeBytes,
        ulong vramTotalBytes,
        uint limitPercent,
        float tolerancePercent)
    {
        var clampedLimit = ClampPercent(limitPercent);
        if (clampedLimit >= 100)
        {
            return false;
        }

        return ComputeUsedPercent(vramFreeBytes, vramTotalBytes) > clampedLimit + Math.Max(0f, tolerancePercent);
    }

    public static string FormatRatio(double value)
        => Math.Max(0d, value).ToString("0.###", CultureInfo.InvariantCulture);

    private static ulong ScaleUnsigned(ulong value, uint percent, bool ensureMinimumOne = false)
    {
        if (value == 0 || percent == 0)
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

    private static float ScaleFloat(float value, uint percent)
    {
        if (percent == 0)
        {
            return 0f;
        }

        if (percent >= 100)
        {
            return value;
        }

        return value * (percent / 100f);
    }

    private static ulong ToleranceBytes(ulong totalBytes, float tolerancePercent)
    {
        if (totalBytes == 0 || tolerancePercent <= 0f)
        {
            return 0UL;
        }

        return (ulong)Math.Round(totalBytes * (tolerancePercent / 100f), MidpointRounding.AwayFromZero);
    }

    private static float NormalizePercent(double value)
    {
        if (!double.IsFinite(value) || value <= 0d)
        {
            return 0f;
        }

        return (float)Math.Round(Math.Min(100d, value), 3, MidpointRounding.AwayFromZero);
    }
}
