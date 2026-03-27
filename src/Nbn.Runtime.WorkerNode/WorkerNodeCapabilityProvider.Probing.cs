using System.Globalization;
using ILGPU;
using ILGPU.Runtime;
using ILGPU.Runtime.Cuda;
using Nbn.Runtime.RegionHost;

namespace Nbn.Runtime.WorkerNode;

public sealed partial class WorkerNodeCapabilityProvider
{
    private WorkerCapabilityBaseline SafeProbeBaseline()
    {
        try
        {
            return _baselineProbe();
        }
        catch
        {
            return CreateFallbackBaseline();
        }
    }

    private WorkerCapabilityScores SafeProbeScores(WorkerCapabilityBaseline baseline)
    {
        try
        {
            return _scoreProbe(baseline);
        }
        catch
        {
            return WorkerCapabilityScores.Empty;
        }
    }

    private static WorkerCapabilityBaseline ProbeBaseline(string storageProbePath)
    {
        var cpuCores = (uint)Math.Max(1, Environment.ProcessorCount);
        var ramInfo = ProbeRamInfo();
        var storageInfo = ProbeStorageInfo(storageProbePath);
        var gpuInfo = ProbeGpuInfo();

        return new WorkerCapabilityBaseline(
            CpuCores: cpuCores,
            RamFreeBytes: ramInfo.FreeBytes,
            RamTotalBytes: ramInfo.TotalBytes,
            StorageFreeBytes: storageInfo.FreeBytes,
            StorageTotalBytes: storageInfo.TotalBytes,
            HasGpu: gpuInfo.HasGpu,
            GpuName: gpuInfo.GpuName,
            VramFreeBytes: gpuInfo.VramFreeBytes,
            VramTotalBytes: gpuInfo.VramTotalBytes,
            IlgpuCudaAvailable: gpuInfo.IlgpuCudaAvailable,
            IlgpuOpenclAvailable: gpuInfo.IlgpuOpenclAvailable);
    }

    private static WorkerGpuInfo ProbeGpuInfo()
    {
        try
        {
            using var context = Context.CreateDefault();
            var devices = context.Devices.ToArray();
            var gpuDevice = RegionShardGpuRuntime.SelectPreferredGpuDevice(devices);
            if (gpuDevice is null)
            {
                return new WorkerGpuInfo(false, string.Empty, 0, 0, false, false);
            }

            var availability = RegionShardGpuRuntime.ProbeAvailability(devices);
            var gpuMemory = ResolveGpuMemory(context, gpuDevice);
            return new WorkerGpuInfo(
                HasGpu: true,
                GpuName: gpuDevice.Name ?? string.Empty,
                VramFreeBytes: gpuMemory.FreeBytes,
                VramTotalBytes: gpuMemory.TotalBytes,
                IlgpuCudaAvailable: availability.CudaAvailable,
                IlgpuOpenclAvailable: availability.OpenClAvailable);
        }
        catch
        {
            return new WorkerGpuInfo(false, string.Empty, 0, 0, false, false);
        }
    }

    private static WorkerMemoryInfo ResolveGpuMemory(Context context, Device device)
    {
        try
        {
            using var accelerator = device.CreateAccelerator(context);
            if (accelerator is CudaAccelerator cudaAccelerator)
            {
                return new WorkerMemoryInfo(
                    ToUnsignedBytes(cudaAccelerator.GetFreeMemory()),
                    ToUnsignedBytes(accelerator.MemorySize));
            }

            var totalBytes = ToUnsignedBytes(accelerator.MemorySize);
            return new WorkerMemoryInfo(totalBytes, totalBytes);
        }
        catch
        {
            var totalBytes = ToUnsignedBytes(device.MemorySize);
            return new WorkerMemoryInfo(totalBytes, totalBytes);
        }
    }

    private static WorkerMemoryInfo ProbeRamInfo()
    {
        if (OperatingSystem.IsWindows())
        {
            try
            {
                if (GlobalMemoryStatusEx(out var windowsStatus))
                {
                    return new WorkerMemoryInfo(windowsStatus.AvailPhys, windowsStatus.TotalPhys);
                }

                if (GetPerformanceInfo(out var performanceInfo))
                {
                    var availablePages = performanceInfo.PhysicalAvailable;
                    var totalPages = performanceInfo.PhysicalTotal;
                    var pageSize = performanceInfo.PageSize;
                    if (pageSize > 0)
                    {
                        return new WorkerMemoryInfo(
                            availablePages > 0 ? availablePages * pageSize : 0UL,
                            totalPages > 0 ? totalPages * pageSize : 0UL);
                    }
                }
            }
            catch
            {
            }
        }

        if (OperatingSystem.IsLinux()
            && TryReadMemInfoValue("MemAvailable:", out var memAvailableKb)
            && TryReadMemInfoValue("MemTotal:", out var memTotalKb))
        {
            return new WorkerMemoryInfo(memAvailableKb * 1024UL, memTotalKb * 1024UL);
        }

        var gcInfo = GC.GetGCMemoryInfo();
        var totalAvailable = gcInfo.TotalAvailableMemoryBytes;
        var totalBytes = totalAvailable > 0 ? (ulong)totalAvailable : 0UL;
        return new WorkerMemoryInfo(totalBytes, totalBytes);
    }

    private static WorkerMemoryInfo ProbeStorageInfo(string storageProbePath)
    {
        try
        {
            var fullPath = Path.GetFullPath(storageProbePath);
            DriveInfo? drive = null;
            foreach (var candidate in DriveInfo.GetDrives())
            {
                try
                {
                    if (!candidate.IsReady || !fullPath.StartsWith(candidate.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    if (drive is null || candidate.Name.Length > drive.Name.Length)
                    {
                        drive = candidate;
                    }
                }
                catch
                {
                }
            }

            if (drive is null)
            {
                return WorkerMemoryInfo.Empty;
            }

            return new WorkerMemoryInfo(
                ToUnsignedBytes(drive.AvailableFreeSpace),
                ToUnsignedBytes(drive.TotalSize));
        }
        catch
        {
            return WorkerMemoryInfo.Empty;
        }
    }

    private static string ResolveStorageProbePath(string? storageProbePath)
    {
        if (!string.IsNullOrWhiteSpace(storageProbePath))
        {
            return storageProbePath.Trim();
        }

        var artifactRoot = Environment.GetEnvironmentVariable("NBN_ARTIFACT_ROOT");
        if (!string.IsNullOrWhiteSpace(artifactRoot))
        {
            return artifactRoot.Trim();
        }

        return Path.Combine(Environment.CurrentDirectory, "artifacts");
    }

    private static bool TryReadMemInfoValue(string key, out ulong value)
    {
        value = 0;
        try
        {
            foreach (var line in File.ReadLines("/proc/meminfo"))
            {
                if (!line.StartsWith(key, StringComparison.Ordinal))
                {
                    continue;
                }

                var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length >= 2
                    && ulong.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out value))
                {
                    return true;
                }

                return false;
            }
        }
        catch
        {
        }

        return false;
    }

    private static float NormalizeScore(double rawScore)
    {
        if (!double.IsFinite(rawScore) || rawScore <= 0d)
        {
            return 0f;
        }

        return (float)Math.Round(rawScore, 3, MidpointRounding.AwayFromZero);
    }

    private static float NormalizePercent(double rawPercent)
    {
        if (!double.IsFinite(rawPercent) || rawPercent <= 0d)
        {
            return 0f;
        }

        return (float)Math.Round(Math.Min(100d, rawPercent), 3, MidpointRounding.AwayFromZero);
    }

    private static ulong ToUnsignedBytes(long value)
        => value > 0 ? (ulong)value : 0UL;
}
