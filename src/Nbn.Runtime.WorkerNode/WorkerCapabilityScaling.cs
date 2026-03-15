using Nbn.Proto.Settings;
using Nbn.Shared;

namespace Nbn.Runtime.WorkerNode;

public static class WorkerCapabilityScaling
{
    public static NodeCapabilities BuildScaledCapabilities(
        WorkerResourceAvailability? availability = null,
        string? storageProbePath = null)
        => new WorkerNodeCapabilityProvider(
            availability: availability,
            storageProbePath: storageProbePath)
            .GetCapabilities();

    public static NodeCapabilities ApplyScale(NodeCapabilities baseline, WorkerResourceAvailability? availability = null)
    {
        ArgumentNullException.ThrowIfNull(baseline);

        var effectiveAvailability = availability ?? WorkerResourceAvailability.Default;
        var scaled = baseline.Clone();
        scaled.CpuLimitPercent = (uint)effectiveAvailability.CpuPercent;
        scaled.RamLimitPercent = (uint)effectiveAvailability.RamPercent;
        scaled.StorageLimitPercent = (uint)effectiveAvailability.StoragePercent;
        scaled.GpuComputeLimitPercent = (uint)effectiveAvailability.GpuComputePercent;
        scaled.GpuVramLimitPercent = (uint)effectiveAvailability.GpuVramPercent;
        return scaled;
    }
}
