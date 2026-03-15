namespace Nbn.Runtime.WorkerNode;

public sealed record WorkerResourceAvailability
{
    public const int DefaultPercent = 80;

    public static WorkerResourceAvailability Default { get; } = new(
        DefaultPercent,
        DefaultPercent,
        DefaultPercent,
        DefaultPercent,
        DefaultPercent);

    public WorkerResourceAvailability(
        int cpuPercent,
        int ramPercent,
        int storagePercent,
        int gpuComputePercent,
        int gpuVramPercent)
    {
        CpuPercent = ClampPercent(cpuPercent);
        RamPercent = ClampPercent(ramPercent);
        StoragePercent = ClampPercent(storagePercent);
        GpuComputePercent = ClampPercent(gpuComputePercent);
        GpuVramPercent = ClampPercent(gpuVramPercent);
    }

    public int CpuPercent { get; }

    public int RamPercent { get; }

    public int StoragePercent { get; }

    public int GpuComputePercent { get; }

    public int GpuVramPercent { get; }

    public static int ClampPercent(int value) => Math.Clamp(value, 0, 100);

    public string ToDisplayString()
        => $"cpu={CpuPercent}% ram={RamPercent}% storage={StoragePercent}% gpu-compute={GpuComputePercent}% gpu-vram={GpuVramPercent}%";
}
