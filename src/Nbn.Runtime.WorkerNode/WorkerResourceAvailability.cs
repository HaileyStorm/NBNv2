namespace Nbn.Runtime.WorkerNode;

public sealed record WorkerResourceAvailability
{
    public const int DefaultPercent = 80;

    public static WorkerResourceAvailability Default { get; } = new(DefaultPercent, DefaultPercent, DefaultPercent, DefaultPercent);

    public WorkerResourceAvailability(int cpuPercent, int ramPercent, int storagePercent, int gpuPercent)
    {
        CpuPercent = ClampPercent(cpuPercent);
        RamPercent = ClampPercent(ramPercent);
        StoragePercent = ClampPercent(storagePercent);
        GpuPercent = ClampPercent(gpuPercent);
    }

    public int CpuPercent { get; }

    public int RamPercent { get; }

    public int StoragePercent { get; }

    public int GpuPercent { get; }

    public static int ClampPercent(int value) => Math.Clamp(value, 0, 100);

    public string ToDisplayString()
        => $"cpu={CpuPercent}% ram={RamPercent}% storage={StoragePercent}% gpu={GpuPercent}%";
}
