using ILGPU;
using ILGPU.Runtime;

namespace Nbn.Runtime.RegionHost;

public readonly record struct RegionShardGpuRuntimeAvailability(
    bool IsBackendAvailable,
    bool CudaAvailable,
    bool OpenClAvailable,
    string DeviceName,
    AcceleratorType PreferredAcceleratorType,
    string FailureReason);

public sealed class RegionShardGpuAcceleratorLease : IDisposable
{
    private readonly Context _context;

    public RegionShardGpuAcceleratorLease(Context context, Device device, Accelerator accelerator)
    {
        _context = context;
        Device = device;
        Accelerator = accelerator;
    }

    public Device Device { get; }

    public Accelerator Accelerator { get; }

    public void Dispose()
    {
        Accelerator.Dispose();
        _context.Dispose();
    }
}

public static class RegionShardGpuRuntime
{
    public static RegionShardGpuRuntimeAvailability ProbeAvailability()
    {
        try
        {
            using var context = Context.CreateDefault();
            var devices = context.Devices.ToArray();
            var cudaAvailable = devices.Any(static device => device.AcceleratorType == AcceleratorType.Cuda);
            var openClAvailable = devices.Any(static device => device.AcceleratorType == AcceleratorType.OpenCL);
            var device = SelectPreferredGpuDevice(devices);
            if (device is null)
            {
                return new RegionShardGpuRuntimeAvailability(
                    IsBackendAvailable: false,
                    CudaAvailable: cudaAvailable,
                    OpenClAvailable: openClAvailable,
                    DeviceName: string.Empty,
                    PreferredAcceleratorType: AcceleratorType.CPU,
                    FailureReason: "gpu_not_detected");
            }

            using var accelerator = device.CreateAccelerator(context);
            return new RegionShardGpuRuntimeAvailability(
                IsBackendAvailable: true,
                CudaAvailable: cudaAvailable,
                OpenClAvailable: openClAvailable,
                DeviceName: device.Name ?? string.Empty,
                PreferredAcceleratorType: device.AcceleratorType,
                FailureReason: string.Empty);
        }
        catch (Exception ex)
        {
            return new RegionShardGpuRuntimeAvailability(
                IsBackendAvailable: false,
                CudaAvailable: false,
                OpenClAvailable: false,
                DeviceName: string.Empty,
                PreferredAcceleratorType: AcceleratorType.CPU,
                FailureReason: ex.GetBaseException().Message);
        }
    }

    public static Device? SelectPreferredGpuDevice(IEnumerable<Device> devices)
        => devices
            .Where(static device => device.AcceleratorType is AcceleratorType.Cuda or AcceleratorType.OpenCL)
            .OrderByDescending(static device => device.AcceleratorType == AcceleratorType.Cuda ? 1 : 0)
            .ThenByDescending(static device => device.MemorySize)
            .FirstOrDefault();

    public static bool TryCreatePreferredAccelerator(out RegionShardGpuAcceleratorLease? lease, out RegionShardGpuRuntimeAvailability availability)
    {
        lease = null;
        availability = ProbeAvailability();
        if (!availability.IsBackendAvailable)
        {
            return false;
        }

        try
        {
            var context = Context.CreateDefault();
            var device = SelectPreferredGpuDevice(context.Devices);
            if (device is null)
            {
                context.Dispose();
                availability = availability with { IsBackendAvailable = false, FailureReason = "gpu_not_detected" };
                return false;
            }

            var accelerator = device.CreateAccelerator(context);
            lease = new RegionShardGpuAcceleratorLease(context, device, accelerator);
            return true;
        }
        catch (Exception ex)
        {
            availability = availability with
            {
                IsBackendAvailable = false,
                FailureReason = ex.GetBaseException().Message
            };
            return false;
        }
    }
}
