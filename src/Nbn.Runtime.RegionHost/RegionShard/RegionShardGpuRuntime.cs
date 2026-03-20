using ILGPU;
using ILGPU.Runtime;
using ILGPU.Runtime.OpenCL;

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
            return ProbeAvailability(context.Devices);
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

    public static RegionShardGpuRuntimeAvailability ProbeAvailability(IEnumerable<Device> devices)
    {
        var orderedGpuDevices = OrderGpuDevices(devices).ToArray();
        if (orderedGpuDevices.Length == 0)
        {
            return new RegionShardGpuRuntimeAvailability(
                IsBackendAvailable: false,
                CudaAvailable: false,
                OpenClAvailable: false,
                DeviceName: string.Empty,
                PreferredAcceleratorType: AcceleratorType.CPU,
                FailureReason: "gpu_not_detected");
        }

        var cudaAvailable = false;
        var openClAvailable = false;
        Device? compatiblePreferred = null;
        var incompatiblePreferred = orderedGpuDevices[0];
        var failureReason = string.Empty;

        foreach (var device in orderedGpuDevices)
        {
            if (IsCompatibleGpuDevice(device, out var deviceFailureReason))
            {
                if (device.AcceleratorType == AcceleratorType.Cuda)
                {
                    cudaAvailable = true;
                }
                else if (device.AcceleratorType == AcceleratorType.OpenCL)
                {
                    openClAvailable = true;
                }

                compatiblePreferred ??= device;
                continue;
            }

            if (string.IsNullOrWhiteSpace(failureReason))
            {
                incompatiblePreferred = device;
                failureReason = deviceFailureReason;
            }
        }

        if (compatiblePreferred is null)
        {
            return new RegionShardGpuRuntimeAvailability(
                IsBackendAvailable: false,
                CudaAvailable: false,
                OpenClAvailable: false,
                DeviceName: incompatiblePreferred.Name ?? string.Empty,
                PreferredAcceleratorType: incompatiblePreferred.AcceleratorType,
                FailureReason: string.IsNullOrWhiteSpace(failureReason)
                    ? "gpu_accelerator_incompatible"
                    : failureReason);
        }

        return new RegionShardGpuRuntimeAvailability(
            IsBackendAvailable: true,
            CudaAvailable: cudaAvailable,
            OpenClAvailable: openClAvailable,
            DeviceName: compatiblePreferred.Name ?? string.Empty,
            PreferredAcceleratorType: compatiblePreferred.AcceleratorType,
            FailureReason: string.Empty);
    }

    public static Device? SelectPreferredGpuDevice(IEnumerable<Device> devices)
        => OrderGpuDevices(devices).FirstOrDefault();

    public static Device? SelectPreferredCompatibleGpuDevice(IEnumerable<Device> devices)
        => OrderGpuDevices(devices).FirstOrDefault(device => IsCompatibleGpuDevice(device, out _));

    public static bool IsCompatibleGpuDevice(Device device, out string failureReason)
    {
        if (device.AcceleratorType != AcceleratorType.Cuda
            && device.AcceleratorType != AcceleratorType.OpenCL)
        {
            failureReason = "unsupported_gpu_accelerator_type";
            return false;
        }

        if (device is CLDevice openClDevice
            && !openClDevice.Capabilities.Float64)
        {
            failureReason = "opencl_float64_not_supported";
            return false;
        }

        failureReason = string.Empty;
        return true;
    }

    public static bool TryCreatePreferredAccelerator(out RegionShardGpuAcceleratorLease? lease, out RegionShardGpuRuntimeAvailability availability)
    {
        lease = null;
        availability = new RegionShardGpuRuntimeAvailability(
            IsBackendAvailable: false,
            CudaAvailable: false,
            OpenClAvailable: false,
            DeviceName: string.Empty,
            PreferredAcceleratorType: AcceleratorType.CPU,
            FailureReason: string.Empty);
        try
        {
            var context = Context.CreateDefault();
            var devices = context.Devices.ToArray();
            availability = ProbeAvailability(devices);
            if (!availability.IsBackendAvailable)
            {
                context.Dispose();
                return false;
            }

            var device = SelectPreferredCompatibleGpuDevice(devices);
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

    private static IEnumerable<Device> OrderGpuDevices(IEnumerable<Device> devices)
        => devices
            .Where(static device => device.AcceleratorType is AcceleratorType.Cuda or AcceleratorType.OpenCL)
            .OrderByDescending(static device => device.AcceleratorType == AcceleratorType.Cuda ? 1 : 0)
            .ThenByDescending(static device => device.MemorySize);
}
