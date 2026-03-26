using System.Runtime.InteropServices;

namespace Nbn.Runtime.WorkerNode;

public sealed partial class WorkerNodeCapabilityProvider
{
    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
    private struct MemoryStatusEx
    {
        public uint Length;
        public uint MemoryLoad;
        public ulong TotalPhys;
        public ulong AvailPhys;
        public ulong TotalPageFile;
        public ulong AvailPageFile;
        public ulong TotalVirtual;
        public ulong AvailVirtual;
        public ulong AvailExtendedVirtual;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct PerformanceInformation
    {
        public uint Size;
        public ulong CommitTotal;
        public ulong CommitLimit;
        public ulong CommitPeak;
        public ulong PhysicalTotal;
        public ulong PhysicalAvailable;
        public ulong SystemCache;
        public ulong KernelTotal;
        public ulong KernelPaged;
        public ulong KernelNonpaged;
        public ulong PageSize;
        public uint HandleCount;
        public uint ProcessCount;
        public uint ThreadCount;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GlobalMemoryStatusExNative(ref MemoryStatusEx buffer);

    [DllImport("psapi.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetPerformanceInfoNative(out PerformanceInformation performanceInformation, uint size);

    private static bool GlobalMemoryStatusEx(out MemoryStatusEx buffer)
    {
        buffer = new MemoryStatusEx
        {
            Length = (uint)Marshal.SizeOf<MemoryStatusEx>()
        };

        return GlobalMemoryStatusExNative(ref buffer);
    }

    private static bool GetPerformanceInfo(out PerformanceInformation performanceInformation)
        => GetPerformanceInfoNative(out performanceInformation, (uint)Marshal.SizeOf<PerformanceInformation>());
}
