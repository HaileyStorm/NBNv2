using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using ILGPU;
using ILGPU.Runtime;
using ILGPU.Runtime.Cuda;
using Nbn.Proto;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.Quantization;
using ProtoSettings = Nbn.Proto.Settings;
using SharedAddress32 = Nbn.Shared.Addressing.Address32;
using SharedShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Runtime.WorkerNode;

public sealed class WorkerNodeCapabilityProvider
{
    private static readonly Guid CpuBenchmarkBrainId = Guid.Parse("4F20A095-A750-4AFE-99FD-3DE769078D0D");
    private static readonly TimeSpan DefaultProbeRefreshInterval = TimeSpan.FromSeconds(15);
    private const int CpuBenchmarkNeuronCount = 2_048;
    private const int CpuBenchmarkFanOut = 4;
    private const int CpuBenchmarkMinimumIterations = 8;
    private static readonly TimeSpan CpuBenchmarkMinimumDuration = TimeSpan.FromMilliseconds(250);
    private const int GpuBenchmarkLength = 1 << 18;
    private const int GpuBenchmarkIterations = 24;
    private static readonly float[] GpuBenchmarkInputA = BuildGpuInput(seedOffset: 0.125f);
    private static readonly float[] GpuBenchmarkInputB = BuildGpuInput(seedOffset: 0.875f);

    private readonly object _sync = new();
    private readonly WorkerResourceAvailability _availability;
    private readonly Func<DateTimeOffset> _clock;
    private readonly Func<WorkerCapabilityBaseline> _baselineProbe;
    private readonly Func<WorkerCapabilityBaseline, WorkerCapabilityScores> _scoreProbe;
    private readonly TimeSpan _probeRefreshInterval;

    private WorkerCapabilityBaseline? _baseline;
    private WorkerCapabilityScores? _scores;
    private DateTimeOffset _baselineSampledAt;
    private ProcessCpuSample? _lastProcessCpuSample;

    public WorkerNodeCapabilityProvider(
        WorkerResourceAvailability? availability = null,
        string? storageProbePath = null,
        TimeSpan? probeRefreshInterval = null,
        Func<DateTimeOffset>? clock = null,
        Func<WorkerCapabilityBaseline>? baselineProbe = null,
        Func<WorkerCapabilityBaseline, WorkerCapabilityScores>? scoreProbe = null)
    {
        _availability = availability ?? WorkerResourceAvailability.Default;
        _clock = clock ?? (static () => DateTimeOffset.UtcNow);
        _probeRefreshInterval = probeRefreshInterval ?? DefaultProbeRefreshInterval;

        var resolvedStorageProbePath = ResolveStorageProbePath(storageProbePath);
        _baselineProbe = baselineProbe ?? (() => ProbeBaseline(resolvedStorageProbePath));
        _scoreProbe = scoreProbe ?? BenchmarkScores;
    }

    public ProtoSettings.NodeCapabilities GetCapabilities()
    {
        lock (_sync)
        {
            var now = _clock();
            var baseline = GetOrRefreshBaseline(now);
            var scores = GetOrRefreshScores(baseline);
            var runtimeLoad = SampleRuntimeLoad(now);

            var capabilities = new ProtoSettings.NodeCapabilities
            {
                CpuCores = baseline.CpuCores,
                RamFreeBytes = baseline.RamFreeBytes,
                StorageFreeBytes = baseline.StorageFreeBytes,
                HasGpu = baseline.HasGpu,
                GpuName = baseline.GpuName,
                VramFreeBytes = baseline.VramFreeBytes,
                CpuScore = scores.CpuScore,
                GpuScore = scores.GpuScore,
                IlgpuCudaAvailable = baseline.IlgpuCudaAvailable,
                IlgpuOpenclAvailable = baseline.IlgpuOpenclAvailable,
                RamTotalBytes = baseline.RamTotalBytes,
                StorageTotalBytes = baseline.StorageTotalBytes,
                VramTotalBytes = baseline.VramTotalBytes,
                CpuLimitPercent = (uint)_availability.CpuPercent,
                RamLimitPercent = (uint)_availability.RamPercent,
                StorageLimitPercent = (uint)_availability.StoragePercent,
                GpuComputeLimitPercent = (uint)_availability.GpuComputePercent,
                GpuVramLimitPercent = (uint)_availability.GpuVramPercent,
                ProcessCpuLoadPercent = runtimeLoad.ProcessCpuLoadPercent,
                ProcessRamUsedBytes = runtimeLoad.ProcessRamUsedBytes
            };

            return WorkerCapabilityScaling.ApplyScale(capabilities, _availability);
        }
    }

    public void MarkDirty()
    {
        lock (_sync)
        {
            _scores = null;
        }
    }

    private WorkerCapabilityBaseline GetOrRefreshBaseline(DateTimeOffset now)
    {
        if (_baseline is not null && now - _baselineSampledAt < _probeRefreshInterval)
        {
            return _baseline;
        }

        var previous = _baseline;
        _baseline = SafeProbeBaseline();
        _baselineSampledAt = now;
        if (previous is null || !CanReuseScores(previous, _baseline))
        {
            _scores = null;
        }

        return _baseline;
    }

    private WorkerCapabilityScores GetOrRefreshScores(WorkerCapabilityBaseline baseline)
    {
        if (_scores is not null)
        {
            return _scores;
        }

        _scores = SafeProbeScores(baseline);
        return _scores;
    }

    private WorkerCapabilityRuntimeLoad SampleRuntimeLoad(DateTimeOffset now)
    {
        using var process = Process.GetCurrentProcess();
        var processCpuTime = process.TotalProcessorTime;
        var processRamUsedBytes = ToUnsignedBytes(process.WorkingSet64);

        var cpuLoadPercent = 0f;
        if (_lastProcessCpuSample is ProcessCpuSample previousSample)
        {
            var elapsed = now - previousSample.Timestamp;
            if (elapsed > TimeSpan.Zero && Environment.ProcessorCount > 0)
            {
                var cpuDelta = processCpuTime - previousSample.ProcessCpuTime;
                var denominator = elapsed.TotalMilliseconds * Environment.ProcessorCount;
                if (cpuDelta > TimeSpan.Zero && denominator > 0)
                {
                    cpuLoadPercent = NormalizePercent(cpuDelta.TotalMilliseconds * 100d / denominator);
                }
            }
        }

        _lastProcessCpuSample = new ProcessCpuSample(now, processCpuTime);
        return new WorkerCapabilityRuntimeLoad(cpuLoadPercent, processRamUsedBytes);
    }

    private static bool CanReuseScores(WorkerCapabilityBaseline left, WorkerCapabilityBaseline right)
        => left.CpuCores == right.CpuCores
           && left.HasGpu == right.HasGpu
           && string.Equals(left.GpuName, right.GpuName, StringComparison.Ordinal)
           && left.IlgpuCudaAvailable == right.IlgpuCudaAvailable
           && left.IlgpuOpenclAvailable == right.IlgpuOpenclAvailable;

    private WorkerCapabilityBaseline SafeProbeBaseline()
    {
        try
        {
            return _baselineProbe();
        }
        catch
        {
            return new WorkerCapabilityBaseline(
                CpuCores: (uint)Math.Max(1, Environment.ProcessorCount),
                RamFreeBytes: 0,
                RamTotalBytes: 0,
                StorageFreeBytes: 0,
                StorageTotalBytes: 0,
                HasGpu: false,
                GpuName: string.Empty,
                VramFreeBytes: 0,
                VramTotalBytes: 0,
                IlgpuCudaAvailable: false,
                IlgpuOpenclAvailable: false);
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

    private static WorkerCapabilityScores BenchmarkScores(WorkerCapabilityBaseline baseline)
    {
        var cpuScore = BenchmarkCpuScore();
        var gpuScore = baseline.HasGpu ? BenchmarkGpuScore() : 0f;
        return new WorkerCapabilityScores(cpuScore, gpuScore);
    }

    private static float BenchmarkCpuScore()
    {
        var state = BuildCpuBenchmarkState(CpuBenchmarkNeuronCount, CpuBenchmarkFanOut);
        var backend = new RegionShardCpuBackend(state);
        var shardId = SharedShardId32.From(state.RegionId, shardIndex: 0);
        var routing = RegionShardRoutingTable.CreateSingleShard(state.RegionId, state.NeuronCount);
        var tickId = 1UL;

        for (var i = 0; i < 3; i++)
        {
            backend.Compute(tickId++, CpuBenchmarkBrainId, shardId, routing);
        }

        var iterations = 0;
        var stopwatch = Stopwatch.StartNew();
        while (iterations < CpuBenchmarkMinimumIterations || stopwatch.Elapsed < CpuBenchmarkMinimumDuration)
        {
            backend.Compute(tickId++, CpuBenchmarkBrainId, shardId, routing);
            iterations++;
        }

        stopwatch.Stop();
        var weightedOperations = (double)iterations * state.NeuronCount * (1 + CpuBenchmarkFanOut);
        return NormalizeScore(weightedOperations / Math.Max(stopwatch.Elapsed.TotalSeconds, 0.001d) / 1_000_000d);
    }

    private static float BenchmarkGpuScore()
    {
        try
        {
            using var context = Context.CreateDefault();
            var device = SelectPreferredGpuDevice(context.Devices);
            if (device is null)
            {
                return 0f;
            }

            using var accelerator = device.CreateAccelerator(context);
            using var bufferA = accelerator.Allocate1D<float>(GpuBenchmarkLength);
            using var bufferB = accelerator.Allocate1D<float>(GpuBenchmarkLength);
            using var bufferC = accelerator.Allocate1D<float>(GpuBenchmarkLength);

            bufferA.CopyFromCPU(GpuBenchmarkInputA);
            bufferB.CopyFromCPU(GpuBenchmarkInputB);

            var kernel = accelerator.LoadAutoGroupedStreamKernel<
                Index1D,
                ArrayView1D<float, Stride1D.Dense>,
                ArrayView1D<float, Stride1D.Dense>,
                ArrayView1D<float, Stride1D.Dense>>(GpuAddKernel);

            kernel(GpuBenchmarkLength, bufferA.View, bufferB.View, bufferC.View);
            accelerator.Synchronize();

            var stopwatch = Stopwatch.StartNew();
            for (var i = 0; i < GpuBenchmarkIterations; i++)
            {
                kernel(GpuBenchmarkLength, bufferA.View, bufferB.View, bufferC.View);
            }

            accelerator.Synchronize();
            stopwatch.Stop();

            var throughput = (double)GpuBenchmarkLength * GpuBenchmarkIterations
                             / Math.Max(stopwatch.Elapsed.TotalSeconds, 0.001d)
                             / 1_000_000d;
            return NormalizeScore(throughput);
        }
        catch
        {
            return 0f;
        }
    }

    private static void GpuAddKernel(
        Index1D index,
        ArrayView1D<float, Stride1D.Dense> inputA,
        ArrayView1D<float, Stride1D.Dense> inputB,
        ArrayView1D<float, Stride1D.Dense> output)
    {
        output[index] = inputA[index] + inputB[index];
    }

    private static WorkerGpuInfo ProbeGpuInfo()
    {
        try
        {
            using var context = Context.CreateDefault();
            var devices = context.Devices.ToArray();
            var cudaAvailable = devices.Any(static device => device.AcceleratorType == AcceleratorType.Cuda);
            var openclAvailable = devices.Any(static device => device.AcceleratorType == AcceleratorType.OpenCL);
            var gpuDevice = SelectPreferredGpuDevice(devices);
            if (gpuDevice is null)
            {
                return new WorkerGpuInfo(false, string.Empty, 0, 0, cudaAvailable, openclAvailable);
            }

            var gpuMemory = ResolveGpuMemory(context, gpuDevice);
            return new WorkerGpuInfo(
                HasGpu: true,
                GpuName: gpuDevice.Name ?? string.Empty,
                VramFreeBytes: gpuMemory.FreeBytes,
                VramTotalBytes: gpuMemory.TotalBytes,
                IlgpuCudaAvailable: cudaAvailable,
                IlgpuOpenclAvailable: openclAvailable);
        }
        catch
        {
            return new WorkerGpuInfo(false, string.Empty, 0, 0, false, false);
        }
    }

    private static Device? SelectPreferredGpuDevice(IEnumerable<Device> devices)
        => devices
            .Where(static device => device.AcceleratorType is AcceleratorType.Cuda or AcceleratorType.OpenCL)
            .OrderByDescending(static device => device.AcceleratorType == AcceleratorType.Cuda ? 1 : 0)
            .ThenByDescending(static device => device.MemorySize)
            .FirstOrDefault();

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

    private static float[] BuildGpuInput(float seedOffset)
    {
        var values = new float[GpuBenchmarkLength];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = (i % 97) * 0.01f + seedOffset;
        }

        return values;
    }

    private static RegionShardState BuildCpuBenchmarkState(int neuronCount, int fanOut)
    {
        const int regionId = 4;
        var regionSpans = new int[NbnConstants.RegionCount];
        regionSpans[regionId] = neuronCount;
        regionSpans[NbnConstants.OutputRegionId] = 1;

        var totalAxons = neuronCount * fanOut;
        var targetRegionIds = new byte[totalAxons];
        var targetNeuronIds = new int[totalAxons];
        var strengths = new float[totalAxons];
        var baseStrengthCodes = new byte[totalAxons];
        var runtimeStrengthCodes = new byte[totalAxons];
        var hasRuntimeOverlay = new bool[totalAxons];
        var fromAddress32 = new uint[totalAxons];
        var toAddress32 = new uint[totalAxons];

        var axonStarts = new int[neuronCount];
        var axonCounts = new ushort[neuronCount];

        var axonIndex = 0;
        for (var neuronId = 0; neuronId < neuronCount; neuronId++)
        {
            axonStarts[neuronId] = axonIndex;
            axonCounts[neuronId] = (ushort)fanOut;
            var sourceAddress = SharedAddress32.From(regionId, neuronId).Value;

            for (var fanOutIndex = 0; fanOutIndex < fanOut; fanOutIndex++)
            {
                var targetRegionId = fanOutIndex == fanOut - 1
                    ? NbnConstants.OutputRegionId
                    : regionId;
                var targetNeuronId = targetRegionId == regionId
                    ? (neuronId + fanOutIndex + 1) % neuronCount
                    : 0;
                var strength = 0.55f + (fanOutIndex * 0.1f);
                var strengthCode = (byte)QuantizationSchemas.DefaultNbn.Strength.Encode(strength, bits: 5);

                targetRegionIds[axonIndex] = (byte)targetRegionId;
                targetNeuronIds[axonIndex] = targetNeuronId;
                strengths[axonIndex] = strength;
                baseStrengthCodes[axonIndex] = strengthCode;
                runtimeStrengthCodes[axonIndex] = strengthCode;
                hasRuntimeOverlay[axonIndex] = false;
                fromAddress32[axonIndex] = sourceAddress;
                toAddress32[axonIndex] = SharedAddress32.From(targetRegionId, targetNeuronId).Value;
                axonIndex++;
            }
        }

        return new RegionShardState(
            regionId: regionId,
            neuronStart: 0,
            neuronCount: neuronCount,
            brainSeed: 0x0102030405060708UL,
            strengthQuantization: QuantizationSchemas.DefaultNbn.Strength,
            regionSpans: regionSpans,
            buffer: Enumerable.Repeat(0.75f, neuronCount).ToArray(),
            enabled: Enumerable.Repeat(true, neuronCount).ToArray(),
            exists: Enumerable.Repeat(true, neuronCount).ToArray(),
            accumulationFunctions: Enumerable.Repeat((byte)AccumulationFunction.AccumSum, neuronCount).ToArray(),
            activationFunctions: Enumerable.Repeat((byte)ActivationFunction.ActRelu, neuronCount).ToArray(),
            resetFunctions: Enumerable.Repeat((byte)ResetFunction.ResetHold, neuronCount).ToArray(),
            paramA: new float[neuronCount],
            paramB: new float[neuronCount],
            preActivationThreshold: Enumerable.Repeat(-1f, neuronCount).ToArray(),
            activationThreshold: Enumerable.Repeat(0.1f, neuronCount).ToArray(),
            axonCounts: axonCounts,
            axonStartOffsets: axonStarts,
            axons: new RegionShardAxons(
                targetRegionIds: targetRegionIds,
                targetNeuronIds: targetNeuronIds,
                strengths: strengths,
                baseStrengthCodes: baseStrengthCodes,
                runtimeStrengthCodes: runtimeStrengthCodes,
                hasRuntimeOverlay: hasRuntimeOverlay,
                fromAddress32: fromAddress32,
                toAddress32: toAddress32));
    }

    public sealed record WorkerCapabilityBaseline(
        uint CpuCores,
        ulong RamFreeBytes,
        ulong RamTotalBytes,
        ulong StorageFreeBytes,
        ulong StorageTotalBytes,
        bool HasGpu,
        string GpuName,
        ulong VramFreeBytes,
        ulong VramTotalBytes,
        bool IlgpuCudaAvailable,
        bool IlgpuOpenclAvailable);

    public sealed record WorkerCapabilityScores(float CpuScore, float GpuScore)
    {
        public static readonly WorkerCapabilityScores Empty = new(0f, 0f);
    }

    private sealed record WorkerCapabilityRuntimeLoad(float ProcessCpuLoadPercent, ulong ProcessRamUsedBytes);

    private sealed record WorkerGpuInfo(
        bool HasGpu,
        string GpuName,
        ulong VramFreeBytes,
        ulong VramTotalBytes,
        bool IlgpuCudaAvailable,
        bool IlgpuOpenclAvailable);

    private sealed record WorkerMemoryInfo(ulong FreeBytes, ulong TotalBytes)
    {
        public static readonly WorkerMemoryInfo Empty = new(0, 0);
    }

    private sealed record ProcessCpuSample(DateTimeOffset Timestamp, TimeSpan ProcessCpuTime);

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
