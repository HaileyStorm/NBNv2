using System.Diagnostics;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Runtime.WorkerNode;

/// <summary>
/// Samples, caches, benchmarks, and scales worker capability snapshots for publication.
/// </summary>
public sealed partial class WorkerNodeCapabilityProvider
{
    private static readonly TimeSpan DefaultProbeRefreshInterval = TimeSpan.FromSeconds(15);

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

    /// <summary>
    /// Creates a capability provider with optional probe overrides for testing or custom host integration.
    /// </summary>
    /// <param name="availability">Availability limits applied to the published capability snapshot.</param>
    /// <param name="storageProbePath">Optional path used to select the storage volume to probe.</param>
    /// <param name="probeRefreshInterval">Optional interval for refreshing the baseline capability sample.</param>
    /// <param name="clock">Optional clock used for sampling and cache invalidation.</param>
    /// <param name="baselineProbe">Optional raw host capability probe.</param>
    /// <param name="scoreProbe">Optional CPU/GPU score benchmark probe.</param>
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

    /// <summary>
    /// Returns the current scaled capability snapshot for this worker process.
    /// </summary>
    public ProtoSettings.NodeCapabilities GetCapabilities()
    {
        lock (_sync)
        {
            var snapshot = CaptureCapabilitySnapshot(_clock());
            var capabilities = CreateCapabilitySnapshot(snapshot.Baseline, snapshot.Scores, snapshot.RuntimeLoad);
            return WorkerCapabilityScaling.ApplyScale(capabilities, _availability);
        }
    }

    /// <summary>
    /// Invalidates cached score benchmarks so the next capability read recomputes them.
    /// </summary>
    public void MarkDirty()
    {
        lock (_sync)
        {
            _scores = null;
        }
    }

    private ProtoSettings.NodeCapabilities CreateCapabilitySnapshot(
        WorkerCapabilityBaseline baseline,
        WorkerCapabilityScores scores,
        WorkerCapabilityRuntimeLoad runtimeLoad)
    {
        return new ProtoSettings.NodeCapabilities
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
    }

    private CapabilitySnapshot CaptureCapabilitySnapshot(DateTimeOffset now)
    {
        var baseline = GetOrRefreshBaseline(now);
        return new CapabilitySnapshot(
            baseline,
            GetOrRefreshScores(baseline),
            SampleRuntimeLoad(now));
    }

    private WorkerCapabilityBaseline GetOrRefreshBaseline(DateTimeOffset now)
    {
        if (_baseline is not null && now - _baselineSampledAt < _probeRefreshInterval)
        {
            return _baseline;
        }

        var previous = _baseline;
        var current = SafeProbeBaseline();
        _baseline = current;
        _baselineSampledAt = now;
        InvalidateScoresIfHardwareProfileChanged(previous, current);

        return current;
    }

    private WorkerCapabilityScores GetOrRefreshScores(WorkerCapabilityBaseline baseline)
    {
        _scores ??= SafeProbeScores(baseline);
        return _scores;
    }

    private void InvalidateScoresIfHardwareProfileChanged(
        WorkerCapabilityBaseline? previous,
        WorkerCapabilityBaseline current)
    {
        if (previous is null || !CanReuseScores(previous, current))
        {
            _scores = null;
        }
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

    private static WorkerCapabilityBaseline CreateFallbackBaseline()
        => new(
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

    /// <summary>
    /// Raw, unscaled worker capabilities sampled directly from the host.
    /// </summary>
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

    /// <summary>
    /// Cached CPU and GPU placement scores published with worker capability snapshots.
    /// </summary>
    public sealed record WorkerCapabilityScores(float CpuScore, float GpuScore)
    {
        /// <summary>
        /// Zero-valued scores used when benchmarking is unavailable or fails.
        /// </summary>
        public static readonly WorkerCapabilityScores Empty = new(0f, 0f);
    }

    private sealed record CapabilitySnapshot(
        WorkerCapabilityBaseline Baseline,
        WorkerCapabilityScores Scores,
        WorkerCapabilityRuntimeLoad RuntimeLoad);

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
}
