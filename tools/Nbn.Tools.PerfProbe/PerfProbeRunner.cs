using System.Diagnostics;
using System.Runtime.InteropServices;
using Nbn.Proto.Control;
using Nbn.Runtime.HiveMind;
using Nbn.Runtime.RegionHost;
using Nbn.Runtime.WorkerNode;
using Nbn.Shared;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Tools.PerfProbe;

/// <summary>
/// Orchestrates PerfProbe scenario execution and shared result shaping.
/// </summary>
public static partial class PerfProbeRunner
{
    private const int DefaultInputWidth = 8;
    private const int DefaultOutputWidth = 8;
    private const int MaxPackedAxonCount = 0x1FF;
    private const float SuccessThresholdRatio = 0.95f;
    private const string RuntimeExecutionGpuSkipReason = "regionshard_gpu_backend_not_available";
    private const string GpuPlannerWorkloadBelowThresholdSkipReason = "gpu_planner_workload_below_threshold";
    private const string GpuRuntimeWorkloadBelowThresholdSkipReason = "gpu_runtime_workload_below_threshold";

    /// <summary>
    /// Runs the requested PerfProbe mode and returns the aggregated report.
    /// </summary>
    public static async Task<PerfReport> RunAsync(
        string mode,
        PerfProbeConfig config,
        CancellationToken cancellationToken = default)
    {
        var totalStopwatch = Stopwatch.StartNew();
        var normalizedMode = string.IsNullOrWhiteSpace(mode)
            ? "all"
            : mode.Trim().ToLowerInvariant();
        var scenarios = new List<PerfScenarioResult>();

        if (normalizedMode is "all" or "worker-profile")
        {
            scenarios.AddRange(await RunWorkerProfileScenariosAsync(config.WorkerProfile, cancellationToken).ConfigureAwait(false));
        }

        if (normalizedMode is "all" or "localhost-stress")
        {
            scenarios.AddRange(await RunLocalhostStressScenariosAsync(config.LocalhostStress, cancellationToken).ConfigureAwait(false));
        }

        totalStopwatch.Stop();
        return new PerfReport(
            ToolName: "Nbn.Tools.PerfProbe",
            GeneratedAtUtc: DateTimeOffset.UtcNow,
            Environment: BuildEnvironmentSnapshot(),
            Scenarios: scenarios)
        {
            TotalDurationMs = totalStopwatch.Elapsed.TotalMilliseconds
        };
    }

    /// <summary>
    /// Runs the placement-facing worker-profile scenarios.
    /// </summary>
    public static Task<IReadOnlyList<PerfScenarioResult>> RunWorkerProfileScenariosAsync(
        WorkerProfileConfig config,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var capabilities = new WorkerNodeCapabilityProvider().GetCapabilities();
        return Task.FromResult(BuildWorkerProfileScenarios(config, capabilities));
    }

    internal static IReadOnlyList<PerfScenarioResult> BuildWorkerProfileScenarios(
        WorkerProfileConfig config,
        ProtoSettings.NodeCapabilities capabilities)
    {
        var results = new List<PerfScenarioResult>();
        results.Add(TimeScenario(() => new PerfScenarioResult(
            Suite: "worker_profile",
            Scenario: "capability_probe",
            Backend: "cpu",
            Status: PerfScenarioStatus.Passed,
            Summary: "Recorded real worker-node capability telemetry for placement-facing CPU resources.",
            Parameters: new Dictionary<string, string>
            {
                ["planner_worker_count"] = config.PlannerWorkerCount.ToString(),
                ["planner_iterations"] = config.PlannerIterations.ToString()
            },
            Metrics: new Dictionary<string, double>
            {
                ["cpu_score"] = capabilities.CpuScore,
                ["cpu_cores"] = capabilities.CpuCores,
                ["ram_free_bytes"] = capabilities.RamFreeBytes,
                ["ram_total_bytes"] = capabilities.RamTotalBytes,
                ["storage_free_bytes"] = capabilities.StorageFreeBytes,
                ["storage_total_bytes"] = capabilities.StorageTotalBytes,
                ["cpu_limit_percent"] = capabilities.CpuLimitPercent,
                ["ram_limit_percent"] = capabilities.RamLimitPercent,
                ["storage_limit_percent"] = capabilities.StorageLimitPercent,
                ["process_cpu_load_percent"] = capabilities.ProcessCpuLoadPercent,
                ["process_ram_used_bytes"] = capabilities.ProcessRamUsedBytes
            })));

        if (HasCompatibleGpuCapabilities(capabilities))
        {
            results.Add(TimeScenario(() => new PerfScenarioResult(
                Suite: "worker_profile",
                Scenario: "capability_probe",
                Backend: "gpu",
                Status: PerfScenarioStatus.Passed,
                Summary: "Recorded GPU accelerator telemetry and score for future worker placement decisions.",
                Parameters: new Dictionary<string, string>
                {
                    ["gpu_name"] = capabilities.GpuName,
                    ["ilgpu_cuda_available"] = capabilities.IlgpuCudaAvailable.ToString(),
                    ["ilgpu_opencl_available"] = capabilities.IlgpuOpenclAvailable.ToString()
                },
                Metrics: new Dictionary<string, double>
                {
                    ["gpu_score"] = capabilities.GpuScore,
                    ["vram_free_bytes"] = capabilities.VramFreeBytes,
                    ["vram_total_bytes"] = capabilities.VramTotalBytes,
                    ["gpu_compute_limit_percent"] = capabilities.GpuComputeLimitPercent,
                    ["gpu_vram_limit_percent"] = capabilities.GpuVramLimitPercent
                })));
        }
        else
        {
            results.Add(TimeScenario(() => new PerfScenarioResult(
                Suite: "worker_profile",
                Scenario: "capability_probe",
                Backend: "gpu",
                Status: PerfScenarioStatus.Skipped,
                Summary: "GPU capability probe is prepared but no usable accelerator score was available on this host.",
                Parameters: new Dictionary<string, string>
                {
                    ["has_gpu"] = capabilities.HasGpu.ToString(),
                    ["ilgpu_cuda_available"] = capabilities.IlgpuCudaAvailable.ToString(),
                    ["ilgpu_opencl_available"] = capabilities.IlgpuOpenclAvailable.ToString()
                },
                Metrics: new Dictionary<string, double>(),
                SkipReason: ResolveGpuCapabilitySkipReason(capabilities))));
        }

        results.Add(TimeScenario(() => RunPlacementPlannerScenario(
            config,
            capabilities,
            RegionShardComputeBackendPreference.Cpu,
            backend: "cpu",
            summary: "Profiled HiveMind placement planning throughput with CPU compute preference using real local worker capability telemetry as the baseline.")));

        if (HasCompatibleGpuCapabilities(capabilities))
        {
            if (CanExerciseGpuPlannerProfile(config))
            {
                results.Add(TimeScenario(() => RunPlacementPlannerScenario(
                    config,
                    capabilities,
                    RegionShardComputeBackendPreference.Gpu,
                    backend: "gpu",
                    summary: "Profiled HiveMind placement planning throughput with GPU compute preference using real local worker capability telemetry as the baseline.")));
            }
            else
            {
                results.Add(TimeScenario(() => BuildGpuPlannerWorkloadSkipResult(config)));
            }
        }
        else
        {
            results.Add(TimeScenario(() => BuildGpuPlacementPlannerSkipResult(capabilities)));
        }

        return results;
    }

    private static PerfScenarioResult RunPlacementPlannerScenario(
        WorkerProfileConfig config,
        ProtoSettings.NodeCapabilities capabilities,
        RegionShardComputeBackendPreference computeBackendPreference,
        string backend,
        string summary)
    {
        var workers = BuildPlannerWorkers(config.PlannerWorkerCount, capabilities);
        var maxNeuronsPerShard = ResolvePlannerMaxNeuronsPerShard();
        var inputs = new PlacementPlanner.PlannerInputs(
            BrainId: Guid.Parse("9B60F377-6DAE-4FD9-90C1-A0D10B08D978"),
            PlacementEpoch: 1,
            RequestId: "perf-probe",
            RequestedMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            PlannedMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            WorkerSnapshotMs: (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            ShardStride: NbnConstants.DefaultAxonStride,
            RequestedShardPlan: new ShardPlan
            {
                Mode = ShardPlanMode.ShardPlanMaxNeurons,
                MaxNeuronsPerShard = (uint)maxNeuronsPerShard
            },
            Regions:
            [
                new PlacementPlanner.RegionSpan(NbnConstants.InputRegionId, DefaultInputWidth),
                new PlacementPlanner.RegionSpan(1, config.HiddenRegionNeurons),
                new PlacementPlanner.RegionSpan(NbnConstants.OutputRegionId, DefaultOutputWidth)
            ],
            CurrentWorkerNodeIds: Array.Empty<Guid>(),
            ComputeBackendPreference: computeBackendPreference);

        for (var i = 0; i < 5; i++)
        {
            PlacementPlanner.TryBuildPlan(inputs with { RequestId = $"warmup-{i}" }, workers, out _, out _, out _);
        }

        var stopwatch = Stopwatch.StartNew();
        PlacementPlanner.PlacementPlanningResult? lastPlan = null;
        for (var i = 0; i < config.PlannerIterations; i++)
        {
            var ok = PlacementPlanner.TryBuildPlan(
                inputs with { RequestId = $"profile-{i}" },
                workers,
                out var plan,
                out var failureReason,
                out var failureMessage);
            if (!ok)
            {
                return new PerfScenarioResult(
                    Suite: "worker_profile",
                    Scenario: "placement_planner_profile",
                    Backend: backend,
                    Status: PerfScenarioStatus.Failed,
                    Summary: "PlacementPlanner profiling failed before completing the requested iteration sweep.",
                    Parameters: new Dictionary<string, string>
                    {
                        ["planner_iterations"] = config.PlannerIterations.ToString(),
                        ["planner_worker_count"] = config.PlannerWorkerCount.ToString(),
                        ["hidden_region_neurons"] = config.HiddenRegionNeurons.ToString(),
                        ["max_neurons_per_shard"] = maxNeuronsPerShard.ToString(),
                        ["compute_backend_preference"] = computeBackendPreference.ToString().ToLowerInvariant()
                    },
                    Metrics: new Dictionary<string, double>(),
                    Failure: $"{failureReason}: {failureMessage}");
            }

            lastPlan = plan;
        }

        stopwatch.Stop();
        var plansPerSecond = config.PlannerIterations / Math.Max(stopwatch.Elapsed.TotalSeconds, 0.001d);
        return new PerfScenarioResult(
            Suite: "worker_profile",
            Scenario: "placement_planner_profile",
            Backend: backend,
            Status: PerfScenarioStatus.Passed,
            Summary: summary,
            Parameters: new Dictionary<string, string>
            {
                ["planner_iterations"] = config.PlannerIterations.ToString(),
                ["planner_worker_count"] = config.PlannerWorkerCount.ToString(),
                ["hidden_region_neurons"] = config.HiddenRegionNeurons.ToString(),
                ["max_neurons_per_shard"] = maxNeuronsPerShard.ToString(),
                ["compute_backend_preference"] = computeBackendPreference.ToString().ToLowerInvariant()
            },
            Metrics: new Dictionary<string, double>
            {
                ["plans_per_second"] = plansPerSecond,
                ["assignment_count"] = lastPlan?.Assignments.Count ?? 0,
                ["eligible_workers"] = lastPlan?.EligibleWorkers.Count ?? 0
            });
    }

    private static bool HasCompatibleGpuCapabilities(ProtoSettings.NodeCapabilities capabilities)
        => capabilities.HasGpu
           && capabilities.GpuScore > 0f
           && (capabilities.IlgpuCudaAvailable || capabilities.IlgpuOpenclAvailable);

    private static bool CanExerciseGpuPlannerProfile(WorkerProfileConfig config)
        => config.HiddenRegionNeurons >= ResolvePlannerMaxNeuronsPerShard();

    private static int ResolvePlannerMaxNeuronsPerShard()
        => Math.Max(4096, NbnConstants.DefaultAxonStride * 2);

    private static int ResolveRuntimeGpuNeuronThreshold()
        => Math.Max(4096, NbnConstants.DefaultAxonStride * 2);

    private static LocalhostStressConfig BuildGpuLocalhostStressConfig(LocalhostStressConfig config)
    {
        var threshold = ResolveRuntimeGpuNeuronThreshold();
        var filteredBrainSizes = config.BrainSizes
            .Where(size => size >= threshold)
            .OrderBy(static size => size)
            .ToArray();
        return config with
        {
            BrainSizes = filteredBrainSizes,
            SustainableWorkloadNeurons = Math.Max(config.SustainableWorkloadNeurons, threshold)
        };
    }

    private static PerfScenarioResult BuildGpuPlacementPlannerSkipResult(ProtoSettings.NodeCapabilities capabilities)
        => new(
            Suite: "worker_profile",
            Scenario: "placement_planner_profile",
            Backend: "gpu",
            Status: PerfScenarioStatus.Skipped,
            Summary: "GPU placement planner profiling is skipped because no compatible ILGPU worker capability snapshot was available on this host.",
            Parameters: new Dictionary<string, string>
            {
                ["has_gpu"] = capabilities.HasGpu.ToString(),
                ["ilgpu_cuda_available"] = capabilities.IlgpuCudaAvailable.ToString(),
                ["ilgpu_opencl_available"] = capabilities.IlgpuOpenclAvailable.ToString()
            },
            Metrics: new Dictionary<string, double>(),
            SkipReason: ResolveGpuCapabilitySkipReason(capabilities));

    private static PerfScenarioResult BuildGpuPlannerWorkloadSkipResult(WorkerProfileConfig config)
        => new(
            Suite: "worker_profile",
            Scenario: "placement_planner_profile",
            Backend: "gpu",
            Status: PerfScenarioStatus.Skipped,
            Summary: "GPU placement planner profiling is skipped because the configured hidden-region workload never crosses the planner's GPU preference threshold.",
            Parameters: new Dictionary<string, string>
            {
                ["hidden_region_neurons"] = config.HiddenRegionNeurons.ToString(),
                ["gpu_planner_neuron_threshold"] = ResolvePlannerMaxNeuronsPerShard().ToString()
            },
            Metrics: new Dictionary<string, double>(),
            SkipReason: GpuPlannerWorkloadBelowThresholdSkipReason);

    private static PerfScenarioResult BuildGpuRuntimeCapabilitySkipResult(
        string suite,
        string scenario,
        ProtoSettings.NodeCapabilities capabilities)
        => new(
            Suite: suite,
            Scenario: scenario,
            Backend: "gpu",
            Status: PerfScenarioStatus.Skipped,
            Summary: "GPU runtime execution is skipped because no compatible ILGPU accelerator score was available on this host.",
            Parameters: new Dictionary<string, string>
            {
                ["has_gpu"] = capabilities.HasGpu.ToString(),
                ["ilgpu_cuda_available"] = capabilities.IlgpuCudaAvailable.ToString(),
                ["ilgpu_opencl_available"] = capabilities.IlgpuOpenclAvailable.ToString()
            },
            Metrics: new Dictionary<string, double>(),
            SkipReason: ResolveGpuCapabilitySkipReason(capabilities));

    private static PerfScenarioResult BuildGpuRuntimeWorkloadSkipResult(
        string suite,
        string scenario,
        IReadOnlyDictionary<string, string> parameters)
        => new(
            Suite: suite,
            Scenario: scenario,
            Backend: "gpu",
            Status: PerfScenarioStatus.Skipped,
            Summary: "GPU runtime execution is skipped because the configured workload never crosses the runtime GPU activation threshold.",
            Parameters: parameters,
            Metrics: new Dictionary<string, double>(),
            SkipReason: GpuRuntimeWorkloadBelowThresholdSkipReason);

    private static PlacementPlanner.WorkerCandidate[] BuildPlannerWorkers(int workerCount, ProtoSettings.NodeCapabilities capabilities)
    {
        var count = Math.Max(1, workerCount);
        var workers = new PlacementPlanner.WorkerCandidate[count];
        for (var i = 0; i < count; i++)
        {
            var availability = Math.Max(35, 100 - (i * 5));
            var scaled = WorkerCapabilityScaling.ApplyScale(
                capabilities,
                new WorkerResourceAvailability(availability, availability, availability, availability, availability));
            workers[i] = new PlacementPlanner.WorkerCandidate(
                NodeId: Guid.NewGuid(),
                WorkerAddress: $"worker-{i}.local",
                WorkerRootActorName: $"worker-{i}",
                IsAlive: true,
                IsReady: true,
                IsFresh: true,
                CpuCores: scaled.CpuCores,
                RamFreeBytes: (long)scaled.RamFreeBytes,
                RamTotalBytes: (long)scaled.RamTotalBytes,
                StorageFreeBytes: (long)scaled.StorageFreeBytes,
                StorageTotalBytes: (long)scaled.StorageTotalBytes,
                HasGpu: scaled.HasGpu,
                VramFreeBytes: (long)scaled.VramFreeBytes,
                VramTotalBytes: (long)scaled.VramTotalBytes,
                CpuScore: scaled.CpuScore,
                GpuScore: scaled.GpuScore,
                CpuLimitPercent: scaled.CpuLimitPercent,
                RamLimitPercent: scaled.RamLimitPercent,
                StorageLimitPercent: scaled.StorageLimitPercent,
                GpuComputeLimitPercent: scaled.GpuComputeLimitPercent,
                GpuVramLimitPercent: scaled.GpuVramLimitPercent,
                ProcessCpuLoadPercent: scaled.ProcessCpuLoadPercent,
                ProcessRamUsedBytes: (long)scaled.ProcessRamUsedBytes,
                PressureLimitTolerancePercent: (float)WorkerCapabilitySettingsKeys.DefaultPressureLimitTolerancePercent,
                AveragePeerLatencyMs: 0.75f + i,
                PeerLatencySampleCount: 4,
                HostedBrainCount: i % 3);
        }

        return workers;
    }

    private static string ResolveGpuCapabilitySkipReason(ProtoSettings.NodeCapabilities capabilities)
    {
        if (!capabilities.HasGpu)
        {
            return "gpu_not_detected";
        }

        if (!capabilities.IlgpuCudaAvailable && !capabilities.IlgpuOpenclAvailable)
        {
            return "ilgpu_gpu_accelerator_unavailable";
        }

        return "gpu_score_not_available";
    }

    private static bool IsGpuRuntimeReadyWorker(ProtoSettings.WorkerReadinessCapability worker)
        => worker.IsAlive
           && worker.IsReady
           && worker.HasCapabilities
           && worker.Capabilities is { } capabilities
           && HasCompatibleGpuCapabilities(capabilities);

    private static IReadOnlyDictionary<string, string> BuildEnvironmentSnapshot()
        => new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["machine_name"] = Environment.MachineName,
            ["os_description"] = RuntimeInformation.OSDescription,
            ["os_architecture"] = RuntimeInformation.OSArchitecture.ToString(),
            ["processor_count"] = Environment.ProcessorCount.ToString(),
            ["current_directory"] = Environment.CurrentDirectory
        };

    private static PerfScenarioResult TimeScenario(Func<PerfScenarioResult> factory)
    {
        var stopwatch = Stopwatch.StartNew();
        var result = factory();
        stopwatch.Stop();
        return result with { DurationMs = stopwatch.Elapsed.TotalMilliseconds };
    }

    private static async Task<PerfScenarioResult> TimeScenarioAsync(Func<Task<PerfScenarioResult>> factory)
    {
        var stopwatch = Stopwatch.StartNew();
        var result = await factory().ConfigureAwait(false);
        stopwatch.Stop();
        return result with { DurationMs = stopwatch.Elapsed.TotalMilliseconds };
    }
}
