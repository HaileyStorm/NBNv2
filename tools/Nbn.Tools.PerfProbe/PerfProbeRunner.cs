using System.Diagnostics;
using System.Runtime.InteropServices;
using Microsoft.Data.Sqlite;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Proto.Settings;
using Nbn.Proto.Signal;
using Nbn.Proto.Viz;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.Brain;
using Nbn.Runtime.HiveMind;
using Nbn.Runtime.IO;
using Nbn.Runtime.RegionHost;
using Nbn.Runtime.WorkerNode;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.HiveMind;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;
using ProtoControl = Nbn.Proto.Control;
using ProtoIo = Nbn.Proto.Io;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Tools.PerfProbe;

public static class PerfProbeRunner
{
    private const int DefaultInputWidth = 8;
    private const int DefaultOutputWidth = 8;
    private const int MaxPackedAxonCount = 0x1FF;
    private const float SuccessThresholdRatio = 0.95f;
    private const string RuntimeExecutionGpuSkipReason = "regionshard_gpu_backend_not_available";
    private const string GpuPlannerWorkloadBelowThresholdSkipReason = "gpu_planner_workload_below_threshold";
    private const string GpuRuntimeWorkloadBelowThresholdSkipReason = "gpu_runtime_workload_below_threshold";

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

    public static async Task<IReadOnlyList<PerfScenarioResult>> RunLocalhostStressScenariosAsync(
        LocalhostStressConfig config,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var capabilities = new WorkerNodeCapabilityProvider().GetCapabilities();
        var canRunGpuRuntime = HasCompatibleGpuCapabilities(capabilities);
        var gpuRuntimeConfig = BuildGpuLocalhostStressConfig(config);
        var results = new List<PerfScenarioResult>();
        foreach (var targetTickHz in config.TargetTickRates.OrderBy(static value => value))
        {
            var cpuResult = await TimeScenarioAsync(() => MeasureBrainSizeLimitAsync(
                targetTickHz,
                config,
                cancellationToken,
                backend: "cpu",
                computeBackendPreference: RegionShardComputeBackendPreference.Cpu)).ConfigureAwait(false);
            results.Add(cpuResult);
            if (canRunGpuRuntime && gpuRuntimeConfig.BrainSizes.Count > 0)
            {
                results.Add(await TimeScenarioAsync(() => MeasureBrainSizeLimitAsync(
                    targetTickHz,
                    gpuRuntimeConfig,
                    cancellationToken,
                    backend: "gpu",
                    computeBackendPreference: RegionShardComputeBackendPreference.Gpu)).ConfigureAwait(false));
            }
            else
            {
                results.Add(TimeScenario(() => canRunGpuRuntime
                    ? BuildGpuRuntimeWorkloadSkipResult(
                        "localhost_stress",
                        cpuResult.Scenario,
                        new Dictionary<string, string>
                        {
                            ["brain_sizes"] = string.Join(",", config.BrainSizes),
                            ["gpu_runtime_neuron_threshold"] = ResolveRuntimeGpuNeuronThreshold().ToString()
                        })
                    : BuildGpuRuntimeCapabilitySkipResult("localhost_stress", cpuResult.Scenario, capabilities)));
            }
        }

        var brainCountResult = await TimeScenarioAsync(() => MeasureBrainCountLimitAsync(
            config,
            cancellationToken,
            backend: "cpu",
            computeBackendPreference: RegionShardComputeBackendPreference.Cpu)).ConfigureAwait(false);
        results.Add(brainCountResult);
        results.Add(canRunGpuRuntime
            ? await TimeScenarioAsync(() => MeasureBrainCountLimitAsync(
                gpuRuntimeConfig,
                cancellationToken,
                backend: "gpu",
                computeBackendPreference: RegionShardComputeBackendPreference.Gpu)).ConfigureAwait(false)
            : TimeScenario(() => BuildGpuRuntimeCapabilitySkipResult("localhost_stress", brainCountResult.Scenario, capabilities)));

        var sustainableRateResult = await TimeScenarioAsync(() => MeasureSustainableTickRateAsync(
            config,
            cancellationToken,
            backend: "cpu",
            computeBackendPreference: RegionShardComputeBackendPreference.Cpu)).ConfigureAwait(false);
        results.Add(sustainableRateResult);
        results.Add(canRunGpuRuntime
            ? await TimeScenarioAsync(() => MeasureSustainableTickRateAsync(
                gpuRuntimeConfig,
                cancellationToken,
                backend: "gpu",
                computeBackendPreference: RegionShardComputeBackendPreference.Gpu)).ConfigureAwait(false)
            : TimeScenario(() => BuildGpuRuntimeCapabilitySkipResult("localhost_stress", sustainableRateResult.Scenario, capabilities)));

        var spawnChurnResult = await TimeScenarioAsync(() => MeasureSpawnChurnAsync(
            config,
            cancellationToken,
            backend: "cpu",
            computeBackendPreference: RegionShardComputeBackendPreference.Cpu)).ConfigureAwait(false);
        results.Add(spawnChurnResult);
        results.Add(canRunGpuRuntime
            ? await TimeScenarioAsync(() => MeasureSpawnChurnAsync(
                gpuRuntimeConfig,
                cancellationToken,
                backend: "gpu",
                computeBackendPreference: RegionShardComputeBackendPreference.Gpu)).ConfigureAwait(false)
            : TimeScenario(() => BuildGpuRuntimeCapabilitySkipResult("localhost_stress", spawnChurnResult.Scenario, capabilities)));

        return results;
    }

    public static async Task<PerfReport> RunCurrentSystemProfileAsync(
        CurrentSystemProfileConfig config,
        CancellationToken cancellationToken = default)
    {
        var totalStopwatch = Stopwatch.StartNew();
        await using var client = await AttachedRuntimeClient.CreateAsync(config, cancellationToken).ConfigureAwait(false);
        var settingsResponse = await client.ListSettingsAsync(cancellationToken).ConfigureAwait(false);
        var workerInventoryResponse = await client.ListWorkerInventoryAsync(cancellationToken).ConfigureAwait(false);
        var endpoints = BuildDiscoveredEndpointLookup(settingsResponse);
        var scenarios = new List<PerfScenarioResult>
        {
            TimeScenario(() => BuildServiceDiscoveryScenario(config, endpoints))
        };

        if (workerInventoryResponse is null)
        {
            scenarios.Add(TimeScenario(() => new PerfScenarioResult(
                Suite: "current_system",
                Scenario: "worker_inventory_snapshot",
                Backend: "cpu",
                Status: PerfScenarioStatus.Failed,
                Summary: "Failed to read worker inventory from the currently connected SettingsMonitor.",
                Parameters: new Dictionary<string, string>
                {
                    ["settings_host"] = config.SettingsHost,
                    ["settings_port"] = config.SettingsPort.ToString(),
                    ["settings_name"] = config.SettingsName
                },
                Metrics: new Dictionary<string, double>(),
                Failure: "settings_worker_inventory_unavailable")));
        }
        else
        {
            var workers = workerInventoryResponse.Workers.ToArray();
            scenarios.Add(TimeScenario(() => new PerfScenarioResult(
                Suite: "current_system",
                Scenario: "worker_inventory_snapshot",
                Backend: "cpu",
                Status: PerfScenarioStatus.Passed,
                Summary: workers.Length == 0
                    ? "No worker inventory rows were reported by SettingsMonitor."
                    : "Captured current worker inventory snapshot from SettingsMonitor.",
                Parameters: new Dictionary<string, string>
                {
                    ["worker_count"] = workers.Length.ToString()
                },
                Metrics: new Dictionary<string, double>
                {
                    ["active_workers"] = workers.Count(static worker => worker.IsAlive),
                    ["ready_workers"] = workers.Count(static worker => worker.IsReady),
                    ["gpu_ready_workers"] = workers.Count(worker => worker.Capabilities?.HasGpu == true && worker.Capabilities.GpuScore > 0f),
                    ["gpu_runtime_ready_workers"] = workers.Count(IsGpuRuntimeReadyWorker),
                    ["max_cpu_score"] = workers.Length == 0 ? 0d : workers.Max(worker => worker.Capabilities?.CpuScore ?? 0f),
                    ["max_gpu_score"] = workers.Length == 0 ? 0d : workers.Max(worker => worker.Capabilities?.GpuScore ?? 0f),
                    ["total_ram_free_bytes"] = workers.Sum(worker => (double)(worker.Capabilities?.RamFreeBytes ?? 0)),
                    ["total_storage_free_bytes"] = workers.Sum(worker => (double)(worker.Capabilities?.StorageFreeBytes ?? 0))
                })));
        }

        if (endpoints.TryGetValue(ServiceEndpointSettings.HiveMindKey, out var hiveMindEndpoint))
        {
            var hiveMindStatus = await client.GetHiveMindStatusAsync(hiveMindEndpoint, cancellationToken).ConfigureAwait(false);
            if (hiveMindStatus is not null)
            {
                scenarios.Add(TimeScenario(() => new PerfScenarioResult(
                    Suite: "current_system",
                    Scenario: "hivemind_status_snapshot",
                    Backend: "cpu",
                    Status: PerfScenarioStatus.Passed,
                    Summary: "Captured current HiveMind tick and registration state from the discovered runtime.",
                    Parameters: new Dictionary<string, string>
                    {
                        ["hivemind_endpoint"] = hiveMindEndpoint.ToString()
                    },
                    Metrics: new Dictionary<string, double>
                    {
                        ["target_tick_hz"] = hiveMindStatus.TargetTickHz,
                        ["last_completed_tick_id"] = hiveMindStatus.LastCompletedTickId,
                        ["registered_brains"] = hiveMindStatus.RegisteredBrains,
                        ["registered_shards"] = hiveMindStatus.RegisteredShards
                    })));
            }

            var placementInventory = await client.GetPlacementInventoryAsync(hiveMindEndpoint, cancellationToken).ConfigureAwait(false);
            if (placementInventory is not null)
            {
                scenarios.Add(TimeScenario(() => new PerfScenarioResult(
                    Suite: "current_system",
                    Scenario: "placement_inventory_snapshot",
                    Backend: "cpu",
                    Status: PerfScenarioStatus.Passed,
                    Summary: "Captured current HiveMind placement inventory from the discovered runtime.",
                    Parameters: new Dictionary<string, string>
                    {
                        ["hivemind_endpoint"] = hiveMindEndpoint.ToString()
                    },
                    Metrics: new Dictionary<string, double>
                    {
                        ["eligible_workers"] = placementInventory.Workers.Count,
                        ["gpu_capable_workers"] = placementInventory.Workers.Count(static worker => worker.GpuScore > 0f),
                        ["max_cpu_score"] = placementInventory.Workers.Count == 0 ? 0d : placementInventory.Workers.Max(static worker => worker.CpuScore),
                        ["max_gpu_score"] = placementInventory.Workers.Count == 0 ? 0d : placementInventory.Workers.Max(static worker => worker.GpuScore)
                    })));
            }
        }

        totalStopwatch.Stop();
        return new PerfReport(
            ToolName: "Nbn.Tools.PerfProbe",
            GeneratedAtUtc: DateTimeOffset.UtcNow,
            Environment: new Dictionary<string, string>(BuildEnvironmentSnapshot())
            {
                ["settings_host"] = config.SettingsHost,
                ["settings_port"] = config.SettingsPort.ToString(),
                ["settings_name"] = config.SettingsName
            },
            Scenarios: scenarios)
        {
            TotalDurationMs = totalStopwatch.Elapsed.TotalMilliseconds
        };
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
            RequestedShardPlan: new ProtoControl.ShardPlan
            {
                Mode = ProtoControl.ShardPlanMode.ShardPlanMaxNeurons,
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

    private static async Task<PerfScenarioResult> MeasureBrainSizeLimitAsync(
        float targetTickHz,
        LocalhostStressConfig config,
        CancellationToken cancellationToken,
        string backend,
        RegionShardComputeBackendPreference computeBackendPreference)
    {
        var maxSupported = 0;
        var bestObservedTickHz = 0d;
        foreach (var hiddenNeuronCount in config.BrainSizes.OrderBy(static value => value))
        {
            await using var harness = await LocalRuntimeHarness.CreateAsync(
                hiddenNeuronCount,
                targetTickHz,
                computeBackendPreference,
                cancellationToken).ConfigureAwait(false);
            var brainId = await harness.SpawnBrainAsync(cancellationToken).ConfigureAwait(false);
            var observedTickHz = await harness.MeasureTickRateAsync(
                    [brainId],
                    targetTickHz,
                    TimeSpan.FromSeconds(config.RunSeconds),
                    cancellationToken)
                .ConfigureAwait(false);
            var backendFailure = await harness.VerifyBackendExecutionAsync([brainId], cancellationToken).ConfigureAwait(false);
            if (!string.IsNullOrWhiteSpace(backendFailure))
            {
                return new PerfScenarioResult(
                    Suite: "localhost_stress",
                    Scenario: $"brain_size_limit_{targetTickHz:0.###}hz",
                    Backend: backend,
                    Status: PerfScenarioStatus.Failed,
                    Summary: "Runtime benchmark did not execute on the expected compute backend.",
                    Parameters: new Dictionary<string, string>
                    {
                        ["target_tick_hz"] = targetTickHz.ToString("0.###"),
                        ["hidden_neurons"] = hiddenNeuronCount.ToString(),
                        ["requested_backend"] = computeBackendPreference.ToString().ToLowerInvariant()
                    },
                    Metrics: new Dictionary<string, double>(),
                    Failure: backendFailure);
            }

            bestObservedTickHz = Math.Max(bestObservedTickHz, observedTickHz);
            if (observedTickHz >= targetTickHz * SuccessThresholdRatio)
            {
                maxSupported = hiddenNeuronCount;
            }
        }

        var status = maxSupported > 0 ? PerfScenarioStatus.Passed : PerfScenarioStatus.Failed;
        return new PerfScenarioResult(
            Suite: "localhost_stress",
            Scenario: $"brain_size_limit_{targetTickHz:0.###}hz",
            Backend: backend,
            Status: status,
            Summary: status == PerfScenarioStatus.Passed
                ? $"Measured the largest hidden-region size that sustained approximately {targetTickHz:0.###} Hz on localhost."
                : $"No configured brain size sustained approximately {targetTickHz:0.###} Hz on localhost.",
            Parameters: new Dictionary<string, string>
            {
                ["target_tick_hz"] = targetTickHz.ToString("0.###"),
                ["brain_sizes"] = string.Join(",", config.BrainSizes),
                ["run_seconds"] = config.RunSeconds.ToString(),
                ["requested_backend"] = computeBackendPreference.ToString().ToLowerInvariant()
            },
            Metrics: new Dictionary<string, double>
            {
                ["target_tick_hz"] = targetTickHz,
                ["max_supported_hidden_neurons"] = maxSupported,
                ["best_observed_tick_hz"] = bestObservedTickHz
            });
    }

    private static async Task<PerfScenarioResult> MeasureBrainCountLimitAsync(
        LocalhostStressConfig config,
        CancellationToken cancellationToken,
        string backend,
        RegionShardComputeBackendPreference computeBackendPreference)
    {
        var targetTickHz = config.TargetTickRates.Count > 1 ? config.TargetTickRates[1] : config.TargetTickRates[0];
        var maxSupported = 0;
        var bestObservedTickHz = 0d;
        foreach (var brainCount in config.BrainCounts.OrderBy(static value => value))
        {
            await using var harness = await LocalRuntimeHarness.CreateAsync(
                config.SustainableWorkloadNeurons,
                targetTickHz,
                computeBackendPreference,
                cancellationToken).ConfigureAwait(false);
            var brainIds = new List<Guid>(brainCount);
            for (var i = 0; i < brainCount; i++)
            {
                brainIds.Add(await harness.SpawnBrainAsync(cancellationToken).ConfigureAwait(false));
            }

            var observedTickHz = await harness.MeasureTickRateAsync(
                    brainIds,
                    targetTickHz,
                    TimeSpan.FromSeconds(config.RunSeconds),
                    cancellationToken)
                .ConfigureAwait(false);
            var backendFailure = await harness.VerifyBackendExecutionAsync(brainIds, cancellationToken).ConfigureAwait(false);
            if (!string.IsNullOrWhiteSpace(backendFailure))
            {
                return new PerfScenarioResult(
                    Suite: "localhost_stress",
                    Scenario: "brain_count_limit",
                    Backend: backend,
                    Status: PerfScenarioStatus.Failed,
                    Summary: "Runtime benchmark did not execute on the expected compute backend.",
                    Parameters: new Dictionary<string, string>
                    {
                        ["target_tick_hz"] = targetTickHz.ToString("0.###"),
                        ["brain_count"] = brainCount.ToString(),
                        ["hidden_neurons"] = config.SustainableWorkloadNeurons.ToString(),
                        ["requested_backend"] = computeBackendPreference.ToString().ToLowerInvariant()
                    },
                    Metrics: new Dictionary<string, double>(),
                    Failure: backendFailure);
            }

            bestObservedTickHz = Math.Max(bestObservedTickHz, observedTickHz);
            if (observedTickHz >= targetTickHz * SuccessThresholdRatio)
            {
                maxSupported = brainCount;
            }
        }

        var status = maxSupported > 0 ? PerfScenarioStatus.Passed : PerfScenarioStatus.Failed;
        return new PerfScenarioResult(
            Suite: "localhost_stress",
            Scenario: "brain_count_limit",
            Backend: backend,
            Status: status,
            Summary: status == PerfScenarioStatus.Passed
                ? $"Measured the largest localhost brain count that sustained approximately {targetTickHz:0.###} Hz."
                : $"No configured brain count sustained approximately {targetTickHz:0.###} Hz.",
            Parameters: new Dictionary<string, string>
            {
                ["target_tick_hz"] = targetTickHz.ToString("0.###"),
                ["brain_counts"] = string.Join(",", config.BrainCounts),
                ["hidden_neurons"] = config.SustainableWorkloadNeurons.ToString(),
                ["run_seconds"] = config.RunSeconds.ToString(),
                ["requested_backend"] = computeBackendPreference.ToString().ToLowerInvariant()
            },
            Metrics: new Dictionary<string, double>
            {
                ["target_tick_hz"] = targetTickHz,
                ["max_supported_brain_count"] = maxSupported,
                ["best_observed_tick_hz"] = bestObservedTickHz
            });
    }

    private static async Task<PerfScenarioResult> MeasureSustainableTickRateAsync(
        LocalhostStressConfig config,
        CancellationToken cancellationToken,
        string backend,
        RegionShardComputeBackendPreference computeBackendPreference)
    {
        var maxSupported = 0f;
        var bestObservedTickHz = 0d;
        foreach (var candidateTickHz in config.SustainableTickSweep.OrderBy(static value => value))
        {
            await using var harness = await LocalRuntimeHarness.CreateAsync(
                config.SustainableWorkloadNeurons,
                candidateTickHz,
                computeBackendPreference,
                cancellationToken).ConfigureAwait(false);
            var brainIds = new List<Guid>(config.SustainableBrainCount);
            for (var i = 0; i < config.SustainableBrainCount; i++)
            {
                brainIds.Add(await harness.SpawnBrainAsync(cancellationToken).ConfigureAwait(false));
            }

            var observedTickHz = await harness.MeasureTickRateAsync(
                    brainIds,
                    candidateTickHz,
                    TimeSpan.FromSeconds(config.RunSeconds),
                    cancellationToken)
                .ConfigureAwait(false);
            var backendFailure = await harness.VerifyBackendExecutionAsync(brainIds, cancellationToken).ConfigureAwait(false);
            if (!string.IsNullOrWhiteSpace(backendFailure))
            {
                return new PerfScenarioResult(
                    Suite: "localhost_stress",
                    Scenario: "max_sustainable_tick_rate",
                    Backend: backend,
                    Status: PerfScenarioStatus.Failed,
                    Summary: "Runtime benchmark did not execute on the expected compute backend.",
                    Parameters: new Dictionary<string, string>
                    {
                        ["tick_hz"] = candidateTickHz.ToString("0.###"),
                        ["brain_count"] = config.SustainableBrainCount.ToString(),
                        ["hidden_neurons"] = config.SustainableWorkloadNeurons.ToString(),
                        ["requested_backend"] = computeBackendPreference.ToString().ToLowerInvariant()
                    },
                    Metrics: new Dictionary<string, double>(),
                    Failure: backendFailure);
            }

            bestObservedTickHz = Math.Max(bestObservedTickHz, observedTickHz);
            if (observedTickHz >= candidateTickHz * SuccessThresholdRatio)
            {
                maxSupported = candidateTickHz;
            }
        }

        var status = maxSupported > 0f ? PerfScenarioStatus.Passed : PerfScenarioStatus.Failed;
        return new PerfScenarioResult(
            Suite: "localhost_stress",
            Scenario: "max_sustainable_tick_rate",
            Backend: backend,
            Status: status,
            Summary: status == PerfScenarioStatus.Passed
                ? "Measured the highest requested tick rate that remained sustainable for a representative localhost workload."
                : "No requested tick rate in the configured sweep remained sustainable.",
            Parameters: new Dictionary<string, string>
            {
                ["tick_sweep"] = string.Join(",", config.SustainableTickSweep.Select(static value => value.ToString("0.###"))),
                ["brain_count"] = config.SustainableBrainCount.ToString(),
                ["hidden_neurons"] = config.SustainableWorkloadNeurons.ToString(),
                ["run_seconds"] = config.RunSeconds.ToString(),
                ["requested_backend"] = computeBackendPreference.ToString().ToLowerInvariant()
            },
            Metrics: new Dictionary<string, double>
            {
                ["max_supported_tick_hz"] = maxSupported,
                ["best_observed_tick_hz"] = bestObservedTickHz
            });
    }

    private static async Task<PerfScenarioResult> MeasureSpawnChurnAsync(
        LocalhostStressConfig config,
        CancellationToken cancellationToken,
        string backend,
        RegionShardComputeBackendPreference computeBackendPreference)
    {
        var targetTickHz = config.TargetTickRates.Count > 1 ? config.TargetTickRates[1] : config.TargetTickRates[0];
        await using var harness = await LocalRuntimeHarness.CreateAsync(
            config.SustainableWorkloadNeurons,
            targetTickHz,
            computeBackendPreference,
            cancellationToken).ConfigureAwait(false);
        var baseBrains = new List<Guid>(config.SustainableBrainCount);
        for (var i = 0; i < config.SustainableBrainCount; i++)
        {
            baseBrains.Add(await harness.SpawnBrainAsync(cancellationToken).ConfigureAwait(false));
        }

        harness.StartTickLoop();
        using var inputLoadCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var inputLoadTask = harness.RunInputLoadAsync(baseBrains, targetTickHz, inputLoadCancellation.Token);

        var latencies = new List<double>(config.SpawnChurnBrainCount);
        try
        {
            for (var i = 0; i < config.SpawnChurnBrainCount; i++)
            {
                var stopwatch = Stopwatch.StartNew();
                _ = await harness.SpawnBrainAsync(cancellationToken).ConfigureAwait(false);
                stopwatch.Stop();
                latencies.Add(stopwatch.Elapsed.TotalMilliseconds);
            }
        }
        finally
        {
            inputLoadCancellation.Cancel();
            try
            {
                await inputLoadTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }

            harness.StopTickLoop();
        }

        var backendFailure = await harness.VerifyBackendExecutionAsync(baseBrains, cancellationToken).ConfigureAwait(false);
        if (!string.IsNullOrWhiteSpace(backendFailure))
        {
            return new PerfScenarioResult(
                Suite: "localhost_stress",
                Scenario: "spawn_churn_under_load",
                Backend: backend,
                Status: PerfScenarioStatus.Failed,
                Summary: "Runtime benchmark did not execute on the expected compute backend.",
                Parameters: new Dictionary<string, string>
                {
                    ["target_tick_hz"] = targetTickHz.ToString("0.###"),
                    ["base_brains"] = config.SustainableBrainCount.ToString(),
                    ["hidden_neurons"] = config.SustainableWorkloadNeurons.ToString(),
                    ["requested_backend"] = computeBackendPreference.ToString().ToLowerInvariant()
                },
                Metrics: new Dictionary<string, double>(),
                Failure: backendFailure);
        }

        var ordered = latencies.OrderBy(static value => value).ToArray();
        var median = ordered.Length == 0 ? 0d : ordered[ordered.Length / 2];
        return new PerfScenarioResult(
            Suite: "localhost_stress",
            Scenario: "spawn_churn_under_load",
            Backend: backend,
            Status: PerfScenarioStatus.Passed,
            Summary: "Measured spawn latency while the local runtime was already ticking representative workloads.",
            Parameters: new Dictionary<string, string>
            {
                ["target_tick_hz"] = targetTickHz.ToString("0.###"),
                ["spawned_brains"] = config.SpawnChurnBrainCount.ToString(),
                ["base_brains"] = config.SustainableBrainCount.ToString(),
                ["hidden_neurons"] = config.SustainableWorkloadNeurons.ToString(),
                ["requested_backend"] = computeBackendPreference.ToString().ToLowerInvariant()
            },
            Metrics: new Dictionary<string, double>
            {
                ["median_spawn_latency_ms"] = median,
                ["max_spawn_latency_ms"] = ordered.Length == 0 ? 0d : ordered[^1]
            });
    }

    internal sealed class LocalRuntimeHarness : IAsyncDisposable
    {
        private static readonly SemaphoreSlim BackendPreferenceGate = new(1, 1);

        private LocalRuntimeHarness(
            ActorSystem system,
            PID hiveMind,
            PID ioGateway,
            PID worker,
            string artifactRoot,
            uint inputWidth,
            ArtifactRef brainDef,
            string bootstrapIoEndpointActorName,
            RegionShardComputeBackendPreference computeBackendPreference)
        {
            System = system;
            HiveMind = hiveMind;
            IoGateway = ioGateway;
            Worker = worker;
            ArtifactRoot = artifactRoot;
            InputWidth = inputWidth;
            BrainDef = brainDef;
            BootstrapIoEndpointActorName = bootstrapIoEndpointActorName;
            ComputeBackendPreference = computeBackendPreference;
        }

        public ActorSystem System { get; }
        public IRootContext Root => System.Root;
        public PID HiveMind { get; }
        public PID IoGateway { get; }
        public PID Worker { get; }
        public string ArtifactRoot { get; }
        public uint InputWidth { get; }
        public ArtifactRef BrainDef { get; }
        public string BootstrapIoEndpointActorName { get; }
        public RegionShardComputeBackendPreference ComputeBackendPreference { get; }

        public static async Task<LocalRuntimeHarness> CreateAsync(
            int hiddenNeuronCount,
            float targetTickHz,
            RegionShardComputeBackendPreference computeBackendPreference,
            CancellationToken cancellationToken)
        {
            var benchmarkAvailability = new WorkerResourceAvailability(
                cpuPercent: 100,
                ramPercent: 100,
                storagePercent: 100,
                gpuComputePercent: 100,
                gpuVramPercent: 100);
            var artifactRoot = Path.Combine(Path.GetTempPath(), "nbn-perf-probe", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(artifactRoot);

            var inputWidth = ResolvePerformanceInputWidth(hiddenNeuronCount);
            var nbnBytes = BuildPerformanceNbn(hiddenNeuronCount, inputWidth, DefaultOutputWidth);
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var manifest = await store.StoreAsync(new MemoryStream(nbnBytes), "application/x-nbn").ConfigureAwait(false);
            var brainDef = manifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifest.ByteLength, "application/x-nbn", artifactRoot);

            var system = new ActorSystem();
            system.WithRemote(
                RemoteConfig.BindToLocalhost(0).WithProtoMessages(
                    NbnCommonReflection.Descriptor,
                    NbnControlReflection.Descriptor,
                    NbnIoReflection.Descriptor,
                    NbnReproReflection.Descriptor,
                    NbnSignalsReflection.Descriptor,
                    NbnDebugReflection.Descriptor,
                    NbnVizReflection.Descriptor,
                    NbnSettingsReflection.Descriptor));
            await system.Remote().StartAsync().ConfigureAwait(false);
            var root = system.Root;
            var workerId = Guid.NewGuid();
            var hiveName = $"hive-{Guid.NewGuid():N}";
            var ioName = $"io-{Guid.NewGuid():N}";
            var metadataName = $"brain-info-{Guid.NewGuid():N}";
            var workerName = $"worker-{Guid.NewGuid():N}";
            var ioPid = new PID(string.Empty, ioName);

            var hiveMind = root.SpawnNamed(
                Props.FromProducer(() => new HiveMindActor(CreateHiveOptions(targetTickHz), ioPid: ioPid)),
                hiveName);
            var ioGateway = root.SpawnNamed(
                Props.FromProducer(() => new IoGatewayActor(CreateIoOptions(), hiveMindPid: hiveMind)),
                ioName);
            _ = root.SpawnNamed(
                Props.FromProducer(() => new FixedBrainInfoActor(brainDef, DefaultInputWidth, DefaultOutputWidth)),
                metadataName);
            var capabilityProvider = new WorkerNodeCapabilityProvider(availability: benchmarkAvailability);
            var worker = root.SpawnNamed(
                Props.FromProducer(() => new WorkerNodeActor(
                    workerId,
                    string.Empty,
                    artifactRootPath: artifactRoot,
                    resourceAvailability: benchmarkAvailability)),
                workerName);

            PrimeWorkerDiscoveryEndpoints(root, worker, hiveName, metadataName);
            PrimeWorkers(root, hiveMind, worker, workerId, capabilityProvider.GetCapabilities());

            await WaitForAsync(
                    async () =>
                    {
                        var snapshot = await root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
                            worker,
                            new WorkerNodeActor.GetWorkerNodeSnapshot()).ConfigureAwait(false);
                        return snapshot.HiveMindEndpoint.HasValue && snapshot.IoGatewayEndpoint.HasValue;
                    },
                    TimeSpan.FromSeconds(5),
                    cancellationToken)
                .ConfigureAwait(false);

            return new LocalRuntimeHarness(
                system,
                hiveMind,
                ioGateway,
                worker,
                artifactRoot,
                (uint)inputWidth,
                brainDef,
                metadataName,
                computeBackendPreference);
        }

        public async Task<Guid> SpawnBrainAsync(CancellationToken cancellationToken)
        {
            PrimeWorkerDiscoveryEndpoints(Root, Worker, HiveMind.Id, BootstrapIoEndpointActorName);

            var response = await WithScopedBackendPreferenceAsync(
                    ComputeBackendPreference,
                    () => Root.RequestAsync<ProtoIo.SpawnBrainViaIOAck>(
                        IoGateway,
                        new ProtoIo.SpawnBrainViaIO
                        {
                            Request = new ProtoControl.SpawnBrain
                            {
                                BrainDef = BrainDef
                            }
                        },
                        TimeSpan.FromSeconds(70)))
                .ConfigureAwait(false);

            if (response.Ack is null || !response.Ack.BrainId.TryToGuid(out var brainId) || brainId == Guid.Empty)
            {
                throw new InvalidOperationException(
                    $"SpawnBrainViaIOAck did not return a brain id. failure={response.Ack?.FailureReasonCode} message={response.Ack?.FailureMessage}");
            }

            await WaitForAsync(
                    async () =>
                    {
                        var lifecycle = await Root.RequestAsync<ProtoControl.PlacementLifecycleInfo>(
                                HiveMind,
                                new ProtoControl.GetPlacementLifecycle
                                {
                                    BrainId = brainId.ToProtoUuid()
                                })
                            .ConfigureAwait(false);

                        return lifecycle.LifecycleState is ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned
                            or ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning;
                    },
                    TimeSpan.FromSeconds(10),
                    cancellationToken)
                .ConfigureAwait(false);

            PrimeWorkerDiscoveryEndpoints(Root, Worker, HiveMind.Id, IoGateway.Id);

            Root.Send(IoGateway, new ProtoIo.RegisterBrain
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = InputWidth,
                OutputWidth = DefaultOutputWidth,
                BaseDefinition = BrainDef,
                InputCoordinatorMode = ProtoControl.InputCoordinatorMode.DirtyOnChange,
                OutputVectorSource = ProtoControl.OutputVectorSource.Potential,
                IoGatewayOwnsInputCoordinator = true,
                IoGatewayOwnsOutputCoordinator = true
            });

            await WaitForAsync(
                    async () =>
                    {
                        var brainInfo = await Root.RequestAsync<ProtoIo.BrainInfo>(
                                IoGateway,
                                new ProtoIo.BrainInfoRequest
                                {
                                    BrainId = brainId.ToProtoUuid()
                                })
                            .ConfigureAwait(false);
                        return brainInfo.InputWidth >= InputWidth && brainInfo.OutputWidth >= DefaultOutputWidth;
                    },
                    TimeSpan.FromSeconds(5),
                    cancellationToken)
                .ConfigureAwait(false);

            ProtoIo.IoCommandAck plasticityAck;
            try
            {
                plasticityAck = await Root.RequestAsync<ProtoIo.IoCommandAck>(
                        IoGateway,
                        new ProtoIo.SetPlasticityEnabled
                        {
                            BrainId = brainId.ToProtoUuid(),
                            PlasticityEnabled = false,
                            PlasticityRate = 0f,
                            ProbabilisticUpdates = false,
                            PlasticityDelta = 0f,
                            PlasticityRebaseThreshold = 0,
                            PlasticityRebaseThresholdPct = 0f,
                            PlasticityEnergyCostModulationEnabled = false,
                            PlasticityEnergyCostReferenceTickCost = 0,
                            PlasticityEnergyCostResponseStrength = 1f,
                            PlasticityEnergyCostMinScale = 1f,
                            PlasticityEnergyCostMaxScale = 1f
                        },
                        TimeSpan.FromSeconds(2))
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Timed out applying perf plasticity config for brain {brainId:N}.",
                    ex);
            }
            if (!plasticityAck.Success)
            {
                throw new InvalidOperationException(
                    $"Failed to disable plasticity for perf brain {brainId:N}: {plasticityAck.Message}");
            }

            ProtoIo.IoCommandAck homeostasisAck;
            try
            {
                homeostasisAck = await Root.RequestAsync<ProtoIo.IoCommandAck>(
                        IoGateway,
                        new ProtoIo.SetHomeostasisEnabled
                        {
                            BrainId = brainId.ToProtoUuid(),
                            HomeostasisEnabled = false,
                            HomeostasisTargetMode = ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero,
                            HomeostasisUpdateMode = ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
                            HomeostasisBaseProbability = 0f,
                            HomeostasisMinStepCodes = 1,
                            HomeostasisEnergyCouplingEnabled = false,
                            HomeostasisEnergyTargetScale = 1f,
                            HomeostasisEnergyProbabilityScale = 1f
                        },
                        TimeSpan.FromSeconds(2))
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Timed out applying perf homeostasis config for brain {brainId:N}.",
                    ex);
            }
            if (!homeostasisAck.Success)
            {
                throw new InvalidOperationException(
                    $"Failed to disable homeostasis for perf brain {brainId:N}: {homeostasisAck.Message}");
            }

            WorkerNodeActor.HostedBrainSnapshot? lastHosted = null;
            ProtoControl.BrainRoutingInfo? lastHiveRouting = null;
            var lastControlPid = "<none>";
            var lastHiddenRoutePresent = false;
            var lastControllerRouteCount = -1;
            var routingReadyDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (DateTime.UtcNow >= routingReadyDeadline)
                {
                    throw new InvalidOperationException(
                        $"Local runtime routing did not stabilize for brain {brainId:N}. " +
                        $"hostedControl={lastControlPid} regionShards={lastHosted?.RegionShardCount ?? 0} hiddenShard={FormatPid(lastHosted?.HiddenShardPid)} " +
                        $"hiveRoot={lastHiveRouting?.BrainRootPid ?? "<none>"} hiveRouter={lastHiveRouting?.SignalRouterPid ?? "<none>"} " +
                        $"hiveShardCount={lastHiveRouting?.ShardCount ?? 0} hiveRoutingCount={lastHiveRouting?.RoutingCount ?? 0} " +
                        $"controllerRouteCount={lastControllerRouteCount} hiddenRoutePresent={lastHiddenRoutePresent}");
                }

                lastHosted = await Root.RequestAsync<WorkerNodeActor.HostedBrainSnapshot>(
                        Worker,
                        new WorkerNodeActor.GetHostedBrainSnapshot(brainId),
                        TimeSpan.FromSeconds(2))
                    .ConfigureAwait(false);
                if (lastHosted.RegionShardCount == 0 || lastHosted.HiddenShardPid is null)
                {
                    await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var controlPid = lastHosted.SignalRouterPid ?? lastHosted.BrainRootPid;
                if (controlPid is null)
                {
                    await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                lastControlPid = FormatPid(controlPid);
                lastHiveRouting = await Root.RequestAsync<ProtoControl.BrainRoutingInfo>(
                        HiveMind,
                        new ProtoControl.GetBrainRouting
                        {
                            BrainId = brainId.ToProtoUuid()
                        },
                        TimeSpan.FromSeconds(2))
                    .ConfigureAwait(false);
                var controlKnownByHiveMind =
                    PidLabelRefersToActor(lastHiveRouting.SignalRouterPid, controlPid)
                    || PidLabelRefersToActor(lastHiveRouting.BrainRootPid, controlPid);
                if (!controlKnownByHiveMind || lastHiveRouting.ShardCount == 0 || lastHiveRouting.RoutingCount == 0)
                {
                    await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var routing = await Root.RequestAsync<RoutingTableSnapshot>(
                        controlPid,
                        new GetRoutingTable(),
                        TimeSpan.FromSeconds(2))
                    .ConfigureAwait(false);
                lastControllerRouteCount = routing.Count;
                lastHiddenRoutePresent = routing.Routes.Any(static route => route.ShardId.RegionId == 1 && route.ShardId.ShardIndex == 0);
                if (lastHiddenRoutePresent)
                {
                    break;
                }

                await Task.Delay(50, cancellationToken).ConfigureAwait(false);
            }

            return brainId;
        }

        public async Task<string?> VerifyBackendExecutionAsync(
            IReadOnlyCollection<Guid> brainIds,
            CancellationToken cancellationToken)
        {
            if (ComputeBackendPreference == RegionShardComputeBackendPreference.Auto || brainIds.Count == 0)
            {
                return null;
            }

            foreach (var brainId in brainIds)
            {
                RegionShardBackendExecutionInfo info = default;
                var observedExecution = false;
                for (var attempt = 0; attempt < 20; attempt++)
                {
                    info = await Root.RequestAsync<RegionShardBackendExecutionInfo>(
                            Worker,
                            new WorkerNodeActor.GetHostedRegionShardBackendExecutionInfo(brainId, RegionId: 1, ShardIndex: 0),
                            TimeSpan.FromSeconds(2))
                        .ConfigureAwait(false);
                    if (info.HasExecuted)
                    {
                        observedExecution = true;
                        break;
                    }

                    await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                }

                if (string.Equals(info.BackendName, "unavailable", StringComparison.Ordinal))
                {
                    return string.IsNullOrWhiteSpace(info.FallbackReason)
                        ? $"backend_execution_query_failed:region_shard_unavailable:{brainId:N}"
                        : info.FallbackReason;
                }
                if (!observedExecution)
                {
                    return $"backend_execution_query_failed:region_shard_never_executed:{brainId:N}";
                }

                var shouldUseGpu = ComputeBackendPreference == RegionShardComputeBackendPreference.Gpu;
                if (info.UsedGpu != shouldUseGpu)
                {
                    var mode = shouldUseGpu ? "gpu" : "cpu";
                    var reason = string.IsNullOrWhiteSpace(info.FallbackReason) ? "none" : info.FallbackReason;
                    return $"expected_{mode}_backend_but_observed_{info.BackendName}:fallback={reason}";
                }
            }

            return null;
        }

        public async Task<double> MeasureTickRateAsync(
            IReadOnlyCollection<Guid> brainIds,
            float targetTickHz,
            TimeSpan duration,
            CancellationToken cancellationToken)
        {
            var before = await Root.RequestAsync<ProtoControl.HiveMindStatus>(
                    HiveMind,
                    new ProtoControl.GetHiveMindStatus())
                .ConfigureAwait(false);

            StartTickLoop();
            using var inputLoadCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var inputLoadTask = RunInputLoadAsync(brainIds, targetTickHz, inputLoadCancellation.Token);
            try
            {
                await Task.Delay(duration, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                inputLoadCancellation.Cancel();
                try
                {
                    await inputLoadTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }

                StopTickLoop();
            }

            var after = await Root.RequestAsync<ProtoControl.HiveMindStatus>(
                    HiveMind,
                    new ProtoControl.GetHiveMindStatus())
                .ConfigureAwait(false);
            var completedTicks = after.LastCompletedTickId >= before.LastCompletedTickId
                ? after.LastCompletedTickId - before.LastCompletedTickId
                : 0UL;
            return completedTicks / Math.Max(duration.TotalSeconds, 0.001d);
        }

        public void StartTickLoop()
            => Root.Send(HiveMind, new StartTickLoop());

        public void StopTickLoop()
            => Root.Send(HiveMind, new StopTickLoop());

        public async Task RunInputLoadAsync(
            IReadOnlyCollection<Guid> brainIds,
            float targetTickHz,
            CancellationToken cancellationToken)
        {
            if (brainIds.Count == 0)
            {
                return;
            }

            var values = Enumerable.Repeat(1f, (int)InputWidth).ToArray();
            var delayMs = Math.Max(5, (int)Math.Round(1000d / Math.Max(targetTickHz * 2d, 1d)));
            while (!cancellationToken.IsCancellationRequested)
            {
                foreach (var brainId in brainIds)
                {
                    Root.Send(IoGateway, new ProtoIo.InputVector
                    {
                        BrainId = brainId.ToProtoUuid(),
                        Values = { values }
                    });
                }

                await Task.Delay(delayMs, cancellationToken).ConfigureAwait(false);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await System.Remote().ShutdownAsync(true).ConfigureAwait(false);
            await System.ShutdownAsync().ConfigureAwait(false);
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(ArtifactRoot))
            {
                Directory.Delete(ArtifactRoot, recursive: true);
            }
        }

        private static string FormatPid(PID? pid)
            => pid is null
                ? "<none>"
                : string.IsNullOrWhiteSpace(pid.Address)
                    ? pid.Id
                    : $"{pid.Address}/{pid.Id}";

        private static bool PidLabelRefersToActor(string? pidLabel, PID pid)
            => string.Equals(
                NormalizeObservedActorId(ExtractActorId(pidLabel)),
                NormalizeObservedActorId(pid.Id),
                StringComparison.Ordinal);

        private static string ExtractActorId(string? pidLabel)
        {
            if (string.IsNullOrWhiteSpace(pidLabel))
            {
                return string.Empty;
            }

            var slash = pidLabel.IndexOf('/');
            return slash >= 0 && slash < pidLabel.Length - 1
                ? pidLabel[(slash + 1)..]
                : pidLabel;
        }

        private static string NormalizeObservedActorId(string actorId)
        {
            if (string.IsNullOrWhiteSpace(actorId) || !actorId.Contains('/', StringComparison.Ordinal))
            {
                return actorId;
            }

            var parts = actorId
                .Split('/', StringSplitOptions.RemoveEmptyEntries)
                .Where(static part => !part.StartsWith("$", StringComparison.Ordinal))
                .ToArray();
            return parts.Length == 0 ? actorId : string.Join("/", parts);
        }

        private static async Task<T> WithScopedBackendPreferenceAsync<T>(
            RegionShardComputeBackendPreference preference,
            Func<Task<T>> action)
        {
            if (preference == RegionShardComputeBackendPreference.Auto)
            {
                return await action().ConfigureAwait(false);
            }

            await BackendPreferenceGate.WaitAsync().ConfigureAwait(false);
            var previous = Environment.GetEnvironmentVariable(RegionShardComputeBackendPreferenceResolver.EnvironmentVariableName);
            try
            {
                Environment.SetEnvironmentVariable(
                    RegionShardComputeBackendPreferenceResolver.EnvironmentVariableName,
                    preference.ToString().ToLowerInvariant());
                return await action().ConfigureAwait(false);
            }
            finally
            {
                Environment.SetEnvironmentVariable(
                    RegionShardComputeBackendPreferenceResolver.EnvironmentVariableName,
                    previous);
                BackendPreferenceGate.Release();
            }
        }

    }

    private sealed class FixedBrainInfoActor : IActor
    {
        private readonly ArtifactRef _brainDef;
        private readonly uint _inputWidth;
        private readonly uint _outputWidth;

        public FixedBrainInfoActor(ArtifactRef brainDef, uint inputWidth, uint outputWidth)
        {
            _brainDef = brainDef.Clone();
            _inputWidth = inputWidth;
            _outputWidth = outputWidth;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is not ProtoIo.BrainInfoRequest request)
            {
                return Task.CompletedTask;
            }

            context.Respond(new ProtoIo.BrainInfo
            {
                BrainId = request.BrainId,
                InputWidth = _inputWidth,
                OutputWidth = _outputWidth,
                BaseDefinition = _brainDef.Clone(),
                LastSnapshot = new ArtifactRef()
            });

            return Task.CompletedTask;
        }
    }

    private static IoOptions CreateIoOptions()
        => new(
            BindHost: "127.0.0.1",
            Port: 0,
            AdvertisedHost: null,
            AdvertisedPort: null,
            GatewayName: IoNames.Gateway,
            ServerName: "nbn.io.perf-probe",
            SettingsHost: null,
            SettingsPort: 0,
            SettingsName: "SettingsMonitor",
            HiveMindAddress: null,
            HiveMindName: null,
            ReproAddress: null,
            ReproName: null);

    private static HiveMindOptions CreateHiveOptions(float targetTickHz)
        => new(
            BindHost: "127.0.0.1",
            Port: 0,
            AdvertisedHost: null,
            AdvertisedPort: null,
            TargetTickHz: targetTickHz,
            MinTickHz: Math.Max(1f, targetTickHz * 0.5f),
            ComputeTimeoutMs: 1_000,
            DeliverTimeoutMs: 1_000,
            BackpressureDecay: 0.9f,
            BackpressureRecovery: 1.1f,
            LateBackpressureThreshold: 2,
            TimeoutRescheduleThreshold: 3,
            TimeoutPauseThreshold: 6,
            RescheduleMinTicks: 10,
            RescheduleMinMinutes: 1,
            RescheduleQuietMs: 50,
            RescheduleSimulatedMs: 50,
            AutoStart: false,
            EnableOpenTelemetry: false,
            EnableOtelMetrics: false,
            EnableOtelTraces: false,
            EnableOtelConsoleExporter: false,
            OtlpEndpoint: null,
            ServiceName: "nbn.hivemind.perf-probe",
            SettingsDbPath: null,
            SettingsHost: null,
            SettingsPort: 0,
            SettingsName: "SettingsMonitor",
            IoAddress: null,
            IoName: null,
            WorkerInventoryRefreshMs: 2_000,
            WorkerInventoryStaleAfterMs: 10_000,
            PlacementAssignmentTimeoutMs: 1_000,
            PlacementAssignmentRetryBackoffMs: 10,
            PlacementAssignmentMaxRetries: 1,
            PlacementReconcileTimeoutMs: 1_000);

    private static void PrimeWorkerDiscoveryEndpoints(IRootContext root, PID workerPid, string hiveName, string ioName)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var known = new Dictionary<string, ServiceEndpointRegistration>(StringComparer.Ordinal)
        {
            [ServiceEndpointSettings.HiveMindKey] = new ServiceEndpointRegistration(
                ServiceEndpointSettings.HiveMindKey,
                new ServiceEndpoint(string.Empty, hiveName),
                nowMs),
            [ServiceEndpointSettings.IoGatewayKey] = new ServiceEndpointRegistration(
                ServiceEndpointSettings.IoGatewayKey,
                new ServiceEndpoint(string.Empty, ioName),
                nowMs)
        };

        root.Send(workerPid, new WorkerNodeActor.DiscoverySnapshotApplied(known));
    }

    private static void PrimeWorkers(
        IRootContext root,
        PID hiveMind,
        PID workerPid,
        Guid workerId,
        ProtoSettings.NodeCapabilities capabilities)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nowMs,
            Workers =
            {
                new ProtoSettings.WorkerReadinessCapability
                {
                    NodeId = workerId.ToProtoUuid(),
                    Address = string.Empty,
                    RootActorName = workerPid.Id,
                    IsAlive = true,
                    IsReady = true,
                    LastSeenMs = (ulong)nowMs,
                    HasCapabilities = true,
                    CapabilityTimeMs = (ulong)nowMs,
                    Capabilities = capabilities.Clone()
                }
            }
        });
    }

    private static async Task WaitForAsync(
        Func<Task<bool>> predicate,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        using var timeoutCancellation = new CancellationTokenSource(timeout);
        using var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCancellation.Token);
        while (true)
        {
            if (await predicate().ConfigureAwait(false))
            {
                return;
            }

            await Task.Delay(20, linkedCancellation.Token).ConfigureAwait(false);
        }
    }

    private static byte[] BuildPerformanceNbn(int hiddenNeuronCount, int inputWidth, int outputWidth)
    {
        const uint stride = 1_024;
        var sections = new List<NbnRegionSection>();
        var directory = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        ulong offset = NbnBinary.NbnHeaderBytes;

        var inputAssignments = Enumerable.Range(0, hiddenNeuronCount)
            .GroupBy(index => index % inputWidth)
            .ToDictionary(group => group.Key, group => group.ToArray());
        offset = AddRegionSection(
            regionId: NbnConstants.InputRegionId,
            neuronSpan: (uint)inputWidth,
            stride: stride,
            directory: directory,
            sections: sections,
            offset: offset,
            neuronFactory: neuronId =>
            {
                inputAssignments.TryGetValue(neuronId, out var targets);
                return new NeuronRecord(
                    axonCount: (ushort)(targets?.Length ?? 0),
                    paramBCode: 0,
                    paramACode: 0,
                    activationThresholdCode: 0,
                    preActivationThresholdCode: 0,
                    resetFunctionId: (byte)ResetFunction.ResetHold,
                    activationFunctionId: (byte)ActivationFunction.ActIdentity,
                    accumulationFunctionId: (byte)AccumulationFunction.AccumSum,
                    exists: true);
            },
            axonsBuilder: () =>
            {
                var inputAxons = new List<AxonRecord>(hiddenNeuronCount);
                for (var neuronId = 0; neuronId < inputWidth; neuronId++)
                {
                    if (!inputAssignments.TryGetValue(neuronId, out var targets))
                    {
                        continue;
                    }

                    foreach (var targetNeuronId in targets)
                    {
                        inputAxons.Add(new AxonRecord(31, targetNeuronId, targetRegionId: 1));
                    }
                }

                return inputAxons;
            });

        offset = AddRegionSection(
            regionId: 1,
            neuronSpan: (uint)hiddenNeuronCount,
            stride: stride,
            directory: directory,
            sections: sections,
            offset: offset,
            neuronFactory: _ => new NeuronRecord(
                axonCount: (ushort)2,
                paramBCode: 0,
                paramACode: 0,
                activationThresholdCode: 0,
                preActivationThresholdCode: 0,
                resetFunctionId: (byte)ResetFunction.ResetHold,
                activationFunctionId: (byte)ActivationFunction.ActRelu,
                accumulationFunctionId: (byte)AccumulationFunction.AccumSum,
                exists: true),
            axonsBuilder: () =>
            {
                var hiddenAxons = new List<AxonRecord>(hiddenNeuronCount * 2);
                for (var neuronId = 0; neuronId < hiddenNeuronCount; neuronId++)
                {
                    hiddenAxons.Add(new AxonRecord(28, (neuronId + 1) % hiddenNeuronCount, targetRegionId: 1));
                    hiddenAxons.Add(new AxonRecord(24, neuronId % outputWidth, targetRegionId: (byte)NbnConstants.OutputRegionId));
                }

                return hiddenAxons;
            });

        offset = AddRegionSection(
            regionId: NbnConstants.OutputRegionId,
            neuronSpan: (uint)outputWidth,
            stride: stride,
            directory: directory,
            sections: sections,
            offset: offset,
            neuronFactory: _ => new NeuronRecord(
                axonCount: (ushort)0,
                paramBCode: 0,
                paramACode: 0,
                activationThresholdCode: 0,
                preActivationThresholdCode: 0,
                resetFunctionId: (byte)ResetFunction.ResetHold,
                activationFunctionId: (byte)ActivationFunction.ActIdentity,
                accumulationFunctionId: (byte)AccumulationFunction.AccumSum,
                exists: true),
            axonsBuilder: static () => Array.Empty<AxonRecord>());

        var header = new NbnHeaderV2(
            "NBN2",
            2,
            1,
            10,
            brainSeed: 1,
            axonStride: stride,
            flags: 0,
            quantization: QuantizationSchemas.DefaultNbn,
            regions: directory);

        return NbnBinary.WriteNbn(header, sections);
    }

    private static int ResolvePerformanceInputWidth(int hiddenNeuronCount)
    {
        if (hiddenNeuronCount <= 0)
        {
            return DefaultInputWidth;
        }

        var requiredWidth = (int)Math.Ceiling(hiddenNeuronCount / (double)MaxPackedAxonCount);
        return Math.Max(DefaultInputWidth, requiredWidth);
    }

    private static ulong AddRegionSection(
        int regionId,
        uint neuronSpan,
        uint stride,
        NbnRegionDirectoryEntry[] directory,
        List<NbnRegionSection> sections,
        ulong offset,
        Func<int, NeuronRecord> neuronFactory,
        Func<IReadOnlyList<AxonRecord>> axonsBuilder)
    {
        var neurons = new NeuronRecord[neuronSpan];
        for (var i = 0; i < neurons.Length; i++)
        {
            neurons[i] = neuronFactory(i);
        }

        var axons = axonsBuilder().ToArray();
        ulong totalAxons = 0;
        foreach (var neuron in neurons)
        {
            totalAxons += neuron.AxonCount;
        }

        if ((ulong)axons.Length != totalAxons)
        {
            throw new InvalidOperationException($"Region {regionId} axon count mismatch. Expected {totalAxons}, got {axons.Length}.");
        }

        var checkpointCount = (uint)((neuronSpan + stride - 1) / stride + 1);
        var checkpoints = new ulong[checkpointCount];
        checkpoints[0] = 0;
        var running = 0UL;
        var checkpointIndex = 1;
        uint nextBoundary = stride;
        for (var i = 0; i < neurons.Length; i++)
        {
            running += neurons[i].AxonCount;
            if ((uint)(i + 1) == nextBoundary && checkpointIndex < checkpointCount)
            {
                checkpoints[checkpointIndex++] = running;
                nextBoundary += stride;
            }
        }

        checkpoints[checkpointCount - 1] = running;
        var section = new NbnRegionSection(
            (byte)regionId,
            neuronSpan,
            totalAxons,
            stride,
            checkpointCount,
            checkpoints,
            neurons,
            axons);

        directory[regionId] = new NbnRegionDirectoryEntry(neuronSpan, totalAxons, offset, 0);
        sections.Add(section);
        return offset + (ulong)section.ByteLength;
    }

    private static Dictionary<string, ServiceEndpoint> BuildDiscoveredEndpointLookup(ProtoSettings.SettingListResponse? settingsResponse)
    {
        var lookup = new Dictionary<string, ServiceEndpoint>(StringComparer.Ordinal);
        if (settingsResponse?.Settings is null)
        {
            return lookup;
        }

        foreach (var setting in settingsResponse.Settings)
        {
            if (!ServiceEndpointSettings.IsKnownKey(setting.Key)
                || !ServiceEndpointSettings.TryParseValue(setting.Value, out var endpoint))
            {
                continue;
            }

            lookup[setting.Key] = endpoint;
        }

        return lookup;
    }

    private static PerfScenarioResult BuildServiceDiscoveryScenario(
        CurrentSystemProfileConfig config,
        IReadOnlyDictionary<string, ServiceEndpoint> endpoints)
    {
        var metrics = new Dictionary<string, double>
        {
            ["discovered_endpoint_count"] = endpoints.Count,
            ["has_hivemind"] = endpoints.ContainsKey(ServiceEndpointSettings.HiveMindKey) ? 1 : 0,
            ["has_io_gateway"] = endpoints.ContainsKey(ServiceEndpointSettings.IoGatewayKey) ? 1 : 0,
            ["has_worker_node"] = endpoints.ContainsKey(ServiceEndpointSettings.WorkerNodeKey) ? 1 : 0,
            ["has_reproduction"] = endpoints.ContainsKey(ServiceEndpointSettings.ReproductionManagerKey) ? 1 : 0,
            ["has_speciation"] = endpoints.ContainsKey(ServiceEndpointSettings.SpeciationManagerKey) ? 1 : 0,
            ["has_observability"] = endpoints.ContainsKey(ServiceEndpointSettings.ObservabilityKey) ? 1 : 0
        };

        return new PerfScenarioResult(
            Suite: "current_system",
            Scenario: "service_discovery_snapshot",
            Backend: "cpu",
            Status: PerfScenarioStatus.Passed,
            Summary: "Captured SettingsMonitor-backed service discovery snapshot for the current runtime.",
            Parameters: new Dictionary<string, string>
            {
                ["settings_host"] = config.SettingsHost,
                ["settings_port"] = config.SettingsPort.ToString(),
                ["settings_name"] = config.SettingsName
            },
            Metrics: metrics);
    }

    private static RemoteConfig BuildRemoteConfig(CurrentSystemProfileConfig config)
    {
        RemoteConfig remoteConfig;
        if (IsLocalhost(config.BindHost))
        {
            remoteConfig = RemoteConfig.BindToLocalhost(config.BindPort);
        }
        else if (IsAllInterfaces(config.BindHost))
        {
            remoteConfig = RemoteConfig.BindToAllInterfaces(
                ResolveCurrentSystemRemoteHost(config),
                config.BindPort);
        }
        else
        {
            remoteConfig = RemoteConfig.BindTo(config.BindHost, config.BindPort);
        }

        return remoteConfig.WithProtoMessages(
            NbnCommonReflection.Descriptor,
            NbnControlReflection.Descriptor,
            NbnIoReflection.Descriptor,
            NbnReproReflection.Descriptor,
            NbnSignalsReflection.Descriptor,
            NbnDebugReflection.Descriptor,
            NbnVizReflection.Descriptor,
            NbnSettingsReflection.Descriptor);
    }

    internal static string ResolveCurrentSystemRemoteHost(CurrentSystemProfileConfig config)
    {
        if (IsAllInterfaces(config.BindHost))
        {
            return NetworkAddressDefaults.ResolveAdvertisedHost(config.BindHost, advertisedHost: null);
        }

        return config.BindHost;
    }

    private static bool IsLocalhost(string host)
        => host.Equals("localhost", StringComparison.OrdinalIgnoreCase)
           || host.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
           || host.Equals("::1", StringComparison.OrdinalIgnoreCase);

    private static bool IsAllInterfaces(string host)
        => host.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase)
           || host.Equals("::", StringComparison.OrdinalIgnoreCase)
           || host.Equals("*", StringComparison.OrdinalIgnoreCase);

    private sealed class AttachedRuntimeClient : IAsyncDisposable
    {
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);

        private AttachedRuntimeClient(ActorSystem system, PID settingsPid)
        {
            System = system;
            _settingsPid = settingsPid;
        }

        public ActorSystem System { get; }
        public IRootContext Root => System.Root;
        private readonly PID _settingsPid;

        public static async Task<AttachedRuntimeClient> CreateAsync(
            CurrentSystemProfileConfig config,
            CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            var system = new ActorSystem();
            var remoteConfig = BuildRemoteConfig(config);
            system.WithRemote(remoteConfig);
            await system.Remote().StartAsync().ConfigureAwait(false);
            var settingsPid = new PID($"{config.SettingsHost}:{config.SettingsPort}", config.SettingsName);
            return new AttachedRuntimeClient(system, settingsPid);
        }

        public async Task<ProtoSettings.SettingListResponse?> ListSettingsAsync(CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            try
            {
                return await Root.RequestAsync<ProtoSettings.SettingListResponse>(
                        _settingsPid,
                        new ProtoSettings.SettingListRequest(),
                        DefaultTimeout)
                    .ConfigureAwait(false);
            }
            catch
            {
                return null;
            }
        }

        public async Task<ProtoSettings.WorkerInventorySnapshotResponse?> ListWorkerInventoryAsync(CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            try
            {
                return await Root.RequestAsync<ProtoSettings.WorkerInventorySnapshotResponse>(
                        _settingsPid,
                        new ProtoSettings.WorkerInventorySnapshotRequest(),
                        DefaultTimeout)
                    .ConfigureAwait(false);
            }
            catch
            {
                return null;
            }
        }

        public async Task<ProtoControl.HiveMindStatus?> GetHiveMindStatusAsync(ServiceEndpoint endpoint, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            try
            {
                return await Root.RequestAsync<ProtoControl.HiveMindStatus>(
                        endpoint.ToPid(),
                        new ProtoControl.GetHiveMindStatus(),
                        DefaultTimeout)
                    .ConfigureAwait(false);
            }
            catch
            {
                return null;
            }
        }

        public async Task<ProtoControl.PlacementWorkerInventory?> GetPlacementInventoryAsync(ServiceEndpoint endpoint, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            try
            {
                return await Root.RequestAsync<ProtoControl.PlacementWorkerInventory>(
                        endpoint.ToPid(),
                        new ProtoControl.PlacementWorkerInventoryRequest(),
                        DefaultTimeout)
                    .ConfigureAwait(false);
            }
            catch
            {
                return null;
            }
        }

        public async ValueTask DisposeAsync()
        {
            await System.Remote().ShutdownAsync(true).ConfigureAwait(false);
            await System.ShutdownAsync().ConfigureAwait(false);
        }
    }

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
