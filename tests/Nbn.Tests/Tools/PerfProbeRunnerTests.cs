using Nbn.Shared;
using Nbn.Tools.PerfProbe;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Tests.Tools;

public sealed class PerfProbeRunnerTests
{
    [Fact]
    public void BuildWorkerProfileScenarios_EmitsPassedGpuPlannerProfile_WhenCompatibleGpuAndWorkloadQualify()
    {
        var config = new WorkerProfileConfig(
            PlannerWorkerCount: 4,
            PlannerIterations: 16,
            HiddenRegionNeurons: 8_192);

        var scenarios = PerfProbeRunner.BuildWorkerProfileScenarios(config, CreateCudaCapabilities()).ToArray();

        var gpuCapability = Assert.Single(scenarios, static scenario =>
            scenario.Suite == "worker_profile"
            && scenario.Scenario == "capability_probe"
            && scenario.Backend == "gpu");
        Assert.Equal(PerfScenarioStatus.Passed, gpuCapability.Status);

        var gpuPlanner = Assert.Single(scenarios, static scenario =>
            scenario.Suite == "worker_profile"
            && scenario.Scenario == "placement_planner_profile"
            && scenario.Backend == "gpu");
        Assert.Equal(PerfScenarioStatus.Passed, gpuPlanner.Status);
        Assert.Null(gpuPlanner.SkipReason);
        Assert.True(gpuPlanner.Metrics["plans_per_second"] > 0d);
        Assert.Equal("gpu", gpuPlanner.Parameters["compute_backend_preference"]);
        Assert.Equal(Math.Max(4096, NbnConstants.DefaultAxonStride * 2).ToString(), gpuPlanner.Parameters["max_neurons_per_shard"]);
        Assert.DoesNotContain(
            scenarios,
            static scenario => scenario.Scenario == "placement_planner_profile"
                               && scenario.Backend == "gpu"
                               && string.Equals(scenario.SkipReason, "regionshard_gpu_backend_not_available", StringComparison.Ordinal));
    }

    [Fact]
    public void BuildWorkerProfileScenarios_SkipsGpuProfiles_WhenCompatibleIlgpuAcceleratorIsUnavailable()
    {
        var config = new WorkerProfileConfig(
            PlannerWorkerCount: 4,
            PlannerIterations: 8,
            HiddenRegionNeurons: 8_192);
        var capabilities = CreateCudaCapabilities();
        capabilities.IlgpuCudaAvailable = false;
        capabilities.IlgpuOpenclAvailable = false;

        var scenarios = PerfProbeRunner.BuildWorkerProfileScenarios(config, capabilities).ToArray();

        var gpuCapability = Assert.Single(scenarios, static scenario =>
            scenario.Suite == "worker_profile"
            && scenario.Scenario == "capability_probe"
            && scenario.Backend == "gpu");
        Assert.Equal(PerfScenarioStatus.Skipped, gpuCapability.Status);
        Assert.Equal("ilgpu_gpu_accelerator_unavailable", gpuCapability.SkipReason);

        var gpuPlanner = Assert.Single(scenarios, static scenario =>
            scenario.Suite == "worker_profile"
            && scenario.Scenario == "placement_planner_profile"
            && scenario.Backend == "gpu");
        Assert.Equal(PerfScenarioStatus.Skipped, gpuPlanner.Status);
        Assert.Equal("ilgpu_gpu_accelerator_unavailable", gpuPlanner.SkipReason);
    }

    [Fact]
    public void BuildWorkerProfileScenarios_SkipsGpuPlanner_WhenHiddenWorkloadCannotReachGpuThreshold()
    {
        var config = new WorkerProfileConfig(
            PlannerWorkerCount: 4,
            PlannerIterations: 8,
            HiddenRegionNeurons: 2_048);

        var scenarios = PerfProbeRunner.BuildWorkerProfileScenarios(config, CreateCudaCapabilities()).ToArray();

        var cpuPlanner = Assert.Single(scenarios, static scenario =>
            scenario.Suite == "worker_profile"
            && scenario.Scenario == "placement_planner_profile"
            && scenario.Backend == "cpu");
        Assert.Equal(PerfScenarioStatus.Passed, cpuPlanner.Status);

        var gpuPlanner = Assert.Single(scenarios, static scenario =>
            scenario.Suite == "worker_profile"
            && scenario.Scenario == "placement_planner_profile"
            && scenario.Backend == "gpu");
        Assert.Equal(PerfScenarioStatus.Skipped, gpuPlanner.Status);
        Assert.Equal("gpu_planner_workload_below_threshold", gpuPlanner.SkipReason);
        Assert.Equal("2048", gpuPlanner.Parameters["hidden_region_neurons"]);
    }

    private static ProtoSettings.NodeCapabilities CreateCudaCapabilities()
        => new()
        {
            CpuCores = 24,
            RamFreeBytes = 96UL * 1024 * 1024 * 1024,
            RamTotalBytes = 128UL * 1024 * 1024 * 1024,
            StorageFreeBytes = 500UL * 1024 * 1024 * 1024,
            StorageTotalBytes = 1_000UL * 1024 * 1024 * 1024,
            HasGpu = true,
            GpuName = "NVIDIA GeForce RTX 5090",
            VramFreeBytes = 28UL * 1024 * 1024 * 1024,
            VramTotalBytes = 32UL * 1024 * 1024 * 1024,
            CpuScore = 128f,
            GpuScore = 6_291.456f,
            IlgpuCudaAvailable = true,
            IlgpuOpenclAvailable = false,
            CpuLimitPercent = 80,
            RamLimitPercent = 80,
            StorageLimitPercent = 80,
            GpuComputeLimitPercent = 80,
            GpuVramLimitPercent = 80,
            ProcessCpuLoadPercent = 0f,
            ProcessRamUsedBytes = 0
        };
}
