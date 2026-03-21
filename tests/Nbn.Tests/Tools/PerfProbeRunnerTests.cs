using Nbn.Shared;
using Nbn.Proto.Io;
using Nbn.Runtime.WorkerNode;
using Nbn.Tools.PerfProbe;
using Proto;
using ProtoControl = Nbn.Proto.Control;
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

    [Fact]
    public void ResolveCurrentSystemRemoteHost_UsesAdvertisedHost_WhenBindingToAllInterfaces()
    {
        var config = new CurrentSystemProfileConfig(
            SettingsHost: "127.0.0.1",
            SettingsPort: 12010,
            SettingsName: "SettingsMonitor",
            BindHost: NetworkAddressDefaults.DefaultBindHost,
            BindPort: 12110);

        var resolved = PerfProbeRunner.ResolveCurrentSystemRemoteHost(config);

        Assert.Equal(
            NetworkAddressDefaults.ResolveAdvertisedHost(NetworkAddressDefaults.DefaultBindHost, advertisedHost: null),
            resolved);
        Assert.NotEqual(NetworkAddressDefaults.DefaultBindHost, resolved);
    }

    [Fact]
    public async Task LocalRuntimeHarness_CreateAsync_PrimesWorkerIoGatewayEndpointWithMetadataBootstrapActor()
    {
        await using var harness = await PerfProbeRunner.LocalRuntimeHarness.CreateAsync(
            hiddenNeuronCount: 128,
            targetTickHz: 10f,
            computeBackendPreference: RegionShardComputeBackendPreference.Cpu,
            cancellationToken: CancellationToken.None);

        var snapshot = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            harness.Worker,
            new WorkerNodeActor.GetWorkerNodeSnapshot(),
            CancellationToken.None);

        var ioEndpoint = Assert.NotNull(snapshot.IoGatewayEndpoint);
        Assert.NotEqual(harness.IoGateway.Id, ioEndpoint.Endpoint.ActorName);
        Assert.StartsWith("brain-info-", ioEndpoint.Endpoint.ActorName, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LocalRuntimeHarness_SpawnBrainAsync_SwitchesWorkerIoEndpointToGatewayAndPrimesBrainInfo()
    {
        await using var harness = await PerfProbeRunner.LocalRuntimeHarness.CreateAsync(
            hiddenNeuronCount: 128,
            targetTickHz: 10f,
            computeBackendPreference: RegionShardComputeBackendPreference.Cpu,
            cancellationToken: CancellationToken.None);

        var brainId = await harness.SpawnBrainAsync(CancellationToken.None);

        var snapshot = await harness.Root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
            harness.Worker,
            new WorkerNodeActor.GetWorkerNodeSnapshot(),
            CancellationToken.None);
        var ioEndpoint = Assert.NotNull(snapshot.IoGatewayEndpoint);
        Assert.Equal(harness.IoGateway.Id, ioEndpoint.Endpoint.ActorName);

        var brainInfo = await harness.Root.RequestAsync<BrainInfo>(
            harness.IoGateway,
            new BrainInfoRequest
            {
                BrainId = brainId.ToProtoUuid()
            },
            CancellationToken.None);
        Assert.True(brainInfo.InputWidth >= harness.InputWidth);
        Assert.True(brainInfo.OutputWidth >= 8);
    }

    [Fact]
    public async Task LocalRuntimeHarness_SpawnBrainAsync_RegistersShardRoutingInHiveMind()
    {
        await using var harness = await PerfProbeRunner.LocalRuntimeHarness.CreateAsync(
            hiddenNeuronCount: 128,
            targetTickHz: 10f,
            computeBackendPreference: RegionShardComputeBackendPreference.Cpu,
            cancellationToken: CancellationToken.None);

        var brainId = await harness.SpawnBrainAsync(CancellationToken.None);
        var routing = await harness.Root.RequestAsync<ProtoControl.BrainRoutingInfo>(
            harness.HiveMind,
            new ProtoControl.GetBrainRouting
            {
                BrainId = brainId.ToProtoUuid()
            },
            CancellationToken.None);

        Assert.True(routing.ShardCount > 0);
        Assert.True(routing.RoutingCount > 0);
    }

    [Fact]
    public async Task LocalRuntimeHarness_MeasureTickRateAsync_CompletesForSingleBrain()
    {
        await using var harness = await PerfProbeRunner.LocalRuntimeHarness.CreateAsync(
            hiddenNeuronCount: 128,
            targetTickHz: 10f,
            computeBackendPreference: RegionShardComputeBackendPreference.Cpu,
            cancellationToken: CancellationToken.None);

        var brainId = await harness.SpawnBrainAsync(CancellationToken.None);
        var tickRate = await harness.MeasureTickRateAsync(
            [brainId],
            targetTickHz: 10f,
            duration: TimeSpan.FromMilliseconds(250),
            cancellationToken: CancellationToken.None);
        var backendFailure = await harness.VerifyBackendExecutionAsync(
            [brainId],
            CancellationToken.None);

        Assert.True(tickRate >= 0d);
        Assert.True(string.IsNullOrWhiteSpace(backendFailure), backendFailure);
    }

    [Fact]
    public async Task LocalRuntimeHarness_MeasureTickRateAsync_CompletesForTwoBrains()
    {
        await using var harness = await PerfProbeRunner.LocalRuntimeHarness.CreateAsync(
            hiddenNeuronCount: 128,
            targetTickHz: 10f,
            computeBackendPreference: RegionShardComputeBackendPreference.Cpu,
            cancellationToken: CancellationToken.None);

        var firstBrainId = await harness.SpawnBrainAsync(CancellationToken.None);
        var secondBrainId = await harness.SpawnBrainAsync(CancellationToken.None);
        var tickRate = await harness.MeasureTickRateAsync(
            [firstBrainId, secondBrainId],
            targetTickHz: 10f,
            duration: TimeSpan.FromMilliseconds(250),
            cancellationToken: CancellationToken.None);

        Assert.True(tickRate >= 0d);
    }

    [Fact]
    public async Task LocalRuntimeHarness_MeasureTickRateAsync_CompletesForEightBrains()
    {
        await using var harness = await PerfProbeRunner.LocalRuntimeHarness.CreateAsync(
            hiddenNeuronCount: 128,
            targetTickHz: 10f,
            computeBackendPreference: RegionShardComputeBackendPreference.Cpu,
            cancellationToken: CancellationToken.None);

        var brainIds = new List<Guid>(8);
        for (var i = 0; i < 8; i++)
        {
            brainIds.Add(await harness.SpawnBrainAsync(CancellationToken.None));
        }

        var tickRate = await harness.MeasureTickRateAsync(
            brainIds,
            targetTickHz: 10f,
            duration: TimeSpan.FromMilliseconds(250),
            cancellationToken: CancellationToken.None);

        Assert.True(tickRate >= 0d);
    }

    [Fact]
    public async Task LocalRuntimeHarness_SpawnBrainAsync_CompletesWhileTickLoopIsActive()
    {
        await using var harness = await PerfProbeRunner.LocalRuntimeHarness.CreateAsync(
            hiddenNeuronCount: 128,
            targetTickHz: 10f,
            computeBackendPreference: RegionShardComputeBackendPreference.Cpu,
            cancellationToken: CancellationToken.None);

        var baseBrainA = await harness.SpawnBrainAsync(CancellationToken.None);
        var baseBrainB = await harness.SpawnBrainAsync(CancellationToken.None);
        harness.StartTickLoop();
        using var inputLoadCancellation = new CancellationTokenSource();
        var inputLoadTask = harness.RunInputLoadAsync([baseBrainA, baseBrainB], 10f, inputLoadCancellation.Token);

        try
        {
            var spawnedBrain = await harness.SpawnBrainAsync(CancellationToken.None);
            Assert.NotEqual(Guid.Empty, spawnedBrain);
        }
        finally
        {
            inputLoadCancellation.Cancel();
            try
            {
                await inputLoadTask;
            }
            catch (OperationCanceledException)
            {
            }

            harness.StopTickLoop();
        }
    }

    [Fact]
    public async Task LocalRuntimeHarness_GpuSingleBrainFlow_Completes_WhenCompatibleGpuIsAvailable()
    {
        var capabilities = new WorkerNodeCapabilityProvider().GetCapabilities();
        if (!capabilities.IlgpuCudaAvailable && !capabilities.IlgpuOpenclAvailable)
        {
            return;
        }

        await using var harness = await PerfProbeRunner.LocalRuntimeHarness.CreateAsync(
            hiddenNeuronCount: 8_192,
            targetTickHz: 10f,
            computeBackendPreference: RegionShardComputeBackendPreference.Gpu,
            cancellationToken: CancellationToken.None);

        var brainId = await harness.SpawnBrainAsync(CancellationToken.None);
        var tickRate = await harness.MeasureTickRateAsync(
            [brainId],
            targetTickHz: 10f,
            duration: TimeSpan.FromMilliseconds(250),
            cancellationToken: CancellationToken.None);
        var backendFailure = await harness.VerifyBackendExecutionAsync(
            [brainId],
            CancellationToken.None);

        Assert.True(tickRate >= 0d);
        Assert.True(string.IsNullOrWhiteSpace(backendFailure), backendFailure);
    }

    [Fact]
    public void BuildCompletedLocalhostStressResult_ReportsBelowTargetMeasurementsAsPassed()
    {
        var result = PerfProbeRunner.BuildCompletedLocalhostStressResult(
            Scenario: "brain_size_limit_10hz",
            Backend: "cpu",
            MetConfiguredTarget: false,
            SuccessSummary: "success",
            BelowTargetSummary: "below target",
            Parameters: new Dictionary<string, string>
            {
                ["target_tick_hz"] = "10"
            },
            Metrics: new Dictionary<string, double>
            {
                ["best_observed_tick_hz"] = 9.25
            });

        Assert.Equal(PerfScenarioStatus.Passed, result.Status);
        Assert.Equal("below target", result.Summary);
        Assert.Equal("localhost_stress", result.Suite);
        Assert.Equal("cpu", result.Backend);
    }

    [Fact]
    public void BuildCompletedLocalhostStressResult_ReportsMetTargetMeasurementsAsPassed()
    {
        var result = PerfProbeRunner.BuildCompletedLocalhostStressResult(
            Scenario: "brain_count_limit",
            Backend: "cpu",
            MetConfiguredTarget: true,
            SuccessSummary: "success",
            BelowTargetSummary: "below target",
            Parameters: new Dictionary<string, string>(),
            Metrics: new Dictionary<string, double>());

        Assert.Equal(PerfScenarioStatus.Passed, result.Status);
        Assert.Equal("success", result.Summary);
    }

    [Fact]
    public async Task LocalRuntimeHarness_DirectTickCompute_ReachesHiddenShard()
    {
        await using var harness = await PerfProbeRunner.LocalRuntimeHarness.CreateAsync(
            hiddenNeuronCount: 128,
            targetTickHz: 10f,
            computeBackendPreference: RegionShardComputeBackendPreference.Cpu,
            cancellationToken: CancellationToken.None);

        var brainId = await harness.SpawnBrainAsync(CancellationToken.None);
        var hosted = await harness.Root.RequestAsync<WorkerNodeActor.HostedBrainSnapshot>(
            harness.Worker,
            new WorkerNodeActor.GetHostedBrainSnapshot(brainId),
            CancellationToken.None);
        var targetPid = hosted.SignalRouterPid ?? hosted.BrainRootPid ?? throw new InvalidOperationException("No local control pid was hosted.");

        harness.Root.Send(targetPid, new ProtoControl.TickCompute
        {
            TickId = 1,
            TargetTickHz = 10f
        });

        var backendFailure = await harness.VerifyBackendExecutionAsync(
            [brainId],
            CancellationToken.None);

        Assert.True(string.IsNullOrWhiteSpace(backendFailure), backendFailure);
    }

    [Fact]
    public async Task LocalRuntimeHarness_StartTickLoop_AdvancesHiveMindTicks()
    {
        await using var harness = await PerfProbeRunner.LocalRuntimeHarness.CreateAsync(
            hiddenNeuronCount: 128,
            targetTickHz: 10f,
            computeBackendPreference: RegionShardComputeBackendPreference.Cpu,
            cancellationToken: CancellationToken.None);

        _ = await harness.SpawnBrainAsync(CancellationToken.None);
        var before = await harness.Root.RequestAsync<ProtoControl.HiveMindStatus>(
            harness.HiveMind,
            new ProtoControl.GetHiveMindStatus(),
            CancellationToken.None);

        harness.StartTickLoop();
        await Task.Delay(300);
        harness.StopTickLoop();

        var after = await harness.Root.RequestAsync<ProtoControl.HiveMindStatus>(
            harness.HiveMind,
            new ProtoControl.GetHiveMindStatus(),
            CancellationToken.None);

        Assert.True(after.LastCompletedTickId > before.LastCompletedTickId);
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
