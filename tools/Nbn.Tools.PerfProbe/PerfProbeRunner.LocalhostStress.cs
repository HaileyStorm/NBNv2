using System.Diagnostics;
using Nbn.Runtime.RegionHost;
using Nbn.Runtime.WorkerNode;
using RegionShardComputeBackendPreference = Nbn.Shared.RegionShardComputeBackendPreference;

namespace Nbn.Tools.PerfProbe;

public static partial class PerfProbeRunner
{
    /// <summary>
    /// Runs localhost runtime stress scenarios using the local harness.
    /// </summary>
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

        var computeDominantResult = await TimeScenarioAsync(() => MeasureComputeDominantTickRateAsync(
            config.ComputeDominantWorkload,
            cancellationToken,
            backend: "cpu",
            computeBackendPreference: RegionShardComputeBackendPreference.Cpu)).ConfigureAwait(false);
        results.Add(computeDominantResult);
        results.Add(canRunGpuRuntime
            ? await TimeScenarioAsync(() => MeasureComputeDominantTickRateAsync(
                config.ComputeDominantWorkload,
                cancellationToken,
                backend: "gpu",
                computeBackendPreference: RegionShardComputeBackendPreference.Gpu)).ConfigureAwait(false)
            : TimeScenario(() => BuildGpuRuntimeCapabilitySkipResult("localhost_stress", computeDominantResult.Scenario, capabilities)));

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

        return BuildCompletedLocalhostStressResult(
            Scenario: $"brain_size_limit_{targetTickHz:0.###}hz",
            Backend: backend,
            MetConfiguredTarget: maxSupported > 0,
            SuccessSummary: $"Measured the largest hidden-region size that sustained approximately {targetTickHz:0.###} Hz on localhost.",
            BelowTargetSummary: $"Measured localhost performance below the configured {targetTickHz:0.###} Hz brain-size target; no configured brain size sustained that rate on this host.",
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

        return BuildCompletedLocalhostStressResult(
            Scenario: "brain_count_limit",
            Backend: backend,
            MetConfiguredTarget: maxSupported > 0,
            SuccessSummary: $"Measured the largest localhost brain count that sustained approximately {targetTickHz:0.###} Hz.",
            BelowTargetSummary: $"Measured localhost performance below the configured {targetTickHz:0.###} Hz brain-count target; no configured brain count sustained that rate on this host.",
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

        return BuildCompletedLocalhostStressResult(
            Scenario: "max_sustainable_tick_rate",
            Backend: backend,
            MetConfiguredTarget: maxSupported > 0f,
            SuccessSummary: "Measured the highest requested tick rate that remained sustainable for a representative localhost workload.",
            BelowTargetSummary: "Measured localhost performance below the configured sustainable tick-rate sweep; no requested tick rate in the sweep remained sustainable on this host.",
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

    private static async Task<PerfScenarioResult> MeasureComputeDominantTickRateAsync(
        ComputeDominantStressConfig config,
        CancellationToken cancellationToken,
        string backend,
        RegionShardComputeBackendPreference computeBackendPreference)
    {
        await using var harness = await LocalRuntimeHarness.CreateAsync(
            config.HiddenNeurons,
            config.TargetTickHz,
            computeBackendPreference,
            cancellationToken,
            workloadProfile: LocalRuntimeWorkloadProfile.ComputeDominantRecurrent).ConfigureAwait(false);
        var brainCount = Math.Max(1, config.BrainCount);
        var brainIds = new List<Guid>(brainCount);
        for (var i = 0; i < brainCount; i++)
        {
            brainIds.Add(await harness.SpawnBrainAsync(cancellationToken).ConfigureAwait(false));
        }

        var observedTickHz = await harness.MeasureTickRateAsync(
                brainIds,
                config.TargetTickHz,
                TimeSpan.FromSeconds(config.RunSeconds),
                cancellationToken,
                inputLoadProfile: LocalRuntimeInputLoadProfile.None)
            .ConfigureAwait(false);
        var backendFailure = await harness.VerifyBackendExecutionAsync(brainIds, cancellationToken).ConfigureAwait(false);
        if (!string.IsNullOrWhiteSpace(backendFailure))
        {
            return new PerfScenarioResult(
                Suite: "localhost_stress",
                Scenario: "compute_dominant_tick_rate",
                Backend: backend,
                Status: PerfScenarioStatus.Failed,
                Summary: "Compute-dominant runtime benchmark did not execute on the expected compute backend.",
                Parameters: new Dictionary<string, string>
                {
                    ["target_tick_hz"] = config.TargetTickHz.ToString("0.###"),
                    ["brain_count"] = brainCount.ToString(),
                    ["hidden_neurons"] = config.HiddenNeurons.ToString(),
                    ["run_seconds"] = config.RunSeconds.ToString(),
                    ["input_mode"] = "runtime_hidden_seed",
                    ["requested_backend"] = computeBackendPreference.ToString().ToLowerInvariant()
                },
                Metrics: new Dictionary<string, double>(),
                Failure: backendFailure);
        }

        return BuildCompletedLocalhostStressResult(
            Scenario: "compute_dominant_tick_rate",
            Backend: backend,
            MetConfiguredTarget: observedTickHz >= config.TargetTickHz * SuccessThresholdRatio,
            SuccessSummary: "Measured compute-dominant localhost throughput for a seeded high-load runtime workload.",
            BelowTargetSummary: "Measured compute-dominant localhost throughput below the configured seeded target; see observed_tick_hz for the sustained rate on this host.",
            Parameters: new Dictionary<string, string>
            {
                ["target_tick_hz"] = config.TargetTickHz.ToString("0.###"),
                ["brain_count"] = brainCount.ToString(),
                ["hidden_neurons"] = config.HiddenNeurons.ToString(),
                ["run_seconds"] = config.RunSeconds.ToString(),
                ["input_mode"] = "runtime_hidden_seed",
                ["requested_backend"] = computeBackendPreference.ToString().ToLowerInvariant()
            },
            Metrics: new Dictionary<string, double>
            {
                ["target_tick_hz"] = config.TargetTickHz,
                ["observed_tick_hz"] = observedTickHz,
                ["total_hidden_neurons"] = (double)config.HiddenNeurons * brainCount
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

    internal static PerfScenarioResult BuildCompletedLocalhostStressResult(
        string Scenario,
        string Backend,
        bool MetConfiguredTarget,
        string SuccessSummary,
        string BelowTargetSummary,
        IReadOnlyDictionary<string, string> Parameters,
        IReadOnlyDictionary<string, double> Metrics)
        => new(
            Suite: "localhost_stress",
            Scenario: Scenario,
            Backend: Backend,
            Status: PerfScenarioStatus.Passed,
            Summary: MetConfiguredTarget ? SuccessSummary : BelowTargetSummary,
            Parameters: Parameters,
            Metrics: Metrics);
}
