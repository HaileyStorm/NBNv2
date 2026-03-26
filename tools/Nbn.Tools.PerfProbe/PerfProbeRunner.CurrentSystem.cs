using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Proto.Settings;
using Nbn.Proto.Signal;
using Nbn.Proto.Viz;
using ProtoSettings = Nbn.Proto.Settings;
using ProtoControl = Nbn.Proto.Control;
using Nbn.Proto;
using Nbn.Shared;

namespace Nbn.Tools.PerfProbe;

public static partial class PerfProbeRunner
{
    /// <summary>
    /// Connects to an existing runtime and captures non-invasive current-system snapshots.
    /// </summary>
    public static async Task<PerfReport> RunCurrentSystemProfileAsync(
        CurrentSystemProfileConfig config,
        CancellationToken cancellationToken = default)
    {
        var totalStopwatch = System.Diagnostics.Stopwatch.StartNew();
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

        private readonly PID _settingsPid;

        private AttachedRuntimeClient(ActorSystem system, PID settingsPid)
        {
            System = system;
            _settingsPid = settingsPid;
        }

        public ActorSystem System { get; }
        public IRootContext Root => System.Root;

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
}
