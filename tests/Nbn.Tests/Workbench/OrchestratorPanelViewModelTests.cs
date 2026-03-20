using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Settings;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Workbench;

public class OrchestratorPanelViewModelTests
{
    [Fact]
    public async Task StartAllCommand_IncludesWorkerLaunch()
    {
        var connections = new ConnectionViewModel
        {
            SettingsPortText = "bad",
            HiveMindPortText = "bad",
            ReproPortText = "bad",
            IoPortText = "bad",
            ObsPortText = "bad",
            WorkerPortText = "bad"
        };

        var vm = CreateViewModel(connections, new FakeWorkbenchClient());
        try
        {
            vm.StartAllCommand.Execute(null);
            await WaitForAsync(() => string.Equals(vm.WorkerLaunchStatus, "Invalid worker port.", StringComparison.Ordinal));
            await WaitForAsync(() => string.Equals(vm.ReproLaunchStatus, "Invalid Reproduction port.", StringComparison.Ordinal));
            await WaitForAsync(() => string.Equals(vm.IoLaunchStatus, "Invalid IO port.", StringComparison.Ordinal));
            await WaitForAsync(() => string.Equals(vm.ObsLaunchStatus, "Invalid Obs port.", StringComparison.Ordinal));

            Assert.Equal("Invalid worker port.", vm.WorkerLaunchStatus);
            Assert.Equal("Invalid Settings port.", vm.SpeciationLaunchStatus);
        }
        finally
        {
            await vm.StopAllAsyncForShutdown();
        }
    }

    [Fact]
    public async Task StartSpeciationServiceAsync_WhenLaunchPreparationFails_ReportsFailure()
    {
        var connections = new ConnectionViewModel
        {
            SettingsPortText = "12030",
            SpeciationPortText = "12080"
        };

        var vm = CreateViewModel(
            connections,
            new FakeWorkbenchClient(),
            new FakeLocalProjectLaunchPreparer("Build failed (code 1). CS1000"));

        await vm.StartSpeciationServiceAsync();

        Assert.Equal("Build failed (code 1). CS1000", vm.SpeciationLaunchStatus);
        Assert.Equal("Speciation launch: Build failed (code 1). CS1000", vm.StatusMessage);
    }

    [Fact]
    public async Task ProfileCurrentSystemCommand_WhenServicesNotReady_ShowsPrereqMessage_AndSkipsLaunch()
    {
        var launchPreparer = new RecordingLocalProjectLaunchPreparer();
        var vm = CreateViewModel(new ConnectionViewModel(), new FakeWorkbenchClient(), launchPreparer);

        vm.ProfileCurrentSystemCommand.Execute(null);

        await WaitForAsync(
            () => string.Equals(vm.ProfileCurrentSystemStatus, "Connect Settings, HiveMind, and IO first.", StringComparison.Ordinal));

        Assert.Equal("Connect Settings, HiveMind, and IO first.", vm.ProfileCurrentSystemStatus);
        Assert.Equal("Profile current system: Connect Settings, HiveMind, and IO first.", vm.StatusMessage);
        Assert.Equal(0, launchPreparer.CallCount);
    }

    [Fact]
    public async Task ProfileCurrentSystemCommand_WhenLaunchPreparationFails_ReportsFailure()
    {
        var connections = new ConnectionViewModel
        {
            SettingsStatus = "Ready",
            HiveMindStatus = "Online",
            IoStatus = "Connected"
        };

        var vm = CreateViewModel(
            connections,
            new FakeWorkbenchClient(),
            new FakeLocalProjectLaunchPreparer("Build failed (code 1). PERF1000"));

        vm.ProfileCurrentSystemCommand.Execute(null);

        await WaitForAsync(
            () => string.Equals(vm.ProfileCurrentSystemStatus, "Build failed (code 1). PERF1000", StringComparison.Ordinal));

        Assert.Equal("Build failed (code 1). PERF1000", vm.ProfileCurrentSystemStatus);
        Assert.Equal("Profile current system: Build failed (code 1). PERF1000", vm.StatusMessage);
    }

    [Fact]
    public async Task StartWorkerCommand_DoesNotIncludeBenchmarkRefreshArgument()
    {
        var connections = new ConnectionViewModel
        {
            WorkerPortText = "12041",
            SettingsPortText = "12010"
        };

        var launchPreparer = new RecordingLocalProjectLaunchPreparer("Build failed (code 1). worker");
        var vm = CreateViewModel(connections, new FakeWorkbenchClient(), launchPreparer);

        vm.StartWorkerCommand.Execute(null);

        await WaitForAsync(() => string.Equals(vm.WorkerLaunchStatus, "Build failed (code 1). worker", StringComparison.Ordinal));

        Assert.Equal(1, launchPreparer.CallCount);
        Assert.DoesNotContain("--capability-benchmark-refresh-seconds", launchPreparer.LastRuntimeArgs, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StartWorkerCommand_IncludesConfiguredWorkerLimitArguments()
    {
        var connections = new ConnectionViewModel
        {
            WorkerPortText = "12041",
            SettingsPortText = "12010",
            WorkerCpuLimitPercentText = "85",
            WorkerRamLimitPercentText = "70",
            WorkerGpuLimitPercentText = "55",
            WorkerVramLimitPercentText = "40"
        };

        var launchPreparer = new RecordingLocalProjectLaunchPreparer("Build failed (code 1). worker");
        var vm = CreateViewModel(connections, new FakeWorkbenchClient(), launchPreparer);

        vm.StartWorkerCommand.Execute(null);

        await WaitForAsync(() => string.Equals(vm.WorkerLaunchStatus, "Build failed (code 1). worker", StringComparison.Ordinal));

        Assert.Contains("--cpu-pct 85", launchPreparer.LastRuntimeArgs, StringComparison.Ordinal);
        Assert.Contains("--ram-pct 70", launchPreparer.LastRuntimeArgs, StringComparison.Ordinal);
        Assert.Contains("--storage-pct 95", launchPreparer.LastRuntimeArgs, StringComparison.Ordinal);
        Assert.Contains("--gpu-compute-pct 55", launchPreparer.LastRuntimeArgs, StringComparison.Ordinal);
        Assert.Contains("--gpu-vram-pct 40", launchPreparer.LastRuntimeArgs, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StartSettingsMonitorCommand_UsesAllInterfacesBindByDefault()
    {
        var connections = new ConnectionViewModel
        {
            SettingsHost = "127.0.0.1",
            SettingsPortText = "12010"
        };

        var launchPreparer = new RecordingLocalProjectLaunchPreparer("Build failed (code 1). settings");
        var vm = CreateViewModel(connections, new FakeWorkbenchClient(), launchPreparer);

        vm.StartSettingsMonitorCommand.Execute(null);

        await WaitForAsync(() => string.Equals(vm.SettingsLaunchStatus, "Build failed (code 1). settings", StringComparison.Ordinal));

        Assert.Contains("--bind-host 0.0.0.0 --port 12010", launchPreparer.LastRuntimeArgs, StringComparison.Ordinal);
        Assert.DoesNotContain("--advertise-host 127.0.0.1", launchPreparer.LastRuntimeArgs, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StartIoCommand_UsesConfiguredNonLoopbackHostAsAdvertiseHost()
    {
        using var _ = new EnvironmentVariableScope(("NBN_DEFAULT_ADVERTISE_HOST", "10.20.30.41"));

        var connections = new ConnectionViewModel
        {
            IoHost = "10.20.30.41",
            IoPortText = "12050",
            SettingsPortText = "12010"
        };

        var launchPreparer = new RecordingLocalProjectLaunchPreparer("Build failed (code 1). io");
        var vm = CreateViewModel(connections, new FakeWorkbenchClient(), launchPreparer);

        vm.StartIoCommand.Execute(null);

        await WaitForAsync(() => string.Equals(vm.IoLaunchStatus, "Build failed (code 1). io", StringComparison.Ordinal));

        Assert.Contains("--bind-host 0.0.0.0 --port 12050", launchPreparer.LastRuntimeArgs, StringComparison.Ordinal);
        Assert.Contains("--advertise-host 10.20.30.41", launchPreparer.LastRuntimeArgs, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StartWorkerCommand_DoesNotAdvertiseDiscoveredRemoteHost_ForLocalLaunch()
    {
        using var _ = new EnvironmentVariableScope(("NBN_DEFAULT_ADVERTISE_HOST", "192.168.0.14"));

        var connections = new ConnectionViewModel
        {
            WorkerHost = "203.0.113.55",
            WorkerPortText = "12041",
            SettingsPortText = "12010"
        };

        var launchPreparer = new RecordingLocalProjectLaunchPreparer("Build failed (code 1). worker");
        var vm = CreateViewModel(connections, new FakeWorkbenchClient(), launchPreparer);

        vm.StartWorkerCommand.Execute(null);

        await WaitForAsync(() => string.Equals(vm.WorkerLaunchStatus, "Build failed (code 1). worker", StringComparison.Ordinal));

        Assert.Contains("--bind-host 0.0.0.0 --port 12041", launchPreparer.LastRuntimeArgs, StringComparison.Ordinal);
        Assert.DoesNotContain("--advertise-host 203.0.113.55", launchPreparer.LastRuntimeArgs, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ApplyWorkerPolicyCommand_WritesSettingsBackedValues()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true
        };

        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(connections, client);
        vm.WorkerCapabilityRefreshSecondsText = "45";
        vm.WorkerPressureRebalanceWindowText = "8";
        vm.WorkerPressureViolationRatioText = "0.75";
        vm.WorkerPressureTolerancePercentText = "4.5";

        vm.ApplyWorkerPolicyCommand.Execute(null);

        await WaitForAsync(() => string.Equals(vm.WorkerPolicyStatus, "Worker policy updated.", StringComparison.Ordinal));

        Assert.Contains(client.SettingCalls, call => call.Key == WorkerCapabilitySettingsKeys.BenchmarkRefreshSecondsKey && call.Value == "45");
        Assert.Contains(client.SettingCalls, call => call.Key == WorkerCapabilitySettingsKeys.PressureRebalanceWindowKey && call.Value == "8");
        Assert.Contains(client.SettingCalls, call => call.Key == WorkerCapabilitySettingsKeys.PressureViolationRatioKey && call.Value == "0.75");
        Assert.Contains(client.SettingCalls, call => call.Key == WorkerCapabilitySettingsKeys.PressureLimitTolerancePercentKey && call.Value == "4.5");
    }

    [Fact]
    public void PullSettings_DefaultsToCurrentSettingsCoordinates_AndDoesNotOverwriteCustomSource()
    {
        var connections = new ConnectionViewModel
        {
            SettingsHost = "127.0.0.1",
            SettingsPortText = "12010",
            SettingsName = "SettingsMonitor"
        };

        var vm = CreateViewModel(connections, new FakeWorkbenchClient());

        Assert.Equal("127.0.0.1", vm.PullSettingsHost);
        Assert.Equal("12010", vm.PullSettingsPortText);
        Assert.Equal("SettingsMonitor", vm.PullSettingsName);

        connections.SettingsHost = "10.20.30.40";
        connections.SettingsPortText = "13010";
        connections.SettingsName = "RemoteSettings";

        Assert.Equal("10.20.30.40", vm.PullSettingsHost);
        Assert.Equal("13010", vm.PullSettingsPortText);
        Assert.Equal("RemoteSettings", vm.PullSettingsName);

        vm.PullSettingsHost = "192.168.1.44";
        connections.SettingsHost = "10.20.30.41";

        Assert.Equal("192.168.1.44", vm.PullSettingsHost);
    }

    [Fact]
    public async Task PullSettingsCommand_CopiesImportableSettings_AndSkipsDiscoveryEndpoints()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true
        };
        var client = new FakeWorkbenchClient
        {
            RemoteSettingsResponse = new SettingListResponse
            {
                Settings =
                {
                    new SettingValue
                    {
                        Key = "tick.cadence.hz",
                        Value = "24",
                        UpdatedMs = 11
                    },
                    new SettingValue
                    {
                        Key = "plasticity.system.enabled",
                        Value = "false",
                        UpdatedMs = 12
                    },
                    new SettingValue
                    {
                        Key = ServiceEndpointSettings.HiveMindKey,
                        Value = "10.20.30.40:12020/HiveMindRemote",
                        UpdatedMs = 13
                    }
                }
            }
        };

        var vm = CreateViewModel(connections, client);
        vm.UpdateSetting(new SettingItem("tick.cadence.hz", "8", "1"));
        await WaitForAsync(() => vm.Settings.Count == 1);

        var cadence = Assert.Single(vm.Settings);
        cadence.Value = "16";
        Assert.True(cadence.IsDirty);

        vm.PullSettingsHost = "10.20.30.50";
        vm.PullSettingsPortText = "12010";
        vm.PullSettingsName = "SettingsMonitorRemote";
        vm.PullSettingsCommand.Execute(null);

        await WaitForAsync(() => client.SettingCalls.Count == 2);
        await WaitForAsync(() => string.Equals(cadence.Value, "24", StringComparison.Ordinal));
        await WaitForAsync(() => vm.Settings.Count == 2);

        Assert.Contains(client.RemoteSettingsCalls, call =>
            string.Equals(call.Host, "10.20.30.50", StringComparison.Ordinal)
            && call.Port == 12010
            && string.Equals(call.ActorName, "SettingsMonitorRemote", StringComparison.Ordinal));
        Assert.Contains(client.SettingCalls, call => call.Key == "tick.cadence.hz" && call.Value == "24");
        Assert.Contains(client.SettingCalls, call => call.Key == "plasticity.system.enabled" && call.Value == "false");
        Assert.DoesNotContain(client.SettingCalls, call => string.Equals(call.Key, ServiceEndpointSettings.HiveMindKey, StringComparison.Ordinal));
        Assert.False(cadence.IsDirty);
        Assert.Equal("24", cadence.Value);
        Assert.Contains(vm.Settings, entry => entry.Key == "plasticity.system.enabled" && entry.Value == "false");
        Assert.Equal(
            "Pulled 2 setting(s) from 10.20.30.50:12010/SettingsMonitorRemote. Skipped 1 endpoint setting(s).",
            vm.SettingsPullStatus);
        Assert.Equal(vm.SettingsPullStatus, vm.StatusMessage);
    }

    [Fact]
    public async Task PullSettingsCommand_WhenSourceIsUnreachable_IncludesTcpAndFirewallGuidance()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true
        };
        var client = new FakeWorkbenchClient
        {
            RemoteSettingsResponse = null,
            ProbeResult = new TcpEndpointProbeResult(false, "TCP connect to 192.168.0.103:12010 timed out.")
        };

        var vm = CreateViewModel(connections, client);
        vm.PullSettingsHost = "192.168.0.103";
        vm.PullSettingsPortText = "12010";
        vm.PullSettingsName = "SettingsMonitor";

        vm.PullSettingsCommand.Execute(null);

        await WaitForAsync(() => vm.SettingsPullStatus.Contains("TCP connect to 192.168.0.103:12010 timed out.", StringComparison.Ordinal));

        Assert.Contains("Using the same port number on another machine is fine.", vm.SettingsPullStatus, StringComparison.Ordinal);
        Assert.Contains("bound to 127.0.0.1", vm.SettingsPullStatus, StringComparison.Ordinal);
        Assert.Contains("firewall", vm.SettingsPullStatus, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task PullSettingsCommand_WhenSourceTcpIsReachable_IncludesWorkbenchClientGuidance()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true
        };
        var client = new FakeWorkbenchClient
        {
            RemoteSettingsResponse = null,
            ProbeResult = new TcpEndpointProbeResult(true, "TCP connect to 192.168.0.103:12010 succeeded.")
        };

        var vm = CreateViewModel(connections, client);
        vm.PullSettingsHost = "192.168.0.103";
        vm.PullSettingsPortText = "12010";
        vm.PullSettingsName = "SettingsMonitor";

        vm.PullSettingsCommand.Execute(null);

        await WaitForAsync(() => vm.SettingsPullStatus.Contains("local Workbench client is not still bound to 127.0.0.1", StringComparison.Ordinal));

        Assert.Contains("compatible NBN remoting build", vm.SettingsPullStatus, StringComparison.Ordinal);
        Assert.Contains("restarting Workbench", vm.SettingsPullStatus, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UpdateSetting_WorkerPolicyValues_UpdateDedicatedInputs()
    {
        var vm = CreateViewModel(new ConnectionViewModel(), new FakeWorkbenchClient());

        vm.UpdateSetting(new SettingItem(WorkerCapabilitySettingsKeys.BenchmarkRefreshSecondsKey, "90", "1"));
        vm.UpdateSetting(new SettingItem(WorkerCapabilitySettingsKeys.PressureRebalanceWindowKey, "7", "2"));
        vm.UpdateSetting(new SettingItem(WorkerCapabilitySettingsKeys.PressureViolationRatioKey, "0.6", "3"));
        vm.UpdateSetting(new SettingItem(WorkerCapabilitySettingsKeys.PressureLimitTolerancePercentKey, "3.5", "4"));

        await WaitForAsync(() => string.Equals(vm.WorkerCapabilityRefreshSecondsText, "90", StringComparison.Ordinal));

        Assert.Equal("90", vm.WorkerCapabilityRefreshSecondsText);
        Assert.Equal("7", vm.WorkerPressureRebalanceWindowText);
        Assert.Equal("0.6", vm.WorkerPressureViolationRatioText);
        Assert.Equal("3.5", vm.WorkerPressureTolerancePercentText);
    }

    [Fact]
    public async Task StopAllCommand_StopsWorkerRunner()
    {
        var connections = new ConnectionViewModel();
        var vm = CreateViewModel(connections, new FakeWorkbenchClient());
        vm.WorkerLaunchStatus = "Running";

        vm.StopAllCommand.Execute(null);
        await WaitForAsync(() => string.Equals(vm.WorkerLaunchStatus, "Not running.", StringComparison.Ordinal));

        Assert.Equal("Not running.", vm.WorkerLaunchStatus);
    }

    [Fact]
    public async Task RefreshSettingsAsync_MapsWorkerNodeIntoStatusAndNodeList()
    {
        var connections = new ConnectionViewModel();
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var workerId = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse
            {
                Nodes =
                {
                    new NodeStatus
                    {
                        NodeId = workerId.ToProtoUuid(),
                        LogicalName = connections.WorkerLogicalName,
                        Address = $"{connections.WorkerHost}:{connections.WorkerPortText}",
                        RootActorName = connections.WorkerRootName,
                        LastSeenMs = (ulong)nowMs,
                        IsAlive = true
                    }
                }
            },
            BrainsResponse = new BrainListResponse(),
            SettingsResponse = new SettingListResponse()
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        Assert.True(connections.WorkerDiscoverable);
        Assert.Equal("1 active worker", connections.WorkerStatus);
        var workerNode = Assert.Single(vm.Nodes);
        Assert.Equal(connections.WorkerLogicalName, workerNode.LogicalName);
        Assert.Equal(connections.WorkerRootName, workerNode.RootActor);
        Assert.Equal("online", workerNode.Status);
        var workerEndpoint = Assert.Single(vm.WorkerEndpoints);
        Assert.Equal(workerId, workerEndpoint.NodeId);
        Assert.Equal("active", workerEndpoint.Status);
        Assert.Equal("none", workerEndpoint.BrainHints);
        Assert.Equal("1 active worker", vm.WorkerEndpointSummary);
    }

    [Fact]
    public async Task RefreshSettingsAsync_MapsServiceEndpointSettings_ToConnectionInputs()
    {
        var connections = new ConnectionViewModel();
        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse(),
            BrainsResponse = new BrainListResponse(),
            SettingsResponse = new SettingListResponse
            {
                Settings =
                {
                    new SettingValue
                    {
                        Key = ServiceEndpointSettings.HiveMindKey,
                        Value = "10.20.30.40:12022/HiveRemote",
                        UpdatedMs = 11
                    },
                    new SettingValue
                    {
                        Key = ServiceEndpointSettings.IoGatewayKey,
                        Value = "10.20.30.41:12052/io-remote",
                        UpdatedMs = 12
                    },
                    new SettingValue
                    {
                        Key = ServiceEndpointSettings.ReproductionManagerKey,
                        Value = "10.20.30.42:12072/repro-remote",
                        UpdatedMs = 13
                    },
                    new SettingValue
                    {
                        Key = ServiceEndpointSettings.SpeciationManagerKey,
                        Value = "10.20.30.45:12082/speciation-remote",
                        UpdatedMs = 14
                    },
                    new SettingValue
                    {
                        Key = ServiceEndpointSettings.WorkerNodeKey,
                        Value = "10.20.30.43:12044/worker-remote",
                        UpdatedMs = 15
                    },
                    new SettingValue
                    {
                        Key = ServiceEndpointSettings.ObservabilityKey,
                        Value = "10.20.30.44:12064/DebugHubRemote",
                        UpdatedMs = 16
                    }
                }
            }
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        Assert.Equal("10.20.30.40", connections.HiveMindHost);
        Assert.Equal("12022", connections.HiveMindPortText);
        Assert.Equal("HiveRemote", connections.HiveMindName);
        Assert.Equal("10.20.30.41", connections.IoHost);
        Assert.Equal("12052", connections.IoPortText);
        Assert.Equal("io-remote", connections.IoGateway);
        Assert.Equal("10.20.30.42", connections.ReproHost);
        Assert.Equal("12072", connections.ReproPortText);
        Assert.Equal("repro-remote", connections.ReproManager);
        Assert.Equal("10.20.30.45", connections.SpeciationHost);
        Assert.Equal("12082", connections.SpeciationPortText);
        Assert.Equal("speciation-remote", connections.SpeciationManager);
        Assert.Equal("10.20.30.43", connections.WorkerHost);
        Assert.Equal("12044", connections.WorkerPortText);
        Assert.Equal("worker-remote", connections.WorkerRootName);
        Assert.Equal("10.20.30.44", connections.ObsHost);
        Assert.Equal("12064", connections.ObsPortText);
        Assert.Equal("DebugHubRemote", connections.DebugHub);
        Assert.Equal("10.20.30.40:12022/HiveRemote", connections.HiveMindEndpointDisplay);
        Assert.Equal("10.20.30.41:12052/io-remote", connections.IoEndpointDisplay);
        Assert.Equal("10.20.30.42:12072/repro-remote", connections.ReproEndpointDisplay);
        Assert.Equal("10.20.30.45:12082/speciation-remote", connections.SpeciationEndpointDisplay);
        Assert.Equal("10.20.30.44:12064/DebugHubRemote", connections.ObsEndpointDisplay);
    }

    [Fact]
    public async Task RefreshSettingsAsync_UsesDiscoveredActorNames_ForOnlineStatus()
    {
        var connections = new ConnectionViewModel
        {
            IoGateway = "io-default"
        };
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse
            {
                Nodes =
                {
                    new NodeStatus
                    {
                        NodeId = Guid.NewGuid().ToProtoUuid(),
                        LogicalName = "io-remote-node",
                        Address = "10.20.30.41:12052",
                        RootActorName = "io-remote",
                        LastSeenMs = (ulong)nowMs,
                        IsAlive = true
                    }
                }
            },
            BrainsResponse = new BrainListResponse(),
            SettingsResponse = new SettingListResponse
            {
                Settings =
                {
                    new SettingValue
                    {
                        Key = ServiceEndpointSettings.IoGatewayKey,
                        Value = "10.20.30.41:12052/io-remote",
                        UpdatedMs = 21
                    }
                }
            }
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        Assert.True(connections.IoDiscoverable);
        Assert.Equal("10.20.30.41:12052/io-remote", connections.IoEndpointDisplay);
        Assert.Equal("Online", connections.IoStatus);
        var ioEndpoint = Assert.Single(vm.Endpoints, endpoint => endpoint.ServiceName == "IO Gateway");
        Assert.Equal("online", ioEndpoint.Status);
        Assert.Equal("10.20.30.41:12052/io-remote", ioEndpoint.EndpointDisplay);
    }

    [Fact]
    public async Task RefreshSettingsAsync_InvalidServiceEndpointSettings_DoNotOverwriteConnectionInputs()
    {
        var connections = new ConnectionViewModel
        {
            HiveMindHost = "127.0.0.1",
            HiveMindPortText = "12020",
            HiveMindName = "HiveMind",
            IoHost = "127.0.0.1",
            IoPortText = "12050",
            IoGateway = "io-gateway",
            ReproHost = "127.0.0.1",
            ReproPortText = "12070",
            ReproManager = "ReproductionManager",
            SpeciationHost = "127.0.0.1",
            SpeciationPortText = "12080",
            SpeciationManager = "SpeciationManager",
            WorkerHost = "127.0.0.1",
            WorkerPortText = "12041",
            WorkerRootName = "worker-node",
            ObsHost = "127.0.0.1",
            ObsPortText = "12060",
            DebugHub = "DebugHub"
        };
        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse(),
            BrainsResponse = new BrainListResponse(),
            SettingsResponse = new SettingListResponse
            {
                Settings =
                {
                    new SettingValue
                    {
                        Key = ServiceEndpointSettings.HiveMindKey,
                        Value = "invalid-hive-value",
                        UpdatedMs = 31
                    },
                    new SettingValue
                    {
                        Key = ServiceEndpointSettings.IoGatewayKey,
                        Value = "127.0.0.1:not-a-port/io-gateway",
                        UpdatedMs = 32
                    },
                    new SettingValue
                    {
                        Key = ServiceEndpointSettings.ReproductionManagerKey,
                        Value = "127.0.0.1:not-a-port/repro",
                        UpdatedMs = 33
                    },
                    new SettingValue
                    {
                        Key = ServiceEndpointSettings.SpeciationManagerKey,
                        Value = "invalid-speciation-value",
                        UpdatedMs = 34
                    },
                    new SettingValue
                    {
                        Key = ServiceEndpointSettings.WorkerNodeKey,
                        Value = "invalid-worker-value",
                        UpdatedMs = 35
                    },
                    new SettingValue
                    {
                        Key = ServiceEndpointSettings.ObservabilityKey,
                        Value = "127.0.0.1:not-a-port/debug",
                        UpdatedMs = 36
                    }
                }
            }
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        Assert.Equal("127.0.0.1", connections.HiveMindHost);
        Assert.Equal("12020", connections.HiveMindPortText);
        Assert.Equal("HiveMind", connections.HiveMindName);
        Assert.Equal("127.0.0.1", connections.IoHost);
        Assert.Equal("12050", connections.IoPortText);
        Assert.Equal("io-gateway", connections.IoGateway);
        Assert.Equal("127.0.0.1", connections.ReproHost);
        Assert.Equal("12070", connections.ReproPortText);
        Assert.Equal("ReproductionManager", connections.ReproManager);
        Assert.Equal("127.0.0.1", connections.SpeciationHost);
        Assert.Equal("12080", connections.SpeciationPortText);
        Assert.Equal("SpeciationManager", connections.SpeciationManager);
        Assert.Equal("127.0.0.1", connections.WorkerHost);
        Assert.Equal("12041", connections.WorkerPortText);
        Assert.Equal("worker-node", connections.WorkerRootName);
        Assert.Equal("127.0.0.1", connections.ObsHost);
        Assert.Equal("12060", connections.ObsPortText);
        Assert.Equal("DebugHub", connections.DebugHub);
    }

    [Fact]
    public async Task UpdateSetting_ServiceEndpointValues_UpdateConnectionInputs()
    {
        var connections = new ConnectionViewModel();
        var vm = CreateViewModel(connections, new FakeWorkbenchClient());

        vm.UpdateSetting(new SettingItem(
            ServiceEndpointSettings.IoGatewayKey,
            "192.168.100.9:13050/io-prod",
            "1"));
        vm.UpdateSetting(new SettingItem(
            ServiceEndpointSettings.HiveMindKey,
            "192.168.100.10:13020/HiveProd",
            "2"));
        vm.UpdateSetting(new SettingItem(
            ServiceEndpointSettings.ReproductionManagerKey,
            "192.168.100.11:13070/ReproProd",
            "3"));
        vm.UpdateSetting(new SettingItem(
            ServiceEndpointSettings.SpeciationManagerKey,
            "192.168.100.14:13080/SpeciationProd",
            "4"));
        vm.UpdateSetting(new SettingItem(
            ServiceEndpointSettings.WorkerNodeKey,
            "192.168.100.12:13041/worker-prod",
            "5"));
        vm.UpdateSetting(new SettingItem(
            ServiceEndpointSettings.ObservabilityKey,
            "192.168.100.13:13060/DebugProd",
            "6"));

        await WaitForAsync(() =>
            string.Equals(connections.IoHost, "192.168.100.9", StringComparison.Ordinal)
            && string.Equals(connections.IoPortText, "13050", StringComparison.Ordinal)
            && string.Equals(connections.IoGateway, "io-prod", StringComparison.Ordinal)
            && string.Equals(connections.HiveMindHost, "192.168.100.10", StringComparison.Ordinal)
            && string.Equals(connections.HiveMindPortText, "13020", StringComparison.Ordinal)
            && string.Equals(connections.HiveMindName, "HiveProd", StringComparison.Ordinal)
            && string.Equals(connections.ReproHost, "192.168.100.11", StringComparison.Ordinal)
            && string.Equals(connections.ReproPortText, "13070", StringComparison.Ordinal)
            && string.Equals(connections.ReproManager, "ReproProd", StringComparison.Ordinal)
            && string.Equals(connections.SpeciationHost, "192.168.100.14", StringComparison.Ordinal)
            && string.Equals(connections.SpeciationPortText, "13080", StringComparison.Ordinal)
            && string.Equals(connections.SpeciationManager, "SpeciationProd", StringComparison.Ordinal)
            && string.Equals(connections.WorkerHost, "192.168.100.12", StringComparison.Ordinal)
            && string.Equals(connections.WorkerPortText, "13041", StringComparison.Ordinal)
            && string.Equals(connections.WorkerRootName, "worker-prod", StringComparison.Ordinal)
            && string.Equals(connections.ObsHost, "192.168.100.13", StringComparison.Ordinal)
            && string.Equals(connections.ObsPortText, "13060", StringComparison.Ordinal)
            && string.Equals(connections.DebugHub, "DebugProd", StringComparison.Ordinal));
    }

    [Fact]
    public async Task UpdateSetting_InvalidServiceEndpointValues_DoNotOverwriteConnectionInputs()
    {
        var connections = new ConnectionViewModel
        {
            ReproHost = "127.0.0.1",
            ReproPortText = "12070",
            ReproManager = "ReproductionManager",
            SpeciationHost = "127.0.0.1",
            SpeciationPortText = "12080",
            SpeciationManager = "SpeciationManager",
            WorkerHost = "127.0.0.1",
            WorkerPortText = "12041",
            WorkerRootName = "worker-node",
            ObsHost = "127.0.0.1",
            ObsPortText = "12060",
            DebugHub = "DebugHub"
        };
        var vm = CreateViewModel(connections, new FakeWorkbenchClient());

        vm.UpdateSetting(new SettingItem(
            ServiceEndpointSettings.ReproductionManagerKey,
            "127.0.0.1:not-a-port/repro",
            "1"));
        vm.UpdateSetting(new SettingItem(
            ServiceEndpointSettings.SpeciationManagerKey,
            "127.0.0.1:not-a-port/speciation",
            "2"));
        vm.UpdateSetting(new SettingItem(
            ServiceEndpointSettings.WorkerNodeKey,
            "invalid-worker-value",
            "3"));
        vm.UpdateSetting(new SettingItem(
            ServiceEndpointSettings.ObservabilityKey,
            "127.0.0.1:not-a-port/debug",
            "4"));

        await WaitForAsync(() =>
            string.Equals(connections.ReproHost, "127.0.0.1", StringComparison.Ordinal)
            && string.Equals(connections.ReproPortText, "12070", StringComparison.Ordinal)
            && string.Equals(connections.ReproManager, "ReproductionManager", StringComparison.Ordinal)
            && string.Equals(connections.SpeciationHost, "127.0.0.1", StringComparison.Ordinal)
            && string.Equals(connections.SpeciationPortText, "12080", StringComparison.Ordinal)
            && string.Equals(connections.SpeciationManager, "SpeciationManager", StringComparison.Ordinal)
            && string.Equals(connections.WorkerHost, "127.0.0.1", StringComparison.Ordinal)
            && string.Equals(connections.WorkerPortText, "12041", StringComparison.Ordinal)
            && string.Equals(connections.WorkerRootName, "worker-node", StringComparison.Ordinal)
            && string.Equals(connections.ObsHost, "127.0.0.1", StringComparison.Ordinal)
            && string.Equals(connections.ObsPortText, "12060", StringComparison.Ordinal)
            && string.Equals(connections.DebugHub, "DebugHub", StringComparison.Ordinal));
    }

    [Fact]
    public async Task RefreshSettingsAsync_TracksMultipleActiveWorkerEndpoints()
    {
        var connections = new ConnectionViewModel
        {
            WorkerRootName = "custom-worker-root"
        };
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var workerA = Guid.NewGuid();
        var workerB = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse
            {
                Nodes =
                {
                    new NodeStatus
                    {
                        NodeId = workerA.ToProtoUuid(),
                        LogicalName = "nbn.worker.east",
                        Address = "127.0.0.1:12041",
                        RootActorName = "worker-node-east",
                        LastSeenMs = (ulong)nowMs,
                        IsAlive = true
                    },
                    new NodeStatus
                    {
                        NodeId = workerB.ToProtoUuid(),
                        LogicalName = "external-runner",
                        Address = "127.0.0.1:12042",
                        RootActorName = connections.WorkerRootName,
                        LastSeenMs = (ulong)(nowMs - 2_000),
                        IsAlive = true
                    }
                }
            },
            BrainsResponse = new BrainListResponse(),
            SettingsResponse = new SettingListResponse()
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        Assert.True(connections.WorkerDiscoverable);
        Assert.Equal("2 active workers", connections.WorkerStatus);
        Assert.Equal(2, vm.WorkerEndpoints.Count);
        Assert.All(vm.WorkerEndpoints, endpoint => Assert.Equal("active", endpoint.Status));
        Assert.All(vm.WorkerEndpoints, endpoint => Assert.Equal("none", endpoint.BrainHints));
        Assert.Equal("2 active workers", vm.WorkerEndpointSummary);
    }

    [Fact]
    public async Task RefreshSettingsAsync_WorkerEndpoints_ReportDegradedAndFailedStates()
    {
        var connections = new ConnectionViewModel();
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var degradedWorker = Guid.NewGuid();
        var failedWorker = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse
            {
                Nodes =
                {
                    new NodeStatus
                    {
                        NodeId = degradedWorker.ToProtoUuid(),
                        LogicalName = "nbn.worker.degraded",
                        Address = "127.0.0.1:12043",
                        RootActorName = "worker-node-degraded",
                        LastSeenMs = (ulong)(nowMs - 20_000),
                        IsAlive = true
                    },
                    new NodeStatus
                    {
                        NodeId = failedWorker.ToProtoUuid(),
                        LogicalName = "nbn.worker.failed",
                        Address = "127.0.0.1:12044",
                        RootActorName = "worker-node-failed",
                        LastSeenMs = (ulong)(nowMs - 70_000),
                        IsAlive = true
                    }
                }
            },
            BrainsResponse = new BrainListResponse(),
            SettingsResponse = new SettingListResponse()
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        Assert.False(connections.WorkerDiscoverable);
        Assert.Equal("1 degraded worker, 1 failed worker", connections.WorkerStatus);
        Assert.Equal(2, vm.WorkerEndpoints.Count);
        Assert.Contains(vm.WorkerEndpoints, endpoint => endpoint.NodeId == degradedWorker && endpoint.Status == "degraded");
        Assert.Contains(vm.WorkerEndpoints, endpoint => endpoint.NodeId == failedWorker && endpoint.Status == "failed");
        Assert.All(vm.WorkerEndpoints, endpoint => Assert.Equal("none", endpoint.BrainHints));
        Assert.Equal("1 degraded worker, 1 failed worker", vm.WorkerEndpointSummary);
    }

    [Fact]
    public async Task RefreshSettingsAsync_WorkerEndpoints_ReportLimitedState_WhenResourceLimitsBlockPlacement()
    {
        var connections = new ConnectionViewModel();
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var limitedWorker = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse
            {
                Nodes =
                {
                    new NodeStatus
                    {
                        NodeId = limitedWorker.ToProtoUuid(),
                        LogicalName = connections.WorkerLogicalName,
                        Address = $"{connections.WorkerHost}:{connections.WorkerPortText}",
                        RootActorName = connections.WorkerRootName,
                        LastSeenMs = (ulong)nowMs,
                        IsAlive = true
                    }
                }
            },
            WorkerInventoryResponse = new WorkerInventorySnapshotResponse
            {
                SnapshotMs = (ulong)nowMs,
                Workers =
                {
                    new WorkerReadinessCapability
                    {
                        NodeId = limitedWorker.ToProtoUuid(),
                        LogicalName = connections.WorkerLogicalName,
                        Address = $"{connections.WorkerHost}:{connections.WorkerPortText}",
                        RootActorName = connections.WorkerRootName,
                        IsAlive = true,
                        IsReady = true,
                        LastSeenMs = (ulong)nowMs,
                        HasCapabilities = true,
                        CapabilityTimeMs = (ulong)nowMs,
                        Capabilities = new Nbn.Proto.Settings.NodeCapabilities
                        {
                            CpuCores = 8,
                            RamFreeBytes = 8UL * 1024 * 1024 * 1024,
                            RamTotalBytes = 16UL * 1024 * 1024 * 1024,
                            StorageFreeBytes = 50UL * 1024 * 1024 * 1024,
                            StorageTotalBytes = 500UL * 1024 * 1024 * 1024,
                            CpuScore = 40f,
                            CpuLimitPercent = 100,
                            RamLimitPercent = 100,
                            StorageLimitPercent = 80
                        }
                    }
                }
            },
            BrainsResponse = new BrainListResponse(),
            SettingsResponse = new SettingListResponse()
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        Assert.True(connections.WorkerDiscoverable);
        var endpoint = Assert.Single(vm.WorkerEndpoints);
        Assert.Equal("limited", endpoint.Status);
        Assert.Contains("Storage used", endpoint.PlacementDetail, StringComparison.Ordinal);
        Assert.Equal("1 limited worker", vm.WorkerEndpointSummary);
    }

    [Fact]
    public async Task RefreshSettingsAsync_WorkerEndpoints_UseSettingsSnapshotTime_ForCachedRemoteWorkerExpiry()
    {
        var connections = new ConnectionViewModel();
        var serverSeenMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 120_000;
        var workerId = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse
            {
                Nodes =
                {
                    new NodeStatus
                    {
                        NodeId = workerId.ToProtoUuid(),
                        LogicalName = connections.WorkerLogicalName,
                        Address = $"{connections.WorkerHost}:{connections.WorkerPortText}",
                        RootActorName = connections.WorkerRootName,
                        LastSeenMs = (ulong)serverSeenMs,
                        IsAlive = true
                    }
                }
            },
            WorkerInventoryResponse = new WorkerInventorySnapshotResponse
            {
                SnapshotMs = (ulong)serverSeenMs,
                Workers =
                {
                    new WorkerReadinessCapability
                    {
                        NodeId = workerId.ToProtoUuid(),
                        LogicalName = connections.WorkerLogicalName,
                        Address = $"{connections.WorkerHost}:{connections.WorkerPortText}",
                        RootActorName = connections.WorkerRootName,
                        IsAlive = true,
                        IsReady = true,
                        LastSeenMs = (ulong)serverSeenMs,
                        HasCapabilities = true,
                        Capabilities = new Nbn.Proto.Settings.NodeCapabilities
                        {
                            CpuCores = 8,
                            RamFreeBytes = 8UL * 1024 * 1024 * 1024,
                            RamTotalBytes = 16UL * 1024 * 1024 * 1024,
                            StorageFreeBytes = 96UL * 1024 * 1024 * 1024,
                            StorageTotalBytes = 128UL * 1024 * 1024 * 1024,
                            CpuLimitPercent = 100,
                            RamLimitPercent = 100,
                            StorageLimitPercent = 100
                        }
                    }
                }
            },
            BrainsResponse = new BrainListResponse(),
            SettingsResponse = new SettingListResponse()
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        Assert.Equal("active", Assert.Single(vm.WorkerEndpoints).Status);

        client.NodesResponse = new NodeListResponse();
        client.WorkerInventoryResponse = new WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)(serverSeenMs + 46_000)
        };

        await vm.RefreshSettingsAsync();

        var workerEndpoint = Assert.Single(vm.WorkerEndpoints);
        Assert.Equal("failed", workerEndpoint.Status);
        Assert.Equal("1 failed worker", vm.WorkerEndpointSummary);
        Assert.False(connections.WorkerDiscoverable);
    }

    [Fact]
    public async Task RefreshSettingsAsync_WorkerEndpoints_OfflineWhenNoWorkersVisible()
    {
        var connections = new ConnectionViewModel();
        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse(),
            BrainsResponse = new BrainListResponse(),
            SettingsResponse = new SettingListResponse()
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        Assert.False(connections.WorkerDiscoverable);
        Assert.Equal("Offline", connections.WorkerStatus);
        Assert.Empty(vm.WorkerEndpoints);
        Assert.Equal("No active workers.", vm.WorkerEndpointSummary);
        Assert.Equal(6, vm.Endpoints.Count);
        Assert.DoesNotContain(vm.Endpoints, endpoint => endpoint.ServiceName == "Worker Node");
        var settingsEndpoint = Assert.Single(vm.Endpoints, endpoint => endpoint.ServiceName == "SettingsMonitor");
        Assert.Equal("online", settingsEndpoint.Status);
        Assert.Equal($"{connections.SettingsHost}:{connections.SettingsPortText}/{connections.SettingsName}", settingsEndpoint.EndpointDisplay);
        Assert.All(
            vm.Endpoints.Where(endpoint => endpoint.ServiceName != "SettingsMonitor"),
            endpoint => Assert.Equal("offline", endpoint.Status));
    }

    [Fact]
    public async Task RefreshSettingsAsync_WhenSettingsDisconnected_ClearsDiscoveryIndicators()
    {
        var connections = new ConnectionViewModel
        {
            IoDiscoverable = true,
            ObsDiscoverable = true,
            ReproDiscoverable = true,
            SpeciationDiscoverable = true,
            WorkerDiscoverable = true,
            HiveMindDiscoverable = true,
            IoStatus = "Online",
            ObsStatus = "Online",
            ReproStatus = "Online",
            SpeciationStatus = "Online",
            WorkerStatus = "1 active worker",
            HiveMindStatus = "Online",
            IoEndpointDisplay = "10.0.0.1:12050/io",
            ObsEndpointDisplay = "10.0.0.2:12060/debug",
            ReproEndpointDisplay = "10.0.0.3:12070/repro",
            SpeciationEndpointDisplay = "10.0.0.6:12080/speciation",
            WorkerEndpointDisplay = "1 active worker",
            HiveMindEndpointDisplay = "10.0.0.4:12020/hive"
        };
        var vm = CreateViewModel(connections, new FakeWorkbenchClient());
        connections.SettingsConnected = false;

        await vm.RefreshSettingsAsync();

        Assert.False(connections.IoDiscoverable);
        Assert.False(connections.ObsDiscoverable);
        Assert.False(connections.ReproDiscoverable);
        Assert.False(connections.SpeciationDiscoverable);
        Assert.False(connections.WorkerDiscoverable);
        Assert.False(connections.HiveMindDiscoverable);
        Assert.Equal("Offline", connections.IoStatus);
        Assert.Equal("Offline", connections.ObsStatus);
        Assert.Equal("Offline", connections.ReproStatus);
        Assert.Equal("Offline", connections.SpeciationStatus);
        Assert.Equal("Offline", connections.WorkerStatus);
        Assert.Equal("Offline", connections.HiveMindStatus);
        Assert.Equal("Missing", connections.IoEndpointDisplay);
        Assert.Equal("Missing", connections.ObsEndpointDisplay);
        Assert.Equal("Missing", connections.ReproEndpointDisplay);
        Assert.Equal("Missing", connections.SpeciationEndpointDisplay);
        Assert.Equal("Missing", connections.WorkerEndpointDisplay);
        Assert.Equal("Missing", connections.HiveMindEndpointDisplay);
        Assert.Empty(vm.Nodes);
        Assert.Empty(vm.WorkerEndpoints);
        Assert.Empty(vm.Actors);
        Assert.Equal("No active workers.", vm.WorkerEndpointSummary);
        Assert.Equal(6, vm.Endpoints.Count);
        Assert.DoesNotContain(vm.Endpoints, endpoint => endpoint.ServiceName == "Worker Node");
        var settingsEndpoint = Assert.Single(vm.Endpoints, endpoint => endpoint.ServiceName == "SettingsMonitor");
        Assert.Equal("offline", settingsEndpoint.Status);
        Assert.Equal($"{connections.SettingsHost}:{connections.SettingsPortText}/{connections.SettingsName}", settingsEndpoint.EndpointDisplay);
        Assert.All(vm.Endpoints, endpoint => Assert.Equal("offline", endpoint.Status));
        Assert.All(
            vm.Endpoints.Where(endpoint => endpoint.ServiceName != "SettingsMonitor"),
            endpoint => Assert.Equal("Missing", endpoint.EndpointDisplay));
    }

    [Fact]
    public async Task Endpoints_SettingsChip_TracksSettingsConnectionWithoutManualRefresh()
    {
        var connections = new ConnectionViewModel();
        var vm = CreateViewModel(connections, new FakeWorkbenchClient());

        var settingsEndpoint = Assert.Single(vm.Endpoints, endpoint => endpoint.ServiceName == "SettingsMonitor");
        Assert.Equal("offline", settingsEndpoint.Status);

        connections.SettingsConnected = true;
        await WaitForAsync(() =>
            string.Equals(
                vm.Endpoints.Single(endpoint => endpoint.ServiceName == "SettingsMonitor").Status,
                "online",
                StringComparison.Ordinal));

        connections.SettingsConnected = false;
        await WaitForAsync(() =>
            string.Equals(
                vm.Endpoints.Single(endpoint => endpoint.ServiceName == "SettingsMonitor").Status,
                "offline",
                StringComparison.Ordinal));
    }

    [Fact]
    public async Task Endpoints_IoAndHiveMindChips_UseReadinessSignalsWhenDiscoveryLags()
    {
        var connections = new ConnectionViewModel
        {
            IoConnected = true,
            HiveMindConnected = true,
            IoDiscoverable = false,
            HiveMindDiscoverable = false
        };
        var vm = CreateViewModel(connections, new FakeWorkbenchClient());

        await WaitForAsync(() =>
            string.Equals(
                vm.Endpoints.Single(endpoint => endpoint.ServiceName == "IO Gateway").Status,
                "online",
                StringComparison.Ordinal)
            && string.Equals(
                vm.Endpoints.Single(endpoint => endpoint.ServiceName == "HiveMind").Status,
                "online",
                StringComparison.Ordinal));
    }

    [Fact]
    public async Task RefreshSettingsAsync_ObservabilityConnected_WhenDebugHubNodeIsAlive()
    {
        var connections = new ConnectionViewModel();
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse
            {
                Nodes =
                {
                    new NodeStatus
                    {
                        NodeId = Guid.NewGuid().ToProtoUuid(),
                        LogicalName = "nbn.obs",
                        Address = $"{connections.ObsHost}:{connections.ObsPortText}",
                        RootActorName = connections.DebugHub,
                        LastSeenMs = (ulong)nowMs,
                        IsAlive = true
                    }
                }
            },
            BrainsResponse = new BrainListResponse(),
            SettingsResponse = new SettingListResponse()
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        Assert.True(connections.ObsDiscoverable);
        Assert.Equal("Online", connections.ObsStatus);
    }

    [Fact]
    public async Task RefreshSettingsAsync_ObservabilityOffline_WhenHubNodesAreMissingOrStale()
    {
        var connections = new ConnectionViewModel();
        var staleMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 60_000;
        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse
            {
                Nodes =
                {
                    new NodeStatus
                    {
                        NodeId = Guid.NewGuid().ToProtoUuid(),
                        LogicalName = "nbn.obs",
                        Address = $"{connections.ObsHost}:{connections.ObsPortText}",
                        RootActorName = connections.DebugHub,
                        LastSeenMs = (ulong)staleMs,
                        IsAlive = true
                    }
                }
            },
            BrainsResponse = new BrainListResponse(),
            SettingsResponse = new SettingListResponse()
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        Assert.False(connections.ObsDiscoverable);
        Assert.Equal("Offline", connections.ObsStatus);
    }

    [Fact]
    public async Task RefreshSettingsAsync_IncludesBrainControllerRowsInHostedActorsList()
    {
        var connections = new ConnectionViewModel();
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var workerId = Guid.NewGuid();
        var brainId = Guid.NewGuid();
        var controllerActor = $"127.0.0.1:12041/worker-node/brain-{brainId:N}-root";

        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse
            {
                Nodes =
                {
                    new NodeStatus
                    {
                        NodeId = workerId.ToProtoUuid(),
                        LogicalName = connections.WorkerLogicalName,
                        Address = $"{connections.WorkerHost}:{connections.WorkerPortText}",
                        RootActorName = connections.WorkerRootName,
                        LastSeenMs = (ulong)nowMs,
                        IsAlive = true
                    }
                }
            },
            BrainsResponse = new BrainListResponse
            {
                Brains =
                {
                    new BrainStatus
                    {
                        BrainId = brainId.ToProtoUuid(),
                        SpawnedMs = (ulong)nowMs,
                        LastTickId = 12,
                        State = "Active"
                    }
                },
                Controllers =
                {
                    new BrainControllerStatus
                    {
                        BrainId = brainId.ToProtoUuid(),
                        NodeId = workerId.ToProtoUuid(),
                        ActorName = controllerActor,
                        LastSeenMs = (ulong)nowMs,
                        IsAlive = true
                    }
                }
            },
            SettingsResponse = new SettingListResponse()
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        Assert.Single(vm.Nodes);
        var controllerRow = Assert.Single(
            vm.Actors,
            node => node.RootActor == controllerActor);
        Assert.Contains("brain", controllerRow.LogicalName, StringComparison.OrdinalIgnoreCase);
        Assert.Contains($"brain {HostedActorBrainToken(brainId)}", controllerRow.LogicalName, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("online", controllerRow.Status);
        var workerEndpoint = Assert.Single(vm.WorkerEndpoints, endpoint => endpoint.NodeId == workerId);
        Assert.Equal(ShortBrainId(brainId), workerEndpoint.BrainHints);
    }

    [Fact]
    public async Task RefreshSettingsAsync_UsesSettingsClock_ForRemoteControllerFreshness()
    {
        var connections = new ConnectionViewModel();
        var serverSeenMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 120_000;
        var workerId = Guid.NewGuid();
        var brainId = Guid.NewGuid();
        var controllerActor = $"127.0.0.1:12041/worker-node/brain-{brainId:N}-root";
        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse
            {
                Nodes =
                {
                    new NodeStatus
                    {
                        NodeId = workerId.ToProtoUuid(),
                        LogicalName = connections.WorkerLogicalName,
                        Address = $"{connections.WorkerHost}:{connections.WorkerPortText}",
                        RootActorName = connections.WorkerRootName,
                        LastSeenMs = (ulong)serverSeenMs,
                        IsAlive = true
                    }
                }
            },
            WorkerInventoryResponse = new WorkerInventorySnapshotResponse
            {
                SnapshotMs = (ulong)serverSeenMs,
                Workers =
                {
                    new WorkerReadinessCapability
                    {
                        NodeId = workerId.ToProtoUuid(),
                        LogicalName = connections.WorkerLogicalName,
                        Address = $"{connections.WorkerHost}:{connections.WorkerPortText}",
                        RootActorName = connections.WorkerRootName,
                        IsAlive = true,
                        IsReady = true,
                        LastSeenMs = (ulong)serverSeenMs,
                        HasCapabilities = true,
                        Capabilities = new Nbn.Proto.Settings.NodeCapabilities
                        {
                            CpuCores = 8,
                            RamFreeBytes = 8UL * 1024 * 1024 * 1024,
                            RamTotalBytes = 16UL * 1024 * 1024 * 1024,
                            StorageFreeBytes = 64UL * 1024 * 1024 * 1024,
                            StorageTotalBytes = 128UL * 1024 * 1024 * 1024
                        }
                    }
                }
            },
            BrainsResponse = new BrainListResponse
            {
                Brains =
                {
                    new BrainStatus
                    {
                        BrainId = brainId.ToProtoUuid(),
                        SpawnedMs = (ulong)serverSeenMs,
                        LastTickId = 12,
                        State = "Active"
                    }
                },
                Controllers =
                {
                    new BrainControllerStatus
                    {
                        BrainId = brainId.ToProtoUuid(),
                        NodeId = workerId.ToProtoUuid(),
                        ActorName = controllerActor,
                        LastSeenMs = (ulong)serverSeenMs,
                        IsAlive = true
                    }
                }
            },
            SettingsResponse = new SettingListResponse()
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        var controllerRow = Assert.Single(vm.Actors, node => node.RootActor == controllerActor);
        Assert.Equal("online", controllerRow.Status);
        var workerEndpoint = Assert.Single(vm.WorkerEndpoints, endpoint => endpoint.NodeId == workerId);
        Assert.Equal(ShortBrainId(brainId), workerEndpoint.BrainHints);
    }

    [Fact]
    public async Task RefreshSettingsAsync_WorkerEndpoints_BrainHints_IgnoreOfflineControllers()
    {
        var connections = new ConnectionViewModel();
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var workerId = Guid.NewGuid();
        var brainId = Guid.NewGuid();
        var controllerActor = $"127.0.0.1:12041/worker-node/brain-{brainId:N}-root";
        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse
            {
                Nodes =
                {
                    new NodeStatus
                    {
                        NodeId = workerId.ToProtoUuid(),
                        LogicalName = connections.WorkerLogicalName,
                        Address = $"{connections.WorkerHost}:{connections.WorkerPortText}",
                        RootActorName = connections.WorkerRootName,
                        LastSeenMs = (ulong)nowMs,
                        IsAlive = true
                    }
                }
            },
            BrainsResponse = new BrainListResponse
            {
                Brains =
                {
                    new BrainStatus
                    {
                        BrainId = brainId.ToProtoUuid(),
                        SpawnedMs = (ulong)nowMs,
                        LastTickId = 12,
                        State = "Active"
                    }
                },
                Controllers =
                {
                    new BrainControllerStatus
                    {
                        BrainId = brainId.ToProtoUuid(),
                        NodeId = workerId.ToProtoUuid(),
                        ActorName = controllerActor,
                        LastSeenMs = (ulong)(nowMs - 120_000),
                        IsAlive = true
                    }
                }
            },
            SettingsResponse = new SettingListResponse()
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        var controllerRow = Assert.Single(vm.Actors, node => node.RootActor == controllerActor);
        Assert.Equal("offline", controllerRow.Status);
        var workerEndpoint = Assert.Single(vm.WorkerEndpoints, endpoint => endpoint.NodeId == workerId);
        Assert.Equal("none", workerEndpoint.BrainHints);
    }

    [Fact]
    public async Task RefreshSettingsAsync_IncludesPlacementHostedActorRows()
    {
        var connections = new ConnectionViewModel();
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var hiveNodeId = Guid.NewGuid();
        var workerNodeId = Guid.NewGuid();
        var brainId = Guid.NewGuid();
        const ulong placementEpoch = 14;
        var shardActorPid = $"127.0.0.1:12041/worker-node/brain-{brainId:N}-r9-s2";

        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse
            {
                Nodes =
                {
                    new NodeStatus
                    {
                        NodeId = hiveNodeId.ToProtoUuid(),
                        LogicalName = "hivemind",
                        Address = "127.0.0.1:12020",
                        RootActorName = "hive-mind",
                        LastSeenMs = (ulong)nowMs,
                        IsAlive = true
                    },
                    new NodeStatus
                    {
                        NodeId = workerNodeId.ToProtoUuid(),
                        LogicalName = connections.WorkerLogicalName,
                        Address = $"{connections.WorkerHost}:{connections.WorkerPortText}",
                        RootActorName = connections.WorkerRootName,
                        LastSeenMs = (ulong)nowMs,
                        IsAlive = true
                    }
                }
            },
            BrainsResponse = new BrainListResponse
            {
                Brains =
                {
                    new BrainStatus
                    {
                        BrainId = brainId.ToProtoUuid(),
                        SpawnedMs = (ulong)nowMs,
                        LastTickId = 22,
                        State = "Active"
                    }
                }
            },
            SettingsResponse = new SettingListResponse(),
            PlacementLifecycleFactory = requestedBrainId =>
                requestedBrainId == brainId
                    ? new PlacementLifecycleInfo
                    {
                        BrainId = requestedBrainId.ToProtoUuid(),
                        PlacementEpoch = placementEpoch,
                        LifecycleState = PlacementLifecycleState.PlacementLifecycleRunning
                    }
                    : null,
            PlacementReconcileFactory = (workerAddress, workerRoot, requestedBrainId, requestedEpoch) =>
            {
                if (requestedBrainId != brainId
                    || requestedEpoch != placementEpoch
                    || !string.Equals(workerRoot, connections.WorkerRootName, StringComparison.OrdinalIgnoreCase))
                {
                    return null;
                }

                return new PlacementReconcileReport
                {
                    BrainId = brainId.ToProtoUuid(),
                    PlacementEpoch = placementEpoch,
                    ReconcileState = PlacementReconcileState.PlacementReconcileMatched,
                    Assignments =
                    {
                        new PlacementObservedAssignment
                        {
                            AssignmentId = Guid.NewGuid().ToString("N"),
                            Target = PlacementAssignmentTarget.PlacementTargetRegionShard,
                            WorkerNodeId = workerNodeId.ToProtoUuid(),
                            RegionId = 9,
                            ShardIndex = 2,
                            ActorPid = shardActorPid
                        }
                    }
                };
            }
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        Assert.Equal(2, vm.Nodes.Count);
        var shardRow = Assert.Single(vm.Actors, row => row.RootActor == shardActorPid);
        Assert.Contains("RegionShard r9 s2", shardRow.LogicalName, StringComparison.Ordinal);
        Assert.Equal("online", shardRow.Status);
        var workerEndpoint = Assert.Single(vm.WorkerEndpoints, endpoint => endpoint.NodeId == workerNodeId);
        Assert.Equal(ShortBrainId(brainId), workerEndpoint.BrainHints);
    }

    [Fact]
    public async Task RefreshSettingsAsync_HostedActors_PrioritizesOnlineWorkerHosts()
    {
        var connections = new ConnectionViewModel();
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var hiveNodeId = Guid.NewGuid();
        var workerNodeId = Guid.NewGuid();
        var brainId = Guid.NewGuid();
        const ulong placementEpoch = 8;
        var controllerActorPid = "127.0.0.1:12020/hive-mind/brain-controller";
        var shardActorPid = $"127.0.0.1:12041/worker-node/brain-{brainId:N}-r9-s0";

        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse
            {
                Nodes =
                {
                    new NodeStatus
                    {
                        NodeId = hiveNodeId.ToProtoUuid(),
                        LogicalName = "hivemind",
                        Address = "127.0.0.1:12020",
                        RootActorName = "hive-mind",
                        LastSeenMs = (ulong)nowMs,
                        IsAlive = true
                    },
                    new NodeStatus
                    {
                        NodeId = workerNodeId.ToProtoUuid(),
                        LogicalName = connections.WorkerLogicalName,
                        Address = $"{connections.WorkerHost}:{connections.WorkerPortText}",
                        RootActorName = connections.WorkerRootName,
                        LastSeenMs = (ulong)(nowMs - 5_000),
                        IsAlive = true
                    }
                }
            },
            BrainsResponse = new BrainListResponse
            {
                Brains =
                {
                    new BrainStatus
                    {
                        BrainId = brainId.ToProtoUuid(),
                        SpawnedMs = (ulong)nowMs,
                        LastTickId = 9,
                        State = "Active"
                    }
                },
                Controllers =
                {
                    new BrainControllerStatus
                    {
                        BrainId = brainId.ToProtoUuid(),
                        NodeId = hiveNodeId.ToProtoUuid(),
                        ActorName = controllerActorPid,
                        LastSeenMs = (ulong)nowMs,
                        IsAlive = true
                    }
                }
            },
            SettingsResponse = new SettingListResponse(),
            PlacementLifecycleFactory = requestedBrainId =>
                requestedBrainId == brainId
                    ? new PlacementLifecycleInfo
                    {
                        BrainId = requestedBrainId.ToProtoUuid(),
                        PlacementEpoch = placementEpoch,
                        LifecycleState = PlacementLifecycleState.PlacementLifecycleRunning
                    }
                    : null,
            PlacementReconcileFactory = (workerAddress, workerRoot, requestedBrainId, requestedEpoch) =>
            {
                if (requestedBrainId != brainId
                    || requestedEpoch != placementEpoch
                    || !string.Equals(workerRoot, connections.WorkerRootName, StringComparison.OrdinalIgnoreCase))
                {
                    return null;
                }

                return new PlacementReconcileReport
                {
                    BrainId = brainId.ToProtoUuid(),
                    PlacementEpoch = placementEpoch,
                    ReconcileState = PlacementReconcileState.PlacementReconcileMatched,
                    Assignments =
                    {
                        new PlacementObservedAssignment
                        {
                            AssignmentId = Guid.NewGuid().ToString("N"),
                            Target = PlacementAssignmentTarget.PlacementTargetRegionShard,
                            WorkerNodeId = workerNodeId.ToProtoUuid(),
                            RegionId = 9,
                            ShardIndex = 0,
                            ActorPid = shardActorPid
                        }
                    }
                };
            }
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        var shardIndex = IndexOfActor(vm.Actors, shardActorPid);
        var controllerIndex = IndexOfActor(vm.Actors, controllerActorPid);
        Assert.True(shardIndex >= 0);
        Assert.True(controllerIndex >= 0);
        Assert.True(shardIndex < controllerIndex, "Online worker-hosted actor should sort ahead of non-worker actors.");
    }

    [Fact]
    public async Task RefreshSettingsAsync_WorkerEndpoints_BrainHints_TruncateAfterTwoIds()
    {
        var connections = new ConnectionViewModel();
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var workerNodeId = Guid.NewGuid();
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();
        var brainC = Guid.NewGuid();
        var brainD = Guid.NewGuid();
        var brainE = Guid.NewGuid();
        var brainF = Guid.NewGuid();
        const ulong placementEpoch = 3;

        var client = new FakeWorkbenchClient
        {
            NodesResponse = new NodeListResponse
            {
                Nodes =
                {
                    new NodeStatus
                    {
                        NodeId = workerNodeId.ToProtoUuid(),
                        LogicalName = connections.WorkerLogicalName,
                        Address = $"{connections.WorkerHost}:{connections.WorkerPortText}",
                        RootActorName = connections.WorkerRootName,
                        LastSeenMs = (ulong)nowMs,
                        IsAlive = true
                    }
                }
            },
            BrainsResponse = new BrainListResponse
            {
                Brains =
                {
                    new BrainStatus { BrainId = brainA.ToProtoUuid(), SpawnedMs = (ulong)nowMs, LastTickId = 1, State = "Active" },
                    new BrainStatus { BrainId = brainB.ToProtoUuid(), SpawnedMs = (ulong)nowMs, LastTickId = 2, State = "Active" },
                    new BrainStatus { BrainId = brainC.ToProtoUuid(), SpawnedMs = (ulong)nowMs, LastTickId = 3, State = "Active" },
                    new BrainStatus { BrainId = brainD.ToProtoUuid(), SpawnedMs = (ulong)nowMs, LastTickId = 4, State = "Active" },
                    new BrainStatus { BrainId = brainE.ToProtoUuid(), SpawnedMs = (ulong)nowMs, LastTickId = 5, State = "Active" },
                    new BrainStatus { BrainId = brainF.ToProtoUuid(), SpawnedMs = (ulong)nowMs, LastTickId = 6, State = "Active" }
                }
            },
            SettingsResponse = new SettingListResponse(),
            PlacementLifecycleFactory = requestedBrainId =>
                requestedBrainId == brainA
                || requestedBrainId == brainB
                || requestedBrainId == brainC
                || requestedBrainId == brainD
                || requestedBrainId == brainE
                || requestedBrainId == brainF
                    ? new PlacementLifecycleInfo
                    {
                        BrainId = requestedBrainId.ToProtoUuid(),
                        PlacementEpoch = placementEpoch,
                        LifecycleState = PlacementLifecycleState.PlacementLifecycleRunning
                    }
                    : null,
            PlacementReconcileFactory = (workerAddress, workerRoot, requestedBrainId, requestedEpoch) =>
            {
                if (requestedEpoch != placementEpoch
                    || (requestedBrainId != brainA
                        && requestedBrainId != brainB
                        && requestedBrainId != brainC
                        && requestedBrainId != brainD
                        && requestedBrainId != brainE
                        && requestedBrainId != brainF)
                    || !string.Equals(workerRoot, connections.WorkerRootName, StringComparison.OrdinalIgnoreCase))
                {
                    return null;
                }

                return new PlacementReconcileReport
                {
                    BrainId = requestedBrainId.ToProtoUuid(),
                    PlacementEpoch = placementEpoch,
                    ReconcileState = PlacementReconcileState.PlacementReconcileMatched,
                    Assignments =
                    {
                        new PlacementObservedAssignment
                        {
                            AssignmentId = Guid.NewGuid().ToString("N"),
                            Target = PlacementAssignmentTarget.PlacementTargetBrainRoot,
                            WorkerNodeId = workerNodeId.ToProtoUuid(),
                            ActorPid = $"127.0.0.1:12041/worker-node/brain-{requestedBrainId:N}-root"
                        }
                    }
                };
            }
        };

        var vm = CreateViewModel(connections, client);
        connections.SettingsConnected = true;

        await vm.RefreshSettingsAsync();

        var workerEndpoint = Assert.Single(vm.WorkerEndpoints, endpoint => endpoint.NodeId == workerNodeId);
        var abbreviated = new[]
            {
                ShortBrainId(brainA),
                ShortBrainId(brainB),
                ShortBrainId(brainC),
                ShortBrainId(brainD),
                ShortBrainId(brainE),
                ShortBrainId(brainF)
            }
            .OrderBy(static value => value, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal($"{abbreviated[0]}, {abbreviated[1]}, ...", workerEndpoint.BrainHints);
    }

    [Fact]
    public async Task SpawnSampleBrainCommand_UsesIoSpawnPath_OnSuccess()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true,
            HiveMindConnected = true,
            IoConnected = true
        };
        var spawnedBrainId = Guid.NewGuid();
        var registrationPolls = 0;
        var lifecyclePolls = 0;
        var artifactPublisher = new FakeWorkbenchArtifactPublisher();
        var client = new FakeWorkbenchClient
        {
            SpawnBrainAck = new SpawnBrainAck { BrainId = spawnedBrainId.ToProtoUuid() },
            BrainListFactory = () =>
            {
                registrationPolls++;
                return registrationPolls < 2
                    ? BuildBrainList(spawnedBrainId, "Active", includeAliveController: false)
                    : BuildBrainList(spawnedBrainId, "Active");
            },
            PlacementLifecycleFactory = requestedBrainId =>
            {
                if (requestedBrainId != spawnedBrainId)
                {
                    return null;
                }

                lifecyclePolls++;
                return lifecyclePolls < 2
                    ? BuildPlacementLifecycle(requestedBrainId, PlacementLifecycleState.PlacementLifecycleAssigning, registeredShards: 0)
                    : BuildPlacementLifecycle(requestedBrainId, PlacementLifecycleState.PlacementLifecycleRunning, registeredShards: 4);
            }
        };
        var vm = CreateViewModel(connections, client, artifactPublisher: artifactPublisher);

        vm.SpawnSampleBrainCommand.Execute(null);
        await WaitForAsync(() => vm.SampleBrainStatus.Contains("Sample brain running", StringComparison.Ordinal));

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.RequestPlacementCallCount);
        Assert.True(client.ListBrainsCallCount >= 2);
        Assert.True(client.GetPlacementLifecycleCallCount >= 2);
        Assert.NotNull(client.LastSpawnRequest);
        Assert.Equal("application/x-nbn", client.LastSpawnRequest!.BrainDef?.MediaType);
        Assert.Equal(artifactPublisher.BaseUri, client.LastSpawnRequest.BrainDef?.StoreUri);
        Assert.Equal(1, artifactPublisher.PublishCallCount);
        Assert.Equal(0, client.KillBrainCallCount);
        Assert.Contains("Spawned via IO; worker placement managed by HiveMind.", vm.SampleBrainStatus, StringComparison.Ordinal);

        vm.StopSampleBrainCommand.Execute(null);
        await WaitForAsync(() => client.KillBrainCallCount == 1);

        Assert.Equal(spawnedBrainId, client.LastKillBrainId);
        Assert.Contains("stop requested", vm.SampleBrainStatus, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task SpawnSampleBrainCommand_AllowsDiscoveryReady_WhenConnectionFlagsAreFalse()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true,
            HiveMindConnected = false,
            IoConnected = false,
            HiveMindDiscoverable = true,
            IoDiscoverable = true
        };
        var spawnedBrainId = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            SpawnBrainAck = new SpawnBrainAck { BrainId = spawnedBrainId.ToProtoUuid() },
            BrainListFactory = () => BuildBrainList(spawnedBrainId, "Active"),
            PlacementLifecycleFactory = requestedBrainId => requestedBrainId == spawnedBrainId
                ? BuildPlacementLifecycle(requestedBrainId, PlacementLifecycleState.PlacementLifecycleRunning, registeredShards: 2)
                : null
        };
        var vm = CreateViewModel(connections, client);

        vm.SpawnSampleBrainCommand.Execute(null);
        await WaitForAsync(() => vm.SampleBrainStatus.Contains("Sample brain running", StringComparison.Ordinal));

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.KillBrainCallCount);
        Assert.Contains("Spawned via IO; worker placement managed by HiveMind.", vm.SampleBrainStatus, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SpawnSampleBrainCommand_AllowsPositiveStatuses_WhenConnectionFlagsAndDiscoveryLag()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = false,
            HiveMindConnected = false,
            IoConnected = false,
            SettingsStatus = "Ready",
            HiveMindStatus = "Online",
            IoStatus = "Connected"
        };
        var spawnedBrainId = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            SpawnBrainAck = new SpawnBrainAck { BrainId = spawnedBrainId.ToProtoUuid() },
            BrainListFactory = () => BuildBrainList(spawnedBrainId, "Active"),
            PlacementLifecycleFactory = requestedBrainId => requestedBrainId == spawnedBrainId
                ? BuildPlacementLifecycle(requestedBrainId, PlacementLifecycleState.PlacementLifecycleRunning, registeredShards: 2)
                : null
        };
        var vm = CreateViewModel(connections, client);

        vm.SpawnSampleBrainCommand.Execute(null);
        await WaitForAsync(() => vm.SampleBrainStatus.Contains("Sample brain running", StringComparison.Ordinal));

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.KillBrainCallCount);
        Assert.Contains("Spawned via IO; worker placement managed by HiveMind.", vm.SampleBrainStatus, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SpawnSampleBrainCommand_DoesNotBlock_WhenPlacementInventorySnapshotIsEmpty()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true,
            HiveMindConnected = true,
            IoConnected = true
        };
        var spawnedBrainId = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            PlacementWorkerInventoryResponse = new PlacementWorkerInventory(),
            SpawnBrainAck = new SpawnBrainAck { BrainId = spawnedBrainId.ToProtoUuid() },
            BrainListFactory = () => BuildBrainList(spawnedBrainId, "Active"),
            PlacementLifecycleFactory = requestedBrainId => requestedBrainId == spawnedBrainId
                ? BuildPlacementLifecycle(requestedBrainId, PlacementLifecycleState.PlacementLifecycleRunning, registeredShards: 2)
                : null
        };
        var vm = CreateViewModel(connections, client);

        vm.SpawnSampleBrainCommand.Execute(null);
        await WaitForAsync(() => vm.SampleBrainStatus.Contains("Sample brain running", StringComparison.Ordinal));

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Contains("Spawned via IO; worker placement managed by HiveMind.", vm.SampleBrainStatus, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SpawnSampleBrainCommand_UsesIoSpawnPath_WithInvalidLocalEndpointText()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true,
            HiveMindConnected = true,
            IoConnected = true,
            SettingsPortText = "bad",
            HiveMindPortText = "bad",
            IoPortText = "bad"
        };
        var spawnedBrainId = Guid.NewGuid();
        var lifecyclePolls = 0;
        var client = new FakeWorkbenchClient
        {
            SpawnBrainAck = new SpawnBrainAck { BrainId = spawnedBrainId.ToProtoUuid() },
            BrainListFactory = () => BuildBrainList(spawnedBrainId, "Active"),
            PlacementLifecycleFactory = requestedBrainId =>
            {
                if (requestedBrainId != spawnedBrainId)
                {
                    return null;
                }

                lifecyclePolls++;
                return lifecyclePolls < 2
                    ? BuildPlacementLifecycle(requestedBrainId, PlacementLifecycleState.PlacementLifecycleAssigned, registeredShards: 0)
                    : BuildPlacementLifecycle(requestedBrainId, PlacementLifecycleState.PlacementLifecycleRunning, registeredShards: 2);
            }
        };
        var vm = CreateViewModel(connections, client);

        vm.SpawnSampleBrainCommand.Execute(null);
        await WaitForAsync(() => vm.SampleBrainStatus.Contains("Sample brain running", StringComparison.Ordinal));

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.RequestPlacementCallCount);
        Assert.True(client.GetPlacementLifecycleCallCount >= 2);
        Assert.Equal(0, client.KillBrainCallCount);
        Assert.NotNull(client.LastSpawnRequest);
        Assert.Contains("Spawned via IO; worker placement managed by HiveMind.", vm.SampleBrainStatus, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SpawnSampleBrainCommand_Fails_WhenIoSpawnReturnsEmptyId()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true,
            HiveMindConnected = true,
            IoConnected = true
        };
        var client = new FakeWorkbenchClient
        {
            SpawnBrainAck = new SpawnBrainAck { BrainId = Guid.Empty.ToProtoUuid() }
        };
        var vm = CreateViewModel(connections, client);

        vm.SpawnSampleBrainCommand.Execute(null);
        await WaitForAsync(() => vm.SampleBrainStatus.Contains("IO did not return a brain id", StringComparison.Ordinal));

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.KillBrainCallCount);
    }

    [Fact]
    public async Task SpawnSampleBrainCommand_Fails_With_ActionableIoSpawnFailureDetails()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true,
            HiveMindConnected = true,
            IoConnected = true
        };
        var client = new FakeWorkbenchClient
        {
            SpawnBrainAck = new SpawnBrainAck
            {
                BrainId = Guid.Empty.ToProtoUuid(),
                FailureReasonCode = "spawn_worker_unavailable",
                FailureMessage = "Spawn failed: no eligible worker was available for the placement plan."
            }
        };
        var vm = CreateViewModel(connections, client);

        vm.SpawnSampleBrainCommand.Execute(null);
        await WaitForAsync(() => vm.SampleBrainStatus.Contains("spawn_worker_unavailable", StringComparison.Ordinal));

        Assert.Contains("no eligible worker", vm.SampleBrainStatus, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.KillBrainCallCount);
    }

    [Fact]
    public async Task SpawnSampleBrainCommand_TimesOut_WhenBrainDoesNotRegister()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true,
            HiveMindConnected = true,
            IoConnected = true
        };
        var spawnedBrainId = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            SpawnBrainAck = new SpawnBrainAck
            {
                BrainId = spawnedBrainId.ToProtoUuid()
            },
            BrainListFactory = static () => new BrainListResponse()
        };
        var vm = CreateViewModel(connections, client);

        vm.SpawnSampleBrainCommand.Execute(null);
        await WaitForAsync(
            () => vm.SampleBrainStatus.Contains("failed to become visualization-ready", StringComparison.OrdinalIgnoreCase),
            timeoutMs: 15_000);

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(1, client.KillBrainCallCount);
        Assert.Equal(spawnedBrainId, client.LastKillBrainId);
        Assert.Equal("workbench_sample_registration_timeout", client.LastKillReason);
        Assert.Contains("after IO/HiveMind worker placement.", vm.SampleBrainStatus, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DesignerSpawn_UsesWorkerFirstIoPath_WithoutLocalHostConfiguration()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true,
            HiveMindConnected = true,
            IoConnected = true,
            SettingsPortText = "bad",
            HiveMindPortText = "bad",
            IoPortText = "bad"
        };
        var spawnedBrainId = Guid.NewGuid();
        var registrationPolls = 0;
        var lifecyclePolls = 0;
        var client = new FakeWorkbenchClient
        {
            SpawnBrainAck = new SpawnBrainAck { BrainId = spawnedBrainId.ToProtoUuid() },
            BrainListFactory = () =>
            {
                registrationPolls++;
                return registrationPolls < 2
                    ? BuildBrainList(spawnedBrainId, "Active", includeAliveController: false)
                    : BuildBrainList(spawnedBrainId, "Active");
            },
            PlacementLifecycleFactory = requestedBrainId =>
            {
                if (requestedBrainId != spawnedBrainId)
                {
                    return null;
                }

                lifecyclePolls++;
                return lifecyclePolls < 2
                    ? BuildPlacementLifecycle(requestedBrainId, PlacementLifecycleState.PlacementLifecycleReconciling, registeredShards: 0)
                    : BuildPlacementLifecycle(requestedBrainId, PlacementLifecycleState.PlacementLifecycleRunning, registeredShards: 8);
            }
        };
        var artifactPublisher = new FakeWorkbenchArtifactPublisher();
        var vm = CreateDesignerViewModel(connections, client, artifactPublisher);
        vm.NewBrainCommand.Execute(null);
        vm.SpawnArtifactRoot = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"));

        vm.SpawnBrainCommand.Execute(null);
        await WaitForAsync(() => vm.Status.Contains("Brain spawned", StringComparison.Ordinal), timeoutMs: 5000);

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.RequestPlacementCallCount);
        Assert.True(client.ListBrainsCallCount >= 2);
        Assert.True(client.GetPlacementLifecycleCallCount >= 2);
        Assert.Equal(0, client.KillBrainCallCount);
        Assert.NotNull(client.LastSpawnRequest);
        Assert.Equal(artifactPublisher.BaseUri, client.LastSpawnRequest!.BrainDef?.StoreUri);
        Assert.Equal(1, artifactPublisher.PublishCallCount);
        Assert.Contains(spawnedBrainId.ToString("D"), vm.Status, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Spawned via IO; worker placement managed by HiveMind.", vm.Status, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DesignerSpawn_AllowsDiscoveryReady_WhenConnectionFlagsAreFalse()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true,
            HiveMindConnected = false,
            IoConnected = false,
            HiveMindDiscoverable = true,
            IoDiscoverable = true,
            SettingsPortText = "bad",
            HiveMindPortText = "bad",
            IoPortText = "bad"
        };
        var spawnedBrainId = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            SpawnBrainAck = new SpawnBrainAck { BrainId = spawnedBrainId.ToProtoUuid() },
            BrainListFactory = () => BuildBrainList(spawnedBrainId, "Active"),
            PlacementLifecycleFactory = requestedBrainId => requestedBrainId == spawnedBrainId
                ? BuildPlacementLifecycle(requestedBrainId, PlacementLifecycleState.PlacementLifecycleRunning, registeredShards: 3)
                : null
        };
        var vm = CreateDesignerViewModel(connections, client);
        vm.NewBrainCommand.Execute(null);
        vm.SpawnArtifactRoot = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"));

        vm.SpawnBrainCommand.Execute(null);
        await WaitForAsync(() => vm.Status.Contains("Brain spawned", StringComparison.Ordinal), timeoutMs: 5000);

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.RequestPlacementCallCount);
        Assert.Equal(0, client.KillBrainCallCount);
        Assert.Contains(spawnedBrainId.ToString("D"), vm.Status, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DesignerSpawn_AllowsPositiveStatuses_WhenConnectionFlagsAndDiscoveryLag()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = false,
            HiveMindConnected = false,
            IoConnected = false,
            HiveMindDiscoverable = false,
            IoDiscoverable = false,
            SettingsStatus = "Ready",
            HiveMindStatus = "Online",
            IoStatus = "Connected",
            SettingsPortText = "bad",
            HiveMindPortText = "bad",
            IoPortText = "bad"
        };
        var spawnedBrainId = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            SpawnBrainAck = new SpawnBrainAck { BrainId = spawnedBrainId.ToProtoUuid() },
            BrainListFactory = () => BuildBrainList(spawnedBrainId, "Active"),
            PlacementLifecycleFactory = requestedBrainId => requestedBrainId == spawnedBrainId
                ? BuildPlacementLifecycle(requestedBrainId, PlacementLifecycleState.PlacementLifecycleRunning, registeredShards: 3)
                : null
        };
        var vm = CreateDesignerViewModel(connections, client);
        vm.NewBrainCommand.Execute(null);
        vm.SpawnArtifactRoot = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"));

        vm.SpawnBrainCommand.Execute(null);
        await WaitForAsync(() => vm.Status.Contains("Brain spawned", StringComparison.Ordinal), timeoutMs: 5000);

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.RequestPlacementCallCount);
        Assert.Equal(0, client.KillBrainCallCount);
        Assert.Contains(spawnedBrainId.ToString("D"), vm.Status, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DesignerSpawn_DoesNotBlock_WhenPlacementInventorySnapshotIsEmpty()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true,
            HiveMindConnected = true,
            IoConnected = true,
            SettingsPortText = "bad",
            HiveMindPortText = "bad",
            IoPortText = "bad"
        };
        var spawnedBrainId = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            PlacementWorkerInventoryResponse = new PlacementWorkerInventory(),
            SpawnBrainAck = new SpawnBrainAck { BrainId = spawnedBrainId.ToProtoUuid() },
            BrainListFactory = () => BuildBrainList(spawnedBrainId, "Active"),
            PlacementLifecycleFactory = requestedBrainId => requestedBrainId == spawnedBrainId
                ? BuildPlacementLifecycle(requestedBrainId, PlacementLifecycleState.PlacementLifecycleRunning, registeredShards: 3)
                : null
        };
        var vm = CreateDesignerViewModel(connections, client);
        vm.NewBrainCommand.Execute(null);
        vm.SpawnArtifactRoot = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"));

        vm.SpawnBrainCommand.Execute(null);
        await WaitForAsync(() => vm.Status.Contains("Brain spawned", StringComparison.Ordinal), timeoutMs: 5000);

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.RequestPlacementCallCount);
        Assert.Equal(0, client.KillBrainCallCount);
        Assert.Contains(spawnedBrainId.ToString("D"), vm.Status, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DesignerSpawn_Shows_Actionable_IoSpawnFailureDetails()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true,
            HiveMindConnected = true,
            IoConnected = true,
            SettingsPortText = "bad",
            HiveMindPortText = "bad",
            IoPortText = "bad"
        };
        var client = new FakeWorkbenchClient
        {
            SpawnBrainAck = new SpawnBrainAck
            {
                BrainId = Guid.Empty.ToProtoUuid(),
                FailureReasonCode = "spawn_assignment_timeout",
                FailureMessage = "Spawn failed: placement assignment acknowledgements timed out and retry budget was exhausted."
            }
        };
        var vm = CreateDesignerViewModel(connections, client);
        vm.NewBrainCommand.Execute(null);
        vm.SpawnArtifactRoot = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"));

        vm.SpawnBrainCommand.Execute(null);
        await WaitForAsync(() => vm.Status.Contains("spawn_assignment_timeout", StringComparison.Ordinal), timeoutMs: 5000);

        Assert.Contains("timed out", vm.Status, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.RequestPlacementCallCount);
        Assert.Equal(0, client.KillBrainCallCount);
    }

    [Fact]
    public async Task DesignerSpawn_Fails_WithWorkerPlacementRegistrationTimeoutCopy()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true,
            HiveMindConnected = true,
            IoConnected = true,
            SettingsPortText = "bad",
            HiveMindPortText = "bad",
            IoPortText = "bad"
        };
        var spawnedBrainId = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            SpawnBrainAck = new SpawnBrainAck { BrainId = spawnedBrainId.ToProtoUuid() },
            BrainListFactory = static () => new BrainListResponse()
        };
        var vm = CreateDesignerViewModel(connections, client);
        vm.NewBrainCommand.Execute(null);
        vm.SpawnArtifactRoot = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"));

        vm.SpawnBrainCommand.Execute(null);
        await WaitForAsync(
            () => vm.Status.Contains("did not become visualization-ready after IO/HiveMind worker placement", StringComparison.Ordinal),
            timeoutMs: 15_000);

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.RequestPlacementCallCount);
        Assert.Equal(1, client.KillBrainCallCount);
        Assert.Equal(spawnedBrainId, client.LastKillBrainId);
        Assert.Equal("designer_managed_spawn_registration_timeout", client.LastKillReason);
    }

    private static OrchestratorPanelViewModel CreateViewModel(
        ConnectionViewModel connections,
        WorkbenchClient client,
        ILocalProjectLaunchPreparer? launchPreparer = null,
        ILocalFirewallManager? firewallManager = null,
        IWorkbenchArtifactPublisher? artifactPublisher = null)
    {
        return new OrchestratorPanelViewModel(
            new UiDispatcher(),
            connections,
            client,
            connectAll: () => Task.CompletedTask,
            disconnectAll: () => { },
            launchPreparer: launchPreparer,
            firewallManager: firewallManager ?? new FakeLocalFirewallManager(),
            artifactPublisher: artifactPublisher ?? new FakeWorkbenchArtifactPublisher());
    }

    private static DesignerPanelViewModel CreateDesignerViewModel(
        ConnectionViewModel connections,
        WorkbenchClient client,
        IWorkbenchArtifactPublisher? artifactPublisher = null)
        => new(connections, client, artifactPublisher: artifactPublisher ?? new FakeWorkbenchArtifactPublisher());

    private static async Task WaitForAsync(Func<bool> predicate, int timeoutMs = 2000)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.ElapsedMilliseconds < timeoutMs)
        {
            if (predicate())
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.True(predicate());
    }

    private static int IndexOfActor(IReadOnlyList<NodeStatusItem> rows, string rootActor)
    {
        for (var i = 0; i < rows.Count; i++)
        {
            if (string.Equals(rows[i].RootActor, rootActor, StringComparison.Ordinal))
            {
                return i;
            }
        }

        return -1;
    }

    private static string ShortBrainId(Guid brainId)
    {
        var compact = brainId.ToString("N");
        return compact.Length <= 4 ? compact : compact[^4..];
    }

    private static string HostedActorBrainToken(Guid brainId)
    {
        var compact = brainId.ToString("N");
        return compact.Length <= 8 ? compact : compact[^8..];
    }

    private static BrainListResponse BuildBrainList(Guid brainId, string state, bool includeAliveController = true)
    {
        var nowMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var response = new BrainListResponse
        {
            Brains =
            {
                new BrainStatus
                {
                    BrainId = brainId.ToProtoUuid(),
                    SpawnedMs = nowMs,
                    LastTickId = 0,
                    State = state
                }
            }
        };

        if (includeAliveController)
        {
            response.Controllers.Add(new BrainControllerStatus
            {
                BrainId = brainId.ToProtoUuid(),
                NodeId = Guid.NewGuid().ToProtoUuid(),
                ActorName = "brain-controller",
                LastSeenMs = nowMs,
                IsAlive = true
            });
        }

        return response;
    }

    private static PlacementLifecycleInfo BuildPlacementLifecycle(
        Guid brainId,
        PlacementLifecycleState state,
        uint registeredShards)
    {
        return new PlacementLifecycleInfo
        {
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = 1,
            LifecycleState = state,
            RegisteredShards = registeredShards
        };
    }

    private sealed class FakeWorkbenchClient : WorkbenchClient
    {
        public NodeListResponse? NodesResponse { get; set; }
        public WorkerInventorySnapshotResponse? WorkerInventoryResponse { get; set; }
        public BrainListResponse? BrainsResponse { get; set; }
        public Func<BrainListResponse?>? BrainListFactory { get; set; }
        public SettingListResponse? SettingsResponse { get; init; }
        public SettingListResponse? RemoteSettingsResponse { get; set; }
        public Func<string, int, string, SettingListResponse?>? RemoteSettingsFactory { get; set; }
        public TcpEndpointProbeResult ProbeResult { get; set; } = new(false, "TCP connect timed out.");
        public List<(string Key, string Value)> SettingCalls { get; } = new();
        public List<(string Host, int Port, string ActorName)> RemoteSettingsCalls { get; } = new();
        public SpawnBrainAck? SpawnBrainAck { get; set; }
        public PlacementAck? PlacementAck { get; set; }
        public PlacementWorkerInventory? PlacementWorkerInventoryResponse { get; set; }
        public Func<Guid, PlacementLifecycleInfo?>? PlacementLifecycleFactory { get; init; }
        public Func<string, string, Guid, ulong, PlacementReconcileReport?>? PlacementReconcileFactory { get; init; }
        public bool KillBrainResult { get; set; } = true;
        public int SpawnViaIoCallCount { get; private set; }
        public int RequestPlacementCallCount { get; private set; }
        public int ListBrainsCallCount { get; private set; }
        public int GetPlacementLifecycleCallCount { get; private set; }
        public int RequestPlacementReconcileCallCount { get; private set; }
        public int KillBrainCallCount { get; private set; }
        public SpawnBrain? LastSpawnRequest { get; private set; }
        public RequestPlacement? LastPlacementRequest { get; private set; }
        public Guid? LastKillBrainId { get; private set; }
        public string? LastKillReason { get; private set; }

        public FakeWorkbenchClient()
            : base(new NullWorkbenchEventSink())
        {
        }

        public override Task<NodeListResponse?> ListNodesAsync()
            => Task.FromResult(NodesResponse);

        public override Task<WorkerInventorySnapshotResponse?> ListWorkerInventorySnapshotAsync()
            => Task.FromResult(WorkerInventoryResponse);

        public override Task<BrainListResponse?> ListBrainsAsync()
        {
            ListBrainsCallCount++;
            return Task.FromResult(BrainListFactory?.Invoke() ?? BrainsResponse);
        }

        public override Task<SettingListResponse?> ListSettingsAsync()
            => Task.FromResult(SettingsResponse);

        public override Task<SettingListResponse?> ListSettingsAsync(string host, int port, string actorName)
        {
            RemoteSettingsCalls.Add((host, port, actorName));
            return Task.FromResult(RemoteSettingsFactory?.Invoke(host, port, actorName) ?? RemoteSettingsResponse);
        }

        public override Task<TcpEndpointProbeResult> ProbeTcpEndpointAsync(string host, int port, TimeSpan? timeout = null)
            => Task.FromResult(ProbeResult);

        public override Task<PlacementWorkerInventory?> GetPlacementWorkerInventoryAsync()
            => Task.FromResult<PlacementWorkerInventory?>(PlacementWorkerInventoryResponse ?? new PlacementWorkerInventory
            {
                Workers =
                {
                    new PlacementWorkerInventoryEntry
                    {
                        WorkerNodeId = Guid.NewGuid().ToProtoUuid(),
                        WorkerAddress = "worker:12040",
                        WorkerRootActorName = "worker-node",
                        CpuCores = 8,
                        RamFreeBytes = 8UL * 1024 * 1024 * 1024,
                        RamTotalBytes = 16UL * 1024 * 1024 * 1024,
                        StorageFreeBytes = 64UL * 1024 * 1024 * 1024,
                        StorageTotalBytes = 128UL * 1024 * 1024 * 1024,
                        CpuScore = 40f
                    }
                }
            });
        public override Task<SettingValue?> SetSettingAsync(string key, string value)
        {
            SettingCalls.Add((key, value));
            return Task.FromResult<SettingValue?>(new SettingValue
            {
                Key = key,
                Value = value,
                UpdatedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            });
        }

        public override Task<PlacementLifecycleInfo?> GetPlacementLifecycleAsync(Guid brainId)
        {
            GetPlacementLifecycleCallCount++;
            return Task.FromResult(PlacementLifecycleFactory?.Invoke(brainId));
        }

        public override Task<PlacementReconcileReport?> RequestPlacementReconcileAsync(
            string workerAddress,
            string workerRootActor,
            Guid brainId,
            ulong placementEpoch)
        {
            RequestPlacementReconcileCallCount++;
            return Task.FromResult(PlacementReconcileFactory?.Invoke(workerAddress, workerRootActor, brainId, placementEpoch));
        }

        public override Task<SpawnBrainAck?> SpawnBrainViaIoAsync(SpawnBrain request)
        {
            SpawnViaIoCallCount++;
            LastSpawnRequest = request;
            return Task.FromResult(SpawnBrainAck);
        }

        public override Task<PlacementAck?> RequestPlacementAsync(RequestPlacement request)
        {
            RequestPlacementCallCount++;
            LastPlacementRequest = request;
            return Task.FromResult(PlacementAck);
        }

        public override Task<bool> KillBrainAsync(Guid brainId, string reason)
        {
            KillBrainCallCount++;
            LastKillBrainId = brainId;
            LastKillReason = reason;
            return Task.FromResult(KillBrainResult);
        }
    }

    private sealed class FakeWorkbenchArtifactPublisher : IWorkbenchArtifactPublisher
    {
        public string BaseUri { get; set; } = "http://127.0.0.1:19091/";
        public string? AttentionMessage { get; set; }
        public int PublishCallCount { get; private set; }
        public string? LastMediaType { get; private set; }
        public string? LastBackingStoreRoot { get; private set; }
        public string? LastBindHost { get; private set; }

        public Task<PublishedArtifact> PublishAsync(
            byte[] bytes,
            string mediaType,
            string backingStoreRoot,
            string bindHost,
            string? advertisedHost = null,
            string? label = null,
            CancellationToken cancellationToken = default)
        {
            PublishCallCount++;
            LastMediaType = mediaType;
            LastBackingStoreRoot = backingStoreRoot;
            LastBindHost = bindHost;
            var artifactRef = SHA256.HashData(bytes)
                .ToArtifactRef((ulong)Math.Max(0, bytes.Length), mediaType, BaseUri);
            return Task.FromResult(new PublishedArtifact(artifactRef, AttentionMessage));
        }

        public Task<PublishedArtifact> PromoteAsync(
            ArtifactRef artifactRef,
            string defaultLocalStoreRootPath,
            string bindHost,
            string? advertisedHost = null,
            string? label = null,
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult(new PublishedArtifact(
                string.IsNullOrWhiteSpace(artifactRef.StoreUri)
                    ? artifactRef.ToSha256Hex().ToArtifactRef(artifactRef.SizeBytes, artifactRef.MediaType, BaseUri)
                    : artifactRef.Clone(),
                AttentionMessage));
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class NullWorkbenchEventSink : IWorkbenchEventSink
    {
        public void OnOutputEvent(OutputEventItem item) { }
        public void OnOutputVectorEvent(OutputVectorEventItem item) { }
        public void OnDebugEvent(DebugEventItem item) { }
        public void OnVizEvent(VizEventItem item) { }
        public void OnBrainTerminated(BrainTerminatedItem item) { }
        public void OnIoStatus(string status, bool connected) { }
        public void OnObsStatus(string status, bool connected) { }
        public void OnSettingsStatus(string status, bool connected) { }
        public void OnHiveMindStatus(string status, bool connected) { }
        public void OnSettingChanged(SettingItem item) { }
    }

    private sealed class FakeLocalProjectLaunchPreparer(string failureMessage) : ILocalProjectLaunchPreparer
    {
        public Task<LocalProjectLaunchPreparation> PrepareAsync(string? projectPath, string exeName, string runtimeArgs, string label)
        {
            return Task.FromResult(new LocalProjectLaunchPreparation(false, null, failureMessage));
        }
    }

    private sealed class RecordingLocalProjectLaunchPreparer(string failureMessage = "unexpected") : ILocalProjectLaunchPreparer
    {
        public int CallCount { get; private set; }
        public string LastRuntimeArgs { get; private set; } = string.Empty;

        public Task<LocalProjectLaunchPreparation> PrepareAsync(string? projectPath, string exeName, string runtimeArgs, string label)
        {
            CallCount++;
            LastRuntimeArgs = runtimeArgs;
            return Task.FromResult(new LocalProjectLaunchPreparation(false, null, failureMessage));
        }
    }

    private sealed class FakeLocalFirewallManager(FirewallAccessResult? result = null) : ILocalFirewallManager
    {
        public FirewallAccessResult Result { get; set; } = result ?? new(FirewallAccessStatus.Unsupported, "No supported active Linux firewall manager was detected.");

        public Task<FirewallAccessResult> EnsureInboundTcpAccessAsync(string label, string bindHost, int port)
            => Task.FromResult(Result);
    }

    private sealed class EnvironmentVariableScope : IDisposable
    {
        private readonly Dictionary<string, string?> _originals = new(StringComparer.Ordinal);

        public EnvironmentVariableScope(params (string Key, string? Value)[] values)
        {
            foreach (var (key, value) in values)
            {
                _originals[key] = Environment.GetEnvironmentVariable(key);
                Environment.SetEnvironmentVariable(key, value);
            }
        }

        public void Dispose()
        {
            foreach (var (key, value) in _originals)
            {
                Environment.SetEnvironmentVariable(key, value);
            }
        }
    }
}
