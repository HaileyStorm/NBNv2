using System.Diagnostics;
using System.IO;
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

        vm.StartAllCommand.Execute(null);
        await WaitForAsync(() => string.Equals(vm.WorkerLaunchStatus, "Invalid worker port.", StringComparison.Ordinal));

        Assert.Equal("Invalid worker port.", vm.WorkerLaunchStatus);
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

        Assert.True(connections.WorkerConnected);
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

        Assert.True(connections.WorkerConnected);
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

        Assert.False(connections.WorkerConnected);
        Assert.Equal("1 degraded worker, 1 failed worker", connections.WorkerStatus);
        Assert.Equal(2, vm.WorkerEndpoints.Count);
        Assert.Contains(vm.WorkerEndpoints, endpoint => endpoint.NodeId == degradedWorker && endpoint.Status == "degraded");
        Assert.Contains(vm.WorkerEndpoints, endpoint => endpoint.NodeId == failedWorker && endpoint.Status == "failed");
        Assert.All(vm.WorkerEndpoints, endpoint => Assert.Equal("none", endpoint.BrainHints));
        Assert.Equal("1 degraded worker, 1 failed worker", vm.WorkerEndpointSummary);
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

        Assert.False(connections.WorkerConnected);
        Assert.Equal("Offline", connections.WorkerStatus);
        Assert.Empty(vm.WorkerEndpoints);
        Assert.Equal("No active workers.", vm.WorkerEndpointSummary);
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

        Assert.True(connections.ObsConnected);
        Assert.Equal("Connected", connections.ObsStatus);
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

        Assert.False(connections.ObsConnected);
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
                    new BrainStatus { BrainId = brainC.ToProtoUuid(), SpawnedMs = (ulong)nowMs, LastTickId = 3, State = "Active" }
                }
            },
            SettingsResponse = new SettingListResponse(),
            PlacementLifecycleFactory = requestedBrainId =>
                requestedBrainId == brainA || requestedBrainId == brainB || requestedBrainId == brainC
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
                    || (requestedBrainId != brainA && requestedBrainId != brainB && requestedBrainId != brainC)
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
        var abbreviated = new[] { ShortBrainId(brainA), ShortBrainId(brainB), ShortBrainId(brainC) }
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
        var vm = CreateViewModel(connections, client);

        vm.SpawnSampleBrainCommand.Execute(null);
        await WaitForAsync(() => vm.SampleBrainStatus.Contains("Sample brain running", StringComparison.Ordinal));

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.RequestPlacementCallCount);
        Assert.True(client.ListBrainsCallCount >= 2);
        Assert.True(client.GetPlacementLifecycleCallCount >= 2);
        Assert.NotNull(client.LastSpawnRequest);
        Assert.Equal("application/x-nbn", client.LastSpawnRequest!.BrainDef?.MediaType);
        Assert.Equal(0, client.KillBrainCallCount);
        Assert.Contains("Spawned via IO; worker placement managed by HiveMind.", vm.SampleBrainStatus, StringComparison.Ordinal);

        vm.StopSampleBrainCommand.Execute(null);
        await WaitForAsync(() => client.KillBrainCallCount == 1);

        Assert.Equal(spawnedBrainId, client.LastKillBrainId);
        Assert.Contains("stop requested", vm.SampleBrainStatus, StringComparison.OrdinalIgnoreCase);
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
        var vm = new DesignerPanelViewModel(connections, client);
        vm.NewBrainCommand.Execute(null);
        vm.SpawnArtifactRoot = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"));

        vm.SpawnBrainCommand.Execute(null);
        await WaitForAsync(() => vm.Status.Contains("Brain spawned", StringComparison.Ordinal), timeoutMs: 5000);

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.RequestPlacementCallCount);
        Assert.True(client.ListBrainsCallCount >= 2);
        Assert.True(client.GetPlacementLifecycleCallCount >= 2);
        Assert.Equal(0, client.KillBrainCallCount);
        Assert.Contains(spawnedBrainId.ToString("D"), vm.Status, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Spawned via IO; worker placement managed by HiveMind.", vm.Status, StringComparison.Ordinal);
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
        var vm = new DesignerPanelViewModel(connections, client);
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
        var vm = new DesignerPanelViewModel(connections, client);
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

    private static OrchestratorPanelViewModel CreateViewModel(ConnectionViewModel connections, WorkbenchClient client)
    {
        return new OrchestratorPanelViewModel(
            new UiDispatcher(),
            connections,
            client,
            connectAll: () => Task.CompletedTask,
            disconnectAll: () => { });
    }

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
        public NodeListResponse? NodesResponse { get; init; }
        public WorkerInventorySnapshotResponse? WorkerInventoryResponse { get; init; }
        public BrainListResponse? BrainsResponse { get; set; }
        public Func<BrainListResponse?>? BrainListFactory { get; set; }
        public SettingListResponse? SettingsResponse { get; init; }
        public SpawnBrainAck? SpawnBrainAck { get; set; }
        public PlacementAck? PlacementAck { get; set; }
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
}
