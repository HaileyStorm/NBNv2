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
        Assert.Equal("Connected", connections.WorkerStatus);
        var workerNode = Assert.Single(vm.Nodes);
        Assert.Equal(connections.WorkerLogicalName, workerNode.LogicalName);
        Assert.Equal(connections.WorkerRootName, workerNode.RootActor);
        Assert.Equal("online", workerNode.Status);
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
        var client = new FakeWorkbenchClient
        {
            SpawnBrainAck = new SpawnBrainAck { BrainId = spawnedBrainId.ToProtoUuid() },
            BrainListFactory = () => BuildBrainList(spawnedBrainId, "Active")
        };
        var vm = CreateViewModel(connections, client);

        vm.SpawnSampleBrainCommand.Execute(null);
        await WaitForAsync(() => vm.SampleBrainStatus.Contains("Sample brain running", StringComparison.Ordinal));

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.RequestPlacementCallCount);
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
        var client = new FakeWorkbenchClient
        {
            SpawnBrainAck = new SpawnBrainAck { BrainId = spawnedBrainId.ToProtoUuid() },
            BrainListFactory = () => BuildBrainList(spawnedBrainId, "Active")
        };
        var vm = CreateViewModel(connections, client);

        vm.SpawnSampleBrainCommand.Execute(null);
        await WaitForAsync(() => vm.SampleBrainStatus.Contains("Sample brain running", StringComparison.Ordinal));

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(0, client.RequestPlacementCallCount);
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
            () => vm.SampleBrainStatus.Contains("failed to register", StringComparison.OrdinalIgnoreCase),
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
        var client = new FakeWorkbenchClient
        {
            SpawnBrainAck = new SpawnBrainAck { BrainId = spawnedBrainId.ToProtoUuid() },
            PlacementAck = new PlacementAck { Accepted = true, Message = "ok" },
            BrainListFactory = () => BuildBrainList(spawnedBrainId, "Active")
        };
        var vm = new DesignerPanelViewModel(connections, client);
        vm.NewBrainCommand.Execute(null);
        vm.SpawnArtifactRoot = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"));

        vm.SpawnBrainCommand.Execute(null);
        await WaitForAsync(() => vm.Status.Contains("Brain spawned", StringComparison.Ordinal), timeoutMs: 5000);

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(1, client.RequestPlacementCallCount);
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
            PlacementAck = new PlacementAck { Accepted = true, Message = "ok" },
            BrainListFactory = static () => new BrainListResponse()
        };
        var vm = new DesignerPanelViewModel(connections, client);
        vm.NewBrainCommand.Execute(null);
        vm.SpawnArtifactRoot = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"));

        vm.SpawnBrainCommand.Execute(null);
        await WaitForAsync(
            () => vm.Status.Contains("did not register after IO/HiveMind worker placement", StringComparison.Ordinal),
            timeoutMs: 15_000);

        Assert.Equal(1, client.SpawnViaIoCallCount);
        Assert.Equal(1, client.RequestPlacementCallCount);
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

    private static BrainListResponse BuildBrainList(Guid brainId, string state)
        => new()
        {
            Brains =
            {
                new BrainStatus
                {
                    BrainId = brainId.ToProtoUuid(),
                    SpawnedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    LastTickId = 0,
                    State = state
                }
            }
        };

    private sealed class FakeWorkbenchClient : WorkbenchClient
    {
        public NodeListResponse? NodesResponse { get; init; }
        public BrainListResponse? BrainsResponse { get; set; }
        public Func<BrainListResponse?>? BrainListFactory { get; set; }
        public SettingListResponse? SettingsResponse { get; init; }
        public SpawnBrainAck? SpawnBrainAck { get; set; }
        public PlacementAck? PlacementAck { get; set; }
        public bool KillBrainResult { get; set; } = true;
        public int SpawnViaIoCallCount { get; private set; }
        public int RequestPlacementCallCount { get; private set; }
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

        public override Task<BrainListResponse?> ListBrainsAsync()
            => Task.FromResult(BrainListFactory?.Invoke() ?? BrainsResponse);

        public override Task<SettingListResponse?> ListSettingsAsync()
            => Task.FromResult(SettingsResponse);

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
