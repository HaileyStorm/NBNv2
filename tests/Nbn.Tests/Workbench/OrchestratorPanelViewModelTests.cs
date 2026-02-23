using System.Diagnostics;
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

    private sealed class FakeWorkbenchClient : WorkbenchClient
    {
        public NodeListResponse? NodesResponse { get; init; }
        public BrainListResponse? BrainsResponse { get; init; }
        public SettingListResponse? SettingsResponse { get; init; }

        public FakeWorkbenchClient()
            : base(new NullWorkbenchEventSink())
        {
        }

        public override Task<NodeListResponse?> ListNodesAsync()
            => Task.FromResult(NodesResponse);

        public override Task<BrainListResponse?> ListBrainsAsync()
            => Task.FromResult(BrainsResponse);

        public override Task<SettingListResponse?> ListSettingsAsync()
            => Task.FromResult(SettingsResponse);
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
