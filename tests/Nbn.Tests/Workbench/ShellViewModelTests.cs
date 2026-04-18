using System.Diagnostics;
using System.Reflection;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Workbench;

public sealed class ShellViewModelTests
{
    [Fact]
    public async Task MultiBrain_SelectionChange_UpdatesVizScope_And_InputRouting()
    {
        var client = new FakeWorkbenchClient();
        var brainA = new BrainListItem(Guid.NewGuid(), "A", true);
        var brainB = new BrainListItem(Guid.NewGuid(), "B", true);
        client.BrainInfoById[brainA.BrainId] = new BrainInfo { InputWidth = 1, OutputWidth = 1 };
        client.BrainInfoById[brainB.BrainId] = new BrainInfo { InputWidth = 1, OutputWidth = 1 };

        await using var shell = new ShellViewModel(client, autoConnect: false);
        shell.Viz.SetBrains(new[] { brainA, brainB });

        shell.Viz.SelectedBrain = brainA;
        shell.OnVizEvent(CreateVizEvent(
            brainA.BrainId,
            tickId: 1,
            eventId: "a1"));
        shell.OnVizEvent(CreateVizEvent(
            brainB.BrainId,
            tickId: 1,
            eventId: "b1"));

        await WaitForAsync(() =>
            shell.Viz.VizEvents.Any(item => item.EventId == "a1"));

        Assert.DoesNotContain(shell.Viz.VizEvents, item => item.EventId == "b1");

        shell.Io.InputVectorText = "1";
        shell.Io.SendVectorCommand.Execute(null);

        shell.Viz.SelectedBrain = brainB;
        await WaitForAsync(() => shell.Io.BrainIdText == brainB.BrainId.ToString("D"));

        shell.OnVizEvent(CreateVizEvent(
            brainA.BrainId,
            tickId: 2,
            eventId: "a2"));
        shell.OnVizEvent(CreateVizEvent(
            brainB.BrainId,
            tickId: 2,
            eventId: "b2"));

        await WaitForAsync(() =>
            shell.Viz.VizEvents.Any(item => item.EventId == "b2"));

        Assert.DoesNotContain(shell.Viz.VizEvents, item => item.EventId == "a2");
        Assert.Contains(shell.Viz.VizEvents, item => item.EventId == "b2");

        shell.Io.InputVectorText = "1";
        shell.Io.SendVectorCommand.Execute(null);

        shell.Io.AutoSendInputVectorEveryTick = true;
        shell.OnOutputEvent(CreateOutputEvent(brainB.BrainId, tickId: 10, outputIndex: 0, value: 0.25f));
        shell.OnOutputEvent(CreateOutputEvent(brainB.BrainId, tickId: 10, outputIndex: 1, value: 0.5f));
        shell.OnOutputEvent(CreateOutputEvent(brainB.BrainId, tickId: 11, outputIndex: 0, value: 0.75f));

        await WaitForAsync(() => client.InputVectorCalls.Count == 4);

        Assert.Equal(
            new[] { brainA.BrainId, brainB.BrainId, brainB.BrainId, brainB.BrainId },
            client.InputVectorCalls.Select(call => call.BrainId).ToArray());
    }

    [Fact]
    public async Task OnBrainsUpdated_FansOutFullSharedBrainList_ToWorkbenchPanels()
    {
        var client = new FakeWorkbenchClient();
        var brains = Enumerable.Range(0, 70)
            .Select(index => new BrainListItem(Guid.NewGuid(), $"Active-{index}", true))
            .ToArray();

        await using var shell = new ShellViewModel(client, autoConnect: false);

        var method = typeof(ShellViewModel).GetMethod(
            "OnBrainsUpdated",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        method!.Invoke(shell, new object[] { brains });

        await WaitForAsync(() =>
            shell.Viz.KnownBrains.Count == 70
            && shell.Repro.ActiveBrains.Count == 70
            && shell.Speciation.SimActiveBrains.Count == 70
            && shell.Io.KnownBrains.Count == 70);

        Assert.Equal(70, shell.Viz.KnownBrains.Count);
        Assert.Equal(70, shell.Repro.ActiveBrains.Count);
        Assert.Equal(70, shell.Speciation.SimActiveBrains.Count);
        Assert.Equal(70, shell.Io.KnownBrains.Count);
        Assert.Equal("Active brains: 70", shell.Io.ActiveBrainsSummary);
    }

    [Fact]
    public async Task OnSettingChanged_UpdatesReproDefaultsFromExternalSettings()
    {
        var client = new FakeWorkbenchClient();

        await using var shell = new ShellViewModel(client, autoConnect: false);

        shell.OnSettingChanged(new SettingItem(
            ReproductionSettingsKeys.SpawnChildKey,
            "spawn_child_never",
            "1"));

        await WaitForAsync(() => shell.Repro.SelectedSpawnPolicy.Value == SpawnChildPolicy.SpawnChildNever);

        Assert.Equal(SpawnChildPolicy.SpawnChildNever, shell.Repro.SelectedSpawnPolicy.Value);
    }

    [Fact]
    public async Task OnSettingChanged_TickCadenceRefreshesAuthoritativeHiveMindStatus()
    {
        var client = new FakeWorkbenchClient
        {
            HiveMindStatusResponse = new HiveMindStatus
            {
                TargetTickHz = 12.5f,
                HasTickRateOverride = true,
                TickRateOverrideHz = 25f
            }
        };

        await using var shell = new ShellViewModel(client, autoConnect: false);

        shell.OnSettingChanged(new SettingItem(
            TickSettingsKeys.CadenceHzKey,
            "25",
            "1"));

        await WaitForAsync(() =>
            shell.Viz.TickRateOverrideText == "40ms"
            && shell.Viz.TickCadenceSummary == "Current cadence: 12.5 Hz (80 ms/tick).");

        Assert.Equal("40ms", shell.Viz.TickRateOverrideText);
        Assert.Equal("Current cadence: 12.5 Hz (80 ms/tick).", shell.Viz.TickCadenceSummary);
        Assert.Equal(
            "Tick cadence control target: 25 Hz (40 ms/tick). Current runtime target 12.5 Hz (80 ms/tick).",
            shell.Viz.TickRateOverrideSummary);
    }

    [Fact]
    public async Task OnSettingChanged_TickCadenceIgnoresStaleHiveMindStatusResponses()
    {
        var firstStatus = new TaskCompletionSource<HiveMindStatus?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondStatus = new TaskCompletionSource<HiveMindStatus?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new FakeWorkbenchClient();
        client.PendingHiveMindStatusResponses.Enqueue(firstStatus.Task);
        client.PendingHiveMindStatusResponses.Enqueue(secondStatus.Task);

        await using var shell = new ShellViewModel(client, autoConnect: false);

        shell.OnSettingChanged(new SettingItem(TickSettingsKeys.CadenceHzKey, "25", "1"));
        shell.OnSettingChanged(new SettingItem(TickSettingsKeys.CadenceHzKey, "20", "2"));

        secondStatus.TrySetResult(new HiveMindStatus
        {
            TargetTickHz = 20f,
            HasTickRateOverride = true,
            TickRateOverrideHz = 20f
        });

        await WaitForAsync(() =>
            shell.Viz.TickRateOverrideText == "50ms"
            && shell.Viz.TickCadenceSummary == "Current cadence: 20 Hz (50 ms/tick).");

        firstStatus.TrySetResult(new HiveMindStatus
        {
            TargetTickHz = 12.5f,
            HasTickRateOverride = true,
            TickRateOverrideHz = 25f
        });

        await Task.Delay(100);

        Assert.Equal("50ms", shell.Viz.TickRateOverrideText);
        Assert.Equal("Current cadence: 20 Hz (50 ms/tick).", shell.Viz.TickCadenceSummary);
        Assert.Equal(
            "Tick cadence control target: 20 Hz (50 ms/tick). Current runtime target 20 Hz (50 ms/tick).",
            shell.Viz.TickRateOverrideSummary);
    }

    [Fact]
    public async Task DisconnectAll_IgnoresInFlightTickCadenceRefreshResponses()
    {
        var pendingStatus = new TaskCompletionSource<HiveMindStatus?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new FakeWorkbenchClient();
        client.PendingHiveMindStatusResponses.Enqueue(pendingStatus.Task);

        await using var shell = new ShellViewModel(client, autoConnect: false);

        shell.OnSettingChanged(new SettingItem(TickSettingsKeys.CadenceHzKey, "20", "1"));
        shell.DisconnectAllCommand.Execute(null);

        pendingStatus.TrySetResult(new HiveMindStatus
        {
            TargetTickHz = 20f,
            HasTickRateOverride = true,
            TickRateOverrideHz = 20f
        });

        await Task.Delay(100);

        Assert.Equal("Current cadence: awaiting HiveMind status.", shell.Viz.TickCadenceSummary);
        Assert.Equal("Tick cadence control is not set.", shell.Viz.TickRateOverrideSummary);
    }

    [Fact]
    public async Task ConnectHiveMindWithRetryAsync_IgnoresCanceledResponses()
    {
        var pendingStatus = new TaskCompletionSource<HiveMindStatus?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new FakeWorkbenchClient();
        client.PendingConnectHiveMindResponses.Enqueue(pendingStatus.Task);

        await using var shell = new ShellViewModel(client, autoConnect: false);

        var method = typeof(ShellViewModel).GetMethod(
            "ConnectHiveMindWithRetryAsync",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        using var cts = new CancellationTokenSource();
        var task = (Task)method!.Invoke(shell, new object[] { cts.Token })!;
        await WaitForAsync(() => client.ConnectHiveMindCallCount == 1);

        cts.Cancel();
        pendingStatus.TrySetResult(new HiveMindStatus
        {
            TargetTickHz = 20f,
            HasTickRateOverride = true,
            TickRateOverrideHz = 20f
        });

        await task;

        Assert.Equal("Current cadence: awaiting HiveMind status.", shell.Viz.TickCadenceSummary);
        Assert.Equal("Tick cadence control is not set.", shell.Viz.TickRateOverrideSummary);
    }

    [Fact]
    public async Task ConnectAllAsync_WhenReceiverStartFails_PreservesExistingConnectionLoop()
    {
        var client = new FakeWorkbenchClient();
        await using var shell = new ShellViewModel(
            client,
            autoConnect: false,
            firewallManager: new FakeLocalFirewallManager(new FirewallAccessResult(FirewallAccessStatus.NotNeeded, string.Empty)));

        var method = typeof(ShellViewModel).GetMethod(
            "ConnectAllAsync",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        await (Task)method!.Invoke(shell, Array.Empty<object>())!;

        var ctsField = typeof(ShellViewModel).GetField(
            "_connectCts",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(ctsField);
        var firstCts = Assert.IsType<CancellationTokenSource>(ctsField!.GetValue(shell));

        client.EnsureStartedException = new InvalidOperationException("receiver restart failed");

        await (Task)method.Invoke(shell, Array.Empty<object>())!;

        Assert.Same(firstCts, ctsField.GetValue(shell));
        Assert.False(firstCts.IsCancellationRequested);
        Assert.Equal("Connect failed: receiver restart failed", shell.Connections.IoStatus);
    }

    [Fact]
    public async Task ConnectAllAsync_WhenReceiverFirewallNeedsAttention_SurfacesReceiverPort()
    {
        var client = new FakeWorkbenchClient();
        var firewall = new FakeLocalFirewallManager(new FirewallAccessResult(
            FirewallAccessStatus.PermissionRequired,
            "Windows Firewall was not updated for TCP 12555."));
        await using var shell = new ShellViewModel(
            client,
            autoConnect: false,
            firewallManager: firewall);
        shell.Connections.LocalBindHost = NetworkAddressDefaults.DefaultBindHost;
        shell.Connections.LocalPortText = "12555";

        var method = typeof(ShellViewModel).GetMethod(
            "ConnectAllAsync",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        await (Task)method!.Invoke(shell, Array.Empty<object>())!;

        var call = Assert.Single(firewall.Calls);
        Assert.Equal("WorkbenchReceiver", call.Label);
        Assert.Equal(NetworkAddressDefaults.DefaultBindHost, call.BindHost);
        Assert.Equal(12555, call.Port);
        Assert.Contains("firewall attention: TCP 12555", shell.ReceiverLabel, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task OnSettingChanged_PreservesDirtyOrchestratorSettingEdits()
    {
        var client = new FakeWorkbenchClient();

        await using var shell = new ShellViewModel(client, autoConnect: false);

        shell.OnSettingChanged(new SettingItem("demo.setting", "server-1", "server-1"));
        await WaitForAsync(() => shell.Orchestrator.Settings.Count == 1);

        var entry = Assert.Single(shell.Orchestrator.Settings);
        entry.Value = "local-draft";
        Assert.True(entry.IsDirty);

        shell.OnSettingChanged(new SettingItem("demo.setting", "server-2", "server-2"));
        await WaitForAsync(() => entry.Updated == "server-2");

        Assert.Equal("local-draft", entry.Value);
        Assert.Equal("server-2", entry.Updated);
        Assert.True(entry.IsDirty);
    }

    private static VizEventItem CreateVizEvent(Guid brainId, ulong tickId, string eventId)
    {
        return new VizEventItem(
            DateTimeOffset.UtcNow,
            Nbn.Proto.Viz.VizEventType.VizNeuronFired.ToString(),
            brainId.ToString("D"),
            tickId,
            Region: "0",
            Source: "1",
            Target: string.Empty,
            Value: 0.5f,
            Strength: 0f,
            EventId: eventId);
    }

    private static OutputEventItem CreateOutputEvent(Guid brainId, ulong tickId, uint outputIndex, float value)
    {
        var now = DateTimeOffset.UtcNow;
        return new OutputEventItem(
            now,
            now.ToString("g"),
            brainId.ToString("D"),
            outputIndex,
            value,
            tickId);
    }

    private static async Task WaitForAsync(Func<bool> predicate, int timeoutMs = 2000)
    {
        var deadline = Stopwatch.StartNew();
        while (deadline.ElapsedMilliseconds < timeoutMs)
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
        public Dictionary<Guid, BrainInfo> BrainInfoById { get; } = new();
        public List<(Guid BrainId, float[] Values)> InputVectorCalls { get; } = new();
        public Queue<Task<HiveMindStatus?>> PendingHiveMindStatusResponses { get; } = new();
        public Queue<Task<HiveMindStatus?>> PendingConnectHiveMindResponses { get; } = new();
        public int ConnectHiveMindCallCount { get; private set; }
        public int EnsureStartedCallCount { get; private set; }
        public Exception? EnsureStartedException { get; set; }
        public HiveMindStatus? HiveMindStatusResponse { get; set; }

        public FakeWorkbenchClient()
            : base(new NullWorkbenchEventSink())
        {
        }

        public override Task EnsureStartedAsync(string bindHost, int port, string? advertisedHost = null, int? advertisedPort = null)
        {
            EnsureStartedCallCount++;
            if (EnsureStartedException is not null)
            {
                throw EnsureStartedException;
            }

            return Task.CompletedTask;
        }

        public override Task<BrainInfo?> RequestBrainInfoAsync(Guid brainId)
        {
            if (BrainInfoById.TryGetValue(brainId, out var info))
            {
                return Task.FromResult<BrainInfo?>(info);
            }

            return Task.FromResult<BrainInfo?>(null);
        }

        public override void SendInputVector(Guid brainId, IReadOnlyList<float> values)
        {
            InputVectorCalls.Add((brainId, values.ToArray()));
        }

        public override Task<HiveMindStatus?> GetHiveMindStatusAsync()
            => PendingHiveMindStatusResponses.Count > 0
                ? PendingHiveMindStatusResponses.Dequeue()
                : Task.FromResult(HiveMindStatusResponse);

        public override Task<HiveMindStatus?> ConnectHiveMindAsync(string host, int port, string actorName)
        {
            ConnectHiveMindCallCount++;
            return PendingConnectHiveMindResponses.Count > 0
                ? PendingConnectHiveMindResponses.Dequeue()
                : Task.FromResult(HiveMindStatusResponse);
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

    private sealed class FakeLocalFirewallManager(FirewallAccessResult result) : ILocalFirewallManager
    {
        public List<(string Label, string BindHost, int Port)> Calls { get; } = new();

        public Task<FirewallAccessResult> EnsureInboundTcpAccessAsync(string label, string bindHost, int port)
        {
            Calls.Add((label, bindHost, port));
            return Task.FromResult(result);
        }
    }
}
