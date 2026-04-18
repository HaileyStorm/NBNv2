using System.Net;
using System.Net.Sockets;
using System.Reflection;
using Nbn.Proto.Debug;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Proto;

namespace Nbn.Tests.Workbench;

public sealed class WorkbenchClientObservabilityTests
{
    [Fact]
    public async Task ConnectObservabilityAsync_ReturnsFalse_WhenEndpointUnreachable()
    {
        var sink = new RecordingSink();
        var clientPort = ReserveFreePort();
        var obsPort = ReserveFreePort();

        await using var client = new WorkbenchClient(sink);
        await client.EnsureStartedAsync("127.0.0.1", clientPort);

        var connected = await client.ConnectObservabilityAsync(
            "127.0.0.1",
            obsPort,
            "DebugHub",
            "VisualizationHub",
            Nbn.Proto.Severity.SevInfo,
            string.Empty);

        Assert.False(connected);
        Assert.False(sink.LastObsConnected);
        Assert.Contains("unreachable", sink.LastObsStatus ?? string.Empty, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ConnectObservabilityAsync_ReturnsTrue_WhenEndpointReachable()
    {
        var sink = new RecordingSink();
        var clientPort = ReserveFreePort();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var obsPort = ((IPEndPoint)listener.LocalEndpoint).Port;

        await using var client = new WorkbenchClient(sink);
        await client.EnsureStartedAsync("127.0.0.1", clientPort);

        var connected = await client.ConnectObservabilityAsync(
            "127.0.0.1",
            obsPort,
            "DebugHub",
            "VisualizationHub",
            Nbn.Proto.Severity.SevInfo,
            string.Empty);

        Assert.True(connected);
        Assert.True(sink.LastObsConnected);
        Assert.Contains("Connected to", sink.LastObsStatus ?? string.Empty, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task EnsureStartedAsync_RestartsClient_WhenAdvertisedHostChanges()
    {
        var sink = new RecordingSink();
        var clientPort = ReserveFreePort();

        await using var client = new WorkbenchClient(sink);
        await client.EnsureStartedAsync("0.0.0.0", clientPort, "198.51.100.10");
        var initialLabel = client.ReceiverLabel;

        await client.EnsureStartedAsync("0.0.0.0", clientPort, "203.0.113.25");
        var updatedLabel = client.ReceiverLabel;

        Assert.Contains("198.51.100.10", initialLabel, StringComparison.Ordinal);
        Assert.Contains("203.0.113.25", updatedLabel, StringComparison.Ordinal);
        Assert.NotEqual(initialLabel, updatedLabel);
    }

    [Fact]
    public async Task EnsureStartedAsync_Retries_WhenAllInterfaceReceiverPortIsTemporarilyBusy()
    {
        var sink = new RecordingSink();
        using var listener = new TcpListener(IPAddress.Any, 0);
        listener.Start();
        var clientPort = ((IPEndPoint)listener.LocalEndpoint).Port;
        var releaseTask = Task.Run(async () =>
        {
            await Task.Delay(500);
            listener.Stop();
        });

        await using var client = new WorkbenchClient(sink);
        await client.EnsureStartedAsync("0.0.0.0", clientPort, "127.0.0.1");
        await releaseTask;

        Assert.True(client.IsRunning);
        Assert.Contains($":{clientPort}/", client.ReceiverLabel, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SetDebugSubscription_DoesNotResubscribe_WhenFilterIsUnchanged()
    {
        var sink = new RecordingSink();
        var clientPort = ReserveFreePort();

        await using var client = new WorkbenchClient(sink);
        await client.EnsureStartedAsync("127.0.0.1", clientPort);

        var system = GetPrivateField<ActorSystem>(client, "_system");
        var debugHubPid = system.Root.Spawn(Props.FromProducer(static () => new DebugSubscribeCounterActor()));
        SetPrivateField(client, "_debugHubPid", debugHubPid);

        var filter = new DebugSubscriptionFilter(
            StreamEnabled: true,
            MinSeverity: Nbn.Proto.Severity.SevWarn,
            ContextRegex: "brain",
            IncludeContextPrefixes: new[] { "hivemind." },
            ExcludeContextPrefixes: Array.Empty<string>(),
            IncludeSummaryPrefixes: new[] { "brain." },
            ExcludeSummaryPrefixes: Array.Empty<string>());

        client.SetDebugSubscription(true, filter);
        client.SetDebugSubscription(true, filter);

        var subscribeCount = await system.Root.RequestAsync<int>(debugHubPid, new GetDebugSubscribeCount());
        Assert.Equal(1, subscribeCount);
    }

    private static int ReserveFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private static T GetPrivateField<T>(object target, string name) where T : class
    {
        var field = target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        var value = field!.GetValue(target);
        var typed = value as T;
        Assert.NotNull(typed);
        return typed!;
    }

    private static void SetPrivateField(object target, string name, object? value)
    {
        var field = target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        field!.SetValue(target, value);
    }

    private sealed record GetDebugSubscribeCount;

    private sealed class DebugSubscribeCounterActor : IActor
    {
        private int _count;

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case DebugSubscribe:
                    _count++;
                    break;
                case GetDebugSubscribeCount:
                    context.Respond(_count);
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class RecordingSink : IWorkbenchEventSink
    {
        public string? LastObsStatus { get; private set; }

        public bool LastObsConnected { get; private set; }

        public void OnOutputEvent(OutputEventItem item) { }

        public void OnOutputVectorEvent(OutputVectorEventItem item) { }

        public void OnDebugEvent(DebugEventItem item) { }

        public void OnVizEvent(VizEventItem item) { }

        public void OnBrainTerminated(BrainTerminatedItem item) { }

        public void OnIoStatus(string status, bool connected) { }

        public void OnObsStatus(string status, bool connected)
        {
            LastObsStatus = status;
            LastObsConnected = connected;
        }

        public void OnSettingsStatus(string status, bool connected) { }

        public void OnHiveMindStatus(string status, bool connected) { }

        public void OnSettingChanged(SettingItem item) { }
    }
}
