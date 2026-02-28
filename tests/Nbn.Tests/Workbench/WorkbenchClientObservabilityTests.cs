using System.Net;
using System.Net.Sockets;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

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

    private static int ReserveFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
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
