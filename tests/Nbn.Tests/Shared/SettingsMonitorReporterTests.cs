using Nbn.Proto.Settings;
using Nbn.Shared;
using Proto;

namespace Nbn.Tests.Shared;

public sealed class SettingsMonitorReporterTests
{
    [Fact]
    public void BuildDefaultCapabilities_ProvideNonZeroCpuAndCapacityLimits()
    {
        var capabilities = SettingsMonitorReporter.BuildDefaultCapabilities();

        Assert.True(capabilities.CpuCores >= 1);
        Assert.Equal((uint)100, capabilities.CpuLimitPercent);
        Assert.Equal((uint)100, capabilities.RamLimitPercent);
        Assert.Equal((uint)100, capabilities.StorageLimitPercent);
        Assert.Equal((uint)100, capabilities.GpuComputeLimitPercent);
        Assert.Equal((uint)100, capabilities.GpuVramLimitPercent);
        Assert.True(capabilities.RamTotalBytes >= capabilities.RamFreeBytes);
        Assert.True(capabilities.StorageTotalBytes >= capabilities.StorageFreeBytes);
    }

    [Fact]
    public async Task Start_ReturnsNull_WhenReporterInputsAreIncomplete()
    {
        var system = new ActorSystem();
        var settingsPid = system.Root.Spawn(Props.FromFunc(_ => Task.CompletedTask));

        try
        {
            Assert.Null(SettingsMonitorReporter.Start(
                system: null,
                settingsPid: settingsPid,
                nodeAddress: "127.0.0.1:12010",
                logicalName: "node-a",
                rootActorName: "SettingsMonitor"));
            Assert.Null(SettingsMonitorReporter.Start(
                system,
                settingsPid: null,
                nodeAddress: "127.0.0.1:12010",
                logicalName: "node-a",
                rootActorName: "SettingsMonitor"));
            Assert.Null(SettingsMonitorReporter.Start(
                system,
                settingsPid: settingsPid,
                nodeAddress: string.Empty,
                logicalName: "node-a",
                rootActorName: "SettingsMonitor"));
            Assert.Null(SettingsMonitorReporter.Start(
                system,
                settingsHost: "127.0.0.1",
                settingsPort: 0,
                settingsName: "SettingsMonitor",
                nodeAddress: "127.0.0.1:12010",
                logicalName: "node-a",
                rootActorName: "SettingsMonitor"));
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task Start_WithDirectSettingsPid_PublishesLifecycleAndUsesLastGoodCapabilitiesAfterProviderFailure()
    {
        var system = new ActorSystem();
        var capture = new ReporterCapture();
        var settingsPid = system.Root.Spawn(Props.FromProducer(() => new ReporterCaptureActor(capture)));

        var fallbackCapabilities = CreateCapabilities(
            cpuCores: 2,
            ramFreeBytes: 2_048,
            storageFreeBytes: 8_192,
            ramTotalBytes: 4_096,
            storageTotalBytes: 16_384);
        var providedCapabilities = CreateCapabilities(
            cpuCores: 8,
            ramFreeBytes: 4_096,
            storageFreeBytes: 16_384,
            ramTotalBytes: 8_192,
            storageTotalBytes: 32_768,
            hasGpu: true,
            gpuName: "gpu0",
            vramFreeBytes: 2_048,
            vramTotalBytes: 4_096,
            cpuScore: 12.5f,
            gpuScore: 24.5f,
            ilgpuCudaAvailable: true);

        var providerCalls = 0;
        var reporter = SettingsMonitorReporter.Start(
            system,
            settingsPid,
            nodeAddress: "127.0.0.1:12010",
            logicalName: "node-a",
            rootActorName: "SettingsMonitor",
            capabilities: fallbackCapabilities,
            capabilitiesProvider: () =>
            {
                if (Interlocked.Increment(ref providerCalls) == 1)
                {
                    return providedCapabilities;
                }

                throw new InvalidOperationException("simulated capability failure");
            },
            heartbeatInterval: TimeSpan.FromMilliseconds(20));

        Assert.NotNull(reporter);
        var activeReporter = reporter;

        try
        {
            var online = await capture.Online.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var heartbeats = await capture.Heartbeats.Task.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Equal(2, heartbeats.Count);
            Assert.Equal(online.NodeId, heartbeats[0].NodeId);
            Assert.Equal(online.NodeId, heartbeats[1].NodeId);
            Assert.Equal((uint)8, heartbeats[0].Caps.CpuCores);
            Assert.Equal((uint)8, heartbeats[1].Caps.CpuCores);
            Assert.Equal((ulong)16_384, heartbeats[0].Caps.StorageFreeBytes);
            Assert.Equal((ulong)16_384, heartbeats[1].Caps.StorageFreeBytes);
            Assert.True(heartbeats[1].TimeMs >= heartbeats[0].TimeMs);

            await activeReporter!.DisposeAsync();
            activeReporter = null;

            var offline = await capture.Offline.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(online.NodeId, offline.NodeId);
            Assert.Equal("node-a", offline.LogicalName);
        }
        finally
        {
            if (activeReporter is not null)
            {
                await activeReporter.DisposeAsync();
            }

            await system.ShutdownAsync();
        }
    }

    private static NodeCapabilities CreateCapabilities(
        uint cpuCores,
        long ramFreeBytes,
        long storageFreeBytes,
        long ramTotalBytes,
        long storageTotalBytes,
        bool hasGpu = false,
        string gpuName = "",
        long vramFreeBytes = 0,
        long vramTotalBytes = 0,
        float cpuScore = 0f,
        float gpuScore = 0f,
        bool ilgpuCudaAvailable = false,
        bool ilgpuOpenclAvailable = false,
        uint cpuLimitPercent = 100,
        uint ramLimitPercent = 100,
        uint storageLimitPercent = 100,
        uint gpuComputeLimitPercent = 100,
        uint gpuVramLimitPercent = 100,
        float processCpuLoadPercent = 0f,
        long processRamUsedBytes = 0)
        => new()
        {
            CpuCores = cpuCores,
            RamFreeBytes = (ulong)ramFreeBytes,
            StorageFreeBytes = (ulong)storageFreeBytes,
            HasGpu = hasGpu,
            GpuName = gpuName,
            VramFreeBytes = (ulong)vramFreeBytes,
            CpuScore = cpuScore,
            GpuScore = gpuScore,
            IlgpuCudaAvailable = ilgpuCudaAvailable,
            IlgpuOpenclAvailable = ilgpuOpenclAvailable,
            RamTotalBytes = (ulong)ramTotalBytes,
            StorageTotalBytes = (ulong)storageTotalBytes,
            VramTotalBytes = (ulong)vramTotalBytes,
            CpuLimitPercent = cpuLimitPercent,
            RamLimitPercent = ramLimitPercent,
            StorageLimitPercent = storageLimitPercent,
            GpuComputeLimitPercent = gpuComputeLimitPercent,
            GpuVramLimitPercent = gpuVramLimitPercent,
            ProcessCpuLoadPercent = processCpuLoadPercent,
            ProcessRamUsedBytes = (ulong)processRamUsedBytes
        };

    private sealed class ReporterCapture
    {
        private readonly List<NodeHeartbeat> _heartbeats = new();

        public TaskCompletionSource<NodeOnline> Online { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<IReadOnlyList<NodeHeartbeat>> Heartbeats { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<NodeOffline> Offline { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void RecordHeartbeat(NodeHeartbeat heartbeat)
        {
            lock (_heartbeats)
            {
                _heartbeats.Add(heartbeat);
                if (_heartbeats.Count >= 2)
                {
                    Heartbeats.TrySetResult(_heartbeats.Take(2).ToArray());
                }
            }
        }
    }

    private sealed class ReporterCaptureActor : IActor
    {
        private readonly ReporterCapture _capture;

        public ReporterCaptureActor(ReporterCapture capture)
        {
            _capture = capture;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case NodeOnline online:
                    _capture.Online.TrySetResult(online);
                    break;
                case NodeHeartbeat heartbeat:
                    _capture.RecordHeartbeat(heartbeat);
                    break;
                case NodeOffline offline:
                    _capture.Offline.TrySetResult(offline);
                    break;
            }

            return Task.CompletedTask;
        }
    }
}
