using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Settings;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Workbench;

public sealed class DebugPanelViewModelTests
{
    [Fact]
    public void ApplySetting_UpdatesSubscriptionFieldsFromSettingsKeys()
    {
        var viewModel = CreateViewModel();

        Assert.True(viewModel.ApplySetting(new SettingItem(DebugSettingsKeys.EnabledKey, "true", string.Empty)));
        Assert.True(viewModel.ApplySetting(new SettingItem(DebugSettingsKeys.MinSeverityKey, "SEV_WARN", string.Empty)));
        Assert.True(viewModel.ApplySetting(new SettingItem(DebugSettingsKeys.ContextRegexKey, "hivemind\\..*", string.Empty)));
        Assert.True(viewModel.ApplySetting(new SettingItem(DebugSettingsKeys.IncludeContextPrefixesKey, "hivemind., region.", string.Empty)));
        Assert.True(viewModel.ApplySetting(new SettingItem(DebugSettingsKeys.ExcludeContextPrefixesKey, "hivemind.tick", string.Empty)));
        Assert.True(viewModel.ApplySetting(new SettingItem(DebugSettingsKeys.IncludeSummaryPrefixesKey, "brain., placement.", string.Empty)));
        Assert.True(viewModel.ApplySetting(new SettingItem(DebugSettingsKeys.ExcludeSummaryPrefixesKey, "brain.terminated", string.Empty)));

        var filter = viewModel.BuildSubscriptionFilter();
        Assert.True(filter.StreamEnabled);
        Assert.Equal(Severity.SevWarn, filter.MinSeverity);
        Assert.Equal("hivemind\\..*", filter.ContextRegex);
        Assert.Equal(new[] { "hivemind.", "region." }, filter.IncludeContextPrefixes);
        Assert.Equal(new[] { "hivemind.tick" }, filter.ExcludeContextPrefixes);
        Assert.Equal(new[] { "brain.", "placement." }, filter.IncludeSummaryPrefixes);
        Assert.Equal(new[] { "brain.terminated" }, filter.ExcludeSummaryPrefixes);
    }

    [Fact]
    public void BuildSubscriptionFilter_DeduplicatesAndNormalizesScopeLists()
    {
        var viewModel = CreateViewModel();

        viewModel.StreamEnabled = true;
        viewModel.SelectedSeverity = viewModel.SeverityOptions.First(option => option.Severity == Severity.SevInfo);
        viewModel.ContextRegex = "brain";
        viewModel.IncludeContextPrefixes = "hivemind., region., hivemind.";
        viewModel.ExcludeContextPrefixes = "region.tick; region.tick";
        viewModel.IncludeSummaryPrefixes = "brain.\nplacement.\nbrain.";
        viewModel.ExcludeSummaryPrefixes = "tick.duplicate\r\ntick.duplicate";

        var filter = viewModel.BuildSubscriptionFilter();

        Assert.Equal(new[] { "hivemind.", "region." }, filter.IncludeContextPrefixes);
        Assert.Equal(new[] { "region.tick" }, filter.ExcludeContextPrefixes);
        Assert.Equal(new[] { "brain.", "placement." }, filter.IncludeSummaryPrefixes);
        Assert.Equal(new[] { "tick.duplicate" }, filter.ExcludeSummaryPrefixes);
    }

    [Fact]
    public void ApplySetting_StreamEnabled_RaisesSubscriptionSettingsChanged()
    {
        var viewModel = CreateViewModel();
        var notifications = 0;
        viewModel.SubscriptionSettingsChanged += () => notifications++;

        var applied = viewModel.ApplySetting(new SettingItem(DebugSettingsKeys.EnabledKey, "true", string.Empty));

        Assert.True(applied);
        Assert.True(viewModel.StreamEnabled);
        Assert.Equal(1, notifications);
    }

    [Fact]
    public void ApplyFilter_PersistsDebugSettingsAndRefreshesSubscription()
    {
        var client = new RecordingWorkbenchClient(new NullWorkbenchEventSink());
        var viewModel = new DebugPanelViewModel(client, new UiDispatcher())
        {
            StreamEnabled = true,
            ContextRegex = "hivemind\\..*",
            IncludeContextPrefixes = "hivemind., region.",
            ExcludeContextPrefixes = "hivemind.tick",
            IncludeSummaryPrefixes = "brain., placement.",
            ExcludeSummaryPrefixes = "brain.terminated"
        };
        viewModel.SelectedSeverity = viewModel.SeverityOptions.First(option => option.Severity == Severity.SevWarn);

        viewModel.ApplyFilterCommand.Execute(null);

        var applied = SpinWait.SpinUntil(
            () => client.Settings.ContainsKey(DebugSettingsKeys.ExcludeSummaryPrefixesKey),
            TimeSpan.FromSeconds(2));
        Assert.True(applied);
        Assert.Equal("true", client.Settings[DebugSettingsKeys.EnabledKey]);
        Assert.Equal(Severity.SevWarn.ToString(), client.Settings[DebugSettingsKeys.MinSeverityKey]);
        Assert.Equal("hivemind\\..*", client.Settings[DebugSettingsKeys.ContextRegexKey]);
        Assert.Equal("hivemind.,region.", client.Settings[DebugSettingsKeys.IncludeContextPrefixesKey]);
        Assert.Equal("hivemind.tick", client.Settings[DebugSettingsKeys.ExcludeContextPrefixesKey]);
        Assert.Equal("brain.,placement.", client.Settings[DebugSettingsKeys.IncludeSummaryPrefixesKey]);
        Assert.Equal("brain.terminated", client.Settings[DebugSettingsKeys.ExcludeSummaryPrefixesKey]);
    }

    [Fact]
    public void ApplyFilter_WhenServicesNotReady_ShowsPrereqMessage_AndSkipsSettingWrites()
    {
        var client = new RecordingWorkbenchClient(new NullWorkbenchEventSink());
        var connections = new ConnectionViewModel
        {
            SettingsStatus = "Disconnected",
            ObsStatus = "Offline"
        };
        var viewModel = new DebugPanelViewModel(client, new UiDispatcher(), connections);

        viewModel.ApplyFilterCommand.Execute(null);

        var blocked = SpinWait.SpinUntil(
            () => string.Equals(viewModel.Status, "Connect Settings and Observability first.", StringComparison.Ordinal),
            TimeSpan.FromSeconds(2));
        Assert.True(blocked);
        Assert.Empty(client.Settings);
    }

    [Fact]
    public void ApplyFilter_WhenStatusesShowReady_AllowsApplyEvenIfFlagsLag()
    {
        var client = new RecordingWorkbenchClient(new NullWorkbenchEventSink());
        var connections = new ConnectionViewModel
        {
            SettingsStatus = "Ready",
            ObsStatus = "Connected"
        };
        var viewModel = new DebugPanelViewModel(client, new UiDispatcher(), connections)
        {
            StreamEnabled = true
        };

        viewModel.ApplyFilterCommand.Execute(null);

        var applied = SpinWait.SpinUntil(
            () => client.Settings.ContainsKey(DebugSettingsKeys.EnabledKey),
            TimeSpan.FromSeconds(2));
        Assert.True(applied);
        Assert.Equal("true", client.Settings[DebugSettingsKeys.EnabledKey]);
    }

    [Fact]
    public async Task SystemLoadRefresh_WhenServicesReady_PopulatesSummaries()
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var client = new RecordingWorkbenchClient(new NullWorkbenchEventSink())
        {
            WorkerInventoryResponse = new WorkerInventorySnapshotResponse
            {
                SnapshotMs = (ulong)nowMs,
                Workers =
                {
                    new WorkerReadinessCapability
                    {
                        NodeId = Guid.NewGuid().ToProtoUuid(),
                        LogicalName = "nbn.worker",
                        Address = "worker-a:12040",
                        RootActorName = "worker-node",
                        IsAlive = true,
                        IsReady = true,
                        LastSeenMs = (ulong)nowMs,
                        HasCapabilities = true,
                        CapabilityTimeMs = (ulong)nowMs,
                        Capabilities = new NodeCapabilities
                        {
                            CpuCores = 16,
                            CpuLimitPercent = 75,
                            ProcessCpuLoadPercent = 37.5f,
                            RamTotalBytes = 32UL * 1024 * 1024 * 1024,
                            RamLimitPercent = 80,
                            ProcessRamUsedBytes = 8UL * 1024 * 1024 * 1024,
                            StorageTotalBytes = 256UL * 1024 * 1024 * 1024,
                            StorageFreeBytes = 192UL * 1024 * 1024 * 1024,
                            StorageLimitPercent = 90
                        }
                    }
                }
            },
            HiveMindStatusResponse = new HiveMindStatus
            {
                TargetTickHz = 15f,
                ConfiguredTargetTickHz = 30f,
                AutomaticBackpressureActive = true,
                RecentTickSampleCount = 32,
                RecentTimeoutTickCount = 4,
                RecentLateTickCount = 2,
                WorkerPressureWindow = 6,
                CurrentPressureWorkerCount = 0,
                RecentPressureWorkerCount = 1
            }
        };
        var connections = new ConnectionViewModel
        {
            SettingsStatus = "Ready",
            HiveMindStatus = "Connected"
        };

        await using var viewModel = new DebugPanelViewModel(client, new UiDispatcher(), connections);

        var updated = SpinWait.SpinUntil(
            () => viewModel.SystemLoadResourceSummary.Contains("CPU 6/12 cores", StringComparison.Ordinal),
            TimeSpan.FromSeconds(3));

        Assert.True(updated);
        Assert.Contains("RAM 8 GiB/25.6 GiB", viewModel.SystemLoadResourceSummary, StringComparison.Ordinal);
        Assert.Contains("storage 64 GiB/230.4 GiB", viewModel.SystemLoadResourceSummary, StringComparison.Ordinal);
        Assert.Contains("0/1 worker over quota now", viewModel.SystemLoadPressureSummary, StringComparison.Ordinal);
        Assert.Contains("last 6 snapshots", viewModel.SystemLoadPressureSummary, StringComparison.Ordinal);
        Assert.Contains("12.5% recent ticks timed out", viewModel.SystemLoadTickSummary, StringComparison.Ordinal);
        Assert.Contains("6.3% had late arrivals", viewModel.SystemLoadTickSummary, StringComparison.Ordinal);
        Assert.Contains("Cadence 15 Hz vs requested 30 Hz", viewModel.SystemLoadTickSummary, StringComparison.Ordinal);
        Assert.Contains("Health:", viewModel.SystemLoadHealthSummary, StringComparison.Ordinal);
        Assert.StartsWith("M ", viewModel.SystemLoadSparklinePathData, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SystemLoadRefresh_WhenServicesNotReady_ShowsGuidance()
    {
        var client = new RecordingWorkbenchClient(new NullWorkbenchEventSink());
        var connections = new ConnectionViewModel
        {
            SettingsStatus = "Disconnected",
            HiveMindStatus = "Disconnected"
        };

        await using var viewModel = new DebugPanelViewModel(client, new UiDispatcher(), connections);

        var updated = SpinWait.SpinUntil(
            () => viewModel.SystemLoadResourceSummary.Contains("connect Settings", StringComparison.OrdinalIgnoreCase),
            TimeSpan.FromSeconds(3));

        Assert.True(updated);
        Assert.Contains("connect HiveMind", viewModel.SystemLoadPressureSummary, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("connect HiveMind", viewModel.SystemLoadTickSummary, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("connect HiveMind", viewModel.SystemLoadHealthSummary, StringComparison.OrdinalIgnoreCase);
    }

    private static DebugPanelViewModel CreateViewModel()
        => new(new WorkbenchClient(new NullWorkbenchEventSink()), new UiDispatcher());

    private sealed class NullWorkbenchEventSink : IWorkbenchEventSink
    {
        public void OnIoStatus(string status, bool connected) { }
        public void OnObsStatus(string status, bool connected) { }
        public void OnSettingsStatus(string status, bool connected) { }
        public void OnHiveMindStatus(string status, bool connected) { }
        public void OnOutputEvent(OutputEventItem item) { }
        public void OnOutputVectorEvent(OutputVectorEventItem item) { }
        public void OnDebugEvent(DebugEventItem item) { }
        public void OnVizEvent(VizEventItem item) { }
        public void OnBrainTerminated(BrainTerminatedItem item) { }
        public void OnSettingChanged(SettingItem item) { }
        public void OnBrainDiscovered(Guid brainId) { }
        public void OnBrainsUpdated(IReadOnlyList<BrainListItem> brains) { }
    }

    private sealed class RecordingWorkbenchClient : WorkbenchClient
    {
        public RecordingWorkbenchClient(IWorkbenchEventSink sink)
            : base(sink)
        {
        }

        public Dictionary<string, string> Settings { get; } = new(StringComparer.OrdinalIgnoreCase);
        public WorkerInventorySnapshotResponse? WorkerInventoryResponse { get; set; }
        public HiveMindStatus? HiveMindStatusResponse { get; set; }

        public override Task<Nbn.Proto.Settings.SettingValue?> SetSettingAsync(string key, string value)
        {
            Settings[key] = value;
            return Task.FromResult<Nbn.Proto.Settings.SettingValue?>(
                new Nbn.Proto.Settings.SettingValue
                {
                    Key = key,
                    Value = value
                });
        }

        public override Task<WorkerInventorySnapshotResponse?> ListWorkerInventorySnapshotAsync()
            => Task.FromResult(WorkerInventoryResponse);

        public override Task<HiveMindStatus?> GetHiveMindStatusAsync()
            => Task.FromResult(HiveMindStatusResponse);
    }
}
