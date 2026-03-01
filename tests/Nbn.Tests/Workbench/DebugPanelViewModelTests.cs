using Nbn.Proto;
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
    }
}
