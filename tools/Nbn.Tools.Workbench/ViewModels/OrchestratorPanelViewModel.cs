using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class OrchestratorPanelViewModel : ViewModelBase
{
    private const int MaxRows = 200;
    private const long StaleNodeMs = 15000;
    private const long SpawnVisibilityGraceMs = 30000;
    private const string SampleRouterPrefix = "demo-router";
    private const string SampleBrainRootPrefix = "sample-root-";
    private const string SampleOutputPrefix = "io-output-";
    private const float LocalDefaultTickHz = 8f;
    private const float LocalDefaultMinTickHz = 2f;
    private static readonly TimeSpan SpawnRegistrationTimeout = TimeSpan.FromSeconds(12);
    private static readonly TimeSpan SpawnRegistrationPollInterval = TimeSpan.FromMilliseconds(300);
    private readonly UiDispatcher _dispatcher;
    private readonly ConnectionViewModel _connections;
    private readonly WorkbenchClient _client;
    private readonly LocalServiceRunner _settingsRunner = new();
    private readonly LocalServiceRunner _hiveMindRunner = new();
    private readonly LocalServiceRunner _ioRunner = new();
    private readonly LocalServiceRunner _reproRunner = new();
    private readonly LocalServiceRunner _obsRunner = new();
    private readonly LocalServiceRunner _sampleBrainRunner = new();
    private readonly LocalServiceRunner _sampleRegionRunner = new();
    private readonly LocalServiceRunner _sampleInputRunner = new();
    private readonly LocalServiceRunner _sampleOutputRunner = new();
    private readonly Action<Guid>? _brainDiscovered;
    private readonly Action<IReadOnlyList<BrainListItem>>? _brainsUpdated;
    private readonly Func<Task>? _connectAll;
    private readonly Action? _disconnectAll;
    private readonly SemaphoreSlim _refreshGate = new(1, 1);
    private string _statusMessage = "Idle";
    private string _settingsLaunchStatus = "Idle";
    private string _hiveMindLaunchStatus = "Idle";
    private string _ioLaunchStatus = "Idle";
    private string _reproLaunchStatus = "Idle";
    private string _obsLaunchStatus = "Idle";
    private string _sampleBrainStatus = "Not running.";
    private readonly Dictionary<Guid, BrainListItem> _lastBrains = new();
    private readonly CancellationTokenSource _refreshCts = new();
    private readonly TimeSpan _autoRefreshInterval = TimeSpan.FromSeconds(3);
    private Guid? _sampleBrainId;

    public OrchestratorPanelViewModel(
        UiDispatcher dispatcher,
        ConnectionViewModel connections,
        WorkbenchClient client,
        Action<Guid>? brainDiscovered = null,
        Action<IReadOnlyList<BrainListItem>>? brainsUpdated = null,
        Func<Task>? connectAll = null,
        Action? disconnectAll = null)
    {
        _dispatcher = dispatcher;
        _connections = connections;
        _client = client;
        _brainDiscovered = brainDiscovered;
        _brainsUpdated = brainsUpdated;
        _connectAll = connectAll;
        _disconnectAll = disconnectAll;
        Nodes = new ObservableCollection<NodeStatusItem>();
        Settings = new ObservableCollection<SettingEntryViewModel>();
        Terminations = new ObservableCollection<BrainTerminatedItem>();
        RefreshCommand = new AsyncRelayCommand(() => RefreshAsync(force: true));
        ApplySettingsCommand = new AsyncRelayCommand(ApplySettingsAsync);
        StartSettingsMonitorCommand = new AsyncRelayCommand(StartSettingsMonitorAsync);
        StopSettingsMonitorCommand = new AsyncRelayCommand(() => StopRunnerAsync(_settingsRunner, value => SettingsLaunchStatus = value));
        StartHiveMindCommand = new AsyncRelayCommand(StartHiveMindAsync);
        StopHiveMindCommand = new AsyncRelayCommand(() => StopRunnerAsync(_hiveMindRunner, value => HiveMindLaunchStatus = value));
        StartIoCommand = new AsyncRelayCommand(StartIoAsync);
        StopIoCommand = new AsyncRelayCommand(() => StopRunnerAsync(_ioRunner, value => IoLaunchStatus = value));
        StartReproCommand = new AsyncRelayCommand(StartReproAsync);
        StopReproCommand = new AsyncRelayCommand(() => StopRunnerAsync(_reproRunner, value => ReproLaunchStatus = value));
        StartObsCommand = new AsyncRelayCommand(StartObsAsync);
        StopObsCommand = new AsyncRelayCommand(() => StopRunnerAsync(_obsRunner, value => ObsLaunchStatus = value));
        StartAllCommand = new AsyncRelayCommand(StartAllAsync);
        StopAllCommand = new AsyncRelayCommand(StopAllAsync);
        SpawnSampleBrainCommand = new AsyncRelayCommand(SpawnSampleBrainAsync);
        StopSampleBrainCommand = new AsyncRelayCommand(StopSampleBrainAsync);
        _ = StartAutoRefreshAsync();
    }

    public ObservableCollection<NodeStatusItem> Nodes { get; }
    public ObservableCollection<SettingEntryViewModel> Settings { get; }
    public ObservableCollection<BrainTerminatedItem> Terminations { get; }

    public ConnectionViewModel Connections => _connections;

    public string StatusMessage
    {
        get => _statusMessage;
        set => SetProperty(ref _statusMessage, value);
    }

    public AsyncRelayCommand RefreshCommand { get; }

    public AsyncRelayCommand ApplySettingsCommand { get; }

    public AsyncRelayCommand StartSettingsMonitorCommand { get; }

    public AsyncRelayCommand StopSettingsMonitorCommand { get; }

    public AsyncRelayCommand StartHiveMindCommand { get; }

    public AsyncRelayCommand StopHiveMindCommand { get; }

    public AsyncRelayCommand StartIoCommand { get; }

    public AsyncRelayCommand StopIoCommand { get; }

    public AsyncRelayCommand StartReproCommand { get; }

    public AsyncRelayCommand StopReproCommand { get; }

    public AsyncRelayCommand StartObsCommand { get; }

    public AsyncRelayCommand StopObsCommand { get; }
    public AsyncRelayCommand StartAllCommand { get; }

    public AsyncRelayCommand StopAllCommand { get; }

    public AsyncRelayCommand SpawnSampleBrainCommand { get; }

    public AsyncRelayCommand StopSampleBrainCommand { get; }

    public string SettingsLaunchStatus
    {
        get => _settingsLaunchStatus;
        set => SetProperty(ref _settingsLaunchStatus, value);
    }

    public string HiveMindLaunchStatus
    {
        get => _hiveMindLaunchStatus;
        set => SetProperty(ref _hiveMindLaunchStatus, value);
    }

    public string IoLaunchStatus
    {
        get => _ioLaunchStatus;
        set => SetProperty(ref _ioLaunchStatus, value);
    }

    public string ReproLaunchStatus
    {
        get => _reproLaunchStatus;
        set => SetProperty(ref _reproLaunchStatus, value);
    }

    public string ObsLaunchStatus
    {
        get => _obsLaunchStatus;
        set => SetProperty(ref _obsLaunchStatus, value);
    }

    public string SampleBrainStatus
    {
        get => _sampleBrainStatus;
        set => SetProperty(ref _sampleBrainStatus, value);
    }

    public void UpdateSetting(SettingItem item)
    {
        _dispatcher.Post(() =>
        {
            var index = Settings.Select((entry, idx) => new { entry, idx })
                .FirstOrDefault(row => string.Equals(row.entry.Key, item.Key, StringComparison.OrdinalIgnoreCase))?.idx ?? -1;

            if (index >= 0)
            {
                Settings[index].UpdateFromServer(item.Value, FormatUpdated(item.Updated), preserveEdits: true);
            }
            else
            {
                Settings.Add(new SettingEntryViewModel(item.Key, item.Value, FormatUpdated(item.Updated)));
            }
        });
    }

    public void AddTermination(BrainTerminatedItem item)
    {
        _dispatcher.Post(() =>
        {
            Terminations.Insert(0, item);
            Trim(Terminations);
        });
    }

    public async Task StopAllAsyncForShutdown()
    {
        _refreshCts.Cancel();
        _disconnectAll?.Invoke();
        await StopSampleBrainAsync().ConfigureAwait(false);
        await StopRunnerAsync(_settingsRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_hiveMindRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_ioRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_reproRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_obsRunner, _ => { }).ConfigureAwait(false);
    }

    private async Task StartSettingsMonitorAsync()
    {
        var dbPath = Connections.SettingsDbPath;
        if (string.IsNullOrWhiteSpace(dbPath))
        {
            SettingsLaunchStatus = "Settings DB path required.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.SettingsMonitor");
        if (string.IsNullOrWhiteSpace(projectPath))
        {
            SettingsLaunchStatus = "Repo root not found.";
            return;
        }

        if (!TryParsePort(Connections.SettingsPortText, out var port))
        {
            SettingsLaunchStatus = "Invalid Settings port.";
            return;
        }

        var args = $"--db \"{dbPath}\" --bind-host {Connections.SettingsHost} --port {port}";
        var startInfo = BuildServiceStartInfo(projectPath, "Nbn.Runtime.SettingsMonitor", args);
        var result = await _settingsRunner.StartAsync(startInfo, waitForExit: false, label: "SettingsMonitor");
        SettingsLaunchStatus = result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartHiveMindAsync()
    {
        if (!TryParsePort(Connections.HiveMindPortText, out var port))
        {
            HiveMindLaunchStatus = "Invalid HiveMind port.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.HiveMind");
        if (string.IsNullOrWhiteSpace(projectPath))
        {
            HiveMindLaunchStatus = "Repo root not found.";
            return;
        }

        var args = $"--bind-host {Connections.HiveMindHost} --port {port} --settings-db \"{Connections.SettingsDbPath}\""
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}"
                 + $" --tick-hz {LocalDefaultTickHz:0.###} --min-tick-hz {LocalDefaultMinTickHz:0.###}";
        var startInfo = BuildServiceStartInfo(projectPath, "Nbn.Runtime.HiveMind", args);
        var result = await _hiveMindRunner.StartAsync(startInfo, waitForExit: false, label: "HiveMind");
        HiveMindLaunchStatus = result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartIoAsync()
    {
        if (!TryParsePort(Connections.IoPortText, out var port))
        {
            IoLaunchStatus = "Invalid IO port.";
            return;
        }

        if (!TryParsePort(Connections.HiveMindPortText, out var hivePort))
        {
            IoLaunchStatus = "Invalid HiveMind port.";
            return;
        }

        if (!TryParsePort(Connections.ReproPortText, out var reproPort))
        {
            IoLaunchStatus = "Invalid Reproduction port.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.IO");
        if (string.IsNullOrWhiteSpace(projectPath))
        {
            IoLaunchStatus = "Repo root not found.";
            return;
        }

        var hiveAddress = $"{Connections.HiveMindHost}:{hivePort}";
        var reproAddress = $"{Connections.ReproHost}:{reproPort}";
        var args = $"--bind-host {Connections.IoHost} --port {port}"
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}"
                 + $" --hivemind-address {hiveAddress} --hivemind-name {Connections.HiveMindName}"
                 + $" --repro-address {reproAddress} --repro-name {Connections.ReproManager}";
        var startInfo = BuildServiceStartInfo(projectPath, "Nbn.Runtime.IO", args);
        var result = await _ioRunner.StartAsync(startInfo, waitForExit: false, label: "IoGateway");
        IoLaunchStatus = result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartReproAsync()
    {
        if (!TryParsePort(Connections.ReproPortText, out var reproPort))
        {
            ReproLaunchStatus = "Invalid Reproduction port.";
            return;
        }

        if (!TryParsePort(Connections.IoPortText, out var ioPort))
        {
            ReproLaunchStatus = "Invalid IO port.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.Reproduction");
        if (string.IsNullOrWhiteSpace(projectPath))
        {
            ReproLaunchStatus = "Repo root not found.";
            return;
        }

        var ioAddress = $"{Connections.IoHost}:{ioPort}";
        var args = $"--bind-host {Connections.ReproHost} --port {reproPort}"
                 + $" --manager-name {Connections.ReproManager}"
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}"
                 + $" --io-address {ioAddress} --io-name {Connections.IoGateway}";
        var startInfo = BuildServiceStartInfo(projectPath, "Nbn.Runtime.Reproduction", args);
        var result = await _reproRunner.StartAsync(startInfo, waitForExit: false, label: "Reproduction");
        ReproLaunchStatus = result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartObsAsync()
    {
        if (!TryParsePort(Connections.ObsPortText, out var port))
        {
            ObsLaunchStatus = "Invalid Obs port.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.Observability");
        if (string.IsNullOrWhiteSpace(projectPath))
        {
            ObsLaunchStatus = "Repo root not found.";
            return;
        }

        var args = $"--bind-host {Connections.ObsHost} --port {port}"
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}"
                 + " --enable-debug --enable-viz";
        var startInfo = BuildServiceStartInfo(projectPath, "Nbn.Runtime.Observability", args);
        var result = await _obsRunner.StartAsync(startInfo, waitForExit: false, label: "Observability");
        ObsLaunchStatus = result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private static async Task StopRunnerAsync(LocalServiceRunner runner, Action<string> setStatus)
    {
        setStatus(await runner.StopAsync().ConfigureAwait(false));
    }

    public async Task RefreshSettingsAsync()
    {
        await RefreshAsync(force: true).ConfigureAwait(false);
    }

    private async Task RefreshAsync(bool force)
    {
        if (!await _refreshGate.WaitAsync(0).ConfigureAwait(false))
        {
            return;
        }

        if (!Connections.SettingsConnected)
        {
            if (force)
            {
                StatusMessage = "SettingsMonitor not connected.";
                Connections.SettingsStatus = "Disconnected";
            }
            _refreshGate.Release();
            return;
        }

        if (force)
        {
            StatusMessage = "Loading settings...";
            Connections.SettingsStatus = "Loading";
        }

        try
        {
            var nodesResponse = await _client.ListNodesAsync().ConfigureAwait(false);
            var brainsResponse = await _client.ListBrainsAsync().ConfigureAwait(false);

            var nodes = nodesResponse?.Nodes?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.NodeStatus>();
            var brains = brainsResponse?.Brains?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.BrainStatus>();
            var controllers = brainsResponse?.Controllers?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.BrainControllerStatus>();
            var sortedNodes = nodes
                .OrderByDescending(node => node.LastSeenMs)
                .ThenBy(node => node.LogicalName ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                .ToArray();

            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            UpdateHiveMindEndpoint(nodes, nowMs);

            var settingsResponse = await _client.ListSettingsAsync().ConfigureAwait(false);
            var settings = settingsResponse?.Settings?
                .Select(entry => new SettingItem(
                    entry.Key ?? string.Empty,
                    entry.Value ?? string.Empty,
                    FormatUpdated(entry.UpdatedMs)))
                .ToList() ?? new List<SettingItem>();

            _dispatcher.Post(() =>
            {
                Nodes.Clear();
                foreach (var node in sortedNodes)
                {
                    var isFresh = IsFresh(node.LastSeenMs, nowMs);
                    var isAlive = node.IsAlive && isFresh;
                    var seen = DateTimeOffset.FromUnixTimeMilliseconds((long)node.LastSeenMs).ToLocalTime();
                    Nodes.Add(new NodeStatusItem(
                        node.LogicalName ?? string.Empty,
                        node.Address ?? string.Empty,
                        node.RootActorName ?? string.Empty,
                        seen.ToString("g"),
                        isAlive ? "online" : "offline"));
                }

                foreach (var entry in settings)
                {
                    var existing = Settings.FirstOrDefault(item => string.Equals(item.Key, entry.Key, StringComparison.OrdinalIgnoreCase));
                    if (existing is null)
                    {
                        Settings.Add(new SettingEntryViewModel(entry.Key, entry.Value, FormatUpdated(entry.Updated)));
                    }
                    else
                    {
                        existing.UpdateFromServer(entry.Value, FormatUpdated(entry.Updated), preserveEdits: true);
                    }
                }

                Trim(Nodes);
                Trim(Settings);
            });

            UpdateConnectionStatusesFromNodes(nodes, nowMs);

            var controllerMap = controllers
                .Where(entry => entry.BrainId is not null && entry.BrainId.TryToGuid(out _))
                .ToDictionary(
                    entry => entry.BrainId!.ToGuid(),
                    entry =>
                    {
                        var controllerAlive = entry.IsAlive && IsFresh(entry.LastSeenMs, nowMs);
                        return (entry, controllerAlive);
                    });

            var brainEntries = brains.Select(entry =>
            {
                var brainId = entry.BrainId?.ToGuid() ?? Guid.Empty;
                var alive = controllerMap.TryGetValue(brainId, out var controller) && controller.Item2;
                var spawnedRecently = IsSpawnRecent(entry.SpawnedMs, nowMs);
                var item = new BrainListItem(brainId, entry.State ?? string.Empty, alive);
                return (item, spawnedRecently);
            }).Where(entry => entry.item.BrainId != Guid.Empty).ToList();

            var brainListAll = brainEntries
                .Select(entry => entry.item)
                .ToList();
            RecordBrainTerminations(brainListAll);
            var brainList = brainEntries
                .Where(entry => !string.Equals(entry.item.State, "Dead", StringComparison.OrdinalIgnoreCase))
                .Where(entry => entry.item.ControllerAlive || entry.spawnedRecently)
                .Select(entry => entry.item)
                .ToList();
            _brainsUpdated?.Invoke(brainList);

            if (force)
            {
                StatusMessage = "Settings loaded.";
                Connections.SettingsStatus = "Ready";
            }
        }
        catch (Exception ex)
        {
            StatusMessage = $"Settings load failed: {ex.Message}";
            Connections.SettingsStatus = "Error";
            WorkbenchLog.Warn($"Settings refresh failed: {ex.Message}");
        }
        finally
        {
            _refreshGate.Release();
        }
    }

    private async Task StartAllAsync()
    {
        await StartSettingsMonitorAsync().ConfigureAwait(false);
        await StartHiveMindAsync().ConfigureAwait(false);
        await StartReproAsync().ConfigureAwait(false);
        await StartIoAsync().ConfigureAwait(false);
        await StartObsAsync().ConfigureAwait(false);
    }

    private async Task StopAllAsync()
    {
        _disconnectAll?.Invoke();
        await StopSampleBrainAsync().ConfigureAwait(false);
        await StopRunnerAsync(_obsRunner, value => ObsLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_ioRunner, value => IoLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_reproRunner, value => ReproLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_hiveMindRunner, value => HiveMindLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_settingsRunner, value => SettingsLaunchStatus = value).ConfigureAwait(false);
    }

    private async Task SpawnSampleBrainAsync()
    {
        if (!Connections.SettingsConnected || !Connections.HiveMindConnected || !Connections.IoConnected)
        {
            SampleBrainStatus = "Connect Settings, HiveMind, and IO first.";
            return;
        }

        if (IsSampleBrainRunning())
        {
            SampleBrainStatus = "Sample brain already running.";
            return;
        }

        if (!TryParsePort(Connections.SettingsPortText, out var settingsPort))
        {
            SampleBrainStatus = "Invalid Settings port.";
            return;
        }

        if (!TryParsePort(Connections.HiveMindPortText, out var hivePort))
        {
            SampleBrainStatus = "Invalid HiveMind port.";
            return;
        }

        if (!TryParsePort(Connections.IoPortText, out var ioPort))
        {
            SampleBrainStatus = "Invalid IO port.";
            return;
        }

        if (!TryParsePort(Connections.SampleBrainPortText, out var sampleBrainPort))
        {
            SampleBrainStatus = "Invalid sample brain port.";
            return;
        }

        if (!TryParsePort(Connections.SampleRegionPortText, out var sampleRegionPort))
        {
            SampleBrainStatus = "Invalid sample region port.";
            return;
        }

        var brainHostProjectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.BrainHost");
        if (string.IsNullOrWhiteSpace(brainHostProjectPath))
        {
            SampleBrainStatus = "BrainHost project not found.";
            return;
        }

        var regionProjectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.RegionHost");
        if (string.IsNullOrWhiteSpace(regionProjectPath))
        {
            SampleBrainStatus = "RegionHost project not found.";
            return;
        }

        var bindHost = string.IsNullOrWhiteSpace(Connections.LocalBindHost) ? "127.0.0.1" : Connections.LocalBindHost;
        var settingsHost = string.IsNullOrWhiteSpace(Connections.SettingsHost) ? "127.0.0.1" : Connections.SettingsHost;
        var hiveAddress = $"{Connections.HiveMindHost}:{hivePort}";
        var ioAddress = $"{Connections.IoHost}:{ioPort}";
        var reservedPorts = new HashSet<int>();
        if (!LocalPortAllocator.TryFindAvailablePort(bindHost, sampleBrainPort, reservedPorts, out var sampleBrainListenPort, out var sampleBrainPortError))
        {
            SampleBrainStatus = sampleBrainPortError ?? "Unable to allocate sample brain port.";
            return;
        }

        reservedPorts.Add(sampleBrainListenPort);
        if (!LocalPortAllocator.TryFindAvailablePort(bindHost, sampleRegionPort, reservedPorts, out var sampleRegionListenPort, out var sampleRegionPortError))
        {
            SampleBrainStatus = sampleRegionPortError ?? "Unable to allocate sample region port.";
            return;
        }

        reservedPorts.Add(sampleRegionListenPort);
        if (!LocalPortAllocator.TryFindAvailablePort(bindHost, sampleRegionListenPort + 1, reservedPorts, out var sampleInputPort, out var sampleInputPortError))
        {
            SampleBrainStatus = sampleInputPortError ?? "Unable to allocate sample input port.";
            return;
        }

        reservedPorts.Add(sampleInputPort);
        if (!LocalPortAllocator.TryFindAvailablePort(bindHost, sampleInputPort + 1, reservedPorts, out var sampleOutputPort, out var sampleOutputPortError))
        {
            SampleBrainStatus = sampleOutputPortError ?? "Unable to allocate sample output port.";
            return;
        }

        Connections.SampleBrainPortText = sampleBrainListenPort.ToString();
        Connections.SampleRegionPortText = sampleRegionListenPort.ToString();

        var brainAddress = $"{bindHost}:{sampleBrainListenPort}";

        var runRoot = BuildSampleRunRoot();
        var artifactRoot = Path.Combine(runRoot, "artifacts");
        ResetDirectory(artifactRoot);

        SampleBrainStatus = "Creating sample artifacts...";
        var artifact = await CreateSampleArtifactsAsync(artifactRoot).ConfigureAwait(false);
        if (artifact is null)
        {
            return;
        }

        var brainId = Guid.NewGuid();
        _sampleBrainId = brainId;
        var sampleRouterId = $"{SampleRouterPrefix}-{brainId:N}";
        var sampleBrainRootId = $"{SampleBrainRootPrefix}{brainId:N}";
        var artifactPath = string.IsNullOrWhiteSpace(artifact.ArtifactRoot) ? artifactRoot : artifact.ArtifactRoot;

        SampleBrainStatus = "Starting sample brain host...";
        var brainArgs = "run-brain"
                        + $" --bind-host {bindHost}"
                        + $" --port {sampleBrainListenPort}"
                        + $" --brain-id {brainId:D}"
                        + $" --hivemind-address {hiveAddress}"
                        + $" --hivemind-id {Connections.HiveMindName}"
                        + $" --router-id {sampleRouterId}"
                        + $" --brain-root-id {sampleBrainRootId}"
                        + $" --io-address {ioAddress}"
                        + $" --io-id {Connections.IoGateway}"
                        + $" --settings-host {settingsHost}"
                        + $" --settings-port {settingsPort}"
                        + $" --settings-name {Connections.SettingsName}"
                        + $" --nbn-sha256 {artifact.Sha256}"
                        + $" --nbn-size {artifact.Size}"
                        + $" --artifact-store-uri \"{artifactPath}\"";
        var brainStartInfo = BuildServiceStartInfo(brainHostProjectPath, "Nbn.Runtime.BrainHost", brainArgs);
        var brainResult = await _sampleBrainRunner.StartAsync(brainStartInfo, waitForExit: false, label: "SampleBrainHost").ConfigureAwait(false);
        if (!brainResult.Success)
        {
            SampleBrainStatus = $"Sample brain host failed: {brainResult.Message}";
            await CleanupSampleBrainAsync().ConfigureAwait(false);
            return;
        }

        var regionArgsBase = $"--bind-host {bindHost}"
                             + $" --settings-host {settingsHost}"
                             + $" --settings-port {settingsPort}"
                             + $" --settings-name {Connections.SettingsName}"
                             + $" --brain-id {brainId:D}"
                             + " --neuron-start 0"
                             + " --neuron-count 1"
                             + " --shard-index 0"
                             + $" --router-address {brainAddress}"
                             + $" --router-id {sampleRouterId}"
                             + $" --tick-address {hiveAddress}"
                             + $" --tick-id {Connections.HiveMindName}"
                             + $" --nbn-sha256 {artifact.Sha256}"
                             + $" --nbn-size {artifact.Size}"
                             + $" --artifact-root \"{artifactPath}\"";

        SampleBrainStatus = "Starting sample region host...";
        var regionArgs = $"{regionArgsBase} --port {sampleRegionListenPort} --region 1";
        var regionStartInfo = BuildServiceStartInfo(regionProjectPath, "Nbn.Runtime.RegionHost", regionArgs);
        var regionResult = await _sampleRegionRunner.StartAsync(regionStartInfo, waitForExit: false, label: "SampleRegionHost").ConfigureAwait(false);
        if (!regionResult.Success)
        {
            SampleBrainStatus = $"Sample region host failed: {regionResult.Message}";
            await CleanupSampleBrainAsync().ConfigureAwait(false);
            return;
        }

        SampleBrainStatus = "Starting sample input region...";
        var inputArgs = $"{regionArgsBase} --port {sampleInputPort} --region 0";
        var inputStartInfo = BuildServiceStartInfo(regionProjectPath, "Nbn.Runtime.RegionHost", inputArgs);
        var inputResult = await _sampleInputRunner.StartAsync(inputStartInfo, waitForExit: false, label: "SampleRegionInput").ConfigureAwait(false);
        if (!inputResult.Success)
        {
            SampleBrainStatus = $"Sample input region failed: {inputResult.Message}";
            await CleanupSampleBrainAsync().ConfigureAwait(false);
            return;
        }

        SampleBrainStatus = "Starting sample output region...";
        var outputId = SampleOutputPrefix + brainId.ToString("N");
        var outputArgs = $"{regionArgsBase} --port {sampleOutputPort} --region 31 --output-address {ioAddress} --output-id {outputId}";
        var outputStartInfo = BuildServiceStartInfo(regionProjectPath, "Nbn.Runtime.RegionHost", outputArgs);
        var outputResult = await _sampleOutputRunner.StartAsync(outputStartInfo, waitForExit: false, label: "SampleRegionOutput").ConfigureAwait(false);
        if (!outputResult.Success)
        {
            SampleBrainStatus = $"Sample output region failed: {outputResult.Message}";
            await CleanupSampleBrainAsync().ConfigureAwait(false);
            return;
        }

        SampleBrainStatus = "Waiting for sample brain registration...";
        if (!await WaitForBrainRegistrationAsync(brainId).ConfigureAwait(false))
        {
            SampleBrainStatus = $"Sample brain failed to register ({brainId:D}).";
            await CleanupSampleBrainAsync().ConfigureAwait(false);
            return;
        }

        _brainDiscovered?.Invoke(brainId);
        SampleBrainStatus = $"Sample brain running ({brainId:D}) on ports {sampleBrainListenPort}/{sampleRegionListenPort}/{sampleInputPort}/{sampleOutputPort}.";
        await Task.Delay(300).ConfigureAwait(false);
        await RefreshAsync(force: true).ConfigureAwait(false);
    }

    private async Task StopSampleBrainAsync()
    {
        if (!IsSampleBrainRunning())
        {
            var stoppedId = _sampleBrainId;
            _sampleBrainId = null;
            SampleBrainStatus = stoppedId.HasValue
                ? $"Sample brain not running ({stoppedId:D})."
                : "Sample brain not running.";
            return;
        }

        var brainId = _sampleBrainId;
        await CleanupSampleBrainAsync().ConfigureAwait(false);
        SampleBrainStatus = brainId.HasValue
            ? $"Sample brain stopped ({brainId:D})."
            : "Sample brain stopped.";
    }

    private async Task CleanupSampleBrainAsync()
    {
        await _sampleOutputRunner.StopAsync().ConfigureAwait(false);
        await _sampleInputRunner.StopAsync().ConfigureAwait(false);
        await _sampleRegionRunner.StopAsync().ConfigureAwait(false);
        await _sampleBrainRunner.StopAsync().ConfigureAwait(false);
        _sampleBrainId = null;
    }

    private bool IsSampleBrainRunning()
        => _sampleBrainRunner.IsRunning
           || _sampleRegionRunner.IsRunning
           || _sampleInputRunner.IsRunning
           || _sampleOutputRunner.IsRunning;

    private async Task<bool> WaitForBrainRegistrationAsync(Guid brainId)
    {
        var deadline = DateTime.UtcNow + SpawnRegistrationTimeout;
        while (DateTime.UtcNow <= deadline)
        {
            var response = await _client.ListBrainsAsync().ConfigureAwait(false);
            if (IsBrainRegistered(response, brainId))
            {
                return true;
            }

            await Task.Delay(SpawnRegistrationPollInterval).ConfigureAwait(false);
        }

        return false;
    }

    private static bool IsBrainRegistered(Nbn.Proto.Settings.BrainListResponse? response, Guid brainId)
    {
        if (response?.Brains is null)
        {
            return false;
        }

        foreach (var entry in response.Brains)
        {
            if (entry.BrainId is null || !entry.BrainId.TryToGuid(out var candidate) || candidate != brainId)
            {
                continue;
            }

            return !string.Equals(entry.State, "Dead", StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    private async Task StartAutoRefreshAsync()
    {
        while (!_refreshCts.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_autoRefreshInterval, _refreshCts.Token).ConfigureAwait(false);
                await RefreshAsync(force: false).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private async Task ApplySettingsAsync()
    {
        if (!Connections.SettingsConnected)
        {
            StatusMessage = "SettingsMonitor not connected.";
            return;
        }

        var dirty = Settings.Where(entry => entry.IsDirty).ToList();
        if (dirty.Count == 0)
        {
            StatusMessage = "No settings changes.";
            return;
        }

        StatusMessage = $"Applying {dirty.Count} setting(s)...";
        foreach (var entry in dirty)
        {
            var result = await _client.SetSettingAsync(entry.Key, entry.Value).ConfigureAwait(false);
            if (result is null)
            {
                continue;
            }

            _dispatcher.Post(() =>
            {
                entry.MarkApplied(result.Value ?? entry.Value, FormatUpdated(result.UpdatedMs));
            });
        }

        StatusMessage = "Settings updated.";
    }

    private static void Trim<T>(ObservableCollection<T> collection)
    {
        while (collection.Count > MaxRows)
        {
            collection.RemoveAt(collection.Count - 1);
        }
    }

    private static string FormatUpdated(long updatedMs)
    {
        if (updatedMs <= 0)
        {
            return string.Empty;
        }

        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(updatedMs).ToLocalTime();
        return timestamp.ToString("g");
    }

    private static string FormatUpdated(ulong updatedMs)
        => FormatUpdated((long)updatedMs);

    private static string FormatUpdated(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        if (long.TryParse(value, out var parsed))
        {
            return FormatUpdated(parsed);
        }

        return value;
    }

    private void UpdateHiveMindEndpoint(IEnumerable<Nbn.Proto.Settings.NodeStatus> nodes)
    {
        var match = nodes.FirstOrDefault(node =>
            string.Equals(node.RootActorName, Connections.HiveMindName, StringComparison.OrdinalIgnoreCase));

        if (match is null)
        {
            return;
        }

        if (!TryParseHostPort(match.Address, out var host, out var port))
        {
            return;
        }

        Connections.HiveMindHost = host;
        Connections.HiveMindPortText = port.ToString();
    }

    private void UpdateHiveMindEndpoint(IEnumerable<Nbn.Proto.Settings.NodeStatus> nodes, long nowMs)
    {
        var match = nodes
            .Where(node => string.Equals(node.RootActorName, Connections.HiveMindName, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(node => IsFresh(node.LastSeenMs, nowMs))
            .ThenByDescending(node => node.LastSeenMs)
            .FirstOrDefault();

        if (match is null)
        {
            return;
        }

        if (!TryParseHostPort(match.Address, out var host, out var port))
        {
            return;
        }

        Connections.HiveMindHost = host;
        Connections.HiveMindPortText = port.ToString();
    }

    private static bool TryParseHostPort(string? address, out string host, out int port)
    {
        host = string.Empty;
        port = 0;

        if (string.IsNullOrWhiteSpace(address))
        {
            return false;
        }

        var trimmed = address.Trim();
        var colonIndex = trimmed.LastIndexOf(':');
        if (colonIndex <= 0 || colonIndex >= trimmed.Length - 1)
        {
            return false;
        }

        var hostPart = trimmed[..colonIndex];
        var portPart = trimmed[(colonIndex + 1)..];

        if (hostPart.StartsWith("[", StringComparison.Ordinal) && hostPart.EndsWith("]", StringComparison.Ordinal))
        {
            hostPart = hostPart[1..^1];
        }

        if (!int.TryParse(portPart, out port))
        {
            return false;
        }

        host = hostPart;
        return !string.IsNullOrWhiteSpace(host);
    }

    private static bool TryParsePort(string value, out int port)
        => int.TryParse(value, out port) && port > 0 && port < 65536;

    private static bool IsFresh(ulong lastSeenMs, long nowMs)
    {
        if (lastSeenMs == 0)
        {
            return false;
        }

        var delta = nowMs - (long)lastSeenMs;
        if (delta < 0)
        {
            return false;
        }

        return delta <= StaleNodeMs;
    }

    private static bool IsSpawnRecent(ulong spawnedMs, long nowMs)
    {
        if (spawnedMs == 0)
        {
            return false;
        }

        var delta = nowMs - (long)spawnedMs;
        if (delta < 0)
        {
            return false;
        }

        return delta <= SpawnVisibilityGraceMs;
    }

    private void UpdateConnectionStatusesFromNodes(IReadOnlyList<Nbn.Proto.Settings.NodeStatus> nodes, long nowMs)
    {
        var hiveAlive = false;
        var ioAlive = false;
        var reproAlive = false;
        var obsAlive = false;
        foreach (var node in nodes)
        {
            if (string.IsNullOrWhiteSpace(node.RootActorName))
            {
                continue;
            }

            var fresh = node.IsAlive && IsFresh(node.LastSeenMs, nowMs);
            if (string.Equals(node.RootActorName, Connections.HiveMindName, StringComparison.OrdinalIgnoreCase))
            {
                hiveAlive = hiveAlive || fresh;
            }

            if (string.Equals(node.RootActorName, Connections.IoGateway, StringComparison.OrdinalIgnoreCase))
            {
                ioAlive = ioAlive || fresh;
            }

            if (string.Equals(node.RootActorName, Connections.ReproManager, StringComparison.OrdinalIgnoreCase))
            {
                reproAlive = reproAlive || fresh;
            }

            if (string.Equals(node.RootActorName, Connections.DebugHub, StringComparison.OrdinalIgnoreCase)
                || string.Equals(node.RootActorName, Connections.VizHub, StringComparison.OrdinalIgnoreCase))
            {
                obsAlive = obsAlive || fresh;
            }
        }

        _dispatcher.Post(() =>
        {
            Connections.HiveMindConnected = hiveAlive;
            Connections.HiveMindStatus = hiveAlive ? "Connected" : "Offline";

            Connections.IoConnected = ioAlive;
            Connections.IoStatus = ioAlive ? "Connected" : "Offline";

            Connections.ReproConnected = reproAlive;
            Connections.ReproStatus = reproAlive ? "Connected" : "Offline";

            Connections.ObsConnected = obsAlive;
            Connections.ObsStatus = obsAlive ? "Connected" : "Offline";
        });
    }

    private void RecordBrainTerminations(IReadOnlyList<BrainListItem> current)
    {
        var seen = new HashSet<Guid>();
        foreach (var brain in current)
        {
            seen.Add(brain.BrainId);
            if (_lastBrains.TryGetValue(brain.BrainId, out var previous))
            {
                if (previous.ControllerAlive && !brain.ControllerAlive)
                {
                    AddTermination(new BrainTerminatedItem(
                        DateTimeOffset.UtcNow,
                        brain.BrainId.ToString("D"),
                        "Controller offline",
                        0,
                        0));
                }
                else if (!string.Equals(previous.State, "Dead", StringComparison.OrdinalIgnoreCase)
                         && string.Equals(brain.State, "Dead", StringComparison.OrdinalIgnoreCase))
                {
                    AddTermination(new BrainTerminatedItem(
                        DateTimeOffset.UtcNow,
                        brain.BrainId.ToString("D"),
                        "State dead",
                        0,
                        0));
                }
            }

            _lastBrains[brain.BrainId] = brain;
        }

        var missing = _lastBrains.Keys.Where(id => !seen.Contains(id)).ToList();
        foreach (var brainId in missing)
        {
            if (!_lastBrains.TryGetValue(brainId, out var previous))
            {
                continue;
            }

            AddTermination(new BrainTerminatedItem(
                DateTimeOffset.UtcNow,
                brainId.ToString("D"),
                "Missing from registry",
                0,
                0));
            _lastBrains.Remove(brainId);
        }
    }

    private static ProcessStartInfo BuildDotnetStartInfo(string args)
    {
        return new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = args,
            UseShellExecute = false,
            CreateNoWindow = true
        };
    }

    private static ProcessStartInfo BuildServiceStartInfo(string projectPath, string exeName, string serviceArgs)
    {
        var exePath = ResolveExecutable(projectPath, exeName);
        if (!string.IsNullOrWhiteSpace(exePath) && File.Exists(exePath))
        {
            return new ProcessStartInfo
            {
                FileName = exePath,
                Arguments = serviceArgs,
                UseShellExecute = false,
                CreateNoWindow = true
            };
        }

        var dotnetArgs = $"run --project \"{projectPath}\" -c Release --no-build -- {serviceArgs}";
        return BuildDotnetStartInfo(dotnetArgs);
    }

    private static string? ResolveExecutable(string projectPath, string exeName)
    {
        if (string.IsNullOrWhiteSpace(projectPath))
        {
            return null;
        }

        var output = Path.Combine(projectPath, "bin", "Release", "net8.0");
        if (OperatingSystem.IsWindows())
        {
            return Path.Combine(output, exeName + ".exe");
        }

        return Path.Combine(output, exeName);
    }

    private async Task TriggerReconnectAsync()
    {
        if (_connectAll is null)
        {
            return;
        }

        await Task.Delay(500).ConfigureAwait(false);
        await _connectAll().ConfigureAwait(false);
    }

    private static string BuildSampleRunRoot()
    {
        var baseDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "Nbn.Workbench",
            "sample-brain");
        Directory.CreateDirectory(baseDir);
        return baseDir;
    }

    private static void ResetDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
        catch
        {
        }

        Directory.CreateDirectory(path);
    }

    private async Task<SampleArtifact?> CreateSampleArtifactsAsync(string artifactRoot)
    {
        try
        {
            Directory.CreateDirectory(artifactRoot);
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var nbnBytes = DemoNbnBuilder.BuildSampleNbn();
            var manifest = await store.StoreAsync(new MemoryStream(nbnBytes), "application/x-nbn").ConfigureAwait(false);
            return new SampleArtifact(manifest.ArtifactId.ToHex(), manifest.ByteLength, artifactRoot);
        }
        catch (Exception ex)
        {
            SampleBrainStatus = $"Sample artifact failed: {ex.Message}";
            return null;
        }
    }

    private sealed record SampleArtifact(string Sha256, long Size, string ArtifactRoot);
}
