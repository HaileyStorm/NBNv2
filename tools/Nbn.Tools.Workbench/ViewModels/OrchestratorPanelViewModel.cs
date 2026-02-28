using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Nbn.Proto.Control;
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
    private const long WorkerFailedAfterMs = 45000;
    private const long WorkerRemoveAfterMs = 120000;
    private const long SpawnVisibilityGraceMs = 30000;
    private const float LocalDefaultTickHz = 8f;
    private const float LocalDefaultMinTickHz = 2f;
    private static readonly TimeSpan SpawnRegistrationTimeout = TimeSpan.FromSeconds(12);
    private static readonly TimeSpan SpawnRegistrationPollInterval = TimeSpan.FromMilliseconds(300);
    private static readonly bool EnableRuntimeDiagnostics = IsEnvTrue("NBN_WORKBENCH_RUNTIME_DIAGNOSTICS_ENABLED");
    private static readonly string ActivityDiagnosticsPeriod =
        ResolveEnvOrDefault("NBN_WORKBENCH_ACTIVITY_DIAGNOSTICS_PERIOD", "64");
    private readonly UiDispatcher _dispatcher;
    private readonly ConnectionViewModel _connections;
    private readonly WorkbenchClient _client;
    private readonly LocalServiceRunner _settingsRunner = new();
    private readonly LocalServiceRunner _hiveMindRunner = new();
    private readonly LocalServiceRunner _ioRunner = new();
    private readonly LocalServiceRunner _reproRunner = new();
    private readonly LocalServiceRunner _workerRunner = new();
    private readonly LocalServiceRunner _obsRunner = new();
    private readonly Action<Guid>? _brainDiscovered;
    private readonly Action<IReadOnlyList<BrainListItem>>? _brainsUpdated;
    private readonly Func<Task>? _connectAll;
    private readonly Action? _disconnectAll;
    private readonly Dictionary<Guid, WorkerEndpointSnapshot> _workerEndpointCache = new();
    private readonly SemaphoreSlim _refreshGate = new(1, 1);
    private string _statusMessage = "Idle";
    private string _settingsLaunchStatus = "Idle";
    private string _hiveMindLaunchStatus = "Idle";
    private string _ioLaunchStatus = "Idle";
    private string _reproLaunchStatus = "Idle";
    private string _workerLaunchStatus = "Idle";
    private string _obsLaunchStatus = "Idle";
    private string _sampleBrainStatus = "Not running.";
    private string _workerEndpointSummary = "No active workers.";
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
        WorkerEndpoints = new ObservableCollection<WorkerEndpointItem>();
        Actors = new ObservableCollection<NodeStatusItem>();
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
        StartWorkerCommand = new AsyncRelayCommand(StartWorkerAsync);
        StopWorkerCommand = new AsyncRelayCommand(() => StopRunnerAsync(_workerRunner, value => WorkerLaunchStatus = value));
        StartObsCommand = new AsyncRelayCommand(StartObsAsync);
        StopObsCommand = new AsyncRelayCommand(() => StopRunnerAsync(_obsRunner, value => ObsLaunchStatus = value));
        StartAllCommand = new AsyncRelayCommand(StartAllAsync);
        StopAllCommand = new AsyncRelayCommand(StopAllAsync);
        SpawnSampleBrainCommand = new AsyncRelayCommand(SpawnSampleBrainAsync);
        StopSampleBrainCommand = new AsyncRelayCommand(StopSampleBrainAsync);
        _ = StartAutoRefreshAsync();
    }

    public ObservableCollection<NodeStatusItem> Nodes { get; }
    public ObservableCollection<WorkerEndpointItem> WorkerEndpoints { get; }
    public ObservableCollection<NodeStatusItem> Actors { get; }
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

    public AsyncRelayCommand StartWorkerCommand { get; }

    public AsyncRelayCommand StopWorkerCommand { get; }

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

    public string WorkerLaunchStatus
    {
        get => _workerLaunchStatus;
        set => SetProperty(ref _workerLaunchStatus, value);
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

    public string WorkerEndpointSummary
    {
        get => _workerEndpointSummary;
        set => SetProperty(ref _workerEndpointSummary, value);
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
        await StopRunnerAsync(_workerRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_settingsRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_hiveMindRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_ioRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_reproRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_obsRunner, _ => { }).ConfigureAwait(false);
    }

    private async Task StartSettingsMonitorAsync()
    {
        var configuredDbPath = Connections.SettingsDbPath?.Trim();
        var defaultDbPath = BuildDefaultSettingsDbPath();
        var resolvedDbPath = string.IsNullOrWhiteSpace(configuredDbPath) ? defaultDbPath : configuredDbPath;
        var includeDbArg = !string.IsNullOrWhiteSpace(configuredDbPath)
            && !PathsEqual(configuredDbPath, defaultDbPath);

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

        var args = includeDbArg
            ? $"--db \"{resolvedDbPath}\" --bind-host {Connections.SettingsHost} --port {port}"
            : $"--bind-host {Connections.SettingsHost} --port {port}";
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

        if (!TryParsePort(Connections.IoPortText, out var ioPort))
        {
            HiveMindLaunchStatus = "Invalid IO port.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.HiveMind");
        if (string.IsNullOrWhiteSpace(projectPath))
        {
            HiveMindLaunchStatus = "Repo root not found.";
            return;
        }

        var ioAddress = $"{Connections.IoHost}:{ioPort}";
        var settingsDbPath = ResolveSettingsDbPath();
        var args = $"--bind-host {Connections.HiveMindHost} --port {port} --settings-db \"{settingsDbPath}\""
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}"
                 + $" --io-address {ioAddress} --io-name {Connections.IoGateway}"
                 + $" --tick-hz {LocalDefaultTickHz:0.###} --min-tick-hz {LocalDefaultMinTickHz:0.###}";
        var startInfo = BuildServiceStartInfo(projectPath, "Nbn.Runtime.HiveMind", args);
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
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
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
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
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
        var result = await _reproRunner.StartAsync(startInfo, waitForExit: false, label: "Reproduction");
        ReproLaunchStatus = result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartWorkerAsync()
    {
        if (!TryParsePort(Connections.WorkerPortText, out var workerPort))
        {
            WorkerLaunchStatus = "Invalid worker port.";
            return;
        }

        if (!TryParsePort(Connections.SettingsPortText, out var settingsPort))
        {
            WorkerLaunchStatus = "Invalid Settings port.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.WorkerNode");
        if (string.IsNullOrWhiteSpace(projectPath))
        {
            WorkerLaunchStatus = "WorkerNode project not found.";
            return;
        }

        var args = $"--bind-host {Connections.WorkerHost} --port {workerPort}"
                 + $" --logical-name {Connections.WorkerLogicalName}"
                 + $" --root-name {Connections.WorkerRootName}"
                 + $" --settings-host {Connections.SettingsHost} --settings-port {settingsPort} --settings-name {Connections.SettingsName}";
        var startInfo = BuildServiceStartInfo(projectPath, "Nbn.Runtime.WorkerNode", args);
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
        ApplyObservabilityEnvironment(startInfo);
        var result = await _workerRunner.StartAsync(startInfo, waitForExit: false, label: "WorkerNode");
        WorkerLaunchStatus = result.Message;
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
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
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
            var nodesResponseTask = _client.ListNodesAsync();
            var brainsResponseTask = _client.ListBrainsAsync();
            var workerInventoryResponseTask = _client.ListWorkerInventorySnapshotAsync();
            var settingsResponseTask = _client.ListSettingsAsync();

            var nodesResponse = await nodesResponseTask.ConfigureAwait(false);
            var brainsResponse = await brainsResponseTask.ConfigureAwait(false);
            var workerInventoryResponse = await workerInventoryResponseTask.ConfigureAwait(false);
            var settingsResponse = await settingsResponseTask.ConfigureAwait(false);

            var nodes = nodesResponse?.Nodes?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.NodeStatus>();
            var brains = brainsResponse?.Brains?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.BrainStatus>();
            var controllers = brainsResponse?.Controllers?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.BrainControllerStatus>();
            var workerInventory = workerInventoryResponse?.Workers?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.WorkerReadinessCapability>();
            var sortedNodes = nodes
                .OrderByDescending(node => node.LastSeenMs)
                .ThenBy(node => node.LogicalName ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                .ToArray();

            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            UpdateHiveMindEndpoint(nodes, nowMs);
            var actorRowsResult = await BuildActorRowsAsync(controllers, sortedNodes, brains, nowMs).ConfigureAwait(false);
            var workerEndpointState = BuildWorkerEndpointState(
                sortedNodes,
                workerInventory,
                actorRowsResult.WorkerBrainHints,
                nowMs);
            var settings = settingsResponse?.Settings?
                .Select(entry => new SettingItem(
                    entry.Key ?? string.Empty,
                    entry.Value ?? string.Empty,
                    FormatUpdated(entry.UpdatedMs)))
                .ToList() ?? new List<SettingItem>();

            _dispatcher.Post(() =>
            {
                Nodes.Clear();
                WorkerEndpoints.Clear();
                Actors.Clear();
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

                foreach (var workerRow in workerEndpointState.Rows)
                {
                    WorkerEndpoints.Add(workerRow);
                }

                foreach (var actorRow in actorRowsResult.Rows)
                {
                    Actors.Add(actorRow);
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

                WorkerEndpointSummary = workerEndpointState.SummaryText;
                Trim(Nodes);
                Trim(WorkerEndpoints);
                Trim(Actors);
                Trim(Settings);
            });

            UpdateConnectionStatusesFromNodes(nodes, nowMs, workerEndpointState);

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
        await StartWorkerAsync().ConfigureAwait(false);
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
        await StopRunnerAsync(_workerRunner, value => WorkerLaunchStatus = value).ConfigureAwait(false);
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

        var runRoot = BuildSampleRunRoot();
        var artifactRoot = Path.Combine(runRoot, "artifacts");
        ResetDirectory(artifactRoot);

        SampleBrainStatus = "Creating sample artifacts...";
        var artifact = await CreateSampleArtifactsAsync(artifactRoot).ConfigureAwait(false);
        if (artifact is null)
        {
            return;
        }

        var artifactPath = string.IsNullOrWhiteSpace(artifact.ArtifactRoot) ? artifactRoot : artifact.ArtifactRoot;
        SampleBrainStatus = "Spawning sample brain via IO/HiveMind worker placement...";
        var spawnAck = await _client.SpawnBrainViaIoAsync(new Nbn.Proto.Control.SpawnBrain
        {
            BrainDef = artifact.Sha256.ToArtifactRef((ulong)Math.Max(0L, artifact.Size), "application/x-nbn", artifactPath)
        }).ConfigureAwait(false);
        if (spawnAck?.BrainId is null || !spawnAck.BrainId.TryToGuid(out var brainId) || brainId == Guid.Empty)
        {
            SampleBrainStatus = SpawnFailureFormatter.Format(
                prefix: "Sample spawn failed",
                ack: spawnAck,
                fallbackMessage: "Sample spawn failed: IO did not return a brain id.");
            return;
        }

        _sampleBrainId = brainId;

        SampleBrainStatus = "Waiting for sample brain placement/runtime readiness...";
        if (!await WaitForBrainRegistrationAsync(brainId).ConfigureAwait(false))
        {
            SampleBrainStatus = $"Sample brain failed to become visualization-ready ({brainId:D}) after IO/HiveMind worker placement.";
            await _client.KillBrainAsync(brainId, "workbench_sample_registration_timeout").ConfigureAwait(false);
            _sampleBrainId = null;
            return;
        }

        _brainDiscovered?.Invoke(brainId);
        await RefreshAsync(force: true).ConfigureAwait(false);
        SampleBrainStatus = $"Sample brain running ({brainId:D}). Spawned via IO; worker placement managed by HiveMind.";
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
        var killSent = brainId.HasValue
            && await _client.KillBrainAsync(brainId.Value, "workbench_sample_stop").ConfigureAwait(false);
        _sampleBrainId = null;
        SampleBrainStatus = brainId.HasValue
            ? killSent
                ? $"Sample brain stop requested ({brainId:D})."
                : $"Sample brain stop request failed ({brainId:D})."
            : "Sample brain stop request failed.";
    }

    private bool IsSampleBrainRunning()
        => _sampleBrainId.HasValue;

    private async Task<bool> WaitForBrainRegistrationAsync(Guid brainId)
    {
        var deadline = DateTime.UtcNow + SpawnRegistrationTimeout;
        while (DateTime.UtcNow <= deadline)
        {
            var response = await _client.ListBrainsAsync().ConfigureAwait(false);
            var lifecycle = await _client.GetPlacementLifecycleAsync(brainId).ConfigureAwait(false);
            if (IsBrainRegistered(response, brainId) && IsPlacementVisualizationReady(lifecycle, brainId))
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

        var brainPresentAndActive = false;
        foreach (var entry in response.Brains)
        {
            if (entry.BrainId is null || !entry.BrainId.TryToGuid(out var candidate) || candidate != brainId)
            {
                continue;
            }

            if (string.Equals(entry.State, "Dead", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            brainPresentAndActive = true;
            break;
        }

        if (!brainPresentAndActive)
        {
            return false;
        }

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return HasLiveController(response.Controllers, brainId, nowMs);
    }

    private static bool IsPlacementVisualizationReady(PlacementLifecycleInfo? lifecycle, Guid brainId)
    {
        if (lifecycle?.BrainId is null
            || !lifecycle.BrainId.TryToGuid(out var candidate)
            || candidate != brainId)
        {
            return false;
        }

        return lifecycle.LifecycleState == PlacementLifecycleState.PlacementLifecycleRunning
               && lifecycle.RegisteredShards > 0;
    }

    private static bool HasLiveController(
        IEnumerable<Nbn.Proto.Settings.BrainControllerStatus>? controllers,
        Guid brainId,
        long nowMs)
    {
        if (controllers is null)
        {
            return false;
        }

        foreach (var controller in controllers)
        {
            if (controller.BrainId is null
                || !controller.BrainId.TryToGuid(out var candidate)
                || candidate != brainId)
            {
                continue;
            }

            if (controller.IsAlive && IsFresh(controller.LastSeenMs, nowMs))
            {
                return true;
            }
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

    private WorkerEndpointState BuildWorkerEndpointState(
        IReadOnlyList<Nbn.Proto.Settings.NodeStatus> nodes,
        IReadOnlyList<Nbn.Proto.Settings.WorkerReadinessCapability> inventory,
        IReadOnlyDictionary<Guid, HashSet<Guid>> workerBrainHints,
        long nowMs)
    {
        foreach (var worker in inventory)
        {
            if (worker.NodeId is null || !worker.NodeId.TryToGuid(out var nodeId))
            {
                continue;
            }

            if (!IsWorkerHostCandidate(worker.LogicalName, worker.RootActorName))
            {
                continue;
            }

            UpdateWorkerEndpointSnapshot(
                nodeId,
                worker.LogicalName,
                worker.Address,
                worker.RootActorName,
                (long)worker.LastSeenMs,
                worker.IsAlive);
        }

        foreach (var node in nodes)
        {
            if (node.NodeId is null || !node.NodeId.TryToGuid(out var nodeId))
            {
                continue;
            }

            if (!IsWorkerHostCandidate(node.LogicalName, node.RootActorName))
            {
                continue;
            }

            UpdateWorkerEndpointSnapshot(
                nodeId,
                node.LogicalName,
                node.Address,
                node.RootActorName,
                (long)node.LastSeenMs,
                node.IsAlive);
        }

        var rows = new List<(int Rank, long LastSeenMs, WorkerEndpointItem Row)>();
        var staleNodeIds = new List<Guid>();
        var activeCount = 0;
        var degradedCount = 0;
        var failedCount = 0;

        foreach (var entry in _workerEndpointCache.Values)
        {
            var (status, remove) = ClassifyWorkerEndpointStatus(entry, nowMs);
            if (remove)
            {
                staleNodeIds.Add(entry.NodeId);
                continue;
            }

            switch (status)
            {
                case "active":
                    activeCount++;
                    break;
                case "degraded":
                    degradedCount++;
                    break;
                default:
                    failedCount++;
                    break;
            }

            rows.Add((WorkerStatusRank(status), entry.LastSeenMs, new WorkerEndpointItem(
                entry.NodeId,
                entry.LogicalName,
                entry.Address,
                entry.RootActorName,
                FormatWorkerBrainHints(workerBrainHints, entry.NodeId),
                FormatUpdated(entry.LastSeenMs),
                status)));
        }

        foreach (var staleNodeId in staleNodeIds)
        {
            _workerEndpointCache.Remove(staleNodeId);
        }

        var orderedRows = rows
            .OrderBy(entry => entry.Rank)
            .ThenByDescending(entry => entry.LastSeenMs)
            .ThenBy(entry => entry.Row.LogicalName, StringComparer.OrdinalIgnoreCase)
            .Select(entry => entry.Row)
            .ToArray();

        var summary = BuildWorkerEndpointSummary(activeCount, degradedCount, failedCount);
        return new WorkerEndpointState(orderedRows, activeCount, degradedCount, failedCount, summary);
    }

    private void UpdateWorkerEndpointSnapshot(
        Guid nodeId,
        string? logicalName,
        string? address,
        string? rootActorName,
        long lastSeenMs,
        bool isAlive)
    {
        if (!_workerEndpointCache.TryGetValue(nodeId, out var snapshot))
        {
            snapshot = new WorkerEndpointSnapshot
            {
                NodeId = nodeId
            };
            _workerEndpointCache[nodeId] = snapshot;
        }

        if (!string.IsNullOrWhiteSpace(logicalName))
        {
            snapshot.LogicalName = logicalName.Trim();
        }

        if (!string.IsNullOrWhiteSpace(address))
        {
            snapshot.Address = address.Trim();
        }

        if (!string.IsNullOrWhiteSpace(rootActorName))
        {
            snapshot.RootActorName = rootActorName.Trim();
        }

        var previousLastSeen = snapshot.LastSeenMs;
        snapshot.LastSeenMs = Math.Max(snapshot.LastSeenMs, lastSeenMs);
        if (lastSeenMs >= previousLastSeen || !isAlive)
        {
            snapshot.IsAlive = isAlive;
        }
    }

    private static (string Status, bool Remove) ClassifyWorkerEndpointStatus(WorkerEndpointSnapshot snapshot, long nowMs)
    {
        if (snapshot.LastSeenMs <= 0)
        {
            return ("failed", false);
        }

        var ageMs = nowMs - snapshot.LastSeenMs;
        if (ageMs < 0)
        {
            ageMs = 0;
        }

        if (ageMs > WorkerRemoveAfterMs)
        {
            return ("failed", true);
        }

        if (snapshot.IsAlive && ageMs <= StaleNodeMs)
        {
            return ("active", false);
        }

        if (!snapshot.IsAlive || ageMs > WorkerFailedAfterMs)
        {
            return ("failed", false);
        }

        return ("degraded", false);
    }

    private static int WorkerStatusRank(string status)
    {
        return status switch
        {
            "active" => 0,
            "degraded" => 1,
            "failed" => 2,
            _ => 3
        };
    }

    private static string BuildWorkerEndpointSummary(int activeCount, int degradedCount, int failedCount)
    {
        var parts = new List<string>();
        if (activeCount > 0)
        {
            parts.Add(FormatCount(activeCount, "active"));
        }

        if (degradedCount > 0)
        {
            parts.Add(FormatCount(degradedCount, "degraded"));
        }

        if (failedCount > 0)
        {
            parts.Add(FormatCount(failedCount, "failed"));
        }

        return parts.Count == 0
            ? "No active workers."
            : string.Join(", ", parts);
    }

    private static string FormatCount(int count, string label)
    {
        var suffix = count == 1 ? "worker" : "workers";
        return $"{count} {label} {suffix}";
    }

    private static string FormatWorkerBrainHints(
        IReadOnlyDictionary<Guid, HashSet<Guid>> workerBrainHints,
        Guid nodeId)
    {
        if (!workerBrainHints.TryGetValue(nodeId, out var brainIds) || brainIds.Count == 0)
        {
            return "none";
        }

        var abbreviated = brainIds
            .Select(AbbreviateBrainId)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();
        return abbreviated.Length <= 2
            ? string.Join(", ", abbreviated)
            : $"{abbreviated[0]}, {abbreviated[1]}, ...";
    }

    private static string AbbreviateBrainId(Guid brainId)
    {
        var compact = brainId.ToString("N");
        return compact.Length <= 4 ? compact : compact[^4..];
    }

    private void UpdateConnectionStatusesFromNodes(
        IReadOnlyList<Nbn.Proto.Settings.NodeStatus> nodes,
        long nowMs,
        WorkerEndpointState workerEndpointState)
    {
        var hiveAlive = false;
        var ioAlive = false;
        var reproAlive = false;
        var obsAlive = false;
        foreach (var node in nodes)
        {
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

            Connections.WorkerConnected = workerEndpointState.ActiveCount > 0;
            Connections.WorkerStatus = workerEndpointState.Rows.Count > 0
                ? workerEndpointState.SummaryText
                : "Offline";

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

    private static string BuildDefaultSettingsDbPath()
    {
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(localAppData))
        {
            return "settingsmonitor.db";
        }

        var baseDir = Path.Combine(localAppData, "Nbn.Workbench");
        Directory.CreateDirectory(baseDir);
        return Path.Combine(baseDir, "settingsmonitor.db");
    }

    private string ResolveSettingsDbPath()
    {
        var configured = Connections.SettingsDbPath?.Trim();
        return string.IsNullOrWhiteSpace(configured) ? BuildDefaultSettingsDbPath() : configured;
    }

    private static bool PathsEqual(string left, string right)
    {
        try
        {
            var leftFull = Path.GetFullPath(left);
            var rightFull = Path.GetFullPath(right);
            var comparison = OperatingSystem.IsWindows()
                ? StringComparison.OrdinalIgnoreCase
                : StringComparison.Ordinal;
            return string.Equals(leftFull, rightFull, comparison);
        }
        catch
        {
            return false;
        }
    }
    private async Task<HostedActorRowsResult> BuildActorRowsAsync(
        IReadOnlyList<Nbn.Proto.Settings.BrainControllerStatus> controllers,
        IReadOnlyList<Nbn.Proto.Settings.NodeStatus> nodes,
        IReadOnlyList<Nbn.Proto.Settings.BrainStatus> brains,
        long nowMs)
    {
        var rows = new List<(bool IsOnlineWorkerHost, bool IsOnline, long LastSeenMs, NodeStatusItem Row)>();
        var workerBrainHints = new Dictionary<Guid, HashSet<Guid>>();
        var dedupe = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var nodeById = nodes
            .Where(entry => entry.NodeId is not null && entry.NodeId.TryToGuid(out _))
            .ToDictionary(entry => entry.NodeId!.ToGuid(), entry => entry);

        void AddActorRow(
            Guid brainId,
            string actorKind,
            string actorPid,
            string hostLabel,
            string address,
            long lastSeenMs,
            bool isOnline,
            bool hostIsWorker = false,
            uint regionId = 0,
            uint shardIndex = 0)
        {
            var dedupeKey = BuildHostedActorKey(brainId, actorPid, actorKind, regionId, shardIndex);
            if (!dedupe.Add(dedupeKey))
            {
                return;
            }

            var brainToken = brainId.ToString("N")[..8];
            var kindToken = actorKind;
            if (string.Equals(actorKind, "RegionShard", StringComparison.Ordinal))
            {
                kindToken = $"{actorKind} r{regionId} s{shardIndex}";
            }

            var seen = lastSeenMs > 0 ? FormatUpdated(lastSeenMs) : string.Empty;
            var logicalName = $"{hostLabel} - brain {brainToken} {kindToken}";
            var rootActor = string.IsNullOrWhiteSpace(actorPid) ? actorKind : actorPid;
            rows.Add((isOnline && hostIsWorker, isOnline, lastSeenMs, new NodeStatusItem(
                logicalName,
                address,
                rootActor,
                seen,
                isOnline ? "online" : "offline")));
        }

        void AddWorkerBrainHint(Guid nodeId, Guid brainId)
        {
            if (nodeId == Guid.Empty || brainId == Guid.Empty)
            {
                return;
            }

            if (!workerBrainHints.TryGetValue(nodeId, out var brainSet))
            {
                brainSet = new HashSet<Guid>();
                workerBrainHints[nodeId] = brainSet;
            }

            brainSet.Add(brainId);
        }

        foreach (var controller in controllers
                     .Where(entry => entry.BrainId is not null && entry.BrainId.TryToGuid(out _))
                     .OrderByDescending(entry => entry.LastSeenMs)
                     .ThenBy(entry => entry.ActorName ?? string.Empty, StringComparer.OrdinalIgnoreCase))
        {
            var brainId = controller.BrainId!.ToGuid();
            var hostLabel = "controller node";
            var address = string.Empty;
            var hostSeenMs = (long)controller.LastSeenMs;
            var hostIsWorker = false;
            var workerHostNodeId = Guid.Empty;
            if (controller.NodeId is not null
                && controller.NodeId.TryToGuid(out var nodeId)
                && nodeById.TryGetValue(nodeId, out var node))
            {
                hostLabel = string.IsNullOrWhiteSpace(node.LogicalName) ? "controller node" : node.LogicalName!;
                address = node.Address ?? string.Empty;
                hostSeenMs = (long)node.LastSeenMs;
                hostIsWorker = IsWorkerHostCandidate(node);
                if (hostIsWorker)
                {
                    workerHostNodeId = nodeId;
                }
            }

            var actorPid = controller.ActorName?.Trim() ?? string.Empty;
            var isOnline = controller.IsAlive && IsFresh(controller.LastSeenMs, nowMs);
            if (hostIsWorker && isOnline && workerHostNodeId != Guid.Empty)
            {
                AddWorkerBrainHint(workerHostNodeId, brainId);
            }

            AddActorRow(
                brainId,
                actorKind: "Controller",
                actorPid: actorPid,
                hostLabel: hostLabel,
                address: address,
                lastSeenMs: hostSeenMs,
                isOnline: isOnline,
                hostIsWorker: hostIsWorker);
        }

        var activeBrainIds = brains
            .Where(entry => entry.BrainId is not null && entry.BrainId.TryToGuid(out _))
            .Select(entry => entry.BrainId!.ToGuid())
            .Where(id => id != Guid.Empty)
            .Distinct()
            .ToArray();
        if (activeBrainIds.Length == 0)
        {
            return new HostedActorRowsResult(
                rows
                .OrderByDescending(entry => entry.IsOnlineWorkerHost)
                .ThenByDescending(entry => entry.IsOnline)
                .ThenByDescending(entry => entry.LastSeenMs)
                .ThenBy(entry => entry.Row.LogicalName, StringComparer.OrdinalIgnoreCase)
                .Select(entry => entry.Row)
                .ToArray(),
                workerBrainHints);
        }

        var lifecycleTasks = activeBrainIds.Select(async brainId =>
        {
            var lifecycle = await _client.GetPlacementLifecycleAsync(brainId).ConfigureAwait(false);
            return (BrainId: brainId, Lifecycle: lifecycle);
        });
        var lifecycles = await Task.WhenAll(lifecycleTasks).ConfigureAwait(false);
        var reconcileNodes = nodes
            .Where(entry =>
                entry.IsAlive
                && IsFresh(entry.LastSeenMs, nowMs)
                && !string.IsNullOrWhiteSpace(entry.Address)
                && !string.IsNullOrWhiteSpace(entry.RootActorName))
            .ToArray();

        var reconcileTasks = new List<Task<(Guid BrainId, Nbn.Proto.Settings.NodeStatus Node, PlacementReconcileReport? Report)>>();
        foreach (var lifecycleEntry in lifecycles)
        {
            var placementEpoch = lifecycleEntry.Lifecycle?.PlacementEpoch ?? 0;
            if (placementEpoch == 0)
            {
                continue;
            }

            foreach (var node in reconcileNodes)
            {
                reconcileTasks.Add(QueryPlacementReconcileAsync(
                    lifecycleEntry.BrainId,
                    node,
                    placementEpoch));
            }
        }

        var reconcileResults = reconcileTasks.Count == 0
            ? Array.Empty<(Guid BrainId, Nbn.Proto.Settings.NodeStatus Node, PlacementReconcileReport? Report)>()
            : await Task.WhenAll(reconcileTasks).ConfigureAwait(false);
        foreach (var reconcileResult in reconcileResults)
        {
            if (reconcileResult.Report?.Assignments is null
                || reconcileResult.Report.Assignments.Count == 0)
            {
                continue;
            }

            var report = reconcileResult.Report;
            var reportBrainId = report.BrainId is not null && report.BrainId.TryToGuid(out var parsedBrainId)
                ? parsedBrainId
                : reconcileResult.BrainId;

            foreach (var assignment in report.Assignments)
            {
                var actorKind = ToAssignmentTargetLabel(assignment.Target);
                var actorPid = assignment.ActorPid?.Trim() ?? string.Empty;
                var hostLabel = string.IsNullOrWhiteSpace(reconcileResult.Node.LogicalName)
                    ? "host node"
                    : reconcileResult.Node.LogicalName!;
                var address = reconcileResult.Node.Address ?? string.Empty;
                var hostSeenMs = (long)reconcileResult.Node.LastSeenMs;
                var isOnline = reconcileResult.Node.IsAlive && IsFresh(reconcileResult.Node.LastSeenMs, nowMs);
                var hostIsWorker = IsWorkerHostCandidate(reconcileResult.Node);
                var hostNodeId = reconcileResult.Node.NodeId is not null
                                 && reconcileResult.Node.NodeId.TryToGuid(out var reconcileNodeId)
                    ? reconcileNodeId
                    : Guid.Empty;

                if (assignment.WorkerNodeId is not null
                    && assignment.WorkerNodeId.TryToGuid(out var workerNodeId)
                    && nodeById.TryGetValue(workerNodeId, out var workerNode))
                {
                    hostLabel = string.IsNullOrWhiteSpace(workerNode.LogicalName) ? "host node" : workerNode.LogicalName!;
                    address = workerNode.Address ?? address;
                    hostSeenMs = (long)workerNode.LastSeenMs;
                    isOnline = workerNode.IsAlive && IsFresh(workerNode.LastSeenMs, nowMs);
                    hostIsWorker = IsWorkerHostCandidate(workerNode);
                    hostNodeId = workerNodeId;
                }

                if (hostIsWorker && isOnline && hostNodeId != Guid.Empty)
                {
                    AddWorkerBrainHint(hostNodeId, reportBrainId);
                }

                AddActorRow(
                    reportBrainId,
                    actorKind: actorKind,
                    actorPid: actorPid,
                    hostLabel: hostLabel,
                    address: address,
                    lastSeenMs: hostSeenMs,
                    isOnline: isOnline,
                    hostIsWorker: hostIsWorker,
                    regionId: assignment.RegionId,
                    shardIndex: assignment.ShardIndex);
            }
        }

        return new HostedActorRowsResult(
            rows
            .OrderByDescending(entry => entry.IsOnlineWorkerHost)
            .ThenByDescending(entry => entry.IsOnline)
            .ThenByDescending(entry => entry.LastSeenMs)
            .ThenBy(entry => entry.Row.LogicalName, StringComparer.OrdinalIgnoreCase)
            .Select(entry => entry.Row)
            .ToArray(),
            workerBrainHints);

        async Task<(Guid BrainId, Nbn.Proto.Settings.NodeStatus Node, PlacementReconcileReport? Report)> QueryPlacementReconcileAsync(
            Guid brainId,
            Nbn.Proto.Settings.NodeStatus node,
            ulong placementEpoch)
        {
            var report = await _client.RequestPlacementReconcileAsync(
                    node.Address ?? string.Empty,
                    node.RootActorName ?? string.Empty,
                    brainId,
                    placementEpoch)
                .ConfigureAwait(false);
            return (brainId, node, report);
        }
    }

    private static string BuildHostedActorKey(
        Guid brainId,
        string actorPid,
        string actorKind,
        uint regionId,
        uint shardIndex)
    {
        if (!string.IsNullOrWhiteSpace(actorPid))
        {
            return actorPid.Trim();
        }

        return $"{brainId:N}|{actorKind}|{regionId}|{shardIndex}";
    }

    private bool IsWorkerHostCandidate(Nbn.Proto.Settings.NodeStatus node)
    {
        return IsWorkerHostCandidate(node.LogicalName, node.RootActorName);
    }

    private bool IsWorkerHostCandidate(string? logicalName, string? rootActorName)
    {
        if (!string.IsNullOrWhiteSpace(Connections.WorkerLogicalName)
            && !string.IsNullOrWhiteSpace(logicalName)
            && string.Equals(logicalName.Trim(), Connections.WorkerLogicalName.Trim(), StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(Connections.WorkerRootName)
            && !string.IsNullOrWhiteSpace(rootActorName)
            && string.Equals(rootActorName.Trim(), Connections.WorkerRootName.Trim(), StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(logicalName)
            && logicalName.Trim().StartsWith("nbn.worker", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (string.IsNullOrWhiteSpace(rootActorName))
        {
            return false;
        }

        var root = rootActorName.Trim();
        return root.StartsWith("worker-node", StringComparison.OrdinalIgnoreCase)
               || root.Equals("regionhost", StringComparison.OrdinalIgnoreCase)
               || root.Equals("region-host", StringComparison.OrdinalIgnoreCase)
               || root.StartsWith("regionhost-", StringComparison.OrdinalIgnoreCase)
               || root.StartsWith("region-host-", StringComparison.OrdinalIgnoreCase);
    }

    private sealed record HostedActorRowsResult(
        IReadOnlyList<NodeStatusItem> Rows,
        IReadOnlyDictionary<Guid, HashSet<Guid>> WorkerBrainHints);

    private sealed record WorkerEndpointState(
        IReadOnlyList<WorkerEndpointItem> Rows,
        int ActiveCount,
        int DegradedCount,
        int FailedCount,
        string SummaryText);

    private sealed class WorkerEndpointSnapshot
    {
        public Guid NodeId { get; init; }
        public string LogicalName { get; set; } = string.Empty;
        public string Address { get; set; } = string.Empty;
        public string RootActorName { get; set; } = string.Empty;
        public long LastSeenMs { get; set; }
        public bool IsAlive { get; set; }
    }

    private void ApplyObservabilityEnvironment(ProcessStartInfo startInfo)
    {
        if (startInfo.UseShellExecute)
        {
            return;
        }

        var host = Connections.ObsHost?.Trim();
        if (!string.IsNullOrWhiteSpace(host)
            && TryParsePort(Connections.ObsPortText, out var obsPort))
        {
            startInfo.EnvironmentVariables["NBN_OBS_HOST"] = host;
            startInfo.EnvironmentVariables["NBN_OBS_PORT"] = obsPort.ToString();
            startInfo.EnvironmentVariables["NBN_OBS_ADDRESS"] = $"{host}:{obsPort}";
        }

        var debugHub = Connections.DebugHub?.Trim();
        if (!string.IsNullOrWhiteSpace(debugHub))
        {
            startInfo.EnvironmentVariables["NBN_OBS_DEBUG_HUB"] = debugHub;
        }

        var vizHub = Connections.VizHub?.Trim();
        if (!string.IsNullOrWhiteSpace(vizHub))
        {
            startInfo.EnvironmentVariables["NBN_OBS_VIZ_HUB"] = vizHub;
        }
    }

    private static void ApplyRuntimeDiagnosticsEnvironment(ProcessStartInfo startInfo)
    {
        if (!EnableRuntimeDiagnostics || startInfo.UseShellExecute)
        {
            return;
        }

        SetEnvIfMissing(startInfo, "NBN_RUNTIME_METADATA_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_METADATA_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_HIVEMIND_METADATA_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_IO_METADATA_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_REGIONSHARD_ACTIVITY_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_REGIONSHARD_INIT_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_REGIONSHARD_ACTIVITY_DIAGNOSTICS_PERIOD", ActivityDiagnosticsPeriod);
        SetEnvIfMissing(startInfo, "NBN_VIZ_DIAGNOSTICS_ENABLED", "1");
    }

    private static void SetEnvIfMissing(ProcessStartInfo startInfo, string key, string value)
    {
        if (!startInfo.EnvironmentVariables.ContainsKey(key))
        {
            startInfo.EnvironmentVariables[key] = value;
        }
    }

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return !string.IsNullOrWhiteSpace(value)
               && (string.Equals(value, "1", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "yes", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "on", StringComparison.OrdinalIgnoreCase));
    }

    private static string ResolveEnvOrDefault(string key, string fallback)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return string.IsNullOrWhiteSpace(value) ? fallback : value.Trim();
    }

    private static string ToAssignmentTargetLabel(PlacementAssignmentTarget target)
    {
        return target switch
        {
            PlacementAssignmentTarget.PlacementTargetBrainRoot => "BrainHost",
            PlacementAssignmentTarget.PlacementTargetSignalRouter => "SignalRouter",
            PlacementAssignmentTarget.PlacementTargetInputCoordinator => "InputCoordinator",
            PlacementAssignmentTarget.PlacementTargetOutputCoordinator => "OutputCoordinator",
            PlacementAssignmentTarget.PlacementTargetRegionShard => "RegionShard",
            _ => "HostedActor"
        };
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

