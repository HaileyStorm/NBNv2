using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class OrchestratorPanelViewModel : ViewModelBase
{
    private const int MaxRows = 200;
    private const long StaleNodeMs = 15000;
    private readonly UiDispatcher _dispatcher;
    private readonly ConnectionViewModel _connections;
    private readonly WorkbenchClient _client;
    private readonly LocalServiceRunner _settingsRunner = new();
    private readonly LocalServiceRunner _hiveMindRunner = new();
    private readonly LocalServiceRunner _ioRunner = new();
    private readonly LocalServiceRunner _obsRunner = new();
    private readonly Action<Guid>? _brainDiscovered;
    private readonly Action<IReadOnlyList<BrainListItem>>? _brainsUpdated;
    private readonly Func<Task>? _connectAll;
    private string _statusMessage = "Idle";
    private string _settingsLaunchStatus = "Idle";
    private string _hiveMindLaunchStatus = "Idle";
    private string _ioLaunchStatus = "Idle";
    private string _obsLaunchStatus = "Idle";
    private readonly Dictionary<Guid, BrainListItem> _lastBrains = new();
    private readonly CancellationTokenSource _refreshCts = new();
    private readonly TimeSpan _autoRefreshInterval = TimeSpan.FromSeconds(3);

    public OrchestratorPanelViewModel(
        UiDispatcher dispatcher,
        ConnectionViewModel connections,
        WorkbenchClient client,
        Action<Guid>? brainDiscovered = null,
        Action<IReadOnlyList<BrainListItem>>? brainsUpdated = null,
        Func<Task>? connectAll = null)
    {
        _dispatcher = dispatcher;
        _connections = connections;
        _client = client;
        _brainDiscovered = brainDiscovered;
        _brainsUpdated = brainsUpdated;
        _connectAll = connectAll;
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
        StartObsCommand = new AsyncRelayCommand(StartObsAsync);
        StopObsCommand = new AsyncRelayCommand(() => StopRunnerAsync(_obsRunner, value => ObsLaunchStatus = value));
        StartAllCommand = new AsyncRelayCommand(StartAllAsync);
        StopAllCommand = new AsyncRelayCommand(StopAllAsync);
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

    public AsyncRelayCommand StartObsCommand { get; }

    public AsyncRelayCommand StopObsCommand { get; }
    public AsyncRelayCommand StartAllCommand { get; }

    public AsyncRelayCommand StopAllCommand { get; }

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

    public string ObsLaunchStatus
    {
        get => _obsLaunchStatus;
        set => SetProperty(ref _obsLaunchStatus, value);
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
        await StopRunnerAsync(_settingsRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_hiveMindRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_ioRunner, _ => { }).ConfigureAwait(false);
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
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}";
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

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.IO");
        if (string.IsNullOrWhiteSpace(projectPath))
        {
            IoLaunchStatus = "Repo root not found.";
            return;
        }

        var hiveAddress = $"{Connections.HiveMindHost}:{hivePort}";
        var args = $"--bind-host {Connections.IoHost} --port {port}"
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}"
                 + $" --hivemind-address {hiveAddress} --hivemind-name {Connections.HiveMindName}";
        var startInfo = BuildServiceStartInfo(projectPath, "Nbn.Runtime.IO", args);
        var result = await _ioRunner.StartAsync(startInfo, waitForExit: false, label: "IoGateway");
        IoLaunchStatus = result.Message;
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
        if (!Connections.SettingsConnected)
        {
            if (force)
            {
                StatusMessage = "SettingsMonitor not connected.";
                Connections.SettingsStatus = "Disconnected";
            }
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
                foreach (var node in nodes)
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

            var nodeAliveMap = new Dictionary<Guid, bool>();
            foreach (var node in nodes)
            {
                if (node.NodeId is null || !node.NodeId.TryToGuid(out var nodeId))
                {
                    continue;
                }

                nodeAliveMap[nodeId] = node.IsAlive && IsFresh(node.LastSeenMs, nowMs);
            }

            var controllerMap = controllers
                .Where(entry => entry.BrainId is not null && entry.BrainId.TryToGuid(out _))
                .ToDictionary(
                    entry => entry.BrainId!.ToGuid(),
                    entry =>
                    {
                        var controllerAlive = entry.IsAlive && IsFresh(entry.LastSeenMs, nowMs);
                        var nodeAlive = entry.NodeId is not null
                            && entry.NodeId.TryToGuid(out var nodeId)
                            && nodeAliveMap.TryGetValue(nodeId, out var alive)
                            && alive;
                        return (entry, controllerAlive && nodeAlive);
                    });

            var brainListAll = brains.Select(entry =>
            {
                var brainId = entry.BrainId?.ToGuid() ?? Guid.Empty;
                var alive = controllerMap.TryGetValue(brainId, out var controller) && controller.Item2;
                return new BrainListItem(brainId, entry.State ?? string.Empty, alive);
            }).Where(entry => entry.BrainId != Guid.Empty).ToList();

            RecordBrainTerminations(brainListAll);
            var brainList = brainListAll.Where(entry => entry.ControllerAlive).ToList();
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
    }

    private async Task StartAllAsync()
    {
        await StartSettingsMonitorAsync().ConfigureAwait(false);
        await StartHiveMindAsync().ConfigureAwait(false);
        await StartIoAsync().ConfigureAwait(false);
        await StartObsAsync().ConfigureAwait(false);
    }

    private async Task StopAllAsync()
    {
        await StopRunnerAsync(_obsRunner, value => ObsLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_ioRunner, value => IoLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_hiveMindRunner, value => HiveMindLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_settingsRunner, value => SettingsLaunchStatus = value).ConfigureAwait(false);
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

    private void UpdateConnectionStatusesFromNodes(IReadOnlyList<Nbn.Proto.Settings.NodeStatus> nodes, long nowMs)
    {
        var hiveAlive = false;
        var ioAlive = false;
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

            if (string.Equals(node.RootActorName, Connections.DebugHub, StringComparison.OrdinalIgnoreCase)
                || string.Equals(node.RootActorName, Connections.VizHub, StringComparison.OrdinalIgnoreCase))
            {
                obsAlive = obsAlive || fresh;
            }
        }

        _dispatcher.Post(() =>
        {
            Connections.HiveMindConnected = hiveAlive;
            if (!hiveAlive)
            {
                Connections.HiveMindStatus = "Offline";
            }

            Connections.IoConnected = ioAlive;
            if (!ioAlive)
            {
                Connections.IoStatus = "Offline";
            }

            Connections.ObsConnected = obsAlive;
            if (!obsAlive)
            {
                Connections.ObsStatus = "Offline";
            }
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
            var previous = _lastBrains[brainId];
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
}
