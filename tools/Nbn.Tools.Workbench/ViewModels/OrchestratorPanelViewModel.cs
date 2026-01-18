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
    private readonly UiDispatcher _dispatcher;
    private readonly ConnectionViewModel _connections;
    private readonly WorkbenchClient _client;
    private readonly LocalDemoRunner _demoRunner = new();
    private readonly LocalServiceRunner _settingsRunner = new();
    private readonly LocalServiceRunner _hiveMindRunner = new();
    private readonly LocalServiceRunner _ioRunner = new();
    private readonly LocalServiceRunner _obsRunner = new();
    private readonly Action<Guid>? _brainDiscovered;
    private readonly Action<IReadOnlyList<BrainListItem>>? _brainsUpdated;
    private readonly Func<Task>? _connectAll;
    private string _statusMessage = "Idle";
    private string _demoStatus = "Demo not running.";
    private string _settingsLaunchStatus = "Idle";
    private string _hiveMindLaunchStatus = "Idle";
    private string _ioLaunchStatus = "Idle";
    private string _obsLaunchStatus = "Idle";
    private bool _demoRunning;

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
        RefreshCommand = new AsyncRelayCommand(RefreshAsync);
        ApplySettingsCommand = new AsyncRelayCommand(ApplySettingsAsync);
        StartDemoCommand = new AsyncRelayCommand(StartDemoAsync);
        StopDemoCommand = new AsyncRelayCommand(StopDemoAsync);
        StartSettingsMonitorCommand = new AsyncRelayCommand(StartSettingsMonitorAsync);
        StopSettingsMonitorCommand = new AsyncRelayCommand(() => StopRunnerAsync(_settingsRunner, value => SettingsLaunchStatus = value));
        StartHiveMindCommand = new AsyncRelayCommand(StartHiveMindAsync);
        StopHiveMindCommand = new AsyncRelayCommand(() => StopRunnerAsync(_hiveMindRunner, value => HiveMindLaunchStatus = value));
        StartIoCommand = new AsyncRelayCommand(StartIoAsync);
        StopIoCommand = new AsyncRelayCommand(() => StopRunnerAsync(_ioRunner, value => IoLaunchStatus = value));
        StartObsCommand = new AsyncRelayCommand(StartObsAsync);
        StopObsCommand = new AsyncRelayCommand(() => StopRunnerAsync(_obsRunner, value => ObsLaunchStatus = value));
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

    public AsyncRelayCommand StartDemoCommand { get; }

    public AsyncRelayCommand StopDemoCommand { get; }

    public AsyncRelayCommand StartSettingsMonitorCommand { get; }

    public AsyncRelayCommand StopSettingsMonitorCommand { get; }

    public AsyncRelayCommand StartHiveMindCommand { get; }

    public AsyncRelayCommand StopHiveMindCommand { get; }

    public AsyncRelayCommand StartIoCommand { get; }

    public AsyncRelayCommand StopIoCommand { get; }

    public AsyncRelayCommand StartObsCommand { get; }

    public AsyncRelayCommand StopObsCommand { get; }

    public string DemoStatus
    {
        get => _demoStatus;
        set => SetProperty(ref _demoStatus, value);
    }

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

    public bool DemoRunning
    {
        get => _demoRunning;
        set => SetProperty(ref _demoRunning, value);
    }

    public void AddTermination(BrainTerminatedItem item)
    {
        _dispatcher.Post(() =>
        {
            Terminations.Insert(0, item);
            Trim(Terminations);
        });
    }

    public Task StopDemoAsyncForShutdown() => StopDemoAsync();

    public async Task StopAllAsyncForShutdown()
    {
        await StopDemoAsync().ConfigureAwait(false);
        await StopRunnerAsync(_settingsRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_hiveMindRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_ioRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_obsRunner, _ => { }).ConfigureAwait(false);
    }

    private async Task StartDemoAsync()
    {
        if (!TryParsePort(Connections.IoPortText, out var ioPort))
        {
            DemoStatus = "Invalid IO port.";
            return;
        }

        if (!TryParsePort(Connections.ObsPortText, out var obsPort))
        {
            DemoStatus = "Invalid Obs port.";
            return;
        }

        if (!TryParsePort(Connections.SettingsPortText, out var settingsPort))
        {
            DemoStatus = "Invalid Settings port.";
            return;
        }

        var options = new DemoLaunchOptions(
            ResolveDemoRoot(),
            Connections.LocalBindHost,
            12020,
            12011,
            12040,
            ioPort,
            obsPort,
            settingsPort);

        var result = await _demoRunner.StartAsync(options);
        DemoStatus = result.Message;
        DemoRunning = _demoRunner.IsRunning;
        if (DemoRunning)
        {
            SettingsLaunchStatus = "Managed by demo.";
            HiveMindLaunchStatus = "Managed by demo.";
            IoLaunchStatus = "Managed by demo.";
            ObsLaunchStatus = "Managed by demo.";
        }
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StopDemoAsync()
    {
        DemoStatus = await _demoRunner.StopAsync();
        DemoRunning = _demoRunner.IsRunning;
        if (!DemoRunning)
        {
            SettingsLaunchStatus = "Idle";
            HiveMindLaunchStatus = "Idle";
            IoLaunchStatus = "Idle";
            ObsLaunchStatus = "Idle";
        }
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

        var args = $"run --project \"{projectPath}\" -c Release --no-build -- --db \"{dbPath}\" --bind-host {Connections.SettingsHost} --port {port}";
        var startInfo = BuildDotnetStartInfo(args);
        var result = await _settingsRunner.StartAsync(startInfo, waitForExit: false);
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

        var args = $"run --project \"{projectPath}\" -c Release --no-build -- --bind-host {Connections.HiveMindHost} --port {port} --settings-db \"{Connections.SettingsDbPath}\""
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}";
        var startInfo = BuildDotnetStartInfo(args);
        var result = await _hiveMindRunner.StartAsync(startInfo, waitForExit: false);
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
        var args = $"run --project \"{projectPath}\" -c Release --no-build -- --bind-host {Connections.IoHost} --port {port}"
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}"
                 + $" --hivemind-address {hiveAddress} --hivemind-name {Connections.HiveMindName}";
        var startInfo = BuildDotnetStartInfo(args);
        var result = await _ioRunner.StartAsync(startInfo, waitForExit: false);
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

        var args = $"run --project \"{projectPath}\" -c Release --no-build -- --bind-host {Connections.ObsHost} --port {port}"
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}"
                 + " --enable-debug --enable-viz";
        var startInfo = BuildDotnetStartInfo(args);
        var result = await _obsRunner.StartAsync(startInfo, waitForExit: false);
        ObsLaunchStatus = result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private static async Task StopRunnerAsync(LocalServiceRunner runner, Action<string> setStatus)
    {
        setStatus(await runner.StopAsync().ConfigureAwait(false));
    }

    public async Task RefreshSettingsAsync()
    {
        await RefreshAsync().ConfigureAwait(false);
    }

    private async Task RefreshAsync()
    {
        if (!Connections.SettingsConnected)
        {
            StatusMessage = "SettingsMonitor not connected.";
            Connections.SettingsStatus = "Disconnected";
            return;
        }

        StatusMessage = "Loading settings...";
        Connections.SettingsStatus = "Loading";

        try
        {
            var nodesResponse = await _client.ListNodesAsync().ConfigureAwait(false);
            var brainsResponse = await _client.ListBrainsAsync().ConfigureAwait(false);

            var nodes = nodesResponse?.Nodes?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.NodeStatus>();
            var brains = brainsResponse?.Brains?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.BrainStatus>();
            var controllers = brainsResponse?.Controllers?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.BrainControllerStatus>();

            UpdateHiveMindEndpoint(nodes);

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
                    var seen = DateTimeOffset.FromUnixTimeMilliseconds((long)node.LastSeenMs).ToLocalTime();
                    Nodes.Add(new NodeStatusItem(
                        node.LogicalName ?? string.Empty,
                        node.Address ?? string.Empty,
                        node.RootActorName ?? string.Empty,
                        seen.ToString("g"),
                        node.IsAlive ? "online" : "offline"));
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

            var controllerMap = controllers
                .Where(entry => entry.BrainId is not null)
                .ToDictionary(entry => entry.BrainId!.ToGuid(), entry => entry);

            var brainList = brains.Select(entry =>
            {
                var brainId = entry.BrainId?.ToGuid() ?? Guid.Empty;
                var alive = controllerMap.TryGetValue(brainId, out var controller) && controller.IsAlive;
                return new BrainListItem(brainId, entry.State ?? string.Empty, alive);
            }).Where(entry => entry.BrainId != Guid.Empty).ToList();

            _brainsUpdated?.Invoke(brainList);

            StatusMessage = "Settings loaded.";
            Connections.SettingsStatus = "Ready";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Settings load failed: {ex.Message}";
            Connections.SettingsStatus = "Error";
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

    private static string ResolveDemoRoot()
        => RepoLocator.ResolvePathFromRepo("tools", "demo", "local-demo") ?? Path.Combine(Environment.CurrentDirectory, "tools", "demo", "local-demo");

    private async Task TriggerReconnectAsync()
    {
        if (_connectAll is null)
        {
            return;
        }

        await Task.Delay(500).ConfigureAwait(false);
        await _connectAll().ConfigureAwait(false);
        await RefreshAsync().ConfigureAwait(false);
    }
}
