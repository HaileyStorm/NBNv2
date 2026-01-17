using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using Nbn.Runtime.SettingsMonitor;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class OrchestratorPanelViewModel : ViewModelBase
{
    private const int MaxRows = 200;
    private readonly UiDispatcher _dispatcher;
    private readonly ConnectionViewModel _connections;
    private string _statusMessage = "Idle";

    public OrchestratorPanelViewModel(UiDispatcher dispatcher, ConnectionViewModel connections)
    {
        _dispatcher = dispatcher;
        _connections = connections;
        Nodes = new ObservableCollection<NodeStatusItem>();
        Settings = new ObservableCollection<SettingItem>();
        Terminations = new ObservableCollection<BrainTerminatedItem>();
        RefreshCommand = new AsyncRelayCommand(RefreshAsync);
    }

    public ObservableCollection<NodeStatusItem> Nodes { get; }
    public ObservableCollection<SettingItem> Settings { get; }
    public ObservableCollection<BrainTerminatedItem> Terminations { get; }

    public ConnectionViewModel Connections => _connections;

    public string StatusMessage
    {
        get => _statusMessage;
        set => SetProperty(ref _statusMessage, value);
    }

    public AsyncRelayCommand RefreshCommand { get; }

    public void AddTermination(BrainTerminatedItem item)
    {
        _dispatcher.Post(() =>
        {
            Terminations.Insert(0, item);
            Trim(Terminations);
        });
    }

    private async Task RefreshAsync()
    {
        var dbPath = Connections.SettingsDbPath;
        if (string.IsNullOrWhiteSpace(dbPath))
        {
            StatusMessage = "Database path required.";
            return;
        }

        StatusMessage = "Loading settings...";
        Connections.SettingsStatus = "Loading";

        try
        {
            var store = new SettingsMonitorStore(dbPath);
            await store.InitializeAsync();
            await store.EnsureDefaultSettingsAsync();

            var nodes = await store.ListNodesAsync();
            var compression = await store.GetArtifactCompressionSettingsAsync();

            var settings = new List<SettingItem>
            {
                new(SettingsMonitorDefaults.ArtifactChunkCompressionKindKey, compression.Kind, "auto"),
                new(SettingsMonitorDefaults.ArtifactChunkCompressionLevelKey, compression.Level.ToString(), "auto"),
                new(SettingsMonitorDefaults.ArtifactChunkCompressionMinBytesKey, compression.MinBytes.ToString(), "auto"),
                new(SettingsMonitorDefaults.ArtifactChunkCompressionOnlyIfSmallerKey, compression.OnlyIfSmaller.ToString(), "auto")
            };

            _dispatcher.Post(() =>
            {
                Nodes.Clear();
                foreach (var node in nodes)
                {
                    var seen = DateTimeOffset.FromUnixTimeMilliseconds(node.LastSeenMs).ToLocalTime();
                    Nodes.Add(new NodeStatusItem(
                        node.LogicalName,
                        node.Address,
                        node.RootActorName,
                        seen.ToString("g"),
                        node.IsAlive ? "online" : "offline"));
                }

                Settings.Clear();
                foreach (var entry in settings)
                {
                    Settings.Add(entry);
                }

                Trim(Nodes);
                Trim(Settings);
            });

            StatusMessage = "Settings loaded.";
            Connections.SettingsStatus = "Ready";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Settings load failed: {ex.Message}";
            Connections.SettingsStatus = "Error";
        }
    }

    private static void Trim<T>(ObservableCollection<T> collection)
    {
        while (collection.Count > MaxRows)
        {
            collection.RemoveAt(collection.Count - 1);
        }
    }
}
