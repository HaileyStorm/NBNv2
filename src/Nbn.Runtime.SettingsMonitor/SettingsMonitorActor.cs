using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Runtime.SettingsMonitor;

/// <summary>
/// Persists SettingsMonitor proto messages and serves merged runtime/query snapshots.
/// </summary>
public sealed partial class SettingsMonitorActor : IActor
{
    private static readonly TimeSpan DefaultExternalSettingsPollInterval = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan DefaultStaleDeadBrainPruneInterval = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan DefaultStaleDeadBrainRetention = TimeSpan.FromMinutes(30);
    private static readonly TimeSpan DefaultStaleNonLiveBrainRetention = TimeSpan.FromHours(1);

    private readonly SettingsMonitorStore _store;
    private readonly TimeSpan _externalSettingsPollInterval;
    private readonly TimeSpan _staleDeadBrainPruneInterval;
    private readonly TimeSpan _staleDeadBrainRetention;
    private readonly TimeSpan _staleNonLiveBrainRetention;
    private readonly Dictionary<string, PID> _subscribers = new(StringComparer.Ordinal);
    private readonly Dictionary<Guid, NodeStatus> _nodes = new();
    private readonly Dictionary<string, ObservedSetting> _observedSettings = new(StringComparer.OrdinalIgnoreCase);
    private bool _externalSettingsPollInFlight;
    private bool _staleDeadBrainPruneInFlight;

    /// <summary>
    /// Initializes a new actor instance backed by the provided store.
    /// </summary>
    public SettingsMonitorActor(
        SettingsMonitorStore store,
        TimeSpan? externalSettingsPollInterval = null,
        TimeSpan? staleDeadBrainPruneInterval = null,
        TimeSpan? staleDeadBrainRetention = null,
        TimeSpan? staleNonLiveBrainRetention = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        var pollInterval = externalSettingsPollInterval.GetValueOrDefault(DefaultExternalSettingsPollInterval);
        _externalSettingsPollInterval = pollInterval > TimeSpan.Zero
            ? pollInterval
            : DefaultExternalSettingsPollInterval;
        var pruneInterval = staleDeadBrainPruneInterval.GetValueOrDefault(DefaultStaleDeadBrainPruneInterval);
        _staleDeadBrainPruneInterval = pruneInterval > TimeSpan.Zero
            ? pruneInterval
            : DefaultStaleDeadBrainPruneInterval;
        var pruneRetention = staleDeadBrainRetention.GetValueOrDefault(DefaultStaleDeadBrainRetention);
        _staleDeadBrainRetention = pruneRetention > TimeSpan.Zero
            ? pruneRetention
            : DefaultStaleDeadBrainRetention;
        var staleNonLiveRetention = staleNonLiveBrainRetention.GetValueOrDefault(DefaultStaleNonLiveBrainRetention);
        _staleNonLiveBrainRetention = staleNonLiveRetention > TimeSpan.Zero
            ? staleNonLiveRetention
            : DefaultStaleNonLiveBrainRetention;
    }

    /// <summary>
    /// Handles SettingsMonitor lifecycle, node, brain, and settings messages.
    /// </summary>
    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Started:
                Initialize(context);
                break;
            case PollExternalSettings:
                HandlePollExternalSettings(context);
                break;
            case PruneStaleDeadBrains:
                HandlePruneStaleDeadBrains(context);
                break;
            case ProtoSettings.NodeOnline message:
                HandleNodeOnline(context, message);
                break;
            case ProtoSettings.NodeOffline message:
                HandleNodeOffline(context, message);
                break;
            case ProtoSettings.NodeHeartbeat message:
                HandleNodeHeartbeat(context, message);
                break;
            case ProtoSettings.NodeListRequest:
                HandleNodeList(context);
                break;
            case ProtoSettings.WorkerInventorySnapshotRequest:
                HandleWorkerInventorySnapshot(context);
                break;
            case ProtoSettings.SettingGet message:
                HandleSettingGet(context, message);
                break;
            case ProtoSettings.SettingSet message:
                HandleSettingSet(context, message);
                break;
            case ProtoSettings.BrainListRequest:
                HandleBrainList(context);
                break;
            case ProtoSettings.SettingListRequest:
                HandleSettingList(context);
                break;
            case ProtoSettings.BrainRegistered message:
                HandleBrainRegistered(context, message);
                break;
            case ProtoSettings.BrainStateChanged message:
                HandleBrainStateChanged(context, message);
                break;
            case ProtoSettings.BrainTick message:
                HandleBrainTick(context, message);
                break;
            case ProtoSettings.BrainControllerHeartbeat message:
                HandleBrainControllerHeartbeat(context, message);
                break;
            case ProtoSettings.BrainUnregistered message:
                HandleBrainUnregistered(context, message);
                break;
            case ProtoSettings.SettingSubscribe subscribe:
                HandleSettingSubscribe(context, subscribe);
                break;
            case ProtoSettings.SettingUnsubscribe unsubscribe:
                HandleSettingUnsubscribe(context, unsubscribe);
                break;
            case Terminated terminated:
                HandleTerminated(terminated);
                break;
        }

        return Task.CompletedTask;
    }
}
