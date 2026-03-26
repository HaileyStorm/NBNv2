using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private void HandleNodeListResponse(ProtoSettings.NodeListResponse message)
    {
        _activeSettingsNodeAddresses.Clear();
        foreach (var node in message.Nodes)
        {
            var normalizedAddress = NormalizeEndpointAddress(node.Address);
            if (string.IsNullOrWhiteSpace(normalizedAddress))
            {
                continue;
            }

            _knownSettingsNodeAddresses.Add(normalizedAddress);
            if (node.IsAlive)
            {
                _activeSettingsNodeAddresses.Add(normalizedAddress);
            }
        }
    }

    private void HandleSweepVisualizationSubscribers(IContext context)
    {
        try
        {
            SweepSubscribersBySettingsNodeLiveness(context);
            SweepSubscribersByLocalProcessLiveness(context);
            SyncVisualizationScopeToShards(context);
        }
        finally
        {
            ScheduleSelf(context, VisualizationSubscriberSweepInterval, new SweepVisualizationSubscribers());
        }
    }

    private void SyncVisualizationScopeToShards(IContext context)
    {
        if (_brains.Count == 0)
        {
            return;
        }

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (nowMs < _nextVisualizationShardSyncMs)
        {
            return;
        }

        _nextVisualizationShardSyncMs = nowMs + (long)VisualizationShardSyncInterval.TotalMilliseconds;
        foreach (var brain in _brains.Values)
        {
            if (!brain.VisualizationEnabled || brain.Shards.Count == 0)
            {
                continue;
            }

            foreach (var entry in brain.Shards)
            {
                SendShardVisualizationUpdate(
                    context,
                    brain.BrainId,
                    entry.Key,
                    entry.Value,
                    enabled: true,
                    brain.VisualizationFocusRegionId,
                    _vizStreamMinIntervalMs);
            }
        }
    }

    private void SweepSubscribersBySettingsNodeLiveness(IContext context)
    {
        if (_vizSubscriberLeases.Count == 0 || _knownSettingsNodeAddresses.Count == 0)
        {
            return;
        }

        foreach (var entry in _vizSubscriberLeases.ToArray())
        {
            var pid = entry.Value.Pid;
            if (pid is null || string.IsNullOrWhiteSpace(pid.Address))
            {
                continue;
            }

            var normalizedAddress = NormalizeEndpointAddress(pid.Address);
            if (string.IsNullOrWhiteSpace(normalizedAddress))
            {
                continue;
            }

            if (!_knownSettingsNodeAddresses.Contains(normalizedAddress)
                || _activeSettingsNodeAddresses.Contains(normalizedAddress))
            {
                continue;
            }

            RemoveVisualizationSubscriber(context, entry.Key);
        }
    }

    private void SweepSubscribersByLocalProcessLiveness(IContext context)
    {
        if (_vizSubscriberLeases.Count == 0)
        {
            return;
        }

        foreach (var entry in _vizSubscriberLeases.ToArray())
        {
            var pid = entry.Value.Pid;
            if (!IsLikelyLocalSubscriberPid(context.System, pid))
            {
                continue;
            }

            if (!TryLookupProcessInRegistry(context.System, pid!, out var process))
            {
                continue;
            }

            if (process is null || process.GetType().Name.Contains("DeadLetter", StringComparison.OrdinalIgnoreCase))
            {
                RemoveVisualizationSubscriber(context, entry.Key);
            }
        }
    }
}
