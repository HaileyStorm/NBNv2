using Nbn.Proto.Settings;
using Proto;

namespace Nbn.Shared;

public sealed class SettingsMonitorReporter : IAsyncDisposable
{
    private readonly ActorSystem _system;
    private readonly PID _settingsPid;
    private readonly NodeOnline _online;
    private readonly NodeOffline _offline;
    private readonly NodeCapabilities _capabilities;
    private readonly TimeSpan _heartbeatInterval;
    private readonly CancellationTokenSource _cts = new();
    private Task? _loop;

    private SettingsMonitorReporter(
        ActorSystem system,
        PID settingsPid,
        NodeOnline online,
        NodeOffline offline,
        NodeCapabilities capabilities,
        TimeSpan heartbeatInterval)
    {
        _system = system;
        _settingsPid = settingsPid;
        _online = online;
        _offline = offline;
        _capabilities = capabilities;
        _heartbeatInterval = heartbeatInterval;
    }

    public static SettingsMonitorReporter? Start(
        ActorSystem? system,
        string? settingsHost,
        int settingsPort,
        string settingsName,
        string nodeAddress,
        string logicalName,
        string rootActorName,
        NodeCapabilities? capabilities = null,
        TimeSpan? heartbeatInterval = null)
    {
        if (system is null)
        {
            return null;
        }

        if (string.IsNullOrWhiteSpace(settingsHost) || settingsPort <= 0 || string.IsNullOrWhiteSpace(settingsName))
        {
            return null;
        }

        var nodeId = NodeIdentity.DeriveNodeId(nodeAddress);
        if (nodeId == Guid.Empty)
        {
            return null;
        }

        var settingsPid = new PID($"{settingsHost}:{settingsPort}", settingsName);
        var online = new NodeOnline
        {
            NodeId = nodeId.ToProtoUuid(),
            LogicalName = logicalName ?? string.Empty,
            Address = nodeAddress ?? string.Empty,
            RootActorName = rootActorName ?? string.Empty
        };
        var offline = new NodeOffline
        {
            NodeId = nodeId.ToProtoUuid(),
            LogicalName = logicalName ?? string.Empty
        };

        var reporter = new SettingsMonitorReporter(
            system,
            settingsPid,
            online,
            offline,
            capabilities ?? BuildDefaultCapabilities(),
            heartbeatInterval ?? TimeSpan.FromSeconds(5));

        reporter.StartInternal();
        return reporter;
    }

    private void StartInternal()
    {
        _system.Root.Send(_settingsPid, _online);
        _loop = RunHeartbeatAsync(_cts.Token);
    }

    private async Task RunHeartbeatAsync(CancellationToken token)
    {
        await SendHeartbeatAsync().ConfigureAwait(false);

        using var timer = new PeriodicTimer(_heartbeatInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(token).ConfigureAwait(false))
            {
                await SendHeartbeatAsync().ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private Task SendHeartbeatAsync()
    {
        _system.Root.Send(_settingsPid, _online);
        var heartbeat = new NodeHeartbeat
        {
            NodeId = _online.NodeId,
            TimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Caps = _capabilities
        };

        _system.Root.Send(_settingsPid, heartbeat);
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        if (_loop is not null)
        {
            try
            {
                await _loop.ConfigureAwait(false);
            }
            catch
            {
            }
        }

        try
        {
            _system.Root.Send(_settingsPid, _offline);
        }
        catch
        {
        }

        _cts.Dispose();
    }

    public static NodeCapabilities BuildDefaultCapabilities()
    {
        return new NodeCapabilities
        {
            CpuCores = (uint)Math.Max(1, Environment.ProcessorCount),
            RamFreeBytes = 0,
            HasGpu = false,
            GpuName = string.Empty,
            VramFreeBytes = 0,
            CpuScore = 0,
            GpuScore = 0,
            IlgpuCudaAvailable = false,
            IlgpuOpenclAvailable = false
        };
    }
}
