using Nbn.Proto.Settings;
using Proto;
using System.IO;

namespace Nbn.Shared;

/// <summary>
/// Reports node liveness and capability heartbeats to SettingsMonitor on a fixed cadence.
/// </summary>
public sealed class SettingsMonitorReporter : IAsyncDisposable
{
    private static readonly TimeSpan DefaultHeartbeatInterval = TimeSpan.FromSeconds(5);

    private readonly ActorSystem _system;
    private readonly PID _settingsPid;
    private readonly NodeOnline _online;
    private readonly NodeOffline _offline;
    private readonly NodeCapabilities _fallbackCapabilities;
    private readonly Func<NodeCapabilities>? _capabilitiesProvider;
    private readonly TimeSpan _heartbeatInterval;
    private readonly CancellationTokenSource _cts = new();
    private Task? _loop;
    private NodeCapabilities? _lastGoodCapabilities;

    private SettingsMonitorReporter(
        ActorSystem system,
        PID settingsPid,
        NodeOnline online,
        NodeOffline offline,
        NodeCapabilities fallbackCapabilities,
        Func<NodeCapabilities>? capabilitiesProvider,
        TimeSpan heartbeatInterval)
    {
        _system = system;
        _settingsPid = settingsPid;
        _online = online;
        _offline = offline;
        _fallbackCapabilities = fallbackCapabilities;
        _capabilitiesProvider = capabilitiesProvider;
        _heartbeatInterval = heartbeatInterval;
        _lastGoodCapabilities = CloneCapabilities(fallbackCapabilities);
    }

    /// <summary>
    /// Starts a reporter when the SettingsMonitor location is described by host, port, and actor name.
    /// </summary>
    public static SettingsMonitorReporter? Start(
        ActorSystem? system,
        string? settingsHost,
        int settingsPort,
        string settingsName,
        string nodeAddress,
        string logicalName,
        string rootActorName,
        NodeCapabilities? capabilities = null,
        Func<NodeCapabilities>? capabilitiesProvider = null,
        TimeSpan? heartbeatInterval = null,
        Guid? nodeId = null)
    {
        return Start(
            system,
            CreateSettingsPid(settingsHost, settingsPort, settingsName),
            nodeAddress,
            logicalName,
            rootActorName,
            capabilities,
            capabilitiesProvider,
            heartbeatInterval,
            nodeId);
    }

    /// <summary>
    /// Starts a reporter bound directly to a SettingsMonitor actor PID.
    /// </summary>
    public static SettingsMonitorReporter? Start(
        ActorSystem? system,
        PID? settingsPid,
        string nodeAddress,
        string logicalName,
        string rootActorName,
        NodeCapabilities? capabilities = null,
        Func<NodeCapabilities>? capabilitiesProvider = null,
        TimeSpan? heartbeatInterval = null,
        Guid? nodeId = null)
    {
        if (system is null || settingsPid is null)
        {
            return null;
        }

        if (!TryCreateLifecycleMessages(nodeAddress, logicalName, rootActorName, nodeId, out var online, out var offline))
        {
            return null;
        }

        var reporter = new SettingsMonitorReporter(
            system,
            settingsPid,
            online,
            offline,
            capabilities ?? BuildDefaultCapabilities(),
            capabilitiesProvider,
            heartbeatInterval ?? DefaultHeartbeatInterval);

        reporter.StartInternal();
        return reporter;
    }

    private void StartInternal()
    {
        SendSettingsMessage(_online);
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
        SendSettingsMessage(_online);
        var capabilities = ResolveCapabilities();
        var heartbeat = new NodeHeartbeat
        {
            NodeId = _online.NodeId,
            TimeMs = GetObservationTimeMs(),
            Caps = capabilities
        };

        SendSettingsMessage(heartbeat);
        return Task.CompletedTask;
    }

    private NodeCapabilities ResolveCapabilities()
    {
        if (_capabilitiesProvider is null)
        {
            return GetLastGoodOrFallbackCapabilities();
        }

        try
        {
            var resolved = _capabilitiesProvider() ?? CloneCapabilities(_fallbackCapabilities);
            RememberLastGoodCapabilities(resolved);
            return resolved;
        }
        catch (Exception ex)
        {
            LogCapabilityProviderFailure(ex);
            return GetLastGoodOrFallbackCapabilities();
        }
    }

    /// <summary>
    /// Stops the heartbeat loop and reports the node as offline.
    /// </summary>
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
            SendSettingsMessage(_offline);
        }
        catch
        {
        }

        _cts.Dispose();
    }

    /// <summary>
    /// Builds a conservative fallback capability snapshot from the local process environment.
    /// </summary>
    public static NodeCapabilities BuildDefaultCapabilities()
    {
        var memory = ProbeFallbackMemory();
        var storage = ProbeFallbackStorage();
        return new NodeCapabilities
        {
            CpuCores = (uint)Math.Max(1, Environment.ProcessorCount),
            RamFreeBytes = memory.FreeBytes,
            StorageFreeBytes = storage.FreeBytes,
            HasGpu = false,
            GpuName = string.Empty,
            VramFreeBytes = 0,
            CpuScore = 0,
            GpuScore = 0,
            IlgpuCudaAvailable = false,
            IlgpuOpenclAvailable = false,
            RamTotalBytes = memory.TotalBytes,
            StorageTotalBytes = storage.TotalBytes,
            VramTotalBytes = 0,
            CpuLimitPercent = 100,
            RamLimitPercent = 100,
            StorageLimitPercent = 100,
            GpuComputeLimitPercent = 100,
            GpuVramLimitPercent = 100
        };
    }

    private static PID? CreateSettingsPid(string? settingsHost, int settingsPort, string? settingsName)
    {
        if (string.IsNullOrWhiteSpace(settingsHost) || settingsPort <= 0 || string.IsNullOrWhiteSpace(settingsName))
        {
            return null;
        }

        return new PID($"{settingsHost}:{settingsPort}", settingsName);
    }

    private static bool TryCreateLifecycleMessages(
        string? nodeAddress,
        string? logicalName,
        string? rootActorName,
        Guid? nodeId,
        out NodeOnline online,
        out NodeOffline offline)
    {
        online = new NodeOnline();
        offline = new NodeOffline();

        var resolvedNodeId = nodeId.GetValueOrDefault();
        if (resolvedNodeId == Guid.Empty)
        {
            resolvedNodeId = NodeIdentity.DeriveNodeId(nodeAddress ?? string.Empty);
        }

        if (resolvedNodeId == Guid.Empty)
        {
            return false;
        }

        online = new NodeOnline
        {
            NodeId = resolvedNodeId.ToProtoUuid(),
            LogicalName = logicalName ?? string.Empty,
            Address = nodeAddress ?? string.Empty,
            RootActorName = rootActorName ?? string.Empty
        };
        offline = new NodeOffline
        {
            NodeId = resolvedNodeId.ToProtoUuid(),
            LogicalName = logicalName ?? string.Empty
        };
        return true;
    }

    private static ulong GetObservationTimeMs()
        => (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private void SendSettingsMessage(object message)
        => _system.Root.Send(_settingsPid, message);

    private NodeCapabilities GetLastGoodOrFallbackCapabilities()
        => CloneCapabilities(_lastGoodCapabilities ?? _fallbackCapabilities);

    private void RememberLastGoodCapabilities(NodeCapabilities capabilities)
    {
        if (HasUsablePlacementCapacity(capabilities))
        {
            _lastGoodCapabilities = CloneCapabilities(capabilities);
        }
    }

    private void LogCapabilityProviderFailure(Exception ex)
    {
        Console.Error.WriteLine(
            $"[WARN] SettingsMonitorReporter capability provider failed for {_online.Address}/{_online.RootActorName}: {ex.GetBaseException().Message}");
    }

    private static bool HasUsablePlacementCapacity(NodeCapabilities capabilities)
        => capabilities is not null
           && capabilities.CpuCores > 0
           && capabilities.RamFreeBytes > 0
           && capabilities.RamTotalBytes > 0
           && capabilities.StorageFreeBytes > 0
           && capabilities.StorageTotalBytes > 0;

    private static NodeCapabilities CloneCapabilities(NodeCapabilities source)
        => new()
        {
            CpuCores = source.CpuCores,
            RamFreeBytes = source.RamFreeBytes,
            StorageFreeBytes = source.StorageFreeBytes,
            HasGpu = source.HasGpu,
            GpuName = source.GpuName,
            VramFreeBytes = source.VramFreeBytes,
            CpuScore = source.CpuScore,
            GpuScore = source.GpuScore,
            IlgpuCudaAvailable = source.IlgpuCudaAvailable,
            IlgpuOpenclAvailable = source.IlgpuOpenclAvailable,
            RamTotalBytes = source.RamTotalBytes,
            StorageTotalBytes = source.StorageTotalBytes,
            VramTotalBytes = source.VramTotalBytes,
            CpuLimitPercent = source.CpuLimitPercent,
            RamLimitPercent = source.RamLimitPercent,
            StorageLimitPercent = source.StorageLimitPercent,
            GpuComputeLimitPercent = source.GpuComputeLimitPercent,
            GpuVramLimitPercent = source.GpuVramLimitPercent,
            ProcessCpuLoadPercent = source.ProcessCpuLoadPercent,
            ProcessRamUsedBytes = source.ProcessRamUsedBytes
        };

    private static FallbackCapacity ProbeFallbackMemory()
    {
        var totalAvailable = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        var totalBytes = totalAvailable > 0 ? (ulong)totalAvailable : 0UL;
        return new FallbackCapacity(totalBytes, totalBytes);
    }

    private static FallbackCapacity ProbeFallbackStorage()
    {
        try
        {
            var fullPath = Path.GetFullPath(Environment.CurrentDirectory);
            DriveInfo? drive = null;
            foreach (var candidate in DriveInfo.GetDrives())
            {
                try
                {
                    if (!candidate.IsReady || !fullPath.StartsWith(candidate.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    if (drive is null || candidate.Name.Length > drive.Name.Length)
                    {
                        drive = candidate;
                    }
                }
                catch
                {
                }
            }

            if (drive is null)
            {
                return FallbackCapacity.Empty;
            }

            return new FallbackCapacity(
                drive.AvailableFreeSpace > 0 ? (ulong)drive.AvailableFreeSpace : 0UL,
                drive.TotalSize > 0 ? (ulong)drive.TotalSize : 0UL);
        }
        catch
        {
            return FallbackCapacity.Empty;
        }
    }

    private readonly record struct FallbackCapacity(ulong FreeBytes, ulong TotalBytes)
    {
        public static readonly FallbackCapacity Empty = new(0, 0);
    }
}
